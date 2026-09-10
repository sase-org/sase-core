//! Pure runner-capacity accounting and queue eligibility.
//!
//! Host adapters own locking, filesystem markers, and process liveness. This
//! module owns the deterministic projection from already-collected record facts
//! to occupied capacity, waiters, blockers, and the next admissible waiter.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::queue_directive::{queue_weight_is_valid, DEFAULT_QUEUE_WEIGHT};

pub const RUNNER_CAPACITY_POLICY_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_WAIT_PRIORITY: i32 = 10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityRequestWire {
    #[serde(default = "runner_capacity_policy_schema_version")]
    pub schema_version: u32,
    pub effective_limit: f64,
    #[serde(default)]
    pub records: Vec<RunnerCapacityRecordWire>,
    #[serde(default)]
    pub now: Option<String>,
    #[serde(default)]
    pub deference_seconds_per_step: u32,
    #[serde(default)]
    pub deference_max_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityRecordWire {
    pub artifact_dir: String,
    pub project_name: String,
    #[serde(default = "default_workflow_dir")]
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default = "default_true")]
    pub has_agent_meta: bool,
    #[serde(default)]
    pub has_done_marker: bool,
    #[serde(default = "default_true")]
    pub appears_as_agent: bool,
    #[serde(default = "default_true")]
    pub live: bool,
    #[serde(default)]
    pub pending_question: bool,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub run_started_at: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub agent_family: Option<String>,
    #[serde(default)]
    pub agent_family_role: Option<String>,
    #[serde(default)]
    pub agent_family_parallel: bool,
    #[serde(default)]
    pub family_shell_kind: Option<String>,
    #[serde(default)]
    pub family_shell_id: Option<String>,
    #[serde(default)]
    pub family_shell_state: Option<String>,
    #[serde(default)]
    pub queue_weight: Option<f64>,
    #[serde(default)]
    pub queue_weight_explicit: bool,
    #[serde(default)]
    pub queue_weight_invalid: bool,
    #[serde(default)]
    pub slot_requested_at: Option<String>,
    #[serde(default)]
    pub wait_runners: Option<i64>,
    #[serde(default)]
    pub wait_runners_explicit: bool,
    #[serde(default)]
    pub wait_priority: Option<i64>,
    #[serde(default)]
    pub eligible_since: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityDiagnosticWire {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityClaimWire {
    pub owner_key: String,
    pub project_name: String,
    pub claim_kind: String,
    pub occupied_lanes: u32,
    pub occupied_capacity: f64,
    #[serde(default)]
    pub artifact_dirs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityBlockerWire {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needed_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occupied_lanes: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_threshold: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityWaiterWire {
    pub artifact_dir: String,
    pub queue_position: u32,
    pub priority: i32,
    pub slot_requested_at: String,
    pub timestamp: String,
    pub requested_weight: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_runners: Option<u32>,
    pub eligible: bool,
    #[serde(default)]
    pub blockers: Vec<RunnerCapacityBlockerWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacitySnapshotWire {
    pub schema_version: u32,
    pub effective_limit: f64,
    pub occupied_lanes: u32,
    pub occupied_capacity: f64,
    #[serde(default)]
    pub claims: Vec<RunnerCapacityClaimWire>,
    #[serde(default)]
    pub waiters: Vec<RunnerCapacityWaiterWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_eligible_artifact_dir: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<RunnerCapacityDiagnosticWire>,
}

struct WaiterEvaluation<'a> {
    request: &'a RunnerCapacityRequestWire,
    record: &'a RunnerCapacityRecordWire,
    occupied_lanes: u32,
    occupied_capacity: f64,
    requested_weight: f64,
    wait_runners: Option<u32>,
    priority: i32,
    fail_closed: bool,
}

pub fn runner_capacity_policy_schema_version() -> u32 {
    RUNNER_CAPACITY_POLICY_SCHEMA_VERSION
}

pub fn runner_capacity_snapshot(
    request: &RunnerCapacityRequestWire,
) -> RunnerCapacitySnapshotWire {
    let mut diagnostics = Vec::new();
    let (mut claims, invalid_live_claim) =
        build_claims(&request.records, &mut diagnostics);
    claims.sort_by(|left, right| {
        left.owner_key
            .cmp(&right.owner_key)
            .then_with(|| left.claim_kind.cmp(&right.claim_kind))
    });
    let occupied_lanes =
        claims.iter().map(|claim| claim.occupied_lanes).sum::<u32>();
    let (occupied_capacity, capacity_overflow) = match compensated_sum(
        claims.iter().map(|claim| claim.occupied_capacity),
    ) {
        Some(value) => (value, false),
        None => {
            diagnostics.push(diagnostic(
                "capacity-overflow",
                "Occupied runner capacity overflowed while summing live claims.",
                None,
            ));
            (f64::MAX, true)
        }
    };
    let limit_invalid = !queue_weight_is_valid(request.effective_limit);
    if limit_invalid {
        diagnostics.push(diagnostic(
            "invalid-capacity-limit",
            "The effective runner capacity limit must be positive and finite.",
            None,
        ));
    }
    let active_serial_claims = active_serial_claim_keys(&claims);
    let fail_closed = invalid_live_claim || capacity_overflow || limit_invalid;
    let waiters = build_waiters(
        request,
        occupied_lanes,
        occupied_capacity,
        &active_serial_claims,
        fail_closed,
    );
    let first_eligible_artifact_dir = waiters
        .iter()
        .find(|waiter| waiter.eligible)
        .map(|waiter| waiter.artifact_dir.clone());

    RunnerCapacitySnapshotWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit: request.effective_limit,
        occupied_lanes,
        occupied_capacity,
        claims,
        waiters,
        first_eligible_artifact_dir,
        diagnostics,
    }
}

fn build_claims(
    records: &[RunnerCapacityRecordWire],
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> (Vec<RunnerCapacityClaimWire>, bool) {
    let mut groups: BTreeMap<(String, String), Vec<&RunnerCapacityRecordWire>> =
        BTreeMap::new();
    for record in records {
        if is_occupying_record(record) {
            groups
                .entry(family_group_key(record))
                .or_default()
                .push(record);
        }
    }

    let mut claims = Vec::new();
    let mut invalid_live_claim = false;
    for ((project_name, family_key), group) in groups {
        let mut serial_records = Vec::new();
        for record in group {
            if record.agent_family_parallel {
                match effective_weight(record) {
                    Ok(weight) => claims.push(RunnerCapacityClaimWire {
                        owner_key: format!(
                            "{project_name}:{family_key}:{}",
                            record.artifact_dir
                        ),
                        project_name: project_name.clone(),
                        claim_kind: "parallel_member".to_string(),
                        occupied_lanes: 1,
                        occupied_capacity: weight,
                        artifact_dirs: vec![record.artifact_dir.clone()],
                    }),
                    Err(message) => {
                        invalid_live_claim = true;
                        diagnostics.push(diagnostic(
                            "invalid-live-claim-weight",
                            &message,
                            Some(record.artifact_dir.clone()),
                        ));
                    }
                }
            } else {
                serial_records.push(record);
            }
        }
        if serial_records.is_empty() {
            continue;
        }
        let mut max_weight = 0.0;
        let mut artifact_dirs = Vec::new();
        let mut serial_invalid = false;
        for record in serial_records {
            artifact_dirs.push(record.artifact_dir.clone());
            match effective_weight(record) {
                Ok(weight) => {
                    if weight > max_weight {
                        max_weight = weight;
                    }
                }
                Err(message) => {
                    serial_invalid = true;
                    invalid_live_claim = true;
                    diagnostics.push(diagnostic(
                        "invalid-live-claim-weight",
                        &message,
                        Some(record.artifact_dir.clone()),
                    ));
                }
            }
        }
        if !serial_invalid {
            let claim_kind = if artifact_dirs.len() > 1
                || artifact_dirs
                    .first()
                    .and_then(|dir| {
                        records.iter().find(|r| &r.artifact_dir == dir)
                    })
                    .and_then(|record| record.agent_family.as_ref())
                    .is_some()
            {
                "serial_family"
            } else {
                "standalone"
            };
            claims.push(RunnerCapacityClaimWire {
                owner_key: format!("{project_name}:{family_key}"),
                project_name,
                claim_kind: claim_kind.to_string(),
                occupied_lanes: 1,
                occupied_capacity: max_weight,
                artifact_dirs,
            });
        }
    }
    (claims, invalid_live_claim)
}

fn build_waiters(
    request: &RunnerCapacityRequestWire,
    occupied_lanes: u32,
    occupied_capacity: f64,
    active_serial_claims: &BTreeSet<String>,
    fail_closed: bool,
) -> Vec<RunnerCapacityWaiterWire> {
    let mut candidates: Vec<&RunnerCapacityRecordWire> = request
        .records
        .iter()
        .filter(|record| is_waiting_record(record, active_serial_claims))
        .collect();
    candidates.sort_by(|left, right| compare_waiters(left, right));

    let mut waiters = Vec::new();
    let mut prior_eligible = false;
    for (index, record) in candidates.into_iter().enumerate() {
        let priority = normalize_wait_priority(record.wait_priority);
        let requested_weight =
            effective_weight(record).unwrap_or(DEFAULT_QUEUE_WEIGHT);
        let wait_runners = explicit_wait_runners(record);
        let mut blockers = waiter_blockers(WaiterEvaluation {
            request,
            record,
            occupied_lanes,
            occupied_capacity,
            requested_weight,
            wait_runners,
            priority,
            fail_closed,
        });
        if blockers.is_empty() {
            if prior_eligible {
                blockers.push(blocker(
                    "queue-order",
                    "An earlier currently eligible waiter is ahead in priority/FIFO order.",
                    None,
                    None,
                    None,
                    None,
                ));
            } else {
                prior_eligible = true;
            }
        }
        waiters.push(RunnerCapacityWaiterWire {
            artifact_dir: record.artifact_dir.clone(),
            queue_position: (index + 1) as u32,
            priority,
            slot_requested_at: record
                .slot_requested_at
                .clone()
                .unwrap_or_default(),
            timestamp: record.timestamp.clone(),
            requested_weight,
            wait_runners,
            eligible: blockers.is_empty(),
            blockers,
        });
    }
    waiters
}

fn waiter_blockers(
    eval: WaiterEvaluation<'_>,
) -> Vec<RunnerCapacityBlockerWire> {
    let mut blockers = Vec::new();
    if eval.fail_closed {
        blockers.push(blocker(
            "capacity-snapshot-invalid",
            "Runner capacity cannot admit new work because the live snapshot is invalid.",
            None,
            None,
            None,
            None,
        ));
    }
    if let Err(message) = effective_weight(eval.record) {
        blockers.push(blocker(
            "invalid-request-weight",
            &message,
            None,
            None,
            None,
            None,
        ));
    }
    if !queue_weight_is_valid(eval.request.effective_limit) {
        blockers.push(blocker(
            "invalid-capacity-limit",
            "The effective runner capacity limit must be positive and finite.",
            None,
            None,
            None,
            None,
        ));
    } else {
        let free_capacity =
            (eval.request.effective_limit - eval.occupied_capacity).max(0.0);
        if !capacity_fits(eval.requested_weight, eval.request.effective_limit) {
            blockers.push(blocker(
                "weight-exceeds-limit",
                "Requested capacity weight exceeds the effective runner limit.",
                Some(eval.requested_weight),
                Some(free_capacity),
                None,
                None,
            ));
        } else if !capacity_fits(eval.requested_weight, free_capacity) {
            blockers.push(blocker(
                "insufficient-capacity",
                "Insufficient free runner capacity for this waiter.",
                Some(eval.requested_weight),
                Some(free_capacity),
                None,
                None,
            ));
        }
    }
    if let Some(threshold) = eval.wait_runners {
        if eval.occupied_lanes > threshold {
            blockers.push(blocker(
                "runner-count-condition",
                "Too many occupied runner lanes for this explicit runners condition.",
                None,
                None,
                Some(eval.occupied_lanes),
                Some(threshold),
            ));
        }
    }
    if blockers.is_empty()
        && eval.priority > DEFAULT_WAIT_PRIORITY
        && deference_window_seconds(
            eval.priority,
            eval.request.deference_seconds_per_step,
            eval.request.deference_max_seconds,
        ) > 0.0
        && better_priority_agent_pending(
            &eval.request.records,
            eval.record,
            eval.priority,
        )
        && !deference_satisfied(
            eval.record.eligible_since.as_deref(),
            eval.request.now.as_deref(),
            deference_window_seconds(
                eval.priority,
                eval.request.deference_seconds_per_step,
                eval.request.deference_max_seconds,
            ),
        )
    {
        blockers.push(blocker(
            "deference-window",
            "A better-priority unparked waiter is pending and the deference window has not elapsed.",
            None,
            None,
            None,
            None,
        ));
    }
    blockers
}

fn active_serial_claim_keys(
    claims: &[RunnerCapacityClaimWire],
) -> BTreeSet<String> {
    claims
        .iter()
        .filter(|claim| claim.claim_kind == "serial_family")
        .map(|claim| claim.owner_key.clone())
        .collect()
}

fn is_user_agent_record(record: &RunnerCapacityRecordWire) -> bool {
    record.workflow_dir_name == "ace-run"
        && !record.has_done_marker
        && record.has_agent_meta
        && record.appears_as_agent
}

fn is_occupying_record(record: &RunnerCapacityRecordWire) -> bool {
    if !is_user_agent_record(record)
        || !record.live
        || record.pending_question
        || is_pending_gate(record)
    {
        return false;
    }
    let started = if is_real_monitor_member(record) {
        record.pid.is_some()
    } else {
        record
            .run_started_at
            .as_deref()
            .is_some_and(|value| !value.is_empty())
    };
    started
}

fn is_waiting_record(
    record: &RunnerCapacityRecordWire,
    active_serial_claims: &BTreeSet<String>,
) -> bool {
    if !is_user_agent_record(record)
        || !record.live
        || record
            .slot_requested_at
            .as_deref()
            .map_or(true, |value| value.is_empty())
    {
        return false;
    }
    if record.parent_timestamp.is_some() && !record.agent_family_parallel {
        let (project, family) = family_group_key(record);
        let owner_key = format!("{project}:{family}");
        return !active_serial_claims.contains(&owner_key);
    }
    true
}

fn is_pending_gate(record: &RunnerCapacityRecordWire) -> bool {
    record.agent_family_role.as_deref() == Some("gate")
        && record.family_shell_kind.as_deref() == Some("gate")
        && record
            .family_shell_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && record.family_shell_state.as_deref() == Some("pending")
}

fn is_real_monitor_member(record: &RunnerCapacityRecordWire) -> bool {
    record.agent_family_role.as_deref() == Some("monitor")
        && record.family_shell_kind.as_deref() == Some("monitor")
        && record
            .family_shell_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn family_group_key(record: &RunnerCapacityRecordWire) -> (String, String) {
    (
        record.project_name.clone(),
        record
            .agent_family
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| record.timestamp.clone()),
    )
}

fn effective_weight(record: &RunnerCapacityRecordWire) -> Result<f64, String> {
    if record.queue_weight_invalid {
        return Err(format!(
            "{} has an invalid queue_weight; expected a positive finite capacity weight.",
            record.artifact_dir
        ));
    }
    match record.queue_weight {
        Some(weight) if queue_weight_is_valid(weight) => Ok(weight),
        Some(_) => Err(format!(
            "{} has an invalid queue_weight; expected a positive finite capacity weight.",
            record.artifact_dir
        )),
        None => Ok(DEFAULT_QUEUE_WEIGHT),
    }
}

fn explicit_wait_runners(record: &RunnerCapacityRecordWire) -> Option<u32> {
    if !record.wait_runners_explicit {
        return None;
    }
    let runners = record.wait_runners?;
    u32::try_from(runners).ok()
}

fn normalize_wait_priority(value: Option<i64>) -> i32 {
    value
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value >= 0)
        .unwrap_or(DEFAULT_WAIT_PRIORITY)
}

fn compare_waiters(
    left: &RunnerCapacityRecordWire,
    right: &RunnerCapacityRecordWire,
) -> Ordering {
    normalize_wait_priority(left.wait_priority)
        .cmp(&normalize_wait_priority(right.wait_priority))
        .then_with(|| {
            requested_at_key(left.slot_requested_at.as_deref())
                .cmp(&requested_at_key(right.slot_requested_at.as_deref()))
        })
        .then_with(|| left.timestamp.cmp(&right.timestamp))
        .then_with(|| left.artifact_dir.cmp(&right.artifact_dir))
}

fn requested_at_key(value: Option<&str>) -> (u8, i64) {
    let Some(value) = value else {
        return (1, i64::MAX);
    };
    match DateTime::parse_from_rfc3339(value) {
        Ok(parsed) => (0, parsed.with_timezone(&Utc).timestamp_millis()),
        Err(_) => (1, i64::MAX),
    }
}

fn better_priority_agent_pending(
    records: &[RunnerCapacityRecordWire],
    me: &RunnerCapacityRecordWire,
    priority: i32,
) -> bool {
    records.iter().any(|record| {
        record.artifact_dir != me.artifact_dir
            && is_user_agent_record(record)
            && record.live
            && record
                .run_started_at
                .as_deref()
                .map_or(true, |value| value.is_empty())
            && record
                .slot_requested_at
                .as_deref()
                .map_or(true, |value| value.is_empty())
            && normalize_wait_priority(record.wait_priority) < priority
    })
}

fn deference_window_seconds(
    priority: i32,
    seconds_per_step: u32,
    max_seconds: u32,
) -> f64 {
    if priority <= DEFAULT_WAIT_PRIORITY {
        return 0.0;
    }
    ((priority - DEFAULT_WAIT_PRIORITY) as u32)
        .saturating_mul(seconds_per_step)
        .min(max_seconds) as f64
}

fn deference_satisfied(
    eligible_since: Option<&str>,
    now: Option<&str>,
    window_seconds: f64,
) -> bool {
    if window_seconds <= 0.0 {
        return true;
    }
    let (Some(eligible_since), Some(now)) = (eligible_since, now) else {
        return false;
    };
    let Ok(started) = DateTime::parse_from_rfc3339(eligible_since) else {
        return false;
    };
    let Ok(now) = DateTime::parse_from_rfc3339(now) else {
        return false;
    };
    let elapsed = now
        .with_timezone(&Utc)
        .signed_duration_since(started.with_timezone(&Utc))
        .num_milliseconds() as f64
        / 1000.0;
    elapsed >= 0.0 && elapsed >= window_seconds
}

fn compensated_sum(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for value in values {
        if !value.is_finite() {
            return None;
        }
        let y = value - compensation;
        let next = sum + y;
        if !next.is_finite() {
            return None;
        }
        compensation = (next - sum) - y;
        sum = next;
    }
    Some(sum)
}

fn capacity_fits(total: f64, limit: f64) -> bool {
    if !total.is_finite() || !limit.is_finite() {
        return false;
    }
    if total <= limit {
        return true;
    }
    total - limit <= 4.0 * ulp_at(total.max(limit))
}

fn ulp_at(value: f64) -> f64 {
    if value == 0.0 {
        return f64::MIN_POSITIVE;
    }
    let value = value.abs();
    if !value.is_finite() {
        return f64::INFINITY;
    }
    next_up(value) - value
}

fn next_up(value: f64) -> f64 {
    if value.is_nan() || value == f64::INFINITY {
        return value;
    }
    if value == -0.0 {
        return f64::MIN_POSITIVE;
    }
    let bits = value.to_bits();
    if value >= 0.0 {
        f64::from_bits(bits + 1)
    } else {
        f64::from_bits(bits - 1)
    }
}

fn diagnostic(
    code: &str,
    message: &str,
    artifact_dir: Option<String>,
) -> RunnerCapacityDiagnosticWire {
    RunnerCapacityDiagnosticWire {
        code: code.to_string(),
        message: message.to_string(),
        artifact_dir,
    }
}

fn blocker(
    code: &str,
    message: &str,
    needed_capacity: Option<f64>,
    free_capacity: Option<f64>,
    occupied_lanes: Option<u32>,
    runner_threshold: Option<u32>,
) -> RunnerCapacityBlockerWire {
    RunnerCapacityBlockerWire {
        code: code.to_string(),
        message: message.to_string(),
        needed_capacity,
        free_capacity,
        occupied_lanes,
        runner_threshold,
    }
}

fn default_true() -> bool {
    true
}

fn default_workflow_dir() -> String {
    "ace-run".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(name: &str) -> RunnerCapacityRecordWire {
        RunnerCapacityRecordWire {
            artifact_dir: format!("/tmp/{name}"),
            project_name: "proj".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            timestamp: name.to_string(),
            has_agent_meta: true,
            has_done_marker: false,
            appears_as_agent: true,
            live: true,
            pending_question: false,
            pid: None,
            run_started_at: None,
            parent_timestamp: None,
            agent_family: None,
            agent_family_role: None,
            agent_family_parallel: false,
            family_shell_kind: None,
            family_shell_id: None,
            family_shell_state: None,
            queue_weight: None,
            queue_weight_explicit: false,
            queue_weight_invalid: false,
            slot_requested_at: None,
            wait_runners: None,
            wait_runners_explicit: false,
            wait_priority: None,
            eligible_since: None,
        }
    }

    fn running(name: &str, weight: Option<f64>) -> RunnerCapacityRecordWire {
        let mut record = rec(name);
        record.run_started_at = Some("2026-09-10T00:00:00Z".to_string());
        record.queue_weight = weight;
        record
    }

    fn waiting(
        name: &str,
        requested_at: &str,
        weight: Option<f64>,
    ) -> RunnerCapacityRecordWire {
        let mut record = rec(name);
        record.slot_requested_at = Some(requested_at.to_string());
        record.queue_weight = weight;
        record
    }

    fn snapshot(
        effective_limit: f64,
        records: Vec<RunnerCapacityRecordWire>,
    ) -> RunnerCapacitySnapshotWire {
        runner_capacity_snapshot(&RunnerCapacityRequestWire {
            schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
            effective_limit,
            records,
            now: Some("2026-09-10T00:01:00Z".to_string()),
            deference_seconds_per_step: 5,
            deference_max_seconds: 60,
        })
    }

    #[test]
    fn default_weights_match_serial_and_parallel_lane_counting() {
        let standalone = running("standalone", None);
        let mut serial_root = running("serial-root", Some(2.0));
        serial_root.agent_family = Some("fam".to_string());
        let mut serial_child = running("serial-child", Some(1.0));
        serial_child.agent_family = Some("fam".to_string());
        serial_child.parent_timestamp = Some("serial-root".to_string());
        let mut parallel = running("parallel", Some(0.25));
        parallel.agent_family = Some("fam".to_string());
        parallel.agent_family_parallel = true;
        let mut pending_question = running("question", Some(4.0));
        pending_question.pending_question = true;
        let mut pending_gate = running("gate", Some(4.0));
        pending_gate.agent_family_role = Some("gate".to_string());
        pending_gate.family_shell_kind = Some("gate".to_string());
        pending_gate.family_shell_id = Some("gate-1".to_string());
        pending_gate.family_shell_state = Some("pending".to_string());
        let mut hidden_workflow = running("workflow", Some(4.0));
        hidden_workflow.workflow_dir_name = "workflow-build".to_string();

        let result = snapshot(
            8.0,
            vec![
                standalone,
                serial_root,
                serial_child,
                parallel,
                pending_question,
                pending_gate,
                hidden_workflow,
            ],
        );
        assert_eq!(result.occupied_lanes, 3);
        assert_eq!(result.occupied_capacity, 3.25);
        assert_eq!(
            result
                .claims
                .iter()
                .map(|claim| claim.claim_kind.as_str())
                .collect::<Vec<_>>(),
            ["serial_family", "parallel_member", "standalone"]
        );
    }

    #[test]
    fn lighter_waiter_can_pass_non_fitting_heavy_waiter() {
        let occupied = running("occupied", Some(0.75));
        let heavy = waiting("heavy", "2026-09-10T00:00:00Z", Some(0.5));
        let light = waiting("light", "2026-09-10T00:00:01Z", Some(0.25));

        let result = snapshot(1.0, vec![occupied, heavy, light]);
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/light")
        );
        assert!(!result.waiters[0].eligible);
        assert_eq!(result.waiters[0].blockers[0].code, "insufficient-capacity");
        assert!(result.waiters[1].eligible);
    }

    #[test]
    fn explicit_runner_count_condition_cannot_bypass_capacity() {
        let occupied = running("occupied", Some(1.0));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiter.wait_runners = Some(99);
        waiter.wait_runners_explicit = true;

        let result = snapshot(1.0, vec![occupied, waiter]);
        assert!(result.first_eligible_artifact_dir.is_none());
        assert_eq!(result.waiters[0].blockers[0].code, "insufficient-capacity");
    }

    #[test]
    fn count_condition_blocks_even_when_capacity_fits() {
        let first = running("first", Some(0.25));
        let second = running("second", Some(0.25));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiter.wait_runners = Some(1);
        waiter.wait_runners_explicit = true;

        let result = snapshot(1.0, vec![first, second, waiter]);
        assert!(result.first_eligible_artifact_dir.is_none());
        assert_eq!(
            result.waiters[0].blockers[0].code,
            "runner-count-condition"
        );
        assert_eq!(result.waiters[0].blockers[0].occupied_lanes, Some(2));
    }

    #[test]
    fn compensated_capacity_allows_fractional_boundary() {
        let mut records = Vec::new();
        for index in 0..10 {
            records.push(running(&format!("r{index}"), Some(0.1)));
        }
        records.push(waiting(
            "next",
            "2026-09-10T00:00:00Z",
            Some(f64::MIN_POSITIVE),
        ));

        let result = snapshot(1.0, records);
        assert!(capacity_fits(result.occupied_capacity, 1.0));
        assert!(result.first_eligible_artifact_dir.is_none());
        assert_eq!(result.waiters[0].blockers[0].code, "insufficient-capacity");
    }

    #[test]
    fn invalid_live_claim_and_aggregate_overflow_fail_closed() {
        let mut bad = running("bad", None);
        bad.queue_weight_invalid = true;
        let huge_a = running("huge-a", Some(f64::MAX));
        let huge_b = running("huge-b", Some(f64::MAX));
        let waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));

        let result = snapshot(f64::MAX, vec![bad, huge_a, huge_b, waiter]);
        assert!(result.first_eligible_artifact_dir.is_none());
        assert!(result
            .diagnostics
            .iter()
            .any(|diag| diag.code == "invalid-live-claim-weight"));
        assert!(result
            .diagnostics
            .iter()
            .any(|diag| diag.code == "capacity-overflow"));
        assert_eq!(
            result.waiters[0].blockers[0].code,
            "capacity-snapshot-invalid"
        );
    }

    #[test]
    fn oversized_and_invalid_request_weights_are_blocked() {
        let oversized = waiting("oversized", "2026-09-10T00:00:00Z", Some(2.0));
        let mut invalid = waiting("invalid", "2026-09-10T00:00:01Z", None);
        invalid.queue_weight_invalid = true;

        let result = snapshot(1.0, vec![oversized, invalid]);
        assert!(result.waiters.iter().all(|waiter| !waiter.eligible));
        assert_eq!(result.waiters[0].blockers[0].code, "weight-exceeds-limit");
        assert_eq!(
            result.waiters[1].blockers[0].code,
            "invalid-request-weight"
        );
    }

    #[test]
    fn serial_successor_waits_only_after_family_releases_claim() {
        let mut active = running("root", Some(2.0));
        active.agent_family = Some("fam".to_string());
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("root".to_string());

        let with_claim = snapshot(4.0, vec![active.clone(), successor.clone()]);
        assert!(with_claim.waiters.is_empty());

        active.live = false;
        let released = snapshot(4.0, vec![active, successor]);
        assert_eq!(released.waiters.len(), 1);
        assert!(released.waiters[0].eligible);
    }

    #[test]
    fn deference_blocks_lower_priority_until_window_elapses() {
        let mut pending_better = rec("pending-better");
        pending_better.wait_priority = Some(5);
        let mut lower = waiting("lower", "2026-09-10T00:00:00Z", Some(0.25));
        lower.wait_priority = Some(12);
        lower.eligible_since = Some("2026-09-10T00:00:55Z".to_string());

        let blocked =
            snapshot(1.0, vec![pending_better.clone(), lower.clone()]);
        assert_eq!(blocked.waiters[0].blockers[0].code, "deference-window");

        lower.eligible_since = Some("2026-09-10T00:00:00Z".to_string());
        let satisfied = snapshot(1.0, vec![pending_better, lower]);
        assert!(satisfied.waiters[0].eligible);
    }
}
