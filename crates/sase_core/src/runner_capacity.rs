//! Pure runner-capacity accounting and queue eligibility.
//!
//! Host adapters own locking, filesystem markers, and process liveness. This
//! module owns the deterministic projection from already-collected record facts
//! to occupied capacity, waiters, blockers, and the next admissible waiter.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::queue_directive::{
    queue_capacity_budget_enabled, queue_weight_is_valid, DEFAULT_QUEUE_WEIGHT,
};

pub const RUNNER_CAPACITY_POLICY_SCHEMA_VERSION: u32 = 4;
pub const DEFAULT_WAIT_PRIORITY: i32 = 10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityRequestWire {
    #[serde(default = "runner_capacity_policy_schema_version")]
    pub schema_version: u32,
    pub effective_limit: f64,
    #[serde(default)]
    pub records: Vec<RunnerCapacityRecordWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<RunnerCapacityRecordWire>,
    #[serde(default)]
    pub now: Option<String>,
    #[serde(default)]
    pub deference_seconds_per_step: u32,
    #[serde(default)]
    pub deference_max_seconds: u32,
    #[serde(default)]
    pub feature_flags: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_claim_owner_key: Option<String>,
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
    #[serde(default, alias = "wait_runners")]
    pub queue_capacity: Option<i64>,
    #[serde(default, alias = "wait_runners_explicit")]
    pub queue_capacity_explicit: bool,
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
    pub lineage_key: String,
    pub occupied_lanes: u32,
    pub occupied_capacity: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_artifact_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_timestamp: Option<String>,
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
    pub occupied_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_threshold: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_limit: Option<f64>,
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
    #[serde(
        default,
        alias = "wait_runners",
        skip_serializing_if = "Option::is_none"
    )]
    pub queue_capacity: Option<u32>,
    pub admission_limit: f64,
    pub eligible: bool,
    #[serde(default)]
    pub parked: bool,
    #[serde(default)]
    pub capacity_shortfall: f64,
    #[serde(default)]
    pub wait_capacity_shortfall: f64,
    #[serde(default)]
    pub blockers: Vec<RunnerCapacityBlockerWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityCandidateDecisionWire {
    pub artifact_dir: String,
    pub decision: String,
    pub owner_key: String,
    pub claim_kind: String,
    pub lineage_key: String,
    pub requested_weight: f64,
    pub effective_weight: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_weight: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_claim_weight: Option<f64>,
    pub explicit_weight_compatibility: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_decision: Option<RunnerCapacityCandidateDecisionWire>,
    #[serde(default)]
    pub diagnostics: Vec<RunnerCapacityDiagnosticWire>,
}

struct WaiterEvaluation<'a> {
    request: &'a RunnerCapacityRequestWire,
    record: &'a RunnerCapacityRecordWire,
    claims: &'a [RunnerCapacityClaimWire],
    occupied_capacity: f64,
    requested_weight: f64,
    queue_capacity: Option<u32>,
    admission_limit: f64,
    capacity_budget: bool,
    priority: i32,
    fail_closed: bool,
}

#[derive(Debug, Clone)]
struct ClaimLineage {
    owner_key: String,
    project_name: String,
    claim_kind: String,
    lineage_key: String,
    owner_artifact_dir: Option<String>,
    owner_timestamp: Option<String>,
}

struct ClaimAccumulator {
    lineage: ClaimLineage,
    artifact_dirs: Vec<String>,
    max_weight: f64,
    invalid: bool,
}

struct WaiterDraft<'a> {
    record: &'a RunnerCapacityRecordWire,
    wire: RunnerCapacityWaiterWire,
}

pub fn runner_capacity_policy_schema_version() -> u32 {
    RUNNER_CAPACITY_POLICY_SCHEMA_VERSION
}

pub fn runner_capacity_snapshot(
    request: &RunnerCapacityRequestWire,
) -> RunnerCapacitySnapshotWire {
    let mut diagnostics = Vec::new();
    let claim_records = records_excluding_candidate(request);
    let (mut claims, invalid_live_claim) =
        build_claims(&claim_records, &mut diagnostics);
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
    let fail_closed = invalid_live_claim || capacity_overflow || limit_invalid;
    let waiter_request = request_for_waiters(request, &claim_records, &claims);
    let active_claims = active_claim_keys(&claims);
    let waiters = build_waiters(
        &waiter_request,
        occupied_capacity,
        &claims,
        &active_claims,
        fail_closed,
        &mut diagnostics,
    );
    let first_eligible_artifact_dir = waiters
        .iter()
        .find(|waiter| waiter.eligible)
        .map(|waiter| waiter.artifact_dir.clone());
    let candidate_decision = request.candidate.as_ref().map(|candidate| {
        build_candidate_decision(
            candidate,
            &claim_records,
            &claims,
            &waiters,
            request,
            fail_closed,
        )
    });

    RunnerCapacitySnapshotWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit: request.effective_limit,
        occupied_lanes,
        occupied_capacity,
        claims,
        waiters,
        first_eligible_artifact_dir,
        candidate_decision,
        diagnostics,
    }
}

fn records_excluding_candidate(
    request: &RunnerCapacityRequestWire,
) -> Vec<RunnerCapacityRecordWire> {
    let Some(candidate) = request.candidate.as_ref() else {
        return request.records.clone();
    };
    request
        .records
        .iter()
        .filter(|record| record.artifact_dir != candidate.artifact_dir)
        .cloned()
        .collect()
}

fn request_for_waiters(
    request: &RunnerCapacityRequestWire,
    claim_records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
) -> RunnerCapacityRequestWire {
    let mut records = claim_records.to_vec();
    if let Some(candidate) = request.candidate.as_ref() {
        records.push(candidate_record_with_effective_weight(
            candidate,
            claim_records,
            claims,
        ));
    }
    RunnerCapacityRequestWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit: request.effective_limit,
        records,
        candidate: None,
        now: request.now.clone(),
        deference_seconds_per_step: request.deference_seconds_per_step,
        deference_max_seconds: request.deference_max_seconds,
        feature_flags: request.feature_flags.clone(),
    }
}

fn candidate_record_with_effective_weight(
    candidate: &RunnerCapacityRecordWire,
    records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
) -> RunnerCapacityRecordWire {
    if candidate.queue_weight_explicit || candidate.queue_weight_invalid {
        return candidate.clone();
    }
    let index = record_index(records);
    let lineage = claim_lineage(candidate, &index);
    let inherited = claims
        .iter()
        .find(|claim| claim.owner_key == lineage.owner_key)
        .map(|claim| Ok(Some(claim.occupied_capacity)))
        .unwrap_or_else(|| inherited_lineage_weight(candidate, &index));
    match inherited {
        Ok(Some(weight)) => {
            let mut record = candidate.clone();
            record.queue_weight = Some(weight);
            record
        }
        Ok(None) => candidate.clone(),
        Err(_) => {
            let mut record = candidate.clone();
            record.queue_weight_invalid = true;
            record
        }
    }
}

fn build_claims(
    records: &[RunnerCapacityRecordWire],
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> (Vec<RunnerCapacityClaimWire>, bool) {
    let index = record_index(records);
    let mut groups: BTreeMap<String, ClaimAccumulator> = BTreeMap::new();
    let mut invalid_live_claim = false;
    for record in records {
        if is_occupying_record(record) {
            let lineage = claim_lineage(record, &index);
            let entry =
                groups.entry(lineage.owner_key.clone()).or_insert_with(|| {
                    ClaimAccumulator {
                        lineage,
                        artifact_dirs: Vec::new(),
                        max_weight: 0.0,
                        invalid: false,
                    }
                });
            entry.artifact_dirs.push(record.artifact_dir.clone());
            match effective_weight(record) {
                Ok(weight) => {
                    if weight > entry.max_weight {
                        entry.max_weight = weight;
                    }
                }
                Err(message) => {
                    entry.invalid = true;
                    invalid_live_claim = true;
                    diagnostics.push(diagnostic(
                        "invalid-live-claim-weight",
                        &message,
                        Some(record.artifact_dir.clone()),
                    ));
                }
            }
        }
    }

    let mut claims = Vec::new();
    for (_, mut group) in groups {
        if group.invalid {
            continue;
        }
        group.artifact_dirs.sort();
        claims.push(RunnerCapacityClaimWire {
            owner_key: group.lineage.owner_key,
            project_name: group.lineage.project_name,
            claim_kind: group.lineage.claim_kind,
            lineage_key: group.lineage.lineage_key,
            occupied_lanes: 1,
            occupied_capacity: group.max_weight,
            owner_artifact_dir: group.lineage.owner_artifact_dir,
            owner_timestamp: group.lineage.owner_timestamp,
            artifact_dirs: group.artifact_dirs,
        });
    }
    (claims, invalid_live_claim)
}

fn build_waiters(
    request: &RunnerCapacityRequestWire,
    occupied_capacity: f64,
    claims: &[RunnerCapacityClaimWire],
    active_claims: &BTreeSet<String>,
    fail_closed: bool,
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> Vec<RunnerCapacityWaiterWire> {
    let index = record_index(&request.records);
    let mut candidates: Vec<&RunnerCapacityRecordWire> = request
        .records
        .iter()
        .filter(|record| is_waiting_record(record, active_claims, &index))
        .collect();
    candidates.sort_by(|left, right| compare_waiters(left, right));

    let mut drafts = Vec::new();
    let mut prior_eligible = false;
    let capacity_budget = queue_capacity_budget_enabled(&request.feature_flags);
    for record in candidates {
        let priority = normalize_wait_priority(record.wait_priority);
        let requested_weight =
            effective_weight(record).unwrap_or(DEFAULT_QUEUE_WEIGHT);
        let queue_capacity = explicit_queue_capacity(record);
        let admission_limit = waiter_admission_limit(
            request,
            record,
            requested_weight,
            queue_capacity,
            capacity_budget,
            diagnostics,
        );
        let mut blockers = waiter_blockers(WaiterEvaluation {
            request,
            record,
            claims,
            occupied_capacity,
            requested_weight,
            queue_capacity,
            admission_limit,
            capacity_budget,
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
                    None,
                ));
            } else {
                prior_eligible = true;
            }
        }
        let parked = has_resource_blocker(&blockers);
        let capacity_shortfall = waiter_capacity_shortfall(
            claims,
            requested_weight,
            admission_limit,
        );
        let wait_capacity_shortfall = if capacity_budget {
            0.0
        } else {
            waiter_capacity_condition_shortfall(
                occupied_capacity,
                queue_capacity,
            )
        };
        drafts.push(WaiterDraft {
            record,
            wire: RunnerCapacityWaiterWire {
                artifact_dir: record.artifact_dir.clone(),
                queue_position: 0,
                priority,
                slot_requested_at: record
                    .slot_requested_at
                    .clone()
                    .unwrap_or_default(),
                timestamp: record.timestamp.clone(),
                requested_weight,
                queue_capacity,
                admission_limit,
                eligible: blockers.is_empty(),
                parked,
                capacity_shortfall,
                wait_capacity_shortfall,
                blockers,
            },
        });
    }
    drafts.sort_by(compare_waiter_drafts);
    drafts
        .into_iter()
        .enumerate()
        .map(|(index, mut draft)| {
            draft.wire.queue_position = (index + 1) as u32;
            draft.wire
        })
        .collect()
}

fn waiter_admission_limit(
    request: &RunnerCapacityRequestWire,
    record: &RunnerCapacityRecordWire,
    requested_weight: f64,
    queue_capacity: Option<u32>,
    capacity_budget: bool,
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> f64 {
    if !capacity_budget || !record.queue_capacity_explicit {
        return request.effective_limit;
    }
    match record.queue_capacity {
        Some(0) => {
            diagnostics.push(diagnostic(
                "legacy-capacity-zero",
                "Persisted queue_capacity=0 was translated to this waiter's effective weight so it drains to zero before admission.",
                Some(record.artifact_dir.clone()),
            ));
            requested_weight
        }
        Some(_) => queue_capacity
            .map(f64::from)
            .unwrap_or(request.effective_limit),
        None => request.effective_limit,
    }
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
            None,
        ));
    }
    if !queue_weight_is_valid(eval.admission_limit) {
        blockers.push(blocker(
            "invalid-capacity-limit",
            "The effective runner capacity limit must be positive and finite.",
            None,
            None,
            None,
            None,
            None,
        ));
    } else {
        let free_capacity =
            (eval.admission_limit - eval.occupied_capacity).max(0.0);
        if !capacity_fits(eval.requested_weight, eval.admission_limit) {
            blockers.push(blocker(
                "weight-exceeds-limit",
                "Requested capacity weight exceeds this waiter's admission limit.",
                Some(eval.requested_weight),
                Some(free_capacity),
                None,
                None,
                Some(eval.admission_limit),
            ));
        } else {
            let proposed = compensated_sum(
                eval.claims
                    .iter()
                    .map(|claim| claim.occupied_capacity)
                    .chain(std::iter::once(eval.requested_weight)),
            );
            if proposed.is_none() {
                blockers.push(blocker(
                    "capacity-overflow",
                    "Runner capacity cannot admit this waiter because the proposed total overflows.",
                    Some(eval.requested_weight),
                    Some(free_capacity),
                    None,
                    None,
                    Some(eval.admission_limit),
                ));
            } else if !capacity_fits(
                proposed.unwrap_or(f64::MAX),
                eval.admission_limit,
            ) {
                blockers.push(blocker(
                    "insufficient-capacity",
                    "Insufficient free runner capacity for this waiter.",
                    Some(eval.requested_weight),
                    Some(free_capacity),
                    None,
                    None,
                    Some(eval.admission_limit),
                ));
            }
        }
    }
    if !eval.capacity_budget {
        if let Some(threshold) = eval.queue_capacity {
            if occupied_capacity_exceeds_threshold(
                eval.occupied_capacity,
                threshold,
            ) {
                let message = if threshold == 0 {
                    "Occupied weighted load must drain to zero before this capacity-0 waiter can start."
                } else {
                    "Occupied weighted load exceeds this explicit capacity threshold."
                };
                blockers.push(blocker(
                    "capacity-condition",
                    message,
                    None,
                    None,
                    Some(eval.occupied_capacity),
                    Some(threshold),
                    Some(eval.admission_limit),
                ));
            }
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
            None,
        ));
    }
    blockers
}

fn build_candidate_decision(
    candidate: &RunnerCapacityRecordWire,
    records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
    waiters: &[RunnerCapacityWaiterWire],
    request: &RunnerCapacityRequestWire,
    fail_closed: bool,
) -> RunnerCapacityCandidateDecisionWire {
    let index = record_index(records);
    let lineage = claim_lineage(candidate, &index);
    let requested = effective_weight(candidate);
    let active_claim = claims
        .iter()
        .find(|claim| claim.owner_key == lineage.owner_key);
    let inherited_result = active_claim
        .map(|claim| Ok(Some(claim.occupied_capacity)))
        .unwrap_or_else(|| inherited_lineage_weight(candidate, &index));
    let inherited_error = inherited_result.as_ref().err().cloned();
    let inherited_weight = inherited_result.ok().flatten();
    let requested_weight =
        requested.as_ref().copied().unwrap_or(DEFAULT_QUEUE_WEIGHT);
    let effective_weight = if candidate.queue_weight_explicit {
        requested_weight
    } else {
        inherited_weight.unwrap_or(requested_weight)
    };
    let mut blockers = Vec::new();
    let explicit_weight_compatibility;
    let decision;
    let eligible;

    if let Err(message) = requested.as_ref() {
        blockers.push(blocker(
            "invalid-request-weight",
            message,
            None,
            None,
            None,
            None,
            None,
        ));
        explicit_weight_compatibility = "invalid".to_string();
        decision = "invalid".to_string();
        eligible = false;
    } else if let Some(message) = inherited_error {
        blockers.push(blocker(
            "invalid-inherited-weight",
            &message,
            None,
            None,
            None,
            None,
            None,
        ));
        explicit_weight_compatibility = "invalid-inherited-lineage".to_string();
        decision = "invalid".to_string();
        eligible = false;
    } else if let Some(claim) = active_claim {
        let active_weight = claim.occupied_capacity;
        if candidate.queue_weight_explicit
            && !weights_equal(requested_weight, active_weight)
        {
            blockers.push(blocker(
                "active-claim-weight-conflict",
                "Serial continuation requested a different explicit weight than its active lineage claim.",
                Some(requested_weight),
                None,
                None,
                None,
                None,
            ));
            explicit_weight_compatibility = "conflict".to_string();
            decision = "invalid".to_string();
            eligible = false;
        } else {
            explicit_weight_compatibility = if candidate.queue_weight_explicit {
                "compatible".to_string()
            } else {
                "inherited-active-claim".to_string()
            };
            decision = "reuse_existing_claim".to_string();
            eligible = true;
        }
    } else {
        let waiter = waiters
            .iter()
            .find(|waiter| waiter.artifact_dir == candidate.artifact_dir);
        if let Some(waiter) = waiter {
            blockers = waiter.blockers.clone();
            eligible = waiter.eligible;
        } else if fail_closed || !is_user_agent_record(candidate) {
            blockers.push(blocker(
                "capacity-snapshot-invalid",
                "Runner capacity cannot admit this candidate from the current snapshot.",
                None,
                None,
                None,
                None,
                None,
            ));
            eligible = false;
        } else {
            eligible = false;
        }
        explicit_weight_compatibility = if candidate.queue_weight_explicit {
            "authored".to_string()
        } else if inherited_weight.is_some() {
            "inherited-released-lineage".to_string()
        } else {
            "default".to_string()
        };
        decision = if eligible {
            "acquire_capacity".to_string()
        } else if blockers.iter().any(|blocker| {
            matches!(
                blocker.code.as_str(),
                "invalid-request-weight"
                    | "invalid-capacity-limit"
                    | "capacity-snapshot-invalid"
                    | "capacity-overflow"
            )
        }) || !queue_weight_is_valid(request.effective_limit)
        {
            "invalid".to_string()
        } else {
            "blocked".to_string()
        };
    }

    RunnerCapacityCandidateDecisionWire {
        artifact_dir: candidate.artifact_dir.clone(),
        decision,
        owner_key: lineage.owner_key,
        claim_kind: lineage.claim_kind,
        lineage_key: lineage.lineage_key,
        requested_weight,
        effective_weight,
        inherited_weight,
        active_claim_weight: active_claim.map(|claim| claim.occupied_capacity),
        explicit_weight_compatibility,
        eligible,
        blockers,
    }
}

fn compare_waiter_drafts(
    left: &WaiterDraft<'_>,
    right: &WaiterDraft<'_>,
) -> Ordering {
    waiter_display_bucket(&left.wire)
        .cmp(&waiter_display_bucket(&right.wire))
        .then_with(|| {
            if left.wire.parked || right.wire.parked {
                compare_f64(
                    left.wire.capacity_shortfall,
                    right.wire.capacity_shortfall,
                )
                .then_with(|| {
                    compare_f64(
                        left.wire.wait_capacity_shortfall,
                        right.wire.wait_capacity_shortfall,
                    )
                })
            } else {
                Ordering::Equal
            }
        })
        .then_with(|| compare_waiters(left.record, right.record))
}

fn waiter_display_bucket(waiter: &RunnerCapacityWaiterWire) -> u8 {
    if waiter.parked {
        2
    } else if waiter.eligible {
        0
    } else {
        1
    }
}

fn compare_f64(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

fn has_resource_blocker(blockers: &[RunnerCapacityBlockerWire]) -> bool {
    blockers.iter().any(|blocker| {
        matches!(
            blocker.code.as_str(),
            "capacity-snapshot-invalid"
                | "invalid-request-weight"
                | "invalid-capacity-limit"
                | "weight-exceeds-limit"
                | "insufficient-capacity"
                | "capacity-overflow"
                | "capacity-condition"
        )
    })
}

fn waiter_capacity_shortfall(
    claims: &[RunnerCapacityClaimWire],
    requested_weight: f64,
    limit: f64,
) -> f64 {
    if !queue_weight_is_valid(limit) {
        return f64::MAX;
    }
    let Some(proposed) = compensated_sum(
        claims
            .iter()
            .map(|claim| claim.occupied_capacity)
            .chain(std::iter::once(requested_weight)),
    ) else {
        return f64::MAX;
    };
    if capacity_fits(proposed, limit) {
        0.0
    } else {
        (proposed - limit).max(0.0)
    }
}

fn waiter_capacity_condition_shortfall(
    occupied_capacity: f64,
    wait_capacity: Option<u32>,
) -> f64 {
    let Some(threshold) = wait_capacity else {
        return 0.0;
    };
    if !occupied_capacity_exceeds_threshold(occupied_capacity, threshold) {
        return 0.0;
    }
    if threshold == 0 {
        occupied_capacity.max(0.0)
    } else {
        (occupied_capacity - f64::from(threshold)).max(0.0)
    }
}

fn occupied_capacity_exceeds_threshold(
    occupied_capacity: f64,
    threshold: u32,
) -> bool {
    if !occupied_capacity.is_finite() {
        return true;
    }
    if threshold == 0 {
        occupied_capacity > 0.0
    } else {
        !capacity_fits(occupied_capacity, f64::from(threshold))
    }
}

fn weights_equal(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return false;
    }
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= 4.0 * ulp_at(scale)
}

fn active_claim_keys(claims: &[RunnerCapacityClaimWire]) -> BTreeSet<String> {
    claims.iter().map(|claim| claim.owner_key.clone()).collect()
}

fn record_index(
    records: &[RunnerCapacityRecordWire],
) -> BTreeMap<(String, String), &RunnerCapacityRecordWire> {
    let mut index = BTreeMap::new();
    for record in records {
        index
            .entry((record.project_name.clone(), record.timestamp.clone()))
            .or_insert(record);
    }
    index
}

fn claim_lineage(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> ClaimLineage {
    let mut visiting = BTreeSet::new();
    claim_lineage_inner(record, index, &mut visiting)
}

fn claim_lineage_inner(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
    visiting: &mut BTreeSet<(String, String)>,
) -> ClaimLineage {
    if let Some(owner) =
        normalized_owner_key(record.runner_claim_owner_key.as_deref())
    {
        return explicit_claim_lineage(record, owner);
    }
    if record.agent_family_parallel {
        return parallel_claim_lineage(record);
    }
    if let Some(parent_timestamp) =
        normalized_owner_key(record.parent_timestamp.as_deref())
    {
        let key = (record.project_name.clone(), parent_timestamp.clone());
        if visiting
            .insert((record.project_name.clone(), record.timestamp.clone()))
        {
            if let Some(parent) = index.get(&key) {
                return claim_lineage_inner(parent, index, visiting);
            }
        }
    }
    serial_or_standalone_claim_lineage(record)
}

fn normalized_owner_key(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn explicit_claim_lineage(
    record: &RunnerCapacityRecordWire,
    owner: String,
) -> ClaimLineage {
    let claim_kind = if record.agent_family_parallel {
        "parallel_member"
    } else if record.parent_timestamp.is_some()
        || record
            .agent_family
            .as_deref()
            .is_some_and(|value| !value.is_empty())
    {
        "serial_family"
    } else {
        "standalone"
    };
    ClaimLineage {
        owner_key: format!("{}:{owner}", record.project_name),
        project_name: record.project_name.clone(),
        claim_kind: claim_kind.to_string(),
        lineage_key: owner,
        owner_artifact_dir: Some(record.artifact_dir.clone()),
        owner_timestamp: Some(record.timestamp.clone()),
    }
}

fn parallel_claim_lineage(record: &RunnerCapacityRecordWire) -> ClaimLineage {
    let family = record
        .agent_family
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("standalone");
    let lineage_key = format!("{family}:parallel:{}", record.timestamp);
    ClaimLineage {
        owner_key: format!("{}:{lineage_key}", record.project_name),
        project_name: record.project_name.clone(),
        claim_kind: "parallel_member".to_string(),
        lineage_key,
        owner_artifact_dir: Some(record.artifact_dir.clone()),
        owner_timestamp: Some(record.timestamp.clone()),
    }
}

fn serial_or_standalone_claim_lineage(
    record: &RunnerCapacityRecordWire,
) -> ClaimLineage {
    if let Some(family) = record
        .agent_family
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return ClaimLineage {
            owner_key: format!("{}:{family}", record.project_name),
            project_name: record.project_name.clone(),
            claim_kind: "serial_family".to_string(),
            lineage_key: family.to_string(),
            owner_artifact_dir: Some(record.artifact_dir.clone()),
            owner_timestamp: Some(record.timestamp.clone()),
        };
    }
    ClaimLineage {
        owner_key: format!("{}:{}", record.project_name, record.timestamp),
        project_name: record.project_name.clone(),
        claim_kind: "standalone".to_string(),
        lineage_key: record.timestamp.clone(),
        owner_artifact_dir: Some(record.artifact_dir.clone()),
        owner_timestamp: Some(record.timestamp.clone()),
    }
}

fn inherited_lineage_weight(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> Result<Option<f64>, String> {
    if let Some(parent_timestamp) =
        normalized_owner_key(record.parent_timestamp.as_deref())
    {
        if let Some(parent) =
            index.get(&(record.project_name.clone(), parent_timestamp))
        {
            return lineage_record_weight(parent, index).map(Some);
        }
    }
    let Some(owner) =
        normalized_owner_key(record.runner_claim_owner_key.as_deref())
    else {
        return Ok(None);
    };
    let owner_key = format!("{}:{owner}", record.project_name);
    let mut inherited: Option<f64> = None;
    for other in index.values() {
        if claim_lineage(other, index).owner_key == owner_key {
            let weight = lineage_record_weight(other, index)?;
            inherited =
                Some(inherited.map_or(weight, |current| current.max(weight)));
        }
    }
    Ok(inherited)
}

fn lineage_record_weight(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> Result<f64, String> {
    let mut visiting = BTreeSet::new();
    lineage_record_weight_inner(record, index, &mut visiting)
}

fn lineage_record_weight_inner(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
    visiting: &mut BTreeSet<(String, String)>,
) -> Result<f64, String> {
    if !record.queue_weight_explicit {
        if let Some(parent_timestamp) =
            normalized_owner_key(record.parent_timestamp.as_deref())
        {
            if let Some(parent) =
                index.get(&(record.project_name.clone(), parent_timestamp))
            {
                let key =
                    (record.project_name.clone(), record.timestamp.clone());
                if visiting.insert(key) {
                    return lineage_record_weight_inner(
                        parent, index, visiting,
                    );
                }
            }
        }
    }
    effective_weight(record)
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
    active_claims: &BTreeSet<String>,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
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
    if serial_continuation_reuses_claim(record) {
        let owner_key = claim_lineage(record, index).owner_key;
        return !active_claims.contains(&owner_key);
    }
    true
}

fn serial_continuation_reuses_claim(record: &RunnerCapacityRecordWire) -> bool {
    !record.agent_family_parallel
        && (record
            .parent_timestamp
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || record
                .runner_claim_owner_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()))
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

fn explicit_queue_capacity(record: &RunnerCapacityRecordWire) -> Option<u32> {
    if !record.queue_capacity_explicit {
        return None;
    }
    let runners = record.queue_capacity?;
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
    occupied_capacity: Option<f64>,
    capacity_threshold: Option<u32>,
    admission_limit: Option<f64>,
) -> RunnerCapacityBlockerWire {
    RunnerCapacityBlockerWire {
        code: code.to_string(),
        message: message.to_string(),
        needed_capacity,
        free_capacity,
        occupied_capacity,
        capacity_threshold,
        admission_limit,
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
            runner_claim_owner_key: None,
            family_shell_kind: None,
            family_shell_id: None,
            family_shell_state: None,
            queue_weight: None,
            queue_weight_explicit: false,
            queue_weight_invalid: false,
            slot_requested_at: None,
            queue_capacity: None,
            queue_capacity_explicit: false,
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
        snapshot_with_flags(effective_limit, records, &[])
    }

    fn snapshot_with_flags(
        effective_limit: f64,
        records: Vec<RunnerCapacityRecordWire>,
        feature_flags: &[String],
    ) -> RunnerCapacitySnapshotWire {
        runner_capacity_snapshot(&RunnerCapacityRequestWire {
            schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
            effective_limit,
            records,
            candidate: None,
            now: Some("2026-09-10T00:01:00Z".to_string()),
            deference_seconds_per_step: 5,
            deference_max_seconds: 60,
            feature_flags: feature_flags.to_vec(),
        })
    }

    fn snapshot_with_candidate(
        effective_limit: f64,
        records: Vec<RunnerCapacityRecordWire>,
        candidate: RunnerCapacityRecordWire,
    ) -> RunnerCapacitySnapshotWire {
        runner_capacity_snapshot(&RunnerCapacityRequestWire {
            schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
            effective_limit,
            records,
            candidate: Some(candidate),
            now: Some("2026-09-10T00:01:00Z".to_string()),
            deference_seconds_per_step: 5,
            deference_max_seconds: 60,
            feature_flags: Vec::new(),
        })
    }

    fn capacity_budget_flags() -> Vec<String> {
        vec!["queue_capacity_budget".to_string()]
    }

    fn waiter<'a>(
        snapshot: &'a RunnerCapacitySnapshotWire,
        name: &str,
    ) -> &'a RunnerCapacityWaiterWire {
        let artifact_dir = format!("/tmp/{name}");
        snapshot
            .waiters
            .iter()
            .find(|waiter| waiter.artifact_dir == artifact_dir)
            .expect("waiter should be present")
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
        assert_eq!(
            result
                .waiters
                .iter()
                .map(|waiter| waiter.artifact_dir.as_str())
                .collect::<Vec<_>>(),
            ["/tmp/light", "/tmp/heavy"]
        );
        assert_eq!(
            waiter(&result, "heavy").blockers[0].code,
            "insufficient-capacity"
        );
        assert!(waiter(&result, "light").eligible);
    }

    #[test]
    fn explicit_capacity_cannot_bypass_global_budget() {
        let occupied = running("occupied", Some(1.0));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiter.queue_capacity = Some(99);
        waiter.queue_capacity_explicit = true;

        let result = snapshot(1.0, vec![occupied, waiter]);
        assert!(result.first_eligible_artifact_dir.is_none());
        assert_eq!(result.waiters[0].blockers[0].code, "insufficient-capacity");
        assert_eq!(result.waiters[0].wait_capacity_shortfall, 0.0);
    }

    #[test]
    fn capacity_budget_can_exceed_global_limit() {
        let flags = capacity_budget_flags();
        let occupied = running("occupied", Some(1.0));
        let mut waiting_agent =
            waiting("waiter", "2026-09-10T00:00:00Z", Some(1.0));
        waiting_agent.queue_capacity = Some(100);
        waiting_agent.queue_capacity_explicit = true;

        let result =
            snapshot_with_flags(1.0, vec![occupied, waiting_agent], &flags);
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/waiter")
        );
        assert_eq!(result.occupied_capacity, 1.0);
        assert_eq!(result.effective_limit, 1.0);
        let waiter = waiter(&result, "waiter");
        assert!(waiter.eligible);
        assert_eq!(waiter.admission_limit, 100.0);
        assert_eq!(waiter.capacity_shortfall, 0.0);
        assert_eq!(waiter.wait_capacity_shortfall, 0.0);
    }

    #[test]
    fn capacity_budget_below_global_limit_is_strict() {
        let flags = capacity_budget_flags();
        let occupied = running("occupied", Some(1.0));
        let mut waiting_agent =
            waiting("waiter", "2026-09-10T00:00:00Z", Some(1.0));
        waiting_agent.queue_capacity = Some(1);
        waiting_agent.queue_capacity_explicit = true;

        let result =
            snapshot_with_flags(8.0, vec![occupied, waiting_agent], &flags);
        assert!(result.first_eligible_artifact_dir.is_none());
        let waiter = waiter(&result, "waiter");
        assert!(!waiter.eligible);
        assert_eq!(waiter.admission_limit, 1.0);
        assert_eq!(waiter.capacity_shortfall, 1.0);
        assert_eq!(waiter.wait_capacity_shortfall, 0.0);
        assert_eq!(waiter.blockers[0].code, "insufficient-capacity");
        assert_eq!(waiter.blockers[0].admission_limit, Some(1.0));
    }

    #[test]
    fn four_light_claims_satisfy_capacity_one() {
        let mut records: Vec<RunnerCapacityRecordWire> = (0..4)
            .map(|index| running(&format!("light{index}"), Some(0.25)))
            .collect();
        let mut waiting_agent =
            waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiting_agent.queue_capacity = Some(1);
        waiting_agent.queue_capacity_explicit = true;
        records.push(waiting_agent);

        let result = snapshot(8.0, records);
        assert_eq!(result.occupied_capacity, 1.0);
        assert_eq!(result.occupied_lanes, 4);
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/waiter")
        );
        assert!(waiter(&result, "waiter").eligible);
        assert_eq!(waiter(&result, "waiter").wait_capacity_shortfall, 0.0);
    }

    #[test]
    fn one_heavy_claim_exceeds_capacity_one() {
        let occupied = running("heavy", Some(2.0));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiter.queue_capacity = Some(1);
        waiter.queue_capacity_explicit = true;

        let result = snapshot(8.0, vec![occupied, waiter]);
        assert!(result.first_eligible_artifact_dir.is_none());
        assert_eq!(result.waiters[0].blockers[0].code, "capacity-condition");
        assert_eq!(result.waiters[0].blockers[0].occupied_capacity, Some(2.0));
        assert_eq!(result.waiters[0].blockers[0].capacity_threshold, Some(1));
        assert_eq!(result.waiters[0].wait_capacity_shortfall, 1.0);
    }

    #[test]
    fn capacity_zero_is_true_drain_including_tiny_weight() {
        let occupied = running("tiny", Some(f64::MIN_POSITIVE));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiter.queue_capacity = Some(0);
        waiter.queue_capacity_explicit = true;

        let blocked = snapshot(8.0, vec![occupied, waiter.clone()]);
        assert!(blocked.first_eligible_artifact_dir.is_none());
        assert_eq!(blocked.waiters[0].blockers[0].code, "capacity-condition");
        assert!(blocked.waiters[0].wait_capacity_shortfall > 0.0);

        occupied_drain_admits(waiter);
    }

    #[test]
    fn capacity_budget_one_is_drain_barrier() {
        let flags = capacity_budget_flags();
        let occupied = running("occupied", Some(0.25));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", None);
        waiter.queue_capacity = Some(1);
        waiter.queue_capacity_explicit = true;

        let blocked =
            snapshot_with_flags(8.0, vec![occupied, waiter.clone()], &flags);
        assert!(blocked.first_eligible_artifact_dir.is_none());
        assert_eq!(
            blocked.waiters[0].blockers[0].code,
            "insufficient-capacity"
        );
        assert_eq!(blocked.waiters[0].admission_limit, 1.0);

        let drained = snapshot_with_flags(8.0, vec![waiter], &flags);
        assert_eq!(
            drained.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/waiter")
        );
        assert!(drained.waiters[0].eligible);
    }

    #[test]
    fn legacy_capacity_zero_translates_under_capacity_budget() {
        let flags = capacity_budget_flags();
        let occupied = running("occupied", Some(0.1));
        let mut waiter = waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiter.queue_capacity = Some(0);
        waiter.queue_capacity_explicit = true;

        let blocked =
            snapshot_with_flags(8.0, vec![occupied, waiter.clone()], &flags);
        assert!(blocked.first_eligible_artifact_dir.is_none());
        assert_eq!(blocked.waiters[0].admission_limit, 0.25);
        assert_eq!(
            blocked.waiters[0].blockers[0].code,
            "insufficient-capacity"
        );
        assert_eq!(blocked.waiters[0].wait_capacity_shortfall, 0.0);
        assert!(blocked
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "legacy-capacity-zero"));

        let drained = snapshot_with_flags(8.0, vec![waiter], &flags);
        assert_eq!(
            drained.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/waiter")
        );
        assert!(drained.waiters[0].eligible);
        assert_eq!(drained.waiters[0].admission_limit, 0.25);
    }

    fn occupied_drain_admits(mut waiter: RunnerCapacityRecordWire) {
        waiter.queue_capacity = Some(0);
        waiter.queue_capacity_explicit = true;
        let drained = snapshot(8.0, vec![waiter]);
        assert_eq!(
            drained.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/waiter")
        );
        assert!(drained.waiters[0].eligible);
        assert_eq!(drained.waiters[0].wait_capacity_shortfall, 0.0);
    }

    #[test]
    fn legacy_wait_runners_aliases_deserialize_to_queue_capacity() {
        let record: RunnerCapacityRecordWire =
            serde_json::from_value(serde_json::json!({
                "artifact_dir": "/tmp/waiter",
                "project_name": "proj",
                "timestamp": "waiter",
                "wait_runners": 3,
                "wait_runners_explicit": true
            }))
            .unwrap();
        assert_eq!(record.queue_capacity, Some(3));
        assert!(record.queue_capacity_explicit);
        let encoded = serde_json::to_value(&record).unwrap();
        assert_eq!(encoded["queue_capacity"], serde_json::json!(3));
        assert!(encoded.get("wait_runners").is_none());

        let waiter: RunnerCapacityWaiterWire =
            serde_json::from_value(serde_json::json!({
                "artifact_dir": "/tmp/waiter",
                "queue_position": 1,
                "priority": 10,
                "slot_requested_at": "2026-09-10T00:00:00Z",
                "timestamp": "waiter",
                "requested_weight": 1.0,
                "wait_runners": 3,
                "admission_limit": 3.0,
                "eligible": false
            }))
            .unwrap();
        assert_eq!(waiter.queue_capacity, Some(3));
    }

    #[test]
    fn shared_family_claim_counts_once_for_capacity() {
        let mut serial_root = running("serial-root", Some(2.0));
        serial_root.agent_family = Some("fam".to_string());
        let mut serial_child = running("serial-child", Some(1.0));
        serial_child.agent_family = Some("fam".to_string());
        serial_child.parent_timestamp = Some("serial-root".to_string());
        let mut waiting_agent =
            waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
        waiting_agent.queue_capacity = Some(2);
        waiting_agent.queue_capacity_explicit = true;

        let result =
            snapshot(8.0, vec![serial_root, serial_child, waiting_agent]);
        assert_eq!(result.occupied_lanes, 1);
        assert_eq!(result.occupied_capacity, 2.0);
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/waiter")
        );
        assert!(waiter(&result, "waiter").eligible);
    }

    #[test]
    fn proposed_total_allows_repeated_decimal_boundary() {
        let mut records = Vec::new();
        for index in 0..99 {
            records.push(running(&format!("r{index}"), Some(0.1)));
        }
        records.push(waiting("next", "2026-09-10T00:00:00Z", Some(0.1)));

        let result = snapshot(10.0, records);
        assert!(capacity_fits(result.occupied_capacity, 10.0));
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/next")
        );
        assert!(waiter(&result, "next").eligible);
    }

    #[test]
    fn heterogeneous_and_meaningful_boundaries_use_proposed_total() {
        let result = snapshot(
            1.0,
            vec![
                running("a", Some(0.1)),
                running("b", Some(0.2)),
                running("c", Some(0.3)),
                waiting("fits", "2026-09-10T00:00:00Z", Some(0.4)),
            ],
        );
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/fits")
        );

        let under = snapshot(
            1.0,
            vec![
                running("held", Some(0.6)),
                waiting("under", "2026-09-10T00:00:00Z", Some(0.399999999999)),
            ],
        );
        assert!(waiter(&under, "under").eligible);

        let over = snapshot(
            1.0,
            vec![
                running("held", Some(0.6)),
                waiting("over", "2026-09-10T00:00:00Z", Some(0.400000000001)),
            ],
        );
        assert_eq!(
            waiter(&over, "over").blockers[0].code,
            "insufficient-capacity"
        );
    }

    #[test]
    fn tiny_positive_weights_are_valid_but_still_accounted() {
        let tiny = snapshot(
            f64::MIN_POSITIVE,
            vec![waiting(
                "tiny",
                "2026-09-10T00:00:00Z",
                Some(f64::MIN_POSITIVE),
            )],
        );
        assert!(waiter(&tiny, "tiny").eligible);

        let over = snapshot(
            f64::MIN_POSITIVE,
            vec![
                running("held", Some(f64::MIN_POSITIVE)),
                waiting(
                    "tiny",
                    "2026-09-10T00:00:00Z",
                    Some(f64::MIN_POSITIVE),
                ),
            ],
        );
        assert_eq!(
            waiter(&over, "tiny").blockers[0].code,
            "insufficient-capacity"
        );
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
        assert_eq!(
            waiter(&result, "oversized").blockers[0].code,
            "weight-exceeds-limit"
        );
        assert_eq!(
            waiter(&result, "invalid").blockers[0].code,
            "invalid-request-weight"
        );
    }

    #[test]
    fn proposed_waiter_overflow_fails_closed() {
        let result = snapshot(
            f64::MAX,
            vec![
                running("held", Some(f64::MAX)),
                waiting("waiter", "2026-09-10T00:00:00Z", Some(f64::MAX)),
            ],
        );
        assert_eq!(
            waiter(&result, "waiter").blockers[0].code,
            "capacity-overflow"
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
    fn serial_successor_of_parallel_member_shares_parallel_lineage_claim() {
        let mut parallel = running("parallel", Some(2.0));
        parallel.agent_family = Some("fam".to_string());
        parallel.agent_family_parallel = true;
        let mut successor = running("successor", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("parallel".to_string());

        let result = snapshot(4.0, vec![parallel, successor]);
        assert_eq!(result.occupied_lanes, 1);
        assert_eq!(result.occupied_capacity, 2.0);
        assert_eq!(result.claims.len(), 1);
        assert_eq!(result.claims[0].claim_kind, "parallel_member");
        assert_eq!(
            result.claims[0].artifact_dirs,
            ["/tmp/parallel", "/tmp/successor"]
        );
    }

    #[test]
    fn unrelated_same_display_family_claim_does_not_hide_parallel_successor() {
        let mut serial_branch = running("serial-branch", Some(2.0));
        serial_branch.agent_family = Some("fam".to_string());
        let mut parallel_parent = running("parallel-parent", Some(2.0));
        parallel_parent.agent_family = Some("fam".to_string());
        parallel_parent.agent_family_parallel = true;
        parallel_parent.live = false;
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("parallel-parent".to_string());

        let result =
            snapshot(4.0, vec![serial_branch, parallel_parent, successor]);
        assert_eq!(
            result.first_eligible_artifact_dir.as_deref(),
            Some("/tmp/successor")
        );
        assert_eq!(result.waiters.len(), 1);
    }

    #[test]
    fn explicit_claim_owner_separates_same_family_branches() {
        let mut left = running("left", Some(1.0));
        left.agent_family = Some("fam".to_string());
        left.runner_claim_owner_key = Some("branch-left".to_string());
        let mut right = running("right", Some(1.0));
        right.agent_family = Some("fam".to_string());
        right.runner_claim_owner_key = Some("branch-right".to_string());

        let result = snapshot(4.0, vec![left, right]);
        assert_eq!(result.occupied_lanes, 2);
        assert_eq!(result.occupied_capacity, 2.0);
        assert_eq!(
            result
                .claims
                .iter()
                .map(|claim| claim.owner_key.as_str())
                .collect::<Vec<_>>(),
            ["proj:branch-left", "proj:branch-right"]
        );
    }

    #[test]
    fn candidate_decision_reuses_active_lineage_or_rejects_reweight() {
        let mut active = running("root", Some(2.0));
        active.agent_family = Some("fam".to_string());
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(1.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("root".to_string());

        let inherited = snapshot_with_candidate(
            2.0,
            vec![active.clone()],
            successor.clone(),
        );
        let decision = inherited.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "reuse_existing_claim");
        assert_eq!(decision.effective_weight, 2.0);
        assert_eq!(decision.inherited_weight, Some(2.0));
        assert_eq!(
            decision.explicit_weight_compatibility,
            "inherited-active-claim"
        );

        successor.queue_weight = Some(3.0);
        successor.queue_weight_explicit = true;
        let conflict = snapshot_with_candidate(2.0, vec![active], successor);
        let decision = conflict.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "invalid");
        assert_eq!(decision.explicit_weight_compatibility, "conflict");
        assert_eq!(decision.blockers[0].code, "active-claim-weight-conflict");
    }

    #[test]
    fn candidate_decision_excludes_unadmitted_candidate_from_own_claim() {
        let unrelated = running("unrelated", Some(2.0));
        let mut candidate =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        candidate.agent_family = Some("fam".to_string());
        candidate.parent_timestamp = Some("released-parent".to_string());
        candidate.run_started_at = Some("2026-09-10T00:00:01Z".to_string());

        let result = snapshot_with_candidate(
            2.0,
            vec![unrelated, candidate.clone()],
            candidate,
        );
        assert_eq!(result.occupied_lanes, 1);
        assert_eq!(result.occupied_capacity, 2.0);
        let decision = result.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "blocked");
        assert_eq!(decision.blockers[0].code, "insufficient-capacity");
    }

    #[test]
    fn released_lineage_inherits_weight_unless_candidate_authors_new_weight() {
        let mut parent = running("parent", Some(2.0));
        parent.live = false;
        let mut successor = waiting("successor", "2026-09-10T00:00:00Z", None);
        successor.parent_timestamp = Some("parent".to_string());

        let inherited = snapshot_with_candidate(
            2.0,
            vec![parent.clone()],
            successor.clone(),
        );
        let decision = inherited.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "acquire_capacity");
        assert_eq!(decision.effective_weight, 2.0);
        assert_eq!(
            decision.explicit_weight_compatibility,
            "inherited-released-lineage"
        );

        successor.queue_weight = Some(0.5);
        successor.queue_weight_explicit = true;
        let authored = snapshot_with_candidate(2.0, vec![parent], successor);
        let decision = authored.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "acquire_capacity");
        assert_eq!(decision.effective_weight, 0.5);
        assert_eq!(decision.explicit_weight_compatibility, "authored");
    }

    #[test]
    fn invalid_released_lineage_weight_blocks_omitted_candidate() {
        let mut parent = running("parent", None);
        parent.live = false;
        parent.queue_weight_invalid = true;
        let mut successor = waiting("successor", "2026-09-10T00:00:00Z", None);
        successor.parent_timestamp = Some("parent".to_string());

        let result = snapshot_with_candidate(2.0, vec![parent], successor);
        let decision = result.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "invalid");
        assert_eq!(
            decision.explicit_weight_compatibility,
            "invalid-inherited-lineage"
        );
        assert_eq!(decision.blockers[0].code, "invalid-inherited-weight");
        assert_eq!(
            waiter(&result, "successor").blockers[0].code,
            "invalid-request-weight"
        );
    }

    #[test]
    fn nested_monitor_successor_reuses_starter_lineage() {
        let mut starter = running("starter", Some(2.0));
        starter.agent_family = Some("fam".to_string());
        let mut monitor = running("monitor", Some(2.0));
        monitor.agent_family = Some("fam".to_string());
        monitor.agent_family_role = Some("monitor".to_string());
        monitor.family_shell_kind = Some("monitor".to_string());
        monitor.family_shell_id = Some("mon-1".to_string());
        monitor.parent_timestamp = Some("starter".to_string());
        monitor.pid = Some(99);
        monitor.run_started_at = None;
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("monitor".to_string());

        let live = snapshot(
            4.0,
            vec![starter.clone(), monitor.clone(), successor.clone()],
        );
        assert_eq!(live.occupied_lanes, 1);
        assert_eq!(live.occupied_capacity, 2.0);
        assert!(live.waiters.is_empty());

        let decision =
            snapshot_with_candidate(4.0, vec![starter, monitor], successor)
                .candidate_decision
                .unwrap();
        assert_eq!(decision.decision, "reuse_existing_claim");
        assert_eq!(decision.owner_key, "proj:fam");
        assert_eq!(decision.lineage_key, "fam");
        assert_eq!(decision.effective_weight, 2.0);
    }

    #[test]
    fn nested_gate_successor_reuses_starter_lineage() {
        let mut starter = running("starter", Some(2.0));
        starter.agent_family = Some("fam".to_string());
        let mut gate = running("gate", Some(2.0));
        gate.agent_family = Some("fam".to_string());
        gate.agent_family_role = Some("gate".to_string());
        gate.family_shell_kind = Some("gate".to_string());
        gate.family_shell_id = Some("gate-1".to_string());
        gate.family_shell_state = Some("approved".to_string());
        gate.parent_timestamp = Some("starter".to_string());
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("gate".to_string());

        let decision =
            snapshot_with_candidate(4.0, vec![starter, gate], successor)
                .candidate_decision
                .unwrap();
        assert_eq!(decision.decision, "reuse_existing_claim");
        assert_eq!(decision.lineage_key, "fam");
        assert_eq!(decision.effective_weight, 2.0);
    }

    #[test]
    fn candidate_serial_successor_of_live_parallel_member_reuses_parallel_lineage(
    ) {
        let mut parallel = running("parallel", Some(2.0));
        parallel.agent_family = Some("fam".to_string());
        parallel.agent_family_parallel = true;
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("parallel".to_string());

        let decision = snapshot_with_candidate(4.0, vec![parallel], successor)
            .candidate_decision
            .unwrap();
        assert_eq!(decision.decision, "reuse_existing_claim");
        assert_eq!(decision.claim_kind, "parallel_member");
        assert_eq!(decision.owner_key, "proj:fam:parallel:parallel");
        assert_eq!(decision.lineage_key, "fam:parallel:parallel");
        assert_eq!(decision.effective_weight, 2.0);
    }

    #[test]
    fn persisted_owner_keeps_released_parallel_successor_off_unrelated_family_claim(
    ) {
        let mut serial_branch = running("serial-branch", Some(2.0));
        serial_branch.agent_family = Some("fam".to_string());
        let mut successor =
            waiting("successor", "2026-09-10T00:00:00Z", Some(2.0));
        successor.agent_family = Some("fam".to_string());
        successor.parent_timestamp = Some("parallel-parent".to_string());
        successor.runner_claim_owner_key =
            Some("fam:parallel:parallel-parent".to_string());

        let result =
            snapshot_with_candidate(4.0, vec![serial_branch], successor);
        assert_eq!(result.occupied_capacity, 2.0);
        let decision = result.candidate_decision.as_ref().unwrap();
        assert_eq!(decision.decision, "acquire_capacity");
        assert_eq!(decision.owner_key, "proj:fam:parallel:parallel-parent");
        assert_eq!(decision.lineage_key, "fam:parallel:parallel-parent");
        assert_ne!(decision.owner_key, result.claims[0].owner_key);
    }

    #[test]
    fn parked_waiters_sort_by_capacity_then_runner_shortfall_before_priority() {
        let held = running("held", Some(1.0));
        let mut heavy = waiting("heavy", "2026-09-10T00:00:00Z", Some(2.0));
        heavy.wait_priority = Some(1);
        let mut light = waiting("light", "2026-09-10T00:00:01Z", Some(0.5));
        light.wait_priority = Some(10);

        let result = snapshot(1.0, vec![held, heavy, light]);
        assert_eq!(
            result
                .waiters
                .iter()
                .map(|waiter| waiter.artifact_dir.as_str())
                .collect::<Vec<_>>(),
            ["/tmp/light", "/tmp/heavy"]
        );
        assert!(result.waiters.iter().all(|waiter| waiter.parked));
        assert_eq!(result.waiters[0].capacity_shortfall, 0.5);
        assert_eq!(result.waiters[1].capacity_shortfall, 2.0);
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
