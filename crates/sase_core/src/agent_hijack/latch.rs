//! Pure policy for durable agent-hijack latches.
//!
//! Python owns target-indexed persistence, process probes, claim mutation, and
//! snapshots. This module validates identity evidence and decides whether a
//! latch is pending, may apply, finished first, or must retain custody.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::wire::*;

pub const AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION: u32 = 1;
pub const HIJACK_LATCH_FORCE_AFTER_SECONDS: f64 = 120.0;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackLatchError {
    #[error("unsupported agent hijack latch wire schema_version {0}")]
    UnsupportedSchema(u32),
}

/// Facts the boundary caller uses to verify it still owns the requested turn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchObservationFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub target_key: String,
    #[serde(default)]
    pub boundary: String,
    #[serde(default)]
    pub member: String,
    #[serde(default)]
    pub artifacts_dir: String,
    #[serde(default)]
    pub owner_pid: Option<i64>,
    #[serde(default)]
    pub owner_identity: Option<String>,
}

/// A structured latch observation shared by runners, CLI, and the TUI.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchObservationWire {
    pub schema_version: u32,
    #[serde(default)]
    pub verdict: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub hijack_id: String,
    #[serde(default)]
    pub target_key: String,
    #[serde(default)]
    pub boundary: Option<String>,
    #[serde(default)]
    pub boundary_outcome: Option<String>,
    #[serde(default)]
    pub suppressed_followup: Option<AgentHijackSuppressedFollowupWire>,
    #[serde(default)]
    pub forced: bool,
}

/// Observe whether *record* still owns the supplied target at a boundary.
pub fn agent_hijack_latch_observe(
    record: &AgentHijackRecordWire,
    facts: &AgentHijackLatchObservationFactsWire,
) -> Result<AgentHijackLatchObservationWire, AgentHijackLatchError> {
    check_schema(facts.schema_version)?;
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackLatchError::UnsupportedSchema(
            record.schema_version,
        ));
    }

    let identity_matches = record_owner_is_verifiable(record)
        && !facts.target_key.trim().is_empty()
        && record.target_key == facts.target_key
        && !facts.member.trim().is_empty()
        && record.agent_name == facts.member
        && !facts.artifacts_dir.trim().is_empty()
        && record.artifacts_dir == facts.artifacts_dir
        && record.owner_pid == facts.owner_pid
        && nonempty(record.owner_identity.as_deref())
            == nonempty(facts.owner_identity.as_deref())
        && nonempty(facts.owner_identity.as_deref()).is_some();
    let (verdict, code, message) = if !identity_matches {
        (
            "ownership_uncertain",
            "owner_identity_mismatch",
            "the latch does not match this member, artifact, target, or process identity; retain custody and inspect the owner",
        )
    } else {
        match record.state.as_str() {
            HIJACK_LATCHED => (
                "pending",
                "pending",
                "the owner reached a boundary while a hijack latch is pending",
            ),
            HIJACK_STOPPING | HIJACK_HELD if record.latched_at.is_some() => (
                "applied",
                "applied",
                "the latch has applied and custody remains with the hijack",
            ),
            HIJACK_CANCELLED => (
                "cancelled",
                "cancelled",
                "the pending hijack latch was cancelled",
            ),
            HIJACK_FINISHED_FIRST if record.latched_at.is_some() => (
                "moot",
                "finished_first",
                "the owner finished first; the real outcome was kept",
            ),
            _ => (
                "no_latch",
                "no_latch",
                "there is no pending hijack latch for this target",
            ),
        }
    };

    Ok(AgentHijackLatchObservationWire {
        schema_version: AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION,
        verdict: verdict.to_owned(),
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
        hijack_id: record.hijack_id.clone(),
        target_key: facts.target_key.clone(),
        boundary: nonempty(Some(facts.boundary.as_str())).map(str::to_owned),
        boundary_outcome: record.boundary_outcome.clone(),
        suppressed_followup: record.suppressed_followup.clone(),
        forced: record.forced,
    })
}

/// Owner liveness observation. `lookup_succeeded` means the exact recorded
/// pid was probed without permission or I/O uncertainty.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchOwnerObservationWire {
    #[serde(default)]
    pub lookup_succeeded: bool,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub process_identity: Option<String>,
}

/// A candidate launched successor tied back to the original owner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchSuccessorObservationWire {
    /// `absent`, `uncertain`, or `launched`.
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub source_member: Option<String>,
    #[serde(default)]
    pub source_artifacts_dir: Option<String>,
    #[serde(default)]
    pub source_identity: Option<String>,
    #[serde(default)]
    pub launch_identity: Option<String>,
    #[serde(default)]
    pub admission_request_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub process_identity: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
}

/// Facts needed to reconcile a latch after its owner disappears.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchReconcileFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub owner: AgentHijackLatchOwnerObservationWire,
    #[serde(default)]
    pub successor: AgentHijackLatchSuccessorObservationWire,
    #[serde(default)]
    pub boundary_outcome: Option<String>,
}

/// Pure reconciliation action. Error outcomes retain custody.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchReconcileDecisionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub hijack_id: String,
}

/// Reconcile a pending latch from identity-proven process and successor data.
pub fn hijack_latch_reconcile(
    record: &AgentHijackRecordWire,
    facts: &AgentHijackLatchReconcileFactsWire,
) -> Result<AgentHijackLatchReconcileDecisionWire, AgentHijackLatchError> {
    check_schema(facts.schema_version)?;
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackLatchError::UnsupportedSchema(
            record.schema_version,
        ));
    }
    if record.state != HIJACK_LATCHED {
        return Ok(reconcile_decision(
            "retain_with_error",
            "not_pending",
            "only a pending latch can be reconciled; keep its current custody",
            record,
        ));
    }
    if has_proven_successor(record, &facts.successor)
        && real_outcome(facts.boundary_outcome.as_deref())
    {
        return Ok(reconcile_decision(
            "moot_latch",
            "successor_won",
            "a real outcome and identity-proven successor show that normal progression won first",
            record,
        ));
    }
    if !record_owner_is_verifiable(record) {
        return Ok(reconcile_decision(
            "retain_with_error",
            "owner_identity_missing",
            "the recorded owner pid or process identity is missing; retain custody and inspect the owner",
            record,
        ));
    }
    if !facts.owner.lookup_succeeded {
        return Ok(reconcile_decision(
            "retain_with_error",
            "owner_liveness_unknown",
            "the owner process probe was inconclusive; retain custody and retry reconciliation",
            record,
        ));
    }
    match owner_liveness(record, &facts.owner) {
        OwnerLiveness::Live => Ok(reconcile_decision(
            "keep_pending",
            "owner_live",
            "the identity-matched owner is still live; leave the latch pending",
            record,
        )),
        OwnerLiveness::Dead => match facts.successor.kind.as_str() {
            "absent" => Ok(reconcile_decision(
                "apply_latch",
                "owner_dead",
                "the recorded owner is identity-proven dead and no successor exists; apply the latch at the saved boundary",
                record,
            )),
            "launched" => Ok(reconcile_decision(
                "retain_with_error",
                "successor_outcome_missing",
                "a launched successor is visible without a matching real terminal outcome; retain custody and inspect its evidence",
                record,
            )),
            _ => Ok(reconcile_decision(
                "retain_with_error",
                "successor_uncertain",
                "successor evidence is uncertain; retain custody and retry reconciliation",
                record,
            )),
        },
        OwnerLiveness::Unknown => Ok(reconcile_decision(
            "retain_with_error",
            "owner_identity_uncertain",
            "the observed process does not prove whether the recorded owner is alive; retain custody",
            record,
        )),
    }
}

/// Decision on whether an explicit interruption is old enough to force now.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackLatchForceDecisionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub eligible: bool,
    #[serde(default)]
    pub age_seconds: Option<f64>,
    #[serde(default)]
    pub threshold_seconds: f64,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

/// Apply the 120-second force threshold using the caller's injected time.
pub fn hijack_latch_force_eligibility(
    record: &AgentHijackRecordWire,
    now_unix: f64,
) -> AgentHijackLatchForceDecisionWire {
    let age = record.latched_at.map(|at| now_unix - at as f64 / 1_000.0);
    let (eligible, code, message) = if record.schema_version
        != AGENT_HIJACK_WIRE_SCHEMA_VERSION
    {
        (
            false,
            "unsupported_schema",
            "the latch record schema is unsupported; keep it pending",
        )
    } else if record.state != HIJACK_LATCHED {
        (false, "not_pending", "only a pending latch can be forced")
    } else if record.latched_at.is_none_or(|at| at < 0)
        || !now_unix.is_finite()
        || age.is_none_or(|value| !value.is_finite())
    {
        (
            false,
            "time_unknown",
            "the latch age cannot be verified; keep it pending",
        )
    } else if age.is_some_and(|value| value >= HIJACK_LATCH_FORCE_AFTER_SECONDS)
    {
        (
            true,
            "force_eligible",
            "the pending latch has waited at least 120 seconds and may be explicitly forced",
        )
    } else if age.is_some_and(|value| value < 0.0) {
        (
            false,
            "clock_before_latch",
            "the injected current time predates the latch; keep it pending",
        )
    } else {
        (
            false,
            "force_too_early",
            "wait until the pending latch is at least 120 seconds old",
        )
    };
    AgentHijackLatchForceDecisionWire {
        schema_version: AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION,
        eligible,
        age_seconds: age,
        threshold_seconds: HIJACK_LATCH_FORCE_AFTER_SECONDS,
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerLiveness {
    Live,
    Dead,
    Unknown,
}

fn owner_liveness(
    record: &AgentHijackRecordWire,
    owner: &AgentHijackLatchOwnerObservationWire,
) -> OwnerLiveness {
    let Some(recorded_pid) = record.owner_pid else {
        return OwnerLiveness::Unknown;
    };
    let Some(recorded_identity) = nonempty(record.owner_identity.as_deref())
    else {
        return OwnerLiveness::Unknown;
    };
    match owner.pid {
        None => OwnerLiveness::Dead,
        Some(pid) if pid != recorded_pid => OwnerLiveness::Unknown,
        Some(_) => match nonempty(owner.process_identity.as_deref()) {
            Some(identity) if identity == recorded_identity => {
                OwnerLiveness::Live
            }
            Some(_) => OwnerLiveness::Dead,
            None => OwnerLiveness::Unknown,
        },
    }
}

fn record_owner_is_verifiable(record: &AgentHijackRecordWire) -> bool {
    record.owner_pid.is_some_and(|pid| pid > 0)
        && nonempty(record.owner_identity.as_deref()).is_some()
        && !record.agent_name.trim().is_empty()
        && !record.artifacts_dir.trim().is_empty()
}

fn has_proven_successor(
    record: &AgentHijackRecordWire,
    successor: &AgentHijackLatchSuccessorObservationWire,
) -> bool {
    successor.kind == "launched"
        && nonempty(successor.source_member.as_deref())
            == nonempty(Some(record.agent_name.as_str()))
        && nonempty(successor.source_artifacts_dir.as_deref())
            == nonempty(Some(record.artifacts_dir.as_str()))
        && nonempty(successor.source_identity.as_deref())
            == nonempty(record.owner_identity.as_deref())
        && (nonempty(successor.launch_identity.as_deref()).is_some()
            || nonempty(successor.admission_request_id.as_deref()).is_some())
        && successor.pid.is_some_and(|pid| pid > 0)
        && nonempty(successor.process_identity.as_deref()).is_some()
        && nonempty(successor.artifacts_dir.as_deref()).is_some()
}

fn real_outcome(outcome: Option<&str>) -> bool {
    nonempty(outcome).is_some_and(|value| value != "hijacked")
}

fn check_schema(schema_version: u32) -> Result<(), AgentHijackLatchError> {
    if schema_version == AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(AgentHijackLatchError::UnsupportedSchema(schema_version))
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn reconcile_decision(
    outcome: &str,
    code: &str,
    message: &str,
    record: &AgentHijackRecordWire,
) -> AgentHijackLatchReconcileDecisionWire {
    AgentHijackLatchReconcileDecisionWire {
        schema_version: AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION,
        outcome: outcome.to_owned(),
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
        hijack_id: record.hijack_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending() -> AgentHijackRecordWire {
        AgentHijackRecordWire {
            schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
            hijack_id: "h-1".to_owned(),
            agent_name: "agent.1".to_owned(),
            artifacts_dir: "/artifacts/agent.1".to_owned(),
            target_key: "target-1".to_owned(),
            state: HIJACK_LATCHED.to_owned(),
            latched_at: Some(100_000),
            owner_pid: Some(42),
            owner_identity: Some("boot:123".to_owned()),
            ..Default::default()
        }
    }

    fn facts() -> AgentHijackLatchReconcileFactsWire {
        AgentHijackLatchReconcileFactsWire {
            schema_version: AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION,
            owner: AgentHijackLatchOwnerObservationWire {
                lookup_succeeded: true,
                pid: Some(42),
                process_identity: Some("boot:123".to_owned()),
            },
            successor: AgentHijackLatchSuccessorObservationWire {
                kind: "absent".to_owned(),
                ..Default::default()
            },
            boundary_outcome: None,
        }
    }

    #[test]
    fn observe_checks_target_member_artifact_and_process_identity() {
        let record = pending();
        let mut observe = AgentHijackLatchObservationFactsWire {
            schema_version: AGENT_HIJACK_LATCH_WIRE_SCHEMA_VERSION,
            target_key: "target-1".to_owned(),
            boundary: "finalizer_done".to_owned(),
            member: "agent.1".to_owned(),
            artifacts_dir: "/artifacts/agent.1".to_owned(),
            owner_pid: Some(42),
            owner_identity: Some("boot:123".to_owned()),
        };
        assert_eq!(
            agent_hijack_latch_observe(&record, &observe)
                .unwrap()
                .verdict,
            "pending"
        );
        observe.owner_identity = Some("boot:reused".to_owned());
        assert_eq!(
            agent_hijack_latch_observe(&record, &observe)
                .unwrap()
                .verdict,
            "ownership_uncertain"
        );
        observe.owner_identity = None;
        assert_eq!(
            agent_hijack_latch_observe(&record, &observe)
                .unwrap()
                .verdict,
            "ownership_uncertain"
        );
    }

    #[test]
    fn reconciliation_requires_identity_proof_and_successor_disposition() {
        let record = pending();
        let mut request = facts();
        assert_eq!(
            hijack_latch_reconcile(&record, &request).unwrap().outcome,
            "keep_pending"
        );
        request.owner.pid = None;
        assert_eq!(
            hijack_latch_reconcile(&record, &request).unwrap().outcome,
            "apply_latch"
        );
        request.successor.kind = "uncertain".to_owned();
        assert_eq!(
            hijack_latch_reconcile(&record, &request)
                .unwrap()
                .code
                .as_deref(),
            Some("successor_uncertain")
        );
    }

    #[test]
    fn a_real_outcome_and_identity_proven_successor_moot_the_latch() {
        let record = pending();
        let mut request = facts();
        request.owner.pid = None;
        request.successor = AgentHijackLatchSuccessorObservationWire {
            kind: "launched".to_owned(),
            source_member: Some("agent.1".to_owned()),
            source_artifacts_dir: Some("/artifacts/agent.1".to_owned()),
            source_identity: Some("boot:123".to_owned()),
            launch_identity: Some("launch-1".to_owned()),
            name: Some("agent.2".to_owned()),
            pid: Some(44),
            process_identity: Some("boot:456".to_owned()),
            artifacts_dir: Some("/artifacts/agent.2".to_owned()),
            ..Default::default()
        };
        request.boundary_outcome = Some("completed".to_owned());
        assert_eq!(
            hijack_latch_reconcile(&record, &request).unwrap().outcome,
            "moot_latch"
        );
        request.boundary_outcome = Some("hijacked".to_owned());
        assert_eq!(
            hijack_latch_reconcile(&record, &request).unwrap().outcome,
            "retain_with_error"
        );
    }

    #[test]
    fn force_wait_is_injected_and_inclusive_at_120_seconds() {
        let record = pending();
        assert!(!hijack_latch_force_eligibility(&record, 219.999).eligible);
        let ready = hijack_latch_force_eligibility(&record, 220.0);
        assert!(ready.eligible);
        assert_eq!(ready.age_seconds, Some(120.0));
        assert!(!hijack_latch_force_eligibility(&record, 99.0).eligible);
    }
}
