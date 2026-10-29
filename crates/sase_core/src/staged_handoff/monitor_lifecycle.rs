//! Staged-monitor lifecycle facts: membership, owner liveness, and
//! self-wait decisions.
//!
//! A `staged` monitor member is durable intent, not a running process: it
//! is nonterminal, blocks another lane start, has no supervisor, and
//! occupies no runner capacity. These predicates preserve current
//! running/terminal behavior and only add the staged facts; they are not a
//! general monitor-domain migration.
//!
//! Owner liveness and self-wait answers fail closed. Unknown or corrupt
//! facts are never proof that a command ran or that a reservation may be
//! stolen, and no wall-clock grace period substitutes for durable stage
//! ownership — so these wires carry no timestamps at all.

use serde::{Deserialize, Serialize};

use super::validate::StagedHandoffError;
use super::wire::STAGED_HANDOFF_WIRE_SCHEMA_VERSION;

/// Monitor states the membership decision accepts.
const STATE_STAGED: &str = "staged";
const STATE_RUNNING: &str = "running";
const TERMINAL_STATES: &[&str] =
    &["completed", "failed", "timeout", "stopped", "lost"];

/// Host observations for one staged-monitor membership decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorMembershipRequestWire {
    pub schema_version: u32,
    pub monitor_state: String,
    pub supervisor_present: bool,
}

/// Durable staged-monitor membership facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedMonitorMembershipWire {
    pub schema_version: u32,
    pub nonterminal: bool,
    pub blocks_lane_start: bool,
    pub has_supervisor: bool,
    pub occupies_runner_capacity: bool,
}

/// Decide staged-monitor membership facts from durable monitor state.
pub fn evaluate_staged_monitor_membership(
    request: StagedMonitorMembershipRequestWire,
) -> Result<StagedMonitorMembershipWire, StagedHandoffError> {
    if request.schema_version != STAGED_HANDOFF_WIRE_SCHEMA_VERSION {
        return Err(StagedHandoffError::UnsupportedSchema(
            request.schema_version,
        ));
    }
    let state = request.monitor_state.as_str();
    if state == STATE_STAGED {
        if request.supervisor_present {
            return Err(StagedHandoffError::Invalid(
                "staged monitors have no supervisor".to_string(),
            ));
        }
        return Ok(StagedMonitorMembershipWire {
            schema_version: STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
            nonterminal: true,
            blocks_lane_start: true,
            has_supervisor: false,
            occupies_runner_capacity: false,
        });
    }
    if state == STATE_RUNNING {
        return Ok(StagedMonitorMembershipWire {
            schema_version: STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
            nonterminal: true,
            blocks_lane_start: true,
            has_supervisor: request.supervisor_present,
            occupies_runner_capacity: true,
        });
    }
    if TERMINAL_STATES.contains(&state) {
        return Ok(StagedMonitorMembershipWire {
            schema_version: STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
            nonterminal: false,
            blocks_lane_start: false,
            has_supervisor: false,
            occupies_runner_capacity: false,
        });
    }
    Err(StagedHandoffError::Invalid(format!(
        "unknown monitor_state: {:?}",
        request.monitor_state
    )))
}

/// Host observations for one staged-owner liveness decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedOwnerLivenessRequestWire {
    pub schema_version: u32,
    pub owner_kind: String,
    pub owner_id: String,
    pub staged_id: String,
    pub stage_record_present: bool,
    pub proc_exists: bool,
}

/// Whether a positively identified staged monitor stays a pending owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedOwnerLivenessDecisionWire {
    pub schema_version: u32,
    pub pending_owner: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Decide whether a staged monitor remains a pending owner.
///
/// Only a positively identified staged monitor with a durable staged
/// record and no proc yet is a pending owner. Every other input —
/// including corrupt identity and a missing record — decides
/// `pending_owner: false` with a reason instead of erroring, because
/// unknown facts are not proof of ownership.
pub fn evaluate_staged_owner_liveness(
    request: StagedOwnerLivenessRequestWire,
) -> Result<StagedOwnerLivenessDecisionWire, StagedHandoffError> {
    if request.schema_version != STAGED_HANDOFF_WIRE_SCHEMA_VERSION {
        return Err(StagedHandoffError::UnsupportedSchema(
            request.schema_version,
        ));
    }
    let decision =
        |pending_owner: bool, reason: &str| StagedOwnerLivenessDecisionWire {
            schema_version: STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
            pending_owner,
            reason: if pending_owner {
                None
            } else {
                Some(reason.to_string())
            },
        };
    if request.owner_kind != "monitor" {
        return Ok(decision(false, "owner_is_not_a_monitor"));
    }
    if request.owner_id.trim().is_empty() {
        return Ok(decision(false, "owner_id_is_missing"));
    }
    if !is_uuid(&request.staged_id) {
        return Ok(decision(false, "staged_id_is_not_a_uuid"));
    }
    if !request.stage_record_present {
        return Ok(decision(false, "no_durable_staged_record"));
    }
    if request.proc_exists {
        return Ok(decision(false, "proc_already_exists"));
    }
    Ok(decision(true, ""))
}

/// Host observations for one staged self-wait decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedSelfWaitRequestWire {
    pub schema_version: u32,
    pub caller_owner_kind: String,
    pub caller_owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_staged_id: Option<String>,
    /// Informational only: agent names never decide self-wait, so a shared
    /// agent name across different durable owners is not a self-wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_agent_name: Option<String>,
    pub run_owner_kind: String,
    pub run_owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_staged_id: Option<String>,
    /// Informational only: see `caller_agent_name`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_starter_agent_name: Option<String>,
    pub run_proc_exists: bool,
}

/// Whether the caller would synchronously wait on its own pending staged
/// monitor's ToolRun.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedSelfWaitDecisionWire {
    pub schema_version: u32,
    pub is_self_wait: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Decide whether a synchronous wait is a self-wait on the caller's own
/// pending staged monitor.
///
/// The decision keys on durable owner identity (`owner_kind` plus
/// `owner_id`) and durable stage ownership (`staged_id` plus the absent
/// proc). Agent names are accepted for context but never consulted: two
/// different owners sharing an agent name are not the same waiter.
pub fn evaluate_staged_self_wait(
    request: StagedSelfWaitRequestWire,
) -> Result<StagedSelfWaitDecisionWire, StagedHandoffError> {
    if request.schema_version != STAGED_HANDOFF_WIRE_SCHEMA_VERSION {
        return Err(StagedHandoffError::UnsupportedSchema(
            request.schema_version,
        ));
    }
    let decision =
        |is_self_wait: bool, reason: &str| StagedSelfWaitDecisionWire {
            schema_version: STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
            is_self_wait,
            reason: if is_self_wait {
                None
            } else {
                Some(reason.to_string())
            },
        };
    let Some(run_staged_id) = request
        .run_staged_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(decision(false, "run_is_not_staged"));
    };
    if request.run_proc_exists {
        return Ok(decision(false, "run_already_launched"));
    }
    if request.caller_owner_kind.trim().is_empty()
        || request.caller_owner_id.trim().is_empty()
        || request.run_owner_kind.trim().is_empty()
        || request.run_owner_id.trim().is_empty()
    {
        return Ok(decision(false, "owner_identity_unknown"));
    }
    if request.caller_owner_kind != request.run_owner_kind
        || request.caller_owner_id != request.run_owner_id
    {
        return Ok(decision(false, "different_owner"));
    }
    match request.caller_staged_id.as_deref() {
        Some(caller_staged_id) if caller_staged_id == run_staged_id => {
            Ok(decision(true, ""))
        }
        _ => Ok(decision(false, "stage_identity_mismatch")),
    }
}

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    let hyphen_at = [8_usize, 13, 18, 23];
    bytes.iter().enumerate().all(|(index, byte)| {
        if hyphen_at.contains(&index) {
            *byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}
