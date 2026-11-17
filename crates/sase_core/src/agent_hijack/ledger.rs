use super::wire::*;
use thiserror::Error;

const RELEASE_ACTIONS: &[&str] = &[
    HIJACK_ACTION_RESTART,
    HIJACK_ACTION_EDIT,
    HIJACK_ACTION_VIEW,
    HIJACK_ACTION_DISMISS,
];
const HELD_ACTIONS: &[&str] = &[
    HIJACK_ACTION_ESCALATE,
    HIJACK_ACTION_CONTINUE,
    HIJACK_ACTION_INSTRUCT,
    HIJACK_ACTION_RERUN,
    HIJACK_ACTION_RESTART,
    HIJACK_ACTION_EDIT,
    HIJACK_ACTION_VIEW,
    HIJACK_ACTION_DISMISS,
];

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackLedgerError {
    #[error("unsupported agent hijack wire schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("illegal agent hijack ledger transition: {state} + {event}")]
    IllegalTransition { state: String, event: String },
    #[error("agent hijack transition requires a supported resolution action")]
    InvalidAction,
    #[error("agent hijack record revision overflow")]
    RevisionOverflow,
    #[error(
        "agent hijack latch requires a target key and owner process identity"
    )]
    InvalidLatchOwner,
    #[error("agent hijack latch application requires a boundary and outcome")]
    InvalidLatchBoundary,
}

/// Advance a hijack record through the durable state machine.
pub fn advance_agent_hijack_record(
    record: &AgentHijackRecordWire,
    event: &AgentHijackEventWire,
) -> Result<AgentHijackRecordWire, AgentHijackLedgerError> {
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackLedgerError::UnsupportedSchema(
            record.schema_version,
        ));
    }

    let next = match (record.state.as_str(), event.event.as_str()) {
        (HIJACK_REQUESTED, HIJACK_EVENT_BEGIN_STOP) => HIJACK_STOPPING,
        (HIJACK_REQUESTED, HIJACK_EVENT_LATCH) => {
            if record.target_key.trim().is_empty()
                || event.owner_pid.is_none_or(|pid| pid <= 0)
                || event
                    .owner_identity
                    .as_deref()
                    .is_none_or(|identity| identity.trim().is_empty())
                || event.latched_at.is_none_or(|at| at < 0)
            {
                return Err(AgentHijackLedgerError::InvalidLatchOwner);
            }
            HIJACK_LATCHED
        }
        (HIJACK_LATCHED, HIJACK_EVENT_APPLY_LATCH) => {
            if event
                .boundary
                .as_deref()
                .is_none_or(|boundary| boundary.trim().is_empty())
                || event
                    .boundary_outcome
                    .as_deref()
                    .is_none_or(|outcome| outcome.trim().is_empty())
            {
                return Err(AgentHijackLedgerError::InvalidLatchBoundary);
            }
            HIJACK_STOPPING
        }
        (HIJACK_LATCHED, HIJACK_EVENT_CANCEL_LATCH) => HIJACK_CANCELLED,
        (
            HIJACK_LATCHED,
            HIJACK_EVENT_MOOT_LATCH | HIJACK_EVENT_FINISHED_FIRST,
        ) => {
            if event
                .boundary
                .as_deref()
                .is_none_or(|boundary| boundary.trim().is_empty())
                || event
                    .boundary_outcome
                    .as_deref()
                    .is_none_or(|outcome| outcome.trim().is_empty())
            {
                return Err(AgentHijackLedgerError::InvalidLatchBoundary);
            }
            HIJACK_FINISHED_FIRST
        }
        (HIJACK_REQUESTED | HIJACK_STOPPING, HIJACK_EVENT_FINISHED_FIRST) => {
            HIJACK_FINISHED_FIRST
        }
        (HIJACK_STOPPING, HIJACK_EVENT_CONFIRMED_STOP) => HIJACK_HELD,
        (HIJACK_STOPPING, HIJACK_EVENT_STOP_FAILED) => HIJACK_STOP_FAILED,
        (HIJACK_STOPPING, HIJACK_EVENT_STOP_BLOCKED) => HIJACK_STOP_BLOCKED,
        (HIJACK_STOP_BLOCKED, HIJACK_EVENT_RETRY_STOP) => HIJACK_STOPPING,
        (HIJACK_STOP_BLOCKED, HIJACK_EVENT_ABANDON_STOP) => HIJACK_STOP_FAILED,
        (HIJACK_HELD, HIJACK_EVENT_EXPIRE) => HIJACK_EXPIRED,
        (HIJACK_HELD | HIJACK_EXPIRED, HIJACK_EVENT_BEGIN_RESOLUTION) => {
            let action = event
                .action
                .as_deref()
                .ok_or(AgentHijackLedgerError::InvalidAction)?;
            let allowed = if record.state == HIJACK_EXPIRED {
                RELEASE_ACTIONS.contains(&action)
            } else {
                HELD_ACTIONS.contains(&action)
            };
            if !allowed {
                return Err(AgentHijackLedgerError::InvalidAction);
            }
            HIJACK_RESOLVING
        }
        (HIJACK_RESOLVING, HIJACK_EVENT_RECORD_LAUNCH) => HIJACK_RESOLVING,
        (HIJACK_RESOLVING, HIJACK_EVENT_COMPLETE_RESOLUTION) => {
            match record.resolution_action.as_deref() {
                Some(HIJACK_ACTION_DISMISS) => HIJACK_DISMISSED,
                Some(HIJACK_ACTION_VIEW) => record
                    .resolution_origin_state
                    .as_deref()
                    .unwrap_or(HIJACK_HELD),
                Some(action) if HELD_ACTIONS.contains(&action) => {
                    HIJACK_RESOLVED
                }
                _ => return Err(AgentHijackLedgerError::InvalidAction),
            }
        }
        (HIJACK_RESOLVING, HIJACK_EVENT_RESOLUTION_FAILED) => HIJACK_HELD,
        _ => {
            return Err(AgentHijackLedgerError::IllegalTransition {
                state: record.state.clone(),
                event: event.event.clone(),
            });
        }
    };

    let mut updated = record.clone();
    updated.state = next.to_string();
    updated.revision = updated
        .revision
        .checked_add(1)
        .ok_or(AgentHijackLedgerError::RevisionOverflow)?;
    updated.updated_at = event.at.clone().filter(|value| !value.is_empty());

    match event.event.as_str() {
        HIJACK_EVENT_BEGIN_RESOLUTION => {
            updated.resolution_action = event.action.clone();
            updated.resolution_origin_state = Some(record.state.clone());
            updated.reserved_successor_name =
                event.reserved_successor_name.clone();
            updated.concrete_target_model = event.concrete_target_model.clone();
            updated.model_changed = event.model_changed;
            updated.resolver_pid = event.resolver_pid;
            updated.resolver_process_identity_token =
                event.resolver_process_identity_token.clone();
            apply_launch_evidence(&mut updated, event);
            updated.error = None;
        }
        HIJACK_EVENT_RECORD_LAUNCH => {
            apply_launch_evidence(&mut updated, event);
            if event.error.is_some() {
                updated.error = event.error.clone();
            }
        }
        HIJACK_EVENT_COMPLETE_RESOLUTION => {
            if next == HIJACK_EXPIRED || next == HIJACK_HELD {
                clear_resolution_fields(&mut updated);
            } else {
                apply_launch_evidence(&mut updated, event);
            }
            updated.error = None;
        }
        HIJACK_EVENT_RESOLUTION_FAILED => {
            updated.resolution_origin_state = None;
            clear_launch_evidence(&mut updated);
            updated.resolver_pid = None;
            updated.resolver_process_identity_token = None;
            updated.error = event.error.clone();
        }
        HIJACK_EVENT_STOP_FAILED | HIJACK_EVENT_ABANDON_STOP => {
            updated.error = event.error.clone();
        }
        HIJACK_EVENT_STOP_BLOCKED if event.error.is_some() => {
            updated.error = event.error.clone();
        }
        HIJACK_EVENT_STOP_BLOCKED => {}
        HIJACK_EVENT_LATCH => {
            updated.owner_pid = event.owner_pid;
            updated.owner_identity = event.owner_identity.clone();
            updated.latched_at = event.latched_at;
            updated.error = None;
        }
        HIJACK_EVENT_APPLY_LATCH => {
            updated.applied_at =
                event.applied_at.clone().or_else(|| event.at.clone());
            updated.boundary = event.boundary.clone();
            updated.boundary_outcome = event.boundary_outcome.clone();
            updated.suppressed_followup = event.suppressed_followup.clone();
            updated.forced = event.forced.unwrap_or(false);
            updated.error = None;
        }
        HIJACK_EVENT_MOOT_LATCH
            if record.state == HIJACK_LATCHED
                || next == HIJACK_FINISHED_FIRST =>
        {
            if event.boundary.is_some() {
                updated.boundary = event.boundary.clone();
            }
            if event.boundary_outcome.is_some() {
                updated.boundary_outcome = event.boundary_outcome.clone();
            }
            if event.suppressed_followup.is_some() {
                updated.suppressed_followup = event.suppressed_followup.clone();
            }
            updated.error = None;
        }
        HIJACK_EVENT_FINISHED_FIRST if record.state == HIJACK_LATCHED => {
            updated.boundary = event.boundary.clone();
            updated.boundary_outcome = event.boundary_outcome.clone();
            updated.suppressed_followup = event.suppressed_followup.clone();
        }
        _ => {}
    }

    Ok(updated)
}

fn apply_launch_evidence(
    updated: &mut AgentHijackRecordWire,
    event: &AgentHijackEventWire,
) {
    if event.launch_identity.is_some() {
        updated.launch_identity = event.launch_identity.clone();
    }
    if event.admission_request_id.is_some() {
        updated.admission_request_id = event.admission_request_id.clone();
    }
    if event.admission_identity.is_some() {
        updated.admission_identity = event.admission_identity.clone();
    }
    if event.launch_stage.is_some() {
        updated.launch_stage = event.launch_stage.clone();
    }
    if event.successor_pid.is_some() {
        updated.successor_pid = event.successor_pid;
    }
    if event.successor_process_identity_token.is_some() {
        updated.successor_process_identity_token =
            event.successor_process_identity_token.clone();
    }
    if event.successor_artifacts_dir.is_some() {
        updated.successor_artifacts_dir = event.successor_artifacts_dir.clone();
    }
    if event.inheritance_acknowledged.is_some() {
        updated.inheritance_acknowledged = event.inheritance_acknowledged;
    }
}

fn clear_launch_evidence(updated: &mut AgentHijackRecordWire) {
    updated.launch_identity = None;
    updated.admission_request_id = None;
    updated.admission_identity = None;
    updated.launch_stage = None;
    updated.successor_pid = None;
    updated.successor_process_identity_token = None;
    updated.successor_artifacts_dir = None;
    updated.inheritance_acknowledged = None;
}

fn clear_resolution_fields(updated: &mut AgentHijackRecordWire) {
    updated.resolution_action = None;
    updated.resolution_origin_state = None;
    updated.reserved_successor_name = None;
    updated.concrete_target_model = None;
    updated.model_changed = None;
    updated.resolver_pid = None;
    updated.resolver_process_identity_token = None;
    clear_launch_evidence(updated);
}
