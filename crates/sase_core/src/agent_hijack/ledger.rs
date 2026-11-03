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
        (HIJACK_REQUESTED | HIJACK_STOPPING, HIJACK_EVENT_FINISHED_FIRST) => {
            HIJACK_FINISHED_FIRST
        }
        (HIJACK_STOPPING, HIJACK_EVENT_CONFIRMED_STOP) => HIJACK_HELD,
        (HIJACK_STOPPING, HIJACK_EVENT_STOP_FAILED) => HIJACK_STOP_FAILED,
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
            updated.error = None;
        }
        HIJACK_EVENT_COMPLETE_RESOLUTION => {
            if next == HIJACK_EXPIRED || next == HIJACK_HELD {
                updated.resolution_action = None;
                updated.resolution_origin_state = None;
                updated.reserved_successor_name = None;
                updated.concrete_target_model = None;
                updated.model_changed = None;
            }
            updated.error = None;
        }
        HIJACK_EVENT_RESOLUTION_FAILED => {
            updated.resolution_origin_state = None;
            updated.error = event.error.clone();
        }
        HIJACK_EVENT_STOP_FAILED => updated.error = event.error.clone(),
        _ => {}
    }

    Ok(updated)
}
