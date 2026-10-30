use crate::agent_runtime::parse_runtime_timestamp;

use super::wire::*;

const ACTIVE_STATES: &[&str] = &["active", "waiting"];
const TERMINAL_STATES: &[&str] =
    &["completed", "failed", "stopped", "dismissed"];

pub fn resolve_agent_run_state(
    request: AgentRunStateResolveRequestWire,
) -> Result<AgentRunStateResolveResponseWire, AgentRunStateError> {
    validate_schema(request.schema_version)?;
    validate_identity(&request.identity, "identity")?;

    let mut response =
        response_from_published(&request, AGENT_RUN_STATE_REASON_UNCHANGED);
    if !ACTIVE_STATES.contains(&request.published_state.as_str())
        && !TERMINAL_STATES.contains(&request.published_state.as_str())
    {
        return Err(AgentRunStateError::InvalidRequest(format!(
            "unsupported published_state {:?}",
            request.published_state
        )));
    }
    if !timestamps_valid(
        request.published_started_at.as_deref(),
        request.published_finished_at.as_deref(),
    ) {
        return Err(AgentRunStateError::InvalidRequest(
            "published timestamps must use a supported SASE timestamp format"
                .to_string(),
        ));
    }

    match request.evidence_kind.as_str() {
        AGENT_RUN_STATE_EVIDENCE_MISSING => {
            response.reason =
                AGENT_RUN_STATE_REASON_MISSING_EVIDENCE.to_string();
        }
        AGENT_RUN_STATE_EVIDENCE_UNFINISHED => {
            let Some(evidence_identity) = request.evidence_identity.as_ref()
            else {
                response.reason =
                    AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string();
                return Ok(response);
            };
            if !identity_matches(&request.identity, evidence_identity) {
                response.reason =
                    AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT.to_string();
                return Ok(response);
            }
            if TERMINAL_STATES.contains(&request.published_state.as_str()) {
                response.reason =
                    AGENT_RUN_STATE_REASON_PRESERVED_TERMINAL.to_string();
            } else {
                response.reason =
                    AGENT_RUN_STATE_REASON_LIVE_EVIDENCE.to_string();
            }
        }
        AGENT_RUN_STATE_EVIDENCE_IDENTITY_CONFLICT => {
            response.reason =
                AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT.to_string();
        }
        AGENT_RUN_STATE_EVIDENCE_MALFORMED => {
            response.reason =
                AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string();
        }
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER
        | AGENT_RUN_STATE_EVIDENCE_DISMISSED => {
            let Some(evidence_identity) = request.evidence_identity.as_ref()
            else {
                response.reason =
                    AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string();
                return Ok(response);
            };
            if !identity_matches(&request.identity, evidence_identity) {
                response.reason =
                    AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT.to_string();
                return Ok(response);
            }
            if !timestamps_valid(
                request.evidence_started_at.as_deref(),
                request.evidence_finished_at.as_deref(),
            ) {
                response.reason =
                    AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string();
                return Ok(response);
            }
            let Some(evidence_state) = terminal_evidence_state(&request) else {
                response.reason =
                    AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string();
                return Ok(response);
            };
            resolve_terminal_evidence(&request, evidence_state, &mut response);
        }
        _ => {
            response.reason =
                AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string()
        }
    }

    Ok(response)
}

fn validate_schema(schema_version: u32) -> Result<(), AgentRunStateError> {
    if schema_version != AGENT_RUN_STATE_WIRE_SCHEMA_VERSION {
        return Err(AgentRunStateError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn validate_identity(
    identity: &AgentRunIdentityWire,
    field: &str,
) -> Result<(), AgentRunStateError> {
    if [
        identity.project.as_str(),
        identity.owner.as_str(),
        identity.global_name.as_str(),
        identity.source_run_id.as_str(),
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        return Err(AgentRunStateError::InvalidRequest(format!(
            "{field} fields must be non-empty"
        )));
    }
    Ok(())
}

fn identity_matches(
    left: &AgentRunIdentityWire,
    right: &AgentRunIdentityWire,
) -> bool {
    left == right
}

fn timestamps_valid(
    started_at: Option<&str>,
    finished_at: Option<&str>,
) -> bool {
    [started_at, finished_at]
        .into_iter()
        .flatten()
        .all(|value| parse_runtime_timestamp(value).is_some())
}

fn terminal_evidence_state(
    request: &AgentRunStateResolveRequestWire,
) -> Option<&str> {
    match request.evidence_kind.as_str() {
        AGENT_RUN_STATE_EVIDENCE_DISMISSED => request
            .evidence_state
            .as_deref()
            .filter(|state| matches!(*state, "stopped" | "dismissed")),
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER => request
            .evidence_state
            .as_deref()
            .filter(|state| TERMINAL_STATES.contains(state)),
        _ => None,
    }
}

fn response_from_published(
    request: &AgentRunStateResolveRequestWire,
    reason: &str,
) -> AgentRunStateResolveResponseWire {
    AgentRunStateResolveResponseWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        selected_state: request.published_state.clone(),
        selected_started_at: request.published_started_at.clone(),
        selected_finished_at: request.published_finished_at.clone(),
        changed: false,
        reason: reason.to_string(),
    }
}

fn resolve_terminal_evidence(
    request: &AgentRunStateResolveRequestWire,
    evidence_state: &str,
    response: &mut AgentRunStateResolveResponseWire,
) {
    let evidence_is_dismissal =
        request.evidence_kind == AGENT_RUN_STATE_EVIDENCE_DISMISSED;
    let published_is_terminal =
        TERMINAL_STATES.contains(&request.published_state.as_str());

    if published_is_terminal {
        if request.published_state == evidence_state {
            fill_missing_timestamps(request, response);
            response.reason = if response.changed {
                if evidence_is_dismissal {
                    AGENT_RUN_STATE_REASON_UPDATED_DISMISSED
                } else {
                    AGENT_RUN_STATE_REASON_UPDATED_TERMINAL_MARKER
                }
            } else {
                AGENT_RUN_STATE_REASON_UNCHANGED
            }
            .to_string();
            return;
        }

        if evidence_is_dismissal
            && is_later(
                request.evidence_finished_at.as_deref(),
                request.published_finished_at.as_deref(),
            )
        {
            response.selected_state = evidence_state.to_string();
            if let Some(finished_at) = request.evidence_finished_at.as_ref() {
                response.selected_finished_at = Some(finished_at.clone());
            }
            fill_missing_timestamps(request, response);
            response.changed = true;
            response.reason =
                AGENT_RUN_STATE_REASON_LATER_DISMISSAL.to_string();
            return;
        }

        response.reason =
            AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL.to_string();
        return;
    }

    response.selected_state = evidence_state.to_string();
    if let Some(started_at) = request.evidence_started_at.as_ref() {
        response.selected_started_at = Some(started_at.clone());
    }
    if let Some(finished_at) = request.evidence_finished_at.as_ref() {
        response.selected_finished_at = Some(finished_at.clone());
    }
    response.changed = response.selected_state != request.published_state
        || response.selected_started_at != request.published_started_at
        || response.selected_finished_at != request.published_finished_at;
    response.reason = if response.changed {
        if evidence_is_dismissal {
            AGENT_RUN_STATE_REASON_UPDATED_DISMISSED
        } else {
            AGENT_RUN_STATE_REASON_UPDATED_TERMINAL_MARKER
        }
    } else {
        AGENT_RUN_STATE_REASON_UNCHANGED
    }
    .to_string();
}

fn fill_missing_timestamps(
    request: &AgentRunStateResolveRequestWire,
    response: &mut AgentRunStateResolveResponseWire,
) {
    if response.selected_started_at.is_none() {
        response.selected_started_at = request.evidence_started_at.clone();
    }
    if response.selected_finished_at.is_none() {
        response.selected_finished_at = request.evidence_finished_at.clone();
    }
    response.changed = response.selected_started_at
        != request.published_started_at
        || response.selected_finished_at != request.published_finished_at;
}

fn is_later(candidate: Option<&str>, current: Option<&str>) -> bool {
    let (Some(candidate), Some(current)) = (candidate, current) else {
        return false;
    };
    match (
        parse_runtime_timestamp(candidate),
        parse_runtime_timestamp(current),
    ) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}
