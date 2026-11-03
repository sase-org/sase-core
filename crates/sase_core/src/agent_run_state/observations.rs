use crate::agent_runtime::parse_runtime_timestamp;

use super::resolve::resolve_agent_run_state;
use super::wire::*;

pub fn resolve_agent_run_state_observations(
    request: AgentRunStateObservationsResolveRequestWire,
) -> Result<AgentRunStateObservationsResolveResponseWire, AgentRunStateError> {
    validate_schema(request.schema_version)?;
    validate_local_name(request.local_name.as_deref())?;

    // Reuse the established request validation and published-state handling.
    let mut base = resolve_agent_run_state(AgentRunStateResolveRequestWire {
        schema_version: request.schema_version,
        identity: request.identity.clone(),
        published_state: request.published_state.clone(),
        published_started_at: request.published_started_at.clone(),
        published_finished_at: request.published_finished_at.clone(),
        evidence_kind: AGENT_RUN_STATE_EVIDENCE_MISSING.to_string(),
        evidence_identity: None,
        evidence_state: None,
        evidence_started_at: None,
        evidence_finished_at: None,
    })?;

    let observations = request
        .observations
        .iter()
        .enumerate()
        .map(|(index, observation)| {
            classify_observation(
                index,
                observation,
                &request.identity,
                request.local_name.as_deref(),
            )
        })
        .collect::<Vec<_>>();

    let resolution = resolve_classified_observations(&request, &observations)?;
    base.selected_state = resolution.selected_state;
    base.selected_started_at = resolution.selected_started_at;
    base.selected_finished_at = resolution.selected_finished_at;
    base.changed = resolution.changed;
    base.reason = resolution.reason;

    Ok(AgentRunStateObservationsResolveResponseWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        selected_state: base.selected_state,
        selected_started_at: base.selected_started_at,
        selected_finished_at: base.selected_finished_at,
        changed: base.changed,
        reason: base.reason,
        observations,
    })
}

fn resolve_classified_observations(
    request: &AgentRunStateObservationsResolveRequestWire,
    observations: &[AgentRunStateObservationClassificationWire],
) -> Result<AgentRunStateResolveResponseWire, AgentRunStateError> {
    if observations.iter().any(|row| {
        row.evidence_kind == AGENT_RUN_STATE_EVIDENCE_IDENTITY_CONFLICT
    }) {
        return unchanged(request, AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT);
    }
    if observations
        .iter()
        .any(|row| row.evidence_kind == AGENT_RUN_STATE_EVIDENCE_UNREADABLE)
    {
        return unchanged(request, AGENT_RUN_STATE_REASON_UNREADABLE_EVIDENCE);
    }
    if observations
        .iter()
        .any(|row| row.evidence_kind == AGENT_RUN_STATE_EVIDENCE_MALFORMED)
    {
        return unchanged(request, AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE);
    }

    let terminals: Vec<_> = observations
        .iter()
        .filter(|row| {
            matches!(
                row.evidence_kind.as_str(),
                AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER
                    | AGENT_RUN_STATE_EVIDENCE_DISMISSED
            )
        })
        .collect();
    if !terminals.is_empty() {
        let Some(selected) = select_terminal_observation(request, &terminals)
        else {
            return unchanged(
                request,
                AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL,
            );
        };
        return resolve_agent_run_state(AgentRunStateResolveRequestWire {
            schema_version: request.schema_version,
            identity: request.identity.clone(),
            published_state: request.published_state.clone(),
            published_started_at: request.published_started_at.clone(),
            published_finished_at: request.published_finished_at.clone(),
            evidence_kind: selected.evidence_kind.clone(),
            evidence_identity: Some(request.identity.clone()),
            evidence_state: selected.evidence_state.clone(),
            evidence_started_at: selected.evidence_started_at.clone(),
            evidence_finished_at: selected.evidence_finished_at.clone(),
        });
    }

    if let Some(unfinished) = observations
        .iter()
        .find(|row| row.evidence_kind == AGENT_RUN_STATE_EVIDENCE_UNFINISHED)
    {
        return resolve_agent_run_state(AgentRunStateResolveRequestWire {
            schema_version: request.schema_version,
            identity: request.identity.clone(),
            published_state: request.published_state.clone(),
            published_started_at: request.published_started_at.clone(),
            published_finished_at: request.published_finished_at.clone(),
            evidence_kind: AGENT_RUN_STATE_EVIDENCE_UNFINISHED.to_string(),
            evidence_identity: Some(request.identity.clone()),
            evidence_state: None,
            evidence_started_at: unfinished.evidence_started_at.clone(),
            evidence_finished_at: unfinished.evidence_finished_at.clone(),
        });
    }

    unchanged(request, AGENT_RUN_STATE_REASON_MISSING_EVIDENCE)
}

fn select_terminal_observation<'a>(
    request: &AgentRunStateObservationsResolveRequestWire,
    rows: &[&'a AgentRunStateObservationClassificationWire],
) -> Option<&'a AgentRunStateObservationClassificationWire> {
    let states: Vec<_> = rows
        .iter()
        .filter_map(|row| row.evidence_state.as_deref())
        .collect();
    if states.len() != rows.len() {
        return None;
    }
    let unique_states: std::collections::BTreeSet<_> =
        states.iter().copied().collect();

    if unique_states.len() == 1 {
        return select_latest_same_state(rows);
    }

    let non_dismissed_states: std::collections::BTreeSet<_> = rows
        .iter()
        .filter(|row| row.evidence_kind != AGENT_RUN_STATE_EVIDENCE_DISMISSED)
        .filter_map(|row| row.evidence_state.as_deref())
        .collect();
    let dismissed: Vec<_> = rows
        .iter()
        .filter(|row| row.evidence_kind == AGENT_RUN_STATE_EVIDENCE_DISMISSED)
        .copied()
        .collect();
    if non_dismissed_states.len() != 1 || dismissed.is_empty() {
        return None;
    }

    // Dismissal can replace one earlier terminal observation only when its
    // timestamp proves it happened later than every competing terminal fact.
    let dismissal = select_latest_same_state(&dismissed)?;
    for row in rows
        .iter()
        .filter(|row| row.evidence_kind != AGENT_RUN_STATE_EVIDENCE_DISMISSED)
    {
        if !is_later(
            dismissal.evidence_finished_at.as_deref(),
            row.evidence_finished_at.as_deref(),
        ) {
            return None;
        }
    }
    if is_terminal_state(&request.published_state)
        && !is_later(
            dismissal.evidence_finished_at.as_deref(),
            request.published_finished_at.as_deref(),
        )
    {
        return None;
    }
    Some(dismissal)
}

fn select_latest_same_state<'a>(
    rows: &[&'a AgentRunStateObservationClassificationWire],
) -> Option<&'a AgentRunStateObservationClassificationWire> {
    let first = *rows.first()?;
    if rows
        .iter()
        .any(|row| row.evidence_state != first.evidence_state)
    {
        return None;
    }
    let starts: std::collections::BTreeSet<_> = rows
        .iter()
        .filter_map(|row| row.evidence_started_at.as_deref())
        .collect();
    if starts.len() > 1 {
        return None;
    }
    rows.iter().copied().max_by(|left, right| {
        compare_optional_timestamp(
            left.evidence_finished_at.as_deref(),
            right.evidence_finished_at.as_deref(),
        )
        // At the same time, prefer the marker outcome. A dismissal only
        // takes precedence when its timestamp proves it came later.
        .then_with(|| {
            observation_kind_priority(&left.evidence_kind)
                .cmp(&observation_kind_priority(&right.evidence_kind))
        })
    })
}

fn observation_kind_priority(kind: &str) -> u8 {
    match kind {
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER => 1,
        AGENT_RUN_STATE_EVIDENCE_DISMISSED => 0,
        _ => 0,
    }
}

fn compare_optional_timestamp(
    left: Option<&str>,
    right: Option<&str>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match (
            parse_runtime_timestamp(left),
            parse_runtime_timestamp(right),
        ) {
            (Some(left), Some(right)) => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => left.cmp(right),
        },
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn classify_observation(
    index: usize,
    observation: &AgentRunStateRawObservationWire,
    expected: &AgentRunIdentityWire,
    expected_local_name: Option<&str>,
) -> AgentRunStateObservationClassificationWire {
    let mut result = classification(index, &observation.source);
    let kind = match observation.read_status.as_str() {
        AGENT_RUN_STATE_OBSERVATION_MISSING => {
            result.evidence_kind = AGENT_RUN_STATE_EVIDENCE_MISSING.to_string();
            result.reason = AGENT_RUN_STATE_REASON_MISSING_EVIDENCE.to_string();
            return result;
        }
        AGENT_RUN_STATE_OBSERVATION_UNREADABLE => {
            result.evidence_kind =
                AGENT_RUN_STATE_EVIDENCE_UNREADABLE.to_string();
            result.reason =
                AGENT_RUN_STATE_REASON_UNREADABLE_EVIDENCE.to_string();
            return result;
        }
        AGENT_RUN_STATE_OBSERVATION_MALFORMED => {
            return malformed(result);
        }
        AGENT_RUN_STATE_OBSERVATION_PRESENT => observation.source.as_str(),
        _ => return malformed(result),
    };

    if observation.imported
        || !identity_claims_match(
            observation.identity_claims.as_ref(),
            expected,
            expected_local_name,
        )
        || !timestamps_valid(
            observation.started_at.as_deref(),
            observation.finished_at.as_deref(),
        )
    {
        if observation.imported
            || !identity_claims_match(
                observation.identity_claims.as_ref(),
                expected,
                expected_local_name,
            )
        {
            result.evidence_kind =
                AGENT_RUN_STATE_EVIDENCE_IDENTITY_CONFLICT.to_string();
            result.reason =
                AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT.to_string();
            return result;
        }
        return malformed(result);
    }

    match kind {
        AGENT_RUN_STATE_EVIDENCE_UNFINISHED => {
            if observation.outcome.is_some() || observation.state.is_some() {
                return malformed(result);
            }
            result.evidence_kind =
                AGENT_RUN_STATE_EVIDENCE_UNFINISHED.to_string();
            result.evidence_started_at = observation.started_at.clone();
            result.evidence_finished_at = observation.finished_at.clone();
            result.reason = AGENT_RUN_STATE_REASON_LIVE_EVIDENCE.to_string();
        }
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER => {
            let Some(state) = marker_state(observation) else {
                return malformed(result);
            };
            result.evidence_kind =
                AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER.to_string();
            result.evidence_state = Some(state.to_string());
            result.evidence_started_at = observation.started_at.clone();
            result.evidence_finished_at = observation.finished_at.clone();
            result.reason =
                AGENT_RUN_STATE_REASON_UPDATED_TERMINAL_MARKER.to_string();
        }
        AGENT_RUN_STATE_EVIDENCE_DISMISSED => {
            let state = match observation.state.as_deref() {
                None | Some("dismissed") => {
                    if observation.repeat_stopped {
                        "stopped"
                    } else {
                        "dismissed"
                    }
                }
                Some("stopped") => "stopped",
                _ => return malformed(result),
            };
            result.evidence_kind =
                AGENT_RUN_STATE_EVIDENCE_DISMISSED.to_string();
            result.evidence_state = Some(state.to_string());
            result.evidence_started_at = observation.started_at.clone();
            result.evidence_finished_at = observation.finished_at.clone();
            result.reason =
                AGENT_RUN_STATE_REASON_UPDATED_DISMISSED.to_string();
        }
        _ => return malformed(result),
    }
    result
}

fn marker_state(
    observation: &AgentRunStateRawObservationWire,
) -> Option<&'static str> {
    match observation.outcome.as_deref()? {
        "completed" if observation.repeat_stopped => Some("stopped"),
        "completed" | "noop" | "epic_approved" | "plan_committed"
        | "plan_rejected" => Some("completed"),
        "failed" | "killed" | "epic_launch_failed" => Some("failed"),
        "stopped" | "hijacked" => Some("stopped"),
        "monitored" => match observation.monitor_state.as_deref()? {
            "completed" => Some("completed"),
            "stopped" => Some("stopped"),
            "failed" | "timeout" | "timed_out" | "lost" | "error" => {
                Some("failed")
            }
            _ => None,
        },
        "gated" => match observation.gate_state.as_deref()? {
            "answered" | "completed" => Some("completed"),
            "stopped" => Some("stopped"),
            "failed" | "timeout" | "timed_out" | "expired" | "error" => {
                Some("failed")
            }
            _ => None,
        },
        _ => None,
    }
}

fn identity_claims_match(
    claims: Option<&AgentRunStateIdentityClaimsWire>,
    expected: &AgentRunIdentityWire,
    expected_local_name: Option<&str>,
) -> bool {
    let Some(claims) = claims else {
        // Exact artifact-directory membership plus metadata-derived identity
        // binds older markers that did not serialize explicit identity fields.
        return true;
    };
    let same = |claim: Option<&str>, expected: &str| {
        claim.is_none_or(|claim| !claim.trim().is_empty() && claim == expected)
    };
    same(claims.project.as_deref(), &expected.project)
        && same(claims.owner.as_deref(), &expected.owner)
        && same(claims.global_name.as_deref(), &expected.global_name)
        && same(claims.source_run_id.as_deref(), &expected.source_run_id)
        && same(
            claims.local_name.as_deref(),
            expected_local_name.unwrap_or(""),
        )
        && claims.name.as_deref().is_none_or(|name| {
            !name.trim().is_empty()
                && (name == expected.global_name
                    || expected_local_name == Some(name))
        })
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

fn validate_local_name(
    local_name: Option<&str>,
) -> Result<(), AgentRunStateError> {
    if local_name.is_some_and(|name| name.trim().is_empty()) {
        return Err(AgentRunStateError::InvalidRequest(
            "local_name must be non-empty when present".to_string(),
        ));
    }
    Ok(())
}

fn validate_schema(schema_version: u32) -> Result<(), AgentRunStateError> {
    if schema_version != AGENT_RUN_STATE_WIRE_SCHEMA_VERSION {
        return Err(AgentRunStateError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn classification(
    index: usize,
    source: &str,
) -> AgentRunStateObservationClassificationWire {
    AgentRunStateObservationClassificationWire {
        index: u32::try_from(index).unwrap_or(u32::MAX),
        source: source.to_string(),
        evidence_kind: AGENT_RUN_STATE_EVIDENCE_MALFORMED.to_string(),
        evidence_state: None,
        evidence_started_at: None,
        evidence_finished_at: None,
        reason: AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string(),
    }
}

fn malformed(
    mut result: AgentRunStateObservationClassificationWire,
) -> AgentRunStateObservationClassificationWire {
    result.evidence_kind = AGENT_RUN_STATE_EVIDENCE_MALFORMED.to_string();
    result.reason = AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE.to_string();
    result
}

fn unchanged(
    request: &AgentRunStateObservationsResolveRequestWire,
    reason: &str,
) -> Result<AgentRunStateResolveResponseWire, AgentRunStateError> {
    Ok(AgentRunStateResolveResponseWire {
        schema_version: request.schema_version,
        selected_state: request.published_state.clone(),
        selected_started_at: request.published_started_at.clone(),
        selected_finished_at: request.published_finished_at.clone(),
        changed: false,
        reason: reason.to_string(),
    })
}

fn is_terminal_state(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "stopped" | "dismissed")
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
