use super::wire::*;

pub fn advance_agent_run_state_schedule(
    request: AgentRunStateScheduleRequestWire,
) -> Result<AgentRunStateScheduleResponseWire, AgentRunStateError> {
    validate_schema(request.schema_version)?;
    let mut projects = request.projects;
    projects.sort_by(|left, right| left.identity.cmp(&right.identity));
    for project in &projects {
        validate_project_identity(&project.identity)?;
    }
    if projects
        .windows(2)
        .any(|pair| pair[0].identity == pair[1].identity)
    {
        return Err(AgentRunStateError::InvalidRequest(
            "duplicate project-owner identity".to_string(),
        ));
    }

    let mut updated_checkpoint = None;
    let mut last_selected = request.last_selected;
    if let Some(outcome) = request.outcome {
        validate_project_identity(&outcome.identity)?;
        if last_selected.as_ref() != Some(&outcome.identity) {
            return Err(AgentRunStateError::InvalidRequest(
                "outcome identity must match last_selected".to_string(),
            ));
        }
        let Some(project) = projects
            .iter_mut()
            .find(|project| project.identity == outcome.identity)
        else {
            return Err(AgentRunStateError::InvalidRequest(
                "outcome identity is not in projects".to_string(),
            ));
        };
        match outcome.result.as_str() {
            AGENT_RUN_STATE_OUTCOME_SUCCESS => {
                if !outcome.pass_complete && outcome.next_cursor.is_none() {
                    return Err(AgentRunStateError::InvalidRequest(
                        "successful incomplete outcome requires next_cursor"
                            .to_string(),
                    ));
                }
                project.cursor = if outcome.pass_complete {
                    None
                } else {
                    outcome.next_cursor
                };
                project.failure_count = 0;
                project.retry_after_ms = request.now_ms;
            }
            AGENT_RUN_STATE_OUTCOME_FAILURE => {
                project.failure_count = project.failure_count.saturating_add(1);
                let exponent = project.failure_count.saturating_sub(1).min(31);
                let delay = AGENT_RUN_STATE_BACKOFF_BASE_MS
                    .saturating_mul(1_u64 << exponent)
                    .min(AGENT_RUN_STATE_BACKOFF_MAX_MS);
                project.retry_after_ms = request.now_ms.saturating_add(delay);
            }
            _ => {
                return Err(AgentRunStateError::InvalidRequest(format!(
                    "unsupported outcome result {:?}",
                    outcome.result
                )))
            }
        }
        updated_checkpoint = Some(AgentRunStateProjectCheckpointWire {
            identity: project.identity.clone(),
            cursor: project.cursor.clone(),
            failure_count: project.failure_count,
            retry_after_ms: project.retry_after_ms,
        });
        last_selected = Some(project.identity.clone());
    }

    let eligible = projects
        .iter()
        .filter(|project| {
            project.enabled
                && project.configured
                && project.retry_after_ms <= request.now_ms
        })
        .collect::<Vec<_>>();
    let next_project = last_selected
        .as_ref()
        .and_then(|last| {
            eligible
                .iter()
                .copied()
                .find(|project| project.identity > *last)
        })
        .or_else(|| eligible.first().copied())
        .map(|project| project.identity.clone());

    Ok(AgentRunStateScheduleResponseWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        next_project,
        updated_checkpoint,
    })
}

fn validate_schema(schema_version: u32) -> Result<(), AgentRunStateError> {
    if schema_version != AGENT_RUN_STATE_WIRE_SCHEMA_VERSION {
        return Err(AgentRunStateError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn validate_project_identity(
    identity: &AgentRunStateProjectIdentityWire,
) -> Result<(), AgentRunStateError> {
    if identity.project.trim().is_empty() || identity.owner.trim().is_empty() {
        return Err(AgentRunStateError::InvalidRequest(
            "project and owner identities must be non-empty".to_string(),
        ));
    }
    Ok(())
}
