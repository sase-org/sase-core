use super::wire::{
    ChopCheckpointPolicyWire, ChopDecisionRequestWire, ChopDecisionWire,
    ChopEngineError, ChopGuardConfigWire, ChopTriggerConfigWire,
    CHOP_ENGINE_SCHEMA_VERSION, CHOP_STATE_SCHEMA_VERSION,
};

/// Evaluate all inhibit guards followed by the configured trigger.
pub fn evaluate_chop_decision(
    request: &ChopDecisionRequestWire,
) -> Result<ChopDecisionWire, ChopEngineError> {
    validate_request(request)?;

    for guard in &request.inhibit_if {
        match guard {
            ChopGuardConfigWire::Changespec {
                name_prefix,
                statuses,
            } => {
                if let Some(changespec) =
                    request.changespecs.iter().find(|row| {
                        row.name.starts_with(name_prefix)
                            && status_matches(&row.status, statuses)
                    })
                {
                    return Ok(decision(
                        "skip",
                        format!(
                            "inhibited by ChangeSpec `{}` in status `{}`",
                            changespec.name, changespec.status
                        ),
                        Some("changespec"),
                    ));
                }
            }
            ChopGuardConfigWire::AgentHood { hood } => {
                if let Some(agent) = request.agents.iter().find(|row| {
                    row.active
                        && (row.hood.as_deref() == Some(hood.as_str())
                            || row.name == *hood
                            || row
                                .name
                                .strip_prefix(hood)
                                .is_some_and(|suffix| suffix.starts_with('.')))
                }) {
                    return Ok(decision(
                        "skip",
                        format!(
                            "inhibited by active agent `{}` in hood `{hood}`",
                            agent.name
                        ),
                        Some("agent_hood"),
                    ));
                }
            }
        }
    }

    match &request.trigger {
        ChopTriggerConfigWire::Always => {
            Ok(decision("fire", "always trigger selected", Some("always")))
        }
        ChopTriggerConfigWire::GitCommitsSince {
            project,
            threshold,
            checkpoint_policy,
        } => evaluate_git_trigger(
            request,
            project,
            *threshold,
            *checkpoint_policy,
        ),
    }
}

fn validate_request(
    request: &ChopDecisionRequestWire,
) -> Result<(), ChopEngineError> {
    if request.schema_version != CHOP_ENGINE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_ENGINE_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    if request.now.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            "$.now",
            "now must not be blank",
        ));
    }
    if request.checkpoint.schema_version != CHOP_STATE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "state_schema_version_mismatch",
            "$.checkpoint.schema_version",
            format!(
                "got {}, expected {CHOP_STATE_SCHEMA_VERSION}",
                request.checkpoint.schema_version
            ),
        ));
    }
    for (index, guard) in request.inhibit_if.iter().enumerate() {
        match guard {
            ChopGuardConfigWire::Changespec { statuses, .. } => {
                if statuses.iter().any(|status| status.trim().is_empty()) {
                    return Err(ChopEngineError::new(
                        "blank_value",
                        format!("$.inhibit_if[{index}].statuses"),
                        "guard statuses must not be blank",
                    ));
                }
            }
            ChopGuardConfigWire::AgentHood { hood }
                if hood.trim().is_empty() =>
            {
                return Err(ChopEngineError::new(
                    "blank_value",
                    format!("$.inhibit_if[{index}].hood"),
                    "agent hood must not be blank",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn status_matches(status: &str, configured: &[String]) -> bool {
    if configured.is_empty() {
        !matches!(status, "Submitted" | "Archived" | "Reverted")
    } else {
        configured.iter().any(|candidate| candidate == status)
    }
}

fn evaluate_git_trigger(
    request: &ChopDecisionRequestWire,
    project: &str,
    threshold: u64,
    checkpoint_policy: ChopCheckpointPolicyWire,
) -> Result<ChopDecisionWire, ChopEngineError> {
    if project.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            "$.trigger.project",
            "git.commits_since project must not be blank",
        ));
    }
    if threshold == 0 {
        return Err(ChopEngineError::new(
            "non_positive_threshold",
            "$.trigger.threshold",
            "git.commits_since threshold must be positive",
        ));
    }
    let Some(snapshot) = request.git.iter().find(|row| row.project == project)
    else {
        return Ok(decision(
            "check_error",
            format!("no git observation was provided for project `{project}`"),
            Some("git.commits_since"),
        ));
    };
    if snapshot.head.trim().is_empty() {
        return Ok(decision(
            "check_error",
            format!("git observation for project `{project}` has no HEAD"),
            Some("git.commits_since"),
        ));
    }

    let checkpoint_key = format!("git.commits_since:{project}");
    let checkpoint_found = snapshot.checkpoint_found
        || request.checkpoint.entries.contains_key(&checkpoint_key);
    let checkpoint_note = if checkpoint_found {
        ""
    } else {
        " (no prior checkpoint; using the host's recovery observation)"
    };
    let mut result = if snapshot.commits_since_checkpoint >= threshold {
        decision(
            "fire",
            format!(
                "{} commits observed for `{project}`; threshold is {threshold}{checkpoint_note}",
                snapshot.commits_since_checkpoint
            ),
            Some("git.commits_since"),
        )
    } else {
        decision(
            "skip",
            format!(
                "{} commits observed for `{project}`; threshold is {threshold}{checkpoint_note}",
                snapshot.commits_since_checkpoint
            ),
            Some("git.commits_since"),
        )
    };
    result.checkpoint_key = Some(checkpoint_key);
    result.checkpoint_cursor = Some(snapshot.head.clone());
    result.checkpoint_policy = Some(checkpoint_policy);
    Ok(result)
}

fn decision(
    outcome: &str,
    reason: impl Into<String>,
    provider: Option<&str>,
) -> ChopDecisionWire {
    ChopDecisionWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        outcome: outcome.to_string(),
        reason: reason.into(),
        provider: provider.map(str::to_string),
        checkpoint_key: None,
        checkpoint_cursor: None,
        checkpoint_policy: None,
    }
}
