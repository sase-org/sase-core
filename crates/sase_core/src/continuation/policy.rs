//! Pure outcome-policy resolution for monitor continuations.

use serde::{Deserialize, Serialize};

use super::evidence::ContinuationEvidencePolicyWire;
use super::schema::{
    validate_non_empty_text, validate_optional_reference, validate_reference,
    validate_schema, validate_text, ContinuationError, MonitorOutcomeWire,
    CONTINUATION_WIRE_SCHEMA_VERSION, MAX_REF_BYTES, MAX_TEXT_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationActionWire {
    Continue,
    None,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationPolicyBranchWire {
    pub action: ContinuationActionWire,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub evidence_policy: Option<ContinuationEvidencePolicyWire>,
    #[serde(default)]
    pub completion_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationOutcomePolicyWire {
    pub completed: ContinuationPolicyBranchWire,
    pub failed: ContinuationPolicyBranchWire,
    pub timeout: ContinuationPolicyBranchWire,
    pub stopped: ContinuationPolicyBranchWire,
    pub lost: ContinuationPolicyBranchWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationPolicyResolutionRequestWire {
    pub schema_version: u32,
    pub outcome: MonitorOutcomeWire,
    #[serde(default)]
    pub explicit_policy: Option<ContinuationOutcomePolicyWire>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub shared_next: Option<String>,
    #[serde(default)]
    pub shared_model: Option<String>,
    #[serde(default)]
    pub inherited_model: Option<String>,
    #[serde(default)]
    pub inherited_effort: Option<String>,
    #[serde(default)]
    pub prepared_completion_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationPolicyDecisionWire {
    pub schema_version: u32,
    pub outcome: MonitorOutcomeWire,
    pub branch: String,
    pub action: ContinuationActionWire,
    pub evidence_policy: ContinuationEvidencePolicyWire,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub completion_ref: Option<String>,
    pub launchable: bool,
    #[serde(default)]
    pub reasons: Vec<String>,
}

pub fn resolve_continuation_policy(
    request: ContinuationPolicyResolutionRequestWire,
) -> Result<ContinuationPolicyDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationPolicyResolutionRequestWire",
    )?;
    validate_request_text(&request)?;
    if request.explicit_policy.is_some() && request.profile.is_some() {
        return Err(ContinuationError::validation(
            "explicit_policy and profile are mutually exclusive",
        ));
    }

    let (branch, mut selected, mut reasons) =
        if let Some(policy) = &request.explicit_policy {
            validate_policy(policy)?;
            (
                branch_name(request.outcome),
                branch_for_outcome(policy, request.outcome).clone(),
                vec!["explicit_policy".to_string()],
            )
        } else if request.profile.as_deref() == Some("verify") {
            verify_profile_branch(&request)?
        } else if let Some(profile) = request.profile.as_deref() {
            return Err(ContinuationError::validation(format!(
                "unsupported continuation policy profile {profile:?}"
            )));
        } else {
            default_branch(&request)
        };

    apply_shared_route(&mut selected, &request);
    validate_branch(&selected, &branch)?;
    if selected.action == ContinuationActionWire::Complete {
        if request.outcome != MonitorOutcomeWire::Completed {
            return Err(ContinuationError::validation(
                "complete action is legal only for completed monitor results",
            ));
        }
        if selected
            .completion_ref
            .as_ref()
            .or(request.prepared_completion_ref.as_ref())
            .is_none()
        {
            return Err(ContinuationError::validation(
                "complete action requires a prepared completion reference",
            ));
        }
    }

    let completion_ref = selected
        .completion_ref
        .or_else(|| request.prepared_completion_ref.clone());
    let launchable = selected.action == ContinuationActionWire::Continue
        && selected.next_action.is_some();
    if selected.action == ContinuationActionWire::Complete {
        reasons.push("prepared_completion_required".to_string());
    }
    if matches!(
        request.outcome,
        MonitorOutcomeWire::Stopped | MonitorOutcomeWire::Lost
    ) && selected.action == ContinuationActionWire::None
    {
        reasons.push(
            "cancelled_or_lost_outcome_does_not_auto_continue".to_string(),
        );
    }

    Ok(ContinuationPolicyDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        outcome: request.outcome,
        branch,
        action: selected.action,
        evidence_policy: selected
            .evidence_policy
            .unwrap_or(ContinuationEvidencePolicyWire::Auto),
        next_action: selected.next_action,
        model: selected.model,
        effort: selected.effort,
        completion_ref,
        launchable,
        reasons,
    })
}

fn validate_request_text(
    request: &ContinuationPolicyResolutionRequestWire,
) -> Result<(), ContinuationError> {
    if let Some(profile) = &request.profile {
        validate_reference(profile, "profile")?;
    }
    validate_optional_reference(&request.shared_model, "shared_model")?;
    validate_optional_reference(&request.inherited_model, "inherited_model")?;
    validate_optional_reference(&request.inherited_effort, "inherited_effort")?;
    validate_optional_reference(
        &request.prepared_completion_ref,
        "prepared_completion_ref",
    )?;
    if let Some(next) = &request.shared_next {
        validate_non_empty_text(next, "shared_next", MAX_TEXT_BYTES)?;
    }
    Ok(())
}

fn validate_policy(
    policy: &ContinuationOutcomePolicyWire,
) -> Result<(), ContinuationError> {
    validate_branch(&policy.completed, "completed")?;
    validate_branch(&policy.failed, "failed")?;
    validate_branch(&policy.timeout, "timeout")?;
    validate_branch(&policy.stopped, "stopped")?;
    validate_branch(&policy.lost, "lost")?;
    Ok(())
}

fn validate_branch(
    branch: &ContinuationPolicyBranchWire,
    field: &str,
) -> Result<(), ContinuationError> {
    if let Some(next) = &branch.next_action {
        validate_non_empty_text(
            next,
            &format!("{field}.next_action"),
            MAX_TEXT_BYTES,
        )?;
    }
    validate_optional_reference(&branch.model, &format!("{field}.model"))?;
    validate_optional_reference(&branch.effort, &format!("{field}.effort"))?;
    validate_optional_reference(
        &branch.completion_ref,
        &format!("{field}.completion_ref"),
    )?;
    if branch.action == ContinuationActionWire::Continue
        && branch.next_action.is_none()
    {
        return Err(ContinuationError::validation(format!(
            "{field}.next_action is required for continue actions"
        )));
    }
    if branch.action != ContinuationActionWire::Complete
        && branch.completion_ref.is_some()
    {
        return Err(ContinuationError::validation(format!(
            "{field}.completion_ref is only valid for complete actions"
        )));
    }
    Ok(())
}

fn branch_for_outcome(
    policy: &ContinuationOutcomePolicyWire,
    outcome: MonitorOutcomeWire,
) -> &ContinuationPolicyBranchWire {
    match outcome {
        MonitorOutcomeWire::Completed => &policy.completed,
        MonitorOutcomeWire::Failed => &policy.failed,
        MonitorOutcomeWire::Timeout => &policy.timeout,
        MonitorOutcomeWire::Stopped => &policy.stopped,
        MonitorOutcomeWire::Lost => &policy.lost,
        MonitorOutcomeWire::Unknown => &policy.failed,
    }
}

fn branch_name(outcome: MonitorOutcomeWire) -> String {
    match outcome {
        MonitorOutcomeWire::Completed => "completed",
        MonitorOutcomeWire::Failed => "failed",
        MonitorOutcomeWire::Timeout => "timeout",
        MonitorOutcomeWire::Stopped => "stopped",
        MonitorOutcomeWire::Lost => "lost",
        MonitorOutcomeWire::Unknown => "unknown",
    }
    .to_string()
}

fn default_branch(
    request: &ContinuationPolicyResolutionRequestWire,
) -> (String, ContinuationPolicyBranchWire, Vec<String>) {
    if let Some(next) = &request.shared_next {
        (
            branch_name(request.outcome),
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::Continue,
                next_action: Some(next.clone()),
                model: request.shared_model.clone(),
                effort: request.inherited_effort.clone(),
                evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
                completion_ref: None,
            },
            vec!["shared_next_default".to_string()],
        )
    } else {
        (
            branch_name(request.outcome),
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
                completion_ref: None,
            },
            vec!["no_successor_default".to_string()],
        )
    }
}

fn verify_profile_branch(
    request: &ContinuationPolicyResolutionRequestWire,
) -> Result<
    (String, ContinuationPolicyBranchWire, Vec<String>),
    ContinuationError,
> {
    let branch = branch_name(request.outcome);
    let recovery_next = request.shared_next.clone().unwrap_or_else(|| {
        "Inspect the monitor result, repair any failed or timed-out verification, and finish the original task."
            .to_string()
    });
    let selected = match request.outcome {
        MonitorOutcomeWire::Completed => {
            if let Some(completion_ref) = &request.prepared_completion_ref {
                ContinuationPolicyBranchWire {
                    action: ContinuationActionWire::Complete,
                    next_action: None,
                    model: None,
                    effort: None,
                    evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
                    completion_ref: Some(completion_ref.clone()),
                }
            } else if let Some(next) = &request.shared_next {
                ContinuationPolicyBranchWire {
                    action: ContinuationActionWire::Continue,
                    next_action: Some(next.clone()),
                    model: request.shared_model.clone(),
                    effort: request.inherited_effort.clone(),
                    evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
                    completion_ref: None,
                }
            } else {
                ContinuationPolicyBranchWire {
                    action: ContinuationActionWire::None,
                    next_action: None,
                    model: None,
                    effort: None,
                    evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
                    completion_ref: None,
                }
            }
        }
        MonitorOutcomeWire::Failed
        | MonitorOutcomeWire::Timeout
        | MonitorOutcomeWire::Unknown => ContinuationPolicyBranchWire {
            action: ContinuationActionWire::Continue,
            next_action: Some(recovery_next),
            model: request.shared_model.clone(),
            effort: request.inherited_effort.clone(),
            evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
            completion_ref: None,
        },
        MonitorOutcomeWire::Stopped | MonitorOutcomeWire::Lost => {
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: Some(ContinuationEvidencePolicyWire::Auto),
                completion_ref: None,
            }
        }
    };
    Ok((branch, selected, vec!["verify_profile".to_string()]))
}

fn apply_shared_route(
    branch: &mut ContinuationPolicyBranchWire,
    request: &ContinuationPolicyResolutionRequestWire,
) {
    if branch.action != ContinuationActionWire::Continue {
        return;
    }
    if branch.model.is_none() {
        branch.model = request
            .shared_model
            .clone()
            .or_else(|| request.inherited_model.clone());
    }
    if branch.effort.is_none() {
        branch.effort = request.inherited_effort.clone();
    }
}

#[allow(dead_code)]
fn _validate_label(value: &str, field: &str) -> Result<(), ContinuationError> {
    validate_text(value, field, MAX_REF_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        outcome: MonitorOutcomeWire,
    ) -> ContinuationPolicyResolutionRequestWire {
        ContinuationPolicyResolutionRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            outcome,
            explicit_policy: None,
            profile: None,
            shared_next: None,
            shared_model: None,
            inherited_model: Some("gpt-5-codex".to_string()),
            inherited_effort: Some("high".to_string()),
            prepared_completion_ref: None,
        }
    }

    #[test]
    fn verify_profile_completed_with_prepared_intent_completes() {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.profile = Some("verify".to_string());
        req.prepared_completion_ref =
            Some("file:explicit:completion".to_string());

        let decision = resolve_continuation_policy(req).unwrap();

        assert_eq!(decision.action, ContinuationActionWire::Complete);
        assert_eq!(
            decision.completion_ref,
            Some("file:explicit:completion".to_string())
        );
        assert!(!decision.launchable);
    }

    #[test]
    fn verify_profile_failure_gets_recovery_next_action() {
        let mut req = request(MonitorOutcomeWire::Failed);
        req.profile = Some("verify".to_string());

        let decision = resolve_continuation_policy(req).unwrap();

        assert_eq!(decision.action, ContinuationActionWire::Continue);
        assert!(decision.launchable);
        assert!(decision.next_action.unwrap().contains("repair"));
    }

    #[test]
    fn explicit_complete_without_prepared_ref_is_rejected() {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.explicit_policy = Some(ContinuationOutcomePolicyWire {
            completed: ContinuationPolicyBranchWire {
                action: ContinuationActionWire::Complete,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            },
            failed: ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            },
            timeout: ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            },
            stopped: ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            },
            lost: ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            },
        });

        let err = resolve_continuation_policy(req).unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("prepared completion"));
    }
}
