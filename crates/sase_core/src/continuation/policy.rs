//! Pure outcome-policy resolution for monitor continuations.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::effort::{is_valid_effort, split_model_effort};

use super::evidence::ContinuationEvidencePolicyWire;
use super::schema::{
    validate_non_empty_text, validate_optional_reference, validate_reference,
    validate_schema, ContinuationError, MonitorOutcomeWire,
    CONTINUATION_WIRE_SCHEMA_VERSION, MAX_TEXT_BYTES,
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

impl Default for ContinuationPolicyBranchWire {
    fn default() -> Self {
        Self {
            action: ContinuationActionWire::None,
            next_action: None,
            model: None,
            effort: None,
            evidence_policy: None,
            completion_ref: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationOutcomePolicyWire {
    pub completed: ContinuationPolicyBranchWire,
    pub failed: ContinuationPolicyBranchWire,
    pub timeout: ContinuationPolicyBranchWire,
    #[serde(default)]
    pub stopped: ContinuationPolicyBranchWire,
    #[serde(default)]
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
    pub cli_next: Option<String>,
    #[serde(default)]
    pub cli_model: Option<String>,
    #[serde(default)]
    pub cli_effort: Option<String>,
    #[serde(default)]
    pub cli_evidence: Option<ContinuationEvidencePolicyWire>,
    #[serde(default)]
    pub inherited_model: Option<String>,
    #[serde(default)]
    pub inherited_effort: Option<String>,
    #[serde(default)]
    pub prepared_completion_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationPolicyFreezeRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub explicit_policy: Option<ContinuationOutcomePolicyWire>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub shared_next: Option<String>,
    #[serde(default)]
    pub shared_model: Option<String>,
    #[serde(default)]
    pub cli_next: Option<String>,
    #[serde(default)]
    pub cli_model: Option<String>,
    #[serde(default)]
    pub cli_effort: Option<String>,
    #[serde(default)]
    pub cli_evidence: Option<ContinuationEvidencePolicyWire>,
    #[serde(default)]
    pub inherited_model: Option<String>,
    #[serde(default)]
    pub inherited_effort: Option<String>,
    #[serde(default)]
    pub prepared_completion_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationFrozenBranchesWire {
    pub completed: ContinuationPolicyDecisionWire,
    pub failed: ContinuationPolicyDecisionWire,
    pub timeout: ContinuationPolicyDecisionWire,
    pub stopped: ContinuationPolicyDecisionWire,
    pub lost: ContinuationPolicyDecisionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationFrozenPolicyWire {
    pub schema_version: u32,
    pub fingerprint: String,
    #[serde(default)]
    pub explicit_policy: Option<ContinuationOutcomePolicyWire>,
    #[serde(default)]
    pub profile: Option<String>,
    pub branches: ContinuationFrozenBranchesWire,
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

pub fn validate_continuation_policy(
    policy: ContinuationOutcomePolicyWire,
) -> Result<ContinuationOutcomePolicyWire, ContinuationError> {
    validate_policy(&policy)?;
    Ok(policy)
}

pub fn freeze_continuation_policy(
    request: ContinuationPolicyFreezeRequestWire,
) -> Result<ContinuationFrozenPolicyWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationPolicyFreezeRequestWire",
    )?;
    let normalized = normalize_freeze_request(request)?;
    let fingerprint = fingerprint_freeze_request(&normalized)?;
    let explicit_policy = normalized.explicit_policy.clone();
    let profile = normalized.profile.clone();
    let branches = ContinuationFrozenBranchesWire {
        completed: resolve_continuation_policy(resolution_request(
            &normalized,
            MonitorOutcomeWire::Completed,
        ))?,
        failed: resolve_continuation_policy(resolution_request(
            &normalized,
            MonitorOutcomeWire::Failed,
        ))?,
        timeout: resolve_continuation_policy(resolution_request(
            &normalized,
            MonitorOutcomeWire::Timeout,
        ))?,
        stopped: resolve_continuation_policy(resolution_request(
            &normalized,
            MonitorOutcomeWire::Stopped,
        ))?,
        lost: resolve_continuation_policy(resolution_request(
            &normalized,
            MonitorOutcomeWire::Lost,
        ))?,
    };
    Ok(ContinuationFrozenPolicyWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        fingerprint,
        explicit_policy,
        profile,
        branches,
    })
}

pub fn resolve_continuation_policy(
    request: ContinuationPolicyResolutionRequestWire,
) -> Result<ContinuationPolicyDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationPolicyResolutionRequestWire",
    )?;
    let request = normalize_resolution_request(request)?;
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

    apply_route_precedence(&mut selected, &request);
    coerce_cancelled_outcome(&mut selected, request.outcome, &mut reasons);
    validate_resolved_branch(&selected, &branch)?;
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

fn normalize_freeze_request(
    mut request: ContinuationPolicyFreezeRequestWire,
) -> Result<ContinuationPolicyFreezeRequestWire, ContinuationError> {
    request.profile = normalize_optional_text(request.profile);
    request.shared_next = normalize_optional_text(request.shared_next);
    request.shared_model = normalize_optional_text(request.shared_model);
    request.cli_next = normalize_optional_text(request.cli_next);
    request.cli_model = normalize_optional_text(request.cli_model);
    request.cli_effort = normalize_optional_text(request.cli_effort);
    request.inherited_model = normalize_optional_text(request.inherited_model);
    request.inherited_effort =
        normalize_optional_text(request.inherited_effort);
    request.prepared_completion_ref =
        normalize_optional_text(request.prepared_completion_ref);
    if let Some(policy) = request.explicit_policy.take() {
        request.explicit_policy = Some(validate_continuation_policy(policy)?);
    }
    Ok(request)
}

fn normalize_resolution_request(
    mut request: ContinuationPolicyResolutionRequestWire,
) -> Result<ContinuationPolicyResolutionRequestWire, ContinuationError> {
    request.profile = normalize_optional_text(request.profile);
    request.shared_next = normalize_optional_text(request.shared_next);
    request.shared_model = normalize_optional_text(request.shared_model);
    request.cli_next = normalize_optional_text(request.cli_next);
    request.cli_model = normalize_optional_text(request.cli_model);
    request.cli_effort = normalize_optional_text(request.cli_effort);
    request.inherited_model = normalize_optional_text(request.inherited_model);
    request.inherited_effort =
        normalize_optional_text(request.inherited_effort);
    request.prepared_completion_ref =
        normalize_optional_text(request.prepared_completion_ref);
    Ok(request)
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn resolution_request(
    request: &ContinuationPolicyFreezeRequestWire,
    outcome: MonitorOutcomeWire,
) -> ContinuationPolicyResolutionRequestWire {
    ContinuationPolicyResolutionRequestWire {
        schema_version: request.schema_version,
        outcome,
        explicit_policy: request.explicit_policy.clone(),
        profile: request.profile.clone(),
        shared_next: request.shared_next.clone(),
        shared_model: request.shared_model.clone(),
        cli_next: request.cli_next.clone(),
        cli_model: request.cli_model.clone(),
        cli_effort: request.cli_effort.clone(),
        cli_evidence: request.cli_evidence,
        inherited_model: request.inherited_model.clone(),
        inherited_effort: request.inherited_effort.clone(),
        prepared_completion_ref: request.prepared_completion_ref.clone(),
    }
}

fn fingerprint_freeze_request(
    request: &ContinuationPolicyFreezeRequestWire,
) -> Result<String, ContinuationError> {
    let encoded = serde_json::to_vec(request).map_err(|error| {
        ContinuationError::validation(format!(
            "could not canonicalize outcome policy: {error}"
        ))
    })?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(encoded))))
}

fn validate_request_text(
    request: &ContinuationPolicyResolutionRequestWire,
) -> Result<(), ContinuationError> {
    if let Some(profile) = &request.profile {
        validate_reference(profile, "profile")?;
    }
    validate_optional_model(&request.shared_model, "shared_model")?;
    validate_optional_model(&request.cli_model, "cli_model")?;
    validate_optional_model(&request.inherited_model, "inherited_model")?;
    validate_optional_effort(&request.cli_effort, "cli_effort")?;
    validate_optional_effort(&request.inherited_effort, "inherited_effort")?;
    validate_optional_reference(
        &request.prepared_completion_ref,
        "prepared_completion_ref",
    )?;
    if let Some(next) = &request.shared_next {
        validate_non_empty_text(next, "shared_next", MAX_TEXT_BYTES)?;
    }
    if let Some(next) = &request.cli_next {
        validate_non_empty_text(next, "cli_next", MAX_TEXT_BYTES)?;
    }
    Ok(())
}

fn validate_policy(
    policy: &ContinuationOutcomePolicyWire,
) -> Result<(), ContinuationError> {
    validate_authored_branch(&policy.completed, "completed")?;
    validate_authored_branch(&policy.failed, "failed")?;
    validate_authored_branch(&policy.timeout, "timeout")?;
    validate_authored_branch(&policy.stopped, "stopped")?;
    validate_authored_branch(&policy.lost, "lost")?;
    if policy.failed.action == ContinuationActionWire::Complete
        || policy.timeout.action == ContinuationActionWire::Complete
        || policy.stopped.action == ContinuationActionWire::Complete
        || policy.lost.action == ContinuationActionWire::Complete
    {
        return Err(ContinuationError::validation(
            "complete action is legal only on the completed branch",
        ));
    }
    Ok(())
}

fn validate_authored_branch(
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
    validate_optional_model(&branch.model, &format!("{field}.model"))?;
    validate_optional_effort(&branch.effort, &format!("{field}.effort"))?;
    validate_optional_reference(
        &branch.completion_ref,
        &format!("{field}.completion_ref"),
    )?;
    if branch.action != ContinuationActionWire::Complete
        && branch.completion_ref.is_some()
    {
        return Err(ContinuationError::validation(format!(
            "{field}.completion_ref is only valid for complete actions"
        )));
    }
    Ok(())
}

fn validate_resolved_branch(
    branch: &ContinuationPolicyBranchWire,
    field: &str,
) -> Result<(), ContinuationError> {
    validate_authored_branch(branch, field)?;
    if branch.action == ContinuationActionWire::Continue
        && branch.next_action.is_none()
    {
        return Err(ContinuationError::validation(format!(
            "{field}.next_action is required for continue actions"
        )));
    }
    Ok(())
}

fn validate_optional_model(
    value: &Option<String>,
    field: &str,
) -> Result<(), ContinuationError> {
    if let Some(raw) = value {
        let (model, effort) = split_model_effort(raw);
        validate_reference(model, field)?;
        if let Some(effort) = effort {
            validate_effort_value(effort, field)?;
        }
    }
    Ok(())
}

fn validate_optional_effort(
    value: &Option<String>,
    field: &str,
) -> Result<(), ContinuationError> {
    if let Some(effort) = value {
        validate_effort_value(effort, field)?;
    }
    Ok(())
}

fn validate_effort_value(
    value: &str,
    field: &str,
) -> Result<(), ContinuationError> {
    if is_valid_effort(value) {
        Ok(())
    } else {
        Err(ContinuationError::validation(format!(
            "{field} is not a canonical reasoning-effort level"
        )))
    }
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
    if request.shared_next.is_some() || request.cli_next.is_some() {
        (
            branch_name(request.outcome),
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::Continue,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
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
                evidence_policy: None,
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
    let recovery_next = request
        .cli_next
        .clone()
        .or_else(|| request.shared_next.clone())
        .unwrap_or_else(|| {
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
                    evidence_policy: None,
                    completion_ref: Some(completion_ref.clone()),
                }
            } else if request.shared_next.is_some()
                || request.cli_next.is_some()
            {
                ContinuationPolicyBranchWire {
                    action: ContinuationActionWire::Continue,
                    next_action: None,
                    model: None,
                    effort: None,
                    evidence_policy: None,
                    completion_ref: None,
                }
            } else {
                ContinuationPolicyBranchWire {
                    action: ContinuationActionWire::None,
                    next_action: None,
                    model: None,
                    effort: None,
                    evidence_policy: None,
                    completion_ref: None,
                }
            }
        }
        MonitorOutcomeWire::Failed
        | MonitorOutcomeWire::Timeout
        | MonitorOutcomeWire::Unknown => ContinuationPolicyBranchWire {
            action: ContinuationActionWire::Continue,
            next_action: Some(recovery_next),
            model: None,
            effort: None,
            evidence_policy: None,
            completion_ref: None,
        },
        MonitorOutcomeWire::Stopped | MonitorOutcomeWire::Lost => {
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::None,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            }
        }
    };
    Ok((branch, selected, vec!["verify_profile".to_string()]))
}

fn apply_route_precedence(
    branch: &mut ContinuationPolicyBranchWire,
    request: &ContinuationPolicyResolutionRequestWire,
) {
    if let Some(evidence) = request.cli_evidence {
        branch.evidence_policy = Some(evidence);
    }
    if branch.action == ContinuationActionWire::Continue {
        if let Some(next) = &request.cli_next {
            branch.next_action = Some(next.clone());
        } else if branch.next_action.is_none() {
            branch.next_action = request.shared_next.clone();
        }
        apply_model_and_effort(branch, request);
    }
    if branch.evidence_policy.is_none() {
        branch.evidence_policy = Some(ContinuationEvidencePolicyWire::Auto);
    }
}

fn apply_model_and_effort(
    branch: &mut ContinuationPolicyBranchWire,
    request: &ContinuationPolicyResolutionRequestWire,
) {
    if let Some(cli_model) = &request.cli_model {
        let (model, effort) = split_owned_model(cli_model);
        branch.model = Some(model);
        if let Some(effort) = effort {
            branch.effort = Some(effort);
        }
    } else if let Some(policy_model) = branch.model.clone() {
        let (model, effort) = split_owned_model(&policy_model);
        branch.model = Some(model);
        if branch.effort.is_none() {
            branch.effort = effort;
        }
    } else if let Some(shared_model) = &request.shared_model {
        let (model, effort) = split_owned_model(shared_model);
        branch.model = Some(model);
        if branch.effort.is_none() {
            branch.effort = effort;
        }
    } else if let Some(inherited_model) = &request.inherited_model {
        let (model, effort) = split_owned_model(inherited_model);
        branch.model = Some(model);
        if branch.effort.is_none() {
            branch.effort = effort;
        }
    }
    if let Some(effort) = &request.cli_effort {
        branch.effort = Some(effort.clone());
    } else if branch.effort.is_none() {
        branch.effort = request.inherited_effort.clone();
    }
}

fn split_owned_model(value: &str) -> (String, Option<String>) {
    let (model, effort) = split_model_effort(value);
    (model.to_string(), effort.map(str::to_string))
}

fn coerce_cancelled_outcome(
    branch: &mut ContinuationPolicyBranchWire,
    outcome: MonitorOutcomeWire,
    reasons: &mut Vec<String>,
) {
    if !matches!(
        outcome,
        MonitorOutcomeWire::Stopped | MonitorOutcomeWire::Lost
    ) {
        return;
    }
    if branch.action != ContinuationActionWire::None
        || branch.next_action.is_some()
    {
        reasons.push(
            "cancelled_or_lost_outcome_does_not_auto_continue".to_string(),
        );
    }
    *branch = ContinuationPolicyBranchWire {
        action: ContinuationActionWire::None,
        next_action: None,
        model: None,
        effort: None,
        evidence_policy: branch
            .evidence_policy
            .or(Some(ContinuationEvidencePolicyWire::Auto)),
        completion_ref: None,
    };
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
            cli_next: None,
            cli_model: None,
            cli_effort: None,
            cli_evidence: None,
            inherited_model: Some("gpt-5-codex".to_string()),
            inherited_effort: Some("high".to_string()),
            prepared_completion_ref: None,
        }
    }

    fn none_branch() -> ContinuationPolicyBranchWire {
        ContinuationPolicyBranchWire::default()
    }

    fn continue_branch(
        next_action: &str,
        model: Option<&str>,
    ) -> ContinuationPolicyBranchWire {
        ContinuationPolicyBranchWire {
            action: ContinuationActionWire::Continue,
            next_action: Some(next_action.to_string()),
            model: model.map(str::to_string),
            effort: None,
            evidence_policy: None,
            completion_ref: None,
        }
    }

    fn policy_with(
        completed: ContinuationPolicyBranchWire,
        failed: ContinuationPolicyBranchWire,
        timeout: ContinuationPolicyBranchWire,
    ) -> ContinuationOutcomePolicyWire {
        ContinuationOutcomePolicyWire {
            completed,
            failed,
            timeout,
            stopped: continue_branch("should not launch on stop", None),
            lost: continue_branch("should not launch on lost", None),
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
    fn verify_profile_without_next_or_completion_is_fire_and_forget_on_success()
    {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.profile = Some("verify".to_string());

        let decision = resolve_continuation_policy(req).unwrap();

        assert_eq!(decision.action, ContinuationActionWire::None);
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
        assert_eq!(decision.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(decision.effort.as_deref(), Some("high"));
    }

    #[test]
    fn explicit_complete_without_prepared_ref_is_rejected() {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.explicit_policy = Some(policy_with(
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::Complete,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: None,
            },
            none_branch(),
            none_branch(),
        ));

        let err = resolve_continuation_policy(req).unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("prepared completion"));
    }

    #[test]
    fn policy_none_wins_over_shared_next() {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.shared_next = Some("shared follow-up".to_string());
        req.explicit_policy = Some(policy_with(
            none_branch(),
            continue_branch("repair failure", Some("opus@high")),
            none_branch(),
        ));

        let completed = resolve_continuation_policy(req.clone()).unwrap();
        req.outcome = MonitorOutcomeWire::Failed;
        let failed = resolve_continuation_policy(req).unwrap();

        assert_eq!(completed.action, ContinuationActionWire::None);
        assert!(!completed.launchable);
        assert_eq!(failed.action, ContinuationActionWire::Continue);
        assert_eq!(failed.next_action.as_deref(), Some("repair failure"));
        assert_eq!(failed.model.as_deref(), Some("opus"));
        assert_eq!(failed.effort.as_deref(), Some("high"));
    }

    #[test]
    fn policy_next_action_wins_over_shared_next() {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.explicit_policy = Some(policy_with(
            continue_branch("from policy", None),
            none_branch(),
            none_branch(),
        ));
        req.shared_next = Some("from shared".to_string());

        let decision = resolve_continuation_policy(req).unwrap();

        assert_eq!(decision.next_action.as_deref(), Some("from policy"));
        assert!(decision.launchable);
    }

    #[test]
    fn cli_evidence_and_model_override_policy_and_inherited_route() {
        let mut req = request(MonitorOutcomeWire::Completed);
        req.explicit_policy = Some(policy_with(
            continue_branch("keep going", Some("sonnet")),
            none_branch(),
            none_branch(),
        ));
        req.cli_model = Some("opus@high".to_string());
        req.cli_evidence = Some(ContinuationEvidencePolicyWire::None);
        req.inherited_model = Some("gpt-5-codex".to_string());

        let decision = resolve_continuation_policy(req).unwrap();

        assert_eq!(decision.model.as_deref(), Some("opus"));
        assert_eq!(decision.effort.as_deref(), Some("high"));
        assert_eq!(
            decision.evidence_policy,
            ContinuationEvidencePolicyWire::None
        );
    }

    #[test]
    fn stopped_and_lost_never_launch_even_when_policy_requests_continue() {
        let mut req = request(MonitorOutcomeWire::Stopped);
        req.explicit_policy = Some(policy_with(
            continue_branch("success next", None),
            continue_branch("failure next", None),
            continue_branch("timeout next", None),
        ));
        req.shared_next = Some("shared".to_string());

        let stopped = resolve_continuation_policy(req.clone()).unwrap();
        req.outcome = MonitorOutcomeWire::Lost;
        let lost = resolve_continuation_policy(req).unwrap();

        assert_eq!(stopped.action, ContinuationActionWire::None);
        assert!(!stopped.launchable);
        assert!(stopped.reasons.iter().any(|reason| {
            reason == "cancelled_or_lost_outcome_does_not_auto_continue"
        }));
        assert_eq!(lost.action, ContinuationActionWire::None);
        assert!(!lost.launchable);
    }

    #[test]
    fn omitted_next_stays_fire_and_forget() {
        let decision =
            resolve_continuation_policy(request(MonitorOutcomeWire::Completed))
                .unwrap();

        assert_eq!(decision.action, ContinuationActionWire::None);
        assert!(!decision.launchable);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason == "no_successor_default"));
    }

    #[test]
    fn freeze_persists_all_branches_and_content_fingerprint() {
        let request = ContinuationPolicyFreezeRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            explicit_policy: Some(policy_with(
                none_branch(),
                continue_branch("repair", Some("@small")),
                none_branch(),
            )),
            profile: None,
            shared_next: Some("shared".to_string()),
            shared_model: None,
            cli_next: None,
            cli_model: None,
            cli_effort: None,
            cli_evidence: Some(ContinuationEvidencePolicyWire::File),
            inherited_model: Some("gpt-5-codex".to_string()),
            inherited_effort: Some("high".to_string()),
            prepared_completion_ref: None,
        };

        let frozen = freeze_continuation_policy(request.clone()).unwrap();
        let again = freeze_continuation_policy(request).unwrap();

        assert_eq!(frozen.fingerprint, again.fingerprint);
        assert!(frozen.fingerprint.starts_with("sha256:"));
        assert_eq!(
            frozen.branches.completed.action,
            ContinuationActionWire::None
        );
        assert_eq!(
            frozen.branches.failed.action,
            ContinuationActionWire::Continue
        );
        assert_eq!(frozen.branches.failed.model.as_deref(), Some("@small"));
        assert_eq!(
            frozen.branches.failed.evidence_policy,
            ContinuationEvidencePolicyWire::File
        );
        assert_eq!(
            frozen.branches.stopped.action,
            ContinuationActionWire::None
        );
        assert_eq!(frozen.branches.lost.action, ContinuationActionWire::None);
    }

    #[test]
    fn freeze_fingerprint_changes_when_policy_content_changes() {
        let mut request = ContinuationPolicyFreezeRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            explicit_policy: Some(policy_with(
                none_branch(),
                none_branch(),
                none_branch(),
            )),
            profile: None,
            shared_next: None,
            shared_model: None,
            cli_next: None,
            cli_model: None,
            cli_effort: None,
            cli_evidence: None,
            inherited_model: None,
            inherited_effort: None,
            prepared_completion_ref: None,
        };
        let first = freeze_continuation_policy(request.clone()).unwrap();
        request.explicit_policy = Some(policy_with(
            continue_branch("now continue", None),
            none_branch(),
            none_branch(),
        ));
        let second = freeze_continuation_policy(request).unwrap();

        assert_ne!(first.fingerprint, second.fingerprint);
    }

    #[test]
    fn complete_on_failed_branch_is_rejected() {
        let err = validate_continuation_policy(policy_with(
            none_branch(),
            ContinuationPolicyBranchWire {
                action: ContinuationActionWire::Complete,
                next_action: None,
                model: None,
                effort: None,
                evidence_policy: None,
                completion_ref: Some("file:explicit:completion".to_string()),
            },
            none_branch(),
        ))
        .unwrap_err();

        assert!(err.message.contains("completed branch"));
    }
}
