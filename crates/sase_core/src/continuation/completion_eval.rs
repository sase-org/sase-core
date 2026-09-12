//! Eligibility evaluation for host-sealed conditional completion.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::completion::{
    consume_conditional_completion, invalidate_conditional_completion,
    render_conditional_completion_message, status_name, worktree_fingerprint,
    ConditionalCompletionIntentWire, ConditionalCompletionStatusWire,
    ExecutorCapabilityWire, RepositoryObservationWire, FIRST_PARTY_PROVIDERS,
};
use super::schema::{
    validate_command_part, validate_non_empty_text, validate_schema,
    validate_sha256, ContinuationError, DiagnosticStageStatusWire,
    DiagnosticStageWire, MonitorOutcomeWire, CONTINUATION_WIRE_SCHEMA_VERSION,
    MAX_COMMAND_PARTS, MAX_REF_BYTES, MAX_STAGES,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionEvaluateRequestWire {
    pub schema_version: u32,
    pub intent: ConditionalCompletionIntentWire,
    pub outcome: MonitorOutcomeWire,
    #[serde(default)]
    pub exit_code: Option<i32>,
    pub command: Vec<String>,
    pub observations: Vec<RepositoryObservationWire>,
    #[serde(default)]
    pub stages: Vec<DiagnosticStageWire>,
    #[serde(default)]
    pub executors: Vec<ExecutorCapabilityWire>,
    pub workspace_identity: String,
    pub original_workspace_identity: String,
    #[serde(default)]
    pub degraded_workspace: bool,
    pub current_plan_digest: String,
    #[serde(default)]
    pub current_obligation_ids: Vec<String>,
    #[serde(default)]
    pub substitutions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionDecisionWire {
    pub schema_version: u32,
    pub eligible: bool,
    pub action: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub rendered_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionConsumeRequestWire {
    pub schema_version: u32,
    pub intent: ConditionalCompletionIntentWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionMessageRequestWire {
    pub schema_version: u32,
    pub success_message: String,
    #[serde(default)]
    pub substitutions: BTreeMap<String, String>,
}

pub fn evaluate_conditional_completion(
    request: ConditionalCompletionEvaluateRequestWire,
) -> Result<ConditionalCompletionDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionEvaluateRequestWire",
    )?;
    validate_non_empty_text(
        &request.workspace_identity,
        "workspace_identity",
        MAX_REF_BYTES,
    )?;
    validate_non_empty_text(
        &request.original_workspace_identity,
        "original_workspace_identity",
        MAX_REF_BYTES,
    )?;
    validate_sha256(&request.current_plan_digest, "current_plan_digest")?;
    if request.command.is_empty() {
        return Err(ContinuationError::validation(
            "command must contain at least one argv part",
        ));
    }
    if request.command.len() > MAX_COMMAND_PARTS {
        return Err(ContinuationError::validation(format!(
            "command has {} parts; maximum is {MAX_COMMAND_PARTS}",
            request.command.len()
        )));
    }
    for (index, part) in request.command.iter().enumerate() {
        validate_command_part(part, &format!("command[{index}]"))?;
    }
    if request.stages.len() > MAX_STAGES {
        return Err(ContinuationError::validation(format!(
            "stages has {} entries; maximum is {MAX_STAGES}",
            request.stages.len()
        )));
    }

    let intent = super::completion::validate_conditional_completion_intent(
        request.intent,
    )?;
    let mut reasons = Vec::new();

    if intent.status != ConditionalCompletionStatusWire::Bound {
        reasons.push(format!("intent_status_{}", status_name(intent.status)));
    }
    if request.outcome != MonitorOutcomeWire::Completed {
        reasons.push("outcome_not_completed".to_string());
    }
    if request.exit_code != Some(0) {
        reasons.push("host_exit_not_zero".to_string());
    }
    if request.command != intent.verification.command {
        reasons
            .push("command_does_not_match_verification_contract".to_string());
    }
    if request.degraded_workspace {
        reasons.push("degraded_workspace".to_string());
    }
    if request.workspace_identity != request.original_workspace_identity {
        reasons.push("workspace_identity_mismatch".to_string());
    }
    if request.observations.iter().any(|repo| !repo.complete) {
        reasons.push("incomplete_observations".to_string());
    }
    if request.observations.iter().any(|repo| {
        repo.head.trim().is_empty() || repo.head == "<unknown-head>"
    }) {
        reasons.push("unknown_head".to_string());
    }
    let current_fingerprint = worktree_fingerprint(&request.observations)?;
    if current_fingerprint != intent.seal.worktree_fingerprint {
        reasons.push("stale_worktree_fingerprint".to_string());
    }
    if request.current_plan_digest != intent.seal.plan_digest {
        reasons.push("changed_finalizer_requirements".to_string());
    }
    let decided: std::collections::BTreeSet<&str> = intent
        .repository_decisions
        .iter()
        .map(|decision| decision.repo_id.as_str())
        .collect();
    for obligation_id in &request.current_obligation_ids {
        if !decided.contains(obligation_id.as_str()) {
            reasons.push(format!("new_repository_obligation:{obligation_id}"));
        }
    }
    for executor in &request.executors {
        if executor.requires_model
            || !executor.headless
            || !executor.durable_replay
            || !FIRST_PARTY_PROVIDERS
                .iter()
                .any(|provider| *provider == executor.provider_ref)
        {
            reasons
                .push(format!("unsupported_executor:{}", executor.instance_id));
        }
    }
    reasons.extend(stage_reasons(
        &intent.verification.required_stages,
        &request.stages,
    ));

    if reasons.is_empty() {
        let rendered = render_conditional_completion_message(
            &intent.success_message,
            &request.substitutions,
        )?;
        Ok(ConditionalCompletionDecisionWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            eligible: true,
            action: "complete".to_string(),
            reason: None,
            reasons: vec![],
            rendered_message: Some(rendered),
        })
    } else {
        Ok(ConditionalCompletionDecisionWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            eligible: false,
            action: "recover".to_string(),
            reason: reasons.first().cloned(),
            reasons,
            rendered_message: None,
        })
    }
}

pub fn consume_conditional_completion_request(
    request: ConditionalCompletionConsumeRequestWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionConsumeRequestWire",
    )?;
    consume_conditional_completion(request.intent)
}

pub fn invalidate_conditional_completion_request(
    request: ConditionalCompletionConsumeRequestWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionConsumeRequestWire",
    )?;
    invalidate_conditional_completion(request.intent)
}

pub fn render_conditional_completion_message_request(
    request: ConditionalCompletionMessageRequestWire,
) -> Result<ConditionalCompletionDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionMessageRequestWire",
    )?;
    let rendered = render_conditional_completion_message(
        &request.success_message,
        &request.substitutions,
    )?;
    Ok(ConditionalCompletionDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        eligible: true,
        action: "complete".to_string(),
        reason: None,
        reasons: vec![],
        rendered_message: Some(rendered),
    })
}

fn stage_reasons(
    required: &[String],
    stages: &[DiagnosticStageWire],
) -> Vec<String> {
    let mut reasons = Vec::new();
    for required_id in required {
        match matching_stage(required_id, stages) {
            None => {
                reasons.push(format!("missing_required_stage:{required_id}"))
            }
            Some(stage) => match stage.status {
                DiagnosticStageStatusWire::Passed => {
                    if stage.exit_code.is_some() && stage.exit_code != Some(0) {
                        reasons
                            .push(format!("contradictory_stage:{required_id}"));
                    }
                }
                DiagnosticStageStatusWire::Skipped => {
                    reasons
                        .push(format!("skipped_required_stage:{required_id}"));
                }
                DiagnosticStageStatusWire::Failed => {
                    reasons.push(format!("contradictory_stage:{required_id}"));
                }
                DiagnosticStageStatusWire::Error => {
                    reasons.push(format!("stage_error:{required_id}"));
                }
                DiagnosticStageStatusWire::Unknown => {
                    reasons
                        .push(format!("unknown_required_stage:{required_id}"));
                }
            },
        }
    }
    reasons
}

fn matching_stage<'a>(
    required: &str,
    stages: &'a [DiagnosticStageWire],
) -> Option<&'a DiagnosticStageWire> {
    stages.iter().find(|stage| stage_matches(required, stage))
}

fn stage_matches(required: &str, stage: &DiagnosticStageWire) -> bool {
    if stage.stage_id == required || stage.name == required {
        return true;
    }
    let haystack =
        format!("{} {}", stage.stage_id, stage.name).to_ascii_lowercase();
    match required {
        "formatting" => {
            haystack.contains("fmt") || haystack.contains("formatting")
        }
        "ruff" => haystack.contains("ruff"),
        "mypy" => haystack.contains("mypy"),
        "validation" => haystack.contains("validation"),
        "scoped_tests" => haystack.contains("scoped"),
        "full_tests" => {
            (haystack.contains("full") && haystack.contains("test"))
                && !haystack.contains("scoped")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::continuation::completion::{
        bind_conditional_completion, seal_conditional_completion,
        ConditionalCompletionBindRequestWire, ConditionalCompletionContextWire,
        ConditionalCompletionPrepareRequestWire, ObservedPathKindWire,
        ObservedPathWire, VerificationLevelWire,
    };
    use crate::continuation::schema::ContinuationExecutionIdentityWire;
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};

    fn digest64(label: &str) -> String {
        hex::encode(Sha256::digest(label.as_bytes()))
    }

    fn creator() -> ContinuationExecutionIdentityWire {
        ContinuationExecutionIdentityWire {
            project: "sase".to_string(),
            run_id: "run-1".to_string(),
            agent_name: "agent-1".to_string(),
            machine_name: Some("athena".to_string()),
            workspace_id: Some("20".to_string()),
        }
    }

    fn observation() -> RepositoryObservationWire {
        RepositoryObservationWire {
            repo_id: "repo-main".to_string(),
            kind: "main".to_string(),
            name: "main".to_string(),
            head: digest64("head"),
            head_tree: digest64("head-tree"),
            index_tree: digest64("index-tree"),
            paths: vec![ObservedPathWire {
                path: "src/app.py".to_string(),
                xy: Some("M".to_string()),
                content_hash: Some(digest64("app")),
                mode: Some("100644".to_string()),
                kind: ObservedPathKindWire::File,
                protected: false,
                foreign: false,
            }],
            complete: true,
        }
    }

    fn declaration() -> Value {
        json!({
            "schema_version": 2,
            "context_digest": digest64("context"),
            "plan_digest": digest64("plan"),
            "payloads": [{
                "instance_id": "commit",
                "payload": {
                    "repositories": [{
                        "repo_id": "repo-main",
                        "action": "commit",
                        "message": "fix: finish the change"
                    }],
                    "deferrals": []
                }
            }]
        })
    }

    fn prepare_request() -> ConditionalCompletionPrepareRequestWire {
        ConditionalCompletionPrepareRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            creator: creator(),
            context: ConditionalCompletionContextWire {
                run_id: "run-1".to_string(),
                agent_id: "agent-1".to_string(),
                turn_nonce: "nonce-1".to_string(),
                plan_digest: digest64("plan"),
                context_digest: digest64("context"),
                obligation_ids: vec!["repo-main".to_string()],
            },
            success_message: "Required checks passed in {duration}."
                .to_string(),
            verification_command: vec![
                "just".to_string(),
                "check-full".to_string(),
            ],
            declaration: declaration(),
            observations: vec![observation()],
            executors: vec![ExecutorCapabilityWire {
                instance_id: "commit".to_string(),
                provider_ref: "builtin@commit".to_string(),
                headless: true,
                durable_replay: true,
                requires_model: false,
            }],
        }
    }

    fn bound_intent() -> ConditionalCompletionIntentWire {
        let prepared = seal_conditional_completion(prepare_request()).unwrap();
        bind_conditional_completion(ConditionalCompletionBindRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            intent: prepared,
            monitor_id: "monitor-1".to_string(),
            command: vec!["just".to_string(), "check-full".to_string()],
            request_fingerprint: "sha256:abc".to_string(),
        })
        .unwrap()
    }

    fn passed_stage(id: &str, name: &str) -> DiagnosticStageWire {
        DiagnosticStageWire {
            stage_id: id.to_string(),
            name: name.to_string(),
            status: DiagnosticStageStatusWire::Passed,
            exit_code: Some(0),
            diagnostic_refs: vec![],
            counts: BTreeMap::new(),
            retained_ranges: vec![],
            capture_errors: vec![],
        }
    }

    fn required_stages() -> Vec<DiagnosticStageWire> {
        vec![
            passed_stage("formatting", "fmt (python)"),
            passed_stage("ruff", "lint (ruff)"),
            passed_stage("mypy", "lint (mypy)"),
            passed_stage("validation", "SASE validation"),
            passed_stage("full_tests", "test (full)"),
        ]
    }

    fn evaluate_request() -> ConditionalCompletionEvaluateRequestWire {
        ConditionalCompletionEvaluateRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            intent: bound_intent(),
            outcome: MonitorOutcomeWire::Completed,
            exit_code: Some(0),
            command: vec!["just".to_string(), "check-full".to_string()],
            observations: vec![observation()],
            stages: required_stages(),
            executors: vec![ExecutorCapabilityWire {
                instance_id: "commit".to_string(),
                provider_ref: "builtin@commit".to_string(),
                headless: true,
                durable_replay: true,
                requires_model: false,
            }],
            workspace_identity: "/ws/20".to_string(),
            original_workspace_identity: "/ws/20".to_string(),
            degraded_workspace: false,
            current_plan_digest: digest64("plan"),
            current_obligation_ids: vec!["repo-main".to_string()],
            substitutions: BTreeMap::from([(
                "duration".to_string(),
                "3m 02s".to_string(),
            )]),
        }
    }

    #[test]
    fn eligible_bound_success_completes_without_model() {
        let decision =
            evaluate_conditional_completion(evaluate_request()).unwrap();
        assert!(decision.eligible);
        assert_eq!(decision.action, "complete");
        assert_eq!(
            decision.rendered_message.as_deref(),
            Some("Required checks passed in 3m 02s.")
        );
        assert_eq!(
            bound_intent().verification.level,
            VerificationLevelWire::CheckFull
        );
    }

    #[test]
    fn missing_or_skipped_stages_are_ineligible() {
        let mut request = evaluate_request();
        request.stages.pop();
        let decision = evaluate_conditional_completion(request).unwrap();
        assert!(!decision.eligible);
        assert_eq!(decision.action, "recover");
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason
                    .contains("missing_required_stage:full_tests"))
        );

        let mut skipped = evaluate_request();
        skipped.stages[4].status = DiagnosticStageStatusWire::Skipped;
        let decision = evaluate_conditional_completion(skipped).unwrap();
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason
                    .contains("skipped_required_stage:full_tests"))
        );
    }

    #[test]
    fn stale_fingerprint_new_obligations_and_degraded_workspace_recover() {
        let mut stale = evaluate_request();
        stale.observations[0].head = digest64("other-head");
        let decision = evaluate_conditional_completion(stale).unwrap();
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason == "stale_worktree_fingerprint"));

        let mut obligations = evaluate_request();
        obligations
            .current_obligation_ids
            .push("repo-new".to_string());
        let decision = evaluate_conditional_completion(obligations).unwrap();
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.starts_with("new_repository_obligation:")));

        let mut degraded = evaluate_request();
        degraded.degraded_workspace = true;
        let decision = evaluate_conditional_completion(degraded).unwrap();
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason == "degraded_workspace"));
    }

    #[test]
    fn producer_pass_cannot_grant_eligibility_without_host_success() {
        let mut request = evaluate_request();
        request.outcome = MonitorOutcomeWire::Failed;
        request.exit_code = Some(1);
        let decision = evaluate_conditional_completion(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason == "host_exit_not_zero"));
    }

    #[test]
    fn model_requiring_executors_are_ineligible() {
        let mut request = evaluate_request();
        request.executors[0].requires_model = true;
        let decision = evaluate_conditional_completion(request).unwrap();
        assert!(!decision.eligible);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.starts_with("unsupported_executor:")));
    }
}
