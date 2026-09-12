//! Conditional host-completion intents: schema, seal, preview, and bind.
//!
//! Pure validation and hashing. The host observes worktrees, persists bytes,
//! and prints previews. This module never reads files or invokes finalizers.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::schema::{
    validate_command_part, validate_identifier, validate_non_empty_text,
    validate_optional_reference, validate_reference, validate_schema,
    validate_sha256, validate_text, validate_unique_ids, ContinuationError,
    ContinuationExecutionIdentityWire, CONTINUATION_WIRE_SCHEMA_VERSION,
    MAX_COMMAND_PARTS, MAX_ID_BYTES, MAX_REF_BYTES, MAX_TEXT_BYTES,
};

pub(crate) const MAX_REPOSITORIES: usize = 128;
pub(crate) const MAX_OBSERVATION_PATHS: usize = 4_096;
pub(crate) const MAX_EXECUTORS: usize = 32;
pub(crate) const MAX_PLACEHOLDER_BYTES: usize = 64;

const CHECK_STAGES: [&str; 5] =
    ["formatting", "ruff", "mypy", "validation", "scoped_tests"];
const CHECK_FULL_STAGES: [&str; 5] =
    ["formatting", "ruff", "mypy", "validation", "full_tests"];
pub(crate) const ALLOWED_MESSAGE_PLACEHOLDERS: [&str; 2] =
    ["{duration}", "{evidence_ref}"];
pub(crate) const FIRST_PARTY_PROVIDERS: [&str; 2] =
    ["builtin@commit", "builtin@command"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionalCompletionStatusWire {
    Prepared,
    Bound,
    Consumed,
    Invalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationLevelWire {
    Check,
    CheckFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedPathKindWire {
    File,
    Symlink,
    Deleted,
    Untracked,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationContractWire {
    pub command: Vec<String>,
    pub level: VerificationLevelWire,
    #[serde(default)]
    pub required_stages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryDecisionWire {
    pub repo_id: String,
    pub action: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedPathWire {
    pub path: String,
    #[serde(default)]
    pub xy: Option<String>,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    pub kind: ObservedPathKindWire,
    #[serde(default)]
    pub protected: bool,
    #[serde(default)]
    pub foreign: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryObservationWire {
    pub repo_id: String,
    pub kind: String,
    pub name: String,
    pub head: String,
    pub head_tree: String,
    pub index_tree: String,
    #[serde(default)]
    pub paths: Vec<ObservedPathWire>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorCapabilityWire {
    pub instance_id: String,
    pub provider_ref: String,
    #[serde(default)]
    pub headless: bool,
    #[serde(default)]
    pub durable_replay: bool,
    #[serde(default)]
    pub requires_model: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionContextWire {
    pub run_id: String,
    pub agent_id: String,
    pub turn_nonce: String,
    pub plan_digest: String,
    pub context_digest: String,
    #[serde(default)]
    pub obligation_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ConditionalCompletionBindingWire {
    #[serde(default)]
    pub monitor_id: Option<String>,
    #[serde(default)]
    pub request_fingerprint: Option<String>,
    #[serde(default)]
    pub bound_command: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionSealWire {
    pub digest: String,
    pub creator: ContinuationExecutionIdentityWire,
    pub plan_digest: String,
    pub context_digest: String,
    pub worktree_fingerprint: String,
    pub declaration_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionIntentWire {
    pub schema_version: u32,
    pub intent_id: String,
    pub kind: String,
    pub status: ConditionalCompletionStatusWire,
    pub verification: VerificationContractWire,
    pub success_message: String,
    pub declaration: Value,
    #[serde(default)]
    pub repository_decisions: Vec<RepositoryDecisionWire>,
    pub observations: Vec<RepositoryObservationWire>,
    pub seal: ConditionalCompletionSealWire,
    #[serde(default)]
    pub binding: ConditionalCompletionBindingWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionPrepareRequestWire {
    pub schema_version: u32,
    pub creator: ContinuationExecutionIdentityWire,
    pub context: ConditionalCompletionContextWire,
    pub success_message: String,
    pub verification_command: Vec<String>,
    pub declaration: Value,
    pub observations: Vec<RepositoryObservationWire>,
    #[serde(default)]
    pub executors: Vec<ExecutorCapabilityWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionBindRequestWire {
    pub schema_version: u32,
    pub intent: ConditionalCompletionIntentWire,
    pub monitor_id: String,
    pub command: Vec<String>,
    pub request_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionRollbackRequestWire {
    pub schema_version: u32,
    pub intent: ConditionalCompletionIntentWire,
    pub monitor_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCompletionPreviewWire {
    pub schema_version: u32,
    pub success_action: String,
    pub repository_decisions: Vec<RepositoryDecisionWire>,
    pub required_checks: VerificationContractWire,
    pub prepared_message: String,
    pub failure_timeout_routing: String,
    pub eligible: bool,
    #[serde(default)]
    pub reasons: Vec<String>,
}

pub fn validate_conditional_completion_intent(
    intent: ConditionalCompletionIntentWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    validate_schema(intent.schema_version, "ConditionalCompletionIntentWire")?;
    if intent.kind != "conditional_completion" {
        return Err(ContinuationError::validation(format!(
            "kind must be conditional_completion, got {:?}",
            intent.kind
        )));
    }
    validate_identifier(&intent.intent_id, "intent_id")?;
    validate_verification(&intent.verification)?;
    validate_success_message(&intent.success_message)?;
    validate_declaration(&intent.declaration)?;
    validate_repository_decisions(&intent.repository_decisions)?;
    validate_observations(&intent.observations)?;
    validate_seal(&intent.seal)?;
    validate_binding(&intent.binding, intent.status)?;
    let expected = compute_seal_digest(&SealMaterial {
        creator: &intent.seal.creator,
        plan_digest: &intent.seal.plan_digest,
        context_digest: &intent.seal.context_digest,
        worktree_fingerprint: &intent.seal.worktree_fingerprint,
        declaration_digest: &intent.seal.declaration_digest,
        verification: &intent.verification,
        success_message: &intent.success_message,
        repository_decisions: &intent.repository_decisions,
    })?;
    if intent.seal.digest != expected {
        return Err(ContinuationError::conflict(
            "conditional completion seal digest does not match bound material",
        ));
    }
    let worktree = worktree_fingerprint(&intent.observations)?;
    if intent.seal.worktree_fingerprint != worktree {
        return Err(ContinuationError::conflict(
            "conditional completion worktree fingerprint does not match observations",
        ));
    }
    Ok(intent)
}

pub fn seal_conditional_completion(
    request: ConditionalCompletionPrepareRequestWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionPrepareRequestWire",
    )?;
    validate_execution_identity(&request.creator)?;
    validate_prepare_context(&request.context)?;
    validate_success_message(&request.success_message)?;
    validate_declaration(&request.declaration)?;
    validate_observations(&request.observations)?;
    validate_executors(&request.executors)?;

    if request.observations.iter().any(|repo| !repo.complete) {
        return Err(ContinuationError::validation(
            "incomplete or unstable repository observation makes the intent ineligible",
        ));
    }
    if let Some(repo) = request.observations.iter().find(|repo| {
        repo.head.trim().is_empty() || repo.head == "<unknown-head>"
    }) {
        return Err(ContinuationError::validation(format!(
            "repository {} has an unknown HEAD; the intent is ineligible",
            repo.repo_id
        )));
    }

    let verification = verification_contract(&request.verification_command)?;
    let repository_decisions =
        repository_decisions_from_declaration(&request.declaration)?;
    validate_obligation_coverage(
        &request.context.obligation_ids,
        &repository_decisions,
    )?;
    reject_protected_or_foreign(&request.observations)?;

    let declaration_digest = sha256_json(&request.declaration)?;
    if let Some(declared) = request
        .declaration
        .get("context_digest")
        .and_then(Value::as_str)
    {
        if declared != request.context.context_digest {
            return Err(ContinuationError::validation(
                "declaration context_digest is stale relative to the host-issued context",
            ));
        }
    }
    if let Some(declared) = request
        .declaration
        .get("plan_digest")
        .and_then(Value::as_str)
    {
        if declared != request.context.plan_digest {
            return Err(ContinuationError::validation(
                "declaration plan_digest does not match the resolved finalizer plan",
            ));
        }
    }

    let worktree = worktree_fingerprint(&request.observations)?;
    let digest = compute_seal_digest(&SealMaterial {
        creator: &request.creator,
        plan_digest: &request.context.plan_digest,
        context_digest: &request.context.context_digest,
        worktree_fingerprint: &worktree,
        declaration_digest: &declaration_digest,
        verification: &verification,
        success_message: &request.success_message,
        repository_decisions: &repository_decisions,
    })?;
    let intent_id = format!("cci:{}", &digest[..16]);
    let intent = ConditionalCompletionIntentWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        intent_id,
        kind: "conditional_completion".to_string(),
        status: ConditionalCompletionStatusWire::Prepared,
        verification,
        success_message: request.success_message,
        declaration: request.declaration,
        repository_decisions,
        observations: request.observations,
        seal: ConditionalCompletionSealWire {
            digest,
            creator: request.creator,
            plan_digest: request.context.plan_digest,
            context_digest: request.context.context_digest,
            worktree_fingerprint: worktree,
            declaration_digest,
        },
        binding: ConditionalCompletionBindingWire {
            monitor_id: None,
            request_fingerprint: None,
            bound_command: None,
        },
    };
    validate_conditional_completion_intent(intent)
}

pub fn preview_conditional_completion(
    intent: ConditionalCompletionIntentWire,
) -> Result<ConditionalCompletionPreviewWire, ContinuationError> {
    let intent = validate_conditional_completion_intent(intent)?;
    Ok(ConditionalCompletionPreviewWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        success_action: "complete".to_string(),
        repository_decisions: intent.repository_decisions.clone(),
        required_checks: intent.verification.clone(),
        prepared_message: intent.success_message.clone(),
        failure_timeout_routing:
            "Diagnose failures or stale verification, then finish the requested change."
                .to_string(),
        eligible: intent.status == ConditionalCompletionStatusWire::Prepared,
        reasons: match intent.status {
            ConditionalCompletionStatusWire::Prepared => vec![],
            other => vec![format!("intent_status_{}", status_name(other))],
        },
    })
}

pub fn bind_conditional_completion(
    request: ConditionalCompletionBindRequestWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionBindRequestWire",
    )?;
    let mut intent = validate_conditional_completion_intent(request.intent)?;
    validate_identifier(&request.monitor_id, "monitor_id")?;
    validate_non_empty_text(
        &request.request_fingerprint,
        "request_fingerprint",
        MAX_REF_BYTES,
    )?;
    let command = normalize_command(&request.command, "command")?;
    if intent.status != ConditionalCompletionStatusWire::Prepared {
        return Err(ContinuationError::conflict(format!(
            "conditional completion intent {} is not reusable (status {})",
            intent.intent_id,
            status_name(intent.status)
        )));
    }
    if command != intent.verification.command {
        return Err(ContinuationError::validation(
            "monitor command does not match the sealed verification contract",
        ));
    }
    intent.status = ConditionalCompletionStatusWire::Bound;
    intent.binding = ConditionalCompletionBindingWire {
        monitor_id: Some(request.monitor_id),
        request_fingerprint: Some(request.request_fingerprint),
        bound_command: Some(command),
    };
    Ok(intent)
}

pub fn rollback_conditional_completion_binding(
    request: ConditionalCompletionRollbackRequestWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ConditionalCompletionRollbackRequestWire",
    )?;
    let mut intent = validate_conditional_completion_intent(request.intent)?;
    validate_identifier(&request.monitor_id, "monitor_id")?;
    match intent.status {
        ConditionalCompletionStatusWire::Bound => {
            if intent.binding.monitor_id.as_deref()
                != Some(request.monitor_id.as_str())
            {
                return Err(ContinuationError::conflict(
                    "cannot roll back a binding owned by a different monitor",
                ));
            }
            intent.status = ConditionalCompletionStatusWire::Prepared;
            intent.binding = ConditionalCompletionBindingWire {
                monitor_id: None,
                request_fingerprint: None,
                bound_command: None,
            };
            Ok(intent)
        }
        ConditionalCompletionStatusWire::Prepared => Ok(intent),
        other => Err(ContinuationError::conflict(format!(
            "cannot roll back conditional completion intent in status {}",
            status_name(other)
        ))),
    }
}

pub fn consume_conditional_completion(
    intent: ConditionalCompletionIntentWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    let mut intent = validate_conditional_completion_intent(intent)?;
    match intent.status {
        ConditionalCompletionStatusWire::Bound
        | ConditionalCompletionStatusWire::Consumed => {
            intent.status = ConditionalCompletionStatusWire::Consumed;
            Ok(intent)
        }
        other => Err(ContinuationError::conflict(format!(
            "cannot consume conditional completion intent in status {}",
            status_name(other)
        ))),
    }
}

pub fn invalidate_conditional_completion(
    intent: ConditionalCompletionIntentWire,
) -> Result<ConditionalCompletionIntentWire, ContinuationError> {
    let mut intent = validate_conditional_completion_intent(intent)?;
    match intent.status {
        ConditionalCompletionStatusWire::Consumed => {
            Err(ContinuationError::conflict(
                "cannot invalidate a consumed conditional completion intent",
            ))
        }
        _ => {
            intent.status = ConditionalCompletionStatusWire::Invalidated;
            Ok(intent)
        }
    }
}

pub fn render_conditional_completion_message(
    message: &str,
    substitutions: &std::collections::BTreeMap<String, String>,
) -> Result<String, ContinuationError> {
    validate_success_message(message)?;
    let mut rendered = message.to_string();
    for placeholder in ALLOWED_MESSAGE_PLACEHOLDERS {
        let key = &placeholder[1..placeholder.len() - 1];
        if let Some(value) = substitutions.get(key) {
            if value.contains('$')
                || value.contains('`')
                || value.contains("{%")
                || value.contains("$(")
            {
                return Err(ContinuationError::validation(format!(
                    "substitution for {placeholder} must not contain shell or template execution"
                )));
            }
            rendered = rendered.replace(placeholder, value);
        }
    }
    Ok(rendered)
}

fn verification_contract(
    command: &[String],
) -> Result<VerificationContractWire, ContinuationError> {
    let command = normalize_command(command, "verification_command")?;
    let level = match command.as_slice() {
        [just, check] if just == "just" && check == "check" => {
            VerificationLevelWire::Check
        }
        [just, check] if just == "just" && check == "check-full" => {
            VerificationLevelWire::CheckFull
        }
        _ => {
            return Err(ContinuationError::validation(
                "conditional completion requires an exact first-party command of `just check` or `just check-full`",
            ));
        }
    };
    Ok(VerificationContractWire {
        command,
        level,
        required_stages: required_stages(level)
            .iter()
            .map(|stage| (*stage).to_string())
            .collect(),
    })
}

fn required_stages(level: VerificationLevelWire) -> &'static [&'static str] {
    match level {
        VerificationLevelWire::Check => &CHECK_STAGES,
        VerificationLevelWire::CheckFull => &CHECK_FULL_STAGES,
    }
}

fn validate_verification(
    contract: &VerificationContractWire,
) -> Result<(), ContinuationError> {
    let expected = verification_contract(&contract.command)?;
    if contract.level != expected.level {
        return Err(ContinuationError::validation(
            "verification level does not match the sealed command",
        ));
    }
    if contract.required_stages != expected.required_stages {
        return Err(ContinuationError::validation(
            "required_stages do not match the sealed verification level",
        ));
    }
    if contract.level == VerificationLevelWire::CheckFull
        && contract
            .required_stages
            .iter()
            .any(|stage| stage == "scoped_tests")
        && !contract
            .required_stages
            .iter()
            .any(|stage| stage == "full_tests")
    {
        return Err(ContinuationError::validation(
            "cannot lower a required full check to a scoped check",
        ));
    }
    Ok(())
}

fn validate_success_message(message: &str) -> Result<(), ContinuationError> {
    validate_non_empty_text(message, "success_message", MAX_TEXT_BYTES)?;
    if message.contains("$(")
        || message.contains('`')
        || message.contains("${")
        || message.contains("{%")
    {
        return Err(ContinuationError::validation(
            "success_message must not contain shell or template execution",
        ));
    }
    for placeholder in extract_braces(message) {
        if !ALLOWED_MESSAGE_PLACEHOLDERS.contains(&placeholder.as_str()) {
            return Err(ContinuationError::validation(format!(
                "success_message placeholder {placeholder} is not a documented host-fact substitution"
            )));
        }
        if placeholder.len() > MAX_PLACEHOLDER_BYTES {
            return Err(ContinuationError::validation(
                "success_message placeholder is too large",
            ));
        }
    }
    Ok(())
}

fn extract_braces(message: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = message;
    while let Some(start) = rest.find('{') {
        let after = &rest[start..];
        if let Some(end) = after.find('}') {
            found.push(after[..=end].to_string());
            rest = &after[end + 1..];
        } else {
            break;
        }
    }
    found
}

fn validate_declaration(declaration: &Value) -> Result<(), ContinuationError> {
    if !declaration.is_object() {
        return Err(ContinuationError::validation(
            "declaration must be a JSON object",
        ));
    }
    let encoded = serde_json::to_vec(declaration).map_err(|error| {
        ContinuationError::validation(format!(
            "declaration is not serializable: {error}"
        ))
    })?;
    if encoded.len() > MAX_TEXT_BYTES {
        return Err(ContinuationError::validation(format!(
            "declaration is too large: {} bytes exceeds {MAX_TEXT_BYTES}",
            encoded.len()
        )));
    }
    Ok(())
}

fn repository_decisions_from_declaration(
    declaration: &Value,
) -> Result<Vec<RepositoryDecisionWire>, ContinuationError> {
    let payloads = declaration
        .get("payloads")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ContinuationError::validation(
                "declaration is missing payloads for repository decisions",
            )
        })?;
    let mut decisions = Vec::new();
    for payload in payloads {
        let Some(map) = payload.get("payload") else {
            continue;
        };
        if let Some(deferrals) = map.get("deferrals").and_then(Value::as_array)
        {
            if !deferrals.is_empty() {
                return Err(ContinuationError::validation(
                    "conditional completion does not accept declaration deferrals",
                ));
            }
        }
        let Some(repositories) =
            map.get("repositories").and_then(Value::as_array)
        else {
            continue;
        };
        for repo in repositories {
            let repo_id = repo
                .get("repo_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ContinuationError::validation(
                        "repository decision is missing repo_id",
                    )
                })?;
            let action = repo
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or("commit");
            if action != "commit" {
                return Err(ContinuationError::validation(format!(
                    "repository {repo_id} action must be commit for conditional completion"
                )));
            }
            let message = repo
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ContinuationError::validation(format!(
                        "repository {repo_id} is missing a commit message"
                    ))
                })?;
            decisions.push(RepositoryDecisionWire {
                repo_id: repo_id.to_string(),
                action: action.to_string(),
                message: message.to_string(),
            });
        }
    }
    if decisions.is_empty() {
        return Err(ContinuationError::validation(
            "declaration is missing repository decisions",
        ));
    }
    validate_repository_decisions(&decisions)?;
    Ok(decisions)
}

fn validate_repository_decisions(
    decisions: &[RepositoryDecisionWire],
) -> Result<(), ContinuationError> {
    if decisions.len() > MAX_REPOSITORIES {
        return Err(ContinuationError::validation(format!(
            "repository_decisions has {} entries; maximum is {MAX_REPOSITORIES}",
            decisions.len()
        )));
    }
    for (index, decision) in decisions.iter().enumerate() {
        validate_identifier(
            &decision.repo_id,
            &format!("repository_decisions[{index}].repo_id"),
        )?;
        if decision.action != "commit" {
            return Err(ContinuationError::validation(format!(
                "repository_decisions[{index}].action must be commit"
            )));
        }
        validate_non_empty_text(
            &decision.message,
            &format!("repository_decisions[{index}].message"),
            MAX_REF_BYTES,
        )?;
    }
    validate_unique_ids(
        decisions.iter().map(|decision| decision.repo_id.as_str()),
        "repository_decisions.repo_id",
    )
}

fn validate_obligation_coverage(
    obligation_ids: &[String],
    decisions: &[RepositoryDecisionWire],
) -> Result<(), ContinuationError> {
    if obligation_ids.len() > MAX_REPOSITORIES {
        return Err(ContinuationError::validation(format!(
            "obligation_ids has {} entries; maximum is {MAX_REPOSITORIES}",
            obligation_ids.len()
        )));
    }
    let decided: BTreeSet<&str> = decisions
        .iter()
        .map(|decision| decision.repo_id.as_str())
        .collect();
    for (index, obligation_id) in obligation_ids.iter().enumerate() {
        validate_identifier(
            obligation_id,
            &format!("obligation_ids[{index}]"),
        )?;
        if !decided.contains(obligation_id.as_str()) {
            return Err(ContinuationError::validation(format!(
                "declaration is missing a repository decision for {obligation_id}"
            )));
        }
    }
    Ok(())
}

fn validate_observations(
    observations: &[RepositoryObservationWire],
) -> Result<(), ContinuationError> {
    if observations.is_empty() {
        return Err(ContinuationError::validation(
            "observations must include every relevant opened repository",
        ));
    }
    if observations.len() > MAX_REPOSITORIES {
        return Err(ContinuationError::validation(format!(
            "observations has {} entries; maximum is {MAX_REPOSITORIES}",
            observations.len()
        )));
    }
    let mut path_count = 0_usize;
    for (index, repo) in observations.iter().enumerate() {
        validate_identifier(
            &repo.repo_id,
            &format!("observations[{index}].repo_id"),
        )?;
        validate_identifier(
            &repo.kind,
            &format!("observations[{index}].kind"),
        )?;
        validate_non_empty_text(
            &repo.name,
            &format!("observations[{index}].name"),
            MAX_ID_BYTES,
        )?;
        validate_non_empty_text(
            &repo.head,
            &format!("observations[{index}].head"),
            MAX_REF_BYTES,
        )?;
        validate_non_empty_text(
            &repo.head_tree,
            &format!("observations[{index}].head_tree"),
            MAX_REF_BYTES,
        )?;
        validate_non_empty_text(
            &repo.index_tree,
            &format!("observations[{index}].index_tree"),
            MAX_REF_BYTES,
        )?;
        path_count += repo.paths.len();
        if path_count > MAX_OBSERVATION_PATHS {
            return Err(ContinuationError::validation(format!(
                "observations path count exceeds {MAX_OBSERVATION_PATHS}"
            )));
        }
        for (path_index, path) in repo.paths.iter().enumerate() {
            validate_non_empty_text(
                &path.path,
                &format!("observations[{index}].paths[{path_index}].path"),
                MAX_REF_BYTES,
            )?;
            if let Some(xy) = &path.xy {
                validate_text(
                    xy,
                    &format!("observations[{index}].paths[{path_index}].xy"),
                    8,
                )?;
            }
            if let Some(mode) = &path.mode {
                validate_text(
                    mode,
                    &format!("observations[{index}].paths[{path_index}].mode"),
                    16,
                )?;
            }
        }
        validate_unique_ids(
            repo.paths.iter().map(|path| path.path.as_str()),
            &format!("observations[{index}].paths.path"),
        )?;
    }
    validate_unique_ids(
        observations.iter().map(|repo| repo.repo_id.as_str()),
        "observations.repo_id",
    )
}

fn reject_protected_or_foreign(
    observations: &[RepositoryObservationWire],
) -> Result<(), ContinuationError> {
    for repo in observations {
        for path in &repo.paths {
            if path.protected {
                return Err(ContinuationError::validation(format!(
                    "protected path {} in {} makes the intent ineligible",
                    path.path, repo.repo_id
                )));
            }
            if path.foreign {
                return Err(ContinuationError::validation(format!(
                    "foreign path {} in {} makes the intent ineligible",
                    path.path, repo.repo_id
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_executors(
    executors: &[ExecutorCapabilityWire],
) -> Result<(), ContinuationError> {
    if executors.len() > MAX_EXECUTORS {
        return Err(ContinuationError::validation(format!(
            "executors has {} entries; maximum is {MAX_EXECUTORS}",
            executors.len()
        )));
    }
    for (index, executor) in executors.iter().enumerate() {
        validate_identifier(
            &executor.instance_id,
            &format!("executors[{index}].instance_id"),
        )?;
        validate_reference(
            &executor.provider_ref,
            &format!("executors[{index}].provider_ref"),
        )?;
        if executor.requires_model
            || !executor.headless
            || !executor.durable_replay
            || !FIRST_PARTY_PROVIDERS
                .iter()
                .any(|provider| *provider == executor.provider_ref)
        {
            return Err(ContinuationError::validation(format!(
                "executor {} ({}) does not support no-model host completion",
                executor.instance_id, executor.provider_ref
            )));
        }
    }
    Ok(())
}

fn validate_prepare_context(
    context: &ConditionalCompletionContextWire,
) -> Result<(), ContinuationError> {
    validate_identifier(&context.run_id, "context.run_id")?;
    validate_identifier(&context.agent_id, "context.agent_id")?;
    validate_identifier(&context.turn_nonce, "context.turn_nonce")?;
    validate_sha256(&context.plan_digest, "context.plan_digest")?;
    validate_sha256(&context.context_digest, "context.context_digest")?;
    for (index, obligation_id) in context.obligation_ids.iter().enumerate() {
        validate_identifier(
            obligation_id,
            &format!("context.obligation_ids[{index}]"),
        )?;
    }
    validate_unique_ids(
        context.obligation_ids.iter().map(String::as_str),
        "context.obligation_ids",
    )
}

fn validate_execution_identity(
    owner: &ContinuationExecutionIdentityWire,
) -> Result<(), ContinuationError> {
    validate_identifier(&owner.project, "creator.project")?;
    validate_identifier(&owner.run_id, "creator.run_id")?;
    validate_identifier(&owner.agent_name, "creator.agent_name")?;
    validate_optional_reference(&owner.machine_name, "creator.machine_name")?;
    validate_optional_reference(&owner.workspace_id, "creator.workspace_id")
}

fn validate_seal(
    seal: &ConditionalCompletionSealWire,
) -> Result<(), ContinuationError> {
    validate_sha256(&seal.digest, "seal.digest")?;
    validate_execution_identity(&seal.creator)?;
    validate_sha256(&seal.plan_digest, "seal.plan_digest")?;
    validate_sha256(&seal.context_digest, "seal.context_digest")?;
    validate_sha256(&seal.worktree_fingerprint, "seal.worktree_fingerprint")?;
    validate_sha256(&seal.declaration_digest, "seal.declaration_digest")
}

fn validate_binding(
    binding: &ConditionalCompletionBindingWire,
    status: ConditionalCompletionStatusWire,
) -> Result<(), ContinuationError> {
    validate_optional_reference(&binding.monitor_id, "binding.monitor_id")?;
    validate_optional_reference(
        &binding.request_fingerprint,
        "binding.request_fingerprint",
    )?;
    if let Some(command) = &binding.bound_command {
        normalize_command(command, "binding.bound_command")?;
    }
    match status {
        ConditionalCompletionStatusWire::Prepared => {
            if binding.monitor_id.is_some()
                || binding.request_fingerprint.is_some()
                || binding.bound_command.is_some()
            {
                return Err(ContinuationError::validation(
                    "prepared intents must not carry a binding",
                ));
            }
        }
        ConditionalCompletionStatusWire::Bound => {
            if binding.monitor_id.is_none()
                || binding.request_fingerprint.is_none()
                || binding.bound_command.is_none()
            {
                return Err(ContinuationError::validation(
                    "bound intents must record monitor_id, fingerprint, and command",
                ));
            }
        }
        ConditionalCompletionStatusWire::Consumed
        | ConditionalCompletionStatusWire::Invalidated => {}
    }
    Ok(())
}

fn normalize_command(
    command: &[String],
    field: &str,
) -> Result<Vec<String>, ContinuationError> {
    if command.is_empty() {
        return Err(ContinuationError::validation(format!(
            "{field} must contain at least one argv part"
        )));
    }
    if command.len() > MAX_COMMAND_PARTS {
        return Err(ContinuationError::validation(format!(
            "{field} has {} parts; maximum is {MAX_COMMAND_PARTS}",
            command.len()
        )));
    }
    for (index, part) in command.iter().enumerate() {
        validate_command_part(part, &format!("{field}[{index}]"))?;
    }
    Ok(command.to_vec())
}

pub(crate) fn worktree_fingerprint(
    observations: &[RepositoryObservationWire],
) -> Result<String, ContinuationError> {
    sha256_json(&serde_json::to_value(observations).map_err(|error| {
        ContinuationError::validation(format!(
            "unable to encode observations for fingerprint: {error}"
        ))
    })?)
}

struct SealMaterial<'a> {
    creator: &'a ContinuationExecutionIdentityWire,
    plan_digest: &'a str,
    context_digest: &'a str,
    worktree_fingerprint: &'a str,
    declaration_digest: &'a str,
    verification: &'a VerificationContractWire,
    success_message: &'a str,
    repository_decisions: &'a [RepositoryDecisionWire],
}

fn compute_seal_digest(
    material: &SealMaterial<'_>,
) -> Result<String, ContinuationError> {
    let mut encoded = serde_json::Map::new();
    encoded.insert(
        "creator".to_string(),
        serde_json::to_value(material.creator).map_err(|error| {
            ContinuationError::validation(format!(
                "unable to encode creator: {error}"
            ))
        })?,
    );
    encoded.insert(
        "plan_digest".to_string(),
        Value::String(material.plan_digest.to_string()),
    );
    encoded.insert(
        "context_digest".to_string(),
        Value::String(material.context_digest.to_string()),
    );
    encoded.insert(
        "worktree_fingerprint".to_string(),
        Value::String(material.worktree_fingerprint.to_string()),
    );
    encoded.insert(
        "declaration_digest".to_string(),
        Value::String(material.declaration_digest.to_string()),
    );
    encoded.insert(
        "verification".to_string(),
        serde_json::to_value(material.verification).map_err(|error| {
            ContinuationError::validation(format!(
                "unable to encode verification: {error}"
            ))
        })?,
    );
    encoded.insert(
        "success_message".to_string(),
        Value::String(material.success_message.to_string()),
    );
    encoded.insert(
        "repository_decisions".to_string(),
        serde_json::to_value(material.repository_decisions).map_err(
            |error| {
                ContinuationError::validation(format!(
                    "unable to encode repository decisions: {error}"
                ))
            },
        )?,
    );
    sha256_json(&Value::Object(encoded))
}

fn sha256_json(value: &Value) -> Result<String, ContinuationError> {
    let encoded =
        serde_json::to_vec(&canonical_json(value)).map_err(|error| {
            ContinuationError::validation(format!(
                "unable to encode canonical JSON: {error}"
            ))
        })?;
    Ok(hex::encode(Sha256::digest(&encoded)))
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(entries) => {
            Value::Array(entries.iter().map(canonical_json).collect())
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut sorted = serde_json::Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&map[key]));
            }
            Value::Object(sorted)
        }
        other => other.clone(),
    }
}

pub(crate) fn status_name(
    status: ConditionalCompletionStatusWire,
) -> &'static str {
    match status {
        ConditionalCompletionStatusWire::Prepared => "prepared",
        ConditionalCompletionStatusWire::Bound => "bound",
        ConditionalCompletionStatusWire::Consumed => "consumed",
        ConditionalCompletionStatusWire::Invalidated => "invalidated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

    fn request() -> ConditionalCompletionPrepareRequestWire {
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

    #[test]
    fn seals_a_valid_first_party_intent() {
        let intent = seal_conditional_completion(request()).unwrap();
        assert_eq!(intent.status, ConditionalCompletionStatusWire::Prepared);
        assert_eq!(intent.verification.level, VerificationLevelWire::CheckFull);
        assert_eq!(
            intent.verification.required_stages,
            vec!["formatting", "ruff", "mypy", "validation", "full_tests"]
        );
        assert!(intent.intent_id.starts_with("cci:"));
        let preview = preview_conditional_completion(intent).unwrap();
        assert_eq!(preview.success_action, "complete");
        assert!(preview.eligible);
    }

    #[test]
    fn rejects_missing_repository_decisions() {
        let mut req = request();
        req.declaration["payloads"][0]["payload"]["repositories"] = json!([]);
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("missing repository decisions"));
    }

    #[test]
    fn rejects_stale_context_digest() {
        let mut req = request();
        req.declaration["context_digest"] = json!(digest64("other-context"));
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("stale"));
    }

    #[test]
    fn rejects_altered_finalizer_plan() {
        let mut req = request();
        req.declaration["plan_digest"] = json!(digest64("other-plan"));
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("plan_digest"));
    }

    #[test]
    fn rejects_unsupported_executors_and_arbitrary_commands() {
        let mut req = request();
        req.executors[0].provider_ref = "plugin@model".to_string();
        let error = seal_conditional_completion(req.clone()).unwrap_err();
        assert!(error.message.contains("no-model"));

        req = request();
        req.verification_command = vec![
            "bash".to_string(),
            "-c".to_string(),
            "just check".to_string(),
        ];
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("just check"));
    }

    #[test]
    fn rejects_protected_and_foreign_paths() {
        let mut req = request();
        req.observations[0].paths[0].protected = true;
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("protected path"));

        let mut req = request();
        req.observations[0].paths[0].foreign = true;
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("foreign path"));
    }

    #[test]
    fn bind_is_single_use_and_command_sensitive() {
        let prepared = seal_conditional_completion(request()).unwrap();
        let bound =
            bind_conditional_completion(ConditionalCompletionBindRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                intent: prepared.clone(),
                monitor_id: "monitor-1".to_string(),
                command: vec!["just".to_string(), "check-full".to_string()],
                request_fingerprint: "sha256:abc".to_string(),
            })
            .unwrap();
        assert_eq!(bound.status, ConditionalCompletionStatusWire::Bound);

        let duplicate =
            bind_conditional_completion(ConditionalCompletionBindRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                intent: bound.clone(),
                monitor_id: "monitor-2".to_string(),
                command: vec!["just".to_string(), "check-full".to_string()],
                request_fingerprint: "sha256:def".to_string(),
            })
            .unwrap_err();
        assert!(duplicate.message.contains("not reusable"));

        let changed =
            bind_conditional_completion(ConditionalCompletionBindRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                intent: prepared,
                monitor_id: "monitor-1".to_string(),
                command: vec!["just".to_string(), "check".to_string()],
                request_fingerprint: "sha256:abc".to_string(),
            })
            .unwrap_err();
        assert!(changed.message.contains("does not match"));
    }

    #[test]
    fn rollback_restores_a_bound_intent() {
        let prepared = seal_conditional_completion(request()).unwrap();
        let bound =
            bind_conditional_completion(ConditionalCompletionBindRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                intent: prepared,
                monitor_id: "monitor-1".to_string(),
                command: vec!["just".to_string(), "check-full".to_string()],
                request_fingerprint: "sha256:abc".to_string(),
            })
            .unwrap();
        let restored = rollback_conditional_completion_binding(
            ConditionalCompletionRollbackRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                intent: bound,
                monitor_id: "monitor-1".to_string(),
            },
        )
        .unwrap();
        assert_eq!(restored.status, ConditionalCompletionStatusWire::Prepared);
        assert!(restored.binding.monitor_id.is_none());
    }

    #[test]
    fn consume_is_idempotent_for_bound_intents() {
        let prepared = seal_conditional_completion(request()).unwrap();
        let bound =
            bind_conditional_completion(ConditionalCompletionBindRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                intent: prepared,
                monitor_id: "monitor-1".to_string(),
                command: vec!["just".to_string(), "check-full".to_string()],
                request_fingerprint: "sha256:abc".to_string(),
            })
            .unwrap();
        let consumed = consume_conditional_completion(bound).unwrap();
        assert_eq!(consumed.status, ConditionalCompletionStatusWire::Consumed);
        let again = consume_conditional_completion(consumed).unwrap();
        assert_eq!(again.status, ConditionalCompletionStatusWire::Consumed);
    }

    #[test]
    fn render_substitutes_only_documented_host_facts() {
        let rendered = render_conditional_completion_message(
            "Required checks passed in {duration} ({evidence_ref}).",
            &std::collections::BTreeMap::from([
                ("duration".to_string(), "3m 02s".to_string()),
                ("evidence_ref".to_string(), "file:explicit:diag".to_string()),
            ]),
        )
        .unwrap();
        assert_eq!(
            rendered,
            "Required checks passed in 3m 02s (file:explicit:diag)."
        );
    }

    #[test]
    fn incomplete_observations_are_ineligible() {
        let mut req = request();
        req.observations[0].complete = false;
        let error = seal_conditional_completion(req).unwrap_err();
        assert!(error.message.contains("incomplete"));
    }
}
