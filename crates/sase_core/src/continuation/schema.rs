//! Shared continuation wire records and validation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const CONTINUATION_WIRE_SCHEMA_VERSION: u32 = 1;

pub(crate) const MAX_ID_BYTES: usize = 256;
pub(crate) const MAX_REF_BYTES: usize = 1024;
pub(crate) const MAX_TEXT_BYTES: usize = 256 * 1024;
pub(crate) const MAX_NODES: usize = 10_000;
pub(crate) const MAX_PARENTS_PER_NODE: usize = 32;
pub(crate) const MAX_SEGMENTS: usize = 512;
pub(crate) const MAX_STAGES: usize = 256;
pub(crate) const MAX_RANGES: usize = 512;
pub(crate) const MAX_COMMAND_PARTS: usize = 256;
pub(crate) const MAX_COMMAND_PART_BYTES: usize = 16 * 1024;
pub(crate) const MAX_ATTEMPTS: usize = 256;
pub(crate) const MAX_CONTEXT_ENTRIES: usize = 128;

/// Contract validation error surfaced through Rust and PyO3.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct ContinuationError {
    pub kind: String,
    pub message: String,
}

impl ContinuationError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }

    pub fn unsupported_schema(
        actual: u32,
        expected: u32,
        subject: &str,
    ) -> Self {
        Self {
            kind: "unsupported_schema_version".to_string(),
            message: format!(
                "unsupported {subject} schema_version {actual}; expected {expected}"
            ),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: "conflict".to_string(),
            message: message.into(),
        }
    }

    pub fn cycle(message: impl Into<String>) -> Self {
        Self {
            kind: "cycle".to_string(),
            message: message.into(),
        }
    }

    pub fn budget(message: impl Into<String>) -> Self {
        Self {
            kind: "budget".to_string(),
            message: message.into(),
        }
    }
}

pub(crate) fn validate_schema(
    schema_version: u32,
    subject: &str,
) -> Result<(), ContinuationError> {
    if schema_version != CONTINUATION_WIRE_SCHEMA_VERSION {
        return Err(ContinuationError::unsupported_schema(
            schema_version,
            CONTINUATION_WIRE_SCHEMA_VERSION,
            subject,
        ));
    }
    Ok(())
}

pub(crate) fn validate_non_empty_text(
    value: &str,
    field: &str,
    max_bytes: usize,
) -> Result<(), ContinuationError> {
    if value.trim().is_empty() {
        return Err(ContinuationError::validation(format!(
            "{field} must be non-empty"
        )));
    }
    validate_text(value, field, max_bytes)
}

pub(crate) fn validate_text(
    value: &str,
    field: &str,
    max_bytes: usize,
) -> Result<(), ContinuationError> {
    if value.len() > max_bytes {
        return Err(ContinuationError::validation(format!(
            "{field} is too large: {} bytes exceeds {max_bytes}",
            value.len()
        )));
    }
    if value
        .chars()
        .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
    {
        return Err(ContinuationError::validation(format!(
            "{field} contains unsupported control characters"
        )));
    }
    Ok(())
}

pub(crate) fn validate_identifier(
    value: &str,
    field: &str,
) -> Result<(), ContinuationError> {
    validate_non_empty_text(value, field, MAX_ID_BYTES)?;
    if value.chars().any(char::is_whitespace) {
        return Err(ContinuationError::validation(format!(
            "{field} must not contain whitespace"
        )));
    }
    Ok(())
}

pub(crate) fn validate_reference(
    value: &str,
    field: &str,
) -> Result<(), ContinuationError> {
    validate_identifier(value, field)?;
    validate_text(value, field, MAX_REF_BYTES)
}

pub(crate) fn validate_command_part(
    value: &str,
    field: &str,
) -> Result<(), ContinuationError> {
    validate_non_empty_text(value, field, MAX_COMMAND_PART_BYTES)
}

pub(crate) fn validate_optional_reference(
    value: &Option<String>,
    field: &str,
) -> Result<(), ContinuationError> {
    if let Some(value) = value {
        validate_reference(value, field)?;
    }
    Ok(())
}

pub(crate) fn validate_sha256(
    value: &str,
    field: &str,
) -> Result<(), ContinuationError> {
    if value.len() != 64 || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(ContinuationError::validation(format!(
            "{field} must be a 64-character hex sha256 digest"
        )));
    }
    Ok(())
}

pub(crate) fn validate_unique_ids<'a, I>(
    values: I,
    field: &str,
) -> Result<(), ContinuationError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ContinuationError::validation(format!(
                "{field} contains duplicate value {value:?}"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationNodeKindWire {
    AgentDelta,
    MonitorResult,
    Checkpoint,
    LegacyBoundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDeltaStatusWire {
    Completed,
    Interrupted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationPromptSegmentProvenanceWire {
    LocalAuthored,
    LocalMaterialized,
    InjectedParent,
    HostFact,
    ToolResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorOutcomeWire {
    Completed,
    Failed,
    Timeout,
    Stopped,
    Lost,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorTimeoutKindWire {
    WallClock,
    NoProgress,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticStageStatusWire {
    Passed,
    Failed,
    Skipped,
    Error,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationDeliveryDispositionWire {
    Pending,
    Reserved,
    Dispatching,
    Acknowledged,
    Settled,
    Cancelled,
    Nonlaunchable,
    NeedsAttention,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchRequesterContinuationModeWire {
    ResumeRequester,
    TerminalHandoff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationExecutionIdentityWire {
    pub project: String,
    pub run_id: String,
    pub agent_name: String,
    #[serde(default)]
    pub machine_name: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationAttributionWire {
    pub actor_kind: String,
    pub actor_id: String,
    #[serde(default)]
    pub decision_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationNodeWire {
    pub schema_version: u32,
    pub node_id: String,
    pub kind: ContinuationNodeKindWire,
    #[serde(default)]
    pub parent_ids: Vec<String>,
    pub owner: ContinuationExecutionIdentityWire,
    pub content_ref: String,
    pub content_sha256: String,
    #[serde(default)]
    pub checkpoint_ref: Option<String>,
    #[serde(default)]
    pub intent_ref: Option<String>,
    #[serde(default)]
    pub workspace_ref: Option<String>,
    #[serde(default)]
    pub attribution: Option<ContinuationAttributionWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationPromptSegmentWire {
    pub segment_id: String,
    pub provenance: ContinuationPromptSegmentProvenanceWire,
    pub text_ref: String,
    pub text_sha256: String,
    pub utf8_bytes: u64,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDeltaWire {
    pub schema_version: u32,
    pub node_id: String,
    pub authored_local_request: String,
    #[serde(default)]
    pub materialized_local_prompt_segments: Vec<ContinuationPromptSegmentWire>,
    #[serde(default)]
    pub final_response_ref: Option<String>,
    #[serde(default)]
    pub handoff_checkpoint_ref: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    pub status: AgentDeltaStatusWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationModelRouteWire {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub inherit_model: bool,
    #[serde(default)]
    pub inherit_effort: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationIntentWire {
    pub schema_version: u32,
    pub intent_id: String,
    pub next_action: String,
    #[serde(default)]
    pub checkpoint_ref: Option<String>,
    pub route: ContinuationModelRouteWire,
    pub outcome_policy_ref: String,
    #[serde(default)]
    pub conditional_completion_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationByteRangeWire {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedLogMetadataWire {
    #[serde(default)]
    pub log_ref: Option<String>,
    #[serde(default)]
    pub local_locator: Option<String>,
    #[serde(default)]
    pub total_observed_bytes: Option<u64>,
    #[serde(default)]
    pub retained_ranges: Vec<ContinuationByteRangeWire>,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub drain_confirmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticStageWire {
    pub stage_id: String,
    pub name: String,
    pub status: DiagnosticStageStatusWire,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub diagnostic_refs: Vec<String>,
    #[serde(default)]
    pub counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub retained_ranges: Vec<ContinuationByteRangeWire>,
    #[serde(default)]
    pub capture_errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticManifestWire {
    pub schema_version: u32,
    pub producer: String,
    #[serde(default)]
    pub stages: Vec<DiagnosticStageWire>,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub manifest_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorResultWire {
    pub schema_version: u32,
    pub result_id: String,
    pub monitor_id: String,
    pub starter_execution_id: String,
    pub outcome: MonitorOutcomeWire,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub command: Vec<String>,
    pub cwd: String,
    pub started_at: String,
    #[serde(default)]
    pub ended_at: Option<String>,
    #[serde(default)]
    pub elapsed_ms: Option<u64>,
    #[serde(default)]
    pub timeout_kind: Option<MonitorTimeoutKindWire>,
    #[serde(default)]
    pub timeout_budget_ms: Option<u64>,
    pub workspace_identity: String,
    #[serde(default)]
    pub diagnostic_manifest_ref: Option<String>,
    pub retained_log: RetainedLogMetadataWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationDeliveryKeyWire {
    pub monitor_id: String,
    pub result_id: String,
    pub branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationDeliveryAttemptWire {
    pub attempt_id: String,
    pub status: ContinuationDeliveryDispositionWire,
    pub recorded_at: String,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationDeliveryRecordWire {
    pub schema_version: u32,
    pub key: ContinuationDeliveryKeyWire,
    pub selected_action: String,
    #[serde(default)]
    pub reserved_identity: Option<String>,
    #[serde(default)]
    pub attempt_history: Vec<ContinuationDeliveryAttemptWire>,
    #[serde(default)]
    pub acknowledged_by: Option<String>,
    pub disposition: ContinuationDeliveryDispositionWire,
    #[serde(default)]
    pub disposition_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchRequesterContinuationWire {
    pub schema_version: u32,
    pub mode: LaunchRequesterContinuationModeWire,
    #[serde(default)]
    pub required: bool,
    pub checkpoint: String,
    #[serde(default)]
    pub context: BTreeMap<String, String>,
    #[serde(default)]
    pub resume_branches: Vec<String>,
    #[serde(default)]
    pub terminal_branches: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationCheckpointCoverageWire {
    pub checkpoint_ref: String,
    #[serde(default)]
    pub covered_node_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationOmissionWire {
    pub kind: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationRenderedComponentWire {
    pub name: String,
    pub utf8_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationGraphValidationWire {
    pub schema_version: u32,
    pub node_count: u64,
    pub edge_count: u64,
    #[serde(default)]
    pub duplicate_ids: Vec<String>,
    #[serde(default)]
    pub missing_parent_ids: Vec<String>,
}

pub fn validate_continuation_node_value(
    value: Value,
) -> Result<ContinuationNodeWire, ContinuationError> {
    let node: ContinuationNodeWire =
        serde_json::from_value(value).map_err(|error| {
            ContinuationError::validation(format!(
                "node is not a valid ContinuationNodeWire: {error}"
            ))
        })?;
    validate_continuation_node(node)
}

pub fn validate_continuation_node(
    node: ContinuationNodeWire,
) -> Result<ContinuationNodeWire, ContinuationError> {
    validate_schema(node.schema_version, "ContinuationNodeWire")?;
    validate_identifier(&node.node_id, "node_id")?;
    if node.parent_ids.len() > MAX_PARENTS_PER_NODE {
        return Err(ContinuationError::validation(format!(
            "parent_ids has {} entries; maximum is {MAX_PARENTS_PER_NODE}",
            node.parent_ids.len()
        )));
    }
    for (index, parent_id) in node.parent_ids.iter().enumerate() {
        validate_identifier(parent_id, &format!("parent_ids[{index}]"))?;
    }
    validate_unique_ids(
        node.parent_ids.iter().map(String::as_str),
        "parent_ids",
    )?;
    validate_execution_identity(&node.owner)?;
    validate_reference(&node.content_ref, "content_ref")?;
    validate_sha256(&node.content_sha256, "content_sha256")?;
    validate_optional_reference(&node.checkpoint_ref, "checkpoint_ref")?;
    validate_optional_reference(&node.intent_ref, "intent_ref")?;
    validate_optional_reference(&node.workspace_ref, "workspace_ref")?;
    if let Some(attribution) = &node.attribution {
        validate_attribution(attribution)?;
    }
    Ok(node)
}

pub fn validate_agent_delta(
    delta: AgentDeltaWire,
) -> Result<AgentDeltaWire, ContinuationError> {
    validate_schema(delta.schema_version, "AgentDeltaWire")?;
    validate_identifier(&delta.node_id, "node_id")?;
    validate_non_empty_text(
        &delta.authored_local_request,
        "authored_local_request",
        MAX_TEXT_BYTES,
    )?;
    if delta.materialized_local_prompt_segments.len() > MAX_SEGMENTS {
        return Err(ContinuationError::validation(format!(
            "materialized_local_prompt_segments has {} entries; maximum is {MAX_SEGMENTS}",
            delta.materialized_local_prompt_segments.len()
        )));
    }
    for (index, segment) in
        delta.materialized_local_prompt_segments.iter().enumerate()
    {
        validate_prompt_segment(segment, index)?;
        if matches!(
            segment.provenance,
            ContinuationPromptSegmentProvenanceWire::InjectedParent
        ) {
            return Err(ContinuationError::validation(format!(
                "materialized_local_prompt_segments[{index}] is injected parent content"
            )));
        }
    }
    validate_unique_ids(
        delta
            .materialized_local_prompt_segments
            .iter()
            .map(|segment| segment.segment_id.as_str()),
        "materialized_local_prompt_segments.segment_id",
    )?;
    validate_optional_reference(
        &delta.final_response_ref,
        "final_response_ref",
    )?;
    validate_optional_reference(
        &delta.handoff_checkpoint_ref,
        "handoff_checkpoint_ref",
    )?;
    for (index, source_ref) in delta.source_refs.iter().enumerate() {
        validate_reference(source_ref, &format!("source_refs[{index}]"))?;
    }
    validate_unique_ids(
        delta.source_refs.iter().map(String::as_str),
        "source_refs",
    )?;
    Ok(delta)
}

pub fn validate_continuation_intent(
    intent: ContinuationIntentWire,
) -> Result<ContinuationIntentWire, ContinuationError> {
    validate_schema(intent.schema_version, "ContinuationIntentWire")?;
    validate_identifier(&intent.intent_id, "intent_id")?;
    validate_non_empty_text(
        &intent.next_action,
        "next_action",
        MAX_TEXT_BYTES,
    )?;
    validate_optional_reference(&intent.checkpoint_ref, "checkpoint_ref")?;
    validate_reference(&intent.outcome_policy_ref, "outcome_policy_ref")?;
    validate_optional_reference(
        &intent.conditional_completion_ref,
        "conditional_completion_ref",
    )?;
    validate_model_route(&intent.route)?;
    Ok(intent)
}

pub fn validate_monitor_result(
    result: MonitorResultWire,
) -> Result<MonitorResultWire, ContinuationError> {
    validate_schema(result.schema_version, "MonitorResultWire")?;
    validate_identifier(&result.result_id, "result_id")?;
    validate_identifier(&result.monitor_id, "monitor_id")?;
    validate_identifier(&result.starter_execution_id, "starter_execution_id")?;
    if result.command.is_empty() {
        return Err(ContinuationError::validation(
            "command must contain at least one argv part",
        ));
    }
    if result.command.len() > MAX_COMMAND_PARTS {
        return Err(ContinuationError::validation(format!(
            "command has {} parts; maximum is {MAX_COMMAND_PARTS}",
            result.command.len()
        )));
    }
    for (index, part) in result.command.iter().enumerate() {
        validate_non_empty_text(
            part,
            &format!("command[{index}]"),
            MAX_COMMAND_PART_BYTES,
        )?;
    }
    validate_non_empty_text(&result.cwd, "cwd", MAX_REF_BYTES)?;
    validate_non_empty_text(&result.started_at, "started_at", MAX_REF_BYTES)?;
    if let Some(ended_at) = &result.ended_at {
        validate_non_empty_text(ended_at, "ended_at", MAX_REF_BYTES)?;
    }
    validate_non_empty_text(
        &result.workspace_identity,
        "workspace_identity",
        MAX_REF_BYTES,
    )?;
    validate_optional_reference(
        &result.diagnostic_manifest_ref,
        "diagnostic_manifest_ref",
    )?;
    validate_retained_log_metadata(&result.retained_log)?;

    match result.outcome {
        MonitorOutcomeWire::Completed => {
            if result.exit_code != Some(0) {
                return Err(ContinuationError::validation(
                    "completed monitor results must have exit_code 0",
                ));
            }
        }
        MonitorOutcomeWire::Failed => {
            if matches!(result.exit_code, None | Some(0)) {
                return Err(ContinuationError::validation(
                    "failed monitor results must have a nonzero exit_code",
                ));
            }
        }
        MonitorOutcomeWire::Timeout => {
            if result.timeout_kind.is_none() {
                return Err(ContinuationError::validation(
                    "timeout monitor results must record timeout_kind",
                ));
            }
        }
        MonitorOutcomeWire::Stopped
        | MonitorOutcomeWire::Lost
        | MonitorOutcomeWire::Unknown => {}
    }
    Ok(result)
}

pub fn validate_diagnostic_manifest(
    manifest: DiagnosticManifestWire,
) -> Result<DiagnosticManifestWire, ContinuationError> {
    validate_schema(manifest.schema_version, "DiagnosticManifestWire")?;
    validate_identifier(&manifest.producer, "producer")?;
    validate_optional_reference(&manifest.manifest_ref, "manifest_ref")?;
    if manifest.stages.len() > MAX_STAGES {
        return Err(ContinuationError::validation(format!(
            "stages has {} entries; maximum is {MAX_STAGES}",
            manifest.stages.len()
        )));
    }
    for (index, stage) in manifest.stages.iter().enumerate() {
        validate_diagnostic_stage(stage, index)?;
    }
    validate_unique_ids(
        manifest.stages.iter().map(|stage| stage.stage_id.as_str()),
        "stages.stage_id",
    )?;
    Ok(manifest)
}

pub fn validate_continuation_delivery_record(
    record: ContinuationDeliveryRecordWire,
) -> Result<ContinuationDeliveryRecordWire, ContinuationError> {
    validate_schema(record.schema_version, "ContinuationDeliveryRecordWire")?;
    validate_delivery_key(&record.key)?;
    validate_identifier(&record.selected_action, "selected_action")?;
    validate_optional_reference(
        &record.reserved_identity,
        "reserved_identity",
    )?;
    validate_optional_reference(&record.acknowledged_by, "acknowledged_by")?;
    if let Some(reason) = &record.disposition_reason {
        validate_text(reason, "disposition_reason", MAX_TEXT_BYTES)?;
    }
    if record.attempt_history.len() > MAX_ATTEMPTS {
        return Err(ContinuationError::validation(format!(
            "attempt_history has {} entries; maximum is {MAX_ATTEMPTS}",
            record.attempt_history.len()
        )));
    }
    for (index, attempt) in record.attempt_history.iter().enumerate() {
        validate_identifier(
            &attempt.attempt_id,
            &format!("attempt_history[{index}].attempt_id"),
        )?;
        validate_non_empty_text(
            &attempt.recorded_at,
            &format!("attempt_history[{index}].recorded_at"),
            MAX_REF_BYTES,
        )?;
        if let Some(detail) = &attempt.detail {
            validate_text(
                detail,
                &format!("attempt_history[{index}].detail"),
                MAX_TEXT_BYTES,
            )?;
        }
    }
    Ok(record)
}

pub fn validate_launch_requester_continuation(
    continuation: LaunchRequesterContinuationWire,
) -> Result<LaunchRequesterContinuationWire, ContinuationError> {
    validate_schema(
        continuation.schema_version,
        "LaunchRequesterContinuationWire",
    )?;
    validate_non_empty_text(
        &continuation.checkpoint,
        "checkpoint",
        MAX_TEXT_BYTES,
    )?;
    if continuation.context.len() > MAX_CONTEXT_ENTRIES {
        return Err(ContinuationError::validation(format!(
            "context has {} entries; maximum is {MAX_CONTEXT_ENTRIES}",
            continuation.context.len()
        )));
    }
    for (key, value) in &continuation.context {
        validate_identifier(key, "context key")?;
        validate_text(value, &format!("context[{key}]"), MAX_TEXT_BYTES)?;
    }
    validate_branch_list(&continuation.resume_branches, "resume_branches")?;
    validate_branch_list(&continuation.terminal_branches, "terminal_branches")?;
    match continuation.mode {
        LaunchRequesterContinuationModeWire::ResumeRequester => {
            if !continuation.required {
                return Err(ContinuationError::validation(
                    "resume_requester continuations must be required",
                ));
            }
            if continuation.resume_branches.is_empty() {
                return Err(ContinuationError::validation(
                    "resume_requester continuations must declare resume_branches",
                ));
            }
            if continuation
                .resume_branches
                .iter()
                .any(|branch| branch == "stopped")
            {
                return Err(ContinuationError::validation(
                    "stopped must not be a requester-resume branch",
                ));
            }
        }
        LaunchRequesterContinuationModeWire::TerminalHandoff => {
            if continuation.required {
                return Err(ContinuationError::validation(
                    "terminal_handoff continuations must not be required",
                ));
            }
            if !continuation.resume_branches.is_empty() {
                return Err(ContinuationError::validation(
                    "terminal_handoff continuations must not declare resume_branches",
                ));
            }
        }
    }
    Ok(continuation)
}

pub fn validate_continuation_graph(
    records: Vec<ContinuationNodeWire>,
) -> Result<ContinuationGraphValidationWire, ContinuationError> {
    if records.len() > MAX_NODES {
        return Err(ContinuationError::validation(format!(
            "records has {} entries; maximum is {MAX_NODES}",
            records.len()
        )));
    }

    let mut ids = BTreeSet::new();
    let mut duplicate_ids = BTreeSet::new();
    let mut validated = Vec::with_capacity(records.len());
    let mut edge_count = 0_u64;
    for record in records {
        let record = validate_continuation_node(record)?;
        edge_count += record.parent_ids.len() as u64;
        if !ids.insert(record.node_id.clone()) {
            duplicate_ids.insert(record.node_id.clone());
        }
        validated.push(record);
    }

    let mut missing_parent_ids = BTreeSet::new();
    for record in &validated {
        for parent_id in &record.parent_ids {
            if !ids.contains(parent_id) {
                missing_parent_ids.insert(parent_id.clone());
            }
        }
    }

    Ok(ContinuationGraphValidationWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        node_count: validated.len() as u64,
        edge_count,
        duplicate_ids: duplicate_ids.into_iter().collect(),
        missing_parent_ids: missing_parent_ids.into_iter().collect(),
    })
}

pub(crate) fn validate_checkpoint_coverage(
    coverage: &ContinuationCheckpointCoverageWire,
    index: usize,
) -> Result<(), ContinuationError> {
    validate_reference(
        &coverage.checkpoint_ref,
        &format!("checkpoint_coverage[{index}].checkpoint_ref"),
    )?;
    for (node_index, node_id) in coverage.covered_node_ids.iter().enumerate() {
        validate_identifier(
            node_id,
            &format!(
                "checkpoint_coverage[{index}].covered_node_ids[{node_index}]"
            ),
        )?;
    }
    validate_unique_ids(
        coverage.covered_node_ids.iter().map(String::as_str),
        &format!("checkpoint_coverage[{index}].covered_node_ids"),
    )?;
    Ok(())
}

pub(crate) fn validate_rendered_component(
    component: &ContinuationRenderedComponentWire,
    index: usize,
) -> Result<(), ContinuationError> {
    validate_identifier(
        &component.name,
        &format!("rendered_components[{index}].name"),
    )?;
    Ok(())
}

fn validate_execution_identity(
    owner: &ContinuationExecutionIdentityWire,
) -> Result<(), ContinuationError> {
    validate_identifier(&owner.project, "owner.project")?;
    validate_identifier(&owner.run_id, "owner.run_id")?;
    validate_identifier(&owner.agent_name, "owner.agent_name")?;
    validate_optional_reference(&owner.machine_name, "owner.machine_name")?;
    validate_optional_reference(&owner.workspace_id, "owner.workspace_id")?;
    Ok(())
}

fn validate_attribution(
    attribution: &ContinuationAttributionWire,
) -> Result<(), ContinuationError> {
    validate_identifier(&attribution.actor_kind, "attribution.actor_kind")?;
    validate_identifier(&attribution.actor_id, "attribution.actor_id")?;
    validate_optional_reference(
        &attribution.decision_ref,
        "attribution.decision_ref",
    )?;
    Ok(())
}

fn validate_prompt_segment(
    segment: &ContinuationPromptSegmentWire,
    index: usize,
) -> Result<(), ContinuationError> {
    validate_identifier(
        &segment.segment_id,
        &format!("materialized_local_prompt_segments[{index}].segment_id"),
    )?;
    validate_reference(
        &segment.text_ref,
        &format!("materialized_local_prompt_segments[{index}].text_ref"),
    )?;
    validate_sha256(
        &segment.text_sha256,
        &format!("materialized_local_prompt_segments[{index}].text_sha256"),
    )?;
    validate_optional_reference(
        &segment.source_ref,
        &format!("materialized_local_prompt_segments[{index}].source_ref"),
    )?;
    Ok(())
}

fn validate_model_route(
    route: &ContinuationModelRouteWire,
) -> Result<(), ContinuationError> {
    validate_optional_reference(&route.model, "route.model")?;
    validate_optional_reference(&route.effort, "route.effort")?;
    if !route.inherit_model && route.model.is_none() {
        return Err(ContinuationError::validation(
            "route must set model or inherit_model",
        ));
    }
    if !route.inherit_effort && route.effort.is_none() {
        return Err(ContinuationError::validation(
            "route must set effort or inherit_effort",
        ));
    }
    Ok(())
}

fn validate_retained_log_metadata(
    retained: &RetainedLogMetadataWire,
) -> Result<(), ContinuationError> {
    validate_optional_reference(&retained.log_ref, "retained_log.log_ref")?;
    validate_optional_reference(
        &retained.local_locator,
        "retained_log.local_locator",
    )?;
    if retained.retained_ranges.len() > MAX_RANGES {
        return Err(ContinuationError::validation(format!(
            "retained_log.retained_ranges has {} entries; maximum is {MAX_RANGES}",
            retained.retained_ranges.len()
        )));
    }
    for (index, range) in retained.retained_ranges.iter().enumerate() {
        validate_byte_range(
            range,
            &format!("retained_log.retained_ranges[{index}]"),
        )?;
    }
    Ok(())
}

fn validate_diagnostic_stage(
    stage: &DiagnosticStageWire,
    index: usize,
) -> Result<(), ContinuationError> {
    validate_identifier(&stage.stage_id, &format!("stages[{index}].stage_id"))?;
    validate_non_empty_text(
        &stage.name,
        &format!("stages[{index}].name"),
        MAX_REF_BYTES,
    )?;
    for (ref_index, reference) in stage.diagnostic_refs.iter().enumerate() {
        validate_reference(
            reference,
            &format!("stages[{index}].diagnostic_refs[{ref_index}]"),
        )?;
    }
    validate_unique_ids(
        stage.diagnostic_refs.iter().map(String::as_str),
        &format!("stages[{index}].diagnostic_refs"),
    )?;
    if stage.retained_ranges.len() > MAX_RANGES {
        return Err(ContinuationError::validation(format!(
            "stages[{index}].retained_ranges has {} entries; maximum is {MAX_RANGES}",
            stage.retained_ranges.len()
        )));
    }
    for (range_index, range) in stage.retained_ranges.iter().enumerate() {
        validate_byte_range(
            range,
            &format!("stages[{index}].retained_ranges[{range_index}]"),
        )?;
    }
    for (error_index, error) in stage.capture_errors.iter().enumerate() {
        validate_text(
            error,
            &format!("stages[{index}].capture_errors[{error_index}]"),
            MAX_TEXT_BYTES,
        )?;
    }
    for key in stage.counts.keys() {
        validate_identifier(key, &format!("stages[{index}].counts key"))?;
    }
    Ok(())
}

fn validate_byte_range(
    range: &ContinuationByteRangeWire,
    field: &str,
) -> Result<(), ContinuationError> {
    if range.end < range.start {
        return Err(ContinuationError::validation(format!(
            "{field}.end must be greater than or equal to start"
        )));
    }
    Ok(())
}

fn validate_delivery_key(
    key: &ContinuationDeliveryKeyWire,
) -> Result<(), ContinuationError> {
    validate_identifier(&key.monitor_id, "key.monitor_id")?;
    validate_identifier(&key.result_id, "key.result_id")?;
    validate_identifier(&key.branch, "key.branch")?;
    Ok(())
}

fn validate_branch_list(
    branches: &[String],
    field: &str,
) -> Result<(), ContinuationError> {
    for (index, branch) in branches.iter().enumerate() {
        validate_identifier(branch, &format!("{field}[{index}]"))?;
    }
    validate_unique_ids(branches.iter().map(String::as_str), field)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;

    fn owner() -> ContinuationExecutionIdentityWire {
        ContinuationExecutionIdentityWire {
            project: "sase".to_string(),
            run_id: "run-1".to_string(),
            agent_name: "agent-1".to_string(),
            machine_name: Some("athena".to_string()),
            workspace_id: None,
        }
    }

    pub(crate) fn node(id: &str, parents: Vec<&str>) -> ContinuationNodeWire {
        ContinuationNodeWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            node_id: id.to_string(),
            kind: ContinuationNodeKindWire::AgentDelta,
            parent_ids: parents.into_iter().map(str::to_string).collect(),
            owner: owner(),
            content_ref: format!("file:explicit:{id}"),
            content_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            checkpoint_ref: None,
            intent_ref: None,
            workspace_ref: None,
            attribution: None,
        }
    }

    fn monitor_result(command: Vec<String>) -> MonitorResultWire {
        MonitorResultWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            result_id: "result-1".to_string(),
            monitor_id: "monitor-1".to_string(),
            starter_execution_id: "run-1".to_string(),
            outcome: MonitorOutcomeWire::Completed,
            exit_code: Some(0),
            command,
            cwd: "/repo".to_string(),
            started_at: "2026-09-11T10:00:00Z".to_string(),
            ended_at: Some("2026-09-11T10:01:00Z".to_string()),
            elapsed_ms: Some(60_000),
            timeout_kind: None,
            timeout_budget_ms: None,
            workspace_identity: "workspace-1".to_string(),
            diagnostic_manifest_ref: Some("file:explicit:manifest".to_string()),
            retained_log: RetainedLogMetadataWire {
                log_ref: Some("file:explicit:log".to_string()),
                local_locator: Some("monitor://monitor-1/log".to_string()),
                total_observed_bytes: Some(100),
                retained_ranges: vec![ContinuationByteRangeWire {
                    start: 0,
                    end: 100,
                }],
                complete: true,
                drain_confirmed: true,
            },
        }
    }

    #[test]
    fn node_value_rejects_unknown_kind() {
        let err = validate_continuation_node_value(json!({
            "schema_version": 1,
            "node_id": "node-1",
            "kind": "bogus",
            "owner": {
                "project": "sase",
                "run_id": "run-1",
                "agent_name": "agent-1"
            },
            "content_ref": "file:explicit:node-1",
            "content_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }))
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("unknown variant"));
    }

    #[test]
    fn node_rejects_unsupported_schema_version() {
        let mut record = node("node-1", vec![]);
        record.schema_version = 99;

        let err = validate_continuation_node(record).unwrap_err();

        assert_eq!(err.kind, "unsupported_schema_version");
    }

    #[test]
    fn node_rejects_oversized_identity() {
        let err = validate_continuation_node(node(
            &"x".repeat(MAX_ID_BYTES + 1),
            vec![],
        ))
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("node_id is too large"));
    }

    #[test]
    fn monitor_result_accepts_shell_command_longer_than_reference_limit() {
        let long_shell_command = "x".repeat(MAX_REF_BYTES + 42);
        let result = validate_monitor_result(monitor_result(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            long_shell_command.clone(),
        ]))
        .unwrap();

        assert_eq!(result.command[2], long_shell_command);
    }

    #[test]
    fn monitor_result_rejects_command_part_over_command_limit() {
        let err = validate_monitor_result(monitor_result(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "x".repeat(MAX_COMMAND_PART_BYTES + 1),
        ]))
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("command[2] is too large"));
    }

    #[test]
    fn agent_delta_rejects_injected_parent_segments() {
        let delta = AgentDeltaWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            node_id: "delta-1".to_string(),
            authored_local_request: "fix it".to_string(),
            materialized_local_prompt_segments: vec![ContinuationPromptSegmentWire {
                segment_id: "seg-1".to_string(),
                provenance: ContinuationPromptSegmentProvenanceWire::InjectedParent,
                text_ref: "file:explicit:seg-1".to_string(),
                text_sha256:
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                utf8_bytes: 42,
                source_ref: None,
            }],
            final_response_ref: None,
            handoff_checkpoint_ref: None,
            source_refs: vec![],
            status: AgentDeltaStatusWire::Interrupted,
        };

        let err = validate_agent_delta(delta).unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("injected parent content"));
    }

    #[test]
    fn launch_requester_continuation_validates_resume_contract() {
        let continuation = LaunchRequesterContinuationWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            mode: LaunchRequesterContinuationModeWire::ResumeRequester,
            required: true,
            checkpoint: "Continue the phase after approval.".to_string(),
            context: BTreeMap::from([
                ("SASE_AGENT_NAME".to_string(), "agent--0".to_string()),
                ("SASE_BEAD_ID".to_string(), "sase-1.2".to_string()),
            ]),
            resume_branches: vec![
                "approve".to_string(),
                "reject".to_string(),
                "timeout".to_string(),
                "failed".to_string(),
            ],
            terminal_branches: vec!["stopped".to_string()],
        };

        let validated =
            validate_launch_requester_continuation(continuation).unwrap();

        assert_eq!(
            validated.mode,
            LaunchRequesterContinuationModeWire::ResumeRequester
        );
        assert_eq!(validated.resume_branches[0], "approve");
    }

    #[test]
    fn launch_requester_continuation_keeps_stopped_terminal() {
        let continuation = LaunchRequesterContinuationWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            mode: LaunchRequesterContinuationModeWire::ResumeRequester,
            required: true,
            checkpoint: "Continue the phase after approval.".to_string(),
            context: BTreeMap::new(),
            resume_branches: vec!["stopped".to_string()],
            terminal_branches: vec![],
        };

        let err =
            validate_launch_requester_continuation(continuation).unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("stopped must not"));
    }

    #[test]
    fn launch_requester_continuation_accepts_terminal_handoff() {
        let continuation = LaunchRequesterContinuationWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            mode: LaunchRequesterContinuationModeWire::TerminalHandoff,
            required: false,
            checkpoint: "Operator selected terminal helper handoff."
                .to_string(),
            context: BTreeMap::new(),
            resume_branches: vec![],
            terminal_branches: vec![
                "approve".to_string(),
                "reject".to_string(),
                "timeout".to_string(),
                "stopped".to_string(),
                "failed".to_string(),
            ],
        };

        let validated =
            validate_launch_requester_continuation(continuation).unwrap();

        assert_eq!(
            validated.mode,
            LaunchRequesterContinuationModeWire::TerminalHandoff
        );
        assert!(validated.resume_branches.is_empty());
    }

    #[test]
    fn graph_reports_missing_parents_without_guessing() {
        let report =
            validate_continuation_graph(vec![node("child", vec!["missing"])])
                .unwrap();

        assert_eq!(report.node_count, 1);
        assert_eq!(report.edge_count, 1);
        assert_eq!(report.missing_parent_ids, vec!["missing"]);
    }
}
