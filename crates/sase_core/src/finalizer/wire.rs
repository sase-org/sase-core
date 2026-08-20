use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const FINALIZER_WIRE_SCHEMA_VERSION: u64 = 1;

pub const FINALIZER_INSTANCE_ID_MAX_LEN: usize = 64;
pub const FINALIZER_PROVIDER_REF_MAX_LEN: usize = 160;
pub const FINALIZER_PROVENANCE_MAX_LEN: usize = 160;
pub const FINALIZER_TEXT_MAX_CHARS: usize = 4_096;
pub const FINALIZER_LIST_MAX_LEN: usize = 128;
pub const FINALIZER_PAYLOAD_MAX_BYTES: usize = 256 * 1024;
pub const FINALIZER_ATTEMPT_MAX_LEN: usize = 32;
pub const FINALIZER_DIAGNOSTIC_MAX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerProviderCapabilityWire {
    Validate,
    Execute,
    Verify,
    RequiresSubmission,
    MutatesRepository,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerProviderSpecWire {
    pub schema_version: u64,
    pub provider_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_version: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<FinalizerProviderCapabilityWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_schema_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_schema_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerRefusalPolicyWire {
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerInstancePolicyWire {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_refusal_policy")]
    pub refusal: FinalizerRefusalPolicyWire,
}

impl Default for FinalizerInstancePolicyWire {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            refusal: default_refusal_policy(),
        }
    }
}

fn default_max_attempts() -> u32 {
    1
}

fn default_refusal_policy() -> FinalizerRefusalPolicyWire {
    FinalizerRefusalPolicyWire::Fail
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerInstanceSpecWire {
    pub schema_version: u64,
    pub instance_id: String,
    pub provider_ref: String,
    #[serde(default)]
    pub after: Vec<String>,
    #[serde(default)]
    pub policy: FinalizerInstancePolicyWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum FinalizerSelectorOpWire {
    Add { instance_id: String },
    Remove { instance_id: String },
    Clear,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerPlanInputWire {
    pub schema_version: u64,
    #[serde(default)]
    pub instances: Vec<FinalizerInstanceSpecWire>,
    #[serde(default)]
    pub defaults: Vec<String>,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub selectors: Vec<FinalizerSelectorOpWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerPlanEntryWire {
    pub instance_id: String,
    pub provider_ref: String,
    #[serde(default)]
    pub after: Vec<String>,
    pub policy: FinalizerInstancePolicyWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_id: Option<String>,
    pub selector_index: u32,
    pub resolved_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerPlanWire {
    pub schema_version: u64,
    pub entries: Vec<FinalizerPlanEntryWire>,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub selectors: Vec<FinalizerSelectorOpWire>,
    pub plan_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerTriggerKindWire {
    NotTriggered,
    Always,
    DirtyRepository,
    ProviderRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerPayloadRequirementWire {
    pub instance_id: String,
    pub trigger: FinalizerTriggerKindWire,
    #[serde(default)]
    pub submission_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerObligationWire {
    pub obligation_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerContextWire {
    pub schema_version: u64,
    pub run_id: String,
    pub agent_id: String,
    pub turn_nonce: String,
    pub plan_digest: String,
    #[serde(default)]
    pub requirements: Vec<FinalizerPayloadRequirementWire>,
    #[serde(default)]
    pub obligations: Vec<FinalizerObligationWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerSubmissionPayloadWire {
    pub instance_id: String,
    pub payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerSubmissionEnvelopeWire {
    pub schema_version: u64,
    pub run_id: String,
    pub agent_id: String,
    pub turn_nonce: String,
    pub plan_digest: String,
    pub context_digest: String,
    #[serde(default)]
    pub payloads: Vec<FinalizerSubmissionPayloadWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerSubmissionValidationWire {
    pub schema_version: u64,
    pub submission_digest: String,
    pub accepted_instances: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerDiagnosticSeverityWire {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerDiagnosticWire {
    pub code: String,
    pub message: String,
    pub severity: FinalizerDiagnosticSeverityWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerAttemptWire {
    pub attempt: u32,
    pub status: FinalizerInstanceStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerOutcomeEvidenceWire {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerInstanceStatusWire {
    Pending,
    Skipped,
    Success,
    Refused,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizerAggregateStatusWire {
    Pending,
    Success,
    Refused,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerInstanceResultWire {
    pub instance_id: String,
    pub status: FinalizerInstanceStatusWire,
    #[serde(default)]
    pub attempts: Vec<FinalizerAttemptWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<FinalizerOutcomeEvidenceWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<FinalizerDiagnosticWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerAggregateResultWire {
    pub schema_version: u64,
    pub status: FinalizerAggregateStatusWire,
    pub instances: Vec<FinalizerInstanceResultWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<FinalizerDiagnosticWire>,
}
