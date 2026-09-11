//! Serde-friendly wire records for gate-follow-up recovery decisions.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version for gate-follow-up requests and decisions.
pub const GATE_FOLLOWUP_WIRE_SCHEMA_VERSION: u32 = 1;

pub const MODE_SETTLE: &str = "settle";
pub const MODE_RESUME: &str = "resume";
pub const MODE_DIAGNOSE: &str = "diagnose";

pub const DISPOSITION_INTENTIONAL_NONE: &str = "intentional-none";
pub const DISPOSITION_SUPPRESSED: &str = "suppressed";
pub const DISPOSITION_IN_PROGRESS: &str = "in-progress";
pub const DISPOSITION_LAUNCHED: &str = "launched";
pub const DISPOSITION_LAUNCHED_DEGRADED: &str = "launched-degraded";
pub const DISPOSITION_NOT_LAUNCHABLE: &str = "not-launchable";
pub const DISPOSITION_FAILED: &str = "failed";
pub const DISPOSITION_INTERRUPTED: &str = "interrupted";
pub const DISPOSITION_AMBIGUOUS: &str = "ambiguous";

pub const RECOVERY_NOOP: &str = "noop";
pub const RECOVERY_ADOPT: &str = "adopt";
pub const RECOVERY_RESUME: &str = "resume";
pub const RECOVERY_WAIT: &str = "wait";
pub const RECOVERY_REPORT_AMBIGUOUS: &str = "report-ambiguous";

pub const OUTCOME_LAUNCHED: &str = "launched";
pub const OUTCOME_LAUNCHED_DEGRADED: &str = "launched-degraded";
pub const OUTCOME_NOT_LAUNCHABLE: &str = "not-launchable";
pub const OUTCOME_SUPPRESSED: &str = "suppressed";
pub const OUTCOME_FAILED: &str = "failed";

pub const ATTEMPT_STAGE_PENDING: &str = "pending";
pub const ATTEMPT_STAGE_PREPARING: &str = "preparing";
pub const ATTEMPT_STAGE_LAUNCHING: &str = "launching";
pub const ATTEMPT_STAGE_RECORDING: &str = "recording";
pub const ATTEMPT_STAGE_LAUNCHED: &str = "launched";
pub const ATTEMPT_STAGE_FAILED: &str = "failed";

/// A path-specific structural request failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(deny_unknown_fields)]
#[error("{code} at {path}: {message}")]
pub struct GateFollowupError {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl GateFollowupError {
    pub(crate) fn new(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// Persisted attempt identity for one gate handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateFollowupAttemptWire {
    pub attempt_id: String,
    #[serde(default)]
    pub fingerprint: Option<String>,
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(default)]
    pub live: bool,
    #[serde(default)]
    pub owner_pid: Option<u32>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub error_stage: Option<String>,
    #[serde(default)]
    pub error_type: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
}

/// Host-observed successor attachment and launch evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateFollowupSuccessorEvidenceWire {
    #[serde(default)]
    pub family_name: Option<String>,
    #[serde(default)]
    pub expected_suffix: Option<String>,
    #[serde(default)]
    pub attached_agent: Option<String>,
    #[serde(default)]
    pub launch_receipt: Option<String>,
    #[serde(default)]
    pub running: bool,
    #[serde(default)]
    pub completed: bool,
    #[serde(default)]
    pub ambiguous: bool,
}

/// Complete host-collected input to the pure classifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateFollowupDecisionRequestWire {
    pub schema_version: u32,
    pub mode: String,
    pub gate_id: String,
    #[serde(default)]
    pub gate_kind: Option<String>,
    pub gate_state: String,
    #[serde(default)]
    pub already_settled: bool,
    #[serde(default)]
    pub request_fingerprint: Option<String>,
    #[serde(default)]
    pub followup_requested: bool,
    #[serde(default)]
    pub creator_live: bool,
    #[serde(default)]
    pub auto_suppressed: bool,
    #[serde(default)]
    pub followup_outcome: Option<String>,
    #[serde(default)]
    pub followup_agent: Option<String>,
    #[serde(default)]
    pub followup_error: Option<String>,
    #[serde(default)]
    pub followup_degraded_reason: Option<String>,
    #[serde(default)]
    pub followup_prompt_path: Option<String>,
    #[serde(default)]
    pub attempt: Option<GateFollowupAttemptWire>,
    #[serde(default)]
    pub successor_evidence: Option<GateFollowupSuccessorEvidenceWire>,
}

/// Portable recovery verdict for one gate handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateFollowupDecisionWire {
    pub schema_version: u32,
    pub disposition: String,
    pub recovery: String,
    #[serde(default)]
    pub persist_outcome: Option<String>,
    #[serde(default)]
    pub adopt_agent: Option<String>,
    pub needs_attention: bool,
    pub resume_eligible: bool,
    pub launch_allowed: bool,
    pub reason: String,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

/// Parse a [`GateFollowupDecisionRequestWire`] from a JSON value.
pub fn gate_followup_decision_request_from_json_value(
    value: &serde_json::Value,
) -> Result<GateFollowupDecisionRequestWire, String> {
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}
