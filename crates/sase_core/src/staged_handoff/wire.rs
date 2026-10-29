//! Staged-handoff wire types and closed vocabulary.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

/// Wire schema version for staged handoff records, receipts, and admission.
pub const STAGED_HANDOFF_WIRE_SCHEMA_VERSION: u32 = 1;

/// Receipt outcome after a successful adoption.
pub const STAGED_HANDOFF_OUTCOME_ADOPTED: &str = "adopted";

/// Receipt outcome when adoption is held.
pub const STAGED_HANDOFF_OUTCOME_NOT_ADOPTED: &str = "handoff_not_adopted";

/// Return the staged-handoff wire schema version.
pub fn staged_handoff_wire_schema_version() -> u32 {
    STAGED_HANDOFF_WIRE_SCHEMA_VERSION
}

/// Handoff kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedHandoffKindWire {
    Plan,
    Questions,
    Pipe,
    Gate,
    Monitor,
}

/// Successor workspace vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuccessorWorkspaceWire {
    Inherit,
    None,
}

/// Durable staged-handoff record stored at `.sase_handoff_staged.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedHandoffWire {
    pub schema_version: u32,
    pub staged_id: String,
    pub kind: StagedHandoffKindWire,
    pub created_at: String,
    pub payload: Map<String, JsonValue>,
    pub successor_workspace: SuccessorWorkspaceWire,
    pub carry_allowed: bool,
    pub dirty_obligations_at_stage: Vec<String>,
}

/// Adoption receipt stored at `.sase_handoff_adopted.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedHandoffReceiptWire {
    pub staged_id: String,
    pub adopted_at: String,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub refs: Map<String, JsonValue>,
}

/// Host observations for one staging-admission decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageAdmissionRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub command: String,
    pub is_agent: bool,
    pub artifacts_dir_present: bool,
    pub is_finalizer_owned: bool,
    pub is_native_helper: bool,
    #[serde(default)]
    pub occupying: Vec<String>,
    pub has_accepted_current_submission: bool,
}
