//! Stage/settle request wires (store logic lives in store/triage_stage.rs).

use serde::{Deserialize, Serialize};

use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::classify::{
    ToolRunTriageFlakeEntryWire, ToolRunTriageKnobsWire,
    ToolRunTriageOwnerCandidateWire,
};

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageStageInputWire {
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageStageRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub stage: ToolRunTriageStageInputWire,
    #[serde(default)]
    pub project_root: Option<String>,
    #[serde(default)]
    pub workspace_roots: Vec<String>,
    #[serde(default)]
    pub ancestry: Vec<String>,
    #[serde(default)]
    pub flake_baseline: Vec<ToolRunTriageFlakeEntryWire>,
    #[serde(default)]
    pub selection_records: Vec<super::classify::ToolRunTriageEvidenceRunWire>,
    #[serde(default)]
    pub owner_candidates: Vec<ToolRunTriageOwnerCandidateWire>,
    #[serde(default)]
    pub knobs: ToolRunTriageKnobsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageStageResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<super::wire::ToolRunTriageRefusalWire>,
    #[serde(default)]
    pub items: Vec<super::wire::ToolRunTriageItemWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_of: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageSettleRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default)]
    pub stages: Vec<ToolRunTriageStageInputWire>,
    /// Fallback run output used under the reserved `*` stage key when a
    /// `stages: none` tool fails without stage rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_output: Option<String>,
    #[serde(default)]
    pub run_output_truncated: bool,
    #[serde(default)]
    pub project_root: Option<String>,
    #[serde(default)]
    pub workspace_roots: Vec<String>,
    #[serde(default)]
    pub ancestry: Vec<String>,
    #[serde(default)]
    pub flake_baseline: Vec<ToolRunTriageFlakeEntryWire>,
    #[serde(default)]
    pub selection_records: Vec<super::classify::ToolRunTriageEvidenceRunWire>,
    #[serde(default)]
    pub owner_candidates: Vec<ToolRunTriageOwnerCandidateWire>,
    #[serde(default)]
    pub knobs: ToolRunTriageKnobsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_mode:
        Option<super::wire::ToolRunTriageContinuationModeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipe_finished_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageSettleResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<super::wire::ToolRunTriageRefusalWire>,
    pub triaged: bool,
    #[serde(default)]
    pub items: Vec<super::wire::ToolRunTriageItemWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_of: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}
