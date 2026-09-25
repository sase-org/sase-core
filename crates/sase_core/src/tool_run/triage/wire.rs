//! Triage wire types for durable failure items.
//!
//! Facts only: extractors produce items with nullable label columns that the
//! next phase fills. Every request/result/item/label/stage/run-facts object
//! carries `schema_version: 1`.

use serde::{Deserialize, Serialize};

use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn empty_diagnostics() -> Vec<String> {
    Vec::new()
}

fn default_empty_json_array() -> serde_json::Value {
    serde_json::Value::Array(Vec::new())
}

pub const TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS: usize = 512;
pub const TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT: &str = "*";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageExtractionStatusWire {
    Parsed,
    Generic,
    OutputMissing,
    OutputTruncated,
}

impl ToolRunTriageExtractionStatusWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parsed => "parsed",
            Self::Generic => "generic",
            Self::OutputMissing => "output_missing",
            Self::OutputTruncated => "output_truncated",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "parsed" => Ok(Self::Parsed),
            "generic" => Ok(Self::Generic),
            "output_missing" => Ok(Self::OutputMissing),
            "output_truncated" => Ok(Self::OutputTruncated),
            other => Err(format!("unknown triage extraction status {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageClassWire {
    New,
    Known,
    Flaky,
    Unknown,
}

impl ToolRunTriageClassWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Known => "known",
            Self::Flaky => "flaky",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "new" => Ok(Self::New),
            "known" => Ok(Self::Known),
            "flaky" => Ok(Self::Flaky),
            "unknown" => Ok(Self::Unknown),
            other => Err(format!("unknown triage class {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageContinuationModeWire {
    Never,
    Always,
    Known,
}

impl ToolRunTriageContinuationModeWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Always => "always",
            Self::Known => "known",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "never" => Ok(Self::Never),
            "always" => Ok(Self::Always),
            "known" => Ok(Self::Known),
            other => Err(format!("unknown triage continuation mode {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageDecisionKindWire {
    Continue,
    Stop,
}

impl ToolRunTriageDecisionKindWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Stop => "stop",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "continue" => Ok(Self::Continue),
            "stop" => Ok(Self::Stop),
            other => Err(format!("unknown triage decision kind {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageRefusalWire {
    RunNotFound,
    AdHocRun,
}

impl ToolRunTriageRefusalWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunNotFound => "run_not_found",
            Self::AdHocRun => "ad_hoc_run",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "run_not_found" => Ok(Self::RunNotFound),
            "ad_hoc_run" => Ok(Self::AdHocRun),
            other => Err(format!("unknown triage refusal {other:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageLabelWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub class: ToolRunTriageClassWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub touched: Option<bool>,
    pub rule_version: u32,
    #[serde(default)]
    pub knobs: serde_json::Value,
    #[serde(default)]
    pub evidence: serde_json::Value,
    #[serde(default = "default_empty_json_array")]
    pub possible_owners: serde_json::Value,
    pub classified_ts: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageItemWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    pub extractor: String,
    pub extractor_version: u32,
    pub signature: String,
    pub display: String,
    #[serde(default)]
    pub locator_paths: Vec<String>,
    pub occurrences: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<ToolRunTriageLabelWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageDecisionWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub mode: ToolRunTriageContinuationModeWire,
    pub decision: ToolRunTriageDecisionKindWire,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<i64>,
    pub decided_ts: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageStageFactsWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    pub extraction_status: ToolRunTriageExtractionStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<ToolRunTriageDecisionWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageRunFactsWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_mode: Option<ToolRunTriageContinuationModeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipe_finished_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_continued_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_extra_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_of_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triaged_ts: Option<i64>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageExtractRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<String>,
    #[serde(default)]
    pub workspace_roots: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageExtractResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    pub status: ToolRunTriageExtractionStatusWire,
    #[serde(default)]
    pub items: Vec<ToolRunTriageItemWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageStageRecordWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    pub extraction_status: ToolRunTriageExtractionStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<ToolRunTriageDecisionWire>,
    #[serde(default)]
    pub items: Vec<ToolRunTriageItemWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageRecordRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default)]
    pub stages: Vec<ToolRunTriageStageRecordWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_facts: Option<ToolRunTriageRunFactsWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageRecordResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<ToolRunTriageRefusalWire>,
    #[serde(default)]
    pub items_inserted: u64,
    #[serde(default)]
    pub items_existing: u64,
    #[serde(default)]
    pub labels_written: u64,
    #[serde(default)]
    pub labels_kept: u64,
    #[serde(default)]
    pub stages_inserted: u64,
    #[serde(default)]
    pub decisions_written: u64,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageShowRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageShowResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub run_found: bool,
    pub triaged: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_facts: Option<ToolRunTriageRunFactsWire>,
    #[serde(default)]
    pub stages: Vec<ToolRunTriageStageFactsWire>,
    #[serde(default)]
    pub items: Vec<ToolRunTriageItemWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}
