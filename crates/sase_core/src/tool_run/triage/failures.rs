//! Failures aggregation wires (store logic in store/failures.rs).

use serde::{Deserialize, Serialize};

use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn default_days() -> u32 {
    7
}

fn default_limit() -> u32 {
    50
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunFailuresRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default)]
    pub all_projects: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    #[serde(default = "default_days")]
    pub days: u32,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunFailuresGroupWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    pub stage_key: String,
    pub extractor: String,
    pub extractor_version: u32,
    pub signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    pub runs: u64,
    pub agents: u64,
    pub workspaces: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seen_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_class: Option<String>,
    #[serde(default)]
    pub newest_owners: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunFailuresResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub groups: Vec<ToolRunFailuresGroupWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}
