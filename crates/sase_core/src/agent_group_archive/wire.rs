use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedAgentGroupRefWire {
    pub agent_type: String,
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub is_workflow_child: bool,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default, alias = "tag")]
    pub tribe: Option<String>,
    #[serde(default)]
    pub prompt_preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedAgentGroupWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub group_id: String,
    pub created_at: String,
    pub source: String,
    pub title: String,
    #[serde(default)]
    pub name: Option<String>,
    pub agent_count: i64,
    pub top_level_agent_count: i64,
    #[serde(default)]
    pub status_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub project_names: Vec<String>,
    #[serde(default)]
    pub cl_names: Vec<String>,
    #[serde(default)]
    pub agent_refs: Vec<SavedAgentGroupRefWire>,
    #[serde(default)]
    pub revived_at: Option<String>,
    #[serde(default)]
    pub times_revived: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedAgentGroupSummaryWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub group_id: String,
    pub created_at: String,
    pub source: String,
    pub title: String,
    #[serde(default)]
    pub name: Option<String>,
    pub agent_count: i64,
    pub top_level_agent_count: i64,
    #[serde(default)]
    pub status_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub project_names: Vec<String>,
    #[serde(default)]
    pub cl_names: Vec<String>,
    #[serde(default)]
    pub revived_at: Option<String>,
    #[serde(default)]
    pub times_revived: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedAgentGroupPageWire {
    pub groups: Vec<SavedAgentGroupSummaryWire>,
    pub next_cursor: Option<i64>,
}

fn default_schema_version() -> u32 {
    AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION
}
