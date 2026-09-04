use serde::{Deserialize, Serialize};

pub const AGENT_ARCHIVE_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArchiveQueryRequestWire {
    pub where_sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArchiveFacetRequestWire {
    pub where_sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    pub facet: String,
    #[serde(default = "default_facet_limit")]
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveKeyWire {
    pub source_username: String,
    pub source_machine: String,
    pub source_run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveVisibilityWire {
    pub visibility: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCapabilityFactsWire {
    pub has_metadata: bool,
    pub has_state: bool,
    pub has_commits: bool,
    pub loader_reconstructible: bool,
    pub has_prompt: bool,
    pub has_model: bool,
    pub has_llm_provider: bool,
    pub has_reasoning_effort: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCapabilitiesWire {
    pub historically_viewable: bool,
    pub durably_revivable: bool,
    pub restartable: bool,
    #[serde(default)]
    pub missing_requirements: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCapabilityValidationRequestWire {
    pub facts: AgentArchiveCapabilityFactsWire,
    #[serde(default)]
    pub asserted: Option<AgentArchiveCapabilitiesWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveSummaryWire {
    pub agent_id: String,
    pub raw_suffix: String,
    pub bundle_path: String,
    pub source_username: Option<String>,
    pub source_machine: Option<String>,
    pub source_run_id: Option<String>,
    pub archive_visibility: String,
    pub historically_viewable: bool,
    pub durably_revivable: bool,
    pub restartable: bool,
    pub missing_requirements: Vec<String>,
    pub cl_name: String,
    pub agent_name: Option<String>,
    pub status: String,
    pub start_time: Option<String>,
    pub dismissed_at: Option<String>,
    pub revived_at: Option<String>,
    pub project_name: Option<String>,
    pub model: Option<String>,
    pub runtime: Option<String>,
    pub llm_provider: Option<String>,
    pub step_index: Option<i64>,
    pub step_name: Option<String>,
    pub step_type: Option<String>,
    pub retry_attempt: i64,
    pub is_workflow_child: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveQueryPageWire {
    pub results: Vec<AgentArchiveSummaryWire>,
    pub next_cursor: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveFacetCountsWire {
    pub facet: String,
    pub counts: Vec<AgentArchiveFacetCountWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveFacetCountWire {
    pub value: String,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveReviveMarkRequestWire {
    #[serde(default)]
    pub bundle_paths: Vec<String>,
    #[serde(default)]
    pub keys: Vec<AgentArchiveKeyWire>,
    pub revived_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveLifecycleFailureWire {
    pub bundle_path: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveReviveMarkReportWire {
    pub ok: bool,
    pub matched: i64,
    pub changed: i64,
    pub failed: Vec<AgentArchiveLifecycleFailureWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveVerifyReportWire {
    pub ok: bool,
    pub indexed_rows: i64,
    pub valid_bundles: i64,
    pub corrupt_bundles: i64,
    pub stale_rows: i64,
    pub missing_rows: i64,
    pub fts_missing_rows: i64,
    pub fts_orphan_rows: i64,
    pub payload_hash_mismatches: i64,
    pub orphan_visibility_rows: i64,
    pub orphan_revision_rows: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchivePurgeReportWire {
    pub ok: bool,
    pub matched: i64,
    pub purged: i64,
    pub failed: Vec<AgentArchiveLifecycleFailureWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArchiveScrubReportWire {
    pub ok: bool,
    pub matched: i64,
    pub scrubbed: i64,
    pub unchanged: i64,
    pub failed: Vec<AgentArchiveLifecycleFailureWire>,
}

fn default_limit() -> i64 {
    50
}

fn default_facet_limit() -> i64 {
    20
}
