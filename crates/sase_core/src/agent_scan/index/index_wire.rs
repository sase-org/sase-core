use crate::agent_scan::wire::{AgentArtifactRecordShapeWire, UsedXPromptWire};
use serde::{Deserialize, Serialize};

pub const AGENT_ARTIFACT_INDEX_SCHEMA_VERSION: u32 = 33;

/// Newest hidden terminal rows kept hot in the materialized SQLite view.
///
/// The artifact tree remains authoritative for older hidden terminal payloads;
/// rebuilding the index from source artifacts restores evicted history.
pub const DEFAULT_HIDDEN_TERMINAL_HOT_ROWS: u32 = 4096;

/// Schema version for indexed model-alias history queries.
pub const AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION: u32 = 1;

pub(super) fn default_alias_history_limit() -> u32 {
    10
}

pub(super) fn default_alias_history_prompt_snippet_bytes() -> u32 {
    240
}

pub(super) fn default_alias_history_freshness(
) -> AgentArtifactIndexFreshnessWire {
    AgentArtifactIndexFreshnessWire::Cached
}

/// Query knobs for bounded per-alias agent history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAliasHistoryQueryWire {
    /// Bare alias names to report on. Empty is an error.
    pub aliases: Vec<String>,
    /// Maximum runs returned per alias. Zero means unlimited.
    #[serde(default = "default_alias_history_limit")]
    pub limit_per_alias: u32,
    #[serde(default)]
    pub include_hidden: bool,
    /// Exact ProjectSpec keys. Empty means every project.
    #[serde(default)]
    pub projects: Vec<String>,
    /// Bounded read budget per returned run. Zero skips prompt reads.
    #[serde(default = "default_alias_history_prompt_snippet_bytes")]
    pub prompt_snippet_bytes: u32,
    /// Cached by default; `revalidate` refreshes matching artifact rows.
    #[serde(default = "default_alias_history_freshness")]
    pub freshness: AgentArtifactIndexFreshnessWire,
}

/// Effective limit and truncation metadata for one alias group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAliasHistoryLimitWire {
    pub limit: u32,
    pub total_count: u64,
    pub returned_count: u64,
    pub truncated: bool,
}

/// One indexed run that used a requested model alias.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAliasRunWire {
    pub artifact_dir: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub model_alias: Option<String>,
    #[serde(default)]
    pub model_alias_origin: Option<String>,
    #[serde(default)]
    pub model_alias_trail: Vec<String>,
    pub alias_position: u32,
    pub status: String,
    #[serde(default)]
    pub workflow_status: Option<String>,
    pub has_done_marker: bool,
    pub hidden: bool,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<f64>,
    #[serde(default)]
    pub retry_attempt: Option<i64>,
    #[serde(default)]
    pub bead_id: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub prompt_snippet: Option<String>,
    #[serde(default)]
    pub used_xprompts: Vec<UsedXPromptWire>,
}

/// History group for one requested alias.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAliasHistoryGroupWire {
    pub alias: String,
    pub runs_limit: AgentAliasHistoryLimitWire,
    pub runs: Vec<AgentAliasRunWire>,
}

/// Bounded alias history returned by the artifact index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAliasHistoryWire {
    pub schema_version: u32,
    pub index_path: String,
    pub query: AgentAliasHistoryQueryWire,
    pub groups: Vec<AgentAliasHistoryGroupWire>,
}

/// Freshness policy for persistent artifact index queries.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactIndexFreshnessWire {
    #[default]
    Revalidate,
    Cached,
}

/// Scalar fields that can be tested before `record_json` is decoded.
///
/// `AgentSession` reads the indexed `agent_session` column, which stores
/// `agent_meta.agent_session`. Matching is case-insensitive like every other
/// `Equals` field, so a caller that needs an exact lane match re-checks the
/// hydrated record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactCandidateFieldWire {
    Project,
    Cl,
    Model,
    Provider,
    Machine,
    Type,
    AgentSession,
}

/// Exact candidate filter compiled by Python from the agent-query AST.
///
/// This is intentionally not a user-facing query parser. Callers only send
/// boolean combinations of atoms whose parity against Python Agent evaluation
/// has been proven for indexed scalar columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentArtifactCandidateFilterWire {
    All {
        #[serde(default)]
        filters: Vec<AgentArtifactCandidateFilterWire>,
    },
    Any {
        #[serde(default)]
        filters: Vec<AgentArtifactCandidateFilterWire>,
    },
    Not {
        filter: Box<AgentArtifactCandidateFilterWire>,
    },
    Contains {
        field: AgentArtifactCandidateFieldWire,
        value: String,
    },
    Equals {
        field: AgentArtifactCandidateFieldWire,
        value: String,
    },
}

/// Query knobs for the persistent artifact index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexQueryWire {
    #[serde(default)]
    pub include_active: bool,
    #[serde(default)]
    pub include_recent_completed: bool,
    #[serde(default)]
    pub include_full_history: bool,
    #[serde(default)]
    pub active_limit: Option<u32>,
    #[serde(default)]
    pub recent_completed_limit: Option<u32>,
    #[serde(default)]
    pub include_hidden: bool,
    #[serde(default)]
    pub freshness: AgentArtifactIndexFreshnessWire,
    /// Restrict results to real monitor agent session members
    /// (`agent_meta.agent_session_role == "monitor"` and a non-empty
    /// `agent_meta.agent_session_shell.id` on a `"monitor"`-kind shell).
    #[serde(default)]
    pub only_monitors: bool,
    #[serde(default)]
    pub record_shape: AgentArtifactRecordShapeWire,
    #[serde(default)]
    pub window_limit: Option<u32>,
    #[serde(default)]
    pub candidate_filter: Option<AgentArtifactCandidateFilterWire>,
    /// When true, hydrate only records that can become Agents-list base
    /// rows. Marker-only waiting/question records stay out of the JSON
    /// decode window; their clan keys are still collected from scalar
    /// columns. Default off so generic index callers keep their current
    /// record sets.
    #[serde(default)]
    pub agents_list_projection: bool,
}

impl Default for AgentArtifactIndexQueryWire {
    fn default() -> Self {
        Self {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(200),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        }
    }
}

/// Summary of one index mutation/rebuild.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexUpdateWire {
    pub schema_version: u32,
    pub index_path: String,
    pub projects_root: String,
    pub rows_indexed: u64,
    pub rows_deleted: u64,
    pub rows_skipped: u64,
    #[serde(default)]
    pub hidden_terminal_rows_retained: u64,
    #[serde(default)]
    pub hidden_terminal_rows_pruned: u64,
}

/// Lightweight status for the persistent artifact index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexStatusWire {
    pub schema_version: u32,
    pub index_path: String,
    pub agent_artifacts_rows: u64,
    pub dismissed_agents_rows: u64,
    pub agent_artifact_aliases_rows: u64,
    pub agent_output_variables_rows: u64,
    pub agent_artifact_model_aliases_rows: u64,
    #[serde(default)]
    pub hidden_terminal_retention_limit: u64,
    #[serde(default)]
    pub hidden_terminal_rows_retained: u64,
    #[serde(default)]
    pub hidden_terminal_rows_prunable: u64,
    /// Free pages left behind by deletes; never reclaimed without a VACUUM.
    #[serde(default)]
    pub freelist_pages: u64,
    #[serde(default)]
    pub freelist_bytes: u64,
    #[serde(default)]
    pub file_size_bytes: u64,
}

/// Outcome of one `VACUUM` compaction pass over the artifact index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexVacuumWire {
    pub index_path: String,
    pub freelist_pages_before: u64,
    pub freelist_pages_after: u64,
    pub file_size_bytes_before: u64,
    pub file_size_bytes_after: u64,
    pub bytes_reclaimed: u64,
}
