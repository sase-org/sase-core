use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent_archive::AgentArchiveKeyWire;
use crate::query::types::QueryErrorWire;

/// Index state observed by the caller before starting a corpus compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AgentArchiveIndexProbeStatusWire {
    Missing,
    Rebuilding { indexed_rows: i64 },
    Ok,
}

/// Typed explanation for a present index whose schema cannot be compiled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveUnsupportedIndexWire {
    pub expected_schema_version: u32,
    pub actual_schema_version: Option<String>,
}

/// Completeness reported with every compiled archive corpus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AgentArchiveCorpusStatusWire {
    Missing,
    Rebuilding {
        indexed_rows: i64,
    },
    Unsupported {
        error: AgentArchiveUnsupportedIndexWire,
    },
    Ok,
}

/// Optional link information supplied by the Python catalog builder.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveLinkFacetsWire {
    #[serde(default)]
    pub relations: Vec<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub count: i64,
}

/// Inputs needed to compile the current v3 archive index into an immutable corpus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCompileRequestWire {
    pub root: String,
    pub index_status: AgentArchiveIndexProbeStatusWire,
    /// IANA timezone name (for example `America/New_York`) or a fixed UTC offset.
    pub timezone: String,
    #[serde(default)]
    pub link_facets: BTreeMap<String, AgentArchiveLinkFacetsWire>,
}

/// The status outcome derived from the shared agent status table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentArchiveOutcomeWire {
    Done,
    Failed,
    Interrupted,
}

/// Source used for a row's last activity timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentArchiveTimeBasisWire {
    Ended,
    Dismissed,
    Started,
}

/// One top-level hidden archive row with fields derived at compile time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCorpusRowWire {
    pub archive_key: AgentArchiveKeyWire,
    pub container_key: String,
    pub container_label: Option<String>,
    pub agent_name: Option<String>,
    pub canonical_name: Option<String>,
    pub global_name: Option<String>,
    pub session: Option<String>,
    pub clan: Option<String>,
    pub clan_generation: Option<String>,
    pub project: Option<String>,
    pub role: Option<String>,
    pub workflow: Option<String>,
    pub kind: String,
    pub status: String,
    pub outcome: AgentArchiveOutcomeWire,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub tab: Option<String>,
    pub tribe: Option<String>,
    pub clan_tribe: Option<String>,
    pub last_activity_at: Option<String>,
    pub time_basis: Option<AgentArchiveTimeBasisWire>,
    pub start_time: Option<String>,
    pub runtime_seconds: Option<i64>,
    pub attempt: i64,
    pub retry: bool,
    pub restorable: bool,
    pub linked: bool,
    pub relations: Vec<String>,
    pub artifacts: Vec<String>,
}

/// One container's all-member aggregate, ready for query-specific filtering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveContainerWire {
    pub key: String,
    pub label: Option<String>,
    pub member_keys: Vec<AgentArchiveKeyWire>,
    pub member_count: i64,
    pub last_activity_at: Option<String>,
    pub outcome: AgentArchiveOutcomeWire,
}

/// A name alias that resolves to the top-level archive owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveNameMatchWire {
    pub owner_key: AgentArchiveKeyWire,
    pub is_workflow_child: bool,
}

/// Cached-ready corpus data used by summary, rows, lookup, and count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCorpusWire {
    pub status: AgentArchiveCorpusStatusWire,
    /// IANA name or fixed UTC offset used when compiling naive timestamps
    /// and when grouping rows by local day.
    pub timezone: String,
    pub rows: Vec<AgentArchiveCorpusRowWire>,
    pub containers: Vec<AgentArchiveContainerWire>,
    /// Normalized short, canonical, and global names map to one or more owners.
    pub name_map: BTreeMap<String, Vec<AgentArchiveNameMatchWire>>,
}

/// Query plus the compiled `agents-archive` profile wire used to evaluate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveCountRequestWire {
    #[serde(default)]
    pub query: String,
    pub profile: Value,
}

/// Group matching presentation roots by day, project, outcome, or model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveSummaryRequestWire {
    #[serde(default)]
    pub query: String,
    pub profile: Value,
    pub group_by: String,
}

/// One window of light rows inside a single group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveRowsRequestWire {
    #[serde(default)]
    pub query: String,
    pub profile: Value,
    pub group_by: String,
    pub group_key: String,
    #[serde(default)]
    pub offset: i64,
    pub limit: i64,
    #[serde(default)]
    pub expanded_container_keys: Vec<String>,
}

/// Exact name lookup against the compiled name map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveLookupRequestWire {
    pub name: String,
}

/// Typed query-surface error, including the query engine's own errors.
#[derive(
    Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize,
)]
pub enum AgentArchiveQueryError {
    #[error(transparent)]
    Query(#[from] QueryErrorWire),
    #[error(
        "unsupported archive group_by {0:?}; expected day, project, outcome, or model"
    )]
    UnknownGroupBy(String),
}

/// Light presentation row returned by `rows` and `lookup`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveLightRowWire {
    pub archive_key: AgentArchiveKeyWire,
    pub container_key: String,
    pub container: bool,
    pub member_count: i64,
    pub agent_name: Option<String>,
    pub status: String,
    pub outcome: AgentArchiveOutcomeWire,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub project: Option<String>,
    pub last_activity_at: Option<String>,
    pub time_basis: Option<AgentArchiveTimeBasisWire>,
    pub start_time: Option<String>,
    pub runtime_seconds: Option<i64>,
    pub restorable: bool,
}

/// One populated group in a summary response. Empty groups are omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveQueryGroupWire {
    pub key: String,
    pub count: i64,
    pub last_activity_at: Option<String>,
}

/// Matching presentation-root total, groups, and index completeness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveQuerySummaryWire {
    pub total: i64,
    pub groups: Vec<AgentArchiveQueryGroupWire>,
    pub status: AgentArchiveCorpusStatusWire,
}

/// One paged window of flattened light rows inside a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveQueryRowsWire {
    pub rows: Vec<AgentArchiveLightRowWire>,
    pub offset: i64,
    pub limit: i64,
    pub total: i64,
}

/// Number of matching presentation roots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveQueryCountWire {
    pub count: i64,
}

/// Exact lookup result. No match is empty rather than an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveQueryLookupWire {
    pub row: Option<AgentArchiveLightRowWire>,
}
