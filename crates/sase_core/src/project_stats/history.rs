//! Internal commit-history contract shared by Output and later attribution.
#![allow(dead_code)] // The history-cache phase fills this contract.

use crate::project_stats::wire::ProjectStatsIssueWire;

/// One raw commit row. Cache rows before attribution so directories can change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryRow {
    pub sha: String,
    pub author_ts: i64,
    pub committer_ts: i64,
    pub is_merge: bool,
    pub agent_tag: Option<String>,
    pub model_tag: Option<String>,
}

/// A repository resolved to a verified revision and Git common directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedRepo {
    pub canonical_path: String,
    pub common_dir: String,
    pub identity: String,
    pub ref_name: String,
    pub tip: String,
    pub shallow: bool,
    pub fetched_at: Option<i64>,
}

/// Raw rows plus the completeness and issues of a history walk.
#[derive(Debug, Clone, Default)]
pub(crate) struct HistoryOutcome {
    pub rows: Vec<HistoryRow>,
    pub complete: bool,
    pub issues: Vec<ProjectStatsIssueWire>,
}

pub(crate) fn empty_history() -> HistoryOutcome {
    HistoryOutcome::default()
}
