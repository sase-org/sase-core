//! Shared accumulator and intermediate types for run-stats aggregation.
//!
//! These types are filled by the per-dimension folds and consumed by
//! the finishing passes. They live here so no fold module widens
//! visibility just to share state.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use super::super::wire::AgentRunBucketWire;

pub(super) const INDEX_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const MAX_BUCKETS: u64 = 1_000_000;
pub(super) const UNKNOWN: &str = "unknown";
pub(super) const NO_PATCH: &str = "(no patch)";
pub(super) const DEFAULT_EFFORT: &str = "default";

#[derive(Debug)]
pub(super) struct IndexRunRow {
    pub(super) project_name: String,
    pub(super) workflow_dir_name: String,
    pub(super) workflow_name: Option<String>,
    pub(super) timestamp: String,
    pub(super) status: String,
    pub(super) cl_name: Option<String>,
    pub(super) agent_name: Option<String>,
    pub(super) model: Option<String>,
    pub(super) provider: Option<String>,
    pub(super) started_at: Option<String>,
    pub(super) finished_at: Option<f64>,
    pub(super) record_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ProviderKey {
    pub(super) provider: String,
    pub(super) model: String,
    pub(super) effort: String,
}

#[derive(Debug, Default)]
pub(super) struct ProviderAccumulator {
    pub(super) runs: u64,
    pub(super) completed: u64,
    pub(super) duration_count: u64,
    pub(super) total_runtime_seconds: f64,
}

#[derive(Debug, Default)]
pub(super) struct DurationAccumulator {
    pub(super) values: Vec<f64>,
}

#[derive(Debug, Default)]
pub(super) struct ProjectWorkAccumulator {
    pub(super) runs: u64,
    pub(super) completed: u64,
    pub(super) failed: u64,
    pub(super) other_terminal: u64,
    pub(super) in_progress: u64,
    pub(super) waiting: u64,
    pub(super) commits: u64,
    pub(super) patches: BTreeSet<String>,
    pub(super) unattributed_runs: u64,
    pub(super) total_runtime_seconds: f64,
    pub(super) last_run_ts: f64,
    pub(super) project_file: PathBuf,
}

#[derive(Debug, Default)]
pub(super) struct PatchWorkAccumulator {
    pub(super) runs: u64,
    pub(super) agents: BTreeSet<String>,
    pub(super) commits: u64,
    pub(super) total_runtime_seconds: f64,
    pub(super) first_run_ts: f64,
    pub(super) last_run_ts: f64,
}

#[derive(Debug, Default)]
pub(super) struct WorkAccumulators {
    pub(super) projects: BTreeMap<String, ProjectWorkAccumulator>,
    pub(super) patches: BTreeMap<(String, String), PatchWorkAccumulator>,
}

#[derive(Debug, Default)]
pub(super) struct XPromptAccumulator {
    pub(super) kind: String,
    pub(super) tags: Vec<String>,
    pub(super) runs: u64,
    pub(super) references: u64,
    pub(super) agents: BTreeSet<String>,
    pub(super) completed: u64,
    pub(super) failed: u64,
    pub(super) duration_count: u64,
    pub(super) total_runtime_seconds: f64,
    pub(super) first_run_ts: f64,
    pub(super) last_run_ts: f64,
    pub(super) models: BTreeMap<String, u64>,
    pub(super) projects: BTreeMap<String, u64>,
    pub(super) partners: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub(super) struct XPromptFocusAccumulator {
    pub(super) providers: BTreeMap<String, u64>,
    pub(super) tribes: BTreeMap<String, u64>,
    pub(super) buckets: Vec<AgentRunBucketWire>,
}

#[derive(Debug)]
pub(super) struct XPromptAccumulators {
    pub(super) runs_with_xprompts: u64,
    pub(super) runs_without_xprompts: u64,
    pub(super) total_references: u64,
    pub(super) by_name: BTreeMap<String, XPromptAccumulator>,
    pub(super) focus: Option<XPromptFocusAccumulator>,
}

#[derive(Debug)]
pub(super) struct RunXPrompt {
    pub(super) kind: String,
    pub(super) tags: Vec<String>,
    pub(super) references: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AttributedPatch {
    pub(super) name: String,
    pub(super) commits: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct RunAttribution {
    pub(super) patches: Vec<AttributedPatch>,
    pub(super) total_commits: u64,
}

#[derive(Debug, Clone)]
pub(super) struct PatchMetadata {
    pub(super) status: String,
    pub(super) has_pr: bool,
}
