//! Wire types for the fingerprint-free ToolRun glance projections.
//!
//! Request wires use `deny_unknown_fields`; result wires stay lenient so
//! older readers keep working. The wire schema version stays
//! `TOOL_RUN_WIRE_SCHEMA_VERSION` (1): these are new wires, not changed ones.

use serde::{Deserialize, Serialize};

use super::super::wire::{ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION};

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn empty_diagnostics() -> Vec<String> {
    Vec::new()
}

/// A run is silent when `now - last_activity_ts` reaches this many seconds
/// (six missed 10 s samples). The TUI derives silence from
/// `last_activity_ts`; core only publishes the threshold.
pub const TOOL_RUN_SILENT_AFTER_SECONDS: i64 = 60;

/// `tool_run_live_glance` returns at most this many unsettled runs, newest
/// first, so a pile of unreconciled zombies stays bounded.
pub const TOOL_RUN_GLANCE_MAX_RUNS: u32 = 200;

pub const TOOL_RUN_BRIEFS_DEFAULT_LIMIT: u32 = 100;
pub const TOOL_RUN_BRIEFS_MAX_LIMIT: u32 = 500;

/// `tool_run_node_summaries` accepts this many selectors per call.
pub const TOOL_RUN_NODE_MAX_SELECTORS: usize = 64;

pub const TOOL_RUN_NODE_DEFAULT_LIMIT: u32 = 20;
pub const TOOL_RUN_NODE_MAX_LIMIT: u32 = 100;

/// Run labels (`tool_name`, or the ad-hoc argv basename) cap here.
pub const TOOL_RUN_LABEL_MAX_CHARS: usize = 12;

fn default_briefs_limit() -> u32 {
    TOOL_RUN_BRIEFS_DEFAULT_LIMIT
}

fn default_node_limit() -> u32 {
    TOOL_RUN_NODE_DEFAULT_LIMIT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunVerdictBucketWire {
    Running,
    Pass,
    NewFailures,
    KnownOnly,
    Undetermined,
    Stopped,
    Killed,
    Lost,
}

impl ToolRunVerdictBucketWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Pass => "pass",
            Self::NewFailures => "new_failures",
            Self::KnownOnly => "known_only",
            Self::Undetermined => "undetermined",
            Self::Stopped => "stopped",
            Self::Killed => "killed",
            Self::Lost => "lost",
        }
    }
}

/// One shared verdict summary. `bucket` applies the state-vocabulary
/// precedence (live first, then terminal cause, then triage verdict);
/// `verdict`/`failure_kind` carry the raw triage outcome underneath.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunVerdictSummaryWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub bucket: ToolRunVerdictBucketWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub new: u32,
    #[serde(default)]
    pub known: u32,
    #[serde(default)]
    pub flaky: u32,
    #[serde(default)]
    pub unknown: u32,
    #[serde(default)]
    pub unlabeled: u32,
}

/// The in-flight stage of a live run. Stamps are milliseconds and say so.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunGlanceStageWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub description: String,
    pub started_ms: i64,
}

/// One unsettled run: identity, attribution, timing, and progress. No
/// fingerprints, no argv beyond the label, no log paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunGlanceWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub label: String,
    pub state: ToolRunStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub created_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_ts: Option<i64>,
    pub last_activity_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_stage: Option<ToolRunGlanceStageWire>,
    pub stages_done: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stages_expected: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_ms: Option<i64>,
    #[serde(default)]
    pub typical_samples: u32,
    #[serde(default)]
    pub stop_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunLiveGlanceRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunLiveGlanceResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub store_exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_write_ts: Option<i64>,
    pub silent_after_s: i64,
    pub truncated: bool,
    pub runs: Vec<ToolRunGlanceWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

/// One `(owner_kind, owner_id)` pair for brief and node filters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunBriefOwnerWire {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunBriefsRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default)]
    pub states: Vec<ToolRunStateWire>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub owners: Vec<ToolRunBriefOwnerWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_ts: Option<i64>,
    #[serde(default = "default_briefs_limit")]
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// One lean run row for lists: outcome, attribution, timing, verdict, and
/// pruning flags. No fingerprints, no argv beyond the label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunBriefWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub label: String,
    pub state: ToolRunStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_cause: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub created_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_ms: Option<i64>,
    pub verdict: ToolRunVerdictSummaryWire,
    #[serde(default)]
    pub detail_pruned: bool,
    #[serde(default)]
    pub stop_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunBriefsResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub store_exists: bool,
    pub runs: Vec<ToolRunBriefWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

/// The core query key for one TUI node: runs match when `agent` is in
/// `agents` or `(owner_kind, owner_id)` is in `owners`, and `created_ts`
/// is at or after `since_ts`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunNodeSelectorWire {
    pub key: String,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub owners: Vec<ToolRunBriefOwnerWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunNodeSummariesRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub nodes: Vec<ToolRunNodeSelectorWire>,
    #[serde(default = "default_node_limit")]
    pub per_node_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunNodeSummaryWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub key: String,
    pub total_runs: u32,
    pub truncated: bool,
    pub live: Vec<ToolRunGlanceWire>,
    pub latest_by_tool: Vec<ToolRunBriefWire>,
    pub runs: Vec<ToolRunBriefWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunNodeSummariesResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub store_exists: bool,
    pub silent_after_s: i64,
    pub nodes: Vec<ToolRunNodeSummaryWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}
