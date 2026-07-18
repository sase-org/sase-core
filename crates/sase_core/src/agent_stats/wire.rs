use serde::{Deserialize, Serialize};

pub const AGENT_STATS_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_bucket_seconds() -> u64 {
    24 * 60 * 60
}

fn default_top_n() -> u32 {
    5
}

/// Dimension used by the runtime ranking in a run-statistics response.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatsRuntimeGroupByWire {
    Tribe,
    Clan,
    Family,
    #[default]
    Agent,
    Provider,
    Model,
    Workflow,
}

/// Query controls for one composite agent-run statistics snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStatsRequestWire {
    /// Inclusive Unix launch timestamp.
    pub start_ts: i64,
    /// Exclusive Unix launch timestamp.
    pub end_ts: i64,
    #[serde(default)]
    pub runtime_group_by: AgentStatsRuntimeGroupByWire,
    #[serde(default = "default_bucket_seconds")]
    pub bucket_seconds: u64,
    #[serde(default = "default_top_n")]
    pub top_n: u32,
}

/// Query controls for durable activity-log and plan statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentActivityStatsRequestWire {
    /// Inclusive Unix event/launch timestamp.
    pub start_ts: i64,
    /// Exclusive Unix event/launch timestamp.
    pub end_ts: i64,
    #[serde(default = "default_top_n")]
    pub top_n: u32,
}

/// Exact count for one named category.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStatsCountWire {
    pub name: String,
    pub count: u64,
}

/// Exact activity count plus the number of agents that contributed to it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentActivityCountWire {
    pub name: String,
    pub count: u64,
    pub distinct_agents: u64,
}

/// One numeric value in a discrete distribution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStatsDistributionWire {
    pub value: u64,
    pub count: u64,
}

/// High-level run lifecycle counts. Fields are disjoint except `runs`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunTotalsWire {
    pub runs: u64,
    pub completed: u64,
    pub failed: u64,
    pub other_terminal: u64,
    pub in_progress: u64,
    pub waiting: u64,
}

/// Retry-chain activity derived from durable run metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRetryStatsWire {
    pub chains: u64,
    pub attempts: u64,
    pub kills: u64,
}

/// One provider/model/effort combination.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentProviderStatsWire {
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub runs: u64,
    pub completed: u64,
    pub success_rate: f64,
    pub total_runtime_seconds: f64,
    pub mean_runtime_seconds: Option<f64>,
}

/// Commits-per-run buckets used by the Runs view.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCommitDistributionWire {
    pub zero: u64,
    pub one: u64,
    pub two: u64,
    pub three_plus: u64,
}

/// Commit attribution across runs in the selected window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentCommitStatsWire {
    pub total_commits: u64,
    pub committing_agents: u64,
    pub average_per_committing_agent: f64,
    pub distribution: AgentCommitDistributionWire,
    #[serde(default)]
    pub top_repos: Vec<AgentStatsCountWire>,
}

/// Plan-proposal lifecycle signals available directly in run metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPlanStatsWire {
    /// Number of entries across all `plan_submitted_at` lists.
    pub proposed: u64,
    /// Number of runs with at least one plan submission.
    pub proposing_agents: u64,
    pub approved: u64,
    pub rejected: u64,
    pub pending: u64,
    #[serde(default)]
    pub actions: Vec<AgentStatsCountWire>,
}

/// Question-session signals available directly in run metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentQuestionStatsWire {
    /// Number of entries across all `questions_submitted_at` lists.
    pub sessions: u64,
    /// Number of runs that submitted at least one question session.
    pub asking_agents: u64,
}

/// Run count for one project workspace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkspaceStatsWire {
    pub project: String,
    pub workspace_num: i64,
    pub runs: u64,
}

/// One caller-sized launch-time bucket. Zero-count buckets are retained.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunBucketWire {
    pub start_ts: i64,
    pub runs: u64,
}

/// Duration distribution for one requested runtime dimension value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentRuntimeGroupStatsWire {
    pub group: String,
    pub runs: u64,
    pub total_seconds: f64,
    pub mean_seconds: f64,
    pub p50_seconds: f64,
    pub p95_seconds: f64,
    pub max_seconds: f64,
}

/// Everything needed by the run-backed Statistics views in one response.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentRunStatsResponseWire {
    pub schema_version: u32,
    pub start_ts: i64,
    pub end_ts: i64,
    pub runtime_group_by: AgentStatsRuntimeGroupByWire,
    pub bucket_seconds: u64,
    pub totals: AgentRunTotalsWire,
    #[serde(default)]
    pub outcomes: Vec<AgentStatsCountWire>,
    pub retries: AgentRetryStatsWire,
    #[serde(default)]
    pub providers: Vec<AgentProviderStatsWire>,
    pub commits: AgentCommitStatsWire,
    pub plans: AgentPlanStatsWire,
    pub questions: AgentQuestionStatsWire,
    #[serde(default)]
    pub workspaces: Vec<AgentWorkspaceStatsWire>,
    #[serde(default)]
    pub buckets: Vec<AgentRunBucketWire>,
    #[serde(default)]
    pub runtime_groups: Vec<AgentRuntimeGroupStatsWire>,
    /// In-window rows whose cached `record_json` could not be decoded.
    pub malformed_rows_skipped: u64,
}

/// Plan-file statistics for runs that submitted plans in the selected window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentPlanActivityStatsWire {
    /// Number of entries across all in-window `plan_submitted_at` lists.
    pub proposed: u64,
    #[serde(default)]
    pub tiers: Vec<AgentStatsCountWire>,
    pub approved: u64,
    pub rejected: u64,
    pub pending: u64,
    #[serde(default)]
    pub phases_per_epic: Vec<AgentStatsDistributionWire>,
    pub mean_phases_per_epic: f64,
}

/// Durable user-question session sizes for the selected window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentQuestionActivityStatsWire {
    pub sessions: u64,
    pub questions: u64,
    #[serde(default)]
    pub questions_per_session: Vec<AgentStatsDistributionWire>,
    pub mean_questions_per_session: f64,
}

/// Everything backed by durable activity logs, question requests, and plans.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentActivityStatsResponseWire {
    pub schema_version: u32,
    pub start_ts: i64,
    pub end_ts: i64,
    #[serde(default)]
    pub skills: Vec<AgentActivityCountWire>,
    #[serde(default)]
    pub memories: Vec<AgentActivityCountWire>,
    pub plans: AgentPlanActivityStatsWire,
    pub questions: AgentQuestionActivityStatsWire,
    /// Invalid JSONL rows across the skill and memory logs.
    pub malformed_log_lines_skipped: u64,
    /// Invalid question request files or request payloads.
    pub malformed_question_files_skipped: u64,
    /// In-window index rows whose cached `record_json` could not be decoded.
    pub malformed_rows_skipped: u64,
    /// Plan proposals whose referenced or mirrored markdown could not be read.
    pub unresolved_plan_files: u64,
}
