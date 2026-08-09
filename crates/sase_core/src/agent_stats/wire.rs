use serde::{Deserialize, Serialize};

pub const AGENT_STATS_WIRE_SCHEMA_VERSION: u32 = 5;

fn default_bucket_seconds() -> u64 {
    24 * 60 * 60
}

fn default_top_n() -> u32 {
    5
}

fn default_work_top_n() -> u32 {
    50
}

fn default_xprompt_top_n() -> u32 {
    40
}

fn default_xprompt_breakdown_n() -> u32 {
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
    Project,
    #[serde(alias = "changespec")]
    Patch,
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
    /// Exact artifact-index project name to include, or every project.
    #[serde(default)]
    pub project: Option<String>,
    /// Maximum number of Patch work rows returned.
    #[serde(default = "default_work_top_n")]
    pub work_top_n: u32,
    /// Maximum number of ranked xprompt rows returned.
    #[serde(default = "default_xprompt_top_n")]
    pub xprompt_top_n: u32,
    /// Maximum number of model/project/partner rows per ranked xprompt.
    #[serde(default = "default_xprompt_breakdown_n")]
    pub xprompt_breakdown_top_n: u32,
    /// Exact xprompt name to include as an unbounded focused breakdown.
    #[serde(default)]
    pub xprompt_focus: Option<String>,
}

/// Query controls for durable activity-log and plan statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentActivityStatsRequestWire {
    /// Inclusive Unix event timestamp.
    pub start_ts: i64,
    /// Exclusive Unix event timestamp.
    pub end_ts: i64,
    #[serde(default = "default_top_n")]
    pub top_n: u32,
    /// Exact project directory name used to scope all durable activity.
    #[serde(default)]
    pub project: Option<String>,
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

/// Index-derived plan signals retained for wire compatibility.
///
/// These fields under-count handed-off planner agents and must not be used for
/// user-facing totals. Use [`AgentPlanActivityStatsWire`] instead.
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

/// Index-derived question signals retained for wire compatibility.
///
/// These fields can diverge from the durable gate store and must not be used
/// for user-facing totals. Use [`AgentQuestionActivityStatsWire`] instead.
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

/// Work performed in one project over the selected window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentProjectWorkStatsWire {
    pub project: String,
    pub runs: u64,
    pub completed: u64,
    pub failed: u64,
    pub other_terminal: u64,
    pub in_progress: u64,
    pub waiting: u64,
    pub success_rate: f64,
    pub commits: u64,
    #[serde(rename = "distinct_changespecs", alias = "distinct_patches")]
    pub distinct_patches: u64,
    pub unattributed_runs: u64,
    pub total_runtime_seconds: f64,
    pub last_run_ts: f64,
}

/// Work attributed to one Patch in one project.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentPatchWorkStatsWire {
    pub project: String,
    pub name: String,
    pub status: String,
    pub has_pr: bool,
    pub runs: u64,
    pub distinct_agents: u64,
    pub commits: u64,
    pub total_runtime_seconds: f64,
    pub first_run_ts: f64,
    pub last_run_ts: f64,
}

/// Backward-compatible Rust alias for older callers naming ChangeSpec rows.
pub type AgentChangeSpecWorkStatsWire = AgentPatchWorkStatsWire;

/// Per-project and per-Patch work attribution for a run snapshot.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentWorkStatsWire {
    #[serde(default)]
    pub projects: Vec<AgentProjectWorkStatsWire>,
    #[serde(default, rename = "changespecs", alias = "patches")]
    pub patches: Vec<AgentPatchWorkStatsWire>,
    pub unattributed_runs: u64,
    #[serde(
        rename = "truncated_changespec_rows",
        alias = "truncated_patch_rows"
    )]
    pub truncated_patch_rows: u64,
    /// Missing, unreadable, or malformed active/archive project files.
    pub malformed_spec_files_skipped: u64,
}

/// One caller-sized launch-time bucket. Zero-count buckets are retained.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunBucketWire {
    pub start_ts: i64,
    pub runs: u64,
}

/// Ranked launch-boundary usage for one xprompt.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentXPromptStatsRowWire {
    pub name: String,
    pub kind: String,
    pub tags: Vec<String>,
    pub runs: u64,
    pub references: u64,
    pub distinct_agents: u64,
    pub completed: u64,
    pub failed: u64,
    pub success_rate: f64,
    pub total_runtime_seconds: f64,
    pub mean_runtime_seconds: Option<f64>,
    pub first_run_ts: f64,
    pub last_run_ts: f64,
    pub models: Vec<AgentStatsCountWire>,
    pub projects: Vec<AgentStatsCountWire>,
    pub partners: Vec<AgentStatsCountWire>,
}

/// Full launch-boundary usage breakdown for one requested xprompt.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentXPromptFocusWire {
    pub name: String,
    pub found: bool,
    pub kind: String,
    pub tags: Vec<String>,
    pub runs: u64,
    pub references: u64,
    pub distinct_agents: u64,
    pub completed: u64,
    pub failed: u64,
    pub success_rate: f64,
    pub total_runtime_seconds: f64,
    pub mean_runtime_seconds: Option<f64>,
    pub first_run_ts: f64,
    pub last_run_ts: f64,
    pub models: Vec<AgentStatsCountWire>,
    pub providers: Vec<AgentStatsCountWire>,
    pub projects: Vec<AgentStatsCountWire>,
    pub partners: Vec<AgentStatsCountWire>,
    pub tribes: Vec<AgentStatsCountWire>,
    pub buckets: Vec<AgentRunBucketWire>,
}

/// Launch-boundary xprompt usage across the selected run window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentXPromptStatsWire {
    pub runs_with_xprompts: u64,
    pub runs_without_xprompts: u64,
    pub distinct_xprompts: u64,
    pub total_references: u64,
    pub rows: Vec<AgentXPromptStatsRowWire>,
    pub truncated_rows: u64,
    pub focus: Option<AgentXPromptFocusWire>,
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

/// Exact wall-clock duration and share observed at one runner count.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentRunnerOccupancyWire {
    pub runners: u64,
    pub seconds: f64,
    pub share: f64,
}

/// One bounded, contiguous slice of the runner-occupancy trend.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentRunnerTrendSliceWire {
    pub start_ts: f64,
    pub end_ts: f64,
    pub average_runners: f64,
    pub peak_runners: u64,
    pub busy_seconds: f64,
    pub runner_seconds: f64,
}

/// Historical runner-slot occupancy over one effective analysis window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentRunnerStatsWire {
    /// Inclusive effective analysis boundary. For all-time requests this is
    /// the first valid active runner segment, not the Unix epoch sentinel.
    pub start_ts: f64,
    /// Exclusive effective analysis boundary.
    pub end_ts: f64,
    pub peak_runners: u64,
    pub peak_seconds: f64,
    pub average_runners: f64,
    pub busy_seconds: f64,
    pub busy_share: f64,
    pub runner_seconds: f64,
    /// Ordered, dense rows from zero runners through `peak_runners`.
    #[serde(default)]
    pub distribution: Vec<AgentRunnerOccupancyWire>,
    /// Chronological, contiguous slices bounded by the backend limit.
    #[serde(default)]
    pub trend: Vec<AgentRunnerTrendSliceWire>,
    /// Runner-eligible lanes that contributed at least one clipped interval.
    #[serde(default)]
    pub lanes_counted: u64,
    /// Runner-eligible lanes omitted because no trustworthy end was available.
    #[serde(default)]
    pub lanes_without_end_skipped: u64,
    /// Candidate rows omitted because `agent_meta.hidden` records user intent.
    #[serde(default)]
    pub user_hidden_skipped: u64,
    /// Overlap candidates whose cached `record_json` could not be decoded.
    pub malformed_rows_skipped: u64,
    /// Eligible records with malformed, reversed, or zero-length bounds.
    pub invalid_intervals_skipped: u64,
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
    pub work: AgentWorkStatsWire,
    #[serde(default)]
    pub buckets: Vec<AgentRunBucketWire>,
    #[serde(default)]
    pub runtime_groups: Vec<AgentRuntimeGroupStatsWire>,
    /// Runner occupancy is absent only when an all-time request has no valid
    /// active runner coverage (or when reading an older/partial payload).
    #[serde(default)]
    pub runners: Option<AgentRunnerStatsWire>,
    /// Launch-boundary xprompt usage is absent in older response payloads.
    #[serde(default)]
    pub xprompts: Option<AgentXPromptStatsWire>,
    /// In-window rows whose cached `record_json` could not be decoded.
    pub malformed_rows_skipped: u64,
}

/// Durable plan-gate statistics for proposals submitted in the selected window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentPlanActivityStatsWire {
    /// Number of in-window plan and epic-plan gate bundles.
    pub proposed: u64,
    /// Number of distinct producing agents across those bundles.
    #[serde(default)]
    pub proposing_agents: u64,
    #[serde(default)]
    pub tiers: Vec<AgentStatsCountWire>,
    pub approved: u64,
    pub rejected: u64,
    #[serde(default)]
    pub feedback: u64,
    pub pending: u64,
    #[serde(default)]
    pub phases_per_epic: Vec<AgentStatsDistributionWire>,
    pub mean_phases_per_epic: f64,
}

/// Durable user-question session sizes for the selected window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentQuestionActivityStatsWire {
    pub sessions: u64,
    /// Number of distinct producing agents across those sessions.
    #[serde(default)]
    pub asking_agents: u64,
    pub questions: u64,
    #[serde(default)]
    pub questions_per_session: Vec<AgentStatsDistributionWire>,
    pub mean_questions_per_session: f64,
}

/// Everything backed by durable activity logs and notification-gate bundles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentActivityStatsResponseWire {
    pub schema_version: u32,
    pub start_ts: i64,
    pub end_ts: i64,
    /// Earliest valid plan, epic-plan, or question gate timestamp observed.
    #[serde(default)]
    pub coverage_start_ts: Option<f64>,
    #[serde(default)]
    pub skills: Vec<AgentActivityCountWire>,
    #[serde(default)]
    pub memories: Vec<AgentActivityCountWire>,
    pub plans: AgentPlanActivityStatsWire,
    pub questions: AgentQuestionActivityStatsWire,
    /// Invalid JSONL rows across the skill and memory logs.
    pub malformed_log_lines_skipped: u64,
    /// Missing or malformed durable question-gate bundles.
    pub malformed_question_files_skipped: u64,
    /// Missing or malformed durable plan-gate bundles.
    pub malformed_rows_skipped: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn older_run_stats_payload_without_runners_deserializes() {
        let mut payload =
            serde_json::to_value(AgentRunStatsResponseWire::default()).unwrap();
        let Value::Object(fields) = &mut payload else {
            panic!("run statistics response must serialize as an object");
        };
        fields.remove("runners");

        let decoded: AgentRunStatsResponseWire =
            serde_json::from_value(payload).unwrap();
        assert!(decoded.runners.is_none());
    }

    #[test]
    fn older_run_stats_payload_without_xprompts_deserializes() {
        let mut payload =
            serde_json::to_value(AgentRunStatsResponseWire::default()).unwrap();
        let Value::Object(fields) = &mut payload else {
            panic!("run statistics response must serialize as an object");
        };
        fields.remove("xprompts");

        let decoded: AgentRunStatsResponseWire =
            serde_json::from_value(payload).unwrap();
        assert!(decoded.xprompts.is_none());
    }

    #[test]
    fn older_run_stats_request_uses_xprompt_defaults() {
        let decoded: AgentRunStatsRequestWire = serde_json::from_value(
            serde_json::json!({"start_ts": 1, "end_ts": 2}),
        )
        .unwrap();

        assert_eq!(decoded.xprompt_top_n, 40);
        assert_eq!(decoded.xprompt_breakdown_top_n, 5);
        assert_eq!(decoded.xprompt_focus, None);
    }
}
