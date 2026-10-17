use serde::{Deserialize, Serialize};

use super::request::{
    ProjectStatsNotCountedWire, ProjectStatsProjectStateWire,
    ProjectStatsRepoKindWire,
};

pub const PROJECT_STATS_METRIC_POLICY_VERSION: u32 = 1;
pub const PROJECT_STATS_CLASSIFIER_VERSION: u32 = 1;
pub const PROJECT_STATS_ENGINE_VERSION: u32 = 1;

/// Versions and measurements returned by one project statistics query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsResponseWire {
    pub schema_version: u32,
    pub metric_policy_version: u32,
    pub classifier_version: u32,
    pub engine_version: u32,
    pub range: ProjectStatsRangeWire,
    pub fingerprint: String,
    pub projects: Vec<ProjectStatsProjectWire>,
    pub portfolio: ProjectStatsPortfolioWire,
    pub issues: Vec<ProjectStatsIssueWire>,
}

/// Half-open Output query interval.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct ProjectStatsRangeWire {
    pub start_ts: i64,
    pub end_ts: i64,
}

/// One source issue. Missing data is reported here while successful sources
/// remain represented in the associated measurements and coverage.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsIssueWire {
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub hint: Option<String>,
}

/// Measurements for one configured project.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsProjectWire {
    pub key: String,
    pub label: String,
    pub state: ProjectStatsProjectStateWire,
    /// Null when unrequested, not implemented, or unavailable as a whole.
    pub output: Option<ProjectStatsOutputWire>,
    /// Null when unrequested, not implemented, or unavailable as a whole.
    pub lifetime: Option<ProjectStatsLifetimeWire>,
    /// Null when unrequested, not implemented, or unavailable as a whole.
    pub code: Option<ProjectStatsCodeWire>,
    /// Null when unrequested, not implemented, or unavailable as a whole.
    pub runs: Option<ProjectStatsRunsWire>,
    pub repos: Vec<ProjectStatsRepoWire>,
    pub not_counted: Vec<ProjectStatsNotCountedWire>,
    pub models: Vec<ProjectStatsModelWire>,
    pub patches: Vec<ProjectStatsPatchWire>,
    pub truncated_patch_rows: u64,
    pub coverage: ProjectStatsCoverageWire,
}

/// Deduplicated aggregates across selected projects.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsPortfolioWire {
    pub output: Option<ProjectStatsOutputWire>,
    pub lifetime: Option<ProjectStatsLifetimeWire>,
    pub code: Option<ProjectStatsCodeWire>,
    pub runs: Option<ProjectStatsRunsWire>,
    pub models: Vec<ProjectStatsModelWire>,
    pub coverage: ProjectStatsCoverageWire,
}

/// Output and model measurements for a project or repository.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsOutputWire {
    pub commits: u64,
    pub agent_commits: u64,
    pub agent_share: Option<f64>,
    pub plans_committed: Option<u64>,
    pub plans_epic: Option<u64>,
    pub plans_tale: Option<u64>,
    pub models: Vec<ProjectStatsModelWire>,
    pub commit_coverage: ProjectStatsCoverageWire,
    pub plan_coverage: ProjectStatsCoverageWire,
}

/// Plan counts remain nullable until the plan source is measured.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsPlanCountsWire {
    pub plans_committed: Option<u64>,
    pub plans_epic: Option<u64>,
    pub plans_tale: Option<u64>,
}

/// Lifetime history, independent of the requested Output date range.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsLifetimeWire {
    pub first_commit_ts: Option<i64>,
    pub last_commit_ts: Option<i64>,
    pub age_days: Option<u64>,
    pub active_days: Option<u64>,
    pub commits: u64,
    pub agent_commits: u64,
}

/// Code size and file-class measurements.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsCodeWire {
    pub counter: ProjectStatsCodeCounterWire,
    pub code_files: u64,
    pub code_lines: u64,
    pub test_lines: u64,
    pub test_share: Option<f64>,
    pub doc_files: u64,
    pub doc_lines: u64,
    pub agent_context_files: u64,
    pub agent_context_lines: u64,
    pub languages: Vec<ProjectStatsLanguageWire>,
    pub repos_configured: u64,
    pub repos_measured: u64,
}

/// Line-count policy used for Code measurements.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsCodeCounterWire {
    #[default]
    Sloc,
    Nonblank,
}

/// File and line totals attributed to a language.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsLanguageWire {
    pub name: String,
    pub files: u64,
    pub lines: u64,
}

/// Run lifecycle and runtime measurements.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsRunsWire {
    pub runs: u64,
    pub completed: u64,
    pub failed: u64,
    pub other_terminal: u64,
    pub in_progress: u64,
    pub waiting: u64,
    pub success_rate: Option<f64>,
    pub total_runtime_seconds: f64,
    pub distinct_patches: u64,
    pub unattributed_runs: u64,
    pub last_run_ts: Option<i64>,
}

/// Attribution bucket for one model row.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsModelKindWire {
    Resolved,
    #[default]
    Unknown,
    NoAgent,
}

/// One resolved or unresolved agent/model attribution bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsModelWire {
    pub kind: ProjectStatsModelKindWire,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub model_is_alias: bool,
    pub commits: u64,
    /// Null until plan measurements are available.
    pub plans: Option<u64>,
    pub plans_epic: Option<u64>,
    pub plans_tale: Option<u64>,
    pub unknown_reasons: Vec<String>,
}

/// One Patch row aggregated from run data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsPatchWire {
    pub name: String,
    pub status: String,
    pub has_pr: bool,
    pub runs: u64,
    pub completed: u64,
    pub failed: u64,
    pub run_commits: u64,
    pub total_runtime_seconds: u64,
    pub first_run_ts: Option<i64>,
    pub last_run_ts: Option<i64>,
}

/// Source coverage separates repository availability from attribution quality.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsCoverageWire {
    pub configured_repos: u64,
    pub measured_repos: u64,
    pub partial_repos: u64,
    pub unavailable_repos: u64,
    pub agent_tagged_commits: u64,
    pub resolved_agent_commits: u64,
    pub unknown_agent_commits: u64,
    pub unknown_reasons: Vec<ProjectStatsUnknownReasonCountWire>,
}

/// Number of commits associated with one attribution uncertainty reason.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsUnknownReasonCountWire {
    pub reason: String,
    pub count: u64,
}

/// Measurement tier for one repository source.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsRepoTierWire {
    #[default]
    Code,
    Records,
}

/// Availability of a configured repository measurement.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsRepoStatusWire {
    Ok,
    #[default]
    Unavailable,
    Partial,
}

/// Selected reference and commit for a repository history walk.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsRefWire {
    pub name: String,
    pub commit: String,
    pub shallow: bool,
    pub fetched_at: Option<i64>,
}

/// Per-repository measurements and source health.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsRepoWire {
    pub id: String,
    pub name: String,
    pub kind: ProjectStatsRepoKindWire,
    pub tier: ProjectStatsRepoTierWire,
    pub status: ProjectStatsRepoStatusWire,
    pub issue: Option<ProjectStatsIssueWire>,
    #[serde(rename = "ref")]
    pub reference: Option<ProjectStatsRefWire>,
    pub output: Option<ProjectStatsOutputWire>,
    pub lifetime: Option<ProjectStatsLifetimeWire>,
    pub snapshot: Option<ProjectStatsSnapshotWire>,
}

/// Tracked-file and lifetime-commit facts from a record repository snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsSnapshotWire {
    pub tracked_files: u64,
    pub tracked_lines: u64,
    pub test_files: u64,
    pub test_lines: u64,
    pub doc_files: u64,
    pub doc_lines: u64,
    pub agent_context_files: u64,
    pub agent_context_lines: u64,
    pub lifetime_commits: u64,
    pub lifetime_agent_commits: u64,
    pub languages: Vec<ProjectStatsLanguageWire>,
    pub as_of_ts: Option<i64>,
}
