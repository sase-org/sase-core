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
    /// Null when unrequested or not implemented. A requested Output facet
    /// stays present; unavailable measurements use null counts.
    pub output: Option<ProjectStatsOutputWire>,
    /// Null when unrequested or not implemented. Returned with Output when
    /// that facet is requested.
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
///
/// Count fields are null when every selected source for this measure is
/// unavailable. Measured empty repositories and empty ranges use `0`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsOutputWire {
    pub commits: Option<u64>,
    pub agent_commits: Option<u64>,
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
    pub commits: Option<u64>,
    pub agent_commits: Option<u64>,
}

/// Code size and file-class measurements.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatsCodeWire {
    pub counter: ProjectStatsCodeCounterWire,
    /// Null when no configured source could be measured. A measured empty
    /// population is `Some(0)`.
    #[serde(default)]
    pub code_files: Option<u64>,
    #[serde(default)]
    pub code_lines: Option<u64>,
    #[serde(default)]
    pub test_lines: Option<u64>,
    pub test_share: Option<f64>,
    #[serde(default)]
    pub doc_files: Option<u64>,
    #[serde(default)]
    pub doc_lines: Option<u64>,
    #[serde(default)]
    pub agent_context_files: Option<u64>,
    #[serde(default)]
    pub agent_context_lines: Option<u64>,
    #[serde(default)]
    pub languages: Option<Vec<ProjectStatsLanguageWire>>,
    pub repos_configured: u64,
    pub repos_measured: u64,
}

/// Line-count policy used for Code measurements.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsCodeCounterWire {
    Sloc,
    #[default]
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
    /// Total tracked blob entries, including symlinks and excluded blobs but
    /// excluding gitlinks.
    pub tracked_files: u64,
    /// Content-derived values are null for metadata-only records snapshots.
    #[serde(default)]
    pub tracked_lines: Option<u64>,
    #[serde(default)]
    pub test_files: Option<u64>,
    #[serde(default)]
    pub test_lines: Option<u64>,
    #[serde(default)]
    pub doc_files: Option<u64>,
    #[serde(default)]
    pub doc_lines: Option<u64>,
    #[serde(default)]
    pub agent_context_files: Option<u64>,
    #[serde(default)]
    pub agent_context_lines: Option<u64>,
    /// Records snapshots measure this count. Code snapshots leave it null.
    #[serde(default)]
    pub lifetime_commits: Option<u64>,
    /// Agent attribution is not measured by either snapshot implementation.
    #[serde(default)]
    pub lifetime_agent_commits: Option<u64>,
    #[serde(default)]
    pub languages: Option<Vec<ProjectStatsLanguageWire>>,
    pub as_of_ts: Option<i64>,
    /// Set only for committed-code snapshots.
    #[serde(default)]
    pub counter: Option<ProjectStatsCodeCounterWire>,
    /// Complete role partitions are present for code snapshots and null for
    /// metadata-only records snapshots.
    #[serde(default)]
    pub role_counts: Option<Vec<ProjectStatsRoleCountWire>>,
    #[serde(default)]
    pub exclusion_counts: Option<Vec<ProjectStatsExclusionCountWire>>,
    #[serde(default)]
    pub skipped_symlinks: Option<u64>,
    #[serde(default)]
    pub skipped_gitlinks: Option<u64>,
    /// Records use author time, matching lifetime history statistics.
    #[serde(default)]
    pub first_commit_ts: Option<i64>,
    #[serde(default)]
    pub last_commit_ts: Option<i64>,
}

/// Role assigned to an included regular file in a committed code snapshot.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsFileRoleWire {
    Source,
    Tests,
    Docs,
    AgentContext,
    ConfigData,
    Other,
}

/// File role totals; `lines` follows the snapshot's selected counter for
/// source/tests and nonblank counting for docs, context, config/data, and
/// other roles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsRoleCountWire {
    pub role: ProjectStatsFileRoleWire,
    pub files: u64,
    pub lines: u64,
}

/// Why an otherwise regular file was excluded from included roles.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsFileExclusionWire {
    Generated,
    Vendored,
    Binary,
}

/// Stable reason bucket used to audit exclusion totals.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatsExclusionReasonWire {
    Attribute,
    PathHeuristic,
    Oversize,
    NulByte,
    LfsPointer,
}

/// Number of excluded files for one reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsExclusionReasonCountWire {
    pub reason: ProjectStatsExclusionReasonWire,
    pub files: u64,
}

/// File exclusion totals and their auditable reason breakdown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStatsExclusionCountWire {
    pub exclusion: ProjectStatsFileExclusionWire,
    pub files: u64,
    #[serde(default)]
    pub reasons: Vec<ProjectStatsExclusionReasonCountWire>,
}
