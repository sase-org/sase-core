mod request;
mod response;

pub use request::{
    validate_project_stats_request, ProjectStatsAgentSidecarSpecWire,
    ProjectStatsBudgetWire, ProjectStatsFacetWire,
    ProjectStatsNotCountedReasonWire, ProjectStatsNotCountedWire,
    ProjectStatsPlanStorageWire, ProjectStatsPlanStoreWire,
    ProjectStatsProjectSpecWire, ProjectStatsProjectStateWire,
    ProjectStatsRepoKindWire, ProjectStatsRepoSpecWire,
    ProjectStatsRequestWire, ProjectStatsSelectionWire,
    PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
pub use response::{
    ProjectStatsCodeCounterWire, ProjectStatsCodeWire,
    ProjectStatsCoverageWire, ProjectStatsExclusionCountWire,
    ProjectStatsExclusionReasonCountWire, ProjectStatsExclusionReasonWire,
    ProjectStatsFileExclusionWire, ProjectStatsFileRoleWire,
    ProjectStatsIssueWire, ProjectStatsLanguageWire, ProjectStatsLifetimeWire,
    ProjectStatsModelKindWire, ProjectStatsModelWire, ProjectStatsOutputWire,
    ProjectStatsPatchWire, ProjectStatsPlanCountsWire,
    ProjectStatsPortfolioWire, ProjectStatsProjectWire, ProjectStatsRangeWire,
    ProjectStatsRefWire, ProjectStatsRepoStatusWire, ProjectStatsRepoTierWire,
    ProjectStatsRepoWire, ProjectStatsResponseWire, ProjectStatsRoleCountWire,
    ProjectStatsRunsWire, ProjectStatsSnapshotWire,
    ProjectStatsUnknownReasonCountWire, PROJECT_STATS_CLASSIFIER_VERSION,
    PROJECT_STATS_ENGINE_VERSION, PROJECT_STATS_METRIC_POLICY_VERSION,
};

use thiserror::Error;

/// Errors that make a project statistics request structurally invalid.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProjectStatsError {
    #[error("unsupported project statistics schema version {actual}; expected {expected}")]
    UnsupportedSchema { actual: u32, expected: u32 },
    #[error("invalid project statistics timezone: {0}")]
    InvalidTimezone(String),
    #[error("project statistics range ends before it starts")]
    ReversedRange,
    #[error("project statistics budget values must be nonzero")]
    InvalidBudget,
    #[error("invalid project statistics request identity: {0}")]
    InvalidIdentity(String),
    #[error("project statistics request repeats facet `{0}`")]
    DuplicateFacet(String),
    #[error("could not fingerprint project statistics request: {0}")]
    FingerprintSerialization(String),
}
