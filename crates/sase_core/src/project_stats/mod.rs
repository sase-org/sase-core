//! Project-wide statistics contracts and query extension points.
//!
//! History, attribution, and facet implementations fill these interfaces
//! without changing the request or response shape.

mod aggregate;
mod attribution;
mod cache;
mod classify;
mod code;
mod counter;
mod directory;
mod facet;
mod git;
mod history;
mod output;
mod plans;
mod query;
mod records;
mod records_cache;
mod runs;
mod snapshot;
mod wire;

#[cfg(test)]
mod classify_tests;
#[cfg(test)]
mod counter_tests;
#[cfg(test)]
mod directory_tests;
#[cfg(test)]
mod plans_tests;
#[cfg(test)]
mod query_tests;
#[cfg(test)]
mod records_tests;
#[cfg(test)]
mod runs_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use facet::{FacetContext, FacetResult, MeasuredRepo};
pub use query::query_project_stats;
pub use wire::{
    validate_project_stats_request, ProjectStatsAgentSidecarSpecWire,
    ProjectStatsBudgetWire, ProjectStatsCodeCounterWire, ProjectStatsCodeWire,
    ProjectStatsCoverageWire, ProjectStatsError,
    ProjectStatsExclusionCountWire, ProjectStatsExclusionReasonCountWire,
    ProjectStatsExclusionReasonWire, ProjectStatsFacetWire,
    ProjectStatsFileExclusionWire, ProjectStatsFileRoleWire,
    ProjectStatsIssueWire, ProjectStatsLanguageWire, ProjectStatsLifetimeWire,
    ProjectStatsModelKindWire, ProjectStatsModelWire,
    ProjectStatsNotCountedReasonWire, ProjectStatsNotCountedWire,
    ProjectStatsOutputWire, ProjectStatsPatchWire, ProjectStatsPlanCountsWire,
    ProjectStatsPlanStorageWire, ProjectStatsPlanStoreWire,
    ProjectStatsPortfolioWire, ProjectStatsProjectSpecWire,
    ProjectStatsProjectStateWire, ProjectStatsProjectWire,
    ProjectStatsRangeWire, ProjectStatsRefWire, ProjectStatsRepoKindWire,
    ProjectStatsRepoSpecWire, ProjectStatsRepoStatusWire,
    ProjectStatsRepoTierWire, ProjectStatsRepoWire, ProjectStatsRequestWire,
    ProjectStatsResponseWire, ProjectStatsRoleCountWire, ProjectStatsRunsWire,
    ProjectStatsSelectionWire, ProjectStatsSnapshotWire,
    ProjectStatsUnknownReasonCountWire, PROJECT_STATS_CLASSIFIER_VERSION,
    PROJECT_STATS_ENGINE_VERSION, PROJECT_STATS_METRIC_POLICY_VERSION,
    PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
