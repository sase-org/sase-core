//! Project-wide statistics contracts and query extension points.
//!
//! History, attribution, and facet implementations fill these interfaces
//! without changing the request or response shape.

mod aggregate;
mod attribution;
mod cache;
mod code;
mod directory;
mod git;
mod history;
mod output;
mod plans;
mod query;
mod runs;
mod snapshot;
mod wire;

#[cfg(test)]
mod query_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

use crate::project_stats::directory::Directory;
use crate::project_stats::git::ResolvedRepo;
use crate::project_stats::history::HistoryOutcome;

/// One configured repository after unique resolve/walk.
pub(crate) struct MeasuredRepo<'a> {
    pub spec: &'a wire::ProjectStatsRepoSpecWire,
    pub tier: wire::ProjectStatsRepoTierWire,
    pub resolved: Option<&'a ResolvedRepo>,
    pub history: Option<&'a HistoryOutcome>,
    pub issue: Option<&'a wire::ProjectStatsIssueWire>,
}

/// Shared read-only inputs passed to facet implementations.
pub(crate) struct FacetContext<'a> {
    pub request: &'a wire::ProjectStatsRequestWire,
    pub project: &'a wire::ProjectStatsProjectSpecWire,
    pub directory: &'a Directory,
    pub repos: &'a [MeasuredRepo<'a>],
}

/// A facet can expose a measurement, source issues, and fingerprint inputs.
pub(crate) struct FacetResult<T> {
    pub value: Option<T>,
    pub issues: Vec<wire::ProjectStatsIssueWire>,
    pub source_tokens: Vec<String>,
}

impl<T> FacetResult<T> {
    pub(crate) fn unimplemented() -> Self {
        Self {
            value: None,
            issues: Vec::new(),
            source_tokens: Vec::new(),
        }
    }
}

pub use query::query_project_stats;
pub use wire::{
    validate_project_stats_request, ProjectStatsAgentSidecarSpecWire,
    ProjectStatsBudgetWire, ProjectStatsCodeCounterWire, ProjectStatsCodeWire,
    ProjectStatsCoverageWire, ProjectStatsError, ProjectStatsFacetWire,
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
    ProjectStatsResponseWire, ProjectStatsRunsWire, ProjectStatsSelectionWire,
    ProjectStatsSnapshotWire, ProjectStatsUnknownReasonCountWire,
    PROJECT_STATS_CLASSIFIER_VERSION, PROJECT_STATS_ENGINE_VERSION,
    PROJECT_STATS_METRIC_POLICY_VERSION, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
