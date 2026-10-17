//! Project-wide statistics contracts and query extension points.
//!
//! The first implementation phase establishes stable wire types and validation.
//! History, attribution, and facet implementations can then be added behind
//! these interfaces without changing the request or response shape.

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
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

use crate::project_stats::directory::Directory;
use crate::project_stats::history::HistoryOutcome;
/// Shared read-only inputs passed to facet implementations.
#[allow(dead_code)] // Facet owners consume their relevant shared inputs.
pub(crate) struct FacetContext<'a> {
    pub request: &'a wire::ProjectStatsRequestWire,
    pub project: &'a wire::ProjectStatsProjectSpecWire,
    pub directory: &'a Directory,
    pub histories: &'a [HistoryOutcome],
}

/// A facet can expose a measurement, source issues, and fingerprint inputs.
#[allow(dead_code)] // Issues and tokens are populated as source seams land.
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
