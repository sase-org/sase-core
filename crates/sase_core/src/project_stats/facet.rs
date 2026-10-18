//! Shared facet dispatch types.

use crate::project_stats::directory::Directory;
use crate::project_stats::git::ResolvedRepo;
use crate::project_stats::history::HistoryOutcome;
use crate::project_stats::wire;

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
