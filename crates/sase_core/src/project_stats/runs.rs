//! Runs and Patch aggregation seam.

use crate::project_stats::wire::ProjectStatsRunsWire;

use super::{FacetContext, FacetResult};

pub(crate) fn query(
    _context: &FacetContext<'_>,
) -> FacetResult<ProjectStatsRunsWire> {
    FacetResult::unimplemented()
}
