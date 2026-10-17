//! Plan-counting facet seam.

use crate::project_stats::wire::ProjectStatsPlanCountsWire;

use super::{FacetContext, FacetResult};

pub(crate) fn query(
    _context: &FacetContext<'_>,
) -> FacetResult<ProjectStatsPlanCountsWire> {
    FacetResult::unimplemented()
}
