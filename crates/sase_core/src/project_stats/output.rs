//! Output facet seam. A successful measurement requires complete history.

use crate::project_stats::wire::ProjectStatsOutputWire;

use super::{FacetContext, FacetResult};

pub(crate) fn query(
    _context: &FacetContext<'_>,
) -> FacetResult<ProjectStatsOutputWire> {
    // Do not publish measured zero before repository history has been walked.
    FacetResult::unimplemented()
}
