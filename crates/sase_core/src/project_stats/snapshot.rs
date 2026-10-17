//! Records snapshot extension seam for tracked files and lifetime commits.

use crate::project_stats::wire::ProjectStatsSnapshotWire;

use super::{FacetContext, FacetResult};

pub(crate) fn query(
    _context: &FacetContext<'_>,
) -> FacetResult<ProjectStatsSnapshotWire> {
    FacetResult::unimplemented()
}
