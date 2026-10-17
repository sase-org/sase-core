//! Code facet seam.

use crate::project_stats::wire::ProjectStatsCodeWire;

use super::{snapshot, FacetContext, FacetResult};

pub(crate) fn query(
    context: &FacetContext<'_>,
) -> FacetResult<ProjectStatsCodeWire> {
    // Retain the snapshot seam so later Code work can consume tracked records.
    let _snapshot = snapshot::query(context);
    FacetResult::unimplemented()
}
