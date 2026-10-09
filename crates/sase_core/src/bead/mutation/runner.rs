//! One runner for every bead mutation, on both backings.
//!
//! Each ordinary mutation is a single algorithm: a closure over
//! [`MutationView`] that stages rows, mints events with `stage_event`,
//! and finishes with `commit`. [`run_mutation`] takes the flock, admits
//! the cached view, and runs that same closure again on the owned replay
//! backing when the cached path declines before any durable write. A
//! decline after a durable write, or any decline on the replay backing,
//! is a bug and fails as an `io` error, so a cache fault can never retry
//! or double-append a mutation.

use std::path::Path;

use super::mutation_wire::BeadMutationOutcomeWire;
use super::store::with_bead_mutation_lock;
use super::view::MutationView;
use crate::bead::wire::BeadError;

/// What one pass of a mutation algorithm over the view reports.
///
/// `Done` carries the finished outcome. `NeedsReplay` asks the runner to
/// discard this view and run the same algorithm again on the replay
/// backing; it is legal only before the first durable write.
pub(crate) enum MutationStep<T> {
    Done(T),
    NeedsReplay,
}

/// Run one mutation algorithm on the warmest usable backing.
///
/// Takes the `beads.db` flock, admits the cached view, and runs `run` on
/// it. When the cached path declines (`NeedsReplay`) before any durable
/// write, builds the replay backing and runs the same closure again.
/// Genuine errors propagate without a retry; store corruption on the
/// replay load fails exactly as today's replay load would.
pub(crate) fn run_mutation(
    beads_dir: &Path,
    operation_name: &str,
    run: impl Fn(
        &mut MutationView,
    ) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    with_bead_mutation_lock(beads_dir, operation_name, || {
        if let Some(mut view) = MutationView::load_cached(beads_dir)? {
            match run(&mut view) {
                Ok(MutationStep::Done(outcome)) => return Ok(outcome),
                Ok(MutationStep::NeedsReplay) if view.durably_written() => {
                    return Err(BeadError::io(format!(
                        "mutation {operation_name} declined after a durable write; a retry would double-append"
                    )));
                }
                Ok(MutationStep::NeedsReplay) => {}
                // A cache fault before any durable write replays the
                // same algorithm on the replay backing instead of
                // failing the mutation; the fault already invalidated
                // the cache best-effort so the next pass rebuilds.
                // Any other error propagates: only the cache may fail
                // open, never the store, and never after an append.
                Err(error)
                    if error.kind == "cache_fault"
                        && !view.durably_written() => {}
                Err(error) => return Err(error),
            }
        }
        let mut view = MutationView::load_replay(beads_dir)?;
        match run(&mut view)? {
            MutationStep::Done(outcome) => Ok(outcome),
            MutationStep::NeedsReplay if view.durably_written() => {
                Err(BeadError::io(format!(
                    "mutation {operation_name} declined after a durable write; a retry would double-append"
                )))
            }
            MutationStep::NeedsReplay => Err(BeadError::io(format!(
                "mutation {operation_name} declined on the replay backing; a replay decline is a bug"
            ))),
        }
    })
}
