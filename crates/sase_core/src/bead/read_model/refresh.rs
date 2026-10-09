//! Serve-or-refresh decision for a stale cache.
//!
//! Pure move from `store.rs`: the finish marker and the tail-first
//! refresh helper. No behavior change.

use std::path::Path;

use rusqlite::Connection;

use super::store::{
    note_serve_outcome, rebuild_from_replay, CacheMeta, CachedStoreSnapshot,
    Fault,
};
use super::tail;
use crate::bead::read_model::freshness::StoreSignatures;

/// How a refresh finishes: with a full snapshot for serving reads,
/// or with readiness only for admission and publication paths that load
/// exactly the rows they need afterwards and must not pay a full-row
/// deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RefreshFinish {
    Snapshot,
    Readiness,
}

/// Serve-or-refresh decision for a store whose sweep differs from the
/// cache: try the incremental tail first, and rebuild only when a tail
/// precondition fails. The fallback reason becomes the rebuild's
/// telemetry so doctor shows why the tail path was refused.
pub(super) fn refresh_changed_store(
    beads_dir: &Path,
    cache_path: &Path,
    connection: Connection,
    meta: &CacheMeta,
    token: &str,
    sweep: &StoreSignatures,
    finish: RefreshFinish,
) -> Result<Option<CachedStoreSnapshot>, Fault> {
    match tail::try_tail_apply(
        beads_dir,
        cache_path,
        &connection,
        meta,
        token,
        sweep,
        finish,
    ) {
        Ok(tail::TailDecision::Served(snapshot)) => {
            note_serve_outcome(&connection);
            Ok(snapshot)
        }
        Ok(tail::TailDecision::Tailed(snapshot)) => Ok(snapshot),
        Ok(tail::TailDecision::Fallback(reason)) => {
            let start_generation = meta.generation;
            let token = token.to_string();
            drop(connection);
            rebuild_from_replay(
                beads_dir,
                cache_path,
                &token,
                start_generation,
                &format!("tail fallback: {reason}"),
            )
            .map(Some)
        }
        Err(fault) => Err(fault),
    }
}
