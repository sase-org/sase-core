//! Versioned SQLite read model over bead event stores.
//!
//! The cache lives under the clone's git dir (see `location`), serves
//! unchanged-store reads without replaying history (see `freshness` for
//! the O(1) token protocol), applies appended events incrementally after
//! the merge frontier (see `tail`), and rebuilds from a full replay
//! whenever a tail precondition fails (see `store`).

pub(crate) mod alloc;
mod freshness;
mod location;
pub(crate) mod publish;
pub mod queries;
mod store;
mod tail;
#[cfg(test)]
mod tests;

pub use freshness::{sweep_store_signatures, READ_MODEL_SWEEP_INTERVAL_SECS};
pub use location::read_model_cache_path_for_store;
pub(crate) use publish::{
    publish_mutation_write, AppendedStream, CacheWitness, PublishOutcome,
};
pub use queries::{
    cached_blocked, cached_blocked_ids, cached_board, cached_closed_ids,
    cached_detail, cached_epic_children, cached_list, cached_ready,
    cached_ready_ids, cached_resolve, cached_search, cached_show, cached_stats,
    cached_statuses_for_ids, lineage_history_streams, with_fresh_cache_at,
    BoardView, ListPage,
};
pub use store::{
    cached_store_snapshot, cached_store_snapshot_at, ensure_cache_ready,
    ensure_cache_ready_at, ensure_cache_ready_for_mutation,
    ensure_cache_ready_for_mutation_at, read_model_status,
    read_model_status_at, read_model_verify_cache, read_model_verify_cache_at,
    rebuild_read_model_at, BeadReadModelStatusWire, BeadReadModelVerifyWire,
    CachedStoreSnapshot, BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
    BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION, READ_MODEL_REDUCER_VERSION,
    READ_MODEL_SCHEMA_VERSION,
};
pub(crate) use store::{
    fingerprint_manifest_config, ManifestConfigFingerprint,
};
