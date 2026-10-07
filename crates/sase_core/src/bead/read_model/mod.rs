//! Versioned SQLite read model over bead event stores.
//!
//! The cache lives under the clone's git dir (see `location`), serves
//! unchanged-store reads without replaying history (see `freshness` for
//! the O(1) token protocol), applies appended events incrementally after
//! the merge frontier (see `tail`), and rebuilds from a full replay
//! whenever a tail precondition fails (see `store`).

mod freshness;
mod location;
mod store;
mod tail;
#[cfg(test)]
mod tests;

pub use freshness::{sweep_store_signatures, READ_MODEL_SWEEP_INTERVAL_SECS};
pub use location::read_model_cache_path_for_store;
pub use store::{
    cached_store_snapshot, cached_store_snapshot_at, read_model_status,
    read_model_status_at, read_model_verify_cache, read_model_verify_cache_at,
    rebuild_read_model_at, BeadReadModelStatusWire, BeadReadModelVerifyWire,
    CachedStoreSnapshot, BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
    BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION, READ_MODEL_REDUCER_VERSION,
    READ_MODEL_SCHEMA_VERSION,
};
