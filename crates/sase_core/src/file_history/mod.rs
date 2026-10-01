//! Generic git file-history index: bounded lock-free log walks, rename-aware lineage, incremental sync, blob reads, and path status.

pub mod blobs;
pub mod index;
pub mod lineage;
pub mod parse;
pub mod runner;
pub mod status;
pub mod wire;

#[cfg(test)]
mod tests;

pub use blobs::{
    read_blobs, BlobCache, BlobReadBudget, DEFAULT_BLOB_CACHE_CAPACITY,
};
pub use index::{
    build_index, index_from_json, index_to_json, load_index, persist_index,
    sync_index, PINNED_LOG_FORMAT,
};
pub use lineage::{fold_lineages, fold_new_commits, path_alias_map};
pub use parse::{parse_raw_log, RawChangeEntry, RawChangeKind, RawCommit};
pub use runner::{
    looks_like_full_sha, nonzero_oid, run_git_checked, run_git_unchecked,
    safe_pathspec, safe_revision_token, GitResult,
};
pub use status::{hash_worktree_blob, path_status};
pub use wire::{
    file_history_wire_schema_version, FileHistoryBudgetWire, FileHistoryError,
    FileHistoryHealthWire, FileHistoryIndexWire, FileHistoryScopeKeyWire,
    FileHistorySyncStatus, FileLineageWire, FileVersionWire, PathStateWire,
    PathStatusWire, FILE_HISTORY_WIRE_SCHEMA_VERSION,
};
