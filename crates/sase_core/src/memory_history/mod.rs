//! Memory history over git alone: subject identity across renames, shim aliasing, and the fixture corpus.

pub mod cache;
pub mod causes;
pub mod classify;
pub mod feed;
pub mod query;
pub mod subjects;
pub mod upstream;
pub mod wire;

#[cfg(test)]
mod tests;

pub use cache::{
    clear_snapshot_memo, load_snapshot, persist_snapshot, snapshot_cache_key,
    snapshot_path, sync_scope, SyncOutcome,
};
pub use causes::attribute_instruction_causes;
pub use classify::classify_subjects;
pub use feed::{build_changesets, build_feed, FeedScope};
pub use query::{
    query_compare, query_feed, query_resolve, query_subjects, query_sync,
    query_timeline, query_version, resolve_subject,
};
pub use subjects::{
    derive_subjects, memory_history_pathspecs, subject_path_aliases,
};
pub use upstream::upstream_ahead;
pub use wire::{
    memory_history_classifier_version, memory_history_wire_schema_version,
    MemoryHistoryCauseSourceWire, MemoryHistoryCauseWire,
    MemoryHistoryChangesetWire, MemoryHistoryClassWire,
    MemoryHistoryCompareRequestWire, MemoryHistoryCompareWire,
    MemoryHistoryError, MemoryHistoryFeedEntryWire,
    MemoryHistoryFeedRequestWire, MemoryHistoryFeedWire,
    MemoryHistoryFooterTagWire, MemoryHistoryInstructionFileWire,
    MemoryHistoryProvenanceWire, MemoryHistoryResolveRequestWire,
    MemoryHistoryResolveWire, MemoryHistoryScopeKindWire,
    MemoryHistoryScopeWire, MemoryHistorySnapshotWire,
    MemoryHistorySubjectKindWire, MemoryHistorySubjectWire,
    MemoryHistorySubjectsRequestWire, MemoryHistorySubjectsWire,
    MemoryHistorySummaryWire, MemoryHistorySyncRequestWire,
    MemoryHistorySyncStatusWire, MemoryHistorySyncWire,
    MemoryHistoryTimelineRequestWire, MemoryHistoryTimelineWire,
    MemoryHistoryVersionRequestWire, MemoryHistoryVersionResponseWire,
    MemoryHistoryVersionWire, CLASSIFIER_VERSION,
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
