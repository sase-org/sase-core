//! Memory history over git alone: subject identity across renames, shim aliasing, and the fixture corpus.

pub mod causes;
pub mod classify;
pub mod feed;
pub mod subjects;
pub mod wire;

#[cfg(test)]
mod tests;

pub use causes::attribute_instruction_causes;
pub use classify::classify_subjects;
pub use feed::{build_changesets, build_feed, FeedScope};
pub use subjects::{
    derive_subjects, memory_history_pathspecs, subject_path_aliases,
};
pub use wire::{
    memory_history_classifier_version, memory_history_wire_schema_version,
    MemoryHistoryCauseSourceWire, MemoryHistoryCauseWire,
    MemoryHistoryChangesetWire, MemoryHistoryClassWire, MemoryHistoryError,
    MemoryHistoryFeedEntryWire, MemoryHistoryFeedWire,
    MemoryHistoryFooterTagWire, MemoryHistoryInstructionFileWire,
    MemoryHistoryProvenanceWire, MemoryHistoryScopeKindWire,
    MemoryHistoryScopeWire, MemoryHistorySubjectKindWire,
    MemoryHistorySubjectWire, MemoryHistorySummaryWire,
    MemoryHistoryVersionWire, CLASSIFIER_VERSION,
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
