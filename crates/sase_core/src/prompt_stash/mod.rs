//! Prompt-stash store and snapshot operations.
pub mod store;
pub mod wire;

pub use store::{
    append_prompt_stash, archive_path_for_prompt_stash, pop_prompt_stash,
    purge_prompt_stash, read_prompt_stash_archive, read_prompt_stash_lifecycle,
    read_prompt_stash_snapshot, reconcile_prompt_stash_trash,
    recover_prompt_stash_archive, restore_prompt_stash, rewrite_prompt_stash,
    set_prompt_stash_pinned, trash_prompt_stash, PromptStashStoreError,
};
pub use wire::{
    PromptStashArchiveReason, PromptStashArchiveRecordWire,
    PromptStashArchiveSnapshotWire, PromptStashCursorWire,
    PromptStashEntryWire, PromptStashLifecycleOutcomeWire,
    PromptStashLifecycleSnapshotWire, PromptStashPopOutcomeWire,
    PromptStashSnapshotWire, PromptStashStoreStatsWire,
    PromptStashTrashRecordWire, PROMPT_STASH_ARCHIVE_LINE_KIND,
    PROMPT_STASH_ARCHIVE_WIRE_SCHEMA_VERSION,
    PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION,
    PROMPT_STASH_WIRE_SCHEMA_VERSION,
};
