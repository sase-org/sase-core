use serde::{Deserialize, Serialize};

pub const PROMPT_STASH_WIRE_SCHEMA_VERSION: u32 = 1;

/// Zero-based editor position for the pane that was active when a stash row
/// was captured.
///
/// `pane_index` is bundle-local: it names the active pane among the row's
/// persisted, non-empty segments, not the original prompt-stack index and not
/// [`PromptStashEntryWire::pane_index`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashCursorWire {
    #[serde(default)]
    pub pane_index: u32,
    #[serde(default)]
    pub row: u32,
    #[serde(default)]
    pub column: u32,
}

/// One stashed prompt draft.
///
/// A stash is a JSONL pile of these entries. Each entry is a canonical
/// single-row bundle: one pane stored as `text`, or several panes joined with
/// `\n---\n`. `pane_index` is ordering metadata (the original stack index of
/// the first captured pane). Optional `cursor` is independent of that field
/// and restores the active pane plus a zero-based `(row, column)` in the
/// stored segment text.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashEntryWire {
    pub id: String,
    pub created_at: String,
    pub text: String,
    #[serde(default)]
    pub frontmatter: String,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub pane_index: u32,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PromptStashCursorWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashStoreStatsWire {
    pub total_lines: u64,
    pub blank_lines: u64,
    pub invalid_json_lines: u64,
    pub invalid_record_lines: u64,
    pub loaded_rows: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashSnapshotWire {
    pub schema_version: u32,
    pub entries: Vec<PromptStashEntryWire>,
    pub stats: PromptStashStoreStatsWire,
}

/// Result of [`pop_prompt_stash`](super::store::pop_prompt_stash): the entries
/// that were removed plus a fresh snapshot of what remains.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashPopOutcomeWire {
    pub schema_version: u32,
    pub removed: Vec<PromptStashEntryWire>,
    pub snapshot: PromptStashSnapshotWire,
}

/// Wire schema version for the trash lifecycle endpoints.
///
/// This is versioned separately from [`PROMPT_STASH_WIRE_SCHEMA_VERSION`] so
/// the existing v1 bindings keep their shape while the lifecycle read and
/// mutation results evolve independently.
pub const PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION: u32 = 1;

/// One trashed stash row: the complete entry plus the UTC deletion time that
/// moved it from Stash to Trash.
///
/// The deletion time is caller-supplied (RFC 3339 UTC, e.g.
/// `2026-09-26T14:00:00+00:00`) so batches share one timestamp and eviction
/// order stays deterministic. Re-trashing a restored row assigns a new
/// deletion time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashTrashRecordWire {
    pub trashed_at: String,
    pub entry: PromptStashEntryWire,
}

/// Authoritative view of both stash collections.
///
/// `active` preserves on-disk order. `trash` is newest-deleted-first
/// (descending `trashed_at`, then insertion order, then id). `stats`
/// describes the whole file: `loaded_rows` counts active plus trash rows,
/// while `entries`-style v1 snapshots expose only the active rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashLifecycleSnapshotWire {
    pub schema_version: u32,
    pub active: Vec<PromptStashEntryWire>,
    pub trash: Vec<PromptStashTrashRecordWire>,
    pub stats: PromptStashStoreStatsWire,
}

/// Result of a trash lifecycle mutation.
///
/// `changed` holds ids that moved collections and remain there (trashed,
/// restored, or purged rows, in caller input order). `evicted` holds ids
/// permanently deleted by trash-limit enforcement in the same transaction,
/// oldest first. The two lists are disjoint: a row trashed straight past the
/// limit (for example under limit zero) appears only in `evicted`. `snapshot`
/// is the authoritative post-commit state; repaint from it, never from an
/// optimistic local deletion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptStashLifecycleOutcomeWire {
    pub schema_version: u32,
    pub changed: Vec<String>,
    pub evicted: Vec<String>,
    pub snapshot: PromptStashLifecycleSnapshotWire,
}
