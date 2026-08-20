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
