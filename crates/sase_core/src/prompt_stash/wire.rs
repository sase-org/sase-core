use serde::{Deserialize, Serialize};

pub const PROMPT_STASH_WIRE_SCHEMA_VERSION: u32 = 1;

/// One stashed prompt draft (a single prompt-bar pane).
///
/// A stash is a flat pile of these entries persisted as JSONL. Each entry is
/// individually restorable; "stash all visible prompts" simply appends one
/// entry per non-empty pane, preserving `pane_index` order.
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
