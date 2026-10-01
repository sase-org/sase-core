//! Wire records for memory history: scopes, subjects, versions, causes, and feed.
//!
//! These records model memory semantics on top of the generic
//! [`crate::file_history`] index: subject identity across renames,
//! shim aliasing into instruction subjects, per-version classes with
//! summaries and provenance, instruction causes, and changesets merged
//! into a feed. JSON shape rules match the rest of the crate:
//!
//! - All field names are lowercase `snake_case` (serde default).
//! - Times are epoch seconds (`git log %ct` / `%at`).
//! - OIDs are full lowercase hex SHAs; an absent OID is `None`, never
//!   the all-zero placeholder git prints for created/deleted sides.
//! - Schema-version pinning is provided by
//!   [`MEMORY_HISTORY_WIRE_SCHEMA_VERSION`]; [`CLASSIFIER_VERSION`]
//!   bumps only when class rules change and is part of the cache key.
//!
//! Later phases fill the classification, cause, and feed fields; they
//! do not rename them or bump the schema unless a serialized meaning
//! changes.

use serde::{Deserialize, Serialize};

use crate::file_history::wire::FileChangeKindWire;

/// Schema version for every memory-history snapshot payload.
pub const MEMORY_HISTORY_WIRE_SCHEMA_VERSION: u32 = 1;

/// Classifier version: bump only when class rules change. Part of the
/// snapshot cache key.
pub const CLASSIFIER_VERSION: u32 = 1;

/// Return the memory-history wire schema version.
pub fn memory_history_wire_schema_version() -> u32 {
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION
}

/// Return the memory-history classifier version.
pub fn memory_history_classifier_version() -> u32 {
    CLASSIFIER_VERSION
}

/// Which checkout a scope describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryHistoryScopeKindWire {
    /// A project checkout.
    Project,
    /// A home checkout.
    Home,
}

/// One instruction render directory: its `AGENTS.md`, its shims, and
/// the flags later phases copy onto the instruction subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryInstructionFileWire {
    /// Scope-relative render directory (`.` for the repo root).
    pub dir: String,
    /// Repo-relative path of the rendered instruction file.
    pub agents_path: String,
    /// Repo-relative shim paths folded into this subject.
    #[serde(default)]
    pub shim_paths: Vec<String>,
    /// Whether the render comes from a template.
    #[serde(default)]
    pub template: bool,
    /// Whether the file is renderer-managed.
    #[serde(default)]
    pub managed: bool,
}

/// Caller identity for one memory-history scope. `cache_dir` is
/// caller-supplied snapshot storage and is ignored by identity and
/// classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryScopeWire {
    /// Caller identity, for example `project:fixture`. Embedded in
    /// subject ids.
    pub scope_key: String,
    /// `project` or `home`.
    pub scope_kind: MemoryHistoryScopeKindWire,
    /// Checkout passed to `file_history`.
    #[serde(default)]
    pub repo_root: String,
    /// Directory pathspecs, longest-prefix wins.
    #[serde(default)]
    pub memory_roots: Vec<String>,
    /// Instruction render directories.
    #[serde(default)]
    pub instruction_files: Vec<MemoryHistoryInstructionFileWire>,
    /// Repo-relative or memory-root-relative generated-note paths. A
    /// subject is generated when any of its paths, or that path
    /// stripped of a matching memory root, equals an entry.
    #[serde(default)]
    pub generated_notes: Vec<String>,
    /// Opaque renderer path prefixes. Empty except for the sase repo.
    #[serde(default)]
    pub renderer_prefixes: Vec<String>,
    /// Repo-relative files whose co-change marks an instruction
    /// version as config-driven.
    #[serde(default)]
    pub config_paths: Vec<String>,
    /// Caller-supplied snapshot root.
    #[serde(default)]
    pub cache_dir: String,
}

/// What a subject is, from the lineage's latest path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryHistorySubjectKindWire {
    /// A render directory: lineage paths include an `agents_path`.
    Instructions,
    /// `{web}/{slug}.md` with `{web}.md` as another latest path.
    Strand,
    /// `{slug}.md` with `{slug}/{file}.md` as another latest path.
    Web,
    /// Any other `*.md` or `*.md.tmpl` under a memory root.
    Note,
    /// Anything else under a memory root. Listed only.
    Asset,
}

/// One primary class per committed version. First match wins; see the
/// plan's priority list. `hidden_by_default` is true only for `moved`,
/// `reflow`, and `whitespace`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum MemoryHistoryClassWire {
    /// Placeholder before `classify` runs. `cache-queries` refuses to
    /// persist it.
    #[default]
    Unclassified,
    /// File-history kind is deleted.
    Deleted,
    /// File-history kind is created, including a re-add after a gap.
    Created,
    /// Pure move: unchanged blob, or reflow/whitespace prose stats.
    Moved,
    /// `type_change` is `promoted` (`reference` to `core`).
    Promoted,
    /// `type_change` is `demoted` (`core` to `reference`).
    Demoted,
    /// `frontmatter_only` with no type promotion or demotion.
    Frontmatter,
    /// `reflow_only`.
    Reflow,
    /// `whitespace_only`.
    Whitespace,
    /// Instruction version on a `managed: false` entry.
    HandEdited,
    /// Instruction version co-changed with a memory source.
    Rendered,
    /// Instruction version co-changed with a config or renderer path
    /// and no memory source.
    Config,
    /// Instruction version with no memory, config, or renderer cause.
    RegenOnly,
    /// A generated subject's own version.
    Regenerated,
    /// Every other content edit.
    Authored,
    /// Query-time pseudo-version for worktree content. Never stored.
    Uncommitted,
    /// Query-time pseudo-version for staged content. Never stored.
    Staged,
}

/// Word and frontmatter summary of one version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MemoryHistorySummaryWire {
    /// Deduped hunk breadcrumbs, first-seen order.
    #[serde(default)]
    pub section_paths: Vec<String>,
    /// Added word count.
    #[serde(default)]
    pub words_added: u64,
    /// Removed word count.
    #[serde(default)]
    pub words_removed: u64,
    /// `promoted reference → core` style when `type_change` is set;
    /// otherwise `key: before → after` joined for remaining
    /// frontmatter entries. Absent when frontmatter did not change.
    #[serde(default)]
    pub frontmatter_phrase: Option<String>,
    /// New document's whitespace-delimited word count. Set on
    /// `created` versions only.
    #[serde(default)]
    pub created_words: Option<u64>,
    /// Raw non-negative volume. Log scaling is a renderer concern.
    #[serde(default)]
    pub volume: u64,
}

/// One footer tag as `{key, label}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryFooterTagWire {
    /// Canonical tag key (`SASE_` prefix stripped).
    pub key: String,
    /// Tag value, or the linked label for `[label][id]` values.
    pub label: String,
}

/// Who committed a version and what the commit said.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MemoryHistoryProvenanceWire {
    /// Footer tag `AGENT`, if present.
    #[serde(default)]
    pub agent: Option<String>,
    /// Footer tag `BEAD`, if present.
    #[serde(default)]
    pub bead: Option<String>,
    /// Every footer tag as `{key, label}`.
    #[serde(default)]
    pub footer_tags: Vec<MemoryHistoryFooterTagWire>,
    /// First line of the commit message.
    #[serde(default)]
    pub subject: String,
    /// Leading `[A-Za-z]+` before `:` or `(`, lowercased. Absent
    /// otherwise.
    #[serde(default)]
    pub conventional_type: Option<String>,
    /// Labels from `classify_commit_types` on
    /// `subject + "\n\n" + body`, with `is_merge` pasted when there is
    /// more than one parent.
    #[serde(default)]
    pub commit_types: Vec<String>,
}

/// Why an instruction version rendered. Instruction versions only;
/// empty for every other kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MemoryHistoryCauseWire {
    /// Co-changed non-generated memory versions.
    #[serde(default)]
    pub sources: Vec<MemoryHistoryCauseSourceWire>,
    /// Matched `config_paths` entries.
    #[serde(default)]
    pub config_paths: Vec<String>,
    /// Matched renderer paths.
    #[serde(default)]
    pub renderer_paths: Vec<String>,
    /// True when sources, config paths, and renderer paths are all
    /// empty.
    #[serde(default)]
    pub regen_only: bool,
}

/// One co-changed memory version behind an instruction render.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryCauseSourceWire {
    /// Subject id of the co-changed memory version.
    pub subject_id: String,
    /// Its ordinal within that subject.
    pub ordinal: u64,
}

/// One committed state of a subject. Ordinals are 1-based with 1 as
/// the oldest across canonical and diverged rows; rows are stored
/// newest-first, matching `file_history`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryVersionWire {
    /// Position within the subject, 1-based with 1 as the oldest.
    pub ordinal: u64,
    /// Full commit SHA that introduced this version.
    pub commit: String,
    /// Full parent SHAs in git order (first is the first parent).
    #[serde(default)]
    pub parents: Vec<String>,
    /// Committer time, epoch seconds (`git log %ct`).
    pub committer_time: i64,
    /// Author time, epoch seconds (`git log %at`).
    pub author_time: i64,
    /// Commit author display name.
    #[serde(default)]
    pub author_name: String,
    /// Commit author email address.
    #[serde(default)]
    pub author_email: String,
    /// Repo-relative path at this commit.
    pub path: String,
    /// Primary content OID for this version. For deleted versions this
    /// names the removed blob so tombstone content stays readable;
    /// `None` only when git reported no OID at all.
    #[serde(default)]
    pub blob_oid: Option<String>,
    /// Content OID at the first parent (`None` for creations).
    #[serde(default)]
    pub prev_blob_oid: Option<String>,
    /// How this version changed the path.
    pub kind: FileChangeKindWire,
    /// Rename/copy similarity score (0-100) for moved versions.
    #[serde(default)]
    pub similarity: Option<u8>,
    /// True when this version resumes the subject after a deletion
    /// gap (delete followed by a re-add at the same path).
    #[serde(default)]
    pub gap_before: bool,
    /// True when this row is a diverged shim version rather than an
    /// `AGENTS.md` version.
    #[serde(default)]
    pub diverged: bool,
    /// The shim path when diverged; otherwise the path.
    #[serde(default)]
    pub source_path: String,
    /// Shim paths whose blob at this commit equals this version's
    /// blob. A matching shim never creates its own row.
    #[serde(default)]
    pub aliased_paths: Vec<String>,
    /// Primary class. `unclassified` until `classify` runs.
    #[serde(default)]
    pub class: MemoryHistoryClassWire,
    /// True for `moved`, `reflow`, and `whitespace`.
    #[serde(default)]
    pub hidden_by_default: bool,
    /// Word and frontmatter summary.
    #[serde(default)]
    pub summary: MemoryHistorySummaryWire,
    /// Commit provenance.
    #[serde(default)]
    pub provenance: MemoryHistoryProvenanceWire,
    /// True for boilerplate commits (`chore: run sase init` prefix or
    /// exactly `chore: initialize sase memory`).
    #[serde(default)]
    pub boilerplate: bool,
    /// Render cause. Instruction versions only.
    #[serde(default)]
    pub cause: MemoryHistoryCauseWire,
}

/// One memory subject: a note, web, strand, instructions render, or
/// asset, with every version of its identity across renames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistorySubjectWire {
    /// Subject id, embedding the scope key.
    pub id: String,
    /// What the subject is.
    pub kind: MemoryHistorySubjectKindWire,
    /// Filename for a note or asset, slug for a web or strand,
    /// `AGENTS.md` for instructions.
    #[serde(default)]
    pub display_name: String,
    /// True when the subject is a generated note.
    #[serde(default)]
    pub generated: bool,
    /// Copied from the matching instruction entry; false otherwise.
    #[serde(default)]
    pub managed: bool,
    /// Copied from the matching instruction entry; false otherwise.
    #[serde(default)]
    pub template: bool,
    /// Number of diverged shim rows in `versions`.
    #[serde(default)]
    pub diverged_count: u64,
    /// Every repo-relative path this subject ever held, including
    /// shim paths, in chronological order with no duplicates.
    #[serde(default)]
    pub paths: Vec<String>,
    /// Versions newest-first. Never re-sorted by wall-clock time,
    /// except that diverged shim rows share their commit's place with
    /// equal committer times tie-broken by path.
    #[serde(default)]
    pub versions: Vec<MemoryHistoryVersionWire>,
}

/// One version inside a changeset: where it points plus what it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryFeedEntryWire {
    /// Subject id of the version.
    pub subject_id: String,
    /// Its ordinal within that subject.
    pub ordinal: u64,
    /// Its class.
    pub class: MemoryHistoryClassWire,
    /// Its summary.
    #[serde(default)]
    pub summary: MemoryHistorySummaryWire,
    /// Repo-relative path at that commit.
    #[serde(default)]
    pub path: String,
}

/// One commit in one scope: its entries split into authored work and
/// consequences. A version is a consequence when its subject is
/// generated, or when it is an instruction version whose class is
/// `rendered`, `config`, or `regen_only`; role decides folding, not
/// class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryChangesetWire {
    /// Owning scope key.
    pub scope_key: String,
    /// Full commit SHA.
    pub commit: String,
    /// Committer time, epoch seconds.
    #[serde(default)]
    pub committer_time: i64,
    /// Commit provenance.
    #[serde(default)]
    pub provenance: MemoryHistoryProvenanceWire,
    /// True for boilerplate commits.
    #[serde(default)]
    pub boilerplate: bool,
    /// True when, after the hidden filter, no authored entry remains
    /// and every remaining consequence is `regen_only` or
    /// `regenerated`.
    #[serde(default)]
    pub regen_only: bool,
    /// Authored entries.
    #[serde(default)]
    pub authored: Vec<MemoryHistoryFeedEntryWire>,
    /// Consequence entries.
    #[serde(default)]
    pub consequences: Vec<MemoryHistoryFeedEntryWire>,
}

/// Time-merged changesets across scopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryHistoryFeedWire {
    /// Wire schema version (see [`MEMORY_HISTORY_WIRE_SCHEMA_VERSION`]).
    #[serde(default)]
    pub schema_version: u32,
    /// Changesets newest-first.
    #[serde(default)]
    pub changesets: Vec<MemoryHistoryChangesetWire>,
    /// Changesets dropped by the hidden filter.
    #[serde(default)]
    pub hidden_changeset_count: u64,
}

/// Every failure the memory-history module can report.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemoryHistoryError {
    /// A caller-supplied scope failed validation.
    #[error("invalid memory-history scope: {0}")]
    InvalidScope(String),
    /// A caller-supplied path or pathspec failed token validation.
    #[error("invalid memory-history path: {0}")]
    InvalidPath(String),
    /// A git subprocess failed where success was required.
    #[error("memory-history git command failed: {0}")]
    GitFailed(String),
    /// A wrapped `file_history` failure.
    #[error(transparent)]
    FileHistory(#[from] crate::file_history::FileHistoryError),
}
