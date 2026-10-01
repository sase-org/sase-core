//! Wire records for the generic git file-history index.
//!
//! These records model file lineage without any memory or instruction
//! semantics: versions, rename-aware lineages, index health, build
//! budgets, path status, and the sync key. JSON shape rules match the
//! rest of the crate:
//!
//! - All field names are lowercase `snake_case` (serde default).
//! - Times are epoch seconds (`git log %ct` / `%at`).
//! - OIDs are full lowercase hex SHAs; an absent OID is `None`, never
//!   the all-zero placeholder git prints for created/deleted sides.
//! - Schema-version pinning is provided by
//!   [`FILE_HISTORY_WIRE_SCHEMA_VERSION`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Schema version for every [`FileHistoryIndexWire`] payload.
pub const FILE_HISTORY_WIRE_SCHEMA_VERSION: u32 = 1;

/// Return the file-history wire schema version.
pub fn file_history_wire_schema_version() -> u32 {
    FILE_HISTORY_WIRE_SCHEMA_VERSION
}

/// How one version changed its path's content.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKindWire {
    /// Content modified in place (includes type changes).
    #[default]
    Edited,
    /// Path appeared (added, or re-added after a deletion gap).
    Created,
    /// Path moved from another path (rename or copy pair).
    Moved,
    /// Path removed (tombstone content stays addressable).
    Deleted,
}

/// One committed state of a lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileVersionWire {
    /// Position within the lineage, 1-based with 1 as the oldest.
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
    /// First line of the commit message.
    #[serde(default)]
    pub subject: String,
    /// Remaining commit-message body (may be empty or multi-line).
    #[serde(default)]
    pub body: String,
    /// Repo-relative path at this commit.
    pub path: String,
    /// Primary content OID for this version. For deleted versions this
    /// names the removed blob so tombstone content stays readable;
    /// `None` only when git reported no OID at all.
    #[serde(default)]
    pub blob_oid: Option<String>,
    /// Content OID at the first parent (`None` for creations).
    /// For deleted versions this also names the removed blob.
    #[serde(default)]
    pub prev_blob_oid: Option<String>,
    /// How this version changed the path.
    pub kind: FileChangeKindWire,
    /// Pre-image file mode (`000000` for creations).
    #[serde(default)]
    pub mode_old: String,
    /// Post-image file mode (`000000` for deletions).
    #[serde(default)]
    pub mode_new: String,
    /// Rename/copy similarity score (0-100) for moved versions.
    #[serde(default)]
    pub similarity: Option<u8>,
    /// True when this version resumes the lineage after a deletion
    /// gap (delete followed by a re-add at the same path).
    #[serde(default)]
    pub gap_before: bool,
}

/// One rename-aware file lineage: every version of one logical file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileLineageWire {
    /// Stable id within an index build, assigned oldest-first by
    /// creation order so full rebuilds and incremental folds agree.
    pub id: u64,
    /// Every repo-relative path this lineage ever held, in
    /// chronological order with no duplicates.
    pub paths: Vec<String>,
    /// Repo-relative path at the indexed tip.
    pub current_path: String,
    /// True when the latest version is a deletion.
    pub tombstone: bool,
    /// True when the oldest version is a creation (no shallow or
    /// budget cut removed older history) and the walk was not
    /// truncated.
    pub complete: bool,
    /// Versions newest-first. Never re-sorted by wall-clock time.
    pub versions: Vec<FileVersionWire>,
}

/// Health of one index build: every way history can be incomplete is
/// labelled here, never hidden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FileHistoryHealthWire {
    /// The owning repo is a shallow clone.
    #[serde(default)]
    pub shallow: bool,
    /// Oldest committer time observed in the walk (`None` unless the
    /// repo is shallow and the walk saw at least one commit). This is
    /// the date history is truncated at.
    #[serde(default)]
    pub shallow_boundary_time: Option<i64>,
    /// A time or output budget cut the walk short, or a commit cap
    /// dropped the oldest commits.
    #[serde(default)]
    pub truncated: bool,
    /// Blob OIDs the log named but no object store produced, in first
    /// sighting order. The index build itself never reads blobs, so a
    /// fresh build reports none; blob readers append here.
    #[serde(default)]
    pub missing_objects: Vec<String>,
    /// True when no lineage is incomplete and the walk was neither
    /// shallow-cut nor truncated.
    #[serde(default)]
    pub complete: bool,
}

/// The whole file-history index for one scope walk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileHistoryIndexWire {
    /// Wire schema version (see [`FILE_HISTORY_WIRE_SCHEMA_VERSION`]).
    pub schema_version: u32,
    /// Canonical repo top level (`git rev-parse --show-toplevel`).
    pub repo_root: String,
    /// `git rev-parse --git-common-dir`, resolved against the top
    /// level when git prints it relative. Part of the sync key.
    pub repo_common_dir: String,
    /// HEAD commit the index was built at ("" for an unborn HEAD).
    pub tip: String,
    /// Normalized (sorted, deduped) explicit pathspecs walked.
    #[serde(default)]
    pub pathspecs: Vec<String>,
    /// Lineages in id order.
    #[serde(default)]
    pub lineages: Vec<FileLineageWire>,
    /// Every historical path to its lineage id, for historical-name
    /// and link-at-revision lookups.
    #[serde(default)]
    pub path_aliases: BTreeMap<String, u64>,
    /// Walk health labels.
    pub health: FileHistoryHealthWire,
    /// Commits walked to build this index (range-walk counts add up
    /// across incremental folds).
    #[serde(default)]
    pub commit_count: u64,
}

/// Cache key for [`crate::file_history::sync_index`]: a mismatch means
/// the cached index describes a different scope and triggers a rebuild.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileHistoryScopeKeyWire {
    /// `git rev-parse --git-common-dir` of the owning checkout.
    pub repo_common_dir: String,
    /// Normalized (sorted, deduped) explicit pathspecs.
    #[serde(default)]
    pub pathspecs: Vec<String>,
    /// Wire schema version the cached index was built with.
    pub schema_version: u32,
}

/// Outcome of [`crate::file_history::sync_index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileHistorySyncStatus {
    /// Cached tip already equals HEAD; the index is untouched.
    Fresh,
    /// New commits folded onto the cached tip by tip ancestry.
    Folded,
    /// The cache was unusable (key mismatch, rewrite, or branch
    /// switch) and the index was rebuilt from scratch.
    Rebuilt,
}

/// Budgets for one git walk. A breached budget sets the index
/// `truncated` flag rather than failing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileHistoryBudgetWire {
    /// Per-command timeout in milliseconds.
    pub timeout_ms: u64,
    /// Stdout byte cap per command; excess bytes are dropped.
    pub max_output_bytes: u64,
    /// Maximum commits kept per walk; older commits are dropped.
    pub max_commits: usize,
}

impl Default for FileHistoryBudgetWire {
    fn default() -> Self {
        Self {
            timeout_ms: 10_000,
            max_output_bytes: 32 * 1024 * 1024,
            max_commits: 50_000,
        }
    }
}

/// Whether a repo-relative path is tracked, and its worktree state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathStateWire {
    /// Present in the index.
    Tracked,
    /// Not in the index and not ignored (or absent entirely).
    Untracked,
    /// Matched by the repo's ignore rules.
    Ignored,
    /// The directory is not inside a git work tree.
    NoVcs,
}

/// Worktree, index, and HEAD state for one repo-relative path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathStatusWire {
    /// Tracked / untracked / ignored / no_vcs classification.
    pub state: PathStateWire,
    /// The queried repo-relative path, as given.
    pub path: String,
    /// Staged blob OID (`git ls-files -s`), if any.
    #[serde(default)]
    pub index_oid: Option<String>,
    /// Worktree blob OID hashed in-process, if the file is readable.
    #[serde(default)]
    pub worktree_oid: Option<String>,
    /// HEAD blob OID (`git ls-tree HEAD`), if any.
    #[serde(default)]
    pub head_oid: Option<String>,
}

/// Every failure this module can report.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FileHistoryError {
    /// The directory is not inside a git work tree.
    #[error("not a git repository: {0}")]
    NotARepo(String),
    /// A git subprocess failed to spawn, timed out unrecoverably, or
    /// exited non-zero where success was required.
    #[error("git command failed: {0}")]
    GitFailed(String),
    /// A caller-supplied path or pathspec failed token validation.
    #[error("invalid path: {0}")]
    InvalidPath(String),
    /// The JSON snapshot could not be written, read, or parsed.
    #[error("snapshot error: {0}")]
    Snapshot(String),
}
