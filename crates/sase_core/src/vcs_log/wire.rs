//! Wire records mirroring `sase/src/sase/core/vcs_log_wire.py`.
//!
//! These records model a provider-agnostic commit history view: the
//! neutral [`VcsCommitWire`] returned by the abstract `vcs_log` provider
//! hook, and the [`AggregatedCommitWire`] produced by interleaving
//! commits from several repos into one chronological timeline.
//!
//! JSON shape rules match the rest of the crate:
//!
//! - All field names are lowercase `snake_case` (serde default).
//! - `timestamp` is the epoch author time in seconds (`git log %at`) so
//!   ordering is timezone-independent; all wall-clock formatting is the
//!   host's concern.
//! - [`AggregatedCommitWire`] flattens a [`VcsCommitWire`] and prefixes it
//!   with the `repo` label, so its JSON object is the flat shape
//!   `{"repo", "full_id", "short_id", "author_name", "author_email",
//!   "timestamp", "subject", "body", "presence"}`.
//! - Schema-version pinning is provided by
//!   [`VCS_LOG_WIRE_SCHEMA_VERSION`].

use serde::{Deserialize, Serialize};

/// Schema version mirrored from
/// `vcs_log_wire.py::VCS_LOG_WIRE_SCHEMA_VERSION`.
pub const VCS_LOG_WIRE_SCHEMA_VERSION: u32 = 2;

/// Where a commit is present relative to the local checkout and the
/// compared remote ref.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum CommitPresenceWire {
    /// Remote state was not available, so only local history was read.
    #[default]
    Unknown,
    /// Commit is in both local `HEAD` and the compared remote ref.
    Synced,
    /// Commit is in the compared remote ref but not the local checkout.
    RemoteOnly,
    /// Commit is in local `HEAD` but not the compared remote ref.
    LocalOnly,
}

/// One commit from a single repository's history.
///
/// Provider-agnostic: this is the return shape of the abstract `vcs_log`
/// hook, so field names avoid git-specific vocabulary (`full_id` /
/// `short_id` rather than "sha"). Mirrors the Python dataclass
/// `VcsCommitWire` field-for-field so JSON output is identical when
/// serialized with order-preserving serializers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsCommitWire {
    /// Full commit identifier (git: full 40-char SHA).
    pub full_id: String,
    /// Abbreviated commit identifier (git: short SHA).
    pub short_id: String,
    /// Commit author display name.
    pub author_name: String,
    /// Commit author email address.
    pub author_email: String,
    /// Epoch author time in seconds. Used for timezone-independent
    /// sorting; the host formats wall-clock time from this.
    pub timestamp: i64,
    /// First line of the commit message.
    pub subject: String,
    /// Remaining commit-message body (may be empty or multi-line).
    pub body: String,
    /// Local/remote presence classification.
    #[serde(default)]
    pub presence: CommitPresenceWire,
}

/// A [`VcsCommitWire`] tagged with the label of the repo it came from.
///
/// The `repo` label is emitted first, then the flattened commit fields,
/// so the JSON object is flat (no nested `commit` key). This is the row
/// shape the host renders in the unified timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatedCommitWire {
    /// Human-facing repo label (primary project name, a linked-repo
    /// name, or the SDD store label).
    pub repo: String,
    /// The flattened commit; serialized inline (no `commit` wrapper key).
    #[serde(flatten)]
    pub commit: VcsCommitWire,
}
