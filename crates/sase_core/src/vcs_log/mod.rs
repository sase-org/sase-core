//! Provider-agnostic commit-history domain: parse a pinned `git log`
//! stream into neutral commit records and interleave per-repo lists into
//! one chronological timeline.
//!
//! Mirrors the Python wire contract in
//! `sase/src/sase/core/vcs_log_wire.py` and the pure-Python golden
//! references in `sase/src/sase/core/vcs_log_facade.py`. The crate stays
//! free of PyO3 types so later UniFFI/WASM/server work can reuse the same
//! logic; the PyO3 binding lives in `sase_core_py`.
//!
//! Scope:
//!
//! - [`parse_git_log`] — parse a pinned, separator-delimited
//!   `git log --format=...` stream into `Vec<VcsCommitWire>`.
//! - [`aggregate_commit_log`] — interleave `(repo_label, commits)` pairs
//!   into a single newest-first `Vec<AggregatedCommitWire>`, truncated to
//!   a limit.
//! - [`classify_commit_presence`] — stamp commit records with local/remote
//!   presence from precomputed ahead/behind id sets.
//!
//! This is the read-only history counterpart to the `git_query` parser
//! family: repo resolution and `git` execution stay host-side, exactly as
//! they do for every other provider hook.

pub mod aggregate;
pub mod classify;
pub mod parsers;
pub mod wire;

pub use aggregate::aggregate_commit_log;
pub use classify::classify_commit_presence;
pub use parsers::parse_git_log;
pub use wire::{
    AggregatedCommitWire, CommitPresenceWire, VcsCommitWire,
    VCS_LOG_WIRE_SCHEMA_VERSION,
};
