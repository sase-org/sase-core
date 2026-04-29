//! Phase 5C: pure-Rust ports of the deterministic Git query parsers.
//!
//! Mirrors the Phase 5B Python wire contract in
//! `sase_100/src/sase/core/git_query_wire.py` and the pure-Python
//! parsers in `sase_100/src/sase/core/git_query_facade.py`. The crate
//! stays free of PyO3 types so later UniFFI/WASM/server work can reuse
//! the same logic; the PyO3 binding lives in `sase_core_py`.
//!
//! Scope:
//!
//! - `parse_git_name_status_z` — parse the NUL-delimited
//!   `git diff --name-status -z` stream into
//!   `Vec<GitNameStatusEntryWire>`.
//! - `parse_git_branch_name` — normalize
//!   `git rev-parse --abbrev-ref HEAD` stdout.
//! - `derive_git_workspace_name` — derive a workspace name from a
//!   remote URL (preferred) or repository root path.
//! - `parse_git_conflicted_files` — split
//!   `git diff --name-only --diff-filter=U` stdout into paths.
//! - `parse_git_local_changes` — normalize `git status --porcelain`
//!   stdout into a clean/dirty signal.
//!
//! See `sase_100/plans/202604/rust_backend_phase5_git_query_ops.md` for
//! the full phase plan and the Phase 5B handoff for the contract this
//! module reproduces.

pub mod parsers;
pub mod wire;

pub use parsers::{
    derive_git_workspace_name, parse_git_branch_name,
    parse_git_conflicted_files, parse_git_local_changes,
    parse_git_name_status_z,
};
pub use wire::{GitNameStatusEntryWire, GIT_QUERY_WIRE_SCHEMA_VERSION};
