//! Wire records mirroring `sase_100/src/sase/core/git_query_wire.py`.
//!
//! The Phase 5C scope is intentionally narrow: only one structured record
//! is needed to model `git diff --name-status -z` rows. The remaining
//! Phase 5 helpers (`parse_git_branch_name`, `derive_git_workspace_name`,
//! `parse_git_conflicted_files`, `parse_git_local_changes`) cross the
//! Python/Rust boundary as primitives (`Option<String>` / `Vec<String>`)
//! and therefore do not need a dedicated wire struct.
//!
//! JSON shape rules match the rest of the crate:
//!
//! - All field names are lowercase `snake_case` (serde default).
//! - `path` carries `"<old>\t<new>"` (a literal tab) for rename/copy
//!   entries so the legacy `list[tuple[str, str]]` Python shape can be
//!   reconstructed without an extra wire field.
//! - Schema-version pinning is provided by
//!   [`GIT_QUERY_WIRE_SCHEMA_VERSION`].

use serde::{Deserialize, Serialize};

/// Schema version mirrored from
/// `git_query_wire.py::GIT_QUERY_WIRE_SCHEMA_VERSION`.
pub const GIT_QUERY_WIRE_SCHEMA_VERSION: u32 = 1;

/// One parsed row from `git diff --name-status -z`.
///
/// Mirrors the Python dataclass `GitNameStatusEntryWire` byte-for-byte.
/// Field declaration order matches the Python side so JSON output is
/// identical when serialized with order-preserving serializers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitNameStatusEntryWire {
    /// Status token from `git diff` (`A`, `M`, `D`, `T`, `U`,
    /// `R<score>`, `C<score>`, ...). Carried verbatim so the facade can
    /// preserve the legacy semantics of `list[tuple[str, str]]`.
    pub status: String,
    /// Path field. For renames and copies (`R`/`C`) this is
    /// `"<old>\t<new>"` (a literal tab) so callers can split the pair
    /// without an extra wire field.
    pub path: String,
}
