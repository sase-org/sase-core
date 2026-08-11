//! Pure classifier for mirroring external pull requests into local Patches.
//!
//! Provider calls, file locking, and ProjectSpec writes stay in Python. This
//! module only owns deterministic reconciliation decisions and URL identity.

pub mod classify;
pub mod url;
pub mod wire;

pub use classify::plan_external_pr_import;
pub use url::{canonical_pull_request_url, CanonicalPullRequestUrl};
pub use wire::{
    ExternalPrImportPlanWire, ExternalPrImportRequestWire, LocalPatchWire,
    RemotePullRequestWire, EXTERNAL_PR_WIRE_SCHEMA_VERSION,
};
