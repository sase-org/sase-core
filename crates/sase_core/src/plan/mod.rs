//! Pure-Rust plan discovery contract for `sase plan search`.
//!
//! Mirrors the bead-search architecture (`bead/`): owned wire records plus a
//! read layer that scans and parses markdown plan artifacts, with no PyO3
//! types so binding/TUI/web frontends can reuse the same logic. This phase
//! covers the wire model and discovery (read layer); search, filtering, and
//! ranking land in a later phase.

pub mod read;
pub mod wire;

pub use read::{read_plans, PLAN_READ_WIRE_SCHEMA_VERSION};
pub use wire::{PlanError, PlanWire, PLAN_WIRE_SCHEMA_VERSION};
