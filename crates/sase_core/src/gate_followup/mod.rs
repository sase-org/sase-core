//! Pure gate-follow-up disposition and recovery decisions.
//!
//! Hosts supply a snapshot of one settled gate plus any observed successor
//! evidence. This module performs no filesystem, process, or lock I/O: it
//! classifies whether a requested coder handoff is intentional-none,
//! suppressed, in progress, confirmed, failed, interrupted, or ambiguous,
//! and whether the host may launch, adopt, wait, or only report.

mod classify;
mod wire;

pub use classify::{decide_gate_followup, gate_followup_attempt_id};
pub use wire::*;

#[cfg(test)]
mod tests;
