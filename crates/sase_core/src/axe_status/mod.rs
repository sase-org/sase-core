//! Deterministic, host-I/O-free AXE runtime status classification.
//!
//! The host supplies already-collected lock, process, marker, runner, and
//! lumberjack observations. This module validates and normalizes that input,
//! then derives one portable versioned snapshot for every frontend.

mod classify;
mod wire;

pub use classify::classify_axe_status;
pub use wire::*;

#[cfg(test)]
mod tests;
