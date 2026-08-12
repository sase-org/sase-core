//! Deterministic, host-I/O-free chop-overrun classification.
//!
//! The host supplies one chop's cached run history plus its lumberjack's
//! effective interval. This module validates and normalizes that input,
//! then derives one portable versioned overrun verdict for every frontend.

mod classify;
mod wire;

pub use classify::classify_chop_overrun;
pub use wire::*;

#[cfg(test)]
mod tests;
