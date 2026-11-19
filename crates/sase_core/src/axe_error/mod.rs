//! Stable error signatures and signature-grouped error summaries.
//!
//! The scheduler error ring records raw error text whose volatile tokens
//! (paths, SHAs, timestamps, pids, ids, counts) would otherwise split one
//! underlying cause across many distinct rows. This module turns error text
//! into a stable signature and folds entries into per-cause groups, so a
//! loud signature can be counted exactly without evicting rare ones.
//!
//! This module performs no filesystem, clock, or process I/O.

mod group;
mod normalize;
mod wire;

pub use group::group_axe_errors;
pub use normalize::normalize_error_signature;
pub use wire::{
    AxeErrorEntryWire, AxeErrorGroupRequestWire, AxeErrorGroupResponseWire,
    AxeErrorGroupWire, AxeErrorWireError, AXE_ERROR_MAX_SAMPLE_CHARS,
    AXE_ERROR_MAX_SIGNATURE_CHARS, AXE_ERROR_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
