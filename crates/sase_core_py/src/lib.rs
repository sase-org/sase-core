//! PyO3 binding placeholder for `sase_core`.
//!
//! Phase 1A only reserves the crate name and the workspace slot. The actual
//! `parse_project_bytes` Python entry point is added in Phase 1D after the
//! Rust parser reaches section parity in Phase 1C.
//!
//! This crate intentionally has no PyO3 dependency yet — pulling pyo3 into
//! the workspace today would require a Python interpreter on every
//! `cargo test --workspace` run, which is exactly what Phase 1A's exit
//! criteria forbid.

pub use sase_core as core;
