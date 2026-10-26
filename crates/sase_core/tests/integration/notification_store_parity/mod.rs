//! Notification store parity tests, split by behavior area.
//!
//! This module is part of the consolidated `integration` test target (see
//! `tests/integration/main.rs`), so it runs under
//! `cargo test --test integration`. The shared `tests/fixtures/` directory
//! is untouched.

mod ack_index;
mod agent_dismissal;
mod concurrency;
mod mute_snooze_expiry;
mod plus_one_upsert;
mod reconcile_upsert;
mod state_updates;
mod store_io;
mod support;
