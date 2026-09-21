//! Notification store parity tests, split by behavior area.
//!
//! Cargo discovers `tests/notification_store_parity/main.rs` as the
//! `notification_store_parity` integration test target, so
//! `cargo test --test notification_store_parity` keeps working with a
//! single test binary. The shared `tests/fixtures/` directory is untouched.

mod agent_dismissal;
mod concurrency;
mod mute_snooze_expiry;
mod plus_one_upsert;
mod state_updates;
mod store_io;
mod support;
