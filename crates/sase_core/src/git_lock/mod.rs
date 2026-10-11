//! Deterministic Git index-lock recovery policy.
//!
//! The host collects filesystem and process facts, runs Git, waits, and
//! unlinks. This module owns failure classification, identity transitions,
//! owner association, and stop/retry/remove decisions.

mod classify;
mod decision;
mod delays;
mod owner;
mod path;
pub mod wire;

#[cfg(test)]
mod tests;

pub use classify::classify_git_lock_failure;
pub use decision::{decide_eager_git_lock_cleanup, decide_git_lock_recovery};
pub use delays::{
    default_git_lock_retry_delays, parse_git_lock_retry_delays,
    validate_git_lock_retry_delays, GitLockError,
};
pub use owner::classify_git_lock_owner;
pub use path::match_git_lock_path;
pub use wire::{
    GitLockEagerCleanupRequestWire, GitLockFailureClassWire,
    GitLockFailureObservationWire, GitLockOwnerFactsWire,
    GitLockOwnerObservationWire, GitLockPathMatchRequestWire,
    GitLockPathMatchWire, GitLockProcessFactWire, GitLockRecoveryDecisionWire,
    GitLockRecoveryRequestWire, GitLockSnapshotWire,
    DEFAULT_GIT_LOCK_RECOVERY_WAIT_CAP_NS, DEFAULT_GIT_LOCK_RETRY_DELAYS,
    GIT_LOCK_ACTION_REMOVE_THEN_RETRY, GIT_LOCK_ACTION_RETRY,
    GIT_LOCK_ACTION_RETRY_WITHOUT_REMOVAL, GIT_LOCK_ACTION_STOP,
    GIT_LOCK_ACTION_WAIT, GIT_LOCK_FAILURE_INDEX_LOCK,
    GIT_LOCK_FAILURE_NOT_LOCK, GIT_LOCK_FAILURE_OTHER_LOCK,
    GIT_LOCK_FAILURE_SUCCESS, GIT_LOCK_FILE_DIRECTORY, GIT_LOCK_FILE_MISSING,
    GIT_LOCK_FILE_OTHER, GIT_LOCK_FILE_REGULAR, GIT_LOCK_FILE_SYMLINK,
    GIT_LOCK_OWNER_ABSENT, GIT_LOCK_OWNER_ACTIVE, GIT_LOCK_OWNER_UNKNOWN,
    GIT_LOCK_PROBE_COMPLETE, GIT_LOCK_PROBE_INCOMPLETE,
    GIT_LOCK_WIRE_SCHEMA_VERSION, STALE_GIT_INDEX_LOCK_MIN_AGE_NS,
};

/// Return the Git-lock recovery wire schema version.
pub fn git_lock_wire_schema_version() -> u32 {
    GIT_LOCK_WIRE_SCHEMA_VERSION
}
