//! Bounded backoff schedule parsing and validation.

use super::wire::DEFAULT_GIT_LOCK_RETRY_DELAYS;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GitLockError {
    #[error("git lock retry delays must be finite and non-negative")]
    InvalidDelays,
    #[error("git lock requires schema_version {expected}, got {actual}")]
    Schema { expected: u32, actual: u32 },
}

/// Return the built-in six-delay backoff schedule.
pub fn default_git_lock_retry_delays() -> Vec<f64> {
    DEFAULT_GIT_LOCK_RETRY_DELAYS.to_vec()
}

/// Parse an environment override. Invalid, empty, negative, or non-finite
/// values fall back to the built-in schedule.
pub fn parse_git_lock_retry_delays(raw: Option<&str>) -> Vec<f64> {
    match raw {
        None => default_git_lock_retry_delays(),
        Some(text) => match parse_delay_list(text) {
            Ok(delays) if !delays.is_empty() => delays,
            _ => default_git_lock_retry_delays(),
        },
    }
}

/// Validate an explicit caller-supplied schedule.
///
/// An empty list is valid and means "no backoff retries". Each delay must be
/// finite and non-negative.
pub fn validate_git_lock_retry_delays(
    delays: &[f64],
) -> Result<Vec<f64>, GitLockError> {
    if delays
        .iter()
        .any(|delay| !delay.is_finite() || *delay < 0.0)
    {
        return Err(GitLockError::InvalidDelays);
    }
    Ok(delays.to_vec())
}

fn parse_delay_list(raw: &str) -> Result<Vec<f64>, GitLockError> {
    let mut delays = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            return Err(GitLockError::InvalidDelays);
        }
        let value: f64 =
            trimmed.parse().map_err(|_| GitLockError::InvalidDelays)?;
        delays.push(value);
    }
    validate_git_lock_retry_delays(&delays)
}

/// Convert a validated delay in seconds to nanoseconds.
pub fn delay_seconds_to_ns(delay: f64) -> Option<i64> {
    if !delay.is_finite() || delay < 0.0 {
        return None;
    }
    let ns = delay * 1_000_000_000.0;
    if !ns.is_finite() || ns > i64::MAX as f64 {
        return None;
    }
    Some(ns.round() as i64)
}
