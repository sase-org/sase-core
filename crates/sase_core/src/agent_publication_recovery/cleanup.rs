//! Shared bound for best-effort publication cleanup after a drain timeout.
//!
//! The cleanup cap is a policy default, not a drain-timeout increase: exception
//! cleanup around both regenerable path families must finish within this
//! budget, with no fresh allowance per git command or path family.

/// Maximum seconds for complete best-effort cleanup after a drain timeout.
pub const DEFAULT_PUBLICATION_CLEANUP_TIMEOUT_SECONDS: f64 = 1.0;

/// Return the shared best-effort cleanup budget in seconds.
pub fn default_publication_cleanup_timeout() -> f64 {
    DEFAULT_PUBLICATION_CLEANUP_TIMEOUT_SECONDS
}
