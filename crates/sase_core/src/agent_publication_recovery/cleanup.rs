//! Shared bound for best-effort publication cleanup after a drain timeout.
//!
//! The cleanup cap is a policy default, not a drain-timeout increase: exception
//! cleanup around both regenerable path families must finish within this
//! budget, with no fresh allowance per git command or path family. The drain
//! alarm fires this many seconds early so the reserve still fits inside the
//! overall timeout.

/// Maximum seconds for complete best-effort cleanup after a drain timeout.
///
/// Sized from athena's `agent_publication_drains.jsonl` on 2026-11-19:
/// payload_clean/cleanup p50 ≈ 1.5 s, p95 ≈ 2.2 s, max ≈ 11.8 s (n=1055).
/// 15 s covers the observed max with headroom so exception cleanup can
/// finish inside the 120 s drain window.
pub const DEFAULT_PUBLICATION_CLEANUP_TIMEOUT_SECONDS: f64 = 15.0;

/// Return the shared best-effort cleanup budget in seconds.
pub fn default_publication_cleanup_timeout() -> f64 {
    DEFAULT_PUBLICATION_CLEANUP_TIMEOUT_SECONDS
}
