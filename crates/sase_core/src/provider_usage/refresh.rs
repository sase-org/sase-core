//! Refresh admission policy: due/backoff/cooldown decisions.
//!
//! Persistence and reservation CAS stay in [`super::store`]. This module is
//! the pure policy those store operations consult so CLI, ACE, AXE, and
//! limit-event callers share one rule.

use serde::{Deserialize, Serialize};

use super::{
    UsageReasonCode, DEFAULT_USAGE_CADENCE_SECONDS, MIN_USAGE_CADENCE_SECONDS,
    PROVIDER_USAGE_STORE_SCHEMA_VERSION,
};

/// Cap for cadence-based exponential backoff after probe failures.
pub const MAX_USAGE_REFRESH_BACKOFF_SECONDS: f64 = 1_800.0;
/// Short abuse-prevention floor for explicit refresh.
pub const USAGE_REFRESH_EXPLICIT_COOLDOWN_SECONDS: f64 = 5.0;
/// Explicit cooldown under the opt-in `adaptive` attempt policy.
pub const ADAPTIVE_EXPLICIT_COOLDOWN_SECONDS: f64 = 60.0;
/// Rate-limit `Retry-After` clamp window.
pub const RATE_LIMIT_RETRY_AFTER_MIN_SECONDS: f64 = 900.0;
pub const RATE_LIMIT_RETRY_AFTER_MAX_SECONDS: f64 = 21_600.0;
/// Rate-limit escalation without `Retry-After`: `900 * 2^(k-1)`, capped.
pub const RATE_LIMIT_ESCALATION_BASE_SECONDS: f64 = 900.0;
pub const RATE_LIMIT_ESCALATION_CAP_SECONDS: f64 = 7_200.0;
/// Parked-provider backoff (not installed / unsupported CLI).
pub const PARKED_PROVIDER_BACKOFF_SECONDS: f64 = 21_600.0;
/// Vendor-drift fixed backoff.
pub const VENDOR_DRIFT_BACKOFF_SECONDS: f64 = 3_600.0;

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Persisted per-provider refresh schedule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshScheduleWire {
    pub version: u32,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub last_started_at: Option<f64>,
    pub last_finished_at: Option<f64>,
    pub last_success_at: Option<f64>,
    #[serde(default)]
    pub first_failure_at: Option<f64>,
    pub consecutive_failures: u32,
    pub backoff_until: Option<f64>,
    pub retry_after_until: Option<f64>,
    pub cooldown_until: Option<f64>,
    pub due_at: Option<f64>,
    pub due_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub consecutive_rate_limits: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parked_fingerprint: Option<String>,
}

/// Request to evaluate whether a provider is due.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshDueRequestWire {
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    #[serde(default = "default_cadence")]
    pub cadence_seconds: f64,
    #[serde(default)]
    pub explicit: bool,
}

fn default_cadence() -> f64 {
    DEFAULT_USAGE_CADENCE_SECONDS
}

/// Result of a due evaluation without reserving work.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshDueOutcomeWire {
    pub version: u32,
    pub due: bool,
    pub reason: String,
    pub due_at: Option<f64>,
}

/// Request to reserve or join refresh work under admission policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshAdmitRequestWire {
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub operation_id: String,
    pub ttl_seconds: f64,
    #[serde(default = "default_cadence")]
    pub cadence_seconds: f64,
    #[serde(default)]
    pub explicit: bool,
}

/// Admission status for one provider/context generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageRefreshAdmissionStatus {
    Reserved,
    Joined,
    Deferred,
}

/// Result of admitting refresh work.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshAdmitOutcomeWire {
    pub version: u32,
    pub status: ProviderUsageRefreshAdmissionStatus,
    pub reason: Option<String>,
    pub due_at: Option<f64>,
    pub reservation: Option<super::store::ProviderUsageRefreshReservationWire>,
}

/// Request to record a finished refresh attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshAttemptWire {
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub outcome: String,
    pub retry_after_seconds: Option<f64>,
    #[serde(default = "default_cadence")]
    pub cadence_seconds: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<UsageReasonCode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_interval_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub adaptive: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Reason-aware failure policy applied to one finished refresh attempt.
///
/// Pure so the class table stays table-tested. `consecutive_failures` is
/// the already-incremented failure streak; `prev_rate_limits` is the
/// stored `consecutive_rate_limits` before this attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshFailurePolicy {
    pub backoff_until: Option<f64>,
    pub retry_after_until: Option<f64>,
    pub consecutive_rate_limits: u32,
    pub parked_fingerprint: Option<String>,
    pub last_failure_reason: Option<String>,
}

/// Classify an attempt outcome plus reason into the adaptive policy class.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn refresh_failure_policy(
    now: f64,
    cadence_seconds: f64,
    min_interval_seconds: Option<f64>,
    outcome: &str,
    reason: Option<UsageReasonCode>,
    retry_after_seconds: Option<f64>,
    cli_fingerprint: Option<&str>,
    prev_rate_limits: u32,
    consecutive_failures: u32,
) -> RefreshFailurePolicy {
    let last_failure_reason = match reason {
        Some(code) => Some(code.as_str().to_string()),
        None => Some(outcome.to_string()),
    };
    match reason {
        Some(UsageReasonCode::RateLimited) => {
            let next_rate = prev_rate_limits.saturating_add(1);
            let retry_after_until = match retry_after_seconds {
                Some(retry_after) if retry_after > 0.0 => Some(
                    now + retry_after.clamp(
                        RATE_LIMIT_RETRY_AFTER_MIN_SECONDS,
                        RATE_LIMIT_RETRY_AFTER_MAX_SECONDS,
                    ),
                ),
                _ => {
                    let exponent =
                        i32::try_from(next_rate.saturating_sub(1).min(16))
                            .unwrap_or(16);
                    let delay = (RATE_LIMIT_ESCALATION_BASE_SECONDS
                        * 2_f64.powi(exponent))
                    .min(RATE_LIMIT_ESCALATION_CAP_SECONDS);
                    Some(now + delay)
                }
            };
            RefreshFailurePolicy {
                backoff_until: None,
                retry_after_until,
                consecutive_rate_limits: next_rate,
                parked_fingerprint: None,
                last_failure_reason,
            }
        }
        Some(
            UsageReasonCode::NotInstalled
            | UsageReasonCode::UnsupportedCliVersion,
        ) => RefreshFailurePolicy {
            backoff_until: Some(now + PARKED_PROVIDER_BACKOFF_SECONDS),
            retry_after_until: None,
            consecutive_rate_limits: 0,
            parked_fingerprint: cli_fingerprint.map(str::to_string),
            last_failure_reason,
        },
        Some(UsageReasonCode::VendorDrift) => RefreshFailurePolicy {
            backoff_until: Some(now + VENDOR_DRIFT_BACKOFF_SECONDS),
            retry_after_until: None,
            consecutive_rate_limits: 0,
            parked_fingerprint: None,
            last_failure_reason,
        },
        Some(UsageReasonCode::LoggedOut | UsageReasonCode::ApiMode) => {
            let backoff =
                refresh_backoff_seconds(consecutive_failures, cadence_seconds);
            RefreshFailurePolicy {
                backoff_until: Some(now + backoff),
                retry_after_until: None,
                consecutive_rate_limits: 0,
                parked_fingerprint: None,
                last_failure_reason,
            }
        }
        _ if matches!(
            outcome,
            "unauthenticated" | "logged_out" | "api_mode"
        ) =>
        {
            let backoff =
                refresh_backoff_seconds(consecutive_failures, cadence_seconds);
            RefreshFailurePolicy {
                backoff_until: Some(now + backoff),
                retry_after_until: None,
                consecutive_rate_limits: 0,
                parked_fingerprint: None,
                last_failure_reason,
            }
        }
        _ => {
            let base = match min_interval_seconds {
                Some(floor) => cadence_seconds.max(floor),
                None => cadence_seconds,
            };
            let exponent =
                i32::try_from(consecutive_failures.saturating_sub(1).min(16))
                    .unwrap_or(16);
            let backoff = (base * 2_f64.powi(exponent))
                .min(MAX_USAGE_REFRESH_BACKOFF_SECONDS);
            RefreshFailurePolicy {
                backoff_until: Some(now + backoff),
                retry_after_until: None,
                consecutive_rate_limits: 0,
                parked_fingerprint: None,
                last_failure_reason,
            }
        }
    }
}

/// Request to mark a provider due, optionally at a future time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshMarkDueRequestWire {
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub reason: String,
    pub due_at: Option<f64>,
}

/// Outcome of marking a provider due.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageRefreshMarkDueOutcomeWire {
    pub version: u32,
    pub marked: bool,
    pub due_at: f64,
    pub reason: String,
}

/// Pure due/backoff decision used by the store admission path.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshDueDecision {
    pub due: bool,
    pub reason: &'static str,
    pub next_at: Option<f64>,
}

/// Cadence-based exponential backoff, capped at 30 minutes.
#[must_use]
pub fn refresh_backoff_seconds(
    consecutive_failures: u32,
    cadence_seconds: f64,
) -> f64 {
    if consecutive_failures == 0 {
        return 0.0;
    }
    let exponent =
        i32::try_from(consecutive_failures.saturating_sub(1).min(16))
            .unwrap_or(16);
    (cadence_seconds * 2_f64.powi(exponent))
        .min(MAX_USAGE_REFRESH_BACKOFF_SECONDS)
}

/// Validate a cadence used by refresh admission.
pub fn validate_refresh_cadence(cadence_seconds: f64) -> Result<f64, String> {
    if !cadence_seconds.is_finite()
        || cadence_seconds < MIN_USAGE_CADENCE_SECONDS
    {
        return Err(format!(
            "cadence_seconds must be finite and at least {MIN_USAGE_CADENCE_SECONDS}"
        ));
    }
    Ok(cadence_seconds)
}

/// Whether *outcome* resets backoff (honest terminal collection states).
#[must_use]
pub fn refresh_attempt_succeeded(outcome: &str) -> bool {
    matches!(outcome, "ok" | "not_applicable" | "unsupported")
}

/// Build an empty schedule row for a provider context.
#[must_use]
pub fn empty_refresh_schedule(
    provider: &str,
    context_id: &str,
    account_generation: u64,
) -> ProviderUsageRefreshScheduleWire {
    ProviderUsageRefreshScheduleWire {
        version: PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        provider: provider.to_string(),
        context_id: context_id.to_string(),
        account_generation,
        last_started_at: None,
        last_finished_at: None,
        last_success_at: None,
        first_failure_at: None,
        consecutive_failures: 0,
        backoff_until: None,
        retry_after_until: None,
        cooldown_until: None,
        due_at: None,
        due_reason: None,
        last_failure_reason: None,
        consecutive_rate_limits: 0,
        parked_fingerprint: None,
    }
}

/// Decide whether automatic or explicit refresh may run.
#[must_use]
pub fn evaluate_refresh_due(
    now: f64,
    cadence_seconds: f64,
    explicit: bool,
    schedule: Option<&ProviderUsageRefreshScheduleWire>,
    last_full_observation_at: Option<f64>,
    reset_passed: bool,
) -> RefreshDueDecision {
    let retry_after = schedule.and_then(|row| row.retry_after_until);
    if retry_after.is_some_and(|until| now < until) {
        return RefreshDueDecision {
            due: false,
            reason: "retry_after",
            next_at: retry_after,
        };
    }
    let cooldown = schedule.and_then(|row| row.cooldown_until);
    if explicit && cooldown.is_some_and(|until| now < until) {
        return RefreshDueDecision {
            due: false,
            reason: "cooldown",
            next_at: cooldown,
        };
    }
    if explicit {
        return RefreshDueDecision {
            due: true,
            reason: "explicit",
            next_at: None,
        };
    }
    let backoff = schedule.and_then(|row| row.backoff_until);
    if backoff.is_some_and(|until| now < until) {
        return RefreshDueDecision {
            due: false,
            reason: "backoff",
            next_at: backoff,
        };
    }
    let marked = schedule.and_then(|row| row.due_at);
    if marked.is_some_and(|due_at| now >= due_at) {
        return RefreshDueDecision {
            due: true,
            reason: "marked_due",
            next_at: None,
        };
    }
    if reset_passed {
        return RefreshDueDecision {
            due: true,
            reason: "reset_passed",
            next_at: None,
        };
    }
    match last_full_observation_at {
        None => RefreshDueDecision {
            due: true,
            reason: "never_observed",
            next_at: None,
        },
        Some(observed_at) => {
            let next = observed_at + cadence_seconds;
            if now >= next {
                RefreshDueDecision {
                    due: true,
                    reason: "cadence",
                    next_at: None,
                }
            } else if let Some(due_at) = marked {
                if due_at <= next {
                    RefreshDueDecision {
                        due: false,
                        reason: "scheduled",
                        next_at: Some(due_at),
                    }
                } else {
                    RefreshDueDecision {
                        due: false,
                        reason: "fresh",
                        next_at: Some(next),
                    }
                }
            } else {
                RefreshDueDecision {
                    due: false,
                    reason: "fresh",
                    next_at: Some(next),
                }
            }
        }
    }
}
