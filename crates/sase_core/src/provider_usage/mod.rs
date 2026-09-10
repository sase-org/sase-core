//! Subscription-capacity observation and public read contracts.
//!
//! Collectors normalize vendor payloads onto these types. This module
//! validates observations, classifies freshness and scope, derives remaining
//! percentage, projects the versioned public read model, and persists the
//! machine-local provider-usage cache. Probe transport and UI are owned by
//! later phases.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

mod indicator;
mod refresh;
mod store;

pub use indicator::{
    project_usage_indicator, validate_usage_indicator_config,
    UsageIndicatorConfigValidationWire, UsageIndicatorConfigWire,
    UsageIndicatorDiagnosticWire, UsageIndicatorPeriodKind,
    UsageIndicatorPeriodWire, UsageIndicatorPolicyKind,
    UsageIndicatorPolicySource, UsageIndicatorPolicyWire,
    UsageIndicatorProjectionRequestWire, UsageIndicatorProjectionWire,
    UsageIndicatorProviderConfigWire, UsageIndicatorProviderStatusWire,
    UsageIndicatorResetState, UsageIndicatorScopeKind, UsageIndicatorScopeWire,
    UsageIndicatorWindowEntryWire, PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
};
pub use refresh::{
    empty_refresh_schedule, evaluate_refresh_due, refresh_attempt_succeeded,
    refresh_backoff_seconds, ProviderUsageRefreshAdmissionStatus,
    ProviderUsageRefreshAdmitOutcomeWire, ProviderUsageRefreshAdmitRequestWire,
    ProviderUsageRefreshAttemptWire, ProviderUsageRefreshDueOutcomeWire,
    ProviderUsageRefreshDueRequestWire, ProviderUsageRefreshMarkDueOutcomeWire,
    ProviderUsageRefreshMarkDueRequestWire, ProviderUsageRefreshScheduleWire,
    RefreshDueDecision, MAX_USAGE_REFRESH_BACKOFF_SECONDS,
    USAGE_REFRESH_EXPLICIT_COOLDOWN_SECONDS,
};
pub use store::{
    admit_provider_usage_refresh, evaluate_provider_usage_refresh_due,
    load_provider_usage_store, mark_provider_usage_refresh_due,
    prepare_provider_usage_account_context, provider_usage_state_path,
    record_provider_usage_observation, record_provider_usage_refresh_attempt,
    release_provider_usage_refresh, reserve_provider_usage_refresh,
    ProviderUsageAccountContextWire,
    ProviderUsageRefreshReservationOutcomeWire,
    ProviderUsageRefreshReservationRequestWire,
    ProviderUsageRefreshReservationStatus, ProviderUsageRefreshReservationWire,
    ProviderUsageStoreDiagnosticWire, ProviderUsageStoreError,
    ProviderUsageStoreReadWire, ProviderUsageStoreWriteOutcomeWire,
    ProviderUsageStoreWriteStatus, PROVIDER_USAGE_STATE_FILENAME,
    PROVIDER_USAGE_STORE_SCHEMA_VERSION,
};

pub const PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION: u32 = 1;
pub const PROVIDER_USAGE_PUBLIC_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_USAGE_CADENCE_SECONDS: f64 = 300.0;
pub const MIN_USAGE_CADENCE_SECONDS: f64 = 60.0;
pub const DEFAULT_USAGE_WARN_PERCENT: f64 = 75.0;
pub const DEFAULT_USAGE_CRITICAL_PERCENT: f64 = 90.0;
pub const USAGE_COLLECTOR_FAILING_THRESHOLD: u32 = 3;

const MAX_FUTURE_SKEW_SECONDS: f64 = 60.0;
const MAX_OBSERVATIONS: usize = 64;
const MAX_WINDOWS: usize = 32;
const MAX_MODEL_IDS: usize = 32;
const MAX_PROVIDER_LEN: usize = 64;
const MAX_KEY_LEN: usize = 128;
const MAX_LABEL_LEN: usize = 200;
const MAX_DIAGNOSTIC_LEN: usize = 240;
const MAX_CONTEXT_LEN: usize = 128;
const MAX_SHORT_TEXT_LEN: usize = 64;
const PERIOD_TOLERANCE_SECONDS: f64 = 1.0;

#[derive(Debug, Error)]
pub enum ProviderUsageError {
    #[error("{0}")]
    Validation(String),
}

pub type Result<T> = std::result::Result<T, ProviderUsageError>;

fn validation(message: impl Into<String>) -> ProviderUsageError {
    ProviderUsageError::Validation(message.into())
}

/// How an observation was collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    Probe,
    StreamEvent,
}

/// Stable collection outcome at the provider envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCollectionOutcome {
    Ok,
    NotApplicable,
    Unauthenticated,
    Unsupported,
    Error,
}

impl UsageCollectionOutcome {
    fn is_problem(self) -> bool {
        matches!(self, Self::Error | Self::Unauthenticated)
    }
}

/// Actionable reason distinguishing a non-ok or qualified outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageReasonCode {
    NotInstalled,
    UnsupportedCliVersion,
    Timeout,
    ParseError,
    AccountContextChanged,
    LoggedOut,
    ApiMode,
    MalformedPayload,
    DeadlineExceeded,
    ProbeFailed,
    VendorDrift,
}

/// Whether the observation is a full inventory or a named-window update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCompleteness {
    Complete,
    Partial,
}

/// Native vendor classification; never synthesized from percent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageVendorState {
    Allowed,
    Warning,
    Rejected,
    Unknown,
}

/// Age class relative to the injected clock and cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageFreshness {
    Fresh,
    Stale,
    Unknown,
}

impl UsageFreshness {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Unknown => "unknown",
        }
    }
}

/// Snapshot-level collection health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCollectionHealth {
    Empty,
    Ok,
    Partial,
    Error,
}

/// Collector health derived from probe/refresh failure streaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCollectorHealthState {
    Ok,
    Degraded,
    Failing,
}

/// Public per-provider collector health block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageCollectorHealthWire {
    pub state: UsageCollectorHealthState,
    pub consecutive_failures: u32,
    pub last_success_at: Option<f64>,
    pub failing_since: Option<f64>,
}

/// Attention kind for a provider or limiting window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageAttentionKind {
    None,
    Rejected,
    VeryLow,
    Low,
    CollectionProblem,
}

impl UsageAttentionKind {
    fn rank(self) -> u8 {
        match self {
            Self::Rejected => 4,
            Self::VeryLow => 3,
            Self::CollectionProblem => 2,
            Self::Low => 1,
            Self::None => 0,
        }
    }

    fn is_constraint(self) -> bool {
        matches!(self, Self::Rejected | Self::VeryLow | Self::Low)
    }
}

/// Whether a window applies to a concrete model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageApplicabilityMatch {
    Applies,
    DoesNotApply,
    Unknown,
}

impl UsageApplicabilityMatch {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applies => "applies",
            Self::DoesNotApply => "does_not_apply",
            Self::Unknown => "unknown",
        }
    }
}

/// Structured window applicability. Never inferred from labels or order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UsageApplicabilityWire {
    Account,
    Product {
        product: String,
        #[serde(default)]
        model_ids: Vec<String>,
    },
    Models {
        model_ids: Vec<String>,
    },
    ModelFamily {
        family: String,
        #[serde(default)]
        model_ids: Vec<String>,
    },
    Unknown {
        #[serde(default)]
        vendor_label: Option<String>,
        #[serde(default)]
        vendor_id: Option<String>,
    },
}

/// One allowance window as collected from a provider plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageWindowObservationWire {
    pub key: String,
    pub label: String,
    pub used_percent: f64,
    pub resets_at: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub period_start: Option<f64>,
    pub applicability: UsageApplicabilityWire,
    pub observed_at: f64,
    pub source: UsageSource,
    pub vendor_state: UsageVendorState,
}

/// Collector observation envelope. Distinct from persisted state and the
/// public read model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageObservationWire {
    pub schema_version: u32,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub ordering_token: f64,
    pub received_at: f64,
    pub source: UsageSource,
    pub outcome: UsageCollectionOutcome,
    pub reason_code: Option<UsageReasonCode>,
    pub diagnostic: Option<String>,
    pub completeness: UsageCompleteness,
    #[serde(default)]
    pub authoritative_empty: bool,
    pub account_mode: Option<String>,
    pub plan: Option<String>,
    pub windows: Vec<UsageWindowObservationWire>,
}

/// Public window in the versioned read model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsagePublicWindowWire {
    pub key: String,
    pub label: String,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub exceeded_by_percent: Option<f64>,
    pub freshness: UsageFreshness,
    pub reset_passed: bool,
    pub resets_at: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub period_start: Option<f64>,
    pub age_seconds: f64,
    pub observed_at: f64,
    pub source: UsageSource,
    pub vendor_state: UsageVendorState,
    pub applicability: UsageApplicabilityWire,
}

/// Provider-overview or model-scoped quantitative summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageScopedSummaryWire {
    pub remaining_percent: f64,
    pub used_percent: f64,
    pub limiting_window_keys: Vec<String>,
    pub scope: UsageApplicabilityWire,
    pub completeness: UsageCompleteness,
    pub freshness: UsageFreshness,
}

/// A known rejected or low constraint retained beside a null or partial
/// summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageKnownConstraintWire {
    pub window_key: String,
    pub vendor_state: UsageVendorState,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub freshness: UsageFreshness,
    pub reset_passed: bool,
    pub observed_at: f64,
    pub attention: UsageAttentionKind,
}

/// Highest-rank attention item for a provider or snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageAttentionWire {
    pub kind: UsageAttentionKind,
    pub provider: String,
    pub window_key: Option<String>,
}

/// Public per-provider read model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsagePublicProviderWire {
    pub provider: String,
    pub context_ref: String,
    pub account_generation: u64,
    pub collection_status: UsageCollectionOutcome,
    pub collection_reason: Option<UsageReasonCode>,
    #[serde(default)]
    pub collector_health: Option<UsageCollectorHealthWire>,
    pub completeness: UsageCompleteness,
    pub last_attempt_at: f64,
    pub last_full_observation_at: Option<f64>,
    pub diagnostic: Option<String>,
    pub account_mode: Option<String>,
    pub plan: Option<String>,
    pub windows: Vec<UsagePublicWindowWire>,
    pub summary: Option<UsageScopedSummaryWire>,
    pub known_constraints: Vec<UsageKnownConstraintWire>,
    pub attention: UsageAttentionWire,
}

/// Public `schema_version: 1` usage snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsagePublicSnapshotWire {
    pub schema_version: u32,
    pub generated_at: f64,
    pub collection_health: UsageCollectionHealth,
    pub providers: Vec<UsagePublicProviderWire>,
    pub attention: Option<UsageAttentionWire>,
}

/// Validate cadence: finite and at least 60 seconds.
pub fn validate_usage_cadence(cadence_seconds: f64) -> Result<f64> {
    if !cadence_seconds.is_finite()
        || cadence_seconds < MIN_USAGE_CADENCE_SECONDS
    {
        return Err(validation(format!(
            "refresh cadence must be finite and at least {MIN_USAGE_CADENCE_SECONDS} seconds"
        )));
    }
    Ok(cadence_seconds)
}

/// Validate `0 <= warn_percent < critical_percent <= 100`.
pub fn validate_usage_thresholds(
    warn_percent: f64,
    critical_percent: f64,
) -> Result<(f64, f64)> {
    if !warn_percent.is_finite() || !critical_percent.is_finite() {
        return Err(validation(
            "warn_percent and critical_percent must be finite",
        ));
    }
    if warn_percent < 0.0
        || critical_percent > 100.0
        || warn_percent >= critical_percent
    {
        return Err(validation(
            "thresholds must satisfy 0 <= warn_percent < critical_percent <= 100",
        ));
    }
    Ok((warn_percent, critical_percent))
}

/// Store one measure, `used_percent`; remaining is `max(0, 100 - used)`.
pub fn remaining_percent(used_percent: f64) -> Result<f64> {
    validate_used_percent(used_percent)?;
    Ok(remaining_percent_value(used_percent))
}

fn remaining_percent_value(used_percent: f64) -> f64 {
    (100.0 - used_percent).max(0.0)
}

/// Percentage points above 100 used, or `None` when not exceeded.
pub fn exceeded_by_percent(used_percent: f64) -> Result<Option<f64>> {
    validate_used_percent(used_percent)?;
    Ok(exceeded_by_percent_value(used_percent))
}

fn exceeded_by_percent_value(used_percent: f64) -> Option<f64> {
    if used_percent > 100.0 {
        Some(used_percent - 100.0)
    } else {
        None
    }
}

/// Text remaining label. JSON keeps raw precision; this is display-only.
///
/// Positive sub-percent remainder is `<1% left`. Exact exhaustion is
/// `0% left`. Partial use never rounds to a falsely exact `100% left`.
pub fn format_remaining_text(used_percent: f64) -> Result<String> {
    validate_used_percent(used_percent)?;
    let remaining = remaining_percent_value(used_percent);
    if remaining <= 0.0 {
        return Ok("0% left".to_string());
    }
    if remaining < 1.0 {
        return Ok("<1% left".to_string());
    }
    let mut rounded = remaining.round();
    if used_percent > 0.0 && rounded >= 100.0 {
        rounded = 99.0;
    }
    Ok(format!("{}% left", rounded as i64))
}

/// Classify observation age. Reset expiry is tracked separately.
pub fn classify_freshness(
    observed_at: f64,
    now: f64,
    cadence_seconds: f64,
) -> Result<UsageFreshness> {
    validate_now(now)?;
    validate_usage_cadence(cadence_seconds)?;
    if !observed_at.is_finite() {
        return Err(validation("observed_at must be finite"));
    }
    Ok(freshness_value(observed_at, now, cadence_seconds))
}

fn freshness_value(
    observed_at: f64,
    now: f64,
    cadence_seconds: f64,
) -> UsageFreshness {
    let age = (now - observed_at.min(now)).max(0.0);
    let fresh_until = 2.0 * cadence_seconds;
    let stale_until = 4.0 * cadence_seconds;
    if age <= fresh_until {
        UsageFreshness::Fresh
    } else if age <= stale_until {
        UsageFreshness::Stale
    } else {
        UsageFreshness::Unknown
    }
}

pub fn reset_has_passed(resets_at: Option<f64>, now: f64) -> bool {
    resets_at.is_some_and(|reset| now >= reset)
}

/// Match a window's structured applicability against an optional model.
///
/// `model_id = None` is the provider overview: every window is in scope.
pub fn usage_window_applies(
    applicability: &UsageApplicabilityWire,
    model_id: Option<&str>,
) -> UsageApplicabilityMatch {
    let Some(model_id) = model_id else {
        return UsageApplicabilityMatch::Applies;
    };
    match applicability {
        UsageApplicabilityWire::Account => UsageApplicabilityMatch::Applies,
        UsageApplicabilityWire::Product { model_ids, .. }
        | UsageApplicabilityWire::ModelFamily { model_ids, .. } => {
            if model_ids.is_empty() {
                UsageApplicabilityMatch::Unknown
            } else if model_ids.iter().any(|id| id == model_id) {
                UsageApplicabilityMatch::Applies
            } else {
                UsageApplicabilityMatch::DoesNotApply
            }
        }
        UsageApplicabilityWire::Models { model_ids } => {
            if model_ids.iter().any(|id| id == model_id) {
                UsageApplicabilityMatch::Applies
            } else {
                UsageApplicabilityMatch::DoesNotApply
            }
        }
        UsageApplicabilityWire::Unknown { .. } => {
            UsageApplicabilityMatch::Unknown
        }
    }
}

/// Validate and sanitize a collector observation against `now`.
pub fn validate_usage_observation(
    observation: ProviderUsageObservationWire,
    now: f64,
) -> Result<ProviderUsageObservationWire> {
    validate_now(now)?;
    if observation.schema_version != PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION {
        return Err(validation(format!(
            "unsupported provider-usage observation schema version: {}",
            observation.schema_version
        )));
    }
    let provider =
        validate_ident("provider", &observation.provider, MAX_PROVIDER_LEN)?;
    let context_id =
        validate_ident("context_id", &observation.context_id, MAX_CONTEXT_LEN)?;
    validate_timestamp("ordering_token", observation.ordering_token, now)?;
    validate_timestamp("received_at", observation.received_at, now)?;
    if observation.received_at + 1e-9 < observation.ordering_token {
        return Err(validation("received_at must not precede ordering_token"));
    }
    let diagnostic = match observation.diagnostic.as_deref() {
        Some(raw) => sanitize_diagnostic(raw),
        None => None,
    };
    let account_mode = match observation.account_mode.as_deref() {
        Some(value) => Some(validate_optional_text(
            "account_mode",
            value,
            MAX_SHORT_TEXT_LEN,
        )?),
        None => None,
    };
    let plan = match observation.plan.as_deref() {
        Some(value) => {
            Some(validate_optional_text("plan", value, MAX_SHORT_TEXT_LEN)?)
        }
        None => None,
    };
    if observation.windows.len() > MAX_WINDOWS {
        return Err(validation(format!(
            "provider {provider} has more than {MAX_WINDOWS} windows"
        )));
    }
    let mut windows = Vec::with_capacity(observation.windows.len());
    let mut seen_keys = BTreeSet::new();
    for window in observation.windows {
        let validated = validate_window(&provider, window, now)?;
        if !seen_keys.insert(validated.key.clone()) {
            return Err(validation(format!(
                "provider {provider} has duplicate window key {:?}",
                validated.key
            )));
        }
        windows.push(validated);
    }
    validate_outcome_inventory(
        &provider,
        observation.outcome,
        observation.completeness,
        observation.authoritative_empty,
        windows.len(),
    )?;
    Ok(ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider,
        context_id,
        account_generation: observation.account_generation,
        ordering_token: observation.ordering_token,
        received_at: observation.received_at,
        source: observation.source,
        outcome: observation.outcome,
        reason_code: observation.reason_code,
        diagnostic,
        completeness: observation.completeness,
        authoritative_empty: observation.authoritative_empty,
        account_mode,
        plan,
        windows,
    })
}

/// Project sanitized observations into the public read model.
///
/// Duplicate provider IDs are rejected: merging belongs to the store phase.
/// Providers and windows are ordered by id/key. Missing quantities are
/// null, never zero.
pub fn project_usage_snapshot(
    observations: &[ProviderUsageObservationWire],
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> Result<UsagePublicSnapshotWire> {
    validate_now(now)?;
    let cadence_seconds = validate_usage_cadence(cadence_seconds)?;
    let (warn_percent, critical_percent) =
        validate_usage_thresholds(warn_percent, critical_percent)?;
    if observations.len() > MAX_OBSERVATIONS {
        return Err(validation(format!(
            "usage snapshot has more than {MAX_OBSERVATIONS} providers"
        )));
    }
    let mut by_provider: BTreeMap<String, UsagePublicProviderWire> =
        BTreeMap::new();
    for observation in observations {
        let validated = validate_usage_observation(observation.clone(), now)?;
        if by_provider.contains_key(&validated.provider) {
            return Err(validation(format!(
                "duplicate provider in usage snapshot: {}",
                validated.provider
            )));
        }
        let public = project_provider(
            validated,
            now,
            cadence_seconds,
            warn_percent,
            critical_percent,
        );
        by_provider.insert(public.provider.clone(), public);
    }
    let providers: Vec<UsagePublicProviderWire> =
        by_provider.into_values().collect();
    let collection_health = snapshot_health(&providers);
    let attention = snapshot_attention(&providers);
    Ok(UsagePublicSnapshotWire {
        schema_version: PROVIDER_USAGE_PUBLIC_SCHEMA_VERSION,
        generated_at: now,
        collection_health,
        providers,
        attention,
    })
}

/// Model-scoped summary over already-projected public windows.
pub fn summarize_usage_windows(
    windows: &[UsagePublicWindowWire],
    model_id: Option<&str>,
) -> Option<UsageScopedSummaryWire> {
    summarize_filtered_windows(windows, model_id)
}

/// Known rejected/low constraints for a provider overview or model.
pub fn usage_known_constraints(
    windows: &[UsagePublicWindowWire],
    model_id: Option<&str>,
    warn_percent: f64,
    critical_percent: f64,
) -> Vec<UsageKnownConstraintWire> {
    constraint_list(windows, model_id, warn_percent, critical_percent)
}

fn project_provider(
    observation: ProviderUsageObservationWire,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> UsagePublicProviderWire {
    let mut windows: Vec<UsagePublicWindowWire> = observation
        .windows
        .iter()
        .map(|window| project_window(window, now, cadence_seconds))
        .collect();
    windows.sort_by(|left, right| left.key.cmp(&right.key));
    let last_full_observation_at =
        if observation.completeness == UsageCompleteness::Complete {
            Some(observation.ordering_token)
        } else {
            None
        };
    let summary = if observation.outcome == UsageCollectionOutcome::Ok {
        summarize_filtered_windows(&windows, None)
    } else {
        None
    };
    let known_constraints = if observation.outcome == UsageCollectionOutcome::Ok
    {
        constraint_list(&windows, None, warn_percent, critical_percent)
    } else {
        Vec::new()
    };
    let attention = provider_attention(
        &observation.provider,
        observation.outcome,
        None,
        &windows,
        &summary,
        warn_percent,
        critical_percent,
    );
    UsagePublicProviderWire {
        provider: observation.provider,
        context_ref: observation.context_id,
        account_generation: observation.account_generation,
        collection_status: observation.outcome,
        collection_reason: observation.reason_code,
        collector_health: None,
        completeness: observation.completeness,
        last_attempt_at: observation.received_at,
        last_full_observation_at,
        diagnostic: observation.diagnostic,
        account_mode: observation.account_mode,
        plan: observation.plan,
        windows,
        summary,
        known_constraints,
        attention,
    }
}

fn project_window(
    window: &UsageWindowObservationWire,
    now: f64,
    cadence_seconds: f64,
) -> UsagePublicWindowWire {
    let used_percent = window.used_percent;
    UsagePublicWindowWire {
        key: window.key.clone(),
        label: window.label.clone(),
        used_percent,
        remaining_percent: remaining_percent_value(used_percent),
        exceeded_by_percent: exceeded_by_percent_value(used_percent),
        freshness: freshness_value(window.observed_at, now, cadence_seconds),
        reset_passed: reset_has_passed(window.resets_at, now),
        resets_at: window.resets_at,
        duration_seconds: window.duration_seconds,
        period_start: window.period_start,
        age_seconds: (now - window.observed_at.min(now)).max(0.0),
        observed_at: window.observed_at,
        source: window.source,
        vendor_state: window.vendor_state,
        applicability: window.applicability.clone(),
    }
}

fn summarize_filtered_windows(
    windows: &[UsagePublicWindowWire],
    model_id: Option<&str>,
) -> Option<UsageScopedSummaryWire> {
    let mut considered = Vec::new();
    let mut has_unknown_scope = false;
    let mut has_stale_or_unknown = false;
    let mut has_reset_or_missing = false;
    for window in windows {
        match usage_window_applies(&window.applicability, model_id) {
            UsageApplicabilityMatch::DoesNotApply => continue,
            UsageApplicabilityMatch::Unknown => {
                has_unknown_scope = true;
                if window.reset_passed
                    || window.freshness != UsageFreshness::Fresh
                {
                    has_stale_or_unknown = true;
                }
            }
            UsageApplicabilityMatch::Applies => {
                if window.reset_passed {
                    has_reset_or_missing = true;
                    continue;
                }
                if window.freshness != UsageFreshness::Fresh {
                    has_stale_or_unknown = true;
                    continue;
                }
                considered.push(window);
            }
        }
    }
    if considered.is_empty() {
        return None;
    }
    let min_remaining = considered
        .iter()
        .map(|window| window.remaining_percent)
        .fold(f64::INFINITY, f64::min);
    let mut limiting: Vec<&&UsagePublicWindowWire> = considered
        .iter()
        .filter(|window| window.remaining_percent == min_remaining)
        .collect();
    limiting.sort_by(|left, right| left.key.cmp(&right.key));
    let limiting_window_keys = limiting
        .iter()
        .map(|window| window.key.clone())
        .collect::<Vec<_>>();
    let scope = limiting[0].applicability.clone();
    let freshness = limiting[0].freshness;
    let used_percent = limiting[0].used_percent;
    let complete =
        !has_unknown_scope && !has_stale_or_unknown && !has_reset_or_missing;
    let completeness = if complete {
        UsageCompleteness::Complete
    } else {
        UsageCompleteness::Partial
    };
    Some(UsageScopedSummaryWire {
        remaining_percent: min_remaining,
        used_percent,
        limiting_window_keys,
        scope,
        completeness,
        freshness,
    })
}

fn constraint_list(
    windows: &[UsagePublicWindowWire],
    model_id: Option<&str>,
    warn_percent: f64,
    critical_percent: f64,
) -> Vec<UsageKnownConstraintWire> {
    let mut constraints = Vec::new();
    for window in windows {
        if usage_window_applies(&window.applicability, model_id)
            == UsageApplicabilityMatch::DoesNotApply
        {
            continue;
        }
        let attention =
            window_attention(window, warn_percent, critical_percent);
        if !attention.is_constraint() {
            continue;
        }
        constraints.push(UsageKnownConstraintWire {
            window_key: window.key.clone(),
            vendor_state: window.vendor_state,
            used_percent: window.used_percent,
            remaining_percent: window.remaining_percent,
            freshness: window.freshness,
            reset_passed: window.reset_passed,
            observed_at: window.observed_at,
            attention,
        });
    }
    constraints.sort_by(|left, right| {
        right
            .attention
            .rank()
            .cmp(&left.attention.rank())
            .then_with(|| left.window_key.cmp(&right.window_key))
    });
    constraints
}

fn window_attention(
    window: &UsagePublicWindowWire,
    warn_percent: f64,
    critical_percent: f64,
) -> UsageAttentionKind {
    if window.vendor_state == UsageVendorState::Rejected {
        return UsageAttentionKind::Rejected;
    }
    if window.reset_passed {
        return UsageAttentionKind::None;
    }
    if window.used_percent >= critical_percent {
        UsageAttentionKind::VeryLow
    } else if window.used_percent >= warn_percent {
        UsageAttentionKind::Low
    } else {
        UsageAttentionKind::None
    }
}

fn provider_attention(
    provider: &str,
    outcome: UsageCollectionOutcome,
    collector_health: Option<&UsageCollectorHealthWire>,
    windows: &[UsagePublicWindowWire],
    summary: &Option<UsageScopedSummaryWire>,
    warn_percent: f64,
    critical_percent: f64,
) -> UsageAttentionWire {
    let mut best = UsageAttentionWire {
        kind: if collection_problem_is_attentive(
            outcome,
            collector_health,
            windows,
            summary,
        ) {
            UsageAttentionKind::CollectionProblem
        } else {
            UsageAttentionKind::None
        },
        provider: provider.to_string(),
        window_key: None,
    };
    for window in windows {
        if usage_window_applies(&window.applicability, None)
            == UsageApplicabilityMatch::DoesNotApply
        {
            continue;
        }
        let kind = window_attention(window, warn_percent, critical_percent);
        if kind.rank() > best.kind.rank()
            || (kind.rank() == best.kind.rank()
                && kind.rank() > 0
                && window_key_is_less(
                    window.key.as_str(),
                    best.window_key.as_deref(),
                ))
        {
            best.kind = kind;
            best.window_key = Some(window.key.clone());
        }
    }
    best
}

fn collector_health_from_schedule(
    schedule: Option<&ProviderUsageRefreshScheduleWire>,
) -> Option<UsageCollectorHealthWire> {
    let schedule = schedule?;
    let consecutive_failures = schedule.consecutive_failures;
    let state = if consecutive_failures >= USAGE_COLLECTOR_FAILING_THRESHOLD {
        UsageCollectorHealthState::Failing
    } else if consecutive_failures > 0 {
        UsageCollectorHealthState::Degraded
    } else {
        UsageCollectorHealthState::Ok
    };
    Some(UsageCollectorHealthWire {
        state,
        consecutive_failures,
        last_success_at: schedule.last_success_at,
        failing_since: if consecutive_failures > 0 {
            schedule.first_failure_at
        } else {
            None
        },
    })
}

fn collection_problem_is_attentive(
    outcome: UsageCollectionOutcome,
    collector_health: Option<&UsageCollectorHealthWire>,
    windows: &[UsagePublicWindowWire],
    summary: &Option<UsageScopedSummaryWire>,
) -> bool {
    if collector_health.is_some_and(|health| {
        health.state == UsageCollectorHealthState::Failing
    }) {
        return true;
    }
    if outcome.is_problem() && windows.is_empty() {
        return true;
    }
    outcome == UsageCollectionOutcome::Ok
        && summary.is_none()
        && !windows.is_empty()
        && windows.iter().all(|window| {
            window.freshness == UsageFreshness::Unknown && !window.reset_passed
        })
}

fn window_key_is_less(candidate: &str, current: Option<&str>) -> bool {
    match current {
        None => true,
        Some(current) => candidate < current,
    }
}

fn snapshot_health(
    providers: &[UsagePublicProviderWire],
) -> UsageCollectionHealth {
    if providers.is_empty() {
        return UsageCollectionHealth::Empty;
    }
    let mut any_problem = false;
    let mut any_ok = false;
    for provider in providers {
        if provider.collection_status.is_problem() {
            any_problem = true;
        } else {
            any_ok = true;
        }
    }
    match (any_problem, any_ok) {
        (true, true) => UsageCollectionHealth::Partial,
        (true, false) => UsageCollectionHealth::Error,
        (false, _) => UsageCollectionHealth::Ok,
    }
}

fn snapshot_attention(
    providers: &[UsagePublicProviderWire],
) -> Option<UsageAttentionWire> {
    let mut best: Option<&UsageAttentionWire> = None;
    for provider in providers {
        let candidate = &provider.attention;
        if candidate.kind == UsageAttentionKind::None {
            continue;
        }
        let take = match best {
            None => true,
            Some(current) => {
                candidate.kind.rank() > current.kind.rank()
                    || (candidate.kind.rank() == current.kind.rank()
                        && candidate.provider < current.provider)
            }
        };
        if take {
            best = Some(candidate);
        }
    }
    best.cloned()
}

fn validate_window(
    provider: &str,
    window: UsageWindowObservationWire,
    now: f64,
) -> Result<UsageWindowObservationWire> {
    let key = validate_ident(
        &format!("provider {provider} window key"),
        &window.key,
        MAX_KEY_LEN,
    )?;
    let label = strip_controls(&window.label);
    let label = label.trim();
    if label.is_empty() {
        return Err(validation(format!(
            "provider {provider} window {key} label must be non-empty text"
        )));
    }
    if label.len() > MAX_LABEL_LEN {
        return Err(validation(format!(
            "provider {provider} window {key} label exceeds {MAX_LABEL_LEN} bytes"
        )));
    }
    if label.chars().any(char::is_control) {
        return Err(validation(format!(
            "provider {provider} window {key} label must not contain control characters"
        )));
    }
    validate_used_percent(window.used_percent)?;
    if let Some(resets_at) = window.resets_at {
        validate_epoch_seconds(
            &format!("provider {provider} window {key} resets_at"),
            resets_at,
        )?;
    }
    if let Some(duration) = window.duration_seconds {
        if !duration.is_finite() || duration <= 0.0 {
            return Err(validation(format!(
                "provider {provider} window {key} duration_seconds must be finite and positive"
            )));
        }
    }
    if let Some(period_start) = window.period_start {
        validate_epoch_seconds(
            &format!("provider {provider} window {key} period_start"),
            period_start,
        )?;
        if let Some(resets_at) = window.resets_at {
            if period_start >= resets_at {
                return Err(validation(format!(
                    "provider {provider} window {key} period_start must precede resets_at"
                )));
            }
            if let Some(duration) = window.duration_seconds {
                let span = resets_at - period_start;
                if (span - duration).abs() > PERIOD_TOLERANCE_SECONDS {
                    return Err(validation(format!(
                        "provider {provider} window {key} period does not match duration_seconds"
                    )));
                }
            }
        }
    }
    validate_timestamp(
        &format!("provider {provider} window {key} observed_at"),
        window.observed_at,
        now,
    )?;
    let applicability =
        validate_applicability(provider, &key, window.applicability)?;
    Ok(UsageWindowObservationWire {
        key,
        label: label.to_string(),
        used_percent: window.used_percent,
        resets_at: window.resets_at,
        duration_seconds: window.duration_seconds,
        period_start: window.period_start,
        applicability,
        observed_at: window.observed_at,
        source: window.source,
        vendor_state: window.vendor_state,
    })
}

fn validate_applicability(
    provider: &str,
    key: &str,
    applicability: UsageApplicabilityWire,
) -> Result<UsageApplicabilityWire> {
    match applicability {
        UsageApplicabilityWire::Account => Ok(UsageApplicabilityWire::Account),
        UsageApplicabilityWire::Product { product, model_ids } => {
            let product = validate_optional_text(
                &format!("provider {provider} window {key} product"),
                &product,
                MAX_SHORT_TEXT_LEN,
            )?;
            let model_ids = validate_model_ids(provider, key, model_ids)?;
            Ok(UsageApplicabilityWire::Product { product, model_ids })
        }
        UsageApplicabilityWire::Models { model_ids } => {
            if model_ids.is_empty() {
                return Err(validation(format!(
                    "provider {provider} window {key} models applicability requires model_ids"
                )));
            }
            let model_ids = validate_model_ids(provider, key, model_ids)?;
            Ok(UsageApplicabilityWire::Models { model_ids })
        }
        UsageApplicabilityWire::ModelFamily { family, model_ids } => {
            let family = validate_optional_text(
                &format!("provider {provider} window {key} family"),
                &family,
                MAX_SHORT_TEXT_LEN,
            )?;
            let model_ids = validate_model_ids(provider, key, model_ids)?;
            Ok(UsageApplicabilityWire::ModelFamily { family, model_ids })
        }
        UsageApplicabilityWire::Unknown {
            vendor_label,
            vendor_id,
        } => {
            let vendor_label = match vendor_label {
                Some(label) => {
                    let cleaned = strip_controls(&label);
                    let cleaned = cleaned.trim();
                    if cleaned.is_empty() {
                        None
                    } else {
                        Some(validate_optional_text(
                            &format!(
                                "provider {provider} window {key} vendor_label"
                            ),
                            cleaned,
                            MAX_LABEL_LEN,
                        )?)
                    }
                }
                None => None,
            };
            let vendor_id = match vendor_id {
                Some(id) => Some(validate_ident(
                    &format!("provider {provider} window {key} vendor_id"),
                    &id,
                    MAX_KEY_LEN,
                )?),
                None => None,
            };
            Ok(UsageApplicabilityWire::Unknown {
                vendor_label,
                vendor_id,
            })
        }
    }
}

fn validate_model_ids(
    provider: &str,
    key: &str,
    model_ids: Vec<String>,
) -> Result<Vec<String>> {
    if model_ids.len() > MAX_MODEL_IDS {
        return Err(validation(format!(
            "provider {provider} window {key} has more than {MAX_MODEL_IDS} model ids"
        )));
    }
    let mut seen = BTreeSet::new();
    let mut cleaned = Vec::with_capacity(model_ids.len());
    for model_id in model_ids {
        let model_id = validate_ident(
            &format!("provider {provider} window {key} model id"),
            &model_id,
            MAX_KEY_LEN,
        )?;
        if !seen.insert(model_id.clone()) {
            return Err(validation(format!(
                "provider {provider} window {key} has duplicate model id {model_id:?}"
            )));
        }
        cleaned.push(model_id);
    }
    Ok(cleaned)
}

fn validate_outcome_inventory(
    provider: &str,
    outcome: UsageCollectionOutcome,
    completeness: UsageCompleteness,
    authoritative_empty: bool,
    window_count: usize,
) -> Result<()> {
    match outcome {
        UsageCollectionOutcome::Ok => {
            if authoritative_empty {
                if window_count != 0 {
                    return Err(validation(format!(
                        "provider {provider} authoritative empty inventory must not include windows"
                    )));
                }
                if completeness != UsageCompleteness::Complete {
                    return Err(validation(format!(
                        "provider {provider} authoritative empty inventory must be complete"
                    )));
                }
                return Ok(());
            }
            if window_count == 0 {
                return Err(validation(format!(
                    "provider {provider} ok observation is not an empty complete inventory"
                )));
            }
            Ok(())
        }
        UsageCollectionOutcome::Error => {
            if window_count != 0 || authoritative_empty {
                return Err(validation(format!(
                    "provider {provider} error observation must not include windows"
                )));
            }
            if completeness != UsageCompleteness::Partial {
                return Err(validation(format!(
                    "provider {provider} error observation must be partial"
                )));
            }
            Ok(())
        }
        UsageCollectionOutcome::NotApplicable
        | UsageCollectionOutcome::Unauthenticated
        | UsageCollectionOutcome::Unsupported => {
            if window_count != 0 || authoritative_empty {
                return Err(validation(format!(
                    "provider {provider} {} observation must not include windows",
                    outcome_name(outcome)
                )));
            }
            if completeness != UsageCompleteness::Complete {
                return Err(validation(format!(
                    "provider {provider} {} observation must be a complete non-subscription state",
                    outcome_name(outcome)
                )));
            }
            Ok(())
        }
    }
}

fn outcome_name(outcome: UsageCollectionOutcome) -> &'static str {
    match outcome {
        UsageCollectionOutcome::Ok => "ok",
        UsageCollectionOutcome::NotApplicable => "not_applicable",
        UsageCollectionOutcome::Unauthenticated => "unauthenticated",
        UsageCollectionOutcome::Unsupported => "unsupported",
        UsageCollectionOutcome::Error => "error",
    }
}

fn validate_used_percent(used_percent: f64) -> Result<()> {
    if !used_percent.is_finite() || used_percent < 0.0 {
        return Err(validation("used_percent must be finite and nonnegative"));
    }
    Ok(())
}

fn validate_now(now: f64) -> Result<()> {
    if !now.is_finite() || now <= 0.0 {
        return Err(validation(
            "current timestamp must be finite and positive",
        ));
    }
    Ok(())
}

fn validate_epoch_seconds(name: &str, value: f64) -> Result<()> {
    if !value.is_finite() || value <= 0.0 {
        return Err(validation(format!(
            "{name} must be a finite positive timestamp"
        )));
    }
    Ok(())
}

fn validate_timestamp(name: &str, value: f64, now: f64) -> Result<()> {
    validate_epoch_seconds(name, value)?;
    if value > now + MAX_FUTURE_SKEW_SECONDS {
        return Err(validation(format!(
            "{name} is implausibly ahead of the injected clock"
        )));
    }
    Ok(())
}

fn validate_ident(name: &str, value: &str, max_len: usize) -> Result<String> {
    if value != value.trim() {
        return Err(validation(format!(
            "{name} must not contain leading or trailing whitespace"
        )));
    }
    if value.is_empty() {
        return Err(validation(format!("{name} must be non-empty")));
    }
    if value.len() > max_len {
        return Err(validation(format!("{name} exceeds {max_len} bytes")));
    }
    if value.chars().any(char::is_control) {
        return Err(validation(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_optional_text(
    name: &str,
    value: &str,
    max_len: usize,
) -> Result<String> {
    let cleaned = strip_controls(value);
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        return Err(validation(format!("{name} must be non-empty text")));
    }
    if cleaned.len() > max_len {
        return Err(validation(format!("{name} exceeds {max_len} bytes")));
    }
    Ok(cleaned.to_string())
}

fn sanitize_diagnostic(raw: &str) -> Option<String> {
    let stripped = strip_controls(raw);
    let stripped = stripped.trim();
    if stripped.is_empty() {
        return None;
    }
    let redacted = if looks_like_secret(stripped) {
        "diagnostic redacted"
    } else {
        stripped
    };
    let truncated = if redacted.len() > MAX_DIAGNOSTIC_LEN {
        let mut end = MAX_DIAGNOSTIC_LEN;
        while end > 0 && !redacted.is_char_boundary(end) {
            end -= 1;
        }
        &redacted[..end]
    } else {
        redacted
    };
    Some(truncated.to_string())
}

fn looks_like_secret(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("bearer ")
        || lower.contains("sk-")
        || lower.contains("token=")
        || lower.contains("/home/")
        || lower.contains("/users/")
        || lower.contains("~/.")
        || value.contains("://")
        || (value.contains('@') && value.contains('.'))
}

fn strip_controls(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            match chars.peek().copied() {
                Some('[') => {
                    chars.next();
                    for next in chars.by_ref() {
                        if ('@'..='~').contains(&next) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next();
                    for next in chars.by_ref() {
                        if next == '\u{7}' || next == '\u{9c}' {
                            break;
                        }
                    }
                }
                Some(_) => {
                    chars.next();
                }
                None => {}
            }
            continue;
        }
        if ch.is_control() {
            continue;
        }
        output.push(ch);
    }
    output
}

#[cfg(test)]
mod tests;
