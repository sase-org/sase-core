//! Grok billing-config normalization for included-allowance observations.
//!
//! Probe transport stays in Python. This module owns percentage, period,
//! completeness, and plan-label decisions for the verified Grok billing
//! wire, including the omitted-zero unified-billing shape after a weekly
//! reset.

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{
    validate_usage_observation, ProviderUsageObservationWire, Result,
    UsageApplicabilityWire, UsageCollectionOutcome, UsageCompleteness,
    UsageReasonCode, UsageSource, UsageVendorState, UsageWindowObservationWire,
    PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
};

const MISSING_PERCENT_DIAGNOSTIC: &str = "grok_billing_usage_percent_missing";
const MISSING_CONFIG_DIAGNOSTIC: &str = "grok_billing_config_missing";
const COMPLETE_DIAGNOSTIC: &str = "grok_build_billing_first_party_unstable";
const MISSING_RESET_DIAGNOSTIC: &str = "grok_billing_period_missing_reset";

/// Versioned request for [`normalize_grok_billing`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageNormalizeGrokBillingRequestWire {
    pub schema_version: u32,
    pub payload: Value,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub request_started_at: f64,
    pub now: f64,
}

#[derive(Clone, Copy)]
enum PeriodKind {
    Weekly,
    Monthly,
    Generic,
}

struct Period {
    kind: PeriodKind,
    resets_at: Option<f64>,
    duration_seconds: Option<f64>,
    period_start: Option<f64>,
}

impl Period {
    fn key(&self) -> &'static str {
        match self.kind {
            PeriodKind::Weekly => "included_weekly",
            PeriodKind::Monthly => "included_monthly",
            PeriodKind::Generic => "included",
        }
    }

    fn label(&self) -> &'static str {
        match self.kind {
            PeriodKind::Weekly => "Grok included weekly allowance",
            PeriodKind::Monthly => "Grok included monthly allowance",
            PeriodKind::Generic => "Grok included allowance",
        }
    }
}

/// Normalize a decoded Grok billing payload into a validated observation.
///
/// Malformed vendor data becomes a structured error observation. Invalid
/// binding requests return [`super::ProviderUsageError::Validation`].
pub fn normalize_grok_billing(
    request: ProviderUsageNormalizeGrokBillingRequestWire,
) -> Result<ProviderUsageObservationWire> {
    if request.schema_version != PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION {
        return Err(super::validation(format!(
            "unsupported provider-usage grok billing schema version: {}",
            request.schema_version
        )));
    }
    let observation = observation_from_payload(&request);
    validate_usage_observation(observation, request.now)
}

fn observation_from_payload(
    request: &ProviderUsageNormalizeGrokBillingRequestWire,
) -> ProviderUsageObservationWire {
    let Some(payload) = request.payload.as_object() else {
        return status_observation(request, MISSING_CONFIG_DIAGNOSTIC);
    };
    let Some(config) = payload.get("config").and_then(Value::as_object) else {
        return status_observation(request, MISSING_CONFIG_DIAGNOSTIC);
    };
    match usage_percent(config) {
        Some(used_percent) => {
            ok_observation(request, payload, config, used_percent)
        }
        None => status_observation(request, MISSING_PERCENT_DIAGNOSTIC),
    }
}

fn ok_observation(
    request: &ProviderUsageNormalizeGrokBillingRequestWire,
    payload: &Map<String, Value>,
    config: &Map<String, Value>,
    used_percent: f64,
) -> ProviderUsageObservationWire {
    let period = billing_period(config);
    let (completeness, diagnostic) = if period.resets_at.is_none() {
        (UsageCompleteness::Partial, MISSING_RESET_DIAGNOSTIC)
    } else {
        (UsageCompleteness::Complete, COMPLETE_DIAGNOSTIC)
    };
    let window = UsageWindowObservationWire {
        key: period.key().to_string(),
        label: period.label().to_string(),
        used_percent,
        resets_at: period.resets_at,
        duration_seconds: period.duration_seconds,
        period_start: period.period_start,
        applicability: UsageApplicabilityWire::Account,
        observed_at: request.now,
        source: UsageSource::Probe,
        vendor_state: UsageVendorState::Unknown,
    };
    ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: request.provider.clone(),
        context_id: request.context_id.clone(),
        account_generation: request.account_generation,
        ordering_token: request.request_started_at,
        received_at: request.now,
        source: UsageSource::Probe,
        outcome: UsageCollectionOutcome::Ok,
        reason_code: None,
        diagnostic: Some(diagnostic.to_string()),
        completeness,
        authoritative_empty: false,
        account_mode: Some("subscription".to_string()),
        plan: plan_label(payload, config),
        windows: vec![window],
    }
}

fn status_observation(
    request: &ProviderUsageNormalizeGrokBillingRequestWire,
    diagnostic: &str,
) -> ProviderUsageObservationWire {
    ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: request.provider.clone(),
        context_id: request.context_id.clone(),
        account_generation: request.account_generation,
        ordering_token: request.request_started_at,
        received_at: request.now,
        source: UsageSource::Probe,
        outcome: UsageCollectionOutcome::Error,
        reason_code: Some(UsageReasonCode::MalformedPayload),
        diagnostic: Some(diagnostic.to_string()),
        completeness: UsageCompleteness::Partial,
        authoritative_empty: false,
        account_mode: None,
        plan: None,
        windows: Vec::new(),
    }
}

fn usage_percent(config: &Map<String, Value>) -> Option<f64> {
    if let Some(raw) = config.get("creditUsagePercent") {
        return finite_nonnegative_number(raw);
    }
    let used_present = config.contains_key("used");
    let limit_present = config.contains_key("monthlyLimit");
    if used_present && limit_present {
        let used = cent_value(config.get("used"));
        let limit = cent_value(config.get("monthlyLimit"));
        return match (used, limit) {
            (Some(used), Some(limit)) if limit > 0.0 => {
                Some((used / limit) * 100.0)
            }
            _ => None,
        };
    }
    if !used_present && !limit_present && omitted_zero_percent(config) {
        return Some(0.0);
    }
    None
}

fn omitted_zero_percent(config: &Map<String, Value>) -> bool {
    if config.get("isUnifiedBillingUser") != Some(&Value::Bool(true)) {
        return false;
    }
    let Some(current) = config.get("currentPeriod").and_then(Value::as_object)
    else {
        return false;
    };
    let Some(kind) = current.get("type").and_then(period_kind) else {
        return false;
    };
    matches!(kind, PeriodKind::Weekly | PeriodKind::Monthly)
        && valid_current_period_span(current).is_some()
}

fn valid_current_period_span(
    current: &Map<String, Value>,
) -> Option<(f64, f64)> {
    let start = current.get("start").and_then(parse_timestamp)?;
    let end = current.get("end").and_then(parse_timestamp)?;
    (end > start).then_some((start, end))
}

fn billing_period(config: &Map<String, Value>) -> Period {
    let current = config.get("currentPeriod").and_then(Value::as_object);
    let mut kind = current
        .and_then(|period| period.get("type"))
        .and_then(period_kind);
    let mut start_value = current.and_then(|period| period.get("start"));
    let mut end_value = current.and_then(|period| period.get("end"));
    if kind.is_none() {
        start_value = start_value.or_else(|| config.get("billingPeriodStart"));
        end_value = end_value.or_else(|| config.get("billingPeriodEnd"));
        if start_value.is_some() || end_value.is_some() {
            kind = Some(PeriodKind::Monthly);
        }
    }
    let period_start = start_value.and_then(parse_timestamp);
    let resets_at = end_value.and_then(parse_timestamp);
    attach_span(kind.unwrap_or(PeriodKind::Generic), period_start, resets_at)
}

fn attach_span(
    kind: PeriodKind,
    period_start: Option<f64>,
    resets_at: Option<f64>,
) -> Period {
    match (period_start, resets_at) {
        (Some(start), Some(end)) if end > start => Period {
            kind,
            resets_at: Some(end),
            duration_seconds: Some(end - start),
            period_start: Some(start),
        },
        (None, Some(end)) => Period {
            kind,
            resets_at: Some(end),
            duration_seconds: None,
            period_start: None,
        },
        (Some(start), None) => Period {
            kind,
            resets_at: None,
            duration_seconds: None,
            period_start: Some(start),
        },
        _ => Period {
            kind,
            resets_at: None,
            duration_seconds: None,
            period_start: None,
        },
    }
}

fn period_kind(value: &Value) -> Option<PeriodKind> {
    let Value::String(raw) = value else {
        return None;
    };
    let normalized = raw.to_ascii_lowercase();
    if normalized.contains("weekly") || normalized.contains("week") {
        return Some(PeriodKind::Weekly);
    }
    if normalized.contains("monthly") || normalized.contains("month") {
        return Some(PeriodKind::Monthly);
    }
    None
}

fn cent_value(value: Option<&Value>) -> Option<f64> {
    let Value::Object(object) = value? else {
        return None;
    };
    match object.get("val") {
        None => Some(0.0),
        Some(raw) => finite_nonnegative_number(raw),
    }
}

fn finite_nonnegative_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0),
        _ => None,
    }
}

fn parse_timestamp(value: &Value) -> Option<f64> {
    match value {
        Value::Bool(_) => None,
        Value::Number(number) => {
            number.as_f64().filter(|value| value.is_finite())
        }
        Value::String(raw) => parse_timestamp_str(raw),
        _ => None,
    }
}

fn parse_timestamp_str(raw: &str) -> Option<f64> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
        return Some(datetime_epoch_seconds(&parsed));
    }
    DateTime::parse_from_rfc3339(&format!("{text}Z"))
        .ok()
        .map(|parsed| datetime_epoch_seconds(&parsed))
}

fn datetime_epoch_seconds(parsed: &DateTime<chrono::FixedOffset>) -> f64 {
    parsed.timestamp() as f64
        + f64::from(parsed.timestamp_subsec_nanos()) / 1_000_000_000.0
}

fn plan_label(
    payload: &Map<String, Value>,
    config: &Map<String, Value>,
) -> Option<String> {
    nonempty_string(payload.get("subscription_tier"))
        .or_else(|| nonempty_string(payload.get("subscriptionTier")))
        .or_else(|| nonempty_string(config.get("subscriptionTier")))
}

fn nonempty_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: f64 = 1_800_000_000.0;

    fn request(payload: Value) -> ProviderUsageNormalizeGrokBillingRequestWire {
        ProviderUsageNormalizeGrokBillingRequestWire {
            schema_version: 1,
            payload,
            provider: "grok".to_string(),
            context_id: "probe".to_string(),
            account_generation: 1,
            request_started_at: NOW,
            now: NOW,
        }
    }

    fn weekly_period() -> Value {
        json!({
            "type": "USAGE_PERIOD_TYPE_WEEKLY",
            "start": "2027-01-15T00:00:00Z",
            "end": "2027-01-22T00:00:00Z",
        })
    }

    fn unified_omitted_zero_payload() -> Value {
        json!({
            "subscription_tier": "SuperGrok Heavy",
            "config": {
                "currentPeriod": {
                    "type": "USAGE_PERIOD_TYPE_WEEKLY",
                    "start": "2027-01-15T00:00:00.000000+00:00",
                    "end": "2027-01-22T00:00:00.000000+00:00",
                },
                "isUnifiedBillingUser": true,
                "billingPeriodStart": "2027-01-15T00:00:00.000000+00:00",
                "billingPeriodEnd": "2027-01-22T00:00:00.000000+00:00",
                "onDemandEnabled": true,
                "prepaidBalance": {"val": 1234},
                "history": [{"amount": 1}],
            }
        })
    }

    fn assert_malformed(payload: Value) {
        let observation = normalize_grok_billing(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Error);
        assert_eq!(
            observation.reason_code,
            Some(UsageReasonCode::MalformedPayload)
        );
        assert_eq!(
            observation.diagnostic.as_deref(),
            Some(MISSING_PERCENT_DIAGNOSTIC)
        );
        assert!(observation.windows.is_empty());
    }

    #[test]
    fn omitted_zero_unified_weekly_is_complete() {
        let observation =
            normalize_grok_billing(request(unified_omitted_zero_payload()))
                .unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert_eq!(observation.plan.as_deref(), Some("SuperGrok Heavy"));
        assert_eq!(observation.account_mode.as_deref(), Some("subscription"));
        assert_eq!(observation.windows.len(), 1);
        let window = &observation.windows[0];
        assert_eq!(window.key, "included_weekly");
        assert_eq!(window.used_percent, 0.0);
        assert_eq!(window.vendor_state, UsageVendorState::Unknown);
        assert_eq!(window.duration_seconds, Some(604_800.0));
        let encoded = serde_json::to_value(&observation).unwrap();
        let encoded_text = encoded.to_string();
        assert!(!encoded_text.contains("onDemand"));
        assert!(!encoded_text.contains("prepaid"));
    }

    #[test]
    fn omitted_zero_unified_monthly_and_explicit_zero_agree() {
        let monthly = json!({
            "subscription_tier": "SuperGrok Heavy",
            "config": {
                "currentPeriod": {
                    "type": "USAGE_PERIOD_TYPE_MONTHLY",
                    "start": "2027-01-01T00:00:00Z",
                    "end": "2027-02-01T00:00:00Z",
                },
                "isUnifiedBillingUser": true,
            }
        });
        let explicit = json!({
            "subscription_tier": "SuperGrok Heavy",
            "config": {
                "creditUsagePercent": 0.0,
                "currentPeriod": weekly_period(),
                "isUnifiedBillingUser": true,
            }
        });
        let monthly_obs = normalize_grok_billing(request(monthly)).unwrap();
        let explicit_obs = normalize_grok_billing(request(explicit)).unwrap();
        assert_eq!(monthly_obs.windows[0].key, "included_monthly");
        assert_eq!(monthly_obs.windows[0].used_percent, 0.0);
        assert_eq!(explicit_obs.windows[0].used_percent, 0.0);
        assert_eq!(explicit_obs.plan.as_deref(), Some("SuperGrok Heavy"));
    }

    #[test]
    fn explicit_percent_beats_conflicting_legacy_amounts() {
        let payload = json!({
            "config": {
                "creditUsagePercent": 37.5,
                "currentPeriod": weekly_period(),
                "monthlyLimit": {"val": 1000},
                "used": 900,
            }
        });
        let observation = normalize_grok_billing(request(payload)).unwrap();
        assert_eq!(observation.windows[0].used_percent, 37.5);
        assert_eq!(observation.windows[0].key, "included_weekly");
    }

    #[test]
    fn over_100_percent_is_preserved() {
        let payload = json!({
            "config": {
                "creditUsagePercent": 150.0,
                "currentPeriod": weekly_period(),
            }
        });
        let observation = normalize_grok_billing(request(payload)).unwrap();
        assert_eq!(observation.windows[0].used_percent, 150.0);
    }

    #[test]
    fn empty_used_object_and_explicit_zero_cent_are_zero() {
        for used in [json!({}), json!({"val": 0})] {
            let payload = json!({
                "config": {
                    "monthlyLimit": {"val": 2000},
                    "used": used,
                    "billingPeriodStart": "2027-01-01T00:00:00Z",
                    "billingPeriodEnd": "2027-02-01T00:00:00Z",
                }
            });
            let observation = normalize_grok_billing(request(payload)).unwrap();
            assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
            assert_eq!(observation.windows[0].used_percent, 0.0);
            assert_eq!(observation.windows[0].key, "included_monthly");
        }
    }

    #[test]
    fn legacy_ratio_still_works() {
        let payload = json!({
            "config": {
                "monthlyLimit": {"val": 1000},
                "used": {"val": 250},
                "billingPeriodStart": "2027-01-01T00:00:00Z",
                "billingPeriodEnd": "2027-02-01T00:00:00Z",
            }
        });
        let observation = normalize_grok_billing(request(payload)).unwrap();
        assert_eq!(observation.windows[0].used_percent, 25.0);
    }

    #[test]
    fn absent_used_object_does_not_become_zero() {
        let payload = json!({
            "config": {
                "monthlyLimit": {"val": 1000},
                "isUnifiedBillingUser": true,
                "currentPeriod": weekly_period(),
            }
        });
        assert_malformed(payload);
    }

    #[test]
    fn invalid_explicit_percent_does_not_fall_back() {
        for percent in [json!(null), json!(true), json!("0"), json!(-1.0)] {
            let payload = json!({
                "config": {
                    "creditUsagePercent": percent,
                    "isUnifiedBillingUser": true,
                    "monthlyLimit": {"val": 1000},
                    "used": {"val": 0},
                    "currentPeriod": weekly_period(),
                }
            });
            assert_malformed(payload);
        }
    }

    #[test]
    fn omitted_zero_guards_reject_incomplete_shapes() {
        assert_malformed(json!({
            "config": { "currentPeriod": weekly_period() }
        }));
        assert_malformed(json!({
            "config": {
                "isUnifiedBillingUser": false,
                "currentPeriod": weekly_period(),
            }
        }));
        assert_malformed(json!({
            "config": {
                "isUnifiedBillingUser": true,
                "currentPeriod": {
                    "type": "USAGE_PERIOD_TYPE_WEEKLY",
                    "start": "2027-01-22T00:00:00Z",
                    "end": "2027-01-15T00:00:00Z",
                }
            }
        }));
        assert_malformed(json!({
            "config": {
                "isUnifiedBillingUser": true,
                "currentPeriod": {
                    "type": "USAGE_PERIOD_TYPE_DAILY",
                    "start": "2027-01-15T00:00:00Z",
                    "end": "2027-01-16T00:00:00Z",
                }
            }
        }));
        assert_malformed(json!({
            "config": {
                "isUnifiedBillingUser": "true",
                "currentPeriod": weekly_period(),
            }
        }));
        assert_malformed(json!({ "config": {} }));
    }

    #[test]
    fn native_plan_label_beats_camel_case() {
        let payload = json!({
            "subscription_tier": "SuperGrok Heavy",
            "subscriptionTier": "Ignored",
            "config": {
                "creditUsagePercent": 10.0,
                "subscriptionTier": "Also Ignored",
                "currentPeriod": weekly_period(),
            }
        });
        let observation = normalize_grok_billing(request(payload)).unwrap();
        assert_eq!(observation.plan.as_deref(), Some("SuperGrok Heavy"));
    }

    #[test]
    fn missing_reset_is_partial() {
        let payload = json!({
            "config": {
                "creditUsagePercent": 12.0,
                "subscriptionTier": "SuperGrok",
            }
        });
        let observation = normalize_grok_billing(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Partial);
        assert_eq!(
            observation.diagnostic.as_deref(),
            Some(MISSING_RESET_DIAGNOSTIC)
        );
        assert_eq!(observation.windows[0].key, "included");
        assert_eq!(observation.windows[0].resets_at, None);
        assert_eq!(observation.plan.as_deref(), Some("SuperGrok"));
    }

    #[test]
    fn unsupported_schema_version_is_binding_error() {
        let mut request = request(unified_omitted_zero_payload());
        request.schema_version = 2;
        let error = normalize_grok_billing(request).unwrap_err();
        assert!(error.to_string().contains("schema version: 2"));
    }
}
