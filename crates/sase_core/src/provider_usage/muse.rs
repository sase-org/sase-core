//! Muse Code subscription-usage normalization.
//!
//! Probe transport stays in Python. This module owns the decisions about the
//! `usage/read` result of `muse serve`: which windows it yields, how their
//! percentages and periods derive, and what an absent `usage` member means.
//!
//! The vendor reports two windows: a 5-hour-class `window` that carries its
//! duration, and a rolling `weekly` block that deliberately does not. The
//! weekly window therefore keeps `duration_seconds` unset rather than
//! inventing one; the indicator classifies it as weekly by provider and key
//! instead.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{
    validate_usage_observation, ProviderUsageObservationWire, Result,
    UsageApplicabilityWire, UsageCollectionOutcome, UsageCompleteness,
    UsageReasonCode, UsageSource, UsageVendorState, UsageWindowObservationWire,
    PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
};

const MALFORMED_DIAGNOSTIC: &str = "muse_usage_malformed";
const NOT_YET_OBSERVED_DIAGNOSTIC: &str = "muse_usage_not_yet_observed";

const SESSION_KEY: &str = "session";
const SESSION_LABEL: &str = "Muse 5-hour session";
const WEEKLY_KEY: &str = "weekly";
const WEEKLY_LABEL: &str = "Muse weekly all models";

/// Versioned request for [`normalize_muse_usage`].
///
/// `payload` is the whole `usage/read` result object, not its `usage` member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageNormalizeMuseUsageRequestWire {
    pub schema_version: u32,
    pub payload: Value,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub request_started_at: f64,
    pub now: f64,
}

struct MuseWindow {
    used_percent: f64,
    resets_at: f64,
    duration_seconds: Option<f64>,
}

struct MuseUsage {
    observed_at: f64,
    session: MuseWindow,
    weekly: MuseWindow,
}

/// Normalize a decoded Muse `usage/read` result into a validated observation.
///
/// An absent `usage` member is truthful absence, not an error: the host has
/// simply not observed anything yet. Malformed vendor data becomes a
/// structured error observation. Invalid binding requests return
/// [`super::ProviderUsageError::Validation`].
pub fn normalize_muse_usage(
    request: ProviderUsageNormalizeMuseUsageRequestWire,
) -> Result<ProviderUsageObservationWire> {
    if request.schema_version != PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION {
        return Err(super::validation(format!(
            "unsupported provider-usage muse usage schema version: {}",
            request.schema_version
        )));
    }
    let observation = observation_from_payload(&request);
    match validate_usage_observation(observation, request.now) {
        Ok(validated) => Ok(validated),
        // Vendor-derived values (an implausible stamp, a period that would
        // start before the epoch) can fail observation validation. That is
        // malformed vendor data, not a bad request: the fallback carries no
        // vendor-derived values, so it only fails when the request itself is
        // invalid, and that error still surfaces.
        Err(_) => validate_usage_observation(
            error_observation(&request, MALFORMED_DIAGNOSTIC),
            request.now,
        ),
    }
}

fn observation_from_payload(
    request: &ProviderUsageNormalizeMuseUsageRequestWire,
) -> ProviderUsageObservationWire {
    let Some(payload) = request.payload.as_object() else {
        return error_observation(request, MALFORMED_DIAGNOSTIC);
    };
    let Some(usage) = payload.get("usage") else {
        return absent_observation(request);
    };
    match parse_usage(usage) {
        Some(parsed) => ok_observation(request, parsed),
        None => error_observation(request, MALFORMED_DIAGNOSTIC),
    }
}

fn ok_observation(
    request: &ProviderUsageNormalizeMuseUsageRequestWire,
    usage: MuseUsage,
) -> ProviderUsageObservationWire {
    let windows = vec![
        window_observation(SESSION_KEY, SESSION_LABEL, &usage.session, &usage),
        window_observation(WEEKLY_KEY, WEEKLY_LABEL, &usage.weekly, &usage),
    ];
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
        retry_after_seconds: None,
        diagnostic: None,
        completeness: UsageCompleteness::Complete,
        authoritative_empty: false,
        account_mode: Some("subscription".to_string()),
        // The vendor's `tier` is an opaque account id, not a plan name, so it
        // is never persisted.
        plan: None,
        windows,
    }
}

fn window_observation(
    key: &str,
    label: &str,
    window: &MuseWindow,
    usage: &MuseUsage,
) -> UsageWindowObservationWire {
    UsageWindowObservationWire {
        key: key.to_string(),
        label: label.to_string(),
        used_percent: window.used_percent,
        resets_at: Some(window.resets_at),
        duration_seconds: window.duration_seconds,
        period_start: window
            .duration_seconds
            .map(|duration| window.resets_at - duration),
        // `Account`, not `Product`: only `Account` is unconditionally an
        // all-model scope for every provider.
        applicability: UsageApplicabilityWire::Account,
        observed_at: usage.observed_at,
        source: UsageSource::Probe,
        vendor_state: UsageVendorState::Allowed,
    }
}

/// A host that has observed nothing reports no `usage` member. That is an
/// empty inventory, never an error and never `0 %`.
fn absent_observation(
    request: &ProviderUsageNormalizeMuseUsageRequestWire,
) -> ProviderUsageObservationWire {
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
        retry_after_seconds: None,
        diagnostic: Some(NOT_YET_OBSERVED_DIAGNOSTIC.to_string()),
        completeness: UsageCompleteness::Complete,
        authoritative_empty: true,
        account_mode: None,
        plan: None,
        windows: Vec::new(),
    }
}

fn error_observation(
    request: &ProviderUsageNormalizeMuseUsageRequestWire,
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
        retry_after_seconds: None,
        diagnostic: Some(diagnostic.to_string()),
        completeness: UsageCompleteness::Partial,
        authoritative_empty: false,
        account_mode: None,
        plan: None,
        windows: Vec::new(),
    }
}

/// Parse a `SubscriptionUsage`. Every member the vendor schema requires must
/// be present with the right type: there is no partial payload.
fn parse_usage(value: &Value) -> Option<MuseUsage> {
    let usage = value.as_object()?;
    // Required by the vendor schema, checked for drift, and never read.
    usage.get("tier")?.as_str()?;
    let observed_at = epoch_millis(usage.get("observedAtMs")?)?;
    let session = parse_window(usage.get("window")?, true)?;
    let weekly = parse_window(usage.get("weekly")?, false)?;
    Some(MuseUsage {
        observed_at,
        session,
        weekly,
    })
}

/// Parse one usage block. Only the 5-hour `window` carries a duration; the
/// weekly block deliberately omits it, and a duration is never fabricated.
fn parse_window(value: &Value, has_duration: bool) -> Option<MuseWindow> {
    let block: &Map<String, Value> = value.as_object()?;
    // Never clamped: over-quota values above 100 are valid vendor data.
    let used_percent = finite_nonnegative_number(block.get("usedPercent")?)?;
    let resets_at = epoch_millis(block.get("resetsAtMs")?)?;
    let duration_seconds = if has_duration {
        let minutes = finite_positive_number(block.get("windowDurationMins")?)?;
        Some(minutes * 60.0)
    } else {
        None
    };
    Some(MuseWindow {
        used_percent,
        resets_at,
        duration_seconds,
    })
}

fn epoch_millis(value: &Value) -> Option<f64> {
    finite_positive_number(value).map(|millis| millis / 1000.0)
}

fn finite_nonnegative_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0),
        _ => None,
    }
}

fn finite_positive_number(value: &Value) -> Option<f64> {
    finite_nonnegative_number(value).filter(|value| *value > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // A few seconds after the live capture's `observedAtMs`.
    const NOW: f64 = 1_789_921_260.0;
    const STARTED_AT: f64 = NOW - 3.0;

    fn request(payload: Value) -> ProviderUsageNormalizeMuseUsageRequestWire {
        ProviderUsageNormalizeMuseUsageRequestWire {
            schema_version: 1,
            payload,
            provider: "muse".to_string(),
            context_id: "probe".to_string(),
            account_generation: 1,
            request_started_at: STARTED_AT,
            now: NOW,
        }
    }

    /// The payload `usage/read` returned from a live `muse serve` host
    /// running Muse Code 1.3.0 (1.3.0-R3401.1).
    fn live_payload() -> Value {
        json!({
            "usage": {
                "observedAtMs": 1_789_921_255_705_u64,
                "tier": "27681631238169137",
                "weekly": {
                    "usedPercent": 0,
                    "resetsAtMs": 1_789_948_800_000_u64,
                },
                "window": {
                    "usedPercent": 0,
                    "windowDurationMins": 300,
                    "resetsAtMs": 1_789_935_797_000_u64,
                },
            }
        })
    }

    fn payload_with(mutate: impl FnOnce(&mut Value)) -> Value {
        let mut payload = live_payload();
        mutate(&mut payload["usage"]);
        payload
    }

    fn assert_malformed(payload: Value) {
        let observation = normalize_muse_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Error);
        assert_eq!(
            observation.reason_code,
            Some(UsageReasonCode::MalformedPayload)
        );
        assert_eq!(
            observation.diagnostic.as_deref(),
            Some(MALFORMED_DIAGNOSTIC)
        );
        assert_eq!(observation.completeness, UsageCompleteness::Partial);
        assert!(!observation.authoritative_empty);
        assert!(observation.windows.is_empty());
    }

    #[test]
    fn live_payload_yields_session_and_weekly_windows() {
        let observation =
            normalize_muse_usage(request(live_payload())).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert_eq!(observation.reason_code, None);
        assert_eq!(observation.diagnostic, None);
        assert!(!observation.authoritative_empty);
        assert_eq!(observation.account_mode.as_deref(), Some("subscription"));
        assert_eq!(observation.plan, None);
        assert_eq!(observation.provider, "muse");
        assert_eq!(observation.ordering_token, STARTED_AT);
        assert_eq!(observation.received_at, NOW);
        assert_eq!(observation.windows.len(), 2);

        let session = &observation.windows[0];
        assert_eq!(session.key, "session");
        assert_eq!(session.label, "Muse 5-hour session");
        assert_eq!(session.used_percent, 0.0);
        assert_eq!(session.resets_at, Some(1_789_935_797.0));
        assert_eq!(session.duration_seconds, Some(18_000.0));
        assert_eq!(session.period_start, Some(1_789_935_797.0 - 18_000.0));
        assert_eq!(session.applicability, UsageApplicabilityWire::Account);
        assert_eq!(session.observed_at, 1_789_921_255.705);
        assert_eq!(session.source, UsageSource::Probe);
        assert_eq!(session.vendor_state, UsageVendorState::Allowed);

        let weekly = &observation.windows[1];
        assert_eq!(weekly.key, "weekly");
        assert_eq!(weekly.label, "Muse weekly all models");
        assert_eq!(weekly.used_percent, 0.0);
        assert_eq!(weekly.resets_at, Some(1_789_948_800.0));
        assert_eq!(weekly.duration_seconds, None);
        assert_eq!(weekly.period_start, None);
        assert_eq!(weekly.applicability, UsageApplicabilityWire::Account);
        assert_eq!(weekly.observed_at, 1_789_921_255.705);
        assert_eq!(weekly.source, UsageSource::Probe);
        assert_eq!(weekly.vendor_state, UsageVendorState::Allowed);
    }

    #[test]
    fn tier_is_never_persisted() {
        let observation =
            normalize_muse_usage(request(live_payload())).unwrap();
        let encoded = serde_json::to_string(&observation).unwrap();
        assert!(!encoded.contains("27681631238169137"));
    }

    #[test]
    fn used_percent_above_100_is_not_clamped() {
        let payload = payload_with(|usage| {
            usage["window"]["usedPercent"] = json!(137);
            usage["weekly"]["usedPercent"] = json!(250);
        });
        let observation = normalize_muse_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.windows[0].used_percent, 137.0);
        assert_eq!(observation.windows[1].used_percent, 250.0);
    }

    #[test]
    fn fractional_percent_is_preserved() {
        let payload = payload_with(|usage| {
            usage["window"]["usedPercent"] = json!(12.5);
        });
        let observation = normalize_muse_usage(request(payload)).unwrap();
        assert_eq!(observation.windows[0].used_percent, 12.5);
    }

    #[test]
    fn absent_usage_member_is_authoritative_empty_not_an_error() {
        let observation = normalize_muse_usage(request(json!({}))).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert!(observation.authoritative_empty);
        assert_eq!(observation.reason_code, None);
        assert_eq!(
            observation.diagnostic.as_deref(),
            Some(NOT_YET_OBSERVED_DIAGNOSTIC)
        );
        assert!(observation.windows.is_empty());
        assert_eq!(observation.plan, None);
    }

    #[test]
    fn unrelated_members_do_not_hide_absence() {
        let observation =
            normalize_muse_usage(request(json!({"other": 1}))).unwrap();
        assert!(observation.authoritative_empty);
        assert!(observation.windows.is_empty());
    }

    #[test]
    fn malformed_usage_member_is_a_structured_error() {
        for usage in [json!(null), json!(7), json!("x"), json!([]), json!(true)]
        {
            assert_malformed(json!({ "usage": usage }));
        }
    }

    #[test]
    fn non_object_payload_is_a_structured_error() {
        for payload in [json!(null), json!([]), json!("x"), json!(1)] {
            assert_malformed(payload);
        }
    }

    #[test]
    fn missing_required_members_are_malformed() {
        for path in [
            &["observedAtMs"][..],
            &["tier"],
            &["window"],
            &["weekly"],
            &["window", "usedPercent"],
            &["window", "windowDurationMins"],
            &["window", "resetsAtMs"],
            &["weekly", "usedPercent"],
            &["weekly", "resetsAtMs"],
        ] {
            let payload = payload_with(|usage| {
                let (leaf, parents) = path.split_last().unwrap();
                let mut node = usage;
                for parent in parents {
                    node = &mut node[*parent];
                }
                node.as_object_mut().unwrap().remove(*leaf);
            });
            assert_malformed(payload);
        }
    }

    #[test]
    fn wrong_typed_members_are_malformed() {
        for (path, bad) in [
            ("observedAtMs", json!("1789921255705")),
            ("tier", json!(27681631238169137_u64)),
            ("window", json!([])),
            ("weekly", json!(null)),
        ] {
            let payload = payload_with(|usage| usage[path] = bad);
            assert_malformed(payload);
        }
        for (block, member, bad) in [
            ("window", "usedPercent", json!("0")),
            ("window", "windowDurationMins", json!(null)),
            ("window", "resetsAtMs", json!(true)),
            ("weekly", "usedPercent", json!(false)),
            ("weekly", "resetsAtMs", json!("1789948800000")),
        ] {
            let payload = payload_with(|usage| usage[block][member] = bad);
            assert_malformed(payload);
        }
    }

    #[test]
    fn out_of_range_members_are_malformed() {
        for (block, member, bad) in [
            ("window", "usedPercent", json!(-1)),
            ("window", "windowDurationMins", json!(0)),
            ("window", "windowDurationMins", json!(-300)),
            ("window", "resetsAtMs", json!(0)),
            ("weekly", "usedPercent", json!(-1)),
            ("weekly", "resetsAtMs", json!(-5)),
        ] {
            let payload = payload_with(|usage| usage[block][member] = bad);
            assert_malformed(payload);
        }
        assert_malformed(payload_with(|usage| {
            usage["observedAtMs"] = json!(0)
        }));
    }

    #[test]
    fn vendor_values_that_fail_validation_are_malformed_not_binding_errors() {
        // A reset stamp so early that the derived period would start before
        // the epoch.
        assert_malformed(payload_with(|usage| {
            usage["window"]["resetsAtMs"] = json!(1_000);
        }));
        // A host stamp implausibly far ahead of the injected clock.
        assert_malformed(payload_with(|usage| {
            usage["observedAtMs"] = json!(1_889_921_255_705_u64);
        }));
    }

    #[test]
    fn unsupported_schema_version_is_binding_error() {
        let mut request = request(live_payload());
        request.schema_version = 2;
        let error = normalize_muse_usage(request).unwrap_err();
        assert!(error.to_string().contains("schema version: 2"));
    }

    #[test]
    fn invalid_request_is_a_binding_error_even_for_malformed_payload() {
        let mut bad_clock = request(json!({ "usage": 7 }));
        bad_clock.now = -1.0;
        assert!(normalize_muse_usage(bad_clock).is_err());

        let mut bad_provider = request(live_payload());
        bad_provider.provider = " muse".to_string();
        assert!(normalize_muse_usage(bad_provider).is_err());
    }

    #[test]
    fn request_rejects_unknown_fields() {
        let mut encoded =
            serde_json::to_value(request(live_payload())).unwrap();
        encoded["extra"] = json!(1);
        assert!(serde_json::from_value::<
            ProviderUsageNormalizeMuseUsageRequestWire,
        >(encoded)
        .is_err());
    }
}
