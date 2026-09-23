//! Antigravity (`agy`) subscription-usage normalization.
//!
//! Probe transport stays in Python. This module owns the decisions about the
//! `agy -p /usage --output-format json` envelope: which windows it yields,
//! how their percentages and periods derive, and what the hardening guards
//! mean.
//!
//! The vendor reports model groups (Gemini, Claude/GPT), each with a weekly
//! and a 5-hour bucket. Buckets are honestly `model_family`-scoped:
//! `gemini-*` buckets apply to the `gemini` family and `3p-*` buckets to the
//! `3p` family. Any other bucket id degrades to `Unknown` applicability.
//!
//! Hardening guards:
//!
//! - Turn-ran guard: a non-zero `num_turns`, a non-empty `conversation_id`,
//!   or a `command.name` other than `usage` means `/usage` was treated as a
//!   prompt, so the observation is `error` / `vendor_drift`.
//! - Omitted zero: an exhausted bucket may arrive with no
//!   `remaining_fraction` (the vendor struct tags are `omitempty`). Such a
//!   bucket is cross-checked against the payload's `response` TSV row: `0%`
//!   means exhausted, a non-percent status means the window is omitted, and
//!   anything else marks the observation `Partial`.
//! - Logged-out envelope: an `ERROR` status whose error text mentions
//!   authentication is `unauthenticated` / `logged_out`.

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

use super::{
    validate_usage_observation, ProviderUsageObservationWire, Result,
    UsageApplicabilityWire, UsageCollectionOutcome, UsageCompleteness,
    UsageReasonCode, UsageSource, UsageVendorState, UsageWindowObservationWire,
    PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
};

const MALFORMED_DIAGNOSTIC: &str = "agy_usage_malformed";
const EMPTY_DIAGNOSTIC: &str = "agy_usage_empty";
const TURN_RAN_DIAGNOSTIC: &str = "agy_usage_turn_ran";
const UNEXPECTED_COMMAND_DIAGNOSTIC: &str = "agy_usage_unexpected_command";
const LOGGED_OUT_DIAGNOSTIC: &str = "agy_usage_logged_out";
const PROBE_FAILED_PREFIX: &str = "agy_usage_probe_failed";
const DISABLED_BUCKET_DIAGNOSTIC: &str = "agy_usage_bucket_disabled";
const TSV_UNMATCHED_DIAGNOSTIC: &str = "agy_usage_tsv_unmatched";

const WEEK_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
const FIVE_HOUR_SECONDS: f64 = 5.0 * 60.0 * 60.0;
const MAX_ERROR_TEXT_CHARS: usize = 160;

/// Versioned request for [`normalize_agy_usage`].
///
/// `payload` is the whole `--output-format json` stdout object, and
/// `model_ids` is the caller provider's known model catalog (bare ids) used
/// to scope `model_family` windows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUsageNormalizeAgyUsageRequestWire {
    pub schema_version: u32,
    pub payload: Value,
    pub model_ids: Vec<String>,
    pub provider: String,
    pub context_id: String,
    pub account_generation: u64,
    pub request_started_at: f64,
    pub now: f64,
}

/// Normalize a decoded agy `/usage` envelope into a validated observation.
///
/// Malformed vendor data becomes a structured error observation. Invalid
/// binding requests return [`super::ProviderUsageError::Validation`].
pub fn normalize_agy_usage(
    request: ProviderUsageNormalizeAgyUsageRequestWire,
) -> Result<ProviderUsageObservationWire> {
    if request.schema_version != PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION {
        return Err(super::validation(format!(
            "unsupported provider-usage agy usage schema version: {}",
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
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
) -> ProviderUsageObservationWire {
    let Some(payload) = request.payload.as_object() else {
        return error_observation(request, MALFORMED_DIAGNOSTIC);
    };
    match payload.get("status").and_then(Value::as_str) {
        Some("SUCCESS") => {}
        Some(_) => return status_observation(request, payload),
        None => return error_observation(request, MALFORMED_DIAGNOSTIC),
    }
    if command_name(payload) != Some("usage") {
        return drift_observation(request, UNEXPECTED_COMMAND_DIAGNOSTIC);
    }
    match turn_guard(payload) {
        Some(Guard::Drift(diagnostic)) => {
            return drift_observation(request, diagnostic);
        }
        Some(Guard::Malformed) => {
            return error_observation(request, MALFORMED_DIAGNOSTIC);
        }
        None => {}
    }
    let Some(groups) = payload
        .get("command")
        .and_then(Value::as_object)
        .and_then(|command| command.get("data"))
        .and_then(Value::as_object)
        .and_then(|data| data.get("groups"))
        .and_then(Value::as_array)
    else {
        return error_observation(request, MALFORMED_DIAGNOSTIC);
    };
    if groups.is_empty() {
        return empty_observation(request, EMPTY_DIAGNOSTIC);
    }
    buckets_observation(request, payload, groups)
}

fn command_name(payload: &Map<String, Value>) -> Option<&str> {
    payload.get("command")?.as_object()?.get("name")?.as_str()
}

enum Guard {
    Drift(&'static str),
    Malformed,
}

/// Post-hoc backstop behind the collector's version gate: any sign that
/// `/usage` ran as a real model turn is `vendor_drift`, while a misshapen
/// envelope member is malformed vendor data.
fn turn_guard(payload: &Map<String, Value>) -> Option<Guard> {
    match payload.get("num_turns") {
        None | Some(Value::Null) => {}
        Some(Value::Number(turns)) => {
            if !matches!(turns.as_f64(), Some(0.0)) {
                return Some(Guard::Drift(TURN_RAN_DIAGNOSTIC));
            }
        }
        Some(_) => return Some(Guard::Malformed),
    }
    match payload.get("conversation_id") {
        None | Some(Value::Null) => {}
        Some(Value::String(id)) => {
            if !id.is_empty() {
                return Some(Guard::Drift(TURN_RAN_DIAGNOSTIC));
            }
        }
        Some(_) => return Some(Guard::Malformed),
    }
    None
}

/// A non-`SUCCESS` envelope. Authentication failures are the logged-out
/// shape; anything else is a bounded probe failure.
fn status_observation(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    payload: &Map<String, Value>,
) -> ProviderUsageObservationWire {
    let error_text = payload
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if error_text.to_ascii_lowercase().contains("authenticat") {
        return envelope(
            request,
            UsageCollectionOutcome::Unauthenticated,
            Some(UsageReasonCode::LoggedOut),
            Some(LOGGED_OUT_DIAGNOSTIC.to_string()),
            UsageCompleteness::Complete,
            false,
            None,
        );
    }
    let detail = if error_text.is_empty() {
        "unknown".to_string()
    } else {
        truncate_chars(error_text, MAX_ERROR_TEXT_CHARS)
    };
    envelope(
        request,
        UsageCollectionOutcome::Error,
        Some(UsageReasonCode::ProbeFailed),
        Some(format!("{PROBE_FAILED_PREFIX}:{detail}")),
        UsageCompleteness::Partial,
        false,
        None,
    )
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_string()
    } else {
        text.chars().take(max_chars).collect()
    }
}

enum BucketNote {
    Disabled,
    Unmatched,
}

struct BucketOutcome {
    window: Option<UsageWindowObservationWire>,
    note: Option<(BucketNote, String)>,
}

fn buckets_observation(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    payload: &Map<String, Value>,
    groups: &[Value],
) -> ProviderUsageObservationWire {
    let tsv = tsv_rows(payload);
    let mut windows = Vec::new();
    let mut disabled: Vec<String> = Vec::new();
    let mut unmatched: Vec<String> = Vec::new();
    let mut saw_bucket = false;
    for group in groups {
        let Some(group_map) = group.as_object() else {
            return error_observation(request, MALFORMED_DIAGNOSTIC);
        };
        let Some(group_name) = group_map.get("name").and_then(Value::as_str)
        else {
            return error_observation(request, MALFORMED_DIAGNOSTIC);
        };
        let Some(buckets) = group_map.get("buckets").and_then(Value::as_array)
        else {
            return error_observation(request, MALFORMED_DIAGNOSTIC);
        };
        for bucket in buckets {
            saw_bucket = true;
            match bucket_window(request, group_name, bucket, &tsv) {
                Ok(outcome) => {
                    if let Some(window) = outcome.window {
                        windows.push(window);
                    }
                    match outcome.note {
                        Some((BucketNote::Disabled, key)) => {
                            disabled.push(key);
                        }
                        Some((BucketNote::Unmatched, key)) => {
                            unmatched.push(key);
                        }
                        None => {}
                    }
                }
                Err(()) => {
                    return error_observation(request, MALFORMED_DIAGNOSTIC);
                }
            }
        }
    }
    if windows.is_empty() {
        // Buckets that cannot be read are never silent zeros. Unmatched
        // rows have no honest inventory to report, so they stay an error;
        // uniformly disabled buckets are an empty inventory.
        if !unmatched.is_empty() || !saw_bucket {
            return error_observation(request, MALFORMED_DIAGNOSTIC);
        }
        let diagnostic = if disabled.is_empty() {
            EMPTY_DIAGNOSTIC.to_string()
        } else {
            format!("{DISABLED_BUCKET_DIAGNOSTIC}:{}", disabled.join(","))
        };
        return empty_observation(request, &diagnostic);
    }
    let mut notes = Vec::new();
    if !disabled.is_empty() {
        notes.push(format!(
            "{DISABLED_BUCKET_DIAGNOSTIC}:{}",
            disabled.join(",")
        ));
    }
    if !unmatched.is_empty() {
        notes.push(format!(
            "{TSV_UNMATCHED_DIAGNOSTIC}:{}",
            unmatched.join(",")
        ));
    }
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
        diagnostic: if notes.is_empty() {
            None
        } else {
            Some(notes.join("; "))
        },
        completeness: if unmatched.is_empty() {
            UsageCompleteness::Complete
        } else {
            UsageCompleteness::Partial
        },
        authoritative_empty: false,
        account_mode: Some("subscription".to_string()),
        // The vendor's tier banner is an opaque account string, not a plan
        // name, so it is never persisted.
        plan: None,
        windows,
    }
}

fn bucket_window(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    group_name: &str,
    bucket: &Value,
    tsv: &[(String, String, String)],
) -> std::result::Result<BucketOutcome, ()> {
    let bucket_map = bucket.as_object().ok_or(())?;
    let bucket_id = bucket_map.get("id").and_then(Value::as_str).ok_or(())?;
    let bucket_name =
        bucket_map.get("name").and_then(Value::as_str).ok_or(())?;
    if bucket_id.is_empty() || bucket_name.is_empty() {
        return Err(());
    }
    let duration_seconds =
        duration_from_window(bucket_map.get("window").and_then(Value::as_str));
    let resets_at = match bucket_map.get("reset_time") {
        None | Some(Value::Null) => None,
        Some(Value::String(raw)) => Some(parse_reset_time(raw).ok_or(())?),
        Some(_) => return Err(()),
    };
    let used_percent = match bucket_map.get("remaining_fraction") {
        None | Some(Value::Null) => {
            // An exhausted bucket may omit its fraction entirely. Never
            // skip it: cross-check the payload's `response` TSV row.
            match omitted_used_percent(group_name, bucket_name, tsv) {
                Omitted::Exhausted => 100.0,
                Omitted::Disabled => {
                    return Ok(BucketOutcome {
                        window: None,
                        note: Some((
                            BucketNote::Disabled,
                            bucket_id.to_string(),
                        )),
                    });
                }
                Omitted::Unmatched => {
                    return Ok(BucketOutcome {
                        window: None,
                        note: Some((
                            BucketNote::Unmatched,
                            bucket_id.to_string(),
                        )),
                    });
                }
            }
        }
        Some(Value::Number(number)) => {
            let remaining = number.as_f64().ok_or(())?;
            if !remaining.is_finite() || !(0.0..=1.0).contains(&remaining) {
                return Err(());
            }
            ((1.0 - remaining) * 100.0).clamp(0.0, 100.0)
        }
        Some(_) => return Err(()),
    };
    Ok(BucketOutcome {
        window: Some(UsageWindowObservationWire {
            key: bucket_id.to_string(),
            label: format!("{group_name} {bucket_name}"),
            used_percent,
            resets_at,
            duration_seconds,
            period_start: match (resets_at, duration_seconds) {
                (Some(resets), Some(duration)) => Some(resets - duration),
                _ => None,
            },
            applicability: applicability(
                bucket_id,
                group_name,
                &request.model_ids,
            ),
            // The envelope carries no vendor timestamp, so the collection
            // time is the observation time.
            observed_at: request.now,
            source: UsageSource::Probe,
            vendor_state: UsageVendorState::Allowed,
        }),
        note: None,
    })
}

fn applicability(
    bucket_id: &str,
    group_name: &str,
    model_ids: &[String],
) -> UsageApplicabilityWire {
    if bucket_id.starts_with("gemini-") {
        UsageApplicabilityWire::ModelFamily {
            family: "gemini".to_string(),
            model_ids: scoped_model_ids(model_ids, true),
        }
    } else if bucket_id.starts_with("3p-") {
        UsageApplicabilityWire::ModelFamily {
            family: "3p".to_string(),
            model_ids: scoped_model_ids(model_ids, false),
        }
    } else {
        // Enterprise and business accounts likely use other group names;
        // they degrade to `Unknown`, never fail.
        UsageApplicabilityWire::Unknown {
            vendor_label: Some(group_name.to_string()),
            vendor_id: Some(bucket_id.to_string()),
        }
    }
}

fn scoped_model_ids(model_ids: &[String], gemini: bool) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut scoped = Vec::new();
    for id in model_ids {
        if (id.starts_with("gemini-") == gemini) && seen.insert(id.clone()) {
            scoped.push(id.clone());
        }
    }
    scoped
}

/// Derive `duration_seconds` from the bucket's `window` token. Anything
/// unrecognized (or absent) yields `None` rather than an invented span.
fn duration_from_window(token: Option<&str>) -> Option<f64> {
    let raw = token?.trim().to_ascii_lowercase();
    if raw == "weekly" {
        return Some(WEEK_SECONDS);
    }
    if raw == "5h" {
        return Some(FIVE_HOUR_SECONDS);
    }
    if let Some(hours) = raw
        .strip_suffix('h')
        .and_then(|hours| hours.parse::<f64>().ok())
    {
        if hours.is_finite() && hours > 0.0 {
            return Some(hours * 3600.0);
        }
        return None;
    }
    if let Some(days) = raw
        .strip_suffix('d')
        .and_then(|days| days.parse::<f64>().ok())
    {
        if days.is_finite() && days > 0.0 {
            return Some(days * 86400.0);
        }
        return None;
    }
    None
}

enum Omitted {
    Exhausted,
    Disabled,
    Unmatched,
}

fn omitted_used_percent(
    group_name: &str,
    bucket_name: &str,
    tsv: &[(String, String, String)],
) -> Omitted {
    let Some((_, _, status)) = tsv.iter().find(|(group, bucket, _)| {
        group == group_name && bucket == bucket_name
    }) else {
        return Omitted::Unmatched;
    };
    match tsv_status_kind(status) {
        TsvStatus::Zero => Omitted::Exhausted,
        TsvStatus::Percent => Omitted::Unmatched,
        TsvStatus::Other => Omitted::Disabled,
    }
}

enum TsvStatus {
    Zero,
    Percent,
    Other,
}

fn tsv_status_kind(status: &str) -> TsvStatus {
    let Some(numeric) = status.trim().strip_suffix('%') else {
        return TsvStatus::Other;
    };
    match numeric.trim().parse::<f64>() {
        Ok(value) if value.is_finite() && value == 0.0 => TsvStatus::Zero,
        Ok(value) if value.is_finite() => TsvStatus::Percent,
        _ => TsvStatus::Other,
    }
}

/// Parse the payload's `response` TSV rows into
/// `(group, bucket, status)` triples.
fn tsv_rows(payload: &Map<String, Value>) -> Vec<(String, String, String)> {
    let Some(text) = payload.get("response").and_then(Value::as_str) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            let mut columns = line.split('\t');
            Some((
                columns.next()?.to_string(),
                columns.next()?.to_string(),
                columns.next()?.to_string(),
            ))
        })
        .collect()
}

fn parse_reset_time(raw: &str) -> Option<f64> {
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

fn envelope(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    outcome: UsageCollectionOutcome,
    reason_code: Option<UsageReasonCode>,
    diagnostic: Option<String>,
    completeness: UsageCompleteness,
    authoritative_empty: bool,
    account_mode: Option<&str>,
) -> ProviderUsageObservationWire {
    ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: request.provider.clone(),
        context_id: request.context_id.clone(),
        account_generation: request.account_generation,
        ordering_token: request.request_started_at,
        received_at: request.now,
        source: UsageSource::Probe,
        outcome,
        reason_code,
        retry_after_seconds: None,
        diagnostic,
        completeness,
        authoritative_empty,
        account_mode: account_mode.map(str::to_string),
        plan: None,
        windows: Vec::new(),
    }
}

fn error_observation(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    diagnostic: &str,
) -> ProviderUsageObservationWire {
    envelope(
        request,
        UsageCollectionOutcome::Error,
        Some(UsageReasonCode::MalformedPayload),
        Some(diagnostic.to_string()),
        UsageCompleteness::Partial,
        false,
        None,
    )
}

fn drift_observation(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    diagnostic: &str,
) -> ProviderUsageObservationWire {
    envelope(
        request,
        UsageCollectionOutcome::Error,
        Some(UsageReasonCode::VendorDrift),
        Some(diagnostic.to_string()),
        UsageCompleteness::Partial,
        false,
        None,
    )
}

/// An authoritative empty inventory: never `0 %` and never an error.
fn empty_observation(
    request: &ProviderUsageNormalizeAgyUsageRequestWire,
    diagnostic: &str,
) -> ProviderUsageObservationWire {
    envelope(
        request,
        UsageCollectionOutcome::Ok,
        None,
        Some(diagnostic.to_string()),
        UsageCompleteness::Complete,
        true,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: f64 = 1_800_000_000.0;
    const STARTED_AT: f64 = NOW - 3.0;

    fn request(payload: Value) -> ProviderUsageNormalizeAgyUsageRequestWire {
        ProviderUsageNormalizeAgyUsageRequestWire {
            schema_version: 1,
            payload,
            model_ids: vec![
                "gemini-3-flash".to_string(),
                "gemini-3-pro".to_string(),
                "claude-opus-4-6".to_string(),
                "gpt-oss-120b".to_string(),
            ],
            provider: "agy".to_string(),
            context_id: "probe".to_string(),
            account_generation: 1,
            request_started_at: STARTED_AT,
            now: NOW,
        }
    }

    /// The verified live capture: `agy -p /usage --output-format json`
    /// against `agy 1.2.7`, exit 0 in ~3.5 s with `num_turns: 0` and every
    /// `usage` token count 0. (The `command.data.description` banner below
    /// stands in for the full vendor string, which the normalizer ignores.)
    fn live_payload() -> Value {
        json!({
            "conversation_id": "",
            "status": "SUCCESS",
            "response": "Gemini Models\tWeekly Limit Remaining\t96%\t2026-09-27T20:14:34Z\nGemini Models\tFive Hour Limit Remaining\t87%\t2026-09-21T23:31:12Z\nClaude and GPT models\tWeekly Limit Remaining\t100%\t2026-09-28T19:16:26Z\nClaude and GPT models\tFive Hour Limit Remaining\t100%\t2026-09-22T00:16:26Z\n",
            "duration_seconds": 0,
            "num_turns": 0,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "thinking_tokens": 0,
                "cache_read_tokens": 0,
                "total_tokens": 0,
            },
            "command": {
                "name": "usage",
                "data": {
                    "description": "Within each group, models share a weekly limit and a 5-hour limit. Gemini Models includes Gemini Flash and Gemini Pro. Claude and GPT models includes Claude Opus, Claude Sonnet, and GPT-OSS.",
                    "groups": [
                        {
                            "name": "Gemini Models",
                            "description": "Models within this group: Gemini Flash, Gemini Pro",
                            "buckets": [
                                {
                                    "id": "gemini-weekly",
                                    "name": "Weekly Limit Remaining",
                                    "description": "You have used some of your weekly limit, it will fully refresh in 6 days.",
                                    "window": "weekly",
                                    "remaining_fraction": 0.9612414240837097,
                                    "reset_time": "2026-09-27T20:14:34Z",
                                },
                                {
                                    "id": "gemini-5h",
                                    "name": "Five Hour Limit Remaining",
                                    "description": "You have used some of your 5-hour limit, it will fully refresh in 4 hours, 14 minutes.",
                                    "window": "5h",
                                    "remaining_fraction": 0.8664969801902771,
                                    "reset_time": "2026-09-21T23:31:12Z",
                                },
                            ],
                        },
                        {
                            "name": "Claude and GPT models",
                            "description": "Models within this group: Claude Opus, Claude Sonnet, GPT-OSS",
                            "buckets": [
                                {
                                    "id": "3p-weekly",
                                    "name": "Weekly Limit Remaining",
                                    "window": "weekly",
                                    "remaining_fraction": 1,
                                    "reset_time": "2026-09-28T19:16:26Z",
                                },
                                {
                                    "id": "3p-5h",
                                    "name": "Five Hour Limit Remaining",
                                    "window": "5h",
                                    "remaining_fraction": 1,
                                    "reset_time": "2026-09-22T00:16:26Z",
                                },
                            ],
                        },
                    ],
                },
            },
        })
    }

    fn bucket_mut(
        payload: &mut Value,
        group: usize,
        bucket: usize,
    ) -> &mut Value {
        &mut payload["command"]["data"]["groups"][group]["buckets"][bucket]
    }

    fn window_by_key<'a>(
        observation: &'a ProviderUsageObservationWire,
        key: &str,
    ) -> &'a UsageWindowObservationWire {
        observation
            .windows
            .iter()
            .find(|window| window.key == key)
            .unwrap_or_else(|| panic!("missing window {key}"))
    }

    fn assert_malformed(payload: Value) {
        let observation = normalize_agy_usage(request(payload)).unwrap();
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
        assert_eq!(observation.plan, None);
    }

    fn assert_drift(payload: Value, diagnostic: &str) {
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Error);
        assert_eq!(observation.reason_code, Some(UsageReasonCode::VendorDrift));
        assert_eq!(observation.diagnostic.as_deref(), Some(diagnostic));
        assert_eq!(observation.completeness, UsageCompleteness::Partial);
        assert!(observation.windows.is_empty());
    }

    #[test]
    fn live_payload_yields_four_family_scoped_windows() {
        let observation = normalize_agy_usage(request(live_payload())).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert_eq!(observation.reason_code, None);
        assert_eq!(observation.diagnostic, None);
        assert!(!observation.authoritative_empty);
        assert_eq!(observation.account_mode.as_deref(), Some("subscription"));
        assert_eq!(observation.plan, None);
        assert_eq!(observation.provider, "agy");
        assert_eq!(observation.ordering_token, STARTED_AT);
        assert_eq!(observation.received_at, NOW);
        assert_eq!(observation.windows.len(), 4);

        let weekly = window_by_key(&observation, "gemini-weekly");
        assert_eq!(weekly.label, "Gemini Models Weekly Limit Remaining");
        assert!((weekly.used_percent - 3.875_857_591_629_03).abs() < 1e-9);
        assert_eq!(weekly.resets_at, Some(1_790_540_074.0));
        assert_eq!(weekly.duration_seconds, Some(604_800.0));
        assert_eq!(weekly.period_start, Some(1_790_540_074.0 - 604_800.0));
        assert_eq!(
            weekly.applicability,
            UsageApplicabilityWire::ModelFamily {
                family: "gemini".to_string(),
                model_ids: vec![
                    "gemini-3-flash".to_string(),
                    "gemini-3-pro".to_string(),
                ],
            }
        );
        assert_eq!(weekly.observed_at, NOW);
        assert_eq!(weekly.source, UsageSource::Probe);
        assert_eq!(weekly.vendor_state, UsageVendorState::Allowed);

        let five_hour = window_by_key(&observation, "gemini-5h");
        assert_eq!(five_hour.label, "Gemini Models Five Hour Limit Remaining");
        assert!((five_hour.used_percent - 13.350_301_980_972_29).abs() < 1e-9);
        assert_eq!(five_hour.resets_at, Some(1_790_033_472.0));
        assert_eq!(five_hour.duration_seconds, Some(18_000.0));
        assert_eq!(five_hour.period_start, Some(1_790_033_472.0 - 18_000.0));
        assert_eq!(
            five_hour.applicability,
            UsageApplicabilityWire::ModelFamily {
                family: "gemini".to_string(),
                model_ids: vec![
                    "gemini-3-flash".to_string(),
                    "gemini-3-pro".to_string(),
                ],
            }
        );

        let third_weekly = window_by_key(&observation, "3p-weekly");
        assert_eq!(
            third_weekly.label,
            "Claude and GPT models Weekly Limit Remaining"
        );
        assert_eq!(third_weekly.used_percent, 0.0);
        assert_eq!(third_weekly.resets_at, Some(1_790_622_986.0));
        assert_eq!(third_weekly.duration_seconds, Some(604_800.0));
        assert_eq!(
            third_weekly.period_start,
            Some(1_790_622_986.0 - 604_800.0)
        );
        assert_eq!(
            third_weekly.applicability,
            UsageApplicabilityWire::ModelFamily {
                family: "3p".to_string(),
                model_ids: vec![
                    "claude-opus-4-6".to_string(),
                    "gpt-oss-120b".to_string(),
                ],
            }
        );

        let third_five_hour = window_by_key(&observation, "3p-5h");
        assert_eq!(third_five_hour.used_percent, 0.0);
        assert_eq!(third_five_hour.resets_at, Some(1_790_036_186.0));
        assert_eq!(third_five_hour.duration_seconds, Some(18_000.0));
        assert_eq!(
            third_five_hour.period_start,
            Some(1_790_036_186.0 - 18_000.0)
        );
    }

    #[test]
    fn vendor_banner_text_is_never_persisted() {
        let observation = normalize_agy_usage(request(live_payload())).unwrap();
        let encoded = serde_json::to_string(&observation).unwrap();
        assert!(!encoded.contains("Models within this group"));
        assert!(!encoded.contains("Within each group"));
    }

    #[test]
    fn integer_fractions_are_accepted() {
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 1)["remaining_fraction"] = json!(1);
        bucket_mut(&mut payload, 1, 0)["remaining_fraction"] = json!(0);
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(window_by_key(&observation, "gemini-5h").used_percent, 0.0);
        assert_eq!(
            window_by_key(&observation, "3p-weekly").used_percent,
            100.0
        );
    }

    #[test]
    fn omitted_fraction_with_zero_tsv_row_is_exhausted() {
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 1)
            .as_object_mut()
            .unwrap()
            .remove("remaining_fraction");
        payload["response"] = json!("Gemini Models\tWeekly Limit Remaining\t96%\t2026-09-27T20:14:34Z\nGemini Models\tFive Hour Limit Remaining\t0%\t2026-09-21T23:31:12Z\nClaude and GPT models\tWeekly Limit Remaining\t100%\t2026-09-28T19:16:26Z\nClaude and GPT models\tFive Hour Limit Remaining\t100%\t2026-09-22T00:16:26Z\n");
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert_eq!(observation.windows.len(), 4);
        assert_eq!(
            window_by_key(&observation, "gemini-5h").used_percent,
            100.0
        );
    }

    #[test]
    fn omitted_fraction_with_disabled_tsv_row_omits_window() {
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 1)
            .as_object_mut()
            .unwrap()
            .remove("remaining_fraction");
        payload["response"] = json!("Gemini Models\tWeekly Limit Remaining\t96%\t2026-09-27T20:14:34Z\nGemini Models\tFive Hour Limit Remaining\tDisabled\t2026-09-21T23:31:12Z\nClaude and GPT models\tWeekly Limit Remaining\t100%\t2026-09-28T19:16:26Z\nClaude and GPT models\tFive Hour Limit Remaining\t100%\t2026-09-22T00:16:26Z\n");
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert_eq!(observation.windows.len(), 3);
        assert!(observation
            .windows
            .iter()
            .all(|window| window.key != "gemini-5h"));
        let diagnostic = observation.diagnostic.as_deref().unwrap();
        assert!(diagnostic.contains(DISABLED_BUCKET_DIAGNOSTIC));
        assert!(diagnostic.contains("gemini-5h"));
    }

    #[test]
    fn omitted_fraction_without_tsv_match_is_partial() {
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 1)
            .as_object_mut()
            .unwrap()
            .remove("remaining_fraction");
        // A healthy percent in the TSV contradicts the missing fraction, so
        // the row cannot corroborate exhaustion.
        payload["response"] = json!("Gemini Models\tWeekly Limit Remaining\t96%\t2026-09-27T20:14:34Z\n");
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Partial);
        assert_eq!(observation.windows.len(), 3);
        let diagnostic = observation.diagnostic.as_deref().unwrap();
        assert!(diagnostic.contains(TSV_UNMATCHED_DIAGNOSTIC));

        // With no TSV at all there is likewise nothing to corroborate.
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 1)
            .as_object_mut()
            .unwrap()
            .remove("remaining_fraction");
        payload.as_object_mut().unwrap().remove("response");
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Partial);
        assert_eq!(observation.windows.len(), 3);
    }

    #[test]
    fn logged_out_envelope_is_unauthenticated() {
        let observation = normalize_agy_usage(request(json!({
            "status": "ERROR",
            "error": "authentication failed or timed out",
            "duration_seconds": 0,
            "num_turns": 0,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "thinking_tokens": 0,
                "cache_read_tokens": 0,
                "total_tokens": 0,
            },
        })))
        .unwrap();
        assert_eq!(
            observation.outcome,
            UsageCollectionOutcome::Unauthenticated
        );
        assert_eq!(observation.reason_code, Some(UsageReasonCode::LoggedOut));
        assert_eq!(
            observation.diagnostic.as_deref(),
            Some(LOGGED_OUT_DIAGNOSTIC)
        );
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert!(!observation.authoritative_empty);
        assert!(observation.windows.is_empty());
        assert_eq!(observation.plan, None);
    }

    #[test]
    fn non_auth_error_envelope_is_probe_failed() {
        let observation = normalize_agy_usage(request(json!({
            "status": "ERROR",
            "error": "usage service unavailable",
            "duration_seconds": 0,
            "num_turns": 0,
        })))
        .unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Error);
        assert_eq!(observation.reason_code, Some(UsageReasonCode::ProbeFailed));
        let diagnostic = observation.diagnostic.as_deref().unwrap();
        assert!(diagnostic.contains(PROBE_FAILED_PREFIX));
        assert!(diagnostic.contains("usage service unavailable"));
        assert_eq!(observation.completeness, UsageCompleteness::Partial);
        assert!(observation.windows.is_empty());
    }

    #[test]
    fn non_object_payload_is_malformed() {
        for payload in [json!(null), json!([]), json!("x"), json!(1)] {
            assert_malformed(payload);
        }
    }

    #[test]
    fn missing_or_unexpected_status_is_malformed() {
        let mut payload = live_payload();
        payload.as_object_mut().unwrap().remove("status");
        assert_malformed(payload);

        let mut payload = live_payload();
        payload["status"] = json!(0);
        assert_malformed(payload);
    }

    #[test]
    fn turn_ran_envelopes_are_vendor_drift() {
        let mut payload = live_payload();
        payload["num_turns"] = json!(1);
        assert_drift(payload, TURN_RAN_DIAGNOSTIC);

        let mut payload = live_payload();
        payload["conversation_id"] = json!("abc123");
        assert_drift(payload, TURN_RAN_DIAGNOSTIC);

        let mut payload = live_payload();
        payload["command"]["name"] = json!("prompt");
        assert_drift(payload, UNEXPECTED_COMMAND_DIAGNOSTIC);

        let mut payload = live_payload();
        payload.as_object_mut().unwrap().remove("command");
        assert_drift(payload, UNEXPECTED_COMMAND_DIAGNOSTIC);
    }

    #[test]
    fn misshapen_turn_members_are_malformed() {
        let mut payload = live_payload();
        payload["num_turns"] = json!("none");
        assert_malformed(payload);

        let mut payload = live_payload();
        payload["conversation_id"] = json!(7);
        assert_malformed(payload);
    }

    #[test]
    fn unknown_bucket_id_degrades_to_unknown_scope() {
        let mut payload = live_payload();
        payload["command"]["data"]["groups"][0]["buckets"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id": "business-daily",
                "name": "Daily Limit Remaining",
                "window": "24h",
                "remaining_fraction": 0.5,
                "reset_time": "2026-09-22T12:00:00Z",
            }));
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.windows.len(), 5);
        let window = window_by_key(&observation, "business-daily");
        assert_eq!(window.used_percent, 50.0);
        assert_eq!(window.duration_seconds, Some(86_400.0));
        assert_eq!(
            window.applicability,
            UsageApplicabilityWire::Unknown {
                vendor_label: Some("Gemini Models".to_string()),
                vendor_id: Some("business-daily".to_string()),
            }
        );
    }

    #[test]
    fn unstarted_bucket_rolling_reset_passes_through() {
        // Untouched buckets carry no `description` and a rolling reset of
        // request time plus the window.
        let mut payload = live_payload();
        let bucket = bucket_mut(&mut payload, 1, 1);
        bucket.as_object_mut().unwrap().remove("description");
        bucket["reset_time"] = json!("2027-01-15T13:00:00Z");
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        let window = window_by_key(&observation, "3p-5h");
        assert_eq!(window.resets_at, Some(1_800_018_000.0));
        assert_eq!(window.duration_seconds, Some(18_000.0));
        assert_eq!(window.period_start, Some(1_800_000_000.0));
    }

    #[test]
    fn generic_window_tokens_derive_durations() {
        for (token, duration) in [
            ("24h", Some(86_400.0)),
            ("30d", Some(2_592_000.0)),
            ("monthly", None),
            ("bogus", None),
        ] {
            let mut payload = live_payload();
            bucket_mut(&mut payload, 0, 0)["window"] = json!(token);
            let observation = normalize_agy_usage(request(payload)).unwrap();
            assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
            assert_eq!(
                window_by_key(&observation, "gemini-weekly").duration_seconds,
                duration,
                "token {token}"
            );
        }

        // An absent window token yields no duration rather than a failure.
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 0)
            .as_object_mut()
            .unwrap()
            .remove("window");
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(
            window_by_key(&observation, "gemini-weekly").duration_seconds,
            None
        );
    }

    #[test]
    fn empty_groups_is_authoritative_empty() {
        let mut payload = live_payload();
        payload["command"]["data"]["groups"] = json!([]);
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert_eq!(observation.outcome, UsageCollectionOutcome::Ok);
        assert_eq!(observation.completeness, UsageCompleteness::Complete);
        assert!(observation.authoritative_empty);
        assert_eq!(observation.diagnostic.as_deref(), Some(EMPTY_DIAGNOSTIC));
        assert!(observation.windows.is_empty());
        assert_eq!(observation.plan, None);
    }

    #[test]
    fn out_of_range_fractions_are_malformed() {
        for bad in [json!(1.5), json!(-0.1), json!("0.5"), json!(true)] {
            let mut payload = live_payload();
            bucket_mut(&mut payload, 0, 0)["remaining_fraction"] = bad;
            assert_malformed(payload);
        }
    }

    #[test]
    fn bad_bucket_members_are_malformed() {
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 0)["reset_time"] = json!("tomorrow");
        assert_malformed(payload);

        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 0)["reset_time"] = json!(7);
        assert_malformed(payload);

        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 0)
            .as_object_mut()
            .unwrap()
            .remove("id");
        assert_malformed(payload);

        let mut payload = live_payload();
        payload["command"]["data"]["groups"] = json!([{"name": "x"}]);
        assert_malformed(payload);

        let mut payload = live_payload();
        payload["command"]["data"]["groups"] = json!([]);
        // Control: empty groups is authoritative empty, not malformed.
        let observation = normalize_agy_usage(request(payload)).unwrap();
        assert!(observation.authoritative_empty);

        // A vendor reset so early that the derived period would start
        // before the epoch fails validation, not the binding.
        let mut payload = live_payload();
        bucket_mut(&mut payload, 0, 0)["reset_time"] =
            json!("1970-01-01T00:01:40Z");
        assert_malformed(payload);
    }

    #[test]
    fn unsupported_schema_version_is_binding_error() {
        let mut bad = request(live_payload());
        bad.schema_version = 2;
        let error = normalize_agy_usage(bad).unwrap_err();
        assert!(error.to_string().contains("schema version: 2"));
    }

    #[test]
    fn invalid_request_is_a_binding_error_even_for_malformed_payload() {
        let mut bad_clock = request(json!({"status": 7}));
        bad_clock.now = -1.0;
        assert!(normalize_agy_usage(bad_clock).is_err());

        let mut bad_provider = request(live_payload());
        bad_provider.provider = " agy".to_string();
        assert!(normalize_agy_usage(bad_provider).is_err());
    }

    #[test]
    fn request_rejects_unknown_fields() {
        let mut encoded =
            serde_json::to_value(request(live_payload())).unwrap();
        encoded["extra"] = json!(1);
        let error: std::result::Result<
            ProviderUsageNormalizeAgyUsageRequestWire,
            _,
        > = serde_json::from_value(encoded);
        assert!(error.is_err());
    }
}
