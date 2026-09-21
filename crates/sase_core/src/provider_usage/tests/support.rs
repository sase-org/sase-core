//! Shared fixtures and builders for provider-usage tests.

use super::super::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;

pub(super) const NOW: f64 = 1_800_000_000.0;
pub(super) const CADENCE: f64 = 300.0;
pub(super) const WEEK_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
pub(super) const MONTH_SECONDS: f64 = 30.0 * 24.0 * 60.0 * 60.0;

#[derive(Debug, Deserialize)]
pub(super) struct FixtureCase {
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
    observations: Vec<ProviderUsageObservationWire>,
    expected: Value,
    #[serde(default)]
    model_summaries: BTreeMap<String, Value>,
}

pub(super) fn load_fixture(name: &str) -> FixtureCase {
    let raw = match name {
        "used_percent" => include_str!("../fixtures/used_percent.json"),
        "shared_and_model" => include_str!("../fixtures/shared_and_model.json"),
        "unknown_scope" => include_str!("../fixtures/unknown_scope.json"),
        "mixed_ages" => include_str!("../fixtures/mixed_ages.json"),
        "reset_expiry" => include_str!("../fixtures/reset_expiry.json"),
        "deterministic_ties" => {
            include_str!("../fixtures/deterministic_ties.json")
        }
        other => panic!("unknown fixture {other}"),
    };
    serde_json::from_str(raw).unwrap_or_else(|error| {
        panic!("fixture {name} is invalid JSON: {error}")
    })
}

pub(super) fn normalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), normalize(&map[&key]));
            }
            Value::Object(out)
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(normalize).collect())
        }
        other => other.clone(),
    }
}

pub(super) fn assert_json_eq(actual: &Value, expected: &Value) {
    assert_eq!(
        normalize(actual),
        normalize(expected),
        "actual={actual}\nexpected={expected}"
    );
}

pub(super) fn project_fixture(
    name: &str,
) -> (UsagePublicSnapshotWire, FixtureCase) {
    let fixture = load_fixture(name);
    let snapshot = project_usage_snapshot(
        &fixture.observations,
        fixture.now,
        fixture.cadence_seconds,
        fixture.warn_percent,
        fixture.critical_percent,
    )
    .unwrap_or_else(|error| panic!("{name}: {error}"));
    (snapshot, fixture)
}

pub(super) fn assert_fixture(name: &str) {
    let (snapshot, fixture) = project_fixture(name);
    let actual = serde_json::to_value(&snapshot).unwrap();
    assert_json_eq(&actual, &fixture.expected);
    for (model_id, expected) in fixture.model_summaries {
        let summary = summarize_usage_windows(
            &snapshot.providers[0].windows,
            Some(&model_id),
        );
        let actual = serde_json::to_value(&summary).unwrap();
        assert_json_eq(&actual, &expected);
    }
}

pub(super) fn valid_window() -> UsageWindowObservationWire {
    UsageWindowObservationWire {
        key: "week".to_string(),
        label: "Weekly".to_string(),
        used_percent: 10.0,
        resets_at: Some(NOW + 3_600.0),
        duration_seconds: None,
        period_start: None,
        applicability: UsageApplicabilityWire::Account,
        observed_at: NOW - 10.0,
        source: UsageSource::Probe,
        vendor_state: UsageVendorState::Allowed,
    }
}

pub(super) fn named_window(
    key: &str,
    used_percent: f64,
    observed_at: f64,
) -> UsageWindowObservationWire {
    UsageWindowObservationWire {
        key: key.to_string(),
        label: format!("{key} allowance"),
        used_percent,
        resets_at: Some(NOW + 3_600.0),
        duration_seconds: None,
        period_start: None,
        applicability: UsageApplicabilityWire::Account,
        observed_at,
        source: UsageSource::Probe,
        vendor_state: UsageVendorState::Allowed,
    }
}

pub(super) fn claude_fable_window(
    key: &str,
    used_percent: f64,
    observed_at: f64,
) -> UsageWindowObservationWire {
    UsageWindowObservationWire {
        key: key.to_string(),
        label: format!("Claude {key} allowance"),
        used_percent,
        resets_at: Some(NOW + WEEK_SECONDS),
        duration_seconds: Some(WEEK_SECONDS),
        period_start: Some(NOW),
        applicability: if key == "weekly:claude-fable-5" {
            UsageApplicabilityWire::Models {
                model_ids: vec!["claude-fable-5".to_string()],
            }
        } else {
            UsageApplicabilityWire::Unknown {
                vendor_label: Some("seven_day_overage_included".to_string()),
                vendor_id: Some("seven-day-overage-included".to_string()),
            }
        },
        observed_at,
        source: if key == "weekly:claude-fable-5" {
            UsageSource::Probe
        } else {
            UsageSource::StreamEvent
        },
        vendor_state: if key == "weekly:claude-fable-5" {
            UsageVendorState::Allowed
        } else {
            UsageVendorState::Unknown
        },
    }
}

pub(super) fn valid_observation() -> ProviderUsageObservationWire {
    ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: "alpha".to_string(),
        context_id: "ctx-alpha".to_string(),
        account_generation: 1,
        ordering_token: NOW - 10.0,
        received_at: NOW - 5.0,
        source: UsageSource::Probe,
        outcome: UsageCollectionOutcome::Ok,
        reason_code: None,
        diagnostic: None,
        completeness: UsageCompleteness::Complete,
        authoritative_empty: false,
        account_mode: Some("subscription".to_string()),
        plan: None,
        windows: vec![valid_window()],
    }
}

pub(super) fn claude_usage_observation(
    ordering_token: f64,
    completeness: UsageCompleteness,
    windows: Vec<UsageWindowObservationWire>,
) -> ProviderUsageObservationWire {
    ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: "claude".to_string(),
        context_id: "ctx-claude".to_string(),
        account_generation: 1,
        ordering_token,
        received_at: ordering_token + 1.0,
        source: UsageSource::Probe,
        outcome: UsageCollectionOutcome::Ok,
        reason_code: None,
        diagnostic: None,
        completeness,
        authoritative_empty: false,
        account_mode: Some("subscription".to_string()),
        plan: Some("Max".to_string()),
        windows,
    }
}

pub(super) fn usage_observation(
    provider: &str,
    context_id: &str,
    account_generation: u64,
    ordering_token: f64,
    completeness: UsageCompleteness,
    windows: Vec<UsageWindowObservationWire>,
) -> ProviderUsageObservationWire {
    ProviderUsageObservationWire {
        schema_version: PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION,
        provider: provider.to_string(),
        context_id: context_id.to_string(),
        account_generation,
        ordering_token,
        received_at: ordering_token + 1.0,
        source: UsageSource::Probe,
        outcome: UsageCollectionOutcome::Ok,
        reason_code: None,
        diagnostic: None,
        completeness,
        authoritative_empty: false,
        account_mode: Some("subscription".to_string()),
        plan: None,
        windows,
    }
}

pub(super) fn indicator_window(
    key: &str,
    used_percent: f64,
    resets_at: Option<f64>,
    duration_seconds: Option<f64>,
    applicability: UsageApplicabilityWire,
    observed_at: f64,
) -> UsageWindowObservationWire {
    UsageWindowObservationWire {
        key: key.to_string(),
        label: format!("{key} allowance"),
        used_percent,
        resets_at,
        duration_seconds,
        period_start: None,
        applicability,
        observed_at,
        source: UsageSource::Probe,
        vendor_state: UsageVendorState::Allowed,
    }
}

pub(super) fn indicator_snapshot(
    observations: Vec<ProviderUsageObservationWire>,
    now: f64,
) -> UsagePublicSnapshotWire {
    project_usage_snapshot(&observations, now, CADENCE, 75.0, 90.0).unwrap()
}

pub(super) fn indicator_projection(
    snapshot: UsagePublicSnapshotWire,
    indicator: Option<Value>,
    now: f64,
) -> UsageIndicatorProjectionWire {
    project_usage_indicator(UsageIndicatorProjectionRequestWire {
        schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        snapshot,
        indicator,
        eligible_providers: None,
        now,
        cadence_seconds: CADENCE,
        warn_percent: 75.0,
        critical_percent: 90.0,
    })
    .unwrap()
}

pub(super) fn muse_payload(session_used: u64, weekly_used: u64) -> Value {
    json!({
        "usage": {
            "observedAtMs": ((NOW - 10.0) * 1000.0) as u64,
            "tier": "27681631238169137",
            "weekly": {
                "usedPercent": weekly_used,
                "resetsAtMs": ((NOW + WEEK_SECONDS) * 1000.0) as u64,
            },
            "window": {
                "usedPercent": session_used,
                "windowDurationMins": 300,
                "resetsAtMs": ((NOW + 3.0 * 60.0 * 60.0) * 1000.0) as u64,
            },
        }
    })
}

pub(super) fn normalized_muse_observation(
    session_used: u64,
    weekly_used: u64,
) -> ProviderUsageObservationWire {
    normalize_muse_usage(ProviderUsageNormalizeMuseUsageRequestWire {
        schema_version: 1,
        payload: muse_payload(session_used, weekly_used),
        provider: "muse".to_string(),
        context_id: "probe".to_string(),
        account_generation: 1,
        request_started_at: NOW - 1.0,
        now: NOW,
    })
    .unwrap()
}

pub(super) fn agy_payload() -> Value {
    json!({
        "conversation_id": "",
        "status": "SUCCESS",
        "response": "Gemini Models\tWeekly Limit Remaining\t96%\t2027-01-21T08:00:00Z\nGemini Models\tFive Hour Limit Remaining\t87%\t2027-01-15T12:00:00Z\nClaude and GPT models\tWeekly Limit Remaining\t100%\t2027-01-22T08:00:00Z\nClaude and GPT models\tFive Hour Limit Remaining\t100%\t2027-01-15T13:00:00Z\n",
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
                "description": "groups share limits",
                "groups": [
                    {
                        "name": "Gemini Models",
                        "description": "Models within this group",
                        "buckets": [
                            {
                                "id": "gemini-weekly",
                                "name": "Weekly Limit Remaining",
                                "description": "weekly",
                                "window": "weekly",
                                "remaining_fraction": 0.96,
                                "reset_time": "2027-01-21T08:00:00Z",
                            },
                            {
                                "id": "gemini-5h",
                                "name": "Five Hour Limit Remaining",
                                "description": "5h",
                                "window": "5h",
                                "remaining_fraction": 0.87,
                                "reset_time": "2027-01-15T12:00:00Z",
                            },
                        ],
                    },
                    {
                        "name": "Claude and GPT models",
                        "buckets": [
                            {
                                "id": "3p-weekly",
                                "name": "Weekly Limit Remaining",
                                "window": "weekly",
                                "remaining_fraction": 1,
                                "reset_time": "2027-01-22T08:00:00Z",
                            },
                            {
                                "id": "3p-5h",
                                "name": "Five Hour Limit Remaining",
                                "window": "5h",
                                "remaining_fraction": 1,
                                "reset_time": "2027-01-15T13:00:00Z",
                            },
                        ],
                    },
                ],
            },
        },
    })
}

pub(super) fn normalized_agy_observation() -> ProviderUsageObservationWire {
    normalize_agy_usage(ProviderUsageNormalizeAgyUsageRequestWire {
        schema_version: 1,
        payload: agy_payload(),
        model_ids: vec![
            "gemini-3-flash".to_string(),
            "claude-opus-4-6".to_string(),
        ],
        provider: "agy".to_string(),
        context_id: "probe".to_string(),
        account_generation: 1,
        request_started_at: NOW - 1.0,
        now: NOW,
    })
    .unwrap()
}

pub(super) fn write_raw_claude_store(
    home: &std::path::Path,
    windows: Vec<(&str, UsageWindowObservationWire, f64, f64)>,
    tombstones: Value,
) {
    let last_attempt = claude_usage_observation(
        NOW - 20.0,
        UsageCompleteness::Complete,
        windows
            .iter()
            .map(|(_, window, _, _)| window.clone())
            .collect(),
    );
    let window_map = windows
        .into_iter()
        .map(|(key, window, ordering_token, received_at)| {
            (
                key.to_string(),
                json!({
                    "window": window,
                    "ordering_token": ordering_token,
                    "received_at": received_at,
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let raw = json!({
        "version": PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        "providers": {
            "claude": {
                "version": PROVIDER_USAGE_STORE_SCHEMA_VERSION,
                "provider": "claude",
                "context_id": "ctx-claude",
                "account_generation": 1,
                "last_attempt": last_attempt,
                "last_attempt_ordering_token": NOW - 20.0,
                "last_attempt_received_at": NOW - 19.0,
                "last_full_observation_at": NOW - 20.0,
                "last_full_ordering_token": NOW - 20.0,
                "windows": window_map,
                "tombstones": tombstones,
            }
        },
        "reservations": {},
        "schedules": {},
    });
    let path = provider_usage_state_path(home);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();
}

pub(super) fn loaded_claude_windows(
    home: &std::path::Path,
) -> Vec<UsagePublicWindowWire> {
    load_provider_usage_store(
        home,
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot
    .providers
    .into_iter()
    .next()
    .unwrap()
    .windows
}

pub(super) fn record_refresh_attempt_at(
    home: &std::path::Path,
    provider: &str,
    context_id: &str,
    account_generation: u64,
    outcome: &str,
    now: f64,
) -> ProviderUsageRefreshScheduleWire {
    record_provider_usage_refresh_attempt(
        home,
        ProviderUsageRefreshAttemptWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation,
            outcome: outcome.to_string(),
            retry_after_seconds: None,
            cadence_seconds: 300.0,
        },
        now,
    )
    .unwrap()
}
