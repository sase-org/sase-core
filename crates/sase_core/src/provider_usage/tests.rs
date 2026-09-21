use super::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

const NOW: f64 = 1_800_000_000.0;
const CADENCE: f64 = 300.0;
const WEEK_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
const MONTH_SECONDS: f64 = 30.0 * 24.0 * 60.0 * 60.0;

#[derive(Debug, Deserialize)]
struct FixtureCase {
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
    observations: Vec<ProviderUsageObservationWire>,
    expected: Value,
    #[serde(default)]
    model_summaries: BTreeMap<String, Value>,
}

fn load_fixture(name: &str) -> FixtureCase {
    let raw = match name {
        "used_percent" => include_str!("fixtures/used_percent.json"),
        "shared_and_model" => include_str!("fixtures/shared_and_model.json"),
        "unknown_scope" => include_str!("fixtures/unknown_scope.json"),
        "mixed_ages" => include_str!("fixtures/mixed_ages.json"),
        "reset_expiry" => include_str!("fixtures/reset_expiry.json"),
        "deterministic_ties" => {
            include_str!("fixtures/deterministic_ties.json")
        }
        other => panic!("unknown fixture {other}"),
    };
    serde_json::from_str(raw).unwrap_or_else(|error| {
        panic!("fixture {name} is invalid JSON: {error}")
    })
}

fn normalize(value: &Value) -> Value {
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

fn assert_json_eq(actual: &Value, expected: &Value) {
    assert_eq!(
        normalize(actual),
        normalize(expected),
        "actual={actual}\nexpected={expected}"
    );
}

fn project_fixture(name: &str) -> (UsagePublicSnapshotWire, FixtureCase) {
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

fn assert_fixture(name: &str) {
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

fn valid_window() -> UsageWindowObservationWire {
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

fn named_window(
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

fn claude_fable_window(
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

fn valid_observation() -> ProviderUsageObservationWire {
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

fn claude_usage_observation(
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

fn usage_observation(
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

fn indicator_window(
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

fn indicator_snapshot(
    observations: Vec<ProviderUsageObservationWire>,
    now: f64,
) -> UsagePublicSnapshotWire {
    project_usage_snapshot(&observations, now, CADENCE, 75.0, 90.0).unwrap()
}

fn indicator_projection(
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

#[test]
fn used_percent_fixture_preserves_raw_precision_and_overage() {
    assert_fixture("used_percent");
    assert_eq!(format_remaining_text(0.0).unwrap(), "100% left");
    assert_eq!(format_remaining_text(0.4).unwrap(), "99% left");
    assert_eq!(format_remaining_text(99.4).unwrap(), "<1% left");
    assert_eq!(format_remaining_text(100.0).unwrap(), "0% left");
    assert_eq!(format_remaining_text(112.5).unwrap(), "0% left");
    assert_eq!(remaining_percent(112.5).unwrap(), 0.0);
    assert_eq!(exceeded_by_percent(112.5).unwrap(), Some(12.5));
    assert_eq!(exceeded_by_percent(100.0).unwrap(), None);
}

#[test]
fn shared_plus_model_specific_limits_do_not_replace_each_other() {
    assert_fixture("shared_and_model");
}

#[test]
fn unknown_scope_makes_model_conclusion_partial() {
    assert_fixture("unknown_scope");
}

#[test]
fn mixed_ages_use_only_fresh_windows_in_the_numeric_summary() {
    assert_fixture("mixed_ages");
}

#[test]
fn reset_expiry_drops_numeric_summary_without_synthesizing_100() {
    assert_fixture("reset_expiry");
    let (snapshot, _) = project_fixture("reset_expiry");
    let window = &snapshot.providers[0].windows[0];
    assert!(window.reset_passed);
    assert_eq!(window.remaining_percent, 60.0);
    assert!(snapshot.providers[0].summary.is_none());
}

#[test]
fn deterministic_ties_break_on_provider_and_window_key() {
    assert_fixture("deterministic_ties");
}

#[test]
fn indicator_defaults_select_weekly_all_and_low_independent_windows() {
    let claude_product = UsageApplicabilityWire::Product {
        product: "claude".to_string(),
        model_ids: vec![],
    };
    let claude_fable = UsageApplicabilityWire::Product {
        product: "claude".to_string(),
        model_ids: vec!["claude-fable-5".to_string()],
    };
    let snapshot = indicator_snapshot(
        vec![usage_observation(
            "claude",
            "ctx-claude",
            1,
            NOW - 10.0,
            UsageCompleteness::Complete,
            vec![
                indicator_window(
                    "session",
                    82.0,
                    Some(NOW + 5.0 * 60.0 * 60.0),
                    None,
                    claude_product.clone(),
                    NOW - 10.0,
                ),
                indicator_window(
                    "weekly",
                    20.0,
                    Some(NOW + WEEK_SECONDS),
                    None,
                    claude_product,
                    NOW - 10.0,
                ),
                indicator_window(
                    "weekly:claude-fable-5",
                    10.0,
                    Some(NOW + WEEK_SECONDS),
                    None,
                    claude_fable,
                    NOW - 10.0,
                ),
            ],
        )],
        NOW,
    );
    let projection = indicator_projection(snapshot, None, NOW);
    assert_eq!(projection.diagnostics, vec![]);
    assert_eq!(
        projection
            .entries
            .iter()
            .map(|entry| entry.window_key.as_str())
            .collect::<Vec<_>>(),
        vec!["weekly", "session"]
    );
    let weekly = projection
        .entries
        .iter()
        .find(|entry| entry.window_key == "weekly")
        .unwrap();
    assert!(weekly.weekly_all);
    assert_eq!(weekly.policy_source, UsageIndicatorPolicySource::WeeklyAll);
    assert_eq!(weekly.scope.kind, UsageIndicatorScopeKind::AllModels);
    assert_eq!(weekly.period.kind, UsageIndicatorPeriodKind::Weekly);

    let session = projection
        .entries
        .iter()
        .find(|entry| entry.window_key == "session")
        .unwrap();
    assert!(!session.weekly_all);
    assert_eq!(session.policy_source, UsageIndicatorPolicySource::Default);
    assert_eq!(session.period.kind, UsageIndicatorPeriodKind::Session);
}

#[test]
fn indicator_policy_precedence_uses_exact_then_provider_then_weekly_defaults() {
    let snapshot = indicator_snapshot(
        vec![usage_observation(
            "claude",
            "ctx-claude",
            1,
            NOW - 10.0,
            UsageCompleteness::Complete,
            vec![
                indicator_window(
                    "session",
                    40.0,
                    Some(NOW + 5.0 * 60.0 * 60.0),
                    None,
                    UsageApplicabilityWire::Product {
                        product: "claude".to_string(),
                        model_ids: vec![],
                    },
                    NOW - 10.0,
                ),
                indicator_window(
                    "weekly",
                    5.0,
                    Some(NOW + WEEK_SECONDS),
                    None,
                    UsageApplicabilityWire::Product {
                        product: "claude".to_string(),
                        model_ids: vec![],
                    },
                    NOW - 10.0,
                ),
            ],
        )],
        NOW,
    );
    let projection = indicator_projection(
        snapshot,
        Some(json!({
            "providers": {
                "claude": {
                    "default": "never",
                    "windows": {
                        "session": "always"
                    }
                }
            }
        })),
        NOW,
    );
    assert_eq!(projection.diagnostics, vec![]);
    assert_eq!(projection.entries.len(), 1);
    assert_eq!(projection.entries[0].window_key, "session");
    assert_eq!(
        projection.entries[0].policy_source,
        UsageIndicatorPolicySource::Window
    );
}

#[test]
fn indicator_thresholds_use_unrounded_remaining_percentage() {
    let snapshot = indicator_snapshot(
        vec![usage_observation(
            "alpha",
            "ctx-alpha",
            1,
            NOW - 10.0,
            UsageCompleteness::Complete,
            vec![
                indicator_window(
                    "below",
                    80.01,
                    Some(NOW + 3_600.0),
                    None,
                    UsageApplicabilityWire::Account,
                    NOW - 10.0,
                ),
                indicator_window(
                    "exact",
                    80.0,
                    Some(NOW + 3_600.0),
                    None,
                    UsageApplicabilityWire::Account,
                    NOW - 10.0,
                ),
                indicator_window(
                    "above",
                    79.99,
                    Some(NOW + 3_600.0),
                    None,
                    UsageApplicabilityWire::Account,
                    NOW - 10.0,
                ),
            ],
        )],
        NOW,
    );
    let projection = indicator_projection(snapshot, None, NOW);
    assert_eq!(
        projection
            .entries
            .iter()
            .map(|entry| (entry.window_key.as_str(), entry.remaining_percent))
            .collect::<Vec<_>>(),
        vec![("below", 19.989999999999995)]
    );
}

#[test]
fn indicator_invalid_policy_diagnostics_keep_inherited_policy() {
    let snapshot = indicator_snapshot(
        vec![usage_observation(
            "grok",
            "ctx-grok",
            1,
            NOW - 10.0,
            UsageCompleteness::Complete,
            vec![indicator_window(
                "included_weekly",
                1.0,
                Some(NOW + WEEK_SECONDS),
                None,
                UsageApplicabilityWire::Account,
                NOW - 10.0,
            )],
        )],
        NOW,
    );
    let projection = indicator_projection(
        snapshot,
        Some(json!({
            "default": true,
            "weekly_all": null,
            "providers": {
                "grok": {
                    "default": {"below_remaining_percent": -1},
                    "windows": {
                        "included_weekly": {"unexpected": 1}
                    }
                },
                "future-provider": {
                    "windows": {
                        "future-window": "never"
                    }
                }
            }
        })),
        NOW,
    );
    assert_eq!(
        projection
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "indicator.default",
            "indicator.weekly_all",
            "indicator.providers.grok.default.below_remaining_percent",
            "indicator.providers.grok.windows.included_weekly",
        ]
    );
    assert_eq!(projection.entries.len(), 1);
    assert_eq!(projection.entries[0].window_key, "included_weekly");
    assert_eq!(
        projection.entries[0].policy_source,
        UsageIndicatorPolicySource::WeeklyAll
    );
}

#[test]
fn indicator_classifies_supported_provider_window_shapes() {
    let snapshot = indicator_snapshot(
        vec![
            usage_observation(
                "codex",
                "ctx-codex",
                1,
                NOW - 10.0,
                UsageCompleteness::Complete,
                vec![
                    indicator_window(
                        "codex:secondary",
                        30.0,
                        Some(NOW + WEEK_SECONDS),
                        Some(WEEK_SECONDS),
                        UsageApplicabilityWire::Account,
                        NOW - 10.0,
                    ),
                    indicator_window(
                        "model-x:secondary",
                        30.0,
                        Some(NOW + WEEK_SECONDS),
                        Some(WEEK_SECONDS),
                        UsageApplicabilityWire::Unknown {
                            vendor_label: Some("model bucket".to_string()),
                            vendor_id: Some("model-x".to_string()),
                        },
                        NOW - 10.0,
                    ),
                ],
            ),
            usage_observation(
                "grok",
                "ctx-grok",
                1,
                NOW - 9.0,
                UsageCompleteness::Complete,
                vec![
                    indicator_window(
                        "included_weekly",
                        30.0,
                        Some(NOW + WEEK_SECONDS),
                        None,
                        UsageApplicabilityWire::Account,
                        NOW - 9.0,
                    ),
                    indicator_window(
                        "included_monthly",
                        85.0,
                        Some(NOW + MONTH_SECONDS),
                        None,
                        UsageApplicabilityWire::Account,
                        NOW - 9.0,
                    ),
                ],
            ),
            usage_observation(
                "plugin",
                "ctx-plugin",
                1,
                NOW - 8.0,
                UsageCompleteness::Complete,
                vec![indicator_window(
                    "team-week",
                    10.0,
                    Some(NOW + WEEK_SECONDS),
                    Some(WEEK_SECONDS),
                    UsageApplicabilityWire::Product {
                        product: "plugin-pro".to_string(),
                        model_ids: vec![],
                    },
                    NOW - 8.0,
                )],
            ),
        ],
        NOW,
    );
    let projection = indicator_projection(
        snapshot,
        Some(json!({
            "providers": {
                "plugin": {"default": "always"}
            }
        })),
        NOW,
    );
    let by_key = projection
        .entries
        .iter()
        .map(|entry| (entry.window_key.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    assert!(by_key["codex:secondary"].weekly_all);
    assert_eq!(
        by_key["codex:secondary"].period.kind,
        UsageIndicatorPeriodKind::Weekly
    );
    assert!(!by_key.contains_key("model-x:secondary"));
    assert!(by_key["included_weekly"].weekly_all);
    assert_eq!(
        by_key["included_monthly"].period.kind,
        UsageIndicatorPeriodKind::Monthly
    );
    assert!(!by_key["team-week"].weekly_all);
    assert_eq!(
        by_key["team-week"].scope.kind,
        UsageIndicatorScopeKind::Product
    );
}

fn muse_payload(session_used: u64, weekly_used: u64) -> Value {
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

fn normalized_muse_observation(
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

#[test]
fn indicator_selects_normalized_muse_weekly_by_default_and_not_session() {
    let snapshot =
        indicator_snapshot(vec![normalized_muse_observation(50, 5)], NOW);
    let projection = indicator_projection(snapshot, None, NOW);
    assert_eq!(projection.diagnostics, vec![]);
    assert_eq!(
        projection
            .entries
            .iter()
            .map(|entry| entry.window_key.as_str())
            .collect::<Vec<_>>(),
        vec!["weekly"]
    );
    let weekly = &projection.entries[0];
    assert!(weekly.weekly_all);
    assert_eq!(weekly.policy_source, UsageIndicatorPolicySource::WeeklyAll);
    assert_eq!(weekly.scope.kind, UsageIndicatorScopeKind::AllModels);
    assert_eq!(weekly.period.kind, UsageIndicatorPeriodKind::Weekly);
    assert_eq!(weekly.period.duration_seconds, None);
    assert_eq!(weekly.remaining_percent, 95.0);
}

#[test]
fn indicator_keeps_muse_session_independent_of_weekly_all() {
    let snapshot =
        indicator_snapshot(vec![normalized_muse_observation(90, 5)], NOW);
    let projection = indicator_projection(snapshot, None, NOW);
    let by_key = projection
        .entries
        .iter()
        .map(|entry| (entry.window_key.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    assert!(by_key["weekly"].weekly_all);
    let session = by_key["session"];
    assert!(!session.weekly_all);
    assert_eq!(session.policy_source, UsageIndicatorPolicySource::Default);
    assert_eq!(session.period.kind, UsageIndicatorPeriodKind::Duration);
    assert_eq!(session.period.duration_seconds, Some(18_000.0));

    let hidden = indicator_projection(
        indicator_snapshot(vec![normalized_muse_observation(90, 5)], NOW),
        Some(json!({
            "providers": {"muse": {"windows": {"session": "never"}}}
        })),
        NOW,
    );
    assert_eq!(
        hidden
            .entries
            .iter()
            .map(|entry| entry.window_key.as_str())
            .collect::<Vec<_>>(),
        vec!["weekly"]
    );
}

#[test]
fn indicator_muse_weekly_needs_muse_provider_and_account_scope() {
    let weekly_entry =
        |provider: &str, applicability: UsageApplicabilityWire| {
            let snapshot = indicator_snapshot(
                vec![usage_observation(
                    provider,
                    "ctx",
                    1,
                    NOW - 10.0,
                    UsageCompleteness::Complete,
                    vec![indicator_window(
                        "weekly",
                        5.0,
                        Some(NOW + WEEK_SECONDS),
                        None,
                        applicability,
                        NOW - 10.0,
                    )],
                )],
                NOW,
            );
            // `always` keeps a not-weekly-all window in the projection so its
            // classification can be read back.
            let projection = indicator_projection(
                snapshot,
                Some(json!({"default": "always"})),
                NOW,
            );
            assert_eq!(projection.entries.len(), 1);
            projection.entries[0].clone()
        };

    let muse = weekly_entry("muse", UsageApplicabilityWire::Account);
    assert!(muse.weekly_all);
    assert_eq!(muse.period.kind, UsageIndicatorPeriodKind::Weekly);

    let other_scopes = [
        UsageApplicabilityWire::Product {
            product: "muse".to_string(),
            model_ids: vec![],
        },
        UsageApplicabilityWire::Models {
            model_ids: vec!["muse-x".to_string()],
        },
        UsageApplicabilityWire::Unknown {
            vendor_label: Some("bucket".to_string()),
            vendor_id: None,
        },
    ];
    for applicability in other_scopes {
        let entry = weekly_entry("muse", applicability);
        assert!(!entry.weekly_all);
        assert_eq!(entry.period.kind, UsageIndicatorPeriodKind::Unknown);
    }

    // The allowlist arm is keyed on the provider: an unrelated provider's
    // duration-less `weekly` window is not thereby weekly.
    let other = weekly_entry("plugin", UsageApplicabilityWire::Account);
    assert!(!other.weekly_all);
    assert_eq!(other.period.kind, UsageIndicatorPeriodKind::Unknown);
}

fn agy_payload() -> Value {
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

fn normalized_agy_observation() -> ProviderUsageObservationWire {
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

#[test]
fn indicator_selects_agy_gemini_weekly_anchor_by_default() {
    let snapshot = indicator_snapshot(vec![normalized_agy_observation()], NOW);
    let projection = indicator_projection(snapshot, None, NOW);
    assert_eq!(projection.diagnostics, vec![]);
    // `gemini-weekly` is the only agy `weekly_all` window, so it is the
    // unlabeled anchor; the healthy sibling windows stay hidden.
    assert_eq!(
        projection
            .entries
            .iter()
            .map(|entry| entry.window_key.as_str())
            .collect::<Vec<_>>(),
        vec!["gemini-weekly"]
    );
    let anchor = &projection.entries[0];
    assert!(anchor.weekly_all);
    assert_eq!(anchor.policy_source, UsageIndicatorPolicySource::WeeklyAll);
    assert_eq!(anchor.scope.kind, UsageIndicatorScopeKind::ModelFamily);
    assert_eq!(anchor.scope.family.as_deref(), Some("gemini"));
    assert_eq!(anchor.period.kind, UsageIndicatorPeriodKind::Weekly);
}

#[test]
fn indicator_agy_3p_weekly_is_weekly_but_not_anchor() {
    let snapshot = indicator_snapshot(vec![normalized_agy_observation()], NOW);
    // `always` keeps the non-anchor windows in the projection so their
    // classification can be read back.
    let projection =
        indicator_projection(snapshot, Some(json!({"default": "always"})), NOW);
    let by_key = projection
        .entries
        .iter()
        .map(|entry| (entry.window_key.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    assert!(by_key["gemini-weekly"].weekly_all);
    let third_party = by_key["3p-weekly"];
    assert!(!third_party.weekly_all);
    assert_eq!(third_party.period.kind, UsageIndicatorPeriodKind::Weekly);
    assert_eq!(third_party.scope.kind, UsageIndicatorScopeKind::ModelFamily);
    assert!(!by_key["gemini-5h"].weekly_all);
    assert!(!by_key["3p-5h"].weekly_all);
}

#[test]
fn indicator_agy_arm_needs_agy_provider_and_gemini_family() {
    let anchor_entry =
        |provider: &str, applicability: UsageApplicabilityWire| {
            let snapshot = indicator_snapshot(
                vec![usage_observation(
                    provider,
                    "ctx",
                    1,
                    NOW - 10.0,
                    UsageCompleteness::Complete,
                    vec![indicator_window(
                        "gemini-weekly",
                        5.0,
                        Some(NOW + WEEK_SECONDS),
                        Some(WEEK_SECONDS),
                        applicability,
                        NOW - 10.0,
                    )],
                )],
                NOW,
            );
            // `always` keeps a not-weekly-all window in the projection so
            // its classification can be read back.
            let projection = indicator_projection(
                snapshot,
                Some(json!({"default": "always"})),
                NOW,
            );
            assert_eq!(projection.entries.len(), 1);
            projection.entries[0].clone()
        };

    let anchor = anchor_entry(
        "agy",
        UsageApplicabilityWire::ModelFamily {
            family: "gemini".to_string(),
            model_ids: vec!["gemini-3-flash".to_string()],
        },
    );
    assert!(anchor.weekly_all);
    // The arm never claims an all-model scope elsewhere.
    assert_eq!(anchor.scope.kind, UsageIndicatorScopeKind::ModelFamily);

    // Another provider's identical window is not the anchor.
    let other_provider = anchor_entry(
        "plugin",
        UsageApplicabilityWire::ModelFamily {
            family: "gemini".to_string(),
            model_ids: vec!["gemini-3-flash".to_string()],
        },
    );
    assert!(!other_provider.weekly_all);

    // Neither is a non-`gemini` family under agy itself.
    let other_family = anchor_entry(
        "agy",
        UsageApplicabilityWire::ModelFamily {
            family: "3p".to_string(),
            model_ids: vec!["claude-opus-4-6".to_string()],
        },
    );
    assert!(!other_family.weekly_all);
}

#[test]
fn indicator_muse_weekly_over_quota_stays_visible_unclamped() {
    let snapshot =
        indicator_snapshot(vec![normalized_muse_observation(0, 137)], NOW);
    let projection = indicator_projection(snapshot, None, NOW);
    let weekly = projection
        .entries
        .iter()
        .find(|entry| entry.window_key == "weekly")
        .unwrap();
    assert_eq!(weekly.used_percent, 137.0);
    assert_eq!(weekly.remaining_percent, 0.0);
}

#[test]
fn muse_absent_usage_projects_no_windows_and_no_zero_percent() {
    let observation =
        normalize_muse_usage(ProviderUsageNormalizeMuseUsageRequestWire {
            schema_version: 1,
            payload: json!({}),
            provider: "muse".to_string(),
            context_id: "probe".to_string(),
            account_generation: 1,
            request_started_at: NOW - 1.0,
            now: NOW,
        })
        .unwrap();
    let snapshot = indicator_snapshot(vec![observation], NOW);
    // Absence is a healthy empty inventory: no collector problem, no
    // attention, and no window to render as `0 %`.
    assert_eq!(snapshot.collection_health, UsageCollectionHealth::Ok);
    assert_eq!(snapshot.attention, None);
    assert!(snapshot.providers[0].windows.is_empty());
    let projection = indicator_projection(snapshot, None, NOW);
    assert!(projection.entries.is_empty());
}

#[test]
fn indicator_recomputes_freshness_and_reset_from_injected_clock() {
    let snapshot = indicator_snapshot(
        vec![usage_observation(
            "alpha",
            "ctx-alpha",
            1,
            NOW - 1_300.0,
            UsageCompleteness::Complete,
            vec![indicator_window(
                "low-expired",
                90.0,
                Some(NOW - 1.0),
                Some(WEEK_SECONDS),
                UsageApplicabilityWire::Account,
                NOW - 1_300.0,
            )],
        )],
        NOW - 1_300.0,
    );
    let projection = indicator_projection(snapshot, None, NOW);
    assert_eq!(projection.entries.len(), 1);
    let entry = &projection.entries[0];
    assert_eq!(entry.freshness, UsageFreshness::Unknown);
    assert_eq!(entry.reset_state, UsageIndicatorResetState::Passed);
    assert_eq!(entry.seconds_until_reset, Some(0.0));
    assert_eq!(entry.remaining_percent, 10.0);
}

#[test]
fn freshness_bands_use_two_and_four_cadences() {
    assert_eq!(
        classify_freshness(NOW, NOW, CADENCE).unwrap(),
        UsageFreshness::Fresh
    );
    assert_eq!(
        classify_freshness(NOW - 600.0, NOW, CADENCE).unwrap(),
        UsageFreshness::Fresh
    );
    assert_eq!(
        classify_freshness(NOW - 600.001, NOW, CADENCE).unwrap(),
        UsageFreshness::Stale
    );
    assert_eq!(
        classify_freshness(NOW - 1_200.0, NOW, CADENCE).unwrap(),
        UsageFreshness::Stale
    );
    assert_eq!(
        classify_freshness(NOW - 1_200.001, NOW, CADENCE).unwrap(),
        UsageFreshness::Unknown
    );
}

#[test]
fn reset_at_now_has_passed() {
    assert!(reset_has_passed(Some(NOW), NOW));
    assert!(!reset_has_passed(Some(NOW + 0.001), NOW));
    assert!(!reset_has_passed(None, NOW));
}

#[test]
fn window_applicability_is_declared_not_inferred() {
    let account = UsageApplicabilityWire::Account;
    let models = UsageApplicabilityWire::Models {
        model_ids: vec!["x-1".to_string()],
    };
    let family = UsageApplicabilityWire::ModelFamily {
        family: "x".to_string(),
        model_ids: vec!["x-1".to_string()],
    };
    let product_unmapped = UsageApplicabilityWire::Product {
        product: "chat".to_string(),
        model_ids: vec![],
    };
    let unknown = UsageApplicabilityWire::Unknown {
        vendor_label: Some("bucket".to_string()),
        vendor_id: Some("lim".to_string()),
    };
    assert_eq!(
        usage_window_applies(&account, None),
        UsageApplicabilityMatch::Applies
    );
    assert_eq!(
        usage_window_applies(&account, Some("y-1")),
        UsageApplicabilityMatch::Applies
    );
    assert_eq!(
        usage_window_applies(&models, Some("x-1")),
        UsageApplicabilityMatch::Applies
    );
    assert_eq!(
        usage_window_applies(&models, Some("y-1")),
        UsageApplicabilityMatch::DoesNotApply
    );
    assert_eq!(
        usage_window_applies(&family, Some("x-1")),
        UsageApplicabilityMatch::Applies
    );
    assert_eq!(
        usage_window_applies(&family, Some("y-1")),
        UsageApplicabilityMatch::DoesNotApply
    );
    assert_eq!(
        usage_window_applies(&product_unmapped, Some("x-1")),
        UsageApplicabilityMatch::Unknown
    );
    assert_eq!(
        usage_window_applies(&unknown, Some("x-1")),
        UsageApplicabilityMatch::Unknown
    );
}

#[test]
fn empty_snapshot_is_empty_health() {
    let snapshot =
        project_usage_snapshot(&[], NOW, CADENCE, 75.0, 90.0).unwrap();
    assert_eq!(snapshot.collection_health, UsageCollectionHealth::Empty);
    assert!(snapshot.providers.is_empty());
    assert!(snapshot.attention.is_none());
}

#[test]
fn not_applicable_clears_numeric_conclusions() {
    let mut observation = valid_observation();
    observation.outcome = UsageCollectionOutcome::NotApplicable;
    observation.reason_code = Some(UsageReasonCode::ApiMode);
    observation.windows.clear();
    let snapshot =
        project_usage_snapshot(&[observation], NOW, CADENCE, 75.0, 90.0)
            .unwrap();
    assert_eq!(
        snapshot.providers[0].collection_status,
        UsageCollectionOutcome::NotApplicable
    );
    assert!(snapshot.providers[0].windows.is_empty());
    assert!(snapshot.providers[0].summary.is_none());
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::None
    );
}

#[test]
fn unauthenticated_is_a_collection_problem() {
    let mut observation = valid_observation();
    observation.outcome = UsageCollectionOutcome::Unauthenticated;
    observation.reason_code = Some(UsageReasonCode::LoggedOut);
    observation.windows.clear();
    let snapshot =
        project_usage_snapshot(&[observation], NOW, CADENCE, 75.0, 90.0)
            .unwrap();
    assert_eq!(snapshot.collection_health, UsageCollectionHealth::Error);
    assert_eq!(
        snapshot.attention.as_ref().unwrap().kind,
        UsageAttentionKind::CollectionProblem
    );
}

#[test]
fn rejects_unknown_schema_negative_used_and_future_timestamps() {
    let mut observation = valid_observation();
    observation.schema_version = 2;
    let error = validate_usage_observation(observation, NOW).unwrap_err();
    assert!(error.to_string().contains("unsupported"));

    let mut observation = valid_observation();
    observation.windows[0].used_percent = -0.01;
    let error = validate_usage_observation(observation, NOW).unwrap_err();
    assert!(error.to_string().contains("used_percent"));

    let mut observation = valid_observation();
    observation.windows[0].observed_at = NOW + 120.0;
    let error = validate_usage_observation(observation, NOW).unwrap_err();
    assert!(error.to_string().contains("implausibly ahead"));
}

#[test]
fn rejects_duplicate_keys_empty_ok_and_impossible_periods() {
    let mut observation = valid_observation();
    observation.windows.push(valid_window());
    let error = validate_usage_observation(observation, NOW).unwrap_err();
    assert!(error.to_string().contains("duplicate window key"));

    let mut observation = valid_observation();
    observation.windows.clear();
    let error = validate_usage_observation(observation, NOW).unwrap_err();
    assert!(error.to_string().contains("empty complete inventory"));

    let mut observation = valid_observation();
    observation.windows[0].period_start = Some(NOW - 10.0);
    observation.windows[0].resets_at = Some(NOW + 10.0);
    observation.windows[0].duration_seconds = Some(999.0);
    let error = validate_usage_observation(observation, NOW).unwrap_err();
    assert!(error.to_string().contains("period does not match"));
}

#[test]
fn vendor_drift_reason_code_is_accepted_and_unknown_codes_still_reject() {
    let mut observation = valid_observation();
    observation.outcome = UsageCollectionOutcome::Error;
    observation.reason_code = Some(UsageReasonCode::VendorDrift);
    observation.completeness = UsageCompleteness::Partial;
    observation.windows.clear();
    assert_eq!(
        validate_usage_observation(observation, NOW)
            .unwrap()
            .reason_code,
        Some(UsageReasonCode::VendorDrift)
    );

    let value = json!({
        "schema_version": 1,
        "provider": "alpha",
        "context_id": "ctx",
        "account_generation": 1,
        "ordering_token": NOW - 1.0,
        "received_at": NOW - 1.0,
        "source": "probe",
        "outcome": "error",
        "reason_code": "vendor_changed",
        "diagnostic": null,
        "completeness": "partial",
        "account_mode": null,
        "plan": null,
        "windows": []
    });
    let parsed: std::result::Result<
        ProviderUsageObservationWire,
        serde_json::Error,
    > = serde_json::from_value(value);
    assert!(parsed.unwrap_err().to_string().contains("unknown variant"));
}

#[test]
fn claude_fable_alias_observation_canonicalizes_and_collapses() {
    let observation = claude_usage_observation(
        NOW - 10.0,
        UsageCompleteness::Partial,
        vec![
            named_window("session", 10.0, NOW - 10.0),
            named_window("weekly", 88.0, NOW - 10.0),
            claude_fable_window(
                "window:seven-day-overage-included",
                82.0,
                NOW - 10.0,
            ),
        ],
    );
    let validated = validate_usage_observation(observation, NOW).unwrap();
    assert_eq!(
        validated
            .windows
            .iter()
            .map(|window| window.key.as_str())
            .collect::<Vec<_>>(),
        vec!["session", "weekly", "weekly:claude-fable-5"]
    );
    let fable = validated
        .windows
        .iter()
        .find(|window| window.key == "weekly:claude-fable-5")
        .unwrap();
    assert_eq!(fable.label, "Claude weekly Fable");
    assert_eq!(fable.used_percent, 82.0);
    assert_eq!(fable.resets_at, Some(NOW + WEEK_SECONDS));
    assert_eq!(fable.source, UsageSource::StreamEvent);
    assert_eq!(fable.vendor_state, UsageVendorState::Unknown);
    assert_eq!(
        fable.applicability,
        UsageApplicabilityWire::Models {
            model_ids: vec!["claude-fable-5".to_string()]
        }
    );

    let alias_newer = validate_usage_observation(
        claude_usage_observation(
            NOW - 8.0,
            UsageCompleteness::Partial,
            vec![
                claude_fable_window("weekly:claude-fable-5", 81.0, NOW - 8.0),
                claude_fable_window(
                    "window:seven-day-overage-included",
                    83.0,
                    NOW - 7.0,
                ),
            ],
        ),
        NOW,
    )
    .unwrap();
    assert_eq!(alias_newer.windows.len(), 1);
    assert_eq!(alias_newer.windows[0].key, "weekly:claude-fable-5");
    assert_eq!(alias_newer.windows[0].used_percent, 83.0);

    let canonical_tie = validate_usage_observation(
        claude_usage_observation(
            NOW - 6.0,
            UsageCompleteness::Partial,
            vec![
                claude_fable_window(
                    "window:seven-day-overage-included",
                    84.0,
                    NOW - 6.0,
                ),
                claude_fable_window("weekly:claude-fable-5", 80.0, NOW - 6.0),
            ],
        ),
        NOW,
    )
    .unwrap();
    assert_eq!(canonical_tie.windows.len(), 1);
    assert_eq!(canonical_tie.windows[0].used_percent, 80.0);

    let other = usage_observation(
        "other",
        "ctx-other",
        1,
        NOW - 5.0,
        UsageCompleteness::Partial,
        vec![claude_fable_window(
            "window:seven-day-overage-included",
            84.0,
            NOW - 5.0,
        )],
    );
    let validated = validate_usage_observation(other, NOW).unwrap();
    assert_eq!(
        validated.windows[0].key,
        "window:seven-day-overage-included"
    );
}

#[test]
fn usage_store_merges_partial_updates_and_fences_tombstones() {
    let temp = tempdir().unwrap();
    let full = usage_observation(
        "claude",
        "ctx-1",
        1,
        NOW - 100.0,
        UsageCompleteness::Complete,
        vec![
            named_window("session", 20.0, NOW - 100.0),
            named_window("week", 30.0, NOW - 100.0),
        ],
    );
    assert_eq!(
        record_provider_usage_observation(temp.path(), full, NOW)
            .unwrap()
            .status,
        ProviderUsageStoreWriteStatus::Recorded
    );

    let partial = usage_observation(
        "claude",
        "ctx-1",
        1,
        NOW - 50.0,
        UsageCompleteness::Partial,
        vec![named_window("week", 95.0, NOW - 50.0)],
    );
    record_provider_usage_observation(temp.path(), partial, NOW).unwrap();

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    let provider = &snapshot.providers[0];
    assert_eq!(
        provider
            .windows
            .iter()
            .map(|window| window.key.as_str())
            .collect::<Vec<_>>(),
        vec!["session", "week"]
    );
    assert_eq!(
        provider.summary.as_ref().unwrap().limiting_window_keys,
        vec!["week"]
    );

    let newer_complete = usage_observation(
        "claude",
        "ctx-1",
        1,
        NOW - 20.0,
        UsageCompleteness::Complete,
        vec![named_window("session", 25.0, NOW - 20.0)],
    );
    record_provider_usage_observation(temp.path(), newer_complete, NOW)
        .unwrap();

    let stale_partial = usage_observation(
        "claude",
        "ctx-1",
        1,
        NOW - 30.0,
        UsageCompleteness::Partial,
        vec![named_window("week", 1.0, NOW - 30.0)],
    );
    let stale =
        record_provider_usage_observation(temp.path(), stale_partial, NOW)
            .unwrap();
    assert_eq!(stale.status, ProviderUsageStoreWriteStatus::Unchanged);
    assert!(!stale.accepted);

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(
        snapshot.providers[0]
            .windows
            .iter()
            .map(|window| window.key.as_str())
            .collect::<Vec<_>>(),
        vec!["session"]
    );
    assert_eq!(
        snapshot.providers[0].last_full_observation_at,
        Some(NOW - 20.0)
    );
}

#[test]
fn usage_store_unifies_claude_fable_probe_and_passive_windows() {
    let temp = tempdir().unwrap();
    let probe = claude_usage_observation(
        NOW - 100.0,
        UsageCompleteness::Complete,
        vec![
            named_window("session", 20.0, NOW - 100.0),
            named_window("weekly", 88.0, NOW - 100.0),
            claude_fable_window("weekly:claude-fable-5", 81.0, NOW - 100.0),
        ],
    );
    record_provider_usage_observation(temp.path(), probe, NOW).unwrap();

    let passive = claude_usage_observation(
        NOW - 50.0,
        UsageCompleteness::Partial,
        vec![claude_fable_window(
            "window:seven-day-overage-included",
            82.0,
            NOW - 50.0,
        )],
    );
    record_provider_usage_observation(temp.path(), passive, NOW).unwrap();

    let late_complete = claude_usage_observation(
        NOW - 80.0,
        UsageCompleteness::Complete,
        vec![
            named_window("session", 25.0, NOW - 80.0),
            named_window("weekly", 89.0, NOW - 80.0),
            claude_fable_window("weekly:claude-fable-5", 81.5, NOW - 80.0),
        ],
    );
    record_provider_usage_observation(temp.path(), late_complete, NOW).unwrap();

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    let provider = &snapshot.providers[0];
    assert_eq!(
        provider
            .windows
            .iter()
            .map(|window| window.key.as_str())
            .collect::<Vec<_>>(),
        vec!["session", "weekly", "weekly:claude-fable-5"]
    );
    let fable = provider
        .windows
        .iter()
        .find(|window| window.key == "weekly:claude-fable-5")
        .unwrap();
    assert_eq!(fable.used_percent, 82.0);

    let temp = tempdir().unwrap();
    let passive = claude_usage_observation(
        NOW - 90.0,
        UsageCompleteness::Partial,
        vec![claude_fable_window(
            "window:seven-day-overage-included",
            82.0,
            NOW - 90.0,
        )],
    );
    record_provider_usage_observation(temp.path(), passive, NOW).unwrap();
    let newer_probe = claude_usage_observation(
        NOW - 40.0,
        UsageCompleteness::Complete,
        vec![claude_fable_window(
            "weekly:claude-fable-5",
            83.0,
            NOW - 40.0,
        )],
    );
    record_provider_usage_observation(temp.path(), newer_probe, NOW).unwrap();
    let stale_generation = ProviderUsageObservationWire {
        account_generation: 0,
        ..claude_usage_observation(
            NOW - 10.0,
            UsageCompleteness::Partial,
            vec![claude_fable_window(
                "window:seven-day-overage-included",
                1.0,
                NOW - 10.0,
            )],
        )
    };
    let stale =
        record_provider_usage_observation(temp.path(), stale_generation, NOW)
            .unwrap();
    assert_eq!(stale.status, ProviderUsageStoreWriteStatus::StaleWriter);

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(snapshot.providers[0].windows.len(), 1);
    assert_eq!(
        snapshot.providers[0].windows[0].key,
        "weekly:claude-fable-5"
    );
    assert_eq!(snapshot.providers[0].windows[0].used_percent, 83.0);
}

fn write_raw_claude_store(
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

fn loaded_claude_windows(home: &std::path::Path) -> Vec<UsagePublicWindowWire> {
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

#[test]
fn usage_store_recovers_legacy_claude_fable_cache_without_read_repair() {
    let temp = tempdir().unwrap();
    write_raw_claude_store(
        temp.path(),
        vec![(
            "window:seven-day-overage-included",
            claude_fable_window(
                "window:seven-day-overage-included",
                82.0,
                NOW - 20.0,
            ),
            NOW - 20.0,
            NOW - 19.0,
        )],
        json!({}),
    );
    let path = provider_usage_state_path(temp.path());
    let before = fs::read(&path).unwrap();
    let windows = loaded_claude_windows(temp.path());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].key, "weekly:claude-fable-5");
    assert_eq!(windows[0].used_percent, 82.0);

    record_provider_usage_observation(
        temp.path(),
        claude_usage_observation(
            NOW - 5.0,
            UsageCompleteness::Partial,
            vec![claude_fable_window(
                "weekly:claude-fable-5",
                83.0,
                NOW - 5.0,
            )],
        ),
        NOW,
    )
    .unwrap();
    let serialized =
        fs::read_to_string(provider_usage_state_path(temp.path())).unwrap();
    assert!(!serialized.contains("seven-day-overage-included"));

    for (
        name,
        alias_order,
        alias_received,
        canonical_order,
        canonical_received,
        expected,
    ) in [
        (
            "alias-newer",
            NOW - 10.0,
            NOW - 9.0,
            NOW - 20.0,
            NOW - 19.0,
            82.0,
        ),
        (
            "canonical-newer",
            NOW - 20.0,
            NOW - 19.0,
            NOW - 10.0,
            NOW - 9.0,
            81.0,
        ),
        (
            "canonical-tie",
            NOW - 10.0,
            NOW - 9.0,
            NOW - 10.0,
            NOW - 9.0,
            81.0,
        ),
    ] {
        let temp = tempdir().unwrap();
        write_raw_claude_store(
            temp.path(),
            vec![
                (
                    "window:seven-day-overage-included",
                    claude_fable_window(
                        "window:seven-day-overage-included",
                        82.0,
                        NOW - 20.0,
                    ),
                    alias_order,
                    alias_received,
                ),
                (
                    "weekly:claude-fable-5",
                    claude_fable_window(
                        "weekly:claude-fable-5",
                        81.0,
                        NOW - 20.0,
                    ),
                    canonical_order,
                    canonical_received,
                ),
            ],
            json!({}),
        );
        let windows = loaded_claude_windows(temp.path());
        assert_eq!(windows.len(), 1, "{name}");
        assert_eq!(windows[0].key, "weekly:claude-fable-5", "{name}");
        assert_eq!(windows[0].used_percent, expected, "{name}");
    }

    let temp = tempdir().unwrap();
    write_raw_claude_store(
        temp.path(),
        vec![(
            "weekly:claude-fable-5",
            claude_fable_window("weekly:claude-fable-5", 81.0, NOW - 20.0),
            NOW - 20.0,
            NOW - 19.0,
        )],
        json!({"window:seven-day-overage-included": NOW - 5.0}),
    );
    let windows = loaded_claude_windows(temp.path());
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].key, "weekly:claude-fable-5");

    let temp = tempdir().unwrap();
    write_raw_claude_store(
        temp.path(),
        vec![(
            "window:seven-day-overage-included",
            claude_fable_window(
                "window:seven-day-overage-included",
                82.0,
                NOW - 20.0,
            ),
            NOW - 20.0,
            NOW - 19.0,
        )],
        json!({"weekly:claude-fable-5": NOW - 5.0}),
    );
    let windows = loaded_claude_windows(temp.path());
    assert!(windows.is_empty());
}

#[test]
fn usage_store_preserves_windows_after_failed_newer_attempt() {
    let temp = tempdir().unwrap();
    let full = usage_observation(
        "codex",
        "ctx-1",
        1,
        NOW - 40.0,
        UsageCompleteness::Complete,
        vec![named_window("weekly", 85.0, NOW - 40.0)],
    );
    record_provider_usage_observation(temp.path(), full, NOW).unwrap();

    let mut failed = usage_observation(
        "codex",
        "ctx-1",
        1,
        NOW - 10.0,
        UsageCompleteness::Partial,
        vec![],
    );
    failed.outcome = UsageCollectionOutcome::Error;
    failed.reason_code = Some(UsageReasonCode::Timeout);
    failed.diagnostic = Some("probe timed out".to_string());
    record_provider_usage_observation(temp.path(), failed, NOW).unwrap();

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    let provider = &snapshot.providers[0];
    assert_eq!(provider.collection_status, UsageCollectionOutcome::Error);
    assert_eq!(provider.collection_reason, Some(UsageReasonCode::Timeout));
    assert_eq!(provider.windows.len(), 1);
    assert!(provider.summary.is_none());
    assert_eq!(
        provider.known_constraints[0].attention,
        UsageAttentionKind::Low
    );
}

#[test]
fn usage_store_advances_generation_and_rejects_stale_writers() {
    let temp = tempdir().unwrap();
    let first = usage_observation(
        "grok",
        "ctx-1",
        1,
        NOW - 20.0,
        UsageCompleteness::Complete,
        vec![named_window("week", 40.0, NOW - 20.0)],
    );
    record_provider_usage_observation(temp.path(), first, NOW).unwrap();

    let context = prepare_provider_usage_account_context(
        temp.path(),
        "grok",
        "ctx-2",
        NOW - 10.0,
    )
    .unwrap();
    assert_eq!(context.account_generation, 2);
    assert!(context.changed);

    let stale = usage_observation(
        "grok",
        "ctx-1",
        1,
        NOW - 5.0,
        UsageCompleteness::Complete,
        vec![named_window("week", 1.0, NOW - 5.0)],
    );
    let outcome =
        record_provider_usage_observation(temp.path(), stale, NOW).unwrap();
    assert_eq!(outcome.status, ProviderUsageStoreWriteStatus::StaleWriter);
    assert!(!outcome.accepted);
    assert_eq!(outcome.account_generation, 2);

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(snapshot.providers[0].context_ref, "ctx-2");
    assert_eq!(snapshot.providers[0].account_generation, 2);
    assert!(snapshot.providers[0].windows.is_empty());
    assert_eq!(
        snapshot.providers[0].collection_reason,
        Some(UsageReasonCode::AccountContextChanged)
    );

    let current = usage_observation(
        "grok",
        "ctx-2",
        2,
        NOW - 3.0,
        UsageCompleteness::Complete,
        vec![named_window("week", 60.0, NOW - 3.0)],
    );
    record_provider_usage_observation(temp.path(), current, NOW).unwrap();
    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(
        snapshot.providers[0].collection_status,
        UsageCollectionOutcome::Ok
    );
    assert_eq!(snapshot.providers[0].windows.len(), 1);
}

#[test]
fn usage_store_reports_bad_provider_records_without_repairing_on_read() {
    let temp = tempdir().unwrap();
    let valid = usage_observation(
        "alpha",
        "ctx-alpha",
        1,
        NOW - 20.0,
        UsageCompleteness::Complete,
        vec![named_window("week", 10.0, NOW - 20.0)],
    );
    record_provider_usage_observation(temp.path(), valid, NOW).unwrap();
    let path = provider_usage_state_path(temp.path());
    let mut raw: Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    raw["providers"]["broken"] = json!({
        "version": 1,
        "provider": "not-broken",
        "context_id": "ctx",
        "account_generation": 1
    });
    fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();
    let before = fs::read(&path).unwrap();

    let read = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();

    assert_eq!(read.snapshot.providers.len(), 1);
    assert_eq!(read.snapshot.providers[0].provider, "alpha");
    assert_eq!(
        read.snapshot.collection_health,
        UsageCollectionHealth::Partial
    );
    assert_eq!(read.diagnostics.len(), 1);
    assert_eq!(read.diagnostics[0].provider.as_deref(), Some("broken"));
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn usage_store_decodes_old_schedules_and_isolates_future_schedule_fields() {
    let temp = tempdir().unwrap();
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx-alpha",
            1,
            NOW - 20.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW - 20.0)],
        ),
        NOW,
    )
    .unwrap();
    record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx-alpha",
        1,
        "error",
        NOW + 1.0,
    );

    let path = provider_usage_state_path(temp.path());
    let mut raw: Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let schedule = raw["schedules"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .as_object_mut()
        .unwrap();
    schedule.remove("first_failure_at");
    fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();

    let read = load_provider_usage_store(
        temp.path(),
        NOW + 2.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    assert!(read.diagnostics.is_empty());
    let health = read.snapshot.providers[0]
        .collector_health
        .as_ref()
        .unwrap();
    assert_eq!(health.state, UsageCollectorHealthState::Degraded);
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(health.failing_since, None);

    let mut raw: Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let schedule = raw["schedules"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .as_object_mut()
        .unwrap();
    schedule.insert("future_schedule_field".to_string(), json!(true));
    fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();

    let read = load_provider_usage_store(
        temp.path(),
        NOW + 3.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    assert_eq!(read.snapshot.providers.len(), 1);
    assert!(read.snapshot.providers[0].collector_health.is_none());
    assert_eq!(read.diagnostics.len(), 1);
    assert!(read.diagnostics[0]
        .message
        .contains("provider usage schedule is not valid v1 JSON"));
}

#[test]
fn usage_refresh_reservations_join_release_and_expire() {
    let temp = tempdir().unwrap();
    let request = ProviderUsageRefreshReservationRequestWire {
        provider: "codex".to_string(),
        context_id: "ctx".to_string(),
        account_generation: 1,
        operation_id: "op-1".to_string(),
        ttl_seconds: 10.0,
    };
    let first =
        reserve_provider_usage_refresh(temp.path(), request.clone(), NOW)
            .unwrap();
    assert_eq!(
        first.status,
        ProviderUsageRefreshReservationStatus::Reserved
    );
    let joined =
        reserve_provider_usage_refresh(temp.path(), request.clone(), NOW + 1.0)
            .unwrap();
    assert_eq!(joined.status, ProviderUsageRefreshReservationStatus::Joined);
    assert_eq!(joined.reservation.lease_id, first.reservation.lease_id);
    assert!(!release_provider_usage_refresh(
        temp.path(),
        "codex",
        "ctx",
        1,
        "wrong-lease",
        NOW + 2.0,
    )
    .unwrap());
    assert!(release_provider_usage_refresh(
        temp.path(),
        "codex",
        "ctx",
        1,
        &first.reservation.lease_id,
        NOW + 3.0,
    )
    .unwrap());
    let next = reserve_provider_usage_refresh(
        temp.path(),
        request.clone(),
        NOW + 11.0,
    )
    .unwrap();
    assert_eq!(next.status, ProviderUsageRefreshReservationStatus::Reserved);
}

#[test]
fn usage_refresh_backoff_grows_and_caps() {
    assert_eq!(refresh_backoff_seconds(0, 300.0), 0.0);
    assert_eq!(refresh_backoff_seconds(1, 300.0), 300.0);
    assert_eq!(refresh_backoff_seconds(2, 300.0), 600.0);
    assert_eq!(refresh_backoff_seconds(3, 300.0), 1_200.0);
    assert_eq!(refresh_backoff_seconds(4, 300.0), 1_800.0);
    assert_eq!(refresh_backoff_seconds(8, 300.0), 1_800.0);
}

fn record_refresh_attempt_at(
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

#[test]
fn usage_refresh_failure_streak_tracks_first_failure_and_clears_on_success() {
    let temp = tempdir().unwrap();
    let first =
        record_refresh_attempt_at(temp.path(), "alpha", "ctx", 1, "error", NOW);
    assert_eq!(first.consecutive_failures, 1);
    assert_eq!(first.first_failure_at, Some(NOW));

    let second = record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx",
        1,
        "error",
        NOW + 1.0,
    );
    assert_eq!(second.consecutive_failures, 2);
    assert_eq!(second.first_failure_at, Some(NOW));

    let success = record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx",
        1,
        "ok",
        NOW + 2.0,
    );
    assert_eq!(success.consecutive_failures, 0);
    assert_eq!(success.first_failure_at, None);
    assert_eq!(success.last_success_at, Some(NOW + 2.0));

    let next_streak = record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx",
        1,
        "error",
        NOW + 3.0,
    );
    assert_eq!(next_streak.consecutive_failures, 1);
    assert_eq!(next_streak.first_failure_at, Some(NOW + 3.0));
}

#[test]
fn collector_health_classifies_failure_boundaries_and_saturation() {
    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.last_success_at = Some(NOW - 10.0);
    schedule.first_failure_at = Some(NOW);
    for (failures, expected_state, expected_since) in [
        (0, UsageCollectorHealthState::Ok, None),
        (1, UsageCollectorHealthState::Degraded, Some(NOW)),
        (2, UsageCollectorHealthState::Degraded, Some(NOW)),
        (3, UsageCollectorHealthState::Failing, Some(NOW)),
        (u32::MAX, UsageCollectorHealthState::Failing, Some(NOW)),
    ] {
        schedule.consecutive_failures = failures;
        let health = collector_health_from_schedule(Some(&schedule)).unwrap();
        assert_eq!(health.state, expected_state);
        assert_eq!(health.consecutive_failures, failures);
        assert_eq!(health.last_success_at, Some(NOW - 10.0));
        assert_eq!(health.failing_since, expected_since);
    }
    assert!(collector_health_from_schedule(None).is_none());
}

#[test]
fn usage_store_projects_current_generation_collector_health_only() {
    let temp = tempdir().unwrap();
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx-1",
            1,
            NOW - 20.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW - 20.0)],
        ),
        NOW,
    )
    .unwrap();
    record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx-1",
        1,
        "error",
        NOW + 1.0,
    );
    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW + 2.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    let health = snapshot.providers[0].collector_health.as_ref().unwrap();
    assert_eq!(health.state, UsageCollectorHealthState::Degraded);
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(health.failing_since, Some(NOW + 1.0));

    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx-2",
            2,
            NOW + 3.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 12.0, NOW + 3.0)],
        ),
        NOW + 4.0,
    )
    .unwrap();
    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW + 5.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(snapshot.providers[0].context_ref, "ctx-2");
    assert_eq!(snapshot.providers[0].account_generation, 2);
    assert!(snapshot.providers[0].collector_health.is_none());

    let success = record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx-2",
        2,
        "ok",
        NOW + 6.0,
    );
    assert_eq!(success.first_failure_at, None);
    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW + 7.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    let health = snapshot.providers[0].collector_health.as_ref().unwrap();
    assert_eq!(health.state, UsageCollectorHealthState::Ok);
    assert_eq!(health.consecutive_failures, 0);
    assert_eq!(health.last_success_at, Some(NOW + 6.0));
    assert_eq!(health.failing_since, None);
}

#[test]
fn collection_problem_attention_requires_consistent_failure_with_cached_data() {
    let temp = tempdir().unwrap();
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx",
            1,
            NOW - 30.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW - 30.0)],
        ),
        NOW,
    )
    .unwrap();
    let mut failed = usage_observation(
        "alpha",
        "ctx",
        1,
        NOW - 10.0,
        UsageCompleteness::Partial,
        vec![],
    );
    failed.outcome = UsageCollectionOutcome::Error;
    failed.reason_code = Some(UsageReasonCode::Timeout);
    record_provider_usage_observation(temp.path(), failed, NOW + 1.0).unwrap();

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW + 2.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::None
    );

    record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx",
        1,
        "error",
        NOW + 3.0,
    );
    record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx",
        1,
        "error",
        NOW + 4.0,
    );
    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW + 5.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(
        snapshot.providers[0]
            .collector_health
            .as_ref()
            .unwrap()
            .state,
        UsageCollectorHealthState::Degraded
    );
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::None
    );

    record_refresh_attempt_at(
        temp.path(),
        "alpha",
        "ctx",
        1,
        "error",
        NOW + 6.0,
    );
    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW + 7.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(
        snapshot.providers[0]
            .collector_health
            .as_ref()
            .unwrap()
            .state,
        UsageCollectorHealthState::Failing
    );
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::CollectionProblem
    );
    assert_eq!(snapshot.providers[0].attention.window_key, None);
}

#[test]
fn collection_problem_attention_keeps_empty_problem_and_silent_unknown_arms() {
    let mut empty_problem = valid_observation();
    empty_problem.outcome = UsageCollectionOutcome::Error;
    empty_problem.reason_code = Some(UsageReasonCode::ProbeFailed);
    empty_problem.completeness = UsageCompleteness::Partial;
    empty_problem.windows.clear();
    let snapshot = project_usage_snapshot(
        &[empty_problem],
        NOW,
        CADENCE,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::CollectionProblem
    );

    let mut silent_unknown = valid_observation();
    silent_unknown.windows[0].used_percent = 10.0;
    silent_unknown.windows[0].observed_at = NOW - 1_500.0;
    let snapshot = project_usage_snapshot(
        &[silent_unknown],
        NOW,
        CADENCE,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    assert!(snapshot.providers[0].summary.is_none());
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::CollectionProblem
    );
}

#[test]
fn usage_attention_rank_order_places_collection_problem_above_low() {
    assert!(
        UsageAttentionKind::Rejected.rank()
            > UsageAttentionKind::VeryLow.rank()
    );
    assert!(
        UsageAttentionKind::VeryLow.rank()
            > UsageAttentionKind::CollectionProblem.rank()
    );
    assert!(
        UsageAttentionKind::CollectionProblem.rank()
            > UsageAttentionKind::Low.rank()
    );
    assert!(UsageAttentionKind::Low.rank() > UsageAttentionKind::None.rank());
}

#[test]
fn indicator_projection_order_is_provider_then_weekly_all_then_window_key() {
    let claude_product = UsageApplicabilityWire::Product {
        product: "claude".to_string(),
        model_ids: vec![],
    };
    let snapshot = indicator_snapshot(
        vec![
            usage_observation(
                "grok",
                "ctx-grok",
                1,
                NOW - 9.0,
                UsageCompleteness::Complete,
                vec![
                    indicator_window(
                        "included_monthly",
                        90.0,
                        Some(NOW + MONTH_SECONDS),
                        None,
                        UsageApplicabilityWire::Account,
                        NOW - 9.0,
                    ),
                    indicator_window(
                        "included_weekly",
                        95.0,
                        Some(NOW + WEEK_SECONDS),
                        None,
                        UsageApplicabilityWire::Account,
                        NOW - 9.0,
                    ),
                ],
            ),
            usage_observation(
                "claude",
                "ctx-claude",
                1,
                NOW - 10.0,
                UsageCompleteness::Complete,
                vec![
                    indicator_window(
                        "session",
                        82.0,
                        Some(NOW + 5.0 * 60.0 * 60.0),
                        None,
                        claude_product.clone(),
                        NOW - 10.0,
                    ),
                    indicator_window(
                        "weekly",
                        20.0,
                        Some(NOW + WEEK_SECONDS),
                        None,
                        claude_product,
                        NOW - 10.0,
                    ),
                ],
            ),
        ],
        NOW,
    );
    let projection = indicator_projection(snapshot, None, NOW);
    assert_eq!(
        projection
            .entries
            .iter()
            .map(|entry| (
                entry.provider.as_str(),
                entry.window_key.as_str(),
                entry.weekly_all
            ))
            .collect::<Vec<_>>(),
        vec![
            ("claude", "weekly", true),
            ("claude", "session", false),
            ("grok", "included_weekly", true),
            ("grok", "included_monthly", false),
        ]
    );
    assert_eq!(
        projection.entries[0].display_attention,
        UsageAttentionKind::None
    );
    assert_eq!(
        projection.entries[2].display_attention,
        UsageAttentionKind::VeryLow
    );
}

#[test]
fn usage_refresh_due_respects_cadence_backoff_and_explicit() {
    let never = evaluate_refresh_due(NOW, 300.0, false, None, None, false);
    assert!(never.due);
    assert_eq!(never.reason, "never_observed");

    let fresh =
        evaluate_refresh_due(NOW, 300.0, false, None, Some(NOW - 10.0), false);
    assert!(!fresh.due);
    assert_eq!(fresh.reason, "fresh");

    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.backoff_until = Some(NOW + 60.0);
    let backed_off = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 400.0),
        false,
    );
    assert!(!backed_off.due);
    assert_eq!(backed_off.reason, "backoff");

    let explicit = evaluate_refresh_due(
        NOW,
        300.0,
        true,
        Some(&schedule),
        Some(NOW - 10.0),
        false,
    );
    assert!(explicit.due);
    assert_eq!(explicit.reason, "explicit");

    schedule.retry_after_until = Some(NOW + 30.0);
    let retry_after = evaluate_refresh_due(
        NOW,
        300.0,
        true,
        Some(&schedule),
        Some(NOW - 10.0),
        false,
    );
    assert!(!retry_after.due);
    assert_eq!(retry_after.reason, "retry_after");
}

#[test]
fn usage_refresh_future_marker_does_not_delay_ordinary_due_reasons() {
    let mut schedule = empty_refresh_schedule("codex", "ctx", 1);
    schedule.due_at = Some(NOW + 4.0 * 24.0 * 60.0 * 60.0);
    schedule.due_reason = Some("disable_expiry".to_string());

    let cadence_due = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 301.0),
        false,
    );
    assert!(cadence_due.due);
    assert_eq!(cadence_due.reason, "cadence");

    let never_observed =
        evaluate_refresh_due(NOW, 300.0, false, Some(&schedule), None, false);
    assert!(never_observed.due);
    assert_eq!(never_observed.reason, "never_observed");

    let reset_passed = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 10.0),
        true,
    );
    assert!(reset_passed.due);
    assert_eq!(reset_passed.reason, "reset_passed");
}

#[test]
fn usage_refresh_future_marker_competes_with_fresh_deadline() {
    let mut schedule = empty_refresh_schedule("codex", "ctx", 1);
    schedule.due_reason = Some("disable_expiry".to_string());

    schedule.due_at = Some(NOW + 60.0);
    let marker_first = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 10.0),
        false,
    );
    assert!(!marker_first.due);
    assert_eq!(marker_first.reason, "scheduled");
    assert_eq!(marker_first.next_at, Some(NOW + 60.0));

    schedule.due_at = Some(NOW + 600.0);
    let cadence_first = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 10.0),
        false,
    );
    assert!(!cadence_first.due);
    assert_eq!(cadence_first.reason, "fresh");
    assert_eq!(cadence_first.next_at, Some(NOW + 290.0));

    schedule.due_at = Some(NOW + 290.0);
    let equal_deadline = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 10.0),
        false,
    );
    assert!(!equal_deadline.due);
    assert_eq!(equal_deadline.reason, "scheduled");
    assert_eq!(equal_deadline.next_at, Some(NOW + 290.0));
}

#[test]
fn usage_refresh_restrictions_outweigh_future_and_reached_markers() {
    let mut schedule = empty_refresh_schedule("codex", "ctx", 1);
    schedule.due_at = Some(NOW - 1.0);
    schedule.due_reason = Some("disable_expiry".to_string());
    schedule.retry_after_until = Some(NOW + 30.0);

    let retry_after_automatic = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 400.0),
        true,
    );
    assert!(!retry_after_automatic.due);
    assert_eq!(retry_after_automatic.reason, "retry_after");
    assert_eq!(retry_after_automatic.next_at, Some(NOW + 30.0));

    let retry_after_explicit = evaluate_refresh_due(
        NOW,
        300.0,
        true,
        Some(&schedule),
        Some(NOW - 400.0),
        true,
    );
    assert!(!retry_after_explicit.due);
    assert_eq!(retry_after_explicit.reason, "retry_after");

    schedule.retry_after_until = None;
    schedule.backoff_until = Some(NOW + 45.0);
    let backoff = evaluate_refresh_due(
        NOW,
        300.0,
        false,
        Some(&schedule),
        Some(NOW - 400.0),
        true,
    );
    assert!(!backoff.due);
    assert_eq!(backoff.reason, "backoff");
    assert_eq!(backoff.next_at, Some(NOW + 45.0));

    schedule.backoff_until = None;
    schedule.due_at = Some(NOW + 60.0);
    schedule.cooldown_until = Some(NOW + 20.0);
    let cooldown = evaluate_refresh_due(
        NOW,
        300.0,
        true,
        Some(&schedule),
        Some(NOW - 400.0),
        true,
    );
    assert!(!cooldown.due);
    assert_eq!(cooldown.reason, "cooldown");
    assert_eq!(cooldown.next_at, Some(NOW + 20.0));
}

#[test]
fn usage_refresh_admission_joins_defers_and_recovers_after_expiry() {
    let temp = tempdir().unwrap();
    let due = evaluate_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshDueRequestWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            cadence_seconds: 300.0,
            explicit: false,
        },
        NOW,
    )
    .unwrap();
    assert!(due.due);
    assert_eq!(due.reason, "never_observed");

    let request = ProviderUsageRefreshAdmitRequestWire {
        provider: "synth".to_string(),
        context_id: "ctx".to_string(),
        account_generation: 1,
        operation_id: "op-1".to_string(),
        ttl_seconds: 10.0,
        cadence_seconds: 300.0,
        explicit: false,
    };
    let first = admit_provider_usage_refresh(temp.path(), request.clone(), NOW)
        .unwrap();
    assert_eq!(first.status, ProviderUsageRefreshAdmissionStatus::Reserved);
    let joined =
        admit_provider_usage_refresh(temp.path(), request.clone(), NOW + 1.0)
            .unwrap();
    assert_eq!(joined.status, ProviderUsageRefreshAdmissionStatus::Joined);
    assert_eq!(
        joined.reservation.as_ref().map(|row| row.lease_id.clone()),
        first.reservation.as_ref().map(|row| row.lease_id.clone())
    );

    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "synth",
            "ctx",
            1,
            NOW,
            UsageCompleteness::Complete,
            vec![named_window("week", 12.5, NOW)],
        ),
        NOW + 2.0,
    )
    .unwrap();
    record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: 300.0,
        },
        NOW + 2.0,
    )
    .unwrap();
    assert!(release_provider_usage_refresh(
        temp.path(),
        "synth",
        "ctx",
        1,
        &first.reservation.unwrap().lease_id,
        NOW + 3.0,
    )
    .unwrap());

    let deferred =
        admit_provider_usage_refresh(temp.path(), request.clone(), NOW + 4.0)
            .unwrap();
    assert_eq!(
        deferred.status,
        ProviderUsageRefreshAdmissionStatus::Deferred
    );
    assert_eq!(deferred.reason.as_deref(), Some("fresh"));

    let explicit = admit_provider_usage_refresh(
        temp.path(),
        ProviderUsageRefreshAdmitRequestWire {
            explicit: true,
            operation_id: "op-2".to_string(),
            ..request.clone()
        },
        NOW + 4.0,
    )
    .unwrap();
    assert_eq!(
        explicit.status,
        ProviderUsageRefreshAdmissionStatus::Deferred
    );
    assert_eq!(explicit.reason.as_deref(), Some("cooldown"));

    let after_cooldown = admit_provider_usage_refresh(
        temp.path(),
        ProviderUsageRefreshAdmitRequestWire {
            explicit: true,
            operation_id: "op-3".to_string(),
            ttl_seconds: 5.0,
            ..request
        },
        NOW + 10.0,
    )
    .unwrap();
    assert_eq!(
        after_cooldown.status,
        ProviderUsageRefreshAdmissionStatus::Reserved
    );

    let expired = admit_provider_usage_refresh(
        temp.path(),
        ProviderUsageRefreshAdmitRequestWire {
            provider: "beta".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            operation_id: "op-old".to_string(),
            ttl_seconds: 2.0,
            cadence_seconds: 300.0,
            explicit: false,
        },
        NOW,
    )
    .unwrap();
    assert_eq!(
        expired.status,
        ProviderUsageRefreshAdmissionStatus::Reserved
    );
    let recovered = admit_provider_usage_refresh(
        temp.path(),
        ProviderUsageRefreshAdmitRequestWire {
            provider: "beta".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            operation_id: "op-new".to_string(),
            ttl_seconds: 2.0,
            cadence_seconds: 300.0,
            explicit: false,
        },
        NOW + 3.0,
    )
    .unwrap();
    assert_eq!(
        recovered.status,
        ProviderUsageRefreshAdmissionStatus::Reserved
    );
    assert_ne!(
        recovered.reservation.unwrap().operation_id,
        expired.reservation.unwrap().operation_id
    );
}

#[test]
fn usage_refresh_future_disable_expiry_preserves_marker_and_recovers_cadence() {
    let temp = tempdir().unwrap();
    let provider = "codex";
    let context_id = "ctx";
    let generation = 1;
    let reminder_at = NOW + 4.0 * 24.0 * 60.0 * 60.0;

    let mut exhausted = named_window("default", 100.0, NOW - 1_000.0);
    exhausted.vendor_state = UsageVendorState::Rejected;
    exhausted.resets_at = Some(NOW - 10.0);
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            provider,
            context_id,
            generation,
            NOW - 1_000.0,
            UsageCompleteness::Complete,
            vec![exhausted],
        ),
        NOW - 999.0,
    )
    .unwrap();
    mark_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshMarkDueRequestWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation: generation,
            reason: "disable_expiry".to_string(),
            due_at: Some(reminder_at),
        },
        NOW - 900.0,
    )
    .unwrap();

    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            provider,
            context_id,
            generation,
            NOW,
            UsageCompleteness::Complete,
            vec![named_window("default", 7.0, NOW)],
        ),
        NOW + 1.0,
    )
    .unwrap();
    let early_success = record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation: generation,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: CADENCE,
        },
        NOW + 1.0,
    )
    .unwrap();
    assert_eq!(early_success.due_at, Some(reminder_at));
    assert_eq!(early_success.due_reason.as_deref(), Some("disable_expiry"));

    let request = ProviderUsageRefreshAdmitRequestWire {
        provider: provider.to_string(),
        context_id: context_id.to_string(),
        account_generation: generation,
        operation_id: "op-cadence-1".to_string(),
        ttl_seconds: 10.0,
        cadence_seconds: CADENCE,
        explicit: false,
    };
    let first_cadence = admit_provider_usage_refresh(
        temp.path(),
        request.clone(),
        NOW + CADENCE,
    )
    .unwrap();
    assert_eq!(
        first_cadence.status,
        ProviderUsageRefreshAdmissionStatus::Reserved
    );

    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            provider,
            context_id,
            generation,
            NOW + CADENCE + 1.0,
            UsageCompleteness::Complete,
            vec![named_window("default", 6.0, NOW + CADENCE + 1.0)],
        ),
        NOW + CADENCE + 2.0,
    )
    .unwrap();
    let second_success = record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation: generation,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: CADENCE,
        },
        NOW + CADENCE + 2.0,
    )
    .unwrap();
    assert_eq!(second_success.due_at, Some(reminder_at));
    release_provider_usage_refresh(
        temp.path(),
        provider,
        context_id,
        generation,
        &first_cadence.reservation.unwrap().lease_id,
        NOW + CADENCE + 3.0,
    )
    .unwrap();

    let second_cadence = admit_provider_usage_refresh(
        temp.path(),
        ProviderUsageRefreshAdmitRequestWire {
            operation_id: "op-cadence-2".to_string(),
            ..request.clone()
        },
        NOW + 2.0 * CADENCE + 2.0,
    )
    .unwrap();
    assert_eq!(
        second_cadence.status,
        ProviderUsageRefreshAdmissionStatus::Reserved
    );
    record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation: generation,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: CADENCE,
        },
        NOW + 2.0 * CADENCE + 3.0,
    )
    .unwrap();
    release_provider_usage_refresh(
        temp.path(),
        provider,
        context_id,
        generation,
        &second_cadence.reservation.unwrap().lease_id,
        NOW + 2.0 * CADENCE + 4.0,
    )
    .unwrap();

    let expiry_due = evaluate_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshDueRequestWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation: generation,
            cadence_seconds: CADENCE,
            explicit: false,
        },
        reminder_at,
    )
    .unwrap();
    assert!(expiry_due.due);
    assert_eq!(expiry_due.reason, "marked_due");

    let consumed = record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: provider.to_string(),
            context_id: context_id.to_string(),
            account_generation: generation,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: CADENCE,
        },
        reminder_at + 1.0,
    )
    .unwrap();
    assert_eq!(consumed.due_at, None);
    assert_eq!(consumed.due_reason, None);
}

#[test]
fn usage_refresh_mark_due_is_once_per_reason_and_survives_future_due() {
    let temp = tempdir().unwrap();
    let first = mark_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshMarkDueRequestWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            reason: "limit_event".to_string(),
            due_at: Some(NOW),
        },
        NOW,
    )
    .unwrap();
    assert!(first.marked);
    let again = mark_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshMarkDueRequestWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            reason: "limit_event".to_string(),
            due_at: Some(NOW),
        },
        NOW,
    )
    .unwrap();
    assert!(!again.marked);

    let future = mark_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshMarkDueRequestWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            reason: "disable_expiry".to_string(),
            due_at: Some(NOW + 60.0),
        },
        NOW,
    )
    .unwrap();
    assert!(future.marked);
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "synth",
            "ctx",
            1,
            NOW,
            UsageCompleteness::Complete,
            vec![named_window("week", 12.5, NOW)],
        ),
        NOW + 1.0,
    )
    .unwrap();
    record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: 300.0,
        },
        NOW + 1.0,
    )
    .unwrap();
    let still_scheduled = evaluate_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshDueRequestWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            cadence_seconds: 300.0,
            explicit: false,
        },
        NOW + 2.0,
    )
    .unwrap();
    assert!(!still_scheduled.due);
    assert_eq!(still_scheduled.reason, "scheduled");
    let after_expiry = evaluate_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshDueRequestWire {
            provider: "synth".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            cadence_seconds: 300.0,
            explicit: false,
        },
        NOW + 61.0,
    )
    .unwrap();
    assert!(after_expiry.due);
    assert_eq!(after_expiry.reason, "marked_due");
}

#[test]
fn usage_store_uses_private_file_permissions_and_ignores_temp_siblings() {
    let temp = tempdir().unwrap();
    let temp_sibling =
        temp.path().join(".llm_provider_usage.json.interrupted.tmp");
    fs::write(&temp_sibling, b"not complete").unwrap();
    let observation = usage_observation(
        "alpha",
        "ctx-alpha",
        1,
        NOW - 20.0,
        UsageCompleteness::Complete,
        vec![named_window("week", 10.0, NOW - 20.0)],
    );
    record_provider_usage_observation(temp.path(), observation, NOW).unwrap();
    assert!(temp_sibling.exists());

    let snapshot = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap()
    .snapshot;
    assert_eq!(snapshot.providers.len(), 1);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let dir_mode =
            fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777;
        let file_mode = fs::metadata(provider_usage_state_path(temp.path()))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }
}

#[test]
fn rejects_duplicate_providers_and_invalid_thresholds() {
    let observations = vec![valid_observation(), valid_observation()];
    let error = project_usage_snapshot(&observations, NOW, CADENCE, 75.0, 90.0)
        .unwrap_err();
    assert!(error.to_string().contains("duplicate provider"));

    let error = validate_usage_cadence(59.9).unwrap_err();
    assert!(error.to_string().contains("at least"));
    let error = validate_usage_thresholds(90.0, 90.0).unwrap_err();
    assert!(error.to_string().contains("warn_percent"));
}

#[test]
fn strips_controls_and_redacts_secret_diagnostics() {
    let mut observation = valid_observation();
    observation.windows[0].label = "Weekly\u{1b}[31m used\u{1b}[0m".to_string();
    observation.diagnostic =
        Some("failed for user@example.com at /home/bryan/.secrets".to_string());
    let validated = validate_usage_observation(observation, NOW).unwrap();
    assert_eq!(validated.windows[0].label, "Weekly used");
    assert_eq!(validated.diagnostic.as_deref(), Some("diagnostic redacted"));
}

#[test]
fn authoritative_empty_ok_is_allowed() {
    let mut observation = valid_observation();
    observation.windows.clear();
    observation.authoritative_empty = true;
    let snapshot =
        project_usage_snapshot(&[observation], NOW, CADENCE, 75.0, 90.0)
            .unwrap();
    assert!(snapshot.providers[0].windows.is_empty());
    assert!(snapshot.providers[0].summary.is_none());
    assert_eq!(
        snapshot.providers[0].completeness,
        UsageCompleteness::Complete
    );
}

#[test]
fn rejected_outranks_very_low() {
    let mut observation = valid_observation();
    observation.windows[0].used_percent = 95.0;
    observation.windows.push(UsageWindowObservationWire {
        key: "hard".to_string(),
        label: "Rejected".to_string(),
        used_percent: 10.0,
        resets_at: Some(NOW + 3_600.0),
        duration_seconds: None,
        period_start: None,
        applicability: UsageApplicabilityWire::Account,
        observed_at: NOW - 10.0,
        source: UsageSource::Probe,
        vendor_state: UsageVendorState::Rejected,
    });
    let snapshot =
        project_usage_snapshot(&[observation], NOW, CADENCE, 75.0, 90.0)
            .unwrap();
    assert_eq!(
        snapshot.providers[0].attention.kind,
        UsageAttentionKind::Rejected
    );
    assert_eq!(
        snapshot.providers[0].attention.window_key.as_deref(),
        Some("hard")
    );
}

#[test]
fn missing_quantities_are_null_not_zero() {
    let snapshot = project_usage_snapshot(
        &[valid_observation()],
        NOW,
        CADENCE,
        75.0,
        90.0,
    )
    .unwrap();
    let value = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(value["providers"][0]["plan"], Value::Null);
    assert_eq!(value["providers"][0]["collection_reason"], Value::Null);
    assert_eq!(value["providers"][0]["collector_health"], Value::Null);
    assert_eq!(
        value["providers"][0]["windows"][0]["exceeded_by_percent"],
        Value::Null
    );
    assert_ne!(
        value["providers"][0]["windows"][0]["remaining_percent"],
        json!(0.0)
    );
}

#[test]
fn public_json_rejects_unknown_observation_fields() {
    let value = json!({
        "schema_version": 1,
        "provider": "alpha",
        "context_id": "ctx",
        "account_generation": 1,
        "ordering_token": NOW - 1.0,
        "received_at": NOW - 1.0,
        "source": "probe",
        "outcome": "ok",
        "reason_code": null,
        "diagnostic": null,
        "completeness": "complete",
        "account_mode": null,
        "plan": null,
        "windows": [],
        "primary": true
    });
    let parsed: std::result::Result<
        ProviderUsageObservationWire,
        serde_json::Error,
    > = serde_json::from_value(value);
    assert!(parsed.unwrap_err().to_string().contains("unknown field"));
}
