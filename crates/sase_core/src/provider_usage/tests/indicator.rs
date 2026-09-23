//! Indicator policy, thresholds, diagnostics, and projection order.

use super::super::*;
use super::support::*;
use serde_json::json;
use std::collections::BTreeMap;

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

#[test]
fn indicator_request_without_floors_projects_as_today() {
    let snapshot = indicator_snapshot(
        vec![usage_observation(
            "claude",
            "ctx-claude",
            1,
            NOW - 200.0,
            UsageCompleteness::Complete,
            vec![indicator_window(
                "session",
                10.0,
                Some(NOW + 3_600.0),
                None,
                UsageApplicabilityWire::Product {
                    product: "claude".to_string(),
                    model_ids: vec![],
                },
                NOW - 200.0,
            )],
        )],
        NOW,
    );
    let request = UsageIndicatorProjectionRequestWire {
        schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        snapshot: snapshot.clone(),
        indicator: Some(json!({"default": "always"})),
        eligible_providers: None,
        now: NOW,
        provider_min_intervals: None,
        cadence_seconds: 60.0,
        warn_percent: 75.0,
        critical_percent: 90.0,
    };
    let projection = project_usage_indicator(request).unwrap();
    assert_eq!(projection.entries.len(), 1);
    // Age 200 with cadence 60 is past 2 x cadence: stale under bare cadence.
    assert_eq!(projection.entries[0].freshness, UsageFreshness::Stale);

    // A legacy request without the field deserializes and projects identically.
    let legacy_json = json!({
        "schema_version": PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        "snapshot": serde_json::to_value(&snapshot).unwrap(),
        "indicator": {"default": "always"},
        "now": NOW,
        "cadence_seconds": 60.0,
        "warn_percent": 75.0,
        "critical_percent": 90.0,
    });
    let legacy_request: UsageIndicatorProjectionRequestWire =
        serde_json::from_value(legacy_json).unwrap();
    assert_eq!(legacy_request.provider_min_intervals, None);
    let legacy_projection = project_usage_indicator(legacy_request).unwrap();
    assert_eq!(
        serde_json::to_value(&legacy_projection).unwrap(),
        serde_json::to_value(&projection).unwrap()
    );

    // An explicitly empty floor map also projects identically.
    let empty_request = UsageIndicatorProjectionRequestWire {
        schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        snapshot,
        indicator: Some(json!({"default": "always"})),
        eligible_providers: None,
        now: NOW,
        provider_min_intervals: Some(BTreeMap::new()),
        cadence_seconds: 60.0,
        warn_percent: 75.0,
        critical_percent: 90.0,
    };
    let empty_projection = project_usage_indicator(empty_request).unwrap();
    assert_eq!(
        serde_json::to_value(&empty_projection).unwrap(),
        serde_json::to_value(&projection).unwrap()
    );
}

#[test]
fn indicator_floor_keeps_window_fresh_for_floored_provider_only() {
    let claude_product = UsageApplicabilityWire::Product {
        product: "claude".to_string(),
        model_ids: vec![],
    };
    let snapshot = indicator_snapshot(
        vec![
            usage_observation(
                "claude",
                "ctx-claude",
                1,
                NOW - 200.0,
                UsageCompleteness::Complete,
                vec![indicator_window(
                    "session",
                    10.0,
                    Some(NOW + 3_600.0),
                    None,
                    claude_product,
                    NOW - 200.0,
                )],
            ),
            usage_observation(
                "codex",
                "ctx-codex",
                1,
                NOW - 200.0,
                UsageCompleteness::Complete,
                vec![indicator_window(
                    "codex:primary",
                    10.0,
                    Some(NOW + WEEK_SECONDS),
                    Some(WEEK_SECONDS),
                    UsageApplicabilityWire::Account,
                    NOW - 200.0,
                )],
            ),
        ],
        NOW,
    );
    let mut floors = BTreeMap::new();
    floors.insert("claude".to_string(), 300.0);
    let request = UsageIndicatorProjectionRequestWire {
        schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        snapshot,
        indicator: Some(json!({"default": "always"})),
        eligible_providers: None,
        now: NOW,
        provider_min_intervals: Some(floors),
        cadence_seconds: 60.0,
        warn_percent: 75.0,
        critical_percent: 90.0,
    };
    let projection = project_usage_indicator(request).unwrap();
    let by_provider = projection
        .entries
        .iter()
        .map(|entry| (entry.provider.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    // Age 200 sits between 2 x cadence (120) and 2 x floor (600): the
    // floored provider stays fresh while the unfloored one goes stale.
    assert_eq!(by_provider["claude"].freshness, UsageFreshness::Fresh);
    assert_eq!(by_provider["codex"].freshness, UsageFreshness::Stale);
}

#[test]
fn indicator_invalid_floors_are_rejected() {
    let snapshot = indicator_snapshot(vec![], NOW);
    for (name, floors) in [
        ("below_minimum", vec![("claude".to_string(), 59.0)]),
        (
            "non_finite_infinite",
            vec![("claude".to_string(), f64::INFINITY)],
        ),
        ("non_finite_nan", vec![("claude".to_string(), f64::NAN)]),
        ("above_maximum", vec![("claude".to_string(), 86_401.0)]),
        ("empty_provider", vec![("".to_string(), 300.0)]),
        ("whitespace_provider", vec![(" claude ".to_string(), 300.0)]),
    ] {
        let request = UsageIndicatorProjectionRequestWire {
            schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
            snapshot: snapshot.clone(),
            indicator: None,
            eligible_providers: None,
            now: NOW,
            provider_min_intervals: Some(
                floors.into_iter().collect::<BTreeMap<_, _>>(),
            ),
            cadence_seconds: 60.0,
            warn_percent: 75.0,
            critical_percent: 90.0,
        };
        assert!(
            project_usage_indicator(request).is_err(),
            "{name} floor must be rejected"
        );
    }
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
