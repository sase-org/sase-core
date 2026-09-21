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
