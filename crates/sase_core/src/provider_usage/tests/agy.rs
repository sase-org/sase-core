//! Agy usage normalization and indicator selection.

use super::super::*;
use super::support::*;
use serde_json::json;
use std::collections::BTreeMap;

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
