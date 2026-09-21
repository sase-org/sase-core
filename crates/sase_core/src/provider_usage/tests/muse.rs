//! Muse usage normalization and indicator selection.

use super::super::*;
use super::support::*;
use serde_json::json;
use std::collections::BTreeMap;

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
