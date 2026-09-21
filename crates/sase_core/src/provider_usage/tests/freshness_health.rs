//! Freshness bands, reset passing, applicability, and health.

use super::super::*;
use super::support::*;

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
