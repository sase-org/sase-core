//! Collection-problem attention and rank order.

use super::super::*;
use super::support::*;
use tempfile::tempdir;

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
