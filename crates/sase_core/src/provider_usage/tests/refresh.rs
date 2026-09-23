//! Refresh scheduling, backoff, and collector health.

use super::super::*;
use super::support::*;
use tempfile::tempdir;

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
        let health =
            collector_health_from_schedule(Some(&schedule), NOW).unwrap();
        assert_eq!(health.state, expected_state);
        assert_eq!(health.consecutive_failures, failures);
        assert_eq!(health.last_success_at, Some(NOW - 10.0));
        assert_eq!(health.failing_since, expected_since);
    }
    assert!(collector_health_from_schedule(None, NOW).is_none());
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
        adaptive: false,
        min_interval_seconds: None,
        cli_fingerprint: None,
        active_cadence_seconds: None,
        warn_percent: None,
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
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
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
        adaptive: false,
        min_interval_seconds: None,
        cli_fingerprint: None,
        active_cadence_seconds: None,
        warn_percent: None,
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
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
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
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
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
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
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
            adaptive: false,
            min_interval_seconds: None,
            cli_fingerprint: None,
            active_cadence_seconds: None,
            warn_percent: None,
        },
        NOW + 61.0,
    )
    .unwrap();
    assert!(after_expiry.due);
    assert_eq!(after_expiry.reason, "marked_due");
}
