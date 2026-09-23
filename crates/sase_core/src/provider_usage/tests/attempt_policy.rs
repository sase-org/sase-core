//! Reason-aware adaptive attempt policy: class table, clamps, streaks,
//! legacy golden compatibility, old-file round-trips, and generation pruning.

use super::super::*;
use super::support::*;
use serde_json::json;
use std::fs;
use tempfile::tempdir;

fn adaptive_attempt(
    outcome: &str,
    reason: Option<UsageReasonCode>,
    retry_after: Option<f64>,
    floor: Option<f64>,
    fingerprint: Option<&str>,
) -> ProviderUsageRefreshAttemptWire {
    ProviderUsageRefreshAttemptWire {
        provider: "alpha".to_string(),
        context_id: "ctx".to_string(),
        account_generation: 1,
        outcome: outcome.to_string(),
        retry_after_seconds: retry_after,
        cadence_seconds: CADENCE,
        reason_code: reason,
        min_interval_seconds: floor,
        cli_fingerprint: fingerprint.map(str::to_string),
        adaptive: true,
    }
}

fn record_adaptive(
    home: &std::path::Path,
    attempt: ProviderUsageRefreshAttemptWire,
    now: f64,
) -> ProviderUsageRefreshScheduleWire {
    record_provider_usage_refresh_attempt(home, attempt, now).unwrap()
}

#[test]
fn adaptive_policy_class_table() {
    // Transient with a floor: base max(300, 600) x 2^0.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        Some(600.0),
        "error",
        Some(UsageReasonCode::Timeout),
        None,
        None,
        0,
        1,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 600.0));
    assert_eq!(policy.retry_after_until, None);
    assert_eq!(policy.consecutive_rate_limits, 0);
    assert_eq!(policy.parked_fingerprint, None);
    assert_eq!(policy.last_failure_reason.as_deref(), Some("timeout"));

    // Transient without a reason: outcome names the failure.
    let policy = refresh_failure_policy(
        NOW, CADENCE, None, "error", None, None, None, 0, 2,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 600.0));
    assert_eq!(policy.last_failure_reason.as_deref(), Some("error"));

    // Transient backoff caps at 30 minutes even with a floor.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        Some(600.0),
        "error",
        Some(UsageReasonCode::ProbeFailed),
        None,
        None,
        0,
        10,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 1_800.0));

    // Auth outcomes and reasons use the generic cadence backoff, no floor.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        Some(600.0),
        "unauthenticated",
        None,
        None,
        None,
        0,
        1,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 300.0));
    assert_eq!(
        policy.last_failure_reason.as_deref(),
        Some("unauthenticated")
    );
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::LoggedOut),
        None,
        None,
        0,
        2,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 600.0));
    assert_eq!(policy.last_failure_reason.as_deref(), Some("logged_out"));

    // Parked: 6 h backoff with the attempt fingerprint.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "unsupported",
        Some(UsageReasonCode::NotInstalled),
        None,
        Some("fp-1"),
        0,
        1,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 21_600.0));
    assert_eq!(policy.parked_fingerprint.as_deref(), Some("fp-1"));
    assert_eq!(policy.last_failure_reason.as_deref(), Some("not_installed"));

    // Vendor drift: fixed 1 h backoff.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::VendorDrift),
        None,
        None,
        0,
        1,
    );
    assert_eq!(policy.backoff_until, Some(NOW + 3_600.0));
    assert_eq!(policy.last_failure_reason.as_deref(), Some("vendor_drift"));
}

#[test]
fn adaptive_rate_limit_clamps_and_escalates() {
    // Retry-After below the 900 s floor clamps up.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::RateLimited),
        Some(60.0),
        None,
        0,
        1,
    );
    assert_eq!(policy.retry_after_until, Some(NOW + 900.0));
    assert_eq!(policy.backoff_until, None);
    assert_eq!(policy.consecutive_rate_limits, 1);

    // Retry-After above the 6 h ceiling clamps down.
    let policy = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::RateLimited),
        Some(100_000.0),
        None,
        0,
        1,
    );
    assert_eq!(policy.retry_after_until, Some(NOW + 21_600.0));

    // Missing Retry-After escalates 900 x 2^(k-1), capped at 7200.
    let first = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::RateLimited),
        None,
        None,
        0,
        1,
    );
    assert_eq!(first.retry_after_until, Some(NOW + 900.0));
    let second = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::RateLimited),
        None,
        None,
        1,
        2,
    );
    assert_eq!(second.retry_after_until, Some(NOW + 1_800.0));
    let capped = refresh_failure_policy(
        NOW,
        CADENCE,
        None,
        "error",
        Some(UsageReasonCode::RateLimited),
        None,
        None,
        10,
        11,
    );
    assert_eq!(capped.retry_after_until, Some(NOW + 7_200.0));
    assert_eq!(capped.consecutive_rate_limits, 11);
}

#[test]
fn adaptive_attempt_streaks_and_cooldown_end_to_end() {
    let temp = tempdir().unwrap();
    let first = record_adaptive(
        temp.path(),
        adaptive_attempt(
            "error",
            Some(UsageReasonCode::RateLimited),
            None,
            None,
            None,
        ),
        NOW,
    );
    assert_eq!(first.consecutive_failures, 1);
    assert_eq!(first.consecutive_rate_limits, 1);
    assert_eq!(first.retry_after_until, Some(NOW + 900.0));
    assert_eq!(first.backoff_until, None);
    assert_eq!(first.last_failure_reason.as_deref(), Some("rate_limited"));
    assert_eq!(first.cooldown_until, Some(NOW + 60.0));

    let second = record_adaptive(
        temp.path(),
        adaptive_attempt(
            "error",
            Some(UsageReasonCode::RateLimited),
            None,
            None,
            None,
        ),
        NOW + 1.0,
    );
    assert_eq!(second.consecutive_failures, 2);
    assert_eq!(second.consecutive_rate_limits, 2);
    assert_eq!(second.retry_after_until, Some(NOW + 1.0 + 1_800.0));

    // A non-rate failure resets the rate streak but keeps the failure streak.
    let parked = record_adaptive(
        temp.path(),
        adaptive_attempt(
            "unsupported",
            Some(UsageReasonCode::NotInstalled),
            None,
            None,
            Some("fp-1"),
        ),
        NOW + 2.0,
    );
    assert_eq!(parked.consecutive_failures, 3);
    assert_eq!(parked.consecutive_rate_limits, 0);
    assert_eq!(parked.backoff_until, Some(NOW + 2.0 + 21_600.0));
    assert_eq!(parked.retry_after_until, None);
    assert_eq!(parked.parked_fingerprint.as_deref(), Some("fp-1"));
    assert_eq!(parked.last_failure_reason.as_deref(), Some("not_installed"));

    // Success clears every adaptive field.
    let success = record_adaptive(
        temp.path(),
        adaptive_attempt("ok", None, None, None, None),
        NOW + 3.0,
    );
    assert_eq!(success.consecutive_failures, 0);
    assert_eq!(success.consecutive_rate_limits, 0);
    assert_eq!(success.backoff_until, None);
    assert_eq!(success.retry_after_until, None);
    assert_eq!(success.last_failure_reason, None);
    assert_eq!(success.parked_fingerprint, None);
    assert_eq!(success.last_success_at, Some(NOW + 3.0));
}

#[test]
fn adaptive_health_reports_retry_at_and_failure_reason() {
    let temp = tempdir().unwrap();
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx",
            1,
            NOW,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW)],
        ),
        NOW + 2.0,
    )
    .unwrap();
    record_adaptive(
        temp.path(),
        adaptive_attempt(
            "unsupported",
            Some(UsageReasonCode::NotInstalled),
            None,
            None,
            Some("fp-1"),
        ),
        NOW + 3.0,
    );
    let read = load_provider_usage_store(
        temp.path(),
        NOW + 4.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    let health = read.snapshot.providers[0]
        .collector_health
        .as_ref()
        .unwrap();
    assert_eq!(health.last_failure_reason.as_deref(), Some("not_installed"));
    assert_eq!(health.retry_at, Some(NOW + 3.0 + 21_600.0));

    // Expired gates report no retry time.
    let read = load_provider_usage_store(
        temp.path(),
        NOW + 100_000.0,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    let health = read.snapshot.providers[0]
        .collector_health
        .as_ref()
        .unwrap();
    assert_eq!(health.retry_at, None);
    assert_eq!(health.last_failure_reason.as_deref(), Some("not_installed"));
}

#[test]
fn legacy_attempts_keep_legacy_shape_and_cooldown() {
    let temp = tempdir().unwrap();
    // Unclamped Retry-After is preserved on the legacy path.
    let schedule = record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "error".to_string(),
            retry_after_seconds: Some(100_000.0),
            cadence_seconds: 300.0,
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
        },
        NOW,
    )
    .unwrap();
    assert_eq!(schedule.retry_after_until, Some(NOW + 100_000.0));
    assert_eq!(schedule.backoff_until, Some(NOW + 300.0));
    assert_eq!(schedule.cooldown_until, Some(NOW + 5.0));
    assert_eq!(schedule.last_failure_reason, None);
    assert_eq!(schedule.consecutive_rate_limits, 0);
    assert_eq!(schedule.parked_fingerprint, None);

    // Persisted JSON carries none of the new keys: legacy shape.
    let value = serde_json::to_value(&schedule).unwrap();
    let map = value.as_object().unwrap();
    for key in [
        "last_failure_reason",
        "consecutive_rate_limits",
        "parked_fingerprint",
    ] {
        assert!(!map.contains_key(key), "legacy row leaks {key}");
    }
    let mut keys = map.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "account_generation",
            "backoff_until",
            "consecutive_failures",
            "context_id",
            "cooldown_until",
            "due_at",
            "due_reason",
            "first_failure_at",
            "last_finished_at",
            "last_started_at",
            "last_success_at",
            "provider",
            "retry_after_until",
            "version",
        ]
    );
}

#[test]
fn legacy_attempt_with_new_fields_still_validates_but_ignores_them() {
    let temp = tempdir().unwrap();
    let schedule = record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "error".to_string(),
            retry_after_seconds: None,
            cadence_seconds: 300.0,
            reason_code: Some(UsageReasonCode::RateLimited),
            min_interval_seconds: Some(600.0),
            cli_fingerprint: Some("fp-1".to_string()),
            adaptive: false,
        },
        NOW,
    )
    .unwrap();
    // Legacy path ignores the reason-aware fields entirely.
    assert_eq!(schedule.backoff_until, Some(NOW + 300.0));
    assert_eq!(schedule.retry_after_until, None);
    assert_eq!(schedule.consecutive_rate_limits, 0);
    assert_eq!(schedule.last_failure_reason, None);
    assert_eq!(schedule.cooldown_until, Some(NOW + 5.0));
}

#[test]
fn attempt_validation_rejects_bad_floor_and_fingerprint() {
    let temp = tempdir().unwrap();
    let bad_floor = adaptive_attempt(
        "error",
        Some(UsageReasonCode::Timeout),
        None,
        Some(59.9),
        None,
    );
    assert!(
        record_provider_usage_refresh_attempt(temp.path(), bad_floor, NOW)
            .unwrap_err()
            .to_string()
            .contains("min_interval_seconds")
    );
    let padded = adaptive_attempt(
        "error",
        Some(UsageReasonCode::Timeout),
        None,
        None,
        Some("  padded  "),
    );
    assert!(
        record_provider_usage_refresh_attempt(temp.path(), padded, NOW)
            .unwrap_err()
            .to_string()
            .contains("cli_fingerprint")
    );
    let too_long = "f".repeat(1_025);
    let long = adaptive_attempt(
        "error",
        Some(UsageReasonCode::Timeout),
        None,
        None,
        Some(&too_long),
    );
    assert!(
        record_provider_usage_refresh_attempt(temp.path(), long, NOW)
            .unwrap_err()
            .to_string()
            .contains("cli_fingerprint")
    );
}

#[test]
fn observation_retry_after_clamps_and_rejects() {
    let mut observation = valid_observation();
    observation.reason_code = Some(UsageReasonCode::RateLimited);
    observation.retry_after_seconds = Some(100_000.0);
    assert_eq!(
        validate_usage_observation(observation, NOW)
            .unwrap()
            .retry_after_seconds,
        Some(86_400.0)
    );

    let mut observation = valid_observation();
    observation.retry_after_seconds = Some(-1.0);
    assert!(validate_usage_observation(observation, NOW)
        .unwrap_err()
        .to_string()
        .contains("retry_after_seconds"));

    let mut observation = valid_observation();
    observation.retry_after_seconds = Some(f64::INFINITY);
    assert!(validate_usage_observation(observation, NOW)
        .unwrap_err()
        .to_string()
        .contains("retry_after_seconds"));

    // Persisted observations keep the clamped value.
    let temp = tempdir().unwrap();
    let mut persisted = usage_observation(
        "alpha",
        "ctx",
        1,
        NOW - 10.0,
        UsageCompleteness::Partial,
        vec![],
    );
    persisted.outcome = UsageCollectionOutcome::Error;
    persisted.reason_code = Some(UsageReasonCode::RateLimited);
    persisted.retry_after_seconds = Some(100_000.0);
    record_provider_usage_observation(temp.path(), persisted, NOW).unwrap();
    let read = load_provider_usage_store(
        temp.path(),
        NOW,
        DEFAULT_USAGE_CADENCE_SECONDS,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    assert_eq!(
        read.snapshot.providers[0].collection_reason,
        Some(UsageReasonCode::RateLimited)
    );
}

#[test]
fn old_store_files_without_new_fields_round_trip() {
    let temp = tempdir().unwrap();
    let last_attempt = json!({
        "schema_version": 1,
        "provider": "alpha",
        "context_id": "ctx",
        "account_generation": 1,
        "ordering_token": NOW - 10.0,
        "received_at": NOW - 5.0,
        "source": "probe",
        "outcome": "error",
        "reason_code": "timeout",
        "diagnostic": null,
        "completeness": "partial",
        "authoritative_empty": false,
        "account_mode": null,
        "plan": null,
        "windows": []
    });
    let schedule_key =
        format!("alpha{}1{}ctx", char::from(31u8), char::from(31u8));
    let mut schedules = serde_json::Map::new();
    schedules.insert(
        schedule_key,
        json!({
            "version": 1,
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "last_started_at": NOW - 20.0,
            "last_finished_at": NOW - 10.0,
            "last_success_at": null,
            "first_failure_at": NOW - 10.0,
            "consecutive_failures": 1,
            "backoff_until": NOW + 290.0,
            "retry_after_until": null,
            "cooldown_until": NOW - 5.0,
            "due_at": null,
            "due_reason": null
        }),
    );
    let raw = json!({
        "version": 1,
        "providers": {
            "alpha": {
                "version": 1,
                "provider": "alpha",
                "context_id": "ctx",
                "account_generation": 1,
                "last_attempt": last_attempt,
                "last_attempt_ordering_token": NOW - 10.0,
                "last_attempt_received_at": NOW - 5.0,
                "last_full_observation_at": null,
                "last_full_ordering_token": null,
                "windows": {},
                "tombstones": {}
            }
        },
        "reservations": {},
        "schedules": schedules,
    });
    let path = provider_usage_state_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();
    let read = load_provider_usage_store(
        temp.path(),
        NOW,
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
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(health.last_failure_reason, None);
    assert_eq!(health.retry_at, Some(NOW + 290.0));
}

#[test]
fn superseded_generation_schedules_pruned_on_write() {
    let temp = tempdir().unwrap();
    // A gen-1 schedule row for claude.
    record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "claude".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "error".to_string(),
            retry_after_seconds: None,
            cadence_seconds: 300.0,
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
        },
        NOW - 100.0,
    )
    .unwrap();
    // The provider record advances to generation 2.
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "claude",
            "ctx",
            2,
            NOW - 10.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW - 10.0)],
        ),
        NOW - 9.0,
    )
    .unwrap();
    // Any further write drops the superseded gen-1 row from the file.
    record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "claude".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 2,
            outcome: "ok".to_string(),
            retry_after_seconds: None,
            cadence_seconds: 300.0,
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
        },
        NOW,
    )
    .unwrap();
    let raw: serde_json::Value = serde_json::from_slice(
        &fs::read(provider_usage_state_path(temp.path())).unwrap(),
    )
    .unwrap();
    let schedules = raw["schedules"].as_object().unwrap();
    assert_eq!(schedules.len(), 1);
    let key = schedules.keys().next().unwrap();
    assert!(key.contains("claude"));
    let row = &schedules[key];
    assert_eq!(row["account_generation"], json!(2));
}
