//! Usage store persistence, recovery, and generations.

use super::super::*;
use super::support::*;
use serde_json::{json, Value};
use std::fs;
use tempfile::tempdir;

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
