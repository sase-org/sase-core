use super::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

const NOW: f64 = 1_800_000_000.0;
const CADENCE: f64 = 300.0;

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
