//! Adaptive admission: floors, jitter, parking, hot cadence, and
//! reservation reads. Covers the evaluation-order table, jitter bounds and
//! determinism, floor gating of marked and reset reasons, park/unpark on CLI
//! fingerprint changes, hot sources with passive suppression, hot-hint
//! coalescing and caps, reservation listing, floor-aware freshness, and the
//! golden legacy-compat shape.

use super::super::*;
use super::support::*;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

fn adaptive_due_request(
    provider: &str,
    explicit: bool,
    floor: Option<f64>,
    fingerprint: Option<&str>,
    active: Option<f64>,
    warn: Option<f64>,
) -> ProviderUsageRefreshDueRequestWire {
    ProviderUsageRefreshDueRequestWire {
        provider: provider.to_string(),
        context_id: "ctx".to_string(),
        account_generation: 1,
        cadence_seconds: CADENCE,
        explicit,
        adaptive: true,
        min_interval_seconds: floor,
        cli_fingerprint: fingerprint.map(str::to_string),
        active_cadence_seconds: active,
        warn_percent: warn,
    }
}

fn adaptive_admit_request(
    operation_id: &str,
    explicit: bool,
    floor: Option<f64>,
    fingerprint: Option<&str>,
) -> ProviderUsageRefreshAdmitRequestWire {
    ProviderUsageRefreshAdmitRequestWire {
        provider: "alpha".to_string(),
        context_id: "ctx".to_string(),
        account_generation: 1,
        operation_id: operation_id.to_string(),
        ttl_seconds: 60.0,
        cadence_seconds: CADENCE,
        explicit,
        adaptive: true,
        min_interval_seconds: floor,
        cli_fingerprint: fingerprint.map(str::to_string),
        active_cadence_seconds: None,
        warn_percent: None,
    }
}

fn parked_schedule(
    fingerprint: &str,
    now: f64,
) -> ProviderUsageRefreshScheduleWire {
    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.backoff_until = Some(now + 10_000.0);
    schedule.parked_fingerprint = Some(fingerprint.to_string());
    schedule
}

fn reset_window(
    used_percent: f64,
    resets_at: f64,
    observed_at: f64,
) -> UsageWindowObservationWire {
    let mut window = named_window("week", used_percent, observed_at);
    window.resets_at = Some(resets_at);
    window
}

#[test]
fn adaptive_evaluation_order() {
    // Retry-After defers everything, explicit requests included.
    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.retry_after_until = Some(NOW + 30.0);
    schedule.cooldown_until = Some(NOW + 20.0);
    schedule.backoff_until = Some(NOW + 60.0);
    schedule.due_at = Some(NOW - 1.0);
    let retry_explicit = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", true, None, None, None, None),
        Some(&schedule),
        Some(NOW - 400.0),
        true,
        &[],
    );
    assert!(!retry_explicit.due);
    assert_eq!(retry_explicit.reason, "retry_after");
    let retry_auto = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, None, None, None, None),
        Some(&schedule),
        Some(NOW - 400.0),
        true,
        &[],
    );
    assert!(!retry_auto.due);
    assert_eq!(retry_auto.reason, "retry_after");

    // The explicit cooldown defers explicit requests, then explicit is due.
    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.cooldown_until = Some(NOW + 20.0);
    let cooldown = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", true, None, None, None, None),
        Some(&schedule),
        Some(NOW - 10.0),
        false,
        &[],
    );
    assert!(!cooldown.due);
    assert_eq!(cooldown.reason, "cooldown");
    schedule.cooldown_until = Some(NOW - 1.0);
    let explicit = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", true, None, None, None, None),
        Some(&schedule),
        Some(NOW - 10.0),
        false,
        &[],
    );
    assert!(explicit.due);
    assert_eq!(explicit.reason, "explicit");

    // Parking: same fingerprint stays parked, a different one unparks.
    let schedule = parked_schedule("fp-1", NOW);
    let parked_same = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, None, Some("fp-1"), None, None),
        Some(&schedule),
        Some(NOW - 20_000.0),
        false,
        &[],
    );
    assert!(!parked_same.due);
    assert_eq!(parked_same.reason, "parked");
    assert_eq!(parked_same.next_at, schedule.backoff_until);
    let parked_none = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, None, None, None, None),
        Some(&schedule),
        Some(NOW - 20_000.0),
        false,
        &[],
    );
    assert!(!parked_none.due);
    assert_eq!(parked_none.reason, "parked");
    let unparked = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, None, Some("fp-2"), None, None),
        Some(&schedule),
        Some(NOW - 20_000.0),
        false,
        &[],
    );
    assert!(unparked.due);
    assert_eq!(unparked.reason, "cli_changed");

    // Plain backoff defers automatic requests only.
    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.backoff_until = Some(NOW + 45.0);
    let backoff = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, None, None, None, None),
        Some(&schedule),
        Some(NOW - 400.0),
        true,
        &[],
    );
    assert!(!backoff.due);
    assert_eq!(backoff.reason, "backoff");
    assert_eq!(backoff.next_at, Some(NOW + 45.0));

    // A fresh floor defers marked, reset, and unseen reasons alike.
    let mut schedule = empty_refresh_schedule("alpha", "ctx", 1);
    schedule.last_started_at = Some(NOW - 10.0);
    schedule.due_at = Some(NOW - 1.0);
    schedule.due_reason = Some("limit_event".to_string());
    let floor = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, Some(300.0), None, None, None),
        Some(&schedule),
        Some(NOW - 400.0),
        true,
        &[],
    );
    assert!(!floor.due);
    assert_eq!(floor.reason, "floor");
    assert_eq!(floor.next_at, Some(NOW - 10.0 + 300.0));
    let floor_unseen = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, Some(300.0), None, None, None),
        Some(&schedule),
        None,
        false,
        &[],
    );
    assert!(!floor_unseen.due);
    assert_eq!(floor_unseen.reason, "floor");

    // Past the floor, marked and reset reasons run again.
    let marked = evaluate_adaptive_refresh_due(
        NOW + 500.0,
        &adaptive_due_request("alpha", false, Some(300.0), None, None, None),
        Some(&schedule),
        Some(NOW - 400.0),
        true,
        &[],
    );
    assert!(marked.due);
    assert_eq!(marked.reason, "marked_due");
    schedule.due_at = None;
    let reset = evaluate_adaptive_refresh_due(
        NOW + 500.0,
        &adaptive_due_request("alpha", false, Some(300.0), None, None, None),
        Some(&schedule),
        Some(NOW - 400.0),
        true,
        &[],
    );
    assert!(reset.due);
    assert_eq!(reset.reason, "reset_passed");
}

#[test]
fn jitter_is_bounded_and_deterministic() {
    for provider in ["alpha", "claude", "codex"] {
        for context in ["ctx", "other"] {
            for generation in [1, 2, 99] {
                for observed in [NOW - 500.0, NOW - 10.0, NOW] {
                    let factor = refresh_jitter_factor(
                        provider, context, generation, observed,
                    );
                    assert!(
                        (ADAPTIVE_JITTER_LOW..=ADAPTIVE_JITTER_HIGH)
                            .contains(&factor),
                        "jitter {factor} out of bounds"
                    );
                    assert_eq!(
                        factor,
                        refresh_jitter_factor(
                            provider, context, generation, observed
                        )
                    );
                }
            }
        }
    }
    // The cadence interval carries the jittered factor.
    let observed = NOW - 10.0;
    let jitter = refresh_jitter_factor("alpha", "ctx", 1, observed);
    let decision = evaluate_adaptive_refresh_due(
        NOW,
        &adaptive_due_request("alpha", false, None, None, None, None),
        None,
        Some(observed),
        false,
        &[],
    );
    assert!(!decision.due);
    assert_eq!(decision.reason, "fresh");
    assert_eq!(decision.next_at, Some(observed + CADENCE * jitter));
}

#[test]
fn floor_blocks_marked_due_and_reset_passed() {
    let temp = tempdir().unwrap();
    // A reset window plus a marked-due row would both run without a floor.
    let window = reset_window(12.5, NOW - 5.0, NOW - 1_000.0);
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx",
            1,
            NOW - 1_000.0,
            UsageCompleteness::Complete,
            vec![window],
        ),
        NOW - 999.0,
    )
    .unwrap();
    mark_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshMarkDueRequestWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            reason: "limit_event".to_string(),
            due_at: Some(NOW),
        },
        NOW,
    )
    .unwrap();
    // Admission stamps last_started_at, arming the floor.
    admit_provider_usage_refresh(
        temp.path(),
        adaptive_admit_request("op-1", false, None, None),
        NOW,
    )
    .unwrap();

    let floored = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request("alpha", false, Some(300.0), None, None, None),
        NOW + 1.0,
    )
    .unwrap();
    assert!(!floored.due);
    assert_eq!(floored.reason, "floor");
    assert_eq!(floored.due_at, Some(NOW + 300.0));

    // Without the floor the marked reason runs.
    let marked = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request("alpha", false, None, None, None, None),
        NOW + 1.0,
    )
    .unwrap();
    assert!(marked.due);
    assert_eq!(marked.reason, "marked_due");
}

#[test]
fn park_and_unpark_on_fingerprint_change_end_to_end() {
    let temp = tempdir().unwrap();
    record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "unsupported".to_string(),
            retry_after_seconds: None,
            cadence_seconds: CADENCE,
            reason_code: Some(UsageReasonCode::NotInstalled),
            min_interval_seconds: None,
            cli_fingerprint: Some("fp-1".to_string()),
            adaptive: true,
        },
        NOW,
    )
    .unwrap();

    let parked = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request("alpha", false, None, Some("fp-1"), None, None),
        NOW + 1.0,
    )
    .unwrap();
    assert!(!parked.due);
    assert_eq!(parked.reason, "parked");

    let unparked = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request("alpha", false, None, Some("fp-2"), None, None),
        NOW + 1.0,
    )
    .unwrap();
    assert!(unparked.due);
    assert_eq!(unparked.reason, "cli_changed");
}

fn hot_view(
    used_percent: f64,
    resets_at: Option<f64>,
    received_at: f64,
) -> HotWindowView {
    HotWindowView {
        used_percent,
        resets_at,
        received_at,
    }
}

#[test]
fn hot_sources_and_passive_suppression() {
    // A hot hint alone marks the provider hot.
    assert!(provider_is_hot(NOW, 120.0, 75.0, Some(NOW + 60.0), &[]));
    assert!(!provider_is_hot(NOW, 120.0, 75.0, Some(NOW - 1.0), &[]));
    assert!(!provider_is_hot(NOW, 120.0, 75.0, None, &[]));

    // A warn-level window that has not reset is hot; a reset one is not.
    let warn = hot_view(80.0, Some(NOW + 3_600.0), NOW - 500.0);
    assert!(provider_is_hot(NOW, 120.0, 75.0, None, &[warn]));
    let cool = hot_view(10.0, Some(NOW + 3_600.0), NOW - 500.0);
    assert!(!provider_is_hot(NOW, 120.0, 75.0, None, &[cool]));
    let reset = hot_view(95.0, Some(NOW - 5.0), NOW - 500.0);
    assert!(!provider_is_hot(NOW, 120.0, 75.0, None, &[reset]));

    // Passive-coverage suppression: recently received windows mean live
    // stream events already keep the provider fresh.
    let fresh_warn = hot_view(80.0, Some(NOW + 3_600.0), NOW - 10.0);
    assert!(!provider_is_hot(NOW, 120.0, 75.0, None, &[fresh_warn]));
    assert!(!provider_is_hot(
        NOW,
        120.0,
        75.0,
        Some(NOW + 900.0),
        &[fresh_warn]
    ));
    // One stale window keeps the hot signal.
    assert!(provider_is_hot(
        NOW,
        120.0,
        75.0,
        Some(NOW + 900.0),
        &[fresh_warn, warn]
    ));
}

#[test]
fn hot_cadence_shortens_due_interval_but_respects_floors() {
    let temp = tempdir().unwrap();
    // A 200 s old observation: cold cadence (300 s) is fresh, hot (120 s)
    // is due. The received time is old enough to avoid suppression.
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx",
            1,
            NOW - 200.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW - 200.0)],
        ),
        NOW - 199.0,
    )
    .unwrap();

    // Without a hint the provider is cold: the 300 s idle cadence is fresh.
    let cold = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request("alpha", false, None, None, Some(120.0), None),
        NOW,
    )
    .unwrap();
    assert!(!cold.due);
    assert_eq!(cold.reason, "fresh");

    mark_provider_usage_hot(
        temp.path(),
        MarkProviderUsageHotRequestWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            until: NOW + 900.0,
        },
        NOW - 199.0,
    )
    .unwrap();

    // With the hint the same request is hot and due on the 120 s cadence.
    let hot = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request("alpha", false, None, None, Some(120.0), None),
        NOW,
    )
    .unwrap();
    assert!(hot.due, "hot provider should be due: {hot:?}");
    assert_eq!(hot.reason, "cadence");

    // A floor dominates a hotter cadence: the interval never drops below
    // the floor even when hot.
    let floored = evaluate_provider_usage_refresh_due(
        temp.path(),
        adaptive_due_request(
            "alpha",
            false,
            Some(300.0),
            None,
            Some(120.0),
            None,
        ),
        NOW,
    )
    .unwrap();
    assert!(!floored.due);
    assert_eq!(floored.reason, "fresh");
    let next_at = floored.due_at.unwrap();
    assert!(
        next_at >= NOW - 200.0 + 300.0,
        "hot interval dropped below the floor: {next_at}"
    );
}

#[test]
fn mark_hot_coalesces_and_caps() {
    let temp = tempdir().unwrap();
    let hot = |until: f64, now: f64| {
        mark_provider_usage_hot(
            temp.path(),
            MarkProviderUsageHotRequestWire {
                provider: "alpha".to_string(),
                context_id: "ctx".to_string(),
                account_generation: 1,
                until,
            },
            now,
        )
        .unwrap()
    };
    let first = hot(NOW + 900.0, NOW);
    assert!(first.marked);
    assert_eq!(first.hot_until, NOW + 900.0);

    // Within 60 s of the stored hint: no rewrite.
    let coalesced = hot(NOW + 890.0, NOW + 1.0);
    assert!(!coalesced.marked);
    assert_eq!(coalesced.hot_until, NOW + 900.0);

    // Beyond the horizon: capped at one hour.
    let capped = hot(NOW + 100_000.0, NOW + 2.0);
    assert!(capped.marked);
    assert_eq!(capped.hot_until, NOW + 2.0 + 3_600.0);

    // A hint far beyond the stored one writes again once the one-hour cap
    // admits it: at NOW + 100 the cap is NOW + 3700, past the stored 3602
    // plus the 60 s coalescing window.
    let extended = hot(NOW + 5_000.0, NOW + 100.0);
    assert!(extended.marked);
    assert_eq!(extended.hot_until, NOW + 100.0 + 3_600.0);

    // Bad timestamps are rejected.
    assert!(mark_provider_usage_hot(
        temp.path(),
        MarkProviderUsageHotRequestWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            until: -5.0,
        },
        NOW,
    )
    .unwrap_err()
    .to_string()
    .contains("until"));
}

#[test]
fn reservation_listing_returns_live_rows_only() {
    let temp = tempdir().unwrap();
    reserve_provider_usage_refresh(
        temp.path(),
        ProviderUsageRefreshReservationRequestWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            operation_id: "op-1".to_string(),
            ttl_seconds: 10.0,
        },
        NOW,
    )
    .unwrap();
    let live = list_provider_usage_refresh_reservations(temp.path(), NOW + 1.0)
        .unwrap();
    assert_eq!(live.version, PROVIDER_USAGE_STORE_SCHEMA_VERSION);
    assert_eq!(live.reservations.len(), 1);
    assert_eq!(live.reservations[0].operation_id, "op-1");

    // Expired rows are excluded, and listing never extends them.
    let expired =
        list_provider_usage_refresh_reservations(temp.path(), NOW + 11.0)
            .unwrap();
    assert!(expired.reservations.is_empty());
}

#[test]
fn floor_aware_freshness() {
    let temp = tempdir().unwrap();
    // A 700 s old window is stale under the 300 s cadence (fresh for 600 s)
    // but fresh under a 600 s floor (fresh for 1200 s).
    record_provider_usage_observation(
        temp.path(),
        usage_observation(
            "alpha",
            "ctx",
            1,
            NOW - 700.0,
            UsageCompleteness::Complete,
            vec![named_window("week", 10.0, NOW - 700.0)],
        ),
        NOW - 699.0,
    )
    .unwrap();

    let base = load_provider_usage_store(
        temp.path(),
        NOW,
        CADENCE,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
    )
    .unwrap();
    assert_eq!(
        base.snapshot.providers[0].windows[0].freshness,
        UsageFreshness::Stale
    );

    let mut floors = BTreeMap::new();
    floors.insert("alpha".to_string(), 600.0);
    let floored = load_provider_usage_store_with_floors(
        temp.path(),
        NOW,
        CADENCE,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
        Some(floors),
    )
    .unwrap();
    assert_eq!(
        floored.snapshot.providers[0].windows[0].freshness,
        UsageFreshness::Fresh
    );

    // A floor below the cadence changes nothing.
    let mut low = BTreeMap::new();
    low.insert("alpha".to_string(), 60.0);
    let unchanged = load_provider_usage_store_with_floors(
        temp.path(),
        NOW,
        CADENCE,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
        Some(low),
    )
    .unwrap();
    assert_eq!(
        unchanged.snapshot.providers[0].windows[0].freshness,
        UsageFreshness::Stale
    );

    // Invalid floors are rejected.
    let mut bad = BTreeMap::new();
    bad.insert("alpha".to_string(), 10.0);
    assert!(load_provider_usage_store_with_floors(
        temp.path(),
        NOW,
        CADENCE,
        DEFAULT_USAGE_WARN_PERCENT,
        DEFAULT_USAGE_CRITICAL_PERCENT,
        Some(bad),
    )
    .unwrap_err()
    .to_string()
    .contains("min_interval_seconds"));
}

#[test]
fn legacy_due_admit_read_compat() {
    // Requests without the new fields parse with legacy defaults.
    let legacy_due: ProviderUsageRefreshDueRequestWire =
        serde_json::from_value(json!({
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "cadence_seconds": 300.0,
            "explicit": false,
        }))
        .unwrap();
    assert!(!legacy_due.adaptive);
    assert_eq!(legacy_due.min_interval_seconds, None);
    assert_eq!(legacy_due.cli_fingerprint, None);
    assert_eq!(legacy_due.active_cadence_seconds, None);
    assert_eq!(legacy_due.warn_percent, None);

    // The legacy path ignores parked fingerprints, hot hints, and floors:
    // a parked schedule still reports plain backoff.
    let temp = tempdir().unwrap();
    let mut schedule = parked_schedule("fp-1", NOW);
    schedule.last_started_at = Some(NOW - 1.0);
    schedule.hot_until = Some(NOW + 900.0);
    let key = format!("alpha{}1{}ctx", char::from(31u8), char::from(31u8));
    let mut schedules = serde_json::Map::new();
    schedules.insert(key, serde_json::to_value(&schedule).unwrap());
    let raw = json!({
        "version": PROVIDER_USAGE_STORE_SCHEMA_VERSION,
        "providers": {},
        "reservations": {},
        "schedules": schedules,
    });
    let path = provider_usage_state_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();
    let due = evaluate_provider_usage_refresh_due(
        temp.path(),
        ProviderUsageRefreshDueRequestWire {
            provider: "alpha".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            cadence_seconds: CADENCE,
            explicit: false,
            adaptive: false,
            min_interval_seconds: Some(300.0),
            cli_fingerprint: Some("fp-2".to_string()),
            active_cadence_seconds: Some(120.0),
            warn_percent: Some(75.0),
        },
        NOW,
    )
    .unwrap();
    assert!(!due.due);
    assert_eq!(due.reason, "backoff");

    // Legacy attempts keep the legacy persisted shape: no hot_until key.
    let row = record_provider_usage_refresh_attempt(
        temp.path(),
        ProviderUsageRefreshAttemptWire {
            provider: "beta".to_string(),
            context_id: "ctx".to_string(),
            account_generation: 1,
            outcome: "error".to_string(),
            retry_after_seconds: None,
            cadence_seconds: CADENCE,
            reason_code: None,
            min_interval_seconds: None,
            cli_fingerprint: None,
            adaptive: false,
        },
        NOW,
    )
    .unwrap();
    assert_eq!(row.hot_until, None);
    let value = serde_json::to_value(&row).unwrap();
    assert!(!value.as_object().unwrap().contains_key("hot_until"));

    // Schedules written before hot_until existed load with no hint.
    let old: ProviderUsageRefreshScheduleWire = serde_json::from_value(json!({
        "version": 1,
        "provider": "gamma",
        "context_id": "ctx",
        "account_generation": 1,
        "last_started_at": null,
        "last_finished_at": null,
        "last_success_at": null,
        "first_failure_at": null,
        "consecutive_failures": 0,
        "backoff_until": null,
        "retry_after_until": null,
        "cooldown_until": null,
        "due_at": null,
        "due_reason": null
    }))
    .unwrap();
    assert_eq!(old.hot_until, None);
}

#[test]
fn admission_validation_rejects_bad_fields() {
    let temp = tempdir().unwrap();
    let bad_floor =
        adaptive_due_request("alpha", false, Some(59.9), None, None, None);
    assert!(
        evaluate_provider_usage_refresh_due(temp.path(), bad_floor, NOW)
            .unwrap_err()
            .to_string()
            .contains("min_interval_seconds")
    );
    let bad_fingerprint = adaptive_due_request(
        "alpha",
        false,
        None,
        Some("  padded  "),
        None,
        None,
    );
    assert!(evaluate_provider_usage_refresh_due(
        temp.path(),
        bad_fingerprint,
        NOW
    )
    .unwrap_err()
    .to_string()
    .contains("cli_fingerprint"));
    let bad_active =
        adaptive_due_request("alpha", false, None, None, Some(59.9), None);
    assert!(
        evaluate_provider_usage_refresh_due(temp.path(), bad_active, NOW)
            .unwrap_err()
            .to_string()
            .contains("active_cadence_seconds")
    );
    for bad_warn in [101.0, -1.0, f64::NAN] {
        let request = adaptive_due_request(
            "alpha",
            false,
            None,
            None,
            Some(120.0),
            Some(bad_warn),
        );
        assert!(
            evaluate_provider_usage_refresh_due(temp.path(), request, NOW)
                .unwrap_err()
                .to_string()
                .contains("warn_percent"),
            "warn {bad_warn} was accepted"
        );
    }
    let bad_admit =
        adaptive_admit_request("op-bad", false, Some(86_401.0), None);
    assert!(admit_provider_usage_refresh(temp.path(), bad_admit, NOW)
        .unwrap_err()
        .to_string()
        .contains("min_interval_seconds"));
}
