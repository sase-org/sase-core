use super::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

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
