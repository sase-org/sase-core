//! Fixture-driven snapshot projection cases.

use super::super::*;
use super::support::*;
use serde_json::{json, Value};

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
    assert_eq!(value["providers"][0]["collector_health"], Value::Null);
    assert_eq!(
        value["providers"][0]["windows"][0]["exceeded_by_percent"],
        Value::Null
    );
    assert_ne!(
        value["providers"][0]["windows"][0]["remaining_percent"],
        json!(0.0)
    );
}
