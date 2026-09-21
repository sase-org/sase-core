//! Observation validation and rejection.

use super::super::*;
use super::support::*;
use serde_json::json;

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
fn vendor_drift_reason_code_is_accepted_and_unknown_codes_still_reject() {
    let mut observation = valid_observation();
    observation.outcome = UsageCollectionOutcome::Error;
    observation.reason_code = Some(UsageReasonCode::VendorDrift);
    observation.completeness = UsageCompleteness::Partial;
    observation.windows.clear();
    assert_eq!(
        validate_usage_observation(observation, NOW)
            .unwrap()
            .reason_code,
        Some(UsageReasonCode::VendorDrift)
    );

    let value = json!({
        "schema_version": 1,
        "provider": "alpha",
        "context_id": "ctx",
        "account_generation": 1,
        "ordering_token": NOW - 1.0,
        "received_at": NOW - 1.0,
        "source": "probe",
        "outcome": "error",
        "reason_code": "vendor_changed",
        "diagnostic": null,
        "completeness": "partial",
        "account_mode": null,
        "plan": null,
        "windows": []
    });
    let parsed: std::result::Result<
        ProviderUsageObservationWire,
        serde_json::Error,
    > = serde_json::from_value(value);
    assert!(parsed.unwrap_err().to_string().contains("unknown variant"));
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
