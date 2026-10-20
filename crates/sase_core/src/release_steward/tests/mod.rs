mod cadence;
mod escalation;
mod fixtures;
mod states;

use super::release_steward_decide;
use super::wire::{
    PrimaryStateWire, ReleaseStewardError, RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
};
use fixtures::releasable_request;
use serde_json::json;

#[test]
fn schema_mismatch_is_an_error() {
    let mut request = releasable_request();
    request.schema_version = 99;
    let error = release_steward_decide(&request).unwrap_err();
    match error {
        ReleaseStewardError::UnsupportedSchema { actual, expected } => {
            assert_eq!(actual, 99);
            assert_eq!(expected, RELEASE_STEWARD_WIRE_SCHEMA_VERSION);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn from_json_round_trips_a_releasable_snapshot() {
    let request = releasable_request();
    let value = serde_json::to_value(&request).unwrap();
    let result = super::release_steward_decide_from_json(&value).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::Releasable { pr_number: 42, .. }
    ));
    assert_eq!(result.schema_version, RELEASE_STEWARD_WIRE_SCHEMA_VERSION);
    assert_eq!(result.process_exit, 0);
}

#[test]
fn from_json_rejects_unknown_fields() {
    let error = super::release_steward_decide_from_json(&json!({
        "schema_version": 1,
        "extra": true
    }))
    .unwrap_err();
    assert!(matches!(error, ReleaseStewardError::InvalidRequest { .. }));
}

#[test]
fn wire_schema_version_matches_constant() {
    assert_eq!(
        super::release_steward_wire_schema_version(),
        RELEASE_STEWARD_WIRE_SCHEMA_VERSION
    );
}
