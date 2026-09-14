use serde_json::json;

use super::policy::{
    decide_gate_decision_acceptance, decide_gate_decision_acceptance_from_json,
    gate_decision_identity_fingerprint,
};
use super::wire::{
    GateDecisionAcceptanceRequestWire, GateDecisionOutcomeStatusWire,
    GateDecisionReceiptWire, GATE_DECISION_CODE_CONFLICT,
    GATE_DECISION_CODE_UNSUPPORTED_SCHEMA, GATE_DECISION_WIRE_SCHEMA_VERSION,
};

fn request(
    existing: Option<GateDecisionReceiptWire>,
) -> GateDecisionAcceptanceRequestWire {
    GateDecisionAcceptanceRequestWire {
        schema_version: GATE_DECISION_WIRE_SCHEMA_VERSION,
        gate_id: "gate-abc123".to_string(),
        request_hash: "sha256:deadbeef".to_string(),
        selected_option_ids: vec!["approve".to_string()],
        input_identity: "sha256:input".to_string(),
        feedback_identity: None,
        source: "cli".to_string(),
        accepted_at_unix: 1_726_000_000.0,
        execution_owner: Some("attempt:1234".to_string()),
        existing_receipt: existing,
    }
}

#[test]
fn accepts_a_fresh_decision_with_a_stable_fingerprint() {
    let outcome = decide_gate_decision_acceptance(&request(None)).unwrap();
    assert_eq!(outcome.status, GateDecisionOutcomeStatusWire::Accepted);
    assert_eq!(outcome.receipt.gate_id, "gate-abc123");
    assert_eq!(
        outcome.receipt.identity_fingerprint,
        gate_decision_identity_fingerprint(
            "sha256:deadbeef",
            &["approve".to_string()],
            "sha256:input",
            None
        )
    );
}

#[test]
fn replays_an_identical_resubmission_without_mutating_the_receipt() {
    let first = decide_gate_decision_acceptance(&request(None))
        .unwrap()
        .receipt;
    let mut second_request = request(Some(first.clone()));
    second_request.source = "ace".to_string();
    second_request.accepted_at_unix += 5.0;
    second_request.execution_owner = Some("attempt:9999".to_string());
    let outcome = decide_gate_decision_acceptance(&second_request).unwrap();
    assert_eq!(outcome.status, GateDecisionOutcomeStatusWire::Replayed);
    assert_eq!(
        outcome.receipt, first,
        "the original receipt wins, unmodified"
    );
}

#[test]
fn rejects_a_conflicting_selection_before_any_execution() {
    let first = decide_gate_decision_acceptance(&request(None))
        .unwrap()
        .receipt;
    let mut second_request = request(Some(first));
    second_request.selected_option_ids = vec!["reject".to_string()];
    let error = decide_gate_decision_acceptance(&second_request).unwrap_err();
    assert_eq!(error.code, GATE_DECISION_CODE_CONFLICT);
}

#[test]
fn rejects_a_conflicting_input_identity() {
    let first = decide_gate_decision_acceptance(&request(None))
        .unwrap()
        .receipt;
    let mut second_request = request(Some(first));
    second_request.input_identity = "sha256:different".to_string();
    let error = decide_gate_decision_acceptance(&second_request).unwrap_err();
    assert_eq!(error.code, GATE_DECISION_CODE_CONFLICT);
}

#[test]
fn rejects_a_conflicting_feedback_identity() {
    let mut base = request(None);
    base.feedback_identity = Some("sha256:feedback-a".to_string());
    let first = decide_gate_decision_acceptance(&base).unwrap().receipt;
    let mut second_request = request(Some(first));
    second_request.feedback_identity = Some("sha256:feedback-b".to_string());
    let error = decide_gate_decision_acceptance(&second_request).unwrap_err();
    assert_eq!(error.code, GATE_DECISION_CODE_CONFLICT);
}

#[test]
fn fingerprint_is_order_sensitive_over_already_normalized_input() {
    let a = gate_decision_identity_fingerprint(
        "h",
        &["a".to_string(), "b".to_string()],
        "i",
        None,
    );
    let b = gate_decision_identity_fingerprint(
        "h",
        &["b".to_string(), "a".to_string()],
        "i",
        None,
    );
    assert_ne!(
        a, b,
        "callers normalize selection order before calling; this function does not re-sort"
    );
}

#[test]
fn decides_from_json_and_rejects_an_unsupported_schema_version() {
    let value = json!({
        "schema_version": GATE_DECISION_WIRE_SCHEMA_VERSION,
        "gate_id": "gate-xyz",
        "request_hash": "sha256:abc",
        "selected_option_ids": ["approve"],
        "input_identity": "sha256:in",
        "source": "mobile",
        "accepted_at_unix": 1.0,
    });
    let outcome = decide_gate_decision_acceptance_from_json(&value).unwrap();
    assert_eq!(outcome.status, GateDecisionOutcomeStatusWire::Accepted);

    let bad = json!({
        "schema_version": 999,
        "gate_id": "gate-xyz",
        "request_hash": "sha256:abc",
        "selected_option_ids": [],
        "input_identity": "sha256:in",
        "source": "mobile",
        "accepted_at_unix": 1.0,
    });
    let error = decide_gate_decision_acceptance_from_json(&bad).unwrap_err();
    assert_eq!(error.code, GATE_DECISION_CODE_UNSUPPORTED_SCHEMA);
}
