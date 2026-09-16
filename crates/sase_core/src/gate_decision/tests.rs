use serde_json::json;

use super::policy::{
    decide_gate_decision_acceptance, decide_gate_decision_acceptance_from_json,
    decide_gate_lifecycle, decide_gate_lifecycle_from_json,
    gate_decision_identity_fingerprint,
};
use super::wire::{
    GateDecisionAcceptanceRequestWire, GateDecisionOutcomeStatusWire,
    GateDecisionReceiptWire, GateLifecycleRequestWire,
    GATE_DECISION_CODE_CONFLICT, GATE_DECISION_CODE_UNSUPPORTED_SCHEMA,
    GATE_DECISION_WIRE_SCHEMA_VERSION, GATE_LIFECYCLE_CODE_INVALID_RECEIPT,
    GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA,
    GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED,
    GATE_LIFECYCLE_DISPOSITION_ANSWERED,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_LOST,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_STOPPED,
    GATE_LIFECYCLE_DISPOSITION_CANCELLED_TIMEOUT,
    GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE,
    GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW,
    GATE_LIFECYCLE_DISPOSITION_PENDING, GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
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

fn lifecycle_receipt() -> GateDecisionReceiptWire {
    GateDecisionReceiptWire {
        schema_version: GATE_DECISION_WIRE_SCHEMA_VERSION,
        gate_id: "gate-abc123".to_string(),
        request_hash: "sha256:deadbeef".to_string(),
        selected_option_ids: vec!["approve".to_string()],
        input_identity: "sha256:input".to_string(),
        feedback_identity: None,
        source: "cli".to_string(),
        accepted_at_unix: 1_726_000_000.0,
        execution_owner: None,
        identity_fingerprint: "fingerprint".to_string(),
    }
}

fn lifecycle_request() -> GateLifecycleRequestWire {
    GateLifecycleRequestWire {
        schema_version: GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        gate_id: "gate-abc123".to_string(),
        request_hash: "sha256:deadbeef".to_string(),
        now_unix: 2_000.0,
        deadline_unix: Some(1_000.0),
        grace_seconds: 300.0,
        has_response: false,
        cancellation_reason: None,
        receipt: None,
        receipt_unreadable: false,
    }
}

#[test]
fn a_published_response_always_wins() {
    let mut request = lifecycle_request();
    request.has_response = true;
    request.receipt = Some(lifecycle_receipt());
    request.cancellation_reason = Some("timeout".to_string());
    let decision = decide_gate_lifecycle(&request).unwrap();
    assert_eq!(decision.disposition, GATE_LIFECYCLE_DISPOSITION_ANSWERED);
}

#[test]
fn an_unreadable_receipt_is_reported_explicitly() {
    let mut request = lifecycle_request();
    request.receipt_unreadable = true;
    let error = decide_gate_lifecycle(&request).unwrap_err();
    assert_eq!(error.code, GATE_LIFECYCLE_CODE_INVALID_RECEIPT);
}

#[test]
fn a_receipt_naming_a_different_gate_is_reported_explicitly() {
    let mut request = lifecycle_request();
    let mut receipt = lifecycle_receipt();
    receipt.gate_id = "gate-someone-else".to_string();
    request.receipt = Some(receipt);
    let error = decide_gate_lifecycle(&request).unwrap_err();
    assert_eq!(error.code, GATE_LIFECYCLE_CODE_INVALID_RECEIPT);
}

#[test]
fn a_receipt_naming_a_different_request_hash_is_reported_explicitly() {
    let mut request = lifecycle_request();
    let mut receipt = lifecycle_receipt();
    receipt.request_hash = "sha256:different".to_string();
    request.receipt = Some(receipt);
    let error = decide_gate_lifecycle(&request).unwrap_err();
    assert_eq!(error.code, GATE_LIFECYCLE_CODE_INVALID_RECEIPT);
}

#[test]
fn a_verified_receipt_is_accepted_unfinished_even_past_the_grace_window() {
    let mut request = lifecycle_request();
    request.receipt = Some(lifecycle_receipt());
    request.now_unix = 100_000.0; // long past deadline + grace_seconds
    let decision = decide_gate_lifecycle(&request).unwrap();
    assert_eq!(
        decision.disposition,
        GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED
    );
}

#[test]
fn cancellation_reason_selects_the_matching_disposition() {
    let mut timeout_request = lifecycle_request();
    timeout_request.cancellation_reason = Some("timeout".to_string());
    assert_eq!(
        decide_gate_lifecycle(&timeout_request).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_CANCELLED_TIMEOUT
    );

    let mut grace_request = lifecycle_request();
    grace_request.cancellation_reason = Some("grace_expired".to_string());
    assert_eq!(
        decide_gate_lifecycle(&grace_request).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_CANCELLED_LOST
    );

    let mut stopped_request = lifecycle_request();
    stopped_request.cancellation_reason =
        Some("cancelled via sase gate cancel".to_string());
    assert_eq!(
        decide_gate_lifecycle(&stopped_request).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_CANCELLED_STOPPED
    );
}

#[test]
fn deadline_math_only_applies_once_response_receipt_and_cancellation_are_absent(
) {
    let mut pending = lifecycle_request();
    pending.now_unix = 500.0;
    assert_eq!(
        decide_gate_lifecycle(&pending).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_PENDING
    );

    let mut no_deadline = lifecycle_request();
    no_deadline.deadline_unix = None;
    assert_eq!(
        decide_gate_lifecycle(&no_deadline).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_PENDING
    );

    let mut expired_review = lifecycle_request();
    expired_review.now_unix = 1_100.0; // past deadline (1_000), inside +300s grace
    assert_eq!(
        decide_gate_lifecycle(&expired_review).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW
    );

    let mut expired_grace = lifecycle_request();
    expired_grace.now_unix = 1_301.0; // past deadline (1_000) + grace (300)
    assert_eq!(
        decide_gate_lifecycle(&expired_grace).unwrap().disposition,
        GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE
    );
}

#[test]
fn lifecycle_decides_from_json_and_rejects_an_unsupported_schema_version() {
    let value = json!({
        "schema_version": GATE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        "gate_id": "gate-xyz",
        "request_hash": "sha256:abc",
        "now_unix": 10.0,
        "grace_seconds": 60.0,
        "has_response": true,
    });
    let decision = decide_gate_lifecycle_from_json(&value).unwrap();
    assert_eq!(decision.disposition, GATE_LIFECYCLE_DISPOSITION_ANSWERED);

    let bad = json!({
        "schema_version": 999,
        "gate_id": "gate-xyz",
        "request_hash": "sha256:abc",
        "now_unix": 10.0,
        "grace_seconds": 60.0,
        "has_response": false,
    });
    let error = decide_gate_lifecycle_from_json(&bad).unwrap_err();
    assert_eq!(error.code, GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA);
}
