use serde_json::{json, Value as JsonValue};

use super::{
    evaluate_stage_admission, receipt_authorizes,
    staged_handoff_wire_schema_version, validate_staged_handoff,
    validate_staged_handoff_receipt, StagedHandoffError,
    STAGED_HANDOFF_WIRE_SCHEMA_VERSION,
};

const STAGED_ID: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

fn plan_record() -> JsonValue {
    json!({
        "schema_version": 1,
        "staged_id": STAGED_ID,
        "kind": "plan",
        "created_at": "2026-10-28T12:00:00Z",
        "payload": {
            "plan_file": "/tmp/archive/plan.md",
            "original_file": "/tmp/scratch/plan.md",
            "proposed_by": "agent-1"
        },
        "successor_workspace": "none",
        "carry_allowed": false,
        "dirty_obligations_at_stage": ["repo:main"]
    })
}

fn adopted_receipt() -> JsonValue {
    json!({
        "staged_id": STAGED_ID,
        "adopted_at": "2026-10-28T12:01:00Z",
        "outcome": "adopted",
        "refs": {"gate_id": "gate-1"}
    })
}

fn admission(
    occupying: Vec<&str>,
    has_accepted_current_submission: bool,
) -> JsonValue {
    json!({
        "schema_version": 1,
        "command": "sase plan propose",
        "is_agent": true,
        "artifacts_dir_present": true,
        "is_finalizer_owned": false,
        "is_native_helper": false,
        "occupying": occupying,
        "has_accepted_current_submission": has_accepted_current_submission
    })
}

#[test]
fn wire_schema_version_is_one() {
    assert_eq!(STAGED_HANDOFF_WIRE_SCHEMA_VERSION, 1);
    assert_eq!(staged_handoff_wire_schema_version(), 1);
}

#[test]
fn valid_plan_record_round_trips() {
    let record = plan_record();
    let normalized =
        validate_staged_handoff(&record).expect("valid plan must normalize");
    assert_eq!(normalized, record);
}

#[test]
fn questions_and_pipe_require_inherit_and_carry() {
    for kind in ["questions", "pipe"] {
        let mut record = plan_record();
        record["kind"] = json!(kind);
        record["successor_workspace"] = json!("inherit");
        record["carry_allowed"] = json!(true);
        validate_staged_handoff(&record)
            .unwrap_or_else(|_| panic!("{kind} must validate"));
    }
}

#[test]
fn monitor_requires_inherit() {
    let mut record = plan_record();
    record["kind"] = json!("monitor");
    record["successor_workspace"] = json!("inherit");
    record["carry_allowed"] = json!(true);
    validate_staged_handoff(&record).expect("monitor must validate");
}

#[test]
fn gate_none_forbids_carry() {
    let mut record = plan_record();
    record["kind"] = json!("gate");
    record["successor_workspace"] = json!("none");
    record["carry_allowed"] = json!(true);
    let error = validate_staged_handoff(&record).expect_err("gate none+carry");
    assert!(error.to_string().contains("carry_allowed"));
}

#[test]
fn plan_rejects_carry_or_inherit() {
    let mut record = plan_record();
    record["carry_allowed"] = json!(true);
    let error = validate_staged_handoff(&record).expect_err("plan carry");
    assert!(error.to_string().contains("carry_allowed"));

    record = plan_record();
    record["successor_workspace"] = json!("inherit");
    let error = validate_staged_handoff(&record).expect_err("plan inherit");
    assert!(error.to_string().contains("successor_workspace none"));
}

#[test]
fn plan_requires_payload_paths() {
    let mut record = plan_record();
    record["payload"]
        .as_object_mut()
        .expect("payload")
        .remove("plan_file");
    let error =
        validate_staged_handoff(&record).expect_err("missing plan_file");
    assert!(error.to_string().contains("plan_file"));
}

#[test]
fn malformed_uuid_and_timestamp_are_invalid() {
    let mut record = plan_record();
    record["staged_id"] = json!("not-a-uuid");
    let error = validate_staged_handoff(&record).expect_err("bad uuid");
    assert!(error.to_string().contains("UUID"));

    record = plan_record();
    record["created_at"] = json!("yesterday");
    let error = validate_staged_handoff(&record).expect_err("bad timestamp");
    assert!(error.to_string().contains("RFC3339"));
}

#[test]
fn unknown_fields_and_schema_are_rejected() {
    let mut record = plan_record();
    record["extra"] = json!(true);
    validate_staged_handoff(&record).expect_err("unknown field");

    record = plan_record();
    record["schema_version"] = json!(0);
    let error = validate_staged_handoff(&record).expect_err("schema 0");
    assert_eq!(error, StagedHandoffError::UnsupportedSchema(0));
}

#[test]
fn empty_obligation_ids_are_rejected() {
    let mut record = plan_record();
    record["dirty_obligations_at_stage"] = json!([""]);
    let error = validate_staged_handoff(&record).expect_err("empty obligation");
    assert!(error.to_string().contains("dirty_obligations_at_stage"));
}

#[test]
fn adopted_receipt_round_trips_and_authorizes() {
    let receipt = adopted_receipt();
    let normalized = validate_staged_handoff_receipt(&receipt)
        .expect("valid receipt must normalize");
    assert_eq!(normalized, receipt);
    assert!(receipt_authorizes(&receipt, STAGED_ID).expect("authorizes"));
    assert!(!receipt_authorizes(
        &receipt,
        "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    )
    .expect("other id"));
}

#[test]
fn held_receipt_requires_reason_and_does_not_authorize() {
    let receipt = json!({
        "staged_id": STAGED_ID,
        "adopted_at": "2026-10-28T12:01:00Z",
        "outcome": "handoff_not_adopted",
        "reason": "commit failed"
    });
    validate_staged_handoff_receipt(&receipt).expect("held receipt");
    assert!(!receipt_authorizes(&receipt, STAGED_ID).expect("held"));

    let mut missing_reason = receipt.clone();
    missing_reason
        .as_object_mut()
        .expect("object")
        .remove("reason");
    let error = validate_staged_handoff_receipt(&missing_reason)
        .expect_err("held without reason");
    assert!(error.to_string().contains("reason"));
}

#[test]
fn admission_allows_a_clean_root_turn() {
    evaluate_stage_admission(&admission(Vec::new(), false))
        .expect("clean turn must stage");
}

#[test]
fn admission_refuses_occupying_artifacts() {
    let error = evaluate_stage_admission(&admission(
        vec![".sase_plan_pending", ".sase_handoff_staged.json"],
        false,
    ))
    .expect_err("occupying");
    assert!(error.to_string().contains(".sase_plan_pending"));
    assert!(error.to_string().contains(".sase_handoff_staged.json"));
}

#[test]
fn admission_refuses_accepted_current_submission() {
    let error = evaluate_stage_admission(&admission(Vec::new(), true))
        .expect_err("accepted submission");
    assert_eq!(error, StagedHandoffError::AcceptedSubmission);
    assert!(error.to_string().contains("/sase_final"));
}

#[test]
fn admission_refuses_helpers_and_finalizer_turns() {
    let mut request = admission(Vec::new(), false);
    request["is_native_helper"] = json!(true);
    let error = evaluate_stage_admission(&request).expect_err("helper");
    assert!(error.to_string().contains("native helper"));

    request = admission(Vec::new(), false);
    request["is_finalizer_owned"] = json!(true);
    let error = evaluate_stage_admission(&request).expect_err("finalizer");
    assert!(error.to_string().contains("host finalizer turn"));
}

#[test]
fn admission_refuses_missing_agent_or_artifacts() {
    let mut request = admission(Vec::new(), false);
    request["is_agent"] = json!(false);
    assert_eq!(
        evaluate_stage_admission(&request).expect_err("agent"),
        StagedHandoffError::MissingAgent
    );

    request = admission(Vec::new(), false);
    request["artifacts_dir_present"] = json!(false);
    assert_eq!(
        evaluate_stage_admission(&request).expect_err("artifacts"),
        StagedHandoffError::MissingArtifactsDir
    );
}
