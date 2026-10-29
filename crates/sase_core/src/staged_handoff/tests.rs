use serde_json::{json, Value as JsonValue};

use super::{
    build_gate_staged_payload, evaluate_stage_admission,
    gate_staged_payload_schema_version, receipt_authorizes,
    staged_handoff_wire_schema_version, validate_gate_staged_payload,
    validate_staged_handoff, validate_staged_handoff_receipt,
    StagedHandoffError, GATE_STAGED_PAYLOAD_SCHEMA_VERSION,
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
    let mut record = gate_record("release");
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

const GATE_REQUEST_ID: &str = "custom-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

fn gate_payload() -> JsonValue {
    json!({
        "gate_schema_version": 1,
        "request_id": GATE_REQUEST_ID,
        "spec": {
            "request_id": GATE_REQUEST_ID,
            "resources": [
                {
                    "path": "notes.md",
                    "role": "editable",
                    "executable": false,
                    "source": "/tmp/work/notes.md"
                },
                {
                    "path": "run.sh",
                    "role": "command",
                    "executable": true,
                    "source": "scripts/run.sh"
                },
                {
                    "path": "prompt.txt",
                    "role": "attachment",
                    "executable": false,
                    "content": "review this"
                }
            ]
        },
        "auto": {
            "decision": {"profile": "manual", "outcome": "ask"},
            "policy_block": {"kind": "custom"},
            "auto_execute": false
        },
        "resources": [
            {
                "path": "notes.md",
                "role": "editable",
                "executable": false,
                "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                "size_bytes": 3,
                "origin": "/tmp/work/notes.md",
                "staged_relpath": "snapshots/notes.md"
            },
            {
                "path": "run.sh",
                "role": "command",
                "executable": true,
                "sha256": "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9",
                "size_bytes": 11,
                "origin": "scripts/run.sh",
                "staged_relpath": "snapshots/run.sh"
            }
        ],
        "creator": {
            "project": "sase",
            "project_file": "/tmp/sase.project.json",
            "artifacts_dir": "/tmp/artifacts/20261029T000000Z",
            "timestamp": "20261029T000000Z",
            "agent": "agent-1",
            "agent_session": "agent-1--session",
            "workspace_num": 23,
            "runner_pid": 4242,
            "cl_name": "cl-1",
            "meta": {"name": "agent-1"}
        },
        "workspace_mode": "inherit"
    })
}

fn gate_record(mode: &str) -> JsonValue {
    let (successor_workspace, carry_allowed) = match mode {
        "release" => ("none", false),
        _ => ("inherit", true),
    };
    let mut payload = gate_payload();
    payload["workspace_mode"] = json!(mode);
    json!({
        "schema_version": 1,
        "staged_id": STAGED_ID,
        "kind": "gate",
        "created_at": "2026-10-28T12:00:00Z",
        "payload": payload,
        "successor_workspace": successor_workspace,
        "carry_allowed": carry_allowed,
        "dirty_obligations_at_stage": []
    })
}

#[test]
fn gate_payload_schema_version_is_one() {
    assert_eq!(GATE_STAGED_PAYLOAD_SCHEMA_VERSION, 1);
    assert_eq!(gate_staged_payload_schema_version(), 1);
}

#[test]
fn valid_gate_payload_round_trips() {
    let payload = gate_payload();
    let normalized = validate_gate_staged_payload(&payload)
        .expect("gate payload must validate");
    assert_eq!(normalized, payload);
}

#[test]
fn build_gate_payload_defaults_schema_version() {
    let mut payload = gate_payload();
    payload
        .as_object_mut()
        .expect("object")
        .remove("gate_schema_version");
    let normalized =
        build_gate_staged_payload(&payload).expect("build must default schema");
    assert_eq!(normalized["gate_schema_version"], json!(1));
    assert_eq!(
        validate_gate_staged_payload(&normalized).expect("built payload"),
        normalized
    );
}

#[test]
fn gate_payload_rejects_schema_and_unknown_fields() {
    let mut payload = gate_payload();
    payload["gate_schema_version"] = json!(0);
    let error = validate_gate_staged_payload(&payload).expect_err("schema 0");
    assert_eq!(error, StagedHandoffError::UnsupportedSchema(0));

    payload = gate_payload();
    payload["extra"] = json!(true);
    validate_gate_staged_payload(&payload).expect_err("unknown field");
}

#[test]
fn gate_payload_rejects_request_id_mismatch() {
    let mut payload = gate_payload();
    payload["request_id"] = json!("custom-other-id");
    let error = validate_gate_staged_payload(&payload)
        .expect_err("request id mismatch");
    assert!(error.to_string().contains("payload.spec.request_id"));

    payload = gate_payload();
    payload["spec"]["request_id"] = json!(JsonValue::Null);
    let error = validate_gate_staged_payload(&payload)
        .expect_err("missing spec request id");
    assert!(error.to_string().contains("payload.spec.request_id"));
}

#[test]
fn gate_payload_requires_snapshots_for_source_resources() {
    let mut payload = gate_payload();
    payload["resources"] = json!([]);
    let error =
        validate_gate_staged_payload(&payload).expect_err("missing snapshots");
    assert!(error.to_string().contains("has no resource snapshot"));

    payload = gate_payload();
    payload["resources"][0]["origin"] = json!("/tmp/other/notes.md");
    let error =
        validate_gate_staged_payload(&payload).expect_err("origin mismatch");
    assert!(error.to_string().contains("origin must preserve"));

    payload = gate_payload();
    payload["resources"][1]["executable"] = json!(false);
    let error = validate_gate_staged_payload(&payload)
        .expect_err("executable mismatch");
    assert!(error.to_string().contains("executable must match"));

    payload = gate_payload();
    payload["resources"][0]["sha256"] = json!("not-hex");
    let error = validate_gate_staged_payload(&payload).expect_err("bad sha256");
    assert!(error.to_string().contains("sha256"));

    payload = gate_payload();
    payload["resources"][0]["role"] = json!("bundle");
    let error = validate_gate_staged_payload(&payload).expect_err("bad role");
    assert!(error.to_string().contains("role"));

    payload = gate_payload();
    let snapshot = payload["resources"][0].clone();
    payload["spec"]["resources"]
        .as_array_mut()
        .expect("array")
        .retain(|resource| resource["path"] != json!("prompt.txt"));
    payload["resources"] = json!([
        snapshot,
        payload["resources"][1].clone(),
        {
            "path": "extra.bin",
            "role": "attachment",
            "executable": false,
            "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            "size_bytes": 3,
            "staged_relpath": "snapshots/extra.bin"
        }
    ]);
    let error =
        validate_gate_staged_payload(&payload).expect_err("orphan snapshot");
    assert!(error.to_string().contains("no matching spec resource"));

    payload = gate_payload();
    payload["spec"]["resources"][2]["source"] = json!("prompt.txt");
    let error = validate_gate_staged_payload(&payload)
        .expect_err("content plus source");
    assert!(error
        .to_string()
        .contains("exactly one of content or source"));

    payload = gate_payload();
    payload["spec"]["resources"][2] = json!({
        "path": "prompt.txt",
        "role": "attachment",
        "executable": false
    });
    let error = validate_gate_staged_payload(&payload)
        .expect_err("neither content nor source");
    assert!(error
        .to_string()
        .contains("exactly one of content or source"));
}

#[test]
fn gate_payload_rejects_inline_content_with_snapshot() {
    let mut payload = gate_payload();
    let snapshot = payload["resources"][0].clone();
    payload["spec"]["resources"][0] = json!({
        "path": "notes.md",
        "role": "editable",
        "executable": false,
        "content": "inline"
    });
    payload["resources"] = json!([snapshot, payload["resources"][1].clone()]);
    let error = validate_gate_staged_payload(&payload)
        .expect_err("content with snapshot");
    assert!(error.to_string().contains("inline content"));
}

#[test]
fn gate_payload_rejects_incomplete_creator() {
    for field in [
        "project",
        "project_file",
        "artifacts_dir",
        "timestamp",
        "agent",
    ] {
        let mut payload = gate_payload();
        payload["creator"][field] = json!("  ");
        let error = validate_gate_staged_payload(&payload)
            .expect_err("blank creator field");
        assert!(
            error.to_string().contains(&format!("creator.{field}")),
            "field {field}: {error}"
        );
    }
}

#[test]
fn valid_gate_records_round_trip_for_both_dispositions() {
    for mode in ["inherit", "release"] {
        let record = gate_record(mode);
        let normalized = validate_staged_handoff(&record)
            .unwrap_or_else(|_| panic!("gate {mode} must validate"));
        assert_eq!(normalized, record);
    }
}

#[test]
fn gate_record_rejects_disposition_mismatch() {
    let mut record = gate_record("inherit");
    record["successor_workspace"] = json!("none");
    record["carry_allowed"] = json!(false);
    let error = validate_staged_handoff(&record)
        .expect_err("inherit payload, none outer");
    assert!(error.to_string().contains("successor_workspace inherit"));

    record = gate_record("inherit");
    record["carry_allowed"] = json!(false);
    let error =
        validate_staged_handoff(&record).expect_err("inherit without carry");
    assert!(error.to_string().contains("carry_allowed"));

    record = gate_record("release");
    record["successor_workspace"] = json!("inherit");
    record["carry_allowed"] = json!(true);
    let error = validate_staged_handoff(&record)
        .expect_err("release payload, inherit outer");
    assert!(error.to_string().contains("successor_workspace none"));

    let mut payload = gate_payload();
    payload["workspace_mode"] = json!("release");
    let mut record = gate_record("release");
    record["payload"] = payload;
    record["carry_allowed"] = json!(true);
    let error =
        validate_staged_handoff(&record).expect_err("release with carry");
    assert!(error.to_string().contains("carry_allowed"));
}

#[test]
fn gate_record_rejects_malformed_payload() {
    let mut record = gate_record("inherit");
    record["payload"]["request_id"] = json!("other-id");
    let error = validate_staged_handoff(&record).expect_err("bad gate payload");
    assert!(error.to_string().contains("payload.spec.request_id"));
}
