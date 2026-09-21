use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn remaining_commit_work_binding_selects_and_rejects() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let plan =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let digest =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let commit_sha = "a".repeat(40);
        let request = json_value_to_py(
            py,
            &json!({
                "current_run_id": "run-1",
                "current_agent_id": "agent-1",
                "current_turn_nonce": "nonce-1",
                "current_plan_digest": plan,
                "declaration_run_id": "run-1",
                "declaration_agent_id": "agent-1",
                "declaration_turn_nonce": "nonce-1",
                "declaration_plan_digest": plan,
                "current_obligations": [
                    {
                        "obligation_id": "linked",
                        "kind": "repository",
                        "current_digest": digest,
                        "submitted_digest": digest,
                        "has_host_identity": true,
                        "host_identity_matches": true,
                        "has_valid_decision": true,
                    }
                ],
                "executed_obligations": [
                    {
                        "obligation_id": "main",
                        "completed": true,
                        "commit_sha": commit_sha,
                    }
                ],
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result =
            py_select_remaining_commit_obligations(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["status"], "remaining");
        assert_eq!(result["obligation_ids"], json!(["linked"]));

        let rejected = json_value_to_py(
            py,
            &json!({
                "current_run_id": "run-1",
                "current_agent_id": "agent-1",
                "current_turn_nonce": "nonce-1",
                "current_plan_digest": plan,
                "declaration_run_id": "run-1",
                "declaration_agent_id": "agent-1",
                "declaration_turn_nonce": "other-turn",
                "declaration_plan_digest": plan,
                "current_obligations": [
                    {
                        "obligation_id": "linked",
                        "kind": "repository",
                        "has_host_identity": true,
                        "host_identity_matches": true,
                        "has_valid_decision": true,
                    }
                ],
                "executed_obligations": [],
            }),
        )
        .unwrap();
        let rejected = rejected.bind(py).downcast::<PyDict>().unwrap();
        let result =
            py_select_remaining_commit_obligations(py, rejected).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["status"], "rejected");
        assert_eq!(result["code"], "repair_handoff_identity_mismatch");
    });
}

fn incident_gate_followup_request_json() -> JsonValue {
    json!({
        "schema_version": GATE_FOLLOWUP_WIRE_SCHEMA_VERSION,
        "mode": "diagnose",
        "gate_id": "c117f874-83de-4840-8405-58a8dc1efd66",
        "gate_kind": "plan",
        "gate_state": "answered",
        "already_settled": true,
        "request_fingerprint": "sha256:plan-approve-commit",
        "followup_requested": true,
        "creator_live": false,
        "auto_suppressed": false,
    })
}

#[test]
fn gate_followup_binding_round_trips_incident_disposition() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_gate_followup_wire_schema_version(),
            GATE_FOLLOWUP_WIRE_SCHEMA_VERSION
        );
        let request_obj =
            json_value_to_py(py, &incident_gate_followup_request_json())
                .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_decide_gate_followup(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["disposition"], json!("interrupted"));
        assert_eq!(value["recovery"], json!("resume"));
        assert_eq!(value["needs_attention"], json!(true));
        assert_eq!(value["launch_allowed"], json!(false));
        assert_eq!(value["resume_eligible"], json!(true));
        let attempt_id = py_gate_followup_attempt_id(
            "c117f874-83de-4840-8405-58a8dc1efd66",
            "sha256:plan-approve-commit",
        );
        assert_eq!(attempt_id.len(), 64);
    });
}

#[test]
fn gate_followup_binding_maps_schema_and_structural_errors_to_value_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut schema = incident_gate_followup_request_json();
        schema["schema_version"] = json!(GATE_FOLLOWUP_WIRE_SCHEMA_VERSION + 1);

        let mut structural = incident_gate_followup_request_json();
        structural["mode"] = json!("replay");

        let mut unknown = incident_gate_followup_request_json();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("surprise".to_string(), json!(true));

        for (value, expected) in [
            (schema, "schema_version_mismatch"),
            (structural, "invalid_mode"),
            (unknown, "unknown field `surprise`"),
        ] {
            let request_obj = json_value_to_py(py, &value).unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let error = py_decide_gate_followup(py, request).unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(error.to_string().contains(expected), "{}", error);
        }
    });
}

#[test]
fn finalizer_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "finalizer_wire_schema_version",
            "validate_finalizer_provider_spec",
            "finalizer_provider_spec_digest",
            "validate_finalizer_instance_spec",
            "finalizer_instance_spec_digest",
            "resolve_finalizer_plan",
            "finalizer_plan_digest",
            "validate_finalizer_plan",
            "authenticate_finalizer_plan",
            "finalizer_context_digest",
            "validate_finalizer_context",
            "validate_finalizer_submission",
            "finalizer_json_digest",
            "aggregate_finalizer_outcomes",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let provider_value = json!({
            "schema_version": 2,
            "provider_ref": "builtin@commit",
            "capabilities": ["validate", "execute", "verify"],
            "provenance_id": "builtin"
        });
        let provider_obj = json_value_to_py(py, &provider_value).unwrap();
        let provider = provider_obj.bind(py).downcast::<PyDict>().unwrap();
        py_validate_finalizer_provider_spec(provider).unwrap();
        assert_eq!(
            py_finalizer_provider_spec_digest(provider).unwrap().len(),
            64
        );
        assert_eq!(py_finalizer_wire_schema_version(), 2);

        let request_value = json!({
            "schema_version": 2,
            "instances": [
                {
                    "schema_version": 2,
                    "instance_id": "commit",
                    "provider_ref": "builtin@commit",
                    "after": ["lint"],
                    "policy": {"max_attempts": 2, "refusal": "fail"}
                },
                {
                    "schema_version": 2,
                    "instance_id": "lint",
                    "provider_ref": "builtin@command",
                    "after": [],
                    "policy": {"max_attempts": 1, "refusal": "fail"}
                }
            ],
            "defaults": ["commit"],
            "required": [],
            "selectors": [{"op": "add", "instance_id": "lint"}]
        });
        let request_obj = json_value_to_py(py, &request_value).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let plan_obj = py_resolve_finalizer_plan(py, request).unwrap();
        let plan_json = py_to_json_value(plan_obj.bind(py)).unwrap();
        assert_eq!(plan_json["entries"][0]["instance_id"], json!("lint"));
        assert_eq!(plan_json["entries"][1]["instance_id"], json!("commit"));
        let plan = plan_obj.bind(py).downcast::<PyDict>().unwrap();
        assert_eq!(
            py_finalizer_plan_digest(plan).unwrap(),
            plan_json["plan_digest"].as_str().unwrap()
        );
        let plan_digest = plan_json["plan_digest"].as_str().unwrap();
        assert_eq!(py_validate_finalizer_plan(plan).unwrap(), plan_digest);
        assert_eq!(
            py_authenticate_finalizer_plan(plan, plan_digest).unwrap(),
            plan_digest
        );

        let mut context_value = json!({
            "schema_version": 2,
            "run_id": "run-1",
            "agent_id": "agent-1",
            "turn_nonce": "nonce-1",
            "plan_digest": plan_json["plan_digest"],
            "requirements": [
                {
                    "instance_id": "lint",
                    "trigger": "always",
                    "submission_required": false
                },
                {
                    "instance_id": "commit",
                    "trigger": "dirty_repository",
                    "submission_required": true
                }
            ],
            "obligations": []
        });
        let context_obj = json_value_to_py(py, &context_value).unwrap();
        let context = context_obj.bind(py).downcast::<PyDict>().unwrap();
        let context_digest = py_finalizer_context_digest(context).unwrap();
        context_value["context_digest"] = json!(context_digest);
        let context_obj = json_value_to_py(py, &context_value).unwrap();
        let context = context_obj.bind(py).downcast::<PyDict>().unwrap();
        assert_eq!(
            py_validate_finalizer_context(plan, context).unwrap(),
            context_value["context_digest"].as_str().unwrap()
        );

        let submission_value = json!({
            "schema_version": 2,
            "run_id": "run-1",
            "agent_id": "agent-1",
            "turn_nonce": "nonce-1",
            "plan_digest": plan_json["plan_digest"],
            "context_digest": context_value["context_digest"],
            "payloads": [{
                "instance_id": "commit",
                "payload": {"repositories": []}
            }]
        });
        let submission_obj = json_value_to_py(py, &submission_value).unwrap();
        let submission = submission_obj.bind(py).downcast::<PyDict>().unwrap();
        let validation =
            py_validate_finalizer_submission(py, plan, context, submission)
                .unwrap();
        let validation = py_to_json_value(validation.bind(py)).unwrap();
        assert_eq!(validation["accepted_instances"], json!(["commit"]));
        assert_eq!(validation["submission_digest"].as_str().unwrap().len(), 64);

        let aggregate_input = json_value_to_py(
            py,
            &json!([
                {"instance_id": "lint", "status": "success"},
                {"instance_id": "commit", "status": "success"}
            ]),
        )
        .unwrap();
        let aggregate = py_aggregate_finalizer_outcomes(
            py,
            aggregate_input.bind(py).downcast::<PyList>().unwrap(),
        )
        .unwrap();
        let aggregate = py_to_json_value(aggregate.bind(py)).unwrap();
        assert_eq!(aggregate["status"], json!("success"));

        let without_bead = context_digest.clone();
        context_value["assigned_bead"] = json!({
            "bead_id": "sase-zq.1",
            "primary_repo_obligation_id": "repo:primary"
        });
        let associated_obj = json_value_to_py(py, &context_value).unwrap();
        let associated = associated_obj.bind(py).downcast::<PyDict>().unwrap();
        let associated_digest =
            py_finalizer_context_digest(associated).unwrap();
        assert_ne!(without_bead, associated_digest);
    });
}

#[test]
fn bead_action_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "bead_action_wire_schema_version",
            "parse_bead_action_field",
            "decide_bead_action",
            "validate_finalizer_bead_decision",
            "validate_finalizer_assigned_bead_binding",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_bead_action_wire_schema_version(), 1);

        let omitted = json_value_to_py(py, &json!({})).unwrap();
        assert_eq!(
            py_parse_bead_action_field(
                omitted.bind(py).downcast::<PyDict>().unwrap()
            )
            .unwrap(),
            None
        );
        let keep =
            json_value_to_py(py, &json!({"bead_action": "keep"})).unwrap();
        assert_eq!(
            py_parse_bead_action_field(
                keep.bind(py).downcast::<PyDict>().unwrap()
            )
            .unwrap()
            .as_deref(),
            Some("keep")
        );
        let invalid =
            json_value_to_py(py, &json!({"bead_action": true})).unwrap();
        assert!(py_parse_bead_action_field(
            invalid.bind(py).downcast::<PyDict>().unwrap()
        )
        .is_err());

        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "assigned_bead_id": "sase-zq.1",
                "commit_method": "create_commit",
                "repository_scope": "primary",
                "primary_repository_identified": true,
                "bead_action": "keep"
            }),
        )
        .unwrap();
        let decision = py_decide_bead_action(
            py,
            request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let decision = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(decision["disposition"], json!("keep"));
        assert_eq!(decision["close_bead"], json!(false));

        let context = json_value_to_py(
            py,
            &json!({
                "schema_version": 2,
                "run_id": "run-1",
                "agent_id": "agent-1",
                "turn_nonce": "nonce-1",
                "plan_digest": "d".repeat(64),
                "requirements": [],
                "obligations": [{
                    "obligation_id": "repo:primary",
                    "kind": "repository",
                    "paths": ["."]
                }],
                "assigned_bead": {
                    "bead_id": "sase-zq.1",
                    "primary_repo_obligation_id": "repo:primary"
                }
            }),
        )
        .unwrap();
        let close_decision = json_value_to_py(
            py,
            &json!({
                "repo_id": "repo:primary",
                "commit_method": "create_commit",
                "bead_action": "close",
                "bead_status": "in_progress"
            }),
        )
        .unwrap();
        let close = py_validate_finalizer_bead_decision(
            py,
            context.bind(py).downcast::<PyDict>().unwrap(),
            close_decision.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let close = py_to_json_value(close.bind(py)).unwrap();
        assert_eq!(close["disposition"], json!("close"));
        assert_eq!(close["close_bead"], json!(true));

        let expected = json_value_to_py(
            py,
            &json!({
                "bead_id": "sase-other",
                "primary_repo_obligation_id": "repo:primary"
            }),
        )
        .unwrap();
        assert!(py_validate_finalizer_assigned_bead_binding(
            context.bind(py).downcast::<PyDict>().unwrap(),
            Some(expected.bind(py).downcast::<PyDict>().unwrap()),
        )
        .is_err());
        let matching = json_value_to_py(
            py,
            &json!({
                "bead_id": "sase-zq.1",
                "primary_repo_obligation_id": "repo:primary"
            }),
        )
        .unwrap();
        py_validate_finalizer_assigned_bead_binding(
            context.bind(py).downcast::<PyDict>().unwrap(),
            Some(matching.bind(py).downcast::<PyDict>().unwrap()),
        )
        .unwrap();
    });
}

#[test]
fn gate_decision_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "gate_decision_wire_schema_version",
            "decide_gate_decision_acceptance",
            "claim_gate_decision_execution",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_gate_decision_wire_schema_version(), 1);

        let fresh_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "selected_option_ids": ["approve"],
                "input_identity": "sha256:input",
                "acceptance_id": "acceptance-a",
                "source": "cli",
                "accepted_at_unix": 1_726_000_000.0,
                "execution_owner": "attempt:1234",
            }),
        )
        .unwrap();
        let accepted = py_decide_gate_decision_acceptance(
            py,
            fresh_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let accepted = py_to_json_value(accepted.bind(py)).unwrap();
        assert_eq!(accepted["status"], json!("accepted"));
        let receipt = accepted["receipt"].clone();
        assert_eq!(receipt["gate_id"], json!("gate-abc"));
        assert_eq!(receipt["acceptance_id"], json!("acceptance-a"));
        assert!(!receipt["identity_fingerprint"].as_str().unwrap().is_empty());

        let mut replay_value = json!({
            "schema_version": 1,
            "gate_id": "gate-abc",
            "request_hash": "sha256:deadbeef",
            "selected_option_ids": ["approve"],
            "input_identity": "sha256:input",
            "acceptance_id": "acceptance-a-replay",
            "source": "ace",
            "accepted_at_unix": 1_726_000_500.0,
            "execution_owner": "attempt:5678",
        });
        replay_value["existing_receipt"] = receipt.clone();
        let replay_request = json_value_to_py(py, &replay_value).unwrap();
        let replayed = py_decide_gate_decision_acceptance(
            py,
            replay_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let replayed = py_to_json_value(replayed.bind(py)).unwrap();
        assert_eq!(replayed["status"], json!("replayed"));
        assert_eq!(
            replayed["receipt"], receipt,
            "replay returns the original receipt unmodified"
        );

        let mut conflict_value = json!({
            "schema_version": 1,
            "gate_id": "gate-abc",
            "request_hash": "sha256:deadbeef",
            "selected_option_ids": ["reject"],
            "input_identity": "sha256:input",
            "acceptance_id": "acceptance-conflict",
            "source": "cli",
            "accepted_at_unix": 1_726_000_600.0,
        });
        conflict_value["existing_receipt"] = receipt.clone();
        let conflict_request = json_value_to_py(py, &conflict_value).unwrap();
        let error = py_decide_gate_decision_acceptance(
            py,
            conflict_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("gate_decision_conflict"));

        let mut supersede_value = json!({
            "schema_version": 1,
            "gate_id": "gate-abc",
            "request_hash": "sha256:deadbeef",
            "selected_option_ids": ["reject"],
            "input_identity": "sha256:input",
            "acceptance_id": "acceptance-b",
            "source": "cli",
            "accepted_at_unix": 1_726_000_700.0,
            "execution_facts": {
                "current_failure": {
                    "outcome_id": "outcome-1",
                    "acceptance_id": "acceptance-a",
                    "attempt_id": "attempt-1",
                    "stage": "command",
                    "code": "command_failed",
                    "message": "option approve failed with exit status 1",
                    "at_unix": 1_726_000_650.0,
                    "error_record": "errors/outcome-1.json"
                }
            }
        });
        supersede_value["existing_receipt"] = receipt.clone();
        let supersede_request = json_value_to_py(py, &supersede_value).unwrap();
        let superseded = py_decide_gate_decision_acceptance(
            py,
            supersede_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let superseded = py_to_json_value(superseded.bind(py)).unwrap();
        assert_eq!(superseded["status"], json!("superseded"));
        assert_eq!(
            superseded["receipt"]["acceptance_id"],
            json!("acceptance-b")
        );
        assert_eq!(
            superseded["superseded_receipt"]["acceptance_id"],
            json!("acceptance-a")
        );
        assert_eq!(superseded["owner_lost"], json!(false));
        assert_eq!(superseded["failure"]["outcome_id"], json!("outcome-1"));

        let claim_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "acceptance_id": "acceptance-a",
                "receipt": receipt,
                "execution_owner": {
                    "kind": "process",
                    "host": "apollo",
                    "pid": 9001,
                    "started_at_unix": 1_726_000_710.0,
                    "identity_token": "boot-b:9001"
                }
            }),
        )
        .unwrap();
        let claimed = py_claim_gate_decision_execution(
            py,
            claim_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let claimed = py_to_json_value(claimed.bind(py)).unwrap();
        assert_eq!(
            claimed["receipt"]["execution_owner"]["kind"],
            json!("process")
        );
    });
}

#[test]
fn gate_lifecycle_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "gate_lifecycle_wire_schema_version",
            "decide_gate_lifecycle",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_gate_lifecycle_wire_schema_version(), 1);

        let answered_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "now_unix": 2_000.0,
                "grace_seconds": 300.0,
                "has_response": true,
            }),
        )
        .unwrap();
        let answered = py_decide_gate_lifecycle(
            py,
            answered_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let answered = py_to_json_value(answered.bind(py)).unwrap();
        assert_eq!(answered["disposition"], json!("answered"));

        let answered_with_post_response_failure_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "now_unix": 2_000.0,
                "grace_seconds": 300.0,
                "has_response": true,
                "receipt": {
                    "schema_version": 1,
                    "gate_id": "gate-abc",
                    "request_hash": "sha256:deadbeef",
                    "selected_option_ids": ["approve"],
                    "input_identity": "sha256:input",
                    "acceptance_id": "acceptance-a",
                    "source": "cli",
                    "accepted_at_unix": 1_726_000_000.0,
                    "identity_fingerprint": "fingerprint",
                },
                "execution_facts": {
                    "post_response_failure": {
                        "outcome_id": "outcome-follow-up",
                        "acceptance_id": "acceptance-a",
                        "attempt_id": "attempt-1",
                        "stage": "follow_up",
                        "code": "follow_up_failed",
                        "message": "follow-up failed",
                        "at_unix": 1_726_000_050.0,
                        "error_record": "errors/outcome-follow-up.json"
                    }
                }
            }),
        )
        .unwrap();
        let answered_with_post_response_failure = py_decide_gate_lifecycle(
            py,
            answered_with_post_response_failure_request
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),
        )
        .unwrap();
        let answered_with_post_response_failure =
            py_to_json_value(answered_with_post_response_failure.bind(py))
                .unwrap();
        assert_eq!(
            answered_with_post_response_failure["disposition"],
            json!("answered")
        );
        assert_eq!(
            answered_with_post_response_failure["failure"]["stage"],
            json!("follow_up")
        );

        let accepted_unfinished_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "now_unix": 1_000_000.0,
                "deadline_unix": 1_000.0,
                "grace_seconds": 300.0,
                "has_response": false,
                "receipt": {
                    "schema_version": 1,
                    "gate_id": "gate-abc",
                    "request_hash": "sha256:deadbeef",
                    "selected_option_ids": ["approve"],
                    "input_identity": "sha256:input",
                    "acceptance_id": "acceptance-a",
                    "source": "cli",
                    "accepted_at_unix": 1_726_000_000.0,
                    "identity_fingerprint": "fingerprint",
                },
            }),
        )
        .unwrap();
        let accepted_unfinished = py_decide_gate_lifecycle(
            py,
            accepted_unfinished_request
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),
        )
        .unwrap();
        let accepted_unfinished =
            py_to_json_value(accepted_unfinished.bind(py)).unwrap();
        assert_eq!(
            accepted_unfinished["disposition"],
            json!("accepted_unfinished"),
            "a verified receipt outranks the review deadline and grace window"
        );
        assert_eq!(accepted_unfinished["can_cancel"], json!(false));
        assert_eq!(accepted_unfinished["can_supersede"], json!(false));
        assert_eq!(accepted_unfinished["owner_liveness"], json!("unknown"));

        let accepted_failed_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "now_unix": 1_000_000.0,
                "deadline_unix": 1_000.0,
                "grace_seconds": 300.0,
                "has_response": false,
                "receipt": {
                    "schema_version": 1,
                    "gate_id": "gate-abc",
                    "request_hash": "sha256:deadbeef",
                    "selected_option_ids": ["approve"],
                    "input_identity": "sha256:input",
                    "acceptance_id": "acceptance-a",
                    "source": "cli",
                    "accepted_at_unix": 1_726_000_000.0,
                    "identity_fingerprint": "fingerprint",
                },
                "execution_facts": {
                    "current_failure": {
                        "outcome_id": "outcome-1",
                        "acceptance_id": "acceptance-a",
                        "attempt_id": "attempt-1",
                        "stage": "terminal_prepare",
                        "code": "archive_failed",
                        "message": "archive preparation failed",
                        "at_unix": 1_726_000_050.0,
                        "error_record": "errors/outcome-1.json"
                    }
                }
            }),
        )
        .unwrap();
        let accepted_failed = py_decide_gate_lifecycle(
            py,
            accepted_failed_request
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),
        )
        .unwrap();
        let accepted_failed =
            py_to_json_value(accepted_failed.bind(py)).unwrap();
        assert_eq!(accepted_failed["disposition"], json!("accepted_failed"));
        assert_eq!(accepted_failed["can_cancel"], json!(true));
        assert_eq!(accepted_failed["can_supersede"], json!(true));

        let mismatched_receipt_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "now_unix": 2_000.0,
                "grace_seconds": 300.0,
                "has_response": false,
                "receipt": {
                    "schema_version": 1,
                    "gate_id": "gate-someone-else",
                    "request_hash": "sha256:deadbeef",
                    "selected_option_ids": ["approve"],
                    "input_identity": "sha256:input",
                    "source": "cli",
                    "accepted_at_unix": 1_726_000_000.0,
                    "identity_fingerprint": "fingerprint",
                },
            }),
        )
        .unwrap();
        let error = py_decide_gate_lifecycle(
            py,
            mismatched_receipt_request
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("invalid_gate_decision_receipt"));

        let expired_grace_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "gate_id": "gate-abc",
                "request_hash": "sha256:deadbeef",
                "now_unix": 10_000.0,
                "deadline_unix": 1_000.0,
                "grace_seconds": 300.0,
                "has_response": false,
            }),
        )
        .unwrap();
        let expired_grace = py_decide_gate_lifecycle(
            py,
            expired_grace_request.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let expired_grace = py_to_json_value(expired_grace.bind(py)).unwrap();
        assert_eq!(expired_grace["disposition"], json!("expired_grace"));
    });
}

#[test]
fn task_type_spec_bindings_round_trip_validation_digest_and_snapshot() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "validate_task_type_spec",
            "task_type_spec_digest",
            "validate_task_type_field_values",
            "render_task_type_body",
            "parse_task_type_snapshot",
            "serialize_task_type_snapshot",
            "task_type_spec_wire_schema_version",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let spec_value = json!({
            "schema_version": 1,
            "task_type": "flake",
            "label": "Flaky test",
            "summary": "A test that fails and then passes on an unchanged tree.",
            "when_to_use": "File one when a test failed then passed.",
            "glyph": "≈",
            "accent_color": "#00D7D7",
            "fields": [
                {
                    "name": "node_id",
                    "type": "string",
                    "required": true,
                    "pattern": "\\S+::\\S+"
                },
                {
                    "name": "evidence",
                    "type": "string",
                    "required": true,
                    "role": ["template"]
                }
            ],
            "body_template": "## Flake report\n\n- **Test:** `{{ node_id }}`\n\n{{ evidence }}\n",
            "triage": {"min_plus_ones": 1}
        });
        let spec_object = json_value_to_py(py, &spec_value).unwrap();
        let spec = spec_object.bind(py).downcast::<PyDict>().unwrap();
        py_validate_task_type_spec(spec).unwrap();
        let digest = py_task_type_spec_digest(spec).unwrap();
        assert_eq!(digest.len(), 64);
        assert_eq!(py_task_type_spec_wire_schema_version(), 1);

        let mut with_refusal = spec_value.clone();
        with_refusal["create_refusal"] =
            json!("Agents never create this type.");
        let with_refusal_object = json_value_to_py(py, &with_refusal).unwrap();
        let with_refusal_spec =
            with_refusal_object.bind(py).downcast::<PyDict>().unwrap();
        py_validate_task_type_spec(with_refusal_spec).unwrap();
        let refusal_digest =
            py_task_type_spec_digest(with_refusal_spec).unwrap();
        assert_ne!(digest, refusal_digest);

        let mut omitted = spec_value.clone();
        if let Some(object) = omitted.as_object_mut() {
            object.remove("create_refusal");
        }
        let omitted_object = json_value_to_py(py, &omitted).unwrap();
        let omitted_spec =
            omitted_object.bind(py).downcast::<PyDict>().unwrap();
        assert_eq!(py_task_type_spec_digest(omitted_spec).unwrap(), digest);

        let values = BTreeMap::from([
            ("node_id".to_string(), "tests/foo.py::test_bar".to_string()),
            ("evidence".to_string(), "failed then passed".to_string()),
        ]);
        let field_errors =
            py_validate_task_type_field_values(py, spec, values.clone())
                .unwrap();
        let field_errors = py_to_json_value(field_errors.bind(py)).unwrap();
        assert_eq!(field_errors, json!([]));
        assert_eq!(
                py_render_task_type_body(spec, values).unwrap(),
                "## Flake report\n\n- **Test:** `tests/foo.py::test_bar`\n\nfailed then passed\n"
            );

        let missing =
            BTreeMap::from([("node_id".to_string(), "not-a-node".to_string())]);
        let field_errors =
            py_validate_task_type_field_values(py, spec, missing).unwrap();
        let field_errors = py_to_json_value(field_errors.bind(py)).unwrap();
        assert_eq!(field_errors[0]["kind"], json!("invalid_string_pattern"));
        assert_eq!(field_errors[1]["kind"], json!("missing_required"));

        let reserved = spec_value.clone();
        let reserved_object = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "task_type": "task",
                "label": reserved["label"],
                "summary": reserved["summary"],
                "when_to_use": reserved["when_to_use"],
            }),
        )
        .unwrap();
        let reserved_spec =
            reserved_object.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_validate_task_type_spec(reserved_spec).is_err());

        let flag_object = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "task_type": "flag",
                "label": reserved["label"],
                "summary": reserved["summary"],
                "when_to_use": reserved["when_to_use"],
            }),
        )
        .unwrap();
        let flag_spec = flag_object.bind(py).downcast::<PyDict>().unwrap();
        py_validate_task_type_spec(flag_spec).unwrap();

        let snapshot_value = json!({
            "types": [{
                "task_type": spec_value["task_type"],
                "label": spec_value["label"],
                "summary": spec_value["summary"],
                "when_to_use": spec_value["when_to_use"],
                "glyph": spec_value["glyph"],
                "accent_color": spec_value["accent_color"],
                "agent_creatable": true,
                "fields": spec_value["fields"],
                "body_template": spec_value["body_template"],
                "triage": spec_value["triage"],
                "source": "builtin",
                "package": "sase",
                "digest": digest
            }]
        });
        let snapshot_object = json_value_to_py(py, &snapshot_value).unwrap();
        let snapshot = snapshot_object.bind(py).downcast::<PyDict>().unwrap();
        let encoded = py_serialize_task_type_snapshot(snapshot).unwrap();
        assert!(encoded.ends_with('\n'));
        let parsed = py_parse_task_type_snapshot(py, &encoded).unwrap();
        let parsed = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(parsed["types"][0]["task_type"], json!("flake"));
        assert_eq!(parsed["types"][0]["digest"], json!(digest));
    });
}
