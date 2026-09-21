use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn continuation_contract_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "continuation_wire_schema_version",
            "continuation_validate_node",
            "continuation_validate_graph",
            "continuation_validate_agent_delta",
            "continuation_validate_intent",
            "continuation_validate_monitor_result",
            "continuation_validate_diagnostic_manifest",
            "continuation_validate_delivery_record",
            "continuation_new_delivery_record",
            "continuation_transition_delivery",
            "continuation_decide_resume_adoption",
            "continuation_plan_replay",
            "continuation_plan_retention",
            "continuation_select_evidence",
            "continuation_resolve_policy",
            "continuation_validate_policy",
            "continuation_freeze_policy",
            "continuation_plan_budget",
            "continuation_validate_conditional_completion",
            "continuation_seal_conditional_completion",
            "continuation_preview_conditional_completion",
            "continuation_bind_conditional_completion",
            "continuation_rollback_conditional_completion_binding",
            "continuation_evaluate_conditional_completion",
            "continuation_consume_conditional_completion",
            "continuation_invalidate_conditional_completion",
            "continuation_render_conditional_completion_message",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_continuation_wire_schema_version(), 1);

        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../../sase_core/tests/fixtures/continuation/serial_replay.json"
        ))
        .unwrap();
        let replay_request_obj =
            json_value_to_py(py, &fixture["request"]).unwrap();
        let replay_request =
            replay_request_obj.bind(py).downcast::<PyDict>().unwrap();
        let manifest = py_continuation_plan_replay(py, replay_request).unwrap();
        let manifest = py_to_json_value(manifest.bind(py)).unwrap();
        assert_eq!(
            manifest["ordered_node_ids"],
            fixture["expected_order"].clone()
        );
        assert_eq!(
            manifest["rendered_component_sizes"]["total_utf8_bytes"],
            fixture["expected_rendered_bytes"].clone()
        );

        let records_obj =
            json_value_to_py(py, &fixture["request"]["records"]).unwrap();
        let records = records_obj.bind(py).downcast::<PyList>().unwrap();
        let graph = py_continuation_validate_graph(py, records).unwrap();
        let graph = py_to_json_value(graph.bind(py)).unwrap();
        assert_eq!(graph["node_count"], json!(3));
        assert_eq!(graph["edge_count"], json!(2));

        let node_obj =
            json_value_to_py(py, &fixture["request"]["records"][0]).unwrap();
        let node = node_obj.bind(py).downcast::<PyDict>().unwrap();
        let validated_node = py_continuation_validate_node(py, node).unwrap();
        let validated_node = py_to_json_value(validated_node.bind(py)).unwrap();
        assert_eq!(validated_node["node_id"], json!("previous-result-1"));

        let bad_node_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "node_id": "bad-node",
                    "kind": "not_real",
                    "owner": {
                        "project": "sase",
                        "run_id": "run-1",
                        "agent_name": "agent-1"
                    },
                    "content_ref": "file:explicit:bad-node",
                    "content_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }),
            )
            .unwrap();
        let bad_node = bad_node_obj.bind(py).downcast::<PyDict>().unwrap();
        let bad_node_error =
            py_continuation_validate_node(py, bad_node).unwrap_err();
        assert!(bad_node_error.is_instance_of::<PyValueError>(py));

        let failed_result = json!({
            "schema_version": 1,
            "result_id": "result-1",
            "monitor_id": "monitor-1",
            "starter_execution_id": "run-1",
            "outcome": "failed",
            "exit_code": 1,
            "command": ["just", "check"],
            "cwd": "/repo",
            "started_at": "2026-09-11T10:00:00Z",
            "ended_at": "2026-09-11T10:01:00Z",
            "elapsed_ms": 60000,
            "workspace_identity": "workspace-19",
            "diagnostic_manifest_ref": "file:explicit:manifest",
            "retained_log": {
                "log_ref": "file:explicit:log",
                "local_locator": "monitor://monitor-1/log",
                "total_observed_bytes": 128,
                "retained_ranges": [{"start": 0, "end": 128}],
                "complete": true,
                "drain_confirmed": true
            }
        });
        let diagnostic_manifest = json!({
            "schema_version": 1,
            "producer": "run-silent",
            "stages": [{
                "stage_id": "mypy",
                "name": "Type checking",
                "status": "failed",
                "exit_code": 1,
                "diagnostic_refs": ["file:explicit:mypy"],
                "counts": {"errors": 2},
                "retained_ranges": [],
                "capture_errors": []
            }],
            "complete": true,
            "manifest_ref": "file:explicit:manifest"
        });
        let evidence_request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "result": failed_result,
                "policy": "auto",
                "historical_result": false,
                "diagnostic_manifest": diagnostic_manifest,
                "limits": {
                    "selected_diagnostics_bytes": 8192,
                    "fallback_tail_bytes": 4096,
                    "total_raw_excerpt_bytes": 12288,
                    "raw_tail_lines": 200
                }
            }),
        )
        .unwrap();
        let evidence_request =
            evidence_request_obj.bind(py).downcast::<PyDict>().unwrap();
        let evidence =
            py_continuation_select_evidence(py, evidence_request).unwrap();
        let evidence = py_to_json_value(evidence.bind(py)).unwrap();
        assert_eq!(evidence["context_kind"], json!("failed_diagnostics"));
        assert_eq!(evidence["include_raw_excerpt"], json!(false));
        assert_eq!(evidence["diagnostic_stage_ids"], json!(["mypy"]));

        let policy_request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "outcome": "completed",
                "profile": "verify",
                "prepared_completion_ref": "file:explicit:completion"
            }),
        )
        .unwrap();
        let policy_request =
            policy_request_obj.bind(py).downcast::<PyDict>().unwrap();
        let policy =
            py_continuation_resolve_policy(py, policy_request).unwrap();
        let policy = py_to_json_value(policy.bind(py)).unwrap();
        assert_eq!(policy["action"], json!("complete"));
        assert_eq!(policy["completion_ref"], json!("file:explicit:completion"));

        let budget_request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "rendered_prompt_bytes": 12000,
                    "essential_bytes": 6000,
                    "provider_budget": {
                        "context_limit_bytes": 10000,
                        "transport_limit_bytes": 9000,
                        "instruction_reserve_bytes": 1000
                    },
                    "reduction_candidates": [
                        {"kind": "checkpoint", "bytes": 5000, "checkpoint_ref": "file:explicit:checkpoint"},
                        {"kind": "identity_deduplication", "bytes": 2000}
                    ]
                }),
            )
            .unwrap();
        let budget_request =
            budget_request_obj.bind(py).downcast::<PyDict>().unwrap();
        let budget = py_continuation_plan_budget(py, budget_request).unwrap();
        let budget = py_to_json_value(budget.bind(py)).unwrap();
        assert_eq!(budget["kind"], json!("compact"));
        assert_eq!(
            budget["reductions"][0]["kind"],
            json!("identity_deduplication")
        );
    });
}
