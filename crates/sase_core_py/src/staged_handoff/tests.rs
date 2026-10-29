use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn plan_record() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "staged_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "kind": "plan",
        "created_at": "2026-10-28T12:00:00Z",
        "payload": {
            "plan_file": "/tmp/archive/plan.md",
            "original_file": "/tmp/scratch/plan.md"
        },
        "successor_workspace": "none",
        "carry_allowed": false,
        "dirty_obligations_at_stage": []
    })
}

#[test]
fn staged_handoff_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_staged_handoff(&module).unwrap();
        for name in [
            "staged_handoff_wire_schema_version",
            "validate_staged_handoff",
            "validate_staged_handoff_receipt",
            "evaluate_stage_admission",
            "staged_handoff_receipt_authorizes",
            "gate_staged_payload_schema_version",
            "validate_gate_staged_payload",
            "build_gate_staged_payload",
        ] {
            assert!(module.getattr(name).is_ok(), "{name}");
        }
        assert_eq!(py_staged_handoff_wire_schema_version(), 1);
        assert_eq!(py_gate_staged_payload_schema_version(), 1);
    });
}

fn gate_payload() -> serde_json::Value {
    json!({
        "gate_schema_version": 1,
        "request_id": "custom-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "spec": {
            "request_id": "custom-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            "resources": [
                {
                    "path": "notes.md",
                    "role": "editable",
                    "executable": false,
                    "source": "/tmp/work/notes.md"
                }
            ]
        },
        "auto": {
            "decision": {"profile": "manual"},
            "policy_block": {},
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
            }
        ],
        "creator": {
            "project": "sase",
            "project_file": "/tmp/sase.project.json",
            "artifacts_dir": "/tmp/artifacts/20261029T000000Z",
            "timestamp": "20261029T000000Z",
            "agent": "agent-1"
        },
        "workspace_mode": "inherit"
    })
}

#[test]
fn validate_staged_handoff_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let raw = plan_record();
        let record = json_value_to_py(py, &raw).unwrap();
        let record = record.bind(py).downcast::<PyDict>().unwrap();
        let result = py_validate_staged_handoff(py, record).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value, raw);
    });
}

#[test]
fn validate_staged_handoff_binding_rejects_plan_carry() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut raw = plan_record();
        raw["carry_allowed"] = json!(true);
        let record = json_value_to_py(py, &raw).unwrap();
        let record = record.bind(py).downcast::<PyDict>().unwrap();
        let error = py_validate_staged_handoff(py, record).unwrap_err();
        assert!(error.to_string().contains("carry_allowed"));
    });
}

#[test]
fn admission_binding_refuses_occupying_and_submission() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let occupying = json!({
            "schema_version": 1,
            "command": "sase plan propose",
            "is_agent": true,
            "artifacts_dir_present": true,
            "is_finalizer_owned": false,
            "is_native_helper": false,
            "occupying": [".sase_plan_pending"],
            "has_accepted_current_submission": false
        });
        let request = json_value_to_py(py, &occupying).unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let error = py_evaluate_stage_admission(request).unwrap_err();
        assert!(error.to_string().contains(".sase_plan_pending"));

        let submitted = json!({
            "schema_version": 1,
            "command": "sase plan propose",
            "is_agent": true,
            "artifacts_dir_present": true,
            "is_finalizer_owned": false,
            "is_native_helper": false,
            "occupying": [],
            "has_accepted_current_submission": true
        });
        let request = json_value_to_py(py, &submitted).unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let error = py_evaluate_stage_admission(request).unwrap_err();
        assert!(error.to_string().contains("/sase_final"));
    });
}

#[test]
fn receipt_authorizes_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let receipt = json!({
            "staged_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            "adopted_at": "2026-10-28T12:01:00Z",
            "outcome": "adopted",
            "refs": {"successor": "next"}
        });
        let py_receipt = json_value_to_py(py, &receipt).unwrap();
        let py_receipt = py_receipt.bind(py).downcast::<PyDict>().unwrap();
        let normalized =
            py_validate_staged_handoff_receipt(py, py_receipt).unwrap();
        let value = py_to_json_value(normalized.bind(py)).unwrap();
        assert_eq!(value, receipt);
        assert!(py_staged_handoff_receipt_authorizes(
            py_receipt,
            "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        )
        .unwrap());
        assert!(!py_staged_handoff_receipt_authorizes(
            py_receipt,
            "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        )
        .unwrap());
    });
}

#[test]
fn gate_payload_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let raw = gate_payload();
        let record = json_value_to_py(py, &raw).unwrap();
        let record = record.bind(py).downcast::<PyDict>().unwrap();
        let result = py_build_gate_staged_payload(py, record).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["gate_schema_version"], json!(1));
        let validated = {
            let normalized = json_value_to_py(py, &value).unwrap();
            let normalized = normalized.bind(py).downcast::<PyDict>().unwrap();
            let result =
                py_validate_gate_staged_payload(py, normalized).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(validated, value);
    });
}

#[test]
fn gate_payload_binding_rejects_request_id_mismatch() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut raw = gate_payload();
        raw["request_id"] = json!("custom-other-id");
        let record = json_value_to_py(py, &raw).unwrap();
        let record = record.bind(py).downcast::<PyDict>().unwrap();
        let error = py_build_gate_staged_payload(py, record).unwrap_err();
        assert!(error.to_string().contains("payload.spec.request_id"));

        let mut missing = gate_payload();
        missing["spec"]["resources"][0]
            .as_object_mut()
            .expect("object")
            .remove("source");
        let record = json_value_to_py(py, &missing).unwrap();
        let record = record.bind(py).downcast::<PyDict>().unwrap();
        let error = py_validate_gate_staged_payload(py, record).unwrap_err();
        assert!(error
            .to_string()
            .contains("exactly one of content or source"));
    });
}
