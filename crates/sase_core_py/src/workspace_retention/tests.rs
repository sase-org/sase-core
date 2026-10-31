use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

fn retain_request() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "expected_claim": {
            "workspace_num": 17,
            "workflow": "ace(run)-260101_000000",
            "cl_name": "feature",
            "artifacts_timestamp": "20260101000000",
            "pid": 111
        },
        "observed_claim": {
            "workspace_num": 17,
            "workflow": "ace(run)-260101_000000",
            "cl_name": "feature",
            "artifacts_timestamp": "20260101000000",
            "pid": 111
        },
        "successor": {
            "confirmed_active": false,
            "same_workspace": false,
            "inherited_obligations": false
        },
        "creator_restored": false,
        "gate_released_workspace": false,
        "dirt": {
            "session_owned": true,
            "only_unchanged_foreign": false,
            "all_proven_clean": false,
            "missing_baseline": false,
            "positive_custody": true
        },
        "probe_failed": false
    })
}

#[test]
fn workspace_retention_binding_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(py, &retain_request()).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_decide_workspace_retention(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["disposition"], json!("retain"));
        assert_eq!(value["reason"], json!("session_owned_dirt"));
        assert!(result.bind(py).downcast::<PyDict>().is_ok());
    });
}

#[test]
fn workspace_retention_binding_registered_by_name() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let decide = module.getattr("decide_workspace_retention").unwrap();
        let version = module
            .getattr("workspace_retention_wire_schema_version")
            .unwrap();
        assert_eq!(version.call0().unwrap().extract::<u32>().unwrap(), 1);
        let request_obj = json_value_to_py(py, &retain_request()).unwrap();
        let result = decide.call1((request_obj,)).unwrap();
        let value = py_to_json_value(&result).unwrap();
        assert_eq!(value["disposition"], json!("retain"));
    });
}

#[test]
fn workspace_retention_binding_rejects_unknown_fields() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut request = retain_request();
        request
            .as_object_mut()
            .unwrap()
            .insert("display_visible".to_string(), json!(false));
        let request_obj = json_value_to_py(py, &request).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let error = py_decide_workspace_retention(py, request).unwrap_err();
        assert!(error.to_string().contains("WorkspaceRetentionRequestWire"));
    });
}
