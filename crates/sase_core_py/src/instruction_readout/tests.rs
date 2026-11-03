use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn instruction_readout_binding_is_registered_and_scores_evidence() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_instruction_readout(&module).unwrap();
        assert!(module.getattr("classify_instruction_readout").is_ok());

        for (declaration, dirty, expected) in [
            ("accepted", true, "plan_handoff"),
            ("not_required", true, "plan_handoff"),
            ("missing", false, "plan_handoff"),
            ("missing", true, "miss"),
        ] {
            let raw = json!({
                "handoff": "plan",
                "declaration": declaration,
                "has_dirty_obligations": dirty
            });
            let value = json_value_to_py(py, &raw).unwrap();
            let request = value.bind(py).downcast::<PyDict>().unwrap();
            let result = py_classify_instruction_readout(py, request).unwrap();
            assert_eq!(
                py_to_json_value(result.bind(py)).unwrap(),
                json!(expected)
            );
        }
    });
}

#[test]
fn instruction_readout_binding_rejects_unknown_evidence() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let raw = json!({
            "handoff": "future_kind",
            "declaration": "accepted",
            "has_dirty_obligations": false
        });
        let value = json_value_to_py(py, &raw).unwrap();
        let request = value.bind(py).downcast::<PyDict>().unwrap();
        let error = py_classify_instruction_readout(py, request).unwrap_err();
        assert!(error.to_string().contains("future_kind"));
    });
}
