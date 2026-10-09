use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use pyo3::types::PyDict;
use serde_json::json;

fn py_dict<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> Bound<'py, PyDict> {
    json_value_to_py(py, value)
        .unwrap()
        .bind(py)
        .clone()
        .downcast_into::<PyDict>()
        .unwrap()
}

fn round_trip(py: Python<'_>, result: PyResult<PyObject>) -> serde_json::Value {
    py_to_json_value(result.unwrap().bind(py).as_any()).unwrap()
}

#[test]
fn autonomy_wire_schema_version_binding() {
    assert_eq!(py_autonomy_wire_schema_version(), 1);
}

#[test]
fn autonomy_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let resolve_request = py_dict(
            py,
            &json!({
                "selection": "tale",
                "source": "prompt",
                "actor": {
                    "kind": "human",
                    "surface": "prompt",
                    "principal": "bryan.zeus",
                },
                "now": "2026-10-09",
            }),
        );
        let record = round_trip(
            py,
            py_autonomy_resolve_selection(py, resolve_request.as_any()),
        );
        assert_eq!(record["profile"], json!("tale"));
        assert_eq!(record["selection"], json!("tale"));
        assert_eq!(record["revision"], json!(1));
        assert_eq!(record["schema_version"], json!(1));

        let record_arg = py_dict(py, &record);
        let evaluate_request = py_dict(
            py,
            &json!({
                "gate_kind": "plan",
                "option_ids": ["commit", "approve"],
                "capabilities": ["approve_archive", "approve", "first"],
            }),
        );
        let decision = round_trip(
            py,
            py_autonomy_evaluate(
                py,
                record_arg.as_any(),
                evaluate_request.as_any(),
            ),
        );
        assert_eq!(decision["outcome"], json!("auto"));
        assert_eq!(decision["value"], json!("approve_archive"));
        assert_eq!(decision["option_ids"], json!(["approve", "commit"]));

        let legacy_arg = py_dict(
            py,
            &json!({
                "auto_approve_plan_action": "epic",
                "auto_approve_argument": "epic",
                "plan": true,
            }),
        );
        let translated = round_trip(
            py,
            py_autonomy_record_from_legacy_meta(py, legacy_arg.as_any()),
        );
        assert_eq!(translated["profile"], json!("epic"));
        assert_eq!(translated["source"], json!("legacy"));

        let translated_arg = py_dict(py, &translated);
        let projection = round_trip(
            py,
            py_autonomy_legacy_projection(py, translated_arg.as_any()),
        );
        assert_eq!(projection["auto_approve_plan_action"], json!("epic"));
        assert_eq!(projection["prompt_mode"], json!("epic"));
    });
}

#[test]
fn autonomy_resolve_selection_rejects_bad_spellings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = py_dict(
            py,
            &json!({
                "selection": "foo",
                "source": "prompt",
                "actor": {"kind": "human"},
                "now": "",
            }),
        );
        let error =
            py_autonomy_resolve_selection(py, request.as_any()).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        assert!(error.to_string().contains("foo"));
    });
}
