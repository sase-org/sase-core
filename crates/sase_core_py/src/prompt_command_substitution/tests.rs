use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn request_dict<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> Bound<'py, PyDict> {
    json_value_to_py(py, value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

#[test]
fn prompt_command_substitution_binding_is_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_prompt_command_substitution(&module).unwrap();
        assert!(module.getattr("plan_prompt_command_substitution").is_ok());
        assert!(module
            .getattr("prompt_command_substitution_wire_schema_version")
            .is_ok());
        assert_eq!(
            py_prompt_command_substitution_wire_schema_version(),
            sase_core::prompt_command_substitution::PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION
        );
    });
}

#[test]
fn generated_rendered_request_is_refused() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            &json!({
                "schema_version": 1,
                "text": "$(printf EXECUTED)",
                "kind": "generated",
                "launch_authorship": "human",
            }),
        );
        let value = py_to_json_value(
            py_plan_prompt_command_substitution(py, &request)
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(value["eligible"], json!(false));
        assert_eq!(value["offset_unit"], json!("utf8_bytes"));
        assert_eq!(value["replacements"], json!([]));
    });
}

#[test]
fn human_source_round_trips_a_command_span() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            &json!({
                "schema_version": 1,
                "text": "café $(printf hi)",
                "kind": "human_source",
                "launch_authorship": "human",
            }),
        );
        let value = py_to_json_value(
            py_plan_prompt_command_substitution(py, &request)
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(value["eligible"], json!(true));
        assert_eq!(value["offset_unit"], json!("utf8_bytes"));
        assert_eq!(
            value["replacements"],
            json!([{
                "start": 6,
                "end": 18,
                "command": "printf hi",
            }])
        );
    });
}

#[test]
fn invalid_protected_range_raises() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            &json!({
                "schema_version": 1,
                "text": "$(echo hi)",
                "kind": "human_source",
                "launch_authorship": "human",
                "protected_ranges": [{"start": 9, "end": 1}],
            }),
        );
        let error =
            py_plan_prompt_command_substitution(py, &request).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("invalid_protected_range"), "{message}");
    });
}
