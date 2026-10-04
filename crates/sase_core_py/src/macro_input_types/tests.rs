use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::{json, Value as JsonValue};

fn request_dict<'py>(py: Python<'py>, value: &JsonValue) -> Bound<'py, PyDict> {
    json_value_to_py(py, value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

#[test]
fn macro_input_type_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_macro_input_types(&module).unwrap();
        for name in [
            "macro_input_type_catalog",
            "resolve_input_type",
            "validate_enum_choices",
            "pyyaml_plain_scalar_is_non_string",
            "check_input_value",
        ] {
            assert!(module.getattr(name).is_ok(), "{name} is registered");
        }
    });
}

#[test]
fn catalog_binding_round_trips_contract_rows() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let result = py_macro_input_type_catalog(py).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let names: Vec<&str> = value
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "word", "line", "text", "path", "int", "float", "bool", "code",
                "string", "enum", "agent",
            ]
        );
        let string = value
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == "string")
            .unwrap();
        assert_eq!(string["base"], json!("line"));
        assert_eq!(string["advertised"], json!(false));
        assert_eq!(string["deprecated_alias_of"], json!("line"));
    });
}

#[test]
fn resolve_binding_round_trips_success_and_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request =
            request_dict(py, &json!({"name": "mode", "raw": "builtin@word"}));
        let resolved = py_resolve_input_type(py, &request).unwrap();
        assert_eq!(
            py_to_json_value(resolved.bind(py)).unwrap()["base"],
            json!("word")
        );

        let request = request_dict(py, &json!({"name": "mode", "raw": "enmu"}));
        let error = py_resolve_input_type(py, &request).unwrap_err();
        assert_eq!(
            error.to_string(),
            "ValueError: input `mode` has unknown type `enmu`; did you mean `enum`?"
        );

        let request = request_dict(
            py,
            &json!({
                "name": "edition",
                "raw": "sase-research-artifacts@audio_editon",
                "plugins": {
                    "sase-research-artifacts": ["audio_edition"]
                }
            }),
        );
        let error = py_resolve_input_type(py, &request).unwrap_err();
        assert!(error.to_string().contains("audio_edition"));
    });
}

#[test]
fn validate_and_check_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            &json!({
                "items": [
                    true,
                    "brief",
                    {"value": "full", "label": "Full", "description": "Long"}
                ]
            }),
        );
        let result = py_validate_enum_choices(py, &request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["choices"][0]["value"], json!("brief"));
        assert_eq!(value["choices"][1]["description"], json!("Long"));
        assert_eq!(
            value["issues"][0]["message"],
            json!("choice arrived as a boolean and must be quoted")
        );
        assert_eq!(value["issues"][0]["severity"], json!("error"));

        let request = request_dict(
            py,
            &json!({
                "name": "edition",
                "value": "breif",
                "resolved": {
                    "base": "enum",
                    "named_type": null,
                    "value_role": null,
                    "choices": [
                        {"value": "brief"},
                        {"value": "full"}
                    ],
                    "deprecated": false
                }
            }),
        );
        let message = py_check_input_value(py, &request).unwrap();
        assert_eq!(
            message.extract::<String>(py).unwrap(),
            "Argument `edition` expects one of brief | full, got `breif`; \
             did you mean `brief`?"
        );
    });
}

#[test]
fn pyyaml_binding_matches_the_contract_vector() {
    pyo3::prepare_freethreaded_python();
    assert!(py_pyyaml_plain_scalar_is_non_string("yes"));
    assert!(py_pyyaml_plain_scalar_is_non_string("42"));
    assert!(py_pyyaml_plain_scalar_is_non_string("2020-01-01"));
    assert!(!py_pyyaml_plain_scalar_is_non_string("ready"));
    assert!(!py_pyyaml_plain_scalar_is_non_string("brief"));
}
