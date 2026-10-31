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
fn wait_target_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_wait_target(&module).unwrap();
        for name in [
            "parse_wait_target",
            "effective_wait_target",
            "select_current_wait_turn",
            "wait_targets_with_descriptions",
        ] {
            assert!(module.getattr(name).is_ok(), "{name} is registered");
        }
    });
}

#[test]
fn parse_binding_round_trips_success_and_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let value = py_to_json_value(
            py_parse_wait_target(py, "session").unwrap().bind(py),
        )
        .unwrap();
        assert_eq!(value, json!("session"));

        let error = py_parse_wait_target(py, "Session").unwrap_err();
        let message = error.to_string();
        assert!(message.contains("got `Session`"), "{message}");
        assert!(message.contains("did you mean `session`"), "{message}");
    });
}

#[test]
fn effective_binding_round_trips_table_cell_and_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            &json!({
                "requested": "turn",
                "kind": "bare_agent",
                "reference": "16h",
            }),
        );
        let value = py_to_json_value(
            py_effective_wait_target(py, &request).unwrap().bind(py),
        )
        .unwrap();
        assert_eq!(value["target"], json!("turn"));
        assert_eq!(value["explicit"], json!(true));
        assert_eq!(value["scope"], json!("current_turn"));

        let request = request_dict(
            py,
            &json!({
                "requested": "turn",
                "kind": "clan",
                "reference": "builders",
            }),
        );
        let error = py_effective_wait_target(py, &request).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("`builders`"), "{message}");
        assert!(message.contains("clan"), "{message}");
    });
}

#[test]
fn select_binding_returns_index_or_none() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = request_dict(
            py,
            &json!({
                "members": [
                    {"timestamp": "t1", "pending": false, "excluded": false},
                    {"timestamp": "t3", "pending": true, "excluded": false},
                    {"timestamp": "t2", "pending": true, "excluded": false},
                ]
            }),
        );
        let value = py_to_json_value(
            py_select_current_wait_turn(py, &request).unwrap().bind(py),
        )
        .unwrap();
        assert_eq!(value, json!(1));

        let request = request_dict(py, &json!({"members": []}));
        let value = py_to_json_value(
            py_select_current_wait_turn(py, &request).unwrap().bind(py),
        )
        .unwrap();
        assert_eq!(value, json!(null));
    });
}

#[test]
fn descriptions_binding_round_trips_shared_constant() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let value = py_to_json_value(
            py_wait_targets_with_descriptions(py).unwrap().bind(py),
        )
        .unwrap();
        assert_eq!(value[0]["value"], json!("session"));
        assert_eq!(value[1]["value"], json!("turn"));
        assert!(value[0]["description"]
            .as_str()
            .unwrap()
            .contains("session"));
        assert!(value[1]["description"].as_str().unwrap().contains("turn"));
    });
}
