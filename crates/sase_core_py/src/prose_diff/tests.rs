use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn prose_diff_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_prose_diff(&module).unwrap();
        assert!(module.getattr("compare_prose").is_ok());
        assert!(module.getattr("prose_diff_wire_schema_version").is_ok());
        assert_eq!(py_prose_diff_wire_schema_version(), 1);
    });
}

#[test]
fn compare_prose_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "base": "SASE agent shells export\n",
                "target": "SASE agent processes export\n",
                "format": "markdown",
                "context_lines": 3,
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_compare_prose(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["stats"]["words_added"], json!(1));
        assert_eq!(value["stats"]["words_removed"], json!(1));
        assert!(value.get("line_marks").is_some());
        assert!(value.get("word_ops").is_some());
        assert!(value.get("hunks").is_some());
        assert!(value.get("line_map").is_some());
        assert!(value.get("unified_diff").is_some());
    });
}

#[test]
fn compare_prose_binding_detects_promotion() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "base": "---\ntype: reference\n---\n\nBody.\n",
                "target": "---\ntype: core\n---\n\nBody.\n",
                "format": "markdown",
                "context_lines": 3,
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_compare_prose(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["frontmatter"]["type_change"], json!("promoted"));
    });
}
