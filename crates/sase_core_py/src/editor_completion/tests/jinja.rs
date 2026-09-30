use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn jinja_request_dict<'a>(
    py: Python<'a>,
    text: &str,
    character: u32,
    scope: &str,
) -> Bound<'a, PyDict> {
    json_value_to_py(
        py,
        &json!({
            "text": text,
            "position": {"line": 0, "character": character},
            "scope": scope,
            "frontmatter": null,
        }),
    )
    .unwrap()
    .bind(py)
    .downcast::<PyDict>()
    .unwrap()
    .clone()
}

fn jinja_scope_request_dict<'a>(
    py: Python<'a>,
    text: &str,
    scope: &str,
) -> Bound<'a, PyDict> {
    json_value_to_py(
        py,
        &json!({
            "text": text,
            "scope": scope,
            "frontmatter": null,
        }),
    )
    .unwrap()
    .bind(py)
    .downcast::<PyDict>()
    .unwrap()
    .clone()
}

#[test]
fn jinja_completion_round_trip_inside_and_outside_tags() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = jinja_request_dict(py, "Hello {{ ", 8, "prompt");
        let completion = py_jinja_completion(py, request).unwrap();
        let value = py_to_json_value(completion.bind(py)).unwrap();
        assert_eq!(value["slot"], json!("variable"));
        assert!(value["items"].as_array().unwrap().len() > 1);
        let names: Vec<&str> = value["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"root"));
        // Rank fields are dense final indexes starting at zero.
        assert_eq!(value["items"][0]["rank"], json!(0));

        let outside = jinja_request_dict(py, "hello", 5, "prompt");
        let none = py_jinja_completion(py, outside).unwrap();
        assert_eq!(py_to_json_value(none.bind(py)).unwrap(), json!(null));
    });
}

#[test]
fn jinja_bindings_are_registered_on_the_module() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_editor_completion(&module).unwrap();
        for name in
            ["jinja_completion", "jinja_scope_variables", "jinja_catalog"]
        {
            assert!(module.hasattr(name).unwrap(), "{name} is not registered");
        }
        let catalog = module.getattr("jinja_catalog").unwrap().call0().unwrap();
        let catalog_value = py_to_json_value(&catalog).unwrap();
        assert!(!catalog_value["variables"].as_array().unwrap().is_empty());
    });
}

#[test]
fn jinja_scope_variables_and_catalog_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request =
            jinja_scope_request_dict(py, "Hello {{ root }}", "prompt");
        let variables = py_jinja_scope_variables(py, request).unwrap();
        let value = py_to_json_value(variables.bind(py)).unwrap();
        assert!(value["known"]
            .as_array()
            .unwrap()
            .iter()
            .any(|name| name == "root"));
        assert!(value["unavailable"].is_array());

        let catalog = py_jinja_catalog(py).unwrap();
        let catalog_value = py_to_json_value(catalog.bind(py)).unwrap();
        for table in [
            "variables",
            "filters",
            "tests",
            "jinja_globals",
            "statements",
        ] {
            assert!(
                !catalog_value[table].as_array().unwrap().is_empty(),
                "{table} is empty"
            );
        }
    });
}
