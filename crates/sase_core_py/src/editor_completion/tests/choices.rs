use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn hint_dict<'py>(
    py: Python<'py>,
    hint: serde_json::Value,
) -> Bound<'py, PyDict> {
    let req = serde_json::json!({
        "hint": hint,
        "partial": "",
        "replacement": "",
        "selected": Vec::<String>::new(),
    });
    json_value_to_py(py, &req)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

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
fn choice_and_label_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        // Rich hint with named_type/value_role survives the binding.
        let hint = serde_json::json!({
            "name": "env",
            "type": "enum",
            "required": true,
            "default_display": null,
            "position": 0,
            "repeatable": false,
            "choices": [
                {"value": "staging", "label": "Staging", "description": "Staging env"},
                {"value": "prod"}
            ],
            "named_type": "deploy_env",
            "value_role": null,
        });
        let req = hint_dict(py, hint);
        let out = py_macro_argument_choice_candidates(py, &req).unwrap();
        let val = py_to_json_value(out.bind(py)).unwrap();
        assert_eq!(val[0]["value"], json!("staging"));
        assert_eq!(val[0]["insertion"], json!("staging"));
        assert_eq!(val[0]["label"], json!("Staging"));
        assert_eq!(val[0]["index"], json!(0));
        assert_eq!(val[1]["value"], json!("prod"));

        let label_req_val = serde_json::json!({
            "hint": {
                "name": "env",
                "type": "enum",
                "required": true,
                "default_display": null,
                "position": 0,
                "repeatable": false,
                "choices": [{"value": "a"}, {"value": "b"}],
            }
        });
        let label_req = request_dict(py, &label_req_val);
        let label = py_macro_input_type_label(py, &label_req).unwrap();
        assert_eq!(py_to_json_value(label.bind(py)).unwrap(), json!("a | b"));

        // Old hints without new fields still bind (additive compatibility).
        let old_hint = serde_json::json!({
            "name": "env",
            "type": "enum",
            "required": true,
            "default_display": null,
            "position": 0,
            "repeatable": false,
            "choices": [{"value": "staging"}]
        });
        let old_req = hint_dict(py, old_hint);
        let old_out =
            py_macro_argument_choice_candidates(py, &old_req).unwrap();
        let old_val = py_to_json_value(old_out.bind(py)).unwrap();
        assert_eq!(old_val[0]["value"], json!("staging"));

        // Bool synthesizes true/false; quoting escapes structural chars.
        let bool_hint = serde_json::json!({
            "name": "deep",
            "type": "bool",
            "required": false,
            "default_display": "false",
            "position": 0,
            "repeatable": false,
            "choices": []
        });
        let bool_req = hint_dict(py, bool_hint);
        let bool_out =
            py_macro_argument_choice_candidates(py, &bool_req).unwrap();
        let bool_val = py_to_json_value(bool_out.bind(py)).unwrap();
        assert_eq!(bool_val[0]["value"], json!("true"));
        assert_eq!(bool_val[1]["value"], json!("false"));
        assert_eq!(bool_val[1]["is_default"], json!(true));

        let comma_hint = serde_json::json!({
            "name": "val",
            "type": "enum",
            "required": true,
            "default_display": null,
            "position": 0,
            "repeatable": false,
            "choices": [{"value": "a,b"}, {"value": "plain"}]
        });
        let comma_req = hint_dict(py, comma_hint);
        let comma_out =
            py_macro_argument_choice_candidates(py, &comma_req).unwrap();
        let comma_val = py_to_json_value(comma_out.bind(py)).unwrap();
        assert_eq!(comma_val[0]["insertion"], json!("\"a,b\""));
        assert_eq!(comma_val[1]["insertion"], json!("plain"));
    });
}
