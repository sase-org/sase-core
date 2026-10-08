use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn hold_directive_bindings_collect_format_and_expand() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let occurrences_obj = json_value_to_py(
                py,
                &json!([
                    {
                        "source": "%hold:reviewer,planner",
                        "source_span": [0, 22],
                        "args": [{"value": "reviewer,planner"}],
                        "has_plus_suffix": false
                    },
                    {
                        "source": "%hold(pending, future, hood=sase-11l, ttl=5m, scope=host)",
                        "source_span": [23, 84],
                        "args": [
                            {"value": "pending"},
                            {"value": "future"},
                            {"name": "hood", "value": "sase-11l"},
                            {"name": "ttl", "value": "5m"},
                            {"name": "scope", "value": "host"}
                        ],
                        "has_plus_suffix": false
                    }
                ]),
            )
            .unwrap();

        let result =
            py_collect_hold_fields(py, occurrences_obj.bind(py), None).unwrap();
        let result_value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result_value["errors"], json!([]));
        assert_eq!(
            result_value["fields"]["names"],
            json!(["planner", "reviewer"])
        );
        assert_eq!(result_value["fields"]["pending"], json!(true));
        assert_eq!(result_value["fields"]["future"], json!(true));
        assert_eq!(result_value["fields"]["ttl_seconds"], json!(300));

        let fields_obj = json_value_to_py(py, &result_value["fields"]).unwrap();
        let formatted = py_format_hold_directive(fields_obj.bind(py)).unwrap();
        assert_eq!(
                formatted.as_deref(),
                Some(
                    "%hold(planner, reviewer, pending, future, hood=sase-11l, ttl=5m, scope=host)"
                )
            );

        let selectors = py_hold_fields_to_selectors(
            py,
            fields_obj.bind(py),
            Some(vec!["artifact/a".to_string(), "artifact/a".to_string()]),
            None,
        )
        .unwrap();
        let selectors_value = py_to_json_value(selectors.bind(py)).unwrap();
        assert_eq!(selectors_value["names"], json!(["planner", "reviewer"]));
        assert_eq!(
            selectors_value["agent_sessions"],
            json!(["planner", "reviewer"])
        );
        assert_eq!(selectors_value["hoods"], json!(["sase-11l"]));
        assert_eq!(selectors_value["artifact_dirs"], json!(["artifact/a"]));
    });
}

#[test]
fn auto_directive_bindings_classify_and_describe_vocabulary() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(
            module.getattr("classify_auto_directive").is_ok(),
            "missing classify_auto_directive"
        );
        assert!(
            module.getattr("auto_directive_vocabulary").is_ok(),
            "missing auto_directive_vocabulary"
        );

        let vocabulary = py_auto_directive_vocabulary(py).unwrap();
        let vocabulary_value = py_to_json_value(vocabulary.bind(py)).unwrap();
        assert_eq!(vocabulary_value["modes"], json!(["plan", "tale", "epic"]));
        assert_eq!(vocabulary_value["manual_values"], json!(["manual", "off"]));
        assert_eq!(vocabulary_value["diagnostic_code"], json!("invalid-auto"));

        let classify = |form: &str, value: &str, spelling: &str| {
            let request = json_value_to_py(
                py,
                &json!({
                    "form": form,
                    "value": value,
                    "spelling": spelling,
                }),
            )
            .unwrap();
            py_classify_auto_directive(py, request.bind(py))
                .map(|result| py_to_json_value(result.bind(py)).unwrap())
        };
        let bare = classify("bare", "", "%auto").unwrap();
        assert_eq!(bare["enabled"], json!(true));
        assert_eq!(bare["mode"], json!("plan"));
        assert_eq!(bare["argument"], json!(null));
        let tale = classify("colon", "tale", "%auto:tale").unwrap();
        assert_eq!(tale["enabled"], json!(true));
        assert_eq!(tale["mode"], json!("tale"));
        assert_eq!(tale["argument"], json!("tale"));
        let manual = classify("colon", "off", "%auto:off").unwrap();
        assert_eq!(manual["enabled"], json!(false));
        assert_eq!(manual["mode"], json!(null));
        assert_eq!(manual["argument"], json!(null));

        let err = classify("paren", "", "%auto(plan=ask)").unwrap_err();
        assert!(err.to_string().contains("%auto(plan=ask)"));
        let err = classify("colon", "foo", "%auto:foo").unwrap_err();
        assert!(err.to_string().contains("%auto:foo"));
    });
}

#[test]
fn standalone_named_proc_validator_has_no_legacy_binding_name() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(
            module
                .getattr("validate_standalone_named_proc_name")
                .is_ok(),
            "missing validate_standalone_named_proc_name"
        );
        assert!(
            module
                .getattr("validate_standalone_proc_shell_name")
                .is_err(),
            "legacy validate_standalone_proc_shell_name must stay unregistered"
        );

        py_validate_standalone_named_proc_name(Some("checks")).unwrap();
        let err = py_validate_standalone_named_proc_name(Some("agent--checks"))
            .unwrap_err();
        assert!(err.to_string().contains("`--`"));
    });
}

#[test]
fn prompt_proc_origin_returns_canonical_and_legacy_is_absent() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("xprompt_proc_origin").is_err());
        let new: String = module
            .getattr("prompt_proc_origin")
            .unwrap()
            .call0()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(new, "prompt-proc");
    });
}
