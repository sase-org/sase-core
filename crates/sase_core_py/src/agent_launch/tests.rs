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
