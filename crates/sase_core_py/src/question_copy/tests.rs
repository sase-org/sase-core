use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::{json, Value};

#[test]
fn question_copy_bindings_round_trip_and_report_stable_errors() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let value = json_value_to_py(
            py,
            &json!(["/Users/bryan/.cargo\n", {"label": "URL", "text": "chrome://settings"}]),
        )
        .unwrap();
        let normalized =
            py_normalize_question_copy(py, value.bind(py)).unwrap();
        let normalized = py_to_json_value(normalized.bind(py)).unwrap();
        assert_eq!(
            normalized,
            json!([
                {"text": "/Users/bryan/.cargo"},
                {"text": "chrome://settings", "label": "URL"}
            ])
        );

        let display = py_display_question_copy(py, value.bind(py)).unwrap();
        let display = py_to_json_value(display.bind(py)).unwrap();
        assert_eq!(display["items"], normalized);
        assert_eq!(display["unavailable"], json!(0));

        let candidates = py_detect_question_copy_candidates(
            py,
            "Run `--help` and see plan:202611/example.md.",
            Vec::new(),
        )
        .unwrap();
        let candidates = py_to_json_value(candidates.bind(py)).unwrap();
        assert_eq!(
            candidates,
            json!([
                {"text": "--help", "kind": "code"},
                {"text": "plan:202611/example.md", "kind": "ref"}
            ])
        );

        let invalid =
            json_value_to_py(py, &json!([{"text": "bad\u{001b}value"}]))
                .unwrap();
        let error =
            py_normalize_question_copy(py, invalid.bind(py)).unwrap_err();
        let error_value = error.into_value(py);
        let message = error_value.bind(py).str().unwrap().to_string();
        assert!(message.starts_with("invalid_question_copy: copy[0].text:"));

        let malformed = json_value_to_py(
            py,
            &json!(["good", {"text": "bad", "name": "typo"}]),
        )
        .unwrap();
        let display = py_display_question_copy(py, malformed.bind(py)).unwrap();
        let display: Value = py_to_json_value(display.bind(py)).unwrap();
        assert_eq!(display["items"], json!([{"text": "good"}]));
        assert_eq!(display["unavailable"], json!(1));
    });
}
