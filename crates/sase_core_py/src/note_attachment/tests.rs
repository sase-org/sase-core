//! Binding round-trip tests for note attachments.

use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn note_attachment_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_note_attachment(&module).unwrap();
        for name in [
            "classify_attachment",
            "scan_note_attachment_refs",
            "compose_note_attachment_text",
            "note_attachment_source_text",
            "stored_attachment_tokens",
            "sanitize_attachment_name",
            "unique_attachment_name",
        ] {
            assert!(module.getattr(name).is_ok(), "{name} is registered");
        }
    });
}

#[test]
fn scan_compose_source_cycle_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let scan = py_scan_note_attachment_refs(
            py,
            "@./shots/login.png and @attachment:login.png",
            vec![],
        )
        .unwrap();
        let scan_value = py_to_json_value(scan.bind(py)).unwrap();
        assert_eq!(scan_value["path_refs"].as_array().unwrap().len(), 1);
        let composed = py_compose_note_attachment_text(
            "@./shots/login.png and @attachment:login.png",
            scan.bind(py),
            vec!["login.png".to_string()],
        )
        .unwrap();
        assert_eq!(composed, "@attachment:login.png and @attachment:login.png");
        let source = py_note_attachment_source_text(
            &composed,
            vec!["login.png".to_string()],
        )
        .unwrap();
        assert_eq!(source, composed);
        let tokens = py_stored_attachment_tokens(py, &composed).unwrap();
        let tokens_value = py_to_json_value(tokens.bind(py)).unwrap();
        assert_eq!(tokens_value.as_array().unwrap().len(), 2);
        let sanitized =
            py_sanitize_attachment_name("My Screenshot (1).PNG").unwrap();
        assert_eq!(sanitized, "My_Screenshot_1.PNG");
    });
}

#[test]
fn classify_png_bytes_wins_over_txt_extension() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let head = PyBytes::new_bound(
            py,
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        );
        let classification =
            py_classify_attachment(py, "note.txt", &head).unwrap();
        let value = py_to_json_value(classification.bind(py)).unwrap();
        assert_eq!(value["mime_type"], json!("image/png"));
        assert_eq!(value["class"], json!("image"));
    });
}

#[test]
fn uniquify_bumps_on_digest_conflict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let existing = json_value_to_py(
            py,
            &json!([{"name": "login.png", "sha256": "b".repeat(64)}]),
        )
        .unwrap();
        let existing = existing.bind(py).downcast::<PyList>().unwrap();
        let uniquified =
            py_unique_attachment_name("login.png", &"a".repeat(64), existing)
                .unwrap();
        assert_eq!(uniquified, "login-2.png");
    });
}
