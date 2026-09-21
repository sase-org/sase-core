use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn source_language_bindings_round_trip_wire_payloads() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_source_language_wire_schema_version(),
            SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION
        );
        assert_eq!(
            py_source_language_prefix_budget_bytes(),
            SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES
        );

        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "category": "raw_file",
                "logical_filename": "src/app.py",
                "prefix": null
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let selected = py_resolve_source_language(py, request).unwrap();
        let value = py_to_json_value(selected.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["language"], json!("python"));
        assert_eq!(value["reason"], json!("filename"));
        assert_eq!(value["supported_text"], json!(true));

        let hints = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "source_path": "/orig/app.py",
                "vcs_relpath": "docs/app.py",
                "resolved_path": "/objects/abc"
            }),
        )
        .unwrap();
        let hints = hints.bind(py).downcast::<PyDict>().unwrap();
        let filename = py_logical_source_filename(py, hints).unwrap();
        let filename = py_to_json_value(filename.bind(py)).unwrap();
        assert_eq!(filename, json!("/orig/app.py"));
    });
}

#[test]
fn memory_xprompt_bindings_expose_the_shared_contract() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(py_memory_reference_name("glossary"), "memory/glossary");
        assert_eq!(
            py_memory_reference_stem("memory/glossary").as_deref(),
            Some("glossary")
        );
        assert_eq!(py_memory_reference_stem("glossary"), None);

        let layout = py_to_json_value(
            py_sase_content_layout(
                py,
                "/home/alice",
                Some("/repo"),
                None,
                Some("demo"),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(layout["schema_version"], json!(5));
        assert_eq!(
            layout["memory_sources"][0]["paths"]["canonical"]["path"],
            json!("/repo/sase/memory")
        );
        assert_eq!(
            layout["memory_sources"][0]["paths"]["read_policy"],
            json!("error")
        );
        assert_eq!(layout["memory_sources"][1]["id"], json!("home_memory"));
        assert_eq!(py_skill_reference_name("plan", None), "skill/plan");
        assert_eq!(
            py_skill_reference_name("plan", Some("demo")),
            "demo/skill/plan"
        );

        let reserved = py_to_json_value(
            py_reserved_memory_namespace_issue(
                py,
                "config xprompt",
                "memory/glossary",
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(reserved["rule"], json!("reserved_namespace"));
        assert!(py_reserved_memory_namespace_issue(py, "src", "foo")
            .unwrap()
            .is_none(py));

        let bad_type = py_to_json_value(
            py_memory_note_issue(
                py,
                "sase/memory/notes.md",
                "notes",
                Some("dynamic"),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(bad_type["rule"], json!("invalid_note_type"));
        assert!(py_memory_note_issue(
            py,
            "sase/memory/glossary.md",
            "glossary",
            Some("short")
        )
        .unwrap()
        .is_none(py));
    });
}

#[test]
fn inline_code_binding_returns_plain_byte_offset_tuples() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_inline_code_ranges, &module).unwrap(),
            )
            .unwrap();
        let value = module
            .getattr("inline_code_ranges")
            .unwrap()
            .call1(("é`值`/`ß`",))
            .unwrap();
        assert_eq!(py_to_json_value(&value).unwrap(), json!([[2, 7], [8, 12]]));
    });
}

#[test]
fn text_tail_binding_returns_plain_dict_and_counts_unicode_chars() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_tail_text_by_lines_and_chars, &module)
                    .unwrap(),
            )
            .unwrap();
        let value = module
            .getattr("tail_text_by_lines_and_chars")
            .unwrap()
            .call1(("alpha\nbeta\nééévalue", 2_usize, 7_usize))
            .unwrap();
        assert_eq!(
            py_to_json_value(&value).unwrap(),
            json!({
                "text": "éévalue",
                "omitted_lines": 1,
                "omitted_chars": 6
            })
        );
    });
}
