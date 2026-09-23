use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn targets(py: Python<'_>) -> Bound<'_, PyList> {
    let targets = json_value_to_py(
        py,
        &json!([
            {
                "key": "gh_sase-org__sase",
                "name": "sase",
                "aliases": ["sa"],
                "workflow_type": "gh",
            },
            {
                "key": "git_notes",
                "name": "notes",
                "aliases": [],
                "workflow_type": "git",
            },
            {
                "key": "home",
                "name": "home",
                "aliases": [],
                "workflow_type": null,
            },
        ]),
    )
    .unwrap();
    targets.bind(py).downcast::<PyList>().unwrap().clone()
}

#[test]
fn project_tag_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_project_tag(&module).unwrap();
        for name in [
            "project_tag_scan",
            "project_tag_resolve",
            "project_tag_expand",
            "project_tag_trigger",
            "project_tag_apply_selection",
        ] {
            assert!(module.getattr(name).is_ok(), "{name} is registered");
        }
    });
}

#[test]
fn project_tag_scan_binding_returns_char_offsets() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        // `é` is one code point but two bytes: the `+` sits at byte 3 and
        // char 2.
        let result = py_project_tag_scan(py, "é +sase").unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(
            value,
            json!([{
                "start": 2,
                "end": 7,
                "name_start": 3,
                "name": "sase",
                "anchored": false,
            }])
        );
    });
}

#[test]
fn project_tag_resolve_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let targets = targets(py);
        let resolved = py_project_tag_resolve(py, "Sase", &targets).unwrap();
        assert_eq!(
            py_to_json_value(resolved.bind(py)).unwrap(),
            json!({"kind": "resolved", "target_index": 0})
        );

        let unknown = py_project_tag_resolve(py, "ssae", &targets).unwrap();
        let unknown = py_to_json_value(unknown.bind(py)).unwrap();
        assert_eq!(unknown["kind"], json!("unknown"));
        assert_eq!(
            unknown["suggestions"][0],
            json!("+sase"),
            "transposed names still suggest"
        );
    });
}

#[test]
fn project_tag_expand_binding_rewrites_and_reports() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let targets = targets(py);
        let expanded =
            py_project_tag_expand(py, "é +sase and +ssae", &targets).unwrap();
        let value = py_to_json_value(expanded.bind(py)).unwrap();
        assert_eq!(value["text"], json!("é #gh:gh_sase-org__sase and +ssae"));
        assert_eq!(
            value["tags"][0]["replacement"],
            json!("#gh:gh_sase-org__sase")
        );
        // Char offsets despite the multibyte prefix.
        assert_eq!(value["tags"][0]["start"], json!(2));
        assert_eq!(value["tags"][0]["end"], json!(7));
        assert_eq!(value["tags"][1]["replacement"], json!(null));
    });
}

#[test]
fn project_tag_trigger_binding_uses_char_offsets() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let trigger = py_project_tag_trigger(py, "é +sa", 5).unwrap();
        assert_eq!(
            py_to_json_value(trigger.bind(py)).unwrap(),
            json!({"start": 2, "end": 5, "query": "sa"})
        );

        let missing = py_project_tag_trigger(py, "a+b", 3).unwrap();
        assert!(py_to_json_value(missing.bind(py)).unwrap().is_null());
    });
}

#[test]
fn project_tag_targets_with_state_fields_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let targets = json_value_to_py(
            py,
            &json!([
                {
                    "key": "gh_sase-org__sase",
                    "name": "sase",
                    "aliases": ["sa"],
                    "workflow_type": "gh",
                    "state": "enabled",
                    "workspace_dir": "/tmp/sase",
                },
                {
                    "key": "home",
                    "name": "home",
                    "aliases": [],
                    "workflow_type": null,
                },
            ]),
        )
        .unwrap();
        let targets = targets.bind(py).downcast::<PyList>().unwrap().clone();
        let resolved = py_project_tag_resolve(py, "Sase", &targets).unwrap();
        assert_eq!(
            py_to_json_value(resolved.bind(py)).unwrap(),
            json!({"kind": "resolved", "target_index": 0})
        );
    });
}

#[test]
fn project_tag_apply_selection_binding_returns_char_cursor() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let targets = targets(py);
        let applied = py_project_tag_apply_selection(
            py,
            "é +sa",
            (2, 5),
            "+sase ",
            vec!["gh".to_string(), "git".to_string()],
            &targets,
        )
        .unwrap();
        let value = py_to_json_value(applied.bind(py)).unwrap();
        assert_eq!(value["text"], json!("é +sase "));
        assert_eq!(value["cursor"], json!(8));
    });
}
