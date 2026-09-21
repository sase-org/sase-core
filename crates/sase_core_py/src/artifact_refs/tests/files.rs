use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;
use std::fs;

#[test]
fn artifact_consumption_binding_returns_summary_and_handshake() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let log = temp.path().join("consumption.jsonl");
    fs::write(
        &log,
        concat!(
            "{\"schema_version\":1,\"consumption\":{",
            "\"id\":\"one\",\"timestamp\":\"2026-07-30T10:00:00Z\",",
            "\"ref\":\"file:default:abc\",\"ref_kind\":\"file\",",
            "\"fragment\":null,\"role\":\"image\",",
            "\"artifact_id\":\"default:abc\",\"resolved_path\":\"/one\",",
            "\"resolution_status\":\"exact\",\"agent_name\":\"agent.two\",",
            "\"agent_source\":\"SASE_AGENT_NAME\",",
            "\"artifacts_dir\":null,\"project\":\"sase\"}}\n",
            "{\"schema_version\":1,\"consumption\":{",
            "\"id\":\"two\",\"timestamp\":\"2026-07-30T11:00:00Z\",",
            "\"ref\":\"file:default:abc\",\"ref_kind\":\"file\",",
            "\"fragment\":null,\"role\":\"report\",",
            "\"artifact_id\":\"default:abc\",\"resolved_path\":\"/two\",",
            "\"resolution_status\":\"exact\",\"agent_name\":\"agent.one\",",
            "\"agent_source\":\"SASE_AGENT_NAME\",",
            "\"artifacts_dir\":null,\"project\":\"sase\"}}\n"
        ),
    )
    .unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("artifact_consumption_summary").is_ok());
        assert!(module
            .getattr("artifact_consumption_wire_schema_version")
            .is_ok());

        let result = py_artifact_consumption_summary(
            py,
            log.to_str().unwrap(),
            Some(vec![
                "file:default:abc".to_string(),
                "file:default:never".to_string(),
            ]),
        )
        .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let summary = &value["file:default:abc"];
        assert_eq!(summary["consumption_count"], json!(2));
        assert_eq!(summary["distinct_agent_count"], json!(2));
        assert_eq!(summary["agent_names"], json!(["agent.one", "agent.two"]));
        assert_eq!(summary["roles"], json!(["image", "report"]));
        assert!(value.get("file:default:never").is_none());
        assert_eq!(py_artifact_consumption_wire_schema_version(), 1);
    });
}

#[test]
fn artifact_file_query_binding_returns_full_rows_and_handshake() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let index = temp.path().join("index.jsonl");
    fs::write(
        &index,
        concat!(
            "{\"schema_version\":1,\"artifact\":{\"id\":\"old\",",
            "\"label\":\"Old\",\"kind\":\"image\",\"path\":\"/old\",",
            "\"created_at\":\"2026-07-01T00:00:00Z\"}}\n",
            "{\"schema_version\":2,\"artifact\":{\"id\":\"new\",",
            "\"label\":\"New\",\"kind\":\"image\",\"path\":\"/new\",",
            "\"created_at\":\"2026-07-02T00:00:00Z\",",
            "\"sha256\":\"abc\",\"size_bytes\":3,",
            "\"mime_type\":\"image/png\"}}\n"
        ),
    )
    .unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("artifact_files_query").is_ok());
        assert!(module
            .getattr("artifact_file_query_wire_schema_version")
            .is_ok());
        assert!(module.getattr("artifact_file_materialize_vcs").is_ok());

        let filters = PyDict::new_bound(py);
        filters.set_item("kinds", ["image"]).unwrap();
        filters.set_item("limit", 1).unwrap();
        let result =
            py_artifact_files_query(py, index.to_str().unwrap(), &filters)
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value[0]["id"], json!("new"));
        assert_eq!(value[0]["schema_version"], json!(2));
        assert_eq!(value[0]["sha256"], json!("abc"));
        assert_eq!(value[0]["size_bytes"], json!(3));
        assert_eq!(value[0]["mime_type"], json!("image/png"));
        assert_eq!(py_artifact_file_query_wire_schema_version(), 3);

        let request = PyDict::new_bound(py);
        request
            .set_item("cache_root", temp.path().join("cache"))
            .unwrap();
        request
            .set_item("checkout_paths", Vec::<String>::new())
            .unwrap();
        request
            .set_item("vcs_sha", "0123456789abcdef0123456789abcdef01234567")
            .unwrap();
        request.set_item("vcs_relpath", "docs/missing.png").unwrap();
        request
                .set_item(
                    "sha256",
                    "0000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap();
        request.set_item("suffix", ".png").unwrap();
        request.set_item("max_history_scan", 20).unwrap();
        let materialized =
            py_artifact_file_materialize_vcs(py, &request).unwrap();
        let materialized = py_to_json_value(materialized.bind(py)).unwrap();
        assert_eq!(materialized["status"], json!("missing"));
    });
}

#[test]
fn artifact_context_query_binding_returns_projected_rows_and_handshake() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let index = temp.path().join("index.jsonl");
    fs::write(
        &index,
        concat!(
            "{\"schema_version\":1,\"artifact\":{\"id\":\"report\",",
            "\"label\":\"Report\",\"kind\":\"markdown\",",
            "\"path\":\"/stored/report.md\",",
            "\"agent_artifacts_dir\":\"/producers/a\",",
            "\"agent_name\":\"researcher.a\",",
            "\"created_at\":\"2026-07-01T00:00:00Z\"}}\n",
            "{\"schema_version\":1,\"artifact\":{\"id\":\"transcript\",",
            "\"label\":\"Transcript\",\"kind\":\"chat\",",
            "\"path\":\"/stored/transcript.md\",",
            "\"agent_artifacts_dir\":\"/producers/a\",",
            "\"agent_name\":\"researcher.a\",",
            "\"created_at\":\"2026-07-01T00:00:00Z\"}}\n"
        ),
    )
    .unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("artifact_context_query").is_ok());
        assert!(module
            .getattr("artifact_context_query_wire_schema_version")
            .is_ok());

        let group = PyDict::new_bound(py);
        group.set_item("wait_name", "research.a").unwrap();
        group
            .set_item("agent_artifacts_dirs", vec!["/producers/a"])
            .unwrap();
        let groups = PyList::new_bound(py, [group]);

        let result =
            py_artifact_context_query(py, index.to_str().unwrap(), &groups)
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(value[0]["wait_name"], json!("research.a"));
        assert_eq!(value[0]["agent_name"], json!("researcher.a"));
        assert_eq!(value[0]["ref"], json!("file:report"));
        assert_eq!(value[0]["kind"], json!("markdown"));
        assert_eq!(value[0]["path"], json!("/stored/report.md"));
        assert_eq!(py_artifact_context_query_wire_schema_version(), 1);
    });
}

#[test]
fn artifact_file_lifecycle_bindings_round_trip_plain_python_shapes() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let index = temp.path().join("index.jsonl");
    let stored = temp.path().join("store/payload.bin");
    fs::create_dir_all(stored.parent().unwrap()).unwrap();
    fs::write(&stored, b"payload").unwrap();
    fs::write(
        &index,
        format!(
            "{{\"schema_version\":2,\"artifact\":{{\"id\":\"old\",\
                 \"label\":\"x\",\"kind\":\"file\",\"path\":{},\
                 \"project\":\"p\",\"created_at\":\"2026-07-01T00:00:00Z\",\
                 \"size_bytes\":7}}}}\n",
            serde_json::to_string(&stored.to_string_lossy()).unwrap()
        ),
    )
    .unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "artifact_file_store_economics",
            "artifact_file_retention_plan",
            "artifact_file_trash_store",
            "artifact_file_trash_list",
            "artifact_file_trash_restore",
            "artifact_file_trash_purge",
            "artifact_file_lifecycle_wire_schema_version",
        ] {
            assert!(module.getattr(name).is_ok(), "{name}");
        }
        assert_eq!(
            py_artifact_file_lifecycle_wire_schema_version(),
            ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION
        );

        let options_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": null,
                "top_n": 10,
                "generation_projections": [1]
            }),
        )
        .unwrap();
        let options = options_obj.bind(py).downcast::<PyDict>().unwrap();
        let economics = py_artifact_file_store_economics(
            py,
            index.to_str().unwrap(),
            options,
        )
        .unwrap();
        let economics = py_to_json_value(economics.bind(py)).unwrap();
        assert_eq!(economics["schema_version"], json!(1));
        assert_eq!(economics["total_rows"], json!(1));
        assert_eq!(economics["total_bytes"], json!(7));

        let policy_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "now": "2026-07-30T00:00:00Z",
                "keep_per_label": 0,
                "before": null,
                "kinds": null,
                "project": null,
                "min_size_bytes": null,
                "protected_ids": [],
                "limit": null
            }),
        )
        .unwrap();
        let policy = policy_obj.bind(py).downcast::<PyDict>().unwrap();
        let plan = py_artifact_file_retention_plan(
            py,
            index.to_str().unwrap(),
            policy,
        )
        .unwrap();
        let plan = py_to_json_value(plan.bind(py)).unwrap();
        assert_eq!(plan["schema_version"], json!(1));
        assert_eq!(plan["counts"]["selected"], json!(0));

        let trash_root = temp.path().join("trash");
        let store_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "trash_root": trash_root,
                "record": {
                    "id": "default:abcdef0123456789abcdef01",
                    "path": stored,
                    "size_bytes": 7
                },
                "stored_path": stored,
                "reason": "binding test",
                "trashed_at": "2026-07-30T12:00:00Z"
            }),
        )
        .unwrap();
        let store_request = store_obj.bind(py).downcast::<PyDict>().unwrap();
        let entry = py_artifact_file_trash_store(py, store_request).unwrap();
        let entry = py_to_json_value(entry.bind(py)).unwrap();
        assert_eq!(entry["schema_version"], json!(1));
        assert!(!stored.exists());

        let listing =
            py_artifact_file_trash_list(py, trash_root.to_str().unwrap())
                .unwrap();
        let listing = py_to_json_value(listing.bind(py)).unwrap();
        assert_eq!(listing["entries"].as_array().unwrap().len(), 1);

        let restore_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "trash_root": trash_root,
                "entry_id": entry["entry_id"]
            }),
        )
        .unwrap();
        let restore_request =
            restore_obj.bind(py).downcast::<PyDict>().unwrap();
        let restored =
            py_artifact_file_trash_restore(py, restore_request).unwrap();
        let restored = py_to_json_value(restored.bind(py)).unwrap();
        assert_eq!(
            restored["record"]["id"],
            json!("default:abcdef0123456789abcdef01")
        );
        assert_eq!(fs::read(&stored).unwrap(), b"payload");

        let invalid_options_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 2,
                "project": null,
                "top_n": 10,
                "generation_projections": []
            }),
        )
        .unwrap();
        let invalid_options =
            invalid_options_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_artifact_file_store_economics(
            py,
            index.to_str().unwrap(),
            invalid_options,
        )
        .is_err());
    });
}
