use super::*;
use crate::json_bridge::json_value_to_py;
use serde_json::json;
use std::fs;
use tempfile::tempdir;

fn write_mini_catalog(dir: &std::path::Path) -> std::path::PathBuf {
    let entry = json!({
        "id": "cli:bead.list",
        "ns": "cli",
        "area": "cli.bead",
        "kind": "command",
        "label": "List beads",
        "parent": null,
        "src": "sase/main/bead_handler.py:handle_list"
    });
    let line = format!("{entry}\n");
    let rev = sase_core::footprint::catalog_rev(line.as_bytes());
    let header = json!({
        "schema": "sase.footprint.catalog/1",
        "rev": rev,
        "count": 1
    });
    let path = dir.join("catalog.jsonl");
    fs::write(
        &path,
        format!("{}\n{line}", serde_json::to_string(&header).unwrap()),
    )
    .unwrap();
    path
}

#[test]
fn footprint_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_footprint(&module).unwrap();
        assert!(module.getattr("footprint_fold").is_ok());
        assert_eq!(py_footprint_wire_schema_version(py), 1);

        let temp = tempdir().unwrap();
        let store = temp.path().join("footprint.sqlite");
        let spool = temp.path().join("spool");
        fs::create_dir_all(&spool).unwrap();
        let catalog = write_mini_catalog(temp.path());
        let enrichment = PyDict::new_bound(py);
        enrichment.set_item("cli:bead.list", 9).unwrap();
        let ingested = py_footprint_ingest_catalog(
            py,
            store.to_str().unwrap(),
            catalog.to_str().unwrap(),
            &enrichment,
            "2026-10-27",
            5_000,
        )
        .unwrap();
        let ingested = py_to_json_value(ingested.bind(py)).unwrap();
        assert_eq!(ingested["known"], json!(true));

        fs::write(
            spool.join("2026-10-26.jsonl"),
            r#"{"s":1,"d":"2026-10-26","f":"cli:bead.list","a":"human","v":"cli","o":"ok","n":1,"ms":10,"ver":"0.18.1"}
"#,
        )
        .unwrap();
        let folded = py_footprint_fold(
            py,
            store.to_str().unwrap(),
            spool.to_str().unwrap(),
            "2026-10-27",
            7,
            30,
            5_000,
        )
        .unwrap();
        let folded = py_to_json_value(folded.bind(py)).unwrap();
        assert_eq!(folded["records_folded"], json!(1));

        let snapshot = py_footprint_status(
            py,
            store.to_str().unwrap(),
            spool.to_str().unwrap(),
            "2026-10-27",
            5_000,
        )
        .unwrap();
        let snapshot = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot["catalog_count"], json!(1));

        let overlay = json_value_to_py(
            py,
            &json!({
                "schema": "sase.footprint.overlay/1",
                "features": {}
            }),
        )
        .unwrap();
        let overlay = overlay.bind(py).downcast::<PyDict>().unwrap();
        py_footprint_validate_overlay(py, overlay).unwrap();

        let request = PyDict::new_bound(py);
        request
            .set_item("store_path", store.to_str().unwrap())
            .unwrap();
        request.set_item("today", "2026-10-27").unwrap();
        request.set_item("lens", "map").unwrap();
        let reported = py_footprint_report(py, &request, 5_000).unwrap();
        let reported = py_to_json_value(reported.bind(py)).unwrap();
        assert_eq!(reported["schema_version"], json!(1));
        assert!(reported["areas"]
            .as_array()
            .unwrap()
            .iter()
            .any(|area| { area["area"] == json!("cli.bead") }));

        request.set_item("feature_id", "cli:bead.list").unwrap();
        let evidence = py_footprint_card(py, &request, 5_000).unwrap();
        let evidence = py_to_json_value(evidence.bind(py)).unwrap();
        assert_eq!(evidence["row"]["id"], json!("cli:bead.list"));
        assert_eq!(evidence["row"]["state"], json!("used"));

        let decided = py_footprint_decide(
            py,
            store.to_str().unwrap(),
            "cli:bead.list",
            "keep",
            Some("agent_facing"),
            "2026-10-27",
            30,
            5_000,
        )
        .unwrap();
        let decided = py_to_json_value(decided.bind(py)).unwrap();
        assert_eq!(decided["decision"], json!("keep"));

        let export_request = PyDict::new_bound(py);
        export_request
            .set_item("store_path", store.to_str().unwrap())
            .unwrap();
        export_request.set_item("today", "2026-10-27").unwrap();
        export_request.set_item("sase", "0.18.1").unwrap();
        export_request.set_item("os", "linux").unwrap();
        let exported = py_footprint_export(py, &export_request, 5_000).unwrap();
        let exported = py_to_json_value(exported.bind(py)).unwrap();
        assert!(exported.as_array().unwrap().len() <= 1);

        let packs = PyList::empty_bound(py);
        let imported = py_footprint_import(
            py,
            store.to_str().unwrap(),
            &packs,
            "2026-10-27",
            true,
            5_000,
        )
        .unwrap();
        let imported = py_to_json_value(imported.bind(py)).unwrap();
        assert_eq!(imported["dry_run"], json!(true));

        let cleared = py_footprint_clear(
            py,
            store.to_str().unwrap(),
            spool.to_str().unwrap(),
            5_000,
        )
        .unwrap();
        let cleared = py_to_json_value(cleared.bind(py)).unwrap();
        assert_eq!(cleared["generation"], json!(1));
    });
}
