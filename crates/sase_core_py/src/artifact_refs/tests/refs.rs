use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::git;
use serde_json::json;
use std::fs;
use std::path::Path;

fn init_git_repo(repo: &Path) {
    fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "--quiet"]);
    git(repo, &["config", "user.name", "Binding Test"]);
    git(repo, &["config", "user.email", "binding@example.com"]);
    git(repo, &["config", "core.abbrev", "7"]);
    // Keep background git maintenance from racing fixture construction (a
    // known class of interference, not a confirmed cause of the commit_at
    // flake seen in CI).
    git(repo, &["config", "gc.auto", "0"]);
    git(repo, &["config", "maintenance.auto", "false"]);
}

fn commit_at(repo: &Path, timestamp: i64, subject: &str, body: &str) -> String {
    let date = format!("{timestamp} +0000");
    let mut command = Command::new("git");
    command.arg("-C").arg(repo).args([
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        subject,
    ]);
    if !body.is_empty() {
        command.args(["-m", body]);
    }
    let output = command
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    git(repo, &["rev-parse", "HEAD"])
}

#[test]
fn artifact_ref_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("plans");
    let target = root.join("202607/plan.md");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "# Plan\n").unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "artifact_ref_parse",
            "artifact_ref_render",
            "artifact_ref_canonicalize",
            "artifact_ref_resolve",
            "artifact_ref_list_normalize",
            "artifact_ref_list_parse",
            "artifact_ref_list_resolve",
            "artifact_ref_list_resolution_wire_schema_version",
            "artifact_ref_context_wire_schema_version",
            "artifact_ref_path_filter_wire_schema_version",
            "artifact_ref_filter_path_payloads",
            "artifact_ref_scan_prompt",
            "artifact_ref_scan_document",
            "artifact_ref_document_scan_wire_schema_version",
            "artifact_ref_split_link_location",
            "artifact_ref_link_location_wire_schema_version",
            "artifact_ref_wire_schema_version",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let parsed =
            py_artifact_ref_parse(py, "plans:202607/plan.md#L2").unwrap();
        let parsed_value = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(parsed_value["schema_version"], json!(5));
        assert_eq!(parsed_value["kind"]["type"], json!("document"));
        assert_eq!(parsed_value["fragment"]["type"], json!("lines"));
        assert_eq!(
            py_artifact_ref_render(parsed.bind(py)).unwrap(),
            "plans:202607/plan.md#L2"
        );
        let mut fragment_free_value = parsed_value.clone();
        fragment_free_value["fragment"] = serde_json::Value::Null;
        let fragment_free = json_value_to_py(py, &fragment_free_value).unwrap();
        assert_eq!(
            py_artifact_ref_render(fragment_free.bind(py)).unwrap(),
            "plans:202607/plan.md"
        );
        let bug = py_artifact_ref_parse(py, "bug:sase#123").unwrap();
        assert_eq!(
            py_artifact_ref_render(bug.bind(py)).unwrap(),
            "bug:sase#123"
        );

        // schema_version 1 deliberately: v1 read-compat regression
        // coverage for `validate_artifact_ref_context`. Do not bump this
        // to 2 — see the v2 case below, which exercises the other end
        // of the supported range.
        let context_value = json!({
            "schema_version": 1,
            "document_roots": [{
                "kind": "plans",
                "root": root.to_string_lossy()
            }]
        });
        let context_object = json_value_to_py(py, &context_value).unwrap();
        let context = context_object.bind(py).downcast::<PyDict>().unwrap();

        assert_eq!(
            py_artifact_ref_canonicalize(target.to_str().unwrap(), context)
                .unwrap()
                .as_deref(),
            Some("plans:202607/plan.md")
        );
        let resolved =
            py_artifact_ref_resolve(py, parsed.bind(py), context).unwrap();
        let resolved = py_to_json_value(resolved.bind(py)).unwrap();
        assert_eq!(resolved["schema_version"], json!(5));
        assert_eq!(resolved["status"], json!("exact"));
        assert_eq!(resolved["resolved_path"], json!(target.to_string_lossy()));

        let files_root = temp.path().join("files");
        fs::create_dir_all(&files_root).unwrap();
        let file_target = files_root.join("notes.md");
        fs::write(&file_target, "notes").unwrap();
        let v2_context_value = json!({
            "schema_version": 2,
            "file_roots": [{
                "name": "notes",
                "path": files_root.to_string_lossy()
            }]
        });
        let v2_context_object =
            json_value_to_py(py, &v2_context_value).unwrap();
        let v2_context =
            v2_context_object.bind(py).downcast::<PyDict>().unwrap();
        let file_parsed = py_artifact_ref_parse(
            py,
            &format!("file:{}", file_target.to_string_lossy()),
        )
        .unwrap();
        let file_resolved =
            py_artifact_ref_resolve(py, file_parsed.bind(py), v2_context)
                .unwrap();
        let file_resolved = py_to_json_value(file_resolved.bind(py)).unwrap();
        assert_eq!(file_resolved["status"], json!("exact"));
        assert_eq!(
            file_resolved["resolved_path"],
            json!(file_target.to_string_lossy())
        );

        let scanned =
            py_artifact_ref_scan_prompt(py, "é @plans:x.md.").unwrap();
        let scanned = py_to_json_value(scanned.bind(py)).unwrap();
        assert_eq!(scanned[0]["candidate_span"]["start"], json!(3));
        assert_eq!(scanned[0]["text"], json!("@plans:x.md"));
        let document_scan = py_artifact_ref_scan_document(
            py,
            "see [plan](plan:202607/plan.md) and @plans:x.md.",
            None,
        )
        .unwrap();
        let document_scan = py_to_json_value(document_scan.bind(py)).unwrap();
        assert_eq!(document_scan["schema_version"], json!(2));
        assert_eq!(
            document_scan["links"][0]["target"],
            json!("plan:202607/plan.md")
        );
        assert_eq!(document_scan["links"][1]["target"], json!("plan:x.md"));

        let linked_repo = temp.path().join("bob-mac-capture");
        fs::create_dir_all(linked_repo.join("Sources/BobMacCapture")).unwrap();
        fs::write(
            linked_repo
                .join("Sources/BobMacCapture/CaptureKeyCommandRouter.swift"),
            "swift",
        )
        .unwrap();
        let repo_context_value = json!({
            "schema_version": 2,
            "repositories": [{
                "name": "bob-mac-capture",
                "checkout_paths": [linked_repo.to_string_lossy()],
            }],
        });
        let repo_context_object =
            json_value_to_py(py, &repo_context_value).unwrap();
        let repo_context =
            repo_context_object.bind(py).downcast::<PyDict>().unwrap();
        let owner_object = json_value_to_py(py, &json!({})).unwrap();
        let owner = owner_object.bind(py).downcast::<PyDict>().unwrap();
        let target_resolved = py_artifact_ref_resolve_document_source_target(
            py,
            "Sources/BobMacCapture/CaptureKeyCommandRouter.swift",
            owner,
            repo_context,
        )
        .unwrap();
        let target_resolved =
            py_to_json_value(target_resolved.bind(py)).unwrap();
        assert_eq!(target_resolved["schema_version"], json!(1));
        assert_eq!(target_resolved["status"], json!("exact"));
        assert_eq!(target_resolved["repository"], json!("bob-mac-capture"));
        assert_eq!(
            target_resolved["resolved_path"],
            json!(linked_repo
                .join("Sources/BobMacCapture/CaptureKeyCommandRouter.swift")
                .canonicalize()
                .unwrap()
                .to_string_lossy())
        );
        assert_eq!(py_artifact_ref_target_resolution_wire_schema_version(), 1);

        assert_eq!(py_artifact_ref_document_scan_wire_schema_version(), 2);
        assert_eq!(py_artifact_ref_link_location_wire_schema_version(), 1);
        let split =
            py_artifact_ref_split_link_location(py, "src/app.py:12:5-40")
                .unwrap();
        let split_value = py_to_json_value(split.bind(py)).unwrap();
        assert_eq!(split_value["schema_version"], json!(1));
        assert_eq!(split_value["base"], json!("src/app.py"));
        assert_eq!(split_value["location"]["line"], json!(12));
        assert_eq!(split_value["location"]["column"], json!(5));
        assert_eq!(split_value["location"]["end_line"], json!(40));
        assert_eq!(py_artifact_ref_wire_schema_version(), 5);
        assert!(py_artifact_ref_parse(py, "commit:sase@BAD").is_err());
        assert_eq!(
            py_artifact_ref_list_normalize(vec![
                "plans:202607/plan.md".to_string(),
                "bead:sase-bb".to_string(),
                "plans:202607/plan.md".to_string(),
            ])
            .unwrap(),
            ["plans:202607/plan.md", "bead:sase-bb"]
        );
        let list_parsed =
            py_artifact_ref_list_parse(py, vec!["bead:sase-bb".to_string()])
                .unwrap();
        assert_eq!(
            py_to_json_value(list_parsed.bind(py)).unwrap()[0]["rendered"],
            json!("bead:sase-bb")
        );
        let list_resolved = py_artifact_ref_list_resolve(
            py,
            vec!["plans:202607/plan.md".to_string(), "broken".to_string()],
            context,
        )
        .unwrap();
        let list_resolved = py_to_json_value(list_resolved.bind(py)).unwrap();
        assert_eq!(list_resolved["schema_version"], json!(2));
        assert_eq!(
            list_resolved["entries"][0]["resolution"]["status"],
            json!("exact")
        );
        assert_eq!(
            list_resolved["entries"][1]["resolution"]["status"],
            json!("unknown_kind")
        );
        assert_eq!(py_artifact_ref_list_resolution_wire_schema_version(), 2);
        assert_eq!(py_artifact_ref_context_wire_schema_version(), 2);
        assert_eq!(py_artifact_ref_path_filter_wire_schema_version(), 1);
    });
}

#[test]
fn artifact_ref_document_source_target_round_trips_stale_and_source_dir() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let live = temp.path().join("live");
    let source_dir = live.join("Sources/BobMacCapture");
    fs::create_dir_all(&source_dir).unwrap();
    fs::write(source_dir.join("Router.swift"), "swift").unwrap();
    Python::with_gil(|py| {
        let context_value = json!({
            "schema_version": 2,
            "repositories": [{
                "name": "capture",
                "checkout_paths": [live.to_string_lossy()],
            }],
        });
        let context_object = json_value_to_py(py, &context_value).unwrap();
        let context = context_object.bind(py).downcast::<PyDict>().unwrap();
        let stale = temp.path().join("deleted-producer");
        let owner_value = json!({
            "repository": "capture",
            "source_directory": source_dir.to_string_lossy(),
            "checkout_candidates": [stale.to_string_lossy()],
        });
        let owner_object = json_value_to_py(py, &owner_value).unwrap();
        let owner = owner_object.bind(py).downcast::<PyDict>().unwrap();
        let resolved = py_artifact_ref_resolve_document_source_target(
            py,
            "Router.swift",
            owner,
            context,
        )
        .unwrap();
        let resolved = py_to_json_value(resolved.bind(py)).unwrap();
        assert_eq!(resolved["status"], json!("exact"));
        assert_eq!(resolved["repository"], json!("capture"));
        assert_eq!(
            resolved["resolved_path"],
            json!(source_dir
                .join("Router.swift")
                .canonicalize()
                .unwrap()
                .to_string_lossy())
        );

        let denied_owner_value = json!({
            "path_globs": ["src/**", "!src/secret.rs"],
        });
        let denied_owner_object =
            json_value_to_py(py, &denied_owner_value).unwrap();
        let denied_owner =
            denied_owner_object.bind(py).downcast::<PyDict>().unwrap();
        let denied_context_value = json!({
            "schema_version": 2,
            "repositories": [{
                "name": "capture",
                "checkout_paths": [live.to_string_lossy()],
            }],
        });
        fs::create_dir_all(live.join("src")).unwrap();
        fs::write(live.join("src/secret.rs"), "secret").unwrap();
        let denied_context_object =
            json_value_to_py(py, &denied_context_value).unwrap();
        let denied_context =
            denied_context_object.bind(py).downcast::<PyDict>().unwrap();
        let denied = py_artifact_ref_resolve_document_source_target(
            py,
            "src/secret.rs",
            denied_owner,
            denied_context,
        )
        .unwrap();
        let denied = py_to_json_value(denied.bind(py)).unwrap();
        assert_eq!(denied["status"], json!("denied"));
        assert_eq!(denied["failure_category"], json!("denied_filtered"));

        let home_error = py_artifact_ref_resolve_document_source_target(
            py,
            "~/.ssh/config",
            owner,
            context,
        )
        .unwrap_err();
        assert!(home_error.is_instance_of::<PyValueError>(py));
        assert!(home_error
            .to_string()
            .contains("home paths belong to the filesystem resolver"));
    });
}

#[test]
fn artifact_ref_document_source_target_rejects_out_of_inventory_source_dir() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let owner_checkout = temp.path().join("owner-checkout");
    fs::create_dir_all(&owner_checkout).unwrap();
    let foreign = temp.path().join("foreign");
    fs::create_dir_all(&foreign).unwrap();
    fs::write(foreign.join("secret.py"), "secret").unwrap();
    Python::with_gil(|py| {
        let context_value = json!({
            "schema_version": 2,
            "repositories": [{
                "name": "owner",
                "checkout_paths": [owner_checkout.to_string_lossy()],
            }],
        });
        let context_object = json_value_to_py(py, &context_value).unwrap();
        let context = context_object.bind(py).downcast::<PyDict>().unwrap();
        let owner_value = json!({
            "repository": "owner",
            "source_directory": foreign.to_string_lossy(),
        });
        let owner_object = json_value_to_py(py, &owner_value).unwrap();
        let owner = owner_object.bind(py).downcast::<PyDict>().unwrap();
        let resolved = py_artifact_ref_resolve_document_source_target(
            py,
            "secret.py",
            owner,
            context,
        )
        .unwrap();
        let resolved = py_to_json_value(resolved.bind(py)).unwrap();
        assert_ne!(resolved["status"], json!("exact"));
        assert!(
            resolved.get("resolved_path").is_none()
                || resolved["resolved_path"].is_null()
        );
        assert_eq!(resolved["failure_category"], json!("proven_missing"));
    });
}

#[test]
fn prompt_artifact_bindings_round_trip_manifest_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "prompt_artifact_pool_filename",
            "prompt_artifact_manifest_parse",
            "prompt_artifact_manifest_render_record",
            "prompt_artifact_manifest_select",
            "prompt_artifact_rewrite_links",
            "prompt_artifact_wire_schema_version",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        assert_eq!(py_prompt_artifact_wire_schema_version(), 2);
        assert_eq!(
            py_prompt_artifact_pool_filename(
                &"a".repeat(64),
                "../../diagram.png"
            ),
            "aaaaaaaaaaaa-diagram.png"
        );
        let record_value = json!({
            "schema_version": 2,
            "recorded_at": "2026-08-01T14:22:03Z",
            "agent_artifacts_dir": "/artifacts/run",
            "raw_ref": "@~/diagram.png",
            "expanded_ref": "@.sase/artifacts/home/diagram.png",
            "ref_kind": "file",
            "label": "diagram.png",
            "source_path": null,
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "size_bytes": 42,
            "mime_type": "image/png",
            "pool_relpath": "pool/aaaaaaaaaaaa-diagram.png",
            "vcs_repo": null,
            "vcs_relpath": null,
            "vcs_revision": null,
            "locator": null,
            "skipped_reason": null,
            "logical_path": null,
            "root_name": null,
            "authored_path": null,
            "origin": null,
            "object_relpath": null,
            "sidecar_visibility": null
        });
        let record_object = json_value_to_py(py, &record_value).unwrap();
        let record = record_object.bind(py).downcast::<PyDict>().unwrap();
        let rendered =
            py_prompt_artifact_manifest_render_record(record).unwrap();
        let parsed = py_prompt_artifact_manifest_parse(
            py,
            &PyBytes::new_bound(py, rendered.as_bytes()),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(parsed.bind(py)).unwrap(),
            json!([record_value.clone()])
        );

        let records_object =
            json_value_to_py(py, &json!([record_value])).unwrap();
        let records = records_object.bind(py).downcast::<PyList>().unwrap();
        let selected =
            py_prompt_artifact_manifest_select(py, records, "/artifacts/run")
                .unwrap();
        assert_eq!(
            py_to_json_value(selected.bind(py))
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let resolver_module = PyModule::from_code_bound(
            py,
            "def resolve(record):\n    return 'archive.png'\n",
            "resolver.py",
            "resolver",
        )
        .unwrap();
        let resolver = resolver_module.getattr("resolve").unwrap();
        let rewritten = py_prompt_artifact_rewrite_links(
            py,
            "Open @~/diagram.png.",
            records,
            &resolver,
        )
        .unwrap();
        let rewritten = py_to_json_value(rewritten.bind(py)).unwrap();
        assert_eq!(
            rewritten["prompt"],
            json!("Open [@~/diagram.png][1].\n\n[1]: archive.png")
        );
        assert_eq!(rewritten["linked_records"].as_array().unwrap().len(), 1);
        assert_eq!(
            rewritten["reference_definitions"],
            json!([{"label": "1", "destination": "archive.png"}])
        );
        assert_eq!(
            rewritten["reference_labels"],
            json!([{
                "raw_ref": "@~/diagram.png",
                "label": "1",
                "destination": "archive.png"
            }])
        );
    });
}

#[test]
fn artifact_ref_payload_inventory_binding_returns_plain_json_shape() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    init_git_repo(&repo);
    let sha = commit_at(
        &repo,
        1_700_000_000,
        "binding inventory subject",
        "body line\nsecond line",
    );

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_artifact_ref_payload_inventory, &module)
                    .unwrap(),
            )
            .unwrap();

        // schema_version 1 deliberately: v1 read-compat regression
        // coverage for `validate_artifact_ref_context`. Do not bump this
        // to 2 — sase_core_py::tests::artifact_ref_bindings_round_trip_json_shapes
        // covers the v2 side of the supported range.
        let context = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "repositories": [{
                    "name": "sase-core",
                    "checkout_paths": [repo.to_string_lossy()],
                }],
            }),
        )
        .unwrap();
        let inventory = module
            .getattr("artifact_ref_payload_inventory")
            .unwrap()
            .call1(("commit", context.bind(py)))
            .unwrap();
        let inventory = py_to_json_value(&inventory).unwrap();

        assert_eq!(inventory["truncated_payloads"], json!(0));
        assert_eq!(inventory["payloads"].as_array().unwrap().len(), 1);
        let row = &inventory["payloads"][0];
        assert_eq!(row["payload"], json!(format!("sase-core@{}", &sha[..12])));
        assert_eq!(row["label"], json!("binding inventory subject"));
        assert_eq!(row["detail"], json!(""));
        assert!(row["age"].as_str().is_some());
        assert_eq!(row["scope"], json!("sase-core"));
        assert_eq!(row["rank"], json!(0));
        assert_eq!(row["body"], json!("body line\nsecond line"));

        let bad_context = json_value_to_py(
            py,
            &json!({"schema_version": 1, "repositories": "invalid"}),
        )
        .unwrap();
        let error = module
            .getattr("artifact_ref_payload_inventory")
            .unwrap()
            .call1(("commit", bad_context.bind(py)))
            .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn artifact_ref_filter_path_payloads_binding_returns_batch_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let filter =
            module.getattr("artifact_ref_filter_path_payloads").unwrap();

        let result = filter
            .call1((
                "research",
                vec!["README.md", "drafts/a.md", "image.png"],
                vec!["**/*.md", "!drafts/**"],
            ))
            .unwrap();
        let result = py_to_json_value(&result).unwrap();
        assert_eq!(result["schema_version"], json!(1));
        assert_eq!(result["kind"], json!("research"));
        assert_eq!(result["allowed"], json!(["README.md"]));
        assert_eq!(result["filtered"], json!(["drafts/a.md", "image.png"]));

        let error = filter
            .call1(("research", vec!["README.md"], vec![""]))
            .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}
