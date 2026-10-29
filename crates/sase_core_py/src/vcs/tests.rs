use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use serde_json::json;
use std::fs;

#[test]
fn git_object_sharing_binding_preserves_existing_reuse() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let objects = temp.path().join("borrower/.git/objects");
    let old = temp.path().join("old/.git/objects");
    let primary = temp.path().join("primary/.git/objects");
    fs::create_dir_all(&old).unwrap();
    fs::create_dir_all(&primary).unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module
            .getattr("git_object_sharing_wire_schema_version")
            .is_ok());
        assert!(module.getattr("plan_git_object_sharing").is_ok());

        let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION,
                    "operation": "install",
                    "checkout_dir": temp.path().join("borrower").to_string_lossy(),
                    "object_dir": objects.to_string_lossy(),
                    "alternates_file": objects.join("info/alternates").to_string_lossy(),
                    "primary_checkout_dir": temp.path().join("primary").to_string_lossy(),
                    "primary_object_dir": primary.to_string_lossy(),
                    "alternates": [old.to_string_lossy()],
                    "config_enabled": true,
                    "config_primary_objects": old.to_string_lossy(),
                    "mutation_context": "existing_reuse"
                }),
            )
            .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_plan_git_object_sharing(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(result["action"], json!("none"));
        assert_eq!(result["status"], json!("preserved"));
        assert_eq!(result["dependency_mutation"], json!(false));
        assert_eq!(result["write_alternates"], JsonValue::Null);
    });
}

#[test]
fn vcs_log_binding_exposes_schema_and_parent_ids() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        use sase_core::vcs_log::parsers::{RECORD_SEP, UNIT_SEP};

        assert_eq!(py_vcs_log_wire_schema_version(), 4);

        let stdout = format!(
                "full{US}short{US}A{US}a@example.com{US}42{US}p1 p2{US}subject{US}body{RS}",
                US = UNIT_SEP,
                RS = RECORD_SEP,
            );
        let parsed = py_parse_git_log(py, &stdout).unwrap();
        let value = py_to_json_value(parsed.as_any()).unwrap();
        assert_eq!(
            value,
            json!([{
                "full_id": "full",
                "short_id": "short",
                "author_name": "A",
                "author_email": "a@example.com",
                "timestamp": 42,
                "parent_ids": ["p1", "p2"],
                "subject": "subject",
                "body": "body",
                "presence": "unknown",
                "origin": "manual",
            }])
        );
    });
}

#[test]
fn classify_commit_origin_binding_returns_origin_string() {
    assert_eq!(py_classify_commit_origin("fix: manual\n\nBody"), "manual",);
    assert_eq!(
        py_classify_commit_origin("fix: tracked\n\nSASE_TYPE=stitch"),
        "stitch",
    );
    assert_eq!(
        py_classify_commit_origin("fix: automatic\n\nSASE_TYPE=sase init"),
        "auto",
    );
}

#[test]
fn classify_commit_types_binding_returns_label_list() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let commit = PyDict::new_bound(py);
        commit.set_item("full_id", "full").unwrap();
        commit.set_item("short_id", "short").unwrap();
        commit.set_item("author_name", "A").unwrap();
        commit.set_item("author_email", "a@example.com").unwrap();
        commit.set_item("timestamp", 42).unwrap();
        commit.set_item("parent_ids", vec!["p1", "p2"]).unwrap();
        commit.set_item("subject", "Merge tracked work").unwrap();
        commit
            .set_item(
                "body",
                "Details\n\nSASE_TYPE=bead_work\nSASE_PATCH=feat-x",
            )
            .unwrap();
        commit.set_item("presence", "unknown").unwrap();
        commit.set_item("origin", "manual").unwrap();

        let labels = py_classify_commit_types(py, commit.as_any()).unwrap();
        let value = py_to_json_value(labels.as_any()).unwrap();
        assert_eq!(value, json!(["automatic", "bead_work", "merge", "patch"]));
    });
}

#[test]
fn parse_merge_summary_binding_returns_dict_or_none() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let summary = py_parse_merge_summary(
            py,
            "Merge pull request #123 from org/feature",
            "\nFeature title\n\nDetails",
        )
        .unwrap();
        let value = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(
            value,
            json!({
                "kind": "pull_request",
                "reference": "123",
                "source": "org/feature",
                "target": null,
                "headline": "Feature title",
            })
        );

        assert!(py_parse_merge_summary(py, "Merge unknown shape", "")
            .unwrap()
            .is_none(py));
    });
}

#[test]
fn commit_footer_bindings_convert_linked_payloads() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_commit_footer_wire_schema_version(),
            COMMIT_FOOTER_WIRE_SCHEMA_VERSION
        );
        let updates = PyList::empty_bound(py);
        append_json(
            py,
            &updates,
            json!({
                "key": "PLAN",
                "label": "202607/p.md",
                "destination": "https://github.com/o/r/blob/main/202607/p.md",
                "reference_id": null
            }),
        );
        let message =
            py_update_commit_footer("Subject", &updates, vec![]).unwrap();
        assert!(message.contains("SASE_PLAN=[202607/p.md][1]"));

        let parsed = py_parse_commit_footer(py, &message).unwrap();
        let value = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["tags"][0]["label"], json!("202607/p.md"));
        assert_eq!(
            value["tags"][0]["destination"],
            json!("https://github.com/o/r/blob/main/202607/p.md")
        );
    });
}

#[test]
fn commit_subject_bindings_round_trip_wire_payload() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_commit_subject_wire_schema_version(),
            COMMIT_SUBJECT_WIRE_SCHEMA_VERSION
        );
        let allowed_types = py_default_commit_subject_types();
        assert_eq!(allowed_types.first().map(String::as_str), Some("build"));

        let parsed = py_parse_commit_subject(
            py,
            "feat(binding)!: expose subject parser\n\nBody",
            allowed_types,
        )
        .unwrap();
        let value = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(
            value["subject"],
            json!("feat(binding)!: expose subject parser")
        );
        assert_eq!(value["valid"], json!(true));
        assert_eq!(value["exempt"], json!(false));
        assert_eq!(value["commit_type"], json!("feat"));
        assert_eq!(value["scope"], json!("binding"));
        assert_eq!(value["breaking"], json!(true));
        assert_eq!(value["description"], json!("expose subject parser"));
        assert_eq!(value["violation"], JsonValue::Null);
        assert_eq!(value["found_type"], JsonValue::Null);
    });
}

#[test]
fn pending_commit_checkpoint_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert_eq!(
            module
                .getattr("pending_commit_checkpoint_wire_schema_version")
                .unwrap()
                .call0()
                .unwrap()
                .extract::<u32>()
                .unwrap(),
            2
        );
        let request = json_value_to_py(
                py,
                &json!({
                    "checkpoint_present": true,
                    "repository_matches": true,
                    "subject_matches": true,
                    "payload_matches": true,
                    "checkpoint_method": "create_commit",
                    "accepted_action": "commit",
                    "checkpoint_payload_identity": "fix(final): reconcile commit declaration\n\nbody",
                    "accepted_payload_identity": "fix(final): reconcile commit declaration\n\nbody",
                    "checkpoint_run_id": "run-1",
                    "current_run_id": "run-1",
                    "checkpoint_agent_id": "agent-1",
                    "current_agent_id": "agent-1",
                    "has_operation_id": true,
                    "dispatch_completed": true,
                    "pending_after_hook": true,
                    "commit_sha_present": true
                }),
            )
            .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let decision = module
            .getattr("decide_pending_commit_checkpoint_recovery")
            .unwrap()
            .call1((request,))
            .unwrap();
        let decision = py_to_json_value(&decision).unwrap();
        assert_eq!(decision["action"], json!("resume"));
        assert_eq!(decision["schema_version"], json!(2));
    });
}

#[test]
fn rebase_gate_bindings_round_trip_context_updates_and_verdicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert_eq!(
            module
                .getattr("rebase_gate_wire_schema_version")
                .unwrap()
                .call0()
                .unwrap()
                .extract::<u32>()
                .unwrap(),
            1
        );

        let facts = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "repository_root": "/work/project",
                "old_head": "1111111111111111111111111111111111111111",
                "upstream_tip": "2222222222222222222222222222222222222222",
                "merge_base": "3333333333333333333333333333333333333333",
                "rebase_outcome": "applied",
                "current_head": "4444444444444444444444444444444444444444",
                "current_tree": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "local_changes": [{
                    "path": "src/deleted.py",
                    "kind": "deleted"
                }],
                "upstream_changes": [{
                    "path": "src/new.py",
                    "kind": "renamed",
                    "old_path": "src/old.py"
                }]
            }),
        )
        .unwrap();
        let facts = facts.bind(py).downcast::<PyDict>().unwrap();
        let context = module
            .getattr("build_rebase_gate_context")
            .unwrap()
            .call1((facts,))
            .unwrap();
        let context = py_to_json_value(&context).unwrap();
        assert_eq!(context["schema_version"], json!(1));
        assert_eq!(context["changed_paths"].as_array().unwrap().len(), 2);

        let merged_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "existing": context,
                "incoming": context
            }),
        )
        .unwrap();
        let merged_request =
            merged_request.bind(py).downcast::<PyDict>().unwrap();
        let merged = module
            .getattr("merge_rebase_gate_contexts")
            .unwrap()
            .call1((merged_request,))
            .unwrap();
        let merged = py_to_json_value(&merged).unwrap();
        assert_eq!(merged["inputs"].as_array().unwrap().len(), 1);

        let update_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "context": merged,
                "update": {
                    "schema_version": 1,
                    "repository_root": "/work/project",
                    "current_head": "5555555555555555555555555555555555555555",
                    "current_tree": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "changed_paths": [{"path": "repair.py", "kind": "modified"}]
                }
            }),
        )
        .unwrap();
        let update_request =
            update_request.bind(py).downcast::<PyDict>().unwrap();
        let updated = module
            .getattr("update_rebase_gate_context")
            .unwrap()
            .call1((update_request,))
            .unwrap();
        let updated = py_to_json_value(&updated).unwrap();
        assert_eq!(
            updated["current_head"],
            json!("5555555555555555555555555555555555555555")
        );
        assert_eq!(updated["changed_paths"].as_array().unwrap().len(), 3);

        let classify_request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "context": updated,
                "config": {"hook_command": "checker", "timeout_seconds": 10},
                "command": {"exit_code": 1, "timed_out": false},
                "report": {
                    "schema_version": 1,
                    "checked_head": "5555555555555555555555555555555555555555",
                    "checked_tree": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "status": "findings",
                    "findings": [{
                        "path": "repair.py",
                        "line": 2,
                        "module": "sase.old",
                        "imported_name": "Thing",
                        "explanation": "missing imported name"
                    }],
                    "diagnostics": null
                }
            }),
        )
        .unwrap();
        let classify_request =
            classify_request.bind(py).downcast::<PyDict>().unwrap();
        let decision = module
            .getattr("classify_rebase_gate_result")
            .unwrap()
            .call1((classify_request,))
            .unwrap();
        let decision = py_to_json_value(&decision).unwrap();
        assert_eq!(decision["verdict"], json!("block"));
        assert_eq!(decision["findings"].as_array().unwrap().len(), 1);
    });
}

#[test]
fn session_manifest_binding_classifies_current_legacy_and_slim() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(py_session_manifest_wire_schema_version(), 1);
        let snapshot = json!({
            "owner_username": "alice",
            "owner_machine": "athena",
            "local_hood": "foo",
            "run_global_names": ["alice.athena.foo.bar"],
            "run_file_paths": ["agents/alice.athena.foo.bar/meta.json"],
            "containers": [
                {"kind": "session", "global_name": "alice.athena.foo.bar.baz"}
            ],
        });
        let canonical: Vec<String> = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "snapshot": snapshot,
                    "explicit_files": null,
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result =
                py_classify_session_manifest_files(py, request).unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(value["classification"], json!("slim"));
            value["canonical_files"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect()
        };
        assert!(canonical
            .contains(&"sessions/alice.athena.foo.bar.baz.md".to_string()));
        for (explicit, expected) in [
            (
                serde_json::Value::Array(
                    canonical.iter().map(|item| json!(item)).collect(),
                ),
                "current",
            ),
            (
                serde_json::Value::Array(
                    canonical
                        .iter()
                        .filter(|path| !path.starts_with("sessions/"))
                        .map(|item| json!(item))
                        .collect(),
                ),
                "supported_legacy",
            ),
        ] {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "snapshot": snapshot,
                    "explicit_files": explicit,
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result =
                py_classify_session_manifest_files(py, request).unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(value["classification"], json!(expected));
        }
    });
}

#[test]
fn session_manifest_repair_binding_round_trips_policy_and_counts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let snapshot = json!({
            "owner_username": "alice",
            "owner_machine": "athena",
            "local_hood": "foo",
            "run_global_names": ["alice.athena.foo.bar"],
            "run_file_paths": ["agents/alice.athena.foo.bar/meta.json"],
            "containers": [
                {"kind": "session", "global_name": "alice.athena.foo.bar.baz"}
            ],
        });
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "snapshot": snapshot,
                "explicit_files": [
                    "agents/alice.athena.foo.bar/README.md",
                    "agents/alice.athena.foo.retired/README.md",
                ],
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_repair_session_manifest_entry(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["action"], json!("normalize_slim"));
        assert_eq!(value["classification"], json!("invalid"));
        assert_eq!(value["run_count"], json!(1));
        assert_eq!(value["agent_session_count"], json!(1));
        assert_eq!(value["schema_version"], json!(1));
    });
}

#[test]
fn replay_recovery_bindings_classify_and_admit() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let classified = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "paths": [
                        "README.md",
                        "agents/alice.athena.foo/chat.md",
                        "files/objects/sha256/ab/abc",
                    ],
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_classify_replay_paths(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(classified["browsing"], json!(["README.md"]));
        assert_eq!(
            classified["owner_content"],
            json!(["agents/alice.athena.foo/chat.md"])
        );
        assert_eq!(
            classified["archive_object"],
            json!(["files/objects/sha256/ab/abc"])
        );

        let refused = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "current_hex": "aaaaaaaaaa",
                    "previous_hex": "bbbb",
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_decide_replay_byte_divergence(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(refused["action"], json!("refuse"));
        assert_eq!(refused["retained"], json!(false));

        let merge_base = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "current_hex": "6c6f63616c",
                    "previous_hex": "62617365",
                    "base_hex": "62617365",
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_decide_replay_byte_divergence(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(merge_base["action"], json!("keep_current"));

        let merge = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "local_owner_username": "alice",
                    "local_owner_machine": "athena",
                    "local_hood": "foo",
                    "upstream_owner_username": "alice",
                    "upstream_owner_machine": "athena",
                    "upstream_hood": "foo",
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_decide_replay_hood_merge(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(merge["action"], json!("merge"));

        let admitted = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "apply": true,
                    "observed_head": "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e",
                    "expected_head": "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e",
                    "backup_complete": true,
                    "backup_verified": true,
                    "unreconcilable": false,
                    "already_complete": false,
                    "mutation_started": false,
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_decide_replay_apply(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(admitted["action"], json!("apply"));
    });
}

#[test]
fn publication_recovery_bindings_select_and_complete() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(py_publication_recovery_wire_schema_version(), 1);
        let selected = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "retry_retired": true,
                    "retry_quarantined": false,
                    "rows": [
                        {
                            "global_agent": "alice.athena.foo",
                            "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "terminal": true,
                            "quarantined": false,
                            "last_error": "legacy mismatch",
                            "terminal_reason": "legacy mismatch"
                        },
                        {
                            "global_agent": "alice.athena.bar",
                            "primary_revision": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            "terminal": false,
                            "quarantined": true,
                            "last_error": "temp",
                            "terminal_reason": null
                        }
                    ]
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_select_publication_retries(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(selected["selected"].as_array().unwrap().len(), 1);
        assert_eq!(
            selected["selected"][0]["global_agent"],
            json!("alice.athena.foo")
        );
        assert_eq!(selected["selected"][0]["error_class"], json!("other"));

        let classified = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "error": "error: Your index contains uncommitted changes."
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_classify_publication_error(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(classified["error_class"], json!("unstaged_changes"));

        let accounting = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "scope": "repository",
                    "error": "agents sync lock is busy",
                    "attempted_keys": [{
                        "global_agent": "alice.athena.foo",
                        "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    }],
                    "rows": [{
                        "global_agent": "alice.athena.foo",
                        "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "attempts": 2
                    }]
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result =
                py_decide_publication_failure_accounting(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(accounting["charge"], json!(false));
        assert_eq!(accounting["scope"], json!("repository"));
        assert_eq!(accounting["error_class"], json!("lock_busy"));
        assert_eq!(accounting["charges"], json!([]));

        let drain = {
            let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "limit": 1,
                    "retry_retired": true,
                    "retry_quarantined": true,
                    "rows": [
                        {
                            "global_agent": "alice.athena.foo",
                            "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "terminal": false,
                            "quarantined": false,
                            "last_error": null,
                            "created_at": 2.0
                        },
                        {
                            "global_agent": "alice.athena.bar",
                            "primary_revision": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            "terminal": true,
                            "quarantined": false,
                            "last_error": "legacy mismatch",
                            "terminal_reason": "legacy mismatch",
                            "created_at": 1.0
                        }
                    ]
                }),
            )
            .unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            let result = py_select_publication_drain(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        assert_eq!(drain["active"].as_array().unwrap().len(), 1);
        assert_eq!(
            drain["active"][0]["global_agent"],
            json!("alice.athena.foo")
        );
        assert_eq!(drain["revive"], json!([]));

        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "request": {
                    "global_agent": "alice.athena.foo",
                    "local_agent": "foo",
                    "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                },
                "pages": [{
                    "path": "agents/alice.athena.foo/README.md",
                    "exists": true
                }],
                "runs": [{
                    "global_name": "alice.athena.foo",
                    "local_name": "foo",
                    "commit_shas": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                    "has_prompt_file": false
                }],
                "containers": []
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result =
            py_decide_publication_request_completion(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["fulfilled"], json!(true));
        assert_eq!(value["kind"], json!("run"));

        let linked = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "request": {
                    "global_agent": "alice.athena.foo",
                    "local_agent": "foo",
                    "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "scope": "linked"
                },
                "pages": [{
                    "path": "agents/alice.athena.foo/README.md",
                    "exists": true
                }],
                "runs": [{
                    "global_name": "alice.athena.foo",
                    "local_name": "foo",
                    "commit_shas": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
                    "has_prompt_file": false
                }],
                "containers": []
            }),
        )
        .unwrap();
        let linked = linked.bind(py).downcast::<PyDict>().unwrap();
        let linked_result =
            py_decide_publication_request_completion(py, linked).unwrap();
        let linked_value = py_to_json_value(linked_result.bind(py)).unwrap();
        assert_eq!(linked_value["fulfilled"], json!(true));
        assert_eq!(linked_value["kind"], json!("run"));
        assert!(linked_value["reason"]
            .as_str()
            .unwrap()
            .contains("linked-scope"));
    });
}

#[test]
fn publication_drain_binding_honors_exact_key() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let drain = |payload: serde_json::Value| {
            let request = json_value_to_py(py, &payload).unwrap();
            let request = request.bind(py).downcast::<PyDict>().unwrap();
            py_select_publication_drain(py, request)
                .map(|result| py_to_json_value(result.bind(py)).unwrap())
                .map_err(|error| error.to_string())
        };
        let rows = json!([
            {
                "global_agent": "alice.athena.active",
                "primary_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "terminal": false,
                "quarantined": false,
                "last_error": "active",
                "created_at": 2.0
            },
            {
                "global_agent": "alice.athena.target",
                "primary_revision": "cccccccccccccccccccccccccccccccccccccccc",
                "terminal": false,
                "quarantined": true,
                "last_error": "agent-hood publication did not complete within 120s",
                "created_at": 1.0
            }
        ]);
        let exact = json!({
            "global_agent": "alice.athena.target",
            "primary_revision": "cccccccccccccccccccccccccccccccccccccccc"
        });

        let hit = drain(json!({
            "schema_version": 1,
            "limit": 2,
            "retry_retired": false,
            "retry_quarantined": true,
            "rows": rows,
            "error_classes": ["timeout"],
            "exact_key": exact,
        }))
        .unwrap();
        assert_eq!(hit["active"], json!([]));
        assert_eq!(hit["revive"].as_array().unwrap().len(), 1);
        assert_eq!(
            hit["revive"][0]["global_agent"],
            json!("alice.athena.target")
        );

        let miss = drain(json!({
            "schema_version": 1,
            "limit": 2,
            "retry_retired": false,
            "retry_quarantined": true,
            "rows": rows,
            "error_classes": ["timeout"],
            "exact_key": {
                "global_agent": "alice.athena.gone",
                "primary_revision": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
        }))
        .unwrap();
        assert_eq!(miss["active"], json!([]));
        assert_eq!(miss["revive"], json!([]));

        let ambiguous = drain(json!({
            "schema_version": 1,
            "limit": 2,
            "retry_retired": false,
            "retry_quarantined": true,
            "rows": rows,
            "error_classes": ["timeout"],
            "preferred_key": exact,
            "exact_key": exact,
        }));
        assert!(ambiguous.is_err());
        assert!(ambiguous.unwrap_err().contains("mutually exclusive"));
    });
}

#[test]
fn stale_publication_repair_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "requested_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "scope": "revision",
                "run_commit_shas": [
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "cccccccccccccccccccccccccccccccccccccccc"
                ],
                "ancestor_commit_shas": [
                    "cccccccccccccccccccccccccccccccccccccccc"
                ],
                "linked_revision": false
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result =
            py_classify_stale_publication_request(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["action"], json!("repaired"));
        assert_eq!(
            value["fulfilling_revision"],
            json!("cccccccccccccccccccccccccccccccccccccccc")
        );
        assert_eq!(value["schema_version"], json!(1));
    });
}

#[test]
fn sidecar_publication_binding_returns_plain_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_decide_sidecar_publication_after_push,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let value = module
            .getattr("decide_sidecar_publication_after_push")
            .unwrap()
            .call1((
                1_i32,
                "",
                "! [rejected] main -> main (fetch first)",
                1_u32,
            ))
            .unwrap();
        assert_eq!(
            py_to_json_value(&value).unwrap(),
            json!({
                "schema_version": 1,
                "action": "integrate_and_retry",
                "classification": "rejected_fetch_first",
                "reason": "git push was rejected by remote divergence; integrate upstream and retry",
                "attempt": 1,
                "max_attempts": 3,
                "retryable": true
            })
        );
    });
}

#[test]
fn sidecar_publication_rejection_binding_returns_sanitized_classification() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let secret = "SYNTHETIC_SECRET_DO_NOT_RETURN";
        let value = module
            .getattr("classify_sidecar_publication_push_result")
            .unwrap()
            .call1((
                1_i32,
                "",
                format!("GH013: {secret}; fetch first; path: agents/a/chat.md"),
            ))
            .unwrap();
        let value = py_to_json_value(&value).unwrap();
        assert_eq!(value["classification"], json!("rejected_secret_scan"));
        assert_eq!(value["relative_paths"], json!(["agents/a/chat.md"]));
        assert!(!value.to_string().contains(secret));
    });
}

#[test]
fn git_lock_bindings_round_trip_classify_and_decide() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert_eq!(
            module
                .getattr("git_lock_wire_schema_version")
                .unwrap()
                .call0()
                .unwrap()
                .extract::<u32>()
                .unwrap(),
            1
        );
        let delays = module
            .getattr("parse_git_lock_retry_delays")
            .unwrap()
            .call1(("0.1,0.2",))
            .unwrap()
            .extract::<Vec<f64>>()
            .unwrap();
        assert_eq!(delays, vec![0.1, 0.2]);

        let observation = json_value_to_py(
            py,
            &json!({
                "exit_status": 128,
                "stdout": "",
                "stderr": "error: Unable to create '/repo/.git/index.lock': File exists.",
                "timed_out": false,
                "signal_terminated": false,
                "resolved_index_lock_path": "/repo/.git/index.lock"
            }),
        )
        .unwrap();
        let classified = module
            .getattr("classify_git_lock_failure")
            .unwrap()
            .call1((observation.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let classified_value = py_to_json_value(&classified).unwrap();
        assert_eq!(classified_value["kind"], json!("index_lock"));
        assert_eq!(classified_value["retryable"], json!(true));

        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "failure": classified_value,
                "delays": [0.1, 0.2],
                "allow_lock_removal": true,
                "attempts": 3,
                "now_unix_ns": 20_000_000_000i64,
                "remaining_recovery_budget_ns": 30_000_000_000i64,
                "cancelled": false,
                "removal_already_attempted": false,
                "identity_disqualified": false,
                "first_snapshot": {
                    "path": "/repo/.git/index.lock",
                    "file_type": "regular",
                    "device": 1,
                    "inode": 4,
                    "size": 8,
                    "mtime_ns": 0,
                    "ctime_ns": 0
                },
                "current_snapshot": {
                    "path": "/repo/.git/index.lock",
                    "file_type": "regular",
                    "device": 1,
                    "inode": 4,
                    "size": 8,
                    "mtime_ns": 0,
                    "ctime_ns": 0
                },
                "path_match": {
                    "schema_version": 1,
                    "matched_path": "/repo/.git/index.lock",
                    "eligible_for_removal": true,
                    "reason": "matched"
                },
                "owner": {
                    "schema_version": 1,
                    "status": "absent",
                    "reason": "none"
                }
            }),
        )
        .unwrap();
        let decision = module
            .getattr("decide_git_lock_recovery")
            .unwrap()
            .call1((request.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let decision_value = py_to_json_value(&decision).unwrap();
        assert_eq!(decision_value["action"], json!("remove_then_retry"));
        assert_eq!(decision_value["claim_lock_removed"], json!(true));
    });
}
