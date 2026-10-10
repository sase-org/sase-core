use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use serde_json::json;

#[test]
fn agent_artifact_run_retention_binding_refuses_apply_without_protection() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();

    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION,
                    "projects_root": temp.path().to_string_lossy(),
                    "apply": true
                }),
            )
            .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let outcome =
            py_apply_agent_artifact_run_retention(py, request).unwrap();
        let outcome = py_to_json_value(outcome.bind(py)).unwrap();

        assert_eq!(
            outcome["blocked_reason"],
            json!("authoritative_protection_unavailable")
        );
        assert_eq!(outcome["removed_runs"], json!(0));
        assert_eq!(outcome["removed_empty_shards"], json!(0));
        assert_eq!(outcome["bytes_reclaimed"], json!(0));
    });
}

#[test]
fn agent_archive_capability_bindings_are_exported_and_preserve_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "query_agent_archive",
            "agent_archive_facet_counts",
            "validate_agent_archive_key",
            "validate_agent_archive_visibility",
            "validate_agent_archive_capabilities",
            "mark_agent_archive_bundles_revived",
            "verify_agent_archive_index",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let key = PyDict::new_bound(py);
        key.set_item("source_username", "alice").unwrap();
        key.set_item("source_machine", "athena").unwrap();
        key.set_item("source_run_id", "run-123").unwrap();
        let key = py_validate_agent_archive_key(py, &key).unwrap();
        let key = py_to_json_value(key.bind(py)).unwrap();
        assert_eq!(key["source_username"], json!("alice"));
        assert_eq!(key["source_machine"], json!("athena"));
        assert_eq!(key["source_run_id"], json!("run-123"));

        let visibility = PyDict::new_bound(py);
        visibility.set_item("visibility", "pinned").unwrap();
        let visibility =
            py_validate_agent_archive_visibility(py, &visibility).unwrap();
        let visibility = py_to_json_value(visibility.bind(py)).unwrap();
        assert_eq!(visibility["visibility"], json!("pinned"));

        let facts = PyDict::new_bound(py);
        facts.set_item("has_metadata", true).unwrap();
        facts.set_item("has_state", true).unwrap();
        facts.set_item("has_commits", true).unwrap();
        facts.set_item("loader_reconstructible", true).unwrap();
        facts.set_item("has_prompt", false).unwrap();
        facts.set_item("has_model", true).unwrap();
        facts.set_item("has_llm_provider", true).unwrap();
        facts.set_item("has_reasoning_effort", true).unwrap();
        let request = PyDict::new_bound(py);
        request.set_item("facts", &facts).unwrap();
        request.set_item("asserted", py.None()).unwrap();
        let capabilities =
            py_validate_agent_archive_capabilities(py, &request).unwrap();
        let capabilities = py_to_json_value(capabilities.bind(py)).unwrap();
        assert_eq!(capabilities["historically_viewable"], json!(true));
        assert_eq!(capabilities["durably_revivable"], json!(true));
        assert_eq!(capabilities["restartable"], json!(false));
        assert_eq!(capabilities["missing_requirements"], json!(["prompt"]));
    });
}

#[test]
fn status_bucket_binding_round_trips_canonical_text() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let binding = module.getattr("status_bucket_for_status").unwrap();
        for (canonical, bucket) in [
            ("DONE", "Done"),
            ("TALE DONE", "Done"),
            ("EPIC CREATED", "Done"),
            ("FAILED", "Failed"),
            ("PLAN FAILED", "Failed"),
            ("RUNNING", "Running"),
            ("WAITING", "Waiting"),
            ("QUESTION", "Stopped"),
        ] {
            let value = binding.call1((canonical,)).unwrap();
            assert_eq!(
                py_to_json_value(&value).unwrap(),
                json!(bucket),
                "{canonical}"
            );
        }
        let direct = py_status_bucket_for_status(py, "STARTING").unwrap();
        assert_eq!(
            py_to_json_value(direct.bind(py)).unwrap(),
            json!("Starting")
        );
    });
}

#[test]
fn agent_publication_batch_binding_returns_plain_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_plan_agent_publication_batches, &module)
                    .unwrap(),
            )
            .unwrap();
        let records = PyList::empty_bound(py);
        append_json(py, &records, json!({"path": "a.txt", "size_bytes": 4}));
        append_json(py, &records, json!({"path": "b.txt", "size_bytes": 7}));
        let value = module
            .getattr("plan_agent_publication_batches")
            .unwrap()
            .call1((records, 10_u64))
            .unwrap();
        assert_eq!(
            py_to_json_value(&value).unwrap(),
            json!({
                "schema_version": 1,
                "budget_bytes": 10,
                "total_size_bytes": 11,
                "batches": [
                    {"paths": ["a.txt"], "size_bytes": 4},
                    {"paths": ["b.txt"], "size_bytes": 7}
                ]
            })
        );
    });
}

#[test]
fn plan_agent_cleanup_binding_round_trips_json_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let targets = PyList::empty_bound(py);
        append_json(
            py,
            &targets,
            json!({
                "identity": {"agent_type": "run", "cl_name": "done", "raw_suffix": "1"},
                "agent_type": "run",
                "status": "DONE",
                "pid": null,
                "workflow": null,
                "parent_workflow": null,
                "parent_timestamp": null,
                "raw_suffix": "1",
                "project_file": "/tmp/project.sase",
                "artifacts_dir": "/tmp/artifacts",
                "workspace": null,
                "tribe": null,
                "agent_clan": "shipping",
                "agent_clan_generation": "current-gen",
                "agent_name": "done",
                "display_name": "done",
                "start_time": null,
                "stop_time": null,
                "is_workflow_child": false,
                "agent_family_parallel": false,
                "appears_as_agent": false,
                "step_type": null
            }),
        );
        let request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": sase_core::AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
                "scope": "clan",
                "mode": "dismiss_completed",
                "focused_panel_tribe": null,
                "tribe": null,
                "clan_name": "shipping",
                "clan_generation": "current-gen",
                "identities": [],
                "include_pidless_as_dismissable": false
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let result = py_plan_agent_cleanup(py, &targets, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(
            value["schema_version"],
            json!(sase_core::AGENT_CLEANUP_WIRE_SCHEMA_VERSION)
        );
        assert_eq!(
            value["dismiss_items"][0]["identity"]["cl_name"],
            json!("done")
        );
        assert_eq!(value["kill_items"], json!([]));
        assert_eq!(value["confirmation_severity"], json!("dismiss"));
    });
}

#[test]
fn plan_agent_cleanup_binding_rejects_schema_mismatch() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let targets = PyList::empty_bound(py);
        let request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 999,
                "scope": "all_panels",
                "mode": "dismiss_completed"
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let err = py_plan_agent_cleanup(py, &targets, request).unwrap_err();
        assert!(err.to_string().contains("schema mismatch"));
    });
}

#[test]
fn force_reuse_stop_barrier_binding_round_trips_json_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": sase_core::FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION,
                    "targets": [{
                        "name": "worker",
                        "artifacts_dir": "/tmp/worker",
                        "pid": 1234,
                        "was_live": true,
                        "stop_status": "killed",
                        "alive_after_stop": false,
                        "detail": null
                    }]
                }),
            )
            .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let result = py_decide_force_reuse_stop_barrier(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(
            value["schema_version"],
            json!(sase_core::FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION)
        );
        assert_eq!(value["proceed"], json!(true));
        assert_eq!(value["stopped"][0]["name"], json!("worker"));
        assert_eq!(value["unresolved"], json!([]));
    });
}

#[test]
fn plan_agent_ownership_batch_binding_round_trips_json_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": sase_core::AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION,
                    "owner": {"username": "alice", "machine_name": "athena"},
                    "known_owner_roots": ["athena", "alice.athena"],
                    "logical_slots": [{
                        "slot_id": "phase-1",
                        "requested_name": "alpha",
                        "expected_bead_id": "sase-xr.2",
                        "expected_assignee": "sase-xr.2",
                        "expected_owner": {
                            "name": "alpha",
                            "raw_suffix": "ts-a",
                            "artifacts_dir": "/projects/proj/artifacts/workflow/ts-a",
                            "reservation_kind": "claimed",
                            "marker_state": {
                                "status": "DONE",
                                "terminal": true,
                                "cleanup_allowed": true
                            }
                        }
                    }],
                    "cleanup_roots": [{
                        "root_id": "phase-1",
                        "requested_name": "alpha",
                        "expected_bead_id": "sase-xr.2",
                        "expected_assignee": "sase-xr.2",
                        "expected_owner": {
                            "name": "alpha",
                            "agent_name": "alpha",
                            "raw_suffix": "ts-a",
                            "artifacts_dir": "/projects/proj/artifacts/workflow/ts-a",
                            "reservation_kind": "claimed",
                            "marker_state": {
                                "status": "DONE",
                                "terminal": true,
                                "cleanup_allowed": true
                            }
                        }
                    }],
                    "source_records": [{
                        "record_id": "artifact-a",
                        "source_kind": "artifact",
                        "artifact_dir": "/projects/proj/artifacts/workflow/ts-a",
                        "raw_suffix": "ts-a",
                        "canonical_names": ["alpha"],
                        "relation_refs": [],
                        "outgoing_suffixes": []
                    }],
                    "reservation_snapshot": [{
                        "name": "alpha",
                        "source": "artifact",
                        "origin": "local",
                        "artifacts_dir": "/projects/proj/artifacts/workflow/ts-a",
                        "reservation_kind": "planned"
                    }],
                    "reservation_requests": [{
                        "request_id": "claim-alpha",
                        "operation": "claim_planned",
                        "name": "alice.athena.alpha",
                        "artifact_dir": "/projects/proj/artifacts/workflow/ts-a"
                    }]
                }),
            )
            .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let result = py_plan_agent_ownership_batch(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(
            value["schema_version"],
            json!(sase_core::AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION)
        );
        assert_eq!(value["selected_owners"][0]["root_id"], json!("phase-1"));
        assert_eq!(
            value["cleanup_closure"]["artifact_dirs"],
            json!(["/projects/proj/artifacts/workflow/ts-a"])
        );
        assert_eq!(
            value["slot_owner_predicates"][0]["expected_bead_id"],
            json!("sase-xr.2")
        );
        assert_eq!(
            value["reservation_decisions"][0]["storage_name"],
            json!("alpha")
        );
        assert_eq!(
            value["registry_merge_plan"][0]["expected"]["reservation_kind"],
            json!("planned")
        );
    });
}

#[test]
fn plan_agent_ownership_batch_binding_rejects_schema_mismatch() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 999,
                "owner": {"username": "alice", "machine_name": "athena"}
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let err = py_plan_agent_ownership_batch(py, request).unwrap_err();
        assert!(err.to_string().contains("schema mismatch"));
    });
}

#[test]
fn agent_archive_corpus_bindings_round_trip_missing_index() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "compile_agent_archive_corpus",
            "summarize_agent_archive_corpus",
            "rows_agent_archive_corpus",
            "lookup_agent_archive_corpus",
            "count_agent_archive_corpus",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        // A root without an index file compiles to a missing, empty corpus
        // without creating or rebuilding anything on disk.
        let temp = tempfile::tempdir().unwrap();
        let request_obj = json_value_to_py(
            py,
            &json!({
                "root": temp.path().to_string_lossy(),
                "index_status": {"status": "missing"},
                "timezone": "America/New_York",
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let corpus = py_compile_agent_archive_corpus(py, request).unwrap();
        let corpus_value = py_to_json_value(corpus.bind(py)).unwrap();
        assert_eq!(corpus_value["status"], json!({"status": "missing"}));
        assert_eq!(corpus_value["rows"], json!([]));
        assert_eq!(corpus_value["timezone"], json!("America/New_York"));
        assert!(
            !temp.path().join("index.sqlite").exists(),
            "compile must not create the index"
        );
        let corpus_bound = corpus.bind(py);
        let corpus_dict = corpus_bound.downcast::<PyDict>().unwrap();

        let profile = json!({
            "pane_id": "agents-archive",
            "boolean": true,
            "fields": [
                {"key": "name", "exact_match": true, "searchable": true},
                {
                    "key": "outcome",
                    "value_kind": "enum",
                    "static_values": ["done", "failed", "interrupted"],
                },
            ],
        });

        let count_obj =
            json_value_to_py(py, &json!({"query": "", "profile": profile}))
                .unwrap();
        let count_request = count_obj.bind(py).downcast::<PyDict>().unwrap();
        let count =
            py_count_agent_archive_corpus(py, corpus_dict, count_request)
                .unwrap();
        assert_eq!(
            py_to_json_value(count.bind(py)).unwrap(),
            json!({"count": 0})
        );
        // The registered module attribute serves the same call.
        let via_module = module
            .getattr("count_agent_archive_corpus")
            .unwrap()
            .call1((corpus_dict, count_request))
            .unwrap();
        assert_eq!(py_to_json_value(&via_module).unwrap(), json!({"count": 0}));

        let summary_obj = json_value_to_py(
            py,
            &json!({
                "query": "",
                "profile": profile,
                "group_by": "day",
            }),
        )
        .unwrap();
        let summary_request =
            summary_obj.bind(py).downcast::<PyDict>().unwrap();
        let summary =
            py_summarize_agent_archive_corpus(py, corpus_dict, summary_request)
                .unwrap();
        let summary = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(summary["total"], json!(0));
        assert_eq!(summary["groups"], json!([]));

        let rows_obj = json_value_to_py(
            py,
            &json!({
                "query": "",
                "profile": profile,
                "group_by": "day",
                "group_key": "2025-01-01",
                "offset": 0,
                "limit": 10,
            }),
        )
        .unwrap();
        let rows_request = rows_obj.bind(py).downcast::<PyDict>().unwrap();
        let rows = py_rows_agent_archive_corpus(py, corpus_dict, rows_request)
            .unwrap();
        let rows = py_to_json_value(rows.bind(py)).unwrap();
        assert_eq!(rows["rows"], json!([]));
        assert_eq!(rows["total"], json!(0));

        let lookup_obj =
            json_value_to_py(py, &json!({"name": "no-such-agent"})).unwrap();
        let lookup_request = lookup_obj.bind(py).downcast::<PyDict>().unwrap();
        let lookup =
            py_lookup_agent_archive_corpus(py, corpus_dict, lookup_request)
                .unwrap();
        assert_eq!(
            py_to_json_value(lookup.bind(py)).unwrap(),
            json!({"row": null})
        );

        // An unknown group_by is a typed error, not a panic or empty page.
        let bad_group_obj = json_value_to_py(
            py,
            &json!({
                "query": "",
                "profile": profile,
                "group_by": "century",
            }),
        )
        .unwrap();
        let bad_group_request =
            bad_group_obj.bind(py).downcast::<PyDict>().unwrap();
        let err = py_summarize_agent_archive_corpus(
            py,
            corpus_dict,
            bad_group_request,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unsupported archive group_by"),
            "unexpected error: {err}"
        );
    });
}
