use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;
use std::fs;

#[test]
fn scan_agent_artifacts_binding_preserves_canonical_and_legacy_capacity() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let dir = root
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20260913030000");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
                dir.join("agent_meta.json"),
                r#"{"name":"canonical","queue_capacity":100,"queue_capacity_explicit":true}"#,
            )
            .unwrap();
        fs::write(
                dir.join("waiting.json"),
                r#"{"wait_runners":0,"wait_runners_explicit":true,"queue_capacity":100,"queue_capacity_explicit":true}"#,
            )
            .unwrap();
        let snapshot =
            py_scan_agent_artifacts(py, root.to_string_lossy().as_ref(), None)
                .unwrap();
        let snapshot = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot["schema_version"], json!(12));
        let record = &snapshot["records"][0];
        assert_eq!(record["agent_meta"]["queue_capacity"], json!(100));
        assert_eq!(
            record["agent_meta"]["queue_capacity_explicit"],
            json!(true)
        );
        assert_eq!(record["waiting"]["queue_capacity"], json!(100));
        assert!(record["waiting"].get("wait_runners").is_none());
    });
}

#[test]
fn scan_agent_artifacts_binding_round_trips_process_identity() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let dir = root
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20260913030100");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("agent_meta.json"),
            r#"{"name":"identity","pid":99,"process_identity":"boot:99"}"#,
        )
        .unwrap();
        fs::write(
            dir.join("running.json"),
            r#"{"pid":99,"process_identity":"boot:100"}"#,
        )
        .unwrap();

        let snapshot =
            py_scan_agent_artifacts(py, root.to_string_lossy().as_ref(), None)
                .unwrap();
        let snapshot = py_to_json_value(snapshot.bind(py)).unwrap();
        let record = &snapshot["records"][0];
        assert_eq!(record["agent_meta"]["process_identity"], json!("boot:99"));
        assert_eq!(record["running"]["process_identity"], json!("boot:100"));
    });
}

#[test]
fn terminalize_binding_round_trips_diagnostic_fields() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        fs::create_dir_all(&projects).unwrap();
        let index = temp.path().join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let module = PyModule::new_bound(py, "agent_scan_test").unwrap();
        register_agent_scan(&module).unwrap();
        let result = module
            .getattr("terminalize_stale_active_agent_artifact_index_rows")
            .unwrap()
            .call1((
                index.to_string_lossy().as_ref(),
                projects.to_string_lossy().as_ref(),
                0u64,
            ))
            .unwrap();
        let result = py_to_json_value(&result).unwrap();
        assert_eq!(result["terminalization_rows_examined"], json!(0));
        assert_eq!(result["terminalization_source_done_repairs"], json!(0));
        assert_eq!(result["terminalization_synthesized_rows"], json!(0));
        assert_eq!(result["terminalization_skipped_live"], json!(0));
        assert_eq!(result["terminalization_skipped_claim"], json!(0));
        assert_eq!(result["terminalization_skipped_fresh"], json!(0));
        assert_eq!(result["terminalization_skipped_unknown"], json!(0));
        assert_eq!(result["terminalization_skipped_race"], json!(0));
        assert_eq!(result["terminalization_wrapped"], json!(true));
    });
}

fn temp_agent_stats_root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("sase-core-py-agent-stats-")
        .tempdir()
        .unwrap()
}

#[test]
fn agent_model_summary_binding_round_trips_python_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
            py,
            &json!({
                "turns": [
                    {"kind": "agent", "provider": "codex", "model": "gpt-5"},
                    {"kind": "agent", "provider": "claude", "model": "opus"},
                    {"kind": "agent", "provider": "codex", "model": "gpt-5"},
                    {"kind": "monitor", "provider": "codex", "model": "ignored"},
                    {"kind": "agent", "provider": null, "model": "@unresolved"}
                ]
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let module = PyModule::new_bound(py, "agent_scan_test").unwrap();
        register_agent_scan(&module).unwrap();
        let result = module
            .getattr("summarize_agent_models")
            .unwrap()
            .call1((request,))
            .unwrap();
        let result = py_to_json_value(&result).unwrap();

        assert_eq!(result["models"].as_array().unwrap().len(), 2);
        assert_eq!(result["models"][0]["model"], json!("gpt-5"));
        assert_eq!(result["models"][1]["model"], json!("opus"));
        assert_eq!(result["latest"]["model"], json!("gpt-5"));
        assert_eq!(result["unknown_turn_count"], json!(1));
    });
}

#[test]
fn agent_stats_binding_round_trips_python_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = temp_agent_stats_root();
        let root = temp.path();
        let projects = root.join("projects");
        let artifact = projects.join("proj/artifacts/ace-run/20260710010000");
        fs::create_dir_all(&artifact).unwrap();
        fs::write(
            projects.join("proj/proj.sase"),
            "NAME: binding-spec\nSTATUS: Ready\n",
        )
        .unwrap();
        fs::write(
            artifact.join("agent_meta.json"),
            serde_json::to_vec(&json!({
                "name": "binding-agent",
                "run_started_at": "100",
                "llm_provider": "codex",
                "model": "gpt-5",
                "reasoning_effort": "high",
                "cl_name": "binding-spec"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact.join("done.json"),
            serde_json::to_vec(&json!({
                "outcome": "completed",
                "finished_at": 160.0,
                "step_output": {"meta_commits": [{
                    "sha": "abc",
                    "changespec_name": "binding-spec"
                }]}
            }))
            .unwrap(),
        )
        .unwrap();
        let index = root.join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let request_obj = json_value_to_py(
            py,
            &json!({
                "start_ts": 0,
                "end_ts": 200,
                "runtime_group_by": "agent",
                "bucket_seconds": 100,
                "top_n": 5,
                "project": "proj",
                "work_top_n": 50
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result =
            py_agent_stats_query_runs(py, index.to_str().unwrap(), request)
                .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["schema_version"], json!(8));
        assert_eq!(result["totals"]["runs"], json!(1));
        assert_eq!(result["totals"]["completed"], json!(1));
        assert_eq!(result["commits"]["committing_runs"], json!(1));
        assert_eq!(result["commits"]["committing_agents"], json!(1));
        assert_eq!(result["providers"][0]["effort"], json!("high"));
        assert_eq!(result["runtime_groups"][0]["total_seconds"], json!(60.0));
        assert_eq!(result["work"]["projects"][0]["project"], json!("proj"));
        // Legacy JSON key is still emitted for compatibility.
        assert_eq!(
            result["work"]["changespecs"][0]["name"], // legacy JSON key
            json!("binding-spec")
        );
        assert_eq!(result["runners"]["start_ts"], json!(100.0));
        assert_eq!(result["runners"]["end_ts"], json!(200.0));
        assert_eq!(result["runners"]["peak_runners"], json!(1));
        assert_eq!(result["runners"]["peak_seconds"], json!(60.0));
        assert_eq!(result["runners"]["average_runners"], json!(0.6));
        assert_eq!(result["runners"]["runner_seconds"], json!(60.0));
        assert_eq!(result["macros"]["runs_without_macros"], json!(1));
        assert_eq!(
            result["runners"]["distribution"][0]["seconds"],
            json!(40.0)
        );
        assert_eq!(
            result["runners"]["distribution"][1]["seconds"],
            json!(60.0)
        );
        assert_eq!(result["runners"]["trend"].as_array().unwrap().len(), 1);
    });
}

#[test]
fn agent_stats_query_macro_usage_binding_returns_parent_split() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = temp_agent_stats_root();
        let root = temp.path();
        let projects = root.join("projects");
        let artifact = projects.join("proj/artifacts/ace-run/20260710010000");
        fs::create_dir_all(&artifact).unwrap();
        fs::write(
            artifact.join("agent_meta.json"),
            serde_json::to_vec(&json!({
                "name": "child",
                "run_started_at": "100",
                "parent_agent_name": "root"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact.join("xprompts.json"),
            serde_json::to_vec(&json!([
                {"name": "research", "kind": "workflow", "tags": []}
            ]))
            .unwrap(),
        )
        .unwrap();
        let index = root.join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(py_agent_stats_macro_usage_wire_schema_version(py), 1);
        let result =
            py_agent_stats_query_macro_usage(py, index.to_str().unwrap())
                .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["schema_version"], json!(1));
        assert_eq!(result["rows"][0]["name"], json!("research"));
        assert_eq!(result["rows"][0]["has_parent"], json!(true));
        assert_eq!(result["rows"][0]["n"], json!(1));
    });
}

#[test]
fn agent_output_variable_history_binding_round_trips_python_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let projects = root.join("projects");
        let early = projects.join("proj/artifacts/ace-run/20260814120000");
        let late = projects.join("proj/artifacts/ace-run/20260814130000");
        fs::create_dir_all(&early).unwrap();
        fs::create_dir_all(&late).unwrap();
        fs::write(
            early.join("agent_meta.json"),
            serde_json::to_vec(&json!({
                "name": "worker",
                "output_variables": {"status": "ok"}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            late.join("agent_meta.json"),
            serde_json::to_vec(&json!({
                "name": "worker.child",
                "output_variables": {
                    "status": "ok",
                    "payload": {"z": 2, "a": 1}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let index = root.join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(py_agent_output_variable_history_wire_schema_version(), 1);
        let request_obj = json_value_to_py(
            py,
            &json!({
                "agents": ["worker.*"],
                "keys": ["status"],
                "value_json": ["ok"],
                "value_limit": 0
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_query_agent_output_variable_history(
            py,
            index.to_str().unwrap(),
            Some(request),
        )
        .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["schema_version"], json!(1));
        assert_eq!(result["keys_limit"]["total_count"], json!(1));
        assert_eq!(result["groups"][0]["key"], json!("status"));
        assert_eq!(
            result["groups"][0]["values"][0]["occurrence_count"],
            json!(2)
        );
        assert_eq!(
            result["groups"][0]["values"][0]["agents"],
            json!(["worker.child", "worker"])
        );
    });
}

#[test]
fn agent_alias_history_binding_round_trips_python_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let projects = root.join("projects");
        let artifact = projects.join("proj/artifacts/ace-run/20260816140000");
        fs::create_dir_all(&artifact).unwrap();
        fs::write(
            artifact.join("agent_meta.json"),
            serde_json::to_vec(&json!({
                "name": "alias-worker",
                "model": "claude-opus",
                "llm_provider": "claude",
                "reasoning_effort": "xhigh",
                "model_alias": "coder",
                "model_alias_trail": ["coder", "large"],
                "model_alias_origin": "directive",
                "bead_id": "sase-n8.2",
                "workspace_num": 15
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact.join("raw_xprompt.md"),
            "%model:@coder\n#gh:sase\nRefactor the workspace module\n",
        )
        .unwrap();
        let index = root.join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        assert_eq!(py_agent_alias_history_wire_schema_version(), 1);
        let request_obj = json_value_to_py(
            py,
            &json!({
                "aliases": ["large", "missing"],
                "limit_per_alias": 10
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result =
            py_query_agent_alias_history(py, index.to_str().unwrap(), request)
                .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["schema_version"], json!(1));
        assert_eq!(result["query"]["aliases"], json!(["large", "missing"]));
        assert_eq!(result["query"]["freshness"], json!("cached"));
        assert_eq!(result["groups"].as_array().unwrap().len(), 2);
        assert_eq!(result["groups"][0]["alias"], json!("large"));
        assert_eq!(result["groups"][0]["runs_limit"]["total_count"], json!(1));
        assert_eq!(result["groups"][0]["runs"][0]["alias_position"], json!(1));
        assert_eq!(
            result["groups"][0]["runs"][0]["model_alias_trail"],
            json!(["coder", "large"])
        );
        assert_eq!(
            result["groups"][0]["runs"][0]["prompt_snippet"],
            json!("Refactor the workspace module")
        );
        assert_eq!(result["groups"][1]["alias"], json!("missing"));
        assert!(result["groups"][1]["runs"].as_array().unwrap().is_empty());
    });
}

#[test]
fn clan_record_bindings_round_trip_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let records = temp.path().join("agent_clans");
        let records_str = records.to_string_lossy().into_owned();

        // Missing records load as None.
        assert!(py_load_agent_clan_record(py, &records_str, "missing")
            .unwrap()
            .is_none());

        // Record declared attributes through the binding.
        let update_obj = json_value_to_py(
            py,
            &json!({
                "clan": "binding-clan",
                "generation": "20260901000000",
                "tribe": {
                    "value": "chop",
                    "source": "declared",
                    "source_identity": "launch"
                },
                "summary": {
                    "value": "Binding summary",
                    "source": "script",
                    "source_identity": "script"
                }
            }),
        )
        .unwrap();
        let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
        let outcome =
            py_record_agent_clan_attributes(py, &records_str, update).unwrap();
        let outcome = py_to_json_value(outcome.bind(py)).unwrap();
        assert_eq!(outcome["changed"], json!(true));
        assert_eq!(
            outcome["record"]["generations"]["20260901000000"]["tribe"]
                ["value"],
            json!("chop")
        );

        // Load the stored record back through the binding.
        let loaded =
            py_load_agent_clan_record(py, &records_str, "binding-clan")
                .unwrap()
                .unwrap();
        let loaded = py_to_json_value(loaded.bind(py)).unwrap();
        assert_eq!(loaded["schema_version"], json!(1));
        assert_eq!(
            loaded["generations"]["20260901000000"]["summary"]["value"],
            json!("Binding summary")
        );

        // Capture from an artifact directory through the binding.
        let artifacts = temp.path().join("artifacts/20260901000000");
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join("agent_meta.json"),
            serde_json::to_vec(&json!({
                "name": "declarer",
                "agent_clan": "binding-clan",
                "agent_clan_generation": "20260901000000",
                "clan_tribe": "chop",
                "clan_summary": "Binding summary"
            }))
            .unwrap(),
        )
        .unwrap();
        let captured = py_capture_agent_clan_record_from_artifacts(
            py,
            &records_str,
            artifacts.to_str().unwrap(),
        )
        .unwrap()
        .unwrap();
        let captured = py_to_json_value(captured.bind(py)).unwrap();
        assert_eq!(captured["clan"], json!("binding-clan"));

        // Non-clan directories capture as None.
        let lonely = temp.path().join("lonely");
        fs::create_dir_all(&lonely).unwrap();
        fs::write(lonely.join("agent_meta.json"), b"{\"name\":\"x\"}").unwrap();
        assert!(py_capture_agent_clan_record_from_artifacts(
            py,
            &records_str,
            lonely.to_str().unwrap()
        )
        .unwrap()
        .is_none());

        // Launch defaults resolve with generation provenance.
        let defaults = py_resolve_agent_clan_launch_defaults(
            py,
            &records_str,
            "binding-clan",
            Some("20260901999999"),
        )
        .unwrap();
        let defaults = py_to_json_value(defaults.bind(py)).unwrap();
        assert_eq!(defaults["tribe"], json!("chop"));
        assert_eq!(defaults["tribe_generation"], json!("20260901000000"));
        assert_eq!(defaults["summary"], json!("Binding summary"));

        // Invalid clans surface as value errors.
        assert!(py_load_agent_clan_record(py, &records_str, "  ").is_err());
    });
}

#[test]
fn agent_activity_stats_binding_round_trips_python_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = temp_agent_stats_root();
        let root = temp.path();
        let projects = root.join("projects");
        let project = projects.join("proj");
        fs::create_dir_all(&project).unwrap();
        fs::write(
            project.join("skill_uses.jsonl"),
            concat!(
                "{\"timestamp\":\"100\",\"skill_name\":\"review\",",
                "\"agent_name\":\"binding-agent\"}\n"
            ),
        )
        .unwrap();
        fs::create_dir_all(root.join("interaction_requests/question/session"))
            .unwrap();
        fs::write(
            root.join("interaction_requests/question/session/request.json"),
            serde_json::to_vec(&json!({
                "request_id": "session",
                "producer": {
                    "agent_name": "binding-agent",
                    "artifacts_dir": root
                        .join("projects/proj/artifacts/ace-run/one")
                },
                "payload": {
                    "timestamp": 120.0,
                    "questions": [{"question": "Continue?"}]
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let index = root.join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();

        let request_obj = json_value_to_py(
            py,
            &json!({
                "start_ts": 0,
                "end_ts": 200,
                "top_n": 5,
                "project": "proj"
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_agent_stats_query_activity(
            py,
            index.to_str().unwrap(),
            root.to_str().unwrap(),
            request,
        )
        .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["schema_version"], json!(8));
        assert_eq!(result["skills"][0]["name"], json!("review"));
        assert_eq!(result["skills"][0]["distinct_agents"], json!(1));
        assert_eq!(result["questions"]["sessions"], json!(1));
        assert_eq!(result["questions"]["asking_agents"], json!(1));
        assert_eq!(result["questions"]["questions"], json!(1));
        assert_eq!(result["coverage_start_ts"], json!(120.0));

        let _ = fs::remove_dir_all(root);
    });
}

#[test]
fn reconcile_dismissed_members_binding_is_canonical_only() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        fs::create_dir_all(&root).unwrap();
        let index = temp.path().join("agent_artifact_index.sqlite");
        let index_str = index.to_string_lossy().into_owned();
        py_rebuild_agent_artifact_index(
            py,
            &index_str,
            root.to_string_lossy().as_ref(),
            None,
        )
        .unwrap();

        let new =
            py_reconcile_agent_artifact_index_dismissed_agent_session_members(
                py, &index_str, true,
            )
            .unwrap();
        let new_value = py_to_json_value(new.bind(py)).unwrap();
        assert_eq!(new_value["dry_run"], json!(true));
    });
}

#[test]
fn gate_turn_lookup_binding_serves_turn_records_without_legacy_name() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        let dir = projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20260812100000");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("agent_meta.json"),
            serde_json::to_string(&json!({
                "name": "proj--gate",
                "agent_session": "approvals",
                "agent_session_role": "gate",
                "gate_id": "gate-1",
                "gate_kind": "approval",
                "gate_state": "pending",
            }))
            .unwrap(),
        )
        .unwrap();
        let index = temp.path().join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let index_str = index.to_string_lossy().into_owned();

        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(
            module.getattr("find_gate_turn_by_gate_id").is_ok(),
            "missing find_gate_turn_by_gate_id"
        );
        assert!(
            module.getattr("find_gate_shell_by_gate_id").is_err(),
            "legacy find_gate_shell_by_gate_id must stay unregistered"
        );
        let found = py_find_gate_turn_by_gate_id(
            py,
            &index_str,
            Some("proj"),
            "gate-1",
        )
        .unwrap();
        let found_value = py_to_json_value(found.bind(py)).unwrap();
        assert_eq!(
            found_value["agent_meta"]["agent_session_turn"]["id"],
            json!("gate-1")
        );
    });
}

#[test]
fn lookup_agent_artifacts_binding_round_trips_name_hits() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        let dir = projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20261116120000");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("agent_meta.json"),
            serde_json::to_string(&json!({
                "name": "lane-a--plan",
                "workflow_name": "lane-a",
                "agent_session": "lane-a",
            }))
            .unwrap(),
        )
        .unwrap();
        let index = temp.path().join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(
            module.getattr("lookup_agent_artifacts").is_ok(),
            "missing lookup_agent_artifacts"
        );
        let query = pyo3::types::PyDict::new_bound(py);
        let target = pyo3::types::PyDict::new_bound(py);
        target.set_item("kind", "names").unwrap();
        target
            .set_item("names", vec!["lane-a--plan".to_string()])
            .unwrap();
        query.set_item("target", target).unwrap();
        query.set_item("include_hidden", true).unwrap();
        let found = py_lookup_agent_artifacts(
            py,
            index.to_str().unwrap(),
            projects.to_str().unwrap(),
            &query,
        )
        .unwrap();
        let value = py_to_json_value(found.bind(py)).unwrap();
        assert_eq!(value["availability"], json!("ready"));
        assert_eq!(value["complete"], json!(true));
        assert_eq!(value["source"], json!("index"));
        assert_eq!(value["hits"][0]["agent_name"], json!("lane-a--plan"));
        assert_eq!(value["hits"][0]["lookup_session"], json!("lane-a"));
        assert_eq!(value["hits"][0]["match_kind"], json!("exact"));
    });
}

#[test]
fn select_named_agent_binding_preserves_later_exact_spelling() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("select_named_agent").is_ok());

        let request = pyo3::types::PyDict::new_bound(py);
        request
            .set_item("names", vec!["foo", "alice.athena.foo"])
            .unwrap();
        request.set_item("only_done", false).unwrap();
        request.set_item("include_dead", false).unwrap();
        let hits = json_value_to_py(
            py,
            &json!([{
                "artifact_dir": "/projects/p/ace-run/2026111701",
                "project_name": "p",
                "workflow_dir_name": "ace-run",
                "timestamp": "2026111701",
                "agent_name": "alice.athena.foo",
                "workflow_name": "foo",
                "parent_timestamp": "root",
                "has_done_marker": false,
                "has_running_marker": true,
                "hidden": false
            }]),
        )
        .unwrap();
        request.set_item("hits", hits.bind(py)).unwrap();
        let observations = json_value_to_py(
            py,
            &json!([{
                "artifact_dir": "/projects/p/ace-run/2026111701",
                "process_alive": true
            }]),
        )
        .unwrap();
        request
            .set_item("observations", observations.bind(py))
            .unwrap();

        let selected = py_select_named_agent(py, &request).unwrap();
        let value = py_to_json_value(selected.bind(py)).unwrap();
        assert_eq!(
            value["selected"]["requested_name"],
            json!("alice.athena.foo")
        );
        assert_eq!(
            value["selected"]["hit"]["agent_name"],
            json!("alice.athena.foo")
        );
        assert!(value["selections"][0].is_null());
        assert_eq!(
            value["selections"][1]["requested_name"],
            json!("alice.athena.foo")
        );
    });
}

#[test]
fn select_agent_artifact_group_binding_round_trips_session_generation() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        crate::sase_core_rs(py, &module).unwrap();
        assert!(
            module.getattr("select_agent_artifact_group").is_ok(),
            "missing select_agent_artifact_group"
        );

        let names = vec!["lane".to_string()];
        let hit = |path: &str, timestamp: &str, parent: Option<&str>| {
            sase_core::agent_scan::AgentArtifactLookupHitWire {
                artifact_dir: path.to_string(),
                project_name: "proj".to_string(),
                workflow_dir_name: "ace-run".to_string(),
                timestamp: timestamp.to_string(),
                agent_name: Some(format!("lane--{timestamp}")),
                workflow_name: None,
                agent_session: None,
                lookup_session: Some("lane".to_string()),
                agent_clan: None,
                agent_clan_generation: None,
                parent_timestamp: parent.map(str::to_string),
                has_done_marker: false,
                has_running_marker: false,
                hidden: false,
                match_kind: Some("session".to_string()),
            }
        };
        let lookup = sase_core::agent_scan::AgentArtifactLookupResultWire {
            schema_version: 38,
            index_path: "/tmp/index.sqlite".to_string(),
            projects_root: "/tmp/projects".to_string(),
            query: sase_core::agent_scan::AgentArtifactLookupQueryWire {
                target: sase_core::agent_scan::AgentArtifactLookupTargetWire::Session {
                    names: names.clone(),
                },
                include_hidden: true,
                forced_unavailability: None,
            },
            availability:
                sase_core::agent_scan::AgentArtifactLookupAvailabilityWire::Ready,
            complete: true,
            source: "index".to_string(),
            diagnostic: None,
            hits: vec![
                hit("/tmp/root", "100", None),
                hit("/tmp/child", "101", Some("100")),
            ],
            truncated: false,
            dirs_visited: 0,
            records_decoded: 0,
        };
        let lookup_obj =
            json_value_to_py(py, &serde_json::to_value(lookup).unwrap())
                .unwrap();
        let selection_obj = json_value_to_py(
            py,
            &json!({"kind": "session_latest", "names": ["lane"]}),
        )
        .unwrap();
        let lookup_dict = lookup_obj
            .bind(py)
            .downcast::<pyo3::types::PyDict>()
            .unwrap();
        let selection_dict = selection_obj
            .bind(py)
            .downcast::<pyo3::types::PyDict>()
            .unwrap();
        let selected =
            py_select_agent_artifact_group(py, lookup_dict, selection_dict)
                .unwrap();
        let value = py_to_json_value(selected.bind(py)).unwrap();
        assert_eq!(value["complete"], json!(true));
        assert_eq!(value["selected_name"], json!("lane"));
        assert_eq!(value["root"]["artifact_dir"], json!("/tmp/root"));
        assert_eq!(value["members"].as_array().unwrap().len(), 2);
    });
}

#[test]
fn artifact_index_reader_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        let dir = projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20261013120000");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("agent_meta.json"),
            serde_json::to_string(&json!({"name": "reader-binding"})).unwrap(),
        )
        .unwrap();
        let index = temp.path().join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let index_str = index.to_string_lossy().into_owned();

        let version =
            py_read_agent_artifact_index_schema_version(py, &index_str);
        assert_eq!(
            version,
            Some(i64::from(sase_core::AGENT_ARTIFACT_INDEX_SCHEMA_VERSION))
        );

        let missing = temp.path().join("missing.sqlite");
        assert_eq!(
            py_read_agent_artifact_index_schema_version(
                py,
                missing.to_str().unwrap(),
            ),
            None
        );

        let columns = vec![
            "artifact_dir".to_string(),
            "project_name".to_string(),
            "hidden".to_string(),
        ];
        let projection = py_load_agent_artifact_index_projection(
            py, &index_str, columns, 250,
        )
        .unwrap();
        let projection = py_to_json_value(projection.bind(py)).unwrap();
        assert_eq!(
            projection["schema_version"],
            json!(sase_core::AGENT_ARTIFACT_INDEX_SCHEMA_VERSION)
        );
        assert_eq!(projection["rows"].as_array().unwrap().len(), 1);
        assert_eq!(projection["rows"][0][1], json!("proj"));

        let rejected = py_load_agent_artifact_index_projection(
            py,
            &index_str,
            vec!["nope".to_string()],
            250,
        )
        .unwrap_err();
        assert!(rejected.is_instance_of::<PyValueError>(py));

        py_upsert_agent_artifact_alias(
            py,
            &index_str,
            "/old/path",
            "/new/path",
        )
        .unwrap();
        let aliases = py_list_agent_artifact_aliases(py, &index_str).unwrap();
        let aliases = py_to_json_value(aliases.bind(py)).unwrap();
        assert_eq!(
            aliases,
            json!([{
                "alias_path": "/old/path",
                "artifact_dir": "/new/path"
            }])
        );
        py_delete_agent_artifact_alias(
            py,
            &index_str,
            "/old/path",
            "/new/path",
        )
        .unwrap();
        let aliases = py_list_agent_artifact_aliases(py, &index_str).unwrap();
        let aliases = py_to_json_value(aliases.bind(py)).unwrap();
        assert_eq!(aliases, json!([]));
    });
}

#[test]
fn compact_list_bindings_round_trip_scan_and_index() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let dir = root
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20261117120000");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("agent_meta.json"),
            r#"{"name":"compact","cl_name":"cl_compact","linked_repos":[{"name":"core"}]}"#,
        )
        .unwrap();
        fs::write(
            dir.join("done.json"),
            r#"{"outcome":"completed","name":"compact","traceback":"nope","step_output":{"_raw":"raw","meta_keep":"yes"}}"#,
        )
        .unwrap();

        assert_eq!(py_agent_artifact_list_wire_schema_version(), 1);

        let snapshot = py_scan_agent_artifacts_list(
            py,
            root.to_string_lossy().as_ref(),
            None,
        )
        .unwrap();
        let snapshot = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot["schema_version"], json!(1));
        let record = &snapshot["records"][0];
        assert_eq!(record["record_shape"], json!("compact"));
        assert_eq!(record["meta"]["name"], json!("compact"));
        assert!(record.get("agent_meta").is_none());
        assert!(record["meta"].get("linked_repos").is_none());
        assert!(record["done"].get("traceback").is_none());
        assert_eq!(
            record["done"]["step_output"]["meta"]["meta_keep"],
            json!("yes")
        );
        assert!(record["done"]["step_output"].get("_raw").is_none());

        let dirs = py_scan_agent_artifact_dirs_list(
            py,
            root.to_string_lossy().as_ref(),
            vec![dir.to_string_lossy().into_owned()],
            None,
        )
        .unwrap();
        let dirs = py_to_json_value(dirs.bind(py)).unwrap();
        assert_eq!(dirs["records"], snapshot["records"]);

        let one = py_scan_agent_artifact_dir_list(
            py,
            root.to_string_lossy().as_ref(),
            dir.to_string_lossy().as_ref(),
            None,
        )
        .unwrap()
        .unwrap();
        let one = py_to_json_value(one.bind(py)).unwrap();
        assert_eq!(one, snapshot["records"][0]);

        let missing = py_scan_agent_artifact_dir_list(
            py,
            root.to_string_lossy().as_ref(),
            root.join("missing").to_string_lossy().as_ref(),
            None,
        )
        .unwrap();
        assert!(missing.is_none());

        let index = temp.path().join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &root,
            sase_core::AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let query_obj = json_value_to_py(
            py,
            &json!({
                "include_active": true,
                "include_recent_completed": true,
                "include_full_history": true,
                "freshness": "cached",
                "record_shape": "list"
            }),
        )
        .unwrap();
        let query = query_obj.bind(py).downcast::<PyDict>().unwrap();
        let indexed = py_query_agent_artifact_index_list(
            py,
            index.to_string_lossy().as_ref(),
            root.to_string_lossy().as_ref(),
            Some(query),
            None,
        )
        .unwrap();
        let indexed = py_to_json_value(indexed.bind(py)).unwrap();
        assert_eq!(indexed["schema_version"], json!(1));
        assert_eq!(indexed["records"][0]["record_shape"], json!("compact"));
        assert_eq!(indexed["records"][0]["meta"]["name"], json!("compact"));
        assert!(indexed["records"][0].get("agent_meta").is_none());
        assert!(indexed["records"][0]["done"].get("traceback").is_none());
    });
}
