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
        assert_eq!(snapshot["schema_version"], json!(9));
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

fn temp_agent_stats_root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("sase-core-py-agent-stats-")
        .tempdir()
        .unwrap()
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
        assert_eq!(result["schema_version"], json!(6));
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
        assert_eq!(result["xprompts"]["runs_without_xprompts"], json!(1));
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
        assert_eq!(result["schema_version"], json!(6));
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
fn reconcile_dismissed_members_bindings_agree_across_spellings() {
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
        let legacy =
            py_reconcile_agent_artifact_index_dismissed_family_members(
                py, &index_str, true,
            )
            .unwrap();
        let new_value = py_to_json_value(new.bind(py)).unwrap();
        let legacy_value = py_to_json_value(legacy.bind(py)).unwrap();
        assert_eq!(new_value, legacy_value);
        assert_eq!(new_value["dry_run"], json!(true));
    });
}
