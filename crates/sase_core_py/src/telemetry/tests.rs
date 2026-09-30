use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;
use std::fs;
use tempfile::tempdir;

fn temp_telemetry_path() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::Builder::new()
        .prefix("sase-core-py-telemetry-")
        .tempdir()
        .unwrap();
    let path = temp.path().join("metrics.sqlite");
    (temp, path)
}

#[test]
fn telemetry_bindings_round_trip_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_telemetry_path();
        let batch_obj = json_value_to_py(
            py,
            &json!({
                "samples": [{
                    "ts": 100,
                    "metric": "sase_agent_runs_total",
                    "kind": "counter",
                    "labels": {"provider": "codex"},
                    "source": "runner-1",
                    "value": 3.0
                }],
                "now_ts": 110
            }),
        )
        .unwrap();
        let batch = batch_obj.bind(py).downcast::<PyDict>().unwrap();
        let recorded =
            py_telemetry_record_batch(py, path.to_str().unwrap(), batch, 1_000)
                .unwrap();
        let recorded = py_to_json_value(recorded.bind(py)).unwrap();
        assert_eq!(recorded["samples_recorded"], json!(1));

        let cleanup_obj = json_value_to_py(
            py,
            &json!({
                "label_matches": {"provider": ["codex"]},
                "dry_run": true
            }),
        )
        .unwrap();
        let cleanup_request =
            cleanup_obj.bind(py).downcast::<PyDict>().unwrap();
        let cleanup = py_telemetry_cleanup_matching_labels(
            py,
            path.to_str().unwrap(),
            cleanup_request,
            1_000,
        )
        .unwrap();
        let cleanup = py_to_json_value(cleanup.bind(py)).unwrap();
        assert_eq!(cleanup["dry_run"], json!(true));
        assert_eq!(cleanup["raw_rows"], json!(1));
        assert_eq!(cleanup["total_rows"], json!(1));

        let instant_obj = json_value_to_py(
            py,
            &json!({
                "metric": "sase_agent_runs_total",
                "group_by": [],
                "now_ts": 110
            }),
        )
        .unwrap();
        let instant_request =
            instant_obj.bind(py).downcast::<PyDict>().unwrap();
        let instant = py_telemetry_query_instant(
            py,
            path.to_str().unwrap(),
            instant_request,
            1_000,
        )
        .unwrap();
        let instant = py_to_json_value(instant.bind(py)).unwrap();
        assert_eq!(instant["values"][0]["value"], json!(3.0));

        let range_obj = json_value_to_py(
            py,
            &json!({
                "metric": "sase_agent_runs_total",
                "start_ts": 100,
                "end_ts": 159,
                "step_seconds": 60,
                "group_by": [],
                "aggregation": "sum"
            }),
        )
        .unwrap();
        let range_request = range_obj.bind(py).downcast::<PyDict>().unwrap();
        let range = py_telemetry_query_range(
            py,
            path.to_str().unwrap(),
            range_request,
            1_000,
        )
        .unwrap();
        let range = py_to_json_value(range.bind(py)).unwrap();
        assert_eq!(range["series"][0]["points"][0]["value"], json!(3.0));

        let prune_obj = json_value_to_py(
            py,
            &json!({
                "now_ts": 1_000,
                "retention": {
                    "raw_seconds": 100,
                    "rollup_5m_seconds": 10_000,
                    "rollup_1h_seconds": 100_000
                }
            }),
        )
        .unwrap();
        let prune_request = prune_obj.bind(py).downcast::<PyDict>().unwrap();
        let pruned = py_telemetry_prune(
            py,
            path.to_str().unwrap(),
            prune_request,
            1_000,
        )
        .unwrap();
        let pruned = py_to_json_value(pruned.bind(py)).unwrap();
        assert_eq!(pruned["raw_rows_folded"], json!(1));

        let stats = py_telemetry_store_stats(py, path.to_str().unwrap(), 1_000)
            .unwrap();
        let stats = py_to_json_value(stats.bind(py)).unwrap();
        assert_eq!(stats["raw_sample_count"], json!(0));
        assert_eq!(stats["rollup_5m_count"], json!(1));
        assert_eq!(stats["last_write_by_subsystem"]["agent"], json!(100));
    });
}

#[test]
fn tool_run_bindings_round_trip_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        assert_eq!(py_tool_run_wire_schema_version(), 1);
        let definition_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "name": "check",
                "argv": ["just", "check"],
                "description": "check",
                "stages": "run_silent",
                "inputs": ["Justfile"],
                "env": [],
                "args": "deny",
                "fingerprint": {"repos": [], "toolchain": {}}
            }),
        )
        .unwrap();
        let definition = definition_obj.bind(py).downcast::<PyDict>().unwrap();
        let normalized =
            py_tool_run_normalize_definition(py, definition).unwrap();
        let normalized = py_to_json_value(normalized.bind(py)).unwrap();
        let digest = normalized["digest"].as_str().unwrap().to_string();
        let begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "now_ts": 10,
                "commit_running": true
            }),
        )
        .unwrap();
        let begin_request = begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let started =
            py_tool_run_begin(py, path.to_str().unwrap(), begin_request, 1_000)
                .unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        assert_eq!(started["run"]["state"], json!("running"));
        let run_id = started["run"]["run_id"].as_str().unwrap().to_string();
        let observe_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "child_pid": 4242,
                "child_pgid": 4242,
                "child_process_start_identity": "boot-1:12345"
            }),
        )
        .unwrap();
        let observe_request =
            observe_obj.bind(py).downcast::<PyDict>().unwrap();
        let observed = py_tool_run_observe(
            py,
            path.to_str().unwrap(),
            observe_request,
            1_000,
        )
        .unwrap();
        let observed = py_to_json_value(observed.bind(py)).unwrap();
        assert_eq!(observed["replayed"], json!(false));
        assert_eq!(observed["run"]["child_pid"], json!(4242));
        assert_eq!(observed["run"]["child_pgid"], json!(4242));
        assert_eq!(
            observed["run"]["child_process_start_identity"],
            json!("boot-1:12345")
        );
        let replayed = py_tool_run_observe(
            py,
            path.to_str().unwrap(),
            observe_request,
            1_000,
        )
        .unwrap();
        let replayed = py_to_json_value(replayed.bind(py)).unwrap();
        assert_eq!(replayed["replayed"], json!(true));
        let finish_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "state": "succeeded",
                "exit_code": 0,
                "duration_ms": 12,
                "now_ts": 20
            }),
        )
        .unwrap();
        let finish_request = finish_obj.bind(py).downcast::<PyDict>().unwrap();
        let finished = py_tool_run_finish(
            py,
            path.to_str().unwrap(),
            finish_request,
            1_000,
        )
        .unwrap();
        let finished = py_to_json_value(finished.bind(py)).unwrap();
        assert_eq!(finished["run"]["state"], json!("succeeded"));
        let stats =
            py_tool_run_store_stats(py, path.to_str().unwrap(), 1_000).unwrap();
        let stats = py_to_json_value(stats.bind(py)).unwrap();
        assert_eq!(stats["run_count"], json!(1));
        let summary_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "tool_name": "check",
                "definition_digest": digest,
                "now_ts": 20
            }),
        )
        .unwrap();
        let summary_request =
            summary_obj.bind(py).downcast::<PyDict>().unwrap();
        let summary = py_tool_run_summary(
            py,
            path.to_str().unwrap(),
            summary_request,
            1_000,
        )
        .unwrap();
        let summary = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(summary["typical_duration_ms"], json!(12));
        let handoff_begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "now_ts": 30,
                "commit_running": false,
                "launch_mode": "handoff",
                "owner_kind": "proc",
                "owner_id": "proc-1",
                "owner_log_path": "logs/proc-1.log",
                "wrapper_pid": 111,
                "boot_id": "boot-1",
                "process_start_identity": "boot-1:111",
                "launch": {
                    "argv": ["just", "check"],
                    "tool_name": "check",
                    "extra_args": [],
                    "display_argv": ["just", "check"],
                    "definition": {
                        "schema_version": 1,
                        "name": "check",
                        "argv": ["just", "check"],
                        "description": "check",
                        "stages": "run_silent",
                        "inputs": ["Justfile"],
                        "env": [],
                        "args": "deny",
                        "fingerprint": {"repos": [], "toolchain": {}}
                    },
                    "digest": digest,
                    "adhoc": false
                }
            }),
        )
        .unwrap();
        let handoff_begin_request =
            handoff_begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let handoff_started = py_tool_run_begin(
            py,
            path.to_str().unwrap(),
            handoff_begin_request,
            1_000,
        )
        .unwrap();
        let handoff_started =
            py_to_json_value(handoff_started.bind(py)).unwrap();
        assert_eq!(handoff_started["run"]["state"], json!("created"));
        assert_eq!(handoff_started["run"]["launch_mode"], json!("handoff"));
        let handoff_id = handoff_started["run"]["run_id"]
            .as_str()
            .unwrap()
            .to_string();
        let claim_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": handoff_id,
                "owner_kind": "proc",
                "owner_id": "proc-1",
                "wrapper_pid": 4242,
                "boot_id": "boot-1",
                "process_start_identity": "boot-1:4242",
                "owner_log_path": "logs/proc-1.log",
                "now_ts": 31
            }),
        )
        .unwrap();
        let claim_request = claim_obj.bind(py).downcast::<PyDict>().unwrap();
        let claimed =
            py_tool_run_claim(py, path.to_str().unwrap(), claim_request, 1_000)
                .unwrap();
        let claimed = py_to_json_value(claimed.bind(py)).unwrap();
        assert_eq!(claimed["outcome"], json!("claimed"));
        assert_eq!(claimed["replayed"], json!(false));
        assert_eq!(claimed["launch"]["argv"], json!(["just", "check"]));
        let replayed_claim =
            py_tool_run_claim(py, path.to_str().unwrap(), claim_request, 1_000)
                .unwrap();
        let replayed_claim = py_to_json_value(replayed_claim.bind(py)).unwrap();
        assert_eq!(replayed_claim["outcome"], json!("claimed"));
        assert_eq!(replayed_claim["replayed"], json!(true));
        let handoff_finish_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": handoff_id,
                "state": "succeeded",
                "exit_code": 0,
                "duration_ms": 5,
                "terminal_cause": "exited",
                "now_ts": 32
            }),
        )
        .unwrap();
        let handoff_finish_request =
            handoff_finish_obj.bind(py).downcast::<PyDict>().unwrap();
        let handoff_finished = py_tool_run_finish(
            py,
            path.to_str().unwrap(),
            handoff_finish_request,
            1_000,
        )
        .unwrap();
        let handoff_finished =
            py_to_json_value(handoff_finished.bind(py)).unwrap();
        assert_eq!(handoff_finished["run"]["state"], json!("succeeded"));
        assert_eq!(handoff_finished["run"]["terminal_cause"], json!("exited"));
        let stop_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": handoff_id,
                "requested_by": "agent-1",
                "reason": "late stop",
                "now_ts": 33
            }),
        )
        .unwrap();
        let stop_request = stop_obj.bind(py).downcast::<PyDict>().unwrap();
        let stopped = py_tool_run_request_stop(
            py,
            path.to_str().unwrap(),
            stop_request,
            1_000,
        )
        .unwrap();
        let stopped = py_to_json_value(stopped.bind(py)).unwrap();
        assert_eq!(stopped["outcome"], json!("already_settled"));
        let reconcile_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "facts": [{
                    "run_id": handoff_id,
                    "wrapper_pid": 4242,
                    "boot_id": "boot-1",
                    "process_start_identity": "boot-1:4242",
                    "observation": "dead",
                    "owner": {
                        "kind": "proc",
                        "id": "proc-1",
                        "state": "terminal",
                        "exit_code": 0,
                        "termination_reason": "success"
                    }
                }],
                "now_ts": 34
            }),
        )
        .unwrap();
        let reconcile_request =
            reconcile_obj.bind(py).downcast::<PyDict>().unwrap();
        let reconciled = py_tool_run_reconcile(
            py,
            path.to_str().unwrap(),
            reconcile_request,
            1_000,
        )
        .unwrap();
        let reconciled = py_to_json_value(reconciled.bind(py)).unwrap();
        assert_eq!(reconciled["persisted"], json!(true));
        let budget_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "ceiling_seconds": 600}),
        )
        .unwrap();
        let budget_request = budget_obj.bind(py).downcast::<PyDict>().unwrap();
        let budgeted =
            py_tool_run_sync_wait_budget(py, budget_request).unwrap();
        let budgeted = py_to_json_value(budgeted.bind(py)).unwrap();
        assert_eq!(budgeted["budget_seconds"], json!(510));
        assert_eq!(budgeted["source"], json!("hard"));
        let bad_budget_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "ceiling_seconds": 0}),
        )
        .unwrap();
        let bad_budget_request =
            bad_budget_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_sync_wait_budget(py, bad_budget_request).is_err());
        let starter_begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "now_ts": 40,
                "commit_running": false,
                "launch_mode": "handoff",
                "owner_kind": "proc",
                "owner_id": "proc-1",
                "wrapper_pid": 111,
                "boot_id": "boot-1",
                "process_start_identity": "boot-1:111",
                "launch": {
                    "argv": ["just", "check"],
                    "tool_name": "check",
                    "extra_args": [],
                    "display_argv": ["just", "check"],
                    "definition": {
                        "schema_version": 1,
                        "name": "check",
                        "argv": ["just", "check"],
                        "description": "check",
                        "stages": "run_silent",
                        "inputs": ["Justfile"],
                        "env": [],
                        "args": "deny",
                        "fingerprint": {"repos": [], "toolchain": {}}
                    },
                    "digest": digest,
                    "adhoc": false,
                    "continuation_mode": "always"
                },
                "starter": {
                    "agent": "agent-1",
                    "pid": 4242,
                    "boot_id": "boot-1",
                    "process_start_identity": "boot-1:4242"
                }
            }),
        )
        .unwrap();
        let starter_begin_request =
            starter_begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let starter_started = py_tool_run_begin(
            py,
            path.to_str().unwrap(),
            starter_begin_request,
            1_000,
        )
        .unwrap();
        let starter_started =
            py_to_json_value(starter_started.bind(py)).unwrap();
        assert_eq!(
            starter_started["run"]["starter"]["agent"],
            json!("agent-1")
        );
        let starter_id = starter_started["run"]["run_id"]
            .as_str()
            .unwrap()
            .to_string();
        let join_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": starter_id,
                "joiner_kind": "monitor",
                "joiner_id": "mon-1",
                "agent": "agent-1",
                "requested_by": "agent-1",
                "now_ts": 41
            }),
        )
        .unwrap();
        let join_request = join_obj.bind(py).downcast::<PyDict>().unwrap();
        let joined =
            py_tool_run_join(py, path.to_str().unwrap(), join_request, 1_000)
                .unwrap();
        let joined = py_to_json_value(joined.bind(py)).unwrap();
        assert_eq!(joined["outcome"], json!("joined"));
        assert_eq!(joined["run"]["starter"]["agent"], json!("agent-1"));
        assert_eq!(joined["run"]["join"]["kind"], json!("monitor"));
        let release_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": starter_id,
                "joiner_kind": "monitor",
                "joiner_id": "mon-1",
                "now_ts": 42
            }),
        )
        .unwrap();
        let release_request =
            release_obj.bind(py).downcast::<PyDict>().unwrap();
        let released = py_tool_run_release_join(
            py,
            path.to_str().unwrap(),
            release_request,
            1_000,
        )
        .unwrap();
        let released = py_to_json_value(released.bind(py)).unwrap();
        assert_eq!(released["outcome"], json!("released"));
        assert!(released["run"].get("join").is_none());
    });
}

#[test]
fn tool_run_duration_bindings_round_trip_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let call_fit = |request: serde_json::Value| {
            let request_obj = json_value_to_py(py, &request).unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let result = py_tool_run_duration_fit(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        let refused = call_fit(json!({
            "schema_version": 1,
            "duration_class": "long",
            "ceiling_seconds": 600,
        }));
        assert_eq!(refused["duration_class"], json!("long"));
        assert_eq!(refused["floor_seconds"], json!(600));
        assert_eq!(refused["ceiling_seconds"], json!(600));
        assert_eq!(refused["fits_inline"], json!(false));

        let fits = call_fit(json!({
            "schema_version": 1,
            "duration_class": "long",
            "ceiling_seconds": 1800,
        }));
        assert_eq!(fits["fits_inline"], json!(true));

        let default = call_fit(json!({"schema_version": 1}));
        assert_eq!(default["duration_class"], json!("short"));
        assert_eq!(default["floor_seconds"], json!(0));
        assert_eq!(default["fits_inline"], json!(true));

        let unbounded = call_fit(json!({
            "schema_version": 1,
            "duration_class": "unbounded",
            "ceiling_seconds": 14400,
        }));
        assert_eq!(unbounded["floor_seconds"], serde_json::Value::Null);
        assert_eq!(unbounded["fits_inline"], json!(false));

        let call_calibration = |request: serde_json::Value| {
            let request_obj = json_value_to_py(py, &request).unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let result = py_tool_run_duration_calibration(py, request).unwrap();
            py_to_json_value(result.bind(py)).unwrap()
        };
        let overstated = call_calibration(json!({
            "schema_version": 1,
            "duration_class": "long",
            "typical_duration_ms": 120_000,
            "typical_sample_count": 12,
        }));
        assert_eq!(overstated["duration_class"], json!("long"));
        assert_eq!(
            overstated["calibration"]["suggested_class"],
            json!("short")
        );
        assert!(overstated["calibration"]["summary"]
            .as_str()
            .unwrap()
            .contains("suggest short"));

        let silent = call_calibration(json!({
            "schema_version": 1,
            "duration_class": "short",
            "typical_duration_ms": 180_000,
            "typical_sample_count": 15,
        }));
        assert_eq!(silent["calibration"], serde_json::Value::Null);

        let thin = call_calibration(json!({
            "schema_version": 1,
            "duration_class": "long",
            "typical_duration_ms": 120_000,
            "typical_sample_count": 9,
        }));
        assert_eq!(thin["calibration"], serde_json::Value::Null);
    });
}

#[test]
fn perf_logs_query_binding_round_trips_python_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let startup = temp.path().join("tui_startup.jsonl");
        fs::write(
            &startup,
            serde_json::to_string(&json!({
                "timestamp": "1970-01-01T00:02:30Z",
                "event": "tui_startup",
                "initial_tab": "agents",
                "visible_ready_seconds": 1.25,
                "all_surfaces_ready_seconds": 1.75
            }))
            .unwrap()
                + "\n",
        )
        .unwrap();

        let request_obj = json_value_to_py(
            py,
            &json!({
                "start_ts": 100,
                "end_ts": 200,
                "sources": [{
                    "id": "startup",
                    "path": startup.to_str().unwrap()
                }]
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_perf_logs_query(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(result["schema_version"], json!(1));
        assert_eq!(result["startup"]["sessions"], json!(1));
        assert_eq!(
            result["startup"]["visible_ready_series"][0]
                ["visible_ready_seconds"],
            json!(1.25)
        );
        assert_eq!(result["coverage"][0]["records_in_window"], json!(1));
    });
}

#[test]
fn tool_run_triage_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        // Begin + observe (with fingerprint_before) + finish a named run.
        let begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "now_ts": 10,
                "commit_running": true
            }),
        )
        .unwrap();
        let begin_request = begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let started =
            py_tool_run_begin(py, path.to_str().unwrap(), begin_request, 1_000)
                .unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        let run_id = started["run"]["run_id"].as_str().unwrap().to_string();
        let observe_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "child_pid": 1,
                "fingerprint_before": {
                    "schema_version": 1,
                    "repos": [{
                        "identity": "sase",
                        "head": "abc",
                        "dirty_paths": [],
                    }],
                    "inputs": [],
                    "env": {},
                    "toolchain": {},
                    "completeness": {"complete": true, "missing": []},
                }
            }),
        )
        .unwrap();
        let observe_request =
            observe_obj.bind(py).downcast::<PyDict>().unwrap();
        let observed = py_tool_run_observe(
            py,
            path.to_str().unwrap(),
            observe_request,
            1_000,
        )
        .unwrap();
        let observed = py_to_json_value(observed.bind(py)).unwrap();
        assert!(observed["run"]["fingerprint_before"].is_object());
        let finish_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "state": "failed",
                "exit_code": 1,
                "duration_ms": 5,
                "now_ts": 20
            }),
        )
        .unwrap();
        let finish_request = finish_obj.bind(py).downcast::<PyDict>().unwrap();
        let finished = py_tool_run_finish(
            py,
            path.to_str().unwrap(),
            finish_request,
            1_000,
        )
        .unwrap();
        let finished = py_to_json_value(finished.bind(py)).unwrap();
        assert_eq!(finished["run"]["state"], json!("failed"));
        // Extract a symvision output.
        let extract_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "stage_key": "lint (symvision)",
                "output": "Unused public functions/classes:\n  my_helper in src/helpers.py\n",
            }),
        )
        .unwrap();
        let extract_request =
            extract_obj.bind(py).downcast::<PyDict>().unwrap();
        let extracted =
            py_tool_run_triage_extract(py, extract_request).unwrap();
        let extracted = py_to_json_value(extracted.bind(py)).unwrap();
        assert_eq!(extracted["status"], json!("parsed"));
        assert_eq!(extracted["items"].as_array().unwrap().len(), 1);
        let mut item = extracted["items"][0].clone();
        item["stage_key"] = json!("lint (symvision)");
        // Record its items.
        let record_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "stages": [{
                    "schema_version": 1,
                    "stage_key": "lint (symvision)",
                    "stage_id": "stage-1",
                    "extraction_status": "parsed",
                    "output_path": "logs/stage.log",
                    "items": [item],
                }],
                "run_facts": {
                    "schema_version": 1,
                    "continuation_mode": "never",
                    "triaged_ts": 21,
                },
                "now_ts": 22
            }),
        )
        .unwrap();
        let record_request = record_obj.bind(py).downcast::<PyDict>().unwrap();
        let recorded = py_tool_run_triage_record(
            py,
            path.to_str().unwrap(),
            record_request,
            1_000,
        )
        .unwrap();
        let recorded = py_to_json_value(recorded.bind(py)).unwrap();
        assert_eq!(recorded["items_inserted"], json!(1));
        // Show them back.
        let show_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "run_id": run_id}),
        )
        .unwrap();
        let show_request = show_obj.bind(py).downcast::<PyDict>().unwrap();
        let shown = py_tool_run_triage_show(
            py,
            path.to_str().unwrap(),
            show_request,
            1_000,
        )
        .unwrap();
        let shown = py_to_json_value(shown.bind(py)).unwrap();
        assert_eq!(shown["run_found"], json!(true));
        assert_eq!(shown["triaged"], json!(true));
        assert_eq!(shown["items"].as_array().unwrap().len(), 1);
        // Replay reports items_existing.
        let replayed = py_tool_run_triage_record(
            py,
            path.to_str().unwrap(),
            record_request,
            1_000,
        )
        .unwrap();
        let replayed = py_to_json_value(replayed.bind(py)).unwrap();
        assert_eq!(replayed["items_inserted"], json!(0));
        assert_eq!(replayed["items_existing"], json!(1));
    });
}

#[test]
fn tool_run_triage_classification_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        let begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "workspace": "ws-a",
                "now_ts": 10,
                "commit_running": true
            }),
        )
        .unwrap();
        let begin_request = begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let started =
            py_tool_run_begin(py, path.to_str().unwrap(), begin_request, 1_000)
                .unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        let run_id = started["run"]["run_id"].as_str().unwrap().to_string();
        let observe_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "child_pid": 1,
                "fingerprint_before": {
                    "schema_version": 1,
                    "repos": [{
                        "identity": "sase",
                        "head": "head-3",
                        "dirty_paths": [],
                    }],
                    "inputs": [],
                    "env": {},
                    "toolchain": {},
                    "completeness": {"complete": true, "missing": []},
                }
            }),
        )
        .unwrap();
        let observe_request =
            observe_obj.bind(py).downcast::<PyDict>().unwrap();
        py_tool_run_observe(py, path.to_str().unwrap(), observe_request, 1_000)
            .unwrap();
        let finish_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "state": "failed",
                "exit_code": 1,
                "duration_ms": 5,
                "terminal_cause": "exited",
                "now_ts": 20
            }),
        )
        .unwrap();
        let finish_request = finish_obj.bind(py).downcast::<PyDict>().unwrap();
        py_tool_run_finish(py, path.to_str().unwrap(), finish_request, 1_000)
            .unwrap();
        // Pure classify: untouched with no evidence -> UNKNOWN.
        let classify_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "subject_run": {
                    "run_id": "subject-1",
                    "project": "sase",
                    "tool": "check",
                    "extra_args_digest": "args-1",
                    "workspace": "ws-a",
                    "base_head": "head-3",
                    "dirty_paths": [],
                    "complete_fingerprint": true,
                    "fingerprint_digest": "fp-1",
                    "ad_hoc": false
                },
                "subjects": [{
                    "stage_key": "lint (mypy)",
                    "extractor": "mypy",
                    "extractor_version": 1,
                    "signature": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "locator_paths": ["src/foo.py"]
                }],
                "evidence_runs": [],
                "ancestry": ["head-3"],
                "knobs": {"min_witnesses": 1, "touched_requires_clean_witness": false},
                "now_ts": 30
            }),
        )
        .unwrap();
        let classify_request =
            classify_obj.bind(py).downcast::<PyDict>().unwrap();
        let classified =
            py_tool_run_triage_classify(py, classify_request).unwrap();
        let classified = py_to_json_value(classified.bind(py)).unwrap();
        assert_eq!(classified["labels"][0]["class"], json!("unknown"));
        // Pure verdict: verification with UNKNOWN -> undetermined.
        let verdict_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "exit_code": 1,
                "terminal_cause": "exited",
                "has_completed_stage": true,
                "has_failed_stage": true,
                "all_stages_complete": true,
                "recipe_finished": true,
                "is_stageful_tool": true,
                "triaged": true,
                "items": [{"class": "unknown"}]
            }),
        )
        .unwrap();
        let verdict_request =
            verdict_obj.bind(py).downcast::<PyDict>().unwrap();
        let verdict = py_tool_run_triage_verdict(py, verdict_request).unwrap();
        let verdict = py_to_json_value(verdict.bind(py)).unwrap();
        assert_eq!(verdict["verdict"], json!("undetermined"));
        // Store stage: extract + classify + persist one stage.
        let stage_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "stage": {
                    "stage_key": "lint (mypy)",
                    "stage_id": "stage-1",
                    "output": "src/foo.py:10:5: error: Bad thing  [attr-defined]\n",
                },
                "ancestry": ["head-3"],
                "now_ts": 31
            }),
        )
        .unwrap();
        let stage_request = stage_obj.bind(py).downcast::<PyDict>().unwrap();
        let staged = py_tool_run_triage_stage(
            py,
            path.to_str().unwrap(),
            stage_request,
            1_000,
        )
        .unwrap();
        let staged = py_to_json_value(staged.bind(py)).unwrap();
        assert_eq!(staged["items"].as_array().unwrap().len(), 1);
        // Store settle: returns kind/verdict and triaged items.
        let settle_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "stages": [],
                "ancestry": ["head-3"],
                "recipe_finished_ts": 40,
                "now_ts": 41
            }),
        )
        .unwrap();
        let settle_request = settle_obj.bind(py).downcast::<PyDict>().unwrap();
        let settled = py_tool_run_triage_settle(
            py,
            path.to_str().unwrap(),
            settle_request,
            1_000,
        )
        .unwrap();
        let settled = py_to_json_value(settled.bind(py)).unwrap();
        assert_eq!(settled["triaged"], json!(true));
        assert!(settled["verdict"].is_string());
        // Show carries kind and verdict.
        let show_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "run_id": run_id}),
        )
        .unwrap();
        let show_request = show_obj.bind(py).downcast::<PyDict>().unwrap();
        let shown = py_tool_run_triage_show(
            py,
            path.to_str().unwrap(),
            show_request,
            1_000,
        )
        .unwrap();
        let shown = py_to_json_value(shown.bind(py)).unwrap();
        assert!(shown["failure_kind"].is_string());
        assert!(shown["verdict"].is_string());
        // Failures aggregation lists the group.
        let failures_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "tool": "check",
                "days": 7,
                "limit": 50,
                "now_ts": 50
            }),
        )
        .unwrap();
        let failures_request =
            failures_obj.bind(py).downcast::<PyDict>().unwrap();
        let failures = py_tool_run_failures(
            py,
            path.to_str().unwrap(),
            failures_request,
            1_000,
        )
        .unwrap();
        let failures = py_to_json_value(failures.bind(py)).unwrap();
        assert_eq!(failures["groups"].as_array().unwrap().len(), 1);
    });
}

#[test]
fn tool_run_receipt_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        let fp = json!({
            "schema_version": 1,
            "repos": [{
                "identity": "sase",
                "head": "head-1",
                "index_tree": "tree-1",
                "dirty_paths": []
            }],
            "inputs": [],
            "env": {},
            "toolchain": {},
            "completeness": {"complete": true, "missing": []},
            "diagnostics": []
        });
        let begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "now_ts": 100,
                "commit_running": true
            }),
        )
        .unwrap();
        let begin_request = begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let started =
            py_tool_run_begin(py, path.to_str().unwrap(), begin_request, 1_000)
                .unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        let run_id = started["run"]["run_id"].as_str().unwrap().to_string();
        let definition_digest = started["run"]["definition_digest"]
            .as_str()
            .unwrap()
            .to_string();
        let extra_args_digest = started["run"]["extra_args_digest"]
            .as_str()
            .unwrap()
            .to_string();
        let observe_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "child_pid": 1,
                "child_pgid": 1,
                "fingerprint_before": fp
            }),
        )
        .unwrap();
        let observe_request =
            observe_obj.bind(py).downcast::<PyDict>().unwrap();
        py_tool_run_observe(py, path.to_str().unwrap(), observe_request, 1_000)
            .unwrap();
        let finish_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "state": "succeeded",
                "exit_code": 0,
                "duration_ms": 5,
                "fingerprint_after": fp,
                "mutated_input": false,
                "terminal_cause": "exited",
                "now_ts": 110
            }),
        )
        .unwrap();
        let finish_request = finish_obj.bind(py).downcast::<PyDict>().unwrap();
        py_tool_run_finish(py, path.to_str().unwrap(), finish_request, 1_000)
            .unwrap();
        let settle_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "recipe_finished_ts": 111,
                "now_ts": 112
            }),
        )
        .unwrap();
        let settle_request = settle_obj.bind(py).downcast::<PyDict>().unwrap();
        py_tool_run_triage_settle(
            py,
            path.to_str().unwrap(),
            settle_request,
            1_000,
        )
        .unwrap();
        let receipt_settle_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "policy": {
                    "schema_version": 1,
                    "accept": ["pass"],
                    "ttl": "2h"
                },
                "bypassed": false,
                "now_ts": 120
            }),
        )
        .unwrap();
        let receipt_settle_request =
            receipt_settle_obj.bind(py).downcast::<PyDict>().unwrap();
        let settled = py_tool_run_receipt_settle(
            py,
            path.to_str().unwrap(),
            receipt_settle_request,
            1_000,
        )
        .unwrap();
        let settled = py_to_json_value(settled.bind(py)).unwrap();
        assert_eq!(settled["schema_version"], json!(1));
        assert_eq!(settled["minted"], json!(true));
        let lookup_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "tool_name": "check",
                "definition_digest": definition_digest,
                "extra_args_digest": extra_args_digest,
                "fingerprint": fp,
                "accept": ["pass"],
                "now_ts": 130
            }),
        )
        .unwrap();
        let lookup_request = lookup_obj.bind(py).downcast::<PyDict>().unwrap();
        let looked = py_tool_run_receipt_lookup(
            py,
            path.to_str().unwrap(),
            lookup_request,
            1_000,
        )
        .unwrap();
        let looked = py_to_json_value(looked.bind(py)).unwrap();
        assert_eq!(looked["schema_version"], json!(1));
        assert_eq!(looked["outcome"], json!("covered"));
    });
}

#[test]
fn tool_run_receipts_report_round_trip_and_rejects_bad_schema() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        let missing = temp.path().join("missing.sqlite");
        // Empty store: zero report with the missing-store diagnostic.
        let empty_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "days": 7,
                "now_ts": 1_700_000_000,
                "project_root": temp.path().to_string_lossy(),
            }),
        )
        .unwrap();
        let empty_request = empty_obj.bind(py).downcast::<PyDict>().unwrap();
        let empty = py_tool_run_receipts_report(
            py,
            missing.to_str().unwrap(),
            empty_request,
            1_000,
        )
        .unwrap();
        let empty = py_to_json_value(empty.bind(py)).unwrap();
        assert_eq!(empty["schema_version"], json!(1));
        assert_eq!(empty["receipts"]["count"], json!(0));
        assert_eq!(empty["opportunities"]["group_count"], json!(0));
        assert_eq!(empty["runs_scanned"], json!(0));
        assert!(empty["note"].as_str().unwrap().contains("measurement only"));
        assert!(empty["diagnostics"][0]
            .as_str()
            .unwrap()
            .contains("does not exist"));

        // Negative days must fail; invalid schema versions must fail.
        let bad_days_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "days": -1,
                "now_ts": 10,
                "project_root": temp.path().to_string_lossy(),
            }),
        )
        .unwrap();
        let bad_days = bad_days_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_receipts_report(
            py,
            path.to_str().unwrap(),
            bad_days,
            1_000
        )
        .is_err());
        let bad_schema_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 999,
                "project": "sase",
                "days": 7,
                "now_ts": 10,
                "project_root": temp.path().to_string_lossy(),
            }),
        )
        .unwrap();
        let bad_schema = bad_schema_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_receipts_report(
            py,
            path.to_str().unwrap(),
            bad_schema,
            1_000
        )
        .is_err());
    });
}

#[test]
fn tool_run_glance_projection_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        let missing = temp.path().join("missing.sqlite");
        // A missing store reports itself on every projection.
        for (name, request) in [
            (
                "glance",
                json!({"schema_version": 1, "now_ts": 1_700_000_000}),
            ),
            ("briefs", json!({"schema_version": 1, "limit": 10})),
            (
                "nodes",
                json!({
                    "schema_version": 1,
                    "nodes": [{"key": "k", "agents": ["a"]}],
                    "per_node_limit": 5
                }),
            ),
            ("detail", json!({"schema_version": 1, "run_id": "missing"})),
        ] {
            let request_obj = json_value_to_py(py, &request).unwrap();
            let request_dict =
                request_obj.bind(py).downcast::<PyDict>().unwrap();
            let result = match name {
                "glance" => py_tool_run_live_glance(
                    py,
                    missing.to_str().unwrap(),
                    request_dict,
                    1_000,
                ),
                "briefs" => py_tool_run_briefs(
                    py,
                    missing.to_str().unwrap(),
                    request_dict,
                    1_000,
                ),
                "nodes" => py_tool_run_node_summaries(
                    py,
                    missing.to_str().unwrap(),
                    request_dict,
                    1_000,
                ),
                _ => py_tool_run_detail(
                    py,
                    missing.to_str().unwrap(),
                    request_dict,
                    1_000,
                ),
            }
            .unwrap();
            let result = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(result["store_exists"], json!(false));
        }
        // One live run round-trips through all three projections.
        let begin_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "tool_name": "check",
                "definition": {
                    "schema_version": 1,
                    "name": "check",
                    "argv": ["just", "check"],
                    "description": "check",
                    "stages": "run_silent",
                    "inputs": ["Justfile"],
                    "env": [],
                    "args": "deny",
                    "fingerprint": {"repos": [], "toolchain": {}}
                },
                "display_argv": ["just", "check"],
                "project": "sase",
                "agent": "agent-1",
                "now_ts": 1_700_000_000,
                "commit_running": true
            }),
        )
        .unwrap();
        let begin_request = begin_obj.bind(py).downcast::<PyDict>().unwrap();
        let started =
            py_tool_run_begin(py, path.to_str().unwrap(), begin_request, 1_000)
                .unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        let run_id = started["run"]["run_id"].as_str().unwrap().to_string();

        let glance_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "now_ts": 1_700_000_100}),
        )
        .unwrap();
        let glance_request = glance_obj.bind(py).downcast::<PyDict>().unwrap();
        let glanced = py_tool_run_live_glance(
            py,
            path.to_str().unwrap(),
            glance_request,
            1_000,
        )
        .unwrap();
        let glanced = py_to_json_value(glanced.bind(py)).unwrap();
        assert_eq!(glanced["store_exists"], json!(true));
        assert_eq!(glanced["silent_after_s"], json!(60));
        assert_eq!(glanced["truncated"], json!(false));
        assert_eq!(glanced["runs"][0]["run_id"], json!(run_id));
        assert_eq!(glanced["runs"][0]["label"], json!("check"));
        assert_eq!(glanced["runs"][0]["state"], json!("running"));

        let briefs_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "agents": ["agent-1"],
                "limit": 10
            }),
        )
        .unwrap();
        let briefs_request = briefs_obj.bind(py).downcast::<PyDict>().unwrap();
        let briefed = py_tool_run_briefs(
            py,
            path.to_str().unwrap(),
            briefs_request,
            1_000,
        )
        .unwrap();
        let briefed = py_to_json_value(briefed.bind(py)).unwrap();
        assert_eq!(briefed["store_exists"], json!(true));
        assert_eq!(briefed["runs"][0]["run_id"], json!(run_id));
        assert_eq!(briefed["runs"][0]["verdict"]["bucket"], json!("running"));

        let nodes_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "nodes": [{"key": "agent-1", "agents": ["agent-1"]}],
                "per_node_limit": 5
            }),
        )
        .unwrap();
        let nodes_request = nodes_obj.bind(py).downcast::<PyDict>().unwrap();
        let nodes = py_tool_run_node_summaries(
            py,
            path.to_str().unwrap(),
            nodes_request,
            1_000,
        )
        .unwrap();
        let nodes = py_to_json_value(nodes.bind(py)).unwrap();
        assert_eq!(nodes["store_exists"], json!(true));
        assert_eq!(nodes["nodes"][0]["key"], json!("agent-1"));
        assert_eq!(nodes["nodes"][0]["total_runs"], json!(1));
        assert_eq!(nodes["nodes"][0]["live"][0]["run_id"], json!(run_id));
        assert_eq!(
            nodes["nodes"][0]["latest_by_tool"][0]["run_id"],
            json!(run_id)
        );

        let detail_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "run_id": run_id}),
        )
        .unwrap();
        let detail_request = detail_obj.bind(py).downcast::<PyDict>().unwrap();
        let detailed = py_tool_run_detail(
            py,
            path.to_str().unwrap(),
            detail_request,
            1_000,
        )
        .unwrap();
        let detailed = py_to_json_value(detailed.bind(py)).unwrap();
        assert_eq!(detailed["store_exists"], json!(true));
        assert_eq!(detailed["found"], json!(true));
        assert_eq!(detailed["brief"]["run_id"], json!(run_id));
        assert_eq!(detailed["display_argv"], json!(["just", "check"]));
        assert_eq!(detailed["detail_pruned"], json!(false));

        // A witness window beyond the 30-day max fails closed.
        let bad_window_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "witness_window_days": 31
            }),
        )
        .unwrap();
        let bad_window = bad_window_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_detail(
            py,
            path.to_str().unwrap(),
            bad_window,
            1_000
        )
        .is_err());

        // Unknown request fields and bad limits fail closed.
        let unknown_obj =
            json_value_to_py(py, &json!({"schema_version": 1, "nope": true}))
                .unwrap();
        let unknown = unknown_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_live_glance(
            py,
            path.to_str().unwrap(),
            unknown,
            1_000
        )
        .is_err());
        let bad_limit_obj =
            json_value_to_py(py, &json!({"schema_version": 1, "limit": 501}))
                .unwrap();
        let bad_limit = bad_limit_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_briefs(
            py,
            path.to_str().unwrap(),
            bad_limit,
            1_000
        )
        .is_err());
    });
}
