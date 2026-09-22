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
