use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn tool_run_record_demand_binding_round_trips() {
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
        assert!(started["run"].get("demand").is_none());

        let demand_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "context": {
                    "provider": "Muse",
                    "sync_ceiling_seconds": 14400
                },
                "usage": {
                    "cpu_user_ms": 200,
                    "cpu_system_ms": 50,
                    "max_process_rss_kib": 1100000,
                    "peak_tree_rss_kib": 9800000,
                    "tree_rss_samples": 54,
                    "availability": []
                },
                "worker_grants": [{
                    "grant_id": "grant-1",
                    "source": "pytest",
                    "observed_ts_ms": 1759240000000i64,
                    "lane": "fast",
                    "path": "lease",
                    "requested_floor": 4,
                    "requested_ceiling": 14,
                    "granted": 12,
                    "budget": 24,
                    "wait_ms": 182000,
                    "escalated_from": "scoped"
                }],
                "diagnostics": []
            }),
        )
        .unwrap();
        let demand_request = demand_obj.bind(py).downcast::<PyDict>().unwrap();
        let recorded = py_tool_run_record_demand(
            py,
            path.to_str().unwrap(),
            demand_request,
            1_000,
        )
        .unwrap();
        let recorded = py_to_json_value(recorded.bind(py)).unwrap();
        assert_eq!(recorded["replayed"], json!(false));
        assert_eq!(recorded["demand"]["context"]["provider"], json!("Muse"));
        assert_eq!(
            recorded["demand"]["worker_grants"][0]["grant_id"],
            json!("grant-1")
        );

        // show carries the same record.
        let show_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "run_id": run_id}),
        )
        .unwrap();
        let show_request = show_obj.bind(py).downcast::<PyDict>().unwrap();
        let shown =
            py_tool_run_show(py, path.to_str().unwrap(), show_request, 1_000)
                .unwrap();
        let shown = py_to_json_value(shown.bind(py)).unwrap();
        assert_eq!(shown["run"]["demand"]["usage"]["cpu_user_ms"], json!(200));

        // An unknown run surfaces the not-found error.
        let missing_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "run_id": "does-not-exist"}),
        )
        .unwrap();
        let missing_request =
            missing_obj.bind(py).downcast::<PyDict>().unwrap();
        let error = py_tool_run_record_demand(
            py,
            path.to_str().unwrap(),
            missing_request,
            1_000,
        )
        .unwrap_err();
        assert!(error.to_string().contains("was not found"));
    })
}
