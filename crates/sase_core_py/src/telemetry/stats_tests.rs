use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn tool_run_stats_report_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        let missing = temp.path().join("missing.sqlite");
        // A missing store reports empty with the missing-store diagnostic.
        let empty_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "days": 7,
                "now_ts": 1_750_000_000,
                "utc_offset_seconds": 0,
            }),
        )
        .unwrap();
        let empty_request = empty_obj.bind(py).downcast::<PyDict>().unwrap();
        let empty = py_tool_run_stats_report(
            py,
            missing.to_str().unwrap(),
            empty_request,
            1_000,
        )
        .unwrap();
        let empty = py_to_json_value(empty.bind(py)).unwrap();
        assert_eq!(empty["schema_version"], json!(1));
        assert_eq!(empty["runs_scanned"], json!(0));
        assert_eq!(empty["tools"], json!([]));
        assert!(empty["thresholds"]["max_runs"].as_u64().unwrap() > 0);
        assert!(empty["diagnostics"][0]
            .as_str()
            .unwrap()
            .contains("does not exist"));

        // One finished run through begin/finish shows up grouped.
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
                "now_ts": 1_750_000_000,
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
        let finish_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "run_id": run_id,
                "state": "succeeded",
                "exit_code": 0,
                "duration_ms": 60_000,
                "now_ts": 1_750_000_060,
            }),
        )
        .unwrap();
        let finish_request = finish_obj.bind(py).downcast::<PyDict>().unwrap();
        py_tool_run_finish(py, path.to_str().unwrap(), finish_request, 1_000)
            .unwrap();

        let report_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "project": "sase",
                "days": 7,
                "now_ts": 1_750_003_600,
                "utc_offset_seconds": 0,
            }),
        )
        .unwrap();
        let report_request = report_obj.bind(py).downcast::<PyDict>().unwrap();
        let reported = py_tool_run_stats_report(
            py,
            path.to_str().unwrap(),
            report_request,
            1_000,
        )
        .unwrap();
        let reported = py_to_json_value(reported.bind(py)).unwrap();
        assert_eq!(reported["runs_scanned"], json!(1));
        assert_eq!(reported["tools"][0]["tool_name"], json!("check"));
        assert_eq!(reported["tools"][0]["outcomes"]["succeeded"], json!(1));
        assert_eq!(reported["tools"][0]["duration"]["p50_ms"], json!(60_000));

        // Days outside 1..=180 fail; so does a bad schema version.
        for days in [json!(0), json!(181)] {
            let bad_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "project": "sase",
                    "days": days,
                    "now_ts": 1_750_003_600,
                }),
            )
            .unwrap();
            let bad = bad_obj.bind(py).downcast::<PyDict>().unwrap();
            assert!(py_tool_run_stats_report(
                py,
                path.to_str().unwrap(),
                bad,
                1_000
            )
            .is_err());
        }
        let bad_schema_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 999,
                "project": "sase",
                "days": 7,
                "now_ts": 1_750_003_600,
            }),
        )
        .unwrap();
        let bad_schema = bad_schema_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_tool_run_stats_report(
            py,
            path.to_str().unwrap(),
            bad_schema,
            1_000
        )
        .is_err());
    });
}
