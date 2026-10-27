use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;
use rusqlite::Connection;
use serde_json::json;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn begin_request<'py>(py: Python<'py>) -> Bound<'py, PyDict> {
    let obj = json_value_to_py(
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
    obj.bind(py).downcast::<PyDict>().unwrap().clone()
}

#[test]
fn tool_run_busy_timeout_omitted_zero_and_explicit() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(wrap_pyfunction!(py_tool_run_show, &module).unwrap())
            .unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_telemetry_store_stats, &module).unwrap(),
            )
            .unwrap();
        let show = module.getattr("tool_run_show").unwrap();
        let signature: String = show
            .getattr("__text_signature__")
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            signature.contains("busy_timeout_ms=0"),
            "omitted default must be the core zero sentinel, got {signature}"
        );

        let telemetry = module.getattr("telemetry_store_stats").unwrap();
        let telemetry_sig: String = telemetry
            .getattr("__text_signature__")
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            telemetry_sig.contains("busy_timeout_ms=250"),
            "unrelated telemetry bindings must keep 250, got {telemetry_sig}"
        );

        let temp = tempdir().unwrap();
        let path = temp.path().join("runs.sqlite");
        let path_str = path.to_str().unwrap();
        let started =
            py_tool_run_begin(py, path_str, &begin_request(py), 1_000).unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        let run_id = started["run"]["run_id"].as_str().unwrap().to_string();
        let show_obj = json_value_to_py(
            py,
            &json!({"schema_version": 1, "run_id": run_id}),
        )
        .unwrap();
        let show_request = show_obj.bind(py).downcast::<PyDict>().unwrap();

        let omitted = show.call1((path_str, show_request.clone())).unwrap();
        let omitted = py_to_json_value(&omitted).unwrap();
        assert_eq!(omitted["run"]["run_id"], json!(run_id));

        let zero = show.call1((path_str, show_request.clone(), 0u64)).unwrap();
        let zero = py_to_json_value(&zero).unwrap();
        assert_eq!(zero["run"]["run_id"], json!(run_id));

        let explicit =
            show.call1((path_str, show_request.clone(), 50u64)).unwrap();
        let explicit = py_to_json_value(&explicit).unwrap();
        assert_eq!(explicit["run"]["run_id"], json!(run_id));
    });
}

#[test]
fn tool_run_show_releases_gil_while_waiting() {
    pyo3::prepare_freethreaded_python();
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let path_str = path.to_str().unwrap().to_string();
    let run_id = Python::with_gil(|py| {
        let started =
            py_tool_run_begin(py, &path_str, &begin_request(py), 1_000)
                .unwrap();
        let started = py_to_json_value(started.bind(py)).unwrap();
        started["run"]["run_id"].as_str().unwrap().to_string()
    });
    let holder = Connection::open(&path).unwrap();
    holder
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    holder
        .execute(
            "UPDATE meta SET value = value WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    let path_show = path_str.clone();
    let run_id_show = run_id.clone();
    let handle = thread::spawn(move || {
        Python::with_gil(|py| {
            let show_obj = json_value_to_py(
                py,
                &json!({"schema_version": 1, "run_id": run_id_show}),
            )
            .unwrap();
            let show_request = show_obj.bind(py).downcast::<PyDict>().unwrap();
            py_tool_run_show(py, &path_show, show_request, 200)
        })
    });
    thread::sleep(Duration::from_millis(30));
    let started = Instant::now();
    Python::with_gil(|_py| {});
    let gil_wait = started.elapsed();
    drop(holder);
    let _ = handle.join().unwrap();
    assert!(
        gil_wait < Duration::from_millis(100),
        "tool_run_show must release the GIL while waiting, gil_wait={gil_wait:?}"
    );
}
