use super::admission::{
    py_tool_run_admission_enter, py_tool_run_admission_force,
    py_tool_run_admission_poll,
};
use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::{json, Value};
use tempfile::tempdir;

fn begin_check(py: Python<'_>, path: &str) -> String {
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
            "wrapper_pid": 4242,
            "boot_id": "boot-1",
            "process_start_identity": "start-1",
            "now_ts": 10,
            "commit_running": true
        }),
    )
    .unwrap();
    let request = begin_obj.bind(py).downcast::<PyDict>().unwrap();
    let started = py_tool_run_begin(py, path, request, 1_000).unwrap();
    let started = py_to_json_value(started.bind(py)).unwrap();
    started["run"]["run_id"].as_str().unwrap().to_string()
}

fn call(
    py: Python<'_>,
    binding: for<'py> fn(
        Python<'py>,
        &str,
        &Bound<'py, PyDict>,
        u64,
    ) -> PyResult<PyObject>,
    path: &str,
    request: Value,
) -> PyResult<Value> {
    let obj = json_value_to_py(py, &request).unwrap();
    let dict = obj.bind(py).downcast::<PyDict>().unwrap();
    let result = binding(py, path, dict, 1_000)?;
    Ok(py_to_json_value(result.bind(py)).unwrap())
}

#[test]
fn tool_run_admission_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        let path = path.to_str().unwrap();
        let holder = begin_check(py, path);
        let waiter = begin_check(py, path);

        let entered = call(
            py,
            py_tool_run_admission_enter,
            path,
            json!({
                "schema_version": 1,
                "run_id": holder,
                "key": "check",
                "limit": 1,
                "now_ms": 1_000
            }),
        )
        .unwrap();
        assert_eq!(entered["outcome"], json!("admitted"));
        assert_eq!(entered["admission"]["admitted_by"], json!("slot"));
        assert_eq!(entered["admission"]["wait_ms"], json!(0));

        let queued = call(
            py,
            py_tool_run_admission_enter,
            path,
            json!({
                "schema_version": 1,
                "run_id": waiter,
                "key": "check",
                "limit": 1,
                "now_ms": 2_000
            }),
        )
        .unwrap();
        assert_eq!(queued["outcome"], json!("waiting"));
        assert_eq!(queued["ahead"], json!(0));
        assert_eq!(queued["holders"][0]["run_id"], json!(holder));
        assert_eq!(queued["holders"][0]["wrapper_pid"], json!(4242));

        let polled = call(
            py,
            py_tool_run_admission_poll,
            path,
            json!({"schema_version": 1, "run_id": waiter, "now_ms": 3_000}),
        )
        .unwrap();
        assert_eq!(polled["outcome"], json!("waiting"));
        assert_eq!(polled["changed"], json!(false));

        let forced = call(
            py,
            py_tool_run_admission_force,
            path,
            json!({
                "schema_version": 1,
                "run_id": waiter,
                "reason": "max_wait",
                "now_ms": 9_000
            }),
        )
        .unwrap();
        assert_eq!(forced["outcome"], json!("admitted"));
        assert_eq!(forced["admission"]["admitted_by"], json!("max_wait"));
        assert_eq!(forced["admission"]["wait_ms"], json!(7_000));

        // show carries the stored record plus the live view.
        let shown = call(
            py,
            py_tool_run_show,
            path,
            json!({"schema_version": 1, "run_id": waiter}),
        )
        .unwrap();
        assert_eq!(
            shown["run"]["demand"]["admission"]["state"],
            json!("admitted")
        );
        assert_eq!(shown["admission"]["holders"], json!(2));

        // Unknown request fields are refused.
        let error = call(
            py,
            py_tool_run_admission_poll,
            path,
            json!({"schema_version": 1, "run_id": waiter, "bogus": true}),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("ToolRunAdmissionPollRequestWire"));
        // A run with no record surfaces the core error.
        let fresh = begin_check(py, path);
        let error = call(
            py,
            py_tool_run_admission_poll,
            path,
            json!({"schema_version": 1, "run_id": fresh}),
        )
        .unwrap_err();
        assert!(error.to_string().contains("no admission record"));
    })
}
