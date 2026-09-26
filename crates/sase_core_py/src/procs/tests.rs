use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;
use std::fs;

#[test]
fn proc_store_bindings_round_trip_python_dicts_and_legacy_aliases() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("procs.jsonl");
    let path = path.to_str().unwrap();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "read_procs_snapshot",
            "append_proc",
            "reserve_proc",
            "update_proc",
            "claim_proc_supervisor",
            "request_proc_stop",
            "begin_proc_settlement",
            "finish_proc",
            "prune_procs",
            "proc_runtime_retention_wire_schema_version",
            "apply_proc_runtime_retention",
            "command_line_proc_tag",
            "command_line_proc_history_limit",
            "agent_artifact_run_retention_wire_schema_version",
            "apply_agent_artifact_run_retention",
            "read_tasks_snapshot",
            "append_task",
            "update_task",
            "prune_tasks",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let task = json_value_to_py(
            py,
            &json!({
                "task_id": "proc-one",
                "label": "Binding proc",
                "kind": "command",
                "status": "pending",
                "command": ["true"],
                "cwd": "/tmp",
                "project": "sase",
                "workspace_num": 16,
                "session_id": "session",
                "session_label": "ace",
                "origin": "test",
                "cl_name": null,
                "tags": ["binding", "binding"],
                "pid": null,
                "pgid": null,
                "exit_code": null,
                "phase": "queued",
                "message": null,
                "created_at": "2026-07-25T12:00:00Z",
                "started_at": null,
                "finished_at": null,
                "log_path": "/tmp/proc-one.log"
            }),
        )
        .unwrap();
        let task = task.bind(py).downcast::<PyDict>().unwrap();
        let appended = py_append_proc(py, path, task, 10).unwrap();
        let appended = py_to_json_value(appended.bind(py)).unwrap();
        assert_eq!(appended["snapshot"]["procs"][0]["proc_id"], "proc-one");
        assert_eq!(
            appended["snapshot"]["procs"][0]["tags"],
            json!(["binding"])
        );

        let update = json_value_to_py(
            py,
            &json!({
                "task_id": "proc-one",
                "status": "running",
                "session_id": null,
                "phase": null,
                "pid": 42
            }),
        )
        .unwrap();
        let update = update.bind(py).downcast::<PyDict>().unwrap();
        let updated = py_update_task(py, path, update).unwrap();
        let updated = py_to_json_value(updated.bind(py)).unwrap();
        assert_eq!(updated["proc"]["status"], "running");
        assert_eq!(updated["proc"]["session_id"], JsonValue::Null);
        assert_eq!(updated["proc"]["phase"], JsonValue::Null);
        assert_eq!(updated["proc"]["pid"], 42);

        let snapshot = py_read_tasks_snapshot(py, path).unwrap();
        let snapshot = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot["procs"][0]["proc_id"], "proc-one");
        let pruned = py_prune_tasks(py, path, 0).unwrap();
        let pruned = py_to_json_value(pruned.bind(py)).unwrap();
        assert!(pruned["pruned_proc_ids"].as_array().unwrap().is_empty());

        let reserve = json_value_to_py(
            py,
            &json!({
                "schema_version": 3,
                "proc_id": "proc-service",
                "label": "Service proc",
                "kind": "detached",
                "argv": ["sleep", "1"],
                "cwd": "/tmp",
                "project": "sase",
                "workspace_num": 16,
                "session_id": null,
                "session_label": null,
                "origin": "test",
                "cl_name": null,
                "tags": ["service"],
                "created_at": "2026-07-25T12:00:10Z",
                "log_path": "/tmp/proc-service.log",
                "log_owner": "proc-store",
                "shell_name": "gateway",
                "shell_kind": "proc",
                "concurrency_keys": ["service:gateway"],
                "request_fingerprint": "service-fingerprint",
                "reserved_by": "agent-one",
                "timeout_seconds": null,
                "idle_timeout_seconds": null,
                "service": {
                    "name": "gateway",
                    "mode": "daemon",
                    "source": "builtin"
                }
            }),
        )
        .unwrap();
        let reserve = reserve.bind(py).downcast::<PyDict>().unwrap();
        let reserved = py_reserve_proc(py, path, reserve, 10).unwrap();
        let reserved = py_to_json_value(reserved.bind(py)).unwrap();
        assert_eq!(
            reserved["proc"]["service"],
            json!({
                "name": "gateway",
                "mode": "daemon",
                "source": "builtin"
            })
        );
    });
}

#[test]
fn command_line_proc_binding_exposes_tag_and_limit() {
    assert_eq!(py_command_line_proc_tag(), "command-line");
    assert_eq!(py_command_line_proc_history_limit(), 50);
}

#[test]
fn proc_runtime_retention_binding_requires_trustworthy_store_snapshot() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let store = temp.path().join("procs.jsonl");
    let runtime_root = temp.path().join("runtime");
    let runtime_dir = runtime_root.join("0123456789ab");
    fs::create_dir_all(&runtime_dir).unwrap();
    fs::write(runtime_dir.join("request.json"), "{}").unwrap();
    fs::write(&store, "not-json\n").unwrap();

    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "store_path": store.to_string_lossy(),
                "runtime_root": runtime_root.to_string_lossy(),
                "now_epoch_seconds": 9_999_999_999.0,
                "orphan_horizon_seconds": 1.0,
                "max_orphan_removals": 10,
                "apply": true,
                "pruned_proc_ids": [],
                "sweep_orphans": true
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let error = py_apply_proc_runtime_retention(py, request).unwrap_err();
        assert!(error.to_string().contains("incomplete proc store snapshot"));
        assert!(runtime_dir.exists());

        fs::write(&store, "").unwrap();
        let outcome = py_apply_proc_runtime_retention(py, request).unwrap();
        let outcome = py_to_json_value(outcome.bind(py)).unwrap();
        assert_eq!(outcome["removed"], json!(1));
        assert!(!runtime_dir.exists());
    });
}

#[test]
fn reserve_proc_accepts_proc_name_spelling_like_shell_name() {
    pyo3::prepare_freethreaded_python();
    fn reserve_payload(proc_id: &str, fingerprint: &str) -> serde_json::Value {
        json!({
            "schema_version": 3,
            "proc_id": proc_id,
            "label": "Binding proc",
            "kind": "detached",
            "argv": ["sleep", "1"],
            "cwd": "/tmp",
            "project": "sase",
            "workspace_num": 16,
            "session_id": null,
            "session_label": null,
            "origin": "test",
            "cl_name": null,
            "tags": ["binding"],
            "created_at": "2026-07-25T12:00:10Z",
            "log_path": "/tmp/proc-binding.log",
            "log_owner": "proc-store",
            "concurrency_keys": [],
            "request_fingerprint": fingerprint,
            "reserved_by": "agent-one",
            "timeout_seconds": null,
            "idle_timeout_seconds": null
        })
    }
    Python::with_gil(|py| {
        let mut legacy_payload = reserve_payload("proc-legacy", "fp-legacy");
        legacy_payload["shell_name"] = json!("gateway");
        legacy_payload["shell_kind"] = json!("proc");
        let mut renamed_payload = reserve_payload("proc-renamed", "fp-renamed");
        renamed_payload["proc_name"] = json!("gateway");
        renamed_payload["proc_role"] = json!("proc");

        let temp = tempfile::tempdir().unwrap();
        let legacy_path = temp
            .path()
            .join("legacy.jsonl")
            .to_string_lossy()
            .into_owned();
        let legacy_dict = json_value_to_py(py, &legacy_payload).unwrap();
        let legacy_dict = legacy_dict.bind(py).downcast::<PyDict>().unwrap();
        let legacy_outcome =
            py_reserve_proc(py, &legacy_path, legacy_dict, 10).unwrap();
        let legacy_outcome = py_to_json_value(legacy_outcome.bind(py)).unwrap();

        let renamed_path = temp
            .path()
            .join("renamed.jsonl")
            .to_string_lossy()
            .into_owned();
        let renamed_dict = json_value_to_py(py, &renamed_payload).unwrap();
        let renamed_dict = renamed_dict.bind(py).downcast::<PyDict>().unwrap();
        let renamed_outcome =
            py_reserve_proc(py, &renamed_path, renamed_dict, 10).unwrap();
        let renamed_outcome =
            py_to_json_value(renamed_outcome.bind(py)).unwrap();

        // Both spellings validate the same; emitted rows keep legacy keys.
        assert_eq!(
            renamed_outcome["proc"]["shell_name"],
            legacy_outcome["proc"]["shell_name"]
        );
        assert_eq!(renamed_outcome["proc"]["shell_name"], json!("gateway"));
        assert!(renamed_outcome["proc"].get("proc_name").is_none());
        assert_eq!(renamed_outcome["proc"]["shell_kind"], json!("proc"));
    });
}
