use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

fn dict<'py>(py: Python<'py>, value: serde_json::Value) -> Bound<'py, PyDict> {
    json_value_to_py(py, &value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

fn incident_witnesses() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "boot_identity": "sase@9c5000f",
        "current_identity": "sase@9fd8a08",
        "journal_updates": ["9fd8a08"],
        "file_proof": {
            "symbol": "auto_launch_prefix",
            "module": "sase.monitor.continuation_delivery",
            "boot_has": true,
            "head_has": false,
            "culprit_commit": "9fd8a081f4",
            "culprit_subject": "feat(autonomy)",
        },
        "probe": {"ok": true, "failures": []},
        "refresh_log_line": {"from": "9c5000f", "to": "9fd8a08"},
    })
}

fn incident_context() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "managed_roots": [{"name": "sase", "root": "/opt/sase"}],
        "workspace_dir": "/home/user/work",
        "outcome": "failed",
        "kill_source": serde_json::Value::Null,
        "lifecycle_phase": "waiting",
        "has_pending_question": false,
        "has_pending_handoff": false,
        "is_remote": false,
        "error_text": "ImportError: cannot import name 'auto_launch_prefix' from 'sase.monitor.continuation_delivery'",
        "traceback_text": "File \"/opt/sase/src/sase/axe/run_agent_runner_refresh.py\", line 42, in refresh_runner_code_after_wait",
        "log_tail": "Refreshing sase runner code after dependency wait",
    })
}

#[test]
fn auto_restart_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_agent_auto_restart(&module).unwrap();
        assert!(module.getattr("classify_agent_failure").is_ok());
        assert!(module.getattr("advance_auto_restart_ledger").is_ok());
        assert!(module.getattr("claim_auto_restart_ledger").is_ok());
        assert!(module.getattr("auto_restart_lineage_root").is_ok());
        assert!(module.getattr("derive_auto_restart_episode").is_ok());
        assert!(module.getattr("auto_restart_recovery_is_in_flight").is_ok());
        assert_eq!(py_agent_auto_restart_wire_schema_version(), 1);
    });
}

#[test]
fn classify_binding_relaunches_incident() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let context = dict(py, incident_context());
        let witnesses = dict(py, incident_witnesses());
        let result =
            py_classify_agent_failure(py, None, &context, &witnesses).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["mode"], json!("relaunch"));
        assert_eq!(value["episode_id"], json!("sase@9fd8a08"));
    });
}

#[test]
fn ledger_binding_rejects_illegal_transitions() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let record = py_claim_auto_restart_ledger(py, "k", "root").unwrap();
        let record = record.bind(py).downcast::<PyDict>().unwrap();
        assert!(
            py_advance_auto_restart_ledger(py, record, "settled_ok").is_err()
        );
        let deferred =
            py_advance_auto_restart_ledger(py, record, "defer").unwrap();
        let value = py_to_json_value(deferred.bind(py)).unwrap();
        assert_eq!(value["state"], json!("deferred"));
        assert_eq!(value["deferrals"], json!(1));
    });
}

#[test]
fn episode_and_in_flight_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let witnesses = dict(py, incident_witnesses());
        let episode = py_derive_auto_restart_episode(py, &witnesses).unwrap();
        let value = py_to_json_value(episode.bind(py)).unwrap();
        assert_eq!(value["id"], json!("sase@9fd8a08"));
        assert!(py_auto_restart_recovery_is_in_flight(Some("pending")));
        assert!(!py_auto_restart_recovery_is_in_flight(Some("declined")));
        assert!(!py_auto_restart_recovery_is_in_flight(None));
        assert_eq!(
            py_auto_restart_lineage_root("row", Some("lineage"), None),
            "lineage"
        );
    });
}
