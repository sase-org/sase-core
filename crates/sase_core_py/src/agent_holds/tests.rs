use super::*;
use crate::agent_launch::{py_launch_unit_hold_armer, py_launch_unit_hold_key};
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn agent_hold_bindings_round_trip_and_predicate() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        assert_eq!(py_agent_hold_wire_schema_version(), 2);
        let armer_obj = json_value_to_py(
            py,
            &json!({
                "kind": "agent",
                "key": "agent:hold-1",
                "display": "Hold 1",
                "project": "sase",
                "agent_name": "hold.agent",
                "family": "hold.agent",
                "clan": "hold-clan",
                "pid": 1234
            }),
        )
        .unwrap();
        let armer = armer_obj.bind(py).downcast::<PyDict>().unwrap();
        let scope_obj = json_value_to_py(
            py,
            &json!({"kind": "project", "project": "sase"}),
        )
        .unwrap();
        let scope = scope_obj.bind(py).downcast::<PyDict>().unwrap();
        let selectors_obj = json_value_to_py(
            py,
            &json!({
                "artifact_dirs": ["artifact/a"],
                "names": ["target.agent--code"],
                "families": ["target.agent"],
                "hoods": ["target"],
                "clans": ["target-clan"],
                "workflows": ["wf"],
                "tribes": ["tribe"],
                "future": true
            }),
        )
        .unwrap();
        let selectors = selectors_obj.bind(py).downcast::<PyDict>().unwrap();
        let liveness_obj = json_value_to_py(
            py,
            &json!({
                "armers": {
                    "agent:hold-1": {
                        "kind": "agent",
                        "pid_alive": true,
                        "done_marker_present": false
                    }
                }
            }),
        )
        .unwrap();
        let liveness = liveness_obj.bind(py).downcast::<PyDict>().unwrap();

        let capture_obj = json_value_to_py(
            py,
            &json!({
                "waiting_count": 2,
                "queued_count": 1,
                "skipped_running_count": 3
            }),
        )
        .unwrap();
        let capture = capture_obj.bind(py).downcast::<PyDict>().unwrap();
        let record = py_agent_hold_arm_relative(
            py,
            &home,
            armer,
            scope,
            selectors,
            60.0,
            Some(liveness),
            Some(now),
            Some(capture),
        )
        .unwrap();
        let record_value = py_to_json_value(record.bind(py)).unwrap();
        assert_eq!(record_value["expires_at"], json!(now + 60.0));
        assert_eq!(record_value["capture"]["waiting_count"], json!(2));
        assert_eq!(record_value["capture"]["queued_count"], json!(1));
        assert_eq!(record_value["capture"]["skipped_running_count"], json!(3));

        let snapshot =
            py_agent_hold_list(py, &home, Some(liveness), Some(now + 1.0))
                .unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["holds"].as_array().unwrap().len(), 1);

        let candidate_obj = json_value_to_py(
            py,
            &json!({
                "project": "sase",
                "created_at": now + 2.0,
                "artifact_dirs": ["artifact/a"],
                "agent_name": "target.agent--code",
                "clan": "target-clan",
                "workflow": "wf",
                "tribe": "tribe"
            }),
        )
        .unwrap();
        let candidate = candidate_obj.bind(py).downcast::<PyDict>().unwrap();
        let record_dict = record.bind(py).downcast::<PyDict>().unwrap();
        let block =
            py_agent_hold_blocks_candidate(py, record_dict, candidate).unwrap();
        let block_value = py_to_json_value(block.bind(py)).unwrap();
        assert_eq!(block_value["armer"]["key"], json!("agent:hold-1"));
        assert_eq!(
            block_value["matches"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "artifact_dir",
                "name",
                "session",
                "hood",
                "clan",
                "workflow",
                "tribe",
                "future"
            ]
        );

        let released = py_agent_hold_release(
            &home,
            "agent:hold-1",
            Some(liveness),
            Some(now + 3.0),
        )
        .unwrap();
        assert!(released);
        assert!(!py_agent_hold_release(
            &home,
            "agent:hold-1",
            Some(liveness),
            Some(now + 4.0),
        )
        .unwrap());
    });
}

#[test]
fn agent_hold_rebind_and_launch_hold_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        assert_eq!(
            py_launch_unit_hold_key("request123", "unit-1").unwrap(),
            "launch:request123/unit-1"
        );
        assert!(py_launch_unit_hold_key("request 123", "unit-1")
            .unwrap_err()
            .is_instance_of::<PyValueError>(py));

        let unit_obj = json_value_to_py(
            py,
            &json!({
                "logical_id": "unit-1",
                "source_order": 1,
                "payload": {
                    "kind": "agent",
                    "prompt": "Review",
                    "identity": "reviewer",
                    "identity_explicit": true,
                    "clan": "research"
                }
            }),
        )
        .unwrap();
        let launch_armer = py_launch_unit_hold_armer(
            py,
            unit_obj.bind(py),
            "request123",
            "sase",
            4321,
            "/tmp/receipt.json",
        )
        .unwrap();
        let launch_armer_value =
            py_to_json_value(launch_armer.bind(py)).unwrap();
        assert_eq!(
            launch_armer_value["key"],
            json!("launch:request123/unit-1")
        );
        assert_eq!(launch_armer_value["kind"], json!("launch"));
        assert_eq!(
            launch_armer_value["agent_name"],
            json!("research.reviewer")
        );
        assert_eq!(launch_armer_value["clan"], json!("research"));

        let armer_obj = json_value_to_py(
            py,
            &json!({
                "kind": "agent",
                "key": "agent:old",
                "display": "Old hold",
                "project": "sase",
                "agent_name": "old.agent",
                "family": "old.agent",
                "pid": 1234
            }),
        )
        .unwrap();
        let armer = armer_obj.bind(py).downcast::<PyDict>().unwrap();
        let scope_obj = json_value_to_py(py, &json!({"kind": "host"})).unwrap();
        let scope = scope_obj.bind(py).downcast::<PyDict>().unwrap();
        let selectors_obj =
            json_value_to_py(py, &json!({"future": true})).unwrap();
        let selectors = selectors_obj.bind(py).downcast::<PyDict>().unwrap();

        py_agent_hold_arm_relative(
            py,
            &home,
            armer,
            scope,
            selectors,
            60.0,
            None,
            Some(now),
            None,
        )
        .unwrap();
        let new_armer_obj = json_value_to_py(
            py,
            &json!({
                "kind": "agent",
                "key": "agent:new",
                "display": "New hold",
                "project": "sase",
                "agent_name": "new.agent",
                "family": "new.agent",
                "pid": 5678
            }),
        )
        .unwrap();
        let new_armer = new_armer_obj.bind(py).downcast::<PyDict>().unwrap();
        let rebound = py_agent_hold_rebind(
            py,
            &home,
            "agent:old",
            new_armer,
            None,
            Some(now + 1.0),
        )
        .unwrap();
        let rebound_value = py_to_json_value(rebound.bind(py)).unwrap();
        assert_eq!(rebound_value["armer"]["key"], json!("agent:new"));
        assert_eq!(rebound_value["created_at"], json!(now));

        let absent = py_agent_hold_rebind(
            py,
            &home,
            "agent:missing",
            new_armer,
            None,
            Some(now + 2.0),
        )
        .unwrap();
        assert!(absent.bind(py).is_none());
    });
}

#[test]
fn agent_hold_capture_summary_and_prune_bindings() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        let scope_obj = json_value_to_py(py, &json!({"kind": "host"})).unwrap();
        let scope = scope_obj.bind(py).downcast::<PyDict>().unwrap();
        let armer_obj = json_value_to_py(
            py,
            &json!({
                "kind": "agent",
                "key": "agent:holder",
                "display": "holder",
                "project": "sase",
                "agent_name": "holder.worker",
                "family": "holder.worker",
                "clan": "builders",
                "pid": 1234
            }),
        )
        .unwrap();
        let armer = armer_obj.bind(py).downcast::<PyDict>().unwrap();
        let identities_obj = json_value_to_py(
            py,
            &json!([
                {
                    "project": "sase",
                    "created_at": now,
                    "bucket": "waiting",
                    "artifact_dir": "artifacts/w1",
                    "agent_name": "target.agent--code",
                    "family": "target.agent",
                    "clan": "ops"
                },
                {
                    "project": "sase",
                    "created_at": now,
                    "bucket": "waiting",
                    "artifact_dir": "artifacts/kin",
                    "agent_name": "holder.worker",
                    "family": "holder.worker",
                    "clan": "builders"
                },
                {
                    "project": "sase",
                    "created_at": now,
                    "bucket": "running",
                    "artifact_dir": "artifacts/r1",
                    "agent_name": "running.agent--code",
                    "family": "running.agent"
                }
            ]),
        )
        .unwrap();
        let identities = identities_obj.bind(py).downcast::<PyList>().unwrap();
        let summary =
            py_agent_hold_summarize_capture(py, scope, identities, Some(armer))
                .unwrap();
        let summary_value = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(summary_value["summary"]["waiting_count"], json!(1));
        assert_eq!(summary_value["summary"]["skipped_running_count"], json!(1));
        assert_eq!(summary_value["artifact_dirs"], json!(["artifacts/w1"]));

        let selectors_obj =
            json_value_to_py(py, &json!({"future": true})).unwrap();
        let selectors = selectors_obj.bind(py).downcast::<PyDict>().unwrap();
        py_agent_hold_arm_relative(
            py,
            &home,
            armer,
            scope,
            selectors,
            5.0,
            None,
            Some(now),
            None,
        )
        .unwrap();
        let snapshot =
            py_agent_hold_list(py, &home, None, Some(now + 10.0)).unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["holds"].as_array().unwrap().len(), 0);
        assert_eq!(snapshot_value["pruned"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot_value["pruned"][0]["reason"], json!("expiry"));
    });
}

#[test]
fn agent_hold_deadlock_reaches_walks_every_wait_branch() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let candidate_obj = json_value_to_py(
            py,
            &json!({
                "artifact_dir": "/a/20260910120000",
                "agent_name": "candidate.agent",
                "timestamp": "20260910120000"
            }),
        )
        .unwrap();
        let candidate = candidate_obj.bind(py).downcast::<PyDict>().unwrap();
        let nodes_obj = json_value_to_py(
            py,
            &json!([
                {
                    "artifact_dir": "/a/20260910120001",
                    "agent_name": "armer.agent",
                    "timestamp": "20260910120001",
                    "waiting_for": ["safe.agent", "bridge.agent"]
                },
                {
                    "artifact_dir": "/a/20260910120002",
                    "agent_name": "safe.agent",
                    "timestamp": "20260910120002",
                    "waiting_for": ["unrelated.agent"]
                },
                {
                    "artifact_dir": "/a/20260910120003",
                    "agent_name": "bridge.agent",
                    "timestamp": "20260910120003",
                    "waiting_for": ["candidate.agent"]
                }
            ]),
        )
        .unwrap();
        let nodes = nodes_obj.bind(py).downcast::<PyList>().unwrap();
        assert!(py_agent_hold_deadlock_reaches(
            "/a/20260910120001",
            candidate,
            nodes,
        )
        .unwrap());
    });
}

#[cfg(unix)]
#[test]
fn agent_hold_bindings_map_validation_and_lock_errors() {
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;

    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        let armer_obj = json_value_to_py(
            py,
            &json!({
                "kind": "cli",
                "key": "cli:hold",
                "display": "CLI hold",
                "project": "sase",
                "pid": 1234
            }),
        )
        .unwrap();
        let armer = armer_obj.bind(py).downcast::<PyDict>().unwrap();
        let scope_obj = json_value_to_py(py, &json!({"kind": "host"})).unwrap();
        let scope = scope_obj.bind(py).downcast::<PyDict>().unwrap();
        let selectors_obj =
            json_value_to_py(py, &json!({"future": true})).unwrap();
        let selectors = selectors_obj.bind(py).downcast::<PyDict>().unwrap();

        let validation_error = py_agent_hold_arm_relative(
            py,
            &home,
            armer,
            scope,
            selectors,
            -1.0,
            None,
            Some(now),
            None,
        )
        .unwrap_err();
        assert!(validation_error.is_instance_of::<PyValueError>(py));

        let lock_path = sase_core::agent_hold_lock_path(temp.path());
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        let lock_result =
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
        assert_eq!(lock_result, 0);
        std::env::set_var("SASE_AGENT_HOLD_LOCK_TIMEOUT", "0.01");
        let timeout =
            py_agent_hold_list(py, &home, None, Some(now)).unwrap_err();
        std::env::remove_var("SASE_AGENT_HOLD_LOCK_TIMEOUT");
        let unlock_result =
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) };
        assert_eq!(unlock_result, 0);
        assert!(timeout.is_instance_of::<PyTimeoutError>(py));
    });
}

#[test]
fn agent_hold_bindings_accept_legacy_spellings_and_serialize_canonical() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        let armer_obj = json_value_to_py(
            py,
            &json!({
                "kind": "agent",
                "key": "agent:hold-new",
                "display": "Hold New",
                "project": "sase",
                "agent_name": "hold.agent",
                "agent_session": "hold.agent",
                "pid": 1234
            }),
        )
        .unwrap();
        let armer = armer_obj.bind(py).downcast::<PyDict>().unwrap();
        let scope_obj = json_value_to_py(
            py,
            &json!({"kind": "project", "project": "sase"}),
        )
        .unwrap();
        let scope = scope_obj.bind(py).downcast::<PyDict>().unwrap();
        let selectors_obj = json_value_to_py(
            py,
            &json!({
                "artifact_dirs": ["artifact/a"],
                "agent_sessions": ["target.agent"],
                "future": false
            }),
        )
        .unwrap();
        let selectors = selectors_obj.bind(py).downcast::<PyDict>().unwrap();
        let record = py_agent_hold_arm_relative(
            py,
            &home,
            armer,
            scope,
            selectors,
            60.0,
            None,
            Some(now),
            None,
        )
        .unwrap();
        let record_value = py_to_json_value(record.bind(py)).unwrap();
        assert_eq!(record_value["armer"]["agent_session"], json!("hold.agent"));
        assert!(record_value["armer"].get("family").is_none());
        assert_eq!(
            record_value["selectors"]["agent_sessions"],
            json!(["target.agent"])
        );
        assert!(record_value["selectors"].get("families").is_none());
    });
}
