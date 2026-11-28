use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

fn leaf_request() -> serde_json::Value {
    json!({
        "schema_version": 2,
        "now": 1_800_000_000.0,
        "settle_grace_seconds": 120.0,
        "beads": [
            {
                "id": "w.1",
                "kind": "phase",
                "status": "in_progress",
                "candidate": true,
                "assignee": "w.1",
                "parent_id": "w",
                "closed_at": null,
                "records_available": true,
                "members": [
                    {
                        "name": "w.1--3",
                        "artifact_dir": "/artifacts/w.1--3",
                        "turn_kind": "agent",
                        "lifecycle": "done",
                        "process_live": false,
                        "remote": false,
                        "finalizing": false,
                        "pending_handoff": null,
                        "handoff_inflight": false,
                        "recovery_in_flight": false,
                        "workspace_retention": false,
                        "turn_state": null,
                        "followup_pending": false,
                        "followup_outcome": null,
                        "followup_agent": null,
                        "declared_bead_action": "keep",
                        "started_at": 1_799_996_400.0,
                        "ended_at": 1_799_996_400.0,
                    }
                ]
            },
            {
                "id": "w",
                "kind": "epic",
                "status": "in_progress",
                "candidate": true,
                "assignee": "w",
                "parent_id": null,
                "closed_at": null,
                "records_available": true,
                "members": [
                    {
                        "name": "w--land",
                        "artifact_dir": "/artifacts/w--land",
                        "turn_kind": "agent",
                        "lifecycle": "waiting",
                        "process_live": true,
                        "remote": false,
                        "finalizing": false,
                        "pending_handoff": null,
                        "handoff_inflight": false,
                        "recovery_in_flight": false,
                        "workspace_retention": false,
                        "turn_state": null,
                        "followup_pending": false,
                        "followup_outcome": null,
                        "followup_agent": null,
                        "declared_bead_action": null,
                        "started_at": 1_799_996_400.0,
                        "ended_at": null,
                    }
                ]
            }
        ]
    })
}

#[test]
fn stranded_work_binding_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(py, &leaf_request()).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_stranded_work_reduce(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(2));
        assert_eq!(value["stranded_ids"], json!(["w.1"]));
        assert_eq!(value["blocked_ids"], json!(["w"]));
        let verdicts = value["verdicts"].as_array().unwrap();
        assert_eq!(verdicts.len(), 2);
        assert_eq!(verdicts[0]["bead_id"], json!("w"));
        assert_eq!(verdicts[1]["bead_id"], json!("w.1"));
        assert_eq!(verdicts[1]["state"], json!("stranded"));
        assert_eq!(verdicts[1]["reason"], json!("kept"));
        assert!(result.bind(py).downcast::<PyDict>().is_ok());
    });
}

#[test]
fn stranded_work_binding_registered_by_name() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let reduce = module.getattr("stranded_work_reduce").unwrap();
        let request_obj = json_value_to_py(py, &leaf_request()).unwrap();
        let result = reduce.call1((request_obj,)).unwrap();
        let value = py_to_json_value(&result).unwrap();
        assert_eq!(value["stranded_ids"], json!(["w.1"]));
        assert_eq!(value["blocked_ids"], json!(["w"]));
    });
}

#[test]
fn zombie_plan_binding_round_trips_the_shared_predicate() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let evaluate = module.getattr("zombie_plan_evaluate").unwrap();
        let request = json!({
            "schema_version": 1,
            "snapshot": {
                "schema_version": 1,
                "complete": true,
                "unknown_reason": null,
                "beads": [
                    {
                        "id": "z",
                        "title": "Epic",
                        "status": "in_progress",
                        "issue_type": "plan",
                        "tier": "epic",
                        "parent_id": null,
                        "assignee": "z--land",
                        "closed_at": null,
                        "close_event_id": null
                    },
                    {
                        "id": "z.1",
                        "title": "Phase",
                        "status": "closed",
                        "issue_type": "phase",
                        "tier": null,
                        "parent_id": "z",
                        "assignee": "",
                        "closed_at": "2026-01-01T00:00:00Z",
                        "close_event_id": "close-1"
                    }
                ]
            },
            "owners": [{
                "plan_id": "z",
                "records_available": true,
                "unknown_reason": null,
                "members": [{
                    "name": "z--land",
                    "turn_kind": "agent",
                    "lifecycle": "done",
                    "started_at": 1767225600.0,
                    "ended_at": 1767225700.0
                }]
            }]
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let result = evaluate.call1((request_obj,)).unwrap();
        let value = py_to_json_value(&result).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["rows"][0]["state"], json!("zombie"));
        assert_eq!(value["rows"][0]["recovery_eligible"], json!(true));
        assert_eq!(value["rows"][0]["close_token"], json!("z.1:close-1"));
    });
}
