use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn wait_epic_follow_binding_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(
            py,
            &json!({
                "waiter_own_bead_ids": [],
                "now": 1_800_000_000.0,
                "launching_grace_seconds": 600.0,
                "launch_settle_seconds": 120.0,
                "targets": [
                    {
                        "target": "planner",
                        "agent_resolved": true,
                        "previous_state": null,
                        "previous_since": null,
                        "cycle_epic_ids": [],
                        "members": [
                            {
                                "name": "planner",
                                "artifact_dir": "/artifacts/planner",
                                "recorded_epic_ids": ["sase-7k"],
                                "attributed_epic_ids": [],
                                "legacy_epic_bead_id": null,
                                "is_epic_worker": false,
                                "launch_reserved": false,
                                "launch_argv_present": false,
                                "launch_in_flight": false,
                                "launch_reserved_age_seconds": null,
                                "member_dismissed": false,
                                "resume_command": null
                            }
                        ]
                    },
                    {
                        "target": "running",
                        "agent_resolved": false,
                        "previous_state": null,
                        "previous_since": null,
                        "cycle_epic_ids": [],
                        "members": []
                    }
                ]
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_wait_epic_follow_reduce(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
        assert_eq!(value[0]["target"], json!("planner"));
        assert_eq!(value[0]["state"], json!("following"));
        assert_eq!(value[0]["epic_ids"], json!(["sase-7k"]));
        assert_eq!(value[1]["state"], json!("agent"));
        assert!(result.bind(py).downcast::<PyList>().is_ok());
    });
}
