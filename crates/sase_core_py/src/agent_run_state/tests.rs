use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn agent_run_state_bindings_register_and_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "agent_run_state_wire_schema_version",
            "resolve_agent_run_state",
            "select_agent_run_state_window",
            "advance_agent_run_state_schedule",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(
            py_to_json_value(
                &module
                    .getattr("agent_run_state_wire_schema_version")
                    .unwrap()
                    .call0()
                    .unwrap()
            )
            .unwrap(),
            json!(AGENT_RUN_STATE_WIRE_SCHEMA_VERSION)
        );

        let identity = json!({
            "project": "project-a",
            "owner": "alice@athena",
            "global_name": "alice.athena.worker",
            "source_run_id": "run-1"
        });
        let resolved = module
            .getattr("resolve_agent_run_state")
            .unwrap()
            .call1((json_value_to_py(
                py,
                &json!({
                    "schema_version": AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
                    "identity": identity,
                    "published_state": "waiting",
                    "published_started_at": null,
                    "published_finished_at": null,
                    "evidence_kind": "terminal_marker",
                    "evidence_identity": identity,
                    "evidence_state": "completed",
                    "evidence_started_at": "2026-10-29T12:00:00Z",
                    "evidence_finished_at": "2026-10-29T12:01:00Z"
                }),
            )
            .unwrap(),))
            .unwrap();
        let resolved = py_to_json_value(&resolved).unwrap();
        assert_eq!(resolved["selected_state"], json!("completed"));
        assert_eq!(resolved["changed"], json!(true));
        assert_eq!(resolved["reason"], json!("updated_from_terminal_marker"));

        let selected = module
            .getattr("select_agent_run_state_window")
            .unwrap()
            .call1((json_value_to_py(
                py,
                &json!({
                    "schema_version": AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
                    "project": "project-a",
                    "owner": "alice@athena",
                    "limit": 1,
                    "after": null,
                    "rows": [
                        {"local_hood": "hood-b", "source_run_id": "run-2"},
                        {"local_hood": "hood-a", "source_run_id": "run-1"}
                    ]
                }),
            )
            .unwrap(),))
            .unwrap();
        let selected = py_to_json_value(&selected).unwrap();
        assert_eq!(selected["selected"][0]["local_hood"], json!("hood-a"));
        assert_eq!(selected["pass_complete"], json!(false));
        assert!(selected["next_cursor"]
            .as_str()
            .unwrap()
            .starts_with("ars1:"));

        let schedule = module
            .getattr("advance_agent_run_state_schedule")
            .unwrap()
            .call1((
                json_value_to_py(
                    py,
                    &json!({
                        "schema_version": AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
                        "now_ms": 10_000,
                        "last_selected": null,
                        "projects": [{
                            "identity": {"project": "project-a", "owner": "alice@athena"},
                            "enabled": true,
                            "configured": true,
                            "cursor": null,
                            "failure_count": 0,
                            "retry_after_ms": 0
                        }],
                        "outcome": null
                    }),
                )
                .unwrap(),
            ))
            .unwrap();
        let schedule = py_to_json_value(&schedule).unwrap();
        assert_eq!(schedule["next_project"]["project"], json!("project-a"));
    });
}

#[test]
fn agent_run_state_binding_maps_core_validation_to_value_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let error = module
            .getattr("select_agent_run_state_window")
            .unwrap()
            .call1((json_value_to_py(
                py,
                &json!({
                    "schema_version": AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
                    "project": "project-a",
                    "owner": "alice@athena",
                    "limit": 0,
                    "after": null,
                    "rows": []
                }),
            )
            .unwrap(),))
            .unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
    });
}
