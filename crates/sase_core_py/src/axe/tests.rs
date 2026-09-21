use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

fn healthy_chop_overrun_request_json() -> JsonValue {
    json!({
        "schema_version": CHOP_OVERRUN_SCHEMA_VERSION,
        "now": "2026-08-12T09:15:00-04:00",
        "interval_seconds": 60,
        "runs": [{
            "status": "success",
            "started_at": "2026-08-12T09:12:35-04:00",
            "duration_ms": 65_000,
            "script_duration_ms": null
        }]
    })
}

#[test]
fn chop_overrun_binding_returns_exact_plain_python_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_chop_overrun_wire_schema_version(),
            CHOP_OVERRUN_SCHEMA_VERSION
        );
        let request_obj =
            json_value_to_py(py, &healthy_chop_overrun_request_json()).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_classify_chop_overrun(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(
            value,
            json!({
                "schema_version": CHOP_OVERRUN_SCHEMA_VERSION,
                "level": "over",
                "sampled_runs": 1,
                "over_runs": 1,
                "worst_ratio": 65_000.0 / 60_000.0,
                "worst_blocking_ms": 65_000,
                "latest_ratio": 65_000.0 / 60_000.0,
                "run_ratios": [65_000.0 / 60_000.0],
            })
        );
        assert!(result.bind(py).downcast::<PyDict>().is_ok());
        let keys = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                "schema_version",
                "level",
                "sampled_runs",
                "over_runs",
                "worst_ratio",
                "worst_blocking_ms",
                "latest_ratio",
                "run_ratios",
            ]
        );
    });
}

#[test]
fn chop_overrun_binding_maps_schema_and_structural_errors_to_value_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut schema = healthy_chop_overrun_request_json();
        schema["schema_version"] = json!(CHOP_OVERRUN_SCHEMA_VERSION + 1);

        let mut structural = healthy_chop_overrun_request_json();
        structural["interval_seconds"] = json!(0);

        let mut unknown = healthy_chop_overrun_request_json();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("surprise".to_string(), json!(true));

        for (value, expected) in [
            (schema, "schema_version_mismatch"),
            (structural, "non_positive_interval"),
            (unknown, "unknown field `surprise`"),
        ] {
            let request_obj = json_value_to_py(py, &value).unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let error = py_classify_chop_overrun(py, request).unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(error.to_string().contains(expected), "{}", error);
        }
    });
}

fn healthy_axe_status_request_json() -> JsonValue {
    json!({
        "schema_version": 1,
        "generated_at": "2026-07-23T12:00:00-04:00",
        "desired_state": {
            "state": "running",
            "source": "binding-test",
            "timestamp": "2026-07-23T11:55:00-04:00"
        },
        "orchestrator": {
            "lifecycle_lock_held": true,
            "lock_holder": {"pid": 100, "live": true},
            "orchestrator_pid_file": {"pid": 100, "live": true},
            "legacy_pid_file": {"pid": null, "live": null}
        },
        "maintenance": null,
        "hook_runners": {"current": 1, "maximum": 3},
        "agent_runners": {"current": 2, "maximum": 4},
        "lumberjacks": [{
            "name": "hooks",
            "configured": true,
            "interval_seconds": 60,
            "configured_chops": ["zeta", "alpha", "zeta"],
            "recorded_pid": 200,
            "reported_state": "running",
            "process_live": true,
            "started_at": "2026-07-23T11:50:00-04:00",
            "start_age_seconds": 600,
            "heartbeat_at": "2026-07-23T11:59:30-04:00",
            "heartbeat_age_seconds": 30,
            "cycles_run": 10,
            "errors_encountered": 2,
            "uptime_seconds": 600
        }],
        "latest_lifecycle_event": {
            "event": "start",
            "timestamp": "2026-07-23T11:50:00-04:00",
            "source": "binding-test",
            "outcome": "started",
            "success": true,
            "reason": null,
            "orchestrator_pid": 100,
            "age_seconds": 600
        },
        "collection_error": null
    })
}

#[test]
fn axe_status_binding_returns_exact_plain_python_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_axe_status_wire_schema_version(),
            AXE_STATUS_SCHEMA_VERSION
        );
        let request_obj =
            json_value_to_py(py, &healthy_axe_status_request_json()).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_classify_axe_status(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(
            value,
            json!({
                "schema_version": 1,
                "generated_at": "2026-07-23T12:00:00-04:00",
                "state": "running",
                "health": "healthy",
                "summary": "AXE is running and healthy.",
                "exit_code": 0,
                "desired_state": {
                    "state": "running",
                    "source": "binding-test",
                    "timestamp": "2026-07-23T11:55:00-04:00"
                },
                "orchestrator": {
                    "state": "running",
                    "coherence": "coherent",
                    "live_pids": [100],
                    "lifecycle_lock_held": true,
                    "lock_holder": {"pid": 100, "live": true},
                    "orchestrator_pid_file": {"pid": 100, "live": true},
                    "legacy_pid_file": {"pid": null, "live": null}
                },
                "maintenance": null,
                "hook_runners": {"current": 1, "maximum": 3},
                "agent_runners": {"current": 2, "maximum": 4},
                "lumberjacks": [{
                    "name": "hooks",
                    "state": "running",
                    "stale_threshold_seconds": 180,
                    "configured": true,
                    "interval_seconds": 60,
                    "configured_chops": ["alpha", "zeta"],
                    "recorded_pid": 200,
                    "reported_state": "running",
                    "process_live": true,
                    "started_at": "2026-07-23T11:50:00-04:00",
                    "start_age_seconds": 600,
                    "heartbeat_at": "2026-07-23T11:59:30-04:00",
                    "heartbeat_age_seconds": 30,
                    "cycles_run": 10,
                    "errors_encountered": 2,
                    "uptime_seconds": 600
                }],
                "latest_lifecycle_event": {
                    "event": "start",
                    "timestamp": "2026-07-23T11:50:00-04:00",
                    "source": "binding-test",
                    "outcome": "started",
                    "success": true,
                    "reason": null,
                    "orchestrator_pid": 100,
                    "age_seconds": 600
                },
                "issues": [],
                "collection_error": null
            })
        );
        assert!(result.bind(py).downcast::<PyDict>().is_ok());
        let keys = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                "schema_version",
                "generated_at",
                "state",
                "health",
                "summary",
                "exit_code",
                "desired_state",
                "orchestrator",
                "maintenance",
                "hook_runners",
                "agent_runners",
                "lumberjacks",
                "latest_lifecycle_event",
                "issues",
                "collection_error",
            ]
        );
    });
}

#[test]
fn axe_status_public_projection_binding_preserves_user_values() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut request_json = healthy_axe_status_request_json();
        request_json["lumberjacks"][0]["name"] = json!("chop-watch");
        request_json["lumberjacks"][0]["configured_chops"] =
            json!(["chop-test"]);
        request_json["lumberjacks"][0]["heartbeat_age_seconds"] = json!(500);

        let request_obj = json_value_to_py(py, &request_json).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let snapshot = py_classify_axe_status(py, request).unwrap();
        let snapshot = snapshot.bind(py).downcast::<PyDict>().unwrap();
        let projected = py_project_axe_status_public(py, snapshot).unwrap();
        let value = py_to_json_value(projected.bind(py)).unwrap();

        assert_eq!(value["schema_version"], json!(2));
        assert_eq!(value["routines"][0]["name"], json!("chop-watch"));
        assert_eq!(value["routines"][0]["routine_name"], json!("chop-watch"));
        assert_eq!(
            value["routines"][0]["configured_jobs"],
            json!(["chop-test"])
        );
        assert_eq!(
            value["issues"][0]["code"],
            json!("routine_stale_heartbeat")
        );
        assert_eq!(value["issues"][0]["subject"], json!("chop-watch"));
        assert_eq!(
                value["issues"][0]["summary"],
                json!(
                    "Configured routine `chop-watch` has a stale heartbeat (500s; threshold 180s)."
                )
            );
        assert!(!value.to_string().contains("job-watch"));
        assert!(!value.to_string().contains("job-test"));
    });
}

#[test]
fn axe_status_binding_maps_schema_structural_and_unknown_errors_to_value_error()
{
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut schema = healthy_axe_status_request_json();
        schema["schema_version"] = json!(2);

        let mut structural = healthy_axe_status_request_json();
        structural["lumberjacks"][0]["interval_seconds"] = JsonValue::Null;

        let mut unknown = healthy_axe_status_request_json();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("surprise".to_string(), json!(true));

        for (value, expected) in [
            (schema, "schema_version_mismatch"),
            (structural, "missing_interval"),
            (unknown, "unknown field `surprise`"),
        ] {
            let request_obj = json_value_to_py(py, &value).unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let error = py_classify_axe_status(py, request).unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(error.to_string().contains(expected), "{}", error);
        }
    });
}

#[test]
fn chop_clan_contracts_round_trip_through_python_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let proposal_obj = json_value_to_py(
            py,
            &json!({
                "prompt": "Split the file.",
                "workspace": "git:sase",
                "agent_name": "split_file.src_lib.a1b2",
                "clan": "toobig-@",
                "clan_summary": "[bold]Large module[/bold]"
            }),
        )
        .unwrap();
        let proposal = proposal_obj.bind(py).downcast::<PyDict>().unwrap();
        let normalized =
            py_validate_chop_proposal(py, proposal, 0, None).unwrap();
        let normalized = py_to_json_value(normalized.bind(py)).unwrap();
        assert_eq!(normalized["clan"], json!("toobig-@"));
        assert_eq!(
            normalized["clan_summary"],
            json!("[bold]Large module[/bold]")
        );
        assert_eq!(normalized["agent_name"], json!("split_file.src_lib.a1b2"));

        let decision_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "inhibit_if": [{
                    "provider": "agent_clan",
                    "name_prefix": "toobig-"
                }],
                "agents": [{
                    "name": "toobig-0.split_file.src_lib.a1b2",
                    "agent_clan": "toobig-0",
                    "active": true
                }],
                "now": "2026-07-19T12:00:00Z"
            }),
        )
        .unwrap();
        let decision = decision_obj.bind(py).downcast::<PyDict>().unwrap();
        let evaluated = py_evaluate_chop_decision(py, decision).unwrap();
        let evaluated = py_to_json_value(evaluated.bind(py)).unwrap();
        assert_eq!(evaluated["outcome"], json!("skip"));
        assert_eq!(evaluated["provider"], json!("agent_clan"));

        let config_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "config": {"lumberjacks": {"guard": {"chops": {
                    "split": {"inhibit_if": {"agent_clan": {
                        "name_prefix": "toobig-"
                    }}}
                }}}}
            }),
        )
        .unwrap();
        let config = config_obj.bind(py).downcast::<PyDict>().unwrap();
        let diagnostics = py_validate_axe_config(py, config).unwrap();
        assert_eq!(py_to_json_value(diagnostics.bind(py)).unwrap(), json!([]));
    });
}

#[test]
fn chop_agent_runners_contracts_round_trip_through_python_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let decision_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "inhibit_if": [{
                    "provider": "agent_runners",
                    "max": 0
                }],
                "agents": [{
                    "name": "foo.cld",
                    "active": true,
                    "holds_runner_slot": true
                }],
                "now": "2026-08-12T12:00:00Z"
            }),
        )
        .unwrap();
        let decision = decision_obj.bind(py).downcast::<PyDict>().unwrap();
        let evaluated = py_evaluate_chop_decision(py, decision).unwrap();
        let evaluated = py_to_json_value(evaluated.bind(py)).unwrap();
        assert_eq!(evaluated["outcome"], json!("skip"));
        assert_eq!(evaluated["provider"], json!("agent_runners"));

        let config_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "config": {"lumberjacks": {"guard": {"chops": {
                    "watch": {"inhibit_if": {"agent_runners": {
                        "max": 0
                    }}}
                }}}}
            }),
        )
        .unwrap();
        let config = config_obj.bind(py).downcast::<PyDict>().unwrap();
        let diagnostics = py_validate_axe_config(py, config).unwrap();
        assert_eq!(py_to_json_value(diagnostics.bind(py)).unwrap(), json!([]));
    });
}

#[test]
fn required_axe_descriptions_round_trip_through_python_binding() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let config_obj = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "require_descriptions": true,
                "config": {"axe": {"lumberjacks": {"checks": {
                    "chops": {"hooks": {}}
                }}}}
            }),
        )
        .unwrap();
        let config = config_obj.bind(py).downcast::<PyDict>().unwrap();

        let diagnostics = py_validate_axe_config(py, config).unwrap();
        let diagnostics = py_to_json_value(diagnostics.bind(py)).unwrap();

        assert_eq!(diagnostics.as_array().unwrap().len(), 2);
        assert!(diagnostics.as_array().unwrap().iter().any(|item| {
            item["code"] == "required_missing"
                && item["path"] == "axe.lumberjacks.checks.description"
        }));
        assert!(diagnostics.as_array().unwrap().iter().any(|item| {
            item["code"] == "required_missing"
                && item["path"]
                    == "axe.lumberjacks.checks.chops.hooks.description"
        }));
    });
}

#[test]
fn axe_description_split_round_trips_through_python_binding() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let result = module
            .getattr("split_axe_description")
            .unwrap()
            .call1(("  Run checks  \r\n\r\nBody line  \r\n",))
            .unwrap()
            .extract::<(String, String)>()
            .unwrap();

        assert_eq!(result, ("Run checks".to_string(), "Body line".to_string()));
    });
}

#[test]
fn chop_subprocess_diagnostic_binding_returns_plain_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_normalize_chop_subprocess_diagnostic,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let request = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "run_id": "20260906T211142_996558",
                    "exit_code": -7,
                    "source_log_path": "/tmp/run.log",
                    "output": "\u{1b}[31mplain failure\u{1b}[0m\nhttps://api.telegram.org/bot123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi/getUpdates\ntelegram.error.TimedOut: Timed out",
                    "input_omitted_bytes": 4,
                    "had_decode_errors": true,
                    "max_lines": 2,
                    "max_bytes": 200
                }),
            )
            .unwrap();
        let value = module
            .getattr("normalize_chop_subprocess_diagnostic")
            .unwrap()
            .call1((request,))
            .unwrap();
        assert_eq!(
            py_to_json_value(&value).unwrap(),
            json!({
                "schema_version": 1,
                "run_id": "20260906T211142_996558",
                "exit_code": -7,
                "source_log_path": "/tmp/run.log",
                "output_status": "captured",
                "unavailable_reason": null,
                "output_excerpt": "https://api.telegram.org/bot<redacted>/getUpdates\ntelegram.error.TimedOut: Timed out",
                "truncated": true,
                "omitted_lines": 1,
                "omitted_bytes": 4,
                "had_decode_errors": true
            })
        );
    });
}
