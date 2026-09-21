use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn disk_inventory_binding_classifies_overlaps() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("disk_inventory_wire_schema_version").is_ok());
        assert!(module.getattr("classify_disk_inventory").is_ok());

        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": DISK_INVENTORY_WIRE_SCHEMA_VERSION,
                "rows": [
                    {
                        "section": "workspaces",
                        "name": "root",
                        "path": "/tmp/root",
                        "size_bytes": 100,
                        "owner": "workspace_cleanup_and_compact",
                        "horizon": "cleanup TTL 14 day(s)",
                    },
                    {
                        "section": "workspaces",
                        "name": "primary",
                        "path": "/tmp/root/primary",
                        "size_bytes": 40,
                        "owner": "workspace_git_object_source",
                        "horizon": "shared Git object source",
                    }
                ],
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_classify_disk_inventory(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(result["logical_total_bytes"], 140);
        assert_eq!(result["total_bytes"], 100);
        assert_eq!(
            result["rows"][1]["overlap_parent_path"].as_str(),
            Some("/tmp/root")
        );
    });
}

#[test]
fn disk_cleanup_outcome_binding_preserves_partial_effects() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module
            .getattr("disk_cleanup_outcome_wire_schema_version")
            .is_ok());
        assert!(module.getattr("normalize_disk_cleanup_outcome").is_ok());

        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION,
                "owners": [
                    {
                        "owner": "workspace",
                        "changed": true,
                        "reclaimed_bytes": 1024,
                        "exit_code": 0
                    },
                    {
                        "owner": "proc",
                        "exit_code": 2
                    }
                ]
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_normalize_disk_cleanup_outcome(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(result["status"], json!("failed"));
        assert_eq!(result["changed"], json!(true));
        assert_eq!(result["known_reclaimed_bytes"], json!(1024));
        assert_eq!(result["problems"][0]["kind"], json!("nonzero_exit"));
    });
}

#[test]
fn machine_setup_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "machine_setup_wire_schema_version",
            "classify_tailnet_health",
            "classify_tailnet_discovery",
            "reconcile_machine_enrollments",
            "assess_machine_init_review",
            "merge_machine_init_review",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        let version: u32 = module
            .getattr("machine_setup_wire_schema_version")
            .unwrap()
            .call0()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(version, MACHINE_SETUP_WIRE_SCHEMA_VERSION);

        let unrelated = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "alias": "web",
                "payload": {"status": "ok", "service": "unrelated"}
            }),
        )
        .unwrap();
        let unrelated = module
            .getattr("classify_tailnet_health")
            .unwrap()
            .call1((unrelated.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let unrelated = py_to_json_value(&unrelated).unwrap();
        assert_eq!(unrelated["compatibility"], json!("incompatible"));
        assert_eq!(
            unrelated["diagnostic"]["code"],
            json!("tailnet_probe_unrelated_service")
        );

        let legacy = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "alias": "old-gateway",
                "payload": {"status": "ok"}
            }),
        )
        .unwrap();
        let legacy = module
            .getattr("classify_tailnet_health")
            .unwrap()
            .call1((legacy.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let legacy = py_to_json_value(&legacy).unwrap();
        assert_eq!(legacy["compatibility"], json!("unknown"));
        assert_eq!(
            legacy["diagnostic"]["code"],
            json!("tailnet_probe_fleet_unknown")
        );

        let malformed_versions = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "alias": "bad",
                "payload": {
                    "status": "ok",
                    "fleet": {"supported_protocol_versions": [1, "x"]}
                }
            }),
        )
        .unwrap();
        let malformed_versions = module
            .getattr("classify_tailnet_health")
            .unwrap()
            .call1((malformed_versions.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let malformed_versions = py_to_json_value(&malformed_versions).unwrap();
        assert_eq!(
            malformed_versions["diagnostic"]["code"],
            json!("tailnet_probe_fleet_malformed")
        );

        let discovery = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "status": {
                    "Self": {
                        "ID": "self-node",
                        "DNSName": "athena.tail297af1.ts.net."
                    },
                    "Peer": {
                        "peer-apollo": {
                            "ID": "peer-apollo",
                            "DNSName": "apollo.tail297af1.ts.net.",
                            "HostName": "apollo",
                            "Online": true,
                            "OS": "linux"
                        }
                    }
                },
                "health_observations": [{
                    "endpoint": "https://apollo.tail297af1.ts.net",
                    "payload": {
                        "status": "ok",
                        "fleet": {"supported_protocol_versions": [1]}
                    }
                }]
            }),
        )
        .unwrap();
        let discovery = module
            .getattr("classify_tailnet_discovery")
            .unwrap()
            .call1((discovery.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let discovery = py_to_json_value(&discovery).unwrap();
        assert_eq!(
            discovery["candidates"][0]["endpoint"],
            json!("https://apollo.tail297af1.ts.net")
        );
        assert_eq!(discovery["candidates"][0]["installation_pin"], json!(""));
        assert_eq!(discovery["candidates"][0]["machine_selector"], json!(""));

        let reconcile = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "enrolled": [{
                    "alias": "apollo",
                    "provider_ref": "builtin@https",
                    "endpoint": "https://apollo.example.test",
                    "pinned_installation_id": format!(
                        "sase_inst_v1_{}",
                        "a".repeat(64)
                    )
                }],
                "candidates": [{
                    "provider_ref": "builtin@https",
                    "endpoint": "https://apollo.example.test",
                    "installation_pin": format!(
                        "sase_inst_v1_{}",
                        "b".repeat(64)
                    )
                }]
            }),
        )
        .unwrap();
        let reconcile = module
            .getattr("reconcile_machine_enrollments")
            .unwrap()
            .call1((reconcile.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let reconcile = py_to_json_value(&reconcile).unwrap();
        assert_eq!(reconcile["items"][0]["status"], json!("repair"));

        let merge = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "presented_candidates": [{
                    "provider_ref": "builtin@https",
                    "endpoint": "https://fleet.example.test",
                    "installation_pin": format!(
                        "sase_inst_v1_{}",
                        "c".repeat(64)
                    ),
                    "display_name": "ignored"
                }]
            }),
        )
        .unwrap();
        let merge = module
            .getattr("merge_machine_init_review")
            .unwrap()
            .call1((merge.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let merged = py_to_json_value(&merge).unwrap();
        assert_eq!(merged["initial_review_completed"], json!(true));
        assert_eq!(
            merged["reviewed"][0]["endpoint"],
            json!("https://fleet.example.test")
        );

        let assess = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "state": merged,
                "candidates": [{
                    "provider_ref": "builtin@https",
                    "endpoint": "https://fleet.example.test",
                    "installation_pin": format!(
                        "sase_inst_v1_{}",
                        "c".repeat(64)
                    )
                }, {
                    "provider_ref": "builtin@https",
                    "endpoint": "https://new.example.test",
                    "installation_pin": ""
                }],
                "enrolled": []
            }),
        )
        .unwrap();
        let assess = module
            .getattr("assess_machine_init_review")
            .unwrap()
            .call1((assess.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let assess = py_to_json_value(&assess).unwrap();
        assert_eq!(assess["offer_enrollment"], json!(false));
        assert_eq!(assess["initial_review_required"], json!(false));
        assert_eq!(assess["unreviewed_candidates"], json!([]));

        let bad_schema = json_value_to_py(
            py,
            &json!({"schema_version": 9, "payload": {"status": "ok"}}),
        )
        .unwrap();
        let err = module
            .getattr("classify_tailnet_health")
            .unwrap()
            .call1((bad_schema.bind(py).downcast::<PyDict>().unwrap(),));
        assert!(err.is_err());

        let not_object = json_value_to_py(py, &json!([1, 2, 3])).unwrap();
        let err = module
            .getattr("classify_tailnet_discovery")
            .unwrap()
            .call1((not_object.bind(py),));
        assert!(err.is_err());
    });
}

#[test]
fn feature_flag_state_bindings_are_exported_and_round_trip() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "feature_flag_state_wire_schema_version",
            "feature_flag_state_get",
            "feature_flag_state_set",
            "feature_flag_state_reconcile",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(
            py_feature_flag_state_wire_schema_version(),
            sase_core::FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION
        );

        let missing = py_feature_flag_state_get(py, &home).unwrap();
        let missing_value = py_to_json_value(missing.bind(py)).unwrap();
        assert_eq!(missing_value["version"], json!(1));
        assert_eq!(missing_value["flags"], json!({}));
        assert_eq!(missing_value["diagnostics"], json!([]));

        let first =
            py_feature_flag_state_set(py, &home, "prettier_enabled", false)
                .unwrap();
        let first_value = py_to_json_value(first.bind(py)).unwrap();
        assert_eq!(first_value["flag"], json!("prettier_enabled"));
        assert_eq!(first_value["enabled"], json!(false));
        assert_eq!(first_value["previous"], json!(null));
        assert_eq!(first_value["changed"], json!(true));

        let second =
            py_feature_flag_state_set(py, &home, "epic_resume_gate", true)
                .unwrap();
        let second_value = py_to_json_value(second.bind(py)).unwrap();
        assert_eq!(
            second_value["flags"],
            json!({
                "epic_resume_gate": true,
                "prettier_enabled": false
            })
        );

        let loaded = py_feature_flag_state_get(py, &home).unwrap();
        let loaded_value = py_to_json_value(loaded.bind(py)).unwrap();
        assert_eq!(loaded_value["flags"], second_value["flags"]);

        let again =
            py_feature_flag_state_set(py, &home, "epic_resume_gate", true)
                .unwrap();
        let again_value = py_to_json_value(again.bind(py)).unwrap();
        assert_eq!(again_value["previous"], json!(true));
        assert_eq!(again_value["changed"], json!(false));

        let reconciled = py_feature_flag_state_reconcile(
            py,
            &home,
            vec!["epic_resume_gate".to_string()],
        )
        .unwrap();
        let reconciled_value = py_to_json_value(reconciled.bind(py)).unwrap();
        assert_eq!(reconciled_value["status"], json!("cleaned"));
        assert_eq!(reconciled_value["removed"], json!(["prettier_enabled"]));
        assert_eq!(
            reconciled_value["flags"],
            json!({"epic_resume_gate": true})
        );
    });
}

#[test]
fn feature_flag_state_binding_rejects_invalid_keys_and_corrupt_files() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let path = temp.path().join("feature_flags.json");
    Python::with_gil(|py| {
        let error =
            py_feature_flag_state_set(py, &home, "NotSnake", true).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        std::fs::write(&path, "not json").unwrap();
        let snapshot = py_feature_flag_state_get(py, &home).unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["flags"], json!({}));
        assert_eq!(
            snapshot_value["diagnostics"][0]["code"],
            json!("malformed_json")
        );
        let error =
            py_feature_flag_state_set(py, &home, "epic_resume_gate", true)
                .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        assert!(error.to_string().contains("left unchanged"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "not json");
    });
}

#[test]
fn config_routine_job_projection_round_trips_through_python_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let schema = json!({
            "type": "object",
            "properties": {
                "axe": {
                    "type": "object",
                    "properties": {
                        "routines": {
                            "type": "object",
                            "properties": {
                                "checks": {
                                    "type": "object",
                                    "properties": {
                                        "interval": {"type": "integer"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        let layers = json!([
            {
                "name": "defaults",
                "kind": "default",
                "path": "defaults.yml",
                "list_strategy": "replace",
                "writable": false,
                "value": {"axe": {"lumberjacks": {"checks": {"interval": 5}}}}
            },
            {
                "name": "user",
                "kind": "user",
                "path": "user.yml",
                "list_strategy": "replace",
                "writable": true,
                "value": {"axe": {"routines": {"checks": {"interval": 19}}}}
            }
        ]);
        let inventory_req = json!({
            "schema": schema.clone(),
            "layers": layers.clone(),
            "routine_job_contract": true
        });
        let inventory_obj = json_value_to_py(py, &inventory_req).unwrap();
        let inventory_req =
            inventory_obj.bind(py).downcast::<PyDict>().unwrap();
        let inventory = py_config_inventory(py, inventory_req).unwrap();
        let inventory = py_to_json_value(inventory.bind(py)).unwrap();
        let interval = inventory["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|field| field["path"] == "axe.routines.checks.interval")
            .unwrap();
        assert_eq!(interval["effective_value"], json!(19));
        assert_eq!(interval["contributions"][0]["raw_value"], json!(5));
        assert_eq!(interval["contributions"][1]["raw_value"], json!(19));

        let edit_req = json!({
            "schema": schema,
            "layers": [{
                "name": "user",
                "kind": "user",
                "path": "user.yml",
                "list_strategy": "replace",
                "writable": true,
                "value": {"axe": {"lumberjacks": {"checks": {"interval": 5}}}}
            }],
            "target_layer": "user",
            "path": "axe.routines.checks.interval",
            "op": {"kind": "set", "value": 19},
            "routine_job_contract": true
        });
        let edit_obj = json_value_to_py(py, &edit_req).unwrap();
        let edit_req = edit_obj.bind(py).downcast::<PyDict>().unwrap();
        let plan = py_config_plan_edit(py, edit_req).unwrap();
        let plan = py_to_json_value(plan.bind(py)).unwrap();
        assert_eq!(
            plan["write_plan"]["key_path"],
            json!(["axe", "lumberjacks", "checks", "interval"])
        );
        assert_eq!(
            plan["candidate_config"]["axe"]["routines"]["checks"]["interval"],
            json!(19)
        );
    });
}

#[test]
fn service_config_compose_binding_round_trips_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = json!({
            "layers": [
                {
                    "name": "default",
                    "kind": "builtin",
                    "path": null,
                    "list_strategy": "concatenate",
                    "writable": false,
                    "value": {"service": {"procs": {
                        "scheduler": {"builtin": "scheduler"},
                        "gateway": {"builtin": "gateway", "enabled": false}
                    }}}
                },
                {
                    "name": "user",
                    "kind": "user",
                    "path": "/home/u/sase.yml",
                    "list_strategy": "concatenate",
                    "writable": true,
                    "value": {"service": {"procs": {
                        "gateway": {"enabled": true}
                    }}}
                }
            ]
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

        let result = py_service_config_compose(py, request).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(result["fatal"], json!(false));
        let procs = result["procs"].as_array().unwrap();
        assert_eq!(procs.len(), 2);
        let gateway = procs
            .iter()
            .find(|entry| entry["name"] == "gateway")
            .unwrap();
        assert_eq!(gateway["enabled"], json!(true));
        assert_eq!(gateway["enablement"]["explicit"], json!(true));
        assert_eq!(
            gateway["enablement"]["layer"],
            json!("user:/home/u/sase.yml")
        );
        assert_eq!(
            gateway["launcher"],
            json!({"kind": "builtin", "builtin": "gateway"})
        );
        let scheduler = procs
            .iter()
            .find(|entry| entry["name"] == "scheduler")
            .unwrap();
        assert_eq!(scheduler["source"], json!("builtin"));
        assert_eq!(scheduler["enabled"], json!(true));
        assert_eq!(scheduler["available"], json!(true));
    });
}

#[test]
fn service_restart_decide_binding_round_trips_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let request = json!({
            "policy": "on-failure",
            "success_exit_codes": [75],
            "exit": {"signal": 9},
            "history": {
                "started_at": 1.0,
                "backoff_seconds": 2.0,
                "consecutive_failures": 1,
                "recent_failures": [3.0],
                "alert_sent": false
            },
            "now": 4.0,
            "tuning": {}
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let result = module
            .getattr("service_restart_decide")
            .unwrap()
            .call1((request_obj,))
            .unwrap();
        let result = py_to_json_value(&result).unwrap();

        assert_eq!(result["action"], json!("restart"));
        assert_eq!(result["delay_seconds"], json!(4.0));
        assert_eq!(
            result["reason"],
            json!("killed by SIGKILL; retrying in 4s")
        );
    });
}

#[test]
fn service_state_bindings_round_trip_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mutation = json_value_to_py(
            py,
            &json!({
                "op": "set_enablement",
                "name": "scheduler",
                "enabled": true,
                "actor": "pytest"
            }),
        )
        .unwrap();

        let outcome = module
            .getattr("service_state_mutate")
            .unwrap()
            .call1((
                temp.path().to_string_lossy().as_ref(),
                mutation,
                "boot-a",
                12.0,
            ))
            .unwrap();
        let outcome = py_to_json_value(&outcome).unwrap();
        assert_eq!(outcome["changed"], json!(true));
        assert_eq!(
            outcome["snapshot"]["state"]["enablement"]["scheduler"]["enabled"],
            json!(true)
        );

        let snapshot = module
            .getattr("service_state_read")
            .unwrap()
            .call1((temp.path().to_string_lossy().as_ref(), "boot-a"))
            .unwrap();
        let snapshot = py_to_json_value(&snapshot).unwrap();
        assert_eq!(
            snapshot["state"]["enablement"]["scheduler"]["updated_by"],
            json!("pytest")
        );
    });
}

#[test]
fn service_status_bindings_round_trip_python_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let entry = json!({
            "name": "scheduler",
            "description": "Scheduler",
            "available": true,
            "unavailable_reasons": [],
            "source": "builtin",
            "declared_by": "default",
            "enabled": true,
            "enablement": {"explicit": false},
            "mode": "daemon",
            "launcher": {"kind": "builtin", "builtin": "scheduler"},
            "env": {},
            "restart": "on-failure",
            "success_exit_codes": [],
            "stop_signal": "SIGTERM",
            "stop_timeout_seconds": 10.0,
            "after": [],
            "log_max_bytes": 4096,
            "field_provenance": []
        });
        let override_value = json_value_to_py(
            py,
            &json!({
                "enabled": false,
                "updated_at": 10.0,
                "updated_by": "pytest"
            }),
        )
        .unwrap();
        let resolved = module
            .getattr("service_enablement_resolve")
            .unwrap()
            .call1((json_value_to_py(py, &entry).unwrap(), override_value))
            .unwrap();
        let resolved = py_to_json_value(&resolved).unwrap();
        assert_eq!(resolved["summary"], json!("disabled here"));

        let request = json!({
            "generated_at": 20.0,
            "boot_id": "boot-a",
            "host": {
                "record": {
                    "pid": 42,
                    "boot_id": "boot-a",
                    "started_at": 1.0,
                    "heartbeat_at": 19.0,
                    "mode": "foreground",
                    "sase_version": "0.test"
                },
                "lock_held": false,
                "pid_alive": true,
                "platform_unit": "sase.service",
                "stale_after_seconds": 15.0
            },
            "config": {
                "schema_version": 1,
                "fatal": false,
                "procs": [entry],
                "diagnostics": [],
                "ignored_layers": []
            },
            "state": {
                "schema_version": 1,
                "enablement": {},
                "stops": {},
                "markers": {},
                "host": null
            },
            "procs": [{
                "name": "scheduler",
                "pid": 123,
                "alive": true,
                "proc_id": "proc-1",
                "started_at": 11.0,
                "restarts": 0
            }]
        });
        let snapshot = module
            .getattr("service_status_build")
            .unwrap()
            .call1((json_value_to_py(py, &request).unwrap(),))
            .unwrap();
        let snapshot_json = py_to_json_value(&snapshot).unwrap();
        assert_eq!(snapshot_json["procs"][0]["state"], json!("running"));
        assert_eq!(
            snapshot_json["procs"][0]["summary"],
            json!("running · pid 123")
        );
        assert_eq!(snapshot_json["change_token"].as_str().unwrap().len(), 64);

        let mut mismatched_stop_request = request.clone();
        mismatched_stop_request["state"]["stops"] = json!({
            "scheduler": {
                "boot_id": "boot-b",
                "stopped_at": 12.0,
                "stopped_by": "pytest"
            }
        });
        mismatched_stop_request["procs"] = json!([]);
        let mismatched_stop_snapshot = module
            .getattr("service_status_build")
            .unwrap()
            .call1((json_value_to_py(py, &mismatched_stop_request).unwrap(),))
            .unwrap();
        let mismatched_stop_json =
            py_to_json_value(&mismatched_stop_snapshot).unwrap();
        assert_eq!(
            mismatched_stop_json["procs"][0]["desired"],
            json!("running")
        );
        assert_eq!(
            mismatched_stop_json["procs"][0]["summary"],
            json!("stopped")
        );
        assert!(
            mismatched_stop_json["procs"][0].get("stop").is_none(),
            "mismatched boot stop must not be emitted"
        );

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("status.json");
        module
            .getattr("service_status_write")
            .unwrap()
            .call1((path.to_string_lossy().as_ref(), snapshot))
            .unwrap();
        let read = module
            .getattr("service_status_read")
            .unwrap()
            .call1((path.to_string_lossy().as_ref(),))
            .unwrap();
        let read = py_to_json_value(&read).unwrap();
        assert_eq!(read["procs"][0]["name"], json!("scheduler"));

        let missing = module
            .getattr("service_status_read")
            .unwrap()
            .call1((temp
                .path()
                .join("missing.json")
                .to_string_lossy()
                .as_ref(),))
            .unwrap();
        assert!(missing.is_none());
    });
}
