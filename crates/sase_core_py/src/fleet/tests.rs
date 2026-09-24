use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn fleet_contract_bindings_round_trip_nested_dicts() {
    use serde_json::json;

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(
            py_fleet_contract_schema_version(),
            core_fleet_contract::FLEET_CONTRACT_SCHEMA_VERSION
        );
        let missing = py_fleet_installation_identity_load(
            py,
            home.path().to_str().unwrap(),
        )
        .unwrap();
        let missing = py_to_json_value(missing.bind(py)).unwrap();
        assert!(missing["record"].is_null());

        let ensured = py_fleet_installation_identity_ensure(
            py,
            home.path().to_str().unwrap(),
        )
        .unwrap();
        let ensured = py_to_json_value(ensured.bind(py)).unwrap();
        assert_eq!(ensured["created"], json!(true));
        let installation_id =
            ensured["record"]["installation_id"].as_str().unwrap();

        let origin = json!({
            "schema_version": 1,
            "installation_id": installation_id,
        });
        let project = json!({
            "schema_version": 1,
            "origin": origin,
            "project_id": "sase-main",
        });
        let logical = json!({
            "schema_version": 1,
            "project": project,
            "agent_id": "agent-1",
            "family_id": "family-1",
        });
        let exact = json!({
            "schema_version": 1,
            "logical": logical,
            "shell_id": "shell-1",
            "run_id": "run-1",
            "attempt_id": "attempt-1",
        });
        let logical_dict =
            json_value_to_py(py, &logical).unwrap().into_bound(py);
        let logical_dict = logical_dict.downcast::<PyDict>().unwrap();
        let logical_key = py_fleet_logical_locator_key(logical_dict).unwrap();
        let exact_dict = json_value_to_py(py, &exact).unwrap().into_bound(py);
        let exact_dict = exact_dict.downcast::<PyDict>().unwrap();
        assert!(py_fleet_instance_locator_key(exact_dict)
            .unwrap()
            .starts_with(&logical_key));

        let owner = json!({
            "schema_version": 1,
            "logical_locator": logical,
            "owner_username": "bryan",
            "owner_machine_name": "athena",
            "display_name": "athena.agent-1",
            "display_alias": "agent-1",
        });
        let owner = json_value_to_py(py, &owner).unwrap().into_bound(py);
        let owner = owner.downcast::<PyDict>().unwrap();
        assert_eq!(
            py_to_json_value(
                py_fleet_associate_owner_display_name(py, owner)
                    .unwrap()
                    .bind(py),
            )
            .unwrap()["owner_label"],
            json!("bryan.athena")
        );

        let revision = json!({
            "schema_version": 1,
            "logical_key": logical_key,
            "revision": 7,
        });
        let handle = json!({
            "schema_version": 1,
            "id": "transcript-1",
            "kind": "transcript",
            "revision": revision,
            "digest": "a".repeat(64),
            "byte_len": 1024,
            "supports_range": true,
            "supports_growth": true,
        });
        let request = json!({
            "schema_version": 1,
            "record": {
                "project_name": "SASE",
                "project_dir": "/tmp/project",
                "project_file": "/tmp/project.sase",
                "workflow_dir_name": "ace-run",
                "artifact_dir": "/tmp/artifacts/20260906120000",
                "timestamp": "20260906120000",
                "agent_meta": {
                    "name": "athena.agent-1",
                    "model": "gpt-5",
                    "llm_provider": "codex",
                    "agent_family": "family-1",
                    "queue_capacity": 100,
                    "queue_capacity_explicit": true
                },
                "running": {
                    "pid": 1234,
                    "model": "gpt-5",
                    "llm_provider": "codex",
                    "workspace_dir": "/tmp/ws"
                },
                "raw_prompt_snippet": "Implement the approved plan",
                "has_done_marker": false
            },
            "logical_locator": logical,
            "owner_facts": {
                "schema_version": 1,
                "exact_locator": exact,
                "row_revision": revision,
                "liveness": "alive",
                "connection_health": "online",
                "freshness": "fresh",
                "observed_at_unix": 10.0,
                "started_at_unix": 8.5,
                "run_started_at_unix": 9.25,
                "stopped_at_unix": null,
                "workspace_num": 17,
                "project_label": "sase",
                "agent_clan": "fleet",
                "agent_clan_generation": "20260913",
                "clan_tribe": "parity",
                "tribe": "review",
                "row_kind": "agent_shell",
                "current_instance": true,
                "dismissable": false,
                "needs_attention": false,
                "occupied_runner_slot": true,
                "container_projected_concrete_agent": false,
                "capabilities": {
                    "schema_version": 1,
                    "resource": ["stop", "content.read"],
                    "host": [],
                    "protocol": []
                },
                "content_handles": [handle]
            }
        });
        let request = json_value_to_py(py, &request).unwrap().into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        let summary =
            py_fleet_project_resolved_agent_summary(py, request).unwrap();
        let summary_value = py_to_json_value(summary.bind(py)).unwrap();
        assert_eq!(
            summary_value["schema_version"],
            json!(core_fleet_contract::FLEET_CONTRACT_SCHEMA_VERSION)
        );
        assert_eq!(summary_value["lifecycle"], json!("running"));
        assert_eq!(summary_value["labels"]["project_label"], json!("sase"));
        assert_eq!(summary_value["started_at_unix"], json!(8.5));
        assert_eq!(summary_value["run_started_at_unix"], json!(9.25));
        assert_eq!(summary_value["workspace_num"], json!(17));
        assert_eq!(summary_value["agent_clan"], json!("fleet"));
        assert_eq!(summary_value["agent_clan_generation"], json!("20260913"));
        assert_eq!(summary_value["clan_tribe"], json!("parity"));
        assert_eq!(summary_value["tribe"], json!("review"));
        assert_eq!(summary_value["queue_capacity"], json!(100));
        assert_eq!(summary_value["queue_capacity_explicit"], json!(true));
        assert_eq!(summary_value["content"]["handle_count"], json!(1));
        let summary_dict = summary.bind(py).downcast::<PyDict>().unwrap();
        let validated =
            py_fleet_validate_resolved_agent_summary(py, summary_dict).unwrap();
        assert_eq!(
            py_to_json_value(validated.bind(py)).unwrap(),
            summary_value
        );

        let detail =
            py_fleet_project_resolved_agent_detail(py, request).unwrap();
        assert_eq!(
            py_to_json_value(detail.bind(py)).unwrap()["content_handles"][0]
                ["id"],
            json!("transcript-1")
        );
        let counts_req = json!({
            "schema_version": 1,
            "summaries": [summary_value],
        });
        let counts_req =
            json_value_to_py(py, &counts_req).unwrap().into_bound(py);
        let counts_req = counts_req.downcast::<PyDict>().unwrap();
        let counts = py_fleet_count_logical_agents(py, counts_req).unwrap();
        assert_eq!(
            py_to_json_value(counts.bind(py)).unwrap()["running"],
            json!(1)
        );

        let catalog_query = json!({
            "schema_version": 1,
            "scope": "presentation",
            "snapshot_id": null,
            "cursor": null,
            "limit": 50,
            "project_ids": [],
            "query": null,
            "status_buckets": [],
            "include_terminal": true
        });
        let catalog_query =
            json_value_to_py(py, &catalog_query).unwrap().into_bound(py);
        let catalog_query = catalog_query.downcast::<PyDict>().unwrap();
        assert_eq!(
            py_to_json_value(
                py_fleet_validate_catalog_query(py, catalog_query)
                    .unwrap()
                    .bind(py)
            )
            .unwrap()["limit"],
            json!(50)
        );
        let summary_for_snapshot: ResolvedAgentSummaryWire =
            serde_json::from_value(summary_value.clone()).unwrap();
        let snapshot_id = core_fleet_contract::fleet_catalog_snapshot_id(
            FleetCatalogScopeWire::Presentation,
            &[summary_for_snapshot],
        )
        .unwrap();
        let summaries_list =
            json_value_to_py(py, &json!([summary_value.clone()]))
                .unwrap()
                .into_bound(py);
        let summaries_list = summaries_list.downcast::<PyList>().unwrap();
        assert_eq!(
            py_fleet_catalog_snapshot_id("presentation", summaries_list)
                .unwrap(),
            snapshot_id
        );
        let catalog_cursor = format!("catcur_v1:p:{snapshot_id}:50");
        assert_eq!(
            py_fleet_validate_catalog_cursor(&catalog_cursor).unwrap(),
            catalog_cursor
        );

        let freshness = json!({
            "schema_version": 1,
            "freshness": "fresh",
            "partial": false,
            "refreshed_at_unix": 11.0,
            "error": null
        });
        let freshness_obj =
            json_value_to_py(py, &freshness).unwrap().into_bound(py);
        let freshness_dict = freshness_obj.downcast::<PyDict>().unwrap();
        assert_eq!(
            py_to_json_value(
                py_fleet_validate_snapshot_freshness(py, freshness_dict)
                    .unwrap()
                    .bind(py)
            )
            .unwrap()["refreshed_at_unix"],
            json!(11.0)
        );

        let authoritative_counts = json!({
            "schema_version": 1,
            "basis": {
                "schema_version": 1,
                "input_rows": 3,
                "selected_rows": 3,
                "max_revision": 3,
                "observed_at_unix_max": 12.0
            },
            "logical_agent_total": 3,
            "running": 3,
            "waiting": 0,
            "attention": 0,
            "occupied_runner_slots": 3
        });
        let accumulation_req = json!({
            "schema_version": 1,
            "current": null,
            "request_generation": 1,
            "requested_scope": "presentation",
            "requested_snapshot_id": null,
            "requested_cursor": null,
            "incoming": {
                "schema_version": 1,
                "cursor": {
                    "schema_version": 1,
                    "store_generation": "gen-apollo",
                    "sequence": 12
                },
                "counts": authoritative_counts.clone(),
                "count_revision": 3,
                "freshness": freshness.clone(),
                "page": {
                    "schema_version": 1,
                    "scope": "presentation",
                    "snapshot_id": snapshot_id.clone(),
                    "rows": [summary_value.clone()],
                    "limit": 50,
                    "total_matching_rows": 1,
                    "next_cursor": null,
                    "has_more": false,
                    "state": "finished"
                }
            }
        });
        let accumulation_req = json_value_to_py(py, &accumulation_req)
            .unwrap()
            .into_bound(py);
        let accumulation_req = accumulation_req.downcast::<PyDict>().unwrap();
        let accumulation =
            py_fleet_accumulate_catalog_page(py, accumulation_req).unwrap();
        let accumulation = py_to_json_value(accumulation.bind(py)).unwrap();
        assert_eq!(accumulation["action"], json!("replaced"));
        assert_eq!(accumulation["state"]["rows"].as_array().unwrap().len(), 1);
        assert_eq!(
            accumulation["state"]["snapshot_id"],
            json!(snapshot_id.clone())
        );
        let federation_response = json!({
            "schema_version": 1,
            "operation": "catalog",
            "configured_hosts": 1,
            "hosts": [{
                "schema_version": 1,
                "alias": "apollo",
                "provider_ref": "apollo-provider",
                "installation_id": installation_id,
                "endpoint": "https://apollo.example.test",
                "status": "ok",
                "cached": false,
                "age_seconds": null,
                "payload": {
                    "schema_version": 1,
                    "cursor": {
                        "schema_version": 1,
                        "store_generation": "gen-apollo",
                        "sequence": 12
                    },
                    "catalog_scope": "presentation",
                    "catalog_snapshot_id": snapshot_id.clone(),
                    "counts": authoritative_counts,
                    "freshness": freshness,
                    "page": {
                        "schema_version": 1,
                        "scope": "presentation",
                        "snapshot_id": snapshot_id.clone(),
                        "rows": [summary_value.clone()],
                        "limit": 50,
                        "total_matching_rows": 3,
                        "next_cursor": format!("catcur_v1:p:{snapshot_id}:50"),
                        "has_more": true,
                        "state": "ready"
                    }
                },
                "error": null
            }]
        });
        let normalize_req = json!({
            "schema_version": 1,
            "response": federation_response.clone()
        });
        let normalize_req =
            json_value_to_py(py, &normalize_req).unwrap().into_bound(py);
        let normalize_req = normalize_req.downcast::<PyDict>().unwrap();
        let normalized =
            py_fleet_normalize_federation_response(py, normalize_req).unwrap();
        let normalized = py_to_json_value(normalized.bind(py)).unwrap();
        assert_eq!(normalized["hosts"][0]["alias"], json!("apollo"));
        assert_eq!(
            normalized["hosts"][0]["catalog"]["next_cursor"],
            json!(format!("catcur_v1:p:{snapshot_id}:50"))
        );
        assert_eq!(
            normalized["hosts"][0]["authoritative_counts"]["running"],
            json!(3)
        );

        let federation_count_req = json!({
            "schema_version": 1,
            "local_summaries": [],
            "followed_response": null,
            "fleet_response": federation_response
        });
        let federation_count_req = json_value_to_py(py, &federation_count_req)
            .unwrap()
            .into_bound(py);
        let federation_count_req =
            federation_count_req.downcast::<PyDict>().unwrap();
        let counted = py_fleet_count_focus_and_fleet_from_federation(
            py,
            federation_count_req,
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(counted.bind(py)).unwrap()["fleet"]["counts"]
                ["running"],
            json!(3)
        );

        let cursor_req = json!({
            "schema_version": 1,
            "cursor": {
                "schema_version": 1,
                "store_generation": "gen-1",
                "sequence": 4
            },
            "current_generation": "gen-1",
            "newest_sequence": 5,
            "oldest_replayable_sequence": 5,
            "deletion_history_complete": true
        });
        let cursor_req =
            json_value_to_py(py, &cursor_req).unwrap().into_bound(py);
        let cursor_req = cursor_req.downcast::<PyDict>().unwrap();
        let cursor = py_fleet_classify_cursor_replay(py, cursor_req).unwrap();
        assert_eq!(
            py_to_json_value(cursor.bind(py)).unwrap()["classification"],
            json!("replayable")
        );

        let fingerprint_req = json!({
            "schema_version": 1,
            "payload": {"b": 2, "a": 1},
        });
        let fingerprint_req = json_value_to_py(py, &fingerprint_req)
            .unwrap()
            .into_bound(py);
        let fingerprint_req = fingerprint_req.downcast::<PyDict>().unwrap();
        let fingerprint =
            py_fleet_operation_payload_fingerprint(py, fingerprint_req)
                .unwrap();
        let fingerprint_value = py_to_json_value(fingerprint.bind(py)).unwrap();
        assert_eq!(fingerprint_value["sha256"].as_str().unwrap().len(), 64);

        let op_req = json!({
            "schema_version": 1,
            "key": {
                "schema_version": 1,
                "controller_id": "controller-a",
                "operation_id": "op-1"
            },
            "payload_fingerprint": fingerprint_value,
            "target": exact,
            "resource_revision": revision,
            "now_unix": 10.0,
            "acceptance_window_seconds": 5.0,
            "existing_record": null
        });
        let op_req = json_value_to_py(py, &op_req).unwrap().into_bound(py);
        let op_req = op_req.downcast::<PyDict>().unwrap();
        let decision = py_fleet_decide_operation_replay(py, op_req).unwrap();
        assert_eq!(
            py_to_json_value(decision.bind(py)).unwrap()["decision"],
            json!("accept_new")
        );

        let plan = json!({
            "schema_version": 1,
            "provider_ref": "provider-a",
            "endpoint": "https://fleet.example.test/api",
            "credential_ref": "cred-main",
            "pinned_installation_id": installation_id,
            "connection_kind": "gateway",
            "tls": {
                "schema_version": 1,
                "mode": "system_roots",
                "ca_ref": null,
                "server_name_ref": null
            }
        });
        let plan = json_value_to_py(py, &plan).unwrap().into_bound(py);
        let plan = plan.downcast::<PyDict>().unwrap();
        assert!(py_fleet_validate_connection_plan(py, plan).is_ok());

        let duration_req = json!({
            "schema_version": 1,
            "owner_started_at_unix": 1.0,
            "owner_stopped_at_unix": null,
            "owner_observed_at_unix": 4.5,
            "max_clock_anomaly_seconds": 1.0
        });
        let duration_req =
            json_value_to_py(py, &duration_req).unwrap().into_bound(py);
        let duration_req = duration_req.downcast::<PyDict>().unwrap();
        assert_eq!(
            py_to_json_value(
                py_fleet_classify_runtime_duration(py, duration_req)
                    .unwrap()
                    .bind(py)
            )
            .unwrap()["elapsed_seconds"],
            json!(3.5)
        );
        let freshness_req = json!({
            "schema_version": 1,
            "viewer_monotonic_elapsed_seconds": null,
            "fresh_threshold_seconds": 3.0,
            "stale_threshold_seconds": 10.0
        });
        let freshness_req =
            json_value_to_py(py, &freshness_req).unwrap().into_bound(py);
        let freshness_req = freshness_req.downcast::<PyDict>().unwrap();
        assert_eq!(
            py_to_json_value(
                py_fleet_classify_cache_freshness(py, freshness_req)
                    .unwrap()
                    .bind(py)
            )
            .unwrap()["freshness"],
            json!("unknown")
        );
    });
}

#[test]
fn gateway_and_bootstrap_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        assert!(module.getattr("gateway_main").unwrap().is_callable());
        assert!(module
            .getattr("fleet_issue_bootstrap")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_validate_catalog_query")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_validate_catalog_cursor")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_catalog_snapshot_id")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_accumulate_catalog_page")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_validate_snapshot_freshness")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_normalize_federation_response")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_count_focus_and_fleet_from_federation")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_project_attention_inventory")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_validate_attention_inventory_request")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("fleet_validate_attention_inventory_response")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("sudo_validate_manifest")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("sudo_manifest_sha256")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("sudo_derive_risk_badges")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("sudo_validate_ledger")
            .unwrap()
            .is_callable());
        assert!(module
            .getattr("sudo_validate_handshake")
            .unwrap()
            .is_callable());
        assert!(module.getattr("sudo_runner_main").unwrap().is_callable());
    });
}

#[test]
fn fleet_issue_bootstrap_binding_delegates_to_store_without_persisting_secret()
{
    use serde_json::json;

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let home = tempfile::tempdir().unwrap();
        let request = json!({
            "schema_version": 1,
            "requested_scopes": [
                " fleet.summary.read ",
                "fleet.hello",
                "fleet.hello"
            ],
            "supported_protocol_versions": [99, 1],
            "expires_at_unix": null,
            "installation_pin": null
        });
        let request_obj =
            json_value_to_py(py, &request).unwrap().into_bound(py);
        let request_dict = request_obj.downcast::<PyDict>().unwrap();

        let before = sase_gateway::current_unix_time();
        let response = py_fleet_issue_bootstrap(
            py,
            home.path().to_str().unwrap(),
            request_dict,
        )
        .unwrap();
        let after = sase_gateway::current_unix_time();
        let response = py_to_json_value(response.bind(py)).unwrap();

        assert_eq!(
            response["schema_version"],
            json!(sase_gateway::FLEET_API_WIRE_SCHEMA_VERSION)
        );
        assert!(response["bootstrap_id"]
            .as_str()
            .unwrap()
            .starts_with("boot_"));
        let secret = response["bootstrap_secret"].as_str().unwrap();
        assert!(secret.starts_with("sase_bootstrap_"));
        assert_eq!(
            response["allowed_scopes"],
            json!(["fleet.hello", "fleet.summary.read"])
        );
        assert_eq!(
            response["protocol_versions"],
            json!([sase_gateway::FLEET_PROTOCOL_VERSION, 99])
        );
        assert!(!response["pinned_installation_id"]
            .as_str()
            .unwrap()
            .is_empty());
        let expires_at = response["expires_at_unix"].as_f64().unwrap();
        assert!(
            expires_at >= before + sase_gateway::FLEET_BOOTSTRAP_TTL_SECONDS
        );
        assert!(
            expires_at
                <= after + sase_gateway::FLEET_BOOTSTRAP_TTL_SECONDS + 1.0
        );

        let auth_path = home
            .path()
            .join(sase_gateway::FLEET_AUTH_DIR)
            .join(sase_gateway::FLEET_AUTH_FILE);
        let stored = std::fs::read_to_string(auth_path).unwrap();
        assert!(!stored.contains(secret));
        assert!(stored.contains("secret_hash"));

        let pinned_request = json!({
            "schema_version": 1,
            "requested_scopes": [],
            "supported_protocol_versions": [1],
            "expires_at_unix": null,
            "installation_pin": "not-the-current-installation"
        });
        let pinned_obj = json_value_to_py(py, &pinned_request)
            .unwrap()
            .into_bound(py);
        let pinned_dict = pinned_obj.downcast::<PyDict>().unwrap();
        let err = py_fleet_issue_bootstrap(
            py,
            home.path().to_str().unwrap(),
            pinned_dict,
        )
        .unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));
        assert!(err.to_string().contains("installation_pin does not match"));
        assert!(!err.to_string().contains(secret));
    });
}

#[test]
fn fleet_followed_batch_agent_session_promotions_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "fleet_followed_batch_agent_session_promotions",
            // legacy binding name; removed in core-contract
            "fleet_followed_batch_family_promotions",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let installation_id = format!("sase_inst_v1_{}", "a".repeat(64));
        let locator = |agent_session_id: Option<&str>| {
            json!({
                "schema_version": 1,
                "project": {
                    "schema_version": 1,
                    "origin": {
                        "schema_version": 1,
                        "installation_id": installation_id,
                    },
                    "project_id": "project-1",
                },
                "agent_id": "worker",
                "family_id": agent_session_id,
            })
        };
        let singleton = locator(None);
        let agent_session = locator(Some("family-1"));
        let singleton_key: String = module
            .getattr("fleet_logical_locator_key")
            .unwrap()
            .call1((json_value_to_py(py, &singleton)
                .unwrap()
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),))
            .unwrap()
            .extract()
            .unwrap();
        let record = json!({
            "schema_version": 1,
            "logical_locator": singleton,
            "logical_key": singleton_key,
            "created_by": "explicit",
            "state": "active",
            "created_at_unix": 10.0,
            "updated_at_unix": 10.0,
            "activated_at_unix": 10.0,
            "operation_key": null,
        });
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "records": [record],
                "observations": [agent_session],
            }),
        )
        .unwrap();
        let legacy_result = module
            .getattr("fleet_followed_batch_family_promotions")
            .unwrap()
            .call1((request.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let legacy_result = py_to_json_value(&legacy_result).unwrap();
        let result = module
            .getattr("fleet_followed_batch_agent_session_promotions")
            .unwrap()
            .call1((request.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let result = py_to_json_value(&result).unwrap();
        assert_eq!(result, legacy_result);
        assert_eq!(
            result["schema_version"],
            json!(core_fleet_contract::FLEET_CONTRACT_SCHEMA_VERSION)
        );
        assert_eq!(result["promotions"][0]["from"], singleton);
        assert_eq!(result["promotions"][0]["to"], agent_session);

        let other_agent_session = locator(Some("family-2"));
        let ambiguous = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "records": [record],
                "observations": [agent_session, other_agent_session],
            }),
        )
        .unwrap();
        let ambiguous = module
            .getattr("fleet_followed_batch_agent_session_promotions")
            .unwrap()
            .call1((ambiguous.bind(py).downcast::<PyDict>().unwrap(),))
            .unwrap();
        let ambiguous = py_to_json_value(&ambiguous).unwrap();
        assert_eq!(ambiguous["promotions"], json!([]));

        let bad_schema = json_value_to_py(
            py,
            &json!({
                "schema_version": 9,
                "records": [],
                "observations": [],
            }),
        )
        .unwrap();
        let err = module
            .getattr("fleet_followed_batch_agent_session_promotions")
            .unwrap()
            .call1((bad_schema.bind(py).downcast::<PyDict>().unwrap(),));
        assert!(err.is_err());

        let not_object = json_value_to_py(py, &json!([1, 2, 3])).unwrap();
        let err = module
            .getattr("fleet_followed_batch_agent_session_promotions")
            .unwrap()
            .call1((not_object.bind(py),));
        assert!(err.is_err());
    });
}
