use super::super::*;
use super::support::*;
use serde_json::json;

#[test]
fn fleet_endpoint_join_accepts_api_bases() {
    assert_eq!(
        fleet_url("https://fleet.example.test", "/summary"),
        "https://fleet.example.test/api/fleet/v1/summary"
    );
    assert_eq!(
        fleet_url("https://fleet.example.test/api", "/summary"),
        "https://fleet.example.test/api/fleet/v1/summary"
    );
    assert_eq!(
        fleet_url("https://fleet.example.test/api/fleet/v1", "/summary"),
        "https://fleet.example.test/api/fleet/v1/summary"
    );
}

#[tokio::test]
async fn replace_config_fails_closed_for_missing_managed_ca_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "c".repeat(64)
    );
    let response = state
        .replace_config(vec![FederationHostConfigWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            alias: Some("apollo".to_string()),
            plan: ConnectionPlanWire {
                schema_version: 1,
                provider_ref: "builtin:https".to_string(),
                endpoint: "https://127.0.0.1:443".to_string(),
                credential_ref: "cred-apollo".to_string(),
                pinned_installation_id: pin,
                connection_kind: sase_core::FleetConnectionKindWire::Gateway,
                tls: sase_core::TlsTrustSettingsWire {
                    schema_version: 1,
                    mode: sase_core::TlsTrustModeWire::PinnedCa,
                    ca_ref: Some("missing".to_string()),
                    server_name_ref: None,
                },
            },
            bearer_token: "token".to_string(),
        }])
        .await
        .unwrap();

    assert_eq!(response["configured_hosts"], json!(0), "{response}");
    assert_eq!(response["hosts"][0]["status"], json!("invalid"));
    assert_eq!(
        response["hosts"][0]["error"]["target"],
        json!("hosts.plan.tls.ca_ref")
    );
}

#[tokio::test]
async fn worker_trusts_pinned_ca_and_preserves_healthy_host_beside_faults() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = FederationWorkerConfig::new(tmp.path());
    config.run_root = tmp.path().join("run");
    config.socket_path = config.run_root.join("worker.sock");
    config.idle_timeout = Duration::from_secs(30);
    let socket_path = config.socket_path.clone();

    let healthy_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "d".repeat(64)
    );
    let untrusted_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "e".repeat(64)
    );
    let hung_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "f".repeat(64)
    );
    let fixture =
        start_https_fixture(tmp.path(), "loopback", &healthy_pin).await;
    let hung_listener =
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hung_port = hung_listener.local_addr().unwrap().port();
    let hung_reached = Arc::new(tokio::sync::Notify::new());
    let hung_reached_writer = hung_reached.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _addr)) = hung_listener.accept().await else {
                return;
            };
            hung_reached_writer.notify_one();
            let _stream = stream;
            std::future::pending::<()>().await;
        }
    });

    let worker = tokio::spawn(run(config));
    let replace = request_worker(
        &socket_path,
        "replace-tls",
        json!({
            "op": "replace_config",
            "hosts": [
                tls_host(
                    "apollo",
                    &healthy_pin,
                    fixture.port,
                    "pinned_ca",
                    Some("loopback"),
                ),
                tls_host(
                    "hera",
                    &untrusted_pin,
                    fixture.port,
                    "system_roots",
                    None,
                ),
                tls_host(
                    "zeus",
                    &hung_pin,
                    hung_port,
                    "system_roots",
                    None,
                ),
            ],
        }),
    )
    .await;
    assert_eq!(replace["configured_hosts"], json!(3), "{replace}");

    let deadline_budget_ms = 700_u64;
    let started = std::time::Instant::now();
    let response = request_worker_with_deadline(
        &socket_path,
        "summary-tls-1",
        json!({"op": "summary", "cache_only": false}),
        unix_now_ms() + deadline_budget_ms,
    )
    .await;
    let elapsed = started.elapsed();

    assert_eq!(response["ok"], json!(true), "{response}");
    assert!(
        elapsed < Duration::from_millis(deadline_budget_ms + 1500),
        "deadline was not bounded: waited {elapsed:?}",
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            hung_reached.notified()
        )
        .await
        .is_ok(),
        "worker never connected to the hung host",
    );
    assert_eq!(
        host_result(&response, "apollo")["status"],
        json!("ok"),
        "{response}"
    );
    assert_eq!(
        host_result(&response, "apollo")["payload"]["counts"]["running"],
        json!(1)
    );
    assert_ne!(
        host_result(&response, "hera")["status"],
        json!("ok"),
        "system roots must not silently trust the pinned CA fixture: {response}"
    );
    assert_eq!(
        host_result(&response, "zeus")["status"],
        json!("deadline"),
        "{response}"
    );

    let second = request_worker_with_deadline(
        &socket_path,
        "summary-tls-2",
        json!({"op": "summary", "cache_only": false}),
        unix_now_ms() + deadline_budget_ms,
    )
    .await;
    assert_eq!(second["ok"], json!(true), "{second}");
    assert_eq!(
        host_result(&second, "apollo")["status"],
        json!("ok"),
        "{second}"
    );

    let paths = request_paths(&fixture.requests);
    assert!(
        paths.iter().any(|path| path == "/api/fleet/v1/hello"),
        "trusted fixture did not receive hello: {paths:?}",
    );
    assert!(
        paths.iter().any(|path| path == "/api/fleet/v1/summary"),
        "trusted fixture did not receive summary: {paths:?}",
    );

    let shutdown =
        request_worker(&socket_path, "shutdown-tls", json!({"op": "shutdown"}))
            .await;
    assert_eq!(shutdown["shutdown"], json!(true));
    worker.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_catalog_hosts_continues_only_requested_hosts() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = FederationWorkerConfig::new(tmp.path());
    config.run_root = tmp.path().join("run");
    config.socket_path = config.run_root.join("worker.sock");
    config.idle_timeout = Duration::from_secs(30);
    let socket_path = config.socket_path.clone();

    let apollo_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "1".repeat(64)
    );
    let zeus_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "2".repeat(64)
    );
    let apollo =
        start_https_fixture(tmp.path(), "apollo-ca", &apollo_pin).await;
    let zeus = start_https_fixture(tmp.path(), "zeus-ca", &zeus_pin).await;
    let worker = tokio::spawn(run(config));
    let replace = request_worker(
        &socket_path,
        "replace-catalog-hosts",
        json!({
            "op": "replace_config",
            "hosts": [
                tls_host(
                    "apollo",
                    &apollo_pin,
                    apollo.port,
                    "pinned_ca",
                    Some("apollo-ca"),
                ),
                tls_host(
                    "zeus",
                    &zeus_pin,
                    zeus.port,
                    "pinned_ca",
                    Some("zeus-ca"),
                ),
            ],
        }),
    )
    .await;
    assert_eq!(replace["configured_hosts"], json!(2), "{replace}");

    let response = request_worker(
        &socket_path,
        "catalog-hosts-1",
        json!({
            "op": "catalog_hosts",
            "cache_only": false,
            "queries": [
                {
                    "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                    "installation_id": zeus_pin,
                    "query": {
                        "schema_version": 1,
                        "cursor": "zeus:100",
                        "limit": 100,
                        "project_ids": [],
                        "query": null,
                        "status_buckets": [],
                        "include_terminal": true,
                    },
                },
            ],
        }),
    )
    .await;
    assert_eq!(response["operation"], json!("catalog"));
    assert_eq!(response["hosts"].as_array().unwrap().len(), 1);
    assert_eq!(response["hosts"][0]["alias"], json!("zeus"));
    assert_eq!(response["hosts"][0]["status"], json!("ok"));
    assert!(request_paths(&apollo.requests).is_empty());
    let zeus_requests = zeus.requests.lock().unwrap().clone();
    assert_eq!(
        zeus_requests
            .iter()
            .filter(|request| request["path"] == json!("/api/fleet/v1/catalog"))
            .count(),
        1
    );
    assert_eq!(
        zeus_requests
            .iter()
            .find(|request| request["path"] == json!("/api/fleet/v1/catalog"))
            .unwrap()["body"]["cursor"],
        json!("zeus:100")
    );

    let shutdown = request_worker(
        &socket_path,
        "shutdown-catalog-hosts",
        json!({"op": "shutdown"}),
    )
    .await;
    assert_eq!(shutdown["shutdown"], json!(true));
    worker.await.unwrap().unwrap();
}

/// Real worker + real per-host HTTPS fan-out deadline, using two genuinely
/// real loopback fixtures instead of a scripted/mocked response: one host
/// accepts the connection (proving the worker actually reached it) and
/// then never answers, so it can only resolve via the worker's real
/// deadline timeout; the other is a real authenticated HTTPS gateway
/// fixture over pinned-CA TLS that returns usable rows. This exercises the
/// actual `read_all`/`read_one_host`/`with_deadline` fan-out in production
/// code, proving the healthy host's usable result is not discarded by the
/// outer envelope deadline racing the still-hanging host, and that the
/// worker stays usable for a subsequent request afterward.
#[tokio::test]
async fn worker_bounds_deadline_and_preserves_fast_host_beside_hung_host() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = FederationWorkerConfig::new(tmp.path());
    config.run_root = tmp.path().join("run");
    config.socket_path = config.run_root.join("worker.sock");
    config.idle_timeout = Duration::from_secs(30);
    let socket_path = config.socket_path.clone();

    let hung_listener =
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hung_port = hung_listener.local_addr().unwrap().port();
    let hung_reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let hung_reached_writer = hung_reached.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _addr)) = hung_listener.accept().await else {
                return;
            };
            hung_reached_writer.notify_one();
            let _stream = stream;
            std::future::pending::<()>().await;
        }
    });

    // A genuinely healthy host beside the hung one: a real HTTPS gateway
    // fixture over pinned-CA TLS that answers hello and summary with
    // usable rows.
    let healthy_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "b".repeat(64)
    );
    let fixture =
        start_https_fixture(tmp.path(), "loopback", &healthy_pin).await;

    let worker = tokio::spawn(run(config));

    let hung_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "a".repeat(64)
    );
    let replace = request_worker(
        &socket_path,
        "replace-1",
        json!({
            "op": "replace_config",
            "hosts": [
                {
                    "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                    "alias": "zeus",
                    "plan": {
                        "schema_version": 1,
                        "provider_ref": "builtin:https",
                        "endpoint": format!("https://127.0.0.1:{hung_port}"),
                        "credential_ref": "cred-zeus",
                        "pinned_installation_id": hung_pin,
                        "connection_kind": "gateway",
                        "tls": {
                            "schema_version": 1,
                            "mode": "system_roots",
                            "ca_ref": null,
                            "server_name_ref": null,
                        },
                    },
                    "bearer_token": "token-zeus",
                },
                tls_host(
                    "apollo",
                    &healthy_pin,
                    fixture.port,
                    "pinned_ca",
                    Some("loopback"),
                ),
            ],
        }),
    )
    .await;
    assert_eq!(replace["configured_hosts"], json!(2), "{replace}");

    let deadline_budget_ms = 700_u64;
    let deadline_unix_ms = unix_now_ms() + deadline_budget_ms;
    let started = std::time::Instant::now();
    let response = request_worker_with_deadline(
        &socket_path,
        "summary-1",
        json!({"op": "summary", "cache_only": false}),
        deadline_unix_ms,
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            hung_reached.notified()
        )
        .await
        .is_ok(),
        "worker never connected to the hung host",
    );
    assert!(
        elapsed < Duration::from_millis(deadline_budget_ms + 1500),
        "deadline was not bounded: waited {elapsed:?}",
    );
    assert_eq!(
        response["ok"],
        json!(true),
        "a hung host must not discard the whole response: {response}",
    );

    let zeus = host_result(&response, "zeus");
    assert_eq!(zeus["status"], json!("deadline"), "{response}");

    let apollo = host_result(&response, "apollo");
    assert_eq!(apollo["status"], json!("ok"), "{response}");
    assert_eq!(
        apollo["payload"]["counts"]["running"],
        json!(1),
        "the healthy host beside the hung host must return usable rows: {response}",
    );

    // The worker must remain usable for a subsequent request: the same
    // still-hung host must not wedge later calls either.
    let second_deadline_unix_ms = unix_now_ms() + deadline_budget_ms;
    let second_started = std::time::Instant::now();
    let second_response = request_worker_with_deadline(
        &socket_path,
        "summary-2",
        json!({"op": "summary", "cache_only": false}),
        second_deadline_unix_ms,
    )
    .await;
    let second_elapsed = second_started.elapsed();
    assert!(
        second_elapsed < Duration::from_millis(deadline_budget_ms + 1500),
        "worker was not usable on a subsequent request: waited {second_elapsed:?}",
    );
    assert_eq!(second_response["ok"], json!(true), "{second_response}");
    assert_eq!(
        host_result(&second_response, "zeus")["status"],
        json!("deadline"),
    );
    assert_eq!(
        host_result(&second_response, "apollo")["status"],
        json!("ok"),
        "{second_response}"
    );

    let paths = request_paths(&fixture.requests);
    assert!(
        paths.iter().any(|path| path == "/api/fleet/v1/hello"),
        "healthy fixture did not receive hello: {paths:?}",
    );
    assert!(
        paths.iter().any(|path| path == "/api/fleet/v1/summary"),
        "healthy fixture did not receive summary: {paths:?}",
    );

    let inventory_deadline_unix_ms = unix_now_ms() + deadline_budget_ms;
    let inventory_started = std::time::Instant::now();
    let inventory_response = request_worker_with_deadline(
        &socket_path,
        "attention-inventory-1",
        json!({
            "op": "attention_inventory",
            "request": {
                "schema_version": 1,
                "limit": 1,
            },
            "cache_only": false,
        }),
        inventory_deadline_unix_ms,
    )
    .await;
    let inventory_elapsed = inventory_started.elapsed();
    assert!(
        inventory_elapsed < Duration::from_millis(deadline_budget_ms + 1500),
        "inventory deadline was not bounded: waited {inventory_elapsed:?}",
    );
    assert_eq!(
        inventory_response["ok"],
        json!(true),
        "a hung inventory host must not discard the whole response: {inventory_response}",
    );
    assert_eq!(
        inventory_response["result"]["operation"],
        json!("attention_inventory"),
    );
    assert_eq!(
        host_result(&inventory_response, "zeus")["status"],
        json!("deadline"),
        "{inventory_response}",
    );
    assert_ne!(
        host_result(&inventory_response, "apollo")["status"],
        json!("deadline"),
        "{inventory_response}",
    );

    let shutdown =
        request_worker(&socket_path, "shutdown-1", json!({"op": "shutdown"}))
            .await;
    assert_eq!(shutdown["shutdown"], json!(true));
    worker.await.unwrap().unwrap();
}

fn system_host_config(
    alias: &str,
    pin: &str,
    endpoint: &str,
    bearer_token: &str,
) -> FederationHostConfigWire {
    FederationHostConfigWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        alias: Some(alias.to_string()),
        plan: ConnectionPlanWire {
            schema_version: 1,
            provider_ref: "builtin:https".to_string(),
            endpoint: endpoint.to_string(),
            credential_ref: format!("cred-{alias}"),
            pinned_installation_id: pin.to_string(),
            connection_kind: sase_core::FleetConnectionKindWire::Gateway,
            tls: sase_core::TlsTrustSettingsWire {
                schema_version: 1,
                mode: sase_core::TlsTrustModeWire::SystemRoots,
                ca_ref: None,
                server_name_ref: None,
            },
        },
        bearer_token: bearer_token.to_string(),
    }
}

#[tokio::test]
async fn replace_config_reuses_unchanged_remote_hosts() {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let pin_a = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "a".repeat(64)
    );
    let pin_b = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "b".repeat(64)
    );
    let endpoint_a = "https://127.0.0.1:11443".to_string();
    let endpoint_b = "https://127.0.0.1:12443".to_string();
    state
        .replace_config(vec![
            system_host_config("apollo", &pin_a, &endpoint_a, "token-apollo"),
            system_host_config("zeus", &pin_b, &endpoint_b, "token-zeus"),
        ])
        .await
        .unwrap();
    assert_eq!(state.hosts.read().await.len(), 2);
    let first = state.hosts.read().await.clone();

    // Identical configs reuse both hosts, keeping their result rows.
    let response = state
        .replace_config(vec![
            system_host_config("apollo", &pin_a, &endpoint_a, "token-apollo"),
            system_host_config("zeus", &pin_b, &endpoint_b, "token-zeus"),
        ])
        .await
        .unwrap();
    assert_eq!(response["configured_hosts"], json!(2), "{response}");
    assert_eq!(response["hosts"][0]["status"], json!("configured"));
    assert_eq!(response["hosts"][1]["status"], json!("configured"));
    let second = state.hosts.read().await.clone();
    assert!(
        Arc::ptr_eq(first.get(&pin_a).unwrap(), second.get(&pin_a).unwrap()),
        "an unchanged host must survive replace_config",
    );
    assert!(
        Arc::ptr_eq(first.get(&pin_b).unwrap(), second.get(&pin_b).unwrap()),
        "an unchanged host must survive replace_config",
    );

    // A changed endpoint rebuilds only that host.
    let endpoint_a2 = "https://127.0.0.1:13443".to_string();
    state
        .replace_config(vec![
            system_host_config("apollo", &pin_a, &endpoint_a2, "token-apollo"),
            system_host_config("zeus", &pin_b, &endpoint_b, "token-zeus"),
        ])
        .await
        .unwrap();
    let third = state.hosts.read().await.clone();
    assert!(
        !Arc::ptr_eq(second.get(&pin_a).unwrap(), third.get(&pin_a).unwrap()),
        "a changed endpoint must build a new host",
    );
    assert!(
        Arc::ptr_eq(second.get(&pin_b).unwrap(), third.get(&pin_b).unwrap()),
        "an unchanged host must survive replace_config",
    );

    // A changed bearer token rebuilds only that host.
    state
        .replace_config(vec![
            system_host_config("apollo", &pin_a, &endpoint_a2, "token-apollo"),
            system_host_config(
                "zeus",
                &pin_b,
                &endpoint_b,
                "token-zeus-rotated",
            ),
        ])
        .await
        .unwrap();
    let fourth = state.hosts.read().await.clone();
    assert!(
        Arc::ptr_eq(third.get(&pin_a).unwrap(), fourth.get(&pin_a).unwrap()),
        "an unchanged host must survive replace_config",
    );
    assert!(
        !Arc::ptr_eq(third.get(&pin_b).unwrap(), fourth.get(&pin_b).unwrap()),
        "a changed bearer token must build a new host",
    );

    // Removed hosts drop while the survivor is still reused.
    state
        .replace_config(vec![system_host_config(
            "zeus",
            &pin_b,
            &endpoint_b,
            "token-zeus-rotated",
        )])
        .await
        .unwrap();
    let fifth = state.hosts.read().await.clone();
    assert_eq!(fifth.len(), 1);
    assert!(
        Arc::ptr_eq(fourth.get(&pin_b).unwrap(), fifth.get(&pin_b).unwrap()),
        "an unchanged host must survive replace_config",
    );
}

#[test]
fn host_backoff_schedule_doubles_and_caps() {
    assert_eq!(host_read_backoff_window(1), Duration::from_secs(5));
    assert_eq!(host_read_backoff_window(2), Duration::from_secs(10));
    assert_eq!(host_read_backoff_window(3), Duration::from_secs(20));
    assert_eq!(host_read_backoff_window(4), Duration::from_secs(40));
    assert_eq!(host_read_backoff_window(5), Duration::from_secs(80));
    assert_eq!(host_read_backoff_window(6), Duration::from_secs(120));
    assert_eq!(host_read_backoff_window(100), Duration::from_secs(120));
}

#[test]
fn read_backoff_triggers_only_on_retryable_errors() {
    for code in ["deadline", "timeout", "unavailable", "internal"] {
        assert!(
            is_retryable_read_error(&federation_error(code, "x", None)),
            "{code} must engage back-off",
        );
    }
    for code in [
        "unauthorized",
        "not_found",
        "invalid_request",
        "invalid_response",
        "quarantined",
        "stale",
        "unsupported_version",
        "missing_credential",
    ] {
        assert!(
            !is_retryable_read_error(&federation_error(code, "x", None)),
            "{code} must not engage back-off",
        );
    }
}

fn sample_launch_request(
    installation_id: &str,
) -> sase_core::FleetLaunchRequestWire {
    sase_core::FleetLaunchRequestWire {
        schema_version: 1,
        key: sase_core::ScopedOperationKeyWire {
            schema_version: 1,
            controller_id: "controller-1".to_string(),
            operation_id: "op-1".to_string(),
        },
        target_installation_id: installation_id.to_string(),
        intent: sase_core::FleetLaunchIntentWire {
            schema_version: 1,
            prompt: "hello".to_string(),
            request_id: None,
            display_name: None,
            name: None,
            model: None,
            provider: None,
            runtime: None,
            project: sase_core::FleetLaunchProjectContextWire {
                schema_version: 1,
                provider_ref: None,
                project_id: "proj".to_string(),
                revision: None,
                patch_ref: None,
            },
            dry_run: Some(true),
            follow: false,
            references: vec![],
        },
        payload_fingerprint: sase_core::PayloadFingerprintWire {
            schema_version: 1,
            sha256: "b".repeat(64),
        },
        acceptance_window_seconds: 30.0,
    }
}

fn sample_mutation_request_for(
    installation_id: &str,
) -> sase_core::FleetMutationRequestWire {
    let target = sase_core::AgentInstanceLocatorWire {
        schema_version: 1,
        logical: sase_core::LogicalAgentLocatorWire {
            schema_version: 1,
            project: sase_core::ProjectLocatorWire {
                schema_version: 1,
                origin: sase_core::OriginLocatorWire {
                    schema_version: 1,
                    installation_id: installation_id.to_string(),
                },
                project_id: "proj".to_string(),
            },
            agent_id: "alpha".to_string(),
            agent_session_id: None,
        },
        turn_id: "ace-run".to_string(),
        run_id: "20260906120000".to_string(),
        attempt_id: "attempt-0".to_string(),
    };
    let logical_key = sase_core::logical_locator_key(&target.logical).unwrap();
    sase_core::FleetMutationRequestWire {
        schema_version: 1,
        key: sase_core::ScopedOperationKeyWire {
            schema_version: 1,
            controller_id: "controller-1".to_string(),
            operation_id: "op-1".to_string(),
        },
        target_installation_id: installation_id.to_string(),
        intent: sase_core::FleetMutationIntentWire {
            schema_version: 1,
            kind: sase_core::FleetMutationKindWire::Stop,
            row_revision: sase_core::ResourceRevisionWire {
                schema_version: 1,
                logical_key,
                revision: 1,
            },
            target,
            reason: Some("stop".to_string()),
            fork_prompt: None,
            kill_source_first: None,
            follow: false,
        },
        payload_fingerprint: sase_core::PayloadFingerprintWire {
            schema_version: 1,
            sha256: "a".repeat(64),
        },
        acceptance_window_seconds: 30.0,
    }
}

/// A slow host engages per-host read back-off: the second read serves the
/// cached payload without contacting the host, the window expiry lets reads
/// through again, a success resets the failure count, and user-initiated
/// launch/mutate still contact the host while reads back off.
#[tokio::test]
async fn read_backoff_serves_cache_and_short_circuits_slow_host() {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let tmp = tempfile::tempdir().unwrap();
    let pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "c".repeat(64)
    );
    let fixture = start_toggle_fixture(tmp.path(), "toggle-ca", &pin).await;
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let host_wire: FederationHostConfigWire = serde_json::from_value(tls_host(
        "apollo",
        &pin,
        fixture.port,
        "pinned_ca",
        Some("toggle-ca"),
    ))
    .unwrap();
    let replace = state.replace_config(vec![host_wire]).await.unwrap();
    assert_eq!(replace["configured_hosts"], json!(1), "{replace}");
    let host = state.hosts.read().await.get(&pin).unwrap().clone();
    let connections = || fixture.connections.load(Ordering::SeqCst);
    let deadline = || RequestDeadline {
        unix_ms: Some(unix_now_ms() + 700),
    };
    let summary_hosts = |value: &serde_json::Value| {
        value["hosts"].as_array().unwrap()[0].clone()
    };

    // A healthy read succeeds and records no failure.
    let first = state
        .read_all(ReadOperation::Summary, false, deadline())
        .await
        .unwrap();
    assert_eq!(summary_hosts(&first)["status"], json!("ok"), "{first}");
    assert_eq!(host.backoff_failures_for_test().await, 0);
    let served = connections();
    assert!(served > 0, "the healthy read must contact the host");
    let paths = request_paths(&fixture.requests);
    assert!(
        paths.iter().any(|path| path == "/api/fleet/v1/hello"),
        "fixture did not receive hello: {paths:?}",
    );

    // While the host hangs, the read times out but still serves the cached
    // payload as stale, and engages back-off.
    fixture.hang.store(true, Ordering::SeqCst);
    let failed = state
        .read_all(ReadOperation::Summary, false, deadline())
        .await
        .unwrap();
    let failed_host = summary_hosts(&failed);
    assert_eq!(failed_host["status"], json!("stale"), "{failed}");
    assert_eq!(failed_host["cached"], json!(true));
    assert_eq!(
        failed_host["payload"],
        summary_hosts(&first)["payload"],
        "a timed-out read must serve the cached payload",
    );
    assert_eq!(failed_host["error"]["target"], json!("deadline_unix_ms"));
    assert_eq!(host.backoff_failures_for_test().await, 1);
    assert!(
        connections() > served,
        "the timed-out read must have contacted the host",
    );

    // An immediate second read makes no HTTP request and reports the
    // back-off instead: same status vocabulary, target backoff.
    let before = connections();
    let started = Instant::now();
    let backed = state
        .read_all(ReadOperation::Summary, false, deadline())
        .await
        .unwrap();
    let backed_host = summary_hosts(&backed);
    assert_eq!(backed_host["status"], json!("stale"), "{backed}");
    assert_eq!(backed_host["cached"], json!(true));
    assert_eq!(
        backed_host["payload"],
        summary_hosts(&first)["payload"],
        "a backed-off read must serve the cached payload",
    );
    assert_eq!(backed_host["error"]["code"], json!("deadline"));
    assert_eq!(backed_host["error"]["target"], json!("backoff"));
    assert!(
        backed_host["error"]["message"]
            .as_str()
            .unwrap()
            .contains("backing off for"),
        "the back-off error must name the remaining window: {backed}",
    );
    assert_eq!(
        connections(),
        before,
        "a backed-off read must not contact the host",
    );
    assert_eq!(host.backoff_failures_for_test().await, 1);
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "a backed-off read must not wait for the deadline",
    );

    // Per-host catalog reads share the same short-circuit.
    let catalog = state
        .read_catalog_hosts(
            vec![FederationHostCatalogQueryWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                installation_id: pin.clone(),
                query: sase_core::FleetCatalogQueryWire {
                    schema_version: 1,
                    scope: sase_core::FleetCatalogScopeWire::Presentation,
                    snapshot_id: None,
                    cursor: None,
                    limit: Some(10),
                    project_ids: vec![],
                    query: None,
                    status_buckets: vec![],
                    include_terminal: true,
                },
            }],
            false,
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        catalog["hosts"][0]["error"]["target"],
        json!("backoff"),
        "{catalog}",
    );
    assert_eq!(
        connections(),
        before,
        "a backed-off catalog read must not contact the host",
    );

    // After the window, a read contacts the host again.
    host.clear_backoff_for_test().await;
    let failed_again = state
        .read_all(ReadOperation::Summary, false, deadline())
        .await
        .unwrap();
    let failed_again_host = summary_hosts(&failed_again);
    assert_eq!(failed_again_host["status"], json!("stale"));
    assert_eq!(
        failed_again_host["error"]["target"],
        json!("deadline_unix_ms"),
        "after the window the read must contact the host again: {failed_again}",
    );
    assert!(connections() > before);
    assert_eq!(host.backoff_failures_for_test().await, 2);

    // A success resets the failure count.
    fixture.hang.store(false, Ordering::SeqCst);
    host.clear_backoff_for_test().await;
    let recovered = state
        .read_all(ReadOperation::Summary, false, deadline())
        .await
        .unwrap();
    assert_eq!(
        summary_hosts(&recovered)["status"],
        json!("ok"),
        "{recovered}",
    );
    assert_eq!(host.backoff_failures_for_test().await, 0);

    // User-initiated launch and mutate still contact the host while reads
    // back off.
    fixture.hang.store(true, Ordering::SeqCst);
    let hang_failed = state
        .read_all(ReadOperation::Summary, false, deadline())
        .await
        .unwrap();
    assert_eq!(
        summary_hosts(&hang_failed)["error"]["target"],
        json!("deadline_unix_ms"),
    );
    assert_eq!(host.backoff_failures_for_test().await, 1);
    let before_launch = connections();
    let launch_error = state
        .launch_one(
            "apollo".to_string(),
            sample_launch_request(&pin),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(connections() > before_launch);
    assert_ne!(
        launch_error.target.as_deref(),
        Some("backoff"),
        "a launch during read back-off must contact the host",
    );
    let before_mutate = connections();
    let mutate_error = state
        .mutate_one(
            "apollo".to_string(),
            sample_mutation_request_for(&pin),
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(connections() > before_mutate);
    assert_ne!(
        mutate_error.target.as_deref(),
        Some("backoff"),
        "a mutate during read back-off must contact the host",
    );
}
