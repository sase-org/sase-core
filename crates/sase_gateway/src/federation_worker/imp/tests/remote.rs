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
/// real loopback TCP fixtures instead of a scripted/mocked response: one
/// host accepts the connection (proving the worker actually reached it)
/// and then never answers, so it can only resolve via the worker's real
/// deadline timeout; the other has nothing listening, so it fails fast
/// with a real connection-refused error. This exercises the actual
/// `read_all`/`read_one_host`/`with_deadline` fan-out in production code,
/// proving the fast host's result is not discarded by the outer envelope
/// deadline racing the still-hanging host, and that the worker stays
/// usable for a subsequent request afterward.
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

    // Reserve a port, then drop the listener: nothing answers there, so
    // connecting to it fails fast with a real connection-refused error.
    let fast_port = {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        probe.local_addr().unwrap().port()
    };

    let worker = tokio::spawn(run(config));

    let hung_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "a".repeat(64)
    );
    let fast_pin = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "b".repeat(64)
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
                {
                    "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                    "alias": "apollo",
                    "plan": {
                        "schema_version": 1,
                        "provider_ref": "builtin:https",
                        "endpoint": format!("https://127.0.0.1:{fast_port}"),
                        "credential_ref": "cred-apollo",
                        "pinned_installation_id": fast_pin,
                        "connection_kind": "gateway",
                        "tls": {
                            "schema_version": 1,
                            "mode": "system_roots",
                            "ca_ref": null,
                            "server_name_ref": null,
                        },
                    },
                    "bearer_token": "token-apollo",
                },
            ],
        }),
    )
    .await;
    assert_eq!(replace["configured_hosts"], json!(2), "{replace}");

    let deadline_budget_ms = 400_u64;
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
    assert_ne!(apollo["status"], json!("deadline"), "{response}");
    assert_ne!(
        apollo["status"],
        json!("ok"),
        "nothing is listening on the fast host's port: {response}",
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
