use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode};

use crate::fleet_auth::{
    current_unix_time, FLEET_SCOPE_HELLO, FLEET_SCOPE_LAUNCH,
    FLEET_SCOPE_MUTATE, FLEET_SCOPE_REVOKE, FLEET_SCOPE_ROTATE,
    FLEET_SCOPE_SUMMARY_READ,
};

use crate::wire::{
    EventPayloadWire, FleetHelloResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

use axum::http::Request;

use chrono::Duration;

use serde_json::{json, Value};

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use super::super::state::{GatewayState, FLEET_PROTOCOL_VERSIONS_HEADER};

use super::support::*;

#[tokio::test]
async fn fleet_enrollment_and_hello_return_identity_and_capabilities() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let bootstrap = fleet_bootstrap(&state, &[], None);
    let (status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_HELLO, FLEET_SCOPE_ROTATE, FLEET_SCOPE_REVOKE],
            vec![99, 1],
        )),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(enrolled["outcome"], "enrolled");
    assert_eq!(enrolled["protocol_version"], 1);
    assert_eq!(enrolled["machine_selector"], "test-host");
    assert_eq!(
        enrolled["installation"]["installation_id"],
        bootstrap.pinned_installation_id
    );
    assert_eq!(enrolled["token_type"], "bearer");
    let token = enrolled["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("sase_fleet_"));
    assert_eq!(
        enrolled["capabilities"]["host"],
        json!([FLEET_SCOPE_REVOKE, FLEET_SCOPE_ROTATE, FLEET_SCOPE_HELLO])
    );

    let (hello_status, hello) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/fleet/v1/hello")
            .header("authorization", format!("Bearer {token}"))
            .header(FLEET_PROTOCOL_VERSIONS_HEADER, "2, 1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(hello_status, StatusCode::OK);
    assert_eq!(hello["protocol_version"], 1);
    assert_eq!(hello["schema_version"], GATEWAY_WIRE_SCHEMA_VERSION);
    assert_eq!(hello["capabilities"]["schema_version"], 1);
    assert_eq!(
        hello["fleet_contract_schema_version"],
        sase_core::FLEET_CONTRACT_SCHEMA_VERSION
    );
    assert_ne!(
        hello["capabilities"]["schema_version"],
        hello["fleet_contract_schema_version"]
    );
    assert_eq!(
        hello["gateway_version"],
        json!({
            "service": "sase-gateway",
            "package_version": env!("CARGO_PKG_VERSION")
        })
    );
    assert_eq!(
        hello["installation"]["installation_id"],
        bootstrap.pinned_installation_id
    );
    assert_eq!(hello["credential"]["controller_id"], "controller-a");
    assert_eq!(
        hello["cursor"]["schema_version"],
        sase_core::FLEET_CONTRACT_SCHEMA_VERSION
    );
    assert_eq!(
        hello["counts"]["schema_version"],
        sase_core::FLEET_CONTRACT_SCHEMA_VERSION
    );
    assert!(hello["counts"]["logical_agent_total"].is_u64());
    assert_eq!(
        hello["freshness"]["schema_version"],
        sase_core::FLEET_CONTRACT_SCHEMA_VERSION
    );

    let mut old_hello = hello.clone();
    old_hello
        .as_object_mut()
        .expect("hello response is an object")
        .remove("fleet_contract_schema_version");
    let parsed: FleetHelloResponseWire =
        serde_json::from_value(old_hello).unwrap();
    assert_eq!(parsed.fleet_contract_schema_version, None);
    assert_eq!(parsed.capabilities.schema_version, 1);
    assert_eq!(parsed.schema_version, GATEWAY_WIRE_SCHEMA_VERSION);
}

#[tokio::test]
async fn fleet_enrollment_rejects_replayed_and_expired_bootstrap_secrets() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let bootstrap = fleet_bootstrap(&state, &[], None);
    let body = fleet_enroll_body(&bootstrap, &[], vec![1]);
    let (first_status, _first) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(body.clone()),
    )
    .await;
    assert_eq!(first_status, StatusCode::OK);

    let (replay_status, replay) =
        json_response_with_state(state.clone(), fleet_enroll_request(body))
            .await;
    assert_eq!(replay_status, StatusCode::CONFLICT);
    assert_eq!(replay["code"], "bootstrap_consumed");

    let expired = fleet_bootstrap_at(&state, 2.0, 1.0);
    let (expired_status, expired_response) = json_response_with_state(
        state,
        fleet_enroll_request(fleet_enroll_body(&expired, &[], vec![1])),
    )
    .await;
    assert_eq!(expired_status, StatusCode::BAD_REQUEST);
    assert_eq!(expired_response["code"], "bootstrap_expired");
}

#[tokio::test]
async fn fleet_routes_enforce_declared_scopes() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let bootstrap = fleet_bootstrap(&state, &[FLEET_SCOPE_HELLO], None);
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_HELLO],
            vec![1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let token = enrolled["token"].as_str().unwrap();

    let (summary_status, summary) = json_response_with_state(
        state.clone(),
        fleet_json_request("GET", "/api/fleet/v1/summary", Some(token), None),
    )
    .await;
    assert_eq!(summary_status, StatusCode::FORBIDDEN);
    assert_eq!(summary["code"], "scope_denied");
    assert_eq!(summary["target"], FLEET_SCOPE_SUMMARY_READ);
    assert_eq!(state.fleet_reads().refresh_count_for_test(), 0);

    let (rotate_status, rotate) = json_response_with_state(
        state,
        fleet_json_request(
            "POST",
            "/api/fleet/v1/credential/rotate",
            Some(token),
            Some(json!({
                "schema_version": 1,
                "supported_protocol_versions": [1]
            })),
        ),
    )
    .await;
    assert_eq!(rotate_status, StatusCode::FORBIDDEN);
    assert_eq!(rotate["code"], "scope_denied");
    assert_eq!(rotate["target"], FLEET_SCOPE_ROTATE);
}

#[tokio::test]
async fn fleet_launch_accepts_scoped_request_and_returns_receipt() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_agent_bridge(&tmp);
    let bootstrap = fleet_bootstrap(&state, &[FLEET_SCOPE_LAUNCH], None);
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_LAUNCH],
            vec![1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let token = enrolled["token"].as_str().unwrap();

    let body = sample_fleet_launch_body(
        &bootstrap.pinned_installation_id,
        "dispatch-request-1",
    );
    let (status, launch) =
        post_fleet_launch(state.clone(), token, body.clone()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(launch["decision"], "accept_new");
    assert_eq!(launch["reason"], "unseen_in_window");
    assert_eq!(launch["receipt"]["state"], "pending");
    assert_eq!(
        launch["receipt"]["target_installation_id"],
        bootstrap.pinned_installation_id
    );

    let settled =
        wait_for_launch_receipt_state(&state, token, &body, "settled").await;
    assert_eq!(settled["decision"], "return_original_receipt");
    assert_eq!(settled["receipt"]["message"], "mobile-demo");
    assert_eq!(
        settled["receipt"]["logical_locator"]["agent_id"],
        "mobile-demo"
    );

    wait_for_agents_changed(&state, "fleet_launch", "mobile-demo").await;
    let events = state
        .event_hub
        .replay_after("0000000000000000")
        .unwrap()
        .unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::AgentsChanged {
            reason,
            agent_name: Some(name),
            timestamp: Some(_),
        } if reason == "fleet_launch" && name == "mobile-demo"
    )));
}

#[tokio::test]
async fn fleet_launch_omits_bridge_name_when_prompt_has_id() {
    let tmp = tempfile::tempdir().unwrap();
    let launches = Arc::new(Mutex::new(Vec::new()));
    let state = state_for_custom_agent_bridge(
        &tmp,
        Arc::new(CapturingLaunchBridge {
            home: tmp.path().to_path_buf(),
            launches: launches.clone(),
        }),
    );
    let bootstrap = fleet_bootstrap(&state, &[FLEET_SCOPE_LAUNCH], None);
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_LAUNCH],
            vec![1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let token = enrolled["token"].as_str().unwrap();

    let mut body = sample_fleet_launch_body(
        &bootstrap.pinned_installation_id,
        "dispatch-named-1",
    );
    body["intent"]["prompt"] = json!("%id:observer\n#gh:sase Watch Apollo");
    body["intent"]["name"] = json!("dispatch-named-1");
    let intent: sase_core::FleetLaunchIntentWire =
        serde_json::from_value(body["intent"].clone()).unwrap();
    body["payload_fingerprint"] = serde_json::to_value(
        sase_core::fleet_launch_payload_fingerprint(&intent).unwrap(),
    )
    .unwrap();

    let (status, launch) =
        post_fleet_launch(state.clone(), token, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(launch["decision"], "accept_new");
    let _settled =
        wait_for_launch_receipt_state(&state, token, &body, "settled").await;
    let captured = launches.lock().expect("launch capture lock");
    assert_eq!(captured.len(), 1);
    assert!(
        captured[0].name.is_none(),
        "gateway must not duplicate an identity already in the prompt: {:?}",
        captured[0].name
    );
    assert!(captured[0].prompt.contains("%id:observer"));
}

#[tokio::test]
async fn fleet_launch_replays_delayed_launch_and_reconciles_visible_row() {
    let tmp = tempfile::tempdir().unwrap();
    let launch_count = Arc::new(AtomicUsize::new(0));
    let state = state_for_custom_agent_bridge(
        &tmp,
        Arc::new(DelayedLaunchBridge {
            home: tmp.path().to_path_buf(),
            launch_count: launch_count.clone(),
            delay: StdDuration::from_millis(80),
        }),
    );
    let bootstrap = fleet_bootstrap(
        &state,
        &[FLEET_SCOPE_LAUNCH, FLEET_SCOPE_MUTATE],
        None,
    );
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_LAUNCH, FLEET_SCOPE_MUTATE],
            vec![1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let token = enrolled["token"].as_str().unwrap();

    let empty = state
        .fleet_reads
        .catalog(sase_core::FleetCatalogQueryWire {
            schema_version: 1,
            scope: sase_core::FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(10),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    assert!(empty.page.rows.is_empty());

    let body = sample_fleet_launch_body(
        &bootstrap.pinned_installation_id,
        "dispatch-request-delayed",
    );
    let (first_status, first) =
        post_fleet_launch(state.clone(), token, body.clone()).await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(first["decision"], "accept_new");
    assert_eq!(first["receipt"]["state"], "pending");

    let (replay_status, replay) =
        post_fleet_launch(state.clone(), token, body.clone()).await;
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replay["decision"], "return_original_receipt");
    assert_eq!(replay["receipt"]["state"], "pending");

    let settled =
        wait_for_launch_receipt_state(&state, token, &body, "settled").await;
    assert_eq!(
        settled["receipt"]["logical_locator"]["agent_id"],
        "mobile-demo"
    );
    assert!(settled["receipt"]["instance_locator"].is_object());
    assert_eq!(launch_count.load(AtomicOrdering::SeqCst), 1);

    let page = state
        .fleet_reads
        .catalog(sase_core::FleetCatalogQueryWire {
            schema_version: 1,
            scope: sase_core::FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(10),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();
    assert!(page
        .page
        .rows
        .iter()
        .any(|row| row.logical_locator.agent_id == "mobile-demo"));

    let summary = first_summary(&state).await;
    let (stop_status, stop) = post_mutate(
        state,
        token,
        mutation_body(
            &summary,
            &bootstrap.pinned_installation_id,
            "stop",
            "op-stop-launched",
            json!({}),
        ),
    )
    .await;
    assert_eq!(stop_status, StatusCode::OK);
    assert_eq!(stop["receipt"]["state"], "settled");
    assert_eq!(
        stop["receipt"]["target"]["logical"]["agent_id"],
        "mobile-demo"
    );
}

#[tokio::test]
async fn fleet_launch_failure_settles_failed_without_raw_bridge_output() {
    let tmp = tempfile::tempdir().unwrap();
    let launch_count = Arc::new(AtomicUsize::new(0));
    let state = state_for_custom_agent_bridge(
        &tmp,
        Arc::new(FailingLaunchBridge {
            launch_count: launch_count.clone(),
        }),
    );
    let bootstrap = fleet_bootstrap(&state, &[FLEET_SCOPE_LAUNCH], None);
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_LAUNCH],
            vec![1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let token = enrolled["token"].as_str().unwrap();
    let body = sample_fleet_launch_body(
        &bootstrap.pinned_installation_id,
        "dispatch-request-failed",
    );

    let (first_status, first) =
        post_fleet_launch(state.clone(), token, body.clone()).await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(first["receipt"]["state"], "pending");

    let failed =
        wait_for_launch_receipt_state(&state, token, &body, "failed").await;
    assert_eq!(failed["decision"], "return_original_receipt");
    assert_eq!(
        failed["receipt"]["message"],
        "launch_failed: agent_bridge:launch-text:invalid-request"
    );
    assert_eq!(launch_count.load(AtomicOrdering::SeqCst), 1);
}

#[tokio::test]
async fn fleet_launch_recovers_pending_reservation_after_gateway_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let first_state = state_for_agent_bridge(&tmp);
    let bootstrap = fleet_bootstrap(&first_state, &[FLEET_SCOPE_LAUNCH], None);
    let (enroll_status, enrolled) = json_response_with_state(
        first_state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[FLEET_SCOPE_LAUNCH],
            vec![1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let token = enrolled["token"].as_str().unwrap();
    let body = sample_fleet_launch_body(
        &bootstrap.pinned_installation_id,
        "dispatch-request-restart",
    );
    let request: sase_core::FleetLaunchRequestWire =
        serde_json::from_value(body.clone()).unwrap();
    let admission = first_state
        .fleet_launches
        .reserve(
            &request,
            &bootstrap.pinned_installation_id,
            current_unix_time(),
        )
        .unwrap();
    assert_eq!(
        admission.decision,
        sase_core::OperationDecisionKindWire::AcceptNew
    );
    assert_eq!(
        admission.receipt.state,
        sase_core::OperationReceiptStateWire::Pending
    );
    assert!(admission.should_launch);
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);

    let launch_count = Arc::new(AtomicUsize::new(0));
    let restarted_state = state_for_custom_agent_bridge(
        &tmp,
        Arc::new(FailingLaunchBridge {
            launch_count: launch_count.clone(),
        }),
    );

    let (status, recovered) =
        post_fleet_launch(restarted_state, token, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recovered["decision"], "return_original_receipt");
    assert_eq!(recovered["receipt"]["state"], "settled");
    assert_eq!(
        recovered["receipt"]["logical_locator"]["agent_id"],
        "mobile-demo"
    );
    assert!(recovered["receipt"]["instance_locator"].is_object());
    assert_eq!(launch_count.load(AtomicOrdering::SeqCst), 0);
}

fn mutation_body(
    summary: &sase_core::ResolvedAgentSummaryWire,
    installation_id: &str,
    kind: &str,
    operation_id: &str,
    extra: serde_json::Value,
) -> Value {
    let mut intent = json!({
        "schema_version": 1,
        "kind": kind,
        "target": summary.exact_locator.clone().expect("exact locator"),
        "row_revision": summary.row_revision.clone(),
        "reason": extra.get("reason").cloned().unwrap_or(Value::Null),
        "fork_prompt": extra.get("fork_prompt").cloned().unwrap_or(Value::Null),
        "kill_source_first": extra.get("kill_source_first").cloned().unwrap_or(Value::Null),
        "follow": extra.get("follow").and_then(Value::as_bool).unwrap_or(false)
    });
    if extra.get("run_id").is_some() {
        intent["target"]["run_id"] = extra["run_id"].clone();
    }
    if extra.get("revision").is_some() {
        intent["row_revision"]["revision"] = extra["revision"].clone();
    }
    let intent_wire: sase_core::FleetMutationIntentWire =
        serde_json::from_value(intent.clone()).unwrap();
    let fingerprint =
        sase_core::fleet_mutation_payload_fingerprint(&intent_wire).unwrap();
    json!({
        "schema_version": 1,
        "key": {
            "schema_version": 1,
            "controller_id": "controller-a",
            "operation_id": operation_id
        },
        "target_installation_id": installation_id,
        "intent": intent,
        "payload_fingerprint": fingerprint,
        "acceptance_window_seconds": 30.0
    })
}

async fn post_mutate(
    state: GatewayState,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = fleet_json_request(
        "POST",
        "/api/fleet/v1/mutate",
        Some(token),
        Some(body),
    );
    request.headers_mut().insert(
        FLEET_PROTOCOL_VERSIONS_HEADER,
        HeaderValue::from_static("1"),
    );
    json_response_with_state(state, request).await
}

async fn post_fleet_launch(
    state: GatewayState,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = fleet_json_request(
        "POST",
        "/api/fleet/v1/launch",
        Some(token),
        Some(body),
    );
    request.headers_mut().insert(
        FLEET_PROTOCOL_VERSIONS_HEADER,
        HeaderValue::from_static("1"),
    );
    json_response_with_state(state, request).await
}

async fn wait_for_launch_receipt_state(
    state: &GatewayState,
    token: &str,
    body: &Value,
    expected: &str,
) -> Value {
    for _ in 0..250 {
        let (status, value) =
            post_fleet_launch(state.clone(), token, body.clone()).await;
        assert_eq!(status, StatusCode::OK);
        if value["receipt"]["state"] == expected {
            return value;
        }
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    panic!("fleet launch receipt never reached state {expected}");
}

async fn wait_for_agents_changed(
    state: &GatewayState,
    reason: &str,
    agent_name: &str,
) {
    for _ in 0..50 {
        let Some(events) =
            state.event_hub.replay_after("0000000000000000").unwrap()
        else {
            tokio::time::sleep(StdDuration::from_millis(20)).await;
            continue;
        };
        if events.iter().any(|event| {
            matches!(
                &event.payload,
                EventPayloadWire::AgentsChanged {
                    reason: event_reason,
                    agent_name: Some(name),
                    timestamp: Some(_),
                } if event_reason == reason && name == agent_name
            )
        }) {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    panic!("agents_changed event {reason}/{agent_name} was not published");
}

#[tokio::test]
async fn fleet_mutate_denies_missing_scope() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let state = state_for_agent_bridge(&tmp);
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_HELLO]).await;
    let summary = first_summary(&state).await;
    let (status, body) = post_mutate(
        state,
        &token,
        mutation_body(&summary, &installation_id, "stop", "op-stop", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "scope_denied");
    assert_eq!(body["target"], FLEET_SCOPE_MUTATE);
}

#[tokio::test]
async fn fleet_mutate_stop_settles_and_replays() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let state = state_for_agent_bridge(&tmp);
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_MUTATE]).await;
    let summary = first_summary(&state).await;
    let body = mutation_body(
        &summary,
        &installation_id,
        "stop",
        "op-stop",
        json!({"reason": "user-stop"}),
    );
    let (status, mutate) =
        post_mutate(state.clone(), &token, body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(mutate["decision"], "accept_new");
    assert_eq!(mutate["receipt"]["state"], "settled");
    assert_eq!(mutate["receipt"]["outcome"], "applied");
    let (replay_status, replay) =
        post_mutate(state.clone(), &token, body).await;
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replay["decision"], "return_original_receipt");
    assert_eq!(replay["receipt"], mutate["receipt"]);
    let events = state
        .event_hub
        .replay_after("0000000000000000")
        .unwrap()
        .unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::AgentsChanged {
            reason,
            agent_name: Some(name),
            timestamp: Some(_),
        } if reason == "fleet_mutate" && name == "mobile-demo"
    )));
}

#[tokio::test]
async fn fleet_mutate_changed_payload_conflicts() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let state = state_for_agent_bridge(&tmp);
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_MUTATE]).await;
    let summary = first_summary(&state).await;
    let first = mutation_body(
        &summary,
        &installation_id,
        "stop",
        "op-stop",
        json!({"reason": "first"}),
    );
    let (status, _) = post_mutate(state.clone(), &token, first).await;
    assert_eq!(status, StatusCode::OK);
    let second = mutation_body(
        &summary,
        &installation_id,
        "stop",
        "op-stop",
        json!({"reason": "second"}),
    );
    let (status, _body) = post_mutate(state, &token, second).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn fleet_mutate_refuses_stale_revision_and_superseded_instance() {
    let tmp = tempfile::tempdir().unwrap();
    let artifact = seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let running_path = artifact.join("running.json");
    let running_before = std::fs::read(&running_path).unwrap();
    let state = state_for_agent_bridge(&tmp);
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_MUTATE]).await;
    let summary = first_summary(&state).await;
    let stale = mutation_body(
        &summary,
        &installation_id,
        "stop",
        "op-stale",
        json!({"revision": 99, "reason": "stale"}),
    );
    let (status, body) = post_mutate(state.clone(), &token, stale).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(body["code"], "gone_stale");
    // "other-run" reuses the seeded agent's logical name/PID but claims a
    // different run_id, i.e. an exact locator for an instance that has
    // since been replaced under the same logical key.
    let superseded = mutation_body(
        &summary,
        &installation_id,
        "stop",
        "op-instance",
        json!({"run_id": "other-run", "reason": "old"}),
    );
    let (status, body) = post_mutate(state.clone(), &token, superseded).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "conflict_already_handled");

    // Zero lifecycle side effects on the replacement: the rejected old-
    // instance mutations must not have touched the real (current) agent's
    // on-disk state, and the real instance's own exact locator must still
    // be mutable normally afterward, proving nothing about its mutation
    // path was consumed, locked, or corrupted by the rejected attempts.
    let running_after = std::fs::read(&running_path).unwrap();
    assert_eq!(
        running_before, running_after,
        "a rejected mutation against a superseded instance must not \
         modify the real replacement's on-disk lifecycle state",
    );
    let accepted = mutation_body(
        &summary,
        &installation_id,
        "stop",
        "op-real",
        json!({"reason": "real"}),
    );
    let (status, body) = post_mutate(state, &token, accepted).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the real replacement instance must still accept a mutation \
         against its own current exact locator: {body}",
    );
}

#[tokio::test]
async fn fleet_mutate_refuses_terminal_missing_capability_and_bridge_failure() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", false, false);
    let state = state_for_agent_bridge(&tmp);
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_MUTATE]).await;
    let summary = first_summary(&state).await;
    let (status, body) = post_mutate(
        state,
        &token,
        mutation_body(
            &summary,
            &installation_id,
            "stop",
            "op-term",
            json!({"reason": "late"}),
        ),
    )
    .await;
    assert!(
        status == StatusCode::FORBIDDEN || status == StatusCode::CONFLICT,
        "{status} {body}"
    );

    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, true);
    let state = state_for_agent_bridge(&tmp);
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_MUTATE]).await;
    let summary = first_summary(&state).await;
    let (status, body) = post_mutate(
        state,
        &token,
        mutation_body(
            &summary,
            &installation_id,
            "stop",
            "op-cap",
            json!({"reason": "nope"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "unsupported_action");

    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_MUTATE]).await;
    let summary = first_summary(&state).await;
    let (status, body) = post_mutate(
        state,
        &token,
        mutation_body(
            &summary,
            &installation_id,
            "stop",
            "op-bridge",
            json!({"reason": "bridge"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "bridge_unavailable");
}
