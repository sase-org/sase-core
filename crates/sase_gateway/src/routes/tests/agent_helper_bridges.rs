use axum::body::Body;
use axum::http::StatusCode;

use chrono::Utc;

use crate::host_bridge::HostBridgeError;

use crate::wire::{ApiErrorCodeWire, EventPayloadWire};

use axum::http::Request;

use chrono::Duration;

use serde_json::{json, Value};

use tower::ServiceExt;

use super::super::errors::ApiError;
use super::super::router::app_with_state;

use super::support::*;

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_changespec_tags_returns_command_output() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "success");
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/changespec-tags?project=sase&limit=2")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["result"]["status"], "partial_success");
    assert_eq!(value["tags"][0]["tag"], "#gh:feature");
    assert_eq!(value["result"]["skipped"][0]["target"], "sase/skipped");

    let bridge_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path()
                .join("mobile-helper-bridge-success.changespec-tags.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(bridge_request["project"], "sase");
    assert_eq!(bridge_request["limit"], 2);
    assert_eq!(
        bridge_request["device_id"].as_str(),
        Some(device_id.as_str())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_xprompt_catalog_returns_new_helper_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "success");
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/xprompts/catalog?project=sase&limit=2")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["result"]["status"], "success");
    assert_eq!(value["entries"][0]["name"], "bd/work_phase_bead");
    assert_eq!(value["entries"][0]["insertion"], "#bd/work_phase_bead");
    assert_eq!(value["entries"][0]["reference_prefix"], "#");
    assert_eq!(value["entries"][0]["kind"], "xprompt");
    assert_eq!(value["entries"][0]["inputs"][0]["name"], "bead_id");
    assert_eq!(value["entries"][0]["inputs"][0]["type"], "word");
    assert_eq!(value["entries"][0]["inputs"][0]["required"], true);
    assert_eq!(
        value["entries"][0]["inputs"][0]["default_display"],
        Value::Null
    );

    let bridge_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path()
                .join("mobile-helper-bridge-success.xprompt-catalog.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(bridge_request["project"], "sase");
    assert_eq!(bridge_request["limit"], 2);
    assert_eq!(
        bridge_request["device_id"].as_str(),
        Some(device_id.as_str())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_update_start_returns_command_output() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "success");
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .method("POST")
            .uri("/api/v1/update/start")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"schema_version":1,"request_id":"req_1"}"#))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["result"]["status"], "success");
    assert_eq!(value["job"]["job_id"], "job_123");
    assert_eq!(value["job"]["status"], "running");

    let bridge_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path()
                .join("mobile-helper-bridge-success.update-start.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(bridge_request["request_id"], "req_1");
    assert_eq!(
        bridge_request["device_id"].as_str(),
        Some(device_id.as_str())
    );
    assert!(bridge_request.get("command").is_none());
    assert!(bridge_request.get("workspace").is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_update_status_returns_command_output() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "success");
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/update/job_123")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["result"]["status"], "success");
    assert_eq!(value["job"]["job_id"], "job_123");
    assert_eq!(value["job"]["status"], "succeeded");

    let bridge_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path()
                .join("mobile-helper-bridge-success.update-status.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(bridge_request["job_id"], "job_123");
    assert_eq!(
        bridge_request["device_id"].as_str(),
        Some(device_id.as_str())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_update_exit_codes_map_to_stable_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "not-found");
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;
    let auth = format!("Bearer {token}");

    let (start_status, start_value) = json_response_with_state(
        state.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/update/start")
            .header("authorization", auth.clone())
            .header("content-type", "application/json")
            .body(Body::from(r#"{"schema_version":1}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(start_status, StatusCode::CONFLICT);
    assert_eq!(start_value["code"], "update_already_running");

    let (status_status, status_value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/update/missing")
            .header("authorization", auth)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status_status, StatusCode::NOT_FOUND);
    assert_eq!(status_value["code"], "update_job_not_found");
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_malformed_json_maps_to_unavailable() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "invalid-json");
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/changespec-tags")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(value["code"], "bridge_unavailable");
    assert_eq!(
        value["target"],
        "helper_bridge:changespec-tags:invalid_json"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_exit_failure_maps_to_unavailable() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "exit-failure");
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/changespec-tags")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(value["code"], "bridge_unavailable");
    assert_eq!(value["target"], "helper_bridge:changespec-tags");
}

#[cfg(unix)]
#[tokio::test]
async fn command_helper_bridge_not_found_maps_to_helper_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_helper_bridge(&tmp, "not-found");
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/beads/missing")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "helper_not_found");
    assert_eq!(value["target"], "helper_bridge:beads-show");
}
#[tokio::test]
async fn invalid_and_revoked_tokens_are_unauthorized() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (missing_status, missing) =
        json_response_with_state(state.clone(), session_request(None)).await;
    assert_eq!(missing_status, StatusCode::UNAUTHORIZED);
    assert_eq!(missing["code"], "unauthorized");

    let (invalid_status, invalid) = json_response_with_state(
        state.clone(),
        session_request(Some("not-a-real-token")),
    )
    .await;
    assert_eq!(invalid_status, StatusCode::UNAUTHORIZED);
    assert_eq!(invalid["code"], "unauthorized");

    state
        .token_store()
        .revoke_device(&device_id, Utc::now())
        .unwrap();
    let (revoked_status, revoked) =
        json_response_with_state(state, session_request(Some(&token))).await;
    assert_eq!(revoked_status, StatusCode::UNAUTHORIZED);
    assert_eq!(revoked["code"], "unauthorized");
}

#[tokio::test]
async fn events_without_token_returns_typed_unauthorized_error() {
    let (status, value) = json_response(events_request(None, None)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
    assert_eq!(value["target"], "authorization");
}

#[tokio::test]
async fn events_with_token_returns_sse_response() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let response = app_with_state(state)
        .oneshot(events_request(Some(&token), None))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
}

#[tokio::test]
async fn agents_without_token_returns_typed_unauthorized_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_agent_bridge(&tmp);

    let (status, value) = json_response_with_state(
        state,
        agent_get_request(None, "/api/v1/agents"),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
    assert_eq!(value["target"], "authorization");
}

#[tokio::test]
async fn production_agent_bridge_returns_typed_unavailable_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        agent_get_request(Some(&token), "/api/v1/agents"),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(value["code"], "bridge_unavailable");
    assert_eq!(value["target"], "agent_bridge");
}

#[tokio::test]
async fn fake_agent_bridge_routes_return_stable_success_shapes() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_agent_bridge(&tmp);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (list_status, list) = json_response_with_state(
        state.clone(),
        agent_get_request(
            Some(&token),
            "/api/v1/agents?include_recent=true&limit=10",
        ),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list["schema_version"], 1);
    assert_eq!(list["total_count"], 1);
    assert_eq!(list["agents"][0]["name"], "mobile-demo");
    assert_eq!(list["agents"][0]["actions"]["can_kill"], true);

    let (resume_status, resume) = json_response_with_state(
        state.clone(),
        agent_get_request(Some(&token), "/api/v1/agents/resume-options"),
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK);
    assert_eq!(resume["options"][0]["prompt_text"], "#resume:mobile-demo");

    let launch_body = json!({
        "schema_version": 1,
        "prompt": "Implement mobile gateway agent route tests",
        "display_name": "Mobile demo",
        "name": "mobile-demo",
        "model": "gpt-5.6-sol",
        "provider": "codex",
        "runtime": "codex",
        "project": "sase",
        "dry_run": false
    });
    let (launch_status, launch) = json_response_with_state(
        state.clone(),
        agent_post_request(Some(&token), "/api/v1/agents/launch", launch_body),
    )
    .await;
    assert_eq!(launch_status, StatusCode::OK);
    assert_eq!(launch["primary"]["name"], "mobile-demo");
    assert_eq!(launch["slots"][0]["status"], "launched");

    let image_body = json!({
        "schema_version": 1,
        "prompt": "Review this screenshot",
        "original_filename": "screen.png",
        "content_type": "image/png",
        "byte_length": 8,
        "base64_image": "iVBORw0K",
        "display_name": null,
        "name": "mobile-demo",
        "model": null,
        "provider": null,
        "runtime": null,
        "project": "sase",
        "dry_run": false
    });
    let (image_status, image) = json_response_with_state(
        state.clone(),
        agent_post_request(
            Some(&token),
            "/api/v1/agents/launch-image",
            image_body,
        ),
    )
    .await;
    assert_eq!(image_status, StatusCode::OK);
    assert_eq!(image["primary"]["name"], "mobile-demo");

    let (kill_status, kill) = json_response_with_state(
        state.clone(),
        agent_post_request(
            Some(&token),
            "/api/v1/agents/mobile-demo/kill",
            json!({"schema_version": 1, "reason": "mobile"}),
        ),
    )
    .await;
    assert_eq!(kill_status, StatusCode::OK);
    assert_eq!(kill["name"], "mobile-demo");
    assert_eq!(kill["changed"], true);

    let (retry_status, retry) = json_response_with_state(
        state.clone(),
        agent_post_request(
            Some(&token),
            "/api/v1/agents/mobile-demo/retry",
            json!({"schema_version": 1, "prompt_override": null, "dry_run": false}),
        ),
    )
    .await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry["source_agent"], "mobile-demo");
    assert_eq!(retry["launch"]["primary"]["name"], "mobile-demo");

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
        } if reason == "launch" && name == "mobile-demo"
    )));
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::AgentsChanged {
            reason,
            agent_name: Some(name),
            timestamp: Some(_),
        } if reason == "kill" && name == "mobile-demo"
    )));
}

#[tokio::test]
async fn helper_routes_without_token_return_typed_unauthorized_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_helper_bridge(&tmp);
    let requests = [
        Request::builder()
            .uri("/api/v1/changespec-tags")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/api/v1/patch-tags")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/api/v1/xprompts/catalog")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/api/v1/beads")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/api/v1/beads/sase-26.4.1")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/update/start")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"schema_version":1}"#))
            .unwrap(),
        Request::builder()
            .uri("/api/v1/update/job_123")
            .body(Body::empty())
            .unwrap(),
    ];

    for request in requests {
        let (status, value) =
            json_response_with_state(state.clone(), request).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(value["code"], "unauthorized");
        assert_eq!(value["target"], "authorization");
    }
}

#[tokio::test]
async fn production_helper_bridge_returns_typed_unavailable_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        Request::builder()
            .uri("/api/v1/changespec-tags")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(value["code"], "bridge_unavailable");
    assert_eq!(value["target"], "helper_bridge");
}

#[test]
fn helper_host_bridge_errors_map_to_stable_api_codes() {
    let cases = [
        (
            HostBridgeError::HelperNotFound("xprompt:missing".to_string()),
            StatusCode::NOT_FOUND,
            ApiErrorCodeWire::HelperNotFound,
            "xprompt:missing",
        ),
        (
            HostBridgeError::UpdateAlreadyRunning("update".to_string()),
            StatusCode::CONFLICT,
            ApiErrorCodeWire::UpdateAlreadyRunning,
            "update",
        ),
        (
            HostBridgeError::UpdateJobNotFound("job_404".to_string()),
            StatusCode::NOT_FOUND,
            ApiErrorCodeWire::UpdateJobNotFound,
            "job_404",
        ),
    ];

    for (error, status, code, target) in cases {
        let api_error = ApiError::from_host_bridge(error);
        assert_eq!(api_error.status, status);
        assert_eq!(api_error.wire.code, code);
        assert_eq!(api_error.wire.target.as_deref(), Some(target));
    }
}

#[tokio::test]
async fn fake_helper_bridge_routes_return_stable_success_shapes() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_helper_bridge(&tmp);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;
    let auth = format!("Bearer {token}");

    let (tags_status, tags) = json_response_with_state(
        state.clone(),
        Request::builder()
            .uri("/api/v1/changespec-tags?project=sase&limit=10")
            .header("authorization", auth.clone())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(tags_status, StatusCode::OK);
    assert_eq!(tags["result"]["status"], "success");
    assert_eq!(tags["tags"][0]["tag"], "#gh:feature");
    assert_eq!(tags["tags"][0]["changespec"], "feature");
    assert!(tags["tags"][0].get("patch").is_none());

    let (patch_tags_status, patch_tags) = json_response_with_state(
        state.clone(),
        Request::builder()
            .uri("/api/v1/patch-tags?project=sase&limit=10")
            .header("authorization", auth.clone())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(patch_tags_status, StatusCode::OK);
    assert_eq!(patch_tags["result"]["status"], "success");
    assert_eq!(patch_tags["tags"][0]["tag"], "#gh:feature");
    assert_eq!(patch_tags["tags"][0]["patch"], "feature");
    assert!(patch_tags["tags"][0].get("changespec").is_none());

    let (catalog_status, catalog) = json_response_with_state(
        state.clone(),
        Request::builder()
            // Legacy `changespec` tag filters remain accepted for compatibility.
            .uri("/api/v1/xprompts/catalog?project=sase&tag=changespec")
            .header("authorization", auth.clone())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(catalog_status, StatusCode::OK);
    assert_eq!(catalog["entries"][0]["name"], "gh");
    assert_eq!(catalog["entries"][0]["insertion"], "#!gh");
    assert_eq!(catalog["entries"][0]["reference_prefix"], "#!");
    assert_eq!(catalog["entries"][0]["kind"], "workflow");
    assert_eq!(catalog["entries"][0]["inputs"][0]["name"], "topic");
    assert_eq!(catalog["entries"][0]["inputs"][0]["type"], "word");
    assert_eq!(catalog["entries"][0]["inputs"][0]["required"], true);
    assert_eq!(catalog["stats"]["total_count"], 1);

    let (beads_status, beads) = json_response_with_state(
        state.clone(),
        Request::builder()
            .uri("/api/v1/beads?project=sase&status=in_progress")
            .header("authorization", auth.clone())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(beads_status, StatusCode::OK);
    assert_eq!(beads["beads"][0]["id"], "sase-26.4.1");

    let (bead_status, bead) = json_response_with_state(
        state.clone(),
        Request::builder()
            .uri("/api/v1/beads/sase-26.4.1?project=sase")
            .header("authorization", auth.clone())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(bead_status, StatusCode::OK);
    assert_eq!(bead["bead"]["summary"]["title"], "Rust helper skeleton");

    let (start_status, start) = json_response_with_state(
        state.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/update/start")
            .header("authorization", auth.clone())
            .header("content-type", "application/json")
            .body(Body::from(r#"{"schema_version":1}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(start_status, StatusCode::OK);
    assert_eq!(start["job"]["job_id"], "job_123");
    assert_eq!(start["job"]["status"], "running");

    let (update_status, update) = json_response_with_state(
        state.clone(),
        Request::builder()
            .uri("/api/v1/update/job_123")
            .header("authorization", auth)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(update_status, StatusCode::OK);
    assert_eq!(update["job"]["job_id"], "job_123");

    let events = state
        .event_hub
        .replay_after("0000000000000000")
        .unwrap()
        .unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::HelpersChanged {
            reason,
            helper: Some(helper),
            job_id: Some(job_id),
            timestamp: Some(_),
        } if reason == "update_start"
            && helper == "update"
            && job_id == "job_123"
    )));
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::HelpersChanged {
            reason,
            helper: Some(helper),
            job_id: Some(job_id),
            timestamp: Some(_),
        } if reason == "update_status"
            && helper == "update"
            && job_id == "job_123"
    )));
}

#[cfg(unix)]
#[tokio::test]
async fn command_agent_bridge_routes_return_command_output() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_command_agent_bridge(&tmp);
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (list_status, list) = json_response_with_state(
        state.clone(),
        agent_get_request(Some(&token), "/api/v1/agents"),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list["agents"][0]["name"], "cmd-demo");

    let (resume_status, resume) = json_response_with_state(
        state.clone(),
        agent_get_request(Some(&token), "/api/v1/agents/resume-options"),
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK);
    assert_eq!(resume["options"][0]["prompt_text"], "#resume:cmd-demo\n");

    let launch_body = json!({
        "schema_version": 1,
        "prompt": "Do work",
        "request_id": "req-text-1",
        "display_name": null,
        "name": null,
        "model": null,
        "provider": null,
        "runtime": null,
        "project": null,
        "dry_run": null,
    });
    let (launch_status, launch) = json_response_with_state(
        state.clone(),
        agent_post_request(Some(&token), "/api/v1/agents/launch", launch_body),
    )
    .await;
    assert_eq!(launch_status, StatusCode::OK);
    assert_eq!(launch["primary"]["name"], "cmd-demo");
    assert_eq!(launch["slots"][0]["status"], "launched");
    let launch_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path().join("mobile-agent-bridge.launch-text.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(launch_request["request_id"], "req-text-1");
    assert_eq!(
        launch_request["device_id"].as_str(),
        Some(device_id.as_str())
    );

    let image_body = json!({
        "schema_version": 1,
        "prompt": "Review",
        "request_id": "req-image-1",
        "original_filename": "screen.png",
        "content_type": "image/png",
        "byte_length": 8,
        "base64_image": "iVBORw0K",
        "display_name": null,
        "name": null,
        "model": null,
        "provider": null,
        "runtime": null,
        "project": null,
        "dry_run": null,
    });
    let (image_status, image) = json_response_with_state(
        state.clone(),
        agent_post_request(
            Some(&token),
            "/api/v1/agents/launch-image",
            image_body,
        ),
    )
    .await;
    assert_eq!(image_status, StatusCode::OK);
    assert_eq!(image["primary"]["name"], "cmd-image");
    let image_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path().join("mobile-agent-bridge.launch-image.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(image_request["request_id"], "req-image-1");
    assert_eq!(
        image_request["device_id"].as_str(),
        Some(device_id.as_str())
    );

    let (kill_status, kill) = json_response_with_state(
        state.clone(),
        agent_post_request(
            Some(&token),
            "/api/v1/agents/cmd-demo/kill",
            json!({"schema_version": 1, "reason": "mobile", "device_id": "ignored"}),
        ),
    )
    .await;
    assert_eq!(kill_status, StatusCode::OK);
    assert_eq!(kill["name"], "cmd-demo");
    assert_eq!(kill["pid"], 4242);
    assert_eq!(kill["changed"], true);

    let (retry_status, retry) = json_response_with_state(
        state,
        agent_post_request(
            Some(&token),
            "/api/v1/agents/cmd-demo/retry",
            json!({
                "schema_version": 1,
                "request_id": "req-retry-1",
                "prompt_override": null,
                "dry_run": false,
                "kill_source_first": false,
                "device_id": "ignored",
            }),
        ),
    )
    .await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry["source_agent"], "cmd-demo");
    assert_eq!(retry["launch"]["primary"]["name"], "cmd-demo.1");
    let retry_request: Value = serde_json::from_str(
        &std::fs::read_to_string(
            tmp.path().join("mobile-agent-bridge.retry-agent.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(retry_request["request_id"], "req-retry-1");
    assert_eq!(
        retry_request["device_id"].as_str(),
        Some(device_id.as_str())
    );
}
