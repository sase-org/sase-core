use std::time::Duration as StdDuration;

use crate::push::PushConfig;
use axum::body::Body;
use axum::http::StatusCode;

use crate::wire::{
    FLEET_API_WIRE_SCHEMA_VERSION, FLEET_PROTOCOL_VERSION,
    GATEWAY_WIRE_SCHEMA_VERSION,
};

use axum::http::Request;

use chrono::Duration;

use serde_json::{json, Value};

use super::super::state::{
    GatewayState, GatewayStateOptions, DEFAULT_EVENT_BUFFER_CAPACITY,
    DEFAULT_MAX_ATTACHMENT_BYTES, FLEET_ENROLLMENT_RATE_LIMIT,
    FLEET_PROTOCOL_VERSIONS_HEADER, FLEET_REQUEST_BODY_LIMIT_BYTES,
};

use super::super::support::default_attachment_token_ttl;
use super::super::support::publish_agents_changed;

use super::support::*;

#[tokio::test]
async fn fleet_rotation_and_revocation_reject_stale_or_revoked_tokens() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let bootstrap = fleet_bootstrap(&state, &[], None);
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[],
            vec![FLEET_PROTOCOL_VERSION],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    let old_token = enrolled["token"].as_str().unwrap().to_string();

    let (rotate_status, rotate) = json_response_with_state(
        state.clone(),
        fleet_json_request(
            "POST",
            "/api/fleet/v1/credential/rotate",
            Some(&old_token),
            Some(json!({
                "schema_version": GATEWAY_WIRE_SCHEMA_VERSION,
                "supported_protocol_versions": [FLEET_PROTOCOL_VERSION]
            })),
        ),
    )
    .await;
    assert_eq!(rotate_status, StatusCode::OK);
    let new_token = rotate["token"].as_str().unwrap().to_string();
    assert_ne!(new_token, old_token);

    let (old_status, old_response) = json_response_with_state(
        state.clone(),
        fleet_json_request(
            "GET",
            "/api/fleet/v1/hello",
            Some(&old_token),
            None,
        ),
    )
    .await;
    assert_eq!(old_status, StatusCode::UNAUTHORIZED);
    assert_eq!(old_response["code"], "unauthorized");

    let (revoke_status, revoke) = json_response_with_state(
        state.clone(),
        fleet_json_request(
            "POST",
            "/api/fleet/v1/credential/revoke",
            Some(&new_token),
            Some(json!({
                "schema_version": FLEET_API_WIRE_SCHEMA_VERSION,
                "reason": "controller retired"
            })),
        ),
    )
    .await;
    assert_eq!(revoke_status, StatusCode::OK);
    assert_eq!(revoke["revoked"], true);
    assert_eq!(revoke["credential"]["revoked_reason"], "controller retired");

    let (revoked_status, revoked) = json_response_with_state(
        state,
        fleet_json_request(
            "GET",
            "/api/fleet/v1/hello",
            Some(&new_token),
            None,
        ),
    )
    .await;
    assert_eq!(revoked_status, StatusCode::UNAUTHORIZED);
    assert_eq!(revoked["code"], "credential_revoked");
}

#[tokio::test]
async fn fleet_protocol_negotiation_rejects_incompatible_versions() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let bootstrap = fleet_bootstrap(&state, &[], None);
    let (status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            &[],
            vec![FLEET_PROTOCOL_VERSION + 1, FLEET_PROTOCOL_VERSION],
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(enrolled["protocol_version"], FLEET_PROTOCOL_VERSION);
    let token = enrolled["token"].as_str().unwrap().to_string();

    let (hello_status, hello) = json_response_with_state(
        state.clone(),
        Request::builder()
            .uri("/api/fleet/v1/hello")
            .header("authorization", format!("Bearer {token}"))
            .header(
                FLEET_PROTOCOL_VERSIONS_HEADER,
                (FLEET_PROTOCOL_VERSION + 1).to_string(),
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(hello_status, StatusCode::UPGRADE_REQUIRED);
    assert_eq!(hello["code"], "incompatible_protocol");

    let incompatible_bootstrap = fleet_bootstrap(&state, &[], None);
    let (enroll_status, enroll) = json_response_with_state(
        state,
        fleet_enroll_request(fleet_enroll_body(
            &incompatible_bootstrap,
            &[],
            vec![FLEET_PROTOCOL_VERSION + 1],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::UPGRADE_REQUIRED);
    assert_eq!(enroll["code"], "incompatible_protocol");
}

#[tokio::test]
async fn fleet_installation_pin_mismatch_returns_quarantine_response() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let bootstrap = fleet_bootstrap(&state, &[], None);
    let mut body =
        fleet_enroll_body(&bootstrap, &[], vec![FLEET_PROTOCOL_VERSION]);
    body["pinned_installation_id"] = json!("sase_inst_v1_deadbeef");

    let (status, value) =
        json_response_with_state(state, fleet_enroll_request(body)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(value["outcome"], "quarantined");
    assert_eq!(value["token"], Value::Null);
    assert_eq!(value["quarantine"]["reason"], "installation_pin_mismatch");
    assert_eq!(
        value["quarantine"]["authoritative_installation_id"],
        bootstrap.pinned_installation_id
    );
}

#[tokio::test]
async fn fleet_enrollment_attempts_are_rate_limited() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let invalid_body = json!({
        "schema_version": FLEET_API_WIRE_SCHEMA_VERSION,
        "bootstrap_id": "missing",
        "bootstrap_secret": "wrong",
        "controller": {
            "schema_version": 1,
            "controller_id": "controller-a",
            "display_name": null,
            "platform": null,
            "app_version": null
        },
        "requested_scopes": [],
        "supported_protocol_versions": [FLEET_PROTOCOL_VERSION],
        "pinned_installation_id": "sase_inst_v1_deadbeef"
    });

    for _ in 0..FLEET_ENROLLMENT_RATE_LIMIT {
        let (status, value) = json_response_with_state(
            state.clone(),
            fleet_enroll_request(invalid_body.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(value["code"], "bootstrap_rejected");
    }
    let (status, value) =
        json_response_with_state(state, fleet_enroll_request(invalid_body))
            .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(value["code"], "rate_limited");
}

#[tokio::test]
async fn fleet_request_body_limit_rejects_large_enrollment_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let oversized = json!({
        "schema_version": FLEET_API_WIRE_SCHEMA_VERSION,
        "bootstrap_id": "missing",
        "bootstrap_secret": "x".repeat(FLEET_REQUEST_BODY_LIMIT_BYTES + 1),
        "controller": {
            "schema_version": 1,
            "controller_id": "controller-a",
            "display_name": null,
            "platform": null,
            "app_version": null
        },
        "requested_scopes": [],
        "supported_protocol_versions": [FLEET_PROTOCOL_VERSION],
        "pinned_installation_id": "sase_inst_v1_deadbeef"
    });

    let (status, value) =
        json_response_with_state(state, fleet_enroll_request(oversized)).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(value["code"], "payload_too_large");
}

#[tokio::test]
async fn health_route_returns_stable_record() {
    let (status, value) = json_response(
        Request::builder()
            .uri("/api/v1/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        value,
        json!({
            "schema_version": 1,
            "status": "ok",
            "service": "sase_gateway",
            "version": env!("CARGO_PKG_VERSION"),
            "build": {
                "package_version": env!("CARGO_PKG_VERSION"),
                "git_sha": null
            },
            "bind": {
                "address": "127.0.0.1:0",
                "is_loopback": true
            },
            "push": {
                "provider": "disabled",
                "enabled": false,
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "last_attempt_at": null,
                "last_success_at": null,
                "last_failure_at": null,
                "last_failure": null
            },
            "fleet": {
                "supported_protocol_versions": [crate::wire::FLEET_PROTOCOL_VERSION]
            }
        })
    );
}

#[tokio::test]
async fn session_without_token_returns_typed_unauthorized_error() {
    let (status, value) = json_response(
        Request::builder()
            .uri("/api/v1/session")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        value,
        json!({
            "schema_version": 1,
            "code": "unauthorized",
            "message": "authentication is required for this endpoint",
            "target": "authorization",
            "details": null
        })
    );
}

#[tokio::test]
async fn pair_start_returns_short_lived_code_without_token() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));

    let (status, value) = json_response_with_state(
        state,
        json_request(
            "POST",
            "/api/v1/session/pair/start",
            json!({
                "schema_version": 1,
                "host_label": "workstation"
            }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["schema_version"], GATEWAY_WIRE_SCHEMA_VERSION);
    assert!(value["pairing_id"].as_str().unwrap().starts_with("pair_"));
    assert_eq!(value["code"].as_str().unwrap().len(), 6);
    assert_eq!(value["host_label"], "workstation");
    assert!(value.get("token").is_none());
}

#[tokio::test]
async fn pair_finish_persists_device_and_returns_token_once() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));

    let (start, finish, token, _device_id) = pair_device(state.clone()).await;

    assert_eq!(finish["schema_version"], 1);
    assert_eq!(finish["token_type"], "bearer");
    assert!(token.starts_with("sase_mobile_"));
    assert_eq!(finish["device"]["display_name"], "Pixel 9");
    assert_eq!(finish["device"]["platform"], "android");

    let devices = std::fs::read_to_string(
        tmp.path().join("mobile_gateway").join("devices.json"),
    )
    .unwrap();
    let audit = std::fs::read_to_string(
        tmp.path().join("mobile_gateway").join("audit.jsonl"),
    )
    .unwrap();
    assert!(!devices.contains(&token));
    assert!(!audit.contains(&token));
    assert!(devices.contains("token_hash"));

    let reuse_body = json!({
        "schema_version": 1,
        "pairing_id": start["pairing_id"],
        "code": start["code"],
        "device": {
            "display_name": "Pixel 9",
            "platform": "android",
            "app_version": null
        }
    });
    let (status, value) = json_response_with_state(
        state,
        json_request("POST", "/api/v1/session/pair/finish", reuse_body),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "pairing_rejected");
}

#[tokio::test]
async fn pair_finish_rejects_expired_code() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::seconds(-1));
    let (start_status, start) = json_response_with_state(
        state.clone(),
        json_request(
            "POST",
            "/api/v1/session/pair/start",
            json!({
                "schema_version": 1,
                "host_label": null
            }),
        ),
    )
    .await;
    assert_eq!(start_status, StatusCode::OK);

    let finish_body = json!({
        "schema_version": 1,
        "pairing_id": start["pairing_id"],
        "code": start["code"],
        "device": {
            "display_name": "Pixel",
            "platform": "android",
            "app_version": null
        }
    });
    let (status, value) = json_response_with_state(
        state,
        json_request("POST", "/api/v1/session/pair/finish", finish_body),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["code"], "pairing_expired");
}

#[tokio::test]
async fn session_returns_authenticated_device() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let (status, value) =
        json_response_with_state(state, session_request(Some(&token))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["schema_version"], GATEWAY_WIRE_SCHEMA_VERSION);
    assert_eq!(value["device"]["device_id"], device_id);
    assert_eq!(
        value["capabilities"],
        json!([
            "session.read",
            "events.read",
            "agents.read",
            "agents.launch",
            "agents.lifecycle.write",
            "helpers.read",
            "update.write",
            "attachments.download",
            "notifications.state.write",
            "push_subscriptions.read",
            "push_subscriptions.write"
        ])
    );
}

#[tokio::test]
async fn push_subscriptions_require_auth() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));

    let (status, value) =
        json_response_with_state(state, push_subscription_get_request(None))
            .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
    assert_eq!(value["target"], "authorization");
}

#[tokio::test]
async fn push_subscription_register_list_and_revoke_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;

    let request_body = json!({
        "schema_version": 1,
        "provider": "fcm",
        "provider_token": "opaque-fcm-token",
        "app_instance_id": "app_instance_pixel",
        "device_display_name": "Pixel 9",
        "platform": "android",
        "app_version": "0.1.0",
        "hint_categories": ["notifications", "agents", "update"]
    });
    let (register_status, register) = json_response_with_state(
        state.clone(),
        push_subscription_post_request(Some(&token), request_body),
    )
    .await;
    assert_eq!(register_status, StatusCode::OK);
    assert_eq!(register["created"], true);
    assert_eq!(register["subscription"]["schema_version"], 1);
    assert_eq!(register["subscription"]["device_id"], device_id);
    assert_eq!(register["subscription"]["provider"], "fcm");
    assert_eq!(
        register["subscription"]["provider_token"],
        "opaque-fcm-token"
    );
    assert_eq!(
        register["subscription"]["hint_categories"],
        json!(["notifications", "agents", "update"])
    );
    assert!(register["subscription"]["enabled_at"].is_string());
    assert!(register["subscription"]["disabled_at"].is_null());
    let subscription_id =
        register["subscription"]["id"].as_str().unwrap().to_string();

    let (list_status, list) = json_response_with_state(
        state.clone(),
        push_subscription_get_request(Some(&token)),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list["subscriptions"].as_array().unwrap().len(), 1);
    assert_eq!(list["subscriptions"][0]["id"], subscription_id);

    let (delete_status, delete) = json_response_with_state(
        state.clone(),
        push_subscription_delete_request(Some(&token), &subscription_id),
    )
    .await;
    assert_eq!(delete_status, StatusCode::OK);
    assert_eq!(delete["revoked"], true);
    assert_eq!(delete["subscription"]["id"], subscription_id);
    assert!(delete["subscription"]["disabled_at"].is_string());

    let (empty_status, empty) = json_response_with_state(
        state,
        push_subscription_get_request(Some(&token)),
    )
    .await;
    assert_eq!(empty_status, StatusCode::OK);
    assert_eq!(empty["subscriptions"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn push_subscription_duplicate_updates_existing_record() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;
    let initial = json!({
        "schema_version": 1,
        "provider": "test",
        "provider_token": "same-token",
        "app_instance_id": "same-app",
        "device_display_name": "Pixel",
        "platform": "android",
        "app_version": "0.1.0",
        "hint_categories": ["notifications"]
    });
    let (first_status, first) = json_response_with_state(
        state.clone(),
        push_subscription_post_request(Some(&token), initial),
    )
    .await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(first["created"], true);
    let id = first["subscription"]["id"].as_str().unwrap().to_string();

    let update = json!({
        "schema_version": 1,
        "provider": "test",
        "provider_token": "same-token",
        "app_instance_id": "same-app",
        "device_display_name": "Pixel",
        "platform": "android",
        "app_version": "0.2.0",
        "hint_categories": ["agents", "helpers"]
    });
    let (second_status, second) = json_response_with_state(
        state,
        push_subscription_post_request(Some(&token), update),
    )
    .await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second["created"], false);
    assert_eq!(second["subscription"]["id"], id);
    assert_eq!(second["subscription"]["app_version"], "0.2.0");
    assert_eq!(
        second["subscription"]["hint_categories"],
        json!(["agents", "helpers"])
    );
    assert!(second["subscription"]["last_seen_at"].is_string());
}

#[tokio::test]
async fn push_subscription_validation_and_audit_do_not_leak_provider_token() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (invalid_status, invalid) = json_response_with_state(
        state.clone(),
        push_subscription_post_request(
            Some(&token),
            json!({
                "schema_version": 1,
                "provider": "fcm",
                "provider_token": "",
                "app_instance_id": null,
                "device_display_name": null,
                "platform": "android",
                "app_version": null,
                "hint_categories": ["notifications"]
            }),
        ),
    )
    .await;
    assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid["code"], "invalid_request");
    assert_eq!(invalid["target"], "provider_token");

    let secret_token = "secret-provider-token";
    let (status, _value) = json_response_with_state(
        state,
        push_subscription_post_request(
            Some(&token),
            json!({
                "schema_version": 1,
                "provider": "fcm",
                "provider_token": secret_token,
                "app_instance_id": null,
                "device_display_name": "Pixel",
                "platform": "android",
                "app_version": null,
                "hint_categories": ["notifications"]
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let audit = std::fs::read_to_string(
        tmp.path().join("mobile_gateway").join("audit.jsonl"),
    )
    .unwrap();
    assert!(!audit.contains(secret_token));
}

#[tokio::test]
async fn test_push_provider_records_hint_attempts() {
    let tmp = tempfile::tempdir().unwrap();
    let state = GatewayState::new_with_options(GatewayStateOptions {
        bind_addr: "127.0.0.1:0".to_string(),
        sase_home: tmp.path().to_path_buf(),
        pairing_ttl: Duration::minutes(5),
        host_label: "test-host".to_string(),
        event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
        heartbeat_interval: StdDuration::from_secs(60),
        attachment_token_ttl: default_attachment_token_ttl(),
        max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
        push_config: PushConfig {
            provider: crate::push::PushProviderMode::Test,
            ..PushConfig::default()
        },
    });
    let (_start, _finish, token, device_id) = pair_device(state.clone()).await;
    let (register_status, _register) = json_response_with_state(
        state.clone(),
        push_subscription_post_request(
            Some(&token),
            json!({
                "schema_version": 1,
                "provider": "test",
                "provider_token": "test-token",
                "app_instance_id": "app-1",
                "device_display_name": "Pixel",
                "platform": "android",
                "app_version": "0.1.0",
                "hint_categories": ["agents"]
            }),
        ),
    )
    .await;
    assert_eq!(register_status, StatusCode::OK);

    publish_agents_changed(&state, "launch", Some("mobile-demo".to_string()))
        .unwrap();

    let attempts = state.push_dispatcher().test_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].device_id, device_id);
    assert_eq!(attempts[0].provider, crate::wire::PushProviderWire::Test);
    assert_eq!(
        attempts[0].hint.category,
        crate::wire::PushHintCategoryWire::Agents
    );
    assert_eq!(attempts[0].hint.agent_name.as_deref(), Some("mobile-demo"));
    let status = state.push_dispatcher().status();
    assert_eq!(status.provider, "test");
    assert_eq!(status.attempted, 1);
    assert_eq!(status.succeeded, 1);
    assert_eq!(status.failed, 0);
}
