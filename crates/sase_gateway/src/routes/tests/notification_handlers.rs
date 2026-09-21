use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};

use sase_core::notifications::{
    ActionResultWire, GateActionRequestWire, MobileActionKindWire,
    QuestionActionChoiceWire, QuestionActionRequestWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
use sase_core::notifications::MobileActionStateWire;

use crate::host_bridge::{
    DynNotificationHostBridge, HostBridgeError, NotificationHostBridge,
};

use crate::push::PushConfig;

use crate::wire::{DeviceRecordWire, EventPayloadWire};

use axum::http::Request;

use chrono::Duration;

use serde_json::{json, Value};

use super::super::state::{
    GatewayState, GatewayStateOptions, DEFAULT_MAX_ATTACHMENT_BYTES,
};

use super::super::support::{
    default_attachment_token_ttl, initial_events_for_stream,
};

use super::support::*;

#[tokio::test]
async fn notifications_without_token_returns_typed_unauthorized_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_notifications(&tmp, Vec::new());

    let (status, value) = json_response_with_state(
        state,
        notifications_request(None, "/api/v1/notifications"),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
    assert_eq!(value["target"], "authorization");
}

#[tokio::test]
async fn notifications_list_filters_and_orders_newest_first() {
    let tmp = tempfile::tempdir().unwrap();
    let mut read =
        notification("read-row", "2026-05-06T15:00:00Z", Some("PlanApproval"));
    read.read = true;
    let mut silent = notification("silent-row", "2026-05-06T16:00:00Z", None);
    silent.silent = true;
    let newest = notification(
        "newest-row",
        "2026-05-06T17:00:00Z",
        Some("PlanApproval"),
    );
    let older = notification("older-row", "2026-05-06T14:00:00Z", None);
    let state =
        state_for_notifications(&tmp, vec![older, newest, silent, read]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(
            Some(&token),
            "/api/v1/notifications?unread=true&limit=1",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        value["schema_version"],
        MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION
    );
    assert_eq!(value["total_count"], 2);
    assert_eq!(value["next_high_water"], "2026-05-06T17:00:00Z|newest-row");
    assert_eq!(value["notifications"][0]["id"], "newest-row");
    assert_eq!(value["notifications"][0]["priority"], true);
    assert_eq!(value["notifications"][0]["actionable"], true);
}

#[tokio::test]
async fn notifications_list_uses_host_action_state() {
    let tmp = tempfile::tempdir().unwrap();
    let row = notification(
        "handled-row",
        "2026-05-06T17:00:00Z",
        Some("PlanApproval"),
    );
    let state = state_for_notifications_with_action_states(
        &tmp,
        vec![row],
        HashMap::from([(
            "handled-row".to_string(),
            sase_core::notifications::MobileActionStateWire::AlreadyHandled,
        )]),
    );
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(Some(&token), "/api/v1/notifications"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["notifications"][0]["actionable"], false);
    assert_eq!(
        value["notifications"][0]["action_summary"]["state"],
        "already_handled"
    );
}

#[tokio::test]
async fn notifications_list_uses_resurface_activity_cursor_and_id_tiebreaker() {
    let tmp = tempfile::tempdir().unwrap();
    let recent = notification("recent", "2026-05-06T17:00:00Z", None);
    let mut resurfaced_a =
        notification("resurfaced-a", "2026-05-01T12:00:00Z", None);
    resurfaced_a.resurfaced_at = Some("2026-05-06T18:00:00Z".to_string());
    let mut resurfaced_b =
        notification("resurfaced-b", "2026-05-01T11:00:00Z", None);
    resurfaced_b.resurfaced_at = Some("2026-05-06T18:00:00Z".to_string());
    let state =
        state_for_notifications(&tmp, vec![recent, resurfaced_a, resurfaced_b]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(
            Some(&token),
            "/api/v1/notifications?newer_than=2026-05-06T18%3A00%3A00Z%7Cresurfaced-a&limit=1",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["total_count"], 1);
    assert_eq!(value["notifications"][0]["id"], "resurfaced-b");
    assert_eq!(
        value["notifications"][0]["timestamp"],
        "2026-05-01T11:00:00Z"
    );
    assert_eq!(
        value["notifications"][0]["resurfaced_at"],
        "2026-05-06T18:00:00Z"
    );
    assert_eq!(
        value["next_high_water"],
        "2026-05-06T18:00:00Z|resurfaced-b"
    );
}

#[tokio::test]
async fn notifications_list_expiry_publishes_activity_cursor_event() {
    let tmp = tempfile::tempdir().unwrap();
    let mut due = notification("due-row", "2026-05-01T12:00:00Z", None);
    due.read = true;
    due.muted = true;
    due.snooze_until = Some("2026-05-02T12:00:00Z".to_string());
    seed_store_notification(&tmp, &due);
    let state =
        GatewayState::new_with_sase_home("127.0.0.1:0".to_string(), tmp.path());
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state.clone(),
        notifications_request(Some(&token), "/api/v1/notifications"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["notifications"][0]["id"], "due-row");
    assert_eq!(value["notifications"][0]["read"], false);
    let event = state
        .event_hub
        .replay_after("0000000000000000")
        .unwrap()
        .unwrap()
        .into_iter()
        .find(|event| {
            matches!(
                &event.payload,
                EventPayloadWire::NotificationsChanged { reason, .. }
                    if reason == "snooze_expired"
            )
        })
        .expect("expiry should publish a notification refresh event");
    match event.payload {
        EventPayloadWire::NotificationsChanged {
            notification_id,
            activity_cursor,
            ..
        } => {
            assert_eq!(notification_id.as_deref(), Some("due-row"));
            assert!(activity_cursor
                .as_deref()
                .is_some_and(|cursor| cursor.ends_with("|due-row")));
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn notifications_list_can_include_dismissed_and_silent_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let mut dismissed =
        notification("dismissed-row", "2026-05-06T15:00:00Z", None);
    dismissed.dismissed = true;
    let mut silent = notification("silent-row", "2026-05-06T16:00:00Z", None);
    silent.silent = true;
    let state = state_for_notifications(&tmp, vec![dismissed, silent]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(
            Some(&token),
            "/api/v1/notifications?include_dismissed=true&include_silent=true",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["total_count"], 2);
    assert_eq!(value["notifications"][0]["id"], "silent-row");
    assert_eq!(value["notifications"][1]["id"], "dismissed-row");
}

#[tokio::test]
async fn notification_detail_returns_notes_action_and_attachments() {
    let tmp = tempfile::tempdir().unwrap();
    let attachment = tmp.path().join("plan.md");
    std::fs::write(&attachment, "plan").unwrap();
    let mut row = notification(
        "detail-row",
        "2026-05-06T15:00:00Z",
        Some("PlanApproval"),
    );
    row.files = vec![attachment.to_string_lossy().to_string()];
    row.action_data
        .insert("response_dir".to_string(), "/tmp/response".to_string());
    let state = state_for_notifications(&tmp, vec![row]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(Some(&token), "/api/v1/notifications/detail-row"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["notification"]["id"], "detail-row");
    assert_eq!(value["notes"], json!(["note detail-row"]));
    assert_eq!(value["attachments"][0]["byte_size"], 4);
    assert_eq!(value["attachments"][0]["downloadable"], true);
    assert_eq!(value["action"]["kind"], "plan_approval");
    assert_eq!(value["action"]["response_dir"], "/tmp/response");
}

#[tokio::test]
async fn notification_detail_mints_short_lived_download_tokens() {
    let tmp = tempfile::tempdir().unwrap();
    let attachment = tmp.path().join("plan.md");
    std::fs::write(&attachment, "# Plan\n").unwrap();
    let mut row = notification(
        "download-row",
        "2026-05-06T15:00:00Z",
        Some("PlanApproval"),
    );
    row.files = vec![attachment.to_string_lossy().to_string()];
    let state = state_for_notifications(&tmp, vec![row]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (detail_status, detail) = json_response_with_state(
        state.clone(),
        notifications_request(
            Some(&token),
            "/api/v1/notifications/download-row",
        ),
    )
    .await;
    assert_eq!(detail_status, StatusCode::OK);
    let attachment_token = detail["attachments"][0]["token"].as_str().unwrap();
    assert!(attachment_token.starts_with("att_"));
    assert_eq!(detail["attachments"][0]["content_type"], "text/markdown");
    assert_eq!(detail["attachments"][0]["download_requires_auth"], true);

    let (download_status, headers, body) = raw_response_with_state(
        state,
        attachment_request(
            Some(&token),
            &format!("/api/v1/attachments/{attachment_token}"),
        ),
    )
    .await;

    assert_eq!(download_status, StatusCode::OK);
    assert_eq!(headers["content-type"], "text/markdown");
    assert_eq!(headers["content-length"], "7");
    assert_eq!(body, b"# Plan\n");
}

#[tokio::test]
async fn attachment_download_requires_gateway_auth() {
    let tmp = tempfile::tempdir().unwrap();
    let attachment = tmp.path().join("digest.txt");
    std::fs::write(&attachment, "digest").unwrap();
    let mut row = notification("digest-row", "2026-05-06T15:00:00Z", None);
    row.files = vec![attachment.to_string_lossy().to_string()];
    let state = state_for_notifications(&tmp, vec![row]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;
    let (_detail_status, detail) = json_response_with_state(
        state.clone(),
        notifications_request(Some(&token), "/api/v1/notifications/digest-row"),
    )
    .await;
    let attachment_token = detail["attachments"][0]["token"].as_str().unwrap();

    let (status, value) = json_response_with_state(
        state,
        attachment_request(
            None,
            &format!("/api/v1/attachments/{attachment_token}"),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
}

#[tokio::test]
async fn attachment_tokens_are_bound_to_device() {
    let tmp = tempfile::tempdir().unwrap();
    let attachment = tmp.path().join("image.png");
    std::fs::write(&attachment, b"\x89PNG\r\n").unwrap();
    let mut row = notification("image-row", "2026-05-06T15:00:00Z", None);
    row.files = vec![attachment.to_string_lossy().to_string()];
    let state = state_for_notifications(&tmp, vec![row]);
    let (_start, _finish, first_token, _first_device) =
        pair_device(state.clone()).await;
    let (_start2, _finish2, second_token, _second_device) =
        pair_device(state.clone()).await;
    let (_detail_status, detail) = json_response_with_state(
        state.clone(),
        notifications_request(
            Some(&first_token),
            "/api/v1/notifications/image-row",
        ),
    )
    .await;
    let attachment_token = detail["attachments"][0]["token"].as_str().unwrap();

    let (status, value) = json_response_with_state(
        state,
        attachment_request(
            Some(&second_token),
            &format!("/api/v1/attachments/{attachment_token}"),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
}

#[tokio::test]
async fn expired_attachment_tokens_return_typed_error() {
    let tmp = tempfile::tempdir().unwrap();
    let attachment = tmp.path().join("artifact.json");
    std::fs::write(&attachment, "{}").unwrap();
    let mut row = notification("json-row", "2026-05-06T15:00:00Z", None);
    row.files = vec![attachment.to_string_lossy().to_string()];
    let state = state_for_notifications_with_attachment_options(
        &tmp,
        vec![row],
        Duration::seconds(-1),
        DEFAULT_MAX_ATTACHMENT_BYTES,
    );
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;
    let (_detail_status, detail) = json_response_with_state(
        state.clone(),
        notifications_request(Some(&token), "/api/v1/notifications/json-row"),
    )
    .await;
    let attachment_token = detail["attachments"][0]["token"].as_str().unwrap();

    let (status, value) = json_response_with_state(
        state,
        attachment_request(
            Some(&token),
            &format!("/api/v1/attachments/{attachment_token}"),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::GONE);
    assert_eq!(value["code"], "attachment_expired");
}

#[tokio::test]
async fn unsafe_or_oversized_attachments_do_not_receive_tokens() {
    let tmp = tempfile::tempdir().unwrap();
    let safe = tmp.path().join("safe.txt");
    let symlink = tmp.path().join("safe-link.txt");
    let oversized = tmp.path().join("large.diff");
    std::fs::write(&safe, "ok").unwrap();
    std::fs::write(&oversized, "too large").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&safe, &symlink).unwrap();
    #[cfg(not(unix))]
    std::fs::write(&symlink, "ok").unwrap();

    let mut row = notification("unsafe-row", "2026-05-06T15:00:00Z", None);
    row.files = vec![
        symlink.to_string_lossy().to_string(),
        oversized.to_string_lossy().to_string(),
        tmp.path()
            .join("child")
            .join("..")
            .join("safe.txt")
            .to_string_lossy()
            .to_string(),
    ];
    let state = state_for_notifications_with_attachment_options(
        &tmp,
        vec![row],
        default_attachment_token_ttl(),
        3,
    );
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(Some(&token), "/api/v1/notifications/unsafe-row"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["attachments"].as_array().unwrap().len(), 3);
    #[cfg(unix)]
    {
        assert_eq!(value["attachments"][0]["token"], Value::Null);
        assert_eq!(value["attachments"][0]["downloadable"], false);
    }
    #[cfg(not(unix))]
    {
        assert!(value["attachments"][0]["token"].is_string());
        assert_eq!(value["attachments"][0]["downloadable"], true);
    }
    assert_eq!(value["attachments"][1]["token"], Value::Null);
    assert_eq!(value["attachments"][1]["downloadable"], false);
    assert_eq!(value["attachments"][2]["token"], Value::Null);
    assert_eq!(value["attachments"][2]["downloadable"], false);
}

#[tokio::test]
async fn action_artifacts_are_declared_as_attachments() {
    let tmp = tempfile::tempdir().unwrap();
    let artifacts_dir = tmp.path().join("agent").join("artifacts");
    std::fs::create_dir_all(&artifacts_dir).unwrap();
    let output_file = tmp.path().join("output.log");
    std::fs::write(&output_file, "step output").unwrap();
    std::fs::write(
        artifacts_dir.join("hitl_request.json"),
        json!({
            "step_name": "review",
            "step_type": "bash",
            "output": {"log_path": output_file},
            "output_types": {"log_path": "path"}
        })
        .to_string(),
    )
    .unwrap();
    let mut row =
        notification("hitl-artifacts", "2026-05-06T15:00:00Z", Some("HITL"));
    row.action_data.insert(
        "artifacts_dir".to_string(),
        artifacts_dir.to_string_lossy().to_string(),
    );
    let state = state_for_notifications(&tmp, vec![row]);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(
            Some(&token),
            "/api/v1/notifications/hitl-artifacts",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = value["attachments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|attachment| attachment["display_name"].as_str().unwrap())
        .collect();
    assert!(names.iter().any(|name| name.ends_with("hitl_request.json")));
    assert!(names.iter().any(|name| name.ends_with("output.log")));
}

#[tokio::test]
async fn notification_detail_not_found_returns_typed_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_notifications(&tmp, Vec::new());
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notifications_request(
            Some(&token),
            "/api/v1/notifications/missing-row",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
    assert_eq!(value["message"], "notification not found");
    assert_eq!(value["target"], "missing-row");
}

#[tokio::test]
async fn notification_state_mutation_without_token_returns_typed_unauthorized_error(
) {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));

    let (status, value) = json_response_with_state(
        state,
        notification_state_request(
            None,
            "/api/v1/notifications/state-row/mark-read",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
    assert_eq!(value["target"], "authorization");
}

#[tokio::test]
async fn notification_state_mutation_not_found_returns_typed_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        notification_state_request(
            Some(&token),
            "/api/v1/notifications/missing-row/dismiss",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");
    assert_eq!(value["message"], "notification not found: missing-row");
    assert_eq!(value["target"], "missing-row");
}

#[tokio::test]
async fn notification_mark_read_updates_store_and_audits() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let row = notification("state-row", "2026-05-06T15:00:00Z", None);
    seed_store_notification(&tmp, &row);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state.clone(),
        notification_state_request(
            Some(&token),
            "/api/v1/notifications/state-row/mark-read",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["notification_id"], "state-row");
    assert_eq!(value["read"], true);
    assert_eq!(value["dismissed"], false);
    assert_eq!(value["changed"], true);
    let snapshot = sase_core::notifications::read_notifications_snapshot(
        &tmp.path().join("notifications").join("notifications.jsonl"),
        true,
    )
    .unwrap();
    assert!(snapshot.notifications[0].read);
    let audit = std::fs::read_to_string(
        tmp.path().join("mobile_gateway").join("audit.jsonl"),
    )
    .unwrap();
    assert!(audit.lines().any(|line| {
        let entry: Value = serde_json::from_str(line).unwrap();
        entry["endpoint"] == "/api/v1/notifications/{id}/mark-read"
            && entry["target_id"] == "state-row"
            && entry["outcome"] == "success"
    }));
}

#[tokio::test]
async fn notification_dismiss_updates_store_and_emits_refresh_event() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let row = notification("dismiss-row", "2026-05-06T15:00:00Z", None);
    seed_store_notification(&tmp, &row);
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state.clone(),
        notification_state_request(
            Some(&token),
            "/api/v1/notifications/dismiss-row/dismiss",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["notification_id"], "dismiss-row");
    assert_eq!(value["read"], false);
    assert_eq!(value["dismissed"], true);
    assert_eq!(value["changed"], true);
    let snapshot = sase_core::notifications::read_notifications_snapshot(
        &tmp.path().join("notifications").join("notifications.jsonl"),
        true,
    )
    .unwrap();
    assert!(snapshot.notifications[0].dismissed);
    let events = state
        .event_hub
        .replay_after("0000000000000000")
        .unwrap()
        .unwrap();
    assert!(events.iter().any(|event| {
        event.payload
            == EventPayloadWire::NotificationsChanged {
                reason: "dismiss".to_string(),
                notification_id: Some("dismiss-row".to_string()),
                activity_cursor: None,
            }
    }));
}

#[derive(Debug, Default)]
struct RecordingNotificationActionBridge {
    gate_request: Mutex<Option<GateActionRequestWire>>,
    question_request: Mutex<Option<QuestionActionRequestWire>>,
}

impl NotificationHostBridge for RecordingNotificationActionBridge {
    fn list_notifications(
        &self,
        _include_dismissed: bool,
    ) -> Result<
        sase_core::notifications::NotificationStoreSnapshotWire,
        HostBridgeError,
    > {
        Err(HostBridgeError::BridgeUnavailable(
            "recording_notification_bridge".to_string(),
        ))
    }

    fn execute_gate_action(
        &self,
        request: &GateActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        *self.gate_request.lock().unwrap() = Some(request.clone());
        Ok(ActionResultWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            action_kind: MobileActionKindWire::PlanApproval,
            prefix: request.prefix.clone(),
            notification_id: Some("abcdef12-plan".to_string()),
            state: MobileActionStateWire::Available,
            response_file: "response.json".to_string(),
            response_json: json!({
                "selected_option_ids": request.selected_option_ids,
                "feedback": request.feedback,
            }),
            message: Some("Gate resolved".to_string()),
        })
    }

    fn execute_question_action(
        &self,
        request: &QuestionActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        *self.question_request.lock().unwrap() = Some(request.clone());
        Ok(ActionResultWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            action_kind: MobileActionKindWire::UserQuestion,
            prefix: request.prefix.clone(),
            notification_id: Some("question-row".to_string()),
            state: MobileActionStateWire::Available,
            response_file: "response.json".to_string(),
            response_json: json!({"selected_option_ids": ["submit"]}),
            message: Some("Question answered".to_string()),
        })
    }
}

#[tokio::test]
async fn gate_action_forwards_selected_option_submission() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = Arc::new(RecordingNotificationActionBridge::default());
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        action_request(
            Some(&token),
            "/api/v1/actions/gate/abcdef12",
            json!({
                "schema_version": MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                "selected_option_ids": ["approve", "commit"],
                "feedback": "Reviewed on mobile"
            }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["response_file"], "response.json");
    assert_eq!(
        value["response_json"]["selected_option_ids"],
        json!(["approve", "commit"])
    );
    let request = bridge.gate_request.lock().unwrap().clone().unwrap();
    assert_eq!(request.prefix, "abcdef12");
    assert_eq!(request.selected_option_ids, ["approve", "commit"]);
    assert_eq!(request.feedback.as_deref(), Some("Reviewed on mobile"));
}

#[tokio::test]
async fn question_action_forwards_specialized_submission() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = Arc::new(RecordingNotificationActionBridge::default());
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (_start, _finish, token, _device_id) = pair_device(state.clone()).await;

    let (status, value) = json_response_with_state(
        state,
        action_request(
            Some(&token),
            "/api/v1/actions/question/question/answer",
            json!({
                "schema_version": MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                "selected_option_id": "safe",
                "global_note": "Use durable path"
            }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["action_kind"], "user_question");
    let request = bridge.question_request.lock().unwrap().clone().unwrap();
    assert_eq!(request.prefix, "question");
    assert_eq!(request.choice, QuestionActionChoiceWire::Answer);
    assert_eq!(request.selected_option_id.as_deref(), Some("safe"));
    assert_eq!(request.global_note.as_deref(), Some("Use durable path"));
}

#[tokio::test]
async fn event_resume_replays_buffered_records_after_last_event_id() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, finish, _token, _device_id) = pair_device(state.clone()).await;
    let device: DeviceRecordWire =
        serde_json::from_value(finish["device"].clone()).unwrap();

    let first_events =
        initial_events_for_stream(&state, &HeaderMap::new(), &device).unwrap();
    assert_eq!(first_events.len(), 2);
    assert_eq!(first_events[0].id, "0000000000000001");
    assert!(matches!(
        first_events[0].payload,
        EventPayloadWire::Session { .. }
    ));
    assert!(matches!(
        first_events[1].payload,
        EventPayloadWire::Heartbeat { sequence: 1 }
    ));

    let mut headers = HeaderMap::new();
    headers.insert("last-event-id", "0000000000000001".parse().unwrap());
    let replay_events =
        initial_events_for_stream(&state, &headers, &device).unwrap();

    assert_eq!(replay_events.len(), 1);
    assert!(matches!(
        replay_events[0].payload,
        EventPayloadWire::Heartbeat { sequence: 1 }
    ));
}

#[tokio::test]
async fn event_resume_outside_buffer_returns_resync_required() {
    let tmp = tempfile::tempdir().unwrap();
    let state = GatewayState::new_with_options(GatewayStateOptions {
        bind_addr: "127.0.0.1:0".to_string(),
        sase_home: tmp.path().to_path_buf(),
        pairing_ttl: Duration::minutes(5),
        host_label: "test-host".to_string(),
        event_buffer_capacity: 1,
        heartbeat_interval: StdDuration::from_secs(60),
        attachment_token_ttl: default_attachment_token_ttl(),
        max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
        push_config: PushConfig::default(),
    });
    let (_start, finish, _token, _device_id) = pair_device(state.clone()).await;
    let device: DeviceRecordWire =
        serde_json::from_value(finish["device"].clone()).unwrap();

    let _first_events =
        initial_events_for_stream(&state, &HeaderMap::new(), &device).unwrap();
    state
        .event_hub
        .append(|_| EventPayloadWire::NotificationsChanged {
            reason: "capacity-test".to_string(),
            notification_id: None,
            activity_cursor: None,
        })
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("last-event-id", "0000000000000000".parse().unwrap());
    let events = initial_events_for_stream(&state, &headers, &device).unwrap();

    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0].payload,
        EventPayloadWire::ResyncRequired { .. }
    ));
    assert!(matches!(
        events[1].payload,
        EventPayloadWire::Heartbeat { .. }
    ));
}

#[tokio::test]
async fn event_resume_after_restart_returns_resync_required() {
    let tmp = tempfile::tempdir().unwrap();
    let first_state = state_for_tmp(&tmp, Duration::minutes(5));
    let (_start, finish, _token, _device_id) =
        pair_device(first_state.clone()).await;
    let device: DeviceRecordWire =
        serde_json::from_value(finish["device"].clone()).unwrap();
    let first_events =
        initial_events_for_stream(&first_state, &HeaderMap::new(), &device)
            .unwrap();

    let restarted_state = state_for_tmp(&tmp, Duration::minutes(5));
    let mut headers = HeaderMap::new();
    headers.insert("last-event-id", first_events[0].id.parse().unwrap());
    let events =
        initial_events_for_stream(&restarted_state, &headers, &device).unwrap();

    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0].payload,
        EventPayloadWire::ResyncRequired { .. }
    ));
    assert!(matches!(
        events[1].payload,
        EventPayloadWire::Heartbeat { .. }
    ));
}

#[tokio::test]
async fn unknown_route_returns_typed_not_found_error() {
    let (status, value) = json_response(
        Request::builder()
            .uri("/api/v1/nope")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        value,
        json!({
            "schema_version": 1,
            "code": "not_found",
            "message": "route not found",
            "target": "/api/v1/nope",
            "details": null
        })
    );
}
