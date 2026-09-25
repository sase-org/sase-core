use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::http::{HeaderValue, StatusCode};

use sase_core::notifications::{
    ActionResultWire, GateActionRequestWire, MobileActionKindWire,
    NotificationWire, QuestionActionChoiceWire, QuestionActionRequestWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
use sase_core::notifications::MobileActionStateWire;

use crate::fleet_auth::{
    FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE,
    FLEET_SCOPE_HELLO,
};

use crate::host_bridge::{
    DynNotificationHostBridge, HostBridgeError, NotificationHostBridge,
};

use crate::wire::{EventPayloadWire, FLEET_PROTOCOL_VERSION};

use chrono::Duration;

use serde_json::{json, Value};

use super::super::state::{GatewayState, FLEET_PROTOCOL_VERSIONS_HEADER};

use super::support::*;

#[derive(Debug, Default)]
pub(crate) struct FakeAttentionNotificationBridge {
    notifications: Mutex<Vec<NotificationWire>>,
    handled: Mutex<std::collections::HashSet<String>>,
    next_gate_error: Mutex<Option<HostBridgeError>>,
    next_question_error: Mutex<Option<HostBridgeError>>,
    gate_calls: Mutex<Vec<GateActionRequestWire>>,
    question_calls: Mutex<Vec<QuestionActionRequestWire>>,
    list_calls: Mutex<u32>,
}

impl FakeAttentionNotificationBridge {
    fn with_notifications(notifications: Vec<NotificationWire>) -> Self {
        Self {
            notifications: Mutex::new(notifications),
            ..Default::default()
        }
    }
}

impl NotificationHostBridge for FakeAttentionNotificationBridge {
    fn list_notifications(
        &self,
        _include_dismissed: bool,
    ) -> Result<
        sase_core::notifications::NotificationStoreSnapshotWire,
        HostBridgeError,
    > {
        *self.list_calls.lock().unwrap() += 1;
        Ok(sase_core::notifications::NotificationStoreSnapshotWire {
            schema_version:
                sase_core::notifications::NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
            notifications: self.notifications.lock().unwrap().clone(),
            counts: sase_core::notifications::NotificationCountsWire::default(),
            tabs: Vec::new(),
            expired_ids: Vec::new(),
            next_snooze_deadline: None,
            stats:
                sase_core::notifications::NotificationStoreStatsWire::default(),
        })
    }

    fn action_state(
        &self,
        notification: &NotificationWire,
    ) -> MobileActionStateWire {
        if self.handled.lock().unwrap().contains(&notification.id) {
            MobileActionStateWire::AlreadyHandled
        } else {
            MobileActionStateWire::Available
        }
    }

    fn execute_gate_action(
        &self,
        request: &GateActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.gate_calls.lock().unwrap().push(request.clone());
        if let Some(error) = self.next_gate_error.lock().unwrap().take() {
            return Err(error);
        }
        self.handled.lock().unwrap().insert(request.prefix.clone());
        Ok(ActionResultWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            action_kind: MobileActionKindWire::CustomGate,
            prefix: request.prefix.clone(),
            notification_id: Some(request.prefix.clone()),
            state: MobileActionStateWire::AlreadyHandled,
            response_file: "response.json".to_string(),
            response_json: json!({
                "selected_option_ids": request.selected_option_ids,
            }),
            message: Some("Approved".to_string()),
        })
    }

    fn execute_question_action(
        &self,
        request: &QuestionActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.question_calls.lock().unwrap().push(request.clone());
        if let Some(error) = self.next_question_error.lock().unwrap().take() {
            return Err(error);
        }
        self.handled.lock().unwrap().insert(request.prefix.clone());
        Ok(ActionResultWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            action_kind: MobileActionKindWire::UserQuestion,
            prefix: request.prefix.clone(),
            notification_id: Some(request.prefix.clone()),
            state: MobileActionStateWire::AlreadyHandled,
            response_file: "response.json".to_string(),
            response_json: json!({"answer": "42"}),
            message: Some("Answered".to_string()),
        })
    }
}

fn attention_gate_notification(
    id: &str,
    origin_agent: &str,
    request_path: &str,
) -> NotificationWire {
    let mut action_data = BTreeMap::new();
    action_data.insert("origin_agent".to_string(), origin_agent.to_string());
    action_data.insert("gate_title".to_string(), "Approve deploy".to_string());
    action_data.insert("request_path".to_string(), request_path.to_string());
    NotificationWire {
        id: id.to_string(),
        timestamp: "2026-09-07T00:00:00Z".to_string(),
        sender: "axe".to_string(),
        action: Some("CustomGate".to_string()),
        action_data,
        notes: vec!["Please approve the deploy".to_string()],
        ..Default::default()
    }
}

fn write_gate_envelope(path: &std::path::Path) {
    std::fs::write(
        path,
        serde_json::to_vec(&json!({
            "schema_version": 3,
            "options": [{"id": "approve", "label": "Approve"}],
            "branches": [["approve"]]
        }))
        .unwrap(),
    )
    .unwrap();
}

fn write_sudo_gate_envelope(path: &std::path::Path) {
    std::fs::write(
        path,
        serde_json::to_vec(&json!({
            "schema_version": 3,
            "options": [
                {
                    "id": "approve",
                    "label": "Approve",
                    "requires_tty": true
                },
                {
                    "id": "deny",
                    "label": "Deny",
                    "requires_tty": false
                }
            ],
            "branches": [["approve"], ["deny"]]
        }))
        .unwrap(),
    )
    .unwrap();
}

fn attention_question_notification(id: &str, sender: &str) -> NotificationWire {
    let mut action_data = BTreeMap::new();
    action_data.insert("question_count".to_string(), "1".to_string());
    NotificationWire {
        id: id.to_string(),
        timestamp: "2026-09-07T00:00:00Z".to_string(),
        sender: sender.to_string(),
        action: Some("UserQuestion".to_string()),
        action_data,
        notes: vec!["What should we do next?".to_string()],
        ..Default::default()
    }
}

async fn post_attention_read(
    state: GatewayState,
    token: &str,
    logical_keys: Vec<String>,
) -> (StatusCode, Value) {
    let mut request = fleet_json_request(
        "POST",
        "/api/fleet/v1/attention",
        Some(token),
        Some(json!({
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            "logical_keys": logical_keys,
        })),
    );
    request.headers_mut().insert(
        FLEET_PROTOCOL_VERSIONS_HEADER,
        HeaderValue::from(FLEET_PROTOCOL_VERSION),
    );
    json_response_with_state(state, request).await
}

async fn post_attention_inventory(
    state: GatewayState,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = fleet_json_request(
        "POST",
        "/api/fleet/v1/attention/inventory",
        Some(token),
        Some(body),
    );
    request.headers_mut().insert(
        FLEET_PROTOCOL_VERSIONS_HEADER,
        HeaderValue::from(FLEET_PROTOCOL_VERSION),
    );
    json_response_with_state(state, request).await
}

fn attention_request_key(
    origin_installation_id: &str,
    request_id: &str,
) -> sase_core::FleetAttentionRequestKeyWire {
    sase_core::FleetAttentionRequestKeyWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        origin_installation_id: origin_installation_id.to_string(),
        request_id: request_id.to_string(),
        pending_action_prefix: request_id.chars().take(8).collect(),
    }
}

fn attention_resolve_body(
    intent: sase_core::FleetAttentionIntentWire,
    target_installation_id: &str,
    controller_id: &str,
    operation_id: &str,
) -> Value {
    let fingerprint =
        sase_core::fleet_attention_payload_fingerprint(&intent).unwrap();
    json!({
        "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        "key": {
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            "controller_id": controller_id,
            "operation_id": operation_id
        },
        "target_installation_id": target_installation_id,
        "intent": intent,
        "payload_fingerprint": fingerprint,
        "acceptance_window_seconds": 30.0
    })
}

async fn post_attention_resolve(
    state: GatewayState,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = fleet_json_request(
        "POST",
        "/api/fleet/v1/attention/resolve",
        Some(token),
        Some(body),
    );
    request.headers_mut().insert(
        FLEET_PROTOCOL_VERSIONS_HEADER,
        HeaderValue::from(FLEET_PROTOCOL_VERSION),
    );
    json_response_with_state(state, request).await
}

#[tokio::test]
async fn fleet_attention_denies_missing_scope() {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    let (token, _installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_HELLO]).await;
    let (status, body) =
        post_attention_read(state.clone(), &token, Vec::new()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "scope_denied");
    assert_eq!(body["target"], FLEET_SCOPE_ATTENTION_READ);

    let (status, body) = post_attention_inventory(
        state.clone(),
        &token,
        json!({
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "scope_denied");
    assert_eq!(body["target"], FLEET_SCOPE_ATTENTION_READ);

    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Gate,
        request_key: attention_request_key(
            "sase_inst_v1_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "gate-0001",
        ),
        observed_revision: 1,
        selected_option_ids: vec!["approve".to_string()],
        feedback: None,
        question_choice: None,
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: None,
        global_note: None,
    };
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_HELLO]).await;
    let body = attention_resolve_body(
        intent,
        &installation_id,
        "controller-a",
        "op-1",
    );
    let (status, body) = post_attention_resolve(state, &token, body).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "scope_denied");
    assert_eq!(body["target"], FLEET_SCOPE_ATTENTION_RESOLVE);
}

#[tokio::test]
async fn fleet_attention_read_empty_request_touches_no_notification_store() {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let bridge = Arc::new(FakeAttentionNotificationBridge::default());
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, _installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

    let (status, snapshot) =
        post_attention_read(state, &token, Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(snapshot["entries"], json!([]));
    assert_eq!(*bridge.list_calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn fleet_attention_inventory_returns_uncataloged_pending_requests() {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let envelope_path = tmp.path().join("gate_request.json");
    write_gate_envelope(&envelope_path);
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_gate_notification(
                "gate-00000001",
                "never-loaded-agent",
                envelope_path.to_str().unwrap(),
            ),
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

    let (status, response) = post_attention_inventory(
        state,
        &token,
        json!({
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            "limit": 10,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["page"]["total_matching_entries"], json!(1));
    let entries = response["page"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "{entries:?}");
    let entry = &entries[0];
    assert_eq!(
        entry["request_key"]["origin_installation_id"],
        json!(installation_id)
    );
    assert_eq!(entry["request_key"]["request_id"], json!("gate-00000001"));
    assert!(entry["logical_key"].is_null());
    assert!(entry["logical_locator"].is_null());
    assert_eq!(entry["state"], json!("pending"));
    assert_eq!(*bridge.list_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn fleet_attention_inventory_pages_pending_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_question_notification("question-00000003", "agent-c"),
            attention_question_notification("question-00000002", "agent-b"),
            attention_question_notification("question-00000001", "agent-a"),
        ]));
    bridge
        .handled
        .lock()
        .unwrap()
        .insert("question-00000002".to_string());
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, _installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

    let (status, first) = post_attention_inventory(
        state.clone(),
        &token,
        json!({
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            "limit": 1,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["page"]["total_matching_entries"], json!(2));
    assert_eq!(first["page"]["has_more"], json!(true));
    assert_eq!(first["page"]["next_cursor"], json!("off:1"));
    assert_eq!(
        first["page"]["entries"][0]["request_key"]["request_id"],
        json!("question-00000001")
    );

    let (status, second) = post_attention_inventory(
        state,
        &token,
        json!({
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            "limit": 1,
            "cursor": first["page"]["next_cursor"].clone(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["page"]["total_matching_entries"], json!(2));
    assert_eq!(second["page"]["has_more"], json!(false));
    assert!(second["page"]["next_cursor"].is_null());
    assert_eq!(
        second["page"]["entries"][0]["request_key"]["request_id"],
        json!("question-00000003")
    );
    assert_eq!(*bridge.list_calls.lock().unwrap(), 2);
}

#[tokio::test]
async fn fleet_attention_inventory_succeeds_on_busy_host() {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let envelope_path = tmp.path().join("gate_request.json");
    write_gate_envelope(&envelope_path);
    let mut notifications = Vec::new();
    for index in 0..200 {
        notifications.push(NotificationWire {
            id: format!("plain-{index:04}"),
            timestamp: "2026-09-07T00:00:00Z".to_string(),
            sender: "axe".to_string(),
            action: None,
            action_data: BTreeMap::new(),
            notes: Vec::new(),
            ..Default::default()
        });
    }
    for index in 0..5 {
        notifications.push(attention_gate_notification(
            &format!("gate-busy-{index:04}"),
            "never-loaded-agent",
            envelope_path.to_str().unwrap(),
        ));
    }
    assert!(notifications.len() > 200);
    let bridge = Arc::new(FakeAttentionNotificationBridge::with_notifications(
        notifications,
    ));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, _installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

    let (status, response) = post_attention_inventory(
        state,
        &token,
        json!({
            "schema_version": sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            "limit": 100,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["page"]["total_matching_entries"], json!(5));
    let entries = response["page"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 5, "{entries:?}");
    assert_eq!(response["page"]["has_more"], json!(false));
}

#[tokio::test]
async fn fleet_attention_read_projects_correlated_gate_and_question() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());

    let envelope_path = tmp.path().join("gate_request.json");
    write_gate_envelope(&envelope_path);
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_gate_notification(
                "gate-00000001",
                &agent_label,
                envelope_path.to_str().unwrap(),
            ),
            attention_question_notification("question-0001", &agent_label),
            // A different agent's gate must never be returned for this
            // followed row.
            attention_gate_notification(
                "gate-99999999",
                "someone-elses-agent",
                envelope_path.to_str().unwrap(),
            ),
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, _installation_id) =
        enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

    let (status, snapshot) =
        post_attention_read(state, &token, vec![summary.logical_key.clone()])
            .await;
    assert_eq!(status, StatusCode::OK);
    let entries = snapshot["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "{entries:?}");
    let gate_entry = entries
        .iter()
        .find(|entry| entry["kind"] == "gate")
        .unwrap();
    assert_eq!(gate_entry["logical_key"], summary.logical_key);
    assert_eq!(gate_entry["options"][0]["id"], "approve");
    let question_entry = entries
        .iter()
        .find(|entry| entry["kind"] == "question")
        .unwrap();
    assert_eq!(question_entry["logical_key"], summary.logical_key);
    assert_eq!(*bridge.list_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn fleet_attention_resolve_gate_settles_replays_and_conflicts() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());

    let envelope_path = tmp.path().join("gate_request.json");
    write_gate_envelope(&envelope_path);
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_gate_notification(
                "gate-00000001",
                &agent_label,
                envelope_path.to_str().unwrap(),
            ),
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;

    let (read_status, snapshot) = post_attention_read(
        state.clone(),
        &token,
        vec![summary.logical_key.clone()],
    )
    .await;
    assert_eq!(read_status, StatusCode::OK);
    let revision = snapshot["entries"][0]["revision"].as_u64().unwrap();

    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Gate,
        request_key: attention_request_key(&installation_id, "gate-00000001"),
        observed_revision: revision,
        selected_option_ids: vec!["approve".to_string()],
        feedback: Some("Looks good".to_string()),
        question_choice: None,
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: None,
        global_note: None,
    };
    let body = attention_resolve_body(
        intent.clone(),
        &installation_id,
        "controller-a",
        "op-1",
    );
    let (status, resolved) =
        post_attention_resolve(state.clone(), &token, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{resolved}");
    assert_eq!(resolved["decision"], "accept_new");
    assert_eq!(resolved["receipt"]["state"], "settled");
    assert_eq!(resolved["receipt"]["outcome"], "applied");
    assert_eq!(
        resolved["receipt"]["settled_response"]["selected_option_ids"],
        json!(["approve"])
    );
    assert_eq!(bridge.gate_calls.lock().unwrap().len(), 1);

    // Identical key and payload replays the original receipt rather
    // than re-executing against the bridge.
    let (replay_status, replayed) =
        post_attention_resolve(state.clone(), &token, body).await;
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replayed["decision"], "return_original_receipt");
    assert_eq!(replayed["receipt"], resolved["receipt"]);
    assert_eq!(bridge.gate_calls.lock().unwrap().len(), 1);

    // The same operation key with a changed payload conflicts.
    let mut changed_intent = intent;
    changed_intent.feedback = Some("Changed my mind".to_string());
    let changed_body = attention_resolve_body(
        changed_intent,
        &installation_id,
        "controller-a",
        "op-1",
    );
    let (conflict_status, conflict) =
        post_attention_resolve(state, &token, changed_body).await;
    assert_eq!(conflict_status, StatusCode::CONFLICT, "{conflict}");
}

#[tokio::test]
async fn fleet_attention_resolve_sudo_approve_is_deny_only_remote() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());

    let envelope_path = tmp.path().join("sudo_gate_request.json");
    write_sudo_gate_envelope(&envelope_path);
    let mut notification = attention_gate_notification(
        "sudo-00000001",
        &agent_label,
        envelope_path.to_str().unwrap(),
    );
    notification.action = Some("SudoRequest".to_string());
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            notification,
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;

    let (read_status, snapshot) = post_attention_read(
        state.clone(),
        &token,
        vec![summary.logical_key.clone()],
    )
    .await;
    assert_eq!(read_status, StatusCode::OK, "{snapshot}");
    let entry = &snapshot["entries"][0];
    assert_eq!(entry["options"][0]["requires_tty"], json!(true));
    assert_eq!(entry["options"][1]["requires_tty"], json!(false));
    let revision = entry["revision"].as_u64().unwrap();

    let approve_intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Gate,
        request_key: attention_request_key(&installation_id, "sudo-00000001"),
        observed_revision: revision,
        selected_option_ids: vec!["approve".to_string()],
        feedback: None,
        question_choice: None,
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: None,
        global_note: None,
    };
    let body = attention_resolve_body(
        approve_intent,
        &installation_id,
        "controller-a",
        "sudo-op-approve",
    );
    let (status, resolved) =
        post_attention_resolve(state.clone(), &token, body).await;
    assert_eq!(status, StatusCode::OK, "{resolved}");
    assert_eq!(resolved["decision"], "accept_new");
    assert_eq!(resolved["receipt"]["outcome"], json!("capability_missing"));
    assert_eq!(bridge.gate_calls.lock().unwrap().len(), 0);

    let deny_intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Gate,
        request_key: attention_request_key(&installation_id, "sudo-00000001"),
        observed_revision: revision,
        selected_option_ids: vec!["deny".to_string()],
        feedback: None,
        question_choice: None,
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: None,
        global_note: None,
    };
    let body = attention_resolve_body(
        deny_intent,
        &installation_id,
        "controller-b",
        "sudo-op-deny",
    );
    let (status, denied) = post_attention_resolve(state, &token, body).await;
    assert_eq!(status, StatusCode::OK, "{denied}");
    assert_eq!(denied["receipt"]["outcome"], json!("applied"));
    assert_eq!(
        denied["receipt"]["settled_response"]["selected_option_ids"],
        json!(["deny"])
    );
    assert_eq!(bridge.gate_calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn fleet_attention_resolve_refuses_stale_revision() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_question_notification("question-0001", &agent_label),
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge);
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;

    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Question,
        request_key: attention_request_key(&installation_id, "question-0001"),
        observed_revision: 0,
        selected_option_ids: Vec::new(),
        feedback: None,
        question_choice: Some(QuestionActionChoiceWire::Custom),
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: Some("Ship it".to_string()),
        global_note: None,
    };
    let body = attention_resolve_body(
        intent,
        &installation_id,
        "controller-a",
        "op-stale",
    );
    let (status, body) = post_attention_resolve(state, &token, body).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["decision"], "accept_new");
    assert_eq!(body["receipt"]["state"], "settled");
    assert_eq!(body["receipt"]["outcome"], "stale_revision");
    assert_eq!(body["receipt"]["settled_by_host_label"], "test-host");
    assert_eq!(
        body["receipt"]["message"],
        "Attention revision is stale on test-host"
    );
}

#[tokio::test]
async fn fleet_attention_resolve_second_controller_gets_already_settled() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_question_notification("question-0002", &agent_label),
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;

    let (_status, snapshot) = post_attention_read(
        state.clone(),
        &token,
        vec![summary.logical_key.clone()],
    )
    .await;
    let revision = snapshot["entries"][0]["revision"].as_u64().unwrap();

    let base_intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Question,
        request_key: attention_request_key(&installation_id, "question-0002"),
        observed_revision: revision,
        selected_option_ids: Vec::new(),
        feedback: None,
        question_choice: Some(QuestionActionChoiceWire::Custom),
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: Some("Ship it".to_string()),
        global_note: None,
    };

    let first_body = attention_resolve_body(
        base_intent.clone(),
        &installation_id,
        "controller-a",
        "op-first",
    );
    let (first_status, first) =
        post_attention_resolve(state.clone(), &token, first_body).await;
    assert_eq!(first_status, StatusCode::OK, "{first}");
    assert_eq!(first["receipt"]["outcome"], "applied");

    // A second controller submits a distinct operation key against the
    // same request; the bridge now reports it AlreadyHandled.
    let second_body = attention_resolve_body(
        base_intent,
        &installation_id,
        "controller-b",
        "op-second",
    );
    let (second_status, second) =
        post_attention_resolve(state, &token, second_body).await;
    assert_eq!(second_status, StatusCode::OK, "{second}");
    assert_eq!(second["decision"], "accept_new");
    assert_eq!(second["receipt"]["outcome"], "already_settled");
    assert_eq!(second["receipt"]["settled_by_host_label"], "test-host");
    assert_eq!(
        second["receipt"]["message"],
        "Already answered on test-host"
    );
    assert_eq!(
        second["receipt"]["settled_response"]["answer"],
        first["receipt"]["settled_response"]["answer"]
    );
    // Only the first controller's submission actually executed.
    assert_eq!(bridge.question_calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn fleet_attention_resolve_settles_unknown_request() {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let bridge = Arc::new(FakeAttentionNotificationBridge::default());
    state.notification_bridge = DynNotificationHostBridge::new(bridge);
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;

    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Gate,
        request_key: attention_request_key(&installation_id, "no-such-gate"),
        observed_revision: 0,
        selected_option_ids: vec!["approve".to_string()],
        feedback: None,
        question_choice: None,
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: None,
        global_note: None,
    };
    let body = attention_resolve_body(
        intent,
        &installation_id,
        "controller-a",
        "op-unknown",
    );
    let (status, body) = post_attention_resolve(state, &token, body).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["receipt"]["state"], "settled");
    assert_eq!(body["receipt"]["outcome"], "unknown_request");
    assert_eq!(
        body["receipt"]["message"],
        "Attention request was not found on test-host"
    );
}

#[tokio::test]
async fn fleet_attention_resolve_maps_bridge_race_to_already_settled() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_question_notification("question-0003", &agent_label),
        ]));
    *bridge.next_question_error.lock().unwrap() = Some(
        HostBridgeError::ActionAlreadyHandled("question-0003".to_string()),
    );
    state.notification_bridge = DynNotificationHostBridge::new(bridge.clone());
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;
    let (_status, snapshot) = post_attention_read(
        state.clone(),
        &token,
        vec![summary.logical_key.clone()],
    )
    .await;
    let revision = snapshot["entries"][0]["revision"].as_u64().unwrap();

    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Question,
        request_key: attention_request_key(&installation_id, "question-0003"),
        observed_revision: revision,
        selected_option_ids: Vec::new(),
        feedback: None,
        question_choice: Some(QuestionActionChoiceWire::Custom),
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: Some("Ship it".to_string()),
        global_note: None,
    };
    let body = attention_resolve_body(
        intent,
        &installation_id,
        "controller-a",
        "op-race",
    );
    let (status, resolved) = post_attention_resolve(state, &token, body).await;
    assert_eq!(status, StatusCode::OK, "{resolved}");
    assert_eq!(resolved["receipt"]["outcome"], "already_settled");
    assert_eq!(
        resolved["receipt"]["message"],
        "Already answered on test-host"
    );
}

#[tokio::test]
async fn fleet_attention_resolve_publishes_invalidation_events() {
    let tmp = tempfile::tempdir().unwrap();
    seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
    let mut state = state_for_tmp(&tmp, Duration::minutes(5));
    let summary = first_summary(&state).await;
    let agent_label = summary
        .labels
        .agent_label
        .clone()
        .unwrap_or_else(|| summary.logical_locator.agent_id.clone());
    let bridge =
        Arc::new(FakeAttentionNotificationBridge::with_notifications(vec![
            attention_question_notification("question-0004", &agent_label),
        ]));
    state.notification_bridge = DynNotificationHostBridge::new(bridge);
    let (token, installation_id) = enroll_mutate(
        &state,
        &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
    )
    .await;
    let (_status, snapshot) = post_attention_read(
        state.clone(),
        &token,
        vec![summary.logical_key.clone()],
    )
    .await;
    let revision = snapshot["entries"][0]["revision"].as_u64().unwrap();

    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        kind: sase_core::FleetAttentionKindWire::Question,
        request_key: attention_request_key(&installation_id, "question-0004"),
        observed_revision: revision,
        selected_option_ids: Vec::new(),
        feedback: None,
        question_choice: Some(QuestionActionChoiceWire::Custom),
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: Some("Ship it".to_string()),
        global_note: None,
    };
    let body = attention_resolve_body(
        intent,
        &installation_id,
        "controller-a",
        "op-events",
    );
    let (status, _resolved) =
        post_attention_resolve(state.clone(), &token, body).await;
    assert_eq!(status, StatusCode::OK);

    let events = state
        .event_hub
        .replay_after("0000000000000000")
        .unwrap()
        .unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::NotificationsChanged {
            reason,
            notification_id: Some(id),
            ..
        } if reason == "fleet_attention_resolve" && id == "question-0004"
    )));
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayloadWire::AgentsChanged { reason, .. }
            if reason == "fleet_attention_resolve"
    )));
}
