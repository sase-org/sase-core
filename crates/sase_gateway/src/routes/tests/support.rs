use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};

use sase_core::notifications::NotificationWire;

use crate::fleet_auth::current_unix_time;

use crate::host_bridge::{AgentHostBridge, HostBridgeError};

use crate::push::PushConfig;

use crate::wire::{
    MobileAgentKillRequestWire, MobileAgentKillResultWire,
    MobileAgentLaunchResultWire, MobileAgentListResponseWire,
    MobileAgentResumeOptionsResponseWire, MobileAgentRetryResultWire,
    MobileAgentTextLaunchRequestWire, MobileBeadListResponseWire,
    MobileBeadShowResponseWire, MobileChangeSpecTagListResponseWire,
    MobileUpdateStartResponseWire, MobileUpdateStatusResponseWire,
    MobileXpromptCatalogResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

use axum::body::to_bytes;
use axum::http::Request;

use chrono::Duration;

use serde_json::{json, Value};

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use tempfile::TempDir;

use tower::ServiceExt;

use super::super::router::app_with_state;

use super::super::state::{
    GatewayState, GatewayStateOptions, DEFAULT_EVENT_BUFFER_CAPACITY,
    DEFAULT_MAX_ATTACHMENT_BYTES,
};

use super::super::support::default_attachment_token_ttl;

pub(crate) async fn json_response(
    request: Request<Body>,
) -> (StatusCode, Value) {
    let tmp = tempfile::tempdir().unwrap();
    let state = state_for_tmp(&tmp, Duration::minutes(5));
    json_response_with_state(state, request).await
}

pub(crate) async fn json_response_with_state(
    state: GatewayState,
    request: Request<Body>,
) -> (StatusCode, Value) {
    let response = app_with_state(state).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

pub(crate) async fn raw_response_with_state(
    state: GatewayState,
    request: Request<Body>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = app_with_state(state).oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, headers, bytes.to_vec())
}

pub(crate) fn state_for_tmp(
    tmp: &TempDir,
    pairing_ttl: Duration,
) -> GatewayState {
    GatewayState::new_with_options(GatewayStateOptions {
        bind_addr: "127.0.0.1:0".to_string(),
        sase_home: tmp.path().to_path_buf(),
        pairing_ttl,
        host_label: "test-host".to_string(),
        event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
        heartbeat_interval: StdDuration::from_secs(60),
        attachment_token_ttl: default_attachment_token_ttl(),
        max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
        push_config: PushConfig::default(),
    })
}

pub(crate) fn json_request(
    method: &str,
    uri: &str,
    body: Value,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub(crate) fn session_request(token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri("/api/v1/session");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

pub(crate) fn push_subscription_get_request(
    token: Option<&str>,
) -> Request<Body> {
    let mut builder =
        Request::builder().uri("/api/v1/session/push-subscriptions");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

pub(crate) fn push_subscription_post_request(
    token: Option<&str>,
    body: Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/session/push-subscriptions")
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub(crate) fn push_subscription_delete_request(
    token: Option<&str>,
    id: &str,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("DELETE")
        .uri(format!("/api/v1/session/push-subscriptions/{id}"));
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

pub(crate) fn events_request(
    token: Option<&str>,
    last_event_id: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder().uri("/api/v1/events");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(last_event_id) = last_event_id {
        builder = builder.header("last-event-id", last_event_id);
    }
    builder.body(Body::empty()).unwrap()
}

pub(crate) fn notifications_request(
    token: Option<&str>,
    uri: &str,
) -> Request<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

pub(crate) fn notification_state_request(
    token: Option<&str>,
    uri: &str,
) -> Request<Body> {
    let mut builder = Request::builder().method("POST").uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

pub(crate) fn attachment_request(
    token: Option<&str>,
    uri: &str,
) -> Request<Body> {
    notifications_request(token, uri)
}

pub(crate) fn action_request(
    token: Option<&str>,
    uri: &str,
    body: Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub(crate) fn agent_get_request(
    token: Option<&str>,
    uri: &str,
) -> Request<Body> {
    notifications_request(token, uri)
}

pub(crate) fn agent_post_request(
    token: Option<&str>,
    uri: &str,
    body: Value,
) -> Request<Body> {
    action_request(token, uri, body)
}

pub(crate) fn fleet_bootstrap(
    state: &GatewayState,
    scopes: &[&str],
    expires_at_unix: Option<f64>,
) -> crate::wire::FleetBootstrapIssueResponseWire {
    state
        .fleet_store()
        .issue_bootstrap(
            crate::wire::FleetBootstrapIssueRequestWire {
                schema_version: crate::wire::FLEET_API_WIRE_SCHEMA_VERSION,
                requested_scopes: scopes
                    .iter()
                    .map(|scope| scope.to_string())
                    .collect(),
                supported_protocol_versions: vec![
                    crate::wire::FLEET_PROTOCOL_VERSION,
                ],
                expires_at_unix,
                installation_pin: None,
            },
            current_unix_time(),
        )
        .unwrap()
}

pub(crate) fn fleet_bootstrap_at(
    state: &GatewayState,
    expires_at_unix: f64,
    issued_at_unix: f64,
) -> crate::wire::FleetBootstrapIssueResponseWire {
    state
        .fleet_store()
        .issue_bootstrap(
            crate::wire::FleetBootstrapIssueRequestWire {
                schema_version: crate::wire::FLEET_API_WIRE_SCHEMA_VERSION,
                requested_scopes: Vec::new(),
                supported_protocol_versions: vec![
                    crate::wire::FLEET_PROTOCOL_VERSION,
                ],
                expires_at_unix: Some(expires_at_unix),
                installation_pin: None,
            },
            issued_at_unix,
        )
        .unwrap()
}

pub(crate) fn fleet_enroll_body(
    bootstrap: &crate::wire::FleetBootstrapIssueResponseWire,
    scopes: &[&str],
    versions: Vec<u32>,
) -> Value {
    json!({
        "schema_version": crate::wire::FLEET_API_WIRE_SCHEMA_VERSION,
        "bootstrap_id": bootstrap.bootstrap_id.clone(),
        "bootstrap_secret": bootstrap.bootstrap_secret.clone(),
        "controller": {
            "schema_version": 1,
            "controller_id": "controller-a",
            "display_name": "Controller A",
            "platform": "linux",
            "app_version": "1.0.0"
        },
        "requested_scopes": scopes,
        "supported_protocol_versions": versions,
        "pinned_installation_id": bootstrap.pinned_installation_id.clone()
    })
}

pub(crate) fn fleet_enroll_request(body: Value) -> Request<Body> {
    json_request("POST", "/api/fleet/v1/enroll", body)
}

pub(crate) fn fleet_json_request(
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let bytes = body
        .map(|value| serde_json::to_vec(&value).unwrap())
        .unwrap_or_default();
    let mut builder = Request::builder().method(method).uri(uri);
    if !bytes.is_empty() {
        builder = builder.header("content-type", "application/json");
    }
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(bytes)).unwrap()
}

pub(crate) fn notification(
    id: &str,
    timestamp: &str,
    action: Option<&str>,
) -> NotificationWire {
    let mut notification = NotificationWire {
        id: id.to_string(),
        timestamp: timestamp.to_string(),
        sender: if action == Some("PlanApproval") {
            "plan".to_string()
        } else {
            "user-workflow".to_string()
        },
        notes: vec![format!("note {id}")],
        action: action.map(str::to_string),
        ..NotificationWire::default()
    };
    if action == Some("PlanApproval") {
        notification
            .action_data
            .insert("response_dir".to_string(), "/tmp/response".to_string());
    }
    notification
}

pub(crate) fn state_for_notifications(
    tmp: &TempDir,
    notifications: Vec<NotificationWire>,
) -> GatewayState {
    state_for_notifications_with_attachment_options(
        tmp,
        notifications,
        default_attachment_token_ttl(),
        DEFAULT_MAX_ATTACHMENT_BYTES,
    )
}

pub(crate) fn state_for_notifications_with_attachment_options(
    tmp: &TempDir,
    notifications: Vec<NotificationWire>,
    attachment_token_ttl: Duration,
    max_attachment_bytes: u64,
) -> GatewayState {
    GatewayState::new_with_notification_bridge(
        GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl: Duration::minutes(5),
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl,
            max_attachment_bytes,
            push_config: PushConfig::default(),
        },
        Arc::new(crate::host_bridge::StaticNotificationHostBridge::new(
            notifications,
        )),
    )
}

pub(crate) fn state_for_notifications_with_action_states(
    tmp: &TempDir,
    notifications: Vec<NotificationWire>,
    action_states: HashMap<
        String,
        sase_core::notifications::MobileActionStateWire,
    >,
) -> GatewayState {
    GatewayState::new_with_notification_bridge(
        GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl: Duration::minutes(5),
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
        push_config: PushConfig::default(),
        },
        Arc::new(
            crate::host_bridge::StaticNotificationHostBridge::new_with_action_states(
                notifications,
                action_states,
            ),
        ),
    )
}

pub(crate) fn sample_agent_summary(
    name: &str,
) -> crate::wire::MobileAgentSummaryWire {
    crate::wire::MobileAgentSummaryWire {
        name: name.to_string(),
        project: Some("sase".to_string()),
        status: "running".to_string(),
        pid: Some(4242),
        model: Some("gpt-5.6-sol".to_string()),
        provider: Some("codex".to_string()),
        workspace_number: Some(102),
        started_at: Some("2026-05-06T14:30:00Z".to_string()),
        duration_seconds: Some(90),
        prompt_snippet: Some("Implement gateway skeleton".to_string()),
        has_artifact_dir: true,
        retry_lineage: crate::wire::MobileAgentRetryLineageWire {
            retry_of_timestamp: None,
            retried_as_timestamp: None,
            retry_chain_root_timestamp: Some(
                "2026-05-06T14:30:00Z".to_string(),
            ),
            retry_attempt: Some(0),
            parent_agent_name: None,
        },
        actions: crate::wire::MobileAgentActionAffordancesWire {
            can_resume: true,
            can_wait: true,
            can_kill: true,
            can_retry: false,
        },
        display: crate::wire::MobileAgentDisplayLabelsWire {
            title: name.to_string(),
            subtitle: Some("sase".to_string()),
            status_label: "Running".to_string(),
        },
    }
}

pub(crate) fn sample_launch_result(name: &str) -> MobileAgentLaunchResultWire {
    let slot = crate::wire::MobileAgentLaunchSlotResultWire {
        slot_id: "0".to_string(),
        name: Some(name.to_string()),
        status: crate::wire::MobileAgentLaunchSlotStatusWire::Launched,
        artifact_dir: Some(format!("/tmp/sase/agents/{name}")),
        message: None,
    };
    MobileAgentLaunchResultWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        primary: Some(slot.clone()),
        slots: vec![slot],
    }
}

pub(crate) fn sample_fleet_launch_body(
    installation_id: &str,
    operation_id: &str,
) -> Value {
    let intent: sase_core::FleetLaunchIntentWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "prompt": "Do remote work",
            "request_id": operation_id,
            "display_name": "Dispatch demo",
            "name": "mobile-demo",
            "model": "gpt-5",
            "provider": "codex",
            "runtime": "codex",
            "project": {
                "schema_version": 1,
                "provider_ref": "builtin:https",
                "project_id": "sase",
                "revision": null,
                "patch_ref": "patch-123"
            },
            "dry_run": false,
            "follow": true,
            "references": []
        }))
        .unwrap();
    let fingerprint =
        sase_core::fleet_launch_payload_fingerprint(&intent).unwrap();
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

pub(crate) fn state_for_custom_agent_bridge(
    tmp: &TempDir,
    agent_bridge: Arc<dyn AgentHostBridge>,
) -> GatewayState {
    GatewayState::new_with_agent_bridge(
        GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl: Duration::minutes(5),
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
            push_config: PushConfig::default(),
        },
        agent_bridge,
    )
}

pub(crate) fn state_for_agent_bridge(tmp: &TempDir) -> GatewayState {
    let launch = sample_launch_result("mobile-demo");
    state_for_custom_agent_bridge(
        tmp,
        Arc::new(crate::host_bridge::StaticAgentHostBridge {
            list_response: MobileAgentListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                agents: vec![sample_agent_summary("mobile-demo")],
                total_count: 1,
            },
            resume_options_response: MobileAgentResumeOptionsResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                options: vec![crate::wire::MobileAgentResumeOptionWire {
                    id: "mobile-demo-resume".to_string(),
                    agent_name: "mobile-demo".to_string(),
                    kind: crate::wire::MobileAgentResumeOptionKindWire::Resume,
                    label: "Resume".to_string(),
                    prompt_text: "#resume:mobile-demo".to_string(),
                    direct_launch_supported: true,
                }],
            },
            text_launch_response: launch.clone(),
            image_launch_response: launch.clone(),
            kill_response: MobileAgentKillResultWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                name: "mobile-demo".to_string(),
                status: "killed".to_string(),
                pid: Some(4242),
                changed: true,
                message: None,
            },
            retry_response: MobileAgentRetryResultWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                source_agent: "mobile-demo".to_string(),
                launch: launch.clone(),
            },
            fork_response: crate::wire::MobileAgentForkResultWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                source_agent: "mobile-demo".to_string(),
                launch,
            },
        }),
    )
}

#[derive(Debug)]
pub(crate) struct DelayedLaunchBridge {
    pub(crate) home: PathBuf,
    pub(crate) launch_count: Arc<AtomicUsize>,
    pub(crate) delay: StdDuration,
}

#[derive(Debug)]
pub(crate) struct CapturingLaunchBridge {
    pub(crate) home: PathBuf,
    pub(crate) launches: Arc<Mutex<Vec<MobileAgentTextLaunchRequestWire>>>,
}

impl AgentHostBridge for CapturingLaunchBridge {
    fn launch_text(
        &self,
        request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.launches
            .lock()
            .expect("launch capture lock")
            .push(request.clone());
        let name = request
            .name
            .clone()
            .or_else(|| {
                sase_core::prompt_has_identity_directive(&request.prompt)
                    .then(|| "observer".to_string())
            })
            .unwrap_or_else(|| "mobile-demo".to_string());
        seed_fleet_agent(&self.home, &name, true, false);
        Ok(sample_launch_result(&name))
    }

    fn kill_agent(
        &self,
        name: &str,
        _request: &MobileAgentKillRequestWire,
    ) -> Result<MobileAgentKillResultWire, HostBridgeError> {
        Ok(MobileAgentKillResultWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            name: name.to_string(),
            status: "killed".to_string(),
            pid: Some(4242),
            changed: true,
            message: None,
        })
    }
}

impl AgentHostBridge for DelayedLaunchBridge {
    fn launch_text(
        &self,
        _request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.launch_count.fetch_add(1, AtomicOrdering::SeqCst);
        std::thread::sleep(self.delay);
        seed_fleet_agent(&self.home, "mobile-demo", true, false);
        Ok(sample_launch_result("mobile-demo"))
    }

    fn kill_agent(
        &self,
        name: &str,
        _request: &MobileAgentKillRequestWire,
    ) -> Result<MobileAgentKillResultWire, HostBridgeError> {
        Ok(MobileAgentKillResultWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            name: name.to_string(),
            status: "killed".to_string(),
            pid: Some(4242),
            changed: true,
            message: None,
        })
    }
}

#[derive(Debug)]
pub(crate) struct FailingLaunchBridge {
    pub(crate) launch_count: Arc<AtomicUsize>,
}

impl AgentHostBridge for FailingLaunchBridge {
    fn launch_text(
        &self,
        _request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.launch_count.fetch_add(1, AtomicOrdering::SeqCst);
        Err(HostBridgeError::LaunchFailed(
            "agent_bridge:launch-text:invalid-request".to_string(),
        ))
    }
}

pub(crate) fn helper_result() -> crate::wire::MobileHelperResultWire {
    crate::wire::MobileHelperResultWire {
        status: crate::wire::MobileHelperStatusWire::Success,
        message: Some("ok".to_string()),
        warnings: Vec::new(),
        skipped: Vec::new(),
        partial_failure_count: None,
    }
}

pub(crate) fn helper_context() -> crate::wire::MobileHelperProjectContextWire {
    crate::wire::MobileHelperProjectContextWire {
        project: Some("sase".to_string()),
        scope: crate::wire::MobileHelperProjectScopeWire::Explicit,
    }
}

pub(crate) fn sample_bead_summary() -> crate::wire::MobileBeadSummaryWire {
    crate::wire::MobileBeadSummaryWire {
        id: "sase-26.4.1".to_string(),
        title: "Rust helper skeleton".to_string(),
        status: "in_progress".to_string(),
        bead_type: "phase".to_string(),
        tier: None,
        project: Some("sase".to_string()),
        parent_id: Some("sase-26.4".to_string()),
        assignee: Some("sase-26.4.1".to_string()),
        updated_at: Some("2026-05-06T15:08:41Z".to_string()),
        dependency_count: 0,
        block_count: 5,
        child_count: 0,
        plan_path_display: Some(
            "sdd/epics/202605/mobile_gateway_epic_4.md".to_string(),
        ),
        changespec_name: None,
        changespec_status: None,
    }
}

pub(crate) fn state_for_helper_bridge(tmp: &TempDir) -> GatewayState {
    let result = helper_result();
    let context = helper_context();
    let bead_summary = sample_bead_summary();
    let update_job = crate::wire::MobileUpdateJobWire {
        job_id: "job_123".to_string(),
        status: crate::wire::MobileUpdateJobStatusWire::Running,
        started_at: Some("2026-05-06T15:00:00Z".to_string()),
        finished_at: None,
        message: Some("update started".to_string()),
        log_path_display: Some("~/.sase/chat_install/job_123.log".to_string()),
        completion_path_display: None,
    };
    GatewayState::new_with_helper_bridge(
        GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl: Duration::minutes(5),
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
            push_config: PushConfig::default(),
        },
        Arc::new(crate::host_bridge::StaticHelperHostBridge {
            agent_catalog_response: serde_json::from_value(
                serde_json::json!({
                    "schema_version": 1,
                    "status": "ok",
                    "message": "",
                    "entries": []
                }),
            )
            .unwrap(),
            finalizer_catalog_response: serde_json::from_value(
                serde_json::json!({
                    "schema_version": 1,
                    "status": "ok",
                    "message": "",
                    "entries": []
                }),
            )
            .unwrap(),
            changespec_tags_response: MobileChangeSpecTagListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context: context.clone(),
                tags: vec![crate::wire::MobileChangeSpecTagEntryWire {
                    tag: "#gh:feature".to_string(),
                    project: Some("sase".to_string()),
                    patch: "feature".to_string(),
                    title: Some("Feature".to_string()),
                    status: "WIP".to_string(),
                    workflow: Some("gh".to_string()),
                    source_path_display: Some(
                        "~/.sase/projects/sase.sase".to_string(),
                    ),
                }],
                total_count: 1,
            },
            xprompt_catalog_response: MobileXpromptCatalogResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context: context.clone(),
                entries: vec![crate::wire::MobileXpromptCatalogEntryWire {
                    name: "gh".to_string(),
                    display_label: "GitHub workflow".to_string(),
                    insertion: Some("#!gh".to_string()),
                    reference_prefix: Some("#!".to_string()),
                    kind: Some("workflow".to_string()),
                    description: Some("Workflow tag".to_string()),
                    source_bucket: "project".to_string(),
                    project: Some("sase".to_string()),
                    tags: vec!["changespec".to_string()],
                    input_signature: Some("topic".to_string()),
                    inputs: vec![crate::wire::MobileXpromptInputWire {
                        name: "topic".to_string(),
                        r#type: "word".to_string(),
                        description: Some("Workflow topic".to_string()),
                        required: true,
                        default_display: None,
                        position: 0,
                        repeatable: false,
                        choices: Vec::new(),
                    }],
                    is_skill: false,
                    skill_name: None,
                    memory_type: None,
                    content_preview: Some("Use gh".to_string()),
                    source_path_display: Some(
                        "sase/xprompts/gh.md".to_string(),
                    ),
                    definition_path: None,
                    definition_range: None,
                }],
                stats: crate::wire::MobileXpromptCatalogStatsWire {
                    total_count: 1,
                    project_count: 1,
                    skill_count: 0,
                    memory_count: 0,
                    pdf_requested: false,
                },
                catalog_attachment: None,
            },
            snippet_catalog_response: serde_json::from_value(
                serde_json::json!({
                    "schema_version": GATEWAY_WIRE_SCHEMA_VERSION,
                    "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
                    "context": {"project": "sase", "scope": "explicit"},
                    "entries": [],
                    "stats": {"total_count": 0}
                }),
            )
            .unwrap(),
            vcs_repo_catalog_response: serde_json::from_value(
                serde_json::json!({
                    "schema_version": 1,
                    "status": "ok",
                    "error_kind": null,
                    "message": "",
                    "provider_display": "GitHub",
                    "stale": false,
                    "entries": []
                }),
            )
            .unwrap(),
            bead_list_response: MobileBeadListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context: context.clone(),
                beads: vec![bead_summary.clone()],
                total_count: 1,
            },
            bead_show_response: MobileBeadShowResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                context,
                bead: crate::wire::MobileBeadDetailWire {
                    summary: bead_summary,
                    description: Some("Define route skeletons".to_string()),
                    notes: None,
                    design_path_display: Some(
                        "sdd/epics/202605/mobile_gateway_epic_4.md"
                            .to_string(),
                    ),
                    dependencies: Vec::new(),
                    blocks: vec!["sase-26.4.2".to_string()],
                    children: Vec::new(),
                    workspace_display: Some("~/projects/sase".to_string()),
                },
            },
            update_start_response: MobileUpdateStartResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result: result.clone(),
                job: update_job.clone(),
            },
            update_status_response: MobileUpdateStatusResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                result,
                job: update_job,
            },
        }),
    )
}

/// Run a just-written helper script through `/bin/sh` rather than making
/// it executable and exec-ing it directly.
///
/// Exec-ing a file this process only just finished writing races every
/// other thread in the test binary: a concurrent `Command::spawn` forks
/// between `std::fs::write`'s open and close, the forked child inherits
/// the write descriptor, and the exec fails with `ETXTBSY`. Passing the
/// script as an argument means nothing ever execs it.
#[cfg(unix)]
pub(crate) fn sh_bridge_command(script: &std::path::Path) -> Vec<String> {
    vec!["/bin/sh".to_string(), script.to_string_lossy().into_owned()]
}

#[cfg(unix)]
pub(crate) fn state_for_command_agent_bridge(tmp: &TempDir) -> GatewayState {
    let script = tmp.path().join("mobile-agent-bridge");
    std::fs::write(
        &script,
        r##"#!/bin/sh
operation="$3"
request_path="$0.$operation.json"
cat >"$request_path"
case "$operation" in
  list-agents)
printf '%s\n' '{"schema_version":1,"agents":[{"name":"cmd-demo","project":"sase","status":"running","pid":4242,"model":"gpt-5.6-sol","provider":"codex","workspace_number":102,"started_at":"2026-05-06T14:30:00Z","duration_seconds":90,"prompt_snippet":"Command bridge","has_artifact_dir":true,"retry_lineage":{"retry_of_timestamp":null,"retried_as_timestamp":null,"retry_chain_root_timestamp":null,"retry_attempt":null,"parent_agent_name":null},"actions":{"can_resume":true,"can_wait":true,"can_kill":true,"can_retry":true},"display":{"title":"cmd-demo","subtitle":"sase","status_label":"Running"}}],"total_count":1}'
;;
  resume-options)
printf '%s\n' '{"schema_version":1,"options":[{"id":"cmd-demo:resume","agent_name":"cmd-demo","kind":"resume","label":"Resume cmd-demo","prompt_text":"#resume:cmd-demo\n","direct_launch_supported":true}]}'
;;
  launch-text)
printf '%s\n' '{"schema_version":1,"primary":{"slot_id":"0","name":"cmd-demo","status":"launched","artifact_dir":"/tmp/cmd-demo","message":"started pid 4242"},"slots":[{"slot_id":"0","name":"cmd-demo","status":"launched","artifact_dir":"/tmp/cmd-demo","message":"started pid 4242"}]}'
;;
  launch-image)
printf '%s\n' '{"schema_version":1,"primary":{"slot_id":"0","name":"cmd-image","status":"launched","artifact_dir":"/tmp/cmd-image","message":"started pid 4243"},"slots":[{"slot_id":"0","name":"cmd-image","status":"launched","artifact_dir":"/tmp/cmd-image","message":"started pid 4243"}]}'
;;
  kill-agent)
printf '%s\n' '{"schema_version":1,"name":"cmd-demo","status":"killed","pid":4242,"changed":true,"message":"Killed agent"}'
;;
  retry-agent)
printf '%s\n' '{"schema_version":1,"source_agent":"cmd-demo","launch":{"schema_version":1,"primary":{"slot_id":"0","name":"cmd-demo.1","status":"launched","artifact_dir":"/tmp/cmd-demo.1","message":"started pid 4244"},"slots":[{"slot_id":"0","name":"cmd-demo.1","status":"launched","artifact_dir":"/tmp/cmd-demo.1","message":"started pid 4244"}]}}'
;;
  *)
exit 2
;;
esac
"##,
    )
    .unwrap();

    GatewayState::new_with_agent_bridge_command(
        GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl: Duration::minutes(5),
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
            push_config: PushConfig::default(),
        },
        sh_bridge_command(&script),
    )
}

#[cfg(unix)]
pub(crate) fn state_for_command_helper_bridge(
    tmp: &TempDir,
    mode: &str,
) -> GatewayState {
    let script = tmp.path().join(format!("mobile-helper-bridge-{mode}"));
    let script_body = match mode {
        "success" => {
            r##"#!/bin/sh
operation="$3"
request_path="$0.$operation.json"
cat >"$request_path"
case "$operation" in
  changespec-tags)
printf '%s\n' '{"schema_version":1,"result":{"status":"partial_success","message":"loaded tags","warnings":[],"skipped":[{"target":"sase/skipped","reason":"could not detect workflow type"}],"partial_failure_count":1},"context":{"project":"sase","scope":"explicit"},"tags":[{"tag":"#gh:feature","project":"sase","changespec":"feature","title":null,"status":"WIP","workflow":"gh","source_path_display":null}],"total_count":1}'
;;
  xprompt-catalog)
printf '%s\n' '{"schema_version":1,"result":{"status":"success","message":"loaded helper records","warnings":[],"skipped":[],"partial_failure_count":null},"context":{"project":"sase","scope":"explicit"},"entries":[{"name":"bd/work_phase_bead","display_label":"bd/work_phase_bead","insertion":"#bd/work_phase_bead","reference_prefix":"#","kind":"xprompt","description":null,"source_bucket":"built_in","project":null,"tags":["work_phase_bead"],"input_signature":"(bead_id: word)","inputs":[{"name":"bead_id","type":"word","required":true,"default_display":null,"position":0}],"is_skill":false,"content_preview":"Complete a phase bead.","source_path_display":"default_config"}],"stats":{"total_count":1,"project_count":0,"skill_count":0,"pdf_requested":false},"catalog_attachment":null}'
;;
  update-start)
printf '%s\n' '{"schema_version":1,"result":{"status":"success","message":"Update worker started.","warnings":[],"skipped":[],"partial_failure_count":null},"job":{"job_id":"job_123","status":"running","started_at":"2026-05-06T15:00:00Z","finished_at":null,"message":"Update worker started.","log_path_display":"~/.sase/chat_install/logs/install_job_123.log","completion_path_display":"~/.sase/chat_install/completions/job_123.json"}}'
;;
  update-status)
printf '%s\n' '{"schema_version":1,"result":{"status":"success","message":"Update completed successfully.","warnings":[],"skipped":[],"partial_failure_count":null},"job":{"job_id":"job_123","status":"succeeded","started_at":"2026-05-06T15:00:00Z","finished_at":"2026-05-06T15:01:00Z","message":"Update completed successfully.","log_path_display":"~/.sase/chat_install/logs/install_job_123.log","completion_path_display":"~/.sase/chat_install/completions/job_123.json"}}'
;;
  *)
exit 2
;;
esac
"##
        }
        "invalid-json" => {
            r##"#!/bin/sh
cat >/dev/null
printf '%s\n' 'not-json'
"##
        }
        "exit-failure" => {
            r##"#!/bin/sh
cat >/dev/null
exit 2
"##
        }
        "not-found" => {
            r##"#!/bin/sh
cat >/dev/null
exit 4
"##
        }
        _ => panic!("unknown helper bridge script mode: {mode}"),
    };
    std::fs::write(&script, script_body).unwrap();

    GatewayState::new_with_helper_bridge_command(
        GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl: Duration::minutes(5),
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
            push_config: PushConfig::default(),
        },
        sh_bridge_command(&script),
    )
}

pub(crate) fn seed_store_notification(
    tmp: &TempDir,
    notification: &NotificationWire,
) {
    let store_path =
        tmp.path().join("notifications").join("notifications.jsonl");
    sase_core::notifications::append_notification(&store_path, notification)
        .unwrap();
}

pub(crate) async fn pair_device(
    state: GatewayState,
) -> (Value, Value, String, String) {
    let start_body = json!({
        "schema_version": 1,
        "host_label": "workstation"
    });
    let (start_status, start) = json_response_with_state(
        state.clone(),
        json_request("POST", "/api/v1/session/pair/start", start_body),
    )
    .await;
    assert_eq!(start_status, StatusCode::OK);

    let finish_body = json!({
        "schema_version": 1,
        "pairing_id": start["pairing_id"],
        "code": start["code"],
        "device": {
            "display_name": "Pixel 9",
            "platform": "android",
            "app_version": "0.1.0"
        }
    });
    let (finish_status, finish) = json_response_with_state(
        state,
        json_request("POST", "/api/v1/session/pair/finish", finish_body),
    )
    .await;
    assert_eq!(finish_status, StatusCode::OK);
    let token = finish["token"].as_str().unwrap().to_string();
    let device_id = finish["device"]["device_id"].as_str().unwrap().to_string();
    (start, finish, token, device_id)
}

pub(crate) fn seed_fleet_agent(
    home: &std::path::Path,
    name: &str,
    live: bool,
    proc: bool,
) -> std::path::PathBuf {
    use sase_core::agent_scan::AgentArtifactScanOptionsWire;
    let projects = home.join("projects");
    let project = projects.join("sase");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(
        project.join("sase.sase"),
        format!(
            "NAME: sase\nWORKSPACE_DIR: {}\nPROJECT_STATE: enabled\n",
            project.display()
        ),
    )
    .unwrap();
    let artifact = project
        .join("artifacts")
        .join("ace-run")
        .join(chrono::Utc::now().format("%Y%m%d%H%M%S").to_string());
    std::fs::create_dir_all(&artifact).unwrap();
    let mut meta = serde_json::json!({
        "name": name,
        "model": "gpt-5",
        "llm_provider": "codex"
    });
    if proc {
        meta["proc_id"] = serde_json::json!("proc-1");
    }
    std::fs::write(
        artifact.join("agent_meta.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    if live {
        std::fs::write(
            artifact.join("running.json"),
            serde_json::to_vec(&serde_json::json!({
                "pid": i64::from(std::process::id())
            }))
            .unwrap(),
        )
        .unwrap();
    } else {
        std::fs::write(
            artifact.join("done.json"),
            serde_json::to_vec(&serde_json::json!({
                "name": name,
                "status": "completed"
            }))
            .unwrap(),
        )
        .unwrap();
    }
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    artifact
}

pub(crate) async fn enroll_mutate(
    state: &GatewayState,
    scopes: &[&str],
) -> (String, String) {
    let bootstrap = fleet_bootstrap(state, scopes, None);
    let (enroll_status, enrolled) = json_response_with_state(
        state.clone(),
        fleet_enroll_request(fleet_enroll_body(
            &bootstrap,
            scopes,
            vec![crate::wire::FLEET_PROTOCOL_VERSION],
        )),
    )
    .await;
    assert_eq!(enroll_status, StatusCode::OK);
    (
        enrolled["token"].as_str().unwrap().to_string(),
        bootstrap.pinned_installation_id,
    )
}

pub(crate) async fn first_summary(
    state: &GatewayState,
) -> sase_core::ResolvedAgentSummaryWire {
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
    page.page.rows.into_iter().next().expect("seeded fleet row")
}
