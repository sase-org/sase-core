use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use axum::{
    body::Body,
    extract::{rejection::JsonRejection, Path as AxumPath, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sase_core::notifications::{
    mobile_action_detail_from_notification,
    mobile_attachment_manifest_from_path, mobile_notification_card_from_wire,
    mobile_notification_priority_from_wire, ActionResultWire,
    HitlActionChoiceWire, HitlActionRequestWire,
    MobileNotificationDetailResponseWire, MobileNotificationListResponseWire,
    NotificationWire, PlanActionChoiceWire, PlanActionRequestWire,
    QuestionActionChoiceWire, QuestionActionRequestWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};
use serde::Deserialize;
use tower_http::trace::TraceLayer;

use crate::host_bridge::{
    AgentHostBridge, CommandAgentHostBridge, DynAgentHostBridge,
    DynNotificationHostBridge, HostBridgeError, LocalJsonlNotificationBridge,
    NotificationHostBridge, UnavailableAgentHostBridge,
};
use crate::storage::{
    format_time, generate_pairing_code, generate_prefixed_id,
    AuditLogEntryWire, DeviceTokenStore, StoreError,
};
use crate::wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, GatewayBindWire, GatewayBuildWire, HealthResponseWire,
    MobileAgentImageLaunchRequestWire, MobileAgentKillRequestWire,
    MobileAgentKillResultWire, MobileAgentLaunchResultWire,
    MobileAgentListRequestWire, MobileAgentListResponseWire,
    MobileAgentResumeOptionsResponseWire, MobileAgentRetryRequestWire,
    MobileAgentRetryResultWire, MobileAgentTextLaunchRequestWire,
    NotificationStateMutationResponseWire, PairFinishRequestWire,
    PairFinishResponseWire, PairStartRequestWire, PairStartResponseWire,
    SessionResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

const DEFAULT_EVENT_BUFFER_CAPACITY: usize = 128;
const DEFAULT_HEARTBEAT_INTERVAL: StdDuration = StdDuration::from_secs(30);
const DEFAULT_MAX_ATTACHMENT_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct GatewayState {
    bind: GatewayBindWire,
    build: GatewayBuildWire,
    token_store: DeviceTokenStore,
    pairings: Arc<Mutex<HashMap<String, PairingChallenge>>>,
    pairing_ttl: ChronoDuration,
    host_label: String,
    event_hub: EventHub,
    heartbeat_interval: StdDuration,
    notification_bridge: DynNotificationHostBridge,
    agent_bridge: DynAgentHostBridge,
    attachment_tokens: AttachmentTokenStore,
}

impl GatewayState {
    pub fn new(bind_addr: String) -> Self {
        Self::new_with_sase_home(bind_addr, default_sase_home())
    }

    pub fn new_with_sase_home(
        bind_addr: String,
        sase_home: impl Into<PathBuf>,
    ) -> Self {
        Self::new_with_options(GatewayStateOptions {
            bind_addr,
            sase_home: sase_home.into(),
            pairing_ttl: ChronoDuration::minutes(5),
            host_label: default_host_label(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
        })
    }

    pub fn new_with_sase_home_and_agent_bridge_command(
        bind_addr: String,
        sase_home: impl Into<PathBuf>,
        command: Vec<String>,
    ) -> Self {
        let sase_home = sase_home.into();
        let mut state = Self::new_with_sase_home(bind_addr, sase_home.clone());
        state.agent_bridge = DynAgentHostBridge::new(Arc::new(
            CommandAgentHostBridge::new_with_sase_home(command, sase_home),
        ));
        state
    }

    pub fn new_with_options(options: GatewayStateOptions) -> Self {
        let is_loopback = options
            .bind_addr
            .parse::<SocketAddr>()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false);
        let state_dir = options.sase_home.join("mobile_gateway");
        Self {
            bind: GatewayBindWire {
                address: options.bind_addr,
                is_loopback,
            },
            build: GatewayBuildWire {
                package_version: env!("CARGO_PKG_VERSION").to_string(),
                git_sha: None,
            },
            token_store: DeviceTokenStore::new(state_dir),
            pairings: Arc::new(Mutex::new(HashMap::new())),
            pairing_ttl: options.pairing_ttl,
            host_label: options.host_label,
            event_hub: EventHub::new(options.event_buffer_capacity),
            heartbeat_interval: options.heartbeat_interval,
            notification_bridge: DynNotificationHostBridge::new(Arc::new(
                LocalJsonlNotificationBridge::new(&options.sase_home),
            )),
            agent_bridge: DynAgentHostBridge::new(Arc::new(
                UnavailableAgentHostBridge,
            )),
            attachment_tokens: AttachmentTokenStore::new(
                options.attachment_token_ttl,
                options.max_attachment_bytes,
            ),
        }
    }

    pub fn new_with_notification_bridge(
        options: GatewayStateOptions,
        notification_bridge: Arc<dyn NotificationHostBridge>,
    ) -> Self {
        let mut state = Self::new_with_options(options);
        state.notification_bridge =
            DynNotificationHostBridge::new(notification_bridge);
        state
    }

    pub fn new_with_agent_bridge(
        options: GatewayStateOptions,
        agent_bridge: Arc<dyn AgentHostBridge>,
    ) -> Self {
        let mut state = Self::new_with_options(options);
        state.agent_bridge = DynAgentHostBridge::new(agent_bridge);
        state
    }

    pub fn new_with_agent_bridge_command(
        options: GatewayStateOptions,
        command: Vec<String>,
    ) -> Self {
        let sase_home = options.sase_home.clone();
        Self::new_with_agent_bridge(
            options,
            Arc::new(CommandAgentHostBridge::new_with_sase_home(
                command, sase_home,
            )),
        )
    }

    pub fn new_with_bridges(
        options: GatewayStateOptions,
        notification_bridge: Arc<dyn NotificationHostBridge>,
        agent_bridge: Arc<dyn AgentHostBridge>,
    ) -> Self {
        let mut state = Self::new_with_options(options);
        state.notification_bridge =
            DynNotificationHostBridge::new(notification_bridge);
        state.agent_bridge = DynAgentHostBridge::new(agent_bridge);
        state
    }

    pub fn token_store(&self) -> DeviceTokenStore {
        self.token_store.clone()
    }
}

#[derive(Clone, Debug)]
pub struct GatewayStateOptions {
    pub bind_addr: String,
    pub sase_home: PathBuf,
    pub pairing_ttl: ChronoDuration,
    pub host_label: String,
    pub event_buffer_capacity: usize,
    pub heartbeat_interval: StdDuration,
    pub attachment_token_ttl: ChronoDuration,
    pub max_attachment_bytes: u64,
}

#[derive(Clone, Debug)]
struct PairingChallenge {
    code: String,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct EventHub {
    inner: Arc<Mutex<EventHubInner>>,
    buffer_capacity: usize,
}

#[derive(Debug)]
struct EventHubInner {
    next_id: u64,
    buffer: VecDeque<EventRecordWire>,
}

#[derive(Clone, Debug)]
struct AttachmentTokenStore {
    inner: Arc<Mutex<HashMap<String, AttachmentTokenRecord>>>,
    ttl: ChronoDuration,
    max_bytes: u64,
}

#[derive(Clone, Debug)]
struct AttachmentTokenRecord {
    device_id: String,
    canonical_path: PathBuf,
    source_notification_id: String,
    display_name: String,
    content_type: Option<String>,
    byte_size: u64,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct AttachmentMintRequest {
    device_id: String,
    canonical_path: PathBuf,
    source_notification_id: String,
    display_name: String,
    content_type: Option<String>,
    byte_size: u64,
}

#[derive(Debug)]
enum AttachmentTokenLookup {
    Found(AttachmentTokenRecord),
    Missing,
    Expired,
    WrongDevice,
}

impl AttachmentTokenStore {
    fn new(ttl: ChronoDuration, max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            max_bytes,
        }
    }

    fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn mint(
        &self,
        request: AttachmentMintRequest,
        now: DateTime<Utc>,
    ) -> Result<String, ApiError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("attachment_tokens"))?;
        inner.retain(|_, record| record.expires_at > now);
        let token = generate_prefixed_id("att");
        inner.insert(
            token.clone(),
            AttachmentTokenRecord {
                device_id: request.device_id,
                canonical_path: request.canonical_path,
                source_notification_id: request.source_notification_id,
                display_name: request.display_name,
                content_type: request.content_type,
                byte_size: request.byte_size,
                expires_at: now + self.ttl,
            },
        );
        Ok(token)
    }

    fn resolve(
        &self,
        token: &str,
        device_id: &str,
        now: DateTime<Utc>,
    ) -> Result<AttachmentTokenLookup, ApiError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("attachment_tokens"))?;
        let Some(record) = inner.get(token).cloned() else {
            return Ok(AttachmentTokenLookup::Missing);
        };
        if record.expires_at <= now {
            inner.remove(token);
            return Ok(AttachmentTokenLookup::Expired);
        }
        if record.device_id != device_id {
            return Ok(AttachmentTokenLookup::WrongDevice);
        }
        Ok(AttachmentTokenLookup::Found(record))
    }
}

impl EventHub {
    fn new(buffer_capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(EventHubInner {
                next_id: 1,
                buffer: VecDeque::new(),
            })),
            buffer_capacity,
        }
    }

    fn append(
        &self,
        make_payload: impl FnOnce(u64) -> EventPayloadWire,
    ) -> Result<EventRecordWire, ApiError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("events"))?;
        let sequence = inner.next_id;
        inner.next_id += 1;
        let record = EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: format_event_id(sequence),
            created_at: format_time(Utc::now()),
            payload: make_payload(sequence),
        };
        inner.buffer.push_back(record.clone());
        while inner.buffer.len() > self.buffer_capacity {
            inner.buffer.pop_front();
        }
        Ok(record)
    }

    fn replay_after(
        &self,
        last_event_id: &str,
    ) -> Result<Option<Vec<EventRecordWire>>, ApiError> {
        let Some(last_seen) = parse_event_id(last_event_id) else {
            return Ok(None);
        };
        let inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("events"))?;
        let Some(oldest) = inner
            .buffer
            .front()
            .and_then(|record| parse_event_id(&record.id))
        else {
            return Ok(None);
        };
        if last_seen.saturating_add(1) < oldest {
            return Ok(None);
        }
        let events = inner
            .buffer
            .iter()
            .filter(|record| {
                parse_event_id(&record.id)
                    .map(|id| id > last_seen)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        Ok(Some(events))
    }
}

pub fn default_sase_home() -> PathBuf {
    std::env::var_os("SASE_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".sase"))
        })
        .unwrap_or_else(|| PathBuf::from(".sase"))
}

fn default_host_label() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "sase-host".to_string())
}

pub fn app(bind_addr: impl Into<String>) -> Router {
    app_with_state(GatewayState::new(bind_addr.into()))
}

pub fn app_with_state(state: GatewayState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/session/pair/start", post(pair_start))
        .route("/api/v1/session/pair/finish", post(pair_finish))
        .route("/api/v1/session", get(session))
        .route("/api/v1/events", get(events))
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/resume-options", get(agent_resume_options))
        .route("/api/v1/agents/launch", post(agent_launch))
        .route("/api/v1/agents/launch-image", post(agent_launch_image))
        .route("/api/v1/agents/:name/kill", post(agent_kill))
        .route("/api/v1/agents/:name/retry", post(agent_retry))
        .route("/api/v1/notifications", get(list_notifications))
        .route("/api/v1/notifications/:id", get(notification_detail))
        .route(
            "/api/v1/notifications/:id/mark-read",
            post(mark_notification_read),
        )
        .route(
            "/api/v1/notifications/:id/dismiss",
            post(dismiss_notification),
        )
        .route("/api/v1/attachments/:token", get(download_attachment))
        .route("/api/v1/actions/plan/:prefix/approve", post(plan_approve))
        .route("/api/v1/actions/plan/:prefix/run", post(plan_run))
        .route("/api/v1/actions/plan/:prefix/reject", post(plan_reject))
        .route("/api/v1/actions/plan/:prefix/epic", post(plan_epic))
        .route("/api/v1/actions/plan/:prefix/legend", post(plan_legend))
        .route("/api/v1/actions/plan/:prefix/feedback", post(plan_feedback))
        .route("/api/v1/actions/hitl/:prefix/accept", post(hitl_accept))
        .route("/api/v1/actions/hitl/:prefix/reject", post(hitl_reject))
        .route("/api/v1/actions/hitl/:prefix/feedback", post(hitl_feedback))
        .route(
            "/api/v1/actions/question/:prefix/answer",
            post(question_answer),
        )
        .route(
            "/api/v1/actions/question/:prefix/custom",
            post(question_custom),
        )
        .fallback(unknown_route)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health(State(state): State<GatewayState>) -> Json<HealthResponseWire> {
    Json(HealthResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        status: "ok".to_string(),
        service: "sase_gateway".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        build: state.build,
        bind: state.bind,
    })
}

async fn pair_start(
    State(state): State<GatewayState>,
    payload: Result<Json<PairStartRequestWire>, JsonRejection>,
) -> Result<Json<PairStartResponseWire>, ApiError> {
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    let now = Utc::now();
    let pairing_id = generate_prefixed_id("pair");
    let code = generate_pairing_code();
    let host_label = payload
        .host_label
        .unwrap_or_else(|| state.host_label.clone());
    let challenge = PairingChallenge {
        code: code.clone(),
        expires_at: now + state.pairing_ttl,
    };
    state
        .pairings
        .lock()
        .map_err(|_| ApiError::internal("pairings"))?
        .insert(pairing_id.clone(), challenge);
    state.audit(None, "/api/v1/session/pair/start", None, "success");
    Ok(Json(PairStartResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        pairing_id,
        code,
        expires_at: format_time(now + state.pairing_ttl),
        host_label,
        host_fingerprint: None,
    }))
}

async fn pair_finish(
    State(state): State<GatewayState>,
    payload: Result<Json<PairFinishRequestWire>, JsonRejection>,
) -> Result<Json<PairFinishResponseWire>, ApiError> {
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    let now = Utc::now();
    let challenge = {
        let mut pairings = state
            .pairings
            .lock()
            .map_err(|_| ApiError::internal("pairings"))?;
        pairings.remove(&payload.pairing_id)
    };
    let Some(challenge) = challenge else {
        state.audit(
            None,
            "/api/v1/session/pair/finish",
            Some(payload.pairing_id),
            "pairing_rejected",
        );
        return Err(ApiError::pairing_rejected("pairing_id"));
    };
    if challenge.expires_at <= now {
        state.audit(
            None,
            "/api/v1/session/pair/finish",
            Some(payload.pairing_id),
            "pairing_expired",
        );
        return Err(ApiError::pairing_expired("code"));
    }
    if challenge.code != payload.code {
        state.audit(
            None,
            "/api/v1/session/pair/finish",
            Some(payload.pairing_id),
            "pairing_rejected",
        );
        return Err(ApiError::pairing_rejected("code"));
    }

    let (device, token) = state
        .token_store
        .pair_device(payload.device, now)
        .map_err(ApiError::from_store)?;
    state.audit(
        Some(device.device_id.clone()),
        "/api/v1/session/pair/finish",
        None,
        "success",
    );
    Ok(Json(PairFinishResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        device,
        token_type: "bearer".to_string(),
        token,
    }))
}

async fn session(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<SessionResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/session").await?;
    Ok(Json(SessionResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        device,
        capabilities: vec![
            "session.read".to_string(),
            "events.read".to_string(),
            "agents.read".to_string(),
            "agents.launch".to_string(),
            "agents.lifecycle.write".to_string(),
            "attachments.download".to_string(),
            "notifications.state.write".to_string(),
        ],
    }))
}

async fn events(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/events").await?;
    let initial_events = initial_events_for_stream(&state, &headers, &device)?;
    let stream_state = state.clone();
    let stream = async_stream::stream! {
        for record in initial_events {
            yield Ok::<_, Infallible>(sse_event(record));
        }
        let mut interval = tokio::time::interval(stream_state.heartbeat_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            let Ok(record) = stream_state.event_hub.append(|sequence| {
                EventPayloadWire::Heartbeat { sequence }
            }) else {
                break;
            };
            yield Ok::<_, Infallible>(sse_event(record));
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AgentListQuery {
    #[serde(default)]
    include_recent: bool,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn list_agents(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<AgentListQuery>,
) -> Result<Json<MobileAgentListResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/agents").await?;
    let request = MobileAgentListRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        include_recent: query.include_recent,
        status: query.status,
        project: query.project,
        device_id: Some(device.device_id),
        limit: query.limit,
    };
    state
        .agent_bridge
        .list_agents(&request)
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

async fn agent_resume_options(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<MobileAgentResumeOptionsResponseWire>, ApiError> {
    authenticate(&state, &headers, "/api/v1/agents/resume-options").await?;
    state
        .agent_bridge
        .resume_options()
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

async fn agent_launch(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<MobileAgentTextLaunchRequestWire>, JsonRejection>,
) -> Result<Json<MobileAgentLaunchResultWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/agents/launch").await?;
    let Json(mut payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    payload.device_id = Some(device.device_id.clone());
    match state.agent_bridge.launch_text(&payload) {
        Ok(result) => {
            let primary_name = launch_primary_name(&result);
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/launch",
                primary_name.clone(),
                "success",
            );
            publish_agents_changed(&state, "launch", primary_name)?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/launch",
                None,
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn agent_launch_image(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<MobileAgentImageLaunchRequestWire>, JsonRejection>,
) -> Result<Json<MobileAgentLaunchResultWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/agents/launch-image").await?;
    let Json(mut payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    payload.device_id = Some(device.device_id.clone());
    match state.agent_bridge.launch_image(&payload) {
        Ok(result) => {
            let primary_name = launch_primary_name(&result);
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/launch-image",
                primary_name.clone(),
                "success",
            );
            publish_agents_changed(&state, "launch_image", primary_name)?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/launch-image",
                None,
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn agent_kill(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    payload: Result<Json<MobileAgentKillRequestWire>, JsonRejection>,
) -> Result<Json<MobileAgentKillResultWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/agents/{name}/kill").await?;
    let Json(mut payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    payload.device_id = Some(device.device_id.clone());
    match state.agent_bridge.kill_agent(&name, &payload) {
        Ok(result) => {
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/{name}/kill",
                Some(result.name.clone()),
                &result.status,
            );
            if result.changed {
                publish_agents_changed(
                    &state,
                    "kill",
                    Some(result.name.clone()),
                )?;
            }
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/{name}/kill",
                Some(name),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn agent_retry(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    payload: Result<Json<MobileAgentRetryRequestWire>, JsonRejection>,
) -> Result<Json<MobileAgentRetryResultWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/agents/{name}/retry").await?;
    let Json(mut payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    payload.device_id = Some(device.device_id.clone());
    match state.agent_bridge.retry_agent(&name, &payload) {
        Ok(result) => {
            let primary_name = launch_primary_name(&result.launch)
                .or_else(|| Some(result.source_agent.clone()));
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/{name}/retry",
                Some(result.source_agent.clone()),
                "success",
            );
            publish_agents_changed(&state, "retry", primary_name)?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                "/api/v1/agents/{name}/retry",
                Some(name),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct NotificationListQuery {
    #[serde(default)]
    unread: bool,
    #[serde(default)]
    unread_only: bool,
    #[serde(default)]
    include_dismissed: bool,
    #[serde(default)]
    include_silent: bool,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    newer_than: Option<String>,
}

async fn list_notifications(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<NotificationListQuery>,
) -> Result<Json<MobileNotificationListResponseWire>, ApiError> {
    authenticate(&state, &headers, "/api/v1/notifications").await?;
    let snapshot = state
        .notification_bridge
        .list_notifications(query.include_dismissed)
        .map_err(ApiError::from_host_bridge)?;
    let mut rows = filtered_notifications(snapshot.notifications, &query);
    sort_newest_first(&mut rows);
    let total_count = rows.len() as u64;
    if let Some(limit) = query.limit {
        rows.truncate(limit as usize);
    }
    let next_high_water = rows.first().map(|row| row.timestamp.clone());
    let notifications = rows
        .iter()
        .map(|row| {
            let action_state = state.notification_bridge.action_state(row);
            mobile_notification_card_from_wire(
                row,
                action_state,
                mobile_notification_priority_from_wire(row),
            )
        })
        .collect();
    Ok(Json(MobileNotificationListResponseWire {
        schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
        notifications,
        total_count,
        next_high_water,
    }))
}

async fn notification_detail(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<MobileNotificationDetailResponseWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/notifications/{id}").await?;
    let snapshot = state
        .notification_bridge
        .list_notifications(true)
        .map_err(ApiError::from_host_bridge)?;
    let Some(notification) =
        snapshot.notifications.into_iter().find(|row| row.id == id)
    else {
        return Err(ApiError::notification_not_found(id));
    };
    let card = mobile_notification_card_from_wire(
        &notification,
        state.notification_bridge.action_state(&notification),
        mobile_notification_priority_from_wire(&notification),
    );
    let attachments =
        build_attachment_manifests(&state, &device, &notification)?;
    Ok(Json(MobileNotificationDetailResponseWire {
        schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
        notification: card,
        notes: notification.notes.clone(),
        attachments,
        action: mobile_action_detail_from_notification(
            &notification,
            state.notification_bridge.action_state(&notification),
        ),
    }))
}

async fn mark_notification_read(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<NotificationStateMutationResponseWire>, ApiError> {
    mutate_notification_state(
        state,
        headers,
        id,
        "/api/v1/notifications/{id}/mark-read",
        "mark_read",
        |bridge, id| bridge.mark_notification_read(id),
    )
    .await
}

async fn dismiss_notification(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<NotificationStateMutationResponseWire>, ApiError> {
    mutate_notification_state(
        state,
        headers,
        id,
        "/api/v1/notifications/{id}/dismiss",
        "dismiss",
        |bridge, id| bridge.dismiss_notification(id),
    )
    .await
}

async fn mutate_notification_state(
    state: GatewayState,
    headers: HeaderMap,
    id: String,
    endpoint: &'static str,
    event_reason: &'static str,
    mutation: impl FnOnce(
        &DynNotificationHostBridge,
        &str,
    ) -> Result<
        NotificationStateMutationResponseWire,
        HostBridgeError,
    >,
) -> Result<Json<NotificationStateMutationResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, endpoint).await?;
    match mutation(&state.notification_bridge, &id) {
        Ok(result) => {
            state.audit(
                Some(device.device_id),
                endpoint,
                Some(result.notification_id.clone()),
                "success",
            );
            publish_notifications_changed(
                &state,
                event_reason,
                Some(result.notification_id.clone()),
            )?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                endpoint,
                Some(id),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn download_attachment(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(token): AxumPath<String>,
) -> Result<Response, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/attachments/{token}").await?;
    let record = match state.attachment_tokens.resolve(
        &token,
        &device.device_id,
        Utc::now(),
    )? {
        AttachmentTokenLookup::Found(record) => record,
        AttachmentTokenLookup::Missing | AttachmentTokenLookup::Expired => {
            return Err(ApiError::attachment_expired("token"));
        }
        AttachmentTokenLookup::WrongDevice => {
            state.audit(
                Some(device.device_id),
                "/api/v1/attachments/{token}",
                None,
                "unauthorized",
            );
            return Err(ApiError::unauthorized("authorization"));
        }
    };
    let canonical = std::fs::canonicalize(&record.canonical_path)
        .map_err(|_| ApiError::not_found("attachment"))?;
    if canonical != record.canonical_path {
        return Err(ApiError::invalid_request(
            "attachment",
            "attachment path changed since token mint",
        ));
    }
    let metadata = std::fs::metadata(&canonical)
        .map_err(|_| ApiError::not_found("attachment"))?;
    if !metadata.is_file() {
        return Err(ApiError::invalid_request(
            "attachment",
            "attachment is not a regular file",
        ));
    }
    if metadata.len() != record.byte_size
        || metadata.len() > state.attachment_tokens.max_bytes()
    {
        return Err(ApiError::invalid_request(
            "attachment",
            "attachment size changed since token mint",
        ));
    }
    let bytes = std::fs::read(&canonical)
        .map_err(|_| ApiError::not_found("attachment"))?;
    state.audit(
        Some(device.device_id),
        "/api/v1/attachments/{token}",
        Some(record.source_notification_id.clone()),
        "success",
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&record.byte_size.to_string())
            .map_err(|_| ApiError::internal("content_length"))?,
    );
    if let Some(content_type) = &record.content_type {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type)
                .map_err(|_| ApiError::internal("content_type"))?,
        );
    }
    let disposition = format!(
        "attachment; filename=\"{}\"",
        sanitize_content_disposition_filename(&record.display_name)
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition)
            .map_err(|_| ApiError::internal("content_disposition"))?,
    );
    Ok((headers, Body::from(bytes)).into_response())
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PlanActionBody {
    #[serde(default = "default_mobile_schema_version")]
    schema_version: u32,
    #[serde(default)]
    feedback: Option<String>,
    #[serde(default)]
    commit_plan: Option<bool>,
    #[serde(default)]
    run_coder: Option<bool>,
    #[serde(default)]
    coder_prompt: Option<String>,
    #[serde(default)]
    coder_model: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct HitlActionBody {
    #[serde(default = "default_mobile_schema_version")]
    schema_version: u32,
    #[serde(default)]
    feedback: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct QuestionActionBody {
    #[serde(default = "default_mobile_schema_version")]
    schema_version: u32,
    #[serde(default)]
    question_index: Option<u32>,
    #[serde(default)]
    selected_option_id: Option<String>,
    #[serde(default)]
    selected_option_label: Option<String>,
    #[serde(default)]
    selected_option_index: Option<u32>,
    #[serde(default)]
    custom_answer: Option<String>,
    #[serde(default)]
    global_note: Option<String>,
}

async fn plan_approve(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_plan_action_route(
        state,
        headers,
        prefix,
        PlanActionChoiceWire::Approve,
        payload,
        "/api/v1/actions/plan/{prefix}/approve",
    )
    .await
}

async fn plan_run(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_plan_action_route(
        state,
        headers,
        prefix,
        PlanActionChoiceWire::Run,
        payload,
        "/api/v1/actions/plan/{prefix}/run",
    )
    .await
}

async fn plan_reject(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_plan_action_route(
        state,
        headers,
        prefix,
        PlanActionChoiceWire::Reject,
        payload,
        "/api/v1/actions/plan/{prefix}/reject",
    )
    .await
}

async fn plan_epic(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_plan_action_route(
        state,
        headers,
        prefix,
        PlanActionChoiceWire::Epic,
        payload,
        "/api/v1/actions/plan/{prefix}/epic",
    )
    .await
}

async fn plan_legend(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_plan_action_route(
        state,
        headers,
        prefix,
        PlanActionChoiceWire::Legend,
        payload,
        "/api/v1/actions/plan/{prefix}/legend",
    )
    .await
}

async fn plan_feedback(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_plan_action_route(
        state,
        headers,
        prefix,
        PlanActionChoiceWire::Feedback,
        payload,
        "/api/v1/actions/plan/{prefix}/feedback",
    )
    .await
}

async fn execute_plan_action_route(
    state: GatewayState,
    headers: HeaderMap,
    prefix: String,
    choice: PlanActionChoiceWire,
    payload: Result<Json<PlanActionBody>, JsonRejection>,
    endpoint: &'static str,
) -> Result<Json<ActionResultWire>, ApiError> {
    let device = authenticate(&state, &headers, endpoint).await?;
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    let request = PlanActionRequestWire {
        schema_version: payload.schema_version,
        prefix: prefix.clone(),
        choice,
        feedback: payload.feedback,
        commit_plan: payload.commit_plan,
        run_coder: payload.run_coder,
        coder_prompt: payload.coder_prompt,
        coder_model: payload.coder_model,
    };
    match state.notification_bridge.execute_plan_action(&request) {
        Ok(result) => {
            state.audit(
                Some(device.device_id),
                endpoint,
                result.notification_id.clone().or(Some(prefix)),
                "success",
            );
            publish_notifications_changed(
                &state,
                "plan_action",
                result.notification_id.clone(),
            )?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                endpoint,
                Some(prefix),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn hitl_accept(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<HitlActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_hitl_action_route(
        state,
        headers,
        prefix,
        HitlActionChoiceWire::Accept,
        payload,
        "/api/v1/actions/hitl/{prefix}/accept",
    )
    .await
}

async fn hitl_reject(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<HitlActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_hitl_action_route(
        state,
        headers,
        prefix,
        HitlActionChoiceWire::Reject,
        payload,
        "/api/v1/actions/hitl/{prefix}/reject",
    )
    .await
}

async fn hitl_feedback(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<HitlActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_hitl_action_route(
        state,
        headers,
        prefix,
        HitlActionChoiceWire::Feedback,
        payload,
        "/api/v1/actions/hitl/{prefix}/feedback",
    )
    .await
}

async fn execute_hitl_action_route(
    state: GatewayState,
    headers: HeaderMap,
    prefix: String,
    choice: HitlActionChoiceWire,
    payload: Result<Json<HitlActionBody>, JsonRejection>,
    endpoint: &'static str,
) -> Result<Json<ActionResultWire>, ApiError> {
    let device = authenticate(&state, &headers, endpoint).await?;
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    let request = HitlActionRequestWire {
        schema_version: payload.schema_version,
        prefix: prefix.clone(),
        choice,
        feedback: payload.feedback,
    };
    match state.notification_bridge.execute_hitl_action(&request) {
        Ok(result) => {
            state.audit(
                Some(device.device_id),
                endpoint,
                result.notification_id.clone().or(Some(prefix)),
                "success",
            );
            publish_notifications_changed(
                &state,
                "hitl_action",
                result.notification_id.clone(),
            )?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                endpoint,
                Some(prefix),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn question_answer(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<QuestionActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_question_action_route(
        state,
        headers,
        prefix,
        QuestionActionChoiceWire::Answer,
        payload,
        "/api/v1/actions/question/{prefix}/answer",
    )
    .await
}

async fn question_custom(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<QuestionActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    execute_question_action_route(
        state,
        headers,
        prefix,
        QuestionActionChoiceWire::Custom,
        payload,
        "/api/v1/actions/question/{prefix}/custom",
    )
    .await
}

async fn execute_question_action_route(
    state: GatewayState,
    headers: HeaderMap,
    prefix: String,
    choice: QuestionActionChoiceWire,
    payload: Result<Json<QuestionActionBody>, JsonRejection>,
    endpoint: &'static str,
) -> Result<Json<ActionResultWire>, ApiError> {
    let device = authenticate(&state, &headers, endpoint).await?;
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    let request = QuestionActionRequestWire {
        schema_version: payload.schema_version,
        prefix: prefix.clone(),
        choice,
        question_index: payload.question_index,
        selected_option_id: payload.selected_option_id,
        selected_option_label: payload.selected_option_label,
        selected_option_index: payload.selected_option_index,
        custom_answer: payload.custom_answer,
        global_note: payload.global_note,
    };
    match state.notification_bridge.execute_question_action(&request) {
        Ok(result) => {
            state.audit(
                Some(device.device_id),
                endpoint,
                result.notification_id.clone().or(Some(prefix)),
                "success",
            );
            publish_notifications_changed(
                &state,
                "question_action",
                result.notification_id.clone(),
            )?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                endpoint,
                Some(prefix),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn unknown_route(uri: Uri) -> ApiError {
    ApiError::not_found(uri.path())
}

async fn authenticate(
    state: &GatewayState,
    headers: &HeaderMap,
    endpoint: &str,
) -> Result<DeviceRecordWire, ApiError> {
    let token = match bearer_token(headers) {
        Ok(token) => token,
        Err(error) => {
            state.audit(None, endpoint, None, "unauthorized");
            return Err(error);
        }
    };
    let device = state
        .token_store
        .authenticate_token(token, Utc::now())
        .map_err(ApiError::from_store)?;
    let Some(device) = device else {
        state.audit(None, endpoint, None, "unauthorized");
        return Err(ApiError::unauthorized("authorization"));
    };
    state.audit(Some(device.device_id.clone()), endpoint, None, "success");
    Ok(device)
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, ApiError> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthorized("authorization"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("authorization"))?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| ApiError::unauthorized("authorization"))
}

fn validate_schema(schema_version: u32) -> Result<(), ApiError> {
    if schema_version == GATEWAY_WIRE_SCHEMA_VERSION {
        return Ok(());
    }
    Err(ApiError::invalid_request(
        "schema_version",
        "unsupported schema_version",
    ))
}

fn default_mobile_schema_version() -> u32 {
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION
}

fn default_attachment_token_ttl() -> ChronoDuration {
    ChronoDuration::minutes(5)
}

fn publish_notifications_changed(
    state: &GatewayState,
    reason: &str,
    notification_id: Option<String>,
) -> Result<(), ApiError> {
    state
        .event_hub
        .append(|_| EventPayloadWire::NotificationsChanged {
            reason: reason.to_string(),
            notification_id,
        })?;
    Ok(())
}

fn publish_agents_changed(
    state: &GatewayState,
    reason: &str,
    agent_name: Option<String>,
) -> Result<(), ApiError> {
    state
        .event_hub
        .append(|_| EventPayloadWire::AgentsChanged {
            reason: reason.to_string(),
            agent_name,
            timestamp: Some(format_time(Utc::now())),
        })?;
    Ok(())
}

fn launch_primary_name(result: &MobileAgentLaunchResultWire) -> Option<String> {
    result
        .primary
        .as_ref()
        .and_then(|slot| slot.name.clone())
        .or_else(|| result.slots.iter().find_map(|slot| slot.name.clone()))
}

fn initial_events_for_stream(
    state: &GatewayState,
    headers: &HeaderMap,
    device: &DeviceRecordWire,
) -> Result<Vec<EventRecordWire>, ApiError> {
    let mut events = Vec::new();
    if let Some(last_event_id) = last_event_id(headers)? {
        match state.event_hub.replay_after(last_event_id)? {
            Some(replay) => events.extend(replay),
            None => events.push(state.event_hub.append(|_| {
                EventPayloadWire::ResyncRequired {
                    reason: "last_event_id_not_available".to_string(),
                }
            })?),
        }
    } else {
        events.push(state.event_hub.append(|_| EventPayloadWire::Session {
            device_id: device.device_id.clone(),
        })?);
    }
    events.push(
        state
            .event_hub
            .append(|sequence| EventPayloadWire::Heartbeat { sequence })?,
    );
    Ok(events)
}

fn last_event_id(headers: &HeaderMap) -> Result<Option<&str>, ApiError> {
    headers
        .get("last-event-id")
        .map(|value| {
            value.to_str().map_err(|_| {
                ApiError::invalid_request(
                    "last-event-id",
                    "invalid Last-Event-ID header",
                )
            })
        })
        .transpose()
}

fn sse_event(record: EventRecordWire) -> Event {
    let event_name = event_name(&record.payload);
    let data = serde_json::to_string(&record)
        .expect("EventRecordWire serialization should be infallible");
    Event::default().id(record.id).event(event_name).data(data)
}

fn event_name(payload: &EventPayloadWire) -> &'static str {
    match payload {
        EventPayloadWire::Heartbeat { .. } => "heartbeat",
        EventPayloadWire::Session { .. } => "session",
        EventPayloadWire::ResyncRequired { .. } => "resync_required",
        EventPayloadWire::NotificationsChanged { .. } => {
            "notifications_changed"
        }
        EventPayloadWire::AgentsChanged { .. } => "agents_changed",
    }
}

fn filtered_notifications(
    notifications: Vec<NotificationWire>,
    query: &NotificationListQuery,
) -> Vec<NotificationWire> {
    notifications
        .into_iter()
        .filter(|row| !query.unread && !query.unread_only || !row.read)
        .filter(|row| query.include_silent || !row.silent)
        .filter(|row| {
            query
                .newer_than
                .as_deref()
                .map(|high_water| {
                    timestamp_is_newer(&row.timestamp, high_water)
                })
                .unwrap_or(true)
        })
        .collect()
}

fn sort_newest_first(notifications: &mut [NotificationWire]) {
    notifications.sort_by(|left, right| {
        timestamp_sort_key(&right.timestamp)
            .cmp(&timestamp_sort_key(&left.timestamp))
            .then_with(|| right.id.cmp(&left.id))
    });
}

fn timestamp_sort_key(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc).to_rfc3339())
        .unwrap_or_else(|_| value.to_string())
}

fn timestamp_is_newer(candidate: &str, high_water: &str) -> bool {
    timestamp_sort_key(candidate) > timestamp_sort_key(high_water)
}

fn normalize_home_path(path: &str) -> String {
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return path.to_string();
    };
    let home = home.to_string_lossy();
    if path == home {
        return "~".to_string();
    }
    path.strip_prefix(&format!("{home}/"))
        .map(|rest| format!("~/{rest}"))
        .unwrap_or_else(|| path.to_string())
}

#[derive(Debug, Clone)]
struct AttachmentCandidate {
    raw_path: String,
}

fn build_attachment_manifests(
    state: &GatewayState,
    device: &DeviceRecordWire,
    notification: &NotificationWire,
) -> Result<Vec<sase_core::notifications::MobileAttachmentManifestWire>, ApiError>
{
    let candidates = attachment_candidates(notification);
    let mut manifests = Vec::with_capacity(candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        let metadata = state
            .notification_bridge
            .notification_file_metadata(&candidate.raw_path);
        let mut manifest = mobile_attachment_manifest_from_path(
            &notification.id,
            index,
            normalize_home_path(&candidate.raw_path),
            metadata.byte_size,
            metadata.path_available,
        );
        if metadata.path_available {
            let path = expand_home_path(&candidate.raw_path);
            match validate_attachment_path(
                &path,
                metadata.byte_size,
                state.attachment_tokens.max_bytes(),
            ) {
                Ok((canonical, byte_size)) => {
                    let token = state.attachment_tokens.mint(
                        AttachmentMintRequest {
                            device_id: device.device_id.clone(),
                            canonical_path: canonical,
                            source_notification_id: notification.id.clone(),
                            display_name: manifest.display_name.clone(),
                            content_type: manifest.content_type.clone(),
                            byte_size,
                        },
                        Utc::now(),
                    )?;
                    manifest.token = Some(token);
                    manifest.downloadable = true;
                    manifest.path_available = true;
                    manifest.byte_size = Some(byte_size);
                }
                Err(_) => {
                    manifest.token = None;
                    manifest.downloadable = false;
                }
            }
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}

fn attachment_candidates(
    notification: &NotificationWire,
) -> Vec<AttachmentCandidate> {
    let mut paths = Vec::new();
    for path in &notification.files {
        push_unique_path(&mut paths, path);
    }
    for key in [
        "plan_file",
        "pdf_path",
        "plan_pdf_path",
        "diff_path",
        "error_report_path",
        "project_file",
        "agent_project_file",
        "output_path",
        "response_path",
        "image_path",
    ] {
        if let Some(path) = notification.action_data.get(key) {
            push_unique_path(&mut paths, path);
        }
    }
    match notification.action.as_deref() {
        Some("PlanApproval") => {
            if let Some(dir) = action_path(notification, "response_dir") {
                push_unique_path(
                    &mut paths,
                    &dir.join("plan_request.json").to_string_lossy(),
                );
            }
        }
        Some("HITL") => {
            if let Some(dir) = action_path(notification, "artifacts_dir") {
                let request_path = dir.join("hitl_request.json");
                push_unique_path(&mut paths, &request_path.to_string_lossy());
                for path in hitl_path_typed_outputs(&request_path) {
                    push_unique_path(&mut paths, &path);
                }
            }
        }
        Some("UserQuestion") => {
            if let Some(dir) = action_path(notification, "response_dir") {
                push_unique_path(
                    &mut paths,
                    &dir.join("question_request.json").to_string_lossy(),
                );
            }
        }
        _ => {}
    }
    paths
        .into_iter()
        .map(|raw_path| AttachmentCandidate { raw_path })
        .collect()
}

fn push_unique_path(paths: &mut Vec<String>, path: &str) {
    let trimmed = path.trim();
    if trimmed.is_empty() || paths.iter().any(|existing| existing == trimmed) {
        return;
    }
    paths.push(trimmed.to_string());
}

fn action_path(notification: &NotificationWire, key: &str) -> Option<PathBuf> {
    let raw = notification.action_data.get(key)?.trim();
    if raw.is_empty() {
        return None;
    }
    Some(expand_home_path(raw))
}

fn expand_home_path(path: &str) -> PathBuf {
    if path == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(path));
    }
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

fn hitl_path_typed_outputs(request_path: &Path) -> Vec<String> {
    let Ok(bytes) = std::fs::read(request_path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Vec::new();
    };
    let Some(output_types) = value
        .get("output_types")
        .and_then(serde_json::Value::as_object)
    else {
        return Vec::new();
    };
    let Some(output) =
        value.get("output").and_then(serde_json::Value::as_object)
    else {
        return Vec::new();
    };
    output_types
        .iter()
        .filter_map(|(field, field_type)| {
            if field_type.as_str() != Some("path") {
                return None;
            }
            output
                .get(field)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn validate_attachment_path(
    path: &Path,
    expected_size: Option<u64>,
    max_bytes: u64,
) -> Result<(PathBuf, u64), ()> {
    if path
        .components()
        .any(|component| component == Component::ParentDir)
        || contains_symlink_component(path)
    {
        return Err(());
    }
    let canonical = std::fs::canonicalize(path).map_err(|_| ())?;
    if contains_symlink_component(&canonical) {
        return Err(());
    }
    let metadata = std::fs::metadata(&canonical).map_err(|_| ())?;
    if !metadata.is_file() {
        return Err(());
    }
    let byte_size = metadata.len();
    if byte_size > max_bytes {
        return Err(());
    }
    if expected_size.is_some_and(|size| size != byte_size) {
        return Err(());
    }
    Ok((canonical, byte_size))
}

fn contains_symlink_component(path: &Path) -> bool {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if std::fs::symlink_metadata(&current)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

fn sanitize_content_disposition_filename(display_name: &str) -> String {
    let name = Path::new(display_name)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("attachment");
    name.chars()
        .map(|ch| match ch {
            '"' | '\\' | '\r' | '\n' => '_',
            _ => ch,
        })
        .collect()
}

fn format_event_id(id: u64) -> String {
    format!("{id:016}")
}

fn parse_event_id(id: &str) -> Option<u64> {
    id.parse::<u64>().ok()
}

impl GatewayState {
    fn audit(
        &self,
        device_id: Option<String>,
        endpoint: &str,
        target_id: Option<String>,
        outcome: &str,
    ) {
        let entry = AuditLogEntryWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            timestamp: format_time(Utc::now()),
            device_id,
            endpoint: endpoint.to_string(),
            target_id,
            outcome: outcome.to_string(),
        };
        let _ = self.token_store.append_audit(entry);
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    wire: ApiErrorWire,
}

impl ApiError {
    fn unauthorized(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Unauthorized,
                message: "authentication is required for this endpoint"
                    .to_string(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn not_found(path: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "route not found".to_string(),
                target: Some(path.into()),
                details: None,
            },
        }
    }

    fn notification_not_found(id: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "notification not found".to_string(),
                target: Some(id.into()),
                details: None,
            },
        }
    }

    fn invalid_request(
        target: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::InvalidRequest,
                message: message.into(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn pairing_expired(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PairingExpired,
                message: "pairing code expired".to_string(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn pairing_rejected(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PairingRejected,
                message: "pairing code rejected".to_string(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn attachment_expired(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AttachmentExpired,
                message: "attachment token is expired".to_string(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn internal(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Internal,
                message: "gateway internal error".to_string(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn from_store(error: StoreError) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Internal,
                message: error.to_string(),
                target: Some("device_store".to_string()),
                details: None,
            },
        }
    }

    fn from_host_bridge(error: HostBridgeError) -> Self {
        let (status, code, target) = match &error {
            HostBridgeError::BridgeUnavailable(target) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ApiErrorCodeWire::BridgeUnavailable,
                target.clone(),
            ),
            HostBridgeError::AgentNotFound(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::AgentNotFound,
                target.clone(),
            ),
            HostBridgeError::AgentNotRunning(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::AgentNotRunning,
                target.clone(),
            ),
            HostBridgeError::LaunchFailed(target) => (
                StatusCode::BAD_GATEWAY,
                ApiErrorCodeWire::LaunchFailed,
                target.clone(),
            ),
            HostBridgeError::InvalidUpload(target) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCodeWire::InvalidUpload,
                target.clone(),
            ),
            HostBridgeError::PermissionDenied(target) => (
                StatusCode::FORBIDDEN,
                ApiErrorCodeWire::PermissionDenied,
                target.clone(),
            ),
            HostBridgeError::NotificationMissing(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::NotFound,
                target.clone(),
            ),
            HostBridgeError::ActionMissing(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::NotFound,
                target.clone(),
            ),
            HostBridgeError::AmbiguousPrefix(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::AmbiguousPrefix,
                target.clone(),
            ),
            HostBridgeError::UnsupportedAction(target) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCodeWire::UnsupportedAction,
                target.clone(),
            ),
            HostBridgeError::ActionAlreadyHandled(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::ConflictAlreadyHandled,
                target.clone(),
            ),
            HostBridgeError::ActionStale(target) => (
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                target.clone(),
            ),
            HostBridgeError::MissingTarget(target)
            | HostBridgeError::InvalidActionRequest(target) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCodeWire::InvalidRequest,
                target.clone(),
            ),
            HostBridgeError::ReadNotifications(_)
            | HostBridgeError::ReadPendingActions(_)
            | HostBridgeError::WriteResponse(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCodeWire::Internal,
                "notification_bridge".to_string(),
            ),
        };
        Self {
            status,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code,
                message: error.to_string(),
                target: Some(target),
                details: None,
            },
        }
    }

    fn from_json_rejection(rejection: JsonRejection) -> Self {
        Self::invalid_request("body", rejection.body_text())
    }
}

impl ApiErrorCodeWire {
    fn outcome_label(&self) -> &'static str {
        match self {
            ApiErrorCodeWire::Unauthorized => "unauthorized",
            ApiErrorCodeWire::NotFound => "not_found",
            ApiErrorCodeWire::InvalidRequest => "invalid_request",
            ApiErrorCodeWire::PairingExpired => "pairing_expired",
            ApiErrorCodeWire::PairingRejected => "pairing_rejected",
            ApiErrorCodeWire::ConflictAlreadyHandled => "already_handled",
            ApiErrorCodeWire::GoneStale => "stale",
            ApiErrorCodeWire::AmbiguousPrefix => "ambiguous_prefix",
            ApiErrorCodeWire::UnsupportedAction => "unsupported_action",
            ApiErrorCodeWire::AttachmentExpired => "attachment_expired",
            ApiErrorCodeWire::AgentNotFound => "agent_not_found",
            ApiErrorCodeWire::AgentNotRunning => "agent_not_running",
            ApiErrorCodeWire::LaunchFailed => "launch_failed",
            ApiErrorCodeWire::InvalidUpload => "invalid_upload",
            ApiErrorCodeWire::BridgeUnavailable => "bridge_unavailable",
            ApiErrorCodeWire::PermissionDenied => "permission_denied",
            ApiErrorCodeWire::Internal => "internal",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.wire)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use chrono::Duration;
    use serde_json::{json, Value};
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;

    async fn json_response(request: Request<Body>) -> (StatusCode, Value) {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        json_response_with_state(state, request).await
    }

    async fn json_response_with_state(
        state: GatewayState,
        request: Request<Body>,
    ) -> (StatusCode, Value) {
        let response = app_with_state(state).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap();
        (status, value)
    }

    async fn raw_response_with_state(
        state: GatewayState,
        request: Request<Body>,
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        let response = app_with_state(state).oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, headers, bytes.to_vec())
    }

    fn state_for_tmp(tmp: &TempDir, pairing_ttl: Duration) -> GatewayState {
        GatewayState::new_with_options(GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl,
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
        })
    }

    fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn session_request(token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri("/api/v1/session");
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    }

    fn events_request(
        token: Option<&str>,
        last_event_id: Option<&str>,
    ) -> Request<Body> {
        let mut builder = Request::builder().uri("/api/v1/events");
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        if let Some(last_event_id) = last_event_id {
            builder = builder.header("last-event-id", last_event_id);
        }
        builder.body(Body::empty()).unwrap()
    }

    fn notifications_request(token: Option<&str>, uri: &str) -> Request<Body> {
        let mut builder = Request::builder().uri(uri);
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    }

    fn notification_state_request(
        token: Option<&str>,
        uri: &str,
    ) -> Request<Body> {
        let mut builder = Request::builder().method("POST").uri(uri);
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    }

    fn attachment_request(token: Option<&str>, uri: &str) -> Request<Body> {
        notifications_request(token, uri)
    }

    fn action_request(
        token: Option<&str>,
        uri: &str,
        body: Value,
    ) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn agent_get_request(token: Option<&str>, uri: &str) -> Request<Body> {
        notifications_request(token, uri)
    }

    fn agent_post_request(
        token: Option<&str>,
        uri: &str,
        body: Value,
    ) -> Request<Body> {
        action_request(token, uri, body)
    }

    fn notification(
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
            files: Vec::new(),
            action: action.map(str::to_string),
            action_data: Default::default(),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
        };
        if action == Some("PlanApproval") {
            notification.action_data.insert(
                "response_dir".to_string(),
                "/tmp/response".to_string(),
            );
        }
        notification
    }

    fn state_for_notifications(
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

    fn state_for_notifications_with_attachment_options(
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
            },
            Arc::new(crate::host_bridge::StaticNotificationHostBridge::new(
                notifications,
            )),
        )
    }

    fn state_for_notifications_with_action_states(
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
            },
            Arc::new(
                crate::host_bridge::StaticNotificationHostBridge::new_with_action_states(
                    notifications,
                    action_states,
                ),
            ),
        )
    }

    fn sample_agent_summary(name: &str) -> crate::wire::MobileAgentSummaryWire {
        crate::wire::MobileAgentSummaryWire {
            name: name.to_string(),
            project: Some("sase".to_string()),
            status: "running".to_string(),
            pid: Some(4242),
            model: Some("gpt-5.5".to_string()),
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

    fn sample_launch_result(name: &str) -> MobileAgentLaunchResultWire {
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

    fn state_for_agent_bridge(tmp: &TempDir) -> GatewayState {
        let launch = sample_launch_result("mobile-demo");
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
            },
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
                        kind:
                            crate::wire::MobileAgentResumeOptionKindWire::Resume,
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
                    launch,
                },
            }),
        )
    }

    #[cfg(unix)]
    fn state_for_command_agent_bridge(tmp: &TempDir) -> GatewayState {
        use std::os::unix::fs::PermissionsExt;

        let script = tmp.path().join("mobile-agent-bridge");
        std::fs::write(
            &script,
            r##"#!/bin/sh
operation="$3"
request_path="$0.$operation.json"
cat >"$request_path"
case "$operation" in
  list-agents)
    printf '%s\n' '{"schema_version":1,"agents":[{"name":"cmd-demo","project":"sase","status":"running","pid":4242,"model":"gpt-5.5","provider":"codex","workspace_number":102,"started_at":"2026-05-06T14:30:00Z","duration_seconds":90,"prompt_snippet":"Command bridge","has_artifact_dir":true,"retry_lineage":{"retry_of_timestamp":null,"retried_as_timestamp":null,"retry_chain_root_timestamp":null,"retry_attempt":null,"parent_agent_name":null},"actions":{"can_resume":true,"can_wait":true,"can_kill":true,"can_retry":true},"display":{"title":"cmd-demo","subtitle":"sase","status_label":"Running"}}],"total_count":1}'
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
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script, permissions).unwrap();

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
            },
            vec![script.to_string_lossy().to_string()],
        )
    }

    fn seed_plan_notification(
        tmp: &TempDir,
        id: &str,
    ) -> (NotificationWire, PathBuf) {
        let response_dir =
            tmp.path().join("agent").join(id).join("plan_approval");
        std::fs::create_dir_all(&response_dir).unwrap();
        std::fs::write(response_dir.join("plan_request.json"), "{}").unwrap();
        let plan_file = tmp.path().join("plan.md");
        std::fs::write(&plan_file, "# Plan\n").unwrap();
        let mut notification =
            notification(id, "2026-05-06T17:00:00Z", Some("PlanApproval"));
        notification.files = vec![plan_file.to_string_lossy().to_string()];
        notification.action_data.insert(
            "response_dir".to_string(),
            response_dir.to_string_lossy().to_string(),
        );
        let store_path =
            tmp.path().join("notifications").join("notifications.jsonl");
        sase_core::notifications::append_notification(
            &store_path,
            &notification,
        )
        .unwrap();
        let pending_path =
            sase_core::notifications::pending_action_store_path(tmp.path());
        let pending =
            sase_core::notifications::pending_action_from_notification(
                &notification,
                sase_core::notifications::current_unix_time(),
            )
            .unwrap();
        sase_core::notifications::register_pending_action(
            &pending_path,
            &pending,
        )
        .unwrap();
        (notification, response_dir)
    }

    fn seed_hitl_notification(
        tmp: &TempDir,
        id: &str,
    ) -> (NotificationWire, PathBuf) {
        let artifacts_dir = tmp.path().join("agent").join(id).join("artifacts");
        std::fs::create_dir_all(&artifacts_dir).unwrap();
        std::fs::write(
            artifacts_dir.join("hitl_request.json"),
            r#"{"step_name":"review","step_type":"bash","output":"ok"}"#,
        )
        .unwrap();
        let mut notification =
            notification(id, "2026-05-06T17:00:00Z", Some("HITL"));
        notification.action_data.insert(
            "artifacts_dir".to_string(),
            artifacts_dir.to_string_lossy().to_string(),
        );
        seed_action_notification(tmp, &notification);
        (notification, artifacts_dir)
    }

    fn seed_question_notification(
        tmp: &TempDir,
        id: &str,
    ) -> (NotificationWire, PathBuf) {
        let response_dir = tmp.path().join("agent").join(id).join("question");
        std::fs::create_dir_all(&response_dir).unwrap();
        std::fs::write(
            response_dir.join("question_request.json"),
            serde_json::to_string_pretty(&json!({
                "questions": [{
                    "question": "Which approach?",
                    "options": [
                        {"id": "fast", "label": "Fast"},
                        {"id": "safe", "label": "Safe"}
                    ]
                }],
                "session_id": "session-question",
                "timestamp": 1778086800.0
            }))
            .unwrap(),
        )
        .unwrap();
        let mut notification =
            notification(id, "2026-05-06T17:00:00Z", Some("UserQuestion"));
        notification.action_data.insert(
            "response_dir".to_string(),
            response_dir.to_string_lossy().to_string(),
        );
        seed_action_notification(tmp, &notification);
        (notification, response_dir)
    }

    fn seed_action_notification(
        tmp: &TempDir,
        notification: &NotificationWire,
    ) {
        let store_path =
            tmp.path().join("notifications").join("notifications.jsonl");
        sase_core::notifications::append_notification(
            &store_path,
            notification,
        )
        .unwrap();
        let pending_path =
            sase_core::notifications::pending_action_store_path(tmp.path());
        let pending =
            sase_core::notifications::pending_action_from_notification(
                notification,
                sase_core::notifications::current_unix_time(),
            )
            .unwrap();
        sase_core::notifications::register_pending_action(
            &pending_path,
            &pending,
        )
        .unwrap();
    }

    fn seed_store_notification(tmp: &TempDir, notification: &NotificationWire) {
        let store_path =
            tmp.path().join("notifications").join("notifications.jsonl");
        sase_core::notifications::append_notification(
            &store_path,
            notification,
        )
        .unwrap();
    }

    async fn pair_device(
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
        let device_id =
            finish["device"]["device_id"].as_str().unwrap().to_string();
        (start, finish, token, device_id)
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
        assert_eq!(value["schema_version"], 1);
        assert!(value["pairing_id"].as_str().unwrap().starts_with("pair_"));
        assert_eq!(value["code"].as_str().unwrap().len(), 6);
        assert_eq!(value["host_label"], "workstation");
        assert!(value.get("token").is_none());
    }

    #[tokio::test]
    async fn pair_finish_persists_device_and_returns_token_once() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));

        let (start, finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

        let (status, value) =
            json_response_with_state(state, session_request(Some(&token)))
                .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["device"]["device_id"], device_id);
        assert_eq!(
            value["capabilities"],
            json!([
                "session.read",
                "events.read",
                "agents.read",
                "agents.launch",
                "agents.lifecycle.write",
                "attachments.download",
                "notifications.state.write"
            ])
        );
    }

    #[tokio::test]
    async fn invalid_and_revoked_tokens_are_unauthorized() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

        let (missing_status, missing) =
            json_response_with_state(state.clone(), session_request(None))
                .await;
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
            json_response_with_state(state, session_request(Some(&token)))
                .await;
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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
            "model": "gpt-5.5",
            "provider": "codex",
            "runtime": "codex",
            "project": "sase",
            "dry_run": false
        });
        let (launch_status, launch) = json_response_with_state(
            state.clone(),
            agent_post_request(
                Some(&token),
                "/api/v1/agents/launch",
                launch_body,
            ),
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

    #[cfg(unix)]
    #[tokio::test]
    async fn command_agent_bridge_routes_return_command_output() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_command_agent_bridge(&tmp);
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

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
            agent_post_request(
                Some(&token),
                "/api/v1/agents/launch",
                launch_body,
            ),
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
        let mut read = notification(
            "read-row",
            "2026-05-06T15:00:00Z",
            Some("PlanApproval"),
        );
        read.read = true;
        let mut silent =
            notification("silent-row", "2026-05-06T16:00:00Z", None);
        silent.silent = true;
        let newest = notification(
            "newest-row",
            "2026-05-06T17:00:00Z",
            Some("PlanApproval"),
        );
        let older = notification("older-row", "2026-05-06T14:00:00Z", None);
        let state =
            state_for_notifications(&tmp, vec![older, newest, silent, read]);
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (status, value) = json_response_with_state(
            state,
            notifications_request(
                Some(&token),
                "/api/v1/notifications?unread=true&limit=1",
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["total_count"], 2);
        assert_eq!(value["next_high_water"], "2026-05-06T17:00:00Z");
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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
    async fn notifications_list_can_include_dismissed_and_silent_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut dismissed =
            notification("dismissed-row", "2026-05-06T15:00:00Z", None);
        dismissed.dismissed = true;
        let mut silent =
            notification("silent-row", "2026-05-06T16:00:00Z", None);
        silent.silent = true;
        let state = state_for_notifications(&tmp, vec![dismissed, silent]);
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (status, value) = json_response_with_state(
            state,
            notifications_request(
                Some(&token),
                "/api/v1/notifications/detail-row",
            ),
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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (detail_status, detail) = json_response_with_state(
            state.clone(),
            notifications_request(
                Some(&token),
                "/api/v1/notifications/download-row",
            ),
        )
        .await;
        assert_eq!(detail_status, StatusCode::OK);
        let attachment_token =
            detail["attachments"][0]["token"].as_str().unwrap();
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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;
        let (_detail_status, detail) = json_response_with_state(
            state.clone(),
            notifications_request(
                Some(&token),
                "/api/v1/notifications/digest-row",
            ),
        )
        .await;
        let attachment_token =
            detail["attachments"][0]["token"].as_str().unwrap();

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
        let attachment_token =
            detail["attachments"][0]["token"].as_str().unwrap();

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;
        let (_detail_status, detail) = json_response_with_state(
            state.clone(),
            notifications_request(
                Some(&token),
                "/api/v1/notifications/json-row",
            ),
        )
        .await;
        let attachment_token =
            detail["attachments"][0]["token"].as_str().unwrap();

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (status, value) = json_response_with_state(
            state,
            notifications_request(
                Some(&token),
                "/api/v1/notifications/unsafe-row",
            ),
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
        let mut row = notification(
            "hitl-artifacts",
            "2026-05-06T15:00:00Z",
            Some("HITL"),
        );
        row.action_data.insert(
            "artifacts_dir".to_string(),
            artifacts_dir.to_string_lossy().to_string(),
        );
        let state = state_for_notifications(&tmp, vec![row]);
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
                }
        }));
    }

    #[tokio::test]
    async fn plan_action_approve_writes_response_and_dismisses_notification() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_notification, response_dir) =
            seed_plan_notification(&tmp, "abcdef12-plan");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (status, value) = json_response_with_state(
            state,
            action_request(
                Some(&token),
                "/api/v1/actions/plan/abcdef12/approve",
                json!({
                    "schema_version": 1,
                    "commit_plan": true,
                    "run_coder": false,
                    "coder_model": "gpt-5.4"
                }),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["notification_id"], "abcdef12-plan");
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    response_dir.join("plan_response.json")
                )
                .unwrap()
            )
            .unwrap(),
            json!({
                "action": "approve",
                "commit_plan": true,
                "run_coder": false,
                "coder_model": "gpt-5.4"
            })
        );
        let snapshot = sase_core::notifications::read_notifications_snapshot(
            &tmp.path().join("notifications").join("notifications.jsonl"),
            true,
        )
        .unwrap();
        assert!(snapshot.notifications[0].dismissed);
        let meta: Value = serde_json::from_str(
            &std::fs::read_to_string(
                response_dir.parent().unwrap().join("agent_meta.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(meta["plan_approved"], true);
        assert_eq!(meta["plan_action"], "commit");
    }

    #[tokio::test]
    async fn plan_action_run_and_feedback_match_existing_response_shapes() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_notification, first_response_dir) =
            seed_plan_notification(&tmp, "run00001-plan");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (run_status, _run_value) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/plan/run00001/run",
                json!({"schema_version": 1, "coder_prompt": "Focus tests"}),
            ),
        )
        .await;
        assert_eq!(run_status, StatusCode::OK);
        let run_response: Value = serde_json::from_str(
            &std::fs::read_to_string(
                first_response_dir.join("plan_response.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            run_response,
            json!({
                "action": "approve",
                "commit_plan": false,
                "run_coder": true,
                "coder_prompt": "Focus tests",
            })
        );

        let (_notification, second_response_dir) =
            seed_plan_notification(&tmp, "feed0001-plan");
        let (feedback_status, _feedback_value) = json_response_with_state(
            state,
            action_request(
                Some(&token),
                "/api/v1/actions/plan/feed0001/feedback",
                json!({"schema_version": 1, "feedback": "Revise scope"}),
            ),
        )
        .await;
        assert_eq!(feedback_status, StatusCode::OK);
        let feedback_response: Value = serde_json::from_str(
            &std::fs::read_to_string(
                second_response_dir.join("plan_response.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            feedback_response,
            json!({"action": "reject", "feedback": "Revise scope"})
        );
    }

    #[tokio::test]
    async fn plan_action_errors_are_deterministic() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_first, _first_dir) = seed_plan_notification(&tmp, "ambig001-one");
        let (_second, _second_dir) =
            seed_plan_notification(&tmp, "ambig002-two");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (ambiguous_status, ambiguous) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/plan/ambig/approve",
                json!({"schema_version": 1}),
            ),
        )
        .await;
        assert_eq!(ambiguous_status, StatusCode::CONFLICT);
        assert_eq!(ambiguous["code"], "ambiguous_prefix");

        let (missing_status, missing) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/plan/missing/approve",
                json!({"schema_version": 1}),
            ),
        )
        .await;
        assert_eq!(missing_status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], "not_found");

        let (_notification, response_dir) =
            seed_plan_notification(&tmp, "handled1-plan");
        std::fs::write(response_dir.join("plan_response.json"), "{}").unwrap();
        let (handled_status, handled) = json_response_with_state(
            state,
            action_request(
                Some(&token),
                "/api/v1/actions/plan/handled1/approve",
                json!({"schema_version": 1}),
            ),
        )
        .await;
        assert_eq!(handled_status, StatusCode::CONFLICT);
        assert_eq!(handled["code"], "conflict_already_handled");
    }

    #[tokio::test]
    async fn hitl_actions_write_response_and_dismiss_notification() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_notification, first_artifacts_dir) =
            seed_hitl_notification(&tmp, "hitl0001-row");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (accept_status, accept_value) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/hitl/hitl0001/accept",
                json!({"schema_version": 1}),
            ),
        )
        .await;
        assert_eq!(accept_status, StatusCode::OK);
        assert_eq!(accept_value["action_kind"], "hitl");
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    first_artifacts_dir.join("hitl_response.json")
                )
                .unwrap()
            )
            .unwrap(),
            json!({"action": "accept", "approved": true})
        );
        let snapshot = sase_core::notifications::read_notifications_snapshot(
            &tmp.path().join("notifications").join("notifications.jsonl"),
            true,
        )
        .unwrap();
        assert!(snapshot
            .notifications
            .iter()
            .any(|row| row.id == "hitl0001-row" && row.dismissed));

        let (_notification, second_artifacts_dir) =
            seed_hitl_notification(&tmp, "hitl0002-row");
        let (feedback_status, _feedback_value) = json_response_with_state(
            state,
            action_request(
                Some(&token),
                "/api/v1/actions/hitl/hitl0002/feedback",
                json!({"schema_version": 1, "feedback": "Needs revision"}),
            ),
        )
        .await;
        assert_eq!(feedback_status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    second_artifacts_dir.join("hitl_response.json")
                )
                .unwrap()
            )
            .unwrap(),
            json!({
                "action": "feedback",
                "approved": false,
                "feedback": "Needs revision"
            })
        );
    }

    #[tokio::test]
    async fn question_actions_support_option_and_custom_answers() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_notification, first_response_dir) =
            seed_question_notification(&tmp, "quest001-row");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (answer_status, answer_value) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/question/quest001/answer",
                json!({
                    "schema_version": 1,
                    "selected_option_id": "safe",
                    "global_note": "Use durable path"
                }),
            ),
        )
        .await;
        assert_eq!(answer_status, StatusCode::OK);
        assert_eq!(answer_value["action_kind"], "user_question");
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    first_response_dir.join("question_response.json")
                )
                .unwrap()
            )
            .unwrap(),
            json!({
                "answers": [{
                    "question": "Which approach?",
                    "selected": ["Safe"],
                    "custom_feedback": null
                }],
                "global_note": "Use durable path"
            })
        );

        let (_notification, second_response_dir) =
            seed_question_notification(&tmp, "quest002-row");
        let (custom_status, _custom_value) = json_response_with_state(
            state,
            action_request(
                Some(&token),
                "/api/v1/actions/question/quest002/custom",
                json!({
                    "schema_version": 1,
                    "custom_answer": "Use SQLite",
                    "global_note": "Small local DB"
                }),
            ),
        )
        .await;
        assert_eq!(custom_status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    second_response_dir.join("question_response.json")
                )
                .unwrap()
            )
            .unwrap(),
            json!({
                "answers": [{
                    "question": "Which approach?",
                    "selected": [],
                    "custom_feedback": "Use SQLite"
                }],
                "global_note": "Small local DB"
            })
        );
    }

    #[tokio::test]
    async fn hitl_and_question_action_errors_are_deterministic() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_hitl, artifacts_dir) =
            seed_hitl_notification(&tmp, "hitlerr1-row");
        let (_question, response_dir) =
            seed_question_notification(&tmp, "questerr-row");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        std::fs::write(artifacts_dir.join("hitl_response.json"), "{}").unwrap();
        let (handled_status, handled) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/hitl/hitlerr1/accept",
                json!({"schema_version": 1}),
            ),
        )
        .await;
        assert_eq!(handled_status, StatusCode::CONFLICT);
        assert_eq!(handled["code"], "conflict_already_handled");

        let (invalid_status, invalid) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/question/questerr/answer",
                json!({"schema_version": 1, "selected_option_id": "missing"}),
            ),
        )
        .await;
        assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid["code"], "invalid_request");
        assert!(!response_dir.join("question_response.json").exists());

        std::fs::remove_file(response_dir.join("question_request.json"))
            .unwrap();
        let (stale_status, stale) = json_response_with_state(
            state,
            action_request(
                Some(&token),
                "/api/v1/actions/question/questerr/custom",
                json!({"schema_version": 1, "custom_answer": "later"}),
            ),
        )
        .await;
        assert_eq!(stale_status, StatusCode::CONFLICT);
        assert_eq!(stale["code"], "conflict_already_handled");
        assert!(!response_dir.join("question_response.json").exists());
    }

    #[tokio::test]
    async fn mobile_notification_actions_attachment_smoke_harness() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (plan, plan_response_dir) =
            seed_plan_notification(&tmp, "smokepln-row");
        let (hitl, hitl_artifacts_dir) =
            seed_hitl_notification(&tmp, "smokehit-row");
        let (question, question_response_dir) =
            seed_question_notification(&tmp, "smokeqst-row");
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

        let (list_status, list) = json_response_with_state(
            state.clone(),
            notifications_request(
                Some(&token),
                "/api/v1/notifications?unread=true",
            ),
        )
        .await;
        assert_eq!(list_status, StatusCode::OK);
        let listed_ids: Vec<&str> = list["notifications"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert!(listed_ids.contains(&plan.id.as_str()));
        assert!(listed_ids.contains(&hitl.id.as_str()));
        assert!(listed_ids.contains(&question.id.as_str()));

        let (detail_status, detail) = json_response_with_state(
            state.clone(),
            notifications_request(
                Some(&token),
                &format!("/api/v1/notifications/{}", plan.id),
            ),
        )
        .await;
        assert_eq!(detail_status, StatusCode::OK);
        assert_eq!(detail["action"]["kind"], "plan_approval");
        let attachment_token =
            detail["attachments"][0]["token"].as_str().unwrap();
        let (download_status, headers, body) = raw_response_with_state(
            state.clone(),
            attachment_request(
                Some(&token),
                &format!("/api/v1/attachments/{attachment_token}"),
            ),
        )
        .await;
        assert_eq!(download_status, StatusCode::OK);
        assert_eq!(headers["content-type"], "text/markdown");
        assert_eq!(body, b"# Plan\n");

        let (plan_status, plan_result) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/plan/smokepln/approve",
                json!({
                    "schema_version": 1,
                    "commit_plan": true,
                    "run_coder": false
                }),
            ),
        )
        .await;
        assert_eq!(plan_status, StatusCode::OK);
        assert_eq!(plan_result["notification_id"], plan.id.as_str());
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    plan_response_dir.join("plan_response.json"),
                )
                .unwrap(),
            )
            .unwrap(),
            json!({
                "action": "approve",
                "commit_plan": true,
                "run_coder": false
            })
        );

        let (hitl_status, _hitl_result) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/hitl/smokehit/accept",
                json!({"schema_version": 1}),
            ),
        )
        .await;
        assert_eq!(hitl_status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    hitl_artifacts_dir.join("hitl_response.json"),
                )
                .unwrap(),
            )
            .unwrap(),
            json!({"action": "accept", "approved": true})
        );

        let (question_status, _question_result) = json_response_with_state(
            state.clone(),
            action_request(
                Some(&token),
                "/api/v1/actions/question/smokeqst/answer",
                json!({
                    "schema_version": 1,
                    "selected_option_index": 1,
                    "global_note": "smoke verified"
                }),
            ),
        )
        .await;
        assert_eq!(question_status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(
                    question_response_dir.join("question_response.json"),
                )
                .unwrap(),
            )
            .unwrap(),
            json!({
                "answers": [{
                    "question": "Which approach?",
                    "selected": ["Safe"],
                    "custom_feedback": null
                }],
                "global_note": "smoke verified"
            })
        );

        let snapshot = sase_core::notifications::read_notifications_snapshot(
            &tmp.path().join("notifications").join("notifications.jsonl"),
            true,
        )
        .unwrap();
        for id in [&plan.id, &hitl.id, &question.id] {
            assert!(
                snapshot
                    .notifications
                    .iter()
                    .any(|row| row.id == id.as_str() && row.dismissed),
                "{id} should be dismissed after its mobile action",
            );
        }
    }

    #[tokio::test]
    async fn event_resume_replays_buffered_records_after_last_event_id() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_start, finish, _token, _device_id) =
            pair_device(state.clone()).await;
        let device: DeviceRecordWire =
            serde_json::from_value(finish["device"].clone()).unwrap();

        let first_events =
            initial_events_for_stream(&state, &HeaderMap::new(), &device)
                .unwrap();
        assert_eq!(first_events.len(), 2);
        assert_eq!(first_events[0].id, "0000000000000001");
        assert!(matches!(
            first_events[0].payload,
            EventPayloadWire::Session { .. }
        ));
        assert_eq!(first_events[1].id, "0000000000000002");
        assert!(matches!(
            first_events[1].payload,
            EventPayloadWire::Heartbeat { sequence: 2 }
        ));

        let mut headers = HeaderMap::new();
        headers.insert("last-event-id", "0000000000000001".parse().unwrap());
        let replay_events =
            initial_events_for_stream(&state, &headers, &device).unwrap();

        assert_eq!(replay_events.len(), 2);
        assert_eq!(replay_events[0], first_events[1]);
        assert_eq!(replay_events[1].id, "0000000000000003");
        assert!(matches!(
            replay_events[1].payload,
            EventPayloadWire::Heartbeat { sequence: 3 }
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
        });
        let (_start, finish, _token, _device_id) =
            pair_device(state.clone()).await;
        let device: DeviceRecordWire =
            serde_json::from_value(finish["device"].clone()).unwrap();

        let _first_events =
            initial_events_for_stream(&state, &HeaderMap::new(), &device)
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("last-event-id", "0000000000000000".parse().unwrap());
        let events =
            initial_events_for_stream(&state, &headers, &device).unwrap();

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
        headers.insert("last-event-id", first_events[1].id.parse().unwrap());
        let events =
            initial_events_for_stream(&restarted_state, &headers, &device)
                .unwrap();

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
}
