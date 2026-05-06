use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use axum::{
    extract::{rejection::JsonRejection, Path as AxumPath, Query, State},
    http::{header, HeaderMap, StatusCode, Uri},
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
    MobileNotificationDetailResponseWire, MobileNotificationListResponseWire,
    NotificationWire, PlanActionChoiceWire, PlanActionRequestWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};
use serde::Deserialize;
use tower_http::trace::TraceLayer;

use crate::host_bridge::{
    DynNotificationHostBridge, HostBridgeError, LocalJsonlNotificationBridge,
    NotificationHostBridge,
};
use crate::storage::{
    format_time, generate_pairing_code, generate_prefixed_id,
    AuditLogEntryWire, DeviceTokenStore, StoreError,
};
use crate::wire::{
    ApiErrorCodeWire, ApiErrorWire, DeviceRecordWire, EventPayloadWire,
    EventRecordWire, GatewayBindWire, GatewayBuildWire, HealthResponseWire,
    PairFinishRequestWire, PairFinishResponseWire, PairStartRequestWire,
    PairStartResponseWire, SessionResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

const DEFAULT_EVENT_BUFFER_CAPACITY: usize = 128;
const DEFAULT_HEARTBEAT_INTERVAL: StdDuration = StdDuration::from_secs(30);

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
        })
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
        .route("/api/v1/notifications", get(list_notifications))
        .route("/api/v1/notifications/:id", get(notification_detail))
        .route("/api/v1/actions/plan/:prefix/approve", post(plan_approve))
        .route("/api/v1/actions/plan/:prefix/run", post(plan_run))
        .route("/api/v1/actions/plan/:prefix/reject", post(plan_reject))
        .route("/api/v1/actions/plan/:prefix/epic", post(plan_epic))
        .route("/api/v1/actions/plan/:prefix/legend", post(plan_legend))
        .route("/api/v1/actions/plan/:prefix/feedback", post(plan_feedback))
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
    let attachments = notification
        .files
        .iter()
        .enumerate()
        .map(|(index, path)| {
            let metadata =
                state.notification_bridge.notification_file_metadata(path);
            mobile_attachment_manifest_from_path(
                &notification.id,
                index,
                normalize_home_path(path),
                metadata.byte_size,
                metadata.path_available,
            )
        })
        .collect();
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

    fn state_for_tmp(tmp: &TempDir, pairing_ttl: Duration) -> GatewayState {
        GatewayState::new_with_options(GatewayStateOptions {
            bind_addr: "127.0.0.1:0".to_string(),
            sase_home: tmp.path().to_path_buf(),
            pairing_ttl,
            host_label: "test-host".to_string(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: StdDuration::from_secs(60),
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
        GatewayState::new_with_notification_bridge(
            GatewayStateOptions {
                bind_addr: "127.0.0.1:0".to_string(),
                sase_home: tmp.path().to_path_buf(),
                pairing_ttl: Duration::minutes(5),
                host_label: "test-host".to_string(),
                event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
                heartbeat_interval: StdDuration::from_secs(60),
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
            },
            Arc::new(
                crate::host_bridge::StaticNotificationHostBridge::new_with_action_states(
                    notifications,
                    action_states,
                ),
            ),
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
            json!(["session.read", "events.read"])
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
