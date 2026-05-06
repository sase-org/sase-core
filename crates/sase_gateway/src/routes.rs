use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use axum::{
    extract::{rejection::JsonRejection, State},
    http::{header, HeaderMap, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tower_http::trace::TraceLayer;

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
        }
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
    }
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

    fn from_json_rejection(rejection: JsonRejection) -> Self {
        Self::invalid_request("body", rejection.body_text())
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
