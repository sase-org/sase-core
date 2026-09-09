use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    convert::Infallible,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use axum::{
    body::Body,
    extract::{
        rejection::JsonRejection, DefaultBodyLimit, Path as AxumPath, Query,
        State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sase_core::notifications::{
    mobile_action_detail_from_notification,
    mobile_attachment_manifest_from_path, mobile_notification_card_from_wire,
    mobile_notification_error_from_wire,
    mobile_notification_priority_from_wire, notification_activity_at,
    ActionResultWire, GateActionRequestWire, MobileActionKindWire,
    MobileNotificationDetailResponseWire, MobileNotificationListResponseWire,
    NotificationWire, QuestionActionChoiceWire, QuestionActionRequestWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tower_http::trace::TraceLayer;

#[cfg(test)]
use sase_core::notifications::MobileActionStateWire;

use crate::fleet_attention::{FleetAttentionStore, FleetAttentionStoreError};
use crate::fleet_auth::{
    credential_has_scope, current_unix_time, fleet_capabilities,
    negotiate_fleet_protocol_version, FleetAuthentication,
    FleetCredentialStore, FleetEnrollmentResult, FleetStoreError,
    FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE,
    FLEET_SCOPE_BATCH_READ, FLEET_SCOPE_CATALOG_READ, FLEET_SCOPE_CONTENT_READ,
    FLEET_SCOPE_DETAIL_READ, FLEET_SCOPE_EVENTS_READ, FLEET_SCOPE_HELLO,
    FLEET_SCOPE_LAUNCH, FLEET_SCOPE_MUTATE, FLEET_SCOPE_PROJECTS_READ,
    FLEET_SCOPE_REVOKE, FLEET_SCOPE_ROTATE, FLEET_SCOPE_SUMMARY_READ,
};
use crate::fleet_launch::{FleetLaunchStore, FleetLaunchStoreError};
use crate::fleet_mutations::{FleetMutationStore, FleetMutationStoreError};
use crate::fleet_reads::{resync_item, FleetReadError, FleetReadService};
use crate::host_bridge::{
    AgentHostBridge, CommandAgentHostBridge, CommandHelperHostBridge,
    DynAgentHostBridge, DynHelperHostBridge, DynNotificationHostBridge,
    HelperHostBridge, HostBridgeError, LocalJsonlNotificationBridge,
    NotificationHostBridge, UnavailableAgentHostBridge,
    UnavailableHelperHostBridge,
};
use crate::push::{PushConfig, PushDispatcher};
use crate::storage::{
    format_time, generate_pairing_code, generate_prefixed_id,
    AuditLogEntryWire, DeviceTokenStore, StoreError,
};
use crate::wire::{
    default_fleet_protocol_versions, ApiErrorCodeWire, ApiErrorWire,
    DeviceRecordWire, EventPayloadWire, EventRecordWire, FleetCatalogQueryWire,
    FleetContentReadRequestWire, FleetContentReadResponseWire,
    FleetCredentialRecordWire, FleetCredentialRevokeRequestWire,
    FleetCredentialRevokeResponseWire, FleetDetailRequestWire,
    FleetDetailResponseWire, FleetEnrollmentRequestWire,
    FleetEnrollmentResponseWire, FleetEventStreamItemWire, FleetHealthWire,
    FleetHelloResponseWire, FleetLaunchRequestWire, FleetLaunchResponseWire,
    FleetLogicalBatchRequestWire, FleetLogicalBatchResponseWire,
    FleetMutationRequestWire, FleetMutationResponseWire,
    FleetProjectEligibilityRequestWire, FleetProjectEligibilityResponseWire,
    FleetResyncReasonWire, FleetSummaryResponseWire,
    FleetTokenRotateRequestWire, FleetTokenRotateResponseWire, GatewayBindWire,
    GatewayBuildWire, HealthResponseWire, MobileAgentForkRequestWire,
    MobileAgentImageLaunchRequestWire, MobileAgentKillRequestWire,
    MobileAgentKillResultWire, MobileAgentLaunchResultWire,
    MobileAgentListRequestWire, MobileAgentListResponseWire,
    MobileAgentResumeOptionsResponseWire, MobileAgentRetryRequestWire,
    MobileAgentRetryResultWire, MobileAgentTextLaunchRequestWire,
    MobileBeadListRequestWire, MobileBeadListResponseWire,
    MobileBeadShowRequestWire, MobileBeadShowResponseWire,
    MobileChangeSpecTagListRequestWire, MobileChangeSpecTagListResponseWire,
    MobileUpdateStartRequestWire, MobileUpdateStartResponseWire,
    MobileUpdateStatusRequestWire, MobileUpdateStatusResponseWire,
    MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
    NotificationStateMutationResponseWire, PairFinishRequestWire,
    PairFinishResponseWire, PairStartRequestWire, PairStartResponseWire,
    PushSubscriptionDeleteResponseWire, PushSubscriptionListResponseWire,
    PushSubscriptionRegisterResponseWire, PushSubscriptionRequestWire,
    SessionResponseWire, StoreCursorWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

const DEFAULT_EVENT_BUFFER_CAPACITY: usize = 128;
const DEFAULT_HEARTBEAT_INTERVAL: StdDuration = StdDuration::from_secs(30);
const DEFAULT_MAX_ATTACHMENT_BYTES: u64 = 20 * 1024 * 1024;
const FLEET_REQUEST_BODY_LIMIT_BYTES: usize = 16 * 1024;
const FLEET_ENROLLMENT_RATE_LIMIT: usize = 8;
const FLEET_ENROLLMENT_RATE_WINDOW_SECONDS: i64 = 60;
const FLEET_PROTOCOL_VERSIONS_HEADER: &str = "x-sase-fleet-protocol-versions";

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
    helper_bridge: DynHelperHostBridge,
    attachment_tokens: AttachmentTokenStore,
    push_dispatcher: PushDispatcher,
    fleet_store: FleetCredentialStore,
    fleet_launches: FleetLaunchStore,
    fleet_mutations: FleetMutationStore,
    fleet_attention: FleetAttentionStore,
    fleet_reads: FleetReadService,
    fleet_enrollment_limiter: FleetEnrollmentRateLimiter,
    machine_selector: String,
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
            push_config: PushConfig::default(),
        })
    }

    pub fn new_with_sase_home_and_agent_bridge_command(
        bind_addr: String,
        sase_home: impl Into<PathBuf>,
        command: Vec<String>,
    ) -> Self {
        Self::new_with_sase_home_and_bridge_commands(
            bind_addr,
            sase_home,
            command,
            CommandHelperHostBridge::default_command(),
            PushConfig::default(),
        )
    }

    pub fn new_with_sase_home_and_bridge_commands(
        bind_addr: String,
        sase_home: impl Into<PathBuf>,
        agent_command: Vec<String>,
        helper_command: Vec<String>,
        push_config: PushConfig,
    ) -> Self {
        let sase_home = sase_home.into();
        let mut state = Self::new_with_options(GatewayStateOptions {
            bind_addr,
            sase_home: sase_home.clone(),
            pairing_ttl: ChronoDuration::minutes(5),
            host_label: default_host_label(),
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            attachment_token_ttl: default_attachment_token_ttl(),
            max_attachment_bytes: DEFAULT_MAX_ATTACHMENT_BYTES,
            push_config,
        });
        state.agent_bridge = DynAgentHostBridge::new(Arc::new(
            CommandAgentHostBridge::new_with_sase_home(
                agent_command,
                sase_home.clone(),
            ),
        ));
        state.helper_bridge = DynHelperHostBridge::new(Arc::new(
            CommandHelperHostBridge::new_with_sase_home(
                helper_command,
                sase_home,
            ),
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
        let machine_selector = default_machine_selector(&options.host_label);
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
            helper_bridge: DynHelperHostBridge::new(Arc::new(
                UnavailableHelperHostBridge,
            )),
            attachment_tokens: AttachmentTokenStore::new(
                options.attachment_token_ttl,
                options.max_attachment_bytes,
            ),
            push_dispatcher: PushDispatcher::new(options.push_config),
            fleet_store: FleetCredentialStore::new(options.sase_home.clone()),
            fleet_launches: FleetLaunchStore::new(options.sase_home.clone()),
            fleet_mutations: FleetMutationStore::new(options.sase_home.clone()),
            fleet_attention: FleetAttentionStore::new(
                options.sase_home.clone(),
            ),
            fleet_reads: FleetReadService::new(options.sase_home.clone()),
            fleet_enrollment_limiter: FleetEnrollmentRateLimiter::new(
                FLEET_ENROLLMENT_RATE_LIMIT,
                ChronoDuration::seconds(FLEET_ENROLLMENT_RATE_WINDOW_SECONDS),
            ),
            machine_selector,
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

    pub fn new_with_helper_bridge(
        options: GatewayStateOptions,
        helper_bridge: Arc<dyn HelperHostBridge>,
    ) -> Self {
        let mut state = Self::new_with_options(options);
        state.helper_bridge = DynHelperHostBridge::new(helper_bridge);
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

    pub fn new_with_helper_bridge_command(
        options: GatewayStateOptions,
        command: Vec<String>,
    ) -> Self {
        let sase_home = options.sase_home.clone();
        Self::new_with_helper_bridge(
            options,
            Arc::new(CommandHelperHostBridge::new_with_sase_home(
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

    pub fn push_dispatcher(&self) -> PushDispatcher {
        self.push_dispatcher.clone()
    }

    pub fn fleet_store(&self) -> FleetCredentialStore {
        self.fleet_store.clone()
    }

    pub fn fleet_reads(&self) -> FleetReadService {
        self.fleet_reads.clone()
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
    pub push_config: PushConfig,
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
    sender: tokio::sync::broadcast::Sender<EventRecordWire>,
}

#[derive(Debug)]
struct EventHubInner {
    next_id: u64,
    buffer: VecDeque<EventRecordWire>,
}

struct EventHubSubscription {
    initial_events: Vec<EventRecordWire>,
    receiver: tokio::sync::broadcast::Receiver<EventRecordWire>,
}

impl std::ops::Deref for EventHubSubscription {
    type Target = [EventRecordWire];

    fn deref(&self) -> &Self::Target {
        &self.initial_events
    }
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

#[derive(Clone, Debug)]
struct FleetEnrollmentRateLimiter {
    inner: Arc<Mutex<VecDeque<DateTime<Utc>>>>,
    limit: usize,
    window: ChronoDuration,
}

impl FleetEnrollmentRateLimiter {
    fn new(limit: usize, window: ChronoDuration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
            limit,
            window,
        }
    }

    fn check(&self, now: DateTime<Utc>) -> Result<bool, ApiError> {
        let mut attempts = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("fleet_enrollment_limiter"))?;
        while attempts
            .front()
            .is_some_and(|attempt| *attempt + self.window <= now)
        {
            attempts.pop_front();
        }
        if attempts.len() >= self.limit {
            return Ok(false);
        }
        attempts.push_back(now);
        Ok(true)
    }
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
        let (sender, _) = tokio::sync::broadcast::channel(
            buffer_capacity.max(1).saturating_mul(2),
        );
        Self {
            inner: Arc::new(Mutex::new(EventHubInner {
                next_id: 1,
                buffer: VecDeque::new(),
            })),
            buffer_capacity,
            sender,
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
        Ok(self.append_with_inner(&mut inner, make_payload))
    }

    fn append_with_inner(
        &self,
        inner: &mut EventHubInner,
        make_payload: impl FnOnce(u64) -> EventPayloadWire,
    ) -> EventRecordWire {
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
        let _ = self.sender.send(record.clone());
        record
    }

    #[cfg(test)]
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
        Ok(replay_events_locked(&inner, last_seen))
    }

    fn subscribe_after(
        &self,
        last_event_id: Option<&str>,
        make_initial_payload: impl FnOnce(u64) -> EventPayloadWire,
    ) -> Result<EventHubSubscription, ApiError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("events"))?;
        let (initial_events, receiver) = match last_event_id {
            Some(last_event_id) => {
                let receiver = self.sender.subscribe();
                let Some(last_seen) = parse_event_id(last_event_id) else {
                    return Ok(EventHubSubscription {
                        initial_events: vec![self.transient_event(
                            &inner,
                            EventPayloadWire::ResyncRequired {
                                reason: "last_event_id_not_available"
                                    .to_string(),
                            },
                        )],
                        receiver,
                    });
                };
                let initial_events = replay_events_locked(&inner, last_seen)
                    .unwrap_or_else(|| {
                        vec![self.transient_event(
                            &inner,
                            EventPayloadWire::ResyncRequired {
                                reason: "last_event_id_not_available"
                                    .to_string(),
                            },
                        )]
                    });
                (initial_events, receiver)
            }
            None => {
                let initial_event =
                    self.append_with_inner(&mut inner, make_initial_payload);
                let receiver = self.sender.subscribe();
                (vec![initial_event], receiver)
            }
        };
        Ok(EventHubSubscription {
            initial_events,
            receiver,
        })
    }

    fn transient_event(
        &self,
        inner: &EventHubInner,
        payload: EventPayloadWire,
    ) -> EventRecordWire {
        let sequence = inner.next_id.saturating_sub(1);
        EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: format_event_id(sequence),
            created_at: format_time(Utc::now()),
            payload,
        }
    }

    fn current_sequence(&self) -> u64 {
        self.inner
            .lock()
            .map(|inner| inner.next_id.saturating_sub(1))
            .unwrap_or(0)
    }
}

fn replay_events_locked(
    inner: &EventHubInner,
    last_seen: u64,
) -> Option<Vec<EventRecordWire>> {
    let oldest = inner
        .buffer
        .front()
        .and_then(|record| parse_event_id(&record.id))?;
    if last_seen.saturating_add(1) < oldest {
        return None;
    }
    Some(
        inner
            .buffer
            .iter()
            .filter(|record| {
                parse_event_id(&record.id)
                    .map(|id| id > last_seen)
                    .unwrap_or(false)
            })
            .cloned()
            .collect(),
    )
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

fn default_machine_selector(host_label: &str) -> String {
    let normalized = host_label.trim();
    if normalized.is_empty() {
        "sase-host".to_string()
    } else {
        normalized.to_string()
    }
}

pub fn app(bind_addr: impl Into<String>) -> Router {
    app_with_state(GatewayState::new(bind_addr.into()))
}

pub fn app_with_state(state: GatewayState) -> Router {
    Router::new()
        .nest("/api/fleet/v1", fleet_v1_routes())
        .route("/api/v1/health", get(health))
        .route("/api/v1/session/pair/start", post(pair_start))
        .route("/api/v1/session/pair/finish", post(pair_finish))
        .route("/api/v1/session", get(session))
        .route(
            "/api/v1/session/push-subscriptions",
            get(list_push_subscriptions).post(register_push_subscription),
        )
        .route(
            "/api/v1/session/push-subscriptions/:id",
            delete(delete_push_subscription),
        )
        .route("/api/v1/events", get(events))
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/resume-options", get(agent_resume_options))
        .route("/api/v1/agents/launch", post(agent_launch))
        .route("/api/v1/agents/launch-image", post(agent_launch_image))
        .route("/api/v1/agents/:name/kill", post(agent_kill))
        .route("/api/v1/agents/:name/retry", post(agent_retry))
        .route("/api/v1/changespec-tags", get(list_changespec_tags))
        .route("/api/v1/xprompts/catalog", get(xprompt_catalog))
        .route("/api/v1/beads", get(list_beads))
        .route("/api/v1/beads/:id", get(show_bead))
        .route("/api/v1/update/start", post(update_start))
        .route("/api/v1/update/:job_id", get(update_status))
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
        .route("/api/v1/actions/gate/:prefix", post(gate_action))
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

fn fleet_v1_routes() -> Router<GatewayState> {
    Router::new()
        .route("/enroll", post(fleet_enroll))
        .route("/hello", get(fleet_hello))
        .route("/summary", get(fleet_summary))
        .route("/catalog", post(fleet_catalog))
        .route("/batch", post(fleet_batch_lookup))
        .route("/detail", post(fleet_detail))
        .route("/content", post(fleet_content))
        .route("/projects/eligibility", post(fleet_project_eligibility))
        .route("/events", get(fleet_events))
        .route("/launch", post(fleet_launch))
        .route("/mutate", post(fleet_mutate))
        .route("/attention", post(fleet_attention_read))
        .route("/attention/resolve", post(fleet_attention_resolve))
        .route("/credential/rotate", post(fleet_token_rotate))
        .route("/credential/revoke", post(fleet_credential_revoke))
        .layer(DefaultBodyLimit::max(FLEET_REQUEST_BODY_LIMIT_BYTES))
}

async fn health(State(state): State<GatewayState>) -> Json<HealthResponseWire> {
    Json(HealthResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        status: "ok".to_string(),
        service: "sase_gateway".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        build: state.build,
        bind: state.bind,
        push: state.push_dispatcher.status(),
        fleet: FleetHealthWire {
            supported_protocol_versions: default_fleet_protocol_versions(),
        },
    })
}

async fn fleet_enroll(
    State(state): State<GatewayState>,
    payload: Result<Json<FleetEnrollmentRequestWire>, JsonRejection>,
) -> Result<(StatusCode, Json<FleetEnrollmentResponseWire>), ApiError> {
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let now = Utc::now();
    if !state.fleet_enrollment_limiter.check(now)? {
        return Err(ApiError::rate_limited("enroll"));
    }
    let now_unix = datetime_to_unix(now);
    let result = state
        .fleet_store
        .enroll(payload, now_unix)
        .map_err(ApiError::from_fleet_store)?;
    match result {
        FleetEnrollmentResult::Enrolled(success) => {
            let success = *success;
            let credential = success.credential;
            let scopes = credential.scopes.clone();
            Ok((
                StatusCode::OK,
                Json(FleetEnrollmentResponseWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    outcome: "enrolled".to_string(),
                    protocol_version: Some(success.protocol_version),
                    installation: success.installation,
                    machine_selector: state.machine_selector.clone(),
                    capabilities: fleet_capabilities(&scopes),
                    credential: Some(credential),
                    token_type: Some("bearer".to_string()),
                    token: Some(success.token),
                    quarantine: None,
                }),
            ))
        }
        FleetEnrollmentResult::Quarantined(mut response) => {
            response.machine_selector = state.machine_selector.clone();
            Ok((StatusCode::CONFLICT, Json(*response)))
        }
    }
}

async fn fleet_hello(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<FleetHelloResponseWire>, ApiError> {
    let protocol_version = fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/hello",
        FLEET_SCOPE_HELLO,
    )
    .await?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let summary = state
        .fleet_reads
        .summary()
        .await
        .map_err(ApiError::from_fleet_read)?;
    Ok(Json(FleetHelloResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        protocol_version,
        installation,
        machine_selector: state.machine_selector.clone(),
        capabilities: fleet_capabilities(&credential.scopes),
        credential,
        cursor: summary.cursor,
        counts: summary.counts,
        count_revision: summary.count_revision,
        freshness: summary.freshness,
    }))
}

async fn fleet_summary(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<FleetSummaryResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/summary",
        FLEET_SCOPE_SUMMARY_READ,
    )
    .await?;
    state
        .fleet_reads
        .summary()
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

async fn fleet_catalog(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetCatalogQueryWire>, JsonRejection>,
) -> Result<Json<crate::wire::FleetCatalogPageWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/catalog",
        FLEET_SCOPE_CATALOG_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .catalog(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

async fn fleet_batch_lookup(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetLogicalBatchRequestWire>, JsonRejection>,
) -> Result<Json<FleetLogicalBatchResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/batch",
        FLEET_SCOPE_BATCH_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .batch_lookup(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

async fn fleet_detail(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetDetailRequestWire>, JsonRejection>,
) -> Result<Json<FleetDetailResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/detail",
        FLEET_SCOPE_DETAIL_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .detail(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

async fn fleet_content(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetContentReadRequestWire>, JsonRejection>,
) -> Result<Json<FleetContentReadResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/content",
        FLEET_SCOPE_CONTENT_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .content(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

async fn fleet_project_eligibility(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetProjectEligibilityRequestWire>, JsonRejection>,
) -> Result<Json<FleetProjectEligibilityResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/projects/eligibility",
        FLEET_SCOPE_PROJECTS_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .project_eligibility(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

async fn fleet_events(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<FleetEventsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/events",
        FLEET_SCOPE_EVENTS_READ,
    )
    .await?;
    let subscription = state
        .fleet_reads
        .subscribe_events(query.cursor()?)
        .await
        .map_err(ApiError::from_fleet_read)?;
    let stream_state = state.clone();
    let stream = async_stream::stream! {
        for item in subscription.initial {
            yield Ok::<_, Infallible>(fleet_sse_event(item));
        }
        let mut receiver = subscription.receiver;
        let mut interval = tokio::time::interval(stream_state.heartbeat_interval);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    yield Ok::<_, Infallible>(fleet_sse_event(
                        FleetEventStreamItemWire::Heartbeat {
                            cursor: stream_state.fleet_reads.current_event_cursor(),
                        },
                    ));
                }
                received = receiver.recv() => {
                    match received {
                        Ok(event) => {
                            yield Ok::<_, Infallible>(fleet_sse_event(
                                FleetEventStreamItemWire::Invalidation(event),
                            ));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let Ok(snapshot) = stream_state.fleet_reads.authoritative_snapshot().await else {
                                break;
                            };
                            yield Ok::<_, Infallible>(fleet_sse_event(resync_item(
                                FleetResyncReasonWire::ReceiverLag,
                                snapshot,
                            )));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn fleet_launch(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetLaunchRequestWire>, JsonRejection>,
) -> Result<Json<FleetLaunchResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/launch",
        FLEET_SCOPE_LAUNCH,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let admission = state
        .fleet_launches
        .reserve(&payload, &installation.installation_id, current_unix_time())
        .map_err(ApiError::from_fleet_launch_store)?;
    if !admission.should_launch {
        return Ok(Json(FleetLaunchResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            decision: admission.decision,
            reason: admission.reason,
            receipt: admission.receipt,
        }));
    }

    let launch_request = MobileAgentTextLaunchRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        prompt: payload.intent.prompt.clone(),
        request_id: payload.intent.request_id.clone(),
        display_name: payload.intent.display_name.clone(),
        name: payload.intent.name.clone(),
        model: payload.intent.model.clone(),
        provider: payload.intent.provider.clone(),
        runtime: payload.intent.runtime.clone(),
        project: Some(payload.intent.project.project_id.clone()),
        device_id: credential.controller_id.clone(),
        dry_run: payload.intent.dry_run,
    };

    match state.agent_bridge.launch_text(&launch_request) {
        Ok(result) => {
            let primary_name = launch_primary_name(&result);
            let message = result
                .primary
                .as_ref()
                .and_then(|slot| slot.message.clone())
                .or_else(|| primary_name.clone());
            let receipt = state
                .fleet_launches
                .settle(
                    &admission.receipt,
                    &result,
                    &payload.intent.project.project_id,
                    message,
                )
                .map_err(ApiError::from_fleet_launch_store)?;
            state.audit(
                credential.controller_id.clone(),
                "/api/fleet/v1/launch",
                primary_name.clone(),
                "success",
            );
            publish_agents_changed(&state, "fleet_launch", primary_name)?;
            Ok(Json(FleetLaunchResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                decision: admission.decision,
                reason: admission.reason,
                receipt,
            }))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                credential.controller_id,
                "/api/fleet/v1/launch",
                None,
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

async fn fleet_mutate(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetMutationRequestWire>, JsonRejection>,
) -> Result<Json<FleetMutationResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/mutate",
        FLEET_SCOPE_MUTATE,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let logical_key =
        sase_core::logical_locator_key(&payload.intent.target.logical)
            .map_err(|error| {
                ApiError::invalid_request("intent.target", error.to_string())
            })?;
    let observed = match state
        .fleet_reads
        .detail(sase_core::FleetDetailRequestWire {
            schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key,
        })
        .await
    {
        Ok(detail) => Some(detail.detail.summary),
        Err(FleetReadError::NotFound(_)) => None,
        Err(error) => return Err(ApiError::from_fleet_read(error)),
    };
    let precondition = sase_core::evaluate_mutation_precondition(
        &payload.intent,
        observed.as_ref(),
    )
    .map_err(|error| ApiError::invalid_request("intent", error.to_string()))?;
    if !precondition.allowed {
        return Err(ApiError::from_mutation_precondition(&precondition));
    }
    let admission = state
        .fleet_mutations
        .reserve(&payload, &installation.installation_id, current_unix_time())
        .map_err(ApiError::from_fleet_mutation_store)?;
    if !admission.should_execute {
        match admission.decision {
            sase_core::OperationDecisionKindWire::Conflict => {
                return Err(ApiError::from_fleet_mutation_store(
                    FleetMutationStoreError::Conflict(format!(
                        "{:?}",
                        admission.reason
                    )),
                ));
            }
            sase_core::OperationDecisionKindWire::Expired => {
                return Err(ApiError::from_fleet_mutation_store(
                    FleetMutationStoreError::Expired(format!(
                        "{:?}",
                        admission.reason
                    )),
                ));
            }
            _ => {
                return Ok(Json(FleetMutationResponseWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    decision: admission.decision,
                    reason: admission.reason,
                    receipt: admission.receipt,
                }));
            }
        }
    }

    let agent_name = payload.intent.target.logical.agent_id.clone();
    let executed = execute_fleet_mutation(
        &state,
        credential.controller_id.as_deref(),
        &payload,
        &agent_name,
    );
    match executed {
        Ok((result_locator, instance_locator, message, primary_name)) => {
            let receipt = state
                .fleet_mutations
                .settle(
                    &admission.receipt,
                    payload.intent.kind,
                    result_locator,
                    instance_locator,
                    message,
                )
                .map_err(ApiError::from_fleet_mutation_store)?;
            state.audit(
                credential.controller_id.clone(),
                "/api/fleet/v1/mutate",
                primary_name.clone(),
                "success",
            );
            publish_agents_changed(&state, "fleet_mutate", primary_name)?;
            Ok(Json(FleetMutationResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                decision: admission.decision,
                reason: admission.reason,
                receipt,
            }))
        }
        Err(api_error) => {
            state.audit(
                credential.controller_id,
                "/api/fleet/v1/mutate",
                Some(agent_name),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

type FleetMutationExecution = (
    Option<sase_core::LogicalAgentLocatorWire>,
    Option<sase_core::AgentInstanceLocatorWire>,
    Option<String>,
    Option<String>,
);

fn execute_fleet_mutation(
    state: &GatewayState,
    controller_id: Option<&str>,
    payload: &FleetMutationRequestWire,
    agent_name: &str,
) -> Result<FleetMutationExecution, ApiError> {
    match payload.intent.kind {
        sase_core::FleetMutationKindWire::Stop => {
            let request = MobileAgentKillRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                reason: payload.intent.reason.clone(),
                device_id: controller_id.map(str::to_string),
            };
            let result = state
                .agent_bridge
                .kill_agent(agent_name, &request)
                .map_err(ApiError::from_host_bridge)?;
            Ok((
                Some(payload.intent.target.logical.clone()),
                Some(payload.intent.target.clone()),
                result.message.clone().or(Some(result.status.clone())),
                Some(result.name),
            ))
        }
        sase_core::FleetMutationKindWire::Retry => {
            let request = MobileAgentRetryRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                request_id: None,
                prompt_override: None,
                dry_run: None,
                kill_source_first: payload.intent.kill_source_first,
                device_id: controller_id.map(str::to_string),
            };
            let result = state
                .agent_bridge
                .retry_agent(agent_name, &request)
                .map_err(ApiError::from_host_bridge)?;
            let primary_name = launch_primary_name(&result.launch);
            Ok((
                mutation_result_locator(payload, primary_name.as_deref()),
                None,
                primary_name.clone(),
                primary_name,
            ))
        }
        sase_core::FleetMutationKindWire::Fork => {
            let prompt = payload.intent.fork_prompt.clone().unwrap_or_default();
            let request = MobileAgentForkRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                request_id: None,
                prompt,
                dry_run: None,
                device_id: controller_id.map(str::to_string),
            };
            let result = state
                .agent_bridge
                .fork_agent(agent_name, &request)
                .map_err(ApiError::from_host_bridge)?;
            let primary_name = launch_primary_name(&result.launch);
            Ok((
                mutation_result_locator(payload, primary_name.as_deref()),
                None,
                primary_name.clone(),
                primary_name,
            ))
        }
    }
}

fn mutation_result_locator(
    payload: &FleetMutationRequestWire,
    agent_id: Option<&str>,
) -> Option<sase_core::LogicalAgentLocatorWire> {
    let agent_id = agent_id.filter(|value| !value.trim().is_empty())?;
    Some(sase_core::LogicalAgentLocatorWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        project: payload.intent.target.logical.project.clone(),
        agent_id: agent_id.to_string(),
        family_id: payload.intent.target.logical.family_id.clone(),
    })
}

async fn fleet_attention_read(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetLogicalBatchRequestWire>, JsonRejection>,
) -> Result<Json<sase_core::FleetAttentionSnapshotWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/attention",
        FLEET_SCOPE_ATTENTION_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let batch = state
        .fleet_reads
        .batch_lookup(payload)
        .await
        .map_err(ApiError::from_fleet_read)?;
    let resolved: Vec<sase_core::FleetAttentionLogicalIdentityWire> =
        batch
            .entries
            .into_iter()
            .filter_map(|entry| entry.summary)
            .map(|summary| sase_core::FleetAttentionLogicalIdentityWire {
                schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
                agent_label: summary.labels.agent_label.clone().unwrap_or_else(
                    || summary.logical_locator.agent_id.clone(),
                ),
                logical_key: summary.logical_key,
                logical_locator: summary.logical_locator,
            })
            .collect();
    // With no resolved followed row, no notification read happens at all:
    // attention stays scoped to logical keys this viewer already follows.
    if resolved.is_empty() {
        return Ok(Json(sase_core::FleetAttentionSnapshotWire {
            schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            entries: Vec::new(),
            observed_at_unix: current_unix_time(),
        }));
    }
    let allowed_labels: BTreeSet<&str> = resolved
        .iter()
        .map(|identity| identity.agent_label.as_str())
        .collect();
    let notifications = state
        .notification_bridge
        .list_notifications(false)
        .map_err(ApiError::from_host_bridge)?;
    let rows: Vec<sase_core::FleetAttentionNotificationRowWire> = notifications
        .notifications
        .iter()
        .filter(|notification| {
            attention_correlation_label(notification)
                .is_some_and(|label| allowed_labels.contains(label))
        })
        .map(
            |notification| sase_core::FleetAttentionNotificationRowWire {
                schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
                state: state.notification_bridge.action_state(notification),
                notification: notification.clone(),
            },
        )
        .collect();
    let snapshot = sase_core::project_fleet_attention(
        &installation.installation_id,
        &rows,
        &resolved,
        current_unix_time(),
    )
    .map_err(|error| {
        ApiError::invalid_request("attention", error.to_string())
    })?;
    Ok(Json(snapshot))
}

/// The agent-label signal used to correlate a notification to a followed
/// row: the gate producer's declared `origin_agent`, or (for a question) the
/// asking agent's own `sender` identity.
fn attention_correlation_label(
    notification: &NotificationWire,
) -> Option<&str> {
    let kind = MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    );
    if kind == MobileActionKindWire::UserQuestion {
        return Some(notification.sender.as_str());
    }
    if kind.is_gate() {
        return notification
            .action_data
            .get("origin_agent")
            .map(String::as_str);
    }
    None
}

async fn fleet_attention_resolve(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<sase_core::FleetAttentionRequestWire>, JsonRejection>,
) -> Result<Json<sase_core::FleetAttentionResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/attention/resolve",
        FLEET_SCOPE_ATTENTION_RESOLVE,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;

    let entry = current_attention_entry(
        &state,
        &installation.installation_id,
        &payload.intent.request_key,
    )?;
    // The bridge's own pending-action state is this host's ground truth for
    // whether the required attention capability is currently available; a
    // row that has gone terminal since the last bulk read simply stops
    // being Pending, which the precondition already refuses on its own.
    let capabilities = sase_core::CapabilitySetWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        resource: match &entry {
            Some(entry)
                if entry.state
                    == sase_core::FleetAttentionStateWire::Pending =>
            {
                vec![payload.intent.kind.required_capability().to_string()]
            }
            _ => Vec::new(),
        },
        host: Vec::new(),
        protocol: vec!["fleet.v1".to_string()],
    };
    let precondition = sase_core::evaluate_attention_precondition(
        &payload.intent,
        entry.as_ref(),
        &capabilities,
    )
    .map_err(|error| ApiError::invalid_request("intent", error.to_string()))?;

    let admission = state
        .fleet_attention
        .reserve(&payload, &installation.installation_id, current_unix_time())
        .map_err(ApiError::from_fleet_attention_store)?;
    let request_id = payload.intent.request_key.request_id.clone();

    let receipt = match admission.decision {
        sase_core::OperationDecisionKindWire::Expired => {
            return Err(ApiError::from_fleet_attention_store(
                FleetAttentionStoreError::Expired(format!(
                    "{:?}",
                    admission.reason
                )),
            ));
        }
        sase_core::OperationDecisionKindWire::Conflict
        | sase_core::OperationDecisionKindWire::PreconditionMismatch => {
            return Err(ApiError::from_fleet_attention_store(
                FleetAttentionStoreError::Conflict(format!(
                    "{:?}",
                    admission.reason
                )),
            ));
        }
        // A replayed submission never re-consults the row: the receipt was
        // already settled (successfully or as a typed refusal) the first
        // time this exact key and payload were seen.
        sase_core::OperationDecisionKindWire::ReturnOriginalReceipt => {
            admission.receipt
        }
        sase_core::OperationDecisionKindWire::AcceptNew
            if precondition.allowed =>
        {
            match execute_fleet_attention(&state, &payload.intent) {
                Ok((outcome, settled_response, message)) => {
                    let receipt = state
                        .fleet_attention
                        .settle(
                            &admission.receipt,
                            outcome,
                            Some(state.host_label.clone()),
                            settled_response,
                            message,
                        )
                        .map_err(ApiError::from_fleet_attention_store)?;
                    state.audit(
                        credential.controller_id.clone(),
                        "/api/fleet/v1/attention/resolve",
                        Some(request_id.clone()),
                        "success",
                    );
                    publish_notifications_changed(
                        &state,
                        "fleet_attention_resolve",
                        Some(request_id.clone()),
                        None,
                    )?;
                    publish_agents_changed(
                        &state,
                        "fleet_attention_resolve",
                        None,
                    )?;
                    receipt
                }
                Err(api_error) => {
                    state.audit(
                        credential.controller_id,
                        "/api/fleet/v1/attention/resolve",
                        Some(request_id),
                        api_error.wire.code.outcome_label(),
                    );
                    return Err(api_error);
                }
            }
        }
        // The row itself refuses this intent (already settled, stale,
        // unknown, capability missing, or an invalid option): settle this
        // operation key against that typed, host-named refusal rather than
        // a bare HTTP error, so a losing controller gets a structured
        // result instead of a failure and never a second execution.
        sase_core::OperationDecisionKindWire::AcceptNew => {
            let (outcome, settled_by_host_label, settled_response, message) =
                attention_refusal_settlement(
                    &state,
                    &precondition,
                    &payload.intent.request_key,
                )
                .map_err(ApiError::from_fleet_attention_store)?;
            let receipt = state
                .fleet_attention
                .settle(
                    &admission.receipt,
                    outcome,
                    settled_by_host_label,
                    settled_response,
                    Some(message),
                )
                .map_err(ApiError::from_fleet_attention_store)?;
            state.audit(
                credential.controller_id,
                "/api/fleet/v1/attention/resolve",
                Some(request_id),
                &format!("{:?}", precondition.reason),
            );
            receipt
        }
    };
    Ok(Json(sase_core::FleetAttentionResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        decision: admission.decision,
        reason: admission.reason,
        receipt,
    }))
}

type FleetAttentionRefusalSettlement = (
    sase_core::FleetAttentionOutcomeWire,
    Option<String>,
    Option<JsonValue>,
    String,
);

/// Map a precondition refusal to the settlement `execute_fleet_attention`
/// would otherwise have produced, naming this host in the message. For
/// `already_settled`, prefers the real settled receipt this journal already
/// recorded (from whichever operation key answered first) over the bare
/// notification-state signal, so a losing controller sees the actual
/// settled result rather than a generic message.
fn attention_refusal_settlement(
    state: &GatewayState,
    precondition: &sase_core::FleetAttentionPreconditionDecisionWire,
    request_key: &sase_core::FleetAttentionRequestKeyWire,
) -> Result<FleetAttentionRefusalSettlement, FleetAttentionStoreError> {
    use sase_core::FleetAttentionPreconditionReasonWire;
    if precondition.reason
        == FleetAttentionPreconditionReasonWire::AlreadySettled
    {
        let prior = state
            .fleet_attention
            .find_settled_by_request_key(request_key)?;
        let host_label = prior
            .as_ref()
            .and_then(|receipt| receipt.settled_by_host_label.clone())
            .or_else(|| precondition.settled_by_host_label.clone())
            .unwrap_or_else(|| state.host_label.clone());
        let settled_response = prior
            .and_then(|receipt| receipt.settled_response)
            .or_else(|| precondition.settled_response.clone());
        return Ok((
            sase_core::FleetAttentionOutcomeWire::AlreadySettled,
            Some(host_label.clone()),
            settled_response,
            format!("Already answered on {host_label}"),
        ));
    }
    let host = state.host_label.clone();
    let (outcome, message) = match precondition.reason {
        FleetAttentionPreconditionReasonWire::StaleRevision => (
            sase_core::FleetAttentionOutcomeWire::StaleRevision,
            format!("Attention revision is stale on {host}"),
        ),
        FleetAttentionPreconditionReasonWire::UnknownRequest => (
            sase_core::FleetAttentionOutcomeWire::UnknownRequest,
            format!("Attention request was not found on {host}"),
        ),
        FleetAttentionPreconditionReasonWire::CapabilityMissing => (
            sase_core::FleetAttentionOutcomeWire::CapabilityMissing,
            format!(
                "Attention capability {} missing on {host}",
                precondition.required_capability
            ),
        ),
        FleetAttentionPreconditionReasonWire::InvalidOption => (
            sase_core::FleetAttentionOutcomeWire::PreconditionFailed,
            format!("Attention option is invalid on {host}"),
        ),
        FleetAttentionPreconditionReasonWire::Ok
        | FleetAttentionPreconditionReasonWire::AlreadySettled => (
            sase_core::FleetAttentionOutcomeWire::PreconditionFailed,
            format!("Attention precondition refused on {host}"),
        ),
    };
    Ok((outcome, Some(host), None, message))
}

/// Re-project the single attention entry `request_key` currently refers to,
/// including a dismissed/already-settled notification, so a losing
/// controller can still be told the settled result.
fn current_attention_entry(
    state: &GatewayState,
    installation_id: &str,
    request_key: &sase_core::FleetAttentionRequestKeyWire,
) -> Result<Option<sase_core::FleetAttentionEntryWire>, ApiError> {
    let snapshot = state
        .notification_bridge
        .list_notifications(true)
        .map_err(ApiError::from_host_bridge)?;
    let Some(notification) = snapshot
        .notifications
        .iter()
        .find(|notification| notification.id == request_key.request_id)
    else {
        return Ok(None);
    };
    let row = sase_core::FleetAttentionNotificationRowWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        state: state.notification_bridge.action_state(notification),
        notification: notification.clone(),
    };
    let projected = sase_core::project_fleet_attention(
        installation_id,
        std::slice::from_ref(&row),
        &[],
        current_unix_time(),
    )
    .map_err(|error| {
        ApiError::invalid_request("intent.request_key", error.to_string())
    })?;
    Ok(projected.entries.into_iter().next())
}

type FleetAttentionExecution = (
    sase_core::FleetAttentionOutcomeWire,
    Option<JsonValue>,
    Option<String>,
);

fn execute_fleet_attention(
    state: &GatewayState,
    intent: &sase_core::FleetAttentionIntentWire,
) -> Result<FleetAttentionExecution, ApiError> {
    let prefix = intent.request_key.request_id.clone();
    let already_handled_message =
        || Some(format!("Already answered on {}", state.host_label));
    match intent.kind {
        sase_core::FleetAttentionKindWire::Gate => {
            let request = GateActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix,
                selected_option_ids: intent.selected_option_ids.clone(),
                feedback: intent.feedback.clone(),
                option_inputs: None,
            };
            match state.notification_bridge.execute_gate_action(&request) {
                Ok(result) => Ok((
                    sase_core::FleetAttentionOutcomeWire::Applied,
                    Some(result.response_json),
                    result.message,
                )),
                Err(HostBridgeError::ActionAlreadyHandled(_)) => Ok((
                    sase_core::FleetAttentionOutcomeWire::AlreadySettled,
                    None,
                    already_handled_message(),
                )),
                Err(error) => Err(ApiError::from_host_bridge(error)),
            }
        }
        sase_core::FleetAttentionKindWire::Question => {
            let choice = intent
                .question_choice
                .unwrap_or(QuestionActionChoiceWire::Answer);
            let request = QuestionActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix,
                choice,
                question_index: intent.question_index,
                selected_option_id: intent.selected_option_id.clone(),
                selected_option_label: intent.selected_option_label.clone(),
                selected_option_index: intent.selected_option_index,
                custom_answer: intent.custom_answer.clone(),
                global_note: intent.global_note.clone(),
            };
            match state.notification_bridge.execute_question_action(&request) {
                Ok(result) => Ok((
                    sase_core::FleetAttentionOutcomeWire::Applied,
                    Some(result.response_json),
                    result.message,
                )),
                Err(HostBridgeError::ActionAlreadyHandled(_)) => Ok((
                    sase_core::FleetAttentionOutcomeWire::AlreadySettled,
                    None,
                    already_handled_message(),
                )),
                Err(error) => Err(ApiError::from_host_bridge(error)),
            }
        }
    }
}

async fn fleet_token_rotate(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetTokenRotateRequestWire>, JsonRejection>,
) -> Result<Json<FleetTokenRotateResponseWire>, ApiError> {
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    validate_schema(payload.schema_version)?;
    let protocol_version =
        negotiate_fleet_protocol_version(&payload.supported_protocol_versions)
            .ok_or_else(|| {
                ApiError::incompatible_protocol("supported_protocol_versions")
            })?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/credential/rotate",
        FLEET_SCOPE_ROTATE,
    )
    .await?;
    let mut response = state
        .fleet_store
        .rotate_credential(&credential.credential_id, current_unix_time())
        .map_err(ApiError::from_fleet_store)?;
    response.protocol_version = protocol_version;
    Ok(Json(response))
}

async fn fleet_credential_revoke(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetCredentialRevokeRequestWire>, JsonRejection>,
) -> Result<Json<FleetCredentialRevokeResponseWire>, ApiError> {
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/credential/revoke",
        FLEET_SCOPE_REVOKE,
    )
    .await?;
    state
        .fleet_store
        .revoke_credential(
            &credential.credential_id,
            payload,
            current_unix_time(),
        )
        .map(Json)
        .map_err(ApiError::from_fleet_store)
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
            "helpers.read".to_string(),
            "update.write".to_string(),
            "attachments.download".to_string(),
            "notifications.state.write".to_string(),
            "push_subscriptions.read".to_string(),
            "push_subscriptions.write".to_string(),
        ],
    }))
}

async fn list_push_subscriptions(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<PushSubscriptionListResponseWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/session/push-subscriptions")
            .await?;
    let subscriptions = state
        .token_store
        .list_push_subscriptions(&device.device_id)
        .map_err(ApiError::from_store)?;
    Ok(Json(PushSubscriptionListResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        subscriptions,
    }))
}

async fn register_push_subscription(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<PushSubscriptionRequestWire>, JsonRejection>,
) -> Result<Json<PushSubscriptionRegisterResponseWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/session/push-subscriptions")
            .await?;
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_push_subscription_request(&payload)?;
    let (subscription, created) = state
        .token_store
        .register_push_subscription(&device.device_id, payload, Utc::now())
        .map_err(ApiError::from_store)?;
    state.audit(
        Some(device.device_id),
        "/api/v1/session/push-subscriptions",
        Some(subscription.id.clone()),
        if created { "created" } else { "updated" },
    );
    Ok(Json(PushSubscriptionRegisterResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        subscription,
        created,
    }))
}

async fn delete_push_subscription(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PushSubscriptionDeleteResponseWire>, ApiError> {
    let device = authenticate(
        &state,
        &headers,
        "/api/v1/session/push-subscriptions/{id}",
    )
    .await?;
    let Some((subscription, revoked)) = state
        .token_store
        .revoke_push_subscription(&device.device_id, &id, Utc::now())
        .map_err(ApiError::from_store)?
    else {
        state.audit(
            Some(device.device_id),
            "/api/v1/session/push-subscriptions/{id}",
            Some(id.clone()),
            "not_found",
        );
        return Err(ApiError::push_subscription_not_found(id));
    };
    state.audit(
        Some(device.device_id),
        "/api/v1/session/push-subscriptions/{id}",
        Some(subscription.id.clone()),
        if revoked {
            "revoked"
        } else {
            "already_revoked"
        },
    );
    Ok(Json(PushSubscriptionDeleteResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        subscription,
        revoked,
    }))
}

async fn events(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/events").await?;
    let subscription = initial_events_for_stream(&state, &headers, &device)?;
    let stream_state = state.clone();
    let stream = async_stream::stream! {
        for record in subscription.initial_events {
            yield Ok::<_, Infallible>(sse_event(record));
        }
        let mut receiver = subscription.receiver;
        let mut interval = tokio::time::interval(stream_state.heartbeat_interval);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let sequence = stream_state.event_hub.current_sequence();
                    yield Ok::<_, Infallible>(sse_event(EventRecordWire {
                        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                        id: format_event_id(sequence),
                        created_at: format_time(Utc::now()),
                        payload: EventPayloadWire::Heartbeat { sequence },
                    }));
                }
                received = receiver.recv() => {
                    match received {
                        Ok(record) => yield Ok::<_, Infallible>(sse_event(record)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let Ok(record) = stream_state.event_hub.append(|_| {
                                EventPayloadWire::ResyncRequired {
                                    reason: "receiver_lagged".to_string(),
                                }
                            }) else {
                                break;
                            };
                            yield Ok::<_, Infallible>(sse_event(record));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
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

#[derive(Debug, Clone, Default, Deserialize)]
struct FleetEventsQuery {
    #[serde(default)]
    store_generation: Option<String>,
    #[serde(default)]
    sequence: Option<u64>,
}

impl FleetEventsQuery {
    fn cursor(&self) -> Result<Option<StoreCursorWire>, ApiError> {
        match (&self.store_generation, self.sequence) {
            (None, None) => Ok(None),
            (Some(store_generation), Some(sequence)) => {
                let cursor = StoreCursorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    store_generation: store_generation.clone(),
                    sequence,
                };
                sase_core::fleet_contract::validate_store_cursor(&cursor)
                    .map(Some)
                    .map_err(|error| {
                        ApiError::invalid_request(
                            "fleet_event_cursor",
                            error.to_string(),
                        )
                    })
            }
            _ => Err(ApiError::invalid_request(
                "fleet_event_cursor",
                "store_generation and sequence must be supplied together",
            )),
        }
    }
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
struct ChangeSpecTagsQuery {
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn list_changespec_tags(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<ChangeSpecTagsQuery>,
) -> Result<Json<MobileChangeSpecTagListResponseWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/changespec-tags").await?;
    let request = MobileChangeSpecTagListRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        project: query.project,
        limit: query.limit,
        device_id: Some(device.device_id),
    };
    state
        .helper_bridge
        .list_changespec_tags(&request)
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct XpromptCatalogQuery {
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    include_pdf: bool,
    #[serde(default)]
    limit: Option<u32>,
}

async fn xprompt_catalog(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<XpromptCatalogQuery>,
) -> Result<Json<MobileXpromptCatalogResponseWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/xprompts/catalog").await?;
    let request = MobileXpromptCatalogRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        project: query.project,
        source: query.source,
        tag: query.tag,
        query: query.query,
        include_pdf: query.include_pdf,
        limit: query.limit,
        device_id: Some(device.device_id),
    };
    state
        .helper_bridge
        .xprompt_catalog(&request)
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BeadListQuery {
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    all_projects: bool,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    bead_type: Option<String>,
    #[serde(default)]
    tier: Option<String>,
    #[serde(default)]
    include_closed: bool,
    #[serde(default)]
    limit: Option<u32>,
}

async fn list_beads(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<BeadListQuery>,
) -> Result<Json<MobileBeadListResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/beads").await?;
    let request = MobileBeadListRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        project: query.project,
        all_projects: query.all_projects,
        status: query.status,
        bead_type: query.bead_type,
        tier: query.tier,
        include_closed: query.include_closed,
        limit: query.limit,
        device_id: Some(device.device_id),
    };
    state
        .helper_bridge
        .list_beads(&request)
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct BeadShowQuery {
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    all_projects: bool,
}

async fn show_bead(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<BeadShowQuery>,
) -> Result<Json<MobileBeadShowResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/beads/{id}").await?;
    let request = MobileBeadShowRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        bead_id: id,
        project: query.project,
        all_projects: query.all_projects,
        device_id: Some(device.device_id),
    };
    state
        .helper_bridge
        .show_bead(&request)
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct UpdateStartBody {
    #[serde(default = "default_gateway_schema_version")]
    schema_version: u32,
    #[serde(default)]
    request_id: Option<String>,
}

async fn update_start(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<UpdateStartBody>, JsonRejection>,
) -> Result<Json<MobileUpdateStartResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/update/start").await?;
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    validate_schema(payload.schema_version)?;
    let request = MobileUpdateStartRequestWire {
        schema_version: payload.schema_version,
        request_id: payload.request_id,
        device_id: Some(device.device_id.clone()),
    };
    match state.helper_bridge.update_start(&request) {
        Ok(result) => {
            publish_helpers_changed(
                &state,
                "update_start",
                Some("update".to_string()),
                Some(result.job.job_id.clone()),
            )?;
            Ok(Json(result))
        }
        Err(error) => Err(ApiError::from_host_bridge(error)),
    }
}

async fn update_status(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<MobileUpdateStatusResponseWire>, ApiError> {
    let device =
        authenticate(&state, &headers, "/api/v1/update/{job_id}").await?;
    let request = MobileUpdateStatusRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        job_id,
        device_id: Some(device.device_id),
    };
    match state.helper_bridge.update_status(&request) {
        Ok(result) => {
            publish_helpers_changed(
                &state,
                "update_status",
                Some("update".to_string()),
                Some(result.job.job_id.clone()),
            )?;
            Ok(Json(result))
        }
        Err(error) => Err(ApiError::from_host_bridge(error)),
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
    publish_expired_notification_activity(
        &state,
        &snapshot.expired_ids,
        &snapshot.notifications,
    )?;
    let mut rows = filtered_notifications(snapshot.notifications, &query);
    sort_newest_first(&mut rows);
    let total_count = rows.len() as u64;
    if let Some(limit) = query.limit {
        rows.truncate(limit as usize);
    }
    let next_high_water = rows.first().map(notification_activity_cursor_value);
    let notifications = rows
        .iter()
        .map(|row| {
            let action_state = state.notification_bridge.action_state(row);
            mobile_notification_card_from_wire(
                row,
                action_state,
                mobile_notification_priority_from_wire(row)
                    || mobile_notification_error_from_wire(row),
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
    publish_expired_notification_activity(
        &state,
        &snapshot.expired_ids,
        &snapshot.notifications,
    )?;
    let Some(notification) =
        snapshot.notifications.into_iter().find(|row| row.id == id)
    else {
        return Err(ApiError::notification_not_found(id));
    };
    let card = mobile_notification_card_from_wire(
        &notification,
        state.notification_bridge.action_state(&notification),
        mobile_notification_priority_from_wire(&notification)
            || mobile_notification_error_from_wire(&notification),
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
                None,
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
struct GateActionBody {
    #[serde(default = "default_mobile_schema_version")]
    schema_version: u32,
    #[serde(default)]
    selected_option_ids: Vec<String>,
    #[serde(default)]
    feedback: Option<String>,
    #[serde(default)]
    option_inputs: Option<BTreeMap<String, JsonValue>>,
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

async fn gate_action(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    AxumPath(prefix): AxumPath<String>,
    payload: Result<Json<GateActionBody>, JsonRejection>,
) -> Result<Json<ActionResultWire>, ApiError> {
    const ENDPOINT: &str = "/api/v1/actions/gate/{prefix}";
    let device = authenticate(&state, &headers, ENDPOINT).await?;
    let Json(payload) = payload.map_err(ApiError::from_json_rejection)?;
    let request = GateActionRequestWire {
        schema_version: payload.schema_version,
        prefix: prefix.clone(),
        selected_option_ids: payload.selected_option_ids,
        feedback: payload.feedback,
        option_inputs: payload.option_inputs,
    };
    match state.notification_bridge.execute_gate_action(&request) {
        Ok(result) => {
            state.audit(
                Some(device.device_id),
                ENDPOINT,
                result.notification_id.clone().or(Some(prefix)),
                "success",
            );
            publish_notifications_changed(
                &state,
                "gate_action",
                result.notification_id.clone(),
                None,
            )?;
            Ok(Json(result))
        }
        Err(error) => {
            let api_error = ApiError::from_host_bridge(error);
            state.audit(
                Some(device.device_id),
                ENDPOINT,
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
                None,
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

async fn fleet_authenticate(
    state: &GatewayState,
    headers: &HeaderMap,
    _endpoint: &str,
    required_scope: &str,
) -> Result<FleetCredentialRecordWire, ApiError> {
    let token = bearer_token(headers)?;
    let credential = match state
        .fleet_store
        .authenticate_token(token, current_unix_time())
        .map_err(ApiError::from_fleet_store)?
    {
        FleetAuthentication::Active(credential) => credential,
        FleetAuthentication::Missing => {
            return Err(ApiError::unauthorized("authorization"));
        }
        FleetAuthentication::Expired(_) => {
            return Err(ApiError::credential_expired("authorization"));
        }
        FleetAuthentication::Revoked(_) => {
            return Err(ApiError::credential_revoked("authorization"));
        }
    };
    if !credential_has_scope(&credential, required_scope) {
        return Err(ApiError::scope_denied(required_scope));
    }
    Ok(credential)
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

fn fleet_protocol_version_from_headers(
    headers: &HeaderMap,
) -> Result<u32, ApiError> {
    let Some(value) = headers.get(FLEET_PROTOCOL_VERSIONS_HEADER) else {
        return Ok(crate::wire::FLEET_PROTOCOL_VERSION);
    };
    let value = value.to_str().map_err(|_| {
        ApiError::invalid_request(
            FLEET_PROTOCOL_VERSIONS_HEADER,
            "fleet protocol versions header must be valid text",
        )
    })?;
    let mut versions = Vec::new();
    for raw in value.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let version = raw.parse::<u32>().map_err(|_| {
            ApiError::invalid_request(
                FLEET_PROTOCOL_VERSIONS_HEADER,
                "fleet protocol versions header contains a non-integer version",
            )
        })?;
        versions.push(version);
    }
    negotiate_fleet_protocol_version(&versions).ok_or_else(|| {
        ApiError::incompatible_protocol(FLEET_PROTOCOL_VERSIONS_HEADER)
    })
}

fn datetime_to_unix(now: DateTime<Utc>) -> f64 {
    now.timestamp() as f64
        + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
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

fn validate_push_subscription_request(
    payload: &PushSubscriptionRequestWire,
) -> Result<(), ApiError> {
    validate_schema(payload.schema_version)?;
    let token = payload.provider_token.trim();
    if token.is_empty() {
        return Err(ApiError::invalid_request(
            "provider_token",
            "provider_token is required",
        ));
    }
    if token.len() > 4096 {
        return Err(ApiError::invalid_request(
            "provider_token",
            "provider_token is too long",
        ));
    }
    if payload.hint_categories.is_empty() {
        return Err(ApiError::invalid_request(
            "hint_categories",
            "at least one hint category is required",
        ));
    }
    Ok(())
}

fn default_mobile_schema_version() -> u32 {
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION
}

fn default_gateway_schema_version() -> u32 {
    GATEWAY_WIRE_SCHEMA_VERSION
}

fn default_attachment_token_ttl() -> ChronoDuration {
    ChronoDuration::minutes(5)
}

fn publish_notifications_changed(
    state: &GatewayState,
    reason: &str,
    notification_id: Option<String>,
    activity_cursor: Option<String>,
) -> Result<(), ApiError> {
    let record =
        state
            .event_hub
            .append(|_| EventPayloadWire::NotificationsChanged {
                reason: reason.to_string(),
                notification_id,
                activity_cursor,
            })?;
    state
        .push_dispatcher
        .dispatch_event(state.token_store.clone(), &record);
    Ok(())
}

fn publish_expired_notification_activity(
    state: &GatewayState,
    expired_ids: &[String],
    notifications: &[NotificationWire],
) -> Result<(), ApiError> {
    if expired_ids.is_empty() {
        return Ok(());
    }
    let newest = notifications
        .iter()
        .filter(|notification| expired_ids.contains(&notification.id))
        .max_by(|left, right| compare_notification_activity(left, right));
    publish_notifications_changed(
        state,
        "snooze_expired",
        (expired_ids.len() == 1).then(|| expired_ids[0].clone()),
        newest.map(notification_activity_cursor_value),
    )
}

fn publish_agents_changed(
    state: &GatewayState,
    reason: &str,
    agent_name: Option<String>,
) -> Result<(), ApiError> {
    let record =
        state
            .event_hub
            .append(|_| EventPayloadWire::AgentsChanged {
                reason: reason.to_string(),
                agent_name,
                timestamp: Some(format_time(Utc::now())),
            })?;
    state
        .push_dispatcher
        .dispatch_event(state.token_store.clone(), &record);
    Ok(())
}

fn publish_helpers_changed(
    state: &GatewayState,
    reason: &str,
    helper: Option<String>,
    job_id: Option<String>,
) -> Result<(), ApiError> {
    let record =
        state
            .event_hub
            .append(|_| EventPayloadWire::HelpersChanged {
                reason: reason.to_string(),
                helper,
                job_id,
                timestamp: Some(format_time(Utc::now())),
            })?;
    state
        .push_dispatcher
        .dispatch_event(state.token_store.clone(), &record);
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
) -> Result<EventHubSubscription, ApiError> {
    let mut subscription =
        state
            .event_hub
            .subscribe_after(last_event_id(headers)?, |_| {
                EventPayloadWire::Session {
                    device_id: device.device_id.clone(),
                }
            })?;
    let sequence = state.event_hub.current_sequence();
    subscription.initial_events.push(EventRecordWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        id: format_event_id(sequence),
        created_at: format_time(Utc::now()),
        payload: EventPayloadWire::Heartbeat { sequence },
    });
    Ok(subscription)
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

fn fleet_sse_event(item: FleetEventStreamItemWire) -> Event {
    let event_name = fleet_event_name(&item);
    let event_id = fleet_event_id(&item);
    let data = serde_json::to_string(&item)
        .expect("FleetEventStreamItemWire serialization should be infallible");
    let event = Event::default().event(event_name).data(data);
    match event_id {
        Some(id) => event.id(id),
        None => event,
    }
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
        EventPayloadWire::HelpersChanged { .. } => "helpers_changed",
    }
}

fn fleet_event_name(item: &FleetEventStreamItemWire) -> &'static str {
    match item {
        FleetEventStreamItemWire::Invalidation(_) => "invalidation",
        FleetEventStreamItemWire::ResyncRequired(_) => "resync_required",
        FleetEventStreamItemWire::Heartbeat { .. } => "heartbeat",
    }
}

fn fleet_event_id(item: &FleetEventStreamItemWire) -> Option<String> {
    match item {
        FleetEventStreamItemWire::Invalidation(event) => {
            Some(format_fleet_event_id(&event.cursor))
        }
        FleetEventStreamItemWire::ResyncRequired(resync) => {
            Some(format_fleet_event_id(&resync.snapshot.cursor))
        }
        FleetEventStreamItemWire::Heartbeat { .. } => None,
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
                .map(|high_water| activity_is_newer(row, high_water))
                .unwrap_or(true)
        })
        .collect()
}

fn sort_newest_first(notifications: &mut [NotificationWire]) {
    notifications
        .sort_by(|left, right| compare_notification_activity(right, left));
}

fn compare_notification_activity(
    left: &NotificationWire,
    right: &NotificationWire,
) -> Ordering {
    compare_activity_timestamps(
        notification_activity_at(left),
        notification_activity_at(right),
    )
    .then_with(|| left.id.cmp(&right.id))
}

fn compare_activity_timestamps(left: &str, right: &str) -> Ordering {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left), Ok(right)) => {
            left.with_timezone(&Utc).cmp(&right.with_timezone(&Utc))
        }
        _ => left.cmp(right),
    }
}

fn notification_activity_cursor_value(
    notification: &NotificationWire,
) -> String {
    format!(
        "{}|{}",
        notification_activity_at(notification),
        notification.id
    )
}

fn activity_is_newer(candidate: &NotificationWire, high_water: &str) -> bool {
    let (activity_at, cursor_id) = high_water
        .rsplit_once('|')
        .map_or((high_water, None), |(activity_at, id)| {
            (activity_at, Some(id))
        });
    match compare_activity_timestamps(
        notification_activity_at(candidate),
        activity_at,
    ) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => {
            cursor_id.is_some_and(|id| candidate.id.as_str() > id)
        }
    }
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
    match MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    ) {
        MobileActionKindWire::PlanApproval
        | MobileActionKindWire::EpicApproval => {
            if let Some(dir) = action_path(notification, "response_dir") {
                push_unique_path(
                    &mut paths,
                    &dir.join("plan_request.json").to_string_lossy(),
                );
            }
        }
        MobileActionKindWire::Hitl => {
            if let Some(dir) = action_path(notification, "artifacts_dir") {
                let request_path = dir.join("hitl_request.json");
                push_unique_path(&mut paths, &request_path.to_string_lossy());
                for path in hitl_path_typed_outputs(&request_path) {
                    push_unique_path(&mut paths, &path);
                }
            }
        }
        MobileActionKindWire::UserQuestion => {
            if let Some(dir) = action_path(notification, "response_dir") {
                push_unique_path(
                    &mut paths,
                    &dir.join("question_request.json").to_string_lossy(),
                );
            }
        }
        MobileActionKindWire::LaunchApproval
        | MobileActionKindWire::TaskTriage
        | MobileActionKindWire::BeadSnooze
        | MobileActionKindWire::FlagTriage
        | MobileActionKindWire::BeadStaleCleanup
        | MobileActionKindWire::PluginsRequired
        | MobileActionKindWire::CustomGate
        | MobileActionKindWire::NonAction
        | MobileActionKindWire::Unsupported => {}
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

fn format_fleet_event_id(cursor: &StoreCursorWire) -> String {
    format!("{}:{}", cursor.store_generation, cursor.sequence)
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
    wire: Box<ApiErrorWire>,
}

impl ApiError {
    fn unauthorized(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Unauthorized,
                message: "authentication is required for this endpoint"
                    .to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn not_found(path: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "route not found".to_string(),
                target: Some(path.into()),
                details: None,
            }),
        }
    }

    fn notification_not_found(id: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "notification not found".to_string(),
                target: Some(id.into()),
                details: None,
            }),
        }
    }

    fn push_subscription_not_found(id: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "push subscription not found".to_string(),
                target: Some(id.into()),
                details: None,
            }),
        }
    }

    fn invalid_request(
        target: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::InvalidRequest,
                message: message.into(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn pairing_expired(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PairingExpired,
                message: "pairing code expired".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn pairing_rejected(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PairingRejected,
                message: "pairing code rejected".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn attachment_expired(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AttachmentExpired,
                message: "attachment token is expired".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn internal(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Internal,
                message: "gateway internal error".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn from_store(error: StoreError) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Internal,
                message: error.to_string(),
                target: Some("device_store".to_string()),
                details: None,
            }),
        }
    }

    fn bootstrap_consumed(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::CONFLICT,
            ApiErrorCodeWire::BootstrapConsumed,
            "bootstrap secret was already used",
            target,
        )
    }

    fn bootstrap_expired(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::BAD_REQUEST,
            ApiErrorCodeWire::BootstrapExpired,
            "bootstrap secret is expired",
            target,
        )
    }

    fn bootstrap_rejected(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UNAUTHORIZED,
            ApiErrorCodeWire::BootstrapRejected,
            "bootstrap secret was rejected",
            target,
        )
    }

    fn credential_expired(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UNAUTHORIZED,
            ApiErrorCodeWire::CredentialExpired,
            "fleet credential is expired",
            target,
        )
    }

    fn credential_revoked(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UNAUTHORIZED,
            ApiErrorCodeWire::CredentialRevoked,
            "fleet credential is revoked",
            target,
        )
    }

    fn incompatible_protocol(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UPGRADE_REQUIRED,
            ApiErrorCodeWire::IncompatibleProtocol,
            "no mutually supported fleet protocol version",
            target,
        )
    }

    fn payload_too_large(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ApiErrorCodeWire::PayloadTooLarge,
            "fleet request body exceeds the configured limit",
            target,
        )
    }

    fn rate_limited(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::TOO_MANY_REQUESTS,
            ApiErrorCodeWire::RateLimited,
            "too many fleet enrollment attempts",
            target,
        )
    }

    fn scope_denied(scope: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::FORBIDDEN,
            ApiErrorCodeWire::ScopeDenied,
            "fleet credential does not include the required scope",
            scope,
        )
    }

    fn fleet_error(
        status: StatusCode,
        code: ApiErrorCodeWire,
        message: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            status,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code,
                message: message.into(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn from_fleet_store(error: FleetStoreError) -> Self {
        match error {
            FleetStoreError::BootstrapConsumed => {
                Self::bootstrap_consumed("bootstrap_secret")
            }
            FleetStoreError::BootstrapExpired => {
                Self::bootstrap_expired("bootstrap_secret")
            }
            FleetStoreError::BootstrapRejected => {
                Self::bootstrap_rejected("bootstrap_secret")
            }
            FleetStoreError::CredentialExpired => {
                Self::credential_expired("authorization")
            }
            FleetStoreError::CredentialMissing => {
                Self::unauthorized("authorization")
            }
            FleetStoreError::CredentialRevoked => {
                Self::credential_revoked("authorization")
            }
            FleetStoreError::IncompatibleProtocol => {
                Self::incompatible_protocol("supported_protocol_versions")
            }
            FleetStoreError::ScopeDenied(scope) => Self::scope_denied(scope),
            FleetStoreError::Validation(message) => {
                Self::invalid_request("fleet_request", message)
            }
            FleetStoreError::LockPoisoned
            | FleetStoreError::Io { .. }
            | FleetStoreError::Json { .. }
            | FleetStoreError::FleetContract(_) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    fn from_fleet_mutation_store(error: FleetMutationStoreError) -> Self {
        match error {
            FleetMutationStoreError::Validation(message) => {
                Self::invalid_request("fleet_mutate", message)
            }
            FleetMutationStoreError::Conflict(message) => Self::fleet_error(
                StatusCode::CONFLICT,
                ApiErrorCodeWire::InvalidRequest,
                message,
                "fleet_mutate",
            ),
            FleetMutationStoreError::Expired(message) => Self::fleet_error(
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                message,
                "fleet_mutate",
            ),
            FleetMutationStoreError::Io { .. }
            | FleetMutationStoreError::Json { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_mutation_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    fn from_mutation_precondition(
        decision: &sase_core::FleetMutationPreconditionDecisionWire,
    ) -> Self {
        use sase_core::FleetMutationPreconditionReasonWire;
        match decision.reason {
            FleetMutationPreconditionReasonWire::Ok => {
                Self::invalid_request("fleet_mutate", "precondition unexpectedly allowed")
            }
            FleetMutationPreconditionReasonWire::UnknownRow => Self::fleet_error(
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::AgentNotFound,
                "fleet mutation target row was not found",
                "intent.target",
            ),
            FleetMutationPreconditionReasonWire::InstanceMismatch => {
                Self::fleet_error(
                    StatusCode::CONFLICT,
                    ApiErrorCodeWire::ConflictAlreadyHandled,
                    "fleet mutation target instance does not match the current run",
                    "intent.target",
                )
            }
            FleetMutationPreconditionReasonWire::StaleRevision => Self::fleet_stale(
                "intent.row_revision",
            ),
            FleetMutationPreconditionReasonWire::CapabilityMissing => {
                Self::fleet_error(
                    StatusCode::FORBIDDEN,
                    ApiErrorCodeWire::UnsupportedAction,
                    format!(
                        "missing lifecycle capability {}",
                        decision.required_capability
                    ),
                    "capabilities",
                )
            }
            FleetMutationPreconditionReasonWire::AlreadyTerminal => {
                Self::fleet_error(
                    StatusCode::CONFLICT,
                    ApiErrorCodeWire::AgentNotRunning,
                    "fleet mutation target is already terminal",
                    "intent.target",
                )
            }
        }
    }

    fn from_fleet_attention_store(error: FleetAttentionStoreError) -> Self {
        match error {
            FleetAttentionStoreError::Validation(message) => {
                Self::invalid_request("fleet_attention", message)
            }
            FleetAttentionStoreError::Conflict(message) => Self::fleet_error(
                StatusCode::CONFLICT,
                ApiErrorCodeWire::InvalidRequest,
                message,
                "fleet_attention",
            ),
            FleetAttentionStoreError::Expired(message) => Self::fleet_error(
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                message,
                "fleet_attention",
            ),
            FleetAttentionStoreError::Io { .. }
            | FleetAttentionStoreError::Json { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_attention_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    fn from_fleet_launch_store(error: FleetLaunchStoreError) -> Self {
        match error {
            FleetLaunchStoreError::Validation(message) => {
                Self::invalid_request("fleet_launch", message)
            }
            FleetLaunchStoreError::Conflict(message) => Self::fleet_error(
                StatusCode::CONFLICT,
                ApiErrorCodeWire::InvalidRequest,
                message,
                "fleet_launch",
            ),
            FleetLaunchStoreError::Expired(message) => Self::fleet_error(
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                message,
                "fleet_launch",
            ),
            FleetLaunchStoreError::Io { .. }
            | FleetLaunchStoreError::Json { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_launch_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    fn fleet_timeout(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Timeout,
                message: "fleet read timed out".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn fleet_stale(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::GoneStale,
                message: "fleet resource is stale".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    fn from_fleet_read(error: FleetReadError) -> Self {
        match error {
            FleetReadError::Validation(message) => {
                Self::invalid_request("fleet_request", message)
            }
            FleetReadError::NotFound(target) => Self {
                status: StatusCode::NOT_FOUND,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::NotFound,
                    message: "fleet resource not found".to_string(),
                    target: Some(target),
                    details: None,
                }),
            },
            FleetReadError::Stale(target) => Self::fleet_stale(target),
            FleetReadError::Timeout(target) => Self::fleet_timeout(target),
            FleetReadError::Backend(target) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: "fleet backend failed".to_string(),
                    target: Some(target),
                    details: None,
                }),
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
            HostBridgeError::HelperNotFound(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::HelperNotFound,
                target.clone(),
            ),
            HostBridgeError::UpdateAlreadyRunning(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::UpdateAlreadyRunning,
                target.clone(),
            ),
            HostBridgeError::UpdateJobNotFound(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::UpdateJobNotFound,
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
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code,
                message: error.to_string(),
                target: Some(target),
                details: None,
            }),
        }
    }

    fn from_json_rejection(rejection: JsonRejection) -> Self {
        Self::invalid_request("body", rejection.body_text())
    }

    fn from_fleet_json_rejection(rejection: JsonRejection) -> Self {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::payload_too_large("body")
        } else {
            Self::invalid_request("body", rejection.body_text())
        }
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
            ApiErrorCodeWire::HelperNotFound => "helper_not_found",
            ApiErrorCodeWire::UpdateAlreadyRunning => "update_already_running",
            ApiErrorCodeWire::UpdateJobNotFound => "update_job_not_found",
            ApiErrorCodeWire::PermissionDenied => "permission_denied",
            ApiErrorCodeWire::BootstrapConsumed => "bootstrap_consumed",
            ApiErrorCodeWire::BootstrapExpired => "bootstrap_expired",
            ApiErrorCodeWire::BootstrapRejected => "bootstrap_rejected",
            ApiErrorCodeWire::CredentialExpired => "credential_expired",
            ApiErrorCodeWire::CredentialRevoked => "credential_revoked",
            ApiErrorCodeWire::IncompatibleProtocol => "incompatible_protocol",
            ApiErrorCodeWire::InstallationPinMismatch => {
                "installation_pin_mismatch"
            }
            ApiErrorCodeWire::PayloadTooLarge => "payload_too_large",
            ApiErrorCodeWire::RateLimited => "rate_limited",
            ApiErrorCodeWire::ScopeDenied => "scope_denied",
            ApiErrorCodeWire::Timeout => "timeout",
            ApiErrorCodeWire::ResyncRequired => "resync_required",
            ApiErrorCodeWire::Internal => "internal",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(*self.wire)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{HeaderValue, Request, StatusCode},
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
            push_config: PushConfig::default(),
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

    fn push_subscription_get_request(token: Option<&str>) -> Request<Body> {
        let mut builder =
            Request::builder().uri("/api/v1/session/push-subscriptions");
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    }

    fn push_subscription_post_request(
        token: Option<&str>,
        body: Value,
    ) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/api/v1/session/push-subscriptions")
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn push_subscription_delete_request(
        token: Option<&str>,
        id: &str,
    ) -> Request<Body> {
        let mut builder = Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/session/push-subscriptions/{id}"));
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

    fn fleet_bootstrap(
        state: &GatewayState,
        scopes: &[&str],
        expires_at_unix: Option<f64>,
    ) -> crate::wire::FleetBootstrapIssueResponseWire {
        state
            .fleet_store()
            .issue_bootstrap(
                crate::wire::FleetBootstrapIssueRequestWire {
                    schema_version: 1,
                    requested_scopes: scopes
                        .iter()
                        .map(|scope| scope.to_string())
                        .collect(),
                    supported_protocol_versions: vec![1],
                    expires_at_unix,
                    installation_pin: None,
                },
                current_unix_time(),
            )
            .unwrap()
    }

    fn fleet_bootstrap_at(
        state: &GatewayState,
        expires_at_unix: f64,
        issued_at_unix: f64,
    ) -> crate::wire::FleetBootstrapIssueResponseWire {
        state
            .fleet_store()
            .issue_bootstrap(
                crate::wire::FleetBootstrapIssueRequestWire {
                    schema_version: 1,
                    requested_scopes: Vec::new(),
                    supported_protocol_versions: vec![1],
                    expires_at_unix: Some(expires_at_unix),
                    installation_pin: None,
                },
                issued_at_unix,
            )
            .unwrap()
    }

    fn fleet_enroll_body(
        bootstrap: &crate::wire::FleetBootstrapIssueResponseWire,
        scopes: &[&str],
        versions: Vec<u32>,
    ) -> Value {
        json!({
            "schema_version": 1,
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

    fn fleet_enroll_request(body: Value) -> Request<Body> {
        json_request("POST", "/api/fleet/v1/enroll", body)
    }

    fn fleet_json_request(
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
            builder =
                builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::from(bytes)).unwrap()
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
            action: action.map(str::to_string),
            ..NotificationWire::default()
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
                push_config: PushConfig::default(),
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

    fn sample_agent_summary(name: &str) -> crate::wire::MobileAgentSummaryWire {
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
                push_config: PushConfig::default(),
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

    fn helper_result() -> crate::wire::MobileHelperResultWire {
        crate::wire::MobileHelperResultWire {
            status: crate::wire::MobileHelperStatusWire::Success,
            message: Some("ok".to_string()),
            warnings: Vec::new(),
            skipped: Vec::new(),
            partial_failure_count: None,
        }
    }

    fn helper_context() -> crate::wire::MobileHelperProjectContextWire {
        crate::wire::MobileHelperProjectContextWire {
            project: Some("sase".to_string()),
            scope: crate::wire::MobileHelperProjectScopeWire::Explicit,
        }
    }

    fn sample_bead_summary() -> crate::wire::MobileBeadSummaryWire {
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

    fn state_for_helper_bridge(tmp: &TempDir) -> GatewayState {
        let result = helper_result();
        let context = helper_context();
        let bead_summary = sample_bead_summary();
        let update_job = crate::wire::MobileUpdateJobWire {
            job_id: "job_123".to_string(),
            status: crate::wire::MobileUpdateJobStatusWire::Running,
            started_at: Some("2026-05-06T15:00:00Z".to_string()),
            finished_at: None,
            message: Some("update started".to_string()),
            log_path_display: Some(
                "~/.sase/chat_install/job_123.log".to_string(),
            ),
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
    fn sh_bridge_command(script: &std::path::Path) -> Vec<String> {
        vec!["/bin/sh".to_string(), script.to_string_lossy().into_owned()]
    }

    #[cfg(unix)]
    fn state_for_command_agent_bridge(tmp: &TempDir) -> GatewayState {
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
    fn state_for_command_helper_bridge(
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

    #[cfg(unix)]
    #[tokio::test]
    async fn command_helper_bridge_changespec_tags_returns_command_output() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_command_helper_bridge(&tmp, "success");
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

        let (status, value) = json_response_with_state(
            state,
            Request::builder()
                .method("POST")
                .uri("/api/v1/update/start")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"schema_version":1,"request_id":"req_1"}"#,
                ))
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
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;
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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        assert_eq!(
            hello["installation"]["installation_id"],
            bootstrap.pinned_installation_id
        );
        assert_eq!(hello["credential"]["controller_id"], "controller-a");
        assert_eq!(
            hello["cursor"]["schema_version"],
            GATEWAY_WIRE_SCHEMA_VERSION
        );
        assert_eq!(
            hello["counts"]["schema_version"],
            GATEWAY_WIRE_SCHEMA_VERSION
        );
        assert!(hello["counts"]["logical_agent_total"].is_u64());
        assert_eq!(
            hello["freshness"]["schema_version"],
            GATEWAY_WIRE_SCHEMA_VERSION
        );
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
            fleet_json_request(
                "GET",
                "/api/fleet/v1/summary",
                Some(token),
                None,
            ),
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

        let intent: sase_core::FleetLaunchIntentWire =
            serde_json::from_value(json!({
                "schema_version": 1,
                "prompt": "Do remote work",
                "request_id": "dispatch-request-1",
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
        let body = json!({
            "schema_version": 1,
            "key": {
                "schema_version": 1,
                "controller_id": "controller-a",
                "operation_id": "dispatch-request-1"
            },
            "target_installation_id": bootstrap.pinned_installation_id,
            "intent": intent,
            "payload_fingerprint": fingerprint,
            "acceptance_window_seconds": 30.0
        });
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
        let (status, launch) =
            json_response_with_state(state.clone(), request).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(launch["decision"], "accept_new");
        assert_eq!(launch["reason"], "unseen_in_window");
        assert_eq!(launch["receipt"]["state"], "settled");
        assert_eq!(launch["receipt"]["message"], "mobile-demo");
        assert_eq!(
            launch["receipt"]["target_installation_id"],
            bootstrap.pinned_installation_id
        );
        assert_eq!(
            launch["receipt"]["logical_locator"]["agent_id"],
            "mobile-demo"
        );

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

    fn seed_fleet_agent(
        home: &std::path::Path,
        name: &str,
        live: bool,
        proc: bool,
    ) {
        use sase_core::agent_scan::AgentArtifactScanOptionsWire;
        let projects = home.join("projects");
        let project = projects.join("proj");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(
            project.join("proj.sase"),
            format!(
                "NAME: proj\nWORKSPACE_DIR: {}\nPROJECT_STATE: enabled\n",
                project.display()
            ),
        )
        .unwrap();
        let artifact = project
            .join("artifacts")
            .join("ace-run")
            .join("20260906120000");
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
    }

    async fn enroll_mutate(
        state: &GatewayState,
        scopes: &[&str],
    ) -> (String, String) {
        let bootstrap = fleet_bootstrap(state, scopes, None);
        let (enroll_status, enrolled) = json_response_with_state(
            state.clone(),
            fleet_enroll_request(fleet_enroll_body(
                &bootstrap,
                scopes,
                vec![1],
            )),
        )
        .await;
        assert_eq!(enroll_status, StatusCode::OK);
        (
            enrolled["token"].as_str().unwrap().to_string(),
            bootstrap.pinned_installation_id,
        )
    }

    async fn first_summary(
        state: &GatewayState,
    ) -> sase_core::ResolvedAgentSummaryWire {
        let page = state
            .fleet_reads
            .catalog(sase_core::FleetCatalogQueryWire {
                schema_version: 1,
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
            sase_core::fleet_mutation_payload_fingerprint(&intent_wire)
                .unwrap();
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
            mutation_body(
                &summary,
                &installation_id,
                "stop",
                "op-stop",
                json!({}),
            ),
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
        seed_fleet_agent(tmp.path(), "mobile-demo", true, false);
        let running_path = tmp
            .path()
            .join("projects")
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join("20260906120000")
            .join("running.json");
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
        let (status, body) =
            post_mutate(state.clone(), &token, superseded).await;
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
    async fn fleet_mutate_refuses_terminal_missing_capability_and_bridge_failure(
    ) {
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

    #[derive(Debug, Default)]
    struct FakeAttentionNotificationBridge {
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
                stats: sase_core::notifications::NotificationStoreStatsWire::default(),
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
            if let Some(error) = self.next_question_error.lock().unwrap().take()
            {
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
        action_data
            .insert("origin_agent".to_string(), origin_agent.to_string());
        action_data
            .insert("gate_title".to_string(), "Approve deploy".to_string());
        action_data
            .insert("request_path".to_string(), request_path.to_string());
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

    fn attention_question_notification(
        id: &str,
        sender: &str,
    ) -> NotificationWire {
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
            HeaderValue::from_static("1"),
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
            HeaderValue::from_static("1"),
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
    async fn fleet_attention_read_empty_request_touches_no_notification_store()
    {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = state_for_tmp(&tmp, Duration::minutes(5));
        let bridge = Arc::new(FakeAttentionNotificationBridge::default());
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
        let (token, _installation_id) =
            enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

        let (status, snapshot) =
            post_attention_read(state, &token, Vec::new()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(snapshot["entries"], json!([]));
        assert_eq!(*bridge.list_calls.lock().unwrap(), 0);
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
        let bridge = Arc::new(
            FakeAttentionNotificationBridge::with_notifications(vec![
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
            ]),
        );
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
        let (token, _installation_id) =
            enroll_mutate(&state, &[FLEET_SCOPE_ATTENTION_READ]).await;

        let (status, snapshot) = post_attention_read(
            state,
            &token,
            vec![summary.logical_key.clone()],
        )
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
            Arc::new(FakeAttentionNotificationBridge::with_notifications(
                vec![attention_gate_notification(
                    "gate-00000001",
                    &agent_label,
                    envelope_path.to_str().unwrap(),
                )],
            ));
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
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
            request_key: attention_request_key(
                &installation_id,
                "gate-00000001",
            ),
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
        let bridge = Arc::new(
            FakeAttentionNotificationBridge::with_notifications(vec![
                attention_question_notification("question-0001", &agent_label),
            ]),
        );
        state.notification_bridge = DynNotificationHostBridge::new(bridge);
        let (token, installation_id) = enroll_mutate(
            &state,
            &[FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE],
        )
        .await;

        let intent = sase_core::FleetAttentionIntentWire {
            schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            kind: sase_core::FleetAttentionKindWire::Question,
            request_key: attention_request_key(
                &installation_id,
                "question-0001",
            ),
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
        let bridge = Arc::new(
            FakeAttentionNotificationBridge::with_notifications(vec![
                attention_question_notification("question-0002", &agent_label),
            ]),
        );
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
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
            request_key: attention_request_key(
                &installation_id,
                "question-0002",
            ),
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
            request_key: attention_request_key(
                &installation_id,
                "no-such-gate",
            ),
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
        let bridge = Arc::new(
            FakeAttentionNotificationBridge::with_notifications(vec![
                attention_question_notification("question-0003", &agent_label),
            ]),
        );
        *bridge.next_question_error.lock().unwrap() = Some(
            HostBridgeError::ActionAlreadyHandled("question-0003".to_string()),
        );
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
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
            request_key: attention_request_key(
                &installation_id,
                "question-0003",
            ),
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
        let (status, resolved) =
            post_attention_resolve(state, &token, body).await;
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
        let bridge = Arc::new(
            FakeAttentionNotificationBridge::with_notifications(vec![
                attention_question_notification("question-0004", &agent_label),
            ]),
        );
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
            request_key: attention_request_key(
                &installation_id,
                "question-0004",
            ),
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

    #[tokio::test]
    async fn fleet_rotation_and_revocation_reject_stale_or_revoked_tokens() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let bootstrap = fleet_bootstrap(&state, &[], None);
        let (enroll_status, enrolled) = json_response_with_state(
            state.clone(),
            fleet_enroll_request(fleet_enroll_body(&bootstrap, &[], vec![1])),
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
                    "schema_version": 1,
                    "supported_protocol_versions": [1]
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
                    "schema_version": 1,
                    "reason": "controller retired"
                })),
            ),
        )
        .await;
        assert_eq!(revoke_status, StatusCode::OK);
        assert_eq!(revoke["revoked"], true);
        assert_eq!(
            revoke["credential"]["revoked_reason"],
            "controller retired"
        );

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
                vec![2, 1],
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(enrolled["protocol_version"], 1);
        let token = enrolled["token"].as_str().unwrap().to_string();

        let (hello_status, hello) = json_response_with_state(
            state.clone(),
            Request::builder()
                .uri("/api/fleet/v1/hello")
                .header("authorization", format!("Bearer {token}"))
                .header(FLEET_PROTOCOL_VERSIONS_HEADER, "2")
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
                vec![2],
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
        let mut body = fleet_enroll_body(&bootstrap, &[], vec![1]);
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
            "schema_version": 1,
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
            "supported_protocol_versions": [1],
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
            "schema_version": 1,
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
            "supported_protocol_versions": [1],
            "pinned_installation_id": "sase_inst_v1_deadbeef"
        });

        let (status, value) =
            json_response_with_state(state, fleet_enroll_request(oversized))
                .await;

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

        let (status, value) = json_response_with_state(
            state,
            push_subscription_get_request(None),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(value["code"], "unauthorized");
        assert_eq!(value["target"], "authorization");
    }

    #[tokio::test]
    async fn push_subscription_register_list_and_revoke_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;
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
    async fn push_subscription_validation_and_audit_do_not_leak_provider_token()
    {
        let tmp = tempfile::tempdir().unwrap();
        let state = state_for_tmp(&tmp, Duration::minutes(5));
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, device_id) =
            pair_device(state.clone()).await;
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

        publish_agents_changed(
            &state,
            "launch",
            Some("mobile-demo".to_string()),
        )
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
            "model": "gpt-5.6-sol",
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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;
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
    async fn notifications_list_uses_resurface_activity_cursor_and_id_tiebreaker(
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let recent = notification("recent", "2026-05-06T17:00:00Z", None);
        let mut resurfaced_a =
            notification("resurfaced-a", "2026-05-01T12:00:00Z", None);
        resurfaced_a.resurfaced_at = Some("2026-05-06T18:00:00Z".to_string());
        let mut resurfaced_b =
            notification("resurfaced-b", "2026-05-01T11:00:00Z", None);
        resurfaced_b.resurfaced_at = Some("2026-05-06T18:00:00Z".to_string());
        let state = state_for_notifications(
            &tmp,
            vec![recent, resurfaced_a, resurfaced_b],
        );
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let state = GatewayState::new_with_sase_home(
            "127.0.0.1:0".to_string(),
            tmp.path(),
        );
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        state.notification_bridge =
            DynNotificationHostBridge::new(bridge.clone());
        let (_start, _finish, token, _device_id) =
            pair_device(state.clone()).await;

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
        let (_start, finish, _token, _device_id) =
            pair_device(state.clone()).await;
        let device: DeviceRecordWire =
            serde_json::from_value(finish["device"].clone()).unwrap();

        let _first_events =
            initial_events_for_stream(&state, &HeaderMap::new(), &device)
                .unwrap();
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
        headers.insert("last-event-id", first_events[0].id.parse().unwrap());
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
