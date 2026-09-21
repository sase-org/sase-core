use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::fleet_attention::FleetAttentionStore;

use crate::fleet_auth::FleetCredentialStore;

use crate::fleet_launch::FleetLaunchStore;

use crate::fleet_mutations::FleetMutationStore;

use crate::fleet_reads::FleetReadService;

use crate::host_bridge::{
    AgentHostBridge, CommandAgentHostBridge, CommandHelperHostBridge,
    DynAgentHostBridge, DynHelperHostBridge, DynNotificationHostBridge,
    HelperHostBridge, LocalJsonlNotificationBridge, NotificationHostBridge,
    UnavailableAgentHostBridge, UnavailableHelperHostBridge,
};

use crate::push::{PushConfig, PushDispatcher};

use crate::storage::{
    format_time, generate_prefixed_id, AuditLogEntryWire, DeviceTokenStore,
};

use crate::wire::{
    EventPayloadWire, EventRecordWire, GatewayBindWire, GatewayBuildWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};

use super::errors::*;

use super::support::*;

pub(crate) const DEFAULT_EVENT_BUFFER_CAPACITY: usize = 128;
pub(crate) const DEFAULT_HEARTBEAT_INTERVAL: StdDuration =
    StdDuration::from_secs(30);
pub(crate) const DEFAULT_MAX_ATTACHMENT_BYTES: u64 = 20 * 1024 * 1024;
pub(crate) const FLEET_REQUEST_BODY_LIMIT_BYTES: usize = 16 * 1024;
pub(crate) const FLEET_ENROLLMENT_RATE_LIMIT: usize = 8;
pub(crate) const FLEET_ENROLLMENT_RATE_WINDOW_SECONDS: i64 = 60;
pub(crate) const FLEET_PROTOCOL_VERSIONS_HEADER: &str =
    "x-sase-fleet-protocol-versions";

#[derive(Clone, Debug)]
pub struct GatewayState {
    pub(crate) bind: GatewayBindWire,
    pub(crate) build: GatewayBuildWire,
    pub(crate) token_store: DeviceTokenStore,
    pub(crate) pairings: Arc<Mutex<HashMap<String, PairingChallenge>>>,
    pub(crate) pairing_ttl: ChronoDuration,
    pub(crate) host_label: String,
    pub(crate) event_hub: EventHub,
    pub(crate) heartbeat_interval: StdDuration,
    pub(crate) notification_bridge: DynNotificationHostBridge,
    pub(crate) agent_bridge: DynAgentHostBridge,
    pub(crate) helper_bridge: DynHelperHostBridge,
    pub(crate) attachment_tokens: AttachmentTokenStore,
    pub(crate) push_dispatcher: PushDispatcher,
    pub(crate) fleet_store: FleetCredentialStore,
    pub(crate) fleet_launches: FleetLaunchStore,
    pub(crate) fleet_mutations: FleetMutationStore,
    pub(crate) fleet_attention: FleetAttentionStore,
    pub(crate) fleet_reads: FleetReadService,
    pub(crate) fleet_enrollment_limiter: FleetEnrollmentRateLimiter,
    pub(crate) machine_selector: String,
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
pub(crate) struct PairingChallenge {
    pub(crate) code: String,
    pub(crate) expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) struct EventHub {
    pub(crate) inner: Arc<Mutex<EventHubInner>>,
    pub(crate) buffer_capacity: usize,
    pub(crate) sender: tokio::sync::broadcast::Sender<EventRecordWire>,
}

#[derive(Debug)]
pub(crate) struct EventHubInner {
    pub(crate) next_id: u64,
    pub(crate) buffer: VecDeque<EventRecordWire>,
}

pub(crate) struct EventHubSubscription {
    pub(crate) initial_events: Vec<EventRecordWire>,
    pub(crate) receiver: tokio::sync::broadcast::Receiver<EventRecordWire>,
}

impl std::ops::Deref for EventHubSubscription {
    type Target = [EventRecordWire];

    fn deref(&self) -> &Self::Target {
        &self.initial_events
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AttachmentTokenStore {
    pub(crate) inner: Arc<Mutex<HashMap<String, AttachmentTokenRecord>>>,
    pub(crate) ttl: ChronoDuration,
    pub(crate) max_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct AttachmentTokenRecord {
    pub(crate) device_id: String,
    pub(crate) canonical_path: PathBuf,
    pub(crate) source_notification_id: String,
    pub(crate) display_name: String,
    pub(crate) content_type: Option<String>,
    pub(crate) byte_size: u64,
    pub(crate) expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) struct AttachmentMintRequest {
    pub(crate) device_id: String,
    pub(crate) canonical_path: PathBuf,
    pub(crate) source_notification_id: String,
    pub(crate) display_name: String,
    pub(crate) content_type: Option<String>,
    pub(crate) byte_size: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct FleetEnrollmentRateLimiter {
    pub(crate) inner: Arc<Mutex<VecDeque<DateTime<Utc>>>>,
    pub(crate) limit: usize,
    pub(crate) window: ChronoDuration,
}

impl FleetEnrollmentRateLimiter {
    pub(crate) fn new(limit: usize, window: ChronoDuration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
            limit,
            window,
        }
    }

    pub(crate) fn check(&self, now: DateTime<Utc>) -> Result<bool, ApiError> {
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
pub(crate) enum AttachmentTokenLookup {
    Found(AttachmentTokenRecord),
    Missing,
    Expired,
    WrongDevice,
}

impl AttachmentTokenStore {
    pub(crate) fn new(ttl: ChronoDuration, max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            max_bytes,
        }
    }

    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub(crate) fn mint(
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

    pub(crate) fn resolve(
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
    pub(crate) fn new(buffer_capacity: usize) -> Self {
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

    pub(crate) fn append(
        &self,
        make_payload: impl FnOnce(u64) -> EventPayloadWire,
    ) -> Result<EventRecordWire, ApiError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ApiError::internal("events"))?;
        Ok(self.append_with_inner(&mut inner, make_payload))
    }

    pub(crate) fn append_with_inner(
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
    pub(crate) fn replay_after(
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

    pub(crate) fn subscribe_after(
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

    pub(crate) fn transient_event(
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

    pub(crate) fn current_sequence(&self) -> u64 {
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
impl GatewayState {
    pub(crate) fn audit(
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
