use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use sase_core::projections::{EventSourceWire, SchedulerEventContextWire};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::{debug, info, info_span, warn, Instrument};

use crate::{
    indexer::{IndexingConfig, IndexingService},
    local_transport::{serve_local_transport, LocalTransportError},
    metrics::DaemonMetrics,
    ownership::{DaemonOwnershipGuard, OwnershipError},
    projection_service::{
        default_projection_db_path, ProjectionService, ProjectionServiceError,
    },
    provider_host_manager::ProviderHostManager,
    push::PushConfig,
    routes::GatewayState,
    server::{
        serve_listener_with_state, validate_bind_policy, GatewayConfig,
        GatewayRunError,
    },
    wire::{
        GatewayBuildWire, LocalDaemonCollectionWire,
        LocalDaemonEventPayloadWire, LocalDaemonEventRecordWire,
        LocalDaemonEventSourceWire, LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
    },
};

const DAEMON_SOCKET_NAME: &str = "sase-daemon.sock";
const LOCAL_EVENT_BUFFER_CAPACITY: usize = 256;
const DAEMON_LOCK_FILE_NAME: &str = "daemon.lock";
const DAEMON_LOCK_METADATA_FILE_NAME: &str = "daemon.lock.json";
const DAEMON_LOG_FILE_NAME: &str = "daemon.log";
const PROJECTION_WAL_SUFFIX: &str = "-wal";
const PROJECTION_SHM_SUFFIX: &str = "-shm";
const SOURCE_ROOT_NAMES: &[&str] = &[
    "projects",
    "notifications",
    "pending_actions",
    "artifacts",
    "chats",
    "beads",
    "repos",
    "workflow_state",
    "telegram",
    "mobile_gateway",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonRuntimePaths {
    pub sase_home: PathBuf,
    pub run_root: PathBuf,
    pub socket_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonConfig {
    pub paths: DaemonRuntimePaths,
    pub host_identity: String,
    pub foreground: bool,
    pub enable_tokio_console: bool,
    pub rebuild_once: bool,
    pub mobile_http_enabled: bool,
    pub mobile_gateway: GatewayConfig,
}

#[derive(Clone, Debug)]
pub struct DaemonConfigOptions {
    pub sase_home: PathBuf,
    pub host_identity: String,
    pub run_root: Option<PathBuf>,
    pub socket_path: Option<PathBuf>,
    pub foreground: bool,
    pub enable_tokio_console: bool,
    pub mobile_http_enabled: bool,
    pub mobile_gateway: GatewayConfig,
}

impl DaemonConfig {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        Self::with_options(DaemonConfigOptions {
            sase_home: sase_home.into(),
            host_identity: host_identity_from_env(),
            run_root: None,
            socket_path: None,
            foreground: false,
            enable_tokio_console: false,
            mobile_http_enabled: true,
            mobile_gateway: GatewayConfig::default(),
        })
    }

    pub fn with_options(options: DaemonConfigOptions) -> Self {
        let sase_home = options.sase_home;
        let host_identity = sanitize_host_identity(&options.host_identity);
        let run_root = options
            .run_root
            .unwrap_or_else(|| default_run_root(&sase_home, &host_identity));
        let socket_path = options
            .socket_path
            .unwrap_or_else(|| default_socket_path(&run_root));
        let mut mobile_gateway = options.mobile_gateway;
        mobile_gateway.sase_home = sase_home.clone();
        Self {
            paths: DaemonRuntimePaths {
                sase_home,
                run_root,
                socket_path,
            },
            host_identity,
            foreground: options.foreground,
            enable_tokio_console: options.enable_tokio_console,
            rebuild_once: false,
            mobile_http_enabled: options.mobile_http_enabled,
            mobile_gateway,
        }
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self::new(crate::routes::default_sase_home())
    }
}

#[derive(Clone, Debug)]
pub struct DaemonRuntime {
    state: DaemonState,
    ownership: Arc<DaemonOwnershipGuard>,
}

impl DaemonRuntime {
    pub fn new(
        config: DaemonConfig,
        ownership: DaemonOwnershipGuard,
        projection_service: ProjectionService,
        metrics: DaemonMetrics,
    ) -> Self {
        let ownership = Arc::new(ownership);
        let shutdown = DaemonShutdown::default();
        let local_events =
            LocalEventHub::new(LOCAL_EVENT_BUFFER_CAPACITY, metrics.clone());
        let projects_root = config.paths.sase_home.join("projects");
        let mut watch_roots = Vec::new();
        for root in [
            projects_root.clone(),
            config.paths.sase_home.join("notifications"),
            config.paths.sase_home.join("pending_actions"),
            config.paths.sase_home.join("telegram"),
        ] {
            if root.exists() {
                watch_roots.push(root);
            }
        }
        let indexing_service = IndexingService::start(
            IndexingConfig {
                watch_roots,
                projects_root: Some(projects_root),
                host_id: config.host_identity.clone(),
                ..IndexingConfig::default()
            },
            shutdown.clone(),
            metrics.clone(),
            local_events.clone(),
            projection_service.clone(),
        );
        let state = DaemonState {
            build: GatewayBuildWire {
                package_version: env!("CARGO_PKG_VERSION").to_string(),
                git_sha: None,
            },
            host_identity: config.host_identity.clone(),
            paths: config.paths.clone(),
            shutdown,
            metrics: metrics.clone(),
            metrics_endpoint: Arc::new(RwLock::new(None)),
            local_events,
            projection_service,
            indexing_service,
            provider_host_manager: ProviderHostManager::from_env_or_default(),
            mobile_gateway: config
                .mobile_http_enabled
                .then_some(config.mobile_gateway.clone()),
        };
        recover_scheduler_starting_slots(&state);
        Self { state, ownership }
    }

    pub fn state(&self) -> &DaemonState {
        &self.state
    }

    pub fn ownership(&self) -> &DaemonOwnershipGuard {
        &self.ownership
    }

    fn mobile_gateway_state(
        &self,
        local_addr: SocketAddr,
    ) -> Option<GatewayState> {
        let config = self.state.mobile_gateway.as_ref()?;
        Some(
            GatewayState::new_with_sase_home_and_bridge_commands(
                local_addr.to_string(),
                config.sase_home.clone(),
                config.agent_bridge_command.clone(),
                config.helper_bridge_command.clone(),
                config.push_config.clone(),
            )
            .with_daemon_metrics(self.state.metrics.clone()),
        )
    }
}

#[derive(Clone, Debug)]
pub struct DaemonState {
    pub build: GatewayBuildWire,
    pub host_identity: String,
    pub paths: DaemonRuntimePaths,
    pub shutdown: DaemonShutdown,
    pub metrics: DaemonMetrics,
    pub metrics_endpoint: Arc<RwLock<Option<String>>>,
    pub local_events: LocalEventHub,
    pub projection_service: ProjectionService,
    pub indexing_service: IndexingService,
    pub provider_host_manager: ProviderHostManager,
    pub mobile_gateway: Option<GatewayConfig>,
}

impl DaemonState {
    pub fn log_path(&self) -> PathBuf {
        self.paths.run_root.join(DAEMON_LOG_FILE_NAME)
    }

    pub fn set_metrics_endpoint(&self, endpoint: Option<String>) {
        if let Ok(mut guard) = self.metrics_endpoint.write() {
            *guard = endpoint;
        }
    }

    pub fn health_details(&self) -> JsonValue {
        let details = self.projection_service.basic_health_details();
        self.with_runtime_details(details)
    }

    pub fn diagnostic_details(&self) -> JsonValue {
        let details = self.projection_service.health_details();
        self.with_runtime_details(details)
    }

    fn with_runtime_details(&self, mut details: JsonValue) -> JsonValue {
        if let JsonValue::Object(ref mut object) = details {
            object.insert(
                "indexing".to_string(),
                self.indexing_service.health_details(),
            );
            object.insert(
                "metrics".to_string(),
                json!({
                    "endpoint": self
                        .metrics_endpoint
                        .read()
                        .ok()
                        .and_then(|value| value.clone()),
                    "loopback_only": true,
                }),
            );
            let host_status = self.provider_host_manager.status();
            object.insert(
                "provider_host".to_string(),
                json!({
                    "command": host_status.command,
                    "active_calls": host_status.active_calls,
                    "max_concurrent_calls": host_status.max_concurrent_calls,
                    "total_started": host_status.total_started,
                    "total_completed": host_status.total_completed,
                    "total_failed": host_status.total_failed,
                    "timeout_count": host_status.total_timeouts,
                    "cancellation_count": host_status.total_cancellations,
                    "backpressure_count": host_status.total_backpressure,
                    "manifest_denial_count": host_status.total_manifest_denials,
                    "resource_policy": self
                        .provider_host_manager
                        .resource_policy_diagnostics(),
                }),
            );
            object.insert(
                "logs".to_string(),
                json!({
                    "path": self.log_path(),
                }),
            );
            let layout = storage_layout_diagnostics(
                &self.paths,
                &self.host_identity,
                &self.projection_service.status().path,
                &self.log_path(),
            );
            if let Some(projection) = object
                .get_mut("projection_db")
                .and_then(JsonValue::as_object_mut)
            {
                projection.insert(
                    "path".to_string(),
                    json!(self.projection_service.status().path),
                );
                projection.insert(
                    "path_kind".to_string(),
                    layout
                        .get("projection_db_path")
                        .and_then(|entry| entry.get("path_kind"))
                        .cloned()
                        .unwrap_or_else(|| json!("unknown")),
                );
            }
            if let Some(logs) =
                object.get_mut("logs").and_then(JsonValue::as_object_mut)
            {
                logs.insert(
                    "path_kind".to_string(),
                    layout
                        .get("log_path")
                        .and_then(|entry| entry.get("path_kind"))
                        .cloned()
                        .unwrap_or_else(|| json!("unknown")),
                );
            }
            object.insert("storage_layout".to_string(), layout);
        }
        details
    }
}

pub fn storage_layout_diagnostics(
    paths: &DaemonRuntimePaths,
    host_identity: &str,
    projection_db_path: &Path,
    log_path: &Path,
) -> JsonValue {
    let default_run = default_run_root(&paths.sase_home, host_identity);
    let source_roots: Vec<PathBuf> = SOURCE_ROOT_NAMES
        .iter()
        .map(|name| paths.sase_home.join(name))
        .collect();
    let runtime_files = runtime_files(
        &paths.run_root,
        &paths.socket_path,
        projection_db_path,
        log_path,
    );
    let path_entries = json!({
        "sase_home": path_entry(&paths.sase_home, "source_root"),
        "run_root": path_entry(
            &paths.run_root,
            classify_storage_path(
                &paths.run_root,
                &paths.sase_home,
                &paths.run_root,
                &default_run,
                &source_roots,
            ),
        ),
        "socket_path": path_entry(
            &paths.socket_path,
            classify_storage_path(
                &paths.socket_path,
                &paths.sase_home,
                &paths.run_root,
                &default_run,
                &source_roots,
            ),
        ),
        "projection_db_path": path_entry(
            projection_db_path,
            classify_storage_path(
                projection_db_path,
                &paths.sase_home,
                &paths.run_root,
                &default_run,
                &source_roots,
            ),
        ),
        "log_path": path_entry(
            log_path,
            classify_storage_path(
                log_path,
                &paths.sase_home,
                &paths.run_root,
                &default_run,
                &source_roots,
            ),
        ),
    });
    let mut layout = path_entries;
    if let JsonValue::Object(ref mut object) = layout {
        object.insert("schema_version".to_string(), json!(1));
        object.insert(
            "source_roots".to_string(),
            JsonValue::Array(
                source_roots
                    .iter()
                    .map(|path| path_entry(path, "source_root"))
                    .collect(),
            ),
        );
        object.insert("runtime_files".to_string(), json!(runtime_files));
        object.insert(
            "warnings".to_string(),
            JsonValue::Array(layout_warnings(
                paths,
                &default_run,
                &source_roots,
                object,
            )),
        );
    }
    layout
}

fn runtime_files(
    run_root: &Path,
    socket_path: &Path,
    projection_db_path: &Path,
    log_path: &Path,
) -> Vec<PathBuf> {
    vec![
        socket_path.to_path_buf(),
        run_root.join(DAEMON_LOCK_FILE_NAME),
        run_root.join(DAEMON_LOCK_METADATA_FILE_NAME),
        log_path.to_path_buf(),
        projection_db_path.to_path_buf(),
        projection_db_path.with_file_name(format!(
            "{}{}",
            projection_db_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("projection.sqlite"),
            PROJECTION_WAL_SUFFIX
        )),
        projection_db_path.with_file_name(format!(
            "{}{}",
            projection_db_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("projection.sqlite"),
            PROJECTION_SHM_SUFFIX
        )),
        run_root.join("checkpoints"),
        run_root.join("backups"),
        run_root.join("queues"),
    ]
}

fn path_entry(path: &Path, path_kind: &str) -> JsonValue {
    json!({
        "path": path,
        "path_kind": path_kind,
    })
}

fn classify_storage_path(
    path: &Path,
    sase_home: &Path,
    run_root: &Path,
    default_run_root: &Path,
    source_roots: &[PathBuf],
) -> &'static str {
    if path == sase_home
        || source_roots.iter().any(|root| path.starts_with(root))
    {
        return "source_root";
    }
    if path.starts_with(default_run_root) {
        return "host_local_default";
    }
    if path.starts_with(run_root) {
        return "host_local_override";
    }
    if path.starts_with(sase_home) && !path.starts_with(sase_home.join("run")) {
        return "unsafe_synced_candidate";
    }
    if path.starts_with(sase_home.join("run")) {
        return "host_local_override";
    }
    "unknown"
}

fn layout_warnings(
    paths: &DaemonRuntimePaths,
    default_run_root: &Path,
    source_roots: &[PathBuf],
    path_entries: &serde_json::Map<String, JsonValue>,
) -> Vec<JsonValue> {
    let mut warnings = Vec::new();
    if paths.run_root != default_run_root {
        warnings.push(json!({
            "id": "run_root_override",
            "severity": "warning",
            "path": paths.run_root,
            "message": "run_root is not the default host-local directory; ensure it is excluded from sync",
        }));
    }
    if source_roots
        .iter()
        .any(|root| paths.run_root.starts_with(root))
    {
        warnings.push(json!({
            "id": "run_root_under_source_root",
            "severity": "error",
            "path": paths.run_root,
            "message": "run_root is under a source store; move daemon runtime files out of synced source state",
        }));
    }
    if !paths.socket_path.starts_with(&paths.run_root) {
        warnings.push(json!({
            "id": "socket_outside_run_root",
            "severity": "warning",
            "path": paths.socket_path,
            "message": "socket_path is outside run_root; keep sockets and locks host-local",
        }));
    }
    for (name, entry) in path_entries {
        let path_kind = entry
            .get("path_kind")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown");
        if path_kind == "unsafe_synced_candidate" {
            let path = entry.get("path").cloned().unwrap_or_else(|| json!(""));
            warnings.push(json!({
                "id": format!("{name}_unsafe_synced_candidate"),
                "severity": "error",
                "path": path,
                "message": format!("{name} looks like it lives in synced source state; exclude daemon runtime files from sync"),
            }));
        }
    }
    if paths.run_root.starts_with(&paths.sase_home)
        && !paths.run_root.starts_with(paths.sase_home.join("run"))
    {
        warnings.push(json!({
            "id": "run_root_under_sase_home_non_run",
            "severity": "error",
            "path": paths.run_root,
            "message": "run_root under sase_home should live below run/<host>",
        }));
    }
    warnings
}

fn recover_scheduler_starting_slots(state: &DaemonState) {
    let context = SchedulerEventContextWire {
        schema_version: sase_core::projections::SCHEDULER_WIRE_SCHEMA_VERSION,
        created_at: Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
        source: EventSourceWire {
            source_type: "daemon_startup".to_string(),
            name: "sase_gateway".to_string(),
            version: Some(state.build.package_version.clone()),
            runtime: Some("rust".to_string()),
            metadata: Default::default(),
        },
        host_id: state.host_identity.clone(),
        project_id: "global".to_string(),
        idempotency_key: None,
        causality: Vec::new(),
        source_path: None,
        source_revision: None,
    };
    let recovered = state
        .projection_service
        .write_blocking(|db| db.recover_scheduler_starting_slots(context));
    match recovered {
        Ok(count) => state
            .metrics
            .record_scheduler_recovery_repairs(u64::from(count)),
        Err(error) => {
            warn!(%error, "failed to recover scheduler starting slots")
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalEventHub {
    inner: Arc<Mutex<LocalEventHubInner>>,
    buffer_capacity: usize,
    metrics: DaemonMetrics,
}

#[derive(Debug)]
struct LocalEventHubInner {
    next_id: u64,
    snapshot_id: String,
    buffer: VecDeque<LocalDaemonEventRecordWire>,
}

impl LocalEventHub {
    pub(crate) fn new(buffer_capacity: usize, metrics: DaemonMetrics) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LocalEventHubInner {
                next_id: 1,
                snapshot_id: format!(
                    "events_{}",
                    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
                ),
                buffer: VecDeque::new(),
            })),
            buffer_capacity,
            metrics,
        }
    }

    pub(crate) fn append_heartbeat(&self) -> LocalDaemonEventRecordWire {
        self.append(|sequence| LocalDaemonEventPayloadWire::Heartbeat {
            sequence,
        })
    }

    pub(crate) fn append_resync_required(
        &self,
        reason: impl Into<String>,
    ) -> LocalDaemonEventRecordWire {
        let reason = reason.into();
        self.append(|_| LocalDaemonEventPayloadWire::ResyncRequired { reason })
    }

    pub(crate) fn append(
        &self,
        make_payload: impl FnOnce(u64) -> LocalDaemonEventPayloadWire,
    ) -> LocalDaemonEventRecordWire {
        let mut inner = self
            .inner
            .lock()
            .expect("local daemon event hub mutex poisoned");
        let sequence = inner.next_id;
        inner.next_id += 1;
        let record = LocalDaemonEventRecordWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            event_id: format_local_event_id(sequence),
            snapshot_id: inner.snapshot_id.clone(),
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            source: LocalDaemonEventSourceWire::Daemon,
            payload: make_payload(sequence),
        };
        inner.buffer.push_back(record.clone());
        while inner.buffer.len() > self.buffer_capacity {
            inner.buffer.pop_front();
            self.metrics.record_dropped_event();
        }
        record
    }

    pub(crate) fn replay_after(
        &self,
        event_id: &str,
        collections: &[LocalDaemonCollectionWire],
        limit: usize,
    ) -> Option<Vec<LocalDaemonEventRecordWire>> {
        let after = parse_local_event_id(event_id)?;
        let inner = self
            .inner
            .lock()
            .expect("local daemon event hub mutex poisoned");
        let oldest = inner
            .buffer
            .front()
            .and_then(|record| parse_local_event_id(&record.event_id))?;
        if after.saturating_add(1) < oldest {
            return None;
        }
        Some(
            inner
                .buffer
                .iter()
                .filter(|record| {
                    parse_local_event_id(&record.event_id)
                        .map(|id| id > after)
                        .unwrap_or(false)
                })
                .filter(|record| event_matches_collections(record, collections))
                .take(limit)
                .cloned()
                .collect(),
        )
    }
}

fn event_matches_collections(
    record: &LocalDaemonEventRecordWire,
    collections: &[LocalDaemonCollectionWire],
) -> bool {
    if collections.is_empty() {
        return true;
    }
    match &record.payload {
        LocalDaemonEventPayloadWire::Delta { collection, .. } => {
            collections.iter().any(|candidate| candidate == collection)
        }
        LocalDaemonEventPayloadWire::Heartbeat { .. }
        | LocalDaemonEventPayloadWire::ResyncRequired { .. } => true,
    }
}

fn format_local_event_id(id: u64) -> String {
    format!("{id:016}")
}

fn parse_local_event_id(id: &str) -> Option<u64> {
    id.parse::<u64>().ok()
}

#[derive(Clone, Debug, Default)]
pub struct DaemonShutdown {
    requested: Arc<AtomicBool>,
}

impl DaemonShutdown {
    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Error)]
pub enum DaemonRunError {
    #[error(transparent)]
    Ownership(#[from] OwnershipError),
    #[error(transparent)]
    LocalTransport(#[from] LocalTransportError),
    #[error(transparent)]
    MobileGateway(#[from] GatewayRunError),
    #[error(transparent)]
    ProjectionService(#[from] ProjectionServiceError),
    #[error("failed to wait for daemon shutdown signal: {0}")]
    ShutdownSignal(std::io::Error),
    #[error("daemon task failed: {0}")]
    TaskJoin(String),
}

pub async fn run_daemon(config: DaemonConfig) -> Result<(), DaemonRunError> {
    initialize_daemon_tracing();
    initialize_tokio_console(config.enable_tokio_console);
    validate_daemon_config(&config)?;
    let startup_span = info_span!(
        "daemon_startup",
        host = %config.host_identity,
        run_root = %config.paths.run_root.display(),
        socket = %config.paths.socket_path.display(),
    );
    async move {
    info!("starting local SASE daemon");
    let build = GatewayBuildWire {
        package_version: env!("CARGO_PKG_VERSION").to_string(),
        git_sha: None,
    };
    let lock_span = info_span!("daemon_lock_acquire");
    let _lock_span_guard = lock_span.enter();
    let ownership = DaemonOwnershipGuard::acquire(
        config.paths.run_root.clone(),
        config.paths.socket_path.clone(),
        config.host_identity.clone(),
        config.paths.sase_home.clone(),
        &build,
    )?;
    debug!("daemon ownership lock acquired");
    drop(_lock_span_guard);
    let metrics = DaemonMetrics::default();
    let projection_service = ProjectionService::initialize_with_metrics(
        default_projection_db_path(&config.paths.run_root),
        metrics.clone(),
    )
    .await;
    if config.rebuild_once {
        let report = projection_service.rebuild_storage_reset_only().await?;
        let health = projection_service.health_details();
        let source_exports = health
            .get("projection_db")
            .and_then(|projection| projection.get("source_exports"))
            .cloned()
            .unwrap_or_else(|| json!({"state": "unknown"}));
        let payload = json!({
            "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            "mode": "projection_storage_rebuild",
            "storage_reset_only": true,
            "limitation": "domain source rebuild is not active until filesystem indexing lands; this reset replays retained projection events only",
            "report": report,
            "source_exports": source_exports,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .unwrap_or_else(|_| payload.to_string())
        );
        return Ok(());
    }
    let runtime =
        DaemonRuntime::new(config, ownership, projection_service, metrics);

    let mut local_transport_task =
        tokio::spawn(serve_local_transport(runtime.state().clone()));

    if let Some(mobile_gateway) = runtime.state().mobile_gateway.as_ref() {
        let listener =
            TcpListener::bind(mobile_gateway.bind)
                .await
                .map_err(|source| GatewayRunError::Bind {
                    bind: mobile_gateway.bind,
                    source,
                })?;
        let local_addr =
            listener.local_addr().map_err(GatewayRunError::Serve)?;
        if local_addr.ip().is_loopback() {
            runtime.state().set_metrics_endpoint(Some(format!(
                "http://{local_addr}/metrics"
            )));
        } else {
            runtime.state().set_metrics_endpoint(None);
        }
        let state = runtime.mobile_gateway_state(local_addr).expect(
            "mobile gateway state is available when mobile HTTP is enabled",
        );
        info!(bind = %local_addr, "mobile HTTP gateway started in daemon mode");
        let mut mobile_task =
            tokio::spawn(serve_listener_with_state(listener, state));
        tokio::select! {
            result = &mut local_transport_task => {
                mobile_task.abort();
                task_result(result)?
            },
            result = &mut mobile_task => {
                runtime.state().shutdown.request();
                local_transport_task.abort();
                result
                    .map_err(|err| DaemonRunError::TaskJoin(err.to_string()))?
                    .map_err(DaemonRunError::from)?
            },
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(DaemonRunError::ShutdownSignal)?;
                info!("shutdown signal received");
                runtime.state().shutdown.request();
                mobile_task.abort();
                wait_for_local_transport_shutdown(local_transport_task).await?;
            }
        }
        return Ok(());
    }

    tokio::select! {
        result = &mut local_transport_task => task_result(result)?,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(DaemonRunError::ShutdownSignal)?;
            info!("shutdown signal received");
            runtime.state().shutdown.request();
            wait_for_local_transport_shutdown(local_transport_task).await?;
        }
    }
    info!("local SASE daemon shutdown complete");
    Ok(())
    }
    .instrument(startup_span)
    .await
}

fn initialize_daemon_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "sase_gateway=info,tower_http=warn".into());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

#[cfg(feature = "tokio-console")]
fn initialize_tokio_console(enable: bool) {
    if enable {
        console_subscriber::init();
    }
}

#[cfg(not(feature = "tokio-console"))]
fn initialize_tokio_console(enable: bool) {
    if enable {
        warn!(
            "tokio console requested but sase_gateway was built without the tokio-console feature"
        );
    }
}

fn task_result(
    result: Result<Result<(), LocalTransportError>, tokio::task::JoinError>,
) -> Result<(), DaemonRunError> {
    result
        .map_err(|err| DaemonRunError::TaskJoin(err.to_string()))?
        .map_err(DaemonRunError::from)
}

async fn wait_for_local_transport_shutdown(
    task: tokio::task::JoinHandle<Result<(), LocalTransportError>>,
) -> Result<(), DaemonRunError> {
    match tokio::time::timeout(Duration::from_secs(1), task).await {
        Ok(result) => task_result(result),
        Err(_) => Ok(()),
    }
}

pub fn validate_daemon_config(
    config: &DaemonConfig,
) -> Result<(), GatewayRunError> {
    if config.mobile_http_enabled {
        validate_bind_policy(&config.mobile_gateway)?;
    }
    Ok(())
}

pub fn default_run_root(sase_home: &Path, host_identity: &str) -> PathBuf {
    sase_home
        .join("run")
        .join(sanitize_host_identity(host_identity))
}

pub fn default_socket_path(run_root: &Path) -> PathBuf {
    run_root.join(DAEMON_SOCKET_NAME)
}

pub fn host_identity_from_env() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| sanitize_host_identity(&value))
        .unwrap_or_else(|| "sase-host".to_string())
}

pub fn sanitize_host_identity(value: &str) -> String {
    let sanitized: String = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "sase-host".to_string()
    } else {
        sanitized.to_string()
    }
}

pub fn mobile_gateway_config(
    bind: SocketAddr,
    sase_home: PathBuf,
    allow_non_loopback: bool,
    agent_bridge_command: Vec<String>,
    helper_bridge_command: Vec<String>,
    push_config: PushConfig,
) -> GatewayConfig {
    GatewayConfig {
        bind,
        sase_home,
        allow_non_loopback,
        agent_bridge_command,
        helper_bridge_command,
        push_config,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::GatewayConfig;

    #[test]
    fn daemon_default_paths_are_host_local_under_sase_home() {
        let config = DaemonConfig::with_options(DaemonConfigOptions {
            sase_home: PathBuf::from("/tmp/sase-home"),
            host_identity: "workstation.local".to_string(),
            run_root: None,
            socket_path: None,
            foreground: false,
            enable_tokio_console: false,
            mobile_http_enabled: true,
            mobile_gateway: GatewayConfig::default(),
        });

        assert_eq!(config.paths.sase_home, PathBuf::from("/tmp/sase-home"));
        assert_eq!(
            config.paths.run_root,
            PathBuf::from("/tmp/sase-home/run/workstation.local")
        );
        assert_eq!(
            config.paths.socket_path,
            PathBuf::from(
                "/tmp/sase-home/run/workstation.local/sase-daemon.sock"
            )
        );
    }

    #[test]
    fn daemon_run_root_override_drives_default_socket_path() {
        let config = DaemonConfig::with_options(DaemonConfigOptions {
            sase_home: PathBuf::from("/tmp/sase-home"),
            host_identity: "workstation".to_string(),
            run_root: Some(PathBuf::from("/tmp/run-root")),
            socket_path: None,
            foreground: true,
            enable_tokio_console: false,
            mobile_http_enabled: false,
            mobile_gateway: GatewayConfig::default(),
        });

        assert_eq!(config.paths.run_root, PathBuf::from("/tmp/run-root"));
        assert_eq!(
            config.paths.socket_path,
            PathBuf::from("/tmp/run-root/sase-daemon.sock")
        );
        assert!(config.foreground);
        assert!(!config.mobile_http_enabled);
    }

    #[test]
    fn daemon_socket_path_override_is_preserved() {
        let config = DaemonConfig::with_options(DaemonConfigOptions {
            sase_home: PathBuf::from("/tmp/sase-home"),
            host_identity: "workstation".to_string(),
            run_root: Some(PathBuf::from("/tmp/run-root")),
            socket_path: Some(PathBuf::from("/tmp/custom.sock")),
            foreground: false,
            enable_tokio_console: false,
            mobile_http_enabled: true,
            mobile_gateway: GatewayConfig::default(),
        });

        assert_eq!(config.paths.socket_path, PathBuf::from("/tmp/custom.sock"));
    }

    #[test]
    fn daemon_host_identity_is_path_safe() {
        assert_eq!(
            sanitize_host_identity("work station/01"),
            "work-station-01"
        );
        assert_eq!(sanitize_host_identity("  "), "sase-host");
    }

    #[test]
    fn daemon_path_contract_matches_python_cases() {
        let cases = [
            (
                "normal",
                "workstation.local",
                "/tmp/sase-home/run/workstation.local",
                "/tmp/sase-home/run/workstation.local/sase-daemon.sock",
            ),
            (
                "empty",
                "  ",
                "/tmp/sase-home/run/sase-host",
                "/tmp/sase-home/run/sase-host/sase-daemon.sock",
            ),
            (
                "malformed",
                "work station/01",
                "/tmp/sase-home/run/work-station-01",
                "/tmp/sase-home/run/work-station-01/sase-daemon.sock",
            ),
        ];

        for (_name, host, run_root, socket_path) in cases {
            let config = DaemonConfig::with_options(DaemonConfigOptions {
                sase_home: PathBuf::from("/tmp/sase-home"),
                host_identity: host.to_string(),
                run_root: None,
                socket_path: None,
                foreground: false,
                enable_tokio_console: false,
                mobile_http_enabled: true,
                mobile_gateway: GatewayConfig::default(),
            });

            assert_eq!(config.paths.run_root, PathBuf::from(run_root));
            assert_eq!(config.paths.socket_path, PathBuf::from(socket_path));
        }

        let override_config = DaemonConfig::with_options(DaemonConfigOptions {
            sase_home: PathBuf::from("/tmp/sase-home"),
            host_identity: "workstation.local".to_string(),
            run_root: Some(PathBuf::from("/tmp/sase-run")),
            socket_path: None,
            foreground: false,
            enable_tokio_console: false,
            mobile_http_enabled: true,
            mobile_gateway: GatewayConfig::default(),
        });
        assert_eq!(
            override_config.paths.run_root,
            PathBuf::from("/tmp/sase-run")
        );
        assert_eq!(
            override_config.paths.socket_path,
            PathBuf::from("/tmp/sase-run/sase-daemon.sock")
        );
    }

    #[test]
    fn storage_layout_diagnostics_warn_for_synced_runtime_paths() {
        let paths = DaemonRuntimePaths {
            sase_home: PathBuf::from("/tmp/sase-home"),
            run_root: PathBuf::from("/tmp/sase-home/projects/demo/run"),
            socket_path: PathBuf::from(
                "/tmp/sase-home/projects/demo/run/sase-daemon.sock",
            ),
        };
        let projection = default_projection_db_path(&paths.run_root);
        let layout = storage_layout_diagnostics(
            &paths,
            "workstation.local",
            &projection,
            &paths.run_root.join("daemon.log"),
        );

        assert_eq!(
            layout["run_root"]["path_kind"],
            JsonValue::String("source_root".to_string())
        );
        let warnings = layout["warnings"].as_array().unwrap();
        assert!(warnings.iter().any(|warning| {
            warning["id"] == "run_root_under_source_root"
                && warning["severity"] == "error"
        }));
    }

    #[test]
    fn daemon_mobile_bind_policy_is_skipped_when_mobile_http_disabled() {
        let config = DaemonConfig::with_options(DaemonConfigOptions {
            sase_home: PathBuf::from("/tmp/sase-home"),
            host_identity: "workstation".to_string(),
            run_root: None,
            socket_path: None,
            foreground: false,
            enable_tokio_console: false,
            mobile_http_enabled: false,
            mobile_gateway: GatewayConfig {
                bind: "0.0.0.0:7629".parse().unwrap(),
                ..GatewayConfig::default()
            },
        });

        validate_daemon_config(&config).unwrap();
    }
}
