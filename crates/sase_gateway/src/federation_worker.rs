use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use sase_core::fleet_attention::{
    FleetAttentionInventoryRequestWire, FleetAttentionRequestWire,
};
use sase_core::fleet_contract::{
    validate_connection_plan, ConnectionPlanWire, FleetCatalogQueryWire,
    FleetContentReadRequestWire, FleetDetailRequestWire,
    FleetLaunchRequestWire, FleetLogicalBatchRequestWire,
    FleetProjectEligibilityRequestWire, TlsTrustModeWire,
};
use sase_core::fleet_mutation::FleetMutationRequestWire;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, Semaphore};

use crate::wire::{FleetHelloResponseWire, FLEET_PROTOCOL_VERSION};

pub const FEDERATION_WORKER_SERVICE: &str = "sase_federation_worker";
pub const FEDERATION_IPC_SCHEMA_VERSION: u32 = 1;
pub const FEDERATION_MAX_FRAME_BYTES: usize = 1024 * 1024;
const FEDERATION_SOCKET_NAME: &str = "sase-federation-worker.sock";
const FEDERATION_LOCK_NAME: &str = "sase-federation-worker.lock";
const FLEET_PROTOCOL_VERSIONS_HEADER: &str = "x-sase-fleet-protocol-versions";
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MAX_CONNECTIONS: usize = 32;
const DEFAULT_MAX_IN_FLIGHT: usize = 16;
const DEFAULT_PER_HOST_IN_FLIGHT: usize = 4;
const DEFAULT_CACHE_ENTRY_LIMIT: usize = 256;
const DEFAULT_CACHE_BYTE_LIMIT: usize = 4 * 1024 * 1024;
/// Slack added to the outer envelope-level deadline beyond the requested
/// `deadline_unix_ms`. Read/mutate handlers already bound their own I/O to the
/// exact requested deadline (each per-host fan-out task races the same instant
/// via its own `with_deadline`), so the outer wrapper is a backstop against an
/// operation that never respects the deadline internally, not the precise
/// enforcement point. Without this grace period the outer timeout can fire a
/// few milliseconds before a handler that resolved right at the deadline
/// finishes sorting/serializing its already-complete per-host results,
/// discarding a correctly bounded response (including hosts that answered
/// successfully) in favor of a bare top-level deadline error.
const OUTER_DEADLINE_GRACE: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FederationWorkerConfig {
    pub sase_home: PathBuf,
    pub run_root: PathBuf,
    pub socket_path: PathBuf,
    pub idle_timeout: Duration,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_in_flight: usize,
    pub cache_entry_limit: usize,
    pub cache_byte_limit: usize,
}

impl FederationWorkerConfig {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        let sase_home = sase_home.into();
        let host_identity = crate::daemon::host_identity_from_env();
        let run_root =
            crate::daemon::default_run_root(&sase_home, &host_identity);
        let socket_path = default_federation_socket_path(&run_root);
        Self {
            sase_home,
            run_root,
            socket_path,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_frame_bytes: FEDERATION_MAX_FRAME_BYTES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            cache_entry_limit: DEFAULT_CACHE_ENTRY_LIMIT,
            cache_byte_limit: DEFAULT_CACHE_BYTE_LIMIT,
        }
    }
}

impl Default for FederationWorkerConfig {
    fn default() -> Self {
        Self::new(crate::routes::default_sase_home())
    }
}

pub fn default_federation_socket_path(run_root: &Path) -> PathBuf {
    run_root.join(FEDERATION_SOCKET_NAME)
}

pub fn run_federation_worker_cli(
    args: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let config = parse_federation_worker_args(args)?;
    run_federation_worker_blocking(config)
        .map_err(|error| format!("federation worker failed: {error}"))
}

pub fn run_federation_worker_blocking(
    config: FederationWorkerConfig,
) -> Result<(), FederationWorkerError> {
    let runtime = tokio::runtime::Runtime::new()
        .map_err(FederationWorkerError::Runtime)?;
    runtime.block_on(run_federation_worker(config))
}

pub async fn run_federation_worker(
    config: FederationWorkerConfig,
) -> Result<(), FederationWorkerError> {
    imp::run(config).await
}

#[derive(Debug, Error)]
pub enum FederationWorkerError {
    #[error("federation worker is only supported on Unix platforms")]
    UnsupportedPlatform,
    #[error("failed to create tokio runtime: {0}")]
    Runtime(io::Error),
    #[error("federation worker is already running for {socket}")]
    AlreadyRunning { socket: PathBuf },
    #[error("failed to prepare federation runtime directory {path}: {source}")]
    RuntimeDir { path: PathBuf, source: io::Error },
    #[error("unsafe federation socket target at {path}: {reason}")]
    UnsafeSocket { path: PathBuf, reason: String },
    #[error("failed to bind federation socket {path}: {source}")]
    Bind { path: PathBuf, source: io::Error },
    #[error("failed to set federation socket permissions on {path}: {source}")]
    SocketPermissions { path: PathBuf, source: io::Error },
    #[error("failed to accept federation IPC connection: {0}")]
    Accept(io::Error),
    #[error("failed to verify federation IPC peer credentials: {0}")]
    PeerCredentials(io::Error),
    #[error("failed to wait for federation worker signal: {0}")]
    Signal(io::Error),
}

fn parse_federation_worker_args(
    args: impl IntoIterator<Item = String>,
) -> Result<FederationWorkerConfig, String> {
    let mut config = FederationWorkerConfig::default();
    let mut run_root_override = false;
    let mut socket_override = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sase-home" | "-H" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a directory path")
                })?;
                config.sase_home = PathBuf::from(value);
                if !run_root_override {
                    let host_identity = crate::daemon::host_identity_from_env();
                    config.run_root = crate::daemon::default_run_root(
                        &config.sase_home,
                        &host_identity,
                    );
                    if !socket_override {
                        config.socket_path =
                            default_federation_socket_path(&config.run_root);
                    }
                }
            }
            "--run-root" | "-R" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a directory path")
                })?;
                config.run_root = PathBuf::from(value);
                run_root_override = true;
                if !socket_override {
                    config.socket_path =
                        default_federation_socket_path(&config.run_root);
                }
            }
            "--socket" | "-S" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a socket path"))?;
                config.socket_path = PathBuf::from(value);
                socket_override = true;
            }
            "--idle-timeout-seconds" | "-I" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a seconds value"))?;
                let seconds = value.parse::<f64>().map_err(|err| {
                    format!("invalid {arg} value {value:?}: {err}")
                })?;
                if seconds <= 0.0 {
                    return Err(format!(
                        "{arg} requires a positive seconds value"
                    ));
                }
                config.idle_timeout = Duration::from_secs_f64(seconds);
            }
            "--max-frame-bytes" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a byte count"))?;
                config.max_frame_bytes =
                    value.parse::<usize>().map_err(|err| {
                        format!("invalid {arg} value {value:?}: {err}")
                    })?;
                if config.max_frame_bytes < 128 {
                    return Err(format!("{arg} requires at least 128 bytes"));
                }
            }
            "--help" | "-h" => {
                println!(
                    "Usage: sase_federation_worker [--sase-home|-H DIR] [--run-root|-R DIR] [--socket|-S PATH] [--idle-timeout-seconds|-I SECONDS] [--max-frame-bytes BYTES]"
                );
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(config)
}

#[derive(Debug, Clone, Serialize)]
pub struct FederationErrorWire {
    pub schema_version: u32,
    pub code: String,
    pub message: String,
    pub target: Option<String>,
    pub details: Option<Box<JsonValue>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FederationIpcRequestEnvelopeWire {
    pub schema_version: u32,
    pub request_id: String,
    pub deadline_unix_ms: Option<u64>,
    pub operation: FederationIpcRequestWire,
}

#[derive(Debug, Clone, Serialize)]
pub struct FederationIpcResponseEnvelopeWire {
    pub schema_version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<JsonValue>,
    pub error: Option<FederationErrorWire>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FederationIpcRequestWire {
    Health,
    Capabilities,
    ReplaceConfig {
        hosts: Vec<FederationHostConfigWire>,
    },
    Summary {
        #[serde(default)]
        cache_only: bool,
    },
    Catalog {
        query: FleetCatalogQueryWire,
        #[serde(default)]
        cache_only: bool,
    },
    CatalogHosts {
        queries: Vec<FederationHostCatalogQueryWire>,
        #[serde(default)]
        cache_only: bool,
    },
    FollowedBatch {
        request: FleetLogicalBatchRequestWire,
        #[serde(default)]
        cache_only: bool,
    },
    Detail {
        request: FleetDetailRequestWire,
        #[serde(default)]
        cache_only: bool,
    },
    ContentRange {
        request: FleetContentReadRequestWire,
        #[serde(default)]
        cache_only: bool,
    },
    ProjectEligibility {
        request: FleetProjectEligibilityRequestWire,
        #[serde(default)]
        cache_only: bool,
    },
    Attention {
        request: FleetLogicalBatchRequestWire,
        #[serde(default)]
        cache_only: bool,
    },
    AttentionInventory {
        request: FleetAttentionInventoryRequestWire,
        #[serde(default)]
        cache_only: bool,
    },
    Launch {
        target: String,
        request: Box<FleetLaunchRequestWire>,
    },
    Mutate {
        target: String,
        request: Box<FleetMutationRequestWire>,
    },
    ResolveAttention {
        target: String,
        request: Box<FleetAttentionRequestWire>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FederationHostConfigWire {
    pub schema_version: u32,
    pub alias: Option<String>,
    pub plan: ConnectionPlanWire,
    pub bearer_token: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FederationHostCatalogQueryWire {
    pub schema_version: u32,
    pub installation_id: String,
    pub query: FleetCatalogQueryWire,
}

#[derive(Debug, Clone, Serialize)]
pub struct FederationHealthWire {
    pub schema_version: u32,
    pub status: String,
    pub service: String,
    pub version: String,
    pub configured_hosts: usize,
    pub socket_path: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FederationReadResponseWire {
    pub schema_version: u32,
    pub operation: String,
    pub hosts: Vec<FederationHostResultWire>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FederationHostResultWire {
    pub schema_version: u32,
    pub alias: Option<String>,
    pub provider_ref: String,
    pub installation_id: String,
    pub endpoint: String,
    pub status: String,
    pub cached: bool,
    pub age_seconds: Option<f64>,
    pub payload: Option<JsonValue>,
    pub error: Option<FederationErrorWire>,
}

#[cfg(unix)]
mod imp {
    use std::{
        future::Future,
        os::unix::{
            fs::{FileTypeExt, PermissionsExt},
            io::AsRawFd,
        },
        sync::atomic::{AtomicUsize, Ordering},
    };

    use reqwest::{Method, StatusCode};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{UnixListener, UnixStream},
        task::JoinSet,
        time,
    };

    use super::*;

    pub async fn run(
        config: FederationWorkerConfig,
    ) -> Result<(), FederationWorkerError> {
        let prepared = prepare_listener(&config).await?;
        let state = Arc::new(FederationWorkerState::new(config.clone()));
        let active_connections = Arc::new(AtomicUsize::new(0));
        loop {
            let accept =
                time::timeout(config.idle_timeout, prepared.listener.accept());
            tokio::select! {
                _ = state.shutdown.notified() => break,
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(FederationWorkerError::Signal)?;
                    break;
                }
                accepted = accept => {
                    match accepted {
                        Ok(Ok((mut stream, _addr))) => {
                            if !peer_is_same_user(&stream)
                                .map_err(FederationWorkerError::PeerCredentials)?
                            {
                                let response = error_response(
                                    "<peer>",
                                    federation_error(
                                        "permission_denied",
                                        "IPC peer is not owned by the current user",
                                        Some("peer"),
                                    ),
                                );
                                let _ = write_response(
                                    &mut stream,
                                    &response,
                                    config.max_frame_bytes,
                                )
                                .await;
                                continue;
                            }
                            let Ok(permit) = state.connection_permits.clone().try_acquire_owned() else {
                                let response = error_response(
                                    "<connection>",
                                    federation_error(
                                        "rate_limited",
                                        "too many active IPC connections",
                                        Some("connection"),
                                    ),
                                );
                                let _ = write_response(
                                    &mut stream,
                                    &response,
                                    config.max_frame_bytes,
                                )
                                .await;
                                continue;
                            };
                            active_connections.fetch_add(1, Ordering::SeqCst);
                            tokio::spawn(handle_connection(
                                stream,
                                state.clone(),
                                config.max_frame_bytes,
                                active_connections.clone(),
                                permit,
                            ));
                        }
                        Ok(Err(error)) => return Err(FederationWorkerError::Accept(error)),
                        Err(_) => {
                            if active_connections.load(Ordering::SeqCst) == 0 {
                                break;
                            }
                        }
                    }
                }
            }
        }
        drop(prepared);
        Ok(())
    }

    struct PreparedListener {
        listener: UnixListener,
        socket_path: PathBuf,
        _lock_file: File,
    }

    impl Drop for PreparedListener {
        fn drop(&mut self) {
            if let Ok(metadata) = fs::symlink_metadata(&self.socket_path) {
                if metadata.file_type().is_socket() {
                    let _ = fs::remove_file(&self.socket_path);
                }
            }
        }
    }

    async fn prepare_listener(
        config: &FederationWorkerConfig,
    ) -> Result<PreparedListener, FederationWorkerError> {
        secure_dir(&config.run_root)?;
        let lock_path = config.run_root.join(FEDERATION_LOCK_NAME);
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| FederationWorkerError::RuntimeDir {
                path: lock_path.clone(),
                source,
            })?;
        lock_file.try_lock_exclusive().map_err(|source| {
            if source.kind() == io::ErrorKind::WouldBlock {
                FederationWorkerError::AlreadyRunning {
                    socket: config.socket_path.clone(),
                }
            } else {
                FederationWorkerError::RuntimeDir {
                    path: lock_path.clone(),
                    source,
                }
            }
        })?;

        recover_socket_target(&config.socket_path).await?;
        let listener =
            UnixListener::bind(&config.socket_path).map_err(|source| {
                FederationWorkerError::Bind {
                    path: config.socket_path.clone(),
                    source,
                }
            })?;
        fs::set_permissions(
            &config.socket_path,
            fs::Permissions::from_mode(0o600),
        )
        .map_err(|source| {
            FederationWorkerError::SocketPermissions {
                path: config.socket_path.clone(),
                source,
            }
        })?;
        Ok(PreparedListener {
            listener,
            socket_path: config.socket_path.clone(),
            _lock_file: lock_file,
        })
    }

    fn secure_dir(path: &Path) -> Result<(), FederationWorkerError> {
        fs::create_dir_all(path).map_err(|source| {
            FederationWorkerError::RuntimeDir {
                path: path.to_path_buf(),
                source,
            }
        })?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(
            |source| FederationWorkerError::RuntimeDir {
                path: path.to_path_buf(),
                source,
            },
        )
    }

    async fn recover_socket_target(
        path: &Path,
    ) -> Result<(), FederationWorkerError> {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return Ok(());
        };
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(FederationWorkerError::UnsafeSocket {
                path: path.to_path_buf(),
                reason: "target is a symlink".to_string(),
            });
        }
        if !file_type.is_socket() {
            return Err(FederationWorkerError::UnsafeSocket {
                path: path.to_path_buf(),
                reason: "target is not a socket".to_string(),
            });
        }
        match UnixStream::connect(path).await {
            Ok(_) => Err(FederationWorkerError::AlreadyRunning {
                socket: path.to_path_buf(),
            }),
            Err(_) => fs::remove_file(path).map_err(|source| {
                FederationWorkerError::RuntimeDir {
                    path: path.to_path_buf(),
                    source,
                }
            }),
        }
    }

    async fn handle_connection(
        mut stream: UnixStream,
        state: Arc<FederationWorkerState>,
        max_frame_bytes: usize,
        active_connections: Arc<AtomicUsize>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        loop {
            let request = match read_request(&mut stream, max_frame_bytes).await
            {
                Ok(Some(request)) => request,
                Ok(None) => break,
                Err(error) => {
                    let response = error_response("<frame>", error);
                    let _ =
                        write_response(&mut stream, &response, max_frame_bytes)
                            .await;
                    break;
                }
            };
            let response = handle_request(state.clone(), request).await;
            let shutdown = matches!(
                response
                    .result
                    .as_ref()
                    .and_then(|value| value.get("shutdown")),
                Some(JsonValue::Bool(true))
            );
            if write_response(&mut stream, &response, max_frame_bytes)
                .await
                .is_err()
            {
                break;
            }
            if shutdown {
                state.shutdown.notify_waiters();
                break;
            }
        }
        active_connections.fetch_sub(1, Ordering::SeqCst);
    }

    async fn read_request(
        stream: &mut UnixStream,
        max_frame_bytes: usize,
    ) -> Result<Option<FederationIpcRequestEnvelopeWire>, FederationErrorWire>
    {
        let mut len_bytes = [0_u8; 4];
        match stream.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(error) => {
                return Err(federation_error(
                    "unavailable",
                    &format!("failed to read IPC frame length: {error}"),
                    Some("frame"),
                ))
            }
        }
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len == 0 {
            return Err(federation_error(
                "invalid_request",
                "IPC frame length must be non-zero",
                Some("frame"),
            ));
        }
        if len > max_frame_bytes {
            return Err(federation_error(
                "payload_too_large",
                "IPC frame exceeds the configured frame limit",
                Some("frame"),
            ));
        }
        let mut bytes = vec![0_u8; len];
        stream.read_exact(&mut bytes).await.map_err(|error| {
            federation_error(
                "unavailable",
                &format!("failed to read IPC frame body: {error}"),
                Some("frame"),
            )
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|error| {
                federation_error(
                    "invalid_request",
                    &format!(
                        "IPC frame is not a valid request envelope: {error}"
                    ),
                    Some("frame"),
                )
            })
            .map(Some)
    }

    async fn write_response(
        stream: &mut UnixStream,
        response: &FederationIpcResponseEnvelopeWire,
        max_frame_bytes: usize,
    ) -> Result<(), FederationErrorWire> {
        let bytes = serde_json::to_vec(response).map_err(|error| {
            federation_error(
                "internal",
                &format!("failed to serialize IPC response: {error}"),
                Some("response"),
            )
        })?;
        if bytes.len() > max_frame_bytes {
            return Err(federation_error(
                "payload_too_large",
                "IPC response exceeds the configured frame limit",
                Some("response"),
            ));
        }
        let len = u32::try_from(bytes.len()).map_err(|_| {
            federation_error(
                "payload_too_large",
                "IPC response exceeds u32 frame length",
                Some("response"),
            )
        })?;
        stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|error| {
                federation_error(
                    "unavailable",
                    &format!("failed to write IPC response length: {error}"),
                    Some("response"),
                )
            })?;
        stream.write_all(&bytes).await.map_err(|error| {
            federation_error(
                "unavailable",
                &format!("failed to write IPC response body: {error}"),
                Some("response"),
            )
        })?;
        stream.shutdown().await.map_err(|error| {
            federation_error(
                "unavailable",
                &format!("failed to flush IPC response: {error}"),
                Some("response"),
            )
        })
    }

    async fn handle_request(
        state: Arc<FederationWorkerState>,
        request: FederationIpcRequestEnvelopeWire,
    ) -> FederationIpcResponseEnvelopeWire {
        if request.schema_version != FEDERATION_IPC_SCHEMA_VERSION {
            return error_response(
                &request.request_id,
                federation_error(
                    "unsupported_version",
                    "unsupported federation IPC schema version",
                    Some("schema_version"),
                ),
            );
        }
        if request.request_id.trim().is_empty()
            || request.request_id.len() > 128
            || request.request_id.chars().any(char::is_control)
        {
            return error_response(
                &request.request_id,
                federation_error(
                    "invalid_request",
                    "request_id must be a short non-empty string",
                    Some("request_id"),
                ),
            );
        }
        let deadline = RequestDeadline {
            unix_ms: request.deadline_unix_ms,
        };
        if let Some(error) = deadline.expired_error() {
            return error_response(&request.request_id, error);
        }
        let request_id = request.request_id.clone();
        let outer_deadline = RequestDeadline {
            unix_ms: deadline
                .unix_ms
                .map(|ms| ms + OUTER_DEADLINE_GRACE.as_millis() as u64),
        };
        match with_deadline(
            outer_deadline,
            handle_operation(state, request.operation, deadline),
        )
        .await
        {
            Ok(value) => success_response(&request_id, value),
            Err(error) => error_response(&request_id, error),
        }
    }

    async fn handle_operation(
        state: Arc<FederationWorkerState>,
        operation: FederationIpcRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        match operation {
            FederationIpcRequestWire::Health => to_json(FederationHealthWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                status: "ok".to_string(),
                service: FEDERATION_WORKER_SERVICE.to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                configured_hosts: state.hosts.read().await.len(),
                socket_path: state.config.socket_path.display().to_string(),
                capabilities: federation_capabilities(),
            }),
            FederationIpcRequestWire::Capabilities => {
                to_json(serde_json::json!({
                    "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                    "capabilities": federation_capabilities(),
                }))
            }
            FederationIpcRequestWire::ReplaceConfig { hosts } => {
                state.replace_config(hosts).await
            }
            FederationIpcRequestWire::Summary { cache_only } => {
                state
                    .read_all(ReadOperation::Summary, cache_only, deadline)
                    .await
            }
            FederationIpcRequestWire::Catalog { query, cache_only } => {
                state
                    .read_all(
                        ReadOperation::Catalog(query),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::CatalogHosts {
                queries,
                cache_only,
            } => {
                state
                    .read_catalog_hosts(queries, cache_only, deadline)
                    .await
            }
            FederationIpcRequestWire::FollowedBatch {
                request,
                cache_only,
            } => {
                state
                    .read_all(
                        ReadOperation::FollowedBatch(request),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::Detail {
                request,
                cache_only,
            } => {
                state
                    .read_all(
                        ReadOperation::Detail(request),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::ContentRange {
                request,
                cache_only,
            } => {
                state
                    .read_all(
                        ReadOperation::ContentRange(request),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::ProjectEligibility {
                request,
                cache_only,
            } => {
                state
                    .read_all(
                        ReadOperation::ProjectEligibility(request),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::Attention {
                request,
                cache_only,
            } => {
                state
                    .read_all(
                        ReadOperation::Attention(request),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::AttentionInventory {
                request,
                cache_only,
            } => {
                state
                    .read_all(
                        ReadOperation::AttentionInventory(request),
                        cache_only,
                        deadline,
                    )
                    .await
            }
            FederationIpcRequestWire::Launch { target, request } => {
                state.launch_one(target, *request, deadline).await
            }
            FederationIpcRequestWire::Mutate { target, request } => {
                state.mutate_one(target, *request, deadline).await
            }
            FederationIpcRequestWire::ResolveAttention { target, request } => {
                state
                    .resolve_attention_one(target, *request, deadline)
                    .await
            }
            FederationIpcRequestWire::Shutdown => to_json(serde_json::json!({
                "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                "shutdown": true,
            })),
        }
    }

    #[derive(Clone, Copy)]
    struct RequestDeadline {
        unix_ms: Option<u64>,
    }

    impl RequestDeadline {
        fn remaining(self) -> Option<Result<Duration, FederationErrorWire>> {
            let deadline = self.unix_ms?;
            let now = unix_now_ms();
            if deadline <= now {
                return Some(Err(federation_error(
                    "deadline",
                    "request deadline has expired",
                    Some("deadline_unix_ms"),
                )));
            }
            Some(Ok(Duration::from_millis(deadline - now)))
        }

        fn expired_error(self) -> Option<FederationErrorWire> {
            self.remaining()?.err()
        }
    }

    async fn with_deadline<T>(
        deadline: RequestDeadline,
        future: impl Future<Output = Result<T, FederationErrorWire>>,
    ) -> Result<T, FederationErrorWire> {
        match deadline.remaining() {
            Some(Ok(remaining)) => match time::timeout(remaining, future).await
            {
                Ok(result) => result,
                Err(_) => Err(federation_error(
                    "deadline",
                    "request deadline elapsed",
                    Some("deadline_unix_ms"),
                )),
            },
            Some(Err(error)) => Err(error),
            None => future.await,
        }
    }

    struct FederationWorkerState {
        config: FederationWorkerConfig,
        hosts: RwLock<BTreeMap<String, Arc<RemoteHost>>>,
        cache: Arc<AsyncMutex<FederationCache>>,
        connection_permits: Arc<Semaphore>,
        in_flight: Arc<Semaphore>,
        shutdown: Notify,
    }

    impl FederationWorkerState {
        fn new(config: FederationWorkerConfig) -> Self {
            let cache = FederationCache::load(
                cache_path(&config.sase_home),
                config.cache_entry_limit,
                config.cache_byte_limit,
            );
            Self {
                connection_permits: Arc::new(Semaphore::new(
                    config.max_connections,
                )),
                in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
                config,
                hosts: RwLock::new(BTreeMap::new()),
                cache: Arc::new(AsyncMutex::new(cache)),
                shutdown: Notify::new(),
            }
        }

        async fn replace_config(
            &self,
            configs: Vec<FederationHostConfigWire>,
        ) -> Result<JsonValue, FederationErrorWire> {
            let mut hosts = BTreeMap::new();
            let mut results = Vec::new();
            for config in configs {
                let result = match validate_host_config(config) {
                    Ok(validated) => {
                        let installation_id =
                            validated.plan.pinned_installation_id.clone();
                        match hosts.entry(installation_id.clone()) {
                            std::collections::btree_map::Entry::Occupied(_) => {
                                FederationHostResultWire {
                                    schema_version:
                                        FEDERATION_IPC_SCHEMA_VERSION,
                                    alias: validated.alias,
                                    provider_ref: validated.plan.provider_ref,
                                    installation_id,
                                    endpoint: validated.plan.endpoint,
                                    status: "invalid".to_string(),
                                    cached: false,
                                    age_seconds: None,
                                    payload: None,
                                    error: Some(federation_error(
                                        "invalid_request",
                                        "duplicate pinned installation ID",
                                        Some("hosts"),
                                    )),
                                }
                            }
                            std::collections::btree_map::Entry::Vacant(
                                entry,
                            ) => {
                                match RemoteHost::new(
                                    validated.clone(),
                                    &self.config.sase_home,
                                ) {
                                    Ok(host) => {
                                        let host = Arc::new(host);
                                        let response =
                                            host.empty_result("configured");
                                        entry.insert(host);
                                        response
                                    }
                                    Err(error) => FederationHostResultWire {
                                        schema_version:
                                            FEDERATION_IPC_SCHEMA_VERSION,
                                        alias: validated.alias,
                                        provider_ref: validated
                                            .plan
                                            .provider_ref,
                                        installation_id,
                                        endpoint: validated.plan.endpoint,
                                        status: status_from_error(&error),
                                        cached: false,
                                        age_seconds: None,
                                        payload: None,
                                        error: Some(error),
                                    },
                                }
                            }
                        }
                    }
                    Err(error) => FederationHostResultWire {
                        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                        alias: None,
                        provider_ref: "".to_string(),
                        installation_id: "".to_string(),
                        endpoint: "".to_string(),
                        status: "invalid".to_string(),
                        cached: false,
                        age_seconds: None,
                        payload: None,
                        error: Some(error),
                    },
                };
                results.push(result);
            }
            let configured_hosts = hosts.len();
            *self.hosts.write().await = hosts;
            to_json(serde_json::json!({
                "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                "configured_hosts": configured_hosts,
                "hosts": results,
            }))
        }

        async fn read_all(
            &self,
            operation: ReadOperation,
            cache_only: bool,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            let hosts = self
                .hosts
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            if hosts.is_empty() {
                return to_json(FederationReadResponseWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    operation: operation.name().to_string(),
                    hosts: Vec::new(),
                });
            }

            let mut tasks = JoinSet::new();
            for host in hosts {
                let op = operation.clone();
                let cache = self.cache_path_limits_owned();
                let cache_ref = self.cache.clone();
                let global = self.in_flight.clone();
                tasks.spawn(async move {
                    read_one_host(
                        host, op, cache_only, deadline, cache_ref, cache,
                        global,
                    )
                    .await
                });
            }
            let mut results = Vec::new();
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok(result) => results.push(result),
                    Err(_) => results.push(FederationHostResultWire {
                        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                        alias: None,
                        provider_ref: "".to_string(),
                        installation_id: "".to_string(),
                        endpoint: "".to_string(),
                        status: "unavailable".to_string(),
                        cached: false,
                        age_seconds: None,
                        payload: None,
                        error: Some(federation_error(
                            "internal",
                            "host worker task failed",
                            Some("host"),
                        )),
                    }),
                }
            }
            results.sort_by(|left, right| {
                left.installation_id
                    .cmp(&right.installation_id)
                    .then_with(|| left.alias.cmp(&right.alias))
            });
            to_json(FederationReadResponseWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                operation: operation.name().to_string(),
                hosts: results,
            })
        }

        async fn read_catalog_hosts(
            &self,
            queries: Vec<FederationHostCatalogQueryWire>,
            cache_only: bool,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            let mut tasks = JoinSet::new();
            let mut results = Vec::new();
            {
                let hosts = self.hosts.read().await;
                for host_query in queries {
                    if host_query.schema_version
                        != FEDERATION_IPC_SCHEMA_VERSION
                    {
                        results.push(FederationHostResultWire {
                            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                            alias: None,
                            provider_ref: "".to_string(),
                            installation_id: host_query.installation_id,
                            endpoint: "".to_string(),
                            status: "invalid".to_string(),
                            cached: false,
                            age_seconds: None,
                            payload: None,
                            error: Some(federation_error(
                                "unsupported_version",
                                "unsupported per-host catalog query schema version",
                                Some("queries.schema_version"),
                            )),
                        });
                        continue;
                    }
                    let installation_id =
                        host_query.installation_id.trim().to_string();
                    let Some(host) = hosts.get(&installation_id).cloned()
                    else {
                        results.push(FederationHostResultWire {
                            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                            alias: None,
                            provider_ref: "".to_string(),
                            installation_id,
                            endpoint: "".to_string(),
                            status: "not_found".to_string(),
                            cached: false,
                            age_seconds: None,
                            payload: None,
                            error: Some(federation_error(
                                "not_found",
                                "no configured dispatch host matches the per-host catalog query",
                                Some("queries.installation_id"),
                            )),
                        });
                        continue;
                    };
                    let cache = self.cache_path_limits_owned();
                    let cache_ref = self.cache.clone();
                    let global = self.in_flight.clone();
                    tasks.spawn(async move {
                        read_one_host(
                            host,
                            ReadOperation::Catalog(host_query.query),
                            cache_only,
                            deadline,
                            cache_ref,
                            cache,
                            global,
                        )
                        .await
                    });
                }
            }
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok(result) => results.push(result),
                    Err(_) => results.push(FederationHostResultWire {
                        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                        alias: None,
                        provider_ref: "".to_string(),
                        installation_id: "".to_string(),
                        endpoint: "".to_string(),
                        status: "unavailable".to_string(),
                        cached: false,
                        age_seconds: None,
                        payload: None,
                        error: Some(federation_error(
                            "internal",
                            "host worker task failed",
                            Some("host"),
                        )),
                    }),
                }
            }
            results.sort_by(|left, right| {
                left.installation_id
                    .cmp(&right.installation_id)
                    .then_with(|| left.alias.cmp(&right.alias))
            });
            to_json(FederationReadResponseWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                operation: "catalog".to_string(),
                hosts: results,
            })
        }

        async fn launch_one(
            &self,
            target: String,
            request: FleetLaunchRequestWire,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            let host = self.resolve_launch_target(&target).await?;
            if request.target_installation_id
                != host.plan.pinned_installation_id
            {
                return Err(federation_error(
                    "quarantined",
                    "launch request target_installation_id does not match the configured host pin",
                    Some("target_installation_id"),
                ));
            }
            let payload = with_deadline(deadline, async {
                let _global =
                    self.in_flight.clone().acquire_owned().await.map_err(
                        |_| {
                            federation_error(
                                "unavailable",
                                "global request limiter is closed",
                                Some("concurrency"),
                            )
                        },
                    )?;
                let _host =
                    host.permits.clone().acquire_owned().await.map_err(
                        |_| {
                            federation_error(
                                "unavailable",
                                "host request limiter is closed",
                                Some("concurrency"),
                            )
                        },
                    )?;
                host.launch_remote(&request, deadline).await
            })
            .await?;
            to_json(FederationReadResponseWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                operation: "launch".to_string(),
                hosts: vec![
                    host.payload_result("ok", false, None, payload, None)
                ],
            })
        }

        async fn mutate_one(
            &self,
            target: String,
            request: FleetMutationRequestWire,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            let host = self.resolve_launch_target(&target).await?;
            if request.target_installation_id
                != host.plan.pinned_installation_id
            {
                return Err(federation_error(
                    "quarantined",
                    "mutation request target_installation_id does not match the configured host pin",
                    Some("target_installation_id"),
                ));
            }
            let payload = with_deadline(deadline, async {
                let _global =
                    self.in_flight.clone().acquire_owned().await.map_err(
                        |_| {
                            federation_error(
                                "unavailable",
                                "global request limiter is closed",
                                Some("concurrency"),
                            )
                        },
                    )?;
                let _host =
                    host.permits.clone().acquire_owned().await.map_err(
                        |_| {
                            federation_error(
                                "unavailable",
                                "host request limiter is closed",
                                Some("concurrency"),
                            )
                        },
                    )?;
                host.mutate_remote(&request, deadline).await
            })
            .await?;
            to_json(FederationReadResponseWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                operation: "mutate".to_string(),
                hosts: vec![
                    host.payload_result("ok", false, None, payload, None)
                ],
            })
        }

        async fn resolve_attention_one(
            &self,
            target: String,
            request: FleetAttentionRequestWire,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            let host = self.resolve_launch_target(&target).await?;
            if request.target_installation_id
                != host.plan.pinned_installation_id
            {
                return Err(federation_error(
                    "quarantined",
                    "attention resolve request target_installation_id does not match the configured host pin",
                    Some("target_installation_id"),
                ));
            }
            let payload = with_deadline(deadline, async {
                let _global =
                    self.in_flight.clone().acquire_owned().await.map_err(
                        |_| {
                            federation_error(
                                "unavailable",
                                "global request limiter is closed",
                                Some("concurrency"),
                            )
                        },
                    )?;
                let _host =
                    host.permits.clone().acquire_owned().await.map_err(
                        |_| {
                            federation_error(
                                "unavailable",
                                "host request limiter is closed",
                                Some("concurrency"),
                            )
                        },
                    )?;
                host.resolve_attention_remote(&request, deadline).await
            })
            .await?;
            to_json(FederationReadResponseWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                operation: "resolve_attention".to_string(),
                hosts: vec![
                    host.payload_result("ok", false, None, payload, None)
                ],
            })
        }

        async fn resolve_launch_target(
            &self,
            target: &str,
        ) -> Result<Arc<RemoteHost>, FederationErrorWire> {
            let target = target.trim();
            if target.is_empty() {
                return Err(federation_error(
                    "invalid_request",
                    "launch target must be non-empty",
                    Some("target"),
                ));
            }
            let hosts = self.hosts.read().await;
            if let Some(host) = hosts.get(target) {
                return Ok(host.clone());
            }
            let matches = hosts
                .values()
                .filter(|host| host.alias.as_deref() == Some(target))
                .cloned()
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [host] => Ok(host.clone()),
                [] => Err(federation_error(
                    "not_found",
                    "no configured dispatch host matches the launch target",
                    Some("target"),
                )),
                _ => Err(federation_error(
                    "invalid_request",
                    "launch target alias matches multiple configured hosts",
                    Some("target"),
                )),
            }
        }

        fn cache_path_limits_owned(&self) -> OwnedCachePathLimits {
            OwnedCachePathLimits {
                path: cache_path(&self.config.sase_home),
            }
        }
    }

    #[derive(Clone)]
    struct OwnedCachePathLimits {
        path: PathBuf,
    }

    #[derive(Clone)]
    struct ValidatedHostConfig {
        alias: Option<String>,
        plan: ConnectionPlanWire,
        bearer_token: String,
    }

    fn validate_host_config(
        config: FederationHostConfigWire,
    ) -> Result<ValidatedHostConfig, FederationErrorWire> {
        if config.schema_version != FEDERATION_IPC_SCHEMA_VERSION {
            return Err(federation_error(
                "unsupported_version",
                "unsupported federation host schema version",
                Some("hosts.schema_version"),
            ));
        }
        let alias = config
            .alias
            .map(|alias| alias.trim().to_string())
            .filter(|alias| !alias.is_empty());
        if alias.as_ref().is_some_and(|alias| {
            alias.len() > 128 || alias.chars().any(char::is_control)
        }) {
            return Err(federation_error(
                "invalid_request",
                "host alias must be a short printable string",
                Some("hosts.alias"),
            ));
        }
        if config.bearer_token.trim().is_empty()
            || config.bearer_token.chars().any(char::is_control)
        {
            return Err(federation_error(
                "missing_credential",
                "host credential is missing or invalid",
                Some("hosts.credential_ref"),
            ));
        }
        let plan = validate_connection_plan(&config.plan).map_err(|error| {
            federation_error(
                "invalid_request",
                &format!("invalid connection plan: {error}"),
                Some("hosts.plan"),
            )
        })?;
        Ok(ValidatedHostConfig {
            alias,
            plan,
            bearer_token: config.bearer_token,
        })
    }

    #[derive(Clone)]
    struct RemoteHost {
        alias: Option<String>,
        plan: ConnectionPlanWire,
        bearer_token: Arc<str>,
        client: reqwest::Client,
        runtime: Arc<AsyncMutex<RemoteHostRuntime>>,
        permits: Arc<Semaphore>,
    }

    #[derive(Debug, Default)]
    struct RemoteHostRuntime {
        verified: bool,
        quarantine: Option<FederationErrorWire>,
    }

    impl RemoteHost {
        fn new(
            config: ValidatedHostConfig,
            sase_home: &Path,
        ) -> Result<Self, FederationErrorWire> {
            let client = build_http_client(&config.plan, sase_home)?;
            Ok(Self {
                alias: config.alias,
                plan: config.plan,
                bearer_token: Arc::from(config.bearer_token),
                client,
                runtime: Arc::new(
                    AsyncMutex::new(RemoteHostRuntime::default()),
                ),
                permits: Arc::new(Semaphore::new(DEFAULT_PER_HOST_IN_FLIGHT)),
            })
        }

        fn empty_result(&self, status: &str) -> FederationHostResultWire {
            FederationHostResultWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                alias: self.alias.clone(),
                provider_ref: self.plan.provider_ref.clone(),
                installation_id: self.plan.pinned_installation_id.clone(),
                endpoint: self.plan.endpoint.clone(),
                status: status.to_string(),
                cached: false,
                age_seconds: None,
                payload: None,
                error: None,
            }
        }

        async fn ensure_hello(
            &self,
            deadline: RequestDeadline,
        ) -> Result<(), FederationErrorWire> {
            {
                let runtime = self.runtime.lock().await;
                if let Some(error) = runtime.quarantine.clone() {
                    return Err(error);
                }
                if runtime.verified {
                    return Ok(());
                }
            }
            let value = self
                .http_json(Method::GET, "/hello", None, deadline)
                .await?;
            let hello: FleetHelloResponseWire = serde_json::from_value(value)
                .map_err(|error| {
                federation_error(
                    "invalid_response",
                    &format!("hello response had invalid shape: {error}"),
                    Some("hello"),
                )
            })?;
            if hello.installation.installation_id
                != self.plan.pinned_installation_id
            {
                let error = federation_error(
                    "quarantined",
                    "authenticated hello reported a different installation identity",
                    Some("installation_id"),
                );
                self.runtime.lock().await.quarantine = Some(error.clone());
                return Err(error);
            }
            self.runtime.lock().await.verified = true;
            Ok(())
        }

        async fn read_remote(
            &self,
            operation: &ReadOperation,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            self.ensure_hello(deadline).await?;
            match operation {
                ReadOperation::Summary => {
                    self.http_json(Method::GET, "/summary", None, deadline)
                        .await
                }
                ReadOperation::Catalog(query) => {
                    self.http_json(
                        Method::POST,
                        "/catalog",
                        Some(to_json(query)?),
                        deadline,
                    )
                    .await
                }
                ReadOperation::FollowedBatch(request) => {
                    self.http_json(
                        Method::POST,
                        "/batch",
                        Some(to_json(request)?),
                        deadline,
                    )
                    .await
                }
                ReadOperation::Detail(request) => {
                    self.http_json(
                        Method::POST,
                        "/detail",
                        Some(to_json(request)?),
                        deadline,
                    )
                    .await
                }
                ReadOperation::ContentRange(request) => {
                    self.http_json(
                        Method::POST,
                        "/content",
                        Some(to_json(request)?),
                        deadline,
                    )
                    .await
                }
                ReadOperation::ProjectEligibility(request) => {
                    self.http_json(
                        Method::POST,
                        "/projects/eligibility",
                        Some(to_json(request)?),
                        deadline,
                    )
                    .await
                }
                ReadOperation::Attention(request) => {
                    self.http_json(
                        Method::POST,
                        "/attention",
                        Some(to_json(request)?),
                        deadline,
                    )
                    .await
                }
                ReadOperation::AttentionInventory(request) => {
                    self.http_json(
                        Method::POST,
                        "/attention/inventory",
                        Some(to_json(request)?),
                        deadline,
                    )
                    .await
                }
            }
        }

        async fn launch_remote(
            &self,
            request: &FleetLaunchRequestWire,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            self.ensure_hello(deadline).await?;
            self.http_json(
                Method::POST,
                "/launch",
                Some(to_json(request)?),
                deadline,
            )
            .await
        }

        async fn mutate_remote(
            &self,
            request: &FleetMutationRequestWire,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            self.ensure_hello(deadline).await?;
            self.http_json(
                Method::POST,
                "/mutate",
                Some(to_json(request)?),
                deadline,
            )
            .await
        }

        async fn resolve_attention_remote(
            &self,
            request: &FleetAttentionRequestWire,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            self.ensure_hello(deadline).await?;
            self.http_json(
                Method::POST,
                "/attention/resolve",
                Some(to_json(request)?),
                deadline,
            )
            .await
        }

        async fn http_json(
            &self,
            method: Method,
            path: &str,
            body: Option<JsonValue>,
            deadline: RequestDeadline,
        ) -> Result<JsonValue, FederationErrorWire> {
            let url = fleet_url(&self.plan.endpoint, path);
            let mut request = self
                .client
                .request(method, url)
                .bearer_auth(self.bearer_token.as_ref())
                .header(
                    FLEET_PROTOCOL_VERSIONS_HEADER,
                    FLEET_PROTOCOL_VERSION.to_string(),
                );
            if let Some(body) = body {
                request = request.json(&body);
            }
            let response = with_deadline(deadline, async {
                request.send().await.map_err(|error| {
                    federation_error(
                        "unavailable",
                        &format!("fleet request failed: {error}"),
                        Some("endpoint"),
                    )
                })
            })
            .await?;
            let status = response.status();
            let value = with_deadline(deadline, async {
                response.json::<JsonValue>().await.map_err(|error| {
                    federation_error(
                        "invalid_response",
                        &format!("fleet response was not JSON: {error}"),
                        Some("response"),
                    )
                })
            })
            .await?;
            if !status.is_success() {
                return Err(fleet_http_error(status, value));
            }
            Ok(value)
        }
    }

    fn build_http_client(
        plan: &ConnectionPlanWire,
        sase_home: &Path,
    ) -> Result<reqwest::Client, FederationErrorWire> {
        let mut builder = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(30));
        match plan.tls.mode {
            TlsTrustModeWire::SystemRoots => {}
            TlsTrustModeWire::PinnedCa => {
                let ca_ref = plan.tls.ca_ref.as_deref().ok_or_else(|| {
                    federation_error(
                        "invalid_request",
                        "pinned_ca trust mode requires ca_ref",
                        Some("hosts.plan.tls.ca_ref"),
                    )
                })?;
                let bytes = fs::read(managed_trust_ref_path(
                    sase_home, "ca", ca_ref, "pem",
                ))
                .map_err(|error| {
                    federation_error(
                        "invalid_request",
                        &format!(
                            "managed TLS CA reference {ca_ref:?} could not be resolved: {error}"
                        ),
                        Some("hosts.plan.tls.ca_ref"),
                    )
                })?;
                let certificate = reqwest::Certificate::from_pem(&bytes)
                    .map_err(|error| {
                        federation_error(
                            "invalid_request",
                            &format!(
                                "managed TLS CA reference {ca_ref:?} is not a PEM certificate: {error}"
                            ),
                            Some("hosts.plan.tls.ca_ref"),
                        )
                    })?;
                builder = builder.add_root_certificate(certificate);
            }
            TlsTrustModeWire::PinnedServerName => {
                let server_name_ref =
                    plan.tls.server_name_ref.as_deref().ok_or_else(|| {
                        federation_error(
                            "invalid_request",
                            "pinned_server_name trust mode requires server_name_ref",
                            Some("hosts.plan.tls.server_name_ref"),
                        )
                    })?;
                let pinned_name = fs::read_to_string(managed_trust_ref_path(
                    sase_home,
                    "server_name",
                    server_name_ref,
                    "txt",
                ))
                .map_err(|error| {
                    federation_error(
                        "invalid_request",
                        &format!(
                            "managed TLS server-name reference {server_name_ref:?} could not be resolved: {error}"
                        ),
                        Some("hosts.plan.tls.server_name_ref"),
                    )
                })?;
                let pinned_name = pinned_name.trim();
                if pinned_name.is_empty()
                    || pinned_name.chars().any(char::is_control)
                {
                    return Err(federation_error(
                        "invalid_request",
                        "managed TLS server-name reference resolved to an invalid name",
                        Some("hosts.plan.tls.server_name_ref"),
                    ));
                }
                let endpoint = reqwest::Url::parse(&plan.endpoint).map_err(
                    |error| {
                        federation_error(
                            "invalid_request",
                            &format!(
                                "connection endpoint is not a valid URL: {error}"
                            ),
                            Some("hosts.plan.endpoint"),
                        )
                    },
                )?;
                let Some(endpoint_host) = endpoint.host_str() else {
                    return Err(federation_error(
                        "invalid_request",
                        "connection endpoint has no host to compare with pinned server name",
                        Some("hosts.plan.endpoint"),
                    ));
                };
                if endpoint_host != pinned_name {
                    return Err(federation_error(
                        "invalid_request",
                        "connection endpoint host does not match pinned server-name reference",
                        Some("hosts.plan.tls.server_name_ref"),
                    ));
                }
            }
        }
        builder.build().map_err(|error| {
            federation_error(
                "invalid_request",
                &format!("failed to build federation HTTP client: {error}"),
                Some("hosts.plan.tls"),
            )
        })
    }

    fn managed_trust_ref_path(
        sase_home: &Path,
        kind: &str,
        reference: &str,
        extension: &str,
    ) -> PathBuf {
        sase_home
            .join("fleet")
            .join("trust")
            .join(kind)
            .join(format!("{reference}.{extension}"))
    }

    #[derive(Clone)]
    enum ReadOperation {
        Summary,
        Catalog(FleetCatalogQueryWire),
        FollowedBatch(FleetLogicalBatchRequestWire),
        Detail(FleetDetailRequestWire),
        ContentRange(FleetContentReadRequestWire),
        ProjectEligibility(FleetProjectEligibilityRequestWire),
        Attention(FleetLogicalBatchRequestWire),
        AttentionInventory(FleetAttentionInventoryRequestWire),
    }

    impl ReadOperation {
        fn name(&self) -> &'static str {
            match self {
                Self::Summary => "summary",
                Self::Catalog(_) => "catalog",
                Self::FollowedBatch(_) => "followed_batch",
                Self::Detail(_) => "detail",
                Self::ContentRange(_) => "content_range",
                Self::ProjectEligibility(_) => "project_eligibility",
                Self::Attention(_) => "attention",
                Self::AttentionInventory(_) => "attention_inventory",
            }
        }

        fn cache_key(&self) -> Result<String, FederationErrorWire> {
            let payload = match self {
                Self::Summary => serde_json::json!({}),
                Self::Catalog(query) => to_json(query)?,
                Self::FollowedBatch(request) => to_json(request)?,
                Self::Detail(request) => to_json(request)?,
                Self::ContentRange(request) => to_json(request)?,
                Self::ProjectEligibility(request) => to_json(request)?,
                Self::Attention(request) => to_json(request)?,
                Self::AttentionInventory(request) => to_json(request)?,
            };
            Ok(format!("{}:{payload}", self.name()))
        }
    }

    async fn read_one_host(
        host: Arc<RemoteHost>,
        operation: ReadOperation,
        cache_only: bool,
        deadline: RequestDeadline,
        cache: Arc<AsyncMutex<FederationCache>>,
        limits: OwnedCachePathLimits,
        global: Arc<Semaphore>,
    ) -> FederationHostResultWire {
        let cache_key = match operation.cache_key() {
            Ok(key) => key,
            Err(error) => return host.error_result(error, false, None),
        };
        if cache_only {
            return cached_or_error(&host, &cache, &cache_key, None).await;
        }
        let result = with_deadline(deadline, async {
            let _global = global.acquire_owned().await.map_err(|_| {
                federation_error(
                    "unavailable",
                    "global request limiter is closed",
                    Some("concurrency"),
                )
            })?;
            let _host =
                host.permits.clone().acquire_owned().await.map_err(|_| {
                    federation_error(
                        "unavailable",
                        "host request limiter is closed",
                        Some("concurrency"),
                    )
                })?;
            host.read_remote(&operation, deadline).await
        })
        .await;
        match result {
            Ok(payload) => {
                let entry = {
                    let mut cache = cache.lock().await;
                    let entry = cache.store(
                        &host.plan.pinned_installation_id,
                        &cache_key,
                        payload.clone(),
                    );
                    let _ = cache.persist(&limits.path);
                    entry
                };
                host.payload_result(
                    "ok",
                    false,
                    entry_age(entry.as_ref()),
                    payload,
                    None,
                )
            }
            Err(error) => {
                cached_or_error(&host, &cache, &cache_key, Some(error)).await
            }
        }
    }

    impl RemoteHost {
        fn error_result(
            &self,
            error: FederationErrorWire,
            cached: bool,
            payload: Option<JsonValue>,
        ) -> FederationHostResultWire {
            let status = status_from_error(&error);
            FederationHostResultWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                alias: self.alias.clone(),
                provider_ref: self.plan.provider_ref.clone(),
                installation_id: self.plan.pinned_installation_id.clone(),
                endpoint: self.plan.endpoint.clone(),
                status,
                cached,
                age_seconds: None,
                payload,
                error: Some(error),
            }
        }

        fn payload_result(
            &self,
            status: &str,
            cached: bool,
            age_seconds: Option<f64>,
            payload: JsonValue,
            error: Option<FederationErrorWire>,
        ) -> FederationHostResultWire {
            FederationHostResultWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                alias: self.alias.clone(),
                provider_ref: self.plan.provider_ref.clone(),
                installation_id: self.plan.pinned_installation_id.clone(),
                endpoint: self.plan.endpoint.clone(),
                status: status.to_string(),
                cached,
                age_seconds,
                payload: Some(payload),
                error,
            }
        }
    }

    async fn cached_or_error(
        host: &RemoteHost,
        cache: &Arc<AsyncMutex<FederationCache>>,
        cache_key: &str,
        error: Option<FederationErrorWire>,
    ) -> FederationHostResultWire {
        let cached = cache
            .lock()
            .await
            .get(&host.plan.pinned_installation_id, cache_key);
        match (cached, error) {
            (Some(entry), Some(error)) => host.payload_result(
                "stale",
                true,
                entry_age(Some(&entry)),
                entry.payload,
                Some(error),
            ),
            (Some(entry), None) => host.payload_result(
                "ok",
                true,
                entry_age(Some(&entry)),
                entry.payload,
                None,
            ),
            (None, Some(error)) => host.error_result(error, false, None),
            (None, None) => host.error_result(
                federation_error(
                    "stale",
                    "no cached projection is available",
                    Some("cache"),
                ),
                false,
                None,
            ),
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct FederationCacheFile {
        schema_version: u32,
        entries: Vec<FederationCacheEntry>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct FederationCacheEntry {
        host_id: String,
        key: String,
        saved_at_unix_ms: u64,
        last_used_unix_ms: u64,
        byte_size: usize,
        payload: JsonValue,
    }

    #[derive(Debug)]
    struct FederationCache {
        entries: BTreeMap<String, FederationCacheEntry>,
        entry_limit: usize,
        byte_limit: usize,
        total_bytes: usize,
    }

    impl FederationCache {
        fn load(path: PathBuf, entry_limit: usize, byte_limit: usize) -> Self {
            let empty = Self {
                entries: BTreeMap::new(),
                entry_limit,
                byte_limit,
                total_bytes: 0,
            };
            let Ok(bytes) = fs::read(path) else {
                return empty;
            };
            let Ok(file) =
                serde_json::from_slice::<FederationCacheFile>(&bytes)
            else {
                return empty;
            };
            if file.schema_version > FEDERATION_IPC_SCHEMA_VERSION {
                return empty;
            }
            let mut cache = empty;
            for entry in file.entries {
                if entry.byte_size <= byte_limit {
                    cache.total_bytes =
                        cache.total_bytes.saturating_add(entry.byte_size);
                    cache.entries.insert(
                        compound_cache_key(&entry.host_id, &entry.key),
                        entry,
                    );
                }
            }
            cache.enforce_budget();
            cache
        }

        fn store(
            &mut self,
            host_id: &str,
            key: &str,
            payload: JsonValue,
        ) -> Option<FederationCacheEntry> {
            let byte_size = serde_json::to_vec(&payload).ok()?.len();
            if byte_size > self.byte_limit {
                return None;
            }
            let now = unix_now_ms();
            let compound = compound_cache_key(host_id, key);
            if let Some(previous) = self.entries.remove(&compound) {
                self.total_bytes =
                    self.total_bytes.saturating_sub(previous.byte_size);
            }
            let entry = FederationCacheEntry {
                host_id: host_id.to_string(),
                key: key.to_string(),
                saved_at_unix_ms: now,
                last_used_unix_ms: now,
                byte_size,
                payload,
            };
            self.total_bytes = self.total_bytes.saturating_add(byte_size);
            self.entries.insert(compound, entry.clone());
            self.enforce_budget();
            Some(entry)
        }

        fn get(
            &mut self,
            host_id: &str,
            key: &str,
        ) -> Option<FederationCacheEntry> {
            let compound = compound_cache_key(host_id, key);
            let entry = self.entries.get_mut(&compound)?;
            entry.last_used_unix_ms = unix_now_ms();
            Some(entry.clone())
        }

        fn persist(&self, path: &Path) -> io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
                fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
            }
            let tmp = path.with_extension("json.tmp");
            let bytes = serde_json::to_vec(&FederationCacheFile {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                entries: self.entries.values().cloned().collect(),
            })?;
            {
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&tmp)?;
                file.set_permissions(fs::Permissions::from_mode(0o600))?;
                use std::io::Write;
                file.write_all(&bytes)?;
                file.sync_all()?;
            }
            fs::rename(tmp, path)
        }

        fn enforce_budget(&mut self) {
            while self.entries.len() > self.entry_limit
                || self.total_bytes > self.byte_limit
            {
                let Some(key) = self
                    .entries
                    .iter()
                    .min_by_key(|(key, entry)| {
                        (entry.last_used_unix_ms, (*key).clone())
                    })
                    .map(|(key, _)| key.clone())
                else {
                    break;
                };
                if let Some(entry) = self.entries.remove(&key) {
                    self.total_bytes =
                        self.total_bytes.saturating_sub(entry.byte_size);
                }
            }
        }
    }

    fn cache_path(sase_home: &Path) -> PathBuf {
        sase_home.join("fleet").join("worker_cache.json")
    }

    fn compound_cache_key(host_id: &str, key: &str) -> String {
        format!("{host_id}\0{key}")
    }

    fn entry_age(entry: Option<&FederationCacheEntry>) -> Option<f64> {
        entry.map(|entry| {
            unix_now_ms().saturating_sub(entry.saved_at_unix_ms) as f64 / 1000.0
        })
    }

    fn fleet_url(endpoint: &str, fleet_path: &str) -> String {
        let base = endpoint.trim_end_matches('/');
        if base.ends_with("/api/fleet/v1") {
            format!("{base}{fleet_path}")
        } else if base.ends_with("/api") {
            format!("{base}/fleet/v1{fleet_path}")
        } else {
            format!("{base}/api/fleet/v1{fleet_path}")
        }
    }

    fn fleet_http_error(
        status: StatusCode,
        payload: JsonValue,
    ) -> FederationErrorWire {
        let code = payload
            .get("code")
            .and_then(JsonValue::as_str)
            .map(|value| value.to_string())
            .unwrap_or_else(|| {
                if status == StatusCode::UNAUTHORIZED {
                    "unauthorized".to_string()
                } else if status == StatusCode::NOT_FOUND {
                    "not_found".to_string()
                } else if status == StatusCode::REQUEST_TIMEOUT {
                    "deadline".to_string()
                } else {
                    "unavailable".to_string()
                }
            });
        let message = payload
            .get("message")
            .and_then(JsonValue::as_str)
            .map(|value| value.to_string())
            .unwrap_or_else(|| {
                format!("fleet endpoint returned HTTP {status}")
            });
        FederationErrorWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            code,
            message,
            target: payload
                .get("target")
                .and_then(JsonValue::as_str)
                .map(ToString::to_string),
            details: payload.get("details").cloned().map(Box::new),
        }
    }

    #[cfg(target_os = "linux")]
    fn peer_is_same_user(stream: &UnixStream) -> io::Result<bool> {
        let fd = stream.as_raw_fd();
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                &mut cred as *mut _ as *mut libc::c_void,
                &mut len,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(cred.uid == unsafe { libc::geteuid() })
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    fn peer_is_same_user(stream: &UnixStream) -> io::Result<bool> {
        let fd = stream.as_raw_fd();
        let mut uid: libc::uid_t = 0;
        let mut gid: libc::gid_t = 0;
        let rc = unsafe { libc::getpeereid(fd, &mut uid, &mut gid) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(uid == unsafe { libc::geteuid() })
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    )))]
    fn peer_is_same_user(_stream: &UnixStream) -> io::Result<bool> {
        Ok(true)
    }

    #[cfg(test)]
    mod tests {
        use std::{
            os::unix::fs::PermissionsExt, path::Path, process::Command,
            time::Duration,
        };

        use serde_json::json;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::UnixStream,
            time,
        };

        use super::*;

        #[tokio::test]
        async fn framed_ipc_rejects_oversized_request() {
            let (mut client, mut server) = UnixStream::pair().unwrap();
            let server = tokio::spawn(async move {
                let err = read_request(&mut server, 8).await.unwrap_err();
                assert_eq!(err.code, "payload_too_large");
            });
            client.write_all(&16_u32.to_be_bytes()).await.unwrap();
            client.write_all(b"0123456789abcdef").await.unwrap();
            server.await.unwrap();
        }

        #[tokio::test]
        async fn listener_creates_private_socket_and_rejects_symlink() {
            let tmp = tempfile::tempdir().unwrap();
            let mut config = FederationWorkerConfig::new(tmp.path());
            config.run_root = tmp.path().join("run");
            config.socket_path = config.run_root.join("worker.sock");
            let prepared = prepare_listener(&config).await.unwrap();
            let dir_mode =
                fs::metadata(&config.run_root).unwrap().permissions().mode()
                    & 0o777;
            let socket_mode = fs::metadata(&config.socket_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(dir_mode, 0o700);
            assert_eq!(socket_mode, 0o600);
            drop(prepared);

            #[cfg(target_family = "unix")]
            std::os::unix::fs::symlink("/tmp/target", &config.socket_path)
                .unwrap();
            match prepare_listener(&config).await {
                Err(FederationWorkerError::UnsafeSocket { .. }) => {}
                Err(error) => panic!("unexpected error: {error}"),
                Ok(_) => panic!("symlink socket path should be rejected"),
            }
        }

        #[tokio::test]
        async fn cache_persists_and_ignores_newer_schema() {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join("fleet").join("worker_cache.json");
            let mut cache = FederationCache::load(path.clone(), 2, 1024);
            cache.store("host-a", "summary:{}", json!({"ok": true}));
            cache.persist(&path).unwrap();
            let mut loaded = FederationCache::load(path.clone(), 2, 1024);
            assert_eq!(
                loaded.get("host-a", "summary:{}").unwrap().payload["ok"],
                json!(true)
            );
            fs::write(
                &path,
                br#"{"schema_version":999,"entries":[{"host_id":"host-a"}]}"#,
            )
            .unwrap();
            let mut empty = FederationCache::load(path, 2, 1024);
            assert!(empty.get("host-a", "summary:{}").is_none());
        }

        fn sample_mutation_request(
            installation_id: &str,
        ) -> FleetMutationRequestWire {
            let target = sase_core::AgentInstanceLocatorWire {
                schema_version: 1,
                logical: sase_core::LogicalAgentLocatorWire {
                    schema_version: 1,
                    project: sase_core::ProjectLocatorWire {
                        schema_version: 1,
                        origin: sase_core::OriginLocatorWire {
                            schema_version: 1,
                            installation_id: installation_id.to_string(),
                        },
                        project_id: "proj".to_string(),
                    },
                    agent_id: "alpha".to_string(),
                    family_id: None,
                },
                shell_id: "ace-run".to_string(),
                run_id: "20260906120000".to_string(),
                attempt_id: "attempt-0".to_string(),
            };
            let logical_key =
                sase_core::logical_locator_key(&target.logical).unwrap();
            FleetMutationRequestWire {
                schema_version: 1,
                key: sase_core::ScopedOperationKeyWire {
                    schema_version: 1,
                    controller_id: "controller-1".to_string(),
                    operation_id: "op-1".to_string(),
                },
                target_installation_id: installation_id.to_string(),
                intent: sase_core::FleetMutationIntentWire {
                    schema_version: 1,
                    kind: sase_core::FleetMutationKindWire::Stop,
                    row_revision: sase_core::ResourceRevisionWire {
                        schema_version: 1,
                        logical_key,
                        revision: 1,
                    },
                    target,
                    reason: Some("stop".to_string()),
                    fork_prompt: None,
                    kill_source_first: None,
                    follow: false,
                },
                payload_fingerprint: sase_core::PayloadFingerprintWire {
                    schema_version: 1,
                    sha256: "a".repeat(64),
                },
                acceptance_window_seconds: 30.0,
            }
        }

        #[tokio::test]
        async fn mutate_rejects_unknown_alias_pin_mismatch_and_deadline() {
            let tmp = tempfile::tempdir().unwrap();
            let state = Arc::new(FederationWorkerState::new(
                FederationWorkerConfig::new(tmp.path()),
            ));
            let installation = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "a".repeat(64)
            );
            let request = sample_mutation_request(&installation);
            let missing = state
                .mutate_one(
                    "apollo".to_string(),
                    request.clone(),
                    RequestDeadline { unix_ms: None },
                )
                .await
                .unwrap_err();
            assert_eq!(missing.code, "not_found");

            let other = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "b".repeat(64)
            );
            state
                .replace_config(vec![FederationHostConfigWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    alias: Some("apollo".to_string()),
                    plan: ConnectionPlanWire {
                        schema_version: 1,
                        provider_ref: "builtin:https".to_string(),
                        endpoint: "https://apollo.example".to_string(),
                        credential_ref: "cred-1".to_string(),
                        pinned_installation_id: other,
                        connection_kind:
                            sase_core::FleetConnectionKindWire::Gateway,
                        tls: sase_core::TlsTrustSettingsWire {
                            schema_version: 1,
                            mode: sase_core::TlsTrustModeWire::SystemRoots,
                            ca_ref: None,
                            server_name_ref: None,
                        },
                    },
                    bearer_token: "token".to_string(),
                }])
                .await
                .unwrap();
            let pin = state
                .mutate_one(
                    "apollo".to_string(),
                    request,
                    RequestDeadline { unix_ms: None },
                )
                .await
                .unwrap_err();
            assert_eq!(pin.code, "quarantined");

            let envelope = FederationIpcRequestEnvelopeWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                request_id: "req-1".to_string(),
                deadline_unix_ms: Some(1),
                operation: FederationIpcRequestWire::Mutate {
                    target: "apollo".to_string(),
                    request: Box::new(sample_mutation_request(&installation)),
                },
            };
            let response = handle_request(state, envelope).await;
            assert!(!response.ok);
            assert_eq!(response.error.unwrap().code, "deadline");
        }

        fn sample_attention_request(
            installation_id: &str,
        ) -> FleetAttentionRequestWire {
            let intent = sase_core::FleetAttentionIntentWire {
                schema_version: 1,
                kind: sase_core::FleetAttentionKindWire::Gate,
                request_key: sase_core::FleetAttentionRequestKeyWire {
                    schema_version: 1,
                    origin_installation_id: installation_id.to_string(),
                    request_id: "gate-00000001".to_string(),
                    pending_action_prefix: "gate-000".to_string(),
                },
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
            let fingerprint =
                sase_core::fleet_attention_payload_fingerprint(&intent)
                    .unwrap();
            FleetAttentionRequestWire {
                schema_version: 1,
                key: sase_core::ScopedOperationKeyWire {
                    schema_version: 1,
                    controller_id: "controller-1".to_string(),
                    operation_id: "op-1".to_string(),
                },
                target_installation_id: installation_id.to_string(),
                intent,
                payload_fingerprint: fingerprint,
                acceptance_window_seconds: 30.0,
            }
        }

        #[tokio::test]
        async fn resolve_attention_rejects_unknown_alias_pin_mismatch_and_deadline(
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let state = Arc::new(FederationWorkerState::new(
                FederationWorkerConfig::new(tmp.path()),
            ));
            let installation = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "a".repeat(64)
            );
            let request = sample_attention_request(&installation);
            let missing = state
                .resolve_attention_one(
                    "apollo".to_string(),
                    request.clone(),
                    RequestDeadline { unix_ms: None },
                )
                .await
                .unwrap_err();
            assert_eq!(missing.code, "not_found");

            let other = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "b".repeat(64)
            );
            state
                .replace_config(vec![FederationHostConfigWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    alias: Some("apollo".to_string()),
                    plan: ConnectionPlanWire {
                        schema_version: 1,
                        provider_ref: "builtin:https".to_string(),
                        endpoint: "https://apollo.example".to_string(),
                        credential_ref: "cred-1".to_string(),
                        pinned_installation_id: other,
                        connection_kind:
                            sase_core::FleetConnectionKindWire::Gateway,
                        tls: sase_core::TlsTrustSettingsWire {
                            schema_version: 1,
                            mode: sase_core::TlsTrustModeWire::SystemRoots,
                            ca_ref: None,
                            server_name_ref: None,
                        },
                    },
                    bearer_token: "token".to_string(),
                }])
                .await
                .unwrap();
            let pin = state
                .resolve_attention_one(
                    "apollo".to_string(),
                    request,
                    RequestDeadline { unix_ms: None },
                )
                .await
                .unwrap_err();
            assert_eq!(pin.code, "quarantined");

            let envelope = FederationIpcRequestEnvelopeWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                request_id: "req-1".to_string(),
                deadline_unix_ms: Some(1),
                operation: FederationIpcRequestWire::ResolveAttention {
                    target: "apollo".to_string(),
                    request: Box::new(sample_attention_request(&installation)),
                },
            };
            let response = handle_request(state, envelope).await;
            assert!(!response.ok);
            assert_eq!(response.error.unwrap().code, "deadline");
        }

        #[tokio::test]
        async fn attention_read_returns_empty_hosts_when_unconfigured() {
            let tmp = tempfile::tempdir().unwrap();
            let state = Arc::new(FederationWorkerState::new(
                FederationWorkerConfig::new(tmp.path()),
            ));
            let result = state
                .read_all(
                    ReadOperation::Attention(FleetLogicalBatchRequestWire {
                        schema_version: 1,
                        logical_keys: vec!["logical:alpha".to_string()],
                    }),
                    false,
                    RequestDeadline { unix_ms: None },
                )
                .await
                .unwrap();
            assert_eq!(result["operation"], "attention");
            assert_eq!(result["hosts"], json!([]));
        }

        #[tokio::test]
        async fn attention_inventory_read_returns_empty_hosts_when_unconfigured(
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let state = Arc::new(FederationWorkerState::new(
                FederationWorkerConfig::new(tmp.path()),
            ));
            let result = state
                .read_all(
                    ReadOperation::AttentionInventory(
                        FleetAttentionInventoryRequestWire {
                            schema_version: 1,
                            cursor: None,
                            limit: None,
                        },
                    ),
                    false,
                    RequestDeadline { unix_ms: None },
                )
                .await
                .unwrap();
            assert_eq!(result["operation"], "attention_inventory");
            assert_eq!(result["hosts"], json!([]));
        }

        #[test]
        fn attention_read_operation_cache_key_is_namespaced_and_stable() {
            let request = FleetLogicalBatchRequestWire {
                schema_version: 1,
                logical_keys: vec!["logical:alpha".to_string()],
            };
            let operation = ReadOperation::Attention(request.clone());
            assert_eq!(operation.name(), "attention");
            let key = operation.cache_key().unwrap();
            assert!(key.starts_with("attention:"));
            assert_eq!(
                key,
                ReadOperation::Attention(request).cache_key().unwrap()
            );
        }

        #[test]
        fn attention_inventory_read_operation_cache_key_is_namespaced_and_stable(
        ) {
            let request = FleetAttentionInventoryRequestWire {
                schema_version: 1,
                cursor: Some("off:50".to_string()),
                limit: Some(50),
            };
            let operation = ReadOperation::AttentionInventory(request.clone());
            assert_eq!(operation.name(), "attention_inventory");
            let key = operation.cache_key().unwrap();
            assert!(key.starts_with("attention_inventory:"));
            assert_eq!(
                key,
                ReadOperation::AttentionInventory(request)
                    .cache_key()
                    .unwrap()
            );
        }

        #[test]
        fn fleet_endpoint_join_accepts_api_bases() {
            assert_eq!(
                fleet_url("https://fleet.example.test", "/summary"),
                "https://fleet.example.test/api/fleet/v1/summary"
            );
            assert_eq!(
                fleet_url("https://fleet.example.test/api", "/summary"),
                "https://fleet.example.test/api/fleet/v1/summary"
            );
            assert_eq!(
                fleet_url(
                    "https://fleet.example.test/api/fleet/v1",
                    "/summary"
                ),
                "https://fleet.example.test/api/fleet/v1/summary"
            );
        }

        #[tokio::test]
        async fn worker_answers_health_over_local_ipc() {
            let tmp = tempfile::tempdir().unwrap();
            let mut config = FederationWorkerConfig::new(tmp.path());
            config.run_root = tmp.path().join("run");
            config.socket_path = config.run_root.join("worker.sock");
            config.idle_timeout = Duration::from_secs(30);
            let socket_path = config.socket_path.clone();

            let worker = tokio::spawn(run(config));
            let health = request_worker(
                &socket_path,
                "health-1",
                json!({"op": "health"}),
            )
            .await;
            assert_eq!(health["service"], json!(FEDERATION_WORKER_SERVICE));
            assert_eq!(health["status"], json!("ok"));
            assert_eq!(health["configured_hosts"], json!(0));

            let shutdown = request_worker(
                &socket_path,
                "shutdown-1",
                json!({"op": "shutdown"}),
            )
            .await;
            assert_eq!(shutdown["shutdown"], json!(true));
            worker.await.unwrap().unwrap();
        }

        async fn request_worker(
            socket_path: &Path,
            request_id: &str,
            operation: serde_json::Value,
        ) -> serde_json::Value {
            let mut last_error = None;
            let mut connected = None;
            for _ in 0..100 {
                match UnixStream::connect(socket_path).await {
                    Ok(stream) => {
                        connected = Some(stream);
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
            let mut stream = connected.unwrap_or_else(|| {
                panic!("worker socket did not become ready: {last_error:?}")
            });
            let payload = json!({
                "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                "request_id": request_id,
                "deadline_unix_ms": unix_now_ms() + 5000,
                "operation": operation,
            });
            let bytes = serde_json::to_vec(&payload).unwrap();
            stream
                .write_all(&(bytes.len() as u32).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&bytes).await.unwrap();

            let mut len_bytes = [0_u8; 4];
            stream.read_exact(&mut len_bytes).await.unwrap();
            let len = u32::from_be_bytes(len_bytes) as usize;
            let mut response_bytes = vec![0_u8; len];
            stream.read_exact(&mut response_bytes).await.unwrap();
            let response: serde_json::Value =
                serde_json::from_slice(&response_bytes).unwrap();
            assert_eq!(response["request_id"], json!(request_id));
            assert_eq!(response["ok"], json!(true));
            response["result"].clone()
        }

        async fn request_worker_with_deadline(
            socket_path: &Path,
            request_id: &str,
            operation: serde_json::Value,
            deadline_unix_ms: u64,
        ) -> serde_json::Value {
            let mut stream = UnixStream::connect(socket_path).await.unwrap();
            let payload = json!({
                "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                "request_id": request_id,
                "deadline_unix_ms": deadline_unix_ms,
                "operation": operation,
            });
            let bytes = serde_json::to_vec(&payload).unwrap();
            stream
                .write_all(&(bytes.len() as u32).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&bytes).await.unwrap();

            let mut len_bytes = [0_u8; 4];
            stream.read_exact(&mut len_bytes).await.unwrap();
            let len = u32::from_be_bytes(len_bytes) as usize;
            let mut response_bytes = vec![0_u8; len];
            stream.read_exact(&mut response_bytes).await.unwrap();
            let response: serde_json::Value =
                serde_json::from_slice(&response_bytes).unwrap();
            assert_eq!(response["request_id"], json!(request_id));
            response
        }

        fn host_result<'a>(
            response: &'a serde_json::Value,
            alias: &str,
        ) -> &'a serde_json::Value {
            response["result"]["hosts"]
                .as_array()
                .unwrap_or_else(|| {
                    panic!("response had no hosts array: {response}")
                })
                .iter()
                .find(|host| host["alias"] == json!(alias))
                .unwrap_or_else(|| {
                    panic!("no host result for alias {alias:?} in {response}")
                })
        }

        struct HttpsFixture {
            port: u16,
            requests: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        }

        async fn start_https_fixture(
            root: &Path,
            ca_ref: &str,
            installation_id: &str,
        ) -> HttpsFixture {
            let cert_dir = root.join("certs").join(ca_ref);
            fs::create_dir_all(&cert_dir).unwrap();
            let ca_path = cert_dir.join("ca.pem");
            let cert_path = cert_dir.join("cert.pem");
            let key_path = cert_dir.join("key.pem");
            generate_loopback_certificate(&ca_path, &cert_path, &key_path);
            let trust_dir = root.join("fleet").join("trust").join("ca");
            fs::create_dir_all(&trust_dir).unwrap();
            fs::copy(&ca_path, trust_dir.join(format!("{ca_ref}.pem")))
                .unwrap();

            let config = rustls_server_config(&cert_path, &key_path);
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            let listener =
                tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let installation = installation_id.to_string();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _addr)) = listener.accept().await else {
                        return;
                    };
                    let acceptor = acceptor.clone();
                    let recorded = recorded.clone();
                    let installation = installation.clone();
                    tokio::spawn(async move {
                        let Ok(mut stream) = acceptor.accept(stream).await
                        else {
                            return;
                        };
                        let Some((path, body)) =
                            read_http_request(&mut stream).await
                        else {
                            return;
                        };
                        recorded
                            .lock()
                            .unwrap()
                            .push(json!({"path": path, "body": body}));
                        let payload = https_fixture_payload(
                            &installation,
                            &path,
                            body.as_ref(),
                        );
                        write_http_json(&mut stream, payload).await;
                    });
                }
            });
            HttpsFixture { port, requests }
        }

        fn generate_loopback_certificate(
            ca_path: &Path,
            cert_path: &Path,
            key_path: &Path,
        ) {
            let cert_dir = cert_path.parent().expect("cert parent");
            let ca_key_path = cert_dir.join("ca-key.pem");
            let csr_path = cert_dir.join("server.csr");
            let ext_path = cert_dir.join("server-ext.cnf");
            fs::write(
                &ext_path,
                "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n",
            )
            .unwrap();
            run_openssl(
                Command::new("openssl")
                    .args([
                        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout",
                    ])
                    .arg(&ca_key_path)
                    .args(["-out"])
                    .arg(ca_path)
                    .args([
                        "-subj",
                        "/CN=SASE Test CA",
                        "-addext",
                        "basicConstraints=critical,CA:TRUE",
                        "-days",
                        "1",
                    ]),
            );
            run_openssl(
                Command::new("openssl")
                    .args(["req", "-newkey", "rsa:2048", "-nodes", "-keyout"])
                    .arg(key_path)
                    .args(["-out"])
                    .arg(&csr_path)
                    .args(["-subj", "/CN=localhost"]),
            );
            let _ = fs::remove_file(cert_dir.join("ca.srl"));
            run_openssl(
                Command::new("openssl")
                    .args(["x509", "-req", "-in"])
                    .arg(&csr_path)
                    .args(["-CA"])
                    .arg(ca_path)
                    .args(["-CAkey"])
                    .arg(&ca_key_path)
                    .args(["-CAcreateserial", "-out"])
                    .arg(cert_path)
                    .args(["-days", "1", "-extfile"])
                    .arg(&ext_path),
            );
        }

        fn run_openssl(command: &mut Command) {
            let output = command.output().unwrap_or_else(|error| {
                panic!("failed to execute openssl: {error}")
            });
            assert!(
                output.status.success(),
                "openssl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        fn rustls_server_config(
            cert_path: &Path,
            key_path: &Path,
        ) -> tokio_rustls::rustls::ServerConfig {
            let cert_bytes = fs::read(cert_path).unwrap();
            let mut cert_reader = std::io::BufReader::new(&cert_bytes[..]);
            let certs = rustls_pemfile::certs(&mut cert_reader)
                .unwrap()
                .into_iter()
                .map(tokio_rustls::rustls::Certificate)
                .collect::<Vec<_>>();
            let key_bytes = fs::read(key_path).unwrap();
            let mut key_reader = std::io::BufReader::new(&key_bytes[..]);
            let mut keys =
                rustls_pemfile::pkcs8_private_keys(&mut key_reader).unwrap();
            if keys.is_empty() {
                let mut key_reader = std::io::BufReader::new(&key_bytes[..]);
                keys =
                    rustls_pemfile::rsa_private_keys(&mut key_reader).unwrap();
            }
            tokio_rustls::rustls::ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(
                    certs,
                    tokio_rustls::rustls::PrivateKey(
                        keys.into_iter().next().expect("private key"),
                    ),
                )
                .unwrap()
        }

        async fn read_http_request<T>(
            stream: &mut T,
        ) -> Option<(String, Option<serde_json::Value>)>
        where
            T: tokio::io::AsyncRead + Unpin,
        {
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await.ok()?;
                if read == 0 {
                    return None;
                }
                bytes.extend_from_slice(&buffer[..read]);
                let Some(header_end) = find_header_end(&bytes) else {
                    continue;
                };
                let headers =
                    String::from_utf8_lossy(&bytes[..header_end]).to_string();
                let content_length = http_content_length(&headers);
                let body_start = header_end + 4;
                if bytes.len() < body_start + content_length {
                    continue;
                }
                let path = headers
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let body = if content_length == 0 {
                    None
                } else {
                    serde_json::from_slice(
                        &bytes[body_start..body_start + content_length],
                    )
                    .ok()
                };
                return Some((path, body));
            }
        }

        fn find_header_end(bytes: &[u8]) -> Option<usize> {
            bytes.windows(4).position(|window| window == b"\r\n\r\n")
        }

        fn http_content_length(headers: &str) -> usize {
            headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if !name.eq_ignore_ascii_case("content-length") {
                        return None;
                    }
                    value.trim().parse::<usize>().ok()
                })
                .unwrap_or(0)
        }

        async fn write_http_json<T>(stream: &mut T, payload: serde_json::Value)
        where
            T: tokio::io::AsyncWrite + Unpin,
        {
            let status = if payload.get("error").is_some() {
                "404 Not Found"
            } else {
                "200 OK"
            };
            let body = serde_json::to_vec(&payload).unwrap();
            let header = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        }

        fn https_fixture_payload(
            installation_id: &str,
            path: &str,
            body: Option<&serde_json::Value>,
        ) -> serde_json::Value {
            match path {
                "/api/fleet/v1/hello" => hello_payload(installation_id),
                "/api/fleet/v1/summary" => json!({
                    "schema_version": 1,
                    "cursor": cursor_payload(),
                    "counts": counts_payload(1, 1),
                    "count_revision": 1,
                    "freshness": freshness_payload(),
                }),
                "/api/fleet/v1/catalog" => {
                    let cursor = body
                        .and_then(|value| value.get("cursor"))
                        .and_then(serde_json::Value::as_str);
                    json!({
                        "schema_version": 1,
                        "cursor": cursor_payload(),
                        "counts": counts_payload(1, 1),
                        "count_revision": 1,
                        "freshness": freshness_payload(),
                        "page": {
                            "schema_version": 1,
                            "rows": [],
                            "limit": 100,
                            "total_matching_rows": 250,
                            "next_cursor": if cursor.is_none() {
                                json!("off:100")
                            } else {
                                serde_json::Value::Null
                            },
                            "has_more": cursor.is_none(),
                        },
                    })
                }
                _ => json!({"error": "not found"}),
            }
        }

        fn hello_payload(installation_id: &str) -> serde_json::Value {
            json!({
                "schema_version": 1,
                "protocol_version": FLEET_PROTOCOL_VERSION,
                "installation": {
                    "schema_version": 1,
                    "installation_id": installation_id,
                    "created_at_unix": 1_800_000_000.0,
                    "generation": 1,
                    "prior_installation_id": null,
                    "rotated_at_unix": null,
                    "adopted_at_unix": null,
                    "reason": null,
                },
                "machine_selector": "loopback",
                "capabilities": {
                    "schema_version": 1,
                    "resource": ["catalog.read", "summary.read"],
                    "host": [],
                    "protocol": ["fleet.v1"],
                },
                "credential": {
                    "schema_version": 1,
                    "credential_id": "cred-1",
                    "controller_id": Some("controller-1"),
                    "controller": {
                        "schema_version": 1,
                        "controller_id": Some("controller-1"),
                        "display_name": "test-controller",
                        "platform": null,
                        "app_version": null,
                    },
                    "scopes": ["fleet.read"],
                    "issued_at_unix": 1_800_000_000.0,
                    "expires_at_unix": null,
                    "rotated_at_unix": null,
                    "revoked_at_unix": null,
                    "revoked_reason": null,
                },
                "cursor": cursor_payload(),
                "counts": counts_payload(1, 1),
                "count_revision": 1,
                "freshness": freshness_payload(),
            })
        }

        fn cursor_payload() -> serde_json::Value {
            json!({
                "schema_version": 1,
                "store_generation": "test-generation",
                "sequence": 1,
            })
        }

        fn counts_payload(total: u64, running: u64) -> serde_json::Value {
            json!({
                "schema_version": 1,
                "basis": {
                    "schema_version": 1,
                    "input_rows": total,
                    "selected_rows": total,
                    "max_revision": 1,
                    "observed_at_unix_max": 1_800_000_000.0,
                },
                "logical_agent_total": total,
                "running": running,
                "waiting": 0,
                "attention": 0,
                "occupied_runner_slots": 0,
            })
        }

        fn freshness_payload() -> serde_json::Value {
            json!({
                "schema_version": 1,
                "freshness": "fresh",
                "partial": false,
                "refreshed_at_unix": 1_800_000_000.0,
                "error": null,
            })
        }

        fn tls_host(
            alias: &str,
            pin: &str,
            port: u16,
            mode: &str,
            ca_ref: Option<&str>,
        ) -> serde_json::Value {
            json!({
                "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                "alias": alias,
                "plan": {
                    "schema_version": 1,
                    "provider_ref": "builtin:https",
                    "endpoint": format!("https://127.0.0.1:{port}"),
                    "credential_ref": format!("cred-{alias}"),
                    "pinned_installation_id": pin,
                    "connection_kind": "gateway",
                    "tls": {
                        "schema_version": 1,
                        "mode": mode,
                        "ca_ref": ca_ref,
                        "server_name_ref": null,
                    },
                },
                "bearer_token": format!("token-{alias}"),
            })
        }

        fn request_paths(
            requests: &Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        ) -> Vec<String> {
            requests
                .lock()
                .unwrap()
                .iter()
                .filter_map(|request| {
                    request
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        }

        #[tokio::test]
        async fn replace_config_fails_closed_for_missing_managed_ca_ref() {
            let tmp = tempfile::tempdir().unwrap();
            let state = Arc::new(FederationWorkerState::new(
                FederationWorkerConfig::new(tmp.path()),
            ));
            let pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "c".repeat(64)
            );
            let response = state
                .replace_config(vec![FederationHostConfigWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    alias: Some("apollo".to_string()),
                    plan: ConnectionPlanWire {
                        schema_version: 1,
                        provider_ref: "builtin:https".to_string(),
                        endpoint: "https://127.0.0.1:443".to_string(),
                        credential_ref: "cred-apollo".to_string(),
                        pinned_installation_id: pin,
                        connection_kind:
                            sase_core::FleetConnectionKindWire::Gateway,
                        tls: sase_core::TlsTrustSettingsWire {
                            schema_version: 1,
                            mode: sase_core::TlsTrustModeWire::PinnedCa,
                            ca_ref: Some("missing".to_string()),
                            server_name_ref: None,
                        },
                    },
                    bearer_token: "token".to_string(),
                }])
                .await
                .unwrap();

            assert_eq!(response["configured_hosts"], json!(0), "{response}");
            assert_eq!(response["hosts"][0]["status"], json!("invalid"));
            assert_eq!(
                response["hosts"][0]["error"]["target"],
                json!("hosts.plan.tls.ca_ref")
            );
        }

        #[tokio::test]
        async fn worker_trusts_pinned_ca_and_preserves_healthy_host_beside_faults(
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let mut config = FederationWorkerConfig::new(tmp.path());
            config.run_root = tmp.path().join("run");
            config.socket_path = config.run_root.join("worker.sock");
            config.idle_timeout = Duration::from_secs(30);
            let socket_path = config.socket_path.clone();

            let healthy_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "d".repeat(64)
            );
            let untrusted_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "e".repeat(64)
            );
            let hung_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "f".repeat(64)
            );
            let fixture =
                start_https_fixture(tmp.path(), "loopback", &healthy_pin).await;
            let hung_listener =
                tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let hung_port = hung_listener.local_addr().unwrap().port();
            let hung_reached = Arc::new(tokio::sync::Notify::new());
            let hung_reached_writer = hung_reached.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _addr)) = hung_listener.accept().await
                    else {
                        return;
                    };
                    hung_reached_writer.notify_one();
                    let _stream = stream;
                    std::future::pending::<()>().await;
                }
            });

            let worker = tokio::spawn(run(config));
            let replace = request_worker(
                &socket_path,
                "replace-tls",
                json!({
                    "op": "replace_config",
                    "hosts": [
                        tls_host(
                            "apollo",
                            &healthy_pin,
                            fixture.port,
                            "pinned_ca",
                            Some("loopback"),
                        ),
                        tls_host(
                            "hera",
                            &untrusted_pin,
                            fixture.port,
                            "system_roots",
                            None,
                        ),
                        tls_host(
                            "zeus",
                            &hung_pin,
                            hung_port,
                            "system_roots",
                            None,
                        ),
                    ],
                }),
            )
            .await;
            assert_eq!(replace["configured_hosts"], json!(3), "{replace}");

            let deadline_budget_ms = 700_u64;
            let started = std::time::Instant::now();
            let response = request_worker_with_deadline(
                &socket_path,
                "summary-tls-1",
                json!({"op": "summary", "cache_only": false}),
                unix_now_ms() + deadline_budget_ms,
            )
            .await;
            let elapsed = started.elapsed();

            assert_eq!(response["ok"], json!(true), "{response}");
            assert!(
                elapsed < Duration::from_millis(deadline_budget_ms + 1500),
                "deadline was not bounded: waited {elapsed:?}",
            );
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    hung_reached.notified()
                )
                .await
                .is_ok(),
                "worker never connected to the hung host",
            );
            assert_eq!(
                host_result(&response, "apollo")["status"],
                json!("ok"),
                "{response}"
            );
            assert_eq!(
                host_result(&response, "apollo")["payload"]["counts"]
                    ["running"],
                json!(1)
            );
            assert_ne!(
                host_result(&response, "hera")["status"],
                json!("ok"),
                "system roots must not silently trust the pinned CA fixture: {response}"
            );
            assert_eq!(
                host_result(&response, "zeus")["status"],
                json!("deadline"),
                "{response}"
            );

            let second = request_worker_with_deadline(
                &socket_path,
                "summary-tls-2",
                json!({"op": "summary", "cache_only": false}),
                unix_now_ms() + deadline_budget_ms,
            )
            .await;
            assert_eq!(second["ok"], json!(true), "{second}");
            assert_eq!(
                host_result(&second, "apollo")["status"],
                json!("ok"),
                "{second}"
            );

            let paths = request_paths(&fixture.requests);
            assert!(
                paths.iter().any(|path| path == "/api/fleet/v1/hello"),
                "trusted fixture did not receive hello: {paths:?}",
            );
            assert!(
                paths.iter().any(|path| path == "/api/fleet/v1/summary"),
                "trusted fixture did not receive summary: {paths:?}",
            );

            let shutdown = request_worker(
                &socket_path,
                "shutdown-tls",
                json!({"op": "shutdown"}),
            )
            .await;
            assert_eq!(shutdown["shutdown"], json!(true));
            worker.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn worker_catalog_hosts_continues_only_requested_hosts() {
            let tmp = tempfile::tempdir().unwrap();
            let mut config = FederationWorkerConfig::new(tmp.path());
            config.run_root = tmp.path().join("run");
            config.socket_path = config.run_root.join("worker.sock");
            config.idle_timeout = Duration::from_secs(30);
            let socket_path = config.socket_path.clone();

            let apollo_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "1".repeat(64)
            );
            let zeus_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "2".repeat(64)
            );
            let apollo =
                start_https_fixture(tmp.path(), "apollo-ca", &apollo_pin).await;
            let zeus =
                start_https_fixture(tmp.path(), "zeus-ca", &zeus_pin).await;
            let worker = tokio::spawn(run(config));
            let replace = request_worker(
                &socket_path,
                "replace-catalog-hosts",
                json!({
                    "op": "replace_config",
                    "hosts": [
                        tls_host(
                            "apollo",
                            &apollo_pin,
                            apollo.port,
                            "pinned_ca",
                            Some("apollo-ca"),
                        ),
                        tls_host(
                            "zeus",
                            &zeus_pin,
                            zeus.port,
                            "pinned_ca",
                            Some("zeus-ca"),
                        ),
                    ],
                }),
            )
            .await;
            assert_eq!(replace["configured_hosts"], json!(2), "{replace}");

            let response = request_worker(
                &socket_path,
                "catalog-hosts-1",
                json!({
                    "op": "catalog_hosts",
                    "cache_only": false,
                    "queries": [
                        {
                            "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                            "installation_id": zeus_pin,
                            "query": {
                                "schema_version": 1,
                                "cursor": "zeus:100",
                                "limit": 100,
                                "project_ids": [],
                                "query": null,
                                "status_buckets": [],
                                "include_terminal": true,
                            },
                        },
                    ],
                }),
            )
            .await;
            assert_eq!(response["operation"], json!("catalog"));
            assert_eq!(response["hosts"].as_array().unwrap().len(), 1);
            assert_eq!(response["hosts"][0]["alias"], json!("zeus"));
            assert_eq!(response["hosts"][0]["status"], json!("ok"));
            assert!(request_paths(&apollo.requests).is_empty());
            let zeus_requests = zeus.requests.lock().unwrap().clone();
            assert_eq!(
                zeus_requests
                    .iter()
                    .filter(|request| request["path"]
                        == json!("/api/fleet/v1/catalog"))
                    .count(),
                1
            );
            assert_eq!(
                zeus_requests
                    .iter()
                    .find(|request| request["path"]
                        == json!("/api/fleet/v1/catalog"))
                    .unwrap()["body"]["cursor"],
                json!("zeus:100")
            );

            let shutdown = request_worker(
                &socket_path,
                "shutdown-catalog-hosts",
                json!({"op": "shutdown"}),
            )
            .await;
            assert_eq!(shutdown["shutdown"], json!(true));
            worker.await.unwrap().unwrap();
        }

        /// Real worker + real per-host HTTPS fan-out deadline, using two genuinely
        /// real loopback TCP fixtures instead of a scripted/mocked response: one
        /// host accepts the connection (proving the worker actually reached it)
        /// and then never answers, so it can only resolve via the worker's real
        /// deadline timeout; the other has nothing listening, so it fails fast
        /// with a real connection-refused error. This exercises the actual
        /// `read_all`/`read_one_host`/`with_deadline` fan-out in production code,
        /// proving the fast host's result is not discarded by the outer envelope
        /// deadline racing the still-hanging host, and that the worker stays
        /// usable for a subsequent request afterward.
        #[tokio::test]
        async fn worker_bounds_deadline_and_preserves_fast_host_beside_hung_host(
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let mut config = FederationWorkerConfig::new(tmp.path());
            config.run_root = tmp.path().join("run");
            config.socket_path = config.run_root.join("worker.sock");
            config.idle_timeout = Duration::from_secs(30);
            let socket_path = config.socket_path.clone();

            let hung_listener =
                tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let hung_port = hung_listener.local_addr().unwrap().port();
            let hung_reached = std::sync::Arc::new(tokio::sync::Notify::new());
            let hung_reached_writer = hung_reached.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _addr)) = hung_listener.accept().await
                    else {
                        return;
                    };
                    hung_reached_writer.notify_one();
                    let _stream = stream;
                    std::future::pending::<()>().await;
                }
            });

            // Reserve a port, then drop the listener: nothing answers there, so
            // connecting to it fails fast with a real connection-refused error.
            let fast_port = {
                let probe =
                    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                probe.local_addr().unwrap().port()
            };

            let worker = tokio::spawn(run(config));

            let hung_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "a".repeat(64)
            );
            let fast_pin = format!(
                "{}{}",
                sase_core::FLEET_INSTALLATION_ID_PREFIX,
                "b".repeat(64)
            );
            let replace = request_worker(
                &socket_path,
                "replace-1",
                json!({
                    "op": "replace_config",
                    "hosts": [
                        {
                            "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                            "alias": "zeus",
                            "plan": {
                                "schema_version": 1,
                                "provider_ref": "builtin:https",
                                "endpoint": format!("https://127.0.0.1:{hung_port}"),
                                "credential_ref": "cred-zeus",
                                "pinned_installation_id": hung_pin,
                                "connection_kind": "gateway",
                                "tls": {
                                    "schema_version": 1,
                                    "mode": "system_roots",
                                    "ca_ref": null,
                                    "server_name_ref": null,
                                },
                            },
                            "bearer_token": "token-zeus",
                        },
                        {
                            "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
                            "alias": "apollo",
                            "plan": {
                                "schema_version": 1,
                                "provider_ref": "builtin:https",
                                "endpoint": format!("https://127.0.0.1:{fast_port}"),
                                "credential_ref": "cred-apollo",
                                "pinned_installation_id": fast_pin,
                                "connection_kind": "gateway",
                                "tls": {
                                    "schema_version": 1,
                                    "mode": "system_roots",
                                    "ca_ref": null,
                                    "server_name_ref": null,
                                },
                            },
                            "bearer_token": "token-apollo",
                        },
                    ],
                }),
            )
            .await;
            assert_eq!(replace["configured_hosts"], json!(2), "{replace}");

            let deadline_budget_ms = 400_u64;
            let deadline_unix_ms = unix_now_ms() + deadline_budget_ms;
            let started = std::time::Instant::now();
            let response = request_worker_with_deadline(
                &socket_path,
                "summary-1",
                json!({"op": "summary", "cache_only": false}),
                deadline_unix_ms,
            )
            .await;
            let elapsed = started.elapsed();

            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    hung_reached.notified()
                )
                .await
                .is_ok(),
                "worker never connected to the hung host",
            );
            assert!(
                elapsed < Duration::from_millis(deadline_budget_ms + 1500),
                "deadline was not bounded: waited {elapsed:?}",
            );
            assert_eq!(
                response["ok"],
                json!(true),
                "a hung host must not discard the whole response: {response}",
            );

            let zeus = host_result(&response, "zeus");
            assert_eq!(zeus["status"], json!("deadline"), "{response}");

            let apollo = host_result(&response, "apollo");
            assert_ne!(apollo["status"], json!("deadline"), "{response}");
            assert_ne!(
                apollo["status"],
                json!("ok"),
                "nothing is listening on the fast host's port: {response}",
            );

            // The worker must remain usable for a subsequent request: the same
            // still-hung host must not wedge later calls either.
            let second_deadline_unix_ms = unix_now_ms() + deadline_budget_ms;
            let second_started = std::time::Instant::now();
            let second_response = request_worker_with_deadline(
                &socket_path,
                "summary-2",
                json!({"op": "summary", "cache_only": false}),
                second_deadline_unix_ms,
            )
            .await;
            let second_elapsed = second_started.elapsed();
            assert!(
                second_elapsed < Duration::from_millis(deadline_budget_ms + 1500),
                "worker was not usable on a subsequent request: waited {second_elapsed:?}",
            );
            assert_eq!(second_response["ok"], json!(true), "{second_response}");
            assert_eq!(
                host_result(&second_response, "zeus")["status"],
                json!("deadline"),
            );

            let inventory_deadline_unix_ms = unix_now_ms() + deadline_budget_ms;
            let inventory_started = std::time::Instant::now();
            let inventory_response = request_worker_with_deadline(
                &socket_path,
                "attention-inventory-1",
                json!({
                    "op": "attention_inventory",
                    "request": {
                        "schema_version": 1,
                        "limit": 1,
                    },
                    "cache_only": false,
                }),
                inventory_deadline_unix_ms,
            )
            .await;
            let inventory_elapsed = inventory_started.elapsed();
            assert!(
                inventory_elapsed
                    < Duration::from_millis(deadline_budget_ms + 1500),
                "inventory deadline was not bounded: waited {inventory_elapsed:?}",
            );
            assert_eq!(
                inventory_response["ok"],
                json!(true),
                "a hung inventory host must not discard the whole response: {inventory_response}",
            );
            assert_eq!(
                inventory_response["result"]["operation"],
                json!("attention_inventory"),
            );
            assert_eq!(
                host_result(&inventory_response, "zeus")["status"],
                json!("deadline"),
                "{inventory_response}",
            );
            assert_ne!(
                host_result(&inventory_response, "apollo")["status"],
                json!("deadline"),
                "{inventory_response}",
            );

            let shutdown = request_worker(
                &socket_path,
                "shutdown-1",
                json!({"op": "shutdown"}),
            )
            .await;
            assert_eq!(shutdown["shutdown"], json!(true));
            worker.await.unwrap().unwrap();
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use super::*;

    pub async fn run(
        _config: FederationWorkerConfig,
    ) -> Result<(), FederationWorkerError> {
        Err(FederationWorkerError::UnsupportedPlatform)
    }
}

fn federation_capabilities() -> Vec<String> {
    vec![
        "ipc.health".to_string(),
        "ipc.capabilities".to_string(),
        "config.replace".to_string(),
        "fleet.summary".to_string(),
        "fleet.catalog".to_string(),
        "fleet.catalog_hosts".to_string(),
        "fleet.followed_batch".to_string(),
        "fleet.detail".to_string(),
        "fleet.content_range".to_string(),
        "fleet.project_eligibility".to_string(),
        "fleet.launch".to_string(),
        "fleet.mutate".to_string(),
        "fleet.attention".to_string(),
        "fleet.attention_inventory".to_string(),
        "fleet.resolve_attention".to_string(),
        "cache.read".to_string(),
        "cache.persist".to_string(),
    ]
}

fn success_response(
    request_id: &str,
    result: JsonValue,
) -> FederationIpcResponseEnvelopeWire {
    FederationIpcResponseEnvelopeWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        request_id: request_id.to_string(),
        ok: true,
        result: Some(result),
        error: None,
    }
}

fn error_response(
    request_id: &str,
    error: FederationErrorWire,
) -> FederationIpcResponseEnvelopeWire {
    FederationIpcResponseEnvelopeWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        request_id: request_id.to_string(),
        ok: false,
        result: None,
        error: Some(error),
    }
}

fn federation_error(
    code: &str,
    message: &str,
    target: Option<&str>,
) -> FederationErrorWire {
    FederationErrorWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        code: code.to_string(),
        message: message.to_string(),
        target: target.map(ToString::to_string),
        details: None,
    }
}

fn status_from_error(error: &FederationErrorWire) -> String {
    match error.code.as_str() {
        "deadline" | "timeout" => "deadline",
        "quarantined" => "quarantined",
        "stale" => "stale",
        "not_found" => "not_found",
        "missing_credential" | "invalid_request" | "unsupported_version" => {
            "invalid"
        }
        _ => "unavailable",
    }
    .to_string()
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn to_json<T: Serialize>(value: T) -> Result<JsonValue, FederationErrorWire> {
    serde_json::to_value(value).map_err(|error| {
        federation_error(
            "internal",
            &format!("failed to serialize federation value: {error}"),
            Some("serialization"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_socket_and_idle_options() {
        let config = parse_federation_worker_args([
            "--sase-home".to_string(),
            "/tmp/sase-home".to_string(),
            "--run-root".to_string(),
            "/tmp/run".to_string(),
            "--socket".to_string(),
            "/tmp/run/custom.sock".to_string(),
            "--idle-timeout-seconds".to_string(),
            "1.5".to_string(),
            "--max-frame-bytes".to_string(),
            "4096".to_string(),
        ])
        .unwrap();

        assert_eq!(config.sase_home, PathBuf::from("/tmp/sase-home"));
        assert_eq!(config.run_root, PathBuf::from("/tmp/run"));
        assert_eq!(config.socket_path, PathBuf::from("/tmp/run/custom.sock"));
        assert_eq!(config.idle_timeout, Duration::from_millis(1500));
        assert_eq!(config.max_frame_bytes, 4096);
    }
}
