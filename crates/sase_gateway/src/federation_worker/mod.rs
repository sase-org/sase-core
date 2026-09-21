//! Federation worker: local IPC service that fans fleet reads out to
//! remote hosts.
//!
//! The platform-neutral surface (config, CLI entry points, error and IPC wire
//! types) lives here. The real implementation lives in [`imp`] on Unix; other
//! platforms get an unsupported-platform stub.

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
mod imp;
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
