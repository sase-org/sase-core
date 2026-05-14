use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    task::JoinError,
};

use crate::{
    daemon::DaemonState,
    projection_service::ProjectionServiceState,
    wire::{
        LocalDaemonBatchResponseWire, LocalDaemonCapabilitiesResponseWire,
        LocalDaemonCollectionWire, LocalDaemonErrorCodeWire,
        LocalDaemonErrorWire, LocalDaemonEventBatchWire,
        LocalDaemonEventPayloadWire, LocalDaemonEventRecordWire,
        LocalDaemonEventRequestWire, LocalDaemonFallbackWire,
        LocalDaemonHealthResponseWire, LocalDaemonHealthStatusWire,
        LocalDaemonHeartbeatWire, LocalDaemonListItemWire,
        LocalDaemonListRequestWire, LocalDaemonListResponseWire,
        LocalDaemonPayloadBoundWire, LocalDaemonRebuildRequestWire,
        LocalDaemonRebuildResponseWire, LocalDaemonRequestEnvelopeWire,
        LocalDaemonRequestPayloadWire, LocalDaemonResponseEnvelopeWire,
        LocalDaemonResponsePayloadWire, LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
        LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION, LOCAL_DAEMON_MAX_PAGE_LIMIT,
        LOCAL_DAEMON_MAX_PAYLOAD_BYTES, LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
        LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
    },
};
use tracing::{debug, info_span, Instrument};

const LOCAL_DAEMON_CONTRACT_NAME: &str = "sase_local_daemon_framed_json_v1";
const LOCAL_DAEMON_SERVICE_NAME: &str = "sase_local_daemon";
const ACCEPT_SHUTDOWN_POLL: Duration = Duration::from_millis(50);
const SUBSCRIPTION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

static SNAPSHOT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Error)]
pub enum LocalTransportError {
    #[error("failed to create local daemon socket parent {path}: {source}")]
    CreateSocketParent {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("failed to remove stale local daemon socket {path}: {source}")]
    RemoveStaleSocket {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("failed to bind local daemon socket {path}: {source}")]
    Bind {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("failed to accept local daemon socket connection: {0}")]
    Accept(std::io::Error),
    #[error("local daemon socket task failed: {0}")]
    Join(#[from] JoinError),
}

pub async fn serve_local_transport(
    state: DaemonState,
) -> Result<(), LocalTransportError> {
    let span = info_span!(
        "local_rpc_serve",
        socket = %state.paths.socket_path.display()
    );
    async move {
    prepare_socket_path(&state.paths.socket_path)?;
    let listener =
        UnixListener::bind(&state.paths.socket_path).map_err(|source| {
            LocalTransportError::Bind {
                path: state.paths.socket_path.clone(),
                source,
            }
        })?;

    while !state.shutdown.is_requested() {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(LocalTransportError::Accept)?;
                let state = state.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, &state).await;
                });
            }
            _ = tokio::time::sleep(ACCEPT_SHUTDOWN_POLL) => {}
        }
    }

    Ok(())
    }
    .instrument(span)
    .await
}

pub async fn handle_connection(
    mut stream: UnixStream,
    state: &DaemonState,
) -> std::io::Result<()> {
    let _connection_guard = state.metrics.connection_guard();
    let started = Instant::now();
    let mut success = false;
    let span = info_span!("local_rpc_connection");
    async {
    let request = match read_request_frame(&mut stream).await {
        Ok(bytes) => request_for_frame(&bytes),
        Err(frame_error) => {
            if matches!(&frame_error, FrameReadError::PayloadTooLarge { .. }) {
                state.metrics.record_payload_rejection();
            }
            Err(frame_error_response(frame_error))
        }
    };
    let request = match request {
        Ok(request) => request,
        Err(response) => {
            let result = write_response_frame(&mut stream, &response).await;
            state.metrics.record_rpc(started.elapsed(), false);
            return result;
        }
    };
    debug!(request_id = %request.request_id, "dispatching local RPC request");
    let request_id = request.request_id.clone();

    match request.payload {
        LocalDaemonRequestPayloadWire::Events(events) => {
            let result = stream_event_subscription(
                &mut stream,
                request_id,
                events,
                state,
            )
            .await;
            success = result.is_ok();
            state.metrics.record_rpc(started.elapsed(), success);
            result
        }
        LocalDaemonRequestPayloadWire::Rebuild(request) => {
            let payload = handle_rebuild_payload(request, state).await;
            success = !matches!(payload, LocalDaemonResponsePayloadWire::Error(_));
            let response = envelope(request_id, None, payload);
            let result = write_response_frame(&mut stream, &response).await;
            state.metrics.record_rpc(started.elapsed(), success && result.is_ok());
            result
        }
        payload => {
            let payload = handle_payload(payload, state);
            success = !matches!(payload, LocalDaemonResponsePayloadWire::Error(_));
            let response = envelope(
                request_id,
                snapshot_id_for_payload(&payload),
                payload,
            );
            let result = write_response_frame(&mut stream, &response).await;
            state.metrics.record_rpc(started.elapsed(), success && result.is_ok());
            result
        }
    }
    }
    .instrument(span)
    .await
}

pub fn response_for_frame(
    frame: &[u8],
    state: &DaemonState,
) -> LocalDaemonResponseEnvelopeWire {
    let request = match request_for_frame(frame) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let payload = handle_payload(request.payload, state);
    envelope(
        request.request_id,
        snapshot_id_for_payload(&payload),
        payload,
    )
}

fn handle_payload(
    payload: LocalDaemonRequestPayloadWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    match payload {
        LocalDaemonRequestPayloadWire::Health {
            include_capabilities: _,
        } => LocalDaemonResponsePayloadWire::Health(health_response(state)),
        LocalDaemonRequestPayloadWire::Capabilities => {
            LocalDaemonResponsePayloadWire::Capabilities(capabilities_response())
        }
        LocalDaemonRequestPayloadWire::List(request) => {
            LocalDaemonResponsePayloadWire::List(list_response(request))
        }
        LocalDaemonRequestPayloadWire::Events(request) => {
            LocalDaemonResponsePayloadWire::Events(event_batch_response(
                request, state,
            ))
        }
        LocalDaemonRequestPayloadWire::Rebuild(_) => {
            LocalDaemonResponsePayloadWire::Error(local_error(
                LocalDaemonErrorCodeWire::UnsupportedCapability,
                "projection rebuild requires a live daemon connection",
                false,
                Some("payload".to_string()),
                None,
            ))
        }
        LocalDaemonRequestPayloadWire::Batch { requests } => {
            let responses = requests
                .into_iter()
                .map(|request| {
                    let payload = match request.payload {
                        LocalDaemonRequestPayloadWire::Batch { .. } => {
                            LocalDaemonResponsePayloadWire::Error(local_error(
                                LocalDaemonErrorCodeWire::InvalidRequest,
                                "nested local daemon batches are not supported",
                                false,
                                Some("payload".to_string()),
                                None,
                            ))
                        }
                        payload => handle_payload(payload, state),
                    };
                    LocalDaemonBatchResponseWire {
                        request_id: request.request_id,
                        snapshot_id: snapshot_id_for_payload(&payload),
                        payload,
                    }
                })
                .collect();
            LocalDaemonResponsePayloadWire::Batch { responses }
        }
    }
}

async fn handle_rebuild_payload(
    _request: LocalDaemonRebuildRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    let report = state.projection_service.rebuild_storage_reset_only().await;
    match report {
        Ok(report) => LocalDaemonResponsePayloadWire::Rebuild(
            LocalDaemonRebuildResponseWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                mode: "projection_storage_rebuild".to_string(),
                storage_reset_only: true,
                limitation: Some(
                    "domain source rebuild is not active until filesystem indexing lands; this reset replays retained projection events only"
                        .to_string(),
                ),
                report: serde_json::to_value(report).unwrap_or_else(|error| {
                    json!({"serialization_error": error.to_string()})
                }),
            },
        ),
        Err(error) => LocalDaemonResponsePayloadWire::Error(local_error(
            LocalDaemonErrorCodeWire::Internal,
            format!("projection rebuild failed: {error}"),
            false,
            Some("projection_db".to_string()),
            Some(state.projection_service.health_details()),
        )),
    }
}

fn request_for_frame(
    frame: &[u8],
) -> Result<LocalDaemonRequestEnvelopeWire, LocalDaemonResponseEnvelopeWire> {
    let request_id = request_id_from_json(frame).unwrap_or_default();
    let request: LocalDaemonRequestEnvelopeWire = serde_json::from_slice(frame)
        .map_err(|error| {
            error_response(
                request_id,
                LocalDaemonErrorCodeWire::InvalidRequest,
                format!("invalid local daemon request JSON: {error}"),
                false,
                None,
                None,
            )
        })?;

    if request.schema_version != LOCAL_DAEMON_WIRE_SCHEMA_VERSION {
        return Err(unsupported_schema_response(request.request_id));
    }
    if request.client.schema_version < LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION
        || request.client.schema_version
            > LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION
    {
        return Err(unsupported_schema_response(request.request_id));
    }

    Ok(request)
}

async fn stream_event_subscription(
    stream: &mut UnixStream,
    request_id: String,
    request: LocalDaemonEventRequestWire,
    state: &DaemonState,
) -> std::io::Result<()> {
    let _subscription_guard = state.metrics.subscription_guard();
    write_event_batch_frame(
        stream,
        &request_id,
        event_batch_response(request, state),
    )
    .await?;

    let mut interval = tokio::time::interval(SUBSCRIPTION_HEARTBEAT_INTERVAL);
    interval.tick().await;
    loop {
        interval.tick().await;
        if state.shutdown.is_requested() {
            return Ok(());
        }
        let heartbeat = event_batch_response(
            LocalDaemonEventRequestWire {
                since_event_id: None,
                collections: Vec::new(),
                snapshot_id: None,
                max_events: 1,
            },
            state,
        );
        write_event_batch_frame(stream, &request_id, heartbeat).await?;
    }
}

async fn write_event_batch_frame(
    stream: &mut UnixStream,
    request_id: &str,
    batch: LocalDaemonEventBatchWire,
) -> std::io::Result<()> {
    let payload = LocalDaemonResponsePayloadWire::Events(batch);
    let response = envelope(
        request_id.to_string(),
        snapshot_id_for_payload(&payload),
        payload,
    );
    write_response_frame(stream, &response).await
}

fn health_response(state: &DaemonState) -> LocalDaemonHealthResponseWire {
    let projection_status = state.projection_service.status();
    let status = match projection_status.state {
        ProjectionServiceState::Ok => LocalDaemonHealthStatusWire::Ok,
        ProjectionServiceState::Degraded => {
            LocalDaemonHealthStatusWire::Degraded
        }
    };
    state
        .metrics
        .set_health_ok(status == LocalDaemonHealthStatusWire::Ok);
    LocalDaemonHealthResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        status,
        service: LOCAL_DAEMON_SERVICE_NAME.to_string(),
        daemon_started: true,
        version: state.build.package_version.clone(),
        min_client_schema_version: LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
        max_client_schema_version: LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION,
        fallback: fallback_unavailable(),
        details: state.diagnostic_details(),
    }
}

fn capabilities_response() -> LocalDaemonCapabilitiesResponseWire {
    LocalDaemonCapabilitiesResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        contract: LOCAL_DAEMON_CONTRACT_NAME.to_string(),
        contract_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        min_client_schema_version: LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
        max_client_schema_version: LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION,
        capabilities: local_daemon_capabilities(),
        max_payload_bytes: LOCAL_DAEMON_MAX_PAYLOAD_BYTES,
        default_page_limit: LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
        max_page_limit: LOCAL_DAEMON_MAX_PAGE_LIMIT,
    }
}

fn list_response(
    request: LocalDaemonListRequestWire,
) -> LocalDaemonListResponseWire {
    let snapshot_id = request.snapshot_id.unwrap_or_else(next_snapshot_id);
    let max_payload_bytes = request
        .max_payload_bytes
        .unwrap_or(LOCAL_DAEMON_MAX_PAYLOAD_BYTES);
    let limit = request.page.limit.clamp(1, LOCAL_DAEMON_MAX_PAGE_LIMIT);
    let mut items = Vec::new();
    if request.collection == LocalDaemonCollectionWire::Mocked && limit > 0 {
        items.push(LocalDaemonListItemWire {
            handle: "mocked:item:1".to_string(),
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            summary: json!({"title": "contract fixture"}),
        });
    }
    LocalDaemonListResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        collection: request.collection,
        snapshot_id,
        items,
        next_cursor: None,
        stable_handle: request.stable_handle,
        bounded: LocalDaemonPayloadBoundWire {
            max_payload_bytes,
            truncated: false,
        },
    }
}

fn event_batch_response(
    request: LocalDaemonEventRequestWire,
    state: &DaemonState,
) -> LocalDaemonEventBatchWire {
    let max_events = event_batch_limit(request.max_events);
    let mut events =
        initial_event_records(&request, state, max_events.saturating_sub(1));
    let heartbeat = state.local_events.append_heartbeat();
    events.push(heartbeat.clone());
    let events = if events.len() > max_events {
        events.split_off(events.len() - max_events)
    } else {
        events
    };
    let snapshot_id = request
        .snapshot_id
        .or_else(|| events.last().map(|record| record.snapshot_id.clone()))
        .unwrap_or_else(next_snapshot_id);
    let next_event_id = events.last().map(|record| record.event_id.clone());
    LocalDaemonEventBatchWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        snapshot_id,
        events,
        heartbeat: Some(LocalDaemonHeartbeatWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            sequence: heartbeat_sequence(&heartbeat),
            created_at: heartbeat.created_at,
        }),
        next_event_id,
    }
}

fn initial_event_records(
    request: &LocalDaemonEventRequestWire,
    state: &DaemonState,
    limit: usize,
) -> Vec<LocalDaemonEventRecordWire> {
    let Some(after_event_id) = request.since_event_id.as_deref() else {
        return Vec::new();
    };
    match state.local_events.replay_after(
        after_event_id,
        &request.collections,
        limit,
    ) {
        Some(events) => events,
        None => vec![state
            .local_events
            .append_resync_required("after_event_id_not_available")],
    }
}

fn event_batch_limit(max_events: u32) -> usize {
    max_events.clamp(1, LOCAL_DAEMON_MAX_PAGE_LIMIT) as usize
}

fn heartbeat_sequence(record: &LocalDaemonEventRecordWire) -> u64 {
    match &record.payload {
        LocalDaemonEventPayloadWire::Heartbeat { sequence } => *sequence,
        _ => 0,
    }
}

fn local_daemon_capabilities() -> Vec<String> {
    vec![
        "health.read".to_string(),
        "capabilities.read".to_string(),
        "mocked.list".to_string(),
        "mocked.events".to_string(),
        "projection.rebuild".to_string(),
        "batch.request".to_string(),
    ]
}

async fn read_request_frame(
    stream: &mut UnixStream,
) -> Result<Vec<u8>, FrameReadError> {
    let mut len_bytes = [0_u8; 4];
    stream
        .read_exact(&mut len_bytes)
        .await
        .map_err(FrameReadError::Io)?;
    let len = u32::from_be_bytes(len_bytes);
    if len > LOCAL_DAEMON_MAX_PAYLOAD_BYTES {
        return Err(FrameReadError::PayloadTooLarge { len });
    }
    let mut payload = vec![0_u8; len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(FrameReadError::Io)?;
    Ok(payload)
}

async fn write_response_frame(
    stream: &mut UnixStream,
    response: &LocalDaemonResponseEnvelopeWire,
) -> std::io::Result<()> {
    let payload =
        serde_json::to_vec(response).map_err(std::io::Error::other)?;
    let len = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "local daemon response exceeded u32 frame length",
        )
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await
}

#[derive(Debug)]
enum FrameReadError {
    Io(std::io::Error),
    PayloadTooLarge { len: u32 },
}

fn frame_error_response(
    error: FrameReadError,
) -> LocalDaemonResponseEnvelopeWire {
    match error {
        FrameReadError::PayloadTooLarge { len } => error_response(
            String::new(),
            LocalDaemonErrorCodeWire::PayloadTooLarge,
            format!(
                "local daemon request frame is {len} bytes; maximum is {LOCAL_DAEMON_MAX_PAYLOAD_BYTES}"
            ),
            false,
            Some("frame_length".to_string()),
            Some(json!({"length": len, "max_payload_bytes": LOCAL_DAEMON_MAX_PAYLOAD_BYTES})),
        ),
        FrameReadError::Io(error) => error_response(
            String::new(),
            LocalDaemonErrorCodeWire::InvalidRequest,
            format!("failed to read local daemon request frame: {error}"),
            false,
            None,
            None,
        ),
    }
}

fn unsupported_schema_response(
    request_id: String,
) -> LocalDaemonResponseEnvelopeWire {
    error_response(
        request_id,
        LocalDaemonErrorCodeWire::UnsupportedClientVersion,
        format!(
            "local daemon supports client schema versions {LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION} through {LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION}"
        ),
        false,
        Some("schema_version".to_string()),
        Some(json!({
            "min_client_schema_version": LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
            "max_client_schema_version": LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION,
        })),
    )
}

fn error_response(
    request_id: String,
    code: LocalDaemonErrorCodeWire,
    message: String,
    retryable: bool,
    target: Option<String>,
    details: Option<JsonValue>,
) -> LocalDaemonResponseEnvelopeWire {
    envelope(
        request_id,
        None,
        LocalDaemonResponsePayloadWire::Error(LocalDaemonErrorWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            code,
            message,
            retryable,
            target,
            details,
            fallback: LocalDaemonFallbackWire {
                available: true,
                reason: None,
                message: Some("use direct source-store readers".to_string()),
            },
        }),
    )
}

fn local_error(
    code: LocalDaemonErrorCodeWire,
    message: impl Into<String>,
    retryable: bool,
    target: Option<String>,
    details: Option<JsonValue>,
) -> LocalDaemonErrorWire {
    LocalDaemonErrorWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        code,
        message: message.into(),
        retryable,
        target,
        details,
        fallback: LocalDaemonFallbackWire {
            available: true,
            reason: None,
            message: Some("use direct source-store readers".to_string()),
        },
    }
}

fn envelope(
    request_id: String,
    snapshot_id: Option<String>,
    payload: LocalDaemonResponsePayloadWire,
) -> LocalDaemonResponseEnvelopeWire {
    LocalDaemonResponseEnvelopeWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        request_id,
        snapshot_id,
        payload,
    }
}

fn snapshot_id_for_payload(
    payload: &LocalDaemonResponsePayloadWire,
) -> Option<String> {
    match payload {
        LocalDaemonResponsePayloadWire::List(response) => {
            Some(response.snapshot_id.clone())
        }
        LocalDaemonResponsePayloadWire::Events(response) => {
            Some(response.snapshot_id.clone())
        }
        _ => None,
    }
}

fn fallback_unavailable() -> LocalDaemonFallbackWire {
    LocalDaemonFallbackWire {
        available: false,
        reason: None,
        message: None,
    }
}

fn next_snapshot_id() -> String {
    let sequence = SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "snap_{}_{}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        sequence
    )
}

fn request_id_from_json(frame: &[u8]) -> Option<String> {
    let value: JsonValue = serde_json::from_slice(frame).ok()?;
    value.get("request_id")?.as_str().map(str::to_string)
}

fn prepare_socket_path(path: &Path) -> Result<(), LocalTransportError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            LocalTransportError::CreateSocketParent {
                path: parent.to_path_buf(),
                source,
            }
        })?;
    }
    if path.exists() {
        fs::remove_file(path).map_err(|source| {
            LocalTransportError::RemoveStaleSocket {
                path: path.to_path_buf(),
                source,
            }
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        daemon::{DaemonRuntimePaths, DaemonShutdown, LocalEventHub},
        projection_service::ProjectionService,
        wire::{
            GatewayBuildWire, LocalDaemonClientWire,
            LocalDaemonPageRequestWire, LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        },
    };

    use super::*;

    fn test_state() -> DaemonState {
        DaemonState {
            build: GatewayBuildWire {
                package_version: "0.1.1".to_string(),
                git_sha: None,
            },
            host_identity: "test-host".to_string(),
            paths: DaemonRuntimePaths {
                sase_home: PathBuf::from("/tmp/sase-home"),
                run_root: PathBuf::from("/tmp/sase-run"),
                socket_path: PathBuf::from("/tmp/sase-run/sase-daemon.sock"),
            },
            shutdown: DaemonShutdown::default(),
            metrics: crate::metrics::DaemonMetrics::default(),
            metrics_endpoint: std::sync::Arc::new(std::sync::RwLock::new(None)),
            local_events: LocalEventHub::new(
                256,
                crate::metrics::DaemonMetrics::default(),
            ),
            projection_service: ProjectionService::unavailable(
                PathBuf::from("/tmp/sase-run/projections/projection.sqlite"),
                "test projection service unavailable",
            ),
            mobile_gateway: None,
        }
    }

    fn request(
        payload: LocalDaemonRequestPayloadWire,
    ) -> LocalDaemonRequestEnvelopeWire {
        LocalDaemonRequestEnvelopeWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            request_id: "req_test".to_string(),
            client: LocalDaemonClientWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                name: "sase-test".to_string(),
                version: "0.1.1".to_string(),
            },
            payload,
        }
    }

    #[test]
    fn health_response_reports_live_daemon() {
        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Health {
                include_capabilities: true,
            },
        ))
        .unwrap();

        let response = response_for_frame(&payload, &test_state());

        assert_eq!(response.request_id, "req_test");
        match response.payload {
            LocalDaemonResponsePayloadWire::Health(health) => {
                assert_eq!(
                    health.status,
                    LocalDaemonHealthStatusWire::Degraded
                );
                assert!(health.daemon_started);
                assert!(!health.fallback.available);
                assert_eq!(
                    health.details["projection_db"]["state"],
                    json!("degraded")
                );
            }
            other => panic!("expected health response, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_schema_is_typed_error() {
        let mut request = request(LocalDaemonRequestPayloadWire::Capabilities);
        request.client.schema_version =
            LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION + 1;
        let payload = serde_json::to_vec(&request).unwrap();

        let response = response_for_frame(&payload, &test_state());

        match response.payload {
            LocalDaemonResponsePayloadWire::Error(error) => {
                assert_eq!(
                    error.code,
                    LocalDaemonErrorCodeWire::UnsupportedClientVersion
                );
                assert_eq!(error.target.as_deref(), Some("schema_version"));
            }
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[test]
    fn mocked_list_returns_bounded_contract_fixture() {
        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::List(LocalDaemonListRequestWire {
                collection: LocalDaemonCollectionWire::Mocked,
                page: LocalDaemonPageRequestWire {
                    limit: LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
                    cursor: None,
                },
                snapshot_id: Some("snap_mock_001".to_string()),
                stable_handle: Some("mocked:list:test".to_string()),
                max_payload_bytes: None,
                filters: None,
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &test_state());

        match response.payload {
            LocalDaemonResponsePayloadWire::List(list) => {
                assert_eq!(list.collection, LocalDaemonCollectionWire::Mocked);
                assert_eq!(list.items.len(), 1);
                assert_eq!(
                    list.bounded.max_payload_bytes,
                    LOCAL_DAEMON_MAX_PAYLOAD_BYTES
                );
                assert_eq!(
                    response.snapshot_id.as_deref(),
                    Some("snap_mock_001")
                );
            }
            other => panic!("expected list response, got {other:?}"),
        }
    }

    #[test]
    fn events_request_returns_heartbeat_batch() {
        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Events(
                LocalDaemonEventRequestWire {
                    since_event_id: None,
                    collections: Vec::new(),
                    snapshot_id: None,
                    max_events: 1,
                },
            ),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &test_state());

        match response.payload {
            LocalDaemonResponsePayloadWire::Events(events) => {
                assert_eq!(events.events.len(), 1);
                assert_eq!(response.snapshot_id, Some(events.snapshot_id));
                assert_eq!(
                    events.next_event_id.as_deref(),
                    Some("0000000000000001")
                );
                assert!(matches!(
                    events.events[0].payload,
                    LocalDaemonEventPayloadWire::Heartbeat { sequence: 1 }
                ));
                assert_eq!(
                    events
                        .heartbeat
                        .as_ref()
                        .map(|heartbeat| heartbeat.sequence),
                    Some(1)
                );
            }
            other => panic!("expected events response, got {other:?}"),
        }
    }

    #[test]
    fn events_request_accepts_after_event_id_alias() {
        let payload = json!({
            "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            "request_id": "req_test",
            "client": {
                "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                "name": "sase-test",
                "version": "0.1.1"
            },
            "payload": {
                "type": "events",
                "data": {
                    "after_event_id": "0000000000000000",
                    "collections": ["agents"],
                    "snapshot_id": null,
                    "max_events": 2
                }
            }
        });
        let decoded_payload: LocalDaemonRequestEnvelopeWire =
            serde_json::from_value(payload.clone()).unwrap();
        match decoded_payload.payload {
            LocalDaemonRequestPayloadWire::Events(events) => {
                assert_eq!(
                    events.since_event_id.as_deref(),
                    Some("0000000000000000")
                );
                assert_eq!(
                    events.collections,
                    vec![LocalDaemonCollectionWire::Agents]
                );
            }
            other => panic!("expected events request, got {other:?}"),
        }
        let payload = serde_json::to_vec(&payload).unwrap();

        let response = response_for_frame(&payload, &test_state());

        match response.payload {
            LocalDaemonResponsePayloadWire::Events(events) => {
                assert!(matches!(
                    events.events.last().map(|record| &record.payload),
                    Some(LocalDaemonEventPayloadWire::Heartbeat { .. })
                ));
                assert!(events.heartbeat.is_some());
            }
            other => panic!("expected events response, got {other:?}"),
        }
    }

    #[test]
    fn events_request_replays_after_event_id() {
        let state = test_state();
        let first_payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Events(
                LocalDaemonEventRequestWire {
                    since_event_id: None,
                    collections: Vec::new(),
                    snapshot_id: None,
                    max_events: 1,
                },
            ),
        ))
        .unwrap();
        let first = response_for_frame(&first_payload, &state);
        let first_event_id = match first.payload {
            LocalDaemonResponsePayloadWire::Events(events) => {
                events.next_event_id.unwrap()
            }
            other => panic!("expected events response, got {other:?}"),
        };
        let second_payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Events(
                LocalDaemonEventRequestWire {
                    since_event_id: Some(first_event_id),
                    collections: Vec::new(),
                    snapshot_id: None,
                    max_events: 2,
                },
            ),
        ))
        .unwrap();

        let second = response_for_frame(&second_payload, &state);

        match second.payload {
            LocalDaemonResponsePayloadWire::Events(events) => {
                assert_eq!(events.events.len(), 1);
                assert!(matches!(
                    events.events[0].payload,
                    LocalDaemonEventPayloadWire::Heartbeat { .. }
                ));
            }
            other => panic!("expected events response, got {other:?}"),
        }
    }

    #[test]
    fn oversized_frame_is_typed_error_without_payload_parse() {
        let response = frame_error_response(FrameReadError::PayloadTooLarge {
            len: LOCAL_DAEMON_MAX_PAYLOAD_BYTES + 1,
        });

        match response.payload {
            LocalDaemonResponsePayloadWire::Error(error) => {
                assert_eq!(
                    error.code,
                    LocalDaemonErrorCodeWire::PayloadTooLarge
                );
                assert_eq!(error.target.as_deref(), Some("frame_length"));
            }
            other => panic!("expected error response, got {other:?}"),
        }
    }
}
