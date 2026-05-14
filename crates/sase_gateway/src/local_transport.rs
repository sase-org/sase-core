use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use chrono::{SecondsFormat, Utc};
use sase_core::agent_archive::AgentArchiveSummaryWire;
use sase_core::notifications::{
    cleanup_stale_pending_actions_in_store, notification_append_jsonl,
    pending_action_store_json, pending_action_store_path,
    plan_notification_state_update_export, read_pending_action_store,
    register_pending_action_in_store, update_pending_action_in_store,
    NotificationStateUpdateWire, NotificationUpdateOutcomeWire,
    NotificationWire, PendingActionStoreWire, PendingActionWire,
};
use sase_core::projections::{
    agent_archive_bundle_indexed_event_request,
    agent_archive_bundle_purged_event_request,
    agent_archive_bundle_revived_event_request,
    agent_artifact_associated_event_request,
    agent_cleanup_result_recorded_event_request,
    agent_dismissed_identity_changed_event_request, changespec_handle,
    notification_append_event_request, notification_projection_counts,
    notification_projection_detail, notification_projection_page,
    notification_source_paths, notification_state_update_event_request,
    pending_action_cleanup_event_request,
    pending_action_register_event_request, pending_action_update_event_request,
    plan_bead_source_export_mutation, projected_pending_action_store,
    source_fingerprint_from_path, AgentArtifactAssociationWire,
    AgentDismissedIdentityWire, AgentProjectionEventContextWire,
    BeadMutationWriteRequestWire, BeadProjectionEventContextWire,
    EventAppendRequestWire, EventSourceWire, LocalDaemonMutationOutcomeWire,
    NotificationPendingActionsReadResponseWire,
    NotificationProjectionEventContextWire, NotificationReadDetailResponseWire,
    ProjectionPayloadBoundWire, ProjectionSnapshotReadWire,
    SourceExportKindWire, SourceExportPlanWire, SourceExportReportWire,
    SourceExportStatusWire, SourceFingerprintWire,
    CHANGESPEC_ACTIVE_ARCHIVE_MOVED, CHANGESPEC_SECTIONS_UPDATED,
    CHANGESPEC_SPEC_CREATED, CHANGESPEC_SPEC_UPDATED,
    CHANGESPEC_STATUS_TRANSITIONED, PROJECTION_READ_WIRE_SCHEMA_VERSION,
};
use serde::Deserialize;
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
        LocalDaemonHeartbeatWire, LocalDaemonIndexingDiffRequestWire,
        LocalDaemonIndexingVerifyRequestWire, LocalDaemonListItemWire,
        LocalDaemonListRequestWire, LocalDaemonListResponseWire,
        LocalDaemonPayloadBoundWire, LocalDaemonReadRequestWire,
        LocalDaemonReadResponseWire, LocalDaemonRebuildRequestWire,
        LocalDaemonRequestEnvelopeWire, LocalDaemonRequestPayloadWire,
        LocalDaemonResponseEnvelopeWire, LocalDaemonResponsePayloadWire,
        LocalDaemonWriteRequestWire, LocalDaemonWriteResponseWire,
        ProjectionPageRequestWire, LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
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
            Err(Box::new(frame_error_response(frame_error)))
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
        LocalDaemonRequestPayloadWire::Verify(request) => {
            let payload = handle_verify_payload(request, state).await;
            success = !matches!(payload, LocalDaemonResponsePayloadWire::Error(_));
            let response = envelope(request_id, None, payload);
            let result = write_response_frame(&mut stream, &response).await;
            state.metrics.record_rpc(started.elapsed(), success && result.is_ok());
            result
        }
        LocalDaemonRequestPayloadWire::Diff(request) => {
            let payload = handle_diff_payload(request, state).await;
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
        Err(response) => return *response,
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
            handle_list_payload(request, state)
        }
        LocalDaemonRequestPayloadWire::Read(request) => {
            handle_read_payload(request, state)
        }
        LocalDaemonRequestPayloadWire::Write(request) => {
            handle_write_payload(request, state)
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
        LocalDaemonRequestPayloadWire::IndexingStatus(request) => {
            LocalDaemonResponsePayloadWire::IndexingStatus(
                state
                    .indexing_service
                    .indexing_status(request, &state.projection_service),
            )
        }
        LocalDaemonRequestPayloadWire::Verify(_) => {
            LocalDaemonResponsePayloadWire::Error(local_error(
                LocalDaemonErrorCodeWire::UnsupportedCapability,
                "indexing verify requires a live daemon connection",
                false,
                Some("payload".to_string()),
                None,
            ))
        }
        LocalDaemonRequestPayloadWire::Diff(_) => {
            LocalDaemonResponsePayloadWire::Error(local_error(
                LocalDaemonErrorCodeWire::UnsupportedCapability,
                "indexing diff requires a live daemon connection",
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
    request: LocalDaemonRebuildRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    let report = state
        .indexing_service
        .rebuild(request, state.projection_service.clone())
        .await;
    match report {
        Ok(report) => LocalDaemonResponsePayloadWire::Rebuild(report),
        Err(error) => LocalDaemonResponsePayloadWire::Error(local_error(
            LocalDaemonErrorCodeWire::Internal,
            format!("indexing rebuild failed: {error}"),
            false,
            Some("projection_db".to_string()),
            Some(state.projection_service.health_details()),
        )),
    }
}

async fn handle_verify_payload(
    request: LocalDaemonIndexingVerifyRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    match state
        .indexing_service
        .verify(request, state.projection_service.clone())
        .await
    {
        Ok(response) => LocalDaemonResponsePayloadWire::Verify(response),
        Err(error) => LocalDaemonResponsePayloadWire::Error(local_error(
            LocalDaemonErrorCodeWire::Internal,
            format!("indexing verify failed: {error}"),
            false,
            Some("projection_db".to_string()),
            Some(state.projection_service.health_details()),
        )),
    }
}

async fn handle_diff_payload(
    request: LocalDaemonIndexingDiffRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    match state
        .indexing_service
        .diff(request, state.projection_service.clone())
        .await
    {
        Ok(response) => LocalDaemonResponsePayloadWire::Diff(response),
        Err(error) => LocalDaemonResponsePayloadWire::Error(local_error(
            LocalDaemonErrorCodeWire::Internal,
            format!("indexing diff failed: {error}"),
            false,
            Some("projection_db".to_string()),
            Some(state.projection_service.health_details()),
        )),
    }
}

fn request_for_frame(
    frame: &[u8],
) -> Result<LocalDaemonRequestEnvelopeWire, Box<LocalDaemonResponseEnvelopeWire>>
{
    let request_id = request_id_from_json(frame).unwrap_or_default();
    let request: LocalDaemonRequestEnvelopeWire = serde_json::from_slice(frame)
        .map_err(|error| {
            Box::new(error_response(
                request_id,
                LocalDaemonErrorCodeWire::InvalidRequest,
                format!("invalid local daemon request JSON: {error}"),
                false,
                None,
                None,
            ))
        })?;

    if request.schema_version != LOCAL_DAEMON_WIRE_SCHEMA_VERSION {
        return Err(Box::new(unsupported_schema_response(request.request_id)));
    }
    if request.client.schema_version < LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION
        || request.client.schema_version
            > LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION
    {
        return Err(Box::new(unsupported_schema_response(request.request_id)));
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

fn handle_list_payload(
    request: LocalDaemonListRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    if request.collection == LocalDaemonCollectionWire::Mocked {
        return LocalDaemonResponsePayloadWire::List(list_response(request));
    }
    if request.collection == LocalDaemonCollectionWire::Notifications {
        return notification_generic_list_payload(request, state)
            .unwrap_or_else(LocalDaemonResponsePayloadWire::Error);
    }
    LocalDaemonResponsePayloadWire::Error(unsupported_collection_error(
        request.collection,
    ))
}

fn handle_read_payload(
    request: LocalDaemonReadRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    match request {
        LocalDaemonReadRequestWire::NotificationList(_) => {
            notification_list_payload(request, state)
                .unwrap_or_else(LocalDaemonResponsePayloadWire::Error)
        }
        LocalDaemonReadRequestWire::NotificationDetail(request) => {
            notification_detail_payload(request.notification_id, state)
                .unwrap_or_else(LocalDaemonResponsePayloadWire::Error)
        }
        LocalDaemonReadRequestWire::NotificationCounts => state
            .projection_service
            .read_blocking(|db| notification_projection_counts(db.connection()))
            .map(|counts| {
                LocalDaemonResponsePayloadWire::Read(Box::new(
                    LocalDaemonReadResponseWire::NotificationCounts(counts),
                ))
            })
            .unwrap_or_else(|error| {
                LocalDaemonResponsePayloadWire::Error(projection_error(error))
            }),
        LocalDaemonReadRequestWire::NotificationPendingActions => {
            notification_pending_actions_payload(state)
                .unwrap_or_else(LocalDaemonResponsePayloadWire::Error)
        }
        other => LocalDaemonResponsePayloadWire::Error(unsupported_read_error(
            read_surface_name(&other),
        )),
    }
}

fn handle_write_payload(
    request: LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> LocalDaemonResponsePayloadWire {
    if request.surface.starts_with("changespec.") {
        return changespec_write_payload(request, state)
            .map(LocalDaemonResponsePayloadWire::Write)
            .unwrap_or_else(LocalDaemonResponsePayloadWire::Error);
    }
    if request.surface.starts_with("notifications.")
        || request.surface.starts_with("pending_actions.")
    {
        return notification_write_payload(request, state)
            .map(LocalDaemonResponsePayloadWire::Write)
            .unwrap_or_else(LocalDaemonResponsePayloadWire::Error);
    }
    if request.surface.starts_with("agents.") {
        return agent_write_payload(request, state)
            .map(LocalDaemonResponsePayloadWire::Write)
            .unwrap_or_else(LocalDaemonResponsePayloadWire::Error);
    }
    if request.surface == "beads" {
        return bead_write_payload(request, state)
            .map(LocalDaemonResponsePayloadWire::Write)
            .unwrap_or_else(LocalDaemonResponsePayloadWire::Error);
    }
    LocalDaemonResponsePayloadWire::Error(unsupported_write_error(&request))
}

#[derive(Debug, Deserialize)]
struct NotificationAppendWritePayload {
    notification: NotificationWire,
}

#[derive(Debug, Deserialize)]
struct NotificationStateUpdateWritePayload {
    update: NotificationStateUpdateWire,
}

#[derive(Debug, Deserialize)]
struct PendingActionWritePayload {
    action: PendingActionWire,
}

#[derive(Debug, Default, Deserialize)]
struct PendingActionCleanupWritePayload {
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    now_unix: Option<f64>,
}

struct PlannedNotificationWrite {
    event_request: EventAppendRequestWire,
    resource_handle: Option<String>,
    source_exports: Vec<SourceExportPlanWire>,
    projection_snapshot: Option<JsonValue>,
    changed: Option<bool>,
}

fn notification_write_payload(
    request: LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> Result<LocalDaemonWriteResponseWire, LocalDaemonErrorWire> {
    let planned = plan_notification_write(&request, state)?;
    let mut outcome = state
        .projection_service
        .write_blocking({
            let source_exports = planned.source_exports;
            let projection_snapshot = planned.projection_snapshot;
            let resource_handle = planned.resource_handle;
            move |db| {
                db.append_mutation_event_with_outbox(
                    planned.event_request,
                    resource_handle,
                    source_exports,
                    projection_snapshot,
                )
            }
        })
        .map_err(projection_write_error)?;
    outcome =
        apply_source_exports(state, outcome).map_err(projection_write_error)?;
    if let Some(changed) = planned.changed {
        outcome.changed = changed && !outcome.duplicate;
    }
    Ok(LocalDaemonWriteResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface: request.surface,
        outcome,
        fallback: LocalDaemonFallbackWire {
            available: false,
            reason: None,
            message: None,
        },
    })
}

fn plan_notification_write(
    request: &LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> Result<PlannedNotificationWrite, LocalDaemonErrorWire> {
    let paths = notification_source_paths(&state.paths.sase_home);
    match request.surface.as_str() {
        "notifications.append" => {
            let payload: NotificationAppendWritePayload =
                decode_surface_payload(request)?;
            let content = notification_append_jsonl(&payload.notification)
                .map_err(notification_plan_error)?;
            let path = paths.notifications_path.clone();
            let event_request = notification_append_event_request(
                notification_event_context(request, Some(path.clone())),
                payload.notification.clone(),
            )
            .map_err(notification_projection_plan_error)?;
            Ok(PlannedNotificationWrite {
                event_request,
                resource_handle: Some(payload.notification.id.clone()),
                source_exports: vec![source_export_plan(
                    &path,
                    SourceExportKindWire::JsonlAppend,
                    content,
                )],
                projection_snapshot: Some(json!({
                    "appended_count": 1,
                    "notification_id": payload.notification.id,
                })),
                changed: Some(true),
            })
        }
        "notifications.state_update" => {
            let payload: NotificationStateUpdateWritePayload =
                decode_surface_payload(request)?;
            let path = paths.notifications_path.clone();
            let (outcome, content) = plan_notification_state_update_export(
                Path::new(&path),
                &payload.update,
                true,
            )
            .map_err(notification_plan_error)?;
            let event_request = notification_state_update_event_request(
                notification_event_context(request, Some(path.clone())),
                payload.update,
                outcome.notifications.clone(),
            )
            .map_err(notification_projection_plan_error)?;
            let changed = outcome.changed_count > 0 || outcome.rewritten;
            Ok(PlannedNotificationWrite {
                event_request,
                resource_handle: notification_update_resource_handle(&outcome),
                source_exports: if changed {
                    vec![source_export_plan(
                        &path,
                        SourceExportKindWire::AtomicJson,
                        content,
                    )]
                } else {
                    Vec::new()
                },
                projection_snapshot: Some(json!({"outcome": outcome})),
                changed: Some(changed),
            })
        }
        "pending_actions.register" => pending_action_write_plan(
            request,
            state,
            PendingActionWriteMode::Register,
        ),
        "pending_actions.update" => pending_action_write_plan(
            request,
            state,
            PendingActionWriteMode::Update,
        ),
        "pending_actions.cleanup" => {
            pending_action_cleanup_write_plan(request, state)
        }
        _ => Err(unsupported_write_error(request)),
    }
}

#[derive(Clone, Copy)]
enum PendingActionWriteMode {
    Register,
    Update,
}

fn pending_action_write_plan(
    request: &LocalDaemonWriteRequestWire,
    state: &DaemonState,
    mode: PendingActionWriteMode,
) -> Result<PlannedNotificationWrite, LocalDaemonErrorWire> {
    let payload: PendingActionWritePayload = decode_surface_payload(request)?;
    let path = pending_action_store_path(&state.paths.sase_home);
    let store = read_pending_action_store(&path, None)
        .map_err(notification_plan_error)?;
    let next = match mode {
        PendingActionWriteMode::Register => {
            register_pending_action_in_store(store, &payload.action)
        }
        PendingActionWriteMode::Update => {
            update_pending_action_in_store(store, &payload.action)
        }
    };
    let content =
        pending_action_store_json(&next).map_err(notification_plan_error)?;
    let source_path = path.to_string_lossy().to_string();
    let context =
        notification_event_context(request, Some(source_path.clone()));
    let event_request = match mode {
        PendingActionWriteMode::Register => {
            pending_action_register_event_request(
                context,
                payload.action.clone(),
            )
        }
        PendingActionWriteMode::Update => {
            pending_action_update_event_request(context, payload.action.clone())
        }
    }
    .map_err(notification_projection_plan_error)?;
    Ok(PlannedNotificationWrite {
        event_request,
        resource_handle: Some(payload.action.notification_id.clone()),
        source_exports: vec![source_export_plan(
            &source_path,
            SourceExportKindWire::AtomicJson,
            content,
        )],
        projection_snapshot: Some(json!({"store": next})),
        changed: Some(true),
    })
}

fn pending_action_cleanup_write_plan(
    request: &LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> Result<PlannedNotificationWrite, LocalDaemonErrorWire> {
    let payload: PendingActionCleanupWritePayload =
        decode_surface_payload(request)?;
    let path = pending_action_store_path(&state.paths.sase_home);
    let mut store = read_pending_action_store(&path, None)
        .map_err(notification_plan_error)?;
    let mut prefixes = payload.prefixes;
    if let Some(now_unix) = payload.now_unix {
        prefixes.extend(cleanup_stale_pending_actions_in_store(
            &mut store, now_unix,
        ));
    }
    prefixes.sort();
    prefixes.dedup();
    for prefix in &prefixes {
        store.actions.remove(prefix);
    }
    let content =
        pending_action_store_json(&store).map_err(notification_plan_error)?;
    let source_path = path.to_string_lossy().to_string();
    let event_request = pending_action_cleanup_event_request(
        notification_event_context(request, Some(source_path.clone())),
        prefixes.clone(),
    )
    .map_err(notification_projection_plan_error)?;
    let changed = !prefixes.is_empty();
    Ok(PlannedNotificationWrite {
        event_request,
        resource_handle: Some("pending_actions".to_string()),
        source_exports: if changed {
            vec![source_export_plan(
                &source_path,
                SourceExportKindWire::AtomicJson,
                content,
            )]
        } else {
            Vec::new()
        },
        projection_snapshot: Some(
            json!({"store": store, "prefixes": prefixes}),
        ),
        changed: Some(changed),
    })
}

fn decode_surface_payload<T>(
    request: &LocalDaemonWriteRequestWire,
) -> Result<T, LocalDaemonErrorWire>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(request.payload.clone()).map_err(|error| {
        local_error(
            LocalDaemonErrorCodeWire::InvalidRequest,
            format!(
                "local daemon write surface '{}' has invalid payload: {error}",
                request.surface
            ),
            false,
            Some("payload".to_string()),
            None,
        )
    })
}

fn notification_event_context(
    request: &LocalDaemonWriteRequestWire,
    source_path: Option<String>,
) -> NotificationProjectionEventContextWire {
    NotificationProjectionEventContextWire {
        created_at: Some(
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        ),
        source: EventSourceWire {
            source_type: request.actor.actor_type.clone(),
            name: request.actor.name.clone(),
            version: request.actor.version.clone(),
            runtime: request.actor.runtime.clone(),
            ..EventSourceWire::default()
        },
        host_id: "local-daemon".to_string(),
        project_id: request.project_id.clone(),
        idempotency_key: Some(request.idempotency_key.clone()),
        source_path,
        source_revision: None,
    }
}

fn source_export_plan(
    target_path: &str,
    kind: SourceExportKindWire,
    content: String,
) -> SourceExportPlanWire {
    let mut plan = SourceExportPlanWire::new(target_path, kind, content);
    plan.expected_fingerprint =
        source_fingerprint_from_path(Path::new(target_path), true);
    plan
}

fn notification_update_resource_handle(
    outcome: &NotificationUpdateOutcomeWire,
) -> Option<String> {
    if outcome.matched_count == 1 {
        outcome
            .notifications
            .first()
            .map(|notification| notification.id.clone())
    } else {
        Some("notifications".to_string())
    }
}

fn notification_plan_error(error: String) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::InvalidRequest,
        error,
        false,
        Some("payload".to_string()),
        Some(json!({"fallbackable": true})),
    )
}

fn notification_projection_plan_error(
    error: sase_core::projections::ProjectionError,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::InvalidRequest,
        format!("invalid notification write event: {error}"),
        false,
        Some("payload".to_string()),
        Some(json!({"fallbackable": true})),
    )
}

fn bead_write_payload(
    request: LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> Result<LocalDaemonWriteResponseWire, LocalDaemonErrorWire> {
    let plan_request: BeadMutationWriteRequestWire =
        serde_json::from_value(request.payload.clone()).map_err(|error| {
            local_error(
                LocalDaemonErrorCodeWire::InvalidRequest,
                format!("invalid bead mutation payload: {error}"),
                false,
                Some("payload".to_string()),
                None,
            )
        })?;
    let write_plan =
        plan_bead_source_export_mutation(plan_request).map_err(|error| {
            local_error(
                LocalDaemonErrorCodeWire::InvalidRequest,
                format!("failed to plan bead mutation: {error}"),
                false,
                Some("payload".to_string()),
                None,
            )
        })?;
    let source_path = write_plan
        .source_exports
        .iter()
        .find(|export| export.target_path.ends_with("issues.jsonl"))
        .map(|export| export.target_path.clone());
    let context = BeadProjectionEventContextWire {
        created_at: Some(
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        ),
        source: EventSourceWire {
            source_type: request.actor.actor_type.clone(),
            name: request.actor.name.clone(),
            version: request.actor.version.clone(),
            runtime: request.actor.runtime.clone(),
            ..EventSourceWire::default()
        },
        host_id: "local-daemon".to_string(),
        project_id: request.project_id.clone(),
        idempotency_key: Some(request.idempotency_key.clone()),
        causality: Vec::new(),
        source_path,
        source_revision: None,
    };
    let outcome = state
        .projection_service
        .write_blocking(move |db| {
            db.append_bead_mutation_event_with_exports(
                context,
                write_plan.outcome,
                write_plan.source_exports,
            )
        })
        .map_err(projection_write_error)?;
    let outcome =
        apply_source_exports(state, outcome).map_err(projection_write_error)?;
    if let Some(report) = outcome
        .source_exports
        .iter()
        .find(|report| report.status != SourceExportStatusWire::Applied)
    {
        return Err(source_export_repair_error(report, &outcome));
    }
    Ok(LocalDaemonWriteResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface: request.surface,
        outcome,
        fallback: fallback_unavailable(),
    })
}

fn changespec_write_payload(
    request: LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> Result<LocalDaemonWriteResponseWire, LocalDaemonErrorWire> {
    preflight_source_exports(&request)?;
    let event_request = changespec_event_request_from_write(&request)?;
    let resource_handle = changespec_resource_handle(&request);
    let outcome = state
        .projection_service
        .write_blocking({
            let source_exports = request.source_exports.clone();
            let projection_snapshot = Some(json!({
                "surface": request.surface,
                "payload": request.payload,
            }));
            move |db| {
                db.append_mutation_event_with_outbox(
                    event_request,
                    resource_handle,
                    source_exports,
                    projection_snapshot,
                )
            }
        })
        .map_err(projection_write_error)?;
    let outcome =
        apply_source_exports(state, outcome).map_err(projection_write_error)?;
    if let Some(report) = outcome
        .source_exports
        .iter()
        .find(|report| report.status != SourceExportStatusWire::Applied)
    {
        return Err(source_export_repair_error(report, &outcome));
    }
    Ok(LocalDaemonWriteResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface: request.surface,
        outcome,
        fallback: fallback_unavailable(),
    })
}

fn changespec_event_request_from_write(
    request: &LocalDaemonWriteRequestWire,
) -> Result<EventAppendRequestWire, LocalDaemonErrorWire> {
    let spec = required_payload_object(request, "spec")?;
    let name = spec
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| invalid_write_payload(request, "spec.name"))?;
    let source_path = spec
        .get("file_path")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    let event_type = changespec_event_type(request)?;
    let payload = changespec_event_payload(request, spec)?;
    let event = EventAppendRequestWire {
        created_at: Some(
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        ),
        source: EventSourceWire {
            source_type: request.actor.actor_type.clone(),
            name: request.actor.name.clone(),
            version: request.actor.version.clone(),
            runtime: request.actor.runtime.clone(),
            ..EventSourceWire::default()
        },
        host_id: "local-daemon".to_string(),
        project_id: request.project_id.clone(),
        event_type: event_type.to_string(),
        payload,
        idempotency_key: Some(request.idempotency_key.clone()),
        causality: vec![],
        source_path,
        source_revision: None,
    };
    debug!(
        surface = %request.surface,
        changespec = %name,
        event_type = %event.event_type,
        "prepared changespec write event"
    );
    Ok(event)
}

fn changespec_event_type(
    request: &LocalDaemonWriteRequestWire,
) -> Result<&'static str, LocalDaemonErrorWire> {
    match request.surface.as_str() {
        "changespec.status" => Ok(CHANGESPEC_STATUS_TRANSITIONED),
        "changespec.field" | "changespec.lifecycle_name" => {
            Ok(CHANGESPEC_SPEC_UPDATED)
        }
        "changespec.comments"
        | "changespec.hooks"
        | "changespec.hook_suffix"
        | "changespec.mentors"
        | "changespec.mentor_status"
        | "changespec.timestamps" => Ok(CHANGESPEC_SECTIONS_UPDATED),
        "changespec.created" => Ok(CHANGESPEC_SPEC_CREATED),
        "changespec.active_archive_moved" => {
            Ok(CHANGESPEC_ACTIVE_ARCHIVE_MOVED)
        }
        _ => Err(unsupported_write_error(request)),
    }
}

fn changespec_event_payload(
    request: &LocalDaemonWriteRequestWire,
    spec: &JsonValue,
) -> Result<JsonValue, LocalDaemonErrorWire> {
    let is_archive = request
        .payload
        .get("is_archive")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    match request.surface.as_str() {
        "changespec.status" => Ok(json!({
            "schema_version": 1,
            "spec": spec,
            "from_status": required_payload_string(request, "from_status")?,
            "to_status": required_payload_string(request, "to_status")?,
            "is_archive": is_archive,
        })),
        "changespec.field"
        | "changespec.lifecycle_name"
        | "changespec.created" => Ok(json!({
            "schema_version": 1,
            "spec": spec,
            "is_archive": is_archive,
        })),
        "changespec.comments"
        | "changespec.hooks"
        | "changespec.hook_suffix"
        | "changespec.mentors"
        | "changespec.mentor_status"
        | "changespec.timestamps" => Ok(json!({
            "schema_version": 1,
            "spec": spec,
            "section_names": request
                .payload
                .get("section_names")
                .cloned()
                .unwrap_or_else(|| default_changespec_section_names(request)),
            "is_archive": is_archive,
        })),
        "changespec.active_archive_moved" => Ok(json!({
            "schema_version": 1,
            "spec": spec,
            "from_path": required_payload_string(request, "from_path")?,
            "to_path": required_payload_string(request, "to_path")?,
            "is_archive": is_archive,
        })),
        _ => Err(unsupported_write_error(request)),
    }
}

fn default_changespec_section_names(
    request: &LocalDaemonWriteRequestWire,
) -> JsonValue {
    match request.surface.as_str() {
        "changespec.hooks" | "changespec.hook_suffix" => json!(["hooks"]),
        "changespec.mentors" | "changespec.mentor_status" => json!(["mentors"]),
        "changespec.timestamps" => json!(["timestamps"]),
        _ => json!(["comments"]),
    }
}

fn changespec_resource_handle(
    request: &LocalDaemonWriteRequestWire,
) -> Option<String> {
    request
        .payload
        .get("spec")
        .and_then(|spec| spec.get("name"))
        .and_then(JsonValue::as_str)
        .map(|name| changespec_handle(&request.project_id, name))
}

fn required_payload_object<'a>(
    request: &'a LocalDaemonWriteRequestWire,
    field: &str,
) -> Result<&'a JsonValue, LocalDaemonErrorWire> {
    request
        .payload
        .get(field)
        .filter(|value| value.is_object())
        .ok_or_else(|| invalid_write_payload(request, field))
}

fn invalid_write_payload(
    request: &LocalDaemonWriteRequestWire,
    field: &str,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::InvalidRequest,
        format!(
            "local daemon write surface '{}' has invalid payload field '{}'",
            request.surface, field
        ),
        false,
        Some(format!("payload.{field}")),
        None,
    )
}

fn preflight_source_exports(
    request: &LocalDaemonWriteRequestWire,
) -> Result<(), LocalDaemonErrorWire> {
    for export in &request.source_exports {
        let Some(expected) = export.expected_fingerprint.as_ref() else {
            continue;
        };
        let actual =
            source_fingerprint_from_path(Path::new(&export.target_path), true);
        if !source_fingerprint_matches(actual.as_ref(), expected) {
            return Err(local_error(
                LocalDaemonErrorCodeWire::ConflictStaleSource,
                "source fingerprint changed before mutation",
                false,
                Some(export.target_path.clone()),
                Some(json!({
                    "expected_fingerprint": expected,
                    "actual_fingerprint": actual,
                    "fallbackable": false,
                })),
            ));
        }
    }
    Ok(())
}

fn source_fingerprint_matches(
    actual: Option<&SourceFingerprintWire>,
    expected: &SourceFingerprintWire,
) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    expected
        .file_size
        .map_or(true, |value| actual.file_size == Some(value))
        && expected
            .modified_at_unix_millis
            .map_or(true, |value| actual.modified_at_unix_millis == Some(value))
        && expected
            .inode
            .map_or(true, |value| actual.inode == Some(value))
        && expected
            .content_sha256
            .as_ref()
            .map_or(true, |value| actual.content_sha256.as_ref() == Some(value))
}

fn apply_source_exports(
    state: &DaemonState,
    mut outcome: LocalDaemonMutationOutcomeWire,
) -> Result<
    LocalDaemonMutationOutcomeWire,
    crate::projection_service::ProjectionServiceError,
> {
    let export_ids: Vec<i64> = outcome
        .source_exports
        .iter()
        .filter(|export| export.status != SourceExportStatusWire::Applied)
        .map(|export| export.export_id)
        .collect();
    if export_ids.is_empty() {
        return Ok(outcome);
    }
    let mut reports = Vec::with_capacity(export_ids.len());
    for export_id in export_ids {
        reports.push(state.projection_service.write_blocking(move |db| {
            db.retry_source_export_once(export_id)
        })?);
    }
    outcome.source_exports = reports;
    Ok(outcome)
}

fn source_export_repair_error(
    report: &SourceExportReportWire,
    outcome: &LocalDaemonMutationOutcomeWire,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::ExportPendingRepair,
        format!(
            "source export for '{}' is {:?}",
            report.target_path, report.status
        ),
        true,
        Some(report.target_path.clone()),
        Some(json!({
            "fallbackable": false,
            "event_seq": outcome.event_seq,
            "export_id": report.export_id,
            "status": report.status,
            "message": report.message,
        })),
    )
}

fn agent_write_payload(
    request: LocalDaemonWriteRequestWire,
    state: &DaemonState,
) -> Result<LocalDaemonWriteResponseWire, LocalDaemonErrorWire> {
    let event_request = agent_event_request_from_write(&request)?;
    let resource_handle = agent_resource_handle(&request);
    let outcome = state
        .projection_service
        .write_blocking({
            let source_exports = request.source_exports.clone();
            let projection_snapshot = Some(json!({
                "surface": request.surface,
                "payload": request.payload,
            }));
            move |db| {
                db.append_mutation_event_with_outbox(
                    event_request,
                    resource_handle,
                    source_exports,
                    projection_snapshot,
                )
            }
        })
        .map_err(projection_write_error)?;
    let outcome =
        apply_source_exports(state, outcome).map_err(projection_write_error)?;
    Ok(LocalDaemonWriteResponseWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface: request.surface,
        outcome,
        fallback: LocalDaemonFallbackWire {
            available: false,
            reason: None,
            message: None,
        },
    })
}

fn agent_event_request_from_write(
    request: &LocalDaemonWriteRequestWire,
) -> Result<EventAppendRequestWire, LocalDaemonErrorWire> {
    let context = agent_event_context(request);
    match request.surface.as_str() {
        "agents.dismissed_identity" => {
            let identity: AgentDismissedIdentityWire =
                decode_write_payload(request, "identity")?;
            agent_dismissed_identity_changed_event_request(context, identity)
        }
        "agents.archive_bundle" => {
            let archive: AgentArchiveSummaryWire =
                decode_write_payload(request, "archive")?;
            let bundle = request.payload.get("bundle").cloned();
            agent_archive_bundle_indexed_event_request(context, archive, bundle)
        }
        "agents.archive_revived" => {
            let bundle_path = required_payload_string(request, "bundle_path")?;
            let revived_at = required_payload_string(request, "revived_at")?;
            agent_archive_bundle_revived_event_request(
                context,
                bundle_path,
                revived_at,
            )
        }
        "agents.archive_purged" => {
            let bundle_path = required_payload_string(request, "bundle_path")?;
            agent_archive_bundle_purged_event_request(context, bundle_path)
        }
        "agents.artifact_associated" => {
            let artifact: AgentArtifactAssociationWire =
                decode_write_payload(request, "artifact")?;
            agent_artifact_associated_event_request(context, artifact)
        }
        "agents.cleanup_result" => agent_cleanup_result_recorded_event_request(
            context,
            request.payload.clone(),
        ),
        _ => {
            return Err(unsupported_write_error(request));
        }
    }
    .map_err(|error| {
        local_error(
            LocalDaemonErrorCodeWire::InvalidRequest,
            format!("invalid agent write payload: {error}"),
            false,
            Some("payload".to_string()),
            None,
        )
    })
}

fn agent_event_context(
    request: &LocalDaemonWriteRequestWire,
) -> AgentProjectionEventContextWire {
    AgentProjectionEventContextWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        created_at: Some(
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        ),
        source: EventSourceWire {
            source_type: request.actor.actor_type.clone(),
            name: request.actor.name.clone(),
            version: request.actor.version.clone(),
            runtime: request.actor.runtime.clone(),
            ..EventSourceWire::default()
        },
        host_id: "local-daemon".to_string(),
        project_id: request.project_id.clone(),
        idempotency_key: Some(request.idempotency_key.clone()),
        causality: Vec::new(),
        source_path: request
            .source_exports
            .first()
            .map(|export| export.target_path.clone()),
        source_revision: None,
    }
}

fn decode_write_payload<T>(
    request: &LocalDaemonWriteRequestWire,
    field: &str,
) -> Result<T, LocalDaemonErrorWire>
where
    T: serde::de::DeserializeOwned,
{
    let value = request.payload.get(field).cloned().ok_or_else(|| {
        local_error(
            LocalDaemonErrorCodeWire::InvalidRequest,
            format!("agent write payload missing '{field}'"),
            false,
            Some(format!("payload.{field}")),
            None,
        )
    })?;
    serde_json::from_value(value).map_err(|error| {
        local_error(
            LocalDaemonErrorCodeWire::InvalidRequest,
            format!("agent write payload field '{field}' is invalid: {error}"),
            false,
            Some(format!("payload.{field}")),
            None,
        )
    })
}

fn required_payload_string(
    request: &LocalDaemonWriteRequestWire,
    field: &str,
) -> Result<String, LocalDaemonErrorWire> {
    request
        .payload
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            local_error(
                LocalDaemonErrorCodeWire::InvalidRequest,
                format!("agent write payload missing string '{field}'"),
                false,
                Some(format!("payload.{field}")),
                None,
            )
        })
}

fn agent_resource_handle(
    request: &LocalDaemonWriteRequestWire,
) -> Option<String> {
    match request.surface.as_str() {
        "agents.dismissed_identity" => request
            .payload
            .get("identity")
            .and_then(|value| value.get("raw_suffix"))
            .and_then(JsonValue::as_str)
            .map(|suffix| format!("dismissed:{suffix}")),
        "agents.archive_bundle" => request
            .payload
            .get("archive")
            .and_then(|value| value.get("bundle_path"))
            .and_then(JsonValue::as_str)
            .map(|path| format!("archive:{path}")),
        "agents.archive_revived" | "agents.archive_purged" => request
            .payload
            .get("bundle_path")
            .and_then(JsonValue::as_str)
            .map(|path| format!("archive:{path}")),
        "agents.artifact_associated" => request
            .payload
            .get("artifact")
            .and_then(|value| value.get("artifact_path"))
            .and_then(JsonValue::as_str)
            .map(|path| format!("artifact:{path}")),
        "agents.cleanup_result" => Some("cleanup_result".to_string()),
        _ => None,
    }
}

fn notification_list_payload(
    request: LocalDaemonReadRequestWire,
    state: &DaemonState,
) -> Result<LocalDaemonResponsePayloadWire, LocalDaemonErrorWire> {
    let LocalDaemonReadRequestWire::NotificationList(request) = request else {
        return Err(local_error(
            LocalDaemonErrorCodeWire::InvalidRequest,
            "notification list dispatch received the wrong request shape",
            false,
            Some("payload".to_string()),
            None,
        ));
    };
    let snapshot_id = next_snapshot_id();
    let max_payload_bytes = LOCAL_DAEMON_MAX_PAYLOAD_BYTES;
    state
        .projection_service
        .read_blocking(move |db| {
            notification_projection_page(
                db.connection(),
                &request,
                snapshot_id,
                max_payload_bytes,
            )
        })
        .map(|response| {
            LocalDaemonResponsePayloadWire::Read(Box::new(
                LocalDaemonReadResponseWire::NotificationList(response),
            ))
        })
        .map_err(projection_error)
}

fn notification_generic_list_payload(
    request: LocalDaemonListRequestWire,
    state: &DaemonState,
) -> Result<LocalDaemonResponsePayloadWire, LocalDaemonErrorWire> {
    let stable_handle = request.stable_handle.clone();
    let max_payload_bytes = request
        .max_payload_bytes
        .unwrap_or(LOCAL_DAEMON_MAX_PAYLOAD_BYTES);
    let snapshot_id =
        request.snapshot_id.clone().unwrap_or_else(next_snapshot_id);
    let read_request = notification_list_request_from_generic(&request);
    state
        .projection_service
        .read_blocking(move |db| {
            notification_projection_page(
                db.connection(),
                &read_request,
                snapshot_id,
                max_payload_bytes,
            )
        })
        .map(|response| {
            let items = response
                .notifications
                .into_iter()
                .map(|notification| {
                    let handle = notification.id.clone();
                    Ok(LocalDaemonListItemWire {
                        handle,
                        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                        summary: serde_json::to_value(notification)?,
                    })
                })
                .collect::<Result<Vec<_>, serde_json::Error>>()
                .map_err(|error| {
                    local_error(
                        LocalDaemonErrorCodeWire::Internal,
                        format!(
                            "failed to serialize notification list item: {error}"
                        ),
                        false,
                        Some("notifications".to_string()),
                        None,
                    )
                })?;
            Ok(LocalDaemonResponsePayloadWire::List(
                LocalDaemonListResponseWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    collection: LocalDaemonCollectionWire::Notifications,
                    snapshot_id: response.snapshot.snapshot_id,
                    items,
                    next_cursor: response.page.next_cursor,
                    stable_handle,
                    bounded: LocalDaemonPayloadBoundWire {
                        max_payload_bytes,
                        truncated: response.bounded.truncated,
                    },
                },
            ))
        })
        .map_err(projection_error)?
}

fn notification_detail_payload(
    notification_id: String,
    state: &DaemonState,
) -> Result<LocalDaemonResponsePayloadWire, LocalDaemonErrorWire> {
    let snapshot_id = next_snapshot_id();
    state
        .projection_service
        .read_blocking(move |db| {
            let notification = notification_projection_detail(
                db.connection(),
                &notification_id,
            )?;
            Ok(NotificationReadDetailResponseWire {
                schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                snapshot: ProjectionSnapshotReadWire {
                    schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                    snapshot_id,
                },
                notification,
                bounded: ProjectionPayloadBoundWire {
                    schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                    max_payload_bytes: LOCAL_DAEMON_MAX_PAYLOAD_BYTES,
                    truncated: false,
                },
            })
        })
        .map(|response| {
            LocalDaemonResponsePayloadWire::Read(Box::new(
                LocalDaemonReadResponseWire::NotificationDetail(response),
            ))
        })
        .map_err(projection_error)
}

fn notification_pending_actions_payload(
    state: &DaemonState,
) -> Result<LocalDaemonResponsePayloadWire, LocalDaemonErrorWire> {
    let snapshot_id = next_snapshot_id();
    state
        .projection_service
        .read_blocking(move |db| {
            let store = projected_pending_action_store(db.connection())?;
            let actions = pending_action_store_actions(&store);
            Ok(NotificationPendingActionsReadResponseWire {
                schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                snapshot: ProjectionSnapshotReadWire {
                    schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                    snapshot_id,
                },
                store,
                actions,
                bounded: ProjectionPayloadBoundWire {
                    schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                    max_payload_bytes: LOCAL_DAEMON_MAX_PAYLOAD_BYTES,
                    truncated: false,
                },
            })
        })
        .map(|response| {
            LocalDaemonResponsePayloadWire::Read(Box::new(
                LocalDaemonReadResponseWire::NotificationPendingActions(
                    response,
                ),
            ))
        })
        .map_err(projection_error)
}

fn pending_action_store_actions(
    store: &PendingActionStoreWire,
) -> Vec<sase_core::notifications::PendingActionWire> {
    store.actions.values().cloned().collect()
}

fn notification_list_request_from_generic(
    request: &LocalDaemonListRequestWire,
) -> crate::wire::NotificationReadListRequestWire {
    let filters = request.filters.as_ref();
    crate::wire::NotificationReadListRequestWire {
        schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
        page: ProjectionPageRequestWire {
            schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
            limit: request.page.limit.clamp(1, LOCAL_DAEMON_MAX_PAGE_LIMIT),
            cursor: request.page.cursor.clone(),
        },
        include_dismissed: filters
            .and_then(|value| value.get("include_dismissed"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        query: filters
            .and_then(|value| value.get("query"))
            .and_then(JsonValue::as_str)
            .map(str::to_string),
        sender: filters
            .and_then(|value| value.get("sender"))
            .and_then(JsonValue::as_str)
            .map(str::to_string),
        unread: filters
            .and_then(|value| value.get("unread"))
            .and_then(JsonValue::as_bool),
    }
}

fn unsupported_collection_error(
    collection: LocalDaemonCollectionWire,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::UnsupportedCapability,
        format!("local daemon collection '{collection:?}' is not implemented"),
        false,
        Some("collection".to_string()),
        Some(json!({"capability": collection_capability(collection)})),
    )
}

fn unsupported_read_error(surface: &str) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::UnsupportedCapability,
        format!("local daemon read surface '{surface}' is not implemented"),
        false,
        Some("payload".to_string()),
        Some(json!({"capability": format!("{surface}.read")})),
    )
}

fn unsupported_write_error(
    request: &LocalDaemonWriteRequestWire,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::UnsupportedMutation,
        format!(
            "local daemon write surface '{}' is not implemented",
            request.surface
        ),
        false,
        Some("payload.surface".to_string()),
        Some(json!({
            "capability": format!("{}.write", request.surface),
            "idempotency_key": request.idempotency_key,
            "fallbackable": true,
        })),
    )
}

fn projection_error(
    error: crate::projection_service::ProjectionServiceError,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::ProjectionDegraded,
        format!("projection read failed: {error}"),
        true,
        Some("projection_db".to_string()),
        None,
    )
}

fn projection_write_error(
    error: crate::projection_service::ProjectionServiceError,
) -> LocalDaemonErrorWire {
    local_error(
        LocalDaemonErrorCodeWire::ExportPendingRepair,
        format!("projection write failed: {error}"),
        true,
        Some("projection_db".to_string()),
        Some(json!({"fallbackable": true})),
    )
}

fn read_surface_name(request: &LocalDaemonReadRequestWire) -> &'static str {
    match request {
        LocalDaemonReadRequestWire::ChangespecList(_) => "changespecs.list",
        LocalDaemonReadRequestWire::ChangespecSearch(_) => "changespecs.search",
        LocalDaemonReadRequestWire::ChangespecDetail(_) => "changespecs.detail",
        LocalDaemonReadRequestWire::AgentActive(_) => "agents.active",
        LocalDaemonReadRequestWire::AgentRecent(_) => "agents.recent",
        LocalDaemonReadRequestWire::AgentArchive(_) => "agents.archive",
        LocalDaemonReadRequestWire::AgentSearch(_) => "agents.search",
        LocalDaemonReadRequestWire::AgentDetail(_) => "agents.detail",
        LocalDaemonReadRequestWire::NotificationList(_) => "notifications.list",
        LocalDaemonReadRequestWire::NotificationDetail(_) => {
            "notifications.detail"
        }
        LocalDaemonReadRequestWire::NotificationCounts => {
            "notifications.counts"
        }
        LocalDaemonReadRequestWire::NotificationPendingActions => {
            "notifications.pending_actions"
        }
        LocalDaemonReadRequestWire::BeadList(_) => "beads.list",
        LocalDaemonReadRequestWire::BeadReady(_) => "beads.ready",
        LocalDaemonReadRequestWire::BeadBlocked(_) => "beads.blocked",
        LocalDaemonReadRequestWire::BeadShow(_) => "beads.show",
        LocalDaemonReadRequestWire::BeadStats(_) => "beads.stats",
        LocalDaemonReadRequestWire::XpromptCatalog(_) => "catalogs.xprompts",
        LocalDaemonReadRequestWire::EditorCatalog(_) => "catalogs.editor",
        LocalDaemonReadRequestWire::SnippetCatalog(_) => "catalogs.snippets",
        LocalDaemonReadRequestWire::FileHistory(_) => "catalogs.file_history",
    }
}

fn collection_capability(
    collection: LocalDaemonCollectionWire,
) -> &'static str {
    match collection {
        LocalDaemonCollectionWire::Agents => "agents.read",
        LocalDaemonCollectionWire::Artifacts => "agents.read",
        LocalDaemonCollectionWire::Beads => "beads.read",
        LocalDaemonCollectionWire::Changespecs => "changespecs.read",
        LocalDaemonCollectionWire::Notifications => "notifications.read",
        LocalDaemonCollectionWire::Workflows => "catalogs.read",
        LocalDaemonCollectionWire::Xprompts => "catalogs.read",
        LocalDaemonCollectionWire::Indexing => "indexing.status",
        LocalDaemonCollectionWire::Mocked => "mocked.list",
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
        "writes.contract".to_string(),
        "agents.write".to_string(),
        "beads.write".to_string(),
        "changespecs.write".to_string(),
        "mocked.list".to_string(),
        "mocked.events".to_string(),
        "notifications.read".to_string(),
        "notifications.write".to_string(),
        "projection.rebuild".to_string(),
        "indexing.status".to_string(),
        "indexing.rebuild".to_string(),
        "indexing.verify".to_string(),
        "indexing.diff".to_string(),
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
        LocalDaemonResponsePayloadWire::Read(response) => {
            read_response_snapshot_id(response)
        }
        LocalDaemonResponsePayloadWire::Events(response) => {
            Some(response.snapshot_id.clone())
        }
        _ => None,
    }
}

fn read_response_snapshot_id(
    response: &LocalDaemonReadResponseWire,
) -> Option<String> {
    match response {
        LocalDaemonReadResponseWire::ChangespecList(response)
        | LocalDaemonReadResponseWire::ChangespecSearch(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::ChangespecDetail(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::AgentActive(response)
        | LocalDaemonReadResponseWire::AgentRecent(response)
        | LocalDaemonReadResponseWire::AgentSearch(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::AgentArchive(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::AgentDetail(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::NotificationList(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::NotificationDetail(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::NotificationCounts(_) => None,
        LocalDaemonReadResponseWire::NotificationPendingActions(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::BeadList(response)
        | LocalDaemonReadResponseWire::BeadReady(response)
        | LocalDaemonReadResponseWire::BeadBlocked(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::BeadShow(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::BeadStats(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
        LocalDaemonReadResponseWire::XpromptCatalog(response)
        | LocalDaemonReadResponseWire::EditorCatalog(response)
        | LocalDaemonReadResponseWire::SnippetCatalog(response)
        | LocalDaemonReadResponseWire::FileHistory(response) => {
            Some(response.snapshot.snapshot_id.clone())
        }
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
            LocalDaemonPageRequestWire, MutationActorWire,
            ProjectionPageRequestWire, LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        },
    };
    use sase_core::notifications::{
        read_notifications_snapshot, NotificationStateUpdateWire,
        NotificationWire,
    };
    use sase_core::projections::{
        notification_append_event_request, EventSourceWire,
        NotificationProjectionEventContextWire,
    };
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    use super::*;

    fn test_state() -> DaemonState {
        test_state_with_projection(ProjectionService::unavailable(
            PathBuf::from("/tmp/sase-run/projections/projection.sqlite"),
            "test projection service unavailable",
        ))
    }

    fn test_state_with_projection(
        projection_service: ProjectionService,
    ) -> DaemonState {
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
            projection_service,
            indexing_service: crate::indexer::IndexingService::disabled(
                crate::metrics::DaemonMetrics::default(),
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
    fn unsupported_list_collection_returns_fallback_error() {
        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::List(LocalDaemonListRequestWire {
                collection: LocalDaemonCollectionWire::Agents,
                page: LocalDaemonPageRequestWire {
                    limit: LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
                    cursor: None,
                },
                snapshot_id: None,
                stable_handle: None,
                max_payload_bytes: None,
                filters: None,
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &test_state());

        match response.payload {
            LocalDaemonResponsePayloadWire::Error(error) => {
                assert_eq!(
                    error.code,
                    LocalDaemonErrorCodeWire::UnsupportedCapability
                );
                assert!(error.fallback.available);
            }
            other => {
                panic!("expected unsupported collection error, got {other:?}")
            }
        }
    }

    #[test]
    fn unsupported_write_surface_returns_typed_fallback_error() {
        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Write(LocalDaemonWriteRequestWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface: "notifications.mark_read".to_string(),
                project_id: "project-a".to_string(),
                idempotency_key: "write-key-1".to_string(),
                actor: MutationActorWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    actor_type: "test".to_string(),
                    name: "local-transport-test".to_string(),
                    version: None,
                    runtime: None,
                },
                payload: json!({"notification_id": "n1"}),
                expected_source_fingerprints: vec![],
                source_exports: vec![],
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &test_state());

        match response.payload {
            LocalDaemonResponsePayloadWire::Error(error) => {
                assert_eq!(
                    error.code,
                    LocalDaemonErrorCodeWire::UnsupportedMutation
                );
                assert!(error.fallback.available);
                assert_eq!(
                    error.details.unwrap()["idempotency_key"],
                    json!("write-key-1")
                );
            }
            other => {
                panic!("expected unsupported write error, got {other:?}")
            }
        }
    }

    #[test]
    fn changespec_status_write_updates_projection_and_source_export() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let projection_service = runtime.block_on(
            ProjectionService::initialize(dir.path().join("projection.sqlite")),
        );
        let state = test_state_with_projection(projection_service.clone());
        let project_dir = dir.path().join("project");
        std::fs::create_dir_all(&project_dir).unwrap();
        let project_file = project_dir.join("project.sase");
        let original =
            "NAME: alpha\nDESCRIPTION: demo\nSTATUS: WIP\n".to_string();
        let updated =
            "NAME: alpha\nDESCRIPTION: demo\nSTATUS: Draft\n".to_string();
        std::fs::write(&project_file, &original).unwrap();

        let spec = sase_core::parser::parse_project_bytes(
            &project_file.display().to_string(),
            updated.as_bytes(),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let mut export = SourceExportPlanWire::new(
            project_file.display().to_string(),
            SourceExportKindWire::ProjectFile,
            updated.clone(),
        );
        export.expected_fingerprint =
            source_fingerprint_from_path(&project_file, true);

        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Write(LocalDaemonWriteRequestWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface: "changespec.status".to_string(),
                project_id: "project".to_string(),
                idempotency_key: "changespec-status-alpha".to_string(),
                actor: MutationActorWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    actor_type: "test".to_string(),
                    name: "local-transport-test".to_string(),
                    version: None,
                    runtime: None,
                },
                payload: json!({
                    "spec": spec,
                    "from_status": "WIP",
                    "to_status": "Draft",
                    "is_archive": false,
                }),
                expected_source_fingerprints: vec![],
                source_exports: vec![export],
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &state);

        match response.payload {
            LocalDaemonResponsePayloadWire::Write(write) => {
                assert_eq!(write.surface, "changespec.status");
                assert_eq!(
                    write.outcome.event_type,
                    CHANGESPEC_STATUS_TRANSITIONED
                );
                assert_eq!(
                    write.outcome.resource_handle.as_deref(),
                    Some("changespec:project:alpha")
                );
                assert_eq!(
                    write.outcome.source_exports[0].status,
                    SourceExportStatusWire::Applied
                );
            }
            other => {
                panic!("expected changespec write response, got {other:?}")
            }
        }

        assert_eq!(std::fs::read_to_string(&project_file).unwrap(), updated);
        let projected = projection_service
            .read_blocking(|db| {
                db.fetch_changespec_detail(&changespec_handle(
                    "project", "alpha",
                ))
            })
            .unwrap()
            .unwrap();
        assert_eq!(projected.summary.status, "Draft");
        assert_eq!(projected.spec.name, "alpha");
    }

    #[test]
    fn changespec_write_stale_source_returns_conflict_before_append() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let projection_service = runtime.block_on(
            ProjectionService::initialize(dir.path().join("projection.sqlite")),
        );
        let state = test_state_with_projection(projection_service.clone());
        let project_dir = dir.path().join("project");
        std::fs::create_dir_all(&project_dir).unwrap();
        let project_file = project_dir.join("project.sase");
        let original =
            "NAME: alpha\nDESCRIPTION: demo\nSTATUS: WIP\n".to_string();
        let legacy =
            "NAME: alpha\nDESCRIPTION: changed\nSTATUS: WIP\n".to_string();
        let updated =
            "NAME: alpha\nDESCRIPTION: demo\nSTATUS: Draft\n".to_string();
        std::fs::write(&project_file, &original).unwrap();
        let expected = source_fingerprint_from_path(&project_file, true);
        std::fs::write(&project_file, &legacy).unwrap();

        let spec = sase_core::parser::parse_project_bytes(
            &project_file.display().to_string(),
            updated.as_bytes(),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let mut export = SourceExportPlanWire::new(
            project_file.display().to_string(),
            SourceExportKindWire::ProjectFile,
            updated,
        );
        export.expected_fingerprint = expected;

        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Write(LocalDaemonWriteRequestWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface: "changespec.status".to_string(),
                project_id: "project".to_string(),
                idempotency_key: "changespec-status-alpha-stale".to_string(),
                actor: MutationActorWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    actor_type: "test".to_string(),
                    name: "local-transport-test".to_string(),
                    version: None,
                    runtime: None,
                },
                payload: json!({
                    "spec": spec,
                    "from_status": "WIP",
                    "to_status": "Draft",
                    "is_archive": false,
                }),
                expected_source_fingerprints: vec![],
                source_exports: vec![export],
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &state);

        match response.payload {
            LocalDaemonResponsePayloadWire::Error(error) => {
                assert_eq!(
                    error.code,
                    LocalDaemonErrorCodeWire::ConflictStaleSource
                );
            }
            other => {
                panic!("expected changespec conflict response, got {other:?}")
            }
        }
        assert_eq!(std::fs::read_to_string(&project_file).unwrap(), legacy);
        assert_eq!(
            projection_service
                .read_blocking(|db| db.event_count())
                .unwrap(),
            0
        );
    }

    #[test]
    fn notification_write_updates_projection_and_source_exports() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let projection_service = runtime.block_on(
            ProjectionService::initialize(dir.path().join("projection.sqlite")),
        );
        let mut state = test_state_with_projection(projection_service.clone());
        state.paths.sase_home = dir.path().join("home");
        let notification = notification("write-notification-1");

        let append_payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Write(LocalDaemonWriteRequestWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface: "notifications.append".to_string(),
                project_id: "home".to_string(),
                idempotency_key: "append-write-notification-1".to_string(),
                actor: MutationActorWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    actor_type: "test".to_string(),
                    name: "local-transport-test".to_string(),
                    version: None,
                    runtime: None,
                },
                payload: json!({"notification": notification}),
                expected_source_fingerprints: vec![],
                source_exports: vec![],
            }),
        ))
        .unwrap();

        let append_response = response_for_frame(&append_payload, &state);

        match append_response.payload {
            LocalDaemonResponsePayloadWire::Write(write) => {
                assert_eq!(write.surface, "notifications.append");
                assert!(write.outcome.changed);
                assert_eq!(write.outcome.source_exports.len(), 1);
                assert_eq!(
                    write.outcome.source_exports[0].status,
                    SourceExportStatusWire::Applied
                );
            }
            other => {
                panic!("expected notification write response, got {other:?}")
            }
        }

        let notification_path = state
            .paths
            .sase_home
            .join("notifications")
            .join("notifications.jsonl");
        let snapshot =
            read_notifications_snapshot(&notification_path, true).unwrap();
        assert_eq!(snapshot.notifications.len(), 1);
        assert!(!snapshot.notifications[0].read);

        let mark_read_payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Write(LocalDaemonWriteRequestWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface: "notifications.state_update".to_string(),
                project_id: "home".to_string(),
                idempotency_key: "mark-read-write-notification-1".to_string(),
                actor: MutationActorWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    actor_type: "test".to_string(),
                    name: "local-transport-test".to_string(),
                    version: None,
                    runtime: None,
                },
                payload: json!({
                    "update": NotificationStateUpdateWire::MarkRead {
                        id: "write-notification-1".to_string(),
                    }
                }),
                expected_source_fingerprints: vec![],
                source_exports: vec![],
            }),
        ))
        .unwrap();

        let mark_read_response = response_for_frame(&mark_read_payload, &state);

        match mark_read_response.payload {
            LocalDaemonResponsePayloadWire::Write(write) => {
                assert_eq!(write.surface, "notifications.state_update");
                assert!(write.outcome.changed);
                assert_eq!(
                    write.outcome.source_exports[0].status,
                    SourceExportStatusWire::Applied
                );
            }
            other => {
                panic!("expected notification write response, got {other:?}")
            }
        }

        let snapshot =
            read_notifications_snapshot(&notification_path, true).unwrap();
        assert!(snapshot.notifications[0].read);
        let projected = projection_service
            .read_blocking(|db| {
                sase_core::projections::notification_projection_detail(
                    db.connection(),
                    "write-notification-1",
                )
            })
            .unwrap();
        assert!(projected.unwrap().read);
    }

    #[test]
    fn bead_write_updates_projection_and_source_exports() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let projection_service = runtime.block_on(
            ProjectionService::initialize(dir.path().join("projection.sqlite")),
        );
        let state = test_state_with_projection(projection_service.clone());
        let beads_dir = dir.path().join("project").join("sdd").join("beads");
        std::fs::create_dir_all(&beads_dir).unwrap();
        std::fs::write(
            beads_dir.join("config.json"),
            r#"{"issue_prefix":"sase","next_counter":1,"owner":"owner@example.com"}"#,
        )
        .unwrap();
        std::fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::Write(LocalDaemonWriteRequestWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface: "beads".to_string(),
                project_id: "project".to_string(),
                idempotency_key: "bead-create-1".to_string(),
                actor: MutationActorWire {
                    schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    actor_type: "test".to_string(),
                    name: "local-transport-test".to_string(),
                    version: None,
                    runtime: None,
                },
                payload: json!({
                    "schema_version": 1,
                    "operation": "create",
                    "beads_dir": beads_dir,
                    "create": {
                        "title": "Daemon bead",
                        "issue_type": "plan",
                        "tier": "epic",
                        "now": "2026-05-14T00:00:00Z"
                    }
                }),
                expected_source_fingerprints: vec![],
                source_exports: vec![],
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &state);

        match response.payload {
            LocalDaemonResponsePayloadWire::Write(write) => {
                assert_eq!(write.surface, "beads");
                assert_eq!(write.outcome.event_type, "bead.created");
                assert_eq!(
                    write.outcome.resource_handle.as_deref(),
                    Some("sase-1")
                );
                assert!(write.outcome.source_exports.iter().all(|export| {
                    export.status == SourceExportStatusWire::Applied
                }));
                assert_eq!(
                    write.outcome.projection_snapshot.as_ref().unwrap()
                        ["operation"],
                    json!("create")
                );
            }
            other => panic!("expected bead write response, got {other:?}"),
        }

        let source =
            std::fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(source.contains("Daemon bead"));
        let projected = projection_service
            .read_blocking(|db| {
                sase_core::projections::bead_projection_show(
                    db.connection(),
                    "project",
                    "sase-1",
                )
            })
            .unwrap();
        assert_eq!(projected.title, "Daemon bead");
    }

    #[test]
    fn notification_read_list_uses_projection_rows() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let projection_service = runtime.block_on(
            ProjectionService::initialize(dir.path().join("projection.sqlite")),
        );
        let mut notification = notification("notification-1");
        notification.notes = vec!["hello projection".to_string()];
        runtime
            .block_on(
                projection_service.append_projected_event(
                    notification_append_event_request(
                        notification_context(),
                        notification.clone(),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        let state = test_state_with_projection(projection_service);
        let payload =
            serde_json::to_vec(&request(LocalDaemonRequestPayloadWire::Read(
                LocalDaemonReadRequestWire::NotificationList(
                    crate::wire::NotificationReadListRequestWire {
                        schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                        page: ProjectionPageRequestWire {
                            schema_version: PROJECTION_READ_WIRE_SCHEMA_VERSION,
                            limit: 10,
                            cursor: None,
                        },
                        include_dismissed: false,
                        query: Some("projection".to_string()),
                        sender: None,
                        unread: Some(true),
                    },
                ),
            )))
            .unwrap();

        let response = response_for_frame(&payload, &state);

        match response.payload {
            LocalDaemonResponsePayloadWire::Read(read_response) => {
                let LocalDaemonReadResponseWire::NotificationList(list) =
                    *read_response
                else {
                    panic!(
                        "expected notification list response, got {read_response:?}"
                    );
                };
                assert_eq!(list.notifications, vec![notification]);
                assert_eq!(list.counts.active, 1);
                assert_eq!(
                    response.snapshot_id,
                    Some(list.snapshot.snapshot_id)
                );
            }
            other => {
                panic!("expected notification read response, got {other:?}")
            }
        }
    }

    #[test]
    fn notification_collection_list_returns_generic_list_items() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let projection_service = runtime.block_on(
            ProjectionService::initialize(dir.path().join("projection.sqlite")),
        );
        let mut notification = notification("notification-list-1");
        notification.notes = vec!["generic list".to_string()];
        runtime
            .block_on(
                projection_service.append_projected_event(
                    notification_append_event_request(
                        notification_context(),
                        notification.clone(),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        let state = test_state_with_projection(projection_service);
        let payload = serde_json::to_vec(&request(
            LocalDaemonRequestPayloadWire::List(LocalDaemonListRequestWire {
                collection: LocalDaemonCollectionWire::Notifications,
                page: LocalDaemonPageRequestWire {
                    limit: 10,
                    cursor: None,
                },
                snapshot_id: Some("snap_notifications".to_string()),
                stable_handle: Some("notifications:list:test".to_string()),
                max_payload_bytes: None,
                filters: Some(json!({"query": "generic"})),
            }),
        ))
        .unwrap();

        let response = response_for_frame(&payload, &state);

        match response.payload {
            LocalDaemonResponsePayloadWire::List(list) => {
                assert_eq!(
                    list.collection,
                    LocalDaemonCollectionWire::Notifications
                );
                assert_eq!(list.items.len(), 1);
                assert_eq!(list.items[0].handle, "notification-list-1");
                assert_eq!(
                    list.items[0].summary["id"],
                    json!("notification-list-1")
                );
                assert_eq!(list.snapshot_id, "snap_notifications");
                assert_eq!(
                    list.stable_handle.as_deref(),
                    Some("notifications:list:test")
                );
            }
            other => {
                panic!("expected generic notification list, got {other:?}")
            }
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

    fn notification_context() -> NotificationProjectionEventContextWire {
        NotificationProjectionEventContextWire {
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "local-transport-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            idempotency_key: None,
            source_path: Some("notifications.jsonl".to_string()),
            source_revision: None,
        }
    }

    fn notification(id: &str) -> NotificationWire {
        NotificationWire {
            id: id.to_string(),
            timestamp: "2026-05-01T01:02:03+00:00".to_string(),
            sender: "test-sender".to_string(),
            notes: Vec::new(),
            files: Vec::new(),
            action: None,
            action_data: BTreeMap::new(),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
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
