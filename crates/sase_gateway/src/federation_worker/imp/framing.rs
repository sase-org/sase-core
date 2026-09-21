use super::*;
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time;

pub(super) async fn read_request(
    stream: &mut UnixStream,
    max_frame_bytes: usize,
) -> Result<Option<FederationIpcRequestEnvelopeWire>, FederationErrorWire> {
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
                &format!("IPC frame is not a valid request envelope: {error}"),
                Some("frame"),
            )
        })
        .map(Some)
}

pub(super) async fn write_response(
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

pub(super) async fn handle_request(
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

pub(super) async fn handle_operation(
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
        FederationIpcRequestWire::Capabilities => to_json(serde_json::json!({
            "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
            "capabilities": federation_capabilities(),
        })),
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
                .read_all(ReadOperation::Catalog(query), cache_only, deadline)
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
                .read_all(ReadOperation::Detail(request), cache_only, deadline)
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
pub(super) struct RequestDeadline {
    pub(super) unix_ms: Option<u64>,
}

impl RequestDeadline {
    pub(super) fn remaining(
        self,
    ) -> Option<Result<Duration, FederationErrorWire>> {
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

    pub(super) fn expired_error(self) -> Option<FederationErrorWire> {
        self.remaining()?.err()
    }
}

pub(super) async fn with_deadline<T>(
    deadline: RequestDeadline,
    future: impl Future<Output = Result<T, FederationErrorWire>>,
) -> Result<T, FederationErrorWire> {
    match deadline.remaining() {
        Some(Ok(remaining)) => match time::timeout(remaining, future).await {
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
