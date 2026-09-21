use std::cmp::Ordering;
use std::path::{Component, Path, PathBuf};

use axum::http::{header, HeaderMap};
use axum::response::sse::Event;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use sase_core::notifications::{
    mobile_attachment_manifest_from_path, notification_activity_at,
    MobileActionKindWire, NotificationWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};

use serde::Deserialize;

use crate::fleet_auth::{
    credential_has_scope, current_unix_time, negotiate_fleet_protocol_version,
    FleetAuthentication,
};

use crate::storage::format_time;

use crate::wire::{
    DeviceRecordWire, EventPayloadWire, EventRecordWire,
    FleetCredentialRecordWire, FleetEventStreamItemWire,
    MobileAgentLaunchResultWire, PushSubscriptionRequestWire, StoreCursorWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};

use super::errors::*;

use super::state::*;

pub(crate) async fn authenticate(
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

pub(crate) async fn fleet_authenticate(
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

pub(crate) fn bearer_token(headers: &HeaderMap) -> Result<&str, ApiError> {
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

pub(crate) fn fleet_protocol_version_from_headers(
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

pub(crate) fn datetime_to_unix(now: DateTime<Utc>) -> f64 {
    now.timestamp() as f64
        + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
}

pub(crate) fn validate_schema(schema_version: u32) -> Result<(), ApiError> {
    if schema_version == GATEWAY_WIRE_SCHEMA_VERSION {
        return Ok(());
    }
    Err(ApiError::invalid_request(
        "schema_version",
        "unsupported schema_version",
    ))
}

pub(crate) fn validate_push_subscription_request(
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

pub(crate) fn default_mobile_schema_version() -> u32 {
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION
}

pub(crate) fn default_gateway_schema_version() -> u32 {
    GATEWAY_WIRE_SCHEMA_VERSION
}

pub(crate) fn default_attachment_token_ttl() -> ChronoDuration {
    ChronoDuration::minutes(5)
}

pub(crate) fn publish_notifications_changed(
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

pub(crate) fn publish_expired_notification_activity(
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

pub(crate) fn publish_agents_changed(
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

pub(crate) fn publish_helpers_changed(
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

pub(crate) fn launch_primary_name(
    result: &MobileAgentLaunchResultWire,
) -> Option<String> {
    result
        .primary
        .as_ref()
        .and_then(|slot| slot.name.clone())
        .or_else(|| result.slots.iter().find_map(|slot| slot.name.clone()))
}

pub(crate) fn initial_events_for_stream(
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

pub(crate) fn last_event_id(
    headers: &HeaderMap,
) -> Result<Option<&str>, ApiError> {
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

pub(crate) fn sse_event(record: EventRecordWire) -> Event {
    let event_name = event_name(&record.payload);
    let data = serde_json::to_string(&record)
        .expect("EventRecordWire serialization should be infallible");
    Event::default().id(record.id).event(event_name).data(data)
}

pub(crate) fn fleet_sse_event(item: FleetEventStreamItemWire) -> Event {
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

pub(crate) fn event_name(payload: &EventPayloadWire) -> &'static str {
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

pub(crate) fn fleet_event_name(
    item: &FleetEventStreamItemWire,
) -> &'static str {
    match item {
        FleetEventStreamItemWire::Invalidation(_) => "invalidation",
        FleetEventStreamItemWire::ResyncRequired(_) => "resync_required",
        FleetEventStreamItemWire::Heartbeat { .. } => "heartbeat",
    }
}

pub(crate) fn fleet_event_id(
    item: &FleetEventStreamItemWire,
) -> Option<String> {
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

pub(crate) fn filtered_notifications(
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

pub(crate) fn sort_newest_first(notifications: &mut [NotificationWire]) {
    notifications
        .sort_by(|left, right| compare_notification_activity(right, left));
}

pub(crate) fn compare_notification_activity(
    left: &NotificationWire,
    right: &NotificationWire,
) -> Ordering {
    compare_activity_timestamps(
        notification_activity_at(left),
        notification_activity_at(right),
    )
    .then_with(|| left.id.cmp(&right.id))
}

pub(crate) fn compare_activity_timestamps(left: &str, right: &str) -> Ordering {
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

pub(crate) fn notification_activity_cursor_value(
    notification: &NotificationWire,
) -> String {
    format!(
        "{}|{}",
        notification_activity_at(notification),
        notification.id
    )
}

pub(crate) fn activity_is_newer(
    candidate: &NotificationWire,
    high_water: &str,
) -> bool {
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

pub(crate) fn normalize_home_path(path: &str) -> String {
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
pub(crate) struct AttachmentCandidate {
    pub(crate) raw_path: String,
}

pub(crate) fn build_attachment_manifests(
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

pub(crate) fn attachment_candidates(
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
        | MobileActionKindWire::SudoRequest
        | MobileActionKindWire::CustomGate
        | MobileActionKindWire::NonAction
        | MobileActionKindWire::Unsupported => {}
    }
    paths
        .into_iter()
        .map(|raw_path| AttachmentCandidate { raw_path })
        .collect()
}

pub(crate) fn push_unique_path(paths: &mut Vec<String>, path: &str) {
    let trimmed = path.trim();
    if trimmed.is_empty() || paths.iter().any(|existing| existing == trimmed) {
        return;
    }
    paths.push(trimmed.to_string());
}

pub(crate) fn action_path(
    notification: &NotificationWire,
    key: &str,
) -> Option<PathBuf> {
    let raw = notification.action_data.get(key)?.trim();
    if raw.is_empty() {
        return None;
    }
    Some(expand_home_path(raw))
}

pub(crate) fn expand_home_path(path: &str) -> PathBuf {
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

pub(crate) fn hitl_path_typed_outputs(request_path: &Path) -> Vec<String> {
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

// Attachment contract (epic sase-157 gateway-attachments, branch (b)): a
// symlinked _ancestor_ is not by itself evidence of an attack — on macOS
// `/tmp` and `/var` are platform aliases, so every system-temp file has one,
// and rejecting on "any ancestor is a symlink" makes every such attachment
// permanently undownloadable. Only a `..` traversal or a symlink used to
// redirect the named file itself (final component) disqualifies. The resolved
// canonical path is what gets stored in the download token and re-verified
// at download time, so an ancestor alias cannot redirect the served bytes.
pub(crate) fn validate_attachment_path(
    path: &Path,
    expected_size: Option<u64>,
    max_bytes: u64,
) -> Result<(PathBuf, u64), ()> {
    if path
        .components()
        .any(|component| component == Component::ParentDir)
        || is_symlink(path)
    {
        return Err(());
    }
    let canonical = std::fs::canonicalize(path).map_err(|_| ())?;
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

/// Reports whether the named file itself is a symlink, without following it.
/// Ancestor symlinks (such as the macOS `/tmp` and `/var` platform aliases)
/// deliberately do not count: see `validate_attachment_path`.
pub(crate) fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
}

pub(crate) fn sanitize_content_disposition_filename(
    display_name: &str,
) -> String {
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

pub(crate) fn format_event_id(id: u64) -> String {
    format!("{id:016}")
}

pub(crate) fn format_fleet_event_id(cursor: &StoreCursorWire) -> String {
    format!("{}:{}", cursor.store_generation, cursor.sequence)
}

pub(crate) fn parse_event_id(id: &str) -> Option<u64> {
    id.parse::<u64>().ok()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct NotificationListQuery {
    #[serde(default)]
    pub(crate) unread: bool,
    #[serde(default)]
    pub(crate) unread_only: bool,
    #[serde(default)]
    pub(crate) include_dismissed: bool,
    #[serde(default)]
    pub(crate) include_silent: bool,
    #[serde(default)]
    pub(crate) limit: Option<u32>,
    #[serde(default)]
    pub(crate) newer_than: Option<String>,
}
