use std::collections::BTreeMap;
use std::convert::Infallible;

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::sse::{KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;

use chrono::Utc;

use sase_core::notifications::{
    mobile_action_detail_from_notification, mobile_notification_card_from_wire,
    mobile_notification_error_from_wire,
    mobile_notification_priority_from_wire, ActionResultWire,
    GateActionRequestWire, MobileNotificationDetailResponseWire,
    MobileNotificationListResponseWire, QuestionActionChoiceWire,
    QuestionActionRequestWire, MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};

use serde::Deserialize;

use serde_json::Value as JsonValue;

use crate::host_bridge::{DynNotificationHostBridge, HostBridgeError};

use crate::storage::{
    format_time, generate_pairing_code, generate_prefixed_id,
};

use crate::wire::{
    EventPayloadWire, EventRecordWire, MobileAgentImageLaunchRequestWire,
    MobileAgentKillRequestWire, MobileAgentKillResultWire,
    MobileAgentLaunchResultWire, MobileAgentListRequestWire,
    MobileAgentListResponseWire, MobileAgentResumeOptionsResponseWire,
    MobileAgentRetryRequestWire, MobileAgentRetryResultWire,
    MobileAgentTextLaunchRequestWire, MobileBeadListRequestWire,
    MobileBeadListResponseWire, MobileBeadShowRequestWire,
    MobileBeadShowResponseWire, MobileChangeSpecTagListRequestWire,
    MobileChangeSpecTagListResponseWire, MobilePatchTagListRequestWire,
    MobilePatchTagListResponseWire, MobileUpdateStartRequestWire,
    MobileUpdateStartResponseWire, MobileUpdateStatusRequestWire,
    MobileUpdateStatusResponseWire, MobileXpromptCatalogRequestWire,
    MobileXpromptCatalogResponseWire, NotificationStateMutationResponseWire,
    PairFinishRequestWire, PairFinishResponseWire, PairStartRequestWire,
    PairStartResponseWire, PushSubscriptionDeleteResponseWire,
    PushSubscriptionListResponseWire, PushSubscriptionRegisterResponseWire,
    PushSubscriptionRequestWire, SessionResponseWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};

use super::errors::*;

use super::state::*;

use super::support::*;

pub(crate) async fn pair_start(
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

pub(crate) async fn pair_finish(
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

pub(crate) async fn session(
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

pub(crate) async fn list_push_subscriptions(
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

pub(crate) async fn register_push_subscription(
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

pub(crate) async fn delete_push_subscription(
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

pub(crate) async fn events(
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
pub(crate) struct AgentListQuery {
    #[serde(default)]
    include_recent: bool,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

pub(crate) async fn list_agents(
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

pub(crate) async fn agent_resume_options(
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

pub(crate) async fn agent_launch(
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

pub(crate) async fn agent_launch_image(
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

pub(crate) async fn agent_kill(
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

pub(crate) async fn agent_retry(
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
pub(crate) struct ChangeSpecTagsQuery {
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

pub(crate) async fn list_changespec_tags(
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

pub(crate) async fn list_patch_tags(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<ChangeSpecTagsQuery>,
) -> Result<Json<MobilePatchTagListResponseWire>, ApiError> {
    let device = authenticate(&state, &headers, "/api/v1/patch-tags").await?;
    let request = MobilePatchTagListRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        project: query.project,
        limit: query.limit,
        device_id: Some(device.device_id),
    };
    state
        .helper_bridge
        .list_patch_tags(&request)
        .map(Json)
        .map_err(ApiError::from_host_bridge)
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct XpromptCatalogQuery {
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

pub(crate) async fn xprompt_catalog(
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
pub(crate) struct BeadListQuery {
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

pub(crate) async fn list_beads(
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
pub(crate) struct BeadShowQuery {
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    all_projects: bool,
}

pub(crate) async fn show_bead(
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
pub(crate) struct UpdateStartBody {
    #[serde(default = "default_gateway_schema_version")]
    schema_version: u32,
    #[serde(default)]
    request_id: Option<String>,
}

pub(crate) async fn update_start(
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

pub(crate) async fn update_status(
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

pub(crate) async fn list_notifications(
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

pub(crate) async fn notification_detail(
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

pub(crate) async fn mark_notification_read(
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

pub(crate) async fn dismiss_notification(
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

pub(crate) async fn mutate_notification_state(
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

pub(crate) async fn download_attachment(
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
pub(crate) struct GateActionBody {
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
pub(crate) struct QuestionActionBody {
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

pub(crate) async fn gate_action(
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

pub(crate) async fn question_answer(
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

pub(crate) async fn question_custom(
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

pub(crate) async fn execute_question_action_route(
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
