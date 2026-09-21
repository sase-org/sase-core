use std::collections::BTreeSet;

use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;

use sase_core::notifications::{
    GateActionRequestWire, MobileActionKindWire, NotificationWire,
    QuestionActionChoiceWire, QuestionActionRequestWire,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
};

use serde_json::Value as JsonValue;

use crate::fleet_attention::FleetAttentionStoreError;

use crate::fleet_auth::{
    current_unix_time, negotiate_fleet_protocol_version,
    FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE,
    FLEET_SCOPE_REVOKE, FLEET_SCOPE_ROTATE,
};

use crate::host_bridge::HostBridgeError;

use crate::wire::{
    FleetCredentialRevokeRequestWire, FleetCredentialRevokeResponseWire,
    FleetLogicalBatchRequestWire, FleetTokenRotateRequestWire,
    FleetTokenRotateResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

use super::errors::*;

use super::state::*;

use super::support::*;

pub(crate) async fn fleet_attention_read(
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

pub(crate) async fn fleet_attention_inventory(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<
        Json<sase_core::FleetAttentionInventoryRequestWire>,
        JsonRejection,
    >,
) -> Result<Json<sase_core::FleetAttentionInventoryResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/attention/inventory",
        FLEET_SCOPE_ATTENTION_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let notifications = state
        .notification_bridge
        .list_notifications(false)
        .map_err(ApiError::from_host_bridge)?;
    let rows =
        attention_notification_rows(&state, notifications.notifications.iter());
    let observed_at_unix = current_unix_time();
    let response = sase_core::project_fleet_attention_inventory(
        &installation.installation_id,
        &rows,
        &[],
        &payload,
        observed_at_unix,
        sase_core::FleetSnapshotFreshnessWire {
            schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: sase_core::ObservationFreshnessWire::Fresh,
            partial: false,
            refreshed_at_unix: Some(observed_at_unix),
            error: None,
        },
    )
    .map_err(|error| {
        ApiError::invalid_request("attention_inventory", error.to_string())
    })?;
    Ok(Json(response))
}

pub(crate) fn attention_notification_rows<'a>(
    state: &GatewayState,
    notifications: impl Iterator<Item = &'a NotificationWire>,
) -> Vec<sase_core::FleetAttentionNotificationRowWire> {
    notifications
        .map(
            |notification| sase_core::FleetAttentionNotificationRowWire {
                schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
                state: state.notification_bridge.action_state(notification),
                notification: notification.clone(),
            },
        )
        .collect()
}

/// The agent-label signal used to correlate a notification to a followed
/// row: the gate producer's declared `origin_agent`, or (for a question) the
/// asking agent's own `sender` identity.
pub(crate) fn attention_correlation_label(
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

pub(crate) async fn fleet_attention_resolve(
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
                    == sase_core::FleetAttentionStateWire::Pending
                    && remote_attention_capability_allowed(
                        entry,
                        &payload.intent,
                    ) =>
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

pub(crate) fn remote_attention_capability_allowed(
    entry: &sase_core::FleetAttentionEntryWire,
    intent: &sase_core::FleetAttentionIntentWire,
) -> bool {
    if intent.kind != sase_core::FleetAttentionKindWire::Gate {
        return true;
    }
    let options = entry
        .options
        .iter()
        .map(|option| (option.id.as_str(), option.requires_tty))
        .collect::<std::collections::BTreeMap<_, _>>();
    intent.selected_option_ids.iter().all(|id| {
        options
            .get(id.as_str())
            .is_some_and(|requires_tty| !requires_tty)
    })
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
pub(crate) fn attention_refusal_settlement(
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
pub(crate) fn current_attention_entry(
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

pub(crate) fn execute_fleet_attention(
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

pub(crate) async fn fleet_token_rotate(
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

pub(crate) async fn fleet_credential_revoke(
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
