use std::convert::Infallible;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;

use chrono::Utc;

use serde::Deserialize;

use crate::fleet_auth::{
    current_unix_time, fleet_capabilities, FleetEnrollmentResult,
    FLEET_SCOPE_BATCH_READ, FLEET_SCOPE_CATALOG_READ, FLEET_SCOPE_CONTENT_READ,
    FLEET_SCOPE_DETAIL_READ, FLEET_SCOPE_EVENTS_READ, FLEET_SCOPE_HELLO,
    FLEET_SCOPE_LAUNCH, FLEET_SCOPE_MUTATE, FLEET_SCOPE_PROJECTS_READ,
    FLEET_SCOPE_SUMMARY_READ,
};

use crate::fleet_mutations::FleetMutationStoreError;

use crate::fleet_reads::{resync_item, FleetReadError};

use crate::wire::{
    FleetCatalogQueryWire, FleetContentReadRequestWire,
    FleetContentReadResponseWire, FleetDetailRequestWire,
    FleetDetailResponseWire, FleetEnrollmentRequestWire,
    FleetEnrollmentResponseWire, FleetEventStreamItemWire,
    FleetHelloResponseWire, FleetLaunchRequestWire, FleetLaunchResponseWire,
    FleetLogicalBatchRequestWire, FleetLogicalBatchResponseWire,
    FleetMutationRequestWire, FleetMutationResponseWire,
    FleetProjectEligibilityRequestWire, FleetProjectEligibilityResponseWire,
    FleetResyncReasonWire, FleetSummaryResponseWire, GatewayServiceVersionWire,
    MobileAgentForkRequestWire, MobileAgentKillRequestWire,
    MobileAgentRetryRequestWire, MobileAgentTextLaunchRequestWire,
    StoreCursorWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

use super::errors::*;

use super::state::*;

use super::support::*;

pub(crate) async fn fleet_enroll(
    State(state): State<GatewayState>,
    payload: Result<Json<FleetEnrollmentRequestWire>, JsonRejection>,
) -> Result<(StatusCode, Json<FleetEnrollmentResponseWire>), ApiError> {
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let now = Utc::now();
    if !state.fleet_enrollment_limiter.check(now)? {
        return Err(ApiError::rate_limited("enroll"));
    }
    let now_unix = datetime_to_unix(now);
    let result = state
        .fleet_store
        .enroll(payload, now_unix)
        .map_err(ApiError::from_fleet_store)?;
    match result {
        FleetEnrollmentResult::Enrolled(success) => {
            let success = *success;
            let credential = success.credential;
            let scopes = credential.scopes.clone();
            Ok((
                StatusCode::OK,
                Json(FleetEnrollmentResponseWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    outcome: "enrolled".to_string(),
                    protocol_version: Some(success.protocol_version),
                    installation: success.installation,
                    machine_selector: state.machine_selector.clone(),
                    capabilities: fleet_capabilities(&scopes),
                    credential: Some(credential),
                    token_type: Some("bearer".to_string()),
                    token: Some(success.token),
                    quarantine: None,
                }),
            ))
        }
        FleetEnrollmentResult::Quarantined(mut response) => {
            response.machine_selector = state.machine_selector.clone();
            Ok((StatusCode::CONFLICT, Json(*response)))
        }
    }
}

pub(crate) async fn fleet_hello(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<FleetHelloResponseWire>, ApiError> {
    let protocol_version = fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/hello",
        FLEET_SCOPE_HELLO,
    )
    .await?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let summary = state
        .fleet_reads
        .summary()
        .await
        .map_err(ApiError::from_fleet_read)?;
    Ok(Json(FleetHelloResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        protocol_version,
        gateway_version: Some(GatewayServiceVersionWire {
            service: "sase-gateway".to_string(),
            package_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
        fleet_contract_schema_version: Some(
            sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        ),
        installation,
        machine_selector: state.machine_selector.clone(),
        capabilities: fleet_capabilities(&credential.scopes),
        credential,
        cursor: summary.cursor,
        counts: summary.counts,
        count_revision: summary.count_revision,
        freshness: summary.freshness,
    }))
}

pub(crate) async fn fleet_summary(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<FleetSummaryResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/summary",
        FLEET_SCOPE_SUMMARY_READ,
    )
    .await?;
    state
        .fleet_reads
        .summary()
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

pub(crate) async fn fleet_catalog(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetCatalogQueryWire>, JsonRejection>,
) -> Result<Json<crate::wire::FleetCatalogPageWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/catalog",
        FLEET_SCOPE_CATALOG_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .catalog(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

pub(crate) async fn fleet_batch_lookup(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetLogicalBatchRequestWire>, JsonRejection>,
) -> Result<Json<FleetLogicalBatchResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/batch",
        FLEET_SCOPE_BATCH_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .batch_lookup(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

pub(crate) async fn fleet_detail(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetDetailRequestWire>, JsonRejection>,
) -> Result<Json<FleetDetailResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/detail",
        FLEET_SCOPE_DETAIL_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .detail(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

pub(crate) async fn fleet_content(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetContentReadRequestWire>, JsonRejection>,
) -> Result<Json<FleetContentReadResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/content",
        FLEET_SCOPE_CONTENT_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .content(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

pub(crate) async fn fleet_project_eligibility(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetProjectEligibilityRequestWire>, JsonRejection>,
) -> Result<Json<FleetProjectEligibilityResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/projects/eligibility",
        FLEET_SCOPE_PROJECTS_READ,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    state
        .fleet_reads
        .project_eligibility(payload)
        .await
        .map(Json)
        .map_err(ApiError::from_fleet_read)
}

pub(crate) async fn fleet_events(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<FleetEventsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/events",
        FLEET_SCOPE_EVENTS_READ,
    )
    .await?;
    let subscription = state
        .fleet_reads
        .subscribe_events(query.cursor()?)
        .await
        .map_err(ApiError::from_fleet_read)?;
    let stream_state = state.clone();
    let stream = async_stream::stream! {
        for item in subscription.initial {
            yield Ok::<_, Infallible>(fleet_sse_event(item));
        }
        let mut receiver = subscription.receiver;
        let mut interval = tokio::time::interval(stream_state.heartbeat_interval);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    yield Ok::<_, Infallible>(fleet_sse_event(
                        FleetEventStreamItemWire::Heartbeat {
                            cursor: stream_state.fleet_reads.current_event_cursor(),
                        },
                    ));
                }
                received = receiver.recv() => {
                    match received {
                        Ok(event) => {
                            yield Ok::<_, Infallible>(fleet_sse_event(
                                FleetEventStreamItemWire::Invalidation(event),
                            ));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let Ok(snapshot) = stream_state.fleet_reads.authoritative_snapshot().await else {
                                break;
                            };
                            yield Ok::<_, Infallible>(fleet_sse_event(resync_item(
                                FleetResyncReasonWire::ReceiverLag,
                                snapshot,
                            )));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub(crate) async fn fleet_launch(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetLaunchRequestWire>, JsonRejection>,
) -> Result<Json<FleetLaunchResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/launch",
        FLEET_SCOPE_LAUNCH,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let admission = state
        .fleet_launches
        .reserve(&payload, &installation.installation_id, current_unix_time())
        .map_err(ApiError::from_fleet_launch_store)?;
    if !admission.should_launch {
        return Ok(Json(FleetLaunchResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            decision: admission.decision,
            reason: admission.reason,
            receipt: admission.receipt,
        }));
    }

    let project_id = payload.intent.project.project_id.clone();
    if admission.decision
        == sase_core::OperationDecisionKindWire::ReturnOriginalReceipt
    {
        let agent_id = payload
            .intent
            .name
            .clone()
            .unwrap_or_else(|| payload.key.operation_id.clone());
        if let Some(receipt) = recover_fleet_launch_settlement(
            &state,
            &admission.receipt,
            &project_id,
            &agent_id,
        )
        .await?
        {
            state.audit(
                credential.controller_id,
                "/api/fleet/v1/launch",
                Some(agent_id),
                "recovered",
            );
            return Ok(Json(FleetLaunchResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                decision: admission.decision,
                reason: admission.reason,
                receipt,
            }));
        }
    }

    let launch_request = MobileAgentTextLaunchRequestWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        prompt: payload.intent.prompt.clone(),
        request_id: payload.intent.request_id.clone(),
        display_name: payload.intent.display_name.clone(),
        name: if sase_core::prompt_has_identity_directive(
            &payload.intent.prompt,
        ) {
            None
        } else {
            payload.intent.name.clone()
        },
        model: payload.intent.model.clone(),
        provider: payload.intent.provider.clone(),
        runtime: payload.intent.runtime.clone(),
        project: Some(payload.intent.project.project_id.clone()),
        device_id: credential.controller_id.clone(),
        dry_run: payload.intent.dry_run,
    };

    spawn_fleet_launch_settlement(
        state.clone(),
        credential.controller_id.clone(),
        admission.receipt.clone(),
        project_id,
        launch_request,
    );
    state.audit(
        credential.controller_id,
        "/api/fleet/v1/launch",
        None,
        "accepted",
    );
    Ok(Json(FleetLaunchResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        decision: admission.decision,
        reason: admission.reason,
        receipt: admission.receipt,
    }))
}

pub(crate) async fn recover_fleet_launch_settlement(
    state: &GatewayState,
    receipt: &sase_core::FleetLaunchReceiptWire,
    project_id: &str,
    agent_id: &str,
) -> Result<Option<sase_core::FleetLaunchReceiptWire>, ApiError> {
    let _ = state.fleet_reads.reconcile().await;
    let snapshot = state
        .fleet_reads
        .authoritative_snapshot()
        .await
        .map_err(ApiError::from_fleet_read)?;
    let Some(summary) = snapshot.summaries.iter().find(|summary| {
        summary.logical_locator.project.origin.installation_id
            == receipt.target_installation_id
            && summary.logical_locator.project.project_id == project_id
            && summary.logical_locator.agent_id == agent_id
    }) else {
        return Ok(None);
    };
    let message = summary
        .labels
        .agent_label
        .clone()
        .or_else(|| Some(agent_id.to_string()));
    let recovered = state
        .fleet_launches
        .settle_recovered(
            receipt,
            Some(summary.logical_locator.clone()),
            summary.exact_locator.clone(),
            message,
        )
        .map_err(ApiError::from_fleet_launch_store)?;
    Ok(Some(recovered))
}

pub(crate) fn spawn_fleet_launch_settlement(
    state: GatewayState,
    controller_id: Option<String>,
    receipt: sase_core::FleetLaunchReceiptWire,
    project_id: String,
    launch_request: MobileAgentTextLaunchRequestWire,
) {
    tokio::spawn(async move {
        finish_fleet_launch_settlement(
            state,
            controller_id,
            receipt,
            project_id,
            launch_request,
        )
        .await;
    });
}

pub(crate) async fn finish_fleet_launch_settlement(
    state: GatewayState,
    controller_id: Option<String>,
    receipt: sase_core::FleetLaunchReceiptWire,
    project_id: String,
    launch_request: MobileAgentTextLaunchRequestWire,
) {
    let bridge = state.agent_bridge.clone();
    let launched = tokio::task::spawn_blocking(move || {
        bridge.launch_text(&launch_request)
    })
    .await;
    match launched {
        Ok(Ok(result)) => {
            let primary_name = launch_primary_name(&result);
            let message = result
                .primary
                .as_ref()
                .and_then(|slot| slot.message.clone())
                .or_else(|| primary_name.clone());
            let agent_id = primary_name
                .clone()
                .unwrap_or_else(|| receipt.key.operation_id.clone());
            let recovered = recover_fleet_launch_settlement(
                &state,
                &receipt,
                &project_id,
                &agent_id,
            )
            .await;
            let settled = match recovered {
                Ok(Some(receipt)) => Ok(receipt),
                Ok(None) | Err(_) => state.fleet_launches.settle(
                    &receipt,
                    &result,
                    &project_id,
                    message,
                ),
            };
            match settled {
                Ok(_) => {
                    let _ = state.fleet_reads.reconcile().await;
                    state.audit(
                        controller_id,
                        "/api/fleet/v1/launch",
                        primary_name.clone(),
                        "success",
                    );
                    let _ = publish_agents_changed(
                        &state,
                        "fleet_launch",
                        primary_name,
                    );
                }
                Err(error) => {
                    let api_error = ApiError::from_fleet_launch_store(error);
                    state.audit(
                        controller_id,
                        "/api/fleet/v1/launch",
                        None,
                        api_error.wire.code.outcome_label(),
                    );
                }
            }
        }
        Ok(Err(error)) => {
            let api_error = ApiError::from_host_bridge(error);
            let target =
                api_error.wire.target.as_deref().unwrap_or("agent_bridge");
            let message =
                format!("{}: {}", api_error.wire.code.outcome_label(), target);
            let _ = state
                .fleet_launches
                .fail(&receipt, Some(message.chars().take(160).collect()));
            state.audit(
                controller_id,
                "/api/fleet/v1/launch",
                None,
                api_error.wire.code.outcome_label(),
            );
        }
        Err(_) => {
            let _ = state.fleet_launches.fail(
                &receipt,
                Some("internal: launch worker task failed".to_string()),
            );
            state.audit(
                controller_id,
                "/api/fleet/v1/launch",
                None,
                "internal",
            );
        }
    }
}

pub(crate) async fn fleet_mutate(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<FleetMutationRequestWire>, JsonRejection>,
) -> Result<Json<FleetMutationResponseWire>, ApiError> {
    fleet_protocol_version_from_headers(&headers)?;
    let credential = fleet_authenticate(
        &state,
        &headers,
        "/api/fleet/v1/mutate",
        FLEET_SCOPE_MUTATE,
    )
    .await?;
    let Json(payload) = payload.map_err(ApiError::from_fleet_json_rejection)?;
    let installation = state
        .fleet_store
        .ensure_installation_identity()
        .map_err(ApiError::from_fleet_store)?;
    let logical_key =
        sase_core::logical_locator_key(&payload.intent.target.logical)
            .map_err(|error| {
                ApiError::invalid_request("intent.target", error.to_string())
            })?;
    let observed = match state
        .fleet_reads
        .detail(sase_core::FleetDetailRequestWire {
            schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key,
        })
        .await
    {
        Ok(detail) => Some(detail.detail.summary),
        Err(FleetReadError::NotFound(_)) => None,
        Err(error) => return Err(ApiError::from_fleet_read(error)),
    };
    let precondition = sase_core::evaluate_mutation_precondition(
        &payload.intent,
        observed.as_ref(),
    )
    .map_err(|error| ApiError::invalid_request("intent", error.to_string()))?;
    if !precondition.allowed {
        return Err(ApiError::from_mutation_precondition(&precondition));
    }
    let admission = state
        .fleet_mutations
        .reserve(&payload, &installation.installation_id, current_unix_time())
        .map_err(ApiError::from_fleet_mutation_store)?;
    if !admission.should_execute {
        match admission.decision {
            sase_core::OperationDecisionKindWire::Conflict => {
                return Err(ApiError::from_fleet_mutation_store(
                    FleetMutationStoreError::Conflict(format!(
                        "{:?}",
                        admission.reason
                    )),
                ));
            }
            sase_core::OperationDecisionKindWire::Expired => {
                return Err(ApiError::from_fleet_mutation_store(
                    FleetMutationStoreError::Expired(format!(
                        "{:?}",
                        admission.reason
                    )),
                ));
            }
            _ => {
                return Ok(Json(FleetMutationResponseWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    decision: admission.decision,
                    reason: admission.reason,
                    receipt: admission.receipt,
                }));
            }
        }
    }

    let agent_name = payload.intent.target.logical.agent_id.clone();
    let executed = execute_fleet_mutation(
        &state,
        credential.controller_id.as_deref(),
        &payload,
        &agent_name,
    );
    match executed {
        Ok((result_locator, instance_locator, message, primary_name)) => {
            let receipt = state
                .fleet_mutations
                .settle(
                    &admission.receipt,
                    payload.intent.kind,
                    result_locator,
                    instance_locator,
                    message,
                )
                .map_err(ApiError::from_fleet_mutation_store)?;
            state.audit(
                credential.controller_id.clone(),
                "/api/fleet/v1/mutate",
                primary_name.clone(),
                "success",
            );
            publish_agents_changed(&state, "fleet_mutate", primary_name)?;
            Ok(Json(FleetMutationResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                decision: admission.decision,
                reason: admission.reason,
                receipt,
            }))
        }
        Err(api_error) => {
            state.audit(
                credential.controller_id,
                "/api/fleet/v1/mutate",
                Some(agent_name),
                api_error.wire.code.outcome_label(),
            );
            Err(api_error)
        }
    }
}

type FleetMutationExecution = (
    Option<sase_core::LogicalAgentLocatorWire>,
    Option<sase_core::AgentInstanceLocatorWire>,
    Option<String>,
    Option<String>,
);

pub(crate) fn execute_fleet_mutation(
    state: &GatewayState,
    controller_id: Option<&str>,
    payload: &FleetMutationRequestWire,
    agent_name: &str,
) -> Result<FleetMutationExecution, ApiError> {
    match payload.intent.kind {
        sase_core::FleetMutationKindWire::Stop => {
            let request = MobileAgentKillRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                reason: payload.intent.reason.clone(),
                device_id: controller_id.map(str::to_string),
            };
            let result = state
                .agent_bridge
                .kill_agent(agent_name, &request)
                .map_err(ApiError::from_host_bridge)?;
            Ok((
                Some(payload.intent.target.logical.clone()),
                Some(payload.intent.target.clone()),
                result.message.clone().or(Some(result.status.clone())),
                Some(result.name),
            ))
        }
        sase_core::FleetMutationKindWire::Retry => {
            let request = MobileAgentRetryRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                request_id: None,
                prompt_override: None,
                dry_run: None,
                kill_source_first: payload.intent.kill_source_first,
                device_id: controller_id.map(str::to_string),
            };
            let result = state
                .agent_bridge
                .retry_agent(agent_name, &request)
                .map_err(ApiError::from_host_bridge)?;
            let primary_name = launch_primary_name(&result.launch);
            Ok((
                mutation_result_locator(payload, primary_name.as_deref()),
                None,
                primary_name.clone(),
                primary_name,
            ))
        }
        sase_core::FleetMutationKindWire::Fork => {
            let prompt = payload.intent.fork_prompt.clone().unwrap_or_default();
            let request = MobileAgentForkRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                request_id: None,
                prompt,
                dry_run: None,
                device_id: controller_id.map(str::to_string),
            };
            let result = state
                .agent_bridge
                .fork_agent(agent_name, &request)
                .map_err(ApiError::from_host_bridge)?;
            let primary_name = launch_primary_name(&result.launch);
            Ok((
                mutation_result_locator(payload, primary_name.as_deref()),
                None,
                primary_name.clone(),
                primary_name,
            ))
        }
    }
}

pub(crate) fn mutation_result_locator(
    payload: &FleetMutationRequestWire,
    agent_id: Option<&str>,
) -> Option<sase_core::LogicalAgentLocatorWire> {
    let agent_id = agent_id.filter(|value| !value.trim().is_empty())?;
    Some(sase_core::LogicalAgentLocatorWire {
        schema_version: sase_core::FLEET_CONTRACT_SCHEMA_VERSION,
        project: payload.intent.target.logical.project.clone(),
        agent_id: agent_id.to_string(),
        family_id: payload.intent.target.logical.family_id.clone(),
    })
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FleetEventsQuery {
    #[serde(default)]
    store_generation: Option<String>,
    #[serde(default)]
    sequence: Option<u64>,
}

impl FleetEventsQuery {
    fn cursor(&self) -> Result<Option<StoreCursorWire>, ApiError> {
        match (&self.store_generation, self.sequence) {
            (None, None) => Ok(None),
            (Some(store_generation), Some(sequence)) => {
                let cursor = StoreCursorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    store_generation: store_generation.clone(),
                    sequence,
                };
                sase_core::fleet_contract::validate_store_cursor(&cursor)
                    .map(Some)
                    .map_err(|error| {
                        ApiError::invalid_request(
                            "fleet_event_cursor",
                            error.to_string(),
                        )
                    })
            }
            _ => Err(ApiError::invalid_request(
                "fleet_event_cursor",
                "store_generation and sequence must be supplied together",
            )),
        }
    }
}
