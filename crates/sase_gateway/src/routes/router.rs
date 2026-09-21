use axum::extract::{DefaultBodyLimit, State};
use axum::http::Uri;
use axum::routing::{delete, get, post};
use axum::{Json, Router};

use tower_http::trace::TraceLayer;

use crate::wire::{
    default_fleet_protocol_versions, FleetHealthWire, HealthResponseWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};

use super::errors::ApiError;

use super::fleet_attention_handlers::*;

use super::fleet_handlers::*;

use super::mobile_handlers::*;

use super::state::*;

pub fn app(bind_addr: impl Into<String>) -> Router {
    app_with_state(GatewayState::new(bind_addr.into()))
}

pub fn app_with_state(state: GatewayState) -> Router {
    Router::new()
        .nest("/api/fleet/v1", fleet_v1_routes())
        .route("/api/v1/health", get(health))
        .route("/api/v1/session/pair/start", post(pair_start))
        .route("/api/v1/session/pair/finish", post(pair_finish))
        .route("/api/v1/session", get(session))
        .route(
            "/api/v1/session/push-subscriptions",
            get(list_push_subscriptions).post(register_push_subscription),
        )
        .route(
            "/api/v1/session/push-subscriptions/:id",
            delete(delete_push_subscription),
        )
        .route("/api/v1/events", get(events))
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/resume-options", get(agent_resume_options))
        .route("/api/v1/agents/launch", post(agent_launch))
        .route("/api/v1/agents/launch-image", post(agent_launch_image))
        .route("/api/v1/agents/:name/kill", post(agent_kill))
        .route("/api/v1/agents/:name/retry", post(agent_retry))
        .route("/api/v1/changespec-tags", get(list_changespec_tags))
        .route("/api/v1/patch-tags", get(list_patch_tags))
        .route("/api/v1/xprompts/catalog", get(xprompt_catalog))
        .route("/api/v1/beads", get(list_beads))
        .route("/api/v1/beads/:id", get(show_bead))
        .route("/api/v1/update/start", post(update_start))
        .route("/api/v1/update/:job_id", get(update_status))
        .route("/api/v1/notifications", get(list_notifications))
        .route("/api/v1/notifications/:id", get(notification_detail))
        .route(
            "/api/v1/notifications/:id/mark-read",
            post(mark_notification_read),
        )
        .route(
            "/api/v1/notifications/:id/dismiss",
            post(dismiss_notification),
        )
        .route("/api/v1/attachments/:token", get(download_attachment))
        .route("/api/v1/actions/gate/:prefix", post(gate_action))
        .route(
            "/api/v1/actions/question/:prefix/answer",
            post(question_answer),
        )
        .route(
            "/api/v1/actions/question/:prefix/custom",
            post(question_custom),
        )
        .fallback(unknown_route)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn fleet_v1_routes() -> Router<GatewayState> {
    Router::new()
        .route("/enroll", post(fleet_enroll))
        .route("/hello", get(fleet_hello))
        .route("/summary", get(fleet_summary))
        .route("/catalog", post(fleet_catalog))
        .route("/batch", post(fleet_batch_lookup))
        .route("/detail", post(fleet_detail))
        .route("/content", post(fleet_content))
        .route("/projects/eligibility", post(fleet_project_eligibility))
        .route("/events", get(fleet_events))
        .route("/launch", post(fleet_launch))
        .route("/mutate", post(fleet_mutate))
        .route("/attention", post(fleet_attention_read))
        .route("/attention/inventory", post(fleet_attention_inventory))
        .route("/attention/resolve", post(fleet_attention_resolve))
        .route("/credential/rotate", post(fleet_token_rotate))
        .route("/credential/revoke", post(fleet_credential_revoke))
        .layer(DefaultBodyLimit::max(FLEET_REQUEST_BODY_LIMIT_BYTES))
}

async fn health(State(state): State<GatewayState>) -> Json<HealthResponseWire> {
    Json(HealthResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        status: "ok".to_string(),
        service: "sase_gateway".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        build: state.build,
        bind: state.bind,
        push: state.push_dispatcher.status(),
        fleet: FleetHealthWire {
            supported_protocol_versions: default_fleet_protocol_versions(),
        },
    })
}

async fn unknown_route(uri: Uri) -> ApiError {
    ApiError::not_found(uri.path())
}
