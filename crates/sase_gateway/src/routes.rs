use std::net::SocketAddr;

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use tower_http::trace::TraceLayer;

use crate::wire::{
    ApiErrorCodeWire, ApiErrorWire, GatewayBindWire, GatewayBuildWire,
    HealthResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

#[derive(Clone, Debug)]
pub struct GatewayState {
    bind: GatewayBindWire,
    build: GatewayBuildWire,
}

impl GatewayState {
    pub fn new(bind_addr: String) -> Self {
        let is_loopback = bind_addr
            .parse::<SocketAddr>()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false);
        Self {
            bind: GatewayBindWire {
                address: bind_addr,
                is_loopback,
            },
            build: GatewayBuildWire {
                package_version: env!("CARGO_PKG_VERSION").to_string(),
                git_sha: None,
            },
        }
    }
}

pub fn app(bind_addr: impl Into<String>) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/session", get(unauthorized_placeholder))
        .route("/api/v1/events", get(unauthorized_placeholder))
        .fallback(unknown_route)
        .layer(TraceLayer::new_for_http())
        .with_state(GatewayState::new(bind_addr.into()))
}

async fn health(State(state): State<GatewayState>) -> Json<HealthResponseWire> {
    Json(HealthResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        status: "ok".to_string(),
        service: "sase_gateway".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        build: state.build,
        bind: state.bind,
    })
}

async fn unauthorized_placeholder(headers: HeaderMap) -> ApiError {
    let target = headers
        .get(header::AUTHORIZATION)
        .map(|_| "authorization")
        .unwrap_or("authorization");
    ApiError::unauthorized(target)
}

async fn unknown_route(uri: Uri) -> ApiError {
    ApiError::not_found(uri.path())
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    wire: ApiErrorWire,
}

impl ApiError {
    fn unauthorized(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Unauthorized,
                message: "authentication is required for this endpoint"
                    .to_string(),
                target: Some(target.into()),
                details: None,
            },
        }
    }

    fn not_found(path: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "route not found".to_string(),
                target: Some(path.into()),
                details: None,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.wire)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use super::*;

    async fn json_response(request: Request<Body>) -> (StatusCode, Value) {
        let response = app("127.0.0.1:0").oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap();
        (status, value)
    }

    #[tokio::test]
    async fn health_route_returns_stable_record() {
        let (status, value) = json_response(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            value,
            json!({
                "schema_version": 1,
                "status": "ok",
                "service": "sase_gateway",
                "version": env!("CARGO_PKG_VERSION"),
                "build": {
                    "package_version": env!("CARGO_PKG_VERSION"),
                    "git_sha": null
                },
                "bind": {
                    "address": "127.0.0.1:0",
                    "is_loopback": true
                }
            })
        );
    }

    #[tokio::test]
    async fn authenticated_placeholder_returns_typed_unauthorized_error() {
        let (status, value) = json_response(
            Request::builder()
                .uri("/api/v1/session")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            value,
            json!({
                "schema_version": 1,
                "code": "unauthorized",
                "message": "authentication is required for this endpoint",
                "target": "authorization",
                "details": null
            })
        );
    }

    #[tokio::test]
    async fn unknown_route_returns_typed_not_found_error() {
        let (status, value) = json_response(
            Request::builder()
                .uri("/api/v1/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            value,
            json!({
                "schema_version": 1,
                "code": "not_found",
                "message": "route not found",
                "target": "/api/v1/nope",
                "details": null
            })
        );
    }
}
