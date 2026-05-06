use std::{net::SocketAddr, path::PathBuf};

use axum::serve as axum_serve;
use thiserror::Error;
use tokio::net::TcpListener;

use crate::routes::{app_with_state, default_sase_home, GatewayState};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayConfig {
    pub bind: SocketAddr,
    pub sase_home: PathBuf,
    pub allow_non_loopback: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 7629)),
            sase_home: default_sase_home(),
            allow_non_loopback: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum GatewayRunError {
    #[error(
        "refusing to bind mobile gateway on non-loopback address {bind}; pass --allow-non-loopback/-L only for explicit LAN or tailnet exposure"
    )]
    NonLoopbackBind { bind: SocketAddr },
    #[error("failed to bind gateway listener on {bind}: {source}")]
    Bind {
        bind: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("gateway server failed: {0}")]
    Serve(#[from] std::io::Error),
}

pub async fn serve(config: GatewayConfig) -> Result<(), GatewayRunError> {
    validate_bind_policy(&config)?;
    let listener = TcpListener::bind(config.bind).await.map_err(|source| {
        GatewayRunError::Bind {
            bind: config.bind,
            source,
        }
    })?;
    serve_listener(listener, config.sase_home).await
}

pub fn validate_bind_policy(
    config: &GatewayConfig,
) -> Result<(), GatewayRunError> {
    if config.bind.ip().is_loopback() || config.allow_non_loopback {
        return Ok(());
    }
    Err(GatewayRunError::NonLoopbackBind { bind: config.bind })
}

pub async fn serve_listener(
    listener: TcpListener,
    sase_home: impl Into<PathBuf>,
) -> Result<(), GatewayRunError> {
    let local_addr = listener.local_addr().map_err(GatewayRunError::Serve)?;
    let state =
        GatewayState::new_with_sase_home(local_addr.to_string(), sase_home);
    axum_serve(listener, app_with_state(state))
        .await
        .map_err(GatewayRunError::Serve)
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;

    #[test]
    fn default_bind_is_loopback() {
        let config = GatewayConfig::default();
        assert!(config.bind.ip().is_loopback());
        assert!(!config.allow_non_loopback);
        validate_bind_policy(&config).unwrap();
    }

    #[test]
    fn non_loopback_bind_requires_explicit_opt_in() {
        let config = GatewayConfig {
            bind: "0.0.0.0:7629".parse().unwrap(),
            sase_home: default_sase_home(),
            allow_non_loopback: false,
        };

        let err = validate_bind_policy(&config).unwrap_err();
        assert_eq!(
            err.to_string(),
            "refusing to bind mobile gateway on non-loopback address 0.0.0.0:7629; pass --allow-non-loopback/-L only for explicit LAN or tailnet exposure"
        );
    }

    #[test]
    fn explicit_non_loopback_opt_in_is_allowed() {
        let config = GatewayConfig {
            bind: "0.0.0.0:7629".parse().unwrap(),
            sase_home: default_sase_home(),
            allow_non_loopback: true,
        };

        validate_bind_policy(&config).unwrap();
    }

    #[tokio::test]
    async fn listener_serves_health_over_http() {
        let tmp = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle =
            tokio::spawn(
                async move { serve_listener(listener, tmp.path()).await },
            );

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                b"GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains(r#""service":"sase_gateway""#));
        handle.abort();
    }

    #[tokio::test]
    async fn listener_smoke_exercises_pairing_auth_and_session() {
        let tmp = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn({
            let sase_home = tmp.path().to_path_buf();
            async move { serve_listener(listener, sase_home).await }
        });

        let (health_status, health) =
            request_json(addr, "GET", "/api/v1/health", None, None).await;
        assert_eq!(health_status, 200);
        assert_eq!(health["service"], "sase_gateway");

        let (start_status, start) = request_json(
            addr,
            "POST",
            "/api/v1/session/pair/start",
            None,
            Some(serde_json::json!({
                "schema_version": 1,
                "host_label": "workstation"
            })),
        )
        .await;
        assert_eq!(start_status, 200);

        let (finish_status, finish) = request_json(
            addr,
            "POST",
            "/api/v1/session/pair/finish",
            None,
            Some(serde_json::json!({
                "schema_version": 1,
                "pairing_id": start["pairing_id"],
                "code": start["code"],
                "device": {
                    "display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0"
                }
            })),
        )
        .await;
        assert_eq!(finish_status, 200);
        let token = finish["token"].as_str().unwrap().to_string();

        let (session_status, session) =
            request_json(addr, "GET", "/api/v1/session", Some(&token), None)
                .await;
        assert_eq!(session_status, 200);
        assert_eq!(
            session["device"]["device_id"],
            finish["device"]["device_id"]
        );

        handle.abort();
    }

    async fn request_json(
        addr: SocketAddr,
        method: &str,
        path: &str,
        bearer_token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> (u16, serde_json::Value) {
        let body = body
            .map(|value| serde_json::to_vec(&value).unwrap())
            .unwrap_or_default();
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        );
        if !body.is_empty() {
            request.push_str("Content-Type: application/json\r\n");
        }
        if let Some(token) = bearer_token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        request.push_str("\r\n");

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        if !body.is_empty() {
            stream.write_all(&body).await.unwrap();
        }

        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let status = head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let value = serde_json::from_str(body.trim()).unwrap();
        (status, value)
    }
}
