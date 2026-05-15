use std::{net::SocketAddr, path::PathBuf};

use axum::serve as axum_serve;
use thiserror::Error;
use tokio::net::TcpListener;

use crate::{
    host_bridge::{CommandAgentHostBridge, CommandHelperHostBridge},
    push::PushConfig,
    routes::{app_with_state, default_sase_home, GatewayState},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayConfig {
    pub bind: SocketAddr,
    pub sase_home: PathBuf,
    pub allow_non_loopback: bool,
    pub agent_bridge_command: Vec<String>,
    pub helper_bridge_command: Vec<String>,
    pub push_config: PushConfig,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 7629)),
            sase_home: default_sase_home(),
            allow_non_loopback: false,
            agent_bridge_command: CommandAgentHostBridge::default_command(),
            helper_bridge_command: CommandHelperHostBridge::default_command(),
            push_config: PushConfig::default(),
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
    serve_listener_with_agent_bridge_command(
        listener,
        config.sase_home,
        config.agent_bridge_command,
        config.helper_bridge_command,
        config.push_config,
    )
    .await
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

pub async fn serve_listener_with_agent_bridge_command(
    listener: TcpListener,
    sase_home: impl Into<PathBuf>,
    agent_bridge_command: Vec<String>,
    helper_bridge_command: Vec<String>,
    push_config: PushConfig,
) -> Result<(), GatewayRunError> {
    let local_addr = listener.local_addr().map_err(GatewayRunError::Serve)?;
    let state = GatewayState::new_with_sase_home_and_bridge_commands(
        local_addr.to_string(),
        sase_home,
        agent_bridge_command,
        helper_bridge_command,
        push_config,
    );
    axum_serve(listener, app_with_state(state))
        .await
        .map_err(GatewayRunError::Serve)
}

pub async fn serve_listener_with_state(
    listener: TcpListener,
    state: GatewayState,
) -> Result<(), GatewayRunError> {
    axum_serve(listener, app_with_state(state))
        .await
        .map_err(GatewayRunError::Serve)
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time::{timeout, Duration},
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
            agent_bridge_command: vec!["sase".to_string()],
            helper_bridge_command: vec!["sase".to_string()],
            push_config: PushConfig::default(),
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
            agent_bridge_command: vec!["sase".to_string()],
            helper_bridge_command: vec!["sase".to_string()],
            push_config: PushConfig::default(),
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
            async move {
                serve_listener_with_agent_bridge_command(
                    listener,
                    sase_home,
                    vec!["sase".to_string()],
                    vec!["sase".to_string()],
                    PushConfig {
                        provider: crate::push::PushProviderMode::Test,
                        ..PushConfig::default()
                    },
                )
                .await
            }
        });

        let (health_status, health) =
            request_json(addr, "GET", "/api/v1/health", None, None).await;
        assert_eq!(health_status, 200);
        assert_eq!(health["service"], "sase_gateway");
        assert_eq!(health["push"]["provider"], "test");
        assert_eq!(health["push"]["enabled"], true);

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

        let (register_push_status, register_push) = request_json(
            addr,
            "POST",
            "/api/v1/session/push-subscriptions",
            Some(&token),
            Some(serde_json::json!({
                "schema_version": 1,
                "provider": "test",
                "provider_token": "test-provider-token",
                "app_instance_id": "listener-smoke-app",
                "platform": "android",
                "app_version": "0.1.0",
                "device_display_name": "Pixel 9",
                "hint_categories": ["notifications", "agents", "session"]
            })),
        )
        .await;
        assert_eq!(register_push_status, 200);
        assert_eq!(register_push["created"], true);
        assert_eq!(register_push["subscription"]["provider"], "test");
        assert_eq!(
            register_push["subscription"]["provider_token"],
            "test-provider-token"
        );

        let (list_push_status, list_push) = request_json(
            addr,
            "GET",
            "/api/v1/session/push-subscriptions",
            Some(&token),
            None,
        )
        .await;
        assert_eq!(list_push_status, 200);
        assert_eq!(list_push["subscriptions"].as_array().unwrap().len(), 1);

        let (events_status, events) =
            request_sse_events(addr, Some(&token), None, 2).await;
        assert_eq!(events_status, 200);
        assert_eq!(events[0]["payload"]["type"], "session");
        assert_eq!(events[1]["payload"]["type"], "heartbeat");
        assert_eq!(events[0]["id"], "0000000000000001");
        assert_eq!(events[1]["id"], "0000000000000002");

        let (resume_status, resume_events) =
            request_sse_events(addr, Some(&token), events[0]["id"].as_str(), 2)
                .await;
        assert_eq!(resume_status, 200);
        assert_eq!(resume_events[0], events[1]);
        assert_eq!(resume_events[1]["payload"]["type"], "heartbeat");
        assert_eq!(resume_events[1]["id"], "0000000000000003");

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

    async fn request_sse_events(
        addr: SocketAddr,
        bearer_token: Option<&str>,
        last_event_id: Option<&str>,
        event_count: usize,
    ) -> (u16, Vec<serde_json::Value>) {
        let mut request = String::from(
            "GET /api/v1/events HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAccept: text/event-stream\r\n",
        );
        if let Some(token) = bearer_token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        if let Some(last_event_id) = last_event_id {
            request.push_str(&format!("Last-Event-ID: {last_event_id}\r\n"));
        }
        request.push_str("\r\n");

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        timeout(Duration::from_secs(2), async {
            loop {
                let mut buffer = [0; 1024];
                let n = stream.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                response.push_str(std::str::from_utf8(&buffer[..n]).unwrap());
                if response.contains("\r\n\r\n")
                    && parse_sse_data(&response).len() >= event_count
                {
                    break;
                }
            }
        })
        .await
        .unwrap();

        let status = response
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        (status, parse_sse_data(&response))
    }

    fn parse_sse_data(response: &str) -> Vec<serde_json::Value> {
        response
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}
