use std::{net::SocketAddr, path::PathBuf};

use axum::serve as axum_serve;
use thiserror::Error;
use tokio::net::TcpListener;

use crate::routes::{app_with_state, default_sase_home, GatewayState};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayConfig {
    pub bind: SocketAddr,
    pub sase_home: PathBuf,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 7629)),
            sase_home: default_sase_home(),
        }
    }
}

#[derive(Debug, Error)]
pub enum GatewayRunError {
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
    let listener = TcpListener::bind(config.bind).await.map_err(|source| {
        GatewayRunError::Bind {
            bind: config.bind,
            source,
        }
    })?;
    serve_listener(listener, config.sase_home).await
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
        assert!(GatewayConfig::default().bind.ip().is_loopback());
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
}
