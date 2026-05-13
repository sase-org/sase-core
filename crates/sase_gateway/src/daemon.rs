use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use thiserror::Error;
use tokio::net::TcpListener;

use crate::{
    ownership::{DaemonOwnershipGuard, OwnershipError},
    push::PushConfig,
    routes::GatewayState,
    server::{
        serve_listener_with_state, validate_bind_policy, GatewayConfig,
        GatewayRunError,
    },
    wire::GatewayBuildWire,
};

const DAEMON_SOCKET_NAME: &str = "sase-daemon.sock";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonRuntimePaths {
    pub sase_home: PathBuf,
    pub run_root: PathBuf,
    pub socket_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonConfig {
    pub paths: DaemonRuntimePaths,
    pub host_identity: String,
    pub foreground: bool,
    pub mobile_http_enabled: bool,
    pub mobile_gateway: GatewayConfig,
}

impl DaemonConfig {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        Self::with_options(
            sase_home,
            host_identity_from_env(),
            None,
            None,
            false,
            true,
            GatewayConfig::default(),
        )
    }

    pub fn with_options(
        sase_home: impl Into<PathBuf>,
        host_identity: impl Into<String>,
        run_root: Option<PathBuf>,
        socket_path: Option<PathBuf>,
        foreground: bool,
        mobile_http_enabled: bool,
        mut mobile_gateway: GatewayConfig,
    ) -> Self {
        let sase_home = sase_home.into();
        let host_identity = sanitize_host_identity(&host_identity.into());
        let run_root = run_root
            .unwrap_or_else(|| default_run_root(&sase_home, &host_identity));
        let socket_path =
            socket_path.unwrap_or_else(|| default_socket_path(&run_root));
        mobile_gateway.sase_home = sase_home.clone();
        Self {
            paths: DaemonRuntimePaths {
                sase_home,
                run_root,
                socket_path,
            },
            host_identity,
            foreground,
            mobile_http_enabled,
            mobile_gateway,
        }
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self::new(crate::routes::default_sase_home())
    }
}

#[derive(Clone, Debug)]
pub struct DaemonRuntime {
    state: DaemonState,
    ownership: Arc<DaemonOwnershipGuard>,
}

impl DaemonRuntime {
    pub fn new(config: DaemonConfig, ownership: DaemonOwnershipGuard) -> Self {
        let ownership = Arc::new(ownership);
        let state = DaemonState {
            build: GatewayBuildWire {
                package_version: env!("CARGO_PKG_VERSION").to_string(),
                git_sha: None,
            },
            host_identity: config.host_identity.clone(),
            paths: config.paths.clone(),
            shutdown: DaemonShutdown::default(),
            mobile_gateway: config
                .mobile_http_enabled
                .then_some(config.mobile_gateway.clone()),
        };
        Self { state, ownership }
    }

    pub fn state(&self) -> &DaemonState {
        &self.state
    }

    pub fn ownership(&self) -> &DaemonOwnershipGuard {
        &self.ownership
    }

    fn mobile_gateway_state(
        &self,
        local_addr: SocketAddr,
    ) -> Option<GatewayState> {
        let config = self.state.mobile_gateway.as_ref()?;
        Some(GatewayState::new_with_sase_home_and_bridge_commands(
            local_addr.to_string(),
            config.sase_home.clone(),
            config.agent_bridge_command.clone(),
            config.helper_bridge_command.clone(),
            config.push_config.clone(),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct DaemonState {
    pub build: GatewayBuildWire,
    pub host_identity: String,
    pub paths: DaemonRuntimePaths,
    pub shutdown: DaemonShutdown,
    pub mobile_gateway: Option<GatewayConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct DaemonShutdown {
    requested: Arc<AtomicBool>,
}

impl DaemonShutdown {
    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Error)]
pub enum DaemonRunError {
    #[error(transparent)]
    Ownership(#[from] OwnershipError),
    #[error(transparent)]
    MobileGateway(#[from] GatewayRunError),
    #[error("failed to wait for daemon shutdown signal: {0}")]
    ShutdownSignal(std::io::Error),
}

pub async fn run_daemon(config: DaemonConfig) -> Result<(), DaemonRunError> {
    validate_daemon_config(&config)?;
    let build = GatewayBuildWire {
        package_version: env!("CARGO_PKG_VERSION").to_string(),
        git_sha: None,
    };
    let ownership = DaemonOwnershipGuard::acquire(
        config.paths.run_root.clone(),
        config.paths.socket_path.clone(),
        config.host_identity.clone(),
        config.paths.sase_home.clone(),
        &build,
    )?;
    let runtime = DaemonRuntime::new(config, ownership);
    if let Some(mobile_gateway) = runtime.state().mobile_gateway.as_ref() {
        let listener =
            TcpListener::bind(mobile_gateway.bind)
                .await
                .map_err(|source| GatewayRunError::Bind {
                    bind: mobile_gateway.bind,
                    source,
                })?;
        let local_addr =
            listener.local_addr().map_err(GatewayRunError::Serve)?;
        let state = runtime.mobile_gateway_state(local_addr).expect(
            "mobile gateway state is available when mobile HTTP is enabled",
        );
        return serve_listener_with_state(listener, state)
            .await
            .map_err(DaemonRunError::from);
    }
    tokio::signal::ctrl_c()
        .await
        .map_err(DaemonRunError::ShutdownSignal)
}

pub fn validate_daemon_config(
    config: &DaemonConfig,
) -> Result<(), GatewayRunError> {
    if config.mobile_http_enabled {
        validate_bind_policy(&config.mobile_gateway)?;
    }
    Ok(())
}

pub fn default_run_root(sase_home: &Path, host_identity: &str) -> PathBuf {
    sase_home
        .join("run")
        .join(sanitize_host_identity(host_identity))
}

pub fn default_socket_path(run_root: &Path) -> PathBuf {
    run_root.join(DAEMON_SOCKET_NAME)
}

pub fn host_identity_from_env() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| sanitize_host_identity(&value))
        .unwrap_or_else(|| "sase-host".to_string())
}

pub fn sanitize_host_identity(value: &str) -> String {
    let sanitized: String = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "sase-host".to_string()
    } else {
        sanitized.to_string()
    }
}

pub fn mobile_gateway_config(
    bind: SocketAddr,
    sase_home: PathBuf,
    allow_non_loopback: bool,
    agent_bridge_command: Vec<String>,
    helper_bridge_command: Vec<String>,
    push_config: PushConfig,
) -> GatewayConfig {
    GatewayConfig {
        bind,
        sase_home,
        allow_non_loopback,
        agent_bridge_command,
        helper_bridge_command,
        push_config,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::GatewayConfig;

    #[test]
    fn daemon_default_paths_are_host_local_under_sase_home() {
        let config = DaemonConfig::with_options(
            "/tmp/sase-home",
            "workstation.local",
            None,
            None,
            false,
            true,
            GatewayConfig::default(),
        );

        assert_eq!(config.paths.sase_home, PathBuf::from("/tmp/sase-home"));
        assert_eq!(
            config.paths.run_root,
            PathBuf::from("/tmp/sase-home/run/workstation.local")
        );
        assert_eq!(
            config.paths.socket_path,
            PathBuf::from(
                "/tmp/sase-home/run/workstation.local/sase-daemon.sock"
            )
        );
    }

    #[test]
    fn daemon_run_root_override_drives_default_socket_path() {
        let config = DaemonConfig::with_options(
            "/tmp/sase-home",
            "workstation",
            Some(PathBuf::from("/tmp/run-root")),
            None,
            true,
            false,
            GatewayConfig::default(),
        );

        assert_eq!(config.paths.run_root, PathBuf::from("/tmp/run-root"));
        assert_eq!(
            config.paths.socket_path,
            PathBuf::from("/tmp/run-root/sase-daemon.sock")
        );
        assert!(config.foreground);
        assert!(!config.mobile_http_enabled);
    }

    #[test]
    fn daemon_socket_path_override_is_preserved() {
        let config = DaemonConfig::with_options(
            "/tmp/sase-home",
            "workstation",
            Some(PathBuf::from("/tmp/run-root")),
            Some(PathBuf::from("/tmp/custom.sock")),
            false,
            true,
            GatewayConfig::default(),
        );

        assert_eq!(config.paths.socket_path, PathBuf::from("/tmp/custom.sock"));
    }

    #[test]
    fn daemon_host_identity_is_path_safe() {
        assert_eq!(
            sanitize_host_identity("work station/01"),
            "work-station-01"
        );
        assert_eq!(sanitize_host_identity("  "), "sase-host");
    }

    #[test]
    fn daemon_mobile_bind_policy_is_skipped_when_mobile_http_disabled() {
        let config = DaemonConfig::with_options(
            "/tmp/sase-home",
            "workstation",
            None,
            None,
            false,
            false,
            GatewayConfig {
                bind: "0.0.0.0:7629".parse().unwrap(),
                ..GatewayConfig::default()
            },
        );

        validate_daemon_config(&config).unwrap();
    }
}
