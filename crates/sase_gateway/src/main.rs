use std::{net::SocketAddr, path::PathBuf, time::Duration};

use sase_gateway::{
    run_daemon, serve, split_command_words, write_api_v1_contract_snapshot,
    write_local_daemon_contract_snapshot, DaemonConfig, GatewayConfig,
    PushProviderMode,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    match parse_args(std::env::args().skip(1))? {
        GatewayCommand::Mobile(cli) => {
            if let Some(path) = cli.contract_out {
                write_api_v1_contract_snapshot(path).map_err(|err| {
                    format!("failed to write gateway contract: {err}")
                })?;
                return Ok(());
            }
            if let Some(path) = cli.local_daemon_contract_out {
                write_local_daemon_contract_snapshot(path).map_err(|err| {
                    format!("failed to write local daemon contract: {err}")
                })?;
                return Ok(());
            }
            serve(cli.config)
                .await
                .map_err(|err| format!("sase gateway failed: {err}"))
        }
        GatewayCommand::Daemon(cli) => run_daemon(cli.config)
            .await
            .map_err(|err| format!("sase daemon failed: {err}")),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum GatewayCommand {
    Mobile(GatewayCli),
    Daemon(DaemonCli),
}

#[derive(Debug, PartialEq, Eq)]
struct GatewayCli {
    config: GatewayConfig,
    contract_out: Option<PathBuf>,
    local_daemon_contract_out: Option<PathBuf>,
}

#[derive(Debug, PartialEq, Eq)]
struct DaemonCli {
    config: DaemonConfig,
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<GatewayCommand, String> {
    let args: Vec<String> = args.into_iter().collect();
    if args.first().map(String::as_str) == Some("daemon") {
        return parse_daemon_args(args.into_iter().skip(1))
            .map(GatewayCommand::Daemon);
    }
    parse_mobile_args(args).map(GatewayCommand::Mobile)
}

fn parse_mobile_args(
    args: impl IntoIterator<Item = String>,
) -> Result<GatewayCli, String> {
    let mut bind = GatewayConfig::default().bind;
    let mut sase_home = GatewayConfig::default().sase_home;
    let mut allow_non_loopback = GatewayConfig::default().allow_non_loopback;
    let mut agent_bridge_command =
        GatewayConfig::default().agent_bridge_command;
    let mut helper_bridge_command =
        GatewayConfig::default().helper_bridge_command;
    let mut push_config = GatewayConfig::default().push_config;
    let mut contract_out = None;
    let mut local_daemon_contract_out = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" | "-b" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a host:port value")
                })?;
                bind = value.parse::<SocketAddr>().map_err(|err| {
                    format!("invalid {arg} value {value:?}: {err}")
                })?;
            }
            "--sase-home" | "-H" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a directory path")
                })?;
                sase_home = PathBuf::from(value);
            }
            "--allow-non-loopback" | "-L" => {
                allow_non_loopback = true;
            }
            "--contract-out" | "-o" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a JSON output path")
                })?;
                contract_out = Some(PathBuf::from(value));
            }
            "--local-daemon-contract-out" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a JSON output path")
                })?;
                local_daemon_contract_out = Some(PathBuf::from(value));
            }
            "--agent-bridge-command" | "-A" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a command path"))?;
                agent_bridge_command = split_command_words(&value)
                    .map_err(|err| format!("invalid {arg} value: {err}"))?;
                if agent_bridge_command.is_empty() {
                    return Err(format!("{arg} requires a command path"));
                }
            }
            "--helper-bridge-command" | "-J" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a command path"))?;
                helper_bridge_command = split_command_words(&value)
                    .map_err(|err| format!("invalid {arg} value: {err}"))?;
                if helper_bridge_command.is_empty() {
                    return Err(format!("{arg} requires a command path"));
                }
            }
            "--push-provider" | "-P" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires disabled, test, or fcm")
                })?;
                push_config.provider = parse_push_provider(&value)?;
            }
            "--fcm-project-id" | "-F" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a project id"))?;
                push_config.fcm_project_id = non_empty(value);
            }
            "--fcm-service-account-json" | "-S" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a JSON path"))?;
                push_config.fcm_service_account_json_path =
                    non_empty(value).map(PathBuf::from);
            }
            "--fcm-credential-env" | "-E" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires an env var name"))?;
                push_config.fcm_credential_env = non_empty(value);
            }
            "--fcm-dry-run" | "-D" => {
                push_config.fcm_dry_run = true;
            }
            "--push-timeout-seconds" | "-U" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a seconds value"))?;
                let seconds = value.parse::<f64>().map_err(|err| {
                    format!("invalid {arg} value {value:?}: {err}")
                })?;
                if seconds <= 0.0 {
                    return Err(format!(
                        "{arg} requires a positive seconds value"
                    ));
                }
                push_config.timeout = Duration::from_secs_f64(seconds);
            }
            "--push-retry-limit" | "-R" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a retry count"))?;
                push_config.retry_limit =
                    value.parse::<u32>().map_err(|err| {
                        format!("invalid {arg} value {value:?}: {err}")
                    })?;
            }
            "--fcm-endpoint" | "-M" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a base URL"))?;
                push_config.fcm_endpoint = non_empty(value);
            }
            "--help" | "-h" => {
                print_gateway_help();
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(GatewayCli {
        config: GatewayConfig {
            bind,
            sase_home,
            allow_non_loopback,
            agent_bridge_command,
            helper_bridge_command,
            push_config,
        },
        contract_out,
        local_daemon_contract_out,
    })
}

fn parse_daemon_args(
    args: impl IntoIterator<Item = String>,
) -> Result<DaemonCli, String> {
    let mut bind = GatewayConfig::default().bind;
    let mut sase_home = GatewayConfig::default().sase_home;
    let mut allow_non_loopback = GatewayConfig::default().allow_non_loopback;
    let mut agent_bridge_command =
        GatewayConfig::default().agent_bridge_command;
    let mut helper_bridge_command =
        GatewayConfig::default().helper_bridge_command;
    let mut push_config = GatewayConfig::default().push_config;
    let mut run_root = None;
    let mut socket_path = None;
    let mut foreground = false;
    let mut mobile_http_enabled = true;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sase-home" | "-H" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a directory path")
                })?;
                sase_home = PathBuf::from(value);
            }
            "--run-root" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a directory path")
                })?;
                run_root = Some(PathBuf::from(value));
            }
            "--socket-path" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a socket path"))?;
                socket_path = Some(PathBuf::from(value));
            }
            "--foreground" => {
                foreground = true;
            }
            "--disable-mobile-http" => {
                mobile_http_enabled = false;
            }
            "--bind" | "-b" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a host:port value")
                })?;
                bind = value.parse::<SocketAddr>().map_err(|err| {
                    format!("invalid {arg} value {value:?}: {err}")
                })?;
            }
            "--allow-non-loopback" | "-L" => {
                allow_non_loopback = true;
            }
            "--agent-bridge-command" | "-A" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a command path"))?;
                agent_bridge_command = split_command_words(&value)
                    .map_err(|err| format!("invalid {arg} value: {err}"))?;
                if agent_bridge_command.is_empty() {
                    return Err(format!("{arg} requires a command path"));
                }
            }
            "--helper-bridge-command" | "-J" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a command path"))?;
                helper_bridge_command = split_command_words(&value)
                    .map_err(|err| format!("invalid {arg} value: {err}"))?;
                if helper_bridge_command.is_empty() {
                    return Err(format!("{arg} requires a command path"));
                }
            }
            "--push-provider" | "-P" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires disabled, test, or fcm")
                })?;
                push_config.provider = parse_push_provider(&value)?;
            }
            "--fcm-project-id" | "-F" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a project id"))?;
                push_config.fcm_project_id = non_empty(value);
            }
            "--fcm-service-account-json" | "-S" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a JSON path"))?;
                push_config.fcm_service_account_json_path =
                    non_empty(value).map(PathBuf::from);
            }
            "--fcm-credential-env" | "-E" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires an env var name"))?;
                push_config.fcm_credential_env = non_empty(value);
            }
            "--fcm-dry-run" | "-D" => {
                push_config.fcm_dry_run = true;
            }
            "--push-timeout-seconds" | "-U" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a seconds value"))?;
                let seconds = value.parse::<f64>().map_err(|err| {
                    format!("invalid {arg} value {value:?}: {err}")
                })?;
                if seconds <= 0.0 {
                    return Err(format!(
                        "{arg} requires a positive seconds value"
                    ));
                }
                push_config.timeout = Duration::from_secs_f64(seconds);
            }
            "--push-retry-limit" | "-R" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a retry count"))?;
                push_config.retry_limit =
                    value.parse::<u32>().map_err(|err| {
                        format!("invalid {arg} value {value:?}: {err}")
                    })?;
            }
            "--fcm-endpoint" | "-M" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{arg} requires a base URL"))?;
                push_config.fcm_endpoint = non_empty(value);
            }
            "--help" | "-h" => {
                print_daemon_help();
                std::process::exit(0);
            }
            _ => return Err(format!("unknown daemon argument: {arg}")),
        }
    }
    let mobile_gateway = GatewayConfig {
        bind,
        sase_home: sase_home.clone(),
        allow_non_loopback,
        agent_bridge_command,
        helper_bridge_command,
        push_config,
    };
    let config = DaemonConfig::with_options(
        sase_home,
        sase_gateway::host_identity_from_env(),
        run_root,
        socket_path,
        foreground,
        mobile_http_enabled,
        mobile_gateway,
    );
    Ok(DaemonCli { config })
}

fn print_gateway_help() {
    println!(
        "Usage: sase_gateway [mobile options]\n       sase_gateway daemon [daemon options]\n\nDefault mode serves the mobile HTTP gateway. Use the daemon subcommand for the local SASE state daemon skeleton.\n\nMobile options: [--bind|-b HOST:PORT] [--sase-home|-H DIR] [--allow-non-loopback|-L] [--contract-out|-o PATH] [--local-daemon-contract-out PATH] [--agent-bridge-command|-A COMMAND] [--helper-bridge-command|-J COMMAND] [--push-provider|-P disabled|test|fcm]"
    );
}

fn print_daemon_help() {
    println!(
        "Usage: sase_gateway daemon [--sase-home|-H DIR] [--run-root DIR] [--socket-path PATH] [--foreground] [--disable-mobile-http] [mobile HTTP options]\n\nDaemon mode starts the local SASE state daemon skeleton. Mobile HTTP remains enabled by default and can be configured with --bind, --allow-non-loopback, bridge, and push options."
    );
}

fn parse_push_provider(value: &str) -> Result<PushProviderMode, String> {
    match value {
        "disabled" => Ok(PushProviderMode::Disabled),
        "test" => Ok(PushProviderMode::Test),
        "fcm" => Ok(PushProviderMode::Fcm),
        _ => Err(format!("invalid push provider {value:?}")),
    }
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bind_short_flag() {
        let config =
            parse_mobile_args(["-b".to_string(), "127.0.0.1:0".to_string()])
                .unwrap();
        assert_eq!(
            config.config.bind,
            "127.0.0.1:0".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn parse_bind_long_flag() {
        let config = parse_mobile_args([
            "--bind".to_string(),
            "127.0.0.1:7629".to_string(),
        ])
        .unwrap();
        assert_eq!(
            config.config.bind,
            "127.0.0.1:7629".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn parse_sase_home_short_flag() {
        let config =
            parse_mobile_args(["-H".to_string(), "/tmp/sase-home".to_string()])
                .unwrap();
        assert_eq!(config.config.sase_home, PathBuf::from("/tmp/sase-home"));
    }

    #[test]
    fn parse_allow_non_loopback_short_flag() {
        let config = parse_mobile_args(["-L".to_string()]).unwrap();
        assert!(config.config.allow_non_loopback);
    }

    #[test]
    fn parse_contract_out_short_flag() {
        let config = parse_mobile_args([
            "-o".to_string(),
            "/tmp/contract.json".to_string(),
        ])
        .unwrap();
        assert_eq!(
            config.contract_out,
            Some(PathBuf::from("/tmp/contract.json"))
        );
    }

    #[test]
    fn parse_local_daemon_contract_out_flag() {
        let config = parse_mobile_args([
            "--local-daemon-contract-out".to_string(),
            "/tmp/local-daemon-contract.json".to_string(),
        ])
        .unwrap();
        assert_eq!(
            config.local_daemon_contract_out,
            Some(PathBuf::from("/tmp/local-daemon-contract.json"))
        );
    }

    #[test]
    fn parse_agent_bridge_command_short_flag() {
        let config =
            parse_mobile_args(["-A".to_string(), "/tmp/sase".to_string()])
                .unwrap();
        assert_eq!(
            config.config.agent_bridge_command,
            vec!["/tmp/sase".to_string()]
        );
    }

    #[test]
    fn parse_helper_bridge_command_short_flag() {
        let config =
            parse_mobile_args(["-J".to_string(), "/tmp/sase".to_string()])
                .unwrap();
        assert_eq!(
            config.config.helper_bridge_command,
            vec!["/tmp/sase".to_string()]
        );
    }

    #[test]
    fn parse_push_flags() {
        let config = parse_mobile_args([
            "-P".to_string(),
            "fcm".to_string(),
            "-F".to_string(),
            "project-123".to_string(),
            "-S".to_string(),
            "/tmp/service-account.json".to_string(),
            "-E".to_string(),
            "SASE_FCM_TOKEN".to_string(),
            "-D".to_string(),
            "-U".to_string(),
            "2.5".to_string(),
            "-R".to_string(),
            "3".to_string(),
        ])
        .unwrap();

        assert_eq!(config.config.push_config.provider, PushProviderMode::Fcm);
        assert_eq!(
            config.config.push_config.fcm_project_id.as_deref(),
            Some("project-123")
        );
        assert_eq!(
            config.config.push_config.fcm_service_account_json_path,
            Some(PathBuf::from("/tmp/service-account.json"))
        );
        assert_eq!(
            config.config.push_config.fcm_credential_env.as_deref(),
            Some("SASE_FCM_TOKEN")
        );
        assert!(config.config.push_config.fcm_dry_run);
        assert_eq!(
            config.config.push_config.timeout,
            Duration::from_millis(2500)
        );
        assert_eq!(config.config.push_config.retry_limit, 3);
    }

    #[test]
    fn parse_args_defaults_to_mobile_gateway_mode() {
        let command =
            parse_args(["-b".to_string(), "127.0.0.1:0".to_string()]).unwrap();

        match command {
            GatewayCommand::Mobile(cli) => {
                assert_eq!(
                    cli.config.bind,
                    "127.0.0.1:0".parse::<SocketAddr>().unwrap()
                );
            }
            GatewayCommand::Daemon(_) => panic!("expected mobile mode"),
        }
    }

    #[test]
    fn parse_daemon_flags_build_daemon_config() {
        let command = parse_args([
            "daemon".to_string(),
            "--sase-home".to_string(),
            "/tmp/sase-home".to_string(),
            "--run-root".to_string(),
            "/tmp/sase-run".to_string(),
            "--socket-path".to_string(),
            "/tmp/sase.sock".to_string(),
            "--foreground".to_string(),
            "--disable-mobile-http".to_string(),
            "--bind".to_string(),
            "127.0.0.1:0".to_string(),
            "--push-provider".to_string(),
            "test".to_string(),
        ])
        .unwrap();

        match command {
            GatewayCommand::Daemon(cli) => {
                assert_eq!(
                    cli.config.paths.sase_home,
                    PathBuf::from("/tmp/sase-home")
                );
                assert_eq!(
                    cli.config.paths.run_root,
                    PathBuf::from("/tmp/sase-run")
                );
                assert_eq!(
                    cli.config.paths.socket_path,
                    PathBuf::from("/tmp/sase.sock")
                );
                assert!(cli.config.foreground);
                assert!(!cli.config.mobile_http_enabled);
                assert_eq!(
                    cli.config.mobile_gateway.bind,
                    "127.0.0.1:0".parse::<SocketAddr>().unwrap()
                );
                assert_eq!(
                    cli.config.mobile_gateway.push_config.provider,
                    PushProviderMode::Test
                );
            }
            GatewayCommand::Mobile(_) => panic!("expected daemon mode"),
        }
    }

    #[test]
    fn parse_daemon_run_root_derives_socket_default() {
        let cli = parse_daemon_args([
            "--sase-home".to_string(),
            "/tmp/sase-home".to_string(),
            "--run-root".to_string(),
            "/tmp/sase-run".to_string(),
        ])
        .unwrap();

        assert_eq!(cli.config.paths.run_root, PathBuf::from("/tmp/sase-run"));
        assert_eq!(
            cli.config.paths.socket_path,
            PathBuf::from("/tmp/sase-run/sase-daemon.sock")
        );
    }
}
