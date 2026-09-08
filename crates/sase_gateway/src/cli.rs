use std::{net::SocketAddr, path::PathBuf, time::Duration};

use crate::{
    contract::{
        write_api_v1_contract_snapshot, write_fleet_api_v1_contract_snapshot,
    },
    host_bridge::split_command_words,
    push::PushProviderMode,
    server::{serve, GatewayConfig},
};

pub fn run_gateway_cli(
    args: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    let cli = parse_gateway_args(args)?;
    let mut wrote_contract = false;
    if let Some(path) = cli.contract_out {
        write_api_v1_contract_snapshot(path).map_err(|err| {
            format!("failed to write gateway contract: {err}")
        })?;
        wrote_contract = true;
    }
    if let Some(path) = cli.fleet_contract_out {
        write_fleet_api_v1_contract_snapshot(path).map_err(|err| {
            format!("failed to write fleet gateway contract: {err}")
        })?;
        wrote_contract = true;
    }
    if wrote_contract {
        return Ok(());
    }
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;
    runtime
        .block_on(serve(cli.config))
        .map_err(|err| format!("sase gateway failed: {err}"))
}

#[derive(Debug, PartialEq, Eq)]
struct GatewayCli {
    config: GatewayConfig,
    contract_out: Option<PathBuf>,
    fleet_contract_out: Option<PathBuf>,
}

fn parse_gateway_args(
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
    let mut fleet_contract_out = None;
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
            "--fleet-contract-out" | "-G" => {
                let value = args.next().ok_or_else(|| {
                    format!("{arg} requires a JSON output path")
                })?;
                fleet_contract_out = Some(PathBuf::from(value));
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
                println!(
                    "Usage: sase_gateway [--bind|-b HOST:PORT] [--sase-home|-H DIR] [--allow-non-loopback|-L] [--contract-out|-o PATH] [--fleet-contract-out|-G PATH] [--agent-bridge-command|-A COMMAND] [--helper-bridge-command|-J COMMAND] [--push-provider|-P disabled|test|fcm]"
                );
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
        fleet_contract_out,
    })
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
    use std::fs;

    use super::*;

    #[test]
    fn parse_bind_short_flag() {
        let config =
            parse_gateway_args(["-b".to_string(), "127.0.0.1:0".to_string()])
                .unwrap();
        assert_eq!(
            config.config.bind,
            "127.0.0.1:0".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn parse_bind_long_flag() {
        let config = parse_gateway_args([
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
        let config = parse_gateway_args([
            "-H".to_string(),
            "/tmp/sase-home".to_string(),
        ])
        .unwrap();
        assert_eq!(config.config.sase_home, PathBuf::from("/tmp/sase-home"));
    }

    #[test]
    fn parse_allow_non_loopback_short_flag() {
        let config = parse_gateway_args(["-L".to_string()]).unwrap();
        assert!(config.config.allow_non_loopback);
    }

    #[test]
    fn parse_contract_out_short_flag() {
        let config = parse_gateway_args([
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
    fn parse_fleet_contract_out_short_flag() {
        let config = parse_gateway_args([
            "-G".to_string(),
            "/tmp/fleet-contract.json".to_string(),
        ])
        .unwrap();
        assert_eq!(
            config.fleet_contract_out,
            Some(PathBuf::from("/tmp/fleet-contract.json"))
        );
    }

    #[test]
    fn parse_agent_bridge_command_short_flag() {
        let config =
            parse_gateway_args(["-A".to_string(), "/tmp/sase".to_string()])
                .unwrap();
        assert_eq!(
            config.config.agent_bridge_command,
            vec!["/tmp/sase".to_string()]
        );
    }

    #[test]
    fn parse_helper_bridge_command_short_flag() {
        let config =
            parse_gateway_args(["-J".to_string(), "/tmp/sase".to_string()])
                .unwrap();
        assert_eq!(
            config.config.helper_bridge_command,
            vec!["/tmp/sase".to_string()]
        );
    }

    #[test]
    fn parse_push_flags() {
        let config = parse_gateway_args([
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
    fn unknown_argument_is_rejected() {
        let err = parse_gateway_args(["--bogus".to_string()]).unwrap_err();
        assert_eq!(err, "unknown argument: --bogus");
    }

    #[test]
    fn run_gateway_cli_writes_contract_outputs_without_serving() {
        let tmp = tempfile::tempdir().unwrap();
        let mobile = tmp.path().join("mobile.json");
        let fleet = tmp.path().join("fleet.json");

        run_gateway_cli([
            "--contract-out".to_string(),
            mobile.to_string_lossy().to_string(),
            "--fleet-contract-out".to_string(),
            fleet.to_string_lossy().to_string(),
        ])
        .unwrap();

        assert!(fs::read_to_string(mobile)
            .unwrap()
            .contains("sase_mobile_gateway_api_v1"));
        assert!(fs::read_to_string(fleet)
            .unwrap()
            .contains("sase_fleet_gateway_api_v1"));
    }
}
