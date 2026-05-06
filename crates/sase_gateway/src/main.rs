use std::{net::SocketAddr, path::PathBuf};

use sase_gateway::{
    serve, split_command_words, write_api_v1_contract_snapshot, GatewayConfig,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cli = parse_args(std::env::args().skip(1))?;
    if let Some(path) = cli.contract_out {
        write_api_v1_contract_snapshot(path).map_err(|err| {
            format!("failed to write gateway contract: {err}")
        })?;
        return Ok(());
    }
    serve(cli.config)
        .await
        .map_err(|err| format!("sase gateway failed: {err}"))
}

#[derive(Debug, PartialEq, Eq)]
struct GatewayCli {
    config: GatewayConfig,
    contract_out: Option<PathBuf>,
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<GatewayCli, String> {
    let mut bind = GatewayConfig::default().bind;
    let mut sase_home = GatewayConfig::default().sase_home;
    let mut allow_non_loopback = GatewayConfig::default().allow_non_loopback;
    let mut agent_bridge_command =
        GatewayConfig::default().agent_bridge_command;
    let mut helper_bridge_command =
        GatewayConfig::default().helper_bridge_command;
    let mut contract_out = None;
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
            "--help" | "-h" => {
                println!(
                    "Usage: sase_gateway [--bind|-b HOST:PORT] [--sase-home|-H DIR] [--allow-non-loopback|-L] [--contract-out|-o PATH] [--agent-bridge-command|-A COMMAND] [--helper-bridge-command|-J COMMAND]"
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
        },
        contract_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bind_short_flag() {
        let config =
            parse_args(["-b".to_string(), "127.0.0.1:0".to_string()]).unwrap();
        assert_eq!(
            config.config.bind,
            "127.0.0.1:0".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn parse_bind_long_flag() {
        let config =
            parse_args(["--bind".to_string(), "127.0.0.1:7629".to_string()])
                .unwrap();
        assert_eq!(
            config.config.bind,
            "127.0.0.1:7629".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn parse_sase_home_short_flag() {
        let config =
            parse_args(["-H".to_string(), "/tmp/sase-home".to_string()])
                .unwrap();
        assert_eq!(config.config.sase_home, PathBuf::from("/tmp/sase-home"));
    }

    #[test]
    fn parse_allow_non_loopback_short_flag() {
        let config = parse_args(["-L".to_string()]).unwrap();
        assert!(config.config.allow_non_loopback);
    }

    #[test]
    fn parse_contract_out_short_flag() {
        let config =
            parse_args(["-o".to_string(), "/tmp/contract.json".to_string()])
                .unwrap();
        assert_eq!(
            config.contract_out,
            Some(PathBuf::from("/tmp/contract.json"))
        );
    }

    #[test]
    fn parse_agent_bridge_command_short_flag() {
        let config =
            parse_args(["-A".to_string(), "/tmp/sase".to_string()]).unwrap();
        assert_eq!(
            config.config.agent_bridge_command,
            vec!["/tmp/sase".to_string()]
        );
    }

    #[test]
    fn parse_helper_bridge_command_short_flag() {
        let config =
            parse_args(["-J".to_string(), "/tmp/sase".to_string()]).unwrap();
        assert_eq!(
            config.config.helper_bridge_command,
            vec!["/tmp/sase".to_string()]
        );
    }
}
