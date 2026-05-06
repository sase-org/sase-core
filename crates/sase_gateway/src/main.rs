use std::{net::SocketAddr, path::PathBuf};

use sase_gateway::{serve, GatewayConfig};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_args(std::env::args().skip(1))?;
    serve(config)
        .await
        .map_err(|err| format!("sase gateway failed: {err}"))
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<GatewayConfig, String> {
    let mut bind = GatewayConfig::default().bind;
    let mut sase_home = GatewayConfig::default().sase_home;
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
            "--help" | "-h" => {
                println!(
                    "Usage: sase_gateway [--bind|-b HOST:PORT] [--sase-home|-H DIR]"
                );
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(GatewayConfig { bind, sase_home })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bind_short_flag() {
        let config =
            parse_args(["-b".to_string(), "127.0.0.1:0".to_string()]).unwrap();
        assert_eq!(config.bind, "127.0.0.1:0".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn parse_bind_long_flag() {
        let config =
            parse_args(["--bind".to_string(), "127.0.0.1:7629".to_string()])
                .unwrap();
        assert_eq!(
            config.bind,
            "127.0.0.1:7629".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn parse_sase_home_short_flag() {
        let config =
            parse_args(["-H".to_string(), "/tmp/sase-home".to_string()])
                .unwrap();
        assert_eq!(config.sase_home, PathBuf::from("/tmp/sase-home"));
    }
}
