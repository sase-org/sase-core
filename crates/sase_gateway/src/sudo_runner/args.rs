use std::path::PathBuf;

use sase_core::{SudoErrorCodeWire, SudoWireError};

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SudoRunnerCli {
    Capabilities,
    Execute(SudoRunnerManifestCli),
    Detach(SudoRunnerDetachCli),
    InternalRootExec(SudoRunnerInternalCli),
    InternalRootWorker(SudoRunnerInternalCli),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SudoRunnerManifestCli {
    pub(crate) manifest_path: PathBuf,
    pub(crate) expected_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SudoRunnerDetachCli {
    pub(crate) manifest_path: PathBuf,
    pub(crate) expected_sha256: String,
    pub(crate) detach_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SudoRunnerInternalCli {
    pub(crate) manifest_path: PathBuf,
    pub(crate) expected_sha256: String,
    pub(crate) detach_dir: PathBuf,
    pub(crate) started_path: Option<PathBuf>,
    pub(crate) parent_uid: u32,
    pub(crate) parent_gid: u32,
}

pub(crate) enum ParseResult {
    Help,
    Cli(SudoRunnerCli),
    Error(String),
}

pub(crate) fn parse_sudo_runner_args(
    args: impl IntoIterator<Item = String>,
) -> ParseResult {
    let mut manifest_path = None;
    let mut expected_sha256 = None;
    let mut detach_dir = None;
    let mut started_path = None;
    let mut parent_uid = None;
    let mut parent_gid = None;
    let mut capabilities = false;
    let mut internal_root_exec = false;
    let mut internal_root_worker = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--capabilities" | "-c" => {
                capabilities = true;
            }
            "--detach-dir" | "-d" => {
                let Some(value) = args.next() else {
                    return ParseResult::Error(format!(
                        "{arg} requires a handoff directory"
                    ));
                };
                detach_dir = Some(PathBuf::from(value));
            }
            "--manifest" | "-m" => {
                let Some(value) = args.next() else {
                    return ParseResult::Error(format!(
                        "{arg} requires a manifest path"
                    ));
                };
                manifest_path = Some(PathBuf::from(value));
            }
            "--expected-sha256" | "-e" => {
                let Some(value) = args.next() else {
                    return ParseResult::Error(format!(
                        "{arg} requires a lowercase SHA-256 digest"
                    ));
                };
                expected_sha256 = Some(value);
            }
            "--help" | "-h" => return ParseResult::Help,
            INTERNAL_ROOT_EXEC_FLAG => {
                internal_root_exec = true;
            }
            INTERNAL_ROOT_WORKER_FLAG => {
                internal_root_worker = true;
            }
            "--parent-gid" => {
                let Some(value) = args.next() else {
                    return ParseResult::Error(format!(
                        "{arg} requires a numeric gid"
                    ));
                };
                parent_gid = match value.parse::<u32>() {
                    Ok(value) => Some(value),
                    Err(_) => {
                        return ParseResult::Error(format!(
                            "{arg} requires a numeric gid"
                        ));
                    }
                };
            }
            "--parent-uid" => {
                let Some(value) = args.next() else {
                    return ParseResult::Error(format!(
                        "{arg} requires a numeric uid"
                    ));
                };
                parent_uid = match value.parse::<u32>() {
                    Ok(value) => Some(value),
                    Err(_) => {
                        return ParseResult::Error(format!(
                            "{arg} requires a numeric uid"
                        ));
                    }
                };
            }
            "--started-path" => {
                let Some(value) = args.next() else {
                    return ParseResult::Error(format!(
                        "{arg} requires a started-handshake path"
                    ));
                };
                started_path = Some(PathBuf::from(value));
            }
            _ => {
                return ParseResult::Error(format!("unknown argument: {arg}"));
            }
        }
    }
    let internal_count =
        usize::from(internal_root_exec) + usize::from(internal_root_worker);
    if internal_count > 1 {
        return ParseResult::Error(
            "internal executor modes are mutually exclusive".to_string(),
        );
    }
    if capabilities {
        if manifest_path.is_some()
            || expected_sha256.is_some()
            || detach_dir.is_some()
            || started_path.is_some()
            || parent_uid.is_some()
            || parent_gid.is_some()
            || internal_count > 0
        {
            return ParseResult::Error(
                "--capabilities|-c cannot be combined with other arguments"
                    .to_string(),
            );
        }
        return ParseResult::Cli(SudoRunnerCli::Capabilities);
    }
    let Some(manifest_path) = manifest_path else {
        return ParseResult::Error("--manifest|-m is required".to_string());
    };
    let Some(expected_sha256) = expected_sha256 else {
        return ParseResult::Error(
            "--expected-sha256|-e is required".to_string(),
        );
    };
    if internal_count > 0 {
        let Some(detach_dir) = detach_dir else {
            return ParseResult::Error(
                "--detach-dir|-d is required for internal executor mode"
                    .to_string(),
            );
        };
        let Some(parent_uid) = parent_uid else {
            return ParseResult::Error(
                "--parent-uid is required for internal executor mode"
                    .to_string(),
            );
        };
        let Some(parent_gid) = parent_gid else {
            return ParseResult::Error(
                "--parent-gid is required for internal executor mode"
                    .to_string(),
            );
        };
        let cli = SudoRunnerInternalCli {
            manifest_path,
            expected_sha256,
            detach_dir,
            started_path,
            parent_uid,
            parent_gid,
        };
        if cli.started_path.is_none() {
            let mode = if internal_root_exec {
                "executor"
            } else {
                "worker"
            };
            return ParseResult::Error(format!(
                "--started-path is required for internal root {mode}"
            ));
        }
        if internal_root_exec {
            return ParseResult::Cli(SudoRunnerCli::InternalRootExec(cli));
        }
        return ParseResult::Cli(SudoRunnerCli::InternalRootWorker(cli));
    }
    if started_path.is_some() || parent_uid.is_some() || parent_gid.is_some() {
        return ParseResult::Error(
            "internal executor arguments require an internal executor mode"
                .to_string(),
        );
    }
    if let Some(detach_dir) = detach_dir {
        return ParseResult::Cli(SudoRunnerCli::Detach(SudoRunnerDetachCli {
            manifest_path,
            expected_sha256,
            detach_dir,
        }));
    }
    ParseResult::Cli(SudoRunnerCli::Execute(SudoRunnerManifestCli {
        manifest_path,
        expected_sha256,
    }))
}

pub(crate) fn sudo_runner_help() -> &'static str {
    "Usage:\n  sase_sudo_runner --capabilities|-c\n  sase_sudo_runner --manifest|-m PATH --expected-sha256|-e SHA256 [--detach-dir|-d DIR]\n\nReads one reviewed sudo manifest, verifies its canonical SHA-256, and emits either one JSON ledger on stdout or, with --detach-dir, one sudo_exec_started handshake after authentication succeeds.\n\nOptions:\n  -c, --capabilities          Print the schema-version-1 capabilities document and exit\n  -d, --detach-dir DIR       Authenticate, spawn the root executor, and print a started handshake\n  -e, --expected-sha256 SHA   Require this lowercase canonical manifest SHA-256 digest\n  -h, --help                  Show this help text\n  -m, --manifest PATH         Read the reviewed sudo manifest JSON from PATH\n\nExit statuses:\n  0  completed, command-level failure recorded, capabilities printed, or detached executor started\n  10 authentication failed\n  11 cancelled\n  12 no controlling TTY\n  13 invalid manifest, digest, handoff directory, or arguments\n  14 runner failure\n\nPlatform support:\n  Detached execution (--detach-dir and the detached_execution capability) requires Linux procfs and is unavailable on other platforms, where --capabilities reports an empty capability list and detach requests fail with an explicit unsupported-platform error. Synchronous manifest execution works everywhere."
}

pub(crate) fn validate_expected_sha256(
    value: &str,
) -> Result<(), SudoWireError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(SudoWireError {
            code: SudoErrorCodeWire::Validation,
            message: "expected SHA-256 must be lowercase hex".to_string(),
            target: Some("expected_sha256".to_string()),
        })
    }
}
