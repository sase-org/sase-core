use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::Once;
use std::time::Duration;

use super::*;

pub const SUDO_RUNNER_SUCCESS_EXIT: i32 = 0;
pub const SUDO_RUNNER_AUTH_FAILED_EXIT: i32 = 10;
pub const SUDO_RUNNER_CANCELLED_EXIT: i32 = 11;
pub const SUDO_RUNNER_TTY_UNAVAILABLE_EXIT: i32 = 12;
pub const SUDO_RUNNER_INVALID_INPUT_EXIT: i32 = 13;
pub const SUDO_RUNNER_RUNNER_ERROR_EXIT: i32 = 14;

pub(crate) const PRODUCTION_SUDO: &str = "/usr/bin/sudo";
/// Isolated-module prefix used when the PyO3 console runner relaunches
/// through the active interpreter. Not a public CLI option.
pub const PYTHON_HOSTED_SUDO_RUNNER_PREFIX: &[&str] =
    &["-I", "-m", "sase_core_rs.sudo_runner"];
pub(crate) const INTERNAL_ROOT_EXEC_FLAG: &str = "--internal-root-exec";
pub(crate) const INTERNAL_ROOT_WORKER_FLAG: &str = "--internal-root-worker";
pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(25);
pub(crate) const KILL_GRACE: Duration = Duration::from_millis(250);
pub(crate) const DRAIN_GRACE: Duration = Duration::from_millis(250);
pub(crate) const DRAIN_JOIN_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) const STARTED_SENTINEL_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const OUTPUT_DRAIN_CHUNK: usize = 8192;
pub(crate) const LEDGER_TAIL_POLICY_BYTES: usize = 8192;
pub(crate) const SUDO_RUNNER_CAPABILITY_DETACHED_EXECUTION: &str =
    "detached_execution";
pub(crate) const LEDGER_FILENAME: &str = "ledger.json";
pub(crate) const LOG_FILENAME: &str = "output.log";
pub(crate) const STARTED_FILENAME: &str = "started.json";
pub(crate) const STOP_FILENAME: &str = "stop";

pub(crate) static CANCELLED: AtomicBool = AtomicBool::new(false);
pub(crate) static SIGNALS_INSTALLED: Once = Once::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SudoRunnerExitStatus {
    Success,
    AuthFailed,
    Cancelled,
    TtyUnavailable,
    InvalidInput,
    RunnerError,
}

impl SudoRunnerExitStatus {
    pub const fn code(self) -> i32 {
        match self {
            Self::Success => SUDO_RUNNER_SUCCESS_EXIT,
            Self::AuthFailed => SUDO_RUNNER_AUTH_FAILED_EXIT,
            Self::Cancelled => SUDO_RUNNER_CANCELLED_EXIT,
            Self::TtyUnavailable => SUDO_RUNNER_TTY_UNAVAILABLE_EXIT,
            Self::InvalidInput => SUDO_RUNNER_INVALID_INPUT_EXIT,
            Self::RunnerError => SUDO_RUNNER_RUNNER_ERROR_EXIT,
        }
    }
}

#[derive(Debug)]
pub struct SudoRunnerCliError {
    pub(crate) status: SudoRunnerExitStatus,
    pub(crate) message: String,
}

impl SudoRunnerCliError {
    pub fn exit_code(&self) -> i32 {
        self.status.code()
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for SudoRunnerCliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for SudoRunnerCliError {}

/// Executable plus fixed prefix used to relaunch the runner across privilege
/// transitions. Tests inject a full launcher rather than a bare path so a
/// Python-hosted prefix cannot silently fall back to `current_exe()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SudoRunnerLauncher {
    pub(crate) program: PathBuf,
    pub(crate) prefix: Vec<String>,
}

impl SudoRunnerLauncher {
    pub(crate) fn native() -> Result<Self, SudoRunnerCliError> {
        Ok(Self {
            program: std::env::current_exe().map_err(|error| {
                cli_error(
                    SudoRunnerExitStatus::RunnerError,
                    format!(
                        "failed to resolve sudo runner executable: {error}"
                    ),
                )
            })?,
            prefix: Vec::new(),
        })
    }

    pub(crate) fn python_hosted(
        python_executable: &Path,
    ) -> Result<Self, SudoRunnerCliError> {
        if python_executable.as_os_str().is_empty()
            || !python_executable.is_absolute()
        {
            return Err(cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "Python sudo runner host executable must be an absolute path: {}",
                    python_executable.display()
                ),
            ));
        }
        Ok(Self {
            program: python_executable.to_path_buf(),
            prefix: PYTHON_HOSTED_SUDO_RUNNER_PREFIX
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        })
    }

    pub(crate) fn append_invocation(&self, command: &mut Command, mode: &str) {
        command.arg(&self.program);
        command.args(&self.prefix);
        command.arg(mode);
    }

    pub(crate) fn command_for_internal_mode(&self, mode: &str) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.prefix);
        command.arg(mode);
        command
    }
}

pub(crate) struct SudoRunnerConfig {
    pub(crate) sudo_path: PathBuf,
    pub(crate) launcher: Option<SudoRunnerLauncher>,
    pub(crate) tty_available: Option<bool>,
    pub(crate) harden_process: bool,
    pub(crate) started_sentinel_timeout: Duration,
    pub(crate) detached_execution: Option<bool>,
    pub(crate) process_identity_error: Option<String>,
    pub(crate) process_identity_override: Option<String>,
    pub(crate) started_publish_error: Option<String>,
}

impl SudoRunnerConfig {
    pub(crate) fn production() -> Self {
        Self {
            sudo_path: PathBuf::from(PRODUCTION_SUDO),
            launcher: None,
            tty_available: None,
            harden_process: true,
            started_sentinel_timeout: STARTED_SENTINEL_TIMEOUT,
            detached_execution: None,
            process_identity_error: None,
            process_identity_override: None,
            started_publish_error: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test(sudo_path: PathBuf) -> Self {
        Self {
            sudo_path,
            launcher: None,
            tty_available: Some(true),
            harden_process: false,
            started_sentinel_timeout: STARTED_SENTINEL_TIMEOUT,
            detached_execution: None,
            process_identity_error: None,
            process_identity_override: None,
            started_publish_error: None,
        }
    }
}
