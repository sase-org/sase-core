use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sase_core::{
    sudo_exec_started_from_json_value, sudo_manifest_from_json_slice,
    sudo_manifest_sha256, truncate_sudo_output_tail,
    validate_sudo_exec_started, validate_sudo_ledger, SudoErrorCodeWire,
    SudoExecStartedWire, SudoLedgerEntryStatusWire, SudoLedgerEntryWire,
    SudoLedgerOutcomeWire, SudoLedgerWire, SudoManifestWire,
    SudoOutputPolicyWire, SudoWireError, SUDO_EXEC_STARTED_KIND,
    SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION, SUDO_LEDGER_WIRE_SCHEMA_VERSION,
    SUDO_MANIFEST_MAX_BYTES, SUDO_MAX_COMMANDS, SUDO_MAX_OUTPUT_TAIL_BYTES,
};

pub const SUDO_RUNNER_SUCCESS_EXIT: i32 = 0;
pub const SUDO_RUNNER_AUTH_FAILED_EXIT: i32 = 10;
pub const SUDO_RUNNER_CANCELLED_EXIT: i32 = 11;
pub const SUDO_RUNNER_TTY_UNAVAILABLE_EXIT: i32 = 12;
pub const SUDO_RUNNER_INVALID_INPUT_EXIT: i32 = 13;
pub const SUDO_RUNNER_RUNNER_ERROR_EXIT: i32 = 14;

const PRODUCTION_SUDO: &str = "/usr/bin/sudo";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const KILL_GRACE: Duration = Duration::from_millis(250);
const STARTED_SENTINEL_TIMEOUT: Duration = Duration::from_secs(5);
const SUDO_RUNNER_CAPABILITY_DETACHED_EXECUTION: &str = "detached_execution";
const LEDGER_FILENAME: &str = "ledger.json";
const LOG_FILENAME: &str = "output.log";
const STARTED_FILENAME: &str = "started.json";
const STOP_FILENAME: &str = "stop";

static CANCELLED: AtomicBool = AtomicBool::new(false);
static SIGNALS_INSTALLED: Once = Once::new();

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
    status: SudoRunnerExitStatus,
    message: String,
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

pub fn run_sudo_runner_cli(
    args: impl IntoIterator<Item = String>,
) -> Result<(), SudoRunnerCliError> {
    let config = SudoRunnerConfig::production();
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    run_sudo_runner_cli_with_io(args, &config, &mut stdout, &mut stderr)
}

struct SudoRunnerConfig {
    sudo_path: PathBuf,
    runner_path: Option<PathBuf>,
    tty_available: Option<bool>,
    harden_process: bool,
    started_sentinel_timeout: Duration,
}

impl SudoRunnerConfig {
    fn production() -> Self {
        Self {
            sudo_path: PathBuf::from(PRODUCTION_SUDO),
            runner_path: None,
            tty_available: None,
            harden_process: true,
            started_sentinel_timeout: STARTED_SENTINEL_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn test(sudo_path: PathBuf) -> Self {
        Self {
            sudo_path,
            runner_path: None,
            tty_available: Some(true),
            harden_process: false,
            started_sentinel_timeout: STARTED_SENTINEL_TIMEOUT,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SudoRunnerCli {
    Capabilities,
    Execute(SudoRunnerManifestCli),
    Detach(SudoRunnerDetachCli),
    InternalRootExec(SudoRunnerInternalCli),
    InternalRootWorker(SudoRunnerInternalCli),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SudoRunnerManifestCli {
    manifest_path: PathBuf,
    expected_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SudoRunnerDetachCli {
    manifest_path: PathBuf,
    expected_sha256: String,
    detach_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SudoRunnerInternalCli {
    manifest_path: PathBuf,
    expected_sha256: String,
    detach_dir: PathBuf,
    started_path: Option<PathBuf>,
    parent_uid: u32,
    parent_gid: u32,
}

fn run_sudo_runner_cli_with_io(
    args: impl IntoIterator<Item = String>,
    config: &SudoRunnerConfig,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), SudoRunnerCliError> {
    CANCELLED.store(false, Ordering::SeqCst);
    let cli = match parse_sudo_runner_args(args) {
        ParseResult::Help => {
            writeln!(stdout, "{}", sudo_runner_help()).map_err(internal_io)?;
            return Ok(());
        }
        ParseResult::Cli(cli) => cli,
        ParseResult::Error(message) => {
            return Err(cli_error(SudoRunnerExitStatus::InvalidInput, message));
        }
    };
    match cli {
        SudoRunnerCli::Capabilities => write_capabilities(stdout),
        SudoRunnerCli::Execute(cli) => {
            run_synchronous_manifest(cli, config, stdout, stderr)
        }
        SudoRunnerCli::Detach(cli) => {
            run_detached_manifest(cli, config, stdout, stderr)
        }
        SudoRunnerCli::InternalRootExec(cli) => {
            run_internal_root_exec(cli, config)
        }
        SudoRunnerCli::InternalRootWorker(cli) => {
            run_internal_root_worker(cli, config)
        }
    }
}

fn run_synchronous_manifest(
    cli: SudoRunnerManifestCli,
    config: &SudoRunnerConfig,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), SudoRunnerCliError> {
    if let Err(error) = validate_expected_sha256(&cli.expected_sha256) {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            error.message,
        ));
    }
    let manifest_bytes =
        read_bounded_manifest(&cli.manifest_path).map_err(|message| {
            cli_error(SudoRunnerExitStatus::InvalidInput, message)
        })?;
    let manifest =
        sudo_manifest_from_json_slice(&manifest_bytes).map_err(|error| {
            cli_error(SudoRunnerExitStatus::InvalidInput, error.message)
        })?;
    let actual_sha256 = sudo_manifest_sha256(&manifest).map_err(|error| {
        cli_error(SudoRunnerExitStatus::InvalidInput, error.message)
    })?;
    if !constant_time_eq(
        cli.expected_sha256.as_bytes(),
        actual_sha256.as_bytes(),
    ) {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            SudoWireError::digest_mismatch(
                &cli.expected_sha256,
                &actual_sha256,
            )
            .message,
        ));
    }

    let status = execute_manifest(&manifest, &actual_sha256, config, stderr)
        .and_then(|ledger| {
            write_ledger(stdout, &ledger, &manifest)?;
            Ok(exit_status_for_ledger(&ledger))
        })?;
    if status == SudoRunnerExitStatus::Success {
        Ok(())
    } else {
        Err(cli_error(
            status,
            format!("sase_sudo_runner finished with {status:?}"),
        ))
    }
}

fn write_capabilities(
    stdout: &mut dyn Write,
) -> Result<(), SudoRunnerCliError> {
    let capabilities = serde_json::json!({
        "schema_version": 1,
        "capabilities": [SUDO_RUNNER_CAPABILITY_DETACHED_EXECUTION],
    });
    serde_json::to_writer(&mut *stdout, &capabilities).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to serialize sudo runner capabilities: {error}"),
        )
    })?;
    writeln!(stdout).map_err(internal_io)
}

enum ParseResult {
    Help,
    Cli(SudoRunnerCli),
    Error(String),
}

fn parse_sudo_runner_args(
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
            "--internal-root-exec" => {
                internal_root_exec = true;
            }
            "--internal-root-worker" => {
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
        if internal_root_exec {
            if cli.started_path.is_none() {
                return ParseResult::Error(
                    "--started-path is required for internal root executor"
                        .to_string(),
                );
            }
            return ParseResult::Cli(SudoRunnerCli::InternalRootExec(cli));
        }
        if cli.started_path.is_some() {
            return ParseResult::Error(
                "--started-path is only valid for internal root executor"
                    .to_string(),
            );
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

fn sudo_runner_help() -> &'static str {
    "Usage:\n  sase_sudo_runner --capabilities|-c\n  sase_sudo_runner --manifest|-m PATH --expected-sha256|-e SHA256 [--detach-dir|-d DIR]\n\nReads one reviewed sudo manifest, verifies its canonical SHA-256, and emits either one JSON ledger on stdout or, with --detach-dir, one sudo_exec_started handshake after authentication succeeds.\n\nOptions:\n  -c, --capabilities          Print the schema-version-1 capabilities document and exit\n  -d, --detach-dir DIR       Authenticate, spawn the root executor, and print a started handshake\n  -e, --expected-sha256 SHA   Require this lowercase canonical manifest SHA-256 digest\n  -h, --help                  Show this help text\n  -m, --manifest PATH         Read the reviewed sudo manifest JSON from PATH\n\nExit statuses:\n  0  completed, command-level failure recorded, capabilities printed, or detached executor started\n  10 authentication failed\n  11 cancelled\n  12 no controlling TTY\n  13 invalid manifest, digest, handoff directory, or arguments\n  14 runner failure"
}

fn validate_expected_sha256(value: &str) -> Result<(), SudoWireError> {
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

fn read_bounded_manifest(path: &Path) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("failed to stat manifest: {error}"))?;
    if metadata.len() > SUDO_MANIFEST_MAX_BYTES as u64 {
        return Err(format!(
            "sudo manifest exceeds {SUDO_MANIFEST_MAX_BYTES} bytes"
        ));
    }
    fs::read(path).map_err(|error| format!("failed to read manifest: {error}"))
}

#[derive(Debug, Clone)]
struct HandoffPaths {
    dir: PathBuf,
    manifest_path: PathBuf,
    ledger_path: PathBuf,
    log_path: PathBuf,
    started_path: PathBuf,
    stop_path: PathBuf,
}

fn validate_handoff_paths(
    detach_dir: &Path,
    manifest_path: &Path,
) -> Result<HandoffPaths, SudoRunnerCliError> {
    if !detach_dir.is_absolute() {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            "--detach-dir must be an absolute path".to_string(),
        ));
    }
    if !manifest_path.is_absolute() {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            "--manifest must be absolute in detach mode".to_string(),
        ));
    }
    let dir_link = fs::symlink_metadata(detach_dir).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::InvalidInput,
            format!("failed to inspect detach dir: {error}"),
        )
    })?;
    if dir_link.file_type().is_symlink() || !dir_link.is_dir() {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            "--detach-dir must name a real directory".to_string(),
        ));
    }
    let dir = detach_dir.canonicalize().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::InvalidInput,
            format!("failed to canonicalize detach dir: {error}"),
        )
    })?;
    let manifest_link =
        fs::symlink_metadata(manifest_path).map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::InvalidInput,
                format!("failed to inspect detach manifest: {error}"),
            )
        })?;
    if manifest_link.file_type().is_symlink() || !manifest_link.is_file() {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            "detach manifest must name a real file".to_string(),
        ));
    }
    let manifest_path = manifest_path.canonicalize().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::InvalidInput,
            format!("failed to canonicalize detach manifest: {error}"),
        )
    })?;
    if manifest_path.parent() != Some(dir.as_path()) {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            "detach manifest must live directly inside --detach-dir"
                .to_string(),
        ));
    }
    Ok(HandoffPaths {
        ledger_path: dir.join(LEDGER_FILENAME),
        log_path: dir.join(LOG_FILENAME),
        started_path: dir.join(STARTED_FILENAME),
        stop_path: dir.join(STOP_FILENAME),
        dir,
        manifest_path,
    })
}

fn read_bounded_manifest_nofollow(path: &Path) -> Result<Vec<u8>, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| format!("failed to open manifest: {error}"))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("failed to stat manifest: {error}"))?;
        if !metadata.is_file() {
            return Err("sudo manifest must be a regular file".to_string());
        }
        if metadata.len() > SUDO_MANIFEST_MAX_BYTES as u64 {
            return Err(format!(
                "sudo manifest exceeds {SUDO_MANIFEST_MAX_BYTES} bytes"
            ));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| format!("failed to read manifest: {error}"))?;
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        read_bounded_manifest(path)
    }
}

fn load_verified_detach_manifest(
    paths: &HandoffPaths,
    expected_sha256: &str,
) -> Result<(SudoManifestWire, String), SudoRunnerCliError> {
    if let Err(error) = validate_expected_sha256(expected_sha256) {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            error.message,
        ));
    }
    let manifest_bytes = read_bounded_manifest_nofollow(&paths.manifest_path)
        .map_err(|message| {
        cli_error(SudoRunnerExitStatus::InvalidInput, message)
    })?;
    let manifest =
        sudo_manifest_from_json_slice(&manifest_bytes).map_err(|error| {
            cli_error(SudoRunnerExitStatus::InvalidInput, error.message)
        })?;
    let actual_sha256 = sudo_manifest_sha256(&manifest).map_err(|error| {
        cli_error(SudoRunnerExitStatus::InvalidInput, error.message)
    })?;
    if !constant_time_eq(expected_sha256.as_bytes(), actual_sha256.as_bytes()) {
        return Err(cli_error(
            SudoRunnerExitStatus::InvalidInput,
            SudoWireError::digest_mismatch(expected_sha256, &actual_sha256)
                .message,
        ));
    }
    Ok((manifest, actual_sha256))
}

fn finish_with_ledger(
    stdout: &mut dyn Write,
    ledger: &SudoLedgerWire,
    manifest: &SudoManifestWire,
) -> Result<(), SudoRunnerCliError> {
    write_ledger(stdout, ledger, manifest)?;
    let status = exit_status_for_ledger(ledger);
    if status == SudoRunnerExitStatus::Success {
        Ok(())
    } else {
        Err(cli_error(
            status,
            format!("sase_sudo_runner finished with {status:?}"),
        ))
    }
}

fn run_detached_manifest(
    cli: SudoRunnerDetachCli,
    config: &SudoRunnerConfig,
    stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> Result<(), SudoRunnerCliError> {
    let paths = validate_handoff_paths(&cli.detach_dir, &cli.manifest_path)?;
    let (manifest, manifest_sha256) =
        load_verified_detach_manifest(&paths, &cli.expected_sha256)?;

    #[cfg(not(unix))]
    {
        let ledger = skipped_ledger(
            &manifest,
            &manifest_sha256,
            SudoLedgerOutcomeWire::RunnerError,
            Some(
                "detached sudo execution is unsupported on this platform"
                    .to_string(),
            ),
        );
        return finish_with_ledger(stdout, &ledger, &manifest);
    }

    #[cfg(unix)]
    {
        if !tty_available(config) {
            let ledger = skipped_ledger(
                &manifest,
                &manifest_sha256,
                SudoLedgerOutcomeWire::TtyUnavailable,
                None,
            );
            return finish_with_ledger(stdout, &ledger, &manifest);
        }
        install_cancellation_handlers();
        if config.harden_process {
            harden_process().map_err(|message| {
                cli_error(SudoRunnerExitStatus::RunnerError, message)
            })?;
        }

        if !sudo_status(config, &manifest, ["-k"], StdioMode::Inherit)?
            .success()
        {
            let ledger = skipped_ledger(
                &manifest,
                &manifest_sha256,
                SudoLedgerOutcomeWire::RunnerError,
                Some("sudo timestamp invalidation failed".to_string()),
            );
            return finish_with_ledger(stdout, &ledger, &manifest);
        }

        if !sudo_status(config, &manifest, ["-v"], StdioMode::Inherit)?
            .success()
        {
            let _ = sudo_status(config, &manifest, ["-k"], StdioMode::Inherit);
            let ledger = skipped_ledger(
                &manifest,
                &manifest_sha256,
                SudoLedgerOutcomeWire::AuthFailed,
                None,
            );
            return finish_with_ledger(stdout, &ledger, &manifest);
        }

        if let Err(error) = spawn_internal_root_executor(
            config,
            &manifest,
            &manifest_sha256,
            &paths,
        ) {
            let _ = sudo_status(config, &manifest, ["-k"], StdioMode::Inherit);
            let ledger = skipped_ledger(
                &manifest,
                &manifest_sha256,
                SudoLedgerOutcomeWire::RunnerError,
                Some(error.message),
            );
            return finish_with_ledger(stdout, &ledger, &manifest);
        }

        let handshake = match wait_for_started_handshake(
            &paths,
            &manifest,
            config.started_sentinel_timeout,
        ) {
            Ok(handshake) => handshake,
            Err(error) => {
                let _ =
                    sudo_status(config, &manifest, ["-k"], StdioMode::Inherit);
                let ledger = skipped_ledger(
                    &manifest,
                    &manifest_sha256,
                    SudoLedgerOutcomeWire::RunnerError,
                    Some(error.message),
                );
                return finish_with_ledger(stdout, &ledger, &manifest);
            }
        };

        if !sudo_status(config, &manifest, ["-k"], StdioMode::Inherit)?
            .success()
        {
            let ledger = skipped_ledger(
                &manifest,
                &manifest_sha256,
                SudoLedgerOutcomeWire::RunnerError,
                Some("sudo timestamp cleanup failed".to_string()),
            );
            return finish_with_ledger(stdout, &ledger, &manifest);
        }

        serde_json::to_writer(&mut *stdout, &handshake).map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to serialize sudo exec started handshake: {error}"
                ),
            )
        })?;
        writeln!(stdout).map_err(internal_io)
    }
}

fn run_internal_root_exec(
    cli: SudoRunnerInternalCli,
    config: &SudoRunnerConfig,
) -> Result<(), SudoRunnerCliError> {
    #[cfg(not(unix))]
    {
        let _ = (cli, config);
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "detached sudo execution is unsupported on this platform"
                .to_string(),
        ));
    }

    #[cfg(unix)]
    {
        let paths =
            validate_handoff_paths(&cli.detach_dir, &cli.manifest_path)?;
        let Some(started_path) = &cli.started_path else {
            return Err(cli_error(
                SudoRunnerExitStatus::InvalidInput,
                "--started-path is required for internal root executor"
                    .to_string(),
            ));
        };
        if started_path != &paths.started_path {
            return Err(cli_error(
                SudoRunnerExitStatus::InvalidInput,
                "internal started path must match the handoff directory"
                    .to_string(),
            ));
        }
        let (manifest, manifest_sha256) =
            load_verified_detach_manifest(&paths, &cli.expected_sha256)?;
        reject_existing_path(&paths.started_path, "started handshake")?;
        let runner_path = runner_executable_path(config)?;
        let mut command = Command::new(&runner_path);
        command
            .arg("--internal-root-worker")
            .arg("--manifest")
            .arg(&paths.manifest_path)
            .arg("--expected-sha256")
            .arg(&manifest_sha256)
            .arg("--detach-dir")
            .arg(&paths.dir)
            .arg("--parent-uid")
            .arg(cli.parent_uid.to_string())
            .arg("--parent-gid")
            .arg(cli.parent_gid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_new_session(&mut command);
        let child = command.spawn().map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to spawn sudo root worker: {error}"),
            )
        })?;
        let executor_pid = child.id();
        let executor_identity = process_identity_token(executor_pid)?;
        let handshake = SudoExecStartedWire {
            schema_version: SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION,
            kind: SUDO_EXEC_STARTED_KIND.to_string(),
            manifest_sha256,
            executor_pid,
            executor_identity,
            ledger_path: path_string(&paths.ledger_path)?,
            log_path: path_string(&paths.log_path)?,
            started_at: current_unix_time()?,
        };
        validate_sudo_exec_started(&handshake, Some(&manifest)).map_err(
            |error| cli_error(SudoRunnerExitStatus::RunnerError, error.message),
        )?;
        write_atomic_json_for_user(
            &paths.started_path,
            &handshake,
            cli.parent_uid,
            cli.parent_gid,
        )
    }
}

fn run_internal_root_worker(
    cli: SudoRunnerInternalCli,
    _config: &SudoRunnerConfig,
) -> Result<(), SudoRunnerCliError> {
    #[cfg(not(unix))]
    {
        let _ = cli;
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "detached sudo execution is unsupported on this platform"
                .to_string(),
        ));
    }

    #[cfg(unix)]
    {
        let paths =
            validate_handoff_paths(&cli.detach_dir, &cli.manifest_path)?;
        let (manifest, manifest_sha256) =
            load_verified_detach_manifest(&paths, &cli.expected_sha256)?;
        match run_internal_root_worker_loaded(
            &manifest,
            &manifest_sha256,
            &paths,
            cli.parent_uid,
            cli.parent_gid,
        ) {
            Ok(()) => Ok(()),
            Err(error) => {
                let ledger = skipped_ledger(
                    &manifest,
                    &manifest_sha256,
                    SudoLedgerOutcomeWire::RunnerError,
                    Some(error.message.clone()),
                );
                write_ledger_file(
                    &paths.ledger_path,
                    &ledger,
                    &manifest,
                    cli.parent_uid,
                    cli.parent_gid,
                )?;
                Err(error)
            }
        }
    }
}

fn execute_manifest(
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    config: &SudoRunnerConfig,
    stderr: &mut dyn Write,
) -> Result<SudoLedgerWire, SudoRunnerCliError> {
    if !tty_available(config) {
        return Ok(skipped_ledger(
            manifest,
            manifest_sha256,
            SudoLedgerOutcomeWire::TtyUnavailable,
            None,
        ));
    }

    #[cfg(not(unix))]
    {
        return Ok(skipped_ledger(
            manifest,
            manifest_sha256,
            SudoLedgerOutcomeWire::RunnerError,
            Some(
                "sudo runner execution is unsupported on this platform"
                    .to_string(),
            ),
        ));
    }

    #[cfg(unix)]
    {
        install_cancellation_handlers();
        if config.harden_process {
            harden_process().map_err(|message| {
                cli_error(SudoRunnerExitStatus::RunnerError, message)
            })?;
        }

        if !sudo_status(config, manifest, ["-k"], StdioMode::Inherit)?.success()
        {
            return Ok(skipped_ledger(
                manifest,
                manifest_sha256,
                SudoLedgerOutcomeWire::RunnerError,
                Some("sudo timestamp invalidation failed".to_string()),
            ));
        }

        if !sudo_status(config, manifest, ["-v"], StdioMode::Inherit)?.success()
        {
            let _ = sudo_status(config, manifest, ["-k"], StdioMode::Inherit);
            return Ok(skipped_ledger(
                manifest,
                manifest_sha256,
                SudoLedgerOutcomeWire::AuthFailed,
                None,
            ));
        }

        let mut ledger = skipped_ledger(
            manifest,
            manifest_sha256,
            SudoLedgerOutcomeWire::Completed,
            None,
        );
        let start_index = manifest
            .resume_from
            .as_deref()
            .and_then(|id| {
                manifest
                    .commands
                    .iter()
                    .position(|command| command.id == id)
            })
            .unwrap_or(0);
        for index in start_index..manifest.commands.len() {
            if CANCELLED.load(Ordering::SeqCst) {
                ledger.outcome = SudoLedgerOutcomeWire::Cancelled;
                break;
            }
            let command = &manifest.commands[index];
            writeln!(stderr, "sase_sudo_runner: running {}", command.id)
                .map_err(internal_io)?;
            let cached = match sudo_status(
                config,
                manifest,
                ["-n", "-v"],
                StdioMode::Null,
            ) {
                Ok(status) => status.success(),
                Err(error) => {
                    ledger.outcome = SudoLedgerOutcomeWire::RunnerError;
                    ledger.diagnostic = Some(error.message);
                    break;
                }
            };
            let result = match run_approved_command(
                config, manifest, index, cached, stderr,
            ) {
                Ok(result) => result,
                Err(error) => {
                    ledger.outcome = SudoLedgerOutcomeWire::RunnerError;
                    ledger.diagnostic = Some(error.message);
                    break;
                }
            };
            ledger.entries[index] = result.entry;
            match result.outcome {
                CommandOutcome::Ok => {}
                CommandOutcome::Failed | CommandOutcome::TimedOut => {
                    if manifest.stop_on_failure {
                        break;
                    }
                }
                CommandOutcome::Cancelled => {
                    ledger.outcome = SudoLedgerOutcomeWire::Cancelled;
                    break;
                }
            }
        }

        let cleanup = sudo_status(config, manifest, ["-k"], StdioMode::Inherit);
        if matches!(ledger.outcome, SudoLedgerOutcomeWire::Completed) {
            match cleanup {
                Ok(status) if status.success() => {}
                Ok(_) => {
                    ledger.outcome = SudoLedgerOutcomeWire::RunnerError;
                    ledger.diagnostic =
                        Some("sudo timestamp cleanup failed".to_string());
                }
                Err(error) => {
                    ledger.outcome = SudoLedgerOutcomeWire::RunnerError;
                    ledger.diagnostic = Some(error.message);
                }
            }
        }

        Ok(ledger)
    }
}

fn skipped_ledger(
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    outcome: SudoLedgerOutcomeWire,
    diagnostic: Option<String>,
) -> SudoLedgerWire {
    let entries = manifest
        .commands
        .iter()
        .take(SUDO_MAX_COMMANDS)
        .map(|command| SudoLedgerEntryWire {
            id: command.id.clone(),
            status: SudoLedgerEntryStatusWire::Skipped,
            exit_code: None,
            duration_seconds: 0.0,
            output_tail: String::new(),
        })
        .collect();
    SudoLedgerWire {
        schema_version: SUDO_LEDGER_WIRE_SCHEMA_VERSION,
        request_id: manifest.request_id.clone(),
        manifest_sha256: manifest_sha256.to_string(),
        outcome,
        entries,
        diagnostic,
    }
}

fn write_ledger(
    stdout: &mut dyn Write,
    ledger: &SudoLedgerWire,
    manifest: &SudoManifestWire,
) -> Result<(), SudoRunnerCliError> {
    validate_sudo_ledger(ledger, Some(manifest)).map_err(|error| {
        cli_error(SudoRunnerExitStatus::RunnerError, error.message)
    })?;
    serde_json::to_writer(&mut *stdout, ledger).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to serialize sudo ledger: {error}"),
        )
    })?;
    writeln!(stdout).map_err(internal_io)
}

fn exit_status_for_ledger(ledger: &SudoLedgerWire) -> SudoRunnerExitStatus {
    match ledger.outcome {
        SudoLedgerOutcomeWire::Completed => SudoRunnerExitStatus::Success,
        SudoLedgerOutcomeWire::AuthFailed => SudoRunnerExitStatus::AuthFailed,
        SudoLedgerOutcomeWire::Cancelled => SudoRunnerExitStatus::Cancelled,
        SudoLedgerOutcomeWire::TtyUnavailable => {
            SudoRunnerExitStatus::TtyUnavailable
        }
        SudoLedgerOutcomeWire::RunnerError => SudoRunnerExitStatus::RunnerError,
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StdioMode {
    Inherit,
    Null,
}

#[cfg(unix)]
fn sudo_status<const N: usize>(
    config: &SudoRunnerConfig,
    manifest: &SudoManifestWire,
    args: [&str; N],
    mode: StdioMode,
) -> Result<ExitStatus, SudoRunnerCliError> {
    let mut command = sudo_command(config, manifest);
    command.args(args);
    match mode {
        StdioMode::Inherit => {
            command
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());
        }
        StdioMode::Null => {
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
        }
    }
    command.status().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to invoke sudo: {error}"),
        )
    })
}

#[cfg(unix)]
fn sudo_command(
    config: &SudoRunnerConfig,
    manifest: &SudoManifestWire,
) -> Command {
    let mut command = Command::new(&config.sudo_path);
    command.env_clear();
    command.env("LC_ALL", "C");
    command.env("LANG", "C");
    command.env(
        "TERM",
        std::env::var("TERM")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "dumb".to_string()),
    );
    for (name, value) in &manifest.env {
        command.env(name, value);
    }
    command
}

#[cfg(unix)]
fn spawn_internal_root_executor(
    config: &SudoRunnerConfig,
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
) -> Result<(), SudoRunnerCliError> {
    reject_existing_path(&paths.started_path, "started handshake")?;
    let runner_path = runner_executable_path(config)?;
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mut command = sudo_command(config, manifest);
    command
        .arg("-n")
        .arg("-u")
        .arg("root")
        .arg("--")
        .arg(runner_path)
        .arg("--internal-root-exec")
        .arg("--manifest")
        .arg(&paths.manifest_path)
        .arg("--expected-sha256")
        .arg(manifest_sha256)
        .arg("--detach-dir")
        .arg(&paths.dir)
        .arg("--started-path")
        .arg(&paths.started_path)
        .arg("--parent-uid")
        .arg(uid.to_string())
        .arg("--parent-gid")
        .arg(gid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = command.status().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to invoke sudo root executor: {error}"),
        )
    })?;
    if status.success() {
        Ok(())
    } else {
        Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("sudo root executor spawn failed with status {status}"),
        ))
    }
}

#[cfg(unix)]
fn wait_for_started_handshake(
    paths: &HandoffPaths,
    manifest: &SudoManifestWire,
    timeout: Duration,
) -> Result<SudoExecStartedWire, SudoRunnerCliError> {
    let deadline = Instant::now() + timeout;
    loop {
        match fs::symlink_metadata(&paths.started_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(cli_error(
                    SudoRunnerExitStatus::RunnerError,
                    "refusing symlink started handshake".to_string(),
                ));
            }
            Ok(metadata) if metadata.is_file() => {
                let bytes = read_small_file_nofollow(&paths.started_path)?;
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                    cli_error(
                        SudoRunnerExitStatus::RunnerError,
                        format!(
                            "sudo started handshake is not valid JSON: {error}"
                        ),
                    )
                })?;
                let handshake = sudo_exec_started_from_json_value(&value)
                    .map_err(|error| {
                        cli_error(
                            SudoRunnerExitStatus::RunnerError,
                            error.message,
                        )
                    })?;
                validate_sudo_exec_started(&handshake, Some(manifest))
                    .map_err(|error| {
                        cli_error(
                            SudoRunnerExitStatus::RunnerError,
                            error.message,
                        )
                    })?;
                validate_handshake_paths(&handshake, paths)?;
                return Ok(handshake);
            }
            Ok(_) => {
                return Err(cli_error(
                    SudoRunnerExitStatus::RunnerError,
                    "started handshake path is not a regular file".to_string(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(cli_error(
                    SudoRunnerExitStatus::RunnerError,
                    format!("failed to inspect started handshake: {error}"),
                ));
            }
        }
        if Instant::now() >= deadline {
            return Err(cli_error(
                SudoRunnerExitStatus::RunnerError,
                "timed out waiting for sudo root executor handshake"
                    .to_string(),
            ));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn validate_handshake_paths(
    handshake: &SudoExecStartedWire,
    paths: &HandoffPaths,
) -> Result<(), SudoRunnerCliError> {
    let ledger_path = path_string(&paths.ledger_path)?;
    let log_path = path_string(&paths.log_path)?;
    if handshake.ledger_path != ledger_path || handshake.log_path != log_path {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "sudo started handshake paths do not match handoff directory"
                .to_string(),
        ));
    }
    Ok(())
}

fn runner_executable_path(
    config: &SudoRunnerConfig,
) -> Result<PathBuf, SudoRunnerCliError> {
    if let Some(path) = &config.runner_path {
        return Ok(path.clone());
    }
    std::env::current_exe().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to resolve sudo runner executable: {error}"),
        )
    })
}

fn path_string(path: &Path) -> Result<String, SudoRunnerCliError> {
    path.to_str().map(str::to_string).ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })
}

fn current_unix_time() -> Result<f64, SudoRunnerCliError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("system clock is before Unix epoch: {error}"),
            )
        })
}

#[cfg(unix)]
fn process_identity_token(pid: u32) -> Result<String, SudoRunnerCliError> {
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to read Linux boot id: {error}"),
            )
        })?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "Linux boot id is empty".to_string(),
        ));
    }
    let stat =
        fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to read executor process stat: {error}"),
            )
        })?;
    let close = stat.rfind(')').ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            "executor process stat is malformed".to_string(),
        )
    })?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    let start_ticks = fields.get(19).ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            "executor process stat is missing start ticks".to_string(),
        )
    })?;
    start_ticks.parse::<u64>().map_err(|_| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            "executor process start ticks are malformed".to_string(),
        )
    })?;
    Ok(format!("{boot_id}:{start_ticks}"))
}

#[cfg(unix)]
fn read_small_file_nofollow(
    path: &Path,
) -> Result<Vec<u8>, SudoRunnerCliError> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to open {}: {error}", path.display()),
            )
        })?;
    let metadata = file.metadata().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to stat {}: {error}", path.display()),
        )
    })?;
    if metadata.len() > 16 * 1024 {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("{} exceeds 16384 bytes", path.display()),
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to read {}: {error}", path.display()),
        )
    })?;
    Ok(bytes)
}

#[cfg(unix)]
struct CommandResult {
    entry: SudoLedgerEntryWire,
    outcome: CommandOutcome,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandOutcome {
    Ok,
    Failed,
    TimedOut,
    Cancelled,
}

#[cfg(unix)]
struct ResolvedAccount {
    uid: libc::uid_t,
    gid: libc::gid_t,
    name: std::ffi::CString,
}

#[cfg(unix)]
fn run_internal_root_worker_loaded(
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
    parent_uid: u32,
    parent_gid: u32,
) -> Result<(), SudoRunnerCliError> {
    reject_existing_path(&paths.ledger_path, "ledger")?;
    reject_symlink_path(&paths.log_path, "log")?;
    let mut log = open_log_file(&paths.log_path, parent_uid, parent_gid)?;
    let ledger = execute_detached_worker_manifest(
        manifest,
        manifest_sha256,
        paths,
        &mut log,
    );
    let ledger = match ledger {
        Ok(ledger) => ledger,
        Err(error) => skipped_ledger(
            manifest,
            manifest_sha256,
            SudoLedgerOutcomeWire::RunnerError,
            Some(error.message),
        ),
    };
    write_ledger_file(
        &paths.ledger_path,
        &ledger,
        manifest,
        parent_uid,
        parent_gid,
    )?;
    Ok(())
}

#[cfg(unix)]
fn execute_detached_worker_manifest(
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
    log: &mut dyn Write,
) -> Result<SudoLedgerWire, SudoRunnerCliError> {
    install_cancellation_handlers();
    let account = resolve_account(&manifest.run_as).map_err(|message| {
        cli_error(SudoRunnerExitStatus::RunnerError, message)
    })?;
    let mut ledger = skipped_ledger(
        manifest,
        manifest_sha256,
        SudoLedgerOutcomeWire::Completed,
        None,
    );
    let start_index = manifest
        .resume_from
        .as_deref()
        .and_then(|id| {
            manifest
                .commands
                .iter()
                .position(|command| command.id == id)
        })
        .unwrap_or(0);
    for index in start_index..manifest.commands.len() {
        if CANCELLED.load(Ordering::SeqCst) || paths.stop_path.exists() {
            ledger.outcome = SudoLedgerOutcomeWire::Cancelled;
            break;
        }
        let command = &manifest.commands[index];
        writeln!(log, "sase_sudo_runner: running {}", command.id)
            .map_err(internal_io)?;
        let result = run_direct_command(
            manifest,
            index,
            &account,
            &paths.stop_path,
            log,
        )?;
        ledger.entries[index] = result.entry;
        match result.outcome {
            CommandOutcome::Ok => {}
            CommandOutcome::Failed | CommandOutcome::TimedOut => {
                if manifest.stop_on_failure {
                    break;
                }
            }
            CommandOutcome::Cancelled => {
                ledger.outcome = SudoLedgerOutcomeWire::Cancelled;
                break;
            }
        }
    }
    Ok(ledger)
}

#[cfg(unix)]
fn run_direct_command(
    manifest: &SudoManifestWire,
    index: usize,
    account: &ResolvedAccount,
    stop_path: &Path,
    log: &mut dyn Write,
) -> Result<CommandResult, SudoRunnerCliError> {
    let command_wire = &manifest.commands[index];
    let mut command = Command::new(&command_wire.argv[0]);
    command
        .args(&command_wire.argv[1..])
        .current_dir(&manifest.cwd)
        .env_clear()
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env(
            "TERM",
            std::env::var("TERM")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "dumb".to_string()),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in &manifest.env {
        command.env(name, value);
    }
    configure_process_group_and_account(&mut command, account);
    let timeout = command_wire
        .timeout_seconds
        .map(Duration::from_secs_f64)
        .unwrap_or(DEFAULT_TIMEOUT);
    let started = Instant::now();
    let mut child = command.spawn().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to start direct sudo command in cwd {}: {error}",
                manifest.cwd
            ),
        )
    })?;
    let stdout_reader = child.stdout.take().map(read_pipe_in_thread);
    let stderr_reader = child.stderr.take().map(read_pipe_in_thread);
    let wait = wait_child_bounded_with_cancel(&mut child, timeout, || {
        CANCELLED.load(Ordering::SeqCst) || stop_path.exists()
    })?;
    let duration_seconds = started.elapsed().as_secs_f64();
    let stdout_bytes = join_reader(stdout_reader)?;
    let stderr_bytes = join_reader(stderr_reader)?;
    if !stdout_bytes.is_empty() {
        log.write_all(&stdout_bytes).map_err(internal_io)?;
    }
    if !stderr_bytes.is_empty() {
        log.write_all(&stderr_bytes).map_err(internal_io)?;
    }
    let mut combined = Vec::new();
    combined.extend_from_slice(&stdout_bytes);
    combined.extend_from_slice(&stderr_bytes);
    let output_tail = ledger_output(&combined, manifest.output_to_agent);
    let (status, exit_code, outcome) = match wait {
        WaitOutcome::Exited(status) if status.success() => (
            SudoLedgerEntryStatusWire::Ran,
            status.code(),
            CommandOutcome::Ok,
        ),
        WaitOutcome::Exited(status) => (
            SudoLedgerEntryStatusWire::Failed,
            status.code(),
            CommandOutcome::Failed,
        ),
        WaitOutcome::TimedOut => (
            SudoLedgerEntryStatusWire::Failed,
            None,
            CommandOutcome::TimedOut,
        ),
        WaitOutcome::Cancelled => (
            SudoLedgerEntryStatusWire::Failed,
            None,
            CommandOutcome::Cancelled,
        ),
    };
    Ok(CommandResult {
        entry: SudoLedgerEntryWire {
            id: command_wire.id.clone(),
            status,
            exit_code,
            duration_seconds,
            output_tail,
        },
        outcome,
    })
}

#[cfg(unix)]
fn run_approved_command(
    config: &SudoRunnerConfig,
    manifest: &SudoManifestWire,
    index: usize,
    cached: bool,
    stderr: &mut dyn Write,
) -> Result<CommandResult, SudoRunnerCliError> {
    let command_wire = &manifest.commands[index];
    let mut command = sudo_command(config, manifest);
    if cached {
        command.arg("-n");
    }
    command
        .current_dir(&manifest.cwd)
        .arg("-u")
        .arg(&manifest.run_as)
        .arg("--")
        .args(&command_wire.argv)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_process_group(&mut command);
    let timeout = command_wire
        .timeout_seconds
        .map(Duration::from_secs_f64)
        .unwrap_or(DEFAULT_TIMEOUT);
    let started = Instant::now();
    let mut child = command.spawn().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to start sudo command in cwd {}: {error}",
                manifest.cwd
            ),
        )
    })?;
    let stdout_reader = child.stdout.take().map(read_pipe_in_thread);
    let stderr_reader = child.stderr.take().map(read_pipe_in_thread);
    let wait = wait_child_bounded(&mut child, timeout)?;
    let duration_seconds = started.elapsed().as_secs_f64();
    let stdout_bytes = join_reader(stdout_reader)?;
    let stderr_bytes = join_reader(stderr_reader)?;
    if !stdout_bytes.is_empty() {
        stderr.write_all(&stdout_bytes).map_err(internal_io)?;
    }
    if !stderr_bytes.is_empty() {
        stderr.write_all(&stderr_bytes).map_err(internal_io)?;
    }
    let mut combined = Vec::new();
    combined.extend_from_slice(&stdout_bytes);
    combined.extend_from_slice(&stderr_bytes);
    let output_tail = ledger_output(&combined, manifest.output_to_agent);
    let (status, exit_code, outcome) = match wait {
        WaitOutcome::Exited(status) if status.success() => (
            SudoLedgerEntryStatusWire::Ran,
            status.code(),
            CommandOutcome::Ok,
        ),
        WaitOutcome::Exited(status) => (
            SudoLedgerEntryStatusWire::Failed,
            status.code(),
            CommandOutcome::Failed,
        ),
        WaitOutcome::TimedOut => (
            SudoLedgerEntryStatusWire::Failed,
            None,
            CommandOutcome::TimedOut,
        ),
        WaitOutcome::Cancelled => (
            SudoLedgerEntryStatusWire::Failed,
            None,
            CommandOutcome::Cancelled,
        ),
    };
    Ok(CommandResult {
        entry: SudoLedgerEntryWire {
            id: command_wire.id.clone(),
            status,
            exit_code,
            duration_seconds,
            output_tail,
        },
        outcome,
    })
}

#[cfg(unix)]
fn read_pipe_in_thread<R>(
    mut reader: R,
) -> thread::JoinHandle<io::Result<Vec<u8>>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
}

#[cfg(unix)]
fn join_reader(
    reader: Option<thread::JoinHandle<io::Result<Vec<u8>>>>,
) -> Result<Vec<u8>, SudoRunnerCliError> {
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };
    reader
        .join()
        .map_err(|_| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                "sudo output reader thread panicked".to_string(),
            )
        })?
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to read sudo output: {error}"),
            )
        })
}

#[cfg(unix)]
enum WaitOutcome {
    Exited(ExitStatus),
    TimedOut,
    Cancelled,
}

#[cfg(unix)]
fn wait_child_bounded(
    child: &mut Child,
    timeout: Duration,
) -> Result<WaitOutcome, SudoRunnerCliError> {
    wait_child_bounded_with_cancel(child, timeout, || {
        CANCELLED.load(Ordering::SeqCst)
    })
}

#[cfg(unix)]
fn wait_child_bounded_with_cancel(
    child: &mut Child,
    timeout: Duration,
    mut cancelled: impl FnMut() -> bool,
) -> Result<WaitOutcome, SudoRunnerCliError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed while waiting for sudo command: {error}"),
            )
        })? {
            return Ok(WaitOutcome::Exited(status));
        }
        if cancelled() {
            terminate_child_group(child);
            return Ok(WaitOutcome::Cancelled);
        }
        if Instant::now() >= deadline {
            terminate_child_group(child);
            return Ok(WaitOutcome::TimedOut);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
}

#[cfg(unix)]
fn configure_new_session(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(unix)]
fn configure_process_group_and_account(
    command: &mut Command,
    account: &ResolvedAccount,
) {
    use std::os::unix::process::CommandExt;

    let uid = account.uid;
    let gid = account.gid;
    let name = account.name.clone();
    unsafe {
        command.pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::geteuid() == 0 {
                if libc::initgroups(name.as_ptr(), gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::setgid(gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::setuid(uid) != 0 {
                    return Err(io::Error::last_os_error());
                }
            } else if libc::geteuid() != uid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "sudo root worker is not privileged enough to switch user",
                ));
            }
            Ok(())
        });
    }
}

#[cfg(unix)]
fn resolve_account(run_as: &str) -> Result<ResolvedAccount, String> {
    let name = std::ffi::CString::new(run_as)
        .map_err(|_| "sudo run_as contains an interior NUL".to_string())?;
    lookup_account_by_name(&name)
        .or_else(|| {
            run_as
                .parse::<libc::uid_t>()
                .ok()
                .and_then(lookup_account_by_uid)
        })
        .ok_or_else(|| format!("sudo run_as account {run_as:?} does not exist"))
}

#[cfg(unix)]
fn lookup_account_by_name(name: &std::ffi::CString) -> Option<ResolvedAccount> {
    let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; passwd_buffer_size()];
    let code = unsafe {
        libc::getpwnam_r(
            name.as_ptr(),
            pwd.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if code != 0 || result.is_null() {
        return None;
    }
    let pwd = unsafe { pwd.assume_init() };
    account_from_passwd(&pwd)
}

#[cfg(unix)]
fn lookup_account_by_uid(uid: libc::uid_t) -> Option<ResolvedAccount> {
    let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; passwd_buffer_size()];
    let code = unsafe {
        libc::getpwuid_r(
            uid,
            pwd.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if code != 0 || result.is_null() {
        return None;
    }
    let pwd = unsafe { pwd.assume_init() };
    account_from_passwd(&pwd)
}

#[cfg(unix)]
fn account_from_passwd(pwd: &libc::passwd) -> Option<ResolvedAccount> {
    let name = unsafe { std::ffi::CStr::from_ptr(pwd.pw_name) }.to_owned();
    Some(ResolvedAccount {
        uid: pwd.pw_uid,
        gid: pwd.pw_gid,
        name,
    })
}

#[cfg(unix)]
fn passwd_buffer_size() -> usize {
    let value = unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) };
    if value > 0 {
        value as usize
    } else {
        16 * 1024
    }
}

#[cfg(unix)]
fn reject_existing_path(
    path: &Path,
    label: &str,
) -> Result<(), SudoRunnerCliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("refusing symlink sudo {label} path {}", path.display()),
        )),
        Ok(_) => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "refusing to replace existing sudo {label} path {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to inspect sudo {label} path {}: {error}",
                path.display()
            ),
        )),
    }
}

#[cfg(unix)]
fn reject_symlink_path(
    path: &Path,
    label: &str,
) -> Result<(), SudoRunnerCliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("refusing symlink sudo {label} path {}", path.display()),
        )),
        Ok(_) | Err(_) => Ok(()),
    }
}

#[cfg(unix)]
fn open_log_file(
    path: &Path,
    uid: u32,
    gid: u32,
) -> Result<File, SudoRunnerCliError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    reject_existing_path(path, "log")?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(path)
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to create sudo log {}: {error}",
                    path.display()
                ),
            )
        })?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to chmod sudo log {}: {error}", path.display()),
            )
        })?;
    chown_file_for_user(&file, uid, gid, path)?;
    Ok(file)
}

#[cfg(unix)]
fn write_ledger_file(
    path: &Path,
    ledger: &SudoLedgerWire,
    manifest: &SudoManifestWire,
    uid: u32,
    gid: u32,
) -> Result<(), SudoRunnerCliError> {
    validate_sudo_ledger(ledger, Some(manifest)).map_err(|error| {
        cli_error(SudoRunnerExitStatus::RunnerError, error.message)
    })?;
    write_atomic_json_for_user(path, ledger, uid, gid)
}

#[cfg(unix)]
fn write_atomic_json_for_user<T: serde::Serialize>(
    path: &Path,
    value: &T,
    uid: u32,
    gid: u32,
) -> Result<(), SudoRunnerCliError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    reject_existing_path(path, "output")?;
    let parent = path.parent().ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("sudo output path has no parent: {}", path.display()),
        )
    })?;
    let filename =
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                cli_error(
                    SudoRunnerExitStatus::RunnerError,
                    format!(
                        "sudo output path is not UTF-8: {}",
                        path.display()
                    ),
                )
            })?;
    let tmp_path =
        parent.join(format!(".{filename}.tmp.{}", std::process::id()));
    let _ = fs::remove_file(&tmp_path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(&tmp_path)
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to create sudo output {}: {error}",
                    tmp_path.display()
                ),
            )
        })?;
    serde_json::to_writer(&mut file, value).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to serialize sudo output JSON: {error}"),
        )
    })?;
    writeln!(file).map_err(internal_io)?;
    file.flush().map_err(internal_io)?;
    file.sync_all().map_err(internal_io)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to chmod sudo output {}: {error}",
                    tmp_path.display()
                ),
            )
        })?;
    chown_file_for_user(&file, uid, gid, &tmp_path)?;
    reject_existing_path(path, "output")?;
    fs::rename(&tmp_path, path).map_err(|error| {
        let _ = fs::remove_file(&tmp_path);
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to publish sudo output {}: {error}",
                path.display()
            ),
        )
    })
}

#[cfg(unix)]
fn chown_file_for_user(
    file: &File,
    uid: u32,
    gid: u32,
    path: &Path,
) -> Result<(), SudoRunnerCliError> {
    use std::os::unix::io::AsRawFd;

    if unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    if unsafe { libc::fchown(file.as_raw_fd(), uid, gid) } == 0 {
        Ok(())
    } else {
        Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to chown sudo output {}: {}",
                path.display(),
                io::Error::last_os_error()
            ),
        ))
    }
}

#[cfg(unix)]
fn terminate_child_group(child: &mut Child) {
    let pgid = child.id() as libc::pid_t;
    let _ = unsafe { libc::kill(-pgid, libc::SIGTERM) };
    let grace = Instant::now() + KILL_GRACE;
    while Instant::now() < grace {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        thread::sleep(POLL_INTERVAL);
    }
    let _ = unsafe { libc::kill(-pgid, libc::SIGKILL) };
    let _ = child.wait();
}

fn ledger_output(combined: &[u8], policy: SudoOutputPolicyWire) -> String {
    if policy == SudoOutputPolicyWire::None {
        return String::new();
    }
    let lossy = String::from_utf8_lossy(combined);
    match policy {
        SudoOutputPolicyWire::None => String::new(),
        SudoOutputPolicyWire::Tail => truncate_sudo_output_tail(
            &lossy,
            SUDO_MAX_OUTPUT_TAIL_BYTES.min(8192),
        ),
        SudoOutputPolicyWire::Full => {
            truncate_sudo_output_tail(&lossy, SUDO_MAX_OUTPUT_TAIL_BYTES)
        }
    }
}

fn tty_available(config: &SudoRunnerConfig) -> bool {
    if let Some(value) = config.tty_available {
        return value;
    }
    platform_tty_available()
}

#[cfg(unix)]
fn platform_tty_available() -> bool {
    (unsafe { libc::isatty(libc::STDIN_FILENO) == 1 })
        || fs::File::open("/dev/tty").is_ok()
}

#[cfg(not(unix))]
fn platform_tty_available() -> bool {
    false
}

#[cfg(unix)]
fn install_cancellation_handlers() {
    SIGNALS_INSTALLED.call_once(|| unsafe {
        libc::signal(
            libc::SIGINT,
            handle_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_signal as *const () as libc::sighandler_t,
        );
    });
}

#[cfg(unix)]
extern "C" fn handle_signal(_signal: libc::c_int) {
    CANCELLED.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
fn harden_process() -> Result<(), String> {
    let rlimit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &rlimit) } != 0 {
        return Err(format!(
            "failed to disable core dumps: {}",
            io::Error::last_os_error()
        ));
    }
    #[cfg(target_os = "linux")]
    {
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            return Err(format!(
                "failed to disable process dumpability: {}",
                io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let a = left.get(index).copied().unwrap_or(0);
        let b = right.get(index).copied().unwrap_or(0);
        diff |= (a ^ b) as usize;
    }
    diff == 0
}

fn internal_io(error: io::Error) -> SudoRunnerCliError {
    cli_error(
        SudoRunnerExitStatus::RunnerError,
        format!("sudo runner I/O failed: {error}"),
    )
}

fn cli_error(
    status: SudoRunnerExitStatus,
    message: String,
) -> SudoRunnerCliError {
    SudoRunnerCliError { status, message }
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use sase_core::{sudo_manifest_sha256, SudoCommandWire, SudoManifestWire};
    use tempfile::TempDir;

    use super::*;

    struct Fixture {
        _tmp: TempDir,
        sudo_path: PathBuf,
        handoff_dir: PathBuf,
        manifest_path: PathBuf,
        config: SudoRunnerConfig,
        digest: String,
    }

    impl Fixture {
        fn new(manifest: SudoManifestWire) -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let sudo_path = tmp.path().join("fake-sudo");
            fs::write(&sudo_path, fake_sudo_script()).unwrap();
            fs::set_permissions(&sudo_path, fs::Permissions::from_mode(0o755))
                .unwrap();
            let executor_path = tmp.path().join("fake-executor");
            fs::write(&executor_path, fake_executor_script()).unwrap();
            fs::set_permissions(
                &executor_path,
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
            fs::write(
                sudo_path.with_extension("executor"),
                executor_path.display().to_string(),
            )
            .unwrap();
            let manifest_path = tmp.path().join("manifest.json");
            fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap())
                .unwrap();
            let digest = sudo_manifest_sha256(&manifest).unwrap();
            let mut config = SudoRunnerConfig::test(sudo_path.clone());
            config.runner_path = Some(executor_path.clone());
            Self {
                handoff_dir: tmp.path().to_path_buf(),
                _tmp: tmp,
                sudo_path,
                manifest_path,
                config,
                digest,
            }
        }

        fn run(&self) -> (Result<(), SudoRunnerCliError>, String, String) {
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let result = run_sudo_runner_cli_with_io(
                [
                    "-m".to_string(),
                    self.manifest_path.display().to_string(),
                    "-e".to_string(),
                    self.digest.clone(),
                ],
                &self.config,
                &mut stdout,
                &mut stderr,
            );
            (
                result,
                String::from_utf8(stdout).unwrap(),
                String::from_utf8(stderr).unwrap(),
            )
        }

        fn run_detach(
            &self,
        ) -> (Result<(), SudoRunnerCliError>, String, String) {
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let result = run_sudo_runner_cli_with_io(
                [
                    "-m".to_string(),
                    self.manifest_path.display().to_string(),
                    "-e".to_string(),
                    self.digest.clone(),
                    "-d".to_string(),
                    self.handoff_dir.display().to_string(),
                ],
                &self.config,
                &mut stdout,
                &mut stderr,
            );
            (
                result,
                String::from_utf8(stdout).unwrap(),
                String::from_utf8(stderr).unwrap(),
            )
        }

        fn calls(&self) -> Vec<String> {
            fs::read_to_string(self.sudo_path.with_extension("calls"))
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }

        fn argv_calls(&self) -> Vec<Vec<String>> {
            let content =
                fs::read_to_string(self.sudo_path.with_extension("argv"))
                    .unwrap_or_default();
            let mut calls = Vec::new();
            let mut current = Vec::new();
            let mut in_call = false;
            for line in content.lines() {
                match line {
                    "BEGIN" => {
                        in_call = true;
                        current.clear();
                    }
                    "END" if in_call => {
                        calls.push(std::mem::take(&mut current));
                        in_call = false;
                    }
                    _ if in_call => current.push(line.to_string()),
                    _ => {}
                }
            }
            calls
        }

        fn cwd_log(&self) -> Vec<String> {
            fs::read_to_string(self.sudo_path.with_extension("cwd"))
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }

        fn env_log(&self) -> String {
            fs::read_to_string(self.sudo_path.with_extension("env"))
                .unwrap_or_default()
        }

        fn touch(&self, suffix: &str) {
            fs::write(self.sudo_path.with_extension(suffix), "").unwrap();
        }

        fn write_marker(&self, suffix: &str, value: &str) {
            fs::write(self.sudo_path.with_extension(suffix), value).unwrap();
        }
    }

    fn manifest() -> SudoManifestWire {
        SudoManifestWire {
            schema_version: 1,
            request_id: "req-1".to_string(),
            host: "athena".to_string(),
            host_is_remote: false,
            run_as: "root".to_string(),
            cwd: "/tmp".to_string(),
            env: [("APP_MODE".to_string(), "reviewed".to_string())]
                .into_iter()
                .collect(),
            stop_on_failure: true,
            output_to_agent: SudoOutputPolicyWire::Tail,
            commands: vec![
                SudoCommandWire {
                    id: "one".to_string(),
                    argv: vec!["ok".to_string()],
                    why: "Run ok".to_string(),
                    timeout_seconds: Some(1.0),
                    shell: false,
                },
                SudoCommandWire {
                    id: "two".to_string(),
                    argv: vec!["ok".to_string()],
                    why: "Run second".to_string(),
                    timeout_seconds: Some(1.0),
                    shell: false,
                },
            ],
            resume_from: None,
        }
    }

    fn fake_sudo_script() -> &'static str {
        r#"#!/bin/sh
set -eu
base="$0"
{
  printf 'BEGIN\n'
  for arg do
    printf '%s\n' "$arg"
  done
  printf 'END\n'
} >> "$base.argv"
pwd >> "$base.cwd"
printf '%s\n' "$*" >> "$base.calls"
if [ "$#" -eq 1 ] && [ "$1" = "-k" ]; then
  printf 'cleanup\n' >> "$base.cleanups"
  exit 0
fi
if [ "$#" -eq 1 ] && [ "$1" = "-v" ]; then
  if [ -f "$base.auth_fail" ]; then exit 1; fi
  exit 0
fi
if [ "$#" -eq 2 ] && [ "$1" = "-n" ] && [ "$2" = "-v" ]; then
  if [ -f "$base.probe_fail" ]; then exit 1; fi
  if [ -f "$base.remove_cwd_on_probe" ]; then
    cwd_to_remove="$(/usr/bin/cat "$base.remove_cwd_on_probe")"
    /usr/bin/rmdir "$cwd_to_remove" 2>/dev/null || /usr/bin/rm -rf "$cwd_to_remove"
  fi
  exit 0
fi
for arg do
  if [ "$arg" = "--" ]; then
    break
  fi
  if [ "$arg" = "-D" ] || [ "$arg" = "--chdir" ]; then
    printf 'sudo: you are not permitted to use the -D option with simulated-command\n' >&2
    exit 1
  fi
done
/usr/bin/env | /usr/bin/sort > "$base.env"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--" ]; then
    shift
    break
  fi
  shift
done
cmd="${1:-}"
executor_path="$(/usr/bin/cat "$base.executor" 2>/dev/null || true)"
if [ -n "$executor_path" ] && [ "$cmd" = "$executor_path" ]; then
  if [ -f "$base.root_spawn_fail" ]; then exit 42; fi
  "$@"
  exit $?
fi
case "$cmd" in
  ok)
    printf 'stdout-ok\n'
    printf 'stderr-ok\n' >&2
    exit 0
    ;;
  fail)
    printf 'failed-command\n'
    exit 7
    ;;
  slow)
    /bin/sleep 5
    exit 0
    ;;
  read-file)
    /usr/bin/cat "$2"
    exit 0
    ;;
  /usr/bin/apt-get)
    printf 'apt-get-simulated\n'
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#
    }

    fn fake_executor_script() -> &'static str {
        r#"#!/bin/sh
set -eu
detach_dir=""
expected=""
started_path=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --detach-dir)
      detach_dir="$2"
      shift 2
      ;;
    --expected-sha256)
      expected="$2"
      shift 2
      ;;
    --started-path)
      started_path="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -z "$started_path" ]; then
  exit 64
fi
tmp="$started_path.tmp"
cat > "$tmp" <<EOF
{"schema_version":1,"kind":"sudo_exec_started","manifest_sha256":"$expected","executor_pid":$$,"executor_identity":"boot-a:12345","ledger_path":"$detach_dir/ledger.json","log_path":"$detach_dir/output.log","started_at":1800000000.0}
EOF
mv "$tmp" "$started_path"
"#
    }

    fn ledger(stdout: &str) -> SudoLedgerWire {
        serde_json::from_str(stdout.trim()).unwrap()
    }

    fn current_username() -> String {
        let uid = unsafe { libc::geteuid() };
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buffer = vec![0u8; 16 * 1024];
        let code = unsafe {
            libc::getpwuid_r(
                uid,
                pwd.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        };
        assert_eq!(code, 0);
        assert!(!result.is_null());
        let pwd = unsafe { pwd.assume_init() };
        unsafe { std::ffi::CStr::from_ptr(pwd.pw_name) }
            .to_string_lossy()
            .into_owned()
    }

    fn write_manifest(path: &Path, manifest: &SudoManifestWire) -> String {
        fs::write(path, serde_json::to_vec(manifest).unwrap()).unwrap();
        sudo_manifest_sha256(manifest).unwrap()
    }

    #[test]
    fn help_succeeds_without_tty_or_sudo() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut config = SudoRunnerConfig::production();
        config.tty_available = Some(false);
        let result = run_sudo_runner_cli_with_io(
            ["--help".to_string()],
            &config,
            &mut stdout,
            &mut stderr,
        );
        assert!(result.is_ok());
        let help = String::from_utf8(stdout).unwrap();
        assert!(help.contains("sase_sudo_runner"));
        assert!(help.contains("--detach-dir"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn capabilities_succeed_without_tty_manifest_or_sudo() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut config = SudoRunnerConfig::production();
        config.tty_available = Some(false);
        let result = run_sudo_runner_cli_with_io(
            ["--capabilities".to_string()],
            &config,
            &mut stdout,
            &mut stderr,
        );
        assert!(result.is_ok());
        assert!(stderr.is_empty());
        let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "schema_version": 1,
                "capabilities": ["detached_execution"]
            })
        );
    }

    #[test]
    fn capabilities_reject_other_arguments() {
        let fixture = Fixture::new(manifest());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = run_sudo_runner_cli_with_io(
            [
                "--capabilities".to_string(),
                "-m".to_string(),
                fixture.manifest_path.display().to_string(),
            ],
            &fixture.config,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), SUDO_RUNNER_INVALID_INPUT_EXIT);
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
    }

    #[test]
    fn digest_mismatch_runs_nothing() {
        let fixture = Fixture::new(manifest());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = run_sudo_runner_cli_with_io(
            [
                "-m".to_string(),
                fixture.manifest_path.display().to_string(),
                "-e".to_string(),
                "a".repeat(64),
            ],
            &fixture.config,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        assert_eq!(result.exit_code(), SUDO_RUNNER_INVALID_INPUT_EXIT);
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        assert!(fixture.calls().is_empty());
    }

    #[test]
    fn tty_unavailable_emits_skipped_ledger_and_never_invokes_sudo() {
        let mut fixture = Fixture::new(manifest());
        fixture.config.tty_available = Some(false);
        let result = fixture.run().0.unwrap_err();
        assert_eq!(result.exit_code(), SUDO_RUNNER_TTY_UNAVAILABLE_EXIT);
        let (_, stdout, _) = fixture.run();
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::TtyUnavailable);
        assert!(ledger
            .entries
            .iter()
            .all(|entry| entry.status == SudoLedgerEntryStatusWire::Skipped));
        assert!(fixture.calls().is_empty());
    }

    #[test]
    fn successful_run_uses_expected_sudo_order_and_cleared_environment() {
        let fixture = Fixture::new(manifest());
        let parent_cwd = std::env::current_dir().unwrap();
        let (result, stdout, stderr) = fixture.run();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(std::env::current_dir().unwrap(), parent_cwd);
        let calls = fixture.calls();
        assert_eq!(calls[0], "-k");
        assert_eq!(calls[1], "-v");
        assert_eq!(calls[2], "-n -v");
        assert_eq!(calls[3], "-n -u root -- ok");
        assert_eq!(calls[4], "-n -v");
        assert_eq!(calls[5], "-n -u root -- ok");
        assert_eq!(calls[6], "-k");
        let argv_calls = fixture.argv_calls();
        assert_eq!(
            argv_calls[3],
            ["-n", "-u", "root", "--", "ok"].map(str::to_string)
        );
        assert_eq!(fixture.cwd_log()[3], "/tmp");
        let env_log = fixture.env_log();
        assert!(env_log.contains("APP_MODE=reviewed"));
        assert!(env_log.contains("LC_ALL=C"));
        assert!(!env_log.contains("PATH="));
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::Completed);
        assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Ran);
        assert!(ledger.entries[0].output_tail.contains("stdout-ok"));
        assert!(stderr.contains("stdout-ok"));
    }

    #[test]
    fn apt_get_shaped_command_uses_requested_cwd_without_sudo_chdir_option() {
        let mut manifest = manifest();
        manifest.cwd = "/".to_string();
        manifest.commands = vec![SudoCommandWire {
            id: "install".to_string(),
            argv: vec![
                "/usr/bin/apt-get".to_string(),
                "install".to_string(),
                "-y".to_string(),
                "texlive-xetex".to_string(),
            ],
            why: "Install package".to_string(),
            timeout_seconds: Some(1.0),
            shell: false,
        }];
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok(), "{result:?}");
        let argv_calls = fixture.argv_calls();
        assert_eq!(
            argv_calls[3],
            [
                "-n",
                "-u",
                "root",
                "--",
                "/usr/bin/apt-get",
                "install",
                "-y",
                "texlive-xetex"
            ]
            .map(str::to_string)
        );
        assert!(!argv_calls[3][..3].iter().any(|arg| arg == "-D"));
        assert_eq!(fixture.cwd_log()[3], "/");
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::Completed);
        assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Ran);
        assert!(ledger.entries[0].output_tail.contains("apt-get-simulated"));
    }

    #[test]
    fn command_literal_chdir_flag_after_boundary_is_allowed() {
        let mut manifest = manifest();
        manifest.commands.truncate(1);
        manifest.commands[0].argv = vec!["ok".to_string(), "-D".to_string()];
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            fixture.argv_calls()[3],
            ["-n", "-u", "root", "--", "ok", "-D"].map(str::to_string)
        );
        assert_eq!(
            ledger(&stdout).entries[0].status,
            SudoLedgerEntryStatusWire::Ran
        );
    }

    #[test]
    fn command_runs_in_distinct_cwd_with_spaces_and_parent_cwd_is_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let cwd = tmp.path().join("working dir with spaces");
        fs::create_dir(&cwd).unwrap();
        fs::write(cwd.join("relative.txt"), "cwd payload\n").unwrap();
        let mut manifest = manifest();
        manifest.cwd = cwd.display().to_string();
        manifest.commands.truncate(1);
        manifest.commands[0].argv =
            vec!["read-file".to_string(), "relative.txt".to_string()];
        let fixture = Fixture::new(manifest);
        let parent_cwd = std::env::current_dir().unwrap();
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(std::env::current_dir().unwrap(), parent_cwd);
        assert_eq!(fixture.cwd_log()[3], cwd.display().to_string());
        let ledger = ledger(&stdout);
        assert!(ledger.entries[0].output_tail.contains("cwd payload"));
    }

    #[test]
    fn failed_command_stops_remaining_entries_by_default() {
        let mut manifest = manifest();
        manifest.commands[0].argv = vec!["fail".to_string()];
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok());
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::Completed);
        assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Failed);
        assert_eq!(ledger.entries[0].exit_code, Some(7));
        assert_eq!(
            ledger.entries[1].status,
            SudoLedgerEntryStatusWire::Skipped
        );
    }

    #[test]
    fn resume_from_skips_prior_commands_and_probe_failure_uses_plain_sudo() {
        let mut manifest = manifest();
        manifest.resume_from = Some("two".to_string());
        let fixture = Fixture::new(manifest);
        fixture.touch("probe_fail");
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok());
        let ledger = ledger(&stdout);
        assert_eq!(
            ledger.entries[0].status,
            SudoLedgerEntryStatusWire::Skipped
        );
        assert_eq!(ledger.entries[1].status, SudoLedgerEntryStatusWire::Ran);
        let calls = fixture.calls();
        assert_eq!(calls[2], "-n -v");
        assert_eq!(calls[3], "-u root -- ok");
    }

    #[test]
    fn initial_auth_failure_marks_all_entries_skipped() {
        let fixture = Fixture::new(manifest());
        fixture.touch("auth_fail");
        let (result, stdout, _) = fixture.run();
        assert_eq!(
            result.unwrap_err().exit_code(),
            SUDO_RUNNER_AUTH_FAILED_EXIT
        );
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::AuthFailed);
        assert!(ledger
            .entries
            .iter()
            .all(|entry| entry.status == SudoLedgerEntryStatusWire::Skipped));
        assert_eq!(fixture.calls(), vec!["-k", "-v", "-k"]);
    }

    #[test]
    fn detached_auth_failure_keeps_ledger_shaped_output() {
        let fixture = Fixture::new(manifest());
        fixture.touch("auth_fail");
        let (result, stdout, _) = fixture.run_detach();
        assert_eq!(
            result.unwrap_err().exit_code(),
            SUDO_RUNNER_AUTH_FAILED_EXIT
        );
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::AuthFailed);
        assert!(ledger
            .entries
            .iter()
            .all(|entry| entry.status == SudoLedgerEntryStatusWire::Skipped));
        assert_eq!(fixture.calls(), vec!["-k", "-v", "-k"]);
        assert!(!fixture.handoff_dir.join(STARTED_FILENAME).exists());
    }

    #[test]
    fn detached_root_spawn_failure_keeps_ledger_shaped_output() {
        let fixture = Fixture::new(manifest());
        fixture.touch("root_spawn_fail");
        let (result, stdout, _) = fixture.run_detach();
        assert_eq!(
            result.unwrap_err().exit_code(),
            SUDO_RUNNER_RUNNER_ERROR_EXIT
        );
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::RunnerError);
        assert!(ledger
            .diagnostic
            .as_deref()
            .unwrap_or_default()
            .contains("root executor spawn failed"));
        assert_eq!(fixture.calls().first().map(String::as_str), Some("-k"));
        assert_eq!(fixture.calls().get(1).map(String::as_str), Some("-v"));
        assert_eq!(fixture.calls().last().map(String::as_str), Some("-k"));
    }

    #[test]
    fn detached_success_emits_validated_handshake() {
        let fixture = Fixture::new(manifest());
        let (result, stdout, stderr) = fixture.run_detach();
        assert!(result.is_ok(), "{result:?}");
        assert!(stderr.is_empty());
        let handshake: SudoExecStartedWire =
            serde_json::from_str(stdout.trim()).unwrap();
        assert_eq!(handshake.kind, SUDO_EXEC_STARTED_KIND);
        assert_eq!(handshake.manifest_sha256, fixture.digest);
        assert_eq!(
            handshake.ledger_path,
            fixture
                .handoff_dir
                .join(LEDGER_FILENAME)
                .display()
                .to_string()
        );
        assert_eq!(
            handshake.log_path,
            fixture.handoff_dir.join(LOG_FILENAME).display().to_string()
        );
        validate_sudo_exec_started(
            &handshake,
            Some(
                &sudo_manifest_from_json_slice(
                    &fs::read(&fixture.manifest_path).unwrap(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(fixture.calls().first().map(String::as_str), Some("-k"));
        assert_eq!(fixture.calls().get(1).map(String::as_str), Some("-v"));
        assert!(fixture
            .calls()
            .iter()
            .any(|call| call.contains("--internal-root-exec")));
        assert_eq!(fixture.calls().last().map(String::as_str), Some("-k"));
    }

    #[test]
    fn nonexistent_cwd_fails_before_dispatch_and_cleans_up() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("missing");
        let mut manifest = manifest();
        manifest.cwd = missing.display().to_string();
        manifest.commands.truncate(1);
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert_eq!(
            result.unwrap_err().exit_code(),
            SUDO_RUNNER_RUNNER_ERROR_EXIT
        );
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::RunnerError);
        assert!(ledger
            .diagnostic
            .as_deref()
            .unwrap_or_default()
            .contains("failed to start sudo command in cwd"));
        assert_eq!(
            ledger.entries[0].status,
            SudoLedgerEntryStatusWire::Skipped
        );
        assert_eq!(fixture.calls(), vec!["-k", "-v", "-n -v", "-k"]);
        assert!(fixture
            .argv_calls()
            .iter()
            .all(|argv| !argv.iter().any(|arg| arg == "--")));
    }

    #[test]
    fn cwd_removed_after_authentication_fails_before_dispatch_and_cleans_up() {
        let tmp = tempfile::tempdir().unwrap();
        let cwd = tmp.path().join("vanishing");
        fs::create_dir(&cwd).unwrap();
        let mut manifest = manifest();
        manifest.cwd = cwd.display().to_string();
        manifest.commands.truncate(1);
        let fixture = Fixture::new(manifest);
        fixture.write_marker("remove_cwd_on_probe", &cwd.display().to_string());
        let (result, stdout, _) = fixture.run();
        assert_eq!(
            result.unwrap_err().exit_code(),
            SUDO_RUNNER_RUNNER_ERROR_EXIT
        );
        assert!(!cwd.exists());
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::RunnerError);
        assert_eq!(
            ledger.entries[0].status,
            SudoLedgerEntryStatusWire::Skipped
        );
        assert_eq!(fixture.calls(), vec!["-k", "-v", "-n -v", "-k"]);
    }

    #[test]
    fn permission_denied_cwd_fails_before_dispatch_when_supported() {
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let cwd = tmp.path().join("blocked");
        fs::create_dir(&cwd).unwrap();
        fs::set_permissions(&cwd, fs::Permissions::from_mode(0o000)).unwrap();
        let mut manifest = manifest();
        manifest.cwd = cwd.display().to_string();
        manifest.commands.truncate(1);
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        fs::set_permissions(&cwd, fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            result.unwrap_err().exit_code(),
            SUDO_RUNNER_RUNNER_ERROR_EXIT
        );
        let ledger = ledger(&stdout);
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::RunnerError);
        assert!(ledger
            .diagnostic
            .as_deref()
            .unwrap_or_default()
            .contains(&cwd.display().to_string()));
        assert_eq!(fixture.calls(), vec!["-k", "-v", "-n -v", "-k"]);
    }

    #[test]
    fn timeout_terminates_process_group_and_records_failed_entry() {
        let mut manifest = manifest();
        manifest.commands[0].argv = vec!["slow".to_string()];
        manifest.commands[0].timeout_seconds = Some(0.05);
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok());
        let ledger = ledger(&stdout);
        assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Failed);
        assert_eq!(ledger.entries[0].exit_code, None);
        assert_eq!(
            ledger.entries[1].status,
            SudoLedgerEntryStatusWire::Skipped
        );
        assert_eq!(fixture.calls().last().map(String::as_str), Some("-k"));
    }

    #[test]
    fn output_policy_none_keeps_ledger_empty() {
        let mut manifest = manifest();
        manifest.output_to_agent = SudoOutputPolicyWire::None;
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok());
        let ledger = ledger(&stdout);
        assert_eq!(ledger.entries[0].output_tail, "");
    }

    #[test]
    fn output_policy_full_is_hard_bounded() {
        let mut manifest = manifest();
        manifest.output_to_agent = SudoOutputPolicyWire::Full;
        manifest.commands[0].argv = vec!["ok".to_string()];
        let fixture = Fixture::new(manifest);
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok());
        let ledger = ledger(&stdout);
        assert!(
            ledger.entries[0].output_tail.len() <= SUDO_MAX_OUTPUT_TAIL_BYTES
        );
    }

    #[test]
    fn internal_worker_writes_ledger_and_log_for_direct_command() {
        let tmp = tempfile::tempdir().unwrap();
        let command_path = tmp.path().join("emit-output.sh");
        fs::write(
            &command_path,
            "#!/bin/sh\nprintf 'worker-stdout\\n'\nprintf 'worker-stderr\\n' >&2\n",
        )
        .unwrap();
        fs::set_permissions(&command_path, fs::Permissions::from_mode(0o755))
            .unwrap();
        let mut manifest = manifest();
        manifest.run_as = current_username();
        manifest.cwd = tmp.path().display().to_string();
        manifest.commands = vec![SudoCommandWire {
            id: "emit".to_string(),
            argv: vec![command_path.display().to_string()],
            why: "Emit output".to_string(),
            timeout_seconds: Some(2.0),
            shell: false,
        }];
        let manifest_path = tmp.path().join("manifest.json");
        let digest = write_manifest(&manifest_path, &manifest);
        let config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = run_sudo_runner_cli_with_io(
            [
                "--internal-root-worker".to_string(),
                "-m".to_string(),
                manifest_path.display().to_string(),
                "-e".to_string(),
                digest,
                "-d".to_string(),
                tmp.path().display().to_string(),
                "--parent-uid".to_string(),
                unsafe { libc::geteuid() }.to_string(),
                "--parent-gid".to_string(),
                unsafe { libc::getegid() }.to_string(),
            ],
            &config,
            &mut stdout,
            &mut stderr,
        );
        assert!(result.is_ok(), "{result:?}");
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        let ledger: SudoLedgerWire = serde_json::from_slice(
            &fs::read(tmp.path().join(LEDGER_FILENAME)).unwrap(),
        )
        .unwrap();
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::Completed);
        assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Ran);
        assert!(ledger.entries[0].output_tail.contains("worker-stdout"));
        let log = fs::read_to_string(tmp.path().join(LOG_FILENAME)).unwrap();
        assert!(log.contains("worker-stdout"));
        assert!(log.contains("worker-stderr"));
    }

    #[test]
    fn internal_worker_stop_file_cancels_running_command() {
        let tmp = tempfile::tempdir().unwrap();
        let command_path = tmp.path().join("slow.sh");
        fs::write(&command_path, "#!/bin/sh\n/bin/sleep 5\n").unwrap();
        fs::set_permissions(&command_path, fs::Permissions::from_mode(0o755))
            .unwrap();
        let mut manifest = manifest();
        manifest.run_as = current_username();
        manifest.cwd = tmp.path().display().to_string();
        manifest.commands = vec![SudoCommandWire {
            id: "slow".to_string(),
            argv: vec![command_path.display().to_string()],
            why: "Wait for cancellation".to_string(),
            timeout_seconds: Some(5.0),
            shell: false,
        }];
        let manifest_path = tmp.path().join("manifest.json");
        let digest = write_manifest(&manifest_path, &manifest);
        let stop_path = tmp.path().join(STOP_FILENAME);
        let stop_thread = thread::spawn({
            let stop_path = stop_path.clone();
            move || {
                thread::sleep(Duration::from_millis(75));
                fs::write(stop_path, "").unwrap();
            }
        });
        let config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = run_sudo_runner_cli_with_io(
            [
                "--internal-root-worker".to_string(),
                "-m".to_string(),
                manifest_path.display().to_string(),
                "-e".to_string(),
                digest,
                "-d".to_string(),
                tmp.path().display().to_string(),
                "--parent-uid".to_string(),
                unsafe { libc::geteuid() }.to_string(),
                "--parent-gid".to_string(),
                unsafe { libc::getegid() }.to_string(),
            ],
            &config,
            &mut stdout,
            &mut stderr,
        );
        stop_thread.join().unwrap();
        assert!(result.is_ok(), "{result:?}");
        let ledger: SudoLedgerWire = serde_json::from_slice(
            &fs::read(tmp.path().join(LEDGER_FILENAME)).unwrap(),
        )
        .unwrap();
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::Cancelled);
        assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Failed);
        assert_eq!(ledger.entries[0].exit_code, None);
    }

    #[test]
    fn internal_worker_records_run_as_resolution_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = manifest();
        manifest.run_as = "definitely-missing-sase-user".to_string();
        manifest.cwd = tmp.path().display().to_string();
        manifest.commands.truncate(1);
        manifest.commands[0].argv = vec!["/bin/true".to_string()];
        let manifest_path = tmp.path().join("manifest.json");
        let digest = write_manifest(&manifest_path, &manifest);
        let config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = run_sudo_runner_cli_with_io(
            [
                "--internal-root-worker".to_string(),
                "-m".to_string(),
                manifest_path.display().to_string(),
                "-e".to_string(),
                digest,
                "-d".to_string(),
                tmp.path().display().to_string(),
                "--parent-uid".to_string(),
                unsafe { libc::geteuid() }.to_string(),
                "--parent-gid".to_string(),
                unsafe { libc::getegid() }.to_string(),
            ],
            &config,
            &mut stdout,
            &mut stderr,
        );
        assert!(result.is_ok(), "{result:?}");
        let ledger: SudoLedgerWire = serde_json::from_slice(
            &fs::read(tmp.path().join(LEDGER_FILENAME)).unwrap(),
        )
        .unwrap();
        assert_eq!(ledger.outcome, SudoLedgerOutcomeWire::RunnerError);
        assert!(ledger
            .diagnostic
            .as_deref()
            .unwrap_or_default()
            .contains("does not exist"));
    }

    #[test]
    fn internal_worker_rejects_ledger_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = manifest();
        manifest.run_as = current_username();
        manifest.cwd = tmp.path().display().to_string();
        manifest.commands.truncate(1);
        manifest.commands[0].argv = vec!["/bin/true".to_string()];
        let manifest_path = tmp.path().join("manifest.json");
        let digest = write_manifest(&manifest_path, &manifest);
        let target = tmp.path().join("target-ledger.json");
        std::os::unix::fs::symlink(&target, tmp.path().join(LEDGER_FILENAME))
            .unwrap();
        let config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = run_sudo_runner_cli_with_io(
            [
                "--internal-root-worker".to_string(),
                "-m".to_string(),
                manifest_path.display().to_string(),
                "-e".to_string(),
                digest,
                "-d".to_string(),
                tmp.path().display().to_string(),
                "--parent-uid".to_string(),
                unsafe { libc::geteuid() }.to_string(),
                "--parent-gid".to_string(),
                unsafe { libc::getegid() }.to_string(),
            ],
            &config,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
        assert!(!target.exists());
    }

    #[test]
    fn internal_worker_reverifies_manifest_digest() {
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = manifest();
        manifest.run_as = current_username();
        manifest.cwd = tmp.path().display().to_string();
        manifest.commands.truncate(1);
        manifest.commands[0].argv = vec!["/bin/true".to_string()];
        let manifest_path = tmp.path().join("manifest.json");
        write_manifest(&manifest_path, &manifest);
        let config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = run_sudo_runner_cli_with_io(
            [
                "--internal-root-worker".to_string(),
                "-m".to_string(),
                manifest_path.display().to_string(),
                "-e".to_string(),
                "a".repeat(64),
                "-d".to_string(),
                tmp.path().display().to_string(),
                "--parent-uid".to_string(),
                unsafe { libc::geteuid() }.to_string(),
                "--parent-gid".to_string(),
                unsafe { libc::getegid() }.to_string(),
            ],
            &config,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), SUDO_RUNNER_INVALID_INPUT_EXIT);
        assert!(!tmp.path().join(LEDGER_FILENAME).exists());
    }

    #[test]
    fn parse_requires_one_manifest_and_digest() {
        assert!(matches!(
            parse_sudo_runner_args(Vec::<String>::new()),
            ParseResult::Error(_)
        ));
        assert!(matches!(
            parse_sudo_runner_args(["-h".to_string()]),
            ParseResult::Help
        ));
        assert!(matches!(
            parse_sudo_runner_args([
                "-m".to_string(),
                "manifest.json".to_string(),
                "-e".to_string(),
                "a".repeat(64)
            ]),
            ParseResult::Cli(_)
        ));
    }

    #[test]
    fn ledger_json_validates_against_manifest() {
        let fixture = Fixture::new(manifest());
        let (result, stdout, _) = fixture.run();
        assert!(result.is_ok());
        let manifest_json =
            serde_json::from_slice(&fs::read(&fixture.manifest_path).unwrap())
                .unwrap();
        let ledger_json = serde_json::from_str(stdout.trim()).unwrap();
        sase_core::sudo_validate_ledger_json_value(
            &ledger_json,
            Some(&manifest_json),
        )
        .unwrap();
    }
}
