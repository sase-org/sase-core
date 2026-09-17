use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};

use sase_core::{
    sudo_manifest_from_json_slice, sudo_manifest_sha256,
    truncate_sudo_output_tail, validate_sudo_ledger, SudoErrorCodeWire,
    SudoLedgerEntryStatusWire, SudoLedgerEntryWire, SudoLedgerOutcomeWire,
    SudoLedgerWire, SudoManifestWire, SudoOutputPolicyWire, SudoWireError,
    SUDO_LEDGER_WIRE_SCHEMA_VERSION, SUDO_MANIFEST_MAX_BYTES,
    SUDO_MAX_COMMANDS, SUDO_MAX_OUTPUT_TAIL_BYTES,
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
    tty_available: Option<bool>,
    harden_process: bool,
}

impl SudoRunnerConfig {
    fn production() -> Self {
        Self {
            sudo_path: PathBuf::from(PRODUCTION_SUDO),
            tty_available: None,
            harden_process: true,
        }
    }

    #[cfg(test)]
    fn test(sudo_path: PathBuf) -> Self {
        Self {
            sudo_path,
            tty_available: Some(true),
            harden_process: false,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SudoRunnerCli {
    manifest_path: PathBuf,
    expected_sha256: String,
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
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
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
            _ => {
                return ParseResult::Error(format!("unknown argument: {arg}"));
            }
        }
    }
    let Some(manifest_path) = manifest_path else {
        return ParseResult::Error("--manifest|-m is required".to_string());
    };
    let Some(expected_sha256) = expected_sha256 else {
        return ParseResult::Error(
            "--expected-sha256|-e is required".to_string(),
        );
    };
    ParseResult::Cli(SudoRunnerCli {
        manifest_path,
        expected_sha256,
    })
}

fn sudo_runner_help() -> &'static str {
    "Usage: sase_sudo_runner --manifest|-m PATH --expected-sha256|-e SHA256\n\nReads one reviewed sudo manifest, verifies its canonical SHA-256, and emits one JSON ledger on stdout.\n\nExit statuses:\n  0  completed or command-level failure recorded in the ledger\n  10 authentication failed\n  11 cancelled\n  12 no controlling TTY\n  13 invalid manifest or digest\n  14 runner failure"
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
        if CANCELLED.load(Ordering::SeqCst) {
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
            let manifest_path = tmp.path().join("manifest.json");
            fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap())
                .unwrap();
            let digest = sudo_manifest_sha256(&manifest).unwrap();
            let config = SudoRunnerConfig::test(sudo_path.clone());
            Self {
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

    fn ledger(stdout: &str) -> SudoLedgerWire {
        serde_json::from_str(stdout.trim()).unwrap()
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
        assert!(String::from_utf8(stdout)
            .unwrap()
            .contains("Usage: sase_sudo_runner"));
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
