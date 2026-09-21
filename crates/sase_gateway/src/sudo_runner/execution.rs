use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::collections::VecDeque;
#[cfg(unix)]
use std::sync::{Arc, Mutex};

use sase_core::{
    SudoLedgerEntryStatusWire, SudoLedgerEntryWire, SudoLedgerOutcomeWire,
    SudoLedgerWire, SudoManifestWire, SudoOutputPolicyWire,
    SUDO_MAX_OUTPUT_TAIL_BYTES,
};

use super::*;

#[cfg(unix)]
pub(crate) struct CommandResult {
    pub(crate) entry: SudoLedgerEntryWire,
    pub(crate) outcome: CommandOutcome,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandOutcome {
    Ok,
    Failed,
    TimedOut,
    Cancelled,
}

#[cfg(unix)]
pub(crate) fn run_internal_root_worker_loaded(
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
    parent_uid: u32,
    parent_gid: u32,
) -> Result<(), SudoRunnerCliError> {
    reject_existing_path(&paths.ledger_path, "ledger")?;
    reject_symlink_path(&paths.log_path, "log")?;
    let log = open_log_file(&paths.log_path, parent_uid, parent_gid)?;
    let log = Arc::new(Mutex::new(log));
    let ledger = execute_detached_worker_manifest(
        manifest,
        manifest_sha256,
        paths,
        &log,
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
pub(crate) fn execute_detached_worker_manifest(
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
    log: &Arc<Mutex<File>>,
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
        write_log_line(
            log,
            &format!("sase_sudo_runner: running {}", command.id),
        )?;
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
pub(crate) fn write_log_line(
    log: &Arc<Mutex<File>>,
    line: &str,
) -> Result<(), SudoRunnerCliError> {
    let mut log = log.lock().unwrap_or_else(|error| error.into_inner());
    writeln!(log, "{line}").map_err(internal_io)?;
    log.flush().map_err(internal_io)
}

#[cfg(unix)]
pub(crate) fn run_direct_command(
    manifest: &SudoManifestWire,
    index: usize,
    account: &ResolvedAccount,
    stop_path: &Path,
    log: &Arc<Mutex<File>>,
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
    let output = Arc::new(SharedCommandOutput::new(
        Arc::clone(log),
        manifest.output_to_agent,
    ));
    let child_done = Arc::new(AtomicBool::new(false));
    let stdout_reader = child.stdout.take().map(|reader| {
        spawn_output_drainer(
            reader,
            Arc::clone(&output),
            Arc::clone(&child_done),
        )
    });
    let stderr_reader = child.stderr.take().map(|reader| {
        spawn_output_drainer(
            reader,
            Arc::clone(&output),
            Arc::clone(&child_done),
        )
    });
    let wait = wait_child_bounded_with_cancel(&mut child, timeout, || {
        CANCELLED.load(Ordering::SeqCst) || stop_path.exists()
    })?;
    child_done.store(true, Ordering::SeqCst);
    let duration_seconds = started.elapsed().as_secs_f64();
    let join_deadline = Instant::now() + DRAIN_JOIN_TIMEOUT;
    join_drainer(stdout_reader, join_deadline)?;
    join_drainer(stderr_reader, join_deadline)?;
    let output_tail =
        ledger_output(&output.tail_bytes(), manifest.output_to_agent);
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
pub(crate) fn run_approved_command(
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
pub(crate) fn read_pipe_in_thread<R>(
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
pub(crate) fn join_reader(
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
pub(crate) struct BoundedByteTail {
    pub(crate) max_bytes: usize,
    pub(crate) data: VecDeque<u8>,
}

#[cfg(unix)]
impl BoundedByteTail {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            data: VecDeque::new(),
        }
    }

    pub(crate) fn extend(&mut self, chunk: &[u8]) {
        if self.max_bytes == 0 || chunk.is_empty() {
            return;
        }
        if chunk.len() >= self.max_bytes {
            self.data.clear();
            self.data
                .extend(chunk[chunk.len() - self.max_bytes..].iter().copied());
            return;
        }
        let overflow = self.data.len() + chunk.len();
        if overflow > self.max_bytes {
            let drop = overflow - self.max_bytes;
            let _ = self.data.drain(..drop);
        }
        self.data.extend(chunk.iter().copied());
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.data.iter().copied().collect()
    }
}

#[cfg(unix)]
pub(crate) struct SharedCommandOutput {
    pub(crate) log: Arc<Mutex<File>>,
    pub(crate) tail: Mutex<BoundedByteTail>,
}

#[cfg(unix)]
impl SharedCommandOutput {
    pub(crate) fn new(
        log: Arc<Mutex<File>>,
        policy: SudoOutputPolicyWire,
    ) -> Self {
        Self {
            log,
            tail: Mutex::new(BoundedByteTail::new(ledger_tail_bound(policy))),
        }
    }

    pub(crate) fn write_chunk(&self, chunk: &[u8]) -> io::Result<()> {
        {
            let mut log =
                self.log.lock().unwrap_or_else(|error| error.into_inner());
            log.write_all(chunk)?;
            log.flush()?;
        }
        let mut tail =
            self.tail.lock().unwrap_or_else(|error| error.into_inner());
        tail.extend(chunk);
        Ok(())
    }

    pub(crate) fn tail_bytes(&self) -> Vec<u8> {
        self.tail
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot()
    }
}

pub(crate) fn ledger_tail_bound(policy: SudoOutputPolicyWire) -> usize {
    match policy {
        SudoOutputPolicyWire::None => 0,
        SudoOutputPolicyWire::Tail => {
            SUDO_MAX_OUTPUT_TAIL_BYTES.min(LEDGER_TAIL_POLICY_BYTES)
        }
        SudoOutputPolicyWire::Full => SUDO_MAX_OUTPUT_TAIL_BYTES,
    }
}

#[cfg(unix)]
pub(crate) fn spawn_output_drainer<R>(
    mut reader: R,
    output: Arc<SharedCommandOutput>,
    child_done: Arc<AtomicBool>,
) -> thread::JoinHandle<io::Result<()>>
where
    R: Read + std::os::unix::io::AsRawFd + Send + 'static,
{
    thread::spawn(move || {
        set_nonblocking_fd(reader.as_raw_fd())?;
        let mut buf = [0u8; OUTPUT_DRAIN_CHUNK];
        let mut idle_since = None;
        loop {
            match reader.read(&mut buf) {
                Ok(0) => return Ok(()),
                Ok(n) => {
                    idle_since = None;
                    output.write_chunk(&buf[..n])?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if child_done.load(Ordering::SeqCst) {
                        let started =
                            *idle_since.get_or_insert_with(Instant::now);
                        if started.elapsed() >= DRAIN_GRACE {
                            return Ok(());
                        }
                    }
                    thread::sleep(POLL_INTERVAL);
                }
                Err(error) => return Err(error),
            }
        }
    })
}

#[cfg(unix)]
pub(crate) fn set_nonblocking_fd(fd: libc::c_int) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn join_drainer(
    reader: Option<thread::JoinHandle<io::Result<()>>>,
    deadline: Instant,
) -> Result<(), SudoRunnerCliError> {
    let Some(reader) = reader else {
        return Ok(());
    };
    loop {
        if reader.is_finished() {
            return reader
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
                });
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
pub(crate) enum WaitOutcome {
    Exited(ExitStatus),
    TimedOut,
    Cancelled,
}

#[cfg(unix)]
pub(crate) fn wait_child_bounded(
    child: &mut Child,
    timeout: Duration,
) -> Result<WaitOutcome, SudoRunnerCliError> {
    wait_child_bounded_with_cancel(child, timeout, || {
        CANCELLED.load(Ordering::SeqCst)
    })
}

#[cfg(unix)]
pub(crate) fn wait_child_bounded_with_cancel(
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
pub(crate) fn configure_process_group(command: &mut Command) {
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
pub(crate) fn configure_new_session(command: &mut Command) {
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
