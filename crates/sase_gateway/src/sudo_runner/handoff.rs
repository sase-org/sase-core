use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sase_core::{
    sudo_exec_started_from_json_value, sudo_manifest_from_json_slice,
    sudo_manifest_sha256, validate_sudo_exec_started, SudoExecStartedWire,
    SudoManifestWire, SudoWireError, SUDO_EXEC_STARTED_KIND,
    SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION, SUDO_MANIFEST_MAX_BYTES,
};

use super::*;

pub(crate) fn read_bounded_manifest(path: &Path) -> Result<Vec<u8>, String> {
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
pub(crate) struct HandoffPaths {
    pub(crate) dir: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) ledger_path: PathBuf,
    pub(crate) log_path: PathBuf,
    pub(crate) started_path: PathBuf,
    pub(crate) stop_path: PathBuf,
}

pub(crate) fn validate_handoff_paths(
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

/// Compare a caller-supplied `--started-path` against the canonical handoff
/// path derived from `--detach-dir`.
///
/// Both sides are canonicalized before comparing: `HandoffPaths::started_path`
/// is built from the canonicalized detach dir, while the caller builds
/// `--started-path` by joining the non-canonical detach dir it was given. A
/// verbatim comparison therefore never matches when the handoff directory has
/// a symlinked ancestor (every macOS temp dir: `/tmp` -> `/private/tmp`),
/// which broke the detached handshake on those hosts. `--started-path` may
/// not exist yet, so the parent is canonicalized and the file name rejoined
/// rather than canonicalizing the full path.
///
/// The security intent is preserved: the file name must be the handshake file
/// and the resolved parent must be the handoff directory, so a started-path
/// outside the detach dir is still rejected. All file operations below use
/// the canonical `HandoffPaths::started_path`, never the caller-supplied
/// value.
pub(crate) fn started_path_matches_handoff(
    started_path: &Path,
    paths: &HandoffPaths,
) -> bool {
    let Some(file_name) = started_path.file_name() else {
        return false;
    };
    if file_name != STARTED_FILENAME {
        return false;
    }
    let Some(parent) = started_path.parent() else {
        return false;
    };
    match parent.canonicalize() {
        Ok(canonical_parent) => canonical_parent == paths.dir,
        Err(_) => false,
    }
}

pub(crate) fn read_bounded_manifest_nofollow(
    path: &Path,
) -> Result<Vec<u8>, String> {
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

pub(crate) fn load_verified_detach_manifest(
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

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StdioMode {
    Inherit,
    Null,
}

#[cfg(unix)]
pub(crate) fn sudo_status<const N: usize>(
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
pub(crate) fn sudo_command(
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
pub(crate) fn spawn_internal_root_executor(
    config: &SudoRunnerConfig,
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
) -> Result<(), SudoRunnerCliError> {
    reject_existing_path(&paths.started_path, "started handshake")?;
    let launcher = runner_launcher(config)?;
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mut command = sudo_command(config, manifest);
    command.arg("-n").arg("-u").arg("root").arg("--");
    launcher.append_invocation(&mut command, INTERNAL_ROOT_EXEC_FLAG);
    command
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
pub(crate) struct WorkerHandshakeExpectation {
    pub(crate) pid: u32,
    pub(crate) manifest_sha256: String,
}

#[cfg(unix)]
pub(crate) fn wait_for_started_handshake(
    paths: &HandoffPaths,
    manifest: &SudoManifestWire,
    timeout: Duration,
    expected: Option<WorkerHandshakeExpectation>,
    config: &SudoRunnerConfig,
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
                if let Some(expected) = expected.as_ref() {
                    validate_worker_started_handshake(
                        &handshake, expected, config,
                    )?;
                }
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
            let message = if expected.is_some() {
                "timed out waiting for sudo started handshake"
            } else {
                "timed out waiting for sudo root executor handshake"
            };
            return Err(cli_error(
                SudoRunnerExitStatus::RunnerError,
                message.to_string(),
            ));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
pub(crate) fn validate_worker_started_handshake(
    handshake: &SudoExecStartedWire,
    expected: &WorkerHandshakeExpectation,
    config: &SudoRunnerConfig,
) -> Result<(), SudoRunnerCliError> {
    if handshake.executor_pid != expected.pid {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "sudo started handshake executor_pid does not match worker"
                .to_string(),
        ));
    }
    if !constant_time_eq(
        handshake.manifest_sha256.as_bytes(),
        expected.manifest_sha256.as_bytes(),
    ) {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "sudo started handshake manifest digest does not match worker"
                .to_string(),
        ));
    }
    let identity = match &config.process_identity_override {
        Some(token) => token.clone(),
        None => process_identity_token(expected.pid)?,
    };
    if handshake.executor_identity != identity {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "sudo started handshake executor identity does not match worker"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn publish_worker_started_handshake(
    config: &SudoRunnerConfig,
    child: &Child,
    manifest: &SudoManifestWire,
    manifest_sha256: &str,
    paths: &HandoffPaths,
    parent_uid: u32,
    parent_gid: u32,
) -> Result<(), SudoRunnerCliError> {
    let executor_pid = child.id();
    let executor_identity = derive_executor_identity(config, executor_pid)?;
    if let Some(message) = &config.started_publish_error {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            message.clone(),
        ));
    }
    let handshake = SudoExecStartedWire {
        schema_version: SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION,
        kind: SUDO_EXEC_STARTED_KIND.to_string(),
        manifest_sha256: manifest_sha256.to_string(),
        executor_pid,
        executor_identity,
        ledger_path: path_string(&paths.ledger_path)?,
        log_path: path_string(&paths.log_path)?,
        started_at: current_unix_time()?,
    };
    validate_sudo_exec_started(&handshake, Some(manifest)).map_err(
        |error| cli_error(SudoRunnerExitStatus::RunnerError, error.message),
    )?;
    write_atomic_json_for_user(
        &paths.started_path,
        &handshake,
        parent_uid,
        parent_gid,
    )
}

#[cfg(unix)]
pub(crate) fn derive_executor_identity(
    config: &SudoRunnerConfig,
    pid: u32,
) -> Result<String, SudoRunnerCliError> {
    if let Some(message) = &config.process_identity_error {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            message.clone(),
        ));
    }
    if let Some(token) = &config.process_identity_override {
        return Ok(token.clone());
    }
    process_identity_token(pid)
}

#[cfg(unix)]
pub(crate) fn terminate_and_reap_worker(child: &mut Child) {
    let pid = child.id() as libc::pid_t;
    let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
    terminate_child_group(child);
    let _ = child.wait();
}

#[cfg(unix)]
pub(crate) fn validate_handshake_paths(
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
