use std::io::{self, Write};
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::Ordering;

use sase_core::{
    sudo_manifest_from_json_slice, sudo_manifest_sha256, validate_sudo_ledger,
    SudoLedgerEntryStatusWire, SudoLedgerEntryWire, SudoLedgerOutcomeWire,
    SudoLedgerWire, SudoManifestWire, SudoWireError,
    SUDO_LEDGER_WIRE_SCHEMA_VERSION, SUDO_MAX_COMMANDS,
};

use super::*;

/// Run the reviewed sudo-runner CLI as the native Rust binary.
///
/// Detached execution relaunches this process via `std::env::current_exe()`
/// with an empty prefix: `sudo ... -- <current_exe> --internal-root-exec
/// ...`, then `<current_exe> --internal-root-worker ...`.
pub fn run_sudo_runner_cli(
    args: impl IntoIterator<Item = String>,
) -> Result<(), SudoRunnerCliError> {
    run_sudo_runner_cli_with_config(args, SudoRunnerConfig::production())
}

/// Run the reviewed sudo-runner CLI when hosted by the `sase-core-rs` PyO3
/// module.
///
/// `python_executable` must be the absolute path of the active interpreter
/// (`sys.executable`). Detached hops relaunch as
/// `<python_executable> -I -m sase_core_rs.sudo_runner` followed by the
/// private internal mode argument. Isolated mode is required so a lookalike
/// package in the reviewed working directory, `PYTHONPATH`, or the user site
/// cannot be imported.
pub fn run_python_hosted_sudo_runner_cli(
    python_executable: impl AsRef<Path>,
    args: impl IntoIterator<Item = String>,
) -> Result<(), SudoRunnerCliError> {
    let mut config = SudoRunnerConfig::production();
    config.launcher = Some(SudoRunnerLauncher::python_hosted(
        python_executable.as_ref(),
    )?);
    run_sudo_runner_cli_with_config(args, config)
}

pub(crate) fn run_sudo_runner_cli_with_config(
    args: impl IntoIterator<Item = String>,
    config: SudoRunnerConfig,
) -> Result<(), SudoRunnerCliError> {
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    run_sudo_runner_cli_with_io(args, &config, &mut stdout, &mut stderr)
}

pub(crate) fn run_sudo_runner_cli_with_io(
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
        SudoRunnerCli::Capabilities => write_capabilities(config, stdout),
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

pub(crate) fn run_synchronous_manifest(
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

pub(crate) fn write_capabilities(
    config: &SudoRunnerConfig,
    stdout: &mut dyn Write,
) -> Result<(), SudoRunnerCliError> {
    let capabilities = advertised_detached_capabilities(config);
    let document = serde_json::json!({
        "schema_version": 1,
        "capabilities": capabilities,
    });
    serde_json::to_writer(&mut *stdout, &document).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to serialize sudo runner capabilities: {error}"),
        )
    })?;
    writeln!(stdout).map_err(internal_io)
}

pub(crate) fn advertised_detached_capabilities(
    config: &SudoRunnerConfig,
) -> Vec<&'static str> {
    if detached_execution_supported(config) {
        vec![SUDO_RUNNER_CAPABILITY_DETACHED_EXECUTION]
    } else {
        Vec::new()
    }
}

pub(crate) fn detached_execution_supported(config: &SudoRunnerConfig) -> bool {
    if let Some(value) = config.detached_execution {
        return value;
    }
    platform_detached_execution_supported()
}

pub(crate) fn platform_detached_execution_supported() -> bool {
    cfg!(unix) && platform_process_identity_available()
}

pub(crate) fn platform_process_identity_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        process_identity_token(std::process::id()).is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

pub(crate) fn finish_with_ledger(
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

pub(crate) fn run_detached_manifest(
    cli: SudoRunnerDetachCli,
    config: &SudoRunnerConfig,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), SudoRunnerCliError> {
    let paths = validate_handoff_paths(&cli.detach_dir, &cli.manifest_path)?;
    let (manifest, manifest_sha256) =
        load_verified_detach_manifest(&paths, &cli.expected_sha256)?;

    if !detached_execution_supported(config) {
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

    #[cfg(not(unix))]
    {
        let _ = (config, stderr, paths);
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
            None,
            config,
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

        match sudo_status(config, &manifest, ["-k"], StdioMode::Inherit) {
            Ok(status) if status.success() => {}
            Ok(_) | Err(_) => {
                let _ = writeln!(
                    stderr,
                    "sase_sudo_runner: sudo timestamp cleanup failed after detached executor started"
                );
            }
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

pub(crate) fn run_internal_root_exec(
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
        if !detached_execution_supported(config) {
            return Err(cli_error(
                SudoRunnerExitStatus::RunnerError,
                "detached sudo execution is unsupported on this platform"
                    .to_string(),
            ));
        }
        let paths =
            validate_handoff_paths(&cli.detach_dir, &cli.manifest_path)?;
        let Some(started_path) = &cli.started_path else {
            return Err(cli_error(
                SudoRunnerExitStatus::InvalidInput,
                "--started-path is required for internal root executor"
                    .to_string(),
            ));
        };
        if !started_path_matches_handoff(started_path, &paths) {
            return Err(cli_error(
                SudoRunnerExitStatus::InvalidInput,
                "internal started path must match the handoff directory"
                    .to_string(),
            ));
        }
        let (manifest, manifest_sha256) =
            load_verified_detach_manifest(&paths, &cli.expected_sha256)?;
        reject_existing_path(&paths.started_path, "started handshake")?;
        let launcher = runner_launcher(config)?;
        let mut command =
            launcher.command_for_internal_mode(INTERNAL_ROOT_WORKER_FLAG);
        command
            .arg("--manifest")
            .arg(&paths.manifest_path)
            .arg("--expected-sha256")
            .arg(&manifest_sha256)
            .arg("--detach-dir")
            .arg(&paths.dir)
            .arg("--started-path")
            .arg(&paths.started_path)
            .arg("--parent-uid")
            .arg(cli.parent_uid.to_string())
            .arg("--parent-gid")
            .arg(cli.parent_gid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_new_session(&mut command);
        let mut child = command.spawn().map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to spawn sudo root worker: {error}"),
            )
        })?;
        if let Err(error) = publish_worker_started_handshake(
            config,
            &child,
            &manifest,
            &manifest_sha256,
            &paths,
            cli.parent_uid,
            cli.parent_gid,
        ) {
            terminate_and_reap_worker(&mut child);
            return Err(error);
        }
        Ok(())
    }
}

pub(crate) fn run_internal_root_worker(
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
                "--started-path is required for internal root worker"
                    .to_string(),
            ));
        };
        if !started_path_matches_handoff(started_path, &paths) {
            return Err(cli_error(
                SudoRunnerExitStatus::InvalidInput,
                "internal started path must match the handoff directory"
                    .to_string(),
            ));
        }
        let (manifest, manifest_sha256) =
            load_verified_detach_manifest(&paths, &cli.expected_sha256)?;
        wait_for_started_handshake(
            &paths,
            &manifest,
            config.started_sentinel_timeout,
            Some(WorkerHandshakeExpectation {
                pid: std::process::id(),
                manifest_sha256: manifest_sha256.clone(),
            }),
            config,
        )?;
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

pub(crate) fn execute_manifest(
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

pub(crate) fn skipped_ledger(
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

pub(crate) fn write_ledger(
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

pub(crate) fn exit_status_for_ledger(
    ledger: &SudoLedgerWire,
) -> SudoRunnerExitStatus {
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
