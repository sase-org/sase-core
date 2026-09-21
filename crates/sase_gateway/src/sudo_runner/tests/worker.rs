use std::fs::{self};
use std::io::{self};
use std::thread;
use std::time::{Duration, Instant};

use sase_core::{
    sudo_manifest_from_json_slice, validate_sudo_exec_started,
    validate_sudo_ledger, SudoCommandWire, SudoExecStartedWire,
    SudoLedgerEntryStatusWire, SudoLedgerOutcomeWire, SudoLedgerWire,
    SudoOutputPolicyWire, SUDO_MAX_OUTPUT_TAIL_BYTES,
};

use super::super::*;
use super::support::*;
use std::os::unix::fs::PermissionsExt;

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
    assert_eq!(ledger.entries[1].status, SudoLedgerEntryStatusWire::Skipped);
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
    assert!(ledger.entries[0].output_tail.len() <= SUDO_MAX_OUTPUT_TAIL_BYTES);
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
    write_self_started(tmp.path(), &digest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
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
    write_self_started(tmp.path(), &digest);
    let stop_path = tmp.path().join(STOP_FILENAME);
    let stop_thread = thread::spawn({
        let stop_path = stop_path.clone();
        move || {
            thread::sleep(Duration::from_millis(75));
            fs::write(stop_path, "").unwrap();
        }
    });
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
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
    write_self_started(tmp.path(), &digest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
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
    write_self_started(tmp.path(), &digest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
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
        worker_cli_args(&manifest_path, "a".repeat(64), tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_INVALID_INPUT_EXIT);
    assert!(!tmp.path().join(LEDGER_FILENAME).exists());
}

#[test]
fn internal_worker_does_not_execute_before_valid_witness() {
    let tmp = tempfile::tempdir().unwrap();
    let ran_path = tmp.path().join("ran");
    let command_path = tmp.path().join("touch-ran.sh");
    write_executable(
        &command_path,
        &format!("#!/bin/sh\nprintf ran > '{}'\n", ran_path.display()),
    );
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands = vec![SudoCommandWire {
        id: "touch".to_string(),
        argv: vec![command_path.display().to_string()],
        why: "Record execution".to_string(),
        timeout_seconds: Some(2.0),
        shell: false,
    }];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.started_sentinel_timeout = Duration::from_millis(200);
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest.clone(), tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error.message().contains("timed out waiting"));
    assert!(!ran_path.exists());
    assert!(!tmp.path().join(LOG_FILENAME).exists());
    assert!(!tmp.path().join(LEDGER_FILENAME).exists());

    write_self_started(tmp.path(), &digest);
    let result = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    );
    assert!(result.is_ok(), "{result:?}");
    assert!(ran_path.exists());
}

#[test]
fn internal_worker_rejects_mismatched_witness_without_executing() {
    let tmp = tempfile::tempdir().unwrap();
    let ran_path = tmp.path().join("ran");
    let command_path = tmp.path().join("touch-ran.sh");
    write_executable(
        &command_path,
        &format!("#!/bin/sh\nprintf ran > '{}'\n", ran_path.display()),
    );
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands = vec![SudoCommandWire {
        id: "touch".to_string(),
        argv: vec![command_path.display().to_string()],
        why: "Record execution".to_string(),
        timeout_seconds: Some(2.0),
        shell: false,
    }];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    let mut handshake = write_self_started(tmp.path(), &digest);
    handshake.executor_pid = handshake.executor_pid.saturating_sub(1).max(1);
    if handshake.executor_pid == std::process::id() {
        handshake.executor_pid = 1;
    }
    fs::write(
        tmp.path().join(STARTED_FILENAME),
        format!("{}\n", serde_json::to_string(&handshake).unwrap()),
    )
    .unwrap();
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error.message().contains("executor_pid"));
    assert!(!ran_path.exists());
    assert!(!tmp.path().join(LOG_FILENAME).exists());
}

#[test]
fn internal_modes_reject_started_path_outside_handoff_dir() {
    // The started-path check exists so an internal worker cannot be
    // pointed at a handshake outside its handoff directory. The match
    // canonicalizes both sides (see `started_path_matches_handoff`), so
    // a path that resolves elsewhere must still be refused on every
    // platform, symlinked temp ancestors or not.
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let outside_started = outside.path().join(STARTED_FILENAME);
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands.truncate(1);
    manifest.commands[0].argv = vec!["/bin/true".to_string()];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    // The exec mode gates on detached-execution support before reaching
    // the started-path check; force it so both modes are exercised.
    config.detached_execution = Some(true);
    for flag in [INTERNAL_ROOT_WORKER_FLAG, INTERNAL_ROOT_EXEC_FLAG] {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = run_sudo_runner_cli_with_io(
            [
                flag.to_string(),
                "-m".to_string(),
                manifest_path.display().to_string(),
                "-e".to_string(),
                digest.clone(),
                "-d".to_string(),
                tmp.path().display().to_string(),
                "--started-path".to_string(),
                outside_started.display().to_string(),
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
        assert!(
            error.message().contains("must match the handoff directory"),
            "unexpected message for {flag}: {}",
            error.message()
        );
    }
    assert!(!outside_started.exists());
    assert!(!tmp.path().join(STARTED_FILENAME).exists());
}

#[test]
fn post_spawn_identity_failure_reaps_barred_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands.truncate(1);
    manifest.commands[0].argv = vec!["/bin/true".to_string()];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    let worker_path = tmp.path().join("waiting-worker");
    write_executable(&worker_path, waiting_worker_script());
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.launcher = Some(SudoRunnerLauncher {
        program: worker_path,
        prefix: Vec::new(),
    });
    config.detached_execution = Some(true);
    config.process_identity_error =
        Some("forced identity derivation failure".to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = run_sudo_runner_cli_with_io(
        exec_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error
        .message()
        .contains("forced identity derivation failure"));
    assert!(!tmp.path().join(STARTED_FILENAME).exists());
    assert!(!tmp.path().join("worker.ran").exists());
    if let Ok(pid) = fs::read_to_string(tmp.path().join("worker.pid")) {
        let pid: u32 = pid.trim().parse().unwrap();
        assert!(!process_alive(pid), "barred worker {pid} still running");
    }
}

#[test]
fn post_spawn_publish_failure_reaps_barred_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands.truncate(1);
    manifest.commands[0].argv = vec!["/bin/true".to_string()];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    let worker_path = tmp.path().join("waiting-worker");
    write_executable(&worker_path, waiting_worker_script());
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.launcher = Some(SudoRunnerLauncher {
        program: worker_path,
        prefix: Vec::new(),
    });
    config.detached_execution = Some(true);
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    config.started_publish_error =
        Some("forced started handshake publish failure".to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = run_sudo_runner_cli_with_io(
        exec_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error
        .message()
        .contains("forced started handshake publish failure"));
    assert!(!tmp.path().join(STARTED_FILENAME).exists());
    assert!(!tmp.path().join("worker.ran").exists());
    if let Ok(pid) = fs::read_to_string(tmp.path().join("worker.pid")) {
        let pid: u32 = pid.trim().parse().unwrap();
        assert!(!process_alive(pid), "barred worker {pid} still running");
    }
}

#[test]
fn detached_timestamp_cleanup_failure_returns_started_handshake() {
    let fixture = Fixture::new(manifest());
    fixture.touch("final_k_fail");
    let (result, stdout, stderr) = fixture.run_detach();
    assert!(result.is_ok(), "{result:?}");
    assert!(stderr.contains(
        "sudo timestamp cleanup failed after detached executor started"
    ));
    let handshake: SudoExecStartedWire =
        serde_json::from_str(stdout.trim()).unwrap();
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
    assert!(fixture.handoff_dir.join(STARTED_FILENAME).exists());
    assert_eq!(
        serde_json::from_slice::<SudoExecStartedWire>(
            &fs::read(fixture.handoff_dir.join(STARTED_FILENAME)).unwrap()
        )
        .unwrap()
        .executor_pid,
        handshake.executor_pid
    );
}

#[test]
fn internal_worker_streams_output_before_command_exit() {
    let tmp = tempfile::tempdir().unwrap();
    let command_path = tmp.path().join("slow-emit.sh");
    write_executable(
        &command_path,
        "#!/bin/sh\nprintf 'early-output\\n'\nexec /bin/sleep 2\n",
    );
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands = vec![SudoCommandWire {
        id: "emit".to_string(),
        argv: vec![command_path.display().to_string()],
        why: "Emit then sleep".to_string(),
        timeout_seconds: Some(5.0),
        shell: false,
    }];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    write_self_started(tmp.path(), &digest);
    let log_path = tmp.path().join(LOG_FILENAME);
    let worker = thread::spawn({
        let manifest_path = manifest_path.clone();
        let digest = digest.clone();
        let dir = tmp.path().to_path_buf();
        move || {
            let mut config = SudoRunnerConfig::test(dir.join("unused-sudo"));
            config.process_identity_override =
                Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            run_sudo_runner_cli_with_io(
                worker_cli_args(&manifest_path, digest, &dir),
                &config,
                &mut stdout,
                &mut stderr,
            )
        }
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if log_path.exists()
            && fs::read_to_string(&log_path)
                .unwrap_or_default()
                .contains("early-output")
        {
            break;
        }
        assert!(
            !worker.is_finished(),
            "worker exited before live output was visible"
        );
        assert!(
            Instant::now() < deadline,
            "timed out waiting for live output.log"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !worker.is_finished(),
        "output.log became visible only after command exit"
    );
    let result = worker.join().unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn internal_worker_keeps_bounded_ledger_tail_for_large_output() {
    let tmp = tempfile::tempdir().unwrap();
    let command_path = tmp.path().join("bulk.sh");
    // Absolute tool paths, split across directories on purpose: macOS
    // keeps dd in /bin while tr lives in /usr/bin; Linux usrmerge
    // provides both locations.
    write_executable(
        &command_path,
        "#!/bin/sh\n\
/bin/dd if=/dev/zero bs=1024 count=200 status=none | /usr/bin/tr '\\0' 'x'\n\
/bin/dd if=/dev/zero bs=1024 count=200 status=none | /usr/bin/tr '\\0' 'y' >&2\n",
    );
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.output_to_agent = SudoOutputPolicyWire::Tail;
    manifest.commands = vec![SudoCommandWire {
        id: "bulk".to_string(),
        argv: vec![command_path.display().to_string()],
        why: "Emit large stdout and stderr".to_string(),
        timeout_seconds: Some(5.0),
        shell: false,
    }];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    write_self_started(tmp.path(), &digest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    );
    assert!(result.is_ok(), "{result:?}");
    let ledger: SudoLedgerWire = serde_json::from_slice(
        &fs::read(tmp.path().join(LEDGER_FILENAME)).unwrap(),
    )
    .unwrap();
    validate_sudo_ledger(&ledger, Some(&manifest)).unwrap();
    assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Ran);
    assert!(ledger.entries[0].output_tail.len() <= LEDGER_TAIL_POLICY_BYTES);
    let log_len = fs::metadata(tmp.path().join(LOG_FILENAME)).unwrap().len();
    assert!(
        log_len > LEDGER_TAIL_POLICY_BYTES as u64 * 2,
        "expected full output.log, got {log_len} bytes"
    );
}

#[test]
fn internal_worker_timeout_completes_when_descendant_holds_pipe() {
    let tmp = tempfile::tempdir().unwrap();
    let command_path = tmp.path().join("hold-pipe.sh");
    // Absolute tool paths: this stub runs with a cleared environment (no
    // PATH), so `command -v` cannot find anything and every tool is
    // probed by absolute path. setsid(1) detaches the pipe-holder into a
    // new session so it survives the timeout's process-group kill; it
    // only exists on Linux (/usr/bin, mirrored at /bin by usrmerge).
    // Elsewhere the holder stays in the command's group and dies with
    // it, so only the timeout path itself is exercised there.
    //
    // The observable output goes first, before any fork/exec in the stub:
    // under a parallel-test fork storm the child can wait ~1s before its
    // first line runs, so anything that must precede the timeout kill
    // cannot sit behind stub setup. The timeout keeps headroom over that
    // scheduling tail; the elapsed bound below is sanity only (joining a
    // drainer is capped by DRAIN_JOIN_TIMEOUT either way).
    write_executable(
        &command_path,
        "#!/bin/sh\n\
printf 'held\\n'\n\
if [ -x /usr/bin/setsid ]; then _SETSID=/usr/bin/setsid; else _SETSID=/bin/setsid; fi\n\
if [ -x \"$_SETSID\" ]; then \"$_SETSID\" /bin/sleep 5 < /dev/null & else /bin/sleep 5 < /dev/null & fi\n\
exec /bin/sleep 30\n",
    );
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands = vec![SudoCommandWire {
        id: "hold".to_string(),
        argv: vec![command_path.display().to_string()],
        why: "Leave a descendant holding the pipe".to_string(),
        timeout_seconds: Some(2.0),
        shell: false,
    }];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    write_self_started(tmp.path(), &digest);
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let started = Instant::now();
    let result = run_sudo_runner_cli_with_io(
        worker_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    );
    assert!(result.is_ok(), "{result:?}");
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "timeout hung joining drainers: {:?}",
        started.elapsed()
    );
    let ledger: SudoLedgerWire = serde_json::from_slice(
        &fs::read(tmp.path().join(LEDGER_FILENAME)).unwrap(),
    )
    .unwrap();
    assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Failed);
    assert_eq!(ledger.entries[0].exit_code, None);
    let log = fs::read_to_string(tmp.path().join(LOG_FILENAME)).unwrap();
    assert!(log.contains("held"));
}

#[test]
fn initgroups_base_group_accepts_current_platform_gid() {
    assert_eq!(initgroups_base_group(0).unwrap() as i64, 0);
}

#[test]
fn initgroups_c_int_conversion_rejects_too_large_gid() {
    let too_large = libc::c_int::MAX as u64 + 1;
    let Ok(gid) = libc::gid_t::try_from(too_large) else {
        return;
    };

    let error = gid_to_c_int_for_initgroups(gid).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error
        .to_string()
        .contains("does not fit initgroups base group type"));
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
