use std::fs::{self};
use std::path::PathBuf;

use sase_core::{
    sudo_manifest_from_json_slice, validate_sudo_exec_started, SudoCommandWire,
    SudoExecStartedWire, SudoLedgerEntryStatusWire, SudoLedgerOutcomeWire,
    SUDO_EXEC_STARTED_KIND,
};

use super::super::*;
use super::support::*;
use std::os::unix::fs::PermissionsExt;

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
    // The stub reports the observed cwd via `pwd`, and the OS resolves
    // symlinked ancestors at `chdir` time, so on macOS this is
    // `/private/tmp` rather than the caller-supplied `/tmp`.
    // `manifest.cwd` is an instruction for where to run, not a value
    // echoed back verbatim, so the expectation canonicalizes.
    let expected_cwd =
        fs::canonicalize("/tmp").unwrap_or_else(|_| PathBuf::from("/tmp"));
    assert_eq!(fixture.cwd_log()[3], expected_cwd.display().to_string());
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
    // Same canonical-cwd contract as above: under a temp root with a
    // symlinked ancestor the stub's `pwd` reports the resolved path.
    let expected_cwd = fs::canonicalize(&cwd).unwrap_or_else(|_| cwd.clone());
    assert_eq!(fixture.cwd_log()[3], expected_cwd.display().to_string());
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
    assert_eq!(ledger.entries[1].status, SudoLedgerEntryStatusWire::Skipped);
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
    assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Skipped);
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
    assert!(!fixture.handoff_dir.join(STARTED_FILENAME).exists());
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
    // Production derives handshake paths from the canonicalized detach
    // dir, so the expectation canonicalizes the fixture dir rather than
    // comparing against its non-canonical spelling.
    let canonical_handoff = fs::canonicalize(&fixture.handoff_dir)
        .unwrap_or_else(|_| fixture.handoff_dir.clone());
    assert_eq!(
        handshake.ledger_path,
        canonical_handoff
            .join(LEDGER_FILENAME)
            .display()
            .to_string()
    );
    assert_eq!(
        handshake.log_path,
        canonical_handoff.join(LOG_FILENAME).display().to_string()
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
    let relaunch = fixture.exec_relaunch_argv();
    assert_eq!(
        relaunch.first().map(String::as_str),
        Some(fixture.runner_program().to_str().unwrap())
    );
    assert_eq!(
        relaunch.get(1).map(String::as_str),
        Some(INTERNAL_ROOT_EXEC_FLAG)
    );
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
    assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Skipped);
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
    assert_eq!(ledger.entries[0].status, SudoLedgerEntryStatusWire::Skipped);
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
