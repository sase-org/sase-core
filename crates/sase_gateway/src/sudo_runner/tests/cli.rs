use std::fs::{self};

use sase_core::{
    sudo_manifest_from_json_slice, validate_sudo_exec_started,
    SudoExecStartedWire, SudoLedgerEntryStatusWire, SudoLedgerOutcomeWire,
};

use super::super::*;
use super::support::*;

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
    config.detached_execution = Some(true);
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
fn capabilities_omit_detached_execution_when_identity_backend_is_absent() {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut config = SudoRunnerConfig::production();
    config.tty_available = Some(false);
    config.detached_execution = Some(false);
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
            "capabilities": []
        })
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_identity_backend_advertises_detached_execution() {
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
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        value["capabilities"],
        serde_json::json!(["detached_execution"])
    );
    assert!(platform_process_identity_available());
    assert!(detached_execution_supported(&config));
}

#[cfg(not(target_os = "linux"))]
#[test]
fn non_linux_identity_backend_reports_explicit_unsupported_error() {
    let error = process_identity_token(std::process::id()).unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error.message().contains("require Linux procfs"));
    assert!(error.message().contains("unavailable on this platform"));
    assert!(!platform_process_identity_available());
    let mut config = SudoRunnerConfig::production();
    config.tty_available = Some(false);
    assert!(!detached_execution_supported(&config));
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_sudo_runner_cli_with_io(
        ["--capabilities".to_string()],
        &config,
        &mut stdout,
        &mut stderr,
    );
    assert!(result.is_ok());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schema_version": 1,
            "capabilities": []
        })
    );
}

#[test]
fn detach_rejects_before_auth_when_capability_absent() {
    let mut fixture = Fixture::new(manifest());
    fixture.config.detached_execution = Some(false);
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
        .contains("unsupported"));
    assert!(ledger
        .entries
        .iter()
        .all(|entry| entry.status == SudoLedgerEntryStatusWire::Skipped));
    sase_core::sudo_validate_ledger_json_value(
        &serde_json::from_str(stdout.trim()).unwrap(),
        Some(
            &serde_json::from_slice(&fs::read(&fixture.manifest_path).unwrap())
                .unwrap(),
        ),
    )
    .unwrap();
    assert!(fixture.calls().is_empty());
    assert!(!fixture.handoff_dir.join(STARTED_FILENAME).exists());
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
fn native_launcher_places_internal_modes_directly_after_executable() {
    let fixture = Fixture::new(manifest());
    let (result, _, _) = fixture.run_detach();
    assert!(result.is_ok(), "{result:?}");
    let relaunch = fixture.exec_relaunch_argv();
    assert_eq!(
        &relaunch[..2],
        &[
            fixture.runner_program().display().to_string(),
            INTERNAL_ROOT_EXEC_FLAG.to_string(),
        ]
    );

    let (worker_argv, result) = run_waiting_worker_exec(Vec::new());
    assert!(
        !worker_argv.is_empty(),
        "worker did not record argv: {result:?}"
    );
    assert_eq!(
        worker_argv.first().map(String::as_str),
        Some(INTERNAL_ROOT_WORKER_FLAG)
    );
}

#[test]
fn python_hosted_launcher_preserves_isolated_module_prefix() {
    let mut fixture = Fixture::new(manifest());
    fixture.with_python_hosted_launcher();
    let (result, stdout, _) = fixture.run_detach();
    assert!(result.is_ok(), "{result:?}");
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
    let relaunch = fixture.exec_relaunch_argv();
    let program = fixture.runner_program().display().to_string();
    let mut expected = vec![program];
    expected.extend(python_hosted_prefix());
    expected.push(INTERNAL_ROOT_EXEC_FLAG.to_string());
    assert_eq!(&relaunch[..expected.len()], expected.as_slice());

    let (worker_argv, result) = run_waiting_worker_exec(python_hosted_prefix());
    assert!(
        worker_argv.len() >= 4,
        "worker did not record Python-hosted argv: {result:?}"
    );
    let mut expected_worker = python_hosted_prefix();
    expected_worker.push(INTERNAL_ROOT_WORKER_FLAG.to_string());
    assert_eq!(
        &worker_argv[..expected_worker.len()],
        expected_worker.as_slice()
    );
}

#[test]
fn python_hosted_entry_rejects_non_absolute_interpreter() {
    let error =
        run_python_hosted_sudo_runner_cli("python", ["--help".to_string()])
            .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error.message().contains("absolute path"));

    let error = run_python_hosted_sudo_runner_cli("", ["--help".to_string()])
        .unwrap_err();
    assert_eq!(error.exit_code(), SUDO_RUNNER_RUNNER_ERROR_EXIT);
    assert!(error.message().contains("absolute path"));
}

#[test]
fn parse_rejects_launcher_override_arguments() {
    let ParseResult::Error(message) = parse_sudo_runner_args([
        "--runner-program".to_string(),
        "/usr/bin/python".to_string(),
    ]) else {
        panic!("expected unknown-argument error");
    };
    assert!(message.contains("unknown argument: --runner-program"));
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
