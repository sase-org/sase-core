use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};

#[cfg(test)]
fn python_hosted_sudo_runner_prefix() -> Vec<String> {
    sase_gateway::PYTHON_HOSTED_SUDO_RUNNER_PREFIX
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

#[cfg(test)]
fn python_hosted_sudo_runner_launcher(
    py: Python<'_>,
) -> PyResult<(PathBuf, Vec<String>)> {
    Ok((
        python_hosted_sudo_runner_executable(py)?,
        python_hosted_sudo_runner_prefix(),
    ))
}

#[test]
fn sudo_bindings_validate_manifest_risk_ledger_and_help() {
    use serde_json::json;

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let manifest = json!({
            "schema_version": 1,
            "request_id": "sudo-bindings",
            "host": "athena",
            "host_is_remote": true,
            "run_as": "root",
            "cwd": "/tmp",
            "env": {},
            "stop_on_failure": true,
            "output_to_agent": "tail",
            "commands": [
                {
                    "id": "pkg",
                    "argv": ["apt-get", "update"],
                    "why": "Refresh package metadata",
                    "timeout_seconds": 10.0,
                    "shell": false
                },
                {
                    "id": "ssh",
                    "argv": ["systemctl", "restart", "sshd.service"],
                    "why": "Restart ssh",
                    "shell": false
                }
            ],
            "resume_from": null
        });
        let manifest_obj =
            json_value_to_py(py, &manifest).unwrap().into_bound(py);
        let manifest_dict = manifest_obj.downcast::<PyDict>().unwrap();

        let normalized = py_sudo_validate_manifest(py, manifest_dict).unwrap();
        let normalized_value = py_to_json_value(normalized.bind(py)).unwrap();
        assert_eq!(normalized_value["commands"][0]["id"], json!("pkg"));

        let digest = py_sudo_manifest_sha256(py, manifest_dict).unwrap();
        let rust_manifest =
            sase_core::sudo_manifest_from_json_value(&manifest).unwrap();
        assert_eq!(
            digest,
            sase_core::sudo_manifest_sha256(&rust_manifest).unwrap()
        );

        let risks = py_sudo_derive_risk_badges(py, manifest_dict).unwrap();
        let risks = py_to_json_value(risks.bind(py)).unwrap();
        assert_eq!(risks[0]["badges"], json!(["network", "package-manager"]));
        assert_eq!(risks[1]["badges"], json!(["service-restart"]));
        assert_eq!(risks[1]["lockout_prone"], json!(true));

        let ledger = json!({
            "schema_version": 1,
            "request_id": "sudo-bindings",
            "manifest_sha256": digest,
            "outcome": "completed",
            "entries": [
                {
                    "id": "pkg",
                    "status": "ran",
                    "exit_code": 0,
                    "duration_seconds": 0.1,
                    "output_tail": ""
                },
                {
                    "id": "ssh",
                    "status": "skipped",
                    "exit_code": null,
                    "duration_seconds": 0.0,
                    "output_tail": ""
                }
            ],
            "diagnostic": null
        });
        let ledger_obj = json_value_to_py(py, &ledger).unwrap().into_bound(py);
        let ledger_dict = ledger_obj.downcast::<PyDict>().unwrap();
        let validated =
            py_sudo_validate_ledger(py, ledger_dict, Some(manifest_dict))
                .unwrap();
        let validated = py_to_json_value(validated.bind(py)).unwrap();
        assert_eq!(validated["entries"][1]["status"], json!("skipped"));

        let handshake = json!({
            "schema_version": 1,
            "kind": "sudo_exec_started",
            "manifest_sha256": digest,
            "executor_pid": 4321,
            "executor_identity": "boot-a:12345",
            "ledger_path": "/tmp/sase-sudo/ledger.json",
            "log_path": "/tmp/sase-sudo/output.log",
            "started_at": 1_800_000_000.0
        });
        let handshake_obj =
            json_value_to_py(py, &handshake).unwrap().into_bound(py);
        let handshake_dict = handshake_obj.downcast::<PyDict>().unwrap();
        let validated =
            py_sudo_validate_handshake(py, handshake_dict, Some(manifest_dict))
                .unwrap();
        let validated = py_to_json_value(validated.bind(py)).unwrap();
        assert_eq!(validated["executor_pid"], json!(4321));

        let invalid_handshake = json!({
            "schema_version": 1,
            "kind": "sudo_exec_started",
            "manifest_sha256": "b".repeat(64),
            "executor_pid": 4321,
            "executor_identity": "boot-a:12345",
            "ledger_path": "/tmp/sase-sudo/ledger.json",
            "log_path": "/tmp/sase-sudo/output.log",
            "started_at": 1_800_000_000.0
        });
        let invalid_obj = json_value_to_py(py, &invalid_handshake)
            .unwrap()
            .into_bound(py);
        let invalid_dict = invalid_obj.downcast::<PyDict>().unwrap();
        let error =
            py_sudo_validate_handshake(py, invalid_dict, Some(manifest_dict))
                .unwrap_err();
        assert!(error.to_string().contains("SHA-256 mismatch"));

        let facts = json!({
            "finalize_proc_live": false,
            "executor_pid_live": false,
            "executor_identity_matches": false
        });
        let facts_obj = json_value_to_py(py, &facts).unwrap().into_bound(py);
        let facts_dict = facts_obj.downcast::<PyDict>().unwrap();

        let legacy_attempt = json!({
            "schema_version": 1,
            "gate_id": "sudo-bindings",
            "selected_command_ids": ["pkg", "ssh"],
            "manifest_sha256": digest,
            "handoff_dir": "/tmp/sase-sudo/req-1",
            "handshake": handshake.clone(),
            "finalize_proc_id": "proc-1",
            "target_kind": "local",
            "startup_state": "started"
        });
        let legacy_obj = json_value_to_py(py, &legacy_attempt)
            .unwrap()
            .into_bound(py);
        let legacy_dict = legacy_obj.downcast::<PyDict>().unwrap();
        let legacy_decision =
            py_sudo_classify_attempt_liveness(py, legacy_dict, facts_dict)
                .unwrap();
        let legacy_decision =
            py_to_json_value(legacy_decision.bind(py)).unwrap();
        assert_eq!(legacy_decision["classification"], json!("dead"));

        let remote_attempt = json!({
            "schema_version": 1,
            "gate_id": "sudo-bindings",
            "selected_command_ids": ["pkg", "ssh"],
            "manifest_sha256": digest,
            "handoff_dir": "/tmp/sase-sudo/req-1",
            "handshake": handshake.clone(),
            "finalize_proc_id": "proc-1",
            "target_kind": "remote",
            "target_host": "apollo",
            "startup_state": "started",
            "remote_handoff": {
                "directory": "/tmp/sase-sudo/req-1",
                "handshake": "/tmp/sase-sudo/req-1/handshake.json",
                "ledger": "/tmp/sase-sudo/req-1/ledger.json",
                "log": "/tmp/sase-sudo/req-1/output.log",
                "manifest": "/tmp/sase-sudo/req-1/manifest.json",
                "stop": "/tmp/sase-sudo/req-1/stop"
            }
        });
        let remote_obj = json_value_to_py(py, &remote_attempt)
            .unwrap()
            .into_bound(py);
        let remote_dict = remote_obj.downcast::<PyDict>().unwrap();
        let remote_decision =
            py_sudo_classify_attempt_liveness(py, remote_dict, facts_dict)
                .unwrap();
        let remote_decision =
            py_to_json_value(remote_decision.bind(py)).unwrap();
        assert_eq!(remote_decision["classification"], json!("unknown"));

        let mut malformed = remote_attempt;
        malformed["remote_handoff"]["log"] = json!("relative.log");
        let malformed_obj =
            json_value_to_py(py, &malformed).unwrap().into_bound(py);
        let malformed_dict = malformed_obj.downcast::<PyDict>().unwrap();
        let malformed_error =
            py_sudo_classify_attempt_liveness(py, malformed_dict, facts_dict)
                .unwrap_err();
        assert!(malformed_error.to_string().contains("absolute"));

        py_sudo_runner_main(py, vec!["--help".to_string()]).unwrap();
    });
}

#[test]
fn python_hosted_sudo_runner_launcher_uses_isolated_module_execution() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (program, prefix) = python_hosted_sudo_runner_launcher(py).unwrap();
        let sys = py.import_bound("sys").unwrap();
        let expected: String =
            sys.getattr("executable").unwrap().extract().unwrap();
        assert_eq!(program, PathBuf::from(&expected));
        assert!(program.is_absolute());
        assert_eq!(prefix, python_hosted_sudo_runner_prefix());
        assert_eq!(prefix, vec!["-I", "-m", "sase_core_rs.sudo_runner"]);
    });
}

#[test]
fn python_hosted_sudo_runner_launcher_rejects_unusable_executable() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|_py| {
        let error =
            python_hosted_sudo_runner_executable_from_str("").unwrap_err();
        assert!(error.to_string().contains("usable absolute path"));

        let error = python_hosted_sudo_runner_executable_from_str("python")
            .unwrap_err();
        assert!(error.to_string().contains("usable absolute path"));
    });
}
