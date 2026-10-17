use super::*;
use crate::json_bridge::py_to_json_value;
use sase_core::mobile_pairing::{
    consume_mobile_pairing_challenge_now, MobilePairingConsumeOutcome,
    MobilePairingConsumeRequestWire, MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
};

fn temp_state() -> tempfile::TempDir {
    let tmp = tempfile::Builder::new()
        .prefix("sase-pairing-py-")
        .tempdir_in("/tmp")
        .expect("create pairing binding test dir under /tmp");
    let mut perms = std::fs::metadata(tmp.path()).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o700);
    std::fs::set_permissions(tmp.path(), perms).unwrap();
    tmp
}

#[test]
fn mobile_pairing_issue_binding_returns_challenge() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let tmp = temp_state();
        let request = pyo3::types::PyDict::new_bound(py);
        request.set_item("schema_version", 1).unwrap();
        request.set_item("host_label", "workstation").unwrap();
        request
            .set_item("state_root", tmp.path().to_str().unwrap())
            .unwrap();
        let response = py_mobile_pairing_issue(py, &request).unwrap();
        let value = py_to_json_value(response.bind(py)).unwrap();
        assert_eq!(value["schema_version"], 1);
        let pairing_id = value["pairing_id"].as_str().unwrap().to_string();
        let code = value["code"].as_str().unwrap().to_string();
        assert!(pairing_id.starts_with("pair_"));
        assert_eq!(code.len(), 6);
        assert_eq!(value["host_label"], "workstation");
        assert!(tmp
            .path()
            .join("mobile_gateway")
            .join("pairing")
            .join("challenges.json")
            .is_file());
        let outcome = consume_mobile_pairing_challenge_now(
            &MobilePairingConsumeRequestWire {
                schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
                pairing_id,
                code,
                state_root: Some(tmp.path().to_string_lossy().into_owned()),
            },
        )
        .expect("redeem binding-issued challenge");
        assert_eq!(outcome, MobilePairingConsumeOutcome::Accepted);
    });
}

#[test]
fn mobile_pairing_issue_binding_schema_error_is_secret_free() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = pyo3::types::PyDict::new_bound(py);
        request.set_item("schema_version", 2).unwrap();
        let error = py_mobile_pairing_issue(py, &request).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        let message = error.to_string();
        assert!(message.contains("unsupported pairing schema version"));
        assert!(!message.contains("pair_"));
        assert!(!message.to_lowercase().contains("code"));
    });
}
