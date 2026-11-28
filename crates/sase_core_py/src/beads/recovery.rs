//! Recovery activation and claim bindings.

use super::bead_result_to_py;
use crate::json_bridge::py_to_json_value;
use crate::prelude::*;
use sase_core::bead::{
    arm_recovery_activation as core_arm_recovery_activation,
    create_recovery_claim as core_create_recovery_claim,
    inspect_recovery_activation as core_inspect_recovery_activation,
    inspect_recovery_claim as core_inspect_recovery_claim,
    inspect_recovery_store as core_inspect_recovery_store,
    recovery_claim_key as core_recovery_claim_key, RecoveryArmRequestWire,
    RecoveryClaimCreateRequestWire, RecoveryClaimLookupWire,
    RECOVERY_WIRE_SCHEMA_VERSION,
};

#[pyfunction]
#[pyo3(name = "bead_recovery_wire_schema_version")]
fn py_bead_recovery_wire_schema_version() -> u32 {
    RECOVERY_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_recovery_claim_key")]
fn py_bead_recovery_claim_key(
    project_id: &str,
    epic_id: &str,
    close_token: &str,
) -> String {
    core_recovery_claim_key(project_id, epic_id, close_token)
}

#[pyfunction]
#[pyo3(name = "bead_recovery_inspect_activation")]
fn py_bead_recovery_inspect_activation<'py>(
    py: Python<'py>,
    beads_dir: &str,
    project_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let project_id = project_id.to_string();
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_inspect_recovery_activation(&beads_dir, &project_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_recovery_arm_activation")]
fn py_bead_recovery_arm_activation<'py>(
    py: Python<'py>,
    beads_dir: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let request: RecoveryArmRequestWire =
        parse_request(request, "RecoveryArmRequestWire")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_arm_recovery_activation(&beads_dir, &request)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_recovery_inspect_claim")]
fn py_bead_recovery_inspect_claim<'py>(
    py: Python<'py>,
    beads_dir: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let lookup: RecoveryClaimLookupWire =
        parse_request(request, "RecoveryClaimLookupWire")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_inspect_recovery_claim(&beads_dir, &lookup)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_recovery_create_claim")]
fn py_bead_recovery_create_claim<'py>(
    py: Python<'py>,
    beads_dir: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let request: RecoveryClaimCreateRequestWire =
        parse_request(request, "RecoveryClaimCreateRequestWire")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_create_recovery_claim(&beads_dir, &request)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_recovery_inspect_store")]
fn py_bead_recovery_inspect_store<'py>(
    py: Python<'py>,
    beads_dir: &str,
    project_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let project_id = project_id.to_string();
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_inspect_recovery_store(&beads_dir, &project_id)
        }),
    )
}

fn parse_request<'py, T: serde::de::DeserializeOwned>(
    request: &Bound<'py, PyDict>,
    label: &str,
) -> PyResult<T> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid {label} dict: {error}"
        ))
    })
}

pub(crate) fn register_recovery(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_bead_recovery_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_recovery_claim_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_recovery_inspect_activation, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_recovery_arm_activation, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_recovery_inspect_claim, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_recovery_create_claim, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_recovery_inspect_store, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_bridge::{json_value_to_py, py_to_json_value};
    use crate::sase_core_rs;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    fn beads_dir() -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        (temp, beads_dir)
    }

    #[test]
    fn recovery_bindings_round_trip_activation_and_claim() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "bead_recovery_wire_schema_version",
                "bead_recovery_claim_key",
                "bead_recovery_inspect_activation",
                "bead_recovery_arm_activation",
                "bead_recovery_inspect_claim",
                "bead_recovery_create_claim",
                "bead_recovery_inspect_store",
            ] {
                assert!(module.getattr(name).is_ok(), "{name}");
            }
            assert_eq!(
                module
                    .getattr("bead_recovery_wire_schema_version")
                    .unwrap()
                    .call0()
                    .unwrap()
                    .extract::<u32>()
                    .unwrap(),
                1
            );

            let (_temp, beads_dir) = beads_dir();
            let dir = beads_dir.to_str().unwrap();
            let absent = module
                .getattr("bead_recovery_inspect_activation")
                .unwrap()
                .call1((dir, "alpha"))
                .unwrap();
            let absent = py_to_json_value(&absent).unwrap();
            assert_eq!(absent["state"], json!("absent"));
            assert_eq!(absent["recovery_enabled"], json!(false));

            let arm_request = json_value_to_py(
                py,
                &json!({
                    "project_id": "alpha",
                    "armed_at": "2026-11-28T00:00:00Z"
                }),
            )
            .unwrap();
            let armed = module
                .getattr("bead_recovery_arm_activation")
                .unwrap()
                .call1((dir, arm_request))
                .unwrap();
            let armed = py_to_json_value(&armed).unwrap();
            assert_eq!(armed["state"], json!("created"));
            assert_eq!(armed["recovery_enabled"], json!(true));

            let create_request = json_value_to_py(
                py,
                &json!({
                    "project_id": "alpha",
                    "epic_id": "demo-1",
                    "close_token": "demo-1.1:evt-1",
                    "owner_machine": "athena",
                    "proc_id": "proc-a",
                    "request_fingerprint": "fp-a"
                }),
            )
            .unwrap();
            let created = module
                .getattr("bead_recovery_create_claim")
                .unwrap()
                .call1((dir, create_request))
                .unwrap();
            let created = py_to_json_value(&created).unwrap();
            assert_eq!(created["state"], json!("created"));
            assert_eq!(created["local_winner"], json!(true));
            let expected_key = module
                .getattr("bead_recovery_claim_key")
                .unwrap()
                .call1(("alpha", "demo-1", "demo-1.1:evt-1"))
                .unwrap()
                .extract::<String>()
                .unwrap();
            assert_eq!(created["claim_key"], json!(expected_key));

            let lookup = json_value_to_py(
                py,
                &json!({
                    "project_id": "alpha",
                    "epic_id": "demo-1",
                    "close_token": "demo-1.1:evt-1"
                }),
            )
            .unwrap();
            let inspected = module
                .getattr("bead_recovery_inspect_claim")
                .unwrap()
                .call1((dir, lookup))
                .unwrap();
            let inspected = py_to_json_value(&inspected).unwrap();
            assert_eq!(inspected["state"], json!("present"));
            assert_eq!(inspected["claim"]["owner_machine"], json!("athena"));

            let store = module
                .getattr("bead_recovery_inspect_store")
                .unwrap()
                .call1((dir, "alpha"))
                .unwrap();
            let store = py_to_json_value(&store).unwrap();
            assert_eq!(store["activation"]["state"], json!("armed"));
            assert_eq!(store["claims"].as_array().unwrap().len(), 1);
        });
    }
}
