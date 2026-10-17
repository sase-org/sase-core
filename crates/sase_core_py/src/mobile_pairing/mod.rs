//! Owner-local mobile pairing challenge issuance.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

use sase_core::mobile_pairing::{
    issue_mobile_pairing_challenge_now, MobilePairingError,
    MobilePairingIssueRequestWire,
};

fn pairing_error_to_pyerr(error: MobilePairingError) -> PyErr {
    match error {
        MobilePairingError::SchemaVersion { .. } => {
            PyValueError::new_err(error.to_string())
        }
        MobilePairingError::LockTimeout => {
            PyTimeoutError::new_err(error.to_string())
        }
        MobilePairingError::UnsupportedPlatform
        | MobilePairingError::UnsafePath { .. }
        | MobilePairingError::Corrupt
        | MobilePairingError::Unavailable
        | MobilePairingError::MissingStore => {
            PyOSError::new_err(error.to_string())
        }
    }
}

#[pyfunction]
#[pyo3(name = "mobile_pairing_issue")]
fn py_mobile_pairing_issue<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: MobilePairingIssueRequestWire = serde_json::from_value(value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let response = py
        .allow_threads(|| issue_mobile_pairing_challenge_now(&request))
        .map_err(pairing_error_to_pyerr)?;
    let json = serde_json::to_value(&response).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal mobile pairing serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

pub(crate) fn register_mobile_pairing(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_mobile_pairing_issue, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
