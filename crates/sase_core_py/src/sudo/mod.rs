//! Sandboxed sudo runner bindings: manifest, ledger, and settlement.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

fn sudo_error_to_pyerr(error: sase_core::SudoWireError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn sudo_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal sudo wire serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

#[pyfunction]
#[pyo3(name = "sudo_validate_manifest")]
fn py_sudo_validate_manifest<'py>(
    py: Python<'py>,
    manifest: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(manifest.as_any())?;
    let normalized = py
        .allow_threads(|| sase_core::sudo_manifest_from_json_value(&value))
        .map_err(sudo_error_to_pyerr)?;
    sudo_wire_to_py(py, &normalized)
}

#[pyfunction]
#[pyo3(name = "sudo_manifest_sha256")]
fn py_sudo_manifest_sha256(
    py: Python<'_>,
    manifest: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let value = py_to_json_value(manifest.as_any())?;
    py.allow_threads(|| sase_core::sudo_manifest_json_sha256(&value))
        .map_err(sudo_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "sudo_derive_risk_badges")]
fn py_sudo_derive_risk_badges<'py>(
    py: Python<'py>,
    manifest: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(manifest.as_any())?;
    let assessments = py
        .allow_threads(|| {
            let manifest = sase_core::sudo_manifest_from_json_value(&value)?;
            sase_core::derive_sudo_risk_badges(&manifest)
        })
        .map_err(sudo_error_to_pyerr)?;
    sudo_wire_to_py(py, &assessments)
}

#[pyfunction]
#[pyo3(name = "sudo_validate_ledger", signature = (ledger, manifest=None))]
fn py_sudo_validate_ledger<'py>(
    py: Python<'py>,
    ledger: &Bound<'py, PyDict>,
    manifest: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let ledger = py_to_json_value(ledger.as_any())?;
    let manifest = manifest
        .map(|value| py_to_json_value(value.as_any()))
        .transpose()?;
    let normalized = py
        .allow_threads(|| {
            sase_core::sudo_validate_ledger_json_value(
                &ledger,
                manifest.as_ref(),
            )
        })
        .map_err(sudo_error_to_pyerr)?;
    sudo_wire_to_py(py, &normalized)
}

#[pyfunction]
#[pyo3(name = "sudo_validate_handshake", signature = (handshake, manifest=None))]
fn py_sudo_validate_handshake<'py>(
    py: Python<'py>,
    handshake: &Bound<'py, PyDict>,
    manifest: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let handshake = py_to_json_value(handshake.as_any())?;
    let manifest = manifest
        .map(|value| py_to_json_value(value.as_any()))
        .transpose()?;
    let normalized = py
        .allow_threads(|| {
            sase_core::sudo_validate_exec_started_json_value(
                &handshake,
                manifest.as_ref(),
            )
        })
        .map_err(sudo_error_to_pyerr)?;
    sudo_wire_to_py(py, &normalized)
}

#[pyfunction]
#[pyo3(name = "sudo_classify_attempt_liveness")]
fn py_sudo_classify_attempt_liveness<'py>(
    py: Python<'py>,
    attempt: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let attempt = py_to_json_value(attempt.as_any())?;
    let facts = py_to_json_value(facts.as_any())?;
    let decision = py
        .allow_threads(|| {
            let attempt = sase_core::sudo_attempt_from_json_value(&attempt)?;
            let facts: sase_core::SudoExecutorFactsWire =
                serde_json::from_value(facts).map_err(|error| {
                    sase_core::SudoWireError {
                        code: sase_core::SudoErrorCodeWire::Json,
                        message: format!(
                            "sudo executor facts JSON does not match wire contract: {error}"
                        ),
                        target: Some("facts".to_string()),
                    }
                })?;
            sase_core::classify_sudo_executor_liveness(&attempt, &facts)
        })
        .map_err(sudo_error_to_pyerr)?;
    sudo_wire_to_py(py, &decision)
}

#[pyfunction]
#[pyo3(name = "sudo_authorize_settlement")]
fn py_sudo_authorize_settlement<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let authorization = py
        .allow_threads(|| {
            sase_core::authorize_sudo_settlement_json_value(&value)
        })
        .map_err(sudo_error_to_pyerr)?;
    sudo_wire_to_py(py, &authorization)
}

fn python_hosted_sudo_runner_executable(py: Python<'_>) -> PyResult<PathBuf> {
    let sys = py.import_bound("sys")?;
    let executable: String = sys.getattr("executable")?.extract()?;
    python_hosted_sudo_runner_executable_from_str(&executable)
}

fn python_hosted_sudo_runner_executable_from_str(
    executable: &str,
) -> PyResult<PathBuf> {
    let program = PathBuf::from(executable);
    if executable.is_empty() || !program.is_absolute() {
        return Err(PyRuntimeError::new_err(format!(
            "Python sys.executable is not a usable absolute path: {executable:?}"
        )));
    }
    Ok(program)
}

#[pyfunction]
#[pyo3(name = "sudo_runner_main")]
fn py_sudo_runner_main(py: Python<'_>, args: Vec<String>) -> PyResult<()> {
    let program = python_hosted_sudo_runner_executable(py)?;
    py.allow_threads(|| {
        sase_gateway::run_python_hosted_sudo_runner_cli(program, args)
    })
    .map_err(|error| {
        let err = format!(
            "sase_sudo_runner exited with {}: {}",
            error.exit_code(),
            error
        );
        PyRuntimeError::new_err(err)
    })
}

pub(crate) fn register_sudo(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_sudo_validate_manifest, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_manifest_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_derive_risk_badges, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_validate_ledger, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_validate_handshake, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_classify_attempt_liveness, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_authorize_settlement, m)?)?;
    m.add_function(wrap_pyfunction!(py_sudo_runner_main, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
