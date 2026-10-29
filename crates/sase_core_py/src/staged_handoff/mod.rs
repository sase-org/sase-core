//! Staged-handoff bindings: wire version, validation, and admission.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;

use sase_core::staged_handoff::{
    evaluate_stage_admission as core_evaluate_stage_admission,
    receipt_authorizes as core_receipt_authorizes,
    staged_handoff_wire_schema_version as core_wire_version,
    validate_staged_handoff as core_validate_staged_handoff,
    validate_staged_handoff_receipt as core_validate_receipt,
};

/// Return the staged-handoff wire schema version.
#[pyfunction]
#[pyo3(name = "staged_handoff_wire_schema_version")]
fn py_staged_handoff_wire_schema_version() -> u32 {
    core_wire_version()
}

/// Validate a staged-handoff record dict.
#[pyfunction]
#[pyo3(name = "validate_staged_handoff")]
fn py_validate_staged_handoff<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(record.as_any())?;
    let normalized = core_validate_staged_handoff(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

/// Validate a staged-handoff adoption receipt dict.
#[pyfunction]
#[pyo3(name = "validate_staged_handoff_receipt")]
fn py_validate_staged_handoff_receipt<'py>(
    py: Python<'py>,
    receipt: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(receipt.as_any())?;
    let normalized = core_validate_receipt(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

/// Decide whether host observations allow staging a handoff.
#[pyfunction]
#[pyo3(name = "evaluate_stage_admission")]
fn py_evaluate_stage_admission(request: &Bound<'_, PyDict>) -> PyResult<()> {
    let value = py_to_json_value(request.as_any())?;
    core_evaluate_stage_admission(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

/// Return whether a receipt authorizes adoption of ``staged_id``.
#[pyfunction]
#[pyo3(name = "staged_handoff_receipt_authorizes")]
fn py_staged_handoff_receipt_authorizes(
    receipt: &Bound<'_, PyDict>,
    staged_id: &str,
) -> PyResult<bool> {
    let value = py_to_json_value(receipt.as_any())?;
    core_receipt_authorizes(&value, staged_id)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

pub(crate) fn register_staged_handoff(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_staged_handoff_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_validate_staged_handoff, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_staged_handoff_receipt, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_stage_admission, m)?)?;
    m.add_function(wrap_pyfunction!(py_staged_handoff_receipt_authorizes, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
