//! Staged-handoff bindings: wire version, validation, and admission.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;

use sase_core::staged_handoff::{
    build_gate_staged_payload as core_build_gate_payload,
    evaluate_stage_admission as core_evaluate_stage_admission,
    evaluate_staged_monitor_membership as core_evaluate_membership,
    evaluate_staged_owner_liveness as core_evaluate_liveness,
    evaluate_staged_self_wait as core_evaluate_self_wait,
    gate_staged_payload_schema_version as core_gate_payload_version,
    receipt_authorizes as core_receipt_authorizes,
    staged_handoff_wire_schema_version as core_wire_version,
    staged_monitor_launch_payload_schema_version as core_launch_version,
    validate_gate_staged_payload as core_validate_gate_payload,
    validate_staged_handoff as core_validate_staged_handoff,
    validate_staged_handoff_receipt as core_validate_receipt,
    validate_staged_monitor_launch_payload as core_validate_launch_payload,
    StagedMonitorMembershipRequestWire as CoreMembershipRequest,
    StagedOwnerLivenessRequestWire as CoreLivenessRequest,
    StagedSelfWaitRequestWire as CoreSelfWaitRequest,
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

/// Return the staged gate payload schema version.
#[pyfunction]
#[pyo3(name = "gate_staged_payload_schema_version")]
fn py_gate_staged_payload_schema_version() -> u32 {
    core_gate_payload_version()
}

/// Validate a staged gate payload dict.
#[pyfunction]
#[pyo3(name = "validate_gate_staged_payload")]
fn py_validate_gate_staged_payload<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(payload.as_any())?;
    let normalized = core_validate_gate_payload(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

/// Return the staged-monitor launch payload wire schema version.
#[pyfunction]
#[pyo3(name = "staged_monitor_launch_payload_schema_version")]
fn py_staged_monitor_launch_payload_schema_version() -> u32 {
    core_launch_version()
}

/// Validate a staged-monitor launch payload dict.
#[pyfunction]
#[pyo3(name = "validate_staged_monitor_launch_payload")]
fn py_validate_staged_monitor_launch_payload<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(payload.as_any())?;
    let normalized = core_validate_launch_payload(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

/// Assemble a staged gate payload from its parts, then validate it.
#[pyfunction]
#[pyo3(name = "build_gate_staged_payload")]
fn py_build_gate_staged_payload<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(payload.as_any())?;
    let normalized = core_build_gate_payload(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

fn staged_decision_from_pydict<T>(
    request: &Bound<'_, PyDict>,
    what: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "{what} is not a valid staged monitor request: {error}"
            ))
        },
    )
}

/// Decide staged-monitor membership facts from durable monitor state.
#[pyfunction]
#[pyo3(name = "evaluate_staged_monitor_membership")]
fn py_evaluate_staged_monitor_membership<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: CoreMembershipRequest =
        staged_decision_from_pydict(request, "membership")?;
    let decision = core_evaluate_membership(request)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
}

/// Decide whether a staged monitor remains a pending owner.
#[pyfunction]
#[pyo3(name = "evaluate_staged_owner_liveness")]
fn py_evaluate_staged_owner_liveness<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: CoreLivenessRequest =
        staged_decision_from_pydict(request, "owner liveness")?;
    let decision = core_evaluate_liveness(request)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
}

/// Decide whether a synchronous wait is a self-wait on the caller's own
/// pending staged monitor.
#[pyfunction]
#[pyo3(name = "evaluate_staged_self_wait")]
fn py_evaluate_staged_self_wait<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: CoreSelfWaitRequest =
        staged_decision_from_pydict(request, "self wait")?;
    let decision = core_evaluate_self_wait(request)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
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
    m.add_function(wrap_pyfunction!(
        py_gate_staged_payload_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_validate_gate_staged_payload, m)?)?;
    m.add_function(wrap_pyfunction!(py_build_gate_staged_payload, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_staged_monitor_launch_payload_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_validate_staged_monitor_launch_payload,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_evaluate_staged_monitor_membership,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_staged_owner_liveness, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_staged_self_wait, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
