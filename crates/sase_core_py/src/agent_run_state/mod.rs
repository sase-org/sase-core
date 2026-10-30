//! Python bindings for core-owned published agent-run state policy.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::agent_run_state::{
    advance_agent_run_state_schedule as core_advance_agent_run_state_schedule,
    resolve_agent_run_state as core_resolve_agent_run_state,
    select_agent_run_state_window as core_select_agent_run_state_window,
    AgentRunStateResolveRequestWire, AgentRunStateScheduleRequestWire,
    AgentRunStateWindowRequestWire, AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
};

fn agent_run_state_error_to_pyerr(
    error: sase_core::agent_run_state::AgentRunStateError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "resolve_agent_run_state")]
fn py_resolve_agent_run_state<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentRunStateResolveRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid agent run state request: {error}"
            ))
        })?;
    let response = core_resolve_agent_run_state(request)
        .map_err(agent_run_state_error_to_pyerr)?;
    serialize_to_py(py, &response)
}

#[pyfunction]
#[pyo3(name = "select_agent_run_state_window")]
fn py_select_agent_run_state_window<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentRunStateWindowRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "invalid agent run state window request: {error}"
            ))
        })?;
    let response = core_select_agent_run_state_window(request)
        .map_err(agent_run_state_error_to_pyerr)?;
    serialize_to_py(py, &response)
}

#[pyfunction]
#[pyo3(name = "advance_agent_run_state_schedule")]
fn py_advance_agent_run_state_schedule<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentRunStateScheduleRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid agent run state schedule request: {error}"
            ))
        })?;
    let response = core_advance_agent_run_state_schedule(request)
        .map_err(agent_run_state_error_to_pyerr)?;
    serialize_to_py(py, &response)
}

#[pyfunction]
#[pyo3(name = "agent_run_state_wire_schema_version")]
fn py_agent_run_state_wire_schema_version<'py>(
    py: Python<'py>,
) -> PyResult<PyObject> {
    serialize_to_py(py, &AGENT_RUN_STATE_WIRE_SCHEMA_VERSION)
}

pub(crate) fn register_agent_run_state(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_resolve_agent_run_state, m)?)?;
    m.add_function(wrap_pyfunction!(py_select_agent_run_state_window, m)?)?;
    m.add_function(wrap_pyfunction!(py_advance_agent_run_state_schedule, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_run_state_wire_schema_version,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
