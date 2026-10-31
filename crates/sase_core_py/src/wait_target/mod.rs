//! Wait-target vocabulary bindings.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::wait_target::{
    effective_wait_target, parse_wait_target, select_current_wait_turn,
    WaitReferenceKind, WaitTurnFacts, WAIT_TARGETS_WITH_DESCRIPTIONS,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct EffectiveWaitTargetRequestWire {
    requested: Option<String>,
    kind: WaitReferenceKind,
    #[serde(default)]
    reference: String,
}

#[derive(Debug, Deserialize)]
struct SelectCurrentWaitTurnRequestWire {
    #[serde(default)]
    members: Vec<WaitTurnFacts>,
}

fn map_error(error: sase_core::wait_target::WaitTargetError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "parse_wait_target")]
fn py_parse_wait_target(py: Python<'_>, raw: &str) -> PyResult<PyObject> {
    match parse_wait_target(raw) {
        Ok(target) => serialize_to_py(py, &target),
        Err(error) => Err(map_error(error)),
    }
}

#[pyfunction]
#[pyo3(name = "effective_wait_target")]
fn py_effective_wait_target(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: EffectiveWaitTargetRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid effective_wait_target dict: {error}"
            ))
        })?;
    let requested = match request.requested.as_deref() {
        None => None,
        Some(raw) => Some(parse_wait_target(raw).map_err(map_error)?),
    };
    match effective_wait_target(requested, request.kind, &request.reference) {
        Ok(result) => serialize_to_py(py, &result),
        Err(error) => Err(map_error(error)),
    }
}

#[pyfunction]
#[pyo3(name = "select_current_wait_turn")]
fn py_select_current_wait_turn(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: SelectCurrentWaitTurnRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid select_current_wait_turn dict: {error}"
            ))
        })?;
    let index = select_current_wait_turn(&request.members);
    serialize_to_py(py, &index)
}

#[pyfunction]
#[pyo3(name = "wait_targets_with_descriptions")]
fn py_wait_targets_with_descriptions(py: Python<'_>) -> PyResult<PyObject> {
    let rows: Vec<serde_json::Value> = WAIT_TARGETS_WITH_DESCRIPTIONS
        .iter()
        .map(|(value, description)| {
            serde_json::json!({
                "value": value,
                "description": description,
            })
        })
        .collect();
    serialize_to_py(py, &rows)
}

pub(crate) fn register_wait_target(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_parse_wait_target, m)?)?;
    m.add_function(wrap_pyfunction!(py_effective_wait_target, m)?)?;
    m.add_function(wrap_pyfunction!(py_select_current_wait_turn, m)?)?;
    m.add_function(wrap_pyfunction!(py_wait_targets_with_descriptions, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
