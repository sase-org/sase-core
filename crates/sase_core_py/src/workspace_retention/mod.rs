//! Workspace retention policy bindings.

use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::workspace_retention::{
    decide_workspace_retention, workspace_retention_wire_schema_version,
    WorkspaceRetentionRequestWire,
};

#[pyfunction]
#[pyo3(name = "workspace_retention_wire_schema_version")]
fn py_workspace_retention_wire_schema_version() -> u32 {
    workspace_retention_wire_schema_version()
}

#[pyfunction]
#[pyo3(name = "decide_workspace_retention")]
fn py_decide_workspace_retention<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let input: WorkspaceRetentionRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid WorkspaceRetentionRequestWire dict: {error}"
            ))
        })?;
    let decision = py.allow_threads(|| decide_workspace_retention(&input));
    let value = serde_json::to_value(decision).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_workspace_retention(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_workspace_retention_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_decide_workspace_retention, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
