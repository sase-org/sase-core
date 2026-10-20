//! Release-steward policy bindings.

use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::release_steward::{
    release_steward_decide_from_json, release_steward_wire_schema_version,
    ReleaseStewardError,
};

fn error_to_py(error: ReleaseStewardError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the release-steward request/result wire schema version.
#[pyfunction]
#[pyo3(name = "release_steward_wire_schema_version")]
fn py_release_steward_wire_schema_version() -> u32 {
    release_steward_wire_schema_version()
}

/// Decide one release-steward tick from a complete snapshot dict.
///
/// *request* must be a `ReleaseStewardRequestWire`-shape dict. Structurally
/// invalid requests surface as `ValueError`. Well-formed snapshots always
/// return a result dict; `WATCHER_BROKEN` is a typed state, not an
/// exception.
#[pyfunction]
#[pyo3(name = "release_steward_decide")]
fn py_release_steward_decide<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let result = py
        .allow_threads(|| release_steward_decide_from_json(&value))
        .map_err(error_to_py)?;
    let value = serde_json::to_value(&result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_release_steward(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_release_steward_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_release_steward_decide, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
