//! Epic-follow reducer bindings.

use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::wait_epic_follow::WaitEpicFollowInputWire;

#[pyfunction]
#[pyo3(name = "wait_epic_follow_reduce")]
fn py_wait_epic_follow_reduce<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let input: WaitEpicFollowInputWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid WaitEpicFollowInputWire dict: {error}"
            ))
        })?;
    let decisions = py.allow_threads(|| {
        sase_core::wait_epic_follow::wait_epic_follow_reduce(&input)
    });
    let value = serde_json::to_value(decisions).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_wait_epic_follow(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_wait_epic_follow_reduce, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
