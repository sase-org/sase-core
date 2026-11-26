//! PyO3 bindings for question copy metadata.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::question_copy::{
    display_question_copy as core_display_question_copy,
    normalize_question_copy as core_normalize_question_copy,
};

#[pyfunction]
#[pyo3(name = "normalize_question_copy")]
fn py_normalize_question_copy<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(value)?;
    let normalized = core_normalize_question_copy(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

#[pyfunction]
#[pyo3(name = "display_question_copy")]
fn py_display_question_copy<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(value)?;
    let display = core_display_question_copy(Some(&value));
    serialize_to_py(py, &display)
}

pub(crate) fn register_question_copy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_normalize_question_copy, m)?)?;
    m.add_function(wrap_pyfunction!(py_display_question_copy, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
