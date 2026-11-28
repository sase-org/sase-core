//! Stranded-work reducer bindings.

use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::bead::ZombiePlanEvaluationInputWire;
use sase_core::stranded_work::StrandedWorkInputWire;

#[pyfunction]
#[pyo3(name = "stranded_work_reduce")]
fn py_stranded_work_reduce<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let input: StrandedWorkInputWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid StrandedWorkInputWire dict: {error}"
            ))
        })?;
    let report = py.allow_threads(|| {
        sase_core::stranded_work::reduce_stranded_work(&input)
    });
    let value = serde_json::to_value(report).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "zombie_plan_evaluate")]
fn py_zombie_plan_evaluate<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let input: ZombiePlanEvaluationInputWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid ZombiePlanEvaluationInputWire dict: {error}"
            ))
        })?;
    let report =
        py.allow_threads(|| sase_core::bead::evaluate_zombie_plans(&input));
    let value = serde_json::to_value(report).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_stranded_work(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_stranded_work_reduce, m)?)?;
    m.add_function(wrap_pyfunction!(py_zombie_plan_evaluate, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
