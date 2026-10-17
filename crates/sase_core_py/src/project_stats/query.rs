use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::project_stats::{
    query_project_stats as core_query, ProjectStatsError,
    ProjectStatsRequestWire,
};

fn parse_request(
    request: &Bound<'_, PyDict>,
) -> PyResult<ProjectStatsRequestWire> {
    serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "request is not a valid ProjectStatsRequestWire dict: {error}"
            ))
        },
    )
}

fn core_error_to_pyerr(error: ProjectStatsError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Query project statistics for the supplied request dict.
#[pyfunction]
#[pyo3(name = "project_stats_query")]
fn py_project_stats_query<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = parse_request(request)?;
    let out = py
        .allow_threads(|| core_query(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

pub(crate) fn register_project_stats(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_project_stats_query, m)?)?;
    Ok(())
}
