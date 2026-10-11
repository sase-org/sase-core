//! Project-onboarding bindings: classify candidates and evaluate
//! repository-creation forms.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::project_onboarding::{
    classify_project_candidates as core_classify_project_candidates,
    evaluate_repo_creation_form as core_evaluate_repo_creation_form,
    project_onboarding_wire_schema_version as core_wire_version,
    CreationFormRequestWire, ProjectCandidateInputWire, ProjectOnboardingError,
};

fn project_onboarding_error_to_pyerr(error: ProjectOnboardingError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the project-onboarding wire schema version.
#[pyfunction]
#[pyo3(name = "project_onboarding_wire_schema_version")]
fn py_project_onboarding_wire_schema_version() -> u32 {
    core_wire_version()
}

/// Classify project candidates into grouped and search-only rows.
#[pyfunction]
#[pyo3(name = "classify_project_candidates")]
fn py_classify_project_candidates<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ProjectCandidateInputWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ProjectCandidateInputWire dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_classify_project_candidates(&request))
        .map_err(project_onboarding_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

/// Evaluate a repository creation form in strict or lenient mode.
#[pyfunction]
#[pyo3(name = "evaluate_repo_creation_form")]
fn py_evaluate_repo_creation_form<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: CreationFormRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid CreationFormRequestWire dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_evaluate_repo_creation_form(&request))
        .map_err(project_onboarding_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

pub(crate) fn register_project_onboarding(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_project_onboarding_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_classify_project_candidates, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_repo_creation_form, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
