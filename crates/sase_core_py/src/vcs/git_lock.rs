//! Git index-lock recovery policy bindings.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::git_lock::{
    classify_git_lock_failure as core_classify_git_lock_failure,
    classify_git_lock_owner as core_classify_git_lock_owner,
    decide_eager_git_lock_cleanup as core_decide_eager_git_lock_cleanup,
    decide_git_lock_recovery as core_decide_git_lock_recovery,
    default_git_lock_retry_delays as core_default_git_lock_retry_delays,
    git_lock_wire_schema_version as core_git_lock_wire_schema_version,
    match_git_lock_path as core_match_git_lock_path,
    parse_git_lock_retry_delays as core_parse_git_lock_retry_delays,
    validate_git_lock_retry_delays as core_validate_git_lock_retry_delays,
    GitLockEagerCleanupRequestWire, GitLockError,
    GitLockFailureObservationWire, GitLockOwnerFactsWire,
    GitLockPathMatchRequestWire, GitLockRecoveryRequestWire,
};

fn git_lock_error_to_pyerr(error: GitLockError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "git_lock_wire_schema_version")]
pub(super) fn py_git_lock_wire_schema_version() -> u32 {
    core_git_lock_wire_schema_version()
}

#[pyfunction]
#[pyo3(name = "default_git_lock_retry_delays")]
pub(super) fn py_default_git_lock_retry_delays() -> Vec<f64> {
    core_default_git_lock_retry_delays()
}

#[pyfunction]
#[pyo3(name = "parse_git_lock_retry_delays", signature = (raw=None))]
pub(super) fn py_parse_git_lock_retry_delays(raw: Option<&str>) -> Vec<f64> {
    core_parse_git_lock_retry_delays(raw)
}

#[pyfunction]
#[pyo3(name = "validate_git_lock_retry_delays")]
pub(super) fn py_validate_git_lock_retry_delays(
    delays: Vec<f64>,
) -> PyResult<Vec<f64>> {
    core_validate_git_lock_retry_delays(&delays)
        .map_err(git_lock_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "classify_git_lock_failure")]
pub(super) fn py_classify_git_lock_failure<'py>(
    py: Python<'py>,
    observation: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let observation: GitLockFailureObservationWire = serde_json::from_value(
        py_to_json_value(observation.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "observation is not a valid GitLockFailureObservationWire dict: {error}"
        ))
    })?;
    serialize_to_py(py, &core_classify_git_lock_failure(&observation))
}

#[pyfunction]
#[pyo3(name = "match_git_lock_path")]
pub(super) fn py_match_git_lock_path<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: GitLockPathMatchRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid GitLockPathMatchRequestWire dict: {error}"
        ))
    })?;
    let result =
        core_match_git_lock_path(&request).map_err(git_lock_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "classify_git_lock_owner")]
pub(super) fn py_classify_git_lock_owner<'py>(
    py: Python<'py>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let facts: GitLockOwnerFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid GitLockOwnerFactsWire dict: {error}"
        ))
    })?;
    let result = core_classify_git_lock_owner(&facts)
        .map_err(git_lock_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "decide_git_lock_recovery")]
pub(super) fn py_decide_git_lock_recovery<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: GitLockRecoveryRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid GitLockRecoveryRequestWire dict: {error}"
        ))
    })?;
    let result = core_decide_git_lock_recovery(&request)
        .map_err(git_lock_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "decide_eager_git_lock_cleanup")]
pub(super) fn py_decide_eager_git_lock_cleanup<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: GitLockEagerCleanupRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid GitLockEagerCleanupRequestWire dict: {error}"
        ))
    })?;
    let result = core_decide_eager_git_lock_cleanup(&request)
        .map_err(git_lock_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

pub(super) fn register_git_lock(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_git_lock_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_default_git_lock_retry_delays, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_lock_retry_delays, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_git_lock_retry_delays, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_git_lock_failure, m)?)?;
    m.add_function(wrap_pyfunction!(py_match_git_lock_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_git_lock_owner, m)?)?;
    m.add_function(wrap_pyfunction!(py_decide_git_lock_recovery, m)?)?;
    m.add_function(wrap_pyfunction!(py_decide_eager_git_lock_cleanup, m)?)?;
    Ok(())
}
