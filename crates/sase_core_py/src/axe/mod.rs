//! AXE runtime bindings: chop engine, status, and overrun classification.

use crate::prelude::*;

use crate::json_bridge::{
    json_value_to_py, py_to_json_value, strings_from_py_list,
};

use pyo3::wrap_pyfunction;

// --- Chop overrun classification ------------------------------------------
fn chop_overrun_error_to_pyerr(error: ChopOverrunError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the supported chop-overrun wire schema version.
#[pyfunction]
#[pyo3(name = "chop_overrun_wire_schema_version")]
fn py_chop_overrun_wire_schema_version() -> u32 {
    CHOP_OVERRUN_SCHEMA_VERSION
}

/// Classify one chop's cached run history against its lumberjack's interval.
#[pyfunction]
#[pyo3(name = "classify_chop_overrun")]
fn py_classify_chop_overrun<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ChopOverrunRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid ChopOverrunRequestWire dict: {error}"
            ))
        })?;
    let verdict = py
        .allow_threads(|| core_classify_chop_overrun(&request))
        .map_err(chop_overrun_error_to_pyerr)?;
    let value = serde_json::to_value(verdict).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Portable AXE runtime status -----------------------------------------
fn axe_status_error_to_pyerr(error: AxeStatusError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the supported AXE runtime status wire schema version.
#[pyfunction]
#[pyo3(name = "axe_status_wire_schema_version")]
fn py_axe_status_wire_schema_version() -> u32 {
    AXE_STATUS_SCHEMA_VERSION
}

/// Classify already-collected AXE runtime observations without host I/O.
#[pyfunction]
#[pyo3(name = "classify_axe_status")]
fn py_classify_axe_status<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AxeStatusRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid AxeStatusRequestWire dict: {error}"
            ))
        })?;
    let snapshot = py
        .allow_threads(|| core_classify_axe_status(&request))
        .map_err(axe_status_error_to_pyerr)?;
    let value = serde_json::to_value(snapshot).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Project an internal AXE status snapshot to the public routine/job envelope.
#[pyfunction]
#[pyo3(name = "project_axe_status_public")]
fn py_project_axe_status_public<'py>(
    py: Python<'py>,
    snapshot: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(snapshot.as_any())?;
    let snapshot: AxeStatusSnapshotWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "snapshot is not a valid AxeStatusSnapshotWire dict: {error}"
            ))
        })?;
    let projected =
        py.allow_threads(|| core_project_axe_status_public(&snapshot));
    let value = serde_json::to_value(projected).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Axe chop engine bindings --------------------------------------------
fn chop_error_to_pyerr(error: ChopEngineError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn chop_request_from_pydict<T>(
    request: &Bound<'_, PyDict>,
    label: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid {label} dict: {error}"
        ))
    })
}

fn chop_result_to_py<T>(py: Python<'_>, result: &T) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "chop_engine_schema_version")]
fn py_chop_engine_schema_version() -> u32 {
    CHOP_ENGINE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "chop_result_schema_version")]
fn py_chop_result_schema_version() -> u32 {
    CHOP_RESULT_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "chop_state_schema_version")]
fn py_chop_state_schema_version() -> u32 {
    CHOP_STATE_SCHEMA_VERSION
}

/// Normalize and bound one captured AXE subprocess failure diagnostic.
#[pyfunction]
#[pyo3(name = "normalize_chop_subprocess_diagnostic")]
fn py_normalize_chop_subprocess_diagnostic<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopSubprocessDiagnosticRequestWire =
        chop_request_from_pydict(
            request,
            "chop subprocess diagnostic request",
        )?;
    let result = core_normalize_chop_subprocess_diagnostic(&request)
        .map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Parse and validate a script-written chop result JSON document.
#[pyfunction]
#[pyo3(name = "parse_chop_result")]
fn py_parse_chop_result<'py>(
    py: Python<'py>,
    document: &str,
) -> PyResult<PyObject> {
    let result =
        core_parse_chop_result(document).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Validate and normalize an already-decoded chop result dict.
#[pyfunction]
#[pyo3(name = "validate_chop_result")]
fn py_validate_chop_result<'py>(
    py: Python<'py>,
    result: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let result: ChopResultDocumentWire =
        chop_request_from_pydict(result, "job result")?;
    core_validate_chop_result(&result).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Validate and normalize one launch proposal.
#[pyfunction]
#[pyo3(name = "validate_chop_proposal")]
#[pyo3(signature = (proposal, index = 0, prior_ids = None))]
fn py_validate_chop_proposal<'py>(
    py: Python<'py>,
    proposal: &Bound<'py, PyDict>,
    index: usize,
    prior_ids: Option<&Bound<'py, PyList>>,
) -> PyResult<PyObject> {
    let proposal: ChopLaunchProposalWire =
        chop_request_from_pydict(proposal, "chop launch proposal")?;
    let prior_ids = match prior_ids {
        Some(items) => strings_from_py_list(items, "prior_ids")?,
        None => Vec::new(),
    };
    core_validate_chop_proposal(&proposal, index, &prior_ids)
        .map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &proposal)
}

/// Derive the default agent name scaffold for one proposal.
#[pyfunction]
#[pyo3(name = "derive_chop_agent_name")]
#[pyo3(signature = (chop_name, target_key = None, proposal_index = 0, run_token = None))]
fn py_derive_chop_agent_name(
    chop_name: &str,
    target_key: Option<&str>,
    proposal_index: usize,
    run_token: Option<&str>,
) -> PyResult<String> {
    core_derive_chop_agent_name(
        chop_name,
        target_key,
        proposal_index,
        run_token,
    )
    .map_err(chop_error_to_pyerr)
}

/// Evaluate inhibit guards followed by the configured trigger.
#[pyfunction]
#[pyo3(name = "evaluate_chop_decision")]
fn py_evaluate_chop_decision<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopDecisionRequestWire =
        chop_request_from_pydict(request, "chop decision request")?;
    let result =
        core_evaluate_chop_decision(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Transform a runner-owned checkpoint document.
#[pyfunction]
#[pyo3(name = "apply_chop_checkpoint_update")]
fn py_apply_chop_checkpoint_update<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopCheckpointUpdateRequestWire =
        chop_request_from_pydict(request, "chop checkpoint update request")?;
    let result =
        core_apply_checkpoint_update(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Test and record one key in a bounded runner-owned seen store.
#[pyfunction]
#[pyo3(name = "check_and_record_chop_once_per")]
fn py_check_and_record_chop_once_per<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopOncePerRequestWire =
        chop_request_from_pydict(request, "chop once-per request")?;
    let result = core_check_and_record_once_per(&request)
        .map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Release exact keys from a bounded runner-owned seen store.
#[pyfunction]
#[pyo3(name = "release_chop_once_per")]
fn py_release_chop_once_per<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopOncePerReleaseRequestWire =
        chop_request_from_pydict(request, "chop once-per release request")?;
    let result =
        core_release_chop_once_per(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Expand literal or host-provided source targets into stable instances.
#[pyfunction]
#[pyo3(name = "expand_chop_targets")]
fn py_expand_chop_targets<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopTargetExpansionRequestWire =
        chop_request_from_pydict(request, "chop target expansion request")?;
    let result =
        core_expand_chop_targets(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Parse one strict positive compound duration into seconds.
#[pyfunction]
#[pyo3(name = "parse_chop_duration")]
fn py_parse_chop_duration(value: &str) -> PyResult<u64> {
    core_parse_chop_duration(value).map_err(chop_error_to_pyerr)
}

/// Normalize and split one AXE description into its summary and body.
#[pyfunction]
#[pyo3(name = "split_axe_description")]
fn py_split_axe_description(text: &str) -> (String, String) {
    core_split_axe_description(text)
}

/// Return provenance-aware diagnostics for the new axe config shape.
#[pyfunction]
#[pyo3(name = "validate_axe_config")]
fn py_validate_axe_config<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AxeConfigValidationRequestWire =
        chop_request_from_pydict(request, "axe config validation request")?;
    let result =
        core_validate_axe_config(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

pub(crate) fn register_axe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_chop_overrun_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_chop_overrun, m)?)?;
    m.add_function(wrap_pyfunction!(py_axe_status_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_axe_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_project_axe_status_public, m)?)?;
    m.add_function(wrap_pyfunction!(py_chop_engine_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_chop_result_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_chop_state_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_normalize_chop_subprocess_diagnostic,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_parse_chop_result, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_chop_result, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_chop_proposal, m)?)?;
    m.add_function(wrap_pyfunction!(py_derive_chop_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_chop_decision, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_chop_checkpoint_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_check_and_record_chop_once_per, m)?)?;
    m.add_function(wrap_pyfunction!(py_release_chop_once_per, m)?)?;
    m.add_function(wrap_pyfunction!(py_expand_chop_targets, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_chop_duration, m)?)?;
    m.add_function(wrap_pyfunction!(py_split_axe_description, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_axe_config, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
