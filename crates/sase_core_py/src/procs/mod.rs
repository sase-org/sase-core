//! Background proc store bindings.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

// --- Background proc store bindings -------------------------------------
fn proc_store_error_to_pyerr(error: ProcStoreError) -> PyErr {
    match error {
        error @ ProcStoreError::LockTimeout { .. } => {
            PyTimeoutError::new_err(error.to_string())
        }
        error => PyValueError::new_err(error.to_string()),
    }
}

/// Read the proc JSONL store and return a snapshot dict.
#[pyfunction]
#[pyo3(name = "read_procs_snapshot")]
fn py_read_procs_snapshot(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| core_read_procs_snapshot(&path));
    proc_store_result_to_py(py, &snapshot.map_err(proc_store_error_to_pyerr)?)
}

/// Append one proc dict, enforce retention, and return the outcome dict.
#[pyfunction]
#[pyo3(name = "append_proc")]
fn py_append_proc<'py>(
    py: Python<'py>,
    path: &str,
    proc: &Bound<'py, PyDict>,
    history_limit: i64,
) -> PyResult<PyObject> {
    let proc = proc_from_pydict(proc)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_append_proc(&path, &proc, history_limit));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

/// Reserve one proc-shell row, replaying an identical active request.
#[pyfunction]
#[pyo3(name = "reserve_proc")]
fn py_reserve_proc<'py>(
    py: Python<'py>,
    path: &str,
    request: &Bound<'py, PyDict>,
    history_limit: i64,
) -> PyResult<PyObject> {
    let request = proc_reserve_from_pydict(request)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_reserve_proc(&path, &request, history_limit));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

/// Apply a partial proc update and return its matched/proc outcome dict.
#[pyfunction]
#[pyo3(name = "update_proc")]
fn py_update_proc<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = proc_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_update_proc(&path, &update));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

#[pyfunction]
#[pyo3(name = "claim_proc_supervisor")]
fn py_claim_proc_supervisor<'py>(
    py: Python<'py>,
    path: &str,
    claim: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let claim = proc_supervisor_claim_from_pydict(claim)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_claim_proc_supervisor(&path, &claim));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

#[pyfunction]
#[pyo3(name = "request_proc_stop")]
fn py_request_proc_stop<'py>(
    py: Python<'py>,
    path: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = proc_stop_request_from_pydict(request)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_request_proc_stop(&path, &request));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

#[pyfunction]
#[pyo3(name = "begin_proc_settlement")]
fn py_begin_proc_settlement<'py>(
    py: Python<'py>,
    path: &str,
    settlement: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let settlement = proc_settlement_from_pydict(settlement)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_begin_proc_settlement(&path, &settlement));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

#[pyfunction]
#[pyo3(name = "finish_proc")]
fn py_finish_proc<'py>(
    py: Python<'py>,
    path: &str,
    finish: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let finish = proc_finish_from_pydict(finish)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_finish_proc(&path, &finish));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

/// Enforce terminal-proc retention and return the fresh snapshot + pruned ids.
#[pyfunction]
#[pyo3(name = "prune_procs")]
fn py_prune_procs(
    py: Python<'_>,
    path: &str,
    history_limit: i64,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_prune_procs(&path, history_limit));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

#[pyfunction]
#[pyo3(name = "proc_runtime_retention_wire_schema_version")]
fn py_proc_runtime_retention_wire_schema_version() -> u32 {
    PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "apply_proc_runtime_retention")]
fn py_apply_proc_runtime_retention<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ProcRuntimeRetentionRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ProcRuntimeRetentionRequestWire dict: {error}"
        ))
    })?;
    let outcome =
        py.allow_threads(|| core_apply_proc_runtime_retention(&request));
    proc_store_result_to_py(py, &outcome.map_err(proc_store_error_to_pyerr)?)
}

#[pyfunction]
#[pyo3(name = "read_tasks_snapshot")]
fn py_read_tasks_snapshot(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    py_read_procs_snapshot(py, path)
}

#[pyfunction]
#[pyo3(name = "append_task")]
fn py_append_task<'py>(
    py: Python<'py>,
    path: &str,
    task: &Bound<'py, PyDict>,
    history_limit: i64,
) -> PyResult<PyObject> {
    py_append_proc(py, path, task, history_limit)
}

#[pyfunction]
#[pyo3(name = "update_task")]
fn py_update_task<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    py_update_proc(py, path, update)
}

#[pyfunction]
#[pyo3(name = "prune_tasks")]
fn py_prune_tasks(
    py: Python<'_>,
    path: &str,
    history_limit: i64,
) -> PyResult<PyObject> {
    py_prune_procs(py, path, history_limit)
}

fn proc_from_pydict(dict: &Bound<'_, PyDict>) -> PyResult<ProcWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "proc is not a valid ProcWire dict: {error}"
        ))
    })
}

fn proc_reserve_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ProcReserveWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "reserve request is not a valid ProcReserveWire dict: {error}"
        ))
    })
}

fn proc_update_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ProcUpdateWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "update is not a valid ProcUpdateWire dict: {error}"
        ))
    })
}

fn proc_supervisor_claim_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ProcSupervisorClaimWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "supervisor claim is not a valid ProcSupervisorClaimWire dict: {error}"
        ))
    })
}

fn proc_stop_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ProcStopRequestWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "stop request is not a valid ProcStopRequestWire dict: {error}"
        ))
    })
}

fn proc_settlement_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ProcSettlementWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "settlement request is not a valid ProcSettlementWire dict: {error}"
        ))
    })
}

fn proc_finish_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ProcFinishWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "finish request is not a valid ProcFinishWire dict: {error}"
        ))
    })
}

pub(crate) fn proc_store_result_to_py<T>(
    py: Python<'_>,
    result: &T,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_procs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_read_procs_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_proc, m)?)?;
    m.add_function(wrap_pyfunction!(py_reserve_proc, m)?)?;
    m.add_function(wrap_pyfunction!(py_update_proc, m)?)?;
    m.add_function(wrap_pyfunction!(py_claim_proc_supervisor, m)?)?;
    m.add_function(wrap_pyfunction!(py_request_proc_stop, m)?)?;
    m.add_function(wrap_pyfunction!(py_begin_proc_settlement, m)?)?;
    m.add_function(wrap_pyfunction!(py_finish_proc, m)?)?;
    m.add_function(wrap_pyfunction!(py_prune_procs, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_proc_runtime_retention_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_apply_proc_runtime_retention, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_tasks_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_task, m)?)?;
    m.add_function(wrap_pyfunction!(py_update_task, m)?)?;
    m.add_function(wrap_pyfunction!(py_prune_tasks, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
