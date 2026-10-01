//! Memory-history bindings: snapshot sync and queries over git alone.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::memory_history::{
    memory_history_wire_schema_version as core_wire_version,
    query_compare as core_compare, query_feed as core_feed,
    query_resolve as core_resolve, query_subjects as core_subjects,
    query_sync as core_sync, query_timeline as core_timeline,
    query_version as core_version, MemoryHistoryCompareRequestWire,
    MemoryHistoryFeedRequestWire, MemoryHistoryResolveRequestWire,
    MemoryHistorySubjectsRequestWire, MemoryHistorySyncRequestWire,
    MemoryHistoryTimelineRequestWire, MemoryHistoryVersionRequestWire,
};

/// Return the memory-history wire schema version. No git runs.
#[pyfunction]
#[pyo3(name = "memory_history_wire_schema_version")]
fn py_memory_history_wire_schema_version() -> u32 {
    core_wire_version()
}

fn parse_request<T>(request: &Bound<'_, PyDict>, label: &str) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "request is not a valid {label} dict: {error}"
            ))
        },
    )
}

fn core_error_to_pyerr(
    error: sase_core::memory_history::MemoryHistoryError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Sync one scope and return the sync record.
#[pyfunction]
#[pyo3(name = "memory_history_sync")]
fn py_memory_history_sync<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistorySyncRequestWire =
        parse_request(request, "MemoryHistorySyncRequestWire")?;
    let out = py
        .allow_threads(|| core_sync(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

/// Sync one scope and list its subjects without version bodies.
#[pyfunction]
#[pyo3(name = "memory_history_subjects")]
fn py_memory_history_subjects<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistorySubjectsRequestWire =
        parse_request(request, "MemoryHistorySubjectsRequestWire")?;
    let out = py
        .allow_threads(|| core_subjects(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

/// Resolve a selector to its subject, with the newest or as-of version.
#[pyfunction]
#[pyo3(name = "memory_history_resolve")]
fn py_memory_history_resolve<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistoryResolveRequestWire =
        parse_request(request, "MemoryHistoryResolveRequestWire")?;
    let out = py
        .allow_threads(|| core_resolve(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

/// One subject's history with worktree pseudo-versions first.
#[pyfunction]
#[pyo3(name = "memory_history_timeline")]
fn py_memory_history_timeline<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistoryTimelineRequestWire =
        parse_request(request, "MemoryHistoryTimelineRequestWire")?;
    let out = py
        .allow_threads(|| core_timeline(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

/// One version by ordinal, `~N`, SHA prefix, or `now`, plus its body.
#[pyfunction]
#[pyo3(name = "memory_history_version")]
fn py_memory_history_version<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistoryVersionRequestWire =
        parse_request(request, "MemoryHistoryVersionRequestWire")?;
    let out = py
        .allow_threads(|| core_version(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

/// Compare two versions with both blobs fetched inside core.
#[pyfunction]
#[pyo3(name = "memory_history_compare")]
fn py_memory_history_compare<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistoryCompareRequestWire =
        parse_request(request, "MemoryHistoryCompareRequestWire")?;
    let out = py
        .allow_threads(|| core_compare(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

/// Sync scopes and merge their changesets into one feed.
#[pyfunction]
#[pyo3(name = "memory_history_feed")]
fn py_memory_history_feed<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: MemoryHistoryFeedRequestWire =
        parse_request(request, "MemoryHistoryFeedRequestWire")?;
    let out = py
        .allow_threads(|| core_feed(&request))
        .map_err(core_error_to_pyerr)?;
    serialize_to_py(py, &out)
}

pub(crate) fn register_memory_history(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_memory_history_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_sync, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_subjects, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_timeline, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_compare, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_history_feed, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
