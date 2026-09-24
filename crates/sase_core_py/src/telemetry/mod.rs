//! Telemetry, tool-run, and usage-statistics bindings.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

/// Persist one telemetry accumulator flush in a single SQLite transaction.
#[pyfunction]
#[pyo3(
    name = "telemetry_record_batch",
    signature = (store_path, batch, busy_timeout_ms=250)
)]
fn py_telemetry_record_batch<'py>(
    py: Python<'py>,
    store_path: &str,
    batch: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let batch: TelemetryRecordBatchWire =
        telemetry_request_from_pydict(batch, "TelemetryRecordBatchWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_record_batch(
                &path,
                batch,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Preview or delete telemetry rows matching exact label values.
#[pyfunction]
#[pyo3(
    name = "telemetry_cleanup_matching_labels",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_cleanup_matching_labels<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryCleanupRequestWire =
        telemetry_request_from_pydict(request, "TelemetryCleanupRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_cleanup_matching_labels(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Query current telemetry values with source-aware gauge staleness.
#[pyfunction]
#[pyo3(
    name = "telemetry_query_instant",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_query_instant<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryInstantQueryWire =
        telemetry_request_from_pydict(request, "TelemetryInstantQueryWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_query_instant(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Query grouped telemetry series across raw and rollup resolutions.
#[pyfunction]
#[pyo3(
    name = "telemetry_query_range",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_query_range<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryRangeQueryWire =
        telemetry_request_from_pydict(request, "TelemetryRangeQueryWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_query_range(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Fold and prune telemetry rows using a caller-supplied retention policy.
#[pyfunction]
#[pyo3(
    name = "telemetry_prune",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_prune<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryPruneRequestWire =
        telemetry_request_from_pydict(request, "TelemetryPruneRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_prune(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Return telemetry database size, tier counts, and write freshness.
#[pyfunction]
#[pyo3(
    name = "telemetry_store_stats",
    signature = (store_path, busy_timeout_ms=250)
)]
fn py_telemetry_store_stats<'py>(
    py: Python<'py>,
    store_path: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_store_stats(
                &path,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "tool_run_wire_schema_version")]
fn py_tool_run_wire_schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "tool_run_normalize_definition")]
fn py_tool_run_normalize_definition<'py>(
    py: Python<'py>,
    definition: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let definition: ToolDefinitionWire =
        telemetry_request_from_pydict(definition, "ToolDefinitionWire")?;
    let result = py
        .allow_threads(|| core_tool_run_normalize_definition(definition))
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "tool_run_canonicalize_fingerprint")]
fn py_tool_run_canonicalize_fingerprint<'py>(
    py: Python<'py>,
    fingerprint: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let fingerprint: ToolFingerprintWire =
        telemetry_request_from_pydict(fingerprint, "ToolFingerprintWire")?;
    let result = py
        .allow_threads(|| core_tool_run_canonicalize_fingerprint(fingerprint))
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "tool_run_unknown_evidence")]
fn py_tool_run_unknown_evidence<'py>(
    py: Python<'py>,
    reason: &str,
) -> PyResult<PyObject> {
    telemetry_result_to_py(py, &core_tool_run_unknown_evidence(reason))
}

#[pyfunction]
#[pyo3(
    name = "tool_run_begin",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_begin<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunBeginRequestWire =
        telemetry_request_from_pydict(request, "ToolRunBeginRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_begin(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_append_event",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_append_event<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunAppendRequestWire =
        telemetry_request_from_pydict(request, "ToolRunAppendRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_append_event(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_finish",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_finish<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunFinishRequestWire =
        telemetry_request_from_pydict(request, "ToolRunFinishRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_finish(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_observe",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_observe<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunObserveRequestWire =
        telemetry_request_from_pydict(request, "ToolRunObserveRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_observe(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_reconcile",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_reconcile<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunReconcileRequestWire =
        telemetry_request_from_pydict(request, "ToolRunReconcileRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_reconcile(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_claim",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_claim<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: sase_core::tool_run::ToolRunClaimRequestWire =
        telemetry_request_from_pydict(request, "ToolRunClaimRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            sase_core::tool_run::claim(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_request_stop",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_request_stop<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: sase_core::tool_run::ToolRunStopRequestWire =
        telemetry_request_from_pydict(request, "ToolRunStopRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            sase_core::tool_run::request_stop(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_list",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_list<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunListRequestWire =
        telemetry_request_from_pydict(request, "ToolRunListRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_list(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_show",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_show<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunShowRequestWire =
        telemetry_request_from_pydict(request, "ToolRunShowRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_show(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_summary",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_summary<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunSummaryRequestWire =
        telemetry_request_from_pydict(request, "ToolRunSummaryRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_summary(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_retention_preview",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_retention_preview<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunRetentionRequestWire =
        telemetry_request_from_pydict(request, "ToolRunRetentionRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_retention_preview(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_retention_apply",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_tool_run_retention_apply<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunRetentionRequestWire =
        telemetry_request_from_pydict(request, "ToolRunRetentionRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_retention_apply(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "tool_run_store_stats",
    signature = (store_path, busy_timeout_ms=250)
)]
fn py_tool_run_store_stats<'py>(
    py: Python<'py>,
    store_path: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_tool_run_store_stats(
                &path,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

/// Aggregate durable TUI and launch performance JSONL logs.
#[pyfunction]
#[pyo3(name = "perf_logs_query")]
fn py_perf_logs_query<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: PerfLogsQueryWire =
        telemetry_request_from_pydict(request, "PerfLogsQueryWire")?;
    let result = py
        .allow_threads(|| core_perf_logs_query(request))
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

pub(crate) fn telemetry_request_from_pydict<T>(
    request: &Bound<'_, PyDict>,
    wire_name: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid {wire_name} dict: {error}"
        ))
    })
}

pub(crate) fn telemetry_result_to_py<T>(
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

pub(crate) fn register_telemetry(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_telemetry_cleanup_matching_labels, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_record_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_query_instant, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_query_range, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_prune, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_store_stats, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_normalize_definition, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_canonicalize_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_unknown_evidence, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_begin, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_append_event, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_finish, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_observe, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_reconcile, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_claim, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_request_stop, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_show, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_summary, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_retention_preview, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_retention_apply, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_store_stats, m)?)?;
    m.add_function(wrap_pyfunction!(py_perf_logs_query, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
