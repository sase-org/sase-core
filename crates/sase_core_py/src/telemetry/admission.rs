//! Host-wide ToolRun admission bindings: enter, poll, and force.

use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::tool_run::{
    admission_enter, admission_force, admission_poll,
    ToolRunAdmissionEnterRequestWire, ToolRunAdmissionForceRequestWire,
    ToolRunAdmissionPollRequestWire,
};

use super::{telemetry_request_from_pydict, telemetry_result_to_py};

/// Record a waiting admission for a running run, then evaluate it.
#[pyfunction]
#[pyo3(
    name = "tool_run_admission_enter",
    signature = (store_path, request, busy_timeout_ms=0)
)]
pub(super) fn py_tool_run_admission_enter<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunAdmissionEnterRequestWire =
        telemetry_request_from_pydict(
            request,
            "ToolRunAdmissionEnterRequestWire",
        )?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            admission_enter(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

/// Report a run's queue position, granting a free slot when eligible.
#[pyfunction]
#[pyo3(
    name = "tool_run_admission_poll",
    signature = (store_path, request, busy_timeout_ms=0)
)]
pub(super) fn py_tool_run_admission_poll<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunAdmissionPollRequestWire =
        telemetry_request_from_pydict(
            request,
            "ToolRunAdmissionPollRequestWire",
        )?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            admission_poll(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

/// Admit a waiting run over the limit after the caller's max wait.
#[pyfunction]
#[pyo3(
    name = "tool_run_admission_force",
    signature = (store_path, request, busy_timeout_ms=0)
)]
pub(super) fn py_tool_run_admission_force<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: ToolRunAdmissionForceRequestWire =
        telemetry_request_from_pydict(
            request,
            "ToolRunAdmissionForceRequestWire",
        )?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            admission_force(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    telemetry_result_to_py(py, &result)
}

pub(super) fn register_tool_run_admission(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_tool_run_admission_enter, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_admission_poll, m)?)?;
    m.add_function(wrap_pyfunction!(py_tool_run_admission_force, m)?)?;
    Ok(())
}
