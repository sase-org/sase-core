//! Durable agent hold bindings and deadlock analysis.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use crate::provider_policy::effort_override_now;

use pyo3::wrap_pyfunction;

// --- Durable agent holds -------------------------------------------------
pub(crate) fn agent_hold_error_to_pyerr(err: AgentHoldDomainError) -> PyErr {
    match err {
        AgentHoldDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        AgentHoldDomainError::LockTimeout { .. } => {
            PyTimeoutError::new_err(err.to_string())
        }
        AgentHoldDomainError::Io(_) | AgentHoldDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

fn agent_hold_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal agent-hold serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

fn agent_hold_dict_from_py<T>(
    value: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!("{label} is not a valid dict: {error}"))
    })
}

fn agent_hold_liveness_from_optional(
    liveness: Option<&Bound<'_, PyDict>>,
) -> PyResult<AgentHoldLivenessFactsWire> {
    match liveness {
        Some(value) => {
            agent_hold_dict_from_py(value.as_any(), "agent hold liveness")
        }
        None => Ok(AgentHoldLivenessFactsWire::default()),
    }
}

#[pyfunction]
#[pyo3(name = "agent_hold_wire_schema_version")]
fn py_agent_hold_wire_schema_version() -> u32 {
    sase_core::AGENT_HOLD_WIRE_SCHEMA_VERSION
}

fn agent_hold_capture_from_optional(
    capture: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<AgentHoldCaptureSummaryWire>> {
    match capture {
        Some(value) => Ok(Some(agent_hold_dict_from_py(
            value.as_any(),
            "agent hold capture",
        )?)),
        None => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(
    name = "agent_hold_arm_relative",
    signature = (
        sase_home,
        armer,
        scope,
        selectors,
        duration_seconds,
        liveness = None,
        now = None,
        capture = None
    )
)]
fn py_agent_hold_arm_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    armer: &Bound<'py, PyDict>,
    scope: &Bound<'py, PyDict>,
    selectors: &Bound<'py, PyDict>,
    duration_seconds: f64,
    liveness: Option<&Bound<'py, PyDict>>,
    now: Option<f64>,
    capture: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let armer: AgentHoldArmerWire =
        agent_hold_dict_from_py(armer.as_any(), "agent hold armer")?;
    let scope: AgentHoldScopeWire =
        agent_hold_dict_from_py(scope.as_any(), "agent hold scope")?;
    let selectors: AgentHoldSelectorsWire =
        agent_hold_dict_from_py(selectors.as_any(), "agent hold selectors")?;
    let liveness = agent_hold_liveness_from_optional(liveness)?;
    let capture = agent_hold_capture_from_optional(capture)?;
    let record = core_arm_agent_hold_relative(
        &PathBuf::from(sase_home),
        armer,
        scope,
        selectors,
        duration_seconds,
        &liveness,
        effort_override_now(now)?,
        capture,
    )
    .map_err(agent_hold_error_to_pyerr)?;
    agent_hold_wire_to_py(py, &record)
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(
    name = "agent_hold_arm_until",
    signature = (
        sase_home,
        armer,
        scope,
        selectors,
        expires_at,
        liveness = None,
        now = None,
        capture = None
    )
)]
fn py_agent_hold_arm_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    armer: &Bound<'py, PyDict>,
    scope: &Bound<'py, PyDict>,
    selectors: &Bound<'py, PyDict>,
    expires_at: f64,
    liveness: Option<&Bound<'py, PyDict>>,
    now: Option<f64>,
    capture: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let armer: AgentHoldArmerWire =
        agent_hold_dict_from_py(armer.as_any(), "agent hold armer")?;
    let scope: AgentHoldScopeWire =
        agent_hold_dict_from_py(scope.as_any(), "agent hold scope")?;
    let selectors: AgentHoldSelectorsWire =
        agent_hold_dict_from_py(selectors.as_any(), "agent hold selectors")?;
    let liveness = agent_hold_liveness_from_optional(liveness)?;
    let capture = agent_hold_capture_from_optional(capture)?;
    let record = core_arm_agent_hold_until(
        &PathBuf::from(sase_home),
        armer,
        scope,
        selectors,
        expires_at,
        &liveness,
        effort_override_now(now)?,
        capture,
    )
    .map_err(agent_hold_error_to_pyerr)?;
    agent_hold_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "agent_hold_rebind",
    signature = (sase_home, old_key, new_armer, liveness = None, now = None)
)]
fn py_agent_hold_rebind<'py>(
    py: Python<'py>,
    sase_home: &str,
    old_key: &str,
    new_armer: &Bound<'py, PyDict>,
    liveness: Option<&Bound<'py, PyDict>>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let new_armer: AgentHoldArmerWire =
        agent_hold_dict_from_py(new_armer.as_any(), "agent hold armer")?;
    let liveness = agent_hold_liveness_from_optional(liveness)?;
    match core_rebind_agent_hold_armer(
        &PathBuf::from(sase_home),
        old_key,
        new_armer,
        &liveness,
        effort_override_now(now)?,
    )
    .map_err(agent_hold_error_to_pyerr)?
    {
        Some(record) => agent_hold_wire_to_py(py, &record),
        None => Ok(py.None()),
    }
}

#[pyfunction]
#[pyo3(
    name = "agent_hold_release",
    signature = (sase_home, armer_key, liveness = None, now = None)
)]
fn py_agent_hold_release(
    sase_home: &str,
    armer_key: &str,
    liveness: Option<&Bound<'_, PyDict>>,
    now: Option<f64>,
) -> PyResult<bool> {
    let liveness = agent_hold_liveness_from_optional(liveness)?;
    core_release_agent_hold(
        &PathBuf::from(sase_home),
        armer_key,
        &liveness,
        effort_override_now(now)?,
    )
    .map_err(agent_hold_error_to_pyerr)
}

#[pyfunction]
#[pyo3(
    name = "agent_hold_list",
    signature = (sase_home, liveness = None, now = None)
)]
fn py_agent_hold_list<'py>(
    py: Python<'py>,
    sase_home: &str,
    liveness: Option<&Bound<'py, PyDict>>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let liveness = agent_hold_liveness_from_optional(liveness)?;
    let snapshot = core_list_agent_holds(
        &PathBuf::from(sase_home),
        &liveness,
        effort_override_now(now)?,
    )
    .map_err(agent_hold_error_to_pyerr)?;
    agent_hold_wire_to_py(py, &snapshot)
}

#[pyfunction]
#[pyo3(name = "agent_hold_blocks_candidate")]
fn py_agent_hold_blocks_candidate<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    candidate: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: AgentHoldRecordWire =
        agent_hold_dict_from_py(record.as_any(), "agent hold record")?;
    let candidate: AgentHoldCandidateWire =
        agent_hold_dict_from_py(candidate.as_any(), "agent hold candidate")?;
    match core_hold_blocks_candidate(&record, &candidate)
        .map_err(agent_hold_error_to_pyerr)?
    {
        Some(block) => agent_hold_wire_to_py(py, &block),
        None => Ok(py.None()),
    }
}

#[pyfunction]
#[pyo3(
    name = "agent_hold_summarize_capture",
    signature = (scope, identities, armer = None)
)]
fn py_agent_hold_summarize_capture<'py>(
    py: Python<'py>,
    scope: &Bound<'py, PyDict>,
    identities: &Bound<'py, PyList>,
    armer: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let scope: AgentHoldScopeWire =
        agent_hold_dict_from_py(scope.as_any(), "agent hold scope")?;
    let identities: Vec<AgentHoldCaptureIdentityWire> =
        serde_json::from_value(py_to_json_value(identities.as_any())?)
            .map_err(|error| {
                PyValueError::new_err(format!(
            "agent hold capture identities are not a valid list: {error}"
        ))
            })?;
    let armer = match armer {
        Some(value) => Some(agent_hold_dict_from_py::<AgentHoldArmerWire>(
            value.as_any(),
            "agent hold armer",
        )?),
        None => None,
    };
    let result =
        core_summarize_hold_capture(armer.as_ref(), &scope, &identities)
            .map_err(agent_hold_error_to_pyerr)?;
    agent_hold_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "agent_hold_deadlock_reaches")]
fn py_agent_hold_deadlock_reaches(
    start_artifact_dir: &str,
    candidate: &Bound<'_, PyDict>,
    nodes: &Bound<'_, PyList>,
) -> PyResult<bool> {
    let candidate: HoldDeadlockCandidateWire =
        agent_hold_dict_from_py(candidate.as_any(), "hold deadlock candidate")?;
    let nodes: Vec<HoldDeadlockWaitNodeWire> = serde_json::from_value(
        py_to_json_value(nodes.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "hold deadlock wait nodes are not a valid list: {error}"
        ))
    })?;
    Ok(core_hold_deadlock_reaches_candidate(
        start_artifact_dir,
        &candidate,
        &nodes,
    ))
}

pub(crate) fn register_agent_holds(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_agent_hold_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_arm_relative, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_arm_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_rebind, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_release, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_blocks_candidate, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_summarize_capture, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_hold_deadlock_reaches, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
