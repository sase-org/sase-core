//! Update-skew auto-restart bindings: classifier, ledger, episodes.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::agent_auto_restart::{
    advance_auto_restart_ledger as core_advance_ledger,
    agent_auto_restart_wire_schema_version as core_wire_version,
    auto_restart_lineage_root as core_lineage_root,
    auto_restart_recovery_is_in_flight as core_recovery_is_in_flight,
    claim_auto_restart_ledger as core_claim_ledger,
    classify_agent_failure as core_classify,
    derive_auto_restart_episode as core_derive_episode, AgentFailureFactsWire,
    AutoRestartContextWire, AutoRestartLedgerRecordWire,
    AutoRestartWitnessesWire,
};

/// Return the auto-restart wire schema version.
#[pyfunction]
#[pyo3(name = "agent_auto_restart_wire_schema_version")]
fn py_agent_auto_restart_wire_schema_version() -> u32 {
    core_wire_version()
}

/// Classify one failed agent into a recovery verdict dict.
///
/// `facts` is the `AgentFailureFactsWire` dict or `None` for legacy
/// rows; `context` and `witnesses` mirror their wire dicts.
#[pyfunction]
#[pyo3(name = "classify_agent_failure", signature = (facts, context, witnesses))]
fn py_classify_agent_failure<'py>(
    py: Python<'py>,
    facts: Option<&Bound<'py, PyDict>>,
    context: &Bound<'py, PyDict>,
    witnesses: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let facts: Option<AgentFailureFactsWire> = facts
        .map(|facts| {
            serde_json::from_value(py_to_json_value(facts.as_any())?)
                .map_err(|error| {
                    PyValueError::new_err(format!(
                        "facts is not a valid AgentFailureFactsWire dict: {error}"
                    ))
                })
        })
        .transpose()?;
    let context: AutoRestartContextWire = serde_json::from_value(
        py_to_json_value(context.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "context is not a valid AutoRestartContextWire dict: {error}"
        ))
    })?;
    let witnesses: AutoRestartWitnessesWire = serde_json::from_value(
        py_to_json_value(witnesses.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "witnesses is not a valid AutoRestartWitnessesWire dict: {error}"
        ))
    })?;
    let verdict = py
        .allow_threads(|| core_classify(facts.as_ref(), &context, &witnesses));
    serialize_to_py(py, &verdict)
}

/// Advance one ledger record; illegal transitions raise `ValueError`.
#[pyfunction]
#[pyo3(name = "advance_auto_restart_ledger")]
fn py_advance_auto_restart_ledger<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    event: &str,
) -> PyResult<PyObject> {
    let record: AutoRestartLedgerRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AutoRestartLedgerRecordWire dict: {error}"
        ))
    })?;
    let updated = core_advance_ledger(&record, event)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &updated)
}

/// Create a freshly claimed ledger record for one lineage.
#[pyfunction]
#[pyo3(name = "claim_auto_restart_ledger")]
fn py_claim_auto_restart_ledger<'py>(
    py: Python<'py>,
    key: &str,
    lineage_root: &str,
) -> PyResult<PyObject> {
    let record = core_claim_ledger(key, lineage_root);
    serialize_to_py(py, &record)
}

/// Derive the lineage root from meta, chain, and row timestamps.
#[pyfunction]
#[pyo3(
    name = "auto_restart_lineage_root",
    signature = (
        artifacts_timestamp,
        auto_restart_lineage_root = None,
        retry_chain_root_timestamp = None,
    )
)]
fn py_auto_restart_lineage_root(
    artifacts_timestamp: &str,
    auto_restart_lineage_root: Option<&str>,
    retry_chain_root_timestamp: Option<&str>,
) -> String {
    core_lineage_root(
        auto_restart_lineage_root,
        retry_chain_root_timestamp,
        artifacts_timestamp,
    )
}

/// Derive the episode identity for one witness bundle.
#[pyfunction]
#[pyo3(name = "derive_auto_restart_episode")]
fn py_derive_auto_restart_episode<'py>(
    py: Python<'py>,
    witnesses: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let witnesses: AutoRestartWitnessesWire = serde_json::from_value(
        py_to_json_value(witnesses.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "witnesses is not a valid AutoRestartWitnessesWire dict: {error}"
        ))
    })?;
    let episode = core_derive_episode(&witnesses);
    serialize_to_py(py, &episode)
}

/// Return whether a done-marker recovery state renders as restarting.
#[pyfunction]
#[pyo3(
    name = "auto_restart_recovery_is_in_flight",
    signature = (state = None)
)]
fn py_auto_restart_recovery_is_in_flight(state: Option<&str>) -> bool {
    core_recovery_is_in_flight(state)
}

pub(crate) fn register_agent_auto_restart(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_agent_auto_restart_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_classify_agent_failure, m)?)?;
    m.add_function(wrap_pyfunction!(py_advance_auto_restart_ledger, m)?)?;
    m.add_function(wrap_pyfunction!(py_claim_auto_restart_ledger, m)?)?;
    m.add_function(wrap_pyfunction!(py_auto_restart_lineage_root, m)?)?;
    m.add_function(wrap_pyfunction!(py_derive_auto_restart_episode, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_auto_restart_recovery_is_in_flight,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
