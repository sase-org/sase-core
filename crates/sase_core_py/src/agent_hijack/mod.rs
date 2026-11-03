//! PyO3 bindings for Agent Hijack state and eligibility policy.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::agent_auto_restart::AgentRecoveryWire;
use sase_core::agent_hijack::{
    advance_agent_hijack_record as core_advance_record,
    agent_hijack_eligibility as core_eligibility,
    agent_hijack_status_for_done_marker as core_done_status,
    agent_hijack_wire_schema_version as core_wire_version,
    AgentHijackEligibilityFactsWire, AgentHijackEventWire,
    AgentHijackRecordWire,
};

#[pyfunction]
#[pyo3(name = "agent_hijack_wire_schema_version")]
fn py_agent_hijack_wire_schema_version() -> u32 {
    core_wire_version()
}

#[pyfunction]
#[pyo3(name = "advance_agent_hijack_record")]
fn py_advance_agent_hijack_record<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    event: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: AgentHijackRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AgentHijackRecordWire dict: {error}"
        ))
    })?;
    let event: AgentHijackEventWire = serde_json::from_value(py_to_json_value(
        event.as_any(),
    )?)
    .map_err(|error| {
        PyValueError::new_err(format!(
            "event is not a valid AgentHijackEventWire dict: {error}"
        ))
    })?;
    let updated = core_advance_record(&record, &event)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &updated)
}

#[pyfunction]
#[pyo3(name = "agent_hijack_eligibility")]
fn py_agent_hijack_eligibility<'py>(
    py: Python<'py>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let facts: AgentHijackEligibilityFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid AgentHijackEligibilityFactsWire dict: {error}"
        ))
    })?;
    let decision = core_eligibility(&facts)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
}

#[pyfunction]
#[pyo3(name = "agent_hijack_status_for_done_marker")]
#[pyo3(signature = (outcome = None, recovery = None))]
fn py_agent_hijack_status_for_done_marker<'py>(
    outcome: Option<&str>,
    recovery: Option<&Bound<'py, PyDict>>,
) -> PyResult<Option<String>> {
    let recovery = recovery
        .map(|value| {
            serde_json::from_value::<AgentRecoveryWire>(py_to_json_value(
                value.as_any(),
            )?)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "recovery is not a valid AgentRecoveryWire dict: {error}"
                ))
            })
        })
        .transpose()?;
    Ok(core_done_status(outcome, recovery.as_ref()).map(str::to_string))
}

pub(crate) fn register_agent_hijack(
    module: &Bound<'_, PyModule>,
) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(
        py_agent_hijack_wire_schema_version,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_advance_agent_hijack_record,
        module
    )?)?;
    module
        .add_function(wrap_pyfunction!(py_agent_hijack_eligibility, module)?)?;
    module.add_function(wrap_pyfunction!(
        py_agent_hijack_status_for_done_marker,
        module
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_bridge::{json_value_to_py, py_to_json_value};
    use serde_json::json;

    fn dict<'py>(
        py: Python<'py>,
        value: serde_json::Value,
    ) -> Bound<'py, PyDict> {
        json_value_to_py(py, &value)
            .unwrap()
            .bind(py)
            .downcast::<PyDict>()
            .unwrap()
            .clone()
    }

    #[test]
    fn hijack_bindings_are_registered_and_advance_records() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            register_agent_hijack(&module).unwrap();
            assert_eq!(py_agent_hijack_wire_schema_version(), 1);
            assert!(module.getattr("agent_hijack_eligibility").is_ok());

            let record = dict(
                py,
                json!({
                    "schema_version": 1,
                    "state": "requested",
                    "revision": 0,
                }),
            );
            let event = dict(py, json!({"event": "begin_stop"}));
            let updated =
                py_advance_agent_hijack_record(py, &record, &event).unwrap();
            let value = py_to_json_value(updated.bind(py)).unwrap();
            assert_eq!(value["state"], json!("stopping"));
            assert_eq!(value["revision"], json!(1));
        });
    }

    #[test]
    fn eligibility_and_done_status_bindings_share_core_rules() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let facts = dict(
                py,
                json!({
                    "schema_version": 1,
                    "is_local": true,
                    "is_llm_agent": true,
                    "workspace_num": 4,
                    "status": "RUNNING",
                    "lifecycle_phase": "provider_running",
                    "has_hijack_capability": true,
                    "pid": 42,
                }),
            );
            let decision = py_agent_hijack_eligibility(py, &facts).unwrap();
            let value = py_to_json_value(decision.bind(py)).unwrap();
            assert_eq!(value["eligible"], json!(true));

            let recovery =
                dict(py, json!({"state": "resolved", "model_changed": true}));
            assert_eq!(
                py_agent_hijack_status_for_done_marker(
                    Some("hijacked"),
                    Some(&recovery),
                )
                .unwrap()
                .as_deref(),
                Some("ESCALATED")
            );
        });
    }
}
