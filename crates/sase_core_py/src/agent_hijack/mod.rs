//! PyO3 bindings for Agent Hijack state, eligibility, escalation,
//! continuation, recovery preview, execution, and reconciliation policy.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::agent_auto_restart::AgentRecoveryWire;
use sase_core::agent_hijack::{
    advance_agent_hijack_record as core_advance_record,
    agent_hijack_continuation_prompt as core_continuation,
    agent_hijack_eligibility as core_eligibility,
    agent_hijack_escalation_target as core_escalation,
    agent_hijack_status_for_done_marker as core_done_status,
    agent_hijack_wire_schema_version as core_wire_version,
    hijack_recovery_confirm as core_confirm,
    hijack_recovery_effective_target as core_effective_target,
    hijack_recovery_preview as core_preview,
    hijack_recovery_reconcile as core_reconcile,
    hijack_recovery_rollback as core_rollback,
    hijack_recovery_validate_execution as core_validate_execution,
    normalize_hijack_rung as core_normalize_rung,
    strip_launch_directives as core_strip, AgentHijackConfirmationFactsWire,
    AgentHijackContinuationRequestWire, AgentHijackEffectiveTargetRequestWire,
    AgentHijackEligibilityFactsWire, AgentHijackEscalationRequestWire,
    AgentHijackEventWire, AgentHijackExecutionFactsWire,
    AgentHijackReconcileFactsWire, AgentHijackRecordWire,
    AgentHijackRecoveryPreviewRequestWire, AgentHijackRollbackFactsWire,
    AGENT_HIJACK_CONTINUATION_WIRE_SCHEMA_VERSION,
    AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
    AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION,
    AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION,
    AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION,
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

/// Return the hijack escalation wire schema version.
#[pyfunction]
#[pyo3(name = "agent_hijack_escalation_wire_schema_version")]
fn py_agent_hijack_escalation_wire_schema_version() -> u32 {
    AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION
}

/// Return the hijack continuation wire schema version.
#[pyfunction]
#[pyo3(name = "agent_hijack_continuation_wire_schema_version")]
fn py_agent_hijack_continuation_wire_schema_version() -> u32 {
    AGENT_HIJACK_CONTINUATION_WIRE_SCHEMA_VERSION
}

/// Normalize a ladder rung to `@`-prefixed canonical form.
#[pyfunction]
#[pyo3(name = "normalize_hijack_rung")]
fn py_normalize_hijack_rung(value: &str) -> String {
    core_normalize_rung(value)
}

/// Compute the never-downgrade escalation target for a hijacked run.
#[pyfunction]
#[pyo3(name = "agent_hijack_escalation_target")]
fn py_agent_hijack_escalation_target<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentHijackEscalationRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid AgentHijackEscalationRequestWire dict: {error}"
        ))
    })?;
    let result = py.allow_threads(|| core_escalation(&request));
    serialize_to_py(py, &result)
}

/// Build the continuation prompt for a hijack successor.
#[pyfunction]
#[pyo3(name = "agent_hijack_continuation_prompt")]
fn py_agent_hijack_continuation_prompt<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentHijackContinuationRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid AgentHijackContinuationRequestWire dict: {error}"
        ))
    })?;
    let prompt = py.allow_threads(|| core_continuation(&request));
    serialize_to_py(py, &prompt)
}

/// Strip launch-only directives, preserving fenced code blocks.
#[pyfunction]
#[pyo3(name = "strip_hijack_launch_directives")]
fn py_strip_hijack_launch_directives(prompt: &str) -> String {
    core_strip(prompt)
}

/// Return the hijack recovery preview wire schema version.
#[pyfunction]
#[pyo3(name = "hijack_recovery_preview_wire_schema_version")]
fn py_hijack_recovery_preview_wire_schema_version() -> u32 {
    AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION
}

/// Assemble a side-effect-free keep-work recovery preview.
#[pyfunction]
#[pyo3(name = "hijack_recovery_preview")]
fn py_hijack_recovery_preview<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentHijackRecoveryPreviewRequestWire =
        serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
            |error| {
                PyValueError::new_err(format!(
                    "request is not a valid AgentHijackRecoveryPreviewRequestWire dict: {error}"
                ))
            },
        )?;
    let preview = py
        .allow_threads(|| core_preview(&request))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &preview)
}

/// Return the hijack recovery reconcile wire schema version.
#[pyfunction]
#[pyo3(name = "hijack_recovery_reconcile_wire_schema_version")]
fn py_hijack_recovery_reconcile_wire_schema_version() -> u32 {
    AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION
}

/// Decide how to repair an abandoned resolving hijack record.
#[pyfunction]
#[pyo3(name = "hijack_recovery_reconcile")]
fn py_hijack_recovery_reconcile<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: AgentHijackRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AgentHijackRecordWire dict: {error}"
        ))
    })?;
    let facts: AgentHijackReconcileFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid AgentHijackReconcileFactsWire dict: {error}"
        ))
    })?;
    let decision = py
        .allow_threads(|| core_reconcile(&record, &facts))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
}

/// Return the hijack recovery execution wire schema version.
#[pyfunction]
#[pyo3(name = "hijack_recovery_execution_wire_schema_version")]
fn py_hijack_recovery_execution_wire_schema_version() -> u32 {
    AGENT_HIJACK_EXECUTION_WIRE_SCHEMA_VERSION
}

/// Resolve the effective concrete target under the shared precedence.
#[pyfunction]
#[pyo3(name = "hijack_recovery_effective_target")]
fn py_hijack_recovery_effective_target<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentHijackEffectiveTargetRequestWire =
        serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
            |error| {
                PyValueError::new_err(format!(
                    "request is not a valid AgentHijackEffectiveTargetRequestWire dict: {error}"
                ))
            },
        )?;
    let result = py
        .allow_threads(|| core_effective_target(&request))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &result)
}

/// Validate the winning reservation without reopening the public menu.
#[pyfunction]
#[pyo3(name = "hijack_recovery_validate_execution")]
fn py_hijack_recovery_validate_execution<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    pinned_target: &str,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: AgentHijackRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AgentHijackRecordWire dict: {error}"
        ))
    })?;
    let facts: AgentHijackExecutionFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid AgentHijackExecutionFactsWire dict: {error}"
        ))
    })?;
    let decision = py
        .allow_threads(|| {
            core_validate_execution(&record, pinned_target, &facts)
        })
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
}

/// Decide whether the observed successor proves settlement.
#[pyfunction]
#[pyo3(name = "hijack_recovery_confirm")]
fn py_hijack_recovery_confirm<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: AgentHijackRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AgentHijackRecordWire dict: {error}"
        ))
    })?;
    let facts: AgentHijackConfirmationFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid AgentHijackConfirmationFactsWire dict: {error}"
        ))
    })?;
    let decision = py
        .allow_threads(|| core_confirm(&record, &facts))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
}

/// Decide whether the record may safely return to held.
#[pyfunction]
#[pyo3(name = "hijack_recovery_rollback")]
fn py_hijack_recovery_rollback<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: AgentHijackRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AgentHijackRecordWire dict: {error}"
        ))
    })?;
    let facts: AgentHijackRollbackFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid AgentHijackRollbackFactsWire dict: {error}"
        ))
    })?;
    let decision = py
        .allow_threads(|| core_rollback(&record, &facts))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &decision)
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
    module.add_function(wrap_pyfunction!(
        py_agent_hijack_escalation_wire_schema_version,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_agent_hijack_continuation_wire_schema_version,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(py_normalize_hijack_rung, module)?)?;
    module.add_function(wrap_pyfunction!(
        py_agent_hijack_escalation_target,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_agent_hijack_continuation_prompt,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_strip_hijack_launch_directives,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_hijack_recovery_preview_wire_schema_version,
        module
    )?)?;
    module
        .add_function(wrap_pyfunction!(py_hijack_recovery_preview, module)?)?;
    module.add_function(wrap_pyfunction!(
        py_hijack_recovery_reconcile_wire_schema_version,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_hijack_recovery_reconcile,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_hijack_recovery_execution_wire_schema_version,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_hijack_recovery_effective_target,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_hijack_recovery_validate_execution,
        module
    )?)?;
    module
        .add_function(wrap_pyfunction!(py_hijack_recovery_confirm, module)?)?;
    module
        .add_function(wrap_pyfunction!(py_hijack_recovery_rollback, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
