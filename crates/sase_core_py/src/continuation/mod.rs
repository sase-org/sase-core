//! Monitor continuation contract bindings.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

// --- Monitor continuation contracts --------------------------------------
/// Return the continuation contract wire schema version.
#[pyfunction]
#[pyo3(name = "continuation_wire_schema_version")]
fn py_continuation_wire_schema_version() -> u32 {
    CONTINUATION_WIRE_SCHEMA_VERSION
}

/// Validate one continuation node and return its normalized wire shape.
#[pyfunction]
#[pyo3(name = "continuation_validate_node")]
fn py_continuation_validate_node<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(record.as_any())?;
    continuation_result_to_py(
        py,
        core_validate_continuation_node_value(value),
        "node validation",
    )
}

/// Validate a continuation node collection and summarize its edges.
#[pyfunction]
#[pyo3(name = "continuation_validate_graph")]
fn py_continuation_validate_graph<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let records: Vec<ContinuationNodeWire> =
        continuation_wire_from_pyany(records.as_any(), "records")?;
    continuation_result_to_py(
        py,
        core_validate_continuation_graph(records),
        "graph validation",
    )
}

/// Validate one agent-delta record.
#[pyfunction]
#[pyo3(name = "continuation_validate_agent_delta")]
fn py_continuation_validate_agent_delta<'py>(
    py: Python<'py>,
    delta: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let delta: AgentDeltaWire =
        continuation_wire_from_pydict(delta, "agent delta")?;
    continuation_result_to_py(
        py,
        core_validate_agent_delta(delta),
        "agent-delta validation",
    )
}

/// Validate one continuation intent record.
#[pyfunction]
#[pyo3(name = "continuation_validate_intent")]
fn py_continuation_validate_intent<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: ContinuationIntentWire =
        continuation_wire_from_pydict(intent, "continuation intent")?;
    continuation_result_to_py(
        py,
        core_validate_continuation_intent(intent),
        "intent validation",
    )
}

/// Validate one monitor-result record.
#[pyfunction]
#[pyo3(name = "continuation_validate_monitor_result")]
fn py_continuation_validate_monitor_result<'py>(
    py: Python<'py>,
    result: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let result: MonitorResultWire =
        continuation_wire_from_pydict(result, "monitor result")?;
    continuation_result_to_py(
        py,
        core_validate_monitor_result(result),
        "monitor-result validation",
    )
}

/// Validate one diagnostic-manifest record.
#[pyfunction]
#[pyo3(name = "continuation_validate_diagnostic_manifest")]
fn py_continuation_validate_diagnostic_manifest<'py>(
    py: Python<'py>,
    manifest: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let manifest: DiagnosticManifestWire =
        continuation_wire_from_pydict(manifest, "diagnostic manifest")?;
    continuation_result_to_py(
        py,
        core_validate_diagnostic_manifest(manifest),
        "diagnostic-manifest validation",
    )
}

/// Validate one mutable delivery record.
#[pyfunction]
#[pyo3(name = "continuation_validate_delivery_record")]
fn py_continuation_validate_delivery_record<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: ContinuationDeliveryRecordWire =
        continuation_wire_from_pydict(record, "delivery record")?;
    continuation_result_to_py(
        py,
        core_validate_continuation_delivery_record(record),
        "delivery-record validation",
    )
}

/// Create a pending continuation delivery record.
#[pyfunction]
#[pyo3(name = "continuation_new_delivery_record")]
fn py_continuation_new_delivery_record<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationDeliveryNewRequestWire =
        continuation_wire_from_pydict(request, "delivery new request")?;
    continuation_result_to_py(
        py,
        core_new_continuation_delivery_record(request),
        "delivery new record",
    )
}

/// Apply one pure continuation delivery transition.
#[pyfunction]
#[pyo3(name = "continuation_transition_delivery")]
fn py_continuation_transition_delivery<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationDeliveryTransitionRequestWire =
        continuation_wire_from_pydict(request, "delivery transition")?;
    continuation_result_to_py(
        py,
        core_transition_continuation_delivery(request),
        "delivery transition",
    )
}

/// Decide whether resume may fence undelivered branches and admit.
#[pyfunction]
#[pyo3(name = "continuation_decide_resume_adoption")]
fn py_continuation_decide_resume_adoption<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationResumeAdoptionRequestWire =
        continuation_wire_from_pydict(request, "resume adoption request")?;
    continuation_result_to_py(
        py,
        core_decide_resume_adoption(request),
        "resume adoption",
    )
}

/// Validate one LaunchApproval requester-continuation contract.
#[pyfunction]
#[pyo3(name = "continuation_validate_launch_requester_continuation")]
fn py_continuation_validate_launch_requester_continuation<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record: LaunchRequesterContinuationWire =
        continuation_wire_from_pydict(record, "launch requester continuation")?;
    continuation_result_to_py(
        py,
        core_validate_launch_requester_continuation(record),
        "launch requester continuation validation",
    )
}

/// Build a deterministic parent-first replay manifest.
#[pyfunction]
#[pyo3(name = "continuation_plan_replay")]
fn py_continuation_plan_replay<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationReplayPlanRequestWire =
        continuation_wire_from_pydict(request, "replay request")?;
    continuation_result_to_py(
        py,
        core_plan_continuation_replay(request),
        "replay planning",
    )
}

/// Compute the live/recoverable continuation ancestry retention closure.
#[pyfunction]
#[pyo3(name = "continuation_plan_retention")]
fn py_continuation_plan_retention<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationRetentionRequestWire =
        continuation_wire_from_pydict(request, "retention request")?;
    continuation_result_to_py(
        py,
        core_plan_continuation_retention(request),
        "retention planning",
    )
}

/// Select the monitor-result evidence projected into continuation context.
#[pyfunction]
#[pyo3(name = "continuation_select_evidence")]
fn py_continuation_select_evidence<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationEvidenceSelectionRequestWire =
        continuation_wire_from_pydict(request, "evidence request")?;
    continuation_result_to_py(
        py,
        core_select_continuation_evidence(request),
        "evidence selection",
    )
}

/// Resolve the next-action branch for a terminal monitor outcome.
#[pyfunction]
#[pyo3(name = "continuation_resolve_policy")]
fn py_continuation_resolve_policy<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationPolicyResolutionRequestWire =
        continuation_wire_from_pydict(request, "policy request")?;
    continuation_result_to_py(
        py,
        core_resolve_continuation_policy(request),
        "policy resolution",
    )
}

/// Validate and normalize a versioned outcome-policy object.
#[pyfunction]
#[pyo3(name = "continuation_validate_policy")]
fn py_continuation_validate_policy<'py>(
    py: Python<'py>,
    policy: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let policy: ContinuationOutcomePolicyWire =
        continuation_wire_from_pydict(policy, "outcome policy")?;
    continuation_result_to_py(
        py,
        core_validate_continuation_policy(policy),
        "policy validation",
    )
}

/// Freeze every outcome branch before monitor claim changes.
#[pyfunction]
#[pyo3(name = "continuation_freeze_policy")]
fn py_continuation_freeze_policy<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationPolicyFreezeRequestWire =
        continuation_wire_from_pydict(request, "policy freeze request")?;
    continuation_result_to_py(
        py,
        core_freeze_continuation_policy(request),
        "policy freeze",
    )
}

/// Decide whether an expanded continuation fits the provider budget.
#[pyfunction]
#[pyo3(name = "continuation_plan_budget")]
fn py_continuation_plan_budget<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ContinuationBudgetRequestWire =
        continuation_wire_from_pydict(request, "budget request")?;
    continuation_result_to_py(
        py,
        core_plan_continuation_budget(request),
        "budget planning",
    )
}

/// Validate one host-sealed conditional completion intent.
#[pyfunction]
#[pyo3(name = "continuation_validate_conditional_completion")]
fn py_continuation_validate_conditional_completion<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: ConditionalCompletionIntentWire =
        continuation_wire_from_pydict(intent, "conditional completion intent")?;
    continuation_result_to_py(
        py,
        core_validate_conditional_completion_intent(intent),
        "conditional completion validation",
    )
}

/// Seal a conditional completion intent from host observations.
#[pyfunction]
#[pyo3(name = "continuation_seal_conditional_completion")]
fn py_continuation_seal_conditional_completion<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionPrepareRequestWire =
        continuation_wire_from_pydict(
            request,
            "conditional completion prepare",
        )?;
    continuation_result_to_py(
        py,
        core_seal_conditional_completion(request),
        "conditional completion seal",
    )
}

/// Render a host-completion preview for a sealed intent.
#[pyfunction]
#[pyo3(name = "continuation_preview_conditional_completion")]
fn py_continuation_preview_conditional_completion<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: ConditionalCompletionIntentWire =
        continuation_wire_from_pydict(intent, "conditional completion intent")?;
    continuation_result_to_py(
        py,
        core_preview_conditional_completion(intent),
        "conditional completion preview",
    )
}

/// Bind a prepared intent to one monitor request (single-use).
#[pyfunction]
#[pyo3(name = "continuation_bind_conditional_completion")]
fn py_continuation_bind_conditional_completion<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionBindRequestWire =
        continuation_wire_from_pydict(request, "conditional completion bind")?;
    continuation_result_to_py(
        py,
        core_bind_conditional_completion(request),
        "conditional completion bind",
    )
}

/// Roll back a failed monitor-start binding so the intent is reusable.
#[pyfunction]
#[pyo3(name = "continuation_rollback_conditional_completion_binding")]
fn py_continuation_rollback_conditional_completion_binding<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionRollbackRequestWire =
        continuation_wire_from_pydict(
            request,
            "conditional completion rollback",
        )?;
    continuation_result_to_py(
        py,
        core_rollback_conditional_completion_binding(request),
        "conditional completion rollback",
    )
}

/// Evaluate whether a bound intent is eligible for no-model host completion.
#[pyfunction]
#[pyo3(name = "continuation_evaluate_conditional_completion")]
fn py_continuation_evaluate_conditional_completion<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionEvaluateRequestWire =
        continuation_wire_from_pydict(
            request,
            "conditional completion evaluate",
        )?;
    continuation_result_to_py(
        py,
        core_evaluate_conditional_completion(request),
        "conditional completion evaluate",
    )
}

/// Mark a bound intent consumed after successful host completion.
#[pyfunction]
#[pyo3(name = "continuation_consume_conditional_completion")]
fn py_continuation_consume_conditional_completion<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionConsumeRequestWire =
        continuation_wire_from_pydict(
            request,
            "conditional completion consume",
        )?;
    continuation_result_to_py(
        py,
        core_consume_conditional_completion(request),
        "conditional completion consume",
    )
}

/// Invalidate a bound intent that cannot complete and must recover.
#[pyfunction]
#[pyo3(name = "continuation_invalidate_conditional_completion")]
fn py_continuation_invalidate_conditional_completion<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionConsumeRequestWire =
        continuation_wire_from_pydict(
            request,
            "conditional completion invalidate",
        )?;
    continuation_result_to_py(
        py,
        core_invalidate_conditional_completion(request),
        "conditional completion invalidate",
    )
}

/// Render a prepared success message with documented host-fact substitutions.
#[pyfunction]
#[pyo3(name = "continuation_render_conditional_completion_message")]
fn py_continuation_render_conditional_completion_message<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ConditionalCompletionMessageRequestWire =
        continuation_wire_from_pydict(
            request,
            "conditional completion message",
        )?;
    continuation_result_to_py(
        py,
        core_render_conditional_completion_message(request),
        "conditional completion message",
    )
}

fn continuation_error_to_pyerr(error: ContinuationError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn continuation_wire_from_pydict<T>(
    dict: &Bound<'_, PyDict>,
    what: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    continuation_wire_from_pyany(dict.as_any(), what)
}

fn continuation_wire_from_pyany<T>(
    value: &Bound<'_, PyAny>,
    what: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!(
            "{what} is not a valid continuation wire value: {error}"
        ))
    })
}

fn continuation_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, ContinuationError>,
    operation: &str,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value =
        serde_json::to_value(result.map_err(continuation_error_to_pyerr)?)
            .map_err(|error| {
                PyValueError::new_err(format!(
                "internal continuation {operation} serialize error: {error}"
            ))
            })?;
    json_value_to_py(py, &value)
}

/// Serialize an optional wire record to Python, or `None`.
pub(crate) fn optional_wire_to_py<T: serde::Serialize>(
    py: Python<'_>,
    value: Option<T>,
) -> PyResult<PyObject> {
    let Some(value) = value else {
        return Ok(py.None());
    };
    let json = serde_json::to_value(value).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

pub(crate) fn register_continuation(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_continuation_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_validate_node, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_validate_graph, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_validate_agent_delta, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_validate_intent, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_validate_monitor_result,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_validate_diagnostic_manifest,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_validate_delivery_record,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_continuation_new_delivery_record, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_transition_delivery, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_decide_resume_adoption,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_validate_launch_requester_continuation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_continuation_plan_replay, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_plan_retention, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_select_evidence, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_resolve_policy, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_validate_policy, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_freeze_policy, m)?)?;
    m.add_function(wrap_pyfunction!(py_continuation_plan_budget, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_validate_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_seal_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_preview_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_bind_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_rollback_conditional_completion_binding,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_evaluate_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_consume_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_invalidate_conditional_completion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_continuation_render_conditional_completion_message,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
