//! Bead policy bindings: actions, finalizers, gate decisions, and task types.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

// --- Phase 4C status state machine bindings -------------------------------
/// Strip workspace and legacy READY-TO-MAIL suffixes from a STATUS string.
///
/// Mirrors `sase.status_state_machine.constants.remove_workspace_suffix`.
/// Useful for tests and for callers that want the canonical base status
/// without going through the planner.
#[pyfunction]
#[pyo3(name = "remove_workspace_suffix")]
fn py_remove_workspace_suffix(status: &str) -> String {
    core_remove_workspace_suffix(status)
}

/// Whether a transition from *from_status* to *to_status* is allowed.
///
/// Mirrors `sase.status_state_machine.constants.is_valid_transition`.
/// Workspace suffixes on either side are stripped before validation.
#[pyfunction]
#[pyo3(name = "is_valid_status_transition")]
fn py_is_valid_status_transition(from_status: &str, to_status: &str) -> bool {
    core_is_valid_transition(from_status, to_status)
}

/// Read the STATUS for the requested Patch name from a list of project-file lines.
///
/// Mirrors `sase.status_state_machine.field_updates.read_status_from_lines_python`.
/// Returns `None` when the Patch is not present.
#[pyfunction]
#[pyo3(name = "read_status_from_lines")]
fn py_read_status_from_lines<'py>(
    py: Python<'py>,
    lines: &Bound<'py, PyList>,
    // Legacy Python keyword retained for compatibility.
    changespec_name: &str,
) -> PyResult<PyObject> {
    let mut owned: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, item) in lines.iter().enumerate() {
        let s: String = item.extract().map_err(|_| {
            PyValueError::new_err(format!("lines[{idx}] must be a string"))
        })?;
        owned.push(s);
    }
    let result = core_read_status_from_lines(&owned, changespec_name);
    Ok(match result {
        Some(s) => s.into_py(py),
        None => py.None(),
    })
}

/// Apply a STATUS update to a list of project-file lines and return the
/// updated content as a single string.
///
/// Mirrors `sase.status_state_machine.field_updates.apply_status_update_python`.
#[pyfunction]
#[pyo3(name = "apply_status_update")]
fn py_apply_status_update<'py>(
    lines: &Bound<'py, PyList>,
    // Legacy Python keyword retained for compatibility.
    changespec_name: &str,
    new_status: &str,
) -> PyResult<String> {
    let mut owned: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, item) in lines.iter().enumerate() {
        let s: String = item.extract().map_err(|_| {
            PyValueError::new_err(format!("lines[{idx}] must be a string"))
        })?;
        owned.push(s);
    }
    Ok(core_apply_status_update(
        &owned,
        changespec_name,
        new_status,
    ))
}

/// Plan a status transition for one Patch.
///
/// *request* must be a `StatusTransitionRequestWire`-shape dict (see
/// `sase.core.status_wire`). The result is a
/// `StatusTransitionPlanWire`-shape dict — the Python adapter rehydrates
/// it via `status_plan_from_dict`.
///
/// Schema-version mismatches and structurally invalid requests surface as
/// `ValueError` so the existing UI validation layer can catch them.
#[pyfunction]
#[pyo3(name = "plan_status_transition")]
fn py_plan_status_transition<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: StatusTransitionRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid StatusTransitionRequestWire dict: {e}"
            ))
        })?;
    let plan =
        core_plan_status_transition(&req).map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn finalizer_error_to_pyerr(error: FinalizerError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn finalizer_wire_from_pydict<T>(
    dict: &Bound<'_, PyDict>,
    what: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "{what} is not a valid finalizer wire dict: {error}"
        ))
    })
}

fn finalizer_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, FinalizerError>,
    operation: &str,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result.map_err(finalizer_error_to_pyerr)?)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal finalizer {operation} serialize error: {error}"
            ))
        })?;
    json_value_to_py(py, &value)
}

/// Return the finalizer protocol wire schema version.
#[pyfunction]
#[pyo3(name = "finalizer_wire_schema_version")]
fn py_finalizer_wire_schema_version() -> u64 {
    FINALIZER_WIRE_SCHEMA_VERSION
}

/// Validate one provider capability/spec wire.
#[pyfunction]
#[pyo3(name = "validate_finalizer_provider_spec")]
fn py_validate_finalizer_provider_spec(
    spec: &Bound<'_, PyDict>,
) -> PyResult<()> {
    let spec: FinalizerProviderSpecWire =
        finalizer_wire_from_pydict(spec, "provider spec")?;
    core_validate_finalizer_provider_spec(&spec)
        .map_err(finalizer_error_to_pyerr)
}

/// Compute the canonical sha256 digest for one provider spec.
#[pyfunction]
#[pyo3(name = "finalizer_provider_spec_digest")]
fn py_finalizer_provider_spec_digest(
    spec: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let spec: FinalizerProviderSpecWire =
        finalizer_wire_from_pydict(spec, "provider spec")?;
    core_finalizer_provider_spec_digest(&spec).map_err(finalizer_error_to_pyerr)
}

/// Validate one configured finalizer instance.
#[pyfunction]
#[pyo3(name = "validate_finalizer_instance_spec")]
fn py_validate_finalizer_instance_spec(
    spec: &Bound<'_, PyDict>,
) -> PyResult<()> {
    let spec: FinalizerInstanceSpecWire =
        finalizer_wire_from_pydict(spec, "instance spec")?;
    core_validate_finalizer_instance_spec(&spec)
        .map_err(finalizer_error_to_pyerr)
}

/// Compute the canonical sha256 digest for one instance spec.
#[pyfunction]
#[pyo3(name = "finalizer_instance_spec_digest")]
fn py_finalizer_instance_spec_digest(
    spec: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let spec: FinalizerInstanceSpecWire =
        finalizer_wire_from_pydict(spec, "instance spec")?;
    core_finalizer_instance_spec_digest(&spec).map_err(finalizer_error_to_pyerr)
}

/// Resolve defaults, required instances, selectors, and dependencies.
#[pyfunction]
#[pyo3(name = "resolve_finalizer_plan")]
fn py_resolve_finalizer_plan<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: FinalizerPlanInputWire =
        finalizer_wire_from_pydict(request, "plan input")?;
    finalizer_result_to_py(
        py,
        core_resolve_finalizer_plan(&request),
        "plan resolution",
    )
}

/// Compute the canonical digest for a resolved plan.
#[pyfunction]
#[pyo3(name = "finalizer_plan_digest")]
fn py_finalizer_plan_digest(plan: &Bound<'_, PyDict>) -> PyResult<String> {
    let plan: FinalizerPlanWire = finalizer_wire_from_pydict(plan, "plan")?;
    core_finalizer_plan_digest(&plan).map_err(finalizer_error_to_pyerr)
}

/// Strictly parse and validate a resolved plan, returning its digest.
#[pyfunction]
#[pyo3(name = "validate_finalizer_plan")]
fn py_validate_finalizer_plan(plan: &Bound<'_, PyDict>) -> PyResult<String> {
    let plan: FinalizerPlanWire = finalizer_wire_from_pydict(plan, "plan")?;
    core_validate_finalizer_plan(&plan).map_err(finalizer_error_to_pyerr)
}

/// Validate a plan and bind it to an independently held expected digest.
#[pyfunction]
#[pyo3(name = "authenticate_finalizer_plan")]
fn py_authenticate_finalizer_plan(
    plan: &Bound<'_, PyDict>,
    expected_digest: &str,
) -> PyResult<String> {
    let plan: FinalizerPlanWire = finalizer_wire_from_pydict(plan, "plan")?;
    core_authenticate_finalizer_plan(&plan, expected_digest)
        .map_err(finalizer_error_to_pyerr)
}

/// Compute the canonical digest for a context, excluding context_digest.
#[pyfunction]
#[pyo3(name = "finalizer_context_digest")]
fn py_finalizer_context_digest(
    context: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let context: FinalizerContextWire =
        finalizer_wire_from_pydict(context, "context")?;
    core_finalizer_context_digest(&context).map_err(finalizer_error_to_pyerr)
}

/// Validate a context against a resolved plan and return its digest.
#[pyfunction]
#[pyo3(name = "validate_finalizer_context")]
fn py_validate_finalizer_context(
    plan: &Bound<'_, PyDict>,
    context: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let plan: FinalizerPlanWire = finalizer_wire_from_pydict(plan, "plan")?;
    let context: FinalizerContextWire =
        finalizer_wire_from_pydict(context, "context")?;
    core_validate_finalizer_context(&plan, &context)
        .map_err(finalizer_error_to_pyerr)
}

/// Select remaining repository obligations after conflict repair.
#[pyfunction]
#[pyo3(name = "select_remaining_commit_obligations")]
fn py_select_remaining_commit_obligations<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: RemainingCommitWorkRequestWire =
        finalizer_wire_from_pydict(request, "remaining commit work request")?;
    finalizer_result_to_py(
        py,
        Ok(core_select_remaining_commit_obligations(&request)),
        "remaining commit work",
    )
}

/// Validate a submission against a resolved plan/context and return a summary.
#[pyfunction]
#[pyo3(name = "validate_finalizer_submission")]
fn py_validate_finalizer_submission<'py>(
    py: Python<'py>,
    plan: &Bound<'_, PyDict>,
    context: &Bound<'_, PyDict>,
    submission: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let plan: FinalizerPlanWire = finalizer_wire_from_pydict(plan, "plan")?;
    let context: FinalizerContextWire =
        finalizer_wire_from_pydict(context, "context")?;
    let submission: FinalizerSubmissionEnvelopeWire =
        finalizer_wire_from_pydict(submission, "submission")?;
    finalizer_result_to_py(
        py,
        core_validate_finalizer_submission(&plan, &context, &submission),
        "submission validation",
    )
}

/// Compute a canonical sha256 digest for an arbitrary JSON-shaped value.
#[pyfunction]
#[pyo3(name = "finalizer_json_digest")]
fn py_finalizer_json_digest(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let value = py_to_json_value(value)?;
    core_finalizer_digest_json_value(&value).map_err(finalizer_error_to_pyerr)
}

/// Aggregate per-instance finalizer results into a terminal run status.
#[pyfunction]
#[pyo3(name = "aggregate_finalizer_outcomes")]
fn py_aggregate_finalizer_outcomes<'py>(
    py: Python<'py>,
    results: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let results = serde_json::from_value::<Vec<FinalizerInstanceResultWire>>(
        py_to_json_value(results.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "results are not valid finalizer instance-result dicts: {error}"
        ))
    })?;
    finalizer_result_to_py(
        py,
        core_aggregate_finalizer_outcomes(results),
        "outcome aggregation",
    )
}

fn bead_action_error_to_pyerr(error: BeadActionError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn bead_action_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, BeadActionError>,
    operation: &str,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value =
        serde_json::to_value(result.map_err(bead_action_error_to_pyerr)?)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "internal bead-action {operation} serialize error: {error}"
                ))
            })?;
    json_value_to_py(py, &value)
}

/// Return the bead-action policy wire schema version.
#[pyfunction]
#[pyo3(name = "bead_action_wire_schema_version")]
fn py_bead_action_wire_schema_version() -> u64 {
    BEAD_ACTION_WIRE_SCHEMA_VERSION
}

/// Parse `bead_action` from a payload, preserving omitted vs explicit.
#[pyfunction]
#[pyo3(name = "parse_bead_action_field")]
fn py_parse_bead_action_field(
    payload: &Bound<'_, PyDict>,
) -> PyResult<Option<String>> {
    let value = py_to_json_value(payload.as_any())?;
    match core_parse_bead_action_field(&value)
        .map_err(bead_action_error_to_pyerr)?
    {
        Some(action) => Ok(Some(action.as_str().to_string())),
        None => Ok(None),
    }
}

/// Decide stitch/commit bead-action disposition from host-collected facts.
#[pyfunction]
#[pyo3(name = "decide_bead_action")]
fn py_decide_bead_action<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    bead_action_result_to_py(
        py,
        core_decide_bead_action_from_json(&value),
        "policy",
    )
}

/// Validate one repository decision against authenticated bead context.
#[pyfunction]
#[pyo3(name = "validate_finalizer_bead_decision")]
fn py_validate_finalizer_bead_decision<'py>(
    py: Python<'py>,
    context: &Bound<'_, PyDict>,
    decision: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let context = py_to_json_value(context.as_any())?;
    let decision = py_to_json_value(decision.as_any())?;
    bead_action_result_to_py(
        py,
        core_validate_finalizer_bead_decision_from_json(&context, &decision),
        "finalizer decision",
    )
}

/// Reject a declaration bound to a different assigned bead.
#[pyfunction]
#[pyo3(
    name = "validate_finalizer_assigned_bead_binding",
    signature = (context, expected=None)
)]
fn py_validate_finalizer_assigned_bead_binding(
    context: &Bound<'_, PyDict>,
    expected: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let context: FinalizerContextWire =
        finalizer_wire_from_pydict(context, "context")?;
    let expected = match expected {
        Some(expected) => Some(finalizer_wire_from_pydict::<
            FinalizerAssignedBeadWire,
        >(expected, "assigned bead")?),
        None => None,
    };
    core_validate_finalizer_assigned_bead_binding(&context, expected.as_ref())
        .map_err(bead_action_error_to_pyerr)
}

fn gate_decision_error_to_pyerr(error: GateDecisionError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn gate_decision_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, GateDecisionError>,
    operation: &str,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let decided = result.map_err(gate_decision_error_to_pyerr)?;
    let value = serde_json::to_value(decided).map_err(|error| {
        PyValueError::new_err(format!(
            "internal gate-decision {operation} serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the gate-decision-acceptance wire schema version.
#[pyfunction]
#[pyo3(name = "gate_decision_wire_schema_version")]
fn py_gate_decision_wire_schema_version() -> u32 {
    GATE_DECISION_WIRE_SCHEMA_VERSION
}

/// Decide one gate's decision acceptance: a fresh accept, an idempotent
/// replay of an identical resubmission, or a prompt conflict rejection
/// raised as a Python `ValueError` -- before any option command, archive,
/// or launch work runs.
#[pyfunction]
#[pyo3(name = "decide_gate_decision_acceptance")]
fn py_decide_gate_decision_acceptance<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    gate_decision_result_to_py(
        py,
        core_decide_gate_decision_acceptance_from_json(&value),
        "acceptance",
    )
}

/// Re-own one still-current accepted gate decision before execution starts.
#[pyfunction]
#[pyo3(name = "claim_gate_decision_execution")]
fn py_claim_gate_decision_execution<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    gate_decision_result_to_py(
        py,
        core_claim_gate_decision_execution_from_json(&value),
        "execution claim",
    )
}

/// Return the gate-lifecycle wire schema version.
#[pyfunction]
#[pyo3(name = "gate_lifecycle_wire_schema_version")]
fn py_gate_lifecycle_wire_schema_version() -> u32 {
    GATE_LIFECYCLE_WIRE_SCHEMA_VERSION
}

/// Classify one gate's current lifecycle disposition (answered, cancelled,
/// accepted with execution still incomplete, pending, or past its review
/// deadline or reclaim grace window) from host-collected evidence. Raises a
/// Python `ValueError` when a decision receipt is unreadable or names a
/// different gate or request -- reported explicitly rather than silently
/// treated as an unanswered gate eligible for cleanup.
#[pyfunction]
#[pyo3(name = "decide_gate_lifecycle")]
fn py_decide_gate_lifecycle<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    gate_decision_result_to_py(
        py,
        core_decide_gate_lifecycle_from_json(&value),
        "lifecycle",
    )
}

fn task_type_spec_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<TaskTypeSpecWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "spec is not a valid TaskTypeSpecWire dict: {error}"
        ))
    })
}

fn task_type_error_to_pyerr(error: TaskTypeError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Validate one assembled task-type spec.
#[pyfunction]
#[pyo3(name = "validate_task_type_spec")]
fn py_validate_task_type_spec(spec: &Bound<'_, PyDict>) -> PyResult<()> {
    let spec = task_type_spec_from_pydict(spec)?;
    core_validate_task_type_spec(&spec).map_err(task_type_error_to_pyerr)
}

/// Compute a stable sha256 hex digest over the normalized task-type spec.
#[pyfunction]
#[pyo3(name = "task_type_spec_digest")]
fn py_task_type_spec_digest(spec: &Bound<'_, PyDict>) -> PyResult<String> {
    let spec = task_type_spec_from_pydict(spec)?;
    core_task_type_spec_digest(&spec).map_err(task_type_error_to_pyerr)
}

/// Validate field values against a spec.
///
/// Returns one typed error dict per problem. An empty list means the values
/// are valid. An invalid spec is raised as `ValueError`.
#[pyfunction]
#[pyo3(name = "validate_task_type_field_values")]
fn py_validate_task_type_field_values<'py>(
    py: Python<'py>,
    spec: &Bound<'_, PyDict>,
    values: BTreeMap<String, String>,
) -> PyResult<PyObject> {
    let spec = task_type_spec_from_pydict(spec)?;
    let errors = core_validate_task_type_field_values(&spec, &values)
        .map_err(task_type_error_to_pyerr)?;
    let value = serde_json::to_value(errors).map_err(|error| {
        PyValueError::new_err(format!(
            "internal task type field-value serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Render the Markdown body block for a spec and its field values.
#[pyfunction]
#[pyo3(name = "render_task_type_body")]
fn py_render_task_type_body(
    spec: &Bound<'_, PyDict>,
    values: BTreeMap<String, String>,
) -> PyResult<String> {
    let spec = task_type_spec_from_pydict(spec)?;
    core_render_task_type_body(&spec, &values).map_err(task_type_error_to_pyerr)
}

/// Parse a committed task-type catalog snapshot.
#[pyfunction]
#[pyo3(name = "parse_task_type_snapshot")]
fn py_parse_task_type_snapshot<'py>(
    py: Python<'py>,
    data: &str,
) -> PyResult<PyObject> {
    let snapshot = core_parse_task_type_snapshot(data)
        .map_err(task_type_error_to_pyerr)?;
    let value = serde_json::to_value(snapshot).map_err(|error| {
        PyValueError::new_err(format!(
            "internal task type snapshot serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Serialize a committed task-type catalog snapshot deterministically.
#[pyfunction]
#[pyo3(name = "serialize_task_type_snapshot")]
fn py_serialize_task_type_snapshot(
    snapshot: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let snapshot: TaskTypeSnapshotWire = serde_json::from_value(
        py_to_json_value(snapshot.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "snapshot is not a valid TaskTypeSnapshotWire dict: {error}"
        ))
    })?;
    core_serialize_task_type_snapshot(&snapshot)
        .map_err(task_type_error_to_pyerr)
}

/// Return the task-type spec wire schema version.
#[pyfunction]
#[pyo3(name = "task_type_spec_wire_schema_version")]
fn py_task_type_spec_wire_schema_version() -> u64 {
    TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION
}

fn gate_followup_error_to_pyerr(error: GateFollowupError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the supported gate-follow-up wire schema version.
#[pyfunction]
#[pyo3(name = "gate_followup_wire_schema_version")]
fn py_gate_followup_wire_schema_version() -> u32 {
    GATE_FOLLOWUP_WIRE_SCHEMA_VERSION
}

/// Return the stable attempt identity for one gate and request fingerprint.
#[pyfunction]
#[pyo3(name = "gate_followup_attempt_id")]
fn py_gate_followup_attempt_id(gate_id: &str, fingerprint: &str) -> String {
    core_gate_followup_attempt_id(gate_id, fingerprint)
}

/// Classify one gate's follow-up disposition without host I/O.
#[pyfunction]
#[pyo3(name = "decide_gate_followup")]
fn py_decide_gate_followup<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request = gate_followup_decision_request_from_json_value(&value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid GateFollowupDecisionRequestWire dict: {error}"
            ))
        })?;
    let verdict = py
        .allow_threads(|| core_decide_gate_followup(&request))
        .map_err(gate_followup_error_to_pyerr)?;
    let value = serde_json::to_value(verdict).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_bead_decisions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_remove_workspace_suffix, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_valid_status_transition, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_status_from_lines, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_status_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_status_transition, m)?)?;
    m.add_function(wrap_pyfunction!(py_finalizer_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_finalizer_provider_spec, m)?)?;
    m.add_function(wrap_pyfunction!(py_finalizer_provider_spec_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_finalizer_instance_spec, m)?)?;
    m.add_function(wrap_pyfunction!(py_finalizer_instance_spec_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_finalizer_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_finalizer_plan_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_finalizer_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_authenticate_finalizer_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_finalizer_context_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_finalizer_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_finalizer_submission, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_select_remaining_commit_obligations,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_finalizer_json_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_aggregate_finalizer_outcomes, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_action_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_bead_action_field, m)?)?;
    m.add_function(wrap_pyfunction!(py_decide_bead_action, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_finalizer_bead_decision, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_validate_finalizer_assigned_bead_binding,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_gate_decision_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_decide_gate_decision_acceptance, m)?)?;
    m.add_function(wrap_pyfunction!(py_claim_gate_decision_execution, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_gate_lifecycle_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_decide_gate_lifecycle, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_task_type_spec, m)?)?;
    m.add_function(wrap_pyfunction!(py_task_type_spec_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_task_type_field_values, m)?)?;
    m.add_function(wrap_pyfunction!(py_render_task_type_body, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_task_type_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_serialize_task_type_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_task_type_spec_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_gate_followup_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_gate_followup_attempt_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_decide_gate_followup, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
