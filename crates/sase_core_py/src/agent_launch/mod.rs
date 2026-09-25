//! Agent launch bindings: request preparation and supervised spawning.

use crate::prelude::*;

use crate::agent_holds::agent_hold_error_to_pyerr;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

/// Return the launch wire schema version pinned by the Rust skeleton structs.
#[pyfunction]
#[pyo3(name = "agent_launch_wire_schema_version")]
fn py_agent_launch_wire_schema_version() -> u32 {
    sase_core::AGENT_LAUNCH_WIRE_SCHEMA_VERSION
}

/// Write the launch prompt temp file and return prepared process data.
#[pyfunction]
#[pyo3(name = "prepare_agent_launch")]
#[pyo3(signature = (
    request,
    python_executable,
    runner_script,
    output_root,
    sase_tmpdir = None,
    preallocated_env = None
))]
fn py_prepare_agent_launch<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
    python_executable: &str,
    runner_script: &str,
    output_root: &str,
    sase_tmpdir: Option<&str>,
    preallocated_env: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let req = agent_launch_request_from_pydict(request)?;
    let preallocated = match preallocated_env {
        Some(env) => env_dict_from_pydict(env)?,
        None => std::collections::BTreeMap::new(),
    };
    let prepared = core_prepare_agent_launch(
        &req,
        python_executable,
        runner_script,
        sase_tmpdir,
        output_root,
        &preallocated,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&prepared).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Spawn a prepared detached agent process and run optional claim callback.
#[pyfunction]
#[pyo3(name = "spawn_prepared_agent_process")]
#[pyo3(signature = (prepared, env, claim_callback = None))]
fn py_spawn_prepared_agent_process(
    py: Python<'_>,
    prepared: &Bound<'_, PyDict>,
    env: &Bound<'_, PyDict>,
    claim_callback: Option<&Bound<'_, PyAny>>,
) -> PyResult<u32> {
    let prepared = agent_launch_prepared_from_pydict(prepared)?;
    let env = env_dict_from_pydict(env)?;
    let mut child = py
        .allow_threads(move || spawn_prepared_detached_process(prepared, env))
        .map_err(PyRuntimeError::new_err)?;
    let pid = child.id();

    if let Some(callback) = claim_callback {
        match callback.call1((pid,)) {
            Ok(value) => {
                let success = value.extract::<bool>().map_err(|err| {
                    PyValueError::new_err(format!(
                        "claim_callback must return bool, got invalid value: {err}"
                    ))
                })?;
                if !success {
                    terminate_child_after_claim_failure(&mut child);
                    return Err(PyRuntimeError::new_err(
                        "agent launch claim callback reported failure",
                    ));
                }
            }
            Err(err) => {
                terminate_child_after_claim_failure(&mut child);
                return Err(err);
            }
        }
    }

    Ok(pid)
}

/// Allocate unique launch timestamps from a base YYmmdd_HHMMSS timestamp.
#[pyfunction]
#[pyo3(name = "allocate_launch_timestamp_batch")]
#[pyo3(signature = (count, base_timestamp, after_timestamp = None))]
fn py_allocate_launch_timestamp_batch<'py>(
    py: Python<'py>,
    count: usize,
    base_timestamp: &str,
    after_timestamp: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    let timestamps = core_allocate_launch_timestamp_batch(
        count,
        base_timestamp,
        after_timestamp,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let list = PyList::empty_bound(py);
    for timestamp in timestamps {
        list.append(timestamp)?;
    }
    Ok(list)
}

/// Plan deterministic prompt fan-out without launching child agents.
#[pyfunction]
#[pyo3(name = "plan_agent_launch_fanout")]
#[pyo3(signature = (prompt, launch_kind = None))]
fn py_plan_agent_launch_fanout<'py>(
    py: Python<'py>,
    prompt: &str,
    launch_kind: Option<&str>,
) -> PyResult<PyObject> {
    let plan = core_plan_agent_launch_fanout(prompt, launch_kind)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Filter static `%if(should_run=...)` prompt segments before launch planning.
#[pyfunction]
#[pyo3(name = "filter_conditional_launch_segments")]
fn py_filter_conditional_launch_segments<'py>(
    py: Python<'py>,
    prompt: &str,
) -> PyResult<PyObject> {
    let filter = core_filter_conditional_launch_segments(prompt)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&filter).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Bind no-argument batch waits to a supplied predecessor launch identity.
#[pyfunction]
#[pyo3(name = "bind_batch_predecessor_waits")]
fn py_bind_batch_predecessor_waits<'py>(
    py: Python<'py>,
    prompt: &str,
    predecessor: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let predecessor: BatchPredecessorContextWire = serde_json::from_value(
        py_to_json_value(predecessor)?,
    )
    .map_err(|err| {
        PyValueError::new_err(format!(
            "invalid batch predecessor context: {err}"
        ))
    })?;
    let binding = core_bind_batch_predecessor_waits(prompt, &predecessor)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&binding).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan a pure typed Agent/Proc launch graph without launching children.
#[pyfunction]
#[pyo3(name = "plan_typed_launch_units")]
#[pyo3(signature = (prompt, launch_kind = None, selected_project = None, enabled_feature_flags = None))]
fn py_plan_typed_launch_units<'py>(
    py: Python<'py>,
    prompt: &str,
    launch_kind: Option<&str>,
    selected_project: Option<&str>,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let flags = enabled_feature_flags.unwrap_or_default();
    let plan = core_plan_typed_launch_units_with_flags(
        prompt,
        launch_kind,
        selected_project,
        &flags,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "launch_unit_hold_key")]
pub(crate) fn py_launch_unit_hold_key(
    request_id: &str,
    logical_id: &str,
) -> PyResult<String> {
    core_launch_unit_hold_key(request_id, logical_id)
        .map_err(agent_hold_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "launch_unit_hold_armer")]
pub(crate) fn py_launch_unit_hold_armer<'py>(
    py: Python<'py>,
    unit: &Bound<'py, PyAny>,
    request_id: &str,
    project: &str,
    pid: u32,
    done_marker_path: &str,
) -> PyResult<PyObject> {
    let unit: LaunchUnitWire = serde_json::from_value(py_to_json_value(unit)?)
        .map_err(|err| {
            PyValueError::new_err(format!("invalid launch unit: {err}"))
        })?;
    let armer = core_launch_unit_hold_armer(
        &unit,
        request_id,
        project,
        pid,
        &PathBuf::from(done_marker_path),
    )
    .map_err(agent_hold_error_to_pyerr)?;
    let value = serde_json::to_value(&armer).map_err(|err| {
        PyRuntimeError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "launch_admission_journal_schema_version")]
fn py_launch_admission_journal_schema_version() -> u32 {
    LAUNCH_ADMISSION_JOURNAL_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "reconcile_admission_journal")]
fn py_reconcile_admission_journal<'py>(
    py: Python<'py>,
    entries: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let entries: Vec<LaunchAdmissionJournalEntryWire> =
        serde_json::from_value(py_to_json_value(entries)?).map_err(|err| {
            PyValueError::new_err(format!(
                "invalid launch admission journal: {err}"
            ))
        })?;
    let states = core_reconcile_admission_journal(&entries);
    let value = serde_json::to_value(&states).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "next_admission_actions", signature = (plan, states, wait_facts, hold_blocks = None))]
fn py_next_admission_actions<'py>(
    py: Python<'py>,
    plan: &Bound<'_, PyAny>,
    states: &Bound<'_, PyAny>,
    wait_facts: &Bound<'_, PyAny>,
    hold_blocks: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let plan: LaunchPlanWire = serde_json::from_value(py_to_json_value(plan)?)
        .map_err(|err| {
            PyValueError::new_err(format!("invalid launch plan: {err}"))
        })?;
    let states: BTreeMap<String, LaunchAdmissionUnitStateWire> =
        serde_json::from_value(py_to_json_value(states)?).map_err(|err| {
            PyValueError::new_err(format!(
                "invalid launch admission states: {err}"
            ))
        })?;
    let wait_facts: Vec<LaunchAdmissionWaitFactWire> = serde_json::from_value(
        py_to_json_value(wait_facts)?,
    )
    .map_err(|err| {
        PyValueError::new_err(format!(
            "invalid launch admission wait facts: {err}"
        ))
    })?;
    let hold_blocks: Vec<LaunchAdmissionHoldBlockWire> = match hold_blocks {
        Some(value) => serde_json::from_value(py_to_json_value(value)?)
            .map_err(|err| {
                PyValueError::new_err(format!(
                    "invalid launch admission hold blocks: {err}"
                ))
            })?,
        None => Vec::new(),
    };
    let actions = core_next_admission_actions_with_holds(
        &plan,
        &states,
        &wait_facts,
        &hold_blocks,
    );
    let value = serde_json::to_value(&actions).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "summarize_admission")]
fn py_summarize_admission<'py>(
    py: Python<'py>,
    plan: &Bound<'_, PyAny>,
    states: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let plan: LaunchPlanWire = serde_json::from_value(py_to_json_value(plan)?)
        .map_err(|err| {
            PyValueError::new_err(format!("invalid launch plan: {err}"))
        })?;
    let states: BTreeMap<String, LaunchAdmissionUnitStateWire> =
        serde_json::from_value(py_to_json_value(states)?).map_err(|err| {
            PyValueError::new_err(format!(
                "invalid launch admission states: {err}"
            ))
        })?;
    let summary = core_summarize_admission(&plan, &states);
    let value = serde_json::to_value(&summary).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "admission_unit_results")]
fn py_admission_unit_results<'py>(
    py: Python<'py>,
    plan: &Bound<'_, PyAny>,
    states: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let plan: LaunchPlanWire = serde_json::from_value(py_to_json_value(plan)?)
        .map_err(|err| {
            PyValueError::new_err(format!("invalid launch plan: {err}"))
        })?;
    let states: BTreeMap<String, LaunchAdmissionUnitStateWire> =
        serde_json::from_value(py_to_json_value(states)?).map_err(|err| {
            PyValueError::new_err(format!(
                "invalid launch admission states: {err}"
            ))
        })?;
    let results = core_admission_unit_results(&plan, &states);
    let value = serde_json::to_value(&results).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "dispatch_fingerprint")]
fn py_dispatch_fingerprint(
    plan_digest: &str,
    logical_id: &str,
    payload: &Bound<'_, PyAny>,
) -> PyResult<String> {
    let payload: LaunchUnitPayloadWire =
        serde_json::from_value(py_to_json_value(payload)?).map_err(|err| {
            PyValueError::new_err(format!("invalid launch unit payload: {err}"))
        })?;
    Ok(core_dispatch_fingerprint(plan_digest, logical_id, &payload))
}

#[pyfunction]
#[pyo3(name = "agent_unit_dispatch_prompt")]
#[pyo3(signature = (agent, enabled_feature_flags = None))]
fn py_agent_unit_dispatch_prompt(
    agent: &Bound<'_, PyAny>,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<String> {
    let agent: AgentUnitWire = serde_json::from_value(py_to_json_value(agent)?)
        .map_err(|err| {
            PyValueError::new_err(format!("invalid agent unit: {err}"))
        })?;
    let flags = enabled_feature_flags.unwrap_or_default();
    Ok(core_agent_unit_dispatch_prompt_with_flags(&agent, &flags))
}

#[pyfunction]
#[pyo3(name = "prompt_has_identity_directive")]
fn py_prompt_has_identity_directive(prompt: &str) -> bool {
    core_prompt_has_identity_directive(prompt)
}

#[pyfunction]
#[pyo3(name = "collect_queue_fields")]
#[pyo3(signature = (occurrences, enabled_feature_flags = None))]
pub(crate) fn py_collect_queue_fields<'py>(
    py: Python<'py>,
    occurrences: &Bound<'_, PyAny>,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let occurrences: Vec<QueueOccurrenceWire> = serde_json::from_value(
        py_to_json_value(occurrences)?,
    )
    .map_err(|err| {
        PyValueError::new_err(format!("invalid queue occurrences: {err}"))
    })?;
    let flags = enabled_feature_flags.unwrap_or_default();
    let result = core_collect_queue_fields_with_flags(&occurrences, &flags);
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "format_queue_directive")]
pub(crate) fn py_format_queue_directive(
    fields: &Bound<'_, PyAny>,
) -> PyResult<Option<String>> {
    let fields: QueueFieldsWire =
        serde_json::from_value(py_to_json_value(fields)?).map_err(|err| {
            PyValueError::new_err(format!("invalid queue fields: {err}"))
        })?;
    Ok(core_format_queue_directive(&fields))
}

#[pyfunction]
#[pyo3(name = "collect_hold_fields")]
#[pyo3(signature = (occurrences, enabled_feature_flags = None))]
fn py_collect_hold_fields<'py>(
    py: Python<'py>,
    occurrences: &Bound<'_, PyAny>,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let occurrences: Vec<HoldOccurrenceWire> = serde_json::from_value(
        py_to_json_value(occurrences)?,
    )
    .map_err(|err| {
        PyValueError::new_err(format!("invalid hold occurrences: {err}"))
    })?;
    let flags = enabled_feature_flags.unwrap_or_default();
    let result = core_collect_hold_fields_with_flags(&occurrences, &flags);
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "format_hold_directive")]
fn py_format_hold_directive(
    fields: &Bound<'_, PyAny>,
) -> PyResult<Option<String>> {
    let fields: HoldFieldsWire =
        serde_json::from_value(py_to_json_value(fields)?).map_err(|err| {
            PyValueError::new_err(format!("invalid hold fields: {err}"))
        })?;
    Ok(core_format_hold_directive(&fields))
}

#[pyfunction]
#[pyo3(name = "hold_fields_to_selectors")]
#[pyo3(signature = (fields, pending_artifact_dirs = None, identity = None))]
fn py_hold_fields_to_selectors<'py>(
    py: Python<'py>,
    fields: &Bound<'_, PyAny>,
    pending_artifact_dirs: Option<Vec<String>>,
    identity: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let fields: HoldFieldsWire =
        serde_json::from_value(py_to_json_value(fields)?).map_err(|err| {
            PyValueError::new_err(format!("invalid hold fields: {err}"))
        })?;
    let identity: HoldSelectorIdentityWire = match identity {
        Some(payload) => serde_json::from_value(py_to_json_value(payload)?)
            .map_err(|err| {
                PyValueError::new_err(format!(
                    "invalid hold selector identity: {err}"
                ))
            })?,
        None => HoldSelectorIdentityWire::default(),
    };
    let pending_artifact_dirs = pending_artifact_dirs.unwrap_or_default();
    let selectors = core_hold_fields_to_selectors_with_identity(
        &fields,
        &pending_artifact_dirs,
        &identity,
    )
    .map_err(|error| PyValueError::new_err(error.message))?;
    let value = serde_json::to_value(&selectors).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "parse_queue_capacity")]
#[pyo3(signature = (raw, enabled_feature_flags = None))]
pub(crate) fn py_parse_queue_capacity(
    raw: &str,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<u32> {
    let flags = enabled_feature_flags.unwrap_or_default();
    core_parse_queue_capacity_with_flags(raw, &flags)
        .map_err(|error| PyValueError::new_err(error.message))
}

#[pyfunction]
#[pyo3(name = "parse_queue_capacity_value")]
#[pyo3(signature = (raw, enabled_feature_flags = None))]
pub(crate) fn py_parse_queue_capacity_value<'py>(
    py: Python<'py>,
    raw: &str,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let flags = enabled_feature_flags.unwrap_or_default();
    let value =
        sase_core::queue_directive::parse_queue_capacity_value_with_flags(
            raw, &flags,
        )
        .map_err(|error| PyValueError::new_err(error.message))?;
    let json = serde_json::to_value(&value).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

#[pyfunction]
#[pyo3(name = "format_queue_capacity_multiplier")]
pub(crate) fn py_format_queue_capacity_multiplier(
    value: f64,
) -> Option<String> {
    sase_core::queue_directive::format_queue_capacity_multiplier(value)
}

#[pyfunction]
#[pyo3(name = "resolve_queue_capacity_multiplier")]
pub(crate) fn py_resolve_queue_capacity_multiplier(
    multiplier: f64,
    effective_limit: f64,
) -> Option<f64> {
    sase_core::queue_directive::resolve_queue_capacity_multiplier(
        multiplier,
        effective_limit,
    )
}

#[pyfunction]
#[pyo3(name = "queue_directive_flag_key")]
pub(crate) fn py_queue_directive_flag_key() -> &'static str {
    core_queue_directive_flag_key()
}

#[pyfunction]
#[pyo3(name = "normalize_persisted_queue_capacity")]
#[pyo3(signature = (queue_capacity, queue_capacity_explicit, effective_weight, global_limit, capacity_budget, queue_capacity_multiplier = None))]
pub(crate) fn py_normalize_persisted_queue_capacity(
    py: Python<'_>,
    queue_capacity: Option<u32>,
    queue_capacity_explicit: bool,
    effective_weight: f64,
    global_limit: f64,
    capacity_budget: bool,
    queue_capacity_multiplier: Option<f64>,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(
        core_normalize_persisted_queue_capacity_with_multiplier(
            queue_capacity,
            queue_capacity_explicit,
            queue_capacity_multiplier,
            effective_weight,
            global_limit,
            capacity_budget,
        ),
    )
    .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "wait_target_key")]
fn py_wait_target_key(target: &Bound<'_, PyAny>) -> PyResult<String> {
    let target: WaitTargetWire =
        serde_json::from_value(py_to_json_value(target)?).map_err(|err| {
            PyValueError::new_err(format!("invalid wait target: {err}"))
        })?;
    Ok(core_wait_target_key(&target))
}

#[pyfunction]
#[pyo3(name = "condition_eval_wire_schema_version")]
fn py_condition_eval_wire_schema_version() -> u32 {
    CONDITION_EVAL_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "condition_context_schema_version")]
fn py_condition_context_schema_version() -> u32 {
    CONDITION_CONTEXT_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "condition_default_timeout_seconds")]
fn py_condition_default_timeout_seconds() -> f64 {
    CONDITION_DEFAULT_TIMEOUT_SECONDS
}

#[pyfunction]
#[pyo3(name = "condition_max_timeout_seconds")]
fn py_condition_max_timeout_seconds() -> f64 {
    CONDITION_MAX_TIMEOUT_SECONDS
}

#[pyfunction]
#[pyo3(name = "condition_output_cap_bytes")]
fn py_condition_output_cap_bytes() -> usize {
    CONDITION_OUTPUT_CAP_BYTES
}

#[pyfunction]
#[pyo3(name = "classify_condition_status")]
#[pyo3(signature = (exit_code=None, signal=None, timed_out=false, exec_error=false, cancelled=false))]
fn py_classify_condition_status(
    exit_code: Option<i32>,
    signal: Option<i32>,
    timed_out: bool,
    exec_error: bool,
    cancelled: bool,
) -> &'static str {
    core_classify_condition_status(
        exit_code, signal, timed_out, exec_error, cancelled,
    )
}

#[pyfunction]
#[pyo3(name = "sanitize_condition_inputs")]
fn py_sanitize_condition_inputs<'py>(
    py: Python<'py>,
    value: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let parsed = py_to_json_value(value)?;
    let sanitized = core_sanitize_safe_inputs(&parsed);
    let encoded = serde_json::to_value(sanitized).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &encoded)
}

#[pyfunction]
#[pyo3(name = "build_condition_context")]
#[pyo3(signature = (unit, waited, selected_project=None, safe_inputs=None, share_workspace=false))]
fn py_build_condition_context<'py>(
    py: Python<'py>,
    unit: &Bound<'_, PyAny>,
    waited: &Bound<'_, PyAny>,
    selected_project: Option<&str>,
    safe_inputs: Option<&Bound<'_, PyAny>>,
    share_workspace: bool,
) -> PyResult<PyObject> {
    let unit: LaunchUnitWire = serde_json::from_value(py_to_json_value(unit)?)
        .map_err(|err| {
            PyValueError::new_err(format!("invalid launch unit: {err}"))
        })?;
    let waited: Vec<WaitedOutcomeWire> =
        serde_json::from_value(py_to_json_value(waited)?).map_err(|err| {
            PyValueError::new_err(format!("invalid waited outcomes: {err}"))
        })?;
    let inputs = match safe_inputs {
        Some(value) => {
            let parsed = py_to_json_value(value)?;
            match parsed {
                JsonValue::Object(map) => map.into_iter().collect(),
                _ => BTreeMap::new(),
            }
        }
        None => BTreeMap::new(),
    };
    let context = core_build_condition_context(
        &unit,
        selected_project,
        inputs,
        &waited,
        share_workspace,
    );
    let encoded = serde_json::to_value(&context).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &encoded)
}

#[pyfunction]
#[pyo3(name = "evaluate_launch_condition")]
fn py_evaluate_launch_condition<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let request: ConditionEvalRequestWire =
        serde_json::from_value(py_to_json_value(request)?).map_err(|err| {
            PyValueError::new_err(format!(
                "invalid condition eval request: {err}"
            ))
        })?;
    let result = core_evaluate_launch_condition(&request);
    let encoded = serde_json::to_value(&result).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &encoded)
}

#[pyfunction]
#[pyo3(name = "proc_dispatch_wire_schema_version")]
fn py_proc_dispatch_wire_schema_version() -> u32 {
    PROC_DISPATCH_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "xprompt_proc_origin")]
fn py_xprompt_proc_origin() -> &'static str {
    XPROMPT_PROC_ORIGIN
}

#[pyfunction]
#[pyo3(name = "prepare_proc_script")]
fn py_prepare_proc_script<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let request: ProcDispatchRequestWire =
        serde_json::from_value(py_to_json_value(request)?).map_err(|err| {
            PyValueError::new_err(format!(
                "invalid proc dispatch request: {err}"
            ))
        })?;
    let prepared =
        core_prepare_proc_script(&request).map_err(PyValueError::new_err)?;
    let encoded = serde_json::to_value(&prepared).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &encoded)
}

#[pyfunction]
#[pyo3(name = "parse_proc_duration_seconds")]
fn py_parse_proc_duration_seconds(raw: &str) -> PyResult<u64> {
    core_parse_proc_duration_seconds(raw).map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(name = "proc_script_argv")]
fn py_proc_script_argv(
    language: &str,
    work_dir: &str,
    python_executable: &str,
) -> PyResult<Vec<String>> {
    core_proc_script_argv(
        language,
        PathBuf::from(work_dir).as_path(),
        python_executable,
    )
    .map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(
    name = "validate_standalone_proc_shell_name",
    signature = (name = None)
)]
fn py_validate_standalone_proc_shell_name(name: Option<&str>) -> PyResult<()> {
    core_validate_standalone_proc_shell_name(name)
        .map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(
    name = "validate_proc_workspace_intent",
    signature = (workspace, selected_project = None, declared_cwd = None)
)]
fn py_validate_proc_workspace_intent(
    workspace: bool,
    selected_project: Option<&str>,
    declared_cwd: Option<&str>,
) -> PyResult<()> {
    core_validate_proc_workspace_intent(
        workspace,
        selected_project,
        declared_cwd,
    )
    .map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(
    name = "resolve_proc_execution_cwd",
    signature = (workspace, declared_cwd = None, source_cwd = None, lease_root = None)
)]
fn py_resolve_proc_execution_cwd(
    workspace: bool,
    declared_cwd: Option<&str>,
    source_cwd: Option<&str>,
    lease_root: Option<&str>,
) -> PyResult<String> {
    core_resolve_proc_execution_cwd(
        workspace,
        declared_cwd,
        source_cwd,
        lease_root,
    )
    .map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(
    name = "sanitized_proc_env",
    signature = (base_env, proc_id, cwd, python_executable, selected_project = None, project_file = None, workspace_num = None)
)]
// The argument list mirrors the exported Python binding signature; grouping it
// locally would add a wrapper type the caller could not use directly.
#[allow(clippy::too_many_arguments)]
fn py_sanitized_proc_env<'py>(
    py: Python<'py>,
    base_env: &Bound<'_, PyDict>,
    proc_id: &str,
    cwd: &str,
    python_executable: &str,
    selected_project: Option<&str>,
    project_file: Option<&str>,
    workspace_num: Option<u32>,
) -> PyResult<PyObject> {
    let base_env = env_dict_from_pydict(base_env)?;
    let env = core_sanitized_proc_env(
        &base_env,
        proc_id,
        PathBuf::from(cwd).as_path(),
        python_executable,
        selected_project,
        project_file,
        workspace_num,
    );
    let encoded = serde_json::to_value(&env).map_err(|err| {
        PyValueError::new_err(format!("internal serialize error: {err}"))
    })?;
    json_value_to_py(py, &encoded)
}

#[pyfunction]
#[pyo3(name = "cleanup_proc_private_inputs")]
fn py_cleanup_proc_private_inputs(work_dir: &str) {
    core_cleanup_proc_private_inputs(PathBuf::from(work_dir).as_path());
}

/// Return parsed RUNNING workspace claims from project-file content.
#[pyfunction]
#[pyo3(name = "list_workspace_claims_from_content")]
fn py_list_workspace_claims_from_content<'py>(
    py: Python<'py>,
    content: &str,
) -> PyResult<PyObject> {
    let claims = core_list_workspace_claims_from_content(content);
    let value = serde_json::to_value(&claims).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan insertion of one RUNNING workspace claim.
#[pyfunction]
#[pyo3(name = "plan_claim_workspace_from_content")]
fn py_plan_claim_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_plan_claim_workspace_from_content(content, &req);
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan transfer of one RUNNING workspace claim to a new PID.
#[pyfunction]
#[pyo3(name = "plan_transfer_workspace_claim_from_content")]
fn py_plan_transfer_workspace_claim_from_content<'py>(
    py: Python<'py>,
    content: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_plan_transfer_workspace_claim_from_content(content, &req);
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan first-free workspace allocation and RUNNING claim insertion together.
#[pyfunction]
#[pyo3(name = "allocate_and_claim_workspace_from_content")]
fn py_allocate_and_claim_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_allocate_and_claim_workspace_from_content(
        content,
        min_workspace,
        max_workspace,
        &req,
    );
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Decide whether a destructive workspace-preparation step may proceed
/// against a checkout that may be occupied by another live agent.
#[pyfunction]
#[pyo3(
    name = "decide_workspace_occupant_conflict",
    signature = (occupant, caller, occupant_pid_alive, running_claim, running_claim_pid_alive)
)]
fn py_decide_workspace_occupant_conflict<'py>(
    py: Python<'py>,
    occupant: Option<&Bound<'py, PyDict>>,
    caller: &Bound<'py, PyDict>,
    occupant_pid_alive: bool,
    running_claim: Option<&Bound<'py, PyDict>>,
    running_claim_pid_alive: bool,
) -> PyResult<PyObject> {
    let occupant_wire =
        occupant.map(occupant_record_from_pydict).transpose()?;
    let caller_wire = occupancy_caller_from_pydict(caller)?;
    let running_claim_wire =
        running_claim.map(workspace_claim_from_pydict).transpose()?;
    let decision = core_decide_workspace_occupant_conflict(
        occupant_wire.as_ref(),
        &caller_wire,
        occupant_pid_alive,
        running_claim_wire.as_ref(),
        running_claim_pid_alive,
    );
    let value = serde_json::to_value(&decision).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn occupant_record_from_pydict(
    record: &Bound<'_, PyDict>,
) -> PyResult<OccupantRecordWire> {
    let value = py_to_json_value(record.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "occupant is not a valid OccupantRecordWire dict: {e}"
        ))
    })
}

fn occupancy_caller_from_pydict(
    caller: &Bound<'_, PyDict>,
) -> PyResult<OccupancyCallerWire> {
    let value = py_to_json_value(caller.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "caller is not a valid OccupancyCallerWire dict: {e}"
        ))
    })
}

fn workspace_claim_from_pydict(
    claim: &Bound<'_, PyDict>,
) -> PyResult<WorkspaceClaimWire> {
    let value = py_to_json_value(claim.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "running_claim is not a valid WorkspaceClaimWire dict: {e}"
        ))
    })
}

fn spawn_prepared_detached_process(
    prepared: AgentLaunchPreparedWire,
    env: BTreeMap<String, String>,
) -> Result<Child, String> {
    let Some((program, args)) = prepared.argv.split_first() else {
        return Err("prepared launch argv must not be empty".to_string());
    };

    let stdout_file = File::create(&prepared.output_path).map_err(|err| {
        format!(
            "failed to open launch output file {}: {err}",
            prepared.output_path
        )
    })?;
    let stderr_file = stdout_file.try_clone().map_err(|err| {
        format!(
            "failed to clone launch output file {} for stderr: {err}",
            prepared.output_path
        )
    })?;

    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&prepared.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .env_clear()
        .envs(env);

    configure_detached_process(&mut command);

    command.spawn().map_err(|err| {
        format!(
            "failed to spawn prepared agent process in cwd {}: {err}",
            prepared.cwd
        )
    })
}

#[cfg(unix)]
fn configure_detached_process(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // Match Python's subprocess.Popen(start_new_session=True) behavior.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(windows)]
fn configure_detached_process(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn configure_detached_process(_command: &mut Command) {}

fn terminate_child_after_claim_failure(child: &mut Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }

    terminate_child_gracefully(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn terminate_child_gracefully(child: &Child) {
    let _ = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
}

#[cfg(not(unix))]
fn terminate_child_gracefully(child: &mut Child) {
    let _ = child.kill();
}

fn workspace_claim_request_from_pydict(
    request: &Bound<'_, PyDict>,
) -> PyResult<WorkspaceClaimRequestWire> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid WorkspaceClaimRequestWire dict: {e}"
        ))
    })
}

fn agent_launch_request_from_pydict(
    request: &Bound<'_, PyDict>,
) -> PyResult<AgentLaunchRequestWire> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentLaunchRequestWire dict: {e}"
        ))
    })
}

fn agent_launch_prepared_from_pydict(
    prepared: &Bound<'_, PyDict>,
) -> PyResult<AgentLaunchPreparedWire> {
    let value = py_to_json_value(prepared.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "prepared is not a valid AgentLaunchPreparedWire dict: {e}"
        ))
    })
}

fn env_dict_from_pydict(
    env: &Bound<'_, PyDict>,
) -> PyResult<std::collections::BTreeMap<String, String>> {
    let value = py_to_json_value(env.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "preallocated_env is not a valid string dict: {e}"
        ))
    })
}

pub(crate) fn register_agent_launch(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_normalize_persisted_queue_capacity,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_collect_queue_fields, m)?)?;
    m.add_function(wrap_pyfunction!(py_format_queue_directive, m)?)?;
    m.add_function(wrap_pyfunction!(py_collect_hold_fields, m)?)?;
    m.add_function(wrap_pyfunction!(py_format_hold_directive, m)?)?;
    m.add_function(wrap_pyfunction!(py_hold_fields_to_selectors, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_queue_capacity, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_queue_capacity_value, m)?)?;
    m.add_function(wrap_pyfunction!(py_format_queue_capacity_multiplier, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_queue_capacity_multiplier, m)?)?;
    m.add_function(wrap_pyfunction!(py_queue_directive_flag_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_launch_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_prepare_agent_launch, m)?)?;
    m.add_function(wrap_pyfunction!(py_spawn_prepared_agent_process, m)?)?;
    m.add_function(wrap_pyfunction!(py_allocate_launch_timestamp_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_launch_fanout, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_filter_conditional_launch_segments,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bind_batch_predecessor_waits, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_typed_launch_units, m)?)?;
    m.add_function(wrap_pyfunction!(py_launch_unit_hold_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_launch_unit_hold_armer, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_launch_admission_journal_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_reconcile_admission_journal, m)?)?;
    m.add_function(wrap_pyfunction!(py_next_admission_actions, m)?)?;
    m.add_function(wrap_pyfunction!(py_summarize_admission, m)?)?;
    m.add_function(wrap_pyfunction!(py_admission_unit_results, m)?)?;
    m.add_function(wrap_pyfunction!(py_dispatch_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_unit_dispatch_prompt, m)?)?;
    m.add_function(wrap_pyfunction!(py_prompt_has_identity_directive, m)?)?;
    m.add_function(wrap_pyfunction!(py_wait_target_key, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_condition_eval_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_condition_context_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_condition_default_timeout_seconds, m)?)?;
    m.add_function(wrap_pyfunction!(py_condition_max_timeout_seconds, m)?)?;
    m.add_function(wrap_pyfunction!(py_condition_output_cap_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_condition_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_sanitize_condition_inputs, m)?)?;
    m.add_function(wrap_pyfunction!(py_build_condition_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_launch_condition, m)?)?;
    m.add_function(wrap_pyfunction!(py_proc_dispatch_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_xprompt_proc_origin, m)?)?;
    m.add_function(wrap_pyfunction!(py_prepare_proc_script, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_proc_duration_seconds, m)?)?;
    m.add_function(wrap_pyfunction!(py_proc_script_argv, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_validate_standalone_proc_shell_name,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_validate_proc_workspace_intent, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_proc_execution_cwd, m)?)?;
    m.add_function(wrap_pyfunction!(py_sanitized_proc_env, m)?)?;
    m.add_function(wrap_pyfunction!(py_cleanup_proc_private_inputs, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_list_workspace_claims_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_plan_claim_workspace_from_content, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_plan_transfer_workspace_claim_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_allocate_and_claim_workspace_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_decide_workspace_occupant_conflict,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
