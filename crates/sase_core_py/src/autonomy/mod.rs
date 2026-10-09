//! Autonomy record bindings: selection resolution, legacy translation,
//! legacy projection, gate evaluation, summary, mutation, inheritance,
//! the profile catalog, and the host decision log store.

use std::path::PathBuf;

use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::autonomy::AutonomyError;

fn autonomy_error_to_pyerr(error: AutonomyError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn autonomy_log_error_to_pyerr(
    error: sase_core::autonomy::AutonomyLogError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn parse_record(
    value: &serde_json::Value,
) -> Result<sase_core::autonomy::AutonomyRecordWire, PyErr> {
    serde_json::from_value(value.clone()).map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid AutonomyRecordWire dict: {error}"
        ))
    })
}

fn to_py(py: Python<'_>, value: &impl serde::Serialize) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &json)
}

/// Resolve a `%auto` selection to a revision-1 autonomy record.
///
/// The request carries `selection` (the text after `%auto`, or `null`
/// when the prompt has none), `source` (prompt, tui, cli, inherited, or
/// legacy), `actor` (`kind`, `surface`, `principal`), and `now` for
/// `updated_at`. Unknown spellings raise the classifier's `invalid-auto`
/// message as `ValueError`.
#[pyfunction]
#[pyo3(name = "autonomy_resolve_selection")]
fn py_autonomy_resolve_selection<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request)?;
    let selection = value
        .get("selection")
        .and_then(|item| item.as_str())
        .map(str::to_string);
    let source = value
        .get("source")
        .and_then(|item| item.as_str())
        .unwrap_or("prompt");
    let actor: sase_core::autonomy::AutonomyActorWire = match value.get("actor")
    {
        Some(actor) => {
            serde_json::from_value(actor.clone()).map_err(|error| {
                PyValueError::new_err(format!(
                    "invalid autonomy actor: {error}"
                ))
            })?
        }
        None => sase_core::autonomy::AutonomyActorWire::default(),
    };
    let now = value
        .get("now")
        .and_then(|item| item.as_str())
        .unwrap_or("");
    let record = sase_core::autonomy::resolve_autonomy_selection(
        selection.as_deref(),
        source,
        &actor,
        now,
    )
    .map_err(autonomy_error_to_pyerr)?;
    let value = serde_json::to_value(&record).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Translate pre-E1 `%auto` meta keys to an autonomy record.
///
/// The request is the raw `agent_meta.json` dict; the record carries
/// `source: legacy`. Covers every shape today's writers produce: bare,
/// `:plan`, `:tale`/`:epic`, toggle-on bare, revive action-only, and the
/// legacy `"plan"` action.
#[pyfunction]
#[pyo3(name = "autonomy_record_from_legacy_meta")]
fn py_autonomy_record_from_legacy_meta<'py>(
    py: Python<'py>,
    meta: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(meta)?;
    let map = value.as_object().cloned().unwrap_or_default();
    let record = sase_core::autonomy::autonomy_record_from_legacy_meta(&map);
    let value = serde_json::to_value(&record).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Reproduce today's legacy writer output from a record's selection.
///
/// Returns the legacy keys (`approve`, `auto_approve_argument`,
/// `auto_approve_plan_action`, `plan`) plus the `prompt_mode` for
/// `set_prompt_auto_mode`.
#[pyfunction]
#[pyo3(name = "autonomy_legacy_projection")]
fn py_autonomy_legacy_projection<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(record)?;
    let parsed: sase_core::autonomy::AutonomyRecordWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "record is not a valid AutonomyRecordWire dict: {error}"
            ))
        })?;
    let projection = sase_core::autonomy::autonomy_legacy_projection(&parsed);
    let value = serde_json::to_value(&projection).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Evaluate one gate request against an autonomy record.
///
/// Both arguments are dicts: the record and `{gate_kind, option_ids,
/// capabilities, request_id}`. Never depends on `primary_branch`,
/// `default_selected`, or option order; combined selections are
/// all-or-nothing.
#[pyfunction]
#[pyo3(name = "autonomy_evaluate")]
fn py_autonomy_evaluate<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyAny>,
    request: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let record_value = py_to_json_value(record)?;
    let parsed: sase_core::autonomy::AutonomyRecordWire =
        serde_json::from_value(record_value).map_err(|error| {
            PyValueError::new_err(format!(
                "record is not a valid AutonomyRecordWire dict: {error}"
            ))
        })?;
    let request_value = py_to_json_value(request)?;
    let parsed_request: sase_core::autonomy::AutonomyEvaluateRequestWire =
        serde_json::from_value(request_value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid AutonomyEvaluateRequestWire dict: \
                {error}"
            ))
        })?;
    let decision = sase_core::autonomy::evaluate(&parsed, &parsed_request);
    let value = serde_json::to_value(&decision).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the autonomy wire schema version pinned by the Rust structs.
#[pyfunction]
#[pyo3(name = "autonomy_wire_schema_version")]
fn py_autonomy_wire_schema_version() -> u32 {
    sase_core::autonomy::AUTONOMY_WIRE_SCHEMA_VERSION
}

/// Human-facing summary of one autonomy record: profile, class,
/// sentence, one-liner, per-kind cells, coverage, and revision.
#[pyfunction]
#[pyo3(name = "autonomy_summary")]
fn py_autonomy_summary<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(record)?;
    let parsed = parse_record(&value)?;
    let summary = sase_core::autonomy::autonomy_summary(&parsed);
    to_py(py, &summary)
}

/// One line per decision, used by `log`, `explain`, and `gate show`.
/// Takes the decision dict and a `{gate_kind}` context dict.
#[pyfunction]
#[pyo3(name = "autonomy_decision_sentence")]
fn py_autonomy_decision_sentence<'py>(
    decision: &Bound<'py, PyAny>,
    context: &Bound<'py, PyAny>,
) -> PyResult<String> {
    let decision_value = py_to_json_value(decision)?;
    let parsed: sase_core::autonomy::AutonomyDecisionWire =
        serde_json::from_value(decision_value).map_err(|error| {
            PyValueError::new_err(format!(
                "decision is not a valid AutonomyDecisionWire dict: {error}"
            ))
        })?;
    let context_value = py_to_json_value(context)?;
    let parsed_context: sase_core::autonomy::AutonomyDecisionSentenceContextWire =
        serde_json::from_value(context_value).map_err(|error| {
            PyValueError::new_err(format!(
                "context is not a valid AutonomyDecisionSentenceContextWire \
                dict: {error}"
            ))
        })?;
    Ok(sase_core::autonomy::autonomy_decision_sentence(
        &parsed,
        &parsed_context,
    ))
}

/// The advisory awareness block for one record, or `None` for `manual`.
#[pyfunction]
#[pyo3(name = "autonomy_awareness_text")]
fn py_autonomy_awareness_text(
    record: &Bound<'_, PyAny>,
) -> PyResult<Option<String>> {
    let value = py_to_json_value(record)?;
    let parsed = parse_record(&value)?;
    Ok(sase_core::autonomy::autonomy_awareness_text(&parsed))
}

/// Apply a `%auto` selection change to a live record with revision and
/// actor checks. Returns `{status, record, reason}` with status
/// `applied`, `unchanged`, `refused`, or `stale`.
#[pyfunction]
#[pyo3(name = "autonomy_mutate")]
fn py_autonomy_mutate<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyAny>,
    request: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let record_value = py_to_json_value(record)?;
    let parsed = parse_record(&record_value)?;
    let request_value = py_to_json_value(request)?;
    let parsed_request: sase_core::autonomy::AutonomyMutateRequestWire =
        serde_json::from_value(request_value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid AutonomyMutateRequestWire dict: \
                {error}"
            ))
        })?;
    let result = sase_core::autonomy::mutate_autonomy(&parsed, &parsed_request)
        .map_err(autonomy_error_to_pyerr)?;
    to_py(py, &result)
}

/// Seed a host-composed successor from its predecessor's live record.
/// The request carries `predecessor_name`, an optional
/// `explicit_selection` (an explicit `%auto` narrows under agent
/// semantics), `actor`, and `now`. Returns `{status, record, reason}`
/// with status `inherited`, `narrowed`, or `refused`.
#[pyfunction]
#[pyo3(name = "autonomy_inherit")]
fn py_autonomy_inherit<'py>(
    py: Python<'py>,
    predecessor: &Bound<'py, PyAny>,
    request: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let predecessor_value = py_to_json_value(predecessor)?;
    let parsed = parse_record(&predecessor_value)?;
    let request_value = py_to_json_value(request)?;
    let name = request_value
        .get("predecessor_name")
        .and_then(|item| item.as_str())
        .unwrap_or("");
    let explicit = request_value
        .get("explicit_selection")
        .and_then(|item| item.as_str());
    let actor: sase_core::autonomy::AutonomyActorWire =
        match request_value.get("actor") {
            Some(actor) => {
                serde_json::from_value(actor.clone()).map_err(|error| {
                    PyValueError::new_err(format!(
                        "invalid autonomy actor: {error}"
                    ))
                })?
            }
            None => sase_core::autonomy::AutonomyActorWire::default(),
        };
    let now = request_value
        .get("now")
        .and_then(|item| item.as_str())
        .unwrap_or("");
    let result = sase_core::autonomy::autonomy_inherit(
        &parsed, name, explicit, &actor, now,
    )
    .map_err(autonomy_error_to_pyerr)?;
    to_py(py, &result)
}

/// The built-in E1 compatibility profile catalog.
#[pyfunction]
#[pyo3(name = "autonomy_profiles")]
fn py_autonomy_profiles<'py>(py: Python<'py>) -> PyResult<PyObject> {
    let profiles = sase_core::autonomy::autonomy_profiles();
    to_py(py, &profiles)
}

/// Append one decision entry to `<sase_home>/autonomy/decisions.jsonl`.
#[pyfunction]
#[pyo3(name = "autonomy_append_decision")]
fn py_autonomy_append_decision(
    home: PathBuf,
    entry: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let value = py_to_json_value(entry)?;
    let parsed: sase_core::autonomy::AutonomyLogEntryWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "entry is not a valid AutonomyLogEntryWire dict: {error}"
            ))
        })?;
    sase_core::autonomy::append_autonomy_decision(&home, &parsed)
        .map_err(autonomy_log_error_to_pyerr)?;
    Ok(())
}

/// Read decision entries newest first with `{since, agent, gate_kind,
/// outcome, limit}` filters.
#[pyfunction]
#[pyo3(name = "autonomy_read_decisions")]
fn py_autonomy_read_decisions<'py>(
    py: Python<'py>,
    home: PathBuf,
    query: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(query)?;
    let parsed: sase_core::autonomy::AutonomyLogQueryWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "query is not a valid AutonomyLogQueryWire dict: {error}"
            ))
        })?;
    let entries = sase_core::autonomy::read_autonomy_decisions(&home, &parsed)
        .map_err(autonomy_log_error_to_pyerr)?;
    to_py(py, &entries)
}

pub(crate) fn register_autonomy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_autonomy_resolve_selection, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_record_from_legacy_meta, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_legacy_projection, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_evaluate, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_summary, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_decision_sentence, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_awareness_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_mutate, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_inherit, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_profiles, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_append_decision, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_read_decisions, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
