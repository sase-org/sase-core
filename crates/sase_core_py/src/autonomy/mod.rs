//! Autonomy record bindings: selection resolution, legacy translation,
//! legacy projection, and gate evaluation over explicit option IDs.

use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::autonomy::AutonomyError;

fn autonomy_error_to_pyerr(error: AutonomyError) -> PyErr {
    PyValueError::new_err(error.to_string())
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

pub(crate) fn register_autonomy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_autonomy_resolve_selection, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_record_from_legacy_meta, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_legacy_projection, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_evaluate, m)?)?;
    m.add_function(wrap_pyfunction!(py_autonomy_wire_schema_version, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
