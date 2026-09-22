//! Project tag bindings: scan, resolve, expand, trigger, and
//! apply-selection.
//!
//! The core works in UTF-8 byte offsets; every offset crossing this boundary
//! is converted to Python code-point offsets, following the
//! `xprompt_argument_spans` convention.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::project_tag::{
    apply_project_tag_selection, expand_project_tags, project_tag_trigger,
    resolve_project_tag, scan_project_tags, ProjectTagTargetWire,
};

/// Convert a UTF-8 byte offset into a Python code-point offset.
fn byte_to_char_offset(text: &str, byte: usize) -> usize {
    text.get(..byte.min(text.len()))
        .map_or_else(|| text.chars().count(), |prefix| prefix.chars().count())
}

/// Convert a Python code-point offset into a UTF-8 byte offset.
fn char_to_byte_offset(text: &str, char_offset: usize) -> Option<usize> {
    let mut count = 0;
    for (byte, _) in text.char_indices() {
        if count == char_offset {
            return Some(byte);
        }
        count += 1;
    }
    (count == char_offset).then_some(text.len())
}

fn parse_targets(
    targets: &Bound<'_, PyList>,
) -> PyResult<Vec<ProjectTagTargetWire>> {
    serde_json::from_value(py_to_json_value(targets.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "targets is not a valid list of ProjectTagTargetWire dicts: {error}"
            ))
        },
    )
}

/// Scan `text` for project tags, returning span dicts with Python
/// code-point offsets.
#[pyfunction]
#[pyo3(name = "project_tag_scan")]
fn py_project_tag_scan(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let spans = scan_project_tags(text)
        .iter()
        .map(|span| {
            serde_json::json!({
                "start": byte_to_char_offset(text, span.start),
                "end": byte_to_char_offset(text, span.end),
                "name_start": byte_to_char_offset(text, span.name_start),
                "name": span.name,
                "anchored": span.anchored,
            })
        })
        .collect::<Vec<_>>();
    json_value_to_py(py, &serde_json::Value::Array(spans))
}

/// Resolve `name` against `targets`, returning a resolution dict.
#[pyfunction]
#[pyo3(name = "project_tag_resolve")]
fn py_project_tag_resolve(
    py: Python<'_>,
    name: &str,
    targets: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let targets = parse_targets(targets)?;
    let resolution = resolve_project_tag(name, &targets);
    serialize_to_py(py, &resolution)
}

/// Expand resolved tags in `text` to `#<workflow_type>:<key>` refs.
///
/// Span offsets in each per-tag report are Python code-point offsets.
#[pyfunction]
#[pyo3(name = "project_tag_expand")]
fn py_project_tag_expand(
    py: Python<'_>,
    text: &str,
    targets: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let targets = parse_targets(targets)?;
    let expansion = expand_project_tags(text, &targets);
    let tags = expansion
        .tags
        .iter()
        .map(|tag| {
            serde_json::json!({
                "start": byte_to_char_offset(text, tag.start),
                "end": byte_to_char_offset(text, tag.end),
                "name_start": byte_to_char_offset(text, tag.name_start),
                "name": tag.name,
                "anchored": tag.anchored,
                "resolution": tag.resolution,
                "replacement": tag.replacement,
            })
        })
        .collect::<Vec<_>>();
    json_value_to_py(
        py,
        &serde_json::json!({
            "text": expansion.text,
            "tags": tags,
        }),
    )
}

/// Detect a live `+query` trigger at code-point `cursor`, or `None`.
#[pyfunction]
#[pyo3(name = "project_tag_trigger")]
fn py_project_tag_trigger(
    py: Python<'_>,
    text: &str,
    cursor: usize,
) -> PyResult<PyObject> {
    let trigger = char_to_byte_offset(text, cursor)
        .and_then(|byte| project_tag_trigger(text, byte));
    let Some(trigger) = trigger else {
        return Ok(py.None().into());
    };
    json_value_to_py(
        py,
        &serde_json::json!({
            "start": byte_to_char_offset(text, trigger.start),
            "end": byte_to_char_offset(text, trigger.end),
            "query": trigger.query,
        }),
    )
}

/// Apply an accepted row: replace the `trigger_span` token (code-point
/// offsets) with `insertion` and delete other workspace targets in the same
/// `---` segment. Returns `{text, cursor}` with a code-point cursor.
#[pyfunction]
#[pyo3(name = "project_tag_apply_selection")]
#[pyo3(signature = (text, trigger_span, insertion, workflow_names, targets))]
fn py_project_tag_apply_selection(
    py: Python<'_>,
    text: &str,
    trigger_span: (usize, usize),
    insertion: &str,
    workflow_names: Vec<String>,
    targets: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let targets = parse_targets(targets)?;
    let (start, end) = trigger_span;
    let (Some(start), Some(end)) = (
        char_to_byte_offset(text, start),
        char_to_byte_offset(text, end),
    ) else {
        return Err(PyValueError::new_err("trigger_span is outside the text"));
    };
    let applied = apply_project_tag_selection(
        text,
        (start, end),
        insertion,
        &workflow_names,
        &targets,
    );
    json_value_to_py(
        py,
        &serde_json::json!({
            "text": applied.text,
            "cursor": byte_to_char_offset(&applied.text, applied.cursor),
        }),
    )
}

pub(crate) fn register_project_tag(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_project_tag_scan, m)?)?;
    m.add_function(wrap_pyfunction!(py_project_tag_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_project_tag_expand, m)?)?;
    m.add_function(wrap_pyfunction!(py_project_tag_trigger, m)?)?;
    m.add_function(wrap_pyfunction!(py_project_tag_apply_selection, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
