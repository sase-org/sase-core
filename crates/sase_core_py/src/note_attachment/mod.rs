//! Note-attachment bindings: scan, compose, names, and classification.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::note_attachment::{
    classify_attachment as core_classify_attachment,
    compose_note_attachment_text as core_compose_note_attachment_text,
    note_attachment_source_text as core_note_attachment_source_text,
    sanitize_attachment_name as core_sanitize_attachment_name,
    scan_note_attachment_refs as core_scan_note_attachment_refs,
    stored_attachment_tokens as core_stored_attachment_tokens,
    unique_attachment_name as core_unique_attachment_name,
    AttachmentNameDigestWire, NoteAttachmentScanWire,
};

fn note_attachment_error_to_pyerr(
    error: sase_core::note_attachment::NoteAttachmentError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "classify_attachment")]
fn py_classify_attachment<'py>(
    py: Python<'py>,
    name: &str,
    head: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    let classification = core_classify_attachment(name, head.as_bytes());
    serialize_to_py(py, &classification)
}

#[pyfunction]
#[pyo3(name = "scan_note_attachment_refs")]
fn py_scan_note_attachment_refs<'py>(
    py: Python<'py>,
    text: &str,
    roster_names: Vec<String>,
) -> PyResult<PyObject> {
    let scan = core_scan_note_attachment_refs(text, &roster_names);
    serialize_to_py(py, &scan)
}

#[pyfunction]
#[pyo3(name = "compose_note_attachment_text")]
fn py_compose_note_attachment_text(
    text: &str,
    scan: &Bound<'_, PyAny>,
    assigned_names: Vec<String>,
) -> PyResult<String> {
    let scan: NoteAttachmentScanWire =
        serde_json::from_value(py_to_json_value(scan)?).map_err(|error| {
            PyValueError::new_err(format!(
                "scan is not a valid NoteAttachmentScanWire dict: {error}"
            ))
        })?;
    core_compose_note_attachment_text(text, &scan, &assigned_names)
        .map_err(note_attachment_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "note_attachment_source_text")]
fn py_note_attachment_source_text(
    stored: &str,
    manifest_names: Vec<String>,
) -> PyResult<String> {
    Ok(core_note_attachment_source_text(stored, &manifest_names))
}

#[pyfunction]
#[pyo3(name = "stored_attachment_tokens")]
fn py_stored_attachment_tokens<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<PyObject> {
    let tokens = core_stored_attachment_tokens(text);
    serialize_to_py(py, &tokens)
}

#[pyfunction]
#[pyo3(name = "sanitize_attachment_name")]
fn py_sanitize_attachment_name(candidate: &str) -> PyResult<String> {
    Ok(core_sanitize_attachment_name(candidate))
}

#[pyfunction]
#[pyo3(name = "unique_attachment_name")]
fn py_unique_attachment_name(
    candidate: &str,
    sha256: &str,
    existing: &Bound<'_, PyList>,
) -> PyResult<String> {
    let existing: Vec<AttachmentNameDigestWire> = serde_json::from_value(
        py_to_json_value(existing.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "existing is not a valid list of name/sha256 dicts: {error}"
        ))
    })?;
    Ok(core_unique_attachment_name(candidate, sha256, &existing))
}

pub(crate) fn register_note_attachment(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_classify_attachment, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_note_attachment_refs, m)?)?;
    m.add_function(wrap_pyfunction!(py_compose_note_attachment_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_note_attachment_source_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_stored_attachment_tokens, m)?)?;
    m.add_function(wrap_pyfunction!(py_sanitize_attachment_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_unique_attachment_name, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
