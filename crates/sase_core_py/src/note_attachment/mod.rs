//! Note-attachment bindings: scan, compose, names, and classification.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::note_attachment::{
    attachment_audience_decision as core_attachment_audience_decision,
    attachment_canonical_extension as core_attachment_canonical_extension,
    attachment_object_digest_from_relpath as core_attachment_digest_from_relpath,
    attachment_placement as core_attachment_placement,
    attachment_public_object_relpath as core_attachment_public_object_relpath,
    attachment_scan_file as core_attachment_scan_file,
    attachment_scanner_rules_version as core_attachment_scanner_rules_version,
    attachment_sensitive_path_reason as core_attachment_sensitive_path_reason,
    attachment_should_auto_fetch as core_attachment_should_auto_fetch,
    classify_attachment as core_classify_attachment,
    compose_note_attachment_text as core_compose_note_attachment_text,
    note_attachment_source_text as core_note_attachment_source_text,
    sanitize_attachment_name as core_sanitize_attachment_name,
    scan_note_attachment_refs as core_scan_note_attachment_refs,
    stored_attachment_tokens as core_stored_attachment_tokens,
    unique_attachment_name as core_unique_attachment_name,
    AttachmentAudienceFactsWire, AttachmentNameDigestWire,
    AttachmentStoreTierWire, NoteAttachmentScanWire,
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

#[pyfunction]
#[pyo3(name = "attachment_placement")]
fn py_attachment_placement<'py>(
    py: Python<'py>,
    size_bytes: u64,
    tiers: &Bound<'py, PyList>,
    local_only: bool,
) -> PyResult<PyObject> {
    let mut parsed = Vec::with_capacity(tiers.len());
    for (idx, item) in tiers.iter().enumerate() {
        let tier: AttachmentStoreTierWire =
            serde_json::from_value(py_to_json_value(&item)?).map_err(
                |error| {
                    PyValueError::new_err(format!(
                        "tiers[{idx}] is not a valid AttachmentStoreTierWire dict: {error}"
                    ))
                },
            )?;
        parsed.push(tier);
    }
    let placement = core_attachment_placement(size_bytes, &parsed, local_only)
        .map_err(note_attachment_error_to_pyerr)?;
    serialize_to_py(py, &placement)
}

#[pyfunction]
#[pyo3(name = "attachment_should_auto_fetch")]
fn py_attachment_should_auto_fetch(size_bytes: u64, cap_bytes: u64) -> bool {
    core_attachment_should_auto_fetch(size_bytes, cap_bytes)
}

#[pyfunction]
#[pyo3(name = "attachment_sensitive_path_reason")]
#[pyo3(signature = (path, home, extra_patterns=None))]
fn py_attachment_sensitive_path_reason(
    path: &str,
    home: &str,
    extra_patterns: Option<Vec<String>>,
) -> Option<String> {
    let extra = extra_patterns.unwrap_or_default();
    core_attachment_sensitive_path_reason(path, home, &extra)
}

#[pyfunction]
#[pyo3(name = "attachment_audience_decision")]
fn py_attachment_audience_decision<'py>(
    py: Python<'py>,
    facts: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let facts: AttachmentAudienceFactsWire = serde_json::from_value(
        py_to_json_value(facts.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid AttachmentAudienceFactsWire dict: {error}"
        ))
    })?;
    let decision = core_attachment_audience_decision(&facts);
    serialize_to_py(py, &decision)
}

#[pyfunction]
#[pyo3(name = "attachment_scan_file")]
#[pyo3(signature = (path, max_bytes, env=None, home="", sase_home=""))]
fn py_attachment_scan_file<'py>(
    py: Python<'py>,
    path: &str,
    max_bytes: u64,
    env: Option<&Bound<'_, PyDict>>,
    home: &str,
    sase_home: &str,
) -> PyResult<PyObject> {
    use std::collections::HashMap;
    let env_map: HashMap<String, String> = match env {
        Some(dict) => serde_json::from_value(py_to_json_value(dict.as_any())?)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "env is not a valid string-to-string dict: {error}"
                ))
            })?,
        None => HashMap::new(),
    };
    let scan =
        core_attachment_scan_file(path, max_bytes, &env_map, home, sase_home);
    serialize_to_py(py, &scan)
}

#[pyfunction]
#[pyo3(name = "attachment_scanner_rules_version")]
fn py_attachment_scanner_rules_version() -> u32 {
    core_attachment_scanner_rules_version()
}

#[pyfunction]
#[pyo3(name = "attachment_canonical_extension")]
fn py_attachment_canonical_extension(mime_type: &str) -> Option<String> {
    core_attachment_canonical_extension(mime_type).map(|ext| ext.to_string())
}

#[pyfunction]
#[pyo3(name = "attachment_public_object_relpath")]
fn py_attachment_public_object_relpath(
    sha256: &str,
    mime_type: &str,
) -> PyResult<String> {
    core_attachment_public_object_relpath(sha256, mime_type)
        .map_err(note_attachment_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "attachment_object_digest_from_relpath")]
fn py_attachment_object_digest_from_relpath(relpath: &str) -> PyResult<String> {
    core_attachment_digest_from_relpath(relpath)
        .map_err(note_attachment_error_to_pyerr)
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
    m.add_function(wrap_pyfunction!(py_attachment_placement, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_should_auto_fetch, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_sensitive_path_reason, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_audience_decision, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_scan_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_scanner_rules_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_canonical_extension, m)?)?;
    m.add_function(wrap_pyfunction!(py_attachment_public_object_relpath, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_attachment_object_digest_from_relpath,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
