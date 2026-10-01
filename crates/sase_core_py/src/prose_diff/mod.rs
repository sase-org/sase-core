//! Prose-diff bindings: Markdown-aware text comparison.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::prose_diff::{
    compare_prose as core_compare_prose,
    prose_diff_wire_schema_version as core_wire_version,
    ProseCompareRequestWire,
};

/// Return the prose-diff wire schema version.
#[pyfunction]
#[pyo3(name = "prose_diff_wire_schema_version")]
fn py_prose_diff_wire_schema_version() -> u32 {
    core_wire_version()
}

/// Compare two texts and return the prose comparison wire dict.
///
/// The request dict mirrors `ProseCompareRequestWire`
/// (`base`, `target`, `format`, `context_lines`). The comparison runs
/// without holding the GIL.
#[pyfunction]
#[pyo3(name = "compare_prose")]
fn py_compare_prose<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ProseCompareRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid ProseCompareRequestWire dict: {e}"
            ))
        })?;
    let out = py.allow_threads(|| core_compare_prose(&req));
    serialize_to_py(py, &out)
}

pub(crate) fn register_prose_diff(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_prose_diff_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_compare_prose, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
