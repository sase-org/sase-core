//! Instruction-manifest bindings: wire version and normalize.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

use sase_core::instruction_manifest::{
    instruction_manifest_wire_schema_version as core_wire_version,
    normalize_instruction_manifest as core_normalize,
};

/// Return the instruction-manifest wire schema version.
#[pyfunction]
#[pyo3(name = "instruction_manifest_wire_schema_version")]
fn py_instruction_manifest_wire_schema_version() -> u32 {
    core_wire_version()
}

/// Validate and normalize an instruction manifest dict.
///
/// Fills `bundle.common_digest` when null and errors when a supplied
/// value differs or any invariant fails. Errors surface as `ValueError`.
#[pyfunction]
#[pyo3(name = "normalize_instruction_manifest")]
fn py_normalize_instruction_manifest<'py>(
    py: Python<'py>,
    manifest: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(manifest.as_any())?;
    let normalized = core_normalize(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &normalized)
}

pub(crate) fn register_instruction_manifest(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_instruction_manifest_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_normalize_instruction_manifest, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
