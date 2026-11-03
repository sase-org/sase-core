//! Instruction readout policy bindings.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::instruction_readout::{
    classify_instruction_readout as core_classify_instruction_readout,
    InstructionReadoutRequestWire,
};

/// Apply the Rust-owned declaration and handoff readout policy.
#[pyfunction]
#[pyo3(name = "classify_instruction_readout")]
fn py_classify_instruction_readout<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: InstructionReadoutRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "invalid instruction readout request: {error}"
            ))
        })?;
    let class = core_classify_instruction_readout(request);
    serialize_to_py(py, &class)
}

pub(crate) fn register_instruction_readout(
    module: &Bound<'_, PyModule>,
) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(
        py_classify_instruction_readout,
        module
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
