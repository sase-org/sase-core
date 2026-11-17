//! Prompt command-substitution policy bindings.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;
use sase_core::prompt_command_substitution::{
    plan_prompt_command_substitution_from_json as core_plan,
    prompt_command_substitution_wire_schema_version as core_schema_version,
};

/// Return the prompt command-substitution wire schema version.
#[pyfunction]
#[pyo3(name = "prompt_command_substitution_wire_schema_version")]
fn py_prompt_command_substitution_wire_schema_version() -> u32 {
    core_schema_version()
}

/// Plan command-substitution replacements from a request dict.
///
/// The core owns eligibility and span planning. The host executes commands.
#[pyfunction]
#[pyo3(name = "plan_prompt_command_substitution")]
fn py_plan_prompt_command_substitution<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let plan = core_plan(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &plan)
}

pub(crate) fn register_prompt_command_substitution(
    module: &Bound<'_, PyModule>,
) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(
        py_prompt_command_substitution_wire_schema_version,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        py_plan_prompt_command_substitution,
        module
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
