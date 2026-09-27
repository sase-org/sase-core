//! Agent tab bindings: name canonicalization, effective-tab resolution,
//! and catalog ordering.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

fn agent_tab_error_to_pyerr(error: AgentTabDomainError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "canonicalize_agent_tab_name")]
fn py_canonicalize_agent_tab_name<'py>(
    py: Python<'py>,
    raw: &str,
) -> PyResult<PyObject> {
    let canonicalized = core_canonicalize_agent_tab_name(raw)
        .map_err(agent_tab_error_to_pyerr)?;
    serialize_to_py(py, &canonicalized)
}

#[pyfunction]
#[pyo3(name = "resolve_effective_agent_tab")]
fn py_resolve_effective_agent_tab<'py>(
    py: Python<'py>,
    root: &Bound<'py, PyDict>,
    machine_mode: bool,
) -> PyResult<PyObject> {
    let value = py_to_json_value(root.as_any())?;
    let root: AgentTabRootWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "agent tab root must be an object with agent_tab and owner: {error}"
            ))
        })?;
    let resolved = core_resolve_effective_agent_tab(&root, machine_mode);
    serialize_to_py(py, &resolved)
}

#[pyfunction]
#[pyo3(name = "build_agent_tab_catalog")]
fn py_build_agent_tab_catalog<'py>(
    py: Python<'py>,
    roots: &Bound<'py, PyList>,
    options: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let roots_value = py_to_json_value(roots.as_any())?;
    let roots: Vec<AgentTabRootWire> = serde_json::from_value(roots_value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "agent tab roots must be a list of root objects: {error}"
            ))
        })?;
    let options_value = py_to_json_value(options.as_any())?;
    let options: AgentTabCatalogOptionsWire =
        serde_json::from_value(options_value).map_err(|error| {
            PyValueError::new_err(format!(
                "agent tab options must be an object: {error}"
            ))
        })?;
    let catalog = core_build_agent_tab_catalog(&roots, &options);
    serialize_to_py(py, &catalog)
}

pub(crate) fn register_agent_tab(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_canonicalize_agent_tab_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_effective_agent_tab, m)?)?;
    m.add_function(wrap_pyfunction!(py_build_agent_tab_catalog, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
