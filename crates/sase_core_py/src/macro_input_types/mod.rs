//! Macro input-type catalog, resolver, choice, and value-check bindings.

use std::collections::BTreeMap;

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

use pyo3::wrap_pyfunction;

use sase_core::macro_input_types::{
    builtin_catalog, check_input_value,
    load_plugin_input_type_registry_with_known,
    pyyaml_plain_scalar_is_non_string, resolve_input_type,
    validate_enum_choices, InputTypeRegistry, PluginInputTypeFileRecord,
    ResolvedInputType,
};
use sase_core::model_validity::{
    classify_model_value, ClassifyModelValueRequestWire,
};

use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize)]
struct ResolveInputTypeRequestWire {
    name: String,
    raw: String,
    #[serde(default)]
    plugins: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    registry: Option<InputTypeRegistry>,
}

#[derive(Debug, Deserialize)]
struct LoadRegistryRequestWire {
    #[serde(default)]
    files: Vec<PluginInputTypeFileRecord>,
    #[serde(default)]
    known_distributions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CatalogRequestWire {
    #[serde(default)]
    registry: Option<InputTypeRegistry>,
}

#[derive(Debug, Deserialize)]
struct ValidateEnumChoicesRequestWire {
    items: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct CheckInputValueRequestWire {
    name: String,
    value: String,
    resolved: ResolvedInputType,
}

#[pyfunction]
#[pyo3(name = "macro_input_type_catalog")]
#[pyo3(signature = (request=None))]
fn py_macro_input_type_catalog(
    py: Python<'_>,
    request: Option<&Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let registry = match request {
        None => None,
        Some(request) => {
            let value = py_to_json_value(request.as_any())?;
            // Accept either `{registry: {...}}` or a bare registry snapshot.
            match serde_json::from_value::<CatalogRequestWire>(value.clone()) {
                Ok(wire) if wire.registry.is_some() => wire.registry,
                _ => serde_json::from_value::<InputTypeRegistry>(value).ok(),
            }
        }
    };
    match registry {
        Some(registry) => serialize_to_py(py, &registry.entries()),
        None => serialize_to_py(py, &builtin_catalog()),
    }
}

#[pyfunction]
#[pyo3(name = "load_macro_input_type_registry")]
fn py_load_macro_input_type_registry(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: LoadRegistryRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid load_macro_input_type_registry dict: {error}"
            ))
        })?;
    let (registry, diagnostics) = load_plugin_input_type_registry_with_known(
        &request.files,
        &request.known_distributions,
    );
    let response = serde_json::json!({
        "registry": registry,
        "diagnostics": diagnostics,
    });
    serialize_to_py(py, &response)
}

#[pyfunction]
#[pyo3(name = "resolve_input_type")]
fn py_resolve_input_type(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ResolveInputTypeRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid resolve_input_type dict: {error}"
            ))
        })?;
    let registry = if let Some(snapshot) = request.registry {
        snapshot
    } else if request.plugins.is_empty() {
        InputTypeRegistry::builtin()
    } else {
        InputTypeRegistry::with_plugins(request.plugins)
    };
    match resolve_input_type(&request.name, &request.raw, &registry) {
        Ok(resolved) => serialize_to_py(py, &resolved),
        Err(error) => Err(PyValueError::new_err(error.to_string())),
    }
}

#[pyfunction]
#[pyo3(name = "validate_enum_choices")]
fn py_validate_enum_choices(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ValidateEnumChoicesRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid validate_enum_choices dict: {error}"
            ))
        })?;
    serialize_to_py(py, &validate_enum_choices(&request.items))
}

#[pyfunction]
#[pyo3(name = "pyyaml_plain_scalar_is_non_string")]
fn py_pyyaml_plain_scalar_is_non_string(text: &str) -> bool {
    pyyaml_plain_scalar_is_non_string(text)
}

#[pyfunction]
#[pyo3(name = "check_input_value")]
fn py_check_input_value(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: CheckInputValueRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid check_input_value dict: {error}"
            ))
        })?;
    match check_input_value(&request.resolved, &request.name, &request.value) {
        Ok(()) => Ok(py.None()),
        Err(message) => Ok(message.into_py(py)),
    }
}

#[pyfunction]
#[pyo3(name = "classify_model_value")]
fn py_classify_model_value(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ClassifyModelValueRequestWire = serde_json::from_value(value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid classify_model_value dict: {error}"
            ))
        })?;
    match classify_model_value(&request.name, &request.value, &request.snapshot)
    {
        Ok(result) => serialize_to_py(py, &result),
        Err(error) => Err(PyValueError::new_err(error.to_string())),
    }
}

pub(crate) fn register_macro_input_types(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_macro_input_type_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_load_macro_input_type_registry, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_input_type, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_enum_choices, m)?)?;
    m.add_function(wrap_pyfunction!(py_pyyaml_plain_scalar_is_non_string, m)?)?;
    m.add_function(wrap_pyfunction!(py_check_input_value, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_model_value, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
