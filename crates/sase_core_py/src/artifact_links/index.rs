//! Opaque artifact-link index handle and GIL-releasing query bindings.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;

use sase_core::artifact_link::{
    artifact_link_index_chips as core_artifact_link_index_chips,
    artifact_link_index_project as core_artifact_link_index_project,
    artifact_link_index_rows as core_artifact_link_index_rows,
    artifact_link_index_target as core_artifact_link_index_target,
    compile_artifact_link_index as core_compile_artifact_link_index,
    ArtifactLinkError, ArtifactLinkIndexBuildRequestWire,
    ArtifactLinkIndexProjectRequestWire, ArtifactLinkIndexRowsRequestWire,
    PreparedArtifactLinkIndex, ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
};

use crate::json_bridge::{py_to_json_value, serialize_to_py};

fn artifact_link_error_to_pyerr(error: ArtifactLinkError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn parse_request<T: serde::de::DeserializeOwned>(
    request: &Bound<'_, PyDict>,
    label: &str,
) -> PyResult<T> {
    serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "request is not a valid {label} dict: {error}"
            ))
        },
    )
}

#[pyclass(name = "ArtifactLinkIndexHandle", module = "sase_core_rs", frozen)]
pub(crate) struct PyArtifactLinkIndexHandle {
    index: Arc<PreparedArtifactLinkIndex>,
}

#[pymethods]
impl PyArtifactLinkIndexHandle {
    pub(crate) fn raw_row_count(&self) -> usize {
        self.index.raw_row_count()
    }

    pub(crate) fn chip_ref_count(&self) -> usize {
        self.index.chip_ref_count()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<PyObject> {
        serialize_to_py(py, &self.index.stats())
    }
}

/// Return the artifact-link index wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_index_wire_schema_version")]
pub(crate) fn py_artifact_link_index_wire_schema_version() -> u64 {
    ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION
}

/// Compile an immutable artifact-link index from already-loaded rows.
#[pyfunction]
#[pyo3(name = "artifact_link_index_build")]
pub(crate) fn py_artifact_link_index_build(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyArtifactLinkIndexHandle> {
    let request = parse_request::<ArtifactLinkIndexBuildRequestWire>(
        request,
        "ArtifactLinkIndexBuildRequestWire",
    )?;
    let index = py
        .allow_threads(|| core_compile_artifact_link_index(request))
        .map_err(artifact_link_error_to_pyerr)?;
    Ok(PyArtifactLinkIndexHandle {
        index: Arc::new(index),
    })
}

/// Return native chip records for one ref without enumerating the graph.
#[pyfunction]
#[pyo3(name = "artifact_link_index_chips")]
pub(crate) fn py_artifact_link_index_chips(
    py: Python<'_>,
    handle: &PyArtifactLinkIndexHandle,
    reference: &str,
) -> PyResult<PyObject> {
    let index = Arc::clone(&handle.index);
    let reference = reference.to_string();
    let chips = py.allow_threads(move || {
        core_artifact_link_index_chips(&index, &reference)
    });
    serialize_to_py(py, &chips)
}

/// Return membership and the first indexed target for one ref.
#[pyfunction]
#[pyo3(name = "artifact_link_index_target")]
pub(crate) fn py_artifact_link_index_target(
    py: Python<'_>,
    handle: &PyArtifactLinkIndexHandle,
    reference: &str,
) -> PyResult<PyObject> {
    let index = Arc::clone(&handle.index);
    let reference = reference.to_string();
    let target = py.allow_threads(move || {
        core_artifact_link_index_target(&index, &reference)
    });
    serialize_to_py(py, &target)
}

/// Return matching raw rows in original order for the supplied lookup keys.
#[pyfunction]
#[pyo3(name = "artifact_link_index_rows")]
pub(crate) fn py_artifact_link_index_rows(
    py: Python<'_>,
    handle: &PyArtifactLinkIndexHandle,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request = parse_request::<ArtifactLinkIndexRowsRequestWire>(
        request,
        "ArtifactLinkIndexRowsRequestWire",
    )?;
    let index = Arc::clone(&handle.index);
    let response = py
        .allow_threads(move || core_artifact_link_index_rows(&index, request));
    serialize_to_py(py, &response)
}

/// Project indexed edges that touch the supplied known row identities.
#[pyfunction]
#[pyo3(name = "artifact_link_index_project")]
pub(crate) fn py_artifact_link_index_project(
    py: Python<'_>,
    handle: &PyArtifactLinkIndexHandle,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request = parse_request::<ArtifactLinkIndexProjectRequestWire>(
        request,
        "ArtifactLinkIndexProjectRequestWire",
    )?;
    let index = Arc::clone(&handle.index);
    let response = py.allow_threads(move || {
        core_artifact_link_index_project(&index, request)
    });
    serialize_to_py(py, &response)
}

pub(crate) fn register_artifact_link_index(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_class::<PyArtifactLinkIndexHandle>()?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_index_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_index_build, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_index_chips, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_index_target, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_index_rows, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_index_project, m)?)?;
    Ok(())
}
