//! Footprint ledger bindings.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;
use pyo3::types::PyList;
use pyo3::wrap_pyfunction;
use sase_core::footprint::{
    catalog_status as core_catalog_status, clear as core_clear,
    fold as core_fold, footprint_wire_schema_version as core_wire_version,
    ingest_catalog as core_ingest_catalog, prune as core_prune,
    status as core_status, upsert_derived as core_upsert_derived,
    validate_overlay_shape as core_validate_overlay_shape,
    DerivedCoverageRowWire, DerivedDailyRowWire, FootprintError,
};

fn footprint_error(error: FootprintError) -> PyErr {
    match error {
        FootprintError::Busy => PyTimeoutError::new_err(error.to_string()),
        FootprintError::Validation(_) | FootprintError::NewerSchema { .. } => {
            PyValueError::new_err(error.to_string())
        }
        FootprintError::Io(_) | FootprintError::Sqlite(_) => {
            PyRuntimeError::new_err(error.to_string())
        }
    }
}

fn enrichment_from_py(
    enrichment: &Bound<'_, PyDict>,
) -> PyResult<BTreeMap<String, i64>> {
    let mut map = BTreeMap::new();
    for (key, value) in enrichment.iter() {
        let id: String = key.extract()?;
        let loc: i64 = value.extract().map_err(|_| {
            PyValueError::new_err(format!(
                "enrichment[{id}] must be an integer loc"
            ))
        })?;
        map.insert(id, loc);
    }
    Ok(map)
}

/// Return the Footprint wire schema version.
#[pyfunction]
#[pyo3(name = "footprint_wire_schema_version")]
fn py_footprint_wire_schema_version(py: Python<'_>) -> u32 {
    py.allow_threads(core_wire_version)
}

/// Return `{rev, known, count}` for a catalog file against the ledger.
#[pyfunction]
#[pyo3(
    name = "footprint_catalog_status",
    signature = (store_path, catalog_path, busy_timeout_ms=250)
)]
fn py_footprint_catalog_status<'py>(
    py: Python<'py>,
    store_path: &str,
    catalog_path: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let store_path = PathBuf::from(store_path);
    let catalog_path = PathBuf::from(catalog_path);
    let result = py
        .allow_threads(|| {
            core_catalog_status(
                &store_path,
                &catalog_path,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(footprint_error)?;
    serialize_to_py(py, &result)
}

/// Ingest a catalog JSONL file and apply optional loc enrichment.
#[pyfunction]
#[pyo3(
    name = "footprint_ingest_catalog",
    signature = (store_path, catalog_path, enrichment, today, busy_timeout_ms=250)
)]
fn py_footprint_ingest_catalog<'py>(
    py: Python<'py>,
    store_path: &str,
    catalog_path: &str,
    enrichment: &Bound<'py, PyDict>,
    today: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let enrichment = enrichment_from_py(enrichment)?;
    let store_path = PathBuf::from(store_path);
    let catalog_path = PathBuf::from(catalog_path);
    let today = today.to_string();
    let result = py
        .allow_threads(|| {
            core_ingest_catalog(
                &store_path,
                &catalog_path,
                &enrichment,
                &today,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(footprint_error)?;
    serialize_to_py(py, &result)
}

/// Fold spool JSONL files into the daily ledger.
#[pyfunction]
#[pyo3(
    name = "footprint_fold",
    signature = (store_path, spool_dir, today, spool_keep_days, spool_max_age_days, busy_timeout_ms=250)
)]
fn py_footprint_fold<'py>(
    py: Python<'py>,
    store_path: &str,
    spool_dir: &str,
    today: &str,
    spool_keep_days: u32,
    spool_max_age_days: u32,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let store_path = PathBuf::from(store_path);
    let spool_dir = PathBuf::from(spool_dir);
    let today = today.to_string();
    let result = py
        .allow_threads(|| {
            core_fold(
                &store_path,
                &spool_dir,
                &today,
                spool_keep_days,
                spool_max_age_days,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(footprint_error)?;
    serialize_to_py(py, &result)
}

/// Replace one derived origin's rows for the given days.
#[pyfunction]
#[pyo3(
    name = "footprint_upsert_derived",
    signature = (store_path, origin, days, rows, coverage, busy_timeout_ms=250)
)]
fn py_footprint_upsert_derived<'py>(
    py: Python<'py>,
    store_path: &str,
    origin: &str,
    days: &Bound<'py, PyList>,
    rows: &Bound<'py, PyList>,
    coverage: &Bound<'py, PyList>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let days: Vec<String> = days.extract()?;
    let rows: Vec<DerivedDailyRowWire> = serde_json::from_value(
        py_to_json_value(rows.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "rows is not a valid DerivedDailyRowWire list: {error}"
        ))
    })?;
    let coverage: Vec<DerivedCoverageRowWire> = serde_json::from_value(
        py_to_json_value(coverage.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "coverage is not a valid DerivedCoverageRowWire list: {error}"
        ))
    })?;
    let store_path = PathBuf::from(store_path);
    let origin = origin.to_string();
    py.allow_threads(|| {
        core_upsert_derived(
            &store_path,
            &origin,
            &days,
            &rows,
            &coverage,
            Duration::from_millis(busy_timeout_ms),
        )
    })
    .map_err(footprint_error)?;
    serialize_to_py(py, &true)
}

/// Return ledger status including the last 7 days of coverage.
#[pyfunction]
#[pyo3(
    name = "footprint_status",
    signature = (store_path, spool_dir, today, busy_timeout_ms=250)
)]
fn py_footprint_status<'py>(
    py: Python<'py>,
    store_path: &str,
    spool_dir: &str,
    today: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let store_path = PathBuf::from(store_path);
    let spool_dir = PathBuf::from(spool_dir);
    let today = today.to_string();
    let result = py
        .allow_threads(|| {
            core_status(
                &store_path,
                &spool_dir,
                &today,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(footprint_error)?;
    serialize_to_py(py, &result)
}

/// Prune daily and coverage rows older than `retention_days`.
#[pyfunction]
#[pyo3(
    name = "footprint_prune",
    signature = (store_path, today, retention_days, busy_timeout_ms=250)
)]
fn py_footprint_prune<'py>(
    py: Python<'py>,
    store_path: &str,
    today: &str,
    retention_days: u32,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let store_path = PathBuf::from(store_path);
    let today = today.to_string();
    let result = py
        .allow_threads(|| {
            core_prune(
                &store_path,
                &today,
                retention_days,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(footprint_error)?;
    serialize_to_py(py, &result)
}

/// Delete data rows and spool files and bump generation.
#[pyfunction]
#[pyo3(
    name = "footprint_clear",
    signature = (store_path, spool_dir, busy_timeout_ms=250)
)]
fn py_footprint_clear<'py>(
    py: Python<'py>,
    store_path: &str,
    spool_dir: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let store_path = PathBuf::from(store_path);
    let spool_dir = PathBuf::from(spool_dir);
    let result = py
        .allow_threads(|| {
            core_clear(
                &store_path,
                &spool_dir,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(footprint_error)?;
    serialize_to_py(py, &result)
}

/// Validate overlay JSON produced from YAML by Python.
#[pyfunction]
#[pyo3(name = "footprint_validate_overlay")]
fn py_footprint_validate_overlay<'py>(
    py: Python<'py>,
    overlay: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(overlay.as_any())?;
    py.allow_threads(|| core_validate_overlay_shape(&value))
        .map_err(footprint_error)?;
    serialize_to_py(py, &true)
}

pub(crate) fn register_footprint(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_footprint_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_catalog_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_ingest_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_fold, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_upsert_derived, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_prune, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_clear, m)?)?;
    m.add_function(wrap_pyfunction!(py_footprint_validate_overlay, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
