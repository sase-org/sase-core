//! Store migration bindings and the bounded migration lock.

use crate::prelude::*;

use crate::json_bridge::{
    json_value_from_pydict, json_value_to_py, py_to_json_value,
};

use pyo3::wrap_pyfunction;

#[pyclass(name = "MigrationBoundedLockHandle", module = "sase_core_rs")]
#[derive(Debug)]
struct PyMigrationBoundedLockHandle {
    lock: Option<MigrationHeldLock>,
}

#[pymethods]
impl PyMigrationBoundedLockHandle {
    #[getter]
    fn waited_ms(&self) -> u64 {
        self.lock
            .as_ref()
            .map(MigrationHeldLock::waited_ms)
            .unwrap_or(0)
    }

    fn release(&mut self) -> PyResult<()> {
        let Some(mut lock) = self.lock.take() else {
            return Ok(());
        };
        lock.release().map_err(migration_lock_error_to_pyerr)
    }
}

#[pyfunction]
#[pyo3(name = "migration_wire_schema_version")]
fn py_migration_wire_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "migration_manifest_normalize")]
fn py_migration_manifest_normalize<'py>(
    py: Python<'py>,
    manifest: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let manifest = migration_manifest_from_pydict(manifest)?;
    migration_value_to_py(py, &manifest)
}

#[pyfunction]
#[pyo3(name = "migration_journal_record_normalize")]
fn py_migration_journal_record_normalize<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let record = migration_journal_record_from_pydict(record)?;
    migration_value_to_py(py, &record)
}

#[pyfunction]
#[pyo3(name = "migration_plan_next_step")]
fn py_migration_plan_next_step<'py>(
    py: Python<'py>,
    manifest: &Bound<'py, PyDict>,
    records: &Bound<'py, PyList>,
    observed_source_digests: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let manifest = migration_manifest_from_pydict(manifest)?;
    let records = migration_journal_records_from_py_list(records)?;
    let observed_source_digests =
        migration_source_digests_from_pydict(observed_source_digests)?;
    let plan = core_migration_plan_next_step(
        &manifest,
        &records,
        &observed_source_digests,
    );
    migration_value_to_py(py, &plan)
}

#[pyfunction]
#[pyo3(name = "migration_tree_digest")]
fn py_migration_tree_digest<'py>(
    py: Python<'py>,
    root: &str,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let digest = py
        .allow_threads(|| core_migration_tree_digest(&root))
        .map_err(migration_digest_error_to_pyerr)?;
    migration_value_to_py(py, &digest)
}

#[pyfunction]
#[pyo3(name = "migration_fingerprint")]
fn py_migration_fingerprint(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let value = py_to_json_value(value)?;
    core_migration_fingerprint(&value).map_err(migration_digest_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "migration_residue_classify")]
fn py_migration_residue_classify<'py>(
    py: Python<'py>,
    entry: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let entry = migration_residue_entry_from_pydict(entry)?;
    let facts = migration_residue_facts_from_pydict(facts)?;
    let classification = core_migration_residue_classify(&entry, &facts);
    migration_value_to_py(py, &classification)
}

#[pyfunction]
#[pyo3(name = "migration_reconcile_procs")]
fn py_migration_reconcile_procs<'py>(
    py: Python<'py>,
    legacy_rows: &Bound<'py, PyList>,
    canonical_proc_ids: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let legacy_rows = migration_legacy_proc_rows_from_py_list(legacy_rows)?;
    let canonical_proc_ids =
        migration_canonical_proc_refs_from_py_list(canonical_proc_ids)?;
    let plan =
        core_migration_reconcile_procs(&legacy_rows, &canonical_proc_ids);
    migration_value_to_py(py, &plan)
}

#[pyfunction]
#[pyo3(name = "migration_patch_records_plan")]
fn py_migration_patch_records_plan<'py>(
    py: Python<'py>,
    path: &str,
    data: &Bound<'py, PyBytes>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let facts = patch_records_facts_from_pydict(facts)?;
    let plan = core_migration_patch_records_plan(path, data.as_bytes(), &facts);
    migration_value_to_py(py, &plan)
}

#[pyfunction]
#[pyo3(name = "migration_patch_records_apply")]
fn py_migration_patch_records_apply<'py>(
    py: Python<'py>,
    path: &str,
    data: &Bound<'py, PyBytes>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let facts = patch_records_facts_from_pydict(facts)?;
    let applied =
        core_migration_patch_records_apply(path, data.as_bytes(), &facts);
    migration_value_to_py(py, &applied)
}

#[pyfunction]
#[pyo3(name = "migration_patch_records_verify")]
fn py_migration_patch_records_verify<'py>(
    py: Python<'py>,
    path: &str,
    original: &Bound<'py, PyBytes>,
    converted: &Bound<'py, PyBytes>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let facts = patch_records_facts_from_pydict(facts)?;
    let verified = core_migration_patch_records_verify(
        path,
        original.as_bytes(),
        converted.as_bytes(),
        &facts,
    );
    migration_value_to_py(py, &verified)
}

#[pyfunction]
#[pyo3(name = "migration_gate_bundles_plan")]
fn py_migration_gate_bundles_plan<'py>(
    py: Python<'py>,
    envelope: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let envelope = json_value_from_pydict(envelope)?;
    let facts = gate_bundle_facts_from_pydict(facts)?;
    let plan = core_migration_gate_bundles_plan(&envelope, &facts);
    migration_value_to_py(py, &plan)
}

#[pyfunction]
#[pyo3(name = "migration_gate_bundles_apply")]
fn py_migration_gate_bundles_apply<'py>(
    py: Python<'py>,
    envelope: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let envelope = json_value_from_pydict(envelope)?;
    let facts = gate_bundle_facts_from_pydict(facts)?;
    let applied = core_migration_gate_bundles_apply(&envelope, &facts);
    migration_value_to_py(py, &applied)
}

#[pyfunction]
#[pyo3(name = "migration_gate_bundles_verify")]
fn py_migration_gate_bundles_verify<'py>(
    py: Python<'py>,
    original: &Bound<'py, PyDict>,
    converted: &Bound<'py, PyDict>,
    facts: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let original = json_value_from_pydict(original)?;
    let converted = json_value_from_pydict(converted)?;
    let facts = gate_bundle_facts_from_pydict(facts)?;
    let verified =
        core_migration_gate_bundles_verify(&original, &converted, &facts);
    migration_value_to_py(py, &verified)
}

#[pyfunction]
#[pyo3(name = "migration_acquire_bounded_lock")]
fn py_migration_acquire_bounded_lock(
    py: Python<'_>,
    lock_path: &str,
    timeout_ms: u64,
    operation: &str,
) -> PyResult<PyMigrationBoundedLockHandle> {
    let lock_path = PathBuf::from(lock_path);
    let lock = py
        .allow_threads(|| {
            core_migration_acquire_bounded_lock(
                &lock_path, timeout_ms, operation,
            )
        })
        .map_err(migration_lock_error_to_pyerr)?;
    Ok(PyMigrationBoundedLockHandle { lock: Some(lock) })
}

fn migration_manifest_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<MigrationManifest> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "manifest is not a valid MigrationManifest dict: {error}"
        ))
    })
}

fn migration_journal_record_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<MigrationJournalRecord> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid MigrationJournalRecord dict: {error}"
        ))
    })
}

fn migration_journal_records_from_py_list(
    records: &Bound<'_, PyList>,
) -> PyResult<Vec<MigrationJournalRecord>> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let value = py_to_json_value(&record)?;
            serde_json::from_value(value).map_err(|error| {
                PyValueError::new_err(format!(
                    "records[{index}] is not a valid MigrationJournalRecord dict: {error}"
                ))
            })
        })
        .collect()
}

fn migration_source_digests_from_pydict(
    digests: &Bound<'_, PyDict>,
) -> PyResult<BTreeMap<String, String>> {
    serde_json::from_value(py_to_json_value(digests.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "observed_source_digests must be a string-to-string dict: {error}"
            ))
        },
    )
}

fn migration_residue_entry_from_pydict(
    entry: &Bound<'_, PyDict>,
) -> PyResult<MigrationResidueEntryWire> {
    serde_json::from_value(py_to_json_value(entry.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "entry is not a valid MigrationResidueEntryWire dict: {error}"
        ))
    })
}

fn migration_residue_facts_from_pydict(
    facts: &Bound<'_, PyDict>,
) -> PyResult<MigrationResidueFactsWire> {
    serde_json::from_value(py_to_json_value(facts.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid MigrationResidueFactsWire dict: {error}"
        ))
    })
}

fn patch_records_facts_from_pydict(
    facts: &Bound<'_, PyDict>,
) -> PyResult<PatchRecordsConvertFactsWire> {
    serde_json::from_value(py_to_json_value(facts.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid PatchRecordsConvertFactsWire dict: {error}"
        ))
    })
}

fn gate_bundle_facts_from_pydict(
    facts: &Bound<'_, PyDict>,
) -> PyResult<GateBundleConvertFactsWire> {
    serde_json::from_value(py_to_json_value(facts.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "facts is not a valid GateBundleConvertFactsWire dict: {error}"
        ))
    })
}

fn migration_legacy_proc_rows_from_py_list(
    rows: &Bound<'_, PyList>,
) -> PyResult<Vec<MigrationLegacyProcRowWire>> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            serde_json::from_value(py_to_json_value(&row)?).map_err(|error| {
                PyValueError::new_err(format!(
                    "legacy_rows[{index}] is not a valid MigrationLegacyProcRowWire dict: {error}"
                ))
            })
        })
        .collect()
}

fn migration_canonical_proc_refs_from_py_list(
    refs: &Bound<'_, PyList>,
) -> PyResult<Vec<MigrationCanonicalProcRefWire>> {
    refs.iter()
        .enumerate()
        .map(|(index, proc_ref)| {
            if let Ok(proc_id) = proc_ref.extract::<String>() {
                return Ok(MigrationCanonicalProcRefWire {
                    proc_id,
                    ..Default::default()
                });
            }
            serde_json::from_value(py_to_json_value(&proc_ref)?).map_err(
                |error| {
                    PyValueError::new_err(format!(
                        "canonical_proc_ids[{index}] is not a string or MigrationCanonicalProcRefWire dict: {error}"
                    ))
                },
            )
        })
        .collect()
}

fn migration_value_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "internal migration serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

fn migration_digest_error_to_pyerr(error: MigrationDigestError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn migration_lock_error_to_pyerr(error: MigrationLockError) -> PyErr {
    match error {
        MigrationLockError::Timeout(message) => {
            PyTimeoutError::new_err(message)
        }
        other => PyValueError::new_err(other.to_string()),
    }
}

pub(crate) fn register_migration(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMigrationBoundedLockHandle>()?;
    m.add_function(wrap_pyfunction!(py_migration_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_manifest_normalize, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_migration_journal_record_normalize,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_migration_plan_next_step, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_tree_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_residue_classify, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_reconcile_procs, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_patch_records_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_patch_records_apply, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_patch_records_verify, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_gate_bundles_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_gate_bundles_apply, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_gate_bundles_verify, m)?)?;
    m.add_function(wrap_pyfunction!(py_migration_acquire_bounded_lock, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
