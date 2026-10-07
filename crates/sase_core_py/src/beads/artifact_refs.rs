//! `bead_referenced_artifact_ids` binding: bead-referenced artifact IDs.

use super::bead_result_to_py;
use crate::prelude::*;
use sase_core::bead::{
    bead_referenced_artifact_ids as core_bead_referenced_artifact_ids,
    BEAD_REFERENCED_ARTIFACT_IDS_WIRE_SCHEMA_VERSION,
};

#[pyfunction]
#[pyo3(name = "bead_referenced_artifact_ids_wire_schema_version")]
fn py_bead_referenced_artifact_ids_wire_schema_version() -> u32 {
    BEAD_REFERENCED_ARTIFACT_IDS_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_referenced_artifact_ids")]
fn py_bead_referenced_artifact_ids<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_referenced_artifact_ids(&beads_dir)),
    )
}

/// Register the referenced-artifact-IDs bindings on the extension module.
pub(crate) fn register_artifact_refs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_bead_referenced_artifact_ids_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_referenced_artifact_ids, m)?)?;
    Ok(())
}

#[cfg(test)]
mod artifact_refs_tests {
    use super::*;
    use crate::json_bridge::py_to_json_value;
    use crate::sase_core_rs;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    fn write(path: &std::path::Path, text: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, text).unwrap();
    }

    #[test]
    fn referenced_artifact_ids_binding_round_trips() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_referenced_artifact_ids").is_ok());
            assert_eq!(
                py_bead_referenced_artifact_ids_wire_schema_version(),
                1
            );

            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("beads");
            let id = "default:0123456789abcdef01234567";
            write(
                &beads_dir.join("issues.jsonl"),
                &format!(
                    "{{\"id\":\"health-1\",\"title\":\"Health\",\
                      \"status\":\"open\",\"issue_type\":\"plan\",\
                      \"parent_id\":null,\"owner\":\"\",\"assignee\":\"\",\
                      \"created_at\":\"2026-01-01T00:00:00Z\",\
                      \"created_by\":\"\",\
                      \"updated_at\":\"2026-01-01T00:00:00Z\",\
                      \"closed_at\":null,\"close_reason\":null,\
                      \"description\":\"\",\"notes\":\"\",\"design\":\"\",\
                      \"is_ready_to_work\":false,\"changespec_name\":\"\",\
                      \"changespec_bug_id\":\"\",\"dependencies\":[],\
                      \"refs\":[\"file:{id}\"]}}\n"
                ),
            );

            let result = py_bead_referenced_artifact_ids(
                py,
                beads_dir.to_str().unwrap(),
            )
            .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(value, json!([id]));

            // Missing stores surface as a Python error, never a panic.
            let missing = temp.path().join("missing");
            py_bead_referenced_artifact_ids(py, missing.to_str().unwrap())
                .expect_err("missing store must fail");
        });
    }
}
