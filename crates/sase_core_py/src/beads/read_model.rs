//! `bead_read_model_status` and `bead_read_model_verify_cache`
//! bindings: cache health and cache-vs-replay parity for doctor.

use crate::json_bridge::json_value_to_py;
use crate::prelude::*;
use sase_core::bead::{
    read_model_status as core_read_model_status,
    read_model_verify_cache as core_read_model_verify_cache,
    BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
    BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION,
};

#[pyfunction]
#[pyo3(name = "bead_read_model_status_wire_schema_version")]
fn py_bead_read_model_status_wire_schema_version() -> u32 {
    BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_read_model_verify_wire_schema_version")]
fn py_bead_read_model_verify_wire_schema_version() -> u32 {
    BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_read_model_status")]
fn py_bead_read_model_status<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    // Status never fails: an unusable cache is a report, not an error.
    let status = py.allow_threads(|| core_read_model_status(&beads_dir));
    serde_json::to_value(&status)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal bead read-model status serialize error: {error}"
            ))
        })
        .and_then(|value| json_value_to_py(py, &value))
}

#[pyfunction]
#[pyo3(name = "bead_read_model_verify_cache")]
fn py_bead_read_model_verify_cache<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    // Verify never fails either: drift is a report, not an error.
    let report = py.allow_threads(|| core_read_model_verify_cache(&beads_dir));
    serde_json::to_value(&report)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal bead read-model verify serialize error: {error}"
            ))
        })
        .and_then(|value| json_value_to_py(py, &value))
}

/// Register the read-model bindings on the extension module.
pub(crate) fn register_read_model(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_bead_read_model_status_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_read_model_verify_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_model_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_model_verify_cache, m)?)?;
    Ok(())
}

#[cfg(test)]
mod read_model_tests {
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

    fn seed_event_store(beads_dir: &std::path::Path) {
        use sase_core::bead::events::import_issues_to_event_streams;
        use sase_core::bead::jsonl::{parse_issues_jsonl, write_event_store};
        write(&beads_dir.join("config.json"), "{}\n");
        let outcome = parse_issues_jsonl(
            "{\"id\":\"beads-1\",\"title\":\"Epic\",\"status\":\"open\",\"issue_type\":\"plan\",\"created_at\":\"2026-01-01T00:00:00Z\"}\n",
        );
        assert_eq!(outcome.loaded_rows, 1);
        let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
        write_event_store(beads_dir, &streams).unwrap();
    }

    #[test]
    fn read_model_bindings_round_trip() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in
                ["bead_read_model_status", "bead_read_model_verify_cache"]
            {
                assert!(module.getattr(name).is_ok(), "{name}");
            }
            assert_eq!(py_bead_read_model_status_wire_schema_version(), 2);
            assert_eq!(py_bead_read_model_verify_wire_schema_version(), 1);

            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("beads");
            seed_event_store(&beads_dir);

            // No git dir: status reports plain replay, verify declines.
            let status =
                py_bead_read_model_status(py, beads_dir.to_str().unwrap())
                    .unwrap();
            let status = py_to_json_value(status.bind(py)).unwrap();
            assert_eq!(status["location"], json!(null));
            assert_eq!(status["fresh"], json!(false));
            let verify = py_bead_read_model_verify_cache(
                py,
                beads_dir.to_str().unwrap(),
            )
            .unwrap();
            let verify = py_to_json_value(verify.bind(py)).unwrap();
            assert_eq!(verify["compared"], json!(false));

            // Missing stores surface as reports, never panics.
            let missing = temp.path().join("missing");
            let status =
                py_bead_read_model_status(py, missing.to_str().unwrap())
                    .unwrap();
            let status = py_to_json_value(status.bind(py)).unwrap();
            assert_eq!(status["fresh"], json!(false));
        });
    }
}
