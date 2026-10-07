//! `bead_seal_watch_triggers` binding: measurable sealed-archive
//! triggers for doctor.

use crate::json_bridge::json_value_to_py;
use crate::prelude::*;
use sase_core::bead::{
    bead_seal_watch_triggers as core_bead_seal_watch_triggers,
    BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION,
};

#[pyfunction]
#[pyo3(name = "bead_seal_watch_triggers_wire_schema_version")]
fn py_bead_seal_watch_triggers_wire_schema_version() -> u32 {
    BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_seal_watch_triggers")]
fn py_bead_seal_watch_triggers<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    // The watch never fails: an unusable store is a report, not an error.
    let report = py.allow_threads(|| core_bead_seal_watch_triggers(&beads_dir));
    serde_json::to_value(&report)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal bead seal-watch serialize error: {error}"
            ))
        })
        .and_then(|value| json_value_to_py(py, &value))
}

/// Register the seal-watch bindings on the extension module.
pub(crate) fn register_seal_watch(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_bead_seal_watch_triggers_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_seal_watch_triggers, m)?)?;
    Ok(())
}

#[cfg(test)]
mod seal_watch_tests {
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
    fn seal_watch_bindings_round_trip() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_seal_watch_triggers").is_ok());
            assert_eq!(py_bead_seal_watch_triggers_wire_schema_version(), 1);

            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("beads");
            seed_event_store(&beads_dir);

            let report =
                py_bead_seal_watch_triggers(py, beads_dir.to_str().unwrap())
                    .unwrap();
            let report = py_to_json_value(report.bind(py)).unwrap();
            assert_eq!(report["available"], json!(true));
            assert_eq!(report["warn"], json!(false));
            assert_eq!(report["triggers"].as_array().unwrap().len(), 3);

            // Missing stores surface as reports, never panics.
            let missing = temp.path().join("missing");
            let report =
                py_bead_seal_watch_triggers(py, missing.to_str().unwrap())
                    .unwrap();
            let report = py_to_json_value(report.bind(py)).unwrap();
            assert_eq!(report["available"], json!(false));
        });
    }
}
