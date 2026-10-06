//! `bead_store_fingerprint` binding: exact stat-only bead store token.

use super::bead_result_to_py;
use crate::prelude::*;
use sase_core::bead::{
    bead_store_fingerprint as core_bead_store_fingerprint,
    BEAD_STORE_FINGERPRINT_WIRE_SCHEMA_VERSION,
};

#[pyfunction]
#[pyo3(name = "bead_store_fingerprint_wire_schema_version")]
fn py_bead_store_fingerprint_wire_schema_version() -> u32 {
    BEAD_STORE_FINGERPRINT_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_store_fingerprint")]
fn py_bead_store_fingerprint<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_store_fingerprint(&beads_dir)),
    )
}

/// Register the fingerprint bindings on the extension module.
pub(crate) fn register_fingerprint(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_bead_store_fingerprint_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_store_fingerprint, m)?)?;
    Ok(())
}

#[cfg(test)]
mod fingerprint_tests {
    use super::*;
    use crate::json_bridge::{json_value_to_py, py_to_json_value};
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
    fn store_fingerprint_binding_round_trips() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_store_fingerprint").is_ok());
            assert_eq!(
                py_bead_store_fingerprint_wire_schema_version(),
                1
            );

            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("beads");
            write(&beads_dir.join("config.json"), "{}\n");
            write(
                &beads_dir.join("events").join("manifest.json"),
                "{\"schema_version\":1,\"stream_count\":0}\n",
            );
            write(&beads_dir.join("issues.jsonl"), "[]\n");

            let result =
                py_bead_store_fingerprint(py, beads_dir.to_str().unwrap())
                    .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(
                value,
                json!({
                    "schema_version": 1,
                    "token": value["token"],
                    "layout": "events",
                    "files": value["files"],
                    "streams": value["streams"],
                })
            );
            let token =
                value.get("token").and_then(|token| token.as_str()).unwrap();
            assert_eq!(token.len(), 64);

            // The projection alone never moves an event store's token.
            write(&beads_dir.join("issues.jsonl"), "[{\"id\":\"demo-1\"}]\n");
            let again =
                py_bead_store_fingerprint(py, beads_dir.to_str().unwrap())
                    .unwrap();
            let again = py_to_json_value(again.bind(py)).unwrap();
            assert_eq!(again["token"], json!(token));

            // Missing stores surface as a Python error, never a panic.
            let missing = temp.path().join("missing");
            py_bead_store_fingerprint(py, missing.to_str().unwrap())
                .expect_err("missing store must fail");
        });
    }

    #[test]
    fn store_fingerprint_json_bridge_matches_core_wire() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("beads");
            write(&beads_dir.join("issues.jsonl"), "[]\n");
            let result =
                py_bead_store_fingerprint(py, beads_dir.to_str().unwrap())
                    .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(value["layout"], json!("legacy"));
            let round_tripped = json_value_to_py(py, &value).unwrap();
            let back = py_to_json_value(round_tripped.bind(py)).unwrap();
            assert_eq!(back, value);
        });
    }
}
