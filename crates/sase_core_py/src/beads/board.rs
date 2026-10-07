//! `bead_board_snapshot` binding: the TUI board views from one store read.

use super::bead_result_to_py;
use crate::prelude::*;
use sase_core::bead::board::{
    board_snapshot as core_board_snapshot,
    BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION,
};

#[pyfunction]
#[pyo3(name = "bead_board_snapshot_wire_schema_version")]
fn py_bead_board_snapshot_wire_schema_version() -> u32 {
    BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_board_snapshot")]
fn py_bead_board_snapshot<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(py, py.allow_threads(|| core_board_snapshot(&beads_dir)))
}

/// Register the board-snapshot bindings on the extension module.
pub(crate) fn register_board(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_bead_board_snapshot_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_board_snapshot, m)?)?;
    Ok(())
}

#[cfg(test)]
mod board_tests {
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
    fn board_snapshot_binding_round_trips() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_board_snapshot").is_ok());
            assert_eq!(py_bead_board_snapshot_wire_schema_version(), 1);

            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("beads");
            write(&beads_dir.join("issues.jsonl"), "[]\n");

            let result =
                py_bead_board_snapshot(py, beads_dir.to_str().unwrap())
                    .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(
                value,
                json!({
                    "schema_version": 1,
                    "issues": [],
                    "ready_ids": [],
                    "blocked_ids": [],
                })
            );

            // Missing stores surface as a Python error, never a panic.
            let missing = temp.path().join("missing");
            py_bead_board_snapshot(py, missing.to_str().unwrap())
                .expect_err("missing store must fail");
        });
    }
}
