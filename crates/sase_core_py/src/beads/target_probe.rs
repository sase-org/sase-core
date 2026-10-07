//! `bead_probe_target_owner` binding: filesystem-only target ownership.
//!
//! Lives in its own file because the beads domain module is at the line
//! cap; it is registered through the domain's `mod.rs` facade.

use std::path::PathBuf;

use pyo3::wrap_pyfunction;

use crate::json_bridge::json_value_to_py;
use crate::prelude::*;

use sase_core::bead::probe_bead_target_owner;

#[pyfunction]
#[pyo3(name = "bead_probe_target_owner")]
pub(crate) fn py_bead_probe_target_owner<'py>(
    py: Python<'py>,
    beads_dir: &str,
    target: &str,
) -> PyResult<PyObject> {
    let outcome = py
        .allow_threads(|| {
            probe_bead_target_owner(&PathBuf::from(beads_dir), target)
        })
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&outcome).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_target_probe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_bead_probe_target_owner, m)?)?;
    Ok(())
}
