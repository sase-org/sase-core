//! Clan dependency-wave projection binding.

use crate::prelude::*;

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use pyo3::wrap_pyfunction;

#[pyfunction]
#[pyo3(name = "project_clan_waves")]
fn py_project_clan_waves<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ClanWaveRequestWire = serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "clan wave request must contain clan_names and keyed members: {error}"
            ))
        })?;
    let waves = py.allow_threads(|| core_project_clan_waves(&request));
    serialize_to_py(py, &waves)
}

pub(crate) fn register_clan_waves(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_project_clan_waves, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
