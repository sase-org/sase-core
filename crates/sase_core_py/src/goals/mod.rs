//! Goal ledger bindings: init, append, reads, doctor, and probe.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;
use sase_core::goal::ledger::{
    goal_ledger_append as core_goal_ledger_append,
    goal_ledger_doctor as core_goal_ledger_doctor,
    goal_ledger_history as core_goal_ledger_history,
    goal_ledger_init as core_goal_ledger_init,
    goal_ledger_list as core_goal_ledger_list,
    goal_ledger_show as core_goal_ledger_show,
    goal_projection_status as core_goal_projection_status,
    probe_goal_ledger_list as core_probe_goal_ledger_list,
    refresh_goal_projection as core_refresh_goal_projection,
    GoalDoctorRequestWire, GoalHistoryFilterWire, GoalLedgerAppendRequestWire,
    GoalLedgerError, GoalListFilterWire,
};
use sase_core::goal::{
    goal_card_markdown as core_goal_card_markdown,
    goal_card_view as core_goal_card_view,
    goal_citation_line as core_goal_citation_line,
    mint_goal_id as core_mint_goal_id, GoalCardViewWire,
    GOAL_LEDGER_SCHEMA_VERSION, GOAL_WIRE_SCHEMA_VERSION,
};
use std::path::PathBuf;

#[cfg(test)]
mod tests;

pub(crate) fn goal_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, GoalLedgerError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(
        result.map_err(|error| PyValueError::new_err(error.prefixed()))?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

fn request_from_dict<T>(dict: &Bound<'_, PyDict>) -> PyResult<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    let value = py_to_json_value(dict)?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!("invalid request dict: {error}"))
    })
}

#[pyfunction]
#[pyo3(name = "goal_ledger_wire_schema_version")]
fn py_goal_ledger_wire_schema_version() -> u32 {
    GOAL_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "goal_ledger_store_schema_version")]
fn py_goal_ledger_store_schema_version() -> u32 {
    GOAL_LEDGER_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "goal_mint_id")]
fn py_goal_mint_id() -> String {
    core_mint_goal_id()
}

#[pyfunction]
#[pyo3(name = "goal_ledger_init")]
fn py_goal_ledger_init<'py>(py: Python<'py>, root: &str) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    goal_result_to_py(py, py.allow_threads(|| core_goal_ledger_init(&root)))
}

#[pyfunction]
#[pyo3(name = "goal_ledger_append")]
fn py_goal_ledger_append<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let request: GoalLedgerAppendRequestWire = request_from_dict(request)?;
    goal_result_to_py(
        py,
        py.allow_threads(|| core_goal_ledger_append(&root, &request)),
    )
}

#[pyfunction]
#[pyo3(name = "goal_ledger_list")]
#[pyo3(signature = (root, filter=None))]
fn py_goal_ledger_list<'py>(
    py: Python<'py>,
    root: &str,
    filter: Option<&Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let filter: GoalListFilterWire = match filter {
        Some(dict) => request_from_dict(dict)?,
        None => GoalListFilterWire::default(),
    };
    goal_result_to_py(
        py,
        py.allow_threads(|| core_goal_ledger_list(&root, &filter, None)),
    )
}

#[pyfunction]
#[pyo3(name = "goal_ledger_show")]
fn py_goal_ledger_show<'py>(
    py: Python<'py>,
    root: &str,
    goal_id: &str,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    goal_result_to_py(
        py,
        py.allow_threads(|| core_goal_ledger_show(&root, goal_id, None)),
    )
}

#[pyfunction]
#[pyo3(name = "goal_ledger_history")]
#[pyo3(signature = (root, filter=None))]
fn py_goal_ledger_history<'py>(
    py: Python<'py>,
    root: &str,
    filter: Option<&Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let filter: GoalHistoryFilterWire = match filter {
        Some(dict) => request_from_dict(dict)?,
        None => GoalHistoryFilterWire::default(),
    };
    goal_result_to_py(
        py,
        py.allow_threads(|| core_goal_ledger_history(&root, &filter, None)),
    )
}

#[pyfunction]
#[pyo3(name = "goal_ledger_doctor")]
fn py_goal_ledger_doctor<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let request: GoalDoctorRequestWire = request_from_dict(request)?;
    goal_result_to_py(
        py,
        py.allow_threads(|| core_goal_ledger_doctor(&root, &request)),
    )
}

#[pyfunction]
#[pyo3(name = "goal_projection_status")]
fn py_goal_projection_status<'py>(
    py: Python<'py>,
    root: &str,
    projection_path: &str,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let projection_path = PathBuf::from(projection_path);
    goal_result_to_py(
        py,
        py.allow_threads(|| {
            core_goal_projection_status(&root, &projection_path)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "goal_projection_refresh")]
fn py_goal_projection_refresh<'py>(
    py: Python<'py>,
    root: &str,
    projection_path: &str,
    project: &str,
    mode: &str,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let projection_path = PathBuf::from(projection_path);
    goal_result_to_py(
        py,
        py.allow_threads(|| {
            core_refresh_goal_projection(
                &root,
                &projection_path,
                project,
                mode,
                "",
                "",
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "goal_ledger_probe_list")]
fn py_goal_ledger_probe_list<'py>(
    py: Python<'py>,
    root: &str,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    goal_result_to_py(
        py,
        py.allow_threads(|| {
            core_probe_goal_ledger_list(&root, &GoalListFilterWire::default())
        }),
    )
}

#[pyfunction]
#[pyo3(name = "goal_card_view")]
fn py_goal_card_view<'py>(
    py: Python<'py>,
    root: &str,
    goal_id: &str,
    now: &str,
) -> PyResult<PyObject> {
    let root = PathBuf::from(root);
    let goal_id = goal_id.to_string();
    let now = now.to_string();
    goal_result_to_py(
        py,
        py.allow_threads(|| {
            core_goal_ledger_show(&root, &goal_id, None)
                .map(|state| core_goal_card_view(&state, &now))
        }),
    )
}

#[pyfunction]
#[pyo3(name = "goal_card_markdown")]
fn py_goal_card_markdown<'py>(
    py: Python<'py>,
    card: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let card: GoalCardViewWire = request_from_dict(card)?;
    Ok(py.allow_threads(|| core_goal_card_markdown(&card)))
}

#[pyfunction]
#[pyo3(name = "goal_citation_line")]
fn py_goal_citation_line<'py>(
    py: Python<'py>,
    card: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let card: GoalCardViewWire = request_from_dict(card)?;
    Ok(py.allow_threads(|| core_goal_citation_line(&card)))
}

pub(crate) fn register_goals(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_goal_ledger_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_store_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_mint_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_init, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_append, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_show, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_history, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_doctor, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_projection_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_projection_refresh, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_ledger_probe_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_card_view, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_card_markdown, m)?)?;
    m.add_function(wrap_pyfunction!(py_goal_citation_line, m)?)?;
    Ok(())
}
