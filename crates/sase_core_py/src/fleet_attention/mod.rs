//! Fleet attention inventory bindings.

use crate::prelude::*;

use crate::fleet::{
    fleet_contract_error_to_pyerr, fleet_wire_from_pydict,
    fleet_wire_list_from_pylist, fleet_wire_to_py,
};

use pyo3::wrap_pyfunction;

#[pyfunction]
#[pyo3(name = "fleet_project_attention")]
fn py_fleet_project_attention<'py>(
    py: Python<'py>,
    origin_installation_id: &str,
    rows: &Bound<'py, PyList>,
    resolved: &Bound<'py, PyList>,
    observed_at_unix: f64,
) -> PyResult<PyObject> {
    let origin_installation_id = origin_installation_id.to_string();
    let rows: Vec<FleetAttentionNotificationRowWire> =
        fleet_wire_list_from_pylist(rows, "fleet attention notification row")?;
    let resolved: Vec<FleetAttentionLogicalIdentityWire> =
        fleet_wire_list_from_pylist(
            resolved,
            "fleet attention logical identity",
        )?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::project_fleet_attention(
                &origin_installation_id,
                &rows,
                &resolved,
                observed_at_unix,
            )
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_project_attention_inventory")]
fn py_fleet_project_attention_inventory<'py>(
    py: Python<'py>,
    origin_installation_id: &str,
    rows: &Bound<'py, PyList>,
    resolved: &Bound<'py, PyList>,
    request: &Bound<'py, PyDict>,
    observed_at_unix: f64,
    freshness: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let origin_installation_id = origin_installation_id.to_string();
    let rows: Vec<FleetAttentionNotificationRowWire> =
        fleet_wire_list_from_pylist(rows, "fleet attention notification row")?;
    let resolved: Vec<FleetAttentionLogicalIdentityWire> =
        fleet_wire_list_from_pylist(
            resolved,
            "fleet attention logical identity",
        )?;
    let request: FleetAttentionInventoryRequestWire =
        fleet_wire_from_pydict(request, "fleet attention inventory request")?;
    let freshness: FleetSnapshotFreshnessWire =
        fleet_wire_from_pydict(freshness, "fleet snapshot freshness")?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::project_fleet_attention_inventory(
                &origin_installation_id,
                &rows,
                &resolved,
                &request,
                observed_at_unix,
                freshness,
            )
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_attention_payload_fingerprint")]
fn py_fleet_attention_payload_fingerprint<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: FleetAttentionIntentWire =
        fleet_wire_from_pydict(intent, "fleet attention intent")?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::fleet_attention_payload_fingerprint(&intent)
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_attention_request")]
fn py_fleet_validate_attention_request<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetAttentionRequestWire =
        fleet_wire_from_pydict(request, "fleet attention request")?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::validate_fleet_attention_request(&request)
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_attention_inventory_request")]
fn py_fleet_validate_attention_inventory_request<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetAttentionInventoryRequestWire =
        fleet_wire_from_pydict(request, "fleet attention inventory request")?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::validate_fleet_attention_inventory_request(
                &request,
            )
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_attention_inventory_response")]
fn py_fleet_validate_attention_inventory_response<'py>(
    py: Python<'py>,
    response: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let response: FleetAttentionInventoryResponseWire =
        fleet_wire_from_pydict(response, "fleet attention inventory response")?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::validate_fleet_attention_inventory_response(
                &response,
            )
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "fleet_evaluate_attention_precondition",
    signature = (intent, capabilities, observed=None)
)]
fn py_fleet_evaluate_attention_precondition<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
    capabilities: &Bound<'py, PyDict>,
    observed: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let intent: FleetAttentionIntentWire =
        fleet_wire_from_pydict(intent, "fleet attention intent")?;
    let observed_entry = match observed {
        Some(value) => Some(fleet_wire_from_pydict::<FleetAttentionEntryWire>(
            value,
            "observed fleet attention entry",
        )?),
        None => None,
    };
    let capabilities: CapabilitySetWire =
        fleet_wire_from_pydict(capabilities, "fleet attention capabilities")?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::evaluate_attention_precondition(
                &intent,
                observed_entry.as_ref(),
                &capabilities,
            )
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_decide_attention_notices")]
fn py_fleet_decide_attention_notices<'py>(
    py: Python<'py>,
    current: &Bound<'py, PyList>,
    ledger: &Bound<'py, PyList>,
    retention_window_seconds: f64,
    now_unix: f64,
) -> PyResult<PyObject> {
    let current: Vec<FleetAttentionEntryWire> =
        fleet_wire_list_from_pylist(current, "fleet attention entry")?;
    let ledger: Vec<FleetAttentionNoticeLedgerEntryWire> =
        fleet_wire_list_from_pylist(
            ledger,
            "fleet attention notice ledger entry",
        )?;
    let result = py
        .allow_threads(|| {
            core_fleet_attention::decide_attention_notices(
                &current,
                &ledger,
                retention_window_seconds,
                now_unix,
            )
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

pub(crate) fn register_fleet_attention(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_fleet_project_attention, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_project_attention_inventory, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_attention_payload_fingerprint,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_attention_request, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_validate_attention_inventory_request,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_validate_attention_inventory_response,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_evaluate_attention_precondition,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_decide_attention_notices, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
