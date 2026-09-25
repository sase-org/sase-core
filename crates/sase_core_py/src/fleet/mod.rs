//! Fleet contract bindings: identity, operations, bootstrap, and gateway entry points.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

// --- Portable fleet identity and operation contracts ---------------------
pub(crate) fn fleet_contract_error_to_pyerr(
    err: FleetContractDomainError,
) -> PyErr {
    let message = err.to_string();
    match err {
        FleetContractDomainError::Validation(_) => {
            PyValueError::new_err(message)
        }
        FleetContractDomainError::LockTimeout { .. }
        | FleetContractDomainError::Io { .. }
        | FleetContractDomainError::Json { .. } => {
            PyRuntimeError::new_err(message)
        }
    }
}

fn fleet_store_error_to_pyerr(err: sase_gateway::FleetStoreError) -> PyErr {
    let message = err.to_string();
    match &err {
        sase_gateway::FleetStoreError::Validation(_)
        | sase_gateway::FleetStoreError::IncompatibleProtocol
        | sase_gateway::FleetStoreError::BootstrapExpired
        | sase_gateway::FleetStoreError::BootstrapConsumed
        | sase_gateway::FleetStoreError::BootstrapRejected
        | sase_gateway::FleetStoreError::CredentialExpired
        | sase_gateway::FleetStoreError::CredentialMissing
        | sase_gateway::FleetStoreError::CredentialRevoked
        | sase_gateway::FleetStoreError::ScopeDenied(_)
        | sase_gateway::FleetStoreError::FleetContract(
            FleetContractDomainError::Validation(_),
        ) => PyValueError::new_err(message),
        sase_gateway::FleetStoreError::LockPoisoned
        | sase_gateway::FleetStoreError::Io { .. }
        | sase_gateway::FleetStoreError::Json { .. }
        | sase_gateway::FleetStoreError::FleetContract(_) => {
            PyRuntimeError::new_err(message)
        }
    }
}

pub(crate) fn fleet_wire_from_pydict<T: DeserializeOwned>(
    dict: &Bound<'_, PyDict>,
    label: &str,
) -> PyResult<T> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "{label} is not a valid fleet contract wire dict: {error}"
        ))
    })
}

pub(crate) fn fleet_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal fleet contract serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

#[pyfunction]
#[pyo3(name = "fleet_contract_schema_version")]
fn py_fleet_contract_schema_version() -> u32 {
    core_fleet_contract::fleet_contract_schema_version()
}

#[pyfunction]
#[pyo3(name = "fleet_installation_identity_load")]
fn py_fleet_installation_identity_load<'py>(
    py: Python<'py>,
    sase_home: &str,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_fleet_contract::load_installation_identity(&home)
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "fleet_installation_identity_ensure")]
fn py_fleet_installation_identity_ensure<'py>(
    py: Python<'py>,
    sase_home: &str,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_fleet_contract::ensure_installation_identity(&home)
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "fleet_installation_identity_rotate")]
fn py_fleet_installation_identity_rotate<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: InstallationIdentityRotateRequestWire =
        fleet_wire_from_pydict(
            request,
            "installation identity rotate request",
        )?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_fleet_contract::rotate_installation_identity(&home, &request)
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "fleet_installation_identity_migrate")]
fn py_fleet_installation_identity_migrate<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: InstallationIdentityMigrateRequestWire =
        fleet_wire_from_pydict(
            request,
            "installation identity migrate request",
        )?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_fleet_contract::migrate_installation_identity(&home, &request)
        })
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "fleet_logical_locator_key")]
fn py_fleet_logical_locator_key(
    logical_locator: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let locator: LogicalAgentLocatorWire =
        fleet_wire_from_pydict(logical_locator, "logical locator")?;
    core_fleet_contract::logical_locator_key(&locator)
        .map_err(fleet_contract_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "fleet_instance_locator_key")]
fn py_fleet_instance_locator_key(
    instance_locator: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let locator: AgentInstanceLocatorWire =
        fleet_wire_from_pydict(instance_locator, "instance locator")?;
    core_fleet_contract::instance_locator_key(&locator)
        .map_err(fleet_contract_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "fleet_associate_owner_display_name")]
fn py_fleet_associate_owner_display_name<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: OwnerDisplayNameRequestWire =
        fleet_wire_from_pydict(request, "owner display name request")?;
    let result = core_fleet_contract::associate_owner_display_name(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_project_resolved_agent_summary")]
fn py_fleet_project_resolved_agent_summary<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ResolvedAgentProjectionRequestWire =
        fleet_wire_from_pydict(request, "resolved agent projection request")?;
    let result = core_fleet_contract::project_resolved_agent_summary(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_project_resolved_agent_detail")]
fn py_fleet_project_resolved_agent_detail<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ResolvedAgentProjectionRequestWire =
        fleet_wire_from_pydict(request, "resolved agent projection request")?;
    let result = core_fleet_contract::project_resolved_agent_detail(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_resolved_agent_summary")]
fn py_fleet_validate_resolved_agent_summary<'py>(
    py: Python<'py>,
    summary: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let summary: ResolvedAgentSummaryWire =
        fleet_wire_from_pydict(summary, "resolved agent summary")?;
    let result = core_fleet_contract::validate_resolved_agent_summary(&summary)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_count_logical_agents")]
fn py_fleet_count_logical_agents<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetLogicalAgentCountsRequestWire =
        fleet_wire_from_pydict(request, "logical agent counts request")?;
    let result = core_fleet_contract::count_logical_agents(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_follow_record_key")]
fn py_fleet_follow_record_key(record: &Bound<'_, PyDict>) -> PyResult<String> {
    let record: FollowRecordWire =
        fleet_wire_from_pydict(record, "follow record")?;
    core_fleet_contract::follow_record_key(&record)
        .map_err(fleet_contract_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "fleet_reconcile_follow_records")]
fn py_fleet_reconcile_follow_records<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FollowReconciliationRequestWire =
        fleet_wire_from_pydict(request, "follow reconciliation request")?;
    let result = core_fleet_contract::reconcile_follow_records(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

fn fleet_followed_batch_agent_session_promotions_impl<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FollowedBatchAgentSessionPromotionRequestWire =
        fleet_wire_from_pydict(
            request,
            "followed-batch agent session promotion request",
        )?;
    let result = core_followed_batch_agent_session_promotions(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_followed_batch_agent_session_promotions")]
fn py_fleet_followed_batch_agent_session_promotions<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    fleet_followed_batch_agent_session_promotions_impl(py, request)
}

#[pyfunction]
#[pyo3(name = "fleet_count_focus_and_fleet")]
fn py_fleet_count_focus_and_fleet<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FocusFleetCountsRequestWire =
        fleet_wire_from_pydict(request, "focus/fleet counts request")?;
    let result = core_fleet_contract::count_focus_and_fleet(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_catalog_query")]
fn py_fleet_validate_catalog_query<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetCatalogQueryWire =
        fleet_wire_from_pydict(request, "fleet catalog query")?;
    let result = core_fleet_contract::validate_fleet_catalog_query(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_catalog_cursor")]
fn py_fleet_validate_catalog_cursor(cursor: &str) -> PyResult<String> {
    core_fleet_contract::validate_fleet_catalog_cursor(cursor)
        .map_err(fleet_contract_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "fleet_catalog_snapshot_id")]
fn py_fleet_catalog_snapshot_id<'py>(
    scope: &str,
    summaries: &Bound<'py, PyList>,
) -> PyResult<String> {
    let scope: FleetCatalogScopeWire = serde_json::from_value(
        JsonValue::String(scope.to_string()),
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "fleet catalog scope is not valid: {error}"
        ))
    })?;
    let summaries: Vec<ResolvedAgentSummaryWire> = serde_json::from_value(
        py_to_json_value(summaries.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "fleet catalog summaries are not valid: {error}"
        ))
    })?;
    core_fleet_contract::fleet_catalog_snapshot_id(scope, &summaries)
        .map_err(fleet_contract_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "fleet_accumulate_catalog_page")]
fn py_fleet_accumulate_catalog_page<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetCatalogAccumulationRequestWire =
        fleet_wire_from_pydict(request, "fleet catalog accumulation request")?;
    let result = core_fleet_contract::accumulate_fleet_catalog_page(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_snapshot_freshness")]
fn py_fleet_validate_snapshot_freshness<'py>(
    py: Python<'py>,
    freshness: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let freshness: FleetSnapshotFreshnessWire =
        fleet_wire_from_pydict(freshness, "fleet snapshot freshness")?;
    let result =
        core_fleet_contract::validate_fleet_snapshot_freshness(&freshness)
            .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_normalize_federation_response")]
fn py_fleet_normalize_federation_response<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetFederationNormalizeRequestWire =
        fleet_wire_from_pydict(request, "fleet federation normalize request")?;
    let result =
        core_fleet_contract::normalize_fleet_federation_response(&request)
            .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_count_focus_and_fleet_from_federation")]
fn py_fleet_count_focus_and_fleet_from_federation<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FocusFleetFederationCountsRequestWire =
        fleet_wire_from_pydict(
            request,
            "focus/fleet federation counts request",
        )?;
    let result =
        core_fleet_contract::count_focus_and_fleet_from_federation(&request)
            .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_classify_cursor_replay")]
fn py_fleet_classify_cursor_replay<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: CursorReplayRequestWire =
        fleet_wire_from_pydict(request, "cursor replay request")?;
    let result = core_fleet_contract::classify_cursor_replay(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_operation_payload_fingerprint")]
fn py_fleet_operation_payload_fingerprint<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: PayloadFingerprintRequestWire =
        fleet_wire_from_pydict(request, "payload fingerprint request")?;
    let result = core_fleet_contract::operation_payload_fingerprint(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_decide_operation_replay")]
fn py_fleet_decide_operation_replay<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: OperationDecisionRequestWire =
        fleet_wire_from_pydict(request, "operation decision request")?;
    let result = core_fleet_contract::decide_operation_replay(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_launch_intent")]
fn py_fleet_validate_launch_intent<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: FleetLaunchIntentWire =
        fleet_wire_from_pydict(intent, "fleet launch intent")?;
    let result = core_fleet_contract::validate_fleet_launch_intent(&intent)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_launch_payload_fingerprint")]
fn py_fleet_launch_payload_fingerprint<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: FleetLaunchIntentWire =
        fleet_wire_from_pydict(intent, "fleet launch intent")?;
    let result = core_fleet_contract::fleet_launch_payload_fingerprint(&intent)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_launch_request")]
fn py_fleet_validate_launch_request<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetLaunchRequestWire =
        fleet_wire_from_pydict(request, "fleet launch request")?;
    let result = core_fleet_contract::validate_fleet_launch_request(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_decide_launch_replay")]
fn py_fleet_decide_launch_replay<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetLaunchDecisionRequestWire =
        fleet_wire_from_pydict(request, "fleet launch decision request")?;
    let result = core_fleet_contract::decide_fleet_launch_replay(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "assemble_fleet_catalog")]
fn py_assemble_fleet_catalog<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AssembleFleetCatalogRequestWire =
        fleet_wire_from_pydict(request, "assemble fleet catalog request")?;
    let result = py
        .allow_threads(|| core_assemble_fleet_catalog(&request))
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_connection_plan")]
fn py_fleet_validate_connection_plan<'py>(
    py: Python<'py>,
    plan: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let plan: ConnectionPlanWire =
        fleet_wire_from_pydict(plan, "connection plan")?;
    let result = core_fleet_contract::validate_connection_plan(&plan)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_mutation_payload_fingerprint")]
fn py_fleet_mutation_payload_fingerprint<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let intent: FleetMutationIntentWire =
        fleet_wire_from_pydict(intent, "fleet mutation intent")?;
    let result =
        core_fleet_mutation::fleet_mutation_payload_fingerprint(&intent)
            .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_validate_mutation_request")]
fn py_fleet_validate_mutation_request<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: FleetMutationRequestWire =
        fleet_wire_from_pydict(request, "fleet mutation request")?;
    let result = core_fleet_mutation::validate_fleet_mutation_request(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(
    name = "fleet_evaluate_mutation_precondition",
    signature = (intent, observed=None)
)]
fn py_fleet_evaluate_mutation_precondition<'py>(
    py: Python<'py>,
    intent: &Bound<'py, PyDict>,
    observed: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let intent: FleetMutationIntentWire =
        fleet_wire_from_pydict(intent, "fleet mutation intent")?;
    let observed_summary = match observed {
        Some(value) => {
            Some(fleet_wire_from_pydict::<ResolvedAgentSummaryWire>(
                value,
                "observed resolved agent summary",
            )?)
        }
        None => None,
    };
    let result = core_fleet_mutation::evaluate_mutation_precondition(
        &intent,
        observed_summary.as_ref(),
    )
    .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_partition_bulk_targets")]
fn py_fleet_partition_bulk_targets<'py>(
    py: Python<'py>,
    targets: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let mut parsed = Vec::with_capacity(targets.len());
    for item in targets.iter() {
        let dict = item.downcast::<PyDict>().map_err(|_| {
            PyValueError::new_err("fleet bulk targets must be objects")
        })?;
        parsed.push(fleet_wire_from_pydict(dict, "fleet bulk target")?);
    }
    let result = core_fleet_mutation::partition_bulk_targets(&parsed)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

pub(crate) fn fleet_wire_list_from_pylist<T: DeserializeOwned>(
    items: &Bound<'_, PyList>,
    label: &str,
) -> PyResult<Vec<T>> {
    let mut parsed = Vec::with_capacity(items.len());
    for item in items.iter() {
        let dict = item.downcast::<PyDict>().map_err(|_| {
            PyValueError::new_err(format!("{label} entries must be objects"))
        })?;
        parsed.push(fleet_wire_from_pydict(dict, label)?);
    }
    Ok(parsed)
}

#[pyfunction]
#[pyo3(name = "fleet_issue_bootstrap")]
fn py_fleet_issue_bootstrap<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: sase_gateway::FleetBootstrapIssueRequestWire =
        fleet_wire_from_pydict(request, "fleet bootstrap issue request")?;
    let home = PathBuf::from(sase_home);
    let result = py
        .allow_threads(|| {
            let store = sase_gateway::FleetCredentialStore::new(home);
            store.issue_bootstrap(request, sase_gateway::current_unix_time())
        })
        .map_err(fleet_store_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "gateway_main")]
fn py_gateway_main(py: Python<'_>, args: Vec<String>) -> PyResult<()> {
    py.allow_threads(|| sase_gateway::run_gateway_cli(args))
        .map_err(PyRuntimeError::new_err)
}

#[pyfunction]
#[pyo3(name = "federation_worker_main")]
fn py_federation_worker_main(
    py: Python<'_>,
    args: Vec<String>,
) -> PyResult<()> {
    py.allow_threads(|| sase_gateway::run_federation_worker_cli(args))
        .map_err(PyRuntimeError::new_err)
}

#[pyfunction]
#[pyo3(name = "fleet_classify_runtime_duration")]
fn py_fleet_classify_runtime_duration<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: RuntimeDurationRequestWire =
        fleet_wire_from_pydict(request, "runtime duration request")?;
    let result = core_fleet_contract::classify_runtime_duration(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "fleet_classify_cache_freshness")]
fn py_fleet_classify_cache_freshness<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: CacheFreshnessRequestWire =
        fleet_wire_from_pydict(request, "cache freshness request")?;
    let result = core_fleet_contract::classify_cache_freshness(&request)
        .map_err(fleet_contract_error_to_pyerr)?;
    fleet_wire_to_py(py, &result)
}

pub(crate) fn register_fleet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_fleet_contract_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_installation_identity_load, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_installation_identity_ensure,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_installation_identity_rotate,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_installation_identity_migrate,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_logical_locator_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_instance_locator_key, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_associate_owner_display_name,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_project_resolved_agent_summary,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_project_resolved_agent_detail,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_validate_resolved_agent_summary,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_count_logical_agents, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_follow_record_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_reconcile_follow_records, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_followed_batch_agent_session_promotions,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_count_focus_and_fleet, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_catalog_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_catalog_cursor, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_catalog_snapshot_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_accumulate_catalog_page, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_snapshot_freshness, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_normalize_federation_response,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_count_focus_and_fleet_from_federation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_classify_cursor_replay, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_operation_payload_fingerprint,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_decide_operation_replay, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_launch_intent, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_launch_payload_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_launch_request, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_decide_launch_replay, m)?)?;
    m.add_function(wrap_pyfunction!(py_assemble_fleet_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_connection_plan, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_mutation_payload_fingerprint,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_validate_mutation_request, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_fleet_evaluate_mutation_precondition,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_fleet_partition_bulk_targets, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_issue_bootstrap, m)?)?;
    m.add_function(wrap_pyfunction!(py_gateway_main, m)?)?;
    m.add_function(wrap_pyfunction!(py_federation_worker_main, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_classify_runtime_duration, m)?)?;
    m.add_function(wrap_pyfunction!(py_fleet_classify_cache_freshness, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
