//! Host configuration bindings: services, flags, setup, and disk state.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use crate::provider_policy::provider_priority_dict_from_py;

use pyo3::wrap_pyfunction;

fn machine_setup_error_to_pyerr(error: MachineSetupError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "machine_setup_wire_schema_version")]
fn py_machine_setup_wire_schema_version() -> u32 {
    MACHINE_SETUP_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "classify_tailnet_health")]
fn py_classify_tailnet_health<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: TailnetHealthRequestWire = provider_priority_dict_from_py(
        request.as_any(),
        "tailnet health request",
    )?;
    let result = core_classify_tailnet_health(&request)
        .map_err(machine_setup_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "classify_tailnet_discovery")]
fn py_classify_tailnet_discovery<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: TailnetDiscoveryRequestWire = provider_priority_dict_from_py(
        request.as_any(),
        "tailnet discovery request",
    )?;
    let result = core_classify_tailnet_discovery(&request)
        .map_err(machine_setup_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "reconcile_machine_enrollments")]
fn py_reconcile_machine_enrollments<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: MachineReconcileRequestWire = provider_priority_dict_from_py(
        request.as_any(),
        "machine enrollment reconcile request",
    )?;
    let result = core_reconcile_machine_enrollments(&request)
        .map_err(machine_setup_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "assess_machine_init_review")]
fn py_assess_machine_init_review<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: MachineInitReviewAssessmentRequestWire =
        provider_priority_dict_from_py(
            request.as_any(),
            "machine init review assessment request",
        )?;
    let result = core_assess_machine_init_review(&request)
        .map_err(machine_setup_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "merge_machine_init_review")]
fn py_merge_machine_init_review<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: MachineInitReviewMergeRequestWire =
        provider_priority_dict_from_py(
            request.as_any(),
            "machine init review merge request",
        )?;
    let result = core_merge_machine_init_review(&request)
        .map_err(machine_setup_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

fn managed_tmp_reap_error_to_pyerr(error: ManagedTmpReapError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn disk_inventory_error_to_pyerr(error: DiskInventoryError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn disk_cleanup_outcome_error_to_pyerr(
    error: DiskCleanupOutcomeError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn disk_pressure_error_to_pyerr(error: DiskPressureError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "disk_inventory_wire_schema_version")]
fn py_disk_inventory_wire_schema_version() -> u32 {
    DISK_INVENTORY_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "classify_disk_inventory")]
fn py_classify_disk_inventory<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: DiskInventoryRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid DiskInventoryRequestWire dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_classify_disk_inventory(&request))
        .map_err(disk_inventory_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "disk_cleanup_outcome_wire_schema_version")]
fn py_disk_cleanup_outcome_wire_schema_version() -> u32 {
    DISK_CLEANUP_OUTCOME_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "normalize_disk_cleanup_outcome")]
fn py_normalize_disk_cleanup_outcome<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: DiskCleanupOutcomeRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid DiskCleanupOutcomeRequestWire dict: {error}"
        ))
    })?;
    let result = core_normalize_disk_cleanup_outcome(&request)
        .map_err(disk_cleanup_outcome_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "disk_pressure_wire_schema_version")]
fn py_disk_pressure_wire_schema_version() -> u32 {
    DISK_PRESSURE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "classify_disk_pressure")]
fn py_classify_disk_pressure<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: DiskPressureRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid DiskPressureRequestWire dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_classify_disk_pressure(&request))
        .map_err(disk_pressure_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "managed_tmp_reap_wire_schema_version")]
fn py_managed_tmp_reap_wire_schema_version() -> u32 {
    MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "reap_managed_tmpdir")]
fn py_reap_managed_tmpdir<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ManagedTmpReapRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ManagedTmpReapRequestWire dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_reap_managed_tmpdir(&request))
        .map_err(managed_tmp_reap_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

// --- Config Center backend bindings ---------------------------------------
//
// JSON-in / JSON-out wrappers over `sase_core::config`. Python supplies the
// already-discovered layer stack and JSON Schema; these return plain
// dicts/lists the Python adapter rehydrates into its dataclass mirrors. Domain
// errors (e.g. an unknown target layer) surface as `ValueError`.
fn config_error_to_pyerr(err: ConfigDomainError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

/// Flatten a JSON Schema dict into the ordered config field model dict.
#[pyfunction]
#[pyo3(name = "config_field_model")]
fn py_config_field_model<'py>(
    py: Python<'py>,
    schema: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let schema_value = py_to_json_value(schema.as_any())?;
    let model = core_config_field_model(&schema_value)
        .map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&model).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Build the config inventory (sources + per-field provenance + diagnostics).
///
/// *request* is a `ConfigInventoryRequestWire`-shape dict: `schema`, ordered
/// `layers`, and the optional `deprecations`/`unsupported` policy.
#[pyfunction]
#[pyo3(name = "config_inventory")]
fn py_config_inventory<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ConfigInventoryRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid config inventory request: {e}"
            ))
        })?;
    let inventory =
        core_config_inventory(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&inventory).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Plan a single set/unset edit, returning the write-plan dict.
///
/// *request* is a `ConfigEditRequestWire`-shape dict: `schema`, `layers`,
/// `target_layer`, `path`, and `op` (`{"kind": "set", "value": ...}` or
/// `{"kind": "unset"}`).
#[pyfunction]
#[pyo3(name = "config_plan_edit")]
fn py_config_plan_edit<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ConfigEditRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid config edit request: {e}"
            ))
        })?;
    let plan = core_config_plan_edit(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Compose the ordered AXE layer stack and return exact-key provenance and
/// entity inventory alongside the effective config.
#[pyfunction]
#[pyo3(name = "axe_config_compose")]
fn py_axe_config_compose<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: AxeConfigComposeRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AXE composition request: {e}"
            ))
        })?;
    let result =
        core_compose_axe_config(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Compose the ordered `service.procs` layer stack into effective entries,
/// per-entry availability, per-field provenance, and diagnostics.
#[pyfunction]
#[pyo3(name = "service_config_compose")]
fn py_service_config_compose<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ServiceConfigComposeRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid service config composition request: {e}"
            ))
        })?;
    let result = core_compose_service_config(&req);
    let json = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

fn service_state_error_to_pyerr(err: ServiceStateDomainError) -> PyErr {
    match err {
        ServiceStateDomainError::Validation(_) => {
            PyValueError::new_err(err.to_string())
        }
        ServiceStateDomainError::LockTimeout(_) => {
            PyTimeoutError::new_err(err.to_string())
        }
        ServiceStateDomainError::NewerSchema { .. }
        | ServiceStateDomainError::Io(_)
        | ServiceStateDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

/// Decide whether a service proc should restart and advance restart history.
#[pyfunction]
#[pyo3(name = "service_restart_decide")]
fn py_service_restart_decide<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ServiceRestartRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid service restart request: {e}"
            ))
        })?;
    let decision = core_decide_service_restart(&req)
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let json = serde_json::to_value(&decision).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Read the locked machine-local service state snapshot.
#[pyfunction]
#[pyo3(name = "service_state_read")]
#[pyo3(signature = (sase_home, boot_id=None))]
fn py_service_state_read<'py>(
    py: Python<'py>,
    sase_home: &str,
    boot_id: Option<String>,
) -> PyResult<PyObject> {
    let snapshot = core_read_service_state(sase_home, boot_id.as_deref())
        .map_err(service_state_error_to_pyerr)?;
    let json = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Mutate the locked machine-local service state and return the new snapshot.
#[pyfunction]
#[pyo3(name = "service_state_mutate")]
#[pyo3(signature = (sase_home, mutation, boot_id=None, now=None))]
fn py_service_state_mutate<'py>(
    py: Python<'py>,
    sase_home: &str,
    mutation: &Bound<'py, PyDict>,
    boot_id: Option<String>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(mutation.as_any())?;
    let mutation: ServiceStateMutationWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "mutation is not a valid service state mutation: {e}"
            ))
        })?;
    let outcome =
        core_mutate_service_state(sase_home, mutation, boot_id.as_deref(), now)
            .map_err(service_state_error_to_pyerr)?;
    let json = serde_json::to_value(&outcome).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

fn service_status_error_to_pyerr(err: ServiceStatusDomainError) -> PyErr {
    match err {
        ServiceStatusDomainError::Validation(_) => {
            PyValueError::new_err(err.to_string())
        }
        ServiceStatusDomainError::NewerSchema { .. }
        | ServiceStatusDomainError::Corrupt(_)
        | ServiceStatusDomainError::Io(_)
        | ServiceStatusDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

/// Resolve the effective enablement, provenance, and display summary.
#[pyfunction]
#[pyo3(name = "service_enablement_resolve")]
#[pyo3(signature = (entry, override_value=None))]
fn py_service_enablement_resolve<'py>(
    py: Python<'py>,
    entry: &Bound<'py, PyDict>,
    override_value: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let entry_value = py_to_json_value(entry.as_any())?;
    let entry: ServiceProcConfigWire = serde_json::from_value(entry_value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "entry is not a valid service proc config: {e}"
            ))
        })?;
    let override_value = override_value
        .map(|payload| {
            let value = py_to_json_value(payload.as_any())?;
            serde_json::from_value::<ServiceEnablementOverrideWire>(value)
                .map_err(|e| {
                    PyValueError::new_err(format!(
                        "override is not a valid service enablement override: {e}"
                    ))
                })
        })
        .transpose()?;
    let result = core_resolve_service_enablement_for_entry(
        &entry,
        override_value.as_ref(),
    );
    let json = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Build a schema-versioned service status snapshot.
#[pyfunction]
#[pyo3(name = "service_status_build")]
fn py_service_status_build<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ServiceStatusRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid service status request: {e}"
            ))
        })?;
    let snapshot = core_build_service_status(&request)
        .map_err(service_status_error_to_pyerr)?;
    let json = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Atomically write a service status snapshot JSON file.
#[pyfunction]
#[pyo3(name = "service_status_write")]
fn py_service_status_write<'py>(
    path: &str,
    snapshot: &Bound<'py, PyDict>,
) -> PyResult<()> {
    let value = py_to_json_value(snapshot.as_any())?;
    let snapshot: ServiceStatusSnapshotWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "snapshot is not a valid service status snapshot: {e}"
            ))
        })?;
    core_write_service_status_snapshot(path, &snapshot)
        .map_err(service_status_error_to_pyerr)?;
    Ok(())
}

/// Read a service status snapshot JSON file when one exists.
#[pyfunction]
#[pyo3(name = "service_status_read")]
fn py_service_status_read<'py>(
    py: Python<'py>,
    path: &str,
) -> PyResult<PyObject> {
    let snapshot = core_read_service_status_snapshot(path)
        .map_err(service_status_error_to_pyerr)?;
    let json = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Plan an exact-key sparse AXE lumberjack/chop contribution mutation.
#[pyfunction]
#[pyo3(name = "axe_config_plan_entry")]
fn py_axe_config_plan_entry<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: AxeEntryMutationRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AXE entry mutation request: {e}"
            ))
        })?;
    let result =
        core_plan_axe_entry_mutation(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Schema-validate a candidate merged config, returning diagnostic dicts.
///
/// *request* is a `ConfigValidateRequestWire`-shape dict: `schema` + `config`.
#[pyfunction]
#[pyo3(name = "config_validate")]
fn py_config_validate<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ConfigValidateRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid config validate request: {e}"
            ))
        })?;
    let diagnostics = core_config_validate(&req);
    let json = serde_json::to_value(&diagnostics).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

fn feature_flag_state_error_to_pyerr(
    err: FeatureFlagStateDomainError,
) -> PyErr {
    let message = err.to_string();
    match err {
        FeatureFlagStateDomainError::Invalid { .. } => {
            PyValueError::new_err(message)
        }
        FeatureFlagStateDomainError::LockTimeout { .. } => {
            PyTimeoutError::new_err(message)
        }
        FeatureFlagStateDomainError::Io { .. } => {
            PyRuntimeError::new_err(message)
        }
    }
}

fn feature_flag_state_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal feature-flag state serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

#[pyfunction]
#[pyo3(name = "feature_flag_state_wire_schema_version")]
fn py_feature_flag_state_wire_schema_version() -> u32 {
    sase_core::FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "feature_flag_state_get")]
fn py_feature_flag_state_get<'py>(
    py: Python<'py>,
    sase_home: &str,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let snapshot = py
        .allow_threads(|| core_feature_flag_state_get(&home))
        .map_err(feature_flag_state_error_to_pyerr)?;
    feature_flag_state_wire_to_py(py, &snapshot)
}

#[pyfunction]
#[pyo3(name = "feature_flag_state_set")]
fn py_feature_flag_state_set<'py>(
    py: Python<'py>,
    sase_home: &str,
    flag: &str,
    enabled: bool,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| core_feature_flag_state_set(&home, flag, enabled))
        .map_err(feature_flag_state_error_to_pyerr)?;
    feature_flag_state_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "feature_flag_state_reconcile")]
fn py_feature_flag_state_reconcile<'py>(
    py: Python<'py>,
    sase_home: &str,
    registered_keys: Vec<String>,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_feature_flag_state_reconcile(&home, &registered_keys)
        })
        .map_err(feature_flag_state_error_to_pyerr)?;
    feature_flag_state_wire_to_py(py, &outcome)
}

pub(crate) fn register_config(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_machine_setup_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_tailnet_health, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_tailnet_discovery, m)?)?;
    m.add_function(wrap_pyfunction!(py_reconcile_machine_enrollments, m)?)?;
    m.add_function(wrap_pyfunction!(py_assess_machine_init_review, m)?)?;
    m.add_function(wrap_pyfunction!(py_merge_machine_init_review, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_disk_inventory_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_classify_disk_inventory, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_disk_cleanup_outcome_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_normalize_disk_cleanup_outcome, m)?)?;
    m.add_function(wrap_pyfunction!(py_disk_pressure_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_disk_pressure, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_managed_tmp_reap_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_reap_managed_tmpdir, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_field_model, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_inventory, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_plan_edit, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_axe_config_compose, m)?)?;
    m.add_function(wrap_pyfunction!(py_axe_config_plan_entry, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_config_compose, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_restart_decide, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_state_read, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_state_mutate, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_enablement_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_status_build, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_status_write, m)?)?;
    m.add_function(wrap_pyfunction!(py_service_status_read, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_feature_flag_state_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_feature_flag_state_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_feature_flag_state_set, m)?)?;
    m.add_function(wrap_pyfunction!(py_feature_flag_state_reconcile, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
