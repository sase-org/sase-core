//! Artifact link store bindings: link operations and cutover.

use crate::prelude::*;

use crate::artifact_refs::artifact_ref_error_to_pyerr;

use crate::beads::bead_result_to_py;

use crate::json_bridge::{
    json_value_to_py, py_to_json_value, strings_from_py_list,
};

use pyo3::wrap_pyfunction;

fn artifact_link_error_to_pyerr(error: ArtifactLinkError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn managed_table_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ManagedTableTableWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "table is not a valid ManagedTableTableWire dict: {error}"
        ))
    })
}

fn artifact_row_identity_from_py(
    value: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<ArtifactRowIdentityWire> {
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!(
            "{label} is not a valid ArtifactRowIdentityWire dict: {error}"
        ))
    })
}

fn artifact_row_identities_from_py_list(
    list: &Bound<'_, PyList>,
    label: &str,
) -> PyResult<Vec<ArtifactRowIdentityWire>> {
    let mut identities = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        identities.push(artifact_row_identity_from_py(
            &item,
            &format!("{label}[{idx}]"),
        )?);
    }
    Ok(identities)
}

fn artifact_row_ref_query_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactRowRefQueryWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "query is not a valid ArtifactRowRefQueryWire dict: {error}"
        ))
    })
}

fn artifact_link_publication_observation_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkPublicationObservationWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "observation is not a valid ArtifactLinkPublicationObservationWire dict: {error}"
        ))
    })
}

fn artifact_link_publication_record_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkPublicationRecordWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid ArtifactLinkPublicationRecordWire dict: {error}"
        ))
    })
}

fn artifact_link_publication_attempt_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkPublicationAttemptWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "attempt is not a valid ArtifactLinkPublicationAttemptWire dict: {error}"
        ))
    })
}

fn artifact_link_owner_requirements_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkOwnerRequirementWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "requirements is not a valid ArtifactLinkOwnerRequirementWire dict: {error}"
        ))
    })
}

fn artifact_link_publication_evidence_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkPublicationEvidenceWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "evidence is not a valid ArtifactLinkPublicationEvidenceWire dict: {error}"
        ))
    })
}

fn artifact_link_event_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkEventWire> {
    let value = py_to_json_value(dict.as_any())?;
    core_canonicalize_artifact_link_event_json_value(&value)
        .map_err(artifact_link_error_to_pyerr)
}

fn artifact_link_events_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ArtifactLinkEventWire>> {
    let mut events = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let dict = item.downcast::<PyDict>().map_err(|_| {
            PyValueError::new_err(format!("events[{index}] must be a dict"))
        })?;
        events.push(artifact_link_event_from_pydict(dict)?);
    }
    Ok(events)
}

fn artifact_link_alias_from_py(
    value: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<ArtifactLinkAliasWire> {
    let alias: ArtifactLinkAliasWire =
        serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
            PyValueError::new_err(format!(
                "{label} is not a valid ArtifactLinkAliasWire dict: {error}"
            ))
        })?;
    core_canonicalize_artifact_link_alias(&alias)
        .map_err(artifact_link_error_to_pyerr)
}

fn artifact_link_aliases_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ArtifactLinkAliasWire>> {
    let mut aliases = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        aliases.push(artifact_link_alias_from_py(
            &item,
            &format!("aliases[{index}]"),
        )?);
    }
    Ok(aliases)
}

fn artifact_link_cutover_marker_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverMarkerWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "marker is not a valid ArtifactLinkCutoverMarkerWire dict: {error}"
        ))
    })
}

fn artifact_link_cutover_import_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverImportRequestWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactLinkCutoverImportRequestWire dict: {error}"
        ))
    })
}

fn artifact_link_cutover_baseline_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverBaselineEventRequestWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactLinkCutoverBaselineEventRequestWire dict: {error}"
        ))
    })
}

fn artifact_link_cutover_event_store_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverEventStoreWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "event_store is not a valid ArtifactLinkCutoverEventStoreWire dict: {error}"
        ))
    })
}

fn artifact_link_cutover_import_identity_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverImportIdentityWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "import is not a valid ArtifactLinkCutoverImportIdentityWire dict: {error}"
        ))
    })
}

fn artifact_link_cutover_roles_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ArtifactLinkCutoverRoleWire>> {
    let mut roles = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let dict = item.downcast::<PyDict>().map_err(|_| {
            PyValueError::new_err(format!("roles[{index}] must be a dict"))
        })?;
        roles.push(serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(
            |error| {
                PyValueError::new_err(format!(
                    "roles[{index}] is not a valid ArtifactLinkCutoverRoleWire dict: {error}"
                ))
            },
        )?);
    }
    Ok(roles)
}

fn artifact_link_cutover_baseline_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverBaselineEventWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "baseline_event is not a valid ArtifactLinkCutoverBaselineEventWire dict: {error}"
        ))
    })
}

fn artifact_link_cutover_state_from_str(
    value: &str,
) -> PyResult<ArtifactLinkCutoverStateWire> {
    match value.trim() {
        "fenced" => Ok(ArtifactLinkCutoverStateWire::Fenced),
        "imported" => Ok(ArtifactLinkCutoverStateWire::Imported),
        _ => Err(PyValueError::new_err(
            "artifact-link cutover state must be `fenced` or `imported`",
        )),
    }
}

fn artifact_link_cutover_read_roots_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ArtifactLinkCutoverReadRootWire>> {
    let mut roots = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        roots.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |error| {
                PyValueError::new_err(format!(
                    "roots[{index}] is not a valid ArtifactLinkCutoverReadRootWire dict: {error}"
                ))
            },
        )?);
    }
    Ok(roots)
}

fn artifact_link_cutover_progress_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkCutoverProgressRequestWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactLinkCutoverProgressRequestWire dict: {error}"
        ))
    })
}

/// Return the v2 artifact-link row schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_row_schema_version")]
pub(crate) fn py_artifact_link_row_schema_version() -> u64 {
    ARTIFACT_LINK_ROW_SCHEMA_VERSION
}

/// Return the immutable artifact-link event wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_event_schema_version")]
pub(crate) fn py_artifact_link_event_schema_version() -> u64 {
    ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION
}

/// Return the artifact-link cutover marker wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_wire_schema_version")]
pub(crate) fn py_artifact_link_cutover_wire_schema_version() -> u64 {
    ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION
}

/// Strictly parse and validate one artifact-link cutover marker JSON string.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_marker_parse")]
pub(crate) fn py_artifact_link_cutover_marker_parse(
    py: Python<'_>,
    payload: &str,
) -> PyResult<PyObject> {
    let marker = core_parse_artifact_link_cutover_marker(payload)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(marker).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return canonical marker JSON for one validated artifact-link cutover marker.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_marker_canonical_json")]
pub(crate) fn py_artifact_link_cutover_marker_canonical_json(
    marker: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let marker = artifact_link_cutover_marker_from_pydict(marker)?;
    core_artifact_link_cutover_marker_canonical_json(&marker)
        .map_err(artifact_link_error_to_pyerr)
}

/// Build a validated artifact-link cutover marker.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_marker_build")]
pub(crate) fn py_artifact_link_cutover_marker_build(
    py: Python<'_>,
    state: &str,
    project_key: &str,
    event_store: &Bound<'_, PyDict>,
    import_identity: &Bound<'_, PyDict>,
    roles: &Bound<'_, PyList>,
    baseline_event: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let state = artifact_link_cutover_state_from_str(state)?;
    let event_store =
        artifact_link_cutover_event_store_from_pydict(event_store)?;
    let import_identity =
        artifact_link_cutover_import_identity_from_pydict(import_identity)?;
    let roles = artifact_link_cutover_roles_from_py_list(roles)?;
    let baseline_event =
        artifact_link_cutover_baseline_from_pydict(baseline_event)?;
    let marker = core_artifact_link_cutover_marker(
        state,
        project_key,
        &event_store,
        &import_identity,
        &roles,
        &baseline_event,
    )
    .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(marker).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the deterministic identity for one legacy-index import request.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_import_identity")]
pub(crate) fn py_artifact_link_cutover_import_identity(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request = artifact_link_cutover_import_request_from_pydict(request)?;
    let identity = core_artifact_link_cutover_import_identity(&request)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(identity).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Build the deterministic baseline-import event for one cutover identity.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_baseline_event")]
pub(crate) fn py_artifact_link_cutover_baseline_event(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request = artifact_link_cutover_baseline_request_from_pydict(request)?;
    let event = core_artifact_link_cutover_baseline_event(&request)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(event).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the operator attestation token for one cutover marker.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_attestation")]
pub(crate) fn py_artifact_link_cutover_attestation(
    marker: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let marker = artifact_link_cutover_marker_from_pydict(marker)?;
    core_artifact_link_cutover_attestation(&marker)
        .map_err(artifact_link_error_to_pyerr)
}

/// Return reader-facing cutover state for root marker observations.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_read_state")]
pub(crate) fn py_artifact_link_cutover_read_state(
    py: Python<'_>,
    roots: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let roots = artifact_link_cutover_read_roots_from_py_list(roots)?;
    let state = core_artifact_link_cutover_read_state(&roots)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(state).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return resumable cutover progress from expected marker and root observations.
#[pyfunction]
#[pyo3(name = "artifact_link_cutover_progress")]
pub(crate) fn py_artifact_link_cutover_progress(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request = artifact_link_cutover_progress_request_from_pydict(request)?;
    let progress = core_artifact_link_cutover_progress(&request)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(progress).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Classify one physical artifact-link outbox JSONL line.
#[pyfunction]
#[pyo3(name = "artifact_link_outbox_classify_line")]
fn py_artifact_link_outbox_classify_line(
    py: Python<'_>,
    line: &str,
    project_key: &str,
) -> PyResult<PyObject> {
    let classification =
        core_artifact_link_outbox_classify_line(line, project_key);
    let value = serde_json::to_value(classification).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Convert or retire one legacy row-only outbox entry.
#[pyfunction]
#[pyo3(name = "artifact_link_outbox_legacy_conversion")]
fn py_artifact_link_outbox_legacy_conversion(
    py: Python<'_>,
    entry: &Bound<'_, PyDict>,
    project_key: &str,
    baseline_rows: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let entry_value = py_to_json_value(entry.as_any())?;
    let mut rows: Vec<ArtifactLinkRowWire> =
        Vec::with_capacity(baseline_rows.len());
    for (index, item) in baseline_rows.iter().enumerate() {
        rows.push(
            serde_json::from_value::<ArtifactLinkRowWire>(py_to_json_value(&item)?)
                .map_err(|error| {
                    PyValueError::new_err(format!(
                        "baseline_rows[{index}] is not a valid ArtifactLinkRowWire dict: {error}"
                    ))
                })?,
        );
    }
    let conversion = core_artifact_link_outbox_legacy_conversion(
        &entry_value,
        project_key,
        &rows,
    )
    .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(conversion).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the stable producer recorded on derived-fact link events.
#[pyfunction]
#[pyo3(name = "artifact_link_derived_producer_id")]
pub(crate) fn py_artifact_link_derived_producer_id() -> &'static str {
    core_artifact_link_derived_producer_id()
}

/// Return the stable producer recorded on artifact-rename alias events.
#[pyfunction]
#[pyo3(name = "artifact_link_alias_producer_id")]
pub(crate) fn py_artifact_link_alias_producer_id() -> &'static str {
    core_artifact_link_alias_producer_id()
}

/// Return the machine run marker used for stable background facts.
#[pyfunction]
#[pyo3(name = "artifact_link_machine_run_id")]
pub(crate) fn py_artifact_link_machine_run_id() -> &'static str {
    core_artifact_link_machine_run_id()
}

/// Return the timestamp sentinel for replay-stable derived facts.
#[pyfunction]
#[pyo3(name = "artifact_link_stable_fact_created_at")]
pub(crate) fn py_artifact_link_stable_fact_created_at() -> &'static str {
    core_artifact_link_stable_fact_created_at()
}

/// Return a deterministic 128-bit operation id for replayable producers.
#[pyfunction]
#[pyo3(name = "artifact_link_stable_operation_id")]
pub(crate) fn py_artifact_link_stable_operation_id(
    parts: &Bound<'_, PyAny>,
) -> PyResult<String> {
    let value = py_to_json_value(parts)?;
    let JsonValue::Array(parts) = value else {
        return Err(PyValueError::new_err(
            "artifact_link_stable_operation_id expects a JSON-shaped list",
        ));
    };
    core_artifact_link_stable_operation_id(&parts)
        .map_err(artifact_link_error_to_pyerr)
}

/// Return the artifact-row ref-resolution wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_row_resolution_wire_schema_version")]
pub(crate) fn py_artifact_row_resolution_wire_schema_version() -> u64 {
    ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION
}

/// Return the artifact-link publication retry state schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_publication_state_wire_schema_version")]
pub(crate) fn py_artifact_link_publication_state_wire_schema_version() -> u64 {
    u64::from(ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION)
}

/// Return the artifact-link publication ownership wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_publication_ownership_wire_schema_version")]
pub(crate) fn py_artifact_link_publication_ownership_wire_schema_version() -> u64
{
    ARTIFACT_LINK_PUBLICATION_OWNERSHIP_WIRE_SCHEMA_VERSION
}

/// Partition one link event's refs into publication owner requirements.
#[pyfunction]
#[pyo3(name = "artifact_link_event_owner_requirements")]
pub(crate) fn py_artifact_link_event_owner_requirements<'py>(
    py: Python<'py>,
    event: &Bound<'py, PyDict>,
    document_kinds: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let event = artifact_link_event_from_pydict(event)?;
    let document_kinds =
        strings_from_py_list(document_kinds, "document_kinds")?;
    let requirements =
        core_artifact_link_event_owner_requirements(&event, &document_kinds)
            .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(requirements).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Decide whether one link event has a durable publication receipt.
#[pyfunction]
#[pyo3(name = "artifact_link_publication_receipt")]
pub(crate) fn py_artifact_link_publication_receipt<'py>(
    py: Python<'py>,
    requirements: &Bound<'py, PyDict>,
    evidence: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let requirements =
        artifact_link_owner_requirements_from_pydict(requirements)?;
    let evidence = artifact_link_publication_evidence_from_pydict(evidence)?;
    let receipt =
        core_artifact_link_publication_receipt(&requirements, &evidence)
            .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(receipt).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the stable state key for one artifact-link publication root.
#[pyfunction]
#[pyo3(name = "artifact_link_publication_record_key")]
pub(crate) fn py_artifact_link_publication_record_key(
    project_key: &str,
    role: &str,
    repo_root: &str,
    remote_url: &str,
    upstream: &str,
) -> PyResult<String> {
    core_artifact_link_publication_record_key(
        project_key,
        role,
        repo_root,
        remote_url,
        upstream,
    )
    .map_err(artifact_link_error_to_pyerr)
}

/// Register or refresh one pending artifact-link publication observation.
#[pyfunction]
#[pyo3(
    name = "artifact_link_publication_register_pending",
    signature = (observation, now, current=None)
)]
pub(crate) fn py_artifact_link_publication_register_pending<'py>(
    py: Python<'py>,
    observation: &Bound<'py, PyDict>,
    now: f64,
    current: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let current_record = current
        .map(artifact_link_publication_record_from_pydict)
        .transpose()?;
    let observation =
        artifact_link_publication_observation_from_pydict(observation)?;
    let record = core_artifact_link_publication_register_pending(
        current_record.as_ref(),
        observation,
        now,
    )
    .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(record).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Classify whether a pending artifact-link publication is due and aging.
#[pyfunction]
#[pyo3(name = "artifact_link_publication_due")]
pub(crate) fn py_artifact_link_publication_due<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let record = artifact_link_publication_record_from_pydict(record)?;
    let due = core_artifact_link_publication_due(record, now)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(due).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Update retry state after a bounded publication worker attempt.
#[pyfunction]
#[pyo3(name = "artifact_link_publication_mark_attempt")]
pub(crate) fn py_artifact_link_publication_mark_attempt<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyDict>,
    attempt: &Bound<'py, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let record = artifact_link_publication_record_from_pydict(record)?;
    let attempt = artifact_link_publication_attempt_from_pydict(attempt)?;
    let record =
        core_artifact_link_publication_mark_attempt(record, attempt, now)
            .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(record).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate and canonicalize one immutable artifact-link event dict.
#[pyfunction]
#[pyo3(name = "artifact_link_event_canonicalize")]
pub(crate) fn py_artifact_link_event_canonicalize(
    py: Python<'_>,
    event: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let event = artifact_link_event_from_pydict(event)?;
    let value = serde_json::to_value(event).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return sorted compact JSON plus one trailing newline for one link event.
#[pyfunction]
#[pyo3(name = "artifact_link_event_canonical_json")]
pub(crate) fn py_artifact_link_event_canonical_json(
    event: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let event = artifact_link_event_from_pydict(event)?;
    core_artifact_link_event_canonical_json(&event)
        .map_err(artifact_link_error_to_pyerr)
}

/// Return the lowercase SHA-256 over canonical event bytes.
#[pyfunction]
#[pyo3(name = "artifact_link_event_digest")]
pub(crate) fn py_artifact_link_event_digest(
    event: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let event = artifact_link_event_from_pydict(event)?;
    core_artifact_link_event_digest(&event)
        .map_err(artifact_link_error_to_pyerr)
}

/// Return the immutable event path for a canonical event digest.
#[pyfunction]
#[pyo3(name = "artifact_link_event_path_for_digest")]
pub(crate) fn py_artifact_link_event_path_for_digest(
    digest: &str,
) -> PyResult<String> {
    core_artifact_link_event_path_for_digest(digest)
        .map_err(artifact_link_error_to_pyerr)
}

/// Validate an immutable event path against a canonical digest.
#[pyfunction]
#[pyo3(name = "artifact_link_event_validate_path")]
pub(crate) fn py_artifact_link_event_validate_path(
    path: &str,
    digest: &str,
) -> PyResult<String> {
    core_artifact_link_event_validate_path(path, digest)
        .map_err(artifact_link_error_to_pyerr)
}

/// Validate exact canonical event bytes and their optional immutable path.
#[pyfunction]
#[pyo3(name = "artifact_link_event_validate_bytes", signature = (payload, path=None))]
pub(crate) fn py_artifact_link_event_validate_bytes(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
    path: Option<&str>,
) -> PyResult<PyObject> {
    let bytes: Vec<u8> = if let Ok(bytes) = payload.extract::<Vec<u8>>() {
        bytes
    } else if let Ok(text) = payload.extract::<String>() {
        text.into_bytes()
    } else {
        return Err(PyValueError::new_err(
            "payload must be bytes, bytearray, or str",
        ));
    };
    let validation = core_artifact_link_event_validate_bytes(&bytes, path)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(validation).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Resolve artifact refs through an alias graph used by link-event reduction.
#[pyfunction]
#[pyo3(name = "artifact_link_event_resolve_aliases")]
pub(crate) fn py_artifact_link_event_resolve_aliases(
    py: Python<'_>,
    aliases: &Bound<'_, PyList>,
    refs: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let aliases = artifact_link_aliases_from_py_list(aliases)?;
    let refs = strings_from_py_list(refs, "refs")?;
    let resolved = core_resolve_artifact_link_event_aliases(&aliases, &refs)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(resolved).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Reduce link events to legacy rows plus deterministic version metadata.
#[pyfunction]
#[pyo3(name = "artifact_link_events_reduce", signature = (events, aliases=None))]
pub(crate) fn py_artifact_link_events_reduce(
    py: Python<'_>,
    events: &Bound<'_, PyList>,
    aliases: Option<&Bound<'_, PyList>>,
) -> PyResult<PyObject> {
    let events = artifact_link_events_from_py_list(events)?;
    let aliases = aliases
        .map(artifact_link_aliases_from_py_list)
        .transpose()?
        .unwrap_or_default();
    let reduction = core_reduce_link_events(&events, &aliases)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(reduction).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Split an artifact-link ref string into canonical kind and payload.
#[pyfunction]
#[pyo3(name = "artifact_link_ref_parts")]
pub(crate) fn py_artifact_link_ref_parts<'py>(
    py: Python<'py>,
    value: &str,
) -> PyResult<Option<PyObject>> {
    let Some(parsed) = core_parse_artifact_link_ref_parts(value) else {
        return Ok(None);
    };
    let value = serde_json::to_value(parsed).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value).map(Some)
}

/// Return batched row-index lookup keys for frontend row identities.
#[pyfunction]
#[pyo3(name = "artifact_row_index_keys")]
pub(crate) fn py_artifact_row_index_keys<'py>(
    py: Python<'py>,
    identities: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let identities =
        artifact_row_identities_from_py_list(identities, "identities")?;
    let batches: Vec<Vec<Vec<String>>> = identities
        .iter()
        .map(core_artifact_row_index_keys)
        .collect();
    let value = serde_json::to_value(batches).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return ordered lookup keys for one artifact-link ref query.
#[pyfunction]
#[pyo3(name = "artifact_row_ref_lookup_keys")]
pub(crate) fn py_artifact_row_ref_lookup_keys<'py>(
    py: Python<'py>,
    query: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let query = artifact_row_ref_query_from_pydict(query)?;
    let keys = core_artifact_row_ref_lookup_keys(&query);
    let value = serde_json::to_value(keys).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Resolve one artifact-link ref query against candidate row identities.
#[pyfunction]
#[pyo3(name = "artifact_row_resolve")]
pub(crate) fn py_artifact_row_resolve<'py>(
    py: Python<'py>,
    query: &Bound<'py, PyDict>,
    candidates: &Bound<'py, PyList>,
) -> PyResult<Option<PyObject>> {
    let query = artifact_row_ref_query_from_pydict(query)?;
    let candidates =
        artifact_row_identities_from_py_list(candidates, "candidates")?;
    let Some(resolved) =
        core_resolve_artifact_row_identity(&query, &candidates)
    else {
        return Ok(None);
    };
    let value = serde_json::to_value(resolved).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value).map(Some)
}

/// Canonicalize one artifact-link ref, stripping `@` and rewriting aliases.
#[pyfunction]
#[pyo3(name = "artifact_link_canonicalize")]
pub(crate) fn py_artifact_link_canonicalize(value: &str) -> PyResult<String> {
    core_canonicalize_artifact_link_ref(value)
        .map_err(artifact_link_error_to_pyerr)
}

/// Validate and canonicalize one link row.
#[pyfunction]
#[pyo3(name = "artifact_link_validate_row")]
fn py_artifact_link_validate_row(
    py: Python<'_>,
    row: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let row: ArtifactLinkRowWire = serde_json::from_value(py_to_json_value(
        row.as_any(),
    )?)
    .map_err(|error| {
        PyValueError::new_err(format!(
            "row is not a valid ArtifactLinkRowWire dict: {error}"
        ))
    })?;
    let validated = core_validate_artifact_link_row(&row)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(validated).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Insert or rewrite one row in a link-row collection.
#[pyfunction]
#[pyo3(name = "artifact_link_upsert_row")]
fn py_artifact_link_upsert_row(
    py: Python<'_>,
    rows: &Bound<'_, PyList>,
    row: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let mut parsed: Vec<ArtifactLinkRowWire> = Vec::with_capacity(rows.len());
    for (index, item) in rows.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        parsed.push(serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "rows[{index}] is not a valid ArtifactLinkRowWire dict: {error}"
            ))
        })?);
    }
    let incoming: ArtifactLinkRowWire = serde_json::from_value(
        py_to_json_value(row.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "row is not a valid ArtifactLinkRowWire dict: {error}"
        ))
    })?;
    let outcome = core_upsert_artifact_link_row(&mut parsed, incoming)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::json!({
        "kind": outcome.kind,
        "row": outcome.row,
        "rows": parsed,
    });
    json_value_to_py(py, &value)
}

fn artifact_link_index_from_pydict(
    label: &str,
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkIndexWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "{label} is not a valid ArtifactLinkIndexWire dict: {error}"
        ))
    })
}

/// Merge three per-artifact link indexes with conflict-aware semantics.
#[pyfunction]
#[pyo3(name = "artifact_link_merge_indexes")]
pub(crate) fn py_artifact_link_merge_indexes(
    py: Python<'_>,
    base: &Bound<'_, PyDict>,
    ours: &Bound<'_, PyDict>,
    theirs: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let base = artifact_link_index_from_pydict("base", base)?;
    let ours = artifact_link_index_from_pydict("ours", ours)?;
    let theirs = artifact_link_index_from_pydict("theirs", theirs)?;
    let merged = core_merge_artifact_link_indexes(&base, &ours, &theirs)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(merged).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the compiled-in v1 relation registry.
#[pyfunction]
#[pyo3(name = "artifact_relations_builtins")]
pub(crate) fn py_artifact_relations_builtins(
    py: Python<'_>,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_builtin_artifact_relations())
        .map_err(|error| {
            PyValueError::new_err(format!("internal serialize error: {error}"))
        })?;
    json_value_to_py(py, &value)
}

/// Look up one writable relation slug.
#[pyfunction]
#[pyo3(name = "artifact_relation_lookup")]
fn py_artifact_relation_lookup(
    py: Python<'_>,
    slug: &str,
) -> PyResult<PyObject> {
    let relation = core_lookup_artifact_relation(slug)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(relation).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Relation label from this document's perspective.
#[pyfunction]
#[pyo3(name = "artifact_relation_label")]
pub(crate) fn py_artifact_relation_label(
    slug: &str,
    this_is_source: bool,
) -> PyResult<String> {
    core_relation_label_from_perspective(slug, this_is_source)
        .map_err(artifact_link_error_to_pyerr)
}

/// Parse the managed `## Links` block out of a document.
#[pyfunction]
#[pyo3(name = "links_block_parse")]
fn py_links_block_parse(py: Python<'_>, document: &str) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_parse_links_block(document))
        .map_err(|error| {
            PyValueError::new_err(format!("internal serialize error: {error}"))
        })?;
    json_value_to_py(py, &value)
}

/// Render one `## Links` block.
#[pyfunction]
#[pyo3(name = "links_block_render", signature = (table, host_document=None))]
fn py_links_block_render(
    table: &Bound<'_, PyDict>,
    host_document: Option<&str>,
) -> PyResult<String> {
    let table = managed_table_from_pydict(table)?;
    core_render_links_block(&table, host_document)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Insert, replace, or remove the top-anchored `## Links` block.
#[pyfunction]
#[pyo3(name = "links_block_upsert")]
pub(crate) fn py_links_block_upsert(
    document: &str,
    table: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let table = managed_table_from_pydict(table)?;
    core_upsert_links_block(document, &table)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Remove the managed `## Links` block, if present.
#[pyfunction]
#[pyo3(name = "links_block_remove")]
fn py_links_block_remove(document: &str) -> String {
    core_remove_links_block(document)
}

/// Strip the managed `## Links` block for content hashing.
#[pyfunction]
#[pyo3(name = "links_block_strip")]
pub(crate) fn py_links_block_strip(document: &str) -> String {
    core_strip_links_block(document)
}

/// Resolve the artifact markdown file for one ref.
#[pyfunction]
#[pyo3(name = "artifact_md_path")]
fn py_artifact_md_path(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: ArtifactMdPathRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactMdPathRequestWire dict: {error}"
        ))
    })?;
    let result = core_artifact_md_path(&request)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Companion markdown path for a published binary, with collision refusal.
#[pyfunction]
#[pyo3(name = "companion_md_path")]
fn py_companion_md_path(
    py: Python<'_>,
    asset_path: &str,
) -> PyResult<PyObject> {
    let result = core_companion_md_path(asset_path)
        .map_err(artifact_link_error_to_pyerr)?;
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Classify a document's `links:` frontmatter inlet.
#[pyfunction]
#[pyo3(name = "artifact_link_frontmatter_inlet")]
pub(crate) fn py_artifact_link_frontmatter_inlet(
    py: Python<'_>,
    document: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(
        core_parse_artifact_link_frontmatter_inlet(document),
    )
    .map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "bead_add_link")]
#[pyo3(signature = (beads_dir, issue_id, target_ref, relation, description, origin="manual", direction="out", uses=1, now=None, operation_id=None))]
#[allow(clippy::too_many_arguments)]
fn py_bead_add_link<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    description: &str,
    origin: &str,
    direction: &str,
    uses: u64,
    now: Option<String>,
    operation_id: Option<String>,
) -> PyResult<PyObject> {
    let origin =
        ArtifactLinkOriginWire::from_name(origin).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown artifact link origin `{origin}`"
            ))
        })?;
    let direction =
        BeadLinkDirectionWire::from_name(direction).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown bead link direction `{direction}`"
            ))
        })?;
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_add_link(
                &beads_dir,
                issue_id,
                target_ref,
                relation,
                description,
                origin,
                direction,
                uses,
                now,
                operation_id,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_set_link_projection")]
#[pyo3(signature = (beads_dir, issue_id, target_ref, relation, direction, present, operation_id, description=None, origin=None, uses=1, now=None))]
#[allow(clippy::too_many_arguments)]
fn py_bead_set_link_projection<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    direction: &str,
    present: bool,
    operation_id: String,
    description: Option<String>,
    origin: Option<&str>,
    uses: u64,
    now: Option<String>,
) -> PyResult<PyObject> {
    let origin = match origin {
        Some(origin) => Some(
            ArtifactLinkOriginWire::from_name(origin).ok_or_else(|| {
                PyValueError::new_err(format!(
                    "unknown artifact link origin `{origin}`"
                ))
            })?,
        ),
        None => None,
    };
    let direction =
        BeadLinkDirectionWire::from_name(direction).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown bead link direction `{direction}`"
            ))
        })?;
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_set_link_projection(
                &beads_dir,
                issue_id,
                target_ref,
                relation,
                direction,
                present,
                description,
                origin,
                uses,
                now,
                operation_id,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_remove_link")]
#[pyo3(signature = (beads_dir, issue_id, target_ref, relation=None, direction="out", now=None, operation_id=None))]
#[allow(clippy::too_many_arguments)]
fn py_bead_remove_link<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    target_ref: &str,
    relation: Option<&str>,
    direction: &str,
    now: Option<String>,
    operation_id: Option<String>,
) -> PyResult<PyObject> {
    let direction =
        BeadLinkDirectionWire::from_name(direction).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown bead link direction `{direction}`"
            ))
        })?;
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_remove_link(
                &beads_dir,
                issue_id,
                target_ref,
                relation,
                direction,
                now,
                operation_id,
            )
        }),
    )
}

pub(crate) fn register_artifact_links(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_artifact_link_row_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_event_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_cutover_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_cutover_marker_parse,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_cutover_marker_canonical_json,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_cutover_marker_build,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_cutover_import_identity,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_cutover_baseline_event,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_cutover_attestation, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_cutover_read_state, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_cutover_progress, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_outbox_classify_line,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_outbox_legacy_conversion,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_derived_producer_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_alias_producer_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_machine_run_id, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_stable_fact_created_at,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_stable_operation_id, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_row_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_publication_state_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_publication_ownership_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_event_owner_requirements,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_publication_receipt, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_publication_record_key,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_publication_register_pending,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_publication_due, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_publication_mark_attempt,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_event_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_event_canonical_json,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_event_digest, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_event_path_for_digest,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_event_validate_path, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_event_validate_bytes,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_event_resolve_aliases,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_events_reduce, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_ref_parts, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_row_index_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_row_ref_lookup_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_row_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_validate_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_upsert_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_merge_indexes, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_relations_builtins, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_relation_lookup, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_relation_label, m)?)?;
    m.add_function(wrap_pyfunction!(py_links_block_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_links_block_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_links_block_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(py_links_block_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_links_block_strip, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_md_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_companion_md_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_frontmatter_inlet, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_add_link, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_set_link_projection, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_remove_link, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
