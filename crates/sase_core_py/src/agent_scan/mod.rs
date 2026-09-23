//! Agent artifact scan and index bindings: scanner, index, stats, and selectors.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use crate::telemetry::{telemetry_request_from_pydict, telemetry_result_to_py};

use pyo3::wrap_pyfunction;

/// Walk an agent-artifact tree and return the snapshot dict.
///
/// Mirrors `sase.core.agent_scan_facade.scan_agent_artifacts_python`. The
/// dict shape matches what `agent_scan_wire_to_json_dict` produces on the
/// Python side, so the facade can rehydrate it into the Phase 3A
/// dataclasses without a custom JSON re-encode step.
///
/// `options` is an optional `AgentArtifactScanOptionsWire`-shape dict. Any
/// fields the dict omits fall back to the wire defaults (matching the
/// pure-Python helper's `AgentArtifactScanOptionsWire()` default). The GIL
/// is released for the duration of the filesystem walk.
#[pyfunction]
#[pyo3(name = "scan_agent_artifacts", signature = (projects_root, options = None))]
fn py_scan_agent_artifacts<'py>(
    py: Python<'py>,
    projects_root: &str,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let root = PathBuf::from(projects_root);
    let snapshot = py.allow_threads(|| core_scan_agent_artifacts(&root, opts));
    let value = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Scan exact agent artifact directories and return scanner-shaped records.
#[pyfunction]
#[pyo3(
    name = "scan_agent_artifact_dirs",
    signature = (projects_root, artifact_dirs, options = None)
)]
fn py_scan_agent_artifact_dirs<'py>(
    py: Python<'py>,
    projects_root: &str,
    artifact_dirs: Vec<String>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let root = PathBuf::from(projects_root);
    let artifacts = artifact_dirs
        .into_iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let snapshot = py.allow_threads(|| {
        core_scan_agent_artifact_dirs(&root, &artifacts, opts)
    });
    let value = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return wall-clock union runtime for clan/family members.
#[pyfunction]
#[pyo3(name = "aggregate_clan_runtime")]
fn py_aggregate_clan_runtime<'py>(
    py: Python<'py>,
    members: &Bound<'py, PyList>,
    now_epoch_seconds: f64,
) -> PyResult<PyObject> {
    let members = clan_runtime_members_from_py_list(members)?;
    let runtime = py.allow_threads(|| {
        core_aggregate_clan_runtime(&members, now_epoch_seconds)
    });
    let value = serde_json::to_value(&runtime).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Build the canonical physical path for one agent artifact timestamp.
#[pyfunction]
#[pyo3(
    name = "canonical_agent_artifact_path",
    signature = (projects_root, project_name, workflow_dir_name, timestamp)
)]
fn py_canonical_agent_artifact_path(
    projects_root: &str,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> String {
    core_canonical_agent_artifact_path(
        &PathBuf::from(projects_root),
        project_name,
        workflow_dir_name,
        timestamp,
    )
    .to_string_lossy()
    .into_owned()
}

/// Resolve a legacy or sharded artifact path to the current physical path.
#[pyfunction]
#[pyo3(name = "resolve_agent_artifact_path")]
fn py_resolve_agent_artifact_path(
    projects_root: &str,
    artifact_dir: &str,
) -> String {
    core_resolve_agent_artifact_path(
        &PathBuf::from(projects_root),
        &PathBuf::from(artifact_dir),
    )
    .to_string_lossy()
    .into_owned()
}

/// Resolve a project/workflow/timestamp tuple to the current physical path.
#[pyfunction]
#[pyo3(
    name = "resolve_agent_artifact_timestamp_path",
    signature = (projects_root, project_name, workflow_dir_name, timestamp)
)]
fn py_resolve_agent_artifact_timestamp_path(
    projects_root: &str,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> String {
    core_resolve_agent_artifact_timestamp_path(
        &PathBuf::from(projects_root),
        project_name,
        workflow_dir_name,
        timestamp,
    )
    .to_string_lossy()
    .into_owned()
}

/// Parse a legacy or sharded artifact path into layout metadata.
#[pyfunction]
#[pyo3(name = "parse_agent_artifact_path")]
fn py_parse_agent_artifact_path<'py>(
    py: Python<'py>,
    projects_root: &str,
    artifact_dir: &str,
) -> PyResult<Option<PyObject>> {
    let info = core_parse_agent_artifact_path(
        &PathBuf::from(projects_root),
        &PathBuf::from(artifact_dir),
    );
    match info {
        Some(info) => {
            let value = serde_json::to_value(&info).map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
            Ok(Some(json_value_to_py(py, &value)?))
        }
        None => Ok(None),
    }
}

/// List artifact directories for one workflow under one project.
#[pyfunction]
#[pyo3(
    name = "iter_agent_artifact_dirs",
    signature = (projects_root, project_name, workflow_dir_name, newest_first = false)
)]
fn py_iter_agent_artifact_dirs(
    projects_root: &str,
    project_name: &str,
    workflow_dir_name: &str,
    newest_first: bool,
) -> Vec<String> {
    let workflow_dir = PathBuf::from(projects_root)
        .join(project_name)
        .join("artifacts")
        .join(workflow_dir_name);
    core_collect_workflow_artifact_candidates(
        &workflow_dir,
        workflow_dir_name,
        newest_first,
    )
    .candidates
    .into_iter()
    .map(|candidate| candidate.artifact_dir.to_string_lossy().into_owned())
    .collect()
}

/// Rebuild the persistent agent artifact index from source artifacts.
#[pyfunction]
#[pyo3(
    name = "rebuild_agent_artifact_index",
    signature = (index_path, projects_root, options = None)
)]
fn py_rebuild_agent_artifact_index<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let update = py
        .allow_threads(|| {
            core_rebuild_agent_artifact_index(&index, &root, opts)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Upsert one artifact directory into the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "upsert_agent_artifact_index_row",
    signature = (index_path, projects_root, artifact_dir, options = None)
)]
fn py_upsert_agent_artifact_index_row<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    artifact_dir: &str,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let artifact = PathBuf::from(artifact_dir);
    let update = py
        .allow_threads(|| {
            core_upsert_agent_artifact_index_row(&index, &root, &artifact, opts)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete one artifact directory row from the persistent artifact index.
#[pyfunction]
#[pyo3(name = "delete_agent_artifact_index_row")]
fn py_delete_agent_artifact_index_row<'py>(
    py: Python<'py>,
    index_path: &str,
    artifact_dir: &str,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let artifact = PathBuf::from(artifact_dir);
    let update = py
        .allow_threads(|| {
            core_delete_agent_artifact_index_row(&index, &artifact)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete one artifact row using a bounded SQLite busy timeout.
#[pyfunction]
#[pyo3(name = "delete_agent_artifact_index_row_bounded")]
fn py_delete_agent_artifact_index_row_bounded<'py>(
    py: Python<'py>,
    index_path: &str,
    artifact_dir: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let artifact = PathBuf::from(artifact_dir);
    let update = py
        .allow_threads(|| {
            core_delete_agent_artifact_index_row_with_busy_timeout(
                &index,
                &artifact,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Terminalize stale, unclaimed active rows in the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "terminalize_stale_active_agent_artifact_index_rows",
    signature = (
        index_path,
        projects_root,
        stale_after_seconds,
        max_rows = None,
        options = None
    )
)]
fn py_terminalize_stale_active_agent_artifact_index_rows<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    stale_after_seconds: u64,
    max_rows: Option<u32>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let update = py
        .allow_threads(|| {
            core_terminalize_stale_active_agent_artifact_index_rows(
                &index,
                &root,
                opts,
                stale_after_seconds,
                max_rows,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Prune old hidden terminal rows from the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "prune_hidden_terminal_agent_artifact_index_rows",
    signature = (index_path, hot_rows = None)
)]
fn py_prune_hidden_terminal_agent_artifact_index_rows<'py>(
    py: Python<'py>,
    index_path: &str,
    hot_rows: Option<u32>,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let update = py
        .allow_threads(|| {
            core_prune_hidden_terminal_agent_artifact_index_rows(
                &index, hot_rows,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Back-fill dismissed identities for visible dead members of dismissed families.
#[pyfunction]
#[pyo3(
    name = "reconcile_agent_artifact_index_dismissed_family_members",
    signature = (index_path, dry_run = false)
)]
fn py_reconcile_agent_artifact_index_dismissed_family_members<'py>(
    py: Python<'py>,
    index_path: &str,
    dry_run: bool,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let update = py
        .allow_threads(|| {
            core_reconcile_agent_artifact_index_dismissed_family_members(
                &index, dry_run,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Read one metadata value from the persistent artifact index.
#[pyfunction]
#[pyo3(name = "read_agent_artifact_index_meta")]
fn py_read_agent_artifact_index_meta<'py>(
    py: Python<'py>,
    index_path: &str,
    key: &str,
) -> PyResult<Option<String>> {
    let index = PathBuf::from(index_path);
    py.allow_threads(|| core_read_agent_artifact_index_meta(&index, key))
        .map_err(PyRuntimeError::new_err)
}

/// Write one metadata value in the persistent artifact index.
#[pyfunction]
#[pyo3(name = "write_agent_artifact_index_meta")]
fn py_write_agent_artifact_index_meta<'py>(
    py: Python<'py>,
    index_path: &str,
    key: &str,
    value: &str,
) -> PyResult<()> {
    let index = PathBuf::from(index_path);
    py.allow_threads(|| {
        core_write_agent_artifact_index_meta(&index, key, value)
    })
    .map_err(PyRuntimeError::new_err)
}

/// Return lightweight row-count status for the persistent artifact index.
#[pyfunction]
#[pyo3(name = "agent_artifact_index_status")]
fn py_agent_artifact_index_status<'py>(
    py: Python<'py>,
    index_path: &str,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let status = py
        .allow_threads(|| core_agent_artifact_index_status(&index))
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&status).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Reclaim freelist pages in the persistent artifact index via `VACUUM`.
#[pyfunction]
#[pyo3(name = "vacuum_agent_artifact_index")]
fn py_vacuum_agent_artifact_index<'py>(
    py: Python<'py>,
    index_path: &str,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let update = py
        .allow_threads(|| core_vacuum_agent_artifact_index(&index))
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Query scanner-shaped rows from the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "query_agent_artifact_index",
    signature = (index_path, projects_root, query = None, options = None)
)]
fn py_query_agent_artifact_index<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    query: Option<&Bound<'py, PyDict>>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let query_wire = match query {
        Some(dict) => {
            let json = py_to_json_value(dict)?;
            serde_json::from_value::<AgentArtifactIndexQueryWire>(json)
                .map_err(|e| {
                    PyValueError::new_err(format!(
                        "query is not a valid AgentArtifactIndexQueryWire dict: {e}"
                    ))
                })?
        }
        None => AgentArtifactIndexQueryWire::default(),
    };
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let snapshot = py
        .allow_threads(|| {
            core_query_agent_artifact_index(&index, &root, query_wire, opts)
        })
        .map_err(PyRuntimeError::new_err)?;
    serialize_to_py(py, &snapshot)
}

/// Load full scanner-shaped artifact records by artifact dir.
#[pyfunction]
#[pyo3(name = "load_agent_artifact_records")]
fn py_load_agent_artifact_records<'py>(
    py: Python<'py>,
    index_path: &str,
    artifact_dirs: Vec<String>,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let records = py
        .allow_threads(|| {
            core_load_agent_artifact_records(&index, &artifact_dirs)
        })
        .map_err(PyRuntimeError::new_err)?;
    serialize_to_py(py, &records)
}

/// Return the newest real gate-shell record for `gate_id`, or `None`.
///
/// Uses the persistent index's indexed `gate_shell_id` column, an O(1) SQL
/// lookup instead of decoding every historical record.
#[pyfunction]
#[pyo3(
    name = "find_gate_shell_by_gate_id",
    signature = (index_path, project_name, gate_id)
)]
fn py_find_gate_shell_by_gate_id<'py>(
    py: Python<'py>,
    index_path: &str,
    project_name: Option<&str>,
    gate_id: &str,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let record = py
        .allow_threads(|| {
            core_find_gate_shell_by_gate_id(&index, project_name, gate_id)
        })
        .map_err(PyRuntimeError::new_err)?;
    serialize_to_py(py, &record)
}

#[pyfunction(name = "agent_output_variable_history_wire_schema_version")]
fn py_agent_output_variable_history_wire_schema_version() -> u32 {
    AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION
}

/// Query grouped output-variable history from the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "query_agent_output_variable_history",
    signature = (index_path, query = None)
)]
fn py_query_agent_output_variable_history<'py>(
    py: Python<'py>,
    index_path: &str,
    query: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let query_wire = match query {
        Some(dict) => {
            let json = py_to_json_value(dict)?;
            serde_json::from_value::<AgentOutputVariableHistoryQueryWire>(json)
                .map_err(|e| {
                    PyValueError::new_err(format!(
                        "query is not a valid AgentOutputVariableHistoryQueryWire dict: {e}"
                    ))
                })?
        }
        None => AgentOutputVariableHistoryQueryWire::default(),
    };
    let index = PathBuf::from(index_path);
    let history = py
        .allow_threads(|| {
            core_query_agent_output_variable_history(&index, query_wire)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&history).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction(name = "agent_alias_history_wire_schema_version")]
fn py_agent_alias_history_wire_schema_version() -> u32 {
    AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION
}

/// Query bounded per-alias agent history from the persistent artifact index.
#[pyfunction]
#[pyo3(name = "query_agent_alias_history")]
fn py_query_agent_alias_history<'py>(
    py: Python<'py>,
    index_path: &str,
    query: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let json = py_to_json_value(query)?;
    let query_wire = serde_json::from_value::<AgentAliasHistoryQueryWire>(json)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "query is not a valid AgentAliasHistoryQueryWire dict: {e}"
            ))
        })?;
    let index = PathBuf::from(index_path);
    let history = py
        .allow_threads(|| core_query_agent_alias_history(&index, query_wire))
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&history).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction(name = "agent_output_variable_selector_wire_schema_version")]
fn py_agent_output_variable_selector_wire_schema_version() -> u32 {
    AGENT_OUTPUT_VARIABLE_SELECTOR_WIRE_SCHEMA_VERSION
}

/// Parse one `sase var get` selector into a typed wire dict.
#[pyfunction]
#[pyo3(name = "parse_output_variable_selector")]
fn py_parse_output_variable_selector<'py>(
    py: Python<'py>,
    selector: &str,
) -> PyResult<PyObject> {
    let parsed = core_parse_output_variable_selector(selector)
        .map_err(selector_error_to_pyerr)?;
    let value = serde_json::to_value(&parsed).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Resolve output-variable selectors against the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "query_agent_output_variable_selectors",
    signature = (index_path, query = None)
)]
fn py_query_agent_output_variable_selectors<'py>(
    py: Python<'py>,
    index_path: &str,
    query: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let query_wire = match query {
        Some(dict) => {
            let json = py_to_json_value(dict)?;
            serde_json::from_value::<AgentOutputVariableSelectorQueryWire>(json)
                .map_err(|e| {
                    PyValueError::new_err(format!(
                        "query is not a valid AgentOutputVariableSelectorQueryWire dict: {e}"
                    ))
                })?
        }
        None => AgentOutputVariableSelectorQueryWire::default(),
    };
    let index = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| {
            core_query_agent_output_variable_selectors(&index, query_wire)
        })
        .map_err(selector_error_to_pyerr)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn selector_error_to_pyerr(err: OutputVariableSelectorError) -> PyErr {
    match err {
        OutputVariableSelectorError::Invalid { .. } => {
            PyValueError::new_err(err.to_string())
        }
        _ => PyRuntimeError::new_err(err.to_json_string()),
    }
}

#[pyfunction]
#[pyo3(
    name = "query_related_agent_artifact_dirs",
    signature = (index_path, artifact_dir, seed_timestamps)
)]
fn py_query_related_agent_artifact_dirs(
    py: Python<'_>,
    index_path: &str,
    artifact_dir: &str,
    seed_timestamps: Vec<String>,
) -> PyResult<Vec<String>> {
    let index = PathBuf::from(index_path);
    let artifact = PathBuf::from(artifact_dir);
    py.allow_threads(|| {
        core_query_related_agent_artifact_dirs(
            &index,
            &artifact,
            &seed_timestamps,
        )
    })
    .map_err(PyRuntimeError::new_err)
}

#[pyfunction]
#[pyo3(name = "resolve_clan_summary")]
fn py_resolve_clan_summary<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ClanTribeResolutionRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid ClanTribeResolutionRequestWire dict: {e}"
            ))
        })?;
    let result = py.allow_threads(|| core_resolve_clan_summary(request));
    let value = serde_json::to_value(result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "resolve_clan_tribe")]
fn py_resolve_clan_tribe<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ClanTribeResolutionRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid ClanTribeResolutionRequestWire dict: {e}"
            ))
        })?;
    let result = py.allow_threads(|| core_resolve_clan_tribe(request));
    let value = serde_json::to_value(result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Load one clan's durable record, or `None` when it is absent.
#[pyfunction]
#[pyo3(name = "load_agent_clan_record", signature = (records_dir, clan))]
fn py_load_agent_clan_record<'py>(
    py: Python<'py>,
    records_dir: &str,
    clan: &str,
) -> PyResult<Option<PyObject>> {
    let dir = PathBuf::from(records_dir);
    let clan = clan.to_string();
    let record = py
        .allow_threads(|| {
            sase_core::agent_clan_record::load_clan_record(&dir, &clan)
        })
        .map_err(clan_record_error_to_pyerr)?;
    match record {
        Some(record) => {
            let value = serde_json::to_value(&record).map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
            Ok(Some(json_value_to_py(py, &value)?))
        }
        None => Ok(None),
    }
}

/// Merge one clan record update and return the outcome dict.
#[pyfunction]
#[pyo3(name = "record_agent_clan_attributes", signature = (records_dir, update))]
fn py_record_agent_clan_attributes<'py>(
    py: Python<'py>,
    records_dir: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(update.as_any())?;
    let update: sase_core::agent_clan_record::ClanRecordUpdateWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "update is not a valid ClanRecordUpdateWire dict: {e}"
            ))
        })?;
    let dir = PathBuf::from(records_dir);
    let outcome = py
        .allow_threads(|| {
            sase_core::agent_clan_record::record_clan_attributes(&dir, update)
        })
        .map_err(clan_record_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

/// Capture a dying artifact directory's clan attributes as `captured`.
///
/// Returns the clan record dict, or `None` when the directory carries
/// nothing to capture.
#[pyfunction]
#[pyo3(
    name = "capture_agent_clan_record_from_artifacts",
    signature = (records_dir, artifacts_dir)
)]
fn py_capture_agent_clan_record_from_artifacts<'py>(
    py: Python<'py>,
    records_dir: &str,
    artifacts_dir: &str,
) -> PyResult<Option<PyObject>> {
    let dir = PathBuf::from(records_dir);
    let artifacts = PathBuf::from(artifacts_dir);
    let record = py
        .allow_threads(|| {
            sase_core::agent_clan_record::capture_clan_record_from_artifacts(
                &dir, &artifacts,
            )
        })
        .map_err(clan_record_error_to_pyerr)?;
    match record {
        Some(record) => {
            let value = serde_json::to_value(&record).map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
            Ok(Some(json_value_to_py(py, &value)?))
        }
        None => Ok(None),
    }
}

/// Resolve remembered attributes for a new generation of a clan.
#[pyfunction]
#[pyo3(
    name = "resolve_agent_clan_launch_defaults",
    signature = (records_dir, clan, exclude_generation = None)
)]
fn py_resolve_agent_clan_launch_defaults<'py>(
    py: Python<'py>,
    records_dir: &str,
    clan: &str,
    exclude_generation: Option<&str>,
) -> PyResult<PyObject> {
    let dir = PathBuf::from(records_dir);
    let clan = clan.to_string();
    let excluded = exclude_generation.map(str::to_string);
    let defaults = py
        .allow_threads(|| {
            sase_core::agent_clan_record::resolve_clan_launch_defaults(
                &dir,
                &clan,
                excluded.as_deref(),
            )
        })
        .map_err(clan_record_error_to_pyerr)?;
    serialize_to_py(py, &defaults)
}

fn clan_record_error_to_pyerr(
    err: sase_core::agent_clan_record::ClanRecordError,
) -> PyErr {
    match err {
        sase_core::agent_clan_record::ClanRecordError::InvalidClan(_)
        | sase_core::agent_clan_record::ClanRecordError::InvalidGeneration(_) => {
            PyValueError::new_err(err.to_string())
        }
        _ => PyRuntimeError::new_err(err.to_string()),
    }
}

/// Deserialize a `AgentArtifactScanOptionsWire` from a Python dict.
///
/// Translates the dict to `serde_json::Value` first so missing fields use
/// the Rust struct's serde defaults — this matches the Python facade's
/// "absent → default" behavior for callers who pass partial dicts.
fn agent_scan_options_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<AgentArtifactScanOptionsWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "options is not a valid AgentArtifactScanOptionsWire dict: {e}"
        ))
    })
}

fn clan_runtime_members_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ClanRuntimeMemberWire>> {
    let mut members = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let member = serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "members[{index}] is not a valid ClanRuntimeMemberWire dict: {error}"
            ))
        })?;
        members.push(member);
    }
    Ok(members)
}

/// Aggregate run-backed Statistics views over a caller-supplied time range.
#[pyfunction]
#[pyo3(name = "agent_stats_query_runs")]
fn py_agent_stats_query_runs<'py>(
    py: Python<'py>,
    index_path: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentRunStatsRequestWire =
        telemetry_request_from_pydict(request, "AgentRunStatsRequestWire")?;
    let path = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| core_query_run_stats(&path, request))
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Aggregate durable skill, memory, question, and plan activity.
#[pyfunction]
#[pyo3(name = "agent_stats_query_activity")]
fn py_agent_stats_query_activity<'py>(
    py: Python<'py>,
    index_path: &str,
    sase_home: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentActivityStatsRequestWire = telemetry_request_from_pydict(
        request,
        "AgentActivityStatsRequestWire",
    )?;
    let index_path = PathBuf::from(index_path);
    let sase_home = PathBuf::from(sase_home);
    let result = py
        .allow_threads(|| {
            core_query_activity_stats(&index_path, &sase_home, request)
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

pub(crate) fn register_agent_scan(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_scan_agent_artifacts, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_agent_artifact_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py_aggregate_clan_runtime, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonical_agent_artifact_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_agent_artifact_path, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_resolve_agent_artifact_timestamp_path,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_parse_agent_artifact_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_agent_artifact_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py_rebuild_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_upsert_agent_artifact_index_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_agent_artifact_index_row, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_delete_agent_artifact_index_row_bounded,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_terminalize_stale_active_agent_artifact_index_rows,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_prune_hidden_terminal_agent_artifact_index_rows,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_reconcile_agent_artifact_index_dismissed_family_members,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_read_agent_artifact_index_meta, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_agent_artifact_index_meta, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_artifact_index_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_vacuum_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_load_agent_artifact_records, m)?)?;
    m.add_function(wrap_pyfunction!(py_find_gate_shell_by_gate_id, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_output_variable_history_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_query_agent_output_variable_history,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_alias_history_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_alias_history, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_output_variable_selector_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_parse_output_variable_selector, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_query_agent_output_variable_selectors,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_query_related_agent_artifact_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_clan_summary, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_clan_tribe, m)?)?;
    m.add_function(wrap_pyfunction!(py_load_agent_clan_record, m)?)?;
    m.add_function(wrap_pyfunction!(py_record_agent_clan_attributes, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_capture_agent_clan_record_from_artifacts,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_resolve_agent_clan_launch_defaults,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_agent_stats_query_runs, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_stats_query_activity, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
