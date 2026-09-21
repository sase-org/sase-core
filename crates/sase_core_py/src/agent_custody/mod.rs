//! Agent lifecycle bindings: cleanup, archives, retention, and ownership batches.

use crate::prelude::*;

use crate::json_bridge::{
    comments_from_py, hooks_from_py, json_value_to_py, mentors_from_py,
    py_to_json_value, serialize_to_py, strings_from_py_list,
};

use crate::procs::proc_store_result_to_py;

use pyo3::wrap_pyfunction;

/// Replace dismissed identities in the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "replace_agent_artifact_index_dismissed_agents",
    signature = (index_path, identities, force = false)
)]
fn py_replace_agent_artifact_index_dismissed_agents<'py>(
    py: Python<'py>,
    index_path: &str,
    identities: &Bound<'_, PyList>,
    force: bool,
) -> PyResult<PyObject> {
    let mut wire_identities: Vec<AgentCleanupIdentityWire> =
        Vec::with_capacity(identities.len());
    for (idx, item) in identities.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let identity: AgentCleanupIdentityWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "identities[{idx}] is not a valid AgentCleanupIdentityWire dict: {e}"
                ))
            })?;
        wire_identities.push(identity);
    }
    let index = PathBuf::from(index_path);
    let update = py
        .allow_threads(|| {
            core_replace_agent_artifact_index_dismissed_agents_with_force(
                &index,
                &wire_identities,
                force,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Query dismissed-agent archive summary rows from the canonical archive index.
#[pyfunction]
#[pyo3(name = "query_agent_archive")]
fn py_query_agent_archive<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveQueryRequestWire = serde_json::from_value(value)
        .map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentArchiveQueryRequestWire dict: {e}"
        ))
    })?;
    let result = py
        .allow_threads(|| {
            core_query_agent_archive(&PathBuf::from(root), request)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return grouped counts for a dismissed-agent archive facet.
#[pyfunction]
#[pyo3(name = "agent_archive_facet_counts")]
fn py_agent_archive_facet_counts<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveFacetRequestWire = serde_json::from_value(value)
        .map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentArchiveFacetRequestWire dict: {e}"
        ))
    })?;
    let result = py
        .allow_threads(|| {
            core_agent_archive_facet_counts(&PathBuf::from(root), request)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate and canonicalize an immutable archive source key.
#[pyfunction]
#[pyo3(name = "validate_agent_archive_key")]
fn py_validate_agent_archive_key<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveKeyWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentArchiveKeyWire dict: {e}"
            ))
        })?;
    let result = core_validate_agent_archive_key(request)
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate one archive visibility projection value.
#[pyfunction]
#[pyo3(name = "validate_agent_archive_visibility")]
fn py_validate_agent_archive_visibility<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveVisibilityWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentArchiveVisibilityWire dict: {e}"
            ))
        })?;
    let result = core_validate_agent_archive_visibility(request)
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate or derive archive capability claims from persisted inputs.
#[pyfunction]
#[pyo3(name = "validate_agent_archive_capabilities")]
fn py_validate_agent_archive_capabilities<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveCapabilityValidationRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentArchiveCapabilityValidationRequestWire dict: {e}"
            ))
        })?;
    let result = core_validate_agent_archive_capabilities(request)
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark preserved archive bundles as revived without deleting payloads.
#[pyfunction]
#[pyo3(name = "mark_agent_archive_bundles_revived")]
fn py_mark_agent_archive_bundles_revived<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveReviveMarkRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentArchiveReviveMarkRequestWire dict: {e}"
            ))
        })?;
    let result = py.allow_threads(|| {
        core_mark_agent_archive_bundles_revived(&PathBuf::from(root), request)
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Verify the dismissed-agent archive index against bundle payload files.
#[pyfunction]
#[pyo3(name = "verify_agent_archive_index")]
fn py_verify_agent_archive_index<'py>(
    py: Python<'py>,
    root: &str,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        core_verify_agent_archive_index(&PathBuf::from(root))
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Save one saved dismissed-agent group metadata record.
#[pyfunction]
#[pyo3(name = "save_dismissed_agent_group")]
fn py_save_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(group.as_any())?;
    let group: SavedAgentGroupWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "group is not a valid SavedAgentGroupWire dict: {e}"
            ))
        })?;
    let result = py
        .allow_threads(|| {
            core_save_dismissed_agent_group(&PathBuf::from(root), group)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// List saved dismissed-agent group summaries in newest-first pages.
#[pyfunction]
#[pyo3(name = "list_dismissed_agent_groups", signature = (root, limit = 20, cursor = None))]
fn py_list_dismissed_agent_groups<'py>(
    py: Python<'py>,
    root: &str,
    limit: i64,
    cursor: Option<i64>,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        core_list_dismissed_agent_groups(&PathBuf::from(root), limit, cursor)
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Load one saved dismissed-agent group, returning None when absent/corrupt.
#[pyfunction]
#[pyo3(name = "load_dismissed_agent_group")]
fn py_load_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_load_dismissed_agent_group(&PathBuf::from(root), group_id)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark one saved dismissed-agent group revived without deleting metadata.
#[pyfunction]
#[pyo3(name = "mark_dismissed_agent_group_revived")]
fn py_mark_dismissed_agent_group_revived<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
    revived_at: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_mark_dismissed_agent_group_revived(
                &PathBuf::from(root),
                group_id,
                revived_at,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete one saved dismissed-agent group metadata record.
#[pyfunction]
#[pyo3(name = "delete_dismissed_agent_group")]
fn py_delete_dismissed_agent_group(
    py: Python<'_>,
    root: &str,
    group_id: &str,
) -> PyResult<bool> {
    py.allow_threads(|| {
        core_delete_dismissed_agent_group(&PathBuf::from(root), group_id)
    })
    .map_err(PyValueError::new_err)
}

/// Record one recent dismissed-agent group and prune the capped recent store.
#[pyfunction]
#[pyo3(name = "record_recent_dismissed_agent_group", signature = (root, group, limit = 10))]
fn py_record_recent_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group: &Bound<'py, PyDict>,
    limit: i64,
) -> PyResult<PyObject> {
    let value = py_to_json_value(group.as_any())?;
    let group: SavedAgentGroupWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "group is not a valid SavedAgentGroupWire dict: {e}"
            ))
        })?;
    let result = py
        .allow_threads(|| {
            core_record_recent_dismissed_agent_group(
                &PathBuf::from(root),
                group,
                limit,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// List recent dismissed-agent group summaries from the capped store.
#[pyfunction]
#[pyo3(name = "list_recent_dismissed_agent_groups", signature = (root, limit = 10))]
fn py_list_recent_dismissed_agent_groups<'py>(
    py: Python<'py>,
    root: &str,
    limit: i64,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        core_list_recent_dismissed_agent_groups(&PathBuf::from(root), limit)
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Load one recent dismissed-agent group, returning None when absent/corrupt.
#[pyfunction]
#[pyo3(name = "load_recent_dismissed_agent_group")]
fn py_load_recent_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_load_recent_dismissed_agent_group(
                &PathBuf::from(root),
                group_id,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark one recent dismissed-agent group revived.
#[pyfunction]
#[pyo3(name = "mark_recent_dismissed_agent_group_revived")]
fn py_mark_recent_dismissed_agent_group_revived<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
    revived_at: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_mark_recent_dismissed_agent_group_revived(
                &PathBuf::from(root),
                group_id,
                revived_at,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan agent cleanup without executing side effects.
///
/// `targets` is a list of `AgentCleanupTargetWire`-shape dicts gathered by
/// the host. `request` is an `AgentCleanupRequestWire`-shape dict choosing
/// the scope and mode. The returned dict is an `AgentCleanupPlanWire` whose
/// kill/dismiss lists can be previewed or executed by Python.
#[pyfunction]
#[pyo3(name = "agent_cleanup_wire_schema_version")]
fn py_agent_cleanup_wire_schema_version() -> u32 {
    sase_core::AGENT_CLEANUP_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "force_reuse_stop_barrier_wire_schema_version")]
fn py_force_reuse_stop_barrier_wire_schema_version() -> u32 {
    sase_core::FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "decide_force_reuse_stop_barrier")]
fn py_decide_force_reuse_stop_barrier<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request_value = py_to_json_value(request.as_any())?;
    let req: ForceReuseStopBarrierRequestWire =
        serde_json::from_value(request_value).map_err(|e| {
            PyValueError::new_err(format!(
            "request is not a valid ForceReuseStopBarrierRequestWire dict: {e}"
        ))
        })?;
    let decision = core_decide_force_reuse_stop_barrier(&req)
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&decision).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "plan_agent_cleanup")]
fn py_plan_agent_cleanup<'py>(
    py: Python<'py>,
    targets: &Bound<'py, PyList>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let mut wire_targets: Vec<AgentCleanupTargetWire> =
        Vec::with_capacity(targets.len());
    for (idx, item) in targets.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let target: AgentCleanupTargetWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "targets[{idx}] is not a valid AgentCleanupTargetWire dict: {e}"
                ))
            })?;
        wire_targets.push(target);
    }

    let request_value = py_to_json_value(request.as_any())?;
    let req: AgentCleanupRequestWire =
        cleanup_request_from_json_value(&request_value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentCleanupRequestWire dict: {e}"
            ))
        })?;
    let plan = core_plan_agent_cleanup(&wire_targets, &req)
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "agent_ownership_batch_wire_schema_version")]
fn py_agent_ownership_batch_wire_schema_version() -> u32 {
    sase_core::AGENT_OWNERSHIP_BATCH_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "plan_agent_ownership_batch")]
fn py_plan_agent_ownership_batch<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request_value = py_to_json_value(request.as_any())?;
    let req = agent_ownership_batch_request_from_json_value(&request_value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentOwnershipBatchRequestWire dict: {e}"
            ))
        })?;
    let plan = core_plan_agent_ownership_batch(&req)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Save dismissed agent identities to the host-provided index file.
#[pyfunction]
#[pyo3(name = "save_dismissed_agents_index")]
fn py_save_dismissed_agents_index(
    path: &str,
    identities: &Bound<'_, PyList>,
) -> PyResult<()> {
    let mut wire_identities: Vec<AgentCleanupIdentityWire> =
        Vec::with_capacity(identities.len());
    for (idx, item) in identities.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let identity: AgentCleanupIdentityWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "identities[{idx}] is not a valid AgentCleanupIdentityWire dict: {e}"
                ))
            })?;
        wire_identities.push(identity);
    }
    core_save_dismissed_agents_index(&PathBuf::from(path), &wire_identities)
        .map_err(PyValueError::new_err)
}

/// Write one dismissed-agent bundle using the sharded bundle layout.
#[pyfunction]
#[pyo3(name = "save_dismissed_bundle")]
fn py_save_dismissed_bundle<'py>(
    py: Python<'py>,
    bundle_root: &str,
    bundle: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let json = py_to_json_value(bundle.as_any())?;
    let result =
        core_save_dismissed_bundle_json(&PathBuf::from(bundle_root), &json)
            .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete loader marker files from an agent artifacts directory.
#[pyfunction]
#[pyo3(name = "delete_agent_artifacts")]
fn py_delete_agent_artifacts<'py>(
    py: Python<'py>,
    artifacts_dir: &str,
) -> PyResult<PyObject> {
    let result =
        core_delete_agent_artifact_markers(&PathBuf::from(artifacts_dir))
            .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return project-file text after releasing one RUNNING workspace claim.
#[pyfunction]
#[pyo3(name = "release_workspace_from_content", signature = (content, workspace_num, workflow = None, cl_name = None))]
fn py_release_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    workspace_num: i64,
    workflow: Option<&str>,
    cl_name: Option<&str>,
) -> PyResult<PyObject> {
    let result = core_release_workspace_from_content(
        content,
        workspace_num,
        workflow,
        cl_name,
    );
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark hook status lines for killed running-agent suffixes.
#[pyfunction]
#[pyo3(name = "mark_hook_agents_as_killed")]
fn py_mark_hook_agents_as_killed<'py>(
    py: Python<'py>,
    hooks: &Bound<'py, PyList>,
    suffixes: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let wire_hooks = hooks_from_py(hooks)?;
    let suffixes = strings_from_py_list(suffixes, "suffixes")?;
    let result = core_mark_hook_agents_as_killed(&wire_hooks, &suffixes);
    json_value_to_py(
        py,
        &serde_json::to_value(result).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?,
    )
}

/// Mark mentor status lines for killed running-agent suffixes.
#[pyfunction]
#[pyo3(name = "mark_mentor_agents_as_killed")]
fn py_mark_mentor_agents_as_killed<'py>(
    py: Python<'py>,
    mentors: &Bound<'py, PyList>,
    suffixes: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let wire_mentors = mentors_from_py(mentors)?;
    let suffixes = strings_from_py_list(suffixes, "suffixes")?;
    let result = core_mark_mentor_agents_as_killed(&wire_mentors, &suffixes);
    json_value_to_py(
        py,
        &serde_json::to_value(result).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?,
    )
}

/// Mark comment entries for killed running-agent suffixes.
#[pyfunction]
#[pyo3(name = "mark_comment_agents_as_killed")]
fn py_mark_comment_agents_as_killed<'py>(
    py: Python<'py>,
    comments: &Bound<'py, PyList>,
    suffixes: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let wire_comments = comments_from_py(comments)?;
    let suffixes = strings_from_py_list(suffixes, "suffixes")?;
    let result = core_mark_comment_agents_as_killed(&wire_comments, &suffixes);
    json_value_to_py(
        py,
        &serde_json::to_value(result).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?,
    )
}

/// Plan byte-bounded filesystem batches for full agent publication.
#[pyfunction]
#[pyo3(name = "plan_agent_publication_batches")]
fn py_plan_agent_publication_batches<'py>(
    py: Python<'py>,
    records: &Bound<'_, PyList>,
    budget_bytes: u64,
) -> PyResult<PyObject> {
    let records =
        serde_json::from_value::<Vec<AgentPublicationPathRecordWire>>(
            py_to_json_value(records.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "records are not valid AgentPublicationPathRecordWire dicts: {error}"
            ))
        })?;
    let plan = core_plan_agent_publication_batches(&records, budget_bytes)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serialize_to_py(py, &plan)
}

fn agent_artifact_run_retention_error_to_pyerr(
    error: AgentArtifactRunRetentionError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the agent-artifact run-retention wire's schema version.
#[pyfunction]
#[pyo3(name = "agent_artifact_run_retention_wire_schema_version")]
fn py_agent_artifact_run_retention_wire_schema_version() -> u32 {
    AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION
}

/// Preview or apply ACE-run artifact-directory retention and the bottom-up
/// empty-shard walk in one owner call.
#[pyfunction]
#[pyo3(name = "apply_agent_artifact_run_retention")]
fn py_apply_agent_artifact_run_retention<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentArtifactRunRetentionRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid AgentArtifactRunRetentionRequestWire dict: {error}"
        ))
    })?;
    let outcome =
        py.allow_threads(|| core_apply_agent_artifact_run_retention(&request));
    proc_store_result_to_py(
        py,
        &outcome.map_err(agent_artifact_run_retention_error_to_pyerr)?,
    )
}

pub(crate) fn register_agent_custody(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_replace_agent_artifact_index_dismissed_agents,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_archive, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_archive_facet_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_archive_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_archive_visibility, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_validate_agent_archive_capabilities,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_agent_archive_bundles_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_verify_agent_archive_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(py_list_dismissed_agent_groups, m)?)?;
    m.add_function(wrap_pyfunction!(py_load_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_dismissed_agent_group_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_delete_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_record_recent_dismissed_agent_group,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_list_recent_dismissed_agent_groups,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_load_recent_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_recent_dismissed_agent_group_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_agent_cleanup_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_force_reuse_stop_barrier_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_decide_force_reuse_stop_barrier, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_cleanup, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_ownership_batch_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_ownership_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_agents_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_agent_artifacts, m)?)?;
    m.add_function(wrap_pyfunction!(py_release_workspace_from_content, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_hook_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_mentor_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_comment_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_publication_batches, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_artifact_run_retention_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_apply_agent_artifact_run_retention,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
