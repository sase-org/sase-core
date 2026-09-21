//! Bead store bindings: reads, mutations, migrations, and wire converters.

use crate::prelude::*;

use crate::artifact_refs::artifact_ref_context_from_pydict;

use crate::json_bridge::{
    json_value_to_py, py_to_json_value, strings_to_paths,
};

use pyo3::wrap_pyfunction;

// --- Bead read bindings ---------------------------------------------------
#[pyfunction]
#[pyo3(
    name = "bead_needs_size_check_relax_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_size_check_relax_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_size_check_relax_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_size_check_relax_migration_sql")]
fn py_bead_size_check_relax_migration_sql() -> &'static str {
    core_bead_size_check_relax_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_task_ready_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_task_ready_migration(create_table_sql: Option<&str>) -> bool {
    core_bead_needs_task_ready_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_task_ready_migration_sql")]
fn py_bead_task_ready_migration_sql() -> &'static str {
    core_bead_task_ready_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_snoozed_status_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_snoozed_status_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_snoozed_status_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_snoozed_status_migration_sql")]
fn py_bead_snoozed_status_migration_sql() -> &'static str {
    core_bead_snoozed_status_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_flag_type_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_flag_type_migration(create_table_sql: Option<&str>) -> bool {
    core_bead_needs_flag_type_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_flag_type_migration_sql")]
fn py_bead_flag_type_migration_sql() -> &'static str {
    core_bead_flag_type_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_drop_flag_type_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_drop_flag_type_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_drop_flag_type_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_drop_flag_type_migration_sql")]
fn py_bead_drop_flag_type_migration_sql() -> &'static str {
    core_bead_drop_flag_type_migration_sql()
}

#[pyfunction]
#[pyo3(name = "bead_prune_removed_flag_event_streams")]
fn py_bead_prune_removed_flag_event_streams(
    py: Python<'_>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let outcome = py
        .allow_threads(|| {
            core_bead_prune_removed_flag_event_streams(&PathBuf::from(
                beads_dir,
            ))
        })
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let value = serde_json::to_value(&outcome).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_external_ref_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_external_ref_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_external_ref_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_external_ref_migration_sql")]
fn py_bead_external_ref_migration_sql() -> &'static str {
    core_bead_external_ref_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_resolution_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_resolution_migration(create_table_sql: Option<&str>) -> bool {
    core_bead_needs_resolution_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_resolution_migration_sql")]
fn py_bead_resolution_migration_sql() -> &'static str {
    core_bead_resolution_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_plus_one_evidence_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_plus_one_evidence_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_plus_one_evidence_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_plus_one_evidence_migration_sql")]
fn py_bead_plus_one_evidence_migration_sql() -> &'static str {
    core_bead_plus_one_evidence_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_task_type_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_task_type_migration(create_table_sql: Option<&str>) -> bool {
    core_bead_needs_task_type_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_task_type_migration_sql")]
fn py_bead_task_type_migration_sql() -> &'static str {
    core_bead_task_type_migration_sql()
}

#[pyfunction]
#[pyo3(name = "bead_target_routing_wire_schema_version")]
fn py_bead_target_routing_wire_schema_version() -> u64 {
    BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_route_targets")]
fn py_bead_route_targets<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: BeadTargetRoutingRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid BeadTargetRoutingRequestWire dict: {error}"
        ))
    })?;
    let value = serde_json::to_value(core_bead_route_targets(&request))
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal bead target-routing serialize error: {error}"
            ))
        })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "bead_read_store")]
fn py_bead_read_store<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_read_store_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_read_event_store")]
fn py_bead_read_event_store<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_read_event_store_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_read_legacy_jsonl")]
fn py_bead_read_legacy_jsonl<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_read_legacy_jsonl_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_resolve_id")]
fn py_bead_resolve_id(
    py: Python<'_>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<String> {
    let beads_dir = PathBuf::from(beads_dir);
    py.allow_threads(|| core_bead_resolve_issue_id(&beads_dir, issue_id))
        .map_err(bead_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "bead_show")]
fn py_bead_show<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_show_issue(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_show_issue_detail")]
#[pyo3(signature = (beads_dir, issue_id, include_links=true))]
fn py_bead_show_issue_detail<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    include_links: bool,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_show_issue_detail(&beads_dir, issue_id, include_links)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_history")]
fn py_bead_history<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_history(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_lost_notes")]
#[pyo3(signature = (beads_dir, issue_id=None))]
fn py_bead_lost_notes<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: Option<&str>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_lost_notes(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_touch_index_wire_schema_version")]
fn py_bead_touch_index_wire_schema_version() -> u32 {
    BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "bead_touch_index_refresh")]
fn py_bead_touch_index_refresh<'py>(
    py: Python<'py>,
    beads_dir: &str,
    index_path: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let index_path = PathBuf::from(index_path);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_refresh_bead_touch_index(&beads_dir, &index_path)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_touch_index_query")]
#[pyo3(signature = (index_path, actors=None))]
fn py_bead_touch_index_query<'py>(
    py: Python<'py>,
    index_path: &str,
    actors: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let index_path = PathBuf::from(index_path);
    let result = py.allow_threads(|| {
        core_query_bead_touches(&index_path, actors.as_deref())
    });
    bead_result_to_py(py, Ok::<_, BeadError>(result))
}

#[pyfunction]
#[pyo3(name = "bead_touch_index_status")]
fn py_bead_touch_index_status<'py>(
    py: Python<'py>,
    beads_dir: &str,
    index_path: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let index_path = PathBuf::from(index_path);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_touch_index_status(&beads_dir, &index_path)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_list")]
#[pyo3(signature = (beads_dir, statuses=None, issue_types=None, tiers=None))]
fn py_bead_list<'py>(
    py: Python<'py>,
    beads_dir: &str,
    statuses: Option<Vec<String>>,
    issue_types: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_list_issues(
                &beads_dir,
                statuses.as_deref(),
                issue_types.as_deref(),
                tiers.as_deref(),
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_search")]
#[pyo3(signature = (beads_dir, query, statuses=None, issue_types=None, tiers=None, limit=None, regex=false))]
#[allow(clippy::too_many_arguments)]
fn py_bead_search<'py>(
    py: Python<'py>,
    beads_dir: &str,
    query: &str,
    statuses: Option<Vec<String>>,
    issue_types: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
    limit: Option<usize>,
    regex: bool,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_search_issues(
                &beads_dir,
                query,
                statuses.as_deref(),
                issue_types.as_deref(),
                tiers.as_deref(),
                limit,
                regex,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_ready")]
fn py_bead_ready<'py>(py: Python<'py>, beads_dir: &str) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_ready_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_blocked")]
fn py_bead_blocked<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_blocked_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_stats")]
fn py_bead_stats<'py>(py: Python<'py>, beads_dir: &str) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(py, py.allow_threads(|| core_bead_stats(&beads_dir)))
}

#[pyfunction]
#[pyo3(signature = (beads_dir, plan_roots=None, reference_context=None))]
#[pyo3(name = "bead_doctor")]
fn py_bead_doctor<'py>(
    beads_dir: &str,
    plan_roots: Option<Vec<String>>,
    reference_context: Option<&Bound<'py, PyDict>>,
) -> PyResult<Vec<String>> {
    let beads_dir = PathBuf::from(beads_dir);
    if plan_roots.is_none() && reference_context.is_none() {
        return core_bead_doctor(&beads_dir).map_err(bead_error_to_pyerr);
    }
    let roots = plan_roots.and_then(|roots| {
        (!roots.is_empty())
            .then(|| roots.into_iter().map(PathBuf::from).collect::<Vec<_>>())
    });
    let reference_context = reference_context
        .map(artifact_ref_context_from_pydict)
        .transpose()?;
    let result = core_bead_doctor_with_contexts(
        &beads_dir,
        roots.as_deref(),
        reference_context.as_ref(),
    );
    result.map_err(bead_error_to_pyerr)
}

#[pyfunction]
#[pyo3(signature = (beads_dir, plan_roots=None, reference_context=None))]
#[pyo3(name = "bead_doctor_report")]
fn py_bead_doctor_report<'py>(
    py: Python<'py>,
    beads_dir: &str,
    plan_roots: Option<Vec<String>>,
    reference_context: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    if plan_roots.is_none() && reference_context.is_none() {
        return bead_result_to_py(
            py,
            py.allow_threads(|| core_bead_doctor_report(&beads_dir)),
        );
    }
    let roots = plan_roots.and_then(|roots| {
        (!roots.is_empty())
            .then(|| roots.into_iter().map(PathBuf::from).collect::<Vec<_>>())
    });
    let reference_context = reference_context
        .map(artifact_ref_context_from_pydict)
        .transpose()?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_doctor_report_with_contexts(
                &beads_dir,
                roots.as_deref(),
                reference_context.as_ref(),
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_get_epic_children")]
fn py_bead_get_epic_children<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_get_epic_children(&beads_dir, epic_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_init_store")]
fn py_bead_init_store<'py>(
    py: Python<'py>,
    root_dir: &str,
    beads_dirname: &str,
    issue_prefix: &str,
    owner: &str,
) -> PyResult<PyObject> {
    let root_dir = PathBuf::from(root_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_init_store(&root_dir, beads_dirname, issue_prefix, owner)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_create")]
fn py_bead_create<'py>(
    py: Python<'py>,
    beads_dir: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let request = bead_create_request_from_pydict(request)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_create_issue(&beads_dir, request)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_update")]
fn py_bead_update<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    fields: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let fields = bead_update_fields_from_pydict(fields)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_update_issue(&beads_dir, issue_id, fields)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_update_many")]
fn py_bead_update_many<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
    fields: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let fields = bead_update_fields_from_pydict(fields)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_update_issues(&beads_dir, &issue_ids, fields)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_append_note")]
#[pyo3(signature = (beads_dir, issue_id, entry, author=None, now=None))]
fn py_bead_append_note<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_append_issue_note(
                &beads_dir, issue_id, entry, author, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_note_edit")]
#[pyo3(signature = (beads_dir, issue_id, note_id, text, author=None, now=None))]
fn py_bead_note_edit<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    note_id: &str,
    text: &str,
    author: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_edit_issue_note(
                &beads_dir, issue_id, note_id, text, author, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_note_remove")]
#[pyo3(signature = (beads_dir, issue_id, note_id, author=None, now=None))]
fn py_bead_note_remove<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    note_id: &str,
    author: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_remove_issue_note(
                &beads_dir, issue_id, note_id, author, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_plus_one", signature = (beads_dir, issue_id, reporter, note, refs=None, now=None, observed_since=None))]
// The argument list mirrors the exported Python binding signature; grouping it
// locally would add a wrapper type the caller could not use directly.
#[allow(clippy::too_many_arguments)]
fn py_bead_plus_one<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    reporter: &str,
    note: &str,
    refs: Option<Vec<String>>,
    now: Option<String>,
    observed_since: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let refs = refs.unwrap_or_default();
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_add_task_plus_one(
                &beads_dir,
                issue_id,
                reporter,
                note,
                &refs,
                now,
                observed_since,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(
    name = "bead_snooze",
    signature = (beads_dir, issue_id, until, plus_ones=None, reason="", actor="", now=None)
)]
// The argument list mirrors `snooze_task`'s signature exactly; bundling it
// into a struct here would only move the same fields behind a wire type the
// Python caller would have to build anyway.
#[allow(clippy::too_many_arguments)]
fn py_bead_snooze<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    until: &str,
    plus_ones: Option<u32>,
    reason: &str,
    actor: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_snooze_task(
                &beads_dir, issue_id, until, plus_ones, reason, actor, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(
    name = "bead_snooze_cancel",
    signature = (beads_dir, issue_id, actor="", now=None)
)]
fn py_bead_snooze_cancel<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    actor: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_cancel_task_snooze(&beads_dir, issue_id, actor, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_claim_for_agent_launch", signature = (beads_dir, bead_id, agent_name, now=None))]
fn py_bead_claim_for_agent_launch<'py>(
    py: Python<'py>,
    beads_dir: &str,
    bead_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_claim_for_agent_launch(
                &beads_dir, bead_id, agent_name, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_claim_for_agent_wait", signature = (beads_dir, bead_id, agent_name, now=None))]
fn py_bead_claim_for_agent_wait<'py>(
    py: Python<'py>,
    beads_dir: &str,
    bead_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_claim_for_agent_wait(&beads_dir, bead_id, agent_name, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_release_agent_claim", signature = (beads_dir, bead_id, agent_name, now=None))]
fn py_bead_release_agent_claim<'py>(
    py: Python<'py>,
    beads_dir: &str,
    bead_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_release_agent_claim(&beads_dir, bead_id, agent_name, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_preclaim_epic_work", signature = (beads_dir, epic_id, assignments, epic_agent_name=None, now=None))]
fn py_bead_preclaim_epic_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    assignments: &Bound<'py, PyList>,
    epic_agent_name: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let assignments = bead_preclaim_assignments_from_py_list(assignments)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_preclaim_epic_work_plan(
                &beads_dir,
                epic_id,
                &assignments,
                epic_agent_name,
                now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_open")]
#[pyo3(signature = (beads_dir, issue_id, now=None))]
fn py_bead_open<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_open_issue(&beads_dir, issue_id, now)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_close")]
#[pyo3(signature = (
    beads_dir,
    issue_ids,
    reason=None,
    resolution=None,
    force=false,
    now=None,
    note=None,
    author=None,
))]
#[allow(clippy::too_many_arguments)]
fn py_bead_close<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
    reason: Option<String>,
    resolution: Option<String>,
    force: bool,
    now: Option<String>,
    note: Option<String>,
    author: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let resolution = resolution
        .as_deref()
        .map(parse_bead_resolution)
        .transpose()?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_close_issues_with_note(
                &beads_dir, &issue_ids, reason, resolution, force, note,
                author, now,
            )
        }),
    )
}

fn parse_bead_resolution(value: &str) -> PyResult<BeadResolutionWire> {
    match value {
        "done" => Ok(BeadResolutionWire::Done),
        "canceled" => Ok(BeadResolutionWire::Canceled),
        "superseded" => Ok(BeadResolutionWire::Superseded),
        _ => Err(PyValueError::new_err(format!(
            "invalid bead resolution: {value}"
        ))),
    }
}

#[pyfunction]
#[pyo3(name = "bead_merge_event_streams")]
fn py_bead_merge_event_streams<'py>(
    py: Python<'py>,
    base: &Bound<'py, PyDict>,
    ours: &Bound<'py, PyDict>,
    theirs: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let base = bead_event_stream_from_pydict(base, "base")?;
    let ours = bead_event_stream_from_pydict(ours, "ours")?;
    let theirs = bead_event_stream_from_pydict(theirs, "theirs")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_merge_bead_event_streams(&base, &ours, &theirs)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_merge_event_streams_with_relocation")]
#[pyo3(signature = (base, ours, theirs, relocation_issue_id=None))]
fn py_bead_merge_event_streams_with_relocation<'py>(
    py: Python<'py>,
    base: &Bound<'py, PyDict>,
    ours: &Bound<'py, PyDict>,
    theirs: &Bound<'py, PyDict>,
    relocation_issue_id: Option<String>,
) -> PyResult<PyObject> {
    let base = bead_event_stream_from_pydict(base, "base")?;
    let ours = bead_event_stream_from_pydict(ours, "ours")?;
    let theirs = bead_event_stream_from_pydict(theirs, "theirs")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_merge_bead_event_streams_with_relocation(
                &base,
                &ours,
                &theirs,
                relocation_issue_id.as_deref(),
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_reduce_event_streams")]
fn py_bead_reduce_event_streams<'py>(
    py: Python<'py>,
    streams: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let streams = bead_event_streams_from_py_list(streams)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_reduce_event_streams(&streams)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_event_store_manifest")]
fn py_bead_event_store_manifest<'py>(
    py: Python<'py>,
    streams: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let streams = bead_event_streams_from_py_list(streams)?;
    let manifest = BeadEventStoreManifestWire::from_streams(&streams);
    bead_result_to_py(py, Ok(manifest))
}

#[pyfunction]
#[pyo3(name = "bead_repair_event_store_manifest")]
fn py_bead_repair_event_store_manifest<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_repair_event_store_manifest(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_remove")]
fn py_bead_remove<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_remove_issue(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_remove_many")]
fn py_bead_remove_many<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_remove_issues(&beads_dir, &issue_ids)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_set_link_projections")]
fn py_bead_set_link_projections<'py>(
    py: Python<'py>,
    beads_dir: &str,
    requests: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let requests = bead_link_projection_requests_from_py_list(requests)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_set_link_projections(&beads_dir, &requests)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_dep_add")]
#[pyo3(signature = (beads_dir, issue_id, depends_on_id, now=None))]
fn py_bead_dep_add<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    depends_on_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_add_dependency(&beads_dir, issue_id, depends_on_id, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_dep_remove")]
#[pyo3(signature = (beads_dir, issue_id, depends_on_ids, now=None))]
fn py_bead_dep_remove<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    depends_on_ids: Vec<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_remove_dependencies(
                &beads_dir,
                issue_id,
                &depends_on_ids,
                now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_mark_ready_to_work")]
#[pyo3(signature = (beads_dir, epic_id, now=None))]
fn py_bead_mark_ready_to_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_mark_ready_to_work(&beads_dir, epic_id, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_unmark_ready_to_work")]
#[pyo3(signature = (beads_dir, epic_id, now=None))]
fn py_bead_unmark_ready_to_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_unmark_ready_to_work(&beads_dir, epic_id, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_export_jsonl")]
fn py_bead_export_jsonl<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_export_jsonl(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_sync_is_clean")]
fn py_bead_sync_is_clean(beads_dir: &str) -> PyResult<bool> {
    let beads_dir = PathBuf::from(beads_dir);
    core_bead_sync_is_clean(&beads_dir).map_err(bead_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "bead_build_epic_work_plan")]
fn py_bead_build_epic_work_plan<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_build_epic_work_plan(&beads_dir, epic_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_build_epic_work_plan_from_issues")]
fn py_bead_build_epic_work_plan_from_issues<'py>(
    py: Python<'py>,
    issues: &Bound<'py, PyList>,
    epic_id: &str,
) -> PyResult<PyObject> {
    let issues = issues_from_py_list(issues)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_build_epic_work_plan_from_issues(issues, epic_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_cli_execute")]
#[pyo3(signature = (
    argv,
    read_beads_dirs,
    write_beads_dir,
    cwd,
    relativize_design_paths,
    plan_roots = Vec::new(),
))]
fn py_bead_cli_execute<'py>(
    py: Python<'py>,
    argv: Vec<String>,
    read_beads_dirs: Vec<String>,
    write_beads_dir: &str,
    cwd: &str,
    relativize_design_paths: bool,
    plan_roots: Vec<String>,
) -> PyResult<PyObject> {
    let read_beads_dirs = strings_to_paths(read_beads_dirs);
    let write_beads_dir = PathBuf::from(write_beads_dir);
    let cwd = PathBuf::from(cwd);
    let plan_roots = strings_to_paths(plan_roots);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_execute_bead_cli(
                &argv,
                &read_beads_dirs,
                &write_beads_dir,
                &cwd,
                relativize_design_paths,
                &plan_roots,
            )
        }),
    )
}

pub(crate) fn bead_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, BeadError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result.map_err(bead_error_to_pyerr)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

fn bead_link_projection_requests_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<BeadLinkProjectionRequestWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let request: BeadLinkProjectionRequestWire = serde_json::from_value(
            value,
        )
        .map_err(|e| {
            PyValueError::new_err(format!(
                "requests[{idx}] is not a valid BeadLinkProjectionRequestWire dict: {e}"
            ))
        })?;
        values.push(request);
    }
    Ok(values)
}

fn bead_create_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<BeadCreateRequestWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid BeadCreateRequestWire dict: {e}"
        ))
    })
}

fn bead_update_fields_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<BeadUpdateFieldsWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "fields is not a valid BeadUpdateFieldsWire dict: {e}"
        ))
    })
}

fn bead_preclaim_assignments_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<BeadPreclaimAssignmentWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let assignment: BeadPreclaimAssignmentWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "assignments[{idx}] is not a valid BeadPreclaimAssignmentWire dict: {e}"
                ))
            })?;
        values.push(assignment);
    }
    Ok(values)
}

fn issues_from_py_list(list: &Bound<'_, PyList>) -> PyResult<Vec<IssueWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let issue: IssueWire = serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "issues[{idx}] is not a valid IssueWire dict: {e}"
            ))
        })?;
        issue.validate().map_err(|e| {
            PyValueError::new_err(format!("issues[{idx}] is invalid: {e}"))
        })?;
        values.push(issue);
    }
    Ok(values)
}

fn bead_event_stream_from_pydict(
    dict: &Bound<'_, PyDict>,
    label: &str,
) -> PyResult<BeadEventStreamWire> {
    let value = py_to_json_value(dict.as_any())?;
    let stream: BeadEventStreamWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "{label} is not a valid BeadEventStreamWire dict: {e}"
            ))
        })?;
    stream.validate().map_err(bead_error_to_pyerr)?;
    Ok(stream)
}

fn bead_event_streams_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<BeadEventStreamWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let stream: BeadEventStreamWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "streams[{idx}] is not a valid BeadEventStreamWire dict: {e}"
                ))
            })?;
        stream.validate().map_err(bead_error_to_pyerr)?;
        values.push(stream);
    }
    Ok(values)
}

fn bead_error_to_pyerr(err: BeadError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

pub(crate) fn register_beads(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_bead_needs_size_check_relax_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_size_check_relax_migration_sql,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_task_ready_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_task_ready_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_needs_snoozed_status_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_snoozed_status_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_flag_type_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_flag_type_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_needs_drop_flag_type_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_drop_flag_type_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_prune_removed_flag_event_streams,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_external_ref_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_external_ref_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_resolution_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_resolution_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_needs_plus_one_evidence_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_plus_one_evidence_migration_sql,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_task_type_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_task_type_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_target_routing_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_route_targets, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_event_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_legacy_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_show, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_history, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_lost_notes, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_touch_index_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_touch_index_refresh, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_touch_index_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_touch_index_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_search, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_ready, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_blocked, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_stats, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_doctor, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_doctor_report, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_get_epic_children, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_show_issue_detail, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_resolve_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_init_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_create, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_update_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_append_note, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_note_edit, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_note_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_plus_one, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_snooze, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_snooze_cancel, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_claim_for_agent_launch, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_claim_for_agent_wait, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_release_agent_claim, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_preclaim_epic_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_open, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_close, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_merge_event_streams, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_merge_event_streams_with_relocation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_reduce_event_streams, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_event_store_manifest, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_repair_event_store_manifest, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_remove_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_set_link_projections, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_dep_add, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_dep_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_mark_ready_to_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_unmark_ready_to_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_export_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_sync_is_clean, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_build_epic_work_plan, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_build_epic_work_plan_from_issues,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_cli_execute, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
