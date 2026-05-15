//! PyO3 bindings for `sase_core`.
//!
//! Phase 1D exposed `parse_project_bytes`. Phase 2D added the query
//! tokenizer/parser/evaluator. Phase 3C adds the agent-artifact snapshot
//! scanner. Currently exposed:
//!
//! - `parse_project_bytes(path: str, data: bytes) -> list[dict]`
//! - `tokenize_query(query: str) -> list[dict]`
//! - `parse_query(query: str) -> dict`
//! - `canonicalize_query(query: str) -> str`
//! - `compile_corpus(specs: list[dict]) -> QueryCorpusHandle`
//! - `compile_query(query: str) -> QueryProgramHandle`
//! - `evaluate_many(program: QueryProgramHandle, corpus: QueryCorpusHandle) -> list[bool]`
//! - `evaluate_query_many(query: str, specs: list[dict]) -> list[bool]`
//! - `scan_agent_artifacts(projects_root: str, options: dict | None = None) -> dict`
//! - `rebuild_agent_artifact_index(index_path: str, projects_root: str, options: dict | None = None) -> dict`
//! - `upsert_agent_artifact_index_row(index_path: str, projects_root: str, artifact_dir: str, options: dict | None = None) -> dict`
//! - `delete_agent_artifact_index_row(index_path: str, artifact_dir: str) -> dict`
//! - `query_agent_artifact_index(index_path: str, projects_root: str, query: dict | None = None, options: dict | None = None) -> dict`
//! - `query_agent_archive(root: str, request: dict) -> dict`
//! - `agent_archive_facet_counts(root: str, request: dict) -> dict`
//! - `mark_agent_archive_bundles_revived(root: str, request: dict) -> dict`
//! - `verify_agent_archive_index(root: str) -> dict`
//! - `plan_agent_cleanup(targets: list[dict], request: dict) -> dict`
//! - `save_dismissed_agents_index(path: str, identities: list[dict]) -> None`
//! - `save_dismissed_bundle(bundle_root: str, bundle: dict) -> dict`
//! - `delete_agent_artifacts(artifacts_dir: str) -> dict`
//! - `release_workspace_from_content(content: str, workspace_num: int, workflow: str | None, cl_name: str | None) -> dict`
//! - `mark_hook_agents_as_killed(hooks: list[dict], suffixes: list[str]) -> list[dict]`
//! - `mark_mentor_agents_as_killed(mentors: list[dict], suffixes: list[str]) -> list[dict]`
//! - `mark_comment_agents_as_killed(comments: list[dict], suffixes: list[str]) -> list[dict]`
//! - `remove_workspace_suffix(status: str) -> str`
//! - `is_valid_status_transition(from_status: str, to_status: str) -> bool`
//! - `read_status_from_lines(lines: list[str], changespec_name: str) -> str | None`
//! - `apply_status_update(lines: list[str], changespec_name: str, new_status: str) -> str`
//! - `plan_status_transition(request: dict) -> dict`
//! - `parse_git_name_status_z(stdout: str) -> list[dict]`
//! - `parse_git_branch_name(stdout: str) -> str | None`
//! - `derive_git_workspace_name(remote_url: str | None, root_path: str | None) -> str | None`
//! - `parse_git_conflicted_files(stdout: str) -> list[str]`
//! - `parse_git_local_changes(stdout: str) -> str | None`
//! - `read_notifications_snapshot(path: str, include_dismissed: bool, expire_due_snoozes: bool = False) -> dict`
//! - `apply_notification_state_update(path: str, update: dict) -> dict`
//! - `apply_notification_state_update_counts(path: str, update: dict) -> dict`
//! - `append_notification(path: str, notification: dict) -> dict`
//! - `append_notification_counts(path: str, notification: dict) -> dict`
//! - `rewrite_notifications(path: str, notifications: list[dict]) -> dict`
//! - `rewrite_notifications_counts(path: str, notifications: list[dict]) -> dict`
//! - `agent_launch_wire_schema_version() -> int`
//! - `prepare_agent_launch(request: dict, python_executable: str, runner_script: str, output_root: str, sase_tmpdir: str | None = None, preallocated_env: dict | None = None) -> dict`
//! - `spawn_prepared_agent_process(prepared: dict, env: dict, claim_callback: Callable[[int], bool] | None = None) -> int`
//! - `allocate_launch_timestamp_batch(count: int, base_timestamp: str, after_timestamp: str | None = None) -> list[str]`
//! - `plan_agent_launch_fanout(prompt: str, launch_kind: str | None = None) -> dict`
//! - `list_workspace_claims_from_content(content: str) -> list[dict]`
//! - `plan_claim_workspace_from_content(content: str, request: dict) -> dict`
//! - `plan_transfer_workspace_claim_from_content(content: str, request: dict) -> dict`
//! - `allocate_and_claim_workspace_from_content(content: str, min_workspace: int, max_workspace: int, request: dict) -> dict`
//!
//! Dict shapes mirror the Python wire dataclasses in
//! `sase_100/src/sase/core/query_wire.py` (rectangular, all fields always
//! present) so the Python side can rehydrate them with the existing wire
//! converters. The pure `sase_core` crate uses serde's tagged-union shape
//! for `QueryExprWire`; the converters in this file translate between the
//! two so neither side has to bend.
//!
//! `QueryErrorWire` is surfaced as a Python `ValueError` whose message is
//! the wire error's `Display` form so existing UI validation that catches
//! `ValueError` keeps working.

// `pyo3::pyfunction` macro expansion contains a `From::from` for `PyErr`
// that clippy 1.95+ reports as `useless_conversion`. The annotation has
// to live at the module scope because the macro generates wrapper code
// outside the user-written function body.
#![allow(clippy::useless_conversion)]

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyTuple};
use sase_core::agent_archive::{
    agent_archive_facet_counts as core_agent_archive_facet_counts,
    mark_agent_archive_bundles_revived as core_mark_agent_archive_bundles_revived,
    query_agent_archive as core_query_agent_archive,
    verify_agent_archive_index as core_verify_agent_archive_index,
    AgentArchiveFacetRequestWire, AgentArchiveQueryRequestWire,
    AgentArchiveReviveMarkRequestWire,
};
use sase_core::agent_cleanup::{
    cleanup_request_from_json_value,
    delete_agent_artifact_markers as core_delete_agent_artifact_markers,
    mark_comment_agents_as_killed as core_mark_comment_agents_as_killed,
    mark_hook_agents_as_killed as core_mark_hook_agents_as_killed,
    mark_mentor_agents_as_killed as core_mark_mentor_agents_as_killed,
    plan_agent_cleanup as core_plan_agent_cleanup,
    release_workspace_from_content as core_release_workspace_from_content,
    save_dismissed_agents_index as core_save_dismissed_agents_index,
    save_dismissed_bundle_json as core_save_dismissed_bundle_json,
    AgentCleanupIdentityWire, AgentCleanupRequestWire, AgentCleanupTargetWire,
};
use sase_core::agent_launch::{
    allocate_and_claim_workspace_from_content as core_allocate_and_claim_workspace_from_content,
    allocate_launch_timestamp_batch as core_allocate_launch_timestamp_batch,
    list_workspace_claims_from_content as core_list_workspace_claims_from_content,
    plan_agent_launch_fanout as core_plan_agent_launch_fanout,
    plan_claim_workspace_from_content as core_plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content as core_plan_transfer_workspace_claim_from_content,
    prepare_agent_launch as core_prepare_agent_launch, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, WorkspaceClaimRequestWire,
};
use sase_core::agent_scan::{
    delete_agent_artifact_index_row as core_delete_agent_artifact_index_row,
    query_agent_artifact_index as core_query_agent_artifact_index,
    rebuild_agent_artifact_index as core_rebuild_agent_artifact_index,
    scan_agent_artifacts as core_scan_agent_artifacts,
    upsert_agent_artifact_index_row as core_upsert_agent_artifact_index_row,
    AgentArtifactIndexQueryWire, AgentArtifactScanOptionsWire,
};
use sase_core::bead::{
    add_dependency as core_bead_add_dependency,
    blocked_issues as core_bead_blocked_issues,
    build_epic_work_plan as core_bead_build_epic_work_plan,
    build_epic_work_plan_from_issues as core_bead_build_epic_work_plan_from_issues,
    build_legend_work_plan as core_bead_build_legend_work_plan,
    build_legend_work_plan_from_issues as core_bead_build_legend_work_plan_from_issues,
    close_issues as core_bead_close_issues,
    create_issue as core_bead_create_issue, doctor as core_bead_doctor,
    execute_bead_cli as core_execute_bead_cli,
    export_jsonl as core_bead_export_jsonl,
    get_epic_children as core_bead_get_epic_children,
    init_store as core_bead_init_store, list_issues as core_bead_list_issues,
    mark_ready_to_work as core_bead_mark_ready_to_work,
    open_issue as core_bead_open_issue,
    preclaim_epic_work_plan as core_bead_preclaim_epic_work_plan,
    read_event_store_issues as core_bead_read_event_store_issues,
    read_legacy_jsonl_issues as core_bead_read_legacy_jsonl_issues,
    read_store_issues as core_bead_read_store_issues,
    ready_issues as core_bead_ready_issues,
    remove_issue as core_bead_remove_issue, show_issue as core_bead_show_issue,
    stats as core_bead_stats, sync_is_clean as core_bead_sync_is_clean,
    unmark_ready_to_work as core_bead_unmark_ready_to_work,
    update_issue as core_bead_update_issue, BeadCreateRequestWire, BeadError,
    BeadPreclaimAssignmentWire, BeadUpdateFieldsWire, IssueWire,
};
use sase_core::git_query::{
    derive_git_workspace_name as core_derive_git_workspace_name,
    parse_git_branch_name as core_parse_git_branch_name,
    parse_git_conflicted_files as core_parse_git_conflicted_files,
    parse_git_local_changes as core_parse_git_local_changes,
    parse_git_name_status_z as core_parse_git_name_status_z,
};
use sase_core::notifications::{
    append_notification as core_append_notification,
    append_notification_counts as core_append_notification_counts,
    apply_notification_state_update as core_apply_notification_state_update,
    apply_notification_state_update_counts as core_apply_notification_state_update_counts,
    read_notifications_snapshot_with_options as core_read_notifications_snapshot_with_options,
    rewrite_notifications as core_rewrite_notifications,
    rewrite_notifications_counts as core_rewrite_notifications_counts,
    NotificationStateUpdateWire, NotificationWire,
};
use sase_core::query::types::{QueryErrorWire, QueryExprWire};
use sase_core::query::{
    QueryCorpus as CoreQueryCorpus, QueryProgram as CoreQueryProgram,
};
use sase_core::status::{
    apply_status_update as core_apply_status_update,
    is_valid_transition as core_is_valid_transition,
    plan_status_transition as core_plan_status_transition,
    read_status_from_lines as core_read_status_from_lines,
    remove_workspace_suffix as core_remove_workspace_suffix,
    StatusTransitionRequestWire,
};
use sase_core::wire::ChangeSpecWire;
use sase_core::wire::{CommentWire, HookWire, MentorWire};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[pyclass(name = "QueryCorpusHandle", module = "sase_core_rs")]
#[derive(Debug)]
struct PyQueryCorpusHandle {
    corpus: CoreQueryCorpus,
}

#[pymethods]
impl PyQueryCorpusHandle {
    fn __len__(&self) -> usize {
        self.corpus.len()
    }
}

#[pyclass(name = "QueryProgramHandle", module = "sase_core_rs")]
#[derive(Debug)]
struct PyQueryProgramHandle {
    program: CoreQueryProgram,
}

/// Parse a project file's bytes into a `list[dict]` mirroring the
/// `ChangeSpecWire` JSON shape.
///
/// Errors raised by the Rust parser become `ValueError` on the Python
/// side. Encoding errors (non-UTF-8 input) are also surfaced as
/// `ValueError` because the Rust parser models them through
/// `ParseErrorWire { kind: "encoding", ... }`.
#[pyfunction]
#[pyo3(name = "parse_project_bytes")]
fn py_parse_project_bytes<'py>(
    py: Python<'py>,
    path: &str,
    data: &Bound<'py, PyBytes>,
) -> PyResult<Bound<'py, PyList>> {
    let bytes: &[u8] = data.as_bytes();
    let specs = sase_core::parse_project_bytes(path, bytes)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;

    let list = PyList::empty_bound(py);
    for spec in &specs {
        // Going through serde_json::Value keeps the conversion logic in one
        // place and inherits the field declaration order baked into the
        // `ChangeSpecWire` derive. Performance is fine for ChangeSpec-sized
        // documents; if it ever isn't, replace with a direct serde -> Py
        // visitor.
        let value = serde_json::to_value(spec).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
        let py_obj = json_value_to_py(py, &value)?;
        list.append(py_obj)?;
    }
    Ok(list)
}

/// Tokenize a query string. Returns Python `QueryTokenWire`-shape dicts.
///
/// The Rust `QueryTokenWire` already serializes to the exact field set the
/// Python wire dataclass expects (`kind`, `value`, `position`,
/// `case_sensitive`, `property_key`), so the conversion is straight serde.
#[pyfunction]
#[pyo3(name = "tokenize_query")]
fn py_tokenize_query<'py>(
    py: Python<'py>,
    query: &str,
) -> PyResult<Bound<'py, PyList>> {
    let tokens =
        sase_core::tokenize_query(query).map_err(query_error_to_pyerr)?;
    let list = PyList::empty_bound(py);
    for tok in &tokens {
        let value = serde_json::to_value(tok).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
        list.append(json_value_to_py(py, &value)?)?;
    }
    Ok(list)
}

/// Parse a query string into the Python `QueryExprWire`-shape dict.
#[pyfunction]
#[pyo3(name = "parse_query")]
fn py_parse_query<'py>(py: Python<'py>, query: &str) -> PyResult<PyObject> {
    let expr = sase_core::parse_query(query).map_err(query_error_to_pyerr)?;
    let value = expr_to_python_wire(&expr);
    json_value_to_py(py, &value)
}

/// Canonicalize a query string. Mirrors Python's
/// `to_canonical_string(parse_query(...))`.
#[pyfunction]
#[pyo3(name = "canonicalize_query")]
fn py_canonicalize_query(query: &str) -> PyResult<String> {
    let expr = sase_core::parse_query(query).map_err(query_error_to_pyerr)?;
    Ok(sase_core::canonicalize_query(&expr))
}

/// Compile a persistent ChangeSpec corpus from Python wire dicts.
///
/// Python dicts are converted to `ChangeSpecWire` before the GIL is released;
/// the reusable corpus indexes and searchable text are then built without
/// holding the GIL.
#[pyfunction]
#[pyo3(name = "compile_corpus")]
fn py_compile_corpus<'py>(
    py: Python<'py>,
    specs: &Bound<'py, PyList>,
) -> PyResult<PyQueryCorpusHandle> {
    let wire_specs = changespecs_from_py_list(specs)?;
    let corpus = py.allow_threads(|| CoreQueryCorpus::new(wire_specs));
    Ok(PyQueryCorpusHandle { corpus })
}

/// Compile a query into a reusable Rust program handle.
#[pyfunction]
#[pyo3(name = "compile_query")]
fn py_compile_query(query: &str) -> PyResult<PyQueryProgramHandle> {
    let program =
        sase_core::compile_query(query).map_err(query_error_to_pyerr)?;
    Ok(PyQueryProgramHandle { program })
}

/// Evaluate a compiled query against a persistent corpus.
///
/// Evaluation releases the GIL because it only reads the owned Rust handles
/// and returns one boolean per corpus row.
#[pyfunction]
#[pyo3(name = "evaluate_many")]
fn py_evaluate_many<'py>(
    py: Python<'py>,
    program: &PyQueryProgramHandle,
    corpus: &PyQueryCorpusHandle,
) -> PyResult<Bound<'py, PyList>> {
    let results = py.allow_threads(|| {
        sase_core::evaluate_query_many_in_corpus(
            &program.program,
            &corpus.corpus,
        )
    });

    let list = PyList::empty_bound(py);
    for b in results {
        list.append(b)?;
    }
    Ok(list)
}

/// Evaluate a query against a list of `ChangeSpecWire`-shape dicts.
///
/// `specs` must be a `list[dict]` matching the JSON shape of
/// `sase_core::wire::ChangeSpecWire` (i.e. the dicts produced by
/// `sase.core.wire.to_json_dict(changespec_to_wire(cs))`).
///
/// Compiles the query, builds a per-list `QueryEvaluationContext`, and
/// evaluates the program against every spec. Returns `list[bool]` of the
/// same length as `specs`.
#[pyfunction]
#[pyo3(name = "evaluate_query_many")]
fn py_evaluate_query_many<'py>(
    py: Python<'py>,
    query: &str,
    specs: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let wire_specs = changespecs_from_py_list(specs)?;
    let program =
        sase_core::compile_query(query).map_err(query_error_to_pyerr)?;
    let results = py.allow_threads(|| {
        sase_core::evaluate_query_many(&program, &wire_specs)
    });

    let list = PyList::empty_bound(py);
    for b in results {
        list.append(b)?;
    }
    Ok(list)
}

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
    let value = serde_json::to_value(&snapshot).map_err(|e| {
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

/// Plan agent cleanup without executing side effects.
///
/// `targets` is a list of `AgentCleanupTargetWire`-shape dicts gathered by
/// the host. `request` is an `AgentCleanupRequestWire`-shape dict choosing
/// the scope and mode. The returned dict is an `AgentCleanupPlanWire` whose
/// kill/dismiss lists can be previewed or executed by Python.
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

// --- Phase 4C status state machine bindings -------------------------------

/// Strip workspace and legacy READY-TO-MAIL suffixes from a STATUS string.
///
/// Mirrors `sase.status_state_machine.constants.remove_workspace_suffix`.
/// Useful for tests and for callers that want the canonical base status
/// without going through the planner.
#[pyfunction]
#[pyo3(name = "remove_workspace_suffix")]
fn py_remove_workspace_suffix(status: &str) -> String {
    core_remove_workspace_suffix(status)
}

/// Whether a transition from *from_status* to *to_status* is allowed.
///
/// Mirrors `sase.status_state_machine.constants.is_valid_transition`.
/// Workspace suffixes on either side are stripped before validation.
#[pyfunction]
#[pyo3(name = "is_valid_status_transition")]
fn py_is_valid_status_transition(from_status: &str, to_status: &str) -> bool {
    core_is_valid_transition(from_status, to_status)
}

/// Read the STATUS for *changespec_name* from a list of project-file lines.
///
/// Mirrors `sase.status_state_machine.field_updates.read_status_from_lines_python`.
/// Returns `None` when the ChangeSpec is not present.
#[pyfunction]
#[pyo3(name = "read_status_from_lines")]
fn py_read_status_from_lines<'py>(
    py: Python<'py>,
    lines: &Bound<'py, PyList>,
    changespec_name: &str,
) -> PyResult<PyObject> {
    let mut owned: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, item) in lines.iter().enumerate() {
        let s: String = item.extract().map_err(|_| {
            PyValueError::new_err(format!("lines[{idx}] must be a string"))
        })?;
        owned.push(s);
    }
    let result = core_read_status_from_lines(&owned, changespec_name);
    Ok(match result {
        Some(s) => s.into_py(py),
        None => py.None(),
    })
}

/// Apply a STATUS update to a list of project-file lines and return the
/// updated content as a single string.
///
/// Mirrors `sase.status_state_machine.field_updates.apply_status_update_python`.
#[pyfunction]
#[pyo3(name = "apply_status_update")]
fn py_apply_status_update<'py>(
    lines: &Bound<'py, PyList>,
    changespec_name: &str,
    new_status: &str,
) -> PyResult<String> {
    let mut owned: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, item) in lines.iter().enumerate() {
        let s: String = item.extract().map_err(|_| {
            PyValueError::new_err(format!("lines[{idx}] must be a string"))
        })?;
        owned.push(s);
    }
    Ok(core_apply_status_update(
        &owned,
        changespec_name,
        new_status,
    ))
}

/// Plan a status transition for one ChangeSpec.
///
/// *request* must be a `StatusTransitionRequestWire`-shape dict (see
/// `sase.core.status_wire`). The result is a
/// `StatusTransitionPlanWire`-shape dict — the Python adapter rehydrates
/// it via `status_plan_from_dict`.
///
/// Schema-version mismatches and structurally invalid requests surface as
/// `ValueError` so the existing UI validation layer can catch them.
#[pyfunction]
#[pyo3(name = "plan_status_transition")]
fn py_plan_status_transition<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: StatusTransitionRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid StatusTransitionRequestWire dict: {e}"
            ))
        })?;
    let plan =
        core_plan_status_transition(&req).map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Phase 5C Git query parser bindings -----------------------------------

/// Parse the NUL-delimited output of `git diff --name-status -z` into a
/// `list[dict]` mirroring `GitNameStatusEntryWire` JSON shape.
///
/// Mirrors `sase.core.git_query_facade._parse_git_name_status_z_python`.
/// The Python facade rehydrates each dict into a
/// `GitNameStatusEntryWire` via `git_name_status_entry_from_dict` and
/// flattens to `list[tuple[str, str]]` for legacy callers. The dict
/// shape (`{"status": str, "path": str}`) is the same one
/// `git_query_wire_to_json_dict` produces, so no extra translation is
/// required.
#[pyfunction]
#[pyo3(name = "parse_git_name_status_z")]
fn py_parse_git_name_status_z<'py>(
    py: Python<'py>,
    stdout: &str,
) -> PyResult<Bound<'py, PyList>> {
    let entries = core_parse_git_name_status_z(stdout);
    let list = PyList::empty_bound(py);
    for entry in &entries {
        let dict = PyDict::new_bound(py);
        dict.set_item("status", &entry.status)?;
        dict.set_item("path", &entry.path)?;
        list.append(dict)?;
    }
    Ok(list)
}

/// Normalize `git rev-parse --abbrev-ref HEAD` stdout into a branch
/// name. Returns `None` for empty stdout or the detached-HEAD sentinel.
///
/// Mirrors `sase.core.git_query_facade.parse_git_branch_name`.
#[pyfunction]
#[pyo3(name = "parse_git_branch_name")]
fn py_parse_git_branch_name(py: Python<'_>, stdout: &str) -> PyObject {
    match core_parse_git_branch_name(stdout) {
        Some(name) => name.into_py(py),
        None => py.None(),
    }
}

/// Derive a workspace name from a remote URL (preferred) or repository
/// root path. Returns `None` when neither input produces a non-empty
/// name.
///
/// Mirrors `sase.core.git_query_facade.derive_git_workspace_name`.
#[pyfunction]
#[pyo3(name = "derive_git_workspace_name", signature = (remote_url, root_path))]
fn py_derive_git_workspace_name(
    py: Python<'_>,
    remote_url: Option<&str>,
    root_path: Option<&str>,
) -> PyObject {
    match core_derive_git_workspace_name(remote_url, root_path) {
        Some(name) => name.into_py(py),
        None => py.None(),
    }
}

/// Split `git diff --name-only --diff-filter=U` stdout into a
/// `list[str]` of conflicted paths (blank lines dropped, order
/// preserved).
///
/// Mirrors `sase.core.git_query_facade.parse_git_conflicted_files`.
#[pyfunction]
#[pyo3(name = "parse_git_conflicted_files")]
fn py_parse_git_conflicted_files<'py>(
    py: Python<'py>,
    stdout: &str,
) -> PyResult<Bound<'py, PyList>> {
    let paths = core_parse_git_conflicted_files(stdout);
    let list = PyList::empty_bound(py);
    for p in paths {
        list.append(p)?;
    }
    Ok(list)
}

/// Normalize `git status --porcelain` stdout into a clean/dirty signal.
/// Returns `None` for an empty/whitespace-only tree, the stripped text
/// otherwise.
///
/// Mirrors `sase.core.git_query_facade.parse_git_local_changes`.
#[pyfunction]
#[pyo3(name = "parse_git_local_changes")]
fn py_parse_git_local_changes(py: Python<'_>, stdout: &str) -> PyObject {
    match core_parse_git_local_changes(stdout) {
        Some(text) => text.into_py(py),
        None => py.None(),
    }
}

// --- Bead read bindings ---------------------------------------------------

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
#[pyo3(name = "bead_doctor")]
fn py_bead_doctor(beads_dir: &str) -> PyResult<Vec<String>> {
    let beads_dir = PathBuf::from(beads_dir);
    core_bead_doctor(&beads_dir).map_err(bead_error_to_pyerr)
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
#[pyo3(name = "bead_preclaim_epic_work", signature = (beads_dir, epic_id, assignments, now=None))]
fn py_bead_preclaim_epic_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    assignments: &Bound<'py, PyList>,
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
#[pyo3(signature = (beads_dir, issue_ids, reason=None, now=None))]
fn py_bead_close<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
    reason: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_close_issues(&beads_dir, &issue_ids, reason, now)
        }),
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
#[pyo3(name = "bead_build_legend_work_plan")]
fn py_bead_build_legend_work_plan<'py>(
    py: Python<'py>,
    beads_dir: &str,
    legend_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_build_legend_work_plan(&beads_dir, legend_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_build_legend_work_plan_from_issues")]
fn py_bead_build_legend_work_plan_from_issues<'py>(
    py: Python<'py>,
    issues: &Bound<'py, PyList>,
    legend_id: &str,
) -> PyResult<PyObject> {
    let issues = issues_from_py_list(issues)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_build_legend_work_plan_from_issues(issues, legend_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_cli_execute")]
fn py_bead_cli_execute<'py>(
    py: Python<'py>,
    argv: Vec<String>,
    read_beads_dirs: Vec<String>,
    write_beads_dir: &str,
    cwd: &str,
    relativize_design_paths: bool,
) -> PyResult<PyObject> {
    let read_beads_dirs = strings_to_paths(read_beads_dirs);
    let write_beads_dir = PathBuf::from(write_beads_dir);
    let cwd = PathBuf::from(cwd);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_execute_bead_cli(
                &argv,
                &read_beads_dirs,
                &write_beads_dir,
                &cwd,
                relativize_design_paths,
            )
        }),
    )
}

fn strings_to_paths(paths: Vec<String>) -> Vec<PathBuf> {
    paths.into_iter().map(PathBuf::from).collect()
}

fn bead_result_to_py<'py, T>(
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

// --- Notification store bindings -----------------------------------------

/// Read the notification JSONL store and return a snapshot dict.
///
/// The GIL is released while Rust performs filesystem work. When
/// ``expire_due_snoozes`` is true, due snoozes are expired under the same
/// store lock before the returned snapshot is built.
#[pyfunction]
#[pyo3(name = "read_notifications_snapshot", signature = (path, include_dismissed, expire_due_snoozes = false))]
fn py_read_notifications_snapshot<'py>(
    py: Python<'py>,
    path: &str,
    include_dismissed: bool,
    expire_due_snoozes: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| {
        core_read_notifications_snapshot_with_options(
            &path,
            include_dismissed,
            expire_due_snoozes,
        )
    });
    let value = serde_json::to_value(snapshot.map_err(PyValueError::new_err)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

/// Apply one notification state update and return the outcome dict.
#[pyfunction]
#[pyo3(name = "apply_notification_state_update")]
fn py_apply_notification_state_update<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = notification_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py
        .allow_threads(|| core_apply_notification_state_update(&path, &update));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Apply one notification state update and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "apply_notification_state_update_counts")]
fn py_apply_notification_state_update_counts<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = notification_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_apply_notification_state_update_counts(&path, &update)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one notification dict and return the outcome dict.
#[pyfunction]
#[pyo3(name = "append_notification")]
fn py_append_notification<'py>(
    py: Python<'py>,
    path: &str,
    notification: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let notification = notification_from_pydict(notification)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_append_notification(&path, &notification));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one notification dict and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "append_notification_counts")]
fn py_append_notification_counts<'py>(
    py: Python<'py>,
    path: &str,
    notification: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let notification = notification_from_pydict(notification)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_append_notification_counts(&path, &notification)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Rewrite the notification JSONL store from notification dicts.
#[pyfunction]
#[pyo3(name = "rewrite_notifications")]
fn py_rewrite_notifications<'py>(
    py: Python<'py>,
    path: &str,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_rewrite_notifications(&path, &notifications));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Rewrite the notification JSONL store and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "rewrite_notifications_counts")]
fn py_rewrite_notifications_counts<'py>(
    py: Python<'py>,
    path: &str,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_rewrite_notifications_counts(&path, &notifications)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
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

fn notification_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "notification is not a valid NotificationWire dict: {e}"
        ))
    })
}

fn notifications_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<NotificationWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let notification: NotificationWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "notifications[{idx}] is not a valid NotificationWire dict: {e}"
                ))
            })?;
        values.push(notification);
    }
    Ok(values)
}

fn notification_update_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationStateUpdateWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "update is not a valid NotificationStateUpdateWire dict: {e}"
        ))
    })
}

fn query_error_to_pyerr(err: QueryErrorWire) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

fn bead_error_to_pyerr(err: BeadError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

fn changespecs_from_py_list(
    specs: &Bound<'_, PyList>,
) -> PyResult<Vec<ChangeSpecWire>> {
    let mut wire_specs: Vec<ChangeSpecWire> = Vec::with_capacity(specs.len());
    for (idx, item) in specs.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let spec: ChangeSpecWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "specs[{idx}] is not a valid ChangeSpecWire dict: {e}"
                ))
            })?;
        wire_specs.push(spec);
    }
    Ok(wire_specs)
}

/// Convert a `QueryExprWire` into the Python rectangular wire shape.
///
/// Python's `QueryExprWire` always carries the same flat field set
/// regardless of `kind`; serde's tagged-union shape only emits the fields
/// relevant to a given variant. Translating here keeps the Python side
/// unchanged: it can `QueryExprWire(**dict)` directly.
fn expr_to_python_wire(expr: &QueryExprWire) -> JsonValue {
    let (kind, value, case_sensitive, is_es, is_ra, is_rp, prop_key, operands) =
        match expr {
            QueryExprWire::StringMatch {
                value,
                case_sensitive,
                is_error_suffix,
                is_running_agent,
                is_running_process,
            } => (
                "string",
                value.clone(),
                *case_sensitive,
                *is_error_suffix,
                *is_running_agent,
                *is_running_process,
                JsonValue::Null,
                JsonValue::Array(vec![]),
            ),
            QueryExprWire::PropertyMatch { key, value } => (
                "property",
                value.clone(),
                false,
                false,
                false,
                false,
                JsonValue::String(key.clone()),
                JsonValue::Array(vec![]),
            ),
            QueryExprWire::Not { operand } => (
                "not",
                String::new(),
                false,
                false,
                false,
                false,
                JsonValue::Null,
                JsonValue::Array(vec![expr_to_python_wire(operand)]),
            ),
            QueryExprWire::And { operands } => (
                "and",
                String::new(),
                false,
                false,
                false,
                false,
                JsonValue::Null,
                JsonValue::Array(
                    operands.iter().map(expr_to_python_wire).collect(),
                ),
            ),
            QueryExprWire::Or { operands } => (
                "or",
                String::new(),
                false,
                false,
                false,
                false,
                JsonValue::Null,
                JsonValue::Array(
                    operands.iter().map(expr_to_python_wire).collect(),
                ),
            ),
        };

    let mut obj = JsonMap::new();
    obj.insert("kind".into(), JsonValue::String(kind.into()));
    obj.insert("value".into(), JsonValue::String(value));
    obj.insert("case_sensitive".into(), JsonValue::Bool(case_sensitive));
    obj.insert("is_error_suffix".into(), JsonValue::Bool(is_es));
    obj.insert("is_running_agent".into(), JsonValue::Bool(is_ra));
    obj.insert("is_running_process".into(), JsonValue::Bool(is_rp));
    obj.insert("property_key".into(), prop_key);
    obj.insert("operands".into(), operands);
    JsonValue::Object(obj)
}

/// Convert a Python value (dict / list / str / number / bool / None) into
/// a `serde_json::Value`. Used to deserialize ChangeSpecWire dicts coming
/// in from the Python side of `evaluate_query_many`.
fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(JsonValue::Number(i.into()));
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(JsonValue::Number(u.into()));
    }
    if let Ok(f) = value.extract::<f64>() {
        return serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                PyValueError::new_err(format!("non-finite float: {f}"))
            });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_json_value(&item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            arr.push(py_to_json_value(&item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut obj = JsonMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k.extract().map_err(|_| {
                PyValueError::new_err("dict keys must be strings")
            })?;
            obj.insert(key, py_to_json_value(&v)?);
        }
        return Ok(JsonValue::Object(obj));
    }
    Err(PyValueError::new_err(format!(
        "unsupported value of type {}",
        value.get_type().name()?
    )))
}

fn strings_from_py_list(
    list: &Bound<'_, PyList>,
    label: &str,
) -> PyResult<Vec<String>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(item.extract::<String>().map_err(|_| {
            PyValueError::new_err(format!("{label}[{idx}] must be a string"))
        })?);
    }
    Ok(values)
}

fn hooks_from_py(list: &Bound<'_, PyList>) -> PyResult<Vec<HookWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "hooks[{idx}] is not a valid HookWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

fn mentors_from_py(list: &Bound<'_, PyList>) -> PyResult<Vec<MentorWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "mentors[{idx}] is not a valid MentorWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

fn comments_from_py(list: &Bound<'_, PyList>) -> PyResult<Vec<CommentWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "comments[{idx}] is not a valid CommentWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

fn json_value_to_py<'py>(
    py: Python<'py>,
    value: &JsonValue,
) -> PyResult<PyObject> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_py(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py(py))
            } else {
                // Should be unreachable for serde_json numbers.
                Err(PyValueError::new_err(format!(
                    "unrepresentable JSON number: {n}"
                )))
            }
        }
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(arr) => json_array_to_py(py, arr),
        JsonValue::Object(obj) => json_object_to_py(py, obj),
    }
}

fn json_array_to_py<'py>(
    py: Python<'py>,
    arr: &[JsonValue],
) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for v in arr {
        list.append(json_value_to_py(py, v)?)?;
    }
    Ok(list.into())
}

fn json_object_to_py<'py>(
    py: Python<'py>,
    obj: &JsonMap<String, JsonValue>,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    for (k, v) in obj {
        dict.set_item(k, json_value_to_py(py, v)?)?;
    }
    Ok(dict.into())
}

/// Return the launch wire schema version pinned by the Rust skeleton structs.
#[pyfunction]
#[pyo3(name = "agent_launch_wire_schema_version")]
fn py_agent_launch_wire_schema_version() -> u32 {
    sase_core::AGENT_LAUNCH_WIRE_SCHEMA_VERSION
}

/// Write the launch prompt temp file and return prepared process data.
#[pyfunction]
#[pyo3(name = "prepare_agent_launch")]
#[pyo3(signature = (
    request,
    python_executable,
    runner_script,
    output_root,
    sase_tmpdir = None,
    preallocated_env = None
))]
fn py_prepare_agent_launch<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
    python_executable: &str,
    runner_script: &str,
    output_root: &str,
    sase_tmpdir: Option<&str>,
    preallocated_env: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let req = agent_launch_request_from_pydict(request)?;
    let preallocated = match preallocated_env {
        Some(env) => env_dict_from_pydict(env)?,
        None => std::collections::BTreeMap::new(),
    };
    let prepared = core_prepare_agent_launch(
        &req,
        python_executable,
        runner_script,
        sase_tmpdir,
        output_root,
        &preallocated,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&prepared).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Spawn a prepared detached agent process and run optional claim callback.
#[pyfunction]
#[pyo3(name = "spawn_prepared_agent_process")]
#[pyo3(signature = (prepared, env, claim_callback = None))]
fn py_spawn_prepared_agent_process(
    py: Python<'_>,
    prepared: &Bound<'_, PyDict>,
    env: &Bound<'_, PyDict>,
    claim_callback: Option<&Bound<'_, PyAny>>,
) -> PyResult<u32> {
    let prepared = agent_launch_prepared_from_pydict(prepared)?;
    let env = env_dict_from_pydict(env)?;
    let mut child = py
        .allow_threads(move || spawn_prepared_detached_process(prepared, env))
        .map_err(PyRuntimeError::new_err)?;
    let pid = child.id();

    if let Some(callback) = claim_callback {
        match callback.call1((pid,)) {
            Ok(value) => {
                let success = value.extract::<bool>().map_err(|err| {
                    PyValueError::new_err(format!(
                        "claim_callback must return bool, got invalid value: {err}"
                    ))
                })?;
                if !success {
                    terminate_child_after_claim_failure(&mut child);
                    return Err(PyRuntimeError::new_err(
                        "agent launch claim callback reported failure",
                    ));
                }
            }
            Err(err) => {
                terminate_child_after_claim_failure(&mut child);
                return Err(err);
            }
        }
    }

    Ok(pid)
}

/// Allocate unique launch timestamps from a base YYmmdd_HHMMSS timestamp.
#[pyfunction]
#[pyo3(name = "allocate_launch_timestamp_batch")]
#[pyo3(signature = (count, base_timestamp, after_timestamp = None))]
fn py_allocate_launch_timestamp_batch<'py>(
    py: Python<'py>,
    count: usize,
    base_timestamp: &str,
    after_timestamp: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    let timestamps = core_allocate_launch_timestamp_batch(
        count,
        base_timestamp,
        after_timestamp,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let list = PyList::empty_bound(py);
    for timestamp in timestamps {
        list.append(timestamp)?;
    }
    Ok(list)
}

/// Plan deterministic prompt fan-out without launching child agents.
#[pyfunction]
#[pyo3(name = "plan_agent_launch_fanout")]
#[pyo3(signature = (prompt, launch_kind = None))]
fn py_plan_agent_launch_fanout<'py>(
    py: Python<'py>,
    prompt: &str,
    launch_kind: Option<&str>,
) -> PyResult<PyObject> {
    let plan = core_plan_agent_launch_fanout(prompt, launch_kind)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return parsed RUNNING workspace claims from project-file content.
#[pyfunction]
#[pyo3(name = "list_workspace_claims_from_content")]
fn py_list_workspace_claims_from_content<'py>(
    py: Python<'py>,
    content: &str,
) -> PyResult<PyObject> {
    let claims = core_list_workspace_claims_from_content(content);
    let value = serde_json::to_value(&claims).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan insertion of one RUNNING workspace claim.
#[pyfunction]
#[pyo3(name = "plan_claim_workspace_from_content")]
fn py_plan_claim_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_plan_claim_workspace_from_content(content, &req);
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan transfer of one RUNNING workspace claim to a new PID.
#[pyfunction]
#[pyo3(name = "plan_transfer_workspace_claim_from_content")]
fn py_plan_transfer_workspace_claim_from_content<'py>(
    py: Python<'py>,
    content: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_plan_transfer_workspace_claim_from_content(content, &req);
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan first-free workspace allocation and RUNNING claim insertion together.
#[pyfunction]
#[pyo3(name = "allocate_and_claim_workspace_from_content")]
fn py_allocate_and_claim_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_allocate_and_claim_workspace_from_content(
        content,
        min_workspace,
        max_workspace,
        &req,
    );
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn spawn_prepared_detached_process(
    prepared: AgentLaunchPreparedWire,
    env: BTreeMap<String, String>,
) -> Result<Child, String> {
    let Some((program, args)) = prepared.argv.split_first() else {
        return Err("prepared launch argv must not be empty".to_string());
    };

    let stdout_file = File::create(&prepared.output_path).map_err(|err| {
        format!(
            "failed to open launch output file {}: {err}",
            prepared.output_path
        )
    })?;
    let stderr_file = stdout_file.try_clone().map_err(|err| {
        format!(
            "failed to clone launch output file {} for stderr: {err}",
            prepared.output_path
        )
    })?;

    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&prepared.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .env_clear()
        .envs(env);

    configure_detached_process(&mut command);

    command.spawn().map_err(|err| {
        format!(
            "failed to spawn prepared agent process in cwd {}: {err}",
            prepared.cwd
        )
    })
}

#[cfg(unix)]
fn configure_detached_process(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // Match Python's subprocess.Popen(start_new_session=True) behavior.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(windows)]
fn configure_detached_process(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn configure_detached_process(_command: &mut Command) {}

fn terminate_child_after_claim_failure(child: &mut Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }

    terminate_child_gracefully(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn terminate_child_gracefully(child: &Child) {
    let _ = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
}

#[cfg(not(unix))]
fn terminate_child_gracefully(child: &mut Child) {
    let _ = child.kill();
}

fn workspace_claim_request_from_pydict(
    request: &Bound<'_, PyDict>,
) -> PyResult<WorkspaceClaimRequestWire> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid WorkspaceClaimRequestWire dict: {e}"
        ))
    })
}

fn agent_launch_request_from_pydict(
    request: &Bound<'_, PyDict>,
) -> PyResult<AgentLaunchRequestWire> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentLaunchRequestWire dict: {e}"
        ))
    })
}

fn agent_launch_prepared_from_pydict(
    prepared: &Bound<'_, PyDict>,
) -> PyResult<AgentLaunchPreparedWire> {
    let value = py_to_json_value(prepared.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "prepared is not a valid AgentLaunchPreparedWire dict: {e}"
        ))
    })
}

fn env_dict_from_pydict(
    env: &Bound<'_, PyDict>,
) -> PyResult<std::collections::BTreeMap<String, String>> {
    let value = py_to_json_value(env.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "preallocated_env is not a valid string dict: {e}"
        ))
    })
}

#[pymodule]
#[pyo3(name = "sase_core_rs")]
fn sase_core_rs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyQueryCorpusHandle>()?;
    m.add_class::<PyQueryProgramHandle>()?;
    m.add_function(wrap_pyfunction!(py_parse_project_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_tokenize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_corpus, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_query_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_agent_artifacts, m)?)?;
    m.add_function(wrap_pyfunction!(py_rebuild_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_upsert_agent_artifact_index_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_agent_artifact_index_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_archive, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_archive_facet_counts, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_agent_archive_bundles_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_verify_agent_archive_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_cleanup, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_agents_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_agent_artifacts, m)?)?;
    m.add_function(wrap_pyfunction!(py_release_workspace_from_content, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_hook_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_mentor_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_comment_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_remove_workspace_suffix, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_valid_status_transition, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_status_from_lines, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_status_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_status_transition, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_name_status_z, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_branch_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_derive_git_workspace_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_conflicted_files, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_local_changes, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_event_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_legacy_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_show, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_ready, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_blocked, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_stats, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_doctor, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_get_epic_children, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_init_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_create, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_preclaim_epic_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_open, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_close, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_dep_add, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_mark_ready_to_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_unmark_ready_to_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_export_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_sync_is_clean, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_build_epic_work_plan, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_build_epic_work_plan_from_issues,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_build_legend_work_plan, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_build_legend_work_plan_from_issues,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_cli_execute, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_notifications_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_notification_state_update, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_apply_notification_state_update_counts,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_append_notification, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_notification_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_notifications, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_notifications_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_launch_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_prepare_agent_launch, m)?)?;
    m.add_function(wrap_pyfunction!(py_spawn_prepared_agent_process, m)?)?;
    m.add_function(wrap_pyfunction!(py_allocate_launch_timestamp_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_launch_fanout, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_list_workspace_claims_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_plan_claim_workspace_from_content, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_plan_transfer_workspace_claim_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_allocate_and_claim_workspace_from_content,
        m
    )?)?;
    Ok(())
}

pub use sase_core as core;

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn append_json<'py>(
        py: Python<'py>,
        list: &Bound<'py, PyList>,
        value: JsonValue,
    ) {
        list.append(json_value_to_py(py, &value).unwrap()).unwrap();
    }

    fn spec_json(name: &str, status: &str, parent: Option<&str>) -> JsonValue {
        json!({
            "schema_version": 2,
            "name": name,
            "project_basename": "proj",
            "file_path": "proj.sase",
            "source_span": {
                "file_path": "proj.sase",
                "start_line": 1,
                "end_line": 10
            },
            "status": status,
            "parent": parent,
            "cl_or_pr": null,
            "bug": null,
            "description": format!("description for {name}"),
            "commits": [],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        })
    }

    fn spec_list<'py>(
        py: Python<'py>,
        specs: &[JsonValue],
    ) -> Bound<'py, PyList> {
        let list = PyList::empty_bound(py);
        for spec in specs {
            append_json(py, &list, spec.clone());
        }
        list
    }

    fn bools_from_py_list(list: &Bound<'_, PyList>) -> Vec<bool> {
        list.iter()
            .map(|item| item.extract::<bool>().unwrap())
            .collect()
    }

    fn query_module<'py>(py: Python<'py>) -> Bound<'py, PyModule> {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module.add_class::<PyQueryCorpusHandle>().unwrap();
        module.add_class::<PyQueryProgramHandle>().unwrap();
        module
            .add_function(wrap_pyfunction!(py_evaluate_many, &module).unwrap())
            .unwrap();
        module
    }

    fn temp_notification_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "sase-core-py-notification-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn query_handles_evaluate_multiple_queries_against_one_corpus() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let specs = spec_list(
                py,
                &[
                    spec_json("alpha", "WIP", None),
                    spec_json("beta", "Submitted", Some("alpha")),
                    spec_json("gamma", "WIP", Some("beta")),
                ],
            );
            let corpus = py_compile_corpus(py, &specs).unwrap();

            let alpha = py_compile_query("name:alpha").unwrap();
            let alpha_results = py_evaluate_many(py, &alpha, &corpus).unwrap();
            assert_eq!(
                bools_from_py_list(&alpha_results),
                vec![true, false, false]
            );

            let ancestor = py_compile_query("ancestor:alpha").unwrap();
            let ancestor_results =
                py_evaluate_many(py, &ancestor, &corpus).unwrap();
            assert_eq!(
                bools_from_py_list(&ancestor_results),
                vec![true, true, true]
            );
            assert_eq!(corpus.__len__(), 3);
        });
    }

    #[test]
    fn query_handles_evaluate_one_query_against_multiple_corpora() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let program = py_compile_query("status:wip").unwrap();

            let first = spec_list(
                py,
                &[
                    spec_json("alpha", "WIP", None),
                    spec_json("beta", "Submitted", None),
                ],
            );
            let first_corpus = py_compile_corpus(py, &first).unwrap();
            let first_results =
                py_evaluate_many(py, &program, &first_corpus).unwrap();
            assert_eq!(bools_from_py_list(&first_results), vec![true, false]);

            let second = spec_list(
                py,
                &[
                    spec_json("gamma", "Submitted", None),
                    spec_json("delta", "WIP", None),
                ],
            );
            let second_corpus = py_compile_corpus(py, &second).unwrap();
            let second_results =
                py_evaluate_many(py, &program, &second_corpus).unwrap();
            assert_eq!(bools_from_py_list(&second_results), vec![false, true]);
        });
    }

    #[test]
    fn query_handles_match_legacy_one_shot_results() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let specs = spec_list(
                py,
                &[
                    spec_json("alpha", "WIP", None),
                    spec_json("beta", "Submitted", Some("alpha")),
                    spec_json("gamma", "WIP", Some("beta")),
                ],
            );
            let corpus = py_compile_corpus(py, &specs).unwrap();

            for query in ["alpha", "status:wip", "ancestor:alpha"] {
                let program = py_compile_query(query).unwrap();
                let handle_results =
                    py_evaluate_many(py, &program, &corpus).unwrap();
                let legacy_results =
                    py_evaluate_query_many(py, query, &specs).unwrap();
                assert_eq!(
                    bools_from_py_list(&handle_results),
                    bools_from_py_list(&legacy_results),
                    "query {query}"
                );
            }
        });
    }

    #[test]
    fn query_compile_errors_are_python_value_errors() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|_py| {
            let err = py_compile_query("").unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(_py));
            assert!(err.to_string().contains("Empty query"));
        });
    }

    #[test]
    fn query_handle_bindings_reject_wrong_handle_types() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = query_module(py);
            let specs = spec_list(py, &[spec_json("alpha", "WIP", None)]);
            let corpus =
                Py::new(py, py_compile_corpus(py, &specs).unwrap()).unwrap();
            let program =
                Py::new(py, py_compile_query("alpha").unwrap()).unwrap();
            let bad = PyDict::new_bound(py);
            let evaluate_many = module.getattr("evaluate_many").unwrap();

            let err = evaluate_many
                .call1((bad.clone(), corpus.clone_ref(py)))
                .unwrap_err();
            assert!(err.to_string().contains("QueryProgramHandle"));

            let err = evaluate_many.call1((program, bad)).unwrap_err();
            assert!(err.to_string().contains("QueryCorpusHandle"));
        });
    }

    #[test]
    fn plan_agent_cleanup_binding_round_trips_json_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let targets = PyList::empty_bound(py);
            append_json(
                py,
                &targets,
                json!({
                    "identity": {"agent_type": "run", "cl_name": "done", "raw_suffix": "1"},
                    "agent_type": "run",
                    "status": "DONE",
                    "pid": null,
                    "workflow": null,
                    "parent_workflow": null,
                    "parent_timestamp": null,
                    "raw_suffix": "1",
                    "project_file": "/tmp/project.sase",
                    "artifacts_dir": "/tmp/artifacts",
                    "workspace": null,
                    "tag": null,
                    "agent_name": "done",
                    "display_name": "done",
                    "start_time": null,
                    "stop_time": null,
                    "is_workflow_child": false,
                    "appears_as_agent": false,
                    "step_type": null
                }),
            );
            let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "scope": "all_panels",
                    "mode": "dismiss_completed",
                    "focused_panel_tag": null,
                    "tag": null,
                    "identities": [],
                    "include_pidless_as_dismissable": false
                }),
            )
            .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

            let result = py_plan_agent_cleanup(py, &targets, request).unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();

            assert_eq!(value["schema_version"], json!(1));
            assert_eq!(
                value["dismiss_items"][0]["identity"]["cl_name"],
                json!("done")
            );
            assert_eq!(value["kill_items"], json!([]));
            assert_eq!(value["confirmation_severity"], json!("dismiss"));
        });
    }

    #[test]
    fn plan_agent_cleanup_binding_rejects_schema_mismatch() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let targets = PyList::empty_bound(py);
            let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 999,
                    "scope": "all_panels",
                    "mode": "dismiss_completed"
                }),
            )
            .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

            let err = py_plan_agent_cleanup(py, &targets, request).unwrap_err();
            assert!(err.to_string().contains("schema mismatch"));
        });
    }

    #[test]
    fn notification_store_binding_round_trips_json_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let path = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "axe",
                    "notes": ["hello"],
                    "files": [],
                    "action": "PlanApproval",
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();

            let appended = py_append_notification(
                py,
                path.to_str().unwrap(),
                notification,
            )
            .unwrap();
            let appended_value = py_to_json_value(appended.bind(py)).unwrap();
            assert_eq!(appended_value["appended_count"], json!(1));

            let snapshot = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                false,
                false,
            )
            .unwrap();
            let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot_value["schema_version"], json!(1));
            assert_eq!(snapshot_value["notifications"][0]["id"], json!("n1"));
            assert_eq!(snapshot_value["counts"]["priority"], json!(1));

            let update_obj =
                json_value_to_py(py, &json!({"kind": "mark_read", "id": "n1"}))
                    .unwrap();
            let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
            let outcome = py_apply_notification_state_update(
                py,
                path.to_str().unwrap(),
                update,
            )
            .unwrap();
            let outcome_value = py_to_json_value(outcome.bind(py)).unwrap();
            assert_eq!(outcome_value["matched_count"], json!(1));
            assert_eq!(outcome_value["changed_count"], json!(1));

            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(
                path.with_file_name("notifications.jsonl.lock"),
            );
            let _ = fs::remove_dir(path.parent().unwrap());
        });
    }

    #[test]
    fn notification_store_binding_rejects_bad_update_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let bad_obj = json_value_to_py(
                py,
                &json!({"kind": "mark_snoozed", "id": "n1"}),
            )
            .unwrap();
            let bad = bad_obj.bind(py).downcast::<PyDict>().unwrap();

            let err = py_apply_notification_state_update(
                py,
                "/tmp/notifications.jsonl",
                bad,
            )
            .unwrap_err();
            assert!(err
                .to_string()
                .contains("NotificationStateUpdateWire dict"));
        });
    }

    #[test]
    fn notification_store_counts_binding_omits_rows_and_persists() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let path = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "axe",
                    "notes": [],
                    "files": [],
                    "action": null,
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();
            py_append_notification(py, path.to_str().unwrap(), notification)
                .unwrap();

            let update_obj =
                json_value_to_py(py, &json!({"kind": "mark_read", "id": "n1"}))
                    .unwrap();
            let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
            let outcome = py_apply_notification_state_update_counts(
                py,
                path.to_str().unwrap(),
                update,
            )
            .unwrap();
            let outcome_value = py_to_json_value(outcome.bind(py)).unwrap();
            assert_eq!(outcome_value["matched_count"], json!(1));
            assert_eq!(outcome_value["changed_count"], json!(1));
            assert_eq!(outcome_value["notifications"], json!([]));
            assert_eq!(outcome_value["counts"]["priority"], json!(0));
            assert_eq!(outcome_value["stats"]["loaded_rows"], json!(0));

            let snapshot = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                true,
                false,
            )
            .unwrap();
            let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot_value["notifications"][0]["read"], json!(true));

            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(
                path.with_file_name("notifications.jsonl.lock"),
            );
            let _ = fs::remove_dir(path.parent().unwrap());
        });
    }

    #[test]
    fn notification_store_append_and_rewrite_counts_bindings_omit_rows() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let path = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "axe",
                    "notes": [],
                    "files": [],
                    "action": null,
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();

            let appended = py_append_notification_counts(
                py,
                path.to_str().unwrap(),
                notification,
            )
            .unwrap();
            let appended_value = py_to_json_value(appended.bind(py)).unwrap();
            assert_eq!(appended_value["appended_count"], json!(1));
            assert_eq!(appended_value["matched_count"], json!(0));
            assert_eq!(appended_value["changed_count"], json!(0));
            assert_eq!(appended_value["rewritten"], json!(false));
            assert_eq!(appended_value["notifications"], json!([]));
            assert_eq!(appended_value["counts"]["priority"], json!(0));
            assert_eq!(appended_value["stats"]["loaded_rows"], json!(0));

            let snapshot = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                true,
                false,
            )
            .unwrap();
            let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot_value["notifications"][0]["id"], json!("n1"));

            let replacement_obj = json_value_to_py(
                py,
                &json!([{
                    "id": "n2",
                    "timestamp": "2026-04-30T13:00:00+00:00",
                    "sender": "axe",
                    "notes": [],
                    "files": [],
                    "action": null,
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }]),
            )
            .unwrap();
            let replacement =
                replacement_obj.bind(py).downcast::<PyList>().unwrap();

            let rewritten = py_rewrite_notifications_counts(
                py,
                path.to_str().unwrap(),
                replacement,
            )
            .unwrap();
            let rewritten_value = py_to_json_value(rewritten.bind(py)).unwrap();
            assert_eq!(rewritten_value["matched_count"], json!(1));
            assert_eq!(rewritten_value["changed_count"], json!(1));
            assert_eq!(rewritten_value["appended_count"], json!(0));
            assert_eq!(rewritten_value["rewritten"], json!(true));
            assert_eq!(rewritten_value["notifications"], json!([]));

            let after = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                true,
                false,
            )
            .unwrap();
            let after_value = py_to_json_value(after.bind(py)).unwrap();
            assert_eq!(after_value["notifications"][0]["id"], json!("n2"));
            assert_eq!(
                after_value["notifications"].as_array().unwrap().len(),
                1
            );

            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(
                path.with_file_name("notifications.jsonl.lock"),
            );
            let _ = fs::remove_dir(path.parent().unwrap());
        });
    }
}
