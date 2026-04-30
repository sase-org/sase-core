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
//! - `evaluate_query_many(query: str, specs: list[dict]) -> list[bool]`
//! - `scan_agent_artifacts(projects_root: str, options: dict | None = None) -> dict`
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

use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
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
use sase_core::agent_scan::{
    scan_agent_artifacts as core_scan_agent_artifacts,
    AgentArtifactScanOptionsWire,
};
use sase_core::git_query::{
    derive_git_workspace_name as core_derive_git_workspace_name,
    parse_git_branch_name as core_parse_git_branch_name,
    parse_git_conflicted_files as core_parse_git_conflicted_files,
    parse_git_local_changes as core_parse_git_local_changes,
    parse_git_name_status_z as core_parse_git_name_status_z,
};
use sase_core::query::types::{QueryErrorWire, QueryExprWire};
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
    let program =
        sase_core::compile_query(query).map_err(query_error_to_pyerr)?;
    let results = sase_core::evaluate_query_many(&program, &wire_specs);

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

fn query_error_to_pyerr(err: QueryErrorWire) -> PyErr {
    PyValueError::new_err(format!("{err}"))
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

#[pymodule]
#[pyo3(name = "sase_core_rs")]
fn sase_core_rs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_parse_project_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_tokenize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_query_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_agent_artifacts, m)?)?;
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
    Ok(())
}

pub use sase_core as core;

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;
    use serde_json::json;

    fn append_json<'py>(
        py: Python<'py>,
        list: &Bound<'py, PyList>,
        value: JsonValue,
    ) {
        list.append(json_value_to_py(py, &value).unwrap()).unwrap();
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
                    "project_file": "/tmp/project.gp",
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
}
