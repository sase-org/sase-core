//! Version-control bindings: logs, git queries, commits, and publication.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use crate::provider_policy::provider_priority_dict_from_py;

use pyo3::wrap_pyfunction;

fn git_object_sharing_error_to_pyerr(error: GitObjectSharingError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "git_object_sharing_wire_schema_version")]
fn py_git_object_sharing_wire_schema_version() -> u32 {
    GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "plan_git_object_sharing")]
fn py_plan_git_object_sharing<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: GitObjectSharingPlanRequestWire = serde_json::from_value(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid GitObjectSharingPlanRequestWire dict: {error}"
        ))
    })?;
    let result = core_plan_git_object_sharing(&request)
        .map_err(git_object_sharing_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "pending_commit_checkpoint_wire_schema_version")]
fn py_pending_commit_checkpoint_wire_schema_version() -> u32 {
    PENDING_COMMIT_CHECKPOINT_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "decide_pending_commit_checkpoint_recovery")]
fn py_decide_pending_commit_checkpoint_recovery<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: PendingCommitCheckpointRequestWire =
        provider_priority_dict_from_py(
            request.as_any(),
            "pending commit checkpoint request",
        )?;
    let decision = core_decide_pending_commit_checkpoint_recovery(&request);
    serialize_to_py(py, &decision)
}

#[pyfunction]
#[pyo3(name = "commit_shas_equivalent")]
pub(crate) fn py_commit_shas_equivalent(left: &str, right: &str) -> bool {
    core_commit_shas_equivalent(left, right)
}

/// Return the schema version for commit-footer binding payloads.
#[pyfunction]
#[pyo3(name = "commit_footer_wire_schema_version")]
fn py_commit_footer_wire_schema_version() -> u32 {
    COMMIT_FOOTER_WIRE_SCHEMA_VERSION
}

/// Parse a terminal SASE commit footer into its structured wire payload.
#[pyfunction]
#[pyo3(name = "parse_commit_footer")]
fn py_parse_commit_footer<'py>(
    py: Python<'py>,
    message: &str,
) -> PyResult<PyObject> {
    let footer = core_parse_commit_footer(message);
    let value = serde_json::to_value(&footer).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Update a terminal SASE commit footer from typed update payloads.
#[pyfunction]
#[pyo3(name = "update_commit_footer")]
fn py_update_commit_footer(
    message: &str,
    updates: &Bound<'_, PyList>,
    remove_keys: Vec<String>,
) -> PyResult<String> {
    let mut wire_updates = Vec::with_capacity(updates.len());
    for (index, item) in updates.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let update: CommitFooterUpdateWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "updates[{index}] is not a valid CommitFooterUpdateWire dict: {e}"
                ))
            })?;
        wire_updates.push(update);
    }
    Ok(core_update_commit_footer(
        message,
        &wire_updates,
        &remove_keys,
    ))
}

/// Return the schema version for commit-subject binding payloads.
#[pyfunction]
#[pyo3(name = "commit_subject_wire_schema_version")]
fn py_commit_subject_wire_schema_version() -> u32 {
    COMMIT_SUBJECT_WIRE_SCHEMA_VERSION
}

/// Return the default accepted Conventional Commit types.
#[pyfunction]
#[pyo3(name = "default_commit_subject_types")]
fn py_default_commit_subject_types() -> Vec<String> {
    core_default_commit_subject_types()
}

/// Parse a Conventional Commit subject into its structured wire payload.
#[pyfunction]
#[pyo3(name = "parse_commit_subject")]
fn py_parse_commit_subject<'py>(
    py: Python<'py>,
    message: &str,
    allowed_types: Vec<String>,
) -> PyResult<PyObject> {
    let subject = core_parse_commit_subject(message, &allowed_types);
    let value = serde_json::to_value(&subject).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Canonicalize a pull-request URL into the external-PR mirror identity dict.
#[pyfunction]
#[pyo3(name = "canonical_pull_request_url")]
fn py_canonical_pull_request_url<'py>(
    py: Python<'py>,
    url: &str,
) -> PyResult<Option<PyObject>> {
    let Some(parsed) = core_canonical_pull_request_url(url) else {
        return Ok(None);
    };
    let value = serde_json::to_value(&parsed).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value).map(Some)
}

/// Plan reconciliation of one remote PR against local Patch records.
#[pyfunction]
#[pyo3(name = "plan_external_pr_import")]
fn py_plan_external_pr_import<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ExternalPrImportRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid ExternalPrImportRequestWire dict: {e}"
            ))
        })?;
    let plan =
        core_plan_external_pr_import(&req).map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the repository-resolution wire schema version.
#[pyfunction]
#[pyo3(name = "repository_resolution_wire_schema_version")]
fn py_repository_resolution_wire_schema_version() -> u32 {
    core_repository_resolution_wire_schema_version()
}

/// Canonicalize a supported repository identity, if one is present.
#[pyfunction]
#[pyo3(name = "canonical_repository_identity")]
fn py_canonical_repository_identity<'py>(
    py: Python<'py>,
    value: &str,
) -> PyResult<Option<PyObject>> {
    let Some(identity) = core_canonical_repository_identity(value) else {
        return Ok(None);
    };
    serialize_to_py(py, &identity).map(Some)
}

/// Resolve a requested repository reference against configured candidates.
#[pyfunction]
#[pyo3(name = "resolve_repository_reference")]
fn py_resolve_repository_reference<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: RepositoryResolutionRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid RepositoryResolutionRequestWire dict: {e}"
            ))
        })?;
    let decision = core_resolve_repository_reference(&req);
    serialize_to_py(py, &decision)
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

// --- GitHub transport retryability bindings -------------------------------
#[pyfunction]
#[pyo3(name = "retryability_wire_schema_version")]
fn py_retryability_wire_schema_version() -> u32 {
    core_retryability_wire_schema_version()
}

/// Classify observed git/gh process output into a retryability verdict.
#[pyfunction]
#[pyo3(
    name = "classify_failure_retryability",
    signature = (operation_kind, exit_status=None, stdout="", stderr="")
)]
fn py_classify_failure_retryability<'py>(
    py: Python<'py>,
    operation_kind: &str,
    exit_status: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let observation = FailureObservationWire {
        operation_kind: operation_kind.to_string(),
        exit_status,
        stdout: stdout.to_string(),
        stderr: stderr.to_string(),
    };
    let verdict = core_classify_failure_retryability(&observation);
    retryability_verdict_to_py(py, &verdict)
}

fn retryability_verdict_to_py<'py>(
    py: Python<'py>,
    verdict: &RetryabilityVerdictWire,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("schema_version", verdict.schema_version)?;
    dict.set_item("verdict", &verdict.verdict)?;
    dict.set_item("reason", &verdict.reason)?;
    dict.set_item("retryable", verdict.retryable)?;
    dict.set_item("retry_after_seconds", verdict.retry_after_seconds)?;
    Ok(dict)
}

/// Decide the next launch-time sidecar publication action after one push.
#[pyfunction]
#[pyo3(name = "decide_sidecar_publication_after_push")]
fn py_decide_sidecar_publication_after_push<'py>(
    py: Python<'py>,
    returncode: i32,
    stdout: &str,
    stderr: &str,
    attempt: u32,
) -> PyResult<Bound<'py, PyDict>> {
    if attempt == 0 {
        return Err(PyValueError::new_err("attempt must be at least 1"));
    }
    let decision = core_decide_sidecar_publication_after_push(
        returncode, stdout, stderr, attempt,
    );
    sidecar_publication_decision_to_py(py, &decision)
}

fn sidecar_publication_decision_to_py<'py>(
    py: Python<'py>,
    decision: &SidecarPublicationDecisionWire,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("schema_version", decision.schema_version)?;
    dict.set_item("action", &decision.action)?;
    dict.set_item("classification", &decision.classification)?;
    dict.set_item("reason", &decision.reason)?;
    dict.set_item("attempt", decision.attempt)?;
    dict.set_item("max_attempts", decision.max_attempts)?;
    dict.set_item("retryable", decision.retryable)?;
    Ok(dict)
}

// --- vcs_log parser + aggregator bindings --------------------------------
/// Serialize a `VcsCommitWire` into a `PyDict` mirroring the Python
/// `VcsCommitWire` dataclass JSON shape.
fn vcs_commit_wire_to_py<'py>(
    py: Python<'py>,
    commit: &VcsCommitWire,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("full_id", &commit.full_id)?;
    dict.set_item("short_id", &commit.short_id)?;
    dict.set_item("author_name", &commit.author_name)?;
    dict.set_item("author_email", &commit.author_email)?;
    dict.set_item("timestamp", commit.timestamp)?;
    dict.set_item("parent_ids", PyList::new_bound(py, &commit.parent_ids))?;
    dict.set_item("subject", &commit.subject)?;
    dict.set_item("body", &commit.body)?;
    dict.set_item("presence", commit_presence_to_str(commit.presence))?;
    dict.set_item("origin", commit_origin_to_str(commit.origin))?;
    Ok(dict)
}

fn commit_presence_to_str(presence: CommitPresenceWire) -> &'static str {
    match presence {
        CommitPresenceWire::Unknown => "unknown",
        CommitPresenceWire::Synced => "synced",
        CommitPresenceWire::RemoteOnly => "remote_only",
        CommitPresenceWire::LocalOnly => "local_only",
    }
}

fn commit_origin_to_str(origin: CommitOriginWire) -> &'static str {
    match origin {
        CommitOriginWire::Manual => "manual",
        CommitOriginWire::Stitch => "stitch",
        CommitOriginWire::Auto => "auto",
    }
}

/// Return the VCS-log wire schema version expected by this binding.
#[pyfunction]
#[pyo3(name = "vcs_log_wire_schema_version")]
fn py_vcs_log_wire_schema_version() -> u32 {
    VCS_LOG_WIRE_SCHEMA_VERSION
}

/// Parse a pinned, separator-delimited `git log --format=...` stream into
/// a `list[dict]` mirroring the `VcsCommitWire` JSON shape.
///
/// Mirrors `sase.core.vcs_log_facade._parse_git_log_python`. The Python
/// facade rehydrates each dict into a `VcsCommitWire` via
/// `vcs_commit_from_dict`.
#[pyfunction]
#[pyo3(name = "parse_git_log")]
fn py_parse_git_log<'py>(
    py: Python<'py>,
    stdout: &str,
) -> PyResult<Bound<'py, PyList>> {
    let commits = core_parse_git_log(stdout);
    let list = PyList::empty_bound(py);
    for commit in &commits {
        list.append(vcs_commit_wire_to_py(py, commit)?)?;
    }
    Ok(list)
}

/// Stamp VCS-log commits with local/remote presence.
#[pyfunction]
#[pyo3(name = "classify_commit_presence")]
fn py_classify_commit_presence<'py>(
    py: Python<'py>,
    commits: &Bound<'_, PyAny>,
    ahead_ids: Vec<String>,
    behind_ids: Vec<String>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(commits)?;
    let parsed: Vec<VcsCommitWire> =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "commits is not a valid list[VcsCommitWire]: {e}"
            ))
        })?;
    let classified =
        core_classify_commit_presence(parsed, ahead_ids, behind_ids);
    let out = serde_json::to_value(&classified).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &out)
}

/// Classify a full commit message as stitch, auto, or manual.
#[pyfunction]
#[pyo3(name = "classify_commit_origin")]
fn py_classify_commit_origin(message: &str) -> &'static str {
    commit_origin_to_str(core_classify_commit_origin(message))
}

/// Return derived type labels for one `VcsCommitWire` dict.
#[pyfunction]
#[pyo3(name = "classify_commit_types")]
fn py_classify_commit_types<'py>(
    py: Python<'py>,
    commit: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let value = py_to_json_value(commit)?;
    let parsed: VcsCommitWire = serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "commit is not a valid VcsCommitWire: {e}"
        ))
    })?;
    let labels = core_classify_commit_types_for_commit(&parsed);
    let list = PyList::empty_bound(py);
    for label in labels {
        list.append(label)?;
    }
    Ok(list)
}

/// Interleave per-repo commit lists into a single newest-first timeline.
///
/// `repos` is a `list[tuple[str, list[dict]]]` of `(repo_label, commits)`
/// where each commit dict has the `VcsCommitWire` shape. Returns a
/// `list[dict]` of `AggregatedCommitWire`-shape dicts (the commit fields
/// flattened with a leading `repo` key), sorted by `timestamp` desc with a
/// stable `(repo, full_id)` tie-break and truncated to `limit`.
///
/// Mirrors `sase.core.vcs_log_facade._aggregate_commit_log_python`.
#[pyfunction]
#[pyo3(name = "aggregate_commit_log")]
fn py_aggregate_commit_log<'py>(
    py: Python<'py>,
    repos: &Bound<'_, PyAny>,
    limit: usize,
) -> PyResult<PyObject> {
    let value = py_to_json_value(repos)?;
    let parsed: Vec<(String, Vec<VcsCommitWire>)> =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
            "repos is not a valid list[tuple[str, list[VcsCommitWire]]]: {e}"
        ))
        })?;
    let aggregated = core_aggregate_commit_log(parsed, limit);
    let out = serde_json::to_value(&aggregated).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &out)
}

/// Strictly summarize a recognized merge commit subject.
#[pyfunction]
#[pyo3(name = "parse_merge_summary")]
fn py_parse_merge_summary<'py>(
    py: Python<'py>,
    subject: &str,
    body: &str,
) -> PyResult<PyObject> {
    match core_parse_merge_summary(subject, body) {
        Some(summary) => {
            let value = serde_json::to_value(&summary).map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
            json_value_to_py(py, &value)
        }
        None => Ok(py.None()),
    }
}

pub(crate) fn register_vcs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_commit_shas_equivalent, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_git_object_sharing_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_plan_git_object_sharing, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_pending_commit_checkpoint_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_decide_pending_commit_checkpoint_recovery,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_commit_footer_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_commit_footer, m)?)?;
    m.add_function(wrap_pyfunction!(py_update_commit_footer, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_commit_subject_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_default_commit_subject_types, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_commit_subject, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonical_pull_request_url, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_external_pr_import, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_repository_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_canonical_repository_identity, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_repository_reference, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_name_status_z, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_branch_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_derive_git_workspace_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_conflicted_files, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_local_changes, m)?)?;
    m.add_function(wrap_pyfunction!(py_retryability_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_failure_retryability, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_decide_sidecar_publication_after_push,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_vcs_log_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_log, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_commit_presence, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_commit_origin, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_commit_types, m)?)?;
    m.add_function(wrap_pyfunction!(py_aggregate_commit_log, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_merge_summary, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
