//! Artifact record bindings: refs, files, consumption, eligibility, and back-references.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

/// Parse one canonical kind-tagged artifact reference.
#[pyfunction]
#[pyo3(name = "artifact_ref_parse")]
fn py_artifact_ref_parse<'py>(
    py: Python<'py>,
    value: &str,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(py, core_parse_artifact_ref(value))
}

/// Render a parsed artifact-reference dictionary.
#[pyfunction]
#[pyo3(name = "artifact_ref_render")]
fn py_artifact_ref_render(reference: &Bound<'_, PyAny>) -> PyResult<String> {
    let reference = artifact_ref_from_py(reference)?;
    core_render_artifact_ref(&reference).map_err(artifact_ref_error_to_pyerr)
}

/// Canonicalize an absolute path against caller-supplied local context.
#[pyfunction]
#[pyo3(name = "artifact_ref_canonicalize")]
fn py_artifact_ref_canonicalize(
    path: &str,
    context: &Bound<'_, PyDict>,
) -> PyResult<Option<String>> {
    let context = artifact_ref_context_from_pydict(context)?;
    let path = PathBuf::from(path);
    core_canonicalize_artifact_ref(&path, &context)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Resolve a string or parsed reference against caller-supplied context.
#[pyfunction]
#[pyo3(name = "artifact_ref_resolve")]
fn py_artifact_ref_resolve<'py>(
    py: Python<'py>,
    reference: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let reference = artifact_ref_from_py(reference)?;
    let context = artifact_ref_context_from_pydict(context)?;
    artifact_ref_result_to_py(
        py,
        py.allow_threads(|| core_resolve_artifact_ref(&reference, &context)),
    )
}

/// Normalize a stored artifact-reference list.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_normalize")]
fn py_artifact_ref_list_normalize(
    entries: Vec<String>,
) -> PyResult<Vec<String>> {
    core_normalize_artifact_ref_list(&entries)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Parse every entry in a stored artifact-reference list.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_parse")]
fn py_artifact_ref_list_parse<'py>(
    py: Python<'py>,
    entries: Vec<String>,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(py, core_parse_artifact_ref_list(&entries))
}

/// Resolve a stored artifact-reference list using one shared context.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_resolve")]
fn py_artifact_ref_list_resolve<'py>(
    py: Python<'py>,
    entries: Vec<String>,
    context: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let context = artifact_ref_context_from_pydict(context)?;
    artifact_ref_result_to_py(
        py,
        py.allow_threads(|| core_resolve_artifact_ref_list(&entries, &context)),
    )
}

/// Return the artifact-reference list-resolution wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_resolution_wire_schema_version")]
fn py_artifact_ref_list_resolution_wire_schema_version() -> u64 {
    ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION
}

/// Return the artifact-reference context wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_context_wire_schema_version")]
fn py_artifact_ref_context_wire_schema_version() -> u64 {
    ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION
}

/// Return the artifact-reference path-filter batch wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_path_filter_wire_schema_version")]
fn py_artifact_ref_path_filter_wire_schema_version() -> u64 {
    ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION
}

/// Filter caller-owned repo-relative path payloads with the shared POSIX matcher.
#[pyfunction]
#[pyo3(name = "artifact_ref_filter_path_payloads")]
#[pyo3(signature = (kind, candidates, path_globs = None))]
fn py_artifact_ref_filter_path_payloads<'py>(
    py: Python<'py>,
    kind: &str,
    candidates: Vec<String>,
    path_globs: Option<Vec<String>>,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(
        py,
        core_filter_artifact_ref_path_payloads(
            kind,
            path_globs.as_deref(),
            &candidates,
        ),
    )
}

/// Return the agents-sidecar object relpath for one full SHA-256 digest.
#[pyfunction]
#[pyo3(name = "artifact_object_relpath")]
fn py_artifact_object_relpath(sha256: &str) -> PyResult<String> {
    core_artifact_object_relpath(sha256).map_err(artifact_ref_error_to_pyerr)
}

/// Return the prompt-relative link target for one validated object relpath.
#[pyfunction]
#[pyo3(name = "artifact_object_prompt_link")]
fn py_artifact_object_prompt_link(relpath: &str) -> PyResult<String> {
    core_artifact_object_prompt_link(relpath)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Parse valid rows from a tolerant artifact-reference file JSONL index.
#[pyfunction]
#[pyo3(name = "artifact_ref_file_index_parse")]
fn py_artifact_ref_file_index_parse<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    prompt_artifact_result_to_py(
        py,
        &core_parse_artifact_ref_file_index(data.as_bytes()),
    )
}

/// Render one artifact-reference file index row as compact JSON.
#[pyfunction]
#[pyo3(name = "artifact_ref_file_row_render")]
fn py_artifact_ref_file_row_render(
    record: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let record = artifact_ref_file_row_from_pydict(record)?;
    core_render_artifact_ref_file_row(&record).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid artifact reference file index row: {error}"
        ))
    })
}

/// Validate one artifact-reference file index row.
#[pyfunction]
#[pyo3(name = "artifact_ref_file_row_validate")]
fn py_artifact_ref_file_row_validate(
    record: &Bound<'_, PyDict>,
) -> PyResult<()> {
    let record = artifact_ref_file_row_from_pydict(record)?;
    core_validate_artifact_ref_file_row(&record)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Fold artifact-reference file index rows into logical files.
#[pyfunction]
#[pyo3(name = "artifact_ref_files_fold")]
fn py_artifact_ref_files_fold<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let records = artifact_ref_file_rows_from_py_list(records)?;
    prompt_artifact_result_to_py(py, &core_fold_artifact_ref_files(&records))
}

/// Return the artifact-reference file index wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_file_index_wire_schema_version")]
fn py_artifact_ref_file_index_wire_schema_version() -> u64 {
    ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION
}

/// Scan prompt text for kind-tagged artifact-reference candidates.
#[pyfunction]
#[pyo3(name = "artifact_ref_scan_prompt")]
fn py_artifact_ref_scan_prompt<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_scan_artifact_refs(text)).map_err(
        |error| {
            PyValueError::new_err(format!(
                "internal artifact reference serialize error: {error}"
            ))
        },
    )?;
    json_value_to_py(py, &value)
}

/// Scan rendered document text for semantic link targets.
#[pyfunction]
#[pyo3(name = "artifact_ref_scan_document")]
#[pyo3(signature = (text, known_kinds = None))]
fn py_artifact_ref_scan_document<'py>(
    py: Python<'py>,
    text: &str,
    known_kinds: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let known_kinds = known_kinds.unwrap_or_default();
    let value = serde_json::to_value(core_scan_artifact_ref_document_links(
        text,
        &known_kinds,
    ))
    .map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact document scan serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the artifact-reference document-scan wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_document_scan_wire_schema_version")]
fn py_artifact_ref_document_scan_wire_schema_version() -> u64 {
    ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION
}

/// Split a trailing colon or GitHub-style line location off a link target.
#[pyfunction]
#[pyo3(name = "artifact_ref_split_link_location")]
fn py_artifact_ref_split_link_location(
    py: Python<'_>,
    target: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_split_link_location(target))
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal link location serialize error: {error}"
            ))
        })?;
    json_value_to_py(py, &value)
}

/// Return the link-location wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_link_location_wire_schema_version")]
fn py_artifact_ref_link_location_wire_schema_version() -> u64 {
    LINK_LOCATION_WIRE_SCHEMA_VERSION
}

/// Resolve an unqualified source-path target in its owning repository.
///
/// `owner` carries whatever provenance the caller already knows about the
/// document that named `path` (its own canonical reference, owning project,
/// repository, revision, source directory, and any already-known checkout
/// candidates). `context` is the same `ArtifactRefContextWire`-shaped dict
/// used by `artifact_ref_resolve`, scoped to the document's owning project so
/// its `repositories` inventory reflects that project's linked repos rather
/// than the viewer's cwd.
#[pyfunction]
#[pyo3(name = "artifact_ref_resolve_document_source_target")]
fn py_artifact_ref_resolve_document_source_target<'py>(
    py: Python<'py>,
    path: &str,
    owner: &Bound<'py, PyDict>,
    context: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let owner: ArtifactRefDocumentOwnerWire = serde_json::from_value(
        py_to_json_value(owner.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "owner is not a valid ArtifactRefDocumentOwnerWire dict: {error}"
        ))
    })?;
    let context = artifact_ref_context_from_pydict(context)?;
    artifact_ref_result_to_py(
        py,
        py.allow_threads(|| {
            core_resolve_document_source_target(path, &owner, &context)
        }),
    )
}

/// Return the artifact-reference target-resolution wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_target_resolution_wire_schema_version")]
fn py_artifact_ref_target_resolution_wire_schema_version() -> u64 {
    ARTIFACT_REF_TARGET_RESOLUTION_WIRE_SCHEMA_VERSION
}

/// Return the shared parse/resolution artifact-reference wire version.
#[pyfunction]
#[pyo3(name = "artifact_ref_wire_schema_version")]
fn py_artifact_ref_wire_schema_version() -> u64 {
    debug_assert_eq!(
        ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
        ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION
    );
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION
}

/// Return every compiled-in artifact-reference kind, live or historical.
#[pyfunction]
#[pyo3(name = "artifact_ref_kind_catalog")]
fn py_artifact_ref_kind_catalog(py: Python<'_>) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_artifact_ref_kind_catalog())
        .map_err(|error| {
            PyValueError::new_err(format!(
                "internal artifact reference serialize error: {error}"
            ))
        })?;
    json_value_to_py(py, &value)
}

/// Resolve one requested kind label against the permanent alias registry.
#[pyfunction]
#[pyo3(name = "artifact_ref_kind_canonicalize")]
fn py_artifact_ref_kind_canonicalize(
    py: Python<'_>,
    label: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_canonical_artifact_ref_kind(label))
        .map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact reference serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Parse one reference after rewriting only its kind label to canonical.
#[pyfunction]
#[pyo3(name = "artifact_ref_parse_canonical")]
fn py_artifact_ref_parse_canonical(
    py: Python<'_>,
    value: &str,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(py, core_parse_artifact_ref_canonical(value))
}

/// Render `argument` as a bare or quoted-and-escaped artifact-ref argument.
#[pyfunction]
#[pyo3(name = "artifact_ref_quote_argument")]
fn py_artifact_ref_quote_argument(argument: &str) -> String {
    core_quote_artifact_ref_argument(argument)
}

/// Return the placeholder names the expansion formatter accepts.
#[pyfunction]
#[pyo3(name = "artifact_ref_expansion_placeholders")]
fn py_artifact_ref_expansion_placeholders() -> Vec<String> {
    ARTIFACT_REF_EXPANSION_PLACEHOLDERS
        .iter()
        .map(|placeholder| (*placeholder).to_string())
        .collect()
}

/// Validate an expansion format and return the placeholders it uses.
#[pyfunction]
#[pyo3(name = "artifact_ref_expansion_validate")]
fn py_artifact_ref_expansion_validate(format: &str) -> PyResult<Vec<String>> {
    core_validate_artifact_ref_expansion_format(format)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Render an expansion format, substituting each placeholder verbatim.
#[pyfunction]
#[pyo3(name = "artifact_ref_expansion_render")]
fn py_artifact_ref_expansion_render(
    format: &str,
    values: BTreeMap<String, String>,
) -> PyResult<String> {
    core_render_artifact_ref_expansion(format, &values)
        .map_err(artifact_ref_error_to_pyerr)
}

fn artifact_ref_provider_spec_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactRefProviderSpecWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "spec is not a valid ArtifactRefProviderSpecWire dict: {error}"
        ))
    })
}

/// Validate one assembled artifact-reference provider spec.
#[pyfunction]
#[pyo3(name = "artifact_ref_provider_spec_validate")]
fn py_artifact_ref_provider_spec_validate(
    spec: &Bound<'_, PyDict>,
) -> PyResult<()> {
    let spec = artifact_ref_provider_spec_from_pydict(spec)?;
    core_validate_artifact_ref_provider_spec(&spec)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Compute a stable sha256 hex digest over the normalized provider spec.
#[pyfunction]
#[pyo3(name = "artifact_ref_provider_spec_digest")]
fn py_artifact_ref_provider_spec_digest(
    spec: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let spec = artifact_ref_provider_spec_from_pydict(spec)?;
    core_artifact_ref_provider_spec_digest(&spec)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Return the artifact-reference provider spec wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_provider_spec_wire_schema_version")]
fn py_artifact_ref_provider_spec_wire_schema_version() -> u64 {
    ARTIFACT_REF_PROVIDER_SPEC_WIRE_SCHEMA_VERSION
}

/// Validate one normalized artifact entry.
#[pyfunction]
#[pyo3(name = "artifact_ref_entry_validate")]
fn py_artifact_ref_entry_validate(entry: &Bound<'_, PyDict>) -> PyResult<()> {
    let entry: ArtifactEntryWire = serde_json::from_value(py_to_json_value(
        entry.as_any(),
    )?)
    .map_err(|error| {
        PyValueError::new_err(format!(
            "entry is not a valid ArtifactEntryWire dict: {error}"
        ))
    })?;
    core_validate_artifact_entry(&entry).map_err(artifact_ref_error_to_pyerr)
}

/// Return the artifact-entry wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_entry_wire_schema_version")]
fn py_artifact_ref_entry_wire_schema_version() -> u64 {
    ARTIFACT_REF_ENTRY_WIRE_SCHEMA_VERSION
}

/// Parse a JSONL artifact-reference use manifest, skipping bad rows.
#[pyfunction]
#[pyo3(name = "artifact_ref_use_manifest_parse")]
fn py_artifact_ref_use_manifest_parse<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    let records = core_parse_artifact_ref_use_manifest(data.as_bytes());
    let value = serde_json::to_value(records).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact reference serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Render one compact JSON artifact-reference use manifest row.
#[pyfunction]
#[pyo3(name = "artifact_ref_use_record_render")]
fn py_artifact_ref_use_record_render(
    record: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let record: ArtifactRefUseRecordWire = serde_json::from_value(
        py_to_json_value(record.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "record is not a valid ArtifactRefUseRecordWire dict: {error}"
        ))
    })?;
    core_render_artifact_ref_use_record(&record).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact reference serialize error: {error}"
        ))
    })
}

/// Return the artifact-reference use manifest wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_use_wire_schema_version")]
fn py_artifact_ref_use_wire_schema_version() -> u64 {
    ARTIFACT_REF_USE_WIRE_SCHEMA_VERSION
}

/// Return the `Referenced By` block wire schema version.
#[pyfunction]
#[pyo3(name = "referenced_by_wire_schema_version")]
fn py_referenced_by_wire_schema_version() -> u64 {
    REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION
}

/// Parse the managed `Referenced By` block out of a document.
#[pyfunction]
#[pyo3(name = "referenced_by_block_parse")]
fn py_referenced_by_block_parse(
    py: Python<'_>,
    document: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_parse_referenced_by_block(document))
        .map_err(|error| {
            PyValueError::new_err(format!("internal serialize error: {error}"))
        })?;
    json_value_to_py(py, &value)
}

fn referenced_by_table_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ReferencedByTableWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "table is not a valid ReferencedByTableWire dict: {error}"
        ))
    })
}

/// Render one `Referenced By` block's heading, table, and link definitions.
#[pyfunction]
#[pyo3(name = "referenced_by_block_render")]
fn py_referenced_by_block_render(
    table: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let table = referenced_by_table_from_pydict(table)?;
    core_render_referenced_by_block(&table).map_err(artifact_ref_error_to_pyerr)
}

/// Insert, replace, or remove the managed `Referenced By` block.
#[pyfunction]
#[pyo3(name = "referenced_by_block_upsert")]
fn py_referenced_by_block_upsert(
    document: &str,
    table: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let table = referenced_by_table_from_pydict(table)?;
    core_upsert_referenced_by_block(document, &table)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Remove the managed `Referenced By` block, if present.
#[pyfunction]
#[pyo3(name = "referenced_by_block_remove")]
fn py_referenced_by_block_remove(document: &str) -> String {
    core_remove_referenced_by_block(document)
}

/// Strip the managed `Referenced By` block for content hashing.
#[pyfunction]
#[pyo3(name = "referenced_by_block_strip")]
fn py_referenced_by_block_strip(document: &str) -> String {
    core_strip_referenced_by_block(document)
}

fn artifact_link_eligibility_error_to_pyerr(
    error: ArtifactLinkEligibilityError,
) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn artifact_link_eligibility_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkEligibilityRequestWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactLinkEligibilityRequestWire dict: {error}"
        ))
    })
}

fn artifact_link_eligibility_decision_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkEligibilityDecisionWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "decision is not a valid ArtifactLinkEligibilityDecisionWire dict: {error}"
        ))
    })
}

fn artifact_link_release_evidence_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<ArtifactLinkReleaseEvidenceWire> {
    serde_json::from_value(py_to_json_value(dict.as_any())?).map_err(|error| {
        PyValueError::new_err(format!(
            "evidence is not a valid ArtifactLinkReleaseEvidenceWire dict: {error}"
        ))
    })
}

/// Return the artifact-link eligibility wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_link_eligibility_wire_schema_version")]
pub(crate) fn py_artifact_link_eligibility_wire_schema_version() -> u64 {
    ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION
}

/// Decide whether a run's host-collected change evidence qualifies it to
/// publish its pending automatic artifact links.
#[pyfunction]
#[pyo3(name = "decide_artifact_link_eligibility")]
pub(crate) fn py_decide_artifact_link_eligibility(
    py: Python<'_>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request = artifact_link_eligibility_request_from_pydict(request)?;
    let decision = core_decide_artifact_link_eligibility(&request)
        .map_err(artifact_link_eligibility_error_to_pyerr)?;
    let value = serde_json::to_value(decision).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Build the durable release-evidence record for an eligible decision.
#[pyfunction]
#[pyo3(name = "artifact_link_release_evidence")]
pub(crate) fn py_artifact_link_release_evidence(
    py: Python<'_>,
    decision: &Bound<'_, PyDict>,
    recorded_at: &str,
) -> PyResult<PyObject> {
    let decision = artifact_link_eligibility_decision_from_pydict(decision)?;
    let evidence = core_artifact_link_release_evidence(&decision, recorded_at)
        .map_err(artifact_link_eligibility_error_to_pyerr)?;
    let value = serde_json::to_value(evidence).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Confirm release evidence is still bound to the run trying to use it.
#[pyfunction]
#[pyo3(name = "validate_artifact_link_release_evidence")]
pub(crate) fn py_validate_artifact_link_release_evidence(
    evidence: &Bound<'_, PyDict>,
    expected_run_id: &str,
    expected_agent_id: &str,
) -> PyResult<()> {
    let evidence = artifact_link_release_evidence_from_pydict(evidence)?;
    core_validate_artifact_link_release_evidence(
        &evidence,
        expected_run_id,
        expected_agent_id,
    )
    .map_err(artifact_link_eligibility_error_to_pyerr)
}

/// Build one content-addressed prompt-artifact pool filename.
#[pyfunction]
#[pyo3(name = "prompt_artifact_pool_filename")]
fn py_prompt_artifact_pool_filename(
    sha256: &str,
    original_name: &str,
) -> String {
    core_artifact_pool_filename(sha256, original_name)
}

/// Parse valid rows from a tolerant prompt-artifact JSONL manifest.
#[pyfunction]
#[pyo3(name = "prompt_artifact_manifest_parse")]
fn py_prompt_artifact_manifest_parse<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    prompt_artifact_result_to_py(
        py,
        &core_parse_prompt_artifact_manifest(data.as_bytes()),
    )
}

/// Render one prompt-artifact manifest row as compact JSON.
#[pyfunction]
#[pyo3(name = "prompt_artifact_manifest_render_record")]
fn py_prompt_artifact_manifest_render_record(
    record: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let record = prompt_artifact_record_from_pydict(record)?;
    core_render_prompt_artifact_record(&record).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid prompt artifact manifest record: {error}"
        ))
    })
}

/// Select the newest rows belonging to one agent artifact directory.
#[pyfunction]
#[pyo3(name = "prompt_artifact_manifest_select")]
fn py_prompt_artifact_manifest_select<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
    agent_artifacts_dir: &str,
) -> PyResult<PyObject> {
    let records = prompt_artifact_records_from_py_list(records)?;
    let selected = core_select_prompt_artifact_manifest_records(
        &records,
        agent_artifacts_dir,
    );
    prompt_artifact_result_to_py(py, &selected)
}

/// Rewrite live artifact tokens using a Python target resolver.
#[pyfunction]
#[pyo3(name = "prompt_artifact_rewrite_links")]
fn py_prompt_artifact_rewrite_links<'py>(
    py: Python<'py>,
    prompt: &str,
    records: &Bound<'py, PyList>,
    resolver: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    if !resolver.is_callable() {
        return Err(PyValueError::new_err(
            "prompt artifact resolver must be callable",
        ));
    }
    let records = prompt_artifact_records_from_py_list(records)?;
    let mut targets = Vec::with_capacity(records.len());
    for record in &records {
        let argument = serde_json::to_value(record)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "internal prompt artifact serialize error: {error}"
                ))
            })
            .and_then(|value| json_value_to_py(py, &value))?;
        let target = resolver.call1((argument,))?;
        targets.push(if target.is_none() {
            None
        } else {
            Some(target.extract::<String>().map_err(|_| {
                PyValueError::new_err(
                    "prompt artifact resolver must return str or None",
                )
            })?)
        });
    }
    let mut target_index = 0;
    let rewritten =
        core_rewrite_prompt_artifact_links(prompt, &records, |_| {
            let target = targets[target_index].clone();
            target_index += 1;
            target
        });
    prompt_artifact_result_to_py(py, &rewritten)
}

/// Return the prompt-artifact manifest wire schema version.
#[pyfunction]
#[pyo3(name = "prompt_artifact_wire_schema_version")]
fn py_prompt_artifact_wire_schema_version() -> u64 {
    PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION
}

fn prompt_artifact_record_from_pydict(
    record: &Bound<'_, PyDict>,
) -> PyResult<PromptArtifactRecord> {
    serde_json::from_value(py_to_json_value(record.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "record is not a valid PromptArtifactRecord dict: {error}"
            ))
        },
    )
}

fn prompt_artifact_records_from_py_list(
    records: &Bound<'_, PyList>,
) -> PyResult<Vec<PromptArtifactRecord>> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            serde_json::from_value(py_to_json_value(&record)?).map_err(
                |error| {
                    PyValueError::new_err(format!(
                        "records[{index}] is not a valid PromptArtifactRecord dict: {error}"
                    ))
                },
            )
        })
        .collect()
}

fn prompt_artifact_result_to_py<T>(
    py: Python<'_>,
    result: &T,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!(
            "internal prompt artifact serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Summarize the tolerant artifact-consumption ledger by canonical reference.
#[pyfunction]
#[pyo3(
    name = "artifact_consumption_summary",
    signature = (log_path, refs = None)
)]
fn py_artifact_consumption_summary<'py>(
    py: Python<'py>,
    log_path: &str,
    refs: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let log_path = PathBuf::from(log_path);
    let events = py
        .allow_threads(|| core_read_artifact_consumption_log(&log_path))
        .map_err(|error| PyOSError::new_err(error.to_string()))?;
    let summary = core_summarize_artifact_consumption(&events, refs.as_deref());
    let value = serde_json::to_value(summary).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-consumption summary serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the artifact-consumption summary wire version.
#[pyfunction]
#[pyo3(name = "artifact_consumption_wire_schema_version")]
fn py_artifact_consumption_wire_schema_version() -> u64 {
    ARTIFACT_CONSUMPTION_WIRE_SCHEMA_VERSION
}

/// Query the tolerant artifact-file index using a frontend-neutral filter dict.
#[pyfunction]
#[pyo3(name = "artifact_files_query")]
fn py_artifact_files_query<'py>(
    py: Python<'py>,
    index_path: &str,
    filters: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let filters = serde_json::from_value::<ArtifactFileQueryFiltersWire>(
        py_to_json_value(filters.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "filters is not a valid ArtifactFileQueryFiltersWire dict: \
                 {error}"
        ))
    })?;
    let index_path = PathBuf::from(index_path);
    let rows = py
        .allow_threads(|| core_query_artifact_files(&index_path, &filters))
        .map_err(artifact_file_query_error_to_pyerr)?;
    let value = serde_json::to_value(rows).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-file query serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the artifact-file query result wire version.
#[pyfunction]
#[pyo3(name = "artifact_file_query_wire_schema_version")]
fn py_artifact_file_query_wire_schema_version() -> u64 {
    ARTIFACT_FILE_QUERY_WIRE_SCHEMA_VERSION
}

/// Batch-query non-chat artifact metadata for waited producers' exact
/// artifact directories.
///
/// `groups` is a list of `{"wait_name": str, "agent_artifacts_dirs": [str]}`
/// dicts in dependency order. Reads the tolerant index at most once, and
/// only when at least one producer directory is requested.
#[pyfunction]
#[pyo3(name = "artifact_context_query")]
fn py_artifact_context_query<'py>(
    py: Python<'py>,
    index_path: &str,
    groups: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let groups =
        serde_json::from_value::<Vec<ArtifactContextProducerGroupWire>>(
            py_to_json_value(groups.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
            "groups is not a valid list of ArtifactContextProducerGroupWire \
                 dicts: {error}"
        ))
        })?;
    let index_path = PathBuf::from(index_path);
    let rows = py
        .allow_threads(|| core_query_artifact_context(&index_path, &groups))
        .map_err(artifact_file_query_error_to_pyerr)?;
    let value = serde_json::to_value(rows).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-context query serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the artifact-context query result wire version.
#[pyfunction]
#[pyo3(name = "artifact_context_query_wire_schema_version")]
fn py_artifact_context_query_wire_schema_version() -> u64 {
    ARTIFACT_CONTEXT_QUERY_WIRE_SCHEMA_VERSION
}

/// Materialize one VCS-backed artifact into its content-addressed cache.
#[pyfunction]
#[pyo3(name = "artifact_file_materialize_vcs")]
fn py_artifact_file_materialize_vcs<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = serde_json::from_value::<
        ArtifactFileVcsMaterializationRequestWire,
    >(py_to_json_value(request.as_any())?)
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid \
                 ArtifactFileVcsMaterializationRequestWire dict: {error}"
        ))
    })?;
    let result =
        py.allow_threads(|| core_materialize_vcs_artifact_file(&request));
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-file materialization serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Aggregate artifact-file store economics without mutating the index.
#[pyfunction]
#[pyo3(name = "artifact_file_store_economics")]
fn py_artifact_file_store_economics<'py>(
    py: Python<'py>,
    index_path: &str,
    options: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let options = serde_json::from_value::<ArtifactFileEconomicsOptionsWire>(
        py_to_json_value(options.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "options is not a valid ArtifactFileEconomicsOptionsWire dict: \
             {error}"
        ))
    })?;
    let index_path = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| {
            core_artifact_file_store_economics(&index_path, &options)
        })
        .map_err(artifact_file_query_error_to_pyerr)?;
    artifact_file_lifecycle_value_to_py(py, &result, "economics")
}

/// Plan deterministic artifact-file retention without mutating the index.
#[pyfunction]
#[pyo3(name = "artifact_file_retention_plan")]
fn py_artifact_file_retention_plan<'py>(
    py: Python<'py>,
    index_path: &str,
    policy: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let policy = serde_json::from_value::<ArtifactFileRetentionPolicyWire>(
        py_to_json_value(policy.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "policy is not a valid ArtifactFileRetentionPolicyWire dict: \
             {error}"
        ))
    })?;
    let index_path = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| {
            core_plan_artifact_file_retention(&index_path, &policy)
        })
        .map_err(artifact_file_query_error_to_pyerr)?;
    artifact_file_lifecycle_value_to_py(py, &result, "retention plan")
}

/// Move one artifact payload and its complete record into restorable trash.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_store")]
fn py_artifact_file_trash_store<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = serde_json::from_value::<ArtifactFileTrashRequestWire>(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactFileTrashRequestWire dict: \
             {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_trash_artifact_file(&request))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash store")
}

/// List restorable trash entries newest first.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_list")]
fn py_artifact_file_trash_list<'py>(
    py: Python<'py>,
    trash_root: &str,
) -> PyResult<PyObject> {
    let trash_root = PathBuf::from(trash_root);
    let result = py
        .allow_threads(|| core_list_artifact_file_trash(&trash_root))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash list")
}

/// Restore one trash entry's payload and return its complete original record.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_restore")]
fn py_artifact_file_trash_restore<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request =
        serde_json::from_value::<ArtifactFileTrashRestoreRequestWire>(
            py_to_json_value(request.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid \
                 ArtifactFileTrashRestoreRequestWire dict: {error}"
            ))
        })?;
    let result = py
        .allow_threads(|| core_restore_artifact_file_trash(&request))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash restore")
}

/// Permanently remove trash entries at or before an explicit cutoff.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_purge")]
fn py_artifact_file_trash_purge<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = serde_json::from_value::<ArtifactFileTrashPurgeRequestWire>(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactFileTrashPurgeRequestWire \
                 dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_purge_artifact_file_trash(&request))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash purge")
}

/// Return the shared artifact-file lifecycle request/result wire version.
#[pyfunction]
#[pyo3(name = "artifact_file_lifecycle_wire_schema_version")]
fn py_artifact_file_lifecycle_wire_schema_version() -> u64 {
    ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION
}

fn artifact_ref_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, ArtifactRefError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value =
        serde_json::to_value(result.map_err(artifact_ref_error_to_pyerr)?)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "internal artifact reference serialize error: {error}"
                ))
            })?;
    json_value_to_py(py, &value)
}

pub(crate) fn artifact_ref_error_to_pyerr(error: ArtifactRefError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn artifact_file_query_error_to_pyerr(error: ArtifactFileQueryError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn artifact_file_lifecycle_value_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
    operation: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-file {operation} serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

fn artifact_ref_from_py(
    value: &Bound<'_, PyAny>,
) -> PyResult<ParsedArtifactRefWire> {
    if let Ok(reference) = value.extract::<String>() {
        return core_parse_artifact_ref(&reference)
            .map_err(artifact_ref_error_to_pyerr);
    }
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!(
            "reference is not a valid ParsedArtifactRefWire dict: {error}"
        ))
    })
}

pub(crate) fn artifact_ref_context_from_pydict(
    context: &Bound<'_, PyDict>,
) -> PyResult<ArtifactRefContextWire> {
    serde_json::from_value(py_to_json_value(context.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "context is not a valid ArtifactRefContextWire dict: {error}"
            ))
        },
    )
}

fn artifact_ref_file_row_from_pydict(
    record: &Bound<'_, PyDict>,
) -> PyResult<ArtifactRefFileVersionRowWire> {
    serde_json::from_value(py_to_json_value(record.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "row is not a valid ArtifactRefFileVersionRowWire dict: {error}"
            ))
        },
    )
}

fn artifact_ref_file_rows_from_py_list(
    records: &Bound<'_, PyList>,
) -> PyResult<Vec<ArtifactRefFileVersionRowWire>> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            serde_json::from_value(py_to_json_value(&record)?).map_err(
                |error| {
                    PyValueError::new_err(format!(
                        "rows[{index}] is not a valid ArtifactRefFileVersionRowWire dict: {error}"
                    ))
                },
            )
        })
        .collect()
}

/// Return the shared artifact-reference payload inventory for one kind.
#[pyfunction]
#[pyo3(name = "artifact_ref_payload_inventory")]
fn py_artifact_ref_payload_inventory(
    py: Python<'_>,
    kind: &str,
    context: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let context = artifact_ref_context_from_pydict(&context)?;
    let inventory =
        sase_core::editor_build_artifact_ref_payload_inventory(kind, &context)
            .map_err(artifact_ref_error_to_pyerr)?;
    let value = serde_json::to_value(&inventory).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_artifact_refs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_artifact_ref_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_list_normalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_list_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_list_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_list_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_context_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_path_filter_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_filter_path_payloads, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_scan_prompt, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_scan_document, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_document_scan_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_split_link_location, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_link_location_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_resolve_document_source_target,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_target_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_kind_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_kind_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_parse_canonical, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_quote_argument, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_object_relpath, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_object_prompt_link, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_file_index_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_file_row_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_file_row_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_files_fold, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_file_index_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_expansion_placeholders,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_expansion_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_expansion_render, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_provider_spec_validate,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_provider_spec_digest, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_provider_spec_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_entry_validate, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_entry_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_use_manifest_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_use_record_render, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_use_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_referenced_by_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_referenced_by_block_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_referenced_by_block_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_referenced_by_block_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(py_referenced_by_block_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_referenced_by_block_strip, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_link_eligibility_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_decide_artifact_link_eligibility, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_link_release_evidence, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_validate_artifact_link_release_evidence,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_pool_filename, m)?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_manifest_parse, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_prompt_artifact_manifest_render_record,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_manifest_select, m)?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_rewrite_links, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_prompt_artifact_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_consumption_summary, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_consumption_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_files_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_materialize_vcs, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_store_economics, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_retention_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_restore, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_purge, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_file_lifecycle_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_file_query_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_context_query, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_context_query_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_payload_inventory, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
