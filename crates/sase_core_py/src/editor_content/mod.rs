//! Editor content bindings: languages, layout, history filters, and stash.

use crate::prelude::*;

use crate::continuation::optional_wire_to_py;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

/// Return the schema version for source-language binding payloads.
#[pyfunction]
#[pyo3(name = "source_language_wire_schema_version")]
fn py_source_language_wire_schema_version() -> u32 {
    SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION
}

/// Return the UTF-8 prefix budget used for shebang and diff sniffing.
#[pyfunction]
#[pyo3(name = "source_language_prefix_budget_bytes")]
fn py_source_language_prefix_budget_bytes() -> usize {
    SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES
}

/// Resolve a canonical source language from a request dict.
#[pyfunction]
#[pyo3(name = "resolve_source_language")]
fn py_resolve_source_language<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request_value = py_to_json_value(request.as_any())?;
    let req = source_language_request_from_json_value(&request_value)
        .map_err(PyValueError::new_err)?;
    let selected = core_resolve_source_language(&req);
    let value = serde_json::to_value(&selected).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Select a logical filename hint from provenance fields.
#[pyfunction]
#[pyo3(name = "logical_source_filename")]
fn py_logical_source_filename<'py>(
    py: Python<'py>,
    hints: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let hints_value = py_to_json_value(hints.as_any())?;
    let hints = source_filename_hints_from_json_value(&hints_value)
        .map_err(PyValueError::new_err)?;
    match core_logical_filename_from_hints(&hints) {
        Some(name) => json_value_to_py(py, &JsonValue::String(name)),
        None => Ok(py.None()),
    }
}

// --- Prompt-history project filter bindings -------------------------------
fn prompt_history_catalog_from_py(
    catalog: &Bound<'_, PyList>,
) -> PyResult<Vec<PromptHistoryProjectIdentityWire>> {
    let value = py_to_json_value(catalog.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "catalog is not a valid list of project identity dicts: {e}"
        ))
    })
}

/// Compile one Ctrl+K prompt-history filter string into a `project:`
/// constraint plus a literal text substring, resolved against *catalog*.
#[pyfunction]
#[pyo3(name = "compile_prompt_history_query")]
fn py_compile_prompt_history_query<'py>(
    py: Python<'py>,
    raw_query: &str,
    catalog: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let catalog = prompt_history_catalog_from_py(catalog)?;
    let compiled = core_compile_prompt_history_query(raw_query, &catalog);
    serialize_to_py(py, &compiled)
}

/// Encode arbitrary literal text so it always round-trips through
/// `compile_prompt_history_query` as an unscoped substring, even when it
/// starts with a `project:`-lookalike prefix.
#[pyfunction]
#[pyo3(name = "encode_prompt_history_literal")]
fn py_encode_prompt_history_literal(text: &str) -> String {
    core_encode_prompt_history_literal(text)
}

/// Build the initial Ctrl+K history query from the recognized leading
/// workspace reference (if any) and the draft's remaining text.
#[pyfunction]
#[pyo3(name = "build_prompt_history_seed")]
fn py_build_prompt_history_seed<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
    catalog: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let request_value = py_to_json_value(request.as_any())?;
    let request: PromptHistorySeedRequestWire =
        serde_json::from_value(request_value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid PromptHistorySeedRequestWire dict: {e}"
            ))
        })?;
    let catalog = prompt_history_catalog_from_py(catalog)?;
    let seed = core_build_prompt_history_seed(&request, &catalog);
    serialize_to_py(py, &seed)
}

/// Batch-match prepared prompt-history rows against one compiled query.
#[pyfunction]
#[pyo3(name = "match_prompt_history_rows")]
fn py_match_prompt_history_rows<'py>(
    py: Python<'py>,
    query: &Bound<'py, PyDict>,
    rows: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let query_value = py_to_json_value(query.as_any())?;
    let query: CompiledPromptHistoryQueryWire =
        serde_json::from_value(query_value).map_err(|e| {
            PyValueError::new_err(format!(
                "query is not a valid CompiledPromptHistoryQueryWire dict: {e}"
            ))
        })?;
    let rows_value = py_to_json_value(rows.as_any())?;
    let rows: Vec<PromptHistoryRowFactsWire> =
        serde_json::from_value(rows_value).map_err(|e| {
            PyValueError::new_err(format!(
            "rows is not a valid list of PromptHistoryRowFactsWire dicts: {e}"
        ))
        })?;
    let result = core_match_prompt_history_rows(&query, &rows);
    serialize_to_py(py, &result)
}

/// Return the shared Markdown link-reference wire schema version.
#[pyfunction]
#[pyo3(name = "markdown_link_refs_wire_schema_version")]
pub(crate) fn py_markdown_link_refs_wire_schema_version() -> u64 {
    MARKDOWN_LINK_REFS_WIRE_SCHEMA_VERSION
}

/// Scan a document for Markdown reference definitions and numeric uses.
#[pyfunction]
#[pyo3(name = "markdown_reference_links_scan")]
pub(crate) fn py_markdown_reference_links_scan(
    py: Python<'_>,
    document: &str,
) -> PyResult<PyObject> {
    let value =
        serde_json::to_value(core_scan_markdown_reference_links(document))
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "internal serialize error: {error}"
                ))
            })?;
    json_value_to_py(py, &value)
}

/// Allocate a numeric Markdown reference label for one destination.
#[pyfunction]
#[pyo3(name = "markdown_reference_label_allocate")]
pub(crate) fn py_markdown_reference_label_allocate(
    scan: &Bound<'_, PyDict>,
    destination: &str,
    assigned: BTreeMap<String, String>,
) -> PyResult<String> {
    let scan: MarkdownReferenceScanWire = serde_json::from_value(
        py_to_json_value(scan.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "scan is not a valid MarkdownReferenceScanWire dict: {error}"
        ))
    })?;
    Ok(core_allocate_markdown_reference_label(
        &scan,
        destination,
        &assigned,
    ))
}

/// Append every missing Markdown reference definition to a document.
#[pyfunction]
#[pyo3(name = "markdown_reference_definitions_append")]
pub(crate) fn py_markdown_reference_definitions_append(
    document: &str,
    definitions: &Bound<'_, PyList>,
) -> PyResult<String> {
    let mut parsed = Vec::with_capacity(definitions.len());
    for (index, item) in definitions.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let definition: MarkdownReferenceDefinitionWire =
            serde_json::from_value(value).map_err(|error| {
                PyValueError::new_err(format!(
                    "definitions[{index}] is not a valid MarkdownReferenceDefinitionWire dict: {error}"
                ))
            })?;
        parsed.push(definition);
    }
    Ok(core_append_markdown_reference_definitions(
        document, &parsed,
    ))
}

/// Return the prompt archive inventory wire schema version.
#[pyfunction]
#[pyo3(name = "prompt_archive_inventory_wire_schema_version")]
fn py_prompt_archive_inventory_wire_schema_version() -> u64 {
    PROMPT_ARCHIVE_INVENTORY_WIRE_SCHEMA_VERSION
}

/// Discover and parse canonical prompt archive documents.
#[pyfunction]
#[pyo3(name = "prompt_archive_inventory", signature = (root, request = None))]
fn py_prompt_archive_inventory<'py>(
    py: Python<'py>,
    root: &str,
    request: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let request = match request {
        Some(dict) => {
            let value = py_to_json_value(dict.as_any())?;
            serde_json::from_value::<PromptArchiveInventoryRequestWire>(value)
                .map_err(|error| {
                    PyValueError::new_err(format!(
                        "request is not a valid PromptArchiveInventoryRequestWire dict: {error}"
                    ))
                })?
        }
        None => PromptArchiveInventoryRequestWire::default(),
    };
    let root = PathBuf::from(root);
    let inventory =
        py.allow_threads(|| core_prompt_archive_inventory(&root, request));
    let value = serde_json::to_value(&inventory).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Prompt stash store bindings -----------------------------------------
fn prompt_stash_error_to_pyerr(error: PromptStashStoreError) -> PyErr {
    match error {
        error @ PromptStashStoreError::LockTimeout { .. } => {
            PyTimeoutError::new_err(error.to_string())
        }
        error => PyValueError::new_err(error.to_string()),
    }
}

/// Read the prompt-stash JSONL store and return a snapshot dict.
///
/// The GIL is released while Rust performs filesystem work.
#[pyfunction]
#[pyo3(name = "read_prompt_stash_snapshot")]
fn py_read_prompt_stash_snapshot(
    py: Python<'_>,
    path: &str,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| core_read_prompt_stash_snapshot(&path));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Append one prompt-stash entry dict and return the updated snapshot dict.
#[pyfunction]
#[pyo3(name = "append_prompt_stash")]
fn py_append_prompt_stash<'py>(
    py: Python<'py>,
    path: &str,
    entry: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let entry = prompt_stash_entry_from_pydict(entry)?;
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| core_append_prompt_stash(&path, &entry));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Remove entries whose ids appear in `ids`; return removed rows + snapshot.
#[pyfunction]
#[pyo3(name = "pop_prompt_stash")]
fn py_pop_prompt_stash(
    py: Python<'_>,
    path: &str,
    ids: Vec<String>,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_pop_prompt_stash(&path, &ids));
    let value =
        serde_json::to_value(outcome.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Set the persisted pin flag for entries whose ids appear in `ids`.
#[pyfunction]
#[pyo3(name = "set_prompt_stash_pinned")]
fn py_set_prompt_stash_pinned(
    py: Python<'_>,
    path: &str,
    ids: Vec<String>,
    pinned: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot =
        py.allow_threads(|| core_set_prompt_stash_pinned(&path, &ids, pinned));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Rewrite the prompt-stash store from entry dicts (merge semantics).
#[pyfunction]
#[pyo3(name = "rewrite_prompt_stash")]
fn py_rewrite_prompt_stash<'py>(
    py: Python<'py>,
    path: &str,
    entries: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let entries = prompt_stash_entries_from_py_list(entries)?;
    let path = PathBuf::from(path);
    let snapshot =
        py.allow_threads(|| core_rewrite_prompt_stash(&path, &entries));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

fn prompt_stash_entry_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<PromptStashEntryWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "entry is not a valid PromptStashEntryWire dict: {e}"
        ))
    })
}

fn prompt_stash_entries_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<PromptStashEntryWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let entry: PromptStashEntryWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "entries[{idx}] is not a valid PromptStashEntryWire dict: {e}"
                ))
            })?;
        values.push(entry);
    }
    Ok(values)
}

// --- Canonical project/home content layout -------------------------------
/// Return the shared canonical/legacy SASE content layout and xprompt order.
#[pyfunction]
#[pyo3(name = "sase_content_layout")]
#[pyo3(signature = (
    home_root,
    project_root = None,
    chezmoi_root = None,
    project = None
))]
fn py_sase_content_layout(
    py: Python<'_>,
    home_root: &str,
    project_root: Option<&str>,
    chezmoi_root: Option<&str>,
    project: Option<&str>,
) -> PyResult<PyObject> {
    let project_root = project_root.map(PathBuf::from);
    let chezmoi_root = chezmoi_root.map(PathBuf::from);
    let layout = core_sase_content_layout(
        project_root.as_deref(),
        &PathBuf::from(home_root),
        chezmoi_root.as_deref(),
        project,
    );
    let json = serde_json::to_value(layout).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Resolve ordered candidate presence with the core collision policy.
#[pyfunction]
#[pyo3(name = "resolve_layout_candidates")]
fn py_resolve_layout_candidates(
    py: Python<'_>,
    policy: &str,
    exists: Vec<bool>,
) -> PyResult<PyObject> {
    let policy = LayoutCollisionPolicyWire::parse(policy).ok_or_else(|| {
        PyValueError::new_err(format!(
            "unsupported layout collision policy {policy:?}; expected 'error' or 'first_wins'"
        ))
    })?;
    let resolution = core_resolve_layout_candidates(policy, &exists);
    let json = serde_json::to_value(resolution).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Return the canonical `skill/<name>` xprompt reference for a skill source.
#[pyfunction]
#[pyo3(name = "skill_reference_name")]
#[pyo3(signature = (skill_name, project = None))]
fn py_skill_reference_name(skill_name: &str, project: Option<&str>) -> String {
    core_skill_reference_name(project, skill_name)
}

/// Return the canonical `memory/<stem>` xprompt reference for a memory note.
#[pyfunction]
#[pyo3(name = "memory_reference_name")]
fn py_memory_reference_name(stem: &str) -> String {
    core_memory_reference_name(stem)
}

/// Split a canonical `memory/<stem>` reference back into its note stem.
#[pyfunction]
#[pyo3(name = "memory_reference_stem")]
fn py_memory_reference_stem(name: &str) -> Option<String> {
    core_memory_reference_stem(name).map(str::to_string)
}

/// Reject a non-memory definition that claims a reserved `memory/` reference.
#[pyfunction]
#[pyo3(name = "reserved_memory_namespace_issue")]
fn py_reserved_memory_namespace_issue(
    py: Python<'_>,
    source: &str,
    name: &str,
) -> PyResult<PyObject> {
    optional_wire_to_py(py, core_reserved_memory_namespace_issue(source, name))
}

/// Apply the shared xprompt-memory note rules to one file in a memory root.
#[pyfunction]
#[pyo3(name = "memory_note_issue")]
#[pyo3(signature = (source, stem, note_type = None))]
fn py_memory_note_issue(
    py: Python<'_>,
    source: &str,
    stem: &str,
    note_type: Option<&str>,
) -> PyResult<PyObject> {
    optional_wire_to_py(py, core_memory_note_issue(source, stem, note_type))
}

/// Apply the shared two-way skill placement rules to one loaded definition.
#[pyfunction]
#[pyo3(name = "skill_placement_issue")]
#[pyo3(signature = (
    source,
    in_skill_source,
    declares_skill,
    migrate_to = None
))]
fn py_skill_placement_issue(
    py: Python<'_>,
    source: &str,
    in_skill_source: bool,
    declares_skill: bool,
    migrate_to: Option<&str>,
) -> PyResult<PyObject> {
    let Some(issue) = core_skill_placement_issue(
        source,
        in_skill_source,
        declares_skill,
        migrate_to,
    ) else {
        return Ok(py.None());
    };
    let json = serde_json::to_value(issue).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Return single-line inline-code ranges as UTF-8 byte offsets.
#[pyfunction]
#[pyo3(name = "inline_code_ranges")]
#[pyo3(signature = (text, masked_ranges = None))]
fn py_inline_code_ranges(
    text: &str,
    masked_ranges: Option<Vec<(usize, usize)>>,
) -> Vec<(usize, usize)> {
    core_inline_code_ranges(text, masked_ranges.as_deref().unwrap_or(&[]))
}

/// Return fenced-block byte ranges as `(start, end)` tuples.
#[pyfunction]
#[pyo3(name = "fenced_block_ranges")]
fn py_fenced_block_ranges(text: &str) -> Vec<(usize, usize)> {
    core_fenced_block_ranges(text)
}

/// Return structured fenced-block details as JSON-shaped dicts.
#[pyfunction]
#[pyo3(name = "fenced_block_details")]
fn py_fenced_block_details<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_fenced_block_details_wire(text))
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

/// Return the output tail bounded by both line count and Unicode chars.
#[pyfunction]
#[pyo3(name = "tail_text_by_lines_and_chars")]
fn py_tail_text_by_lines_and_chars<'py>(
    py: Python<'py>,
    text: &str,
    max_lines: usize,
    max_chars: usize,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_tail_text_by_lines_and_chars(
        text, max_lines, max_chars,
    ))
    .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Wire schema version for `CodeValue` and directive-owned fence scans.
#[pyfunction]
#[pyo3(name = "code_value_wire_schema_version")]
fn py_code_value_wire_schema_version() -> u32 {
    CODE_VALUE_WIRE_SCHEMA_VERSION
}

pub(crate) fn register_editor_content(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_source_language_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_source_language_prefix_budget_bytes,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_resolve_source_language, m)?)?;
    m.add_function(wrap_pyfunction!(py_logical_source_filename, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_prompt_history_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_encode_prompt_history_literal, m)?)?;
    m.add_function(wrap_pyfunction!(py_build_prompt_history_seed, m)?)?;
    m.add_function(wrap_pyfunction!(py_match_prompt_history_rows, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_markdown_link_refs_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_markdown_reference_links_scan, m)?)?;
    m.add_function(wrap_pyfunction!(py_markdown_reference_label_allocate, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_markdown_reference_definitions_append,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_prompt_archive_inventory_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_prompt_archive_inventory, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_prompt_stash_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_prompt_stash, m)?)?;
    m.add_function(wrap_pyfunction!(py_pop_prompt_stash, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_prompt_stash_pinned, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_prompt_stash, m)?)?;
    m.add_function(wrap_pyfunction!(py_sase_content_layout, m)?)?;
    m.add_function(wrap_pyfunction!(py_skill_reference_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_reference_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_reference_stem, m)?)?;
    m.add_function(wrap_pyfunction!(py_reserved_memory_namespace_issue, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_note_issue, m)?)?;
    m.add_function(wrap_pyfunction!(py_skill_placement_issue, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_layout_candidates, m)?)?;
    m.add_function(wrap_pyfunction!(py_inline_code_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(py_fenced_block_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(py_fenced_block_details, m)?)?;
    m.add_function(wrap_pyfunction!(py_tail_text_by_lines_and_chars, m)?)?;
    m.add_function(wrap_pyfunction!(py_code_value_wire_schema_version, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
