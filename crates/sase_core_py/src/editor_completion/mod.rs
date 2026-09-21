//! Editor completion bindings: snippets, placeholders, directives, and catalogs.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

/// Immutable native payload rows and fuzzy-match metadata.
#[pyclass(name = "AtReferenceInventory", module = "sase_core_rs", frozen)]
#[derive(Clone, Debug)]
struct PyAtReferenceInventory {
    payloads: sase_core::AtReferencePayloadIndex,
}

/// Immutable compiled glossary matcher catalog.
#[pyclass(name = "GlossaryCatalogHandle", module = "sase_core_rs", frozen)]
#[derive(Clone, Debug)]
struct PyGlossaryCatalogHandle {
    catalog: CoreCompiledGlossaryCatalog,
}

#[pymethods]
impl PyAtReferenceInventory {
    #[new]
    #[pyo3(signature = (*, payloads))]
    fn new(payloads: &Bound<'_, PyList>) -> PyResult<Self> {
        let payloads = serde_json::from_value::<
            Vec<sase_core::AtReferencePayloadRowWire>,
        >(py_to_json_value(payloads.as_any())?)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "payloads are not valid AtReferencePayloadRowWire dicts: \
                     {error}"
            ))
        })?;
        Ok(Self {
            payloads: sase_core::AtReferencePayloadIndex::new(payloads),
        })
    }

    fn __len__(&self) -> usize {
        self.payloads.len()
    }
}

#[pymethods]
impl PyGlossaryCatalogHandle {
    fn __len__(&self) -> usize {
        self.catalog.len()
    }

    fn catalog(&self, py: Python<'_>) -> PyResult<PyObject> {
        glossary_to_py(py, self.catalog.catalog())
    }

    fn scan(&self, py: Python<'_>, text: &str) -> PyResult<PyObject> {
        glossary_to_py(py, &self.catalog.scan(text))
    }

    fn lookup(
        &self,
        py: Python<'_>,
        text: &str,
        line: u32,
        character: u32,
    ) -> PyResult<PyObject> {
        let span = self
            .catalog
            .lookup(text, sase_core::EditorPosition { line, character });
        glossary_to_py(py, &span)
    }
}

#[pyfunction]
#[pyo3(name = "compose_snippet_catalog")]
fn py_compose_snippet_catalog(
    py: Python<'_>,
    templates: BTreeMap<String, String>,
) -> PyResult<PyObject> {
    let composed = core_compose_snippet_catalog(&templates);
    let value = serde_json::to_value(composed).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "validate_snippet_trigger")]
fn py_validate_snippet_trigger(
    py: Python<'_>,
    trigger: &str,
) -> PyResult<PyObject> {
    let validation = core_validate_snippet_trigger(trigger);
    let value = serde_json::to_value(validation).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "load_editor_snippet_catalog")]
#[pyo3(signature = (project = None, root_dir = None))]
fn py_load_editor_snippet_catalog(
    py: Python<'_>,
    project: Option<String>,
    root_dir: Option<String>,
) -> PyResult<PyObject> {
    let request = EditorSnippetCatalogRequestWire {
        schema_version: 1,
        project,
    };
    let options = XpromptCatalogLoadOptions::new(root_dir.map(PathBuf::from));
    let response = core_load_editor_snippet_catalog(&request, &options)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let value = serde_json::to_value(response).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[derive(Debug, Default, Deserialize)]
struct PyXpromptCatalogOptions {
    root_dir: Option<PathBuf>,
    package_xprompts_dir: Option<PathBuf>,
    package_skills_dir: Option<PathBuf>,
    default_xprompts_dir: Option<PathBuf>,
    default_config_path: Option<PathBuf>,
    #[serde(default)]
    plugin_xprompt_dirs: BTreeMap<String, PathBuf>,
    #[serde(default)]
    plugin_skill_dirs: BTreeMap<String, PathBuf>,
    #[serde(default)]
    plugin_config_paths: BTreeMap<String, PathBuf>,
}

fn xprompt_catalog_options_from_py(
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<XpromptCatalogLoadOptions> {
    let raw = match options {
        Some(options) => serde_json::from_value::<PyXpromptCatalogOptions>(
            py_to_json_value(options.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "xprompt catalog options are invalid: {error}"
            ))
        })?,
        None => PyXpromptCatalogOptions::default(),
    };
    let resource_paths = XpromptCatalogResourcePaths {
        package_xprompts_dir: raw.package_xprompts_dir,
        package_skills_dir: raw.package_skills_dir,
        default_xprompts_dir: raw.default_xprompts_dir,
        default_config_path: raw.default_config_path,
        plugin_xprompt_dirs: raw.plugin_xprompt_dirs,
        plugin_skill_dirs: raw.plugin_skill_dirs,
        plugin_config_paths: raw.plugin_config_paths,
    };
    Ok(XpromptCatalogLoadOptions::new(raw.root_dir)
        .with_resource_paths(resource_paths))
}

#[pyfunction]
#[pyo3(name = "resolve_xprompt_skill_definition")]
#[pyo3(signature = (request, options = None))]
fn py_resolve_xprompt_skill_definition<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let request: XpromptSkillDefinitionRequestWire =
        serde_json::from_value(py_to_json_value(request.as_any())?).map_err(
            |error| {
                PyValueError::new_err(format!(
                    "request is not a valid XpromptSkillDefinitionRequestWire dict: {error}"
                ))
            },
        )?;
    let options = xprompt_catalog_options_from_py(options)?;
    let resolution = core_resolve_xprompt_skill_definition(&request, &options);
    let value = serde_json::to_value(resolution).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "xprompt_skill_definition_wire_schema_version")]
fn py_xprompt_skill_definition_wire_schema_version() -> u64 {
    XPROMPT_SKILL_DEFINITION_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "filter_model_completion_entries")]
fn py_filter_model_completion_entries(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
    partial: &str,
) -> PyResult<PyObject> {
    let entries = model_completion_entries_from_py_list(entries)?;
    let filtered = core_filter_model_completion_entries(&entries, partial);
    let value = serde_json::to_value(filtered).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "model_shortcut_context")]
fn py_model_shortcut_context(
    py: Python<'_>,
    text: &str,
    position: &Bound<'_, PyAny>,
) -> PyResult<Option<PyObject>> {
    let position = editor_position_from_py(position)?;
    core_model_shortcut_context(text, position)
        .map(|context| serialize_to_py(py, &context))
        .transpose()
}

#[pyfunction]
#[pyo3(name = "model_shortcut_edit")]
fn py_model_shortcut_edit(
    py: Python<'_>,
    text: &str,
    position: &Bound<'_, PyAny>,
    entries: &Bound<'_, PyList>,
    selected_value: &str,
) -> PyResult<Option<PyObject>> {
    let position = editor_position_from_py(position)?;
    let entries = model_completion_entries_from_py_list(entries)?;
    core_model_shortcut_edit(text, position, &entries, selected_value)
        .map(|edit| serialize_to_py(py, &edit))
        .transpose()
}

#[pyfunction]
#[pyo3(name = "argument_colon_to_parentheses_edit")]
fn py_argument_colon_to_parentheses_edit(
    py: Python<'_>,
    text: &str,
    position: &Bound<'_, PyAny>,
) -> PyResult<Option<PyObject>> {
    let position = editor_position_from_py(position)?;
    let document = sase_core::DocumentSnapshot::new(text);
    core_plan_argument_colon_to_parentheses_edit(&document, position)
        .map(|edit| serialize_to_py(py, &edit))
        .transpose()
}

#[pyfunction]
#[pyo3(name = "argument_double_colon_to_parentheses_edit")]
fn py_argument_double_colon_to_parentheses_edit(
    py: Python<'_>,
    text: &str,
    position: &Bound<'_, PyAny>,
) -> PyResult<Option<PyObject>> {
    let position = editor_position_from_py(position)?;
    let document = sase_core::DocumentSnapshot::new(text);
    core_plan_argument_double_colon_to_parentheses_edit(&document, position)
        .map(|edit| serialize_to_py(py, &edit))
        .transpose()
}

/// Filter a `%model:`-shaped catalog down to concrete model rows a
/// `==query` shortcut may expand to, in canonical catalog order.
#[pyfunction]
#[pyo3(name = "filter_explicit_model_shortcut_entries")]
fn py_filter_explicit_model_shortcut_entries(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
    query: &str,
) -> PyResult<PyObject> {
    let entries = model_completion_entries_from_py_list(entries)?;
    let filtered = core_filter_explicit_model_shortcut_entries(&entries, query);
    let value = serde_json::to_value(filtered).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "model_alias_shortcut_context")]
fn py_model_alias_shortcut_context(
    py: Python<'_>,
    text: &str,
    position: &Bound<'_, PyAny>,
) -> PyResult<Option<PyObject>> {
    let position = editor_position_from_py(position)?;
    core_detect_model_alias_shortcut_context(text, position)
        .map(|context| serialize_to_py(py, &context))
        .transpose()
}

#[pyfunction]
#[pyo3(name = "model_alias_shortcut_edit")]
fn py_model_alias_shortcut_edit(
    py: Python<'_>,
    text: &str,
    position: &Bound<'_, PyAny>,
    entries: &Bound<'_, PyList>,
    selected_alias: &str,
) -> PyResult<Option<PyObject>> {
    let position = editor_position_from_py(position)?;
    let entries = model_completion_entries_from_py_list(entries)?;
    core_plan_model_alias_shortcut_edit(
        text,
        position,
        &entries,
        selected_alias,
    )
    .map(|edit| serialize_to_py(py, &edit))
    .transpose()
}

/// Filter a `%model:`-shaped catalog down to the effective alias rows a
/// `=query` shortcut may expand to, in canonical catalog order. ACE's equals
/// shortcut menu and `sase-xprompt-lsp`'s `=` completion both build their
/// candidate rows from this one binding so alias filtering never drifts
/// between the two frontends.
#[pyfunction]
#[pyo3(name = "filter_model_alias_shortcut_entries")]
fn py_filter_model_alias_shortcut_entries(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
    query: &str,
) -> PyResult<PyObject> {
    let entries = model_completion_entries_from_py_list(entries)?;
    let filtered = core_filter_model_alias_shortcut_entries(&entries, query);
    let value = serde_json::to_value(filtered).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// The nested snippet session engine's single entry point: apply one wire
/// event (`plan`, `expand`, `advance`, `retreat`, `apply_edit`, or `clear`)
/// to the current session state dict and return the new state plus
/// whatever the event resolved, as a plain `dict` with the keys `state`,
/// `cursor_offset`, `text`, and `tabstop_offsets` always present.
///
/// One binding rather than five keeps the wire surface and the
/// `schema_version` story small; `event["kind"]` selects the transition.
#[pyfunction]
#[pyo3(name = "apply_snippet_session_event")]
fn py_apply_snippet_session_event<'py>(
    py: Python<'py>,
    state: &Bound<'py, PyDict>,
    event: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let state: SnippetSessionState = serde_json::from_value(py_to_json_value(
        state.as_any(),
    )?)
    .map_err(|error| {
        PyValueError::new_err(format!("invalid snippet session state: {error}"))
    })?;
    let event: SnippetSessionEvent = serde_json::from_value(py_to_json_value(
        event.as_any(),
    )?)
    .map_err(|error| {
        PyValueError::new_err(format!("invalid snippet session event: {error}"))
    })?;

    let result = core_apply_snippet_session_event(state, event);
    let value = serde_json::to_value(&result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

fn model_completion_entries_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ModelCompletionEntryWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let Some(object) = value.as_object() else {
            return Err(PyValueError::new_err(format!(
                "entries[{idx}] must be a dict"
            )));
        };
        for field in MODEL_COMPLETION_ENTRY_WIRE_FIELDS {
            if !object.contains_key(*field) {
                return Err(PyValueError::new_err(format!(
                    "entries[{idx}] is missing field {field:?}"
                )));
            }
        }
        for field in object.keys() {
            if !MODEL_COMPLETION_ENTRY_WIRE_FIELDS.contains(&field.as_str()) {
                return Err(PyValueError::new_err(format!(
                    "entries[{idx}] contains unexpected field {field:?}"
                )));
            }
        }
        let entry: ModelCompletionEntryWire =
            serde_json::from_value(value).map_err(|error| {
                PyValueError::new_err(format!(
                    "entries[{idx}] is not a valid ModelCompletionEntryWire dict: {error}"
                ))
            })?;
        if entry.value.is_empty() {
            return Err(PyValueError::new_err(format!(
                "entries[{idx}].value must be non-empty"
            )));
        }
        values.push(entry);
    }
    Ok(values)
}

fn editor_position_from_py(
    value: &Bound<'_, PyAny>,
) -> PyResult<EditorPosition> {
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!(
            "position is not a valid EditorPosition dict with UTF-16 line/character units: {error}"
        ))
    })
}

// --- Prompt frontmatter panel: schema & validation surface ---
//
// These four bindings expose the panel-oriented frontmatter API from
// `sase_core::editor`. They are the single source of truth the TUI prompt
// frontmatter panel shares with the xprompt LSP, so panel guidance and editor
// diagnostics never drift. Results are serialized through `serde_json` and
// rehydrated as plain dicts/lists on the Python side (see
// `sase.xprompt.frontmatter_schema`).
/// Return the ordered panel frontmatter field schema as a list of dicts.
#[pyfunction]
#[pyo3(name = "frontmatter_field_schema")]
fn py_frontmatter_field_schema(py: Python<'_>) -> PyResult<PyObject> {
    let schema = sase_core::editor_frontmatter_field_schema();
    let value = serde_json::to_value(&schema).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the supported `input` type catalog as a list of dicts.
#[pyfunction]
#[pyo3(name = "frontmatter_input_type_schema")]
fn py_frontmatter_input_type_schema(py: Python<'_>) -> PyResult<PyObject> {
    let schema = sase_core::editor_frontmatter_input_type_schema();
    let value = serde_json::to_value(&schema).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate a whole frontmatter block. Returns LSP-shape diagnostic dicts.
#[pyfunction]
#[pyo3(name = "validate_frontmatter")]
fn py_validate_frontmatter(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let diagnostics = sase_core::editor_validate_frontmatter(text);
    let value = serde_json::to_value(&diagnostics).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate a single frontmatter field value. Returns LSP-shape diagnostic
/// dicts. `value` is the YAML text that would follow `field:`.
#[pyfunction]
#[pyo3(name = "validate_frontmatter_field")]
fn py_validate_frontmatter_field(
    py: Python<'_>,
    field: &str,
    value: &str,
) -> PyResult<PyObject> {
    let diagnostics =
        sase_core::editor_validate_frontmatter_field(field, value);
    let json = serde_json::to_value(&diagnostics).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

// --- At-reference menu surface ------------------------------------------
//
// These bindings expose the core `@` reference context detector and grouped
// menu builder through plain JSON-shaped Python dict/list values.
/// Return `@` reference context at the cursor, or `None` when the cursor is
/// not inside a valid reference candidate.
#[pyfunction]
#[pyo3(name = "at_reference_context")]
#[pyo3(signature = (text, line, character, known_kinds = None))]
fn py_at_reference_context(
    py: Python<'_>,
    text: &str,
    line: u32,
    character: u32,
    known_kinds: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let known_kinds = known_kinds.unwrap_or_default();
    let document = sase_core::DocumentSnapshot::new(text);
    let context = sase_core::editor_detect_at_reference_context(
        &document,
        sase_core::EditorPosition { line, character },
        &known_kinds,
    );
    let value = serde_json::to_value(&context).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return grouped `@` reference menu rows for a detected context and caller
/// supplied inventory.
///
/// `payload_index`, when supplied, replaces `inventory["payloads"]` without
/// converting those rows through Python objects on each call.
#[pyfunction]
#[pyo3(name = "at_reference_menu")]
#[pyo3(signature = (context, inventory, payload_index = None, options = None))]
fn py_at_reference_menu(
    py: Python<'_>,
    context: Bound<'_, PyDict>,
    inventory: Bound<'_, PyDict>,
    payload_index: Option<PyRef<'_, PyAtReferenceInventory>>,
    options: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let context = serde_json::from_value::<sase_core::AtReferenceContextWire>(
        py_to_json_value(context.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "context is not a valid AtReferenceContextWire dict: {error}"
        ))
    })?;
    let inventory =
        serde_json::from_value::<sase_core::AtReferenceInventoryWire>(
            py_to_json_value(inventory.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "inventory is not a valid AtReferenceInventoryWire dict: {error}"
            ))
        })?;
    let options = options
        .map(|options| {
            serde_json::from_value::<sase_core::AtReferenceMenuOptionsWire>(
                py_to_json_value(options.as_any())?,
            )
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "options is not a valid AtReferenceMenuOptionsWire dict: {error}"
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();
    let menu = sase_core::editor_build_at_reference_menu_with_options(
        &context,
        &inventory,
        payload_index
            .as_ref()
            .map(|payload_index| &payload_index.payloads),
        options,
    );
    let value = serde_json::to_value(&menu).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Fuzzy-match a query against text using the shared editor matcher.
#[pyfunction]
#[pyo3(name = "fuzzy_match")]
fn py_fuzzy_match(
    py: Python<'_>,
    query: &str,
    text: &str,
) -> PyResult<PyObject> {
    let Some(match_result) = sase_core::editor_fuzzy_match(query, text) else {
        return Ok(py.None());
    };
    json_value_to_py(
        py,
        &serde_json::json!({
            "tier": match_result.tier,
            "score": match_result.score,
            "runs": match_result.runs,
        }),
    )
}

// --- Placeholder completion and highlighting surface ---------------------
//
// These bindings expose the same Rust placeholder engine consumed directly
// by the xprompt LSP. They are the single source of truth shared by the TUI
// and LSP for extraction, completion filtering, and replacement ranges.
/// Return placeholder completion context and candidates, or `None` when the
/// cursor is outside a placeholder or no reusable candidates exist.
///
/// `common` carries caller-ranked placeholders from a durable store. They are
/// emitted after the document's own candidates and tagged `"common"`.
#[pyfunction]
#[pyo3(name = "placeholder_completion")]
#[pyo3(signature = (text, line, character, common = None))]
fn py_placeholder_completion(
    py: Python<'_>,
    text: &str,
    line: u32,
    character: u32,
    common: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let common = common.unwrap_or_default();
    let document = sase_core::DocumentSnapshot::new(text);
    let completion = sase_core::editor_build_placeholder_completion_candidates(
        &document,
        sase_core::EditorPosition { line, character },
        &common,
    )
    .filter(|completion| !completion.candidates.is_empty());
    let value = serde_json::to_value(&completion).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return all complete placeholder spans for prompt highlighting.
#[pyfunction]
#[pyo3(name = "placeholder_spans")]
fn py_placeholder_spans(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let document = sase_core::DocumentSnapshot::new(text);
    let spans = sase_core::editor_extract_placeholder_spans(&document);
    let value = serde_json::to_value(&spans).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return xprompt and directive argument spans as UTF-8 byte offsets.
#[pyfunction]
#[pyo3(name = "xprompt_argument_spans")]
#[pyo3(signature = (text, entries = None))]
fn py_xprompt_argument_spans(
    py: Python<'_>,
    text: &str,
    entries: Option<Bound<'_, PyList>>,
) -> PyResult<PyObject> {
    let document = sase_core::DocumentSnapshot::new(text);
    let spans = if let Some(entries) = entries {
        let entries = serde_json::from_value::<Vec<sase_core::XpromptAssistEntry>>(
            py_to_json_value(entries.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "entries is not a valid list of XpromptAssistEntry dicts: {error}"
            ))
        })?;
        sase_core::editor_extract_xprompt_argument_spans_with_catalog(
            &document, &entries,
        )
    } else {
        sase_core::editor_extract_xprompt_argument_spans(&document)
    };
    let value = serde_json::to_value(&spans).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return ordered summaries for the prompt's unique raw placeholders.
#[pyfunction]
#[pyo3(name = "raw_placeholder_fields")]
fn py_raw_placeholder_fields(
    py: Python<'_>,
    text: &str,
    context_width: usize,
) -> PyResult<PyObject> {
    let fields = sase_core::editor_raw_placeholder_fields(text, context_width);
    let value = serde_json::to_value(&fields).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Replace mapped raw placeholders without touching literal spans.
#[pyfunction]
#[pyo3(name = "substitute_raw_placeholders")]
fn py_substitute_raw_placeholders(
    text: &str,
    values: BTreeMap<String, String>,
) -> String {
    sase_core::editor_substitute_raw_placeholders(text, &values)
}

/// Convert placeholder labels into stable xprompt input names.
#[pyfunction]
#[pyo3(name = "placeholder_input_names")]
fn py_placeholder_input_names(texts: Vec<String>) -> Vec<String> {
    sase_core::editor_placeholder_input_names(texts)
}

// --- Directive completion contract ---------------------------------------
//
// These bindings expose the shared xprompt directive contract, grammar-aware
// cursor classifier, and JSON-shaped candidate builder consumed by ACE.
/// Return the canonical directive completion contract as a list of dicts.
#[pyfunction]
#[pyo3(name = "directive_contract")]
#[pyo3(signature = (enabled_feature_flags = None))]
fn py_directive_contract(
    py: Python<'_>,
    enabled_feature_flags: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let flags = enabled_feature_flags.unwrap_or_default();
    let contract = sase_core::editor_directive_contract_with_flags(&flags);
    let value = serde_json::to_value(&contract).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Classify directive completion context at a UTF-16 cursor, or `None`.
#[pyfunction]
#[pyo3(name = "directive_completion_context")]
fn py_directive_completion_context(
    py: Python<'_>,
    text: &str,
    line: u32,
    character: u32,
) -> PyResult<PyObject> {
    let document = sase_core::DocumentSnapshot::new(text);
    let context = sase_core::editor_detect_directive_context_at_position(
        &document,
        sase_core::EditorPosition { line, character },
    );
    let value = serde_json::to_value(&context).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Build JSON-shaped directive completion candidates for a classified context.
///
/// `inventories` supplies host-owned model, agent, and bead rows. Static
/// keyword and example values come from the shared contract even when the
/// inventory is empty.
#[pyfunction]
#[pyo3(name = "directive_completion_candidates")]
#[pyo3(signature = (context, inventories = None))]
fn py_directive_completion_candidates(
    py: Python<'_>,
    context: Bound<'_, PyDict>,
    inventories: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let context = serde_json::from_value::<sase_core::CompletionContext>(
        py_to_json_value(context.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "context is not a valid CompletionContext dict: {error}"
        ))
    })?;
    let inventories = inventories
        .map(|inventories| {
            serde_json::from_value::<sase_core::DirectiveCompletionInventories>(
                py_to_json_value(inventories.as_any())?,
            )
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "inventories is not a valid DirectiveCompletionInventories dict: {error}"
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();
    let list = sase_core::editor_build_directive_clause_candidates(
        &context,
        &inventories,
    );
    let value = serde_json::to_value(&list).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Glossary catalog bindings -------------------------------------------
fn glossary_error_to_pyerr(error: GlossaryError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn glossary_entries_from_pylist(
    entries: &Bound<'_, PyList>,
) -> PyResult<Vec<GlossaryInputEntryWire>> {
    serde_json::from_value(py_to_json_value(entries.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "entries are not valid GlossaryInputEntryWire dicts: {error}"
            ))
        },
    )
}

fn glossary_to_py<T>(py: Python<'_>, value: &T) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "glossary_validate")]
fn py_glossary_validate(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let entries = glossary_entries_from_pylist(entries)?;
    let diagnostics = core_validate_glossary_entries(&entries);
    glossary_to_py(py, &diagnostics)
}

#[pyfunction]
#[pyo3(name = "glossary_catalog")]
fn py_glossary_catalog(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let entries = glossary_entries_from_pylist(entries)?;
    let catalog = core_build_glossary_catalog(entries)
        .map_err(glossary_error_to_pyerr)?;
    glossary_to_py(py, &catalog)
}

#[pyfunction]
#[pyo3(name = "compile_glossary_catalog")]
fn py_compile_glossary_catalog(
    entries: &Bound<'_, PyList>,
) -> PyResult<PyGlossaryCatalogHandle> {
    let entries = glossary_entries_from_pylist(entries)?;
    let catalog = core_compile_glossary_catalog(entries)
        .map_err(glossary_error_to_pyerr)?;
    Ok(PyGlossaryCatalogHandle { catalog })
}

/// Scan `%if::` / `%proc::` directive-owned fences into a versioned wire.
#[pyfunction]
#[pyo3(name = "scan_directive_owned_fences")]
fn py_scan_directive_owned_fences<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_scan_directive_owned_fences(text))
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_editor_completion(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_compose_snippet_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_snippet_trigger, m)?)?;
    m.add_function(wrap_pyfunction!(py_load_editor_snippet_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_xprompt_skill_definition, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_xprompt_skill_definition_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_filter_model_completion_entries, m)?)?;
    m.add_function(wrap_pyfunction!(py_model_shortcut_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_model_shortcut_edit, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_argument_colon_to_parentheses_edit,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_argument_double_colon_to_parentheses_edit,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_filter_explicit_model_shortcut_entries,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_model_alias_shortcut_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_model_alias_shortcut_edit, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_filter_model_alias_shortcut_entries,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_apply_snippet_session_event, m)?)?;
    m.add_function(wrap_pyfunction!(py_frontmatter_field_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_frontmatter_input_type_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_frontmatter, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_frontmatter_field, m)?)?;
    m.add_class::<PyAtReferenceInventory>()?;
    m.add_class::<PyGlossaryCatalogHandle>()?;
    m.add_function(wrap_pyfunction!(py_at_reference_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_at_reference_menu, m)?)?;
    m.add_function(wrap_pyfunction!(py_fuzzy_match, m)?)?;
    m.add_function(wrap_pyfunction!(py_placeholder_completion, m)?)?;
    m.add_function(wrap_pyfunction!(py_placeholder_spans, m)?)?;
    m.add_function(wrap_pyfunction!(py_xprompt_argument_spans, m)?)?;
    m.add_function(wrap_pyfunction!(py_raw_placeholder_fields, m)?)?;
    m.add_function(wrap_pyfunction!(py_substitute_raw_placeholders, m)?)?;
    m.add_function(wrap_pyfunction!(py_placeholder_input_names, m)?)?;
    m.add_function(wrap_pyfunction!(py_directive_contract, m)?)?;
    m.add_function(wrap_pyfunction!(py_directive_completion_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_directive_completion_candidates, m)?)?;
    m.add_function(wrap_pyfunction!(py_glossary_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_glossary_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_glossary_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_directive_owned_fences, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
