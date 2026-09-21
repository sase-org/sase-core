//! Query engine bindings: tokenizer, parser, evaluator, profiles, and corpus handles.

use crate::prelude::*;

use crate::json_bridge::{
    json_value_to_py, py_dict_item, py_dict_key_as_string, py_to_json_value,
    strings_from_py_list,
};

use pyo3::wrap_pyfunction;

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

/// Parse a project file's bytes into canonical PatchWire-shape dicts.
///
/// This accepts both canonical `## Patch` / `STITCHES:` and legacy
/// `## ChangeSpec` / `COMMITS:` text, then emits `stitches` and `stitch_id`
/// keys for new Python callers.
#[pyfunction]
#[pyo3(name = "parse_patch_project_bytes")]
fn py_parse_patch_project_bytes<'py>(
    py: Python<'py>,
    path: &str,
    data: &Bound<'py, PyBytes>,
) -> PyResult<Bound<'py, PyList>> {
    let bytes: &[u8] = data.as_bytes();
    let patches = sase_core::parse_patch_project_bytes(path, bytes)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;

    let list = PyList::empty_bound(py);
    for patch in &patches {
        let value = serde_json::to_value(patch).map_err(|e| {
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

/// Tokenize a query against a compiled profile dict.
#[pyfunction]
#[pyo3(name = "tokenize_query_with_profile")]
fn py_tokenize_query_with_profile<'py>(
    py: Python<'py>,
    query: &str,
    profile: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    let profile = profile_from_pydict(profile)?;
    let tokens = core_tokenize_query_with_profile(query, &profile)
        .map_err(query_error_to_pyerr)?;
    let list = PyList::empty_bound(py);
    for tok in &tokens {
        let value = serde_json::to_value(tok).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
        list.append(json_value_to_py(py, &value)?)?;
    }
    Ok(list)
}

/// Parse a query against a compiled profile dict.
#[pyfunction]
#[pyo3(name = "parse_query_with_profile")]
fn py_parse_query_with_profile<'py>(
    py: Python<'py>,
    query: &str,
    profile: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let profile = profile_from_pydict(profile)?;
    let expr = core_parse_query_with_profile(query, &profile)
        .map_err(query_error_to_pyerr)?;
    let value = expr_to_python_wire(&expr);
    json_value_to_py(py, &value)
}

/// Canonicalize a query against a compiled profile dict.
#[pyfunction]
#[pyo3(name = "canonicalize_query_with_profile")]
fn py_canonicalize_query_with_profile<'py>(
    query: &str,
    profile: &Bound<'py, PyDict>,
) -> PyResult<String> {
    let profile = profile_from_pydict(profile)?;
    core_canonicalize_query_with_profile(query, &profile)
        .map_err(query_error_to_pyerr)
}

/// Compile a query against a compiled profile dict.
#[pyfunction]
#[pyo3(name = "compile_query_with_profile")]
fn py_compile_query_with_profile<'py>(
    query: &str,
    profile: &Bound<'py, PyDict>,
) -> PyResult<PyQueryProgramHandle> {
    let profile = profile_from_pydict(profile)?;
    let program = core_compile_query_with_profile(query, &profile)
        .map_err(query_error_to_pyerr)?;
    Ok(PyQueryProgramHandle { program })
}

/// Compile a generic corpus from a compiled profile and precomputed rows.
///
/// Profile and row conversion happens while holding the GIL. Indexing
/// runs without it. Rows are read directly from each `PyDict` into a
/// `QueryRow` — no `serde_json::Value` intermediate — since this loop runs
/// once per corpus row and the JSON tree was otherwise a full second
/// materialization of data the caller already built as a Python dict.
#[pyfunction]
#[pyo3(name = "compile_corpus_with_profile")]
fn py_compile_corpus_with_profile<'py>(
    py: Python<'py>,
    profile: &Bound<'py, PyDict>,
    rows: &Bound<'py, PyList>,
) -> PyResult<PyQueryCorpusHandle> {
    let profile = profile_from_pydict(profile)?;
    let mut wire_rows = Vec::with_capacity(rows.len());
    for (idx, item) in rows.iter().enumerate() {
        let row = query_row_from_py_row(&item, &profile).map_err(|error| {
            PyValueError::new_err(format!("rows[{idx}]: {error}"))
        })?;
        wire_rows.push(row);
    }
    let corpus =
        py.allow_threads(|| CoreQueryCorpus::from_rows(&profile, wire_rows));
    Ok(PyQueryCorpusHandle { corpus })
}

/// Build one `QueryRow` directly from a Python row mapping.
///
/// Mirrors `QueryRow::from_wire`'s accepted shape and validation exactly,
/// but reads scalars and containers straight off the `PyAny` tree instead
/// of first converting the whole row to a `serde_json::Value`.
fn query_row_from_py_row(
    item: &Bound<'_, PyAny>,
    profile: &CompiledQueryProfile,
) -> Result<QueryRow, String> {
    let dict = item
        .downcast::<PyDict>()
        .map_err(|_| "row must be a mapping".to_string())?;

    let mut fields: BTreeMap<String, QueryFieldValues> = BTreeMap::new();
    if let Some(raw_fields) = py_dict_item(dict, "fields")? {
        let raw_fields = raw_fields
            .downcast::<PyDict>()
            .map_err(|_| "fields must be a mapping".to_string())?;
        for (key, value) in raw_fields.iter() {
            let key = py_dict_key_as_string(&key)?;
            let strings = py_query_row_field_strings(&value)?;
            fields.insert(key, QueryFieldValues { strings });
        }
    }

    let mut searchable: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if let Some(raw_searchable) = py_dict_item(dict, "searchable")? {
        let raw_searchable = raw_searchable
            .downcast::<PyDict>()
            .map_err(|_| "searchable must be a mapping".to_string())?;
        for (key, value) in raw_searchable.iter() {
            let key = py_dict_key_as_string(&key)?;
            searchable.insert(key, py_query_row_field_strings(&value)?);
        }
    }

    let mut searchable_values: Vec<String> = Vec::new();
    if let Some(raw_values) = py_dict_item(dict, "searchable_values")? {
        let raw_values = raw_values.downcast::<PyList>().map_err(|_| {
            "searchable_values must be a list of strings".to_string()
        })?;
        for (idx, value) in raw_values.iter().enumerate() {
            searchable_values.push(value.extract::<String>().map_err(
                |_| format!("searchable_values[{idx}] must be a string"),
            )?);
        }
    }

    let searchable_text = match py_dict_item(dict, "searchable_text")? {
        Some(value) if !value.is_none() => Some(
            value
                .extract::<String>()
                .map_err(|_| "searchable_text must be a string".to_string())?,
        ),
        _ => None,
    };

    let mut predicates = QueryPredicateFacts::default();
    if let Some(raw_predicates) = py_dict_item(dict, "predicates")? {
        let raw_predicates = raw_predicates
            .downcast::<PyDict>()
            .map_err(|_| "predicates must be a mapping".to_string())?;
        predicates.error_suffix =
            py_predicate_flag(raw_predicates, "error_suffix")?;
        predicates.running_agent =
            py_predicate_flag(raw_predicates, "running_agent")?;
        predicates.running_process =
            py_predicate_flag(raw_predicates, "running_process")?;
    }

    let mut searchable_parts: Vec<String> = Vec::new();
    if !searchable.is_empty() {
        for key in profile.searchable_keys() {
            if let Some(values) = searchable.get(key) {
                searchable_parts.extend(values.iter().cloned());
            }
        }
        for (key, values) in &searchable {
            if profile.field(key).is_none() {
                searchable_parts.extend(values.iter().cloned());
            }
        }
    }
    searchable_parts.extend(searchable_values);

    let searchable_text = match searchable_text {
        Some(text) => text,
        None if !searchable_parts.is_empty() => searchable_parts.join("\n"),
        None => {
            let mut parts = Vec::new();
            for key in profile.searchable_keys() {
                if let Some(values) = fields.get(key).or_else(|| {
                    fields.iter().find_map(|(candidate, values)| {
                        if candidate.eq_ignore_ascii_case(key) {
                            Some(values)
                        } else {
                            None
                        }
                    })
                }) {
                    parts.extend(values.strings.iter().cloned());
                }
            }
            parts.join("\n")
        }
    };

    Ok(QueryRow {
        fields,
        searchable_text,
        predicates,
    })
}

fn py_predicate_flag(
    predicates: &Bound<'_, PyDict>,
    key: &str,
) -> Result<bool, String> {
    match py_dict_item(predicates, key)? {
        Some(value) if !value.is_none() => value
            .extract::<bool>()
            .map_err(|_| format!("predicates.{key} must be a bool")),
        _ => Ok(false),
    }
}

/// Extract a query row field/searchable value's strings directly from a
/// `PyAny`, matching `strings_from_json`'s scalar-or-nested-list contract
/// without building a `serde_json::Value` first.
fn py_query_row_field_strings(
    value: &Bound<'_, PyAny>,
) -> Result<Vec<String>, String> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(flag) = value.extract::<bool>() {
        return Ok(vec![if flag {
            "true".to_string()
        } else {
            "false".to_string()
        }]);
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(vec![i.to_string()]);
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(vec![u.to_string()]);
    }
    if let Ok(f) = value.extract::<f64>() {
        return match serde_json::Number::from_f64(f) {
            Some(number) => Ok(vec![number.to_string()]),
            None => Err(format!("non-finite float: {f}")),
        };
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(if text.is_empty() {
            Vec::new()
        } else {
            vec![text]
        });
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.extend(py_query_row_field_strings(&item)?);
        }
        return Ok(out);
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut out = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            out.extend(py_query_row_field_strings(&item)?);
        }
        return Ok(out);
    }
    Err("query row field values must be a scalar or list".to_string())
}

/// Compile a persistent Patch corpus from Python wire dicts.
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
    let wire_specs = patches_from_py_list(specs)?;
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
    let results = py
        .allow_threads(|| {
            core_try_evaluate_query_many_in_corpus(
                &program.program,
                &corpus.corpus,
            )
        })
        .map_err(query_error_to_pyerr)?;

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
/// `sase.core.wire.to_json_dict(changespec_to_wire(cs))` legacy compatibility).
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
    let wire_specs = patches_from_py_list(specs)?;
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

// --- Project lifecycle bindings ------------------------------------------
/// Read effective project lifecycle metadata from ProjectSpec content.
#[pyfunction]
#[pyo3(name = "read_project_lifecycle_from_content")]
fn py_read_project_lifecycle_from_content<'py>(
    py: Python<'py>,
    content: &str,
) -> PyResult<PyObject> {
    let lifecycle = core_read_project_lifecycle_from_content(content);
    let value = serde_json::to_value(&lifecycle).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return ProjectSpec content with PROJECT_STATE updated in the metadata header.
#[pyfunction]
#[pyo3(name = "apply_project_lifecycle_update")]
fn py_apply_project_lifecycle_update(
    content: &str,
    state: &str,
) -> PyResult<String> {
    core_apply_project_lifecycle_update(content, state)
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

/// Return ProjectSpec content with PROJECT_ALIASES updated in the metadata header.
#[pyfunction]
#[pyo3(name = "apply_project_aliases_update")]
fn py_apply_project_aliases_update<'py>(
    content: &str,
    aliases: &Bound<'py, PyList>,
) -> PyResult<String> {
    let aliases = strings_from_py_list(aliases, "aliases")?;
    core_apply_project_aliases_update(content, &aliases)
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

/// Return ProjectSpec content with PROJECT_NAME updated in the metadata header.
#[pyfunction]
#[pyo3(name = "apply_project_name_update")]
#[pyo3(signature = (content, name=None))]
fn py_apply_project_name_update(
    content: &str,
    name: Option<String>,
) -> PyResult<String> {
    core_apply_project_name_update(content, name.as_deref())
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

/// List lifecycle records for project directories under *projects_root*.
#[pyfunction]
#[pyo3(name = "list_project_records", signature = (projects_root, include_states, include_home = false, projects_only = false))]
fn py_list_project_records<'py>(
    py: Python<'py>,
    projects_root: &str,
    include_states: &Bound<'py, PyList>,
    include_home: bool,
    projects_only: bool,
) -> PyResult<PyObject> {
    let states = strings_from_py_list(include_states, "include_states")?;
    let root = PathBuf::from(projects_root);
    let records = py
        .allow_threads(|| {
            core_list_project_records(
                &root,
                &states,
                include_home,
                projects_only,
            )
        })
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let value = serde_json::to_value(&records).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn query_error_to_pyerr(err: QueryErrorWire) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

fn profile_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<CompiledQueryProfile> {
    let value = py_to_json_value(dict.as_any())?;
    CompiledQueryProfile::from_wire(&value).map_err(query_error_to_pyerr)
}

fn patches_from_py_list(
    specs: &Bound<'_, PyList>,
) -> PyResult<Vec<ChangeSpecWire>> {
    let mut wire_specs: Vec<ChangeSpecWire> = Vec::with_capacity(specs.len());
    for (idx, item) in specs.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let spec: ChangeSpecWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "specs[{idx}] is not a valid ChangeSpecWire/PatchWire-compatible dict: {e}"
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

pub(crate) fn register_query(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyQueryCorpusHandle>()?;
    m.add_class::<PyQueryProgramHandle>()?;
    m.add_function(wrap_pyfunction!(py_parse_project_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_patch_project_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_tokenize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_tokenize_query_with_profile, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_query_with_profile, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_query_with_profile, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_query_with_profile, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_corpus_with_profile, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_corpus, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_query_many, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_read_project_lifecycle_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_apply_project_lifecycle_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_project_aliases_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_project_name_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_list_project_records, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
