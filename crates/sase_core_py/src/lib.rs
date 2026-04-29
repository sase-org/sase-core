//! PyO3 bindings for `sase_core`.
//!
//! Phase 1D exposed `parse_project_bytes`. Phase 2D adds the query
//! tokenizer/parser/evaluator behind:
//!
//! - `tokenize_query(query: str) -> list[dict]`
//! - `parse_query(query: str) -> dict`
//! - `canonicalize_query(query: str) -> str`
//! - `evaluate_query_many(query: str, specs: list[dict]) -> list[bool]`
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

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use sase_core::query::types::{QueryErrorWire, QueryExprWire};
use sase_core::wire::ChangeSpecWire;
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
    Ok(())
}

pub use sase_core as core;
