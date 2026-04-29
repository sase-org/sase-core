//! PyO3 binding for `sase_core::parse_project_bytes`.
//!
//! Phase 1D exposes a single stateless function importable from Python as
//! `sase_core_rs.parse_project_bytes(path: str, data: bytes) -> list[dict]`.
//!
//! The result is a list of plain Python dicts whose keys and value types
//! mirror the JSON shape of `ChangeSpecWire` (see
//! `sase_100/src/sase/core/wire.py`). No PyO3 classes are exported in
//! Phase 1 — the Python adapter rebuilds typed `ChangeSpecWire`
//! dataclasses from the dicts at the boundary.
//!
//! A Rust `ParseErrorWire` is surfaced as a Python `ValueError` whose
//! message is the wire error's `Display` form (`kind: message
//! (file_path)`). The Python adapter does not need structured fields in
//! Phase 1; if that changes, this is the right place to grow a custom
//! Python exception class.

// `pyo3::pyfunction` macro expansion contains a `From::from` for `PyErr`
// that clippy 1.95+ reports as `useless_conversion`. The annotation has
// to live at the module scope because the macro generates wrapper code
// outside the user-written function body.
#![allow(clippy::useless_conversion)]

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
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
    Ok(())
}

pub use sase_core as core;
