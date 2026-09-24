//! Command-line grammar bindings: frozen resolver for the `:` panel.

use crate::json_bridge::{py_to_json_value, serialize_to_py};
use crate::prelude::*;

#[pyclass(name = "CommandLineGrammar", module = "sase_core_rs", frozen)]
struct PyCommandLineGrammar {
    grammar: sase_core::command_line::CommandLineGrammar,
}

#[pymethods]
impl PyCommandLineGrammar {
    #[classattr]
    const SCHEMA_VERSION: u32 =
        sase_core::command_line::COMMAND_LINE_WIRE_SCHEMA_VERSION;

    #[new]
    fn new(py: Python<'_>, spec_json: &str) -> PyResult<Self> {
        let owned = spec_json.to_owned();
        let grammar = py
            .allow_threads(move || {
                sase_core::command_line::CommandLineGrammar::from_json(&owned)
            })
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { grammar })
    }

    fn resolve(
        &self,
        py: Python<'_>,
        line: &str,
        cursor: usize,
    ) -> PyResult<PyObject> {
        let out = self.grammar.resolve(line, cursor);
        serialize_to_py(py, &out)
    }

    #[pyo3(signature = (line, cursor, dynamic = None, selected = None, limit = 100))]
    fn complete(
        &self,
        py: Python<'_>,
        line: &str,
        cursor: usize,
        dynamic: Option<&Bound<'_, PyAny>>,
        selected: Option<&Bound<'_, PyAny>>,
        limit: usize,
    ) -> PyResult<PyObject> {
        let dynamic = dynamic_candidates_from_py(dynamic)?;
        let selected = selected_strings_from_py(selected)?;
        let out = self
            .grammar
            .complete(line, cursor, &dynamic, &selected, limit);
        serialize_to_py(py, &out)
    }

    fn command_help(
        &self,
        py: Python<'_>,
        path: Vec<String>,
    ) -> PyResult<Option<PyObject>> {
        self.grammar
            .command_help_path(&path)
            .map(|help| serialize_to_py(py, &help))
            .transpose()
    }

    fn __len__(&self) -> usize {
        self.grammar.command_count()
    }
}

fn dynamic_candidates_from_py(
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<sase_core::command_line::DynamicCandidateWire>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_none() {
        return Ok(Vec::new());
    }
    let json = py_to_json_value(value)?;
    serde_json::from_value(json).map_err(|error| {
        PyValueError::new_err(format!(
            "dynamic is not a list of DynamicCandidateWire dicts: {error}"
        ))
    })
}

fn selected_strings_from_py(
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_none() {
        return Ok(Vec::new());
    }
    let json = py_to_json_value(value)?;
    serde_json::from_value(json).map_err(|error| {
        PyValueError::new_err(format!("selected is not a list of str: {error}"))
    })
}

pub(crate) fn register_command_line(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCommandLineGrammar>()?;
    Ok(())
}

#[cfg(test)]
mod tests;
