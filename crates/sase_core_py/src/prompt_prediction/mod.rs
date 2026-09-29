//! Next-word prompt prediction bindings: frozen corpus and model.
//!
//! The corpus compiles typed prompt rows off the event loop with the GIL
//! released; the model cheaply composes frozen corpora and answers
//! per-keystroke `predict` / `rank_prefix` queries synchronously. Dict
//! shapes mirror the Python wire dataclasses in
//! `src/sase/core/prompt_prediction_wire.py` (rectangular, all fields
//! always present); inputs arrive as JSON strings so the parse errors stay
//! `ValueError`s like the neighbouring domains.

use std::sync::Arc;

use crate::json_bridge::serialize_to_py;
use crate::prelude::*;
use pyo3::wrap_pyfunction;

#[pyclass(name = "PromptPredictionCorpus", module = "sase_core_rs", frozen)]
struct PyPromptPredictionCorpus {
    corpus: Arc<sase_core::prompt_prediction::CompiledPromptPredictionCorpus>,
}

#[pymethods]
impl PyPromptPredictionCorpus {
    #[classattr]
    const SCHEMA_VERSION: u32 =
        sase_core::prompt_prediction::PROMPT_PREDICTION_WIRE_SCHEMA_VERSION;

    #[new]
    fn new(
        py: Python<'_>,
        rows_json: &str,
        options_json: &str,
    ) -> PyResult<Self> {
        let rows: Vec<sase_core::prompt_prediction::PromptPredictionRowWire> =
            serde_json::from_str(rows_json).map_err(|error| {
                PyValueError::new_err(format!(
                "rows is not a list of PromptPredictionRowWire dicts: {error}"
            ))
            })?;
        let options: sase_core::prompt_prediction::PromptPredictionCorpusOptionsWire =
            serde_json::from_str(options_json).map_err(|error| {
                PyValueError::new_err(format!(
                    "options is not a PromptPredictionCorpusOptionsWire dict: {error}"
                ))
            })?;
        let corpus = py.allow_threads(move || {
            sase_core::prompt_prediction::CompiledPromptPredictionCorpus::compile(
                &rows, &options,
            )
        });
        Ok(Self {
            corpus: Arc::new(corpus),
        })
    }

    fn stats(&self, py: Python<'_>) -> PyResult<PyObject> {
        serialize_to_py(py, &self.corpus.stats())
    }

    fn __len__(&self) -> usize {
        self.corpus.vocab_len()
    }
}

#[pyclass(name = "PromptPredictionModel", module = "sase_core_rs", frozen)]
struct PyPromptPredictionModel {
    model: sase_core::prompt_prediction::PromptPredictionModel,
}

#[pymethods]
impl PyPromptPredictionModel {
    #[classattr]
    const SCHEMA_VERSION: u32 =
        sase_core::prompt_prediction::PROMPT_PREDICTION_WIRE_SCHEMA_VERSION;

    #[new]
    fn new(
        sources: Vec<(Bound<'_, PyPromptPredictionCorpus>, String, f64)>,
        config_json: &str,
    ) -> PyResult<Self> {
        let mut composed = Vec::with_capacity(sources.len());
        for (handle, role_name, weight) in &sources {
            let role =
                sase_core::prompt_prediction::PromptPredictionSourceRole::parse(
                    role_name,
                )
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "unknown prompt prediction source role: {role_name:?}"
                    ))
                })?;
            if !weight.is_finite() {
                return Err(PyValueError::new_err(format!(
                    "source weight for role {role_name:?} is not finite: {weight}"
                )));
            }
            composed.push((
                Arc::clone(&handle.get().corpus),
                role,
                *weight as f32,
            ));
        }
        let config: sase_core::prompt_prediction::PromptPredictionModelConfigWire =
            serde_json::from_str(config_json).map_err(|error| {
                PyValueError::new_err(format!(
                    "config is not a PromptPredictionModelConfigWire dict: {error}"
                ))
            })?;
        Ok(Self {
            model: sase_core::prompt_prediction::PromptPredictionModel::new(
                composed, config,
            ),
        })
    }

    fn predict(
        &self,
        py: Python<'_>,
        request_json: &str,
    ) -> PyResult<PyObject> {
        let request: sase_core::prompt_prediction::PromptPredictionRequestWire =
            serde_json::from_str(request_json).map_err(|error| {
                PyValueError::new_err(format!(
                    "request is not a PromptPredictionRequestWire dict: {error}"
                ))
            })?;
        serialize_to_py(py, &self.model.predict(&request))
    }

    fn rank_prefix(
        &self,
        py: Python<'_>,
        request_json: &str,
    ) -> PyResult<PyObject> {
        let request: sase_core::prompt_prediction::PromptPrefixRankRequestWire =
            serde_json::from_str(request_json).map_err(|error| {
                PyValueError::new_err(format!(
                    "request is not a PromptPrefixRankRequestWire dict: {error}"
                ))
            })?;
        serialize_to_py(py, &self.model.rank_prefix(&request))
    }
}

/// Return the prompt prediction wire schema version.
#[pyfunction]
#[pyo3(name = "prompt_prediction_wire_schema_version")]
fn py_prompt_prediction_wire_schema_version() -> u32 {
    sase_core::prompt_prediction::PROMPT_PREDICTION_WIRE_SCHEMA_VERSION
}

pub(crate) fn register_prompt_prediction(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_class::<PyPromptPredictionCorpus>()?;
    m.add_class::<PyPromptPredictionModel>()?;
    m.add_function(wrap_pyfunction!(
        py_prompt_prediction_wire_schema_version,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
