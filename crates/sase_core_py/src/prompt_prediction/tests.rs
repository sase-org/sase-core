use super::*;
use crate::json_bridge::py_to_json_value;
use crate::sase_core_rs;

const ROWS_JSON: &str = r#"[
{"text": "help me implement the plan", "epoch_seconds": 100, "project": "sase", "origin": "typed", "cancelled": false},
{"text": "help me implement the fix", "epoch_seconds": 101, "project": "sase", "origin": "typed", "cancelled": false},
{"text": "help me implement the docs", "epoch_seconds": 102, "project": "sase", "origin": "typed", "cancelled": false},
{"text": "help me implement the test", "epoch_seconds": 104, "project": "sase", "origin": "typed", "cancelled": false},
{"text": "help me implement the code", "epoch_seconds": 105, "project": "sase", "origin": "typed", "cancelled": false},
{"text": "can you help me review this", "epoch_seconds": 103, "project": "sase", "origin": "typed", "cancelled": false}
]"#;

const OPTIONS_JSON: &str = r#"{"schema_version": 1, "now_epoch": 200}"#;

const CONFIG_JSON: &str = r#"{"schema_version": 1}"#;

fn compile_corpus<'py>(module: &Bound<'py, PyModule>) -> Bound<'py, PyAny> {
    let cls = module.getattr("PromptPredictionCorpus").unwrap();
    cls.call1((ROWS_JSON, OPTIONS_JSON)).unwrap()
}

#[test]
fn prompt_prediction_classes_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("PromptPredictionCorpus").is_ok());
        assert!(module.getattr("PromptPredictionModel").is_ok());
        let version: u32 = module
            .call_method0("prompt_prediction_wire_schema_version")
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(
            version,
            sase_core::prompt_prediction::PROMPT_PREDICTION_WIRE_SCHEMA_VERSION
        );
    });
}

#[test]
fn prompt_prediction_corpus_compiles_and_reports_stats() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let corpus = compile_corpus(&module);
        let schema: u32 =
            corpus.getattr("SCHEMA_VERSION").unwrap().extract().unwrap();
        assert_eq!(schema, 1);
        assert!(
            corpus
                .call_method0("__len__")
                .unwrap()
                .extract::<usize>()
                .unwrap()
                > 0
        );
        let stats = corpus.call_method0("stats").unwrap();
        let value = py_to_json_value(stats.as_any()).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["rows_used"], 6);
        assert_eq!(value["rows_generated_skipped"], 0);

        let bad = module
            .getattr("PromptPredictionCorpus")
            .unwrap()
            .call1(("not json", OPTIONS_JSON));
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));

        let bad = module
            .getattr("PromptPredictionCorpus")
            .unwrap()
            .call1((ROWS_JSON, "not json"));
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));
    });
}

fn compose_model<'py>(
    py: Python<'py>,
    module: &Bound<'py, PyModule>,
    corpus: &Bound<'py, PyAny>,
) -> Bound<'py, PyAny> {
    let cls = module.getattr("PromptPredictionModel").unwrap();
    let sources = PyList::new_bound(py, [(corpus.clone(), "history", 1.0)]);
    cls.call1((sources, CONFIG_JSON)).unwrap()
}

#[test]
fn prompt_prediction_model_predicts_and_ranks() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let corpus = compile_corpus(&module);
        let model_cls = module.getattr("PromptPredictionModel").unwrap();
        let schema: u32 = model_cls
            .getattr("SCHEMA_VERSION")
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(schema, 1);
        let model = compose_model(py, &module, &corpus);

        let request = serde_json::json!({
            "schema_version": 1,
            "text_before_cursor": "help me implement",
            "project": "sase",
            "limit": 5,
            "max_words": 4,
            "confidence": "balanced",
            "include_draft": true,
        });
        let result = model
            .call_method1("predict", (request.to_string(),))
            .unwrap();
        let value = py_to_json_value(result.as_any()).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert!(value["blocked_reason"].is_null());
        assert_eq!(value["confident"], true);
        assert_eq!(value["ghost"][0], "the");
        assert!(!value["candidates"].as_array().unwrap().is_empty());

        let blocked = model
            .call_method1(
                "predict",
                (serde_json::json!({
                    "schema_version": 1,
                    "text_before_cursor": "",
                    "project": null,
                    "limit": 5,
                    "max_words": 4,
                    "confidence": "balanced",
                    "include_draft": true,
                })
                .to_string(),),
            )
            .unwrap();
        let value = py_to_json_value(blocked.as_any()).unwrap();
        assert_eq!(value["confident"], false);
        assert!(value["blocked_reason"].is_string());

        let ranked = model
            .call_method1(
                "rank_prefix",
                (serde_json::json!({
                    "schema_version": 1,
                    "text_before_word": "help me ",
                    "prefix": "imp",
                    "project": "sase",
                    "limit": 5,
                })
                .to_string(),),
            )
            .unwrap();
        let value = py_to_json_value(ranked.as_any()).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["matches"][0]["key"], "implement");

        let bad = model.call_method1("predict", ("not json",));
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn prompt_prediction_replay_reports_aggregates_only() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let report = module
            .call_method1(
                "evaluate_prompt_prediction_replay",
                (ROWS_JSON, OPTIONS_JSON),
            )
            .unwrap();
        let value = py_to_json_value(report.as_any()).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["rows_total"], 6);
        assert_eq!(value["rows_typed"], 6);
        // 40% of 6 rows warm the builder; the rest are scored.
        assert_eq!(value["rows_warmed"], 2);
        assert_eq!(value["rows_scored"], 4);
        assert!(value["positions_total"].as_u64().unwrap() > 0);
        assert_eq!(value["cohorts"].as_array().unwrap().len(), 3);
        assert!(!value["sweep"].as_array().unwrap().is_empty());
        assert!(value["corpus_bytes"].as_u64().unwrap() > 0);
        // Aggregates only: no prompt text leaves the evaluator.
        let serialized = value.to_string();
        assert!(!serialized.contains("help me implement the plan"));

        let bad = module.call_method1(
            "evaluate_prompt_prediction_replay",
            ("not json", OPTIONS_JSON),
        );
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));

        let bad = module.call_method1(
            "evaluate_prompt_prediction_replay",
            (ROWS_JSON, "not json"),
        );
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn prompt_prediction_model_rejects_unknown_role() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let corpus = compile_corpus(&module);
        let cls = module.getattr("PromptPredictionModel").unwrap();
        let sources = PyList::new_bound(py, [(corpus.clone(), "draft", 1.0)]);
        let bad = cls.call1((sources, CONFIG_JSON));
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));

        let sources = PyList::new_bound(py, [(corpus, "history", f64::NAN)]);
        let bad = cls.call1((sources, CONFIG_JSON));
        assert!(bad.is_err());
        assert!(bad.unwrap_err().is_instance_of::<PyValueError>(py));
    });
}
