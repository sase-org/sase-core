//! `plan_decisions_*` and `plan_decision_quote_match` bindings:
//! frozen definitions, digest, resolution, and human quote matching.

use std::collections::BTreeMap;

use super::plan_result_to_py;
use crate::json_bridge::py_to_json_value;
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::plan::{
    plan_decision_quote_match as core_plan_decision_quote_match,
    plan_decisions_digest as core_plan_decisions_digest,
    plan_decisions_payload as core_plan_decisions_payload,
    plan_decisions_resolve as core_plan_decisions_resolve,
    PlanDecisionDefinitionWire, PlanDecisionHostFactWire,
    PlanDecisionQuoteTextWire, ValidatedPlanWire,
};

/// Freeze a validated plan's decisions into an ordered review vector.
///
/// `validated` is the normalized validated plan object (the `plan` member
/// of a `plan_validate` result, not the outer envelope). `host_facts` maps
/// decision ids to `{requested_verified, provenance, resolved}` records.
#[pyfunction]
#[pyo3(name = "plan_decisions_payload")]
fn py_plan_decisions_payload<'py>(
    py: Python<'py>,
    validated: &Bound<'_, PyAny>,
    host_facts: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let validated_value = py_to_json_value(validated)?;
    let validated: ValidatedPlanWire = serde_json::from_value(validated_value)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "invalid validated plan payload: {error}"
            ))
        })?;
    let facts_value = py_to_json_value(host_facts)?;
    let facts: BTreeMap<String, PlanDecisionHostFactWire> =
        serde_json::from_value(facts_value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid host facts payload: {error}"
            ))
        })?;
    plan_result_to_py(py, core_plan_decisions_payload(&validated, &facts))
}

/// Digest frozen definitions for the review edit freeze.
#[pyfunction]
#[pyo3(name = "plan_decisions_digest")]
fn py_plan_decisions_digest<'py>(
    py: Python<'py>,
    definitions: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(definitions)?;
    let definitions: Vec<PlanDecisionDefinitionWire> =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid decision definitions payload: {error}"
            ))
        })?;
    plan_result_to_py(py, core_plan_decisions_digest(&definitions))
}

/// Strictly resolve one accepted answer vector.
///
/// `caller` is `human`, `agent`, or `auto`. Unknown callers and non-object
/// submissions fail as binding errors, never as human callers.
#[pyfunction]
#[pyo3(name = "plan_decisions_resolve")]
fn py_plan_decisions_resolve<'py>(
    py: Python<'py>,
    definitions: &Bound<'_, PyAny>,
    submitted: &Bound<'_, PyAny>,
    caller: &str,
) -> PyResult<PyObject> {
    let value = py_to_json_value(definitions)?;
    let definitions: Vec<PlanDecisionDefinitionWire> =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid decision definitions payload: {error}"
            ))
        })?;
    let submitted = py_to_json_value(submitted)?;
    plan_result_to_py(
        py,
        core_plan_decisions_resolve(&definitions, &submitted, caller),
    )
}

/// Verify a human quote against ordered human-authored texts.
///
/// `texts` is a list of `{source, ref, text}` records; plain strings
/// are accepted as legacy text-only entries, and `None` means no
/// texts. Malformed entries fail as binding errors, never as panics.
#[pyfunction]
#[pyo3(name = "plan_decision_quote_match")]
fn py_plan_decision_quote_match<'py>(
    py: Python<'py>,
    quote: &str,
    texts: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let texts = quote_texts_from_py(texts)?;
    plan_result_to_py(py, Ok(core_plan_decision_quote_match(quote, &texts)))
}

fn quote_texts_from_py(
    texts: &Bound<'_, PyAny>,
) -> PyResult<Vec<PlanDecisionQuoteTextWire>> {
    if texts.is_none() {
        return Ok(Vec::new());
    }
    let items = texts.extract::<Vec<PyObject>>().map_err(|_| {
        PyValueError::new_err(
            "quote texts must be a list of {source, ref, text} records",
        )
    })?;
    items
        .iter()
        .map(|item| {
            let item = item.bind(texts.py());
            if let Ok(text) = item.extract::<String>() {
                return Ok(PlanDecisionQuoteTextWire {
                    source: String::new(),
                    r#ref: String::new(),
                    text,
                });
            }
            let value = py_to_json_value(item)?;
            serde_json::from_value(value).map_err(|error| {
                PyValueError::new_err(format!(
                    "invalid quote text record: {error}"
                ))
            })
        })
        .collect()
}

/// Register the decision resolution bindings on the extension module.
pub(crate) fn register_decisions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_plan_decisions_payload, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decision_quote_match, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_bridge::json_value_to_py;
    use crate::sase_core_rs;
    use serde_json::json;

    fn validated_value() -> serde_json::Value {
        json!({
            "tier": "tale",
            "goal": "G",
            "size": "small",
            "model": null,
            "title": "T",
            "phases": [],
            "changespec": null,
            "bug_id": null,
            "parent_bead": null,
            "bead": null,
            "proposed_by": null,
            "parent": null,
            "decisions": [
                {
                    "id": "grouping",
                    "kind": "choice",
                    "ask": "How to group?",
                    "why": "Keeps order",
                    "choices": [
                        {"key": "pane", "label": "By pane"},
                        {"key": "mode", "label": "By mode"}
                    ],
                    "default": "pane"
                },
                {
                    "id": "tui_note",
                    "kind": "toggle",
                    "ask": "Edit the note?",
                    "choices": [],
                    "default": true,
                    "memory": {"selectors": ["tui.md"]},
                    "requested": "and note the convention in the tui memory"
                }
            ],
            "decision_callouts": [],
            "decided_by": null,
            "decided_via": null
        })
    }

    fn facts_value() -> serde_json::Value {
        json!({
            "tui_note": {
                "requested_verified": true,
                "provenance": "asked",
                "resolved": [
                    {
                        "selector": "tui.md",
                        "kind": "note",
                        "scope": "project",
                        "path": "sase/memory/tui.md",
                        "type": "reference",
                        "exists": true
                    }
                ]
            }
        })
    }

    fn to_object(py: Python<'_>, value: &serde_json::Value) -> PyObject {
        json_value_to_py(py, value).unwrap()
    }

    fn from_object(py: Python<'_>, object: &PyObject) -> serde_json::Value {
        py_to_json_value(object.bind(py)).unwrap()
    }

    fn payload(
        py: Python<'_>,
        validated: &serde_json::Value,
        facts: &serde_json::Value,
    ) -> serde_json::Value {
        let validated = to_object(py, validated);
        let facts = to_object(py, facts);
        let out =
            py_plan_decisions_payload(py, validated.bind(py), facts.bind(py))
                .unwrap();
        from_object(py, &out)
    }

    fn digest(
        py: Python<'_>,
        definitions: &serde_json::Value,
    ) -> serde_json::Value {
        let definitions = to_object(py, definitions);
        let out = py_plan_decisions_digest(py, definitions.bind(py)).unwrap();
        from_object(py, &out)
    }

    fn quote_match(
        py: Python<'_>,
        quote: &str,
        texts: &serde_json::Value,
    ) -> serde_json::Value {
        let texts = to_object(py, texts);
        let out =
            py_plan_decision_quote_match(py, quote, texts.bind(py).as_any())
                .unwrap();
        from_object(py, &out)
    }

    fn resolve(
        py: Python<'_>,
        definitions: &serde_json::Value,
        submitted: &serde_json::Value,
        caller: &str,
    ) -> serde_json::Value {
        let definitions = to_object(py, definitions);
        let submitted = to_object(py, submitted);
        let out = py_plan_decisions_resolve(
            py,
            definitions.bind(py),
            submitted.bind(py),
            caller,
        )
        .unwrap();
        from_object(py, &out)
    }

    #[test]
    fn decision_bindings_are_registered_and_round_trip() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("plan_decisions_payload").is_ok());
            assert!(module.getattr("plan_decisions_digest").is_ok());
            assert!(module.getattr("plan_decisions_resolve").is_ok());

            let definitions = payload(py, &validated_value(), &facts_value());
            assert_eq!(definitions.as_array().unwrap().len(), 2);
            assert_eq!(definitions[1]["effective_default"], json!(true));

            // The digest of the frozen vector is a 64-hex SHA-256.
            let digest = digest(py, &definitions);
            assert_eq!(digest.as_str().unwrap().len(), 64);

            // Ordinary toggles and choices resolve; overrides are accepted.
            let resolved = resolve(
                py,
                &definitions,
                &json!({"grouping": "MODE", "tui_note": true}),
                "human",
            );
            assert_eq!(
                resolved["values"],
                json!({"grouping": "mode", "tui_note": true})
            );
            assert!(resolved["errors"].as_array().unwrap().is_empty());

            // The agent boundary holds through the binding too.
            let clamped_defs = payload(
                py,
                &validated_value(),
                &json!({
                    "tui_note": {
                        "requested_verified": false,
                        "provenance": "quote_not_found",
                        "resolved": []
                    }
                }),
            );
            assert_eq!(clamped_defs[1]["effective_default"], json!(false));
            let refused =
                resolve(py, &clamped_defs, &json!({"tui_note": true}), "agent");
            assert_eq!(
                refused["errors"][0]["code"],
                json!("memory_decision_requires_human")
            );
            assert_eq!(refused["values"], json!({}));
        });
    }

    fn quote_texts() -> serde_json::Value {
        json!([
            {
                "source": "chat",
                "ref": "m1",
                "text": "Nothing relevant here at all."
            },
            {
                "source": "note",
                "ref": "tui.md",
                "text": "Please note the convention in the tui memory."
            }
        ])
    }

    #[test]
    fn quote_match_binding_is_registered_and_round_trips() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("plan_decision_quote_match").is_ok());

            // The contiguous run verifies and keeps source identity.
            let matched = quote_match(
                py,
                "note the convention in the tui memory",
                &quote_texts(),
            );
            assert_eq!(matched["verified"], json!(true));
            assert_eq!(matched["matched_source"]["source"], json!("note"));
            assert_eq!(matched["matched_source"]["ref"], json!("tui.md"));
            assert!(matched.get("closest").is_none());

            // The three-word boundary holds through the binding.
            let short = quote_match(py, "note the", &quote_texts());
            assert_eq!(short["verified"], json!(false));
            assert!(short.get("matched_source").is_none());
            assert_eq!(
                short["closest"]["sentence"],
                json!("Please note the convention in the tui memory.")
            );

            // Noncontiguous halves and partial words never verify.
            let split = quote_match(
                py,
                "relevant here note the convention",
                &quote_texts(),
            );
            assert_eq!(split["verified"], json!(false));

            // Two texts holding separate halves still fail: no joining.
            let halves = quote_match(
                py,
                "relevant here at all please note",
                &json!([
                    {"source": "a", "ref": "1", "text": "Relevant here at all."},
                    {"source": "b", "ref": "2", "text": "Please note this."}
                ]),
            );
            assert_eq!(halves["verified"], json!(false));

            // Suggestion ties stay stable by input order.
            let tied = quote_match(
                py,
                "missing quote tokens entirely here",
                &json!([
                    {
                        "source": "b",
                        "ref": "2",
                        "text": "Unrelated filler words here."
                    },
                    {
                        "source": "a",
                        "ref": "1",
                        "text": "Other filler words here."
                    }
                ]),
            );
            assert_eq!(tied["closest"]["source"], json!("b"));

            // Empty and legacy text arrays without the run verify nothing.
            for texts in [json!([]), json!(["something else entirely here"])] {
                let out = quote_match(py, "note the convention", &texts);
                assert_eq!(out["verified"], json!(false));
            }
            // A legacy plain-string array still matches a real run.
            let legacy = quote_match(
                py,
                "note the convention",
                &json!(["please note the convention today"]),
            );
            assert_eq!(legacy["verified"], json!(true));

            // None means no texts.
            let none = to_object(py, &serde_json::Value::Null);
            let out = py_plan_decision_quote_match(
                py,
                "note the convention",
                none.bind(py).as_any(),
            )
            .unwrap();
            assert_eq!(from_object(py, &out)["verified"], json!(false));
        });
    }

    #[test]
    fn quote_match_binding_rejects_malformed_records() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // A non-list is a binding error, not a panic.
            let bad = to_object(py, &json!({"text": "note the convention"}));
            py_plan_decision_quote_match(
                py,
                "note the convention",
                bad.bind(py).as_any(),
            )
            .expect_err("non-list texts must fail");

            // A record missing its text is a binding error too.
            let bad =
                to_object(py, &json!([{"source": "note", "ref": "tui.md"}]));
            py_plan_decision_quote_match(
                py,
                "note the convention",
                bad.bind(py).as_any(),
            )
            .expect_err("textless record must fail");
        });
    }

    #[test]
    fn decision_bindings_reject_invalid_input_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();

            // A malformed validated plan is a binding error, not a panic.
            let bad = to_object(py, &json!({"tier": 7}));
            let facts = to_object(py, &facts_value());
            py_plan_decisions_payload(py, bad.bind(py), facts.bind(py))
                .expect_err("malformed validated plan must fail");

            // Unknown callers are usage errors, never human callers.
            let definitions = payload(py, &validated_value(), &facts_value());
            let defs = to_object(py, &definitions);
            let submitted = to_object(py, &json!({}));
            py_plan_decisions_resolve(
                py,
                defs.bind(py),
                submitted.bind(py),
                "robot",
            )
            .expect_err("unknown caller must fail");
        });
    }
}
