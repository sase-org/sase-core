//! `plan_decisions_*`, `plan_decision_quote_match`, `plan_decision_sheet`,
//! `plan_decision_summary`, and `plan_decisions_prompt_block` bindings:
//! frozen definitions, digest, resolution, human quote matching, the
//! Decision Sheet, the shared summary sentence, and the implementer
//! block.

use std::collections::BTreeMap;

use super::plan_result_to_py;
use crate::json_bridge::py_to_json_value;
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::plan::{
    plan_decision_quote_match as core_plan_decision_quote_match,
    plan_decision_sheet as core_plan_decision_sheet,
    plan_decision_summary as core_plan_decision_summary,
    plan_decisions_digest as core_plan_decisions_digest,
    plan_decisions_payload as core_plan_decisions_payload,
    plan_decisions_prompt_block as core_plan_decisions_prompt_block,
    plan_decisions_resolve as core_plan_decisions_resolve,
    PlanDecisionDefinitionWire, PlanDecisionHostFactWire,
    PlanDecisionInheritedWire, PlanDecisionQuoteTextWire,
    PlanDecisionSheetWire, ValidatedPlanWire,
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

/// Build the Decision Sheet from frozen definitions and an accepted
/// answer vector.
///
/// `definitions` is the frozen review vector, `values` maps each id
/// to its canonical value, and `review_revision` is the displayed
/// revision the approval refers to.
#[pyfunction]
#[pyo3(name = "plan_decision_sheet")]
fn py_plan_decision_sheet<'py>(
    py: Python<'py>,
    definitions: &Bound<'_, PyAny>,
    values: &Bound<'_, PyAny>,
    review_revision: u64,
) -> PyResult<PyObject> {
    let value = py_to_json_value(definitions)?;
    let definitions: Vec<PlanDecisionDefinitionWire> =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid decision definitions payload: {error}"
            ))
        })?;
    let values = py_to_json_value(values)?;
    plan_result_to_py(
        py,
        core_plan_decision_sheet(&definitions, &values, review_revision),
    )
}

/// Render the one summary sentence every surface shares.
///
/// `verdict` is `coder + commit`, `coder`, `commit`, or `epic launch`;
/// `form` is `short` or `full`. Unknown verdicts and forms fail as
/// binding errors, never as interpolated text.
#[pyfunction]
#[pyo3(name = "plan_decision_summary")]
fn py_plan_decision_summary<'py>(
    py: Python<'py>,
    sheet: &Bound<'_, PyAny>,
    verdict: &str,
    form: &str,
) -> PyResult<PyObject> {
    let value = py_to_json_value(sheet)?;
    let sheet: PlanDecisionSheetWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid decision sheet payload: {error}"
            ))
        })?;
    plan_result_to_py(py, core_plan_decision_summary(&sheet, verdict, form))
}

/// Render the host-owned implementer block.
///
/// `decided_by` is `reviewer`, `auto`, or `agent`; `decided_via` is a
/// review surface (`tui`, `telegram`, `mobile`, or `cli`), absent for
/// auto. `audience` is `tale_coder`, `epic_phase`, or `epic_land`;
/// `inherited` is an optional `{sheet, epic_title}` record carrying
/// the epic's accepted authorization.
#[pyfunction]
#[pyo3(name = "plan_decisions_prompt_block")]
fn py_plan_decisions_prompt_block<'py>(
    py: Python<'py>,
    sheet: &Bound<'_, PyAny>,
    decided_by: &str,
    decided_via: &Bound<'_, PyAny>,
    audience: &str,
    inherited: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(sheet)?;
    let sheet: PlanDecisionSheetWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "invalid decision sheet payload: {error}"
            ))
        })?;
    // `None` means no transport and no inherited grants; anything else
    // must be a surface string and a `{sheet, epic_title}` record.
    // Malformed entries fail as binding errors, never as panics.
    let via: Option<String> = if decided_via.is_none() {
        None
    } else {
        Some(decided_via.extract::<String>().map_err(|_| {
            PyValueError::new_err(
                "decided_via must be tui, telegram, mobile, cli, or None",
            )
        })?)
    };
    let inherited_wire: Option<PlanDecisionInheritedWire> =
        if inherited.is_none() {
            None
        } else {
            let inherited_value = py_to_json_value(inherited)?;
            Some(serde_json::from_value(inherited_value).map_err(|error| {
                PyValueError::new_err(format!(
                    "invalid inherited decisions payload: {error}"
                ))
            })?)
        };
    plan_result_to_py(
        py,
        core_plan_decisions_prompt_block(
            &sheet,
            decided_by,
            via.as_deref(),
            audience,
            inherited_wire.as_ref(),
        ),
    )
}

/// Register the decision bindings on the extension module.
pub(crate) fn register_decisions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_plan_decisions_payload, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decision_quote_match, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decision_sheet, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decision_summary, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_prompt_block, m)?)?;
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

    fn sheet(
        py: Python<'_>,
        definitions: &serde_json::Value,
        values: &serde_json::Value,
        review_revision: u64,
    ) -> serde_json::Value {
        let definitions = to_object(py, definitions);
        let values = to_object(py, values);
        let out = py_plan_decision_sheet(
            py,
            definitions.bind(py),
            values.bind(py),
            review_revision,
        )
        .unwrap();
        from_object(py, &out)
    }

    fn summary(
        py: Python<'_>,
        sheet: &serde_json::Value,
        verdict: &str,
        form: &str,
    ) -> String {
        let sheet = to_object(py, sheet);
        let out = py_plan_decision_summary(py, sheet.bind(py), verdict, form)
            .unwrap();
        from_object(py, &out).as_str().unwrap().to_string()
    }

    #[allow(clippy::too_many_arguments)]
    fn block(
        py: Python<'_>,
        sheet: &serde_json::Value,
        decided_by: &str,
        decided_via: Option<&str>,
        audience: &str,
        inherited: Option<&serde_json::Value>,
    ) -> String {
        let sheet_arg = to_object(py, sheet);
        let via_arg = match decided_via {
            Some(via) => to_object(py, &json!(via)),
            None => to_object(py, &serde_json::Value::Null),
        };
        let owned = inherited.map(|record| to_object(py, record));
        let none_arg = to_object(py, &serde_json::Value::Null);
        let inherited_arg = owned.as_ref().unwrap_or(&none_arg);
        let out = py_plan_decisions_prompt_block(
            py,
            sheet_arg.bind(py),
            decided_by,
            via_arg.bind(py),
            audience,
            inherited_arg.bind(py),
        )
        .unwrap();
        from_object(py, &out).as_str().unwrap().to_string()
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

    #[test]
    fn sheet_summary_and_block_bindings_round_trip() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("plan_decision_sheet").is_ok());
            assert!(module.getattr("plan_decision_summary").is_ok());
            assert!(module.getattr("plan_decisions_prompt_block").is_ok());

            let definitions = payload(py, &validated_value(), &facts_value());
            let sheet = sheet(
                py,
                &definitions,
                &json!({"grouping": "mode", "tui_note": true}),
                4,
            );
            assert_eq!(sheet["count"], json!(2));
            assert_eq!(sheet["memory_count"], json!(1));
            assert_eq!(sheet["changed_count"], json!(1));
            assert_eq!(sheet["review_revision"], json!(4));
            assert_eq!(sheet["rows"][0]["default"], json!("pane"));
            assert_eq!(sheet["rows"][0]["value"], json!("mode"));
            assert_eq!(
                sheet["rows"][1]["memory"]["provenance"],
                json!("asked"),
            );
            assert_eq!(
                sheet["rows"][1]["memory"]["quote"],
                json!("and note the convention in the tui memory"),
            );

            assert_eq!(
                summary(py, &sheet, "coder + commit", "full"),
                "→ coder + commit · grouping=mode ● · 🧠 tui.md",
            );
            assert_eq!(summary(py, &sheet, "coder", "short"), "1 change · 🧠",);

            let text =
                block(py, &sheet, "reviewer", Some("tui"), "tale_coder", None);
            assert!(text.starts_with(
                "Reviewer decisions for this plan (final · reviewer \
                 via ACE):",
            ));
            assert!(text.contains(
                "- grouping = mode (planner default: pane). Implement \
                 the \"grouping = mode\" branch;",
            ));
            assert!(text.contains(
                "- tui_note = yes 🧠. Memory edits are authorized for \
                 tui.md only.",
            ));
            assert!(
                text.contains("Implement only the branches selected above.",)
            );

            // An explicit Python None carries no inherited grants.
            let none = to_object(py, &serde_json::Value::Null);
            let via = to_object(py, &json!("tui"));
            let sheet_arg = to_object(py, &sheet);
            let out = py_plan_decisions_prompt_block(
                py,
                sheet_arg.bind(py),
                "reviewer",
                via.bind(py),
                "epic_phase",
                none.bind(py),
            )
            .unwrap();
            let explicit = from_object(py, &out);
            assert!(explicit.as_str().unwrap().contains("reviewer via ACE"));
        });
    }

    #[test]
    fn sheet_bindings_reject_invalid_input_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let definitions = payload(py, &validated_value(), &facts_value());
            let defs = to_object(py, &definitions);

            // A vector missing an answer is a binding error, not a
            // coerced display value.
            let partial = to_object(py, &json!({"grouping": "mode"}));
            py_plan_decision_sheet(py, defs.bind(py), partial.bind(py), 1)
                .expect_err("missing answers must fail");

            let sheet = sheet(
                py,
                &definitions,
                &json!({"grouping": "pane", "tui_note": true}),
                1,
            );
            let sheet_arg = to_object(py, &sheet);

            // Unknown verdicts never interpolate into the sentence.
            py_plan_decision_summary(py, sheet_arg.bind(py), "approve", "full")
                .expect_err("unknown verdicts must fail");

            // Unknown audiences and malformed inherited records fail.
            let via = to_object(py, &json!("tui"));
            let none = to_object(py, &serde_json::Value::Null);
            py_plan_decisions_prompt_block(
                py,
                sheet_arg.bind(py),
                "reviewer",
                via.bind(py),
                "tablet",
                none.bind(py),
            )
            .expect_err("unknown audiences must fail");
            let bad_via = to_object(py, &json!(7));
            py_plan_decisions_prompt_block(
                py,
                sheet_arg.bind(py),
                "reviewer",
                bad_via.bind(py),
                "tale_coder",
                none.bind(py),
            )
            .expect_err("non-string transports must fail");
            let bad_inherited = to_object(py, &json!({"epic_title": 7}));
            py_plan_decisions_prompt_block(
                py,
                sheet_arg.bind(py),
                "reviewer",
                via.bind(py),
                "tale_coder",
                bad_inherited.bind(py),
            )
            .expect_err("malformed inherited records must fail");
        });
    }

    /// Call a registered binding the way the host does: by name on
    /// the initialized module, then read the JSON bridge result back.
    fn call(
        module: &Bound<'_, PyModule>,
        name: &str,
        args: Vec<PyObject>,
    ) -> serde_json::Value {
        let py = module.py();
        let tuple = PyTuple::new_bound(py, args);
        let out = module.getattr(name).unwrap().call1(tuple).unwrap();
        py_to_json_value(&out).unwrap()
    }

    #[test]
    fn seven_binding_decision_integration() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "plan_decisions_payload",
                "plan_decisions_digest",
                "plan_decisions_resolve",
                "plan_decision_quote_match",
                "plan_decision_sheet",
                "plan_decision_summary",
                "plan_decisions_prompt_block",
            ] {
                assert!(module.getattr(name).is_ok(), "{name} is registered",);
            }

            // A tale with a choice and a memory toggle flows through
            // every binding: validate, freeze, digest, resolve, match
            // the quote, sheet, both summaries, implementer block.
            let tale = "---\ntier: tale\ntitle: Keymap help \
                overlay\ngoal: Ship the overlay grouping rule\nsize: \
                small\ndecisions:\n  grouping:\n    ask: How should the \
                overlay group bindings?\n    choices:\n      pane: By \
                pane, matching the footer hints\n      mode: By leader \
                mode\n    default: pane\n    why: Keeps the review \
                short\n  tui_note:\n    ask: Record the overlay \
                conventions in the tui note?\n    default: true\n    \
                memory:\n      - tui.md\n    requested: and note the \
                convention in the tui memory\n---\n# Plan\nShip grouping \
                and tui_note.\n> [!decision] grouping = mode\n> Group by \
                mode.\n> [!decision] tui_note\n> Note the convention.\n";
            let validated = call(
                &module,
                "plan_validate",
                vec![
                    to_object(py, &json!(tale)),
                    to_object(py, &json!("tale")),
                    to_object(py, &json!("authoring")),
                ],
            );
            assert_eq!(validated["ok"], json!(true));
            assert_eq!(
                validated["plan"]["decisions"].as_array().unwrap().len(),
                2
            );

            let facts = json!({
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
            });
            let plan = to_object(py, &validated["plan"]);
            let facts_arg = to_object(py, &facts);
            let definitions =
                call(&module, "plan_decisions_payload", vec![plan, facts_arg]);
            assert_eq!(definitions.as_array().unwrap().len(), 2);

            let digest_arg = to_object(py, &definitions);
            let digest =
                call(&module, "plan_decisions_digest", vec![digest_arg]);
            assert_eq!(digest.as_str().unwrap().len(), 64);

            let resolve_defs = to_object(py, &definitions);
            let submitted =
                to_object(py, &json!({"grouping": "MODE", "tui_note": true}));
            let human = to_object(py, &json!("human"));
            let resolved = call(
                &module,
                "plan_decisions_resolve",
                vec![resolve_defs, submitted, human],
            );
            assert_eq!(
                resolved["values"],
                json!({"grouping": "mode", "tui_note": true}),
            );
            assert!(resolved["errors"].as_array().unwrap().is_empty());

            let texts = to_object(
                py,
                &json!([
                    {
                        "source": "chat",
                        "ref": "m1",
                        "text": "Please review and note the convention \
                         in the tui memory today."
                    }
                ]),
            );
            let quote = to_object(
                py,
                &json!("and note the convention in the tui memory"),
            );
            let matched =
                call(&module, "plan_decision_quote_match", vec![quote, texts]);
            assert_eq!(matched["verified"], json!(true));
            assert_eq!(matched["matched_source"]["ref"], json!("m1"));

            let sheet_defs = to_object(py, &definitions);
            let values = to_object(py, &resolved["values"]);
            let revision = to_object(py, &json!(4));
            let sheet = call(
                &module,
                "plan_decision_sheet",
                vec![sheet_defs, values, revision],
            );
            assert_eq!(sheet["count"], json!(2));
            assert_eq!(sheet["changed_count"], json!(1));

            let full_arg = to_object(py, &sheet);
            let full_verdict = to_object(py, &json!("coder + commit"));
            let full_form = to_object(py, &json!("full"));
            let full = call(
                &module,
                "plan_decision_summary",
                vec![full_arg, full_verdict, full_form],
            );
            assert_eq!(
                full.as_str().unwrap(),
                "→ coder + commit · grouping=mode ● · 🧠 tui.md",
            );
            let short_arg = to_object(py, &sheet);
            let short_form = to_object(py, &json!("short"));
            let short_verdict = to_object(py, &json!("coder + commit"));
            let short = call(
                &module,
                "plan_decision_summary",
                vec![short_arg, short_verdict, short_form],
            );
            assert_eq!(short.as_str().unwrap(), "1 change · 🧠");

            let block_arg = to_object(py, &sheet);
            let reviewer = to_object(py, &json!("reviewer"));
            let via_tui = to_object(py, &json!("tui"));
            let coder = to_object(py, &json!("tale_coder"));
            let none = to_object(py, &serde_json::Value::Null);
            let rendered = call(
                &module,
                "plan_decisions_prompt_block",
                vec![block_arg, reviewer, via_tui, coder, none],
            );
            let rendered = rendered.as_str().unwrap();
            assert!(rendered.contains("reviewer via ACE"));
            assert!(rendered.contains("grouping = mode"));
            assert!(rendered.contains("authorized for tui.md only"));

            // An epic in Archived mode resolves under auto with no
            // memory grants and an honest no-human header.
            let epic = "---\ntier: epic\ntitle: Overlay \
                epic\ngoal: Ship overlays\nparent_bead: \
                sase-1hi\nbead: sase-1hi.9\nparent: \
                sase/repos/plans/202610/parent.md\nphases:\n  - id: \
                core\n    title: Core\n    depends_on: []\n    \
                description: Core work section for the archived epic \
                example.\n    size: small\ndecisions:\n  notify:\n    \
                ask: Send a notification?\n    default: \
                false\n---\n# Plan\nShip notify.\n";
            let archived = call(
                &module,
                "plan_validate",
                vec![
                    to_object(py, &json!(epic)),
                    to_object(py, &json!("epic")),
                    to_object(py, &json!("archived")),
                ],
            );
            assert_eq!(archived["ok"], json!(true));
            let epic_plan = to_object(py, &archived["plan"]);
            let empty_facts = to_object(py, &json!({}));
            let epic_defs = call(
                &module,
                "plan_decisions_payload",
                vec![epic_plan, empty_facts],
            );
            let empty_submitted = to_object(py, &json!({}));
            let auto_defs = to_object(py, &epic_defs);
            let auto_caller = to_object(py, &json!("auto"));
            let auto_resolved = call(
                &module,
                "plan_decisions_resolve",
                vec![auto_defs, empty_submitted, auto_caller],
            );
            assert_eq!(auto_resolved["values"], json!({"notify": false}));
            let auto_values = to_object(py, &auto_resolved["values"]);
            let auto_sheet_defs = to_object(py, &epic_defs);
            let auto_revision = to_object(py, &json!(12));
            let auto_sheet = call(
                &module,
                "plan_decision_sheet",
                vec![auto_sheet_defs, auto_values, auto_revision],
            );
            assert_eq!(auto_sheet["memory_count"], json!(0));
            let auto_short_arg = to_object(py, &auto_sheet);
            let commit = to_object(py, &json!("commit"));
            let short = to_object(py, &json!("short"));
            let auto_short = call(
                &module,
                "plan_decision_summary",
                vec![auto_short_arg, commit, short],
            );
            assert_eq!(auto_short.as_str().unwrap(), "defaults");
            let auto_block_arg = to_object(py, &auto_sheet);
            let auto_none = to_object(py, &serde_json::Value::Null);
            let auto_by = to_object(py, &json!("auto"));
            let auto_for = to_object(py, &json!("tale_coder"));
            let auto_block = call(
                &module,
                "plan_decisions_prompt_block",
                vec![
                    auto_block_arg,
                    auto_by,
                    auto_none.clone_ref(py),
                    auto_for,
                    auto_none,
                ],
            );
            // The same Null converts twice: no transport, no grants.
            let auto_block = auto_block.as_str().unwrap();
            assert!(auto_block.contains("no human reviewed this plan"));
            assert!(!auto_block.contains("No other memory note"));
        });
    }
}
