//! `plan_decisions_*` bindings: frozen definitions, digest, resolution.

use std::collections::BTreeMap;

use super::plan_result_to_py;
use crate::json_bridge::py_to_json_value;
use crate::prelude::*;
use pyo3::wrap_pyfunction;
use sase_core::plan::{
    plan_decisions_digest as core_plan_decisions_digest,
    plan_decisions_payload as core_plan_decisions_payload,
    plan_decisions_resolve as core_plan_decisions_resolve,
    PlanDecisionDefinitionWire, PlanDecisionHostFactWire, ValidatedPlanWire,
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

/// Register the decision resolution bindings on the extension module.
pub(crate) fn register_decisions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_plan_decisions_payload, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_digest, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_decisions_resolve, m)?)?;
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
