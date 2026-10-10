use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use pyo3::types::PyDict;
use serde_json::json;

fn py_dict<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> Bound<'py, PyDict> {
    json_value_to_py(py, value)
        .unwrap()
        .bind(py)
        .clone()
        .downcast_into::<PyDict>()
        .unwrap()
}

fn round_trip(py: Python<'_>, result: PyResult<PyObject>) -> serde_json::Value {
    py_to_json_value(result.unwrap().bind(py).as_any()).unwrap()
}

#[test]
fn autonomy_wire_schema_version_binding() {
    assert_eq!(py_autonomy_wire_schema_version(), 1);
}

#[test]
fn autonomy_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let resolve_request = py_dict(
            py,
            &json!({
                "selection": "tale",
                "source": "prompt",
                "actor": {
                    "kind": "human",
                    "surface": "prompt",
                    "principal": "bryan.zeus",
                },
                "now": "2026-10-09",
            }),
        );
        let record = round_trip(
            py,
            py_autonomy_resolve_selection(py, resolve_request.as_any()),
        );
        assert_eq!(record["profile"], json!("tale"));
        assert_eq!(record["selection"], json!("tale"));
        assert_eq!(record["revision"], json!(1));
        assert_eq!(record["schema_version"], json!(1));

        let record_arg = py_dict(py, &record);
        let evaluate_request = py_dict(
            py,
            &json!({
                "gate_kind": "plan",
                "option_ids": ["commit", "approve"],
                "capabilities": ["approve_archive", "approve", "first"],
            }),
        );
        let decision = round_trip(
            py,
            py_autonomy_evaluate(
                py,
                record_arg.as_any(),
                evaluate_request.as_any(),
            ),
        );
        assert_eq!(decision["outcome"], json!("auto"));
        assert_eq!(decision["value"], json!("approve_archive"));
        assert_eq!(decision["option_ids"], json!(["approve", "commit"]));

        let legacy_arg = py_dict(
            py,
            &json!({
                "auto_approve_plan_action": "epic",
                "auto_approve_argument": "epic",
                "plan": true,
            }),
        );
        let translated = round_trip(
            py,
            py_autonomy_record_from_legacy_meta(py, legacy_arg.as_any()),
        );
        assert_eq!(translated["profile"], json!("epic"));
        assert_eq!(translated["source"], json!("legacy"));

        let translated_arg = py_dict(py, &translated);
        let projection = round_trip(
            py,
            py_autonomy_legacy_projection(py, translated_arg.as_any()),
        );
        assert_eq!(projection["auto_approve_plan_action"], json!("epic"));
        assert_eq!(projection["prompt_mode"], json!("epic"));
    });
}

#[test]
fn autonomy_resolve_selection_rejects_bad_spellings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request = py_dict(
            py,
            &json!({
                "selection": "foo",
                "source": "prompt",
                "actor": {"kind": "human"},
                "now": "",
            }),
        );
        let error =
            py_autonomy_resolve_selection(py, request.as_any()).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        assert!(error.to_string().contains("foo"));
    });
}

#[test]
fn autonomy_summary_and_sentence_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let resolve_request = py_dict(
            py,
            &json!({
                "selection": "tale",
                "source": "prompt",
                "actor": {
                    "kind": "human",
                    "surface": "prompt",
                    "principal": "bryan.zeus",
                },
                "now": "2026-10-09",
            }),
        );
        let record = round_trip(
            py,
            py_autonomy_resolve_selection(py, resolve_request.as_any()),
        );
        let record_arg = py_dict(py, &record);
        let summary =
            round_trip(py, py_autonomy_summary(py, record_arg.as_any()));
        assert_eq!(summary["profile"], json!("tale"));
        assert_eq!(summary["class"], json!("attended"));
        assert_eq!(
            summary["short"],
            json!("tales ✓ · epics ✋ · questions first")
        );
        assert_eq!(
            summary["coverage"],
            json!(
                "Covers host checkpoints only · the agent's shell is not \
                 restricted"
            )
        );
        assert_eq!(summary["cells"].as_array().unwrap().len(), 3);

        let evaluate_request = py_dict(
            py,
            &json!({
                "gate_kind": "plan",
                "option_ids": ["commit", "approve"],
                "capabilities": ["approve_archive", "approve", "first"],
            }),
        );
        let decision = round_trip(
            py,
            py_autonomy_evaluate(
                py,
                record_arg.as_any(),
                evaluate_request.as_any(),
            ),
        );
        let decision_arg = py_dict(py, &decision);
        let context_arg = py_dict(py, &json!({"gate_kind": "plan"}));
        let sentence = py_autonomy_decision_sentence(
            decision_arg.as_any(),
            context_arg.as_any(),
        )
        .unwrap();
        assert_eq!(sentence, "✓ tale approved + archived · tale · gates.plan");

        let profiles = round_trip(py, Ok(py_autonomy_profiles(py).unwrap()));
        let names: Vec<&str> = profiles
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["manual", "standard", "tale", "epic"]);
    });
}

#[test]
fn autonomy_mutate_and_inherit_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let resolve_request = py_dict(
            py,
            &json!({
                "selection": "tale",
                "source": "prompt",
                "actor": {"kind": "human"},
                "now": "",
            }),
        );
        let record = round_trip(
            py,
            py_autonomy_resolve_selection(py, resolve_request.as_any()),
        );
        let record_arg = py_dict(py, &record);
        // Human toggle to manual stores the last profile.
        let mutate_request = py_dict(
            py,
            &json!({
                "selection": "manual",
                "expected_revision": 1,
                "actor": {"kind": "human", "surface": "tui"},
                "now": "2026-10-09T12:00:00Z",
            }),
        );
        let mutated = round_trip(
            py,
            py_autonomy_mutate(
                py,
                record_arg.as_any(),
                mutate_request.as_any(),
            ),
        );
        assert_eq!(mutated["status"], json!("applied"));
        assert_eq!(mutated["record"]["profile"], json!("manual"));
        assert_eq!(mutated["record"]["revision"], json!(2));
        assert_eq!(mutated["record"]["last"]["profile"], json!("tale"));
        // A stale revision is refused without touching the record.
        let stale_request = py_dict(
            py,
            &json!({
                "selection": "manual",
                "expected_revision": 99,
                "actor": {"kind": "human"},
                "now": "",
            }),
        );
        let stale = round_trip(
            py,
            py_autonomy_mutate(py, record_arg.as_any(), stale_request.as_any()),
        );
        assert_eq!(stale["status"], json!("stale"));
        // Inheritance carries the record with an inherited source.
        let inherit_request = py_dict(
            py,
            &json!({
                "predecessor_name": "sase.1",
                "explicit_selection": null,
                "actor": {"kind": "host", "surface": "successor"},
                "now": "",
            }),
        );
        let inherited = round_trip(
            py,
            py_autonomy_inherit(
                py,
                record_arg.as_any(),
                inherit_request.as_any(),
            ),
        );
        assert_eq!(inherited["status"], json!("inherited"));
        assert_eq!(inherited["record"]["source"], json!("inherited"));
        assert_eq!(inherited["record"]["inherited_from"], json!("sase.1"));
    });
}

#[test]
fn autonomy_decision_log_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().to_path_buf();
        let entry = py_dict(
            py,
            &json!({
                "schema_version": 1,
                "at": "2026-10-09T10:00:00Z",
                "agent": "sase.x",
                "agent_session": "sase.sess",
                "project": "sase",
                "gate_kind": "plan",
                "gate_id": "gate-1",
                "creator_role": "top_level",
                "decision": {
                    "outcome": "auto",
                    "value": "approve_archive",
                    "option_ids": ["approve", "commit"],
                    "rule": "gates.plan",
                    "reason": "r",
                    "profile": "tale",
                    "selection": "tale",
                    "revision": 1,
                    "digest": "d",
                    "source": "prompt",
                },
            }),
        );
        py_autonomy_append_decision(home.clone().into(), entry.as_any())
            .unwrap();
        let query = py_dict(py, &json!({}));
        let entries = round_trip(
            py,
            py_autonomy_read_decisions(py, home.clone().into(), query.as_any()),
        );
        assert_eq!(entries.as_array().unwrap().len(), 1);
        assert_eq!(entries[0]["agent"], json!("sase.x"));
        let filtered = py_dict(py, &json!({"agent": "nobody"}));
        let none = round_trip(
            py,
            py_autonomy_read_decisions(py, home.into(), filtered.as_any()),
        );
        assert_eq!(none.as_array().unwrap().len(), 0);
    });
}
