use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;
use tempfile::tempdir;

fn actor() -> serde_json::Value {
    json!({"principal": "bryan.athena", "kind": "human"})
}

fn new_request(key: &str) -> serde_json::Value {
    json!({
        "action": {
            "action": "new",
            "title": "Binding goal",
            "outcome": "the outcome",
            "criteria": [],
            "project": "sase",
            "via": "cli",
            "idempotency_key": key,
        },
        "actor": actor(),
        "now": "2026-09-28T14:00:00.000Z",
        "new_goal_id": "7k2mq",
    })
}

#[test]
fn goal_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "goal_ledger_init",
            "goal_ledger_append",
            "goal_ledger_list",
            "goal_ledger_show",
            "goal_ledger_history",
            "goal_ledger_doctor",
            "goal_projection_status",
            "goal_projection_refresh",
            "goal_mint_id",
            "goal_ledger_wire_schema_version",
            "goal_ledger_store_schema_version",
            "goal_ledger_probe_list",
            "goal_card_view",
            "goal_card_markdown",
            "goal_citation_line",
        ] {
            assert!(module.getattr(name).is_ok(), "{name}");
        }
        assert_eq!(py_goal_ledger_wire_schema_version(), 1);
        assert_eq!(py_goal_ledger_store_schema_version(), 1);
        let minted = py_goal_mint_id();
        assert_eq!(minted.len(), 5);
    });
}

#[test]
fn goal_ledger_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let dir = tempdir().unwrap();
        let root = dir.path().join("goals").display().to_string();
        let init = py_goal_ledger_init(py, &root).unwrap();
        let value = py_to_json_value(init.bind(py)).unwrap();
        assert_eq!(value["created"], json!(true));

        let request = json_value_to_py(py, &new_request("k1"))
            .unwrap()
            .into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        let appended = py_goal_ledger_append(py, &root, request).unwrap();
        let value = py_to_json_value(appended.bind(py)).unwrap();
        assert_eq!(value["status"], json!("applied"));
        assert_eq!(value["events"].as_array().unwrap().len(), 1);

        let listed = py_goal_ledger_list(py, &root, None).unwrap();
        let value = py_to_json_value(listed.bind(py)).unwrap();
        assert_eq!(value["goals"].as_array().unwrap().len(), 1);
        assert_eq!(value["goals"][0]["id"], json!("7k2mq"));

        let shown = py_goal_ledger_show(py, &root, "7K2MQ").unwrap();
        let value = py_to_json_value(shown.bind(py)).unwrap();
        assert_eq!(value["id"], json!("7k2mq"));

        let history = py_goal_ledger_history(py, &root, None).unwrap();
        let value = py_to_json_value(history.bind(py)).unwrap();
        assert_eq!(value["goals"].as_array().unwrap().len(), 0);

        let doctor_request = json_value_to_py(py, &json!({"repair": false}))
            .unwrap()
            .into_bound(py);
        let doctor_request = doctor_request.downcast::<PyDict>().unwrap();
        let doctor = py_goal_ledger_doctor(py, &root, doctor_request).unwrap();
        let value = py_to_json_value(doctor.bind(py)).unwrap();
        assert_eq!(value["ok"], json!(true));

        let projection =
            dir.path().join("goals-hot.json").display().to_string();
        let refreshed =
            py_goal_projection_refresh(py, &root, &projection, "sase", "local")
                .unwrap();
        let value = py_to_json_value(refreshed.bind(py)).unwrap();
        assert_eq!(value["wrote"], json!(true));

        let status = py_goal_projection_status(py, &root, &projection).unwrap();
        let value = py_to_json_value(status.bind(py)).unwrap();
        assert_eq!(value["status"], json!("Fresh"));

        let probe = py_goal_ledger_probe_list(py, &root).unwrap();
        let value = py_to_json_value(probe.bind(py)).unwrap();
        assert_eq!(value["counts"]["settled_event_opens"], json!(0));
    });
}

#[test]
fn goal_render_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let card = json_value_to_py(
            py,
            &json!({
                "title": "Binding goal",
                "status_badge": "ACTIVE",
                "goal_ref": "goal:7k2mq",
                "project": "sase",
                "opened_by": "bryan.athena",
                "opened_at": "2026-09-28T14:00:00.000Z",
                "outcome": "the outcome",
                "criteria": [
                    {"id": "e.0", "text": "cover sync", "source": "user"},
                ],
            }),
        )
        .unwrap()
        .into_bound(py);
        let card = card.downcast::<PyDict>().unwrap();
        let markdown = py_goal_card_markdown(py, card).unwrap();
        assert!(markdown.contains("⌖ Binding goal"));
        assert!(markdown.contains("goal:7k2mq · sase · ACTIVE"));
        assert!(markdown.contains("- cover sync (user)"));
        let line = py_goal_citation_line(py, card).unwrap();
        assert!(line.starts_with(
            "goal ⌖7k2mq \"Binding goal\" in the sase project (active)"
        ));
        assert!(line.contains("sase goal show 7k2mq"));
        assert!(line.chars().count() <= 400);
    });
}

#[test]
fn goal_card_view_binding_reduces_state() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let dir = tempdir().unwrap();
        let root = dir.path().join("goals").display().to_string();
        py_goal_ledger_init(py, &root).unwrap();
        let request = json_value_to_py(py, &new_request("k1"))
            .unwrap()
            .into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        py_goal_ledger_append(py, &root, request).unwrap();

        let card =
            py_goal_card_view(py, &root, "7k2mq", "2026-09-28T15:00:00.000Z")
                .unwrap();
        let value = py_to_json_value(card.bind(py)).unwrap();
        assert_eq!(value["goal_ref"], json!("goal:7k2mq"));
        assert_eq!(value["status_badge"], json!("ACTIVE"));
        assert_eq!(value["title"], json!("Binding goal"));
    });
}

#[test]
fn goal_ledger_append_reports_stale_basis() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let dir = tempdir().unwrap();
        let root = dir.path().join("goals").display().to_string();
        py_goal_ledger_init(py, &root).unwrap();
        let request = json_value_to_py(py, &new_request("k1"))
            .unwrap()
            .into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        py_goal_ledger_append(py, &root, request).unwrap();

        let drop_action = json!({
            "action": {
                "action": "drop",
                "goal_id": "7k2mq",
                "why": "no longer needed",
                "expected_head": "bogus",
                "idempotency_key": "k2",
            },
            "actor": actor(),
            "now": "2026-09-28T14:02:00.000Z",
        });
        let request =
            json_value_to_py(py, &drop_action).unwrap().into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        let outcome = py_goal_ledger_append(py, &root, request).unwrap();
        let value = py_to_json_value(outcome.bind(py)).unwrap();
        assert_eq!(value["status"], json!("stale_basis"));
        assert!(!value["states"].as_array().unwrap().is_empty());
    });
}
