use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use serde_json::json;
use std::fs;

fn temp_notification_path(name: &str) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::Builder::new()
        .prefix("sase-core-py-notification-")
        .tempdir()
        .unwrap();
    let path = temp.path().join(name);
    (temp, path)
}

#[test]
fn notification_store_binding_round_trips_json_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        let notification_obj = json_value_to_py(
            py,
            &json!({
                "id": "n1",
                "timestamp": "2026-04-30T12:00:00+00:00",
                "sender": "axe",
                "icon": "🚀",
                "notes": ["hello"],
                "files": [],
                "action": "EpicApproval",
                "action_data": {},
                "read": false,
                "dismissed": false,
                "silent": false,
                "muted": false,
                "snooze_until": null
            }),
        )
        .unwrap();
        let notification =
            notification_obj.bind(py).downcast::<PyDict>().unwrap();

        let appended =
            py_append_notification(py, path.to_str().unwrap(), notification)
                .unwrap();
        let appended_value = py_to_json_value(appended.bind(py)).unwrap();
        assert_eq!(appended_value["appended_count"], json!(1));

        let snapshot = py_read_notifications_snapshot(
            py,
            path.to_str().unwrap(),
            false,
            false,
        )
        .unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["schema_version"], json!(1));
        assert_eq!(snapshot_value["notifications"][0]["id"], json!("n1"));
        assert_eq!(snapshot_value["notifications"][0]["icon"], json!("🚀"));
        assert_eq!(
            snapshot_value["notifications"][0]["action"],
            json!("EpicApproval")
        );
        assert_eq!(snapshot_value["counts"]["priority"], json!(1));

        let update_obj =
            json_value_to_py(py, &json!({"kind": "mark_read", "id": "n1"}))
                .unwrap();
        let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
        let outcome = py_apply_notification_state_update(
            py,
            path.to_str().unwrap(),
            update,
        )
        .unwrap();
        let outcome_value = py_to_json_value(outcome.bind(py)).unwrap();
        assert_eq!(outcome_value["matched_count"], json!(1));
        assert_eq!(outcome_value["changed_count"], json!(1));
    });
}

#[test]
fn notification_store_current_snapshot_binding_reconciles_snoozes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        let notification_obj = json_value_to_py(
            py,
            &json!({
                "id": "due",
                "timestamp": "2000-01-01T00:00:00+00:00",
                "sender": "axe",
                "read": true,
                "muted": true,
                "snooze_until": "2000-01-02T00:00:00+00:00"
            }),
        )
        .unwrap();
        let notification =
            notification_obj.bind(py).downcast::<PyDict>().unwrap();
        py_append_notification(py, path.to_str().unwrap(), notification)
            .unwrap();

        let snapshot = py_read_current_notifications_snapshot(
            py,
            path.to_str().unwrap(),
            false,
        )
        .unwrap();
        let value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(value["expired_ids"], json!(["due"]));
        assert_eq!(value["notifications"][0]["muted"], json!(false));
        assert_eq!(value["notifications"][0]["read"], json!(false));
        assert_eq!(value["notifications"][0]["snooze_until"], json!(null));
        assert!(value["notifications"][0]["resurfaced_at"].is_string());
        assert_eq!(value["next_snooze_deadline"], json!(null));
    });
}

#[test]
fn pending_action_bindings_round_trip_transport_records() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("actions.json");
        let notification_obj = json_value_to_py(
            py,
            &json!({
                "id": "abcd1234-full",
                "timestamp": "2026-04-30T12:00:00+00:00",
                "sender": "axe",
                "notes": [],
                "files": ["plan.md"],
                "action": "PlanApproval",
                "action_data": {"response_dir": "/tmp/plan"},
                "read": false,
                "dismissed": false,
                "silent": false,
                "muted": false,
                "snooze_until": null
            }),
        )
        .unwrap();
        let notification =
            notification_obj.bind(py).downcast::<PyDict>().unwrap();
        let pending =
            py_pending_action_from_notification(py, notification, 10.0)
                .unwrap()
                .unwrap();
        let pending_value = py_to_json_value(pending.bind(py)).unwrap();
        assert_eq!(pending_value["prefix"], json!("abcd1234"));
        let pending_dict = pending.bind(py).downcast::<PyDict>().unwrap();

        py_register_pending_action(py, path.to_str().unwrap(), pending_dict)
            .unwrap();
        let record_obj = json_value_to_py(
            py,
            &json!({"message_id": 42, "nested": {"keep": true}}),
        )
        .unwrap();
        let record = record_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_merge_pending_action_transport(
            py,
            path.to_str().unwrap(),
            "abcd1234-full",
            "telegram",
            record,
            Some(20.0),
        )
        .unwrap());
        assert!(py_mark_pending_action_handled(
            py,
            path.to_str().unwrap(),
            "abcd1234",
            "test",
            Some("approve"),
            Some(30.0),
        )
        .unwrap());

        let request_obj = json_value_to_py(
            py,
            &json!({
                "operation": "list",
                "path": path,
                "legacy_path": null,
                "now_unix": 40.0,
                "transport": "telegram"
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let listed = py_pending_action_transport(py, request).unwrap();
        let listed = py_to_json_value(listed.bind(py)).unwrap();
        assert_eq!(listed["abcd1234"]["message_id"], json!(42));
        assert_eq!(
            listed["abcd1234"]["action_data"]["response_dir"],
            json!("/tmp/plan")
        );

        assert!(py_remove_pending_action(
            py,
            path.to_str().unwrap(),
            "abcd1234-full",
        )
        .unwrap());
    });
}

#[test]
fn notification_store_binding_rejects_bad_update_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let bad_obj =
            json_value_to_py(py, &json!({"kind": "mark_snoozed", "id": "n1"}))
                .unwrap();
        let bad = bad_obj.bind(py).downcast::<PyDict>().unwrap();

        let err = py_apply_notification_state_update(
            py,
            "/tmp/notifications.jsonl",
            bad,
        )
        .unwrap_err();
        assert!(err.to_string().contains("NotificationStateUpdateWire dict"));
    });
}

#[test]
fn notification_store_counts_binding_omits_rows_and_persists() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        let notification_obj = json_value_to_py(
            py,
            &json!({
                "id": "n1",
                "timestamp": "2026-04-30T12:00:00+00:00",
                "sender": "axe",
                "notes": [],
                "files": [],
                "action": null,
                "action_data": {},
                "read": false,
                "dismissed": false,
                "silent": false,
                "muted": false,
                "snooze_until": null
            }),
        )
        .unwrap();
        let notification =
            notification_obj.bind(py).downcast::<PyDict>().unwrap();
        py_append_notification(py, path.to_str().unwrap(), notification)
            .unwrap();

        let update_obj =
            json_value_to_py(py, &json!({"kind": "mark_read", "id": "n1"}))
                .unwrap();
        let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
        let outcome = py_apply_notification_state_update_counts(
            py,
            path.to_str().unwrap(),
            update,
        )
        .unwrap();
        let outcome_value = py_to_json_value(outcome.bind(py)).unwrap();
        assert_eq!(outcome_value["matched_count"], json!(1));
        assert_eq!(outcome_value["changed_count"], json!(1));
        assert_eq!(outcome_value["notifications"], json!([]));
        assert_eq!(outcome_value["counts"]["priority"], json!(0));
        assert_eq!(outcome_value["stats"]["loaded_rows"], json!(0));

        let snapshot = py_read_notifications_snapshot(
            py,
            path.to_str().unwrap(),
            true,
            false,
        )
        .unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["notifications"][0]["read"], json!(true));
    });
}

#[test]
fn notification_store_append_and_rewrite_counts_bindings_omit_rows() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        let notification_obj = json_value_to_py(
            py,
            &json!({
                "id": "n1",
                "timestamp": "2026-04-30T12:00:00+00:00",
                "sender": "axe",
                "notes": [],
                "files": [],
                "action": null,
                "action_data": {},
                "read": false,
                "dismissed": false,
                "silent": false,
                "muted": false,
                "snooze_until": null
            }),
        )
        .unwrap();
        let notification =
            notification_obj.bind(py).downcast::<PyDict>().unwrap();

        let appended = py_append_notification_counts(
            py,
            path.to_str().unwrap(),
            notification,
        )
        .unwrap();
        let appended_value = py_to_json_value(appended.bind(py)).unwrap();
        assert_eq!(appended_value["appended_count"], json!(1));
        assert_eq!(appended_value["matched_count"], json!(0));
        assert_eq!(appended_value["changed_count"], json!(0));
        assert_eq!(appended_value["rewritten"], json!(false));
        assert_eq!(appended_value["notifications"], json!([]));
        assert_eq!(appended_value["counts"]["priority"], json!(0));
        assert_eq!(appended_value["stats"]["loaded_rows"], json!(0));

        let snapshot = py_read_notifications_snapshot(
            py,
            path.to_str().unwrap(),
            true,
            false,
        )
        .unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["notifications"][0]["id"], json!("n1"));

        let replacement_obj = json_value_to_py(
            py,
            &json!([{
                "id": "n2",
                "timestamp": "2026-04-30T13:00:00+00:00",
                "sender": "axe",
                "notes": [],
                "files": [],
                "action": null,
                "action_data": {},
                "read": false,
                "dismissed": false,
                "silent": false,
                "muted": false,
                "snooze_until": null
            }]),
        )
        .unwrap();
        let replacement =
            replacement_obj.bind(py).downcast::<PyList>().unwrap();

        let rewritten = py_rewrite_notifications_counts(
            py,
            path.to_str().unwrap(),
            replacement,
        )
        .unwrap();
        let rewritten_value = py_to_json_value(rewritten.bind(py)).unwrap();
        assert_eq!(rewritten_value["matched_count"], json!(1));
        assert_eq!(rewritten_value["changed_count"], json!(1));
        assert_eq!(rewritten_value["appended_count"], json!(0));
        assert_eq!(rewritten_value["rewritten"], json!(true));
        assert_eq!(rewritten_value["notifications"], json!([]));

        let after = py_read_notifications_snapshot(
            py,
            path.to_str().unwrap(),
            true,
            false,
        )
        .unwrap();
        let after_value = py_to_json_value(after.bind(py)).unwrap();
        assert_eq!(after_value["notifications"][0]["id"], json!("n2"));
        assert_eq!(after_value["notifications"][1]["id"], json!("n1"));
        assert_eq!(after_value["notifications"].as_array().unwrap().len(), 2);

        let _ = fs::remove_file(&path);
        let _ =
            fs::remove_file(path.with_file_name("notifications.jsonl.lock"));
        let _ = fs::remove_dir(path.parent().unwrap());
    });
}

#[test]
fn notification_plus_one_and_upsert_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        let created_obj = json_value_to_py(
            py,
            &json!({
                "notification": {
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "ci_watch",
                    "notes": ["CI failure: a"],
                    "dedup_key": "combo"
                },
                "plus_one_note": "first",
                "plus_one_timestamp": "2026-04-30T12:00:00+00:00"
            }),
        )
        .unwrap();
        let created = created_obj.bind(py).downcast::<PyDict>().unwrap();
        let created_outcome =
            py_upsert_notification(py, path.to_str().unwrap(), created)
                .unwrap();
        let created_value = py_to_json_value(created_outcome.bind(py)).unwrap();
        assert_eq!(created_value["action"], json!("created"));
        assert_eq!(created_value["id"], json!("n1"));

        let plus_obj = json_value_to_py(
            py,
            &json!({
                "id": "n1",
                "timestamp": "2026-04-30T13:00:00+00:00",
                "sender": "ci_watch",
                "note": "  churn  again  "
            }),
        )
        .unwrap();
        let plus = plus_obj.bind(py).downcast::<PyDict>().unwrap();
        let plus_outcome =
            py_append_notification_plus_one(py, path.to_str().unwrap(), plus)
                .unwrap();
        let plus_value = py_to_json_value(plus_outcome.bind(py)).unwrap();
        assert_eq!(plus_value["action"], json!("applied"));
        assert_eq!(plus_value["plus_one_count"], json!(1));
        assert_eq!(
            plus_value["notification"]["plus_ones"][0]["note"],
            json!("churn again")
        );

        let upsert_obj = json_value_to_py(
            py,
            &json!({
                "notification": {
                    "id": "n2",
                    "timestamp": "2026-04-30T14:00:00+00:00",
                    "sender": "ci_watch",
                    "notes": ["ignored"],
                    "dedup_key": "combo"
                },
                "plus_one_note": "second",
                "plus_one_timestamp": "2026-04-30T14:00:00+00:00"
            }),
        )
        .unwrap();
        let upsert = upsert_obj.bind(py).downcast::<PyDict>().unwrap();
        let upsert_outcome =
            py_upsert_notification(py, path.to_str().unwrap(), upsert).unwrap();
        let upsert_value = py_to_json_value(upsert_outcome.bind(py)).unwrap();
        assert_eq!(upsert_value["action"], json!("plus_oned"));
        assert_eq!(upsert_value["id"], json!("n1"));
        assert_eq!(upsert_value["plus_one_count"], json!(2));
    });
}

#[test]
fn notification_delivery_binding_resolves_a_batch_in_one_call() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("resolve_notification_deliveries").is_ok());

        let rules = PyList::empty_bound(py);
        append_json(
            py,
            &rules,
            json!({
                "name": "quiet-task-beads",
                "match": {"tab": "beads"},
                "toast": false,
                "sound": "none",
            }),
        );
        append_json(
            py,
            &rules,
            json!({"name": "chime", "sound": "/sounds/glass.aiff"}),
        );
        let notifications = PyList::empty_bound(py);
        append_json(
            py,
            &notifications,
            json!({
                "id": "triage",
                "timestamp": "2026-09-20T12:00:00-04:00",
                "sender": "bead",
                "notes": ["Task triage"],
                "tags": ["bead", "task"],
                "action": "TaskTriage",
                "action_data": {"panel": "beads"},
            }),
        );
        append_json(
            py,
            &notifications,
            json!({
                "id": "axe",
                "timestamp": "2026-09-20T12:00:01-04:00",
                "sender": "axe",
                "action": "ViewErrorReport",
            }),
        );

        let result =
            py_resolve_notification_deliveries(py, &rules, &notifications)
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let deliveries = value.as_array().unwrap();
        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0]["toast"], json!(false));
        assert_eq!(deliveries[0]["sound"], json!({"kind": "none"}));
        assert_eq!(deliveries[0]["toast_rule"], json!("quiet-task-beads"));
        assert_eq!(deliveries[0]["sound_rule"], json!("quiet-task-beads"));
        assert_eq!(deliveries[0]["telegram"], json!(true));
        assert_eq!(deliveries[0].get("telegram_rule"), None);
        assert_eq!(deliveries[1]["toast"], json!(true));
        assert_eq!(
            deliveries[1]["sound"],
            json!({"kind": "file", "path": "/sounds/glass.aiff"})
        );
        assert_eq!(deliveries[1].get("toast_rule"), None);
        assert_eq!(deliveries[1]["sound_rule"], json!("chime"));
        assert_eq!(deliveries[1]["telegram"], json!(true));
        assert!(deliveries[1]["schema_version"].is_u64());

        // No rules is today's behavior: toast, ring the bell, permit Telegram.
        let empty = PyList::empty_bound(py);
        let result =
            py_resolve_notification_deliveries(py, &empty, &notifications)
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        for delivery in value.as_array().unwrap() {
            assert_eq!(delivery["toast"], json!(true));
            assert_eq!(delivery["sound"], json!({"kind": "bell"}));
            assert_eq!(delivery["telegram"], json!(true));
            assert_eq!(delivery.get("toast_rule"), None);
            assert_eq!(delivery.get("sound_rule"), None);
            assert_eq!(delivery.get("telegram_rule"), None);
        }

        // A telegram-only rule suppresses Telegram without touching TUI.
        let tg_rules = PyList::empty_bound(py);
        append_json(
            py,
            &tg_rules,
            json!({
                "name": "quiet-task-beads-telegram",
                "match": {"tab": "beads"},
                "telegram": false,
            }),
        );
        let result =
            py_resolve_notification_deliveries(py, &tg_rules, &notifications)
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let deliveries = value.as_array().unwrap();
        assert_eq!(deliveries[0]["telegram"], json!(false));
        assert_eq!(
            deliveries[0]["telegram_rule"],
            json!("quiet-task-beads-telegram")
        );
        assert_eq!(deliveries[0]["toast"], json!(true));
        assert_eq!(deliveries[1]["telegram"], json!(true));
        assert_eq!(deliveries[1].get("telegram_rule"), None);

        // Global TUI rule plus Athena telegram rule resolve independently.
        let layered = PyList::empty_bound(py);
        append_json(
            py,
            &layered,
            json!({
                "name": "quiet-task-beads",
                "match": {"tab": "beads"},
                "toast": false,
                "sound": "none",
            }),
        );
        append_json(
            py,
            &layered,
            json!({
                "name": "quiet-task-beads-telegram",
                "match": {"tab": "beads"},
                "telegram": false,
            }),
        );
        let result =
            py_resolve_notification_deliveries(py, &layered, &notifications)
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let deliveries = value.as_array().unwrap();
        assert_eq!(deliveries[0]["toast"], json!(false));
        assert_eq!(deliveries[0]["telegram"], json!(false));
        assert_eq!(deliveries[0]["toast_rule"], json!("quiet-task-beads"));
        assert_eq!(
            deliveries[0]["telegram_rule"],
            json!("quiet-task-beads-telegram")
        );

        // An empty batch resolves to an empty list.
        let none = PyList::empty_bound(py);
        let result =
            py_resolve_notification_deliveries(py, &rules, &none).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value, json!([]));
    });
}

#[test]
fn notification_reconcile_binding_applies_field_scoped_write() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        for id in ["remote-1", "completion-1"] {
            let row_obj = json_value_to_py(
                py,
                &json!({
                    "id": id,
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "remote-attention",
                    "notes": ["old"],
                    "read": true,
                }),
            )
            .unwrap();
            let row = row_obj.bind(py).downcast::<PyDict>().unwrap();
            py_append_notification(py, path.to_str().unwrap(), row).unwrap();
        }

        // A concurrent dismissal lands after the caller's snapshot read.
        let dismiss_obj = json_value_to_py(
            py,
            &json!({"kind": "mark_dismissed", "id": "completion-1"}),
        )
        .unwrap();
        let dismiss = dismiss_obj.bind(py).downcast::<PyDict>().unwrap();
        py_apply_notification_state_update(py, path.to_str().unwrap(), dismiss)
            .unwrap();

        // Stale refresh of remote-1, auto-dismiss of the completion row
        // (already dismissed: stays put), plus one brand-new row.
        let request_obj = json_value_to_py(
            py,
            &json!({
                "notifications": [
                    {
                        "id": "remote-1",
                        "timestamp": "1999-01-01T00:00:00+00:00",
                        "sender": "impostor",
                        "notes": ["new"],
                        "read": false,
                        "dismissed": true,
                    },
                    {
                        "id": "remote-2",
                        "timestamp": "2026-04-30T13:00:00+00:00",
                        "sender": "remote-attention",
                        "notes": ["fresh"],
                    },
                ],
                "reversible_dismiss_marker_key": "test_marker",
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let outcome =
            py_reconcile_notification_rows(py, path.to_str().unwrap(), request)
                .unwrap();
        let value = py_to_json_value(outcome.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["created"], json!(1));
        assert_eq!(value["updated"], json!(1));
        assert_eq!(value["dismissed"], json!(0));
        assert_eq!(value["resurfaced"], json!(0));

        let snapshot = py_read_notifications_snapshot(
            py,
            path.to_str().unwrap(),
            true,
            false,
        )
        .unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        let rows = snapshot_value["notifications"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        let refreshed = rows
            .iter()
            .find(|row| row["id"] == json!("remote-1"))
            .unwrap();
        assert_eq!(refreshed["notes"], json!(["new"]));
        // Disk-owned fields win over the stale input.
        assert_eq!(refreshed["timestamp"], json!("2026-04-30T12:00:00+00:00"));
        assert_eq!(refreshed["sender"], json!("remote-attention"));
        assert_eq!(refreshed["read"], json!(true));
        assert_eq!(refreshed["dismissed"], json!(false));
    });
}

#[test]
fn notification_reconcile_binding_rejects_malformed_requests() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let bad_obj =
            json_value_to_py(py, &json!({"notifications": "nope"})).unwrap();
        let bad = bad_obj.bind(py).downcast::<PyDict>().unwrap();
        let err =
            py_reconcile_notification_rows(py, "/tmp/notifications.jsonl", bad)
                .unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));
        assert!(err.to_string().contains("NotificationReconcileRequestWire"));
    });
}

#[test]
fn notification_delivery_binding_rejects_malformed_rules() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let notifications = PyList::empty_bound(py);
        for (bad, needle) in [
            (json!({"match": {"tabs": "beads"}}), "unknown field `tabs`"),
            (json!({"toats": false}), "unknown field `toats`"),
            (json!({"match": {"tags": 7}}), "a string or a list"),
            (json!({"toast": "yes"}), "invalid type"),
        ] {
            let rules = PyList::empty_bound(py);
            append_json(py, &rules, json!({"toast": true}));
            append_json(py, &rules, bad);
            let err =
                py_resolve_notification_deliveries(py, &rules, &notifications)
                    .unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(py));
            let message = err.to_string();
            assert!(message.contains("rules[1]"), "{message}");
            assert!(message.contains(needle), "{message}");
        }
    });
}

#[test]
fn notification_ack_and_index_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, path) = temp_notification_path("notifications.jsonl");
        for row in [
            json!({
                "id": "completion-1",
                "timestamp": "2026-04-30T12:00:00+00:00",
                "sender": "user-agent",
                "action": "JumpToAgent",
                "action_data": {
                    "cl_name": "feature",
                    "raw_suffix": "20260430120000",
                },
                "read": false,
                "dismissed": false,
            }),
            json!({
                "id": "settlement-1",
                "timestamp": "2026-04-30T12:01:00+00:00",
                "sender": "epic-launch",
                "action_data": {
                    "cl_name": "feature",
                    "raw_suffix": "20260430120000",
                },
                "read": true,
                "dismissed": false,
            }),
            json!({
                "id": "remote-1",
                "timestamp": "2026-04-30T12:02:00+00:00",
                "sender": "remote-attention",
                "notes": ["old"],
                "read": false,
                "dismissed": false,
            }),
        ] {
            let row_obj = json_value_to_py(py, &row).unwrap();
            let row_dict = row_obj.bind(py).downcast::<PyDict>().unwrap();
            py_append_notification(py, path.to_str().unwrap(), row_dict)
                .unwrap();
        }

        let before_obj =
            py_read_unread_completion_index(py, path.to_str().unwrap())
                .unwrap();
        let before = py_to_json_value(before_obj.bind(py)).unwrap();
        assert_eq!(before["schema_version"], json!(1));
        let generation_before = before["generation"].as_u64().unwrap();

        let request_obj = json_value_to_py(
            py,
            &json!({
                "agents": [
                    {
                        "cl_name": "feature",
                        "raw_suffix": "20260430120000",
                    },
                ],
            }),
        )
        .unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let outcome_obj =
            py_ack_agent_completions(py, path.to_str().unwrap(), request)
                .unwrap();
        let outcome = py_to_json_value(outcome_obj.bind(py)).unwrap();
        assert_eq!(outcome["schema_version"], json!(1));
        assert_eq!(
            outcome["dismissed_ids"],
            json!(["completion-1", "settlement-1"])
        );
        assert_eq!(
            outcome["generation"].as_u64().unwrap(),
            generation_before + 1
        );

        // A repeat ack with the same keys dismisses nothing new and does
        // not bump the generation.
        let repeat_obj =
            py_ack_agent_completions(py, path.to_str().unwrap(), request)
                .unwrap();
        let repeat = py_to_json_value(repeat_obj.bind(py)).unwrap();
        assert_eq!(repeat["dismissed_ids"], json!([]));
        assert_eq!(repeat["generation"], outcome["generation"]);

        // The index lists the completion and settlement rows with their
        // flags and the post-ack generation, and omits the unrelated row.
        let index_obj =
            py_read_unread_completion_index(py, path.to_str().unwrap())
                .unwrap();
        let index = py_to_json_value(index_obj.bind(py)).unwrap();
        assert_eq!(index["schema_version"], json!(1));
        assert_eq!(index["generation"], outcome["generation"]);
        let rows = index["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], json!("completion-1"));
        assert_eq!(
            rows[0]["agent"],
            json!({
                "cl_name": "feature",
                "raw_suffix": "20260430120000",
            })
        );
        assert_eq!(rows[0]["read"], json!(false));
        assert_eq!(rows[0]["dismissed"], json!(true));
        assert_eq!(rows[1]["id"], json!("settlement-1"));
        assert_eq!(rows[1]["read"], json!(true));
        assert_eq!(rows[1]["dismissed"], json!(true));

        // Malformed ack input is a ValueError, not a panic.
        let bad_obj = json_value_to_py(py, &json!({"agents": "nope"})).unwrap();
        let bad = bad_obj.bind(py).downcast::<PyDict>().unwrap();
        let err = py_ack_agent_completions(py, path.to_str().unwrap(), bad)
            .unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));
        assert!(err.to_string().contains("NotificationAckRequestWire"));
    });
}
