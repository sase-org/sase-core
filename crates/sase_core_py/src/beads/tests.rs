use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use sase_core::bead::IssueTypeWire;
use serde_json::json;
use std::fs;
use tempfile::tempdir;

#[test]
fn bead_target_routing_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_route_targets").is_ok());
        assert_eq!(py_bead_target_routing_wire_schema_version(), 1);

        let request = json_value_to_py(
            py,
            &json!({
                "targets": ["bob-cli-1"],
                "candidate_stores": [{
                    "store_key": "bob",
                    "project_key": "gh_acme__bob-cli",
                    "project_label": "bob-cli",
                    "issue_ids": ["bob-cli-1"]
                }]
            }),
        )
        .unwrap()
        .into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        let result = py_bead_route_targets(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(value["routes"][0]["resolved_id"], json!("bob-cli-1"));
        assert_eq!(
            value["routes"][0]["store"]["project_label"],
            json!("bob-cli")
        );
    });
}

#[test]
fn bead_touch_index_bindings_round_trip_the_complete_snapshot() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "bead_touch_index_wire_schema_version",
            "bead_touch_index_refresh",
            "bead_touch_index_query",
            "bead_touch_index_status",
        ] {
            assert!(module.getattr(name).is_ok(), "{name}");
        }
        assert_eq!(py_bead_touch_index_wire_schema_version(), 3);

        let dir = tempdir().unwrap();
        let beads_dir = dir.path().join("beads");
        let streams = beads_dir.join("events/streams");
        fs::create_dir_all(&streams).unwrap();
        let event =
            |id: &str, actor: &str, op: &str, at: &str, body: JsonValue| {
                let mut payload = body;
                payload["kind"] = json!(op);
                json!({
                    "schema_version": 1,
                    "event_id": format!("b-1:{id}"),
                    "timestamp": at,
                    "actor": actor,
                    "operation": op,
                    "issue_id": "b-1",
                    "payload": payload,
                })
                .to_string()
            };
        let text = [
                event(
                    "1",
                    "owner@example.com",
                    "issue_created",
                    "2026-01-01T00:00:00Z",
                    json!({"issue": {"id": "b-1", "title": "Bead", "status": "open", "issue_type": "task"}}),
                ),
                event(
                    "2",
                    "bbugyi200.athena.0aa",
                    "note_appended",
                    "2026-01-01T00:01:00Z",
                    json!({"entry": "x"}),
                ),
                event(
                    "3",
                    "bbugyi200.athena.0aa",
                    "issue_closed",
                    "2026-01-01T00:02:00Z",
                    json!({"close_reason": null}),
                ),
                event(
                    "4",
                    "013",
                    "note_appended",
                    "2026-01-01T00:03:00Z",
                    json!({"entry": "y"}),
                ),
            ]
            .join("\n");
        fs::write(streams.join("b-1.jsonl"), format!("{text}\n")).unwrap();
        let beads_dir_str = beads_dir.to_str().unwrap();
        let index_path = dir.path().join("project/agent_bead_touches.json");
        let index_str = index_path.to_str().unwrap();

        let missing =
            py_bead_touch_index_status(py, beads_dir_str, index_str).unwrap();
        let missing = py_to_json_value(missing.bind(py)).unwrap();
        assert_eq!(missing["state"], json!("missing"));

        let miss = py_bead_touch_index_query(py, index_str, None).unwrap();
        let miss = py_to_json_value(miss.bind(py)).unwrap();
        assert_eq!(
            miss,
            json!({"schema_version": 3, "generation": "", "touches": []})
        );

        let refresh =
            py_bead_touch_index_refresh(py, beads_dir_str, index_str).unwrap();
        let refresh = py_to_json_value(refresh.bind(py)).unwrap();
        let generation = refresh["generation"].clone();
        assert_eq!(
            refresh,
            json!({
                "schema_version": 3,
                "generation": generation,
                "full_rebuild": true,
                "wrote": true,
                "stream_count": 1,
                "reduced_streams": ["b-1"],
                "reused_streams": 0,
                "removed_streams": [],
                "touch_count": 2,
            })
        );

        let query = py_bead_touch_index_query(py, index_str, None).unwrap();
        let query = py_to_json_value(query.bind(py)).unwrap();
        assert_eq!(
            query,
            json!({
                "schema_version": 3,
                "generation": generation,
                "touches": [
                    {
                        "actor": "013",
                        "bead_id": "b-1",
                        "title": "Bead",
                        "issue_type": "task",
                        "status": "closed",
                        "verbs": {"noted": 1},
                        "first_at": "2026-01-01T00:03:00Z",
                        "last_at": "2026-01-01T00:03:00Z",
                        "current_note_count": 1,
                        "note_preview": {
                            "id": "b-1:4",
                            "author": "013",
                            "timestamp": "2026-01-01T00:03:00Z",
                            "text": "y",
                            "truncated": false,
                        },
                        "stream_id": "b-1",
                    },
                    {
                        "actor": "bbugyi200.athena.0aa",
                        "bead_id": "b-1",
                        "title": "Bead",
                        "issue_type": "task",
                        "status": "closed",
                        "verbs": {"noted": 1},
                        "first_at": "2026-01-01T00:01:00Z",
                        "last_at": "2026-01-01T00:01:00Z",
                        "current_note_count": 1,
                        "note_preview": {
                            "id": "b-1:2",
                            "author": "bbugyi200.athena.0aa",
                            "timestamp": "2026-01-01T00:01:00Z",
                            "text": "x",
                            "truncated": false,
                        },
                        "stream_id": "b-1",
                    },
                ],
            })
        );

        let only = py_bead_touch_index_query(
            py,
            index_str,
            Some(vec!["013".to_string()]),
        )
        .unwrap();
        let only = py_to_json_value(only.bind(py)).unwrap();
        assert_eq!(only["touches"], json!([query["touches"][0].clone()]));

        let fresh =
            py_bead_touch_index_status(py, beads_dir_str, index_str).unwrap();
        let fresh = py_to_json_value(fresh.bind(py)).unwrap();
        assert_eq!(
            fresh,
            json!({
                "schema_version": 3,
                "state": "fresh",
                "index_schema_version": 3,
                "generation": generation,
                "indexed_streams": 1,
                "current_streams": 1,
                "changed_streams": [],
                "vanished_streams": [],
            })
        );
    });
}

#[test]
fn bead_doctor_binding_keeps_contexts_optional_and_marks_unavailable() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("beads.db"), "").unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let doctor = module.getattr("bead_doctor").unwrap();
        let path = beads_dir.to_str().unwrap();

        let compatibility: Vec<String> =
            doctor.call1((path,)).unwrap().extract().unwrap();
        assert_eq!(compatibility, vec!["OK: no issues found"]);

        let unavailable: Vec<String> = doctor
            .call1((path, Vec::<String>::new()))
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(
                unavailable,
                vec![
                    "NOTE: bead design reference validation skipped: plan roots unavailable",
                    "NOTE: bead artifact reference validation skipped: reference context unavailable"
                ]
            );

        let available: Vec<String> = doctor
            .call1((path, Vec::<String>::new(), PyDict::new_bound(py)))
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(
                available,
                vec![
                    "NOTE: bead design reference validation skipped: plan roots unavailable"
                ]
            );
    });
}

#[test]
fn bead_mutation_bindings_preserve_changed_and_epic_preclaim() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let epic = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let phase = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    Python::with_gil(|py| {
        let path = beads_dir.to_str().unwrap();
        let first = py_bead_claim_for_agent_launch(
            py,
            path,
            &phase.id,
            "worker",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        assert!(py_to_json_value(first.bind(py)).unwrap()["changed"]
            .as_bool()
            .unwrap());

        let repeated = py_bead_claim_for_agent_launch(
            py,
            path,
            &phase.id,
            "worker",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        let repeated = py_to_json_value(repeated.bind(py)).unwrap();
        assert!(!repeated["changed"].as_bool().unwrap());
        assert_eq!(repeated["issue"]["updated_at"], "2026-01-01T00:02:00Z");

        let retained = py_bead_claim_for_agent_wait(
            py,
            path,
            &phase.id,
            "worker",
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        let retained = py_to_json_value(retained.bind(py)).unwrap();
        assert!(!retained["changed"].as_bool().unwrap());
        assert_eq!(retained["message"], "");

        let fields = json_value_to_py(
            py,
            &json!({
                "title": "Phase",
                "now": "2026-01-01T00:05:00Z"
            }),
        )
        .unwrap();
        let fields = fields.bind(py).downcast::<PyDict>().unwrap();
        let unchanged = py_bead_update(py, path, &phase.id, fields).unwrap();
        assert!(!py_to_json_value(unchanged.bind(py)).unwrap()["changed"]
            .as_bool()
            .unwrap());

        let assignments = PyList::empty_bound(py);
        append_json(
            py,
            &assignments,
            json!({
                "bead_id": phase.id,
                "agent_name": "worker-2"
            }),
        );
        let preclaimed = py_bead_preclaim_epic_work(
            py,
            path,
            &epic.id,
            &assignments,
            Some("land".to_string()),
            Some("2026-01-01T00:06:00Z".to_string()),
        )
        .unwrap();
        let preclaimed = py_to_json_value(preclaimed.bind(py)).unwrap();
        assert!(preclaimed["changed"].as_bool().unwrap());
        assert_eq!(preclaimed["issue_ids"], json!([phase.id, epic.id]));
        assert_eq!(preclaimed["rollback_preclaims"][1]["bead_id"], epic.id);
    });
}

#[test]
fn bead_update_binding_preserves_resolution_presence_semantics() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Closable".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            task_type_fields: [
                ("location".to_string(), "tests".to_string()),
                ("repro".to_string(), "run pytest".to_string()),
            ]
            .into_iter()
            .collect(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    core_bead_close_issues_with_note(
        &beads_dir,
        std::slice::from_ref(&issue.id),
        Some("verified".to_string()),
        Some(BeadResolutionWire::Done),
        false,
        None,
        None,
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap();

    Python::with_gil(|py| {
        let path = beads_dir.to_str().unwrap();
        let omitted = json_value_to_py(
            py,
            &json!({
                "title": "Retitled",
                "now": "2026-01-01T00:02:00Z"
            }),
        )
        .unwrap();
        let omitted = omitted.bind(py).downcast::<PyDict>().unwrap();
        let result = py_bead_update(py, path, &issue.id, omitted).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["issue"]["resolution"], json!("done"));

        let clear = json_value_to_py(
            py,
            &json!({
                "resolution": null,
                "now": "2026-01-01T00:03:00Z"
            }),
        )
        .unwrap();
        let clear = clear.bind(py).downcast::<PyDict>().unwrap();
        let result = py_bead_update(py, path, &issue.id, clear).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert!(result["issue"].get("resolution").is_none());

        let set = json_value_to_py(
            py,
            &json!({
                "resolution": "superseded",
                "now": "2026-01-01T00:04:00Z"
            }),
        )
        .unwrap();
        let set = set.bind(py).downcast::<PyDict>().unwrap();
        let result = py_bead_update(py, path, &issue.id, set).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["issue"]["resolution"], json!("superseded"));
    });
}

#[test]
fn bead_update_many_binding_applies_batch_and_reports_unchanged() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let first = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let second = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Second task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    Python::with_gil(|py| {
        let path = beads_dir.to_str().unwrap();
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_update_many").is_ok());

        let fields = json_value_to_py(
            py,
            &json!({
                "status": "in_progress",
                "now": "2026-01-01T00:02:00Z"
            }),
        )
        .unwrap();
        let fields = fields.bind(py).downcast::<PyDict>().unwrap();

        let first_call = py_bead_update_many(
            py,
            path,
            vec![first.id.clone(), second.id.clone()],
            fields,
        )
        .unwrap();
        let first_call = py_to_json_value(first_call.bind(py)).unwrap();
        assert!(first_call["changed"].as_bool().unwrap());
        assert_eq!(
            first_call["issue_ids"],
            json!([first.id.clone(), second.id.clone()])
        );
        assert_eq!(first_call["unchanged_ids"], json!([]));
        assert_eq!(first_call["issues"][0]["status"], "in_progress");
        assert_eq!(first_call["issues"][1]["status"], "in_progress");

        let repeat_call = py_bead_update_many(
            py,
            path,
            vec![first.id.clone(), second.id.clone()],
            fields,
        )
        .unwrap();
        let repeat_call = py_to_json_value(repeat_call.bind(py)).unwrap();
        assert!(!repeat_call["changed"].as_bool().unwrap());
        assert_eq!(repeat_call["issue_ids"], json!([]));
        assert_eq!(repeat_call["unchanged_ids"], json!([first.id, second.id]));
    });
}

#[test]
fn bead_plus_one_binding_exports_structured_atomic_result() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let task = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_plus_one").is_ok());
        let result = py_bead_plus_one(
            py,
            beads_dir.to_str().unwrap(),
            &task.id,
            "reporter-agent",
            "reproduced",
            Some(vec!["research:202608/repro.md".to_string()]),
            Some("2026-01-02T00:00:00Z".to_string()),
            None,
        )
        .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["issue"]["status"], "ready");
        assert_eq!(
            result["issue"]["plus_one_evidence"][0]["reporter"],
            "reporter-agent"
        );
    });
}

#[test]
fn bead_note_edit_and_remove_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Notes".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let noted = core_bead_append_issue_note(
        &beads_dir,
        &issue.id,
        "first draft",
        Some("agent-1".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    let note_id = noted.notes[0].id.clone();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_note_edit").is_ok());
        assert!(module.getattr("bead_note_remove").is_ok());

        let edited = py_bead_note_edit(
            py,
            beads_dir.to_str().unwrap(),
            &issue.id,
            &note_id,
            "corrected",
            Some("agent-2".to_string()),
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        let edited = py_to_json_value(edited.bind(py)).unwrap();
        assert_eq!(edited["issue"]["notes"][0]["text"], "corrected");
        assert_eq!(edited["issue"]["notes"][0]["edited_by"], "agent-2");

        let removed = py_bead_note_remove(
            py,
            beads_dir.to_str().unwrap(),
            &issue.id,
            &note_id,
            Some("agent-2".to_string()),
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        let removed = py_to_json_value(removed.bind(py)).unwrap();
        assert!(removed["issue"]["notes"]
            .as_array()
            .map(|notes| notes.is_empty())
            .unwrap_or(true));

        let missing = py_bead_note_edit(
            py,
            beads_dir.to_str().unwrap(),
            &issue.id,
            &note_id,
            "too late",
            None,
            None,
        );
        assert!(missing.is_err());
    });
}

#[test]
fn bead_snooze_bindings_round_trip_the_whole_lifecycle() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let task = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let beads_dir = beads_dir.to_str().unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "bead_snooze",
            "bead_snooze_cancel",
            "bead_needs_snoozed_status_migration",
            "bead_snoozed_status_migration_sql",
            "bead_needs_flag_type_migration",
            "bead_flag_type_migration_sql",
            "bead_needs_drop_flag_type_migration",
            "bead_drop_flag_type_migration_sql",
            "bead_prune_removed_flag_event_streams",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let snoozed = py_bead_snooze(
            py,
            beads_dir,
            &task.id,
            "2026-01-04T00:00:00Z",
            Some(2),
            "waiting on upstream",
            "owner",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        let snoozed = py_to_json_value(snoozed.bind(py)).unwrap();
        assert_eq!(snoozed["issue"]["status"], "snoozed");
        assert_eq!(snoozed["issue"]["snooze"]["until"], "2026-01-04T00:00:00Z");
        assert_eq!(snoozed["issue"]["snooze"]["plus_one_target"], 2);

        let canceled = py_bead_snooze_cancel(
            py,
            beads_dir,
            &task.id,
            "owner",
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();
        let canceled = py_to_json_value(canceled.bind(py)).unwrap();
        assert_eq!(canceled["issue"]["status"], "ready");
        assert!(canceled["issue"].get("snooze").is_none());
    });
}

#[test]
fn bead_drop_flag_type_migration_bindings_are_exported() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module
            .getattr("bead_needs_drop_flag_type_migration")
            .is_ok());
        assert!(module.getattr("bead_drop_flag_type_migration_sql").is_ok());
        assert!(module
            .getattr("bead_prune_removed_flag_event_streams")
            .is_ok());
        assert!(py_bead_needs_drop_flag_type_migration(Some(
                "flag        TEXT, CHECK((issue_type = 'flag') = (flag IS NOT NULL))"
            )));
        assert!(!py_bead_needs_drop_flag_type_migration(Some(
            "CREATE TABLE issues (id TEXT, task_type_fields TEXT)"
        )));
        assert_eq!(
            py_bead_drop_flag_type_migration_sql(),
            core_bead_drop_flag_type_migration_sql()
        );
    });
}

fn temp_beads_dir() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::Builder::new()
        .prefix("sase-core-py-bead-")
        .tempdir()
        .unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    (temp, beads_dir)
}

#[test]
fn bead_search_binding_round_trips_json_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, beads_dir) = temp_beads_dir();
        fs::write(
            beads_dir.join("issues.jsonl"),
            serde_json::to_string(&json!({
                "id": "beads-1.1",
                "title": "Needle binding",
                "status": "open",
                "issue_type": "phase",
                "parent_id": "beads-1",
                "created_at": "2026-01-01T00:01:00Z",
                "updated_at": "2026-01-01T00:01:00Z"
            }))
            .unwrap()
                + "\n",
        )
        .unwrap();

        let result = py_bead_search(
            py,
            beads_dir.to_str().unwrap(),
            "needle",
            None,
            None,
            None,
            Some(1),
            false,
        )
        .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(value[0]["issue"]["id"], json!("beads-1.1"));
        assert_eq!(value[0]["matched_fields"], json!(["title"]));
    });
}

#[test]
fn bead_search_binding_accepts_regex_keyword() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, beads_dir) = temp_beads_dir();
        fs::write(
            beads_dir.join("issues.jsonl"),
            serde_json::to_string(&json!({
                "id": "beads-1.1",
                "title": "Auth binding",
                "status": "open",
                "issue_type": "phase",
                "parent_id": "beads-1",
                "created_at": "2026-01-01T00:01:00Z",
                "updated_at": "2026-01-01T00:01:00Z"
            }))
            .unwrap()
                + "\n",
        )
        .unwrap();
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(wrap_pyfunction!(py_bead_search, &module).unwrap())
            .unwrap();
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("regex", true).unwrap();

        let result = module
            .getattr("bead_search")
            .unwrap()
            .call(
                (beads_dir.to_str().unwrap(), r"auth\s+binding"),
                Some(&kwargs),
            )
            .unwrap();
        let value = py_to_json_value(&result).unwrap();

        assert_eq!(value[0]["issue"]["id"], json!("beads-1.1"));
        assert_eq!(value[0]["matched_fields"], json!(["title"]));
    });
}

#[test]
fn bead_merge_event_streams_binding_preserves_replay_stable_union() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_merge_event_streams").is_ok());

        let event = |event_id: &str, timestamp: &str, operation: &str| {
            json!({
                "schema_version": 1,
                "event_id": event_id,
                "timestamp": timestamp,
                "actor": "owner@example.com",
                "operation": operation,
                "issue_id": "gold-1",
                "payload": {"kind": operation},
            })
        };
        let first =
            event("legacy-first", "2026-01-01T00:01:00Z", "ready_marked");
        let second =
            event("legacy-second", "2026-01-01T00:02:00Z", "ready_unmarked");
        let before =
            event("added-before", "2026-01-01T00:00:00Z", "ready_marked");
        let between =
            event("added-between", "2026-01-01T00:01:30Z", "ready_unmarked");
        let stream = |events: Vec<JsonValue>| {
            json!({
                "stream_id": "gold-1",
                "root_issue_id": "gold-1",
                "events": events,
            })
        };
        let base_value = stream(vec![first.clone(), second.clone()]);
        let ours_value = stream(vec![before, first.clone(), second.clone()]);
        let theirs_value = stream(vec![first, between, second]);
        let base_obj = json_value_to_py(py, &base_value).unwrap();
        let ours_obj = json_value_to_py(py, &ours_value).unwrap();
        let theirs_obj = json_value_to_py(py, &theirs_value).unwrap();
        let base = base_obj.bind(py).downcast::<PyDict>().unwrap();
        let ours = ours_obj.bind(py).downcast::<PyDict>().unwrap();
        let theirs = theirs_obj.bind(py).downcast::<PyDict>().unwrap();

        let result =
            py_bead_merge_event_streams(py, base, ours, theirs).unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(
            result["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|event| event["event_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "legacy-first",
                "legacy-second",
                "added-before",
                "added-between",
            ]
        );
    });
}

#[test]
fn bead_merge_event_streams_binding_exposes_typed_relocations() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let issue = |id: &str, title: &str, created_at: &str| {
            json!({
                "id": id,
                "title": title,
                "status": "open",
                "issue_type": "plan",
                "tier": "epic",
                "parent_id": null,
                "owner": "owner@example.com",
                "assignee": "",
                "created_at": created_at,
                "created_by": "owner@example.com",
                "updated_at": created_at,
                "closed_at": null,
                "close_reason": null,
                "resolution": null,
                "close_history": [],
                "description": "",
                "notes": "",
                "design": "",
                "refs": [],
                "links": [],
                "plus_one_evidence": [],
                "snooze": null,
                "model": "",
                "size": null,
                "task_type": null,
                "task_type_fields": {},
                "is_ready_to_work": false,
                "changespec_name": "",
                "changespec_bug_id": "",
                "external_ref": "",
                "dependencies": [],
            })
        };
        let created_event = |event_id: &str, title: &str, created_at: &str| {
            json!({
                "schema_version": 1,
                "event_id": event_id,
                "timestamp": created_at,
                "actor": "owner@example.com",
                "operation": "issue_created",
                "issue_id": "sase-ey",
                "payload": {
                    "kind": "issue_created",
                    "issue": issue("sase-ey", title, created_at),
                },
            })
        };
        let stream = |events: Vec<JsonValue>| {
            json!({
                "stream_id": "sase-ey",
                "root_issue_id": "sase-ey",
                "events": events,
            })
        };
        let base_value = stream(vec![]);
        let ours_value = stream(vec![created_event(
            "sase-ey:000001:issue_created:sase-ey:ours",
            "Ours",
            "2026-08-03T11:00:00Z",
        )]);
        let theirs_value = stream(vec![created_event(
            "sase-ey:000001:issue_created:sase-ey:theirs",
            "Theirs",
            "2026-08-03T11:00:01Z",
        )]);
        let base_obj = json_value_to_py(py, &base_value).unwrap();
        let ours_obj = json_value_to_py(py, &ours_value).unwrap();
        let theirs_obj = json_value_to_py(py, &theirs_value).unwrap();
        let base = base_obj.bind(py).downcast::<PyDict>().unwrap();
        let ours = ours_obj.bind(py).downcast::<PyDict>().unwrap();
        let theirs = theirs_obj.bind(py).downcast::<PyDict>().unwrap();

        let result = py_bead_merge_event_streams_with_relocation(
            py,
            base,
            ours,
            theirs,
            Some("sase-ez".to_string()),
        )
        .unwrap();
        let result = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(result["relocations"], json!([["sase-ey", "sase-ez"]]));
        assert_eq!(
            result["relocation_records"],
            json!([{
                "old_id": "sase-ey",
                "new_id": "sase-ez",
                "kind": "top_level_duplicate",
            }])
        );
    });
}

#[test]
fn bead_remove_many_binding_is_exported_and_removes_multiple_roots() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_remove").is_ok());
        assert!(module.getattr("bead_remove_many").is_ok());

        let (_temp, beads_dir) = temp_beads_dir();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                json!({
                    "id": "beads-1",
                    "title": "First",
                    "status": "open",
                    "issue_type": "plan",
                    "parent_id": null,
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z"
                }),
                json!({
                    "id": "beads-2",
                    "title": "Second",
                    "status": "open",
                    "issue_type": "plan",
                    "parent_id": null,
                    "created_at": "2026-01-01T00:01:00Z",
                    "updated_at": "2026-01-01T00:01:00Z"
                }),
            ]
            .into_iter()
            .map(|issue| serde_json::to_string(&issue).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = py_bead_remove_many(
            py,
            beads_dir.to_str().unwrap(),
            vec!["beads-2".to_string(), "beads-1".to_string()],
        )
        .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(value["issue_ids"], json!(["beads-2", "beads-1"]));
        assert_eq!(value["issues"][0]["id"], json!("beads-2"));
        assert_eq!(value["issues"][1]["id"], json!("beads-1"));
    });
}

#[test]
fn bead_size_check_relax_bindings_are_exported_and_forward_core_policy() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "bead_needs_resolution_migration",
            "bead_resolution_migration_sql",
            "bead_needs_external_ref_migration",
            "bead_external_ref_migration_sql",
            "bead_needs_size_check_relax_migration",
            "bead_size_check_relax_migration_sql",
            "bead_needs_task_ready_migration",
            "bead_task_ready_migration_sql",
            "bead_needs_task_type_migration",
            "bead_task_type_migration_sql",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        assert!(!py_bead_needs_size_check_relax_migration(None));
        assert!(py_bead_needs_size_check_relax_migration(Some(
            "size TEXT CHECK(size IN ('small','medium','large'))"
        )));
        assert!(!py_bead_needs_size_check_relax_migration(Some(
            "size TEXT CHECK(size IN \
                 ('xsmall','small','medium','large','xlarge'))"
        )));
        assert_eq!(
            py_bead_size_check_relax_migration_sql(),
            core_bead_size_check_relax_migration_sql()
        );
        assert!(py_bead_needs_task_ready_migration(Some(
            "CHECK(issue_type IN ('plan','phase'))"
        )));
        assert!(!py_bead_needs_task_ready_migration(Some(
            "CHECK(issue_type IN ('plan','phase','task')); \
                 CHECK(status IN ('open','ready','closed')); \
                 CHECK(status!='ready' OR issue_type='task')"
        )));
        assert_eq!(
            py_bead_task_ready_migration_sql(),
            core_bead_task_ready_migration_sql()
        );
        assert!(py_bead_needs_external_ref_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert!(!py_bead_needs_external_ref_migration(Some(
            "external_ref TEXT"
        )));
        assert_eq!(
            py_bead_external_ref_migration_sql(),
            core_bead_external_ref_migration_sql()
        );
        assert!(py_bead_needs_resolution_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert_eq!(
            py_bead_resolution_migration_sql(),
            core_bead_resolution_migration_sql()
        );
        assert!(!py_bead_needs_task_type_migration(None));
        assert!(py_bead_needs_task_type_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert!(!py_bead_needs_task_type_migration(Some(
            "task_type_fields TEXT NOT NULL DEFAULT '{}'"
        )));
        assert_eq!(
            py_bead_task_type_migration_sql(),
            core_bead_task_type_migration_sql()
        );
    });
}

#[test]
fn bead_create_binding_round_trips_task_type_and_fields() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");

    Python::with_gil(|py| {
        let request = json_value_to_py(
            py,
            &json!({
                "title": "Flaky test",
                "issue_type": "task",
                "size": "small",
                "task_type": "flake",
                "task_type_fields": {
                    "node_id": "tests/foo.py::test_bar",
                    "evidence": "failed then passed"
                },
                "now": "2026-01-01T00:00:00Z"
            }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let created =
            py_bead_create(py, beads_dir.to_str().unwrap(), request).unwrap();
        let value = py_to_json_value(created.bind(py)).unwrap();
        assert_eq!(value["issue"]["task_type"], "flake");
        assert_eq!(
            value["issue"]["task_type_fields"]["node_id"],
            "tests/foo.py::test_bar"
        );
        assert_eq!(
            value["issue"]["task_type_fields"]["evidence"],
            "failed then passed"
        );
    });
}

#[test]
fn bead_set_link_projections_binding_is_registered_and_projects_batch() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("bead_set_link_projection").is_ok());
        assert!(module.getattr("bead_set_link_projections").is_ok());

        let requests = PyList::empty_bound(py);
        append_json(
            py,
            &requests,
            json!({
                "issue_id": issue.id,
                "target_ref": "plan:202609/a.md",
                "relation": "related",
                "direction": "out",
                "present": true,
                "operation_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "description": "present edge",
                "origin": "manual",
                "uses": 1,
                "now": "2026-01-01T00:01:00Z"
            }),
        );
        append_json(
            py,
            &requests,
            json!({
                "issue_id": issue.id,
                "target_ref": "plan:202609/a.md",
                "relation": "related",
                "direction": "out",
                "present": false,
                "operation_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "now": "2026-01-01T00:02:00Z"
            }),
        );
        let result = py_bead_set_link_projections(
            py,
            beads_dir.to_str().unwrap(),
            &requests,
        )
        .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["operation"], json!("link_project"));
        assert_eq!(value["changed"], json!(true));
        assert_eq!(value["issue_ids"], json!([issue.id]));
    });
}

#[test]
fn bead_set_link_projections_binding_rejects_invalid_request_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let requests = PyList::empty_bound(py);
        append_json(py, &requests, json!("not-a-dict"));
        let error = py_bead_set_link_projections(py, "/tmp/beads", &requests)
            .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        assert!(error.to_string().contains(
            "requests[0] is not a valid BeadLinkProjectionRequestWire dict"
        ));

        let requests = PyList::empty_bound(py);
        append_json(
            py,
            &requests,
            json!({
                "issue_id": "sase-1",
                "target_ref": "plan:202609/a.md",
                "relation": "related",
                "direction": "sideways",
                "present": true,
                "operation_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }),
        );
        let error = py_bead_set_link_projections(py, "/tmp/beads", &requests)
            .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        let message = error.to_string();
        assert!(
            message.contains(
                "requests[0] is not a valid BeadLinkProjectionRequestWire dict"
            ),
            "{message}"
        );
        assert!(
            message.contains("sideways") || message.contains("unknown variant"),
            "{message}"
        );
    });
}

#[test]
fn bead_set_link_projections_binding_surfaces_core_validation() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    Python::with_gil(|py| {
        let requests = PyList::empty_bound(py);
        append_json(
            py,
            &requests,
            json!({
                "issue_id": issue.id,
                "target_ref": "plan:202609/a.md",
                "relation": "related",
                "direction": "out",
                "present": true,
                "operation_id": "not-a-hex-operation-id",
                "description": "present edge",
                "origin": "manual",
                "now": "2026-01-01T00:01:00Z"
            }),
        );
        let error = py_bead_set_link_projections(
            py,
            beads_dir.to_str().unwrap(),
            &requests,
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        assert!(error.to_string().contains(
                "artifact link operation_id must be 32 lowercase hexadecimal characters"
            ));
    });
}

#[test]
fn bead_manifest_repair_binding_round_trips_structured_outcome() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let (_temp, beads_dir) = temp_beads_dir();

        let result = py_bead_repair_event_store_manifest(
            py,
            beads_dir.to_str().unwrap(),
        )
        .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(value["status"], json!("noop"));
        assert_eq!(value["stream_count"], json!(0));
        assert!(value["manifest_path"]
            .as_str()
            .unwrap()
            .ends_with("events/manifest.json"));
    });
}

#[test]
fn bead_work_plan_binding_exposes_additive_bead_id_fields() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let issues = PyList::empty_bound(py);
        append_json(
            py,
            &issues,
            json!({
                "id": "beads-1",
                "title": "Epic",
                "status": "open",
                "issue_type": "plan",
                "tier": "epic",
                "parent_id": null
            }),
        );
        append_json(
            py,
            &issues,
            json!({
                "id": "beads-1.0",
                "title": "Closed blocker",
                "status": "closed",
                "issue_type": "phase",
                "parent_id": "beads-1"
            }),
        );
        append_json(
            py,
            &issues,
            json!({
                "id": "beads-1.1",
                "title": "Large phase",
                "status": "open",
                "issue_type": "phase",
                "parent_id": "beads-1",
                "size": "large",
                "dependencies": [{
                    "issue_id": "beads-1.1",
                    "depends_on_id": "beads-1.0"
                }]
            }),
        );

        let result =
            py_bead_build_epic_work_plan_from_issues(py, &issues, "beads-1")
                .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();

        assert_eq!(value["epic_id"], json!("beads-1"));
        assert_eq!(value["launch_tag_id"], json!("beads-1"));
        assert_eq!(value["total_phase_count"], json!(2));
        assert_eq!(value["phase_bead_ids"], json!(["beads-1.0", "beads-1.1"]));
        assert_eq!(value["waves"][0][0]["bead_id"], json!("beads-1.1"));
        assert_eq!(value["waves"][0][0]["agent_name"], json!("beads-1.1"));
        assert_eq!(value["waves"][0][0]["size"], json!("large"));
        assert_eq!(value["waves"][0][0]["waits_on"], json!([]));
        assert_eq!(
            value["waves"][0][0]["blocker_bead_ids"],
            json!(["beads-1.0"])
        );
        assert_eq!(value["waves"][0][0]["wave"], json!(0));
        assert_eq!(value["land_agent_name"], json!("beads-1.land"));
        assert_eq!(value["land_waits_on"], json!(["beads-1.1"]));
    });
}

#[test]
fn bead_close_binding_stamps_the_supplied_author_on_the_close_event() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
    let beads_dir = temp.path().join("beads");
    let issue = core_bead_create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Closable".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    Python::with_gil(|py| {
        let path = beads_dir.to_str().unwrap();
        py_bead_close(
            py,
            path,
            vec![issue.id.clone()],
            None,
            None,
            false,
            Some("2026-01-01T00:01:00Z".to_string()),
            None,
            Some("worker".to_string()),
        )
        .unwrap();
    });

    let streams = beads_dir.join("events/streams");
    let mut found = false;
    for entry in fs::read_dir(&streams).unwrap() {
        let text = fs::read_to_string(entry.unwrap().path()).unwrap();
        for line in text.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            if value["operation"] == "issue_closed"
                && value["issue_id"] == issue.id.as_str()
            {
                assert_eq!(value["actor"], "worker");
                assert_eq!(value["payload"]["closed_by"], "worker");
                found = true;
            }
        }
    }
    assert!(found, "expected one stamped issue_closed event");
}
