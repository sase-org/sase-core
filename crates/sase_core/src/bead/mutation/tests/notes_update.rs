use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
#[test]
fn updating_a_snoozed_bead_off_snoozed_drops_the_record() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", Some(2));

    let issue = update_issue(
        &beads_dir,
        &task_id,
        BeadUpdateFieldsWire {
            status: Some("ready".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert_eq!(issue.snooze, None);
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);
}

#[test]
fn update_refuses_the_snoozed_status_shortcut() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);

    let error = update_issue(
        &beads_dir,
        &task_id,
        BeadUpdateFieldsWire {
            status: Some("snoozed".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(
        error.message,
        "snoozed requires a wake time; use: sase bead snooze <id> -u <time>"
    );
}

#[test]
fn update_replaces_task_type_fields_and_replays_from_events() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let mut fields = BTreeMap::new();
    fields.insert("key".to_string(), "demo_key".to_string());
    fields.insert("kind".to_string(), "beta".to_string());
    fields.insert("remove_by_date".to_string(), "2026-12-01".to_string());
    fields.insert("remove_by_release".to_string(), "0.19.0".to_string());
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Retire demo_key".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("flag".to_string()),
            task_type_fields: fields.clone(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    fields.insert("remove_by_date".to_string(), "2026-12-15".to_string());
    fields.insert("remove_by_release".to_string(), "0.20.0".to_string());
    let updated = update_issue(
        &beads_dir,
        &task.id,
        BeadUpdateFieldsWire {
            task_type_fields: Some(fields.clone()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(
        updated
            .task_type_fields
            .get("remove_by_date")
            .map(String::as_str),
        Some("2026-12-15")
    );
    assert_eq!(
        updated
            .task_type_fields
            .get("remove_by_release")
            .map(String::as_str),
        Some("0.20.0")
    );
    assert_eq!(updated.task_type.as_deref(), Some("flag"));
    let stored = MutableStore::load(&beads_dir)
        .unwrap()
        .get_issue(&task.id)
        .unwrap()
        .clone();
    assert_eq!(stored.task_type_fields, fields);
    assert_eq!(reduces_to_store(&beads_dir), vec![stored]);

    let untyped = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Legacy untyped task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(untyped.kind, "validation");
    assert!(untyped.message.contains("requires an explicit task type"));
}

#[test]
fn update_with_matching_fields_is_a_quiet_no_op() {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
    let before = persisted_claim_state(&beads_dir);

    let result = update_issue(
        &beads_dir,
        &phase_id,
        BeadUpdateFieldsWire {
            title: Some("Phase".to_string()),
            status: Some("open".to_string()),
            assignee: Some(String::new()),
            model: Some(String::new()),
            now: Some("2026-01-01T00:09:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(result.operation, "update");
    assert!(!result.changed);
    assert_eq!(result.issue_ids, vec![phase_id]);
    assert_eq!(result.issue.unwrap().updated_at, "2026-01-01T00:01:00Z");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn update_fields_resolution_preserves_omitted_clear_and_set() {
    let omitted: BeadUpdateFieldsWire =
        serde_json::from_value(serde_json::json!({"title": "rename"})).unwrap();
    assert_eq!(omitted.resolution, None);
    let omitted_json = serde_json::to_value(&omitted).unwrap();
    assert!(omitted_json.get("resolution").is_none());

    let cleared: BeadUpdateFieldsWire =
        serde_json::from_value(serde_json::json!({"resolution": null}))
            .unwrap();
    assert_eq!(cleared.resolution, Some(None));
    let cleared_json = serde_json::to_value(&cleared).unwrap();
    assert_eq!(cleared_json["resolution"], serde_json::Value::Null);

    let set: BeadUpdateFieldsWire =
        serde_json::from_value(serde_json::json!({"resolution": "done"}))
            .unwrap();
    assert_eq!(set.resolution, Some(Some(BeadResolutionWire::Done)));
    let set_json = serde_json::to_value(&set).unwrap();
    assert_eq!(set_json["resolution"], serde_json::json!("done"));
}

#[test]
fn update_issue_resolution_omitted_null_and_set_mutate_deliberately() {
    let (_temp, beads_dir, issue_id) =
        closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));

    let omitted: BeadUpdateFieldsWire =
        serde_json::from_value(serde_json::json!({
            "title": "Retitled closed issue",
            "now": "2026-01-03T00:00:00Z"
        }))
        .unwrap();
    let after_omitted = update_issue(&beads_dir, &issue_id, omitted)
        .unwrap()
        .issue
        .unwrap();
    assert_eq!(after_omitted.resolution, Some(BeadResolutionWire::Done));

    let clear: BeadUpdateFieldsWire =
        serde_json::from_value(serde_json::json!({
            "resolution": null,
            "now": "2026-01-03T00:00:01Z"
        }))
        .unwrap();
    let after_clear = update_issue(&beads_dir, &issue_id, clear)
        .unwrap()
        .issue
        .unwrap();
    assert_eq!(after_clear.resolution, None);

    let set: BeadUpdateFieldsWire = serde_json::from_value(serde_json::json!({
        "resolution": "superseded",
        "now": "2026-01-03T00:00:02Z"
    }))
    .unwrap();
    let after_set = update_issue(&beads_dir, &issue_id, set)
        .unwrap()
        .issue
        .unwrap();
    assert_eq!(after_set.resolution, Some(BeadResolutionWire::Superseded));
}

#[test]
fn update_status_closed_rejects_open_descendants() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let original = [
        issue(
            "sase-1",
            "Plan",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ),
        issue(
            "sase-1.1",
            "Unfinished",
            "phase",
            Some("sase-1"),
            "open",
            "2026-01-01T00:01:00Z",
        ),
    ]
    .join("\n")
        + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let error = update_issue(
        &beads_dir,
        "sase-1",
        BeadUpdateFieldsWire {
            status: Some("closed".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert!(error.message.contains("cannot close sase-1"));
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
}

#[test]
fn update_out_of_closed_reopens_closed_ancestor() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "Root",
                "plan",
                None,
                "closed",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "Child",
                "phase",
                Some("sase-1"),
                "closed",
                "2026-01-01T00:01:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let outcome = update_issue(
        &beads_dir,
        "sase-1.1",
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(outcome.reopened_ancestor_ids, vec!["sase-1"]);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);
    assert_eq!(
        store.get_issue("sase-1.1").unwrap().status,
        StatusWire::InProgress
    );
}

#[test]
fn update_issues_applies_same_fields_to_every_target_in_one_pass() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "First",
                "task",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-2",
                "Second",
                "task",
                None,
                "open",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-3",
                "Third",
                "task",
                None,
                "open",
                "2026-01-01T00:02:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let outcome = update_issues(
        &beads_dir,
        &[
            "sase-1".to_string(),
            "sase-2".to_string(),
            "sase-3".to_string(),
        ],
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert!(outcome.changed);
    assert_eq!(outcome.issue_ids, vec!["sase-1", "sase-2", "sase-3"]);
    assert!(outcome.unchanged_ids.is_empty());
    assert_eq!(outcome.issues.len(), 3);
    for issue in &outcome.issues {
        assert_eq!(issue.status, StatusWire::InProgress);
    }

    let store = MutableStore::load(&beads_dir).unwrap();
    for issue_id in ["sase-1", "sase-2", "sase-3"] {
        assert_eq!(
            store.get_issue(issue_id).unwrap().status,
            StatusWire::InProgress
        );
    }
}

#[test]
fn update_issues_mixed_batch_reports_changed_and_unchanged() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "First",
                "task",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-2",
                "Second",
                "task",
                None,
                "in_progress",
                "2026-01-01T00:01:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let outcome = update_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-2".to_string()],
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert!(outcome.changed);
    assert_eq!(outcome.issue_ids, vec!["sase-1".to_string()]);
    assert_eq!(outcome.unchanged_ids, vec!["sase-2".to_string()]);
    assert_eq!(outcome.issues.len(), 2);
    assert_eq!(outcome.issues[0].id, "sase-1");
    assert_eq!(outcome.issues[0].updated_at, "2026-01-02T00:00:00Z");
    assert_eq!(outcome.issues[1].id, "sase-2");
    assert_eq!(outcome.issues[1].updated_at, "2026-01-01T00:01:00Z");
}

#[test]
fn update_issues_unknown_id_leaves_store_untouched() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let original = issue(
        "sase-1",
        "First",
        "task",
        None,
        "open",
        "2026-01-01T00:00:00Z",
    ) + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let error = update_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-missing".to_string()],
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert_eq!(error.kind, "not_found");
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
}

#[test]
fn update_issues_invalid_field_value_leaves_every_target_unmodified() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let original = [
        issue(
            "sase-1",
            "First",
            "task",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ),
        issue(
            "sase-2",
            "Second",
            "task",
            None,
            "open",
            "2026-01-01T00:01:00Z",
        ),
    ]
    .join("\n")
        + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let error = update_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-2".to_string()],
        BeadUpdateFieldsWire {
            model: Some("bad\nmodel".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
}

#[test]
fn update_issues_collapses_duplicate_ids_to_one_update() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "sase-1",
            "First",
            "task",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ) + "\n",
    )
    .unwrap();

    let outcome = update_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-1".to_string()],
        BeadUpdateFieldsWire {
            title: Some("Renamed".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(outcome.issue_ids, vec!["sase-1".to_string()]);
    assert_eq!(outcome.issues.len(), 1);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let update_events = streams[0]
        .events
        .iter()
        .filter(|event| event.operation == BeadEventOperationWire::IssueUpdated)
        .count();
    assert_eq!(update_events, 1);
}

#[test]
fn update_issues_closes_parent_and_child_regardless_of_argument_order() {
    for order in [
        vec!["sase-1".to_string(), "sase-1.1".to_string()],
        vec!["sase-1.1".to_string(), "sase-1".to_string()],
    ] {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Parent",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Child",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let outcome = update_issues(
            &beads_dir,
            &order,
            BeadUpdateFieldsWire {
                status: Some("closed".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(outcome.changed);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store.get_issue("sase-1").unwrap().status,
            StatusWire::Closed
        );
        assert_eq!(
            store.get_issue("sase-1.1").unwrap().status,
            StatusWire::Closed
        );
    }
}

#[test]
fn update_issues_status_closed_rejects_out_of_batch_descendant() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let original = [
        issue(
            "sase-1",
            "Parent",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ),
        issue(
            "sase-1.1",
            "In batch",
            "phase",
            Some("sase-1"),
            "open",
            "2026-01-01T00:01:00Z",
        ),
        issue(
            "sase-1.2",
            "Out of batch",
            "phase",
            Some("sase-1"),
            "open",
            "2026-01-01T00:02:00Z",
        ),
    ]
    .join("\n")
        + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let error = update_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-1.1".to_string()],
        BeadUpdateFieldsWire {
            status: Some("closed".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert!(error.message.contains("sase-1.2"));
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
}

#[test]
fn update_issues_reopens_shared_ancestor_only_once() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "Parent",
                "plan",
                None,
                "closed",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "First child",
                "phase",
                Some("sase-1"),
                "closed",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-1.2",
                "Second child",
                "phase",
                Some("sase-1"),
                "closed",
                "2026-01-01T00:02:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let outcome = update_issues(
        &beads_dir,
        &["sase-1.1".to_string(), "sase-1.2".to_string()],
        BeadUpdateFieldsWire {
            status: Some("in_progress".to_string()),
            now: Some("2026-01-02T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(outcome.reopened_ancestor_ids, vec!["sase-1".to_string()]);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);
}

#[test]
fn append_issue_note_appends_attributed_entries_and_event() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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

    let first = append_issue_note(
        &beads_dir,
        &issue.id,
        " first note ",
        Some("agent-1".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap();
    let second = append_issue_note(
        &beads_dir,
        &issue.id,
        "second note",
        Some("agent-1".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(first.operation, "note");
    assert_eq!(first.issue_ids, vec![issue.id.clone()]);
    assert_eq!(second.notes.len(), 2);
    assert_eq!(second.notes[0].text, "first note");
    assert_eq!(second.notes[1].text, "second note");
    assert_eq!(
            note_text(&second),
            "[2026-01-01T00:01:00Z · agent-1] first note\n\n[2026-01-01T00:02:00Z · agent-1] second note"
        );
    assert_eq!(second.updated_at, "2026-01-01T00:02:00Z");
    assert_reprojection_byte_stable(&beads_dir, "note append");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let note_events: Vec<_> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| {
            event.issue_id == issue.id
                && event.operation == BeadEventOperationWire::NoteAppended
        })
        .collect();
    assert_eq!(note_events.len(), 2);
    assert_eq!(note_events[0].actor, "agent-1");
    assert!(matches!(
        &note_events[1].payload,
        BeadEventPayloadWire::NoteAppended { entry }
            if entry == "second note"
    ));

    let reduced = reduce_event_streams(&streams).unwrap();
    let reduced_issue =
        reduced.iter().find(|issue| issue.id == second.id).unwrap();
    assert_eq!(reduced_issue.notes, second.notes);
}

#[test]
fn append_issue_note_defaults_blank_author_to_store_owner() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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

    let noted = append_issue_note(
        &beads_dir,
        &issue.id,
        "owner note",
        Some("  ".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(noted.notes.len(), 1);
    assert_eq!(noted.notes[0].author, "owner@example.com");
    assert_eq!(
        note_text(&noted),
        "[2026-01-01T00:01:00Z · owner@example.com] owner note"
    );
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let note_event = streams[0].events.last().unwrap();
    assert_eq!(note_event.actor, "owner@example.com");
}

#[test]
fn append_issue_note_rejects_blank_entry_without_writing() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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
    let before = persisted_claim_state(&beads_dir);

    let error = append_issue_note(&beads_dir, &issue.id, " \t ", None, None)
        .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert_eq!(error.message, "note entry cannot be empty or blank");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn edit_issue_note_rewrites_text_and_preserves_original_authorship() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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
    let noted = append_issue_note(
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

    let edited = edit_issue_note(
        &beads_dir,
        &issue.id,
        &note_id,
        " corrected ",
        Some("agent-2".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(edited.notes.len(), 1);
    assert_eq!(edited.notes[0].id, note_id);
    assert_eq!(edited.notes[0].text, "corrected");
    assert_eq!(edited.notes[0].timestamp, "2026-01-01T00:01:00Z");
    assert_eq!(edited.notes[0].author, "agent-1");
    assert_eq!(
        edited.notes[0].edited_at.as_deref(),
        Some("2026-01-01T00:02:00Z")
    );
    assert_eq!(edited.notes[0].edited_by.as_deref(), Some("agent-2"));
    assert_eq!(edited.updated_at, "2026-01-01T00:02:00Z");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let operations: Vec<_> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| event.issue_id == issue.id)
        .map(|event| event.operation)
        .collect();
    assert_eq!(
        operations,
        vec![
            BeadEventOperationWire::IssueCreated,
            BeadEventOperationWire::NoteAppended,
            BeadEventOperationWire::NoteEdited,
        ]
    );

    let reduced = reduce_event_streams(&streams).unwrap();
    let reduced_issue =
        reduced.iter().find(|found| found.id == issue.id).unwrap();
    assert_eq!(reduced_issue.notes, edited.notes);
}

#[test]
fn edit_issue_note_rejects_unknown_note_id_without_writing() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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
    let before = persisted_claim_state(&beads_dir);

    let error = edit_issue_note(
        &beads_dir,
        &issue.id,
        "does-not-exist",
        "text",
        None,
        None,
    )
    .unwrap_err();

    assert_eq!(error.kind, "not_found");
    assert_eq!(error.message, "Note not found: does-not-exist");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn edit_issue_note_rejects_blank_text_without_writing() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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
    let noted = append_issue_note(
        &beads_dir,
        &issue.id,
        "first draft",
        Some("agent-1".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    let before = persisted_claim_state(&beads_dir);

    let error = edit_issue_note(
        &beads_dir,
        &issue.id,
        &noted.notes[0].id,
        " \t ",
        None,
        None,
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert_eq!(error.message, "note text cannot be empty or blank");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn remove_issue_note_retracts_the_record_and_history_still_replays_it() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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
    let noted = append_issue_note(
        &beads_dir,
        &issue.id,
        "retract me",
        Some("agent-1".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    let note_id = noted.notes[0].id.clone();

    let removed = remove_issue_note(
        &beads_dir,
        &issue.id,
        &note_id,
        Some("agent-2".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();

    assert!(removed.notes.is_empty());
    assert_eq!(removed.updated_at, "2026-01-01T00:02:00Z");
    assert_eq!(note_text(&removed), "");

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let events: Vec<_> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| event.issue_id == issue.id)
        .collect();
    let removed_event = events
        .iter()
        .find(|event| event.operation == BeadEventOperationWire::NoteRemoved)
        .unwrap();
    assert!(matches!(
        &removed_event.payload,
        BeadEventPayloadWire::NoteRemoved { note_id: removed_id }
            if *removed_id == note_id
    ));

    let reduced = reduce_event_streams(&streams).unwrap();
    let reduced_issue =
        reduced.iter().find(|found| found.id == issue.id).unwrap();
    assert!(reduced_issue.notes.is_empty());
}

#[test]
fn remove_issue_note_rejects_unknown_note_id_without_writing() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
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
    let before = persisted_claim_state(&beads_dir);

    let error =
        remove_issue_note(&beads_dir, &issue.id, "does-not-exist", None, None)
            .unwrap_err();

    assert_eq!(error.kind, "not_found");
    assert_eq!(error.message, "Note not found: does-not-exist");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn close_with_note_appends_to_every_requested_issue_before_close() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let first = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First".to_string(),
            issue_type: IssueTypeWire::Plan,
            notes: "Existing context".to_string(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let second = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Second".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let result = close_issues_with_note(
        &beads_dir,
        &[first.id.clone(), second.id.clone()],
        None,
        None,
        false,
        Some(" verified with cargo test ".to_string()),
        Some("agent-1".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();

    assert!(result.changed);
    assert_eq!(result.issue_ids, vec![first.id.clone(), second.id.clone()]);
    assert_eq!(result.issues.len(), 2);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
            note_text(store.get_issue(&first.id).unwrap()),
            "[2026-01-01T00:00:00Z · owner@example.com] Existing context\n\n[2026-01-01T00:02:00Z · agent-1] verified with cargo test"
        );
    assert_eq!(
        note_text(store.get_issue(&second.id).unwrap()),
        "[2026-01-01T00:02:00Z · agent-1] verified with cargo test"
    );
    for issue_id in [&first.id, &second.id] {
        let issue = store.get_issue(issue_id).unwrap();
        assert_eq!(issue.status, StatusWire::Closed);
        assert_eq!(issue.updated_at, "2026-01-01T00:02:00Z");
        let stream = store
            .streams
            .all()
            .iter()
            .find(|stream| stream.root_issue_id == *issue_id)
            .unwrap();
        assert_eq!(
            stream
                .events
                .iter()
                .rev()
                .take(2)
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            vec![
                BeadEventOperationWire::IssueClosed,
                BeadEventOperationWire::NoteAppended,
            ]
        );
        assert_eq!(stream.events[stream.events.len() - 2].actor, "agent-1");
    }
    assert_reprojection_byte_stable(&beads_dir, "close with note");
}

#[test]
fn close_with_note_rejects_blank_entry_without_writing() {
    let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
    let before = persisted_claim_state(&beads_dir);

    let error = close_issues_with_note(
        &beads_dir,
        &[phase_id],
        None,
        None,
        false,
        Some(" \t ".to_string()),
        None,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert_eq!(error.message, "note entry cannot be empty or blank");
    assert_eq!(persisted_claim_state(&beads_dir), before);
}
