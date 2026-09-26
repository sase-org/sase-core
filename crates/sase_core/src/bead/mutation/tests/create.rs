use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::event_streams_dir;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::read::read_store_issues;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use crate::bead::wire::StatusWire;
use crate::bead::wire::CREATION_REASON_MAX_LEN;
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

use crate::bead::config::default_config;
use crate::bead::config::load_config;
use crate::bead::config::save_config;
use crate::bead::config::BeadConfigWire;
#[test]
fn create_requires_size_only_for_new_tasks() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let before = fs::read(beads_dir.join("issues.jsonl")).unwrap();

    let error = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Missing size".to_string(),
            issue_type: IssueTypeWire::Task,
            ..Default::default()
        },
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert!(error.message.contains("requires an explicit size"));
    assert_eq!(fs::read(beads_dir.join("issues.jsonl")).unwrap(), before);
}

#[test]
fn create_requires_task_type_only_for_new_tasks() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let before = fs::read(beads_dir.join("issues.jsonl")).unwrap();

    let error = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Missing task type".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert!(error.message.contains("requires an explicit task type"));
    assert_eq!(fs::read(beads_dir.join("issues.jsonl")).unwrap(), before);
}

#[test]
fn init_store_writes_a_root_level_store_for_a_dot_dirname() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("beads-sidecar");
    fs::create_dir_all(&root).unwrap();

    init_store(&root, ".", "sase", "").unwrap();

    assert!(root.join("config.json").is_file());
    assert!(root.join("issues.jsonl").is_file());
    assert!(root.join("beads.db").is_file());
}

#[test]
fn create_resolves_creator_from_phase_parent_then_store_owner() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");

    let blank = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Blank explicit creator".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("   ".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(blank.created_by, "owner@example.com");
    let absent = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Absent explicit creator".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(absent.created_by, "owner@example.com");

    let parent = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Attributed epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            created_by: Some("bbugyi200.athena.q8--plan".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let inherited = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Inherited phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(parent.id.clone()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(inherited.created_by, "bbugyi200.athena.q8--plan");
    let overridden = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Explicit phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(parent.id.clone()),
            created_by: Some("bbugyi200.athena.other".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(overridden.created_by, "bbugyi200.athena.other");

    let child_plan = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Child plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            parent_id: Some(parent.id),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(child_plan.created_by, "owner@example.com");
    let missing_parent = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Missing-parent phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some("sase-missing".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(missing_parent.created_by, "owner@example.com");
}

#[test]
fn phase_with_blank_parent_creator_falls_back_to_store_owner() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "").unwrap();
    let beads_dir = temp.path().join("beads");
    let parent = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Legacy unattributed epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(parent.created_by.is_empty());
    let mut config =
        load_config(&beads_dir, default_config("sase", "")).unwrap();
    config.owner = "owner@example.com".to_string();
    save_config(&beads_dir, &config).unwrap();

    let phase = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Fallback phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(parent.id),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(phase.created_by, "owner@example.com");
}

#[test]
fn task_type_create_rejects_cross_field_and_slug_errors() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");

    let on_plan = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            task_type: Some("flake".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(
        on_plan.message,
        "Only task issues can carry task_type metadata"
    );

    let mut fields = BTreeMap::new();
    fields.insert("node_id".to_string(), "tests/foo.py::test_bar".to_string());
    let fields_without_type = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Untyped fields".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type_fields: fields,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(fields_without_type
        .message
        .contains("requires an explicit task type"));

    let bad_slug = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Bad slug".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("Not_Snake".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(bad_slug
        .message
        .contains("task_type must be a non-empty snake_case slug"));
}

#[test]
fn task_create_and_ready_updates_round_trip_through_events() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Discovered follow-up".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Medium),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(task.issue_type, IssueTypeWire::Task);
    assert_eq!(task.status, StatusWire::Open);
    assert_eq!(task.parent_id, None);
    assert_eq!(task.tier, None);

    let ready = update_issue(
        &beads_dir,
        &task.id,
        BeadUpdateFieldsWire {
            status: Some("ready".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_reprojection_byte_stable(&beads_dir, "update");
    assert_eq!(ready.status, StatusWire::Ready);
    assert_eq!(
        MutableStore::load(&beads_dir)
            .unwrap()
            .get_issue(&task.id)
            .unwrap()
            .status,
        StatusWire::Ready
    );
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert!(streams
        .iter()
        .flat_map(|stream| &stream.events)
        .any(|event| {
            matches!(
                &event.payload,
                BeadEventPayloadWire::IssueUpdated { fields }
                    if fields.status == Some(StatusWire::Ready)
            )
        }));

    let plan = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Plan".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let phase = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(plan.id),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let error = update_issue(
        &beads_dir,
        &phase.id,
        BeadUpdateFieldsWire {
            status: Some("ready".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.message, "Only task issues can have ready status");
}

#[test]
fn create_top_level_uses_current_store_max_and_persists_counter() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sase/sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(
        &beads_dir,
        &BeadConfigWire {
            issue_prefix: "sase".to_string(),
            next_counter: 1,
            owner: String::new(),
        },
    )
    .unwrap();
    fs::write(
            beads_dir.join("issues.jsonl"),
            r#"{"id":"sase-z","title":"Other","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        )
        .unwrap();

    let result = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Next".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(result.issue.unwrap().id, "sase-10");
    assert_eq!(
        load_config(&beads_dir, default_config("x", ""))
            .unwrap()
            .next_counter,
        37
    );
}

#[test]
fn mark_ready_rejects_phase_and_idempotent_plan() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
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
                "A",
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

    assert_eq!(
        mark_ready_to_work(&beads_dir, "sase-1.1", None)
            .unwrap_err()
            .kind,
        "not_a_plan"
    );
    mark_ready_to_work(&beads_dir, "sase-1", None).unwrap();
    assert_eq!(
        mark_ready_to_work(&beads_dir, "sase-1", None)
            .unwrap_err()
            .kind,
        "already_ready"
    );
}

#[test]
fn create_and_update_model() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let created = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            model: " codex/gpt-5.5 ".to_string(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(created.model, "codex/gpt-5.5");

    let updated = update_issue(
        &beads_dir,
        &created.id,
        BeadUpdateFieldsWire {
            model: Some("#pro".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(updated.model, "#pro");
}

#[test]
fn create_rejects_duplicate_external_ref_without_writing() {
    let (_temp, beads_dir) = external_ref_store();
    let first =
        create_plan_with_external_ref(&beads_dir, "First", "bug:sase#42");

    let error = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Duplicate".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            external_ref: "bug:sase#42".to_string(),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert_eq!(error.kind, "conflict");
    assert!(error.message.contains("external_ref bug:sase#42"));
    assert!(error.message.contains(&first.id));
    let issues = read_store_issues(&beads_dir).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].id, first.id);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].events.len(), 1);
}

#[test]
fn external_ref_create_update_clear_and_batch_conflicts_are_atomic() {
    let (_temp, beads_dir) = external_ref_store();
    let plan = create_plan_with_external_ref(&beads_dir, "Plan", "bug:sase#42");
    let phase = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(plan.id.clone()),
            external_ref: "bug:sase#43".to_string(),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let task = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            external_ref: "bug:sase#44".to_string(),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let error = update_issue(
        &beads_dir,
        &phase.id,
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#42".to_string()),
            now: Some("2026-01-01T00:03:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind, "conflict");
    assert_eq!(
        read_store_issues(&beads_dir)
            .unwrap()
            .into_iter()
            .find(|issue| issue.id == phase.id)
            .unwrap()
            .external_ref,
        "bug:sase#43"
    );

    let cleared = update_issue(
        &beads_dir,
        &task.id,
        BeadUpdateFieldsWire {
            external_ref: Some(String::new()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(cleared.external_ref, "");

    let error = update_issues(
        &beads_dir,
        &[task.id.clone(), plan.id.clone()],
        BeadUpdateFieldsWire {
            external_ref: Some("bug:sase#99".to_string()),
            now: Some("2026-01-01T00:05:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind, "conflict");

    let issues = read_store_issues(&beads_dir).unwrap();
    assert_eq!(
        issues
            .iter()
            .find(|issue| issue.id == plan.id)
            .unwrap()
            .external_ref,
        "bug:sase#42"
    );
    assert_eq!(
        issues
            .iter()
            .find(|issue| issue.id == task.id)
            .unwrap()
            .external_ref,
        ""
    );
}

#[test]
fn create_persists_creation_reason_through_projection_and_replay() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");

    let issue = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Retry race".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            creation_reason: Some(
                "  A second agent reproduced dropped retries  ".to_string(),
            ),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(
        issue.creation_reason,
        "A second agent reproduced dropped retries"
    );

    // The normal projection persists the reason verbatim.
    let raw = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(raw.contains(
        "\"creation_reason\":\"A second agent reproduced dropped retries\""
    ));

    // The reason is immutable through normal updates, in both the
    // projection and the event replay.
    let updated = update_issue(
        &beads_dir,
        &issue.id,
        BeadUpdateFieldsWire {
            title: Some("Retry race (renamed)".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(updated.creation_reason, issue.creation_reason);
    assert_reprojection_byte_stable(&beads_dir, "reasoned update");
    assert_reopen_parity(&beads_dir, &issue.id, "creation reason");
}

#[test]
fn create_rejects_blank_and_overlong_reasons_without_writing() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let before = fs::read(beads_dir.join("issues.jsonl")).unwrap();

    for reason in [Some(String::new()), Some("   ".to_string())] {
        let error = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Blank reason".to_string(),
                issue_type: IssueTypeWire::Plan,
                creation_reason: reason,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.kind, "validation");
        assert!(error.message.contains("cannot be empty or blank"));
    }

    let error = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Overlong reason".to_string(),
            issue_type: IssueTypeWire::Plan,
            creation_reason: Some("r".repeat(CREATION_REASON_MAX_LEN + 1)),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind, "validation");
    assert!(error.message.contains("at most 2000 characters"));

    // Rejected reasons never reach the store or the event streams.
    assert_eq!(fs::read(beads_dir.join("issues.jsonl")).unwrap(), before);
    let streams_dir = event_streams_dir(&beads_dir);
    assert!(
        !streams_dir.exists()
            || fs::read_dir(&streams_dir).unwrap().next().is_none()
    );

    // An absent reason stays the historical empty state for older clients.
    let legacy = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Reasonless".to_string(),
            issue_type: IssueTypeWire::Plan,
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    assert!(legacy.creation_reason.is_empty());
    assert_reopen_parity(&beads_dir, &legacy.id, "reasonless create");
}

#[test]
fn create_rejects_model_control_characters() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let err = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            model: "codex/gpt-5.5\n%tag:bad".to_string(),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert!(err.message.contains("model cannot contain"));
}
