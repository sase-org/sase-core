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
use std::fs;
use tempfile::tempdir;

use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::wire::BeadReopenCauseWire;
#[test]
fn forced_close_plan_sweeps_open_children_before_parent() {
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
            issue(
                "sase-1.2",
                "B",
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

    let result = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        Some("Canceled unfinished work".to_string()),
        Some(BeadResolutionWire::Canceled),
        true,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-1.1", "sase-1"]);
    assert_eq!(result.closed_ids, vec!["sase-1.1", "sase-1"]);
    assert_eq!(result.cascade_closed_ids, vec!["sase-1.1"]);
    assert!(result.already_closed_ids.is_empty());
    assert!(result.noted_ids.is_empty());
    let exported = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
    assert!(
        exported.contains(r#""id":"sase-1.1","title":"A","status":"closed""#)
    );
    assert!(exported.contains(
        r#""close_reason":"forced by sase-1: Canceled unfinished work""#
    ));
    assert!(exported.contains(r#""resolution":"canceled""#));
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let forced_ids = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .find_map(|event| match &event.payload {
            BeadEventPayloadWire::IssueClosed {
                forced_descendant_ids,
                ..
            } if event.issue_id == "sase-1" => {
                Some(forced_descendant_ids.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(forced_ids, vec!["sase-1.1"]);
}

#[test]
fn unforced_close_with_open_descendant_fails_without_writing() {
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
            "in_progress",
            "2026-01-01T00:01:00Z",
        ),
    ]
    .join("\n")
        + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let error = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        None,
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap_err();

    assert!(error.message.contains("cannot close sase-1"));
    assert!(error.message.contains("sase-1.1 (in_progress)"));
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
    assert!(!beads_dir.join("events").exists());
}

#[test]
fn batch_close_preflights_every_request_before_writing() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let original = [
        issue(
            "sase-1",
            "First",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ),
        issue(
            "sase-2",
            "Second",
            "plan",
            None,
            "open",
            "2026-01-01T00:01:00Z",
        ),
        issue(
            "sase-2.1",
            "Unfinished",
            "phase",
            Some("sase-2"),
            "open",
            "2026-01-01T00:02:00Z",
        ),
    ]
    .join("\n")
        + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let error = close_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-2".to_string()],
        None,
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap_err();

    assert!(error.message.contains("cannot close sase-2"));
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
    assert!(!beads_dir.join("events").exists());
}

#[test]
fn repeat_close_is_write_free_and_classified_as_already_closed() {
    let (_temp, beads_dir, issue_id) =
        closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));
    let before = persisted_claim_state(&beads_dir);

    let result = close_issues(
        &beads_dir,
        std::slice::from_ref(&issue_id),
        None,
        None,
        false,
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap();

    assert!(!result.changed);
    assert!(result.issue_ids.is_empty());
    assert!(result.closed_ids.is_empty());
    assert_eq!(result.already_closed_ids, vec![issue_id.clone()]);
    assert!(result.noted_ids.is_empty());
    assert!(result.cascade_closed_ids.is_empty());
    assert_eq!(result.issues.len(), 1);
    assert_eq!(
        result.issues[0].closed_at.as_deref(),
        Some("2026-01-02T00:00:00Z")
    );
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn repeat_close_with_note_writes_only_the_note() {
    let (_temp, beads_dir, issue_id) =
        closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));

    let result = close_issues_with_note(
        &beads_dir,
        std::slice::from_ref(&issue_id),
        None,
        None,
        false,
        Some("extra evidence".to_string()),
        Some("agent-1".to_string()),
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap();

    assert!(result.changed);
    assert_eq!(result.issue_ids, vec![issue_id.clone()]);
    assert!(result.closed_ids.is_empty());
    assert_eq!(result.already_closed_ids, vec![issue_id.clone()]);
    assert_eq!(result.noted_ids, vec![issue_id.clone()]);
    assert!(result.cascade_closed_ids.is_empty());
    let issue = MutableStore::load(&beads_dir)
        .unwrap()
        .get_issue(&issue_id)
        .unwrap()
        .clone();
    assert_eq!(issue.closed_at.as_deref(), Some("2026-01-02T00:00:00Z"));
    assert_eq!(issue.close_reason.as_deref(), Some("verified"));
    assert!(note_text(&issue).contains("extra evidence"));
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert_eq!(
        streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.issue_id == issue_id
                    && event.operation == BeadEventOperationWire::IssueClosed
            })
            .count(),
        1
    );
}

#[test]
fn conflicting_resolution_aborts_mixed_batch_before_writing() {
    let (_temp, beads_dir, closed_id) =
        closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));
    let open_id = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Still open".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-02T01:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    let before = persisted_claim_state(&beads_dir);

    let error = close_issues_with_note(
        &beads_dir,
        &[open_id, closed_id.clone()],
        None,
        Some(BeadResolutionWire::Canceled),
        false,
        Some("must not land".to_string()),
        Some("agent-1".to_string()),
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert!(error.message.contains(&closed_id));
    assert!(error.message.contains("closed at 2026-01-02T00:00:00Z"));
    assert!(error.message.contains("resolution done"));
    assert!(error.message.contains("requested resolution canceled"));
    assert!(error.message.contains("sase bead open"));
    assert!(error.message.contains("sase bead note"));
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn conflicting_reason_aborts_before_writing() {
    let (_temp, beads_dir, issue_id) =
        closed_issue_fixture(BeadResolutionWire::Done, Some("original reason"));
    let before = persisted_claim_state(&beads_dir);

    let error = close_issues(
        &beads_dir,
        std::slice::from_ref(&issue_id),
        Some("different reason".to_string()),
        None,
        false,
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap_err();

    assert_eq!(error.kind, "validation");
    assert!(error.message.contains("\"original reason\""));
    assert!(error.message.contains("\"different reason\""));
    assert!(error.message.contains("requested resolution (unspecified)"));
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn absent_resolution_does_not_conflict_with_recorded_canceled_close() {
    let (_temp, beads_dir, issue_id) = closed_issue_fixture(
        BeadResolutionWire::Canceled,
        Some("canceled intentionally"),
    );
    let before = persisted_claim_state(&beads_dir);

    let result = close_issues(
        &beads_dir,
        std::slice::from_ref(&issue_id),
        None,
        None,
        false,
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap();

    assert!(!result.changed);
    assert_eq!(result.already_closed_ids, vec![issue_id]);
    assert_eq!(persisted_claim_state(&beads_dir), before);
}

#[test]
fn forced_close_requires_reason_and_non_done_resolution() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let original = issue(
        "sase-1",
        "Plan",
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
    ) + "\n";
    fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

    let no_reason = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        None,
        Some(BeadResolutionWire::Canceled),
        true,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap_err();
    assert!(no_reason.message.contains("requires a non-empty --reason"));

    let done = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        Some("Finished".to_string()),
        Some(BeadResolutionWire::Done),
        true,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap_err();
    assert!(done.message.contains("'done' is not allowed"));
    assert_eq!(
        fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
        original
    );
    assert!(!beads_dir.join("events").exists());
}

#[test]
fn reopening_grandchild_reopens_closed_ancestors_and_clears_resolution() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    let closed_with_resolution = |value: String| {
        value.replace(
            r#","changespec_name":""#,
            r#","resolution":"done","changespec_name":""#,
        )
    };
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            closed_with_resolution(issue(
                "sase-1",
                "Root",
                "plan",
                None,
                "closed",
                "2026-01-01T00:00:00Z",
            )),
            closed_with_resolution(issue(
                "sase-1.1",
                "Parent",
                "phase",
                Some("sase-1"),
                "closed",
                "2026-01-01T00:01:00Z",
            )),
            closed_with_resolution(issue(
                "sase-1.1.1",
                "Grandchild",
                "plan",
                Some("sase-1.1"),
                "closed",
                "2026-01-01T00:02:00Z",
            )),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let outcome = open_issue(
        &beads_dir,
        "sase-1.1.1",
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(outcome.reopened_ancestor_ids, vec!["sase-1.1", "sase-1"]);
    let store = MutableStore::load(&beads_dir).unwrap();
    for issue_id in ["sase-1", "sase-1.1", "sase-1.1.1"] {
        let reopened = store.get_issue(issue_id).unwrap();
        assert_eq!(reopened.status, StatusWire::Open);
        assert_eq!(reopened.resolution, None);
    }
    let opened_ids: Vec<String> = store
        .streams
        .all()
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| event.operation == BeadEventOperationWire::IssueOpened)
        .map(|event| event.issue_id.clone())
        .collect();
    assert_eq!(opened_ids, vec!["sase-1.1.1", "sase-1.1", "sase-1"]);
}

#[test]
fn close_records_explicit_resolution_and_reopen_update_clears_it() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "sase-1",
            "Superseded plan",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ) + "\n",
    )
    .unwrap();

    let closed = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        Some("A replacement shipped".to_string()),
        Some(BeadResolutionWire::Superseded),
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_eq!(
        closed.issues[0].resolution,
        Some(BeadResolutionWire::Superseded)
    );

    let reopened = update_issue(
        &beads_dir,
        "sase-1",
        BeadUpdateFieldsWire {
            status: Some("open".to_string()),
            now: Some("2026-01-03T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(reopened.issue.unwrap().resolution, None);
    assert_eq!(
        MutableStore::load(&beads_dir).unwrap().issues[0].resolution,
        None
    );
}

#[test]
fn every_reopen_cause_archives_the_close_reason_it_used_to_destroy() {
    let (_temp, beads_dir, ids) = close_history_fixture();
    let task_id = ids[2].clone();
    close_for_history(&beads_dir, &task_id, "2026-01-02T00:00:00Z");

    add_task_plus_one(
        &beads_dir,
        &task_id,
        "claude.probe",
        "Saw the same flake in CI run 4821.",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    let (projected, _) = projected_and_reduced(&beads_dir, &task_id);
    assert_eq!(projected.status, StatusWire::Ready);
    assert_eq!(projected.closed_at, None);
    assert_eq!(projected.close_reason, None);
    assert_eq!(projected.resolution, None);
    assert_eq!(projected.close_history.len(), 1);
    let record = &projected.close_history[0];
    assert_eq!(record.closed_at.as_str(), "2026-01-02T00:00:00Z");
    assert_eq!(
        record.close_reason.as_deref(),
        Some("Not reproducible on main.")
    );
    assert_eq!(record.resolution, Some(BeadResolutionWire::Canceled));
    assert_eq!(record.reopened_via, BeadReopenCauseWire::PlusOne);
    assert_eq!(record.reopened_by.as_deref(), Some("claude.probe"));
}

#[test]
fn mutation_and_reducer_agree_on_every_reopen_path() {
    // plus_one
    let (_temp, beads_dir, ids) = close_history_fixture();
    let task_id = ids[2].clone();
    close_for_history(&beads_dir, &task_id, "2026-01-02T00:00:00Z");
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "claude.probe",
        "Still flaky.",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_reopen_parity(&beads_dir, &task_id, "plus_one");

    // open — the path that used to leave a stale closed_at in issues.jsonl
    let phase_id = ids[1].clone();
    close_for_history(&beads_dir, &phase_id, "2026-01-02T00:00:00Z");
    open_issue(
        &beads_dir,
        &phase_id,
        Some("2026-01-04T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_reopen_parity(&beads_dir, &phase_id, "open");
    // ...and its closed ancestor, reopened by the same call.
    assert_reopen_parity(&beads_dir, &ids[0], "open ancestor");

    // update
    let (_temp, beads_dir, ids) = close_history_fixture();
    let phase_id = ids[1].clone();
    close_for_history(&beads_dir, &phase_id, "2026-01-02T00:00:00Z");
    update_issue(
        &beads_dir,
        &phase_id,
        BeadUpdateFieldsWire {
            status: Some("open".to_string()),
            now: Some("2026-01-05T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_reopen_parity(&beads_dir, &phase_id, "update");

    // epic_preclaim
    let (_temp, beads_dir, ids) = close_history_fixture();
    preclaim_epic_work_plan(
        &beads_dir,
        &ids[0],
        &[BeadPreclaimAssignmentWire {
            bead_id: ids[1].clone(),
            agent_name: "phase-agent".to_string(),
        }],
        Some("epic-agent".to_string()),
        Some("2026-01-06T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_reopen_parity(&beads_dir, &ids[0], "epic_preclaim epic");
    assert_reopen_parity(&beads_dir, &ids[1], "epic_preclaim phase");
}

#[test]
fn plus_one_close_record_joins_its_evidence_entry_exactly() {
    let (_temp, beads_dir, ids) = close_history_fixture();
    let task_id = ids[2].clone();
    close_for_history(&beads_dir, &task_id, "2026-01-02T00:00:00Z");
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "claude.probe",
        "Still flaky.",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    let (_, reduced) = projected_and_reduced(&beads_dir, &task_id);
    let record = &reduced.close_history[0];
    let evidence = &reduced.plus_one_evidence[0];
    assert_eq!(record.reopened_at, evidence.timestamp);
    assert_eq!(
        record.reopened_by.as_deref(),
        Some(evidence.reporter.as_str())
    );
}

#[test]
fn repeated_close_episodes_append_oldest_first_with_their_causes() {
    let (_temp, beads_dir, ids) = close_history_fixture();
    let task_id = ids[2].clone();

    close_for_history(&beads_dir, &task_id, "2026-01-02T00:00:00Z");
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "claude.probe",
        "Still flaky.",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    close_for_history(&beads_dir, &task_id, "2026-01-04T00:00:00Z");
    open_issue(
        &beads_dir,
        &task_id,
        Some("2026-01-05T00:00:00Z".to_string()),
    )
    .unwrap();

    let (projected, reduced) = projected_and_reduced(&beads_dir, &task_id);
    assert_eq!(projected, reduced);
    let causes: Vec<_> = projected
        .close_history
        .iter()
        .map(|record| (record.closed_at.as_str(), &record.reopened_via))
        .collect();
    assert_eq!(
        causes,
        vec![
            ("2026-01-02T00:00:00Z", &BeadReopenCauseWire::PlusOne),
            ("2026-01-04T00:00:00Z", &BeadReopenCauseWire::Open),
        ]
    );
    assert_eq!(projected.closed_at, None);
    assert_eq!(projected.close_history[1].reopened_by, None);
}

#[test]
fn reopening_a_bead_that_was_never_closed_archives_nothing() {
    let (_temp, beads_dir, ids) = close_history_fixture();
    let task_id = ids[2].clone();
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "claude.probe",
        "Also hit this.",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    open_issue(
        &beads_dir,
        &ids[1],
        Some("2026-01-04T00:00:00Z".to_string()),
    )
    .unwrap();

    for issue_id in [&task_id, &ids[1]] {
        let (projected, reduced) = projected_and_reduced(&beads_dir, issue_id);
        assert_eq!(projected, reduced);
        assert!(projected.close_history.is_empty(), "{issue_id}");
    }
}

#[test]
fn close_stamps_the_supplied_actor_on_the_envelope_and_closed_by() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue_id = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Closable".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;

    // Without `--note` the actor is still recorded on the close event.
    close_issues_with_note(
        &beads_dir,
        std::slice::from_ref(&issue_id),
        Some("verified".to_string()),
        None,
        false,
        None,
        Some("closer-agent".to_string()),
        Some("2026-01-01T00:01:00Z".to_string()),
    )
    .unwrap();

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let close = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .find(|event| {
            event.issue_id == issue_id
                && event.operation == BeadEventOperationWire::IssueClosed
        })
        .expect("one close event");
    // The creator never leaks onto the close: the envelope and the durable
    // payload both name the acting closer.
    assert_eq!(close.actor, "closer-agent");
    assert!(matches!(
        &close.payload,
        BeadEventPayloadWire::IssueClosed { closed_by, .. }
            if closed_by.as_deref() == Some("closer-agent")
    ));

    // A close with a note attributes the note to the same actor.
    let noted_id = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Noted".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    close_issues_with_note(
        &beads_dir,
        std::slice::from_ref(&noted_id),
        None,
        None,
        false,
        Some("evidence".to_string()),
        Some("closer-agent".to_string()),
        Some("2026-01-01T00:03:00Z".to_string()),
    )
    .unwrap();
    let issue = MutableStore::load(&beads_dir)
        .unwrap()
        .get_issue(&noted_id)
        .unwrap()
        .clone();
    assert!(note_text(&issue).contains("closer-agent"));

    // No actor falls back to the store owner.
    let owned_id = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Owned".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2026-01-01T00:04:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    close_issues(
        &beads_dir,
        std::slice::from_ref(&owned_id),
        None,
        None,
        false,
        Some("2026-01-01T00:05:00Z".to_string()),
    )
    .unwrap();
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let close = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .find(|event| {
            event.issue_id == owned_id
                && event.operation == BeadEventOperationWire::IssueClosed
        })
        .expect("one close event");
    assert_eq!(close.actor, "owner@example.com");
    assert!(matches!(
        &close.payload,
        BeadEventPayloadWire::IssueClosed { closed_by, .. }
            if closed_by.as_deref() == Some("owner@example.com")
    ));
}

#[test]
fn close_stamps_the_actor_on_the_delegated_parent() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "Root epic",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "Delegated phase",
                "phase",
                Some("sase-1"),
                "open",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-1.1.1",
                "Child epic",
                "plan",
                Some("sase-1.1"),
                "open",
                "2026-01-01T00:02:00Z",
            ),
            issue(
                "sase-1.1.1.1",
                "Child phase",
                "phase",
                Some("sase-1.1.1"),
                "open",
                "2026-01-01T00:03:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    close_issues_with_note(
        &beads_dir,
        &["sase-1.1.1.1".to_string()],
        Some("phase complete".to_string()),
        None,
        false,
        None,
        Some("worker".to_string()),
        Some("2026-01-01T12:00:00Z".to_string()),
    )
    .unwrap();
    close_issues_with_note(
        &beads_dir,
        &["sase-1.1.1".to_string()],
        Some("landed".to_string()),
        None,
        false,
        None,
        Some("worker".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    // The auto-closed delegated parent shares the actor with the request
    // that completed it.
    for issue_id in ["sase-1.1.1", "sase-1.1"] {
        let close = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find(|event| {
                event.issue_id == issue_id
                    && event.operation == BeadEventOperationWire::IssueClosed
            })
            .unwrap_or_else(|| panic!("close event for {issue_id}"));
        assert_eq!(close.actor, "worker", "{issue_id}");
        assert!(
            matches!(
                &close.payload,
                BeadEventPayloadWire::IssueClosed { closed_by, .. }
                    if closed_by.as_deref() == Some("worker")
            ),
            "{issue_id}"
        );
    }
}

#[test]
fn forced_close_stamps_the_actor_on_every_swept_descendant() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-9",
                "Plan",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-9.1",
                "Open phase",
                "phase",
                Some("sase-9"),
                "open",
                "2026-01-01T00:01:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    close_issues_with_note(
        &beads_dir,
        &["sase-9".to_string()],
        Some("Canceled unfinished work".to_string()),
        Some(BeadResolutionWire::Canceled),
        true,
        None,
        Some("worker".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    for issue_id in ["sase-9.1", "sase-9"] {
        let close = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find(|event| {
                event.issue_id == issue_id
                    && event.operation == BeadEventOperationWire::IssueClosed
            })
            .unwrap_or_else(|| panic!("close event for {issue_id}"));
        assert_eq!(close.actor, "worker", "{issue_id}");
        assert!(
            matches!(
                &close.payload,
                BeadEventPayloadWire::IssueClosed { closed_by, .. }
                    if closed_by.as_deref() == Some("worker")
            ),
            "{issue_id}"
        );
    }
}
