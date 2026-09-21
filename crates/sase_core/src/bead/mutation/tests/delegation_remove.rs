use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadEventRecordWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::StatusWire;
use std::fs;
use tempfile::tempdir;

use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
#[test]
fn forced_close_plan_sweeps_through_nested_child_epics() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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
                "Root phase",
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

    let result = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        Some("Superseded unfinished tree".to_string()),
        Some(BeadResolutionWire::Superseded),
        true,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(
        result.issue_ids,
        vec!["sase-1.1.1.1", "sase-1.1.1", "sase-1.1", "sase-1"]
    );
    let store = MutableStore::load(&beads_dir).unwrap();
    assert!(store
        .issues
        .iter()
        .all(|issue| issue.status == StatusWire::Closed));
    assert!(store
        .issues
        .iter()
        .all(|issue| issue.resolution == Some(BeadResolutionWire::Superseded)));
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
    assert_eq!(forced_ids, vec!["sase-1.1.1.1", "sase-1.1.1", "sase-1.1"]);
}

#[test]
fn closing_child_epic_closes_completed_parent_phase() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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

    close_issues(
        &beads_dir,
        &["sase-1.1.1.1".to_string()],
        Some("phase complete".to_string()),
        None,
        false,
        Some("2026-01-01T12:00:00Z".to_string()),
    )
    .unwrap();
    let result = close_issues(
        &beads_dir,
        &["sase-1.1.1".to_string()],
        Some("landed".to_string()),
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-1.1.1", "sase-1.1"]);
    assert_eq!(result.closed_ids, vec!["sase-1.1.1", "sase-1.1"]);
    assert_eq!(result.cascade_closed_ids, vec!["sase-1.1"]);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        store.get_issue("sase-1.1").unwrap().close_reason.as_deref(),
        Some("delegated work landed")
    );
    assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let parent_close_events: Vec<&BeadEventRecordWire> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| {
            event.issue_id == "sase-1.1"
                && event.operation == BeadEventOperationWire::IssueClosed
        })
        .collect();
    assert_eq!(parent_close_events.len(), 1);
    assert!(matches!(
        &parent_close_events[0].payload,
        BeadEventPayloadWire::IssueClosed { close_reason, .. }
            if close_reason.as_deref() == Some("delegated work landed")
    ));
    let projected = reduce_event_streams(&streams).unwrap();
    let projected_parent = projected
        .iter()
        .find(|issue| issue.id == "sase-1.1")
        .unwrap();
    assert_eq!(projected_parent.status, StatusWire::Closed);
    assert_eq!(
        projected_parent.close_reason.as_deref(),
        Some("delegated work landed")
    );
}

#[test]
fn open_sibling_delegated_work_keeps_parent_phase_open() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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
                "First child epic",
                "plan",
                Some("sase-1.1"),
                "open",
                "2026-01-01T00:02:00Z",
            ),
            issue(
                "sase-1.1.2",
                "Second child epic",
                "plan",
                Some("sase-1.1"),
                "open",
                "2026-01-01T00:03:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let result = close_issues(
        &beads_dir,
        &["sase-1.1.1".to_string()],
        None,
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-1.1.1"]);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        store.get_issue("sase-1.1").unwrap().status,
        StatusWire::Open
    );
}

#[test]
fn nested_delegation_closes_only_phase_parents() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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
                "Root phase",
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
                "Nested delegated phase",
                "phase",
                Some("sase-1.1.1"),
                "open",
                "2026-01-01T00:03:00Z",
            ),
            issue(
                "sase-1.1.1.1.1",
                "Grandchild epic",
                "plan",
                Some("sase-1.1.1.1"),
                "open",
                "2026-01-01T00:04:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    close_issues(
        &beads_dir,
        &["sase-1.1.1.1.1".to_string()],
        Some("grandchild landed".to_string()),
        None,
        false,
        Some("2026-01-01T12:00:00Z".to_string()),
    )
    .unwrap();
    let result = close_issues(
        &beads_dir,
        &["sase-1.1.1".to_string()],
        Some("child landed".to_string()),
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-1.1.1", "sase-1.1"]);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        store
            .get_issue("sase-1.1.1.1")
            .unwrap()
            .close_reason
            .as_deref(),
        Some("delegated work landed")
    );
    assert_eq!(
        store.get_issue("sase-1.1").unwrap().close_reason.as_deref(),
        Some("delegated work landed")
    );
    assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);
}

#[test]
fn explicitly_closing_parent_and_child_emits_one_explicit_parent_event() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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
        &["sase-1.1.1".to_string(), "sase-1.1".to_string()],
        None,
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-1.1"]);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let parent_close_events: Vec<&BeadEventRecordWire> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| {
            event.issue_id == "sase-1.1"
                && event.operation == BeadEventOperationWire::IssueClosed
        })
        .collect();
    assert_eq!(parent_close_events.len(), 1);
    assert!(matches!(
        &parent_close_events[0].payload,
        BeadEventPayloadWire::IssueClosed { close_reason, .. }
            if close_reason.is_none()
    ));
}

#[test]
fn remove_plan_cascades_through_nested_child_epics() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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
                "Root phase",
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
            issue(
                "sase-2",
                "Unrelated",
                "plan",
                None,
                "open",
                "2026-01-01T00:04:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    let result = remove_issue(&beads_dir, "sase-1").unwrap();

    assert_eq!(
        result.issue_ids,
        vec!["sase-1.1.1.1", "sase-1.1.1", "sase-1.1", "sase-1"]
    );
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        store
            .issues
            .iter()
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>(),
        vec!["sase-2"]
    );
}

#[test]
fn remove_issues_removes_independent_roots_in_argument_order() {
    let (_temp, beads_dir) = batch_remove_fixture();

    let result = remove_issues(
        &beads_dir,
        &["sase-2".to_string(), "sase-1.1".to_string()],
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-2", "sase-1.1"]);
    assert_eq!(
        result
            .issues
            .iter()
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>(),
        vec!["sase-2", "sase-1.1"]
    );
    let reloaded = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        reloaded
            .issues
            .iter()
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>(),
        vec!["sase-1", "sase-1.2"]
    );

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let reduced = reduce_event_streams(&streams).unwrap();
    assert_eq!(reduced, reloaded.issues);
    let removal_events: Vec<&BeadEventRecordWire> = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| event.operation == BeadEventOperationWire::IssueRemoved)
        .collect();
    assert_eq!(removal_events.len(), 2);
    assert_eq!(removal_events[0].timestamp, removal_events[1].timestamp);
}

#[test]
fn remove_issues_deduplicates_overlapping_roots_in_both_orders() {
    let (_temp, beads_dir) = batch_remove_fixture();
    let plan_first = remove_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-1.2".to_string()],
    )
    .unwrap();
    assert_eq!(plan_first.issue_ids, vec!["sase-1.1", "sase-1.2", "sase-1"]);

    let (_temp, beads_dir) = batch_remove_fixture();
    let descendant_first = remove_issues(
        &beads_dir,
        &["sase-1.2".to_string(), "sase-1".to_string()],
    )
    .unwrap();
    assert_eq!(
        descendant_first.issue_ids,
        vec!["sase-1.2", "sase-1.1", "sase-1"]
    );
}

#[test]
fn remove_issues_deduplicates_duplicate_requests_and_events() {
    let (_temp, beads_dir) = batch_remove_fixture();

    let result = remove_issues(
        &beads_dir,
        &["sase-1".to_string(), "sase-1".to_string()],
    )
    .unwrap();

    assert_eq!(result.issue_ids, vec!["sase-1.1", "sase-1.2", "sase-1"]);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let removal_events = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| event.operation == BeadEventOperationWire::IssueRemoved)
        .count();
    assert_eq!(removal_events, 1);
}

#[test]
fn remove_issues_missing_later_id_leaves_store_unchanged() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner")).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let first = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First".to_string(),
            issue_type: IssueTypeWire::Plan,
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
    add_dependency(
        &beads_dir,
        &second.id,
        &first.id,
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();
    assert_reprojection_byte_stable(&beads_dir, "dependency add");
    let projection_before = fs::read(beads_dir.join("issues.jsonl")).unwrap();
    let config_before = fs::read(beads_dir.join("config.json")).unwrap();
    let (_, streams_before) = read_event_store(&beads_dir).unwrap();

    let error = remove_issues(
        &beads_dir,
        &[first.id.clone(), "sase-missing".to_string()],
    )
    .unwrap_err();

    assert_eq!(error.kind, "not_found");
    assert_eq!(error.message, "Issue not found: sase-missing");
    assert_eq!(
        fs::read(beads_dir.join("issues.jsonl")).unwrap(),
        projection_before
    );
    assert_eq!(
        fs::read(beads_dir.join("config.json")).unwrap(),
        config_before
    );
    let (_, streams_after) = read_event_store(&beads_dir).unwrap();
    assert_eq!(streams_after, streams_before);
    let reloaded = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        reloaded
            .get_issue(&second.id)
            .unwrap()
            .dependencies
            .iter()
            .map(|dependency| dependency.depends_on_id.as_str())
            .collect::<Vec<_>>(),
        vec![first.id.as_str()]
    );
}

#[test]
fn remove_issues_rejects_an_empty_request() {
    let (_temp, beads_dir) = batch_remove_fixture();

    let error = remove_issues(&beads_dir, &[]).unwrap_err();

    assert_eq!(error.kind, "validation");
    assert_eq!(
        error.message,
        "remove_issues() requires at least one issue ID"
    );
}

#[test]
fn removing_child_epic_does_not_close_parent_phase() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
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
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();

    remove_issue(&beads_dir, "sase-1.1.1").unwrap();

    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        store.get_issue("sase-1.1").unwrap().status,
        StatusWire::Open
    );
    assert!(store.get_issue("sase-1.1.1").is_err());
}

#[test]
fn close_skips_already_closed_issues_without_new_events() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "")).unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        issue(
            "sase-1",
            "Already done",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ) + "\n",
    )
    .unwrap();
    close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        None,
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let event_count_before = streams
        .iter()
        .flat_map(|stream| stream.events.iter())
        .count();
    let before = persisted_claim_state(&beads_dir);

    let result = close_issues(
        &beads_dir,
        &["sase-1".to_string()],
        None,
        None,
        false,
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap();

    assert!(!result.changed);
    assert!(result.issue_ids.is_empty());
    assert_eq!(result.issues.len(), 1);
    assert_eq!(result.issues[0].id, "sase-1");
    assert_eq!(result.issues[0].status, StatusWire::Closed);
    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let event_count_after = streams
        .iter()
        .flat_map(|stream| stream.events.iter())
        .count();
    assert_eq!(event_count_after, event_count_before);
    assert_eq!(persisted_claim_state(&beads_dir), before);
}
