//! Merge tests, covering `super::super::merge`: three-way plus-one
//! merges and concurrent-mint relocation.

use std::collections::BTreeSet;

use crate::bead::wire::{
    IssueTypeWire, IssueWire, PhaseSizeWire, StatusWire,
    TaskPlusOneEvidenceWire,
};

use super::super::import::PendingEvent;
use super::super::*;
use super::support::*;

fn task_issue() -> IssueWire {
    let mut issue = issue_with_refs(Vec::new());
    issue.id = "sase-task".to_string();
    issue.title = "Task".to_string();
    issue.issue_type = IssueTypeWire::Task;
    issue.tier = None;
    issue.created_by = "creator-agent".to_string();
    issue.size = Some(PhaseSizeWire::Small);
    issue
}

fn plus_one_event(event_id: &str, reporter: &str) -> BeadEventRecordWire {
    let evidence = TaskPlusOneEvidenceWire {
        timestamp: "2026-01-02T00:00:00Z".to_string(),
        observed_since: None,
        reporter: reporter.to_string(),
        note: "independent reproduction".to_string(),
        refs: Vec::new(),
    };
    BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        timestamp: evidence.timestamp.clone(),
        actor: reporter.to_string(),
        operation: BeadEventOperationWire::TaskPlusOneRecorded,
        issue_id: "sase-task".to_string(),
        payload: BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
    }
}

#[test]
fn three_way_merge_preserves_independent_plus_ones_and_deduplicates_reporter() {
    let created = PendingEvent::created(&task_issue())
        .into_record("sase-task", 1)
        .unwrap();
    let base = BeadEventStreamWire {
        stream_id: "sase-task".to_string(),
        root_issue_id: "sase-task".to_string(),
        events: vec![created],
    };
    let mut ours = base.clone();
    ours.events.push(plus_one_event("ours-a", "agent-a"));
    ours.events.push(plus_one_event("ours-a-retry", "agent-a"));
    let mut theirs = base.clone();
    theirs.events.push(plus_one_event("theirs-b", "agent-b"));

    let merged = merge_bead_event_streams(&base, &ours, &theirs).unwrap();
    let issues = reduce_event_streams(&[merged]).unwrap();

    assert_eq!(issues[0].plus_one_count(), 2);
    assert_eq!(issues[0].status, StatusWire::Ready);
    assert_eq!(
        issues[0]
            .plus_one_evidence
            .iter()
            .map(|evidence| evidence.reporter.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["agent-a", "agent-b"])
    );
}

/// Build one side of a concurrent mint: same id, different bead.
fn colliding_stream(
    issue_id: &str,
    title: &str,
    created_at: &str,
) -> BeadEventStreamWire {
    let mut issue = issue_with_refs(Vec::new());
    issue.id = issue_id.to_string();
    issue.title = title.to_string();
    issue.created_at = created_at.to_string();
    issue.updated_at = created_at.to_string();
    let created = PendingEvent::created(&issue)
        .into_record(issue_id, 1)
        .unwrap();
    BeadEventStreamWire {
        stream_id: issue_id.to_string(),
        root_issue_id: issue_id.to_string(),
        events: vec![created],
    }
}

fn empty_stream(stream_id: &str) -> BeadEventStreamWire {
    BeadEventStreamWire {
        stream_id: stream_id.to_string(),
        root_issue_id: stream_id.to_string(),
        events: Vec::new(),
    }
}

#[test]
fn concurrently_minted_bead_id_relocates_instead_of_wedging_the_store() {
    let base = empty_stream("sase-ey");
    let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
    let theirs = colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

    let outcome = merge_bead_event_streams_with_relocation(
        &base,
        &ours,
        &theirs,
        Some("sase-ez"),
    )
    .unwrap();

    assert_eq!(
        outcome.relocations,
        vec![("sase-ey".to_string(), "sase-ez".to_string())]
    );
    assert_eq!(
        outcome.relocation_records,
        vec![BeadIdRelocationWire {
            old_id: "sase-ey".to_string(),
            new_id: "sase-ez".to_string(),
            kind: BeadIdRelocationKindWire::TopLevelDuplicate,
        }]
    );
    let serialized = serde_json::to_value(&outcome).unwrap();
    assert_eq!(
        serialized["relocation_records"][0]["kind"],
        serde_json::json!("top_level_duplicate")
    );
    let relocated = outcome.relocated.clone().unwrap();
    assert_eq!(relocated.stream_id, "sase-ez");
    // Both beads survive: the older creation keeps the contested id.
    let issues =
        reduce_event_streams(&[outcome.merged.clone(), relocated]).unwrap();
    assert_eq!(
        issues
            .iter()
            .map(|issue| (issue.id.as_str(), issue.title.as_str()))
            .collect::<Vec<_>>(),
        vec![("sase-ey", "Ours"), ("sase-ez", "Theirs")]
    );
}

#[test]
fn relocation_picks_the_same_loser_whichever_side_git_calls_ours() {
    let base = empty_stream("sase-ey");
    let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
    let theirs = colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

    let forward = merge_bead_event_streams_with_relocation(
        &base,
        &ours,
        &theirs,
        Some("sase-ez"),
    )
    .unwrap();
    let swapped = merge_bead_event_streams_with_relocation(
        &base,
        &theirs,
        &ours,
        Some("sase-ez"),
    )
    .unwrap();

    assert_eq!(forward, swapped);
}

#[test]
fn relocated_events_are_reminted_onto_their_new_stream() {
    let base = empty_stream("sase-ey");
    let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
    let theirs = colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

    let outcome = merge_bead_event_streams_with_relocation(
        &base,
        &ours,
        &theirs,
        Some("sase-ez"),
    )
    .unwrap();

    let relocated = outcome.relocated.unwrap();
    let event = &relocated.events[0];
    assert_eq!(event.issue_id, "sase-ez");
    assert!(event.event_id.starts_with("sase-ez:000001:"));
    assert!(outcome
        .merged
        .events
        .iter()
        .all(|kept| kept.event_id != event.event_id));
}

#[test]
fn merging_without_a_relocation_id_still_reports_the_duplicate() {
    let base = empty_stream("sase-ey");
    let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
    let theirs = colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

    assert_eq!(
        merge_bead_event_streams(&base, &ours, &theirs)
            .unwrap_err()
            .message,
        "duplicate issue_created event for sase-ey"
    );
}

#[test]
fn concurrently_minted_child_id_renumbers_to_a_free_sibling() {
    let base = colliding_stream("sase-ey", "Epic", "2026-08-03T11:00:00Z");
    let mut ours = base.clone();
    ours.events.push(child_created(
        "sase-ey",
        "sase-ey.1",
        "Phase ours",
        "2026-08-03T11:01:00Z",
        2,
    ));
    let mut theirs = base.clone();
    theirs.events.push(child_created(
        "sase-ey",
        "sase-ey.1",
        "Phase theirs",
        "2026-08-03T11:01:01Z",
        2,
    ));

    let outcome = merge_bead_event_streams_with_relocation(
        &base,
        &ours,
        &theirs,
        Some("sase-ez"),
    )
    .unwrap();

    assert!(outcome.relocated.is_none());
    assert_eq!(
        outcome.relocations,
        vec![("sase-ey.1".to_string(), "sase-ey.2".to_string())]
    );
    assert_eq!(
        outcome.relocation_records,
        vec![BeadIdRelocationWire {
            old_id: "sase-ey.1".to_string(),
            new_id: "sase-ey.2".to_string(),
            kind: BeadIdRelocationKindWire::ChildDuplicate,
        }]
    );
    let issues = reduce_event_streams(&[outcome.merged]).unwrap();
    assert_eq!(
        issues
            .iter()
            .map(|issue| (issue.id.as_str(), issue.title.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("sase-ey", "Epic"),
            ("sase-ey.1", "Phase ours"),
            ("sase-ey.2", "Phase theirs"),
        ]
    );
}

fn child_created(
    stream_id: &str,
    issue_id: &str,
    title: &str,
    created_at: &str,
    ordinal: usize,
) -> BeadEventRecordWire {
    let mut issue = issue_with_refs(Vec::new());
    issue.id = issue_id.to_string();
    issue.title = title.to_string();
    issue.issue_type = IssueTypeWire::Phase;
    issue.tier = None;
    issue.parent_id = Some(stream_id.to_string());
    issue.size = Some(PhaseSizeWire::Small);
    issue.created_at = created_at.to_string();
    issue.updated_at = created_at.to_string();
    PendingEvent::created(&issue)
        .into_record(stream_id, ordinal)
        .unwrap()
}
