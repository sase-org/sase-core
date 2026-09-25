//! Reduction tests, covering `super::super::reduction`: close
//! idempotence, note events, legacy note parsing, and external-ref
//! collapsing.

use std::collections::BTreeMap;

use crate::bead::wire::{
    notes_text, parse_legacy_note_blob, BeadResolutionWire, IssueWire,
};

use super::super::import::PendingEvent;
use super::super::*;
use super::support::*;

#[test]
fn redundant_close_is_an_exact_no_op() {
    let mut issues =
        BTreeMap::from([("sase-1".to_string(), issue_with_refs(Vec::new()))]);
    let first_close = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "first-close".to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        actor: "owner@example.com".to_string(),
        operation: BeadEventOperationWire::IssueClosed,
        issue_id: "sase-1".to_string(),
        payload: BeadEventPayloadWire::IssueClosed {
            close_reason: Some("shipped".to_string()),
            resolution: Some(BeadResolutionWire::Done),
            forced_descendant_ids: Vec::new(),
            closed_by: None,
        },
    };
    let redundant_close = BeadEventRecordWire {
        event_id: "redundant-close".to_string(),
        timestamp: "2026-01-01T00:02:00Z".to_string(),
        payload: BeadEventPayloadWire::IssueClosed {
            close_reason: None,
            resolution: Some(BeadResolutionWire::Canceled),
            forced_descendant_ids: Vec::new(),
            closed_by: None,
        },
        ..first_close.clone()
    };

    apply_event(&mut issues, &first_close).unwrap();
    let first_projection = issues["sase-1"].clone();
    apply_event(&mut issues, &redundant_close).unwrap();

    assert_eq!(issues["sase-1"], first_projection);
}

#[test]
fn note_append_validation_and_rendering_are_owned_by_the_event() {
    let mut issues =
        BTreeMap::from([("sase-1".to_string(), issue_with_refs(Vec::new()))]);
    let note = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "note".to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        actor: "agent-1".to_string(),
        operation: BeadEventOperationWire::NoteAppended,
        issue_id: "sase-1".to_string(),
        payload: BeadEventPayloadWire::NoteAppended {
            entry: " verified ".to_string(),
        },
    };

    apply_event(&mut issues, &note).unwrap();

    assert_eq!(issues["sase-1"].notes.len(), 1);
    assert_eq!(issues["sase-1"].notes[0].id, "note");
    assert_eq!(issues["sase-1"].notes[0].timestamp, note.timestamp);
    assert_eq!(issues["sase-1"].notes[0].author, note.actor);
    assert_eq!(issues["sase-1"].notes[0].text, "verified");
    assert_eq!(
        notes_text(&issues["sase-1"].notes),
        "[2026-01-01T00:01:00Z · agent-1] verified"
    );
    assert_eq!(issues["sase-1"].updated_at, "2026-01-01T00:01:00Z");

    let blank = BeadEventRecordWire {
        payload: BeadEventPayloadWire::NoteAppended {
            entry: " \t ".to_string(),
        },
        ..note
    };
    assert_eq!(
        blank.validate().unwrap_err().message,
        "note_appended entry cannot be empty or blank"
    );
}

#[test]
fn note_edited_rewrites_text_and_stamps_editor() {
    let mut issues =
        BTreeMap::from([("sase-1".to_string(), issue_with_refs(Vec::new()))]);
    let appended = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "note".to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        actor: "agent-1".to_string(),
        operation: BeadEventOperationWire::NoteAppended,
        issue_id: "sase-1".to_string(),
        payload: BeadEventPayloadWire::NoteAppended {
            entry: "first draft".to_string(),
        },
    };
    apply_event(&mut issues, &appended).unwrap();

    let edited = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "edit".to_string(),
        timestamp: "2026-01-01T00:02:00Z".to_string(),
        actor: "agent-2".to_string(),
        operation: BeadEventOperationWire::NoteEdited,
        issue_id: "sase-1".to_string(),
        payload: BeadEventPayloadWire::NoteEdited {
            note_id: "note".to_string(),
            text: " corrected ".to_string(),
        },
    };
    apply_event(&mut issues, &edited).unwrap();

    let note = &issues["sase-1"].notes[0];
    assert_eq!(note.text, "corrected");
    assert_eq!(note.timestamp, "2026-01-01T00:01:00Z");
    assert_eq!(note.author, "agent-1");
    assert_eq!(note.edited_at.as_deref(), Some("2026-01-01T00:02:00Z"));
    assert_eq!(note.edited_by.as_deref(), Some("agent-2"));
    assert_eq!(issues["sase-1"].updated_at, "2026-01-01T00:02:00Z");

    let unknown = BeadEventRecordWire {
        payload: BeadEventPayloadWire::NoteEdited {
            note_id: "does-not-exist".to_string(),
            text: "x".to_string(),
        },
        ..edited.clone()
    };
    assert_eq!(
        apply_event(&mut issues, &unknown).unwrap_err().message,
        "event references unknown note: does-not-exist"
    );

    let blank = BeadEventRecordWire {
        payload: BeadEventPayloadWire::NoteEdited {
            note_id: "note".to_string(),
            text: " \t ".to_string(),
        },
        ..edited
    };
    assert_eq!(
        blank.validate().unwrap_err().message,
        "note_edited text cannot be empty or blank"
    );
}

#[test]
fn note_removed_retracts_the_record() {
    let mut issues =
        BTreeMap::from([("sase-1".to_string(), issue_with_refs(Vec::new()))]);
    let appended = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "note".to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        actor: "agent-1".to_string(),
        operation: BeadEventOperationWire::NoteAppended,
        issue_id: "sase-1".to_string(),
        payload: BeadEventPayloadWire::NoteAppended {
            entry: "retract me".to_string(),
        },
    };
    apply_event(&mut issues, &appended).unwrap();

    let removed = BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "remove".to_string(),
        timestamp: "2026-01-01T00:02:00Z".to_string(),
        actor: "agent-2".to_string(),
        operation: BeadEventOperationWire::NoteRemoved,
        issue_id: "sase-1".to_string(),
        payload: BeadEventPayloadWire::NoteRemoved {
            note_id: "note".to_string(),
        },
    };
    apply_event(&mut issues, &removed).unwrap();

    assert!(issues["sase-1"].notes.is_empty());
    assert_eq!(issues["sase-1"].updated_at, "2026-01-01T00:02:00Z");

    let unknown = BeadEventRecordWire {
        payload: BeadEventPayloadWire::NoteRemoved {
            note_id: "does-not-exist".to_string(),
        },
        ..removed
    };
    assert_eq!(
        apply_event(&mut issues, &unknown).unwrap_err().message,
        "event references unknown note: does-not-exist"
    );
}

#[test]
fn legacy_note_parser_recovers_pure_appended_blob() {
    let notes = parse_legacy_note_blob(
            "[2026-01-01T00:00:00Z · alpha] first\n\n[2026-01-02T00:00:00Z · beta] second",
            "legacy-event",
            "2026-01-03T00:00:00Z",
            "fallback",
        );

    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0].id, "legacy-event#1");
    assert_eq!(notes[0].timestamp, "2026-01-01T00:00:00Z");
    assert_eq!(notes[0].author, "alpha");
    assert_eq!(notes[0].text, "first");
    assert_eq!(notes[1].id, "legacy-event#2");
    assert_eq!(notes[1].timestamp, "2026-01-02T00:00:00Z");
    assert_eq!(notes[1].author, "beta");
    assert_eq!(notes[1].text, "second");
    assert_eq!(
            notes_text(&notes),
            "[2026-01-01T00:00:00Z · alpha] first\n\n[2026-01-02T00:00:00Z · beta] second"
        );
}

#[test]
fn legacy_note_parser_attributes_bare_prose_to_update_event() {
    let notes = parse_legacy_note_blob(
        "first paragraph\n\nsecond paragraph",
        "legacy-event",
        "2026-01-03T00:00:00Z",
        "fallback",
    );

    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].timestamp, "2026-01-03T00:00:00Z");
    assert_eq!(notes[0].author, "fallback");
    assert_eq!(notes[0].text, "first paragraph\n\nsecond paragraph");
}

#[test]
fn legacy_note_parser_keeps_prose_before_first_marker_as_one_record() {
    let notes = parse_legacy_note_blob(
        "context before marker\n\n[2026-01-04T00:00:00Z · beta] marked",
        "legacy-event",
        "2026-01-03T00:00:00Z",
        "fallback",
    );

    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0].timestamp, "2026-01-03T00:00:00Z");
    assert_eq!(notes[0].author, "fallback");
    assert_eq!(notes[0].text, "context before marker");
    assert_eq!(notes[1].timestamp, "2026-01-04T00:00:00Z");
    assert_eq!(notes[1].author, "beta");
    assert_eq!(notes[1].text, "marked");
}

#[test]
fn legacy_note_parser_does_not_promote_unparseable_timestamp_marker() {
    let notes = parse_legacy_note_blob(
        "[not-a-date · beta] stays prose",
        "legacy-event",
        "2026-01-03T00:00:00Z",
        "fallback",
    );

    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].timestamp, "2026-01-03T00:00:00Z");
    assert_eq!(notes[0].author, "fallback");
    assert_eq!(notes[0].text, "[not-a-date · beta] stays prose");
}

#[test]
fn legacy_note_parser_does_not_promote_marker_looking_line_mid_paragraph() {
    let notes = parse_legacy_note_blob(
        "intro\n[2026-01-04T00:00:00Z · beta] not a header",
        "legacy-event",
        "2026-01-03T00:00:00Z",
        "fallback",
    );

    assert_eq!(notes.len(), 1);
    assert_eq!(
        notes[0].text,
        "intro\n[2026-01-04T00:00:00Z · beta] not a header"
    );
}

#[test]
fn legacy_note_parser_ignores_empty_and_whitespace_only_blobs() {
    assert!(parse_legacy_note_blob(
        "",
        "legacy-event",
        "2026-01-03T00:00:00Z",
        "fallback"
    )
    .is_empty());
    assert!(parse_legacy_note_blob(
        " \n\t\n ",
        "legacy-event",
        "2026-01-03T00:00:00Z",
        "fallback"
    )
    .is_empty());
}

#[test]
fn legacy_note_parser_accepts_crlf_input() {
    let notes = parse_legacy_note_blob(
            "[2026-01-01T00:00:00Z · alpha] first\r\n\r\ncontinued\r\n\r\n[2026-01-02T00:00:00Z · beta] second",
            "legacy-event",
            "2026-01-03T00:00:00Z",
            "fallback",
        );

    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0].text, "first\n\ncontinued");
    assert_eq!(notes[1].text, "second");
}

fn created_stream(issue: &IssueWire) -> BeadEventStreamWire {
    BeadEventStreamWire {
        stream_id: issue.id.clone(),
        root_issue_id: issue.id.clone(),
        events: vec![PendingEvent::created(issue)
            .into_record(&issue.id, 1)
            .unwrap()],
    }
}

#[test]
fn reduction_collapses_duplicate_external_refs_regardless_of_stream_order() {
    let mut first = issue_with_refs(Vec::new());
    first.id = "sase-1".to_string();
    first.external_ref = "bug:sase#42".to_string();
    let mut second = issue_with_refs(Vec::new());
    second.id = "sase-2".to_string();
    second.external_ref = "bug:sase#42".to_string();

    let forward = vec![created_stream(&first), created_stream(&second)];
    let mut reversed = forward.clone();
    reversed.reverse();

    for streams in [forward, reversed] {
        let reduced = reduce_event_streams(&streams).unwrap();
        assert_eq!(reduced.len(), 1);
        assert_eq!(reduced[0].id, "sase-1");
        assert_eq!(reduced[0].external_ref, "bug:sase#42");
    }
}

#[test]
fn reduction_collapse_prefers_earlier_created_at_over_id_order() {
    let mut first = issue_with_refs(Vec::new());
    first.id = "sase-1".to_string();
    first.external_ref = "bug:sase#42".to_string();
    first.created_at = "2026-02-01T00:00:00Z".to_string();
    let mut second = issue_with_refs(Vec::new());
    second.id = "sase-2".to_string();
    second.external_ref = "bug:sase#42".to_string();
    second.created_at = "2026-01-01T00:00:00Z".to_string();

    let streams = vec![created_stream(&first), created_stream(&second)];

    let reduced = reduce_event_streams(&streams).unwrap();

    assert_eq!(reduced.len(), 1);
    assert_eq!(reduced[0].id, "sase-2");
}

#[test]
fn reduction_collapse_does_not_disturb_unrelated_issues() {
    let mut first = issue_with_refs(Vec::new());
    first.id = "sase-1".to_string();
    first.external_ref = "bug:sase#42".to_string();
    let mut second = issue_with_refs(Vec::new());
    second.id = "sase-2".to_string();
    second.external_ref = "bug:sase#42".to_string();
    let mut unrelated = issue_with_refs(Vec::new());
    unrelated.id = "sase-3".to_string();
    unrelated.external_ref = String::new();

    let streams = vec![
        created_stream(&first),
        created_stream(&second),
        created_stream(&unrelated),
    ];

    let reduced = reduce_event_streams(&streams).unwrap();

    let ids: Vec<&str> =
        reduced.iter().map(|issue| issue.id.as_str()).collect();
    assert_eq!(ids, vec!["sase-1", "sase-3"]);
}
