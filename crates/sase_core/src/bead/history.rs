//! Read-only replay of bead event streams as field-level history.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::events::{
    apply_event, merge_stream_events, validated_event_streams,
    BeadEventOperationWire, BeadEventStreamWire,
};
use super::jsonl::read_event_store;
use super::wire::{BeadError, IssueWire};

pub const BEAD_HISTORY_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadHistoryChangeWire {
    pub field: String,
    pub from: Option<Value>,
    pub to: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadHistoryEntryWire {
    pub event_id: String,
    pub timestamp: String,
    pub actor: String,
    pub operation: BeadEventOperationWire,
    pub changes: Vec<BeadHistoryChangeWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadHistoryWire {
    pub issue_id: String,
    pub schema_version: u32,
    pub entries: Vec<BeadHistoryEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadLostNoteRevisionWire {
    pub timestamp: String,
    pub actor: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadLostNotesWire {
    pub issue_id: String,
    pub current_notes: String,
    pub dropped_revisions: Vec<BeadLostNoteRevisionWire>,
}

pub fn bead_history(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<BeadHistoryWire, BeadError> {
    let (_manifest, streams) = read_event_store(beads_dir)?;
    history_from_streams(&streams, issue_id)
}

pub fn bead_lost_notes(
    beads_dir: &Path,
    issue_id: Option<&str>,
) -> Result<Vec<BeadLostNotesWire>, BeadError> {
    let (_manifest, streams) = read_event_store(beads_dir)?;
    lost_notes_from_streams(&streams, issue_id)
}

fn history_from_streams(
    streams: &[BeadEventStreamWire],
    issue_id: &str,
) -> Result<BeadHistoryWire, BeadError> {
    let streams = validated_event_streams(streams)?;
    let mut issues = BTreeMap::new();
    let mut entries = Vec::new();
    let mut found = false;

    for event in merge_stream_events(&streams) {
        let is_target = event.issue_id == issue_id;
        let before = is_target.then(|| issues.get(issue_id).cloned()).flatten();
        apply_event(&mut issues, event)?;
        if !is_target {
            continue;
        }

        found = true;
        let after = issues.get(issue_id).cloned();
        entries.push(BeadHistoryEntryWire {
            event_id: event.event_id.clone(),
            timestamp: event.timestamp.clone(),
            actor: event.actor.clone(),
            operation: event.operation,
            changes: issue_changes(before.as_ref(), after.as_ref())?,
        });
        if event.operation == BeadEventOperationWire::IssueRemoved {
            break;
        }
    }

    if !found {
        return Err(BeadError::validation(format!(
            "Issue not found: {issue_id}"
        )));
    }

    Ok(BeadHistoryWire {
        issue_id: issue_id.to_string(),
        schema_version: BEAD_HISTORY_WIRE_SCHEMA_VERSION,
        entries,
    })
}

fn lost_notes_from_streams(
    streams: &[BeadEventStreamWire],
    issue_id: Option<&str>,
) -> Result<Vec<BeadLostNotesWire>, BeadError> {
    let streams = validated_event_streams(streams)?;
    let mut issues = BTreeMap::new();
    let mut revisions: BTreeMap<String, Vec<BeadLostNoteRevisionWire>> =
        BTreeMap::new();

    for event in merge_stream_events(&streams) {
        let before_notes = issues
            .get(&event.issue_id)
            .map(|issue: &IssueWire| issue.notes.clone());
        apply_event(&mut issues, event)?;
        let after_notes =
            issues.get(&event.issue_id).map(|issue| issue.notes.clone());

        match after_notes {
            Some(notes) if before_notes.as_ref() != Some(&notes) => {
                revisions.entry(event.issue_id.clone()).or_default().push(
                    BeadLostNoteRevisionWire {
                        timestamp: event.timestamp.clone(),
                        actor: event.actor.clone(),
                        text: notes,
                    },
                );
            }
            None => {
                revisions.remove(&event.issue_id);
            }
            Some(_) => {}
        }
    }

    if let Some(issue_id) = issue_id {
        if !issues.contains_key(issue_id) {
            return Err(BeadError::validation(format!(
                "Issue not found: {issue_id}"
            )));
        }
    }

    let mut findings = Vec::new();
    for (candidate_id, issue) in &issues {
        if issue_id.is_some_and(|selected| selected != candidate_id) {
            continue;
        }
        let dropped_revisions = revisions
            .get(candidate_id)
            .into_iter()
            .flatten()
            .filter_map(|revision| {
                let text = revision.text.trim();
                (!text.is_empty() && !issue.notes.contains(text)).then(|| {
                    BeadLostNoteRevisionWire {
                        timestamp: revision.timestamp.clone(),
                        actor: revision.actor.clone(),
                        text: text.to_string(),
                    }
                })
            })
            .collect::<Vec<_>>();
        if dropped_revisions.is_empty() {
            continue;
        }
        findings.push(BeadLostNotesWire {
            issue_id: candidate_id.clone(),
            current_notes: issue.notes.clone(),
            dropped_revisions,
        });
    }
    Ok(findings)
}

fn issue_changes(
    before: Option<&IssueWire>,
    after: Option<&IssueWire>,
) -> Result<Vec<BeadHistoryChangeWire>, BeadError> {
    let before = issue_fields(before)?;
    let after = issue_fields(after)?;
    let fields: BTreeSet<&str> = before
        .keys()
        .chain(after.keys())
        .map(String::as_str)
        .collect();
    let creating = before.is_empty();
    let removing = after.is_empty();
    let mut changes = Vec::new();

    for field in fields {
        // updated_at is projection metadata set by otherwise no-op events.
        // Omitting it keeps a no-op update truthful: it has no field changes.
        if field == "updated_at" && !creating && !removing {
            continue;
        }
        let from = before.get(field);
        let to = after.get(field);
        if from == to {
            continue;
        }
        if creating && to.map_or(true, |value| is_default_field(field, value)) {
            continue;
        }
        if removing && from.map_or(true, |value| is_default_field(field, value))
        {
            continue;
        }
        changes.push(BeadHistoryChangeWire {
            field: field.to_string(),
            from: from.cloned(),
            to: to.cloned(),
        });
    }
    Ok(changes)
}

fn issue_fields(
    issue: Option<&IssueWire>,
) -> Result<BTreeMap<String, Value>, BeadError> {
    let Some(issue) = issue else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(fields) = serde_json::to_value(issue)? else {
        return Err(BeadError::validation(
            "serialized bead issue is not an object",
        ));
    };
    Ok(fields.into_iter().collect())
}

fn is_default_field(field: &str, value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(value) => !value,
        Value::String(value) => {
            value.is_empty()
                || (field == "status" && value == "open")
                || (field == "issue_type" && value == "phase")
        }
        Value::Array(value) => value.is_empty(),
        Value::Object(value) => value.is_empty(),
        Value::Number(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::events::{
        BeadEventPayloadWire, BeadEventRecordWire,
        BeadIssueUpdateEventFieldsWire, BEAD_EVENT_SCHEMA_VERSION,
    };
    use crate::bead::wire::{BeadTierWire, IssueTypeWire, StatusWire};

    fn issue(
        id: &str,
        issue_type: IssueTypeWire,
        parent: Option<&str>,
    ) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: format!("Issue {id}"),
            status: StatusWire::Open,
            issue_type: issue_type.clone(),
            tier: (issue_type == IssueTypeWire::Plan)
                .then_some(BeadTierWire::Epic),
            parent_id: parent.map(str::to_string),
            owner: "owner@example.com".to_string(),
            assignee: String::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            created_by: "creator".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            size: None,
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: Vec::new(),
        }
    }

    fn event(
        event_id: &str,
        timestamp: &str,
        issue_id: &str,
        operation: BeadEventOperationWire,
        payload: BeadEventPayloadWire,
    ) -> BeadEventRecordWire {
        BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: event_id.to_string(),
            timestamp: timestamp.to_string(),
            actor: "agent".to_string(),
            operation,
            issue_id: issue_id.to_string(),
            payload,
        }
    }

    fn created(
        event_id: &str,
        timestamp: &str,
        issue: IssueWire,
    ) -> BeadEventRecordWire {
        event(
            event_id,
            timestamp,
            &issue.id.clone(),
            BeadEventOperationWire::IssueCreated,
            BeadEventPayloadWire::IssueCreated { issue },
        )
    }

    fn update(
        event_id: &str,
        timestamp: &str,
        issue_id: &str,
        fields: BeadIssueUpdateEventFieldsWire,
    ) -> BeadEventRecordWire {
        event(
            event_id,
            timestamp,
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated { fields },
        )
    }

    fn stream(
        id: &str,
        events: Vec<BeadEventRecordWire>,
    ) -> BeadEventStreamWire {
        BeadEventStreamWire {
            stream_id: id.to_string(),
            root_issue_id: id.to_string(),
            events,
        }
    }

    fn change<'a>(
        entry: &'a BeadHistoryEntryWire,
        field: &str,
    ) -> &'a BeadHistoryChangeWire {
        entry
            .changes
            .iter()
            .find(|change| change.field == field)
            .unwrap()
    }

    #[test]
    fn single_update_and_noop_report_only_real_changes() {
        let issue = issue("beads-1", IssueTypeWire::Plan, None);
        let streams = vec![stream(
            "beads-1",
            vec![
                created("create", "2026-01-01T00:00:00Z", issue),
                update(
                    "update",
                    "2026-01-02T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        title: Some("Renamed".to_string()),
                        ..Default::default()
                    },
                ),
                update(
                    "noop",
                    "2026-01-03T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        title: Some("Renamed".to_string()),
                        ..Default::default()
                    },
                ),
            ],
        )];

        let history = history_from_streams(&streams, "beads-1").unwrap();
        assert_eq!(history.entries.len(), 3);
        assert_eq!(
            change(&history.entries[1], "title"),
            &BeadHistoryChangeWire {
                field: "title".to_string(),
                from: Some(Value::String("Issue beads-1".to_string())),
                to: Some(Value::String("Renamed".to_string())),
            }
        );
        assert!(history.entries[2].changes.is_empty());
    }

    #[test]
    fn notes_history_preserves_each_revision_pair() {
        let mut issue = issue("beads-1", IssueTypeWire::Plan, None);
        issue.notes = "first".to_string();
        let streams = vec![stream(
            "beads-1",
            vec![
                created("create", "2026-01-01T00:00:00Z", issue),
                update(
                    "notes-2",
                    "2026-01-02T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        notes: Some("second".to_string()),
                        ..Default::default()
                    },
                ),
                update(
                    "notes-3",
                    "2026-01-03T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        notes: Some("third".to_string()),
                        ..Default::default()
                    },
                ),
            ],
        )];

        let history = history_from_streams(&streams, "beads-1").unwrap();
        let revisions = history.entries[1..]
            .iter()
            .map(|entry| {
                let notes = change(entry, "notes");
                (notes.from.clone(), notes.to.clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            revisions,
            vec![
                (
                    Some(Value::String("first".to_string())),
                    Some(Value::String("second".to_string()))
                ),
                (
                    Some(Value::String("second".to_string())),
                    Some(Value::String("third".to_string()))
                ),
            ]
        );
    }

    #[test]
    fn lost_notes_reports_overwritten_nonempty_revisions() {
        let mut issue = issue("beads-1", IssueTypeWire::Plan, None);
        issue.notes = "first".to_string();
        let streams = vec![stream(
            "beads-1",
            vec![
                created("create", "2026-01-01T00:00:00Z", issue),
                update(
                    "notes-2",
                    "2026-01-02T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        notes: Some("second".to_string()),
                        ..Default::default()
                    },
                ),
                update(
                    "notes-3",
                    "2026-01-03T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        notes: Some("third".to_string()),
                        ..Default::default()
                    },
                ),
            ],
        )];

        let findings = lost_notes_from_streams(&streams, None).unwrap();

        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].issue_id, "beads-1");
        assert_eq!(findings[0].current_notes, "third");
        assert_eq!(
            findings[0]
                .dropped_revisions
                .iter()
                .map(|revision| revision.text.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        assert_eq!(
            findings[0].dropped_revisions[0].timestamp,
            "2026-01-01T00:00:00Z"
        );
        assert_eq!(findings[0].dropped_revisions[0].actor, "agent");
    }

    #[test]
    fn lost_notes_ignores_append_only_revision_chains() {
        let mut issue = issue("beads-1", IssueTypeWire::Plan, None);
        issue.notes = "first".to_string();
        let streams = vec![stream(
            "beads-1",
            vec![
                created("create", "2026-01-01T00:00:00Z", issue),
                update(
                    "append",
                    "2026-01-02T00:00:00Z",
                    "beads-1",
                    BeadIssueUpdateEventFieldsWire {
                        notes: Some("first\n\nsecond".to_string()),
                        ..Default::default()
                    },
                ),
            ],
        )];

        assert!(lost_notes_from_streams(&streams, None).unwrap().is_empty());
    }

    #[test]
    fn lost_notes_are_stable_by_issue_id_and_support_one_issue() {
        let mut second = issue("beads-2", IssueTypeWire::Plan, None);
        second.notes = "old two".to_string();
        let mut first = issue("beads-1", IssueTypeWire::Plan, None);
        first.notes = "old one".to_string();
        let streams = vec![
            stream(
                "beads-2",
                vec![
                    created("create-2", "2026-01-01T00:00:00Z", second),
                    update(
                        "overwrite-2",
                        "2026-01-02T00:00:00Z",
                        "beads-2",
                        BeadIssueUpdateEventFieldsWire {
                            notes: Some("new two".to_string()),
                            ..Default::default()
                        },
                    ),
                ],
            ),
            stream(
                "beads-1",
                vec![
                    created("create-1", "2026-01-01T00:00:00Z", first),
                    update(
                        "overwrite-1",
                        "2026-01-02T00:00:00Z",
                        "beads-1",
                        BeadIssueUpdateEventFieldsWire {
                            notes: Some("new one".to_string()),
                            ..Default::default()
                        },
                    ),
                ],
            ),
        ];

        let all = lost_notes_from_streams(&streams, None).unwrap();
        assert_eq!(
            all.iter()
                .map(|finding| finding.issue_id.as_str())
                .collect::<Vec<_>>(),
            vec!["beads-1", "beads-2"]
        );
        assert_eq!(
            lost_notes_from_streams(&streams, Some("beads-2"))
                .unwrap()
                .as_slice(),
            &all[1..]
        );
        let error =
            lost_notes_from_streams(&streams, Some("missing")).unwrap_err();
        assert_eq!(error.message, "Issue not found: missing");
    }

    #[test]
    fn close_reopen_close_status_timeline_is_complete() {
        let streams = vec![stream(
            "beads-1",
            vec![
                created(
                    "create",
                    "2026-01-01T00:00:00Z",
                    issue("beads-1", IssueTypeWire::Plan, None),
                ),
                event(
                    "close-1",
                    "2026-01-02T00:00:00Z",
                    "beads-1",
                    BeadEventOperationWire::IssueClosed,
                    BeadEventPayloadWire::IssueClosed {
                        close_reason: Some("done".to_string()),
                        resolution: None,
                        forced_descendant_ids: Vec::new(),
                    },
                ),
                event(
                    "open",
                    "2026-01-03T00:00:00Z",
                    "beads-1",
                    BeadEventOperationWire::IssueOpened,
                    BeadEventPayloadWire::IssueOpened,
                ),
                event(
                    "close-2",
                    "2026-01-04T00:00:00Z",
                    "beads-1",
                    BeadEventOperationWire::IssueClosed,
                    BeadEventPayloadWire::IssueClosed {
                        close_reason: None,
                        resolution: None,
                        forced_descendant_ids: Vec::new(),
                    },
                ),
            ],
        )];

        let history = history_from_streams(&streams, "beads-1").unwrap();
        assert_eq!(
            history
                .entries
                .iter()
                .map(|entry| entry.operation)
                .collect::<Vec<_>>(),
            vec![
                BeadEventOperationWire::IssueCreated,
                BeadEventOperationWire::IssueClosed,
                BeadEventOperationWire::IssueOpened,
                BeadEventOperationWire::IssueClosed,
            ]
        );
        assert_eq!(
            change(&history.entries[2], "status").to,
            Some(Value::String("open".to_string()))
        );
        assert_eq!(
            change(&history.entries[3], "closed_at").to,
            Some(Value::String("2026-01-04T00:00:00Z".to_string()))
        );
    }

    #[test]
    fn phase_history_is_read_from_parent_stream() {
        let phase = issue("beads-1.1", IssueTypeWire::Phase, Some("beads-1"));
        let streams = vec![stream(
            "beads-1",
            vec![
                created(
                    "root-create",
                    "2026-01-01T00:00:00Z",
                    issue("beads-1", IssueTypeWire::Plan, None),
                ),
                created("phase-create", "2026-01-02T00:00:00Z", phase),
                update(
                    "phase-update",
                    "2026-01-03T00:00:00Z",
                    "beads-1.1",
                    BeadIssueUpdateEventFieldsWire {
                        notes: Some("phase note".to_string()),
                        ..Default::default()
                    },
                ),
            ],
        )];

        let history = history_from_streams(&streams, "beads-1.1").unwrap();
        assert_eq!(history.entries.len(), 2);
        assert_eq!(history.entries[1].event_id, "phase-update");
    }

    #[test]
    fn merged_order_keeps_appended_predated_event_after_earlier_stream_event() {
        let streams = vec![
            stream(
                "beads-1",
                vec![
                    created(
                        "create",
                        "2026-01-02T00:00:00Z",
                        issue("beads-1", IssueTypeWire::Plan, None),
                    ),
                    update(
                        "appended",
                        "2026-01-01T00:00:00Z",
                        "beads-1",
                        BeadIssueUpdateEventFieldsWire {
                            notes: Some("late append".to_string()),
                            ..Default::default()
                        },
                    ),
                ],
            ),
            stream(
                "beads-2",
                vec![created(
                    "other-create",
                    "2025-12-31T00:00:00Z",
                    issue("beads-2", IssueTypeWire::Plan, None),
                )],
            ),
        ];

        let history = history_from_streams(&streams, "beads-1").unwrap();
        assert_eq!(
            history
                .entries
                .iter()
                .map(|entry| entry.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["create", "appended"]
        );
    }

    #[test]
    fn unknown_issue_is_an_error() {
        let streams = vec![stream(
            "beads-1",
            vec![created(
                "create",
                "2026-01-01T00:00:00Z",
                issue("beads-1", IssueTypeWire::Plan, None),
            )],
        )];

        let error = history_from_streams(&streams, "missing").unwrap_err();
        assert_eq!(error.message, "Issue not found: missing");
    }

    #[test]
    fn removal_ends_the_timeline() {
        let streams = vec![stream(
            "beads-1",
            vec![
                created(
                    "create",
                    "2026-01-01T00:00:00Z",
                    issue("beads-1", IssueTypeWire::Plan, None),
                ),
                event(
                    "remove",
                    "2026-01-02T00:00:00Z",
                    "beads-1",
                    BeadEventOperationWire::IssueRemoved,
                    BeadEventPayloadWire::IssueRemoved {
                        cascade_removed_issue_ids: Vec::new(),
                    },
                ),
                created(
                    "recreate",
                    "2026-01-03T00:00:00Z",
                    issue("beads-1", IssueTypeWire::Plan, None),
                ),
            ],
        )];

        let history = history_from_streams(&streams, "beads-1").unwrap();
        assert_eq!(
            history
                .entries
                .iter()
                .map(|entry| entry.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["create", "remove"]
        );
        assert_eq!(change(&history.entries[1], "id").to, None);
    }
}
