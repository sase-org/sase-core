use sase_core::{
    export_issues_to_jsonl, import_issues_to_event_streams, parse_issues_jsonl,
    reduce_event_streams, BeadError, BeadEventOperationWire,
    BeadEventPayloadWire, BeadEventRecordWire, BeadEventStoreManifestWire,
    BeadEventStreamWire, BeadIssueUpdateEventFieldsWire, BeadTierWire,
    DependencyWire, IssueTypeWire, IssueWire, StatusWire,
    BEAD_EVENT_SCHEMA_VERSION,
};

const EVENT_ROUNDTRIP_SCHEMA: &str =
    include_str!("fixtures/bead/jsonl/event_roundtrip_schema.jsonl");
const CURRENT_SCHEMA: &str =
    include_str!("fixtures/bead/jsonl/current_schema.jsonl");
const PRE_CHANGESPEC_SCHEMA: &str =
    include_str!("fixtures/bead/jsonl/pre_changespec_metadata_schema.jsonl");
const CORRUPT_LINES: &str =
    include_str!("fixtures/bead/jsonl/corrupt_lines.jsonl");
const EVENT_MANIFEST: &str =
    include_str!("fixtures/bead/events/event_roundtrip/manifest.json");
const EVENT_GOLD_1_STREAM: &str =
    include_str!("fixtures/bead/events/event_roundtrip/streams/gold-1.jsonl");
const EVENT_GOLD_LEGEND_STREAM: &str = include_str!(
    "fixtures/bead/events/event_roundtrip/streams/gold-legend.jsonl"
);

#[test]
fn jsonl_import_to_events_reduces_to_byte_compatible_projection() {
    let outcome = parse_issues_jsonl(EVENT_ROUNDTRIP_SCHEMA);
    assert_eq!(outcome.loaded_rows, 3);

    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
    assert_eq!(
        streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["gold-1", "gold-legend"]
    );
    let manifest = BeadEventStoreManifestWire::from_streams(&streams);
    manifest.validate().unwrap();
    assert_eq!(manifest.stream_count, 2);

    let operations = streams
        .iter()
        .flat_map(|stream| stream.events.iter().map(|event| event.operation))
        .collect::<Vec<_>>();
    assert!(operations.contains(&BeadEventOperationWire::IssueCreated));
    assert!(operations.contains(&BeadEventOperationWire::DependencyAdded));

    let reduced = reduce_event_streams(&streams).unwrap();
    let exported = export_issues_to_jsonl(&reduced).unwrap();
    assert_eq!(exported, EVENT_ROUNDTRIP_SCHEMA);
}

#[test]
fn serialized_event_store_fixture_matches_import_and_reduces() {
    let manifest: BeadEventStoreManifestWire =
        serde_json::from_str(EVENT_MANIFEST).unwrap();
    manifest.validate().unwrap();
    let streams = vec![
        event_stream_fixture("gold-1", EVENT_GOLD_1_STREAM),
        event_stream_fixture("gold-legend", EVENT_GOLD_LEGEND_STREAM),
    ];
    assert_eq!(manifest, BeadEventStoreManifestWire::from_streams(&streams));

    let imported = import_issues_to_event_streams(
        &parse_issues_jsonl(EVENT_ROUNDTRIP_SCHEMA).issues,
    )
    .unwrap();
    assert_eq!(streams, imported);

    let exported =
        export_issues_to_jsonl(&reduce_event_streams(&streams).unwrap())
            .unwrap();
    assert_eq!(exported, EVENT_ROUNDTRIP_SCHEMA);
}

#[test]
fn event_import_preserves_legacy_defaults_and_corrupt_jsonl_tolerance() {
    for input in [CURRENT_SCHEMA, PRE_CHANGESPEC_SCHEMA, CORRUPT_LINES] {
        let outcome = parse_issues_jsonl(input);
        let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
        let reduced = reduce_event_streams(&streams).unwrap();
        assert_eq!(reduced, outcome.issues);
    }
}

#[test]
fn legacy_empty_timestamps_still_import_to_valid_events() {
    let outcome = parse_issues_jsonl(
        r#"{"id":"legacy-1","title":"Legacy","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
    );

    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();

    assert_eq!(streams[0].events[0].timestamp, "1970-01-01T00:00:00Z");
    assert_eq!(reduce_event_streams(&streams).unwrap(), outcome.issues);
}

#[test]
fn reducer_handles_current_mutation_operation_variants() {
    let epic = issue(
        "gold-1",
        "Epic",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let phase_one = issue(
        "gold-1.1",
        "Phase One",
        IssueTypeWire::Phase,
        Some("gold-1"),
        "2026-01-01T00:01:00Z",
    );
    let phase_two = issue(
        "gold-1.2",
        "Phase Two",
        IssueTypeWire::Phase,
        Some("gold-1"),
        "2026-01-01T00:02:00Z",
    );
    let stream = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![
            event(
                "gold-1",
                "2026-01-01T00:00:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: epic },
            ),
            event(
                "gold-1.1",
                "2026-01-01T00:01:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: phase_one },
            ),
            event(
                "gold-1.2",
                "2026-01-01T00:02:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: phase_two },
            ),
            event(
                "gold-1",
                "2026-01-01T00:03:00Z",
                BeadEventOperationWire::IssueUpdated,
                BeadEventPayloadWire::IssueUpdated {
                    fields: BeadIssueUpdateEventFieldsWire {
                        title: Some("Updated Epic".to_string()),
                        model: Some("codex/gpt-5.5".to_string()),
                        changespec_name: Some("updated_changespec".to_string()),
                        changespec_bug_id: Some("BUG-1".to_string()),
                        ..Default::default()
                    },
                },
            ),
            event(
                "gold-1.2",
                "2026-01-01T00:04:00Z",
                BeadEventOperationWire::DependencyAdded,
                BeadEventPayloadWire::DependencyAdded {
                    dependency: DependencyWire {
                        issue_id: "gold-1.2".to_string(),
                        depends_on_id: "gold-1.1".to_string(),
                        created_at: "2026-01-01T00:04:00Z".to_string(),
                        created_by: "owner@example.com".to_string(),
                    },
                },
            ),
            event(
                "gold-1",
                "2026-01-01T00:05:00Z",
                BeadEventOperationWire::ReadyMarked,
                BeadEventPayloadWire::ReadyMarked,
            ),
            event(
                "gold-1",
                "2026-01-01T00:06:00Z",
                BeadEventOperationWire::ReadyUnmarked,
                BeadEventPayloadWire::ReadyUnmarked,
            ),
            event(
                "gold-1.1",
                "2026-01-01T00:07:00Z",
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed {
                    agent_name: "agent-1".to_string(),
                },
            ),
            event(
                "gold-1.2",
                "2026-01-01T00:08:00Z",
                BeadEventOperationWire::IssueClosed,
                BeadEventPayloadWire::IssueClosed {
                    close_reason: Some("done".to_string()),
                },
            ),
            event(
                "gold-1.2",
                "2026-01-01T00:09:00Z",
                BeadEventOperationWire::IssueOpened,
                BeadEventPayloadWire::IssueOpened,
            ),
            event(
                "gold-1.2",
                "2026-01-01T00:10:00Z",
                BeadEventOperationWire::IssueRemoved,
                BeadEventPayloadWire::IssueRemoved {
                    cascade_removed_issue_ids: Vec::new(),
                },
            ),
        ],
    };

    let reduced = reduce_event_streams(&[stream]).unwrap();
    assert_eq!(reduced.len(), 2);
    let epic = reduced.iter().find(|issue| issue.id == "gold-1").unwrap();
    assert_eq!(epic.title, "Updated Epic");
    assert_eq!(epic.model, "codex/gpt-5.5");
    assert_eq!(epic.changespec_bug_id, "BUG-1");
    assert!(!epic.is_ready_to_work);
    let phase_one =
        reduced.iter().find(|issue| issue.id == "gold-1.1").unwrap();
    assert_eq!(phase_one.status, StatusWire::InProgress);
    assert_eq!(phase_one.assignee, "agent-1");
    assert!(reduced.iter().all(|issue| issue.id != "gold-1.2"));
}

#[test]
fn reducer_removes_plan_children_and_dependency_edges_on_cascade_remove() {
    let epic = issue(
        "gold-1",
        "Epic",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let phase = issue(
        "gold-1.1",
        "Phase",
        IssueTypeWire::Phase,
        Some("gold-1"),
        "2026-01-01T00:01:00Z",
    );
    let other = issue(
        "gold-2",
        "Other",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:02:00Z",
    );
    let streams = [
        BeadEventStreamWire {
            stream_id: "gold-1".to_string(),
            root_issue_id: "gold-1".to_string(),
            events: vec![
                event(
                    "gold-1",
                    "2026-01-01T00:00:00Z",
                    BeadEventOperationWire::IssueCreated,
                    BeadEventPayloadWire::IssueCreated { issue: epic },
                ),
                event(
                    "gold-1.1",
                    "2026-01-01T00:01:00Z",
                    BeadEventOperationWire::IssueCreated,
                    BeadEventPayloadWire::IssueCreated { issue: phase },
                ),
            ],
        },
        BeadEventStreamWire {
            stream_id: "gold-2".to_string(),
            root_issue_id: "gold-2".to_string(),
            events: vec![
                event(
                    "gold-2",
                    "2026-01-01T00:02:00Z",
                    BeadEventOperationWire::IssueCreated,
                    BeadEventPayloadWire::IssueCreated { issue: other },
                ),
                event(
                    "gold-2",
                    "2026-01-01T00:03:00Z",
                    BeadEventOperationWire::DependencyAdded,
                    BeadEventPayloadWire::DependencyAdded {
                        dependency: DependencyWire {
                            issue_id: "gold-2".to_string(),
                            depends_on_id: "gold-1.1".to_string(),
                            created_at: "2026-01-01T00:03:00Z".to_string(),
                            created_by: String::new(),
                        },
                    },
                ),
                event(
                    "gold-1",
                    "2026-01-01T00:04:00Z",
                    BeadEventOperationWire::IssueRemoved,
                    BeadEventPayloadWire::IssueRemoved {
                        cascade_removed_issue_ids: vec!["gold-1.1".to_string()],
                    },
                ),
            ],
        },
    ];

    let reduced = reduce_event_streams(&streams).unwrap();
    assert_eq!(reduced.len(), 1);
    assert_eq!(reduced[0].id, "gold-2");
    assert!(reduced[0].dependencies.is_empty());
}

#[test]
fn event_validation_rejects_operation_payload_mismatch() {
    let invalid = event(
        "gold-1",
        "2026-01-01T00:00:00Z",
        BeadEventOperationWire::ReadyMarked,
        BeadEventPayloadWire::IssueOpened,
    );

    let err = invalid.validate().unwrap_err();
    assert!(err.message.contains("operation/payload mismatch"));
}

fn event(
    issue_id: &str,
    timestamp: &str,
    operation: BeadEventOperationWire,
    payload: BeadEventPayloadWire,
) -> BeadEventRecordWire {
    BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: format!("{issue_id}:{timestamp:?}"),
        timestamp: timestamp.to_string(),
        actor: "owner@example.com".to_string(),
        operation,
        issue_id: issue_id.to_string(),
        payload,
    }
}

fn event_stream_fixture(
    stream_id: &str,
    contents: &str,
) -> BeadEventStreamWire {
    BeadEventStreamWire {
        stream_id: stream_id.to_string(),
        root_issue_id: stream_id.to_string(),
        events: contents
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect(),
    }
}

fn issue(
    id: &str,
    title: &str,
    issue_type: IssueTypeWire,
    parent_id: Option<&str>,
    timestamp: &str,
) -> IssueWire {
    IssueWire {
        id: id.to_string(),
        title: title.to_string(),
        status: StatusWire::Open,
        issue_type: issue_type.clone(),
        tier: match issue_type {
            IssueTypeWire::Plan => Some(BeadTierWire::Epic),
            IssueTypeWire::Phase => None,
        },
        parent_id: parent_id.map(str::to_string),
        owner: "owner@example.com".to_string(),
        assignee: String::new(),
        created_at: timestamp.to_string(),
        created_by: "owner@example.com".to_string(),
        updated_at: timestamp.to_string(),
        closed_at: None,
        close_reason: None,
        description: String::new(),
        notes: String::new(),
        design: String::new(),
        model: String::new(),
        is_ready_to_work: false,
        epic_count: None,
        changespec_name: String::new(),
        changespec_bug_id: String::new(),
        dependencies: Vec::new(),
    }
}

fn _assert_error_send_sync(_: &BeadError) {}
