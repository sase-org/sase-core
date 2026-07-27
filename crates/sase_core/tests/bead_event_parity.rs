use sase_core::{
    export_issues_to_jsonl, import_issues_to_event_streams,
    merge_bead_event_streams, parse_issues_jsonl, reduce_event_streams,
    BeadError, BeadEventOperationWire, BeadEventPayloadWire,
    BeadEventRecordWire, BeadEventStoreManifestWire, BeadEventStreamWire,
    BeadIssueUpdateEventFieldsWire, BeadTierWire, DependencyWire,
    IssueTypeWire, IssueWire, StatusWire, BEAD_EVENT_SCHEMA_VERSION,
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

#[test]
fn jsonl_import_to_events_reduces_to_byte_compatible_projection() {
    let outcome = parse_issues_jsonl(EVENT_ROUNDTRIP_SCHEMA);
    assert_eq!(outcome.loaded_rows, 2);

    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
    assert_eq!(
        streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["gold-1"]
    );
    let manifest = BeadEventStoreManifestWire::from_streams(&streams);
    manifest.validate().unwrap();
    assert_eq!(manifest.stream_count, 1);

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
    let streams = vec![event_stream_fixture("gold-1", EVENT_GOLD_1_STREAM)];
    assert_eq!(manifest, BeadEventStoreManifestWire::from_streams(&streams));

    let imported = import_issues_to_event_streams(
        &parse_issues_jsonl(EVENT_ROUNDTRIP_SCHEMA).issues,
    )
    .unwrap();
    let mut imported_with_legacy_ids = imported.clone();
    for (imported_stream, legacy_stream) in
        imported_with_legacy_ids.iter_mut().zip(&streams)
    {
        for (imported_event, legacy_event) in
            imported_stream.events.iter_mut().zip(&legacy_stream.events)
        {
            imported_event.event_id.clone_from(&legacy_event.event_id);
        }
    }
    assert_eq!(streams, imported_with_legacy_ids);
    assert!(streams[0].events.iter().all(|event| event
        .event_id
        .rsplit(':')
        .next()
        .unwrap()
        .len()
        != 64));
    assert!(imported[0]
        .events
        .iter()
        .all(|event| is_content_hashed_event_id(&event.event_id)));

    let exported =
        export_issues_to_jsonl(&reduce_event_streams(&streams).unwrap())
            .unwrap();
    assert_eq!(exported, EVENT_ROUNDTRIP_SCHEMA);
    assert_eq!(
        export_issues_to_jsonl(&reduce_event_streams(&imported).unwrap())
            .unwrap(),
        EVENT_ROUNDTRIP_SCHEMA
    );
}

#[test]
fn event_import_mints_reproducible_content_hashed_ids() {
    let issues = parse_issues_jsonl(EVENT_ROUNDTRIP_SCHEMA).issues;
    let first = import_issues_to_event_streams(&issues).unwrap();
    let repeated = import_issues_to_event_streams(&issues).unwrap();
    assert_eq!(first, repeated);

    let mut changed_issues = issues;
    changed_issues[0].title = "Changed content".to_string();
    let changed = import_issues_to_event_streams(&changed_issues).unwrap();

    assert_ne!(first[0].events[0].event_id, changed[0].events[0].event_id);
    assert!(first[0].events[0]
        .event_id
        .starts_with("gold-1:000001:issue_created:gold-1:"));
    assert!(first[0]
        .events
        .iter()
        .all(|event| is_content_hashed_event_id(&event.event_id)));
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
                    resolution: None,
                    forced_descendant_ids: Vec::new(),
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
fn cross_stream_dependencies_resolve_after_all_creates() {
    let dependent = issue(
        "alpha",
        "Dependent",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let target = issue(
        "omega",
        "Target",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:01:00Z",
    );
    let streams = [
        BeadEventStreamWire {
            stream_id: "alpha".to_string(),
            root_issue_id: "alpha".to_string(),
            events: vec![
                event(
                    "alpha",
                    "2026-01-01T00:00:00Z",
                    BeadEventOperationWire::IssueCreated,
                    BeadEventPayloadWire::IssueCreated { issue: dependent },
                ),
                event(
                    "alpha",
                    "2026-01-01T00:02:00Z",
                    BeadEventOperationWire::DependencyAdded,
                    BeadEventPayloadWire::DependencyAdded {
                        dependency: DependencyWire {
                            issue_id: "alpha".to_string(),
                            depends_on_id: "omega".to_string(),
                            created_at: "2026-01-01T00:02:00Z".to_string(),
                            created_by: String::new(),
                        },
                    },
                ),
            ],
        },
        BeadEventStreamWire {
            stream_id: "omega".to_string(),
            root_issue_id: "omega".to_string(),
            events: vec![event(
                "omega",
                "2026-01-01T00:01:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: target },
            )],
        },
    ];

    let reduced = reduce_event_streams(&streams).unwrap();
    let alpha = reduced.iter().find(|issue| issue.id == "alpha").unwrap();
    assert_eq!(alpha.dependencies[0].depends_on_id, "omega");
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

#[test]
fn merge_event_stream_unions_concurrent_appends_deterministically() {
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
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![
            numbered_event(
                "gold-1",
                1,
                "2026-01-01T00:00:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: epic },
            ),
            numbered_event(
                "gold-1.1",
                2,
                "2026-01-01T00:01:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: phase_one },
            ),
            numbered_event(
                "gold-1.2",
                3,
                "2026-01-01T00:02:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: phase_two },
            ),
        ],
    };
    let mut ours = base.clone();
    ours.events.push(numbered_event(
        "gold-1.1",
        4,
        "2026-01-01T00:03:00Z",
        BeadEventOperationWire::IssueClosed,
        BeadEventPayloadWire::IssueClosed {
            close_reason: Some("local done".to_string()),
            resolution: None,
            forced_descendant_ids: Vec::new(),
        },
    ));
    let mut theirs = base.clone();
    theirs.events.push(numbered_event(
        "gold-1.2",
        4,
        "2026-01-01T00:04:00Z",
        BeadEventOperationWire::IssueClosed,
        BeadEventPayloadWire::IssueClosed {
            close_reason: Some("upstream done".to_string()),
            resolution: None,
            forced_descendant_ids: Vec::new(),
        },
    ));

    let merged = merge_bead_event_streams(&base, &ours, &theirs).unwrap();
    let repeated = merge_bead_event_streams(&base, &ours, &theirs).unwrap();
    let swapped = merge_bead_event_streams(&base, &theirs, &ours).unwrap();

    assert_eq!(merged.events.len(), 5);
    assert_eq!(merged, repeated);
    assert_eq!(merged, swapped);
    assert_eq!(merged.events[3].issue_id, "gold-1.1");
    assert_eq!(merged.events[4].issue_id, "gold-1.2");
    assert_eq!(
        merged.events[3].event_id,
        "gold-1:000004:issue_closed:gold-1.1"
    );
    assert_eq!(
        merged.events[4].event_id,
        "gold-1:000004:issue_closed:gold-1.2"
    );
    assert_eq!(&merged.events[..base.events.len()], &base.events);
    let reduced = reduce_event_streams(&[merged]).unwrap();
    assert_eq!(
        reduced
            .iter()
            .filter(|issue| issue.status == StatusWire::Closed)
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>(),
        vec!["gold-1.1", "gold-1.2"]
    );
}

#[test]
fn merge_event_stream_keeps_exact_duplicate_append_once() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![numbered_event(
            "gold-1",
            1,
            "2026-01-01T00:00:00Z",
            BeadEventOperationWire::IssueCreated,
            BeadEventPayloadWire::IssueCreated { issue: root },
        )],
    };
    let duplicate = numbered_event(
        "gold-1",
        2,
        "2026-01-01T00:01:00Z",
        BeadEventOperationWire::ReadyMarked,
        BeadEventPayloadWire::ReadyMarked,
    );
    let mut ours = base.clone();
    ours.events.push(duplicate.clone());
    let mut theirs = base.clone();
    theirs.events.push(duplicate);

    let merged = merge_bead_event_streams(&base, &ours, &theirs).unwrap();

    assert_eq!(merged.events.len(), 2);
    assert_eq!(
        merged.events[1].event_id,
        "gold-1:000002:ready_marked:gold-1"
    );
}

#[test]
fn merge_event_stream_orders_non_base_union_deterministically() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![numbered_event(
            "gold-1",
            1,
            "2026-01-01T00:00:00Z",
            BeadEventOperationWire::IssueCreated,
            BeadEventPayloadWire::IssueCreated { issue: root },
        )],
    };
    let mut ours = base.clone();
    ours.events.extend([
        numbered_event(
            "gold-1",
            2,
            "2026-01-01T00:04:00Z",
            BeadEventOperationWire::ReadyMarked,
            BeadEventPayloadWire::ReadyMarked,
        ),
        numbered_event(
            "gold-1",
            3,
            "2026-01-01T00:01:00Z",
            BeadEventOperationWire::ReadyUnmarked,
            BeadEventPayloadWire::ReadyUnmarked,
        ),
    ]);
    let mut theirs = base.clone();
    theirs.events.extend([
        numbered_event(
            "gold-1",
            2,
            "2026-01-01T00:02:00Z",
            BeadEventOperationWire::IssueClosed,
            BeadEventPayloadWire::IssueClosed {
                close_reason: None,
                resolution: None,
                forced_descendant_ids: Vec::new(),
            },
        ),
        numbered_event(
            "gold-1",
            3,
            "2026-01-01T00:03:00Z",
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
        ),
    ]);

    let merged = merge_bead_event_streams(&base, &ours, &theirs).unwrap();
    let operations = merged
        .events
        .iter()
        .skip(1)
        .map(|event| event.operation)
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            BeadEventOperationWire::ReadyUnmarked,
            BeadEventOperationWire::IssueClosed,
            BeadEventOperationWire::IssueOpened,
            BeadEventOperationWire::ReadyMarked,
        ]
    );
}

#[test]
fn merge_event_stream_rejects_deleted_or_rewritten_base_events() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![
            numbered_event(
                "gold-1",
                1,
                "2026-01-01T00:00:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: root },
            ),
            numbered_event(
                "gold-1",
                2,
                "2026-01-01T00:01:00Z",
                BeadEventOperationWire::ReadyMarked,
                BeadEventPayloadWire::ReadyMarked,
            ),
        ],
    };
    let mut deleted = base.clone();
    deleted.events.remove(1);
    deleted.events.insert(
        0,
        event(
            "gold-1",
            "2025-12-31T23:59:00Z",
            BeadEventOperationWire::ReadyUnmarked,
            BeadEventPayloadWire::ReadyUnmarked,
        ),
    );
    let mut rewritten = base.clone();
    rewritten.events[1].actor = "rewriter@example.com".to_string();

    let deletion_error =
        merge_bead_event_streams(&base, &deleted, &base).unwrap_err();
    let rewrite_error =
        merge_bead_event_streams(&base, &base, &rewritten).unwrap_err();

    assert!(deletion_error.message.contains("missing base event 2"));
    assert!(rewrite_error.message.contains("rewrote base event 2"));
}

#[test]
fn merge_event_stream_accepts_interleaved_additions_and_preserves_ids() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![
            numbered_event(
                "gold-1",
                1,
                "2026-01-01T00:00:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: root },
            ),
            numbered_event(
                "gold-1",
                2,
                "2026-01-01T00:04:00Z",
                BeadEventOperationWire::ReadyMarked,
                BeadEventPayloadWire::ReadyMarked,
            ),
        ],
    };
    let before = event(
        "gold-1",
        "2025-12-31T23:59:00Z",
        BeadEventOperationWire::IssueOpened,
        BeadEventPayloadWire::IssueOpened,
    );
    let between = event(
        "gold-1",
        "2026-01-01T00:02:00Z",
        BeadEventOperationWire::ReadyUnmarked,
        BeadEventPayloadWire::ReadyUnmarked,
    );
    let after = event(
        "gold-1",
        "2026-01-01T00:05:00Z",
        BeadEventOperationWire::IssueClosed,
        BeadEventPayloadWire::IssueClosed {
            close_reason: None,
            resolution: None,
            forced_descendant_ids: Vec::new(),
        },
    );
    let ours = BeadEventStreamWire {
        events: vec![
            before.clone(),
            base.events[0].clone(),
            between.clone(),
            base.events[1].clone(),
            after.clone(),
        ],
        ..base.clone()
    };

    let merged = merge_bead_event_streams(&base, &ours, &base).unwrap();

    assert_eq!(&merged.events[..2], &base.events);
    assert_eq!(&merged.events[2..], &[before, between, after]);
}

#[test]
fn merge_event_stream_union_is_associative_and_idempotent() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![numbered_event(
            "gold-1",
            1,
            "2026-01-01T00:00:00Z",
            BeadEventOperationWire::IssueCreated,
            BeadEventPayloadWire::IssueCreated { issue: root },
        )],
    };
    let additions = [
        event(
            "gold-1",
            "2026-01-01T00:03:00Z",
            BeadEventOperationWire::ReadyMarked,
            BeadEventPayloadWire::ReadyMarked,
        ),
        event(
            "gold-1",
            "2026-01-01T00:01:00Z",
            BeadEventOperationWire::ReadyUnmarked,
            BeadEventPayloadWire::ReadyUnmarked,
        ),
        event(
            "gold-1",
            "2026-01-01T00:02:00Z",
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
        ),
    ];
    let branches = additions.map(|addition| BeadEventStreamWire {
        events: vec![base.events[0].clone(), addition],
        ..base.clone()
    });

    let ab =
        merge_bead_event_streams(&base, &branches[0], &branches[1]).unwrap();
    let bc =
        merge_bead_event_streams(&base, &branches[1], &branches[2]).unwrap();
    let left = merge_bead_event_streams(&base, &ab, &branches[2]).unwrap();
    let right = merge_bead_event_streams(&base, &branches[0], &bc).unwrap();
    let repeated = merge_bead_event_streams(&base, &left, &left).unwrap();

    assert_eq!(left, right);
    assert_eq!(left, repeated);
    assert_eq!(
        left.events
            .iter()
            .skip(1)
            .map(|event| event.timestamp.as_str())
            .collect::<Vec<_>>(),
        vec![
            "2026-01-01T00:01:00Z",
            "2026-01-01T00:02:00Z",
            "2026-01-01T00:03:00Z",
        ]
    );
}

#[test]
fn merge_event_stream_supports_sequential_rebase_replay() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:00:00Z",
    );
    let base = BeadEventStreamWire {
        stream_id: "gold-1".to_string(),
        root_issue_id: "gold-1".to_string(),
        events: vec![numbered_event(
            "gold-1",
            1,
            "2026-01-01T00:00:00Z",
            BeadEventOperationWire::IssueCreated,
            BeadEventPayloadWire::IssueCreated { issue: root },
        )],
    };
    let local_events = vec![
        event(
            "gold-1",
            "2026-01-01T00:04:00Z",
            BeadEventOperationWire::ReadyMarked,
            BeadEventPayloadWire::ReadyMarked,
        ),
        event(
            "gold-1",
            "2026-01-01T00:02:00Z",
            BeadEventOperationWire::ReadyUnmarked,
            BeadEventPayloadWire::ReadyUnmarked,
        ),
        event(
            "gold-1",
            "2026-01-01T00:06:00Z",
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
        ),
    ];
    let upstream_events = vec![
        event(
            "gold-1",
            "2026-01-01T00:01:00Z",
            BeadEventOperationWire::IssueClosed,
            BeadEventPayloadWire::IssueClosed {
                close_reason: None,
                resolution: None,
                forced_descendant_ids: Vec::new(),
            },
        ),
        event(
            "gold-1",
            "2026-01-01T00:03:00Z",
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
        ),
    ];
    let mut upstream = base.clone();
    upstream.events.extend(upstream_events.clone());
    let local_states = (1..=local_events.len())
        .map(|count| {
            let mut stream = base.clone();
            stream.events.extend(local_events[..count].iter().cloned());
            stream
        })
        .collect::<Vec<_>>();

    let mut rebased = upstream;
    let mut previous_local = base.clone();
    for local_state in local_states {
        rebased =
            merge_bead_event_streams(&previous_local, &rebased, &local_state)
                .unwrap();
        rebased.validate().unwrap();
        previous_local = local_state;
    }

    let expected_events = base
        .events
        .iter()
        .chain(&local_events)
        .chain(&upstream_events)
        .collect::<Vec<_>>();
    assert_eq!(rebased.events.len(), expected_events.len());
    for expected in expected_events {
        assert_eq!(
            rebased
                .events
                .iter()
                .filter(|event| *event == expected)
                .count(),
            1,
            "event {} must appear exactly once",
            expected.event_id
        );
    }
}

#[test]
fn reduce_applies_merged_stream_events_in_recorded_order() {
    let root = issue(
        "gold-1",
        "Root",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:05:00Z",
    );
    let other = issue(
        "gold-2",
        "Other",
        IssueTypeWire::Plan,
        None,
        "2026-01-01T00:03:00Z",
    );
    let streams = [
        BeadEventStreamWire {
            stream_id: "gold-1".to_string(),
            root_issue_id: "gold-1".to_string(),
            events: vec![
                event(
                    "gold-1",
                    "2026-01-01T00:05:00Z",
                    BeadEventOperationWire::IssueCreated,
                    BeadEventPayloadWire::IssueCreated { issue: root },
                ),
                // Merged-in event appended after later-timestamped entries:
                // recorded order, not timestamp order, is causal in-stream.
                event(
                    "gold-1",
                    "2026-01-01T00:01:00Z",
                    BeadEventOperationWire::IssueClosed,
                    BeadEventPayloadWire::IssueClosed {
                        close_reason: None,
                        resolution: None,
                        forced_descendant_ids: Vec::new(),
                    },
                ),
            ],
        },
        BeadEventStreamWire {
            stream_id: "gold-2".to_string(),
            root_issue_id: "gold-2".to_string(),
            events: vec![event(
                "gold-2",
                "2026-01-01T00:03:00Z",
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue: other },
            )],
        },
    ];

    let reduced = reduce_event_streams(&streams).unwrap();

    let root = reduced.iter().find(|issue| issue.id == "gold-1").unwrap();
    assert_eq!(root.status, StatusWire::Closed);
    assert_eq!(root.closed_at.as_deref(), Some("2026-01-01T00:01:00Z"));
}

#[test]
fn reduce_survives_many_merged_streams_with_non_monotonic_timestamps() {
    let timestamp = |seconds: usize| {
        format!(
            "2026-01-01T{:02}:{:02}:{:02}Z",
            seconds / 3600,
            (seconds / 60) % 60,
            seconds % 60
        )
    };
    let streams: Vec<BeadEventStreamWire> = (0..24)
        .map(|index| {
            let id = format!("meld-{index:02}");
            let base = 1000 + (index * 37) % 700;
            let created_at = timestamp(base);
            BeadEventStreamWire {
                stream_id: id.clone(),
                root_issue_id: id.clone(),
                events: vec![
                    event(
                        &id,
                        &created_at,
                        BeadEventOperationWire::IssueCreated,
                        BeadEventPayloadWire::IssueCreated {
                            issue: issue(
                                &id,
                                "Merged",
                                IssueTypeWire::Plan,
                                None,
                                &created_at,
                            ),
                        },
                    ),
                    event(
                        &id,
                        &timestamp(base + 50),
                        BeadEventOperationWire::IssueOpened,
                        BeadEventPayloadWire::IssueOpened,
                    ),
                    event(
                        &id,
                        &timestamp(base + 100),
                        BeadEventOperationWire::ReadyMarked,
                        BeadEventPayloadWire::ReadyMarked,
                    ),
                    event(
                        &id,
                        &timestamp(base + 150),
                        BeadEventOperationWire::ReadyUnmarked,
                        BeadEventPayloadWire::ReadyUnmarked,
                    ),
                    // Merged-in tail whose timestamp predates the whole
                    // stream, as produced by merge_bead_event_streams.
                    event(
                        &id,
                        &timestamp(base - 400),
                        BeadEventOperationWire::IssueClosed,
                        BeadEventPayloadWire::IssueClosed {
                            close_reason: None,
                            resolution: None,
                            forced_descendant_ids: Vec::new(),
                        },
                    ),
                ],
            }
        })
        .collect();

    let reduced = reduce_event_streams(&streams).unwrap();

    assert_eq!(reduced.len(), 24);
    assert!(reduced
        .iter()
        .all(|issue| issue.status == StatusWire::Closed));
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

fn numbered_event(
    issue_id: &str,
    ordinal: usize,
    timestamp: &str,
    operation: BeadEventOperationWire,
    payload: BeadEventPayloadWire,
) -> BeadEventRecordWire {
    let operation_label = serde_json::to_string(&operation)
        .unwrap()
        .trim_matches('"')
        .to_string();
    BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: format!("gold-1:{ordinal:06}:{operation_label}:{issue_id}"),
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

fn is_content_hashed_event_id(event_id: &str) -> bool {
    event_id.rsplit(':').next().is_some_and(|digest| {
        digest.len() == 64
            && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
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

fn _assert_error_send_sync(_: &BeadError) {}
