use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::OptionalExtension;
use sase_core::notifications::{
    pending_action_from_notification, read_notifications_snapshot,
    MobileActionStateWire, NotificationWire,
};
use sase_core::parse_project_bytes;
use sase_core::projection::{
    apply_changespec_event, apply_notification_event,
    changespec_projection_snapshot, changespec_wires,
    notification_projection_snapshot, notification_wires, ChangeSpecEventKind,
    ChangeSpecSnapshotUpsertPayload, ChangeSpecTombstonePayload, EventSource,
    EventSourceKind, HostIdentity, NewProjectionEvent,
    NotificationAppendedPayload, NotificationEventKind,
    NotificationSnapshotRewrittenPayload, NotificationTombstonePayload,
    PendingActionCleanedPayload, PendingActionPayload, ProjectIdentity,
    ProjectionEvent, ProjectionEventType, ProjectionStore,
};
use tempfile::tempdir;

const MYPROJ_SASE: &[u8] = include_bytes!("fixtures/myproj.sase");
const MYPROJ_ARCHIVE_SASE: &[u8] =
    include_bytes!("fixtures/myproj-archive.sase");
const NOTIFICATION_CONTRACT: &str =
    include_str!("fixtures/notifications/store_contract.jsonl");

fn source() -> EventSource {
    EventSource::new(EventSourceKind::Test, "projection-phase1b")
}

fn project() -> ProjectIdentity {
    ProjectIdentity {
        id: "sase-core".to_string(),
        root: Some("/tmp/sase-core".to_string()),
    }
}

fn host() -> HostIdentity {
    HostIdentity {
        id: "host-1".to_string(),
        hostname: Some("devhost".to_string()),
    }
}

fn event(
    event_type: ProjectionEventType,
    payload: serde_json::Value,
    key: &str,
) -> NewProjectionEvent {
    NewProjectionEvent {
        timestamp: "2026-05-13T19:00:00.000Z".to_string(),
        source: source(),
        project: project(),
        host: host(),
        event_type,
        payload,
        idempotency_key: key.to_string(),
        causality: Default::default(),
    }
}

fn append_phase1b(
    store: &mut ProjectionStore,
    event: NewProjectionEvent,
) -> ProjectionEvent {
    store
        .append_event_with_projection(event, |tx, event| {
            apply_changespec_event(tx, event)?;
            apply_notification_event(tx, event)?;
            Ok(())
        })
        .unwrap()
        .event
}

fn replay_phase1b(events: &[ProjectionEvent]) -> ProjectionStore {
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    for event in events {
        append_phase1b(&mut store, new_event_from_existing(event));
    }
    store
}

fn new_event_from_existing(event: &ProjectionEvent) -> NewProjectionEvent {
    NewProjectionEvent {
        timestamp: event.timestamp.clone(),
        source: event.source.clone(),
        project: event.project.clone(),
        host: event.host.clone(),
        event_type: event.event_type.clone(),
        payload: event.payload.clone(),
        idempotency_key: event.idempotency_key.clone(),
        causality: event.causality.clone(),
    }
}

fn notification(id: &str) -> NotificationWire {
    NotificationWire {
        id: id.to_string(),
        timestamp: "2026-05-01T01:02:03+00:00".to_string(),
        sender: "test-sender".to_string(),
        notes: vec![format!("note for {id}")],
        files: Vec::new(),
        action: None,
        action_data: BTreeMap::new(),
        read: false,
        dismissed: false,
        silent: false,
        muted: false,
        snooze_until: None,
    }
}

#[test]
fn projection_migrates_changespec_and_notification_tables() {
    let temp = tempdir().unwrap();
    let store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();

    for table in [
        "changespecs",
        "changespec_edges",
        "changespec_sections",
        "changespec_search_fts",
        "notifications",
        "pending_actions",
        "notification_search_fts",
    ] {
        assert!(table_exists(&store, table), "missing table {table}");
    }
}

#[test]
fn changespec_projection_preserves_parser_wire_identity_and_replays() {
    let active = parse_project_bytes("myproj.sase", MYPROJ_SASE).unwrap();
    let archive =
        parse_project_bytes("myproj-archive.sase", MYPROJ_ARCHIVE_SASE)
            .unwrap();
    let mut expected = active.clone();
    expected.extend(archive.clone());
    expected.sort_by(|left, right| left.name.cmp(&right.name));

    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    let inserted = append_phase1b(
        &mut store,
        event(
            ProjectionEventType::ChangeSpec(
                ChangeSpecEventKind::ParsedSnapshotUpsert,
            ),
            serde_json::to_value(ChangeSpecSnapshotUpsertPayload {
                changespecs: expected.clone(),
            })
            .unwrap(),
            "changespec-snapshot",
        ),
    );

    assert_eq!(changespec_wires(store.connection()).unwrap(), expected);
    let snapshot = changespec_projection_snapshot(store.connection()).unwrap();
    let archived = snapshot
        .changespecs
        .iter()
        .find(|row| row.name == "archived_one")
        .unwrap();
    assert!(archived.archived);
    assert!(snapshot
        .edges
        .iter()
        .any(|row| row.changespec_name == "alpha"
            && row.edge_kind == "cl_or_pr"
            && row.target == "https://example.test/repo/pull/1"));
    assert!(snapshot
        .sections
        .iter()
        .any(|row| row.changespec_name == "alpha"
            && row.section_kind == "commits"));
    assert!(snapshot.search.iter().any(|row| row.name == "alpha"
        && row.summary.contains("Initial feature work")));

    let replayed = replay_phase1b(&[inserted]);
    assert_eq!(
        changespec_projection_snapshot(store.connection()).unwrap(),
        changespec_projection_snapshot(replayed.connection()).unwrap()
    );
}

#[test]
fn changespec_tombstone_removes_rows_sections_edges_and_search() {
    let specs = parse_project_bytes("myproj.sase", MYPROJ_SASE).unwrap();
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    append_phase1b(
        &mut store,
        event(
            ProjectionEventType::ChangeSpec(
                ChangeSpecEventKind::ParsedSnapshotUpsert,
            ),
            serde_json::to_value(ChangeSpecSnapshotUpsertPayload {
                changespecs: specs,
            })
            .unwrap(),
            "changespec-upsert",
        ),
    );
    append_phase1b(
        &mut store,
        event(
            ProjectionEventType::ChangeSpec(ChangeSpecEventKind::Tombstone),
            serde_json::to_value(ChangeSpecTombstonePayload {
                name: "alpha".to_string(),
            })
            .unwrap(),
            "changespec-alpha-tombstone",
        ),
    );

    let snapshot = changespec_projection_snapshot(store.connection()).unwrap();
    assert!(!snapshot.changespecs.iter().any(|row| row.name == "alpha"));
    assert!(!snapshot
        .edges
        .iter()
        .any(|row| row.changespec_name == "alpha"));
    assert!(!snapshot
        .sections
        .iter()
        .any(|row| row.changespec_name == "alpha"));
    assert!(!snapshot.search.iter().any(|row| row.name == "alpha"));
}

#[test]
fn notification_projection_preserves_store_visibility_counts_and_pending_actions(
) {
    let temp = tempdir().unwrap();
    let path = notification_store_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, NOTIFICATION_CONTRACT).unwrap();
    let current = read_notifications_snapshot(&path, false).unwrap();

    let mut plan = notification("plan-action");
    plan.action = Some("PlanApproval".to_string());
    plan.action_data
        .insert("response_dir".to_string(), "/tmp/response".to_string());
    let pending = pending_action_from_notification(&plan, 10.0).unwrap();

    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    append_phase1b(
        &mut store,
        event(
            ProjectionEventType::Notification(
                NotificationEventKind::SnapshotRewritten,
            ),
            serde_json::to_value(NotificationSnapshotRewrittenPayload {
                notifications: current.notifications.clone(),
                pending_actions: vec![pending.clone()],
            })
            .unwrap(),
            "notification-rewrite",
        ),
    );

    let snapshot =
        notification_projection_snapshot(store.connection()).unwrap();
    assert_eq!(snapshot.counts, current.counts);
    assert_eq!(
        notification_wires(store.connection()).unwrap(),
        sorted_notifications(current.notifications)
    );
    assert_eq!(snapshot.pending_actions.len(), 1);
    assert_eq!(snapshot.pending_actions[0].prefix, pending.prefix);
    assert_eq!(
        snapshot.pending_actions[0].state,
        MobileActionStateWire::Available
    );
}

#[test]
fn notification_projection_rewrites_cleans_tombstones_and_replays() {
    let mut first = notification("first");
    first.action = Some("PlanApproval".to_string());
    let pending = pending_action_from_notification(&first, 100.0).unwrap();
    let second = notification("second");

    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    let mut events = Vec::new();
    events.push(append_phase1b(
        &mut store,
        event(
            ProjectionEventType::Notification(
                NotificationEventKind::SnapshotRewritten,
            ),
            serde_json::to_value(NotificationSnapshotRewrittenPayload {
                notifications: vec![first.clone()],
                pending_actions: Vec::new(),
            })
            .unwrap(),
            "notification-initial-rewrite",
        ),
    ));
    events.push(append_phase1b(
        &mut store,
        event(
            ProjectionEventType::Notification(NotificationEventKind::Appended),
            serde_json::to_value(NotificationAppendedPayload {
                notification: second.clone(),
            })
            .unwrap(),
            "notification-append-second",
        ),
    ));
    events.push(append_phase1b(
        &mut store,
        event(
            ProjectionEventType::Notification(
                NotificationEventKind::PendingActionRegistered,
            ),
            serde_json::to_value(PendingActionPayload {
                action: pending.clone(),
            })
            .unwrap(),
            "pending-register",
        ),
    ));
    events.push(append_phase1b(
        &mut store,
        event(
            ProjectionEventType::Notification(
                NotificationEventKind::PendingActionCleaned,
            ),
            serde_json::to_value(PendingActionCleanedPayload {
                prefixes: vec![pending.prefix.clone()],
            })
            .unwrap(),
            "pending-clean",
        ),
    ));
    events.push(append_phase1b(
        &mut store,
        event(
            ProjectionEventType::Notification(NotificationEventKind::Tombstone),
            serde_json::to_value(NotificationTombstonePayload {
                id: "first".to_string(),
            })
            .unwrap(),
            "notification-first-tombstone",
        ),
    ));

    let snapshot =
        notification_projection_snapshot(store.connection()).unwrap();
    assert_eq!(snapshot.notifications.len(), 1);
    assert_eq!(snapshot.notifications[0].id, "second");
    assert!(snapshot.pending_actions.is_empty());
    assert!(!snapshot.search.iter().any(|row| row.id == "first"));

    let replayed = replay_phase1b(&events);
    assert_eq!(
        notification_projection_snapshot(store.connection()).unwrap(),
        notification_projection_snapshot(replayed.connection()).unwrap()
    );
}

#[test]
fn independent_notification_appends_materialize_deterministically() {
    let first = notification("first");
    let second = notification("second");

    let temp_a = tempdir().unwrap();
    let mut store_a =
        ProjectionStore::open(temp_a.path().join("projection.db")).unwrap();
    append_notification_append(&mut store_a, &first, "a-first");
    append_notification_append(&mut store_a, &second, "a-second");

    let temp_b = tempdir().unwrap();
    let mut store_b =
        ProjectionStore::open(temp_b.path().join("projection.db")).unwrap();
    append_notification_append(&mut store_b, &second, "b-second");
    append_notification_append(&mut store_b, &first, "b-first");

    assert_eq!(
        notification_wires(store_a.connection()).unwrap(),
        notification_wires(store_b.connection()).unwrap()
    );
}

fn append_notification_append(
    store: &mut ProjectionStore,
    notification: &NotificationWire,
    key: &str,
) {
    append_phase1b(
        store,
        event(
            ProjectionEventType::Notification(NotificationEventKind::Appended),
            serde_json::to_value(NotificationAppendedPayload {
                notification: notification.clone(),
            })
            .unwrap(),
            key,
        ),
    );
}

fn sorted_notifications(
    mut notifications: Vec<NotificationWire>,
) -> Vec<NotificationWire> {
    notifications.sort_by(|left, right| left.id.cmp(&right.id));
    notifications
}

fn notification_store_path(root: &Path) -> PathBuf {
    root.join("notifications").join("notifications.jsonl")
}

fn table_exists(store: &ProjectionStore, table_name: &str) -> bool {
    store
        .connection()
        .query_row(
            "SELECT name FROM sqlite_master WHERE name = ?1",
            [table_name],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap()
        .is_some()
}
