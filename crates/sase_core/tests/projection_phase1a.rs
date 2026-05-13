use rusqlite::OptionalExtension;
use sase_core::projection::{
    BeadEventKind, CausalityMetadata, EventSource, EventSourceKind,
    HostIdentity, NewProjectionEvent, ProjectIdentity, ProjectionError,
    ProjectionEventType, ProjectionStore, PROJECTION_EVENT_SCHEMA_VERSION,
    PROJECTION_SCHEMA_VERSION,
};
use serde_json::json;
use tempfile::tempdir;

fn test_event(idempotency_key: &str) -> NewProjectionEvent {
    NewProjectionEvent {
        timestamp: "2026-05-13T18:00:00.000Z".to_string(),
        source: EventSource::new(EventSourceKind::Test, "projection-test"),
        project: ProjectIdentity {
            id: "sase-core".to_string(),
            root: Some("/tmp/sase-core".to_string()),
        },
        host: HostIdentity {
            id: "host-1".to_string(),
            hostname: Some("devhost".to_string()),
        },
        event_type: ProjectionEventType::Bead(BeadEventKind::Created),
        payload: json!({
            "id": "sase-1",
            "title": "Projection test"
        }),
        idempotency_key: idempotency_key.to_string(),
        causality: CausalityMetadata {
            parent_event_ids: vec![],
            correlation_id: Some("corr-1".to_string()),
            command_id: Some("cmd-1".to_string()),
        },
    }
}

#[test]
fn projection_new_store_migrates_base_tables_and_metadata() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("nested").join("projection.db");

    let store = ProjectionStore::open(&db_path).unwrap();

    assert_eq!(
        store.current_schema_version().unwrap(),
        PROJECTION_SCHEMA_VERSION
    );
    assert_eq!(store.last_seq().unwrap(), 0);
    assert!(table_exists(&store, "schema_migrations"));
    assert!(table_exists(&store, "event_log"));
    assert!(table_exists(&store, "projection_meta"));
    assert_eq!(
        store
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                [PROJECTION_SCHEMA_VERSION],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(pragma_string(&store, "journal_mode").to_lowercase(), "wal");
    assert_eq!(pragma_i64(&store, "foreign_keys"), 1);
}

#[test]
fn projection_reopen_existing_store_does_not_rewrite_migration_metadata() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("projection.db");

    let first = ProjectionStore::open(&db_path).unwrap();
    let first_applied_at = first
        .migration_applied_at(PROJECTION_SCHEMA_VERSION)
        .unwrap()
        .unwrap();
    drop(first);

    let second = ProjectionStore::open(&db_path).unwrap();
    let second_applied_at = second
        .migration_applied_at(PROJECTION_SCHEMA_VERSION)
        .unwrap()
        .unwrap();

    assert_eq!(
        second.current_schema_version().unwrap(),
        PROJECTION_SCHEMA_VERSION
    );
    assert_eq!(second_applied_at, first_applied_at);
}

#[test]
fn projection_event_json_shape_is_stable() {
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();

    let outcome = store.append_event(test_event("shape-1")).unwrap();
    let value = serde_json::to_value(&outcome.event).unwrap();

    assert_eq!(outcome.seq, 1);
    assert!(outcome.inserted);
    assert_eq!(
        value,
        json!({
            "schema_version": PROJECTION_EVENT_SCHEMA_VERSION,
            "seq": 1,
            "timestamp": "2026-05-13T18:00:00.000Z",
            "source": {"kind": "test", "name": "projection-test"},
            "project": {"id": "sase-core", "root": "/tmp/sase-core"},
            "host": {"id": "host-1", "hostname": "devhost"},
            "event_type": {"family": "bead", "kind": "created"},
            "payload": {
                "id": "sase-1",
                "title": "Projection test"
            },
            "idempotency_key": "shape-1",
            "causality": {
                "correlation_id": "corr-1",
                "command_id": "cmd-1"
            }
        })
    );
}

#[test]
fn projection_duplicate_idempotency_key_reuses_existing_event() {
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();

    let inserted = store.append_event(test_event("same-key")).unwrap();
    let mut duplicate = test_event("same-key");
    duplicate.payload = json!({"different": true});

    let reused = store.append_event(duplicate).unwrap();

    assert!(inserted.inserted);
    assert!(!reused.inserted);
    assert_eq!(reused.seq, inserted.seq);
    assert_eq!(store.event_count().unwrap(), 1);
    assert_eq!(store.last_seq().unwrap(), 1);
    assert_eq!(reused.event.payload, inserted.event.payload);
}

#[test]
fn projection_appended_events_have_monotonic_sequences_and_update_metadata() {
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();

    let first = store.append_event(test_event("seq-1")).unwrap();
    let second = store.append_event(test_event("seq-2")).unwrap();

    assert_eq!(first.seq, 1);
    assert_eq!(second.seq, 2);
    assert!(second.seq > first.seq);
    assert_eq!(store.event_count().unwrap(), 2);
    assert_eq!(store.last_seq().unwrap(), 2);

    let fetched = store.event_by_seq(2).unwrap().unwrap();
    assert_eq!(fetched.idempotency_key, "seq-2");
}

#[test]
fn projection_hook_participates_in_append_transaction() {
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    store
        .connection()
        .execute(
            "CREATE TABLE hook_projection(seq INTEGER PRIMARY KEY, label TEXT)",
            [],
        )
        .unwrap();

    store
        .append_event_with_projection(test_event("hook-1"), |tx, event| {
            tx.execute(
                "INSERT INTO hook_projection(seq, label) VALUES(?1, ?2)",
                (event.seq, event.event_type.family_name()),
            )?;
            Ok(())
        })
        .unwrap();

    let label: String = store
        .connection()
        .query_row(
            "SELECT label FROM hook_projection WHERE seq = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(label, "bead");

    let err = store
        .append_event_with_projection(test_event("hook-fail"), |_tx, _event| {
            Err(ProjectionError::InvalidEvent("test rollback".to_string()))
        })
        .unwrap_err();
    assert!(matches!(err, ProjectionError::InvalidEvent(_)));
    assert_eq!(store.event_count().unwrap(), 1);
    assert_eq!(store.last_seq().unwrap(), 1);
}

fn table_exists(store: &ProjectionStore, table_name: &str) -> bool {
    store
        .connection()
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table_name],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap()
        .is_some()
}

fn pragma_string(store: &ProjectionStore, name: &str) -> String {
    store
        .connection()
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .unwrap()
}

fn pragma_i64(store: &ProjectionStore, name: &str) -> i64 {
    store
        .connection()
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .unwrap()
}
