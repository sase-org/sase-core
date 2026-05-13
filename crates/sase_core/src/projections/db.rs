use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row};

use super::error::ProjectionError;
use super::event::{
    canonical_causality_string, canonical_json_string, canonical_source_string,
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire, PROJECTION_EVENT_SCHEMA_VERSION,
};
use super::migrations::run_migrations;

const BASE_PROJECTION_NAME: &str = "event_log";

pub struct ProjectionDb {
    conn: Connection,
}

impl ProjectionDb {
    pub fn open(path: &Path) -> Result<Self, ProjectionError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        configure_connection(&conn)?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self, ProjectionError> {
        let conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub fn with_immediate_transaction<T, F>(
        &mut self,
        f: F,
    ) -> Result<T, ProjectionError>
    where
        F: FnOnce(&Connection) -> Result<T, ProjectionError>,
    {
        self.conn.execute_batch("BEGIN IMMEDIATE;")?;
        let result = f(&self.conn);
        match result {
            Ok(value) => {
                self.conn.execute_batch("COMMIT;")?;
                Ok(value)
            }
            Err(error) => match self.conn.execute_batch("ROLLBACK;") {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(ProjectionError::Rollback {
                    source: Box::new(error),
                    rollback_error,
                }),
            },
        }
    }

    pub fn append_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| append_event_tx(conn, request))
    }

    pub fn get_event(
        &self,
        seq: i64,
    ) -> Result<Option<EventEnvelopeWire>, ProjectionError> {
        conn_get_event(&self.conn, seq)
    }

    pub fn events_after(
        &self,
        after_seq: i64,
        limit: Option<usize>,
    ) -> Result<Vec<EventEnvelopeWire>, ProjectionError> {
        conn_events_after(&self.conn, after_seq, limit)
    }

    pub fn projection_last_seq(
        &self,
        projection: &str,
    ) -> Result<i64, ProjectionError> {
        projection_last_seq_tx(&self.conn, projection)
    }

    pub fn event_count(&self) -> Result<i64, ProjectionError> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM event_log",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn max_event_seq(&self) -> Result<i64, ProjectionError> {
        max_event_seq_tx(&self.conn)
    }
}

pub(crate) fn append_event_tx(
    conn: &Connection,
    request: EventAppendRequestWire,
) -> Result<EventAppendOutcomeWire, ProjectionError> {
    if let Some(idempotency_key) = request.idempotency_key.as_ref() {
        if let Some(seq) = conn
            .query_row(
                "SELECT seq FROM event_idempotency WHERE idempotency_key = ?1",
                [idempotency_key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            let event = conn_get_event(conn, seq)?.ok_or_else(|| {
                ProjectionError::Invariant(format!(
                    "idempotency key {idempotency_key:?} points to missing event seq {seq}"
                ))
            })?;
            return Ok(EventAppendOutcomeWire {
                schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
                event,
                duplicate: true,
            });
        }
    }

    let seq = conn.query_row(
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM event_log",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let created_at = request.created_at.clone().unwrap_or_else(now_timestamp);
    let envelope = request.into_envelope(seq, created_at);
    insert_event_tx(conn, &envelope)?;
    set_projection_last_seq_tx(conn, BASE_PROJECTION_NAME, seq)?;

    Ok(EventAppendOutcomeWire {
        schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
        event: envelope,
        duplicate: false,
    })
}

pub(crate) fn set_projection_last_seq_tx(
    conn: &Connection,
    projection: &str,
    last_seq: i64,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO projection_meta(projection, last_seq, updated_at)
        VALUES (?1, ?2, CURRENT_TIMESTAMP)
        ON CONFLICT(projection) DO UPDATE SET
            last_seq = excluded.last_seq,
            updated_at = CURRENT_TIMESTAMP
        "#,
        params![projection, last_seq],
    )?;
    Ok(())
}

pub(crate) fn projection_last_seq_tx(
    conn: &Connection,
    projection: &str,
) -> Result<i64, ProjectionError> {
    Ok(conn
        .query_row(
            "SELECT last_seq FROM projection_meta WHERE projection = ?1",
            [projection],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

pub(crate) fn max_event_seq_tx(
    conn: &Connection,
) -> Result<i64, ProjectionError> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(seq), 0) FROM event_log",
        [],
        |row| row.get(0),
    )?)
}

fn configure_connection(conn: &Connection) -> Result<(), ProjectionError> {
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn insert_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    let source_json = canonical_source_string(&event.source)?;
    let payload_json = canonical_json_string(&event.payload)?;
    let causality_json = canonical_causality_string(&event.causality)?;
    conn.execute(
        r#"
        INSERT INTO event_log (
            seq, schema_version, created_at, source_json, host_id, project_id,
            event_type, payload_json, idempotency_key, causality_json,
            source_path, source_revision
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            event.seq,
            event.schema_version,
            event.created_at,
            source_json,
            event.host_id,
            event.project_id,
            event.event_type,
            payload_json,
            event.idempotency_key,
            causality_json,
            event.source_path,
            event.source_revision,
        ],
    )?;
    if let Some(idempotency_key) = event.idempotency_key.as_ref() {
        conn.execute(
            "INSERT INTO event_idempotency(idempotency_key, seq) VALUES (?1, ?2)",
            params![idempotency_key, event.seq],
        )?;
    }
    Ok(())
}

fn conn_get_event(
    conn: &Connection,
    seq: i64,
) -> Result<Option<EventEnvelopeWire>, ProjectionError> {
    Ok(conn
        .query_row(
            r#"
            SELECT seq, schema_version, created_at, source_json, host_id,
                   project_id, event_type, payload_json, idempotency_key,
                   causality_json, source_path, source_revision
            FROM event_log
            WHERE seq = ?1
            "#,
            [seq],
            event_from_row,
        )
        .optional()?)
}

fn conn_events_after(
    conn: &Connection,
    after_seq: i64,
    limit: Option<usize>,
) -> Result<Vec<EventEnvelopeWire>, ProjectionError> {
    let mut sql = r#"
        SELECT seq, schema_version, created_at, source_json, host_id,
               project_id, event_type, payload_json, idempotency_key,
               causality_json, source_path, source_revision
        FROM event_log
        WHERE seq > ?1
        ORDER BY seq ASC
    "#
    .to_string();
    if limit.is_some() {
        sql.push_str(" LIMIT ?2");
    }
    let mut stmt = conn.prepare(&sql)?;
    let rows = if let Some(limit) = limit {
        stmt.query_map(params![after_seq, limit as i64], event_from_row)?
    } else {
        stmt.query_map([after_seq], event_from_row)?
    };
    let mut events = Vec::new();
    for row in rows {
        events.push(row?);
    }
    Ok(events)
}

fn event_from_row(row: &Row<'_>) -> rusqlite::Result<EventEnvelopeWire> {
    let source_json: String = row.get(3)?;
    let payload_json: String = row.get(7)?;
    let causality_json: String = row.get(9)?;
    let source: EventSourceWire =
        serde_json::from_str(&source_json).map_err(json_decode_error)?;
    let payload =
        serde_json::from_str(&payload_json).map_err(json_decode_error)?;
    let causality: Vec<EventCausalityWire> =
        serde_json::from_str(&causality_json).map_err(json_decode_error)?;
    Ok(EventEnvelopeWire {
        seq: row.get(0)?,
        schema_version: row.get(1)?,
        created_at: row.get(2)?,
        source,
        host_id: row.get(4)?,
        project_id: row.get(5)?,
        event_type: row.get(6)?,
        payload,
        idempotency_key: row.get(8)?,
        causality,
        source_path: row.get(10)?,
        source_revision: row.get(11)?,
    })
}

fn json_decode_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(error),
    )
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn request(idempotency_key: Option<&str>) -> EventAppendRequestWire {
        EventAppendRequestWire {
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "projection-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            event_type: "test.event".to_string(),
            payload: json!({"b": 2, "a": 1}),
            idempotency_key: idempotency_key.map(str::to_string),
            causality: vec![],
            source_path: Some("source.jsonl".to_string()),
            source_revision: Some("rev-1".to_string()),
        }
    }

    #[test]
    fn open_creates_parent_directories_and_reopen_is_noop() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("nested").join("projection.sqlite");

        let mut db = ProjectionDb::open(&db_path).unwrap();
        db.append_event(request(None)).unwrap();
        drop(db);

        let db = ProjectionDb::open(&db_path).unwrap();
        assert_eq!(db.event_count().unwrap(), 1);
        assert!(fs::metadata(db_path).unwrap().is_file());
    }

    #[test]
    fn append_allocates_monotonic_sequence_and_updates_meta() {
        let mut db = ProjectionDb::open_in_memory().unwrap();

        let first = db.append_event(request(None)).unwrap();
        let second = db.append_event(request(None)).unwrap();

        assert_eq!(first.event.seq, 1);
        assert_eq!(second.event.seq, 2);
        assert_eq!(db.projection_last_seq(BASE_PROJECTION_NAME).unwrap(), 2);
        assert_eq!(db.event_count().unwrap(), 2);
    }

    #[test]
    fn duplicate_idempotency_key_returns_original_event() {
        let mut db = ProjectionDb::open_in_memory().unwrap();

        let first = db.append_event(request(Some("same-key"))).unwrap();
        let duplicate = db.append_event(request(Some("same-key"))).unwrap();

        assert!(!first.duplicate);
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.event.seq, first.event.seq);
        assert_eq!(db.event_count().unwrap(), 1);
        assert_eq!(db.projection_last_seq(BASE_PROJECTION_NAME).unwrap(), 1);
    }

    #[test]
    fn metadata_cannot_advance_without_event_row() {
        let db = ProjectionDb::open_in_memory().unwrap();
        let error =
            set_projection_last_seq_tx(db.connection(), "test_projection", 1)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("cannot advance beyond event_log"));
        assert_eq!(db.projection_last_seq("test_projection").unwrap(), 0);
    }

    #[test]
    fn failed_transaction_rolls_back_event_and_metadata() {
        let mut db = ProjectionDb::open_in_memory().unwrap();

        let error = db
            .with_immediate_transaction(|conn| {
                append_event_tx(conn, request(None))?;
                Err::<(), ProjectionError>(ProjectionError::Invariant(
                    "forced failure".to_string(),
                ))
            })
            .unwrap_err();

        assert!(error.to_string().contains("forced failure"));
        assert_eq!(db.event_count().unwrap(), 0);
        assert_eq!(db.projection_last_seq(BASE_PROJECTION_NAME).unwrap(), 0);
    }
}
