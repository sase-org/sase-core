use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{
    params, Connection, OptionalExtension, Transaction, TransactionBehavior,
};

use super::event::{
    current_timestamp, NewProjectionEvent, ProjectionEvent,
    PROJECTION_EVENT_SCHEMA_VERSION,
};
use super::schema;
use super::ProjectionError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionStoreConfig {
    pub db_path: PathBuf,
    pub busy_timeout: Duration,
}

impl ProjectionStoreConfig {
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendEventOutcome {
    pub seq: i64,
    pub inserted: bool,
    pub event: ProjectionEvent,
}

pub struct ProjectionStore {
    conn: Connection,
}

impl ProjectionStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ProjectionError> {
        Self::open_with_config(ProjectionStoreConfig::new(path.as_ref()))
    }

    pub fn open_with_config(
        config: ProjectionStoreConfig,
    ) -> Result<Self, ProjectionError> {
        ensure_parent_dir(&config.db_path)?;
        let conn = Connection::open(&config.db_path)?;
        configure_connection(&conn, config.busy_timeout)?;
        schema::migrate(&conn)?;
        Ok(Self { conn })
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn append_event(
        &mut self,
        event: NewProjectionEvent,
    ) -> Result<AppendEventOutcome, ProjectionError> {
        self.append_event_with_projection(event, |_tx, _event| Ok(()))
    }

    pub fn append_event_with_projection<F>(
        &mut self,
        event: NewProjectionEvent,
        apply_projection: F,
    ) -> Result<AppendEventOutcome, ProjectionError>
    where
        F: FnOnce(
            &Transaction<'_>,
            &ProjectionEvent,
        ) -> Result<(), ProjectionError>,
    {
        validate_event(&event)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) =
            event_by_idempotency_key(&tx, &event.idempotency_key)?
        {
            tx.commit()?;
            return Ok(AppendEventOutcome {
                seq: existing.seq,
                inserted: false,
                event: existing,
            });
        }

        let event_type_json = serde_json::to_string(&event.event_type)?;
        let payload_json = serde_json::to_string(&event.payload)?;
        let causality_json = serde_json::to_string(&event.causality)?;
        let inserted_at = current_timestamp();
        tx.execute(
            "INSERT INTO event_log(
                 schema_version,
                 occurred_at,
                 source_kind,
                 source_name,
                 project_id,
                 project_root,
                 host_id,
                 hostname,
                 event_family,
                 event_type_json,
                 payload_json,
                 idempotency_key,
                 causality_json,
                 inserted_at
             )
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                PROJECTION_EVENT_SCHEMA_VERSION,
                event.timestamp,
                event.source.kind.as_str(),
                event.source.name,
                event.project.id,
                event.project.root,
                event.host.id,
                event.host.hostname,
                event.event_type.family_name(),
                event_type_json,
                payload_json,
                event.idempotency_key,
                causality_json,
                inserted_at,
            ],
        )?;
        let seq = tx.last_insert_rowid();
        let inserted = event_by_seq(&tx, seq)?;
        apply_projection(&tx, &inserted)?;
        schema::set_last_seq(&tx, seq)?;
        tx.commit()?;

        Ok(AppendEventOutcome {
            seq,
            inserted: true,
            event: inserted,
        })
    }

    pub fn get_meta(
        &self,
        key: &str,
    ) -> Result<Option<String>, ProjectionError> {
        let value = self
            .conn
            .query_row(
                "SELECT value FROM projection_meta WHERE key = ?1",
                [key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value)
    }

    pub fn current_schema_version(&self) -> Result<u32, ProjectionError> {
        let value = self.get_meta("schema_version")?.ok_or_else(|| {
            ProjectionError::Invariant(
                "missing schema_version metadata".to_string(),
            )
        })?;
        value.parse::<u32>().map_err(|e| {
            ProjectionError::Invariant(format!(
                "invalid schema_version metadata: {e}"
            ))
        })
    }

    pub fn last_seq(&self) -> Result<i64, ProjectionError> {
        let value = self.get_meta("last_seq")?.ok_or_else(|| {
            ProjectionError::Invariant("missing last_seq metadata".to_string())
        })?;
        value.parse::<i64>().map_err(|e| {
            ProjectionError::Invariant(format!(
                "invalid last_seq metadata: {e}"
            ))
        })
    }

    pub fn event_count(&self) -> Result<i64, ProjectionError> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM event_log",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn event_by_seq(
        &self,
        seq: i64,
    ) -> Result<Option<ProjectionEvent>, ProjectionError> {
        event_by_seq_optional(&self.conn, seq)
    }

    pub fn migration_applied_at(
        &self,
        version: u32,
    ) -> Result<Option<String>, ProjectionError> {
        Ok(self
            .conn
            .query_row(
                "SELECT applied_at FROM schema_migrations WHERE version = ?1",
                [version],
                |row| row.get(0),
            )
            .optional()?)
    }
}

fn configure_connection(
    conn: &Connection,
    busy_timeout: Duration,
) -> Result<(), ProjectionError> {
    conn.busy_timeout(busy_timeout)?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        "#,
    )?;
    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<(), ProjectionError> {
    if path == Path::new(":memory:") {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

fn validate_event(event: &NewProjectionEvent) -> Result<(), ProjectionError> {
    if event.idempotency_key.trim().is_empty() {
        return Err(ProjectionError::InvalidEvent(
            "idempotency_key must not be empty".to_string(),
        ));
    }
    if event.project.id.trim().is_empty() {
        return Err(ProjectionError::InvalidEvent(
            "project.id must not be empty".to_string(),
        ));
    }
    if event.host.id.trim().is_empty() {
        return Err(ProjectionError::InvalidEvent(
            "host.id must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn event_by_idempotency_key(
    conn: &Connection,
    key: &str,
) -> Result<Option<ProjectionEvent>, ProjectionError> {
    conn.query_row(
        EVENT_SELECT_BY_IDEMPOTENCY_KEY,
        [key],
        projection_event_from_row,
    )
    .optional()
    .map_err(ProjectionError::from)
}

fn event_by_seq(
    conn: &Connection,
    seq: i64,
) -> Result<ProjectionEvent, ProjectionError> {
    event_by_seq_optional(conn, seq)?.ok_or_else(|| {
        ProjectionError::Invariant(format!("missing inserted event seq {seq}"))
    })
}

fn event_by_seq_optional(
    conn: &Connection,
    seq: i64,
) -> Result<Option<ProjectionEvent>, ProjectionError> {
    conn.query_row(EVENT_SELECT_BY_SEQ, [seq], projection_event_from_row)
        .optional()
        .map_err(ProjectionError::from)
}

fn projection_event_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ProjectionEvent> {
    let event_type_json: String = row.get("event_type_json")?;
    let payload_json: String = row.get("payload_json")?;
    let causality_json: String = row.get("causality_json")?;
    let source_kind_raw: String = row.get("source_kind")?;
    let source_kind = source_kind_raw.parse().map_err(|e: String| {
        rusqlite::Error::FromSqlConversionFailure(
            source_kind_raw.len(),
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        )
    })?;
    Ok(ProjectionEvent {
        schema_version: row.get("schema_version")?,
        seq: row.get("seq")?,
        timestamp: row.get("occurred_at")?,
        source: super::event::EventSource {
            kind: source_kind,
            name: row.get("source_name")?,
        },
        project: super::event::ProjectIdentity {
            id: row.get("project_id")?,
            root: row.get("project_root")?,
        },
        host: super::event::HostIdentity {
            id: row.get("host_id")?,
            hostname: row.get("hostname")?,
        },
        event_type: serde_json::from_str(&event_type_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                event_type_json.len(),
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?,
        payload: serde_json::from_str(&payload_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                payload_json.len(),
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?,
        idempotency_key: row.get("idempotency_key")?,
        causality: serde_json::from_str(&causality_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                causality_json.len(),
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?,
    })
}

const EVENT_SELECT_BY_IDEMPOTENCY_KEY: &str = r#"
SELECT
    seq,
    schema_version,
    occurred_at,
    source_kind,
    source_name,
    project_id,
    project_root,
    host_id,
    hostname,
    event_type_json,
    payload_json,
    idempotency_key,
    causality_json
FROM event_log
WHERE idempotency_key = ?1
"#;

const EVENT_SELECT_BY_SEQ: &str = r#"
SELECT
    seq,
    schema_version,
    occurred_at,
    source_kind,
    source_name,
    project_id,
    project_root,
    host_id,
    hostname,
    event_type_json,
    payload_json,
    idempotency_key,
    causality_json
FROM event_log
WHERE seq = ?1
"#;
