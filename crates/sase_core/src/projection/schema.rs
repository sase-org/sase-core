use rusqlite::{params, Connection};

use super::{event::current_timestamp, ProjectionError};

pub const PROJECTION_SCHEMA_VERSION: u32 = 1;

pub(crate) fn migrate(conn: &Connection) -> Result<(), ProjectionError> {
    conn.execute_batch(BASE_SCHEMA_SQL)?;
    record_migration(
        conn,
        PROJECTION_SCHEMA_VERSION,
        "base projection schema",
    )?;
    insert_meta_if_absent(
        conn,
        "schema_version",
        &PROJECTION_SCHEMA_VERSION.to_string(),
    )?;
    insert_meta_if_absent(conn, "last_seq", "0")?;
    Ok(())
}

pub(crate) fn set_last_seq(
    conn: &Connection,
    seq: i64,
) -> Result<(), ProjectionError> {
    let now = current_timestamp();
    conn.execute(
        "INSERT INTO projection_meta(key, value, updated_at)
         VALUES('last_seq', ?1, ?2)
         ON CONFLICT(key) DO UPDATE
         SET value = excluded.value,
             updated_at = excluded.updated_at",
        params![seq.to_string(), now],
    )?;
    Ok(())
}

pub(crate) fn insert_meta_if_absent(
    conn: &Connection,
    key: &str,
    value: &str,
) -> Result<(), ProjectionError> {
    let now = current_timestamp();
    conn.execute(
        "INSERT OR IGNORE INTO projection_meta(key, value, updated_at)
         VALUES(?1, ?2, ?3)",
        params![key, value, now],
    )?;
    Ok(())
}

fn record_migration(
    conn: &Connection,
    version: u32,
    name: &str,
) -> Result<(), ProjectionError> {
    let now = current_timestamp();
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at)
         VALUES(?1, ?2, ?3)",
        params![version, name, now],
    )?;
    Ok(())
}

pub const BASE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_name TEXT NOT NULL,
    project_id TEXT NOT NULL,
    project_root TEXT,
    host_id TEXT NOT NULL,
    hostname TEXT,
    event_family TEXT NOT NULL,
    event_type_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    causality_json TEXT NOT NULL,
    inserted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_log_project_seq
    ON event_log(project_id, seq);
CREATE INDEX IF NOT EXISTS idx_event_log_family_seq
    ON event_log(event_family, seq);
"#;
