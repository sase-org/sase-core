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

CREATE TABLE IF NOT EXISTS changespecs (
    name TEXT PRIMARY KEY,
    project_basename TEXT NOT NULL,
    file_path TEXT NOT NULL,
    source_start_line INTEGER NOT NULL,
    source_end_line INTEGER NOT NULL,
    status TEXT NOT NULL,
    parent TEXT,
    cl_or_pr TEXT,
    bug TEXT,
    description TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0,
    wire_json TEXT NOT NULL,
    updated_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_changespecs_project_status
    ON changespecs(project_basename, status);
CREATE INDEX IF NOT EXISTS idx_changespecs_archived
    ON changespecs(archived);

CREATE TABLE IF NOT EXISTS changespec_edges (
    changespec_name TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    target TEXT NOT NULL,
    updated_seq INTEGER NOT NULL,
    PRIMARY KEY(changespec_name, edge_kind, target),
    FOREIGN KEY(changespec_name) REFERENCES changespecs(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_changespec_edges_target
    ON changespec_edges(edge_kind, target);

CREATE TABLE IF NOT EXISTS changespec_sections (
    changespec_name TEXT NOT NULL,
    section_kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    entry_json TEXT NOT NULL,
    updated_seq INTEGER NOT NULL,
    PRIMARY KEY(changespec_name, section_kind, ordinal),
    FOREIGN KEY(changespec_name) REFERENCES changespecs(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_changespec_sections_kind
    ON changespec_sections(section_kind);

CREATE VIRTUAL TABLE IF NOT EXISTS changespec_search_fts
    USING fts5(name UNINDEXED, summary);

CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    sender TEXT NOT NULL,
    action TEXT,
    read INTEGER NOT NULL DEFAULT 0,
    dismissed INTEGER NOT NULL DEFAULT 0,
    silent INTEGER NOT NULL DEFAULT 0,
    muted INTEGER NOT NULL DEFAULT 0,
    snooze_until TEXT,
    wire_json TEXT NOT NULL,
    updated_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_notifications_timestamp
    ON notifications(timestamp);
CREATE INDEX IF NOT EXISTS idx_notifications_visibility
    ON notifications(dismissed, read, silent, muted);

CREATE TABLE IF NOT EXISTS pending_actions (
    prefix TEXT PRIMARY KEY,
    notification_id TEXT NOT NULL,
    action_kind TEXT NOT NULL,
    action TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at_unix REAL NOT NULL,
    updated_at_unix REAL NOT NULL,
    stale_deadline_unix REAL NOT NULL,
    wire_json TEXT NOT NULL,
    updated_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_actions_notification
    ON pending_actions(notification_id);
CREATE INDEX IF NOT EXISTS idx_pending_actions_state
    ON pending_actions(state);

CREATE VIRTUAL TABLE IF NOT EXISTS notification_search_fts
    USING fts5(id UNINDEXED, summary);
"#;
