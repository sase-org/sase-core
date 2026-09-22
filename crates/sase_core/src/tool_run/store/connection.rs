//! SQLite connection lifecycle, schema enforcement, and corruption quarantine.
//!
//! Owns `SCHEMA_SQL`, schema-version gates, read/write connection setup with
//! WAL and file-mode hardening, the `with_*_store` side-effect boundaries,
//! corruption detection and quarantine, and the small time helper shared by
//! the rest of the store tree.

use super::super::wire::{
    TOOL_RUN_DEFAULT_BUSY_TIMEOUT, TOOL_RUN_MAX_BUSY_TIMEOUT,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::{lock_paths, ToolRunError};
use crate::store_lock::{acquire_store_lock, LockMode, StoreLockError};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction,
};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    source TEXT NOT NULL,
    executor TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    tool_name TEXT,
    definition_digest TEXT NOT NULL,
    extra_args_digest TEXT NOT NULL,
    display_argv_json TEXT NOT NULL,
    private_argv_json TEXT,
    project TEXT,
    agent TEXT,
    workspace TEXT,
    bead TEXT,
    owner_kind TEXT,
    owner_id TEXT,
    parent_run_id TEXT,
    created_ts INTEGER NOT NULL,
    running_ts INTEGER,
    settled_ts INTEGER,
    duration_ms INTEGER,
    duration_missing TEXT,
    exit_code INTEGER,
    signal INTEGER,
    interruption_reason TEXT,
    lost_reason TEXT,
    wrapper_pid INTEGER,
    boot_id TEXT,
    process_start_identity TEXT,
    child_pid INTEGER,
    child_pgid INTEGER,
    child_process_start_identity TEXT,
    mutated_input INTEGER,
    fingerprint_before_json TEXT,
    fingerprint_after_json TEXT,
    log_stdout_path TEXT,
    log_stderr_path TEXT,
    events_path TEXT,
    evidence_json TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    FOREIGN KEY (parent_run_id) REFERENCES runs(run_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_ts INTEGER,
    settled_ts INTEGER,
    exit_code INTEGER,
    signal INTEGER,
    diagnostics_json TEXT NOT NULL,
    PRIMARY KEY (run_id, attempt),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE TABLE IF NOT EXISTS stages (
    stage_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    description TEXT NOT NULL,
    started_ts INTEGER,
    finished_ts INTEGER,
    elapsed_ms INTEGER,
    exit_code INTEGER,
    output_bytes INTEGER,
    incomplete INTEGER NOT NULL DEFAULT 1,
    diagnostics_json TEXT NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    observed_ts INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE INDEX IF NOT EXISTS idx_tool_runs_created
    ON runs(created_ts DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_project_tool
    ON runs(project, tool_name, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_state
    ON runs(state, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_events_run
    ON events(run_id, created_ts);
CREATE INDEX IF NOT EXISTS idx_tool_stages_run
    ON stages(run_id, started_ts);
CREATE INDEX IF NOT EXISTS idx_tool_samples_run
    ON samples(run_id, observed_ts);
"#;

pub(super) fn validate_schema(version: u32) -> Result<(), ToolRunError> {
    if version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: version,
        });
    }
    Ok(())
}

/// Add columns that postdate the store file without touching existing rows.
/// Fresh stores already carry them through `SCHEMA_SQL`; a store written by
/// an older binary gains them here on its first write. Reads never migrate:
/// `load_run` selects a `NULL` placeholder when the column is absent, so a
/// read-only open of an unmigrated store keeps working.
pub(super) fn ensure_child_observation_columns(
    conn: &Connection,
) -> Result<(), ToolRunError> {
    let mut names = Vec::new();
    let mut stmt = conn.prepare("PRAGMA table_info(runs)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for name in rows {
        names.push(name?);
    }
    if !names
        .iter()
        .any(|name| name == "child_process_start_identity")
    {
        conn.execute(
            "ALTER TABLE runs ADD COLUMN child_process_start_identity TEXT",
            [],
        )?;
    }
    Ok(())
}

pub(super) fn runs_has_child_observation_column(
    conn: &Connection,
) -> Result<bool, ToolRunError> {
    let mut stmt = conn.prepare("PRAGMA table_info(runs)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for name in rows {
        if name? == "child_process_start_identity" {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

pub(super) fn touch_write_meta(
    tx: &Transaction<'_>,
    now: i64,
) -> Result<(), ToolRunError> {
    tx.execute(
        "INSERT INTO meta(key, value) VALUES ('last_write_ts', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [now.to_string()],
    )?;
    Ok(())
}

fn bounded_busy_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        TOOL_RUN_DEFAULT_BUSY_TIMEOUT
    } else {
        timeout.min(TOOL_RUN_MAX_BUSY_TIMEOUT)
    }
}

pub(super) fn with_write_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&mut Connection) -> Result<T, ToolRunError>,
) -> Result<T, ToolRunError> {
    let timeout = bounded_busy_timeout(busy_timeout);
    match open_write_store(store_path, timeout)
        .and_then(|mut conn| operation(&mut conn))
    {
        Ok(value) => Ok(value),
        Err(error) if should_quarantine(&error) && store_path.exists() => {
            quarantine_corrupt_store(store_path, timeout)?;
            let mut conn = open_write_store(store_path, timeout)?;
            operation(&mut conn)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn with_read_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&Connection) -> Result<T, ToolRunError>,
) -> Result<T, ToolRunError> {
    let timeout = bounded_busy_timeout(busy_timeout);
    let conn = open_read_store(store_path, timeout)?;
    operation(&conn)
}

fn should_quarantine(error: &ToolRunError) -> bool {
    match error {
        ToolRunError::Busy { .. }
        | ToolRunError::ReadOnly { .. }
        | ToolRunError::NewerSchema { .. } => false,
        ToolRunError::Store { message } | ToolRunError::Io { message } => {
            is_sqlite_corruption_error(message)
        }
        _ => false,
    }
}

fn is_sqlite_corruption_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database disk image is malformed")
        || lower.contains("file is not a database")
        || lower.contains("not a database")
        || lower.contains("malformed database schema")
        || lower.contains("unsupported file format")
}

fn quarantine_corrupt_store(
    store_path: &Path,
    timeout: Duration,
) -> Result<(), ToolRunError> {
    let (lock_path, holder_path) = lock_paths(store_path);
    let _lock = acquire_store_lock(
        &lock_path,
        &holder_path,
        LockMode::Exclusive,
        timeout,
        "tool_run_quarantine",
    )
    .map_err(|error| match error {
        StoreLockError::Timeout { .. } => ToolRunError::Busy {
            message: error.to_string(),
        },
        other => ToolRunError::store(other.to_string()),
    })?;
    let quarantined = corrupt_store_quarantine_path(store_path);
    match fs::rename(store_path, &quarantined) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ToolRunError::store(format!(
                "failed to quarantine unusable tool run store {}: {error}",
                store_path.display()
            )));
        }
    }
    for suffix in ["-wal", "-shm"] {
        let source = sqlite_sidecar_path(store_path, suffix);
        let target = sqlite_sidecar_path(&quarantined, suffix);
        match fs::rename(&source, &target) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ToolRunError::store(format!(
                    "failed to quarantine tool run store sidecar {}: {error}",
                    source.display()
                )));
            }
        }
    }
    Ok(())
}

fn corrupt_store_quarantine_path(store_path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = store_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "runs.sqlite".into());
    store_path.with_file_name(format!("{file_name}.corrupt-{nanos}"))
}

fn sqlite_sidecar_path(store_path: &Path, suffix: &str) -> PathBuf {
    let mut raw = store_path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

fn open_write_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, ToolRunError> {
    if let Some(parent) = store_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
            restrict_mode(parent, 0o700)?;
        }
    }
    let conn = Connection::open(store_path)?;
    restrict_mode(store_path, 0o600)?;
    conn.busy_timeout(busy_timeout)?;
    enable_wal_mode(&conn, busy_timeout)?;
    conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
    conn.execute_batch(SCHEMA_SQL)?;
    ensure_child_observation_columns(&conn)?;
    enforce_schema_version(&conn, true)?;
    Ok(conn)
}

fn open_read_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, ToolRunError> {
    let conn = Connection::open_with_flags(
        store_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        if matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::CannotOpen | ErrorCode::ReadOnly)
        ) {
            ToolRunError::ReadOnly {
                message: error.to_string(),
            }
        } else {
            error.into()
        }
    })?;
    conn.busy_timeout(busy_timeout)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    enforce_schema_version(&conn, false)?;
    Ok(conn)
}

fn enforce_schema_version(
    conn: &Connection,
    write: bool,
) -> Result<(), ToolRunError> {
    let prior: Option<u32> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|raw| raw.parse().ok());
    if let Some(version) = prior {
        if version > TOOL_RUN_WIRE_SCHEMA_VERSION {
            return Err(ToolRunError::NewerSchema {
                expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
                actual: version,
            });
        }
    }
    if write {
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [TOOL_RUN_WIRE_SCHEMA_VERSION.to_string()],
        )?;
    }
    Ok(())
}

fn enable_wal_mode(
    conn: &Connection,
    busy_timeout: Duration,
) -> Result<(), ToolRunError> {
    let started = Instant::now();
    let result = loop {
        let remaining = busy_timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break Err(ToolRunError::Busy {
                message: "timed out waiting to enable WAL journal mode"
                    .to_string(),
            });
        }
        conn.busy_timeout(remaining)?;
        match conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => break Ok(()),
            Ok(mode) => {
                break Err(ToolRunError::store(format!(
                    "failed to enable WAL journal mode: SQLite returned {mode:?}"
                )));
            }
            Err(error)
                if matches!(
                    error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                ) =>
            {
                let remaining = busy_timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    break Err(error.into());
                }
                thread::sleep(Duration::from_millis(5).min(remaining));
            }
            Err(error) => break Err(error.into()),
        }
    };
    conn.busy_timeout(busy_timeout)?;
    result
}

fn restrict_mode(path: &Path, mode: u32) -> Result<(), ToolRunError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    let _ = (path, mode);
    Ok(())
}
