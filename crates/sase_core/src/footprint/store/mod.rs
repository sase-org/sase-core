//! SQLite openers, schema, and mode bits for the Footprint ledger.

use std::fs;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{
    config::DbConfig, Connection, ErrorCode, OpenFlags, OptionalExtension,
    TransactionBehavior,
};

use super::wire::FOOTPRINT_WIRE_SCHEMA_VERSION;
use super::FootprintError;

#[cfg(test)]
mod tests;

/// Default busy timeout for Footprint store operations.
pub const FOOTPRINT_BUSY_DEFAULT: Duration = Duration::from_millis(250);
/// Upper bound on busy timeouts.
pub const FOOTPRINT_BUSY_MAX: Duration = Duration::from_secs(30);

const SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS catalog_revs (
    rev TEXT PRIMARY KEY,
    sase_version TEXT,
    entry_count INTEGER,
    first_seen_day TEXT,
    last_seen_day TEXT
);
CREATE TABLE IF NOT EXISTS features (
    feature_id TEXT PRIMARY KEY,
    ns TEXT,
    area TEXT,
    kind TEXT,
    label TEXT,
    parent TEXT,
    src TEXT,
    loc INTEGER,
    first_in_catalog TEXT,
    last_in_catalog TEXT
);
CREATE TABLE IF NOT EXISTS daily (
    day TEXT NOT NULL,
    feature_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    via TEXT NOT NULL,
    outcome TEXT NOT NULL,
    origin TEXT NOT NULL,
    n INTEGER NOT NULL,
    dur_ms_sum INTEGER NOT NULL,
    dur_ms_max INTEGER NOT NULL,
    PRIMARY KEY (day, feature_id, actor, via, outcome, origin)
);
CREATE TABLE IF NOT EXISTS coverage (
    day TEXT NOT NULL,
    surface TEXT NOT NULL,
    origin TEXT NOT NULL,
    measured INTEGER NOT NULL,
    human_active INTEGER NOT NULL,
    records INTEGER NOT NULL,
    torn INTEGER NOT NULL,
    dropped INTEGER NOT NULL,
    unknown_ids INTEGER NOT NULL,
    incomplete INTEGER NOT NULL,
    PRIMARY KEY (day, surface, origin)
);
CREATE TABLE IF NOT EXISTS fold_checkpoints (
    file TEXT PRIMARY KEY,
    file_id TEXT,
    offset INTEGER NOT NULL,
    complete INTEGER NOT NULL,
    folded_at TEXT
);
CREATE TABLE IF NOT EXISTS reviews (
    feature_id TEXT PRIMARY KEY,
    decision TEXT NOT NULL,
    reason TEXT,
    decided_day TEXT NOT NULL,
    snooze_until TEXT
);
CREATE TABLE IF NOT EXISTS import_packs (
    nonce TEXT PRIMARY KEY,
    week TEXT NOT NULL,
    imported_day TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS imported_weekly (
    nonce TEXT NOT NULL,
    week TEXT NOT NULL,
    feature_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    days INTEGER NOT NULL,
    n_bucket TEXT NOT NULL,
    PRIMARY KEY (nonce, week, feature_id, actor)
);
CREATE TABLE IF NOT EXISTS imported_observed (
    nonce TEXT NOT NULL,
    week TEXT NOT NULL,
    surface TEXT NOT NULL,
    days INTEGER NOT NULL,
    PRIMARY KEY (nonce, week, surface)
);
CREATE INDEX IF NOT EXISTS idx_footprint_daily_day ON daily(day);
CREATE INDEX IF NOT EXISTS idx_footprint_daily_feature
    ON daily(feature_id, day);
CREATE INDEX IF NOT EXISTS idx_footprint_coverage_day ON coverage(day);
"#;

/// Exact column allowlist per table. Guards against identity columns.
pub const SCHEMA_TABLE_COLUMNS: &[(&str, &[&str])] = &[
    ("meta", &["key", "value"]),
    (
        "catalog_revs",
        &[
            "rev",
            "sase_version",
            "entry_count",
            "first_seen_day",
            "last_seen_day",
        ],
    ),
    (
        "features",
        &[
            "feature_id",
            "ns",
            "area",
            "kind",
            "label",
            "parent",
            "src",
            "loc",
            "first_in_catalog",
            "last_in_catalog",
        ],
    ),
    (
        "daily",
        &[
            "day",
            "feature_id",
            "actor",
            "via",
            "outcome",
            "origin",
            "n",
            "dur_ms_sum",
            "dur_ms_max",
        ],
    ),
    (
        "coverage",
        &[
            "day",
            "surface",
            "origin",
            "measured",
            "human_active",
            "records",
            "torn",
            "dropped",
            "unknown_ids",
            "incomplete",
        ],
    ),
    (
        "fold_checkpoints",
        &["file", "file_id", "offset", "complete", "folded_at"],
    ),
    (
        "reviews",
        &[
            "feature_id",
            "decision",
            "reason",
            "decided_day",
            "snooze_until",
        ],
    ),
    ("import_packs", &["nonce", "week", "imported_day"]),
    (
        "imported_weekly",
        &["nonce", "week", "feature_id", "actor", "days", "n_bucket"],
    ),
    ("imported_observed", &["nonce", "week", "surface", "days"]),
];

#[cfg(test)]
const FORBIDDEN_COLUMN_FRAGMENTS: &[&str] = &[
    "pid",
    "host",
    "path",
    "cwd",
    "project",
    "session",
    "agent_name",
    "username",
    "ip",
    "prompt",
    "query",
    "clipboard",
    "remote",
    "branch",
    "repo",
    "artifact",
    "bead",
    "model",
];

/// Bound a caller-supplied busy timeout.
pub fn bounded_busy_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        FOOTPRINT_BUSY_DEFAULT
    } else {
        timeout.min(FOOTPRINT_BUSY_MAX)
    }
}

/// Open a write connection, creating the store and applying schema.
pub fn open_write_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, FootprintError> {
    if let Some(parent) = store_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
            restrict_mode(parent, 0o700)?;
        }
    }
    let conn =
        Connection::open(store_path).map_err(FootprintError::from_sqlite)?;
    restrict_mode(store_path, 0o600)?;
    conn.busy_timeout(busy_timeout)
        .map_err(FootprintError::from_sqlite)?;
    enable_wal_mode(&conn, busy_timeout)?;
    persist_wal_files(&conn);
    skip_checkpoint_on_close(&conn);
    conn.execute_batch("PRAGMA synchronous = NORMAL;")
        .map_err(FootprintError::from_sqlite)?;
    ensure_schema(&conn, true)?;
    Ok(conn)
}

/// Open a read-only connection. Missing files become a validation error
/// that callers map to empty results.
pub fn open_read_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, FootprintError> {
    let conn = Connection::open_with_flags(
        store_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(FootprintError::from_sqlite)?;
    conn.busy_timeout(busy_timeout)
        .map_err(FootprintError::from_sqlite)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(FootprintError::from_sqlite)?;
    ensure_schema(&conn, false)?;
    Ok(conn)
}

/// Run `operation` on a write connection.
pub fn with_write<T>(
    store_path: &Path,
    busy_timeout: Duration,
    operation: impl FnOnce(&mut Connection) -> Result<T, FootprintError>,
) -> Result<T, FootprintError> {
    let mut conn = open_write_store(store_path, busy_timeout)?;
    operation(&mut conn)
}

fn ensure_schema(conn: &Connection, write: bool) -> Result<(), FootprintError> {
    let prior = schema_version(conn)?;
    if let Some(actual) = prior {
        if actual > FOOTPRINT_WIRE_SCHEMA_VERSION {
            return Err(FootprintError::NewerSchema {
                expected: FOOTPRINT_WIRE_SCHEMA_VERSION,
                actual,
            });
        }
    }
    if !write {
        return Ok(());
    }
    conn.execute_batch(SCHEMA_SQL)
        .map_err(FootprintError::from_sqlite)?;
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
        [FOOTPRINT_WIRE_SCHEMA_VERSION.to_string()],
    )
    .map_err(FootprintError::from_sqlite)?;
    if meta_get(conn, "generation")?.is_none() {
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('generation', '0')",
            [],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    Ok(())
}

fn schema_version(conn: &Connection) -> Result<Option<u32>, FootprintError> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    if exists.is_none() {
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    Ok(raw.and_then(|value| value.parse().ok()))
}

pub(crate) fn meta_get(
    conn: &Connection,
    key: &str,
) -> Result<Option<String>, FootprintError> {
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
        row.get(0)
    })
    .optional()
    .map_err(FootprintError::from_sqlite)
}

pub(crate) fn meta_get_u64(
    conn: &Connection,
    key: &str,
) -> Result<u64, FootprintError> {
    Ok(meta_get(conn, key)?
        .and_then(|value| value.parse().ok())
        .unwrap_or(0))
}

pub(crate) fn meta_set(
    conn: &Connection,
    key: &str,
    value: &str,
) -> Result<(), FootprintError> {
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
        rusqlite::params![key, value],
    )
    .map_err(FootprintError::from_sqlite)?;
    Ok(())
}

/// True when `name` is a table in this connection.
pub(crate) fn table_exists(
    conn: &Connection,
    name: &str,
) -> Result<bool, FootprintError> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name],
            |row| row.get(0),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    Ok(exists.is_some())
}

/// Return live column names for `table`, ordered by cid.
pub fn schema_columns(
    conn: &Connection,
    table: &str,
) -> Result<Vec<String>, FootprintError> {
    if !SCHEMA_TABLE_COLUMNS.iter().any(|(name, _)| *name == table) {
        return Err(FootprintError::validation(format!(
            "unknown footprint table {table}"
        )));
    }
    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info(?1) ORDER BY cid")
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map([table], |row| row.get::<_, String>(0))
        .map_err(FootprintError::from_sqlite)?;
    let mut names = Vec::new();
    for name in rows {
        names.push(name.map_err(FootprintError::from_sqlite)?);
    }
    Ok(names)
}

#[cfg(test)]
pub(crate) fn assert_no_forbidden_columns(
    columns: &[String],
) -> Result<(), FootprintError> {
    for column in columns {
        let lowered = column.to_ascii_lowercase();
        for fragment in FORBIDDEN_COLUMN_FRAGMENTS {
            if lowered.contains(fragment) {
                return Err(FootprintError::validation(format!(
                    "forbidden identity column {column}"
                )));
            }
        }
    }
    Ok(())
}

fn enable_wal_mode(
    conn: &Connection,
    busy_timeout: Duration,
) -> Result<(), FootprintError> {
    let started = Instant::now();
    let result = loop {
        let remaining = busy_timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break Err(FootprintError::Busy);
        }
        conn.busy_timeout(remaining)
            .map_err(FootprintError::from_sqlite)?;
        match conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => break Ok(()),
            Ok(mode) => {
                break Err(FootprintError::validation(format!(
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
                    break Err(FootprintError::Busy);
                }
                thread::sleep(Duration::from_millis(5).min(remaining));
            }
            Err(error) => break Err(FootprintError::from_sqlite(error)),
        }
    };
    conn.busy_timeout(busy_timeout)
        .map_err(FootprintError::from_sqlite)?;
    result
}

fn persist_wal_files(conn: &Connection) {
    let mut persist: std::ffi::c_int = 1;
    let _ = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            std::ptr::null(),
            rusqlite::ffi::SQLITE_FCNTL_PERSIST_WAL,
            std::ptr::from_mut(&mut persist).cast(),
        )
    };
}

fn skip_checkpoint_on_close(conn: &Connection) {
    let _ =
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
}

fn restrict_mode(path: &Path, mode: u32) -> Result<(), FootprintError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    let _ = (path, mode);
    Ok(())
}

pub(crate) fn file_id_for(path: &Path) -> Result<String, FootprintError> {
    let meta = fs::metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!("{}:{}", meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        Ok(format!(
            "{}:{}",
            meta.len(),
            meta.modified()?.elapsed().is_ok()
        ))
    }
}

pub(crate) fn begin_immediate(
    conn: &mut Connection,
) -> Result<rusqlite::Transaction<'_>, FootprintError> {
    conn.transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(FootprintError::from_sqlite)
}

pub(crate) fn store_size(store_path: &Path) -> u64 {
    fs::metadata(store_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}
