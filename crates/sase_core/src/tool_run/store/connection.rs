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
use rusqlite::config::DbConfig;
use rusqlite::{
    ffi, Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction,
    TransactionBehavior,
};
use std::ffi::c_void;
use std::fs;
use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::os::raw::c_int;
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
    launch_mode TEXT,
    launch_envelope_json TEXT,
    launcher_json TEXT,
    terminal_cause TEXT,
    settled_by TEXT,
    stop_request_json TEXT,
    starter_json TEXT,
    join_json TEXT,
    staged_join_reservation_json TEXT,
    demand_json TEXT,
    owner_log_path TEXT,
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
CREATE INDEX IF NOT EXISTS idx_tool_runs_agent_created
    ON runs(agent, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_owner_created
    ON runs(owner_kind, owner_id, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_reference
    ON runs(project, tool_name, definition_digest, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_events_run
    ON events(run_id, created_ts);
CREATE INDEX IF NOT EXISTS idx_tool_stages_run
    ON stages(run_id, started_ts);
CREATE INDEX IF NOT EXISTS idx_tool_samples_run
    ON samples(run_id, observed_ts);
CREATE TABLE IF NOT EXISTS tool_triage_items (
    item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    stage_id TEXT, stage_key TEXT NOT NULL,
    extractor TEXT NOT NULL, extractor_version INTEGER NOT NULL,
    signature TEXT NOT NULL, display TEXT NOT NULL,
    locator_paths_json TEXT NOT NULL, occurrences INTEGER NOT NULL,
    created_ts INTEGER NOT NULL,
    class TEXT, touched INTEGER, rule_version INTEGER, knobs_json TEXT,
    evidence_json TEXT, possible_owners_json TEXT, classified_ts INTEGER);
CREATE TABLE IF NOT EXISTS tool_triage_stages (
    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    stage_key TEXT NOT NULL, stage_id TEXT,
    extraction_status TEXT NOT NULL, output_path TEXT, created_ts INTEGER NOT NULL,
    mode TEXT, decision TEXT, reason TEXT, elapsed_ms INTEGER, decided_ts INTEGER,
    PRIMARY KEY (run_id, stage_key));
CREATE TABLE IF NOT EXISTS tool_triage_runs (
    run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    continuation_mode TEXT, recipe_finished_ts INTEGER,
    first_continued_exit_code INTEGER, continuation_extra_ms INTEGER,
    repeat_of_run_id TEXT, triaged_ts INTEGER, created_ts INTEGER NOT NULL,
    diagnostics_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_tool_triage_items_run
    ON tool_triage_items(run_id, stage_key);
CREATE INDEX IF NOT EXISTS idx_tool_triage_items_signature
    ON tool_triage_items(signature, extractor_version);
CREATE TABLE IF NOT EXISTS tool_receipts (
    receipt_id TEXT PRIMARY KEY,
    source_run_id TEXT NOT NULL UNIQUE,
    project TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    extra_args_digest TEXT NOT NULL,
    fingerprint_digest TEXT NOT NULL,
    verdict TEXT NOT NULL,
    signature_refs_json TEXT NOT NULL,
    proof_json TEXT NOT NULL,
    issue_ts INTEGER NOT NULL,
    mint_ts INTEGER NOT NULL,
    expiry_ts INTEGER NOT NULL,
    policy_version INTEGER NOT NULL,
    ttl_seconds INTEGER NOT NULL,
    accept_json TEXT NOT NULL,
    status TEXT NOT NULL,
    superseded_by_run_id TEXT,
    superseded_ts INTEGER,
    explanation TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tool_receipts_one_active
    ON tool_receipts(
        project, tool_name, definition_digest,
        extra_args_digest, fingerprint_digest
    )
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_tool_receipts_lookup
    ON tool_receipts(
        project, tool_name, definition_digest,
        extra_args_digest, fingerprint_digest, mint_ts
    );
"#;

pub(crate) fn validate_schema(version: u32) -> Result<(), ToolRunError> {
    if version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: version,
        });
    }
    Ok(())
}

const REQUIRED_TABLES: &[&str] = &[
    "meta",
    "runs",
    "attempts",
    "events",
    "stages",
    "samples",
    "tool_triage_items",
    "tool_triage_stages",
    "tool_triage_runs",
    "tool_receipts",
];

const CHILD_OBSERVATION_COLUMNS: &[(&str, &str)] = &[
    ("child_process_start_identity", "TEXT"),
    ("launch_mode", "TEXT"),
    ("launch_envelope_json", "TEXT"),
    ("launcher_json", "TEXT"),
    ("terminal_cause", "TEXT"),
    ("settled_by", "TEXT"),
    ("stop_request_json", "TEXT"),
    ("starter_json", "TEXT"),
    ("join_json", "TEXT"),
    ("staged_join_reservation_json", "TEXT"),
    ("demand_json", "TEXT"),
    ("owner_log_path", "TEXT"),
];

/// Add columns that postdate the store file without touching existing rows.
/// Fresh stores already carry them through `SCHEMA_SQL`; a store written by
/// an older binary gains them here on its first write. Reads never migrate:
/// `load_run` selects a `NULL` placeholder when the column is absent, so a
/// read-only open of an unmigrated store keeps working.
pub(super) fn ensure_child_observation_columns(
    conn: &Connection,
) -> Result<(), ToolRunError> {
    let existing = runs_column_set(conn)?;
    for (column, ddl) in CHILD_OBSERVATION_COLUMNS {
        if !existing.contains(*column) {
            conn.execute(
                &format!("ALTER TABLE runs ADD COLUMN {column} {ddl}"),
                [],
            )?;
        }
    }
    Ok(())
}

pub(crate) fn runs_column_set(
    conn: &Connection,
) -> Result<std::collections::HashSet<String>, ToolRunError> {
    let mut stmt = conn.prepare("PRAGMA table_info(runs)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut set = std::collections::HashSet::new();
    for name in rows {
        set.insert(name?);
    }
    Ok(set)
}

pub(crate) fn unix_now() -> i64 {
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

/// One store operation's elapsed-time budget. Open, WAL setup, statement
/// busy-waits, and contention retries all consume this same remaining time.
struct BusyBudget {
    deadline: Instant,
}

/// Keep quarantine's store-lock acquisition short so the outer operation can
/// retry while its original deadline still has time remaining.
const STORE_LOCK_WAIT_SLICE: Duration = Duration::from_millis(25);

/// Limit one sleep in SQLite's busy callback. Checking the wall clock after
/// every sleep bounds overshoot to one scheduler delay, even on hosts where a
/// requested short sleep takes much longer than requested.
const SQLITE_BUSY_SLEEP_SLICE: Duration = Duration::from_millis(1);

fn should_retry_busy_until(
    deadline: Instant,
    mut now: impl FnMut() -> Instant,
    mut sleep: impl FnMut(Duration),
) -> bool {
    let remaining = deadline.saturating_duration_since(now());
    if remaining.is_zero() {
        return false;
    }
    sleep(SQLITE_BUSY_SLEEP_SLICE.min(remaining));
    now() < deadline
}

unsafe extern "C" fn sqlite_busy_until_deadline(
    context: *mut c_void,
    _prior_retries: c_int,
) -> c_int {
    if context.is_null() {
        return 0;
    }
    // SAFETY: BusyConnection owns this boxed Instant until its SQLite
    // connection is closed, and SQLite calls this callback synchronously.
    let deadline = unsafe { *context.cast::<Instant>() };
    c_int::from(should_retry_busy_until(
        deadline,
        Instant::now,
        thread::sleep,
    ))
}

/// A SQLite connection whose busy handler shares the store operation's
/// absolute wall-clock deadline. Keep `connection` first so it is dropped
/// before the boxed callback context.
struct BusyConnection {
    connection: Connection,
    deadline: Box<Instant>,
}

impl BusyConnection {
    fn new(
        connection: Connection,
        deadline: Instant,
    ) -> Result<Self, ToolRunError> {
        let mut busy_connection = Self {
            connection,
            deadline: Box::new(deadline),
        };
        // SAFETY: the callback argument points into the stable Box stored in
        // this wrapper. Drop clears the callback before releasing that Box.
        let result = unsafe {
            ffi::sqlite3_busy_handler(
                busy_connection.connection.handle(),
                Some(sqlite_busy_until_deadline),
                (&mut *busy_connection.deadline as *mut Instant).cast(),
            )
        };
        if result != ffi::SQLITE_OK {
            return Err(ToolRunError::store(format!(
                "failed to install SQLite wall-clock busy handler: {result}"
            )));
        }
        Ok(busy_connection)
    }
}

impl Deref for BusyConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for BusyConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl Drop for BusyConnection {
    fn drop(&mut self) {
        // SAFETY: the connection is live here and no SQLite operation is in
        // progress. Clear the raw callback before its boxed deadline is freed.
        unsafe {
            ffi::sqlite3_busy_handler(
                self.connection.handle(),
                None,
                std::ptr::null_mut(),
            );
        }
    }
}

impl BusyBudget {
    fn new(timeout: Duration) -> Self {
        let total = bounded_busy_timeout(timeout);
        Self {
            deadline: Instant::now() + total,
        }
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    fn exhausted(&self) -> bool {
        self.remaining().is_zero()
    }

    fn lock_wait_slice(&self) -> Duration {
        let remaining = self.remaining();
        if remaining.is_zero() {
            remaining
        } else {
            remaining.min(STORE_LOCK_WAIT_SLICE)
        }
    }
}

fn is_transient_contention(error: &ToolRunError) -> bool {
    matches!(error, ToolRunError::Busy { .. })
}

fn contention_backoff(budget: &BusyBudget, attempt: u32) {
    let remaining = budget.remaining();
    if remaining.is_zero() {
        return;
    }
    let shift = attempt.min(4);
    let slot = Duration::from_millis(1u64 << shift);
    thread::sleep(slot.min(Duration::from_millis(16)).min(remaining));
}

pub(super) fn with_write_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&mut Connection) -> Result<T, ToolRunError>,
) -> Result<T, ToolRunError> {
    let budget = BusyBudget::new(busy_timeout);
    let mut attempt = 0u32;
    let mut last_busy: Option<ToolRunError> = None;
    // The elapsed budget bounds contention retries only. Every call opens at
    // least once, and a quarantine is recovery rather than contention, so the
    // recreated store also gets one open however long the quarantine took.
    let mut attempt_owed = true;
    let mut quarantined = false;
    loop {
        if !attempt_owed && budget.exhausted() {
            return Err(last_busy.unwrap_or_else(|| ToolRunError::Busy {
                message: "timed out waiting for the tool run store".into(),
            }));
        }
        attempt_owed = false;
        match open_write_store(store_path, &budget)
            .and_then(|mut conn| operation(&mut conn))
        {
            Ok(value) => return Ok(value),
            Err(error) if is_transient_contention(&error) => {
                // Replay is safe: callers commit inside `operation`, and a
                // rusqlite `Transaction` rolls back on drop when commit did
                // not succeed. Busy never means a committed effect.
                last_busy = Some(error);
                attempt = attempt.saturating_add(1);
                contention_backoff(&budget, attempt);
            }
            Err(error)
                if should_quarantine(&error)
                    && store_path.exists()
                    && !file_too_new_to_quarantine(store_path) =>
            {
                quarantine_corrupt_store(store_path, budget.lock_wait_slice())?;
                // One owed recreate per call: a store corrupt again after
                // its quarantine falls back to the elapsed budget.
                attempt_owed = !quarantined;
                quarantined = true;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(crate) fn with_read_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&Connection) -> Result<T, ToolRunError>,
) -> Result<T, ToolRunError> {
    let budget = BusyBudget::new(busy_timeout);
    let mut attempt = 0u32;
    let mut last_busy: Option<ToolRunError> = None;
    // As for writes: the budget bounds retries, never the first open.
    let mut attempt_owed = true;
    loop {
        if !attempt_owed && budget.exhausted() {
            return Err(last_busy.unwrap_or_else(|| ToolRunError::Busy {
                message: "timed out waiting for the tool run store".into(),
            }));
        }
        attempt_owed = false;
        match open_read_store(store_path, &budget)
            .and_then(|conn| operation(&conn))
        {
            Ok(value) => return Ok(value),
            Err(error) if is_transient_contention(&error) => {
                last_busy = Some(error);
                attempt = attempt.saturating_add(1);
                contention_backoff(&budget, attempt);
            }
            Err(error) => return Err(error),
        }
    }
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

/// A concurrent first-create can leave a 0-byte file after `sqlite3_open`
/// creates it and before the header is written. Quarantining that file
/// would steal the other opener's in-progress init.
fn file_too_new_to_quarantine(store_path: &Path) -> bool {
    matches!(fs::metadata(store_path), Ok(meta) if meta.len() == 0)
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
    budget: &BusyBudget,
) -> Result<BusyConnection, ToolRunError> {
    if let Some(parent) = store_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
            restrict_mode(parent, 0o700)?;
        }
    }
    let connection = Connection::open(store_path)?;
    restrict_mode(store_path, 0o600)?;
    let mut conn = BusyConnection::new(connection, budget.deadline)?;
    enable_wal_mode(&conn, budget.deadline)?;
    conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    ensure_current_schema(&mut conn, true)?;
    Ok(conn)
}

fn open_read_store(
    store_path: &Path,
    budget: &BusyBudget,
) -> Result<BusyConnection, ToolRunError> {
    let connection = Connection::open_with_flags(
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
    let mut conn = BusyConnection::new(connection, budget.deadline)?;
    configure_wal_persistence(&conn);
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    ensure_current_schema(&mut conn, false)?;
    Ok(conn)
}

fn ensure_current_schema(
    conn: &mut Connection,
    write: bool,
) -> Result<(), ToolRunError> {
    match schema_status(conn)? {
        SchemaStatus::Newer { actual } => Err(ToolRunError::NewerSchema {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual,
        }),
        SchemaStatus::Current => Ok(()),
        SchemaStatus::NeedsInit if !write => Ok(()),
        SchemaStatus::NeedsInit => {
            let tx =
                conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(SCHEMA_SQL)?;
            ensure_child_observation_columns(&tx)?;
            write_schema_version(&tx)?;
            tx.commit()?;
            Ok(())
        }
    }
}

enum SchemaStatus {
    Newer { actual: u32 },
    Current,
    NeedsInit,
}

fn schema_status(conn: &Connection) -> Result<SchemaStatus, ToolRunError> {
    let prior = read_schema_version(conn)?;
    if let Some(version) = prior {
        if version > TOOL_RUN_WIRE_SCHEMA_VERSION {
            return Ok(SchemaStatus::Newer { actual: version });
        }
        if version == TOOL_RUN_WIRE_SCHEMA_VERSION
            && child_observation_columns_present(conn)?
            && required_tables_present(conn)?
        {
            return Ok(SchemaStatus::Current);
        }
    }
    Ok(SchemaStatus::NeedsInit)
}

fn child_observation_columns_present(
    conn: &Connection,
) -> Result<bool, ToolRunError> {
    let existing = runs_column_set(conn)?;
    Ok(CHILD_OBSERVATION_COLUMNS
        .iter()
        .all(|(column, _)| existing.contains(*column)))
}

fn required_tables_present(conn: &Connection) -> Result<bool, ToolRunError> {
    let mut stmt =
        conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut names = std::collections::HashSet::new();
    for name in rows {
        names.insert(name?);
    }
    Ok(REQUIRED_TABLES.iter().all(|table| names.contains(*table)))
}

fn read_schema_version(conn: &Connection) -> Result<Option<u32>, ToolRunError> {
    match conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
    {
        Ok(raw) => Ok(raw.and_then(|raw| raw.parse().ok())),
        Err(error)
            if error
                .to_string()
                .to_ascii_lowercase()
                .contains("no such table") =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn write_schema_version(conn: &Connection) -> Result<(), ToolRunError> {
    conn.execute(
        "INSERT INTO meta(key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [TOOL_RUN_WIRE_SCHEMA_VERSION.to_string()],
    )?;
    Ok(())
}

fn enable_wal_mode(
    conn: &Connection,
    deadline: Instant,
) -> Result<(), ToolRunError> {
    // The connection's handler applies this same deadline to both the initial
    // probe and any subsequent WAL conversion attempt.
    let current: Result<String, _> =
        conn.query_row("PRAGMA journal_mode", [], |row| row.get(0));
    match current {
        Ok(mode) if mode.eq_ignore_ascii_case("wal") => {
            configure_wal_persistence(conn);
            return Ok(());
        }
        Ok(_) => {}
        Err(error)
            if matches!(
                error.sqlite_error_code(),
                Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
            ) =>
        {
            // Fall through to the converting retry loop, which honors remaining
            // time instead of treating this as a hard failure.
        }
        Err(error) => return Err(error.into()),
    }
    loop {
        // Attempt the conversion before honoring the deadline: with no time
        // left the attempt just does not wait, so an uncontended store that
        // a spent budget still owes an open can convert.
        match conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => {
                configure_wal_persistence(conn);
                return Ok(());
            }
            Ok(mode) => {
                return Err(ToolRunError::store(format!(
                    "failed to enable WAL journal mode: SQLite returned {mode:?}"
                )));
            }
            Err(error)
                if matches!(
                    error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                ) =>
            {
                let remaining =
                    deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(ToolRunError::Busy {
                        message: format!(
                            "timed out waiting to enable WAL journal mode: \
                             {error}"
                        ),
                    });
                }
                thread::sleep(Duration::from_millis(5).min(remaining));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Keep WAL/SHM files and skip the exclusive checkpoint that SQLite
/// otherwise runs when the last connection closes. That checkpoint is
/// the last-close lock that busies a concurrent read-only `show`:
/// `sqlite3_open_v2` does not honor `busy_timeout` for it.
fn configure_wal_persistence(conn: &Connection) {
    persist_wal_files(conn);
    skip_checkpoint_on_close(conn);
}

fn persist_wal_files(conn: &Connection) {
    let mut persist: std::ffi::c_int = 1;
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            std::ptr::null(),
            rusqlite::ffi::SQLITE_FCNTL_PERSIST_WAL,
            std::ptr::from_mut(&mut persist).cast(),
        )
    };
    let _ = rc;
}

fn skip_checkpoint_on_close(conn: &Connection) {
    let _ =
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_mode_probe_consumes_conversion_budget() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runs.sqlite");
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA locking_mode = EXCLUSIVE;
                 CREATE TABLE evidence (id INTEGER);",
            )
            .unwrap();
        // The holder owns its lock before the contender starts and retains
        // it throughout both the journal-mode probe and possible conversion.
        let budget = Duration::from_millis(250);
        let started = Instant::now();
        let result = with_write_store(&path, budget, |_| Ok(()));
        let elapsed = started.elapsed();
        drop(holder);

        assert!(matches!(result, Err(ToolRunError::Busy { .. })));
        // Allow 175ms for host scheduling, but reject the former two full
        // 250ms waits. The lock schedule is deterministic, not a startup race.
        assert!(
            elapsed < Duration::from_millis(425),
            "WAL setup renewed an exhausted budget: {elapsed:?}"
        );
    }

    #[test]
    fn busy_handler_stops_after_a_scheduler_oversleep() {
        use std::cell::Cell;

        let start = Instant::now();
        let deadline = start + Duration::from_millis(10);
        let now = Cell::new(start);
        let mut requested_sleeps = Vec::new();

        let retry = should_retry_busy_until(
            deadline,
            || now.get(),
            |requested| {
                requested_sleeps.push(requested);
                // Model a host where a 1ms sleep wakes after the deadline.
                now.set(now.get() + requested + Duration::from_millis(10));
            },
        );

        assert!(!retry, "an overslept busy wait must not renew its budget");
        assert_eq!(requested_sleeps, [SQLITE_BUSY_SLEEP_SLICE]);
    }
}
