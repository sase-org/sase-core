//! Controlled contention schedules for the ToolRun SQLite store.
//!
//! Clean-base measurements (sase-1ou.2.1.1, unfixed store): WAL
//! `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE` never busy a reader (show succeeded
//! in ~1-2 ms). `sqlite3_open_v2` under `PRAGMA locking_mode=EXCLUSIVE`
//! returns BUSY immediately without honoring `busy_timeout` (the show
//! failure). Last-close/reopen show overlap was 40/40 ok, max 2.7 ms.
//! Schema upsert on a current store was 64 µs. Writer vs `BEGIN EXCLUSIVE`
//! exhausted a 25 ms budget in 27 ms.

use super::{begin_named, definition, store};
use crate::tool_run::store::{begin, show_run, store_stats};
use crate::tool_run::wire::{
    ToolRunBeginRequestWire, ToolRunShowRequestWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use crate::tool_run::ToolRunError;
use rusqlite::config::DbConfig;
use rusqlite::{Connection, ErrorCode, OpenFlags};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn show_req(run_id: &str) -> ToolRunShowRequestWire {
    ToolRunShowRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
    }
}

fn begin_req(now: i64) -> ToolRunBeginRequestWire {
    begin_req_with_id(None, now)
}

fn begin_req_with_id(
    run_id: Option<&str>,
    now: i64,
) -> ToolRunBeginRequestWire {
    ToolRunBeginRequestWire {
        starter: None,
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.map(ToOwned::to_owned),
        created_event_id: None,
        running_event_id: None,
        tool_name: Some("check".into()),
        definition: definition(),
        extra_args: Vec::new(),
        display_argv: vec!["just".into(), "check".into()],
        private_argv: None,
        project: Some("sase".into()),
        agent: None,
        workspace: None,
        bead: None,
        owner_kind: None,
        owner_id: None,
        parent_run_id: None,
        wrapper_pid: None,
        boot_id: None,
        process_start_identity: None,
        events_path: None,
        log_stdout_path: None,
        log_stderr_path: None,
        now_ts: Some(now),
        commit_running: true,
        launch_mode: None,
        launch: None,
        owner_log_path: None,
    }
}

fn hold_exclusive(path: &std::path::Path) -> Connection {
    let holder = Connection::open(path).unwrap();
    holder
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    holder
        .execute(
            "UPDATE meta SET value = value WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    holder
}

/// Match production last-close: persist WAL and skip checkpoint-on-close
/// so dropping the last writer does not take an exclusive file lock.
fn production_last_close_writer(path: &std::path::Path) -> Connection {
    let writer = Connection::open(path).unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();
    writer
        .set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
        .unwrap();
    writer
        .execute(
            "UPDATE meta SET value = value WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    writer
}

fn no_quarantine(path: &std::path::Path) {
    let parent = path.parent().unwrap();
    let corrupt: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.ok()?.file_name();
            let name = name.to_string_lossy();
            name.contains(".corrupt-").then(|| name.into_owned())
        })
        .collect();
    assert!(
        corrupt.is_empty(),
        "busy must not quarantine a healthy store: {corrupt:?}"
    );
}

/// WAL reserved (BEGIN IMMEDIATE) must not block a reader. An ordinary
/// writer lock is not an explanation for the read-only `show` failure.
#[test]
fn wal_reserved_writer_coexists_with_readonly_show() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let holder = Connection::open(&path).unwrap();
    holder.pragma_update(None, "journal_mode", "WAL").unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    let result = show_run(
        &path,
        show_req(&started_run.run.run_id),
        Duration::from_millis(250),
    );
    let elapsed = started.elapsed();
    holder.execute_batch("ROLLBACK").unwrap();
    result.expect("WAL reader must coexist with a reserved writer");
    assert!(elapsed < Duration::from_millis(250));
}

/// WAL `BEGIN EXCLUSIVE` is equivalent to IMMEDIATE for readers.
#[test]
fn wal_begin_exclusive_does_not_busy_readonly_show() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let holder = Connection::open(&path).unwrap();
    holder.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let result = show_run(
        &path,
        show_req(&started_run.run.run_id),
        Duration::from_millis(250),
    );
    holder.execute_batch("ROLLBACK").unwrap();
    result.expect("WAL BEGIN EXCLUSIVE must not busy a reader");
}

/// Reproduces the controlled show-failure schedule: a reader statement
/// under `locking_mode=EXCLUSIVE` returns BUSY immediately when no busy
/// timeout is installed (the store boundary must retry with remaining
/// time rather than relying on `sqlite3_busy_timeout` at open).
#[test]
fn readonly_query_under_locking_mode_exclusive_is_immediate_busy() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let holder = hold_exclusive(&path);
    let conn = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("WAL persist-open can succeed; the statement is what busies");
    conn.busy_timeout(Duration::ZERO).unwrap();
    let started = Instant::now();
    let query = conn.query_row(
        "SELECT value FROM meta WHERE key = 'schema_version'",
        [],
        |row| row.get::<_, String>(0),
    );
    let elapsed = started.elapsed();
    drop(holder);
    let err =
        query.expect_err("locking_mode EXCLUSIVE must busy a reader statement");
    match err {
        rusqlite::Error::SqliteFailure(info, _) => {
            assert!(
                matches!(
                    info.code,
                    ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
                ),
                "code={:?} extended={}",
                info.code,
                info.extended_code
            );
        }
        other => panic!("expected sqlite failure, got {other}"),
    }
    assert!(
        elapsed < Duration::from_millis(50),
        "zero busy_timeout must fail immediately, elapsed={elapsed:?}"
    );
}

/// Unreleased exclusive file lock exhausts an explicit short budget.
#[test]
fn unreleased_exclusive_exhausts_explicit_budget() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let holder = hold_exclusive(&path);
    let started = Instant::now();
    let begin_result = begin(&path, begin_req(2), Duration::from_millis(25));
    let show_result = show_run(
        &path,
        show_req(&started_run.run.run_id),
        Duration::from_millis(25),
    );
    let elapsed = started.elapsed();
    drop(holder);
    assert!(matches!(begin_result, Err(ToolRunError::Busy { .. })));
    assert!(matches!(show_result, Err(ToolRunError::Busy { .. })));
    assert!(elapsed < Duration::from_secs(2));
    no_quarantine(&path);
}

/// A releasable exclusive lock succeeds under the production zero-means-default
/// budget: the store retries open until the holder drops.
///
/// The holder thread blocks only on the release channel so drop runs as
/// soon as the reader exists. A sleep on the test thread under a loaded
/// `cargo test` run queue can delay that drop past the 250 ms budget.
#[test]
fn releasable_exclusive_lock_succeeds_under_production_budget() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let run_id = started_run.run.run_id.clone();
    let holder = hold_exclusive(&path);
    let (release_tx, release_rx) = mpsc::channel();
    let holder_thread = thread::spawn(move || {
        let _ = release_rx.recv();
        drop(holder);
    });
    let path_reader = path.clone();
    let handle = thread::spawn(move || {
        let started = Instant::now();
        let result = show_run(&path_reader, show_req(&run_id), Duration::ZERO);
        (result, started.elapsed())
    });
    release_tx.send(()).unwrap();
    let (result, elapsed) = handle.join().unwrap();
    holder_thread.join().unwrap();
    result.expect("show must succeed once the exclusive holder releases");
    assert!(
        elapsed < Duration::from_secs(2),
        "production budget must bound the wait, elapsed={elapsed:?}"
    );
    no_quarantine(&path);
}

/// Retrying a write after a releasable lock must not insert twice.
#[test]
fn retry_after_releasable_lock_does_not_duplicate_run() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let holder = hold_exclusive(&path);
    let (release_tx, release_rx) = mpsc::channel();
    let holder_thread = thread::spawn(move || {
        let _ = release_rx.recv();
        drop(holder);
    });
    let path_writer = path.clone();
    let handle = thread::spawn(move || {
        begin(
            &path_writer,
            begin_req_with_id(Some("retry-run"), 2),
            Duration::ZERO,
        )
    });
    release_tx.send(()).unwrap();
    holder_thread.join().unwrap();
    handle
        .join()
        .unwrap()
        .expect("begin must succeed after the exclusive holder releases");
    let duplicate = begin(
        &path,
        begin_req_with_id(Some("retry-run"), 3),
        Duration::ZERO,
    );
    assert!(matches!(
        duplicate,
        Err(ToolRunError::DuplicateRun { run_id }) if run_id == "retry-run"
    ));
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.run_count, 2);
    no_quarantine(&path);
}

/// Concurrent first-open of a missing store: both writers complete
/// without quarantining the other.
#[test]
fn concurrent_first_open_does_not_quarantine() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("tools").join("runs.sqlite");
    let barrier = Arc::new(Barrier::new(2));
    let path_a = path.clone();
    let path_b = path.clone();
    let barrier_a = barrier.clone();
    let handle = thread::spawn(move || {
        barrier_a.wait();
        begin(&path_a, begin_req(1), Duration::from_secs(2))
    });
    barrier.wait();
    let second = begin(&path_b, begin_req(2), Duration::from_secs(2));
    let first = handle.join().unwrap();
    assert!(
        first.is_ok() && second.is_ok(),
        "both concurrent creators must succeed, first={first:?} second={second:?}"
    );
    no_quarantine(&path);
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.run_count, 2);
}

/// All eligible writers succeed under releasable contention with the
/// production budget (zero → core default).
#[test]
fn concurrent_begins_all_succeed_under_production_budget() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for now in [2_i64, 3] {
        let path = path.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            begin(&path, begin_req(now), Duration::ZERO)
        }));
    }
    barrier.wait();
    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    for result in &results {
        result.as_ref().expect("each concurrent begin must succeed");
    }
    let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
    assert_eq!(stats.run_count, 3);
}

/// Last-connection close overlapping a reader must stay within the
/// production budget. The unfixed store was already 40/40 here; the
/// retry policy keeps that schedule green.
#[test]
fn last_close_reopen_overlap_show() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let run_id = started_run.run.run_id.clone();
    let mut max_elapsed = Duration::ZERO;
    for i in 0..40 {
        let writer = production_last_close_writer(&path);
        let path_reader = path.clone();
        let run_id = run_id.clone();
        let handle = thread::spawn(move || {
            let started = Instant::now();
            let result =
                show_run(&path_reader, show_req(&run_id), Duration::ZERO);
            (result, started.elapsed())
        });
        drop(writer);
        let (result, elapsed) = handle.join().unwrap();
        max_elapsed = max_elapsed.max(elapsed);
        result.unwrap_or_else(|error| {
            panic!("iteration {i} show failed: {error}")
        });
    }
    assert!(
        max_elapsed < Duration::from_secs(2),
        "last-close overlap exceeded the production bound: {max_elapsed:?}"
    );
}

/// Held schema write vs a reader: WAL readers still snapshot; this
/// documents that CREATE-in-transaction is not the show failure.
#[test]
fn uncommitted_schema_write_vs_show() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let holder = Connection::open(&path).unwrap();
    holder
        .execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS tool_run_probe (id INTEGER);",
        )
        .unwrap();
    let result = show_run(
        &path,
        show_req(&started_run.run.run_id),
        Duration::from_millis(250),
    );
    holder.execute_batch("ROLLBACK").unwrap();
    result
        .expect("uncommitted CREATE IF NOT EXISTS must not busy a WAL reader");
}

/// Barrier-ordered lock: exclusive holder first, then show waits, then
/// release. The channel is the oracle, not a timing hope.
#[test]
fn barrier_orders_exclusive_holder_then_show() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let run_id = started_run.run.run_id.clone();
    let (tx, rx) = mpsc::channel();
    let holder_ready = Arc::new(Barrier::new(2));
    let path_holder = path.clone();
    let holder_ready_t = holder_ready.clone();
    let holder_thread = thread::spawn(move || {
        let holder = hold_exclusive(&path_holder);
        holder_ready_t.wait();
        let _ = rx.recv();
        drop(holder);
    });
    holder_ready.wait();
    let path_reader = path.clone();
    let show_thread = thread::spawn(move || {
        show_run(&path_reader, show_req(&run_id), Duration::ZERO)
    });
    tx.send(()).unwrap();
    show_thread
        .join()
        .unwrap()
        .expect("show succeeds after the ordered release");
    holder_thread.join().unwrap();
    no_quarantine(&path);
}
