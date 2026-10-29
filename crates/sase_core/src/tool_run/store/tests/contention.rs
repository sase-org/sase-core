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
use crate::tool_run::store::connection::with_write_store;
use crate::tool_run::store::{begin, show_run, store_stats};
use crate::tool_run::wire::{
    ToolRunBeginRequestWire, ToolRunShowRequestWire,
    TOOL_RUN_DEFAULT_BUSY_TIMEOUT, TOOL_RUN_MAX_BUSY_TIMEOUT,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use crate::tool_run::ToolRunError;
use rusqlite::config::DbConfig;
use rusqlite::{Connection, ErrorCode, OpenFlags};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Budget for schedules whose oracle is eventual success once contention
/// clears. The releasing thread, and the holder's close-time WAL checkpoint,
/// run on a loaded `cargo test` run queue and can outlast the 250 ms
/// production default there (sase-1p6), turning scheduler delay into a false
/// Busy. `zero_budget_waits_the_core_default_before_busy` pins the default.
const RELEASE_BUDGET: Duration = Duration::from_secs(10);

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
///
/// The holder keeps RESERVED for the whole call, so a reader that needed
/// the lock could only end Busy: success alone is the oracle. Wall-clock
/// time is not, because a loaded run queue can stretch an uncontended show.
#[test]
fn wal_reserved_writer_coexists_with_readonly_show() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let holder = Connection::open(&path).unwrap();
    holder.pragma_update(None, "journal_mode", "WAL").unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let result = show_run(
        &path,
        show_req(&started_run.run.run_id),
        Duration::from_millis(250),
    );
    holder.execute_batch("ROLLBACK").unwrap();
    result.expect("WAL reader must coexist with a reserved writer");
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

/// A zero budget means the core default, not an immediate failure: with an
/// exclusive holder that never releases, the call waits at least the
/// default before Busy and stays under the core cap. Load only lengthens
/// the wait, so the lower bound cannot flake.
#[test]
fn zero_budget_waits_the_core_default_before_busy() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let holder = hold_exclusive(&path);
    let started = Instant::now();
    let result =
        show_run(&path, show_req(&started_run.run.run_id), Duration::ZERO);
    let elapsed = started.elapsed();
    drop(holder);
    assert!(
        matches!(result, Err(ToolRunError::Busy { .. })),
        "an unreleased exclusive holder must end the call Busy"
    );
    assert!(
        elapsed >= TOOL_RUN_DEFAULT_BUSY_TIMEOUT,
        "zero must wait the core default, elapsed={elapsed:?}"
    );
    assert!(
        elapsed < TOOL_RUN_MAX_BUSY_TIMEOUT,
        "zero must not wait the core cap, elapsed={elapsed:?}"
    );
    no_quarantine(&path);
}

/// A releasable exclusive lock succeeds: the store retries open until the
/// holder drops. The holder releases only after the reader thread starts,
/// so the reader meets the lock rather than racing past it.
#[test]
fn releasable_exclusive_lock_succeeds_once_released() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    let run_id = started_run.run.run_id.clone();
    let holder = hold_exclusive(&path);
    let (release_tx, release_rx) = mpsc::channel();
    let holder_thread = thread::spawn(move || {
        let _ = release_rx.recv();
        drop(holder);
    });
    let (reader_started_tx, reader_started_rx) = mpsc::channel();
    let path_reader = path.clone();
    let handle = thread::spawn(move || {
        reader_started_tx.send(()).unwrap();
        show_run(&path_reader, show_req(&run_id), RELEASE_BUDGET)
    });
    reader_started_rx.recv().unwrap();
    release_tx.send(()).unwrap();
    let result = handle.join().unwrap();
    holder_thread.join().unwrap();
    result.expect("show must succeed once the exclusive holder releases");
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
    let (writer_started_tx, writer_started_rx) = mpsc::channel();
    let path_writer = path.clone();
    let handle = thread::spawn(move || {
        writer_started_tx.send(()).unwrap();
        begin(
            &path_writer,
            begin_req_with_id(Some("retry-run"), 2),
            RELEASE_BUDGET,
        )
    });
    writer_started_rx.recv().unwrap();
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

/// All eligible writers succeed under releasable contention. Each writer's
/// transaction is short, but a loaded run queue can preempt one while it
/// holds the write lock, so the oracle uses the release budget.
#[test]
fn concurrent_begins_all_succeed_under_releasable_contention() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for now in [2_i64, 3] {
        let path = path.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            begin(&path, begin_req(now), RELEASE_BUDGET)
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

/// Barrier-ordered lock: exclusive holder first, then show starts, then
/// release. The channels are the oracle, not a timing hope.
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
    let (show_started_tx, show_started_rx) = mpsc::channel();
    let path_reader = path.clone();
    let show_thread = thread::spawn(move || {
        show_started_tx.send(()).unwrap();
        show_run(&path_reader, show_req(&run_id), RELEASE_BUDGET)
    });
    show_started_rx.recv().unwrap();
    tx.send(()).unwrap();
    show_thread
        .join()
        .unwrap()
        .expect("show succeeds after the ordered release");
    holder_thread.join().unwrap();
    no_quarantine(&path);
}

/// The budget bounds contention retries, never the first open: a budget
/// already spent when the call starts still reads an uncontended store.
#[test]
fn spent_budget_still_owes_the_first_open() {
    let (_temp, path) = store();
    let started_run = begin_named(&path, 1);
    show_run(
        &path,
        show_req(&started_run.run.run_id),
        Duration::from_nanos(1),
    )
    .expect("an uncontended store must not be Busy before its first open");
}

/// A quarantine is recovery, not contention. When the corrupt-store path
/// outlasts the budget, the recreated store still gets its open rather
/// than a Busy with no contender. The first operation sleeps past the
/// budget and then reports corruption, so the schedule needs no timing luck.
#[test]
fn slow_quarantine_still_recreates_the_store() {
    let (_temp, path) = store();
    begin_named(&path, 1);
    let budget = Duration::from_millis(25);
    let mut calls = 0u32;
    let result = with_write_store(&path, budget, |_conn| {
        calls += 1;
        if calls == 1 {
            thread::sleep(budget * 4);
            return Err(ToolRunError::store("file is not a database"));
        }
        Ok(calls)
    });
    assert_eq!(
        result.expect("the recreated store must get its open"),
        2,
        "the operation must run once on the old store and once recreated"
    );
    let parent = path.parent().unwrap();
    let quarantined = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| {
            name.contains(".corrupt-")
                && !name.ends_with("-wal")
                && !name.ends_with("-shm")
        })
        .count();
    assert_eq!(quarantined, 1, "exactly one quarantine per recovery");
}
