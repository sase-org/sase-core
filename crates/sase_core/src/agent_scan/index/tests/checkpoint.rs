use super::super::checkpoint::checkpoint_agent_artifact_index_wal_if_oversized;
use super::super::storage::{open_index, sqlite_sidecar_path};
use super::super::*;
use super::support::{artifact, write_json};
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use rusqlite::{params, Connection};
use serde_json::json;
use std::fs;
use tempfile::tempdir;

fn wal_len(index: &std::path::Path) -> u64 {
    fs::metadata(sqlite_sidecar_path(index, "-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

/// Grow the WAL past `min_bytes` with autocheckpoint disabled and
/// return the still-open writer connection.
///
/// The handle must stay alive until the helper runs: SQLite
/// auto-checkpoints the WAL when the last connection closes, which
/// would shrink it back before the helper ever sees it. An idle open
/// connection holds no locks and never blocks a checkpoint.
fn grow_wal(index: &std::path::Path, min_bytes: u64) -> Connection {
    let conn = Connection::open(index).unwrap();
    conn.execute_batch("PRAGMA wal_autocheckpoint = 0;")
        .unwrap();
    let padding = "x".repeat(512);
    let mut rows = 0;
    while wal_len(index) <= min_bytes {
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
            params![format!("stress-{rows}"), format!("{padding}-{rows}")],
        )
        .unwrap();
        rows += 1;
        assert!(rows < 10_000, "WAL did not grow as expected");
    }
    conn
}

#[test]
fn read_write_open_bounds_wal_and_uses_normal_sync() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    let conn = open_index(&index).unwrap();
    let synchronous: i64 = conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    assert_eq!(synchronous, 1, "WAL mode must use NORMAL synchronous");
    let journal_size_limit: i64 = conn
        .query_row("PRAGMA journal_size_limit", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        journal_size_limit,
        AGENT_ARTIFACT_INDEX_WAL_SIZE_LIMIT_BYTES as i64,
    );
    // Reopening a bounded index keeps the bound.
    drop(conn);
    let reopened = open_index(&index).unwrap();
    let journal_size_limit: i64 = reopened
        .query_row("PRAGMA journal_size_limit", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        journal_size_limit,
        AGENT_ARTIFACT_INDEX_WAL_SIZE_LIMIT_BYTES as i64,
    );
}

#[test]
fn checkpoint_skips_missing_wal_without_opening() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    let outcome =
        checkpoint_agent_artifact_index_wal_if_oversized(&index, 1).unwrap();
    assert!(!outcome.checkpoint_attempted);
    assert_eq!(outcome.wal_bytes_before, 0);
    assert_eq!(outcome.wal_bytes_after, 0);
    assert!(
        !index.exists(),
        "a skipped checkpoint must not create the index file"
    );
}

#[test]
fn checkpoint_skips_small_wal() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260521170000");
    write_json(&dir.join("agent_meta.json"), json!({"name": "small"}));
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let outcome = checkpoint_agent_artifact_index_wal_if_oversized(
        &index,
        AGENT_ARTIFACT_INDEX_WAL_SIZE_LIMIT_BYTES,
    )
    .unwrap();
    assert!(!outcome.checkpoint_attempted);
    assert_eq!(outcome.wal_bytes_before, wal_len(&index));
    assert_eq!(outcome.wal_bytes_after, wal_len(&index));
}

#[test]
fn checkpoint_truncates_oversized_wal() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260521170100");
    write_json(&dir.join("agent_meta.json"), json!({"name": "sized"}));
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let _writer = grow_wal(&index, 4096);

    let outcome =
        checkpoint_agent_artifact_index_wal_if_oversized(&index, 1024).unwrap();
    assert!(outcome.checkpoint_attempted);
    assert!(outcome.wal_bytes_before > 1024);
    assert!(
        outcome.wal_bytes_after < outcome.wal_bytes_before,
        "checkpoint must shrink the WAL: {outcome:?}"
    );
    assert!(
        outcome.wal_bytes_after <= 4096,
        "TRUNCATE leaves a near-zero WAL: {outcome:?}"
    );
    assert!(!outcome.checkpoint_busy);
    // Checkpointing never loses indexed rows.
    assert_eq!(
        super::support::count_sql(
            &index,
            "SELECT COUNT(*) FROM agent_artifacts"
        ),
        1
    );
}

#[test]
fn checkpoint_reports_busy_instead_of_failing() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260521170200");
    write_json(&dir.join("agent_meta.json"), json!({"name": "busy"}));
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    // The grower doubles as the blocking reader: it stays open so
    // the WAL survives, then holds a read transaction across the
    // checkpoint so the WAL cannot be reset.
    let holder = grow_wal(&index, 4096);
    holder.execute_batch("BEGIN").unwrap();
    let _probe: i64 = holder
        .query_row("SELECT COUNT(*) FROM agent_artifacts", [], |row| row.get(0))
        .unwrap();

    let outcome =
        checkpoint_agent_artifact_index_wal_if_oversized(&index, 1).unwrap();
    holder.execute_batch("ROLLBACK").unwrap();
    assert!(outcome.checkpoint_attempted);
    assert!(
        outcome.checkpoint_busy,
        "an active reader is a normal busy outcome, not an error: \
         {outcome:?}"
    );

    // Once the reader goes away the same WAL checkpoints cleanly.
    let retried =
        checkpoint_agent_artifact_index_wal_if_oversized(&index, 1).unwrap();
    assert!(retried.checkpoint_attempted);
    assert!(!retried.checkpoint_busy);
}
