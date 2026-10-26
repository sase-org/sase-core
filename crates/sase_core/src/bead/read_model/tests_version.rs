//! Crate-version mismatch recovery: a cache marked with an older
//! `sase_core` version must rebuild once, persist the running version,
//! and then serve warm reads — including when readers race.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use super::freshness::{
    freshness_token, now_ns, READ_MODEL_SWEEP_INTERVAL_SECS,
};
use super::queries::with_fresh_cache_at;
use super::store::{
    cached_store_snapshot_at, crate_version, drop_cache_file,
    ensure_cache_ready_at, ensure_cache_ready_for_mutation_at, open_read_only,
    open_read_write, read_meta, read_model_verify_cache_at,
    rebuild_from_replay, rebuild_read_model_at, rebuild_write_stats, CacheMeta,
    Fault,
};
use crate::bead::events::import_issues_to_event_streams;
use crate::bead::jsonl::{parse_issues_jsonl, write_event_store};
use crate::bead::mutation::append_issue_note;

const OLD_CRATE_VERSION: &str = "0.40.0";

const SEED_ISSUES_JSONL: &str = concat!(
    "{\"id\":\"beads-1\",\"title\":\"Epic\",\"status\":\"open\",\"issue_type\":\"plan\",\"created_at\":\"2026-01-01T00:00:00Z\"}\n",
    "{\"id\":\"beads-1.1\",\"title\":\"Phase\",\"status\":\"open\",\"issue_type\":\"phase\",\"parent_id\":\"beads-1\",\"created_at\":\"2026-01-01T00:01:00Z\"}\n",
);

fn test_store_path(temp: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    let cache_path = temp
        .path()
        .join("cache")
        .join("bead-read-model-test.sqlite");
    (beads_dir, cache_path)
}

fn seed_event_store(beads_dir: &Path) {
    fs::create_dir_all(beads_dir.join("events/streams")).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    let outcome = parse_issues_jsonl(SEED_ISSUES_JSONL);
    assert_eq!(outcome.loaded_rows, 2);
    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
    assert!(!streams.is_empty());
    write_event_store(beads_dir, &streams).unwrap();
}

fn committed_meta(cache_path: &Path) -> CacheMeta {
    let connection =
        open_read_only(cache_path, Duration::from_secs(5)).unwrap();
    let meta = read_meta(&connection).unwrap();
    drop(connection);
    meta
}

fn set_meta(cache_path: &Path, key: &str, value: &str) {
    let connection = open_read_write(cache_path, Duration::from_secs(5))
        .unwrap_or_else(|_| panic!("cache must open to seed {key}"));
    connection
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = ?2",
            rusqlite::params![value, key],
        )
        .unwrap();
    drop(connection);
}

fn seed_old_crate_version(
    temp: &tempfile::TempDir,
) -> (PathBuf, PathBuf, CacheMeta) {
    let (beads_dir, cache_path) = test_store_path(temp);
    seed_event_store(&beads_dir);
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    set_meta(&cache_path, "crate_version", OLD_CRATE_VERSION);
    let meta = committed_meta(&cache_path);
    assert_eq!(meta.crate_version, OLD_CRATE_VERSION);
    assert_ne!(
        crate_version(),
        OLD_CRATE_VERSION,
        "fixture crate version must differ from the running crate"
    );
    (beads_dir, cache_path, meta)
}

fn assert_stored_crate_current(cache_path: &Path) {
    let meta = committed_meta(cache_path);
    assert_eq!(
        meta.crate_version,
        crate_version(),
        "committed crate_version must be the running crate, not the status wire"
    );
}

fn assert_cache_matches_replay(beads_dir: &Path, cache_path: &Path) {
    let report = read_model_verify_cache_at(beads_dir, cache_path);
    assert!(report.compared, "{}", report.reason);
    assert!(report.matched, "{}", report.reason);
}

fn age_sweep_timestamp(cache_path: &Path) {
    let aged = now_ns().saturating_sub(
        (READ_MODEL_SWEEP_INTERVAL_SECS as i64 + 5) * 1_000_000_000,
    );
    set_meta(cache_path, "last_sweep_ns", &aged.to_string());
}

fn expect_ok<T>(result: Result<T, Fault>, what: &str) -> T {
    match result {
        Ok(value) => value,
        Err(Fault::Cache) => panic!("{what}: cache fault"),
        Err(Fault::CacheDiagnostic(reason)) => {
            panic!("{what}: cache diagnostic: {reason}")
        }
        Err(Fault::Store(error)) => {
            panic!("{what}: store error: {}", error.message)
        }
    }
}

#[test]
fn snapshot_read_recovers_old_crate_version() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, before) = seed_old_crate_version(&temp);
    rebuild_write_stats::reset();

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .expect("old-version snapshot read must serve rebuilt rows");
    assert_eq!(served.issues.len(), 2);
    assert_stored_crate_current(&cache_path);
    assert_cache_matches_replay(&beads_dir, &cache_path);
    let after = committed_meta(&cache_path);
    assert!(
        after.outcome_rebuild > before.outcome_rebuild,
        "recovery must commit one rebuild"
    );
    assert!(after.streams_known);
    assert!(!after.token.is_empty());
}

#[test]
fn indexed_read_recovers_old_crate_version() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, _) = seed_old_crate_version(&temp);

    assert!(ensure_cache_ready_at(&beads_dir, &cache_path).unwrap());
    let count = with_fresh_cache_at(&beads_dir, &cache_path, |connection| {
        connection
            .query_row("SELECT COUNT(*) FROM issues", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|error| error.to_string())
    })
    .unwrap()
    .expect("indexed read must use the recovered cache, not a replay fallback");
    assert_eq!(count, 2);
    assert_stored_crate_current(&cache_path);
    assert_cache_matches_replay(&beads_dir, &cache_path);
}

#[test]
fn mutation_admission_recovers_old_crate_version() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, _) = seed_old_crate_version(&temp);

    assert!(
        ensure_cache_ready_for_mutation_at(&beads_dir, &cache_path).unwrap()
    );
    assert_stored_crate_current(&cache_path);
    assert_cache_matches_replay(&beads_dir, &cache_path);
}

#[test]
fn recovered_cache_is_a_hit_on_later_reads_including_aged_sweep() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, _) = seed_old_crate_version(&temp);

    cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    let recovered = committed_meta(&cache_path);
    assert_eq!(recovered.crate_version, crate_version());
    rebuild_write_stats::reset();

    cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    age_sweep_timestamp(&cache_path);
    cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();

    let later = committed_meta(&cache_path);
    assert_eq!(later.crate_version, crate_version());
    assert_eq!(later.generation, recovered.generation);
    assert_eq!(later.content_generation, recovered.content_generation);
    assert_eq!(later.outcome_rebuild, recovered.outcome_rebuild);
    assert_eq!(
        rebuild_write_stats::table_rewrites(),
        0,
        "warm reads and an aged sweep must not rewrite snapshot tables"
    );
    assert_cache_matches_replay(&beads_dir, &cache_path);
}

#[test]
fn rebuild_publishes_crate_version_when_invalidation_fails() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, before) = seed_old_crate_version(&temp);
    rebuild_write_stats::reset();
    rebuild_write_stats::force_next_invalidation_fault();

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .expect(
            "failed invalidation must still rebuild, not serve the old version",
        );
    assert_eq!(served.issues.len(), 2);
    assert_stored_crate_current(&cache_path);
    assert_cache_matches_replay(&beads_dir, &cache_path);
    let after = committed_meta(&cache_path);
    assert!(after.outcome_rebuild > before.outcome_rebuild);
    assert_eq!(after.generation, before.generation + 1);
}

#[test]
fn indexed_read_publishes_crate_version_when_invalidation_fails() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, _) = seed_old_crate_version(&temp);
    rebuild_write_stats::force_next_invalidation_fault();

    assert!(ensure_cache_ready_at(&beads_dir, &cache_path).unwrap());
    let count = with_fresh_cache_at(&beads_dir, &cache_path, |connection| {
        connection
            .query_row("SELECT COUNT(*) FROM issues", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|error| error.to_string())
    })
    .unwrap()
    .expect("indexed read must not fall back after a published recovery");
    assert_eq!(count, 2);
    assert_stored_crate_current(&cache_path);
}

#[test]
fn stale_rebuild_losing_to_invalidation_is_not_a_publication() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, before) = seed_old_crate_version(&temp);
    let token = freshness_token(&beads_dir).unwrap();

    drop_cache_file(&cache_path).unwrap();
    let invalidated = committed_meta(&cache_path);
    assert_eq!(invalidated.generation, before.generation + 1);
    assert_eq!(invalidated.crate_version, crate_version());
    assert!(invalidated.token.is_empty());
    assert!(!invalidated.streams_known);

    rebuild_write_stats::reset();
    let result = rebuild_from_replay(
        &beads_dir,
        &cache_path,
        &token,
        before.generation,
        "stale generation after invalidation",
    );
    assert!(
        matches!(result, Err(Fault::Cache)),
        "invalidated rows must not be returned as a successful publication"
    );
    assert_eq!(
        rebuild_write_stats::table_rewrites(),
        0,
        "a known generation loss must not rewrite snapshot tables"
    );

    let after = committed_meta(&cache_path);
    assert_eq!(after.generation, invalidated.generation);
    assert_eq!(after.token, "");
    assert!(!after.streams_known);

    assert!(ensure_cache_ready_at(&beads_dir, &cache_path).unwrap());
    assert_stored_crate_current(&cache_path);
    assert_cache_matches_replay(&beads_dir, &cache_path);
}

#[test]
fn stale_rebuild_losing_to_a_winner_does_not_overwrite_it() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, before) = seed_old_crate_version(&temp);
    let token = freshness_token(&beads_dir).unwrap();

    let winner = expect_ok(
        rebuild_from_replay(
            &beads_dir,
            &cache_path,
            &token,
            before.generation,
            "winning rebuild",
        ),
        "winning rebuild",
    );
    assert_eq!(winner.issues.len(), 2);
    let winning_meta = committed_meta(&cache_path);
    assert_eq!(winning_meta.crate_version, crate_version());
    assert_eq!(winning_meta.generation, before.generation + 1);

    rebuild_write_stats::reset();
    let loser = rebuild_from_replay(
        &beads_dir,
        &cache_path,
        &token,
        before.generation,
        "losing rebuild",
    );
    match loser {
        Ok(snapshot) => assert_eq!(
            serde_json::to_value(&snapshot.issues).unwrap(),
            serde_json::to_value(&winner.issues).unwrap()
        ),
        Err(Fault::Cache) => {}
        Err(Fault::CacheDiagnostic(reason)) => {
            panic!("loser cache diagnostic: {reason}")
        }
        Err(Fault::Store(error)) => {
            panic!("loser must not surface a store error: {}", error.message)
        }
    }
    assert_eq!(
        rebuild_write_stats::table_rewrites(),
        0,
        "a known generation loss must not rewrite snapshot tables"
    );

    let after = committed_meta(&cache_path);
    assert_eq!(after.generation, winning_meta.generation);
    assert_eq!(after.content_generation, winning_meta.content_generation);
    assert_eq!(after.crate_version, crate_version());
    assert_eq!(after.token, winning_meta.token);
    assert_cache_matches_replay(&beads_dir, &cache_path);
}

#[test]
fn racing_readers_converge_on_one_published_version() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, _) = seed_old_crate_version(&temp);
    let beads_dir = Arc::new(beads_dir);
    let cache_path = Arc::new(cache_path);
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let beads_dir = Arc::clone(&beads_dir);
        let cache_path = Arc::clone(&cache_path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            cached_store_snapshot_at(&beads_dir, &cache_path)
        }));
    }
    barrier.wait();
    for handle in handles {
        let result = handle.join().expect("reader thread must not panic");
        if let Ok(Some(snapshot)) = result {
            assert_eq!(snapshot.issues.len(), 2);
        }
    }

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .expect("post-race read must serve the published cache");
    assert_eq!(served.issues.len(), 2);
    assert_stored_crate_current(&cache_path);
    let published = committed_meta(&cache_path);
    rebuild_write_stats::reset();
    cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    let later = committed_meta(&cache_path);
    assert_eq!(later.generation, published.generation);
    assert_eq!(later.content_generation, published.content_generation);
    assert_eq!(later.outcome_rebuild, published.outcome_rebuild);
    assert_eq!(rebuild_write_stats::table_rewrites(), 0);
    assert_cache_matches_replay(&beads_dir, &cache_path);
}

#[test]
fn held_connection_stays_readable_across_version_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path, _) = seed_old_crate_version(&temp);
    let held = open_read_only(&cache_path, Duration::from_secs(5)).unwrap();
    let held_rows: i64 = held
        .query_row("SELECT COUNT(*) FROM issues", [], |row| row.get(0))
        .unwrap();
    assert_eq!(held_rows, 2);

    cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();

    assert!(cache_path.is_file());
    let reread: i64 = held
        .query_row("SELECT COUNT(*) FROM issues", [], |row| row.get(0))
        .unwrap();
    assert_eq!(reread, 2);
    drop(held);

    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A note appended after version recovery.",
        Some("read-model-version-test".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    let phase = served
        .issues
        .iter()
        .find(|issue| issue.id == "beads-1.1")
        .unwrap();
    assert_eq!(phase.notes.len(), 1);
    assert_stored_crate_current(&cache_path);
    assert_cache_matches_replay(&beads_dir, &cache_path);
}
