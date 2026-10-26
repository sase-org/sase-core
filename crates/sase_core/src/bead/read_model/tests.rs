//! Unit tests for the read-model substrate: location, freshness, and
//! rebuild/serve semantics on small synthetic event stores.

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::location::read_model_cache_path_for_store;
use super::store::{
    cached_store_snapshot_at, read_model_status_at, read_model_verify_cache_at,
    rebuild_read_model_at, READ_MODEL_REDUCER_VERSION,
    READ_MODEL_SCHEMA_VERSION,
};
use crate::bead::events::import_issues_to_event_streams;
use crate::bead::jsonl::{parse_issues_jsonl, write_event_store};
use crate::bead::mutation::{
    append_issue_note, close_issues, create_issue, remove_issue,
    remove_issue_note,
};
use crate::bead::{BeadCreateRequestWire, IssueTypeWire, PhaseSizeWire};

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

fn test_git_store_path(temp: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
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

/// Reproduce athena's orphaned `streams_known` entry in the `meta` primary
/// key index, preserving the old root page over a valid table update.
fn corrupt_meta_primary_key_index(
    cache_path: &Path,
    crate_version: &str,
    stale_token: bool,
) {
    let connection = rusqlite::Connection::open(cache_path).unwrap();
    connection
        .execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;",
        )
        .unwrap();
    let page_size: u32 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    let root_page: u32 = connection
        .query_row(
            "SELECT rootpage FROM sqlite_schema WHERE name = 'sqlite_autoindex_meta_1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let page_offset = u64::from(root_page - 1) * u64::from(page_size);
    let bytes = fs::read(cache_path).unwrap();
    let saved_page = bytes
        [page_offset as usize..(page_offset + u64::from(page_size)) as usize]
        .to_vec();

    connection
        .execute("DELETE FROM meta WHERE key = 'streams_known'", [])
        .unwrap();
    connection
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = 'crate_version'",
            [crate_version],
        )
        .unwrap();
    if stale_token {
        connection
            .execute(
                "UPDATE meta SET value = 'stale-token' WHERE key = 'token'",
                [],
            )
            .unwrap();
    }
    drop(connection);

    let mut file = OpenOptions::new().write(true).open(cache_path).unwrap();
    file.seek(SeekFrom::Start(page_offset)).unwrap();
    file.write_all(&saved_page).unwrap();
    file.sync_all().unwrap();
}

fn integrity_check(cache_path: &Path) -> String {
    rusqlite::Connection::open(cache_path)
        .unwrap()
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn corrupt_meta_index_is_reindexed_and_rebuilt_once() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_git_store_path(&temp);
    seed_event_store(&beads_dir);
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let before = read_model_status_at(&beads_dir, &cache_path);
    assert!(before.fresh, "{}", before.reason);

    corrupt_meta_primary_key_index(&cache_path, "0.40.0", false);
    let corrupted = integrity_check(&cache_path);
    assert!(
        corrupted
            .contains("wrong # of entries in index sqlite_autoindex_meta_1"),
        "expected athena's meta index corruption, got {corrupted}"
    );

    let served = crate::bead::read::read_store_issues(&beads_dir).unwrap();
    assert_eq!(served.len(), 2);
    assert_eq!(integrity_check(&cache_path), "ok");
    let after = read_model_status_at(&beads_dir, &cache_path);
    assert!(after.fresh, "{}", after.reason);
    assert_eq!(after.crate_version, super::store::crate_version());
    assert_eq!(after.rebuild_count, before.rebuild_count + 1);
    let generation = after.generation;

    assert_eq!(
        crate::bead::read::read_store_issues(&beads_dir).unwrap(),
        served
    );
    let second = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(second.generation, generation);
    assert_eq!(second.rebuild_count, after.rebuild_count);
}

#[test]
fn corrupt_meta_index_with_matching_version_repairs_stale_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_git_store_path(&temp);
    seed_event_store(&beads_dir);
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let before = read_model_status_at(&beads_dir, &cache_path);

    corrupt_meta_primary_key_index(
        &cache_path,
        &super::store::crate_version(),
        true,
    );
    let corrupted = integrity_check(&cache_path);
    assert!(corrupted.contains("wrong # of entries in index"));

    let served = crate::bead::read::read_store_issues(&beads_dir).unwrap();
    assert_eq!(served.len(), 2);
    assert_eq!(integrity_check(&cache_path), "ok");
    let after = read_model_status_at(&beads_dir, &cache_path);
    assert!(after.fresh, "{}", after.reason);
    assert_eq!(after.rebuild_count, before.rebuild_count + 1);
    assert_eq!(
        crate::bead::read::read_store_issues(&beads_dir).unwrap(),
        served
    );
}

#[test]
fn newer_cache_version_replays_without_modifying_the_cache() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_git_store_path(&temp);
    seed_event_store(&beads_dir);
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let expected =
        crate::bead::read::read_event_store_issues(&beads_dir).unwrap();

    let mut newer =
        semver::Version::parse(&super::store::crate_version()).unwrap();
    newer.patch += 1;
    let connection = rusqlite::Connection::open(&cache_path).unwrap();
    connection
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = 'crate_version'",
            [newer.to_string()],
        )
        .unwrap();
    drop(connection);
    let before_bytes = fs::read(&cache_path).unwrap();
    let before_modified =
        fs::metadata(&cache_path).unwrap().modified().unwrap();

    let actual = crate::bead::read::read_store_issues(&beads_dir).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(fs::read(&cache_path).unwrap(), before_bytes);
    assert_eq!(
        fs::metadata(&cache_path).unwrap().modified().unwrap(),
        before_modified
    );
}

#[test]
fn failed_rebuild_backs_off_for_the_process_window() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_git_store_path(&temp);
    seed_event_store(&beads_dir);
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let expected =
        crate::bead::read::read_event_store_issues(&beads_dir).unwrap();
    super::store::inject_persistent_rebuild_write_failure(&cache_path);

    assert!(rebuild_read_model_at(&beads_dir, &cache_path)
        .unwrap()
        .is_none());
    let after_failure = fs::read(&cache_path).unwrap();
    assert!(rebuild_read_model_at(&beads_dir, &cache_path)
        .unwrap()
        .is_none());
    assert_eq!(
        super::store::injected_rebuild_write_attempts(&cache_path),
        1
    );
    assert_eq!(fs::read(&cache_path).unwrap(), after_failure);
    assert_eq!(
        crate::bead::read::read_store_issues(&beads_dir).unwrap(),
        expected
    );
}

#[test]
fn rebuild_and_serve_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    let snapshot = rebuild_read_model_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.issues.len(), 2);
    assert!(cache_path.is_file());

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&served.issues).unwrap(),
        serde_json::to_value(&snapshot.issues).unwrap()
    );
    assert_eq!(served.provenance, snapshot.provenance);
}

#[test]
fn second_read_serves_without_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let before = read_model_status_at(&beads_dir, &cache_path);
    assert!(before.fresh, "{}", before.reason);
    let generation = before.generation;

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(served.issues.len(), 2);
    let after = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(after.generation, generation);
    assert!(after.fresh, "{}", after.reason);
}

#[test]
fn mutation_invalidates_and_rebuilds() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A note appended after the cached read.",
        Some("read-model-test".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(served.issues.len(), 2);
    let phase = served
        .issues
        .iter()
        .find(|issue| issue.id == "beads-1.1")
        .unwrap();
    assert_eq!(phase.notes.len(), 1);
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.issues, 2);
}

#[test]
fn verify_cache_matches_after_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    let missing = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(!missing.compared);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let report = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(report.compared, "{}", report.reason);
    assert!(report.matched, "{}", report.reason);
    assert_eq!(report.replay_issues, 2);
    assert_eq!(report.cache_issues, 2);
}

#[test]
fn legacy_store_has_no_cache() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "{}\n").unwrap();

    assert!(rebuild_read_model_at(&beads_dir, &cache_path)
        .unwrap()
        .is_none());
    assert!(cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .is_none());
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(!status.fresh);
}

#[test]
fn cache_path_lives_under_the_git_dir() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();

    let path = read_model_cache_path_for_store(&beads_dir).unwrap();
    let canonical_root = fs::canonicalize(temp.path()).unwrap();
    assert!(path.starts_with(canonical_root.join(".git")));
    assert!(path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .starts_with("bead-read-model-"));
    assert_ne!(
        path.file_name().and_then(|name| name.to_str()),
        Some("beads.db")
    );
}

#[test]
fn versions_are_current() {
    // Schema 2 adds the `task_type`, `plus_one`, and `is_flag` issue
    // columns behind the `read-model-queries` indexed filters; schema 3
    // adds disposable `alloc_top`/`alloc_child` allocation metadata for
    // index-only ID minting. The reducer semantics did not change.
    assert_eq!(READ_MODEL_SCHEMA_VERSION, 3);
    assert_eq!(READ_MODEL_REDUCER_VERSION, 2);
}

#[test]
fn tail_applies_appended_note_without_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let before = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(before.generation, 1);

    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A note appended after the cached read.",
        Some("read-model-tail-test".to_string()),
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
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    // The incremental path commits without bumping the generation the
    // way a full rebuild would: content changed, generation did not.
    assert_eq!(status.generation, 1);
    assert_eq!(status.tail_count, 1);
    assert_eq!(status.rebuild_count, 1);
    assert_eq!(status.last_refresh, "tail");
    assert!(
        status.last_refresh_reason.contains("tail events"),
        "{}",
        status.last_refresh_reason
    );
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn tail_applies_new_issue_stream() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "A task created after the cached read".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("feature".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(served.issues.len(), 3);
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.generation, 1);
    assert_eq!(status.tail_count, 1);
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn tail_applies_removals() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A note to retract.",
        Some("read-model-tail-test".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    // First read takes the tail so the retraction below resumes from it.
    cached_store_snapshot_at(&beads_dir, &cache_path).unwrap();
    let note_id = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap()
        .issues
        .iter()
        .find(|issue| issue.id == "beads-1.1")
        .unwrap()
        .notes
        .first()
        .unwrap()
        .id
        .clone();
    remove_issue_note(
        &beads_dir,
        "beads-1.1",
        &note_id,
        Some("read-model-tail-test".to_string()),
        Some("2026-01-03T00:00:00Z".to_string()),
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
    assert!(phase.notes.is_empty());
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(status.generation, 1);
    assert_eq!(status.tail_count, 2);
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);

    remove_issue(&beads_dir, "beads-1.1").unwrap();
    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(served.issues.len(), 1);
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(status.generation, 1);
    assert_eq!(status.tail_count, 3);
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn tail_applies_write_to_closed_bead() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    close_issues(
        &beads_dir,
        &["beads-1.1".to_string()],
        Some("closing before the tail test".to_string()),
        None,
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();
    cached_store_snapshot_at(&beads_dir, &cache_path).unwrap();
    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A note on a closed bead.",
        Some("read-model-tail-test".to_string()),
        Some("2026-01-03T00:00:00Z".to_string()),
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
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(status.generation, 1);
    assert_eq!(status.tail_count, 2);
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn backdated_append_falls_back_to_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    // A second machine's clock-skewed event sorts before the merge
    // frontier, so the tail refuses it and the full replay heals.
    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A clock-skewed note from another machine.",
        Some("read-model-tail-test".to_string()),
        Some("2020-06-01T00:00:00Z".to_string()),
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
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.generation, 2);
    assert_eq!(status.tail_count, 0);
    assert_eq!(status.rebuild_count, 2);
    assert_eq!(status.last_refresh, "rebuild");
    assert!(
        status.last_refresh_reason.contains("merge frontier"),
        "{}",
        status.last_refresh_reason
    );
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn rewritten_stream_falls_back_to_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    // A relocation-shaped rewrite: history bytes change mid-stream, so
    // the append-prefix proof fails. Rename into place so the freshness
    // token sees the change, exactly as real writers do.
    let stream_path = beads_dir.join("events/streams/beads-1.jsonl");
    let bytes = fs::read(&stream_path).unwrap();
    let mut text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("Phase"));
    text = text.replacen("Phase", "PhaseX", 1);
    let tmp_path = stream_path.with_extension("jsonl.tmp");
    fs::write(&tmp_path, text.as_bytes()).unwrap();
    fs::rename(&tmp_path, &stream_path).unwrap();

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    let phase = served
        .issues
        .iter()
        .find(|issue| issue.id == "beads-1.1")
        .unwrap();
    assert_eq!(phase.title, "PhaseX");
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.generation, 2);
    assert_eq!(status.tail_count, 0);
    assert_eq!(status.last_refresh, "rebuild");
    assert!(
        status.last_refresh_reason.contains("rewritten"),
        "{}",
        status.last_refresh_reason
    );
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn config_change_falls_back_to_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    // A whitespace-only rewrite keeps the tail open (covered by the
    // note test's mutation, which materializes config defaults); a real
    // field change falls back.
    let config_path = beads_dir.join("config.json");
    fs::write(
        &config_path,
        b"{\"issue_prefix\":\"\",\"next_counter\":1,\"owner\":\"tail-test\"}\n",
    )
    .unwrap();

    let served = cached_store_snapshot_at(&beads_dir, &cache_path)
        .unwrap()
        .unwrap();
    assert_eq!(served.issues.len(), 2);
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.generation, 2);
    assert_eq!(status.tail_count, 0);
    assert_eq!(status.last_refresh, "rebuild");
    assert!(
        status.last_refresh_reason.contains("config changed"),
        "{}",
        status.last_refresh_reason
    );
}

#[test]
fn warm_reads_count_serves_without_new_generations() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    for _ in 0..3 {
        cached_store_snapshot_at(&beads_dir, &cache_path).unwrap();
    }
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.generation, 1);
    assert_eq!(status.serve_count, 3);
    assert_eq!(status.tail_count, 0);
    assert_eq!(status.rebuild_count, 1);
}

#[test]
fn invalidation_never_unlinks_a_live_cache() {
    use std::time::Duration;
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();
    let before = read_model_status_at(&beads_dir, &cache_path);
    assert_eq!(before.generation, 1);

    // A read-only connection held open across the invalidation, like a
    // concurrent reader's: unlinking the file (or its WAL companions)
    // under it is what crashed readers with `no such table` and SIGBUS.
    let held =
        super::store::open_read_only(&cache_path, Duration::from_secs(5))
            .unwrap();
    let held_rows: i64 = held
        .query_row("SELECT COUNT(*) FROM issues", [], |row| row.get(0))
        .unwrap();
    assert_eq!(held_rows, 2);

    super::store::drop_cache_file(&cache_path).unwrap();

    // The file was never unlinked, so the held connection still reads
    // the intact rows instead of failing.
    assert!(cache_path.is_file());
    let reread: i64 = held
        .query_row("SELECT COUNT(*) FROM issues", [], |row| row.get(0))
        .unwrap();
    assert_eq!(reread, 2);
    drop(held);

    // The generation moved monotonically so any in-flight writer's
    // compare-and-swap loses rather than committing stale rows.
    let reopened =
        super::store::open_read_only(&cache_path, Duration::from_secs(5))
            .unwrap();
    let meta = super::store::read_meta(&reopened).unwrap();
    assert_eq!(meta.generation, before.generation + 1);
    drop(reopened);

    // The next freshness pass rebuilds instead of serving, and the
    // cache verifies against a full replay.
    assert!(
        super::store::ensure_cache_ready_at(&beads_dir, &cache_path).unwrap()
    );
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn schemaless_cache_file_heals_on_next_read() {
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    // A valid-but-schema-less SQLite file, like the implicit
    // `Connection::open` creation used to publish before any schema
    // existed: concurrent openers could observe it and fault forever.
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    rusqlite::Connection::open(&cache_path).unwrap();

    // The next freshness pass heals the schema and the rows in place
    // instead of staying `Fault::Cache` forever.
    assert!(
        super::store::ensure_cache_ready_at(&beads_dir, &cache_path).unwrap()
    );
    let status = read_model_status_at(&beads_dir, &cache_path);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.issues, 2);
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn freshness_stamp_loses_to_in_place_invalidation() {
    use std::time::Duration;
    let temp = tempfile::tempdir().unwrap();
    let (beads_dir, cache_path) = test_store_path(&temp);
    seed_event_store(&beads_dir);

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();

    // Capture the reader view a Fresh probe would hold, then invalidate
    // in place the way a lost post-commit guard does.
    let connection =
        super::store::open_read_write(&cache_path, Duration::from_secs(5))
            .unwrap_or_else(|_| {
                panic!("cache must open for the probe snapshot")
            });
    let meta = super::store::read_meta(&connection).unwrap();
    let token = super::freshness::freshness_token(&beads_dir).unwrap();
    let _sweep = super::freshness::sweep_store_signatures(&beads_dir).unwrap();
    assert_eq!(token, meta.token);
    assert!(meta.streams_known);
    drop(connection);

    super::store::drop_cache_file(&cache_path).unwrap();

    let connection =
        super::store::open_read_write(&cache_path, Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("invalidated cache must still open"));
    let held = super::store::stamp_freshness_if_current(
        &cache_path,
        &connection,
        &meta,
        &token,
        super::freshness::now_ns(),
    )
    .unwrap_or_else(|_| panic!("stamp must report held or lost"));
    assert!(!held, "stamp must lose to in-place invalidation");
    drop(connection);

    let reopened =
        super::store::open_read_only(&cache_path, Duration::from_secs(5))
            .unwrap();
    let after = super::store::read_meta(&reopened).unwrap();
    assert_eq!(after.token, "");
    assert_eq!(after.last_sweep_ns, 0);
    assert!(!after.streams_known);
    drop(reopened);

    assert!(
        super::store::ensure_cache_ready_at(&beads_dir, &cache_path).unwrap()
    );
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn sweep_refresh_loses_to_concurrent_commit() {
    use std::collections::BTreeMap;
    use std::time::Duration;
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    let beads_dir = temp.path().join("beads");
    seed_event_store(&beads_dir);
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();

    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();

    let connection =
        super::store::open_read_write(&cache_path, Duration::from_secs(5))
            .unwrap_or_else(|_| {
                panic!("cache must open before the concurrent commit")
            });
    let stale_meta = super::store::read_meta(&connection).unwrap();
    let stale_stored = super::store::load_stored_stream_sigs(&connection);
    let stale_token = stale_meta.token.clone();
    drop(connection);

    append_issue_note(
        &beads_dir,
        "beads-1.1",
        "A note appended after the cached read.",
        Some("read-model-cas-test".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    let winner =
        super::store::open_read_only(&cache_path, Duration::from_secs(5))
            .unwrap();
    let winner_meta = super::store::read_meta(&winner).unwrap();
    let winner_stored = super::store::load_stored_stream_sigs(&winner);
    assert!(
        winner_meta.content_generation > stale_meta.content_generation,
        "publish must move content_generation so the stale sweep CAS loses"
    );
    assert!(winner_meta.streams_known);
    assert!(!winner_meta.token.is_empty());
    drop(winner);

    let connection =
        super::store::open_read_write(&cache_path, Duration::from_secs(5))
            .unwrap_or_else(|_| {
                panic!("cache must open for the stale sweep refresh")
            });
    let result = super::tail::commit_sweep_refresh(
        &beads_dir,
        &cache_path,
        &connection,
        &stale_meta,
        &stale_token,
        &stale_stored,
        &BTreeMap::new(),
        super::refresh::RefreshFinish::Readiness,
    );
    assert!(
        matches!(result, Err(super::store::Fault::Cache)),
        "stale sweep refresh must replay rather than overwrite the winner"
    );
    drop(connection);

    let after =
        super::store::open_read_only(&cache_path, Duration::from_secs(5))
            .unwrap();
    let after_meta = super::store::read_meta(&after).unwrap();
    let after_stored = super::store::load_stored_stream_sigs(&after);
    assert_eq!(after_meta.token, winner_meta.token);
    assert_eq!(after_meta.last_sweep_ns, winner_meta.last_sweep_ns);
    assert_eq!(after_meta.generation, winner_meta.generation);
    assert_eq!(
        after_meta.content_generation,
        winner_meta.content_generation
    );
    assert!(after_meta.streams_known);
    assert!(!after_meta.token.is_empty());
    assert_eq!(after_stored.len(), winner_stored.len());
    for (id, sig) in &winner_stored {
        assert_eq!(
            after_stored.get(id).map(|row| row.content_hash.as_str()),
            Some(sig.content_hash.as_str())
        );
    }
    drop(after);

    assert!(cache_path.is_file());
    let verify = read_model_verify_cache_at(&beads_dir, &cache_path);
    assert!(verify.matched, "{}", verify.reason);
}
