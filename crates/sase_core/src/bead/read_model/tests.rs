//! Unit tests for the read-model substrate: location, freshness, and
//! rebuild/serve semantics on small synthetic event stores.

use std::fs;
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

fn seed_event_store(beads_dir: &Path) {
    fs::create_dir_all(beads_dir.join("events/streams")).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    let outcome = parse_issues_jsonl(SEED_ISSUES_JSONL);
    assert_eq!(outcome.loaded_rows, 2);
    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
    assert!(!streams.is_empty());
    write_event_store(beads_dir, &streams).unwrap();
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
    assert_eq!(READ_MODEL_SCHEMA_VERSION, 1);
    assert_eq!(READ_MODEL_REDUCER_VERSION, 1);
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
