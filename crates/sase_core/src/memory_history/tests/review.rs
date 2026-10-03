//! Review-watermark tests: first-use emptiness, descendant counting,
//! the unknown-commit time fallback, atomic mark merging, and corrupt
//! stores that report instead of repairing.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::super::review::{
    load_review_store, query_mark_reviewed, query_review_state,
    review_store_path, REVIEW_STORE_FILENAME, REVIEW_STORE_SCHEMA_VERSION,
};
use super::super::wire::{
    MemoryHistoryError, MemoryHistoryMarkReviewedRequestWire,
    MemoryHistoryReviewStateRequestWire, MemoryHistoryScopeKindWire,
    MemoryHistoryScopeWire, MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};

const BASE_TIME: i64 = 1_700_000_000;
const STEP_SECS: i64 = 300;

fn git(repo: &Path, args: &[&str], time: Option<i64>) -> String {
    let mut command = Command::new("git");
    command.arg("-C").arg(repo).args(args);
    if let Some(stamp) = time {
        let value = format!("{stamp} +0000");
        command.env("GIT_AUTHOR_DATE", &value);
        command.env("GIT_COMMITTER_DATE", &value);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn write_file(repo: &Path, rel: &str, contents: &str) {
    let path = repo.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

/// A tiny repo with three memory commits (oldest first): note
/// created, note revised, other created.
struct TinyRepo {
    _tmp: tempfile::TempDir,
    repo: PathBuf,
    cache_dir: PathBuf,
    state_dir: PathBuf,
    commits: Vec<String>,
}

fn tiny_repo() -> TinyRepo {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--initial-branch=master"], None);
    git(&repo, &["config", "user.name", "SASE Test"], None);
    git(&repo, &["config", "user.email", "sase@example.com"], None);
    git(&repo, &["config", "commit.gpgsign", "false"], None);
    let mut commits = Vec::new();
    write_file(&repo, "memory/note.md", "# Note\n\nFirst words here.\n");
    git(&repo, &["add", "-A"], None);
    git(&repo, &["commit", "-m", "add note"], Some(BASE_TIME));
    commits.push(git(&repo, &["rev-parse", "HEAD"], None));
    write_file(
        &repo,
        "memory/note.md",
        "# Note\n\nFirst words here.\n\nSecond words follow.\n",
    );
    git(&repo, &["add", "-A"], None);
    git(
        &repo,
        &["commit", "-m", "revise note"],
        Some(BASE_TIME + STEP_SECS),
    );
    commits.push(git(&repo, &["rev-parse", "HEAD"], None));
    write_file(&repo, "memory/other.md", "# Other\n\nOther words.\n");
    git(&repo, &["add", "-A"], None);
    git(
        &repo,
        &["commit", "-m", "add other"],
        Some(BASE_TIME + 2 * STEP_SECS),
    );
    commits.push(git(&repo, &["rev-parse", "HEAD"], None));
    TinyRepo {
        cache_dir: tmp.path().join("cache"),
        state_dir: tmp.path().join("state"),
        repo,
        _tmp: tmp,
        commits,
    }
}

fn scope(repo: &TinyRepo, key: &str) -> MemoryHistoryScopeWire {
    MemoryHistoryScopeWire {
        scope_key: key.to_string(),
        scope_kind: MemoryHistoryScopeKindWire::Project,
        repo_root: repo.repo.to_string_lossy().into_owned(),
        memory_roots: vec!["memory".to_string()],
        instruction_files: Vec::new(),
        generated_notes: Vec::new(),
        renderer_prefixes: Vec::new(),
        config_paths: Vec::new(),
        cache_dir: repo.cache_dir.to_string_lossy().into_owned(),
    }
}

fn state_dir(repo: &TinyRepo) -> String {
    repo.state_dir.to_string_lossy().into_owned()
}

fn review_state(
    repo: &TinyRepo,
    key: &str,
) -> super::super::wire::MemoryHistoryReviewStateWire {
    query_review_state(&MemoryHistoryReviewStateRequestWire {
        scopes: vec![scope(repo, key)],
        state_dir: state_dir(repo),
    })
    .unwrap()
}

#[test]
fn first_use_has_no_watermark_and_zero_new() {
    let repo = tiny_repo();
    let state = review_state(&repo, "project:tiny");

    assert_eq!(state.schema_version, MEMORY_HISTORY_WIRE_SCHEMA_VERSION);
    assert!(!state.store_corrupt);
    assert_eq!(state.scopes.len(), 1);
    let entry = &state.scopes[0];
    assert_eq!(entry.scope_key, "project:tiny");
    assert!(entry.watermark.is_none());
    assert_eq!(entry.new_count, 0);
    assert_eq!(entry.newest_commit, repo.commits[2]);
    assert_eq!(entry.newest_committer_time, BASE_TIME + 2 * STEP_SECS);
    // First use creates nothing on disk.
    assert!(!review_store_path(&state_dir(&repo)).exists());
}

#[test]
fn mark_counts_strict_descendants() {
    let repo = tiny_repo();
    let scope_key = "project:tiny";

    let marked = query_mark_reviewed(&MemoryHistoryMarkReviewedRequestWire {
        scope: scope(&repo, scope_key),
        through_commit: repo.commits[0].clone(),
        state_dir: state_dir(&repo),
    })
    .unwrap();
    assert_eq!(marked.schema_version, MEMORY_HISTORY_WIRE_SCHEMA_VERSION);
    assert_eq!(marked.scope_key, scope_key);
    assert_eq!(marked.watermark.commit, repo.commits[0]);
    assert_eq!(marked.watermark.committer_time, BASE_TIME);
    assert!(marked.watermark.marked_at > BASE_TIME);

    let entry = &review_state(&repo, scope_key).scopes[0];
    assert_eq!(entry.new_count, 2);
    assert_eq!(entry.watermark.as_ref().unwrap().commit, repo.commits[0]);

    query_mark_reviewed(&MemoryHistoryMarkReviewedRequestWire {
        scope: scope(&repo, scope_key),
        through_commit: repo.commits[1].clone(),
        state_dir: state_dir(&repo),
    })
    .unwrap();
    let entry = &review_state(&repo, scope_key).scopes[0];
    assert_eq!(entry.new_count, 1);

    query_mark_reviewed(&MemoryHistoryMarkReviewedRequestWire {
        scope: scope(&repo, scope_key),
        through_commit: repo.commits[2].clone(),
        state_dir: state_dir(&repo),
    })
    .unwrap();
    let entry = &review_state(&repo, scope_key).scopes[0];
    assert_eq!(entry.new_count, 0);
}

#[test]
fn marks_merge_across_scopes_in_one_store() {
    let repo = tiny_repo();
    for key in ["project:tiny", "project:other"] {
        query_mark_reviewed(&MemoryHistoryMarkReviewedRequestWire {
            scope: scope(&repo, key),
            through_commit: repo.commits[2].clone(),
            state_dir: state_dir(&repo),
        })
        .unwrap();
    }
    // The second mark preserved the first: the write merged under
    // the lock instead of clobbering.
    let (stored, corrupt) = load_review_store(&state_dir(&repo)).unwrap();
    assert!(!corrupt);
    assert_eq!(stored.len(), 2);
    assert_eq!(stored["project:tiny"].commit, repo.commits[2]);
    assert_eq!(stored["project:other"].commit, repo.commits[2]);
    // One shared file, so every workspace clone reads the same marks.
    let text =
        fs::read_to_string(review_store_path(&state_dir(&repo))).unwrap();
    assert!(text.contains("project:tiny"));
    assert!(text.contains("project:other"));
}

#[test]
fn unknown_watermark_commit_falls_back_to_time() {
    let repo = tiny_repo();
    fs::create_dir_all(&repo.state_dir).unwrap();
    let unknown = "1".repeat(40);
    fs::write(
        review_store_path(&state_dir(&repo)),
        serde_json::json!({
            "schema_version": REVIEW_STORE_SCHEMA_VERSION,
            "watermarks": {
                "project:tiny": {
                    "commit": unknown,
                    "committer_time": BASE_TIME,
                    "marked_at": BASE_TIME,
                },
            },
        })
        .to_string(),
    )
    .unwrap();

    let state = review_state(&repo, "project:tiny");
    assert!(!state.store_corrupt);
    let entry = &state.scopes[0];
    assert_eq!(entry.watermark.as_ref().unwrap().commit, unknown);
    // This checkout never saw the watermark commit, so both later
    // changesets count by committer time.
    assert_eq!(entry.new_count, 2);
}

#[test]
fn corrupt_store_reads_empty_reports_and_stays_put() {
    let repo = tiny_repo();
    fs::create_dir_all(&repo.state_dir).unwrap();
    let path = review_store_path(&state_dir(&repo));
    fs::write(&path, "{ not json").unwrap();

    let state = review_state(&repo, "project:tiny");
    assert!(state.store_corrupt);
    let entry = &state.scopes[0];
    assert!(entry.watermark.is_none());
    assert_eq!(entry.new_count, 0);
    assert_eq!(entry.newest_commit, repo.commits[2]);
    // The read never repaired the file.
    assert_eq!(fs::read_to_string(&path).unwrap(), "{ not json");

    fs::write(
        &path,
        serde_json::json!({"schema_version": 999, "watermarks": {}})
            .to_string(),
    )
    .unwrap();
    let state = review_state(&repo, "project:tiny");
    assert!(state.store_corrupt);
    assert!(state.scopes[0].watermark.is_none());
}

#[test]
fn mark_unknown_commit_errors_without_touching_the_store() {
    let repo = tiny_repo();
    let error = query_mark_reviewed(&MemoryHistoryMarkReviewedRequestWire {
        scope: scope(&repo, "project:tiny"),
        through_commit: "2".repeat(40),
        state_dir: state_dir(&repo),
    })
    .unwrap_err();
    assert!(
        matches!(error, MemoryHistoryError::InvalidPath(_)),
        "unexpected {error:?}"
    );
    assert!(!review_store_path(&state_dir(&repo)).exists());
}

#[test]
fn mark_rejects_an_empty_state_dir() {
    let repo = tiny_repo();
    let error = query_mark_reviewed(&MemoryHistoryMarkReviewedRequestWire {
        scope: scope(&repo, "project:tiny"),
        through_commit: repo.commits[2].clone(),
        state_dir: String::new(),
    })
    .unwrap_err();
    assert!(
        matches!(error, MemoryHistoryError::InvalidScope(_)),
        "unexpected {error:?}"
    );
}

#[test]
fn review_state_rejects_empty_scopes() {
    let repo = tiny_repo();
    let error = query_review_state(&MemoryHistoryReviewStateRequestWire {
        scopes: Vec::new(),
        state_dir: state_dir(&repo),
    })
    .unwrap_err();
    assert!(
        matches!(error, MemoryHistoryError::InvalidScope(_)),
        "unexpected {error:?}"
    );
}

#[test]
fn store_filename_lives_outside_the_snapshot_cache() {
    assert_eq!(REVIEW_STORE_FILENAME, "memory_history_review.json");
    let path = review_store_path("/state");
    assert_eq!(path, Path::new("/state").join(REVIEW_STORE_FILENAME));
}
