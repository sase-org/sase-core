//! Fixture integration tests: temporary git repos exercised through
//! the public index, blob, and status API.
//!
//! Covered by §8 of the memory-history plan: a renamed file (similarity
//! around 63%, across directories), delete and recreate, first-parent
//! behaviour across merges, a shallow clone, a truncated budget, and
//! the three §5.2 invariants (blob OIDs match `rev-parse`, incremental
//! equals rebuild, lineage agrees with `git log --follow`).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::blobs::{read_blobs, BlobCache, BlobReadBudget};
use super::index::{
    build_index, index_from_json, index_to_json, load_index, persist_index,
    sync_index,
};
use super::status::path_status;
use super::wire::{
    FileChangeKindWire, FileHistoryBudgetWire, FileHistoryError,
    FileHistoryIndexWire, FileHistorySyncStatus, PathStateWire,
    FILE_HISTORY_WIRE_SCHEMA_VERSION,
};

struct Fixture {
    _tmp: tempfile::TempDir,
    repo: PathBuf,
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

// A clone does not inherit its source's local config, so every repo a test
// commits in needs its own identity.
fn configure_identity(repo: &Path) {
    git(repo, &["config", "user.name", "SASE Test"]);
    git(repo, &["config", "user.email", "sase@example.com"]);
    git(repo, &["config", "commit.gpgsign", "false"]);
}

fn init_repo() -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--initial-branch=master"]);
    configure_identity(&repo);
    Fixture { _tmp: tmp, repo }
}

fn write_file(repo: &Path, rel: &str, contents: &str) {
    let path = repo.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn commit_all(repo: &Path, message: &str) {
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-m", message]);
}

fn head(repo: &Path) -> String {
    git(repo, &["rev-parse", "HEAD"])
}

fn rev_blob(repo: &Path, rev: &str, path: &str) -> String {
    git(repo, &["rev-parse", &format!("{rev}:{path}")])
}

fn budget() -> FileHistoryBudgetWire {
    FileHistoryBudgetWire::default()
}

/// Sixty-line note body; *changed* line indexes are replaced with
/// unrelated text (22 of 60 yields rename similarity R063).
fn note_body(changed: &[usize]) -> String {
    (0..60)
        .map(|index| {
            if changed.contains(&index) {
                format!(
                    "CHANGED {index:02} completely different words here now yes"
                )
            } else {
                format!(
                    "line {index:02} the quick brown fox jumps over the lazy dog"
                )
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn changed_22() -> Vec<usize> {
    vec![
        1, 4, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54,
        56, 58, 59,
    ]
}

fn build(fixture: &Fixture, pathspecs: &[&str]) -> FileHistoryIndexWire {
    build_index(
        &fixture.repo,
        &pathspecs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        &budget(),
    )
    .unwrap()
}

#[test]
fn rename_across_directories_builds_one_lineage() {
    let fixture = init_repo();
    write_file(&fixture.repo, "notes/a.md", &note_body(&[]));
    commit_all(&fixture.repo, "create a");
    write_file(&fixture.repo, "notes/a.md", &note_body(&changed_22()));
    fs::create_dir_all(fixture.repo.join("archive")).unwrap();
    git(&fixture.repo, &["mv", "notes/a.md", "archive/b.md"]);
    commit_all(&fixture.repo, "move a to b");

    let index = build(&fixture, &["notes", "archive"]);
    assert_eq!(index.schema_version, FILE_HISTORY_WIRE_SCHEMA_VERSION);
    assert_eq!(index.lineages.len(), 1);
    let lineage = &index.lineages[0];
    assert_eq!(lineage.paths, vec!["notes/a.md", "archive/b.md"]);
    assert_eq!(lineage.current_path, "archive/b.md");
    assert!(!lineage.tombstone);
    assert!(lineage.complete);
    assert_eq!(lineage.versions.len(), 2);
    assert_eq!(lineage.versions[0].kind, FileChangeKindWire::Moved);
    assert_eq!(lineage.versions[1].kind, FileChangeKindWire::Created);
    let similarity = lineage.versions[0].similarity.unwrap();
    assert!(
        (55..=75).contains(&similarity),
        "similarity around 63, got {similarity}"
    );
    assert_eq!(index.path_aliases.get("notes/a.md"), Some(&lineage.id));
    assert_eq!(index.path_aliases.get("archive/b.md"), Some(&lineage.id));
    assert!(index.health.complete);
    assert!(!index.health.truncated);
    assert!(!index.health.shallow);
}

#[test]
fn delete_and_recreate_continues_lineage_with_gap() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "v1\n");
    commit_all(&fixture.repo, "create");
    git(&fixture.repo, &["rm", "-q", "f.md"]);
    git(&fixture.repo, &["commit", "-qm", "delete"]);
    write_file(&fixture.repo, "f.md", "v2\n");
    commit_all(&fixture.repo, "recreate");

    let index = build(&fixture, &["f.md"]);
    assert_eq!(index.lineages.len(), 1);
    let lineage = &index.lineages[0];
    assert!(!lineage.tombstone);
    assert_eq!(lineage.versions.len(), 3);
    assert!(lineage.versions[0].gap_before);
    assert_eq!(lineage.versions[0].kind, FileChangeKindWire::Created);
    assert_eq!(lineage.versions[1].kind, FileChangeKindWire::Deleted);
    assert!(!lineage.versions[1].gap_before);
    assert_eq!(lineage.paths, vec!["f.md"]);
}

#[test]
fn first_parent_walk_ignores_side_branch_commits() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "base\n");
    commit_all(&fixture.repo, "create f");
    git(&fixture.repo, &["checkout", "-qb", "side"]);
    write_file(&fixture.repo, "g.md", "side\n");
    commit_all(&fixture.repo, "create g on side");
    let side = head(&fixture.repo);
    git(&fixture.repo, &["checkout", "-q", "master"]);
    write_file(&fixture.repo, "f.md", "master change\n");
    commit_all(&fixture.repo, "edit f on master");
    git(
        &fixture.repo,
        &["merge", "--no-ff", "-m", "merge side", "side"],
    );
    let merge = head(&fixture.repo);

    let index = build(&fixture, &["f.md", "g.md"]);
    assert_eq!(index.lineages.len(), 2);
    let lineage_f = index
        .lineages
        .iter()
        .find(|lineage| lineage.current_path == "f.md")
        .unwrap();
    let commits_f: Vec<&str> = lineage_f
        .versions
        .iter()
        .map(|version| version.commit.as_str())
        .collect();
    assert_eq!(commits_f.len(), 2);
    assert!(!commits_f.contains(&side.as_str()));
    assert!(!commits_f.contains(&merge.as_str()));
    let lineage_g = index
        .lineages
        .iter()
        .find(|lineage| lineage.current_path == "g.md")
        .unwrap();
    assert_eq!(lineage_g.versions.len(), 1);
    assert_eq!(lineage_g.versions[0].commit, merge);
    assert_eq!(lineage_g.versions[0].kind, FileChangeKindWire::Created);
}

#[test]
fn blob_oids_match_rev_parse_for_every_version() {
    let fixture = init_repo();
    write_file(&fixture.repo, "notes/a.md", &note_body(&[]));
    commit_all(&fixture.repo, "create");
    write_file(&fixture.repo, "notes/a.md", &note_body(&changed_22()));
    fs::create_dir_all(fixture.repo.join("archive")).unwrap();
    git(&fixture.repo, &["mv", "notes/a.md", "archive/b.md"]);
    commit_all(&fixture.repo, "move");
    write_file(&fixture.repo, "archive/b.md", "final\n");
    commit_all(&fixture.repo, "edit");
    git(&fixture.repo, &["rm", "-q", "archive/b.md"]);
    git(&fixture.repo, &["commit", "-qm", "delete"]);

    let index = build(&fixture, &["notes", "archive"]);
    assert!(!index.lineages.is_empty());
    for lineage in &index.lineages {
        for version in &lineage.versions {
            let expected = if version.kind == FileChangeKindWire::Deleted {
                git(
                    &fixture.repo,
                    &[
                        "rev-parse",
                        &format!("{}^:{}", version.commit, version.path),
                    ],
                )
            } else {
                rev_blob(&fixture.repo, &version.commit, &version.path)
            };
            assert_eq!(
                version.blob_oid.as_deref(),
                Some(expected.as_str()),
                "blob mismatch at {}:{}",
                version.commit,
                version.path
            );
        }
    }
}

#[test]
fn lineage_agrees_with_git_follow() {
    let fixture = init_repo();
    write_file(&fixture.repo, "notes/a.md", &note_body(&[]));
    commit_all(&fixture.repo, "create");
    write_file(&fixture.repo, "notes/a.md", &note_body(&changed_22()));
    fs::create_dir_all(fixture.repo.join("archive")).unwrap();
    git(&fixture.repo, &["mv", "notes/a.md", "archive/b.md"]);
    commit_all(&fixture.repo, "move");

    let index = build(&fixture, &["notes", "archive"]);
    assert_eq!(index.lineages.len(), 1);
    let lineage_commits: Vec<String> = index.lineages[0]
        .versions
        .iter()
        .map(|version| version.commit.clone())
        .collect();
    let follow = git(
        &fixture.repo,
        &["log", "--follow", "--format=%H", "--", "archive/b.md"],
    );
    let follow_commits: Vec<String> =
        follow.lines().map(ToString::to_string).collect();
    assert_eq!(lineage_commits, follow_commits);
}

#[test]
fn shallow_clone_marks_health_and_incomplete() {
    let src = init_repo();
    for round in 1..=3 {
        write_file(&src.repo, "f.md", &format!("v{round}\n"));
        commit_all(&src.repo, &format!("v{round}"));
    }
    let tmp = tempfile::tempdir().unwrap();
    let shallow = tmp.path().join("shallow");
    let status = Command::new("git")
        .arg("clone")
        .arg("--depth")
        .arg("1")
        .arg(format!("file://{}", src.repo.display()))
        .arg(&shallow)
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "clone failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    configure_identity(&shallow);
    let index =
        build_index(&shallow, &["f.md".to_string()], &budget()).unwrap();
    assert!(index.health.shallow);
    assert!(index.health.shallow_boundary_time.is_some());
    assert!(!index.health.complete);
    assert_eq!(index.lineages.len(), 1);
    let lineage = &index.lineages[0];
    assert_eq!(lineage.versions.len(), 1);
    // The missing parent side reads as an add, but the boundary cut
    // still marks the lineage incomplete.
    assert_eq!(lineage.versions[0].kind, FileChangeKindWire::Created);
    assert!(!lineage.complete);
    let blob = rev_blob(&shallow, &index.tip, "f.md");
    assert_eq!(lineage.versions[0].blob_oid.as_deref(), Some(blob.as_str()));

    // A file genuinely created past the boundary stays complete.
    write_file(&shallow, "post.md", "post\n");
    git(&shallow, &["add", "-A"]);
    git(&shallow, &["commit", "-qm", "post"]);
    let index = build_index(
        &shallow,
        &["f.md".to_string(), "post.md".to_string()],
        &budget(),
    )
    .unwrap();
    assert_eq!(index.lineages.len(), 2);
    let post = index
        .lineages
        .iter()
        .find(|lineage| lineage.current_path == "post.md")
        .unwrap();
    assert!(post.complete);
    let cut = index
        .lineages
        .iter()
        .find(|lineage| lineage.current_path == "f.md")
        .unwrap();
    assert!(!cut.complete);
    assert!(!index.health.complete);
}

#[test]
fn truncated_budget_sets_flag_and_forces_incomplete() {
    let fixture = init_repo();
    for round in 1..=5 {
        write_file(&fixture.repo, "f.md", &format!("v{round}\n"));
        commit_all(&fixture.repo, &format!("v{round}"));
    }
    let mut capped = budget();
    capped.max_commits = 2;
    let index =
        build_index(&fixture.repo, &["f.md".to_string()], &capped).unwrap();
    assert!(index.health.truncated);
    assert!(!index.health.complete);
    assert_eq!(index.commit_count, 2);
    assert_eq!(index.lineages.len(), 1);
    assert_eq!(index.lineages[0].versions.len(), 2);
    assert!(!index.lineages[0].complete);
}

#[test]
fn incremental_fold_equals_full_rebuild() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "v1\n");
    commit_all(&fixture.repo, "create f");
    write_file(&fixture.repo, "f.md", "v2\n");
    commit_all(&fixture.repo, "edit f");
    let cached = build(&fixture, &["."]);
    assert_eq!(cached.commit_count, 2);

    write_file(&fixture.repo, "f.md", "v3\n");
    commit_all(&fixture.repo, "edit f again");
    write_file(&fixture.repo, "h.md", "new\n");
    commit_all(&fixture.repo, "create h");
    git(&fixture.repo, &["mv", "f.md", "f2.md"]);
    commit_all(&fixture.repo, "rename f");

    let pathspecs = vec![".".to_string()];
    let (folded, status) =
        sync_index(&fixture.repo, &pathspecs, &cached, &budget()).unwrap();
    assert_eq!(status, FileHistorySyncStatus::Folded);
    let rebuilt = build_index(&fixture.repo, &pathspecs, &budget()).unwrap();
    assert_eq!(folded, rebuilt);
    let lineage = folded
        .lineages
        .iter()
        .find(|lineage| lineage.current_path == "f2.md")
        .unwrap();
    assert_eq!(lineage.paths, vec!["f.md".to_string(), "f2.md".to_string()]);
    assert_eq!(lineage.versions.len(), 4);
}

#[test]
fn sync_returns_fresh_when_tip_unchanged() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "v1\n");
    commit_all(&fixture.repo, "create");
    let cached = build(&fixture, &["f.md"]);
    let pathspecs = vec!["f.md".to_string()];
    let (fresh, status) =
        sync_index(&fixture.repo, &pathspecs, &cached, &budget()).unwrap();
    assert_eq!(status, FileHistorySyncStatus::Fresh);
    assert_eq!(fresh, cached);
}

#[test]
fn sync_rebuilds_after_history_rewrite() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "v1\n");
    commit_all(&fixture.repo, "create");
    write_file(&fixture.repo, "f.md", "v2\n");
    commit_all(&fixture.repo, "edit");
    let cached = build(&fixture, &["f.md"]);

    git(&fixture.repo, &["reset", "--hard", "-q", "HEAD~1"]);
    write_file(&fixture.repo, "other.md", "new\n");
    commit_all(&fixture.repo, "replacement");
    let pathspecs = vec!["f.md".to_string()];
    let (rebuilt, status) =
        sync_index(&fixture.repo, &pathspecs, &cached, &budget()).unwrap();
    assert_eq!(status, FileHistorySyncStatus::Rebuilt);
    assert_eq!(rebuilt.tip, head(&fixture.repo));
    assert_eq!(rebuilt.lineages.len(), 1);
    assert_eq!(rebuilt.lineages[0].versions.len(), 1);
}

#[test]
fn sync_rebuilds_on_pathspec_change() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "v1\n");
    commit_all(&fixture.repo, "create");
    let cached = build(&fixture, &["f.md"]);
    let (rebuilt, status) = sync_index(
        &fixture.repo,
        &["f.md".to_string(), "g.md".to_string()],
        &cached,
        &budget(),
    )
    .unwrap();
    assert_eq!(status, FileHistorySyncStatus::Rebuilt);
    assert_eq!(rebuilt.pathspecs, vec!["f.md", "g.md"]);
}

#[test]
fn snapshot_round_trip_persist_and_load() {
    let fixture = init_repo();
    write_file(&fixture.repo, "f.md", "v1\n");
    commit_all(&fixture.repo, "create");
    let index = build(&fixture, &["f.md"]);

    let json = index_to_json(&index).unwrap();
    assert_eq!(index_from_json(&json).unwrap(), index);
    assert!(index_from_json(r#"{"schema_version":999}"#).is_err());

    let snapshot = fixture._tmp.path().join("cache").join("index.json");
    persist_index(&snapshot, &index).unwrap();
    assert_eq!(load_index(&snapshot).unwrap(), Some(index));
    assert_eq!(
        load_index(&snapshot.with_extension("absent")).unwrap(),
        None
    );
    fs::write(&snapshot, b"{corrupt").unwrap();
    assert!(load_index(&snapshot).is_err());
}

#[test]
fn path_status_reports_tracked_untracked_ignored() {
    let fixture = init_repo();
    write_file(&fixture.repo, "tracked.md", "body\n");
    write_file(&fixture.repo, ".gitignore", "ignored.md\n");
    commit_all(&fixture.repo, "create");
    write_file(&fixture.repo, "new.md", "new\n");
    write_file(&fixture.repo, "ignored.md", "skip\n");
    write_file(&fixture.repo, "tracked.md", "dirty\n");

    let tracked = path_status(&fixture.repo, "tracked.md", &budget()).unwrap();
    assert_eq!(tracked.state, PathStateWire::Tracked);
    let head_blob = rev_blob(&fixture.repo, "HEAD", "tracked.md");
    assert_eq!(tracked.head_oid.as_deref(), Some(head_blob.as_str()));
    assert_eq!(tracked.index_oid, tracked.head_oid);
    assert!(tracked.worktree_oid.is_some());
    assert_ne!(tracked.worktree_oid, tracked.index_oid);

    let fresh = path_status(&fixture.repo, "new.md", &budget()).unwrap();
    assert_eq!(fresh.state, PathStateWire::Untracked);
    assert_eq!(fresh.index_oid, None);
    assert!(fresh.worktree_oid.is_some());

    let ignored = path_status(&fixture.repo, "ignored.md", &budget()).unwrap();
    assert_eq!(ignored.state, PathStateWire::Ignored);

    let outside = tempfile::tempdir().unwrap();
    let no_vcs = path_status(outside.path(), "f.md", &budget()).unwrap();
    assert_eq!(no_vcs.state, PathStateWire::NoVcs);

    assert!(matches!(
        path_status(&fixture.repo, "/abs", &budget()),
        Err(FileHistoryError::InvalidPath(_))
    ));
    assert!(matches!(
        path_status(&fixture.repo, "../evil", &budget()),
        Err(FileHistoryError::InvalidPath(_))
    ));
}

#[test]
fn blob_batch_reads_contents_caches_hits_and_missing() {
    let fixture = init_repo();
    write_file(&fixture.repo, "notes/a.md", &note_body(&[]));
    commit_all(&fixture.repo, "create");
    let index = build(&fixture, &["notes"]);
    let oid = index.lineages[0].versions[0].blob_oid.clone().unwrap();

    let budget_read = BlobReadBudget::default();
    let zero = "0".repeat(40);
    let blobs =
        read_blobs(&fixture.repo, &[oid.clone(), zero.clone()], &budget_read)
            .unwrap();
    assert_eq!(blobs[0].as_deref(), Some(note_body(&[]).as_bytes()));
    assert_eq!(blobs[1], None);
    let invalid =
        read_blobs(&fixture.repo, &["xyz".to_string()], &budget_read).unwrap();
    assert_eq!(invalid, vec![None]);

    let mut cache = BlobCache::new(8);
    let first = cache
        .read(&fixture.repo, std::slice::from_ref(&oid), &budget_read)
        .unwrap();
    assert!(cache.contains(&oid));
    assert_eq!(first[0].as_deref(), Some(note_body(&[]).as_bytes()));
    let second = cache
        .read(&fixture.repo, &[oid.clone(), zero.clone()], &budget_read)
        .unwrap();
    assert_eq!(second, first.into_iter().chain([None]).collect::<Vec<_>>());
    assert!(!cache.contains(&zero));
}

#[test]
fn rejects_bad_pathspecs_and_non_repo() {
    let fixture = init_repo();
    for bad in ["", "/abs", "../evil", "a:b", "-x"] {
        assert!(
            build_index(&fixture.repo, &[bad.to_string()], &budget()).is_err(),
            "{bad} should be rejected"
        );
    }
    let outside = tempfile::tempdir().unwrap();
    assert!(matches!(
        build_index(outside.path(), &["f.md".to_string()], &budget()),
        Err(FileHistoryError::NotARepo(_))
    ));
}
