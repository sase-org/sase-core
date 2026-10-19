use super::git::ResolvedRepo;
use super::records;
use super::records_cache;
use super::test_support::{commit_at, init_repo, spec};
use super::wire::ProjectStatsBudgetWire;

fn resolved(
    path: &std::path::Path,
    tip: String,
    shallow: bool,
) -> ResolvedRepo {
    ResolvedRepo {
        canonical_path: path.to_string_lossy().into_owned(),
        common_dir: path.join(".git").to_string_lossy().into_owned(),
        identity: "fixture-records".to_owned(),
        ref_name: "main".to_owned(),
        tip,
        shallow,
        fetched_at: None,
    }
}

#[test]
fn records_summary_counts_blobs_and_author_time_bounds() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "record one.md",
        "secret body\n",
        "first",
        1_700_000_300,
        1_700_000_310,
    );
    commit_at(
        &fixture.repo,
        "nested/record\ttwo.json",
        "{\"secret\":true}",
        "second",
        1_700_000_100,
        1_700_000_320,
    );
    let tip = super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]);
    let repo = resolved(&fixture.repo, tip, false);
    let measured = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        "",
    );

    assert!(measured.complete);
    assert!(measured.issue.is_none());
    assert_eq!(measured.snapshot.tracked_files, 2);
    assert_eq!(measured.snapshot.lifetime_commits, Some(2));
    assert_eq!(measured.snapshot.first_commit_ts, Some(1_700_000_100));
    assert_eq!(measured.snapshot.last_commit_ts, Some(1_700_000_300));
    assert_eq!(measured.snapshot.tracked_lines, None);
    assert_eq!(measured.snapshot.languages, None);
}

#[test]
fn empty_repository_has_zero_counts_and_null_time_bounds() {
    let fixture = init_repo();
    let repo = resolved(&fixture.repo, String::new(), false);
    let measured = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        "",
    );

    assert!(measured.complete);
    assert_eq!(measured.snapshot.tracked_files, 0);
    assert_eq!(measured.snapshot.lifetime_commits, Some(0));
    assert_eq!(measured.snapshot.first_commit_ts, None);
    assert_eq!(measured.snapshot.last_commit_ts, None);
}

#[test]
fn shallow_summary_is_partial_and_not_cached_as_complete() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "entry.md",
        "private contents\n",
        "commit",
        1_700_000_500,
        1_700_000_501,
    );
    let tip = super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]);
    let repo = resolved(&fixture.repo, tip, true);
    let cache = tempfile::tempdir().expect("cache directory");
    let measured = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );

    assert!(!measured.complete);
    assert_eq!(
        measured.issue.as_ref().map(|issue| issue.code.as_str()),
        Some("shallow_records")
    );
    assert_eq!(measured.snapshot.lifetime_commits, Some(1));
    assert_eq!(
        std::fs::read_dir(cache.path()).expect("cache root").count(),
        0
    );
}

#[test]
fn successful_metadata_is_cached_by_tip() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "entry.md",
        "contents\n",
        "commit",
        1_700_000_500,
        1_700_000_501,
    );
    let tip = super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]);
    let repo = resolved(&fixture.repo, tip.clone(), false);
    let cache = tempfile::tempdir().expect("cache directory");
    let first = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );
    let mut cache_only = repo.clone();
    cache_only.canonical_path =
        fixture.repo.join("missing").to_string_lossy().into_owned();
    let second = records::measure(
        &cache_only,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );

    assert_eq!(first.snapshot, second.snapshot);
    assert!(second.complete);
}

#[test]
fn merge_history_counts_all_parents_and_uses_author_time_bounds() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "base.md",
        "base\n",
        "base",
        1_700_000_300,
        1_700_000_301,
    );
    super::test_support::git(&fixture.repo, &["switch", "-c", "side"]);
    commit_at(
        &fixture.repo,
        "side.md",
        "side\n",
        "side",
        1_700_000_500,
        1_700_000_501,
    );
    super::test_support::git(&fixture.repo, &["switch", "main"]);
    commit_at(
        &fixture.repo,
        "main.md",
        "main\n",
        "main",
        1_700_000_100,
        1_700_000_101,
    );
    let merged = std::process::Command::new("git")
        .arg("-C")
        .arg(&fixture.repo)
        .args(["merge", "--no-ff", "side", "-m", "merge"])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_AUTHOR_DATE", "@1700000200")
        .env("GIT_COMMITTER_DATE", "@1700000201")
        .output()
        .expect("merge histories");
    assert!(
        merged.status.success(),
        "merge failed: {}",
        String::from_utf8_lossy(&merged.stderr)
    );

    let tip = super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]);
    let measured = records::measure(
        &resolved(&fixture.repo, tip, false),
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        "",
    );
    assert!(measured.complete);
    assert_eq!(measured.snapshot.lifetime_commits, Some(4));
    assert_eq!(measured.snapshot.tracked_files, 3);
    assert_eq!(measured.snapshot.first_commit_ts, Some(1_700_000_100));
    assert_eq!(measured.snapshot.last_commit_ts, Some(1_700_000_500));
}

#[test]
fn corrupted_cache_recovers_with_a_fresh_complete_measurement() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "entry.md",
        "contents\n",
        "commit",
        1_700_000_500,
        1_700_000_501,
    );
    let repo = resolved(
        &fixture.repo,
        super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]),
        false,
    );
    let cache = tempfile::tempdir().expect("cache directory");
    let cache_path = records_cache::cache_path(
        &cache.path().to_string_lossy(),
        &records_cache::cache_key(&repo),
    );
    let first = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );
    assert!(first.complete);
    std::fs::write(&cache_path, "not valid json").expect("corrupt cache");

    let recovered = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );
    assert!(recovered.complete);
    assert!(recovered.issue.is_none());
    assert_eq!(recovered.snapshot, first.snapshot);
}

#[test]
fn a_new_tip_refreshes_records_instead_of_reusing_the_old_entry() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "first.md",
        "first\n",
        "first",
        1_700_000_500,
        1_700_000_501,
    );
    let mut repo = resolved(
        &fixture.repo,
        super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]),
        false,
    );
    let cache = tempfile::tempdir().expect("cache directory");
    let first = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );
    assert_eq!(first.snapshot.tracked_files, 1);
    commit_at(
        &fixture.repo,
        "second.md",
        "second\n",
        "second",
        1_700_000_100,
        1_700_000_601,
    );
    repo.tip = super::test_support::git(&fixture.repo, &["rev-parse", "HEAD"]);

    let refreshed = records::measure(
        &repo,
        &spec(&fixture.repo),
        &ProjectStatsBudgetWire::default(),
        &cache.path().to_string_lossy(),
    );
    assert!(refreshed.complete);
    assert_eq!(refreshed.snapshot.tracked_files, 2);
    assert_eq!(refreshed.snapshot.lifetime_commits, Some(2));
    assert_eq!(refreshed.snapshot.first_commit_ts, Some(1_700_000_100));
    assert_eq!(refreshed.snapshot.last_commit_ts, Some(1_700_000_500));
}
