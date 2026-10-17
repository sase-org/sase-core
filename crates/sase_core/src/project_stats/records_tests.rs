use super::git::ResolvedRepo;
use super::records;
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
