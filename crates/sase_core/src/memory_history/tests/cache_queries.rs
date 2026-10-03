//! Snapshot-cache and query tests over the shared fixture corpus.
//!
//! Cache round trips, corrupt snapshots, key mismatches, incremental
//! equality against a cold rebuild, the upstream marker, and the
//! resolve / timeline / version / compare queries.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::super::cache::{
    clear_snapshot_memo, load_snapshot, snapshot_cache_key, snapshot_path,
    sync_scope,
};
use super::super::feed::{build_feed, FeedScope};
use super::super::query::{
    query_compare, query_feed, query_resolve, query_subjects, query_timeline,
    query_version,
};
use super::super::subjects::memory_history_pathspecs;
use super::super::wire::{
    MemoryHistoryClassWire, MemoryHistoryCompareRequestWire,
    MemoryHistoryFeedRequestWire, MemoryHistoryResolveRequestWire,
    MemoryHistorySubjectWire, MemoryHistorySubjectsRequestWire,
    MemoryHistorySyncStatusWire, MemoryHistoryTimelineRequestWire,
    MemoryHistoryVersionRequestWire, MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
use super::corpus::{build_project_corpus, Corpus, PROJECT_SCOPE_KEY};

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

fn lint_id() -> String {
    format!("note:{PROJECT_SCOPE_KEY}/lint_and_test")
}

fn scope_cache(
    corpus: &Corpus,
    dir: &Path,
) -> super::super::wire::MemoryHistoryScopeWire {
    let mut scope = corpus.scope.clone();
    scope.cache_dir = dir.to_string_lossy().into_owned();
    scope
}

fn snapshot_file(cache_dir: &Path) -> PathBuf {
    let versioned =
        cache_dir.join(format!("v{MEMORY_HISTORY_WIRE_SCHEMA_VERSION}"));
    let entries: Vec<PathBuf> = fs::read_dir(&versioned)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert_eq!(entries.len(), 1, "one snapshot in {versioned:?}");
    entries[0].clone()
}

fn feed_of(
    subjects: &[MemoryHistorySubjectWire],
) -> super::super::wire::MemoryHistoryFeedWire {
    let scopes = vec![FeedScope {
        scope_key: PROJECT_SCOPE_KEY,
        subjects,
    }];
    build_feed(&scopes, None, None, false)
}

#[test]
fn sync_round_trip_is_fresh_with_same_subjects() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    let first = sync_scope(&scope).unwrap();
    assert_eq!(first.sync.status, MemoryHistorySyncStatusWire::Rebuilt);
    assert_eq!(
        first.sync.schema_version,
        MEMORY_HISTORY_WIRE_SCHEMA_VERSION
    );
    assert_eq!(first.sync.tip, corpus.commits["gone_delete"]);
    assert_eq!(first.sync.subject_count, first.subjects.len() as u64);
    assert!(first.sync.upstream_ahead.is_none());
    assert!(first.sync.health.complete);

    // Drop the process memo so the second sync proves the disk round
    // trip instead of the in-memory cache.
    clear_snapshot_memo();
    let second = sync_scope(&scope).unwrap();
    assert_eq!(second.sync.status, MemoryHistorySyncStatusWire::Fresh);
    assert_eq!(second.sync.tip, first.sync.tip);
    assert_eq!(second.subjects, first.subjects);

    let listed =
        query_subjects(&MemoryHistorySubjectsRequestWire { scope }).unwrap();
    assert_eq!(listed.schema_version, MEMORY_HISTORY_WIRE_SCHEMA_VERSION);
    assert!(
        listed
            .subjects
            .iter()
            .all(|subject| subject.versions.is_empty()),
        "subjects carry no version bodies"
    );
    let ids: Vec<&str> = listed
        .subjects
        .iter()
        .map(|subject| subject.id.as_str())
        .collect();
    assert!(ids.contains(&lint_id().as_str()));
}

#[test]
fn corrupt_snapshot_rebuilds_and_parses() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    let first = sync_scope(&scope).unwrap();
    assert_eq!(first.sync.status, MemoryHistorySyncStatusWire::Rebuilt);
    fs::write(snapshot_file(holder.path()), b"definitely not json").unwrap();

    clear_snapshot_memo();
    let second = sync_scope(&scope).unwrap();
    assert_eq!(second.sync.status, MemoryHistorySyncStatusWire::Rebuilt);
    assert_eq!(second.subjects, first.subjects);

    // The rebuild persisted a file that parses again.
    let specs = memory_history_pathspecs(&scope).unwrap();
    let toplevel = git(&corpus.repo, &["rev-parse", "--show-toplevel"]);
    let common_dir = git(&corpus.repo, &["rev-parse", "--git-common-dir"]);
    let common_dir = if Path::new(&common_dir).is_absolute() {
        common_dir
    } else {
        format!("{toplevel}/{common_dir}")
    };
    let key = snapshot_cache_key(&scope, &common_dir, &specs);
    let loaded = load_snapshot(&snapshot_path(&scope.cache_dir, &key));
    assert!(loaded.is_some(), "rebuilt snapshot parses from disk");
}

#[test]
fn key_mismatch_rebuilds_and_then_is_fresh() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());
    let first = sync_scope(&scope).unwrap();
    assert_eq!(first.sync.status, MemoryHistorySyncStatusWire::Rebuilt);

    // A scope change (generated notes) hashes a different key, so the
    // old snapshot never matches: the sync rebuilds.
    let mut changed = scope.clone();
    changed
        .generated_notes
        .push("sase/memory/extra.md".to_string());
    let rebuilt = sync_scope(&changed).unwrap();
    assert_eq!(rebuilt.sync.status, MemoryHistorySyncStatusWire::Rebuilt);

    clear_snapshot_memo();
    let fresh = sync_scope(&changed).unwrap();
    assert_eq!(fresh.sync.status, MemoryHistorySyncStatusWire::Fresh);
    assert_eq!(fresh.subjects, rebuilt.subjects);
}

#[test]
fn incremental_fold_equals_cold_rebuild() {
    let corpus = build_project_corpus();
    let incremental = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();

    // Sync at the intermediate co-rendered commit, then at HEAD.
    git(
        &corpus.repo,
        &["checkout", "--quiet", &corpus.commits["co_render"]],
    );
    let scope = scope_cache(&corpus, incremental.path());
    let at_co_render = sync_scope(&scope).unwrap();
    assert_eq!(
        at_co_render.sync.status,
        MemoryHistorySyncStatusWire::Rebuilt
    );
    assert_eq!(at_co_render.sync.tip, corpus.commits["co_render"]);

    git(&corpus.repo, &["checkout", "--quiet", "master"]);
    clear_snapshot_memo();
    let folded = sync_scope(&scope).unwrap();
    assert_eq!(folded.sync.status, MemoryHistorySyncStatusWire::Folded);
    assert_eq!(folded.sync.tip, corpus.commits["gone_delete"]);

    // Cold rebuild at HEAD in a separate cache.
    let cold_scope = scope_cache(&corpus, cold.path());
    let rebuilt = sync_scope(&cold_scope).unwrap();
    assert_eq!(rebuilt.sync.status, MemoryHistorySyncStatusWire::Rebuilt);

    assert_eq!(folded.subjects, rebuilt.subjects);
    assert_eq!(feed_of(&folded.subjects), feed_of(&rebuilt.subjects));
}

#[test]
fn upstream_ahead_is_none_then_one() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    let plain = sync_scope(&scope).unwrap();
    assert_eq!(plain.sync.upstream_ahead, None);

    // One in-scope commit ahead on the remote-tracking ref, without
    // fetching: commit it, move HEAD back, then point
    // refs/remotes/origin/master at the dangling commit.
    let mut lint =
        fs::read(corpus.repo.join("sase/memory/lint_and_test.md")).unwrap();
    lint.extend_from_slice(b"\nupstream probe line\n");
    fs::write(corpus.repo.join("sase/memory/lint_and_test.md"), &lint).unwrap();
    git(&corpus.repo, &["add", "-A"]);
    git(&corpus.repo, &["commit", "-qm", "upstream probe"]);
    let ahead = git(&corpus.repo, &["rev-parse", "HEAD"]);
    git(&corpus.repo, &["reset", "--quiet", "--hard", "HEAD~1"]);
    git(
        &corpus.repo,
        &["update-ref", "refs/remotes/origin/master", &ahead],
    );
    git(
        &corpus.repo,
        &[
            "symbolic-ref",
            "refs/remotes/origin/HEAD",
            "refs/remotes/origin/master",
        ],
    );

    clear_snapshot_memo();
    let marked = sync_scope(&scope).unwrap();
    assert_eq!(marked.sync.upstream_ahead, Some(1));
    assert_eq!(marked.subjects.len(), plain.subjects.len());
}

#[test]
fn resolve_historical_path_and_predating_commit() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    // A pre-rename path resolves to the lint note.
    let by_history = query_resolve(&MemoryHistoryResolveRequestWire {
        scope: scope.clone(),
        selector: "memory/build_and_run.md".to_string(),
        at_commit: None,
    })
    .unwrap();
    assert_eq!(by_history.subject.id, lint_id());
    assert!(by_history.existed);
    assert!(by_history.version.is_some());

    // A bare historical basename resolves too.
    let by_basename = query_resolve(&MemoryHistoryResolveRequestWire {
        scope: scope.clone(),
        selector: "build_and_run.md".to_string(),
        at_commit: None,
    })
    .unwrap();
    assert_eq!(by_basename.subject.id, lint_id());

    // The roster note postdates the init commit.
    let roster_id = format!("note:{PROJECT_SCOPE_KEY}/roster");
    let predated = query_resolve(&MemoryHistoryResolveRequestWire {
        scope: scope.clone(),
        selector: roster_id,
        at_commit: Some(corpus.commits["init"].clone()),
    })
    .unwrap();
    assert!(!predated.existed);
    assert!(predated.version.is_none());

    // An unsafe revision token is an error, not an empty answer.
    assert!(query_resolve(&MemoryHistoryResolveRequestWire {
        scope,
        selector: lint_id(),
        at_commit: Some("--output=evil".to_string()),
    })
    .is_err());
}

#[test]
fn timeline_dirty_worktree_has_uncommitted_without_ordinal() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    let mut lint =
        fs::read(corpus.repo.join("sase/memory/lint_and_test.md")).unwrap();
    lint.extend_from_slice(b"\ndirty worktree line\n");
    fs::write(corpus.repo.join("sase/memory/lint_and_test.md"), &lint).unwrap();

    let timeline = query_timeline(&MemoryHistoryTimelineRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        include_hidden: false,
    })
    .unwrap();
    assert_eq!(timeline.subject_id, lint_id());
    assert_eq!(
        timeline.versions[0].class,
        MemoryHistoryClassWire::Uncommitted
    );
    assert_eq!(timeline.versions[0].ordinal, 0);
    assert!(
        timeline.versions[1..]
            .iter()
            .all(|version| version.ordinal >= 1),
        "no committed ordinal is invented for the worktree row"
    );

    // Staging the edit, then dirtying the worktree again, adds a
    // staged row (the index differs from both) that also carries no
    // ordinal.
    git(&corpus.repo, &["add", "sase/memory/lint_and_test.md"]);
    let mut restaged =
        fs::read(corpus.repo.join("sase/memory/lint_and_test.md")).unwrap();
    restaged.extend_from_slice(b"\nsecond dirty line\n");
    fs::write(corpus.repo.join("sase/memory/lint_and_test.md"), &restaged)
        .unwrap();
    let staged = query_timeline(&MemoryHistoryTimelineRequestWire {
        scope,
        selector: lint_id(),
        include_hidden: false,
    })
    .unwrap();
    assert_eq!(
        staged.versions[0].class,
        MemoryHistoryClassWire::Uncommitted
    );
    assert_eq!(staged.versions[1].class, MemoryHistoryClassWire::Staged);
    assert_eq!(staged.versions[1].ordinal, 0);
}

#[test]
fn version_selectors_and_bodies() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    // Ordinal form, with and without the `v` prefix, names one row.
    let by_ordinal = query_version(&MemoryHistoryVersionRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        version: "1".to_string(),
        include_body: true,
    })
    .unwrap();
    assert_eq!(by_ordinal.version.ordinal, 1);
    assert!(!by_ordinal.body_missing);
    assert!(!by_ordinal.body.is_empty());

    let by_v = query_version(&MemoryHistoryVersionRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        version: "v1".to_string(),
        include_body: false,
    })
    .unwrap();
    assert_eq!(by_v.version.commit, by_ordinal.version.commit);
    assert!(by_v.body.is_empty());
    assert!(!by_v.body_missing);

    // `~1` is the newest committed version.
    let newest = query_version(&MemoryHistoryVersionRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        version: "~1".to_string(),
        include_body: false,
    })
    .unwrap();
    let synced = sync_scope(&scope).unwrap();
    let lint = synced
        .subjects
        .iter()
        .find(|subject| subject.id == lint_id())
        .unwrap();
    assert_eq!(newest.version.commit, lint.versions[0].commit);

    // A unique SHA prefix names the same row as its ordinal.
    let prefix = by_ordinal.version.commit[..12].to_string();
    let by_sha = query_version(&MemoryHistoryVersionRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        version: prefix,
        include_body: false,
    })
    .unwrap();
    assert_eq!(by_sha.version.commit, by_ordinal.version.commit);

    // `now` reads the dirty worktree file.
    let mut lint_bytes =
        fs::read(corpus.repo.join("sase/memory/lint_and_test.md")).unwrap();
    lint_bytes.extend_from_slice(b"\nnow marker line\n");
    fs::write(
        corpus.repo.join("sase/memory/lint_and_test.md"),
        &lint_bytes,
    )
    .unwrap();
    let now = query_version(&MemoryHistoryVersionRequestWire {
        scope,
        selector: lint_id(),
        version: "now".to_string(),
        include_body: false,
    })
    .unwrap();
    assert_eq!(now.version.class, MemoryHistoryClassWire::Uncommitted);
    assert!(now.body.contains("now marker line"));
    assert!(!now.body_missing);
}

#[test]
fn compare_promotion_reports_type_change() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());
    let synced = sync_scope(&scope).unwrap();
    let policies_id = format!("note:{PROJECT_SCOPE_KEY}/policies");
    let policies = synced
        .subjects
        .iter()
        .find(|subject| subject.id == policies_id)
        .unwrap();
    let promoted = policies
        .versions
        .iter()
        .find(|version| version.commit == corpus.commits["promotion"])
        .unwrap();
    assert_eq!(promoted.class, MemoryHistoryClassWire::Promoted);

    let compared = query_compare(&MemoryHistoryCompareRequestWire {
        scope,
        base_selector: policies_id.clone(),
        base_version: (promoted.ordinal - 1).to_string(),
        target_selector: policies_id,
        target_version: promoted.ordinal.to_string(),
    })
    .unwrap();
    assert_eq!(
        compared.comparison.frontmatter.type_change,
        Some("promoted".to_string())
    );
    assert_eq!(compared.schema_version, MEMORY_HISTORY_WIRE_SCHEMA_VERSION);
}

#[test]
fn feed_query_merges_synced_scopes() {
    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    let feed = query_feed(&MemoryHistoryFeedRequestWire {
        scopes: vec![scope],
        since: None,
        limit: None,
        include_hidden: false,
    })
    .unwrap();
    assert_eq!(feed.schema_version, MEMORY_HISTORY_WIRE_SCHEMA_VERSION);
    // The default feed drops the hidden reflow, the pure directory
    // move, and the regen-only changeset.
    for label in ["reflow", "dirmove", "regen_only"] {
        assert!(
            feed.changesets
                .iter()
                .all(|entry| entry.commit != corpus.commits[label]),
            "{label} leaks into the default feed"
        );
    }
}

#[test]
fn blob_selector_resolves_newest_match_on_corpus() {
    use super::super::wire::MemoryHistoryVersionRequestWire;

    let corpus = build_project_corpus();
    let holder = tempfile::tempdir().unwrap();
    let scope = scope_cache(&corpus, holder.path());

    let timeline = query_timeline(&MemoryHistoryTimelineRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        include_hidden: true,
    })
    .unwrap();
    let committed: Vec<_> = timeline
        .versions
        .iter()
        .filter(|version| version.ordinal >= 1)
        .collect();
    assert!(!committed.is_empty(), "lint subject has committed versions");
    // Skip deleted rows: their tombstone blob must not resolve through blob:.
    let live = committed
        .iter()
        .find(|version| {
            version.kind
                != crate::file_history::wire::FileChangeKindWire::Deleted
                && version.blob_oid.is_some()
        })
        .expect("lint subject has a live blob version");
    let blob = live.blob_oid.clone().unwrap();
    let expected_newest = committed
        .iter()
        .filter(|version| {
            version.kind
                != crate::file_history::wire::FileChangeKindWire::Deleted
                && version.blob_oid.as_deref() == Some(blob.as_str())
        })
        .map(|version| version.ordinal)
        .max()
        .unwrap();

    let by_blob = query_version(&MemoryHistoryVersionRequestWire {
        scope: scope.clone(),
        selector: lint_id(),
        version: format!("blob:{blob}"),
        include_body: false,
    })
    .unwrap();
    assert_eq!(by_blob.version.ordinal, expected_newest);

    // A unique 12-char prefix names the same row when unambiguous.
    let prefix = blob[..12].to_string();
    let distinct = committed
        .iter()
        .filter(|version| {
            version.kind
                != crate::file_history::wire::FileChangeKindWire::Deleted
        })
        .filter_map(|version| version.blob_oid.clone())
        .filter(|other| {
            other
                .to_ascii_lowercase()
                .starts_with(&prefix.to_ascii_lowercase())
        })
        .collect::<std::collections::HashSet<_>>();
    if distinct.len() == 1 {
        let by_prefix = query_version(&MemoryHistoryVersionRequestWire {
            scope: scope.clone(),
            selector: lint_id(),
            version: format!("blob:{prefix}"),
            include_body: false,
        })
        .unwrap();
        assert_eq!(by_prefix.version.ordinal, expected_newest);
    }

    // A missing blob names the blob in its error.
    let missing = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string();
    let err = query_version(&MemoryHistoryVersionRequestWire {
        scope,
        selector: lint_id(),
        version: format!("blob:{missing}"),
        include_body: false,
    })
    .unwrap_err();
    assert!(err.to_string().contains(&missing));
}
