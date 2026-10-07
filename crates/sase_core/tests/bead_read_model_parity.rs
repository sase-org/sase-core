//! Cache-vs-replay parity: the full read query surface must return
//! identical outputs in replay mode and cache mode.
//!
//! Replay mode is a copy of the same store without a git dir (plain
//! replay serves every read). Cache mode is the store with a git dir, so
//! `read_store_issues` and `show_issue_detail_with_options` serve from
//! the SQLite read model. A randomized test interleaves real mutations
//! with cached reads and asserts equality after every step. When
//! `SASE_BEAD_BENCH_STORE` names a bench corpus directory, one sampled
//! parity pass runs over it too.

use std::fs;
use std::path::{Path, PathBuf};

use sase_core::bead::events::import_issues_to_event_streams;
use sase_core::bead::jsonl::{parse_issues_jsonl, write_event_store};
use sase_core::bead::{
    add_bead_link, add_dependency, append_issue_note, blocked_issues,
    close_issues, create_issue, list_issues, read_model_status,
    read_model_verify_cache, read_store_issues, ready_issues, resolve_issue_id,
    search_issues, show_issue_detail_with_options, stats,
    BeadCreateRequestWire, BeadUpdateFieldsWire, IssueTypeWire, PhaseSizeWire,
};
use sase_core::{ArtifactLinkOriginWire, BeadLinkDirectionWire};
use tempfile::tempdir;

const SEED_ISSUES_JSONL: &str = concat!(
    "{\"id\":\"bench-1\",\"title\":\"Epic one\",\"status\":\"open\",\"issue_type\":\"plan\",\"created_at\":\"2026-01-01T00:00:00Z\"}\n",
    "{\"id\":\"bench-1.1\",\"title\":\"Phase one\",\"status\":\"open\",\"issue_type\":\"phase\",\"parent_id\":\"bench-1\",\"created_at\":\"2026-01-01T00:01:00Z\"}\n",
    "{\"id\":\"bench-1.2\",\"title\":\"Phase two\",\"status\":\"closed\",\"issue_type\":\"phase\",\"parent_id\":\"bench-1\",\"created_at\":\"2026-01-01T00:02:00Z\"}\n",
    "{\"id\":\"bench-2\",\"title\":\"Ready task\",\"status\":\"ready\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:03:00Z\"}\n",
    "{\"id\":\"bench-3\",\"title\":\"Blocked task\",\"status\":\"ready\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:04:00Z\",\"dependencies\":[{\"issue_id\":\"bench-3\",\"depends_on_id\":\"bench-2\",\"created_at\":\"2026-01-01T00:04:00Z\",\"created_by\":\"\"}]}\n",
    "{\"id\":\"bench-4\",\"title\":\"External\",\"status\":\"open\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:05:00Z\",\"external_ref\":\"ext-1\"}\n",
);

/// A cache-mode store (with `.git`) plus its replay-mode copy.
struct ParityStores {
    _temp: tempfile::TempDir,
    cache_dir: PathBuf,
    replay_dir: PathBuf,
}

fn seed_stores() -> ParityStores {
    let temp = tempdir().unwrap();
    // A bare `.git` directory is enough for cache discovery: no commit,
    // no identity, no git binary needed.
    fs::create_dir_all(temp.path().join(".git")).unwrap();
    let cache_dir = temp.path().join("store");
    fs::create_dir_all(&cache_dir).unwrap();
    fs::write(cache_dir.join("config.json"), "{}\n").unwrap();
    let outcome = parse_issues_jsonl(SEED_ISSUES_JSONL);
    assert_eq!(outcome.loaded_rows, 6);
    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
    write_event_store(&cache_dir, &streams).unwrap();
    // One link so provenance neighborhoods are non-empty in both modes.
    add_bead_link(
        &cache_dir,
        "bench-2",
        "artifact:demo",
        "related",
        "parity seed link",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:06:00Z".to_string()),
        None,
    )
    .unwrap();
    append_issue_note(
        &cache_dir,
        "bench-1.1",
        "parity seed note",
        Some("parity".to_string()),
        Some("2026-01-01T00:07:00Z".to_string()),
        None,
    )
    .unwrap();
    let replay_dir = temp.path().join("replay-store");
    copy_store(&cache_dir, &replay_dir);
    ParityStores {
        _temp: temp,
        cache_dir,
        replay_dir,
    }
}

/// Refresh the replay copy from the cache-mode store.
fn resync(stores: &ParityStores) {
    fs::remove_dir_all(&stores.replay_dir).unwrap();
    copy_store(&stores.cache_dir, &stores.replay_dir);
}

fn copy_store(source: &Path, dest: &Path) {
    fs::create_dir_all(dest).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let from = entry.path();
        let to = dest.join(&name);
        if from.is_dir() {
            copy_store(&from, &to);
        } else {
            fs::copy(&from, &to).unwrap();
        }
    }
}

/// Assert the whole read surface agrees between cache and replay.
fn assert_parity(stores: &ParityStores) {
    let cached = read_store_issues(&stores.cache_dir).unwrap();
    let replayed = read_store_issues(&stores.replay_dir).unwrap();
    assert_eq!(
        serde_json::to_value(&cached).unwrap(),
        serde_json::to_value(&replayed).unwrap(),
        "read_store_issues differs between cache and replay"
    );
    let ids: Vec<String> =
        cached.iter().map(|issue| issue.id.clone()).collect();
    for id in &ids {
        for include_links in [true, false] {
            let cached_detail = show_issue_detail_with_options(
                &stores.cache_dir,
                id,
                include_links,
            )
            .unwrap();
            let replayed_detail = show_issue_detail_with_options(
                &stores.replay_dir,
                id,
                include_links,
            )
            .unwrap();
            assert_eq!(
                serde_json::to_value(&cached_detail).unwrap(),
                serde_json::to_value(&replayed_detail).unwrap(),
                "detail view of {id} (links={include_links}) differs"
            );
        }
        assert_eq!(
            resolve_issue_id(&stores.cache_dir, id).unwrap(),
            resolve_issue_id(&stores.replay_dir, id).unwrap(),
        );
    }
    assert_eq!(
        serde_json::to_value(
            list_issues(&stores.cache_dir, None, None, None).unwrap()
        )
        .unwrap(),
        serde_json::to_value(
            list_issues(&stores.replay_dir, None, None, None).unwrap()
        )
        .unwrap(),
    );
    assert_eq!(
        serde_json::to_value(ready_issues(&stores.cache_dir).unwrap()).unwrap(),
        serde_json::to_value(ready_issues(&stores.replay_dir).unwrap())
            .unwrap(),
    );
    assert_eq!(
        serde_json::to_value(blocked_issues(&stores.cache_dir).unwrap())
            .unwrap(),
        serde_json::to_value(blocked_issues(&stores.replay_dir).unwrap())
            .unwrap(),
    );
    assert_eq!(
        stats(&stores.cache_dir).unwrap(),
        stats(&stores.replay_dir).unwrap(),
    );
    for query in ["Epic", "task", "bench-1"] {
        assert_eq!(
            serde_json::to_value(
                search_issues(
                    &stores.cache_dir,
                    query,
                    None,
                    None,
                    None,
                    None,
                    false,
                )
                .unwrap()
            )
            .unwrap(),
            serde_json::to_value(
                search_issues(
                    &stores.replay_dir,
                    query,
                    None,
                    None,
                    None,
                    None,
                    false,
                )
                .unwrap()
            )
            .unwrap(),
            "search for {query} differs",
        );
    }
}

#[test]
fn cache_matches_replay_on_seeded_store() {
    let stores = seed_stores();
    // Warm the cache, then compare.
    let cached = read_store_issues(&stores.cache_dir).unwrap();
    assert_eq!(cached.len(), 6);
    assert_parity(&stores);

    let status = read_model_status(&stores.cache_dir);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.issues, 6);
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}

#[test]
fn cache_matches_replay_after_mutation_sequence() {
    let stores = seed_stores();
    let mut rng: u64 = 0x1234_5678_9abc_def0;
    let mut next_rng = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as usize
    };
    for step in 0..30 {
        let ids: Vec<String> = read_store_issues(&stores.cache_dir)
            .unwrap()
            .iter()
            .map(|issue| issue.id.clone())
            .collect();
        assert!(!ids.is_empty());
        let id = ids[next_rng() % ids.len()].clone();
        let other = ids[next_rng() % ids.len()].clone();
        let stamp =
            format!("2026-03-01T00:{:02}:{:02}Z", (step / 60) % 60, step % 60);
        match next_rng() % 5 {
            0 => {
                let _ = append_issue_note(
                    &stores.cache_dir,
                    &id,
                    &format!("randomized parity note {step}"),
                    Some("parity".to_string()),
                    Some(stamp),
                    None,
                );
            }
            1 => {
                let _ = sase_core::bead::update_issue(
                    &stores.cache_dir,
                    &id,
                    BeadUpdateFieldsWire {
                        title: Some(format!("Renamed at step {step}")),
                        ..Default::default()
                    },
                );
            }
            2 => {
                let _ = create_issue(
                    &stores.cache_dir,
                    BeadCreateRequestWire {
                        title: format!("randomized task {step}"),
                        issue_type: IssueTypeWire::Task,
                        size: Some(PhaseSizeWire::Small),
                        ..Default::default()
                    },
                );
            }
            3 => {
                let _ =
                    add_dependency(&stores.cache_dir, &id, &other, Some(stamp));
            }
            _ => {
                let _ = close_issues(
                    &stores.cache_dir,
                    &[id],
                    Some(format!("parity close {step}")),
                    None,
                    true,
                    Some(stamp),
                );
            }
        }
        resync(&stores);
        assert_parity(&stores);
    }
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}

/// Cold-rebuild cost, warm token-only read cost, and footprint.
///
/// Set `SASE_BEAD_BENCH_STORE` to a bench corpus beads directory laid out
/// as `<root>/store` with `<root>/.git` present (see `just bead-perf-scale`
/// and `tools/bead_scale_corpus`). The test drops any existing cache,
/// times one cold rebuild, then times warm reads that take the O(1)
/// token path, and prints the numbers for the bead notes. Without the
/// variable the test passes trivially.
#[test]
fn bench_corpus_read_model_timings() {
    use std::time::Instant;

    let Some(store) = std::env::var("SASE_BEAD_BENCH_STORE")
        .ok()
        .map(PathBuf::from)
    else {
        return;
    };
    // Drop any existing cache so the first read rebuilds cold. The
    // layout is `<root>/store` plus `<root>/.git`, mirroring the bench
    // harness scratch layout.
    if let Some(root) = store.parent() {
        let cache_dir = root.join(".git/sase/bead-read-model");
        if cache_dir.is_dir() {
            for entry in fs::read_dir(&cache_dir).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().and_then(|ext| ext.to_str())
                    == Some("sqlite")
                {
                    fs::remove_file(&path).unwrap();
                }
            }
        }
    }
    let replay_start = Instant::now();
    let replayed = sase_core::bead::read_event_store_issues(&store).unwrap();
    let replay_ms = replay_start.elapsed().as_secs_f64() * 1000.0;
    assert!(!replayed.is_empty());
    let cold_start = Instant::now();
    let cold = read_store_issues(&store).unwrap();
    let cold_ms = cold_start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(cold.len(), replayed.len());
    let mut warm_ms = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let warm = read_store_issues(&store).unwrap();
        warm_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(warm.len(), cold.len());
    }
    warm_ms.sort_by(|left, right| {
        left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
    });
    let status = read_model_status(&store);
    assert!(status.fresh, "{}", status.reason);
    eprintln!(
        "read-model timings: issues={} replay_ms={:.0} cold_rebuild_ms={:.0} warm_ms={warm_ms:.1?} cache_bytes={} generation={}",
        cold.len(),
        replay_ms,
        cold_ms,
        status.size_bytes,
        status.generation,
    );
    let verify = read_model_verify_cache(&store);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}

/// Sampled parity over a real bench corpus when one is provided.
///
/// Set `SASE_BEAD_BENCH_STORE` to a bench corpus directory (see
/// `just bead-perf-scale`): the harness compares the full issue list plus
/// a sample of detail views, then `--verify-cache` semantics. Without the
/// variable the test passes trivially; CI's record-only 4x run sets it.
#[test]
fn bench_corpus_sampled_parity() {
    let Some(store) = std::env::var("SASE_BEAD_BENCH_STORE")
        .ok()
        .map(PathBuf::from)
    else {
        return;
    };
    let cached = read_store_issues(&store).unwrap();
    assert!(!cached.is_empty());
    let stride = (cached.len() / 50).max(1);
    for issue in cached.iter().step_by(stride) {
        let detail =
            show_issue_detail_with_options(&store, &issue.id, true).unwrap();
        assert_eq!(detail.issue.id, issue.id);
    }
    let status = read_model_status(&store);
    assert!(status.fresh, "{}", status.reason);
    let verify = read_model_verify_cache(&store);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}
