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
    add_bead_link, add_dependency, add_task_plus_one, append_issue_note,
    bead_history, bead_store_fingerprint, blocked_issues, board_snapshot,
    cancel_task_snooze, close_issues, create_issue, edit_issue_note,
    get_epic_children, list_issue_page, list_issues, open_issue,
    read_model_status, read_model_verify_cache, read_store_issues,
    ready_issues, remove_bead_link, remove_dependencies, remove_issue,
    remove_issue_note, resolve_issue_id, search_issues, show_issue,
    show_issue_detail_with_options, snooze_task, stats, statuses_for_ids,
    BeadCreateRequestWire, BeadUpdateFieldsWire, IssueTypeWire, PhaseSizeWire,
};
use sase_core::{ArtifactLinkOriginWire, BeadLinkDirectionWire};
use tempfile::tempdir;

const SEED_ISSUES_JSONL: &str = concat!(
    "{\"id\":\"bench-1\",\"title\":\"Epic one\",\"status\":\"open\",\"issue_type\":\"plan\",\"tier\":\"epic\",\"created_at\":\"2026-01-01T00:00:00Z\"}\n",
    "{\"id\":\"bench-1.1\",\"title\":\"Phase one\",\"status\":\"open\",\"issue_type\":\"phase\",\"parent_id\":\"bench-1\",\"created_at\":\"2026-01-01T00:01:00Z\"}\n",
    "{\"id\":\"bench-1.2\",\"title\":\"Phase two\",\"status\":\"closed\",\"issue_type\":\"phase\",\"parent_id\":\"bench-1\",\"created_at\":\"2026-01-01T00:02:00Z\"}\n",
    "{\"id\":\"bench-2\",\"title\":\"Ready task\",\"status\":\"ready\",\"issue_type\":\"task\",\"task_type\":\"feature\",\"created_at\":\"2026-01-01T00:03:00Z\"}\n",
    "{\"id\":\"bench-3\",\"title\":\"Blocked task\",\"status\":\"ready\",\"issue_type\":\"task\",\"task_type\":\"bug\",\"created_at\":\"2026-01-01T00:04:00Z\",\"dependencies\":[{\"issue_id\":\"bench-3\",\"depends_on_id\":\"bench-2\",\"created_at\":\"2026-01-01T00:04:00Z\",\"created_by\":\"\"}]}\n",
    "{\"id\":\"bench-4\",\"title\":\"External\",\"status\":\"open\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:05:00Z\",\"external_ref\":\"ext-1\"}\n",
    "{\"id\":\"bench-a1\",\"title\":\"Suffix task\",\"status\":\"open\",\"issue_type\":\"task\",\"task_type\":\"feature\",\"created_at\":\"2026-01-01T00:06:00Z\"}\n",
    "{\"id\":\"zz-a1\",\"title\":\"Suffix collision\",\"status\":\"open\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:07:00Z\"}\n",
    "{\"id\":\"bench-5\",\"title\":\"Flag task\",\"status\":\"open\",\"issue_type\":\"task\",\"task_type\":\"flag\",\"task_type_fields\":{\"remove_by_date\":\"2020-01-01\",\"remove_by_release\":\"0.0.0\"},\"created_at\":\"2026-01-01T00:08:00Z\"}\n",
    "{\"id\":\"bench-6\",\"title\":\"Closed task\",\"status\":\"closed\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:09:00Z\"}\n",
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
    assert_eq!(outcome.loaded_rows, 10);
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
    // One bead-target link so the provenance touch rule resolves a
    // shorthand neighbor in both modes.
    add_bead_link(
        &cache_dir,
        "bench-4",
        "bead:bench-2",
        "related",
        "parity bead link",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2026-01-01T00:06:30Z".to_string()),
        None,
    )
    .unwrap();
    add_task_plus_one(
        &cache_dir,
        "bench-2",
        "parity-seed",
        "parity evidence",
        &[],
        Some("2026-01-01T00:06:45Z".to_string()),
        None,
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
        assert_eq!(
            serde_json::to_value(show_issue(&stores.cache_dir, id).unwrap())
                .unwrap(),
            serde_json::to_value(show_issue(&stores.replay_dir, id).unwrap())
                .unwrap(),
            "point lookup of {id} differs"
        );
        assert_history_parity(stores, id);
    }
    // Resolution answers, not just successes: unique suffixes, ambiguous
    // suffixes (`a1` names two beads), unknown shorthands, and full IDs.
    for probe in ["2", "a1", "zzz", "bench-1.1", "bench-9", ""] {
        assert_eq!(
            resolve_issue_id(&stores.cache_dir, probe)
                .map_err(|error| (error.kind, error.message)),
            resolve_issue_id(&stores.replay_dir, probe)
                .map_err(|error| (error.kind, error.message)),
            "resolve of {probe:?} differs"
        );
    }
    assert_history_parity(stores, "zzz");
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
    assert_list_page_parity(stores);
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
    assert_eq!(
        sase_core::bead::closed_ids(&stores.cache_dir).unwrap(),
        sase_core::bead::closed_ids(&stores.replay_dir).unwrap(),
        "closed IDs differ"
    );
    // Multi-get: exact IDs, a unique suffix, an ambiguous suffix, and
    // unknown IDs are omitted in both modes, never errors.
    for probes in [
        vec!["bench-2", "2", "a1", "zzz", "bench-1.1", "bench-9"],
        vec!["bench-5", "5", "a1"],
        Vec::<&str>::new(),
    ] {
        let wanted: Vec<String> =
            probes.iter().map(|probe| probe.to_string()).collect();
        assert_eq!(
            statuses_for_ids(&stores.cache_dir, &wanted).unwrap(),
            statuses_for_ids(&stores.replay_dir, &wanted).unwrap(),
            "statuses for {probes:?} differ"
        );
    }
    assert_eq!(
        serde_json::to_value(board_snapshot(&stores.cache_dir).unwrap())
            .unwrap(),
        serde_json::to_value(board_snapshot(&stores.replay_dir).unwrap())
            .unwrap(),
    );
    for epic in ["bench-1", "bench-2", "zzz"] {
        assert_eq!(
            serde_json::to_value(
                get_epic_children(&stores.cache_dir, epic).unwrap()
            )
            .unwrap(),
            serde_json::to_value(
                get_epic_children(&stores.replay_dir, epic).unwrap()
            )
            .unwrap(),
            "children of {epic} differ"
        );
    }
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
    // Filtered search parity, including a regex lane.
    assert_eq!(
        serde_json::to_value(
            search_issues(
                &stores.cache_dir,
                "task",
                Some(&["open".to_string()]),
                Some(&["task".to_string()]),
                None,
                Some(2),
                false,
            )
            .unwrap()
        )
        .unwrap(),
        serde_json::to_value(
            search_issues(
                &stores.replay_dir,
                "task",
                Some(&["open".to_string()]),
                Some(&["task".to_string()]),
                None,
                Some(2),
                false,
            )
            .unwrap()
        )
        .unwrap(),
        "filtered search differs",
    );
    assert_eq!(
        serde_json::to_value(
            search_issues(
                &stores.cache_dir,
                "bench-[0-9]",
                None,
                None,
                None,
                None,
                true,
            )
            .unwrap()
        )
        .unwrap(),
        serde_json::to_value(
            search_issues(
                &stores.replay_dir,
                "bench-[0-9]",
                None,
                None,
                None,
                None,
                true,
            )
            .unwrap()
        )
        .unwrap(),
        "regex search differs",
    );
}

/// Filtered list parity with pre-limit totals: every filter combination
/// must agree between the indexed lane and the replay lane, including
/// empty task-type filters (which match nothing) and invalid filters
/// (which fail identically).
/// One filtered-list case: status, type, tier, and task-type filters
/// plus an optional newest-N limit.
type ListPageCase = (
    Option<Vec<String>>,
    Option<Vec<String>>,
    Option<Vec<String>>,
    Option<Vec<String>>,
    Option<usize>,
);

fn assert_list_page_parity(stores: &ParityStores) {
    let open = Some(vec!["open".to_string()]);
    let closed = Some(vec!["closed".to_string()]);
    let ready = Some(vec!["ready".to_string()]);
    let task = Some(vec!["task".to_string()]);
    let plan = Some(vec!["plan".to_string()]);
    let epic_tier = Some(vec!["epic".to_string()]);
    let feature = Some(vec!["feature".to_string()]);
    let empty_task = Some(vec!["".to_string()]);
    let cases: Vec<ListPageCase> = vec![
        (None, None, None, None, None),
        (open.clone(), None, None, None, None),
        (closed.clone(), None, None, None, None),
        (closed.clone(), None, None, None, Some(1)),
        (closed.clone(), None, None, None, Some(20)),
        (None, task.clone(), None, None, None),
        (None, task.clone(), None, feature.clone(), None),
        (None, task.clone(), None, feature.clone(), Some(1)),
        (None, None, None, empty_task.clone(), None),
        (None, plan.clone(), epic_tier.clone(), None, None),
        (ready.clone(), task.clone(), None, None, Some(0)),
        (open.clone(), task.clone(), None, feature.clone(), Some(2)),
    ];
    for (statuses, issue_types, tiers, task_types, limit) in &cases {
        let cached = list_issue_page(
            &stores.cache_dir,
            statuses.as_deref(),
            issue_types.as_deref(),
            tiers.as_deref(),
            task_types.as_deref(),
            *limit,
        )
        .map(|(total, issues)| (total, serde_json::to_value(&issues).unwrap()))
        .map_err(|error| (error.kind, error.message));
        let replayed = list_issue_page(
            &stores.replay_dir,
            statuses.as_deref(),
            issue_types.as_deref(),
            tiers.as_deref(),
            task_types.as_deref(),
            *limit,
        )
        .map(|(total, issues)| (total, serde_json::to_value(&issues).unwrap()))
        .map_err(|error| (error.kind, error.message));
        assert_eq!(
            cached, replayed,
            "list page {statuses:?} {issue_types:?} {tiers:?} {task_types:?} {limit:?} differs"
        );
    }
    // Invalid filters fail identically on both lanes.
    let bogus = Some(vec!["bogus".to_string()]);
    assert_eq!(
        list_issue_page(
            &stores.cache_dir,
            bogus.as_deref(),
            None,
            None,
            None,
            None,
        )
        .map_err(|error| (error.kind, error.message)),
        list_issue_page(
            &stores.replay_dir,
            bogus.as_deref(),
            None,
            None,
            None,
            None,
        )
        .map_err(|error| (error.kind, error.message)),
    );
}

/// History parity between the lineage lane (cache mode) and the full
/// replay (replay mode), comparing successes and error text alike.
fn assert_history_parity(stores: &ParityStores, issue_id: &str) {
    assert_eq!(
        bead_history(&stores.cache_dir, issue_id)
            .map(|history| serde_json::to_value(&history).unwrap())
            .map_err(|error| (error.kind, error.message)),
        bead_history(&stores.replay_dir, issue_id)
            .map(|history| serde_json::to_value(&history).unwrap())
            .map_err(|error| (error.kind, error.message)),
        "history of {issue_id} differs"
    );
}

#[test]
fn cache_matches_replay_on_seeded_store() {
    let stores = seed_stores();
    // Warm the cache, then compare.
    let cached = read_store_issues(&stores.cache_dir).unwrap();
    assert_eq!(cached.len(), 10);
    assert_parity(&stores);

    let status = read_model_status(&stores.cache_dir);
    assert!(status.fresh, "{}", status.reason);
    assert_eq!(status.issues, 10);
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}

/// Store fingerprint before a mutation step, for change detection.
fn step_fingerprint(stores: &ParityStores) -> String {
    serde_json::to_value(bead_store_fingerprint(&stores.cache_dir).unwrap())
        .unwrap()
        .to_string()
}

/// Tail/rebuild outcome counters after a step.
fn refresh_counts(stores: &ParityStores) -> (u64, u64, u64) {
    let status = read_model_status(&stores.cache_dir);
    (status.generation, status.tail_count, status.rebuild_count)
}

/// Assert cache equals replay after one step, and that a step which
/// changed the store refreshed through exactly one content path (tail
/// or rebuild) while a no-op step served without refreshing.
fn assert_step_refresh(
    stores: &ParityStores,
    changed: bool,
    before: (u64, u64, u64),
) {
    resync(stores);
    assert_parity(stores);
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
    let after = refresh_counts(stores);
    let refreshes = (after.1 + after.2) - (before.1 + before.2);
    if changed {
        assert_eq!(
            refreshes, 1,
            "a changed store must refresh through exactly one path"
        );
    } else {
        assert_eq!(refreshes, 0, "an unchanged store must serve");
    }
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
        let before_fingerprint = step_fingerprint(&stores);
        let before_counts = refresh_counts(&stores);
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
                // Stamp creations like every other step: wall-clock
                // creations sort after later past-stamp mutations, which
                // trips the dependency target-exists check on replay.
                let _ = create_issue(
                    &stores.cache_dir,
                    BeadCreateRequestWire {
                        title: format!("randomized task {step}"),
                        issue_type: IssueTypeWire::Task,
                        size: Some(PhaseSizeWire::Small),
                        task_type: Some("feature".to_string()),
                        now: Some(stamp.clone()),
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
                    false,
                    Some(stamp),
                );
            }
        }
        let changed = step_fingerprint(&stores) != before_fingerprint;
        assert_step_refresh(&stores, changed, before_counts);
    }
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}

/// Rewrite one stream file through an atomic rename, as real writers do.
fn rewrite_stream_file(stores: &ParityStores, stream_id: &str, text: String) {
    let path = stores
        .cache_dir
        .join("events/streams")
        .join(format!("{stream_id}.jsonl"));
    let tmp_path = path.with_extension("jsonl.tmp");
    fs::write(&tmp_path, text.as_bytes()).unwrap();
    fs::rename(&tmp_path, &path).unwrap();
}

/// Assert one adversarial step: the mutation runs, the store refreshes
/// through the expected path (tail keeps the generation, rebuild bumps
/// it), and cache still equals replay afterwards.
fn assert_adversarial_step(
    stores: &ParityStores,
    before: (u64, u64, u64),
    expect_tail: bool,
) {
    resync(stores);
    assert_parity(stores);
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
    let after = refresh_counts(stores);
    assert_eq!(
        (after.1 + after.2) - (before.1 + before.2),
        1,
        "each adversarial step must refresh exactly once"
    );
    if expect_tail {
        assert_eq!(after.0, before.0, "tail must not open a new generation");
        assert_eq!(after.1, before.1 + 1);
    } else {
        assert_eq!(after.0, before.0 + 1, "fallback must rebuild");
        assert_eq!(after.2, before.2 + 1);
    }
}

#[test]
fn cache_matches_replay_with_adversarial_history() {
    let stores = seed_stores();
    // Warm the cache so every step below exercises a refresh decision.
    assert_parity(&stores);
    let step = |stores: &ParityStores| refresh_counts(stores);

    // A second machine's clock running ahead still sorts after the
    // frontier, so the tail applies it.
    let before = step(&stores);
    append_issue_note(
        &stores.cache_dir,
        "bench-2",
        "note from a fast clock",
        Some("parity".to_string()),
        Some("2027-05-01T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);

    // The same clock running behind sorts before the frontier: rebuild.
    let before = step(&stores);
    append_issue_note(
        &stores.cache_dir,
        "bench-2",
        "note from a slow clock",
        Some("parity".to_string()),
        Some("2020-01-01T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_adversarial_step(&stores, before, false);

    // Link add and remove resume through the tail, including the
    // provenance rescoping.
    let before = step(&stores);
    add_bead_link(
        &stores.cache_dir,
        "bench-4",
        "artifact:adversarial",
        "related",
        "adversarial link",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        Some("2027-06-01T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let before = step(&stores);
    remove_bead_link(
        &stores.cache_dir,
        "bench-4",
        "artifact:adversarial",
        Some("related"),
        BeadLinkDirectionWire::Out,
        Some("2027-06-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);

    // Dependency add and remove.
    let before = step(&stores);
    add_dependency(
        &stores.cache_dir,
        "bench-4",
        "bench-2",
        Some("2027-06-03T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let before = step(&stores);
    remove_dependencies(
        &stores.cache_dir,
        "bench-4",
        &["bench-2".to_string()],
        Some("2027-06-04T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);

    // Task +1 evidence, snooze, and wake.
    let before = step(&stores);
    add_task_plus_one(
        &stores.cache_dir,
        "bench-2",
        "parity-reporter",
        "adversarial evidence",
        &[],
        Some("2027-06-05T00:00:00Z".to_string()),
        None,
        None,
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let before = step(&stores);
    snooze_task(
        &stores.cache_dir,
        "bench-2",
        "2027-07-01T00:00:00Z",
        None,
        "adversarial snooze",
        "parity",
        Some("2027-06-06T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let before = step(&stores);
    cancel_task_snooze(
        &stores.cache_dir,
        "bench-2",
        "parity",
        Some("2027-06-07T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);

    // Note edit then retraction on a closed bead's note.
    let before = step(&stores);
    close_issues(
        &stores.cache_dir,
        &["bench-1.1".to_string()],
        Some("adversarial close".to_string()),
        None,
        false,
        Some("2027-06-08T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let note_id = read_store_issues(&stores.cache_dir)
        .unwrap()
        .iter()
        .find(|issue| issue.id == "bench-1.1")
        .unwrap()
        .notes
        .first()
        .unwrap()
        .id
        .clone();
    let before = step(&stores);
    edit_issue_note(
        &stores.cache_dir,
        "bench-1.1",
        &note_id,
        "edited after close",
        Some("parity".to_string()),
        Some("2027-06-09T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let before = step(&stores);
    remove_issue_note(
        &stores.cache_dir,
        "bench-1.1",
        &note_id,
        Some("parity".to_string()),
        Some("2027-06-10T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
    let before = step(&stores);
    open_issue(
        &stores.cache_dir,
        "bench-1.1",
        Some("2027-06-11T00:00:00Z".to_string()),
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);

    // A relocation-shaped rewrite: an event inserted mid-stream breaks
    // the append-prefix proof, so the full replay heals instead.
    let before = step(&stores);
    let stream_path = stores.cache_dir.join("events/streams/bench-2.jsonl");
    let text = fs::read_to_string(&stream_path).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let mut inserted: serde_json::Value =
        serde_json::from_str(&lines[1]).unwrap();
    inserted["event_id"] = serde_json::Value::from(
        "bench-2:000007:note_appended:bench-2:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    inserted["timestamp"] = serde_json::Value::from("2026-01-01T00:04:30Z");
    lines.insert(1, serde_json::to_string(&inserted).unwrap());
    rewrite_stream_file(&stores, "bench-2", lines.join("\n") + "\n");
    assert_adversarial_step(&stores, before, false);

    // A conflict-resolver-shaped rewrite: identical events, different
    // bytes (a trailing space per line keeps the JSONL valid). Same
    // fallback, same healed result.
    let before = step(&stores);
    let stream_path = stores.cache_dir.join("events/streams/bench-3.jsonl");
    let text = fs::read_to_string(&stream_path).unwrap();
    let spaced: Vec<String> = text
        .lines()
        .map(|line| {
            if line.trim().is_empty() {
                line.to_string()
            } else {
                format!("{line} ")
            }
        })
        .collect();
    rewrite_stream_file(&stores, "bench-3", spaced.join("\n") + "\n");
    assert_adversarial_step(&stores, before, false);

    // Whole-issue removal, including its link provenance rows.
    // `remove_issue` stamps wall-clock time, which sorts before this
    // test's artificial 2027 frontier, so the gate correctly rebuilds:
    // the removal genuinely interleaves before already-cached history.
    let before = step(&stores);
    remove_issue(&stores.cache_dir, "bench-4").unwrap();
    assert_adversarial_step(&stores, before, false);

    // Creation lands a brand-new stream through the tail.
    let before = step(&stores);
    create_issue(
        &stores.cache_dir,
        BeadCreateRequestWire {
            title: "adversarial task".to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            now: Some("2027-06-12T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_adversarial_step(&stores, before, true);
}

#[test]
fn concurrent_readers_see_consistent_snapshots() {
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    let stores = seed_stores();
    // The synchronously observed replay states; every concurrent cached
    // read must equal one of them.
    let mut states = HashSet::new();
    states.insert(
        serde_json::to_string(&read_store_issues(&stores.cache_dir).unwrap())
            .unwrap(),
    );
    let cache_dir = Arc::new(stores.cache_dir.clone());
    let barrier = Arc::new(Barrier::new(5));
    let snapshots: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let cache_dir = Arc::clone(&cache_dir);
        let barrier = Arc::clone(&barrier);
        let snapshots = Arc::clone(&snapshots);
        readers.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..25 {
                let snapshot = read_store_issues(&cache_dir).unwrap();
                snapshots
                    .lock()
                    .unwrap()
                    .push(serde_json::to_string(&snapshot).unwrap());
            }
        }));
    }
    barrier.wait();
    // Single-stream note appends: each lands atomically, so readers see
    // the old or the new file, never a mix.
    for step in 0..10 {
        append_issue_note(
            &cache_dir,
            "bench-2",
            &format!("concurrent note {step}"),
            Some("parity".to_string()),
            Some(format!("2026-03-03T00:00:{step:02}Z")),
            None,
        )
        .unwrap();
        states.insert(
            serde_json::to_string(
                &sase_core::bead::read_event_store_issues(&cache_dir).unwrap(),
            )
            .unwrap(),
        );
    }
    for reader in readers {
        reader.join().unwrap();
    }
    for snapshot in snapshots.lock().unwrap().iter() {
        assert!(
            states.contains(snapshot),
            "concurrent read served a state no replay ever produced"
        );
    }
    resync(&stores);
    assert_parity(&stores);
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
    // Snapshot-plus-tail: two note appends. The first absorbs one-time
    // store evolution (such as the under-lock prune of removed-flag
    // tombstone streams, which correctly forces one rebuild); the
    // second must take the incremental path (no new generation).
    let tail_target = cold[0].id.clone();
    append_issue_note(
        &store,
        &tail_target,
        "bench prime note",
        Some("parity".to_string()),
        Some("2030-01-01T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    read_store_issues(&store).unwrap();
    let status = read_model_status(&store);
    assert!(status.fresh, "{}", status.reason);
    // Re-baseline after priming: the probe below must tail from here.
    let primed_generation = status.generation;
    let primed_tails = status.tail_count;
    append_issue_note(
        &store,
        &tail_target,
        "bench tail-probe note",
        Some("parity".to_string()),
        Some("2030-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    let tail_start = Instant::now();
    let tailed = read_store_issues(&store).unwrap();
    let tail_ms = tail_start.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(tailed.len(), cold.len());
    let tail_status = read_model_status(&store);
    assert!(tail_status.fresh, "{}", tail_status.reason);
    assert_eq!(
        tail_status.generation, primed_generation,
        "tail probe rebuilt: {}",
        tail_status.last_refresh_reason
    );
    assert_eq!(tail_status.tail_count, primed_tails + 1);
    eprintln!(
        "read-model timings: issues={} replay_ms={:.0} cold_rebuild_ms={:.0} warm_ms={warm_ms:.1?} tail_ms={:.0} cache_bytes={} generation={} last_refresh={} ({})",
        cold.len(),
        replay_ms,
        cold_ms,
        tail_ms,
        tail_status.size_bytes,
        tail_status.generation,
        tail_status.last_refresh,
        tail_status.last_refresh_reason,
    );
    // Indexed queries (`read-model-queries` acceptance): a warm point
    // read, `ready`, the unfiltered list, and the default closed listing
    // (newest 20) must all stay flat while history grows.
    let point_id = cold[cold.len() / 2].id.clone();
    let mut point_ms = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let shown = show_issue(&store, &point_id).unwrap();
        point_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(shown.id, point_id);
    }
    let mut ready_ms = vec![];
    let mut ready_len = 0;
    for _ in 0..5 {
        let start = Instant::now();
        let ready = ready_issues(&store).unwrap();
        ready_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        ready_len = ready.len();
    }
    let mut list_ms = vec![];
    let mut list_len = 0;
    for _ in 0..5 {
        let start = Instant::now();
        let listed = list_issues(&store, None, None, None).unwrap();
        list_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        list_len = listed.len();
    }
    let mut closed_ms = vec![];
    let mut closed_total = 0;
    for _ in 0..5 {
        let start = Instant::now();
        let (total, closed) = list_issue_page(
            &store,
            Some(&["closed".to_string()]),
            None,
            None,
            None,
            Some(20),
        )
        .unwrap();
        closed_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        closed_total = total;
        assert!(closed.len() <= 20);
    }
    for samples in [&mut point_ms, &mut ready_ms, &mut list_ms, &mut closed_ms]
    {
        samples.sort_by(|left, right| {
            left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    eprintln!(
        "indexed timings: issues={} ready={} closed_total={} point_ms={point_ms:.1?} ready_ms={ready_ms:.1?} list_ms={list_ms:.1?} closed20_ms={closed_ms:.1?}",
        list_len, ready_len, closed_total,
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
