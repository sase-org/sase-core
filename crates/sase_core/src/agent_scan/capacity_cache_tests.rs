use std::fs;
use std::sync::Barrier;

use serde_json::{json, Value};
use tempfile::tempdir;

use super::tests::{capacity_artifact_dir, write_capacity_meta, write_json};
use super::*;
use crate::agent_scan::capacity_cache::{
    capacity_cache_path, load_capacity_cache, CAPACITY_CACHE_FILENAME,
    CAPACITY_CACHE_VERSION,
};

const DEAD_PID: i64 = 1_700_000_000;

fn capacity_options() -> AgentArtifactScanOptionsWire {
    AgentArtifactScanOptionsWire {
        capacity_only: true,
        ..AgentArtifactScanOptionsWire::default()
    }
}

fn assert_scan_data_equal(
    optimized: &AgentArtifactScanWire,
    legacy: &AgentArtifactScanWire,
) {
    assert_eq!(
        serde_json::to_value(&optimized.records).unwrap(),
        serde_json::to_value(&legacy.records).unwrap(),
        "retained record JSON differs"
    );
    assert_eq!(
        serde_json::to_value(&optimized.clan_context).unwrap(),
        serde_json::to_value(&legacy.clan_context).unwrap(),
        "pre-retention clan context JSON differs"
    );
    assert_eq!(
        serde_json::to_value(super::tests::rust_capacity_snapshot(
            &optimized.records,
            None,
        ))
        .unwrap(),
        serde_json::to_value(super::tests::rust_capacity_snapshot(
            &legacy.records,
            None,
        ))
        .unwrap(),
        "runner-capacity decisions differ"
    );
}

fn cold_scan(
    projects: &std::path::Path,
    options: AgentArtifactScanOptionsWire,
    process_missing: impl Fn(i32) -> bool + Copy,
) -> AgentArtifactScanWire {
    // A missing cache file is the cold path.
    let _ = fs::remove_file(capacity_cache_path(projects));
    scan_agent_artifacts_with_process_observation(
        projects,
        options,
        process_missing,
    )
}

fn read_cache_entries(projects: &std::path::Path) -> Value {
    let bytes = fs::read(capacity_cache_path(projects))
        .expect("expected a verdict cache file");
    serde_json::from_slice(&bytes).expect("cache file must be valid JSON")
}

#[test]
fn warm_scan_reuses_stopped_verdicts_without_rereading_metadata() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for (index, timestamp) in
        ["20260427190000", "20260427190001", "20260427190002"]
            .into_iter()
            .enumerate()
    {
        write_capacity_meta(
            &projects,
            timestamp,
            json!({
                "name": format!("stopped-{index}"),
                "stopped_at": "2026-04-27T19:00:00Z",
            }),
        );
    }

    let options = capacity_options();
    let process_missing = |_pid| false;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    assert!(cold.records.is_empty());
    assert!(capacity_cache_path(&projects).exists());

    // Every terminal row was cacheable: the warm scan classifies from
    // signatures alone and parses nothing.
    let warm = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert!(warm.records.is_empty());
    assert_scan_data_equal(&warm, &cold);
    assert_eq!(warm.stats.artifact_dirs_visited, 3);
    assert_eq!(
        warm.stats.marker_files_parsed, 0,
        "warm pruned history must not reread metadata JSON"
    );

    // The cache file itself is never treated as a project.
    assert_eq!(
        capacity_cache_path(&projects)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap(),
        CAPACITY_CACHE_FILENAME
    );
    assert_eq!(warm.stats.projects_visited, cold.stats.projects_visited);
}

#[test]
fn cold_warm_and_corrupt_cache_agree() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );
    write_capacity_meta(
        &projects,
        "20260427190001",
        json!({"name": "live", "pid": std::process::id()}),
    );

    let options = capacity_options();
    let process_missing = |pid| pid == DEAD_PID as i32;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    let warm = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&warm, &cold);

    fs::write(capacity_cache_path(&projects), b"{truncated").unwrap();
    let corrupt = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&corrupt, &cold);

    // A wrong-version cache is also a miss, then rebuilt.
    fs::write(
        capacity_cache_path(&projects),
        serde_json::to_vec(&json!({
            "version": 999,
            "root": "elsewhere",
            "entries": {},
        }))
        .unwrap(),
    )
    .unwrap();
    let wrong_version = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&wrong_version, &cold);

    // Change only the root identity of a valid current-version cache. The
    // modified cache still deserializes, so rejection proves root validation
    // runs independently of version and entry-shape checks.
    let mut wrong_root = read_cache_entries(&projects);
    assert_eq!(
        wrong_root.get("version").and_then(Value::as_u64),
        Some(u64::from(CAPACITY_CACHE_VERSION))
    );
    let actual_root = wrong_root
        .get("root")
        .and_then(Value::as_str)
        .expect("a valid cache records its root")
        .to_owned();
    wrong_root["root"] = json!(format!("{actual_root}-wrong"));
    fs::write(
        capacity_cache_path(&projects),
        serde_json::to_vec(&wrong_root).unwrap(),
    )
    .unwrap();
    assert!(
        load_capacity_cache(&projects).is_none(),
        "a valid cache with a different root identity must be rejected"
    );
    let mismatched = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&mismatched, &cold);
    assert!(
        mismatched.stats.marker_files_parsed > 0,
        "root rejection must force fresh classification instead of reusing entries"
    );
}

#[test]
fn unwritable_cache_location_falls_back_to_the_conservative_path() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let stopped = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );
    assert!(stopped.is_dir());

    // A directory at the cache path makes the atomic replacement fail;
    // the scan still returns the conservative result.
    fs::create_dir_all(capacity_cache_path(&projects)).unwrap();
    let options = capacity_options();
    let process_missing = |_pid| false;
    let scan = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&scan, &legacy);
    assert!(scan.records.is_empty());

    fs::remove_dir(capacity_cache_path(&projects)).unwrap();
    let rebuilt = scan_agent_artifacts_with_process_observation(
        &projects,
        options,
        process_missing,
    );
    assert!(capacity_cache_path(&projects).is_file());
    assert_scan_data_equal(&rebuilt, &legacy);
}

#[test]
fn in_place_metadata_edits_invalidate_stopped_verdicts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );

    let options = capacity_options();
    let process_missing = |_pid| false;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    assert!(cold.records.is_empty());

    // An in-place content change does not touch the directory mtime on
    // Linux, which is exactly why directory mtime alone cannot key the
    // verdict. Record it to document the trap.
    let dir_mtime_before = fs::metadata(&artifact).unwrap().modified().unwrap();
    fs::write(
        artifact.join("agent_meta.json"),
        serde_json::to_vec(&json!({"name": "unstopped"})).unwrap(),
    )
    .unwrap();
    let dir_mtime_after = fs::metadata(&artifact).unwrap().modified().unwrap();
    assert_eq!(dir_mtime_before, dir_mtime_after);

    let edited = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&edited, &legacy);
    assert_eq!(
        edited.records.len(),
        1,
        "clearing stopped_at revives the row"
    );
}

#[test]
fn replaced_metadata_and_recreated_directories_invalidate() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );

    let options = capacity_options();
    let process_missing = |_pid| false;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    assert!(cold.records.is_empty());

    // Atomic replacement changes the file identity even when the payload
    // still claims to be stopped.
    let staged = artifact.join("agent_meta.json.staged");
    fs::write(
        &staged,
        serde_json::to_vec(&json!({
            "name": "replaced",
            "stopped_at": "stopped",
            "parent_timestamp": "20260427190009",
        }))
        .unwrap(),
    )
    .unwrap();
    fs::rename(&staged, artifact.join("agent_meta.json")).unwrap();
    let replaced = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&replaced, &legacy);
    assert!(replaced.records.is_empty());

    // Delete and recreate: a new directory identity must not reuse the old
    // verdict, and a full scan then drops entries for removed paths.
    fs::remove_dir_all(&artifact).unwrap();
    let pruned = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert!(pruned.records.is_empty());
    let cache = read_cache_entries(&projects);
    assert_eq!(
        cache.get("entries").unwrap().as_object().unwrap().len(),
        0,
        "a full enumeration discards entries for removed paths"
    );

    write_json(
        &artifact.join("agent_meta.json"),
        json!({"name": "recreated"}),
    );
    let recreated = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let recreated_legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options,
        process_missing,
    );
    assert_scan_data_equal(&recreated, &recreated_legacy);
    assert_eq!(recreated.records.len(), 1);
}

#[test]
fn added_or_removed_markers_defeat_cached_verdicts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    // No PID: a dead-PID running marker would still prune, so the stopped
    // row carries no PID and every live marker below re-admits it.
    let stopped = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );

    let options = capacity_options();
    let process_missing = |pid| pid == DEAD_PID as i32;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    assert!(cold.records.is_empty());

    // Each live marker takes the full conservative path again.
    let live_pid = i64::from(std::process::id());
    for (marker, payload) in [
        ("running.json", json!({"pid": live_pid})),
        ("waiting.json", json!({"slot_requested_at": "t"})),
        ("pending_question.json", json!({"id": "q"})),
    ] {
        write_json(&stopped.join(marker), payload);
        let marked = scan_agent_artifacts_with_process_observation(
            &projects,
            options.clone(),
            process_missing,
        );
        let legacy = scan_agent_artifacts_legacy_capacity_for_test(
            &projects,
            options.clone(),
            process_missing,
        );
        assert_scan_data_equal(&marked, &legacy);
        assert_eq!(
            marked.records.len(),
            1,
            "a {marker} must re-admit the cached candidate"
        );
        fs::remove_file(stopped.join(marker)).unwrap();
        let unmarked = scan_agent_artifacts_with_process_observation(
            &projects,
            options.clone(),
            process_missing,
        );
        assert!(
            unmarked.records.is_empty(),
            "removing the {marker} restores the cached verdict"
        );
    }

    // Replacing a marker keeps the row live: the verdict follows the
    // current filesystem, not the cached bytes.
    write_json(
        &stopped.join("waiting.json"),
        json!({"slot_requested_at": "u"}),
    );
    let replaced_marker = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&replaced_marker, &legacy);
    assert_eq!(replaced_marker.records.len(), 1);
}

#[test]
fn stopped_and_closure_key_changes_invalidate() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    // The stopped row is the live row's ancestor, so closure re-admits
    // it; the owner key and clan tribe give the same treatment.
    let live = write_capacity_meta(
        &projects,
        "20260427190001",
        json!({
            "name": "live",
            "pid": std::process::id(),
            "parent_timestamp": "20260427190000",
            "runner_claim_owner_key": "owner-a",
            "agent_clan": "research",
            "agent_clan_generation": "g1",
        }),
    );
    let stopped = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({
            "name": "stopped",
            "stopped_at": "stopped",
            "agent_clan": "research",
            "agent_clan_generation": "g1",
            "clan_tribe": "night shift",
            "runner_claim_owner_key": "owner-a",
        }),
    );
    assert!(live.is_dir() && stopped.is_dir());

    let options = capacity_options();
    let process_missing = |_pid| false;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    // The stopped row is retained only through closure with the live row.
    assert_eq!(cold.records.len(), 2);

    // Clearing the terminal field revives then drops the row only via the
    // conservative path; parity with the legacy oracle must hold either
    // way.
    write_json(
        &stopped.join("agent_meta.json"),
        json!({
            "name": "unstopped",
            "parent_timestamp": "20260427190001",
            "pid": DEAD_PID,
        }),
    );
    let unstopped = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        |pid| pid == DEAD_PID as i32,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        |pid| pid == DEAD_PID as i32,
    );
    assert_scan_data_equal(&unstopped, &legacy);

    // Closure/context key changes invalidate just as content changes do.
    write_json(
        &stopped.join("agent_meta.json"),
        json!({
            "name": "stopped",
            "stopped_at": "stopped",
            "parent_timestamp": "20260427190001",
            "agent_clan": "research",
            "agent_clan_generation": "g1",
            "clan_tribe": "day shift",
            "runner_claim_owner_key": "owner-b",
        }),
    );
    let rekeyed = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let rekeyed_legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&rekeyed, &rekeyed_legacy);
    assert!(
        rekeyed
            .clan_context
            .iter()
            .any(|context| context.clan_tribe.as_deref() == Some("day shift")),
        "context key changes must be visible, never served stale"
    );
}

#[test]
fn pid_only_verdicts_are_reprobed_every_scan() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "dead-pid", "pid": DEAD_PID}),
    );

    let options = capacity_options();
    let dead =
        cold_scan(&projects, options.clone(), |pid| pid == DEAD_PID as i32);
    assert!(dead.records.is_empty());

    // The same bytes with a live PID observation must retain the row: a
    // cached dead-PID observation can never become a permanent exclusion.
    let revived = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        |_pid| false,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        |_pid| false,
    );
    assert_scan_data_equal(&revived, &legacy);
    assert_eq!(revived.records.len(), 1);

    // And a stopped row stays excluded across PID-observation changes
    // without rereading its verdict from the PID.
    let stopped_dir = capacity_artifact_dir(&projects, "20260427190001");
    write_json(
        &stopped_dir.join("agent_meta.json"),
        json!({"name": "stopped", "stopped_at": "stopped", "pid": DEAD_PID}),
    );
    let stopped_dead = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        |pid| pid == DEAD_PID as i32,
    );
    let stopped_live_view = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        |_pid| false,
    );
    // The PID-only row is revived under the live observation while the
    // stopped row stays excluded in both views.
    assert!(stopped_dead.records.is_empty());
    assert_eq!(stopped_live_view.records.len(), 1);
    assert!(
        stopped_live_view
            .records
            .iter()
            .all(|record| record.artifact_dir != stopped_dir.to_string_lossy()),
        "the stopped row stays excluded across PID-observation changes"
    );
    let stopped_legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        |_pid| false,
    );
    assert_scan_data_equal(&stopped_live_view, &stopped_legacy);
}

#[test]
fn closure_readmits_cached_negatives_with_a_full_parse() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let stopped_timestamp = "20260427190000";
    let live_timestamp = "20260427190001";
    let stopped = write_capacity_meta(
        &projects,
        stopped_timestamp,
        json!({
            "name": "stopped-ancestor",
            "stopped_at": "stopped",
        }),
    );
    write_capacity_meta(
        &projects,
        live_timestamp,
        json!({
            "name": "live",
            "pid": std::process::id(),
            "parent_timestamp": stopped_timestamp,
        }),
    );

    let options = capacity_options();
    let process_missing = |_pid| false;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    assert_eq!(cold.records.len(), 2);
    assert!(
        cold.records
            .iter()
            .any(|record| record.artifact_dir == stopped.to_string_lossy()),
        "the stopped ancestor is retained through closure"
    );

    // The warm scan serves the stopped projection from the cache, then
    // re-admits it through closure with a full parse: identical records.
    let warm = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&warm, &cold);
    let retained = warm
        .records
        .iter()
        .find(|record| record.artifact_dir == stopped.to_string_lossy())
        .expect("closure must re-admit the cached negative");
    assert_eq!(
        retained.agent_meta.as_ref().unwrap().stopped_at.as_deref(),
        Some("stopped"),
        "the re-admitted row is fully parsed, not served from the cache"
    );
}

#[test]
fn exact_and_filtered_scans_add_hits_but_never_prune() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let keep = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "keep", "stopped_at": "stopped"}),
    );
    let removed = write_capacity_meta(
        &projects,
        "20260427190001",
        json!({"name": "removed", "stopped_at": "stopped"}),
    );

    let options = capacity_options();
    let process_missing = |_pid| false;
    let cold = cold_scan(&projects, options.clone(), process_missing);
    assert!(cold.records.is_empty());
    assert_eq!(
        read_cache_entries(&projects)
            .get("entries")
            .unwrap()
            .as_object()
            .unwrap()
            .len(),
        2
    );
    fs::remove_dir_all(&removed).unwrap();

    // An exact scan over the surviving directory adds hits but must not
    // treat its subset as the complete archive.
    let exact = scan_agent_artifact_dirs_with_process_observation(
        &projects,
        std::slice::from_ref(&keep),
        options.clone(),
        process_missing,
    );
    let exact_legacy = scan_agent_artifact_dirs_legacy_capacity_for_test(
        &projects,
        std::slice::from_ref(&keep),
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&exact, &exact_legacy);
    assert!(
        read_cache_entries(&projects)
            .get("entries")
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .any(|key| key.ends_with("20260427190001")),
        "exact scans must not prune entries they did not visit"
    );

    // A filtered scan is the same: hits are welcome, pruning is not.
    let mut filtered_options = options.clone();
    filtered_options.only_projects = vec!["proj".to_string()];
    let filtered = scan_agent_artifacts_with_process_observation(
        &projects,
        filtered_options,
        process_missing,
    );
    assert!(filtered.records.is_empty());
    assert!(
        read_cache_entries(&projects)
            .get("entries")
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .any(|key| key.ends_with("20260427190001")),
        "filtered scans must not prune entries they did not visit"
    );
}

#[test]
fn symlinked_metadata_is_classified_but_never_cached() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );
    let target = artifact.join("agent_meta.real.json");
    fs::rename(artifact.join("agent_meta.json"), &target).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, artifact.join("agent_meta.json"))
        .unwrap();

    let options = capacity_options();
    let process_missing = |_pid| false;
    let scan = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options,
        process_missing,
    );
    assert_scan_data_equal(&scan, &legacy);

    // Symlinked metadata is classified by a best-effort read but never
    // publishes a reusable entry: either no cache file exists or it holds
    // no entries.
    #[cfg(unix)]
    {
        if capacity_cache_path(&projects).exists() {
            let cache = read_cache_entries(&projects);
            assert_eq!(
                cache.get("entries").unwrap().as_object().unwrap().len(),
                0,
                "symlinked metadata must never publish a reusable entry"
            );
        }
    }
}

#[test]
fn concurrent_full_and_subset_writers_stay_valid() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let mut dirs = Vec::new();
    for index in 0..16 {
        let timestamp = format!("20260427{:06}", 190000 + index);
        dirs.push(write_capacity_meta(
            &projects,
            &timestamp,
            json!({
                "name": format!("stopped-{index}"),
                "stopped_at": "stopped",
                "agent_clan": "proof-clan",
                "agent_clan_generation": "g1",
            }),
        ));
    }
    let live_timestamp = "20260427190100";
    let live = write_capacity_meta(
        &projects,
        live_timestamp,
        json!({
            "name": "live-child",
            "pid": std::process::id(),
            "parent_timestamp": "20260427190000",
            "agent_clan": "proof-clan",
            "agent_clan_generation": "g1",
        }),
    );
    dirs.push(live.clone());

    let options = capacity_options();
    let live_pid_barrier = Barrier::new(3);
    let exact_dirs = vec![dirs[0].clone(), live.clone()];
    let filtered_options = AgentArtifactScanOptionsWire {
        only_projects: vec!["proj".to_owned()],
        only_workflow_dirs: vec!["ace-run".to_owned()],
        ..options.clone()
    };

    std::thread::scope(|scope| {
        let full_projects = &projects;
        let full_options = options.clone();
        let full_barrier = &live_pid_barrier;
        let full = scope.spawn(move || {
            scan_agent_artifacts_with_process_observation(
                full_projects,
                full_options,
                move |_pid| {
                    full_barrier.wait();
                    false
                },
            )
        });

        let exact_projects = &projects;
        let exact_options = options.clone();
        let exact_dirs = exact_dirs.clone();
        let exact_barrier = &live_pid_barrier;
        let exact = scope.spawn(move || {
            scan_agent_artifact_dirs_with_process_observation(
                exact_projects,
                &exact_dirs,
                exact_options,
                move |_pid| {
                    exact_barrier.wait();
                    false
                },
            )
        });

        let filtered_projects = &projects;
        let filtered_barrier = &live_pid_barrier;
        let filtered = scope.spawn(move || {
            scan_agent_artifacts_with_process_observation(
                filtered_projects,
                filtered_options,
                move |_pid| {
                    filtered_barrier.wait();
                    false
                },
            )
        });

        let full = full.join().expect("full scan thread panicked");
        let exact = exact.join().expect("exact scan thread panicked");
        let filtered = filtered.join().expect("filtered scan thread panicked");

        let full_legacy = scan_agent_artifacts_legacy_capacity_for_test(
            &projects,
            options.clone(),
            |_pid| false,
        );
        assert_scan_data_equal(&full, &full_legacy);
        assert_eq!(
            full.records.len(),
            2,
            "live row and retained ancestor are present"
        );

        let exact_legacy = scan_agent_artifact_dirs_legacy_capacity_for_test(
            &projects,
            &[dirs[0].clone(), live.clone()],
            options.clone(),
            |_pid| false,
        );
        assert_scan_data_equal(&exact, &exact_legacy);
        assert_eq!(
            exact.records.len(),
            2,
            "exact scan includes closure ancestor"
        );

        let filtered_legacy = scan_agent_artifacts_legacy_capacity_for_test(
            &projects,
            AgentArtifactScanOptionsWire {
                only_projects: vec!["proj".to_owned()],
                only_workflow_dirs: vec!["ace-run".to_owned()],
                ..options.clone()
            },
            |_pid| false,
        );
        assert_scan_data_equal(&filtered, &filtered_legacy);
        assert_eq!(
            filtered.records.len(),
            2,
            "filtered scan includes closure ancestor"
        );
    });

    // Inspect the race output before a subsequent full scan can repair it.
    // Every worker loaded the missing cache before its live-row probe joined
    // the barrier, so each reached publication with a staged set.
    let raced_cache = read_cache_entries(&projects);
    assert_eq!(
        raced_cache.get("version").and_then(Value::as_u64),
        Some(u64::from(CAPACITY_CACHE_VERSION))
    );
    assert!(
        !raced_cache
            .get("entries")
            .unwrap()
            .as_object()
            .unwrap()
            .is_empty(),
        "a concurrent writer must have published staged entries"
    );
    assert!(
        load_capacity_cache(&projects).is_some(),
        "the raced file must parse as a complete current-root cache"
    );

    // A later full scan still agrees with its independent oracle and the
    // valid cache provides effective warm reuse.
    let after = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        |_pid| false,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options,
        |_pid| false,
    );
    assert_scan_data_equal(&after, &legacy);
    assert_eq!(after.records.len(), 2);
    let warm = scan_agent_artifacts_with_process_observation(
        &projects,
        capacity_options(),
        |_pid| false,
    );
    assert_scan_data_equal(
        &warm,
        &scan_agent_artifacts_legacy_capacity_for_test(
            &projects,
            capacity_options(),
            |_pid| false,
        ),
    );
    assert!(
        warm.stats.marker_files_parsed < 16,
        "warm reuse must avoid reparsing most stopped history"
    );
}

#[test]
fn missing_projects_root_stays_an_empty_scan_without_creating_caches() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("does-not-exist");
    let options = capacity_options();
    let scan = scan_agent_artifacts_with_process_observation(
        &projects,
        options,
        |_pid| false,
    );
    assert!(scan.records.is_empty());
    assert!(!capacity_cache_path(&projects).exists());
    assert!(!projects.exists());
}

#[test]
fn exact_capacity_scans_reuse_stopped_verdicts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let stopped = write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );

    let options = capacity_options();
    let process_missing = |_pid| false;
    let full = cold_scan(&projects, options.clone(), process_missing);
    assert!(full.records.is_empty());

    let exact = scan_agent_artifact_dirs_with_process_observation(
        &projects,
        &[stopped],
        options.clone(),
        process_missing,
    );
    let exact_legacy = scan_agent_artifact_dirs_legacy_capacity_for_test(
        &projects,
        &[capacity_artifact_dir(&projects, "20260427190000")],
        options,
        process_missing,
    );
    assert_scan_data_equal(&exact, &exact_legacy);
    assert!(exact.records.is_empty());
    assert_eq!(
        exact.stats.marker_files_parsed, 0,
        "exact scans reuse disk verdicts in a new observation"
    );
}

#[test]
fn cache_ignores_non_capacity_scans() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_capacity_meta(
        &projects,
        "20260427190000",
        json!({"name": "stopped", "stopped_at": "stopped"}),
    );

    let full_options = AgentArtifactScanOptionsWire::default();
    let full = scan_agent_artifacts(&projects, full_options);
    assert_eq!(full.records.len(), 1);
    assert!(
        !capacity_cache_path(&projects).exists(),
        "non-capacity scans must not create the verdict cache"
    );
}
