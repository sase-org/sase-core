//! Opt-in archive-scale budget proof for capacity scans.
//!
//! Run with:
//! `just test -p sase_core --release capacity_scan_archive_cost -- --ignored --nocapture`

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::tempdir;

use super::tests::{capacity_artifact_dir, write_json};
use super::*;
use crate::agent_scan::capacity_cache::capacity_cache_path;
use crate::agent_scan::layout::canonical_agent_artifact_path;

const CHILD_ROOT_ENV: &str = "SASE_CORE_CAPACITY_BENCH_CHILD_ROOT";
const CHILD_RESULT_ENV: &str = "SASE_CORE_CAPACITY_BENCH_CHILD_RESULT";
const LIVE_ROWS: usize = 100;
const CLOSURE_ROWS: usize = 75;
const RETAINED_ROWS: usize = LIVE_ROWS + CLOSURE_ROWS;
// This cardinality makes the preserved legacy resolver's clone-all-members
// cost visible (archive rows x represented generations) without making the
// optimized resolver's grouped pass depend on archive size per generation.
const CLAN_COUNT: usize = 128;
const GENERATIONS_PER_CLAN: usize = 32;
const CLAN_GENERATION_COUNT: usize = CLAN_COUNT * GENERATIONS_PER_CLAN;
const ARCHIVE_ROWS: usize = 20_000;
const DONE_ROWS: usize = 2_500;
const LARGE_ARCHIVE_ROWS: usize = 40_000;
const BENCH_REPEATS: usize = 3;

#[derive(Debug, Serialize, Deserialize)]
struct FreshProcessResult {
    elapsed_micros: u128,
    scan: AgentArtifactScanWire,
}

#[test]
#[ignore = "release-mode archive benchmark; run the command in the module docs"]
fn capacity_scan_archive_cost() {
    if run_fresh_process_child_if_requested() {
        return;
    }

    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    build_archive(&projects, ARCHIVE_ROWS, DONE_ROWS);
    let options = capacity_options();
    let process_is_missing = |_pid| false;

    let (baseline, baseline_samples) = measure(BENCH_REPEATS, || {
        scan_agent_artifacts_legacy_capacity_for_test(
            &projects,
            options.clone(),
            process_is_missing,
        )
    });
    assert_eq!(baseline.records.len(), RETAINED_ROWS);
    assert_eq!(
        baseline.clan_context.len(),
        CLAN_GENERATION_COUNT,
        "the fixture must exercise many clan generations"
    );
    assert_eq!(
        baseline.stats.artifact_dirs_visited,
        (ARCHIVE_ROWS + DONE_ROWS) as u64
    );
    let expected_baseline_marker_parses = (ARCHIVE_ROWS * 2 + LIVE_ROWS) as u64;
    assert_eq!(
        baseline.stats.marker_files_parsed,
        expected_baseline_marker_parses,
        "legacy full-record work must parse metadata and workflow state for every historical row"
    );

    let (cold, cold_samples) = measure(BENCH_REPEATS, || {
        let _ = fs::remove_file(capacity_cache_path(&projects));
        scan_agent_artifacts_with_process_observation(
            &projects,
            options.clone(),
            process_is_missing,
        )
    });
    assert_scan_data_equal(&cold, &baseline);
    let expected_cold_marker_parses = (ARCHIVE_ROWS
        + (RETAINED_ROWS - LIVE_ROWS)
        + RETAINED_ROWS
        + LIVE_ROWS) as u64;
    assert_eq!(
        cold.stats.marker_files_parsed,
        expected_cold_marker_parses,
        "cold scan should project all candidates and fully hydrate only retained rows"
    );
    assert!(capacity_cache_path(&projects).is_file());

    let (warm, warm_samples) = measure(BENCH_REPEATS, || {
        scan_agent_artifacts_with_process_observation(
            &projects,
            options.clone(),
            process_is_missing,
        )
    });
    assert_scan_data_equal(&warm, &baseline);
    let expected_warm_marker_parses = (RETAINED_ROWS * 2 + LIVE_ROWS) as u64;
    assert_eq!(
        warm.stats.marker_files_parsed, expected_warm_marker_parses,
        "warm scan must not reread metadata or hydrate pruned history"
    );

    let fresh_process = fresh_process_cache_hit(&projects);
    assert_scan_data_equal(&fresh_process.scan, &baseline);
    assert_eq!(
        fresh_process.scan.stats.marker_files_parsed,
        expected_warm_marker_parses,
        "a fresh process must reuse persisted terminal verdicts"
    );

    let baseline_median = median(&baseline_samples);
    let cold_median = median(&cold_samples);
    let warm_median = median(&warm_samples);
    let cold_speedup =
        baseline_median.as_secs_f64() / cold_median.as_secs_f64();
    let cache_bytes =
        fs::metadata(capacity_cache_path(&projects)).unwrap().len();
    eprintln!(
        "capacity_scan_archive_cost: rows={} done={} retained={} clans={} repeats={} cache_bytes={} baseline_full_records={} baseline_metadata_parses={} cold_metadata_parses={} cold_full_records={} warm_metadata_parses={} warm_full_records={} baseline_marker_parses={} cold_marker_parses={} warm_marker_parses={} fresh_process_ms={:.3} baseline_median_ms={:.3} cold_median_ms={:.3} warm_median_ms={:.3} cold_speedup={:.2}x",
        ARCHIVE_ROWS,
        DONE_ROWS,
        RETAINED_ROWS,
        CLAN_GENERATION_COUNT,
        BENCH_REPEATS,
        cache_bytes,
        ARCHIVE_ROWS,
        ARCHIVE_ROWS,
        ARCHIVE_ROWS + CLOSURE_ROWS,
        RETAINED_ROWS,
        RETAINED_ROWS,
        RETAINED_ROWS,
        baseline.stats.marker_files_parsed,
        cold.stats.marker_files_parsed,
        warm.stats.marker_files_parsed,
        fresh_process.elapsed_micros as f64 / 1_000.0,
        baseline_median.as_secs_f64() * 1_000.0,
        cold_median.as_secs_f64() * 1_000.0,
        warm_median.as_secs_f64() * 1_000.0,
        cold_speedup,
    );
    assert!(
        cold_speedup >= 10.0,
        "cold capacity scan must be at least 10x faster than legacy; got {cold_speedup:.2}x (legacy {baseline_median:?}, cold {cold_median:?})"
    );
    assert!(
        warm_median < Duration::from_secs(1),
        "warm archive scan must remain below 1 second; got {warm_median:?}"
    );

    // The larger warm archive catches a regression to full parsing or
    // archive-wide context cloning while keeping the same retained set.
    let larger_projects = tmp.path().join("larger-projects");
    build_archive(&larger_projects, LARGE_ARCHIVE_ROWS, DONE_ROWS);
    let larger_cold = scan_agent_artifacts_with_process_observation(
        &larger_projects,
        options.clone(),
        process_is_missing,
    );
    assert_eq!(larger_cold.records.len(), RETAINED_ROWS);
    assert_eq!(
        larger_cold.stats.marker_files_parsed,
        (LARGE_ARCHIVE_ROWS
            + (RETAINED_ROWS - LIVE_ROWS)
            + RETAINED_ROWS
            + LIVE_ROWS) as u64
    );
    let (larger_warm, larger_warm_samples) = measure(BENCH_REPEATS, || {
        scan_agent_artifacts_with_process_observation(
            &larger_projects,
            options.clone(),
            process_is_missing,
        )
    });
    assert_scan_data_equal(&larger_warm, &larger_cold);
    assert_eq!(
        larger_warm.stats.marker_files_parsed,
        expected_warm_marker_parses,
        "doubling archive history must not increase warm metadata or record work"
    );
    let larger_warm_median = median(&larger_warm_samples);
    assert!(
        larger_warm_median < Duration::from_secs(2),
        "larger archive warm scan must remain below 2 seconds; got {larger_warm_median:?}"
    );
    eprintln!(
        "capacity_scan_archive_cost_large: rows={} done={} retained={} clans={} repeats={} cache_bytes={} warm_marker_parses={} warm_median_ms={:.3}",
        LARGE_ARCHIVE_ROWS,
        DONE_ROWS,
        RETAINED_ROWS,
        CLAN_GENERATION_COUNT,
        BENCH_REPEATS,
        fs::metadata(capacity_cache_path(&larger_projects))
            .unwrap()
            .len(),
        larger_warm.stats.marker_files_parsed,
        larger_warm_median.as_secs_f64() * 1_000.0,
    );
}

fn run_fresh_process_child_if_requested() -> bool {
    let (Ok(projects), Ok(result_path)) = (
        std::env::var(CHILD_ROOT_ENV),
        std::env::var(CHILD_RESULT_ENV),
    ) else {
        return false;
    };

    let started = Instant::now();
    let scan = scan_agent_artifacts_with_process_observation(
        Path::new(&projects),
        capacity_options(),
        |_pid| false,
    );
    let result = FreshProcessResult {
        elapsed_micros: started.elapsed().as_micros(),
        scan,
    };
    fs::write(result_path, serde_json::to_vec(&result).unwrap()).unwrap();
    true
}

fn fresh_process_cache_hit(projects: &Path) -> FreshProcessResult {
    let result_path =
        projects.parent().unwrap().join("fresh-process-result.json");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("capacity_scan_archive_cost")
        .arg("--ignored")
        .arg("--nocapture")
        .env(CHILD_ROOT_ENV, projects)
        .env(CHILD_RESULT_ENV, &result_path)
        .status()
        .expect("start a fresh test process for the persisted cache hit");
    assert!(status.success(), "fresh-process cache scan failed");
    let bytes =
        fs::read(result_path).expect("fresh process writes scan results");
    serde_json::from_slice(&bytes).expect("fresh-process result is valid JSON")
}

fn capacity_options() -> AgentArtifactScanOptionsWire {
    AgentArtifactScanOptionsWire {
        capacity_only: true,
        ..AgentArtifactScanOptionsWire::default()
    }
}

fn build_archive(projects: &Path, historical_rows: usize, done_rows: usize) {
    let metadata_history = (0..8)
        .map(|turn| {
            json!({
                "turn": turn,
                "input": "x".repeat(192),
                "output": "y".repeat(192),
            })
        })
        .collect::<Vec<_>>();
    let stopped_state = workflow_state_bytes(false);
    let live_state = workflow_state_bytes(true);
    let dead_pid = 1_700_000_000_i64;

    for index in 0..historical_rows {
        let timestamp = timestamp_for(index);
        let artifact_dir = artifact_dir(projects, &timestamp, index);
        fs::create_dir_all(&artifact_dir).unwrap();
        let clan = index % CLAN_COUNT;
        let generation = (index / CLAN_COUNT) % GENERATIONS_PER_CLAN;
        let is_live = index < LIVE_ROWS;
        let is_closure = (LIVE_ROWS..RETAINED_ROWS).contains(&index);
        let mut meta = serde_json::Map::new();
        meta.insert("name".to_string(), json!(format!("agent-{timestamp}")));
        meta.insert("workflow_name".to_string(), json!("run"));
        meta.insert("agent_clan".to_string(), json!(format!("clan-{clan:02}")));
        meta.insert(
            "agent_clan_generation".to_string(),
            json!(format!("generation-{generation:02}")),
        );
        meta.insert("recent_activity".to_string(), json!(metadata_history));
        if index.is_multiple_of(13) {
            meta.insert(
                "clan_summary".to_string(),
                json!(format!("summary from launch {timestamp}")),
            );
        }
        if is_live {
            meta.insert(
                "pid".to_string(),
                json!(i64::from(std::process::id())),
            );
            meta.insert(
                "parent_timestamp".to_string(),
                json!(timestamp_for(RETAINED_ROWS - 1)),
            );
            meta.insert(
                "runner_claim_owner_key".to_string(),
                json!(format!("owner-{}", index % CLOSURE_ROWS)),
            );
        } else {
            meta.insert(
                "stopped_at".to_string(),
                json!("2026-11-12T12:00:00Z"),
            );
            if is_closure {
                if index > LIVE_ROWS {
                    meta.insert(
                        "parent_timestamp".to_string(),
                        json!(timestamp_for(index - 1)),
                    );
                }
                meta.insert(
                    "runner_claim_owner_key".to_string(),
                    json!(format!("owner-{}", index - LIVE_ROWS)),
                );
            } else if index.is_multiple_of(29) {
                meta.insert("pid".to_string(), json!(dead_pid));
            }
        }
        write_json(&artifact_dir.join("agent_meta.json"), Value::Object(meta));
        fs::write(
            artifact_dir.join("workflow_state.json"),
            if is_live { &live_state } else { &stopped_state },
        )
        .unwrap();
        if is_live {
            write_json(
                &artifact_dir.join("running.json"),
                json!({"pid": std::process::id(), "started_at": "2026-11-12T12:00:00Z"}),
            );
        }
    }

    for offset in 0..done_rows {
        let index = historical_rows + offset;
        let timestamp = timestamp_for(index);
        let artifact_dir = artifact_dir(projects, &timestamp, index);
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": timestamp}),
        );
        write_json(&artifact_dir.join("done.json"), json!({"outcome": "done"}));
    }
}

fn workflow_state_bytes(appears_as_agent: bool) -> Vec<u8> {
    let steps = (0..16)
        .map(|index| {
            json!({
                "name": format!("workflow-step-{index}"),
                "status": "completed",
                "output": "z".repeat(192),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "appears_as_agent": appears_as_agent,
        "workflow_name": "archive-benchmark",
        "steps": steps,
    }))
    .unwrap()
}

fn artifact_dir(projects: &Path, timestamp: &str, index: usize) -> PathBuf {
    if index.is_multiple_of(2) {
        canonical_agent_artifact_path(projects, "proj", "ace-run", timestamp)
    } else {
        capacity_artifact_dir(projects, timestamp)
    }
}

fn timestamp_for(index: usize) -> String {
    // Use day 1-28 so every generated date is valid across all months. Each
    // combination is unique, including in the larger 40k-directory fixture.
    let year = 2026 + index / (12 * 28);
    let month = (index / 28) % 12 + 1;
    let day = index % 28 + 1;
    format!("{year:04}{month:02}{day:02}120000")
}

fn measure<T>(
    repeats: usize,
    mut run: impl FnMut() -> T,
) -> (T, Vec<Duration>) {
    let mut samples = Vec::with_capacity(repeats);
    let mut last = None;
    for _ in 0..repeats {
        let started = Instant::now();
        last = Some(run());
        samples.push(started.elapsed());
    }
    (last.unwrap(), samples)
}

fn median(samples: &[Duration]) -> Duration {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    ordered[ordered.len() / 2]
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
