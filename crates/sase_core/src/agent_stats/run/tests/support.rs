//! Shared fixtures for run-stats tests.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::DateTime;
use serde_json::{json, Value};

use super::super::super::wire::{
    AgentRunStatsRequestWire, AgentRunnerStatsWire,
    AgentStatsRuntimeGroupByWire,
};
use super::super::*;

pub(super) fn write_json(path: &Path, payload: Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
}

pub(super) fn artifact(root: &Path, timestamp: &str) -> PathBuf {
    artifact_for_project(root, "proj", timestamp)
}

pub(super) fn artifact_for_project(
    root: &Path,
    project: &str,
    timestamp: &str,
) -> PathBuf {
    artifact_for_workflow(root, project, "ace-run", timestamp)
}

pub(super) fn artifact_for_workflow(
    root: &Path,
    project: &str,
    workflow: &str,
    timestamp: &str,
) -> PathBuf {
    root.join(project)
        .join("artifacts")
        .join(workflow)
        .join(timestamp)
}

pub(super) fn finish_at(timestamp: &str, seconds: f64) -> f64 {
    parse_timestamp(timestamp).unwrap() + seconds
}

pub(super) fn add_run(
    projects: &Path,
    timestamp: &str,
    meta: Value,
    done: Option<Value>,
    waiting: bool,
) -> PathBuf {
    let dir = artifact(projects, timestamp);
    write_json(&dir.join("agent_meta.json"), meta);
    if let Some(done) = done {
        write_json(&dir.join("done.json"), done);
    }
    if waiting {
        write_json(&dir.join("waiting.json"), json!({"waiting_for": []}));
    }
    dir
}

pub(super) fn add_project_run(
    projects: &Path,
    project: &str,
    timestamp: &str,
    meta: Value,
    done: Option<Value>,
    waiting: bool,
) -> PathBuf {
    let dir = artifact_for_project(projects, project, timestamp);
    write_json(&dir.join("agent_meta.json"), meta);
    if let Some(done) = done {
        write_json(&dir.join("done.json"), done);
    }
    if waiting {
        write_json(&dir.join("waiting.json"), json!({"waiting_for": []}));
    }
    dir
}

pub(super) fn request() -> AgentRunStatsRequestWire {
    AgentRunStatsRequestWire {
        start_ts: parse_timestamp("2026-07-10T00:00:00Z").unwrap() as i64,
        end_ts: parse_timestamp("2026-07-11T00:00:00Z").unwrap() as i64,
        runtime_group_by: AgentStatsRuntimeGroupByWire::Agent,
        bucket_seconds: 6 * 60 * 60,
        top_n: 10,
        project: None,
        work_top_n: 50,
        xprompt_top_n: 40,
        xprompt_breakdown_top_n: 5,
        xprompt_focus: None,
    }
}

pub(super) const RUNNER_BASE: i64 = 1_783_641_600; // 2026-07-10T00:00:00Z

pub(super) fn runner_time(offset: i64) -> String {
    DateTime::from_timestamp(RUNNER_BASE + offset, 0)
        .unwrap()
        .to_rfc3339()
}

pub(super) fn runner_artifact_timestamp(offset: i64) -> String {
    DateTime::from_timestamp(RUNNER_BASE + offset, 0)
        .unwrap()
        .format("%Y%m%d%H%M%S")
        .to_string()
}

pub(super) fn runner_request(
    start: i64,
    end: i64,
    bucket_seconds: u64,
) -> AgentRunStatsRequestWire {
    AgentRunStatsRequestWire {
        start_ts: RUNNER_BASE + start,
        end_ts: RUNNER_BASE + end,
        runtime_group_by: AgentStatsRuntimeGroupByWire::Agent,
        bucket_seconds,
        top_n: 10,
        project: None,
        work_top_n: 50,
        xprompt_top_n: 40,
        xprompt_breakdown_top_n: 5,
        xprompt_focus: None,
    }
}

pub(super) fn assert_runner_conservation(runners: &AgentRunnerStatsWire) {
    let span = runners.end_ts - runners.start_ts;
    let distribution_seconds = runners
        .distribution
        .iter()
        .map(|row| row.seconds)
        .sum::<f64>();
    let weighted_seconds = runners
        .distribution
        .iter()
        .map(|row| row.runners as f64 * row.seconds)
        .sum::<f64>();
    let idle_seconds = runners.distribution[0].seconds;
    assert!((distribution_seconds - span).abs() < 1e-9);
    assert!((weighted_seconds - runners.runner_seconds).abs() < 1e-9);
    assert!((runners.busy_seconds + idle_seconds - span).abs() < 1e-9);
    assert!(
        (runners
            .distribution
            .iter()
            .map(|row| row.share)
            .sum::<f64>()
            - 1.0)
            .abs()
            < 1e-9
    );
    assert!(
        (runners
            .trend
            .iter()
            .map(|slice| slice.runner_seconds)
            .sum::<f64>()
            - runners.runner_seconds)
            .abs()
            < 1e-9
    );
}
