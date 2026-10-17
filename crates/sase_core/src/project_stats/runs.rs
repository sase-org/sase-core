//! Runs and Patch aggregation over this machine's agent-run index.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::agent_stats::{
    query_run_stats, AgentPatchWorkStatsWire, AgentProjectWorkStatsWire,
    AgentRunStatsRequestWire, AgentStatsRuntimeGroupByWire, AgentWorkStatsWire,
};
use crate::project_stats::wire::{
    ProjectStatsCoverageWire, ProjectStatsFacetWire, ProjectStatsIssueWire,
    ProjectStatsPatchWire, ProjectStatsProjectSpecWire,
    ProjectStatsProjectStateWire, ProjectStatsProjectWire,
    ProjectStatsRequestWire, ProjectStatsRunsWire, ProjectStatsSelectionWire,
};

/// Large enough that typical Patch tables stay complete; remainder is
/// reported through `truncated_patch_rows` and an issue.
const RUNS_WORK_TOP_N: u32 = 10_000;

pub(crate) struct RunsSnapshot {
    pub available: bool,
    by_index_name: BTreeMap<String, AgentProjectWorkStatsWire>,
    patches_by_index_name: BTreeMap<String, Vec<AgentPatchWorkStatsWire>>,
    truncated_patch_rows: u64,
    pub issues: Vec<ProjectStatsIssueWire>,
    pub source_tokens: Vec<String>,
}

pub(crate) struct MappedProjectRuns {
    pub runs: Option<ProjectStatsRunsWire>,
    pub patches: Vec<ProjectStatsPatchWire>,
    pub truncated_patch_rows: u64,
}

pub(crate) fn needs_snapshot(request: &ProjectStatsRequestWire) -> bool {
    wants_runs(request)
        || request.include_run_only_projects
        || request.projects.iter().any(|project| {
            project.selection == ProjectStatsSelectionWire::IfRuns
        })
}

pub(crate) fn wants_runs(request: &ProjectStatsRequestWire) -> bool {
    request.facets.contains(&ProjectStatsFacetWire::Runs)
}

pub(crate) fn load(request: &ProjectStatsRequestWire) -> RunsSnapshot {
    let path = request
        .run_index_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty());
    let Some(path) = path else {
        return missing(None, "run index path is unset");
    };
    let token = index_token(path);
    if request.end_ts <= request.start_ts {
        return RunsSnapshot {
            available: true,
            by_index_name: BTreeMap::new(),
            patches_by_index_name: BTreeMap::new(),
            truncated_patch_rows: 0,
            issues: Vec::new(),
            source_tokens: vec![token],
        };
    }
    let index_path = Path::new(path);
    if !index_path.is_file() {
        return missing(Some(path), "run index file is missing");
    }
    let span = (request.end_ts - request.start_ts) as u64;
    let stats_request = AgentRunStatsRequestWire {
        start_ts: request.start_ts,
        end_ts: request.end_ts,
        runtime_group_by: AgentStatsRuntimeGroupByWire::Project,
        bucket_seconds: span.max(1),
        top_n: 10,
        project: None,
        work_top_n: RUNS_WORK_TOP_N,
        macro_top_n: 1,
        macro_breakdown_top_n: 1,
        macro_focus: None,
        excluded_commit_repo_names: request.record_repo_names.clone(),
    };
    match query_run_stats(index_path, stats_request) {
        Ok(response) => from_work(response.work, token),
        Err(error) => missing(Some(path), &error),
    }
}

pub(crate) fn include_project(
    project: &ProjectStatsProjectSpecWire,
    snapshot: Option<&RunsSnapshot>,
) -> bool {
    match project.selection {
        ProjectStatsSelectionWire::Always => true,
        ProjectStatsSelectionWire::IfRuns => match snapshot {
            None => true,
            Some(snapshot) if !snapshot.available => false,
            Some(snapshot) => mapped_run_count(snapshot, project) > 0,
        },
    }
}

pub(crate) fn map_project(
    snapshot: &RunsSnapshot,
    project: &ProjectStatsProjectSpecWire,
    wants_runs: bool,
) -> MappedProjectRuns {
    if !wants_runs {
        return MappedProjectRuns {
            runs: None,
            patches: Vec::new(),
            truncated_patch_rows: 0,
        };
    }
    if !snapshot.available {
        return MappedProjectRuns {
            runs: None,
            patches: Vec::new(),
            truncated_patch_rows: 0,
        };
    }
    let names = index_names(project);
    let rows: Vec<&AgentProjectWorkStatsWire> = names
        .iter()
        .filter_map(|name| snapshot.by_index_name.get(*name))
        .collect();
    let mut patches = Vec::new();
    for name in &names {
        if let Some(rows) = snapshot.patches_by_index_name.get(*name) {
            patches.extend(rows.iter().map(patch_wire));
        }
    }
    patches.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.name.cmp(&right.name))
    });
    MappedProjectRuns {
        runs: Some(fold_work_rows(&rows)),
        patches,
        truncated_patch_rows: 0,
    }
}

pub(crate) fn unregistered_projects(
    snapshot: &RunsSnapshot,
    claimed: &BTreeSet<&str>,
    wants_runs: bool,
) -> Vec<ProjectStatsProjectWire> {
    if !wants_runs || !snapshot.available {
        return Vec::new();
    }
    snapshot
        .by_index_name
        .values()
        .filter(|row| !claimed.contains(row.project.as_str()))
        .map(|row| {
            let patches = snapshot
                .patches_by_index_name
                .get(&row.project)
                .into_iter()
                .flatten()
                .map(patch_wire)
                .collect();
            ProjectStatsProjectWire {
                key: row.project.clone(),
                label: row.project.clone(),
                state: ProjectStatsProjectStateWire::Unregistered,
                output: None,
                lifetime: None,
                code: None,
                runs: Some(runs_from_work(row)),
                repos: Vec::new(),
                not_counted: Vec::new(),
                models: Vec::new(),
                patches,
                truncated_patch_rows: 0,
                coverage: ProjectStatsCoverageWire::default(),
            }
        })
        .collect()
}

pub(crate) fn truncation_issue(
    snapshot: &RunsSnapshot,
) -> Option<ProjectStatsIssueWire> {
    if !snapshot.available || snapshot.truncated_patch_rows == 0 {
        return None;
    }
    Some(ProjectStatsIssueWire {
        project: None,
        repo: None,
        code: "truncated_patch_rows".to_owned(),
        message: format!(
            "omitted {} Patch rows beyond the work cap",
            snapshot.truncated_patch_rows
        ),
        hint: None,
    })
}

pub(crate) fn claimed_index_names(
    projects: &[ProjectStatsProjectSpecWire],
) -> BTreeSet<&str> {
    projects
        .iter()
        .flat_map(|project| {
            project.index_project_names.iter().map(String::as_str)
        })
        .collect()
}

pub(crate) fn portfolio_runs(
    projects: &[ProjectStatsProjectWire],
) -> Option<ProjectStatsRunsWire> {
    let rows: Vec<&ProjectStatsRunsWire> = projects
        .iter()
        .filter_map(|project| project.runs.as_ref())
        .collect();
    if rows.is_empty() {
        return None;
    }
    Some(sum_runs(&rows))
}

fn from_work(work: AgentWorkStatsWire, token: String) -> RunsSnapshot {
    let mut patches_by_index_name: BTreeMap<
        String,
        Vec<AgentPatchWorkStatsWire>,
    > = BTreeMap::new();
    for row in work.patches {
        patches_by_index_name
            .entry(row.project.clone())
            .or_default()
            .push(row);
    }
    let mut by_index_name = BTreeMap::new();
    for row in work.projects {
        by_index_name.insert(row.project.clone(), row);
    }
    RunsSnapshot {
        available: true,
        by_index_name,
        patches_by_index_name,
        truncated_patch_rows: work.truncated_patch_rows,
        issues: Vec::new(),
        source_tokens: vec![token],
    }
}

fn missing(path: Option<&str>, message: &str) -> RunsSnapshot {
    let token = match path {
        Some(path) => index_token(path),
        None => serde_json::json!({
            "run_index": serde_json::Value::Null,
            "status": "missing",
        })
        .to_string(),
    };
    RunsSnapshot {
        available: false,
        by_index_name: BTreeMap::new(),
        patches_by_index_name: BTreeMap::new(),
        truncated_patch_rows: 0,
        issues: vec![ProjectStatsIssueWire {
            project: None,
            repo: None,
            code: "index_missing".to_owned(),
            message: message.to_owned(),
            hint: Some(
                "Pass a readable agent artifact index sqlite path.".to_owned(),
            ),
        }],
        source_tokens: vec![token],
    }
}

fn index_token(path: &str) -> String {
    match fs::metadata(path) {
        Ok(meta) => {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            serde_json::json!({
                "run_index": path,
                "mtime": mtime,
                "len": meta.len(),
            })
            .to_string()
        }
        Err(_) => serde_json::json!({
            "run_index": path,
            "status": "missing",
        })
        .to_string(),
    }
}

fn index_names(project: &ProjectStatsProjectSpecWire) -> Vec<&str> {
    project
        .index_project_names
        .iter()
        .map(String::as_str)
        .collect()
}

fn mapped_run_count(
    snapshot: &RunsSnapshot,
    project: &ProjectStatsProjectSpecWire,
) -> u64 {
    index_names(project)
        .into_iter()
        .filter_map(|name| snapshot.by_index_name.get(name))
        .map(|row| row.runs)
        .sum()
}

fn fold_work_rows(rows: &[&AgentProjectWorkStatsWire]) -> ProjectStatsRunsWire {
    if rows.is_empty() {
        return ProjectStatsRunsWire::default();
    }
    if rows.len() == 1 {
        return runs_from_work(rows[0]);
    }
    let mut runs = ProjectStatsRunsWire::default();
    for row in rows {
        runs.runs += row.runs;
        runs.completed += row.completed;
        runs.failed += row.failed;
        runs.other_terminal += row.other_terminal;
        runs.in_progress += row.in_progress;
        runs.waiting += row.waiting;
        runs.total_runtime_seconds += row.total_runtime_seconds;
        runs.unattributed_runs += row.unattributed_runs;
        runs.last_run_ts =
            max_ts(runs.last_run_ts, ts_opt(row.last_run_ts, row.runs));
        runs.distinct_patches += row.distinct_patches;
    }
    runs.success_rate = success_rate(runs.completed, runs.runs);
    runs
}

fn runs_from_work(row: &AgentProjectWorkStatsWire) -> ProjectStatsRunsWire {
    ProjectStatsRunsWire {
        runs: row.runs,
        completed: row.completed,
        failed: row.failed,
        other_terminal: row.other_terminal,
        in_progress: row.in_progress,
        waiting: row.waiting,
        success_rate: Some(row.success_rate),
        total_runtime_seconds: row.total_runtime_seconds,
        distinct_patches: row.distinct_patches,
        unattributed_runs: row.unattributed_runs,
        last_run_ts: ts_opt(row.last_run_ts, row.runs),
    }
}

fn sum_runs(rows: &[&ProjectStatsRunsWire]) -> ProjectStatsRunsWire {
    let mut runs = ProjectStatsRunsWire::default();
    for row in rows {
        runs.runs += row.runs;
        runs.completed += row.completed;
        runs.failed += row.failed;
        runs.other_terminal += row.other_terminal;
        runs.in_progress += row.in_progress;
        runs.waiting += row.waiting;
        runs.total_runtime_seconds += row.total_runtime_seconds;
        runs.distinct_patches += row.distinct_patches;
        runs.unattributed_runs += row.unattributed_runs;
        runs.last_run_ts = max_ts(runs.last_run_ts, row.last_run_ts);
    }
    runs.success_rate = success_rate(runs.completed, runs.runs);
    runs
}

fn patch_wire(row: &AgentPatchWorkStatsWire) -> ProjectStatsPatchWire {
    ProjectStatsPatchWire {
        name: row.name.clone(),
        status: row.status.clone(),
        has_pr: row.has_pr,
        runs: row.runs,
        completed: 0,
        failed: 0,
        run_commits: row.commits,
        total_runtime_seconds: row.total_runtime_seconds.round() as u64,
        first_run_ts: ts_opt(row.first_run_ts, row.runs),
        last_run_ts: ts_opt(row.last_run_ts, row.runs),
    }
}

fn ts_opt(ts: f64, runs: u64) -> Option<i64> {
    if runs == 0 {
        None
    } else {
        Some(ts.round() as i64)
    }
}

fn max_ts(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, right) => right,
    }
}

fn success_rate(completed: u64, runs: u64) -> Option<f64> {
    if runs == 0 {
        None
    } else {
        Some(completed as f64 / runs as f64)
    }
}
