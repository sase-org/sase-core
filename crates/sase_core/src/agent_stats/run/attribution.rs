//! Project and patch work attribution.
//!
//! Resolves each run to the patches it touched from commit metadata,
//! then folds and finishes the per-project and per-patch work tables,
//! including spec-file metadata.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::agent_scan::index::cl_name_is_unknownish;
use crate::agent_scan::AgentArtifactRecordWire;
use crate::parser::parse_project_bytes;
use crate::project_spec::{preferred_project_spec_path, project_spec_basename};

use super::super::wire::{
    AgentPatchWorkStatsWire, AgentProjectWorkStatsWire, AgentWorkStatsWire,
};
use super::finishing::ratio;
use super::types::*;

pub(super) fn resolve_run_attribution(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> RunAttribution {
    let meta_commits = record
        .done
        .as_ref()
        .and_then(|done| done.step_output.as_ref())
        .and_then(|output| output.get("meta_commits"))
        .and_then(|value| value.as_array());
    let mut total_commits = 0u64;
    let mut commit_patches = BTreeMap::<String, u64>::new();
    if let Some(meta_commits) = meta_commits {
        for commit in meta_commits.iter().filter_map(|value| value.as_object())
        {
            total_commits += 1;
            let name = commit
                .get("patch_name")
                .and_then(|value| value.as_str())
                .or_else(|| {
                    commit
                        .get("changespec_name")
                        .and_then(|value| value.as_str())
                })
                .or_else(|| {
                    commit
                        .get("commit_patch_name")
                        .and_then(|value| value.as_str())
                })
                .or_else(|| {
                    commit
                        .get("commit_changespec_name")
                        .and_then(|value| value.as_str())
                });
            if let Some(name) = real_patch_name(name, record, row) {
                *commit_patches.entry(name).or_default() += 1;
            }
        }
    }
    if !commit_patches.is_empty() {
        return RunAttribution {
            patches: commit_patches
                .into_iter()
                .map(|(name, commits)| AttributedPatch { name, commits })
                .collect(),
            total_commits,
        };
    }

    let commit_name = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.commit_changespec_name.as_deref());
    if let Some(name) = real_patch_name(commit_name, record, row) {
        return RunAttribution {
            patches: vec![AttributedPatch {
                name,
                commits: total_commits,
            }],
            total_commits,
        };
    }
    if let Some(name) = real_patch_name(row.cl_name.as_deref(), record, row) {
        return RunAttribution {
            patches: vec![AttributedPatch {
                name,
                commits: total_commits,
            }],
            total_commits,
        };
    }
    RunAttribution {
        patches: Vec::new(),
        total_commits,
    }
}

pub(super) fn real_patch_name(
    name: Option<&str>,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> Option<String> {
    let name = name.map(str::trim).filter(|value| !value.is_empty())?;
    if cl_name_is_unknownish(Some(name))
        || is_project_identity_placeholder(name, record, row)
    {
        return None;
    }
    Some(name.to_string())
}

pub(super) fn is_project_identity_placeholder(
    name: &str,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> bool {
    if name == row.project_name
        || name == project_spec_basename(&record.project_file)
    {
        return true;
    }
    let Some(github_name) = row.project_name.strip_prefix("gh_") else {
        return false;
    };
    let Some((owner, repo)) = github_name.split_once("__") else {
        return false;
    };
    name == repo || name == format!("{owner}/{repo}")
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fold_work(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
    agent: &str,
    launch_ts: f64,
    duration: Option<f64>,
    outcome: Option<&str>,
    attribution: &RunAttribution,
    work: &mut WorkAccumulators,
) {
    let project = work.projects.entry(row.project_name.clone()).or_default();
    let first_project_run = project.runs == 0;
    project.runs += 1;
    project.commits += attribution.total_commits;
    if first_project_run || launch_ts > project.last_run_ts {
        project.last_run_ts = launch_ts;
    }
    if project.project_file.as_os_str().is_empty() {
        project.project_file = PathBuf::from(&record.project_file);
    }
    match outcome {
        Some("completed") => project.completed += 1,
        Some("failed" | "epic_launch_failed") => project.failed += 1,
        Some(_) => project.other_terminal += 1,
        None if record.waiting.is_some() || row.status == "waiting" => {
            project.waiting += 1;
        }
        None => project.in_progress += 1,
    }
    if let Some(duration) = duration {
        project.total_runtime_seconds += duration;
    }
    if attribution.patches.is_empty() {
        project.unattributed_runs += 1;
        return;
    }

    for attributed in &attribution.patches {
        project.patches.insert(attributed.name.clone());
        let patch = work
            .patches
            .entry((row.project_name.clone(), attributed.name.clone()))
            .or_default();
        let first_patch_run = patch.runs == 0;
        patch.runs += 1;
        patch.agents.insert(agent.to_string());
        patch.commits += attributed.commits;
        if let Some(duration) = duration {
            patch.total_runtime_seconds += duration;
        }
        if first_patch_run || launch_ts < patch.first_run_ts {
            patch.first_run_ts = launch_ts;
        }
        if first_patch_run || launch_ts > patch.last_run_ts {
            patch.last_run_ts = launch_ts;
        }
    }
}

pub(super) fn finish_work(
    work: WorkAccumulators,
    work_top_n: usize,
) -> AgentWorkStatsWire {
    let WorkAccumulators { projects, patches } = work;
    let (metadata, malformed_spec_files_skipped) =
        load_patch_metadata(&projects);
    let unattributed_runs = projects
        .values()
        .map(|project| project.unattributed_runs)
        .sum();
    let mut project_rows = projects
        .into_iter()
        .map(|(project, value)| AgentProjectWorkStatsWire {
            project,
            runs: value.runs,
            completed: value.completed,
            failed: value.failed,
            other_terminal: value.other_terminal,
            in_progress: value.in_progress,
            waiting: value.waiting,
            success_rate: ratio(value.completed, value.runs),
            commits: value.commits,
            distinct_patches: value.patches.len() as u64,
            unattributed_runs: value.unattributed_runs,
            total_runtime_seconds: value.total_runtime_seconds,
            last_run_ts: value.last_run_ts,
        })
        .collect::<Vec<_>>();
    project_rows.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.project.cmp(&right.project))
    });

    let mut patch_rows = patches
        .into_iter()
        .map(|((project, name), value)| {
            let metadata = metadata.get(&(project.clone(), name.clone()));
            AgentPatchWorkStatsWire {
                project,
                name,
                status: metadata
                    .map(|value| value.status.clone())
                    .unwrap_or_else(|| UNKNOWN.to_string()),
                has_pr: metadata.is_some_and(|value| value.has_pr),
                runs: value.runs,
                distinct_agents: value.agents.len() as u64,
                commits: value.commits,
                total_runtime_seconds: value.total_runtime_seconds,
                first_run_ts: value.first_run_ts,
                last_run_ts: value.last_run_ts,
            }
        })
        .collect::<Vec<_>>();
    patch_rows.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.project.cmp(&right.project))
            .then_with(|| left.name.cmp(&right.name))
    });
    let truncated_patch_rows =
        patch_rows.len().saturating_sub(work_top_n) as u64;
    patch_rows.truncate(work_top_n);

    AgentWorkStatsWire {
        projects: project_rows,
        patches: patch_rows,
        unattributed_runs,
        truncated_patch_rows,
        malformed_spec_files_skipped,
    }
}

pub(super) fn load_patch_metadata(
    projects: &BTreeMap<String, ProjectWorkAccumulator>,
) -> (BTreeMap<(String, String), PatchMetadata>, u64) {
    let mut metadata = BTreeMap::new();
    let mut malformed = 0u64;
    for (project, value) in projects {
        if value.patches.is_empty() {
            continue;
        }
        let active = value.project_file.as_path();
        let basename = project_spec_basename(&active.to_string_lossy());
        let archive = active
            .parent()
            .map(|parent| preferred_project_spec_path(parent, &basename, true));
        let mut paths = vec![active.to_path_buf()];
        if let Some(archive) = archive.filter(|path| path != active) {
            paths.push(archive);
        }
        for path in paths {
            let content = match fs::read(&path) {
                Ok(content) => content,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    continue;
                }
                Err(_) => {
                    malformed += 1;
                    continue;
                }
            };
            let Ok(specs) =
                parse_project_bytes(&path.to_string_lossy(), &content)
            else {
                malformed += 1;
                continue;
            };
            for spec in specs {
                metadata.entry((project.clone(), spec.name)).or_insert_with(
                    || PatchMetadata {
                        status: spec.status,
                        has_pr: spec
                            .pr_url
                            .as_deref()
                            .is_some_and(|value| !value.trim().is_empty()),
                    },
                );
            }
        }
    }
    (metadata, malformed)
}
