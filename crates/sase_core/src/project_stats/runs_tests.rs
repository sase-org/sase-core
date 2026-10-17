use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use tempfile::TempDir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
};
use crate::agent_stats::{
    query_run_stats, AgentRunStatsRequestWire, AgentStatsRuntimeGroupByWire,
};

use super::test_support::{commit, init_repo};
use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsFacetWire, ProjectStatsPlanStorageWire,
    ProjectStatsPlanStoreWire, ProjectStatsProjectSpecWire,
    ProjectStatsProjectStateWire, ProjectStatsRequestWire,
    ProjectStatsSelectionWire, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use super::*;

const START_TS: i64 = 1_783_641_600; // 2026-07-10T00:00:00Z
const END_TS: i64 = START_TS + 24 * 60 * 60;

fn write_json(path: &Path, payload: Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
}

fn add_project_run(
    projects: &Path,
    project: &str,
    timestamp: &str,
    meta: Value,
    done: Value,
) -> PathBuf {
    let dir = projects
        .join(project)
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    write_json(&dir.join("agent_meta.json"), meta);
    write_json(&dir.join("done.json"), done);
    dir
}

fn plan_store() -> ProjectStatsPlanStoreWire {
    ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::InTree,
        repo_path: None,
        plans_dir: "sdd/plans".to_owned(),
        project_filter: None,
    }
}

fn project(
    key: &str,
    index_names: &[&str],
    repo_path: &Path,
    selection: ProjectStatsSelectionWire,
) -> ProjectStatsProjectSpecWire {
    let mut spec = super::test_support::spec(repo_path);
    spec.id = format!("{key}-code");
    spec.name = key.to_owned();
    ProjectStatsProjectSpecWire {
        key: key.to_owned(),
        label: format!("Project {key}"),
        state: ProjectStatsProjectStateWire::Enabled,
        selection,
        index_project_names: index_names
            .iter()
            .map(|name| (*name).to_owned())
            .collect(),
        code_repos: vec![spec],
        record_repos: Vec::new(),
        not_counted: Vec::new(),
        plan_store: plan_store(),
    }
}

fn request(
    projects: Vec<ProjectStatsProjectSpecWire>,
    cache_dir: &str,
    index_path: Option<&str>,
) -> ProjectStatsRequestWire {
    ProjectStatsRequestWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: START_TS,
        end_ts: END_TS,
        now_ts: END_TS,
        timezone: "America/New_York".to_owned(),
        facets: vec![ProjectStatsFacetWire::Runs],
        cache_dir: cache_dir.to_owned(),
        run_index_path: index_path.map(ToOwned::to_owned),
        agents_sidecars: Vec::new(),
        local_agent_name_prefix: String::new(),
        known_model_aliases: BTreeMap::new(),
        record_repo_names: Vec::new(),
        include_run_only_projects: false,
        budget: ProjectStatsBudgetWire::default(),
        projects,
    }
}

fn agent_request() -> AgentRunStatsRequestWire {
    AgentRunStatsRequestWire {
        start_ts: START_TS,
        end_ts: END_TS,
        runtime_group_by: AgentStatsRuntimeGroupByWire::Project,
        bucket_seconds: (END_TS - START_TS) as u64,
        top_n: 10,
        project: None,
        work_top_n: 10_000,
        macro_top_n: 1,
        macro_breakdown_top_n: 1,
        macro_focus: None,
        excluded_commit_repo_names: Vec::new(),
    }
}

fn fixture_index() -> (TempDir, PathBuf, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let alpha_start = "2026-07-10T01:00:00Z";
    add_project_run(
        &projects,
        "alpha-index",
        "20260710010000",
        json!({
            "name": "alpha-agent",
            "run_started_at": alpha_start,
            "cl_name": "alpha-spec"
        }),
        json!({
            "outcome": "completed",
            "finished_at": 1_783_644_660.0,
            "step_output": {"meta_commits": [
                {"sha": "1", "repo_name": "sase", "patch_name": "alpha-spec"},
                {"sha": "2", "repo_name": "beads", "patch_name": "alpha-spec"}
            ]}
        }),
    );
    let failed_start = "2026-07-10T02:00:00Z";
    add_project_run(
        &projects,
        "alpha-index",
        "20260710020000",
        json!({
            "name": "alpha-fail",
            "run_started_at": failed_start,
            "cl_name": "alpha-spec"
        }),
        json!({
            "outcome": "failed",
            "finished_at": 1_783_648_860.0
        }),
    );
    let beta_start = "2026-07-10T03:00:00Z";
    add_project_run(
        &projects,
        "beta-index",
        "20260710030000",
        json!({
            "name": "beta-agent",
            "run_started_at": beta_start,
            "cl_name": "beta-spec"
        }),
        json!({
            "outcome": "completed",
            "finished_at": 1_783_652_460.0,
            "step_output": {"meta_commits": [
                {"sha": "3", "repo_name": "sase", "patch_name": "beta-spec"}
            ]}
        }),
    );
    let ghost_start = "2026-07-10T04:00:00Z";
    add_project_run(
        &projects,
        "ghost-index",
        "20260710040000",
        json!({
            "name": "ghost-agent",
            "run_started_at": ghost_start,
            "cl_name": "ghost-spec"
        }),
        json!({
            "outcome": "completed",
            "finished_at": 1_783_656_060.0
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    (tmp, index, projects)
}

#[test]
fn missing_index_is_an_issue_and_leaves_runs_null() {
    let cache = tempfile::tempdir().unwrap();
    let git = init_repo();
    commit(&git.repo, "file.txt", "one\n", "one");
    let req = request(
        vec![project(
            "alpha",
            &["alpha-index"],
            &git.repo,
            ProjectStatsSelectionWire::Always,
        )],
        &cache.path().to_string_lossy(),
        None,
    );
    let response = query_project_stats(&req).unwrap();
    assert!(response.projects[0].runs.is_none());
    assert!(response.portfolio.runs.is_none());
    assert!(response
        .issues
        .iter()
        .any(|issue| issue.code == "index_missing"));
}

#[test]
fn migration_oracle_matches_query_run_stats_per_project_and_patch() {
    let (_tmp, index, _) = fixture_index();
    let cache = tempfile::tempdir().unwrap();
    let git = init_repo();
    commit(&git.repo, "file.txt", "one\n", "one");
    let mut req = request(
        vec![
            project(
                "alpha",
                &["alpha-index"],
                &git.repo,
                ProjectStatsSelectionWire::Always,
            ),
            project(
                "beta",
                &["beta-index"],
                &git.repo,
                ProjectStatsSelectionWire::Always,
            ),
        ],
        &cache.path().to_string_lossy(),
        Some(&index.to_string_lossy()),
    );
    req.facets =
        vec![ProjectStatsFacetWire::Output, ProjectStatsFacetWire::Runs];
    let response = query_project_stats(&req).unwrap();
    let baseline = query_run_stats(&index, agent_request()).unwrap();

    let alpha_work = baseline
        .work
        .projects
        .iter()
        .find(|row| row.project == "alpha-index")
        .unwrap();
    let beta_work = baseline
        .work
        .projects
        .iter()
        .find(|row| row.project == "beta-index")
        .unwrap();
    let alpha = response
        .projects
        .iter()
        .find(|row| row.key == "alpha")
        .unwrap();
    let beta = response
        .projects
        .iter()
        .find(|row| row.key == "beta")
        .unwrap();
    let alpha_runs = alpha.runs.as_ref().unwrap();
    let beta_runs = beta.runs.as_ref().unwrap();

    assert_eq!(alpha_runs.runs, alpha_work.runs);
    assert_eq!(alpha_runs.completed, alpha_work.completed);
    assert_eq!(alpha_runs.failed, alpha_work.failed);
    assert_eq!(alpha_runs.success_rate, Some(alpha_work.success_rate));
    assert_eq!(
        alpha_runs.total_runtime_seconds,
        alpha_work.total_runtime_seconds
    );
    assert_eq!(alpha_runs.distinct_patches, alpha_work.distinct_patches);
    assert_eq!(beta_runs.runs, beta_work.runs);
    assert_eq!(beta_runs.success_rate, Some(beta_work.success_rate));
    assert_eq!(
        beta_runs.total_runtime_seconds,
        beta_work.total_runtime_seconds
    );

    let alpha_patch = alpha
        .patches
        .iter()
        .find(|row| row.name == "alpha-spec")
        .unwrap();
    let work_patch = baseline
        .work
        .patches
        .iter()
        .find(|row| row.project == "alpha-index" && row.name == "alpha-spec")
        .unwrap();
    assert_eq!(alpha_patch.runs, work_patch.runs);
    assert_eq!(
        alpha_patch.total_runtime_seconds,
        work_patch.total_runtime_seconds.round() as u64
    );
    assert_eq!(alpha_patch.run_commits, work_patch.commits);

    let portfolio = response.portfolio.runs.as_ref().unwrap();
    assert_eq!(portfolio.runs, alpha_runs.runs + beta_runs.runs);
}

#[test]
fn record_repo_names_drop_run_commits_from_patch_rows() {
    let (_tmp, index, _) = fixture_index();
    let cache = tempfile::tempdir().unwrap();
    let git = init_repo();
    commit(&git.repo, "file.txt", "one\n", "one");
    let mut req = request(
        vec![project(
            "alpha",
            &["alpha-index"],
            &git.repo,
            ProjectStatsSelectionWire::Always,
        )],
        &cache.path().to_string_lossy(),
        Some(&index.to_string_lossy()),
    );
    req.record_repo_names = vec!["beads".to_owned()];
    let response = query_project_stats(&req).unwrap();
    let patch = response.projects[0]
        .patches
        .iter()
        .find(|row| row.name == "alpha-spec")
        .unwrap();
    assert_eq!(patch.run_commits, 1);

    let mut excluded = agent_request();
    excluded.excluded_commit_repo_names = vec!["beads".to_owned()];
    let baseline = query_run_stats(&index, excluded).unwrap();
    assert_eq!(baseline.work.projects[0].project, "alpha-index");
    let work_patch = baseline
        .work
        .patches
        .iter()
        .find(|row| row.name == "alpha-spec")
        .unwrap();
    assert_eq!(patch.run_commits, work_patch.commits);
}

#[test]
fn if_runs_drops_projects_without_runs_in_range() {
    let (_tmp, index, _) = fixture_index();
    let cache = tempfile::tempdir().unwrap();
    let git = init_repo();
    commit(&git.repo, "file.txt", "one\n", "one");
    let req = request(
        vec![
            project(
                "alpha",
                &["alpha-index"],
                &git.repo,
                ProjectStatsSelectionWire::IfRuns,
            ),
            project(
                "quiet",
                &["missing-index"],
                &git.repo,
                ProjectStatsSelectionWire::IfRuns,
            ),
            project(
                "always-quiet",
                &["missing-index"],
                &git.repo,
                ProjectStatsSelectionWire::Always,
            ),
        ],
        &cache.path().to_string_lossy(),
        Some(&index.to_string_lossy()),
    );
    let response = query_project_stats(&req).unwrap();
    let keys: Vec<_> = response
        .projects
        .iter()
        .map(|project| project.key.as_str())
        .collect();
    assert_eq!(keys, vec!["alpha", "always-quiet"]);
    assert_eq!(response.projects[1].runs.as_ref().unwrap().runs, 0);
}

#[test]
fn include_run_only_projects_appends_unregistered_index_rows() {
    let (_tmp, index, _) = fixture_index();
    let cache = tempfile::tempdir().unwrap();
    let git = init_repo();
    commit(&git.repo, "file.txt", "one\n", "one");
    let mut req = request(
        vec![project(
            "alpha",
            &["alpha-index"],
            &git.repo,
            ProjectStatsSelectionWire::Always,
        )],
        &cache.path().to_string_lossy(),
        Some(&index.to_string_lossy()),
    );
    req.include_run_only_projects = true;
    let response = query_project_stats(&req).unwrap();
    let ghost = response
        .projects
        .iter()
        .find(|row| row.key == "ghost-index")
        .expect("unregistered ghost");
    assert_eq!(ghost.state, ProjectStatsProjectStateWire::Unregistered);
    assert!(ghost.output.is_none());
    assert!(ghost.code.is_none());
    assert_eq!(ghost.runs.as_ref().unwrap().runs, 1);
    assert!(response.projects.iter().any(|row| row.key == "beta-index"));
}
