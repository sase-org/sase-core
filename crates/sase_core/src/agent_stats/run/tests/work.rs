//! Project and patch work-attribution tests.

use std::fs;

use serde_json::json;
use tempfile::tempdir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
};

use super::super::super::wire::AgentStatsRuntimeGroupByWire;
use super::super::types::{NO_PATCH, UNKNOWN};
use super::super::*;
use super::support::*;

#[test]
fn attributes_project_and_patch_work_with_filters_and_statuses() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let project = "gh_sase-org__sase";
    fs::create_dir_all(projects.join(project)).unwrap();
    fs::write(
        projects.join(project).join(format!("{project}.sase")),
        concat!(
            "PROJECT_NAME: sase\n",
            "NAME: commit-spec\n",
            "STATUS: Ready\n",
            "PR: https://example.test/pr/1\n\n\n",
            "NAME: launch-spec\n",
            "STATUS: WIP\n",
        ),
    )
    .unwrap();
    fs::write(
        projects
            .join(project)
            .join(format!("{project}-archive.sase")),
        "NAME: archived-spec\nSTATUS: Submitted\n",
    )
    .unwrap();

    let multi_start = "2026-07-10T01:00:00Z";
    add_project_run(
        &projects,
        project,
        "20260710010000",
        json!({
            "name": "multi",
            "run_started_at": multi_start,
            "cl_name": "launch-spec",
            "commit_changespec_name": "launch-spec"
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(multi_start, 60.0),
            "step_output": {"meta_commits": [
                {"sha": "1", "patch_name": "commit-spec"},
                {"sha": "2", "patch_name": "commit-spec"},
                {"sha": "3", "patch_name": "archived-spec"}
            ]}
        })),
        false,
    );
    let fallback_start = "2026-07-10T02:00:00Z";
    add_project_run(
        &projects,
        project,
        "20260710020000",
        json!({
            "name": "fallback",
            "run_started_at": fallback_start,
            "cl_name": "launch-spec",
            "commit_changespec_name": "archived-spec"
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(fallback_start, 30.0),
            "step_output": {"meta_commits": [{"sha": "4"}]}
        })),
        false,
    );
    let orphan_start = "2026-07-10T03:00:00Z";
    add_project_run(
        &projects,
        project,
        "20260710030000",
        json!({
            "name": "orphan",
            "run_started_at": orphan_start,
            "cl_name": "orphan-spec"
        }),
        Some(json!({
            "outcome": "failed",
            "finished_at": finish_at(orphan_start, 20.0)
        })),
        false,
    );
    let key_start = "2026-07-10T04:00:00Z";
    add_project_run(
        &projects,
        project,
        "20260710040000",
        json!({
            "name": "key-placeholder",
            "run_started_at": key_start,
            "cl_name": project
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(key_start, 10.0)
        })),
        false,
    );
    add_project_run(
        &projects,
        project,
        "20260710050000",
        json!({
            "name": "display-placeholder",
            "run_started_at": "2026-07-10T05:00:00Z",
            "cl_name": "sase-org/sase"
        }),
        None,
        false,
    );
    add_project_run(
        &projects,
        project,
        "20260710060000",
        json!({
            "name": "bare-placeholder",
            "run_started_at": "2026-07-10T06:00:00Z",
            "cl_name": "sase"
        }),
        None,
        true,
    );
    let unknown_start = "2026-07-10T07:00:00Z";
    add_project_run(
        &projects,
        project,
        "20260710070000",
        json!({
            "name": "unknown-placeholder",
            "run_started_at": unknown_start,
            "cl_name": "unknown"
        }),
        Some(json!({
            "outcome": "cancelled",
            "finished_at": finish_at(unknown_start, 5.0)
        })),
        false,
    );

    let other = "other";
    fs::create_dir_all(projects.join(other)).unwrap();
    fs::write(projects.join(other).join("other.sase"), [0xff]).unwrap();
    let other_start = "2026-07-10T08:00:00Z";
    add_project_run(
        &projects,
        other,
        "20260710080000",
        json!({
            "name": "other-agent",
            "run_started_at": other_start,
            "cl_name": "other-spec"
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(other_start, 10.0)
        })),
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let mut filtered = request();
    filtered.project = Some(project.to_string());
    filtered.runtime_group_by = AgentStatsRuntimeGroupByWire::Patch;
    let result = query_run_stats(&index, filtered.clone()).unwrap();
    assert_eq!(result.totals.runs, 7);
    assert_eq!(result.work.projects.len(), 1);
    let project_row = &result.work.projects[0];
    assert_eq!(project_row.project, project);
    assert_eq!(project_row.runs, 7);
    assert_eq!(project_row.completed, 3);
    assert_eq!(project_row.failed, 1);
    assert_eq!(project_row.other_terminal, 1);
    assert_eq!(project_row.in_progress, 1);
    assert_eq!(project_row.waiting, 1);
    assert_eq!(project_row.commits, 4);
    assert_eq!(project_row.distinct_patches, 3);
    assert_eq!(project_row.unattributed_runs, 4);
    assert_eq!(project_row.total_runtime_seconds, 125.0);
    assert_eq!(result.work.unattributed_runs, 4);
    assert_eq!(result.work.malformed_spec_files_skipped, 0);

    let archived = result
        .work
        .patches
        .iter()
        .find(|row| row.name == "archived-spec")
        .unwrap();
    assert_eq!(archived.status, "Submitted");
    assert_eq!(archived.runs, 2);
    assert_eq!(archived.distinct_agents, 2);
    assert_eq!(archived.commits, 2);
    assert_eq!(archived.total_runtime_seconds, 90.0);
    let committed = result
        .work
        .patches
        .iter()
        .find(|row| row.name == "commit-spec")
        .unwrap();
    assert_eq!(committed.status, "Ready");
    assert!(committed.has_pr);
    assert_eq!(committed.commits, 2);
    assert!(result
        .work
        .patches
        .iter()
        .all(|row| row.name != "launch-spec"));
    let orphan = result
        .work
        .patches
        .iter()
        .find(|row| row.name == "orphan-spec")
        .unwrap();
    assert_eq!(orphan.status, UNKNOWN);

    let runtime = |name: &str| {
        result
            .runtime_groups
            .iter()
            .find(|group| group.group == name)
            .unwrap()
            .total_seconds
    };
    assert_eq!(runtime("archived-spec"), 90.0);
    assert_eq!(runtime("commit-spec"), 60.0);
    assert_eq!(runtime("orphan-spec"), 20.0);
    assert_eq!(runtime(NO_PATCH), 15.0);

    filtered.work_top_n = 2;
    let truncated = query_run_stats(&index, filtered).unwrap();
    assert_eq!(truncated.work.patches.len(), 2);
    assert_eq!(truncated.work.truncated_patch_rows, 1);

    let mut by_project = request();
    by_project.runtime_group_by = AgentStatsRuntimeGroupByWire::Project;
    let all_projects = query_run_stats(&index, by_project).unwrap();
    assert_eq!(all_projects.totals.runs, 8);
    assert_eq!(all_projects.work.projects.len(), 2);
    assert_eq!(all_projects.work.malformed_spec_files_skipped, 1);
    assert_eq!(
        all_projects
            .runtime_groups
            .iter()
            .find(|group| group.group == project)
            .unwrap()
            .total_seconds,
        125.0
    );
}

#[test]
fn missing_archive_spec_is_not_malformed() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let project = "clean";
    fs::create_dir_all(projects.join(project)).unwrap();
    fs::write(
        projects.join(project).join("clean.sase"),
        "NAME: clean-spec\nSTATUS: Ready\n",
    )
    .unwrap();
    add_project_run(
        &projects,
        project,
        "20260710010000",
        json!({
            "name": "clean-agent",
            "run_started_at": "2026-07-10T01:00:00Z",
            "cl_name": "clean-spec"
        }),
        Some(json!({"outcome": "completed"})),
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let result = query_run_stats(&index, request()).unwrap();
    assert_eq!(result.work.malformed_spec_files_skipped, 0);
    assert_eq!(result.work.patches[0].name, "clean-spec");
    assert_eq!(result.work.patches[0].status, "Ready");
}
