//! XPrompt aggregation tests.

use serde_json::json;
use tempfile::tempdir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
};

use super::super::types::UNKNOWN;
use super::super::*;
use super::support::*;

#[test]
fn aggregates_ranked_xprompt_usage_and_focused_breakdowns() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");

    let first_start = "2026-07-10T01:00:00Z";
    let first = add_project_run(
        &projects,
        "alpha-project",
        "20260710010000",
        json!({
            "name": "alpha",
            "run_started_at": first_start,
            "llm_provider": "codex",
            "model": "gpt-5",
            "clan_tribe": "builders"
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(first_start, 60.0)
        })),
        false,
    );
    write_json(
        &first.join("xprompts.json"),
        json!([
            {"name": "gh", "kind": "workflow", "tags": ["vcs"]},
            {"name": "gh", "kind": "workflow", "tags": ["vcs"]},
            {"name": "split_file", "kind": "part", "tags": []}
        ]),
    );

    let second_start = "2026-07-10T02:00:00Z";
    let second = add_project_run(
        &projects,
        "beta-project",
        "20260710020000",
        json!({
            "name": "beta",
            "run_started_at": second_start,
            "llm_provider": "claude",
            "model": "opus",
            "clan_tribe": "reviewers"
        }),
        Some(json!({
            "outcome": "failed",
            "finished_at": finish_at(second_start, 30.0)
        })),
        false,
    );
    write_json(
        &second.join("xprompts.json"),
        json!([{"name": "gh", "kind": "workflow", "tags": ["vcs"]}]),
    );

    let third = add_project_run(
        &projects,
        "beta-project",
        "20260710070000",
        json!({
            "name": "alpha",
            "run_started_at": "2026-07-10T07:00:00Z",
            "llm_provider": "codex",
            "model": "gpt-5",
            "clan_tribe": "builders"
        }),
        None,
        false,
    );
    write_json(
        &third.join("xprompts.json"),
        json!([
            {"name": "plan", "kind": "part", "tags": ["planning"]},
            {"name": "split_file", "kind": "part", "tags": []}
        ]),
    );
    add_project_run(
        &projects,
        "beta-project",
        "20260710130000",
        json!({
            "name": "no-xprompts",
            "run_started_at": "2026-07-10T13:00:00Z"
        }),
        None,
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let mut focused_request = request();
    focused_request.xprompt_top_n = 2;
    focused_request.xprompt_breakdown_top_n = 1;
    focused_request.xprompt_focus = Some("gh".to_string());
    let result = query_run_stats(&index, focused_request).unwrap();
    assert_eq!(result.schema_version, 6);
    let xprompts = result.xprompts.as_ref().unwrap();
    assert_eq!(xprompts.runs_with_xprompts, 3);
    assert_eq!(xprompts.runs_without_xprompts, 1);
    assert_eq!(xprompts.distinct_xprompts, 3);
    assert_eq!(xprompts.total_references, 6);
    assert_eq!(xprompts.truncated_rows, 1);
    assert_eq!(
        xprompts
            .rows
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["gh", "split_file"]
    );

    let gh = &xprompts.rows[0];
    assert_eq!(gh.kind, "workflow");
    assert_eq!(gh.tags, vec!["vcs"]);
    assert_eq!(gh.runs, 2);
    assert_eq!(gh.references, 3);
    assert_eq!(gh.distinct_agents, 2);
    assert_eq!(gh.completed, 1);
    assert_eq!(gh.failed, 1);
    assert_eq!(gh.success_rate, 0.5);
    assert_eq!(gh.total_runtime_seconds, 90.0);
    assert_eq!(gh.mean_runtime_seconds, Some(45.0));
    assert_eq!(gh.models.len(), 1);
    assert_eq!(gh.models[0].name, "gpt-5");
    assert_eq!(gh.models_truncated, 1);
    assert_eq!(gh.projects.len(), 1);
    assert_eq!(gh.projects[0].name, "alpha-project");
    assert_eq!(gh.projects_truncated, 1);
    assert_eq!(
        gh.partners
            .iter()
            .map(|row| (row.name.as_str(), row.count))
            .collect::<Vec<_>>(),
        vec![("split_file", 1)]
    );
    assert_eq!(gh.partners_truncated, 0);

    let focus = xprompts.focus.as_ref().unwrap();
    assert!(focus.found);
    assert_eq!(focus.name, "gh");
    assert_eq!(focus.runs, 2);
    assert_eq!(focus.references, 3);
    assert_eq!(focus.distinct_agents, 2);
    assert_eq!(focus.completed, 1);
    assert_eq!(focus.failed, 1);
    assert_eq!(focus.total_runtime_seconds, 90.0);
    assert_eq!(focus.mean_runtime_seconds, Some(45.0));
    assert_eq!(
        focus
            .models
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["gpt-5", "opus"]
    );
    assert_eq!(
        focus
            .providers
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["claude", "codex"]
    );
    assert_eq!(
        focus
            .projects
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha-project", "beta-project"]
    );
    assert_eq!(
        focus
            .tribes
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["builders", "reviewers"]
    );
    assert_eq!(
        focus
            .buckets
            .iter()
            .map(|bucket| bucket.runs)
            .collect::<Vec<_>>(),
        vec![2, 0, 0, 0]
    );

    let mut filtered_request = request();
    filtered_request.project = Some("alpha-project".to_string());
    let filtered = query_run_stats(&index, filtered_request).unwrap();
    let filtered_xprompts = filtered.xprompts.unwrap();
    assert_eq!(filtered_xprompts.runs_with_xprompts, 1);
    assert_eq!(filtered_xprompts.runs_without_xprompts, 0);
    assert_eq!(filtered_xprompts.total_references, 3);
    assert_eq!(filtered_xprompts.rows[0].projects.len(), 1);
    assert_eq!(filtered_xprompts.rows[0].projects[0].name, "alpha-project");

    let mut unknown_request = request();
    unknown_request.xprompt_focus = Some("missing".to_string());
    let unknown = query_run_stats(&index, unknown_request)
        .unwrap()
        .xprompts
        .unwrap()
        .focus
        .unwrap();
    assert_eq!(unknown.name, "missing");
    assert!(!unknown.found);
    assert_eq!(unknown.kind, UNKNOWN);
    assert_eq!(unknown.runs, 0);
    assert_eq!(unknown.buckets.len(), 4);
    assert!(unknown.buckets.iter().all(|bucket| bucket.runs == 0));

    let mut no_duration_request = request();
    no_duration_request.xprompt_focus = Some("plan".to_string());
    let no_duration = query_run_stats(&index, no_duration_request)
        .unwrap()
        .xprompts
        .unwrap()
        .focus
        .unwrap();
    assert!(no_duration.found);
    assert_eq!(no_duration.runs, 1);
    assert_eq!(no_duration.total_runtime_seconds, 0.0);
    assert_eq!(no_duration.mean_runtime_seconds, None);
}

#[test]
fn aggregates_swarm_xprompt_kind_through_stats_wire() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");

    let start = "2026-07-10T01:00:00Z";
    let artifact_dir = add_project_run(
        &projects,
        "alpha-project",
        "20260710010000",
        json!({
            "name": "swarm-child",
            "run_started_at": start,
            "llm_provider": "codex",
            "model": "gpt-5",
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(start, 45.0)
        })),
        false,
    );
    write_json(
        &artifact_dir.join("xprompts.json"),
        json!([
            {"name": "research_swarm", "kind": "swarm", "tags": ["research", "fanout"]},
            {"name": "research_swarm", "kind": "swarm", "tags": ["research", "fanout"]},
            {"name": "gh", "kind": "workflow", "tags": ["vcs"]}
        ]),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let mut focused_request = request();
    focused_request.xprompt_focus = Some("research_swarm".to_string());
    let result = query_run_stats(&index, focused_request).unwrap();
    let xprompts = result.xprompts.unwrap();
    let row = xprompts
        .rows
        .iter()
        .find(|row| row.name == "research_swarm")
        .unwrap();
    assert_eq!(row.kind, "swarm");
    assert_eq!(row.tags, vec!["fanout", "research"]);
    assert_eq!(row.runs, 1);
    assert_eq!(row.references, 2);
    assert_eq!(
        row.partners
            .iter()
            .map(|partner| (partner.name.as_str(), partner.count))
            .collect::<Vec<_>>(),
        vec![("gh", 1)]
    );

    let focus = xprompts.focus.unwrap();
    assert!(focus.found);
    assert_eq!(focus.kind, "swarm");
    assert_eq!(focus.runs, 1);
    assert_eq!(focus.references, 2);
}
