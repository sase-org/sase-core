//! Core aggregation tests: window outcomes, commit counts, runtime
//! percentiles, and request validation.

use std::path::Path;

use rusqlite::{params, Connection};
use serde_json::json;
use tempfile::tempdir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
};

use super::super::super::wire::AgentCommitDistributionWire;
use super::super::finishing::percentile;
use super::super::types::MAX_BUCKETS;
use super::super::*;
use super::support::*;

#[test]
fn aggregates_window_outcomes_metadata_and_runtime() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let first_start = "2026-07-10T01:00:00Z";
    let first = add_run(
        &projects,
        "20260710010000",
        json!({
            "name": "alpha",
            "workflow_name": "review",
            "run_started_at": first_start,
            "stopped_at": "2026-07-10T01:09:00Z",
            "llm_provider": "codex",
            "model": "gpt-5",
            "reasoning_effort": "high",
            "agent_clan": "red",
            "agent_family": "fam",
            "clan_tribe": "builders",
            "workspace_num": 1,
            "plan_submitted_at": ["p1", "p2"],
            "plan_approved": true,
            "plan_action": "epic",
            "questions_submitted_at": ["q1", "q2"],
            "retry_chain_root_timestamp": "20260710010000",
            "retried_as_timestamp": "20260710020000",
            "retry_terminal": true
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(first_start, 120.0),
            "workspace_num": 1,
            "step_output": {"meta_commits": [
                {"sha": "1", "repo_name": "sase"},
                {"sha": "2", "repo_name": "sase"},
                {"sha": "3", "repo_name": "core"}
            ]}
        })),
        false,
    );
    let second_start = "2026-07-10T02:00:00Z";
    add_run(
        &projects,
        "20260710020000",
        json!({
            "name": "beta",
            "run_started_at": second_start,
            "stopped_at": "2026-07-10T02:05:00Z",
            "llm_provider": "codex",
            "model": "gpt-5",
            "reasoning_effort": "turbo",
            "workspace_num": 1,
            "plan_submitted_at": ["p3"],
            "plan_action": "reject",
            "retry_attempt": 1,
            "retry_of_timestamp": "20260710010000",
            "retry_chain_root_timestamp": "20260710010000"
        }),
        Some(json!({
            "outcome": "plan_rejected",
            "step_output": {"meta_commits": [
                {"sha": "4", "repo_name": "sase"}
            ]}
        })),
        false,
    );
    add_run(
        &projects,
        "20260710070000",
        json!({
            "name": "gamma",
            "run_started_at": "2026-07-10T07:00:00Z",
            "llm_provider": "claude",
            "model": "opus",
            "reasoning_effort": "max",
            "workspace_num": 2
        }),
        None,
        true,
    );
    let fourth_start = "2026-07-10T13:00:00Z";
    add_run(
        &projects,
        "20260710130000",
        json!({
            "name": "delta",
            "run_started_at": fourth_start,
            "workspace_num": 3
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": finish_at(fourth_start, 60.0),
            "step_output": {"meta_commits": [
                {"sha": "5", "repo_name": "core"},
                {"sha": "6", "repo_name": "core"}
            ]}
        })),
        false,
    );
    add_run(
        &projects,
        "20260709010000",
        json!({
            "name": "outside",
            "run_started_at": "2026-07-09T01:00:00Z"
        }),
        Some(json!({
            "outcome": "failed",
            "finished_at": finish_at("2026-07-09T01:00:00Z", 30.0)
        })),
        false,
    );
    let malformed = add_run(
        &projects,
        "20260710190000",
        json!({
            "name": "malformed",
            "run_started_at": "2026-07-10T19:00:00Z"
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
    Connection::open(&index)
        .unwrap()
        .execute(
            "UPDATE agent_artifacts SET record_json = '{' WHERE artifact_dir = ?1",
            params![malformed.to_string_lossy().as_ref()],
        )
        .unwrap();

    let result = query_run_stats(&index, request()).unwrap();
    assert_eq!(result.totals.runs, 4);
    assert_eq!(result.totals.completed, 2);
    assert_eq!(result.totals.failed, 0);
    assert_eq!(result.totals.other_terminal, 1);
    assert_eq!(result.totals.waiting, 1);
    assert_eq!(result.totals.in_progress, 0);
    assert_eq!(result.malformed_rows_skipped, 1);
    let xprompts = result.xprompts.as_ref().unwrap();
    assert_eq!(xprompts.runs_with_xprompts, 0);
    assert_eq!(xprompts.runs_without_xprompts, 4);
    assert_eq!(
        result
            .outcomes
            .iter()
            .map(|value| (value.name.as_str(), value.count))
            .collect::<Vec<_>>(),
        vec![("completed", 2), ("plan_rejected", 1)]
    );
    assert_eq!(result.retries.chains, 1);
    assert_eq!(result.retries.attempts, 1);
    assert_eq!(result.retries.kills, 1);

    let high = result
        .providers
        .iter()
        .find(|value| value.effort == "high")
        .unwrap();
    assert_eq!(high.runs, 1);
    assert_eq!(high.completed, 1);
    assert_eq!(high.mean_runtime_seconds, Some(120.0));
    let default = result
        .providers
        .iter()
        .find(|value| value.provider == "codex" && value.effort == "default")
        .unwrap();
    assert_eq!(default.runs, 1);
    assert_eq!(default.mean_runtime_seconds, Some(300.0));

    assert_eq!(result.commits.total_commits, 6);
    assert_eq!(result.commits.committing_agents, 3);
    assert_eq!(result.commits.committing_runs, 3);
    assert_eq!(result.commits.average_per_committing_agent, 2.0);
    assert_eq!(
        result.commits.distribution,
        AgentCommitDistributionWire {
            zero: 1,
            one: 1,
            two: 1,
            three_plus: 1,
        }
    );
    assert_eq!(result.commits.top_repos[0].name, "core");
    assert_eq!(result.commits.top_repos[0].count, 3);
    assert_eq!(result.commits.top_repos[1].name, "sase");
    assert_eq!(result.commits.top_repos[1].count, 3);

    assert_eq!(result.plans.proposed, 3);
    assert_eq!(result.plans.proposing_agents, 2);
    assert_eq!(result.plans.approved, 1);
    assert_eq!(result.plans.rejected, 1);
    assert_eq!(result.plans.pending, 0);
    assert_eq!(result.questions.sessions, 2);
    assert_eq!(result.questions.asking_agents, 1);
    assert_eq!(result.workspaces[0].project, "proj");
    assert_eq!(result.workspaces[0].workspace_num, 1);
    assert_eq!(result.workspaces[0].runs, 2);
    assert_eq!(
        result
            .buckets
            .iter()
            .map(|bucket| bucket.runs)
            .collect::<Vec<_>>(),
        vec![2, 1, 1, 0]
    );

    let alpha = result
        .runtime_groups
        .iter()
        .find(|value| value.group == "alpha")
        .unwrap();
    assert_eq!(alpha.total_seconds, 120.0);
    assert_eq!(alpha.p50_seconds, 120.0);
    let beta = result
        .runtime_groups
        .iter()
        .find(|value| value.group == "beta")
        .unwrap();
    assert_eq!(beta.total_seconds, 300.0);
    assert!(result
        .runtime_groups
        .iter()
        .all(|value| value.group != "gamma"));

    // The fixture DB was built through the real scanner/index insertion
    // path, so this also verifies the denormalized fields used before JSON
    // decoding. Keep the variable live to make that relationship clear.
    assert!(first.exists());
    assert_eq!(result.work.projects[0].runs, 4);
}

#[test]
fn committing_agents_counts_distinct_names_not_runs() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    add_run(
        &projects,
        "20260710010000",
        json!({
            "name": "alpha",
            "run_started_at": "2026-07-10T01:00:00Z"
        }),
        Some(json!({
            "outcome": "completed",
            "step_output": {"meta_commits": [
                {"sha": "1", "repo_name": "sase"}
            ]}
        })),
        false,
    );
    add_run(
        &projects,
        "20260710020000",
        json!({
            "name": "alpha",
            "run_started_at": "2026-07-10T02:00:00Z"
        }),
        Some(json!({
            "outcome": "completed",
            "step_output": {"meta_commits": [
                {"sha": "2", "repo_name": "sase"}
            ]}
        })),
        false,
    );
    add_run(
        &projects,
        "20260710030000",
        json!({
            "name": "beta",
            "run_started_at": "2026-07-10T03:00:00Z"
        }),
        Some(json!({
            "outcome": "completed",
            "step_output": {"meta_commits": [
                {"sha": "3", "repo_name": "core"},
                {"sha": "4", "repo_name": "core"}
            ]}
        })),
        false,
    );
    add_run(
        &projects,
        "20260710040000",
        json!({
            "name": "gamma",
            "run_started_at": "2026-07-10T04:00:00Z"
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
    assert_eq!(result.commits.total_commits, 4);
    assert_eq!(result.commits.committing_runs, 3);
    assert_eq!(result.commits.committing_agents, 2);
    assert_eq!(result.commits.average_per_committing_agent, 2.0);
    assert_eq!(
        result.commits.distribution,
        AgentCommitDistributionWire {
            zero: 1,
            one: 2,
            two: 1,
            three_plus: 0,
        }
    );
}

#[test]
fn runtime_percentiles_interpolate_sorted_durations() {
    assert_eq!(percentile(&[10.0, 20.0, 30.0], 0.50), 20.0);
    assert_eq!(percentile(&[10.0, 20.0, 30.0], 0.95), 29.0);
}

#[test]
fn rejects_invalid_ranges_and_bucket_explosions() {
    let missing = Path::new("/does/not/matter.sqlite");
    let mut invalid = request();
    invalid.end_ts = invalid.start_ts;
    assert!(query_run_stats(missing, invalid)
        .unwrap_err()
        .contains("end_ts"));

    let mut zero_bucket = request();
    zero_bucket.bucket_seconds = 0;
    assert!(query_run_stats(missing, zero_bucket)
        .unwrap_err()
        .contains("bucket_seconds"));

    let mut excessive = request();
    excessive.end_ts = excessive.start_ts + MAX_BUCKETS as i64 + 1;
    excessive.bucket_seconds = 1;
    assert!(query_run_stats(missing, excessive)
        .unwrap_err()
        .contains("maximum"));
}
