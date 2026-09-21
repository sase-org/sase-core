//! Runner query, eligibility, diagnostics, and range tests.

use std::fs;

use rusqlite::{params, Connection};
use serde_json::json;
use tempfile::tempdir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire,
};

use super::super::super::runner::MAX_RUNNER_TREND_SLICES;
use super::super::super::wire::{
    AgentRunStatsRequestWire, AgentStatsRuntimeGroupByWire,
};
use super::super::query::query_run_stats_with_liveness;
use super::super::*;
use super::support::*;

#[test]
fn runner_question_wait_uses_matching_gate_response_time() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let runner = add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({
            "name": "question",
            "run_started_at": runner_time(0),
            "questions_submitted_at": [runner_time(20)]
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 100) as f64
        })),
        false,
    );
    let gate = tmp.path().join("interaction_requests/question/req-1");
    write_json(
        &gate.join("request.json"),
        json!({
            "request_id": "req-1",
            "created_at_unix": (RUNNER_BASE + 20) as f64,
            "payload": {
                "timestamp": (RUNNER_BASE + 20) as f64,
                "questions": [{"question": "Continue?"}]
            },
            "producer": {"artifacts_dir": runner.to_string_lossy()}
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let unresolved =
        query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    assert_eq!(unresolved.runners.as_ref().unwrap().runner_seconds, 100.0);

    write_json(
        &gate.join("response.json"),
        json!({"responded_at_unix": (RUNNER_BASE + 55) as f64}),
    );
    let resolved =
        query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = resolved.runners.as_ref().unwrap();
    assert_eq!(runners.runner_seconds, 65.0);
    assert_eq!(runners.lanes_counted, 1);
}

#[test]
fn runner_query_filters_stale_and_never_started_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    add_run(
        &projects,
        &runner_artifact_timestamp(10),
        json!({"name": "first", "run_started_at": runner_time(10)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 30) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(30),
        json!({"name": "boundary", "run_started_at": runner_time(30)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 40) as f64
        })),
        false,
    );
    let live = add_run(
        &projects,
        &runner_artifact_timestamp(20),
        json!({"name": "live", "run_started_at": runner_time(20)}),
        None,
        false,
    );
    write_json(
        &live.join("pending_question.json"),
        json!({"submitted_at": runner_time(35)}),
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({"name": "stale", "run_started_at": runner_time(0)}),
        None,
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(5),
        json!({"name": "never-started", "pid": 12345}),
        None,
        false,
    );
    add_project_run(
        &projects,
        "other",
        &runner_artifact_timestamp(0),
        json!({"name": "other", "run_started_at": runner_time(0)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 40) as f64
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
    let mut request = runner_request(0, 40, 10);
    request.project = Some("proj".to_string());
    let live_probe = |record: &AgentArtifactRecordWire| {
        record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.name.as_deref())
            == Some("live")
    };
    let result =
        query_run_stats_with_liveness(&index, request, &live_probe).unwrap();
    let runners = result.runners.as_ref().unwrap();

    assert_eq!(result.totals.runs, 5);
    assert_eq!(runners.peak_runners, 2);
    assert_eq!(runners.peak_seconds, 15.0);
    assert_eq!(runners.average_runners, 1.125);
    assert_eq!(runners.busy_seconds, 30.0);
    assert_eq!(runners.runner_seconds, 45.0);
    assert_eq!(
        runners
            .distribution
            .iter()
            .map(|row| (row.runners, row.seconds))
            .collect::<Vec<_>>(),
        vec![(0, 10.0), (1, 15.0), (2, 15.0)]
    );
    assert_eq!(runners.lanes_counted, 3);
    assert_eq!(runners.lanes_without_end_skipped, 1);
    assert_eq!(runners.invalid_intervals_skipped, 0);
    assert_eq!(runners.malformed_rows_skipped, 0);
    assert_eq!(
        runners
            .trend
            .iter()
            .map(|slice| (slice.peak_runners, slice.runner_seconds))
            .collect::<Vec<_>>(),
        vec![(0, 0.0), (1, 10.0), (2, 20.0), (2, 15.0)]
    );
    assert_runner_conservation(runners);
}

#[test]
fn runner_query_requires_matching_live_workspace_claim() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let timestamp = runner_artifact_timestamp(10);
    let pid = std::process::id();
    add_run(
        &projects,
        &timestamp,
        json!({
            "name": "host-live",
            "pid": pid,
            "workspace_num": 7,
            "run_started_at": runner_time(10)
        }),
        None,
        false,
    );
    let project_file = projects.join("proj/proj.sase");
    fs::write(
        &project_file,
        format!("RUNNING:\n  #7 | {pid} | run | demo | {timestamp}\n"),
    )
    .unwrap();

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let result = query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = result.runners.as_ref().unwrap();
    assert_eq!(runners.runner_seconds, 90.0);
    assert_eq!(runners.lanes_without_end_skipped, 0);
    assert_eq!(runners.invalid_intervals_skipped, 0);

    fs::write(
        &project_file,
        format!("RUNNING:\n  #7 | {} | run | demo | {timestamp}\n", pid + 1),
    )
    .unwrap();
    let result = query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = result.runners.as_ref().unwrap();
    assert_eq!(runners.runner_seconds, 0.0);
    assert_eq!(runners.lanes_without_end_skipped, 1);
    assert_eq!(runners.invalid_intervals_skipped, 0);
    assert_runner_conservation(runners);
}

#[test]
fn runner_eligibility_honors_family_workflow_visibility_and_project() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let terminal = || {
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 100) as f64
        }))
    };

    add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({"name": "root", "run_started_at": runner_time(0)}),
        terminal(),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(1),
        json!({
            "name": "parallel",
            "run_started_at": runner_time(0),
            "parent_timestamp": "parent",
            "agent_family_parallel": true
        }),
        terminal(),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(2),
        json!({
            "name": "serial",
            "run_started_at": runner_time(0),
            "parent_timestamp": "parent"
        }),
        terminal(),
        false,
    );
    let appears = add_run(
        &projects,
        &runner_artifact_timestamp(3),
        json!({"name": "workflow-agent", "run_started_at": runner_time(0)}),
        terminal(),
        false,
    );
    write_json(
        &appears.join("workflow_state.json"),
        json!({
            "workflow_name": "agent-workflow",
            "status": "completed",
            "appears_as_agent": true
        }),
    );
    let workflow = add_run(
        &projects,
        &runner_artifact_timestamp(4),
        json!({"name": "workflow", "run_started_at": runner_time(0)}),
        terminal(),
        false,
    );
    write_json(
        &workflow.join("workflow_state.json"),
        json!({
            "workflow_name": "bookkeeping",
            "status": "completed",
            "appears_as_agent": false
        }),
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(5),
        json!({
            "name": "hidden",
            "run_started_at": runner_time(0),
            "hidden": true
        }),
        terminal(),
        false,
    );
    for (workflow_name, offset) in [("hook-run", 6), ("axe-run", 7)] {
        let dir = artifact_for_workflow(
            &projects,
            "proj",
            workflow_name,
            &runner_artifact_timestamp(offset),
        );
        write_json(
            &dir.join("agent_meta.json"),
            json!({"name": workflow_name, "run_started_at": runner_time(0)}),
        );
        write_json(&dir.join("done.json"), terminal().unwrap());
    }
    add_project_run(
        &projects,
        "other",
        &runner_artifact_timestamp(8),
        json!({"name": "other", "run_started_at": runner_time(0)}),
        terminal(),
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let mut filtered = runner_request(0, 100, 100);
    filtered.project = Some("proj".to_string());
    let result = query_run_stats(&index, filtered).unwrap();
    let runners = result.runners.as_ref().unwrap();

    // root, parallel, serial (now occupancy-eligible without agent_family),
    // and workflow-agent. The serial child is its own family because it
    // has no agent_family, matching the Python grouping fallback.
    assert_eq!(runners.peak_runners, 4);
    assert_eq!(runners.runner_seconds, 400.0);
    assert_eq!(runners.distribution[4].seconds, 100.0);
    assert_eq!(runners.lanes_counted, 4);
    assert_eq!(runners.user_hidden_skipped, 1);
    assert_eq!(runners.invalid_intervals_skipped, 0);
    assert_runner_conservation(runners);
}

#[test]
fn runner_diagnostics_separate_malformed_rows_and_invalid_intervals() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let valid = add_run(
        &projects,
        &runner_artifact_timestamp(10),
        json!({"name": "valid", "run_started_at": runner_time(10)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 20) as f64
        })),
        false,
    );
    let malformed = add_run(
        &projects,
        &runner_artifact_timestamp(15),
        json!({"name": "malformed", "run_started_at": runner_time(15)}),
        None,
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(25),
        json!({"name": "bad-start", "run_started_at": "not-a-time"}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 30) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(30),
        json!({"name": "reversed", "run_started_at": runner_time(30)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 20) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(40),
        json!({"name": "zero", "run_started_at": runner_time(40)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 40) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(50),
        json!({
            "name": "valid-finish",
            "run_started_at": runner_time(50),
            "stopped_at": "not-a-time"
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 60) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(70),
        json!({"name": "missing-finish", "run_started_at": runner_time(70)}),
        Some(json!({"outcome": "completed"})),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(80),
        json!({
            "name": "bad-stop",
            "run_started_at": runner_time(80),
            "stopped_at": "not-a-time"
        }),
        None,
        false,
    );
    let old = add_run(
        &projects,
        &runner_artifact_timestamp(-200),
        json!({"name": "old", "run_started_at": runner_time(-200)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE - 100) as f64
        })),
        false,
    );
    let future = add_run(
        &projects,
        &runner_artifact_timestamp(200),
        json!({"name": "future", "run_started_at": runner_time(200)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 300) as f64
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
    let conn = Connection::open(&index).unwrap();
    for path in [&malformed, &old, &future] {
        conn.execute(
            "UPDATE agent_artifacts SET record_json = '{' WHERE artifact_dir = ?1",
            params![path.to_string_lossy().as_ref()],
        )
        .unwrap();
    }
    drop(conn);

    let result = query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = result.runners.as_ref().unwrap();
    assert!(valid.exists());
    assert_eq!(result.malformed_rows_skipped, 1);
    assert_eq!(runners.malformed_rows_skipped, 1);
    assert_eq!(runners.invalid_intervals_skipped, 4);
    assert_eq!(runners.runner_seconds, 20.0);
    assert_eq!(runners.distribution[0].seconds, 80.0);
    assert_eq!(runners.distribution[1].seconds, 20.0);
    assert_runner_conservation(runners);
}

#[test]
fn runner_fixed_and_all_time_empty_ranges_have_distinct_contracts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    add_run(
        &projects,
        &runner_artifact_timestamp(40),
        json!({
            "name": "waited",
            "run_started_at": runner_time(40),
            "plan_submitted_at": [runner_time(40)],
            "feedback_submitted_at": [runner_time(50)]
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 60) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(80),
        json!({"name": "later", "run_started_at": runner_time(80)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 90) as f64
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

    let all_time = query_run_stats(
        &index,
        AgentRunStatsRequestWire {
            start_ts: 0,
            end_ts: RUNNER_BASE + 100,
            runtime_group_by: AgentStatsRuntimeGroupByWire::Agent,
            bucket_seconds: 1_000_000,
            top_n: 10,
            project: None,
            work_top_n: 50,
            xprompt_top_n: 40,
            xprompt_breakdown_top_n: 5,
            xprompt_focus: None,
        },
    )
    .unwrap();
    let runners = all_time.runners.as_ref().unwrap();
    assert_eq!(runners.start_ts, (RUNNER_BASE + 40) as f64);
    assert_eq!(runners.end_ts, (RUNNER_BASE + 100) as f64);
    assert_eq!(runners.runner_seconds, 30.0);
    assert_eq!(runners.distribution[0].seconds, 30.0);
    assert_eq!(runners.distribution[1].seconds, 30.0);
    assert_runner_conservation(runners);

    let mut no_data = runner_request(0, 100, 30);
    no_data.project = Some("missing".to_string());
    let fixed = query_run_stats(&index, no_data.clone()).unwrap();
    let idle = fixed.runners.as_ref().unwrap();
    assert_eq!(idle.peak_runners, 0);
    assert_eq!(idle.peak_seconds, 100.0);
    assert_eq!(idle.distribution.len(), 1);
    assert_eq!(idle.distribution[0].seconds, 100.0);
    assert_eq!(idle.distribution[0].share, 1.0);
    assert_runner_conservation(idle);

    no_data.start_ts = 0;
    no_data.end_ts = RUNNER_BASE + 100;
    no_data.bucket_seconds = 1_000_000;
    assert!(query_run_stats(&index, no_data).unwrap().runners.is_none());
}

#[test]
fn runner_peak_can_exceed_ten_and_long_trend_stays_bounded() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..11 {
        add_run(
            &projects,
            &runner_artifact_timestamp(index),
            json!({
                "name": format!("runner-{index}"),
                "run_started_at": runner_time(0)
            }),
            Some(json!({
                "outcome": "completed",
                "finished_at": (RUNNER_BASE + 10_001) as f64
            })),
            false,
        );
    }
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let result = query_run_stats(&index, runner_request(0, 10_001, 1)).unwrap();
    let runners = result.runners.as_ref().unwrap();
    assert_eq!(runners.peak_runners, 11);
    assert_eq!(runners.distribution.len(), 12);
    assert_eq!(runners.distribution[11].seconds, 10_001.0);
    assert!(runners.trend.len() <= MAX_RUNNER_TREND_SLICES);
    assert_eq!(runners.trend.first().unwrap().start_ts, RUNNER_BASE as f64);
    assert_eq!(
        runners.trend.last().unwrap().end_ts,
        (RUNNER_BASE + 10_001) as f64
    );
    assert_eq!(
        runners.trend.last().unwrap().end_ts
            - runners.trend.last().unwrap().start_ts,
        1.0
    );
    assert!(runners
        .trend
        .windows(2)
        .all(|pair| pair[0].end_ts == pair[1].start_ts));
    assert_runner_conservation(runners);
}
