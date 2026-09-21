//! Runner occupancy tests: overlap, carry, handoff, and lanes.

use serde_json::json;
use tempfile::tempdir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire,
};

use super::super::query::query_run_stats_with_liveness;
use super::super::*;
use super::support::*;

#[test]
fn runner_occupancy_handles_overlap_carry_in_waits_and_boundaries() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");

    add_run(
        &projects,
        &runner_artifact_timestamp(-20),
        json!({"name": "carry", "run_started_at": runner_time(-20)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 40) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(10),
        json!({
            "name": "stopped-first",
            "run_started_at": runner_time(10),
            "stopped_at": runner_time(50)
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 80) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(40),
        json!({"name": "boundary", "run_started_at": runner_time(40)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 60) as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(20),
        json!({
            "name": "planner",
            "run_started_at": runner_time(20),
            "plan_submitted_at": [runner_time(30)],
            "feedback_submitted_at": [runner_time(50)]
        }),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 80) as f64
        })),
        false,
    );
    let live = add_run(
        &projects,
        &runner_artifact_timestamp(70),
        json!({"name": "live", "run_started_at": runner_time(70)}),
        None,
        false,
    );
    write_json(
        &live.join("pending_question.json"),
        json!({"submitted_at": runner_time(90)}),
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(-10),
        json!({"name": "ends-at-start", "run_started_at": runner_time(-10)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": RUNNER_BASE as f64
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(100),
        json!({"name": "starts-at-end", "run_started_at": runner_time(100)}),
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + 110) as f64
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
    let live_probe = |record: &AgentArtifactRecordWire| {
        record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.name.as_deref())
            == Some("live")
    };
    let result = query_run_stats_with_liveness(
        &index,
        runner_request(0, 100, 30),
        &live_probe,
    )
    .unwrap();
    let runners = result.runners.as_ref().unwrap();

    assert_eq!(result.schema_version, 6);
    assert_eq!(result.totals.runs, 4);
    assert_eq!(runners.start_ts, RUNNER_BASE as f64);
    assert_eq!(runners.end_ts, (RUNNER_BASE + 100) as f64);
    assert_eq!(runners.peak_runners, 3);
    assert_eq!(runners.peak_seconds, 30.0);
    assert_eq!(runners.average_runners, 1.8);
    assert_eq!(runners.busy_seconds, 90.0);
    assert_eq!(runners.busy_share, 0.9);
    assert_eq!(runners.runner_seconds, 180.0);
    assert_eq!(
        runners
            .distribution
            .iter()
            .map(|row| (row.runners, row.seconds))
            .collect::<Vec<_>>(),
        vec![(0, 10.0), (1, 30.0), (2, 30.0), (3, 30.0)]
    );
    assert_eq!(runners.trend.len(), 4);
    assert_eq!(runners.trend[0].average_runners, 2.0);
    assert_eq!(runners.trend[0].peak_runners, 3);
    assert_eq!(runners.trend[0].runner_seconds, 60.0);
    assert_eq!(runners.trend[1].average_runners, 8.0 / 3.0);
    assert_eq!(runners.trend[2].average_runners, 4.0 / 3.0);
    assert_eq!(runners.trend[3].start_ts, (RUNNER_BASE + 90) as f64);
    assert_eq!(runners.trend[3].end_ts, (RUNNER_BASE + 100) as f64);
    assert_eq!(runners.trend[3].runner_seconds, 0.0);
    assert_eq!(runners.lanes_counted, 5);
    assert_runner_conservation(runners);
}

#[test]
fn runner_occupancy_merges_serial_family_and_counts_parallel() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let terminal_at = |offset: i64| {
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + offset) as f64
        }))
    };

    add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({
            "name": "root",
            "agent_family": "fam",
            "run_started_at": runner_time(0)
        }),
        terminal_at(20),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(30),
        json!({
            "name": "monitor",
            "agent_family": "fam",
            "agent_family_role": "monitor",
            "monitor_id": "mon-1",
            "run_started_at": runner_time(30)
        }),
        terminal_at(80),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(80),
        json!({
            "name": "followup",
            "agent_family": "fam",
            "parent_timestamp": runner_artifact_timestamp(0),
            "run_started_at": runner_time(80)
        }),
        terminal_at(100),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(70),
        json!({
            "name": "parallel",
            "agent_family": "fam",
            "parent_timestamp": runner_artifact_timestamp(0),
            "agent_family_parallel": true,
            "run_started_at": runner_time(70)
        }),
        terminal_at(90),
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let result = query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = result.runners.as_ref().unwrap();

    // Serial family occupies [0, 100] after the starter-to-monitor gap is
    // filled and the follow-up abuts the monitor. The parallel member
    // adds a second slot on [70, 90].
    assert_eq!(runners.peak_runners, 2);
    assert_eq!(runners.runner_seconds, 120.0);
    assert_eq!(runners.busy_seconds, 100.0);
    assert_eq!(
        runners
            .distribution
            .iter()
            .map(|row| (row.runners, row.seconds))
            .collect::<Vec<_>>(),
        vec![(0, 0.0), (1, 80.0), (2, 20.0)]
    );
    assert_eq!(runners.lanes_counted, 4);
    assert_runner_conservation(runners);
}

#[test]
fn runner_monitor_handoff_is_query_window_invariant() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let terminal_at = |offset: i64| {
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + offset) as f64
        }))
    };

    add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({
            "name": "starter",
            "agent_family": "fam",
            "run_started_at": runner_time(0)
        }),
        terminal_at(20),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(10),
        json!({
            "name": "monitor",
            "agent_family": "fam",
            "agent_family_role": "monitor",
            "monitor_id": "mon-1",
            "run_started_at": runner_time(30)
        }),
        terminal_at(80),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(80),
        json!({
            "name": "followup",
            "agent_family": "fam",
            "parent_timestamp": runner_artifact_timestamp(0),
            "run_started_at": runner_time(80)
        }),
        terminal_at(100),
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let broad = query_run_stats(&index, runner_request(0, 100, 25)).unwrap();
    let nested = query_run_stats(&index, runner_request(25, 75, 25)).unwrap();
    let broad_runners = broad.runners.as_ref().unwrap();
    let nested_runners = nested.runners.as_ref().unwrap();

    assert_eq!(broad_runners.peak_runners, 1);
    assert_eq!(broad_runners.runner_seconds, 100.0);
    assert_eq!(nested_runners.peak_runners, 1);
    assert_eq!(nested_runners.runner_seconds, 50.0);
    assert_eq!(broad_runners.trend[1].peak_runners, 1);
    assert_eq!(broad_runners.trend[2].peak_runners, 1);
    assert_runner_conservation(broad_runners);
    assert_runner_conservation(nested_runners);
}

#[test]
fn runner_inherited_monitor_id_and_artifact_stamp_do_not_move_start_back() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let terminal_at = |offset: i64| {
        Some(json!({
            "outcome": "completed",
            "finished_at": (RUNNER_BASE + offset) as f64
        }))
    };

    add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({
            "name": "baseline",
            "run_started_at": runner_time(0)
        }),
        terminal_at(60),
        false,
    );
    for (offset, start, role) in
        [(5, 70, "root"), (10, 80, "code"), (15, 90, "feedback")]
    {
        add_run(
            &projects,
            &runner_artifact_timestamp(offset),
            json!({
                "name": format!("ordinary-{offset}"),
                "agent_family_role": role,
                "monitor_id": "mon-1",
                "run_started_at": runner_time(start)
            }),
            terminal_at(100),
            false,
        );
    }
    add_run(
        &projects,
        &runner_artifact_timestamp(3_600),
        json!({
            "name": "true-monitor-later",
            "agent_family_role": "monitor",
            "monitor_id": "mon-2",
            "run_started_at": runner_time(5 * 3_600)
        }),
        terminal_at(6 * 3_600),
        false,
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let early = query_run_stats(&index, runner_request(0, 60, 60)).unwrap();
    let broad = query_run_stats(&index, runner_request(0, 120, 60)).unwrap();
    let late_monitor_span =
        query_run_stats(&index, runner_request(0, 6 * 3_600, 3_600)).unwrap();
    let early_runners = early.runners.as_ref().unwrap();
    let broad_runners = broad.runners.as_ref().unwrap();
    let late_monitor_runners = late_monitor_span.runners.as_ref().unwrap();

    assert_eq!(early_runners.peak_runners, 1);
    assert_eq!(early_runners.runner_seconds, 60.0);
    assert_eq!(broad_runners.trend[0].peak_runners, 1);
    assert_eq!(broad_runners.peak_runners, 3);
    assert_eq!(late_monitor_runners.trend[1].peak_runners, 0);
    assert_eq!(late_monitor_runners.trend[5].peak_runners, 1);
    assert_runner_conservation(early_runners);
    assert_runner_conservation(broad_runners);
    assert_runner_conservation(late_monitor_runners);
}

#[test]
fn runner_recovers_synthesized_hidden_lanes_without_trusting_the_stamp() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    add_run(
        &projects,
        &runner_artifact_timestamp(0),
        json!({
            "name": "recovered",
            "run_started_at": runner_time(0),
            "stopped_at": runner_time(10)
        }),
        Some(json!({
            "outcome": "abandoned",
            "finished_at": (RUNNER_BASE + 40 * 60 * 60) as f64,
            "hidden": true
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(20),
        json!({
            "name": "no-end",
            "run_started_at": runner_time(20)
        }),
        Some(json!({
            "outcome": "abandoned",
            "finished_at": (RUNNER_BASE + 40 * 60 * 60) as f64,
            "hidden": true
        })),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(30),
        json!({
            "name": "user-hidden",
            "run_started_at": runner_time(30),
            "hidden": true
        }),
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
    let result = query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = result.runners.as_ref().unwrap();

    assert_eq!(result.totals.runs, 2);
    assert_eq!(runners.runner_seconds, 10.0);
    assert_eq!(runners.peak_runners, 1);
    assert_eq!(runners.lanes_counted, 1);
    assert_eq!(runners.lanes_without_end_skipped, 1);
    assert_eq!(runners.user_hidden_skipped, 1);
    assert_eq!(runners.invalid_intervals_skipped, 0);
    assert_runner_conservation(runners);
}

#[test]
fn user_hidden_skipped_counts_only_runner_eligible_rows() {
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
        json!({
            "name": "hidden-runner",
            "run_started_at": runner_time(0),
            "hidden": true
        }),
        terminal(),
        false,
    );
    add_run(
        &projects,
        &runner_artifact_timestamp(1),
        json!({
            "name": "hidden-serial",
            "run_started_at": runner_time(0),
            "parent_timestamp": "parent",
            "hidden": true
        }),
        terminal(),
        false,
    );
    let hidden_hook = artifact_for_workflow(
        &projects,
        "proj",
        "hook-run",
        &runner_artifact_timestamp(2),
    );
    write_json(
        &hidden_hook.join("agent_meta.json"),
        json!({
            "name": "hidden-hook",
            "run_started_at": runner_time(0),
            "hidden": true
        }),
    );
    write_json(&hidden_hook.join("done.json"), terminal().unwrap());

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let result = query_run_stats(&index, runner_request(0, 100, 100)).unwrap();
    let runners = result.runners.as_ref().unwrap();

    assert_eq!(result.totals.runs, 0);
    assert_eq!(runners.user_hidden_skipped, 2);
    assert_eq!(runners.lanes_counted, 0);
    assert_runner_conservation(runners);
}
