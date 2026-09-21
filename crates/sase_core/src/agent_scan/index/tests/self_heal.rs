use super::super::*;
use super::support::{
    artifact, default_query, full_history_cached_query,
    full_history_revalidate_query, write_completed_artifact, write_json,
};
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::collections::BTreeSet;
use std::fs;
use tempfile::tempdir;

#[test]
fn query_self_heals_appended_plan_submitted_at() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521150000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "active",
            "run_started_at": "2026-05-21T15:00:00Z",
            "plan_submitted_at": [],
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let initial = query_agent_artifact_index(
        &index,
        &projects,
        default_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(initial.records.len(), 1);
    assert!(initial.records[0]
        .agent_meta
        .as_ref()
        .unwrap()
        .plan_submitted_at
        .is_empty());

    // Mid-run mutation: state-transition path writes a new plan
    // timestamp directly to agent_meta.json without calling upsert.
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "active",
            "run_started_at": "2026-05-21T15:00:00Z",
            "plan_submitted_at": ["2026-05-21T15:05:00Z"],
        }),
    );

    let refreshed = query_agent_artifact_index(
        &index,
        &projects,
        default_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(refreshed.records.len(), 1);
    let meta = refreshed.records[0].agent_meta.as_ref().unwrap();
    assert_eq!(meta.plan_submitted_at, vec!["2026-05-21T15:05:00Z"]);

    // And the stored row was refreshed so a follow-up direct read of
    // the record_json reflects the new data.
    let stored_json: String = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(stored_json.contains("2026-05-21T15:05:00Z"));
}

#[test]
fn query_self_heals_appended_feedback_submitted_at() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521151500");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "active",
            "run_started_at": "2026-05-21T15:15:00Z",
            "feedback_submitted_at": [],
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "active",
            "run_started_at": "2026-05-21T15:15:00Z",
            "feedback_submitted_at": ["2026-05-21T15:20:00Z"],
        }),
    );

    let refreshed = query_agent_artifact_index(
        &index,
        &projects,
        default_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let meta = refreshed.records[0].agent_meta.as_ref().unwrap();
    assert_eq!(meta.feedback_submitted_at, vec!["2026-05-21T15:20:00Z"]);
}

#[test]
fn query_self_heals_newly_added_run_started_at() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521152000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "starting"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let initial = query_agent_artifact_index(
        &index,
        &projects,
        default_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(initial.records[0]
        .agent_meta
        .as_ref()
        .unwrap()
        .run_started_at
        .is_none());

    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "starting",
            "run_started_at": "2026-05-21T15:21:00Z",
        }),
    );

    let refreshed = query_agent_artifact_index(
        &index,
        &projects,
        default_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let meta = refreshed.records[0].agent_meta.as_ref().unwrap();
    assert_eq!(meta.run_started_at.as_deref(), Some("2026-05-21T15:21:00Z"));
}

#[test]
fn query_self_heals_running_to_done_transition() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521153000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "active", "run_started_at": "2026-05-21T15:30:00Z"}),
    );
    write_json(
        &artifact_dir.join("running.json"),
        json!({"pid": 1234, "cl_name": "cl"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    // Simulate done: remove running.json and write done.json without
    // calling upsert.
    fs::remove_file(artifact_dir.join("running.json")).unwrap();
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "active",
            "cl_name": "cl",
        }),
    );

    let refreshed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(refreshed.records.len(), 1);
    assert!(refreshed.records[0].has_done_marker);
    assert!(refreshed.records[0].running.is_none());
}

#[test]
fn query_self_heals_hidden_to_visible_before_visible_filter() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521153100");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "hidden-then-visible",
            "run_started_at": "2026-05-21T15:31:00Z",
            "hidden": true,
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let hidden = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(hidden.records.is_empty());

    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "hidden-then-visible",
            "run_started_at": "2026-05-21T15:31:00Z",
            "hidden": false,
        }),
    );

    let visible = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(visible.records.len(), 1);
    assert_eq!(visible.records[0].timestamp, "20260521153100");
}

#[test]
fn query_self_heals_waiting_deletion_to_running() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521153200");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "wait-then-run",
            "run_started_at": "2026-05-21T15:32:00Z",
        }),
    );
    write_json(
        &artifact_dir.join("waiting.json"),
        json!({"waiting_for": ["parent"]}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    fs::remove_file(artifact_dir.join("waiting.json")).unwrap();

    let refreshed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(refreshed.records.len(), 1);
    assert!(refreshed.records[0].waiting.is_none());

    let status: String = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT status FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "running");
}

#[test]
fn query_self_heals_pending_question_creation_and_deletion() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521153300");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "question-agent",
            "run_started_at": "2026-05-21T15:33:00Z",
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    write_json(
        &artifact_dir.join("pending_question.json"),
        json!({
            "session_id": "question-session",
            "request_path": "/tmp/question_request.json",
        }),
    );
    let with_question = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(
        with_question.records[0]
            .pending_question
            .as_ref()
            .and_then(|marker| marker.session_id.as_deref()),
        Some("question-session")
    );

    fs::remove_file(artifact_dir.join("pending_question.json")).unwrap();
    let without_question = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(without_question.records[0].pending_question.is_none());
}

#[test]
fn query_self_heals_done_creation_before_completed_filter() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521153400");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "active-then-done",
            "run_started_at": "2026-05-21T15:34:00Z",
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "active-then-done",
            "cl_name": "cl_completed",
        }),
    );

    let completed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(completed.records.len(), 1);
    assert_eq!(completed.records[0].timestamp, "20260521153400");
    assert!(completed.records[0].has_done_marker);
}

#[test]
fn query_skips_rescan_when_signatures_match() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521154000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "active", "run_started_at": "2026-05-21T15:40:00Z"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    // Inject a sentinel into the stored record_json without touching the
    // on-disk marker files. Signatures still match what is on disk, so a
    // correct query path must skip revalidation and return the sentinel
    // unchanged. If rescan ran unconditionally, the sentinel would be
    // overwritten by the real on-disk value.
    let sentinel_name = "sentinel-skip-rescan-marker";
    {
        let conn = Connection::open(&index).unwrap();
        let mut record_json: String = conn
            .query_row(
                "SELECT record_json FROM agent_artifacts \
                 WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        let mut record: AgentArtifactRecordWire =
            serde_json::from_str(&record_json).unwrap();
        if let Some(meta) = record.agent_meta.as_mut() {
            meta.name = Some(sentinel_name.to_string());
        }
        record_json = serde_json::to_string(&record).unwrap();
        conn.execute(
            "UPDATE agent_artifacts SET record_json = ?1 \
             WHERE artifact_dir = ?2",
            params![record_json, artifact_dir.to_string_lossy().as_ref(),],
        )
        .unwrap();
    }

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        default_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    let returned_name = snapshot.records[0]
        .agent_meta
        .as_ref()
        .and_then(|m| m.name.as_deref());
    assert_eq!(returned_name, Some(sentinel_name));
}

#[test]
fn full_history_revalidate_discovers_unindexed_artifact_and_claims_complete() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let first = artifact(&projects, "20260912090000");
    write_completed_artifact(&first, "indexed");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let second = artifact(&projects, "20260912090100");
    write_completed_artifact(&second, "unindexed");

    let cached = query_agent_artifact_index(
        &index,
        &projects,
        full_history_cached_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let cached_names: BTreeSet<&str> = cached
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(cached_names, BTreeSet::from(["20260912090000"]));
    let cached_complete = cached.index_completeness.unwrap();
    assert!(!cached_complete.complete_history);
    assert!(!cached_complete.source_reconciled);

    let fresh = query_agent_artifact_index(
        &index,
        &projects,
        full_history_revalidate_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let fresh_names: BTreeSet<&str> = fresh
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(
        fresh_names,
        BTreeSet::from(["20260912090000", "20260912090100"])
    );
    let completeness = fresh.index_completeness.unwrap();
    assert!(completeness.complete_history);
    assert!(completeness.source_reconciled);
    assert_eq!(fresh.stats.rows_discovered, 1);
}

#[test]
fn full_history_revalidate_drops_deleted_artifact_instead_of_stale_json() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let first = artifact(&projects, "20260912090000");
    let second = artifact(&projects, "20260912090100");
    write_completed_artifact(&first, "keep");
    write_completed_artifact(&second, "delete-me");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    fs::remove_dir_all(&second).unwrap();
    let fresh = query_agent_artifact_index(
        &index,
        &projects,
        full_history_revalidate_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let names: BTreeSet<&str> = fresh
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(names, BTreeSet::from(["20260912090000"]));
    assert_eq!(fresh.stats.rows_removed, 1);
    assert!(fresh.index_completeness.unwrap().complete_history);
}

#[test]
fn cached_full_history_after_reconcile_reuses_watermark_without_discovery() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_completed_artifact(&artifact(&projects, "20260912090000"), "keep");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    query_agent_artifact_index(
        &index,
        &projects,
        full_history_revalidate_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let cached = query_agent_artifact_index(
        &index,
        &projects,
        full_history_cached_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(cached.index_completeness.unwrap().complete_history);
    assert_eq!(cached.stats.rows_discovered, 0);
    assert_eq!(cached.stats.marker_signatures_checked, 0);
    assert_eq!(cached.stats.rows_repaired, 0);
}

#[test]
fn tier1_revalidate_with_candidate_filter_does_not_prefilter_all_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..6 {
        let dir = artifact(&projects, &format!("2026091210000{index}"));
        write_json(
            &dir.join("agent_meta.json"),
            json!({
                "name": format!("row-{index}"),
                "model": if index == 5 { "keep-me" } else { "other" },
            }),
        );
        write_json(
            &dir.join("done.json"),
            json!({
                "outcome": "completed",
                "name": format!("row-{index}")
            }),
        );
    }
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(2),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: Some(
                AgentArtifactCandidateFilterWire::Contains {
                    field: AgentArtifactCandidateFieldWire::Model,
                    value: "keep".to_string(),
                },
            ),
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert!(
        snapshot.stats.marker_signatures_checked <= 2,
        "capped candidate revalidate must not signature-check the whole tier, got {}",
        snapshot.stats.marker_signatures_checked
    );
}

#[test]
fn full_history_revalidate_repairs_hidden_toggle_via_dirty_directory() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260912090000");
    write_completed_artifact(&dir, "visible");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    query_agent_artifact_index(
        &index,
        &projects,
        full_history_revalidate_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    write_json(
        &dir.join("done.json"),
        json!({
            "outcome": "completed",
            "name": "visible",
            "hidden": true
        }),
    );
    let fresh = query_agent_artifact_index(
        &index,
        &projects,
        full_history_revalidate_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(fresh.records.is_empty());
    assert!(fresh.stats.rows_repaired >= 1);
}
