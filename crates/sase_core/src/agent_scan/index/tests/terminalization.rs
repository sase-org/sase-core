use super::super::refresh::terminalized_abandoned_record;
use super::super::*;
use super::support::{artifact, indexed_finished_at, write_json};
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::fs;
use std::time::{Duration, UNIX_EPOCH};
use tempfile::tempdir;

#[test]
fn terminalize_stale_active_rows_hides_abandoned_record() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521160000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "abandoned", "cl_name": "cl_abandoned"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let update = terminalize_stale_active_agent_artifact_index_rows(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
        0,
        None,
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 1);

    let active = query_agent_artifact_index(
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
    assert!(active.records.is_empty());

    let recent = query_agent_artifact_index(
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
    assert!(recent.records.is_empty());

    let full_history = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
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
    assert!(full_history.records.is_empty());

    let hidden_completed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: true,
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
    assert_eq!(hidden_completed.records.len(), 1);
    assert!(hidden_completed.records[0].has_done_marker);
    assert_eq!(
        hidden_completed.records[0]
            .done
            .as_ref()
            .and_then(|done| done.outcome.as_deref()),
        Some("abandoned")
    );
    assert_eq!(
        hidden_completed.records[0]
            .done
            .as_ref()
            .and_then(|done| done.cl_name.as_deref()),
        Some("cl_abandoned")
    );
    assert!(hidden_completed.records[0]
        .done
        .as_ref()
        .is_some_and(|done| done.hidden));
    assert!(hidden_completed.records[0]
        .done
        .as_ref()
        .is_some_and(|done| done.finished_at.is_some()));
    assert!(hidden_completed.records[0]
        .done
        .as_ref()
        .is_some_and(|done| done.finished_at_estimated));
}

#[test]
fn abandoned_terminalization_prefers_stopped_at_then_directory_mtime() {
    let record = |stopped_at: &str| {
        serde_json::from_value::<AgentArtifactRecordWire>(json!({
            "project_name": "proj",
            "project_dir": "/tmp/proj",
            "project_file": "/tmp/proj/proj.sase",
            "workflow_dir_name": "ace-run",
            "artifact_dir": "/tmp/proj/artifacts/ace-run/record",
            "timestamp": "record",
            "agent_meta": {
                "name": "abandoned",
                "stopped_at": stopped_at
            }
        }))
        .unwrap()
    };
    let latest = UNIX_EPOCH + Duration::from_secs(999);

    let stopped = terminalized_abandoned_record(record("123.5"), Some(latest));
    let done = stopped.done.as_ref().unwrap();
    assert_eq!(done.finished_at, Some(123.5));
    assert!(done.finished_at_estimated);

    let fallback =
        terminalized_abandoned_record(record("not-a-time"), Some(latest));
    let done = fallback.done.as_ref().unwrap();
    assert_eq!(done.finished_at, Some(999.0));
    assert!(done.finished_at_estimated);
}

#[test]
fn missing_done_finished_at_indexes_from_meta_stopped_at() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260828100000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "acme--mon",
            "stopped_at": "1800000000"
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "monitored",
            "name": "acme--mon"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(
        indexed_finished_at(&index, &artifact_dir),
        Some(1_800_000_000.0)
    );
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert!(snapshot.records[0]
        .done
        .as_ref()
        .is_some_and(|done| done.finished_at.is_none()));
}

#[test]
fn explicit_done_finished_at_is_not_overridden_by_stopped_at() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260828101000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "acme--gate",
            "stopped_at": "1800000000"
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "gated",
            "finished_at": 1_777_000_000.0,
            "name": "acme--gate"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(
        indexed_finished_at(&index, &artifact_dir),
        Some(1_777_000_000.0)
    );
}

#[test]
fn done_marker_without_finished_at_or_stopped_at_indexes_null() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260828102000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "legacy"}),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "name": "legacy"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(indexed_finished_at(&index, &artifact_dir), None);
}

#[test]
fn settled_monitor_without_finished_at_stays_in_recent_completed_window() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let older_agent = artifact(&projects, "20260101000000");
    let monitor = artifact(&projects, "20260828103000");
    write_json(
        &older_agent.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1_700_000_000.0,
            "name": "old-agent"
        }),
    );
    write_json(
        &monitor.join("agent_meta.json"),
        json!({
            "name": "acme--mon",
            "agent_family": "acme",
            "agent_family_role": "monitor",
            "monitor_id": "m4kq",
            "monitor_command": "just check-full",
            "stopped_at": "1800000000"
        }),
    );
    write_json(
        &monitor.join("done.json"),
        json!({
            "outcome": "monitored",
            "name": "acme--mon"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(indexed_finished_at(&index, &monitor), Some(1_800_000_000.0));
    assert_eq!(
        indexed_finished_at(&index, &older_agent),
        Some(1_700_000_000.0)
    );

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(1),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let names: Vec<Option<&str>> = snapshot
        .records
        .iter()
        .map(|record| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.as_deref())
                .or_else(|| {
                    record.done.as_ref().and_then(|done| done.name.as_deref())
                })
        })
        .collect();
    assert!(
        names.contains(&Some("acme--mon")),
        "without the stopped_at fallback, COALESCE(finished_at, 0) \
         sorts the monitor to unix epoch 0 and drops it from the \
         recent-completed window; got {names:?}"
    );
    assert!(
        snapshot.records.iter().any(|record| {
            record.timestamp == "20260828103000"
                && record
                    .done
                    .as_ref()
                    .is_some_and(|done| done.finished_at.is_none())
        }),
        "record_json must keep the omitted finished_at; only the \
         summary column is derived"
    );
}

#[test]
fn terminalize_repairs_visible_abandoned_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521160030");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "repair-abandoned", "cl_name": "cl_repaired"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    terminalize_stale_active_agent_artifact_index_rows(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
        0,
        None,
    )
    .unwrap();

    {
        let conn = Connection::open(&index).unwrap();
        let record_json: String = conn
            .query_row(
                "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
                [artifact_dir.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        let mut record: AgentArtifactRecordWire =
            serde_json::from_str(&record_json).unwrap();
        let done = record.done.as_mut().unwrap();
        done.hidden = false;
        done.cl_name = Some("unknown".to_string());
        let corrupted = serde_json::to_string(&record).unwrap();
        conn.execute(
            "UPDATE agent_artifacts \
             SET hidden = 0, cl_name = 'unknown', record_json = ?1 \
             WHERE artifact_dir = ?2",
            params![corrupted, artifact_dir.to_string_lossy().as_ref()],
        )
        .unwrap();
    }

    let update = terminalize_stale_active_agent_artifact_index_rows(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
        0,
        None,
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 1);

    let visible_recent = query_agent_artifact_index(
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
    assert!(visible_recent.records.is_empty());

    let hidden_recent = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: true,
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
    assert_eq!(hidden_recent.records.len(), 1);
    let done = hidden_recent.records[0].done.as_ref().unwrap();
    assert_eq!(done.cl_name.as_deref(), Some("cl_repaired"));
    assert!(done.hidden);

    let conn = Connection::open(&index).unwrap();
    let (hidden, cl_name): (i64, String) = conn
        .query_row(
            "SELECT hidden, cl_name FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(hidden, 1);
    assert_eq!(cl_name, "cl_repaired");
}

#[test]
fn terminalize_stale_active_rows_skips_fresh_missing_marker_race() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521160100");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "fresh"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let update = terminalize_stale_active_agent_artifact_index_rows(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
        24 * 60 * 60,
        None,
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 0);
    assert_eq!(update.rows_skipped, 1);

    let active = query_agent_artifact_index(
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
    assert_eq!(active.records.len(), 1);
}

#[test]
fn terminalize_stale_active_rows_revalidates_new_running_marker() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521160200");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "became-running"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    write_json(
        &artifact_dir.join("running.json"),
        json!({"pid": 1234, "cl_name": "cl"}),
    );

    let update = terminalize_stale_active_agent_artifact_index_rows(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
        0,
        None,
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 0);
    assert_eq!(update.rows_skipped, 1);

    let active = query_agent_artifact_index(
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
    assert_eq!(active.records.len(), 1);
    assert!(active.records[0].running.is_some());
}

#[test]
fn terminalize_stale_active_rows_skips_workspace_claim() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521160300");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "claimed",
            "workspace_num": 2,
        }),
    );
    fs::write(
        projects.join("proj").join("proj.sase"),
        "NAME: proj\nRUNNING:\n  #2 | 1234 | ace-run | cl | 20260521160300\n",
    )
    .unwrap();

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let update = terminalize_stale_active_agent_artifact_index_rows(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
        0,
        None,
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 0);
    assert_eq!(update.rows_skipped, 1);

    let active = query_agent_artifact_index(
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
    assert_eq!(active.records.len(), 1);
}
