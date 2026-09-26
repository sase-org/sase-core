use super::super::query::project_record_for_list;
use super::super::storage::open_index;
use super::super::*;
use super::support::{
    artifact, machine_index_query, query_timestamps, write_json,
};
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::collections::BTreeSet;
use tempfile::tempdir;

#[test]
fn machine_candidate_column_is_populated_and_queryable() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let local = artifact(&projects, "20260827103000");
    let from_meta = artifact(&projects, "20260827103100");
    let from_done = artifact(&projects, "20260827103200");
    write_json(&local.join("agent_meta.json"), json!({"name": "local"}));
    write_json(
        &from_meta.join("agent_meta.json"),
        json!({"name": "from-meta", "source_machine": "apollo"}),
    );
    write_json(
        &from_done.join("done.json"),
        json!({
            "outcome": "completed",
            "name": "from-done",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "zeus"
            }
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let conn = Connection::open(&index).unwrap();
    let source_machines = {
        let mut stmt = conn
            .prepare(
                "SELECT timestamp, source_machine FROM agent_artifacts \
                 ORDER BY timestamp",
            )
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    };
    assert_eq!(
        source_machines,
        vec![
            ("20260827103000".to_string(), None),
            ("20260827103100".to_string(), Some("apollo".to_string())),
            ("20260827103200".to_string(), Some("zeus".to_string())),
        ]
    );

    let apollo = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            candidate_filter: Some(AgentArtifactCandidateFilterWire::Equals {
                field: AgentArtifactCandidateFieldWire::Machine,
                value: "apollo".to_string(),
            }),
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(apollo.records.len(), 1);
    assert_eq!(apollo.records[0].artifact_dir, from_meta.to_string_lossy());

    let not_apollo = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            candidate_filter: Some(AgentArtifactCandidateFilterWire::Not {
                filter: Box::new(AgentArtifactCandidateFilterWire::Equals {
                    field: AgentArtifactCandidateFieldWire::Machine,
                    value: "apollo".to_string(),
                }),
            }),
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = not_apollo
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(
        timestamps,
        BTreeSet::from(["20260827103000", "20260827103200"])
    );
}

#[test]
fn machine_candidate_keeps_conflicting_source_and_owner_values() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let conflicting = artifact(&projects, "20260827105000");
    write_json(
        &conflicting.join("agent_meta.json"),
        json!({
            "name": "different-provenance",
            "source_machine": "athena",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "apollo"
            }
        }),
    );
    write_json(
        &conflicting.join("done.json"),
        json!({
            "outcome": "completed",
            "name": "different-provenance",
            "source_machine": "athena",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "apollo"
            }
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let conn = Connection::open(&index).unwrap();
    let row: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT source_machine, imported_owner_machine \
             FROM agent_artifacts WHERE artifact_dir = ?1",
            [conflicting.to_string_lossy().as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (Some("athena".to_string()), Some("apollo".to_string()))
    );

    let apollo = query_timestamps(
        &index,
        &projects,
        machine_index_query("apollo", false),
    );
    assert_eq!(apollo, BTreeSet::from(["20260827105000".to_string()]));
    let athena = query_timestamps(
        &index,
        &projects,
        machine_index_query("athena", false),
    );
    assert_eq!(athena, BTreeSet::from(["20260827105000".to_string()]));
    let not_apollo = query_timestamps(
        &index,
        &projects,
        machine_index_query("apollo", true),
    );
    assert!(not_apollo.is_empty());
}

#[test]
fn machine_candidate_uses_meta_then_done_machine_precedence() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260827105100");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "meta-wins",
            "source_machine": "  Athena ",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "apollo"
            }
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "name": "meta-wins",
            "source_machine": "zeus",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "hera"
            }
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let conn = Connection::open(&index).unwrap();
    let row: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT source_machine, imported_owner_machine \
             FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (Some("Athena".to_string()), Some("apollo".to_string()))
    );

    let timestamps = query_timestamps(
        &index,
        &projects,
        machine_index_query("athena", false),
    );
    assert_eq!(timestamps, BTreeSet::from(["20260827105100".to_string()]));
    assert!(query_timestamps(
        &index,
        &projects,
        machine_index_query("zeus", false),
    )
    .is_empty());
    assert!(query_timestamps(
        &index,
        &projects,
        machine_index_query("hera", false),
    )
    .is_empty());
}

#[test]
fn machine_candidate_includes_mixed_provenance_agent_session_relatives() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let parent = artifact(&projects, "20260827105200");
    let child = artifact(&projects, "20260827105201");
    let unrelated = artifact(&projects, "20260827105202");
    write_json(
        &parent.join("agent_meta.json"),
        json!({
            "name": "crew--plan",
            "agent_family": "crew",
            "agent_family_role": "plan",
            "source_machine": "athena"
        }),
    );
    write_json(
        &parent.join("done.json"),
        json!({"outcome": "completed", "name": "crew--plan"}),
    );
    write_json(
        &child.join("agent_meta.json"),
        json!({
            "name": "crew--code",
            "agent_family": "crew",
            "agent_family_role": "code",
            "parent_timestamp": "20260827105200",
            "source_machine": "athena",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "apollo"
            }
        }),
    );
    write_json(
        &child.join("done.json"),
        json!({"outcome": "completed", "name": "crew--code"}),
    );
    write_json(
        &unrelated.join("agent_meta.json"),
        json!({"name": "other", "source_machine": "hera"}),
    );
    write_json(
        &unrelated.join("done.json"),
        json!({"outcome": "completed", "name": "other"}),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let apollo = query_timestamps(
        &index,
        &projects,
        machine_index_query("apollo", false),
    );
    assert_eq!(
        apollo,
        BTreeSet::from([
            "20260827105200".to_string(),
            "20260827105201".to_string()
        ])
    );
    let not_apollo = query_timestamps(
        &index,
        &projects,
        machine_index_query("apollo", true),
    );
    assert_eq!(
        not_apollo,
        BTreeSet::from([
            "20260827105200".to_string(),
            "20260827105201".to_string(),
            "20260827105202".to_string()
        ])
    );
}

#[test]
fn schema_v29_upgrade_adds_imported_owner_machine_projection() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260827105300");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "legacy-owner",
            "source_machine": "athena",
            "imported_source_owner": {
                "username": "bryan",
                "machine_name": "apollo"
            }
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_agent_artifacts_imported_owner_machine;
             ALTER TABLE agent_artifacts DROP COLUMN imported_owner_machine;
             INSERT OR REPLACE INTO meta(key, value)
             VALUES ('schema_version', '29');",
        )
        .unwrap();
    }

    drop(open_index(&index).unwrap());

    let conn = Connection::open(&index).unwrap();
    let owner: Option<String> = conn
        .query_row(
            "SELECT imported_owner_machine FROM agent_artifacts \
             WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner.as_deref(), Some("apollo"));
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
}

#[test]
fn schema_v27_upgrade_adds_and_backfills_source_machine_projection() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260827104000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "legacy", "source_machine": "apollo"}),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    {
        let conn = Connection::open(&index).unwrap();
        let legacy_record = json!({
            "project_name": "proj",
            "project_dir": projects.join("proj").to_string_lossy(),
            "project_file": projects.join("proj").join("proj.sase").to_string_lossy(),
            "workflow_dir_name": "ace-run",
            "artifact_dir": artifact_dir.to_string_lossy(),
            "timestamp": "20260827104000",
            "agent_meta": {"name": "legacy"},
            "prompt_steps": [],
            "has_done_marker": false
        });
        conn.execute(
            "UPDATE agent_artifacts SET record_json = ?1 WHERE artifact_dir = ?2",
            params![
                legacy_record.to_string(),
                artifact_dir.to_string_lossy().as_ref()
            ],
        )
        .unwrap();
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_agent_artifacts_source_machine;
             ALTER TABLE agent_artifacts DROP COLUMN source_machine;
             INSERT OR REPLACE INTO meta(key, value)
             VALUES ('schema_version', '27');",
        )
        .unwrap();
    }

    drop(open_index(&index).unwrap());

    let conn = Connection::open(&index).unwrap();
    let source_machine: Option<String> = conn
        .query_row(
            "SELECT source_machine FROM agent_artifacts \
             WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(source_machine.as_deref(), Some("apollo"));
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
}

#[test]
fn only_monitors_filters_to_monitor_agent_session_role() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let plain_agent = artifact(&projects, "20260812170000");
    let monitor_member = artifact(&projects, "20260812170100");
    write_json(
        &plain_agent.join("agent_meta.json"),
        json!({"name": "acme--0"}),
    );
    write_json(
        &monitor_member.join("agent_meta.json"),
        json!({
            "name": "acme--mon",
            "agent_family": "acme",
            "agent_family_role": "monitor",
            "monitor_id": "m4kq",
            "monitor_command": "sleep 60",
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let indexed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: true,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(indexed.records.len(), 1);
    assert_eq!(
        indexed.records[0]
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.agent_session_turn.as_ref())
            .and_then(|turn| turn.id.as_deref()),
        Some("m4kq")
    );
}

#[test]
fn list_record_shape_projects_only_heavy_leaves() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260827110000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "projected",
            "cl_name": "cl_projected",
            "linked_repos": [
                {"name": "core", "workspace_dir": "/tmp/core"}
            ]
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "projected",
            "cl_name": "cl_projected",
            "step_output": {
                "_raw": "done raw",
                "_data": {"body": "done data"},
                "meta_commit_message": "keep",
                "meta_commits": [{"sha": "abc123"}]
            }
        }),
    );
    write_json(
        &artifact_dir.join("workflow_state.json"),
        json!({
            "workflow_name": "projected",
            "status": "completed",
            "steps": [
                {
                    "name": "build",
                    "status": "completed",
                    "output": {
                        "_raw": "workflow raw",
                        "_data": "workflow data",
                        "meta_workflow": "keep"
                    }
                }
            ]
        }),
    );
    write_json(
        &artifact_dir.join("prompt_step_001.json"),
        json!({
            "workflow_name": "projected",
            "step_name": "build",
            "step_type": "exec",
            "status": "completed",
            "output": {
                "_raw": "prompt raw",
                "_data": "prompt data",
                "meta_prompt": "keep"
            }
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let full = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: true,
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
    let list = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            record_shape: AgentArtifactRecordShapeWire::List,
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(full.records.len(), 1);
    assert_eq!(list.records.len(), 1);
    let full_record = &full.records[0];
    let list_record = &list.records[0];
    assert_eq!(full_record.record_shape, AgentArtifactRecordShapeWire::Full);
    assert_eq!(list_record.record_shape, AgentArtifactRecordShapeWire::List);
    assert!(serde_json::to_value(full_record)
        .unwrap()
        .get("record_shape")
        .is_none());
    assert_eq!(
        serde_json::to_value(list_record).unwrap()["record_shape"],
        json!("list")
    );

    let mut expected = full_record.clone();
    project_record_for_list(&mut expected);
    assert_eq!(list_record, &expected);
    assert_eq!(
        list_record.prompt_steps.len(),
        full_record.prompt_steps.len()
    );
    assert_eq!(
        list_record.workflow_state.as_ref().unwrap().steps.len(),
        full_record.workflow_state.as_ref().unwrap().steps.len()
    );
    assert_eq!(
        list_record
            .done
            .as_ref()
            .unwrap()
            .step_output
            .as_ref()
            .unwrap()
            .get("meta_commit_message"),
        Some(&json!("keep"))
    );
    assert!(list_record
        .done
        .as_ref()
        .unwrap()
        .step_output
        .as_ref()
        .unwrap()
        .get("_raw")
        .is_none());
    assert!(list_record
        .agent_meta
        .as_ref()
        .unwrap()
        .linked_repos
        .is_empty());

    let stored_json: String = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    let stored_value: serde_json::Value =
        serde_json::from_str(&stored_json).unwrap();
    assert!(stored_value.get("record_shape").is_none());
    let stored_record: AgentArtifactRecordWire =
        serde_json::from_str(&stored_json).unwrap();
    assert_eq!(serde_json::to_string(&stored_record).unwrap(), stored_json);
}
