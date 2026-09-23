use super::super::storage::open_index;
use super::super::*;
use super::support::{artifact, artifact_for_project, write_json};
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
};
use rusqlite::Connection;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn bounded_query_retains_dismissed_clan_declaration_as_context() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let declarer =
        artifact_for_project(&projects, "declarations", "20260701000000");
    write_json(
        &declarer.join("agent_meta.json"),
        json!({
            "name": "toobig-0.declarer",
            "cl_name": "cl_declarer",
            "agent_clan": "toobig-0",
            "agent_clan_generation": "generation-1",
            "clan_tribe": "chop",
            "clan_summary": "Chop generation"
        }),
    );
    write_json(
        &declarer.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1782864000.0,
            "name": "toobig-0.declarer",
            "cl_name": "cl_declarer"
        }),
    );

    for (timestamp, name) in [
        ("20260701000001", "toobig-0.joiner-a"),
        ("20260701000002", "toobig-0.joiner-b"),
    ] {
        let joiner = artifact(&projects, timestamp);
        write_json(
            &joiner.join("agent_meta.json"),
            json!({
                "name": name,
                "agent_clan": "toobig-0",
                "agent_clan_generation": "generation-1"
            }),
        );
        write_json(
            &joiner.join("waiting.json"),
            json!({"waiting_for": ["predecessor"]}),
        );
    }
    for (offset, timestamp) in
        ["20260702000000", "20260703000000", "20260704000000"]
            .into_iter()
            .enumerate()
    {
        let completed = artifact(&projects, timestamp);
        write_json(
            &completed.join("done.json"),
            json!({
                "outcome": "completed",
                "finished_at": 1782950400.0 + offset as f64,
                "name": format!("recent-{offset}"),
                "cl_name": format!("cl_recent_{offset}")
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
    replace_agent_artifact_index_dismissed_agents(
        &index,
        &[AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "cl_declarer".to_string(),
            raw_suffix: Some("20260701000000".to_string()),
        }],
    )
    .unwrap();

    let bounded = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(1),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire {
            only_projects: vec!["proj".to_string()],
            ..AgentArtifactScanOptionsWire::default()
        },
    )
    .unwrap();
    assert_eq!(bounded.records.len(), 3);
    assert!(!bounded
        .records
        .iter()
        .any(|record| record.timestamp == "20260701000000"));
    let context = bounded
        .clan_context
        .iter()
        .find(|context| context.agent_clan == "toobig-0")
        .unwrap();
    assert_eq!(
        context.agent_clan_generation.as_deref(),
        Some("generation-1")
    );
    assert_eq!(context.clan_tribe.as_deref(), Some("chop"));
    assert_eq!(context.clan_summary.as_deref(), Some("Chop generation"));
    assert_eq!(
        context.clan_tribe_source_launch_timestamp.as_deref(),
        Some("20260701000000")
    );

    let visible_history = query_agent_artifact_index(
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
    assert!(!visible_history
        .records
        .iter()
        .any(|record| record.timestamp == "20260701000000"));
    assert_eq!(
        visible_history.clan_context[0].clan_tribe.as_deref(),
        Some("chop")
    );
}

#[test]
fn recorded_clan_attributes_overlay_index_queries() {
    use crate::agent_clan_record::{
        record_clan_attributes, ClanAttributeSourceWire,
        ClanAttributeUpdateWire, ClanRecordUpdateWire,
    };
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    // A joiner carrying the epic environment tribe; the declarer that
    // held the summary is already deleted.
    let joiner = artifact(&projects, "20260901000001");
    write_json(
        &joiner.join("agent_meta.json"),
        json!({
            "name": "epic-joiner",
            "agent_clan": "recorded-clan",
            "agent_clan_generation": "20260901000000",
            "clan_tribe": "epic"
        }),
    );
    write_json(&joiner.join("waiting.json"), json!({}));
    // A member of an unrelated generation must not see the record.
    let other = artifact(&projects, "20260901000002");
    write_json(
        &other.join("agent_meta.json"),
        json!({
            "name": "other-joiner",
            "agent_clan": "recorded-clan",
            "agent_clan_generation": "20260901999999",
            "clan_tribe": "epic"
        }),
    );
    write_json(&other.join("waiting.json"), json!({}));

    let records_dir = tmp.path().join("agent_clans");
    record_clan_attributes(
        &records_dir,
        ClanRecordUpdateWire {
            clan: "recorded-clan".to_string(),
            generation: "20260901000000".to_string(),
            tribe: Some(ClanAttributeUpdateWire {
                value: Some("custom".to_string()),
                source: ClanAttributeSourceWire::Edited,
                source_identity: Some("tui".to_string()),
            }),
            summary: Some(ClanAttributeUpdateWire {
                value: Some("Recorded summary".to_string()),
                source: ClanAttributeSourceWire::Script,
                source_identity: Some("script".to_string()),
            }),
            summary_script: None,
        },
    )
    .unwrap();

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let options = AgentArtifactScanOptionsWire {
        clan_records_dir: Some(records_dir.to_string_lossy().into_owned()),
        ..AgentArtifactScanOptionsWire::default()
    };
    for agents_list_projection in [false, true] {
        let snapshot = query_agent_artifact_index(
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
                agents_list_projection,
            },
            options.clone(),
        )
        .unwrap();
        let context = snapshot
            .clan_context
            .iter()
            .find(|context| {
                context.agent_clan_generation.as_deref()
                    == Some("20260901000000")
            })
            .unwrap();
        // The edited record beats the member-derived epic tribe.
        assert_eq!(context.clan_tribe.as_deref(), Some("custom"));
        assert_eq!(context.clan_summary.as_deref(), Some("Recorded summary"));
        assert_eq!(context.clan_tribe_source_launch_timestamp, None);
        assert_eq!(context.clan_tribe_source_identity.as_deref(), Some("tui"));
        let other = snapshot
            .clan_context
            .iter()
            .find(|context| {
                context.agent_clan_generation.as_deref()
                    == Some("20260901999999")
            })
            .unwrap();
        assert_eq!(other.clan_tribe.as_deref(), Some("epic"));
        assert_eq!(other.clan_summary, None);
    }
}

#[test]
fn indexed_clan_context_honors_latest_declarations_and_generations() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for (timestamp, generation, tribe, summary) in [
        ("20260710000000", "g1", "alpha", Some("g1 summary")),
        ("20260710000001", "g1", "beta", None),
        ("20260710000002", "g2", "other", Some("g2 summary")),
    ] {
        let declaration = artifact(&projects, timestamp);
        write_json(
            &declaration.join("agent_meta.json"),
            json!({
                "name": format!("declaration-{timestamp}"),
                "agent_clan": "shared",
                "agent_clan_generation": generation,
                "clan_tribe": tribe,
                "clan_summary": summary
            }),
        );
        write_json(
            &declaration.join("done.json"),
            json!({"outcome": "completed", "name": "declaration"}),
        );
    }
    for (timestamp, generation) in [
        ("20260710000003", "g1"),
        ("20260710000004", "g2"),
        ("20260710000005", "g3"),
    ] {
        let joiner = artifact(&projects, timestamp);
        write_json(
            &joiner.join("agent_meta.json"),
            json!({
                "name": format!("joiner-{generation}"),
                "agent_clan": "shared",
                "agent_clan_generation": generation
            }),
        );
        write_json(&joiner.join("waiting.json"), json!({}));
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
    assert_eq!(snapshot.records.len(), 3);
    let context = |generation: &str| {
        snapshot
            .clan_context
            .iter()
            .find(|context| {
                context.agent_clan_generation.as_deref() == Some(generation)
            })
            .unwrap()
    };
    assert_eq!(context("g1").clan_tribe.as_deref(), Some("beta"));
    assert_eq!(context("g1").clan_summary.as_deref(), Some("g1 summary"));
    assert_eq!(context("g2").clan_tribe.as_deref(), Some("other"));
    assert_eq!(context("g2").clan_summary.as_deref(), Some("g2 summary"));
    assert_eq!(context("g3").clan_tribe, None);
    assert_eq!(context("g3").clan_summary, None);
}

#[test]
fn recent_completed_rows_remain_visible_when_not_dismissed() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260514130000");
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1777900100.0,
            "name": "done-visible",
            "cl_name": "cl_visible"
        }),
    );

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
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260514130000");
}

#[test]
fn terminal_workflow_state_rows_are_recent_completed_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260514140000");
    write_json(
        &artifact_dir.join("workflow_state.json"),
        json!({
            "workflow_name": "wf",
            "status": "failed",
            "cl_name": "cl_failed",
            "start_time": "2026-05-14T14:00:00Z",
            "steps": []
        }),
    );

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
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260514140000");
}

#[test]
fn anonymous_appears_as_agent_workflow_is_not_hidden() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521100533");
    write_json(
        &artifact_dir.join("workflow_state.json"),
        json!({
            "workflow_name": "tmp_260521_104058",
            "status": "completed",
            "appears_as_agent": true,
            "is_anonymous": true,
            "hidden": false,
            "start_time": "2026-05-21T10:05:33Z",
            "steps": []
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "tmp_260521_104058",
            "cl_name": "cl_anon",
            "hidden": false
        }),
    );

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
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260521100533");
}

#[test]
fn explicit_workflow_state_hidden_is_still_filtered() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260521100600");
    write_json(
        &artifact_dir.join("workflow_state.json"),
        json!({
            "workflow_name": "tmp_260521_104100",
            "status": "completed",
            "appears_as_agent": true,
            "is_anonymous": true,
            "hidden": true,
            "start_time": "2026-05-21T10:06:00Z",
            "steps": []
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "tmp_260521_104100",
            "cl_name": "cl_hidden"
        }),
    );

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
    assert!(snapshot.records.is_empty());
}

#[test]
fn migration_recomputes_hidden_for_v1_indexes() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    // Visible-but-anonymous: would have been wrongly hidden by v1.
    let anon_dir = artifact(&projects, "20260521110000");
    write_json(
        &anon_dir.join("workflow_state.json"),
        json!({
            "workflow_name": "tmp_anon",
            "status": "completed",
            "appears_as_agent": true,
            "is_anonymous": true,
            "hidden": false,
            "steps": []
        }),
    );
    write_json(
        &anon_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "tmp_anon",
            "cl_name": "cl_anon"
        }),
    );
    // Truly hidden: workflow_state.hidden = true. Must stay hidden.
    let hidden_dir = artifact(&projects, "20260521110001");
    write_json(
        &hidden_dir.join("workflow_state.json"),
        json!({
            "workflow_name": "wf_hidden",
            "status": "completed",
            "hidden": true,
            "steps": []
        }),
    );
    write_json(
        &hidden_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "wf_hidden",
            "cl_name": "cl_hidden"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    // Force the index back to the v1 state: schema_version=1 in meta,
    // and the anonymous row's hidden bit flipped to 1 (matching what
    // the buggy v1 projection would have written).
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) \
             VALUES ('schema_version', '1')",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE agent_artifacts SET hidden = 1 WHERE artifact_dir = ?1",
            [anon_dir.to_string_lossy().as_ref()],
        )
        .unwrap();
    }

    // Re-opening must run the migration and clear the spurious hidden
    // bit on the anonymous row, while leaving the explicit-hidden row
    // untouched.
    let _conn = open_index(&index).unwrap();
    let conn = Connection::open(&index).unwrap();
    let anon_hidden: i64 = conn
        .query_row(
            "SELECT hidden FROM agent_artifacts WHERE artifact_dir = ?1",
            [anon_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(anon_hidden, 0);
    let hidden_hidden: i64 = conn
        .query_row(
            "SELECT hidden FROM agent_artifacts WHERE artifact_dir = ?1",
            [hidden_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hidden_hidden, 1);
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
fn schema_v18_upgrade_adds_xprompts_signature_column() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    drop(open_index(&index).unwrap());
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute_batch(
            "ALTER TABLE agent_artifacts DROP COLUMN xprompts_sig;
             INSERT OR REPLACE INTO meta(key, value)
             VALUES ('schema_version', '18');",
        )
        .unwrap();
    }

    drop(open_index(&index).unwrap());

    let conn = Connection::open(&index).unwrap();
    let columns = {
        let mut stmt =
            conn.prepare("PRAGMA table_info(agent_artifacts)").unwrap();
        let rows = stmt.query_map([], |row| row.get::<_, String>(1)).unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    };
    assert!(columns.iter().any(|column| column == "xprompts_sig"));
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
fn schema_v19_upgrade_refreshes_record_json_for_model_aliases() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    drop(open_index(&index).unwrap());
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value)
             VALUES ('schema_version', '19')",
            [],
        )
        .unwrap();
    }

    drop(open_index(&index).unwrap());

    let conn = Connection::open(&index).unwrap();
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
fn schema_v24_upgrade_backfills_done_outcome_projection() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260827113000");
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "abandoned",
            "finished_at": 1779999999.0,
            "name": "abandoned"
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
            "DROP INDEX IF EXISTS idx_agent_artifacts_done_outcome;
             ALTER TABLE agent_artifacts DROP COLUMN done_outcome;
             INSERT OR REPLACE INTO meta(key, value)
             VALUES ('schema_version', '23');",
        )
        .unwrap();
    }

    drop(open_index(&index).unwrap());

    let conn = Connection::open(&index).unwrap();
    let outcome: Option<String> = conn
        .query_row(
            "SELECT done_outcome FROM agent_artifacts \
             WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outcome.as_deref(), Some("abandoned"));
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
}
