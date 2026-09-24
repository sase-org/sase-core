use super::super::*;
use super::support::{
    artifact, artifact_for_project, projection_full_history_query,
    projection_windowed_query, rebuild_index, windowed_index_query, write_json,
};
use crate::agent_scan::scanner::scan_agent_artifacts;
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::collections::BTreeSet;
use tempfile::tempdir;

#[test]
fn rebuild_indexes_scanner_equivalent_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let first = artifact(&projects, "20260504101010");
    let second = artifact(&projects, "20260504111111");
    write_json(
        &first.join("agent_meta.json"),
        json!({"name": "active", "pid": 123, "model": "gpt"}),
    );
    write_json(
        &second.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1777900000.0,
            "name": "done",
            "cl_name": "cl_alpha"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    let update = rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(update.rows_indexed, 2);
    assert_eq!(update.schema_version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION);

    let indexed = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: true,
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
    let source = scan_agent_artifacts(
        &projects,
        AgentArtifactScanOptionsWire::default(),
    );
    assert_eq!(indexed.records, source.records);
}

#[test]
fn index_rebuild_preserves_canonical_queue_capacity_without_waiting_marker() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let running = artifact(&projects, "20260913020000");
    write_json(
        &running.join("agent_meta.json"),
        json!({
            "name": "runner",
            "queue_capacity": 100,
            "queue_capacity_explicit": true,
            "pid": 42
        }),
    );
    write_json(&running.join("running.json"), json!({"pid": 42}));

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
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::List,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    let meta = snapshot.records[0].agent_meta.as_ref().unwrap();
    assert_eq!(meta.queue_capacity, Some(100));
    assert!(meta.queue_capacity_explicit);
    assert_eq!(
        snapshot.records[0].record_shape,
        AgentArtifactRecordShapeWire::List
    );
}

#[test]
fn windowed_query_decodes_only_selected_candidates() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let older = artifact(&projects, "20260827090000");
    let newer = artifact(&projects, "20260827100000");
    for (artifact_dir, name) in [(&older, "older"), (&newer, "newer")] {
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": name, "model": "gpt-5"}),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({"outcome": "completed", "name": name}),
        );
    }

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
            "UPDATE agent_artifacts SET record_json = ?1 WHERE artifact_dir = ?2",
            params!["{not valid json", older.to_string_lossy().as_ref()],
        )
        .unwrap();

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        windowed_index_query(1),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260827100000");
    assert_eq!(snapshot.stats.json_decode_errors, 0);
    let window = snapshot.index_window.unwrap();
    assert_eq!(window.requested_limit, Some(1));
    assert_eq!(window.selected_candidate_count, 1);
    assert_eq!(window.returned_record_count, 1);
    assert_eq!(window.active_candidate_count, 0);
    assert_eq!(window.completed_candidate_count, 2);
    assert!(window.has_more);
    assert!(window.truncated);
}

#[test]
fn agents_list_projection_defaults_off() {
    assert!(!AgentArtifactIndexQueryWire::default().agents_list_projection);
}

#[test]
fn agents_list_projection_skips_marker_only_active_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let waiting = artifact(&projects, "20260828090000");
    write_json(
        &waiting.join("agent_meta.json"),
        json!({"name": "waiting-only", "agent_clan": "diet-clan"}),
    );
    write_json(&waiting.join("waiting.json"), json!({"cl_name": "wait"}));
    let question = artifact(&projects, "20260828090100");
    write_json(
        &question.join("agent_meta.json"),
        json!({"name": "question-only"}),
    );
    write_json(
        &question.join("pending_question.json"),
        json!({"session_id": "q"}),
    );
    let done = artifact(&projects, "20260828090200");
    write_json(&done.join("agent_meta.json"), json!({"name": "done"}));
    write_json(
        &done.join("done.json"),
        json!({"outcome": "completed", "name": "done"}),
    );

    let index = rebuild_index(tmp.path(), &projects);
    let defaulted = query_agent_artifact_index(
        &index,
        &projects,
        windowed_index_query(10),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let default_ts: BTreeSet<&str> = defaulted
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert!(default_ts.contains("20260828090000"));
    assert!(default_ts.contains("20260828090100"));
    assert!(default_ts.contains("20260828090200"));

    let projected = query_agent_artifact_index(
        &index,
        &projects,
        projection_windowed_query(10),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(projected.records.len(), 1);
    assert_eq!(projected.records[0].timestamp, "20260828090200");
    assert_eq!(projected.stats.record_json_decoded, 1);
    let window = projected.index_window.unwrap();
    assert_eq!(window.active_candidate_count, 0);
    assert_eq!(window.completed_candidate_count, 1);
    assert_eq!(window.returned_record_count, 1);
}

#[test]
fn agents_list_projection_keeps_home_running_and_workflow_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let home_running =
        artifact_for_project(&projects, "home", "20260828100000");
    write_json(
        &home_running.join("agent_meta.json"),
        json!({"name": "home-run", "pid": 42}),
    );
    write_json(&home_running.join("running.json"), json!({"pid": 42}));
    let workflow = projects
        .join("proj")
        .join("artifacts")
        .join("workflow-feature")
        .join("20260828100100");
    write_json(
        &workflow.join("agent_meta.json"),
        json!({"name": "wf", "workflow_name": "feature"}),
    );
    write_json(
        &workflow.join("workflow_state.json"),
        json!({
            "workflow_name": "feature",
            "status": "running",
            "pid": 7,
            "steps": []
        }),
    );
    let waiting = artifact(&projects, "20260828100200");
    write_json(&waiting.join("agent_meta.json"), json!({"name": "wait"}));
    write_json(&waiting.join("waiting.json"), json!({}));

    let index = rebuild_index(tmp.path(), &projects);
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        projection_windowed_query(10),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(
        timestamps,
        BTreeSet::from(["20260828100000", "20260828100100"])
    );
    assert_eq!(snapshot.stats.record_json_decoded, 2);
}

#[test]
fn agents_list_projection_keeps_noop_done_and_skips_hidden() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let noop = artifact(&projects, "20260828110000");
    write_json(&noop.join("agent_meta.json"), json!({"name": "noop"}));
    write_json(
        &noop.join("done.json"),
        json!({"outcome": "noop", "name": "noop"}),
    );
    let hidden = artifact(&projects, "20260828110100");
    write_json(
        &hidden.join("agent_meta.json"),
        json!({"name": "hidden", "hidden": true}),
    );
    write_json(
        &hidden.join("done.json"),
        json!({"outcome": "completed", "name": "hidden", "hidden": true}),
    );
    let visible = artifact(&projects, "20260828110200");
    write_json(&visible.join("agent_meta.json"), json!({"name": "visible"}));
    write_json(
        &visible.join("done.json"),
        json!({"outcome": "completed", "name": "visible"}),
    );

    let index = rebuild_index(tmp.path(), &projects);
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        projection_windowed_query(10),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert!(timestamps.contains("20260828110000"));
    assert!(timestamps.contains("20260828110200"));
    assert!(!timestamps.contains("20260828110100"));
}

#[test]
fn agents_list_projection_derives_clan_context_from_waiting_only_scalars() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let waiting = artifact(&projects, "20260828120000");
    write_json(
        &waiting.join("agent_meta.json"),
        json!({
            "name": "waiting-context",
            "agent_clan": "running-clan",
            "agent_clan_generation": "g1",
            "clan_tribe": "chop",
            "clan_summary": "Waiting supplies context"
        }),
    );
    write_json(&waiting.join("waiting.json"), json!({"cl_name": "run"}));
    let done = artifact(&projects, "20260828120100");
    write_json(
        &done.join("agent_meta.json"),
        json!({
            "name": "done-member",
            "agent_clan": "running-clan",
            "agent_clan_generation": "g1"
        }),
    );
    write_json(
        &done.join("done.json"),
        json!({"outcome": "completed", "name": "done-member"}),
    );

    let index = rebuild_index(tmp.path(), &projects);
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        projection_windowed_query(10),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260828120100");
    assert_eq!(snapshot.stats.record_json_decoded, 1);
    assert_eq!(snapshot.clan_context.len(), 1);
    assert_eq!(snapshot.clan_context[0].agent_clan, "running-clan");
    assert_eq!(
        snapshot.clan_context[0].agent_clan_generation.as_deref(),
        Some("g1")
    );
    assert_eq!(snapshot.clan_context[0].clan_tribe.as_deref(), Some("chop"));
    assert_eq!(
        snapshot.clan_context[0].clan_summary.as_deref(),
        Some("Waiting supplies context")
    );
}

#[test]
fn agents_list_projection_preserves_agent_session_relative_projectable_extras()
{
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let parent = artifact(&projects, "20260828130000");
    write_json(
        &parent.join("agent_meta.json"),
        json!({
            "name": "parent",
            "agent_family": "crew",
            "source_machine": "athena"
        }),
    );
    write_json(
        &parent.join("done.json"),
        json!({"outcome": "completed", "name": "parent"}),
    );
    let child = artifact(&projects, "20260828130100");
    write_json(
        &child.join("agent_meta.json"),
        json!({
            "name": "child",
            "agent_family": "crew",
            "source_machine": "apollo"
        }),
    );
    write_json(
        &child.join("done.json"),
        json!({"outcome": "completed", "name": "child"}),
    );
    let waiting_relative = artifact(&projects, "20260828130200");
    write_json(
        &waiting_relative.join("agent_meta.json"),
        json!({
            "name": "waiting-relative",
            "agent_family": "crew",
            "source_machine": "apollo"
        }),
    );
    write_json(&waiting_relative.join("waiting.json"), json!({}));

    let index = rebuild_index(tmp.path(), &projects);
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            record_shape: AgentArtifactRecordShapeWire::List,
            window_limit: Some(10),
            agents_list_projection: true,
            candidate_filter: Some(AgentArtifactCandidateFilterWire::Equals {
                field: AgentArtifactCandidateFieldWire::Machine,
                value: "athena".to_string(),
            }),
            ..AgentArtifactIndexQueryWire::default()
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert!(timestamps.contains("20260828130000"));
    assert!(timestamps.contains("20260828130100"));
    assert!(!timestamps.contains("20260828130200"));
}

#[test]
fn agents_list_projection_full_history_skips_non_projectable_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let waiting = artifact(&projects, "20260828140000");
    write_json(&waiting.join("agent_meta.json"), json!({"name": "waiting"}));
    write_json(&waiting.join("waiting.json"), json!({}));
    let done = artifact(&projects, "20260828140100");
    write_json(&done.join("agent_meta.json"), json!({"name": "done"}));
    write_json(
        &done.join("done.json"),
        json!({"outcome": "completed", "name": "done"}),
    );

    let index = rebuild_index(tmp.path(), &projects);
    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        projection_full_history_query(),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].timestamp, "20260828140100");
    assert_eq!(snapshot.stats.record_json_decoded, 1);
}

#[test]
fn windowed_query_preserves_active_rows_and_selects_completed_budget() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for ts in ["20260827090000", "20260827090100", "20260827090200"] {
        write_json(
            &artifact(&projects, ts).join("agent_meta.json"),
            json!({"name": format!("active-{ts}")}),
        );
    }
    for ts in ["20260827090300", "20260827090400"] {
        let artifact_dir = artifact(&projects, ts);
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("done-{ts}")}),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({"outcome": "completed", "name": format!("done-{ts}")}),
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
        windowed_index_query(4),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();

    assert_eq!(timestamps.len(), 5);
    assert!(timestamps.contains("20260827090000"));
    assert!(timestamps.contains("20260827090100"));
    assert!(timestamps.contains("20260827090200"));
    assert!(timestamps.contains("20260827090300"));
    assert!(timestamps.contains("20260827090400"));
    let window = snapshot.index_window.unwrap();
    assert_eq!(window.selected_candidate_count, 5);
    assert_eq!(window.returned_record_count, 5);
    assert_eq!(window.active_candidate_count, 3);
    assert_eq!(window.completed_candidate_count, 2);
    assert!(!window.has_more);
    assert!(!window.truncated);
}

#[test]
fn windowed_query_selects_completed_budget_when_active_exceeds_limit() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for ts in [
        "20260827090000",
        "20260827090100",
        "20260827090200",
        "20260827090300",
        "20260827090400",
    ] {
        write_json(
            &artifact(&projects, ts).join("agent_meta.json"),
            json!({"name": format!("active-{ts}")}),
        );
    }
    for ts in ["20260827090500", "20260827090600", "20260827090700"] {
        let artifact_dir = artifact(&projects, ts);
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("done-{ts}")}),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({"outcome": "completed", "name": format!("done-{ts}")}),
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
        windowed_index_query(2),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: BTreeSet<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();

    assert_eq!(timestamps.len(), 7);
    assert!(timestamps.contains("20260827090000"));
    assert!(timestamps.contains("20260827090100"));
    assert!(timestamps.contains("20260827090200"));
    assert!(timestamps.contains("20260827090300"));
    assert!(timestamps.contains("20260827090400"));
    assert!(timestamps.contains("20260827090600"));
    assert!(timestamps.contains("20260827090700"));
    assert!(!timestamps.contains("20260827090500"));
    let window = snapshot.index_window.unwrap();
    assert_eq!(window.requested_limit, Some(2));
    assert_eq!(window.selected_candidate_count, 7);
    assert_eq!(window.returned_record_count, 7);
    assert_eq!(window.active_candidate_count, 5);
    assert_eq!(window.completed_candidate_count, 3);
    assert!(window.has_more);
    assert!(window.truncated);
}

#[test]
fn windowed_query_applies_safe_candidate_filter() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let keep = artifact_for_project(&projects, "proj", "20260827101000");
    let wrong_model = artifact_for_project(&projects, "proj", "20260827101100");
    let wrong_project =
        artifact_for_project(&projects, "other", "20260827101200");
    for (artifact_dir, name, cl_name, model, provider) in [
        (&keep, "keep", "target-cl", "claude-opus-4", "anthropic"),
        (
            &wrong_model,
            "wrong-model",
            "target-cl",
            "gpt-5",
            "anthropic",
        ),
        (
            &wrong_project,
            "wrong-project",
            "target-cl",
            "claude-opus-4",
            "anthropic",
        ),
    ] {
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": name,
                "cl_name": cl_name,
                "model": model,
                "llm_provider": provider,
            }),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({"outcome": "completed", "name": name, "cl_name": cl_name}),
        );
    }

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let mut query = windowed_index_query(10);
    query.candidate_filter = Some(AgentArtifactCandidateFilterWire::All {
        filters: vec![
            AgentArtifactCandidateFilterWire::Contains {
                field: AgentArtifactCandidateFieldWire::Model,
                value: "opus".to_string(),
            },
            AgentArtifactCandidateFilterWire::Any {
                filters: vec![
                    AgentArtifactCandidateFilterWire::Equals {
                        field: AgentArtifactCandidateFieldWire::Provider,
                        value: "anthropic".to_string(),
                    },
                    AgentArtifactCandidateFilterWire::Contains {
                        field: AgentArtifactCandidateFieldWire::Cl,
                        value: "target".to_string(),
                    },
                ],
            },
            AgentArtifactCandidateFilterWire::Not {
                filter: Box::new(AgentArtifactCandidateFilterWire::Contains {
                    field: AgentArtifactCandidateFieldWire::Project,
                    value: "other".to_string(),
                }),
            },
        ],
    });

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        query,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].artifact_dir, keep.to_string_lossy());
    let window = snapshot.index_window.unwrap();
    assert_eq!(window.selected_candidate_count, 1);
    assert_eq!(window.completed_candidate_count, 1);
    assert!(!window.has_more);
}

#[test]
fn full_history_query_applies_candidate_filter_before_decoding() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let keep = artifact_for_project(&projects, "proj", "20260827102000");
    let wrong_model = artifact_for_project(&projects, "proj", "20260827102100");
    let wrong_project =
        artifact_for_project(&projects, "other", "20260827102200");
    for (artifact_dir, name, model, provider) in [
        (&keep, "keep", "claude-opus-4", "anthropic"),
        (&wrong_model, "wrong-model", "gpt-5", "openai"),
        (
            &wrong_project,
            "wrong-project",
            "claude-opus-4",
            "anthropic",
        ),
    ] {
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": name,
                "cl_name": "target-cl",
                "model": model,
                "llm_provider": provider,
            }),
        );
        write_json(
            &artifact_dir.join("done.json"),
            json!({
                "outcome": "completed",
                "name": name,
                "cl_name": "target-cl"
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
    Connection::open(&index)
        .unwrap()
        .execute(
            "UPDATE agent_artifacts SET record_json = ?1 WHERE artifact_dir = ?2",
            params!["{not valid json", wrong_model.to_string_lossy().as_ref()],
        )
        .unwrap();

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: Some(AgentArtifactCandidateFilterWire::All {
                filters: vec![
                    AgentArtifactCandidateFilterWire::Any {
                        filters: vec![
                            AgentArtifactCandidateFilterWire::Contains {
                                field: AgentArtifactCandidateFieldWire::Model,
                                value: "opus".to_string(),
                            },
                            AgentArtifactCandidateFilterWire::Equals {
                                field:
                                    AgentArtifactCandidateFieldWire::Provider,
                                value: "anthropic".to_string(),
                            },
                        ],
                    },
                    AgentArtifactCandidateFilterWire::Not {
                        filter: Box::new(
                            AgentArtifactCandidateFilterWire::Contains {
                                field: AgentArtifactCandidateFieldWire::Project,
                                value: "other".to_string(),
                            },
                        ),
                    },
                ],
            }),
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].artifact_dir, keep.to_string_lossy());
    assert_eq!(snapshot.stats.json_decode_errors, 0);
    assert!(snapshot.index_window.is_none());
}
