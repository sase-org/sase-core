use super::super::alias_history::truncate_prompt_snippet;
use super::super::refresh::refresh_stale_rows_sql;
use super::super::storage::open_index;
use super::super::*;
use super::support::{
    alias_query, artifact, artifact_for_project, write_json, write_text,
};
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
    AgentOutputVariableHistoryQueryWire,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[test]
fn load_agent_artifact_records_returns_full_records_for_dirs_and_aliases() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260827111500");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "hydrate",
            "linked_repos": [{"name": "core"}]
        }),
    );
    write_json(
        &artifact_dir.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": 1779999999.0,
            "name": "hydrate",
            "step_output": {"_raw": "full body", "meta_key": "keep"}
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let alias = "/tmp/alias/hydrate";
    Connection::open(&index)
        .unwrap()
        .execute(
            "INSERT INTO agent_artifact_aliases(alias_path, artifact_dir) \
             VALUES (?1, ?2)",
            params![alias, artifact_dir.to_string_lossy().as_ref()],
        )
        .unwrap();

    let projected = query_agent_artifact_index(
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
            record_shape: AgentArtifactRecordShapeWire::List,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(projected.records.len(), 1);
    assert!(projected.records[0]
        .done
        .as_ref()
        .unwrap()
        .step_output
        .as_ref()
        .unwrap()
        .get("_raw")
        .is_none());

    let loaded = load_agent_artifact_records(
        &index,
        &[
            artifact_dir.to_string_lossy().into_owned(),
            alias.to_string(),
            "/tmp/missing".to_string(),
        ],
    )
    .unwrap();
    assert_eq!(loaded.len(), 2);
    for record in loaded {
        assert_eq!(record.record_shape, AgentArtifactRecordShapeWire::Full);
        assert_eq!(record.artifact_dir, artifact_dir.to_string_lossy());
        assert_eq!(
            record
                .done
                .as_ref()
                .unwrap()
                .step_output
                .as_ref()
                .unwrap()
                .get("_raw"),
            Some(&json!("full body"))
        );
        assert_eq!(record.agent_meta.as_ref().unwrap().linked_repos.len(), 1);
    }
}

#[test]
fn refresh_stale_rows_signature_query_does_not_select_record_json() {
    let sql = refresh_stale_rows_sql("WHERE hidden = 1");
    assert!(sql.contains("agent_meta_sig"));
    assert!(sql.contains("prompt_steps_sig"));
    assert!(!sql.contains("record_json"));
}

#[test]
fn output_variable_history_filters_groups_and_truncates() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let build_root = artifact_for_project(&projects, "proj", "20260814101010");
    let build_worker =
        artifact_for_project(&projects, "proj", "20260814111111");
    let deploy = artifact_for_project(&projects, "other", "20260814121212");
    write_json(
        &build_root.join("agent_meta.json"),
        json!({
            "name": "build",
            "cl_name": "proj",
            "output_variables": {
                "status": "ok",
                "count": 1,
                "report": {"z": 2, "a": 1}
            }
        }),
    );
    write_json(
        &build_worker.join("agent_meta.json"),
        json!({
            "name": "build.worker",
            "hidden": true,
            "output_variables": {
                "status": "ok",
                "count": 1.0
            }
        }),
    );
    write_json(
        &deploy.join("agent_meta.json"),
        json!({
            "name": "deploy",
            "output_variables": {
                "status": "failed",
                "result": "Snowman ☃"
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

    let status_history = query_agent_output_variable_history(
        &index,
        AgentOutputVariableHistoryQueryWire {
            keys: vec!["status".to_string()],
            value_limit: 1,
            ..AgentOutputVariableHistoryQueryWire::default()
        },
    )
    .unwrap();

    assert_eq!(status_history.schema_version, 1);
    assert_eq!(status_history.groups.len(), 1);
    assert_eq!(status_history.keys_limit.total_count, 1);
    let status_group = &status_history.groups[0];
    assert_eq!(status_group.key, "status");
    assert_eq!(status_group.occurrence_count, 2);
    assert_eq!(status_group.distinct_value_count, 2);
    assert!(status_group.values_limit.truncated);
    assert_eq!(status_group.values[0].value, json!("failed"));
    assert_eq!(status_group.values[0].agents, vec!["deploy"]);

    let oldest_first_status = query_agent_output_variable_history(
        &index,
        AgentOutputVariableHistoryQueryWire {
            keys: vec!["status".to_string()],
            reverse: true,
            value_limit: 0,
            ..AgentOutputVariableHistoryQueryWire::default()
        },
    )
    .unwrap();
    assert_eq!(oldest_first_status.groups[0].values[0].value, json!("ok"));

    let build_counts = query_agent_output_variable_history(
        &index,
        AgentOutputVariableHistoryQueryWire {
            agents: vec!["build.*".to_string()],
            keys: vec!["count".to_string()],
            include_hidden: true,
            value_limit: 0,
            ..AgentOutputVariableHistoryQueryWire::default()
        },
    )
    .unwrap();
    let count_values = &build_counts.groups[0].values;
    assert_eq!(build_counts.groups[0].occurrence_count, 2);
    assert_eq!(count_values.len(), 2);
    assert_eq!(count_values[0].value_json, "1.0");
    assert_eq!(count_values[0].agents, vec!["build.worker"]);
    assert_eq!(count_values[1].value_json, "1");
    assert_eq!(count_values[1].agents, vec!["build"]);

    let unicode_result = query_agent_output_variable_history(
        &index,
        AgentOutputVariableHistoryQueryWire {
            values: vec!["snowman".to_string()],
            projects: vec!["other".to_string()],
            since_timestamp: Some("20260814000000".to_string()),
            until_timestamp: Some("20260814235959".to_string()),
            ..AgentOutputVariableHistoryQueryWire::default()
        },
    )
    .unwrap();
    assert_eq!(unicode_result.groups.len(), 1);
    assert_eq!(unicode_result.groups[0].key, "result");
    assert_eq!(unicode_result.groups[0].values[0].value, json!("Snowman ☃"));
}

#[test]
fn output_variable_projection_backfills_replaces_and_deletes_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260814131313");
    let meta_path = artifact_dir.join("agent_meta.json");
    write_json(
        &meta_path,
        json!({
            "name": "writer",
            "output_variables": {
                "status": "old",
                "drop_me": true
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
    assert_eq!(
        agent_artifact_index_status(&index)
            .unwrap()
            .agent_output_variables_rows,
        2
    );

    write_json(
        &meta_path,
        json!({
            "name": "writer",
            "output_variables": {
                "status": "new"
            }
        }),
    );
    upsert_agent_artifact_index_row(
        &index,
        &projects,
        &artifact_dir,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let refreshed = query_agent_output_variable_history(
        &index,
        AgentOutputVariableHistoryQueryWire {
            value_limit: 0,
            key_limit: 0,
            ..AgentOutputVariableHistoryQueryWire::default()
        },
    )
    .unwrap();
    assert_eq!(refreshed.groups.len(), 1);
    assert_eq!(refreshed.groups[0].key, "status");
    assert_eq!(refreshed.groups[0].values[0].value, json!("new"));

    {
        let conn = Connection::open(&index).unwrap();
        conn.execute("DELETE FROM agent_output_variables", [])
            .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '20')",
            [],
        )
        .unwrap();
    }
    let backfilled = agent_artifact_index_status(&index).unwrap();
    assert_eq!(
        backfilled.schema_version,
        AGENT_ARTIFACT_INDEX_SCHEMA_VERSION
    );
    assert_eq!(backfilled.agent_output_variables_rows, 1);

    delete_agent_artifact_index_row(&index, &artifact_dir).unwrap();
    assert_eq!(
        agent_artifact_index_status(&index)
            .unwrap()
            .agent_output_variables_rows,
        0
    );
}

#[test]
fn alias_history_preserves_request_order_and_empty_groups() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let older = artifact(&projects, "20260816010101");
    let newer = artifact(&projects, "20260816020202");
    write_json(
        &older.join("agent_meta.json"),
        json!({
            "name": "older",
            "model_alias": "coder",
            "model_alias_trail": ["coder", "large"],
            "model_alias_origin": "directive"
        }),
    );
    write_json(
        &newer.join("agent_meta.json"),
        json!({
            "name": "newer",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "default_model"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let history = query_agent_alias_history(
        &index,
        alias_query(&["missing", "large", "coder"]),
    )
    .unwrap();
    assert_eq!(history.schema_version, 1);
    assert_eq!(history.groups.len(), 3);
    assert_eq!(history.groups[0].alias, "missing");
    assert!(history.groups[0].runs.is_empty());
    assert_eq!(history.groups[0].runs_limit.total_count, 0);
    assert_eq!(history.groups[1].alias, "large");
    assert_eq!(history.groups[1].runs.len(), 2);
    assert_eq!(
        history.groups[1].runs[0].agent_name.as_deref(),
        Some("newer")
    );
    assert_eq!(
        history.groups[1].runs[1].agent_name.as_deref(),
        Some("older")
    );
    assert_eq!(history.groups[1].runs[1].alias_position, 1);
    assert_eq!(history.groups[2].alias, "coder");
    assert_eq!(history.groups[2].runs.len(), 1);
    assert_eq!(history.groups[2].runs[0].alias_position, 0);
}

#[test]
fn alias_history_truncates_newest_first_and_reports_counts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for (ts, name) in [
        ("20260816010101", "one"),
        ("20260816020202", "two"),
        ("20260816030303", "three"),
    ] {
        let dir = artifact(&projects, ts);
        write_json(
            &dir.join("agent_meta.json"),
            json!({
                "name": name,
                "model_alias": "large",
                "model_alias_trail": ["large"],
                "model_alias_origin": "directive"
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

    let mut query = alias_query(&["large"]);
    query.limit_per_alias = 2;
    let history = query_agent_alias_history(&index, query).unwrap();
    let group = &history.groups[0];
    assert_eq!(group.runs_limit.limit, 2);
    assert_eq!(group.runs_limit.total_count, 3);
    assert_eq!(group.runs_limit.returned_count, 2);
    assert!(group.runs_limit.truncated);
    assert_eq!(group.runs[0].agent_name.as_deref(), Some("three"));
    assert_eq!(group.runs[1].agent_name.as_deref(), Some("two"));

    let mut unlimited = alias_query(&["large"]);
    unlimited.limit_per_alias = 0;
    let all = query_agent_alias_history(&index, unlimited).unwrap();
    assert_eq!(all.groups[0].runs.len(), 3);
    assert!(!all.groups[0].runs_limit.truncated);
}

#[test]
fn alias_history_filters_hidden_and_project_keys() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let visible = artifact_for_project(&projects, "proj", "20260816040404");
    let hidden = artifact_for_project(&projects, "proj", "20260816050505");
    let other = artifact_for_project(&projects, "other", "20260816060606");
    write_json(
        &visible.join("agent_meta.json"),
        json!({
            "name": "visible",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
        }),
    );
    write_json(
        &hidden.join("agent_meta.json"),
        json!({
            "name": "hidden",
            "hidden": true,
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
        }),
    );
    write_json(
        &other.join("agent_meta.json"),
        json!({
            "name": "other",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let default =
        query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
    assert_eq!(default.groups[0].runs.len(), 2);

    let mut hidden_query = alias_query(&["large"]);
    hidden_query.include_hidden = true;
    hidden_query.projects = vec!["proj".to_string()];
    let filtered = query_agent_alias_history(&index, hidden_query).unwrap();
    assert_eq!(filtered.groups[0].runs.len(), 2);
    assert!(filtered.groups[0]
        .runs
        .iter()
        .all(|run| run.project_name == "proj"));
    assert!(filtered.groups[0]
        .runs
        .iter()
        .any(|run| run.agent_name.as_deref() == Some("hidden")));
}

#[test]
fn alias_history_falls_back_to_legacy_first_hop() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260816070707");
    write_json(
        &dir.join("agent_meta.json"),
        json!({
            "name": "legacy",
            "model_alias": "large"
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let history =
        query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
    assert_eq!(history.groups[0].runs.len(), 1);
    assert_eq!(history.groups[0].runs[0].alias_position, 0);
    assert_eq!(
        history.groups[0].runs[0].model_alias.as_deref(),
        Some("large")
    );
    assert_eq!(
        history.groups[0].runs[0].model_alias_trail,
        vec!["large".to_string()]
    );
    assert_eq!(history.groups[0].runs[0].model_alias_origin, None);
}

#[test]
fn alias_history_projection_replaces_and_deletes_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260816080808");
    let meta_path = dir.join("agent_meta.json");
    write_json(
        &meta_path,
        json!({
            "name": "writer",
            "model_alias": "coder",
            "model_alias_trail": ["coder", "large"],
            "model_alias_origin": "directive"
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
        agent_artifact_index_status(&index)
            .unwrap()
            .agent_artifact_model_aliases_rows,
        2
    );

    write_json(
        &meta_path,
        json!({
            "name": "writer",
            "model_alias": "medium",
            "model_alias_trail": ["medium"],
            "model_alias_origin": "default_model"
        }),
    );
    upsert_agent_artifact_index_row(
        &index,
        &projects,
        &dir,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let after_replace = query_agent_alias_history(
        &index,
        alias_query(&["coder", "medium", "large"]),
    )
    .unwrap();
    assert!(after_replace.groups[0].runs.is_empty());
    assert_eq!(after_replace.groups[1].runs.len(), 1);
    assert!(after_replace.groups[2].runs.is_empty());
    assert_eq!(
        agent_artifact_index_status(&index)
            .unwrap()
            .agent_artifact_model_aliases_rows,
        1
    );

    delete_agent_artifact_index_row(&index, &dir).unwrap();
    assert_eq!(
        agent_artifact_index_status(&index)
            .unwrap()
            .agent_artifact_model_aliases_rows,
        0
    );
}

#[test]
fn schema_v21_upgrade_backfills_model_alias_projection() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260816090909");
    write_json(
        &dir.join("agent_meta.json"),
        json!({
            "name": "legacy",
            "model_alias": "large",
            "model_alias_origin": "directive",
            "model_alias_trail": ["coder", "large"]
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
        conn.execute("DELETE FROM agent_artifact_model_aliases", [])
            .unwrap();
        conn.execute(
            "INSERT INTO agent_artifacts (
                artifact_dir, projects_root, project_name, project_dir,
                project_file, workflow_dir_name, timestamp, status,
                agent_type, has_done_marker, has_running_marker,
                has_waiting_marker, has_workflow_state, hidden,
                record_json
            ) VALUES (
                'malformed', 'root', 'proj', 'dir', 'file', 'ace-run',
                '20260816000000', 'done', 'agent', 1, 0, 0, 0, 0,
                '{not-json'
            )",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) \
             VALUES ('schema_version', '21')",
            [],
        )
        .unwrap();
    }

    let status = agent_artifact_index_status(&index).unwrap();
    assert_eq!(status.schema_version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION);
    assert_eq!(status.agent_artifact_model_aliases_rows, 2);
    let history =
        query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
    assert_eq!(history.groups[0].runs.len(), 1);
    assert_eq!(history.groups[0].runs[0].alias_position, 1);
}

#[test]
fn alias_history_prompt_snippets_strip_collapse_and_truncate() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let readable = artifact(&projects, "20260816101010");
    let directives = artifact(&projects, "20260816111111");
    let missing = artifact(&projects, "20260816121212");
    write_json(
        &readable.join("agent_meta.json"),
        json!({
            "name": "readable",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
        }),
    );
    write_text(
        &readable.join("raw_xprompt.md"),
        "%model:@large\n#gh:sase\n\nRefactor   the\nworkspace ☃ module\n",
    );
    write_json(
        &directives.join("agent_meta.json"),
        json!({
            "name": "directives",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
        }),
    );
    write_text(
        &directives.join("raw_xprompt.md"),
        "%model:@large\n#gh:sase\n\n",
    );
    write_json(
        &missing.join("agent_meta.json"),
        json!({
            "name": "missing",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let history =
        query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
    let by_name: BTreeMap<_, _> = history.groups[0]
        .runs
        .iter()
        .map(|run| (run.agent_name.clone().unwrap(), run.clone()))
        .collect();
    assert_eq!(
        by_name["readable"].prompt_snippet.as_deref(),
        Some("Refactor the workspace ☃ module")
    );
    assert_eq!(by_name["directives"].prompt_snippet.as_deref(), Some(""));
    assert_eq!(by_name["missing"].prompt_snippet, None);

    let mut short = alias_query(&["large"]);
    short.prompt_snippet_bytes = 12;
    let truncated = query_agent_alias_history(&index, short).unwrap();
    let readable_snip = truncated.groups[0]
        .runs
        .iter()
        .find(|run| run.agent_name.as_deref() == Some("readable"))
        .unwrap()
        .prompt_snippet
        .as_deref()
        .unwrap();
    assert!(readable_snip.ends_with("..."));
    assert!(readable_snip.is_char_boundary(readable_snip.len()));
    assert!(readable_snip.len() <= 12);
    assert!(!readable_snip.contains("☃") || readable_snip.ends_with("..."));

    let mut skipped = alias_query(&["large"]);
    skipped.prompt_snippet_bytes = 0;
    let no_reads = query_agent_alias_history(&index, skipped).unwrap();
    assert!(no_reads.groups[0]
        .runs
        .iter()
        .all(|run| run.prompt_snippet.is_none()));
}

#[test]
fn alias_history_rejects_empty_aliases() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    drop(open_index(&index).unwrap());
    let err = query_agent_alias_history(&index, alias_query(&[])).unwrap_err();
    assert!(err.contains("non-empty"), "{err}");
}

#[test]
fn alias_history_revalidate_refreshes_candidate_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260816131313");
    let meta_path = dir.join("agent_meta.json");
    write_json(
        &meta_path,
        json!({
            "name": "before",
            "model_alias": "large",
            "model_alias_trail": ["large"],
            "model_alias_origin": "directive"
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
        &meta_path,
        json!({
            "name": "after",
            "model_alias": "coder",
            "model_alias_trail": ["coder", "large"],
            "model_alias_origin": "directive"
        }),
    );
    let cached =
        query_agent_alias_history(&index, alias_query(&["large"])).unwrap();
    assert_eq!(
        cached.groups[0].runs[0].agent_name.as_deref(),
        Some("before")
    );
    assert_eq!(cached.groups[0].runs[0].alias_position, 0);

    let mut revalidate = alias_query(&["large"]);
    revalidate.freshness = AgentArtifactIndexFreshnessWire::Revalidate;
    let fresh = query_agent_alias_history(&index, revalidate).unwrap();
    assert_eq!(fresh.groups[0].runs[0].agent_name.as_deref(), Some("after"));
    assert_eq!(fresh.groups[0].runs[0].alias_position, 1);
    assert_eq!(
        fresh.groups[0].runs[0].model_alias_trail,
        vec!["coder".to_string(), "large".to_string()]
    );
}

#[test]
fn prompt_snippet_truncation_stays_on_utf8_char_boundary() {
    let truncated = truncate_prompt_snippet("ab☃cd", 5);
    assert!(truncated.ends_with("..."));
    assert!(truncated.is_char_boundary(truncated.len()));
    assert!(truncated.len() <= 5);
    assert_eq!(truncated, "ab...");
}

#[test]
fn alias_history_status_counts_projection_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dir = artifact(&projects, "20260816141414");
    write_json(
        &dir.join("agent_meta.json"),
        json!({
            "name": "counted",
            "model_alias": "coder",
            "model_alias_trail": ["coder", "large"],
            "model_alias_origin": "directive"
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let status = agent_artifact_index_status(&index).unwrap();
    assert_eq!(status.schema_version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION);
    assert_eq!(status.agent_artifact_model_aliases_rows, 2);
}
