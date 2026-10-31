use super::super::candidates::session_name_pushdown_predicate;
use super::super::storage::read_index_schema_version;
use super::super::*;
use super::support::{artifact, query_timestamps, rebuild_index, write_json};
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use rusqlite::{params_from_iter, Connection};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::tempdir;

fn session_query(value: &str) -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: true,
        include_full_history: false,
        active_limit: None,
        recent_completed_limit: None,
        include_hidden: true,
        candidate_filter: Some(AgentArtifactCandidateFilterWire::Equals {
            field: AgentArtifactCandidateFieldWire::AgentSession,
            value: value.to_string(),
        }),
        ..AgentArtifactIndexQueryWire::default()
    }
}

#[test]
fn agent_session_candidate_selects_only_that_lane() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let lane_a_done = artifact(&projects, "20260924100000");
    let lane_a_active = artifact(&projects, "20260924100100");
    let lane_b = artifact(&projects, "20260924100200");
    let bare = artifact(&projects, "20260924100300");
    write_json(
        &lane_a_done.join("agent_meta.json"),
        json!({"name": "lane-a--plan", "agent_session": "lane-a"}),
    );
    write_json(
        &lane_a_done.join("done.json"),
        json!({"outcome": "completed", "name": "lane-a--plan"}),
    );
    write_json(
        &lane_a_active.join("agent_meta.json"),
        json!({"name": "lane-a--code", "agent_session": "lane-a"}),
    );
    write_json(
        &lane_b.join("agent_meta.json"),
        json!({"name": "lane-b--plan", "agent_session": "lane-b"}),
    );
    write_json(&bare.join("agent_meta.json"), json!({"name": "bare"}));
    let index = rebuild_index(tmp.path(), &projects);

    assert_eq!(
        query_timestamps(&index, &projects, session_query("lane-a")),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string()
        ])
    );
    assert_eq!(
        query_timestamps(&index, &projects, session_query("lane-b")),
        BTreeSet::from(["20260924100200".to_string()])
    );
    assert!(
        query_timestamps(&index, &projects, session_query("lane-c")).is_empty()
    );
    // Equals is case-insensitive like every other candidate field; callers
    // that need an exact lane re-check the hydrated record.
    assert_eq!(
        query_timestamps(&index, &projects, session_query("LANE-B")),
        BTreeSet::from(["20260924100200".to_string()])
    );
}

#[test]
fn agent_session_candidate_decodes_only_matching_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..6 {
        let dir = artifact(&projects, &format!("2026092410{index:02}00"));
        let session = if index == 2 { "lane-a" } else { "other" };
        write_json(
            &dir.join("agent_meta.json"),
            json!({"name": format!("agent-{index}"), "agent_session": session}),
        );
    }
    let index = rebuild_index(tmp.path(), &projects);

    let scan = query_agent_artifact_index(
        &index,
        &projects,
        session_query("lane-a"),
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(scan.records.len(), 1);
    assert_eq!(scan.stats.record_json_decoded, 1);
}

/// Rewind a freshly built index to the on-disk shape a v32 core left
/// behind: the session lane still lives in the legacy `agent_family`
/// column and index.
fn rewind_to_legacy_agent_family_column(index: &Path) {
    Connection::open(index)
        .unwrap()
        .execute_batch(
            "DROP INDEX idx_agent_artifacts_agent_session;
             ALTER TABLE agent_artifacts
                 RENAME COLUMN agent_session TO agent_family;
             CREATE INDEX idx_agent_artifacts_agent_family
                 ON agent_artifacts(agent_family, timestamp);
             UPDATE meta SET value = '32' WHERE key = 'schema_version';",
        )
        .unwrap();
}

fn sqlite_names(conn: &Connection, sql: &str) -> BTreeSet<String> {
    conn.prepare(sql)
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn legacy_agent_family_column_migrates_in_place() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let lane_a = artifact(&projects, "20260924100000");
    let lane_b = artifact(&projects, "20260924100100");
    write_json(
        &lane_a.join("agent_meta.json"),
        json!({"name": "lane-a--plan", "agent_session": "lane-a"}),
    );
    write_json(
        &lane_b.join("agent_meta.json"),
        json!({"name": "lane-b--plan", "agent_session": "lane-b"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    rewind_to_legacy_agent_family_column(&index);

    // The first query after an upgrade opens the v32 index; it must migrate
    // it instead of failing on the renamed column.
    assert_eq!(
        query_timestamps(&index, &projects, session_query("lane-a")),
        BTreeSet::from(["20260924100000".to_string()])
    );

    let conn = Connection::open(&index).unwrap();
    let columns = sqlite_names(
        &conn,
        "SELECT name FROM pragma_table_info('agent_artifacts')",
    );
    assert!(columns.contains("agent_session"));
    assert!(!columns.contains("agent_family"));
    let indexes = sqlite_names(
        &conn,
        "SELECT name FROM sqlite_master \
         WHERE type = 'index' AND tbl_name = 'agent_artifacts'",
    );
    assert!(indexes.contains("idx_agent_artifacts_agent_session"));
    assert!(!indexes.contains("idx_agent_artifacts_agent_family"));
    assert_eq!(
        read_index_schema_version(&conn).unwrap(),
        AGENT_ARTIFACT_INDEX_SCHEMA_VERSION
    );
    let lane: String = conn
        .query_row(
            "SELECT agent_session FROM agent_artifacts WHERE timestamp = ?1",
            ["20260924100100"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(lane, "lane-b");
}

#[test]
fn legacy_agent_family_column_beside_agent_session_merges_in_place() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let lane_a = artifact(&projects, "20260924100000");
    let lane_b = artifact(&projects, "20260924100100");
    write_json(
        &lane_a.join("agent_meta.json"),
        json!({"name": "lane-a--plan", "agent_session": "lane-a"}),
    );
    write_json(
        &lane_b.join("agent_meta.json"),
        json!({"name": "lane-b--plan", "agent_session": "lane-b"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    // A v32 core re-added `agent_family` beside `agent_session` and kept only
    // the legacy column current.
    Connection::open(&index)
        .unwrap()
        .execute_batch(
            "ALTER TABLE agent_artifacts ADD COLUMN agent_family TEXT;
             UPDATE agent_artifacts
                 SET agent_family = agent_session, agent_session = NULL;
             CREATE INDEX idx_agent_artifacts_agent_family
                 ON agent_artifacts(agent_family, timestamp);
             UPDATE meta SET value = '32' WHERE key = 'schema_version';",
        )
        .unwrap();

    // Renaming onto the existing column would fail this open; the legacy
    // lanes must land in `agent_session` instead.
    assert_eq!(
        query_timestamps(&index, &projects, session_query("lane-a")),
        BTreeSet::from(["20260924100000".to_string()])
    );

    let conn = Connection::open(&index).unwrap();
    let columns = sqlite_names(
        &conn,
        "SELECT name FROM pragma_table_info('agent_artifacts')",
    );
    assert!(columns.contains("agent_session"));
    assert!(!columns.contains("agent_family"));
    assert_eq!(
        read_index_schema_version(&conn).unwrap(),
        AGENT_ARTIFACT_INDEX_SCHEMA_VERSION
    );
}

fn session_equals(value: &str) -> AgentArtifactCandidateFilterWire {
    AgentArtifactCandidateFilterWire::Equals {
        field: AgentArtifactCandidateFieldWire::AgentSession,
        value: value.to_string(),
    }
}

fn filtered_full_history_query(
    filter: AgentArtifactCandidateFilterWire,
) -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: false,
        include_recent_completed: false,
        include_full_history: true,
        active_limit: None,
        recent_completed_limit: None,
        include_hidden: true,
        candidate_filter: Some(filter),
        ..AgentArtifactIndexQueryWire::default()
    }
}

/// Fixture: two lane-a rows (done + active, model `GPT-test`), one lane-b
/// row, one row whose session is the `agent` alias target, and one row
/// with a non-ASCII session. Timestamps double as the expected sets.
fn pushdown_fixture(projects: &Path) {
    let lane_a_done = artifact(projects, "20260924100000");
    let lane_a_active = artifact(projects, "20260924100100");
    let lane_b = artifact(projects, "20260924100200");
    let agent_session = artifact(projects, "20260924100300");
    let unicode_session = artifact(projects, "20260924100400");
    write_json(
        &lane_a_done.join("agent_meta.json"),
        json!({
            "name": "lane-a--plan",
            "agent_session": "lane-a",
            "model": "GPT-test",
        }),
    );
    write_json(
        &lane_a_done.join("done.json"),
        json!({"outcome": "completed", "name": "lane-a--plan"}),
    );
    write_json(
        &lane_a_active.join("agent_meta.json"),
        json!({
            "name": "lane-a--code",
            "agent_session": "lane-a",
            "model": "GPT-test",
        }),
    );
    write_json(
        &lane_b.join("agent_meta.json"),
        json!({
            "name": "lane-b--plan",
            "agent_session": "lane-b",
            "model": "gpt-other",
        }),
    );
    write_json(
        &agent_session.join("agent_meta.json"),
        json!({"name": "runner", "agent_session": "agent"}),
    );
    write_json(
        &unicode_session.join("agent_meta.json"),
        json!({"name": "uni", "agent_session": "lane-ä"}),
    );
}

#[test]
fn session_pushdown_matches_rust_post_filter_across_shapes() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    let lane_a = BTreeSet::from([
        "20260924100000".to_string(),
        "20260924100100".to_string(),
    ]);
    // Plain equals, plus the case-insensitive spelling.
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            filtered_full_history_query(session_equals("lane-a"))
        ),
        lane_a
    );
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            filtered_full_history_query(session_equals("LANE-A"))
        ),
        lane_a
    );
    // An `any` of session terms is the union of the singletons.
    let any = AgentArtifactCandidateFilterWire::Any {
        filters: vec![session_equals("lane-a"), session_equals("lane-b")],
    };
    assert_eq!(
        query_timestamps(&index, &projects, filtered_full_history_query(any)),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string(),
            "20260924100200".to_string(),
        ])
    );
    // A top-level `all` pushes its session term and leaves the
    // unpushable `contains` term to the Rust backstop.
    let all = AgentArtifactCandidateFilterWire::All {
        filters: vec![
            session_equals("lane-a"),
            AgentArtifactCandidateFilterWire::Contains {
                field: AgentArtifactCandidateFieldWire::Model,
                value: "gpt".to_string(),
            },
        ],
    };
    assert_eq!(
        query_timestamps(&index, &projects, filtered_full_history_query(all)),
        lane_a
    );
    let all_no_match = AgentArtifactCandidateFilterWire::All {
        filters: vec![
            session_equals("lane-a"),
            AgentArtifactCandidateFilterWire::Contains {
                field: AgentArtifactCandidateFieldWire::Model,
                value: "zzz-no-such-model".to_string(),
            },
        ],
    };
    assert!(query_timestamps(
        &index,
        &projects,
        filtered_full_history_query(all_no_match)
    )
    .is_empty());
    // A partially translatable `any` pushes nothing and still answers
    // exactly through the Rust post-filter.
    let any_mixed = AgentArtifactCandidateFilterWire::Any {
        filters: vec![
            session_equals("lane-a"),
            AgentArtifactCandidateFilterWire::Contains {
                field: AgentArtifactCandidateFieldWire::Model,
                value: "gpt".to_string(),
            },
        ],
    };
    assert!(session_name_pushdown_predicate(Some(&any_mixed)).is_none());
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            filtered_full_history_query(any_mixed)
        ),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string(),
            "20260924100200".to_string(),
        ])
    );
}

#[test]
fn session_pushdown_skips_non_ascii_and_run_alias_values() {
    // Non-ASCII values and the `run`/`running` alias (which also matches a
    // stored `agent` value in Rust) stay out of SQL entirely.
    assert!(
        session_name_pushdown_predicate(Some(&session_equals("lane-a")))
            .is_some()
    );
    assert!(
        session_name_pushdown_predicate(Some(&session_equals("lane-ä")))
            .is_none()
    );
    assert!(
        session_name_pushdown_predicate(Some(&session_equals("run"))).is_none()
    );
    assert!(
        session_name_pushdown_predicate(Some(&session_equals("RUNNING")))
            .is_none()
    );
    // Every other field, `contains`, and `not` never push down.
    let model_equals = AgentArtifactCandidateFilterWire::Equals {
        field: AgentArtifactCandidateFieldWire::Model,
        value: "gpt".to_string(),
    };
    assert!(session_name_pushdown_predicate(Some(&model_equals)).is_none());
    let contains = AgentArtifactCandidateFilterWire::Contains {
        field: AgentArtifactCandidateFieldWire::AgentSession,
        value: "lane".to_string(),
    };
    assert!(session_name_pushdown_predicate(Some(&contains)).is_none());

    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    // The skipped filters still answer exactly through the Rust backstop:
    // the non-ASCII lane matches itself (including case variants), and
    // `run` matches the stored `agent` session through the alias.
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            filtered_full_history_query(session_equals("lane-ä"))
        ),
        BTreeSet::from(["20260924100400".to_string()])
    );
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            filtered_full_history_query(session_equals("LANE-Ä"))
        ),
        BTreeSet::from(["20260924100400".to_string()])
    );
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            filtered_full_history_query(session_equals("run"))
        ),
        BTreeSet::from(["20260924100300".to_string()])
    );
}

#[test]
fn session_pushdown_applies_to_windowed_queries() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    let query = AgentArtifactIndexQueryWire {
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        window_limit: Some(100),
        include_hidden: false,
        candidate_filter: Some(session_equals("lane-a")),
        ..AgentArtifactIndexQueryWire::default()
    };
    assert_eq!(
        query_timestamps(&index, &projects, query),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string()
        ])
    );
}

#[test]
fn session_pushdown_uses_nocase_index() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    let filter = session_equals("lane-a");
    let (predicate, params) =
        session_name_pushdown_predicate(Some(&filter)).unwrap();
    let conn = Connection::open(&index).unwrap();
    let sql = format!(
        "EXPLAIN QUERY PLAN SELECT artifact_dir FROM agent_artifacts \
         WHERE {predicate}"
    );
    let mut stmt = conn.prepare(&sql).unwrap();
    let details: Vec<String> = stmt
        .query_map(params_from_iter(params.iter()), |row| row.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        details.iter().any(|detail| detail
            .contains("idx_agent_artifacts_agent_session_nocase")),
        "expected the NOCASE session index in {details:?}"
    );
}
