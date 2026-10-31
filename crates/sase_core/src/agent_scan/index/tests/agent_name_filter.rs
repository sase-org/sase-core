use super::super::candidates::session_name_pushdown_predicate;
use super::super::*;
use super::support::{artifact, query_timestamps, rebuild_index, write_json};
use rusqlite::{params_from_iter, Connection};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::tempdir;

fn session_or_name_query(value: &str) -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: true,
        include_full_history: false,
        active_limit: None,
        recent_completed_limit: None,
        include_hidden: true,
        candidate_filter: Some(AgentArtifactCandidateFilterWire::Any {
            filters: vec![
                AgentArtifactCandidateFilterWire::Equals {
                    field: AgentArtifactCandidateFieldWire::AgentSession,
                    value: value.to_string(),
                },
                AgentArtifactCandidateFilterWire::Equals {
                    field: AgentArtifactCandidateFieldWire::AgentName,
                    value: value.to_string(),
                },
            ],
        }),
        ..AgentArtifactIndexQueryWire::default()
    }
}

fn name_query(value: &str) -> AgentArtifactIndexQueryWire {
    AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: true,
        include_full_history: false,
        active_limit: None,
        recent_completed_limit: None,
        include_hidden: true,
        candidate_filter: Some(AgentArtifactCandidateFilterWire::Equals {
            field: AgentArtifactCandidateFieldWire::AgentName,
            value: value.to_string(),
        }),
        ..AgentArtifactIndexQueryWire::default()
    }
}

#[test]
fn agent_name_candidate_sees_session_root_members() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let lane_a_root = artifact(&projects, "20260924100000");
    let lane_a_successor = artifact(&projects, "20260924100100");
    let lane_b = artifact(&projects, "20260924100200");
    let bare = artifact(&projects, "20260924100300");
    // A session's root member keeps `agent_session` null until it gets a
    // successor, so only its `agent_name` identifies the lane.
    write_json(
        &lane_a_root.join("agent_meta.json"),
        json!({"name": "lane-a"}),
    );
    write_json(
        &lane_a_root.join("done.json"),
        json!({"outcome": "completed", "name": "lane-a"}),
    );
    write_json(
        &lane_a_successor.join("agent_meta.json"),
        json!({"name": "lane-a--plan", "agent_session": "lane-a"}),
    );
    write_json(
        &lane_b.join("agent_meta.json"),
        json!({"name": "lane-b--plan", "agent_session": "lane-b"}),
    );
    write_json(&bare.join("agent_meta.json"), json!({"name": "bare"}));
    let index = rebuild_index(tmp.path(), &projects);

    assert_eq!(
        query_timestamps(&index, &projects, session_or_name_query("lane-a")),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string()
        ])
    );
    assert!(query_timestamps(
        &index,
        &projects,
        session_or_name_query("lane-c")
    )
    .is_empty());
    // `Equals` on the new field is case-insensitive like every other
    // candidate field.
    assert_eq!(
        query_timestamps(&index, &projects, session_or_name_query("LANE-A")),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string()
        ])
    );
    // The bare record shares no lane with either filter atom.
    assert!(!query_timestamps(
        &index,
        &projects,
        session_or_name_query("lane-a")
    )
    .contains("20260924100300"));
}

#[test]
fn agent_name_equals_selects_only_that_name() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let lane_a_root = artifact(&projects, "20260924100000");
    let lane_a_successor = artifact(&projects, "20260924100100");
    write_json(
        &lane_a_root.join("agent_meta.json"),
        json!({"name": "lane-a"}),
    );
    write_json(
        &lane_a_successor.join("agent_meta.json"),
        json!({"name": "lane-a--plan", "agent_session": "lane-a"}),
    );
    let index = rebuild_index(tmp.path(), &projects);

    assert_eq!(
        query_timestamps(&index, &projects, name_query("lane-a")),
        BTreeSet::from(["20260924100000".to_string()])
    );
    assert_eq!(
        query_timestamps(&index, &projects, name_query("lane-a--plan")),
        BTreeSet::from(["20260924100100".to_string()])
    );
}

fn name_equals(value: &str) -> AgentArtifactCandidateFilterWire {
    AgentArtifactCandidateFilterWire::Equals {
        field: AgentArtifactCandidateFieldWire::AgentName,
        value: value.to_string(),
    }
}

fn name_filtered_full_history_query(
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

/// Fixture: two rows sharing session `lane-a` under different names, one
/// lane-b row, and one row with a non-ASCII name.
fn name_pushdown_fixture(projects: &Path) {
    let plan = artifact(projects, "20260924100000");
    let code = artifact(projects, "20260924100100");
    let other = artifact(projects, "20260924100200");
    let unicode = artifact(projects, "20260924100300");
    write_json(
        &plan.join("agent_meta.json"),
        json!({"name": "lane-a--plan", "agent_session": "lane-a"}),
    );
    write_json(
        &plan.join("done.json"),
        json!({"outcome": "completed", "name": "lane-a--plan"}),
    );
    write_json(
        &code.join("agent_meta.json"),
        json!({"name": "lane-a--code", "agent_session": "lane-a"}),
    );
    write_json(
        &other.join("agent_meta.json"),
        json!({"name": "lane-b--plan", "agent_session": "lane-b"}),
    );
    write_json(
        &unicode.join("agent_meta.json"),
        json!({"name": "naïve", "agent_session": "other"}),
    );
}

#[test]
fn name_pushdown_matches_rust_post_filter_across_shapes() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    name_pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    // Plain equals, plus the case-insensitive spelling.
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(name_equals("lane-a--plan"))
        ),
        BTreeSet::from(["20260924100000".to_string()])
    );
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(name_equals("LANE-A--PLAN"))
        ),
        BTreeSet::from(["20260924100000".to_string()])
    );
    // An `any` of name terms is the union of the singletons.
    let any = AgentArtifactCandidateFilterWire::Any {
        filters: vec![name_equals("lane-a--plan"), name_equals("lane-a--code")],
    };
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(any)
        ),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string(),
        ])
    );
    // A top-level `all` pushes its name term and leaves the session term
    // to the Rust backstop alongside it.
    let all = AgentArtifactCandidateFilterWire::All {
        filters: vec![
            name_equals("lane-a--plan"),
            AgentArtifactCandidateFilterWire::Equals {
                field: AgentArtifactCandidateFieldWire::AgentSession,
                value: "lane-a".to_string(),
            },
        ],
    };
    assert!(session_name_pushdown_predicate(Some(&all)).is_some());
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(all)
        ),
        BTreeSet::from(["20260924100000".to_string()])
    );
    // The stranded-query shape: one `any` over both columns for two keys.
    let stranded_shape = AgentArtifactCandidateFilterWire::Any {
        filters: vec![
            AgentArtifactCandidateFilterWire::Equals {
                field: AgentArtifactCandidateFieldWire::AgentSession,
                value: "lane-a".to_string(),
            },
            name_equals("lane-a"),
            AgentArtifactCandidateFilterWire::Equals {
                field: AgentArtifactCandidateFieldWire::AgentSession,
                value: "lane-b".to_string(),
            },
            name_equals("lane-b"),
        ],
    };
    assert!(session_name_pushdown_predicate(Some(&stranded_shape)).is_some());
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(stranded_shape)
        ),
        BTreeSet::from([
            "20260924100000".to_string(),
            "20260924100100".to_string(),
            "20260924100200".to_string(),
        ])
    );
}

#[test]
fn name_pushdown_skips_non_ascii_values_but_stays_exact() {
    assert!(session_name_pushdown_predicate(Some(&name_equals(
        "lane-a--plan"
    )))
    .is_some());
    assert!(
        session_name_pushdown_predicate(Some(&name_equals("naïve"))).is_none()
    );

    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    name_pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    // The skipped filter still answers exactly through Rust.
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(name_equals("naïve"))
        ),
        BTreeSet::from(["20260924100300".to_string()])
    );
    assert_eq!(
        query_timestamps(
            &index,
            &projects,
            name_filtered_full_history_query(name_equals("NAÏVE"))
        ),
        BTreeSet::from(["20260924100300".to_string()])
    );
}

#[test]
fn name_pushdown_applies_to_windowed_queries() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    name_pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    let query = AgentArtifactIndexQueryWire {
        freshness: AgentArtifactIndexFreshnessWire::Cached,
        window_limit: Some(100),
        include_hidden: false,
        candidate_filter: Some(name_equals("lane-a--code")),
        ..AgentArtifactIndexQueryWire::default()
    };
    assert_eq!(
        query_timestamps(&index, &projects, query),
        BTreeSet::from(["20260924100100".to_string()])
    );
}

#[test]
fn name_pushdown_uses_nocase_index() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    name_pushdown_fixture(&projects);
    let index = rebuild_index(tmp.path(), &projects);

    let filter = name_equals("lane-a--plan");
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
        details
            .iter()
            .any(|detail| detail
                .contains("idx_agent_artifacts_agent_name_nocase")),
        "expected the NOCASE name index in {details:?}"
    );
}
