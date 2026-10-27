use super::super::*;
use super::support::{artifact, query_timestamps, rebuild_index, write_json};
use serde_json::json;
use std::collections::BTreeSet;
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
