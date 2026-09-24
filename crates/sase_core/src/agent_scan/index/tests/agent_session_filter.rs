use super::super::*;
use super::support::{artifact, query_timestamps, rebuild_index, write_json};
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use serde_json::json;
use std::collections::BTreeSet;
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
