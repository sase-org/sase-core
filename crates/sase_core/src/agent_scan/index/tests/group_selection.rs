use super::super::group_selection::*;
use crate::agent_scan::index::lookup::{
    AgentArtifactLookupAvailabilityWire, AgentArtifactLookupHitWire,
    AgentArtifactLookupQueryWire, AgentArtifactLookupResultWire,
    AgentArtifactLookupTargetWire,
};

fn hit(
    artifact_dir: &str,
    timestamp: &str,
    name: &str,
    parent: Option<&str>,
    generation: Option<&str>,
) -> AgentArtifactLookupHitWire {
    AgentArtifactLookupHitWire {
        artifact_dir: artifact_dir.to_string(),
        project_name: "proj".to_string(),
        workflow_dir_name: "ace-run".to_string(),
        timestamp: timestamp.to_string(),
        agent_name: Some(name.to_string()),
        workflow_name: None,
        agent_session: None,
        lookup_session: Some("lane".to_string()),
        agent_clan: Some("clan".to_string()),
        agent_clan_generation: generation.map(str::to_string),
        parent_timestamp: parent.map(str::to_string),
        has_done_marker: false,
        has_running_marker: false,
        hidden: false,
        match_kind: None,
    }
}

fn lookup(
    target: AgentArtifactLookupTargetWire,
    hits: Vec<AgentArtifactLookupHitWire>,
) -> AgentArtifactLookupResultWire {
    AgentArtifactLookupResultWire {
        schema_version: super::super::AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: "/tmp/index".to_string(),
        projects_root: "/tmp/projects".to_string(),
        query: AgentArtifactLookupQueryWire {
            target,
            include_hidden: true,
            forced_unavailability: None,
        },
        availability: AgentArtifactLookupAvailabilityWire::Ready,
        complete: true,
        source: "index".to_string(),
        diagnostic: None,
        hits,
        truncated: false,
        dirs_visited: 0,
        records_decoded: 0,
    }
}

#[test]
fn latest_session_selects_newest_root_and_transitive_descendants() {
    let names = vec!["lane".to_string(), "athena.lane".to_string()];
    let rows = vec![
        hit("/p/old-root", "100", "lane--plan", None, None),
        hit(
            "/p/old-child",
            "101",
            "athena.lane--code",
            Some("100"),
            None,
        ),
        hit("/p/new-root", "200", "lane--plan", None, None),
        hit("/p/new-child", "201", "lane--commit", Some("200"), None),
        hit("/p/unrelated", "202", "lane--gate", Some("100"), None),
    ];
    let lookup = lookup(
        AgentArtifactLookupTargetWire::Session {
            names: names.clone(),
        },
        rows,
    );
    let selected = select_agent_artifact_group(
        lookup,
        AgentArtifactGroupSelectionRequestWire::SessionLatest { names },
    );
    assert!(selected.complete);
    assert_eq!(selected.generation.as_deref(), Some("200"));
    assert_eq!(
        selected
            .members
            .iter()
            .map(|hit| hit.timestamp.as_str())
            .collect::<Vec<_>>(),
        ["200", "201"]
    );
}

#[test]
fn explicit_member_keeps_its_older_generation() {
    let names = vec!["lane".to_string()];
    let rows = vec![
        hit("/p/old-root", "100", "lane--plan", None, None),
        hit("/p/old-child", "101", "lane--code", Some("100"), None),
        hit("/p/new-root", "200", "lane--plan", None, None),
    ];
    let selected = select_agent_artifact_group(
        lookup(
            AgentArtifactLookupTargetWire::Session {
                names: names.clone(),
            },
            rows,
        ),
        AgentArtifactGroupSelectionRequestWire::SessionMember {
            names,
            artifact_dir: "/p/old-child".to_string(),
        },
    );
    assert_eq!(selected.generation.as_deref(), Some("100"));
    assert_eq!(
        selected
            .member
            .as_ref()
            .map(|hit| hit.artifact_dir.as_str()),
        Some("/p/old-child")
    );
    assert_eq!(selected.members.len(), 2);
}

#[test]
fn clan_selects_latest_generation_and_sorts_members() {
    let names = vec!["clan".to_string()];
    let selected = select_agent_artifact_group(
        lookup(
            AgentArtifactLookupTargetWire::Clan {
                names: names.clone(),
            },
            vec![
                hit("/p/old", "100", "a", None, Some("099")),
                hit("/p/new-b", "102", "b", None, Some("101")),
                hit("/p/new-a", "101", "a", None, Some("101")),
            ],
        ),
        AgentArtifactGroupSelectionRequestWire::ClanLatest { names },
    );
    assert_eq!(selected.generation.as_deref(), Some("101"));
    assert_eq!(
        selected
            .members
            .iter()
            .map(|hit| hit.timestamp.as_str())
            .collect::<Vec<_>>(),
        ["101", "102"]
    );
}

#[test]
fn incomplete_group_lookup_never_returns_partial_members() {
    let names = vec!["lane".to_string()];
    let mut lookup = lookup(
        AgentArtifactLookupTargetWire::Session {
            names: names.clone(),
        },
        vec![hit("/p/root", "100", "lane", None, None)],
    );
    lookup.complete = false;
    let selected = select_agent_artifact_group(
        lookup,
        AgentArtifactGroupSelectionRequestWire::SessionLatest { names },
    );
    assert!(!selected.complete);
    assert!(selected.members.is_empty());
    assert!(selected.root.is_none());
}

#[test]
fn exhausted_bounded_recovery_selects_a_complete_group() {
    let names = vec!["lane".to_string()];
    let mut lookup = lookup(
        AgentArtifactLookupTargetWire::Session {
            names: names.clone(),
        },
        vec![
            hit("/p/root", "100", "lane--plan", None, None),
            hit("/p/child", "101", "lane--code", Some("100"), None),
        ],
    );
    lookup.availability = AgentArtifactLookupAvailabilityWire::Missing;
    lookup.complete = false;
    lookup.source = "fallback".to_string();
    lookup.dirs_visited = 2;
    lookup.complete = true;

    let selected = select_agent_artifact_group(
        lookup,
        AgentArtifactGroupSelectionRequestWire::SessionLatest { names },
    );

    assert!(selected.complete);
    assert_eq!(selected.members.len(), 2);
    assert_eq!(selected.generation.as_deref(), Some("100"));
}

#[test]
fn truncated_bounded_recovery_cannot_select_partial_members() {
    let names = vec!["lane".to_string()];
    let mut lookup = lookup(
        AgentArtifactLookupTargetWire::Session {
            names: names.clone(),
        },
        vec![hit("/p/root", "100", "lane--plan", None, None)],
    );
    lookup.availability = AgentArtifactLookupAvailabilityWire::Missing;
    lookup.complete = false;
    lookup.source = "fallback".to_string();
    lookup.dirs_visited = 200;
    lookup.truncated = true;

    let selected = select_agent_artifact_group(
        lookup,
        AgentArtifactGroupSelectionRequestWire::SessionLatest { names },
    );

    assert!(!selected.complete);
    assert!(selected.members.is_empty());
    assert!(selected.root.is_none());
}
