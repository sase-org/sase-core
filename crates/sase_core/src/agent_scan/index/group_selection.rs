//! Select one session or clan generation from targeted identity hits.

use super::lookup::{
    AgentArtifactLookupAvailabilityWire, AgentArtifactLookupHitWire,
    AgentArtifactLookupResultWire, AgentArtifactLookupTargetWire,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Requested group resolution over one already-targeted lookup result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentArtifactGroupSelectionRequestWire {
    SessionLatest {
        names: Vec<String>,
    },
    SessionRoot {
        names: Vec<String>,
        artifact_dir: String,
    },
    SessionMember {
        names: Vec<String>,
        artifact_dir: String,
    },
    ClanLatest {
        names: Vec<String>,
    },
}

/// Selected generation. Incomplete lookups never contain selected members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactGroupSelectionResultWire {
    pub availability: AgentArtifactLookupAvailabilityWire,
    pub complete: bool,
    pub source: String,
    #[serde(default)]
    pub diagnostic: Option<String>,
    #[serde(default)]
    pub selected_name: Option<String>,
    #[serde(default)]
    pub generation: Option<String>,
    #[serde(default)]
    pub root: Option<AgentArtifactLookupHitWire>,
    #[serde(default)]
    pub member: Option<AgentArtifactLookupHitWire>,
    pub members: Vec<AgentArtifactLookupHitWire>,
}

/// Select a complete session or clan generation using only identity hits.
pub fn select_agent_artifact_group(
    lookup: AgentArtifactLookupResultWire,
    request: AgentArtifactGroupSelectionRequestWire,
) -> AgentArtifactGroupSelectionResultWire {
    let mut result = AgentArtifactGroupSelectionResultWire {
        availability: lookup.availability,
        complete: lookup.complete && !lookup.truncated,
        source: lookup.source,
        diagnostic: lookup.diagnostic,
        selected_name: None,
        generation: None,
        root: None,
        member: None,
        members: Vec::new(),
    };
    if !result.complete {
        return result;
    }

    let (names, expected_target) = request.names_and_target();
    if !query_matches(&lookup.query.target, names, expected_target) {
        result.complete = false;
        result.diagnostic = Some(
            "group selection does not match the targeted lookup query"
                .to_string(),
        );
        return result;
    }

    let relation = match expected_target {
        ExpectedTarget::Session => AgentRelation::Session,
        ExpectedTarget::Clan => AgentRelation::Clan,
    };
    let candidates = lookup
        .hits
        .into_iter()
        .filter(|hit| hit.workflow_dir_name == "ace-run")
        .filter(|hit| {
            !matches!(relation, AgentRelation::Clan)
                || hit.agent_name.as_ref().is_some_and(|name| !name.is_empty())
        })
        .filter(|hit| {
            relation_value(hit, relation)
                .is_some_and(|value| names.iter().any(|name| name == value))
        })
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return result;
    }
    // All supplied spellings are equivalent for the current owner. The legacy
    // API returns the requested spelling even when only a qualified row exists.
    result.selected_name = names.first().cloned();

    match request {
        AgentArtifactGroupSelectionRequestWire::SessionLatest { .. } => {
            select_latest_session(candidates, &mut result);
        }
        AgentArtifactGroupSelectionRequestWire::SessionRoot {
            artifact_dir,
            ..
        } => {
            let Some(root) = candidates.iter().find(|hit| {
                super::lookup::canonical_path(&hit.artifact_dir)
                    == super::lookup::canonical_path(&artifact_dir)
                    && hit.parent_timestamp.is_none()
            }) else {
                return result;
            };
            result.generation = Some(root.timestamp.clone());
            result.root = Some(root.clone());
            result.members = session_descendants(root, &candidates);
        }
        AgentArtifactGroupSelectionRequestWire::SessionMember {
            artifact_dir,
            ..
        } => {
            let Some(member) = candidates.iter().find(|hit| {
                super::lookup::canonical_path(&hit.artifact_dir)
                    == super::lookup::canonical_path(&artifact_dir)
            }) else {
                return result;
            };
            result.member = Some(member.clone());
            let root = session_root_for_member(member, &candidates);
            result.generation = Some(root.timestamp.clone());
            if root.parent_timestamp.is_none() {
                result.root = Some(root.clone());
            }
            result.members = session_descendants(&root, &candidates);
        }
        AgentArtifactGroupSelectionRequestWire::ClanLatest { .. } => {
            select_latest_clan(candidates, &mut result);
        }
    }
    result
}

#[derive(Clone, Copy)]
enum ExpectedTarget {
    Session,
    Clan,
}

#[derive(Clone, Copy)]
enum AgentRelation {
    Session,
    Clan,
}

impl AgentArtifactGroupSelectionRequestWire {
    fn names_and_target(&self) -> (&[String], ExpectedTarget) {
        match self {
            Self::SessionLatest { names }
            | Self::SessionRoot { names, .. }
            | Self::SessionMember { names, .. } => {
                (names, ExpectedTarget::Session)
            }
            Self::ClanLatest { names } => (names, ExpectedTarget::Clan),
        }
    }
}

fn query_matches(
    target: &AgentArtifactLookupTargetWire,
    names: &[String],
    expected: ExpectedTarget,
) -> bool {
    match (target, expected) {
        (
            AgentArtifactLookupTargetWire::Session { names: queried },
            ExpectedTarget::Session,
        )
        | (
            AgentArtifactLookupTargetWire::Clan { names: queried },
            ExpectedTarget::Clan,
        ) => queried == names,
        _ => false,
    }
}

fn relation_value(
    hit: &AgentArtifactLookupHitWire,
    relation: AgentRelation,
) -> Option<&str> {
    match relation {
        AgentRelation::Session => hit.lookup_session.as_deref(),
        AgentRelation::Clan => hit.agent_clan.as_deref(),
    }
}

fn select_latest_session(
    candidates: Vec<AgentArtifactLookupHitWire>,
    result: &mut AgentArtifactGroupSelectionResultWire,
) {
    let mut roots = candidates
        .iter()
        .filter(|hit| hit.parent_timestamp.is_none())
        .collect::<Vec<_>>();
    roots.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| right.artifact_dir.cmp(&left.artifact_dir))
    });
    if let Some(root) = roots.first() {
        result.generation = Some(root.timestamp.clone());
        result.root = Some((*root).clone());
        result.members = session_descendants(root, &candidates);
        return;
    }

    result.members = sorted_members(candidates);
}

fn session_root_for_member(
    member: &AgentArtifactLookupHitWire,
    candidates: &[AgentArtifactLookupHitWire],
) -> AgentArtifactLookupHitWire {
    let mut by_timestamp = BTreeMap::new();
    for candidate in candidates {
        by_timestamp.insert(candidate.timestamp.clone(), candidate.clone());
    }

    let mut root = member.clone();
    let mut seen = BTreeSet::from([root.timestamp.clone()]);
    while let Some(parent) = root.parent_timestamp.as_deref() {
        if seen.contains(parent) {
            break;
        }
        let Some(next) = by_timestamp.get(parent) else {
            break;
        };
        root = next.clone();
        seen.insert(root.timestamp.clone());
    }
    root
}

fn session_descendants(
    root: &AgentArtifactLookupHitWire,
    candidates: &[AgentArtifactLookupHitWire],
) -> Vec<AgentArtifactLookupHitWire> {
    let mut selected = vec![root.clone()];
    let mut timestamps = BTreeSet::from([root.timestamp.clone()]);
    let mut remaining = candidates
        .iter()
        .filter(|hit| hit.artifact_dir != root.artifact_dir)
        .cloned()
        .collect::<Vec<_>>();
    loop {
        let attached = remaining
            .iter()
            .filter(|hit| {
                hit.parent_timestamp
                    .as_ref()
                    .is_some_and(|parent| timestamps.contains(parent))
            })
            .cloned()
            .collect::<Vec<_>>();
        if attached.is_empty() {
            break;
        }
        let attached_dirs = attached
            .iter()
            .map(|hit| hit.artifact_dir.clone())
            .collect::<BTreeSet<_>>();
        for hit in attached {
            timestamps.insert(hit.timestamp.clone());
            selected.push(hit);
        }
        remaining.retain(|hit| !attached_dirs.contains(&hit.artifact_dir));
    }
    sorted_members(selected)
}

fn select_latest_clan(
    candidates: Vec<AgentArtifactLookupHitWire>,
    result: &mut AgentArtifactGroupSelectionResultWire,
) {
    let Some(generation) = candidates
        .iter()
        .map(effective_clan_generation)
        .max()
        .map(str::to_string)
    else {
        return;
    };
    result.generation = Some(generation.clone());
    result.members = sorted_members(
        candidates
            .into_iter()
            .filter(|hit| effective_clan_generation(hit) == generation)
            .collect(),
    );
}

fn effective_clan_generation(hit: &AgentArtifactLookupHitWire) -> &str {
    hit.agent_clan_generation
        .as_deref()
        .filter(|value| !value.is_empty())
        .or(hit.parent_timestamp.as_deref())
        .unwrap_or(&hit.timestamp)
}

fn sorted_members(
    mut members: Vec<AgentArtifactLookupHitWire>,
) -> Vec<AgentArtifactLookupHitWire> {
    members.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| right.artifact_dir.cmp(&left.artifact_dir))
    });
    members
}
