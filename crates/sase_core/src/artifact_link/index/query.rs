//! Indexed chip, target, row, and projection queries.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::artifact_link::row_resolution::{
    artifact_row_index_keys, artifact_row_ref_lookup_keys,
    ArtifactRowIdentityWire, ArtifactRowRefQueryWire,
    ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
};

use super::aliases::agent_lookup_candidates;
use super::prepared::PreparedArtifactLinkIndex;
use super::target::{parse_index_ref, target_for_ref_kind};
use super::wire::{
    ArtifactLinkIndexChipWire, ArtifactLinkIndexProjectRequestWire,
    ArtifactLinkIndexProjectResponseWire, ArtifactLinkIndexProjectedEdgeWire,
    ArtifactLinkIndexRowsRequestWire, ArtifactLinkIndexRowsResponseWire,
    ArtifactLinkIndexTargetWire,
};

pub fn artifact_link_index_chips(
    index: &PreparedArtifactLinkIndex,
    reference: &str,
) -> Vec<ArtifactLinkIndexChipWire> {
    index
        .chips
        .get(reference)
        .map(|chips| chips.as_ref().to_vec())
        .unwrap_or_default()
}

pub fn artifact_link_index_target(
    index: &PreparedArtifactLinkIndex,
    reference: &str,
) -> ArtifactLinkIndexTargetWire {
    match index.targets.get(reference) {
        Some(target) => ArtifactLinkIndexTargetWire {
            present: true,
            target: target.clone(),
        },
        None => ArtifactLinkIndexTargetWire {
            present: index.chips.contains_key(reference),
            target: None,
        },
    }
}

pub fn artifact_link_index_rows(
    index: &PreparedArtifactLinkIndex,
    request: ArtifactLinkIndexRowsRequestWire,
) -> ArtifactLinkIndexRowsResponseWire {
    let ordinals = candidate_ordinals(index, &request.lookup_keys);
    let rows = ordinals
        .iter()
        .filter_map(|&ordinal| {
            index.rows.get(ordinal as usize).map(|row| row.to_wire())
        })
        .collect();
    ArtifactLinkIndexRowsResponseWire {
        candidate_row_count: ordinals.len() as u64,
        rows,
    }
}

pub fn artifact_link_index_project(
    index: &PreparedArtifactLinkIndex,
    request: ArtifactLinkIndexProjectRequestWire,
) -> ArtifactLinkIndexProjectResponseWire {
    let permitted: HashSet<String> =
        request.permitted_relations.into_iter().collect();
    let known_index = KnownTargetIndex::build(&request.known_targets);
    let lookup_keys = known_index.all_lookup_keys();
    let ordinals = candidate_ordinals(index, &lookup_keys);
    let fallback_hint = request
        .project_hint
        .as_deref()
        .map(str::trim)
        .filter(|hint| !hint.is_empty());
    let mut seen = BTreeSet::new();
    let mut edges = Vec::new();
    for ordinal in &ordinals {
        let Some(row) = index.rows.get(*ordinal as usize) else {
            continue;
        };
        if row.source_ref.is_empty()
            || row.target_ref.is_empty()
            || row.relation.is_empty()
        {
            continue;
        }
        if !permitted.is_empty() && !permitted.contains(&row.relation) {
            continue;
        }
        let project = row
            .project
            .as_deref()
            .or(fallback_hint)
            .map(str::trim)
            .filter(|hint| !hint.is_empty());
        let Some(source) =
            resolve_endpoint(index, &row.source_ref, &known_index, project)
        else {
            continue;
        };
        let Some(target) =
            resolve_endpoint(index, &row.target_ref, &known_index, project)
        else {
            continue;
        };
        let source_known = known_index.contains(&source);
        let target_known = known_index.contains(&target);
        if !source_known && !target_known {
            continue;
        }
        let key = edge_key(&row.relation, &source, &target);
        if !seen.insert(key) {
            continue;
        }
        edges.push(ArtifactLinkIndexProjectedEdgeWire {
            relation: row.relation.clone(),
            source,
            target,
            description: row.description.clone(),
            origin: row.origin.clone(),
            uses: row.uses,
            source_known,
            target_known,
        });
    }
    ArtifactLinkIndexProjectResponseWire {
        candidate_row_count: ordinals.len() as u64,
        edges,
    }
}

fn candidate_ordinals(
    index: &PreparedArtifactLinkIndex,
    lookup_keys: &[Vec<String>],
) -> BTreeSet<u32> {
    let mut ordinals = BTreeSet::new();
    if lookup_keys.is_empty() {
        return ordinals;
    }
    for key in lookup_keys {
        if let Some(hits) = index.inverted.get(key) {
            ordinals.extend(hits.iter().copied());
        }
    }
    ordinals
}

struct KnownTargetIndex {
    targets: BTreeSet<ArtifactRowIdentityWire>,
    by_key: HashMap<Vec<String>, ArtifactRowIdentityWire>,
}

impl KnownTargetIndex {
    fn build(known: &[ArtifactRowIdentityWire]) -> Self {
        let mut ordered = known.to_vec();
        ordered.sort();
        let mut by_key = HashMap::new();
        for identity in &ordered {
            for key in artifact_row_index_keys(identity) {
                by_key.entry(key).or_insert_with(|| identity.clone());
            }
        }
        Self {
            targets: ordered.into_iter().collect(),
            by_key,
        }
    }

    fn contains(&self, identity: &ArtifactRowIdentityWire) -> bool {
        self.targets.contains(identity)
    }

    fn lookup(&self, key: &[String]) -> Option<&ArtifactRowIdentityWire> {
        self.by_key.get(key)
    }

    fn all_lookup_keys(&self) -> Vec<Vec<String>> {
        self.by_key.keys().cloned().collect()
    }
}

fn resolve_endpoint(
    index: &PreparedArtifactLinkIndex,
    reference: &str,
    known: &KnownTargetIndex,
    project_hint: Option<&str>,
) -> Option<ArtifactRowIdentityWire> {
    let parsed = parse_index_ref(reference)?;
    if let Some(exact) = known_target_for_ref(
        index,
        &parsed.kind,
        &parsed.payload,
        known,
        project_hint,
    ) {
        return Some(exact);
    }
    target_for_ref_kind(&parsed.kind, &parsed.payload, project_hint)
}

fn known_target_for_ref(
    index: &PreparedArtifactLinkIndex,
    kind: &str,
    payload: &str,
    known: &KnownTargetIndex,
    project_hint: Option<&str>,
) -> Option<ArtifactRowIdentityWire> {
    let agent_name_candidates = if kind == "agent" {
        agent_lookup_candidates(payload, &index.identity)
    } else {
        Vec::new()
    };
    let query = ArtifactRowRefQueryWire {
        schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
        kind: kind.to_string(),
        payload: payload.to_string(),
        project_hint: project_hint.map(str::to_string),
        agent_name_candidates,
    };
    for key in artifact_row_ref_lookup_keys(&query) {
        if let Some(identity) = known.lookup(&key) {
            return Some(identity.clone());
        }
    }
    None
}

fn edge_key(
    relation: &str,
    source: &ArtifactRowIdentityWire,
    target: &ArtifactRowIdentityWire,
) -> (String, ArtifactRowIdentityWire, ArtifactRowIdentityWire) {
    if relation == "related" {
        let (left, right) = if identity_token(source) <= identity_token(target)
        {
            (source.clone(), target.clone())
        } else {
            (target.clone(), source.clone())
        };
        (relation.to_string(), left, right)
    } else {
        (relation.to_string(), source.clone(), target.clone())
    }
}

fn identity_token(identity: &ArtifactRowIdentityWire) -> String {
    let mut parts = Vec::with_capacity(identity.parts.len() + 2);
    parts.push("v1");
    parts.push(identity.pane_id.as_str());
    for part in &identity.parts {
        parts.push(part.as_str());
    }
    parts.join("\u{1f}")
}
