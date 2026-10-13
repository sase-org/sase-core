//! Compile a permissive aggregate snapshot into an immutable read index.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::artifact_link::relation::builtin_artifact_relations;
use crate::artifact_link::row_resolution::{
    artifact_row_ref_lookup_keys, ArtifactRowIdentityWire,
    ArtifactRowRefQueryWire, ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
};
use crate::artifact_link::wire::ArtifactLinkError;

use super::aliases::{agent_lookup_candidates, aliases_for_ref, IndexIdentity};
use super::prepared::{IndexedRow, PreparedArtifactLinkIndex};
use super::target::{
    canonicalize_index_ref, neighbor_kind_of, parse_index_ref,
    public_neighbor_ref, target_for_ref_kind,
};
use super::wire::{
    is_semantic_relation, ArtifactLinkIndexBuildRequestWire,
    ArtifactLinkIndexChipWire, ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
};

#[derive(Clone, PartialEq, Eq, Hash)]
enum ChipIdentity {
    Directed {
        source: String,
        relation: String,
        target: String,
    },
    Undirected {
        relation: String,
        left: String,
        right: String,
    },
}

struct RelationMeta {
    inverse: String,
    directed: bool,
}

pub fn compile_artifact_link_index(
    request: ArtifactLinkIndexBuildRequestWire,
) -> Result<PreparedArtifactLinkIndex, ArtifactLinkError> {
    if request.schema_version != ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "unsupported artifact link index schema_version {}; expected {}",
            request.schema_version, ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION
        )));
    }
    let identity = IndexIdentity::from_wire(&request.identity);
    let relations = relation_table(&request.relations);
    let mut rows = Vec::with_capacity(request.rows.len());
    for value in request.rows {
        rows.push(parse_input_row(value));
    }

    let inverted = build_inverted_index(&rows, &identity);
    let indexed_row_count = rows
        .iter()
        .filter(|row| {
            !row.source_ref.is_empty()
                && !row.target_ref.is_empty()
                && !row.relation.is_empty()
        })
        .count();
    let chip_index = build_chip_index(&rows, &identity, &relations);

    Ok(PreparedArtifactLinkIndex {
        rows,
        inverted,
        chips: chip_index.chips,
        targets: chip_index.targets,
        canonical_ref_count: chip_index.canonical_ref_count,
        indexed_row_count,
        identity,
    })
}

fn relation_table(
    extra: &[super::wire::ArtifactLinkIndexRelationMetaWire],
) -> HashMap<String, RelationMeta> {
    let mut table = HashMap::new();
    for relation in builtin_artifact_relations() {
        table.insert(
            relation.slug.clone(),
            RelationMeta {
                inverse: relation.inverse,
                directed: relation.directed,
            },
        );
    }
    for relation in extra {
        let slug = relation.slug.trim();
        if slug.is_empty() {
            continue;
        }
        table.insert(
            slug.to_string(),
            RelationMeta {
                inverse: if relation.inverse.trim().is_empty() {
                    slug.to_string()
                } else {
                    relation.inverse.trim().to_string()
                },
                directed: relation.directed,
            },
        );
    }
    table
}

fn parse_input_row(value: Value) -> IndexedRow {
    let fields = match value {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    let source_ref = string_field(&fields, "source_ref");
    let target_ref = string_field(&fields, "target_ref");
    let relation = string_field(&fields, "relation");
    let description = string_field(&fields, "description");
    let origin = string_field(&fields, "origin");
    let created_by = string_field(&fields, "created_by");
    let created_at = string_field(&fields, "created_at");
    let uses = parse_uses(fields.get("uses"));
    let project = optional_string_field(&fields, "_project")
        .or_else(|| optional_string_field(&fields, "project"));
    let canon_source = if source_ref.is_empty() {
        String::new()
    } else {
        canonicalize_index_ref(&source_ref)
    };
    let canon_target = if target_ref.is_empty() {
        String::new()
    } else {
        canonicalize_index_ref(&target_ref)
    };
    IndexedRow {
        source_ref,
        target_ref,
        canon_source,
        canon_target,
        relation,
        description,
        origin,
        created_by,
        created_at,
        uses,
        project,
    }
}

fn string_field(fields: &Map<String, Value>, key: &str) -> String {
    match fields.get(key) {
        Some(Value::String(value)) => value.trim().to_string(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn optional_string_field(
    fields: &Map<String, Value>,
    key: &str,
) -> Option<String> {
    let value = string_field(fields, key);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn parse_uses(value: Option<&Value>) -> u64 {
    let Some(value) = value else {
        return 0;
    };
    let parsed = match value {
        Value::Null => return 0,
        Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                int
            } else if let Some(uint) = number.as_u64() {
                return uint;
            } else {
                return 0;
            }
        }
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return 0;
            }
            match trimmed.parse::<i64>() {
                Ok(int) => int,
                Err(_) => return 0,
            }
        }
        _ => return 0,
    };
    parsed.max(0) as u64
}

fn build_inverted_index(
    rows: &[IndexedRow],
    identity: &IndexIdentity,
) -> HashMap<Vec<String>, Vec<u32>> {
    let mut inverted: HashMap<Vec<String>, Vec<u32>> = HashMap::new();
    for (ordinal, row) in rows.iter().enumerate() {
        let ordinal = ordinal as u32;
        index_endpoint(
            &mut inverted,
            &row.source_ref,
            row.project.as_deref(),
            identity,
            ordinal,
        );
        index_endpoint(
            &mut inverted,
            &row.target_ref,
            row.project.as_deref(),
            identity,
            ordinal,
        );
    }
    inverted
}

fn index_endpoint(
    inverted: &mut HashMap<Vec<String>, Vec<u32>>,
    reference: &str,
    project_hint: Option<&str>,
    identity: &IndexIdentity,
    ordinal: u32,
) {
    if reference.is_empty() {
        return;
    }
    let Some(parsed) = parse_index_ref(reference) else {
        return;
    };
    let agent_name_candidates = if parsed.kind == "agent" {
        agent_lookup_candidates(&parsed.payload, identity)
    } else {
        Vec::new()
    };
    let query = ArtifactRowRefQueryWire {
        schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
        kind: parsed.kind,
        payload: parsed.payload,
        project_hint: project_hint.map(str::to_string),
        agent_name_candidates,
    };
    for key in artifact_row_ref_lookup_keys(&query) {
        let slots = inverted.entry(key).or_default();
        if slots.last().copied() != Some(ordinal) {
            slots.push(ordinal);
        }
    }
}

struct ChipIndex {
    chips: HashMap<String, Arc<[ArtifactLinkIndexChipWire]>>,
    targets: HashMap<String, Option<ArtifactRowIdentityWire>>,
    canonical_ref_count: usize,
}

fn build_chip_index(
    rows: &[IndexedRow],
    identity: &IndexIdentity,
    relations: &HashMap<String, RelationMeta>,
) -> ChipIndex {
    let mut best: HashMap<ChipIdentity, &IndexedRow> = HashMap::new();
    let mut order: Vec<ChipIdentity> = Vec::new();
    for row in rows {
        if row.source_ref.is_empty()
            || row.target_ref.is_empty()
            || row.relation.is_empty()
        {
            continue;
        }
        let directed = is_directed(&row.relation, relations);
        let identity_key = if directed {
            ChipIdentity::Directed {
                source: row.canon_source.clone(),
                relation: row.relation.clone(),
                target: row.canon_target.clone(),
            }
        } else {
            let (left, right) = if row.canon_source <= row.canon_target {
                (row.canon_source.clone(), row.canon_target.clone())
            } else {
                (row.canon_target.clone(), row.canon_source.clone())
            };
            ChipIdentity::Undirected {
                relation: row.relation.clone(),
                left,
                right,
            }
        };
        match best.get(&identity_key) {
            None => {
                order.push(identity_key.clone());
                best.insert(identity_key, row);
            }
            Some(existing) if row.uses > existing.uses => {
                best.insert(identity_key, row);
            }
            Some(_) => {}
        }
    }

    let mut grouped: HashMap<String, Vec<ArtifactLinkIndexChipWire>> =
        HashMap::new();
    let mut targets: HashMap<String, Option<ArtifactRowIdentityWire>> =
        HashMap::new();
    for key in &order {
        let Some(row) = best.get(key) else {
            continue;
        };
        let directed = matches!(key, ChipIdentity::Directed { .. });
        let project_hint = row.project.as_deref();
        record_indexed_target(&mut targets, &row.canon_source, project_hint);
        record_indexed_target(&mut targets, &row.canon_target, project_hint);
        grouped
            .entry(row.canon_source.clone())
            .or_default()
            .push(build_chip(
                row,
                relations,
                directed,
                true,
                &row.canon_target,
                project_hint,
            ));
        grouped
            .entry(row.canon_target.clone())
            .or_default()
            .push(build_chip(
                row,
                relations,
                directed,
                false,
                &row.canon_source,
                project_hint,
            ));
    }

    let mut chips: HashMap<String, Arc<[ArtifactLinkIndexChipWire]>> =
        HashMap::with_capacity(grouped.len());
    for (reference, mut entries) in grouped {
        entries.sort_by(|left, right| {
            (
                !is_semantic_relation(&left.relation),
                left.label.as_str(),
                left.neighbor_ref.as_str(),
            )
                .cmp(&(
                    !is_semantic_relation(&right.relation),
                    right.label.as_str(),
                    right.neighbor_ref.as_str(),
                ))
        });
        chips.insert(reference, Arc::from(entries));
    }
    let canonical_ref_count = chips.len();
    let canonical_refs: Vec<String> = chips.keys().cloned().collect();
    for reference in canonical_refs {
        let Some(entries) = chips.get(&reference).cloned() else {
            continue;
        };
        let target = targets.get(&reference).cloned().flatten();
        for alias in aliases_for_ref(&reference, identity) {
            chips
                .entry(alias.clone())
                .or_insert_with(|| Arc::clone(&entries));
            targets.entry(alias).or_insert_with(|| target.clone());
        }
    }
    ChipIndex {
        chips,
        targets,
        canonical_ref_count,
    }
}

fn is_directed(
    relation: &str,
    relations: &HashMap<String, RelationMeta>,
) -> bool {
    relations
        .get(relation)
        .map(|meta| meta.directed)
        .unwrap_or(relation != "related")
}

fn relation_label(
    relation: &str,
    this_is_source: bool,
    relations: &HashMap<String, RelationMeta>,
) -> String {
    match relations.get(relation) {
        Some(_) if this_is_source => relation.to_string(),
        Some(meta) => meta.inverse.clone(),
        None => relation.to_string(),
    }
}

fn record_indexed_target(
    targets: &mut HashMap<String, Option<ArtifactRowIdentityWire>>,
    reference: &str,
    project_hint: Option<&str>,
) {
    if targets.contains_key(reference) {
        return;
    }
    let Some(parsed) = parse_index_ref(reference) else {
        targets.insert(reference.to_string(), None);
        return;
    };
    targets.insert(
        reference.to_string(),
        target_for_ref_kind(&parsed.kind, &parsed.payload, project_hint),
    );
}

fn build_chip(
    row: &IndexedRow,
    relations: &HashMap<String, RelationMeta>,
    directed: bool,
    this_is_source: bool,
    neighbor_ref: &str,
    project_hint: Option<&str>,
) -> ArtifactLinkIndexChipWire {
    let parsed = parse_index_ref(neighbor_ref);
    let neighbor_kind = parsed
        .as_ref()
        .map(|parts| parts.kind.clone())
        .unwrap_or_else(|| neighbor_kind_of(neighbor_ref));
    let neighbor_target = parsed.as_ref().and_then(|parts| {
        target_for_ref_kind(&parts.kind, &parts.payload, project_hint)
    });
    ArtifactLinkIndexChipWire {
        relation: row.relation.clone(),
        label: relation_label(&row.relation, this_is_source, relations),
        directed,
        this_is_source,
        neighbor_ref: public_neighbor_ref(neighbor_ref),
        neighbor_kind,
        neighbor_target,
        why: row.description.clone(),
        origin: row.origin.clone(),
        uses: row.uses,
        created_by: row.created_by.clone(),
        created_at: row.created_at.clone(),
        writable: row.origin != "projected",
    }
}
