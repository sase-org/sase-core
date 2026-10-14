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
        None => String::new(),
        Some(value) => legacy_display_string(value).trim().to_string(),
    }
}

/// Recover the legacy Python aggregate-reader display semantics
/// (`str(row.get(key) or "").strip()`).
///
/// Falsy inputs (null, false, zero numbers, empty string/list/map) yield
/// empty; `True` yields `True`; non-empty lists/maps yield Python `str`
/// (single-quoted, `True`/`False`/`None`) rather than JSON.
fn legacy_display_string(value: &Value) -> String {
    if is_python_falsy(value) {
        return String::new();
    }
    match value {
        Value::Null => String::new(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => String::new(),
        Value::Number(number) => python_number_string(number),
        Value::String(text) => text.clone(),
        Value::Array(_) | Value::Object(_) => python_repr(value),
    }
}

fn is_python_falsy(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(flag) => !flag,
        Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                int == 0
            } else if let Some(uint) = number.as_u64() {
                uint == 0
            } else if let Some(float) = number.as_f64() {
                float == 0.0
            } else {
                false
            }
        }
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
    }
}

/// Python `repr` for JSON values, matching `str(list/dict)` output:
/// single-quoted strings, `True`/`False`/`None`, `", "` separators.
fn python_repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Number(number) => python_number_string(number),
        Value::String(text) => python_string_repr(text),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(python_repr).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(key, val)| {
                    format!("{}: {}", python_string_repr(key), python_repr(val))
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

fn python_number_string(number: &serde_json::Number) -> String {
    let value = number.to_string();
    if !number.is_f64() {
        return value;
    }
    let Some((mantissa, exponent)) = value.split_once(['e', 'E']) else {
        return value;
    };
    let Ok(exponent) = exponent.parse::<i32>() else {
        return value;
    };
    let sign = if exponent >= 0 { '+' } else { '-' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
}

fn python_string_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' if quote == '\'' => out.push_str("\\'"),
            '"' if quote == '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\x08"),
            '\u{000c}' => out.push_str("\\x0c"),
            ch if ch.is_control() => {
                let code = ch as u32;
                if code <= 0xff {
                    out.push_str(&format!("\\x{code:02x}"));
                } else if code <= 0xffff {
                    out.push_str(&format!("\\u{code:04x}"));
                } else {
                    out.push_str(&format!("\\U{code:08x}"));
                }
            }
            _ => out.push(ch),
        }
    }
    out.push(quote);
    out
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
    // Legacy `int(row.get("uses") or 0)`: falsy inputs coerce to 0, truthy
    // inputs go through `int()` with truncation and tolerated numeric
    // strings (`1_000` -> 1000), then `max(0, ...)`.
    if is_python_falsy(value) {
        return 0;
    }
    match value {
        Value::Null => 0,
        Value::Bool(true) => 1,
        Value::Bool(false) => 0,
        Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                int.max(0) as u64
            } else if let Some(uint) = number.as_u64() {
                uint
            } else if let Some(float) = number.as_f64() {
                if !float.is_finite() {
                    0
                } else {
                    let truncated = float.trunc();
                    if truncated <= 0.0 {
                        0
                    } else if truncated >= u64::MAX as f64 {
                        u64::MAX
                    } else {
                        truncated as u64
                    }
                }
            } else {
                0
            }
        }
        Value::String(text) => parse_python_int_string(text),
        Value::Array(_) | Value::Object(_) => 0,
    }
}

/// Parse a decimal integer string with Python `int(s)` semantics: surrounding
/// whitespace, an optional sign, Unicode decimal digits, and single
/// underscores strictly between digits (`1_000` -> 1000, `1__000` invalid).
/// Negative results clamp to 0; values beyond the wire's u64 range saturate.
fn parse_python_int_string(text: &str) -> u64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    let (negative, digits) = match trimmed.strip_prefix(['+', '-']) {
        Some(rest) => (trimmed.starts_with('-'), rest),
        None => (false, trimmed),
    };
    if digits.is_empty() {
        return 0;
    }
    let mut value = 0u64;
    let mut previous_was_digit = false;
    let mut has_digit = false;
    for ch in digits.chars() {
        if ch == '_' {
            if !previous_was_digit {
                return 0;
            }
            previous_was_digit = false;
            continue;
        }
        let Some(digit) = decimal_digit_value(ch) else {
            return 0;
        };
        has_digit = true;
        previous_was_digit = true;
        value = value.saturating_mul(10).saturating_add(u64::from(digit));
    }
    if !has_digit || !previous_was_digit || negative && value > 0 {
        0
    } else {
        value
    }
}

fn decimal_digit_value(ch: char) -> Option<u8> {
    let code = ch as u32;
    // Unicode decimal-number blocks (Nd), matching Python's int(str) digit
    // acceptance while keeping this parser independent of Python and locale.
    const ZEROES: &[u32] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66,
        0x0be6, 0x0c66, 0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040,
        0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0,
        0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0,
        0xff10, 0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0, 0x112f0,
        0x11450, 0x114d0, 0x11650, 0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50,
        0x11d50, 0x11da0, 0x11f50, 0x16a60, 0x16ac0, 0x16b50, 0x1d7ce, 0x1d7d8,
        0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0, 0x1e5f1, 0x1e950,
        0x1fbf0,
    ];
    ZEROES.iter().find_map(|&zero| {
        if (zero..zero + 10).contains(&code) {
            Some((code - zero) as u8)
        } else {
            None
        }
    })
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
    // Canonical first-occurrence order in winning-row order. The alias pass
    // below must follow this order so colliding alias spellings resolve to
    // the earliest row's chips deterministically, matching the historical
    // Python index; HashMap iteration order is randomized per process.
    let mut canon_order: Vec<String> = Vec::new();
    for key in &order {
        let Some(row) = best.get(key) else {
            continue;
        };
        let directed = matches!(key, ChipIdentity::Directed { .. });
        let project_hint = row.project.as_deref();
        record_indexed_target(&mut targets, &row.canon_source, project_hint);
        record_indexed_target(&mut targets, &row.canon_target, project_hint);
        for endpoint in [&row.canon_source, &row.canon_target] {
            if !grouped.contains_key(endpoint) {
                canon_order.push(endpoint.clone());
            }
        }
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
    for reference in &canon_order {
        let Some(entries) = chips.get(reference).cloned() else {
            continue;
        };
        let target = targets.get(reference).cloned().flatten();
        for alias in aliases_for_ref(reference, identity) {
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
