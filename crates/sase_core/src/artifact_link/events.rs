//! Immutable artifact-link event wires, canonical bytes, and deterministic reduction.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use super::relation::lookup_artifact_relation;
use super::wire::{
    artifact_link_dedup_key, canonicalize_artifact_link_ref,
    validate_artifact_link_description, validate_artifact_link_row,
    ArtifactLinkDedupKeyWire, ArtifactLinkError, ArtifactLinkOriginWire,
    ArtifactLinkRowWire, ARTIFACT_LINK_ROW_SCHEMA_VERSION,
};

/// Schema version for immutable artifact-link events.
pub const ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION: u64 = 1;

/// Schema version for deterministic event reduction output.
pub const ARTIFACT_LINK_EVENT_REDUCTION_WIRE_SCHEMA_VERSION: u64 = 1;

const LINK_EVENT_PATH_PREFIX: &str = "link-events/v1";

/// One immutable artifact-link operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkEventWire {
    pub schema_version: u64,
    pub project_key: String,
    pub operation_id: String,
    pub created_by: String,
    pub origin: ArtifactLinkOriginWire,
    pub created_at: String,
    pub kind: ArtifactLinkEventKindWire,
}

/// A relation-aware edge identity stored in artifact-link events.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ArtifactLinkEventEdgeWire {
    Directed {
        source_ref: String,
        relation: String,
        target_ref: String,
    },
    Undirected {
        relation: String,
        left_ref: String,
        right_ref: String,
    },
}

/// Operation-specific payload for an immutable artifact-link event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "kebab-case")]
pub enum ArtifactLinkEventKindWire {
    Observation {
        edge: ArtifactLinkEventEdgeWire,
        description: String,
        occurrences: u64,
    },
    EdgePut {
        edge: ArtifactLinkEventEdgeWire,
        description: String,
        #[serde(default)]
        observed_operation_ids: Vec<String>,
    },
    EdgeRemove {
        edge: ArtifactLinkEventEdgeWire,
        observed_operation_ids: Vec<String>,
    },
    Alias {
        old_ref: String,
        new_ref: String,
    },
    BaselineImport {
        import_id: String,
        source_head: String,
        rows: Vec<ArtifactLinkRowWire>,
    },
}

/// A canonical alias supplied by the caller or extracted from alias events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkAliasWire {
    pub old_ref: String,
    pub new_ref: String,
}

/// Result of validating canonical event bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkEventCanonicalWire {
    pub schema_version: u64,
    pub event: ArtifactLinkEventWire,
    pub canonical_json: String,
    pub digest: String,
    pub path: String,
}

/// Alias resolution probe output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkAliasResolutionWire {
    pub schema_version: u64,
    pub aliases: Vec<ArtifactLinkAliasWire>,
    pub resolved_refs: BTreeMap<String, String>,
}

/// Deterministic reduction of artifact-link events into legacy rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkEventReductionWire {
    pub schema_version: u64,
    pub rows: Vec<ArtifactLinkRowWire>,
    pub edges: Vec<ArtifactLinkReducedEdgeWire>,
    pub aliases: Vec<ArtifactLinkAliasWire>,
}

/// Reduction metadata for one canonical edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkReducedEdgeWire {
    pub edge: ArtifactLinkEventEdgeWire,
    pub row: Option<ArtifactLinkRowWire>,
    pub versions: Vec<ArtifactLinkReducedVersionWire>,
    pub tombstones: Vec<ArtifactLinkReducedTombstoneWire>,
}

/// One retained assertion/observation/baseline version in reduction metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkReducedVersionWire {
    pub operation_id: String,
    pub kind: String,
    pub description: String,
    pub origin: ArtifactLinkOriginWire,
    pub created_by: String,
    pub created_at: String,
    pub uses: u64,
    pub active: bool,
    pub removed_by: Vec<String>,
    pub superseded_by: Vec<String>,
}

/// One retained remove event in reduction metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkReducedTombstoneWire {
    pub operation_id: String,
    pub observed_operation_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct VersionAccum {
    operation_id: String,
    kind: String,
    description: String,
    origin: ArtifactLinkOriginWire,
    created_by: String,
    created_at: String,
    uses: u64,
    supersedes: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct TombstoneAccum {
    operation_id: String,
    observed_operation_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Default)]
struct EdgeAccum {
    versions: BTreeMap<String, VersionAccum>,
    tombstones: BTreeMap<String, TombstoneAccum>,
}

#[derive(Debug, Clone)]
struct AliasResolver {
    direct: BTreeMap<String, String>,
    terminal: BTreeMap<String, String>,
}

/// Validate and canonicalize one immutable artifact-link event.
pub fn canonicalize_artifact_link_event(
    event: &ArtifactLinkEventWire,
) -> Result<ArtifactLinkEventWire, ArtifactLinkError> {
    if event.schema_version != ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION {
        return Err(validation(format!(
            "unsupported artifact link event schema_version {}; expected {}",
            event.schema_version, ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION
        )));
    }
    let project_key = validate_project_key(&event.project_key)?;
    let operation_id =
        validate_operation_id("operation_id", &event.operation_id)?;
    let created_by = validate_single_line("created_by", &event.created_by)?;
    let created_at = validate_single_line("created_at", &event.created_at)?;
    let kind = canonicalize_event_kind(&event.kind, &operation_id)?;

    Ok(ArtifactLinkEventWire {
        schema_version: ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION,
        project_key,
        operation_id,
        created_by,
        origin: event.origin,
        created_at,
        kind,
    })
}

/// Validate JSON before deserializing so float fields cannot be silently accepted.
pub fn canonicalize_artifact_link_event_json_value(
    value: &JsonValue,
) -> Result<ArtifactLinkEventWire, ArtifactLinkError> {
    ensure_integer_only_json(value, "$")?;
    let event: ArtifactLinkEventWire = serde_json::from_value(value.clone())
        .map_err(|error| {
            validation(format!(
                "event is not a valid ArtifactLinkEventWire: {error}"
            ))
        })?;
    canonicalize_artifact_link_event(&event)
}

/// Serialize one canonical event as sorted compact JSON with one trailing newline.
pub fn artifact_link_event_canonical_json(
    event: &ArtifactLinkEventWire,
) -> Result<String, ArtifactLinkError> {
    let event = canonicalize_artifact_link_event(event)?;
    canonical_json_for_serializable(&event)
}

/// Return the lowercase SHA-256 of [`artifact_link_event_canonical_json`].
pub fn artifact_link_event_digest(
    event: &ArtifactLinkEventWire,
) -> Result<String, ArtifactLinkError> {
    let canonical = artifact_link_event_canonical_json(event)?;
    Ok(sha256_hex(canonical.as_bytes()))
}

/// Return `link-events/v1/<first-two>/<digest>.json` for a validated digest.
pub fn artifact_link_event_path_for_digest(
    digest: &str,
) -> Result<String, ArtifactLinkError> {
    let digest = validate_sha256_digest(digest)?;
    Ok(format!(
        "{}/{}/{}.json",
        LINK_EVENT_PATH_PREFIX,
        &digest[..2],
        digest
    ))
}

/// Validate and return the canonical immutable event path for a digest.
pub fn artifact_link_event_validate_path(
    path: &str,
    digest: &str,
) -> Result<String, ArtifactLinkError> {
    let expected = artifact_link_event_path_for_digest(digest)?;
    if path.trim() != expected {
        return Err(validation(format!(
            "artifact link event path must be {expected:?} for digest {digest}"
        )));
    }
    Ok(expected)
}

/// Validate exact canonical bytes and, when supplied, their immutable path.
pub fn artifact_link_event_validate_bytes(
    bytes: &[u8],
    expected_path: Option<&str>,
) -> Result<ArtifactLinkEventCanonicalWire, ArtifactLinkError> {
    let value: JsonValue = serde_json::from_slice(bytes).map_err(|error| {
        validation(format!("event bytes are not valid JSON: {error}"))
    })?;
    ensure_integer_only_json(&value, "$")?;
    let event = canonicalize_artifact_link_event_json_value(&value)?;
    let canonical_json = artifact_link_event_canonical_json(&event)?;
    if bytes != canonical_json.as_bytes() {
        return Err(validation(
            "artifact link event bytes are not canonical sorted JSON with exactly one trailing newline",
        ));
    }
    let digest = sha256_hex(bytes);
    let path = artifact_link_event_path_for_digest(&digest)?;
    if let Some(expected_path) = expected_path {
        artifact_link_event_validate_path(expected_path, &digest)?;
    }
    Ok(ArtifactLinkEventCanonicalWire {
        schema_version: ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION,
        event,
        canonical_json,
        digest,
        path,
    })
}

/// Validate a canonical alias mapping.
pub fn canonicalize_artifact_link_alias(
    alias: &ArtifactLinkAliasWire,
) -> Result<ArtifactLinkAliasWire, ArtifactLinkError> {
    let old_ref = canonicalize_artifact_link_ref(&alias.old_ref)?;
    let new_ref = canonicalize_artifact_link_ref(&alias.new_ref)?;
    if old_ref == new_ref {
        return Err(validation(
            "artifact link alias cannot map a ref to itself",
        ));
    }
    Ok(ArtifactLinkAliasWire { old_ref, new_ref })
}

/// Resolve refs through a canonical alias graph, rejecting cycles and conflicts.
pub fn resolve_artifact_link_event_aliases(
    aliases: &[ArtifactLinkAliasWire],
    refs: &[String],
) -> Result<ArtifactLinkAliasResolutionWire, ArtifactLinkError> {
    let resolver = AliasResolver::new(aliases)?;
    let mut resolved_refs = BTreeMap::new();
    for raw in refs {
        let canonical = canonicalize_artifact_link_ref(raw)?;
        let resolved = resolver.resolve_canonical_ref(&canonical);
        resolved_refs.insert(canonical, resolved);
    }
    Ok(ArtifactLinkAliasResolutionWire {
        schema_version: ARTIFACT_LINK_EVENT_REDUCTION_WIRE_SCHEMA_VERSION,
        aliases: resolver.aliases(),
        resolved_refs,
    })
}

/// Reduce immutable link events into stable legacy row projections and metadata.
pub fn reduce_link_events(
    events: &[ArtifactLinkEventWire],
    aliases: &[ArtifactLinkAliasWire],
) -> Result<ArtifactLinkEventReductionWire, ArtifactLinkError> {
    let events = dedupe_events(events)?;
    let mut event_aliases = Vec::new();
    for alias in aliases {
        event_aliases.push(canonicalize_artifact_link_alias(alias)?);
    }
    for event in &events {
        if let ArtifactLinkEventKindWire::Alias { old_ref, new_ref } =
            &event.kind
        {
            event_aliases.push(canonicalize_artifact_link_alias(
                &ArtifactLinkAliasWire {
                    old_ref: old_ref.clone(),
                    new_ref: new_ref.clone(),
                },
            )?);
        }
    }
    let resolver = AliasResolver::new(&event_aliases)?;
    let mut edge_accums: BTreeMap<ArtifactLinkEventEdgeWire, EdgeAccum> =
        BTreeMap::new();
    let mut all_operation_ids = BTreeSet::new();
    let mut version_edges_by_op =
        BTreeMap::<String, BTreeSet<ArtifactLinkEventEdgeWire>>::new();
    let mut baseline_imports = BTreeMap::<String, String>::new();

    for event in events {
        all_operation_ids.insert(event.operation_id.clone());
        match event.kind {
            ArtifactLinkEventKindWire::Observation {
                edge,
                description,
                occurrences,
            } => {
                let edge = resolve_edge_aliases(&edge, &resolver)?;
                insert_version(
                    &mut edge_accums,
                    &mut version_edges_by_op,
                    &edge,
                    VersionAccum {
                        operation_id: event.operation_id,
                        kind: "observation".to_string(),
                        description,
                        origin: event.origin,
                        created_by: event.created_by,
                        created_at: event.created_at,
                        uses: occurrences,
                        supersedes: BTreeSet::new(),
                    },
                )?;
            }
            ArtifactLinkEventKindWire::EdgePut {
                edge,
                description,
                observed_operation_ids,
            } => {
                let edge = resolve_edge_aliases(&edge, &resolver)?;
                insert_version(
                    &mut edge_accums,
                    &mut version_edges_by_op,
                    &edge,
                    VersionAccum {
                        operation_id: event.operation_id,
                        kind: "edge-put".to_string(),
                        description,
                        origin: event.origin,
                        created_by: event.created_by,
                        created_at: event.created_at,
                        uses: 0,
                        supersedes: observed_operation_ids
                            .into_iter()
                            .collect(),
                    },
                )?;
            }
            ArtifactLinkEventKindWire::EdgeRemove {
                edge,
                observed_operation_ids,
            } => {
                let edge = resolve_edge_aliases(&edge, &resolver)?;
                let state = edge_accums.entry(edge).or_default();
                state.tombstones.insert(
                    event.operation_id.clone(),
                    TombstoneAccum {
                        operation_id: event.operation_id,
                        observed_operation_ids: observed_operation_ids
                            .into_iter()
                            .collect(),
                    },
                );
            }
            ArtifactLinkEventKindWire::Alias { .. } => {}
            ArtifactLinkEventKindWire::BaselineImport {
                import_id,
                source_head,
                rows,
            } => {
                let signature = baseline_import_signature(&source_head, &rows)?;
                if let Some(existing) = baseline_imports.get(&import_id) {
                    if existing != &signature {
                        return Err(validation(format!(
                            "baseline import_id `{import_id}` was reused with different content"
                        )));
                    }
                    continue;
                }
                baseline_imports.insert(import_id, signature);
                for row in rows {
                    let edge = edge_from_row(&row)?;
                    let edge = resolve_edge_aliases(&edge, &resolver)?;
                    insert_version(
                        &mut edge_accums,
                        &mut version_edges_by_op,
                        &edge,
                        VersionAccum {
                            operation_id: event.operation_id.clone(),
                            kind: "baseline-import".to_string(),
                            description: row.description,
                            origin: row.origin,
                            created_by: row.created_by,
                            created_at: row.created_at,
                            uses: row.uses,
                            supersedes: BTreeSet::new(),
                        },
                    )?;
                }
            }
        }
    }

    validate_predecessor_edges(
        &edge_accums,
        &all_operation_ids,
        &version_edges_by_op,
    )?;

    let mut rows = Vec::new();
    let mut edges = Vec::new();
    for (edge, state) in edge_accums {
        let reduced = reduce_edge(edge.clone(), state)?;
        if let Some(row) = &reduced.row {
            rows.push(row.clone());
        }
        edges.push(reduced);
    }
    rows.sort_by_key(row_sort_key);
    Ok(ArtifactLinkEventReductionWire {
        schema_version: ARTIFACT_LINK_EVENT_REDUCTION_WIRE_SCHEMA_VERSION,
        rows,
        edges,
        aliases: resolver.aliases(),
    })
}

fn canonicalize_event_kind(
    kind: &ArtifactLinkEventKindWire,
    operation_id: &str,
) -> Result<ArtifactLinkEventKindWire, ArtifactLinkError> {
    match kind {
        ArtifactLinkEventKindWire::Observation {
            edge,
            description,
            occurrences,
        } => {
            if *occurrences == 0 {
                return Err(validation(
                    "artifact link observation occurrences must be positive",
                ));
            }
            Ok(ArtifactLinkEventKindWire::Observation {
                edge: canonicalize_event_edge(edge)?,
                description: validate_artifact_link_description(description)?,
                occurrences: *occurrences,
            })
        }
        ArtifactLinkEventKindWire::EdgePut {
            edge,
            description,
            observed_operation_ids,
        } => Ok(ArtifactLinkEventKindWire::EdgePut {
            edge: canonicalize_event_edge(edge)?,
            description: validate_artifact_link_description(description)?,
            observed_operation_ids: normalize_operation_ids(
                "observed_operation_ids",
                observed_operation_ids,
                operation_id,
                false,
            )?,
        }),
        ArtifactLinkEventKindWire::EdgeRemove {
            edge,
            observed_operation_ids,
        } => Ok(ArtifactLinkEventKindWire::EdgeRemove {
            edge: canonicalize_event_edge(edge)?,
            observed_operation_ids: normalize_operation_ids(
                "observed_operation_ids",
                observed_operation_ids,
                operation_id,
                true,
            )?,
        }),
        ArtifactLinkEventKindWire::Alias { old_ref, new_ref } => {
            let alias =
                canonicalize_artifact_link_alias(&ArtifactLinkAliasWire {
                    old_ref: old_ref.clone(),
                    new_ref: new_ref.clone(),
                })?;
            Ok(ArtifactLinkEventKindWire::Alias {
                old_ref: alias.old_ref,
                new_ref: alias.new_ref,
            })
        }
        ArtifactLinkEventKindWire::BaselineImport {
            import_id,
            source_head,
            rows,
        } => Ok(ArtifactLinkEventKindWire::BaselineImport {
            import_id: validate_single_line("import_id", import_id)?,
            source_head: validate_single_line("source_head", source_head)?,
            rows: canonicalize_baseline_rows(rows)?,
        }),
    }
}

fn canonicalize_event_edge(
    edge: &ArtifactLinkEventEdgeWire,
) -> Result<ArtifactLinkEventEdgeWire, ArtifactLinkError> {
    match edge {
        ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => {
            let source_ref = canonicalize_artifact_link_ref(source_ref)?;
            let target_ref = canonicalize_artifact_link_ref(target_ref)?;
            if source_ref == target_ref {
                return Err(validation("artifact link cannot target itself"));
            }
            let relation = lookup_artifact_relation(relation)?;
            if relation.directed {
                Ok(ArtifactLinkEventEdgeWire::Directed {
                    source_ref,
                    relation: relation.slug,
                    target_ref,
                })
            } else {
                let (left_ref, right_ref) =
                    ordered_pair(source_ref, target_ref);
                Ok(ArtifactLinkEventEdgeWire::Undirected {
                    relation: relation.slug,
                    left_ref,
                    right_ref,
                })
            }
        }
        ArtifactLinkEventEdgeWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => {
            let relation = lookup_artifact_relation(relation)?;
            if relation.directed {
                return Err(validation(format!(
                    "directed relation `{}` cannot use an undirected edge identity",
                    relation.slug
                )));
            }
            let left_ref = canonicalize_artifact_link_ref(left_ref)?;
            let right_ref = canonicalize_artifact_link_ref(right_ref)?;
            if left_ref == right_ref {
                return Err(validation("artifact link cannot target itself"));
            }
            let (left_ref, right_ref) = ordered_pair(left_ref, right_ref);
            Ok(ArtifactLinkEventEdgeWire::Undirected {
                relation: relation.slug,
                left_ref,
                right_ref,
            })
        }
    }
}

fn canonicalize_baseline_rows(
    rows: &[ArtifactLinkRowWire],
) -> Result<Vec<ArtifactLinkRowWire>, ArtifactLinkError> {
    if rows.is_empty() {
        return Err(validation(
            "baseline-import rows must contain at least one row",
        ));
    }
    let mut by_edge =
        BTreeMap::<ArtifactLinkEventEdgeWire, ArtifactLinkRowWire>::new();
    for row in rows {
        let row = validate_artifact_link_row(row)?;
        let edge = edge_from_row(&row)?;
        if by_edge.insert(edge.clone(), row).is_some() {
            return Err(validation(format!(
                "baseline-import contains duplicate row for edge {}",
                edge_display(&edge)
            )));
        }
    }
    Ok(by_edge.into_values().collect())
}

fn edge_from_row(
    row: &ArtifactLinkRowWire,
) -> Result<ArtifactLinkEventEdgeWire, ArtifactLinkError> {
    match artifact_link_dedup_key(row)? {
        ArtifactLinkDedupKeyWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => Ok(ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            relation,
            target_ref,
        }),
        ArtifactLinkDedupKeyWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => Ok(ArtifactLinkEventEdgeWire::Undirected {
            relation,
            left_ref,
            right_ref,
        }),
    }
}

fn row_from_edge(
    edge: &ArtifactLinkEventEdgeWire,
    description: &str,
    origin: ArtifactLinkOriginWire,
    created_by: &str,
    created_at: &str,
    uses: u64,
) -> Result<ArtifactLinkRowWire, ArtifactLinkError> {
    let row = match edge {
        ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => ArtifactLinkRowWire {
            schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
            source_ref: source_ref.clone(),
            relation: relation.clone(),
            target_ref: target_ref.clone(),
            description: description.to_string(),
            origin,
            created_by: created_by.to_string(),
            created_at: created_at.to_string(),
            uses,
        },
        ArtifactLinkEventEdgeWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => ArtifactLinkRowWire {
            schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
            source_ref: left_ref.clone(),
            relation: relation.clone(),
            target_ref: right_ref.clone(),
            description: description.to_string(),
            origin,
            created_by: created_by.to_string(),
            created_at: created_at.to_string(),
            uses,
        },
    };
    validate_artifact_link_row(&row)
}

fn resolve_edge_aliases(
    edge: &ArtifactLinkEventEdgeWire,
    resolver: &AliasResolver,
) -> Result<ArtifactLinkEventEdgeWire, ArtifactLinkError> {
    match edge {
        ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => canonicalize_event_edge(&ArtifactLinkEventEdgeWire::Directed {
            source_ref: resolver.resolve_canonical_ref(source_ref),
            relation: relation.clone(),
            target_ref: resolver.resolve_canonical_ref(target_ref),
        }),
        ArtifactLinkEventEdgeWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => canonicalize_event_edge(&ArtifactLinkEventEdgeWire::Undirected {
            relation: relation.clone(),
            left_ref: resolver.resolve_canonical_ref(left_ref),
            right_ref: resolver.resolve_canonical_ref(right_ref),
        }),
    }
}

fn dedupe_events(
    events: &[ArtifactLinkEventWire],
) -> Result<Vec<ArtifactLinkEventWire>, ArtifactLinkError> {
    let mut by_operation =
        BTreeMap::<String, (String, ArtifactLinkEventWire)>::new();
    for event in events {
        let canonical = canonicalize_artifact_link_event(event)?;
        let canonical_json = canonical_json_for_serializable(&canonical)?;
        match by_operation.get(&canonical.operation_id) {
            Some((existing_json, _)) if existing_json == &canonical_json => {}
            Some(_) => {
                return Err(validation(format!(
                    "operation_id `{}` was reused for different artifact link events",
                    canonical.operation_id
                )));
            }
            None => {
                by_operation.insert(
                    canonical.operation_id.clone(),
                    (canonical_json, canonical),
                );
            }
        }
    }
    Ok(by_operation.into_values().map(|(_, event)| event).collect())
}

fn insert_version(
    edge_accums: &mut BTreeMap<ArtifactLinkEventEdgeWire, EdgeAccum>,
    version_edges_by_op: &mut BTreeMap<
        String,
        BTreeSet<ArtifactLinkEventEdgeWire>,
    >,
    edge: &ArtifactLinkEventEdgeWire,
    version: VersionAccum,
) -> Result<(), ArtifactLinkError> {
    let state = edge_accums.entry(edge.clone()).or_default();
    if state
        .versions
        .insert(version.operation_id.clone(), version.clone())
        .is_some()
    {
        return Err(validation(format!(
            "operation_id `{}` creates more than one version for edge {}",
            version.operation_id,
            edge_display(edge)
        )));
    }
    version_edges_by_op
        .entry(version.operation_id)
        .or_default()
        .insert(edge.clone());
    Ok(())
}

fn validate_predecessor_edges(
    edges: &BTreeMap<ArtifactLinkEventEdgeWire, EdgeAccum>,
    all_operation_ids: &BTreeSet<String>,
    version_edges_by_op: &BTreeMap<String, BTreeSet<ArtifactLinkEventEdgeWire>>,
) -> Result<(), ArtifactLinkError> {
    for (edge, state) in edges {
        for version in state.versions.values() {
            for predecessor in &version.supersedes {
                validate_known_predecessor_edge(
                    edge,
                    predecessor,
                    all_operation_ids,
                    version_edges_by_op,
                )?;
            }
        }
        for tombstone in state.tombstones.values() {
            for predecessor in &tombstone.observed_operation_ids {
                validate_known_predecessor_edge(
                    edge,
                    predecessor,
                    all_operation_ids,
                    version_edges_by_op,
                )?;
            }
        }
    }
    Ok(())
}

fn validate_known_predecessor_edge(
    edge: &ArtifactLinkEventEdgeWire,
    predecessor: &str,
    all_operation_ids: &BTreeSet<String>,
    version_edges_by_op: &BTreeMap<String, BTreeSet<ArtifactLinkEventEdgeWire>>,
) -> Result<(), ArtifactLinkError> {
    let Some(predecessor_edges) = version_edges_by_op.get(predecessor) else {
        if all_operation_ids.contains(predecessor) {
            return Err(validation(format!(
                "observed_operation_ids references non-version operation_id `{predecessor}`"
            )));
        }
        return Ok(());
    };
    if !predecessor_edges.contains(edge) {
        return Err(validation(format!(
            "observed_operation_ids references operation_id `{predecessor}` from a different edge"
        )));
    }
    Ok(())
}

fn reduce_edge(
    edge: ArtifactLinkEventEdgeWire,
    state: EdgeAccum,
) -> Result<ArtifactLinkReducedEdgeWire, ArtifactLinkError> {
    let mut removed_by = BTreeMap::<String, BTreeSet<String>>::new();
    for tombstone in state.tombstones.values() {
        for observed in &tombstone.observed_operation_ids {
            if state.versions.contains_key(observed) {
                removed_by
                    .entry(observed.clone())
                    .or_default()
                    .insert(tombstone.operation_id.clone());
            }
        }
    }
    let mut superseded_by = BTreeMap::<String, BTreeSet<String>>::new();
    for version in state.versions.values() {
        for observed in &version.supersedes {
            if state.versions.contains_key(observed) {
                superseded_by
                    .entry(observed.clone())
                    .or_default()
                    .insert(version.operation_id.clone());
            }
        }
    }

    let mut active_operation_ids = Vec::new();
    let mut uses = 0_u64;
    let mut versions = Vec::new();
    for (operation_id, version) in &state.versions {
        let removed_by_ids = sorted_strings(removed_by.get(operation_id));
        let superseded_by_ids = sorted_strings(superseded_by.get(operation_id));
        let active = removed_by_ids.is_empty() && superseded_by_ids.is_empty();
        if active {
            active_operation_ids.push(operation_id.clone());
        }
        if version.uses > 0 && removed_by_ids.is_empty() {
            uses = uses.saturating_add(version.uses);
        }
        versions.push(ArtifactLinkReducedVersionWire {
            operation_id: version.operation_id.clone(),
            kind: version.kind.clone(),
            description: version.description.clone(),
            origin: version.origin,
            created_by: version.created_by.clone(),
            created_at: version.created_at.clone(),
            uses: version.uses,
            active,
            removed_by: removed_by_ids,
            superseded_by: superseded_by_ids,
        });
    }

    active_operation_ids.sort();
    let row = active_operation_ids
        .last()
        .map(|operation_id| {
            let version = &state.versions[operation_id];
            row_from_edge(
                &edge,
                &version.description,
                version.origin,
                &version.created_by,
                &version.created_at,
                if uses == 0 { 1 } else { uses },
            )
        })
        .transpose()?;

    let tombstones = state
        .tombstones
        .into_values()
        .map(|tombstone| ArtifactLinkReducedTombstoneWire {
            operation_id: tombstone.operation_id,
            observed_operation_ids: tombstone
                .observed_operation_ids
                .into_iter()
                .collect(),
        })
        .collect();

    Ok(ArtifactLinkReducedEdgeWire {
        edge,
        row,
        versions,
        tombstones,
    })
}

impl AliasResolver {
    fn new(
        aliases: &[ArtifactLinkAliasWire],
    ) -> Result<Self, ArtifactLinkError> {
        let mut direct = BTreeMap::new();
        for alias in aliases {
            let alias = canonicalize_artifact_link_alias(alias)?;
            match direct.get(&alias.old_ref) {
                Some(existing) if existing == &alias.new_ref => {}
                Some(existing) => {
                    return Err(validation(format!(
                        "artifact link alias conflict for `{}`: `{}` and `{}`",
                        alias.old_ref, existing, alias.new_ref
                    )));
                }
                None => {
                    direct.insert(alias.old_ref, alias.new_ref);
                }
            }
        }

        let mut terminal = BTreeMap::new();
        for old_ref in direct.keys() {
            terminal.insert(old_ref.clone(), terminal_ref(old_ref, &direct)?);
        }
        Ok(Self { direct, terminal })
    }

    fn resolve_canonical_ref(&self, canonical: &str) -> String {
        self.terminal
            .get(canonical)
            .cloned()
            .unwrap_or_else(|| canonical.to_string())
    }

    fn aliases(&self) -> Vec<ArtifactLinkAliasWire> {
        self.direct
            .iter()
            .map(|(old_ref, new_ref)| ArtifactLinkAliasWire {
                old_ref: old_ref.clone(),
                new_ref: new_ref.clone(),
            })
            .collect()
    }
}

fn terminal_ref(
    start: &str,
    direct: &BTreeMap<String, String>,
) -> Result<String, ArtifactLinkError> {
    let mut seen = BTreeSet::new();
    let mut current = start.to_string();
    loop {
        if !seen.insert(current.clone()) {
            return Err(validation(format!(
                "artifact link alias cycle involving `{start}`"
            )));
        }
        let Some(next) = direct.get(&current) else {
            return Ok(current);
        };
        current = next.clone();
    }
}

fn baseline_import_signature(
    source_head: &str,
    rows: &[ArtifactLinkRowWire],
) -> Result<String, ArtifactLinkError> {
    canonical_json_for_value(&serde_json::json!({
        "source_head": source_head,
        "rows": rows,
    }))
}

fn canonical_json_for_serializable<T: Serialize>(
    value: &T,
) -> Result<String, ArtifactLinkError> {
    let value = serde_json::to_value(value).map_err(|error| {
        validation(format!(
            "unable to normalize artifact link event JSON: {error}"
        ))
    })?;
    canonical_json_for_value(&value)
}

fn canonical_json_for_value(
    value: &JsonValue,
) -> Result<String, ArtifactLinkError> {
    let sorted = canonical_json_value(value);
    let mut bytes = serde_json::to_vec(&sorted).map_err(|error| {
        validation(format!(
            "unable to encode artifact link event JSON: {error}"
        ))
    })?;
    bytes.push(b'\n');
    String::from_utf8(bytes).map_err(|error| {
        validation(format!(
            "artifact link event JSON was not valid UTF-8: {error}"
        ))
    })
}

fn canonical_json_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(entries) => {
            JsonValue::Array(entries.iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut sorted = JsonMap::new();
            for key in keys {
                sorted.insert(key.clone(), canonical_json_value(&map[key]));
            }
            JsonValue::Object(sorted)
        }
        other => other.clone(),
    }
}

fn ensure_integer_only_json(
    value: &JsonValue,
    path: &str,
) -> Result<(), ArtifactLinkError> {
    match value {
        JsonValue::Array(entries) => {
            for (index, entry) in entries.iter().enumerate() {
                ensure_integer_only_json(entry, &format!("{path}[{index}]"))?;
            }
        }
        JsonValue::Object(map) => {
            for (key, entry) in map {
                ensure_integer_only_json(entry, &format!("{path}.{key}"))?;
            }
        }
        JsonValue::Number(number) if number.is_f64() => {
            return Err(validation(format!(
                "artifact link event JSON number at {path} must be an integer"
            )));
        }
        _ => {}
    }
    Ok(())
}

fn normalize_operation_ids(
    label: &str,
    values: &[String],
    owner_operation_id: &str,
    require_non_empty: bool,
) -> Result<Vec<String>, ArtifactLinkError> {
    if require_non_empty && values.is_empty() {
        return Err(validation(format!("{label} must not be empty")));
    }
    let mut seen = BTreeSet::new();
    for raw in values {
        let operation_id = validate_operation_id(label, raw)?;
        if operation_id == owner_operation_id {
            return Err(validation(format!(
                "{label} cannot reference its own operation_id `{owner_operation_id}`"
            )));
        }
        if !seen.insert(operation_id.clone()) {
            return Err(validation(format!(
                "{label} contains duplicate operation_id `{operation_id}`"
            )));
        }
    }
    Ok(seen.into_iter().collect())
}

fn validate_operation_id(
    label: &str,
    value: &str,
) -> Result<String, ArtifactLinkError> {
    let value = value.trim();
    if value.len() != 32 || !is_lowercase_hex(value) {
        return Err(validation(format!(
            "{label} must be 32 lowercase hexadecimal characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_sha256_digest(value: &str) -> Result<String, ArtifactLinkError> {
    let value = value.trim();
    if value.len() != 64 || !is_lowercase_hex(value) {
        return Err(validation(
            "artifact link event digest must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(value.to_string())
}

fn validate_project_key(value: &str) -> Result<String, ArtifactLinkError> {
    let value = validate_single_line("project_key", value)?;
    let canonical = value.bytes().all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'_' | b'-' | b'.')
    });
    if !canonical {
        return Err(validation(
            "project_key must be canonical lowercase ASCII using letters, digits, `_`, `-`, or `.`",
        ));
    }
    Ok(value)
}

fn validate_single_line(
    label: &str,
    value: &str,
) -> Result<String, ArtifactLinkError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(validation(format!("{label} must be non-empty")));
    }
    if value.contains('\n') || value.contains('\r') {
        return Err(validation(format!("{label} must be a single line")));
    }
    Ok(value.to_string())
}

fn is_lowercase_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn ordered_pair(left: String, right: String) -> (String, String) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn sorted_strings(values: Option<&BTreeSet<String>>) -> Vec<String> {
    values
        .map(|values| values.iter().cloned().collect())
        .unwrap_or_default()
}

fn row_sort_key(row: &ArtifactLinkRowWire) -> ArtifactLinkEventEdgeWire {
    edge_from_row(row).expect("reduced rows are validated before sorting")
}

fn edge_display(edge: &ArtifactLinkEventEdgeWire) -> String {
    match edge {
        ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => format!("{source_ref} {relation} {target_ref}"),
        ArtifactLinkEventEdgeWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => format!("{left_ref} {relation} {right_ref}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validation(message: impl Into<String>) -> ArtifactLinkError {
    ArtifactLinkError::validation(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(ch: char) -> String {
        ch.to_string().repeat(32)
    }

    fn edge(
        source: &str,
        relation: &str,
        target: &str,
    ) -> ArtifactLinkEventEdgeWire {
        ArtifactLinkEventEdgeWire::Directed {
            source_ref: source.to_string(),
            relation: relation.to_string(),
            target_ref: target.to_string(),
        }
    }

    fn observation(
        operation_id: &str,
        target: &str,
        occurrences: u64,
    ) -> ArtifactLinkEventWire {
        ArtifactLinkEventWire {
            schema_version: 1,
            project_key: "gh_acme__widget".to_string(),
            operation_id: operation_id.to_string(),
            created_by: "agent:reader".to_string(),
            origin: ArtifactLinkOriginWire::Read,
            created_at: "2026-09-09T12:00:00Z".to_string(),
            kind: ArtifactLinkEventKindWire::Observation {
                edge: edge("agent:reader", "read", target),
                description: "read the artifact".to_string(),
                occurrences,
            },
        }
    }

    fn put(
        operation_id: &str,
        target: &str,
        observed: &[&str],
        description: &str,
    ) -> ArtifactLinkEventWire {
        ArtifactLinkEventWire {
            schema_version: 1,
            project_key: "gh_acme__widget".to_string(),
            operation_id: operation_id.to_string(),
            created_by: "agent:writer".to_string(),
            origin: ArtifactLinkOriginWire::Manual,
            created_at: "2026-09-09T12:01:00Z".to_string(),
            kind: ArtifactLinkEventKindWire::EdgePut {
                edge: edge("agent:reader", "read", target),
                description: description.to_string(),
                observed_operation_ids: observed
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect(),
            },
        }
    }

    fn remove(
        operation_id: &str,
        target: &str,
        observed: &[&str],
    ) -> ArtifactLinkEventWire {
        ArtifactLinkEventWire {
            schema_version: 1,
            project_key: "gh_acme__widget".to_string(),
            operation_id: operation_id.to_string(),
            created_by: "agent:writer".to_string(),
            origin: ArtifactLinkOriginWire::Manual,
            created_at: "2026-09-09T12:02:00Z".to_string(),
            kind: ArtifactLinkEventKindWire::EdgeRemove {
                edge: edge("agent:reader", "read", target),
                observed_operation_ids: observed
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect(),
            },
        }
    }

    fn row(
        source: &str,
        relation: &str,
        target: &str,
        description: &str,
        uses: u64,
    ) -> ArtifactLinkRowWire {
        ArtifactLinkRowWire {
            schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
            source_ref: source.to_string(),
            relation: relation.to_string(),
            target_ref: target.to_string(),
            description: description.to_string(),
            origin: ArtifactLinkOriginWire::Migrated,
            created_by: "importer".to_string(),
            created_at: "2026-09-09T11:59:00Z".to_string(),
            uses,
        }
    }

    #[test]
    fn canonical_json_digest_and_path_are_byte_stable() {
        let event = observation(&op('a'), "@plans:202609/a.md", 2);
        let canonical = canonicalize_artifact_link_event(&event).unwrap();
        let expected = format!(
            "{{\"created_at\":\"2026-09-09T12:00:00Z\",\"created_by\":\"agent:reader\",\"kind\":{{\"description\":\"read the artifact\",\"edge\":{{\"kind\":\"directed\",\"relation\":\"read\",\"source_ref\":\"agent:reader\",\"target_ref\":\"plan:202609/a.md\"}},\"occurrences\":2,\"type\":\"observation\"}},\"operation_id\":\"{}\",\"origin\":\"read\",\"project_key\":\"gh_acme__widget\",\"schema_version\":1}}\n",
            op('a')
        );
        assert_eq!(
            artifact_link_event_canonical_json(&canonical).unwrap(),
            expected
        );
        let digest = artifact_link_event_digest(&canonical).unwrap();
        assert_eq!(digest.len(), 64);
        let path = artifact_link_event_path_for_digest(&digest).unwrap();
        assert_eq!(
            path,
            format!("link-events/v1/{}/{}.json", &digest[..2], digest)
        );
        let validated = artifact_link_event_validate_bytes(
            expected.as_bytes(),
            Some(&path),
        )
        .unwrap();
        assert_eq!(validated.digest, digest);
        assert_eq!(validated.event, canonical);
        assert!(artifact_link_event_validate_bytes(
            expected.trim_end().as_bytes(),
            Some(&path)
        )
        .unwrap_err()
        .message
        .contains("not canonical"));
    }

    #[test]
    fn event_edge_identity_normalizes_directed_and_undirected_relations() {
        let related = ArtifactLinkEventWire {
            kind: ArtifactLinkEventKindWire::Observation {
                edge: edge("plan:202609/b.md", "related", "plan:202609/a.md"),
                description: "same concern".to_string(),
                occurrences: 1,
            },
            ..observation(&op('a'), "plan:202609/a.md", 1)
        };
        let canonical = canonicalize_artifact_link_event(&related).unwrap();
        assert_eq!(
            canonical.kind,
            ArtifactLinkEventKindWire::Observation {
                edge: ArtifactLinkEventEdgeWire::Undirected {
                    relation: "related".to_string(),
                    left_ref: "plan:202609/a.md".to_string(),
                    right_ref: "plan:202609/b.md".to_string(),
                },
                description: "same concern".to_string(),
                occurrences: 1,
            }
        );

        let invalid = ArtifactLinkEventEdgeWire::Undirected {
            relation: "read".to_string(),
            left_ref: "agent:reader".to_string(),
            right_ref: "plan:202609/a.md".to_string(),
        };
        assert!(canonicalize_event_edge(&invalid)
            .unwrap_err()
            .message
            .contains("directed relation"));
    }

    #[test]
    fn validation_rejects_bad_identifiers_payloads_and_edges() {
        let mut bad = observation("A", "plan:202609/a.md", 1);
        assert!(canonicalize_artifact_link_event(&bad)
            .unwrap_err()
            .message
            .contains("32 lowercase"));

        bad = observation(&op('a'), "plan:202609/a.md", 0);
        assert!(canonicalize_artifact_link_event(&bad)
            .unwrap_err()
            .message
            .contains("positive"));

        bad = put(&op('b'), "plan:202609/a.md", &[&op('a'), &op('a')], "new");
        assert!(canonicalize_artifact_link_event(&bad)
            .unwrap_err()
            .message
            .contains("duplicate"));

        bad = observation(&op('a'), "agent:reader", 1);
        assert!(canonicalize_artifact_link_event(&bad)
            .unwrap_err()
            .message
            .contains("itself"));

        let alias = ArtifactLinkAliasWire {
            old_ref: "plan:202609/a.md".to_string(),
            new_ref: "plans:202609/a.md".to_string(),
        };
        assert!(canonicalize_artifact_link_alias(&alias)
            .unwrap_err()
            .message
            .contains("itself"));
    }

    #[test]
    fn bytes_validation_rejects_floats_and_path_mismatches() {
        let raw = format!(
            "{{\"schema_version\":1,\"project_key\":\"gh_acme__widget\",\"operation_id\":\"{}\",\"created_by\":\"agent:reader\",\"origin\":\"read\",\"created_at\":\"2026-09-09T12:00:00Z\",\"kind\":{{\"type\":\"observation\",\"edge\":{{\"kind\":\"directed\",\"source_ref\":\"agent:reader\",\"relation\":\"read\",\"target_ref\":\"plan:202609/a.md\"}},\"description\":\"read the artifact\",\"occurrences\":1.0}}}}\n",
            op('a')
        );
        assert!(artifact_link_event_validate_bytes(raw.as_bytes(), None)
            .unwrap_err()
            .message
            .contains("integer"));

        let event = observation(&op('a'), "plan:202609/a.md", 1);
        let canonical = artifact_link_event_canonical_json(&event).unwrap();
        assert!(artifact_link_event_validate_bytes(
            canonical.as_bytes(),
            Some("link-events/v1/00/not-the-digest.json")
        )
        .unwrap_err()
        .message
        .contains("path"));
    }

    #[test]
    fn reduction_deduplicates_exact_retries_but_counts_distinct_observations() {
        let first = observation(&op('a'), "plan:202609/a.md", 2);
        let second = observation(&op('b'), "plan:202609/a.md", 3);
        let reduction =
            reduce_link_events(&[first.clone(), second, first], &[]).unwrap();
        assert_eq!(reduction.rows.len(), 1);
        assert_eq!(reduction.rows[0].uses, 5);
        assert_eq!(reduction.edges[0].versions.len(), 2);
    }

    #[test]
    fn reduction_rejects_operation_id_collisions() {
        let first = observation(&op('a'), "plan:202609/a.md", 1);
        let second = observation(&op('a'), "plan:202609/b.md", 1);
        assert!(reduce_link_events(&[first, second], &[])
            .unwrap_err()
            .message
            .contains("reused"));
    }

    #[test]
    fn edge_put_supersedes_named_versions_and_uses_lexical_tie_break() {
        let old = observation(&op('a'), "plan:202609/a.md", 4);
        let newer =
            put(&op('b'), "plan:202609/a.md", &[&op('a')], "new description");
        let concurrent = put(&op('c'), "plan:202609/a.md", &[], "tie wins");
        let reduction =
            reduce_link_events(&[concurrent, old, newer], &[]).unwrap();
        assert_eq!(reduction.rows[0].description, "tie wins");
        assert_eq!(reduction.rows[0].uses, 4);
        let old_version = reduction.edges[0]
            .versions
            .iter()
            .find(|version| version.operation_id == op('a'))
            .unwrap();
        assert!(!old_version.active);
        assert_eq!(old_version.superseded_by, vec![op('b')]);
    }

    #[test]
    fn remove_before_add_removes_only_observed_versions() {
        let removed = observation(&op('a'), "plan:202609/a.md", 2);
        let surviving = observation(&op('b'), "plan:202609/a.md", 3);
        let tombstone = remove(&op('c'), "plan:202609/a.md", &[&op('a')]);
        let reduction =
            reduce_link_events(&[tombstone, surviving, removed], &[]).unwrap();
        assert_eq!(reduction.rows.len(), 1);
        assert_eq!(reduction.rows[0].uses, 3);
        assert_eq!(
            reduction.edges[0].tombstones[0].observed_operation_ids,
            vec![op('a')]
        );
        let removed_version = reduction.edges[0]
            .versions
            .iter()
            .find(|version| version.operation_id == op('a'))
            .unwrap();
        assert!(!removed_version.active);
        assert_eq!(removed_version.removed_by, vec![op('c')]);
    }

    #[test]
    fn baseline_imports_preserve_counts_and_are_one_shot_by_import_id() {
        let baseline = ArtifactLinkEventWire {
            schema_version: 1,
            project_key: "gh_acme__widget".to_string(),
            operation_id: op('a'),
            created_by: "importer".to_string(),
            origin: ArtifactLinkOriginWire::Migrated,
            created_at: "2026-09-09T12:00:00Z".to_string(),
            kind: ArtifactLinkEventKindWire::BaselineImport {
                import_id: "legacy-main".to_string(),
                source_head: "main@abc123".to_string(),
                rows: vec![row(
                    "agent:reader",
                    "read",
                    "plan:202609/a.md",
                    "legacy description",
                    7,
                )],
            },
        };
        let duplicate_import = ArtifactLinkEventWire {
            operation_id: op('b'),
            ..baseline.clone()
        };
        let observed = observation(&op('c'), "plan:202609/a.md", 2);
        let rewrite =
            put(&op('d'), "plan:202609/a.md", &[&op('a')], "rewritten");
        let reduction = reduce_link_events(
            &[baseline, duplicate_import.clone(), observed, rewrite],
            &[],
        )
        .unwrap();
        assert_eq!(reduction.rows[0].description, "rewritten");
        assert_eq!(reduction.rows[0].uses, 9);

        let conflicting = ArtifactLinkEventWire {
            operation_id: op('e'),
            kind: ArtifactLinkEventKindWire::BaselineImport {
                import_id: "legacy-main".to_string(),
                source_head: "main@def456".to_string(),
                rows: vec![row(
                    "agent:reader",
                    "read",
                    "plan:202609/a.md",
                    "legacy description",
                    7,
                )],
            },
            ..observation(&op('e'), "plan:202609/a.md", 1)
        };
        assert!(reduce_link_events(&[duplicate_import, conflicting], &[])
            .unwrap_err()
            .message
            .contains("import_id"));
    }

    #[test]
    fn alias_chains_resolve_edges_and_cycles_or_conflicts_fail() {
        let aliases = vec![
            ArtifactLinkAliasWire {
                old_ref: "plan:202609/old.md".to_string(),
                new_ref: "plan:202609/mid.md".to_string(),
            },
            ArtifactLinkAliasWire {
                old_ref: "plan:202609/mid.md".to_string(),
                new_ref: "plan:202609/new.md".to_string(),
            },
        ];
        let reduction = reduce_link_events(
            &[observation(&op('a'), "plan:202609/old.md", 1)],
            &aliases,
        )
        .unwrap();
        assert_eq!(reduction.rows[0].target_ref, "plan:202609/new.md");
        let resolved = resolve_artifact_link_event_aliases(
            &aliases,
            &["plan:202609/old.md".to_string()],
        )
        .unwrap();
        assert_eq!(
            resolved.resolved_refs["plan:202609/old.md"],
            "plan:202609/new.md"
        );

        let cycle = vec![
            ArtifactLinkAliasWire {
                old_ref: "plan:a.md".to_string(),
                new_ref: "plan:b.md".to_string(),
            },
            ArtifactLinkAliasWire {
                old_ref: "plan:b.md".to_string(),
                new_ref: "plan:a.md".to_string(),
            },
        ];
        assert!(resolve_artifact_link_event_aliases(&cycle, &[])
            .unwrap_err()
            .message
            .contains("cycle"));

        let conflict = vec![
            ArtifactLinkAliasWire {
                old_ref: "plan:a.md".to_string(),
                new_ref: "plan:b.md".to_string(),
            },
            ArtifactLinkAliasWire {
                old_ref: "plan:a.md".to_string(),
                new_ref: "plan:c.md".to_string(),
            },
        ];
        assert!(resolve_artifact_link_event_aliases(&conflict, &[])
            .unwrap_err()
            .message
            .contains("conflict"));
    }

    #[test]
    fn predecessor_ids_known_on_other_edges_are_rejected() {
        let first = observation(&op('a'), "plan:202609/a.md", 1);
        let bad = put(&op('b'), "plan:202609/b.md", &[&op('a')], "bad edge");
        assert!(reduce_link_events(&[first, bad], &[])
            .unwrap_err()
            .message
            .contains("different edge"));
    }

    #[test]
    fn reduction_is_stable_for_reordered_and_duplicated_deliveries() {
        let events = vec![
            observation(&op('a'), "plan:202609/a.md", 2),
            put(&op('b'), "plan:202609/a.md", &[&op('a')], "new"),
            observation(&op('c'), "plan:202609/a.md", 3),
        ];
        let mut reordered = vec![
            events[2].clone(),
            events[0].clone(),
            events[1].clone(),
            events[0].clone(),
        ];
        let first = reduce_link_events(&events, &[]).unwrap();
        let second = reduce_link_events(&reordered, &[]).unwrap();
        assert_eq!(first, second);

        reordered.reverse();
        let third = reduce_link_events(&reordered, &[]).unwrap();
        assert_eq!(first, third);
    }
}
