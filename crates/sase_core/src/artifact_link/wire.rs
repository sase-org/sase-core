//! Link-row wire types, canonicalization, and per-artifact / aggregate indexes.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::artifact_ref::{
    parse_artifact_ref_canonical, ArtifactRefError, CanonicalArtifactRefWire,
};

use super::relation::{lookup_artifact_relation, ArtifactRelationWire};

/// Schema version for v2 link rows and the indexes that store them.
pub const ARTIFACT_LINK_ROW_SCHEMA_VERSION: u64 = 2;

const MAX_DESCRIPTION_CHARS: usize = 240;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct ArtifactLinkError {
    pub kind: String,
    pub message: String,
}

impl ArtifactLinkError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }

    pub fn reserved(message: impl Into<String>) -> Self {
        Self {
            kind: "reserved".to_string(),
            message: message.into(),
        }
    }

    pub fn none(message: impl Into<String>) -> Self {
        Self {
            kind: "none".to_string(),
            message: message.into(),
        }
    }
}

impl From<ArtifactRefError> for ArtifactLinkError {
    fn from(error: ArtifactRefError) -> Self {
        Self {
            kind: error.kind,
            message: error.message,
        }
    }
}

/// How a stored link row was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkOriginWire {
    Manual,
    Migrated,
    PromptRef,
    Read,
    Derived,
}

impl ArtifactLinkOriginWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Migrated => "migrated",
            Self::PromptRef => "prompt_ref",
            Self::Read => "read",
            Self::Derived => "derived",
        }
    }

    pub fn increments_uses(self) -> bool {
        matches!(self, Self::PromptRef | Self::Read)
    }
}

/// One directed (or undirected-stored) link graph row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkRowWire {
    pub schema_version: u64,
    pub source_ref: String,
    pub relation: String,
    pub target_ref: String,
    pub description: String,
    pub origin: ArtifactLinkOriginWire,
    pub created_by: String,
    pub created_at: String,
    #[serde(default = "default_uses")]
    pub uses: u64,
}

fn default_uses() -> u64 {
    1
}

/// Per-artifact JSON: every row touching this artifact, both directions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexWire {
    pub schema_version: u64,
    pub artifact_ref: String,
    #[serde(default)]
    pub rows: Vec<ArtifactLinkRowWire>,
}

/// Rebuildable project-local aggregate of every link row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkAggregateWire {
    pub schema_version: u64,
    #[serde(default)]
    pub rows: Vec<ArtifactLinkRowWire>,
}

/// Projected bead-page / `IssueWire.links` row.
///
/// The bead itself is the source; events carry `target_ref`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadLinkWire {
    pub target_ref: String,
    pub relation: String,
    pub description: String,
    pub origin: ArtifactLinkOriginWire,
}

/// Directed vs undirected identity for one stored edge.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactLinkDedupKeyWire {
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

/// Result of inserting or rewriting one row in a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkUpsertKindWire {
    Added,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkUpsertWire {
    pub kind: ArtifactLinkUpsertKindWire,
    pub row: ArtifactLinkRowWire,
}

/// Strip a leading `@`, rewrite kind aliases, and render the canonical ref.
pub fn canonicalize_artifact_link_ref(
    value: &str,
) -> Result<String, ArtifactLinkError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ArtifactLinkError::validation(
            "artifact reference must not be empty",
        ));
    }
    let without_sigil = trimmed.strip_prefix('@').unwrap_or(trimmed).trim();
    if without_sigil.is_empty() {
        return Err(ArtifactLinkError::validation(
            "artifact reference must not be empty",
        ));
    }
    let canonical: CanonicalArtifactRefWire =
        parse_artifact_ref_canonical(without_sigil)?;
    if canonical.reference.rendered.is_empty() {
        return Err(ArtifactLinkError::validation(
            "canonical artifact reference is empty",
        ));
    }
    Ok(canonical.reference.rendered)
}

/// Trim, require a single line, and cap at 240 characters.
pub fn validate_artifact_link_description(
    description: &str,
) -> Result<String, ArtifactLinkError> {
    let trimmed = description.trim();
    if trimmed.is_empty() {
        return Err(ArtifactLinkError::validation(
            "link description must be non-empty",
        ));
    }
    if trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(ArtifactLinkError::validation(
            "link description must be a single line",
        ));
    }
    if trimmed.chars().count() > MAX_DESCRIPTION_CHARS {
        return Err(ArtifactLinkError::validation(format!(
            "link description must be at most {MAX_DESCRIPTION_CHARS} characters"
        )));
    }
    Ok(trimmed.to_string())
}

/// Canonicalize refs, validate relation and description, reject self-links.
pub fn validate_artifact_link_row(
    row: &ArtifactLinkRowWire,
) -> Result<ArtifactLinkRowWire, ArtifactLinkError> {
    if row.schema_version != ARTIFACT_LINK_ROW_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "unsupported artifact link schema_version {}; expected {}",
            row.schema_version, ARTIFACT_LINK_ROW_SCHEMA_VERSION
        )));
    }
    let source_ref = canonicalize_artifact_link_ref(&row.source_ref)?;
    let target_ref = canonicalize_artifact_link_ref(&row.target_ref)?;
    if source_ref == target_ref {
        return Err(ArtifactLinkError::validation(
            "artifact link cannot target itself",
        ));
    }
    let _relation = lookup_artifact_relation(&row.relation)?;
    let description = validate_artifact_link_description(&row.description)?;
    if row.created_by.trim().is_empty() {
        return Err(ArtifactLinkError::validation(
            "link created_by must be non-empty",
        ));
    }
    if row.created_at.trim().is_empty() {
        return Err(ArtifactLinkError::validation(
            "link created_at must be non-empty",
        ));
    }
    let uses = if row.uses == 0 { 1 } else { row.uses };
    Ok(ArtifactLinkRowWire {
        schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
        source_ref,
        relation: row.relation.trim().to_string(),
        target_ref,
        description,
        origin: row.origin,
        created_by: row.created_by.trim().to_string(),
        created_at: row.created_at.trim().to_string(),
        uses,
    })
}

/// Identity used to collapse rewrites of the same stored edge.
pub fn artifact_link_dedup_key(
    row: &ArtifactLinkRowWire,
) -> Result<ArtifactLinkDedupKeyWire, ArtifactLinkError> {
    let relation = lookup_artifact_relation(&row.relation)?;
    let source = canonicalize_artifact_link_ref(&row.source_ref)?;
    let target = canonicalize_artifact_link_ref(&row.target_ref)?;
    Ok(dedup_key_for(&relation, &source, &target))
}

fn dedup_key_for(
    relation: &ArtifactRelationWire,
    source: &str,
    target: &str,
) -> ArtifactLinkDedupKeyWire {
    if relation.directed {
        ArtifactLinkDedupKeyWire::Directed {
            source_ref: source.to_string(),
            relation: relation.slug.to_string(),
            target_ref: target.to_string(),
        }
    } else {
        let (left_ref, right_ref) = if source <= target {
            (source.to_string(), target.to_string())
        } else {
            (target.to_string(), source.to_string())
        };
        ArtifactLinkDedupKeyWire::Undirected {
            relation: relation.slug.to_string(),
            left_ref,
            right_ref,
        }
    }
}

/// Insert or rewrite `incoming` in `rows`.
///
/// A rewrite updates `description` (and increments `uses` for `prompt_ref` /
/// `read`) while leaving `created_at` / `created_by` stable.
pub fn upsert_artifact_link_row(
    rows: &mut Vec<ArtifactLinkRowWire>,
    incoming: ArtifactLinkRowWire,
) -> Result<ArtifactLinkUpsertWire, ArtifactLinkError> {
    let incoming = validate_artifact_link_row(&incoming)?;
    let incoming_key = artifact_link_dedup_key(&incoming)?;
    if let Some(existing) = rows.iter_mut().find(|row| {
        artifact_link_dedup_key(row).ok().as_ref() == Some(&incoming_key)
    }) {
        let description_changed = existing.description != incoming.description;
        let origin_increments = incoming.origin.increments_uses();
        if !description_changed && !origin_increments {
            return Ok(ArtifactLinkUpsertWire {
                kind: ArtifactLinkUpsertKindWire::Unchanged,
                row: existing.clone(),
            });
        }
        existing.description = incoming.description;
        if origin_increments {
            existing.uses = existing.uses.saturating_add(1);
        }
        return Ok(ArtifactLinkUpsertWire {
            kind: ArtifactLinkUpsertKindWire::Updated,
            row: existing.clone(),
        });
    }
    rows.push(incoming.clone());
    Ok(ArtifactLinkUpsertWire {
        kind: ArtifactLinkUpsertKindWire::Added,
        row: incoming,
    })
}

/// Reserved-slug error text used by the relation registry and bead events.
pub(crate) fn reserved_relation_message(slug: &str) -> String {
    format!(
        "relation `{slug}` is reserved for bead dependencies; use `sase bead dep`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, relation: &str, target: &str) -> ArtifactLinkRowWire {
        ArtifactLinkRowWire {
            schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
            source_ref: source.to_string(),
            relation: relation.to_string(),
            target_ref: target.to_string(),
            description: "extends the ref contract this epic landed"
                .to_string(),
            origin: ArtifactLinkOriginWire::Manual,
            created_by: "bbugyi200.athena.y2".to_string(),
            created_at: "2026-08-18T23:40:00Z".to_string(),
            uses: 1,
        }
    }

    #[test]
    fn canonicalize_strips_sigil_and_rewrites_kind_aliases() {
        assert_eq!(
            canonicalize_artifact_link_ref(
                "@commit:sase@0123456789abcdef0123456789abcdef01234567"
            )
            .unwrap(),
            "stitch:sase@0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(
            canonicalize_artifact_link_ref("plans:202608/report.md").unwrap(),
            "plan:202608/report.md"
        );
        assert_eq!(
            canonicalize_artifact_link_ref("bead:sase-js").unwrap(),
            "bead:sase-js"
        );
    }

    #[test]
    fn validate_rejects_self_links_and_blank_descriptions() {
        let err = validate_artifact_link_row(&row(
            "bead:sase-js",
            "related",
            "bead:sase-js",
        ))
        .unwrap_err();
        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("itself"));

        let mut blank = row("bead:sase-js", "related", "bead:sase-ct");
        blank.description = "   ".to_string();
        let err = validate_artifact_link_row(&blank).unwrap_err();
        assert!(err.message.contains("non-empty"));
    }

    #[test]
    fn validate_rejects_multiline_and_overlong_descriptions() {
        let mut multiline = row("bead:sase-js", "related", "bead:sase-ct");
        multiline.description = "one\ntwo".to_string();
        assert!(validate_artifact_link_row(&multiline)
            .unwrap_err()
            .message
            .contains("single line"));

        let mut long = row("bead:sase-js", "related", "bead:sase-ct");
        long.description = "x".repeat(241);
        assert!(validate_artifact_link_row(&long)
            .unwrap_err()
            .message
            .contains("240"));
    }

    #[test]
    fn undirected_related_dedups_either_direction() {
        let forward = row("bead:sase-a", "related", "bead:sase-b");
        let reverse = row("bead:sase-b", "related", "bead:sase-a");
        assert_eq!(
            artifact_link_dedup_key(&forward).unwrap(),
            artifact_link_dedup_key(&reverse).unwrap()
        );
        let mut rows = Vec::new();
        let added = upsert_artifact_link_row(&mut rows, forward).unwrap();
        assert_eq!(added.kind, ArtifactLinkUpsertKindWire::Added);
        let mut reverse = reverse;
        reverse.description = "shares the ACE-TUI flake root cause".to_string();
        let updated = upsert_artifact_link_row(&mut rows, reverse).unwrap();
        assert_eq!(updated.kind, ArtifactLinkUpsertKindWire::Updated);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].description, "shares the ACE-TUI flake root cause");
        assert_eq!(rows[0].created_by, "bbugyi200.athena.y2");
    }

    #[test]
    fn directed_same_pair_may_carry_several_relations() {
        let mut rows = Vec::new();
        upsert_artifact_link_row(
            &mut rows,
            row("plan:a.md", "implements", "bead:sase-js"),
        )
        .unwrap();
        upsert_artifact_link_row(
            &mut rows,
            row("plan:a.md", "derives-from", "bead:sase-js"),
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn prompt_ref_rewrite_increments_uses_and_keeps_created_at() {
        let mut rows = Vec::new();
        upsert_artifact_link_row(
            &mut rows,
            ArtifactLinkRowWire {
                origin: ArtifactLinkOriginWire::PromptRef,
                ..row("agent:alice", "cites", "plan:a.md")
            },
        )
        .unwrap();
        let again = upsert_artifact_link_row(
            &mut rows,
            ArtifactLinkRowWire {
                origin: ArtifactLinkOriginWire::PromptRef,
                description: "cited again".to_string(),
                created_by: "someone-else".to_string(),
                created_at: "2026-08-19T00:00:00Z".to_string(),
                ..row("agent:alice", "cites", "plan:a.md")
            },
        )
        .unwrap();
        assert_eq!(again.kind, ArtifactLinkUpsertKindWire::Updated);
        assert_eq!(rows[0].uses, 2);
        assert_eq!(rows[0].created_by, "bbugyi200.athena.y2");
        assert_eq!(rows[0].created_at, "2026-08-18T23:40:00Z");
        assert_eq!(rows[0].description, "cited again");
    }

    #[test]
    fn reserved_slugs_are_not_stored() {
        let err = validate_artifact_link_row(&row(
            "bead:sase-a",
            "blocks",
            "bead:sase-b",
        ))
        .unwrap_err();
        assert_eq!(err.kind, "reserved");
        assert!(err.message.contains("sase bead dep"));
    }
}
