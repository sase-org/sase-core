//! Closed v1 relation registry: builtins, inverses, reserved slugs.

use serde::{Deserialize, Serialize};

use super::wire::{reserved_relation_message, ArtifactLinkError};

pub const ARTIFACT_RELATION_WIRE_SCHEMA_VERSION: u64 = 1;

/// Slugs that error with a pointer to `sase bead dep` and are never stored.
pub const RESERVED_ARTIFACT_RELATION_SLUGS: &[&str] = &["blocks", "depends-on"];

/// One compiled-in relation. Assembly of plugins + config is a Python concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRelationWire {
    pub schema_version: u64,
    pub slug: String,
    pub inverse: String,
    pub directed: bool,
    /// Who is allowed to write this slug in v1 (`prompt_ref`, `read`, `cli`).
    pub written_by: String,
}

impl ArtifactRelationWire {
    fn builtin(
        slug: &str,
        inverse: &str,
        directed: bool,
        written_by: &str,
    ) -> Self {
        Self {
            schema_version: ARTIFACT_RELATION_WIRE_SCHEMA_VERSION,
            slug: slug.to_string(),
            inverse: inverse.to_string(),
            directed,
            written_by: written_by.to_string(),
        }
    }
}

/// v1 builtin relations. Callers may concatenate plugins later.
pub fn builtin_artifact_relations() -> Vec<ArtifactRelationWire> {
    vec![
        ArtifactRelationWire::builtin("cites", "cited-by", true, "prompt_ref"),
        ArtifactRelationWire::builtin("read", "read-by", true, "read"),
        ArtifactRelationWire::builtin("related", "related", false, "cli"),
        ArtifactRelationWire::builtin(
            "supersedes",
            "superseded-by",
            true,
            "cli",
        ),
        ArtifactRelationWire::builtin(
            "implements",
            "implemented-by",
            true,
            "cli",
        ),
        ArtifactRelationWire::builtin(
            "derives-from",
            "derived-into",
            true,
            "cli",
        ),
    ]
}

pub fn reserved_artifact_relation_slugs() -> Vec<String> {
    RESERVED_ARTIFACT_RELATION_SLUGS
        .iter()
        .map(|slug| (*slug).to_string())
        .collect()
}

/// Look up a write slug. Inverse-only names (`cited-by`) are not writable.
pub fn lookup_artifact_relation(
    slug: &str,
) -> Result<ArtifactRelationWire, ArtifactLinkError> {
    let slug = slug.trim();
    if slug.is_empty() {
        return Err(ArtifactLinkError::validation(
            "relation slug must not be empty",
        ));
    }
    if RESERVED_ARTIFACT_RELATION_SLUGS.contains(&slug) {
        return Err(ArtifactLinkError::reserved(reserved_relation_message(
            slug,
        )));
    }
    builtin_artifact_relations()
        .into_iter()
        .find(|relation| relation.slug == slug)
        .ok_or_else(|| {
            ArtifactLinkError::validation(format!(
                "unknown relation `{slug}`; expected one of {}",
                builtin_slugs_csv()
            ))
        })
}

/// Relation cell for a rendered table, from this document's perspective.
///
/// When this artifact is the source, the write slug is shown. When it is the
/// target, the registry inverse is shown. Undirected `related` is unchanged.
pub fn relation_label_from_perspective(
    slug: &str,
    this_is_source: bool,
) -> Result<String, ArtifactLinkError> {
    let relation = lookup_artifact_relation(slug)?;
    if this_is_source {
        Ok(relation.slug)
    } else {
        Ok(relation.inverse)
    }
}

fn builtin_slugs_csv() -> String {
    builtin_artifact_relations()
        .into_iter()
        .map(|relation| relation.slug)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_cover_v1_table() {
        let slugs: Vec<String> = builtin_artifact_relations()
            .into_iter()
            .map(|relation| relation.slug)
            .collect();
        assert_eq!(
            slugs,
            [
                "cites",
                "read",
                "related",
                "supersedes",
                "implements",
                "derives-from",
            ]
        );
        let related = lookup_artifact_relation("related").unwrap();
        assert!(!related.directed);
        assert_eq!(related.inverse, "related");
        let cites = lookup_artifact_relation("cites").unwrap();
        assert!(cites.directed);
        assert_eq!(cites.inverse, "cited-by");
    }

    #[test]
    fn reserved_slugs_point_at_bead_dep() {
        for slug in ["blocks", "depends-on"] {
            let err = lookup_artifact_relation(slug).unwrap_err();
            assert_eq!(err.kind, "reserved");
            assert!(err.message.contains("sase bead dep"), "{slug}");
        }
    }

    #[test]
    fn unknown_slug_lists_builtins() {
        let err = lookup_artifact_relation("duplicates").unwrap_err();
        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("cites"));
        assert!(err.message.contains("related"));
    }

    #[test]
    fn inverse_label_is_from_this_document() {
        assert_eq!(
            relation_label_from_perspective("implements", true).unwrap(),
            "implements"
        );
        assert_eq!(
            relation_label_from_perspective("implements", false).unwrap(),
            "implemented-by"
        );
        assert_eq!(
            relation_label_from_perspective("related", false).unwrap(),
            "related"
        );
    }
}
