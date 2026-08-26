//! Closed v1 relation registry: builtins, inverses, reserved slugs.

use serde::{Deserialize, Serialize};

use super::wire::{reserved_relation_message, ArtifactLinkError};

pub const ARTIFACT_RELATION_WIRE_SCHEMA_VERSION: u64 = 2;

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
    /// One sentence naming what the source and target endpoints mean.
    pub direction_note: String,
    /// A correctly-directed worked example, written `source relation target`.
    pub positive_example: String,
    /// The same pair inverted (or otherwise misused), to disambiguate direction.
    pub negative_example: String,
    /// Guidance only, not a validation gate. Empty means no recommendation.
    pub recommended_source_kinds: Vec<String>,
    /// Guidance only, not a validation gate. Empty means no recommendation.
    pub recommended_target_kinds: Vec<String>,
}

impl ArtifactRelationWire {
    #[allow(clippy::too_many_arguments)]
    fn builtin(
        slug: &str,
        inverse: &str,
        directed: bool,
        written_by: &str,
        direction_note: &str,
        positive_example: &str,
        negative_example: &str,
        recommended_source_kinds: &[&str],
        recommended_target_kinds: &[&str],
    ) -> Self {
        Self {
            schema_version: ARTIFACT_RELATION_WIRE_SCHEMA_VERSION,
            slug: slug.to_string(),
            inverse: inverse.to_string(),
            directed,
            written_by: written_by.to_string(),
            direction_note: direction_note.to_string(),
            positive_example: positive_example.to_string(),
            negative_example: negative_example.to_string(),
            recommended_source_kinds: recommended_source_kinds
                .iter()
                .map(|kind| (*kind).to_string())
                .collect(),
            recommended_target_kinds: recommended_target_kinds
                .iter()
                .map(|kind| (*kind).to_string())
                .collect(),
        }
    }
}

/// v1 builtin relations. Callers may concatenate plugins later.
pub fn builtin_artifact_relations() -> Vec<ArtifactRelationWire> {
    vec![
        ArtifactRelationWire::builtin(
            "cites",
            "cited-by",
            true,
            "prompt_ref",
            "The citing agent is the source; the cited artifact is the target.",
            "agent:sase-tj.land cites plan:202608/artifact_link_durability_and_derivation.md",
            "plan:202608/artifact_link_durability_and_derivation.md cites agent:sase-tj.land",
            &["agent"],
            &["plan", "research"],
        ),
        ArtifactRelationWire::builtin(
            "read",
            "read-by",
            true,
            "read",
            "The reading agent is the source; the artifact it read is the target.",
            "agent:sase-tj.land read research:202608/artifact_link_derivation.md",
            "research:202608/artifact_link_derivation.md read agent:sase-tj.land",
            &["agent"],
            &[],
        ),
        ArtifactRelationWire::builtin(
            "related",
            "related",
            false,
            "cli",
            "Undirected: the same fact either way, so source and target are \
             interchangeable.",
            "plan:202608/a.md related plan:202608/b.md",
            "plan:202608/a.md related bead:sase-tw (imprecise -- prefer `implements` \
             when a plan implements a bead's requirements)",
            &[],
            &[],
        ),
        ArtifactRelationWire::builtin(
            "supersedes",
            "superseded-by",
            true,
            "cli",
            "The replacement artifact is the source; the artifact it replaces is the \
             target.",
            "plan:202608/v2_design.md supersedes plan:202608/v1_design.md",
            "plan:202608/v1_design.md supersedes plan:202608/v2_design.md",
            &[],
            &[],
        ),
        ArtifactRelationWire::builtin(
            "implements",
            "implemented-by",
            true,
            "cli",
            "A plan implements a bead's requirements: the plan is the source, the \
             bead is the target.",
            "plan:202608/artifact_link_durability_and_derivation.md implements \
             bead:sase-tw",
            "bead:sase-tw implements \
             plan:202608/artifact_link_durability_and_derivation.md",
            &["plan"],
            &["bead"],
        ),
        ArtifactRelationWire::builtin(
            "derives-from",
            "derived-into",
            true,
            "cli",
            "The derived artifact is the source; the artifact it was derived from is \
             the target.",
            "research:202608/artifact_link_derivation.md derives-from \
             research:202608/artifact_link_derivation__a.md",
            "research:202608/artifact_link_derivation__a.md derives-from \
             research:202608/artifact_link_derivation.md",
            &["plan", "research"],
            &["plan", "research"],
        ),
        ArtifactRelationWire::builtin(
            "produced-by",
            "produced",
            true,
            "projection",
            "The stitch is the source; the agent that produced it is the target.",
            "stitch:sase@0123456789abcdef0123456789abcdef01234567 produced-by \
             agent:sase-tj.land",
            "agent:sase-tj.land produced-by \
             stitch:sase@0123456789abcdef0123456789abcdef01234567",
            &["stitch"],
            &["agent"],
        ),
        ArtifactRelationWire::builtin(
            "launched",
            "launched-by",
            true,
            "projection",
            "The chop is the source; the agent it launched is the target.",
            "chop:refresh_docs/refresh_docs launched agent:sase-tj.land",
            "agent:sase-tj.land launched chop:refresh_docs/refresh_docs",
            &["chop"],
            &["agent"],
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
                "produced-by",
                "launched",
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
    fn every_builtin_documents_direction_and_examples() {
        for relation in builtin_artifact_relations() {
            assert!(
                !relation.direction_note.is_empty(),
                "{} is missing a direction note",
                relation.slug
            );
            assert!(
                !relation.positive_example.is_empty(),
                "{} is missing a positive example",
                relation.slug
            );
            assert!(
                !relation.negative_example.is_empty(),
                "{} is missing a negative example",
                relation.slug
            );
            assert!(
                relation.positive_example.contains(&relation.slug),
                "{} positive example does not use its own slug",
                relation.slug
            );
        }
    }

    #[test]
    fn implements_settles_the_plan_bead_direction() {
        let implements = lookup_artifact_relation("implements").unwrap();
        assert_eq!(implements.inverse, "implemented-by");
        assert_eq!(implements.recommended_source_kinds, ["plan"]);
        assert_eq!(implements.recommended_target_kinds, ["bead"]);
        assert!(implements.positive_example.starts_with("plan:"));
        assert!(implements.positive_example.contains("implements bead:"));
        assert!(implements.negative_example.starts_with("bead:"));
        assert!(implements.negative_example.contains("implements plan:"));
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

    #[test]
    fn produced_by_and_launched_round_trip() {
        assert_eq!(
            relation_label_from_perspective("produced-by", true).unwrap(),
            "produced-by"
        );
        assert_eq!(
            relation_label_from_perspective("produced-by", false).unwrap(),
            "produced"
        );
        assert_eq!(
            relation_label_from_perspective("launched", true).unwrap(),
            "launched"
        );
        assert_eq!(
            relation_label_from_perspective("launched", false).unwrap(),
            "launched-by"
        );
    }
}
