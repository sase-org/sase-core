//! Permanent kind-alias registry and canonicalizing parse entry point.
//!
//! `parse_artifact_ref` in the parent module never rewrites a kind label —
//! that keeps every persisted `commit:`/`plans:` string byte-identical.
//! This module is the one opt-in place a caller can ask for the canonical
//! spelling instead, carrying the alias and any diagnostic along with it.

use super::wire::{ArtifactRefError, ArtifactRefKindWire};
use super::{
    kind_rejects_fragments, parse_artifact_ref, ParsedArtifactRefWire,
};

use serde::{Deserialize, Serialize};

pub const ARTIFACT_REF_KIND_CATALOG_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRefKindStatusWire {
    Live,
    Alias,
    Deprecated,
    Historical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefKindDescriptorWire {
    pub schema_version: u64,
    pub kind: String,
    pub display_name: String,
    pub status: ArtifactRefKindStatusWire,
    pub aliases: Vec<String>,
    pub reserved: bool,
    pub argument_summary: String,
    pub offered_in_completion: bool,
    pub accepts_fragment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefKindAliasWire {
    pub schema_version: u64,
    pub requested: String,
    pub canonical: String,
    pub alias: bool,
    pub status: ArtifactRefKindStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalArtifactRefWire {
    pub schema_version: u64,
    pub reference: ParsedArtifactRefWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

/// One compiled-in entry of the kind registry.
///
/// `canonical` is `Some(target)` for a permanent alias; `None` for a kind
/// that parses and renders under its own name. `fragment_probe` reuses the
/// parent module's fragment-acceptance rule instead of duplicating it.
struct KindRegistration {
    kind: &'static str,
    display_name: &'static str,
    status: ArtifactRefKindStatusWire,
    canonical: Option<&'static str>,
    aliases: &'static [&'static str],
    reserved: bool,
    argument_summary: &'static str,
    offered_in_completion: bool,
    fragment_probe: ArtifactRefKindWire,
    diagnostic: Option<&'static str>,
}

fn registrations() -> Vec<KindRegistration> {
    vec![
        KindRegistration {
            kind: "stitch",
            display_name: "Stitch",
            status: ArtifactRefKindStatusWire::Live,
            canonical: None,
            aliases: &["commit"],
            reserved: true,
            argument_summary: "stitch:<sha> or stitch:<repo>@<sha>",
            offered_in_completion: true,
            fragment_probe: ArtifactRefKindWire::Stitch,
            diagnostic: None,
        },
        KindRegistration {
            kind: "patch",
            display_name: "Patch",
            status: ArtifactRefKindStatusWire::Live,
            canonical: None,
            aliases: &[],
            reserved: true,
            argument_summary: "patch:<name>",
            offered_in_completion: true,
            fragment_probe: ArtifactRefKindWire::Patch,
            diagnostic: None,
        },
        KindRegistration {
            kind: "bead",
            display_name: "Bead",
            status: ArtifactRefKindStatusWire::Live,
            canonical: None,
            aliases: &[],
            reserved: true,
            argument_summary: "bead:<id>",
            offered_in_completion: true,
            fragment_probe: ArtifactRefKindWire::Bead,
            diagnostic: None,
        },
        KindRegistration {
            kind: "agent",
            display_name: "Agent",
            status: ArtifactRefKindStatusWire::Live,
            canonical: None,
            aliases: &[],
            reserved: true,
            argument_summary: "agent:<name>",
            offered_in_completion: true,
            fragment_probe: ArtifactRefKindWire::Agent,
            diagnostic: None,
        },
        KindRegistration {
            kind: "file",
            display_name: "File",
            status: ArtifactRefKindStatusWire::Live,
            canonical: None,
            aliases: &[],
            reserved: true,
            argument_summary: "file:<path> or file:(explicit|default):<hex24>",
            offered_in_completion: true,
            fragment_probe: ArtifactRefKindWire::File,
            diagnostic: None,
        },
        KindRegistration {
            kind: "commit",
            display_name: "Commit",
            status: ArtifactRefKindStatusWire::Alias,
            canonical: Some("stitch"),
            aliases: &[],
            reserved: false,
            argument_summary: "commit:<repo>@<sha> (permanent alias of stitch)",
            offered_in_completion: false,
            fragment_probe: ArtifactRefKindWire::Commit,
            diagnostic: None,
        },
        KindRegistration {
            kind: "plans",
            display_name: "Plans",
            status: ArtifactRefKindStatusWire::Deprecated,
            canonical: Some("plan"),
            aliases: &[],
            reserved: false,
            argument_summary: "plans:<path> (deprecated alias of plan)",
            offered_in_completion: false,
            fragment_probe: ArtifactRefKindWire::Document {
                role: "plan".to_string(),
            },
            diagnostic: Some("@plans: is deprecated; write @plan:<path>"),
        },
        KindRegistration {
            kind: "chat",
            display_name: "Chat",
            status: ArtifactRefKindStatusWire::Historical,
            canonical: None,
            aliases: &[],
            reserved: false,
            argument_summary: "chat:<path>",
            offered_in_completion: false,
            fragment_probe: ArtifactRefKindWire::Chat,
            diagnostic: None,
        },
        KindRegistration {
            kind: "bug",
            display_name: "Bug",
            status: ArtifactRefKindStatusWire::Historical,
            canonical: None,
            aliases: &[],
            reserved: false,
            argument_summary: "bug:<project>#<number>",
            offered_in_completion: false,
            fragment_probe: ArtifactRefKindWire::Bug,
            diagnostic: None,
        },
    ]
}

/// Return every compiled-in artifact-reference kind, live or historical.
pub fn artifact_ref_kind_catalog() -> Vec<ArtifactRefKindDescriptorWire> {
    registrations()
        .into_iter()
        .map(|registration| ArtifactRefKindDescriptorWire {
            schema_version: ARTIFACT_REF_KIND_CATALOG_WIRE_SCHEMA_VERSION,
            kind: registration.kind.to_string(),
            display_name: registration.display_name.to_string(),
            status: registration.status,
            aliases: registration
                .aliases
                .iter()
                .map(|alias| (*alias).to_string())
                .collect(),
            reserved: registration.reserved,
            argument_summary: registration.argument_summary.to_string(),
            offered_in_completion: registration.offered_in_completion,
            accepts_fragment: !kind_rejects_fragments(
                &registration.fragment_probe,
            ),
        })
        .collect()
}

/// Resolve one requested kind label against the permanent alias registry.
///
/// A label absent from the registry is an unregistered document kind:
/// canonical to itself, `Live`, not reserved.
pub fn canonical_artifact_ref_kind(label: &str) -> ArtifactRefKindAliasWire {
    if let Some(registration) = registrations()
        .into_iter()
        .find(|entry| entry.kind == label)
    {
        let canonical = registration.canonical.unwrap_or(registration.kind);
        return ArtifactRefKindAliasWire {
            schema_version: ARTIFACT_REF_KIND_CATALOG_WIRE_SCHEMA_VERSION,
            requested: label.to_string(),
            canonical: canonical.to_string(),
            alias: registration.canonical.is_some(),
            status: registration.status,
            diagnostic: registration.diagnostic.map(str::to_string),
        };
    }
    ArtifactRefKindAliasWire {
        schema_version: ARTIFACT_REF_KIND_CATALOG_WIRE_SCHEMA_VERSION,
        requested: label.to_string(),
        canonical: label.to_string(),
        alias: false,
        status: ArtifactRefKindStatusWire::Live,
        diagnostic: None,
    }
}

/// Parse one reference after rewriting only its kind label to canonical.
///
/// `parse_artifact_ref` itself never rewrites a kind; this is the one entry
/// point that does, so a caller can opt in to accepting `commit:`/`plans:`
/// spellings while getting back the canonical `stitch:`/`plan:` reference.
pub fn parse_artifact_ref_canonical(
    value: &str,
) -> Result<CanonicalArtifactRefWire, ArtifactRefError> {
    let (kind_label, rest) = value.split_once(':').ok_or_else(|| {
        ArtifactRefError::validation(
            "artifact reference must contain a kind separator ':'",
        )
    })?;
    let resolved = canonical_artifact_ref_kind(kind_label);
    let rewritten = if resolved.alias {
        format!("{}:{rest}", resolved.canonical)
    } else {
        value.to_string()
    };
    let reference = parse_artifact_ref(&rewritten)?;
    Ok(CanonicalArtifactRefWire {
        schema_version: ARTIFACT_REF_KIND_CATALOG_WIRE_SCHEMA_VERSION,
        reference,
        alias: resolved.alias.then(|| kind_label.to_string()),
        diagnostic: resolved.diagnostic,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_lists_reserved_kinds_offered_in_completion() {
        let catalog = artifact_ref_kind_catalog();
        for kind in ["stitch", "patch", "bead", "agent", "file"] {
            let descriptor = catalog
                .iter()
                .find(|descriptor| descriptor.kind == kind)
                .unwrap_or_else(|| panic!("missing {kind}"));
            assert!(descriptor.reserved, "{kind}");
            assert!(descriptor.offered_in_completion, "{kind}");
            assert_eq!(descriptor.status, ArtifactRefKindStatusWire::Live);
        }
        let stitch = catalog
            .iter()
            .find(|descriptor| descriptor.kind == "stitch")
            .unwrap();
        assert_eq!(stitch.aliases, ["commit"]);
    }

    #[test]
    fn catalog_marks_aliases_and_historical_kinds_absent_from_completion() {
        let catalog = artifact_ref_kind_catalog();
        for (kind, status) in [
            ("commit", ArtifactRefKindStatusWire::Alias),
            ("plans", ArtifactRefKindStatusWire::Deprecated),
            ("chat", ArtifactRefKindStatusWire::Historical),
            ("bug", ArtifactRefKindStatusWire::Historical),
        ] {
            let descriptor = catalog
                .iter()
                .find(|descriptor| descriptor.kind == kind)
                .unwrap_or_else(|| panic!("missing {kind}"));
            assert!(!descriptor.offered_in_completion, "{kind}");
            assert_eq!(descriptor.status, status, "{kind}");
        }
    }

    #[test]
    fn commit_canonicalizes_to_stitch_without_a_diagnostic() {
        let resolved = canonical_artifact_ref_kind("commit");
        assert!(resolved.alias);
        assert_eq!(resolved.canonical, "stitch");
        assert_eq!(resolved.status, ArtifactRefKindStatusWire::Alias);
        assert!(resolved.diagnostic.is_none());
    }

    #[test]
    fn plans_canonicalizes_to_plan_with_a_diagnostic() {
        let resolved = canonical_artifact_ref_kind("plans");
        assert!(resolved.alias);
        assert_eq!(resolved.canonical, "plan");
        assert_eq!(resolved.status, ArtifactRefKindStatusWire::Deprecated);
        assert_eq!(
            resolved.diagnostic.as_deref(),
            Some("@plans: is deprecated; write @plan:<path>")
        );
    }

    #[test]
    fn unregistered_labels_canonicalize_to_themselves() {
        let resolved = canonical_artifact_ref_kind("designs");
        assert!(!resolved.alias);
        assert_eq!(resolved.canonical, "designs");
        assert_eq!(resolved.status, ArtifactRefKindStatusWire::Live);
    }

    #[test]
    fn canonical_parse_rewrites_commit_and_plans_but_nothing_else() {
        let commit = parse_artifact_ref_canonical(
            "commit:sase@0123456789abcdef0123456789abcdef01234567",
        )
        .unwrap();
        assert_eq!(
            commit.reference.rendered,
            "stitch:sase@0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(commit.alias.as_deref(), Some("commit"));
        assert!(commit.diagnostic.is_none());

        let plans = parse_artifact_ref_canonical("plans:202608/x.md").unwrap();
        assert_eq!(plans.reference.rendered, "plan:202608/x.md");
        assert_eq!(plans.alias.as_deref(), Some("plans"));
        assert_eq!(
            plans.diagnostic.as_deref(),
            Some("@plans: is deprecated; write @plan:<path>")
        );

        let bead = parse_artifact_ref_canonical("bead:sase-9z").unwrap();
        assert_eq!(bead.reference.rendered, "bead:sase-9z");
        assert!(bead.alias.is_none());
        assert!(bead.diagnostic.is_none());
    }

    /// Regression net for the whole backward-compatibility argument: the
    /// default (non-canonicalizing) `parse_artifact_ref` must stay
    /// byte-identical for every historical kind, aliases included.
    #[test]
    fn default_parse_is_byte_identical_for_every_historical_kind() {
        for value in [
            "plans:202607/plan.md",
            "designs:guide.md#L12",
            "chat:202607/main.md#L12-L18",
            "commit:sase@0123456789abcdef0123456789abcdef01234567",
            "bug:sase#123",
            "file:default:52895d68931185056fd0e49f",
            "bead:sase-9z",
            "agent:bbugyi200.athena.9w",
        ] {
            assert_eq!(
                parse_artifact_ref(value).unwrap().rendered,
                value,
                "{value}"
            );
        }
    }
}
