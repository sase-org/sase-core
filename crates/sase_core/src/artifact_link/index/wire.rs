//! Versioned request/response wires for the artifact-link read index.

use serde::{Deserialize, Serialize};

use crate::artifact_link::row_resolution::ArtifactRowIdentityWire;

pub const ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION: u64 = 1;

const SEMANTIC_RELATIONS: &[&str] =
    &["related", "supersedes", "implements", "derives-from"];

/// Owner/alias context captured at compile time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactLinkIndexIdentityWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_name: Option<String>,
    #[serde(default)]
    pub sibling_machines: Vec<String>,
    #[serde(default)]
    pub known_owner_roots: Vec<String>,
}

/// Extra relation metadata the caller assembled for fallback lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexRelationMetaWire {
    pub slug: String,
    pub inverse: String,
    pub directed: bool,
}

/// Permissive bulk compile request. Rows use the aggregate read model,
/// not the strict mutation-only [`super::super::wire::ArtifactLinkRowWire`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexBuildRequestWire {
    pub schema_version: u64,
    #[serde(default)]
    pub rows: Vec<serde_json::Value>,
    #[serde(default)]
    pub identity: ArtifactLinkIndexIdentityWire,
    #[serde(default)]
    pub relations: Vec<ArtifactLinkIndexRelationMetaWire>,
}

/// Native chip record. Presentation (accent/icon) stays in Python.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexChipWire {
    pub relation: String,
    pub label: String,
    pub directed: bool,
    pub this_is_source: bool,
    pub neighbor_ref: String,
    pub neighbor_kind: String,
    pub neighbor_target: Option<ArtifactRowIdentityWire>,
    pub why: String,
    pub origin: String,
    pub uses: u64,
    pub created_by: String,
    pub created_at: String,
    pub writable: bool,
}

/// Membership plus first-indexed target for one ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexTargetWire {
    pub present: bool,
    pub target: Option<ArtifactRowIdentityWire>,
}

/// One raw aggregate row returned by an indexed candidate query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexRawRowWire {
    pub source_ref: String,
    pub target_ref: String,
    pub relation: String,
    pub description: String,
    pub origin: String,
    pub created_by: String,
    pub created_at: String,
    pub uses: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Rows query: lookup keys from [`crate::artifact_link::row_resolution`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArtifactLinkIndexRowsRequestWire {
    pub schema_version: u64,
    #[serde(default)]
    pub lookup_keys: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexRowsResponseWire {
    pub rows: Vec<ArtifactLinkIndexRawRowWire>,
    pub candidate_row_count: u64,
}

/// Pane projection request. Python supplies known identities and
/// permitted relation names; it decorates kind/label after return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexProjectRequestWire {
    pub schema_version: u64,
    #[serde(default)]
    pub known_targets: Vec<ArtifactRowIdentityWire>,
    #[serde(default)]
    pub permitted_relations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexProjectedEdgeWire {
    pub relation: String,
    pub source: ArtifactRowIdentityWire,
    pub target: ArtifactRowIdentityWire,
    pub description: String,
    pub origin: String,
    pub uses: u64,
    pub source_known: bool,
    pub target_known: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexProjectResponseWire {
    pub edges: Vec<ArtifactLinkIndexProjectedEdgeWire>,
    pub candidate_row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkIndexStatsWire {
    pub raw_row_count: u64,
    pub indexed_row_count: u64,
    pub canonical_ref_count: u64,
    pub chip_ref_count: u64,
}

pub(crate) fn is_semantic_relation(relation: &str) -> bool {
    SEMANTIC_RELATIONS.contains(&relation)
}

pub(crate) fn default_known_owner_roots(
    identity: &ArtifactLinkIndexIdentityWire,
) -> Vec<String> {
    if !identity.known_owner_roots.is_empty() {
        return identity.known_owner_roots.clone();
    }
    let Some(username) = identity.username.as_deref() else {
        return Vec::new();
    };
    let Some(machine) = identity.machine_name.as_deref() else {
        return Vec::new();
    };
    let mut roots = identity.sibling_machines.clone();
    roots.push(machine.to_string());
    roots.push(format!("{username}.{machine}"));
    let mut seen = std::collections::BTreeSet::new();
    roots
        .into_iter()
        .filter(|root| !root.is_empty() && seen.insert(root.clone()))
        .collect()
}
