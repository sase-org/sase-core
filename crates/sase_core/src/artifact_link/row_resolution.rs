//! Shared artifact-link ref to frontend row-identity resolution.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::artifact_ref::canonical_artifact_ref_kind;

pub const ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION: u64 = 1;

/// One selectable frontend row identity: owning pane plus ordered parts.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ArtifactRowIdentityWire {
    pub pane_id: String,
    pub parts: Vec<String>,
}

/// A link-graph ref split into its canonical kind and payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkRefPartsWire {
    pub schema_version: u64,
    pub kind: String,
    pub payload: String,
}

/// Everything a resolution needs besides the candidate rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRowRefQueryWire {
    pub schema_version: u64,
    pub kind: String,
    pub payload: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_hint: Option<String>,
    /// Exact-first compatibility spellings for an `agent:` payload; empty
    /// means "match the payload verbatim".
    #[serde(default)]
    pub agent_name_candidates: Vec<String>,
}

/// Split a link-graph ref string into canonical kind and opaque payload.
pub fn parse_artifact_link_ref_parts(
    value: &str,
) -> Option<ArtifactLinkRefPartsWire> {
    let trimmed = value.trim();
    let without_sigil = trimmed.strip_prefix('@').unwrap_or(trimmed);
    let fragment_free = without_sigil
        .split_once('#')
        .map_or(without_sigil, |(prefix, _fragment)| prefix)
        .trim();
    let (kind, payload) = fragment_free.split_once(':')?;
    if kind.is_empty() || payload.is_empty() {
        return None;
    }
    let canonical = canonical_artifact_ref_kind(kind);
    Some(ArtifactLinkRefPartsWire {
        schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
        kind: canonical.canonical,
        payload: payload.to_string(),
    })
}

/// Return every lookup key one rendered row identity answers to.
pub fn artifact_row_index_keys(
    identity: &ArtifactRowIdentityWire,
) -> Vec<Vec<String>> {
    let Some(last) = identity.parts.last() else {
        return Vec::new();
    };

    let mut keys = Vec::new();
    let mut exact = Vec::with_capacity(identity.parts.len() + 2);
    exact.push("exact".to_string());
    exact.push(identity.pane_id.clone());
    exact.extend(identity.parts.iter().cloned());
    keys.push(exact);

    match identity.pane_id.as_str() {
        "files" => {
            keys.push(vec!["files.id".to_string(), identity.parts[0].clone()]);
        }
        "stitches"
            if identity.parts.len() >= 2 && !identity.parts[1].is_empty() =>
        {
            let repo = identity.parts[0].clone();
            let mut prefix = String::new();
            for ch in identity.parts[1].chars() {
                prefix.push(ch);
                keys.push(vec![
                    "stitches.sha".to_string(),
                    repo.clone(),
                    prefix.clone(),
                ]);
            }
        }
        "agents" => {
            keys.push(vec!["agents.name".to_string(), last.clone()]);
        }
        "beads" => {
            keys.push(vec!["beads.id".to_string(), last.clone()]);
            if identity.parts.len() >= 2 {
                keys.push(vec![
                    "beads.project.id".to_string(),
                    identity.parts[0].clone(),
                    last.clone(),
                ]);
            }
        }
        "patches" => {
            keys.push(vec!["patches.name".to_string(), last.clone()]);
            if identity.parts.len() >= 2 {
                keys.push(vec![
                    "patches.project.name".to_string(),
                    identity.parts[0].clone(),
                    last.clone(),
                ]);
            }
        }
        pane_id if pane_id.starts_with("ref:") => {
            keys.push(vec![format!("{pane_id}.id"), last.clone()]);
            if identity.parts.len() >= 2 {
                keys.push(vec![
                    format!("{pane_id}.project.id"),
                    identity.parts[0].clone(),
                    last.clone(),
                ]);
            }
        }
        _ => {}
    }
    keys
}

/// Return ordered lookup keys one artifact-link ref should probe.
pub fn artifact_row_ref_lookup_keys(
    query: &ArtifactRowRefQueryWire,
) -> Vec<Vec<String>> {
    if query.payload.is_empty() {
        return Vec::new();
    }
    let kind = canonical_artifact_ref_kind(&query.kind).canonical;
    let payload = query.payload.as_str();
    let project_hint = query
        .project_hint
        .as_deref()
        .filter(|hint| !hint.is_empty());
    let mut keys = Vec::new();

    match kind.as_str() {
        "file" => {
            keys.push(vec![
                "exact".to_string(),
                "files".to_string(),
                payload.to_string(),
            ]);
            keys.push(vec!["files.id".to_string(), payload.to_string()]);
        }
        "agent" => {
            keys.push(vec![
                "exact".to_string(),
                "agents".to_string(),
                payload.to_string(),
            ]);
            if query.agent_name_candidates.is_empty() {
                keys.push(vec!["agents.name".to_string(), payload.to_string()]);
            } else {
                for candidate in &query.agent_name_candidates {
                    keys.push(vec![
                        "agents.name".to_string(),
                        candidate.to_string(),
                    ]);
                }
            }
        }
        "stitch" => {
            if let Some((repo, sha)) = payload.split_once('@') {
                if !repo.is_empty() && !sha.is_empty() {
                    keys.push(vec![
                        "stitches.sha".to_string(),
                        repo.to_string(),
                        sha.to_string(),
                    ]);
                }
            }
        }
        "patch" => {
            if let Some(hint) = project_hint {
                keys.push(vec![
                    "patches.project.name".to_string(),
                    hint.to_string(),
                    payload.to_string(),
                ]);
            }
            keys.push(vec!["patches.name".to_string(), payload.to_string()]);
        }
        "bead" => {
            if let Some(hint) = project_hint {
                keys.push(vec![
                    "beads.project.id".to_string(),
                    hint.to_string(),
                    payload.to_string(),
                ]);
            }
            keys.push(vec!["beads.id".to_string(), payload.to_string()]);
        }
        "bug" | "chat" | "chop" => {}
        other => {
            let pane_id = format!("ref:{other}");
            if let Some(hint) = project_hint {
                keys.push(vec![
                    format!("{pane_id}.project.id"),
                    hint.to_string(),
                    payload.to_string(),
                ]);
            }
            keys.push(vec![format!("{pane_id}.id"), payload.to_string()]);
        }
    }
    keys
}

/// Resolve one artifact-link ref query against candidate frontend row identities.
pub fn resolve_artifact_row_identity(
    query: &ArtifactRowRefQueryWire,
    candidates: &[ArtifactRowIdentityWire],
) -> Option<ArtifactRowIdentityWire> {
    let mut ordered = candidates.to_vec();
    ordered.sort();

    let mut by_key: BTreeMap<Vec<String>, ArtifactRowIdentityWire> =
        BTreeMap::new();
    for candidate in ordered {
        for key in artifact_row_index_keys(&candidate) {
            by_key.entry(key).or_insert_with(|| candidate.clone());
        }
    }

    for key in artifact_row_ref_lookup_keys(query) {
        if let Some(identity) = by_key.get(&key) {
            return Some(identity.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(pane_id: &str, parts: &[&str]) -> ArtifactRowIdentityWire {
        ArtifactRowIdentityWire {
            pane_id: pane_id.to_string(),
            parts: parts.iter().map(|part| (*part).to_string()).collect(),
        }
    }

    fn query(
        kind: &str,
        payload: &str,
        project_hint: Option<&str>,
    ) -> ArtifactRowRefQueryWire {
        ArtifactRowRefQueryWire {
            schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
            kind: kind.to_string(),
            payload: payload.to_string(),
            project_hint: project_hint.map(str::to_string),
            agent_name_candidates: Vec::new(),
        }
    }

    fn query_with_agent_candidates(
        payload: &str,
        candidates: &[&str],
    ) -> ArtifactRowRefQueryWire {
        ArtifactRowRefQueryWire {
            schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
            kind: "agent".to_string(),
            payload: payload.to_string(),
            project_hint: None,
            agent_name_candidates: candidates
                .iter()
                .map(|candidate| (*candidate).to_string())
                .collect(),
        }
    }

    #[test]
    fn parse_link_ref_parts_strips_aliases_sigil_and_fragment() {
        assert_eq!(
            parse_artifact_link_ref_parts("bead:sase-1"),
            Some(ArtifactLinkRefPartsWire {
                schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
                kind: "bead".to_string(),
                payload: "sase-1".to_string(),
            })
        );
        assert_eq!(
            parse_artifact_link_ref_parts("commit:sase@abc123")
                .unwrap()
                .kind,
            "stitch"
        );
        assert_eq!(
            parse_artifact_link_ref_parts("plans:a.md").unwrap().kind,
            "plan"
        );
        assert_eq!(
            parse_artifact_link_ref_parts("@plan:202608/a.md#why").unwrap(),
            ArtifactLinkRefPartsWire {
                schema_version: ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
                kind: "plan".to_string(),
                payload: "202608/a.md".to_string(),
            }
        );
        assert!(parse_artifact_link_ref_parts("not-a-ref").is_none());
        assert!(parse_artifact_link_ref_parts("kind:").is_none());
    }

    #[test]
    fn resolves_class_a_row_identity_shapes() {
        let candidates = vec![
            id("beads", &["alpha", "epic", "alpha-1"]),
            id("beads", &["alpha", "phase", "alpha-1.1"]),
            id("beads", &["alpha", "flag", "alpha-flag"]),
            id("ref:plan", &["alpha", "active", "202609/design.md"]),
            id("ref:plan", &["alpha", "proposal", "notify-1"]),
            id(
                "stitches",
                &["sase", "0123456789abcdef0123456789abcdef01234567"],
            ),
            id("patches", &["alpha", "same"]),
            id("patches", &["beta", "same"]),
            id("agents", &["worker"]),
            id("files", &["logical", "version-1"]),
        ];

        assert_eq!(
            resolve_artifact_row_identity(
                &query("bead", "alpha-1", Some("alpha")),
                &candidates,
            ),
            Some(id("beads", &["alpha", "epic", "alpha-1"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("bead", "alpha-1.1", Some("alpha")),
                &candidates,
            ),
            Some(id("beads", &["alpha", "phase", "alpha-1.1"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("bead", "alpha-flag", Some("alpha")),
                &candidates,
            ),
            Some(id("beads", &["alpha", "flag", "alpha-flag"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("plan", "202609/design.md", Some("alpha")),
                &candidates,
            ),
            Some(id("ref:plan", &["alpha", "active", "202609/design.md"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("plan", "notify-1", Some("alpha")),
                &candidates,
            ),
            Some(id("ref:plan", &["alpha", "proposal", "notify-1"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("stitch", "sase@012345", None),
                &candidates,
            ),
            Some(id(
                "stitches",
                &["sase", "0123456789abcdef0123456789abcdef01234567"],
            ))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("patch", "same", Some("beta")),
                &candidates,
            ),
            Some(id("patches", &["beta", "same"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query_with_agent_candidates(
                    "athena.worker",
                    &["athena.worker", "worker"]
                ),
                &candidates,
            ),
            Some(id("agents", &["worker"]))
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("file", "logical", None),
                &candidates,
            ),
            Some(id("files", &["logical", "version-1"]))
        );
    }

    #[test]
    fn project_hint_precedes_deterministic_fallback() {
        let beta = id("patches", &["beta", "same"]);
        let alpha = id("patches", &["alpha", "same"]);
        let candidates = vec![beta.clone(), alpha.clone()];

        assert_eq!(
            resolve_artifact_row_identity(
                &query("patch", "same", Some("beta")),
                &candidates,
            ),
            Some(beta.clone())
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("patch", "same", None),
                &candidates
            ),
            Some(alpha.clone())
        );
        assert_eq!(
            resolve_artifact_row_identity(
                &query("patch", "same", None),
                &[alpha.clone(), beta.clone()],
            ),
            Some(alpha)
        );
    }

    #[test]
    fn resolver_matches_key_probe_equivalence() {
        let candidates = vec![
            id("files", &["doc", "v1"]),
            id("beads", &["alpha", "phase", "sase-1.1"]),
            id("ref:plan", &["alpha", "archive", "design.md"]),
        ];
        let query = query("plan", "design.md", Some("alpha"));

        let mut ordered = candidates.clone();
        ordered.sort();
        let mut by_key = BTreeMap::new();
        for candidate in ordered {
            for key in artifact_row_index_keys(&candidate) {
                by_key.entry(key).or_insert_with(|| candidate.clone());
            }
        }
        let probed = artifact_row_ref_lookup_keys(&query)
            .into_iter()
            .find_map(|key| by_key.get(&key).cloned());

        assert_eq!(resolve_artifact_row_identity(&query, &candidates), probed);
    }

    #[test]
    fn guards_do_not_resolve_virtual_or_malformed_refs() {
        let candidates = vec![
            id("files", &["doc"]),
            id("stitches", &["sase", "abcdef"]),
            id("patches", &[]),
        ];

        assert!(artifact_row_index_keys(&id("patches", &[])).is_empty());
        assert!(resolve_artifact_row_identity(
            &query("file", "", None),
            &candidates
        )
        .is_none());
        assert!(resolve_artifact_row_identity(
            &query("stitch", "sase@", None),
            &candidates,
        )
        .is_none());
        for kind in ["bug", "chat", "chop"] {
            assert!(resolve_artifact_row_identity(
                &query(kind, "anything", None),
                &candidates,
            )
            .is_none());
        }
    }
}
