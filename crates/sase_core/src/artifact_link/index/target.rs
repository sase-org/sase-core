//! Target synthesis and permissive ref canonicalization for the read index.

use crate::artifact_link::row_resolution::{
    parse_artifact_link_ref_parts, ArtifactLinkRefPartsWire,
    ArtifactRowIdentityWire,
};
use crate::artifact_link::wire::canonicalize_artifact_link_ref;

/// Split a link-graph ref the way the Python index does, mapping `job` to
/// `chop` after the shared kind catalog rewrite.
pub(crate) fn parse_index_ref(value: &str) -> Option<ArtifactLinkRefPartsWire> {
    let mut parsed = parse_artifact_link_ref_parts(value)?;
    if parsed.kind == "job" {
        parsed.kind = "chop".to_string();
    }
    Some(parsed)
}

/// Canonicalize a ref, keeping the original spelling when the strict
/// parser rejects historical or malformed aggregate rows.
pub(crate) fn canonicalize_index_ref(value: &str) -> String {
    match canonicalize_artifact_link_ref(value) {
        Ok(canonical) => canonical,
        Err(_) => value.to_string(),
    }
}

/// Synthesize a frontend row identity with no known-target lookup.
///
/// Preserves `target_for_ref_kind`: project hints, document/archive
/// identities, malformed stitch refs, unknown document kinds, and the
/// non-selectable bug/chat/job kinds.
pub(crate) fn target_for_ref_kind(
    kind: &str,
    payload: &str,
    project_hint: Option<&str>,
) -> Option<ArtifactRowIdentityWire> {
    let project = project_hint.unwrap_or("").to_string();
    match kind {
        "stitch" => {
            let (repo, sha) = payload.split_once('@')?;
            if repo.is_empty() || sha.is_empty() {
                return None;
            }
            Some(ArtifactRowIdentityWire {
                pane_id: "stitches".to_string(),
                parts: vec![repo.to_string(), sha.to_string()],
            })
        }
        "patch" => Some(ArtifactRowIdentityWire {
            pane_id: "patches".to_string(),
            parts: vec![project, payload.to_string()],
        }),
        "bead" => Some(ArtifactRowIdentityWire {
            pane_id: "beads".to_string(),
            parts: vec![project, "task".to_string(), payload.to_string()],
        }),
        "file" => Some(ArtifactRowIdentityWire {
            pane_id: "files".to_string(),
            parts: vec![payload.to_string()],
        }),
        "agent" => Some(ArtifactRowIdentityWire {
            pane_id: "agents".to_string(),
            parts: vec![payload.to_string()],
        }),
        "bug" | "chat" | "chop" | "job" => None,
        other => Some(ArtifactRowIdentityWire {
            pane_id: format!("ref:{other}"),
            parts: vec![project, "archive".to_string(), payload.to_string()],
        }),
    }
}

pub(crate) fn public_neighbor_ref(value: &str) -> String {
    match parse_index_ref(value) {
        Some(parsed) if parsed.kind == "chop" => {
            format!("job:{}", parsed.payload)
        }
        _ => value.to_string(),
    }
}

pub(crate) fn neighbor_kind_of(value: &str) -> String {
    parse_index_ref(value)
        .map(|parsed| parsed.kind)
        .unwrap_or_default()
}
