//! `artifact_md_path` and companion naming, including collision refusal.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::agent_identity::{
    agent_link_target, parse_agent_family_name, AgentOwnerIdentity,
};
use crate::artifact_ref::{
    parse_artifact_ref_canonical, ArtifactRefAgentOwnerWire,
    ArtifactRefBeadStoreWire, ArtifactRefContextWire, ArtifactRefKindWire,
    ArtifactRefPayloadWire,
};

use super::wire::ArtifactLinkError;

pub const ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION: u64 = 1;

const UNPUBLISHED_PAGES_DIR: &str = ".sase/artifacts/pages";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactMdPathKindWire {
    Document,
    Generated,
    Companion,
    Unpublished,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMdPathRequestWire {
    pub schema_version: u64,
    pub reference: String,
    #[serde(default)]
    pub resolved_path: Option<String>,
    /// When set, overrides inference of unpublished `file:` digest objects.
    #[serde(default)]
    pub published: Option<bool>,
    #[serde(default)]
    pub context: ArtifactRefContextWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMdPathWire {
    pub schema_version: u64,
    pub reference: String,
    pub kind: ArtifactMdPathKindWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub collision: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactCompanionPathWire {
    pub schema_version: u64,
    pub path: String,
    pub collision: bool,
}

/// Lexical lineage root of a bead id (`sase-ag.1` → `sase-ag`).
pub fn bead_lineage_root(id: &str) -> &str {
    crate::artifact_ref::bead_lineage_root(id)
}

/// Beads-sidecar-relative page path (`pages/<root>/README.md` or `<id>.md`).
pub fn bead_page_relpath(id: &str) -> PathBuf {
    crate::artifact_ref::bead_page_path(id)
}

/// Companion markdown path for a published binary.
///
/// `diagram.png` → `diagram.md`. When that path already exists, refuse and
/// use `diagram.png.md` instead so a research report is never overwritten.
pub fn companion_md_path(
    asset_path: &str,
) -> Result<ArtifactCompanionPathWire, ArtifactLinkError> {
    let path = Path::new(asset_path);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ArtifactLinkError::validation(
                "companion asset path has no file name",
            )
        })?;
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem =
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| {
                ArtifactLinkError::validation(
                    "companion asset path has no stem",
                )
            })?;
    let preferred = parent.join(format!("{stem}.md"));
    if preferred.exists() && preferred != path {
        let disambiguated = parent.join(format!("{file_name}.md"));
        return Ok(ArtifactCompanionPathWire {
            schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
            path: path_to_string(&disambiguated),
            collision: true,
        });
    }
    Ok(ArtifactCompanionPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        path: path_to_string(&preferred),
        collision: false,
    })
}

/// Resolve the artifact markdown file for one canonical (or raw) ref.
pub fn artifact_md_path(
    request: &ArtifactMdPathRequestWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    if request.schema_version != ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "unsupported artifact_md_path schema_version {}; expected {}",
            request.schema_version, ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION
        )));
    }
    let canonical = super::canonicalize_artifact_link_ref(&request.reference)?;
    let parsed = parse_artifact_ref_canonical(&canonical)?.reference;
    match (&parsed.kind, &parsed.payload) {
        (ArtifactRefKindWire::Stitch, _)
        | (ArtifactRefKindWire::Commit, _) => Ok(none_path(
            canonical,
            "stitch references have no artifact markdown file; links render on the peer",
        )),
        (ArtifactRefKindWire::Bead, ArtifactRefPayloadWire::Bead { id }) => {
            bead_md_path(canonical, id, &request.context)
        }
        (ArtifactRefKindWire::Agent, ArtifactRefPayloadWire::Agent { name }) => {
            agent_md_path(canonical, name, &request.context)
        }
        (ArtifactRefKindWire::Patch, ArtifactRefPayloadWire::Patch { name }) => {
            patch_md_path(canonical, name, request)
        }
        (
            ArtifactRefKindWire::File,
            ArtifactRefPayloadWire::File { digest, .. },
        ) => file_digest_md_path(canonical, digest, request),
        (
            ArtifactRefKindWire::File,
            ArtifactRefPayloadWire::FilePath { path },
        ) => file_path_md_path(canonical, path, request),
        (
            ArtifactRefKindWire::Document { .. },
            ArtifactRefPayloadWire::Document { path },
        ) => document_md_path(canonical, path, request),
        (ArtifactRefKindWire::Chat, ArtifactRefPayloadWire::Chat { path }) => {
            chat_md_path(canonical, path, request)
        }
        _ => {
            if let Some(resolved) = request.resolved_path.as_deref() {
                if is_markdown_path(Path::new(resolved)) {
                    return Ok(document_result(canonical, resolved));
                }
                return companion_result(canonical, resolved);
            }
            Err(ArtifactLinkError::validation(format!(
                "cannot determine artifact markdown file for {canonical}"
            )))
        }
    }
}

fn bead_md_path(
    reference: String,
    id: &str,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    let relative = bead_page_relpath(id);
    let store = select_bead_store(id, &context.bead_stores);
    let path = match store {
        Some(store) => Path::new(&store.root).join(&relative),
        None => relative,
    };
    Ok(ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::Generated,
        path: Some(path_to_string(&path)),
        collision: false,
        reason: None,
    })
}

fn select_bead_store<'a>(
    id: &str,
    stores: &'a [ArtifactRefBeadStoreWire],
) -> Option<&'a ArtifactRefBeadStoreWire> {
    if stores.is_empty() {
        return None;
    }
    let prefix = bead_lineage_root(id)
        .rsplit_once('-')
        .map(|(prefix, _)| prefix);
    let matched: Vec<&ArtifactRefBeadStoreWire> = stores
        .iter()
        .filter(|store| match prefix {
            Some(prefix) => store.prefix == prefix,
            None => true,
        })
        .collect();
    match matched.as_slice() {
        [store] => Some(*store),
        [] => stores.first(),
        many => many
            .iter()
            .copied()
            .find(|store| {
                Path::new(&store.root).join(bead_page_relpath(id)).is_file()
            })
            .or_else(|| many.first().copied()),
    }
}

fn agent_md_path(
    reference: String,
    name: &str,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    let relative = agent_page_relpath(name, context.agent_owner.as_ref())?;
    let path = if let Some(root) = context.agent_roots.first() {
        Path::new(&root.root).join(&relative)
    } else {
        relative
    };
    Ok(ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::Generated,
        path: Some(path_to_string(&path)),
        collision: false,
        reason: None,
    })
}

fn agent_page_relpath(
    name: &str,
    owner: Option<&ArtifactRefAgentOwnerWire>,
) -> Result<PathBuf, ArtifactLinkError> {
    if let Some(owner) = owner {
        let identity =
            AgentOwnerIdentity::new(&owner.username, &owner.machine_name)
                .map_err(|error| {
                    ArtifactLinkError::validation(format!(
                        "invalid agent owner: {error}"
                    ))
                })?;
        let target = agent_link_target(name, &identity).map_err(|error| {
            ArtifactLinkError::validation(format!(
                "invalid agent name: {error}"
            ))
        })?;
        return Ok(PathBuf::from(target.path));
    }
    let parsed = parse_agent_family_name(name).map_err(|error| {
        ArtifactLinkError::validation(format!("invalid agent name: {error}"))
    })?;
    if parsed.member_role.is_some() {
        Ok(PathBuf::from(format!("families/{}.md", parsed.family_name)))
    } else {
        Ok(PathBuf::from("agents")
            .join(&parsed.family_name)
            .join("README.md"))
    }
}

fn patch_md_path(
    reference: String,
    name: &str,
    request: &ArtifactMdPathRequestWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    if let Some(resolved) = request.resolved_path.as_deref() {
        return Ok(document_result(reference, resolved));
    }
    Ok(ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::Document,
        path: Some(format!("{name}.md")),
        collision: false,
        reason: None,
    })
}

fn file_digest_md_path(
    reference: String,
    digest: &str,
    request: &ArtifactMdPathRequestWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    if request.published == Some(true) {
        if let Some(resolved) = request.resolved_path.as_deref() {
            if is_markdown_path(Path::new(resolved)) {
                return Ok(document_result(reference, resolved));
            }
            return companion_result(reference, resolved);
        }
    }
    let file_name = format!("{digest}.md");
    let path = match &request.context.home_dir {
        Some(home) if !home.is_empty() => {
            Path::new(home).join(UNPUBLISHED_PAGES_DIR).join(file_name)
        }
        _ => Path::new(UNPUBLISHED_PAGES_DIR).join(file_name),
    };
    Ok(ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::Unpublished,
        path: Some(path_to_string(&path)),
        collision: false,
        reason: Some(
            "unpublished file artifacts use a local page, never a sibling in the object store"
                .to_string(),
        ),
    })
}

fn file_path_md_path(
    reference: String,
    path: &str,
    request: &ArtifactMdPathRequestWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    let resolved = request.resolved_path.as_deref().unwrap_or(path);
    if is_markdown_path(Path::new(resolved)) {
        return Ok(document_result(reference, resolved));
    }
    companion_result(reference, resolved)
}

fn document_md_path(
    reference: String,
    path: &str,
    request: &ArtifactMdPathRequestWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    if let Some(resolved) = request.resolved_path.as_deref() {
        return Ok(document_result(reference, resolved));
    }
    let kind_label = parse_artifact_ref_canonical(&reference)?
        .reference
        .kind
        .label()
        .to_string();
    let rooted = request
        .context
        .document_roots
        .iter()
        .find(|root| root.kind == kind_label)
        .map(|root| Path::new(&root.root).join(path));
    let path = rooted.unwrap_or_else(|| PathBuf::from(path));
    Ok(document_result(reference, &path_to_string(&path)))
}

fn chat_md_path(
    reference: String,
    path: &str,
    request: &ArtifactMdPathRequestWire,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    if let Some(resolved) = request.resolved_path.as_deref() {
        return Ok(document_result(reference, resolved));
    }
    let path = match &request.context.chats_root {
        Some(root) => Path::new(root).join(path),
        None => PathBuf::from(path),
    };
    Ok(document_result(reference, &path_to_string(&path)))
}

fn document_result(reference: String, path: &str) -> ArtifactMdPathWire {
    ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::Document,
        path: Some(path.to_string()),
        collision: false,
        reason: None,
    }
}

fn companion_result(
    reference: String,
    asset_path: &str,
) -> Result<ArtifactMdPathWire, ArtifactLinkError> {
    let companion = companion_md_path(asset_path)?;
    Ok(ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::Companion,
        path: Some(companion.path),
        collision: companion.collision,
        reason: companion.collision.then(|| {
            "companion stem collides with an existing document; using the disambiguated name"
                .to_string()
        }),
    })
}

fn none_path(reference: String, reason: &str) -> ArtifactMdPathWire {
    ArtifactMdPathWire {
        schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
        reference,
        kind: ArtifactMdPathKindWire::None,
        path: None,
        collision: false,
        reason: Some(reason.to_string()),
    }
}

fn is_markdown_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("md"))
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_ref::{
        ArtifactRefBeadStoreWire, ArtifactRefContextWire,
        ArtifactRefDocumentRootWire, ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION,
    };
    use tempfile::tempdir;

    fn request(reference: &str) -> ArtifactMdPathRequestWire {
        ArtifactMdPathRequestWire {
            schema_version: ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
            reference: reference.to_string(),
            resolved_path: None,
            published: None,
            context: ArtifactRefContextWire {
                schema_version: ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION,
                ..ArtifactRefContextWire::default()
            },
        }
    }

    #[test]
    fn stitch_has_no_markdown_file() {
        let result = artifact_md_path(&request(
            "stitch:sase@0123456789abcdef0123456789abcdef01234567",
        ))
        .unwrap();
        assert_eq!(result.kind, ArtifactMdPathKindWire::None);
        assert!(result.path.is_none());
        assert!(result.reason.as_deref().unwrap().contains("peer"));
    }

    #[test]
    fn bead_page_uses_readme_for_lineage_root() {
        assert_eq!(bead_lineage_root("sase-js.4"), "sase-js");
        assert_eq!(
            bead_page_relpath("sase-js").to_string_lossy(),
            "pages/sase-js/README.md"
        );
        assert_eq!(
            bead_page_relpath("sase-js.4").to_string_lossy(),
            "pages/sase-js/sase-js.4.md"
        );
        let mut request = request("bead:sase-js.4");
        request.context.bead_stores = vec![ArtifactRefBeadStoreWire {
            project: "sase".to_string(),
            prefix: "sase".to_string(),
            root: "/beads".to_string(),
        }];
        let result = artifact_md_path(&request).unwrap();
        assert_eq!(result.kind, ArtifactMdPathKindWire::Generated);
        assert_eq!(
            result.path.as_deref(),
            Some("/beads/pages/sase-js/sase-js.4.md")
        );
    }

    #[test]
    fn document_kind_is_itself() {
        let mut request = request("plan:202608/report.md");
        request.context.document_roots = vec![ArtifactRefDocumentRootWire {
            kind: "plan".to_string(),
            root: "/plans".to_string(),
            path_globs: None,
        }];
        let result = artifact_md_path(&request).unwrap();
        assert_eq!(result.kind, ArtifactMdPathKindWire::Document);
        assert_eq!(result.path.as_deref(), Some("/plans/202608/report.md"));
    }

    #[test]
    fn unpublished_file_uses_local_pages_dir() {
        let mut request = request("file:default:0123456789abcdef01234567");
        request.context.home_dir = Some("/home/bryan".to_string());
        let result = artifact_md_path(&request).unwrap();
        assert_eq!(result.kind, ArtifactMdPathKindWire::Unpublished);
        assert_eq!(
            result.path.as_deref(),
            Some(
                "/home/bryan/.sase/artifacts/pages/0123456789abcdef01234567.md"
            )
        );
    }

    #[test]
    fn companion_uses_stem_and_disambiguates_on_collision() {
        let temp = tempdir().unwrap();
        let png = temp.path().join("diagram.png");
        std::fs::write(&png, b"png").unwrap();
        let companion = companion_md_path(&png.to_string_lossy()).unwrap();
        assert!(!companion.collision);
        assert!(companion.path.ends_with("diagram.md"));

        std::fs::write(temp.path().join("diagram.md"), "# report\n").unwrap();
        let collided = companion_md_path(&png.to_string_lossy()).unwrap();
        assert!(collided.collision);
        assert!(collided.path.ends_with("diagram.png.md"));
    }

    #[test]
    fn published_markdown_file_is_itself() {
        let mut request = request("file:notes/guide.md");
        request.resolved_path = Some("/repo/notes/guide.md".to_string());
        let result = artifact_md_path(&request).unwrap();
        assert_eq!(result.kind, ArtifactMdPathKindWire::Document);
        assert_eq!(result.path.as_deref(), Some("/repo/notes/guide.md"));
    }
}
