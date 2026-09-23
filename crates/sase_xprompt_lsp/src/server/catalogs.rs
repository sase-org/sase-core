use super::state::{
    ArtifactRefCatalog, ArtifactRefCatalogProject, ArtifactRefCatalogSignature,
    GlossaryCatalog, GlossaryCatalogProject, GlossaryCatalogProjectPayload,
    GlossaryCatalogSignature, ServerConfig, VcsProjectCatalog,
};
use super::*;
use sase_core::editor::{
    VcsProjectCatalogWire, VCS_PROJECT_CATALOG_SCHEMA_VERSION,
};

pub(super) fn file_history() -> Vec<String> {
    let Some(home) = std::env::var_os("HOME") else {
        return Vec::new();
    };
    let path = PathBuf::from(home)
        .join(".sase")
        .join("file_reference_history.json");
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return Vec::new();
    };
    value
        .get("paths")
        .and_then(|paths| paths.as_array())
        .into_iter()
        .flatten()
        .filter_map(|path| path.as_str())
        .filter(|path| !path.starts_with(".sase/"))
        .map(str::to_string)
        .collect()
}

/// Load the enabled project/PR completion catalog from the materialized JSON
/// file at `path`.
///
/// Read fresh on every `+` completion request. Any failure (no path, unreadable
/// file, malformed JSON) degrades to empty results so the `+` menu simply
/// shows nothing rather than breaking completion. Schema versions 1 through
/// `VCS_PROJECT_CATALOG_SCHEMA_VERSION` are accepted; v1 entries default to
/// project rows, v1/v2 catalogs default
/// `namespaces` to empty, and pre-v5 catalogs default `accent_palette`,
/// `project_tags`, and the per-entry v5 fields to empty. Parsing goes
/// through [`VcsProjectCatalogWire`] so the wire shape stays the single
/// source of truth. The v5 file shape is `{ "schema_version": 5,
/// "workflow_names": [..], "entries": [VcsProjectEntry, ..], "namespaces":
/// {"gh": [VcsNamespaceEntry, ..]}, "accent_palette": [..], "project_tags":
/// [ProjectTagTargetWire, ..] }`.
pub(super) fn load_vcs_project_catalog(
    path: Option<&Path>,
) -> VcsProjectCatalog {
    let Some(path) = path else {
        return VcsProjectCatalog::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return VcsProjectCatalog::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse vcs project catalog at {path:?}");
        return VcsProjectCatalog::default();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    let max_schema_version = u64::from(VCS_PROJECT_CATALOG_SCHEMA_VERSION);
    if !(1..=max_schema_version).contains(&schema_version) {
        warn!(
            "unsupported vcs project catalog schema_version {schema_version} at {path:?}"
        );
        return VcsProjectCatalog::default();
    }
    // Parsing goes through the wire shape; a catalog with one malformed
    // section still degrades per-field (e.g. bad `namespaces` keeps its
    // entries) instead of dropping everything.
    if let Ok(wire) =
        serde_json::from_value::<VcsProjectCatalogWire>(value.clone())
    {
        return VcsProjectCatalog {
            entries: wire.entries,
            workflow_names: wire.workflow_names,
            namespaces: wire.namespaces,
            accent_palette: wire.accent_palette,
            project_tags: wire.project_tags,
        };
    }
    warn!("vcs project catalog at {path:?} has malformed sections; loading per-field");
    let entries = value
        .get("entries")
        .cloned()
        .and_then(|entries| {
            serde_json::from_value::<Vec<VcsProjectEntry>>(entries).ok()
        })
        .unwrap_or_default();
    let workflow_names = value
        .get("workflow_names")
        .and_then(serde_json::Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(|name| name.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let namespaces = value
        .get("namespaces")
        .cloned()
        .and_then(|namespaces| {
            serde_json::from_value::<HashMap<String, Vec<VcsNamespaceEntry>>>(
                namespaces,
            )
            .ok()
        })
        .unwrap_or_default();
    let accent_palette = value
        .get("accent_palette")
        .and_then(serde_json::Value::as_array)
        .map(|palette| {
            palette
                .iter()
                .filter_map(|color| color.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let project_tags = value
        .get("project_tags")
        .cloned()
        .and_then(|targets| {
            serde_json::from_value::<Vec<ProjectTagTargetWire>>(targets).ok()
        })
        .unwrap_or_default();
    VcsProjectCatalog {
        entries,
        workflow_names,
        namespaces,
        accent_palette,
        project_tags,
    }
}

pub(super) fn artifact_ref_catalog_signature(
    path: Option<&Path>,
) -> ArtifactRefCatalogSignature {
    let metadata = path.and_then(|path| fs::metadata(path).ok());
    ArtifactRefCatalogSignature {
        path: path.map(Path::to_path_buf),
        modified: metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok()),
        len: metadata.as_ref().map_or(0, fs::Metadata::len),
    }
}

/// Load the launcher-generated local artifact-reference catalog.
///
/// The schema is version-gated and every failure degrades to no artifact
/// assistance. [`XpromptLspServer::artifact_ref_catalog`] caches this parsed
/// value together with the payload inventories and invalidates it by file
/// signature, TTL, or explicit refresh.
pub(super) fn load_artifact_ref_catalog(
    path: Option<&Path>,
) -> ArtifactRefCatalog {
    let Some(path) = path else {
        return ArtifactRefCatalog::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return ArtifactRefCatalog::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse artifact-reference catalog at {path:?}");
        return ArtifactRefCatalog::default();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    if schema_version != Some(1) {
        warn!(
            "unsupported artifact-reference catalog schema_version {:?} at {path:?}",
            schema_version
        );
        return ArtifactRefCatalog::default();
    }
    ArtifactRefCatalog {
        default_project: value
            .get("default_project")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        projects: value
            .get("projects")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|project| {
                serde_json::from_value::<ArtifactRefCatalogProject>(
                    project.clone(),
                )
                .ok()
            })
            .filter(|project| {
                !project.name.is_empty() && !project.key.is_empty()
            })
            .collect(),
    }
}

pub(super) fn glossary_catalog_signature(
    path: Option<&Path>,
) -> GlossaryCatalogSignature {
    let metadata = path.and_then(|path| fs::metadata(path).ok());
    GlossaryCatalogSignature {
        path: path.map(Path::to_path_buf),
        modified: metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok()),
        len: metadata.as_ref().map_or(0, fs::Metadata::len),
    }
}

/// Load and compile the launcher-generated project glossary catalog.
///
/// The schema is version-gated and every failure degrades to no glossary
/// semantics. [`XpromptLspServer::glossary_catalog`] caches this parsed value
/// and invalidates it by file signature, TTL, explicit refresh, or watched
/// config changes.
pub(super) fn load_glossary_catalog(path: Option<&Path>) -> GlossaryCatalog {
    let Some(path) = path else {
        return GlossaryCatalog::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return GlossaryCatalog::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse glossary catalog at {path:?}");
        return GlossaryCatalog::default();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    if schema_version != Some(1) {
        warn!(
            "unsupported glossary catalog schema_version {:?} at {path:?}",
            schema_version
        );
        return GlossaryCatalog::default();
    }
    GlossaryCatalog {
        default_project: value
            .get("default_project")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        projects: value
            .get("projects")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|project| glossary_catalog_project(project.clone()))
            .collect(),
    }
}

pub(super) fn glossary_catalog_project(
    value: serde_json::Value,
) -> Option<GlossaryCatalogProject> {
    let payload =
        serde_json::from_value::<GlossaryCatalogProjectPayload>(value).ok()?;
    if payload.schema_version != 1
        || payload.project.key.is_empty()
        || payload.project.name.is_empty()
        || payload.entries.is_empty()
    {
        return None;
    }
    let catalog = CompiledGlossaryCatalog::new(GlossaryCatalogWire {
        schema_version: payload.schema_version,
        entries: payload.entries,
    })
    .ok()?;
    if catalog.is_empty() {
        return None;
    }
    Some(GlossaryCatalogProject {
        key: payload.project.key,
        name: payload.project.name,
        aliases: payload.project.aliases,
        config_path: payload.config_path,
        catalog: Arc::new(catalog),
    })
}

pub(super) fn known_at_reference_kinds(
    context: Option<&ArtifactRefContextWire>,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    sase_core::editor::at_reference::BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .copied()
        .chain(
            context
                .into_iter()
                .flat_map(|context| context.document_roots.iter())
                .map(|root| root.kind.as_str()),
        )
        .filter(|kind| !kind.is_empty())
        .filter(|kind| seen.insert((*kind).to_string()))
        .map(str::to_string)
        .collect()
}

pub(super) fn at_reference_kind_inventory(
    context: Option<&ArtifactRefContextWire>,
) -> Vec<AtReferenceKindRowWire> {
    known_at_reference_kinds(context)
        .into_iter()
        .map(|kind| {
            let builtin =
                sase_core::editor::at_reference::is_builtin_at_reference_kind(
                    &kind,
                );
            let detail = if builtin {
                "builtin artifact kind".to_string()
            } else {
                context
                    .into_iter()
                    .flat_map(|context| context.document_roots.iter())
                    .find(|root| root.kind == kind)
                    .map(|root| format!("document artifact · {}", root.root))
                    .unwrap_or_else(|| "document artifact".to_string())
            };
            AtReferenceKindRowWire {
                kind,
                builtin,
                detail,
            }
        })
        .collect()
}

pub(super) fn at_reference_path_inventory(
    context: &AtReferenceContextWire,
    config: &ServerConfig,
) -> Vec<AtReferencePathRowWire> {
    if context.stage != AtReferenceStage::Kind {
        return Vec::new();
    }
    let Some(path_query) = context.path_query.as_ref() else {
        return Vec::new();
    };
    let Some(directory) = resolve_at_reference_directory(
        config.root_dir.as_deref(),
        &path_query.directory,
    ) else {
        return Vec::new();
    };
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .take(1_000)
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            Some(AtReferencePathRowWire {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: file_type.is_dir(),
            })
        })
        .collect()
}

pub(super) fn resolve_at_reference_directory(
    root_dir: Option<&Path>,
    directory: &str,
) -> Option<PathBuf> {
    let expanded = if directory == "~/" {
        std::env::var_os("HOME").map(PathBuf::from)?
    } else if let Some(rest) = directory.strip_prefix("~/") {
        std::env::var_os("HOME").map(PathBuf::from)?.join(rest)
    } else {
        PathBuf::from(directory)
    };
    let resolved = if expanded.is_absolute() {
        expanded
    } else {
        root_dir?.join(expanded)
    };
    resolved.canonicalize().ok()
}

pub(super) fn active_artifact_ref_project<'a>(
    document: &DocumentSnapshot,
    config: &ServerConfig,
    vcs_catalog: &VcsProjectCatalog,
    artifact_catalog: &'a ArtifactRefCatalog,
) -> Option<&'a ArtifactRefCatalogProject> {
    let leading_project = leading_vcs_project(
        document.text(),
        &vcs_catalog.entries,
        &vcs_catalog.project_tags,
    );
    leading_project
        .as_deref()
        .and_then(|project| artifact_ref_project(artifact_catalog, project))
        .or_else(|| {
            artifact_catalog
                .default_project
                .as_deref()
                .and_then(|project| {
                    artifact_ref_project(artifact_catalog, project)
                })
        })
        .or_else(|| {
            config.project.as_deref().and_then(|project| {
                artifact_ref_project(artifact_catalog, project).or_else(|| {
                    initialized_project_basename(project).and_then(|basename| {
                        artifact_ref_project(artifact_catalog, basename)
                    })
                })
            })
        })
}

pub(super) fn active_artifact_ref_context<'a>(
    document: &DocumentSnapshot,
    config: &ServerConfig,
    vcs_catalog: &VcsProjectCatalog,
    artifact_catalog: &'a ArtifactRefCatalog,
) -> Option<&'a ArtifactRefContextWire> {
    active_artifact_ref_project(document, config, vcs_catalog, artifact_catalog)
        .map(|project| &project.context)
}

pub(super) fn active_glossary_project<'a>(
    document: &DocumentSnapshot,
    config: &ServerConfig,
    vcs_catalog: &VcsProjectCatalog,
    glossary_catalog: &'a GlossaryCatalog,
) -> Option<&'a GlossaryCatalogProject> {
    let leading_project = leading_vcs_project(
        document.text(),
        &vcs_catalog.entries,
        &vcs_catalog.project_tags,
    );
    leading_project
        .as_deref()
        .and_then(|project| glossary_project(glossary_catalog, project))
        .or_else(|| {
            glossary_catalog
                .default_project
                .as_deref()
                .and_then(|project| glossary_project(glossary_catalog, project))
        })
        .or_else(|| {
            config.project.as_deref().and_then(|project| {
                glossary_project(glossary_catalog, project).or_else(|| {
                    initialized_project_basename(project).and_then(|basename| {
                        glossary_project(glossary_catalog, basename)
                    })
                })
            })
        })
}

pub(super) fn leading_vcs_project(
    text: &str,
    entries: &[VcsProjectEntry],
    targets: &[ProjectTagTargetWire],
) -> Option<String> {
    let token = text.split_ascii_whitespace().next()?;
    if let Some(identity) =
        crate::project_tags::leading_tag_identity(text, targets, entries)
    {
        return Some(identity);
    }
    if !token.starts_with('#') {
        return None;
    }
    entries.iter().find_map(|entry| {
        let canonical = token == entry.display_tag;
        let alias = token
            .strip_prefix(&format!("#{}:", entry.vcs_prefix))
            .is_some_and(|value| {
                value.eq_ignore_ascii_case(&entry.name)
                    || entry
                        .aliases
                        .iter()
                        .any(|alias| alias.eq_ignore_ascii_case(value))
            });
        if !canonical && !alias {
            return None;
        }
        if !entry.project.is_empty() {
            Some(entry.project.clone())
        } else {
            Some(entry.name.clone())
        }
    })
}

pub(super) fn artifact_ref_project<'a>(
    catalog: &'a ArtifactRefCatalog,
    identity: &str,
) -> Option<&'a ArtifactRefCatalogProject> {
    catalog.projects.iter().find(|project| {
        project.name.eq_ignore_ascii_case(identity)
            || project.key.eq_ignore_ascii_case(identity)
            || project
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(identity))
    })
}

pub(super) fn glossary_project<'a>(
    catalog: &'a GlossaryCatalog,
    identity: &str,
) -> Option<&'a GlossaryCatalogProject> {
    catalog.projects.iter().find(|project| {
        project.name.eq_ignore_ascii_case(identity)
            || project.key.eq_ignore_ascii_case(identity)
            || project
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(identity))
    })
}

pub(super) fn initialized_project_basename(project: &str) -> Option<&str> {
    let (basename, suffix) = project.rsplit_once('_')?;
    (!basename.is_empty()
        && !suffix.is_empty()
        && suffix.bytes().all(|byte| byte.is_ascii_digit()))
    .then_some(basename)
}

/// Load the `%model` completion catalog from the materialized JSON file.
///
/// Read fresh on every `%model` completion request. Any failure (no path,
/// unreadable file, malformed JSON) degrades to empty results.
pub(super) fn load_model_catalog(
    path: Option<&Path>,
) -> Vec<ModelCompletionEntryWire> {
    let Some(path) = path else {
        return Vec::new();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse model catalog at {path:?}");
        return Vec::new();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    if schema_version != 1 {
        warn!(
            "unsupported model catalog schema_version {schema_version} at {path:?}"
        );
        return Vec::new();
    }
    value
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let entry: ModelCompletionEntryWire =
                        serde_json::from_value(entry.clone()).ok()?;
                    (!entry.value.is_empty()).then_some(entry)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Load the `%dispatch` machine completion catalog from materialized JSON.
pub(super) fn load_machine_catalog(
    path: Option<&Path>,
) -> Vec<DirectiveMachineEntry> {
    let Some(path) = path else {
        return Vec::new();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse machine catalog at {path:?}");
        return Vec::new();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    if schema_version != 1 {
        warn!(
            "unsupported machine catalog schema_version {schema_version} at {path:?}"
        );
        return Vec::new();
    }
    value
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let entry: DirectiveMachineEntry =
                        serde_json::from_value(entry.clone()).ok()?;
                    (!entry.alias.is_empty()).then_some(entry)
                })
                .collect()
        })
        .unwrap_or_default()
}
