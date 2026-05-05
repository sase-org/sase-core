//! Derived artifact ingestion framework and shared path helpers.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use serde_json::{Map, Value};

use super::store::{
    deterministic_artifact_link_id, upsert_artifact_link, upsert_artifact_node,
    upsert_artifact_payload, ArtifactStore,
};
use super::wire::{
    ArtifactLinkUpsertWire, ArtifactLinkWire, ArtifactMutationResultWire,
    ArtifactNodeUpsertWire, ArtifactNodeWire, ArtifactPathUpsertRequestWire,
    ArtifactPayloadWire, ArtifactRebuildRequestWire, ARTIFACT_KIND_DIRECTORY,
    ARTIFACT_KIND_FILE, ARTIFACT_KIND_ROOT, ARTIFACT_LINK_PARENT,
    ARTIFACT_PROVENANCE_DERIVED, ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID,
    ARTIFACT_STALE_CLEANUP_MARK, ARTIFACT_STALE_CLEANUP_NONE,
    ARTIFACT_WIRE_SCHEMA_VERSION,
};

pub const ARTIFACT_SOURCE_PROJECT_FILE: &str = "project_file";
pub const ARTIFACT_SOURCE_CHANGESPEC: &str = "changespec";
pub const ARTIFACT_SOURCE_COMMIT: &str = "commit";
pub const ARTIFACT_SOURCE_DIRECTORY: &str = "directory";
pub const ARTIFACT_SOURCE_BEAD_STORE: &str = "bead_store";
pub const ARTIFACT_SOURCE_AGENT_ARTIFACT: &str = "agent_artifact";
pub const ARTIFACT_SOURCE_AGENT_CREATED_FILE: &str = "agent_created_file";
pub const ARTIFACT_SOURCE_AGENT_THOUGHT: &str = "agent_thought";

#[derive(Debug, Clone)]
pub struct ResolvedArtifactRebuildRequest {
    pub projects_root: Option<PathBuf>,
    pub workspace_root: Option<PathBuf>,
    pub beads_dir: Option<PathBuf>,
    pub include_sources: BTreeSet<String>,
    pub exclude_sources: BTreeSet<String>,
    pub target_path: Option<PathBuf>,
    pub artifact_dir: Option<PathBuf>,
    pub stale_cleanup: String,
}

#[derive(Debug, Clone, Default)]
pub struct ArtifactIngestMutations {
    result: ArtifactMutationResultWire,
}

impl ArtifactIngestMutations {
    pub fn new(operation: &str) -> Self {
        Self {
            result: ArtifactMutationResultWire {
                operation: operation.to_string(),
                ..ArtifactMutationResultWire::default()
            },
        }
    }

    pub fn merge(&mut self, result: ArtifactMutationResultWire) {
        merge_mutation_results(&mut self.result, result);
    }

    pub fn error(&mut self, message: impl Into<String>) {
        self.result.errors.push(message.into());
    }

    pub fn into_result(self) -> ArtifactMutationResultWire {
        self.result
    }
}

pub fn artifact_rebuild(
    store: &mut ArtifactStore,
    request: ArtifactRebuildRequestWire,
) -> Result<ArtifactMutationResultWire, String> {
    let resolved = resolve_rebuild_request(request)?;
    let mut mutations = ArtifactIngestMutations::new("rebuild");

    if !source_selected(&resolved, ARTIFACT_SOURCE_DIRECTORY) {
        return Ok(mutations.into_result());
    }

    for maybe_path in [
        resolved.projects_root.as_deref(),
        resolved.workspace_root.as_deref(),
        resolved.beads_dir.as_deref(),
        resolved.target_path.as_deref(),
        resolved.artifact_dir.as_deref(),
    ] {
        if let Some(path) = maybe_path {
            let request = ArtifactPathUpsertRequestWire {
                kind: Some(ARTIFACT_KIND_DIRECTORY.to_string()),
                provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
                source_kind: Some(ARTIFACT_SOURCE_DIRECTORY.to_string()),
                source_id: Some(path_to_artifact_id(path)),
                ..ArtifactPathUpsertRequestWire::default()
            };
            mutations.merge(artifact_upsert_path(store, path, request)?);
        }
    }

    Ok(mutations.into_result())
}

pub fn artifact_upsert_path(
    store: &mut ArtifactStore,
    artifact_path: &Path,
    request: ArtifactPathUpsertRequestWire,
) -> Result<ArtifactMutationResultWire, String> {
    validate_schema_version(request.schema_version)?;

    let normalized = normalize_artifact_path(artifact_path)?;
    let mut mutations = ArtifactIngestMutations::new("upsert_path");

    let mut known_dirs = ancestor_directories(&normalized);
    if path_is_directory(&normalized) {
        known_dirs.push(normalized.clone());
    }
    known_dirs.sort();
    known_dirs.dedup();

    for dir in &known_dirs {
        let node = directory_node_for_path(dir, &request)?;
        let upsert = upsert_artifact_node(
            store,
            ArtifactNodeUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                node: node.clone(),
                replace_payloads: false,
            },
        )?;
        let node_was_visible = mutation_touched_node(&upsert, &node.id);
        mutations.merge(upsert);
        if node.id != ARTIFACT_ROOT_ID && node_was_visible {
            let parent_id = directory_parent_id(dir, &known_dirs);
            mutations.merge(upsert_parent_link(
                store, &node.id, &parent_id, &request,
            )?);
        }
    }

    if !path_is_directory(&normalized) {
        let node = file_node_for_path(&normalized, &request)?;
        let upsert = upsert_artifact_node(
            store,
            ArtifactNodeUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                node: node.clone(),
                replace_payloads: false,
            },
        )?;
        let node_was_visible = mutation_touched_node(&upsert, &node.id);
        mutations.merge(upsert);
        if node.id != ARTIFACT_ROOT_ID && node_was_visible {
            let parent_id =
                select_directory_parent_id(&normalized, &known_dirs);
            mutations.merge(upsert_parent_link(
                store, &node.id, &parent_id, &request,
            )?);
        }
    }

    Ok(mutations.into_result())
}

pub fn normalize_artifact_path(path: &Path) -> Result<PathBuf, String> {
    if path.as_os_str().is_empty() {
        return Err("artifact_path must not be empty".to_string());
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(path)
    };
    if let Ok(canonical) = std::fs::canonicalize(&absolute) {
        return Ok(canonical);
    }

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
            Component::RootDir | Component::Prefix(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    Ok(normalized)
}

pub fn file_node_for_path(
    path: &Path,
    request: &ArtifactPathUpsertRequestWire,
) -> Result<ArtifactNodeWire, String> {
    node_for_path(path, ARTIFACT_KIND_FILE, request)
}

pub fn directory_node_for_path(
    path: &Path,
    request: &ArtifactPathUpsertRequestWire,
) -> Result<ArtifactNodeWire, String> {
    let inferred_kind = if path_to_artifact_id(path) == ARTIFACT_ROOT_ID {
        ARTIFACT_KIND_ROOT
    } else {
        ARTIFACT_KIND_DIRECTORY
    };
    node_for_path(path, inferred_kind, request)
}

pub fn select_directory_parent_id(
    path: &Path,
    known_dirs: &[PathBuf],
) -> String {
    let normalized =
        normalize_artifact_path(path).unwrap_or_else(|_| path.to_path_buf());
    known_dirs
        .iter()
        .filter_map(|dir| normalize_artifact_path(dir).ok())
        .filter(|dir| dir != &normalized && normalized.starts_with(dir))
        .max_by_key(|dir| dir.components().count())
        .map(|dir| path_to_artifact_id(&dir))
        .unwrap_or_else(|| ARTIFACT_ROOT_ID.to_string())
}

pub fn derived_payload(
    artifact_id: impl Into<String>,
    payload_type: impl Into<String>,
    source_kind: impl Into<String>,
    source_id: impl Into<String>,
    source_version: Option<String>,
    payload: Value,
) -> ArtifactPayloadWire {
    ArtifactPayloadWire {
        artifact_id: artifact_id.into(),
        payload_type: payload_type.into(),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(source_kind.into()),
        source_id: Some(source_id.into()),
        source_version,
        payload,
        updated_at: None,
    }
}

pub fn upsert_derived_payload(
    store: &mut ArtifactStore,
    payload: ArtifactPayloadWire,
) -> Result<ArtifactMutationResultWire, String> {
    upsert_artifact_payload(store, payload)
}

pub fn merge_mutation_results(
    target: &mut ArtifactMutationResultWire,
    source: ArtifactMutationResultWire,
) {
    target.nodes_added += source.nodes_added;
    target.nodes_updated += source.nodes_updated;
    target.nodes_removed += source.nodes_removed;
    target.links_added += source.links_added;
    target.links_updated += source.links_updated;
    target.links_removed += source.links_removed;
    target.tombstones_added += source.tombstones_added;
    append_unique(&mut target.affected_node_ids, source.affected_node_ids);
    append_unique(&mut target.affected_link_ids, source.affected_link_ids);
    append_unique(&mut target.tombstone_ids, source.tombstone_ids);
    target.errors.extend(source.errors);
}

pub fn path_to_artifact_id(path: &Path) -> String {
    let value = path.to_string_lossy();
    if value.is_empty() {
        ARTIFACT_ROOT_ID.to_string()
    } else {
        value.into_owned()
    }
}

fn resolve_rebuild_request(
    request: ArtifactRebuildRequestWire,
) -> Result<ResolvedArtifactRebuildRequest, String> {
    validate_schema_version(request.schema_version)?;
    validate_stale_cleanup(&request.stale_cleanup)?;

    Ok(ResolvedArtifactRebuildRequest {
        projects_root: normalize_optional_path(
            request.projects_root,
            default_projects_root(),
        )?,
        workspace_root: normalize_optional_path(
            request.workspace_root,
            std::env::current_dir().ok(),
        )?,
        beads_dir: normalize_optional_path(
            request.beads_dir,
            std::env::current_dir()
                .ok()
                .map(|cwd| cwd.join("sdd/beads")),
        )?,
        include_sources: request.include_sources.into_iter().collect(),
        exclude_sources: request.exclude_sources.into_iter().collect(),
        target_path: normalize_optional_path(request.target_path, None)?,
        artifact_dir: normalize_optional_path(request.artifact_dir, None)?,
        stale_cleanup: request.stale_cleanup,
    })
}

fn normalize_optional_path(
    value: Option<String>,
    default: Option<PathBuf>,
) -> Result<Option<PathBuf>, String> {
    match value {
        Some(path) => normalize_artifact_path(Path::new(&path)).map(Some),
        None => match default {
            Some(path) => normalize_artifact_path(&path).map(Some),
            None => Ok(None),
        },
    }
}

fn source_selected(
    request: &ResolvedArtifactRebuildRequest,
    source_kind: &str,
) -> bool {
    (request.include_sources.is_empty()
        || request.include_sources.contains(source_kind))
        && !request.exclude_sources.contains(source_kind)
}

fn validate_schema_version(schema_version: u32) -> Result<(), String> {
    if schema_version != ARTIFACT_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported artifact schema_version {schema_version}; expected {ARTIFACT_WIRE_SCHEMA_VERSION}"
        ));
    }
    Ok(())
}

fn validate_stale_cleanup(value: &str) -> Result<(), String> {
    if value == ARTIFACT_STALE_CLEANUP_NONE
        || value == ARTIFACT_STALE_CLEANUP_MARK
    {
        Ok(())
    } else {
        Err(format!(
            "unsupported artifact stale_cleanup {value:?}; expected none or mark"
        ))
    }
}

fn default_projects_root() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(|home| PathBuf::from(home).join(".sase/projects"))
}

fn node_for_path(
    path: &Path,
    inferred_kind: &str,
    request: &ArtifactPathUpsertRequestWire,
) -> Result<ArtifactNodeWire, String> {
    let normalized = normalize_artifact_path(path)?;
    let id = path_to_artifact_id(&normalized);
    let display_title = display_title_for_path(&normalized, &id);
    let subtitle = normalized
        .parent()
        .map(path_to_artifact_id)
        .filter(|parent| parent != &id);
    let search_text = format!("{display_title} {id}");

    Ok(ArtifactNodeWire {
        id,
        kind: request
            .kind
            .clone()
            .unwrap_or_else(|| inferred_kind.to_string()),
        display_title: request.display_title.clone().unwrap_or(display_title),
        subtitle: request.subtitle.clone().or(subtitle),
        provenance: request
            .provenance
            .clone()
            .unwrap_or_else(|| ARTIFACT_PROVENANCE_MANUAL.to_string()),
        source_kind: request.source_kind.clone(),
        source_id: request.source_id.clone(),
        source_version: request.source_version.clone(),
        search_text: request.search_text.clone().unwrap_or(search_text),
        metadata: request.metadata.clone().unwrap_or_else(Map::new),
        created_at: None,
        updated_at: None,
    })
}

fn upsert_parent_link(
    store: &mut ArtifactStore,
    source_id: &str,
    target_id: &str,
    request: &ArtifactPathUpsertRequestWire,
) -> Result<ArtifactMutationResultWire, String> {
    let link = ArtifactLinkWire {
        id: deterministic_artifact_link_id(
            ARTIFACT_LINK_PARENT,
            source_id,
            target_id,
        ),
        link_type: ARTIFACT_LINK_PARENT.to_string(),
        source_id: source_id.to_string(),
        target_id: target_id.to_string(),
        provenance: request
            .provenance
            .clone()
            .unwrap_or_else(|| ARTIFACT_PROVENANCE_MANUAL.to_string()),
        source_kind: request.source_kind.clone(),
        source_id_hint: request.source_id.clone(),
        source_version: request.source_version.clone(),
        metadata: Map::new(),
        created_at: None,
        updated_at: None,
    };
    upsert_artifact_link(
        store,
        ArtifactLinkUpsertWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            link,
        },
    )
}

fn path_is_directory(path: &Path) -> bool {
    if path_to_artifact_id(path) == ARTIFACT_ROOT_ID {
        return true;
    }
    std::fs::metadata(path)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

fn ancestor_directories(path: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut current = if path_is_directory(path) {
        Some(path)
    } else {
        path.parent()
    };
    while let Some(dir) = current {
        dirs.push(dir.to_path_buf());
        current = dir.parent().filter(|parent| *parent != dir);
    }
    dirs.reverse();
    dirs
}

fn directory_parent_id(dir: &Path, known_dirs: &[PathBuf]) -> String {
    select_directory_parent_id(dir, known_dirs)
}

fn mutation_touched_node(
    result: &ArtifactMutationResultWire,
    node_id: &str,
) -> bool {
    result
        .affected_node_ids
        .iter()
        .any(|affected| affected == node_id)
}

fn display_title_for_path(path: &Path, id: &str) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| id.to_string())
}

fn append_unique(target: &mut Vec<String>, source: Vec<String>) {
    for value in source {
        if !target.contains(&value) {
            target.push(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::super::query::artifact_show;
    use super::super::store::{open_artifact_store, remove_artifact_node};
    use super::super::wire::{
        ArtifactNodeRemoveWire, ARTIFACT_PROVENANCE_DERIVED,
        ARTIFACT_TOMBSTONE_NODE,
    };
    use super::*;

    #[test]
    fn path_upsert_creates_file_directories_and_parent_links_to_root() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let file = tmp.path().join("a/b/note.md");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "hello").unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result = artifact_upsert_path(
            &mut store,
            &file,
            ArtifactPathUpsertRequestWire::default(),
        )
        .unwrap();

        assert!(result.nodes_added >= 4, "{result:?}");
        assert!(result.links_added >= 4, "{result:?}");

        let detail = artifact_show(&store, file.to_str().unwrap()).unwrap();
        let path_ids: Vec<_> = detail
            .path_to_root
            .into_iter()
            .map(|node| node.id)
            .collect();
        assert_eq!(
            &path_ids[..4],
            vec![
                file.to_string_lossy().into_owned(),
                file.parent().unwrap().to_string_lossy().into_owned(),
                tmp.path().join("a").to_string_lossy().into_owned(),
                tmp.path().to_string_lossy().into_owned(),
            ]
        );
        assert_eq!(path_ids.last().unwrap(), ARTIFACT_ROOT_ID);
    }

    #[test]
    fn directory_parent_selection_prefers_longest_known_parent() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let shallow = root.join("project");
        let deep = shallow.join("src");
        let file = deep.join("lib.rs");
        let known = vec![root.to_path_buf(), shallow, deep.clone()];

        assert_eq!(
            select_directory_parent_id(&file, &known),
            deep.to_string_lossy().into_owned()
        );
        assert_eq!(
            select_directory_parent_id(&root.join("other/file.txt"), &known),
            root.to_string_lossy().into_owned()
        );
        assert_eq!(
            select_directory_parent_id(
                Path::new("/definitely-outside"),
                &known
            ),
            ARTIFACT_ROOT_ID
        );
    }

    #[test]
    fn rebuild_request_defaults_and_source_filters_are_stable() {
        let tmp = tempdir().unwrap();
        let request = ArtifactRebuildRequestWire {
            projects_root: Some(
                tmp.path().join("projects").to_string_lossy().into_owned(),
            ),
            workspace_root: Some(
                tmp.path().join("workspace").to_string_lossy().into_owned(),
            ),
            beads_dir: Some(
                tmp.path()
                    .join("workspace/sdd/beads")
                    .to_string_lossy()
                    .into_owned(),
            ),
            include_sources: vec![ARTIFACT_SOURCE_DIRECTORY.to_string()],
            exclude_sources: vec![ARTIFACT_SOURCE_BEAD_STORE.to_string()],
            target_path: Some(
                tmp.path().join("target").to_string_lossy().into_owned(),
            ),
            artifact_dir: None,
            stale_cleanup: ARTIFACT_STALE_CLEANUP_NONE.to_string(),
            ..ArtifactRebuildRequestWire::default()
        };

        let resolved = resolve_rebuild_request(request).unwrap();
        assert!(resolved
            .projects_root
            .as_ref()
            .is_some_and(|path| path.ends_with("projects")));
        assert!(source_selected(&resolved, ARTIFACT_SOURCE_DIRECTORY));
        assert!(!source_selected(&resolved, ARTIFACT_SOURCE_BEAD_STORE));
    }

    #[test]
    fn tombstoned_derived_path_node_is_not_resurrected_by_path_upsert() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let file = tmp.path().join("note.md");
        std::fs::write(&file, "hello").unwrap();
        let request = ArtifactPathUpsertRequestWire {
            provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
            source_kind: Some(ARTIFACT_SOURCE_DIRECTORY.to_string()),
            source_id: Some(file.to_string_lossy().into_owned()),
            ..ArtifactPathUpsertRequestWire::default()
        };

        let mut store = open_artifact_store(&db).unwrap();
        artifact_upsert_path(&mut store, &file, request.clone()).unwrap();
        remove_artifact_node(
            &mut store,
            ArtifactNodeRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: file.to_string_lossy().into_owned(),
                provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
                source_kind: Some(ARTIFACT_SOURCE_DIRECTORY.to_string()),
                source_id: Some(file.to_string_lossy().into_owned()),
                reason: Some("hidden".to_string()),
            },
        )
        .unwrap();

        let result = artifact_upsert_path(&mut store, &file, request).unwrap();
        assert!(!result
            .affected_node_ids
            .contains(&file.to_string_lossy().into_owned()));
        let detail = artifact_show(&store, file.to_str().unwrap()).unwrap();
        assert!(detail.node.is_none());

        let tombstone_count: u64 = store
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM manual_tombstones WHERE tombstone_type = ?1",
                [ARTIFACT_TOMBSTONE_NODE],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tombstone_count, 1);
    }
}
