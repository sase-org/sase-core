//! Derived artifact ingestion framework and shared path helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::agent_scan::{
    scan_agent_artifact_dir, scan_agent_artifacts, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, DoneMarkerWire, PromptStepMarkerWire,
};
use crate::bead::{read_store_issues, IssueWire};
use crate::parse_project_bytes;
use crate::query::get_searchable_text;
use crate::wire::{ChangeSpecWire, CommitWire};

use super::store::{
    deterministic_artifact_link_id, upsert_artifact_link, upsert_artifact_node,
    upsert_artifact_payload, ArtifactStore,
};
use super::wire::{
    ArtifactLinkUpsertWire, ArtifactLinkWire, ArtifactMutationResultWire,
    ArtifactNodeUpsertWire, ArtifactNodeWire, ArtifactPathUpsertRequestWire,
    ArtifactPayloadWire, ArtifactRebuildRequestWire, ARTIFACT_KIND_AGENT,
    ARTIFACT_KIND_BEAD, ARTIFACT_KIND_CHANGESPEC, ARTIFACT_KIND_COMMIT,
    ARTIFACT_KIND_DIRECTORY, ARTIFACT_KIND_FILE, ARTIFACT_KIND_PROJECT,
    ARTIFACT_KIND_ROOT, ARTIFACT_KIND_THOUGHT, ARTIFACT_LINK_CREATED,
    ARTIFACT_LINK_PARENT, ARTIFACT_LINK_RELATED, ARTIFACT_LINK_WORKER,
    ARTIFACT_PROVENANCE_DERIVED, ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID,
    ARTIFACT_STALE_CLEANUP_MARK, ARTIFACT_STALE_CLEANUP_NONE,
    ARTIFACT_TOMBSTONE_LINK, ARTIFACT_TOMBSTONE_NODE,
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

const AGENT_PAYLOAD_TYPE: &str = "agent";
const AGENT_CREATED_FILE_PAYLOAD_TYPE: &str = "agent_created_file";
const AGENT_THOUGHT_PAYLOAD_TYPE: &str = "agent_thought";
const STALE_DERIVED_REASON: &str = "stale_derived";

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
    let cleanup_plan = cleanup_plan_for_request(store, &resolved)?;
    let mut mutations = ArtifactIngestMutations::new("rebuild");

    if let Some(target_path) = resolved.target_path.as_deref() {
        mutations.merge(artifact_rebuild_target_path(
            store,
            &resolved,
            target_path,
        )?);
    } else if source_selected(&resolved, ARTIFACT_SOURCE_DIRECTORY) {
        for path in [
            resolved.projects_root.as_deref(),
            resolved.workspace_root.as_deref(),
            resolved.beads_dir.as_deref(),
            resolved.artifact_dir.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
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

    if resolved.target_path.is_none() && project_ingestion_selected(&resolved) {
        mutations.merge(ingest_project_sources(store, &resolved)?);
    }

    if resolved.target_path.is_none() && agent_ingestion_selected(&resolved) {
        mutations.merge(ingest_agent_artifacts(store, &resolved)?);
    }

    if resolved.target_path.is_none()
        && source_selected(&resolved, ARTIFACT_SOURCE_BEAD_STORE)
    {
        mutations.merge(ingest_beads(store, &resolved)?);
    }

    mutations.merge(reconcile_cross_source_links(store)?);

    let active_sources = discover_active_sources(&resolved)?;
    mutations.merge(update_source_watermarks(store, &active_sources)?);
    if resolved.stale_cleanup == ARTIFACT_STALE_CLEANUP_MARK {
        mutations.merge(mark_stale_sources(
            store,
            &cleanup_plan,
            &active_sources,
        )?);
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

fn artifact_rebuild_target_path(
    store: &mut ArtifactStore,
    request: &ResolvedArtifactRebuildRequest,
    target_path: &Path,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("rebuild_target_path");
    let normalized = normalize_artifact_path(target_path)?;

    if source_selected(request, ARTIFACT_SOURCE_DIRECTORY)
        && normalized.exists()
    {
        mutations.merge(artifact_upsert_path(
            store,
            &normalized,
            ArtifactPathUpsertRequestWire {
                provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
                source_kind: Some(ARTIFACT_SOURCE_DIRECTORY.to_string()),
                source_id: Some(path_to_artifact_id(&normalized)),
                ..ArtifactPathUpsertRequestWire::default()
            },
        )?);
    }

    if project_ingestion_selected(request) && is_project_file(&normalized) {
        if normalized.exists() {
            let mut seen_changespec_sources = BTreeMap::new();
            mutations.merge(upsert_project_file(
                store,
                &normalized,
                request,
                &mut seen_changespec_sources,
            )?);
        }
        return Ok(mutations.into_result());
    }

    if source_selected(request, ARTIFACT_SOURCE_BEAD_STORE)
        && target_matches_bead_store(request, &normalized)
    {
        mutations.merge(ingest_beads(store, request)?);
    }

    Ok(mutations.into_result())
}

#[derive(Debug, Clone)]
struct CleanupPlan {
    scoped_sources: BTreeMap<String, BTreeSet<String>>,
}

fn cleanup_plan_for_request(
    store: &ArtifactStore,
    request: &ResolvedArtifactRebuildRequest,
) -> Result<CleanupPlan, String> {
    let mut scoped_sources = BTreeMap::new();
    for source_kind in selected_stale_source_kinds(request) {
        scoped_sources.insert(
            source_kind.to_string(),
            cleanup_scope_source_ids(store, request, source_kind)?,
        );
    }
    Ok(CleanupPlan { scoped_sources })
}

fn cleanup_scope_source_ids(
    store: &ArtifactStore,
    request: &ResolvedArtifactRebuildRequest,
    source_kind: &str,
) -> Result<BTreeSet<String>, String> {
    if let Some(target_path) = request.target_path.as_deref() {
        let normalized = normalize_artifact_path(target_path)?;
        let target_id = path_to_artifact_id(&normalized);
        if source_kind == ARTIFACT_SOURCE_DIRECTORY {
            return Ok(BTreeSet::from([target_id]));
        }
        if project_source_kind(source_kind) && is_project_file(&normalized) {
            return Ok(BTreeSet::from([target_id]));
        }
        if source_kind == ARTIFACT_SOURCE_BEAD_STORE
            && target_matches_bead_store(request, &normalized)
        {
            return request
                .beads_dir
                .as_deref()
                .map(path_to_artifact_id)
                .map(|id| BTreeSet::from([id]))
                .ok_or_else(|| {
                    "targeted bead cleanup requires beads_dir".to_string()
                });
        }
        return Ok(BTreeSet::new());
    }

    if let Some(artifact_dir) = request.artifact_dir.as_deref() {
        let artifact_dir_id = path_to_artifact_id(artifact_dir);
        if source_kind == ARTIFACT_SOURCE_AGENT_ARTIFACT {
            return Ok(BTreeSet::from([artifact_dir_id]));
        }
        if source_kind == ARTIFACT_SOURCE_AGENT_CREATED_FILE
            || source_kind == ARTIFACT_SOURCE_AGENT_THOUGHT
        {
            return existing_source_ids_for_hint(
                store,
                source_kind,
                &artifact_dir_id,
            );
        }
        return Ok(BTreeSet::new());
    }

    existing_watermark_source_ids(store, source_kind)
}

fn discover_active_sources(
    request: &ResolvedArtifactRebuildRequest,
) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
    let mut active = BTreeMap::new();

    if source_selected(request, ARTIFACT_SOURCE_DIRECTORY) {
        let mut ids = BTreeSet::new();
        for path in [
            request.projects_root.as_deref(),
            request.workspace_root.as_deref(),
            request.beads_dir.as_deref(),
            request.target_path.as_deref(),
            request.artifact_dir.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if path.exists() {
                ids.insert(path_to_artifact_id(path));
            }
        }
        active.insert(ARTIFACT_SOURCE_DIRECTORY.to_string(), ids);
    }

    if project_ingestion_selected(request) {
        let project_files = discover_selected_project_files(request);
        for source_kind in [
            ARTIFACT_SOURCE_PROJECT_FILE,
            ARTIFACT_SOURCE_CHANGESPEC,
            ARTIFACT_SOURCE_COMMIT,
        ] {
            if source_selected(request, source_kind) {
                active.insert(
                    source_kind.to_string(),
                    project_files
                        .iter()
                        .map(|path| path_to_artifact_id(path))
                        .collect(),
                );
            }
        }
    }

    if source_selected(request, ARTIFACT_SOURCE_BEAD_STORE) {
        let ids = request
            .beads_dir
            .as_deref()
            .filter(|path| path.join("issues.jsonl").exists())
            .map(path_to_artifact_id)
            .map(|id| BTreeSet::from([id]))
            .unwrap_or_default();
        active.insert(ARTIFACT_SOURCE_BEAD_STORE.to_string(), ids);
    }

    if agent_ingestion_selected(request) {
        let records = discover_selected_agent_records(request);
        if source_selected(request, ARTIFACT_SOURCE_AGENT_ARTIFACT) {
            active.insert(
                ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string(),
                records
                    .iter()
                    .filter_map(|record| {
                        normalize_artifact_path(Path::new(&record.artifact_dir))
                            .ok()
                    })
                    .map(|path| path_to_artifact_id(&path))
                    .collect(),
            );
        }
        if source_selected(request, ARTIFACT_SOURCE_AGENT_CREATED_FILE) {
            let mut ids = BTreeSet::new();
            for record in &records {
                ids.extend(
                    collect_agent_created_file_paths(record)
                        .keys()
                        .filter_map(|path| normalize_artifact_path(path).ok())
                        .map(|path| path_to_artifact_id(&path)),
                );
            }
            active.insert(ARTIFACT_SOURCE_AGENT_CREATED_FILE.to_string(), ids);
        }
        if source_selected(request, ARTIFACT_SOURCE_AGENT_THOUGHT) {
            let mut ids = BTreeSet::new();
            for record in &records {
                ids.extend(
                    collect_agent_thoughts(record)
                        .source_files
                        .iter()
                        .filter_map(|path| normalize_artifact_path(path).ok())
                        .map(|path| path_to_artifact_id(&path)),
                );
            }
            active.insert(ARTIFACT_SOURCE_AGENT_THOUGHT.to_string(), ids);
        }
    }

    Ok(active)
}

fn update_source_watermarks(
    store: &mut ArtifactStore,
    active_sources: &BTreeMap<String, BTreeSet<String>>,
) -> Result<ArtifactMutationResultWire, String> {
    let mutations = ArtifactIngestMutations::new("update_source_watermarks");
    let tx = store
        .connection_mut()
        .transaction()
        .map_err(|e| e.to_string())?;
    for (source_kind, source_ids) in active_sources {
        for source_id in source_ids {
            tx.execute(
                r#"
                INSERT INTO source_watermarks (source_kind, source_id, watermark)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(source_kind, source_id) DO UPDATE SET
                    watermark = excluded.watermark,
                    updated_at = CURRENT_TIMESTAMP
                "#,
                params![source_kind, source_id, source_watermark(source_id)],
            )
            .map_err(|e| e.to_string())?;
        }
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(mutations.into_result())
}

fn mark_stale_sources(
    store: &mut ArtifactStore,
    cleanup_plan: &CleanupPlan,
    active_sources: &BTreeMap<String, BTreeSet<String>>,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("mark_stale_sources");
    let mut stale_by_kind = BTreeMap::new();
    for (source_kind, scoped_ids) in &cleanup_plan.scoped_sources {
        let active_ids =
            active_sources.get(source_kind).cloned().unwrap_or_default();
        let stale_ids: BTreeSet<_> =
            scoped_ids.difference(&active_ids).cloned().collect();
        if !stale_ids.is_empty() {
            stale_by_kind.insert(source_kind.clone(), stale_ids);
        }
    }
    if stale_by_kind.is_empty() {
        return Ok(mutations.into_result());
    }

    let tx = store
        .connection_mut()
        .transaction()
        .map_err(|e| e.to_string())?;
    for (source_kind, source_ids) in stale_by_kind {
        for source_id in source_ids {
            let node_ids = stale_node_ids(&tx, &source_kind, &source_id)?;
            for node_id in node_ids {
                if node_id == ARTIFACT_ROOT_ID {
                    continue;
                }
                if insert_stale_node_tombstone(
                    &tx,
                    &node_id,
                    &source_kind,
                    &source_id,
                )? {
                    mutations.result.tombstones_added += 1;
                    mutations.result.affected_node_ids.push(node_id);
                }
            }

            let link_ids = stale_link_ids(&tx, &source_kind, &source_id)?;
            for link_id in link_ids {
                if insert_stale_link_tombstone(
                    &tx,
                    &link_id,
                    &source_kind,
                    &source_id,
                )? {
                    mutations.result.tombstones_added += 1;
                    mutations.result.affected_link_ids.push(link_id);
                }
            }

            tx.execute(
                "DELETE FROM source_watermarks WHERE source_kind = ?1 AND source_id = ?2",
                params![source_kind, source_id],
            )
            .map_err(|e| e.to_string())?;
        }
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(mutations.into_result())
}

fn ingest_project_sources(
    store: &mut ArtifactStore,
    request: &ResolvedArtifactRebuildRequest,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("ingest_projects");
    let Some(projects_root) = request.projects_root.as_deref() else {
        return Ok(mutations.into_result());
    };

    let mut seen_changespec_sources: BTreeMap<String, String> = BTreeMap::new();
    for project_file in discover_project_files(projects_root, &mut mutations) {
        mutations.merge(upsert_project_file(
            store,
            &project_file,
            request,
            &mut seen_changespec_sources,
        )?);
    }

    Ok(mutations.into_result())
}

fn discover_project_files(
    projects_root: &Path,
    mutations: &mut ArtifactIngestMutations,
) -> Vec<PathBuf> {
    let mut project_files = Vec::new();
    let project_dirs = match sorted_read_dir(projects_root) {
        Ok(entries) => entries,
        Err(err) => {
            mutations.error(format!(
                "failed to read projects_root {}: {err}",
                projects_root.display()
            ));
            return project_files;
        }
    };

    for project_dir in project_dirs {
        let metadata = match fs::metadata(&project_dir) {
            Ok(metadata) => metadata,
            Err(err) => {
                mutations.error(format!(
                    "failed to stat project path {}: {err}",
                    project_dir.display()
                ));
                continue;
            }
        };
        if !metadata.is_dir() {
            continue;
        }
        match sorted_read_dir(&project_dir) {
            Ok(entries) => {
                project_files.extend(
                    entries.into_iter().filter(|path| is_project_file(path)),
                );
            }
            Err(err) => mutations.error(format!(
                "failed to read project directory {}: {err}",
                project_dir.display()
            )),
        }
    }

    project_files.sort();
    project_files
}

fn upsert_project_file(
    store: &mut ArtifactStore,
    project_file: &Path,
    request: &ResolvedArtifactRebuildRequest,
    seen_changespec_sources: &mut BTreeMap<String, String>,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("upsert_project_file");
    let normalized = normalize_artifact_path(project_file)?;
    let source_id = path_to_artifact_id(&normalized);
    let source_version = file_source_version(&normalized);

    if let Some(parent) = normalized.parent() {
        mutations.merge(upsert_directory_path(store, parent)?);
    }

    let upsert_project_node =
        source_selected(request, ARTIFACT_SOURCE_PROJECT_FILE)
            || source_selected(request, ARTIFACT_SOURCE_CHANGESPEC)
            || source_selected(request, ARTIFACT_SOURCE_COMMIT);
    if upsert_project_node {
        let node = project_file_node(&normalized, source_version.clone())?;
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
        if node_was_visible {
            let parent_id = normalized
                .parent()
                .map(path_to_artifact_id)
                .unwrap_or_else(|| ARTIFACT_ROOT_ID.to_string());
            mutations.merge(upsert_derived_link(
                store,
                ARTIFACT_LINK_PARENT,
                &node.id,
                &parent_id,
                ARTIFACT_SOURCE_PROJECT_FILE,
                &source_id,
            )?);
            mutations.merge(upsert_derived_payload(
                store,
                derived_payload(
                    node.id.clone(),
                    ARTIFACT_SOURCE_PROJECT_FILE,
                    ARTIFACT_SOURCE_PROJECT_FILE,
                    source_id.clone(),
                    source_version.clone(),
                    project_file_payload(&normalized)?,
                ),
            )?);
        }
    }

    let data = match fs::read(&normalized) {
        Ok(data) => data,
        Err(err) => {
            mutations.error(format!(
                "failed to read project file {}: {err}",
                normalized.display()
            ));
            return Ok(mutations.into_result());
        }
    };
    let specs = match parse_project_bytes(&source_id, &data) {
        Ok(specs) => specs,
        Err(err) => {
            mutations.error(format!("failed to parse project file: {err}"));
            return Ok(mutations.into_result());
        }
    };

    for spec in specs {
        if let Some(first_source) =
            seen_changespec_sources.insert(spec.name.clone(), source_id.clone())
        {
            mutations.error(format!(
                "duplicate ChangeSpec artifact id {} in {} conflicts with {}; keeping first source",
                spec.name, source_id, first_source
            ));
            seen_changespec_sources.insert(spec.name.clone(), first_source);
            continue;
        }
        if source_selected(request, ARTIFACT_SOURCE_CHANGESPEC) {
            mutations.merge(upsert_changespec(
                store,
                &spec,
                &source_id,
                source_version.clone(),
            )?);
        }
        if source_selected(request, ARTIFACT_SOURCE_COMMIT) {
            for commit in &spec.commits {
                mutations.merge(upsert_commit(
                    store,
                    &spec,
                    commit,
                    &source_id,
                    source_version.clone(),
                )?);
            }
        }
    }

    Ok(mutations.into_result())
}

fn upsert_changespec(
    store: &mut ArtifactStore,
    spec: &ChangeSpecWire,
    source_id: &str,
    source_version: Option<String>,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("upsert_changespec");
    let node = changespec_node(spec, source_id, source_version.clone());
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
    if node_was_visible {
        mutations.merge(upsert_derived_link(
            store,
            ARTIFACT_LINK_PARENT,
            &node.id,
            source_id,
            ARTIFACT_SOURCE_CHANGESPEC,
            source_id,
        )?);
        mutations.merge(upsert_derived_payload(
            store,
            derived_payload(
                node.id.clone(),
                ARTIFACT_SOURCE_CHANGESPEC,
                ARTIFACT_SOURCE_CHANGESPEC,
                source_id.to_string(),
                source_version,
                serde_json::to_value(spec).map_err(|e| e.to_string())?,
            ),
        )?);
    }
    Ok(mutations.into_result())
}

fn upsert_commit(
    store: &mut ArtifactStore,
    spec: &ChangeSpecWire,
    commit: &CommitWire,
    source_id: &str,
    source_version: Option<String>,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("upsert_commit");
    let node = commit_node(spec, commit, source_id, source_version.clone());
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
    if node_was_visible {
        mutations.merge(upsert_derived_link(
            store,
            ARTIFACT_LINK_PARENT,
            &node.id,
            &spec.name,
            ARTIFACT_SOURCE_COMMIT,
            source_id,
        )?);
        mutations.merge(upsert_derived_payload(
            store,
            derived_payload(
                node.id.clone(),
                ARTIFACT_SOURCE_COMMIT,
                ARTIFACT_SOURCE_COMMIT,
                source_id.to_string(),
                source_version,
                commit_payload(commit)?,
            ),
        )?);
    }
    Ok(mutations.into_result())
}

fn commit_payload(commit: &CommitWire) -> Result<Value, String> {
    let mut payload =
        serde_json::to_value(commit).map_err(|e| e.to_string())?;
    if let Some(object) = payload.as_object_mut() {
        if let Some(note) = object.get("note").and_then(Value::as_str) {
            object.insert(
                "note".to_string(),
                Value::String(display_commit_note(note).to_string()),
            );
        }
    }
    Ok(payload)
}

fn ingest_agent_artifacts(
    store: &mut ArtifactStore,
    request: &ResolvedArtifactRebuildRequest,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("ingest_agent_artifacts");
    let Some(projects_root) = request.projects_root.as_deref() else {
        return Ok(mutations.into_result());
    };

    let options = AgentArtifactScanOptionsWire::default();
    let records = if let Some(artifact_dir) = request.artifact_dir.as_deref() {
        match scan_agent_artifact_dir(projects_root, artifact_dir, &options) {
            Some(record) => vec![record],
            None => {
                mutations.error(format!(
                    "agent_artifact {} is outside supported projects_root layout",
                    artifact_dir.display()
                ));
                Vec::new()
            }
        }
    } else {
        scan_agent_artifacts(projects_root, options).records
    };

    let agent_ids_by_timestamp = agent_ids_by_timestamp(&records);
    let created_files_selected =
        source_selected(request, ARTIFACT_SOURCE_AGENT_CREATED_FILE);
    let thoughts_selected =
        source_selected(request, ARTIFACT_SOURCE_AGENT_THOUGHT);

    for record in records {
        let agent_id = agent_artifact_id(&record);
        let source_id =
            normalize_artifact_path(Path::new(&record.artifact_dir))
                .map(|path| path_to_artifact_id(&path))
                .unwrap_or_else(|_| record.artifact_dir.clone());

        for dir in agent_directory_paths(&record) {
            mutations.merge(upsert_agent_directory(store, &dir, &source_id)?);
        }

        mutations
            .merge(upsert_agent_node(store, &record, &agent_id, &source_id)?);
        if !artifact_node_exists(store, &agent_id)? {
            continue;
        }
        mutations.merge(upsert_derived_link(
            store,
            ARTIFACT_LINK_PARENT,
            &agent_id,
            &source_id,
            ARTIFACT_SOURCE_AGENT_ARTIFACT,
            &source_id,
        )?);

        let created_files = collect_agent_created_file_paths(&record);
        let related_targets =
            collect_agent_related_targets(&record, &agent_ids_by_timestamp);
        let thought_extraction = if thoughts_selected {
            collect_agent_thoughts(&record)
        } else {
            ThoughtExtraction::default()
        };
        mutations.merge(upsert_derived_payload(
            store,
            derived_payload(
                agent_id.clone(),
                AGENT_PAYLOAD_TYPE,
                ARTIFACT_SOURCE_AGENT_ARTIFACT,
                source_id.clone(),
                None,
                json!({
                    "agent_id": agent_id,
                    "agent_id_source": if explicit_agent_artifact_id(&record).is_some() {
                        "marker_name"
                    } else {
                        "fallback"
                    },
                    "fallback_agent_id": fallback_agent_artifact_id(&record),
                    "source_artifact_dir": source_id,
                    "record": record,
                    "created_file_paths": created_files
                        .keys()
                        .map(|path| path_to_artifact_id(path))
                        .collect::<Vec<_>>(),
                    "thought_count": thought_extraction.thoughts.len(),
                    "thought_source_files": thought_extraction
                        .source_files
                        .iter()
                        .map(|path| path_to_artifact_id(path))
                        .collect::<Vec<_>>(),
                    "thought_errors": thought_extraction.errors.clone(),
                    "unresolved_related": related_targets.unresolved,
                }),
            ),
        )?);

        if created_files_selected {
            for (path, reason) in created_files {
                mutations.merge(upsert_created_file(
                    store, &agent_id, &source_id, &path, &reason,
                )?);
            }
        }

        if thoughts_selected {
            for source_file in &thought_extraction.source_files {
                mutations.merge(upsert_created_file(
                    store,
                    &agent_id,
                    &source_id,
                    source_file,
                    "thought_source",
                )?);
            }
            for error in &thought_extraction.errors {
                mutations.error(format!(
                    "agent_thought {}: {error}",
                    record.artifact_dir
                ));
            }
            for thought in thought_extraction.thoughts {
                mutations.merge(upsert_agent_thought(
                    store, &agent_id, &source_id, thought,
                )?);
            }
        }

        for target_id in related_targets.resolved {
            if target_id != agent_id && artifact_node_exists(store, &target_id)?
            {
                mutations.merge(upsert_derived_link(
                    store,
                    ARTIFACT_LINK_RELATED,
                    &agent_id,
                    &target_id,
                    ARTIFACT_SOURCE_AGENT_ARTIFACT,
                    &source_id,
                )?);
            }
        }
    }

    Ok(mutations.into_result())
}

fn agent_artifact_id(record: &AgentArtifactRecordWire) -> String {
    explicit_agent_artifact_id(record)
        .unwrap_or_else(|| fallback_agent_artifact_id(record))
}

fn explicit_agent_artifact_id(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| {
            meta.artifact_agent_id
                .as_deref()
                .and_then(non_empty)
                .or_else(|| meta.name.as_deref().and_then(non_empty))
        })
        .or_else(|| {
            record
                .done
                .as_ref()
                .and_then(|done| done.name.as_deref().and_then(non_empty))
        })
}

fn fallback_agent_artifact_id(record: &AgentArtifactRecordWire) -> String {
    format!(
        "agent:{}:{}:{}",
        record.project_name, record.workflow_dir_name, record.timestamp
    )
}

fn upsert_agent_node(
    store: &mut ArtifactStore,
    record: &AgentArtifactRecordWire,
    agent_id: &str,
    source_id: &str,
) -> Result<ArtifactMutationResultWire, String> {
    let mut metadata = Map::new();
    metadata.insert(
        "project".to_string(),
        Value::String(record.project_name.clone()),
    );
    metadata.insert(
        "workflow".to_string(),
        Value::String(record.workflow_dir_name.clone()),
    );
    metadata.insert(
        "timestamp".to_string(),
        Value::String(record.timestamp.clone()),
    );
    metadata.insert(
        "artifact_dir".to_string(),
        Value::String(record.artifact_dir.clone()),
    );
    metadata.insert(
        "source_artifact_dir".to_string(),
        Value::String(source_id.to_string()),
    );
    let uses_fallback_agent_id = explicit_agent_artifact_id(record).is_none();
    metadata.insert(
        "fallback_agent_id".to_string(),
        Value::String(fallback_agent_artifact_id(record)),
    );
    metadata.insert(
        "agent_id_source".to_string(),
        Value::String(
            if uses_fallback_agent_id {
                "fallback"
            } else {
                "marker_name"
            }
            .to_string(),
        ),
    );
    metadata.insert(
        "uses_fallback_agent_id".to_string(),
        Value::Bool(uses_fallback_agent_id),
    );
    metadata.insert(
        "has_done_marker".to_string(),
        Value::Bool(record.has_done_marker),
    );

    let node = ArtifactNodeWire {
        id: agent_id.to_string(),
        kind: ARTIFACT_KIND_AGENT.to_string(),
        display_title: agent_display_title(record, agent_id),
        subtitle: Some(format!(
            "{} / {} / {}",
            record.project_name, record.workflow_dir_name, record.timestamp
        )),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string()),
        source_id: Some(source_id.to_string()),
        source_version: None,
        search_text: agent_search_text(record, agent_id),
        metadata,
        created_at: record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.run_started_at.clone()),
        updated_at: record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.stopped_at.clone()),
    };

    upsert_artifact_node(
        store,
        ArtifactNodeUpsertWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            node,
            replace_payloads: false,
        },
    )
}

fn agent_display_title(
    record: &AgentArtifactRecordWire,
    agent_id: &str,
) -> String {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.name.as_deref().and_then(non_empty))
        .or_else(|| {
            record
                .done
                .as_ref()
                .and_then(|done| done.name.as_deref().and_then(non_empty))
        })
        .unwrap_or_else(|| agent_id.to_string())
}

fn agent_search_text(
    record: &AgentArtifactRecordWire,
    agent_id: &str,
) -> String {
    let mut parts = vec![
        agent_id.to_string(),
        record.project_name.clone(),
        record.workflow_dir_name.clone(),
        record.timestamp.clone(),
        record.artifact_dir.clone(),
    ];
    if let Some(done) = &record.done {
        push_optional(&mut parts, &done.cl_name);
        push_optional(&mut parts, &done.model);
        push_optional(&mut parts, &done.llm_provider);
        push_optional(&mut parts, &done.vcs_provider);
        push_optional(&mut parts, &done.outcome);
    }
    if let Some(meta) = &record.agent_meta {
        push_optional(&mut parts, &meta.name);
        push_optional(&mut parts, &meta.artifact_agent_id);
        push_optional(&mut parts, &meta.changespec_name);
        push_optional(&mut parts, &meta.cl_name);
        push_optional(&mut parts, &meta.bead_id);
        push_optional(&mut parts, &meta.epic_bead_id);
        push_optional(&mut parts, &meta.phase_bead_id);
        push_optional(&mut parts, &meta.legend_bead_id);
        push_optional(&mut parts, &meta.commit_changespec_name);
        push_optional(&mut parts, &meta.commit_entry_id);
        push_optional(&mut parts, &meta.model);
        push_optional(&mut parts, &meta.llm_provider);
        push_optional(&mut parts, &meta.vcs_provider);
        push_optional(&mut parts, &meta.role_suffix);
    }
    if let Some(running) = &record.running {
        push_optional(&mut parts, &running.cl_name);
        push_optional(&mut parts, &running.model);
        push_optional(&mut parts, &running.llm_provider);
        push_optional(&mut parts, &running.vcs_provider);
    }
    if record.running.is_some() {
        parts.push("running".to_string());
    }
    if record.waiting.is_some() {
        parts.push("waiting".to_string());
    }
    if record.done.is_some() {
        parts.push("done".to_string());
    }
    parts.join(" ")
}

fn agent_directory_paths(record: &AgentArtifactRecordWire) -> Vec<PathBuf> {
    let mut paths = vec![
        PathBuf::from(&record.project_dir),
        Path::new(&record.project_dir).join("artifacts"),
        Path::new(&record.project_dir)
            .join("artifacts")
            .join(&record.workflow_dir_name),
        PathBuf::from(&record.artifact_dir),
    ];
    paths.sort();
    paths.dedup();
    paths
}

fn upsert_agent_directory(
    store: &mut ArtifactStore,
    path: &Path,
    source_id: &str,
) -> Result<ArtifactMutationResultWire, String> {
    artifact_upsert_path(
        store,
        path,
        ArtifactPathUpsertRequestWire {
            kind: Some(ARTIFACT_KIND_DIRECTORY.to_string()),
            provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
            source_kind: Some(ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string()),
            source_id: Some(source_id.to_string()),
            ..ArtifactPathUpsertRequestWire::default()
        },
    )
}

#[derive(Debug, Default)]
struct RelatedTargets {
    resolved: BTreeSet<String>,
    unresolved: Vec<String>,
}

fn collect_agent_related_targets(
    record: &AgentArtifactRecordWire,
    ids_by_timestamp: &BTreeMap<String, BTreeSet<String>>,
) -> RelatedTargets {
    let mut targets = RelatedTargets::default();

    for maybe_changespec in [
        record
            .done
            .as_ref()
            .and_then(|done| done.cl_name.as_deref()),
        record
            .running
            .as_ref()
            .and_then(|running| running.cl_name.as_deref()),
        record
            .workflow_state
            .as_ref()
            .and_then(|workflow| workflow.cl_name.as_deref()),
    ] {
        if let Some(changespec) = maybe_changespec.and_then(non_empty) {
            targets.resolved.insert(changespec);
        }
    }

    let mut timestamp_refs = Vec::new();
    if let Some(meta) = &record.agent_meta {
        push_optional(&mut timestamp_refs, &meta.parent_timestamp);
        push_optional(&mut timestamp_refs, &meta.parent_agent_timestamp);
        push_optional(&mut timestamp_refs, &meta.retry_of_timestamp);
        push_optional(&mut timestamp_refs, &meta.retried_as_timestamp);
        push_optional(&mut timestamp_refs, &meta.retry_chain_root_timestamp);
        timestamp_refs.extend(meta.plan_submitted_at.iter().cloned());
        timestamp_refs.extend(meta.feedback_submitted_at.iter().cloned());
        timestamp_refs.extend(meta.questions_submitted_at.iter().cloned());
        timestamp_refs.extend(meta.retry_started_at.iter().cloned());

        for maybe_related in [
            meta.changespec_name.as_deref(),
            meta.cl_name.as_deref(),
            meta.commit_changespec_name.as_deref(),
            meta.bead_id.as_deref(),
            meta.epic_bead_id.as_deref(),
            meta.phase_bead_id.as_deref(),
            meta.legend_bead_id.as_deref(),
            meta.parent_agent_name.as_deref(),
        ] {
            if let Some(target) = maybe_related.and_then(non_empty) {
                targets.resolved.insert(target.to_string());
            }
        }
        if let (Some(changespec), Some(entry_id)) = (
            meta.commit_changespec_name.as_deref().and_then(non_empty),
            meta.commit_entry_id.as_deref().and_then(non_empty),
        ) {
            targets.resolved.insert(format!("{changespec}:{entry_id}"));
        }
    }
    if let Some(done) = &record.done {
        push_optional(&mut timestamp_refs, &done.retried_as_timestamp);
        push_optional(&mut timestamp_refs, &done.retry_chain_root_timestamp);
    }

    timestamp_refs.sort();
    timestamp_refs.dedup();
    for timestamp in timestamp_refs {
        match ids_by_timestamp.get(&timestamp) {
            Some(ids) if ids.len() == 1 => {
                targets.resolved.extend(ids.iter().cloned());
            }
            Some(ids) => targets.unresolved.push(format!(
                "ambiguous timestamp {timestamp}: {}",
                ids.iter().cloned().collect::<Vec<_>>().join(", ")
            )),
            None => targets.unresolved.push(timestamp),
        }
    }

    targets
}

fn agent_ids_by_timestamp(
    records: &[AgentArtifactRecordWire],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for record in records {
        out.entry(record.timestamp.clone())
            .or_default()
            .insert(agent_artifact_id(record));
    }
    out
}

#[derive(Debug, Clone, Default)]
struct ThoughtExtraction {
    thoughts: Vec<ThoughtRecord>,
    source_files: Vec<PathBuf>,
    errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct ThoughtRecord {
    text: String,
    source_provider: String,
    source_file: PathBuf,
    ordinal: usize,
    timestamp: Option<String>,
    following_action: Option<String>,
    display_title: String,
}

fn collect_agent_thoughts(
    record: &AgentArtifactRecordWire,
) -> ThoughtExtraction {
    let artifact_dir = Path::new(&record.artifact_dir);
    let mut extraction = ThoughtExtraction::default();

    let codex_path = artifact_dir.join("codex_thinking.jsonl");
    if codex_path.exists() {
        merge_thought_parse_result(
            &mut extraction,
            parse_codex_thinking_file(&codex_path),
        );
    }

    for claude_path in discover_claude_thinking_paths(artifact_dir) {
        merge_thought_parse_result(
            &mut extraction,
            parse_claude_thinking_file(&claude_path),
        );
    }

    extraction.source_files.sort();
    extraction.source_files.dedup();
    extraction.thoughts.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.source_provider.cmp(&right.source_provider))
            .then_with(|| left.source_file.cmp(&right.source_file))
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    extraction
}

fn merge_thought_parse_result(
    extraction: &mut ThoughtExtraction,
    result: ThoughtParseResult,
) {
    if result.source_file.is_some() && !result.thoughts.is_empty() {
        extraction
            .source_files
            .extend(result.source_file.iter().cloned());
    }
    extraction.thoughts.extend(result.thoughts);
    extraction.errors.extend(result.errors);
}

#[derive(Debug, Clone, Default)]
struct ThoughtParseResult {
    source_file: Option<PathBuf>,
    thoughts: Vec<ThoughtRecord>,
    errors: Vec<String>,
}

fn parse_codex_thinking_file(path: &Path) -> ThoughtParseResult {
    let mut result = ThoughtParseResult {
        source_file: Some(path.to_path_buf()),
        ..ThoughtParseResult::default()
    };
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) => {
            result.errors.push(format!(
                "failed to read Codex thinking file {}: {error}",
                path.display()
            ));
            return result;
        }
    };

    let mut ordinal = 0usize;
    let mut malformed = 0usize;
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else {
            malformed += 1;
            continue;
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(Value::Object(entry)) = serde_json::from_str::<Value>(line)
        else {
            malformed += 1;
            continue;
        };
        let Some(text) = entry.get("text").and_then(Value::as_str) else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        ordinal += 1;
        result.thoughts.push(ThoughtRecord {
            text: text.to_string(),
            source_provider: "codex".to_string(),
            source_file: path.to_path_buf(),
            ordinal,
            timestamp: entry
                .get("timestamp")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string),
            following_action: entry
                .get("following_action")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string),
            display_title: thought_display_title(text),
        });
    }
    if malformed > 0 {
        result.errors.push(format!(
            "skipped {malformed} malformed Codex thinking line(s) in {}",
            path.display()
        ));
    }
    result
}

fn discover_claude_thinking_paths(artifact_dir: &Path) -> Vec<PathBuf> {
    let Some(meta) = load_json_object(&artifact_dir.join("agent_meta.json"))
    else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    for key in ["claude_session_path", "claude_transcript_path"] {
        push_json_string_paths(&mut paths, artifact_dir, meta.get(key));
    }
    for key in ["claude_session_paths", "claude_transcript_paths"] {
        push_json_string_paths(&mut paths, artifact_dir, meta.get(key));
    }
    paths.sort();
    paths.dedup();
    paths.retain(|path| path.exists());
    paths
}

fn push_json_string_paths(
    paths: &mut Vec<PathBuf>,
    artifact_dir: &Path,
    value: Option<&Value>,
) {
    match value {
        Some(Value::String(path)) if !path.trim().is_empty() => {
            paths.push(resolve_agent_path(artifact_dir, path));
        }
        Some(Value::Array(items)) => {
            for item in items {
                if let Value::String(path) = item {
                    if !path.trim().is_empty() {
                        paths.push(resolve_agent_path(artifact_dir, path));
                    }
                }
            }
        }
        _ => {}
    }
}

fn parse_claude_thinking_file(path: &Path) -> ThoughtParseResult {
    let mut result = ThoughtParseResult {
        source_file: Some(path.to_path_buf()),
        ..ThoughtParseResult::default()
    };
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            result.errors.push(format!(
                "failed to read Claude transcript {}: {error}",
                path.display()
            ));
            return result;
        }
    };
    let content = String::from_utf8_lossy(&bytes);
    let mut events = Vec::new();
    let mut malformed = 0usize;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(event)) => events.push(event),
            Ok(_) | Err(_) => malformed += 1,
        }
    }

    let mut ordinal = 0usize;
    for (index, event) in events.iter().enumerate() {
        if event.get("type").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(content) = event
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for block in content {
            if block.get("type").and_then(Value::as_str) != Some("thinking") {
                continue;
            }
            let Some(text) = block.get("thinking").and_then(Value::as_str)
            else {
                continue;
            };
            if text.trim().is_empty() {
                continue;
            }
            ordinal += 1;
            result.thoughts.push(ThoughtRecord {
                text: text.to_string(),
                source_provider: "claude".to_string(),
                source_file: path.to_path_buf(),
                ordinal,
                timestamp: event
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_string),
                following_action: find_claude_following_action(&events, index),
                display_title: thought_display_title(text),
            });
        }
    }
    if malformed > 0 {
        result.errors.push(format!(
            "skipped {malformed} malformed Claude transcript line(s) in {}",
            path.display()
        ));
    }
    result
}

fn find_claude_following_action(
    events: &[Map<String, Value>],
    index: usize,
) -> Option<String> {
    let mut text_action = None;
    for event in events.iter().skip(index + 1).take(5).filter(|event| {
        event.get("type").and_then(Value::as_str) == Some("assistant")
    }) {
        let Some(content) = event
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for block in content {
            if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                return Some(format_claude_tool_action(block));
            }
            if block.get("type").and_then(Value::as_str) == Some("text")
                && text_action.is_none()
            {
                if let Some(text) = block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    text_action = Some(truncate_chars(text, 80));
                }
            }
        }
    }
    text_action
}

fn format_claude_tool_action(block: &Value) -> String {
    let name = block
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Unknown");
    let input = block.get("input").and_then(Value::as_object);

    if matches!(name, "Read" | "Write" | "Edit") {
        let filename = input
            .and_then(|input| input.get("file_path"))
            .and_then(Value::as_str)
            .and_then(path_basename);
        return filename
            .map(|filename| format!("{name} {filename}"))
            .unwrap_or_else(|| name.to_string());
    }

    if name == "Bash" {
        let first_word = input
            .and_then(|input| input.get("command"))
            .and_then(Value::as_str)
            .and_then(|command| command.split_whitespace().next());
        return first_word
            .map(|word| format!("Bash `{word}`"))
            .unwrap_or_else(|| "Bash".to_string());
    }

    if matches!(name, "Grep" | "Glob") {
        let pattern = input
            .and_then(|input| input.get("pattern"))
            .and_then(Value::as_str)
            .filter(|pattern| !pattern.is_empty());
        return pattern
            .map(|pattern| format!("{name} /{pattern}/"))
            .unwrap_or_else(|| name.to_string());
    }

    if name == "Task" {
        return "Task (subagent)".to_string();
    }
    if name == "Skill" {
        let skill = input
            .and_then(|input| input.get("skill"))
            .and_then(Value::as_str)
            .filter(|skill| !skill.is_empty());
        return skill
            .map(|skill| format!("Skill {skill}"))
            .unwrap_or_else(|| "Skill".to_string());
    }

    name.to_string()
}

fn path_basename(path: &str) -> Option<&str> {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
}

fn upsert_agent_thought(
    store: &mut ArtifactStore,
    agent_id: &str,
    agent_source_id: &str,
    thought: ThoughtRecord,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("upsert_agent_thought");
    let source_file_id = normalize_artifact_path(&thought.source_file)
        .map(|path| path_to_artifact_id(&path))
        .unwrap_or_else(|_| thought.source_file.to_string_lossy().into_owned());
    let thought_id = thought_artifact_id(&thought, &source_file_id);
    let mut metadata = Map::new();
    metadata.insert("created_by".to_string(), json!(agent_id));
    metadata.insert(
        "source_provider".to_string(),
        json!(thought.source_provider),
    );
    metadata.insert("source_file".to_string(), json!(source_file_id.clone()));
    metadata.insert("ordinal".to_string(), json!(thought.ordinal));
    if let Some(timestamp) = &thought.timestamp {
        metadata.insert("timestamp".to_string(), json!(timestamp));
    }

    let node = ArtifactNodeWire {
        id: thought_id.clone(),
        kind: ARTIFACT_KIND_THOUGHT.to_string(),
        display_title: thought.display_title.clone(),
        subtitle: Some(format!(
            "{} thought {}",
            thought.source_provider, thought.ordinal
        )),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(ARTIFACT_SOURCE_AGENT_THOUGHT.to_string()),
        source_id: Some(source_file_id.clone()),
        source_version: None,
        search_text: format!(
            "{} {} {} {}",
            thought.display_title,
            thought.source_provider,
            source_file_id,
            thought.text
        ),
        metadata,
        created_at: thought.timestamp.clone(),
        updated_at: None,
    };

    mutations.merge(upsert_artifact_node(
        store,
        ArtifactNodeUpsertWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            node,
            replace_payloads: false,
        },
    )?);

    if artifact_node_exists(store, &thought_id)? {
        mutations.merge(upsert_derived_payload(
            store,
            derived_payload(
                thought_id.clone(),
                AGENT_THOUGHT_PAYLOAD_TYPE,
                ARTIFACT_SOURCE_AGENT_THOUGHT,
                source_file_id.clone(),
                None,
                json!({
                    "text": thought.text,
                    "source_provider": thought.source_provider,
                    "source_file": source_file_id,
                    "ordinal": thought.ordinal,
                    "timestamp": thought.timestamp,
                    "following_action": thought.following_action,
                    "display_title": thought.display_title,
                }),
            ),
        )?);
        mutations.merge(upsert_derived_link(
            store,
            ARTIFACT_LINK_CREATED,
            agent_id,
            &thought_id,
            ARTIFACT_SOURCE_AGENT_THOUGHT,
            agent_source_id,
        )?);
        mutations.merge(upsert_derived_link(
            store,
            ARTIFACT_LINK_PARENT,
            &thought_id,
            agent_id,
            ARTIFACT_SOURCE_AGENT_THOUGHT,
            agent_source_id,
        )?);
    }

    Ok(mutations.into_result())
}

fn thought_artifact_id(
    thought: &ThoughtRecord,
    source_file_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(thought.text.as_bytes());
    hasher.update(b"\0");
    hasher.update(thought.source_provider.as_bytes());
    hasher.update(b"\0");
    hasher.update(source_file_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(thought.ordinal.to_string().as_bytes());
    format!("thought:{}", hex_prefix(&hasher.finalize(), 16))
}

fn hex_prefix(bytes: &[u8], chars: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(chars);
    for byte in bytes {
        if output.len() >= chars {
            break;
        }
        output.push(HEX[(byte >> 4) as usize] as char);
        if output.len() >= chars {
            break;
        }
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn thought_display_title(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        "Thought".to_string()
    } else {
        truncate_chars(&compact, 60)
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let mut iter = text.chars();
    let mut truncated = String::new();
    for _ in 0..max_chars {
        let Some(ch) = iter.next() else {
            return text.to_string();
        };
        truncated.push(ch);
    }
    if iter.next().is_some() {
        truncated.push_str("...");
    }
    truncated
}

fn load_json_object(path: &Path) -> Option<Map<String, Value>> {
    let bytes = fs::read(path).ok()?;
    match serde_json::from_slice::<Value>(&bytes).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

fn collect_agent_created_file_paths(
    record: &AgentArtifactRecordWire,
) -> BTreeMap<PathBuf, String> {
    let artifact_dir = Path::new(&record.artifact_dir);
    let mut paths = BTreeMap::new();

    for name in [
        "agent_meta.json",
        "done.json",
        "running.json",
        "waiting.json",
        "workflow_state.json",
        "plan_path.json",
        "commit_result.json",
        "plan_feedback.jsonl",
        "question_request.json",
        "question_response.json",
        "raw_xprompt.md",
        "live_reply.md",
        "live_reply_timestamps.jsonl",
        "qa_log.jsonl",
    ] {
        insert_if_exists(&mut paths, artifact_dir.join(name), "well_known");
    }

    for prompt_step in &record.prompt_steps {
        insert_if_exists(
            &mut paths,
            artifact_dir.join(&prompt_step.file_name),
            "prompt_step_marker",
        );
        insert_prompt_step_paths(&mut paths, artifact_dir, prompt_step);
    }
    insert_artifact_dir_prompt_files(&mut paths, artifact_dir);

    if let Some(done) = &record.done {
        insert_done_marker_paths(&mut paths, artifact_dir, done);
    }
    if let Some(plan_path) = &record.plan_path {
        insert_optional_marker_path(
            &mut paths,
            artifact_dir,
            plan_path.plan_path.as_ref(),
            "plan_path_marker",
        );
    }
    for marker_name in [
        "agent_meta.json",
        "done.json",
        "workflow_state.json",
        "commit_result.json",
    ] {
        insert_raw_marker_paths(&mut paths, artifact_dir, marker_name);
    }

    paths
}

fn insert_done_marker_paths(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    done: &DoneMarkerWire,
) {
    insert_optional_marker_path(
        paths,
        artifact_dir,
        done.plan_path.as_ref(),
        "done_plan",
    );
    insert_optional_marker_path(
        paths,
        artifact_dir,
        done.diff_path.as_ref(),
        "done_diff",
    );
    insert_optional_marker_path(
        paths,
        artifact_dir,
        done.response_path.as_ref(),
        "done_response",
    );
    insert_optional_marker_path(
        paths,
        artifact_dir,
        done.output_path.as_ref(),
        "done_output",
    );
    for path in &done.markdown_pdf_paths {
        insert_marker_path(paths, artifact_dir, path, "done_markdown_pdf");
    }
    if let Some(step_output) = &done.step_output {
        insert_typed_output_paths(
            paths,
            artifact_dir,
            step_output,
            None,
            "done_step_output",
        );
    }
}

fn insert_prompt_step_paths(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    prompt_step: &PromptStepMarkerWire,
) {
    insert_optional_marker_path(
        paths,
        artifact_dir,
        prompt_step.diff_path.as_ref(),
        "prompt_step_diff",
    );
    insert_optional_marker_path(
        paths,
        artifact_dir,
        prompt_step.response_path.as_ref(),
        "prompt_step_response",
    );
    if let Some(output) = &prompt_step.output {
        insert_typed_output_paths(
            paths,
            artifact_dir,
            output,
            prompt_step.output_types.as_ref(),
            "prompt_step_output",
        );
    }
}

fn insert_typed_output_paths(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    output: &Map<String, Value>,
    output_types: Option<&BTreeMap<String, String>>,
    reason: &str,
) {
    if let Some(output_types) = output_types {
        for (key, output_type) in output_types {
            if output_type == "path" || output_type == "file" {
                insert_value_paths(
                    paths,
                    artifact_dir,
                    output.get(key),
                    reason,
                );
            }
        }
    } else {
        for key in ["path", "file", "diff_path", "response_path", "plan_path"] {
            insert_value_paths(paths, artifact_dir, output.get(key), reason);
        }
    }
}

fn insert_artifact_dir_prompt_files(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
) {
    let Ok(read_dir) = fs::read_dir(artifact_dir) else {
        return;
    };
    for entry in read_dir.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with("_prompt.md") || name == "prompt.md" {
            paths
                .entry(path)
                .or_insert_with(|| "prompt_markdown".to_string());
        }
    }
}

fn insert_raw_marker_paths(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    marker_name: &str,
) {
    let marker_path = artifact_dir.join(marker_name);
    let Ok(bytes) = fs::read(&marker_path) else {
        return;
    };
    let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&bytes) else {
        return;
    };
    for key in [
        "chat_path",
        "diff_path",
        "plan_path",
        "sdd_prompt_path",
        "sdd_plan_path",
        "response_path",
        "output_path",
        "live_reply_path",
        "timestamps_path",
        "feedback_path",
        "plan_feedback_path",
        "question_path",
        "question_request_path",
        "question_response_path",
        "commit_diff_path",
    ] {
        insert_value_paths(paths, artifact_dir, map.get(key), marker_name);
    }
    for key in ["markdown_pdf_paths", "image_paths", "file_paths"] {
        insert_value_paths(paths, artifact_dir, map.get(key), marker_name);
    }
}

fn insert_value_paths(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    value: Option<&Value>,
    reason: &str,
) {
    match value {
        Some(Value::String(path)) => {
            insert_marker_path(paths, artifact_dir, path, reason)
        }
        Some(Value::Array(items)) => {
            for item in items {
                if let Value::String(path) = item {
                    insert_marker_path(paths, artifact_dir, path, reason);
                }
            }
        }
        _ => {}
    }
}

fn insert_if_exists(
    paths: &mut BTreeMap<PathBuf, String>,
    path: PathBuf,
    reason: &str,
) {
    if path.exists() {
        paths.entry(path).or_insert_with(|| reason.to_string());
    }
}

fn insert_optional_marker_path(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    maybe_path: Option<&String>,
    reason: &str,
) {
    if let Some(path) = maybe_path {
        insert_marker_path(paths, artifact_dir, path, reason);
    }
}

fn insert_marker_path(
    paths: &mut BTreeMap<PathBuf, String>,
    artifact_dir: &Path,
    path: &str,
    reason: &str,
) {
    if path.trim().is_empty() {
        return;
    }
    paths
        .entry(resolve_agent_path(artifact_dir, path))
        .or_insert_with(|| reason.to_string());
}

fn upsert_created_file(
    store: &mut ArtifactStore,
    agent_id: &str,
    agent_source_id: &str,
    path: &Path,
    reason: &str,
) -> Result<ArtifactMutationResultWire, String> {
    let normalized = normalize_artifact_path(path)?;
    let artifact_id = path_to_artifact_id(&normalized);
    let mut mutations =
        ArtifactIngestMutations::new("upsert_agent_created_file");
    let mut metadata = Map::new();
    metadata.insert(
        "created_by".to_string(),
        Value::String(agent_id.to_string()),
    );
    metadata.insert("reason".to_string(), Value::String(reason.to_string()));

    mutations.merge(artifact_upsert_path(
        store,
        &normalized,
        ArtifactPathUpsertRequestWire {
            kind: Some(ARTIFACT_KIND_FILE.to_string()),
            provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
            source_kind: Some(ARTIFACT_SOURCE_AGENT_CREATED_FILE.to_string()),
            source_id: Some(artifact_id.clone()),
            metadata: Some(metadata),
            ..ArtifactPathUpsertRequestWire::default()
        },
    )?);

    if artifact_node_exists(store, &artifact_id)? {
        mutations.merge(upsert_derived_payload(
            store,
            derived_payload(
                artifact_id.clone(),
                AGENT_CREATED_FILE_PAYLOAD_TYPE,
                ARTIFACT_SOURCE_AGENT_CREATED_FILE,
                artifact_id.clone(),
                None,
                json!({
                    "path": artifact_id,
                    "created_by": agent_id,
                    "reason": reason,
                }),
            ),
        )?);
        mutations.merge(upsert_derived_link(
            store,
            ARTIFACT_LINK_CREATED,
            agent_id,
            &artifact_id,
            ARTIFACT_SOURCE_AGENT_CREATED_FILE,
            agent_source_id,
        )?);
    }

    Ok(mutations.into_result())
}

fn resolve_agent_path(artifact_dir: &Path, path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        artifact_dir.join(candidate)
    }
}

fn non_empty(value: &str) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn push_optional(parts: &mut Vec<String>, value: &Option<String>) {
    if let Some(value) = value.as_deref().and_then(non_empty) {
        parts.push(value);
    }
}

fn ingest_beads(
    store: &mut ArtifactStore,
    request: &ResolvedArtifactRebuildRequest,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("ingest_beads");
    let Some(beads_dir) = request.beads_dir.as_deref() else {
        return Ok(mutations.into_result());
    };

    let beads_dir_id = path_to_artifact_id(beads_dir);
    mutations.merge(artifact_upsert_path(
        store,
        beads_dir,
        ArtifactPathUpsertRequestWire {
            kind: Some(ARTIFACT_KIND_DIRECTORY.to_string()),
            provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
            source_kind: Some(ARTIFACT_SOURCE_DIRECTORY.to_string()),
            source_id: Some(beads_dir_id.clone()),
            ..ArtifactPathUpsertRequestWire::default()
        },
    )?);

    let issues = match read_store_issues(beads_dir) {
        Ok(issues) => issues,
        Err(error) => {
            mutations.error(format!(
                "bead_store {}: {}",
                beads_dir.display(),
                error
            ));
            return Ok(mutations.into_result());
        }
    };

    let issue_by_id: BTreeMap<&str, &IssueWire> = issues
        .iter()
        .map(|issue| (issue.id.as_str(), issue))
        .collect();
    let mut existing_worker_agent_ids = BTreeSet::new();
    for issue in &issues {
        if let Some(worker_agent_id) = worker_agent_id(issue) {
            if artifact_node_exists_with_kind(
                store,
                worker_agent_id,
                ARTIFACT_KIND_AGENT,
            )? {
                existing_worker_agent_ids.insert(worker_agent_id.to_string());
            }
        }
    }
    let source_id = beads_dir_id.as_str();

    for issue in &issues {
        let node = bead_node(issue, source_id)?;
        let node_id = node.id.clone();
        mutations.merge(upsert_artifact_node(
            store,
            ArtifactNodeUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                node,
                replace_payloads: false,
            },
        )?);
        if artifact_node_exists(store, &node_id)? {
            mutations.merge(upsert_derived_payload(
                store,
                bead_payload(issue, source_id, &existing_worker_agent_ids)?,
            )?);
        }
    }

    for issue in &issues {
        if !artifact_node_exists(store, &issue.id)? {
            continue;
        }

        if let Some(parent_id) = issue.parent_id.as_deref() {
            if issue_by_id.contains_key(parent_id)
                && artifact_node_exists(store, parent_id)?
            {
                mutations.merge(upsert_derived_link(
                    store,
                    ARTIFACT_LINK_PARENT,
                    &issue.id,
                    parent_id,
                    ARTIFACT_SOURCE_BEAD_STORE,
                    source_id,
                )?);
            }
        } else if artifact_node_exists(store, source_id)? {
            mutations.merge(upsert_derived_link(
                store,
                ARTIFACT_LINK_PARENT,
                &issue.id,
                source_id,
                ARTIFACT_SOURCE_BEAD_STORE,
                source_id,
            )?);
        }

        if let Some(worker_agent_id) = worker_agent_id(issue) {
            if existing_worker_agent_ids.contains(worker_agent_id) {
                mutations.merge(upsert_derived_link(
                    store,
                    ARTIFACT_LINK_WORKER,
                    &issue.id,
                    worker_agent_id,
                    ARTIFACT_SOURCE_BEAD_STORE,
                    source_id,
                )?);
            }
        }

        if !issue.changespec_name.is_empty()
            && artifact_node_exists_with_kind(
                store,
                &issue.changespec_name,
                ARTIFACT_KIND_CHANGESPEC,
            )?
        {
            mutations.merge(upsert_derived_link(
                store,
                ARTIFACT_LINK_RELATED,
                &issue.id,
                &issue.changespec_name,
                ARTIFACT_SOURCE_BEAD_STORE,
                source_id,
            )?);
        }
    }

    Ok(mutations.into_result())
}

fn bead_node(
    issue: &IssueWire,
    source_id: &str,
) -> Result<ArtifactNodeWire, String> {
    let mut metadata = Map::new();
    metadata.insert("status".to_string(), issue_field_value(&issue.status)?);
    metadata.insert(
        "issue_type".to_string(),
        issue_field_value(&issue.issue_type)?,
    );
    metadata.insert("tier".to_string(), issue_field_value(&issue.tier)?);
    metadata.insert(
        "parent_id".to_string(),
        issue_field_value(&issue.parent_id)?,
    );

    Ok(ArtifactNodeWire {
        id: issue.id.clone(),
        kind: ARTIFACT_KIND_BEAD.to_string(),
        display_title: issue.title.clone(),
        subtitle: Some(bead_subtitle(issue)?),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(ARTIFACT_SOURCE_BEAD_STORE.to_string()),
        source_id: Some(source_id.to_string()),
        source_version: None,
        search_text: bead_search_text(issue)?,
        metadata,
        created_at: None,
        updated_at: None,
    })
}

fn bead_payload(
    issue: &IssueWire,
    source_id: &str,
    existing_worker_agent_ids: &BTreeSet<String>,
) -> Result<ArtifactPayloadWire, String> {
    let mut payload = issue_field_value(issue)?;
    if let Some(worker_agent_id) = worker_agent_id(issue) {
        if !existing_worker_agent_ids.contains(worker_agent_id) {
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "pending_worker_agent_id".to_string(),
                    Value::String(worker_agent_id.to_string()),
                );
            }
        }
    }
    Ok(derived_payload(
        issue.id.clone(),
        "bead",
        ARTIFACT_SOURCE_BEAD_STORE,
        source_id,
        None,
        payload,
    ))
}

fn bead_subtitle(issue: &IssueWire) -> Result<String, String> {
    let status = issue_field_string(&issue.status)?;
    let issue_type = issue_field_string(&issue.issue_type)?;
    let tier = match &issue.tier {
        Some(tier) => issue_field_string(tier)?,
        None => issue_type.clone(),
    };
    Ok(format!("{status} {tier}"))
}

fn bead_search_text(issue: &IssueWire) -> Result<String, String> {
    let status = issue_field_string(&issue.status)?;
    let issue_type = issue_field_string(&issue.issue_type)?;
    let tier = issue
        .tier
        .as_ref()
        .map(issue_field_string)
        .transpose()?
        .unwrap_or_default();
    Ok([
        issue.id.as_str(),
        issue.title.as_str(),
        status.as_str(),
        issue_type.as_str(),
        tier.as_str(),
        issue.owner.as_str(),
        issue.assignee.as_str(),
        issue.changespec_name.as_str(),
        issue.changespec_bug_id.as_str(),
    ]
    .into_iter()
    .filter(|value| !value.is_empty())
    .collect::<Vec<_>>()
    .join(" "))
}

fn issue_field_value<T: serde::Serialize>(value: &T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|e| e.to_string())
}

fn issue_field_string<T: serde::Serialize>(
    value: &T,
) -> Result<String, String> {
    match issue_field_value(value)? {
        Value::String(value) => Ok(value),
        value => Ok(value.to_string()),
    }
}

fn worker_agent_id(issue: &IssueWire) -> Option<&str> {
    let assignee = issue.assignee.trim();
    (!assignee.is_empty()).then_some(assignee)
}

fn reconcile_cross_source_links(
    store: &mut ArtifactStore,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations =
        ArtifactIngestMutations::new("reconcile_cross_source_links");
    mutations.merge(reconcile_bead_links(store)?);
    mutations.merge(reconcile_agent_links(store)?);
    Ok(mutations.into_result())
}

fn reconcile_bead_links(
    store: &mut ArtifactStore,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("reconcile_bead_links");
    for mut payload in load_derived_payloads(store, "bead")? {
        if !artifact_node_visible_with_kind(
            store,
            &payload.artifact_id,
            ARTIFACT_KIND_BEAD,
        )? {
            continue;
        }

        let original_payload = payload.payload.clone();
        let source_id = payload.source_id.clone().unwrap_or_default();
        let mut unresolved_related = BTreeSet::new();

        if let Some(worker_id) = payload
            .payload
            .get("pending_worker_agent_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
        {
            if artifact_node_visible_with_kind(
                store,
                &worker_id,
                ARTIFACT_KIND_AGENT,
            )? {
                mutations.merge(upsert_derived_link(
                    store,
                    ARTIFACT_LINK_WORKER,
                    &payload.artifact_id,
                    &worker_id,
                    ARTIFACT_SOURCE_BEAD_STORE,
                    &source_id,
                )?);
                remove_payload_key(
                    &mut payload.payload,
                    "pending_worker_agent_id",
                );
            }
        }

        if let Some(changespec_name) = payload
            .payload
            .get("changespec_name")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
        {
            if artifact_node_visible_with_kind(
                store,
                &changespec_name,
                ARTIFACT_KIND_CHANGESPEC,
            )? {
                mutations.merge(upsert_derived_link(
                    store,
                    ARTIFACT_LINK_RELATED,
                    &payload.artifact_id,
                    &changespec_name,
                    ARTIFACT_SOURCE_BEAD_STORE,
                    &source_id,
                )?);
            } else {
                unresolved_related.insert(changespec_name);
            }
        }

        replace_payload_string_array(
            &mut payload.payload,
            "unresolved_related",
            unresolved_related,
        );
        if payload.payload != original_payload {
            mutations.merge(upsert_derived_payload(store, payload)?);
        }
    }
    Ok(mutations.into_result())
}

fn reconcile_agent_links(
    store: &mut ArtifactStore,
) -> Result<ArtifactMutationResultWire, String> {
    let mut mutations = ArtifactIngestMutations::new("reconcile_agent_links");
    let mut payloads = load_derived_payloads(store, AGENT_PAYLOAD_TYPE)?;
    payloads.retain(|payload| {
        artifact_node_visible_with_kind(
            store,
            &payload.artifact_id,
            ARTIFACT_KIND_AGENT,
        )
        .unwrap_or(false)
    });
    let ids_by_timestamp = agent_payload_ids_by_timestamp(&payloads);

    for mut payload in payloads {
        let original_payload = payload.payload.clone();
        let source_id = payload.source_id.clone().unwrap_or_default();
        let mut unresolved_related = BTreeSet::new();

        for changespec_id in agent_payload_changespec_targets(&payload.payload)
        {
            if changespec_id == payload.artifact_id {
                continue;
            }
            if artifact_node_visible_with_kind(
                store,
                &changespec_id,
                ARTIFACT_KIND_CHANGESPEC,
            )? {
                mutations.merge(upsert_derived_link(
                    store,
                    ARTIFACT_LINK_RELATED,
                    &payload.artifact_id,
                    &changespec_id,
                    ARTIFACT_SOURCE_AGENT_ARTIFACT,
                    &source_id,
                )?);
            } else {
                unresolved_related.insert(changespec_id);
            }
        }

        for timestamp in agent_payload_timestamp_refs(&payload.payload) {
            match ids_by_timestamp.get(&timestamp) {
                Some(ids) if ids.len() == 1 => {
                    let target_id = ids.iter().next().unwrap();
                    if target_id != &payload.artifact_id
                        && artifact_node_visible_with_kind(
                            store,
                            target_id,
                            ARTIFACT_KIND_AGENT,
                        )?
                    {
                        mutations.merge(upsert_derived_link(
                            store,
                            ARTIFACT_LINK_RELATED,
                            &payload.artifact_id,
                            target_id,
                            ARTIFACT_SOURCE_AGENT_ARTIFACT,
                            &source_id,
                        )?);
                    }
                }
                Some(ids) => {
                    unresolved_related.insert(format!(
                        "ambiguous timestamp {timestamp}: {}",
                        ids.iter().cloned().collect::<Vec<_>>().join(", ")
                    ));
                }
                None => {
                    unresolved_related.insert(timestamp);
                }
            }
        }

        replace_payload_string_array(
            &mut payload.payload,
            "unresolved_related",
            unresolved_related,
        );
        if payload.payload != original_payload {
            mutations.merge(upsert_derived_payload(store, payload)?);
        }
    }
    Ok(mutations.into_result())
}

fn load_derived_payloads(
    store: &ArtifactStore,
    payload_type: &str,
) -> Result<Vec<ArtifactPayloadWire>, String> {
    let mut stmt = store
        .connection()
        .prepare(
            r#"
            SELECT payload_json
            FROM artifact_payloads
            WHERE provenance = ?1
              AND payload_type = ?2
            ORDER BY artifact_id, source_kind, source_id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params![ARTIFACT_PROVENANCE_DERIVED, payload_type], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| e.to_string())?;
    let mut payloads = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        payloads.push(serde_json::from_str(&json).map_err(|e| e.to_string())?);
    }
    Ok(payloads)
}

fn agent_payload_ids_by_timestamp(
    payloads: &[ArtifactPayloadWire],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for payload in payloads {
        if let Some(timestamp) = payload
            .payload
            .get("record")
            .and_then(|record| record.get("timestamp"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            out.entry(timestamp.to_string())
                .or_default()
                .insert(payload.artifact_id.clone());
        }
    }
    out
}

fn agent_payload_changespec_targets(payload: &Value) -> BTreeSet<String> {
    let mut targets = BTreeSet::new();
    let Some(record) = payload.get("record") else {
        return targets;
    };
    for path in [
        &["done", "cl_name"][..],
        &["running", "cl_name"][..],
        &["workflow_state", "cl_name"][..],
    ] {
        if let Some(value) = json_path_string(record, path) {
            targets.insert(value);
        }
    }
    if let Some(meta) = record.get("agent_meta") {
        for key in ["changespec_name", "cl_name", "commit_changespec_name"] {
            push_json_string(&mut targets, meta.get(key));
        }
    }
    targets
}

fn agent_payload_timestamp_refs(payload: &Value) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    let Some(record) = payload.get("record") else {
        return refs;
    };
    if let Some(meta) = record.get("agent_meta") {
        for key in [
            "parent_timestamp",
            "parent_agent_timestamp",
            "retry_of_timestamp",
            "retried_as_timestamp",
            "retry_chain_root_timestamp",
        ] {
            push_json_string(&mut refs, meta.get(key));
        }
        for key in [
            "plan_submitted_at",
            "feedback_submitted_at",
            "questions_submitted_at",
            "retry_started_at",
        ] {
            push_json_string_array(&mut refs, meta.get(key));
        }
    }
    if let Some(done) = record.get("done") {
        for key in ["retried_as_timestamp", "retry_chain_root_timestamp"] {
            push_json_string(&mut refs, done.get(key));
        }
    }
    refs
}

fn json_path_string(root: &Value, path: &[&str]) -> Option<String> {
    let mut current = root;
    for key in path {
        current = current.get(*key)?;
    }
    current
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

fn push_json_string(values: &mut BTreeSet<String>, value: Option<&Value>) {
    if let Some(value) = value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        values.insert(value.to_string());
    }
}

fn push_json_string_array(
    values: &mut BTreeSet<String>,
    value: Option<&Value>,
) {
    if let Some(items) = value.and_then(Value::as_array) {
        for item in items {
            push_json_string(values, Some(item));
        }
    }
}

fn replace_payload_string_array(
    payload: &mut Value,
    key: &str,
    values: BTreeSet<String>,
) {
    let Value::Object(object) = payload else {
        return;
    };
    if values.is_empty() {
        object.remove(key);
    } else {
        object.insert(
            key.to_string(),
            Value::Array(values.into_iter().map(Value::String).collect()),
        );
    }
}

fn remove_payload_key(payload: &mut Value, key: &str) {
    if let Value::Object(object) = payload {
        object.remove(key);
    }
}

fn upsert_derived_link(
    store: &mut ArtifactStore,
    link_type: &str,
    source_id: &str,
    target_id: &str,
    source_kind: &str,
    source_id_hint: &str,
) -> Result<ArtifactMutationResultWire, String> {
    let link = ArtifactLinkWire {
        id: deterministic_artifact_link_id(link_type, source_id, target_id),
        link_type: link_type.to_string(),
        source_id: source_id.to_string(),
        target_id: target_id.to_string(),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(source_kind.to_string()),
        source_id_hint: Some(source_id_hint.to_string()),
        source_version: None,
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

fn artifact_node_exists(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<bool, String> {
    let exists: Option<i64> = store
        .connection()
        .query_row(
            "SELECT 1 FROM artifacts WHERE id = ?1",
            [artifact_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(exists.is_some())
}

fn artifact_node_exists_with_kind(
    store: &ArtifactStore,
    artifact_id: &str,
    kind: &str,
) -> Result<bool, String> {
    let exists: Option<i64> = store
        .connection()
        .query_row(
            "SELECT 1 FROM artifacts WHERE id = ?1 AND kind = ?2",
            params![artifact_id, kind],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(exists.is_some())
}

fn artifact_node_visible_with_kind(
    store: &ArtifactStore,
    artifact_id: &str,
    kind: &str,
) -> Result<bool, String> {
    let exists: bool = store
        .connection()
        .query_row(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM artifacts a
                WHERE a.id = ?1
                  AND a.kind = ?2
                  AND NOT EXISTS (
                      SELECT 1 FROM manual_tombstones t
                      WHERE t.tombstone_type = ?3
                        AND t.artifact_id = a.id
                  )
            )
            "#,
            params![artifact_id, kind, ARTIFACT_TOMBSTONE_NODE],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    Ok(exists)
}

fn project_ingestion_selected(
    request: &ResolvedArtifactRebuildRequest,
) -> bool {
    source_selected(request, ARTIFACT_SOURCE_PROJECT_FILE)
        || source_selected(request, ARTIFACT_SOURCE_CHANGESPEC)
        || source_selected(request, ARTIFACT_SOURCE_COMMIT)
}

fn agent_ingestion_selected(request: &ResolvedArtifactRebuildRequest) -> bool {
    source_selected(request, ARTIFACT_SOURCE_AGENT_ARTIFACT)
        || source_selected(request, ARTIFACT_SOURCE_AGENT_CREATED_FILE)
        || source_selected(request, ARTIFACT_SOURCE_AGENT_THOUGHT)
}

fn upsert_directory_path(
    store: &mut ArtifactStore,
    path: &Path,
) -> Result<ArtifactMutationResultWire, String> {
    artifact_upsert_path(
        store,
        path,
        ArtifactPathUpsertRequestWire {
            kind: Some(ARTIFACT_KIND_DIRECTORY.to_string()),
            provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
            source_kind: Some(ARTIFACT_SOURCE_DIRECTORY.to_string()),
            source_id: Some(path_to_artifact_id(path)),
            ..ArtifactPathUpsertRequestWire::default()
        },
    )
}

fn sorted_read_dir(path: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut entries = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    Ok(entries)
}

fn is_project_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension == "gp")
}

fn project_file_node(
    path: &Path,
    source_version: Option<String>,
) -> Result<ArtifactNodeWire, String> {
    let normalized = normalize_artifact_path(path)?;
    let id = path_to_artifact_id(&normalized);
    let mut metadata = Map::new();
    metadata.insert(
        "project_name".to_string(),
        Value::String(project_name_for_file(&normalized)),
    );
    metadata.insert(
        "file_type".to_string(),
        Value::String(project_file_type(&normalized).to_string()),
    );
    metadata.insert("source_path".to_string(), Value::String(id.clone()));
    if let Some((size, mtime_unix)) = file_size_and_mtime(&normalized) {
        metadata.insert("size".to_string(), json!(size));
        metadata.insert("mtime_unix".to_string(), json!(mtime_unix));
    }

    Ok(ArtifactNodeWire {
        id: id.clone(),
        kind: ARTIFACT_KIND_PROJECT.to_string(),
        display_title: normalized
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(id.as_str())
            .to_string(),
        subtitle: Some(project_name_for_file(&normalized)),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(ARTIFACT_SOURCE_PROJECT_FILE.to_string()),
        source_id: Some(id.clone()),
        source_version,
        search_text: format!(
            "{} {} {} {}",
            project_name_for_file(&normalized),
            normalized.display(),
            project_file_type(&normalized),
            id
        ),
        metadata,
        created_at: None,
        updated_at: None,
    })
}

fn changespec_node(
    spec: &ChangeSpecWire,
    source_id: &str,
    source_version: Option<String>,
) -> ArtifactNodeWire {
    let mut metadata = Map::new();
    metadata.insert("status".to_string(), Value::String(spec.status.clone()));
    metadata.insert(
        "project".to_string(),
        Value::String(spec.project_basename.clone()),
    );
    metadata.insert("source_path".to_string(), Value::String(source_id.into()));
    if let Some(parent) = &spec.parent {
        metadata.insert("parent".to_string(), Value::String(parent.clone()));
    }
    if let Some(cl_or_pr) = &spec.cl_or_pr {
        metadata
            .insert("cl_or_pr".to_string(), Value::String(cl_or_pr.clone()));
    }
    if let Some(bug) = &spec.bug {
        metadata.insert("bug".to_string(), Value::String(bug.clone()));
    }

    ArtifactNodeWire {
        id: spec.name.clone(),
        kind: ARTIFACT_KIND_CHANGESPEC.to_string(),
        display_title: spec.name.clone(),
        subtitle: Some(format!("{} {}", spec.status, spec.project_basename)),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(ARTIFACT_SOURCE_CHANGESPEC.to_string()),
        source_id: Some(source_id.to_string()),
        source_version,
        search_text: changespec_search_text(spec),
        metadata,
        created_at: None,
        updated_at: None,
    }
}

fn commit_node(
    spec: &ChangeSpecWire,
    commit: &CommitWire,
    source_id: &str,
    source_version: Option<String>,
) -> ArtifactNodeWire {
    let id = format!("{}:{}", spec.name, commit.number);
    let mut metadata = Map::new();
    metadata.insert("changespec".to_string(), Value::String(spec.name.clone()));
    metadata.insert("number".to_string(), json!(commit.number));
    metadata.insert("source_path".to_string(), Value::String(source_id.into()));

    ArtifactNodeWire {
        id,
        kind: ARTIFACT_KIND_COMMIT.to_string(),
        display_title: format!("{} commit {}", spec.name, commit.number),
        subtitle: Some(commit.note.clone()),
        provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
        source_kind: Some(ARTIFACT_SOURCE_COMMIT.to_string()),
        source_id: Some(source_id.to_string()),
        source_version,
        search_text: commit_search_text(spec, commit),
        metadata,
        created_at: None,
        updated_at: None,
    }
}

fn project_file_payload(path: &Path) -> Result<Value, String> {
    let normalized = normalize_artifact_path(path)?;
    let (size, mtime_unix) =
        file_size_and_mtime(&normalized).unwrap_or_default();
    Ok(json!({
        "project_name": project_name_for_file(&normalized),
        "file_type": project_file_type(&normalized),
        "source_path": path_to_artifact_id(&normalized),
        "size": size,
        "mtime_unix": mtime_unix,
    }))
}

fn changespec_search_text(spec: &ChangeSpecWire) -> String {
    let mut parts = vec![
        get_searchable_text(spec),
        spec.project_basename.clone(),
        spec.file_path.clone(),
    ];
    if let Some(bug) = &spec.bug {
        parts.push(bug.clone());
    }
    parts.join("\n")
}

fn commit_search_text(spec: &ChangeSpecWire, commit: &CommitWire) -> String {
    let mut parts = vec![
        spec.name.clone(),
        spec.project_basename.clone(),
        commit.number.to_string(),
        display_commit_note(&commit.note).to_string(),
    ];
    parts.extend(commit.body.clone());
    for value in [
        commit.chat.as_deref(),
        commit.diff.as_deref(),
        commit.plan.as_deref(),
        commit.proposal_letter.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        parts.push(value.to_string());
    }
    parts.join("\n")
}

fn display_commit_note(note: &str) -> &str {
    note.strip_prefix("[run] ").unwrap_or(note)
}

fn project_name_for_file(path: &Path) -> String {
    path.parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

fn project_file_type(path: &Path) -> &'static str {
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with("-archive.gp"))
    {
        "archive"
    } else {
        "current"
    }
}

fn file_source_version(path: &Path) -> Option<String> {
    file_size_and_mtime(path)
        .map(|(size, mtime_unix)| format!("{size}:{mtime_unix}"))
}

fn file_size_and_mtime(path: &Path) -> Option<(u64, u64)> {
    let metadata = fs::metadata(path).ok()?;
    let mtime_unix = metadata
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs();
    Some((metadata.len(), mtime_unix))
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

fn selected_stale_source_kinds(
    request: &ResolvedArtifactRebuildRequest,
) -> Vec<&'static str> {
    [
        ARTIFACT_SOURCE_PROJECT_FILE,
        ARTIFACT_SOURCE_CHANGESPEC,
        ARTIFACT_SOURCE_COMMIT,
        ARTIFACT_SOURCE_BEAD_STORE,
        ARTIFACT_SOURCE_AGENT_ARTIFACT,
        ARTIFACT_SOURCE_AGENT_CREATED_FILE,
        ARTIFACT_SOURCE_AGENT_THOUGHT,
        ARTIFACT_SOURCE_DIRECTORY,
    ]
    .into_iter()
    .filter(|source_kind| source_selected(request, source_kind))
    .collect()
}

fn project_source_kind(source_kind: &str) -> bool {
    matches!(
        source_kind,
        ARTIFACT_SOURCE_PROJECT_FILE
            | ARTIFACT_SOURCE_CHANGESPEC
            | ARTIFACT_SOURCE_COMMIT
    )
}

fn discover_selected_project_files(
    request: &ResolvedArtifactRebuildRequest,
) -> Vec<PathBuf> {
    if let Some(target_path) = request.target_path.as_deref() {
        if target_path.exists() && is_project_file(target_path) {
            return vec![target_path.to_path_buf()];
        }
        return Vec::new();
    }
    let Some(projects_root) = request.projects_root.as_deref() else {
        return Vec::new();
    };
    let mut mutations = ArtifactIngestMutations::new("discover_project_files");
    discover_project_files(projects_root, &mut mutations)
}

fn discover_selected_agent_records(
    request: &ResolvedArtifactRebuildRequest,
) -> Vec<AgentArtifactRecordWire> {
    let Some(projects_root) = request.projects_root.as_deref() else {
        return Vec::new();
    };
    let options = AgentArtifactScanOptionsWire::default();
    if let Some(artifact_dir) = request.artifact_dir.as_deref() {
        scan_agent_artifact_dir(projects_root, artifact_dir, &options)
            .into_iter()
            .collect()
    } else {
        scan_agent_artifacts(projects_root, options).records
    }
}

fn target_matches_bead_store(
    request: &ResolvedArtifactRebuildRequest,
    target_path: &Path,
) -> bool {
    request.beads_dir.as_deref().is_some_and(|beads_dir| {
        target_path == beads_dir
            || target_path.starts_with(beads_dir)
            || target_path.file_name().and_then(|name| name.to_str())
                == Some("issues.jsonl")
    })
}

fn existing_watermark_source_ids(
    store: &ArtifactStore,
    source_kind: &str,
) -> Result<BTreeSet<String>, String> {
    let mut stmt = store
        .connection()
        .prepare(
            "SELECT source_id FROM source_watermarks WHERE source_kind = ?1 ORDER BY source_id",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([source_kind], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<BTreeSet<_>, _>>()
        .map_err(|e| e.to_string())
}

fn existing_source_ids_for_hint(
    store: &ArtifactStore,
    source_kind: &str,
    source_id_hint: &str,
) -> Result<BTreeSet<String>, String> {
    let mut ids = existing_watermark_source_ids(store, source_kind)?;
    let mut stmt = store
        .connection()
        .prepare(
            r#"
            SELECT DISTINCT target_id
            FROM artifact_links
            WHERE provenance = ?1
              AND source_kind = ?2
              AND source_id_hint = ?3
            ORDER BY target_id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![ARTIFACT_PROVENANCE_DERIVED, source_kind, source_id_hint],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| e.to_string())?;
    for row in rows {
        ids.insert(row.map_err(|e| e.to_string())?);
    }
    Ok(ids)
}

fn source_watermark(source_id: &str) -> String {
    if let Some((size, mtime_unix)) = file_size_and_mtime(Path::new(source_id))
    {
        format!("{size}:{mtime_unix}")
    } else {
        "present".to_string()
    }
}

fn stale_node_ids(
    tx: &rusqlite::Transaction<'_>,
    source_kind: &str,
    source_id: &str,
) -> Result<Vec<String>, String> {
    let mut stmt = tx
        .prepare(
            r#"
            SELECT id
            FROM artifacts
            WHERE provenance = ?1
              AND source_kind = ?2
              AND source_id = ?3
            ORDER BY id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![ARTIFACT_PROVENANCE_DERIVED, source_kind, source_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

fn stale_link_ids(
    tx: &rusqlite::Transaction<'_>,
    source_kind: &str,
    source_id: &str,
) -> Result<Vec<String>, String> {
    let mut stmt = tx
        .prepare(
            r#"
            SELECT id
            FROM artifact_links
            WHERE provenance = ?1
              AND source_kind = ?2
              AND source_id_hint = ?3
            ORDER BY id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![ARTIFACT_PROVENANCE_DERIVED, source_kind, source_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

fn insert_stale_node_tombstone(
    tx: &rusqlite::Transaction<'_>,
    artifact_id: &str,
    source_kind: &str,
    source_id: &str,
) -> Result<bool, String> {
    insert_stale_tombstone(
        tx,
        ARTIFACT_TOMBSTONE_NODE,
        Some(artifact_id),
        None,
        source_kind,
        source_id,
    )
}

fn insert_stale_link_tombstone(
    tx: &rusqlite::Transaction<'_>,
    link_id: &str,
    source_kind: &str,
    source_id: &str,
) -> Result<bool, String> {
    insert_stale_tombstone(
        tx,
        ARTIFACT_TOMBSTONE_LINK,
        None,
        Some(link_id),
        source_kind,
        source_id,
    )
}

fn insert_stale_tombstone(
    tx: &rusqlite::Transaction<'_>,
    tombstone_type: &str,
    artifact_id: Option<&str>,
    link_id: Option<&str>,
    source_kind: &str,
    source_id: &str,
) -> Result<bool, String> {
    let exists: bool = tx
        .query_row(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM manual_tombstones
                WHERE tombstone_type = ?1
                  AND COALESCE(artifact_id, '') = COALESCE(?2, '')
                  AND COALESCE(link_id, '') = COALESCE(?3, '')
                  AND reason = ?4
            )
            "#,
            params![tombstone_type, artifact_id, link_id, STALE_DERIVED_REASON],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    if exists {
        return Ok(false);
    }
    tx.execute(
        r#"
        INSERT INTO manual_tombstones (
            tombstone_type, artifact_id, link_id, source_kind, source_id, reason
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            tombstone_type,
            artifact_id,
            link_id,
            source_kind,
            source_id,
            STALE_DERIVED_REASON,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(true)
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
        metadata: request.metadata.clone().unwrap_or_default(),
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

    use super::super::export::{
        artifact_export_json, artifact_materialize_graph,
    };
    use super::super::query::{artifact_doctor, artifact_list, artifact_show};
    use super::super::store::{open_artifact_store, remove_artifact_node};
    use super::super::wire::{
        ArtifactDoctorOptionsWire, ArtifactGraphOptionsWire,
        ArtifactNodeRemoveWire, ArtifactNodeUpsertWire, ArtifactNodeWire,
        ArtifactQueryWire, ARTIFACT_KIND_AGENT, ARTIFACT_KIND_CHANGESPEC,
        ARTIFACT_KIND_COMMIT, ARTIFACT_KIND_FILE, ARTIFACT_KIND_THOUGHT,
        ARTIFACT_LINK_CREATED, ARTIFACT_LINK_PARENT, ARTIFACT_LINK_RELATED,
        ARTIFACT_LINK_WORKER, ARTIFACT_PROVENANCE_DERIVED,
        ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_TOMBSTONE_NODE,
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

    #[test]
    fn bead_ingestion_creates_parent_hierarchy_and_top_level_directory_link() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let beads_dir = tmp.path().join("workspace/sdd/beads");
        std::fs::create_dir_all(&beads_dir).unwrap();
        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-1","title":"Epic","status":"open","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"sdd/epics/epic.md","is_ready_to_work":true,"dependencies":[]}"#,
                r#"{"id":"sase-1.1","title":"Phase","status":"in_progress","issue_type":"phase","parent_id":"sase-1","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":false,"dependencies":[{"issue_id":"sase-1.1","depends_on_id":"sase-0","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com"}]}"#,
            ],
        );

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
                include_sources: vec![ARTIFACT_SOURCE_BEAD_STORE.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let child = artifact_show(&store, "sase-1.1").unwrap();
        assert_eq!(child.node.unwrap().kind, ARTIFACT_KIND_BEAD);
        assert!(child.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_PARENT
                && link.source_id == "sase-1.1"
                && link.target_id == "sase-1"
        }));
        assert_eq!(
            child.payloads[0].payload["dependencies"][0]["depends_on_id"],
            "sase-0"
        );

        let top = artifact_show(&store, "sase-1").unwrap();
        assert!(top.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_PARENT
                && link.source_id == "sase-1"
                && link.target_id == beads_dir.to_string_lossy()
        }));
    }

    #[test]
    fn bead_worker_link_targets_existing_agent_and_records_pending_target() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let beads_dir = tmp.path().join("workspace/sdd/beads");
        std::fs::create_dir_all(&beads_dir).unwrap();
        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-2","title":"Epic","status":"open","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":true,"dependencies":[]}"#,
                r#"{"id":"sase-2.1","title":"Linked phase","status":"in_progress","issue_type":"phase","parent_id":"sase-2","owner":"owner@example.com","assignee":"sase-2.1","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":false,"dependencies":[]}"#,
                r#"{"id":"sase-2.2","title":"Pending phase","status":"in_progress","issue_type":"phase","parent_id":"sase-2","owner":"owner@example.com","assignee":"sase-2.2","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":false,"dependencies":[]}"#,
            ],
        );

        let mut store = open_artifact_store(&db).unwrap();
        upsert_node(
            &mut store,
            manual_node("sase-2.1", ARTIFACT_KIND_AGENT, "phase agent"),
        );
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
                include_sources: vec![ARTIFACT_SOURCE_BEAD_STORE.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let linked = artifact_show(&store, "sase-2.1").unwrap();
        assert!(linked.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_WORKER
                && link.source_id == "sase-2.1"
                && link.target_id == "sase-2.1"
        }));

        let pending = artifact_show(&store, "sase-2.2").unwrap();
        assert_eq!(
            pending.payloads[0].payload["pending_worker_agent_id"],
            "sase-2.2"
        );
    }

    #[test]
    fn bead_changespec_metadata_links_only_to_existing_changespec() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let beads_dir = tmp.path().join("workspace/sdd/beads");
        std::fs::create_dir_all(&beads_dir).unwrap();
        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-3","title":"Plan","status":"open","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":true,"changespec_name":"cl-one","changespec_bug_id":"123","dependencies":[]}"#,
            ],
        );

        let mut store = open_artifact_store(&db).unwrap();
        upsert_node(
            &mut store,
            manual_node("cl-one", ARTIFACT_KIND_CHANGESPEC, "cl one"),
        );
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
                include_sources: vec![ARTIFACT_SOURCE_BEAD_STORE.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let detail = artifact_show(&store, "sase-3").unwrap();
        assert!(detail.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == "sase-3"
                && link.target_id == "cl-one"
        }));
        assert!(detail.node.unwrap().search_text.contains("123"));
    }

    #[test]
    fn missing_bead_store_records_scoped_error_without_aborting() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let missing_beads_dir = tmp.path().join("workspace/sdd/beads");

        let mut store = open_artifact_store(&db).unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                beads_dir: Some(
                    missing_beads_dir.to_string_lossy().into_owned(),
                ),
                include_sources: vec![ARTIFACT_SOURCE_BEAD_STORE.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result
            .errors
            .iter()
            .any(|error| error.contains("bead_store")));
        assert!(artifact_show(&store, missing_beads_dir.to_str().unwrap())
            .unwrap()
            .node
            .is_some());
    }

    #[test]
    fn agent_ingestion_creates_agent_created_files_and_changespec_link() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let artifact_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let chat_path = tmp.path().join("chats/agent-alpha.md");
        let diff_path = artifact_dir.join("commit.diff");
        let plan_path = artifact_dir.join("plan.md");
        let response_path = artifact_dir.join("response.md");
        let pdf_path = artifact_dir.join("summary.pdf");
        for path in [
            &chat_path,
            &diff_path,
            &plan_path,
            &response_path,
            &pdf_path,
        ] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "artifact").unwrap();
        }
        std::fs::write(artifact_dir.join("raw_xprompt.md"), "#xprompt")
            .unwrap();
        std::fs::write(artifact_dir.join("live_reply.md"), "reply").unwrap();
        std::fs::write(artifact_dir.join("qa_log.jsonl"), "{}\n").unwrap();
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({
                "name": "agent-alpha",
                "model": "gpt-test",
                "llm_provider": "codex",
                "chat_path": chat_path,
                "run_started_at": "2026-05-05T12:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("done.json"),
            json!({
                "name": "agent-alpha",
                "cl_name": "cl-one",
                "outcome": "completed",
                "diff_path": diff_path,
                "plan_path": plan_path,
                "response_path": response_path,
                "markdown_pdf_paths": [pdf_path]
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("prompt_step_001_code.json"),
            json!({
                "workflow_name": "ace-run",
                "step_name": "code",
                "step_type": "agent",
                "status": "completed",
                "response_path": response_path,
                "output": {"diff_path": diff_path},
                "output_types": {"diff_path": "path"}
            })
            .to_string(),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        upsert_node(
            &mut store,
            manual_node("cl-one", ARTIFACT_KIND_CHANGESPEC, "cl one"),
        );
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(
                    projects_root.to_string_lossy().into_owned(),
                ),
                workspace_root: None,
                beads_dir: None,
                include_sources: vec![
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string(),
                    ARTIFACT_SOURCE_AGENT_CREATED_FILE.to_string(),
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        let agent = artifact_show(&store, "agent-alpha").unwrap();
        assert_eq!(
            agent.node.as_ref().map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_AGENT)
        );
        assert!(agent.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == "agent-alpha"
                && link.target_id == "cl-one"
        }));
        assert!(agent.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_CREATED
                && link.source_id == "agent-alpha"
                && link.target_id == diff_path.to_string_lossy()
        }));

        let diff = artifact_show(&store, diff_path.to_str().unwrap()).unwrap();
        assert_eq!(
            diff.node.as_ref().map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_FILE)
        );
        assert_eq!(
            diff.path_to_root.first().map(|node| node.id.as_str()),
            Some(diff_path.to_str().unwrap())
        );
    }

    #[test]
    fn agent_ingestion_uses_workflow_relationship_metadata() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let artifact_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let plan_path = artifact_dir.join("plan.md");
        let sdd_plan_path =
            tmp.path().join("workspace/sdd/tales/202605/plan.md");
        let question_path =
            tmp.path().join("questions/session/question_response.json");
        let diff_path = artifact_dir.join("commit.diff");
        for path in [&plan_path, &sdd_plan_path, &question_path, &diff_path] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "artifact").unwrap();
        }
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({
                "name": "agent-alpha",
                "changespec_name": "cl-one",
                "phase_bead_id": "sase-9.1",
                "plan_path": plan_path,
                "sdd_plan_path": sdd_plan_path,
                "question_response_path": question_path,
                "commit_changespec_name": "cl-one",
                "commit_entry_id": "7",
                "commit_diff_path": diff_path
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("commit_result.json"),
            json!({
                "commit_changespec_name": "cl-one",
                "commit_entry_id": "7",
                "commit_diff_path": diff_path
            })
            .to_string(),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        upsert_node(
            &mut store,
            manual_node("cl-one", ARTIFACT_KIND_CHANGESPEC, "cl one"),
        );
        upsert_node(
            &mut store,
            manual_node("cl-one:7", ARTIFACT_KIND_COMMIT, "commit 7"),
        );
        upsert_node(
            &mut store,
            manual_node("sase-9.1", ARTIFACT_KIND_BEAD, "phase"),
        );

        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(
                    projects_root.to_string_lossy().into_owned(),
                ),
                workspace_root: None,
                beads_dir: None,
                include_sources: vec![
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string(),
                    ARTIFACT_SOURCE_AGENT_CREATED_FILE.to_string(),
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let agent = artifact_show(&store, "agent-alpha").unwrap();
        for target in ["cl-one", "cl-one:7", "sase-9.1"] {
            assert!(agent.outbound_links.iter().any(|link| {
                link.link_type == ARTIFACT_LINK_RELATED
                    && link.source_id == "agent-alpha"
                    && link.target_id == target
            }));
        }
        for path in [&plan_path, &sdd_plan_path, &question_path, &diff_path] {
            let id = path.to_string_lossy();
            assert!(agent.outbound_links.iter().any(|link| {
                link.link_type == ARTIFACT_LINK_CREATED
                    && link.source_id == "agent-alpha"
                    && link.target_id == id.as_ref()
            }));
        }
    }

    #[test]
    fn agent_ingestion_uses_fallback_id_and_links_retry_timestamp() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let parent_dir =
            projects_root.join("acme/artifacts/ace-run/20260505115900");
        let retry_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        std::fs::create_dir_all(&parent_dir).unwrap();
        std::fs::create_dir_all(&retry_dir).unwrap();
        std::fs::write(
            parent_dir.join("agent_meta.json"),
            json!({"name": "retry-parent"}).to_string(),
        )
        .unwrap();
        std::fs::write(
            retry_dir.join("agent_meta.json"),
            json!({
                "retry_of_timestamp": "20260505115900",
                "questions_submitted_at": ["20260505000000"]
            })
            .to_string(),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(
                    projects_root.to_string_lossy().into_owned(),
                ),
                workspace_root: None,
                beads_dir: None,
                include_sources: vec![
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string()
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let fallback_id = "agent:acme:ace-run:20260505120000";
        let retry = artifact_show(&store, fallback_id).unwrap();
        assert_eq!(
            retry.node.as_ref().map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_AGENT)
        );
        let metadata = &retry.node.as_ref().unwrap().metadata;
        assert_eq!(metadata["uses_fallback_agent_id"], json!(true));
        assert_eq!(metadata["fallback_agent_id"], json!(fallback_id));
        assert_eq!(
            metadata["source_artifact_dir"],
            json!(retry_dir.to_string_lossy())
        );
        assert_eq!(
            retry.payloads[0].payload["agent_id_source"],
            json!("fallback")
        );
        assert!(retry.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == fallback_id
                && link.target_id == "retry-parent"
        }));
        let doctor =
            artifact_doctor(&store, ArtifactDoctorOptionsWire::default())
                .unwrap();
        assert!(doctor.issues.iter().any(|issue| {
            issue.issue_type == "fallback_agent_id"
                && issue.artifact_id.as_deref() == Some(fallback_id)
        }));
        assert!(doctor.issues.iter().any(|issue| {
            issue.issue_type == "unresolved_timestamp_link"
                && issue.artifact_id.as_deref() == Some(fallback_id)
        }));
    }

    #[test]
    fn agent_thought_ingestion_creates_codex_thoughts_and_created_links() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let artifact_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({
                "name": "codex-agent",
                "llm_provider": "codex"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("codex_thinking.jsonl"),
            [
                json!({
                    "text": "first codex thought",
                    "timestamp": "2026-05-05T12:00:00Z"
                })
                .to_string(),
                json!({
                    "text": "second codex thought",
                    "timestamp": "2026-05-05T12:01:00Z",
                    "following_action": "Read ingest.rs"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(
                    projects_root.to_string_lossy().into_owned(),
                ),
                include_sources: vec![
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string(),
                    ARTIFACT_SOURCE_AGENT_THOUGHT.to_string(),
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        let thoughts = artifact_list(
            &store,
            ArtifactQueryWire {
                kinds: vec![ARTIFACT_KIND_THOUGHT.to_string()],
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(thoughts.len(), 2);

        let agent = artifact_show(&store, "codex-agent").unwrap();
        let thought_ids: BTreeSet<_> =
            thoughts.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(
            agent
                .outbound_links
                .iter()
                .filter(|link| {
                    link.link_type == ARTIFACT_LINK_CREATED
                        && link.source_id == "codex-agent"
                        && thought_ids.contains(link.target_id.as_str())
                })
                .count(),
            2
        );
        let source_id = artifact_dir
            .join("codex_thinking.jsonl")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            artifact_show(&store, &source_id)
                .unwrap()
                .node
                .as_ref()
                .map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_FILE)
        );
        assert!(agent.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_CREATED
                && link.source_id == "codex-agent"
                && link.target_id == source_id
        }));
    }

    #[test]
    fn agent_thought_ingestion_parses_claude_transcript_following_action() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let artifact_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        let transcript = tmp.path().join("claude/session.jsonl");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(
            &transcript,
            [
                json!({
                    "type": "assistant",
                    "timestamp": "2026-05-05T12:00:00Z",
                    "message": {
                        "content": [
                            {"type": "thinking", "thinking": "inspect the Rust parser"}
                        ]
                    }
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "message": {
                        "content": [
                            {"type": "text", "text": "I will inspect the file first."}
                        ]
                    }
                })
                .to_string(),
                json!({
                    "type": "assistant",
                    "message": {
                        "content": [
                            {
                                "type": "tool_use",
                                "name": "Read",
                                "input": {"file_path": "/tmp/lib.rs"}
                            }
                        ]
                    }
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({
                "name": "claude-agent",
                "llm_provider": "claude",
                "claude_session_path": transcript.to_string_lossy()
            })
            .to_string(),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result =
            artifact_rebuild(
                &mut store,
                ArtifactRebuildRequestWire {
                    projects_root: Some(
                        projects_root.to_string_lossy().into_owned(),
                    ),
                    include_sources: vec![
                        ARTIFACT_SOURCE_AGENT_THOUGHT.to_string()
                    ],
                    ..ArtifactRebuildRequestWire::default()
                },
            )
            .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        let thoughts = artifact_list(
            &store,
            ArtifactQueryWire {
                kinds: vec![ARTIFACT_KIND_THOUGHT.to_string()],
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(thoughts.len(), 1);
        let detail = artifact_show(&store, &thoughts[0].id).unwrap();
        let payload = &detail.payloads[0].payload;
        assert_eq!(payload["source_provider"], "claude");
        assert_eq!(payload["text"], "inspect the Rust parser");
        assert_eq!(payload["following_action"], "Read lib.rs");
        assert!(detail.inbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_CREATED
                && link.source_id == "claude-agent"
        }));
    }

    #[test]
    fn agent_thought_ids_do_not_collide_for_duplicate_text_sessions() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let artifact_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        let first = tmp.path().join("claude/first.jsonl");
        let second = tmp.path().join("claude/second.jsonl");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::create_dir_all(first.parent().unwrap()).unwrap();
        for path in [&first, &second] {
            std::fs::write(
                path,
                json!({
                    "type": "assistant",
                    "timestamp": "2026-05-05T12:00:00Z",
                    "message": {
                        "content": [
                            {"type": "thinking", "thinking": "same thought text"}
                        ]
                    }
                })
                .to_string(),
            )
            .unwrap();
        }
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({
                "name": "duplicate-agent",
                "claude_session_paths": [
                    first.to_string_lossy(),
                    second.to_string_lossy()
                ]
            })
            .to_string(),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(
                    projects_root.to_string_lossy().into_owned(),
                ),
                include_sources: vec![ARTIFACT_SOURCE_AGENT_THOUGHT.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let thoughts = artifact_list(
            &store,
            ArtifactQueryWire {
                kinds: vec![ARTIFACT_KIND_THOUGHT.to_string()],
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        let ids: BTreeSet<_> = thoughts.iter().map(|node| &node.id).collect();
        assert_eq!(thoughts.len(), 2);
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn agent_thought_ingestion_diagnoses_malformed_provider_lines() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let artifact_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({"name": "bad-thought-agent"}).to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("codex_thinking.jsonl"),
            "not json\n{\"text\":\"valid thought\"}\n{broken\n",
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result =
            artifact_rebuild(
                &mut store,
                ArtifactRebuildRequestWire {
                    projects_root: Some(
                        projects_root.to_string_lossy().into_owned(),
                    ),
                    include_sources: vec![
                        ARTIFACT_SOURCE_AGENT_THOUGHT.to_string()
                    ],
                    ..ArtifactRebuildRequestWire::default()
                },
            )
            .unwrap();

        assert!(result
            .errors
            .iter()
            .any(|error| error.contains("malformed Codex thinking line")));
        let thoughts = artifact_list(
            &store,
            ArtifactQueryWire {
                kinds: vec![ARTIFACT_KIND_THOUGHT.to_string()],
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(thoughts.len(), 1);
    }

    #[test]
    fn project_rebuild_ingests_current_archive_specs_and_commits() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        std::fs::create_dir_all(&project_dir).unwrap();
        let current = project_dir.join("myproj.gp");
        let archive = project_dir.join("myproj-archive.gp");
        std::fs::write(
            &current,
            "NAME: alpha\nDESCRIPTION: Build alpha.\nBUG: BUG-1\nSTATUS: WIP\nCOMMITS:\n  (1) [run] Initial commit\n\nNAME: beta\nDESCRIPTION: Build beta.\nPARENT: alpha\nPR: 42\nSTATUS: Ready\n",
        )
        .unwrap();
        std::fs::write(
            &archive,
            "NAME: archived_one\nDESCRIPTION: Old work.\nSTATUS: Archived\nCOMMITS:\n  (1) [run] Archive commit\n",
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                workspace_root: None,
                beads_dir: None,
                include_sources: vec![
                    ARTIFACT_SOURCE_DIRECTORY.to_string(),
                    ARTIFACT_SOURCE_PROJECT_FILE.to_string(),
                    ARTIFACT_SOURCE_CHANGESPEC.to_string(),
                    ARTIFACT_SOURCE_COMMIT.to_string(),
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        let commit_detail = artifact_show(&store, "alpha:1").unwrap();
        assert_eq!(
            commit_detail.node.as_ref().map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_COMMIT)
        );
        assert_eq!(
            commit_detail.payloads[0].payload["note"],
            json!("Initial commit")
        );
        let path_ids: Vec<_> = commit_detail
            .path_to_root
            .iter()
            .map(|node| node.id.as_str())
            .collect();
        assert_eq!(
            &path_ids[..3],
            vec!["alpha:1", "alpha", current.to_str().unwrap()]
        );
        assert_eq!(path_ids.last().copied(), Some(ARTIFACT_ROOT_ID));

        let archive_detail =
            artifact_show(&store, archive.to_str().unwrap()).unwrap();
        assert_eq!(
            archive_detail.node.as_ref().unwrap().metadata["file_type"],
            json!("archive")
        );
        assert!(artifact_show(&store, "archived_one:1")
            .unwrap()
            .node
            .is_some());
    }

    #[test]
    fn project_rebuild_reports_malformed_file_and_continues() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        std::fs::create_dir_all(&project_dir).unwrap();
        std::fs::write(
            project_dir.join("good.gp"),
            "NAME: good\nSTATUS: WIP\nCOMMITS:\n  (1) [run] Good commit\n",
        )
        .unwrap();
        std::fs::write(project_dir.join("bad.gp"), b"NAME: bad\n\xff\n")
            .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                workspace_root: None,
                beads_dir: None,
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(
            result.errors.iter().any(|error| error.contains("encoding")),
            "{result:?}"
        );
        assert!(artifact_show(&store, "good:1").unwrap().node.is_some());
    }

    #[test]
    fn duplicate_changespec_names_are_diagnosed_deterministically() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let first_dir = projects_root.join("a");
        let second_dir = projects_root.join("b");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = first_dir.join("one.gp");
        let second = second_dir.join("two.gp");
        std::fs::write(&first, "NAME: dup\nDESCRIPTION: first\nSTATUS: WIP\n")
            .unwrap();
        std::fs::write(
            &second,
            "NAME: dup\nDESCRIPTION: second\nSTATUS: WIP\n",
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                workspace_root: None,
                beads_dir: None,
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(
            result
                .errors
                .iter()
                .any(|error| error.contains("duplicate ChangeSpec")),
            "{result:?}"
        );
        let detail = artifact_show(&store, "dup").unwrap();
        assert_eq!(detail.payloads[0].payload["description"], json!("first"));
        assert_eq!(
            detail.node.as_ref().unwrap().source_id.as_deref(),
            Some(first.to_str().unwrap())
        );
    }

    #[test]
    fn project_reingest_updates_commit_payload_and_search_text() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        std::fs::create_dir_all(&project_dir).unwrap();
        let project_file = project_dir.join("myproj.gp");
        std::fs::write(
            &project_file,
            "NAME: alpha\nSTATUS: WIP\nCOMMITS:\n  (1) [run] Initial commit\n",
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let request = ArtifactRebuildRequestWire {
            projects_root: Some(projects_root.to_string_lossy().into()),
            workspace_root: None,
            beads_dir: None,
            ..ArtifactRebuildRequestWire::default()
        };
        artifact_rebuild(&mut store, request.clone()).unwrap();

        std::fs::write(
            &project_file,
            "NAME: alpha\nSTATUS: WIP\nCOMMITS:\n  (1) [run] Updated commit note\n",
        )
        .unwrap();
        let result = artifact_rebuild(&mut store, request).unwrap();
        assert!(result.nodes_updated > 0, "{result:?}");

        let detail = artifact_show(&store, "alpha:1").unwrap();
        assert_eq!(
            detail.payloads[0].payload["note"],
            json!("Updated commit note")
        );
        let matches = artifact_list(
            &store,
            ArtifactQueryWire {
                text: Some("Updated commit note".to_string()),
                kinds: vec![ARTIFACT_KIND_COMMIT.to_string()],
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "alpha:1");
    }

    #[test]
    fn targeted_project_file_upsert_updates_only_that_source() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        std::fs::create_dir_all(&project_dir).unwrap();
        let first = project_dir.join("first.gp");
        let second = project_dir.join("second.gp");
        std::fs::write(
            &first,
            "NAME: first\nSTATUS: WIP\nCOMMITS:\n  (1) First note\n",
        )
        .unwrap();
        std::fs::write(
            &second,
            "NAME: second\nSTATUS: WIP\nCOMMITS:\n  (1) Second note\n",
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let full = ArtifactRebuildRequestWire {
            projects_root: Some(projects_root.to_string_lossy().into()),
            workspace_root: None,
            beads_dir: None,
            include_sources: vec![
                ARTIFACT_SOURCE_PROJECT_FILE.to_string(),
                ARTIFACT_SOURCE_CHANGESPEC.to_string(),
                ARTIFACT_SOURCE_COMMIT.to_string(),
            ],
            ..ArtifactRebuildRequestWire::default()
        };
        artifact_rebuild(&mut store, full).unwrap();

        std::fs::write(
            &first,
            "NAME: first\nSTATUS: WIP\nCOMMITS:\n  (1) Updated first note\n",
        )
        .unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                workspace_root: None,
                beads_dir: None,
                include_sources: vec![
                    ARTIFACT_SOURCE_PROJECT_FILE.to_string(),
                    ARTIFACT_SOURCE_CHANGESPEC.to_string(),
                    ARTIFACT_SOURCE_COMMIT.to_string(),
                ],
                target_path: Some(first.to_string_lossy().into()),
                stale_cleanup: ARTIFACT_STALE_CLEANUP_MARK.to_string(),
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        assert_eq!(
            artifact_show(&store, "first:1").unwrap().payloads[0].payload
                ["note"],
            json!("Updated first note")
        );
        assert_eq!(
            artifact_show(&store, "second:1").unwrap().payloads[0].payload
                ["note"],
            json!("Second note")
        );
    }

    #[test]
    fn targeted_directory_upsert_adds_file_without_rescanning_sources() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let workspace_root = tmp.path().join("workspace");
        let existing_file = workspace_root.join("existing/output.txt");
        let target_file = workspace_root.join("nested/new/output.txt");
        std::fs::create_dir_all(existing_file.parent().unwrap()).unwrap();
        std::fs::create_dir_all(target_file.parent().unwrap()).unwrap();
        std::fs::write(&existing_file, "existing").unwrap();
        std::fs::write(&target_file, "new").unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                workspace_root: Some(workspace_root.to_string_lossy().into()),
                include_sources: vec![ARTIFACT_SOURCE_DIRECTORY.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();
        assert!(artifact_show(&store, existing_file.to_str().unwrap())
            .unwrap()
            .node
            .is_none());

        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                workspace_root: Some(workspace_root.to_string_lossy().into()),
                include_sources: vec![ARTIFACT_SOURCE_DIRECTORY.to_string()],
                target_path: Some(target_file.to_string_lossy().into()),
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        let target =
            artifact_show(&store, target_file.to_str().unwrap()).unwrap();
        assert_eq!(
            target.node.as_ref().map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_FILE)
        );
        assert_eq!(
            target
                .outbound_links
                .iter()
                .find(|link| link.link_type == ARTIFACT_LINK_PARENT)
                .map(|link| link.target_id.as_str()),
            Some(target_file.parent().unwrap().to_str().unwrap())
        );
        assert!(artifact_show(&store, existing_file.to_str().unwrap())
            .unwrap()
            .node
            .is_none());
    }

    #[test]
    fn targeted_bead_store_upsert_updates_beads_without_project_churn() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        let project_file = project_dir.join("acme.gp");
        let beads_dir = tmp.path().join("workspace/sdd/beads");
        std::fs::create_dir_all(&project_dir).unwrap();
        std::fs::create_dir_all(&beads_dir).unwrap();
        std::fs::write(
            &project_file,
            "NAME: cl-one\nSTATUS: WIP\nCOMMITS:\n  (1) Project note\n",
        )
        .unwrap();
        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-11","title":"Epic","status":"open","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":true,"dependencies":[]}"#,
            ],
        );

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
                include_sources: vec![
                    ARTIFACT_SOURCE_PROJECT_FILE.to_string(),
                    ARTIFACT_SOURCE_CHANGESPEC.to_string(),
                    ARTIFACT_SOURCE_COMMIT.to_string(),
                    ARTIFACT_SOURCE_BEAD_STORE.to_string(),
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-11","title":"Renamed Epic","status":"in_progress","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":true,"dependencies":[]}"#,
            ],
        );
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
                include_sources: vec![
                    ARTIFACT_SOURCE_PROJECT_FILE.to_string(),
                    ARTIFACT_SOURCE_CHANGESPEC.to_string(),
                    ARTIFACT_SOURCE_COMMIT.to_string(),
                    ARTIFACT_SOURCE_BEAD_STORE.to_string(),
                ],
                target_path: Some(
                    beads_dir.join("issues.jsonl").to_string_lossy().into(),
                ),
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        assert!(!result
            .affected_node_ids
            .iter()
            .any(|id| id == "cl-one" || id == "cl-one:1"));
        let bead = artifact_show(&store, "sase-11").unwrap();
        assert_eq!(bead.node.as_ref().unwrap().display_title, "Renamed Epic");
        assert_eq!(bead.payloads[0].payload["status"], json!("in_progress"));
        assert_eq!(
            artifact_show(&store, "cl-one:1").unwrap().payloads[0].payload
                ["note"],
            json!("Project note")
        );
    }

    #[test]
    fn targeted_agent_artifact_dir_upsert_updates_one_agent() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let first_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120000");
        let second_dir =
            projects_root.join("acme/artifacts/ace-run/20260505120100");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        std::fs::write(
            first_dir.join("agent_meta.json"),
            json!({"name": "agent-alpha", "model": "old-model"}).to_string(),
        )
        .unwrap();
        std::fs::write(
            second_dir.join("agent_meta.json"),
            json!({"name": "agent-beta", "model": "beta-model"}).to_string(),
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                include_sources: vec![
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string()
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        std::fs::write(
            first_dir.join("agent_meta.json"),
            json!({"name": "agent-alpha", "model": "new-model"}).to_string(),
        )
        .unwrap();
        let result = artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                artifact_dir: Some(first_dir.to_string_lossy().into()),
                include_sources: vec![
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string()
                ],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        assert!(result.errors.is_empty(), "{result:?}");
        assert!(result
            .affected_node_ids
            .iter()
            .any(|id| id == "agent-alpha"));
        assert!(!result.affected_node_ids.iter().any(|id| id == "agent-beta"));
        assert_eq!(
            artifact_show(&store, "agent-alpha").unwrap().payloads[0].payload
                ["record"]["agent_meta"]["model"],
            json!("new-model")
        );
        assert_eq!(
            artifact_show(&store, "agent-beta").unwrap().payloads[0].payload
                ["record"]["agent_meta"]["model"],
            json!("beta-model")
        );
    }

    #[test]
    fn stale_cleanup_marks_removed_project_source_and_clears_on_return() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        std::fs::create_dir_all(&project_dir).unwrap();
        let project_file = project_dir.join("stale.gp");
        std::fs::write(
            &project_file,
            "NAME: stale_cl\nSTATUS: WIP\nCOMMITS:\n  (1) Old note\n",
        )
        .unwrap();

        let mut store = open_artifact_store(&db).unwrap();
        let request = ArtifactRebuildRequestWire {
            projects_root: Some(projects_root.to_string_lossy().into()),
            workspace_root: None,
            beads_dir: None,
            stale_cleanup: ARTIFACT_STALE_CLEANUP_MARK.to_string(),
            ..ArtifactRebuildRequestWire::default()
        };
        artifact_rebuild(&mut store, request.clone()).unwrap();
        assert!(artifact_show(&store, "stale_cl").unwrap().node.is_some());

        std::fs::remove_file(&project_file).unwrap();
        artifact_rebuild(&mut store, request.clone()).unwrap();
        assert!(artifact_show(&store, "stale_cl").unwrap().node.is_none());
        let doctor =
            artifact_doctor(&store, ArtifactDoctorOptionsWire::default())
                .unwrap();
        assert!(doctor.issues.iter().any(|issue| {
            issue.issue_type == "stale_derived"
                && issue.artifact_id.as_deref() == Some("stale_cl")
        }));

        std::fs::write(
            &project_file,
            "NAME: stale_cl\nSTATUS: WIP\nCOMMITS:\n  (1) New note\n",
        )
        .unwrap();
        artifact_rebuild(&mut store, request).unwrap();
        assert!(artifact_show(&store, "stale_cl").unwrap().node.is_some());
        assert_eq!(
            artifact_show(&store, "stale_cl:1").unwrap().payloads[0].payload
                ["note"],
            json!("New note")
        );
    }

    #[test]
    fn reconciliation_links_targets_after_out_of_order_targeted_rebuilds() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        let project_file = project_dir.join("acme.gp");
        let artifact_dir = project_dir.join("artifacts/ace-run/20260505120000");
        let beads_dir = tmp.path().join("workspace/sdd/beads");
        std::fs::create_dir_all(&project_dir).unwrap();
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::create_dir_all(&beads_dir).unwrap();
        std::fs::write(
            &project_file,
            "NAME: cl-one\nSTATUS: WIP\nCOMMITS:\n  (1) Initial note\n",
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({"name": "agent-alpha"}).to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("done.json"),
            json!({"name": "agent-alpha", "cl_name": "cl-one"}).to_string(),
        )
        .unwrap();
        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-9","title":"Epic","status":"open","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"agent-alpha","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":true,"changespec_name":"cl-one","dependencies":[]}"#,
            ],
        );

        let mut store = open_artifact_store(&db).unwrap();
        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
                include_sources: vec![ARTIFACT_SOURCE_BEAD_STORE.to_string()],
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();
        let pending = artifact_show(&store, "sase-9").unwrap();
        assert_eq!(
            pending.payloads[0].payload["pending_worker_agent_id"],
            "agent-alpha"
        );
        assert_eq!(
            pending.payloads[0].payload["unresolved_related"][0],
            "cl-one"
        );
        let doctor =
            artifact_doctor(&store, ArtifactDoctorOptionsWire::default())
                .unwrap();
        assert!(doctor.issues.iter().any(|issue| {
            issue.issue_type == "unresolved_bead_reference"
                && issue.artifact_id.as_deref() == Some("sase-9")
        }));
        assert!(doctor.issues.iter().any(|issue| {
            issue.issue_type == "unresolved_changespec_reference"
                && issue.artifact_id.as_deref() == Some("sase-9")
        }));

        artifact_rebuild(
            &mut store,
            ArtifactRebuildRequestWire {
                projects_root: Some(projects_root.to_string_lossy().into()),
                include_sources: vec![
                    ARTIFACT_SOURCE_PROJECT_FILE.to_string(),
                    ARTIFACT_SOURCE_CHANGESPEC.to_string(),
                    ARTIFACT_SOURCE_COMMIT.to_string(),
                    ARTIFACT_SOURCE_AGENT_ARTIFACT.to_string(),
                ],
                artifact_dir: Some(artifact_dir.to_string_lossy().into()),
                ..ArtifactRebuildRequestWire::default()
            },
        )
        .unwrap();

        let phase = artifact_show(&store, "sase-9").unwrap();
        assert!(phase.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_WORKER
                && link.source_id == "sase-9"
                && link.target_id == "agent-alpha"
        }));
        assert!(phase.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == "sase-9"
                && link.target_id == "cl-one"
        }));
        assert!(phase.payloads[0]
            .payload
            .get("pending_worker_agent_id")
            .is_none());
        assert!(phase.payloads[0]
            .payload
            .get("unresolved_related")
            .is_none());

        let agent = artifact_show(&store, "agent-alpha").unwrap();
        assert!(agent.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == "agent-alpha"
                && link.target_id == "cl-one"
        }));
        assert!(agent.payloads[0]
            .payload
            .get("unresolved_related")
            .is_none());
    }

    #[test]
    fn end_to_end_fixture_passes_doctor_and_detail_queries() {
        let tmp = tempdir().unwrap();
        let projects_root = tmp.path().join("projects");
        let project_dir = projects_root.join("acme");
        let project_file = project_dir.join("acme.gp");
        let artifact_dir = project_dir.join("artifacts/ace-run/20260505120000");
        let beads_dir = tmp.path().join("workspace/sdd/beads");
        let response_path = artifact_dir.join("response.md");
        std::fs::create_dir_all(&project_dir).unwrap();
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::create_dir_all(&beads_dir).unwrap();
        std::fs::write(
            &project_file,
            "NAME: cl-one\nDESCRIPTION: Build the feature.\nSTATUS: WIP\nCOMMITS:\n  (1) Initial note\n",
        )
        .unwrap();
        std::fs::write(&response_path, "done").unwrap();
        std::fs::write(
            artifact_dir.join("agent_meta.json"),
            json!({"name": "agent-alpha", "llm_provider": "codex"}).to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("done.json"),
            json!({
                "name": "agent-alpha",
                "cl_name": "cl-one",
                "response_path": response_path
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            artifact_dir.join("codex_thinking.jsonl"),
            json!({
                "text": "inspect the artifact graph",
                "timestamp": "2026-05-05T12:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        write_bead_issues(
            &beads_dir,
            &[
                r#"{"id":"sase-10","title":"Epic","status":"open","issue_type":"plan","tier":"epic","owner":"owner@example.com","assignee":"","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":true,"changespec_name":"cl-one","dependencies":[]}"#,
                r#"{"id":"sase-10.1","title":"Phase","status":"in_progress","issue_type":"phase","parent_id":"sase-10","owner":"owner@example.com","assignee":"agent-alpha","created_at":"2026-05-05T00:00:00Z","created_by":"owner@example.com","updated_at":"2026-05-05T00:00:00Z","description":"","notes":"","design":"","is_ready_to_work":false,"dependencies":[]}"#,
            ],
        );

        let first_db = tmp.path().join("first.sqlite");
        let second_db = tmp.path().join("second.sqlite");
        let request = ArtifactRebuildRequestWire {
            projects_root: Some(projects_root.to_string_lossy().into()),
            workspace_root: Some(
                tmp.path().join("workspace").to_string_lossy().into(),
            ),
            beads_dir: Some(beads_dir.to_string_lossy().into_owned()),
            ..ArtifactRebuildRequestWire::default()
        };

        let mut first = open_artifact_store(&first_db).unwrap();
        let result = artifact_rebuild(&mut first, request.clone()).unwrap();
        assert!(result.errors.is_empty(), "{result:?}");
        let doctor =
            artifact_doctor(&first, ArtifactDoctorOptionsWire::default())
                .unwrap();
        assert!(doctor.ok, "{doctor:?}");

        let changespec = artifact_show(&first, "cl-one").unwrap();
        assert!(changespec.children.iter().any(|node| {
            node.kind == ARTIFACT_KIND_COMMIT && node.id == "cl-one:1"
        }));
        assert!(changespec.inbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == "agent-alpha"
        }));
        assert!(changespec.inbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == "sase-10"
        }));

        let agent = artifact_show(&first, "agent-alpha").unwrap();
        assert!(agent.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_CREATED
                && link.target_id == response_path.to_string_lossy()
        }));
        assert!(agent.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_CREATED
                && link.target_id.starts_with("thought:")
        }));

        let bead = artifact_show(&first, "sase-10.1").unwrap();
        assert!(bead.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_PARENT
                && link.target_id == "sase-10"
        }));
        assert!(bead.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_WORKER
                && link.target_id == "agent-alpha"
        }));

        let response =
            artifact_show(&first, response_path.to_str().unwrap()).unwrap();
        assert_eq!(
            response.node.as_ref().map(|node| node.kind.as_str()),
            Some(ARTIFACT_KIND_FILE)
        );
        assert_eq!(response.path_to_root.last().unwrap().id, ARTIFACT_ROOT_ID);

        let thoughts = artifact_list(
            &first,
            ArtifactQueryWire {
                kinds: vec![ARTIFACT_KIND_THOUGHT.to_string()],
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(thoughts.len(), 1);
        let thought = artifact_show(&first, &thoughts[0].id).unwrap();
        assert_eq!(
            thought.path_to_root.first().map(|node| node.id.as_str()),
            Some(thoughts[0].id.as_str())
        );
        assert_eq!(thought.path_to_root.last().unwrap().id, ARTIFACT_ROOT_ID);

        let graph = artifact_materialize_graph(
            &first,
            ArtifactGraphOptionsWire {
                root_id: Some("cl-one".to_string()),
                include_inbound: true,
                include_outbound: true,
                max_depth: Some(1),
                limit: None,
                ..ArtifactGraphOptionsWire::default()
            },
        )
        .unwrap();
        let graph_ids: BTreeSet<_> =
            graph.nodes.iter().map(|node| node.id.as_str()).collect();
        assert!(graph_ids.contains("cl-one:1"));
        assert!(graph_ids.contains("agent-alpha"));
        assert!(graph_ids.contains("sase-10"));

        let mut second = open_artifact_store(&second_db).unwrap();
        artifact_rebuild(&mut second, request).unwrap();
        let export_options = ArtifactGraphOptionsWire {
            full_graph: true,
            root_id: Some(ARTIFACT_ROOT_ID.to_string()),
            limit: None,
            ..ArtifactGraphOptionsWire::default()
        };
        assert_eq!(
            artifact_export_json(&first, export_options.clone()).unwrap(),
            artifact_export_json(&second, export_options).unwrap()
        );
    }

    fn write_bead_issues(beads_dir: &Path, lines: &[&str]) {
        std::fs::write(beads_dir.join("issues.jsonl"), lines.join("\n"))
            .unwrap();
    }

    fn manual_node(
        id: &str,
        kind: &str,
        display_title: &str,
    ) -> ArtifactNodeWire {
        ArtifactNodeWire {
            id: id.to_string(),
            kind: kind.to_string(),
            display_title: display_title.to_string(),
            subtitle: None,
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id: None,
            source_version: None,
            search_text: display_title.to_string(),
            metadata: Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn upsert_node(store: &mut ArtifactStore, node: ArtifactNodeWire) {
        upsert_artifact_node(
            store,
            ArtifactNodeUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                node,
                replace_payloads: false,
            },
        )
        .unwrap();
    }
}
