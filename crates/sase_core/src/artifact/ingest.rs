//! Derived artifact ingestion framework and shared path helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};

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
    ARTIFACT_KIND_ROOT, ARTIFACT_LINK_CREATED, ARTIFACT_LINK_PARENT,
    ARTIFACT_LINK_RELATED, ARTIFACT_LINK_WORKER, ARTIFACT_PROVENANCE_DERIVED,
    ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID, ARTIFACT_STALE_CLEANUP_MARK,
    ARTIFACT_STALE_CLEANUP_NONE, ARTIFACT_WIRE_SCHEMA_VERSION,
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

    if source_selected(&resolved, ARTIFACT_SOURCE_DIRECTORY) {
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
    }

    if project_ingestion_selected(&resolved) {
        mutations.merge(ingest_project_sources(store, &resolved)?);
    }

    if agent_ingestion_selected(&resolved) {
        mutations.merge(ingest_agent_artifacts(store, &resolved)?);
    }

    if source_selected(&resolved, ARTIFACT_SOURCE_BEAD_STORE) {
        mutations.merge(ingest_beads(store, &resolved)?);
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

        let created_files = collect_agent_created_file_paths(&record);
        let related_targets =
            collect_agent_related_targets(&record, &agent_ids_by_timestamp);
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
                    "record": record,
                    "created_file_paths": created_files
                        .keys()
                        .map(|path| path_to_artifact_id(path))
                        .collect::<Vec<_>>(),
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
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.name.as_ref().and_then(non_empty))
        .or_else(|| {
            record
                .done
                .as_ref()
                .and_then(|done| done.name.as_ref().and_then(non_empty))
        })
        .unwrap_or_else(|| {
            format!(
                "agent:{}:{}:{}",
                record.project_name, record.workflow_dir_name, record.timestamp
            )
        })
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
        .and_then(|meta| meta.name.as_ref().and_then(non_empty))
        .or_else(|| {
            record
                .done
                .as_ref()
                .and_then(|done| done.name.as_ref().and_then(non_empty))
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
        record.done.as_ref().and_then(|done| done.cl_name.as_ref()),
        record
            .running
            .as_ref()
            .and_then(|running| running.cl_name.as_ref()),
        record
            .workflow_state
            .as_ref()
            .and_then(|workflow| workflow.cl_name.as_ref()),
    ] {
        if let Some(changespec) = maybe_changespec.and_then(non_empty) {
            targets.resolved.insert(changespec);
        }
    }

    let mut timestamp_refs = Vec::new();
    if let Some(meta) = &record.agent_meta {
        push_optional(&mut timestamp_refs, &meta.parent_timestamp);
        push_optional(&mut timestamp_refs, &meta.retry_of_timestamp);
        push_optional(&mut timestamp_refs, &meta.retried_as_timestamp);
        push_optional(&mut timestamp_refs, &meta.retry_chain_root_timestamp);
        timestamp_refs.extend(meta.plan_submitted_at.iter().cloned());
        timestamp_refs.extend(meta.feedback_submitted_at.iter().cloned());
        timestamp_refs.extend(meta.questions_submitted_at.iter().cloned());
        timestamp_refs.extend(meta.retry_started_at.iter().cloned());
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
    for marker_name in ["agent_meta.json", "done.json", "workflow_state.json"] {
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
        "response_path",
        "output_path",
        "live_reply_path",
        "timestamps_path",
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

fn non_empty(value: &String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value.clone())
    }
}

fn push_optional(parts: &mut Vec<String>, value: &Option<String>) {
    if let Some(value) = value.as_ref().and_then(non_empty) {
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

    use super::super::query::{artifact_list, artifact_show};
    use super::super::store::{open_artifact_store, remove_artifact_node};
    use super::super::wire::{
        ArtifactNodeRemoveWire, ArtifactNodeUpsertWire, ArtifactNodeWire,
        ArtifactQueryWire, ARTIFACT_KIND_AGENT, ARTIFACT_KIND_CHANGESPEC,
        ARTIFACT_KIND_COMMIT, ARTIFACT_LINK_PARENT, ARTIFACT_LINK_RELATED,
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
            json!({"retry_of_timestamp": "20260505115900"}).to_string(),
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
        assert!(retry.outbound_links.iter().any(|link| {
            link.link_type == ARTIFACT_LINK_RELATED
                && link.source_id == fallback_id
                && link.target_id == "retry-parent"
        }));
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
