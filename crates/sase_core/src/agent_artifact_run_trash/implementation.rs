//! Journaled, restorable quarantine for ACE-run artifact directories.

use super::helpers::*;

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::agent_artifact_run_retention::{
    canonical_protected_dirs, canonicalize_existing, classify_run_candidate,
    existing_path_safety_violation, fresh_active_marker_reason,
    prune_empty_tree, run_candidate_safety_violation, tree_size,
    AgentArtifactEmptyShardResultWire, AgentArtifactRunCandidateWire,
};

pub const AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION: u32 = 1;

const MAX_TRASH_ITEMS: usize = 200_000;

#[derive(Debug, Error)]
pub enum AgentArtifactRunTrashError {
    #[error("agent artifact run trash requires schema_version {expected}, got {actual}")]
    UnsupportedSchema { actual: u32, expected: u32 },
    #[error("unsafe projects root {path}: {reason}")]
    UnsafeProjectsRoot { path: String, reason: String },
    #[error("projects root could not be resolved: {0}")]
    InvalidProjectsRoot(String),
    #[error("trash root must be an absolute path: {0}")]
    RelativeTrashRoot(String),
    #[error("trash root is unsafe: {0}")]
    UnsafeTrashRoot(String),
    #[error("invalid batch id: {0}")]
    InvalidBatchId(String),
    #[error("too many items: {actual} exceeds {max}")]
    TooManyItems { actual: usize, max: usize },
    #[error("trash operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("trash lock failed: {0}")]
    Lock(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashQuarantineRequestWire {
    pub schema_version: u32,
    pub projects_root: String,
    pub sources_unavailable: Vec<String>,
    pub protected_dirs: Vec<String>,
    pub protected_timestamps: Vec<String>,
    pub recent_months: Vec<String>,
    pub current_timestamp: String,
    pub items: Vec<AgentArtifactRunCandidateWire>,
    pub trash_root: String,
    pub batch_id: String,
    pub created_at: String,
    #[serde(default)]
    pub empty_shard_roots: Vec<String>,
    #[serde(default)]
    pub empty_shard_watched_paths: Vec<String>,
    #[serde(default)]
    pub empty_shard_removal_budget: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentArtifactRunTrashItemResultWire {
    pub artifact_dir: String,
    pub project: String,
    pub timestamp: String,
    pub outcome: String,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashQuarantineResultWire {
    pub schema_version: u32,
    pub batch_id: String,
    pub status: String,
    pub requested: u64,
    pub quarantined: u64,
    pub kept: u64,
    pub errors: u64,
    pub bytes_quarantined: u64,
    #[serde(default)]
    pub blocked_reason: Option<String>,
    #[serde(default)]
    pub items: Vec<AgentArtifactRunTrashItemResultWire>,
    pub removed_empty_shards: u64,
    pub journal_path: String,
    #[serde(default)]
    pub shard_items: Vec<AgentArtifactEmptyShardResultWire>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashListResultWire {
    pub schema_version: u32,
    pub batches: Vec<AgentArtifactRunTrashBatchWire>,
    pub unreadable_journal_count: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashBatchWire {
    pub id: String,
    pub created_at: String,
    pub status: String,
    pub run_count: u64,
    pub bytes: u64,
    pub projects: Vec<String>,
    pub orphan_items: Vec<AgentArtifactRunTrashOrphanWire>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashOrphanWire {
    pub project: String,
    pub timestamp: String,
    pub original_path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashRestoreRequestWire {
    pub schema_version: u32,
    pub trash_root: String,
    pub projects_root: String,
    pub batch_id: String,
    /// Optional original paths, relative to `projects_root`; absent restores all.
    pub items: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashRestoreResultWire {
    pub schema_version: u32,
    pub batch_id: String,
    pub status: String,
    pub restored: u64,
    pub conflicts: u64,
    pub errors: u64,
    pub restored_dirs: Vec<String>,
    #[serde(default)]
    pub items: Vec<AgentArtifactRunTrashItemResultWire>,
    #[serde(default)]
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashPurgeRequestWire {
    pub schema_version: u32,
    pub trash_root: String,
    pub projects_root: String,
    pub before: Option<String>,
    pub purge_all: bool,
    pub sources_unavailable: Vec<String>,
    pub protected_dirs: Vec<String>,
    pub protected_timestamps: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunTrashPurgeResultWire {
    pub schema_version: u32,
    pub status: String,
    pub purged: u64,
    pub kept: u64,
    pub restored: u64,
    pub errors: u64,
    pub freed_bytes: u64,
    pub restored_dirs: Vec<String>,
    #[serde(default)]
    pub purged_batches: Vec<String>,
    #[serde(default)]
    pub items: Vec<AgentArtifactRunTrashItemResultWire>,
    #[serde(default)]
    pub blocked_reason: Option<String>,
}

pub fn agent_artifact_run_trash_wire_schema_version() -> u32 {
    AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION
}

pub fn quarantine_agent_artifact_runs(
    request: &AgentArtifactRunTrashQuarantineRequestWire,
) -> Result<AgentArtifactRunTrashQuarantineResultWire, AgentArtifactRunTrashError>
{
    check_schema(request.schema_version)?;
    validate_batch_id(&request.batch_id)?;
    if request.items.len() > MAX_TRASH_ITEMS {
        return Err(AgentArtifactRunTrashError::TooManyItems {
            actual: request.items.len(),
            max: MAX_TRASH_ITEMS,
        });
    }
    let mut result = AgentArtifactRunTrashQuarantineResultWire {
        schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        batch_id: request.batch_id.clone(),
        status: "blocked".to_string(),
        requested: request.items.len() as u64,
        journal_path: String::new(),
        ..Default::default()
    };
    if !request.sources_unavailable.is_empty() {
        result.blocked_reason = Some("protection_unavailable".to_string());
        return Ok(result);
    }

    let projects_root = validate_projects_root(&request.projects_root)?;
    let trash_root = ensure_trash_root(
        Path::new(&request.trash_root),
        Some(&projects_root),
    )?;
    let Some(_lock) = acquire_trash_lock(&trash_root)? else {
        result.blocked_reason = Some("busy".to_string());
        return Ok(result);
    };
    let batch_dir = trash_root.join(&request.batch_id);
    let runs_root = batch_dir.join("runs");
    let journal_path = batch_dir.join("journal.jsonl");
    result.journal_path = journal_path.to_string_lossy().into_owned();
    let mut journal = if fs::symlink_metadata(&batch_dir).is_ok() {
        ensure_safe_batch(&trash_root, &batch_dir)?;
        let info = read_journal(&batch_dir);
        if info.unreadable || info.batch_id != request.batch_id {
            result.blocked_reason = Some("batch_exists".to_string());
            return Ok(result);
        }
        if fs::symlink_metadata(&runs_root).is_ok()
            && !safe_real_dir(&runs_root)
        {
            return Err(AgentArtifactRunTrashError::UnsafeTrashRoot(
                runs_root.display().to_string(),
            ));
        }
        fs::create_dir_all(&runs_root)?;
        open_journal_append(&journal_path)?
    } else {
        fs::create_dir(&batch_dir)?;
        fs::create_dir(&runs_root)?;
        let mut journal = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&journal_path)?;
        append_journal_line(
            &mut journal,
            &json!({
                "kind": "header",
                "schema_version": AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
                "batch_id": request.batch_id,
                "created_at": request.created_at,
            }),
        )?;
        journal
    };

    let protected_dirs = canonical_protected_dirs(&request.protected_dirs);
    let protected_timestamps: BTreeSet<String> =
        request.protected_timestamps.iter().cloned().collect();
    let recent_months: BTreeSet<String> =
        request.recent_months.iter().cloned().collect();
    let mut indices: Vec<usize> = (0..request.items.len()).collect();
    indices.sort_by(|a, b| {
        let left = &request.items[*a];
        let right = &request.items[*b];
        (&left.timestamp, &left.project, &left.artifact_dir).cmp(&(
            &right.timestamp,
            &right.project,
            &right.artifact_dir,
        ))
    });
    let mut moved_projects = BTreeSet::new();
    let mut protected_run_dirs = protected_dirs.clone();
    for index in indices {
        let candidate = &request.items[index];
        let source = Path::new(&candidate.artifact_dir);
        let reasons = classify_run_candidate(
            &projects_root,
            candidate,
            &protected_dirs,
            &protected_timestamps,
            &recent_months,
            &request.current_timestamp,
        );
        if !reasons.is_empty() {
            let safety_reason = reasons.iter().any(|reason| {
                matches!(
                    reason.as_str(),
                    "missing"
                        | "outside_projects_root"
                        | "symlink"
                        | "symlink_ancestor"
                        | "invalid_run_path"
                )
            });
            let reasons = if safety_reason {
                vec!["unsafe_path".to_string()]
            } else {
                reasons
                    .into_iter()
                    .map(|reason| format!("protected:{reason}"))
                    .collect()
            };
            result.kept += 1;
            result
                .items
                .push(item_result(candidate, "kept", reasons, None, 0));
            if let Some(path) = canonicalize_existing(source) {
                protected_run_dirs.insert(path);
            }
            continue;
        }
        if let Some(reason) = fresh_active_marker_reason(source) {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec![reason],
                None,
                0,
            ));
            if let Some(path) = canonicalize_existing(source) {
                protected_run_dirs.insert(path);
            }
            continue;
        }
        if run_candidate_safety_violation(&projects_root, candidate).is_some() {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec!["unsafe_path".to_string()],
                None,
                0,
            ));
            if let Some(path) = canonicalize_existing(source) {
                protected_run_dirs.insert(path);
            }
            continue;
        }
        let Some(timestamp) = valid_run_parts(&candidate.timestamp) else {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec!["unsafe_path".to_string()],
                None,
                0,
            ));
            continue;
        };
        let destination = runs_root
            .join(&candidate.project)
            .join(timestamp.0)
            .join(timestamp.1)
            .join(&candidate.timestamp);
        if fs::symlink_metadata(&destination).is_ok() {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec!["trash_conflict".to_string()],
                None,
                0,
            ));
            continue;
        }
        let source_meta = match fs::symlink_metadata(source) {
            Ok(metadata)
                if metadata.is_dir() && !metadata.file_type().is_symlink() =>
            {
                metadata
            }
            _ => {
                result.kept += 1;
                result.items.push(item_result(
                    candidate,
                    "kept",
                    vec!["unsafe_path".to_string()],
                    None,
                    0,
                ));
                continue;
            }
        };
        let trash_meta = fs::metadata(&trash_root)?;
        if !same_device(&source_meta, &trash_meta) {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec!["cross_device".to_string()],
                None,
                0,
            ));
            continue;
        }
        let bytes = tree_size(source);
        let destination_parent = destination.parent().expect("run parent");
        let directories_created = match create_directories_no_symlinks(
            &runs_root,
            destination_parent,
        ) {
            Ok(created) => created,
            Err(error) => {
                result.errors += 1;
                result.items.push(item_result(
                    candidate,
                    "error",
                    Vec::new(),
                    Some(error.to_string()),
                    0,
                ));
                continue;
            }
        };
        if !directories_created {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec!["unsafe_path".to_string()],
                None,
                0,
            ));
            continue;
        }
        if fs::symlink_metadata(&destination).is_ok() {
            result.kept += 1;
            result.items.push(item_result(
                candidate,
                "kept",
                vec!["trash_conflict".to_string()],
                None,
                0,
            ));
            continue;
        }
        if let Err(error) = fs::rename(source, &destination) {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                result.kept += 1;
                result.items.push(item_result(
                    candidate,
                    "kept",
                    vec!["trash_conflict".to_string()],
                    None,
                    0,
                ));
            } else {
                result.errors += 1;
                result.items.push(item_result(
                    candidate,
                    "error",
                    Vec::new(),
                    Some(error.to_string()),
                    0,
                ));
            }
            continue;
        }
        result.quarantined += 1;
        result.bytes_quarantined =
            result.bytes_quarantined.saturating_add(bytes);
        moved_projects.insert(candidate.project.clone());
        result.items.push(item_result(
            candidate,
            "quarantined",
            Vec::new(),
            None,
            bytes,
        ));
        let original_path =
            expected_original_path(&candidate.project, &candidate.timestamp);
        if let Err(error) = append_journal_line(
            &mut journal,
            &json!({
                "kind": "item",
                "original_path": original_path,
                "project": candidate.project,
                "timestamp": candidate.timestamp,
                "bytes": bytes,
            }),
        ) {
            result.errors += 1;
            if let Some(item) = result.items.last_mut() {
                item.detail =
                    Some(format!("moved but journal append failed: {error}"));
            }
        }
    }

    if !moved_projects.is_empty() {
        let watched: BTreeSet<PathBuf> = request
            .empty_shard_watched_paths
            .iter()
            .filter_map(|path| canonicalize_existing(Path::new(path)))
            .collect();
        let mut budget = request.empty_shard_removal_budget;
        for root in &request.empty_shard_roots {
            let root_path = Path::new(root);
            let Some(canonical_root) = canonicalize_existing(root_path) else {
                continue;
            };
            let Some(project) = canonical_root
                .strip_prefix(&projects_root)
                .ok()
                .and_then(|relative| relative.components().next())
                .and_then(|component| match component {
                    Component::Normal(name) => name.to_str(),
                    _ => None,
                })
            else {
                continue;
            };
            if !moved_projects.contains(project)
                || canonical_root
                    .strip_prefix(&projects_root)
                    .map(|relative| relative.components().count() != 3)
                    .unwrap_or(true)
                || !root_path.ends_with("ace-run")
                || existing_path_safety_violation(&projects_root, root_path)
                    .is_some()
            {
                continue;
            }
            let Ok(children) = fs::read_dir(&canonical_root) else {
                continue;
            };
            for child in children.filter_map(Result::ok) {
                prune_empty_tree(
                    &canonical_root,
                    &child.path(),
                    &watched,
                    &protected_run_dirs,
                    true,
                    &mut budget,
                    &mut result.shard_items,
                );
            }
        }
    }
    result.removed_empty_shards = result
        .shard_items
        .iter()
        .filter(|item| item.outcome == "removed")
        .count() as u64;
    result.errors += result
        .shard_items
        .iter()
        .filter(|item| item.outcome == "error")
        .count() as u64;
    result.kept = result
        .items
        .iter()
        .filter(|item| item.outcome == "kept")
        .count() as u64;
    result.status = if result.errors == 0 && result.kept == 0 {
        "complete".to_string()
    } else {
        "partial".to_string()
    };
    append_journal_line(
        &mut journal,
        &json!({
            "kind": "footer",
            "status": result.status,
            "requested": result.requested,
            "quarantined": result.quarantined,
            "kept": result.kept,
            "errors": result.errors,
            "bytes_quarantined": result.bytes_quarantined,
        }),
    )?;
    Ok(result)
}

pub fn list_agent_artifact_run_trash(
    trash_root: &str,
) -> Result<AgentArtifactRunTrashListResultWire, AgentArtifactRunTrashError> {
    let root = Path::new(trash_root);
    if !root.is_absolute() {
        return Err(AgentArtifactRunTrashError::RelativeTrashRoot(
            trash_root.to_string(),
        ));
    }
    let mut result = AgentArtifactRunTrashListResultWire {
        schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        ..Default::default()
    };
    if fs::symlink_metadata(root).is_err() {
        return Ok(result);
    }
    let canonical_root = ensure_trash_root(root, None)?;
    for entry in fs::read_dir(&canonical_root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        let id = entry.file_name().to_string_lossy().into_owned();
        if validate_batch_id(&id).is_err() {
            continue;
        }
        let journal = read_journal(&path);
        if journal.unreadable {
            result.unreadable_journal_count += 1;
        }
        let runs = collect_trash_runs(&path, &journal);
        let mut projects = BTreeSet::new();
        let mut orphan_items = Vec::new();
        let mut bytes = 0_u64;
        for run in &runs {
            projects.insert(run.project.clone());
            bytes = bytes.saturating_add(run.bytes);
            if !run.journaled {
                orphan_items.push(AgentArtifactRunTrashOrphanWire {
                    project: run.project.clone(),
                    timestamp: run.timestamp.clone(),
                    original_path: run.original_relative_path.clone(),
                    bytes: run.bytes,
                });
            }
        }
        result.batches.push(AgentArtifactRunTrashBatchWire {
            id,
            created_at: if journal.created_at.is_empty() {
                directory_modified_at(&path)
            } else {
                journal.created_at
            },
            status: if journal.status.is_empty() {
                "partial".to_string()
            } else {
                journal.status
            },
            run_count: runs.len() as u64,
            bytes,
            projects: projects.into_iter().collect(),
            orphan_items,
        });
    }
    result.batches.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    Ok(result)
}

pub fn restore_agent_artifact_run_trash(
    request: &AgentArtifactRunTrashRestoreRequestWire,
) -> Result<AgentArtifactRunTrashRestoreResultWire, AgentArtifactRunTrashError>
{
    check_schema(request.schema_version)?;
    validate_batch_id(&request.batch_id)?;
    let projects_root = validate_projects_root(&request.projects_root)?;
    let trash_root = ensure_trash_root(
        Path::new(&request.trash_root),
        Some(&projects_root),
    )?;
    let mut result = AgentArtifactRunTrashRestoreResultWire {
        schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        batch_id: request.batch_id.clone(),
        status: "complete".to_string(),
        ..Default::default()
    };
    let Some(_lock) = acquire_trash_lock(&trash_root)? else {
        result.status = "blocked".to_string();
        result.blocked_reason = Some("busy".to_string());
        return Ok(result);
    };
    let batch_dir = trash_root.join(&request.batch_id);
    if fs::symlink_metadata(&batch_dir).is_err() {
        return Ok(result);
    }
    ensure_safe_batch(&trash_root, &batch_dir)?;
    let journal_info = read_journal(&batch_dir);
    let entries = collect_trash_runs(&batch_dir, &journal_info);
    let selected: Option<BTreeSet<String>> = request
        .items
        .as_ref()
        .map(|items| items.iter().cloned().collect());
    let journal_path = batch_dir.join("journal.jsonl");
    let mut journal = open_journal_append(&journal_path).ok();
    for entry in entries {
        if selected
            .as_ref()
            .is_some_and(|items| !items.contains(&entry.original_relative_path))
        {
            continue;
        }
        let target = projects_root.join(&entry.original_relative_path);
        if !is_expected_original_path(
            &entry.original_relative_path,
            &entry.project,
            &entry.timestamp,
        ) {
            result.errors += 1;
            result.items.push(trash_item_result(
                &entry,
                "error",
                vec!["unsafe_path".to_string()],
                Some("journal path did not match trash layout".to_string()),
            ));
            continue;
        }
        if fs::symlink_metadata(&target).is_ok() {
            result.conflicts += 1;
            result.items.push(trash_item_result(
                &entry,
                "conflict",
                vec!["conflict".to_string()],
                None,
            ));
            continue;
        }
        let Some(parent) = target.parent() else {
            result.errors += 1;
            continue;
        };
        match create_directories_no_symlinks(&projects_root, parent) {
            Err(error) => {
                result.errors += 1;
                result.items.push(trash_item_result(
                    &entry,
                    "error",
                    vec!["unsafe_path".to_string()],
                    Some(error.to_string()),
                ));
                continue;
            }
            Ok(false) => {
                result.conflicts += 1;
                result.items.push(trash_item_result(
                    &entry,
                    "conflict",
                    vec!["unsafe_path".to_string()],
                    None,
                ));
                continue;
            }
            Ok(true) => {}
        }
        if fs::symlink_metadata(&target).is_ok() {
            result.conflicts += 1;
            result.items.push(trash_item_result(
                &entry,
                "conflict",
                vec!["conflict".to_string()],
                None,
            ));
            continue;
        }
        match fs::rename(&entry.trash_path, &target) {
            Ok(()) => {
                result.restored += 1;
                let target_string = target.to_string_lossy().into_owned();
                result.restored_dirs.push(target_string);
                result.items.push(trash_item_result(
                    &entry,
                    "restored",
                    Vec::new(),
                    None,
                ));
                if let Some(journal) = journal.as_mut() {
                    let _ = append_journal_line(
                        journal,
                        &json!({
                            "kind": "restore_item",
                            "original_path": entry.original_relative_path,
                            "project": entry.project,
                            "timestamp": entry.timestamp,
                        }),
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                result.conflicts += 1;
                result.items.push(trash_item_result(
                    &entry,
                    "conflict",
                    vec!["conflict".to_string()],
                    None,
                ));
            }
            Err(error) => {
                result.errors += 1;
                result.items.push(trash_item_result(
                    &entry,
                    "error",
                    Vec::new(),
                    Some(error.to_string()),
                ));
            }
        }
    }
    if result.restored > 0 {
        cleanup_empty_batch(&batch_dir)?;
    }
    if result.conflicts > 0 || result.errors > 0 {
        result.status = "partial".to_string();
    }
    Ok(result)
}

pub fn purge_agent_artifact_run_trash(
    request: &AgentArtifactRunTrashPurgeRequestWire,
) -> Result<AgentArtifactRunTrashPurgeResultWire, AgentArtifactRunTrashError> {
    check_schema(request.schema_version)?;
    let mut result = AgentArtifactRunTrashPurgeResultWire {
        schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        status: "complete".to_string(),
        ..Default::default()
    };
    if !request.sources_unavailable.is_empty() {
        result.status = "blocked".to_string();
        result.blocked_reason = Some("protection_unavailable".to_string());
        return Ok(result);
    }
    let projects_root = validate_projects_root(&request.projects_root)?;
    let trash_root = ensure_trash_root(
        Path::new(&request.trash_root),
        Some(&projects_root),
    )?;
    let Some(_lock) = acquire_trash_lock(&trash_root)? else {
        result.status = "blocked".to_string();
        result.blocked_reason = Some("busy".to_string());
        return Ok(result);
    };
    let protected_dirs =
        normalized_protected_dirs(&request.protected_dirs, &projects_root);
    let protected_timestamps: BTreeSet<String> =
        request.protected_timestamps.iter().cloned().collect();
    let batch_dirs = batch_directories(&trash_root)?;
    for (batch_id, batch_dir) in batch_dirs {
        let journal_info = read_journal(&batch_dir);
        let eligible = if request.purge_all {
            true
        } else {
            request.before.as_ref().is_some_and(|before| {
                !journal_info.created_at.is_empty()
                    && journal_info.created_at.as_str() <= before.as_str()
            })
        };
        if !eligible || journal_info.unreadable {
            continue;
        }
        ensure_safe_batch(&trash_root, &batch_dir)?;
        let entries = collect_trash_runs(&batch_dir, &journal_info);
        let mut journal =
            open_journal_append(&batch_dir.join("journal.jsonl")).ok();
        for entry in entries {
            let original = projects_root.join(&entry.original_relative_path);
            let protected = protected_timestamps.contains(&entry.timestamp)
                || normalized_path_matches(
                    &original,
                    &protected_dirs,
                    &projects_root,
                );
            if protected {
                match restore_one(&entry, &projects_root) {
                    Ok(target) => {
                        result.restored += 1;
                        result
                            .restored_dirs
                            .push(target.to_string_lossy().into_owned());
                        if let Some(journal) = journal.as_mut() {
                            if append_journal_line(
                                journal,
                                &json!({
                                    "kind": "restore_item",
                                    "original_path": entry.original_relative_path,
                                    "project": entry.project,
                                    "timestamp": entry.timestamp,
                                }),
                            )
                            .is_err()
                            {
                                result.errors += 1;
                            }
                        }
                        result.items.push(trash_item_result(
                            &entry,
                            "restored_protected",
                            Vec::new(),
                            None,
                        ));
                    }
                    Err(RestoreOneError::Conflict) => {
                        result.kept += 1;
                        result.items.push(trash_item_result(
                            &entry,
                            "kept",
                            vec!["conflict".to_string()],
                            None,
                        ));
                    }
                    Err(RestoreOneError::Unsafe(error)) => {
                        result.kept += 1;
                        result.items.push(trash_item_result(
                            &entry,
                            "kept",
                            vec!["unsafe_path".to_string()],
                            Some(error),
                        ));
                    }
                    Err(RestoreOneError::Io(error)) => {
                        result.errors += 1;
                        result.items.push(trash_item_result(
                            &entry,
                            "error",
                            Vec::new(),
                            Some(error.to_string()),
                        ));
                    }
                }
                continue;
            }
            if !safe_trash_item(&trash_root, &entry.trash_path) {
                result.kept += 1;
                result.items.push(trash_item_result(
                    &entry,
                    "kept",
                    vec!["unsafe_path".to_string()],
                    None,
                ));
                continue;
            }
            let bytes = tree_size(&entry.trash_path);
            match fs::remove_dir_all(&entry.trash_path) {
                Ok(()) => {
                    result.purged += 1;
                    result.freed_bytes =
                        result.freed_bytes.saturating_add(bytes);
                    if let Some(journal) = journal.as_mut() {
                        if append_journal_line(
                            journal,
                            &json!({
                                "kind": "purge_item",
                                "original_path": entry.original_relative_path,
                                "project": entry.project,
                                "timestamp": entry.timestamp,
                                "bytes": bytes,
                            }),
                        )
                        .is_err()
                        {
                            result.errors += 1;
                        }
                    }
                    result.items.push(trash_item_result(
                        &entry,
                        "purged",
                        Vec::new(),
                        None,
                    ));
                }
                Err(error) => {
                    result.errors += 1;
                    result.items.push(trash_item_result(
                        &entry,
                        "error",
                        Vec::new(),
                        Some(error.to_string()),
                    ));
                }
            }
        }
        cleanup_empty_batch(&batch_dir)?;
        if !batch_dir.exists() {
            result.purged_batches.push(batch_id);
        }
    }
    if result.errors > 0 {
        result.status = "partial".to_string();
    }
    Ok(result)
}
