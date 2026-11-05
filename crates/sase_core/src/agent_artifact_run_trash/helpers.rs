use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{
    AgentArtifactRunTrashError, AgentArtifactRunTrashItemResultWire,
    AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
};
use crate::agent_artifact_run_retention::{
    canonicalize_existing, projects_root_safety_violation, tree_size,
    valid_project_component, valid_run_timestamp,
    AgentArtifactRunCandidateWire,
};
use crate::store_lock::{acquire_store_lock, LockMode, StoreLockError};

const LOCK_WAIT: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Debug, Default)]
pub(super) struct JournalInfo {
    pub(super) batch_id: String,
    pub(super) created_at: String,
    pub(super) status: String,
    pub(super) original_paths: BTreeMap<(String, String), String>,
    pub(super) unreadable: bool,
}

#[derive(Debug, Clone)]
pub(super) struct TrashRun {
    pub(super) trash_path: PathBuf,
    pub(super) project: String,
    pub(super) timestamp: String,
    pub(super) original_relative_path: String,
    pub(super) bytes: u64,
    pub(super) journaled: bool,
}

pub(super) fn check_schema(
    actual: u32,
) -> Result<(), AgentArtifactRunTrashError> {
    if actual == AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(AgentArtifactRunTrashError::UnsupportedSchema {
            actual,
            expected: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        })
    }
}

pub(super) fn validate_projects_root(
    raw: &str,
) -> Result<PathBuf, AgentArtifactRunTrashError> {
    let path = Path::new(raw);
    if let Some(reason) = projects_root_safety_violation(path) {
        return Err(AgentArtifactRunTrashError::UnsafeProjectsRoot {
            path: raw.to_string(),
            reason: reason.to_string(),
        });
    }
    let canonical = canonicalize_existing(path).ok_or_else(|| {
        AgentArtifactRunTrashError::InvalidProjectsRoot(raw.to_string())
    })?;
    if !canonical.is_dir() {
        return Err(AgentArtifactRunTrashError::InvalidProjectsRoot(
            raw.to_string(),
        ));
    }
    Ok(canonical)
}

pub(super) fn ensure_trash_root(
    input: &Path,
    projects_root: Option<&Path>,
) -> Result<PathBuf, AgentArtifactRunTrashError> {
    if !input.is_absolute() {
        return Err(AgentArtifactRunTrashError::RelativeTrashRoot(
            input.to_string_lossy().into_owned(),
        ));
    }
    if let Ok(metadata) = fs::symlink_metadata(input) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(AgentArtifactRunTrashError::UnsafeTrashRoot(
                input.to_string_lossy().into_owned(),
            ));
        }
    } else if let Some(projects_root) = projects_root {
        let prospective = normalize_path(input, projects_root);
        if prospective.starts_with(projects_root)
            || projects_root.starts_with(&prospective)
        {
            return Err(AgentArtifactRunTrashError::UnsafeTrashRoot(
                "trash and projects roots overlap".to_string(),
            ));
        }
        fs::create_dir_all(input)?;
    } else {
        return Ok(input.to_path_buf());
    }
    let canonical = canonicalize_existing(input).ok_or_else(|| {
        AgentArtifactRunTrashError::UnsafeTrashRoot(
            input.to_string_lossy().into_owned(),
        )
    })?;
    if let Some(projects_root) = projects_root {
        if canonical.starts_with(projects_root)
            || projects_root.starts_with(&canonical)
        {
            return Err(AgentArtifactRunTrashError::UnsafeTrashRoot(
                "trash and projects roots overlap".to_string(),
            ));
        }
    }
    Ok(canonical)
}

pub(super) fn acquire_trash_lock(
    root: &Path,
) -> Result<Option<crate::store_lock::HeldStoreLock>, AgentArtifactRunTrashError>
{
    let lock_path = root.join(".agent-artifact-run-trash.lock");
    let holder_path = root.join(".agent-artifact-run-trash.lock.json");
    if fs::symlink_metadata(&lock_path)
        .is_ok_and(|metadata| metadata.file_type().is_symlink())
        || fs::symlink_metadata(&holder_path)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err(AgentArtifactRunTrashError::UnsafeTrashRoot(
            "trash lock path is a symlink".to_string(),
        ));
    }
    match acquire_store_lock(
        &lock_path,
        &holder_path,
        LockMode::Exclusive,
        LOCK_WAIT,
        "agent-artifact-run-trash",
    ) {
        Ok(lock) => Ok(Some(lock)),
        Err(StoreLockError::Timeout { .. }) => Ok(None),
        Err(error) => Err(AgentArtifactRunTrashError::Lock(error.to_string())),
    }
}

pub(super) fn validate_batch_id(
    value: &str,
) -> Result<(), AgentArtifactRunTrashError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
        })
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(AgentArtifactRunTrashError::InvalidBatchId(
            value.to_string(),
        ));
    }
    Ok(())
}

pub(super) fn valid_run_parts(timestamp: &str) -> Option<(&str, &str)> {
    if !valid_run_timestamp(timestamp) {
        return None;
    }
    Some((&timestamp[..6], &timestamp[6..8]))
}

pub(super) fn item_result(
    candidate: &AgentArtifactRunCandidateWire,
    outcome: &str,
    reasons: Vec<String>,
    detail: Option<String>,
    bytes: u64,
) -> AgentArtifactRunTrashItemResultWire {
    AgentArtifactRunTrashItemResultWire {
        artifact_dir: candidate.artifact_dir.clone(),
        project: candidate.project.clone(),
        timestamp: candidate.timestamp.clone(),
        outcome: outcome.to_string(),
        reasons,
        detail,
        bytes,
    }
}

pub(super) fn append_journal_line(
    file: &mut File,
    value: &Value,
) -> std::io::Result<()> {
    serde_json::to_writer(&mut *file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

pub(super) fn open_journal_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().append(true).open(path)
}

pub(super) fn expected_original_path(project: &str, timestamp: &str) -> String {
    format!(
        "{project}/artifacts/ace-run/{}/{}/{}",
        &timestamp[..6],
        &timestamp[6..8],
        timestamp
    )
}

pub(super) fn is_expected_original_path(
    path: &str,
    project: &str,
    timestamp: &str,
) -> bool {
    valid_project_component(project)
        && valid_run_timestamp(timestamp)
        && path == expected_original_path(project, timestamp)
}

pub(super) fn read_journal(batch_dir: &Path) -> JournalInfo {
    let path = batch_dir.join("journal.jsonl");
    let mut info = JournalInfo::default();
    let Ok(mut file) = File::open(path) else {
        info.unreadable = true;
        return info;
    };
    let mut content = String::new();
    if file.read_to_string(&mut content).is_err() {
        info.unreadable = true;
        return info;
    }
    for line in content.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            info.unreadable = true;
            continue;
        };
        match value.get("kind").and_then(Value::as_str) {
            Some("header") => {
                info.batch_id = value
                    .get("batch_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                info.created_at = value
                    .get("created_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
            }
            Some("item") => {
                if let (Some(project), Some(timestamp), Some(path)) = (
                    value.get("project").and_then(Value::as_str),
                    value.get("timestamp").and_then(Value::as_str),
                    value.get("original_path").and_then(Value::as_str),
                ) {
                    if is_expected_original_path(path, project, timestamp) {
                        info.original_paths.insert(
                            (project.to_string(), timestamp.to_string()),
                            path.to_string(),
                        );
                    } else {
                        info.unreadable = true;
                    }
                } else {
                    info.unreadable = true;
                }
            }
            Some("footer") => {
                info.status = value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
            }
            Some("restore_item") | Some("purge_item") => {}
            _ => info.unreadable = true,
        }
    }
    info
}

pub(super) fn collect_trash_runs(
    batch_dir: &Path,
    journal: &JournalInfo,
) -> Vec<TrashRun> {
    let runs_root = batch_dir.join("runs");
    let Ok(project_entries) = fs::read_dir(&runs_root) else {
        return Vec::new();
    };
    let mut runs = Vec::new();
    for project_entry in project_entries.filter_map(Result::ok) {
        let project = project_entry.file_name().to_string_lossy().into_owned();
        if !valid_project_component(&project)
            || !safe_real_dir(&project_entry.path())
        {
            continue;
        }
        let Ok(month_entries) = fs::read_dir(project_entry.path()) else {
            continue;
        };
        for month_entry in month_entries.filter_map(Result::ok) {
            let month = month_entry.file_name().to_string_lossy().into_owned();
            if month.len() != 6
                || !month.bytes().all(|byte| byte.is_ascii_digit())
                || !safe_real_dir(&month_entry.path())
            {
                continue;
            }
            let Ok(day_entries) = fs::read_dir(month_entry.path()) else {
                continue;
            };
            for day_entry in day_entries.filter_map(Result::ok) {
                let day = day_entry.file_name().to_string_lossy().into_owned();
                if day.len() != 2
                    || !day.bytes().all(|byte| byte.is_ascii_digit())
                    || !safe_real_dir(&day_entry.path())
                {
                    continue;
                }
                let Ok(run_entries) = fs::read_dir(day_entry.path()) else {
                    continue;
                };
                for run_entry in run_entries.filter_map(Result::ok) {
                    let timestamp =
                        run_entry.file_name().to_string_lossy().into_owned();
                    if !valid_run_timestamp(&timestamp)
                        || !timestamp.starts_with(&month)
                        || timestamp[6..8] != day
                        || !safe_real_dir(&run_entry.path())
                    {
                        continue;
                    }
                    let journaled = journal
                        .original_paths
                        .contains_key(&(project.clone(), timestamp.clone()));
                    let original_relative_path = journal
                        .original_paths
                        .get(&(project.clone(), timestamp.clone()))
                        .cloned()
                        .unwrap_or_else(|| {
                            expected_original_path(&project, &timestamp)
                        });
                    runs.push(TrashRun {
                        trash_path: run_entry.path(),
                        project: project.clone(),
                        timestamp,
                        original_relative_path,
                        bytes: tree_size(&run_entry.path()),
                        journaled,
                    });
                }
            }
        }
    }
    runs.sort_by(|left, right| {
        (&left.timestamp, &left.project, &left.original_relative_path).cmp(&(
            &right.timestamp,
            &right.project,
            &right.original_relative_path,
        ))
    });
    runs
}

pub(super) fn safe_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| {
        metadata.is_dir() && !metadata.file_type().is_symlink()
    })
}

pub(super) fn batch_directories(
    root: &Path,
) -> Result<Vec<(String, PathBuf)>, AgentArtifactRunTrashError> {
    let mut output = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let id = entry.file_name().to_string_lossy().into_owned();
        if validate_batch_id(&id).is_ok() && safe_real_dir(&path) {
            output.push((id, path));
        }
    }
    output.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(output)
}

pub(super) fn ensure_safe_batch(
    root: &Path,
    batch: &Path,
) -> Result<(), AgentArtifactRunTrashError> {
    let canonical_root = canonicalize_existing(root).ok_or_else(|| {
        AgentArtifactRunTrashError::UnsafeTrashRoot(root.display().to_string())
    })?;
    let canonical_batch = canonicalize_existing(batch).ok_or_else(|| {
        AgentArtifactRunTrashError::UnsafeTrashRoot(batch.display().to_string())
    })?;
    if canonical_batch == canonical_root
        || !canonical_batch.starts_with(&canonical_root)
        || !path_has_no_symlink_components(&canonical_root, &canonical_batch)
    {
        return Err(AgentArtifactRunTrashError::UnsafeTrashRoot(
            batch.display().to_string(),
        ));
    }
    Ok(())
}

pub(super) fn safe_trash_item(root: &Path, item: &Path) -> bool {
    let Some(canonical_root) = canonicalize_existing(root) else {
        return false;
    };
    let Some(canonical_item) = canonicalize_existing(item) else {
        return false;
    };
    canonical_item != canonical_root
        && canonical_item.starts_with(&canonical_root)
        && path_has_no_symlink_components(&canonical_root, item)
        && safe_real_dir(item)
}

pub(super) fn path_has_no_symlink_components(
    root: &Path,
    target: &Path,
) -> bool {
    let Some(relative) = target.strip_prefix(root).ok() else {
        return false;
    };
    let mut current = root.to_path_buf();
    for component in relative.components() {
        match component {
            Component::Normal(part) => current.push(part),
            _ => return false,
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return false,
            Ok(_) => {}
            Err(_) => return false,
        }
    }
    true
}

pub(super) fn create_directories_no_symlinks(
    root: &Path,
    target: &Path,
) -> std::io::Result<bool> {
    let Ok(relative) = target.strip_prefix(root) else {
        return Ok(false);
    };
    let mut current = root.to_path_buf();
    for component in relative.components() {
        match component {
            Component::Normal(part) => current.push(part),
            _ => return Ok(false),
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata)
                if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(create_error)
                        if create_error.kind()
                            == std::io::ErrorKind::AlreadyExists =>
                    {
                        let metadata = fs::symlink_metadata(&current)?;
                        if !metadata.is_dir()
                            || metadata.file_type().is_symlink()
                        {
                            return Ok(false);
                        }
                    }
                    Err(create_error) => return Err(create_error),
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

pub(super) fn cleanup_empty_batch(batch_dir: &Path) -> std::io::Result<()> {
    let runs = batch_dir.join("runs");
    if safe_real_dir(&runs) {
        for entry in fs::read_dir(&runs)? {
            let path = entry?.path();
            if safe_real_dir(&path) {
                remove_empty_directories(&path)?;
            }
        }
        if fs::read_dir(&runs)?.next().is_none() {
            fs::remove_dir(&runs)?;
        }
    }

    let mut entries = fs::read_dir(batch_dir)?;
    let mut only_journal = true;
    for entry in entries.by_ref() {
        let entry = entry?;
        if entry.file_name() != "journal.jsonl" {
            only_journal = false;
        }
    }
    if only_journal {
        let journal = batch_dir.join("journal.jsonl");
        if let Ok(metadata) = fs::symlink_metadata(&journal) {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Ok(());
            }
            fs::remove_file(journal)?;
        }
        fs::remove_dir(batch_dir)?;
    }
    Ok(())
}

fn remove_empty_directories(path: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(path)? {
        let child = entry?.path();
        if safe_real_dir(&child) {
            remove_empty_directories(&child)?;
        }
    }
    if fs::read_dir(path)?.next().is_none() {
        fs::remove_dir(path)?;
    }
    Ok(())
}

pub(super) fn directory_modified_at(path: &Path) -> String {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .map(DateTime::<Utc>::from)
        .map(|date| date.to_rfc3339())
        .unwrap_or_default()
}

pub(super) fn normalized_protected_dirs(
    raw: &[String],
    projects_root: &Path,
) -> BTreeSet<PathBuf> {
    raw.iter()
        .map(|path| normalize_path(Path::new(path), projects_root))
        .collect()
}

pub(super) fn normalized_path_matches(
    path: &Path,
    protected: &BTreeSet<PathBuf>,
    projects_root: &Path,
) -> bool {
    protected.contains(&normalize_path(path, projects_root))
}

pub(super) fn normalize_path(path: &Path, canonical_root: &Path) -> PathBuf {
    if let Some(canonical) = canonicalize_existing(path) {
        return canonical;
    }
    let Some(absolute) = absolutize_lexically(path) else {
        return path.to_path_buf();
    };
    let mut cursor = absolute.clone();
    let mut suffix = Vec::new();
    let mut anchor = None;
    loop {
        if let Some(canonical) = canonicalize_existing(&cursor) {
            anchor = Some(canonical);
            break;
        }
        let Some(name) = cursor.file_name() else {
            break;
        };
        suffix.push(name.to_os_string());
        if !cursor.pop() {
            break;
        }
    }
    let Some(mut normalized) = anchor else {
        return absolute;
    };
    for part in suffix.iter().rev() {
        normalized.push(part);
    }
    if normalized.starts_with(canonical_root) {
        normalized
    } else {
        absolute
    }
}

pub(super) fn absolutize_lexically(path: &Path) -> Option<PathBuf> {
    let mut output = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().ok()?
    };
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => output.push(prefix.as_os_str()),
            Component::RootDir => output.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                output.pop();
            }
            Component::Normal(part) => output.push(part),
        }
    }
    Some(output)
}

pub(super) enum RestoreOneError {
    Conflict,
    Unsafe(String),
    Io(std::io::Error),
}

pub(super) fn restore_one(
    entry: &TrashRun,
    projects_root: &Path,
) -> Result<PathBuf, RestoreOneError> {
    if !is_expected_original_path(
        &entry.original_relative_path,
        &entry.project,
        &entry.timestamp,
    ) {
        return Err(RestoreOneError::Unsafe(
            "journal path did not match trash layout".to_string(),
        ));
    }
    let target = projects_root.join(&entry.original_relative_path);
    if fs::symlink_metadata(&target).is_ok() {
        return Err(RestoreOneError::Conflict);
    }
    let parent = target.parent().ok_or_else(|| {
        RestoreOneError::Unsafe("missing restore parent".to_string())
    })?;
    if !create_directories_no_symlinks(projects_root, parent)
        .map_err(RestoreOneError::Io)?
    {
        return Err(RestoreOneError::Unsafe(
            "restore path contains a symlink".to_string(),
        ));
    }
    if fs::symlink_metadata(&target).is_ok() {
        return Err(RestoreOneError::Conflict);
    }
    fs::rename(&entry.trash_path, &target).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            RestoreOneError::Conflict
        } else {
            RestoreOneError::Io(error)
        }
    })?;
    Ok(target)
}

pub(super) fn trash_item_result(
    entry: &TrashRun,
    outcome: &str,
    reasons: Vec<String>,
    detail: Option<String>,
) -> AgentArtifactRunTrashItemResultWire {
    AgentArtifactRunTrashItemResultWire {
        artifact_dir: entry.trash_path.to_string_lossy().into_owned(),
        project: entry.project.clone(),
        timestamp: entry.timestamp.clone(),
        outcome: outcome.to_string(),
        reasons,
        detail,
        bytes: entry.bytes,
    }
}

#[cfg(unix)]
pub(super) fn same_device(
    source: &fs::Metadata,
    destination: &fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt;
    source.dev() == destination.dev()
}

#[cfg(not(unix))]
pub(super) fn same_device(
    _source: &fs::Metadata,
    _destination: &fs::Metadata,
) -> bool {
    false
}
