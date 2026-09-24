//! Deterministic cleanup execution helpers.
//!
//! These helpers intentionally avoid process signalling and UI concerns.
//! Callers provide already-serialized bundle data, project-file text, or
//! wire entries; Rust performs the repeatable file/text mutation and returns
//! structured results for the host to report.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::wire::{CommentWire, HookWire, MentorWire};

use super::wire::AgentCleanupIdentityWire;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupBundleWriteResultWire {
    pub path: String,
    pub filename: String,
    pub shard: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupArtifactDeleteResultWire {
    pub deleted_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupWorkspaceReleaseResultWire {
    pub content: String,
    pub removed: bool,
    pub has_remaining_claims: bool,
}

pub fn save_dismissed_agents_index(
    path: &Path,
    dismissed: &[AgentCleanupIdentityWire],
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    let entries: Vec<(&str, &str, Option<&str>)> = dismissed
        .iter()
        .map(|identity| {
            (
                identity.agent_type.as_str(),
                identity.cl_name.as_str(),
                identity.raw_suffix.as_deref(),
            )
        })
        .collect();
    let payload = serde_json::to_string_pretty(&entries).map_err(|e| {
        format!("failed to serialize dismissed agent index: {e}")
    })?;
    write_file_atomically(path, payload.as_bytes())
}

/// Read one dismissed identity from either on-disk shape: the legacy
/// `[agent_type, cl_name, raw_suffix]` triple or the
/// `{agent_type, cl_name, raw_suffix}` dict.
fn dismissed_identity_from_json_value(
    value: &JsonValue,
) -> Option<AgentCleanupIdentityWire> {
    if let Some(triple) = value.as_array() {
        if triple.len() != 3 {
            return None;
        }
        let agent_type = triple[0].as_str()?;
        let cl_name = triple[1].as_str()?;
        let raw_suffix = match &triple[2] {
            JsonValue::Null => None,
            JsonValue::String(suffix) => Some(suffix.clone()),
            _ => return None,
        };
        return Some(AgentCleanupIdentityWire {
            agent_type: agent_type.to_string(),
            cl_name: cl_name.to_string(),
            raw_suffix,
        });
    }
    if let Some(record) = value.as_object() {
        let agent_type = record.get("agent_type")?.as_str()?;
        let cl_name = record.get("cl_name")?.as_str()?;
        let raw_suffix = match record.get("raw_suffix") {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::String(suffix)) => Some(suffix.clone()),
            Some(_) => return None,
        };
        return Some(AgentCleanupIdentityWire {
            agent_type: agent_type.to_string(),
            cl_name: cl_name.to_string(),
            raw_suffix,
        });
    }
    None
}

fn load_dismissed_index_entries(
    path: &Path,
) -> Result<Vec<AgentCleanupIdentityWire>, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(err) => return Err(err.to_string()),
    };
    let value: JsonValue = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            let backup = corrupt_backup_path(path, stamp);
            fs::rename(path, &backup).map_err(|e| e.to_string())?;
            return Ok(Vec::new());
        }
    };
    let entries = value.as_array().ok_or_else(|| {
        format!(
            "dismissed agent index is not a JSON array: {}",
            path.to_string_lossy()
        )
    })?;
    let mut identities = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(identity) = dismissed_identity_from_json_value(entry) else {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            let backup = corrupt_backup_path(path, stamp);
            fs::rename(path, &backup).map_err(|e| e.to_string())?;
            return Ok(Vec::new());
        };
        identities.push(identity);
    }
    Ok(identities)
}

fn sort_dismissed_identities(identities: &mut Vec<AgentCleanupIdentityWire>) {
    identities.sort_by(|left, right| {
        left.agent_type
            .cmp(&right.agent_type)
            .then(left.cl_name.cmp(&right.cl_name))
            .then(left.raw_suffix.cmp(&right.raw_suffix))
    });
    identities.dedup();
}

fn corrupt_backup_path(path: &Path, stamp: u64) -> std::path::PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "dismissed_agents.json".to_string());
    path.with_file_name(format!("{file_name}.corrupt-{stamp}"))
}

fn dismissed_index_lock_path(path: &Path) -> std::path::PathBuf {
    let name = path
        .file_name()
        .map(|name| format!("{}.lock", name.to_string_lossy()))
        .unwrap_or_else(|| "dismissed_agents.lock".to_string());
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => std::path::PathBuf::from(name),
    }
}

/// Atomically add and remove dismissed identities under an exclusive lock.
///
/// Holds an exclusive advisory lock on a sibling `<file>.lock` file, reads
/// the current entries, applies the removals and then the additions, writes
/// the merged set back atomically, and returns the resulting set. A missing
/// file counts as empty. An unparsable file is renamed to
/// `<file>.corrupt-<unix_ts>` and treated as empty, never silently
/// overwritten.
pub fn update_dismissed_agents_index(
    path: &Path,
    additions: &[AgentCleanupIdentityWire],
    removals: &[AgentCleanupIdentityWire],
) -> Result<Vec<AgentCleanupIdentityWire>, String> {
    use fs2::FileExt;
    use std::fs::OpenOptions;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    let lock_path = dismissed_index_lock_path(path);
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| e.to_string())?;
    lock_file.lock_exclusive().map_err(|e| e.to_string())?;

    let mut current = load_dismissed_index_entries(path)?;
    let remove_set: BTreeSet<AgentCleanupIdentityWire> =
        removals.iter().cloned().collect();
    current.retain(|identity| !remove_set.contains(identity));
    for identity in additions {
        if !current.contains(identity) {
            current.push(identity.clone());
        }
    }
    sort_dismissed_identities(&mut current);
    save_dismissed_agents_index(path, &current)?;
    Ok(current)
}

fn write_file_atomically(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().filter(|dir| !dir.as_os_str().is_empty());
    let file_name = path.file_name().ok_or_else(|| {
        format!(
            "dismissed agent index path has no file name: {}",
            path.to_string_lossy()
        )
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_name = format!(
        ".{}.tmp.{}.{}",
        file_name.to_string_lossy(),
        std::process::id(),
        nonce
    );
    let tmp_path = match parent {
        Some(dir) => dir.join(tmp_name),
        None => std::path::PathBuf::from(tmp_name),
    };
    write_file_synced(&tmp_path, bytes)?;
    fs::rename(&tmp_path, path).map_err(|e| e.to_string())?;
    if let Some(dir) = parent {
        sync_dir(dir);
    }
    Ok(())
}

pub fn bundle_filename_from_json(bundle: &JsonValue) -> Result<String, String> {
    let raw_suffix = bundle
        .get("raw_suffix")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "dismissed bundle missing raw_suffix".to_string())?;
    let is_child = bundle
        .get("is_workflow_child")
        .and_then(JsonValue::as_bool)
        .unwrap_or_else(|| {
            bundle
                .get("parent_workflow")
                .is_some_and(|value| !value.is_null())
                || bundle
                    .get("parent_timestamp")
                    .is_some_and(|value| !value.is_null())
        });
    if is_child {
        let step_index = bundle
            .get("step_index")
            .and_then(JsonValue::as_i64)
            .unwrap_or(0);
        return Ok(format!("{raw_suffix}__c{step_index}.json"));
    }
    Ok(format!("{raw_suffix}.json"))
}

pub fn bundle_shard_for_filename(filename: &str) -> String {
    if filename.len() >= 6
        && filename.as_bytes()[0..6].iter().all(u8::is_ascii_digit)
    {
        return filename[0..6].to_string();
    }
    "000101".to_string()
}

pub fn save_dismissed_bundle_json(
    bundles_root: &Path,
    bundle: &JsonValue,
) -> Result<AgentCleanupBundleWriteResultWire, String> {
    let raw_suffix = bundle
        .get("raw_suffix")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "dismissed bundle missing raw_suffix".to_string())?;
    let filename = bundle_filename_from_json(bundle)?;
    let shard = bundle_shard_for_raw_suffix(raw_suffix);
    let shard_dir = bundles_root.join(&shard);
    fs::create_dir_all(&shard_dir).map_err(|e| e.to_string())?;
    let path = shard_dir.join(&filename);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path = shard_dir.join(format!(
        ".{filename}.tmp.{}.{}",
        std::process::id(),
        nonce
    ));
    let payload = serde_json::to_string_pretty(bundle).map_err(|e| {
        format!("failed to serialize dismissed agent bundle: {e}")
    })?;
    let mut bytes = payload.into_bytes();
    bytes.push(b'\n');
    write_file_synced(&tmp_path, &bytes)?;
    fs::rename(&tmp_path, &path).map_err(|e| e.to_string())?;
    sync_dir(&shard_dir);
    Ok(AgentCleanupBundleWriteResultWire {
        path: path_to_string(&path),
        filename,
        shard,
    })
}

fn bundle_shard_for_raw_suffix(raw_suffix: &str) -> String {
    if raw_suffix.len() >= 6
        && raw_suffix.as_bytes()[0..6].iter().all(u8::is_ascii_digit)
    {
        return raw_suffix[0..6].to_string();
    }
    "000101".to_string()
}

fn write_file_synced(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = File::create(path).map_err(|e| e.to_string())?;
    file.write_all(bytes).map_err(|e| e.to_string())?;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())
}

fn sync_dir(path: &Path) {
    if let Ok(dir) = File::open(path) {
        let _ = dir.sync_all();
    }
}

pub fn delete_agent_artifact_markers(
    artifacts_dir: &Path,
) -> Result<AgentCleanupArtifactDeleteResultWire, String> {
    let mut result = AgentCleanupArtifactDeleteResultWire::default();
    if !artifacts_dir.is_dir() {
        return Ok(result);
    }

    for filename in ["workflow_state.json", "done.json"] {
        delete_if_file(&artifacts_dir.join(filename), &mut result)?;
    }

    for entry in fs::read_dir(artifacts_dir).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str())
        else {
            continue;
        };
        if name.starts_with("prompt_step_") && name.ends_with(".json") {
            delete_if_file(&path, &mut result)?;
        }
    }
    Ok(result)
}

pub fn release_workspace_from_content(
    content: &str,
    workspace_num: i64,
    workflow: Option<&str>,
    cl_name: Option<&str>,
) -> AgentCleanupWorkspaceReleaseResultWire {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut new_lines: Vec<String> = Vec::with_capacity(lines.len());
    let mut in_running_field = false;
    let mut running_field_idx: Option<usize> = None;
    let mut has_remaining_claims = false;
    let mut removed = false;

    for line in lines {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            running_field_idx = Some(new_lines.len());
            new_lines.push(line.to_string());
            continue;
        }

        if in_running_field && line.starts_with("  ") {
            if let Some(claim) = WorkspaceClaimLine::parse(line) {
                let mut should_remove = claim.workspace_num == workspace_num;
                if let Some(expected) = workflow {
                    if claim.workflow != expected {
                        should_remove = false;
                    }
                }
                if let Some(expected) = cl_name {
                    if claim.cl_name.as_deref() != Some(expected) {
                        should_remove = false;
                    }
                }
                if should_remove {
                    removed = true;
                    continue;
                }
                has_remaining_claims = true;
            }
        } else {
            in_running_field = false;
        }

        new_lines.push(line.to_string());
    }

    if let Some(idx) = running_field_idx {
        if !has_remaining_claims {
            new_lines.remove(idx);
        }
    }

    let joined = new_lines.join("\n");
    let content = if has_remaining_claims {
        normalize_running_field_spacing(&joined)
    } else {
        clean_orphaned_blank_lines(&joined)
    };

    AgentCleanupWorkspaceReleaseResultWire {
        content,
        removed,
        has_remaining_claims,
    }
}

pub fn mark_hook_agents_as_killed(
    hooks: &[HookWire],
    suffixes: &[String],
) -> Vec<HookWire> {
    let suffixes: BTreeSet<&str> =
        suffixes.iter().map(String::as_str).collect();
    hooks
        .iter()
        .cloned()
        .map(|mut hook| {
            for line in &mut hook.status_lines {
                if line.suffix_type.as_deref() == Some("running_agent")
                    && line
                        .suffix
                        .as_deref()
                        .is_some_and(|suffix| suffixes.contains(suffix))
                {
                    line.suffix_type = Some("killed_agent".to_string());
                }
            }
            hook
        })
        .collect()
}

pub fn mark_mentor_agents_as_killed(
    mentors: &[MentorWire],
    suffixes: &[String],
) -> Vec<MentorWire> {
    let suffixes: BTreeSet<&str> =
        suffixes.iter().map(String::as_str).collect();
    mentors
        .iter()
        .cloned()
        .map(|mut mentor| {
            for line in &mut mentor.status_lines {
                if line.suffix_type.as_deref() == Some("running_agent")
                    && line
                        .suffix
                        .as_deref()
                        .is_some_and(|suffix| suffixes.contains(suffix))
                {
                    line.suffix_type = Some("killed_agent".to_string());
                }
            }
            mentor
        })
        .collect()
}

pub fn mark_comment_agents_as_killed(
    comments: &[CommentWire],
    suffixes: &[String],
) -> Vec<CommentWire> {
    let suffixes: BTreeSet<&str> =
        suffixes.iter().map(String::as_str).collect();
    comments
        .iter()
        .cloned()
        .map(|mut comment| {
            if comment.suffix_type.as_deref() == Some("running_agent")
                && comment
                    .suffix
                    .as_deref()
                    .is_some_and(|suffix| suffixes.contains(suffix))
            {
                comment.suffix_type = Some("killed_agent".to_string());
            }
            comment
        })
        .collect()
}

fn delete_if_file(
    path: &Path,
    result: &mut AgentCleanupArtifactDeleteResultWire,
) -> Result<(), String> {
    if !path.is_file() {
        return Ok(());
    }
    fs::remove_file(path).map_err(|e| e.to_string())?;
    result.deleted_paths.push(path_to_string(path));
    Ok(())
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceClaimLine {
    workspace_num: i64,
    workflow: String,
    cl_name: Option<String>,
}

impl WorkspaceClaimLine {
    fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        if !trimmed.starts_with('#') {
            return None;
        }
        let parts: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        if parts.len() < 4 {
            return None;
        }
        let workspace_num = parts[0].strip_prefix('#')?.parse::<i64>().ok()?;
        let _pid = parts[1].parse::<i64>().ok()?;
        Some(Self {
            workspace_num,
            workflow: parts[2].to_string(),
            cl_name: if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            },
        })
    }
}

fn normalize_running_field_spacing(content: &str) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut result_lines = Vec::with_capacity(lines.len());
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        if line.starts_with("RUNNING:") {
            result_lines.push(line.to_string());
            i += 1;
            while i < lines.len() {
                let entry_line = lines[i];
                if entry_line.starts_with("  ")
                    && entry_line.trim().starts_with('#')
                {
                    result_lines.push(entry_line.to_string());
                    i += 1;
                } else {
                    break;
                }
            }
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            if i < lines.len() {
                result_lines.push(String::new());
                result_lines.push(String::new());
            }
        } else {
            result_lines.push(line.to_string());
            i += 1;
        }
    }

    result_lines.join("\n")
}

fn clean_orphaned_blank_lines(content: &str) -> String {
    let mut result_lines = Vec::new();
    let mut consecutive_blank_count = 0;

    for line in content.split('\n') {
        if line.trim().is_empty() {
            consecutive_blank_count += 1;
            if consecutive_blank_count > 2 {
                continue;
            }
        } else {
            consecutive_blank_count = 0;
        }
        result_lines.push(line.to_string());
    }

    result_lines.join("\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::wire::{
        CommentWire, HookStatusLineWire, HookWire, MentorStatusLineWire,
        MentorWire,
    };

    #[test]
    fn bundle_filename_and_shard_match_dismissed_layout() {
        let parent = json!({
            "raw_suffix": "20260430010203",
            "is_workflow_child": false
        });
        let child = json!({
            "raw_suffix": "20260430010203",
            "is_workflow_child": true,
            "step_index": 2
        });

        assert_eq!(
            bundle_filename_from_json(&parent).unwrap(),
            "20260430010203.json"
        );
        assert_eq!(
            bundle_filename_from_json(&child).unwrap(),
            "20260430010203__c2.json"
        );
        assert_eq!(bundle_shard_for_filename("20260430010203.json"), "202604");
    }

    #[test]
    fn save_dismissed_bundle_json_writes_sharded_pretty_json() {
        let dir = TempDir::new().unwrap();
        let bundle = json!({
            "agent_type": "run",
            "cl_name": "demo",
            "project_file": "/tmp/p.sase",
            "status": "DONE",
            "start_time": "2026-04-30T01:02:03",
            "raw_suffix": "20260430010203"
        });

        let result = save_dismissed_bundle_json(dir.path(), &bundle).unwrap();

        assert_eq!(result.filename, "20260430010203.json");
        assert_eq!(result.shard, "202604");
        let written = fs::read_to_string(result.path).unwrap();
        assert!(written.contains("\"cl_name\": \"demo\""));
    }

    #[test]
    fn save_dismissed_bundle_json_replaces_existing_bundle() {
        let dir = TempDir::new().unwrap();
        let bundle = json!({
            "agent_type": "run",
            "cl_name": "demo",
            "project_file": "/tmp/p.sase",
            "status": "DONE",
            "start_time": "2026-04-30T01:02:03",
            "raw_suffix": "20260430010203"
        });

        save_dismissed_bundle_json(dir.path(), &bundle).unwrap();
        let updated = json!({
            "agent_type": "run",
            "cl_name": "demo",
            "project_file": "/tmp/p.sase",
            "status": "FAILED",
            "start_time": "2026-04-30T01:02:03",
            "raw_suffix": "20260430010203"
        });
        let result = save_dismissed_bundle_json(dir.path(), &updated).unwrap();
        let written = fs::read_to_string(result.path).unwrap();

        assert!(written.contains("\"status\": \"FAILED\""));
    }

    #[test]
    fn release_workspace_text_removes_matching_claim_and_cleans_field() {
        let content = "RUNNING:\n  #1 | 111 | wf | cl\n  #2 | 222 | other | cl\n\n\nNAME: cl\nSTATUS: WIP\n";

        let result =
            release_workspace_from_content(content, 1, Some("wf"), Some("cl"));

        assert!(result.removed);
        assert!(result.has_remaining_claims);
        assert!(!result.content.contains("#1 | 111"));
        assert!(result.content.contains("#2 | 222"));
        assert!(result
            .content
            .contains("RUNNING:\n  #2 | 222 | other | cl\n\n\nNAME: cl"));
    }

    #[test]
    fn release_workspace_text_removes_empty_running_field() {
        let content =
            "RUNNING:\n  #1 | 111 | wf | cl\n\n\nNAME: cl\nSTATUS: WIP\n";

        let result =
            release_workspace_from_content(content, 1, Some("wf"), Some("cl"));

        assert!(result.removed);
        assert!(!result.has_remaining_claims);
        assert!(!result.content.contains("RUNNING:"));
        assert!(result.content.starts_with("\n\nNAME: cl"));
    }

    #[test]
    fn delete_agent_artifact_markers_removes_loader_files_only() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("workflow_state.json"), "{}").unwrap();
        fs::write(dir.path().join("done.json"), "{}").unwrap();
        fs::write(dir.path().join("prompt_step_1.json"), "{}").unwrap();
        fs::write(dir.path().join("keep.json"), "{}").unwrap();

        let result = delete_agent_artifact_markers(dir.path()).unwrap();

        assert_eq!(result.deleted_paths.len(), 3);
        assert!(!dir.path().join("workflow_state.json").exists());
        assert!(dir.path().join("keep.json").exists());
    }

    #[test]
    fn mark_running_hook_mentor_and_comment_suffixes_as_killed() {
        let hooks = vec![HookWire {
            command: "test".to_string(),
            status_lines: vec![HookStatusLineWire {
                commit_entry_num: "1".to_string(),
                timestamp: "20260430010203".to_string(),
                status: "RUNNING".to_string(),
                duration: None,
                suffix: Some("agent-123".to_string()),
                suffix_type: Some("running_agent".to_string()),
                summary: None,
            }],
        }];
        let mentors = vec![MentorWire {
            entry_id: "e1".to_string(),
            profiles: vec!["p".to_string()],
            status_lines: vec![MentorStatusLineWire {
                profile_name: "p".to_string(),
                mentor_name: "m".to_string(),
                status: "RUNNING".to_string(),
                timestamp: Some("20260430010203".to_string()),
                duration: None,
                suffix: Some("agent-123".to_string()),
                suffix_type: Some("running_agent".to_string()),
            }],
            is_draft: false,
        }];
        let comments = vec![CommentWire {
            reviewer: "crs".to_string(),
            file_path: "/tmp/comments.json".to_string(),
            suffix: Some("agent-123".to_string()),
            suffix_type: Some("running_agent".to_string()),
        }];
        let suffixes = vec!["agent-123".to_string()];

        let hooks = mark_hook_agents_as_killed(&hooks, &suffixes);
        let mentors = mark_mentor_agents_as_killed(&mentors, &suffixes);
        let comments = mark_comment_agents_as_killed(&comments, &suffixes);

        assert_eq!(
            hooks[0].status_lines[0].suffix_type.as_deref(),
            Some("killed_agent")
        );
        assert_eq!(
            mentors[0].status_lines[0].suffix_type.as_deref(),
            Some("killed_agent")
        );
        assert_eq!(comments[0].suffix_type.as_deref(), Some("killed_agent"));
    }

    fn dismissed_identity(
        cl_name: &str,
        raw_suffix: Option<&str>,
    ) -> AgentCleanupIdentityWire {
        AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: cl_name.to_string(),
            raw_suffix: raw_suffix.map(str::to_string),
        }
    }

    #[test]
    fn concurrent_index_updates_lose_no_identities() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dismissed_agents.json");

        std::thread::scope(|scope| {
            for worker in 0..8 {
                let path = path.clone();
                scope.spawn(move || {
                    let additions: Vec<AgentCleanupIdentityWire> = (0..8)
                        .map(|slot| {
                            dismissed_identity(
                                &format!("worker-{worker}"),
                                Some(&format!("ts-{worker}-{slot}")),
                            )
                        })
                        .collect();
                    update_dismissed_agents_index(&path, &additions, &[])
                        .unwrap();
                });
            }
        });

        let final_entries =
            update_dismissed_agents_index(&path, &[], &[]).unwrap();
        assert_eq!(final_entries.len(), 64);
    }

    #[test]
    fn index_removals_and_additions_compose() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dismissed_agents.json");
        let keep = dismissed_identity("keep", Some("ts-keep"));
        let drop = dismissed_identity("drop", Some("ts-drop"));
        let fresh = dismissed_identity("fresh", Some("ts-fresh"));

        update_dismissed_agents_index(
            &path,
            &[keep.clone(), drop.clone()],
            &[],
        )
        .unwrap();
        let result = update_dismissed_agents_index(
            &path,
            std::slice::from_ref(&fresh),
            std::slice::from_ref(&drop),
        )
        .unwrap();

        assert_eq!(result, vec![fresh, keep]);
    }

    #[test]
    fn corrupt_index_is_preserved_not_overwritten() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dismissed_agents.json");
        fs::write(&path, "{not valid json").unwrap();
        let fresh = dismissed_identity("fresh", Some("ts-fresh"));

        let result = update_dismissed_agents_index(
            &path,
            std::slice::from_ref(&fresh),
            &[],
        )
        .unwrap();

        assert_eq!(result, vec![fresh]);
        let corrupt: Vec<std::path::PathBuf> = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|candidate| {
                candidate
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("dismissed_agents.json.corrupt-")
                    })
            })
            .collect();
        assert_eq!(corrupt.len(), 1);
        assert_eq!(fs::read_to_string(&corrupt[0]).unwrap(), "{not valid json");
    }

    #[test]
    fn index_reads_both_triple_and_dict_entry_shapes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dismissed_agents.json");
        fs::write(
            &path,
            serde_json::to_string_pretty(&serde_json::json!([
                ["run", "legacy", "ts-legacy"],
                {
                    "agent_type": "run",
                    "cl_name": "shaped",
                    "raw_suffix": "ts-shaped",
                },
            ]))
            .unwrap(),
        )
        .unwrap();

        let result = update_dismissed_agents_index(&path, &[], &[]).unwrap();

        assert_eq!(
            result,
            vec![
                dismissed_identity("legacy", Some("ts-legacy")),
                dismissed_identity("shaped", Some("ts-shaped")),
            ]
        );
    }
}
