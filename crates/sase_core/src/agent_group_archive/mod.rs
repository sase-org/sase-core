//! Canonical backend operations for saved dismissed-agent groups.
//!
//! Groups are lightweight metadata records that reference existing
//! per-agent dismissed bundles. The group archive deliberately stores enough
//! summary data for frontends to page and preview saved groups without
//! reading every bundle payload.

pub mod wire;

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub use wire::{
    SavedAgentGroupPageWire, SavedAgentGroupRefWire,
    SavedAgentGroupSummaryWire, SavedAgentGroupWire,
    AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION,
};

pub fn save_dismissed_agent_group(
    root: &Path,
    group: SavedAgentGroupWire,
) -> Result<SavedAgentGroupWire, String> {
    let group = normalize_group(group)?;
    let path = group_path(root, &group.group_id)?;
    let value = serde_json::to_value(&group)
        .map_err(|e| format!("failed to serialize saved agent group: {e}"))?;
    write_json_file_atomic(&path, &value)?;
    Ok(group)
}

pub fn list_dismissed_agent_groups(
    root: &Path,
    limit: i64,
    cursor: Option<i64>,
) -> SavedAgentGroupPageWire {
    if limit <= 0 {
        return SavedAgentGroupPageWire {
            groups: Vec::new(),
            next_cursor: None,
        };
    }

    let offset = cursor.unwrap_or(0).max(0) as usize;
    let limit_usize = limit as usize;
    let mut groups: Vec<SavedAgentGroupWire> = iter_group_paths(root)
        .into_iter()
        .filter_map(|path| read_group_file(&path).ok().flatten())
        .collect();
    groups.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.group_id.cmp(&b.group_id))
    });

    let mut page: Vec<SavedAgentGroupSummaryWire> = groups
        .into_iter()
        .skip(offset)
        .take(limit_usize + 1)
        .map(summary_from_group)
        .collect();
    let next_cursor = if page.len() > limit_usize {
        page.truncate(limit_usize);
        Some((offset + limit_usize) as i64)
    } else {
        None
    };

    SavedAgentGroupPageWire {
        groups: page,
        next_cursor,
    }
}

pub fn load_dismissed_agent_group(
    root: &Path,
    group_id: &str,
) -> Result<Option<SavedAgentGroupWire>, String> {
    let path = group_path(root, group_id)?;
    read_group_file(&path)
}

pub fn mark_dismissed_agent_group_revived(
    root: &Path,
    group_id: &str,
    revived_at: &str,
) -> Result<Option<SavedAgentGroupWire>, String> {
    let Some(mut group) = load_dismissed_agent_group(root, group_id)? else {
        return Ok(None);
    };
    group.revived_at = Some(revived_at.to_string());
    group.times_revived = group.times_revived.max(0) + 1;
    save_dismissed_agent_group(root, group).map(Some)
}

pub fn delete_dismissed_agent_group(
    root: &Path,
    group_id: &str,
) -> Result<bool, String> {
    let path = group_path(root, group_id)?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

pub fn record_recent_dismissed_agent_group(
    root: &Path,
    group: SavedAgentGroupWire,
    limit: i64,
) -> Result<SavedAgentGroupWire, String> {
    let saved = save_dismissed_agent_group(root, group)?;
    prune_recent_dismissed_agent_groups(root, limit)?;
    Ok(saved)
}

pub fn list_recent_dismissed_agent_groups(
    root: &Path,
    limit: i64,
) -> SavedAgentGroupPageWire {
    let mut page = list_dismissed_agent_groups(root, limit, None);
    page.next_cursor = None;
    page
}

pub fn load_recent_dismissed_agent_group(
    root: &Path,
    group_id: &str,
) -> Result<Option<SavedAgentGroupWire>, String> {
    load_dismissed_agent_group(root, group_id)
}

pub fn mark_recent_dismissed_agent_group_revived(
    root: &Path,
    group_id: &str,
    revived_at: &str,
) -> Result<Option<SavedAgentGroupWire>, String> {
    mark_dismissed_agent_group_revived(root, group_id, revived_at)
}

fn normalize_group(
    mut group: SavedAgentGroupWire,
) -> Result<SavedAgentGroupWire, String> {
    validate_group_id(&group.group_id)?;
    if group.schema_version != AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "saved agent group schema mismatch: got {}, expected {}",
            group.schema_version, AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION
        ));
    }
    if group.created_at.trim().is_empty() {
        return Err(
            "saved agent group created_at must not be empty".to_string()
        );
    }
    if group.source.trim().is_empty() {
        return Err("saved agent group source must not be empty".to_string());
    }
    if group.title.trim().is_empty() {
        return Err("saved agent group title must not be empty".to_string());
    }
    group.name = normalize_optional_name(group.name);
    if group.agent_count < 0 || group.top_level_agent_count < 0 {
        return Err("saved agent group counts must be non-negative".to_string());
    }
    group.times_revived = group.times_revived.max(0);
    Ok(group)
}

fn summary_from_group(
    group: SavedAgentGroupWire,
) -> SavedAgentGroupSummaryWire {
    SavedAgentGroupSummaryWire {
        schema_version: group.schema_version,
        group_id: group.group_id,
        created_at: group.created_at,
        source: group.source,
        title: group.title,
        name: group.name,
        agent_count: group.agent_count,
        top_level_agent_count: group.top_level_agent_count,
        status_counts: group.status_counts,
        project_names: group.project_names,
        cl_names: group.cl_names,
        revived_at: group.revived_at,
        times_revived: group.times_revived,
    }
}

fn normalize_optional_name(name: Option<String>) -> Option<String> {
    name.map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn prune_recent_dismissed_agent_groups(
    root: &Path,
    limit: i64,
) -> Result<(), String> {
    let keep = limit.max(1) as usize;
    let mut groups: Vec<SavedAgentGroupWire> = iter_group_paths(root)
        .into_iter()
        .filter_map(|path| read_group_file(&path).ok().flatten())
        .collect();
    groups.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.group_id.cmp(&b.group_id))
    });
    for group in groups.into_iter().skip(keep) {
        let path = group_path(root, &group.group_id)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

fn read_group_file(path: &Path) -> Result<Option<SavedAgentGroupWire>, String> {
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None)
        }
        Err(error) => return Err(error.to_string()),
    };
    let group: SavedAgentGroupWire = match serde_json::from_str(&data) {
        Ok(group) => group,
        Err(_) => return Ok(None),
    };
    match normalize_group(group) {
        Ok(group) => Ok(Some(group)),
        Err(_) => Ok(None),
    }
}

fn iter_group_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return paths;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("json")
        {
            paths.push(path);
        } else if path.is_dir() && is_shard_dir(&path) {
            if let Ok(children) = fs::read_dir(&path) {
                for child in children.flatten() {
                    let child_path = child.path();
                    if child_path.is_file()
                        && child_path.extension().and_then(|s| s.to_str())
                            == Some("json")
                    {
                        paths.push(child_path);
                    }
                }
            }
        }
    }
    paths
}

fn group_path(root: &Path, group_id: &str) -> Result<PathBuf, String> {
    validate_group_id(group_id)?;
    Ok(root.join(format!("{group_id}.json")))
}

fn validate_group_id(group_id: &str) -> Result<(), String> {
    if group_id.is_empty() {
        return Err("saved agent group id must not be empty".to_string());
    }
    if !group_id.as_bytes().iter().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')
    }) {
        return Err(
            "saved agent group id may only contain ASCII letters, digits, '.', '-', and '_'"
                .to_string(),
        );
    }
    Ok(())
}

fn is_shard_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| {
            name.len() == 6 && name.as_bytes().iter().all(u8::is_ascii_digit)
        })
}

fn write_json_file_atomic(
    path: &Path,
    value: &serde_json::Value,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_file_name(format!(
        ".{}.tmp.{}.{}",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("group"),
        std::process::id(),
        nonce
    ));
    let payload = serde_json::to_string_pretty(value)
        .map_err(|e| format!("failed to serialize saved agent group: {e}"))?;
    {
        let mut file = File::create(&tmp_path).map_err(|e| e.to_string())?;
        file.write_all(payload.as_bytes())
            .map_err(|e| e.to_string())?;
        file.write_all(b"\n").map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
    }
    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        e.to_string()
    })?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn save_and_list_pages_saved_groups_newest_first() {
        let tmp = TempDir::new().unwrap();
        for idx in 0..25 {
            save_dismissed_agent_group(
                tmp.path(),
                sample_group(
                    &format!("group-{idx:02}"),
                    &format!("2026-05-27T12:{idx:02}:00Z"),
                ),
            )
            .unwrap();
        }

        let first = list_dismissed_agent_groups(tmp.path(), 20, None);
        assert_eq!(first.groups.len(), 20);
        assert_eq!(first.groups[0].group_id, "group-24");
        assert_eq!(first.groups[19].group_id, "group-05");
        assert_eq!(first.next_cursor, Some(20));

        let second =
            list_dismissed_agent_groups(tmp.path(), 20, first.next_cursor);
        assert_eq!(second.groups.len(), 5);
        assert_eq!(second.groups[0].group_id, "group-04");
        assert_eq!(second.groups[4].group_id, "group-00");
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn load_group_keeps_refs_when_bundle_files_are_missing() {
        let tmp = TempDir::new().unwrap();
        let mut group = sample_group("missing-bundle", "2026-05-27T12:00:00Z");
        group.agent_refs[0].bundle_path =
            Some(tmp.path().join("missing.json").display().to_string());
        save_dismissed_agent_group(tmp.path(), group).unwrap();

        let loaded = load_dismissed_agent_group(tmp.path(), "missing-bundle")
            .unwrap()
            .unwrap();

        assert_eq!(loaded.agent_refs.len(), 1);
        assert!(loaded.agent_refs[0]
            .bundle_path
            .as_deref()
            .unwrap()
            .ends_with("missing.json"));
    }

    #[test]
    fn corrupt_and_missing_group_files_are_tolerated() {
        let tmp = TempDir::new().unwrap();
        save_dismissed_agent_group(
            tmp.path(),
            sample_group("valid", "2026-05-27T12:00:00Z"),
        )
        .unwrap();
        fs::write(tmp.path().join("corrupt.json"), "not json").unwrap();

        let page = list_dismissed_agent_groups(tmp.path(), 20, None);

        assert_eq!(page.groups.len(), 1);
        assert_eq!(page.groups[0].group_id, "valid");
        assert!(load_dismissed_agent_group(tmp.path(), "missing")
            .unwrap()
            .is_none());
        assert!(load_dismissed_agent_group(tmp.path(), "corrupt")
            .unwrap()
            .is_none());
    }

    #[test]
    fn mark_group_revived_preserves_metadata() {
        let tmp = TempDir::new().unwrap();
        save_dismissed_agent_group(
            tmp.path(),
            sample_group("revivable", "2026-05-27T12:00:00Z"),
        )
        .unwrap();

        let updated = mark_dismissed_agent_group_revived(
            tmp.path(),
            "revivable",
            "2026-05-27T13:00:00Z",
        )
        .unwrap()
        .unwrap();

        assert_eq!(updated.revived_at.as_deref(), Some("2026-05-27T13:00:00Z"));
        assert_eq!(updated.times_revived, 1);
        assert_eq!(updated.title, "1 agent in cl");
        assert_eq!(updated.agent_refs.len(), 1);

        let loaded = load_dismissed_agent_group(tmp.path(), "revivable")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.times_revived, 1);
        assert_eq!(loaded.agent_refs[0].raw_suffix.as_deref(), Some("ts-1"));
        assert_eq!(loaded.agent_refs[0].tag.as_deref(), Some("backend"));
        assert_eq!(
            loaded.agent_refs[0].prompt_preview.as_deref(),
            Some("Restore this backend worker.")
        );
    }

    #[test]
    fn delete_group_removes_only_requested_metadata_record() {
        let tmp = TempDir::new().unwrap();
        save_dismissed_agent_group(
            tmp.path(),
            sample_group("delete-me", "2026-05-27T12:00:00Z"),
        )
        .unwrap();
        save_dismissed_agent_group(
            tmp.path(),
            sample_group("keep-me", "2026-05-27T12:01:00Z"),
        )
        .unwrap();

        let deleted =
            delete_dismissed_agent_group(tmp.path(), "delete-me").unwrap();
        let missing =
            delete_dismissed_agent_group(tmp.path(), "delete-me").unwrap();
        let page = list_dismissed_agent_groups(tmp.path(), 20, None);

        assert!(deleted);
        assert!(!missing);
        assert!(!tmp.path().join("delete-me.json").exists());
        assert!(tmp.path().join("keep-me.json").exists());
        assert_eq!(page.groups.len(), 1);
        assert_eq!(page.groups[0].group_id, "keep-me");
        assert!(delete_dismissed_agent_group(tmp.path(), "bad/id").is_err());
    }

    #[test]
    fn recent_groups_are_capped_and_list_newest_first() {
        let tmp = TempDir::new().unwrap();
        for idx in 0..12 {
            record_recent_dismissed_agent_group(
                tmp.path(),
                sample_group(
                    &format!("recent-{idx:02}"),
                    &format!("2026-05-27T12:{idx:02}:00Z"),
                ),
                10,
            )
            .unwrap();
        }

        let page = list_recent_dismissed_agent_groups(tmp.path(), 10);

        assert_eq!(page.groups.len(), 10);
        assert_eq!(page.groups[0].group_id, "recent-11");
        assert_eq!(page.groups[9].group_id, "recent-02");
        assert_eq!(page.next_cursor, None);
        assert!(!tmp.path().join("recent-00.json").exists());
        assert!(!tmp.path().join("recent-01.json").exists());
    }

    #[test]
    fn recent_groups_replace_same_group_id() {
        let tmp = TempDir::new().unwrap();
        record_recent_dismissed_agent_group(
            tmp.path(),
            sample_group("recent-same", "2026-05-27T12:00:00Z"),
            10,
        )
        .unwrap();
        let updated = record_recent_dismissed_agent_group(
            tmp.path(),
            sample_group("recent-same", "2026-05-27T12:30:00Z"),
            10,
        )
        .unwrap();

        let page = list_recent_dismissed_agent_groups(tmp.path(), 10);
        let loaded =
            load_recent_dismissed_agent_group(tmp.path(), "recent-same")
                .unwrap()
                .unwrap();

        assert_eq!(updated.created_at, "2026-05-27T12:30:00Z");
        assert_eq!(page.groups.len(), 1);
        assert_eq!(loaded.created_at, "2026-05-27T12:30:00Z");
    }

    #[test]
    fn recent_groups_tolerate_corrupt_files_and_mark_revived() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("corrupt.json"), "not json").unwrap();
        record_recent_dismissed_agent_group(
            tmp.path(),
            sample_group("recent-valid", "2026-05-27T12:00:00Z"),
            10,
        )
        .unwrap();

        let page = list_recent_dismissed_agent_groups(tmp.path(), 10);
        let corrupt =
            load_recent_dismissed_agent_group(tmp.path(), "corrupt").unwrap();
        let revived = mark_recent_dismissed_agent_group_revived(
            tmp.path(),
            "recent-valid",
            "2026-05-27T13:00:00Z",
        )
        .unwrap()
        .unwrap();

        assert_eq!(page.groups.len(), 1);
        assert_eq!(page.groups[0].group_id, "recent-valid");
        assert!(corrupt.is_none());
        assert_eq!(revived.revived_at.as_deref(), Some("2026-05-27T13:00:00Z"));
        assert_eq!(revived.times_revived, 1);
    }

    #[test]
    fn save_list_and_load_preserve_group_name() {
        let tmp = TempDir::new().unwrap();
        let mut group = sample_group("named", "2026-05-27T12:00:00Z");
        group.name = Some("  Backend batch  ".to_string());

        let saved = save_dismissed_agent_group(tmp.path(), group).unwrap();
        let page = list_dismissed_agent_groups(tmp.path(), 20, None);
        let loaded = load_dismissed_agent_group(tmp.path(), "named")
            .unwrap()
            .unwrap();

        assert_eq!(saved.name.as_deref(), Some("Backend batch"));
        assert_eq!(page.groups[0].name.as_deref(), Some("Backend batch"));
        assert_eq!(loaded.name.as_deref(), Some("Backend batch"));
    }

    #[test]
    fn missing_group_name_loads_as_none() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("legacy.json"),
            serde_json::json!({
                "schema_version": AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION,
                "group_id": "legacy",
                "created_at": "2026-05-27T12:00:00Z",
                "source": "marked_agents",
                "title": "1 agent in cl",
                "agent_count": 1,
                "top_level_agent_count": 1,
                "status_counts": {"DONE": 1},
                "project_names": ["proj"],
                "cl_names": ["cl"],
                "agent_refs": [],
                "revived_at": null,
                "times_revived": 0,
            })
            .to_string(),
        )
        .unwrap();

        let loaded = load_dismissed_agent_group(tmp.path(), "legacy")
            .unwrap()
            .unwrap();
        let page = list_dismissed_agent_groups(tmp.path(), 20, None);

        assert_eq!(loaded.name, None);
        assert_eq!(page.groups[0].name, None);
    }

    #[test]
    fn missing_ref_prompt_preview_loads_as_none() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("legacy-prompt.json"),
            serde_json::json!({
                "schema_version": AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION,
                "group_id": "legacy-prompt",
                "created_at": "2026-05-27T12:00:00Z",
                "source": "marked_agents",
                "title": "1 agent in cl",
                "agent_count": 1,
                "top_level_agent_count": 1,
                "status_counts": {"DONE": 1},
                "project_names": ["proj"],
                "cl_names": ["cl"],
                "agent_refs": [{
                    "agent_type": "run",
                    "cl_name": "cl",
                    "raw_suffix": "ts-1",
                    "tag": "backend"
                }],
                "revived_at": null,
                "times_revived": 0,
            })
            .to_string(),
        )
        .unwrap();

        let loaded = load_dismissed_agent_group(tmp.path(), "legacy-prompt")
            .unwrap()
            .unwrap();

        assert_eq!(loaded.agent_refs.len(), 1);
        assert_eq!(loaded.agent_refs[0].prompt_preview, None);
    }

    #[test]
    fn whitespace_group_name_normalizes_to_none() {
        let tmp = TempDir::new().unwrap();
        let mut group = sample_group("blank-name", "2026-05-27T12:00:00Z");
        group.name = Some(" \n\t ".to_string());

        let saved = save_dismissed_agent_group(tmp.path(), group).unwrap();
        let loaded = load_dismissed_agent_group(tmp.path(), "blank-name")
            .unwrap()
            .unwrap();
        let page = list_dismissed_agent_groups(tmp.path(), 20, None);

        assert_eq!(saved.name, None);
        assert_eq!(loaded.name, None);
        assert_eq!(page.groups[0].name, None);
    }

    fn sample_group(group_id: &str, created_at: &str) -> SavedAgentGroupWire {
        let mut status_counts = BTreeMap::new();
        status_counts.insert("DONE".to_string(), 1);
        SavedAgentGroupWire {
            schema_version: AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION,
            group_id: group_id.to_string(),
            created_at: created_at.to_string(),
            source: "marked_agents".to_string(),
            title: "1 agent in cl".to_string(),
            name: None,
            agent_count: 1,
            top_level_agent_count: 1,
            status_counts,
            project_names: vec!["proj".to_string()],
            cl_names: vec!["cl".to_string()],
            agent_refs: vec![SavedAgentGroupRefWire {
                agent_type: "run".to_string(),
                cl_name: "cl".to_string(),
                raw_suffix: Some("ts-1".to_string()),
                bundle_path: None,
                is_workflow_child: false,
                parent_timestamp: None,
                display_name: Some("agent".to_string()),
                agent_name: Some("agent".to_string()),
                status: Some("DONE".to_string()),
                start_time: Some("2026-05-27T11:00:00Z".to_string()),
                model: Some("gpt".to_string()),
                llm_provider: Some("codex".to_string()),
                tag: Some("backend".to_string()),
                prompt_preview: Some(
                    "Restore this backend worker.".to_string(),
                ),
            }],
            revived_at: None,
            times_revived: 0,
        }
    }
}
