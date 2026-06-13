//! Shared on-disk layout rules for agent artifact directories.
//!
//! The public workflow name stays `ace-run`; day sharding is a storage
//! detail below that workflow directory:
//!
//! `projects/<project>/artifacts/ace-run/YYYYMM/DD/<timestamp>/`

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const ACE_RUN_WORKFLOW_DIR: &str = "ace-run";
pub const LEGACY_LAYOUT_VERSION: u32 = 1;
pub const DAY_SHARDED_LAYOUT_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactPathInfo {
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    pub artifact_dir: String,
    pub layout_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowArtifactCandidate {
    pub artifact_dir: PathBuf,
    pub timestamp: String,
    pub layout_version: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkflowArtifactCandidates {
    pub candidates: Vec<WorkflowArtifactCandidate>,
    pub os_errors: u64,
}

pub fn is_day_sharded_workflow(workflow_dir_name: &str) -> bool {
    workflow_dir_name == ACE_RUN_WORKFLOW_DIR
}

pub fn is_artifact_timestamp(value: &str) -> bool {
    value.len() == 14 && value.bytes().all(|b| b.is_ascii_digit())
}

pub fn canonical_agent_artifact_path(
    projects_root: &Path,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> PathBuf {
    let workflow_dir = projects_root
        .join(project_name)
        .join("artifacts")
        .join(workflow_dir_name);
    if is_day_sharded_workflow(workflow_dir_name)
        && is_artifact_timestamp(timestamp)
    {
        return workflow_dir
            .join(&timestamp[..6])
            .join(&timestamp[6..8])
            .join(timestamp);
    }
    workflow_dir.join(timestamp)
}

pub fn legacy_agent_artifact_path(
    projects_root: &Path,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> PathBuf {
    projects_root
        .join(project_name)
        .join("artifacts")
        .join(workflow_dir_name)
        .join(timestamp)
}

pub fn resolve_agent_artifact_timestamp_path(
    projects_root: &Path,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> PathBuf {
    let canonical = canonical_agent_artifact_path(
        projects_root,
        project_name,
        workflow_dir_name,
        timestamp,
    );
    if canonical.is_dir() {
        return canonical;
    }
    let legacy = legacy_agent_artifact_path(
        projects_root,
        project_name,
        workflow_dir_name,
        timestamp,
    );
    if legacy.is_dir() {
        return legacy;
    }
    canonical
}

pub fn parse_agent_artifact_path(
    projects_root: &Path,
    artifact_dir: &Path,
) -> Option<AgentArtifactPathInfo> {
    let rel = artifact_dir.strip_prefix(projects_root).ok()?;
    let parts: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts.len() == 4 && parts[1] == "artifacts" {
        return Some(AgentArtifactPathInfo {
            project_name: parts[0].clone(),
            workflow_dir_name: parts[2].clone(),
            timestamp: parts[3].clone(),
            artifact_dir: artifact_dir.to_string_lossy().into_owned(),
            layout_version: LEGACY_LAYOUT_VERSION,
        });
    }
    if parts.len() == 6
        && parts[1] == "artifacts"
        && is_day_sharded_workflow(&parts[2])
        && is_valid_month_shard(&parts[3])
        && is_valid_day_shard(&parts[4])
        && is_artifact_timestamp(&parts[5])
        && parts[5].starts_with(&format!("{}{}", parts[3], parts[4]))
    {
        return Some(AgentArtifactPathInfo {
            project_name: parts[0].clone(),
            workflow_dir_name: parts[2].clone(),
            timestamp: parts[5].clone(),
            artifact_dir: artifact_dir.to_string_lossy().into_owned(),
            layout_version: DAY_SHARDED_LAYOUT_VERSION,
        });
    }
    None
}

pub fn resolve_agent_artifact_path(
    projects_root: &Path,
    artifact_dir: &Path,
) -> PathBuf {
    let Some(info) = parse_agent_artifact_path(projects_root, artifact_dir)
    else {
        return artifact_dir.to_path_buf();
    };
    if info.layout_version == LEGACY_LAYOUT_VERSION {
        return resolve_agent_artifact_timestamp_path(
            projects_root,
            &info.project_name,
            &info.workflow_dir_name,
            &info.timestamp,
        );
    }
    if artifact_dir.is_dir() {
        return artifact_dir.to_path_buf();
    }
    let legacy = legacy_agent_artifact_path(
        projects_root,
        &info.project_name,
        &info.workflow_dir_name,
        &info.timestamp,
    );
    if legacy.is_dir() {
        return legacy;
    }
    artifact_dir.to_path_buf()
}

pub fn collect_workflow_artifact_candidates(
    workflow_dir: &Path,
    workflow_dir_name: &str,
    newest_first: bool,
) -> WorkflowArtifactCandidates {
    if !is_day_sharded_workflow(workflow_dir_name) {
        return collect_legacy_workflow_candidates(workflow_dir, newest_first);
    }

    let mut result = WorkflowArtifactCandidates::default();
    let mut by_timestamp: BTreeMap<String, WorkflowArtifactCandidate> =
        BTreeMap::new();
    for entry in
        sorted_dir_entries(workflow_dir, newest_first, &mut result.os_errors)
    {
        if !is_dir(&entry, &mut result.os_errors) {
            continue;
        }
        let name = file_name_string(&entry);
        if name.starts_with('.') {
            continue;
        }
        if is_valid_month_shard(&name) {
            collect_day_shard_candidates(
                &entry,
                &name,
                newest_first,
                &mut result.os_errors,
                &mut by_timestamp,
            );
            continue;
        }
        by_timestamp
            .entry(name.clone())
            .or_insert(WorkflowArtifactCandidate {
                artifact_dir: entry,
                timestamp: name,
                layout_version: LEGACY_LAYOUT_VERSION,
            });
    }
    result.candidates = candidates_from_map(by_timestamp, newest_first);
    result
}

fn collect_legacy_workflow_candidates(
    workflow_dir: &Path,
    newest_first: bool,
) -> WorkflowArtifactCandidates {
    let mut result = WorkflowArtifactCandidates::default();
    for entry in
        sorted_dir_entries(workflow_dir, newest_first, &mut result.os_errors)
    {
        if !is_dir(&entry, &mut result.os_errors) {
            continue;
        }
        let timestamp = file_name_string(&entry);
        result.candidates.push(WorkflowArtifactCandidate {
            artifact_dir: entry,
            timestamp,
            layout_version: LEGACY_LAYOUT_VERSION,
        });
    }
    result
}

fn collect_day_shard_candidates(
    month_dir: &Path,
    month_name: &str,
    newest_first: bool,
    os_errors: &mut u64,
    by_timestamp: &mut BTreeMap<String, WorkflowArtifactCandidate>,
) {
    for day_dir in sorted_dir_entries(month_dir, newest_first, os_errors) {
        if !is_dir(&day_dir, os_errors) {
            continue;
        }
        let day_name = file_name_string(&day_dir);
        if !is_valid_day_shard(&day_name) {
            continue;
        }
        for artifact_dir in
            sorted_dir_entries(&day_dir, newest_first, os_errors)
        {
            if !is_dir(&artifact_dir, os_errors) {
                continue;
            }
            let timestamp = file_name_string(&artifact_dir);
            if !is_artifact_timestamp(&timestamp)
                || !timestamp.starts_with(&format!("{month_name}{day_name}"))
            {
                continue;
            }
            by_timestamp.insert(
                timestamp.clone(),
                WorkflowArtifactCandidate {
                    artifact_dir,
                    timestamp,
                    layout_version: DAY_SHARDED_LAYOUT_VERSION,
                },
            );
        }
    }
}

fn candidates_from_map(
    by_timestamp: BTreeMap<String, WorkflowArtifactCandidate>,
    newest_first: bool,
) -> Vec<WorkflowArtifactCandidate> {
    let mut candidates: Vec<WorkflowArtifactCandidate> =
        by_timestamp.into_values().collect();
    if newest_first {
        candidates.reverse();
    }
    candidates
}

fn sorted_dir_entries(
    path: &Path,
    newest_first: bool,
    os_errors: &mut u64,
) -> Vec<PathBuf> {
    let read_dir = match fs::read_dir(path) {
        Ok(rd) => rd,
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                *os_errors += 1;
            }
            return Vec::new();
        }
    };
    let mut entries = Vec::new();
    for entry in read_dir {
        match entry {
            Ok(e) => entries.push(e.path()),
            Err(_) => *os_errors += 1,
        }
    }
    entries.sort_by(|a, b| {
        a.file_name()
            .unwrap_or_default()
            .cmp(b.file_name().unwrap_or_default())
    });
    if newest_first {
        entries.reverse();
    }
    entries
}

fn is_dir(path: &Path, os_errors: &mut u64) -> bool {
    match fs::metadata(path) {
        Ok(metadata) => metadata.is_dir(),
        Err(_) => {
            *os_errors += 1;
            false
        }
    }
}

fn file_name_string(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn is_valid_month_shard(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|b| b.is_ascii_digit())
}

fn is_valid_day_shard(value: &str) -> bool {
    if value.len() != 2 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    matches!(value.parse::<u8>(), Ok(day) if (1..=31).contains(&day))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn canonical_ace_run_path_is_day_sharded() {
        let root = Path::new("/tmp/projects");
        assert_eq!(
            canonical_agent_artifact_path(
                root,
                "proj",
                "ace-run",
                "20260613120000"
            ),
            Path::new(
                "/tmp/projects/proj/artifacts/ace-run/202606/13/20260613120000"
            )
        );
    }

    #[test]
    fn non_ace_run_path_stays_flat() {
        let root = Path::new("/tmp/projects");
        assert_eq!(
            canonical_agent_artifact_path(
                root,
                "proj",
                "crs",
                "20260613120000"
            ),
            Path::new("/tmp/projects/proj/artifacts/crs/20260613120000")
        );
    }

    #[test]
    fn parse_accepts_legacy_and_sharded_paths() {
        let root = Path::new("/tmp/projects");
        let legacy = parse_agent_artifact_path(
            root,
            Path::new("/tmp/projects/proj/artifacts/ace-run/20260613120000"),
        )
        .unwrap();
        assert_eq!(legacy.layout_version, LEGACY_LAYOUT_VERSION);
        assert_eq!(legacy.timestamp, "20260613120000");

        let sharded = parse_agent_artifact_path(
            root,
            Path::new(
                "/tmp/projects/proj/artifacts/ace-run/202606/13/20260613120000",
            ),
        )
        .unwrap();
        assert_eq!(sharded.layout_version, DAY_SHARDED_LAYOUT_VERSION);
        assert_eq!(sharded.workflow_dir_name, "ace-run");
    }

    #[test]
    fn parse_rejects_malformed_shard_paths() {
        let root = Path::new("/tmp/projects");
        assert!(parse_agent_artifact_path(
            root,
            Path::new(
                "/tmp/projects/proj/artifacts/ace-run/202606/14/20260613120000",
            ),
        )
        .is_none());
        assert!(parse_agent_artifact_path(
            root,
            Path::new(
                "/tmp/projects/proj/artifacts/ace-run/202606/99/20260699120000",
            ),
        )
        .is_none());
    }

    #[test]
    fn collect_prefers_sharded_duplicate_timestamp() {
        let tmp = tempdir().unwrap();
        let workflow = tmp.path().join("ace-run");
        let legacy = workflow.join("20260613120000");
        let sharded = workflow.join("202606").join("13").join("20260613120000");
        fs::create_dir_all(&legacy).unwrap();
        fs::create_dir_all(&sharded).unwrap();

        let result =
            collect_workflow_artifact_candidates(&workflow, "ace-run", false);
        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.candidates[0].artifact_dir, sharded);
        assert_eq!(
            result.candidates[0].layout_version,
            DAY_SHARDED_LAYOUT_VERSION
        );
    }

    #[test]
    fn resolve_legacy_path_to_shard_when_present() {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let sharded = canonical_agent_artifact_path(
            &projects,
            "proj",
            "ace-run",
            "20260613120000",
        );
        fs::create_dir_all(&sharded).unwrap();
        let legacy = legacy_agent_artifact_path(
            &projects,
            "proj",
            "ace-run",
            "20260613120000",
        );
        assert_eq!(resolve_agent_artifact_path(&projects, &legacy), sharded);
    }
}
