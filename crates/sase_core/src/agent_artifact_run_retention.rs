//! Shared ACE-run artifact-directory retention decisions.
//!
//! Protection facts (bead liveness, continuation ancestry, artifact-index
//! references, scan markers) are gathered by callers from their own
//! Rust-backed sources and passed in per candidate; this module owns the
//! final classification, the canonical-root/symlink-ancestor safety check,
//! fresh revalidation immediately before each deletion, and the bottom-up
//! empty-shard walk. It never trusts a stale snapshot: every mutation is
//! preceded by its own fresh filesystem read.

use std::collections::BTreeSet;
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION: u32 = 2;

/// Runs scale with disk usage and retention backlog, not with any bounded
/// graph, so this is deliberately generous — it only exists to fail closed
/// instead of hanging on a pathological request.
const MAX_RUN_RETENTION_CANDIDATES: usize = 200_000;

#[derive(Debug, Error)]
pub enum AgentArtifactRunRetentionError {
    #[error(
        "agent artifact run retention requires schema_version {expected}, got {actual}"
    )]
    UnsupportedSchema { actual: u32, expected: u32 },
    #[error("projects root {0} could not be resolved")]
    InvalidProjectsRoot(String),
    #[error("projects root {path} is unsafe: {reason}")]
    UnsafeProjectsRoot { path: String, reason: String },
    #[error(
        "agent artifact run retention received {actual} candidates, exceeding the \
         {max} bound"
    )]
    TooManyCandidates { actual: usize, max: usize },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunCandidateWire {
    pub artifact_dir: String,
    pub project: String,
    pub timestamp: String,
    #[serde(default)]
    pub protected_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunRetentionRequestWire {
    pub schema_version: u32,
    pub projects_root: String,
    /// Trailing `YYYYMM` months to protect, precomputed by the caller so
    /// this module never has to reason about calendars or timezones.
    #[serde(default)]
    pub recent_months: Vec<String>,
    /// `YYYYMMDDHHMMSS` "now" marker; any candidate timestamp greater than
    /// this is a future run and stays protected.
    #[serde(default)]
    pub current_timestamp: String,
    pub limit: Option<u64>,
    #[serde(default)]
    pub apply: bool,
    #[serde(default)]
    pub sources_unavailable: Vec<String>,
    #[serde(default)]
    pub protected_dirs: Vec<String>,
    #[serde(default)]
    pub protected_timestamps: Vec<String>,
    #[serde(default)]
    pub candidates: Vec<AgentArtifactRunCandidateWire>,
    #[serde(default)]
    pub empty_shard_roots: Vec<String>,
    #[serde(default)]
    pub empty_shard_watched_paths: Vec<String>,
    #[serde(default)]
    pub empty_shard_removal_budget: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentArtifactRunItemResultWire {
    pub artifact_dir: String,
    pub project: String,
    pub timestamp: String,
    /// One of: `protected`, `deferred`, `selected`, `removed`, `skipped`, `error`.
    pub outcome: String,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub bytes_reclaimed: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentArtifactEmptyShardResultWire {
    pub path: String,
    /// One of: `would_remove`, `removed`, `skipped`, `error`.
    pub outcome: String,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentArtifactRunRetentionResultWire {
    pub schema_version: u32,
    pub apply: bool,
    #[serde(default)]
    pub blocked_reason: Option<String>,
    pub candidates: u64,
    pub selected: u64,
    pub protected: u64,
    pub truncated: u64,
    pub removed_runs: u64,
    pub bytes_reclaimed: u64,
    pub removed_empty_shards: u64,
    #[serde(default)]
    pub run_items: Vec<AgentArtifactRunItemResultWire>,
    #[serde(default)]
    pub shard_items: Vec<AgentArtifactEmptyShardResultWire>,
}

pub fn apply_agent_artifact_run_retention(
    request: &AgentArtifactRunRetentionRequestWire,
) -> Result<AgentArtifactRunRetentionResultWire, AgentArtifactRunRetentionError>
{
    if request.schema_version
        != AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION
    {
        return Err(AgentArtifactRunRetentionError::UnsupportedSchema {
            actual: request.schema_version,
            expected: AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION,
        });
    }
    if request.candidates.len() > MAX_RUN_RETENTION_CANDIDATES {
        return Err(AgentArtifactRunRetentionError::TooManyCandidates {
            actual: request.candidates.len(),
            max: MAX_RUN_RETENTION_CANDIDATES,
        });
    }
    let projects_root_input = Path::new(&request.projects_root);
    if let Some(reason) = projects_root_safety_violation(projects_root_input) {
        return Err(AgentArtifactRunRetentionError::UnsafeProjectsRoot {
            path: request.projects_root.clone(),
            reason: reason.to_string(),
        });
    }
    let projects_root =
        canonicalize_existing(projects_root_input).ok_or_else(|| {
            AgentArtifactRunRetentionError::InvalidProjectsRoot(
                request.projects_root.clone(),
            )
        })?;

    let mut result = AgentArtifactRunRetentionResultWire {
        schema_version: AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION,
        apply: request.apply,
        candidates: request.candidates.len() as u64,
        ..Default::default()
    };

    // Preview (`apply: false`) still classifies best-effort so callers can
    // show what *would* happen; only a real mutation pass refuses outright,
    // per the "apply must refuse missing protection sources" contract.
    if request.apply && !request.sources_unavailable.is_empty() {
        result.blocked_reason = Some(format!(
            "protection sources unavailable: {}",
            request.sources_unavailable.join(", ")
        ));
        return Ok(result);
    }

    let recent_months: BTreeSet<&str> =
        request.recent_months.iter().map(String::as_str).collect();
    let protected_dirs = canonical_protected_dirs(&request.protected_dirs);
    let protected_timestamps: BTreeSet<&str> = request
        .protected_timestamps
        .iter()
        .map(String::as_str)
        .collect();

    let mut reasons_by_index: Vec<Vec<String>> =
        Vec::with_capacity(request.candidates.len());
    for candidate in &request.candidates {
        let mut reasons = candidate.protected_reasons.clone();
        if protected_timestamps.contains(candidate.timestamp.as_str()) {
            reasons.push("referenced_timestamp".to_string());
        }
        if candidate.timestamp.len() >= 6
            && recent_months.contains(&candidate.timestamp[..6])
        {
            reasons.push("recent_month".to_string());
        }
        if !request.current_timestamp.is_empty()
            && candidate.timestamp.as_str() > request.current_timestamp.as_str()
        {
            reasons.push("future_timestamp".to_string());
        }
        if let Some(canonical_dir) =
            canonicalize_existing(Path::new(&candidate.artifact_dir))
        {
            if protected_dirs.contains(&canonical_dir) {
                reasons.push("referenced_dir".to_string());
            }
        }
        if let Some(violation) =
            run_candidate_safety_violation(&projects_root, candidate)
        {
            reasons.push(violation.to_string());
        }
        reasons_by_index.push(dedupe(reasons));
    }

    let mut eligible: Vec<usize> = (0..request.candidates.len())
        .filter(|&index| reasons_by_index[index].is_empty())
        .collect();
    eligible.sort_by(|&a, &b| {
        let ca = &request.candidates[a];
        let cb = &request.candidates[b];
        (&ca.timestamp, &ca.project, &ca.artifact_dir).cmp(&(
            &cb.timestamp,
            &cb.project,
            &cb.artifact_dir,
        ))
    });

    let limit = request.limit.map(|n| n as usize).unwrap_or(eligible.len());
    let selected_count = eligible.len().min(limit);
    let selected_set: BTreeSet<usize> =
        eligible[..selected_count].iter().copied().collect();

    result.protected =
        reasons_by_index.iter().filter(|r| !r.is_empty()).count() as u64;
    result.selected = selected_set.len() as u64;
    result.truncated = (eligible.len() - selected_set.len()) as u64;

    for (index, candidate) in request.candidates.iter().enumerate() {
        let reasons = &reasons_by_index[index];
        if !reasons.is_empty() {
            result.run_items.push(AgentArtifactRunItemResultWire {
                artifact_dir: candidate.artifact_dir.clone(),
                project: candidate.project.clone(),
                timestamp: candidate.timestamp.clone(),
                outcome: "protected".to_string(),
                reasons: reasons.clone(),
                ..Default::default()
            });
            continue;
        }
        if !selected_set.contains(&index) {
            result.run_items.push(AgentArtifactRunItemResultWire {
                artifact_dir: candidate.artifact_dir.clone(),
                project: candidate.project.clone(),
                timestamp: candidate.timestamp.clone(),
                outcome: "deferred".to_string(),
                ..Default::default()
            });
            continue;
        }
        if !request.apply {
            result.run_items.push(AgentArtifactRunItemResultWire {
                artifact_dir: candidate.artifact_dir.clone(),
                project: candidate.project.clone(),
                timestamp: candidate.timestamp.clone(),
                outcome: "selected".to_string(),
                ..Default::default()
            });
            continue;
        }

        let path = PathBuf::from(&candidate.artifact_dir);
        if let Some(violation) =
            run_candidate_safety_violation(&projects_root, candidate)
        {
            result.run_items.push(item_skip(candidate, violation));
            continue;
        }
        if let Some(reason) = fresh_active_marker_reason(&path) {
            result.run_items.push(item_skip(candidate, &reason));
            continue;
        }
        match remove_run_dir(&path) {
            Ok(size_bytes) => {
                result.removed_runs += 1;
                result.bytes_reclaimed += size_bytes;
                result.run_items.push(AgentArtifactRunItemResultWire {
                    artifact_dir: candidate.artifact_dir.clone(),
                    project: candidate.project.clone(),
                    timestamp: candidate.timestamp.clone(),
                    outcome: "removed".to_string(),
                    bytes_reclaimed: size_bytes,
                    ..Default::default()
                });
            }
            Err(message) => {
                result.run_items.push(AgentArtifactRunItemResultWire {
                    artifact_dir: candidate.artifact_dir.clone(),
                    project: candidate.project.clone(),
                    timestamp: candidate.timestamp.clone(),
                    outcome: "error".to_string(),
                    detail: Some(message),
                    ..Default::default()
                });
            }
        }
    }

    let watched: BTreeSet<PathBuf> = request
        .empty_shard_watched_paths
        .iter()
        .filter_map(|path| canonicalize_existing(Path::new(path)))
        .collect();
    let mut protected_run_dirs = protected_dirs;
    for item in &result.run_items {
        if item.outcome != "protected" && item.outcome != "skipped" {
            continue;
        }
        if let Some(canonical_dir) =
            canonicalize_existing(Path::new(&item.artifact_dir))
        {
            if canonical_dir.starts_with(&projects_root) {
                protected_run_dirs.insert(canonical_dir);
            }
        }
    }
    let mut shard_budget = request.empty_shard_removal_budget;
    for root in &request.empty_shard_roots {
        let root_path = Path::new(root);
        let Some(canonical_root) = canonicalize_existing(root_path) else {
            continue;
        };
        if existing_path_safety_violation(&projects_root, root_path).is_some() {
            continue;
        }
        let children = match fs::read_dir(root_path) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect::<Vec<_>>(),
            Err(error) => {
                result.shard_items.push(shard_outcome(
                    root_path,
                    "error",
                    Some(error.to_string()),
                ));
                continue;
            }
        };
        for child in children {
            prune_empty_tree(
                &canonical_root,
                &child,
                &watched,
                &protected_run_dirs,
                request.apply,
                &mut shard_budget,
                &mut result.shard_items,
            );
        }
    }
    result.removed_empty_shards = result
        .shard_items
        .iter()
        .filter(|item| item.outcome == "removed")
        .count() as u64;

    Ok(result)
}

/// Walk *dir* bottom-up, removing (or reporting) it only when every
/// descendant is itself empty and not a watched shard. Returns whether *dir*
/// is (or would be, in preview) empty and eligible for removal. Actual
/// removal happens child-first: by the time a directory's own `remove_dir`
/// runs, every subdirectory beneath it has already been removed, so the
/// call fails closed (an `OSError`, surfaced as an `error` outcome) if
/// anything reappeared between the read and the removal.
fn prune_empty_tree(
    workflow_root: &Path,
    dir: &Path,
    watched: &BTreeSet<PathBuf>,
    protected: &BTreeSet<PathBuf>,
    apply: bool,
    budget: &mut u32,
    out: &mut Vec<AgentArtifactEmptyShardResultWire>,
) -> bool {
    let Ok(metadata) = fs::symlink_metadata(dir) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return false;
    }
    let Some(canonical) = canonicalize_existing(dir) else {
        return false;
    };
    if watched.contains(&canonical) || protected.contains(&canonical) {
        return false;
    }
    if !is_eligible_empty_shard_path(workflow_root, &canonical) {
        return false;
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            out.push(shard_outcome(dir, "error", Some(error.to_string())));
            return false;
        }
    };
    let mut all_removable = true;
    for entry in entries {
        let Ok(entry) = entry else {
            all_removable = false;
            continue;
        };
        let Ok(file_type) = entry.file_type() else {
            all_removable = false;
            continue;
        };
        if file_type.is_dir() {
            if !prune_empty_tree(
                workflow_root,
                &entry.path(),
                watched,
                protected,
                apply,
                budget,
                out,
            ) {
                all_removable = false;
            }
        } else {
            // Files and symlinks both block emptiness; symlinks are never
            // traversed or removed by this walk.
            all_removable = false;
        }
    }
    if !all_removable {
        return false;
    }
    if *budget == 0 {
        out.push(shard_outcome(
            dir,
            "skipped",
            Some("removal budget exhausted".to_string()),
        ));
        return false;
    }
    if !apply {
        *budget -= 1;
        out.push(shard_outcome(dir, "would_remove", None));
        return true;
    }
    match fs::remove_dir(dir) {
        Ok(()) => {
            *budget -= 1;
            out.push(shard_outcome(dir, "removed", None));
            true
        }
        Err(error) => {
            out.push(shard_outcome(dir, "error", Some(error.to_string())));
            false
        }
    }
}

fn shard_outcome(
    dir: &Path,
    outcome: &str,
    detail: Option<String>,
) -> AgentArtifactEmptyShardResultWire {
    AgentArtifactEmptyShardResultWire {
        path: dir.to_string_lossy().into_owned(),
        outcome: outcome.to_string(),
        detail,
    }
}

fn item_skip(
    candidate: &AgentArtifactRunCandidateWire,
    reason: &str,
) -> AgentArtifactRunItemResultWire {
    AgentArtifactRunItemResultWire {
        artifact_dir: candidate.artifact_dir.clone(),
        project: candidate.project.clone(),
        timestamp: candidate.timestamp.clone(),
        outcome: "skipped".to_string(),
        detail: Some(reason.to_string()),
        ..Default::default()
    }
}

/// Return why *candidate* cannot safely be mutated, if any: it must exist,
/// canonicalize under *projects_root*, have no symlink in the submitted path
/// components, and resolve to the exact project/workflow/run location named
/// by its wire identity.
fn run_candidate_safety_violation(
    projects_root: &Path,
    candidate: &AgentArtifactRunCandidateWire,
) -> Option<&'static str> {
    let candidate_path = Path::new(&candidate.artifact_dir);
    let violation =
        existing_path_safety_violation(projects_root, candidate_path);
    if violation.is_some() {
        return violation;
    }
    let canonical = canonicalize_existing(candidate_path)?;
    if !valid_project_component(&candidate.project)
        || !valid_run_timestamp(&candidate.timestamp)
    {
        return Some("invalid_run_path");
    }
    let expected = projects_root
        .join(&candidate.project)
        .join("artifacts")
        .join("ace-run")
        .join(&candidate.timestamp[..6])
        .join(&candidate.timestamp[6..8])
        .join(&candidate.timestamp);
    if canonical != expected {
        return Some("invalid_run_path");
    }
    None
}

fn existing_path_safety_violation(
    projects_root: &Path,
    path: &Path,
) -> Option<&'static str> {
    let Some(canonical) = canonicalize_existing(path) else {
        return Some("missing");
    };
    if !canonical.starts_with(projects_root) {
        return Some("outside_projects_root");
    }
    symlink_component_violation(projects_root, path)
}

fn projects_root_safety_violation(root: &Path) -> Option<&'static str> {
    let Ok(metadata) = fs::symlink_metadata(root) else {
        return None;
    };
    if metadata.file_type().is_symlink() {
        return Some("symlink_root");
    }
    None
}

fn symlink_component_violation(
    projects_root: &Path,
    path: &Path,
) -> Option<&'static str> {
    let lexical_root = absolutize_lexically(projects_root)?;
    let lexical_path = absolutize_lexically(path)?;
    let relative = lexical_path.strip_prefix(&lexical_root).ok()?;
    let mut current = lexical_root;
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Some("invalid_path_component");
        };
        current.push(part);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(_) => return Some("missing"),
        };
        if metadata.file_type().is_symlink() {
            return Some(if current == lexical_path {
                "symlink"
            } else {
                "symlink_ancestor"
            });
        }
    }
    None
}

fn absolutize_lexically(path: &Path) -> Option<PathBuf> {
    let mut out = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().ok()?
    };
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
        }
    }
    Some(out)
}

fn canonical_protected_dirs(raw_dirs: &[String]) -> BTreeSet<PathBuf> {
    raw_dirs
        .iter()
        .filter_map(|path| canonicalize_existing(Path::new(path)))
        .collect()
}

fn valid_project_component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
}

fn valid_run_timestamp(value: &str) -> bool {
    value.len() == 14 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_eligible_empty_shard_path(workflow_root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(workflow_root) else {
        return false;
    };
    let parts: Vec<String> = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => {
                Some(part.to_string_lossy().into_owned())
            }
            _ => None,
        })
        .collect();
    match parts.as_slice() {
        [month] => is_month_shard_name(month),
        [month, day] => is_month_shard_name(month) && is_day_shard_name(day),
        [month, day, timestamp] => {
            valid_run_timestamp(timestamp)
                && timestamp.starts_with(month)
                && &timestamp[6..8] == day.as_str()
        }
        _ => false,
    }
}

fn is_month_shard_name(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_day_shard_name(value: &str) -> bool {
    value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn fresh_active_marker_reason(path: &Path) -> Option<String> {
    if !path.join("done.json").exists() {
        return Some("not_terminal".to_string());
    }
    for marker in ["running.json", "waiting.json", "pending_question.json"] {
        if path.join(marker).exists() {
            return Some(format!("active_marker:{marker}"));
        }
    }
    None
}

fn remove_run_dir(path: &Path) -> Result<u64, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("{}: symlink skipped", path.display()));
    }
    if !metadata.is_dir() {
        return Err(format!("{}: not a directory", path.display()));
    }
    let size_bytes = tree_size(path);
    fs::remove_dir_all(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(size_bytes)
}

fn tree_size(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.file_type().is_symlink() {
        return 0;
    }
    let mut total = metadata.len();
    if metadata.is_dir() {
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.filter_map(Result::ok) {
                total += tree_size(&entry.path());
            }
        }
    }
    total
}

fn canonicalize_existing(path: &Path) -> Option<PathBuf> {
    fs::canonicalize(path).ok()
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn candidate(
        dir: &Path,
        project: &str,
        timestamp: &str,
    ) -> AgentArtifactRunCandidateWire {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("done.json"), "{}").unwrap();
        AgentArtifactRunCandidateWire {
            artifact_dir: dir.to_string_lossy().into_owned(),
            project: project.to_string(),
            timestamp: timestamp.to_string(),
            protected_reasons: Vec::new(),
        }
    }

    fn base_request(root: &Path) -> AgentArtifactRunRetentionRequestWire {
        AgentArtifactRunRetentionRequestWire {
            schema_version: AGENT_ARTIFACT_RUN_RETENTION_WIRE_SCHEMA_VERSION,
            projects_root: root.to_string_lossy().into_owned(),
            recent_months: vec!["202609".to_string()],
            current_timestamp: "20260914120000".to_string(),
            limit: None,
            apply: false,
            sources_unavailable: Vec::new(),
            protected_dirs: Vec::new(),
            protected_timestamps: Vec::new(),
            candidates: Vec::new(),
            empty_shard_roots: Vec::new(),
            empty_shard_watched_paths: Vec::new(),
            empty_shard_removal_budget: 0,
        }
    }

    #[test]
    fn apply_refuses_when_sources_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let mut request = base_request(temp.path());
        request.apply = true;
        request.sources_unavailable = vec!["beads: unreachable".to_string()];
        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert!(result.blocked_reason.is_some());
        assert_eq!(result.selected, 0);
        assert_eq!(result.removed_runs, 0);
    }

    #[test]
    fn preview_still_classifies_when_sources_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let project_dir = temp.path().join("proj");
        let mut request = base_request(temp.path());
        request.sources_unavailable = vec!["beads: unreachable".to_string()];
        request.candidates = vec![candidate(
            &project_dir.join("artifacts/ace-run/202601/01/20260101000000"),
            "proj",
            "20260101000000",
        )];
        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert!(result.blocked_reason.is_none());
        assert_eq!(result.selected, 1);
    }

    #[test]
    fn protects_recent_month_and_future_timestamp_and_selects_the_rest() {
        let temp = tempfile::tempdir().unwrap();
        let project_dir = temp.path().join("proj");
        let mut request = base_request(temp.path());
        request.candidates = vec![
            candidate(
                &project_dir.join("artifacts/ace-run/202608/01/20260801000000"),
                "proj",
                "20260801000000",
            ),
            candidate(
                &project_dir.join("artifacts/ace-run/202609/01/20260901000000"),
                "proj",
                "20260901000000",
            ),
            candidate(
                &project_dir.join("artifacts/ace-run/202612/01/20261201000000"),
                "proj",
                "20261201000000",
            ),
        ];
        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert_eq!(result.selected, 1);
        assert_eq!(result.protected, 2);
        let protected_reasons: Vec<&str> = result
            .run_items
            .iter()
            .find(|item| item.timestamp == "20260901000000")
            .unwrap()
            .reasons
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(protected_reasons, vec!["recent_month"]);
        let future_reasons: Vec<&str> = result
            .run_items
            .iter()
            .find(|item| item.timestamp == "20261201000000")
            .unwrap()
            .reasons
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(future_reasons, vec!["future_timestamp"]);
    }

    #[test]
    fn apply_removes_selected_and_revalidates_active_marker() {
        let temp = tempfile::tempdir().unwrap();
        let project_dir = temp.path().join("proj");
        let stale =
            project_dir.join("artifacts/ace-run/202608/01/20260801000000");
        let reactivated =
            project_dir.join("artifacts/ace-run/202608/02/20260802000000");
        let mut request = base_request(temp.path());
        request.apply = true;
        request.candidates = vec![
            candidate(&stale, "proj", "20260801000000"),
            candidate(&reactivated, "proj", "20260802000000"),
        ];
        // Simulate the run becoming active again after the plan snapshot
        // was taken but before apply runs.
        fs::write(reactivated.join("running.json"), "{}").unwrap();

        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert_eq!(result.removed_runs, 1);
        assert!(!stale.exists());
        assert!(reactivated.exists());
        let skipped = result
            .run_items
            .iter()
            .find(|item| item.timestamp == "20260802000000")
            .unwrap();
        assert_eq!(skipped.outcome, "skipped");
        assert_eq!(
            skipped.detail.as_deref(),
            Some("active_marker:running.json")
        );
    }

    #[test]
    fn rejects_paths_outside_projects_root() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_dir = outside.path().join("20260801000000");
        let mut request = base_request(temp.path());
        request.apply = true;
        request.candidates =
            vec![candidate(&outside_dir, "proj", "20260801000000")];

        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert_eq!(result.removed_runs, 0);
        assert!(outside_dir.exists());
        let item = &result.run_items[0];
        assert_eq!(item.outcome, "protected");
        assert_eq!(item.reasons, vec!["outside_projects_root".to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_ancestor_before_canonicalizing_candidate() {
        let temp = tempfile::tempdir().unwrap();
        let projects_root = temp.path().join("projects");
        let real_run = projects_root
            .join("demo/artifacts/ace-run/202601/01/20260101000000");
        candidate(&real_run, "demo", "20260101000000");
        std::os::unix::fs::symlink(
            projects_root.join("demo"),
            projects_root.join("alias"),
        )
        .unwrap();
        let alias_run = projects_root
            .join("alias/artifacts/ace-run/202601/01/20260101000000");
        let mut request = base_request(&projects_root);
        request.apply = true;
        request.candidates = vec![AgentArtifactRunCandidateWire {
            artifact_dir: alias_run.to_string_lossy().into_owned(),
            project: "alias".to_string(),
            timestamp: "20260101000000".to_string(),
            protected_reasons: Vec::new(),
        }];

        let result = apply_agent_artifact_run_retention(&request).unwrap();

        assert_eq!(result.removed_runs, 0);
        assert!(real_run.exists());
        let item = &result.run_items[0];
        assert_eq!(item.outcome, "protected");
        assert_eq!(item.reasons, vec!["symlink_ancestor".to_string()]);
    }

    #[test]
    fn authoritative_timestamp_protection_overrides_empty_candidate_reasons() {
        let temp = tempfile::tempdir().unwrap();
        let project_dir = temp.path().join("proj");
        let referenced =
            project_dir.join("artifacts/ace-run/202601/01/20260101000000");
        let mut request = base_request(temp.path());
        request.apply = true;
        request.protected_timestamps = vec!["20260101000000".to_string()];
        request.candidates =
            vec![candidate(&referenced, "proj", "20260101000000")];

        let result = apply_agent_artifact_run_retention(&request).unwrap();

        assert_eq!(result.removed_runs, 0);
        assert!(referenced.exists());
        let item = &result.run_items[0];
        assert_eq!(item.outcome, "protected");
        assert_eq!(item.reasons, vec!["referenced_timestamp".to_string()]);
    }

    #[test]
    fn empty_shard_walk_removes_bottom_up_across_three_levels() {
        let temp = tempfile::tempdir().unwrap();
        let workflow_dir = temp.path().join("artifacts/ace-run");
        let leaf = workflow_dir.join("202704/01/20270401000000");
        fs::create_dir_all(&leaf).unwrap();

        let mut request = base_request(temp.path());
        request.apply = true;
        request.empty_shard_roots =
            vec![workflow_dir.to_string_lossy().into_owned()];
        request.empty_shard_removal_budget = 10;

        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert_eq!(result.removed_empty_shards, 3, "{:?}", result.shard_items);
        assert!(result
            .shard_items
            .iter()
            .all(|item| item.outcome == "removed"));
        assert!(!workflow_dir.join("202704").exists());
        assert!(workflow_dir.exists());
    }

    #[test]
    fn empty_shard_walk_preserves_watched_and_nonempty_dirs() {
        let temp = tempfile::tempdir().unwrap();
        let workflow_dir = temp.path().join("artifacts/ace-run");
        let watched_month = workflow_dir.join("202609");
        fs::create_dir_all(&watched_month).unwrap();
        let live_leaf = workflow_dir.join("202608/02/20260802000000");
        fs::create_dir_all(&live_leaf).unwrap();
        fs::write(live_leaf.join("agent_meta.json"), "{}").unwrap();
        let empty_month = workflow_dir.join("202607");
        fs::create_dir_all(&empty_month).unwrap();

        let mut request = base_request(temp.path());
        request.apply = true;
        request.empty_shard_roots =
            vec![workflow_dir.to_string_lossy().into_owned()];
        request.empty_shard_watched_paths =
            vec![watched_month.to_string_lossy().into_owned()];
        request.empty_shard_removal_budget = 10;

        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert_eq!(result.removed_empty_shards, 1);
        assert!(watched_month.exists());
        assert!(live_leaf.exists());
        assert!(!empty_month.exists());
    }

    #[test]
    fn empty_shard_walk_preserves_protected_empty_run_and_parents() {
        let temp = tempfile::tempdir().unwrap();
        let workflow_dir = temp.path().join("proj/artifacts/ace-run");
        let referenced = workflow_dir.join("202601/01/20260101000000");
        fs::create_dir_all(&referenced).unwrap();
        let mut request = base_request(temp.path());
        request.apply = true;
        request.protected_dirs =
            vec![referenced.to_string_lossy().into_owned()];
        request.candidates = vec![AgentArtifactRunCandidateWire {
            artifact_dir: referenced.to_string_lossy().into_owned(),
            project: "proj".to_string(),
            timestamp: "20260101000000".to_string(),
            protected_reasons: Vec::new(),
        }];
        request.empty_shard_roots =
            vec![workflow_dir.to_string_lossy().into_owned()];
        request.empty_shard_removal_budget = 10;

        let result = apply_agent_artifact_run_retention(&request).unwrap();

        assert_eq!(result.removed_runs, 0);
        assert_eq!(result.removed_empty_shards, 0);
        assert!(referenced.exists());
        assert!(referenced.parent().unwrap().exists());
        assert!(referenced.parent().unwrap().parent().unwrap().exists());
    }

    #[test]
    fn empty_shard_walk_respects_removal_budget() {
        let temp = tempfile::tempdir().unwrap();
        let workflow_dir = temp.path().join("artifacts/ace-run");
        fs::create_dir_all(workflow_dir.join("202601")).unwrap();
        fs::create_dir_all(workflow_dir.join("202602")).unwrap();
        fs::create_dir_all(workflow_dir.join("202603")).unwrap();

        let mut request = base_request(temp.path());
        request.apply = true;
        request.empty_shard_roots =
            vec![workflow_dir.to_string_lossy().into_owned()];
        request.empty_shard_removal_budget = 2;

        let result = apply_agent_artifact_run_retention(&request).unwrap();
        assert_eq!(result.removed_empty_shards, 2);
        assert!(result
            .shard_items
            .iter()
            .any(|item| item.outcome == "skipped"));
    }

    #[test]
    fn empty_shard_preview_respects_apply_budget() {
        let temp = tempfile::tempdir().unwrap();
        let workflow_dir = temp.path().join("artifacts/ace-run");
        fs::create_dir_all(workflow_dir.join("202601")).unwrap();
        fs::create_dir_all(workflow_dir.join("202602")).unwrap();
        fs::create_dir_all(workflow_dir.join("202603")).unwrap();

        let mut request = base_request(temp.path());
        request.empty_shard_roots =
            vec![workflow_dir.to_string_lossy().into_owned()];
        request.empty_shard_removal_budget = 2;

        let result = apply_agent_artifact_run_retention(&request).unwrap();

        assert_eq!(
            result
                .shard_items
                .iter()
                .filter(|item| item.outcome == "would_remove")
                .count(),
            2
        );
        assert!(result
            .shard_items
            .iter()
            .any(|item| item.outcome == "skipped"));
    }

    #[test]
    fn empty_shard_walk_preserves_unknown_directories() {
        let temp = tempfile::tempdir().unwrap();
        let workflow_dir = temp.path().join("artifacts/ace-run");
        let unknown = workflow_dir.join("manual");
        fs::create_dir_all(&unknown).unwrap();

        let mut request = base_request(temp.path());
        request.apply = true;
        request.empty_shard_roots =
            vec![workflow_dir.to_string_lossy().into_owned()];
        request.empty_shard_removal_budget = 10;

        let result = apply_agent_artifact_run_retention(&request).unwrap();

        assert_eq!(result.removed_empty_shards, 0);
        assert!(result.shard_items.is_empty());
        assert!(unknown.exists());
    }
}
