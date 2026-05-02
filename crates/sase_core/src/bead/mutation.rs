//! Bead store mutations backed by JSONL persistence.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::config::{default_config, load_config, save_config, BeadConfigWire};
use super::jsonl::{export_issues_to_jsonl, import_issues_from_jsonl};
use super::wire::{
    BeadError, BeadTierWire, DependencyWire, IssueTypeWire, IssueWire,
    StatusWire,
};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadCreateRequestWire {
    pub title: String,
    pub issue_type: IssueTypeWire,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub notes: String,
    #[serde(default)]
    pub design: String,
    #[serde(default)]
    pub assignee: String,
    #[serde(default)]
    pub changespec_name: String,
    #[serde(default)]
    pub changespec_bug_id: String,
    #[serde(default)]
    pub now: Option<String>,
    #[serde(default)]
    pub workspace_beads_dirs: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadUpdateFieldsWire {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub closed_at: Option<Option<String>>,
    #[serde(default)]
    pub close_reason: Option<Option<String>>,
    #[serde(default)]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub changespec_bug_id: Option<String>,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub is_ready_to_work: Option<bool>,
    #[serde(default)]
    pub now: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadMutationOutcomeWire {
    pub operation: String,
    pub changed: bool,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub issue: Option<IssueWire>,
    #[serde(default)]
    pub issues: Vec<IssueWire>,
    #[serde(default)]
    pub dependency: Option<DependencyWire>,
    #[serde(default)]
    pub next_counter: Option<u64>,
}

pub fn init_store(
    root_dir: &Path,
    beads_dirname: &str,
    issue_prefix: &str,
    owner: &str,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let beads_dir = root_dir.join(beads_dirname);
    fs::create_dir_all(&beads_dir)?;
    save_config(&beads_dir, &default_config(issue_prefix, owner))?;
    if !beads_dir.join("issues.jsonl").exists() {
        fs::write(beads_dir.join("issues.jsonl"), "")?;
    }
    if !beads_dir.join("beads.db").exists() {
        fs::write(beads_dir.join("beads.db"), "")?;
    }
    Ok(outcome("init", true, Vec::new()))
}

pub fn create_issue(
    beads_dir: &Path,
    request: BeadCreateRequestWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let mut store = MutableStore::load(beads_dir)?;
    let tier = default_create_tier(&request);
    let now = request.now.unwrap_or_else(now_utc);
    let owner = store.config.owner.clone();
    let issue_id = match request.parent_id.as_deref() {
        Some(parent_id) => next_child_id(
            parent_id,
            &store.issues,
            workspace_dirs(beads_dir, &request.workspace_beads_dirs),
        ),
        None => {
            let counter = next_top_level_counter(
                &store.config.issue_prefix,
                store.config.next_counter,
                workspace_dirs(beads_dir, &request.workspace_beads_dirs),
            );
            store.config.next_counter = counter + 1;
            format!("{}-{}", store.config.issue_prefix, to_base36(counter))
        }
    };

    let issue = IssueWire {
        id: issue_id,
        title: request.title,
        status: StatusWire::Open,
        issue_type: request.issue_type.clone(),
        tier,
        parent_id: request.parent_id,
        owner: owner.clone(),
        assignee: request.assignee,
        created_at: now.clone(),
        created_by: owner,
        updated_at: now,
        closed_at: None,
        close_reason: None,
        description: request.description,
        notes: request.notes,
        design: request.design,
        is_ready_to_work: false,
        changespec_name: request.changespec_name,
        changespec_bug_id: request.changespec_bug_id,
        dependencies: Vec::new(),
    };
    issue.validate()?;
    store.issues.push(issue.clone());
    store.save()?;

    let mut result = outcome("create", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    result.next_counter = Some(store.config.next_counter);
    Ok(result)
}

pub fn update_issue(
    beads_dir: &Path,
    issue_id: &str,
    fields: BeadUpdateFieldsWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if fields.is_ready_to_work.is_some() {
        return Err(BeadError::validation(
            "is_ready_to_work cannot be set via update(); use mark_ready_to_work() instead.",
        ));
    }
    let mut store = MutableStore::load(beads_dir)?;
    let index = store.issue_index(issue_id)?;
    let mut issue = store.issues[index].clone();
    apply_update_fields(&mut issue, fields)?;
    issue.validate()?;
    store.issues[index] = issue.clone();
    store.save()?;

    let mut result = outcome("update", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(result)
}

pub fn open_issue(
    beads_dir: &Path,
    issue_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    update_issue(
        beads_dir,
        issue_id,
        BeadUpdateFieldsWire {
            status: Some("open".to_string()),
            now,
            ..Default::default()
        },
    )
    .map(|mut outcome| {
        outcome.operation = "open".to_string();
        outcome
    })
}

pub fn close_issues(
    beads_dir: &Path,
    issue_ids: &[String],
    reason: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let mut store = MutableStore::load(beads_dir)?;
    let now = now.unwrap_or_else(now_utc);
    let mut closed = Vec::new();
    let mut closed_ids = Vec::new();

    for issue_id in issue_ids {
        let issue = store.get_issue(issue_id)?.clone();
        if issue.issue_type == IssueTypeWire::Plan {
            let child_ids: Vec<String> =
                sorted_children(&store.issues, issue_id)
                    .into_iter()
                    .filter(|child| child.status != StatusWire::Closed)
                    .map(|child| child.id.clone())
                    .collect();
            for child_id in child_ids {
                let child = store.close_one(&child_id, &now, reason.clone())?;
                closed_ids.push(child.id.clone());
                closed.push(child);
            }
        }
        let issue = store.close_one(issue_id, &now, reason.clone())?;
        closed_ids.push(issue.id.clone());
        closed.push(issue);
    }

    store.save()?;
    let mut result = outcome("close", true, closed_ids);
    result.issues = closed;
    Ok(result)
}

pub fn remove_issue(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let mut store = MutableStore::load(beads_dir)?;
    let issue = store.get_issue(issue_id)?.clone();
    let mut removed = Vec::new();
    if issue.issue_type == IssueTypeWire::Plan {
        removed.extend(
            sorted_children(&store.issues, issue_id)
                .into_iter()
                .cloned(),
        );
    }
    removed.push(issue);
    let removed_ids: BTreeSet<String> =
        removed.iter().map(|issue| issue.id.clone()).collect();
    store
        .issues
        .retain(|issue| !removed_ids.contains(&issue.id));
    for issue in &mut store.issues {
        issue.dependencies.retain(|dep| {
            !removed_ids.contains(&dep.issue_id)
                && !removed_ids.contains(&dep.depends_on_id)
        });
    }
    store.save()?;

    let mut result = outcome(
        "rm",
        true,
        removed.iter().map(|issue| issue.id.clone()).collect(),
    );
    result.issues = removed;
    Ok(result)
}

pub fn add_dependency(
    beads_dir: &Path,
    issue_id: &str,
    depends_on_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let mut store = MutableStore::load(beads_dir)?;
    store.get_issue(depends_on_id)?;
    let owner = store.config.owner.clone();
    let index = store.issue_index(issue_id)?;
    if store.issues[index]
        .dependencies
        .iter()
        .any(|dep| dep.depends_on_id == depends_on_id)
    {
        return Err(BeadError::validation(format!(
            "Dependency already exists: {issue_id} depends on {depends_on_id}"
        )));
    }
    let dep = DependencyWire {
        issue_id: issue_id.to_string(),
        depends_on_id: depends_on_id.to_string(),
        created_at: now.unwrap_or_else(now_utc),
        created_by: owner,
    };
    store.issues[index].dependencies.push(dep.clone());
    store.save()?;

    let mut result = outcome("dep_add", true, vec![issue_id.to_string()]);
    result.dependency = Some(dep);
    Ok(result)
}

pub fn mark_ready_to_work(
    beads_dir: &Path,
    epic_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    set_ready_to_work(beads_dir, epic_id, true, true, now).map(|mut outcome| {
        outcome.operation = "mark_ready_to_work".to_string();
        outcome
    })
}

pub fn unmark_ready_to_work(
    beads_dir: &Path,
    epic_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    set_ready_to_work(beads_dir, epic_id, false, false, now).map(
        |mut outcome| {
            outcome.operation = "unmark_ready_to_work".to_string();
            outcome
        },
    )
}

pub fn export_jsonl(
    beads_dir: &Path,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let store = MutableStore::load(beads_dir)?;
    store.save_issues()?;
    Ok(outcome(
        "export_jsonl",
        true,
        store.issues.iter().map(|issue| issue.id.clone()).collect(),
    ))
}

pub fn sync_is_clean(beads_dir: &Path) -> Result<bool, BeadError> {
    let jsonl_path = beads_dir.join("issues.jsonl");
    if !jsonl_path.exists() {
        return Ok(true);
    }
    let repo_root = find_git_root(beads_dir)?;
    let Some(repo_root) = repo_root else {
        return Ok(true);
    };
    let status = Command::new("git")
        .arg("diff")
        .arg("--quiet")
        .arg(&jsonl_path)
        .current_dir(repo_root)
        .status()?;
    Ok(status.success())
}

fn set_ready_to_work(
    beads_dir: &Path,
    epic_id: &str,
    ready: bool,
    reject_already_ready: bool,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let mut store = MutableStore::load(beads_dir)?;
    let index = store.issue_index(epic_id)?;
    if store.issues[index].issue_type != IssueTypeWire::Plan {
        return Err(BeadError {
            kind: "not_a_plan".to_string(),
            message: format!(
                "is_ready_to_work only applies to plan beads (got phase for {epic_id})"
            ),
        });
    }
    if store.issues[index].tier != Some(BeadTierWire::Epic) {
        return Err(BeadError {
            kind: "not_an_epic".to_string(),
            message: format!(
                "sase bead work only applies to epic plan beads (got {} for {epic_id})",
                tier_label(store.issues[index].tier.as_ref())
            ),
        });
    }
    if reject_already_ready && store.issues[index].is_ready_to_work {
        return Err(BeadError {
            kind: "already_ready".to_string(),
            message: format!(
                "{epic_id} is already marked is_ready_to_work=True"
            ),
        });
    }
    store.issues[index].is_ready_to_work = ready;
    store.issues[index].updated_at = now.unwrap_or_else(now_utc);
    let issue = store.issues[index].clone();
    store.save()?;

    let mut result = outcome("ready_to_work", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(result)
}

fn apply_update_fields(
    issue: &mut IssueWire,
    fields: BeadUpdateFieldsWire,
) -> Result<(), BeadError> {
    if let Some(value) = fields.title {
        issue.title = value;
    }
    if let Some(value) = fields.status {
        issue.status = parse_status(&value)?;
    }
    if let Some(value) = fields.assignee {
        issue.assignee = value;
    }
    if let Some(value) = fields.description {
        issue.description = value;
    }
    if let Some(value) = fields.notes {
        issue.notes = value;
    }
    if let Some(value) = fields.design {
        issue.design = value;
    }
    if let Some(value) = fields.closed_at {
        issue.closed_at = value;
    }
    if let Some(value) = fields.close_reason {
        issue.close_reason = value;
    }
    if let Some(value) = fields.changespec_name {
        issue.changespec_name = value;
    }
    if let Some(value) = fields.changespec_bug_id {
        issue.changespec_bug_id = value;
    }
    if let Some(value) = fields.tier {
        issue.tier = Some(value);
    }
    issue.updated_at = fields.now.unwrap_or_else(now_utc);
    Ok(())
}

fn default_create_tier(
    request: &BeadCreateRequestWire,
) -> Option<BeadTierWire> {
    match request.issue_type {
        IssueTypeWire::Plan => {
            Some(request.tier.clone().unwrap_or(BeadTierWire::Epic))
        }
        IssueTypeWire::Phase => request.tier.clone(),
    }
}

fn tier_label(tier: Option<&BeadTierWire>) -> &'static str {
    match tier {
        Some(BeadTierWire::Plan) => "plan",
        Some(BeadTierWire::Epic) => "epic",
        Some(BeadTierWire::Legend) => "legend",
        None => "missing tier",
    }
}

struct MutableStore {
    beads_dir: PathBuf,
    config: BeadConfigWire,
    issues: Vec<IssueWire>,
}

impl MutableStore {
    fn load(beads_dir: &Path) -> Result<Self, BeadError> {
        if !beads_dir.is_dir() {
            return Err(BeadError::io(format!(
                "No beads directory found at {}",
                beads_dir.display()
            )));
        }
        let fallback = default_config("beads", "");
        let config = load_config(beads_dir, fallback)?;
        let issues =
            import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))?.issues;
        Ok(Self {
            beads_dir: beads_dir.to_path_buf(),
            config,
            issues,
        })
    }

    fn save(&self) -> Result<(), BeadError> {
        save_config(&self.beads_dir, &self.config)?;
        self.save_issues()
    }

    fn save_issues(&self) -> Result<(), BeadError> {
        let jsonl = export_issues_to_jsonl(&self.issues)?;
        let path = self.beads_dir.join("issues.jsonl");
        let tmp_path = self.beads_dir.join("issues.jsonl.tmp");
        fs::write(&tmp_path, jsonl)?;
        fs::rename(tmp_path, path)?;
        Ok(())
    }

    fn issue_index(&self, issue_id: &str) -> Result<usize, BeadError> {
        self.issues
            .iter()
            .position(|issue| issue.id == issue_id)
            .ok_or_else(|| not_found(issue_id))
    }

    fn get_issue(&self, issue_id: &str) -> Result<&IssueWire, BeadError> {
        self.issues
            .iter()
            .find(|issue| issue.id == issue_id)
            .ok_or_else(|| not_found(issue_id))
    }

    fn close_one(
        &mut self,
        issue_id: &str,
        closed_at: &str,
        reason: Option<String>,
    ) -> Result<IssueWire, BeadError> {
        let index = self.issue_index(issue_id)?;
        self.issues[index].status = StatusWire::Closed;
        self.issues[index].closed_at = Some(closed_at.to_string());
        self.issues[index].close_reason = reason;
        self.issues[index].updated_at = closed_at.to_string();
        Ok(self.issues[index].clone())
    }
}

fn sorted_children<'a>(
    issues: &'a [IssueWire],
    parent_id: &str,
) -> Vec<&'a IssueWire> {
    let mut children: Vec<&IssueWire> = issues
        .iter()
        .filter(|issue| issue.parent_id.as_deref() == Some(parent_id))
        .collect();
    children.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    children
}

fn workspace_dirs<'a>(
    beads_dir: &'a Path,
    requested: &'a [PathBuf],
) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut dirs = Vec::new();
    for path in requested {
        let key = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        if seen.insert(key) {
            dirs.push(path.to_path_buf());
        }
    }
    let key = beads_dir
        .canonicalize()
        .unwrap_or_else(|_| beads_dir.to_path_buf());
    if seen.insert(key) {
        dirs.push(beads_dir.to_path_buf());
    }
    dirs
}

fn next_top_level_counter(
    issue_prefix: &str,
    config_counter: u64,
    beads_dirs: Vec<PathBuf>,
) -> u64 {
    std::cmp::max(
        config_counter,
        max_top_level_counter(issue_prefix, &beads_dirs) + 1,
    )
}

fn next_child_id(
    parent_id: &str,
    issues: &[IssueWire],
    beads_dirs: Vec<PathBuf>,
) -> String {
    let local_max = issues
        .iter()
        .filter_map(|issue| direct_child_counter(parent_id, &issue.id))
        .max()
        .unwrap_or(0);
    let workspace_max = max_child_counter(parent_id, &beads_dirs);
    format!(
        "{parent_id}.{}",
        std::cmp::max(local_max, workspace_max) + 1
    )
}

fn max_top_level_counter(issue_prefix: &str, beads_dirs: &[PathBuf]) -> u64 {
    let expected_prefix = format!("{issue_prefix}-");
    iter_jsonl_issue_ids(beads_dirs)
        .filter_map(|issue_id| {
            issue_id.strip_prefix(&expected_prefix).map(str::to_string)
        })
        .filter(|suffix| !suffix.contains('.'))
        .filter_map(|suffix| from_base36(&suffix))
        .max()
        .unwrap_or(0)
}

fn max_child_counter(parent_id: &str, beads_dirs: &[PathBuf]) -> u64 {
    iter_jsonl_issue_ids(beads_dirs)
        .filter_map(|issue_id| direct_child_counter(parent_id, &issue_id))
        .max()
        .unwrap_or(0)
}

fn direct_child_counter(parent_id: &str, issue_id: &str) -> Option<u64> {
    let prefix = format!("{parent_id}.");
    let suffix = issue_id.strip_prefix(&prefix)?;
    if suffix.contains('.') {
        return None;
    }
    suffix.parse::<u64>().ok()
}

fn iter_jsonl_issue_ids(
    beads_dirs: &[PathBuf],
) -> impl Iterator<Item = String> {
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    for beads_dir in beads_dirs {
        let path = beads_dir.join("issues.jsonl");
        let key = path.canonicalize().unwrap_or_else(|_| path.clone());
        if !seen.insert(key) || !path.exists() {
            continue;
        }
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };
        for line in contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if let Some(issue_id) = value.get("id").and_then(Value::as_str) {
                ids.push(issue_id.to_string());
            }
        }
    }
    ids.into_iter()
}

fn parse_status(value: &str) -> Result<StatusWire, BeadError> {
    match value {
        "open" => Ok(StatusWire::Open),
        "in_progress" => Ok(StatusWire::InProgress),
        "closed" => Ok(StatusWire::Closed),
        _ => Err(BeadError::validation(format!(
            "invalid bead status: {value}"
        ))),
    }
}

fn to_base36(mut n: u64) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".to_string();
    }
    let mut digits = Vec::new();
    while n > 0 {
        digits.push(ALPHABET[(n % 36) as usize] as char);
        n /= 36;
    }
    digits.iter().rev().collect()
}

fn from_base36(value: &str) -> Option<u64> {
    u64::from_str_radix(value, 36).ok()
}

fn now_utc() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn find_git_root(path: &Path) -> Result<Option<PathBuf>, BeadError> {
    let cwd = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if root.is_empty() {
        Ok(None)
    } else {
        Ok(Some(PathBuf::from(root)))
    }
}

fn not_found(issue_id: &str) -> BeadError {
    BeadError {
        kind: "not_found".to_string(),
        message: format!("Issue not found: {issue_id}"),
    }
}

fn outcome(
    operation: &str,
    changed: bool,
    issue_ids: Vec<String>,
) -> BeadMutationOutcomeWire {
    BeadMutationOutcomeWire {
        operation: operation.to_string(),
        changed,
        issue_ids,
        message: String::new(),
        issue: None,
        issues: Vec::new(),
        dependency: None,
        next_counter: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_top_level_uses_workspace_max_and_persists_counter() {
        let temp = tempdir().unwrap();
        let a = temp.path().join("sase/sdd/beads");
        let b = temp.path().join("sase_101/sdd/beads");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        save_config(
            &a,
            &BeadConfigWire {
                issue_prefix: "sase".to_string(),
                next_counter: 1,
                owner: String::new(),
            },
        )
        .unwrap();
        save_config(
            &b,
            &BeadConfigWire {
                issue_prefix: "sase".to_string(),
                next_counter: 1,
                owner: String::new(),
            },
        )
        .unwrap();
        fs::write(a.join("issues.jsonl"), "").unwrap();
        fs::write(
            b.join("issues.jsonl"),
            r#"{"id":"sase-z","title":"Other","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        )
        .unwrap();

        let result = create_issue(
            &a,
            BeadCreateRequestWire {
                title: "Next".to_string(),
                issue_type: IssueTypeWire::Plan,
                workspace_beads_dirs: vec![b],
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.issue.unwrap().id, "sase-10");
        assert_eq!(
            load_config(&a, default_config("x", ""))
                .unwrap()
                .next_counter,
            37
        );
    }

    #[test]
    fn close_plan_cascades_open_children_before_parent() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Plan",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "A",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.2",
                    "B",
                    "phase",
                    Some("sase-1"),
                    "closed",
                    "2026-01-01T00:02:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            Some("done".to_string()),
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1", "sase-1"]);
        let exported =
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(exported
            .contains(r#""id":"sase-1.1","title":"A","status":"closed""#));
        assert!(exported.contains(r#""close_reason":"done""#));
    }

    #[test]
    fn mark_ready_rejects_phase_and_idempotent_plan() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Plan",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "A",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        assert_eq!(
            mark_ready_to_work(&beads_dir, "sase-1.1", None)
                .unwrap_err()
                .kind,
            "not_a_plan"
        );
        mark_ready_to_work(&beads_dir, "sase-1", None).unwrap();
        assert_eq!(
            mark_ready_to_work(&beads_dir, "sase-1", None)
                .unwrap_err()
                .kind,
            "already_ready"
        );
    }

    fn issue(
        id: &str,
        title: &str,
        issue_type: &str,
        parent_id: Option<&str>,
        status: &str,
        timestamp: &str,
    ) -> String {
        let parent = parent_id.map_or_else(
            || "null".to_string(),
            |value| format!(r#""{value}""#),
        );
        format!(
            r#"{{"id":"{id}","title":"{title}","status":"{status}","issue_type":"{issue_type}","parent_id":{parent},"owner":"","assignee":"","created_at":"{timestamp}","created_by":"","updated_at":"{timestamp}","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[]}}"#
        )
    }
}
