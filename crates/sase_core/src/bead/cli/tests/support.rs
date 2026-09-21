//! Shared fixtures for bead CLI tests.

use super::super::*;
use crate::bead::wire::{BeadTierWire, IssueTypeWire, IssueWire, StatusWire};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

pub(super) struct SeededStore {
    pub(super) _temp: TempDir,
    pub(super) beads_dir: PathBuf,
}

pub(super) fn execute_search(
    beads_dir: &Path,
    args: &[&str],
) -> BeadCliOutcomeWire {
    let argv = string_args(args);
    execute_bead_cli(
        &argv,
        &[beads_dir.to_path_buf()],
        beads_dir,
        Path::new("/repo"),
        false,
        &[],
    )
    .unwrap()
}

pub(super) fn string_args(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| arg.to_string()).collect()
}

pub(super) fn seed_issues(issues: Vec<IssueWire>) -> SeededStore {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    seed_issues_at(temp, beads_dir, issues)
}

pub(super) fn seed_issues_at(
    temp: TempDir,
    beads_dir: PathBuf,
    issues: Vec<IssueWire>,
) -> SeededStore {
    fs::create_dir_all(&beads_dir).unwrap();
    let jsonl = issues
        .iter()
        .map(|issue| serde_json::to_string(issue).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(
        beads_dir.join("issues.jsonl"),
        if jsonl.is_empty() {
            String::new()
        } else {
            format!("{jsonl}\n")
        },
    )
    .unwrap();
    SeededStore {
        _temp: temp,
        beads_dir,
    }
}

pub(super) fn show_plan_section(
    design: &str,
    plan_roots: &[PathBuf],
    cwd: &Path,
    relativize_design_paths: bool,
) -> String {
    let mut issue = plan_issue(
        "beads-1",
        "Plan",
        "",
        StatusWire::Open,
        "2026-01-01T00:00:00Z",
    );
    issue.design = design.to_string();
    let store = seed_issues(vec![issue]);
    let outcome = execute_bead_cli(
        &["show".to_string(), "beads-1".to_string()],
        std::slice::from_ref(&store.beads_dir),
        &store.beads_dir,
        cwd,
        relativize_design_paths,
        plan_roots,
    )
    .unwrap();
    assert_eq!(outcome.exit_code, 0);
    let (_, plan) = outcome.stdout.split_once("\nPLAN\n").unwrap();
    plan.to_string()
}

pub(super) fn seed_plan_root(
    temp: &TempDir,
    month: &str,
    name: &str,
) -> PathBuf {
    let month_dir = temp.path().join(month);
    fs::create_dir_all(&month_dir).unwrap();
    fs::write(month_dir.join(name), "# Plan\n").unwrap();
    temp.path().to_path_buf()
}

pub(super) fn phase_issue(
    id: &str,
    title: &str,
    description: &str,
    status: StatusWire,
    created_at: &str,
) -> IssueWire {
    IssueWire {
        id: id.to_string(),
        title: title.to_string(),
        status,
        issue_type: IssueTypeWire::Phase,
        tier: None,
        parent_id: Some("beads-1".to_string()),
        owner: String::new(),
        assignee: String::new(),
        created_at: created_at.to_string(),
        created_by: String::new(),
        updated_at: created_at.to_string(),
        closed_at: None,
        close_reason: None,
        resolution: None,
        close_history: Vec::new(),
        description: description.to_string(),
        notes: Vec::new(),
        design: String::new(),
        refs: Vec::new(),
        links: Vec::new(),
        plus_one_evidence: Vec::new(),
        snooze: None,
        model: String::new(),
        size: None,
        task_type: None,
        task_type_fields: BTreeMap::new(),
        is_ready_to_work: false,
        changespec_name: String::new(),
        changespec_bug_id: String::new(),
        external_ref: String::new(),
        dependencies: Vec::new(),
    }
}

pub(super) fn plan_issue(
    id: &str,
    title: &str,
    description: &str,
    status: StatusWire,
    created_at: &str,
) -> IssueWire {
    let mut issue = phase_issue(id, title, description, status, created_at);
    issue.issue_type = IssueTypeWire::Plan;
    issue.tier = Some(BeadTierWire::Epic);
    issue.parent_id = None;
    issue
}

pub(super) fn task_issue(
    id: &str,
    title: &str,
    description: &str,
    status: StatusWire,
    created_at: &str,
) -> IssueWire {
    let mut issue = phase_issue(id, title, description, status, created_at);
    issue.issue_type = IssueTypeWire::Task;
    issue.parent_id = None;
    issue
}
