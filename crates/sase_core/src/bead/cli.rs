//! Narrow `sase bead` CLI execution planner.
//!
//! Python still owns workspace discovery, help text, and host-coupled
//! commands. This module handles the common bead commands once Python has
//! resolved the store paths.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::mutation::{
    add_dependency, close_issues, open_issue, update_issue,
    BeadMutationOutcomeWire, BeadUpdateFieldsWire,
};
use super::read::{merge_workspace_issues, read_store_issues};
use super::wire::{
    BeadError, DependencyWire, IssueTypeWire, IssueWire, StatusWire,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCliOutcomeWire {
    pub handled: bool,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub mutation_summary: Option<BeadCliMutationSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadCliMutationSummaryWire {
    pub operation: String,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default)]
    pub status_transitions: Vec<BeadCliStatusTransitionWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCliStatusTransitionWire {
    pub from_status: String,
    pub to_status: String,
}

pub fn execute_bead_cli(
    argv: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if argv.is_empty() || argv.iter().any(|arg| arg == "-h" || arg == "--help")
    {
        return Ok(defer());
    }

    match argv[0].as_str() {
        "list" => handle_list(&argv[1..], read_beads_dirs, write_beads_dir),
        "show" => handle_show(
            &argv[1..],
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
        ),
        "ready" => handle_ready(&argv[1..], read_beads_dirs, write_beads_dir),
        "blocked" => {
            handle_blocked(&argv[1..], read_beads_dirs, write_beads_dir)
        }
        "stats" => handle_stats(&argv[1..], read_beads_dirs, write_beads_dir),
        "open" => handle_open(&argv[1..], write_beads_dir),
        "update" => handle_update(&argv[1..], write_beads_dir),
        "close" => handle_close(&argv[1..], write_beads_dir),
        "dep" => handle_dep(&argv[1..], write_beads_dir),
        _ => Ok(defer()),
    }
}

fn handle_list(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let Some(filters) = parse_list_filters(args) else {
        return Ok(defer());
    };
    let mut issues = read_issues(read_beads_dirs, write_beads_dir)?;
    issues.retain(|issue| {
        filters.statuses.contains(&issue.status)
            && filters
                .issue_types
                .as_ref()
                .map_or(true, |types| types.contains(&issue.issue_type))
    });
    sort_by_created_at(&mut issues);

    let mut stdout = String::new();
    if issues.is_empty() {
        stdout.push_str("No issues found.\n");
    } else {
        for issue in &issues {
            let parent = issue
                .parent_id
                .as_ref()
                .map_or(String::new(), |parent_id| format!(" ← {parent_id}"));
            writeln!(
                stdout,
                "{} {} · {}{}",
                status_icon(&issue.status),
                issue.id,
                issue.title,
                parent
            )
            .expect("writing to String cannot fail");
        }
    }
    Ok(success(stdout))
}

fn handle_show(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.len() != 1 {
        return Ok(defer());
    }
    let issues = read_issues(read_beads_dirs, write_beads_dir)?;
    let Some(issue) = find_issue(&issues, &args[0]) else {
        return Ok(error(format!("Error: issue not found: {}\n", args[0])));
    };

    let mut stdout = String::new();
    writeln!(
        stdout,
        "{} {} · {}   [{}]",
        status_icon(&issue.status),
        issue.id,
        issue.title,
        status_upper(&issue.status)
    )
    .expect("writing to String cannot fail");
    writeln!(
        stdout,
        "Type: {} · Owner: {}",
        issue_type_value(&issue.issue_type),
        if issue.owner.is_empty() {
            "(none)"
        } else {
            issue.owner.as_str()
        }
    )
    .expect("writing to String cannot fail");
    if !issue.assignee.is_empty() {
        writeln!(stdout, "Assignee: {}", issue.assignee)
            .expect("writing to String cannot fail");
    }
    if let Some(parent_id) = &issue.parent_id {
        if let Some(parent) = find_issue(&issues, parent_id) {
            write!(
                stdout,
                "\nPARENT\n  ↑ {} · {}   [{}]\n",
                parent.id,
                parent.title,
                status_upper(&parent.status)
            )
            .expect("writing to String cannot fail");
        } else {
            write!(stdout, "\nPARENT\n  ↑ {parent_id}\n")
                .expect("writing to String cannot fail");
        }
    }
    if issue.issue_type == IssueTypeWire::Plan {
        let mut children: Vec<&IssueWire> = issues
            .iter()
            .filter(|candidate| {
                candidate.parent_id.as_deref() == Some(issue.id.as_str())
            })
            .collect();
        children.sort_by(|a, b| {
            a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
        });
        if !children.is_empty() {
            stdout.push_str("\nCHILDREN\n");
            for child in children {
                writeln!(
                    stdout,
                    "  {} {}: {}",
                    status_icon(&child.status),
                    child.id,
                    child.title
                )
                .expect("writing to String cannot fail");
            }
        }
    }
    if !issue.dependencies.is_empty() {
        stdout.push_str("\nDEPENDS ON\n");
        for dep in &issue.dependencies {
            render_dependency(&mut stdout, &issues, dep, "→");
        }
    }

    let blocks = blocking_issue_ids(&issues, &issue.id);
    if !blocks.is_empty() {
        stdout.push_str("\nBLOCKS\n");
        for blocker_id in blocks {
            if let Some(blocker) = find_issue(&issues, &blocker_id) {
                writeln!(
                    stdout,
                    "  ← {} {}: {}   [{}]",
                    status_icon(&blocker.status),
                    blocker.id,
                    blocker.title,
                    status_upper(&blocker.status)
                )
                .expect("writing to String cannot fail");
            } else {
                writeln!(stdout, "  ← {blocker_id} (not found)")
                    .expect("writing to String cannot fail");
            }
        }
    }
    if !issue.description.is_empty() {
        write!(stdout, "\nDESCRIPTION\n  {}\n", issue.description)
            .expect("writing to String cannot fail");
    }
    if !issue.notes.is_empty() {
        write!(stdout, "\nNOTES\n  {}\n", issue.notes)
            .expect("writing to String cannot fail");
    }
    if issue.issue_type == IssueTypeWire::Plan
        && (!issue.changespec_name.is_empty()
            || !issue.changespec_bug_id.is_empty())
    {
        stdout.push_str("\nCHANGESPEC\n");
        if !issue.changespec_name.is_empty() {
            writeln!(stdout, "  Name: {}", issue.changespec_name)
                .expect("writing to String cannot fail");
        }
        if !issue.changespec_bug_id.is_empty() {
            writeln!(stdout, "  Bug ID: {}", issue.changespec_bug_id)
                .expect("writing to String cannot fail");
        }
    }
    if !issue.design.is_empty() {
        let display =
            display_design_path(&issue.design, cwd, relativize_design_paths);
        write!(stdout, "\nPLAN\n  {display}\n")
            .expect("writing to String cannot fail");
    }

    Ok(success(stdout))
}

fn handle_ready(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if !args.is_empty() {
        return Ok(defer());
    }
    let mut issues = read_issues(read_beads_dirs, write_beads_dir)?;
    let status_by_id: BTreeMap<String, StatusWire> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue.status.clone()))
        .collect();
    issues.retain(|issue| {
        issue.status == StatusWire::Open
            && !has_active_blocker(issue, &status_by_id)
    });
    sort_by_created_at(&mut issues);

    let mut stdout = String::new();
    if issues.is_empty() {
        stdout.push_str("No issues ready (all blocked or none open).\n");
    } else {
        for issue in &issues {
            let parent = issue
                .parent_id
                .as_ref()
                .map_or(String::new(), |parent_id| format!(" ← {parent_id}"));
            writeln!(stdout, "○ {} · {}{}", issue.id, issue.title, parent)
                .expect("writing to String cannot fail");
        }
        write!(
            stdout,
            "\n{}\nReady: {} issues with no active blockers\n",
            "-".repeat(60),
            issues.len()
        )
        .expect("writing to String cannot fail");
    }
    Ok(success(stdout))
}

fn handle_blocked(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if !args.is_empty() {
        return Ok(defer());
    }
    let mut issues = read_issues(read_beads_dirs, write_beads_dir)?;
    let status_by_id: BTreeMap<String, StatusWire> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue.status.clone()))
        .collect();
    issues.retain(|issue| has_active_blocker(issue, &status_by_id));
    sort_by_created_at(&mut issues);

    let mut stdout = String::new();
    if issues.is_empty() {
        stdout.push_str("No blocked issues.\n");
    } else {
        for issue in &issues {
            let blockers = issue
                .dependencies
                .iter()
                .map(|dep| dep.depends_on_id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(
                stdout,
                "● {} · {}  [blocked by: {blockers}]",
                issue.id, issue.title
            )
            .expect("writing to String cannot fail");
        }
    }
    Ok(success(stdout))
}

fn handle_stats(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if !args.is_empty() {
        return Ok(defer());
    }
    let issues = read_issues(read_beads_dirs, write_beads_dir)?;
    let stats = stats_for_issues(&issues);
    let stdout = format!(
        "Issue Statistics\n  Total:       {}\n  Open:        {}\n  In Progress: {}\n  Closed:      {}\n  Plans:       {}\n  Phases:      {}\n",
        stats.get("total").copied().unwrap_or(0),
        stats.get("open").copied().unwrap_or(0),
        stats.get("in_progress").copied().unwrap_or(0),
        stats.get("closed").copied().unwrap_or(0),
        stats.get("plan").copied().unwrap_or(0),
        stats.get("phase").copied().unwrap_or(0),
    );
    Ok(success(stdout))
}

fn handle_open(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.len() != 1 {
        return Ok(defer());
    }
    let old = read_store_issues(write_beads_dir)
        .ok()
        .and_then(|issues| find_issue(&issues, &args[0]).cloned());
    match open_issue(write_beads_dir, &args[0], None) {
        Ok(outcome) => {
            let issue = outcome.issue.as_ref().expect("open outcome has issue");
            Ok(success_with_mutation(
                format!("○ Opened: {} — {}\n", issue.id, issue.title),
                mutation_summary("update", &outcome, old.as_ref()),
            ))
        }
        Err(err) if err.kind == "not_found" => {
            Ok(error(format!("Error: issue not found: {}\n", args[0])))
        }
        Err(err) => Err(err),
    }
}

fn handle_update(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.is_empty() {
        return Ok(defer());
    }
    let issue_id = &args[0];
    let Some(fields) = parse_update_fields(&args[1..]) else {
        return Ok(defer());
    };
    if fields == BeadUpdateFieldsWire::default() {
        return Ok(error("No fields to update.\n".to_string()));
    }

    let old = read_store_issues(write_beads_dir)
        .ok()
        .and_then(|issues| find_issue(&issues, issue_id).cloned());
    match update_issue(write_beads_dir, issue_id, fields) {
        Ok(outcome) => {
            let issue =
                outcome.issue.as_ref().expect("update outcome has issue");
            Ok(success_with_mutation(
                format!("✓ Updated issue: {} — {}\n", issue.id, issue.title),
                mutation_summary("update", &outcome, old.as_ref()),
            ))
        }
        Err(err) if err.kind == "not_found" => {
            Ok(error(format!("Error: issue not found: {issue_id}\n")))
        }
        Err(err) => Err(err),
    }
}

fn handle_close(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let Some((ids, reason)) = parse_close_args(args) else {
        return Ok(defer());
    };
    if ids.is_empty() {
        return Ok(defer());
    }
    match close_issues(write_beads_dir, &ids, reason, None) {
        Ok(outcome) => {
            let mut stdout = String::new();
            for issue in &outcome.issues {
                writeln!(stdout, "✓ Closed: {} — {}", issue.id, issue.title)
                    .expect("writing to String cannot fail");
            }
            Ok(success_with_mutation(
                stdout,
                BeadCliMutationSummaryWire {
                    operation: "close".to_string(),
                    issue_ids: outcome.issue_ids.clone(),
                    status_transitions: outcome
                        .issues
                        .iter()
                        .map(|_| BeadCliStatusTransitionWire {
                            from_status: "open".to_string(),
                            to_status: "closed".to_string(),
                        })
                        .collect(),
                },
            ))
        }
        Err(err) if err.kind == "not_found" => {
            Ok(error(format!("Error: '{}'\n", err.message)))
        }
        Err(err) => Err(err),
    }
}

fn handle_dep(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.len() != 3 || args[0] != "add" {
        return Ok(defer());
    }
    let issue_id = &args[1];
    let depends_on_id = &args[2];
    match add_dependency(write_beads_dir, issue_id, depends_on_id, None) {
        Ok(outcome) => {
            let dep = outcome
                .dependency
                .as_ref()
                .expect("dep add outcome has dependency");
            Ok(success_with_mutation(
                format!(
                    "✓ Added dependency: {} depends on {}\n",
                    dep.issue_id, dep.depends_on_id
                ),
                BeadCliMutationSummaryWire {
                    operation: "dep_add".to_string(),
                    issue_ids: outcome.issue_ids.clone(),
                    status_transitions: Vec::new(),
                },
            ))
        }
        Err(err) => Err(err),
    }
}

#[derive(Debug)]
struct ListFilters {
    statuses: Vec<StatusWire>,
    issue_types: Option<Vec<IssueTypeWire>>,
}

fn parse_list_filters(args: &[String]) -> Option<ListFilters> {
    let mut statuses = Vec::new();
    let mut issue_types = Vec::new();
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-s" || arg == "--status" {
            idx += 1;
            let value = args.get(idx)?;
            statuses.push(parse_status(value)?);
        } else if let Some(value) = arg.strip_prefix("--status=") {
            statuses.push(parse_status(value)?);
        } else if arg == "-t" || arg == "--type" {
            idx += 1;
            let value = args.get(idx)?;
            issue_types.push(parse_issue_type(value)?);
        } else if let Some(value) = arg.strip_prefix("--type=") {
            issue_types.push(parse_issue_type(value)?);
        } else {
            return None;
        }
        idx += 1;
    }
    if statuses.is_empty() {
        statuses.push(StatusWire::Open);
        statuses.push(StatusWire::InProgress);
    }
    Some(ListFilters {
        statuses,
        issue_types: (!issue_types.is_empty()).then_some(issue_types),
    })
}

fn parse_update_fields(args: &[String]) -> Option<BeadUpdateFieldsWire> {
    let mut fields = BeadUpdateFieldsWire::default();
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        let (name, value) = if matches!(
            arg.as_str(),
            "-s" | "--status"
                | "-t"
                | "--title"
                | "-d"
                | "--description"
                | "-n"
                | "--notes"
                | "-D"
                | "--design"
                | "-a"
                | "--assignee"
        ) {
            idx += 1;
            (arg.as_str(), args.get(idx)?.clone())
        } else if let Some(value) = arg.strip_prefix("--status=") {
            ("--status", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--title=") {
            ("--title", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--description=") {
            ("--description", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--notes=") {
            ("--notes", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--design=") {
            ("--design", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--assignee=") {
            ("--assignee", value.to_string())
        } else {
            return None;
        };
        match name {
            "-s" | "--status" => {
                parse_status(&value)?;
                fields.status = Some(value);
            }
            "-t" | "--title" => fields.title = Some(value),
            "-d" | "--description" => fields.description = Some(value),
            "-n" | "--notes" => fields.notes = Some(value),
            "-D" | "--design" => fields.design = Some(value),
            "-a" | "--assignee" => fields.assignee = Some(value),
            _ => return None,
        }
        idx += 1;
    }
    Some(fields)
}

fn parse_close_args(args: &[String]) -> Option<(Vec<String>, Option<String>)> {
    let mut ids = Vec::new();
    let mut reason = None;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-r" || arg == "--reason" {
            idx += 1;
            reason = Some(args.get(idx)?.clone());
        } else if let Some(value) = arg.strip_prefix("--reason=") {
            reason = Some(value.to_string());
        } else if arg.starts_with('-') {
            return None;
        } else {
            ids.push(arg.clone());
        }
        idx += 1;
    }
    Some((ids, reason))
}

fn read_issues(
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    if read_beads_dirs.is_empty() {
        read_store_issues(write_beads_dir)
    } else {
        merge_workspace_issues(read_beads_dirs)
    }
}

fn find_issue<'a>(
    issues: &'a [IssueWire],
    issue_id: &str,
) -> Option<&'a IssueWire> {
    issues.iter().find(|issue| issue.id == issue_id)
}

fn render_dependency(
    stdout: &mut String,
    issues: &[IssueWire],
    dep: &DependencyWire,
    arrow: &str,
) {
    if let Some(dep_issue) = find_issue(issues, &dep.depends_on_id) {
        writeln!(
            stdout,
            "  {arrow} {} {}: {}   [{}]",
            status_icon(&dep_issue.status),
            dep_issue.id,
            dep_issue.title,
            status_upper(&dep_issue.status)
        )
        .expect("writing to String cannot fail");
    } else {
        writeln!(stdout, "  {arrow} {} (not found)", dep.depends_on_id)
            .expect("writing to String cannot fail");
    }
}

fn blocking_issue_ids(issues: &[IssueWire], issue_id: &str) -> Vec<String> {
    let mut sorted = issues.to_vec();
    sort_by_created_at(&mut sorted);
    sorted
        .into_iter()
        .filter(|other| {
            other
                .dependencies
                .iter()
                .any(|dep| dep.depends_on_id == issue_id)
        })
        .map(|issue| issue.id)
        .collect()
}

fn has_active_blocker(
    issue: &IssueWire,
    status_by_id: &BTreeMap<String, StatusWire>,
) -> bool {
    issue.dependencies.iter().any(|dep| {
        status_by_id
            .get(dep.depends_on_id.as_str())
            .is_some_and(|status| {
                matches!(*status, StatusWire::Open | StatusWire::InProgress)
            })
    })
}

fn stats_for_issues(issues: &[IssueWire]) -> BTreeMap<String, usize> {
    let mut stats = BTreeMap::new();
    for issue in issues {
        *stats
            .entry(status_value(&issue.status).to_string())
            .or_insert(0) += 1;
        *stats
            .entry(issue_type_value(&issue.issue_type).to_string())
            .or_insert(0) += 1;
    }
    stats.insert("total".to_string(), issues.len());
    stats
}

fn mutation_summary(
    operation: &str,
    outcome: &BeadMutationOutcomeWire,
    old_issue: Option<&IssueWire>,
) -> BeadCliMutationSummaryWire {
    let mut status_transitions = Vec::new();
    if let (Some(old), Some(new)) = (old_issue, outcome.issue.as_ref()) {
        if old.status != new.status {
            status_transitions.push(BeadCliStatusTransitionWire {
                from_status: status_value(&old.status).to_string(),
                to_status: status_value(&new.status).to_string(),
            });
        }
    }
    BeadCliMutationSummaryWire {
        operation: operation.to_string(),
        issue_ids: outcome.issue_ids.clone(),
        status_transitions,
    }
}

fn sort_by_created_at(issues: &mut [IssueWire]) {
    issues
        .sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
}

fn parse_status(value: &str) -> Option<StatusWire> {
    match value {
        "open" => Some(StatusWire::Open),
        "in_progress" => Some(StatusWire::InProgress),
        "closed" => Some(StatusWire::Closed),
        _ => None,
    }
}

fn parse_issue_type(value: &str) -> Option<IssueTypeWire> {
    match value {
        "plan" => Some(IssueTypeWire::Plan),
        "phase" => Some(IssueTypeWire::Phase),
        _ => None,
    }
}

fn status_icon(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "○",
        StatusWire::InProgress => "◐",
        StatusWire::Closed => "✓",
    }
}

fn status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn status_upper(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "OPEN",
        StatusWire::InProgress => "IN_PROGRESS",
        StatusWire::Closed => "CLOSED",
    }
}

fn issue_type_value(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
    }
}

fn display_design_path(
    design: &str,
    cwd: &Path,
    relativize_design_paths: bool,
) -> String {
    if !relativize_design_paths {
        return design.to_string();
    }
    let path = Path::new(design);
    path.strip_prefix(cwd)
        .map(|relative| relative.display().to_string())
        .unwrap_or_else(|_| design.to_string())
}

fn success(stdout: String) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 0,
        stdout,
        stderr: String::new(),
        mutation_summary: None,
    }
}

fn success_with_mutation(
    stdout: String,
    mutation_summary: BeadCliMutationSummaryWire,
) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 0,
        stdout,
        stderr: String::new(),
        mutation_summary: Some(mutation_summary),
    }
}

fn error(stderr: String) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 1,
        stdout: String::new(),
        stderr,
        mutation_summary: None,
    }
}

fn defer() -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: false,
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
        mutation_summary: None,
    }
}
