//! Narrow `sase bead` CLI execution planner.
//!
//! Python still owns workspace discovery, help text, and host-coupled
//! commands. This module handles the common bead commands once Python has
//! resolved the store paths.

use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::plan::canonicalize_plan_reference;

use super::mutation::{
    add_dependency, close_issues, create_issue, open_issue, remove_issues,
    update_issue, BeadCreateRequestWire, BeadMutationOutcomeWire,
    BeadUpdateFieldsWire,
};
use super::read::read_store_issues;
use super::search::search_issues_in_issues;
use super::wire::{
    BeadError, BeadResolutionWire, BeadSearchMatchWire, BeadTierWire,
    DependencyWire, IssueTypeWire, IssueWire, StatusWire,
};
use crate::plan::refs::{parse_plan_reference, resolve_plan_reference};

/// Rendered when a stored plan reference matches no plan file.
const PLAN_REFERENCE_MISSING_LABEL: &str = "(unresolved: no plan file found)";
/// Rendered when a stored plan reference matches more than one plan file.
const PLAN_REFERENCE_AMBIGUOUS_LABEL: &str =
    "(ambiguous: multiple plans match this reference)";
/// Rendered when a stored `plans:` value violates the reference grammar.
const PLAN_REFERENCE_INVALID_LABEL: &str =
    "(unresolved: malformed plan reference)";
/// Marks a reference that only resolved after ignoring its month directory.
const PLAN_REFERENCE_DRIFT_SUFFIX: &str = " (month drift)";

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
    plan_roots: &[PathBuf],
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
            plan_roots,
        ),
        "search" => handle_search(
            &argv[1..],
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
            plan_roots,
        ),
        "ready" => handle_ready(&argv[1..], read_beads_dirs, write_beads_dir),
        "blocked" => {
            handle_blocked(&argv[1..], read_beads_dirs, write_beads_dir)
        }
        "stats" => handle_stats(&argv[1..], read_beads_dirs, write_beads_dir),
        "create" => handle_create(
            &argv[1..],
            write_beads_dir,
            cwd,
            relativize_design_paths,
        ),
        "open" => handle_open(&argv[1..], write_beads_dir),
        "update" => handle_update(&argv[1..], write_beads_dir),
        "close" => handle_close(&argv[1..], write_beads_dir),
        "dep" => handle_dep(&argv[1..], write_beads_dir),
        "rm" => handle_rm(&argv[1..], write_beads_dir),
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
            && filters.tiers.as_ref().map_or(true, |tiers| {
                issue.tier.as_ref().is_some_and(|tier| tiers.contains(tier))
            })
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
    plan_roots: &[PathBuf],
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
        "Type: {}{} · Owner: {}",
        issue_type_value(&issue.issue_type),
        issue_tier_suffix(issue),
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
    if issue.status == StatusWire::Claimed {
        writeln!(
            stdout,
            "Claimed by: {} (agent has not started working yet)",
            issue.assignee
        )
        .expect("writing to String cannot fail");
    }
    if !issue.model.is_empty() {
        writeln!(stdout, "Model: {}", issue.model)
            .expect("writing to String cannot fail");
    }
    if issue.status == StatusWire::Closed {
        write!(
            stdout,
            "\nRESOLUTION\n  Resolution: {}\n  Close reason: {}\n  Closed at: {}\n",
            issue
                .resolution
                .as_ref()
                .map(BeadResolutionWire::as_str)
                .unwrap_or("(unrecorded)"),
            issue.close_reason.as_deref().unwrap_or("(none)"),
            issue.closed_at.as_deref().unwrap_or("(unknown)")
        )
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
        let display = display_design_path(
            &issue.design,
            cwd,
            relativize_design_paths,
            plan_roots,
        );
        write!(stdout, "\nPLAN\n  {}\n", display.join("\n  "))
            .expect("writing to String cannot fail");
    }

    Ok(success(stdout))
}

fn handle_search(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
    plan_roots: &[PathBuf],
) -> Result<BeadCliOutcomeWire, BeadError> {
    let search_args = match parse_search_args(args) {
        SearchParseOutcome::Parsed(args) => args,
        SearchParseOutcome::UsageError(message) => {
            return Ok(usage_error(format!("Error: {message}\n")));
        }
        SearchParseOutcome::Defer => return Ok(defer()),
    };
    let issues = read_issues(read_beads_dirs, write_beads_dir)?;
    let matches = match search_issues_in_issues(
        issues,
        &search_args.query,
        optional_filter(&search_args.statuses),
        optional_filter(&search_args.issue_types),
        optional_filter(&search_args.tiers),
        search_args.limit,
    ) {
        Ok(matches) => matches,
        Err(err) if err.kind == "validation" => {
            return Ok(usage_error(format!("Error: {}\n", err.message)));
        }
        Err(err) => return Err(err),
    };
    let color = search_args.format != SearchFormat::Json
        && search_args.color.resolve_stdout();
    let stdout = match search_args.format {
        SearchFormat::Compact => {
            render_search_compact(&matches, &search_args.query, color)
        }
        SearchFormat::Json => render_search_json(&matches, &search_args.query)?,
        SearchFormat::Full => render_search_full(
            &matches,
            &search_args.query,
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
            plan_roots,
        )?,
    };

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
            && is_ready_surface_issue(issue)
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
        "Issue Statistics\n  Total:       {}\n  Open:        {}\n  Claimed:     {}\n  In Progress: {}\n  Closed:      {}\n  Plans:       {}\n  Phases:      {}\n",
        stats.get("total").copied().unwrap_or(0),
        stats.get("open").copied().unwrap_or(0),
        stats.get("claimed").copied().unwrap_or(0),
        stats.get("in_progress").copied().unwrap_or(0),
        stats.get("closed").copied().unwrap_or(0),
        stats.get("plan").copied().unwrap_or(0),
        stats.get("phase").copied().unwrap_or(0),
    );
    Ok(success(stdout))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CreateArgs {
    title: String,
    issue_type: IssueTypeWire,
    parent_id: Option<String>,
    plan_path: Option<String>,
    description: String,
    assignee: String,
    tier: Option<BeadTierWire>,
    changespec_name: String,
    changespec_bug_id: String,
    model: String,
}

fn handle_create(
    args: &[String],
    write_beads_dir: &Path,
    cwd: &Path,
    _relativize_design_paths: bool,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let parsed = match parse_create_args(args) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => return Ok(defer()),
        Err(message) => return Ok(error(format!("Error: {message}\n"))),
    };

    if parsed.issue_type != IssueTypeWire::Plan
        && (!parsed.changespec_name.is_empty()
            || !parsed.changespec_bug_id.is_empty())
    {
        return Ok(error(
            "Error: ChangeSpec metadata can only be attached to plan beads\n"
                .to_string(),
        ));
    }
    if !parsed.changespec_bug_id.is_empty() && parsed.changespec_name.is_empty()
    {
        return Ok(error(
            "Error: --bug-id requires --changespec\n".to_string(),
        ));
    }
    if parsed.issue_type != IssueTypeWire::Plan && parsed.tier.is_some() {
        return Ok(error(
            "Error: --tier can only be set on plan beads\n".to_string(),
        ));
    }
    if let Some(parent_id) = parsed.parent_id.as_deref() {
        let issues = read_store_issues(write_beads_dir).unwrap_or_default();
        if find_issue(&issues, parent_id).is_none() {
            return Ok(error(format!(
                "Error: parent bead not found: {parent_id}\n"
            )));
        }
    }

    let design = match parsed.plan_path.as_deref() {
        Some(plan_path) => {
            match storage_design_path(plan_path, cwd, write_beads_dir) {
                Ok(path) => path,
                Err(message) => {
                    return Ok(error(format!("Error: {message}\n")))
                }
            }
        }
        None => String::new(),
    };
    let request = BeadCreateRequestWire {
        title: parsed.title,
        issue_type: parsed.issue_type,
        tier: parsed.tier,
        parent_id: parsed.parent_id,
        description: parsed.description,
        design,
        model: parsed.model,
        assignee: parsed.assignee,
        changespec_name: parsed.changespec_name,
        changespec_bug_id: parsed.changespec_bug_id,
        ..BeadCreateRequestWire::default()
    };
    match create_issue(write_beads_dir, request) {
        Ok(outcome) => {
            let issue =
                outcome.issue.as_ref().expect("create outcome has issue");
            Ok(success_with_mutation(
                format!(
                    "Created {}: {} — {}\n",
                    issue_type_value(&issue.issue_type),
                    issue.id,
                    issue.title
                ),
                mutation_summary("create", &outcome, None),
            ))
        }
        Err(err) if err.kind == "validation" || err.kind == "not_found" => {
            Ok(error(format!("Error: {}\n", err.message)))
        }
        Err(err) => Err(err),
    }
}

fn storage_design_path(
    raw_path: &str,
    cwd: &Path,
    write_beads_dir: &Path,
) -> Result<String, String> {
    let supplied = Path::new(raw_path);
    let resolved = if supplied.is_absolute() {
        supplied.to_path_buf()
    } else {
        cwd.join(supplied)
    };
    if !resolved.is_file() {
        return Err(format!("plan file not found: {raw_path}"));
    }
    let normalized = fs::canonicalize(&resolved).unwrap_or(resolved);
    let storage_root = design_storage_root(cwd, write_beads_dir);
    let plan_roots = design_plan_roots(storage_root, write_beads_dir)
        .into_iter()
        .map(|root| fs::canonicalize(&root).unwrap_or(root))
        .collect::<Vec<_>>();
    if let Some(reference) =
        canonicalize_plan_reference(&normalized, &plan_roots)
            .map_err(|err| err.message)?
    {
        return Ok(reference);
    }
    Ok(normalized
        .strip_prefix(storage_root)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| normalized.display().to_string()))
}

fn design_plan_roots(storage_root: &Path, beads_dir: &Path) -> Vec<PathBuf> {
    let components = beads_dir
        .components()
        .rev()
        .take(4)
        .map(|component| component.as_os_str())
        .collect::<Vec<_>>();
    if components
        .iter()
        .map(|value| value.to_string_lossy())
        .eq(["beads", "plans", "repos", "sase"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched sidecar beads directory has a parent")
            .to_path_buf()];
    }
    if components
        .iter()
        .take(3)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd", ".sase"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched local beads directory has a parent")
            .join("plans")];
    }
    if components
        .iter()
        .take(2)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd"])
    {
        return vec![beads_dir
            .parent()
            .expect("matched in-tree beads directory has a parent")
            .join("plans")];
    }
    vec![storage_root.join("plans")]
}

fn design_storage_root<'a>(cwd: &'a Path, beads_dir: &'a Path) -> &'a Path {
    let components = beads_dir
        .components()
        .rev()
        .take(4)
        .map(|component| component.as_os_str())
        .collect::<Vec<_>>();
    if components
        .iter()
        .map(|value| value.to_string_lossy())
        .eq(["beads", "plans", "repos", "sase"])
    {
        return beads_dir
            .ancestors()
            .nth(4)
            .expect("four matched path components have a parent");
    }
    if components
        .iter()
        .take(3)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd", ".sase"])
    {
        return beads_dir
            .ancestors()
            .nth(3)
            .expect("three matched path components have a parent");
    }
    if components
        .iter()
        .take(2)
        .map(|value| value.to_string_lossy())
        .eq(["beads", "sdd"])
    {
        return beads_dir
            .ancestors()
            .nth(2)
            .expect("two matched path components have a parent");
    }
    cwd
}

fn parse_create_args(args: &[String]) -> Result<Option<CreateArgs>, String> {
    let mut title = None;
    let mut type_arg = None;
    let mut description = String::new();
    let mut assignee = String::new();
    let mut tier = None;
    let mut changespec_name = String::new();
    let mut changespec_bug_id = String::new();
    let mut model = String::new();
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        let (name, value) = if matches!(
            arg.as_str(),
            "-t" | "--title"
                | "-T"
                | "--type"
                | "-d"
                | "--description"
                | "-a"
                | "--assignee"
                | "--tier"
                | "-c"
                | "--changespec"
                | "-b"
                | "--bug-id"
                | "-m"
                | "--model"
        ) {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return Ok(None);
            };
            (arg.as_str(), value.clone())
        } else if let Some((name, value)) = arg.split_once('=') {
            (name, value.to_string())
        } else {
            return Ok(None);
        };
        match name {
            "-t" | "--title" => title = Some(value),
            "-T" | "--type" => type_arg = Some(value),
            "-d" | "--description" => description = value,
            "-a" | "--assignee" => assignee = value,
            "--tier" => {
                tier =
                    Some(parse_tier(&value).ok_or_else(|| {
                        format!("invalid --tier value: {value}")
                    })?)
            }
            "-c" | "--changespec" => changespec_name = value,
            "-b" | "--bug-id" => changespec_bug_id = value,
            "-m" | "--model" => model = value,
            _ => return Ok(None),
        }
        idx += 1;
    }
    let (Some(title), Some(type_arg)) = (title, type_arg) else {
        return Ok(None);
    };
    let (issue_type, plan_path, parent_id) = parse_create_type(&type_arg)?;
    Ok(Some(CreateArgs {
        title,
        issue_type,
        parent_id,
        plan_path,
        description,
        assignee,
        tier,
        changespec_name,
        changespec_bug_id,
        model,
    }))
}

fn parse_create_type(
    value: &str,
) -> Result<(IssueTypeWire, Option<String>, Option<String>), String> {
    let Some((kind, rest)) = value.split_once('(') else {
        return Err(format!(
            "invalid --type value: {value}\nExpected: plan(<plan_file>), plan(<plan_file>,<parent_id>), or phase(<parent_id>)"
        ));
    };
    let Some(inner) = rest.strip_suffix(')') else {
        return Err(format!(
            "invalid --type value: {value}\nExpected: plan(<plan_file>), plan(<plan_file>,<parent_id>), or phase(<parent_id>)"
        ));
    };
    let parts = inner
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    match (kind, parts.as_slice()) {
        ("plan", [path]) => Ok((
            IssueTypeWire::Plan,
            Some(path.clone()),
            None,
        )),
        ("plan", [path, parent]) => Ok((
            IssueTypeWire::Plan,
            Some(path.clone()),
            Some(parent.clone()),
        )),
        ("plan", _) => Err(format!(
            "plan() expects 1 or 2 arguments, got {}",
            parts.len()
        )),
        ("phase", [parent]) => Ok((
            IssueTypeWire::Phase,
            None,
            Some(parent.clone()),
        )),
        ("phase", _) => Err(format!(
            "phase() expects exactly 1 argument, got {}",
            parts.len()
        )),
        _ => Err(format!(
            "invalid --type value: {value}\nExpected: plan(<plan_file>), plan(<plan_file>,<parent_id>), or phase(<parent_id>)"
        )),
    }
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
            let mut stdout =
                format!("○ Opened: {} — {}\n", issue.id, issue.title);
            for ancestor in &outcome.issues {
                writeln!(
                    stdout,
                    "○ Reopened ancestor: {} — {}",
                    ancestor.id, ancestor.title
                )
                .expect("writing to String cannot fail");
            }
            Ok(success_with_mutation(
                stdout,
                mutation_summary("open", &outcome, old.as_ref()),
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
    let Some((ids, force, reason, resolution)) = parse_close_args(args) else {
        return Ok(defer());
    };
    if ids.is_empty() {
        return Ok(defer());
    }
    let old_issues = read_store_issues(write_beads_dir).unwrap_or_default();
    match close_issues(write_beads_dir, &ids, reason, resolution, force, None) {
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
                    issue_ids: ids,
                    status_transitions: outcome
                        .issues
                        .iter()
                        .filter_map(|issue| {
                            let old = find_issue(&old_issues, &issue.id)?;
                            (old.status != StatusWire::Closed).then(|| {
                                BeadCliStatusTransitionWire {
                                    from_status: status_value(&old.status)
                                        .to_string(),
                                    to_status: "closed".to_string(),
                                }
                            })
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
                    issue_ids: vec![issue_id.clone(), depends_on_id.clone()],
                    status_transitions: Vec::new(),
                },
            ))
        }
        Err(err) => Err(err),
    }
}

fn handle_rm(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.is_empty() {
        return Ok(defer());
    }
    match remove_issues(write_beads_dir, args) {
        Ok(outcome) => {
            let mut stdout = String::new();
            for issue in &outcome.issues {
                writeln!(stdout, "✗ Removed: {} — {}", issue.id, issue.title)
                    .expect("writing to String cannot fail");
            }
            Ok(success_with_mutation(
                stdout,
                BeadCliMutationSummaryWire {
                    operation: "rm".to_string(),
                    issue_ids: args.to_vec(),
                    status_transitions: Vec::new(),
                },
            ))
        }
        Err(err) if err.kind == "not_found" => {
            let issue_id = err
                .message
                .strip_prefix("Issue not found: ")
                .unwrap_or(&err.message);
            Ok(error(format!("Error: issue not found: {issue_id}\n")))
        }
        Err(err) => Err(err),
    }
}

#[derive(Debug)]
struct ListFilters {
    statuses: Vec<StatusWire>,
    issue_types: Option<Vec<IssueTypeWire>>,
    tiers: Option<Vec<BeadTierWire>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchFormat {
    Compact,
    Json,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColorMode {
    Auto,
    Always,
    Never,
}

impl ColorMode {
    fn resolve_stdout(self) -> bool {
        match self {
            Self::Auto => {
                std::io::stdout().is_terminal()
                    && env::var_os("NO_COLOR").is_none()
            }
            Self::Always => true,
            Self::Never => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchArgs {
    query: String,
    format: SearchFormat,
    statuses: Vec<String>,
    issue_types: Vec<String>,
    tiers: Vec<String>,
    limit: Option<usize>,
    color: ColorMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SearchParseOutcome {
    Parsed(SearchArgs),
    UsageError(String),
    Defer,
}

fn optional_filter(values: &[String]) -> Option<&[String]> {
    (!values.is_empty()).then_some(values)
}

fn parse_list_filters(args: &[String]) -> Option<ListFilters> {
    let mut statuses = Vec::new();
    let mut issue_types = Vec::new();
    let mut tiers = Vec::new();
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
        } else if arg == "--tier" {
            idx += 1;
            let value = args.get(idx)?;
            tiers.push(parse_tier(value)?);
        } else {
            let value = arg.strip_prefix("--tier=")?;
            tiers.push(parse_tier(value)?);
        }
        idx += 1;
    }
    if statuses.is_empty() {
        statuses.push(StatusWire::Open);
        statuses.push(StatusWire::Claimed);
        statuses.push(StatusWire::InProgress);
    }
    Some(ListFilters {
        statuses,
        issue_types: (!issue_types.is_empty()).then_some(issue_types),
        tiers: (!tiers.is_empty()).then_some(tiers),
    })
}

fn parse_search_args(args: &[String]) -> SearchParseOutcome {
    let mut query = None;
    let mut format = SearchFormat::Compact;
    let mut statuses = Vec::new();
    let mut issue_types = Vec::new();
    let mut tiers = Vec::new();
    let mut limit = None;
    let mut color = ColorMode::Auto;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-f" || arg == "--format" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            let Some(parsed) = parse_search_format(value) else {
                return SearchParseOutcome::Defer;
            };
            format = parsed;
        } else if let Some(value) = arg.strip_prefix("--format=") {
            let Some(parsed) = parse_search_format(value) else {
                return SearchParseOutcome::Defer;
            };
            format = parsed;
        } else if arg == "-s" || arg == "--status" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            if parse_status(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            statuses.push(value.clone());
        } else if let Some(value) = arg.strip_prefix("--status=") {
            if parse_status(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            statuses.push(value.to_string());
        } else if arg == "-t" || arg == "--type" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            if parse_issue_type(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            issue_types.push(value.clone());
        } else if let Some(value) = arg.strip_prefix("--type=") {
            if parse_issue_type(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            issue_types.push(value.to_string());
        } else if arg == "--tier" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            if parse_tier(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            tiers.push(value.clone());
        } else if let Some(value) = arg.strip_prefix("--tier=") {
            if parse_tier(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            tiers.push(value.to_string());
        } else if arg == "-n" || arg == "--limit" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            let Some(parsed) = parse_limit(value) else {
                return SearchParseOutcome::Defer;
            };
            limit = Some(parsed);
        } else if let Some(value) = arg.strip_prefix("--limit=") {
            let Some(parsed) = parse_limit(value) else {
                return SearchParseOutcome::Defer;
            };
            limit = Some(parsed);
        } else if arg == "-c" || arg == "--color" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            let Some(parsed) = parse_color_mode(value) else {
                return SearchParseOutcome::Defer;
            };
            color = parsed;
        } else if let Some(value) = arg.strip_prefix("--color=") {
            let Some(parsed) = parse_color_mode(value) else {
                return SearchParseOutcome::Defer;
            };
            color = parsed;
        } else if arg.starts_with('-') {
            return SearchParseOutcome::Defer;
        } else if query.is_none() {
            query = Some(arg.clone());
        } else {
            return SearchParseOutcome::Defer;
        }
        idx += 1;
    }

    let Some(query) = query else {
        return SearchParseOutcome::UsageError(
            "search query cannot be empty".to_string(),
        );
    };
    if query.trim().is_empty() {
        return SearchParseOutcome::UsageError(
            "search query cannot be empty".to_string(),
        );
    }

    SearchParseOutcome::Parsed(SearchArgs {
        query,
        format,
        statuses,
        issue_types,
        tiers,
        limit,
        color,
    })
}

fn parse_search_format(value: &str) -> Option<SearchFormat> {
    match value {
        "compact" => Some(SearchFormat::Compact),
        "json" => Some(SearchFormat::Json),
        "full" => Some(SearchFormat::Full),
        _ => None,
    }
}

fn parse_color_mode(value: &str) -> Option<ColorMode> {
    match value {
        "auto" => Some(ColorMode::Auto),
        "always" => Some(ColorMode::Always),
        "never" => Some(ColorMode::Never),
        _ => None,
    }
}

fn parse_limit(value: &str) -> Option<usize> {
    value.parse::<usize>().ok()
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
                | "-m"
                | "--model"
                | "-a"
                | "--assignee"
                | "-E"
                | "--epic-count"
                | "--tier"
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
        } else if let Some(value) = arg.strip_prefix("--model=") {
            ("--model", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--assignee=") {
            ("--assignee", value.to_string())
        } else {
            let value = arg.strip_prefix("--tier=")?;
            ("--tier", value.to_string())
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
            "-m" | "--model" => fields.model = Some(value),
            "-a" | "--assignee" => fields.assignee = Some(value),
            "--tier" => fields.tier = Some(parse_tier(&value)?),
            _ => return None,
        }
        idx += 1;
    }
    Some(fields)
}

type ParsedCloseArgs = (
    Vec<String>,
    bool,
    Option<String>,
    Option<BeadResolutionWire>,
);

fn parse_close_args(args: &[String]) -> Option<ParsedCloseArgs> {
    let mut ids = Vec::new();
    let mut force = false;
    let mut reason = None;
    let mut resolution = None;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-f" || arg == "--force" {
            force = true;
        } else if arg == "-r" || arg == "--reason" {
            idx += 1;
            reason = Some(args.get(idx)?.clone());
        } else if let Some(value) = arg.strip_prefix("--reason=") {
            reason = Some(value.to_string());
        } else if arg == "-R" || arg == "--resolution" {
            idx += 1;
            resolution = Some(parse_resolution(args.get(idx)?)?);
        } else if let Some(value) = arg.strip_prefix("--resolution=") {
            resolution = Some(parse_resolution(value)?);
        } else if arg.starts_with('-') {
            return None;
        } else {
            ids.push(arg.clone());
        }
        idx += 1;
    }
    Some((ids, force, reason, resolution))
}

fn parse_resolution(value: &str) -> Option<BeadResolutionWire> {
    match value {
        "done" => Some(BeadResolutionWire::Done),
        "canceled" => Some(BeadResolutionWire::Canceled),
        "superseded" => Some(BeadResolutionWire::Superseded),
        _ => None,
    }
}

fn read_issues(
    _read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
) -> Result<Vec<IssueWire>, BeadError> {
    read_store_issues(write_beads_dir)
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

fn render_search_compact(
    matches: &[BeadSearchMatchWire],
    query: &str,
    color: bool,
) -> String {
    if matches.is_empty() {
        return format!("No beads match \"{query}\".\n");
    }

    let mut stdout = String::new();
    for result in matches {
        let issue = &result.issue;
        writeln!(
            stdout,
            "{} {} · {}",
            color_status_icon(&issue.status, color),
            color_issue_id(&issue.id, color),
            highlight_matches(&issue.title, query, color),
        )
        .expect("writing to String cannot fail");
        if let Some(snippet) = compact_snippet(result, query, color) {
            writeln!(stdout, "{}", dim_line(&format!("  {snippet}"), color))
                .expect("writing to String cannot fail");
        }
    }
    stdout
}

fn render_search_json(
    matches: &[BeadSearchMatchWire],
    query: &str,
) -> Result<String, BeadError> {
    #[derive(Serialize)]
    struct SearchEnvelope<'a> {
        query: &'a str,
        count: usize,
        results: &'a [BeadSearchMatchWire],
    }

    let mut stdout = serde_json::to_string_pretty(&SearchEnvelope {
        query,
        count: matches.len(),
        results: matches,
    })?;
    stdout.push('\n');
    Ok(stdout)
}

fn render_search_full(
    matches: &[BeadSearchMatchWire],
    query: &str,
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
    plan_roots: &[PathBuf],
) -> Result<String, BeadError> {
    if matches.is_empty() {
        return Ok(format!("No beads match \"{query}\".\n"));
    }

    let mut stdout = String::new();
    for (idx, result) in matches.iter().enumerate() {
        if idx > 0 {
            writeln!(stdout, "\n{}", "-".repeat(60))
                .expect("writing to String cannot fail");
        }
        let show_outcome = handle_show(
            std::slice::from_ref(&result.issue.id),
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
            plan_roots,
        )?;
        stdout.push_str(&show_outcome.stdout);
    }
    Ok(stdout)
}

fn compact_snippet(
    result: &BeadSearchMatchWire,
    query: &str,
    color: bool,
) -> Option<String> {
    let issue = &result.issue;
    let has_title_or_description_match = result
        .matched_fields
        .iter()
        .any(|field| field == "title" || field == "description");
    let description = single_line_snippet(&issue.description, query, 96);
    if has_title_or_description_match && !description.is_empty() {
        return Some(highlight_matches(&description, query, color));
    }

    result
        .matched_fields
        .iter()
        .filter(|field| field.as_str() != "title")
        .find_map(|field| {
            let value = search_field_display_value(issue, field)?;
            let snippet = single_line_snippet(&value, query, 96);
            (!snippet.is_empty()).then(|| {
                format!(
                    "{}: \"{}\"",
                    field,
                    highlight_matches(&snippet, query, color)
                )
            })
        })
}

fn search_field_display_value(
    issue: &IssueWire,
    field: &str,
) -> Option<String> {
    match field {
        "id" => Some(issue.id.clone()),
        "title" => Some(issue.title.clone()),
        "description" => Some(issue.description.clone()),
        "notes" => Some(issue.notes.clone()),
        "design" => Some(issue.design.clone()),
        "owner" => Some(issue.owner.clone()),
        "assignee" => Some(issue.assignee.clone()),
        "model" => Some(issue.model.clone()),
        "size" => issue.size.as_ref().map(|size| size.as_str().to_string()),
        "changespec_name" => Some(issue.changespec_name.clone()),
        "changespec_bug_id" => Some(issue.changespec_bug_id.clone()),
        "status" => Some(status_value(&issue.status).to_string()),
        "type" => Some(issue_type_value(&issue.issue_type).to_string()),
        "tier" => issue.tier.as_ref().map(|tier| tier_value(tier).to_string()),
        _ => None,
    }
}

fn single_line_snippet(value: &str, query: &str, max_chars: usize) -> String {
    let line = value.lines().next().unwrap_or("").trim();
    if line.chars().count() <= max_chars {
        return line.to_string();
    }

    let ranges = case_insensitive_byte_ranges(line, query);
    let Some((match_start, match_end)) = ranges.first().copied() else {
        return truncate_chars(line, max_chars);
    };
    let total_chars = line.chars().count();
    let match_start_char = byte_to_char_index(line, match_start);
    let match_end_char = byte_to_char_index(line, match_end);
    let match_len = match_end_char.saturating_sub(match_start_char);
    let context = max_chars.saturating_sub(match_len).saturating_div(2);
    let mut start = match_start_char.saturating_sub(context);
    let mut end = (start + max_chars).min(total_chars);
    if end < match_end_char {
        end = match_end_char.min(total_chars);
        start = end.saturating_sub(max_chars);
    }

    let mut snippet = String::new();
    if start > 0 {
        snippet.push_str("...");
    }
    snippet.push_str(&chars_range(line, start, end));
    if end < total_chars {
        snippet.push_str("...");
    }
    snippet
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut truncated = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        truncated.push_str("...");
    }
    truncated
}

fn chars_range(value: &str, start: usize, end: usize) -> String {
    value
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn byte_to_char_index(value: &str, byte_idx: usize) -> usize {
    value
        .char_indices()
        .take_while(|(idx, _)| *idx < byte_idx)
        .count()
}

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_BOLD_BLUE: &str = "\x1b[1;34m";
const ANSI_HIGHLIGHT: &str = "\x1b[30;43m";
const ANSI_HIGHLIGHT_RESET: &str = "\x1b[39;49m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_MAGENTA: &str = "\x1b[35m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_CYAN: &str = "\x1b[36m";

fn color_status_icon(status: &StatusWire, color: bool) -> String {
    let icon = status_icon(status);
    if !color {
        return icon.to_string();
    }
    let code = match status {
        StatusWire::Open => ANSI_CYAN,
        StatusWire::Claimed => ANSI_MAGENTA,
        StatusWire::InProgress => ANSI_YELLOW,
        StatusWire::Closed => ANSI_GREEN,
    };
    format!("{code}{icon}{ANSI_RESET}")
}

fn color_issue_id(issue_id: &str, color: bool) -> String {
    if color {
        format!("{ANSI_BOLD_BLUE}{issue_id}{ANSI_RESET}")
    } else {
        issue_id.to_string()
    }
}

fn dim_line(line: &str, color: bool) -> String {
    if color {
        format!("{ANSI_DIM}{line}{ANSI_RESET}")
    } else {
        line.to_string()
    }
}

fn highlight_matches(text: &str, query: &str, color: bool) -> String {
    if !color {
        return text.to_string();
    }
    let ranges = case_insensitive_byte_ranges(text, query);
    if ranges.is_empty() {
        return text.to_string();
    }

    let mut highlighted = String::new();
    let mut cursor = 0;
    for (start, end) in ranges {
        if start < cursor {
            continue;
        }
        highlighted.push_str(&text[cursor..start]);
        highlighted.push_str(ANSI_HIGHLIGHT);
        highlighted.push_str(&text[start..end]);
        highlighted.push_str(ANSI_HIGHLIGHT_RESET);
        cursor = end;
    }
    highlighted.push_str(&text[cursor..]);
    highlighted
}

fn case_insensitive_byte_ranges(
    text: &str,
    query: &str,
) -> Vec<(usize, usize)> {
    let needle = query.to_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }
    let (folded, offsets) = folded_with_offsets(text);
    let mut ranges = Vec::new();
    let mut search_start = 0;
    while let Some(relative_start) = folded[search_start..].find(&needle) {
        let folded_start = search_start + relative_start;
        let folded_end = folded_start + needle.len();
        let Some(original_start) = offsets
            .iter()
            .find(|(start, end, _, _)| {
                *start <= folded_start && folded_start < *end
            })
            .map(|(_, _, original_start, _)| *original_start)
        else {
            break;
        };
        let Some(original_end) = offsets
            .iter()
            .rev()
            .find(|(start, end, _, _)| {
                *start < folded_end && folded_end <= *end
            })
            .map(|(_, _, _, original_end)| *original_end)
        else {
            break;
        };
        ranges.push((original_start, original_end));
        search_start = folded_end;
    }
    ranges
}

fn folded_with_offsets(
    text: &str,
) -> (String, Vec<(usize, usize, usize, usize)>) {
    let mut folded = String::new();
    let mut offsets = Vec::new();
    for (original_start, ch) in text.char_indices() {
        let original_end = original_start + ch.len_utf8();
        for lower in ch.to_lowercase() {
            let folded_start = folded.len();
            folded.push(lower);
            let folded_end = folded.len();
            offsets.push((
                folded_start,
                folded_end,
                original_start,
                original_end,
            ));
        }
    }
    (folded, offsets)
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
                matches!(
                    *status,
                    StatusWire::Open
                        | StatusWire::Claimed
                        | StatusWire::InProgress
                )
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
        "claimed" => Some(StatusWire::Claimed),
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

fn parse_tier(value: &str) -> Option<BeadTierWire> {
    match value {
        "plan" => Some(BeadTierWire::Plan),
        "epic" => Some(BeadTierWire::Epic),
        _ => None,
    }
}

fn is_ready_surface_issue(issue: &IssueWire) -> bool {
    issue.issue_type == IssueTypeWire::Phase
        || issue.tier == Some(BeadTierWire::Epic)
}

fn status_icon(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "○",
        StatusWire::Claimed => "◎",
        StatusWire::InProgress => "◐",
        StatusWire::Closed => "✓",
    }
}

fn status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn status_upper(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "OPEN",
        StatusWire::Claimed => "CLAIMED",
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

fn issue_tier_suffix(issue: &IssueWire) -> String {
    if issue.issue_type != IssueTypeWire::Plan {
        return String::new();
    }
    issue
        .tier
        .as_ref()
        .map(|tier| format!(" · Tier: {}", tier_value(tier)))
        .unwrap_or_default()
}

fn tier_value(tier: &BeadTierWire) -> &'static str {
    match tier {
        BeadTierWire::Plan => "plan",
        BeadTierWire::Epic => "epic",
    }
}

/// Render a stored plan reference and where it currently resolves.
///
/// The first line is always the stable reference as stored. A second line
/// reports the resolved path, or says plainly that the reference resolves
/// nowhere; it is omitted when the resolved path is the reference itself.
fn display_design_path(
    design: &str,
    cwd: &Path,
    relativize_design_paths: bool,
    plan_roots: &[PathBuf],
) -> Vec<String> {
    let reference = design.to_string();
    let resolved = resolve_design_reference(design, cwd, plan_roots);
    let detail = match resolved {
        DesignResolution::Resolved { path, drifted } => {
            let display =
                display_plan_path(&path, cwd, relativize_design_paths);
            if display == reference {
                return vec![reference];
            }
            format!(
                "{display}{}",
                if drifted {
                    PLAN_REFERENCE_DRIFT_SUFFIX
                } else {
                    ""
                }
            )
        }
        DesignResolution::Ambiguous => {
            PLAN_REFERENCE_AMBIGUOUS_LABEL.to_string()
        }
        DesignResolution::Invalid => PLAN_REFERENCE_INVALID_LABEL.to_string(),
        DesignResolution::Missing => PLAN_REFERENCE_MISSING_LABEL.to_string(),
    };
    vec![reference, format!("→ {detail}")]
}

/// Where one stored `design` value points once the shared resolver has run.
enum DesignResolution {
    Resolved { path: PathBuf, drifted: bool },
    Ambiguous,
    Invalid,
    Missing,
}

fn resolve_design_reference(
    design: &str,
    cwd: &Path,
    plan_roots: &[PathBuf],
) -> DesignResolution {
    let Ok(resolution) = resolve_plan_reference(design, plan_roots) else {
        return DesignResolution::Invalid;
    };
    if let Some(path) = resolution.resolved_path.as_ref() {
        return DesignResolution::Resolved {
            path: PathBuf::from(path),
            drifted: resolution.status == "drifted",
        };
    }
    // A legacy relative path still names a file below the working directory,
    // which is how in-tree stores linked plans before `plans:` references.
    if let Some(path) = legacy_path_below_cwd(design, cwd) {
        return DesignResolution::Resolved {
            path,
            drifted: false,
        };
    }
    if resolution.status == "ambiguous" {
        return DesignResolution::Ambiguous;
    }
    DesignResolution::Missing
}

fn legacy_path_below_cwd(design: &str, cwd: &Path) -> Option<PathBuf> {
    let parsed = parse_plan_reference(design).ok()?;
    if !parsed.legacy {
        return None;
    }
    let path = Path::new(design);
    if path.is_absolute() {
        return None;
    }
    let candidate = cwd.join(path);
    candidate.is_file().then_some(candidate)
}

fn display_plan_path(
    path: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
) -> String {
    if !relativize_design_paths {
        return path.display().to_string();
    }
    path.strip_prefix(cwd)
        .map(|relative| relative.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
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

fn usage_error(stderr: String) -> BeadCliOutcomeWire {
    BeadCliOutcomeWire {
        handled: true,
        exit_code: 2,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::fs;
    use tempfile::{tempdir, TempDir};

    struct SeededStore {
        _temp: TempDir,
        beads_dir: PathBuf,
    }

    #[test]
    fn search_compact_renders_name_and_description() {
        let store = seed_issues(vec![phase_issue(
            "beads-1.1",
            "Fix Auth Token",
            "Rotate auth tokens safely.",
            StatusWire::InProgress,
            "2026-01-01T00:01:00Z",
        )]);

        let outcome = execute_search(
            &store.beads_dir,
            &["search", "auth", "--format", "compact", "--color", "never"],
        );

        assert_eq!(outcome.exit_code, 0);
        assert_eq!(
            outcome.stdout,
            "◐ beads-1.1 · Fix Auth Token\n  Rotate auth tokens safely.\n"
        );
    }

    #[test]
    fn claimed_status_is_in_default_list_with_claim_details_and_color() {
        let mut claimed = phase_issue(
            "beads-1.1",
            "Claimed phase",
            "Waiting to start.",
            StatusWire::Claimed,
            "2026-01-01T00:01:00Z",
        );
        claimed.assignee = "agent-one".to_string();
        let store = seed_issues(vec![
            claimed,
            phase_issue(
                "beads-1.2",
                "Closed phase",
                "",
                StatusWire::Closed,
                "2026-01-01T00:02:00Z",
            ),
        ]);

        let list = execute_search(&store.beads_dir, &["list"]);
        assert_eq!(list.stdout, "◎ beads-1.1 · Claimed phase ← beads-1\n");

        let show = execute_search(&store.beads_dir, &["show", "beads-1.1"]);
        assert!(show
            .stdout
            .starts_with("◎ beads-1.1 · Claimed phase   [CLAIMED]\n"));
        assert!(show.stdout.contains(
            "Claimed by: agent-one (agent has not started working yet)\n"
        ));

        let search = execute_search(
            &store.beads_dir,
            &["search", "waiting", "--color", "always"],
        );
        assert!(search.stdout.contains("\x1b[35m◎\x1b[0m"));
    }

    #[test]
    fn stats_prints_claimed_between_open_and_in_progress() {
        let store = seed_issues(vec![
            phase_issue(
                "beads-1.1",
                "Open phase",
                "",
                StatusWire::Open,
                "2026-01-01T00:01:00Z",
            ),
            phase_issue(
                "beads-1.2",
                "Claimed phase",
                "",
                StatusWire::Claimed,
                "2026-01-01T00:02:00Z",
            ),
            phase_issue(
                "beads-1.3",
                "Active phase",
                "",
                StatusWire::InProgress,
                "2026-01-01T00:03:00Z",
            ),
        ]);

        let stats = execute_search(&store.beads_dir, &["stats"]);
        assert_eq!(
            stats.stdout,
            concat!(
                "Issue Statistics\n",
                "  Total:       3\n",
                "  Open:        1\n",
                "  Claimed:     1\n",
                "  In Progress: 1\n",
                "  Closed:      0\n",
                "  Plans:       0\n",
                "  Phases:      3\n",
            )
        );
    }

    #[test]
    fn search_compact_orders_matches_newest_first() {
        // Seed oldest-first to prove ordering follows `created_at`, not the
        // stored/input order.
        let store = seed_issues(vec![
            phase_issue(
                "beads-1.1",
                "Auth older",
                "Older item.",
                StatusWire::Open,
                "2026-01-01T00:01:00Z",
            ),
            phase_issue(
                "beads-1.2",
                "Auth newer",
                "Newer item.",
                StatusWire::Open,
                "2026-01-01T00:02:00Z",
            ),
        ]);

        let outcome = execute_search(
            &store.beads_dir,
            &["search", "auth", "--format", "compact", "--color", "never"],
        );

        assert_eq!(outcome.exit_code, 0);
        assert_eq!(
            outcome.stdout,
            "○ beads-1.2 · Auth newer\n  Newer item.\n\
             ○ beads-1.1 · Auth older\n  Older item.\n"
        );
    }

    #[test]
    fn search_json_renders_stable_uncolored_envelope() {
        let store = seed_issues(vec![phase_issue(
            "beads-1.1",
            "Auth JSON",
            "Structured output",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        )]);

        let outcome = execute_search(
            &store.beads_dir,
            &["search", "auth", "-f", "json", "--color", "always"],
        );

        assert_eq!(outcome.exit_code, 0);
        assert!(!outcome.stdout.contains("\x1b["));
        let parsed: Value = serde_json::from_str(&outcome.stdout).unwrap();
        assert_eq!(parsed["query"], "auth");
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["results"][0]["issue"]["id"], "beads-1.1");
        assert_eq!(
            parsed["results"][0]["matched_fields"],
            serde_json::json!(["title"])
        );
    }

    #[test]
    fn search_full_reuses_show_rendering_for_single_result() {
        let plan = plan_issue(
            "beads-1",
            "Parent plan",
            "Plan description",
            StatusWire::Open,
            "2026-01-01T00:00:00Z",
        );
        let phase = phase_issue(
            "beads-1.1",
            "Full term phase",
            "Phase description",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        );
        let store = seed_issues(vec![plan, phase]);

        let search = execute_search(
            &store.beads_dir,
            &["search", "full term", "--format", "full"],
        );
        let show = execute_search(&store.beads_dir, &["show", "beads-1.1"]);

        assert_eq!(search.exit_code, 0);
        assert_eq!(search.stdout, show.stdout);
    }

    #[test]
    fn search_compact_color_always_highlights_matches() {
        let store = seed_issues(vec![phase_issue(
            "beads-1.1",
            "Auth token",
            "Rotate Auth token safely.",
            StatusWire::Closed,
            "2026-01-01T00:01:00Z",
        )]);

        let outcome = execute_search(
            &store.beads_dir,
            &["search", "auth", "--color", "always"],
        );

        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.stdout.contains("\x1b[32m✓\x1b[0m"));
        assert!(outcome.stdout.contains("\x1b[30;43mAuth\x1b[39;49m token"));
        assert!(outcome.stdout.contains(
            "\x1b[2m  Rotate \x1b[30;43mAuth\x1b[39;49m token safely.\x1b[0m"
        ));
    }

    #[test]
    fn search_applies_filters_and_limit() {
        let mut epic = plan_issue(
            "beads-2",
            "Auth epic",
            "Plan description",
            StatusWire::Open,
            "2026-01-01T00:03:00Z",
        );
        epic.tier = Some(BeadTierWire::Epic);
        let store = seed_issues(vec![
            phase_issue(
                "beads-1.1",
                "Auth phase",
                "Phase description",
                StatusWire::Open,
                "2026-01-01T00:01:00Z",
            ),
            plan_issue(
                "beads-1",
                "Auth closed plan",
                "Closed description",
                StatusWire::Closed,
                "2026-01-01T00:02:00Z",
            ),
            epic,
        ]);

        let outcome = execute_search(
            &store.beads_dir,
            &[
                "search", "auth", "--status", "open", "--type", "plan",
                "--tier", "epic", "--limit", "1", "--color", "never",
            ],
        );

        assert_eq!(outcome.exit_code, 0);
        assert_eq!(
            outcome.stdout,
            "○ beads-2 · Auth epic\n  Plan description\n"
        );
    }

    #[test]
    fn search_design_matches_canonical_plan_reference() {
        let mut epic = plan_issue(
            "beads-1",
            "Linked epic",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        );
        epic.design = "plans:202607/roadmap.md".to_string();
        let store = seed_issues(vec![epic]);

        let outcome = execute_search(
            &store.beads_dir,
            &["search", "202607", "--color", "never"],
        );

        assert_eq!(outcome.exit_code, 0);
        assert_eq!(
            outcome.stdout,
            "○ beads-1 · Linked epic\n  design: \"plans:202607/roadmap.md\"\n"
        );

        let old_prefix = execute_search(
            &store.beads_dir,
            &["search", "sdd/plans", "--color", "never"],
        );
        assert_eq!(old_prefix.stdout, "No beads match \"sdd/plans\".\n");
    }

    #[test]
    fn search_no_match_is_successful() {
        let store = seed_issues(vec![phase_issue(
            "beads-1.1",
            "Auth phase",
            "Phase description",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        )]);

        let outcome = execute_search(
            &store.beads_dir,
            &["search", "missing", "--format", "full"],
        );

        assert_eq!(outcome.exit_code, 0);
        assert_eq!(outcome.stdout, "No beads match \"missing\".\n");
    }

    #[test]
    fn search_whitespace_query_is_usage_error() {
        let store = seed_issues(Vec::new());

        let outcome = execute_search(&store.beads_dir, &["search", "   "]);

        assert_eq!(outcome.exit_code, 2);
        assert_eq!(outcome.stderr, "Error: search query cannot be empty\n");
    }

    #[test]
    fn create_and_remove_are_handled_with_mutation_summaries() {
        let store = seed_issues(Vec::new());
        let plan_path = store.beads_dir.parent().unwrap().join("plan.md");
        fs::write(&plan_path, "# Plan\n").unwrap();
        let create_args = vec![
            "create".to_string(),
            "--title".to_string(),
            "Fast plan".to_string(),
            "--type".to_string(),
            format!("plan({})", plan_path.display()),
            "--tier".to_string(),
            "epic".to_string(),
            "--model".to_string(),
            "codex/test".to_string(),
        ];

        let created = execute_bead_cli(
            &create_args,
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            store.beads_dir.parent().unwrap(),
            false,
            &[],
        )
        .unwrap();

        assert!(created.handled);
        assert_eq!(created.exit_code, 0);
        assert!(created
            .stdout
            .starts_with("Created plan: beads-1 — Fast plan"));
        let summary = created.mutation_summary.unwrap();
        assert_eq!(summary.operation, "create");
        assert_eq!(summary.issue_ids, vec!["beads-1"]);
        let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
        assert_eq!(issue.design, "sdd/plan.md");
        assert_eq!(issue.tier, Some(BeadTierWire::Epic));
        assert_eq!(issue.model, "codex/test");

        let removed = execute_bead_cli(
            &["rm".to_string(), "beads-1".to_string()],
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            store.beads_dir.parent().unwrap(),
            false,
            &[],
        )
        .unwrap();
        assert_eq!(removed.exit_code, 0);
        assert_eq!(removed.mutation_summary.unwrap().operation, "rm");
        assert!(read_store_issues(&store.beads_dir).unwrap().is_empty());
    }

    #[test]
    fn remove_handles_multiple_ids_with_unique_output_and_requested_summary() {
        let mut child = phase_issue(
            "beads-1.1",
            "Child",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        );
        child.parent_id = Some("beads-1".to_string());
        let store = seed_issues(vec![
            plan_issue(
                "beads-1",
                "Plan",
                "",
                StatusWire::Open,
                "2026-01-01T00:00:00Z",
            ),
            child,
            plan_issue(
                "beads-2",
                "Independent",
                "",
                StatusWire::Open,
                "2026-01-01T00:02:00Z",
            ),
        ]);
        let args = vec![
            "rm".to_string(),
            "beads-1".to_string(),
            "beads-1.1".to_string(),
            "beads-2".to_string(),
            "beads-2".to_string(),
        ];

        let outcome = execute_bead_cli(
            &args,
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            Path::new("/repo"),
            false,
            &[],
        )
        .unwrap();

        assert_eq!(outcome.exit_code, 0);
        assert_eq!(
            outcome.stdout,
            concat!(
                "✗ Removed: beads-1.1 — Child\n",
                "✗ Removed: beads-1 — Plan\n",
                "✗ Removed: beads-2 — Independent\n",
            )
        );
        let summary = outcome.mutation_summary.unwrap();
        assert_eq!(summary.operation, "rm");
        assert_eq!(summary.issue_ids, args[1..]);
        assert!(read_store_issues(&store.beads_dir).unwrap().is_empty());
    }

    #[test]
    fn remove_missing_later_id_is_an_atomic_fast_path_error() {
        let store = seed_issues(vec![
            plan_issue(
                "beads-1",
                "First",
                "",
                StatusWire::Open,
                "2026-01-01T00:00:00Z",
            ),
            plan_issue(
                "beads-2",
                "Second",
                "",
                StatusWire::Open,
                "2026-01-01T00:01:00Z",
            ),
        ]);
        let projection_before =
            fs::read(store.beads_dir.join("issues.jsonl")).unwrap();

        let outcome = execute_bead_cli(
            &[
                "rm".to_string(),
                "beads-1".to_string(),
                "beads-missing".to_string(),
            ],
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            Path::new("/repo"),
            false,
            &[],
        )
        .unwrap();

        assert_eq!(outcome.exit_code, 1);
        assert_eq!(outcome.stderr, "Error: issue not found: beads-missing\n");
        assert!(outcome.stdout.is_empty());
        assert!(outcome.mutation_summary.is_none());
        assert_eq!(
            fs::read(store.beads_dir.join("issues.jsonl")).unwrap(),
            projection_before
        );
        assert_eq!(
            read_store_issues(&store.beads_dir)
                .unwrap()
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["beads-1", "beads-2"]
        );
    }

    #[test]
    fn close_summary_preserves_requested_ids_and_real_prior_status() {
        let store = seed_issues(vec![phase_issue(
            "beads-1.1",
            "Active phase",
            "",
            StatusWire::InProgress,
            "2026-01-01T00:01:00Z",
        )]);
        let outcome = execute_bead_cli(
            &["close".to_string(), "beads-1.1".to_string()],
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            Path::new("/repo"),
            false,
            &[],
        )
        .unwrap();
        let summary = outcome.mutation_summary.unwrap();
        assert_eq!(summary.issue_ids, vec!["beads-1.1"]);
        assert_eq!(summary.status_transitions.len(), 1);
        assert_eq!(summary.status_transitions[0].from_status, "in_progress");
        assert_eq!(summary.status_transitions[0].to_status, "closed");
    }

    #[test]
    fn close_parser_accepts_force_with_reason_and_resolution() {
        let (ids, force, reason, resolution) = parse_close_args(&[
            "beads-1".to_string(),
            "--force".to_string(),
            "--reason".to_string(),
            "Requirements changed".to_string(),
            "--resolution=canceled".to_string(),
        ])
        .unwrap();

        assert_eq!(ids, vec!["beads-1"]);
        assert!(force);
        assert_eq!(reason.as_deref(), Some("Requirements changed"));
        assert_eq!(resolution, Some(BeadResolutionWire::Canceled));
    }

    #[test]
    fn create_plan_path_is_relative_to_store_workspace_from_nested_cwd() {
        let store = seed_issues(Vec::new());
        let workspace = store.beads_dir.ancestors().nth(2).unwrap();
        let nested = workspace.join("src/pkg");
        let plan_path = workspace.join("plans/plan.md");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
        fs::write(&plan_path, "# Plan\n").unwrap();
        let outcome = execute_bead_cli(
            &[
                "create".to_string(),
                "--title".to_string(),
                "Nested plan".to_string(),
                "--type".to_string(),
                format!("plan({})", plan_path.display()),
            ],
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            &nested,
            true,
            &[],
        )
        .unwrap();
        assert_eq!(outcome.exit_code, 0);
        let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
        assert_eq!(issue.design, "plans/plan.md");
    }

    #[test]
    fn create_plan_under_in_tree_plans_root_stores_canonical_reference() {
        let store = seed_issues(Vec::new());
        let workspace = store.beads_dir.ancestors().nth(2).unwrap();
        let plan_path = workspace.join("sdd/plans/202607/roadmap.md");
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
        fs::write(&plan_path, "# Roadmap\n").unwrap();

        let outcome = execute_bead_cli(
            &[
                "create".to_string(),
                "--title".to_string(),
                "Canonical plan".to_string(),
                "--type".to_string(),
                format!("plan({})", plan_path.display()),
            ],
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            workspace,
            true,
            &[],
        )
        .unwrap();

        assert_eq!(outcome.exit_code, 0);
        let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
        assert_eq!(issue.design, "plans:202607/roadmap.md");
    }

    #[test]
    fn create_plan_under_sidecar_plans_root_stores_canonical_reference() {
        let temp = tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let beads_dir = workspace.join("sase/repos/plans/beads");
        let store = seed_issues_at(temp, beads_dir, Vec::new());
        let plan_path = workspace.join("sase/repos/plans/202607/roadmap.md");
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
        fs::write(&plan_path, "# Roadmap\n").unwrap();

        let outcome = execute_bead_cli(
            &[
                "create".to_string(),
                "--title".to_string(),
                "Sidecar plan".to_string(),
                "--type".to_string(),
                format!("plan({})", plan_path.display()),
            ],
            std::slice::from_ref(&store.beads_dir),
            &store.beads_dir,
            &workspace,
            false,
            &[],
        )
        .unwrap();

        assert_eq!(outcome.exit_code, 0);
        let issue = read_store_issues(&store.beads_dir).unwrap().remove(0);
        assert_eq!(issue.design, "plans:202607/roadmap.md");
    }

    fn execute_search(beads_dir: &Path, args: &[&str]) -> BeadCliOutcomeWire {
        let argv = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
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

    fn seed_issues(issues: Vec<IssueWire>) -> SeededStore {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        seed_issues_at(temp, beads_dir, issues)
    }

    fn seed_issues_at(
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

    fn show_plan_section(
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

    fn seed_plan_root(temp: &TempDir, month: &str, name: &str) -> PathBuf {
        let month_dir = temp.path().join(month);
        fs::create_dir_all(&month_dir).unwrap();
        fs::write(month_dir.join(name), "# Plan\n").unwrap();
        temp.path().to_path_buf()
    }

    #[test]
    fn show_renders_reference_above_its_resolved_path() {
        let plans = tempdir().unwrap();
        let root = seed_plan_root(&plans, "202607", "durable.md");

        let plan = show_plan_section(
            "plans:202607/durable.md",
            std::slice::from_ref(&root),
            Path::new("/repo"),
            false,
        );

        assert_eq!(
            plan,
            format!(
                "  plans:202607/durable.md\n  → {}\n",
                root.join("202607/durable.md").display()
            )
        );
    }

    #[test]
    fn show_marks_a_reference_resolved_through_month_drift() {
        let plans = tempdir().unwrap();
        let root = seed_plan_root(&plans, "202607", "drifted.md");

        let plan = show_plan_section(
            "plans:202606/drifted.md",
            std::slice::from_ref(&root),
            Path::new("/repo"),
            false,
        );

        assert_eq!(
            plan,
            format!(
                "  plans:202606/drifted.md\n  → {} (month drift)\n",
                root.join("202607/drifted.md").display()
            )
        );
    }

    #[test]
    fn show_says_plainly_when_a_reference_resolves_nowhere() {
        let plans = tempdir().unwrap();
        let root = plans.path().to_path_buf();

        let plan = show_plan_section(
            "plans:202607/gone.md",
            std::slice::from_ref(&root),
            Path::new("/repo"),
            false,
        );

        assert_eq!(
            plan,
            concat!(
                "  plans:202607/gone.md\n",
                "  → (unresolved: no plan file found)\n",
            )
        );
    }

    #[test]
    fn show_reports_an_ambiguous_reference_instead_of_guessing() {
        let plans = tempdir().unwrap();
        seed_plan_root(&plans, "202606", "twin.md");
        let root = seed_plan_root(&plans, "202607", "twin.md");

        let plan = show_plan_section(
            "plans:202605/twin.md",
            std::slice::from_ref(&root),
            Path::new("/repo"),
            false,
        );

        assert_eq!(
            plan,
            concat!(
                "  plans:202605/twin.md\n",
                "  → (ambiguous: multiple plans match this reference)\n",
            )
        );
    }

    #[test]
    fn show_reports_a_malformed_reference() {
        let plan = show_plan_section(
            "plans:../escape.md",
            &[],
            Path::new("/repo"),
            false,
        );

        assert_eq!(
            plan,
            concat!(
                "  plans:../escape.md\n",
                "  → (unresolved: malformed plan reference)\n",
            )
        );
    }

    #[test]
    fn show_keeps_one_line_when_a_legacy_path_resolves_to_itself() {
        let workspace = tempdir().unwrap();
        let plan_path = workspace.path().join("plans/legacy.md");
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
        fs::write(&plan_path, "# Plan\n").unwrap();

        let plan =
            show_plan_section("plans/legacy.md", &[], workspace.path(), true);

        assert_eq!(plan, "  plans/legacy.md\n");
    }

    #[test]
    fn show_resolves_a_legacy_path_against_the_working_directory() {
        let workspace = tempdir().unwrap();
        let plan_path = workspace.path().join("plans/legacy.md");
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
        fs::write(&plan_path, "# Plan\n").unwrap();

        let plan =
            show_plan_section("plans/legacy.md", &[], workspace.path(), false);

        assert_eq!(
            plan,
            format!("  plans/legacy.md\n  → {}\n", plan_path.display())
        );
    }

    fn phase_issue(
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
            description: description.to_string(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            size: None,
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: Vec::new(),
        }
    }

    fn plan_issue(
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
}
