//! Narrow `sase bead` CLI execution planner.
//!
//! Python still owns workspace discovery, help text, and host-coupled
//! commands. This module handles the common bead commands once Python has
//! resolved the store paths.

use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::mutation::{
    add_dependency, close_issues, open_issue, update_issue,
    BeadMutationOutcomeWire, BeadUpdateFieldsWire,
};
use super::read::read_store_issues;
use super::search::search_issues_in_issues;
use super::wire::{
    BeadError, BeadSearchMatchWire, BeadTierWire, DependencyWire,
    IssueTypeWire, IssueWire, StatusWire,
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
        "search" => handle_search(
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
    if !issue.model.is_empty() {
        writeln!(stdout, "Model: {}", issue.model)
            .expect("writing to String cannot fail");
    }
    if let Some(epic_count) = issue.epic_count {
        writeln!(stdout, "Epic Count: {epic_count}")
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

fn handle_search(
    args: &[String],
    read_beads_dirs: &[PathBuf],
    write_beads_dir: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
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
        } else if let Some(value) = arg.strip_prefix("--tier=") {
            tiers.push(parse_tier(value)?);
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
        } else if let Some(value) = arg.strip_prefix("--epic-count=") {
            ("--epic-count", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--tier=") {
            ("--tier", value.to_string())
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
            "-m" | "--model" => fields.model = Some(value),
            "-a" | "--assignee" => fields.assignee = Some(value),
            "-E" | "--epic-count" => {
                fields.epic_count = Some(parse_positive_i64(&value)?);
            }
            "--tier" => fields.tier = Some(parse_tier(&value)?),
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
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_CYAN: &str = "\x1b[36m";

fn color_status_icon(status: &StatusWire, color: bool) -> String {
    let icon = status_icon(status);
    if !color {
        return icon.to_string();
    }
    let code = match status {
        StatusWire::Open => ANSI_CYAN,
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

fn parse_tier(value: &str) -> Option<BeadTierWire> {
    match value {
        "plan" => Some(BeadTierWire::Plan),
        "epic" => Some(BeadTierWire::Epic),
        "legend" => Some(BeadTierWire::Legend),
        _ => None,
    }
}

fn parse_positive_i64(value: &str) -> Option<i64> {
    let parsed = value.parse::<i64>().ok()?;
    (parsed > 0).then_some(parsed)
}

fn is_ready_surface_issue(issue: &IssueWire) -> bool {
    issue.issue_type == IssueTypeWire::Phase
        || issue.tier == Some(BeadTierWire::Epic)
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
        BeadTierWire::Legend => "legend",
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

    fn execute_search(beads_dir: &Path, args: &[&str]) -> BeadCliOutcomeWire {
        let argv = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        execute_bead_cli(
            &argv,
            &[beads_dir.to_path_buf()],
            beads_dir,
            Path::new("/repo"),
            false,
        )
        .unwrap()
    }

    fn seed_issues(issues: Vec<IssueWire>) -> SeededStore {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
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
            description: description.to_string(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            is_ready_to_work: false,
            epic_count: None,
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
