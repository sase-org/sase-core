//! Read-only bead command handlers: list, show, search, ready, blocked,
//! stats, with their blocking/statistics helpers.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{DateTime, NaiveDate, Utc};

use super::super::search::{
    search_issues_in_issues_with_matcher, SearchMatcher,
};
use super::super::wire::{
    notes_text, BeadError, BeadResolutionWire, BeadSearchMatchWire,
    IssueTypeWire, IssueWire, StatusWire,
};
use super::design_refs::display_design_path;
use super::dispatch::{defer, error, success, usage_error, BeadCliOutcomeWire};
use super::parsing::{
    optional_filter, parse_list_filters, parse_search_args, SearchFormat,
    SearchParseOutcome,
};
use super::presentation::{
    color_issue_id, color_issue_type_cell, color_status_icon,
    compact_type_width, issue_tier_suffix, issue_type_value, status_icon,
    status_upper, status_value,
};
use super::rendering::{
    render_dependency, render_search_compact, render_search_json,
};
use super::resolution::{
    find_issue, issue_resolution_outcome, read_issues, resolve_cli_issue_id,
};

pub(super) fn handle_list(
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
                .is_none_or(|types| types.contains(&issue.issue_type))
            && filters.tiers.as_ref().is_none_or(|tiers| {
                issue.tier.as_ref().is_some_and(|tier| tiers.contains(tier))
            })
    });
    sort_by_created_at(&mut issues);

    let mut stdout = String::new();
    if issues.is_empty() {
        stdout.push_str("No issues found.\n");
    } else {
        let color = filters.color.resolve_stdout();
        let type_width = compact_type_width();
        for issue in &issues {
            let parent = issue
                .parent_id
                .as_ref()
                .map_or(String::new(), |parent_id| format!(" ← {parent_id}"));
            writeln!(
                stdout,
                "{} {} {} · {}{}",
                color_issue_type_cell(&issue.issue_type, color, type_width),
                color_status_icon(&issue.status, color),
                color_issue_id(&issue.id, color),
                issue.title,
                parent
            )
            .expect("writing to String cannot fail");
        }
    }
    Ok(success(stdout))
}

pub(super) fn handle_show(
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
    let issue_id = match resolve_cli_issue_id(&issues, &args[0]) {
        Ok(issue_id) => issue_id,
        Err(err) => return Ok(issue_resolution_outcome(&args[0], err)),
    };
    let Some(issue) = find_issue(&issues, &issue_id) else {
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
    let note_text = notes_text(&issue.notes);
    if !note_text.is_empty() {
        write!(stdout, "\nNOTES\n  {}\n", note_text)
            .expect("writing to String cannot fail");
    }
    if issue.issue_type == IssueTypeWire::Plan
        && (!issue.changespec_name.is_empty()
            || !issue.changespec_bug_id.is_empty())
    {
        stdout.push_str("\nPATCH\n");
        if !issue.changespec_name.is_empty() {
            writeln!(stdout, "  Name: {}", issue.changespec_name)
                .expect("writing to String cannot fail");
        }
        if !issue.changespec_bug_id.is_empty() {
            writeln!(stdout, "  Bug ID: {}", issue.changespec_bug_id)
                .expect("writing to String cannot fail");
        }
    }
    if !issue.external_ref.is_empty() {
        writeln!(stdout, "\nEXTERNAL\n  Ref: {}", issue.external_ref)
            .expect("writing to String cannot fail");
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
    if !issue.refs.is_empty() {
        stdout.push_str("\nREFS\n");
        for reference in &issue.refs {
            writeln!(stdout, "  {reference}")
                .expect("writing to String cannot fail");
        }
    }

    Ok(success(stdout))
}

pub(super) fn handle_search(
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
    let matcher =
        match SearchMatcher::new(&search_args.query, search_args.regex) {
            Ok(matcher) => matcher,
            Err(err) if err.kind == "validation" => {
                return Ok(usage_error(format!("Error: {}\n", err.message)));
            }
            Err(err) => return Err(err),
        };
    let issues = read_issues(read_beads_dirs, write_beads_dir)?;
    let matches = match search_issues_in_issues_with_matcher(
        issues,
        &matcher,
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
            render_search_compact(&matches, &matcher, color)
        }
        SearchFormat::Json => render_search_json(&matches, &matcher)?,
        SearchFormat::Full => render_search_full(
            &matches,
            matcher.query(),
            read_beads_dirs,
            write_beads_dir,
            cwd,
            relativize_design_paths,
            plan_roots,
        )?,
    };

    Ok(success(stdout))
}

pub(super) fn handle_ready(
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
        issue.status == StatusWire::Ready
            && issue.issue_type == IssueTypeWire::Task
            && !has_active_blocker(issue, &status_by_id)
    });
    sort_by_created_at(&mut issues);

    let mut stdout = String::new();
    if issues.is_empty() {
        stdout.push_str(
            "No ready task beads (epic work is preassigned at launch).\n",
        );
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
        write!(
            stdout,
            "\n{}\nReady: {} task bead{} with no active blockers\n",
            "-".repeat(60),
            issues.len(),
            if issues.len() == 1 { "" } else { "s" }
        )
        .expect("writing to String cannot fail");
    }
    Ok(success(stdout))
}

pub(super) fn handle_blocked(
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

pub(super) fn handle_stats(
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
        "Issue Statistics\n  Total:       {}\n  Open:        {}\n  Claimed:     {}\n  Ready:       {}\n  In Progress: {}\n  Closed:      {}\n  Plans:       {}\n  Phases:      {}\n  Tasks:       {}\n  Flags:       {}\n  Due Flags:   {}\n",
        stats.get("total").copied().unwrap_or(0),
        stats.get("open").copied().unwrap_or(0),
        stats.get("claimed").copied().unwrap_or(0),
        stats.get("ready").copied().unwrap_or(0),
        stats.get("in_progress").copied().unwrap_or(0),
        stats.get("closed").copied().unwrap_or(0),
        stats.get("plan").copied().unwrap_or(0),
        stats.get("phase").copied().unwrap_or(0),
        stats.get("task").copied().unwrap_or(0),
        stats.get("flag").copied().unwrap_or(0),
        stats.get("due_flag").copied().unwrap_or(0),
    );
    Ok(success(stdout))
}

pub(super) fn blocking_issue_ids(
    issues: &[IssueWire],
    issue_id: &str,
) -> Vec<String> {
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

pub(super) fn has_active_blocker(
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
                        | StatusWire::Ready
                        | StatusWire::Snoozed
                        | StatusWire::InProgress
                )
            })
    })
}

pub(super) fn stats_for_issues(
    issues: &[IssueWire],
) -> BTreeMap<String, usize> {
    let mut stats = BTreeMap::new();
    let mut plus_one_total = 0;
    let today = current_date();
    let release = env!("CARGO_PKG_VERSION");
    for issue in issues {
        *stats
            .entry(status_value(&issue.status).to_string())
            .or_insert(0) += 1;
        *stats
            .entry(issue_type_value(&issue.issue_type).to_string())
            .or_insert(0) += 1;
        if issue.is_flag_task() {
            *stats.entry("flag".to_string()).or_insert(0) += 1;
        }
        if issue.flag_is_due(today, release) {
            *stats.entry("due_flag".to_string()).or_insert(0) += 1;
        }
        plus_one_total += issue.plus_one_count();
    }
    stats.insert("total".to_string(), issues.len());
    stats.insert("plus_one".to_string(), plus_one_total);
    stats
}

pub(super) fn current_date() -> NaiveDate {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.date_naive()
}

pub(super) fn sort_by_created_at(issues: &mut [IssueWire]) {
    issues
        .sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
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
