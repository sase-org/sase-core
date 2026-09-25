//! Mutating bead command handlers: open, update, close, dep, ref, rm.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use serde::Serialize;

use super::super::mutation::{
    add_bead_references, add_dependency, close_issues_with_note, open_issue,
    remove_bead_references, remove_dependencies, remove_issues, update_issues,
    BeadUpdateFieldsWire,
};
use super::super::read::read_store_issues;
use super::super::wire::{BeadError, IssueTypeWire, IssueWire, StatusWire};
use super::dispatch::{
    defer, error, mutation_summary, success, success_with_mutation,
    BeadCliMutationSummaryWire, BeadCliOutcomeWire,
    BeadCliStatusTransitionWire,
};
use super::parsing::{close_note_author, parse_close_args, parse_update_args};
use super::presentation::status_value;
use super::resolution::{
    find_issue, issue_ids_resolution_outcome, issue_resolution_outcome,
    resolve_cli_issue_id, resolve_cli_issue_ids,
};

pub(super) fn handle_open(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.len() != 1 {
        return Ok(defer());
    }
    let issues = read_store_issues(write_beads_dir).unwrap_or_default();
    let issue_id = match resolve_cli_issue_id(&issues, &args[0]) {
        Ok(issue_id) => issue_id,
        Err(err) => return Ok(issue_resolution_outcome(&args[0], err)),
    };
    let old = find_issue(&issues, &issue_id).cloned();
    match open_issue(write_beads_dir, &issue_id, None) {
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

pub(super) fn handle_update(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.is_empty() {
        return Ok(defer());
    }
    let Some((raw_ids, fields)) = parse_update_args(args) else {
        return Ok(defer());
    };
    if raw_ids.is_empty() {
        return Ok(defer());
    }
    if fields == BeadUpdateFieldsWire::default() {
        return Ok(error("No fields to update.\n".to_string()));
    }

    let issues = read_store_issues(write_beads_dir).unwrap_or_default();
    let issue_ids = match raw_ids
        .iter()
        .map(|issue_id| resolve_cli_issue_id(&issues, issue_id))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(ids) => ids,
        Err(err) => return Ok(issue_ids_resolution_outcome(err)),
    };

    match update_issues(write_beads_dir, &issue_ids, fields) {
        Ok(outcome) => {
            let changed: std::collections::HashSet<&str> =
                outcome.issue_ids.iter().map(String::as_str).collect();
            let mut stdout = String::new();
            for issue in &outcome.issues {
                if changed.contains(issue.id.as_str()) {
                    writeln!(
                        stdout,
                        "✓ Updated issue: {} — {}",
                        issue.id, issue.title
                    )
                    .expect("writing to String cannot fail");
                } else {
                    writeln!(
                        stdout,
                        "· Unchanged: {} — {}",
                        issue.id, issue.title
                    )
                    .expect("writing to String cannot fail");
                }
            }
            for ancestor_id in &outcome.reopened_ancestor_ids {
                if let Some(ancestor) = find_issue(&issues, ancestor_id) {
                    writeln!(
                        stdout,
                        "○ Reopened ancestor: {} — {}",
                        ancestor.id, ancestor.title
                    )
                    .expect("writing to String cannot fail");
                }
            }
            let status_transitions = outcome
                .issues
                .iter()
                .filter_map(|issue| {
                    let old = find_issue(&issues, &issue.id)?;
                    (old.status != issue.status).then(|| {
                        BeadCliStatusTransitionWire {
                            from_status: status_value(&old.status).to_string(),
                            to_status: status_value(&issue.status).to_string(),
                        }
                    })
                })
                .collect();
            Ok(success_with_mutation(
                stdout,
                BeadCliMutationSummaryWire {
                    operation: "update".to_string(),
                    changed: outcome.changed,
                    issue_ids: outcome.issue_ids.clone(),
                    status_transitions,
                },
            ))
        }
        Err(err) if err.kind == "not_found" => {
            Ok(error(format!("Error: {}\n", err.message)))
        }
        Err(err) => Err(err),
    }
}

pub(super) fn handle_close(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let Some((ids, force, note, reason, resolution)) = parse_close_args(args)
    else {
        return Ok(defer());
    };
    if ids.is_empty() {
        return Ok(defer());
    }
    let old_issues = read_store_issues(write_beads_dir).unwrap_or_default();
    let ids = match ids
        .iter()
        .map(|issue_id| resolve_cli_issue_id(&old_issues, issue_id))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(ids) => ids,
        Err(err) => return Ok(issue_ids_resolution_outcome(err)),
    };
    // The close actor is always resolved, with or without `--note`: it
    // stamps the note author and every `issue_closed` event in the batch.
    let close_actor = close_note_author();
    match close_issues_with_note(
        write_beads_dir,
        &ids,
        reason,
        resolution,
        force,
        note,
        close_actor,
        None,
    ) {
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
                    changed: outcome.changed,
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

pub(super) fn handle_dep(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.iter().any(|arg| arg.starts_with('-')) {
        return Ok(defer());
    }
    match args.first().map(String::as_str) {
        Some("add") if args.len() == 3 => {
            let issue_ids =
                match resolve_cli_issue_ids(write_beads_dir, &args[1..]) {
                    Ok(issue_ids) => issue_ids,
                    Err(err) => return Ok(issue_ids_resolution_outcome(err)),
                };
            let issue_id = &issue_ids[0];
            let depends_on_id = &issue_ids[1];
            let outcome =
                add_dependency(write_beads_dir, issue_id, depends_on_id, None)?;
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
                    changed: outcome.changed,
                    issue_ids: vec![issue_id.clone(), depends_on_id.clone()],
                    status_transitions: Vec::new(),
                },
            ))
        }
        Some("rm") if args.len() >= 3 => {
            let issue_ids =
                match resolve_cli_issue_ids(write_beads_dir, &args[1..]) {
                    Ok(issue_ids) => issue_ids,
                    Err(err) => return Ok(issue_ids_resolution_outcome(err)),
                };
            let issue_id = &issue_ids[0];
            let outcome = remove_dependencies(
                write_beads_dir,
                issue_id,
                &issue_ids[1..],
                None,
            )?;
            let mut stdout = String::new();
            for dependency in &outcome.dependencies {
                writeln!(
                    stdout,
                    "✗ Removed dependency: {} no longer depends on {}",
                    dependency.issue_id, dependency.depends_on_id
                )
                .expect("writing to String cannot fail");
            }
            let issues = read_store_issues(write_beads_dir)?;
            let active_blockers =
                active_blocker_ids(&issues, issue_id.as_str());
            let source_is_ready = issues
                .iter()
                .find(|issue| issue.id == *issue_id)
                .is_some_and(|issue| {
                    issue.status == StatusWire::Ready
                        && issue.issue_type == IssueTypeWire::Task
                        && active_blockers.is_empty()
                });
            if source_is_ready {
                writeln!(
                    stdout,
                    "○ {issue_id} is now ready (no active blockers)."
                )
                .expect("writing to String cannot fail");
            } else {
                if active_blockers.is_empty() {
                    writeln!(stdout, "○ {issue_id} has no active blockers.")
                        .expect("writing to String cannot fail");
                } else {
                    writeln!(
                        stdout,
                        "○ {issue_id} still has {} active blocker{}: {}.",
                        active_blockers.len(),
                        if active_blockers.len() == 1 { "" } else { "s" },
                        active_blockers.join(", ")
                    )
                    .expect("writing to String cannot fail");
                }
            }
            Ok(success_with_mutation(
                stdout,
                BeadCliMutationSummaryWire {
                    operation: "dep_rm".to_string(),
                    changed: outcome.changed,
                    issue_ids: outcome.issue_ids,
                    status_transitions: Vec::new(),
                },
            ))
        }
        _ => Ok(defer()),
    }
}

pub(super) fn handle_ref(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let (action, action_args) = match args.first().map(String::as_str) {
        None => ("list", &[][..]),
        Some("add" | "list" | "rm") => (args[0].as_str(), &args[1..]),
        _ => return Ok(defer()),
    };
    match action {
        "add" if action_args.len() >= 2 => {
            let issue_id = match resolve_cli_issue_ids(
                write_beads_dir,
                &[action_args[0].clone()],
            ) {
                Ok(mut issue_ids) => issue_ids.remove(0),
                Err(err) => return Ok(issue_ids_resolution_outcome(err)),
            };
            match add_bead_references(
                write_beads_dir,
                &issue_id,
                &action_args[1..],
                None,
            ) {
                Ok(outcome) => {
                    let mut stdout = String::new();
                    for reference in &outcome.references {
                        writeln!(
                            stdout,
                            "✓ Added reference to {issue_id}: {reference}"
                        )
                        .expect("writing to String cannot fail");
                    }
                    if !outcome.changed {
                        stdout.push_str("No artifact references changed.\n");
                    }
                    Ok(success_with_mutation(
                        stdout,
                        BeadCliMutationSummaryWire {
                            operation: "ref_add".to_string(),
                            changed: outcome.changed,
                            issue_ids: vec![issue_id.clone()],
                            status_transitions: Vec::new(),
                        },
                    ))
                }
                Err(err)
                    if matches!(
                        err.kind.as_str(),
                        "not_found" | "validation"
                    ) =>
                {
                    Ok(error(format!("Error: {}\n", err.message)))
                }
                Err(err) => Err(err),
            }
        }
        "rm" if action_args.len() >= 2 => {
            let issue_id = match resolve_cli_issue_ids(
                write_beads_dir,
                &[action_args[0].clone()],
            ) {
                Ok(mut issue_ids) => issue_ids.remove(0),
                Err(err) => return Ok(issue_ids_resolution_outcome(err)),
            };
            match remove_bead_references(
                write_beads_dir,
                &issue_id,
                &action_args[1..],
                None,
            ) {
                Ok(outcome) => {
                    let mut stdout = String::new();
                    for reference in &outcome.references {
                        writeln!(
                            stdout,
                            "✗ Removed reference from {issue_id}: {reference}"
                        )
                        .expect("writing to String cannot fail");
                    }
                    if !outcome.changed {
                        stdout.push_str("No artifact references changed.\n");
                    }
                    Ok(success_with_mutation(
                        stdout,
                        BeadCliMutationSummaryWire {
                            operation: "ref_rm".to_string(),
                            changed: outcome.changed,
                            issue_ids: vec![issue_id.clone()],
                            status_transitions: Vec::new(),
                        },
                    ))
                }
                Err(err)
                    if matches!(
                        err.kind.as_str(),
                        "not_found" | "validation"
                    ) =>
                {
                    Ok(error(format!("Error: {}\n", err.message)))
                }
                Err(err) => Err(err),
            }
        }
        "list" => handle_ref_list(action_args, write_beads_dir),
        _ => Ok(defer()),
    }
}

pub(super) fn handle_ref_list(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    let mut issue_id = None;
    let mut json = false;
    let mut resolve = false;
    for arg in args {
        match arg.as_str() {
            "-j" | "--json" => json = true,
            "-r" | "--resolve" => resolve = true,
            _ if !arg.starts_with('-') && issue_id.is_none() => {
                issue_id = Some(arg.as_str());
            }
            _ => return Ok(defer()),
        }
    }
    if resolve {
        return Ok(defer());
    }

    let issues = read_store_issues(write_beads_dir)?;
    let selected = if let Some(issue_id) = issue_id {
        let issue_id = match resolve_cli_issue_id(&issues, issue_id) {
            Ok(issue_id) => issue_id,
            Err(err) => return Ok(issue_resolution_outcome(issue_id, err)),
        };
        let Some(issue) = find_issue(&issues, &issue_id) else {
            return Ok(error(format!("Error: issue not found: {issue_id}\n")));
        };
        vec![issue]
    } else {
        issues
            .iter()
            .filter(|issue| !issue.refs.is_empty())
            .collect()
    };

    if json {
        #[derive(Serialize)]
        struct ReferenceListEntry<'a> {
            issue_id: &'a str,
            refs: &'a [String],
        }
        #[derive(Serialize)]
        struct ReferenceListEnvelope<'a> {
            count: usize,
            results: Vec<ReferenceListEntry<'a>>,
        }
        let count = selected.iter().map(|issue| issue.refs.len()).sum();
        let mut stdout =
            serde_json::to_string_pretty(&ReferenceListEnvelope {
                count,
                results: selected
                    .iter()
                    .map(|issue| ReferenceListEntry {
                        issue_id: &issue.id,
                        refs: &issue.refs,
                    })
                    .collect(),
            })?;
        stdout.push('\n');
        return Ok(success(stdout));
    }

    let mut stdout = String::new();
    for issue in selected {
        for reference in &issue.refs {
            if issue_id.is_some() {
                writeln!(stdout, "{reference}")
            } else {
                writeln!(stdout, "{}  {reference}", issue.id)
            }
            .expect("writing to String cannot fail");
        }
    }
    if stdout.is_empty() {
        stdout.push_str("No artifact references found.\n");
    }
    Ok(success(stdout))
}

pub(super) fn active_blocker_ids(
    issues: &[IssueWire],
    issue_id: &str,
) -> Vec<String> {
    let status_by_id: BTreeMap<&str, &StatusWire> = issues
        .iter()
        .map(|issue| (issue.id.as_str(), &issue.status))
        .collect();
    issues
        .iter()
        .find(|issue| issue.id == issue_id)
        .into_iter()
        .flat_map(|issue| &issue.dependencies)
        .filter(|dependency| {
            status_by_id
                .get(dependency.depends_on_id.as_str())
                .is_some_and(|status| {
                    matches!(
                        status,
                        StatusWire::Open
                            | StatusWire::Claimed
                            | StatusWire::Ready
                            | StatusWire::Snoozed
                            | StatusWire::InProgress
                    )
                })
        })
        .map(|dependency| dependency.depends_on_id.clone())
        .collect()
}

pub(super) fn handle_rm(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.is_empty() {
        return Ok(defer());
    }
    let issue_ids = match resolve_cli_issue_ids(write_beads_dir, args) {
        Ok(issue_ids) => issue_ids,
        Err(err) => return Ok(issue_ids_resolution_outcome(err)),
    };
    match remove_issues(write_beads_dir, &issue_ids) {
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
                    changed: outcome.changed,
                    issue_ids,
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
