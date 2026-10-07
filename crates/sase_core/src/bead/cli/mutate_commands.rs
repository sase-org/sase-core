//! Mutating bead command handlers: open, update, close, dep, ref, rm.

use std::fmt::Write as _;
use std::path::Path;

use serde::Serialize;

use super::super::mutation::{
    add_bead_references, add_dependency, close_issues_with_note, open_issue,
    remove_bead_references, remove_dependencies, remove_issues, update_issues,
    BeadUpdateFieldsWire,
};
use super::super::read::read_store_issues;
use super::super::wire::{BeadError, IssueTypeWire, StatusWire};
use super::dispatch::{
    defer, error, mutation_summary, success, success_with_mutation,
    BeadCliMutationSummaryWire, BeadCliOutcomeWire,
    BeadCliStatusTransitionWire,
};
use super::parsing::{close_note_author, parse_close_args, parse_update_args};
use super::presentation::status_value;
use super::resolution::{
    find_issue, issue_ids_resolution_outcome, issue_resolution_outcome,
    resolve_cli_issue_id,
};

pub(super) fn handle_open(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.len() != 1 {
        return Ok(defer());
    }
    // The raw ID goes straight into the mutation: resolution and the
    // pre-mutation snapshot happen inside its single locked load, so this
    // command performs no store read of its own.
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
                mutation_summary("open", &outcome, outcome.old_issues.first()),
            ))
        }
        Err(err) if err.kind == "not_found" || err.kind == "ambiguous" => {
            Ok(issue_resolution_outcome(&args[0], err))
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

    // Raw IDs go straight into the mutation: resolution and the
    // pre-mutation snapshot happen inside its single locked load, so this
    // command performs no store read of its own.
    match update_issues(write_beads_dir, &raw_ids, fields) {
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
            for ancestor in &outcome.reopened_ancestors {
                writeln!(
                    stdout,
                    "○ Reopened ancestor: {} — {}",
                    ancestor.id, ancestor.title
                )
                .expect("writing to String cannot fail");
            }
            let status_transitions = outcome
                .issues
                .iter()
                .filter_map(|issue| {
                    let old = find_issue(&outcome.old_issues, &issue.id)?;
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
        Err(err) if err.kind == "not_found" || err.kind == "ambiguous" => {
            Ok(issue_ids_resolution_outcome(err))
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
    // Raw IDs go straight into the mutation: resolution and the
    // pre-mutation snapshot happen inside its single locked load, so this
    // command performs no store read of its own.
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
                    issue_ids: outcome.requested_issue_ids.clone(),
                    status_transitions: outcome
                        .issues
                        .iter()
                        .filter_map(|issue| {
                            let old =
                                find_issue(&outcome.old_issues, &issue.id)?;
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
        Err(err) if err.kind == "not_found" || err.kind == "ambiguous" => {
            Ok(issue_ids_resolution_outcome(err))
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
            // Raw IDs go straight into the mutation: resolution happens
            // inside its single locked load, so this command performs no
            // store read of its own.
            let outcome =
                match add_dependency(write_beads_dir, &args[1], &args[2], None)
                {
                    Ok(outcome) => outcome,
                    Err(err)
                        if err.kind == "not_found"
                            || err.kind == "ambiguous" =>
                    {
                        return Ok(issue_ids_resolution_outcome(err));
                    }
                    Err(err) => return Err(err),
                };
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
                    issue_ids: vec![
                        dep.issue_id.clone(),
                        dep.depends_on_id.clone(),
                    ],
                    status_transitions: Vec::new(),
                },
            ))
        }
        Some("rm") if args.len() >= 3 => {
            // Raw IDs go straight into the mutation: resolution happens
            // inside its single locked load, and the outcome carries the
            // post-mutation source plus its active blockers, so this
            // command performs no store read of its own.
            let outcome = match remove_dependencies(
                write_beads_dir,
                &args[1],
                &args[2..],
                None,
            ) {
                Ok(outcome) => outcome,
                Err(err)
                    if err.kind == "not_found" || err.kind == "ambiguous" =>
                {
                    return Ok(issue_ids_resolution_outcome(err));
                }
                Err(err) => return Err(err),
            };
            let issue_id = outcome
                .issue
                .as_ref()
                .map(|issue| issue.id.clone())
                .unwrap_or_else(|| args[1].clone());
            let mut stdout = String::new();
            for dependency in &outcome.dependencies {
                writeln!(
                    stdout,
                    "✗ Removed dependency: {} no longer depends on {}",
                    dependency.issue_id, dependency.depends_on_id
                )
                .expect("writing to String cannot fail");
            }
            let active_blockers = outcome.active_blocker_ids.clone();
            let source_is_ready = outcome.issue.as_ref().is_some_and(|issue| {
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
            // The raw ID goes straight into the mutation: resolution
            // happens inside its single locked load, so this command
            // performs no store read of its own.
            match add_bead_references(
                write_beads_dir,
                &action_args[0],
                &action_args[1..],
                None,
            ) {
                Ok(outcome) => {
                    let issue_id = outcome
                        .issue_ids
                        .first()
                        .cloned()
                        .unwrap_or_else(|| action_args[0].clone());
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
                            issue_ids: vec![issue_id],
                            status_transitions: Vec::new(),
                        },
                    ))
                }
                Err(err)
                    if err.kind == "not_found" || err.kind == "ambiguous" =>
                {
                    Ok(issue_ids_resolution_outcome(err))
                }
                Err(err) if err.kind == "validation" => {
                    Ok(error(format!("Error: {}\n", err.message)))
                }
                Err(err) => Err(err),
            }
        }
        "rm" if action_args.len() >= 2 => {
            // The raw ID goes straight into the mutation: resolution
            // happens inside its single locked load, so this command
            // performs no store read of its own.
            match remove_bead_references(
                write_beads_dir,
                &action_args[0],
                &action_args[1..],
                None,
            ) {
                Ok(outcome) => {
                    let issue_id = outcome
                        .issue_ids
                        .first()
                        .cloned()
                        .unwrap_or_else(|| action_args[0].clone());
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
                            issue_ids: vec![issue_id],
                            status_transitions: Vec::new(),
                        },
                    ))
                }
                Err(err)
                    if err.kind == "not_found" || err.kind == "ambiguous" =>
                {
                    Ok(issue_ids_resolution_outcome(err))
                }
                Err(err) if err.kind == "validation" => {
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

pub(super) fn handle_rm(
    args: &[String],
    write_beads_dir: &Path,
) -> Result<BeadCliOutcomeWire, BeadError> {
    if args.is_empty() {
        return Ok(defer());
    }
    // Raw IDs go straight into the mutation: resolution happens inside its
    // single locked load, so this command performs no store read of its own.
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
                    changed: outcome.changed,
                    issue_ids: outcome.requested_issue_ids.clone(),
                    status_transitions: Vec::new(),
                },
            ))
        }
        Err(err) if err.kind == "not_found" || err.kind == "ambiguous" => {
            Ok(issue_ids_resolution_outcome(err))
        }
        Err(err) => Err(err),
    }
}
