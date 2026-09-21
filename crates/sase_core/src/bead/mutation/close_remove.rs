use super::mutation_wire::BeadMutationOutcomeWire;
use super::notes_update::append_note_to_store;
use super::store::mutation_status_value;
use super::store::now_utc;
use super::store::outcome;
use super::store::sorted_descendants;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use std::collections::BTreeSet;
use std::path::Path;

pub fn open_issue(
    beads_dir: &Path,
    issue_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    with_bead_mutation_lock(beads_dir, "open", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let was_closed = store.issues[index].status == StatusWire::Closed;
        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::Open;
        archive_close_metadata(
            &mut store.issues[index],
            &now,
            BeadReopenCauseWire::Open,
            None,
        );
        clear_snooze_record(&mut store.issues[index]);
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            &now,
            &issue.created_by,
        )?;
        let reopened_ancestors = if was_closed {
            reopen_closed_ancestors(&mut store, issue_id, &now)?
        } else {
            Vec::new()
        };
        store.save()?;

        let mut result = outcome("open", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        result.reopened_ancestor_ids = reopened_ancestors
            .iter()
            .map(|ancestor| ancestor.id.clone())
            .collect();
        result.issues = reopened_ancestors;
        Ok(result)
    })
}

pub fn close_issues(
    beads_dir: &Path,
    issue_ids: &[String],
    reason: Option<String>,
    resolution: Option<BeadResolutionWire>,
    force: bool,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    close_issues_with_note(
        beads_dir, issue_ids, reason, resolution, force, None, None, now,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn close_issues_with_note(
    beads_dir: &Path,
    issue_ids: &[String],
    reason: Option<String>,
    resolution: Option<BeadResolutionWire>,
    force: bool,
    note: Option<String>,
    note_author: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let note = match note {
        None => None,
        Some(entry) => {
            let entry = entry.trim().to_string();
            if entry.is_empty() {
                return Err(BeadError::validation(
                    "note entry cannot be empty or blank",
                ));
            }
            Some((entry, note_author))
        }
    };

    with_bead_mutation_lock(beads_dir, "close", || {
        let mut store = MutableStore::load(beads_dir)?;
        let now = now.unwrap_or_else(now_utc);
        let effective_resolution =
            resolution.clone().unwrap_or(BeadResolutionWire::Done);
        if force {
            if reason
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(BeadError::validation(
                    "forced close requires a non-empty --reason",
                ));
            }
            if effective_resolution == BeadResolutionWire::Done {
                return Err(BeadError::validation(
                    "forced close requires --resolution canceled or superseded; 'done' is not allowed",
                ));
            }
        }
        let mut standard_close_ids = BTreeSet::new();
        let mut requested_ids = BTreeSet::new();
        let mut unresolved_by_request = Vec::new();
        let mut already_closed_ids = Vec::new();

        for issue_id in issue_ids {
            let issue = store.get_issue(issue_id)?;
            if !requested_ids.insert(issue.id.clone()) {
                continue;
            }
            if issue.status == StatusWire::Closed {
                reject_conflicting_close(
                    issue,
                    resolution.as_ref(),
                    reason.as_deref(),
                )?;
                already_closed_ids.push(issue.id.clone());
            }
            standard_close_ids.insert(issue.id.clone());
            let unresolved = unresolved_descendants(&store.issues, issue_id);
            if !force && !unresolved.is_empty() {
                return Err(unclosed_descendants_error(issue_id, &unresolved));
            }
            standard_close_ids.extend(
                sorted_descendants(&store.issues, issue_id)
                    .into_iter()
                    .map(|descendant| descendant.id.clone()),
            );
            unresolved_by_request.push((
                issue.id.clone(),
                unresolved
                    .into_iter()
                    .map(|descendant| descendant.id.clone())
                    .collect::<Vec<_>>(),
            ));
        }

        let mut noted_ids = Vec::new();
        if let Some((entry, requested_author)) = note.as_ref() {
            let author = requested_author
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&store.config.owner)
                .to_string();
            for issue_id in &requested_ids {
                let index = store.issue_index(issue_id)?;
                append_note_to_store(&mut store, index, entry, &author, &now)?;
                noted_ids.push(issue_id.clone());
            }
        }
        let mut batch = CloseBatch {
            standard_close_ids,
            ..Default::default()
        };

        for (issue_id, swept_ids) in unresolved_by_request {
            let mut forced_descendant_ids = Vec::new();
            if force {
                let forced_reason = Some(format!(
                    "forced by {issue_id}: {}",
                    reason.as_deref().expect("forced reason was validated")
                ));
                for child_id in &swept_ids {
                    if close_one_and_delegated_parent(
                        &mut store,
                        child_id,
                        &now,
                        forced_reason.clone(),
                        effective_resolution.clone(),
                        Vec::new(),
                        &mut batch,
                    )? {
                        forced_descendant_ids.push(child_id.clone());
                    }
                }
            }
            if !close_one_and_delegated_parent(
                &mut store,
                &issue_id,
                &now,
                reason.clone(),
                effective_resolution.clone(),
                forced_descendant_ids,
                &mut batch,
            )? {
                batch.returned.push(store.get_issue(&issue_id)?.clone());
            }
        }
        for event in &batch.event_closed {
            store.append_issue_event(
                &event.issue.id,
                BeadEventOperationWire::IssueClosed,
                BeadEventPayloadWire::IssueClosed {
                    close_reason: event.issue.close_reason.clone(),
                    resolution: event.issue.resolution.clone(),
                    forced_descendant_ids: event.forced_descendant_ids.clone(),
                },
                &now,
                &event.issue.created_by,
            )?;
        }

        let closed_ids = batch.closed_ids;
        let cascade_closed_ids = closed_ids
            .iter()
            .filter(|issue_id| !requested_ids.contains(*issue_id))
            .cloned()
            .collect::<Vec<_>>();
        let changed = !closed_ids.is_empty() || !noted_ids.is_empty();
        if changed {
            store.save()?;
        }
        let mut affected_ids = closed_ids.clone();
        for issue_id in &noted_ids {
            if !affected_ids.contains(issue_id) {
                affected_ids.push(issue_id.clone());
            }
        }
        let mut result = outcome("close", changed, affected_ids);
        if !changed {
            result.message =
                "all requested issues were already closed".to_string();
        }
        result.issues = batch.returned;
        result.closed_ids = closed_ids;
        result.already_closed_ids = already_closed_ids;
        result.noted_ids = noted_ids;
        result.cascade_closed_ids = cascade_closed_ids;
        Ok(result)
    })
}

fn reject_conflicting_close(
    issue: &IssueWire,
    requested_resolution: Option<&BeadResolutionWire>,
    requested_reason: Option<&str>,
) -> Result<(), BeadError> {
    let resolution_conflicts = requested_resolution
        .is_some_and(|requested| issue.resolution.as_ref() != Some(requested));
    let requested_reason =
        requested_reason.filter(|value| !value.trim().is_empty());
    let reason_conflicts = requested_reason.is_some_and(|requested| {
        issue.close_reason.as_deref() != Some(requested)
    });
    if !resolution_conflicts && !reason_conflicts {
        return Ok(());
    }

    let recorded_resolution = issue
        .resolution
        .as_ref()
        .map(BeadResolutionWire::as_str)
        .unwrap_or("(unrecorded)");
    let requested_resolution = requested_resolution
        .map(BeadResolutionWire::as_str)
        .unwrap_or("(unspecified)");
    let recorded_reason = issue.close_reason.as_deref().unwrap_or("(none)");
    let requested_reason = requested_reason.unwrap_or("(unspecified)");
    let closed_at = issue.closed_at.as_deref().unwrap_or("(unknown)");
    Err(BeadError::validation(format!(
        "close request conflicts with already-closed bead {} (closed at {}, resolution {}, reason {:?}); requested resolution {}, reason {:?}. Reopen it with `sase bead open {}` before closing it again, or append evidence without re-closing it with `sase bead note {} '…'`",
        issue.id,
        closed_at,
        recorded_resolution,
        recorded_reason,
        requested_resolution,
        requested_reason,
        issue.id,
        issue.id,
    )))
}

#[derive(Default)]
struct CloseBatch {
    standard_close_ids: BTreeSet<String>,
    closed_ids: Vec<String>,
    event_closed: Vec<CloseEvent>,
    returned: Vec<IssueWire>,
}

struct CloseEvent {
    issue: IssueWire,
    forced_descendant_ids: Vec<String>,
}

fn close_one_and_delegated_parent(
    store: &mut MutableStore,
    issue_id: &str,
    closed_at: &str,
    reason: Option<String>,
    resolution: BeadResolutionWire,
    forced_descendant_ids: Vec<String>,
    batch: &mut CloseBatch,
) -> Result<bool, BeadError> {
    let Some(issue) =
        store.close_one(issue_id, closed_at, reason, resolution)?
    else {
        return Ok(false);
    };
    batch.closed_ids.push(issue.id.clone());
    batch.event_closed.push(CloseEvent {
        issue: issue.clone(),
        forced_descendant_ids,
    });
    batch.returned.push(issue.clone());

    if issue.issue_type != IssueTypeWire::Plan {
        return Ok(true);
    }
    let Some(parent_id) = issue.parent_id.as_deref() else {
        return Ok(true);
    };
    if batch.standard_close_ids.contains(parent_id) {
        return Ok(true);
    }
    let Some(parent) = store
        .issues
        .iter()
        .find(|candidate| candidate.id == parent_id)
    else {
        return Ok(true);
    };
    if parent.issue_type != IssueTypeWire::Phase
        || parent.status == StatusWire::Closed
    {
        return Ok(true);
    }
    let all_children_closed = store.issues.iter().all(|candidate| {
        candidate.parent_id.as_deref() != Some(parent_id)
            || candidate.status == StatusWire::Closed
    });
    if !all_children_closed {
        return Ok(true);
    }

    let parent = store
        .close_one(
            parent_id,
            closed_at,
            Some("delegated work landed".to_string()),
            BeadResolutionWire::Done,
        )?
        .expect("non-closed delegated parent phase closes");
    batch.closed_ids.push(parent.id.clone());
    batch.event_closed.push(CloseEvent {
        issue: parent.clone(),
        forced_descendant_ids: Vec::new(),
    });
    batch.returned.push(parent);
    Ok(true)
}

const UNRESOLVED_DESCENDANT_DISPLAY_LIMIT: usize = 8;

fn unresolved_descendants<'a>(
    issues: &'a [IssueWire],
    issue_id: &str,
) -> Vec<&'a IssueWire> {
    sorted_descendants(issues, issue_id)
        .into_iter()
        .filter(|descendant| descendant.status != StatusWire::Closed)
        .collect()
}

/// Reject closing any batch target whose unresolved descendants are not
/// themselves also being closed by the same batch.
pub(crate) fn reject_unclosed_descendants_in_batch(
    issues: &[IssueWire],
    targets: &[String],
) -> Result<(), BeadError> {
    let target_set: BTreeSet<&str> =
        targets.iter().map(String::as_str).collect();
    for issue_id in targets {
        let unresolved: Vec<&IssueWire> =
            unresolved_descendants(issues, issue_id)
                .into_iter()
                .filter(|descendant| {
                    !target_set.contains(descendant.id.as_str())
                })
                .collect();
        if !unresolved.is_empty() {
            return Err(unclosed_descendants_error(issue_id, &unresolved));
        }
    }
    Ok(())
}

fn unclosed_descendants_error(
    issue_id: &str,
    unresolved: &[&IssueWire],
) -> BeadError {
    let shown = unresolved
        .iter()
        .take(UNRESOLVED_DESCENDANT_DISPLAY_LIMIT)
        .map(|descendant| {
            format!(
                "{} ({})",
                descendant.id,
                mutation_status_value(&descendant.status)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let remainder = unresolved
        .len()
        .saturating_sub(UNRESOLVED_DESCENDANT_DISPLAY_LIMIT);
    let remainder_text = if remainder == 0 {
        String::new()
    } else {
        format!(", and {remainder} more")
    };
    BeadError::validation(format!(
        "cannot close {issue_id}: {} descendant(s) are not closed: {shown}{remainder_text}; close them first or use --force with --reason and --resolution canceled|superseded",
        unresolved.len()
    ))
}

pub(crate) fn reopen_closed_ancestors(
    store: &mut MutableStore,
    issue_id: &str,
    opened_at: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    let mut parent_id = store.get_issue(issue_id)?.parent_id.clone();
    let mut visited = BTreeSet::new();
    let mut reopened = Vec::new();
    while let Some(current_id) = parent_id {
        if !visited.insert(current_id.clone()) {
            break;
        }
        let Some(index) =
            store.issues.iter().position(|issue| issue.id == current_id)
        else {
            break;
        };
        parent_id = store.issues[index].parent_id.clone();
        if store.issues[index].status != StatusWire::Closed {
            continue;
        }
        store.issues[index].status = StatusWire::Open;
        archive_close_metadata(
            &mut store.issues[index],
            opened_at,
            BeadReopenCauseWire::Open,
            None,
        );
        // A closed ancestor carries no snooze, so this clears nothing today;
        // it keeps every `issue_opened` emitter aligned with the reducer arm.
        clear_snooze_record(&mut store.issues[index]);
        store.issues[index].updated_at = opened_at.to_string();
        let ancestor = store.issues[index].clone();
        ancestor.validate()?;
        store.append_issue_event(
            &ancestor.id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            opened_at,
            &ancestor.created_by,
        )?;
        reopened.push(ancestor);
    }
    Ok(reopened)
}

pub fn remove_issues(
    beads_dir: &Path,
    issue_ids: &[String],
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if issue_ids.is_empty() {
        return Err(BeadError::validation(
            "remove_issues() requires at least one issue ID",
        ));
    }

    with_bead_mutation_lock(beads_dir, "remove", || {
        let mut store = MutableStore::load(beads_dir)?;
        let mut requested = Vec::new();
        let mut requested_ids = BTreeSet::new();
        for issue_id in issue_ids {
            let issue = store.get_issue(issue_id)?.clone();
            if requested_ids.insert(issue.id.clone()) {
                requested.push(issue);
            }
        }

        let mut removed = Vec::new();
        let mut removed_ids = BTreeSet::new();
        for issue in &requested {
            if issue.issue_type == IssueTypeWire::Plan {
                for descendant in sorted_descendants(&store.issues, &issue.id) {
                    if removed_ids.insert(descendant.id.clone()) {
                        removed.push(descendant.clone());
                    }
                }
            }
            if removed_ids.insert(issue.id.clone()) {
                removed.push(issue.clone());
            }
        }

        let removed_at = now_utc();
        for issue in &requested {
            let cascade_removed_issue_ids =
                if issue.issue_type == IssueTypeWire::Plan {
                    sorted_descendants(&store.issues, &issue.id)
                        .into_iter()
                        .map(|descendant| descendant.id.clone())
                        .collect()
                } else {
                    Vec::new()
                };
            store.append_issue_event(
                &issue.id,
                BeadEventOperationWire::IssueRemoved,
                BeadEventPayloadWire::IssueRemoved {
                    cascade_removed_issue_ids,
                },
                &removed_at,
                &issue.created_by,
            )?;
        }

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
    })
}

pub fn remove_issue(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    remove_issues(beads_dir, &[issue_id.to_string()])
}
