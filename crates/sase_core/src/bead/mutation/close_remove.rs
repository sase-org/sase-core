use super::mutation_wire::BeadMutationOutcomeWire;
use super::runner::run_mutation;
use super::runner::MutationStep;
use super::store::mutation_status_value;
use super::store::now_utc;
use super::store::outcome;
use super::store::sorted_descendants;
use super::view::MutationView;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadNoteWire;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

pub fn open_issue(
    beads_dir: &Path,
    issue_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    run_mutation(beads_dir, "open", |view| {
        run_open(view, issue_id, now.clone())
    })
}

/// The single `open_issue` algorithm, over the view on both backings.
///
/// Reads resolve through the view, the flip stages the row so ancestor
/// reopening sees it, every event mints with `stage_event`, and one
/// `commit` persists both backings. A decline retries the same closure
/// on the replay backing; errors preserve the replay oracle's kinds and
/// messages.
fn run_open(
    view: &mut MutationView,
    issue_id: &str,
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let old_issue = view.get(&resolved)?;
    let mut issue = old_issue.clone();
    let was_closed = issue.status == StatusWire::Closed;
    let now = now.unwrap_or_else(now_utc);
    issue.status = StatusWire::Open;
    archive_close_metadata(&mut issue, &now, BeadReopenCauseWire::Open, None);
    clear_snooze_record(&mut issue);
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());

    let reopened = if was_closed {
        reopen_closed_ancestors_via_view(view, &issue.id, &now)?
    } else {
        Vec::new()
    };

    let Some(_) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::IssueOpened,
        BeadEventPayloadWire::IssueOpened,
        &now,
        &issue.created_by,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    for ancestor in &reopened {
        let Some(_) = view.stage_event(
            &ancestor.id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            &now,
            &ancestor.created_by,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }

    let mut expected_ids = vec![issue.id.clone()];
    for ancestor in &reopened {
        expected_ids.push(ancestor.id.clone());
    }
    let Some(committed) = view.commit(&expected_ids)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let mut corrected_by_id: BTreeMap<String, IssueWire> = committed
        .into_iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect();
    if let Some(truth) = corrected_by_id.remove(&issue.id) {
        issue = truth;
    }
    let mut reopened_out = Vec::with_capacity(reopened.len());
    for ancestor in reopened {
        reopened_out
            .push(corrected_by_id.remove(&ancestor.id).unwrap_or(ancestor));
    }

    let mut result = outcome("open", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    result.old_issues = vec![old_issue];
    result.reopened_ancestor_ids = reopened_out
        .iter()
        .map(|ancestor| ancestor.id.clone())
        .collect();
    result.issues = reopened_out;
    Ok(MutationStep::Done(result))
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
        beads_dir, issue_ids, reason, resolution, force, None, None, now, None,
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
    actor: Option<String>,
    now: Option<String>,
    note_attachments: Option<
        Vec<crate::note_attachment::BeadNoteAttachmentWire>,
    >,
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
            Some(entry)
        }
    };

    run_mutation(beads_dir, "close", |view| {
        run_close(
            view,
            issue_ids,
            reason.clone(),
            resolution.clone(),
            force,
            note.clone(),
            actor.clone(),
            now.clone(),
            note_attachments.clone(),
        )
    })
}

/// The single `close_issues_with_note` algorithm, over the view on both
/// backings.
///
/// Runs the replay oracle's phases in its order (preflight every
/// request, then notes, then closes with delegated-parent completion,
/// then one `IssueClosed` mint per closed row), so outcomes, orderings,
/// error kinds and error text match exactly. Descendant guards load only
/// the affected subtrees through the view, delegated-parent completion
/// uses the children lookup, note rows stage through `stage_event`, and
/// one `commit` persists both backings. A decline retries the same
/// closure on the replay backing.
#[allow(clippy::too_many_arguments)]
fn run_close(
    view: &mut MutationView,
    issue_ids: &[String],
    reason: Option<String>,
    resolution: Option<BeadResolutionWire>,
    force: bool,
    note: Option<String>,
    actor: Option<String>,
    now: Option<String>,
    note_attachments: Option<
        Vec<crate::note_attachment::BeadNoteAttachmentWire>,
    >,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved_ids: Vec<String> = issue_ids
        .iter()
        .map(|issue_id| view.resolve(issue_id))
        .collect::<Result<Vec<_>, _>>()?;
    let issue_ids = &resolved_ids;
    let now = now.unwrap_or_else(now_utc);
    // The acting closer: trimmed non-blank value, else the store owner.
    // It is the note author and the envelope actor plus `closed_by` on
    // every `issue_closed` event in the batch (requested ids,
    // `--force`-swept descendants, and auto-closed delegated parents).
    // A blank owner (test-only stores) leaves `closed_by` absent rather
    // than writing a blank value `validate_for` rejects.
    let owner = view.config()?.owner.clone();
    let actor = actor
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&owner)
        .to_string();
    let closed_by = (!actor.trim().is_empty()).then(|| actor.clone());
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
        let issue = view.get(issue_id)?;
        if !requested_ids.insert(issue.id.clone()) {
            continue;
        }
        if issue.status == StatusWire::Closed {
            reject_conflicting_close(
                &issue,
                resolution.as_ref(),
                reason.as_deref(),
            )?;
            already_closed_ids.push(issue.id.clone());
        }
        standard_close_ids.insert(issue.id.clone());
        // One subtree load per request: descendants arrive in
        // post-order, mirroring the replay collector, so both the
        // guard and the standard-close extension read the same order.
        let descendants = view.descendants(&issue.id)?;
        let unresolved: Vec<IssueWire> = descendants
            .iter()
            .filter(|descendant| descendant.status != StatusWire::Closed)
            .cloned()
            .collect();
        if !force && !unresolved.is_empty() {
            let unresolved_refs: Vec<&IssueWire> = unresolved.iter().collect();
            return Err(unclosed_descendants_error(
                &issue.id,
                &unresolved_refs,
            ));
        }
        standard_close_ids.extend(
            descendants
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
    let mut noted_rows: BTreeMap<String, IssueWire> = BTreeMap::new();
    if let Some(entry) = note.as_ref() {
        let attachments = note_attachments.clone().unwrap_or_default();
        for issue_id in &requested_ids {
            let mut issue = view.get(issue_id)?;
            let Some(event_id) = view.stage_event(
                &issue.id,
                BeadEventOperationWire::NoteAppended,
                BeadEventPayloadWire::NoteAppended {
                    entry: entry.clone(),
                    attachments: attachments.clone(),
                },
                &now,
                &actor,
            )?
            else {
                return Ok(MutationStep::NeedsReplay);
            };
            if let Some(note_row) = BeadNoteWire::from_event(
                &event_id,
                &now,
                &actor,
                entry,
                attachments.clone(),
            ) {
                issue.notes.push(note_row);
            }
            issue.updated_at = now.clone();
            issue.validate()?;
            view.stage_issue(issue.clone());
            noted_rows.insert(issue.id.clone(), issue.clone());
            noted_ids.push(issue_id.clone());
        }
    } else if note_attachments
        .as_ref()
        .is_some_and(|manifest| !manifest.is_empty())
    {
        return Err(BeadError::validation(
            "note_attachments requires a close note to attach to",
        ));
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
                    view,
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
            view,
            &issue_id,
            &now,
            reason.clone(),
            effective_resolution.clone(),
            forced_descendant_ids,
            &mut batch,
        )? {
            // Already closed: the row still joins the commit's expected
            // set, exactly as the old cached path passed its unchanged
            // rows to the commit tail, so stage it verbatim.
            let unchanged = view.get(&issue_id)?;
            view.stage_issue(unchanged.clone());
            batch.returned.push(unchanged);
        }
    }
    for event in &batch.event_closed {
        let Some(_) = view.stage_event(
            &event.issue.id,
            BeadEventOperationWire::IssueClosed,
            BeadEventPayloadWire::IssueClosed {
                close_reason: event.issue.close_reason.clone(),
                resolution: event.issue.resolution.clone(),
                forced_descendant_ids: event.forced_descendant_ids.clone(),
                closed_by: closed_by.clone(),
            },
            &now,
            &actor,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }

    let closed_ids = batch.closed_ids;
    let cascade_closed_ids = closed_ids
        .iter()
        .filter(|issue_id| !requested_ids.contains(*issue_id))
        .cloned()
        .collect::<Vec<_>>();
    let changed = !closed_ids.is_empty() || !noted_ids.is_empty();
    if !changed {
        let mut result = outcome("close", false, Vec::new());
        result.requested_issue_ids = resolved_ids.clone();
        result.message = "all requested issues were already closed".to_string();
        result.issues = batch.returned;
        result.old_issues = batch.old_issues;
        result.closed_ids = closed_ids;
        result.already_closed_ids = already_closed_ids;
        result.noted_ids = noted_ids;
        result.cascade_closed_ids = cascade_closed_ids;
        return Ok(MutationStep::Done(result));
    }
    let mut expected_ids: Vec<String> = noted_rows.into_keys().collect();
    for issue in &batch.returned {
        if !expected_ids.iter().any(|id| id == &issue.id) {
            expected_ids.push(issue.id.clone());
        }
    }
    let Some(committed) = view.commit(&expected_ids)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let mut corrected_by_id: BTreeMap<String, IssueWire> = committed
        .into_iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect();
    let mut returned = Vec::with_capacity(batch.returned.len());
    for issue in batch.returned {
        returned.push(corrected_by_id.remove(&issue.id).unwrap_or(issue));
    }

    let mut affected_ids = closed_ids.clone();
    for issue_id in &noted_ids {
        if !affected_ids.contains(issue_id) {
            affected_ids.push(issue_id.clone());
        }
    }
    let mut result = outcome("close", true, affected_ids);
    // Request order with duplicates, exactly as the replay path produces
    // it for the mutation summary.
    result.requested_issue_ids = resolved_ids.clone();
    result.issues = returned;
    result.old_issues = batch.old_issues;
    result.closed_ids = closed_ids;
    result.already_closed_ids = already_closed_ids;
    result.noted_ids = noted_ids;
    result.cascade_closed_ids = cascade_closed_ids;
    Ok(MutationStep::Done(result))
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

/// Closed rows staged by one close batch.
#[derive(Default)]
struct CloseBatch {
    standard_close_ids: BTreeSet<String>,
    closed_ids: Vec<String>,
    event_closed: Vec<CloseEvent>,
    returned: Vec<IssueWire>,
    /// Pre-mutation states parallel to [`CloseBatch::returned`], so callers
    /// can render status transitions without their own pre-read.
    old_issues: Vec<IssueWire>,
}

struct CloseEvent {
    issue: IssueWire,
    forced_descendant_ids: Vec<String>,
}

/// Close one row through the view plus its delegated parent.
///
/// The flip matches the replay oracle exactly (already-closed rows
/// report `false` with their pre-mutation state recorded), and
/// delegated-parent completion uses the children lookup instead of an
/// all-issue scan. Every closed row is staged immediately so later
/// sibling checks see the staged final state.
fn close_one_and_delegated_parent(
    view: &mut MutationView,
    issue_id: &str,
    closed_at: &str,
    reason: Option<String>,
    resolution: BeadResolutionWire,
    forced_descendant_ids: Vec<String>,
    batch: &mut CloseBatch,
) -> Result<bool, BeadError> {
    let old_issue = view.get(issue_id)?;
    let mut issue = old_issue.clone();
    if issue.status == StatusWire::Closed {
        batch.old_issues.push(old_issue);
        return Ok(false);
    }
    issue.status = StatusWire::Closed;
    issue.closed_at = Some(closed_at.to_string());
    issue.close_reason = reason;
    issue.resolution = Some(resolution);
    clear_snooze_record(&mut issue);
    issue.updated_at = closed_at.to_string();
    issue.validate()?;
    view.stage_issue(issue.clone());
    batch.old_issues.push(old_issue);
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
    let Ok(parent) = view.get(parent_id) else {
        return Ok(true);
    };
    if parent.issue_type != IssueTypeWire::Phase
        || parent.status == StatusWire::Closed
    {
        return Ok(true);
    }
    let all_children_closed = view
        .children(parent_id)?
        .iter()
        .all(|candidate| candidate.status == StatusWire::Closed);
    if !all_children_closed {
        return Ok(true);
    }

    let old_parent = view.get(parent_id)?;
    let mut parent = old_parent.clone();
    parent.status = StatusWire::Closed;
    parent.closed_at = Some(closed_at.to_string());
    parent.close_reason = Some("delegated work landed".to_string());
    parent.resolution = Some(BeadResolutionWire::Done);
    clear_snooze_record(&mut parent);
    parent.updated_at = closed_at.to_string();
    parent.validate()?;
    view.stage_issue(parent.clone());
    batch.old_issues.push(old_parent);
    batch.closed_ids.push(parent.id.clone());
    batch.event_closed.push(CloseEvent {
        issue: parent.clone(),
        forced_descendant_ids: Vec::new(),
    });
    batch.returned.push(parent);
    Ok(true)
}

const UNRESOLVED_DESCENDANT_DISPLAY_LIMIT: usize = 8;

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

/// Reopen closed ancestors through the view, staging each reopen.
///
/// Walks the ancestor chain via the view (which sees already-staged
/// reopens), flips closed ancestors to open with archived close
/// metadata, stages them, and returns them in chain order. Callers mint
/// one `IssueOpened` event per returned ancestor in its own stream.
fn reopen_closed_ancestors_via_view(
    view: &mut MutationView,
    issue_id: &str,
    opened_at: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    let mut reopened = Vec::new();
    let mut seen = BTreeSet::new();
    let mut next = view.get(issue_id)?.parent_id.clone();
    while let Some(parent_id) = next {
        if !seen.insert(parent_id.clone()) {
            break;
        }
        let Ok(parent) = view.get(&parent_id) else {
            break;
        };
        next = parent.parent_id.clone();
        if parent.status != StatusWire::Closed {
            continue;
        }
        let mut ancestor = parent.clone();
        ancestor.status = StatusWire::Open;
        archive_close_metadata(
            &mut ancestor,
            opened_at,
            BeadReopenCauseWire::Open,
            None,
        );
        // A closed ancestor carries no snooze, so this clears nothing today;
        // it keeps every `issue_opened` emitter aligned with the reducer arm.
        clear_snooze_record(&mut ancestor);
        ancestor.updated_at = opened_at.to_string();
        ancestor.validate()?;
        view.stage_issue(ancestor.clone());
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

    run_mutation(beads_dir, "remove", |view| run_remove(view, issue_ids))
}

pub fn remove_issue(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    remove_issues(beads_dir, &[issue_id.to_string()])
}

/// The single `remove_issues` algorithm, over the view on both backings.
///
/// Resolve inside the locked admission: the single view read is the
/// authority for existence and ambiguity, so callers pass raw IDs. Plan
/// cascades compute in post-order before any staging (once removals are
/// staged the subtree lookups no longer see them), with replay-identical
/// `cascade_removed_issue_ids`. Survivor dependency cleanup runs through
/// reverse dependents instead of iterating every issue, and one
/// `IssueRemoved` mint per requested issue runs in argument order. Events
/// mint before removals stage, because a staged removal no longer
/// resolves to a row for stream routing. One `commit` persists both
/// backings; a decline retries the same closure on the replay backing.
fn run_remove(
    view: &mut MutationView,
    issue_ids: &[String],
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    // A missing later ID fails before anything is staged or written.
    let resolved_ids: Vec<String> = issue_ids
        .iter()
        .map(|issue_id| view.resolve(issue_id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut requested = Vec::new();
    let mut requested_ids = BTreeSet::new();
    for issue_id in &resolved_ids {
        let issue = view.get(issue_id)?.clone();
        if requested_ids.insert(issue.id.clone()) {
            requested.push(issue);
        }
    }
    let requested_issue_ids = resolved_ids.clone();

    let mut removed = Vec::new();
    let mut removed_ids = BTreeSet::new();
    // Cascade order is computed before any staging: once removals are
    // staged the subtree lookups no longer see them.
    let mut cascades: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for issue in &requested {
        if issue.issue_type == IssueTypeWire::Plan {
            let descendants = view.descendants(&issue.id)?;
            cascades.insert(
                issue.id.clone(),
                descendants
                    .iter()
                    .map(|descendant| descendant.id.clone())
                    .collect(),
            );
            for descendant in descendants {
                if removed_ids.insert(descendant.id.clone()) {
                    removed.push(descendant);
                }
            }
        }
        if removed_ids.insert(issue.id.clone()) {
            removed.push(issue.clone());
        }
    }

    // Survivor dependents through the edge table instead of an
    // every-issue scan. Queried before staging so the overlay still
    // reflects the pre-removal graph.
    let mut survivor_ids = BTreeSet::new();
    for removed_id in &removed_ids {
        for dependent in view.reverse_dependents(removed_id)? {
            if !removed_ids.contains(&dependent.id) {
                survivor_ids.insert(dependent.id.clone());
            }
        }
    }

    let removed_at = now_utc();
    let mut survivors = Vec::new();
    for survivor_id in &survivor_ids {
        let mut survivor = view.get(survivor_id)?;
        survivor.dependencies.retain(|dep| {
            !removed_ids.contains(&dep.issue_id)
                && !removed_ids.contains(&dep.depends_on_id)
        });
        survivor.validate()?;
        view.stage_issue(survivor.clone());
        survivors.push(survivor);
    }

    for issue in &requested {
        let Some(_) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::IssueRemoved,
            BeadEventPayloadWire::IssueRemoved {
                cascade_removed_issue_ids: cascades
                    .remove(&issue.id)
                    .unwrap_or_default(),
            },
            &removed_at,
            &issue.created_by,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    for issue in &removed {
        view.stage_removal(&issue.id);
    }

    let expected_ids: Vec<String> = survivors
        .iter()
        .map(|survivor| survivor.id.clone())
        .collect();
    let Some(_) = view.commit(&expected_ids)? else {
        return Ok(MutationStep::NeedsReplay);
    };

    let mut result = outcome(
        "rm",
        true,
        removed.iter().map(|issue| issue.id.clone()).collect(),
    );
    result.requested_issue_ids = requested_issue_ids;
    result.issues = removed;
    Ok(MutationStep::Done(result))
}
