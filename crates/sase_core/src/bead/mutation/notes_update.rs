use super::close_remove::reject_unclosed_descendants_in_batch;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::mutation_wire::BeadUpdateFieldsWire;
use super::runner::run_mutation;
use super::runner::MutationStep;
use super::store::normalize_model;
use super::store::now_utc;
use super::store::outcome;
use super::view::MutationView;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadIssueUpdateEventFieldsWire;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::path::Path;

pub fn update_issue(
    beads_dir: &Path,
    issue_id: &str,
    fields: BeadUpdateFieldsWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let requested = vec![issue_id.to_string()];
    let mut result = update_issues(beads_dir, &requested, fields)?;
    let issue = result.issues.pop().expect(
        "update_issues returns exactly one issue for a single-ID request",
    );
    result.issue = Some(issue);
    result.issue_ids = vec![issue_id.to_string()];
    result.issues = Vec::new();
    Ok(result)
}

/// Apply the same field changes to every named issue as one atomic mutation.
///
/// Every ID is resolved and every resulting issue is validated before
/// anything is written, so an unknown ID or an invalid field value leaves the
/// store byte-identical. Duplicate IDs (including a shorthand alongside its
/// resolved full form) collapse to a single update.
pub fn update_issues(
    beads_dir: &Path,
    issue_ids: &[String],
    fields: BeadUpdateFieldsWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if fields.is_ready_to_work.is_some() {
        return Err(BeadError::validation(
            "is_ready_to_work cannot be set via update(); use mark_ready_to_work() instead.",
        ));
    }
    if fields.notes.is_some() {
        return Err(BeadError::validation(
            "notes cannot be replaced via update(); use append_issue_note() instead.",
        ));
    }
    run_mutation(beads_dir, "update", |view| {
        run_update(view, issue_ids, fields.clone())
    })
}

pub fn append_issue_note(
    beads_dir: &Path,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
    attachments: Option<Vec<crate::note_attachment::BeadNoteAttachmentWire>>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let entry = entry.trim();
    if entry.is_empty() {
        return Err(BeadError::validation(
            "note entry cannot be empty or blank",
        ));
    }

    run_mutation(beads_dir, "note", |view| {
        run_append_note(
            view,
            issue_id,
            entry,
            author.clone(),
            now.clone(),
            attachments.clone(),
        )
    })
}

/// Rewrite a note's text, with `attachments` replacing the manifest when
/// `Some` (including `Some([])`, which detaches every attachment) and
/// keeping it when `None` (an older writer's edit).
pub fn edit_issue_note(
    beads_dir: &Path,
    issue_id: &str,
    note_id: &str,
    text: &str,
    author: Option<String>,
    now: Option<String>,
    attachments: Option<Vec<crate::note_attachment::BeadNoteAttachmentWire>>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let note_id = note_id.trim();
    if note_id.is_empty() {
        return Err(BeadError::validation("note id cannot be empty or blank"));
    }
    let text = text.trim();
    if text.is_empty() {
        return Err(BeadError::validation(
            "note text cannot be empty or blank",
        ));
    }

    run_mutation(beads_dir, "note_edit", |view| {
        run_edit_note(
            view,
            issue_id,
            note_id,
            text,
            author.clone(),
            now.clone(),
            attachments.clone(),
        )
    })
}

pub fn remove_issue_note(
    beads_dir: &Path,
    issue_id: &str,
    note_id: &str,
    author: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let note_id = note_id.trim();
    if note_id.is_empty() {
        return Err(BeadError::validation("note id cannot be empty or blank"));
    }

    run_mutation(beads_dir, "note_remove", |view| {
        run_remove_note(view, issue_id, note_id, author.clone(), now.clone())
    })
}

/// The single `update_issues` algorithm, over the view on both backings.
///
/// Resolves every ID and validates every resulting issue before anything
/// is written, so an unknown ID or an invalid field value leaves the
/// store byte-identical. Events mint through `stage_event` and one
/// `commit` persists both backings; a decline retries the same closure on
/// the replay backing. Store and decode faults stay errors and never
/// become not-found; only pre-append cache faults decline.
fn run_update(
    view: &mut MutationView,
    issue_ids: &[String],
    fields: BeadUpdateFieldsWire,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let mut seen = HashSet::new();
    let mut targets: Vec<String> = Vec::new();
    let mut requested_ids: Vec<String> = Vec::with_capacity(issue_ids.len());
    for issue_id in issue_ids {
        let resolved = view.resolve(issue_id)?;
        requested_ids.push(resolved.clone());
        if seen.insert(resolved.clone()) {
            targets.push(resolved);
        }
    }
    let mut currents = Vec::with_capacity(targets.len());
    for target in &targets {
        currents.push(view.get(target)?);
    }
    let old_issues = currents.clone();

    if fields.status.as_deref() == Some("closed") {
        reject_unclosed_via_view(view, &currents, &targets)?;
    }

    let event_fields = event_fields_from_update_fields(&fields)?;
    let now = fields.now.clone().unwrap_or_else(now_utc);

    let mut planned: Vec<(IssueWire, bool)> = Vec::new();
    let mut unchanged_ids = Vec::new();
    let mut resulting_issues = Vec::with_capacity(targets.len());
    for (target_id, current) in targets.iter().zip(currents.iter()) {
        let was_closed = current.status == StatusWire::Closed;
        let mut issue = current.clone();
        apply_update_fields(&mut issue, fields.clone(), &now)?;
        if issue == *current {
            unchanged_ids.push(target_id.clone());
            resulting_issues.push(current.clone());
            continue;
        }
        issue.updated_at = now.clone();
        issue.validate()?;
        resulting_issues.push(issue.clone());
        planned.push((issue, was_closed));
    }

    if planned.is_empty() {
        let mut result = outcome("update", false, Vec::new());
        result.requested_issue_ids = requested_ids;
        result.unchanged_ids = unchanged_ids;
        result.issues = resulting_issues;
        result.old_issues = old_issues;
        return Ok(MutationStep::Done(result));
    }

    // External-ref uniqueness against the final overlay, including batch
    // exchanges: check before staging so a staged self cannot hide another
    // owner. A ref whose backing owner is itself vacating in this batch is
    // allowed; any other occupied ref conflicts, exactly as the replay
    // oracle's `validate_unique_external_refs` does.
    {
        let mut new_ref_owner: BTreeMap<String, &str> = BTreeMap::new();
        for (issue, _) in &planned {
            let external_ref = issue.external_ref.trim().to_string();
            if external_ref.is_empty() {
                continue;
            }
            if let Some(first) =
                new_ref_owner.insert(external_ref.clone(), issue.id.as_str())
            {
                return Err(BeadError::conflict(format!(
                    "external_ref {external_ref} already belongs to {first}; cannot also assign it to {}",
                    issue.id
                )));
            }
        }
        // Map every target's old ref for the vacate check below.
        let mut old_refs: BTreeMap<String, String> = BTreeMap::new();
        for (target_id, current) in targets.iter().zip(currents.iter()) {
            old_refs.insert(
                target_id.clone(),
                current.external_ref.trim().to_string(),
            );
        }
        for (issue, _) in &planned {
            let external_ref = issue.external_ref.trim().to_string();
            if external_ref.is_empty() {
                continue;
            }
            let old = old_refs.get(&issue.id).cloned().unwrap_or_default();
            if external_ref == old {
                continue;
            }
            if let Some(owner) = view.external_ref_owner(&external_ref)? {
                if owner.id == issue.id {
                    continue;
                }
                let vacating = planned.iter().any(|(other, _)| {
                    other.id == owner.id
                        && other.external_ref.trim() != external_ref
                });
                if !vacating {
                    return Err(BeadError::conflict(format!(
                        "external_ref {external_ref} already belongs to {}; cannot also assign it to {}",
                        owner.id, issue.id
                    )));
                }
            }
        }
    }
    for (issue, _) in &planned {
        view.stage_issue(issue.clone());
    }

    let mut changed_ids = Vec::with_capacity(planned.len());
    let mut reopened_ancestors: Vec<IssueWire> = Vec::new();

    for (issue, was_closed) in &planned {
        let Some(_) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: event_fields.clone(),
            },
            &issue.updated_at,
            &issue.created_by,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        changed_ids.push(issue.id.clone());
        if *was_closed && issue.status != StatusWire::Closed {
            let newly = reopen_ancestors_via_view(
                &mut *view,
                &issue.id,
                &issue.updated_at,
            )?;
            for ancestor in newly {
                let Some(_) = view.stage_event(
                    &ancestor.id,
                    BeadEventOperationWire::IssueOpened,
                    BeadEventPayloadWire::IssueOpened,
                    &ancestor.updated_at,
                    &ancestor.created_by,
                )?
                else {
                    return Ok(MutationStep::NeedsReplay);
                };
                reopened_ancestors.push(ancestor);
            }
        }
    }

    let mut expected_ids: Vec<String> =
        planned.iter().map(|(issue, _)| issue.id.clone()).collect();
    for ancestor in &reopened_ancestors {
        expected_ids.push(ancestor.id.clone());
    }
    let Some(committed) = view.commit(&expected_ids)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let mut corrected_by_id: BTreeMap<String, IssueWire> = committed
        .into_iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect();
    for issue in resulting_issues.iter_mut() {
        if let Some(truth) = corrected_by_id.remove(&issue.id) {
            *issue = truth;
        }
    }
    let mut reopened_out = Vec::with_capacity(reopened_ancestors.len());
    for ancestor in reopened_ancestors {
        reopened_out
            .push(corrected_by_id.remove(&ancestor.id).unwrap_or(ancestor));
    }

    let mut result = outcome("update", true, changed_ids);
    result.requested_issue_ids = requested_ids;
    result.unchanged_ids = unchanged_ids;
    result.issues = resulting_issues;
    result.old_issues = old_issues;
    result.reopened_ancestor_ids =
        reopened_out.iter().map(|issue| issue.id.clone()).collect();
    result.reopened_ancestors = reopened_out;
    Ok(MutationStep::Done(result))
}

/// Descendant guard through the view instead of an all-issue scan.
///
/// Loads only the affected subtrees, then reuses the shared batch guard
/// so the error kind and text match the replay path exactly.
fn reject_unclosed_via_view(
    view: &MutationView,
    currents: &[IssueWire],
    targets: &[String],
) -> Result<(), BeadError> {
    let mut check_by_id: BTreeMap<String, IssueWire> = currents
        .iter()
        .map(|issue| (issue.id.clone(), issue.clone()))
        .collect();
    for target in targets {
        for descendant in view.descendants(target)? {
            check_by_id
                .entry(descendant.id.clone())
                .or_insert(descendant);
        }
    }
    let check_set: Vec<IssueWire> = check_by_id.into_values().collect();
    reject_unclosed_descendants_in_batch(&check_set, targets)
}

/// Reopen closed ancestors through the view, staging each reopen.
///
/// Mirrors `close_remove::reopen_closed_ancestors` exactly: walks the
/// ancestor chain via the view (which sees already-staged reopens),
/// flips closed ancestors to open with archived close metadata, stages
/// them, and returns them in chain order. Callers mint one `IssueOpened`
/// event per returned ancestor in its own stream.
fn reopen_ancestors_via_view(
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
        clear_snooze_record(&mut ancestor);
        ancestor.updated_at = opened_at.to_string();
        ancestor.validate()?;
        view.stage_issue(ancestor.clone());
        reopened.push(ancestor);
    }
    Ok(reopened)
}

/// The single `append_issue_note` algorithm, over the view on both backings.
fn run_append_note(
    view: &mut MutationView,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
    attachments: Option<Vec<crate::note_attachment::BeadNoteAttachmentWire>>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let mut issue = view.get(&resolved)?;
    let owner = view.config()?.owner.clone();
    let now = now.unwrap_or_else(now_utc);
    let author = author
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(owner);

    let Some(event_id) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::NoteAppended,
        BeadEventPayloadWire::NoteAppended {
            entry: entry.to_string(),
            attachments: attachments.clone().unwrap_or_default(),
        },
        &now,
        &author,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    if let Some(note) = crate::bead::wire::BeadNoteWire::from_event(
        &event_id,
        &now,
        &author,
        entry,
        attachments.clone().unwrap_or_default(),
    ) {
        issue.notes.push(note);
    }
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());

    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one note row");
    let mut result = outcome("note", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

/// The single `edit_issue_note` algorithm, over the view on both backings.
///
/// `attachments` replaces the manifest when `Some` (including `Some([])`)
/// and keeps it when `None`, exactly as the replay oracle does. An unknown
/// note ID fails without writing, byte-identical to replay.
#[allow(clippy::too_many_arguments)]
fn run_edit_note(
    view: &mut MutationView,
    issue_id: &str,
    note_id: &str,
    text: &str,
    author: Option<String>,
    now: Option<String>,
    attachments: Option<Vec<crate::note_attachment::BeadNoteAttachmentWire>>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let mut issue = view.get(&resolved)?;
    if !issue.notes.iter().any(|note| note.id == note_id) {
        return Err(note_not_found(note_id));
    }
    let owner = view.config()?.owner.clone();
    let now = now.unwrap_or_else(now_utc);
    let author = author
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(owner);

    let Some(_) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::NoteEdited,
        BeadEventPayloadWire::NoteEdited {
            note_id: note_id.to_string(),
            text: text.to_string(),
            attachments: attachments.clone(),
        },
        &now,
        &author,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    let note = issue
        .notes
        .iter_mut()
        .find(|note| note.id == note_id)
        .ok_or_else(|| note_not_found(note_id))?;
    note.text = text.to_string();
    if let Some(manifest) = attachments {
        note.attachments = manifest;
    }
    note.edited_at = Some(now.clone());
    note.edited_by = Some(author.clone());
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());

    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one edited row");
    let mut result = outcome("note_edit", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

/// The single `remove_issue_note` algorithm, over the view on both backings.
///
/// An unknown note ID fails without writing, byte-identical to replay.
fn run_remove_note(
    view: &mut MutationView,
    issue_id: &str,
    note_id: &str,
    author: Option<String>,
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let mut issue = view.get(&resolved)?;
    if !issue.notes.iter().any(|note| note.id == note_id) {
        return Err(note_not_found(note_id));
    }
    let owner = view.config()?.owner.clone();
    let now = now.unwrap_or_else(now_utc);
    let author = author
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(owner);

    let Some(_) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::NoteRemoved,
        BeadEventPayloadWire::NoteRemoved {
            note_id: note_id.to_string(),
        },
        &now,
        &author,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    issue.notes.retain(|note| note.id != note_id);
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());

    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one removed row");
    let mut result = outcome("note_remove", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

pub(crate) fn apply_update_fields(
    issue: &mut IssueWire,
    fields: BeadUpdateFieldsWire,
    timestamp: &str,
) -> Result<(), BeadError> {
    let mut reopened = false;
    if let Some(value) = fields.title {
        issue.title = value;
    }
    if let Some(value) = fields.status {
        issue.status = parse_status(&value)?;
        reopened = issue.status != StatusWire::Closed;
        // Moving off `snoozed` drops the record, exactly as moving off
        // `closed` archives the close fields. Without this an ordinary
        // status update would leave a record the model refuses to store.
        clear_snooze_record(issue);
    }
    if let Some(value) = fields.assignee {
        issue.assignee = value;
    }
    if let Some(value) = fields.description {
        issue.description = value;
    }
    if let Some(value) = fields.design {
        issue.design = value;
    }
    if let Some(value) = fields.model {
        issue.model = normalize_model(value)?;
    }
    if let Some(value) = fields.size {
        issue.size = Some(value);
    }
    if let Some(value) = fields.closed_at {
        issue.closed_at = value;
    }
    if let Some(value) = fields.close_reason {
        issue.close_reason = value;
    }
    if let Some(value) = fields.resolution {
        issue.resolution = value;
    }
    if let Some(value) = fields.changespec_name {
        issue.changespec_name = value;
    }
    if let Some(value) = fields.changespec_bug_id {
        issue.changespec_bug_id = value;
    }
    if let Some(value) = fields.external_ref {
        issue.external_ref = value;
    }
    if let Some(value) = fields.tier {
        issue.tier = Some(value);
    }
    if let Some(value) = fields.task_type_fields {
        issue.task_type_fields = value;
    }
    // Archive last, matching `apply_update_event_fields`, so an explicit
    // closed_at/close_reason/resolution in the same update is archived rather
    // than surviving a move away from closed.
    if reopened {
        archive_close_metadata(
            issue,
            timestamp,
            BeadReopenCauseWire::Update,
            None,
        );
    }
    Ok(())
}

pub(crate) fn event_fields_from_update_fields(
    fields: &BeadUpdateFieldsWire,
) -> Result<BeadIssueUpdateEventFieldsWire, BeadError> {
    let status = fields.status.as_deref().map(parse_status).transpose()?;
    // Pass `resolution` through unchanged, exactly like `closed_at` and
    // `close_reason`: `archive_close_metadata` already archives-then-clears
    // it on any reopen, on both the direct-mutation and reducer paths. An
    // explicit `Some(None)` here used to be silently dropped by a
    // deserializer bug (see bead_event_resolution_roundtrip), so replay
    // never actually applied it; now that it round-trips, applying it
    // before `archive_close_metadata` runs would wipe the value that needs
    // to be archived into `close_history`.
    let event_fields = BeadIssueUpdateEventFieldsWire {
        title: fields.title.clone(),
        status,
        assignee: fields.assignee.clone(),
        description: fields.description.clone(),
        notes: fields.notes.clone(),
        design: fields.design.clone(),
        model: fields.model.clone().map(normalize_model).transpose()?,
        size: fields.size.clone(),
        closed_at: fields.closed_at.clone(),
        close_reason: fields.close_reason.clone(),
        resolution: fields.resolution.clone(),
        changespec_name: fields.changespec_name.clone(),
        changespec_bug_id: fields.changespec_bug_id.clone(),
        external_ref: fields.external_ref.clone(),
        tier: fields.tier.clone(),
        is_ready_to_work: fields.is_ready_to_work,
        task_type_fields: fields.task_type_fields.clone(),
    };
    if event_fields == BeadIssueUpdateEventFieldsWire::default() {
        return Err(BeadError::validation(
            "update() requires at least one mutable bead field",
        ));
    }
    Ok(event_fields)
}

pub(crate) fn parse_status(value: &str) -> Result<StatusWire, BeadError> {
    match value {
        "open" => Ok(StatusWire::Open),
        "claimed" => Ok(StatusWire::Claimed),
        "ready" => Ok(StatusWire::Ready),
        // `snoozed` is reachable through `snooze_task` only: the status alone
        // cannot express a wake time, so accepting it here would produce a
        // bead the model rejects. The refusal names the command that works.
        "snoozed" => Err(BeadError::validation(
            "snoozed requires a wake time; use: sase bead snooze <id> -u <time>",
        )),
        "in_progress" => Ok(StatusWire::InProgress),
        "closed" => Ok(StatusWire::Closed),
        _ => Err(BeadError::validation(format!(
            "invalid bead status: {value}"
        ))),
    }
}

fn note_not_found(note_id: &str) -> BeadError {
    BeadError {
        kind: "not_found".to_string(),
        message: format!("Note not found: {note_id}"),
    }
}
