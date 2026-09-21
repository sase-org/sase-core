use super::close_remove::reject_unclosed_descendants_in_batch;
use super::close_remove::reopen_closed_ancestors;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::mutation_wire::BeadUpdateFieldsWire;
use super::store::normalize_model;
use super::store::now_utc;
use super::store::outcome;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadIssueUpdateEventFieldsWire;
use crate::bead::wire::validate_unique_external_refs;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
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
    with_bead_mutation_lock(beads_dir, "update", || {
        let mut store = MutableStore::load(beads_dir)?;

        let mut seen = HashSet::new();
        let targets: Vec<String> = issue_ids
            .iter()
            .filter(|issue_id| seen.insert((*issue_id).clone()))
            .cloned()
            .collect();

        let indexes = targets
            .iter()
            .map(|issue_id| store.issue_index(issue_id))
            .collect::<Result<Vec<_>, _>>()?;

        if fields.status.as_deref() == Some("closed") {
            reject_unclosed_descendants_in_batch(&store.issues, &targets)?;
        }

        let event_fields = event_fields_from_update_fields(&fields)?;
        let now = fields.now.clone().unwrap_or_else(now_utc);

        let mut planned: Vec<(usize, IssueWire, bool)> = Vec::new();
        let mut unchanged_ids = Vec::new();
        let mut resulting_issues = Vec::with_capacity(targets.len());
        for (target_id, index) in targets.iter().zip(indexes.iter().copied()) {
            let current = store.issues[index].clone();
            let was_closed = current.status == StatusWire::Closed;
            let mut issue = current.clone();
            apply_update_fields(&mut issue, fields.clone(), &now)?;
            if issue == current {
                unchanged_ids.push(target_id.clone());
                resulting_issues.push(current);
                continue;
            }
            issue.updated_at = now.clone();
            issue.validate()?;
            resulting_issues.push(issue.clone());
            planned.push((index, issue, was_closed));
        }

        let mut candidate_issues = store.issues.clone();
        for (index, issue, _) in &planned {
            candidate_issues[*index] = issue.clone();
        }
        validate_unique_external_refs(&candidate_issues)?;

        if planned.is_empty() {
            let mut result = outcome("update", false, Vec::new());
            result.unchanged_ids = unchanged_ids;
            result.issues = resulting_issues;
            return Ok(result);
        }

        let mut changed_ids = Vec::with_capacity(planned.len());
        let mut reopened_ancestors: Vec<IssueWire> = Vec::new();
        for (index, issue, was_closed) in planned {
            store.issues[index] = issue.clone();
            changed_ids.push(issue.id.clone());
            store.append_issue_event(
                &issue.id,
                BeadEventOperationWire::IssueUpdated,
                BeadEventPayloadWire::IssueUpdated {
                    fields: event_fields.clone(),
                },
                &issue.updated_at,
                &issue.created_by,
            )?;
            if was_closed && issue.status != StatusWire::Closed {
                let newly_reopened = reopen_closed_ancestors(
                    &mut store,
                    &issue.id,
                    &issue.updated_at,
                )?;
                reopened_ancestors.extend(newly_reopened);
            }
        }
        store.save()?;

        let mut result = outcome("update", true, changed_ids);
        result.unchanged_ids = unchanged_ids;
        result.issues = resulting_issues;
        result.reopened_ancestor_ids = reopened_ancestors
            .iter()
            .map(|ancestor| ancestor.id.clone())
            .collect();
        Ok(result)
    })
}

pub fn append_issue_note(
    beads_dir: &Path,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let entry = entry.trim();
    if entry.is_empty() {
        return Err(BeadError::validation(
            "note entry cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "note", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let now = now.unwrap_or_else(now_utc);
        let author = author
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| store.config.owner.clone());
        let issue =
            append_note_to_store(&mut store, index, entry, &author, &now)?;
        store.save()?;

        let mut result = outcome("note", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub(crate) fn append_note_to_store(
    store: &mut MutableStore,
    issue_index: usize,
    entry: &str,
    author: &str,
    now: &str,
) -> Result<IssueWire, BeadError> {
    let issue_id = store.issues[issue_index].id.clone();
    let event_id = store.append_issue_event(
        &issue_id,
        BeadEventOperationWire::NoteAppended,
        BeadEventPayloadWire::NoteAppended {
            entry: entry.to_string(),
        },
        now,
        author,
    )?;
    if let Some(note) = crate::bead::wire::BeadNoteWire::from_event(
        &event_id, now, author, entry,
    ) {
        store.issues[issue_index].notes.push(note);
    }
    store.issues[issue_index].updated_at = now.to_string();
    let issue = store.issues[issue_index].clone();
    issue.validate()?;
    Ok(issue)
}

pub fn edit_issue_note(
    beads_dir: &Path,
    issue_id: &str,
    note_id: &str,
    text: &str,
    author: Option<String>,
    now: Option<String>,
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

    with_bead_mutation_lock(beads_dir, "note_edit", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let now = now.unwrap_or_else(now_utc);
        let author = author
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| store.config.owner.clone());
        let issue = edit_note_in_store(
            &mut store, index, note_id, text, &author, &now,
        )?;
        store.save()?;

        let mut result = outcome("note_edit", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

fn edit_note_in_store(
    store: &mut MutableStore,
    issue_index: usize,
    note_id: &str,
    text: &str,
    author: &str,
    now: &str,
) -> Result<IssueWire, BeadError> {
    if !store.issues[issue_index]
        .notes
        .iter()
        .any(|note| note.id == note_id)
    {
        return Err(note_not_found(note_id));
    }
    let issue_id = store.issues[issue_index].id.clone();
    store.append_issue_event(
        &issue_id,
        BeadEventOperationWire::NoteEdited,
        BeadEventPayloadWire::NoteEdited {
            note_id: note_id.to_string(),
            text: text.to_string(),
        },
        now,
        author,
    )?;
    let note = store.issues[issue_index]
        .notes
        .iter_mut()
        .find(|note| note.id == note_id)
        .ok_or_else(|| note_not_found(note_id))?;
    note.text = text.to_string();
    note.edited_at = Some(now.to_string());
    note.edited_by = Some(author.to_string());
    store.issues[issue_index].updated_at = now.to_string();
    let issue = store.issues[issue_index].clone();
    issue.validate()?;
    Ok(issue)
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

    with_bead_mutation_lock(beads_dir, "note_remove", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let now = now.unwrap_or_else(now_utc);
        let author = author
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| store.config.owner.clone());
        let issue =
            remove_note_from_store(&mut store, index, note_id, &author, &now)?;
        store.save()?;

        let mut result = outcome("note_remove", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

fn remove_note_from_store(
    store: &mut MutableStore,
    issue_index: usize,
    note_id: &str,
    author: &str,
    now: &str,
) -> Result<IssueWire, BeadError> {
    if !store.issues[issue_index]
        .notes
        .iter()
        .any(|note| note.id == note_id)
    {
        return Err(note_not_found(note_id));
    }
    let issue_id = store.issues[issue_index].id.clone();
    store.append_issue_event(
        &issue_id,
        BeadEventOperationWire::NoteRemoved,
        BeadEventPayloadWire::NoteRemoved {
            note_id: note_id.to_string(),
        },
        now,
        author,
    )?;
    store.issues[issue_index]
        .notes
        .retain(|note| note.id != note_id);
    store.issues[issue_index].updated_at = now.to_string();
    let issue = store.issues[issue_index].clone();
    issue.validate()?;
    Ok(issue)
}

fn apply_update_fields(
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

fn event_fields_from_update_fields(
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

fn parse_status(value: &str) -> Result<StatusWire, BeadError> {
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
