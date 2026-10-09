use super::mutation_wire::BeadMutationOutcomeWire;
use super::notes_update::append_note_to_store;
use super::shared::commit_staged_write;
use super::shared::load_mutation_stream;
use super::shared::mint_stream_event;
use super::store::mutation_status_value;
use super::store::now_utc;
use super::store::outcome;
use super::store::sorted_descendants;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use super::view::MutationView;
use crate::bead::config::default_config;
use crate::bead::config::load_config;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadEventStreamWire;
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
    with_bead_mutation_lock(beads_dir, "open", || {
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) =
                try_cached_open(beads_dir, view, issue_id, now.clone())?
            {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        let issue_id = store.resolve_issue_id(issue_id)?;
        let index = store.issue_index(&issue_id)?;
        let old_issue = store.issues[index].clone();
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
            &issue_id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            &now,
            &issue.created_by,
        )?;
        let reopened_ancestors = if was_closed {
            reopen_closed_ancestors(&mut store, &issue_id, &now)?
        } else {
            Vec::new()
        };
        store.save()?;

        let mut result = outcome("open", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        result.old_issues = vec![old_issue];
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

    with_bead_mutation_lock(beads_dir, "close", || {
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) = try_cached_close(
                beads_dir,
                view,
                issue_ids,
                reason.clone(),
                resolution.clone(),
                force,
                note.clone(),
                actor.clone(),
                now.clone(),
                note_attachments.clone(),
            )? {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        // Resolve inside the locked load: the single store read is the
        // authority for existence and ambiguity, so callers pass raw IDs.
        let resolved_ids: Vec<String> = issue_ids
            .iter()
            .map(|issue_id| store.resolve_issue_id(issue_id))
            .collect::<Result<Vec<_>, _>>()?;
        let issue_ids = &resolved_ids;
        let now = now.unwrap_or_else(now_utc);
        // The acting closer: trimmed non-blank value, else the store owner.
        // It is the note author and the envelope actor plus `closed_by` on
        // every `issue_closed` event in the batch (requested ids,
        // `--force`-swept descendants, and auto-closed delegated parents).
        // A blank owner (test-only stores) leaves `closed_by` absent rather
        // than writing a blank value `validate_for` rejects.
        let actor = actor
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(&store.config.owner)
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
        if let Some(entry) = note.as_ref() {
            let attachments = note_attachments.clone().unwrap_or_default();
            for issue_id in &requested_ids {
                let index = store.issue_index(issue_id)?;
                append_note_to_store(
                    &mut store,
                    index,
                    entry,
                    &actor,
                    &now,
                    &attachments,
                )?;
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
                    closed_by: closed_by.clone(),
                },
                &now,
                &actor,
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
        // Request order with duplicates, exactly as the old caller-side
        // pre-resolve produced it for the mutation summary.
        result.requested_issue_ids = resolved_ids.clone();
        if !changed {
            result.message =
                "all requested issues were already closed".to_string();
        }
        result.issues = batch.returned;
        result.old_issues = batch.old_issues;
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
    /// Pre-mutation states parallel to [`CloseBatch::returned`], so callers
    /// can render status transitions without their own pre-read.
    old_issues: Vec<IssueWire>,
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
    let old_issue = store.get_issue(issue_id)?.clone();
    let Some(issue) =
        store.close_one(issue_id, closed_at, reason, resolution)?
    else {
        batch.old_issues.push(old_issue);
        return Ok(false);
    };
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

    let old_parent = store
        .issues
        .iter()
        .find(|candidate| candidate.id == parent_id)
        .cloned();
    let parent = store
        .close_one(
            parent_id,
            closed_at,
            Some("delegated work landed".to_string()),
            BeadResolutionWire::Done,
        )?
        .expect("non-closed delegated parent phase closes");
    if let Some(old_parent) = old_parent {
        batch.old_issues.push(old_parent);
    }
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
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) =
                try_cached_remove(beads_dir, view, issue_ids)?
            {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        // Resolve inside the locked load: the single store read is the
        // authority for existence and ambiguity, so callers pass raw IDs.
        let resolved_ids: Vec<String> = issue_ids
            .iter()
            .map(|issue_id| store.resolve_issue_id(issue_id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut requested = Vec::new();
        let mut requested_ids = BTreeSet::new();
        for issue_id in &resolved_ids {
            let issue = store.get_issue(issue_id)?.clone();
            if requested_ids.insert(issue.id.clone()) {
                requested.push(issue);
            }
        }
        let requested_issue_ids = resolved_ids.clone();

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
        result.requested_issue_ids = requested_issue_ids;
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

/// Load one physical stream for a lifecycle mutation, caching loaded
/// streams by ID so issues that share a stream mint into one copy.
///
/// Returns `Ok(None)` when a stream file the mutation needs is missing,
/// so the caller falls back to replay (which owns the corruption and
/// legacy behavior). Staging is in-memory only, so a later fallback
/// leaves nothing durable behind.
fn lifecycle_stream_slot(
    beads_dir: &Path,
    streams: &mut Vec<BeadEventStreamWire>,
    base_lens: &mut Vec<usize>,
    stream_index: &mut BTreeMap<String, usize>,
    stream_id: &str,
) -> Result<Option<usize>, BeadError> {
    if let Some(index) = stream_index.get(stream_id) {
        return Ok(Some(*index));
    }
    let Some(loaded) = load_mutation_stream(beads_dir, stream_id)? else {
        return Ok(None);
    };
    base_lens.push(loaded.events.len());
    streams.push(loaded);
    let index = streams.len() - 1;
    stream_index.insert(stream_id.to_string(), index);
    Ok(Some(index))
}

/// Cached `open_issue` over the mutation view: the target row, its
/// closed ancestors, and their streams only.
///
/// Mirrors the replay path exactly: the flip, archive, snooze clear and
/// `IssueOpened` mint run even for a never-closed bead, and closed
/// ancestors reopen in chain order. Returns `Ok(None)` when a needed
/// stream file is missing so replay owns the corruption error.
fn try_cached_open(
    beads_dir: &Path,
    mut view: MutationView,
    issue_id: &str,
    now: Option<String>,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
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
        reopen_closed_ancestors_via_view(&mut view, &issue.id, &now)?
    } else {
        Vec::new()
    };

    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let mut streams: Vec<BeadEventStreamWire> = Vec::new();
    let mut base_lens: Vec<usize> = Vec::new();
    let mut stream_index: BTreeMap<String, usize> = BTreeMap::new();
    let stream_id = view.stream_id_for_issue(&issue.id)?;
    let Some(index) = lifecycle_stream_slot(
        beads_dir,
        &mut streams,
        &mut base_lens,
        &mut stream_index,
        &stream_id,
    )?
    else {
        return Ok(None);
    };
    mint_stream_event(
        &mut streams[index],
        BeadEventOperationWire::IssueOpened,
        BeadEventPayloadWire::IssueOpened,
        &now,
        &issue.created_by,
        &issue.id,
    )?;
    for ancestor in &reopened {
        let stream_id = view.stream_id_for_issue(&ancestor.id)?;
        let Some(index) = lifecycle_stream_slot(
            beads_dir,
            &mut streams,
            &mut base_lens,
            &mut stream_index,
            &stream_id,
        )?
        else {
            return Ok(None);
        };
        mint_stream_event(
            &mut streams[index],
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            &now,
            &ancestor.created_by,
            &ancestor.id,
        )?;
    }

    let mut expected = vec![(issue.id.clone(), issue.clone())];
    for ancestor in &reopened {
        expected.push((ancestor.id.clone(), ancestor.clone()));
    }
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        &streams,
        &base_lens,
        &expected,
    )?;
    let Some(corrected) = committed else {
        return Ok(None);
    };
    let mut corrected_by_id: BTreeMap<String, IssueWire> = corrected
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
    Ok(Some(result))
}

/// Reopen closed ancestors through the view, staging each reopen.
///
/// Mirrors [`reopen_closed_ancestors`] exactly: walks the ancestor chain
/// via the view (which sees already-staged reopens), flips closed
/// ancestors to open with archived close metadata, stages them, and
/// returns them in chain order. Callers mint one `IssueOpened` event
/// per returned ancestor in its own stream.
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

/// Closed rows staged by one cached close batch, mirroring [`CloseBatch`].
#[derive(Default)]
struct CachedCloseBatch {
    standard_close_ids: BTreeSet<String>,
    closed_ids: Vec<String>,
    event_closed: Vec<CachedCloseEvent>,
    returned: Vec<IssueWire>,
    /// Pre-mutation states parallel to [`CachedCloseBatch::returned`].
    old_issues: Vec<IssueWire>,
}

struct CachedCloseEvent {
    issue: IssueWire,
    forced_descendant_ids: Vec<String>,
}

/// Cached `close_issues_with_note` over the mutation view: affected
/// subtrees plus affected streams only.
///
/// Runs the replay algorithm in the same phases (preflight every
/// request, then notes, then closes with delegated-parent completion,
/// then one `IssueClosed` mint per closed row), so outcomes,
/// orderings, error kinds and error text match the replay path
/// exactly. Descendant guards load only the affected subtrees through
/// the view, and delegated-parent completion uses the children lookup
/// instead of an all-issue scan. Returns `Ok(None)` when a needed
/// stream file or the manifest is missing so replay owns the
/// corruption and legacy behavior.
#[allow(clippy::too_many_arguments)]
fn try_cached_close(
    beads_dir: &Path,
    mut view: MutationView,
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
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
    let resolved_ids: Vec<String> = issue_ids
        .iter()
        .map(|issue_id| view.resolve(issue_id))
        .collect::<Result<Vec<_>, _>>()?;
    let issue_ids = &resolved_ids;
    let now = now.unwrap_or_else(now_utc);
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    // The acting closer: trimmed non-blank value, else the store owner.
    // It is the note author and the envelope actor plus `closed_by` on
    // every `issue_closed` event in the batch (requested ids,
    // `--force`-swept descendants, and auto-closed delegated parents).
    // A blank owner (test-only stores) leaves `closed_by` absent rather
    // than writing a blank value `validate_for` rejects.
    let actor = actor
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&config.owner)
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

    let mut streams: Vec<BeadEventStreamWire> = Vec::new();
    let mut base_lens: Vec<usize> = Vec::new();
    let mut stream_index: BTreeMap<String, usize> = BTreeMap::new();
    let mut noted_ids = Vec::new();
    let mut noted_rows: BTreeMap<String, IssueWire> = BTreeMap::new();
    if let Some(entry) = note.as_ref() {
        let attachments = note_attachments.clone().unwrap_or_default();
        for issue_id in &requested_ids {
            let mut issue = view.get(issue_id)?;
            let stream_id = view.stream_id_for_issue(&issue.id)?;
            let Some(index) = lifecycle_stream_slot(
                beads_dir,
                &mut streams,
                &mut base_lens,
                &mut stream_index,
                &stream_id,
            )?
            else {
                return Ok(None);
            };
            let event_id = mint_stream_event(
                &mut streams[index],
                BeadEventOperationWire::NoteAppended,
                BeadEventPayloadWire::NoteAppended {
                    entry: entry.clone(),
                    attachments: attachments.clone(),
                },
                &now,
                &actor,
                &issue.id,
            )?;
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
    let mut batch = CachedCloseBatch {
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
                if cached_close_one_and_delegated_parent(
                    &mut view,
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
        if !cached_close_one_and_delegated_parent(
            &mut view,
            &issue_id,
            &now,
            reason.clone(),
            effective_resolution.clone(),
            forced_descendant_ids,
            &mut batch,
        )? {
            batch.returned.push(view.get(&issue_id)?);
        }
    }
    for event in &batch.event_closed {
        let stream_id = view.stream_id_for_issue(&event.issue.id)?;
        let Some(index) = lifecycle_stream_slot(
            beads_dir,
            &mut streams,
            &mut base_lens,
            &mut stream_index,
            &stream_id,
        )?
        else {
            return Ok(None);
        };
        mint_stream_event(
            &mut streams[index],
            BeadEventOperationWire::IssueClosed,
            BeadEventPayloadWire::IssueClosed {
                close_reason: event.issue.close_reason.clone(),
                resolution: event.issue.resolution.clone(),
                forced_descendant_ids: event.forced_descendant_ids.clone(),
                closed_by: closed_by.clone(),
            },
            &now,
            &actor,
            &event.issue.id,
        )?;
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
        return Ok(Some(result));
    }
    let mut expected: Vec<(String, IssueWire)> =
        noted_rows.into_iter().collect();
    for issue in &batch.returned {
        if expected.iter().all(|(id, _)| id != &issue.id) {
            expected.push((issue.id.clone(), issue.clone()));
        }
    }
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        &streams,
        &base_lens,
        &expected,
    )?;
    let Some(corrected) = committed else {
        return Ok(None);
    };
    let mut corrected_by_id: BTreeMap<String, IssueWire> = corrected
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
    Ok(Some(result))
}

/// Close one row through the view plus its delegated parent, mirroring
/// [`close_one_and_delegated_parent`].
///
/// The flip matches `MutableStore::close_one` exactly (already-closed
/// rows report `false` with their pre-mutation state recorded), and
/// delegated-parent completion uses the children lookup instead of an
/// all-issue scan. Every closed row is staged immediately so later
/// sibling checks see the staged final state.
fn cached_close_one_and_delegated_parent(
    view: &mut MutationView,
    issue_id: &str,
    closed_at: &str,
    reason: Option<String>,
    resolution: BeadResolutionWire,
    forced_descendant_ids: Vec<String>,
    batch: &mut CachedCloseBatch,
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
    batch.event_closed.push(CachedCloseEvent {
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
    batch.event_closed.push(CachedCloseEvent {
        issue: parent.clone(),
        forced_descendant_ids: Vec::new(),
    });
    batch.returned.push(parent);
    Ok(true)
}

/// Cached `remove_issues` over the mutation view: affected rows plus
/// affected streams only.
///
/// Mirrors the replay path exactly: plan cascades in post-order with
/// replay-identical `cascade_removed_issue_ids`, survivor dependency
/// cleanup through reverse dependents (`edges_dst`) instead of
/// iterating every issue, and one `IssueRemoved` mint per requested
/// issue in argument order. Published deletes update edges, suffixes
/// and allocation through the shared tail resume. Returns `Ok(None)`
/// when a needed stream file or the manifest is missing so replay
/// owns the corruption and legacy behavior.
fn try_cached_remove(
    beads_dir: &Path,
    mut view: MutationView,
    issue_ids: &[String],
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
    // Resolve inside the locked admission: the single view read is the
    // authority for existence and ambiguity, so callers pass raw IDs. A
    // missing later ID fails before anything is staged or written.
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
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    // Physical stream routing is resolved while every row still loads:
    // a staged removal no longer resolves to a row.
    let mut stream_ids: BTreeMap<String, String> = BTreeMap::new();
    for issue in &requested {
        stream_ids
            .insert(issue.id.clone(), view.stream_id_for_issue(&issue.id)?);
    }
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
    for issue in &removed {
        view.stage_removal(&issue.id);
    }

    let mut streams: Vec<BeadEventStreamWire> = Vec::new();
    let mut base_lens: Vec<usize> = Vec::new();
    let mut stream_index: BTreeMap<String, usize> = BTreeMap::new();
    for issue in &requested {
        let stream_id =
            stream_ids.get(&issue.id).expect("routed stream recorded");
        let Some(index) = lifecycle_stream_slot(
            beads_dir,
            &mut streams,
            &mut base_lens,
            &mut stream_index,
            stream_id,
        )?
        else {
            return Ok(None);
        };
        mint_stream_event(
            &mut streams[index],
            BeadEventOperationWire::IssueRemoved,
            BeadEventPayloadWire::IssueRemoved {
                cascade_removed_issue_ids: cascades
                    .remove(&issue.id)
                    .unwrap_or_default(),
            },
            &removed_at,
            &issue.created_by,
            &issue.id,
        )?;
    }

    let expected: Vec<(String, IssueWire)> = survivors
        .into_iter()
        .map(|survivor| (survivor.id.clone(), survivor))
        .collect();
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        &streams,
        &base_lens,
        &expected,
    )?;
    if committed.is_none() {
        return Ok(None);
    }

    let mut result = outcome(
        "rm",
        true,
        removed.iter().map(|issue| issue.id.clone()).collect(),
    );
    result.requested_issue_ids = requested_issue_ids;
    result.issues = removed;
    Ok(Some(result))
}
