//! Event-stream reduction back into bead snapshots.
//!
//! Owns `reduce_event_streams`, link-provenance projection, the
//! per-event `apply_event` reducer, and the close/snooze/plus-one
//! metadata chokepoints shared with the mutation side.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::artifact_link::{
    canonicalize_artifact_link_ref, lookup_artifact_relation,
    validate_artifact_link_description, ArtifactLinkOriginWire,
    ArtifactLinkRowWire, BeadLinkDirectionWire, BeadLinkWire,
    ARTIFACT_LINK_ROW_SCHEMA_VERSION,
};

use super::super::wire::{
    parse_legacy_note_blob, parse_task_plus_one_observed_since,
    rekey_legacy_note_ids, validate_unique_external_refs, BeadCloseRecordWire,
    BeadError, BeadNoteWire, BeadReopenCauseWire, IssueTypeWire, IssueWire,
    StatusWire, TaskPlusOneEvidenceWire,
};
use super::merge::merge_stream_events;
use super::wire::{
    canonical_bead_source_ref, link_error, BeadEventPayloadWire,
    BeadEventRecordWire, BeadEventStreamWire, BeadIssueUpdateEventFieldsWire,
};

pub fn reduce_event_streams(
    streams: &[BeadEventStreamWire],
) -> Result<Vec<IssueWire>, BeadError> {
    Ok(reduce_event_streams_inner(streams, false)?.0)
}

/// Identity of one stored bead-owned link, keyed as the owning event stream
/// recorded it (source issue, relation, canonical target).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::bead) struct StoredLinkIdentity {
    pub source_issue_id: String,
    pub relation: String,
    pub target_ref: String,
    pub direction: BeadLinkDirectionWire,
}

/// Winning `LinkAdded` provenance for one currently active stored link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::bead) struct ActiveLinkProvenance {
    pub source_issue_id: String,
    pub target_ref: String,
    pub relation: String,
    pub description: String,
    pub origin: ArtifactLinkOriginWire,
    pub direction: BeadLinkDirectionWire,
    pub uses: u64,
    pub actor: String,
    pub timestamp: String,
}

/// Reduce streams once, optionally collecting exact `LinkAdded` provenance.
pub(in crate::bead) fn reduce_event_streams_with_link_provenance(
    streams: &[BeadEventStreamWire],
) -> Result<
    (
        Vec<IssueWire>,
        BTreeMap<StoredLinkIdentity, ActiveLinkProvenance>,
    ),
    BeadError,
> {
    reduce_event_streams_inner(streams, true)
}

fn reduce_event_streams_inner(
    streams: &[BeadEventStreamWire],
    collect_links: bool,
) -> Result<
    (
        Vec<IssueWire>,
        BTreeMap<StoredLinkIdentity, ActiveLinkProvenance>,
    ),
    BeadError,
> {
    let mut issues: BTreeMap<String, IssueWire> = BTreeMap::new();
    let mut provenance: BTreeMap<StoredLinkIdentity, ActiveLinkProvenance> =
        BTreeMap::new();
    let streams = validated_event_streams(streams)?;

    for event in merge_stream_events(&streams) {
        apply_event(&mut issues, event)?;
        if collect_links {
            apply_link_provenance(&mut provenance, event)?;
        }
    }

    let mut reduced: Vec<IssueWire> = issues.into_values().collect();
    reduced.sort_by(compare_issues_canonically);
    for issue in &reduced {
        issue.validate()?;
    }
    let reduced = collapse_duplicate_external_refs(reduced);
    validate_unique_external_refs(&reduced)?;
    Ok((reduced, provenance))
}

fn apply_link_provenance(
    provenance: &mut BTreeMap<StoredLinkIdentity, ActiveLinkProvenance>,
    event: &BeadEventRecordWire,
) -> Result<(), BeadError> {
    match &event.payload {
        BeadEventPayloadWire::LinkAdded {
            target_ref,
            relation,
            description,
            origin,
            direction,
            uses,
            ..
        } => {
            let target_ref = canonicalize_artifact_link_ref(target_ref)
                .map_err(link_error)?;
            let identity = StoredLinkIdentity {
                source_issue_id: event.issue_id.clone(),
                relation: relation.clone(),
                target_ref: target_ref.clone(),
                direction: *direction,
            };
            provenance.insert(
                identity,
                ActiveLinkProvenance {
                    source_issue_id: event.issue_id.clone(),
                    target_ref,
                    relation: relation.clone(),
                    description: description.clone(),
                    origin: *origin,
                    direction: *direction,
                    uses: *uses,
                    actor: event.actor.clone(),
                    timestamp: event.timestamp.clone(),
                },
            );
        }
        BeadEventPayloadWire::LinkRemoved {
            target_ref,
            relation,
            direction,
            ..
        } => {
            if let Ok(target_ref) = canonicalize_artifact_link_ref(target_ref) {
                provenance.remove(&StoredLinkIdentity {
                    source_issue_id: event.issue_id.clone(),
                    relation: relation.clone(),
                    target_ref,
                    direction: *direction,
                });
            }
        }
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => {
            let mut removed: BTreeSet<String> =
                cascade_removed_issue_ids.iter().cloned().collect();
            removed.insert(event.issue_id.clone());
            let removed_refs: BTreeSet<String> = removed
                .iter()
                .map(|id| canonical_bead_source_ref(id))
                .collect();
            provenance.retain(|_, link| {
                !removed.contains(&link.source_issue_id)
                    && !removed_refs.contains(&link.target_ref)
            });
        }
        _ => {}
    }
    Ok(())
}

pub(in crate::bead) fn artifact_link_row_from_provenance(
    link: &ActiveLinkProvenance,
) -> ArtifactLinkRowWire {
    let bead_ref = canonical_bead_source_ref(&link.source_issue_id);
    let (source_ref, target_ref) = match link.direction {
        BeadLinkDirectionWire::Out => (bead_ref, link.target_ref.clone()),
        BeadLinkDirectionWire::In => (link.target_ref.clone(), bead_ref),
    };
    ArtifactLinkRowWire {
        schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
        source_ref,
        relation: link.relation.clone(),
        target_ref,
        description: link.description.clone(),
        origin: link.origin,
        created_by: link.actor.clone(),
        created_at: link.timestamp.clone(),
        uses: link.uses,
    }
}

/// Collapse independently created issues that share one non-empty
/// `external_ref` into a single deterministic winner.
///
/// Two clones can each mint their own task bead mirroring the same external
/// tracker issue before their bead sidecars ever sync with each other. That
/// concurrent history must not make the whole store unreadable the moment it
/// is integrated, so this runs at the one seam where streams from different
/// origins first meet: reduction, not creation. Direct local mutation and
/// JSONL import still call [`validate_unique_external_refs`] themselves and
/// keep failing atomically on a duplicate.
///
/// The winner is the issue with the earliest `created_at`, tied-broken by
/// the smaller issue id, so the outcome is identical regardless of which
/// stream the merge visits first. The losing issue is dropped from this
/// materialized projection only — its append-only event stream is left on
/// disk untouched, so no source history is destroyed.
fn collapse_duplicate_external_refs(issues: Vec<IssueWire>) -> Vec<IssueWire> {
    let mut winner_index_by_ref: BTreeMap<String, usize> = BTreeMap::new();
    for (index, issue) in issues.iter().enumerate() {
        let external_ref = issue.external_ref.trim();
        if external_ref.is_empty() {
            continue;
        }
        winner_index_by_ref
            .entry(external_ref.to_string())
            .and_modify(|winner_index| {
                if external_ref_collapse_key(issue)
                    < external_ref_collapse_key(&issues[*winner_index])
                {
                    *winner_index = index;
                }
            })
            .or_insert(index);
    }

    let winning_indexes: BTreeSet<usize> =
        winner_index_by_ref.into_values().collect();
    issues
        .into_iter()
        .enumerate()
        .filter(|(index, issue)| {
            issue.external_ref.trim().is_empty()
                || winning_indexes.contains(index)
        })
        .map(|(_, issue)| issue)
        .collect()
}

fn external_ref_collapse_key(issue: &IssueWire) -> (&str, &str) {
    (issue.created_at.as_str(), issue.id.as_str())
}

/// Order regenerated issue projections identically across every writer.
///
/// Some binding consumers serialize the reducer result directly, while the
/// JSONL writer receives an arbitrary issue slice. Keeping their comparator
/// here prevents those paths from alternating between hierarchy-first and
/// plain-ID ordering.
pub(in crate::bead) fn compare_issues_canonically(
    left: &IssueWire,
    right: &IssueWire,
) -> Ordering {
    left.id.cmp(&right.id)
}

pub(in crate::bead) fn validated_event_streams(
    streams: &[BeadEventStreamWire],
) -> Result<Vec<BeadEventStreamWire>, BeadError> {
    let mut stream_ids = BTreeSet::new();
    let mut streams = streams.to_vec();
    streams.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
    for stream in &streams {
        stream.validate()?;
        if !stream_ids.insert(stream.stream_id.clone()) {
            return Err(BeadError::validation(format!(
                "duplicate bead event stream: {}",
                stream.stream_id
            )));
        }
    }
    Ok(streams)
}

pub(in crate::bead) fn apply_event(
    issues: &mut BTreeMap<String, IssueWire>,
    event: &BeadEventRecordWire,
) -> Result<(), BeadError> {
    event.validate()?;
    match &event.payload {
        BeadEventPayloadWire::IssueCreated { issue } => {
            if issues.contains_key(&issue.id) {
                return Err(BeadError::validation(format!(
                    "duplicate issue_created event for {}",
                    issue.id
                )));
            }
            let mut issue = issue.clone();
            rekey_legacy_note_ids(&mut issue.notes, &event.event_id);
            issue.dependencies.clear();
            issue.refs.clear();
            issue.links.clear();
            issue.plus_one_evidence.clear();
            issue.close_history.clear();
            issues.insert(issue.id.clone(), issue);
        }
        BeadEventPayloadWire::IssueUpdated { fields } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            apply_update_event_fields(issue, fields, event);
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::NoteAppended { entry } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            if let Some(note) = BeadNoteWire::from_event(
                &event.event_id,
                &event.timestamp,
                &event.actor,
                entry,
            ) {
                issue.notes.push(note);
            }
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::NoteEdited { note_id, text } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            let note = issue
                .notes
                .iter_mut()
                .find(|note| note.id == *note_id)
                .ok_or_else(|| {
                    BeadError::validation(format!(
                        "event references unknown note: {note_id}"
                    ))
                })?;
            note.text = text.trim().to_string();
            note.edited_at = Some(event.timestamp.clone());
            note.edited_by = Some(event.actor.clone());
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::NoteRemoved { note_id } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            let before = issue.notes.len();
            issue.notes.retain(|note| note.id != *note_id);
            if issue.notes.len() == before {
                return Err(BeadError::validation(format!(
                    "event references unknown note: {note_id}"
                )));
            }
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::IssueOpened => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.status = StatusWire::Open;
            archive_close_metadata(
                issue,
                &event.timestamp,
                BeadReopenCauseWire::Open,
                None,
            );
            clear_snooze_record(issue);
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::IssueClosed {
            close_reason,
            resolution,
            ..
        } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            if issue.status != StatusWire::Closed || issue.closed_at.is_none() {
                issue.status = StatusWire::Closed;
                issue.closed_at = Some(event.timestamp.clone());
                issue.close_reason = close_reason.clone();
                issue.resolution = resolution.clone();
                // Replaying an `issue_closed` event over a snoozed bead is
                // what heals a store the pre-fix close already bricked.
                clear_snooze_record(issue);
                issue.updated_at = event.timestamp.clone();
                issue.validate()?;
            }
        }
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => {
            let mut removed_ids: BTreeSet<String> =
                cascade_removed_issue_ids.iter().cloned().collect();
            removed_ids.insert(event.issue_id.clone());
            for removed_id in &removed_ids {
                issues.remove(removed_id);
            }
            for issue in issues.values_mut() {
                issue.dependencies.retain(|dep| {
                    !removed_ids.contains(&dep.issue_id)
                        && !removed_ids.contains(&dep.depends_on_id)
                });
            }
        }
        BeadEventPayloadWire::DependencyAdded { dependency } => {
            if !issues.contains_key(&dependency.depends_on_id) {
                return Err(BeadError::validation(format!(
                    "dependency_added target does not exist: {}",
                    dependency.depends_on_id
                )));
            }
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            if !issue
                .dependencies
                .iter()
                .any(|dep| dep.depends_on_id == dependency.depends_on_id)
            {
                issue.dependencies.push(dependency.clone());
            }
            issue.validate()?;
        }
        BeadEventPayloadWire::DependencyRemoved { dependency } => {
            if let Some(issue) = issues.get_mut(&event.issue_id) {
                issue.dependencies.retain(|existing| {
                    existing.depends_on_id != dependency.depends_on_id
                });
                issue.validate()?;
            }
        }
        BeadEventPayloadWire::ReferenceAdded { reference } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            if !issue.refs.contains(reference) {
                issue.refs.push(reference.clone());
            }
            issue.validate()?;
        }
        BeadEventPayloadWire::ReferenceRemoved { reference } => {
            if let Some(issue) = issues.get_mut(&event.issue_id) {
                issue.refs.retain(|existing| existing != reference);
                issue.validate()?;
            }
        }
        BeadEventPayloadWire::LinkAdded {
            target_ref,
            relation,
            description,
            origin,
            direction,
            uses,
            ..
        } => {
            apply_link_added(
                issues,
                &event.issue_id,
                target_ref,
                relation,
                description,
                *origin,
                *direction,
                *uses,
            )?;
        }
        BeadEventPayloadWire::LinkRemoved {
            target_ref,
            relation,
            direction,
            ..
        } => {
            if let Some(issue) = issues.get_mut(&event.issue_id) {
                let canonical = canonicalize_artifact_link_ref(target_ref)
                    .map_err(link_error)?;
                issue.links.retain(|existing| {
                    existing.target_ref != canonical
                        || existing.relation != *relation
                        || existing.direction != *direction
                });
                issue.validate()?;
            }
        }
        BeadEventPayloadWire::ReadyMarked => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.is_ready_to_work = true;
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::ReadyUnmarked => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.is_ready_to_work = false;
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::EpicWorkPreclaimed { agent_name } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.status = StatusWire::InProgress;
            archive_close_metadata(
                issue,
                &event.timestamp,
                BeadReopenCauseWire::EpicPreclaim,
                None,
            );
            clear_snooze_record(issue);
            issue.assignee = agent_name.clone();
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::TaskPlusOneRecorded { evidence } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            if issue.issue_type != IssueTypeWire::Task {
                return Err(BeadError::validation(format!(
                    "task +1 only applies to task beads: {}",
                    event.issue_id
                )));
            }
            if evidence.reporter == issue.created_by
                || issue
                    .plus_one_evidence
                    .iter()
                    .any(|existing| existing.reporter == evidence.reporter)
            {
                return Ok(());
            }
            issue.plus_one_evidence.push(evidence.clone());
            for reference in &evidence.refs {
                if !issue.refs.contains(reference) {
                    issue.refs.push(reference.clone());
                }
            }
            if let TaskPlusOneReopenDecision::Reopen =
                task_plus_one_reopen_decision(issue, evidence)?
            {
                let was_closed = issue.status == StatusWire::Closed;
                issue.status = StatusWire::Ready;
                archive_close_metadata(
                    issue,
                    &event.timestamp,
                    BeadReopenCauseWire::PlusOne,
                    Some(evidence.reporter.clone()),
                );
                if was_closed {
                    issue.assignee.clear();
                }
            }
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::TaskSnoozed { snooze } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.status = StatusWire::Snoozed;
            issue.snooze = Some(snooze.clone());
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        // Both wake branches return the bead to triage rather than to its
        // pre-snooze status: a snooze is only reachable from `open` or
        // `ready`, and the wake is exactly the moment the bead wants a
        // decision again.
        BeadEventPayloadWire::TaskSnoozeCanceled
        | BeadEventPayloadWire::TaskSnoozeWoken { .. } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.status = StatusWire::Ready;
            clear_snooze_record(issue);
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
    }
    Ok(())
}

fn apply_update_event_fields(
    issue: &mut IssueWire,
    fields: &BeadIssueUpdateEventFieldsWire,
    event: &BeadEventRecordWire,
) {
    if let Some(value) = &fields.title {
        issue.title = value.clone();
    }
    if let Some(value) = &fields.status {
        issue.status = value.clone();
        // Mirrors `apply_update_fields`, so a bead reprojected from its
        // events is byte-identical to the same bead mutated in memory.
        clear_snooze_record(issue);
    }
    if let Some(value) = &fields.assignee {
        issue.assignee = value.clone();
    }
    if let Some(value) = &fields.description {
        issue.description = value.clone();
    }
    if let Some(value) = &fields.notes {
        issue.notes = parse_legacy_note_blob(
            value,
            &event.event_id,
            &event.timestamp,
            &event.actor,
        );
    }
    if let Some(value) = &fields.design {
        issue.design = value.clone();
    }
    if let Some(value) = &fields.model {
        issue.model = value.clone();
    }
    if let Some(value) = &fields.size {
        issue.size = Some(value.clone());
    }
    if let Some(value) = &fields.closed_at {
        issue.closed_at = value.clone();
    }
    if let Some(value) = &fields.close_reason {
        issue.close_reason = value.clone();
    }
    if let Some(value) = &fields.resolution {
        issue.resolution = value.clone();
    }
    if let Some(value) = &fields.changespec_name {
        issue.changespec_name = value.clone();
    }
    if let Some(value) = &fields.changespec_bug_id {
        issue.changespec_bug_id = value.clone();
    }
    if let Some(value) = &fields.external_ref {
        issue.external_ref = value.clone();
    }
    if let Some(value) = &fields.tier {
        issue.tier = Some(value.clone());
    }
    if let Some(value) = fields.is_ready_to_work {
        issue.is_ready_to_work = value;
    }
    if let Some(value) = &fields.task_type_fields {
        issue.task_type_fields = value.clone();
    }
    if fields
        .status
        .as_ref()
        .is_some_and(|status| *status != StatusWire::Closed)
    {
        archive_close_metadata(
            issue,
            &event.timestamp,
            BeadReopenCauseWire::Update,
            None,
        );
    }
}

/// Move an undone close out of the flat close fields and into `close_history`.
///
/// This is the single chokepoint for every reopen path on both the reducer and
/// the mutation side, so a bead reprojected from its event streams is
/// byte-identical to the same bead mutated in memory.  Reopening a bead that
/// was never closed archives nothing; the trailing nulls still run so an
/// already-invalid stray `resolution` is cleared exactly as before.
pub(in crate::bead) fn archive_close_metadata(
    issue: &mut IssueWire,
    reopened_at: &str,
    reopened_via: BeadReopenCauseWire,
    reopened_by: Option<String>,
) {
    if let Some(closed_at) = issue.closed_at.take() {
        issue.close_history.push(BeadCloseRecordWire {
            closed_at,
            close_reason: issue.close_reason.take(),
            resolution: issue.resolution.take(),
            reopened_at: reopened_at.to_string(),
            reopened_via,
            reopened_by,
        });
    }
    issue.closed_at = None;
    issue.close_reason = None;
    issue.resolution = None;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::bead) enum TaskPlusOneReopenDecision {
    Reopen,
    Withheld { closed_at: String },
    Unchanged,
}

pub(in crate::bead) fn task_plus_one_reopen_decision(
    issue: &IssueWire,
    evidence: &TaskPlusOneEvidenceWire,
) -> Result<TaskPlusOneReopenDecision, BeadError> {
    match issue.status {
        StatusWire::Open => Ok(TaskPlusOneReopenDecision::Reopen),
        StatusWire::Closed => {
            let Some(observed_since) = evidence.observed_since.as_deref()
            else {
                return Ok(TaskPlusOneReopenDecision::Reopen);
            };
            let Some(closed_at) = issue.closed_at.as_deref() else {
                return Ok(TaskPlusOneReopenDecision::Reopen);
            };
            let observed = parse_task_plus_one_observed_since(observed_since)?;
            let closed =
                chrono::DateTime::parse_from_rfc3339(closed_at.trim())
                    .map_err(|error| {
                        BeadError::validation(format!(
                            "closed_at must be an RFC-3339 timestamp when comparing task +1 observed_since: {closed_at:?} ({error})"
                        ))
                    })?;
            if observed > closed {
                Ok(TaskPlusOneReopenDecision::Reopen)
            } else {
                Ok(TaskPlusOneReopenDecision::Withheld {
                    closed_at: closed_at.to_string(),
                })
            }
        }
        _ => Ok(TaskPlusOneReopenDecision::Unchanged),
    }
}

/// Drop the snooze record a bead leaving `snoozed` no longer owns.
///
/// The mirror of [`archive_close_metadata`], and the single chokepoint for
/// every transition out of `snoozed` on both the reducer and the mutation
/// side.  `IssueWire::validate` rejects a non-snoozed issue that still carries
/// snooze metadata, so a path that forgets this derives a record the model
/// refuses to store.  Clearing an issue that was never snoozed is a no-op.
pub(in crate::bead) fn clear_snooze_record(issue: &mut IssueWire) {
    issue.snooze = None;
}

fn existing_issue_mut<'a>(
    issues: &'a mut BTreeMap<String, IssueWire>,
    issue_id: &str,
) -> Result<&'a mut IssueWire, BeadError> {
    issues.get_mut(issue_id).ok_or_else(|| {
        BeadError::validation(format!(
            "event references unknown issue: {issue_id}"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_link_added(
    issues: &mut BTreeMap<String, IssueWire>,
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    description: &str,
    origin: ArtifactLinkOriginWire,
    direction: BeadLinkDirectionWire,
    uses: u64,
) -> Result<(), BeadError> {
    let canonical =
        canonicalize_artifact_link_ref(target_ref).map_err(link_error)?;
    lookup_artifact_relation(relation).map_err(link_error)?;
    let description =
        validate_artifact_link_description(description).map_err(link_error)?;
    if canonical == canonical_bead_source_ref(issue_id) {
        return Err(BeadError::validation(
            "artifact link cannot target itself",
        ));
    }
    let issue = existing_issue_mut(issues, issue_id)?;
    if let Some(existing) = issue.links.iter_mut().find(|link| {
        link.target_ref == canonical
            && link.relation == relation
            && link.direction == direction
    }) {
        existing.description = description;
        existing.origin = origin;
        existing.uses = uses;
    } else {
        issue.links.push(BeadLinkWire {
            target_ref: canonical,
            relation: relation.to_string(),
            description,
            origin,
            direction,
            uses,
        });
    }
    issue.validate()?;
    Ok(())
}
