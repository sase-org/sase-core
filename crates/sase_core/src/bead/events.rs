//! Append-only bead event wire records and pure reducers.
//!
//! Phase 1 keeps this module side-effect free: callers can import legacy
//! `IssueWire` snapshots into deterministic streams, then reduce streams back
//! into the current snapshot model. Later phases own filesystem integration.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::wire::{
    BeadCloseRecordWire, BeadError, BeadReopenCauseWire, BeadResolutionWire,
    BeadTierWire, DependencyWire, IssueTypeWire, IssueWire, PhaseSizeWire,
    StatusWire, TaskPlusOneEvidenceWire,
};

pub const BEAD_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventStoreManifestWire {
    pub schema_version: u32,
    pub stream_count: usize,
    pub generated_from: String,
    pub migration_tool: String,
}

impl BeadEventStoreManifestWire {
    pub fn from_streams(streams: &[BeadEventStreamWire]) -> Self {
        Self {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            stream_count: streams.len(),
            generated_from: "issues.jsonl".to_string(),
            migration_tool: "sase-core bead events".to_string(),
        }
    }

    pub fn validate(&self) -> Result<(), BeadError> {
        if self.schema_version != BEAD_EVENT_SCHEMA_VERSION {
            return Err(BeadError::validation(format!(
                "unsupported bead event manifest schema_version: {}",
                self.schema_version
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventStreamWire {
    pub stream_id: String,
    pub root_issue_id: String,
    #[serde(default)]
    pub events: Vec<BeadEventRecordWire>,
}

impl BeadEventStreamWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.stream_id.is_empty() {
            return Err(BeadError::validation("bead event stream_id is empty"));
        }
        if self.root_issue_id.is_empty() {
            return Err(BeadError::validation(
                "bead event root_issue_id is empty",
            ));
        }
        if self.stream_id != self.root_issue_id {
            return Err(BeadError::validation(format!(
                "bead event stream_id must match root_issue_id: {} != {}",
                self.stream_id, self.root_issue_id
            )));
        }
        for event in &self.events {
            event.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventRecordWire {
    pub schema_version: u32,
    pub event_id: String,
    pub timestamp: String,
    pub actor: String,
    pub operation: BeadEventOperationWire,
    pub issue_id: String,
    pub payload: BeadEventPayloadWire,
}

impl BeadEventRecordWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.schema_version != BEAD_EVENT_SCHEMA_VERSION {
            return Err(BeadError::validation(format!(
                "unsupported bead event schema_version: {}",
                self.schema_version
            )));
        }
        if self.event_id.is_empty() {
            return Err(BeadError::validation("bead event_id is empty"));
        }
        if self.issue_id.is_empty() {
            return Err(BeadError::validation("bead event issue_id is empty"));
        }
        if self.timestamp.is_empty() {
            return Err(BeadError::validation("bead event timestamp is empty"));
        }
        self.payload.validate_for(self.operation, &self.issue_id)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BeadEventOperationWire {
    IssueCreated,
    IssueUpdated,
    NoteAppended,
    IssueOpened,
    IssueClosed,
    IssueRemoved,
    DependencyAdded,
    DependencyRemoved,
    ReferenceAdded,
    ReferenceRemoved,
    ReadyMarked,
    ReadyUnmarked,
    EpicWorkPreclaimed,
    TaskPlusOneRecorded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
// Keep the public wire shape direct and serde-compatible with existing event logs.
#[allow(clippy::large_enum_variant)]
pub enum BeadEventPayloadWire {
    IssueCreated {
        issue: IssueWire,
    },
    IssueUpdated {
        fields: BeadIssueUpdateEventFieldsWire,
    },
    NoteAppended {
        entry: String,
    },
    IssueOpened,
    IssueClosed {
        close_reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolution: Option<BeadResolutionWire>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        forced_descendant_ids: Vec<String>,
    },
    IssueRemoved {
        #[serde(default)]
        cascade_removed_issue_ids: Vec<String>,
    },
    DependencyAdded {
        dependency: DependencyWire,
    },
    DependencyRemoved {
        dependency: DependencyWire,
    },
    ReferenceAdded {
        reference: String,
    },
    ReferenceRemoved {
        reference: String,
    },
    ReadyMarked,
    ReadyUnmarked,
    EpicWorkPreclaimed {
        agent_name: String,
    },
    TaskPlusOneRecorded {
        evidence: TaskPlusOneEvidenceWire,
    },
}

impl BeadEventPayloadWire {
    fn validate_for(
        &self,
        operation: BeadEventOperationWire,
        issue_id: &str,
    ) -> Result<(), BeadError> {
        match (operation, self) {
            (
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue },
            ) => {
                if issue.id != issue_id {
                    return Err(BeadError::validation(format!(
                        "issue_created payload id mismatch: {} != {}",
                        issue.id, issue_id
                    )));
                }
                issue.validate()
            }
            (
                BeadEventOperationWire::IssueUpdated,
                BeadEventPayloadWire::IssueUpdated { fields },
            ) => fields.validate(),
            (
                BeadEventOperationWire::NoteAppended,
                BeadEventPayloadWire::NoteAppended { entry },
            ) => {
                if entry.trim().is_empty() {
                    return Err(BeadError::validation(
                        "note_appended entry cannot be empty or blank",
                    ));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::IssueOpened,
                BeadEventPayloadWire::IssueOpened,
            )
            | (
                BeadEventOperationWire::IssueClosed,
                BeadEventPayloadWire::IssueClosed { .. },
            )
            | (
                BeadEventOperationWire::IssueRemoved,
                BeadEventPayloadWire::IssueRemoved { .. },
            )
            | (
                BeadEventOperationWire::ReadyMarked,
                BeadEventPayloadWire::ReadyMarked,
            )
            | (
                BeadEventOperationWire::ReadyUnmarked,
                BeadEventPayloadWire::ReadyUnmarked,
            ) => Ok(()),
            (
                BeadEventOperationWire::DependencyAdded,
                BeadEventPayloadWire::DependencyAdded { dependency },
            ) => {
                if dependency.issue_id != issue_id {
                    return Err(BeadError::validation(format!(
                        "dependency_added payload issue_id mismatch: {} != {}",
                        dependency.issue_id, issue_id
                    )));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::DependencyRemoved,
                BeadEventPayloadWire::DependencyRemoved { dependency },
            ) => {
                if dependency.issue_id != issue_id {
                    return Err(BeadError::validation(format!(
                        "dependency_removed payload issue_id mismatch: {} != {}",
                        dependency.issue_id, issue_id
                    )));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::ReferenceAdded,
                BeadEventPayloadWire::ReferenceAdded { .. },
            )
            | (
                BeadEventOperationWire::ReferenceRemoved,
                BeadEventPayloadWire::ReferenceRemoved { .. },
            ) => Ok(()),
            (
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed { agent_name },
            ) => {
                if agent_name.is_empty() {
                    return Err(BeadError::validation(
                        "epic_work_preclaimed agent_name is empty",
                    ));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::TaskPlusOneRecorded,
                BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
            ) => evidence.validate(),
            _ => Err(BeadError::validation(format!(
                "event operation/payload mismatch for {issue_id}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadIssueUpdateEventFieldsWire {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<StatusWire>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub size: Option<PhaseSizeWire>,
    #[serde(default)]
    pub closed_at: Option<Option<String>>,
    #[serde(default)]
    pub close_reason: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<Option<BeadResolutionWire>>,
    #[serde(default)]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub changespec_bug_id: Option<String>,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub is_ready_to_work: Option<bool>,
}

impl BeadIssueUpdateEventFieldsWire {
    fn validate(&self) -> Result<(), BeadError> {
        if self.status.is_none()
            && self.title.is_none()
            && self.assignee.is_none()
            && self.description.is_none()
            && self.notes.is_none()
            && self.design.is_none()
            && self.model.is_none()
            && self.size.is_none()
            && self.closed_at.is_none()
            && self.close_reason.is_none()
            && self.resolution.is_none()
            && self.changespec_name.is_none()
            && self.changespec_bug_id.is_none()
            && self.tier.is_none()
            && self.is_ready_to_work.is_none()
        {
            return Err(BeadError::validation(
                "issue_updated event has no fields",
            ));
        }
        Ok(())
    }
}

pub fn import_issues_to_event_streams(
    issues: &[IssueWire],
) -> Result<Vec<BeadEventStreamWire>, BeadError> {
    let mut issues = issues.to_vec();
    issues.sort_by_key(event_issue_key);
    let root_by_issue = root_issue_ids(&issues);
    let mut streams: BTreeMap<String, Vec<PendingEvent>> = BTreeMap::new();

    for issue in &issues {
        issue.validate()?;
        let stream_id = root_by_issue.get(&issue.id).ok_or_else(|| {
            BeadError::validation(format!(
                "cannot determine event stream for issue {}",
                issue.id
            ))
        })?;
        streams
            .entry(stream_id.clone())
            .or_default()
            .push(PendingEvent::created(issue));
    }

    let mut dependencies: Vec<DependencyWire> = issues
        .iter()
        .flat_map(|issue| issue.dependencies.iter().cloned())
        .collect();
    dependencies.sort_by(|a, b| {
        (&a.issue_id, &a.depends_on_id, &a.created_at, &a.created_by).cmp(&(
            &b.issue_id,
            &b.depends_on_id,
            &b.created_at,
            &b.created_by,
        ))
    });
    for dependency in dependencies {
        let stream_id = root_by_issue
            .get(&dependency.issue_id)
            .ok_or_else(|| {
                BeadError::validation(format!(
                    "cannot determine event stream for dependency {} -> {}",
                    dependency.issue_id, dependency.depends_on_id
                ))
            })?
            .clone();
        streams
            .entry(stream_id)
            .or_default()
            .push(PendingEvent::dependency_added(dependency));
    }

    for issue in &issues {
        let stream_id = root_by_issue
            .get(&issue.id)
            .ok_or_else(|| {
                BeadError::validation(format!(
                    "cannot determine event stream for references on {}",
                    issue.id
                ))
            })?
            .clone();
        for reference in &issue.refs {
            streams
                .entry(stream_id.clone())
                .or_default()
                .push(PendingEvent::reference_added(issue, reference.clone()));
        }
    }

    for issue in &issues {
        let stream_id = root_by_issue
            .get(&issue.id)
            .ok_or_else(|| {
                BeadError::validation(format!(
                    "cannot determine event stream for +1 evidence on {}",
                    issue.id
                ))
            })?
            .clone();
        for evidence in &issue.plus_one_evidence {
            streams.entry(stream_id.clone()).or_default().push(
                PendingEvent::task_plus_one_recorded(issue, evidence.clone()),
            );
        }
    }

    streams
        .into_iter()
        .map(|(root_issue_id, pending)| {
            let events = pending
                .into_iter()
                .enumerate()
                .map(|(index, pending)| {
                    pending.into_record(&root_issue_id, index + 1)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let stream = BeadEventStreamWire {
                stream_id: root_issue_id.clone(),
                root_issue_id,
                events,
            };
            stream.validate()?;
            Ok(stream)
        })
        .collect()
}

pub fn reduce_event_streams(
    streams: &[BeadEventStreamWire],
) -> Result<Vec<IssueWire>, BeadError> {
    let mut issues: BTreeMap<String, IssueWire> = BTreeMap::new();
    let streams = validated_event_streams(streams)?;

    for event in merge_stream_events(&streams) {
        apply_event(&mut issues, event)?;
    }

    let mut reduced: Vec<IssueWire> = issues.into_values().collect();
    reduced.sort_by(compare_issues_canonically);
    for issue in &reduced {
        issue.validate()?;
    }
    Ok(reduced)
}

/// Order regenerated issue projections identically across every writer.
///
/// Some binding consumers serialize the reducer result directly, while the
/// JSONL writer receives an arbitrary issue slice. Keeping their comparator
/// here prevents those paths from alternating between hierarchy-first and
/// plain-ID ordering.
pub(super) fn compare_issues_canonically(
    left: &IssueWire,
    right: &IssueWire,
) -> Ordering {
    left.id.cmp(&right.id)
}

pub(super) fn validated_event_streams(
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

/// Outcome of merging one conflicted bead event stream.
///
/// Two clones minting from their own counters can allocate the same bead id,
/// so both sides add an `issue_created` event for it. The union of those
/// branches is unreducible, and before relocation that error wedged every
/// later sync behind the same conflicted store. Renumbering the losing side
/// keeps the merge total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventStreamMergeWire {
    /// Merged stream, keeping the winning side of any id collision.
    pub merged: BeadEventStreamWire,
    /// Losing side of a top-level id collision, renamed onto a free id.
    #[serde(default)]
    pub relocated: Option<BeadEventStreamWire>,
    /// Every `(old_id, new_id)` pair this merge had to renumber.
    #[serde(default)]
    pub relocations: Vec<(String, String)>,
}

/// Which merge input contributed an event, used to attribute id collisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchTag {
    Base,
    Ours,
    Theirs,
    Both,
}

pub fn merge_bead_event_streams(
    base: &BeadEventStreamWire,
    ours: &BeadEventStreamWire,
    theirs: &BeadEventStreamWire,
) -> Result<BeadEventStreamWire, BeadError> {
    Ok(
        merge_bead_event_streams_with_relocation(base, ours, theirs, None)?
            .merged,
    )
}

/// Merge one stream, relocating a duplicated top-level bead onto a free id.
///
/// `relocation_issue_id` must be an id no stream in the store uses yet; the
/// caller owns that allocation because a single stream cannot see the rest of
/// the store. Passing `None` keeps the historical behavior of failing on a
/// duplicate `issue_created`. Duplicated *child* ids need no caller input:
/// the next free sibling number is derivable from the stream itself.
pub fn merge_bead_event_streams_with_relocation(
    base: &BeadEventStreamWire,
    ours: &BeadEventStreamWire,
    theirs: &BeadEventStreamWire,
    relocation_issue_id: Option<&str>,
) -> Result<BeadEventStreamMergeWire, BeadError> {
    base.validate()?;
    ours.validate()?;
    theirs.validate()?;
    if ours.stream_id != theirs.stream_id
        || ours.root_issue_id != theirs.root_issue_id
    {
        return Err(BeadError::validation(format!(
            "cannot merge bead event streams with different ids: {} != {}",
            ours.stream_id, theirs.stream_id
        )));
    }
    if base.stream_id != ours.stream_id
        || base.root_issue_id != ours.root_issue_id
    {
        return Err(BeadError::validation(format!(
            "cannot merge base bead event stream {} into {}",
            base.stream_id, ours.stream_id
        )));
    }

    let ours_base_indexes = validate_append_only_branch(base, ours, "ours")?;
    let theirs_base_indexes =
        validate_append_only_branch(base, theirs, "theirs")?;
    let base_events = event_keys(&base.events)?;
    let mut additions: BTreeMap<String, (BeadEventRecordWire, BranchTag)> =
        BTreeMap::new();
    for (tag, branch, base_indexes) in [
        (BranchTag::Ours, ours, &ours_base_indexes),
        (BranchTag::Theirs, theirs, &theirs_base_indexes),
    ] {
        for (index, event) in branch.events.iter().enumerate() {
            if base_indexes.contains(&index) {
                continue;
            }
            let key = serde_json::to_string(event)?;
            if base_events.contains(&key) {
                continue;
            }
            additions
                .entry(key)
                .and_modify(|entry| entry.1 = BranchTag::Both)
                .or_insert_with(|| (event.clone(), tag));
        }
    }

    let mut additions = additions.into_iter().collect::<Vec<_>>();
    additions.sort_by_key(|(serialized, (event, _))| {
        event_union_key(event, serialized)
    });
    let mut merged = base.clone();
    let mut provenance = vec![BranchTag::Base; base.events.len()];
    for (_, (event, tag)) in additions {
        merged.events.push(event);
        provenance.push(tag);
    }

    let (relocated, relocations) = split_duplicate_creations(
        &mut merged,
        provenance,
        relocation_issue_id,
    )?;
    merged.validate()?;
    if let Some(stream) = &relocated {
        stream.validate()?;
    }
    Ok(BeadEventStreamMergeWire {
        merged,
        relocated,
        relocations,
    })
}

/// A relocated stream, if any, plus every `(old_id, new_id)` pair renumbered.
type DuplicateCreationSplit =
    (Option<BeadEventStreamWire>, Vec<(String, String)>);

/// Renumber every bead whose `issue_created` event was minted twice.
///
/// Each round resolves exactly one collision and then re-scans, because
/// relocating a colliding root also carries away its children, which is often
/// what made those children look duplicated in the first place.
fn split_duplicate_creations(
    merged: &mut BeadEventStreamWire,
    provenance: Vec<BranchTag>,
    relocation_issue_id: Option<&str>,
) -> Result<DuplicateCreationSplit, BeadError> {
    let mut tagged: Vec<(BeadEventRecordWire, BranchTag)> =
        merged.events.drain(..).zip(provenance).collect();
    let mut relocated: Option<BeadEventStreamWire> = None;
    let mut relocations: Vec<(String, String)> = Vec::new();

    while let Some((issue_id, loser_tag)) = losing_creation(&tagged)? {
        if issue_id == merged.stream_id {
            let Some(new_id) = relocation_issue_id else {
                return Err(BeadError::validation(format!(
                    "duplicate issue_created event for {issue_id}"
                )));
            };
            if new_id.is_empty() || new_id == issue_id {
                return Err(BeadError::validation(format!(
                    "invalid relocation id for duplicate bead {issue_id}"
                )));
            }
            let moved = extract_subtree(&mut tagged, &issue_id, loser_tag);
            relocated = Some(relocated_stream(new_id, &issue_id, moved)?);
            relocations.push((issue_id, new_id.to_string()));
        } else {
            let new_id = next_sibling_issue_id(&tagged, &issue_id)?;
            remap_subtree(
                &mut tagged,
                &issue_id,
                loser_tag,
                &new_id,
                &merged.stream_id,
            )?;
            relocations.push((issue_id, new_id));
        }
    }

    merged.events = tagged.into_iter().map(|(event, _)| event).collect();
    Ok((relocated, relocations))
}

/// Return the issue id and contributing branch of the next creation to move.
fn losing_creation(
    tagged: &[(BeadEventRecordWire, BranchTag)],
) -> Result<Option<(String, BranchTag)>, BeadError> {
    let mut creations: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, (event, _)) in tagged.iter().enumerate() {
        if event.operation == BeadEventOperationWire::IssueCreated {
            creations
                .entry(event.issue_id.as_str())
                .or_default()
                .push(index);
        }
    }
    for (issue_id, indexes) in creations {
        if indexes.len() < 2 {
            continue;
        }
        // Whichever creation the merge base already carried is authoritative;
        // otherwise the older creation keeps the id so both clones agree
        // regardless of which side git happened to call "ours".
        let winner = indexes
            .iter()
            .copied()
            .find(|index| tagged[*index].1 == BranchTag::Base)
            .unwrap_or_else(|| {
                *indexes
                    .iter()
                    .min_by_key(|index| {
                        let event = &tagged[**index].0;
                        (event.timestamp.as_str(), event.event_id.as_str())
                    })
                    .expect("collision groups are non-empty")
            });
        let loser = indexes
            .iter()
            .copied()
            .find(|index| *index != winner)
            .expect("collision groups hold at least two creations");
        let loser_tag = tagged[loser].1;
        // A single side that minted the same id twice, or two byte-identical
        // creations, cannot be told apart by provenance, so there is nothing
        // safe to relocate.
        let ambiguous = matches!(loser_tag, BranchTag::Base | BranchTag::Both)
            || indexes
                .iter()
                .filter(|index| **index != winner)
                .any(|index| tagged[*index].1 != loser_tag);
        if ambiguous {
            return Err(BeadError::validation(format!(
                "duplicate issue_created event for {issue_id}"
            )));
        }
        return Ok(Some((issue_id.to_string(), loser_tag)));
    }
    Ok(None)
}

/// Remove and return the losing branch's events for `issue_id` and its children.
fn extract_subtree(
    tagged: &mut Vec<(BeadEventRecordWire, BranchTag)>,
    issue_id: &str,
    tag: BranchTag,
) -> Vec<BeadEventRecordWire> {
    let mut moved = Vec::new();
    let mut kept = Vec::with_capacity(tagged.len());
    for (event, event_tag) in tagged.drain(..) {
        if event_tag == tag && issue_id_in_subtree(&event.issue_id, issue_id) {
            moved.push(event);
        } else {
            kept.push((event, event_tag));
        }
    }
    *tagged = kept;
    moved
}

fn relocated_stream(
    new_id: &str,
    old_id: &str,
    events: Vec<BeadEventRecordWire>,
) -> Result<BeadEventStreamWire, BeadError> {
    let events = events
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            remapped_event(event, old_id, new_id, new_id, index + 1)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BeadEventStreamWire {
        stream_id: new_id.to_string(),
        root_issue_id: new_id.to_string(),
        events,
    })
}

/// Rewrite a duplicated child bead onto a free sibling number, in place.
fn remap_subtree(
    tagged: &mut [(BeadEventRecordWire, BranchTag)],
    issue_id: &str,
    tag: BranchTag,
    new_id: &str,
    stream_id: &str,
) -> Result<(), BeadError> {
    for (index, (event, event_tag)) in tagged.iter_mut().enumerate() {
        if *event_tag != tag || !issue_id_in_subtree(&event.issue_id, issue_id)
        {
            continue;
        }
        *event = remapped_event(
            event.clone(),
            issue_id,
            new_id,
            stream_id,
            index + 1,
        )?;
    }
    Ok(())
}

fn issue_id_in_subtree(issue_id: &str, root: &str) -> bool {
    issue_id == root || issue_id.starts_with(&format!("{root}."))
}

fn remapped_id(issue_id: &str, old_id: &str, new_id: &str) -> String {
    if issue_id == old_id {
        return new_id.to_string();
    }
    match issue_id.strip_prefix(&format!("{old_id}.")) {
        Some(rest) => format!("{new_id}.{rest}"),
        None => issue_id.to_string(),
    }
}

/// Rewrite every id a single event carries, then re-mint its event id.
///
/// The event id embeds the stream, ordinal, and issue id, so leaving the
/// original in place would let a relocated event collide with the bead it was
/// moved away from.
fn remapped_event(
    mut event: BeadEventRecordWire,
    old_id: &str,
    new_id: &str,
    stream_id: &str,
    ordinal: usize,
) -> Result<BeadEventRecordWire, BeadError> {
    event.issue_id = remapped_id(&event.issue_id, old_id, new_id);
    match &mut event.payload {
        BeadEventPayloadWire::IssueCreated { issue } => {
            issue.id = remapped_id(&issue.id, old_id, new_id);
            if let Some(parent_id) = &issue.parent_id {
                issue.parent_id = Some(remapped_id(parent_id, old_id, new_id));
            }
            for dependency in &mut issue.dependencies {
                remap_dependency(dependency, old_id, new_id);
            }
        }
        BeadEventPayloadWire::IssueClosed {
            forced_descendant_ids,
            ..
        } => {
            for descendant_id in forced_descendant_ids.iter_mut() {
                *descendant_id = remapped_id(descendant_id, old_id, new_id);
            }
        }
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => {
            for removed_id in cascade_removed_issue_ids.iter_mut() {
                *removed_id = remapped_id(removed_id, old_id, new_id);
            }
        }
        BeadEventPayloadWire::DependencyAdded { dependency }
        | BeadEventPayloadWire::DependencyRemoved { dependency } => {
            remap_dependency(dependency, old_id, new_id);
        }
        _ => {}
    }
    event.event_id = mint_bead_event_id(
        stream_id,
        ordinal,
        &event.timestamp,
        &event.actor,
        event.operation,
        &event.issue_id,
        &event.payload,
    )?;
    event.validate()?;
    Ok(event)
}

fn remap_dependency(
    dependency: &mut DependencyWire,
    old_id: &str,
    new_id: &str,
) {
    dependency.issue_id = remapped_id(&dependency.issue_id, old_id, new_id);
    dependency.depends_on_id =
        remapped_id(&dependency.depends_on_id, old_id, new_id);
}

/// Return the next unused direct-child id next to a duplicated child bead.
fn next_sibling_issue_id(
    tagged: &[(BeadEventRecordWire, BranchTag)],
    issue_id: &str,
) -> Result<String, BeadError> {
    let Some((parent_id, _)) = issue_id.rsplit_once('.') else {
        return Err(BeadError::validation(format!(
            "duplicate issue_created event for {issue_id}"
        )));
    };
    let child_prefix = format!("{parent_id}.");
    let mut max_child = 0u64;
    for (event, _) in tagged {
        for candidate in event_issue_ids(event) {
            let Some(suffix) = candidate.strip_prefix(&child_prefix) else {
                continue;
            };
            if let Ok(counter) = suffix.parse::<u64>() {
                max_child = max_child.max(counter);
            }
        }
    }
    Ok(format!("{child_prefix}{}", max_child + 1))
}

fn event_issue_ids(event: &BeadEventRecordWire) -> Vec<String> {
    let mut ids = vec![event.issue_id.clone()];
    if let BeadEventPayloadWire::IssueCreated { issue } = &event.payload {
        ids.push(issue.id.clone());
    }
    ids
}

fn validate_append_only_branch(
    base: &BeadEventStreamWire,
    branch: &BeadEventStreamWire,
    branch_name: &str,
) -> Result<BTreeSet<usize>, BeadError> {
    let mut matched_indexes = BTreeSet::new();
    let mut branch_start = 0;
    for (base_index, base_event) in base.events.iter().enumerate() {
        if branch.events.iter().any(|branch_event| {
            branch_event.event_id == base_event.event_id
                && branch_event != base_event
        }) {
            return Err(BeadError::validation(format!(
                "cannot merge non-append-only bead event stream {}: {branch_name} rewrote base event {}",
                base.stream_id,
                base_index + 1
            )));
        }
        let Some(offset) = branch.events[branch_start..]
            .iter()
            .position(|branch_event| branch_event == base_event)
        else {
            return Err(BeadError::validation(format!(
                "cannot merge non-append-only bead event stream {}: {branch_name} missing base event {}",
                base.stream_id,
                base_index + 1
            )));
        };
        let branch_index = branch_start + offset;
        matched_indexes.insert(branch_index);
        branch_start = branch_index + 1;
    }
    Ok(matched_indexes)
}

fn event_union_key(
    event: &BeadEventRecordWire,
    serialized: &str,
) -> (String, usize, String, String) {
    (
        event.timestamp.clone(),
        event_operation_priority(event.operation),
        event.event_id.clone(),
        serialized.to_string(),
    )
}

fn event_keys(
    events: &[BeadEventRecordWire],
) -> Result<BTreeSet<String>, BeadError> {
    events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(BeadError::from)
}

pub(super) fn mint_bead_event_id(
    stream_id: &str,
    ordinal: usize,
    timestamp: &str,
    actor: &str,
    operation: BeadEventOperationWire,
    issue_id: &str,
    payload: &BeadEventPayloadWire,
) -> Result<String, BeadError> {
    let operation_label = serde_json::to_string(&operation)?
        .trim_matches('"')
        .to_string();
    let content = serde_json::to_vec(&(
        BEAD_EVENT_SCHEMA_VERSION,
        timestamp,
        actor,
        operation,
        issue_id,
        payload,
    ))?;
    let digest = hex::encode(Sha256::digest(content));
    Ok(format!(
        "{stream_id}:{ordinal:06}:{operation_label}:{issue_id}:{digest}"
    ))
}

/// Interleave events from every stream into one deterministic apply order.
///
/// Events within a stream must apply in recorded order: stream merges append
/// events whose timestamps can predate earlier entries, so intra-stream
/// position is the causal order while timestamps only decide how independent
/// streams interleave. No single comparator can express both rules (mixing
/// index order with timestamp order is not a total order), so a k-way merge
/// keeps one cursor per stream and always emits the smallest head event by
/// (timestamp, operation priority, event_id, stream index).
pub(super) fn merge_stream_events(
    streams: &[BeadEventStreamWire],
) -> Vec<&BeadEventRecordWire> {
    let mut heads: BinaryHeap<Reverse<StreamHead<'_>>> = streams
        .iter()
        .enumerate()
        .filter_map(|(stream_index, stream)| {
            stream.events.first().map(|event| {
                Reverse(StreamHead {
                    event,
                    stream_index,
                    event_index: 0,
                })
            })
        })
        .collect();
    let mut ordered = Vec::with_capacity(
        streams.iter().map(|stream| stream.events.len()).sum(),
    );
    while let Some(Reverse(head)) = heads.pop() {
        ordered.push(head.event);
        let event_index = head.event_index + 1;
        if let Some(event) = streams[head.stream_index].events.get(event_index)
        {
            heads.push(Reverse(StreamHead {
                event,
                stream_index: head.stream_index,
                event_index,
            }));
        }
    }
    ordered
}

struct StreamHead<'a> {
    event: &'a BeadEventRecordWire,
    stream_index: usize,
    event_index: usize,
}

impl StreamHead<'_> {
    fn merge_key(&self) -> (&str, usize, &str, usize) {
        (
            self.event.timestamp.as_str(),
            event_operation_priority(self.event.operation),
            self.event.event_id.as_str(),
            self.stream_index,
        )
    }
}

impl PartialEq for StreamHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.merge_key() == other.merge_key()
    }
}

impl Eq for StreamHead<'_> {}

impl PartialOrd for StreamHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamHead<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.merge_key().cmp(&other.merge_key())
    }
}

fn event_operation_priority(operation: BeadEventOperationWire) -> usize {
    match operation {
        BeadEventOperationWire::IssueCreated => 0,
        BeadEventOperationWire::DependencyAdded => 2,
        BeadEventOperationWire::DependencyRemoved => 3,
        BeadEventOperationWire::ReferenceAdded => 4,
        BeadEventOperationWire::ReferenceRemoved => 5,
        _ => 1,
    }
}

pub(super) fn apply_event(
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
            issue.dependencies.clear();
            issue.refs.clear();
            issue.plus_one_evidence.clear();
            issue.close_history.clear();
            issues.insert(issue.id.clone(), issue);
        }
        BeadEventPayloadWire::IssueUpdated { fields } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            apply_update_event_fields(issue, fields, &event.timestamp);
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::NoteAppended { entry } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.notes = appended_note_text(
                &issue.notes,
                &event.timestamp,
                &event.actor,
                entry,
            );
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
            if matches!(issue.status, StatusWire::Open | StatusWire::Closed) {
                issue.status = StatusWire::Ready;
                archive_close_metadata(
                    issue,
                    &event.timestamp,
                    BeadReopenCauseWire::PlusOne,
                    Some(evidence.reporter.clone()),
                );
            }
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
    }
    Ok(())
}

fn apply_update_event_fields(
    issue: &mut IssueWire,
    fields: &BeadIssueUpdateEventFieldsWire,
    timestamp: &str,
) {
    if let Some(value) = &fields.title {
        issue.title = value.clone();
    }
    if let Some(value) = &fields.status {
        issue.status = value.clone();
    }
    if let Some(value) = &fields.assignee {
        issue.assignee = value.clone();
    }
    if let Some(value) = &fields.description {
        issue.description = value.clone();
    }
    if let Some(value) = &fields.notes {
        issue.notes = value.clone();
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
    if let Some(value) = &fields.tier {
        issue.tier = Some(value.clone());
    }
    if let Some(value) = fields.is_ready_to_work {
        issue.is_ready_to_work = value;
    }
    if fields
        .status
        .as_ref()
        .is_some_and(|status| *status != StatusWire::Closed)
    {
        archive_close_metadata(
            issue,
            timestamp,
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
pub(super) fn archive_close_metadata(
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

pub(super) fn appended_note_text(
    existing: &str,
    timestamp: &str,
    actor: &str,
    entry: &str,
) -> String {
    let appended = format!("[{timestamp} · {actor}] {}", entry.trim());
    if existing.trim().is_empty() {
        appended
    } else {
        format!("{}\n\n{appended}", existing.trim_end())
    }
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

fn event_issue_key(issue: &IssueWire) -> (u8, String) {
    let kind_order = match issue.issue_type {
        IssueTypeWire::Plan => 0,
        IssueTypeWire::Phase => 1,
        IssueTypeWire::Task => 2,
    };
    (kind_order, issue.id.clone())
}

fn root_issue_ids(issues: &[IssueWire]) -> BTreeMap<String, String> {
    let mut roots = BTreeMap::new();
    let ids: BTreeSet<&str> =
        issues.iter().map(|issue| issue.id.as_str()).collect();
    for issue in issues {
        let root = if matches!(
            issue.issue_type,
            IssueTypeWire::Plan | IssueTypeWire::Task
        ) {
            issue.id.clone()
        } else {
            issue
                .parent_id
                .as_deref()
                .filter(|parent_id| ids.contains(parent_id))
                .unwrap_or(&issue.id)
                .to_string()
        };
        roots.insert(issue.id.clone(), root);
    }
    roots
}

struct PendingEvent {
    timestamp: String,
    actor: String,
    operation: BeadEventOperationWire,
    issue_id: String,
    payload: BeadEventPayloadWire,
}

impl PendingEvent {
    fn created(issue: &IssueWire) -> Self {
        let mut issue = issue.clone();
        issue.dependencies.clear();
        issue.refs.clear();
        issue.plus_one_evidence.clear();
        Self {
            timestamp: event_timestamp(&issue.created_at, &issue.updated_at),
            actor: issue.created_by.clone(),
            operation: BeadEventOperationWire::IssueCreated,
            issue_id: issue.id.clone(),
            payload: BeadEventPayloadWire::IssueCreated { issue },
        }
    }

    fn dependency_added(dependency: DependencyWire) -> Self {
        Self {
            timestamp: event_timestamp(&dependency.created_at, ""),
            actor: dependency.created_by.clone(),
            operation: BeadEventOperationWire::DependencyAdded,
            issue_id: dependency.issue_id.clone(),
            payload: BeadEventPayloadWire::DependencyAdded { dependency },
        }
    }

    fn reference_added(issue: &IssueWire, reference: String) -> Self {
        Self {
            timestamp: event_timestamp(&issue.created_at, &issue.updated_at),
            actor: issue.created_by.clone(),
            operation: BeadEventOperationWire::ReferenceAdded,
            issue_id: issue.id.clone(),
            payload: BeadEventPayloadWire::ReferenceAdded { reference },
        }
    }

    fn task_plus_one_recorded(
        issue: &IssueWire,
        evidence: TaskPlusOneEvidenceWire,
    ) -> Self {
        Self {
            timestamp: evidence.timestamp.clone(),
            actor: evidence.reporter.clone(),
            operation: BeadEventOperationWire::TaskPlusOneRecorded,
            issue_id: issue.id.clone(),
            payload: BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
        }
    }

    fn into_record(
        self,
        stream_id: &str,
        ordinal: usize,
    ) -> Result<BeadEventRecordWire, BeadError> {
        let event_id = mint_bead_event_id(
            stream_id,
            ordinal,
            &self.timestamp,
            &self.actor,
            self.operation,
            &self.issue_id,
            &self.payload,
        )?;
        Ok(BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id,
            timestamp: self.timestamp,
            actor: self.actor,
            operation: self.operation,
            issue_id: self.issue_id,
            payload: self.payload,
        })
    }
}

fn event_timestamp(primary: &str, fallback: &str) -> String {
    if !primary.is_empty() {
        primary.to_string()
    } else if !fallback.is_empty() {
        fallback.to_string()
    } else {
        "1970-01-01T00:00:00Z".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue_with_refs(refs: Vec<String>) -> IssueWire {
        IssueWire {
            id: "sase-1".to_string(),
            title: "Plan".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            parent_id: None,
            owner: "owner@example.com".to_string(),
            assignee: String::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            created_by: "owner@example.com".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs,
            plus_one_evidence: Vec::new(),
            model: String::new(),
            size: None,
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: Vec::new(),
        }
    }

    fn reference_event(
        event_id: &str,
        operation: BeadEventOperationWire,
        reference: &str,
    ) -> BeadEventRecordWire {
        let payload = match operation {
            BeadEventOperationWire::ReferenceAdded => {
                BeadEventPayloadWire::ReferenceAdded {
                    reference: reference.to_string(),
                }
            }
            BeadEventOperationWire::ReferenceRemoved => {
                BeadEventPayloadWire::ReferenceRemoved {
                    reference: reference.to_string(),
                }
            }
            _ => panic!("reference_event requires a reference operation"),
        };
        BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: event_id.to_string(),
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            actor: "owner@example.com".to_string(),
            operation,
            issue_id: "sase-1".to_string(),
            payload,
        }
    }

    fn task_issue() -> IssueWire {
        let mut issue = issue_with_refs(Vec::new());
        issue.id = "sase-task".to_string();
        issue.title = "Task".to_string();
        issue.issue_type = IssueTypeWire::Task;
        issue.tier = None;
        issue.created_by = "creator-agent".to_string();
        issue.size = Some(PhaseSizeWire::Small);
        issue
    }

    fn plus_one_event(event_id: &str, reporter: &str) -> BeadEventRecordWire {
        let evidence = TaskPlusOneEvidenceWire {
            timestamp: "2026-01-02T00:00:00Z".to_string(),
            reporter: reporter.to_string(),
            note: "independent reproduction".to_string(),
            refs: Vec::new(),
        };
        BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: event_id.to_string(),
            timestamp: evidence.timestamp.clone(),
            actor: reporter.to_string(),
            operation: BeadEventOperationWire::TaskPlusOneRecorded,
            issue_id: "sase-task".to_string(),
            payload: BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
        }
    }

    #[test]
    fn three_way_merge_preserves_independent_plus_ones_and_deduplicates_reporter(
    ) {
        let created = PendingEvent::created(&task_issue())
            .into_record("sase-task", 1)
            .unwrap();
        let base = BeadEventStreamWire {
            stream_id: "sase-task".to_string(),
            root_issue_id: "sase-task".to_string(),
            events: vec![created],
        };
        let mut ours = base.clone();
        ours.events.push(plus_one_event("ours-a", "agent-a"));
        ours.events.push(plus_one_event("ours-a-retry", "agent-a"));
        let mut theirs = base.clone();
        theirs.events.push(plus_one_event("theirs-b", "agent-b"));

        let merged = merge_bead_event_streams(&base, &ours, &theirs).unwrap();
        let issues = reduce_event_streams(&[merged]).unwrap();

        assert_eq!(issues[0].plus_one_count(), 2);
        assert_eq!(issues[0].status, StatusWire::Ready);
        assert_eq!(
            issues[0]
                .plus_one_evidence
                .iter()
                .map(|evidence| evidence.reporter.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["agent-a", "agent-b"])
        );
    }

    #[test]
    fn redundant_close_is_an_exact_no_op() {
        let mut issues = BTreeMap::from([(
            "sase-1".to_string(),
            issue_with_refs(Vec::new()),
        )]);
        let first_close = BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: "first-close".to_string(),
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            actor: "owner@example.com".to_string(),
            operation: BeadEventOperationWire::IssueClosed,
            issue_id: "sase-1".to_string(),
            payload: BeadEventPayloadWire::IssueClosed {
                close_reason: Some("shipped".to_string()),
                resolution: Some(BeadResolutionWire::Done),
                forced_descendant_ids: Vec::new(),
            },
        };
        let redundant_close = BeadEventRecordWire {
            event_id: "redundant-close".to_string(),
            timestamp: "2026-01-01T00:02:00Z".to_string(),
            payload: BeadEventPayloadWire::IssueClosed {
                close_reason: None,
                resolution: Some(BeadResolutionWire::Canceled),
                forced_descendant_ids: Vec::new(),
            },
            ..first_close.clone()
        };

        apply_event(&mut issues, &first_close).unwrap();
        let first_projection = issues["sase-1"].clone();
        apply_event(&mut issues, &redundant_close).unwrap();

        assert_eq!(issues["sase-1"], first_projection);
    }

    #[test]
    fn note_append_validation_and_rendering_are_owned_by_the_event() {
        let mut issues = BTreeMap::from([(
            "sase-1".to_string(),
            issue_with_refs(Vec::new()),
        )]);
        let note = BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: "note".to_string(),
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            actor: "agent-1".to_string(),
            operation: BeadEventOperationWire::NoteAppended,
            issue_id: "sase-1".to_string(),
            payload: BeadEventPayloadWire::NoteAppended {
                entry: " verified ".to_string(),
            },
        };

        apply_event(&mut issues, &note).unwrap();

        assert_eq!(
            issues["sase-1"].notes,
            "[2026-01-01T00:01:00Z · agent-1] verified"
        );
        assert_eq!(issues["sase-1"].updated_at, "2026-01-01T00:01:00Z");

        let blank = BeadEventRecordWire {
            payload: BeadEventPayloadWire::NoteAppended {
                entry: " \t ".to_string(),
            },
            ..note
        };
        assert_eq!(
            blank.validate().unwrap_err().message,
            "note_appended entry cannot be empty or blank"
        );
    }

    #[test]
    fn refs_import_as_individual_events_and_replay_idempotently() {
        let issue = issue_with_refs(vec![
            "research:202607/report.md".to_string(),
            "bead:sase-bb.1".to_string(),
        ]);
        let mut streams =
            import_issues_to_event_streams(std::slice::from_ref(&issue))
                .unwrap();
        assert_eq!(
            streams[0]
                .events
                .iter()
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            vec![
                BeadEventOperationWire::IssueCreated,
                BeadEventOperationWire::ReferenceAdded,
                BeadEventOperationWire::ReferenceAdded,
            ]
        );
        let BeadEventPayloadWire::IssueCreated { issue: created } =
            &streams[0].events[0].payload
        else {
            panic!("first event should create the issue");
        };
        assert!(created.refs.is_empty());

        streams[0].events.extend([
            reference_event(
                "duplicate-add",
                BeadEventOperationWire::ReferenceAdded,
                "research:202607/report.md",
            ),
            reference_event(
                "absent-remove",
                BeadEventOperationWire::ReferenceRemoved,
                "bead:sase-missing",
            ),
        ]);
        assert_eq!(reduce_event_streams(&streams).unwrap(), vec![issue]);

        streams[0].events.push(reference_event(
            "real-remove",
            BeadEventOperationWire::ReferenceRemoved,
            "research:202607/report.md",
        ));
        assert_eq!(
            reduce_event_streams(&streams).unwrap()[0].refs,
            vec!["bead:sase-bb.1"]
        );
    }

    /// Build one side of a concurrent mint: same id, different bead.
    fn colliding_stream(
        issue_id: &str,
        title: &str,
        created_at: &str,
    ) -> BeadEventStreamWire {
        let mut issue = issue_with_refs(Vec::new());
        issue.id = issue_id.to_string();
        issue.title = title.to_string();
        issue.created_at = created_at.to_string();
        issue.updated_at = created_at.to_string();
        let created = PendingEvent::created(&issue)
            .into_record(issue_id, 1)
            .unwrap();
        BeadEventStreamWire {
            stream_id: issue_id.to_string(),
            root_issue_id: issue_id.to_string(),
            events: vec![created],
        }
    }

    fn empty_stream(stream_id: &str) -> BeadEventStreamWire {
        BeadEventStreamWire {
            stream_id: stream_id.to_string(),
            root_issue_id: stream_id.to_string(),
            events: Vec::new(),
        }
    }

    #[test]
    fn concurrently_minted_bead_id_relocates_instead_of_wedging_the_store() {
        let base = empty_stream("sase-ey");
        let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
        let theirs =
            colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

        let outcome = merge_bead_event_streams_with_relocation(
            &base,
            &ours,
            &theirs,
            Some("sase-ez"),
        )
        .unwrap();

        assert_eq!(
            outcome.relocations,
            vec![("sase-ey".to_string(), "sase-ez".to_string())]
        );
        let relocated = outcome.relocated.clone().unwrap();
        assert_eq!(relocated.stream_id, "sase-ez");
        // Both beads survive: the older creation keeps the contested id.
        let issues =
            reduce_event_streams(&[outcome.merged.clone(), relocated]).unwrap();
        assert_eq!(
            issues
                .iter()
                .map(|issue| (issue.id.as_str(), issue.title.as_str()))
                .collect::<Vec<_>>(),
            vec![("sase-ey", "Ours"), ("sase-ez", "Theirs")]
        );
    }

    #[test]
    fn relocation_picks_the_same_loser_whichever_side_git_calls_ours() {
        let base = empty_stream("sase-ey");
        let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
        let theirs =
            colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

        let forward = merge_bead_event_streams_with_relocation(
            &base,
            &ours,
            &theirs,
            Some("sase-ez"),
        )
        .unwrap();
        let swapped = merge_bead_event_streams_with_relocation(
            &base,
            &theirs,
            &ours,
            Some("sase-ez"),
        )
        .unwrap();

        assert_eq!(forward, swapped);
    }

    #[test]
    fn relocated_events_are_reminted_onto_their_new_stream() {
        let base = empty_stream("sase-ey");
        let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
        let theirs =
            colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

        let outcome = merge_bead_event_streams_with_relocation(
            &base,
            &ours,
            &theirs,
            Some("sase-ez"),
        )
        .unwrap();

        let relocated = outcome.relocated.unwrap();
        let event = &relocated.events[0];
        assert_eq!(event.issue_id, "sase-ez");
        assert!(event.event_id.starts_with("sase-ez:000001:"));
        assert!(outcome
            .merged
            .events
            .iter()
            .all(|kept| kept.event_id != event.event_id));
    }

    #[test]
    fn merging_without_a_relocation_id_still_reports_the_duplicate() {
        let base = empty_stream("sase-ey");
        let ours = colliding_stream("sase-ey", "Ours", "2026-08-03T11:00:00Z");
        let theirs =
            colliding_stream("sase-ey", "Theirs", "2026-08-03T11:00:01Z");

        assert_eq!(
            merge_bead_event_streams(&base, &ours, &theirs)
                .unwrap_err()
                .message,
            "duplicate issue_created event for sase-ey"
        );
    }

    #[test]
    fn concurrently_minted_child_id_renumbers_to_a_free_sibling() {
        let base = colliding_stream("sase-ey", "Epic", "2026-08-03T11:00:00Z");
        let mut ours = base.clone();
        ours.events.push(child_created(
            "sase-ey",
            "sase-ey.1",
            "Phase ours",
            "2026-08-03T11:01:00Z",
            2,
        ));
        let mut theirs = base.clone();
        theirs.events.push(child_created(
            "sase-ey",
            "sase-ey.1",
            "Phase theirs",
            "2026-08-03T11:01:01Z",
            2,
        ));

        let outcome = merge_bead_event_streams_with_relocation(
            &base,
            &ours,
            &theirs,
            Some("sase-ez"),
        )
        .unwrap();

        assert!(outcome.relocated.is_none());
        assert_eq!(
            outcome.relocations,
            vec![("sase-ey.1".to_string(), "sase-ey.2".to_string())]
        );
        let issues = reduce_event_streams(&[outcome.merged]).unwrap();
        assert_eq!(
            issues
                .iter()
                .map(|issue| (issue.id.as_str(), issue.title.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("sase-ey", "Epic"),
                ("sase-ey.1", "Phase ours"),
                ("sase-ey.2", "Phase theirs"),
            ]
        );
    }

    fn child_created(
        stream_id: &str,
        issue_id: &str,
        title: &str,
        created_at: &str,
        ordinal: usize,
    ) -> BeadEventRecordWire {
        let mut issue = issue_with_refs(Vec::new());
        issue.id = issue_id.to_string();
        issue.title = title.to_string();
        issue.issue_type = IssueTypeWire::Phase;
        issue.tier = None;
        issue.parent_id = Some(stream_id.to_string());
        issue.size = Some(PhaseSizeWire::Small);
        issue.created_at = created_at.to_string();
        issue.updated_at = created_at.to_string();
        PendingEvent::created(&issue)
            .into_record(stream_id, ordinal)
            .unwrap()
    }
}
