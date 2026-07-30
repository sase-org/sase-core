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
    BeadError, BeadResolutionWire, BeadTierWire, DependencyWire, IssueTypeWire,
    IssueWire, PhaseSizeWire, StatusWire,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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

pub fn merge_bead_event_streams(
    base: &BeadEventStreamWire,
    ours: &BeadEventStreamWire,
    theirs: &BeadEventStreamWire,
) -> Result<BeadEventStreamWire, BeadError> {
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
    let mut additions: BTreeMap<String, BeadEventRecordWire> = BTreeMap::new();
    for (branch, base_indexes) in
        [(ours, &ours_base_indexes), (theirs, &theirs_base_indexes)]
    {
        for (index, event) in branch.events.iter().enumerate() {
            if base_indexes.contains(&index) {
                continue;
            }
            let key = serde_json::to_string(event)?;
            if base_events.contains(&key) {
                continue;
            }
            additions.entry(key).or_insert_with(|| event.clone());
        }
    }

    let mut additions = additions.into_iter().collect::<Vec<_>>();
    additions
        .sort_by_key(|(serialized, event)| event_union_key(event, serialized));
    let mut merged = base.clone();
    merged
        .events
        .extend(additions.into_iter().map(|(_, event)| event));
    merged.validate()?;
    Ok(merged)
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
            issues.insert(issue.id.clone(), issue);
        }
        BeadEventPayloadWire::IssueUpdated { fields } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            apply_update_event_fields(issue, fields);
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
            clear_close_metadata(issue);
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
            clear_close_metadata(issue);
            issue.assignee = agent_name.clone();
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
    }
    Ok(())
}

fn apply_update_event_fields(
    issue: &mut IssueWire,
    fields: &BeadIssueUpdateEventFieldsWire,
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
        clear_close_metadata(issue);
    }
}

fn clear_close_metadata(issue: &mut IssueWire) {
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
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs,
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
}
