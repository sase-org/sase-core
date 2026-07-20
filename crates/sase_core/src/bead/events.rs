//! Append-only bead event wire records and pure reducers.
//!
//! Phase 1 keeps this module side-effect free: callers can import legacy
//! `IssueWire` snapshots into deterministic streams, then reduce streams back
//! into the current snapshot model. Later phases own filesystem integration.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};

use super::wire::{
    BeadError, BeadTierWire, DependencyWire, IssueTypeWire, IssueWire,
    PhaseSizeWire, StatusWire,
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
    IssueOpened,
    IssueClosed,
    IssueRemoved,
    DependencyAdded,
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
    IssueOpened,
    IssueClosed {
        close_reason: Option<String>,
    },
    IssueRemoved {
        #[serde(default)]
        cascade_removed_issue_ids: Vec<String>,
    },
    DependencyAdded {
        dependency: DependencyWire,
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
    let mut stream_ids = BTreeSet::new();
    let mut issues: BTreeMap<String, IssueWire> = BTreeMap::new();

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

    for event in merge_stream_events(&streams) {
        apply_event(&mut issues, event)?;
    }

    let mut reduced: Vec<IssueWire> = issues.into_values().collect();
    reduced.sort_by_key(event_issue_key);
    for issue in &reduced {
        issue.validate()?;
    }
    Ok(reduced)
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

    let mut merged = theirs.clone();
    let base_events = event_keys(&base.events)?;
    let theirs_events = event_keys(&theirs.events)?;
    let mut ordinal = merged.events.len();
    for event in &ours.events {
        let event_key = serde_json::to_string(event)?;
        if base_events.contains(&event_key)
            || theirs_events.contains(&event_key)
        {
            continue;
        }
        ordinal += 1;
        merged
            .events
            .push(renumber_event(event, &merged.stream_id, ordinal)?);
    }
    merged.validate()?;
    Ok(merged)
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

fn renumber_event(
    event: &BeadEventRecordWire,
    stream_id: &str,
    ordinal: usize,
) -> Result<BeadEventRecordWire, BeadError> {
    let operation_label = serde_json::to_string(&event.operation)?
        .trim_matches('"')
        .to_string();
    let mut event = event.clone();
    event.event_id = format!(
        "{stream_id}:{ordinal:06}:{operation_label}:{}",
        event.issue_id
    );
    event.validate()?;
    Ok(event)
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
fn merge_stream_events(
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
        _ => 1,
    }
}

fn apply_event(
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
            issues.insert(issue.id.clone(), issue);
        }
        BeadEventPayloadWire::IssueUpdated { fields } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            apply_update_event_fields(issue, fields);
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::IssueOpened => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.status = StatusWire::Open;
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
        }
        BeadEventPayloadWire::IssueClosed { close_reason } => {
            let issue = existing_issue_mut(issues, &event.issue_id)?;
            issue.status = StatusWire::Closed;
            issue.closed_at = Some(event.timestamp.clone());
            issue.close_reason = close_reason.clone();
            issue.updated_at = event.timestamp.clone();
            issue.validate()?;
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
    };
    (kind_order, issue.id.clone())
}

fn root_issue_ids(issues: &[IssueWire]) -> BTreeMap<String, String> {
    let mut roots = BTreeMap::new();
    let ids: BTreeSet<&str> =
        issues.iter().map(|issue| issue.id.as_str()).collect();
    for issue in issues {
        let root = if issue.issue_type == IssueTypeWire::Plan {
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

    fn into_record(
        self,
        stream_id: &str,
        ordinal: usize,
    ) -> Result<BeadEventRecordWire, BeadError> {
        let operation = serde_json::to_string(&self.operation)?
            .trim_matches('"')
            .to_string();
        Ok(BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id: format!(
                "{stream_id}:{ordinal:06}:{operation}:{}",
                self.issue_id
            ),
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
