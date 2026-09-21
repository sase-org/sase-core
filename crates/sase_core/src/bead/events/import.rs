//! Legacy-snapshot import into bead event streams.
//!
//! Owns `import_issues_to_event_streams` and the `PendingEvent`
//! builder it folds into per-stream records.

use std::collections::{BTreeMap, BTreeSet};

use crate::artifact_link::BeadLinkWire;

use super::super::wire::{
    BeadError, DependencyWire, IssueTypeWire, IssueWire,
    TaskPlusOneEvidenceWire,
};
use super::merge::mint_bead_event_id;
use super::wire::{
    BeadEventOperationWire, BeadEventPayloadWire, BeadEventRecordWire,
    BeadEventStreamWire, BEAD_EVENT_SCHEMA_VERSION,
};

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
        for link in &issue.links {
            streams
                .entry(stream_id.clone())
                .or_default()
                .push(PendingEvent::link_added(issue, link.clone()));
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

pub(super) struct PendingEvent {
    timestamp: String,
    actor: String,
    operation: BeadEventOperationWire,
    issue_id: String,
    payload: BeadEventPayloadWire,
}

impl PendingEvent {
    pub(super) fn created(issue: &IssueWire) -> Self {
        let mut issue = issue.clone();
        issue.dependencies.clear();
        issue.refs.clear();
        issue.links.clear();
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

    fn link_added(issue: &IssueWire, link: BeadLinkWire) -> Self {
        Self {
            timestamp: event_timestamp(&issue.created_at, &issue.updated_at),
            actor: issue.created_by.clone(),
            operation: BeadEventOperationWire::LinkAdded,
            issue_id: issue.id.clone(),
            payload: BeadEventPayloadWire::LinkAdded {
                target_ref: link.target_ref,
                relation: link.relation,
                description: link.description,
                origin: link.origin,
                direction: link.direction,
                uses: link.uses,
                operation_id: None,
            },
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

    pub(super) fn into_record(
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
