//! Atomic recovery for closes owned by a run whose host completion failed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::DateTime;
use serde::{Deserialize, Serialize};

use super::close_remove::reopen_closed_ancestors_via_view;
use super::runner::{run_mutation, MutationStep};
use super::view::MutationView;
use crate::bead::events::{
    archive_close_metadata, clear_snooze_record, BeadEventOperationWire,
    BeadEventPayloadWire, BeadEventRecordWire,
};
use crate::bead::wire::{
    BeadCloseProvenanceWire, BeadCompletionFailureWire, BeadError,
    BeadNoteWire, BeadReopenCauseWire, BeadResolutionWire, IssueWire,
    StatusWire,
};

/// Evidence collected by the host after a terminal completion failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadFailedCloseRecoveryRequestWire {
    pub run_id: String,
    pub failed_at: String,
    pub failure_code: String,
    #[serde(default)]
    pub evidence_locations: Vec<String>,
    pub note: String,
    pub actor: String,
}

impl BeadFailedCloseRecoveryRequestWire {
    fn validate(&self) -> Result<(), BeadError> {
        for (field, value) in [
            ("run_id", &self.run_id),
            ("failure_code", &self.failure_code),
            ("actor", &self.actor),
        ] {
            if value.trim().is_empty() {
                return Err(BeadError::validation(format!(
                    "failed close recovery {field} cannot be empty or blank"
                )));
            }
        }
        DateTime::parse_from_rfc3339(self.failed_at.trim()).map_err(|error| {
            BeadError::validation(format!(
                "failed close recovery failed_at must be an RFC-3339 timestamp: {:?} ({error})",
                self.failed_at
            ))
        })?;
        if self.note.trim().is_empty()
            || !self.note.starts_with("STITCH FAILED:")
        {
            return Err(BeadError::validation(
                "failed close recovery note must start with 'STITCH FAILED:' and contain text",
            ));
        }
        if self
            .evidence_locations
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(BeadError::validation(
                "failed close recovery evidence locations cannot be empty or blank",
            ));
        }
        Ok(())
    }
}

/// One close episode returned by the recovery mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadFailedCloseRecoveryEntryWire {
    pub issue_id: String,
    pub close_event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Recovery report. `skipped` carries a reason for every close not changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadFailedCloseRecoveryResultWire {
    #[serde(default)]
    pub lock_wait_ms: u64,
    pub changed: bool,
    #[serde(default)]
    pub recovered: Vec<BeadFailedCloseRecoveryEntryWire>,
    #[serde(default)]
    pub already_applied: Vec<BeadFailedCloseRecoveryEntryWire>,
    #[serde(default)]
    pub skipped: Vec<BeadFailedCloseRecoveryEntryWire>,
    #[serde(default)]
    pub reopened_ancestor_ids: Vec<String>,
}

impl super::store::MutationLockWait for BeadFailedCloseRecoveryResultWire {
    fn set_lock_wait_ms(&mut self, lock_wait_ms: u64) {
        self.lock_wait_ms = lock_wait_ms;
    }
}

/// Reopen every still-current `done` close attributed to `request.run_id`.
///
/// Discovery, eligibility, close-history archival, ancestor reopening,
/// structured facts, and the `STITCH FAILED:` note share one locked mutation.
pub fn recover_failed_closes(
    beads_dir: &Path,
    request: BeadFailedCloseRecoveryRequestWire,
) -> Result<BeadFailedCloseRecoveryResultWire, BeadError> {
    request.validate()?;
    run_mutation(beads_dir, "recover_failed_closes", |view| {
        run_recovery(view, &request)
    })
}

fn run_recovery(
    view: &mut MutationView,
    request: &BeadFailedCloseRecoveryRequestWire,
) -> Result<MutationStep<BeadFailedCloseRecoveryResultWire>, BeadError> {
    let streams = view.all_event_streams()?;
    let mut close_events = Vec::new();
    for stream in streams {
        for event in stream.events {
            if let BeadEventPayloadWire::IssueClosed {
                resolution,
                close_provenance: Some(provenance),
                ..
            } = &event.payload
            {
                if provenance.run_id == request.run_id {
                    let resolution = resolution.clone();
                    let provenance = provenance.clone();
                    close_events.push((event, resolution, provenance));
                }
            }
        }
    }
    close_events.sort_by(|left, right| left.0.event_id.cmp(&right.0.event_id));

    let mut result = BeadFailedCloseRecoveryResultWire {
        lock_wait_ms: 0,
        changed: false,
        recovered: Vec::new(),
        already_applied: Vec::new(),
        skipped: Vec::new(),
        reopened_ancestor_ids: Vec::new(),
    };
    let mut expected_ids = BTreeSet::new();
    let mut reopened_ancestors = BTreeMap::<String, IssueWire>::new();
    let mut eligible = Vec::new();

    // Classify every close against the pre-mutation view so recovering a
    // descendant cannot hide an eligible ancestor as `close_not_current`.
    for (event, resolution, provenance) in close_events {
        let entry = BeadFailedCloseRecoveryEntryWire {
            issue_id: event.issue_id.clone(),
            close_event_id: event.event_id.clone(),
            reason: None,
        };
        let issue = match view.get(&event.issue_id) {
            Ok(issue) => issue,
            Err(_) => {
                result.skipped.push(with_reason(entry, "bead_removed"));
                continue;
            }
        };

        if issue.completion_failure.as_ref().is_some_and(|failure| {
            failure.close_event_id == event.event_id
                && failure.run_id == request.run_id
        }) {
            result.already_applied.push(entry);
            continue;
        }
        if resolution != Some(BeadResolutionWire::Done) {
            result
                .skipped
                .push(with_reason(entry, "resolution_not_done"));
            continue;
        }
        if !close_is_current(&issue, &event, &provenance) {
            result.skipped.push(with_reason(entry, "close_not_current"));
            continue;
        }
        eligible.push((event, provenance, issue, entry));
    }

    let mut recovered_ids = BTreeSet::new();
    for (event, provenance, issue, entry) in eligible {
        let failure = BeadCompletionFailureWire {
            close_event_id: event.event_id.clone(),
            run_id: provenance.run_id.clone(),
            agent_name: provenance.agent_name.clone(),
            artifact_dir: provenance.artifact_dir.clone(),
            failed_at: request.failed_at.clone(),
            evidence_locations: request.evidence_locations.clone(),
            failure_code: request.failure_code.clone(),
        };
        failure.validate()?;
        let mut issue = issue;
        issue.status = StatusWire::Open;
        archive_close_metadata(
            &mut issue,
            &request.failed_at,
            BeadReopenCauseWire::CompletionFailure,
            Some(request.actor.clone()),
        );
        clear_snooze_record(&mut issue);
        issue.completion_failure = Some(failure.clone());
        issue.updated_at = request.failed_at.clone();
        issue.validate()?;
        view.stage_issue(issue.clone());

        let note = format!(
            "{}\nBead: {}\nClose event: {}",
            request.note, issue.id, event.event_id
        );
        let Some(recovery_event_id) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::CompletionFailureRecovered,
            BeadEventPayloadWire::CompletionFailureRecovered {
                failure,
                note: note.clone(),
            },
            &request.failed_at,
            &request.actor,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        if let Some(note_row) = BeadNoteWire::from_event(
            &recovery_event_id,
            &request.failed_at,
            &request.actor,
            &note,
            Vec::new(),
        ) {
            issue.notes.push(note_row);
        }
        issue.validate()?;
        view.stage_issue(issue.clone());
        expected_ids.insert(issue.id.clone());
        recovered_ids.insert(issue.id.clone());
        result.recovered.push(entry);
    }

    if result.recovered.is_empty() {
        result.already_applied.sort_by(|left, right| {
            left.close_event_id.cmp(&right.close_event_id)
        });
        result.skipped.sort_by(|left, right| {
            left.close_event_id.cmp(&right.close_event_id)
        });
        return Ok(MutationStep::Done(result));
    }

    // Eligible ancestors already carry CompletionFailureRecovered. Walk
    // remaining closed ancestors only after that, and never mint a later
    // ordinary IssueOpened that would archive their recovery fact.
    for recovered_id in &recovered_ids {
        for ancestor in reopen_closed_ancestors_via_view(
            view,
            recovered_id,
            &request.failed_at,
        )? {
            if recovered_ids.contains(&ancestor.id) {
                continue;
            }
            expected_ids.insert(ancestor.id.clone());
            reopened_ancestors.insert(ancestor.id.clone(), ancestor);
        }
    }

    for ancestor in reopened_ancestors.values() {
        let Some(_) = view.stage_event(
            &ancestor.id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            &request.failed_at,
            &request.actor,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    let expected_ids: Vec<String> = expected_ids.into_iter().collect();
    let Some(committed) = view.commit(&expected_ids)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let committed_by_id: BTreeMap<String, IssueWire> = committed
        .into_iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect();
    result
        .recovered
        .sort_by(|left, right| left.close_event_id.cmp(&right.close_event_id));
    result
        .already_applied
        .sort_by(|left, right| left.close_event_id.cmp(&right.close_event_id));
    result
        .skipped
        .sort_by(|left, right| left.close_event_id.cmp(&right.close_event_id));
    result.reopened_ancestor_ids = reopened_ancestors.keys().cloned().collect();
    result.changed = true;
    // Ensure the commit returned every row the algorithm staged. This also
    // catches a broken cache publisher before a success report can escape.
    if expected_ids
        .iter()
        .any(|id| !committed_by_id.contains_key(id))
    {
        return Err(BeadError::io(
            "failed close recovery did not publish every staged bead row",
        ));
    }
    Ok(MutationStep::Done(result))
}

fn close_is_current(
    issue: &IssueWire,
    event: &BeadEventRecordWire,
    provenance: &BeadCloseProvenanceWire,
) -> bool {
    issue.status == StatusWire::Closed
        && issue.resolution == Some(BeadResolutionWire::Done)
        && issue.close_event_id.as_deref() == Some(event.event_id.as_str())
        && issue.closed_at.as_deref() == Some(event.timestamp.as_str())
        && issue.close_provenance.as_ref() == Some(provenance)
}

fn with_reason(
    mut entry: BeadFailedCloseRecoveryEntryWire,
    reason: &str,
) -> BeadFailedCloseRecoveryEntryWire {
    entry.reason = Some(reason.to_string());
    entry
}
