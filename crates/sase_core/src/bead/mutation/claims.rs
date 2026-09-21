use super::mutation_wire::BeadMutationOutcomeWire;
use super::mutation_wire::BeadPreclaimAssignmentWire;
use super::mutation_wire::BeadPreclaimRollbackWire;
use super::store::mutation_status_value;
use super::store::now_utc;
use super::store::outcome;
use super::store::tier_label;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadIssueUpdateEventFieldsWire;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::StatusWire;
use std::collections::HashSet;
use std::path::Path;

pub fn claim_for_agent_launch(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead launch claim cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "claim_for_launch", || {
        let mut store = MutableStore::load(beads_dir)
            .map_err(|error| durable_store_error("read", beads_dir, error))?;
        let index = store.issue_index(issue_id)?;
        if store.issues[index].status == StatusWire::Closed {
            return Err(BeadError {
                kind: "closed".to_string(),
                message: format!(
                    "cannot claim closed bead for agent launch: {issue_id}"
                ),
            });
        }

        let current = store.issues[index].clone();
        if current.status == StatusWire::InProgress
            && current.assignee == agent_name
        {
            let mut result = outcome(
                "claim_for_agent_launch",
                false,
                vec![current.id.clone()],
            );
            result.issue = Some(current);
            return Ok(result);
        }

        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::InProgress;
        store.issues[index].assignee = agent_name.to_string();
        // The event this appends is an `issue_updated` carrying a status, so
        // the reducer clears the record through `apply_update_event_fields`;
        // clearing it here too is what keeps the two projections identical.
        clear_snooze_record(&mut store.issues[index]);
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: BeadIssueUpdateEventFieldsWire {
                    status: Some(StatusWire::InProgress),
                    assignee: Some(agent_name.to_string()),
                    ..Default::default()
                },
            },
            &now,
            &issue.created_by,
        )?;
        store
            .save()
            .map_err(|error| durable_store_error("write", beads_dir, error))?;

        let mut result =
            outcome("claim_for_agent_launch", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub fn claim_for_agent_wait(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead wait claim cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "claim_for_wait", || {
        let mut store = MutableStore::load(beads_dir)
            .map_err(|error| durable_store_error("read", beads_dir, error))?;
        let index = store.issue_index(issue_id)?;
        let current = store.issues[index].clone();

        if matches!(
            current.status,
            StatusWire::Claimed | StatusWire::InProgress
        ) && current.assignee == agent_name
        {
            let mut result = outcome(
                "claim_for_agent_wait",
                false,
                vec![current.id.clone()],
            );
            result.issue = Some(current);
            return Ok(result);
        }

        if current.status != StatusWire::Open {
            let holder = if current.assignee.is_empty() {
                "<unassigned>"
            } else {
                current.assignee.as_str()
            };
            let mut result = outcome(
                "claim_for_agent_wait",
                false,
                vec![current.id.clone()],
            );
            result.message = format!(
                "cannot claim bead {issue_id} for agent wait: current status is {} and holder is {holder}",
                mutation_status_value(&current.status)
            );
            result.issue = Some(current);
            return Ok(result);
        }

        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::Claimed;
        store.issues[index].assignee = agent_name.to_string();
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: BeadIssueUpdateEventFieldsWire {
                    status: Some(StatusWire::Claimed),
                    assignee: Some(agent_name.to_string()),
                    ..Default::default()
                },
            },
            &now,
            &issue.created_by,
        )?;
        store
            .save()
            .map_err(|error| durable_store_error("write", beads_dir, error))?;

        let mut result =
            outcome("claim_for_agent_wait", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub fn release_agent_claim(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead claim release cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "release_wait_claim", || {
        let mut store = MutableStore::load(beads_dir)
            .map_err(|error| durable_store_error("read", beads_dir, error))?;
        let index = store.issue_index(issue_id)?;
        let current = store.issues[index].clone();

        if current.status != StatusWire::Claimed
            || current.assignee != agent_name
        {
            let mut result =
                outcome("release_agent_claim", false, vec![current.id.clone()]);
            result.issue = Some(current);
            return Ok(result);
        }

        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::Open;
        store.issues[index].assignee.clear();
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: BeadIssueUpdateEventFieldsWire {
                    status: Some(StatusWire::Open),
                    assignee: Some(String::new()),
                    ..Default::default()
                },
            },
            &now,
            &issue.created_by,
        )?;
        store
            .save()
            .map_err(|error| durable_store_error("write", beads_dir, error))?;

        let mut result =
            outcome("release_agent_claim", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub fn preclaim_epic_work_plan(
    beads_dir: &Path,
    epic_id: &str,
    assignments: &[BeadPreclaimAssignmentWire],
    epic_agent_name: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    with_bead_mutation_lock(beads_dir, "preclaim_epic_work", || {
        let mut store = MutableStore::load(beads_dir)?;
        let epic_index = store.issue_index(epic_id)?;
        let epic = store.issues[epic_index].clone();
        if epic.issue_type != IssueTypeWire::Plan {
            return Err(BeadError {
                kind: "not_a_plan".to_string(),
                message: format!(
                    "sase bead work preclaim only applies to epic plan beads (got phase for {epic_id})"
                ),
            });
        }
        if !matches!(epic.tier.as_ref(), Some(BeadTierWire::Epic)) {
            return Err(BeadError {
                kind: "not_workable_plan".to_string(),
                message: format!(
                    "sase bead work preclaim only applies to epic plan beads (got {} for {epic_id})",
                    tier_label(epic.tier.as_ref())
                ),
            });
        }
        if let Some(agent_name) = epic_agent_name.as_deref() {
            if agent_name.trim().is_empty() {
                return Err(BeadError::validation(
                    "epic agent name for work preclaim cannot be empty or blank",
                ));
            }
            if epic.status == StatusWire::Closed {
                return Err(BeadError::validation(format!(
                    "preclaim target is closed: {epic_id}"
                )));
            }
        }

        let mut seen = HashSet::new();
        let target_count =
            assignments.len() + usize::from(epic_agent_name.is_some());
        let mut indexes = Vec::with_capacity(assignments.len());
        let mut rollback = Vec::with_capacity(target_count);
        for assignment in assignments {
            if !seen.insert(assignment.bead_id.as_str()) {
                return Err(BeadError::validation(format!(
                    "duplicate preclaim target: {}",
                    assignment.bead_id
                )));
            }
            let index = store.issue_index(&assignment.bead_id)?;
            let issue = &store.issues[index];
            if issue.issue_type != IssueTypeWire::Phase {
                return Err(BeadError::validation(format!(
                    "preclaim target is not a phase bead: {}",
                    assignment.bead_id
                )));
            }
            if issue.parent_id.as_deref() != Some(epic_id) {
                return Err(BeadError::validation(format!(
                    "preclaim target {} is not a child of epic {}",
                    assignment.bead_id, epic_id
                )));
            }
            if issue.status == StatusWire::Closed {
                return Err(BeadError::validation(format!(
                    "preclaim target is closed: {}",
                    assignment.bead_id
                )));
            }
            indexes.push(index);
            rollback.push(BeadPreclaimRollbackWire {
                bead_id: issue.id.clone(),
                status: issue.status.clone(),
                assignee: issue.assignee.clone(),
            });
        }
        if epic_agent_name.is_some() {
            rollback.push(BeadPreclaimRollbackWire {
                bead_id: epic.id.clone(),
                status: epic.status.clone(),
                assignee: epic.assignee.clone(),
            });
        }

        let now = now.unwrap_or_else(now_utc);
        let mut updated = Vec::with_capacity(target_count);
        for (assignment, index) in assignments.iter().zip(indexes) {
            let issue = &mut store.issues[index];
            issue.status = StatusWire::InProgress;
            // A closed target is rejected above, so this archives nothing
            // today; it keeps the mutation path aligned with the
            // `EpicWorkPreclaimed` reducer branch if that guard ever moves.
            archive_close_metadata(
                issue,
                &now,
                BeadReopenCauseWire::EpicPreclaim,
                None,
            );
            clear_snooze_record(issue);
            issue.assignee = assignment.agent_name.clone();
            issue.updated_at = now.clone();
            issue.validate()?;
            updated.push(issue.clone());
        }
        for (assignment, issue) in assignments.iter().zip(&updated) {
            store.append_issue_event(
                &assignment.bead_id,
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed {
                    agent_name: assignment.agent_name.clone(),
                },
                &now,
                &issue.created_by,
            )?;
        }
        if let Some(agent_name) = epic_agent_name {
            let issue = &mut store.issues[epic_index];
            issue.status = StatusWire::InProgress;
            archive_close_metadata(
                issue,
                &now,
                BeadReopenCauseWire::EpicPreclaim,
                None,
            );
            clear_snooze_record(issue);
            issue.assignee = agent_name.clone();
            issue.updated_at = now.clone();
            issue.validate()?;
            let updated_epic = issue.clone();
            store.append_issue_event(
                epic_id,
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed { agent_name },
                &now,
                &updated_epic.created_by,
            )?;
            updated.push(updated_epic);
        }

        if !updated.is_empty() {
            store.save()?;
        }
        let mut result = outcome(
            "preclaim_epic_work",
            !updated.is_empty(),
            updated.iter().map(|issue| issue.id.clone()).collect(),
        );
        result.issues = updated;
        result.rollback_preclaims = rollback;
        Ok(result)
    })
}

fn durable_store_error(
    operation: &str,
    beads_dir: &Path,
    error: BeadError,
) -> BeadError {
    BeadError {
        kind: error.kind,
        message: format!(
            "failed to {operation} durable bead store {}: {}",
            beads_dir.display(),
            error.message
        ),
    }
}
