use super::mutation_wire::BeadLaunchClaimContextWire;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::mutation_wire::BeadPreclaimAssignmentWire;
use super::mutation_wire::BeadPreclaimRollbackWire;
use super::runner::run_mutation;
use super::runner::MutationStep;
use super::store::mutation_status_value;
use super::store::now_utc;
use super::store::outcome;
use super::store::tier_label;
use super::view::MutationView;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadIssueUpdateEventFieldsWire;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadNoteWire;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use std::collections::HashSet;
use std::path::Path;

/// Fixed prefix for the host-written closed-bead launch audit note.
pub const CLOSED_BEAD_LAUNCH_NOTE_PREFIX: &str = "CLOSED-BEAD LAUNCH:";

/// Compose the `CLOSED-BEAD LAUNCH:` note written when a launch attaches
/// to a bead that was already closed at submission time.
pub fn closed_bead_launch_note(
    issue: &IssueWire,
    agent_name: &str,
    launch: &BeadLaunchClaimContextWire,
) -> String {
    let submitted = launch.submitted_at.as_deref().unwrap_or("unknown");
    let run_part = match launch
        .run_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(run_id) => format!("; run {run_id}"),
        None => String::new(),
    };
    let closed_at = issue.closed_at.as_deref().unwrap_or("unknown");
    let resolution = issue
        .resolution
        .as_ref()
        .map(BeadResolutionWire::as_str)
        .unwrap_or("unknown");
    format!(
        "{CLOSED_BEAD_LAUNCH_NOTE_PREFIX} agent {agent_name} launched against this closed bead; status left closed.\nLaunch submitted {submitted}{run_part}.\nPrior close: {closed_at} · {resolution}."
    )
}

pub fn claim_for_agent_launch(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
    launch: &BeadLaunchClaimContextWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead launch claim cannot be empty or blank",
        ));
    }
    if let Some(submitted_at) = launch.submitted_at.as_deref() {
        if let Err(error) =
            chrono::DateTime::parse_from_rfc3339(submitted_at.trim())
        {
            return Err(BeadError::validation(format!(
                "submitted_at must be an RFC-3339 timestamp: {submitted_at:?} ({error})"
            )));
        }
    }

    run_mutation(beads_dir, "claim_for_launch", |view| {
        run_claim_launch(view, issue_id, agent_name, now.clone(), launch)
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

    run_mutation(beads_dir, "claim_for_wait", |view| {
        run_claim_wait(view, issue_id, agent_name, now.clone())
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

    run_mutation(beads_dir, "release_wait_claim", |view| {
        run_claim_release(view, issue_id, agent_name, now.clone())
    })
}

pub fn preclaim_epic_work_plan(
    beads_dir: &Path,
    epic_id: &str,
    assignments: &[BeadPreclaimAssignmentWire],
    epic_agent_name: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    run_mutation(beads_dir, "preclaim_epic_work", |view| {
        run_preclaim(
            view,
            epic_id,
            assignments,
            epic_agent_name.clone(),
            now.clone(),
        )
    })
}

/// The single `claim_for_agent_launch` algorithm, over the view on both
/// backings.
///
/// Raw-ID lookup only (no shorthand), exactly as the locked replay load
/// did. The row stages before its event so stream routing sees it; one
/// `commit` persists both backings. A decline retries the same closure on
/// the replay backing; errors preserve the replay oracle's kinds and
/// messages.
fn run_claim_launch(
    view: &mut MutationView,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
    launch: &BeadLaunchClaimContextWire,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let current = view.get(issue_id)?;
    if current.status == StatusWire::Closed {
        return run_closed_launch_claim(
            view, issue_id, agent_name, now, launch, current,
        );
    }
    if current.status == StatusWire::InProgress
        && current.assignee == agent_name
    {
        let mut result =
            outcome("claim_for_agent_launch", false, vec![current.id.clone()]);
        result.issue = Some(current);
        return Ok(MutationStep::Done(result));
    }
    let now = now.unwrap_or_else(now_utc);
    let mut issue = current;
    issue.status = StatusWire::InProgress;
    issue.assignee = agent_name.to_string();
    issue.completion_failure = None;
    // The event this stages is an `issue_updated` carrying a status, so
    // the reducer clears the record through `apply_update_event_fields`;
    // clearing it here too is what keeps the two projections identical.
    clear_snooze_record(&mut issue);
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());
    let Some(_) = view.stage_event(
        &issue.id,
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
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one claim row");
    let mut result =
        outcome("claim_for_agent_launch", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

fn run_closed_launch_claim(
    view: &mut MutationView,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
    launch: &BeadLaunchClaimContextWire,
    current: IssueWire,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let Some(submitted_at) = launch.submitted_at.as_deref() else {
        return Err(BeadError {
            kind: "closed".to_string(),
            message: format!(
                "cannot claim closed bead for agent launch: {issue_id}"
            ),
        });
    };
    if let Some(closed_at) = current.closed_at.as_deref() {
        let submitted =
            chrono::DateTime::parse_from_rfc3339(submitted_at.trim()).map_err(
                |error| {
                    BeadError::validation(format!(
                        "submitted_at must be an RFC-3339 timestamp: {submitted_at:?} ({error})"
                    ))
                },
            )?;
        let closed = chrono::DateTime::parse_from_rfc3339(closed_at.trim())
            .map_err(|error| {
                BeadError::validation(format!(
                    "closed_at must be an RFC-3339 timestamp when comparing launch submitted_at: {closed_at:?} ({error})"
                ))
            })?;
        if closed > submitted {
            return Err(closed_while_queued_error(
                &current,
                submitted_at.trim(),
            ));
        }
    }
    let now = now.unwrap_or_else(now_utc);
    let entry = closed_bead_launch_note(&current, agent_name, launch);
    let mut issue = current;
    let Some(event_id) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::NoteAppended,
        BeadEventPayloadWire::NoteAppended {
            entry: entry.clone(),
            attachments: vec![],
        },
        &now,
        agent_name,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    if let Some(note) =
        BeadNoteWire::from_event(&event_id, &now, agent_name, &entry, vec![])
    {
        issue.notes.push(note);
    }
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one closed-bead launch row");
    let mut result =
        outcome("claim_for_agent_launch", true, vec![issue.id.clone()]);
    result.noted_ids = vec![issue.id.clone()];
    result.message =
        format!("attached to closed bead {}; status left closed", issue.id);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

fn closed_while_queued_error(
    issue: &IssueWire,
    submitted_at: &str,
) -> BeadError {
    let closed_at = issue.closed_at.as_deref().unwrap_or("unknown");
    let resolution = issue
        .resolution
        .as_ref()
        .map(BeadResolutionWire::as_str)
        .unwrap_or("unknown");
    let resolution_part = match issue
        .close_reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(reason) => format!("{resolution}: {reason}"),
        None => resolution.to_string(),
    };
    BeadError {
        kind: "closed_while_queued".to_string(),
        message: format!(
            "bead {} was closed at {closed_at} ({resolution_part}) after this launch was submitted at {submitted_at}; relaunch to run it against the closed bead deliberately",
            issue.id
        ),
    }
}

/// The single `claim_for_agent_wait` algorithm, over the view on both
/// backings.
///
/// Raw-ID lookup only (no shorthand). Declined claims (wrong holder,
/// terminal state) return a no-change outcome without writing,
/// byte-identical to replay.
fn run_claim_wait(
    view: &mut MutationView,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let current = view.get(issue_id)?;
    if matches!(current.status, StatusWire::Claimed | StatusWire::InProgress)
        && current.assignee == agent_name
    {
        let mut result =
            outcome("claim_for_agent_wait", false, vec![current.id.clone()]);
        result.issue = Some(current);
        return Ok(MutationStep::Done(result));
    }
    if current.status != StatusWire::Open {
        let holder = if current.assignee.is_empty() {
            "<unassigned>"
        } else {
            current.assignee.as_str()
        };
        let mut result =
            outcome("claim_for_agent_wait", false, vec![current.id.clone()]);
        result.message = format!(
            "cannot claim bead {issue_id} for agent wait: current status is {} and holder is {holder}",
            mutation_status_value(&current.status)
        );
        result.issue = Some(current);
        return Ok(MutationStep::Done(result));
    }
    let now = now.unwrap_or_else(now_utc);
    let mut issue = current;
    issue.status = StatusWire::Claimed;
    issue.assignee = agent_name.to_string();
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());
    let Some(_) = view.stage_event(
        &issue.id,
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
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one wait-claim row");
    let mut result =
        outcome("claim_for_agent_wait", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

/// The single `release_agent_claim` algorithm, over the view on both
/// backings.
///
/// Raw-ID lookup only (no shorthand). A non-owner or non-claimed release
/// returns a no-change outcome without writing, byte-identical to replay.
fn run_claim_release(
    view: &mut MutationView,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let current = view.get(issue_id)?;
    if current.status != StatusWire::Claimed || current.assignee != agent_name {
        let mut result =
            outcome("release_agent_claim", false, vec![current.id.clone()]);
        result.issue = Some(current);
        return Ok(MutationStep::Done(result));
    }
    let now = now.unwrap_or_else(now_utc);
    let mut issue = current;
    issue.status = StatusWire::Open;
    issue.assignee.clear();
    issue.updated_at = now.clone();
    issue.validate()?;
    view.stage_issue(issue.clone());
    let Some(_) = view.stage_event(
        &issue.id,
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
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one release row");
    let mut result =
        outcome("release_agent_claim", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    Ok(MutationStep::Done(result))
}

/// The single `preclaim_epic_work_plan` algorithm, over the view on both
/// backings.
///
/// All-or-nothing: every target validates before anything stages, so a
/// bad batch leaves bytes untouched, exactly as replay does. Raw-ID
/// lookups only (no shorthand). Each staged issue already carries its
/// assigned agent, so the event agent rides alongside the staged row.
fn run_preclaim(
    view: &mut MutationView,
    epic_id: &str,
    assignments: &[BeadPreclaimAssignmentWire],
    epic_agent_name: Option<String>,
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let epic = view.get(epic_id)?;
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
    let mut currents = Vec::with_capacity(assignments.len());
    let mut rollback = Vec::with_capacity(target_count);
    for assignment in assignments {
        if !seen.insert(assignment.bead_id.as_str()) {
            return Err(BeadError::validation(format!(
                "duplicate preclaim target: {}",
                assignment.bead_id
            )));
        }
        let issue = view.get(&assignment.bead_id)?;
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
        rollback.push(BeadPreclaimRollbackWire {
            bead_id: issue.id.clone(),
            status: issue.status.clone(),
            assignee: issue.assignee.clone(),
        });
        currents.push(issue);
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
    for (assignment, current) in assignments.iter().zip(currents) {
        let mut issue = current;
        issue.status = StatusWire::InProgress;
        issue.completion_failure = None;
        // A closed target is rejected above, so this archives nothing
        // today; it keeps the mutation path aligned with the
        // `EpicWorkPreclaimed` reducer branch if that guard ever moves.
        archive_close_metadata(
            &mut issue,
            &now,
            BeadReopenCauseWire::EpicPreclaim,
            None,
        );
        clear_snooze_record(&mut issue);
        issue.assignee = assignment.agent_name.clone();
        issue.updated_at = now.clone();
        issue.validate()?;
        view.stage_issue(issue.clone());
        updated.push((assignment.bead_id.clone(), issue));
    }
    if let Some(agent_name) = epic_agent_name.clone() {
        let mut issue = epic;
        issue.status = StatusWire::InProgress;
        issue.completion_failure = None;
        archive_close_metadata(
            &mut issue,
            &now,
            BeadReopenCauseWire::EpicPreclaim,
            None,
        );
        clear_snooze_record(&mut issue);
        issue.assignee = agent_name.clone();
        issue.updated_at = now.clone();
        issue.validate()?;
        view.stage_issue(issue.clone());
        updated.push((epic_id.to_string(), issue));
    }
    if updated.is_empty() {
        let mut result = outcome("preclaim_epic_work", false, Vec::new());
        result.rollback_preclaims = rollback;
        return Ok(MutationStep::Done(result));
    }
    for (bead_id, issue) in &updated {
        let Some(_) = view.stage_event(
            bead_id,
            BeadEventOperationWire::EpicWorkPreclaimed,
            BeadEventPayloadWire::EpicWorkPreclaimed {
                agent_name: issue.assignee.clone(),
            },
            &now,
            &issue.created_by,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    let expected_ids: Vec<String> =
        updated.iter().map(|(id, _)| id.clone()).collect();
    let Some(rows) = view.commit(&expected_ids)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let mut result = outcome(
        "preclaim_epic_work",
        !rows.is_empty(),
        rows.iter().map(|issue| issue.id.clone()).collect(),
    );
    result.issues = rows;
    result.rollback_preclaims = rollback;
    Ok(MutationStep::Done(result))
}
