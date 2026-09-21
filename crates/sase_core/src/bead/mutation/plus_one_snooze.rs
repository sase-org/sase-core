use super::mutation_wire::BeadMutationOutcomeWire;
use super::notes_update::append_note_to_store;
use super::store::mutation_status_value;
use super::store::normalize_references;
use super::store::now_utc;
use super::store::outcome;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use crate::bead::events::archive_close_metadata;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::task_plus_one_reopen_decision;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadSnoozeWakeCauseWire;
use crate::bead::events::TaskPlusOneReopenDecision;
use crate::bead::read::resolve_issue_id_in_issues;
use crate::bead::wire::parse_snooze_timestamp;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::BeadSnoozeWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use crate::bead::wire::TaskPlusOneEvidenceWire;
use std::path::Path;

/// Append one independently attributed report to an existing task bead.
///
/// The evidence, referenced artifacts, and any draft/closed-to-ready status
/// promotion are persisted together under the bead mutation lock. Repeating
/// the creator or an existing reporter is an exact no-op.
pub fn add_task_plus_one(
    beads_dir: &Path,
    issue_id: &str,
    reporter: &str,
    note: &str,
    references: &[String],
    now: Option<String>,
    observed_since: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let reporter = reporter.trim().to_string();
    if reporter.is_empty() {
        return Err(BeadError::validation(
            "task +1 reporter cannot be empty or blank",
        ));
    }
    let note = note.trim().to_string();
    if note.is_empty() {
        return Err(BeadError::validation(
            "task +1 note cannot be empty or blank",
        ));
    }
    let references = normalize_references(references)?;

    with_bead_mutation_lock(beads_dir, "plus_one", || {
        let mut store = MutableStore::load(beads_dir)?;
        let resolved_id = resolve_issue_id_in_issues(&store.issues, issue_id)?;
        let index = store.issue_index(&resolved_id)?;
        let current = store.issues[index].clone();
        if current.issue_type != IssueTypeWire::Task {
            return Err(BeadError::validation(format!(
                "task +1 only applies to task beads: {resolved_id}"
            )));
        }
        if reporter == current.created_by
            || current
                .plus_one_evidence
                .iter()
                .any(|evidence| evidence.reporter == reporter)
        {
            let mut result =
                outcome("plus_one", false, vec![resolved_id.clone()]);
            result.issue = Some(current);
            result.message =
                "reporter already represented; use sase bead note for supplementary evidence"
                    .to_string();
            return Ok(result);
        }

        let timestamp = now.unwrap_or_else(now_utc);
        let evidence = TaskPlusOneEvidenceWire {
            timestamp: timestamp.clone(),
            observed_since,
            reporter: reporter.clone(),
            note,
            refs: references.clone(),
        };
        evidence.validate()?;

        let issue = &mut store.issues[index];
        issue.plus_one_evidence.push(evidence.clone());
        for reference in &references {
            if !issue.refs.contains(reference) {
                issue.refs.push(reference.clone());
            }
        }
        // A snoozed bead is deliberately excluded from the open/closed
        // promotion below: it only leaves `snoozed` when its own +1 target is
        // reached, and never on an ordinary +1.
        let wake_note = plus_one_wake_note(issue);
        let mut reopen_withheld_closed_at = None;
        if wake_note.is_some() {
            issue.status = StatusWire::Ready;
            clear_snooze_record(issue);
        } else {
            match task_plus_one_reopen_decision(issue, &evidence)? {
                TaskPlusOneReopenDecision::Reopen => {
                    let was_closed = issue.status == StatusWire::Closed;
                    issue.status = StatusWire::Ready;
                    archive_close_metadata(
                        issue,
                        &timestamp,
                        BeadReopenCauseWire::PlusOne,
                        Some(reporter.clone()),
                    );
                    if was_closed {
                        issue.assignee.clear();
                    }
                }
                TaskPlusOneReopenDecision::Withheld { closed_at } => {
                    reopen_withheld_closed_at = Some(closed_at);
                }
                TaskPlusOneReopenDecision::Unchanged => {}
            }
        }
        issue.updated_at = timestamp.clone();
        issue.validate()?;

        store.append_issue_event(
            &resolved_id,
            BeadEventOperationWire::TaskPlusOneRecorded,
            BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
            &timestamp,
            &reporter,
        )?;
        if let Some(note) = &wake_note {
            store.append_issue_event(
                &resolved_id,
                BeadEventOperationWire::TaskSnoozeWoken,
                BeadEventPayloadWire::TaskSnoozeWoken {
                    cause: BeadSnoozeWakeCauseWire::PlusOne,
                },
                &timestamp,
                &reporter,
            )?;
            append_note_to_store(
                &mut store, index, note, &reporter, &timestamp,
            )?;
        }
        let issue = store.issues[index].clone();
        store.save()?;

        let mut result = outcome("plus_one", true, vec![resolved_id]);
        result.issue = Some(issue);
        result.references = references;
        result.reopen_withheld = reopen_withheld_closed_at.is_some();
        result.reopen_withheld_closed_at = reopen_withheld_closed_at;
        Ok(result)
    })
}

/// Return the preset wake note when this bead just reached its +1 target.
///
/// `None` covers every other shape — not snoozed, no target, or short of it
/// — so the caller has exactly one place to ask "did this +1 wake the bead".
fn plus_one_wake_note(issue: &IssueWire) -> Option<String> {
    if issue.status != StatusWire::Snoozed {
        return None;
    }
    let snooze = issue.snooze.as_ref()?;
    let target = snooze.plus_one_target?;
    if u32::try_from(issue.plus_one_count()).unwrap_or(u32::MAX) < target {
        return None;
    }
    Some(format!(
        "Reopened by +1 threshold: reached {target} +1s while snoozed until {}.",
        snooze.until
    ))
}

/// Render the note appended to a task bead the moment its deferral begins,
/// so the "why and until when" survives the wake that clears the snooze
/// record.
///
/// `previous_until` is `Some` exactly when this call replaces an existing
/// snooze record (a re-snooze), naming the wake time it displaces.
/// `deferral_seconds` is `until - snoozed_at`, a snapshot of the length that
/// was chosen rather than a countdown, so the note never goes stale.
fn snooze_note(
    snooze: &BeadSnoozeWire,
    plus_ones: Option<u32>,
    previous_until: Option<&str>,
    deferral_seconds: i64,
) -> String {
    let until = snooze.until.trim();
    let length = deferral_length_label(deferral_seconds);
    let mut note = match previous_until {
        Some(previous_until) => format!(
            "Re-snoozed until {until} (in {length}), replacing the wake time {}.",
            previous_until.trim()
        ),
        None => format!("Snoozed until {until} (in {length})."),
    };

    if let Some(requested) = plus_ones {
        let unit = if requested == 1 { "+1" } else { "+1s" };
        let baseline = snooze.plus_one_baseline.unwrap_or(0);
        if baseline > 0 {
            let target = snooze.plus_one_target.unwrap_or(baseline + requested);
            note.push_str(&format!(
                " Also wakes at {requested} more {unit} ({target} total)."
            ));
        } else {
            note.push_str(&format!(" Also wakes at {requested} more {unit}."));
        }
    }

    let reason: String = snooze
        .reason
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if !reason.is_empty() {
        note.push_str(&format!(" Reason: {reason}"));
    }

    note
}

/// Round to the nearest integer, half away from zero, without floats.
fn round_div(numerator: i64, denominator: i64) -> i64 {
    (numerator + denominator / 2) / denominator
}

/// Render a deferral length in the same compact vocabulary as
/// `_snooze_remaining_label` (`src/sase/bead/snooze_presentation.py`) —
/// `s`/`m`/`h`/`d`/`mo`/`y` — but for a fixed length rather than a countdown
/// from "now". Two deliberate departures from that ladder, both because this
/// is a length and not a remaining time: the sub-minute bucket renders
/// `<secs>s` rather than `now`, and there is no `due now` case — `until`
/// after `snoozed_at` is enforced before this is ever called, so the delta is
/// always at least one second.
pub(crate) fn deferral_length_label(seconds: i64) -> String {
    if seconds < 60 {
        return format!("{seconds}s");
    }
    if seconds < 3_600 {
        return format!("{}m", round_div(seconds, 60));
    }
    if seconds < 86_400 {
        return format!("{}h", round_div(seconds, 3_600));
    }
    if seconds < 30 * 86_400 {
        return format!("{}d", round_div(seconds, 86_400));
    }
    if seconds < 365 * 86_400 {
        return format!("{}mo", round_div(seconds, 30 * 86_400));
    }
    format!("{}y", round_div(seconds, 365 * 86_400))
}

/// Defer one task bead until a wake time, and optionally a +1 threshold.
///
/// Re-snoozing an already-snoozed bead is allowed and replaces the record;
/// that is the "snooze for longer" path, and it appends a fresh event rather
/// than editing the old one so the history stays readable.
pub fn snooze_task(
    beads_dir: &Path,
    issue_id: &str,
    until: &str,
    plus_ones: Option<u32>,
    reason: &str,
    actor: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let actor = actor.trim().to_string();
    if actor.is_empty() {
        return Err(BeadError::validation(
            "bead snooze actor cannot be empty or blank",
        ));
    }
    if plus_ones == Some(0) {
        return Err(BeadError::validation(
            "bead snooze +1 target must be at least 1",
        ));
    }
    let until_at = parse_snooze_timestamp(until, "until")?;
    let reason = reason.trim().to_string();

    with_bead_mutation_lock(beads_dir, "snooze", || {
        let mut store = MutableStore::load(beads_dir)?;
        let resolved_id = resolve_issue_id_in_issues(&store.issues, issue_id)?;
        let index = store.issue_index(&resolved_id)?;
        let current = store.issues[index].clone();
        if current.issue_type != IssueTypeWire::Task {
            return Err(BeadError::validation(format!(
                "bead snooze only applies to task beads: {resolved_id}"
            )));
        }
        if !matches!(
            current.status,
            StatusWire::Open | StatusWire::Ready | StatusWire::Snoozed
        ) {
            return Err(BeadError::validation(format!(
                "cannot snooze {resolved_id}: only open, ready, and already snoozed task beads can be snoozed (current status is {})",
                mutation_status_value(&current.status)
            )));
        }

        let timestamp = now.unwrap_or_else(now_utc);
        let now_at = parse_snooze_timestamp(&timestamp, "now")?;
        if until_at <= now_at {
            return Err(BeadError::validation(format!(
                "bead snooze wake time must be in the future: {until} is not after {timestamp}"
            )));
        }

        let baseline =
            u32::try_from(current.plus_one_count()).unwrap_or(u32::MAX);
        let snooze = BeadSnoozeWire {
            until: until.trim().to_string(),
            snoozed_at: timestamp.clone(),
            snoozed_by: actor.clone(),
            plus_one_target: plus_ones
                .map(|count| baseline.saturating_add(count)),
            plus_one_baseline: plus_ones.map(|_| baseline),
            reason,
        };
        snooze.validate()?;
        let deferral_seconds = (until_at - now_at).num_seconds();
        let note = snooze_note(
            &snooze,
            plus_ones,
            current
                .snooze
                .as_ref()
                .map(|previous| previous.until.as_str()),
            deferral_seconds,
        );

        let issue = &mut store.issues[index];
        issue.status = StatusWire::Snoozed;
        issue.snooze = Some(snooze.clone());
        issue.updated_at = timestamp.clone();
        issue.validate()?;

        store.append_issue_event(
            &resolved_id,
            BeadEventOperationWire::TaskSnoozed,
            BeadEventPayloadWire::TaskSnoozed { snooze },
            &timestamp,
            &actor,
        )?;
        append_note_to_store(&mut store, index, &note, &actor, &timestamp)?;
        let issue = store.issues[index].clone();
        store.save()?;

        let mut result = outcome("snooze", true, vec![resolved_id]);
        result.issue = Some(issue);
        Ok(result)
    })
}

/// Undo a snooze, returning the bead to triage with no record left behind.
pub fn cancel_task_snooze(
    beads_dir: &Path,
    issue_id: &str,
    actor: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let actor = actor.trim().to_string();
    if actor.is_empty() {
        return Err(BeadError::validation(
            "bead snooze actor cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "snooze_cancel", || {
        let mut store = MutableStore::load(beads_dir)?;
        let resolved_id = resolve_issue_id_in_issues(&store.issues, issue_id)?;
        let index = store.issue_index(&resolved_id)?;
        if store.issues[index].status != StatusWire::Snoozed {
            return Err(BeadError::validation(format!(
                "cannot cancel snooze: {resolved_id} is not snoozed"
            )));
        }

        let timestamp = now.unwrap_or_else(now_utc);
        let issue = &mut store.issues[index];
        issue.status = StatusWire::Ready;
        clear_snooze_record(issue);
        issue.updated_at = timestamp.clone();
        issue.validate()?;
        let issue = issue.clone();

        store.append_issue_event(
            &resolved_id,
            BeadEventOperationWire::TaskSnoozeCanceled,
            BeadEventPayloadWire::TaskSnoozeCanceled,
            &timestamp,
            &actor,
        )?;
        store.save()?;

        let mut result = outcome("snooze_cancel", true, vec![resolved_id]);
        result.issue = Some(issue);
        Ok(result)
    })
}
