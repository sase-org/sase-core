//! Per-bead classification plus the `reduce_stranded_work` entry point.
//!
//! Rule order per candidate bead: owner, delegation, unknown, settle grace,
//! then the stranded cause precedence list. See the epic plan
//! `plan:202610/stranded_epic_work.md` for the full contract.

use std::collections::BTreeSet;

use chrono::DateTime;

use super::propagate::apply_stranded_propagation;
use super::wire::{
    StrandedWorkBeadFactsWire, StrandedWorkInputWire,
    StrandedWorkMemberFactsWire, StrandedWorkReportWire,
    StrandedWorkVerdictWire, STRANDED_CAUSE_CHILD_LANDED,
    STRANDED_CAUSE_CLOSE_FAILED, STRANDED_CAUSE_FAILED,
    STRANDED_CAUSE_FOLLOWUP_FAILED, STRANDED_CAUSE_KEPT, STRANDED_CAUSE_LOST,
    STRANDED_CAUSE_STOPPED, STRANDED_CAUSE_UNCLOSED, STRANDED_STATE_BLOCKED,
    STRANDED_STATE_DELEGATED, STRANDED_STATE_OWNED, STRANDED_STATE_SETTLING,
    STRANDED_STATE_STRANDED, STRANDED_STATE_UNKNOWN,
    STRANDED_UNKNOWN_NO_RECORDS, STRANDED_UNKNOWN_RECORDS_UNAVAILABLE,
    STRANDED_WORK_WIRE_SCHEMA_VERSION,
};

/// Lifecycles with no terminal marker: still outstanding, or never classified.
/// `unknown` counts as non-terminal: it carries no done marker, so a live or
/// remote `unknown` member owns the bead (never contradict ACE), while a dead
/// local one is `lost` rather than silently unowned.
fn is_non_terminal_lifecycle(lifecycle: &str) -> bool {
    matches!(
        lifecycle,
        "starting" | "running" | "asking" | "waiting" | "queued" | "unknown"
    )
}

/// Follow-up outcomes that leave nothing to resume: the follow-up errored,
/// launched degraded, or settled lost/stopped without launching a follow-up.
fn is_failed_followup_outcome(outcome: &str) -> bool {
    matches!(
        outcome,
        "error"
            | "errored"
            | "failed"
            | "degraded"
            | "launched_degraded"
            | "lost"
            | "stopped"
    )
}

/// Lowercase, trimmed, with `-` folded to `_` so `launched-degraded` and
/// `launched_degraded` compare equal.
fn normalize_token(value: &str) -> String {
    value.trim().to_lowercase().replace('-', "_")
}

/// Normalize an optional wire string; blank becomes absent.
fn normalize_optional(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(normalize_token)
        .filter(|normed| !normed.is_empty())
}

/// Normalize a required wire string for comparison.
fn normalize_field(value: &str) -> String {
    normalize_token(value)
}

/// Recency key: `ended_at`, falling back to `started_at`. Members without
/// either timestamp sort before every timestamped member.
fn member_recency(member: &StrandedWorkMemberFactsWire) -> f64 {
    member
        .ended_at
        .or(member.started_at)
        .unwrap_or(f64::NEG_INFINITY)
}

/// Newest member by recency, breaking ties by name so shuffled input still
/// reduces identically.
fn newest_member(
    members: &[StrandedWorkMemberFactsWire],
) -> Option<&StrandedWorkMemberFactsWire> {
    members.iter().max_by(|left, right| {
        member_recency(left)
            .total_cmp(&member_recency(right))
            .then_with(|| left.name.cmp(&right.name))
    })
}

/// Newest member whose turn kind is `agent`.
fn newest_agent_member(
    members: &[StrandedWorkMemberFactsWire],
) -> Option<&StrandedWorkMemberFactsWire> {
    members
        .iter()
        .filter(|member| normalize_field(&member.turn_kind) == "agent")
        .max_by(|left, right| {
            member_recency(left)
                .total_cmp(&member_recency(right))
                .then_with(|| left.name.cmp(&right.name))
        })
}

/// Owner kind for one member, or `None` when it owns nothing.
///
/// First match wins: `finalizing`, `handoff`, `recovery`, the monitor/gate
/// turn states and pending follow-ups, then the lifecycle rule. `starting`
/// and `unknown` lifecycles report as `running`: both mean execution in
/// flight, and the owner vocabulary has no finer word for them.
///
/// A monitor or gate whose follow-up launched needs no special case here: a
/// live successor owns the bead through this same rule, which is exactly what
/// "passes ownership to `followup_agent`" means.
fn owner_kind(member: &StrandedWorkMemberFactsWire) -> Option<String> {
    if member.finalizing {
        return Some("finalizing".to_string());
    }
    let handoff = normalize_optional(&member.pending_handoff);
    if handoff.is_some() || member.handoff_inflight {
        return Some("handoff".to_string());
    }
    // Staged handoffs are never legacy markers, but unresolved staging owns
    // the bead like a handoff so the later reconciler (not the healer) owns
    // it. Malformed staged state also owns: it must never be discarded as an
    // ordinary abandoned run. A held `handoff_not_adopted` receipt owns for
    // the same reason; a successful `adopted` receipt does not.
    if normalize_optional(&member.staged_handoff).is_some() {
        return Some("handoff".to_string());
    }
    if let Some(outcome) = normalize_optional(&member.staged_receipt_outcome) {
        if outcome != "adopted" {
            return Some("handoff".to_string());
        }
    }
    if member.recovery_in_flight {
        return Some("recovery".to_string());
    }
    let turn_kind = normalize_field(&member.turn_kind);
    if turn_kind == "monitor" || turn_kind == "gate" {
        let turn_state = normalize_optional(&member.turn_state);
        if matches!(
            turn_state.as_deref(),
            Some("running") | Some("pending") | Some("settling")
        ) || member.followup_pending
        {
            return Some(turn_kind);
        }
    }
    let lifecycle = normalize_field(&member.lifecycle);
    if is_non_terminal_lifecycle(&lifecycle)
        && (member.process_live || member.remote)
    {
        if lifecycle == "asking"
            || lifecycle == "waiting"
            || lifecycle == "queued"
        {
            return Some(lifecycle);
        }
        return Some("running".to_string());
    }
    None
}

/// Newest qualifying member plus its owner kind, if any own the bead.
fn bead_owner(
    members: &[StrandedWorkMemberFactsWire],
) -> Option<(&StrandedWorkMemberFactsWire, String)> {
    members
        .iter()
        .filter_map(|member| owner_kind(member).map(|kind| (member, kind)))
        .max_by(|left, right| {
            member_recency(left.0)
                .total_cmp(&member_recency(right.0))
                .then_with(|| left.0.name.cmp(&right.0.name))
        })
}

/// Direct children of a bead, candidates and context alike.
fn child_beads<'a>(
    beads: &'a [StrandedWorkBeadFactsWire],
    id: &str,
) -> Vec<&'a StrandedWorkBeadFactsWire> {
    beads
        .iter()
        .filter(|bead| bead.parent_id.as_deref() == Some(id))
        .collect()
}

/// Launched child epics: the delegation targets, sorted.
fn launched_child_epics(
    beads: &[StrandedWorkBeadFactsWire],
    id: &str,
) -> Vec<String> {
    let mut delegates = BTreeSet::new();
    for child in child_beads(beads, id) {
        if normalize_field(&child.kind) != "epic" {
            continue;
        }
        if matches!(
            normalize_field(&child.status).as_str(),
            "in_progress" | "claimed"
        ) {
            delegates.insert(child.id.clone());
        }
    }
    delegates.into_iter().collect()
}

/// Latest activity: the maximum of members' recency and closed children's
/// `closed_at`. `None` when nothing carries a timestamp.
fn activity_since(
    bead: &StrandedWorkBeadFactsWire,
    beads: &[StrandedWorkBeadFactsWire],
) -> Option<f64> {
    let mut since: Option<f64> = None;
    for member in &bead.members {
        let recency = member.ended_at.or(member.started_at);
        since = match (since, recency) {
            (Some(current), Some(next)) => Some(current.max(next)),
            (None, next) => next,
            (current, None) => current,
        };
    }
    for child in child_beads(beads, &bead.id) {
        if let Some(closed_at) = child.closed_at {
            since =
                Some(since.map_or(closed_at, |current| current.max(closed_at)));
        }
    }
    since
}

/// True when a closed child epic landed with no newer agent turn after it:
/// the child carries the work, but the phase is still open.
fn is_child_landed(
    bead: &StrandedWorkBeadFactsWire,
    beads: &[StrandedWorkBeadFactsWire],
) -> bool {
    for child in child_beads(beads, &bead.id) {
        if normalize_field(&child.kind) != "epic" {
            continue;
        }
        let closed = normalize_field(&child.status) == "closed"
            || child.closed_at.is_some();
        if !closed {
            continue;
        }
        let landed_at = child.closed_at.unwrap_or(f64::NEG_INFINITY);
        let newer_agent = bead.members.iter().any(|member| {
            normalize_field(&member.turn_kind) == "agent"
                && member_recency(member) > landed_at
        });
        if !newer_agent {
            return true;
        }
    }
    false
}

/// Stranded cause for a bead with no owner, no delegate, and cold activity.
/// First match wins over the precedence list.
fn stranded_cause(
    bead: &StrandedWorkBeadFactsWire,
    beads: &[StrandedWorkBeadFactsWire],
) -> &'static str {
    if let Some(newest) = newest_member(&bead.members) {
        let turn_kind = normalize_field(&newest.turn_kind);
        if turn_kind == "monitor" || turn_kind == "gate" {
            if let Some(outcome) = normalize_optional(&newest.followup_outcome)
            {
                if is_failed_followup_outcome(&outcome) {
                    return STRANDED_CAUSE_FOLLOWUP_FAILED;
                }
            }
        }
    }
    if bead.completion_failure.is_some() {
        return STRANDED_CAUSE_FAILED;
    }
    if let Some(latest) = newest_agent_member(&bead.members) {
        let lifecycle = normalize_field(&latest.lifecycle);
        if lifecycle == "failed" {
            return STRANDED_CAUSE_FAILED;
        }
        if lifecycle == "stopped" {
            return STRANDED_CAUSE_STOPPED;
        }
        if is_non_terminal_lifecycle(&lifecycle)
            && !latest.process_live
            && !latest.remote
        {
            return STRANDED_CAUSE_LOST;
        }
        if is_child_landed(bead, beads) {
            return STRANDED_CAUSE_CHILD_LANDED;
        }
        match normalize_optional(&latest.declared_bead_action).as_deref() {
            Some("close") => return STRANDED_CAUSE_CLOSE_FAILED,
            Some("keep") => return STRANDED_CAUSE_KEPT,
            _ => {}
        }
    } else if is_child_landed(bead, beads) {
        return STRANDED_CAUSE_CHILD_LANDED;
    }
    STRANDED_CAUSE_UNCLOSED
}

fn classify_bead(
    input: &StrandedWorkInputWire,
    bead: &StrandedWorkBeadFactsWire,
) -> StrandedWorkVerdictWire {
    let latest_agent = newest_agent_member(&bead.members);
    let mut verdict = StrandedWorkVerdictWire {
        bead_id: bead.id.clone(),
        kind: bead.kind.clone(),
        state: String::new(),
        reason: None,
        owner: None,
        delegates: Vec::new(),
        session: bead.assignee.clone(),
        last_turn: bead
            .completion_failure
            .as_ref()
            .map(|failure| failure.agent_name.clone())
            .or_else(|| latest_agent.map(|member| member.name.clone())),
        last_turn_artifact_dir: bead
            .completion_failure
            .as_ref()
            .map(|failure| failure.artifact_dir.clone())
            .or_else(|| latest_agent.map(|member| member.artifact_dir.clone())),
        completion_failure: bead.completion_failure.clone(),
        declared_bead_action: latest_agent
            .and_then(|member| member.declared_bead_action.clone()),
        since: activity_since(bead, &input.beads),
        parent_id: bead.parent_id.clone(),
        blocked_by: Vec::new(),
        blocks: Vec::new(),
    };
    if verdict.last_turn_artifact_dir.as_deref() == Some("") {
        verdict.last_turn_artifact_dir = None;
    }

    if let Some((owner, kind)) = bead_owner(&bead.members) {
        verdict.state = STRANDED_STATE_OWNED.to_string();
        verdict.reason = Some(kind);
        verdict.owner = Some(owner.name.clone());
        return verdict;
    }

    let delegates = launched_child_epics(&input.beads, &bead.id);
    if !delegates.is_empty() {
        verdict.state = STRANDED_STATE_DELEGATED.to_string();
        verdict.delegates = delegates;
        return verdict;
    }

    if bead.members.is_empty() && bead.completion_failure.is_none() {
        verdict.state = STRANDED_STATE_UNKNOWN.to_string();
        verdict.reason = Some(
            if bead.records_available {
                STRANDED_UNKNOWN_NO_RECORDS
            } else {
                STRANDED_UNKNOWN_RECORDS_UNAVAILABLE
            }
            .to_string(),
        );
        return verdict;
    }

    if let Some(failed_at) = bead
        .completion_failure
        .as_ref()
        .and_then(|failure| {
            DateTime::parse_from_rfc3339(&failure.failed_at).ok()
        })
        .map(|timestamp| timestamp.timestamp_millis() as f64 / 1_000.0)
    {
        verdict.since = Some(
            verdict
                .since
                .map_or(failed_at, |since| since.max(failed_at)),
        );
    }
    if let Some(since) = verdict.since {
        if input.now - since < input.settle_grace_seconds {
            verdict.state = STRANDED_STATE_SETTLING.to_string();
            return verdict;
        }
    }

    verdict.state = STRANDED_STATE_STRANDED.to_string();
    verdict.reason = Some(stranded_cause(bead, &input.beads).to_string());
    verdict
}

/// Map bead and session-member facts to per-bead verdicts with ancestor
/// propagation. Verdicts come out sorted by bead id; `stranded_ids` and
/// `blocked_ids` are sorted.
pub fn reduce_stranded_work(
    input: &StrandedWorkInputWire,
) -> StrandedWorkReportWire {
    let mut verdicts: Vec<StrandedWorkVerdictWire> = input
        .beads
        .iter()
        .filter(|bead| {
            bead.candidate
                || (normalize_field(&bead.status) == "open"
                    && bead.completion_failure.is_some())
        })
        .map(|bead| classify_bead(input, bead))
        .collect();
    apply_stranded_propagation(&mut verdicts, &input.beads);
    verdicts.sort_by(|left, right| left.bead_id.cmp(&right.bead_id));
    let mut stranded_ids = Vec::new();
    let mut blocked_ids = Vec::new();
    for verdict in &verdicts {
        if verdict.state == STRANDED_STATE_STRANDED {
            stranded_ids.push(verdict.bead_id.clone());
        } else if verdict.state == STRANDED_STATE_BLOCKED {
            blocked_ids.push(verdict.bead_id.clone());
        }
    }
    stranded_ids.sort();
    blocked_ids.sort();
    StrandedWorkReportWire {
        schema_version: STRANDED_WORK_WIRE_SCHEMA_VERSION,
        verdicts,
        stranded_ids,
        blocked_ids,
    }
}
