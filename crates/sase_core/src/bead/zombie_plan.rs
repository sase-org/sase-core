//! Shared zombie-plan detection over one complete project bead snapshot.
//!
//! This module owns the deterministic tree and close-event predicate. Host
//! callers collect the project-wide liveness facts once and pass them here;
//! ownership itself is reduced by the shared stranded-work policy.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use super::wire::{BeadTierWire, IssueTypeWire, IssueWire, StatusWire};
use crate::stranded_work::{
    reduce_stranded_work, StrandedWorkBeadFactsWire, StrandedWorkInputWire,
    StrandedWorkMemberFactsWire,
};

pub const ZOMBIE_PLAN_WIRE_SCHEMA_VERSION: u32 = 1;

/// One bead's tree and current close-event evidence. No prose or note history
/// crosses this project-wide snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZombiePlanBeadWire {
    pub id: String,
    pub title: String,
    pub status: StatusWire,
    pub issue_type: IssueTypeWire,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub assignee: String,
    #[serde(default)]
    pub closed_at: Option<String>,
    #[serde(default)]
    pub close_event_id: Option<String>,
}

impl From<IssueWire> for ZombiePlanBeadWire {
    fn from(issue: IssueWire) -> Self {
        Self {
            id: issue.id,
            title: issue.title,
            status: issue.status,
            issue_type: issue.issue_type,
            tier: issue.tier,
            parent_id: issue.parent_id,
            assignee: issue.assignee,
            closed_at: issue.closed_at,
            close_event_id: issue.close_event_id,
        }
    }
}

/// A successful read is explicitly complete; failed reads return an error
/// instead. A host may also send `complete: false` when its collection is
/// partial, which makes every candidate unknown and ineligible.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZombiePlanTreeSnapshotWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub unknown_reason: Option<String>,
    #[serde(default)]
    pub beads: Vec<ZombiePlanBeadWire>,
}

/// Complete session-member evidence for one plan's land session.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZombiePlanOwnerFactsWire {
    pub plan_id: String,
    #[serde(default)]
    pub records_available: bool,
    #[serde(default)]
    pub unknown_reason: Option<String>,
    #[serde(default)]
    pub members: Vec<StrandedWorkMemberFactsWire>,
}

/// One batched evaluation request for a complete project graph.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZombiePlanEvaluationInputWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub snapshot: ZombiePlanTreeSnapshotWire,
    #[serde(default)]
    pub owners: Vec<ZombiePlanOwnerFactsWire>,
}

/// A verdict for one in-progress plan bead.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZombiePlanVerdictWire {
    pub plan_id: String,
    pub title: String,
    pub tier: Option<BeadTierWire>,
    pub assignee: String,
    /// `zombie`, `owned`, `not_zombie`, or `unknown`.
    pub state: String,
    pub zombie: bool,
    pub recovery_eligible: bool,
    /// Predicate reason for non-zombies, owners, or unknown evidence.
    pub reason: Option<String>,
    /// Specific evidence that made the row unknown.
    pub unknown_reason: Option<String>,
    /// Why a confirmed zombie cannot be automatically recovered.
    pub recovery_reason: Option<String>,
    pub owner: Option<String>,
    pub ownership_reason: Option<String>,
    pub tree_complete: bool,
    pub descendant_count: usize,
    pub all_descendants_closed: Option<bool>,
    pub latest_descendant_closed_at: Option<String>,
    pub latest_descendant_id: Option<String>,
    pub latest_descendant_close_event_id: Option<String>,
    /// Stable token derived from the selected current descendant close event.
    /// Missing/legacy identity never receives a synthetic token.
    pub close_token: Option<String>,
}

/// Project-wide zombie-plan report, sorted by plan ID.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZombiePlanEvaluationReportWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub rows: Vec<ZombiePlanVerdictWire>,
}

fn default_schema_version() -> u32 {
    ZOMBIE_PLAN_WIRE_SCHEMA_VERSION
}

fn tier_name(tier: Option<&BeadTierWire>) -> &'static str {
    match tier {
        Some(BeadTierWire::Epic) => "epic",
        Some(BeadTierWire::Plan) | None => "plan",
    }
}

fn issue_kind(bead: &ZombiePlanBeadWire) -> &'static str {
    match &bead.issue_type {
        IssueTypeWire::Plan => tier_name(bead.tier.as_ref()),
        IssueTypeWire::Phase => "phase",
        IssueTypeWire::Task => "task",
    }
}

fn status_name(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::Snoozed => "snoozed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn graph_problem(beads: &[ZombiePlanBeadWire]) -> Option<String> {
    let mut parents = BTreeMap::new();
    for bead in beads {
        if parents
            .insert(bead.id.as_str(), bead.parent_id.as_deref())
            .is_some()
        {
            return Some(format!("duplicate_bead_id:{}", bead.id));
        }
    }
    for bead in beads {
        if let Some(parent_id) = bead.parent_id.as_deref() {
            if !parents.contains_key(parent_id) {
                return Some(format!("missing_parent:{parent_id}"));
            }
        }
    }

    let mut complete = BTreeSet::new();
    for bead in beads {
        if complete.contains(bead.id.as_str()) {
            continue;
        }
        let mut path = Vec::new();
        let mut visiting = BTreeSet::new();
        let mut current = Some(bead.id.as_str());
        while let Some(id) = current {
            if complete.contains(id) {
                break;
            }
            if !visiting.insert(id) {
                return Some(format!("parent_cycle:{id}"));
            }
            path.push(id);
            current = parents.get(id).copied().flatten();
        }
        complete.extend(path);
    }
    None
}

fn descendants_by_parent(
    beads: &[ZombiePlanBeadWire],
) -> BTreeMap<String, Vec<String>> {
    let mut children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for bead in beads {
        if let Some(parent_id) = bead.parent_id.as_ref() {
            children
                .entry(parent_id.clone())
                .or_default()
                .push(bead.id.clone());
        }
    }
    for children in children.values_mut() {
        children.sort();
    }
    children
}

fn transitive_descendants(
    plan_id: &str,
    children: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut descendants = Vec::new();
    let mut pending = children.get(plan_id).cloned().unwrap_or_default();
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        descendants.push(id.clone());
        if let Some(next) = children.get(&id) {
            pending.extend(next.iter().cloned());
        }
    }
    descendants.sort();
    descendants
}

fn close_time(bead: &ZombiePlanBeadWire) -> Option<DateTime<FixedOffset>> {
    bead.closed_at
        .as_deref()
        .and_then(|closed_at| DateTime::parse_from_rfc3339(closed_at).ok())
}

fn base_verdict(plan: &ZombiePlanBeadWire) -> ZombiePlanVerdictWire {
    ZombiePlanVerdictWire {
        plan_id: plan.id.clone(),
        title: plan.title.clone(),
        tier: plan.tier.clone(),
        assignee: plan.assignee.clone(),
        ..ZombiePlanVerdictWire::default()
    }
}

fn unknown(
    mut verdict: ZombiePlanVerdictWire,
    reason: impl Into<String>,
) -> ZombiePlanVerdictWire {
    verdict.state = "unknown".to_string();
    let reason = reason.into();
    verdict.reason = Some(reason.clone());
    verdict.unknown_reason = Some(reason);
    verdict
}

/// Evaluate every in-progress plan in one project snapshot.
///
/// Tree faults and unavailable owner evidence fail closed. The ownership
/// verdict is delegated to `reduce_stranded_work`, keeping this predicate in
/// lockstep with stranded-work's live/finalizing/handoff/recovery policy.
pub fn evaluate_zombie_plans(
    input: &ZombiePlanEvaluationInputWire,
) -> ZombiePlanEvaluationReportWire {
    let candidates: Vec<&ZombiePlanBeadWire> = input
        .snapshot
        .beads
        .iter()
        .filter(|bead| {
            bead.issue_type == IssueTypeWire::Plan
                && bead.status == StatusWire::InProgress
        })
        .collect();
    let mut report = ZombiePlanEvaluationReportWire {
        schema_version: ZOMBIE_PLAN_WIRE_SCHEMA_VERSION,
        rows: Vec::new(),
    };

    let request_problem = if input.schema_version
        != ZOMBIE_PLAN_WIRE_SCHEMA_VERSION
    {
        Some("unsupported_request_schema".to_string())
    } else if input.snapshot.schema_version != ZOMBIE_PLAN_WIRE_SCHEMA_VERSION {
        Some("unsupported_snapshot_schema".to_string())
    } else if !input.snapshot.complete {
        Some(
            input
                .snapshot
                .unknown_reason
                .clone()
                .unwrap_or_else(|| "incomplete_tree_snapshot".to_string()),
        )
    } else {
        graph_problem(&input.snapshot.beads)
    };

    let children = descendants_by_parent(&input.snapshot.beads);
    let beads_by_id: BTreeMap<&str, &ZombiePlanBeadWire> = input
        .snapshot
        .beads
        .iter()
        .map(|bead| (bead.id.as_str(), bead))
        .collect();

    let mut owner_groups: BTreeMap<&str, Vec<&ZombiePlanOwnerFactsWire>> =
        BTreeMap::new();
    for facts in &input.owners {
        owner_groups.entry(&facts.plan_id).or_default().push(facts);
    }

    let mut reducer_input = StrandedWorkInputWire {
        now: f64::MAX,
        settle_grace_seconds: 0.0,
        ..StrandedWorkInputWire::default()
    };
    for bead in &input.snapshot.beads {
        let owner_facts =
            owner_groups.get(bead.id.as_str()).and_then(|facts| {
                if facts.len() == 1 {
                    Some(facts[0])
                } else {
                    None
                }
            });
        let members: Vec<StrandedWorkMemberFactsWire> = owner_facts
            .map(|facts| facts.members.clone())
            .unwrap_or_default();
        reducer_input.beads.push(StrandedWorkBeadFactsWire {
            id: bead.id.clone(),
            kind: issue_kind(bead).to_string(),
            status: status_name(&bead.status).to_string(),
            candidate: bead.issue_type == IssueTypeWire::Plan
                && bead.status == StatusWire::InProgress,
            assignee: Some(bead.assignee.clone()),
            parent_id: bead.parent_id.clone(),
            closed_at: close_time(bead).map(|date| date.timestamp() as f64),
            records_available: owner_facts
                .is_some_and(|facts| facts.records_available),
            members,
            completion_failure: None,
        });
    }
    let ownership = reduce_stranded_work(&reducer_input);
    let owner_verdicts: BTreeMap<&str, _> = ownership
        .verdicts
        .iter()
        .map(|verdict| (verdict.bead_id.as_str(), verdict))
        .collect();

    for plan in candidates {
        let mut verdict = base_verdict(plan);
        if let Some(problem) = request_problem.as_ref() {
            report.rows.push(unknown(verdict, problem.clone()));
            continue;
        }

        let descendants = transitive_descendants(&plan.id, &children);
        verdict.tree_complete = true;
        verdict.descendant_count = descendants.len();
        if descendants.is_empty() {
            verdict.state = "not_zombie".to_string();
            verdict.all_descendants_closed = Some(true);
            verdict.reason = Some("no_descendants".to_string());
            report.rows.push(verdict);
            continue;
        }
        let Some(descendant_beads): Option<Vec<&ZombiePlanBeadWire>> =
            descendants
                .iter()
                .map(|id| beads_by_id.get(id.as_str()).copied())
                .collect()
        else {
            report
                .rows
                .push(unknown(verdict, "incomplete_tree_snapshot"));
            continue;
        };
        let all_closed = descendant_beads
            .iter()
            .all(|bead| bead.status == StatusWire::Closed);
        verdict.all_descendants_closed = Some(all_closed);
        if !all_closed {
            verdict.state = "not_zombie".to_string();
            verdict.reason = Some("open_descendant".to_string());
            report.rows.push(verdict);
            continue;
        }

        // Report the latest known close evidence even when ownership facts
        // later make the row `owned` or `unknown`.
        let latest = descendant_beads
            .iter()
            .filter_map(|bead| close_time(bead).map(|time| (time, *bead)))
            .max_by(|(left_time, left_bead), (right_time, right_bead)| {
                left_time
                    .cmp(right_time)
                    .then_with(|| {
                        left_bead.close_event_id.cmp(&right_bead.close_event_id)
                    })
                    .then_with(|| left_bead.id.cmp(&right_bead.id))
            });
        if let Some((_, latest_bead)) = latest {
            verdict.latest_descendant_closed_at = latest_bead.closed_at.clone();
            verdict.latest_descendant_id = Some(latest_bead.id.clone());
            verdict.latest_descendant_close_event_id = latest_bead
                .close_event_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string);
        }
        let missing_timestamp = descendant_beads
            .iter()
            .any(|bead| close_time(bead).is_none());
        if missing_timestamp {
            verdict.recovery_reason =
                Some("legacy_close_timestamp".to_string());
        } else if verdict.latest_descendant_close_event_id.is_none() {
            verdict.recovery_reason =
                Some("missing_close_event_identity".to_string());
        } else {
            let descendant_id = verdict
                .latest_descendant_id
                .as_deref()
                .expect("latest descendant is set with its close identity");
            let event_id = verdict
                .latest_descendant_close_event_id
                .as_deref()
                .expect("close identity was checked above");
            verdict.close_token = Some(format!("{descendant_id}:{event_id}"));
        }

        let owner_facts = owner_groups.get(plan.id.as_str());
        if owner_facts.is_some_and(|facts| facts.len() > 1) {
            report.rows.push(unknown(verdict, "duplicate_owner_facts"));
            continue;
        }
        if let Some(Some(reason)) = owner_facts
            .and_then(|facts| facts.first())
            .map(|facts| facts.unknown_reason.as_ref())
        {
            report.rows.push(unknown(verdict, reason.clone()));
            continue;
        }
        if !owner_facts
            .and_then(|facts| facts.first())
            .is_some_and(|facts| facts.records_available)
        {
            report
                .rows
                .push(unknown(verdict, "ownership_records_unavailable"));
            continue;
        }

        let Some(owner_verdict) = owner_verdicts.get(plan.id.as_str()) else {
            report.rows.push(unknown(verdict, "ownership_unavailable"));
            continue;
        };
        verdict.ownership_reason = owner_verdict.reason.clone();
        if owner_verdict.state == "owned" {
            verdict.state = "owned".to_string();
            verdict.owner = owner_verdict.owner.clone();
            verdict.reason = owner_verdict.reason.clone();
            report.rows.push(verdict);
            continue;
        }
        if owner_verdict.state != "stranded" {
            let reason = match owner_verdict.reason.as_deref() {
                Some("no_records") => "ownership_no_records",
                Some("records_unavailable") => "ownership_records_unavailable",
                Some(reason) => reason,
                None => "ownership_unknown",
            };
            report.rows.push(unknown(verdict, reason));
            continue;
        }

        verdict.state = "zombie".to_string();
        verdict.zombie = true;
        if verdict.recovery_reason.is_none() {
            if plan.tier != Some(BeadTierWire::Epic) {
                verdict.recovery_reason =
                    Some("unsupported_plan_tier".to_string());
            } else {
                verdict.recovery_eligible = true;
            }
        }
        report.rows.push(verdict);
    }
    report
        .rows
        .sort_by(|left, right| left.plan_id.cmp(&right.plan_id));
    report
}

#[cfg(test)]
mod tests;
