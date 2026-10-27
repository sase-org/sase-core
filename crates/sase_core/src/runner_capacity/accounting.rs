//! Per-record capacity accounting for the Rust capacity snapshot.
//!
//! Every request record gets exactly one [`RunnerCapacityAccountingWire`]
//! entry: what it adds to `occupied_capacity`, where its weight came from,
//! which claim it belongs to, and why it holds nothing when it does not. The
//! candidate is excluded here and stays visible only through
//! `candidate_decision`.

use std::collections::{BTreeMap, BTreeSet};

use super::claims::claim_lineage;
use super::records::{
    effective_weight, is_occupying_record, is_pending_gate,
    is_real_monitor_member, is_user_agent_record, record_index,
};
use super::wire::{
    RunnerCapacityAccountingWire, RunnerCapacityClaimWire,
    RunnerCapacityRecordWire, RunnerCapacityWaiterWire,
};

pub(super) fn build_accounting(
    records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
    invalid_claim_keys: &BTreeSet<String>,
    waiters: &[RunnerCapacityWaiterWire],
) -> Vec<RunnerCapacityAccountingWire> {
    let index = record_index(records);
    let waiter_dirs: BTreeSet<&str> = waiters
        .iter()
        .map(|waiter| waiter.artifact_dir.as_str())
        .collect();
    let holders = elect_claim_holders(records, claims, &index);
    records
        .iter()
        .map(|record| {
            account_record(
                record,
                &index,
                invalid_claim_keys,
                &waiter_dirs,
                &holders,
            )
        })
        .collect()
}

/// Charged member per claim owner key: the maximum-weight occupying member,
/// ties broken by lexicographically smallest artifact directory so the choice
/// is deterministic.
fn elect_claim_holders(
    records: &[RunnerCapacityRecordWire],
    claims: &[RunnerCapacityClaimWire],
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> BTreeMap<String, (String, f64)> {
    let mut holders = BTreeMap::new();
    for claim in claims {
        let mut best: Option<(&RunnerCapacityRecordWire, f64)> = None;
        for record in records {
            if !is_occupying_record(record) {
                continue;
            }
            if claim_lineage(record, index).owner_key != claim.owner_key {
                continue;
            }
            let Ok(weight) = effective_weight(record) else {
                continue;
            };
            let replace = match &best {
                None => true,
                Some((current, current_weight)) => {
                    weight > *current_weight
                        || (weight == *current_weight
                            && record.artifact_dir < current.artifact_dir)
                }
            };
            if replace {
                best = Some((record, weight));
            }
        }
        if let Some((holder, weight)) = best {
            holders.insert(
                claim.owner_key.clone(),
                (holder.artifact_dir.clone(), weight),
            );
        }
    }
    holders
}

fn account_record(
    record: &RunnerCapacityRecordWire,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
    invalid_claim_keys: &BTreeSet<String>,
    waiter_dirs: &BTreeSet<&str>,
    holders: &BTreeMap<String, (String, f64)>,
) -> RunnerCapacityAccountingWire {
    let owner_key = claim_lineage(record, index).owner_key;
    let effective = effective_weight(record).ok();
    let weight_source = weight_source(record, &effective).to_string();
    let base =
        |state: &str, reason: Option<&str>| RunnerCapacityAccountingWire {
            artifact_dir: record.artifact_dir.clone(),
            project_name: record.project_name.clone(),
            agent_name: record.agent_name.clone(),
            state: state.to_string(),
            charged_weight: 0.0,
            effective_weight: effective,
            weight_source: weight_source.clone(),
            claim_owner_key: None,
            claim_holder_artifact_dir: None,
            reason_code: reason.map(str::to_string),
        };
    // Waiter membership wins over every other non-holding disposition: the
    // waiter set is computed by the same predicates, and `waiting` entries
    // must equal `waiters` exactly.
    if waiter_dirs.contains(record.artifact_dir.as_str()) {
        let mut entry = base("waiting", Some("queued"));
        entry.claim_owner_key = Some(owner_key);
        return entry;
    }
    // Members of a claim dropped for an invalid weight never charge.
    if is_occupying_record(record) && invalid_claim_keys.contains(&owner_key) {
        return base("unknown", Some("invalid_weight"));
    }
    if is_occupying_record(record) {
        let holder = holders.get(&owner_key);
        let is_holder = holder
            .is_some_and(|(holder_dir, _)| *holder_dir == record.artifact_dir);
        if is_holder {
            let weight = holder.map(|(_, weight)| *weight).unwrap_or(0.0);
            let reason = if weight == 0.0 {
                Some("explicit_zero_weight")
            } else {
                None
            };
            let mut entry = base("holding", reason);
            entry.charged_weight = weight;
            entry.claim_owner_key = Some(owner_key);
            return entry;
        }
        let mut entry = base("riding_claim", None);
        entry.claim_owner_key = Some(owner_key.clone());
        entry.claim_holder_artifact_dir =
            holder.map(|(holder_dir, _)| holder_dir.clone());
        // A rider whose claim has no elected holder cannot happen while the
        // claim exists, but an unknown holder must never read as a holder.
        if entry.claim_holder_artifact_dir.is_none() {
            entry.state = "unknown".to_string();
        }
        return entry;
    }
    if !is_user_agent_record(record) {
        let reason = if record.has_done_marker {
            "done"
        } else {
            "not_agent_record"
        };
        return base("not_holding", Some(reason));
    }
    if !record.live {
        return base("not_holding", Some("not_live"));
    }
    if record.pending_question {
        return base("not_holding", Some("pending_question"));
    }
    if is_pending_gate(record) {
        return base("not_holding", Some("pending_gate"));
    }
    if is_real_monitor_member(record) && record.pid.is_none() {
        return base("unknown", Some("no_pid"));
    }
    if effective.is_none() {
        return base("unknown", Some("invalid_weight"));
    }
    if effective == Some(0.0) && record.queue_weight_explicit {
        return base("not_holding", Some("explicit_zero_weight"));
    }
    base("not_holding", Some("not_started"))
}

fn weight_source(
    record: &RunnerCapacityRecordWire,
    effective: &Option<f64>,
) -> &'static str {
    if effective.is_none() {
        return "invalid";
    }
    if record.queue_weight_explicit {
        if *effective == Some(0.0) {
            return "explicit_zero";
        }
        return "explicit";
    }
    if record.queue_weight_inherited {
        return "inherited";
    }
    "default"
}
