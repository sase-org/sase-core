use std::collections::{BTreeMap, BTreeSet};

use crate::queue_directive::{
    authored_queue_weight_is_valid, queue_capacity_as_u32,
    queue_weight_is_valid, DEFAULT_QUEUE_WEIGHT,
};

use super::claims::claim_lineage;
use super::wire::RunnerCapacityRecordWire;

pub(super) fn record_index(
    records: &[RunnerCapacityRecordWire],
) -> BTreeMap<(String, String), &RunnerCapacityRecordWire> {
    let mut index = BTreeMap::new();
    for record in records {
        index
            .entry((record.project_name.clone(), record.timestamp.clone()))
            .or_insert(record);
    }
    index
}

pub(super) fn is_user_agent_record(record: &RunnerCapacityRecordWire) -> bool {
    record.workflow_dir_name == "ace-run"
        && !record.has_done_marker
        && record.has_agent_meta
        && record.appears_as_agent
}

pub(super) fn is_occupying_record(record: &RunnerCapacityRecordWire) -> bool {
    if !is_user_agent_record(record)
        || !record.live
        || record.pending_question
        || is_pending_gate(record)
    {
        return false;
    }
    let started = if is_real_monitor_member(record) {
        record.pid.is_some()
    } else {
        record
            .run_started_at
            .as_deref()
            .is_some_and(|value| !value.is_empty())
    };
    started
}

pub(super) fn is_waiting_record(
    record: &RunnerCapacityRecordWire,
    active_claims: &BTreeSet<String>,
    index: &BTreeMap<(String, String), &RunnerCapacityRecordWire>,
) -> bool {
    if !is_user_agent_record(record)
        || !record.live
        || record
            .slot_requested_at
            .as_deref()
            .is_none_or(|value| value.is_empty())
    {
        return false;
    }
    if serial_continuation_reuses_claim(record) {
        let owner_key = claim_lineage(record, index).owner_key;
        return !active_claims.contains(&owner_key);
    }
    true
}

fn serial_continuation_reuses_claim(record: &RunnerCapacityRecordWire) -> bool {
    !record.agent_session_parallel
        && (record
            .parent_timestamp
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || record
                .runner_claim_owner_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()))
}

fn is_pending_gate(record: &RunnerCapacityRecordWire) -> bool {
    record.agent_session_role.as_deref() == Some("gate")
        && record.agent_session_shell_kind.as_deref() == Some("gate")
        && record
            .agent_session_shell_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && record.agent_session_shell_state.as_deref() == Some("pending")
}

fn is_real_monitor_member(record: &RunnerCapacityRecordWire) -> bool {
    record.agent_session_role.as_deref() == Some("monitor")
        && record.agent_session_shell_kind.as_deref() == Some("monitor")
        && record
            .agent_session_shell_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

pub(super) fn effective_weight(
    record: &RunnerCapacityRecordWire,
) -> Result<f64, String> {
    if record.queue_weight_invalid {
        return Err(format!(
            "{} has an invalid queue_weight; expected a positive finite capacity weight.",
            record.artifact_dir
        ));
    }
    match record.queue_weight {
        Some(weight)
            if record_weight_is_valid(weight, record.queue_weight_explicit) =>
        {
            Ok(weight)
        }
        Some(_) => Err(format!(
            "{} has an invalid queue_weight; expected a positive finite capacity weight, or an explicit zero.",
            record.artifact_dir
        )),
        None => Ok(DEFAULT_QUEUE_WEIGHT),
    }
}

/// Record-weight validity for wire capacity records.
///
/// An explicit `0.0` is a valid non-occupying weight (e.g. the epic-launch
/// monitor); every other value, and every implicit weight, still follows the
/// strictly-positive `%queue`/`%q` weight contract in `queue_weight_is_valid`.
fn record_weight_is_valid(weight: f64, explicit: bool) -> bool {
    if explicit {
        authored_queue_weight_is_valid(weight)
    } else {
        queue_weight_is_valid(weight)
    }
}

pub(super) fn explicit_queue_capacity(
    record: &RunnerCapacityRecordWire,
) -> Option<u32> {
    if !record.queue_capacity_explicit {
        return None;
    }
    queue_capacity_as_u32(record.queue_capacity)
}
