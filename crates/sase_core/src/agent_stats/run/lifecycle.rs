//! Lifecycle and runner-overlap helpers.
//!
//! Launch timestamps, overlap/handoff candidacy, hidden-record
//! filtering, timestamp parsing, lifecycle folding, and run durations.

use chrono::{DateTime, NaiveDateTime, Utc};

use crate::agent_runtime::parse_runtime_timestamp;
use crate::agent_scan::{AgentArtifactRecordWire, ACE_RUN_WORKFLOW_DIR};

use super::super::wire::AgentRunTotalsWire;
use super::finishing::normalized;
use super::types::*;

pub(super) fn launch_timestamp(row: &IndexRunRow) -> Option<f64> {
    row.started_at
        .as_deref()
        .and_then(parse_timestamp)
        .or_else(|| parse_artifact_timestamp(&row.timestamp))
}

pub(super) fn runner_overlap_candidate(
    row: &IndexRunRow,
    requested_start: f64,
    requested_end: f64,
) -> bool {
    if row.workflow_dir_name != ACE_RUN_WORKFLOW_DIR {
        return false;
    }
    let Some(started) = row.started_at.as_deref().and_then(parse_timestamp)
    else {
        return false;
    };
    if row
        .finished_at
        .filter(|value| value.is_finite())
        .is_some_and(|finished| finished <= requested_start)
    {
        return false;
    }
    if started >= requested_end {
        return false;
    }
    true
}

pub(super) fn runner_handoff_carry_candidate(
    row: &IndexRunRow,
    requested_start: f64,
    requested_end: f64,
) -> bool {
    if row.workflow_dir_name != ACE_RUN_WORKFLOW_DIR {
        return false;
    }
    let Some(started) = row.started_at.as_deref().and_then(parse_timestamp)
    else {
        return false;
    };
    if started >= requested_end {
        return false;
    }
    row.finished_at
        .filter(|value| value.is_finite())
        .is_some_and(|finished| finished <= requested_start)
}

pub(super) fn record_is_user_hidden(record: &AgentArtifactRecordWire) -> bool {
    record.agent_meta.as_ref().is_some_and(|meta| meta.hidden)
}

pub(in crate::agent_stats) fn parse_timestamp(value: &str) -> Option<f64> {
    parse_runtime_timestamp(value)
}

pub(super) fn parse_artifact_timestamp(value: &str) -> Option<f64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(datetime_seconds(parsed.and_utc()))
}

pub(super) fn datetime_seconds(value: DateTime<Utc>) -> f64 {
    value.timestamp() as f64
        + f64::from(value.timestamp_subsec_nanos()) / 1_000_000_000.0
}

pub(super) fn fold_lifecycle(
    totals: &mut AgentRunTotalsWire,
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> Option<String> {
    if let Some(done) = record.done.as_ref() {
        let outcome = normalized(done.outcome.as_deref());
        match outcome.as_str() {
            "completed" => totals.completed += 1,
            "failed" | "epic_launch_failed" => totals.failed += 1,
            _ => totals.other_terminal += 1,
        }
        return Some(outcome);
    }
    if record.waiting.is_some() || row.status == "waiting" {
        totals.waiting += 1;
    } else {
        totals.in_progress += 1;
    }
    None
}

pub(super) fn run_duration_seconds(
    record: &AgentArtifactRecordWire,
    row: &IndexRunRow,
) -> Option<f64> {
    let start = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.run_started_at.as_deref())
        .and_then(parse_timestamp)
        .or_else(|| row.started_at.as_deref().and_then(parse_timestamp))?;
    let end = record
        .done
        .as_ref()
        .and_then(|done| done.finished_at)
        .or(row.finished_at)
        .filter(|value| value.is_finite())
        .or_else(|| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.stopped_at.as_deref())
                .and_then(parse_timestamp)
        })?;
    let duration = end - start;
    (duration >= 0.0 && duration.is_finite()).then_some(duration)
}
