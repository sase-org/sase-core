//! Wall-clock runtime aggregation for agent clans and sequential families.
//!
//! A member contributes the interval from `run_started_at` through its
//! terminal marker (or `now` while it is live). Callers choose whether to
//! remove human waits or only waits that release a runner slot before member
//! intervals are unioned.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::agent_scan::{AgentArtifactRecordWire, ACE_RUN_WORKFLOW_DIR};

/// Runtime-relevant projection of one agent artifact record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ClanRuntimeMemberWire {
    #[serde(default)]
    pub run_started_at: Option<String>,
    #[serde(default)]
    pub stopped_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<f64>,
    /// Whether the source artifact has a terminal marker. This distinguishes
    /// a live member from a malformed terminal record with no usable end.
    #[serde(default)]
    pub has_done_marker: bool,
    /// Whether the terminal marker was synthesized while abandoning a stale
    /// artifact. Its `finished_at` is not a trustworthy runtime boundary.
    #[serde(default)]
    pub terminal_is_synthesized: bool,
    #[serde(default)]
    pub plan_submitted_at: Vec<String>,
    #[serde(default)]
    pub feedback_submitted_at: Vec<String>,
    #[serde(default)]
    pub plan_approved: bool,
    #[serde(default)]
    pub questions_submitted_at: Vec<String>,
    #[serde(default)]
    pub question_response_path: Option<String>,
    #[serde(default)]
    pub pending_question_submitted_at: Option<String>,
}

impl ClanRuntimeMemberWire {
    /// Project the runtime fields carried by one artifact scan record.
    pub fn from_record(record: &AgentArtifactRecordWire) -> Self {
        let meta = record.agent_meta.as_ref();
        Self {
            run_started_at: meta.and_then(|value| value.run_started_at.clone()),
            stopped_at: meta.and_then(|value| value.stopped_at.clone()),
            finished_at: record.done.as_ref().and_then(|done| done.finished_at),
            has_done_marker: record.has_done_marker,
            terminal_is_synthesized: record
                .done
                .as_ref()
                .and_then(|done| done.outcome.as_deref())
                == Some("abandoned"),
            plan_submitted_at: meta
                .map(|value| value.plan_submitted_at.clone())
                .unwrap_or_default(),
            feedback_submitted_at: meta
                .map(|value| value.feedback_submitted_at.clone())
                .unwrap_or_default(),
            plan_approved: meta
                .map(|value| value.plan_approved)
                .unwrap_or(false),
            questions_submitted_at: meta
                .map(|value| value.questions_submitted_at.clone())
                .unwrap_or_default(),
            question_response_path: meta
                .and_then(|value| value.question_response_path.clone()),
            pending_question_submitted_at: record
                .pending_question
                .as_ref()
                .and_then(|marker| marker.submitted_at.clone()),
        }
    }
}

/// Wall-clock runtime for a clan or sequential family.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ClanRuntimeWire {
    pub wall_clock_seconds: f64,
    pub active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ActiveInterval {
    pub(crate) start: f64,
    pub(crate) end: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveIntervalError {
    InvalidStart,
    InvalidTerminal,
    UnusableTerminal,
    ImpossibleBounds,
}

/// Which inactive windows a runtime consumer removes from a member interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitPolicy {
    /// Exclude plan-review and question waits from clan/family wall time.
    HumanWaits,
    /// Exclude only question waits where runner admission released the slot.
    SlotYield,
}

#[derive(Debug, Default)]
pub(crate) struct ActiveIntervalDerivation {
    pub(crate) intervals: Vec<ActiveInterval>,
    pub(crate) live_at_end: bool,
    /// The member declared no terminal boundary and was therefore capped at
    /// the caller's query end. Consumers that report current occupancy must
    /// corroborate this with host liveness before accepting the intervals.
    pub(crate) open_ended: bool,
}

/// Measure the union of member active intervals through `now_epoch_seconds`.
///
/// Human-wait policy:
///
/// - a plan submission is excluded until the next feedback timestamp;
/// - an unmatched plan submission is open through the member end unless the
///   metadata says the plan was approved;
/// - an unanswered question is open through the member end;
/// - `pending_question_submitted_at` is authoritative and stays excluded
///   through the member end, including `now` for a live member;
/// - submissions inherited by a follow-up record (before its run start) are
///   ignored because that record begins when the human wait has resolved.
///
/// Malformed timestamps and records without a run start do not contribute.
pub fn aggregate_clan_runtime(
    members: &[ClanRuntimeMemberWire],
    now_epoch_seconds: f64,
) -> ClanRuntimeWire {
    if !now_epoch_seconds.is_finite() {
        return ClanRuntimeWire::default();
    }

    let mut intervals = Vec::new();
    let mut active = false;
    for member in members {
        let Ok(derived) = derive_active_intervals(
            member,
            now_epoch_seconds,
            WaitPolicy::HumanWaits,
            &[],
        ) else {
            continue;
        };
        active |= derived.live_at_end;
        intervals.extend(derived.intervals);
    }

    ClanRuntimeWire {
        wall_clock_seconds: union_measure(&mut intervals),
        active,
    }
}

/// Convenience wrapper for callers that already hold artifact scan records.
pub fn aggregate_clan_runtime_records(
    records: &[AgentArtifactRecordWire],
    now_epoch_seconds: f64,
) -> ClanRuntimeWire {
    let members = records
        .iter()
        .map(ClanRuntimeMemberWire::from_record)
        .collect::<Vec<_>>();
    aggregate_clan_runtime(&members, now_epoch_seconds)
}

/// Derive active intervals for one member under the requested wait policy,
/// capped at an exclusive query end. Runner analytics clips these intervals at
/// its lower analysis bound; clan/family runtime consumes them from run start.
pub(crate) fn derive_active_intervals(
    member: &ClanRuntimeMemberWire,
    query_end: f64,
    wait_policy: WaitPolicy,
    resolved_question_answers: &[f64],
) -> Result<ActiveIntervalDerivation, ActiveIntervalError> {
    if !query_end.is_finite() {
        return Err(ActiveIntervalError::ImpossibleBounds);
    }
    let start = member
        .run_started_at
        .as_deref()
        .and_then(parse_runtime_timestamp)
        .ok_or(ActiveIntervalError::InvalidStart)?;
    let terminal = member_terminal(member)?;
    if terminal.is_some_and(|end| end <= start) {
        return Err(ActiveIntervalError::ImpossibleBounds);
    }
    let end = terminal.unwrap_or(query_end).min(query_end);
    if end <= start {
        return Ok(ActiveIntervalDerivation::default());
    }

    let exclusions = match wait_policy {
        WaitPolicy::HumanWaits => member_human_waits(member, start, end),
        WaitPolicy::SlotYield => {
            member_slot_yields(member, start, end, resolved_question_answers)
        }
    };
    let live_at_end =
        terminal.is_none() && !point_is_excluded(query_end, &exclusions);
    Ok(ActiveIntervalDerivation {
        intervals: subtract_intervals(
            ActiveInterval { start, end },
            &exclusions,
        ),
        live_at_end,
        open_ended: terminal.is_none(),
    })
}

/// Return whether an artifact may itself be parked at the runner-slot gate.
///
/// This is the admission/eligibility half of the split shared with Python
/// `is_runner_slot_user_agent_record`: a root or a parallel family member
/// waits for its own slot, while a serial child, monitor, or post-handoff
/// follow-up rides the slot its family already holds. Terminal records stay
/// eligible so historical analytics can still classify the row; occupancy
/// uses [`is_runner_occupancy_record`] / [`running_agent_slot_count`].
pub fn is_runner_eligible_record(
    record: &AgentArtifactRecordWire,
) -> bool {
    if !is_runner_user_agent_kind(record) {
        return false;
    }
    let Some(meta) = record.agent_meta.as_ref() else {
        return false;
    };
    !(meta
        .parent_timestamp
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        && !meta.agent_family_parallel)
}

/// Return whether an artifact is the kind of user-agent row that can hold a
/// runner slot.
///
/// Unlike [`is_runner_eligible_record`], this ignores lineage
/// (`parent_timestamp`). A live serial child, monitor member, or post-handoff
/// follow-up can be the shell currently holding a family's slot. Terminal
/// rows stay included so historical occupancy can reconstruct their
/// intervals. Mirrors the record-kind half of Python
/// `is_runner_slot_occupying_record`.
pub(crate) fn is_runner_occupancy_record(
    record: &AgentArtifactRecordWire,
) -> bool {
    is_runner_user_agent_kind(record)
}

fn is_runner_user_agent_kind(record: &AgentArtifactRecordWire) -> bool {
    if record.workflow_dir_name != ACE_RUN_WORKFLOW_DIR {
        return false;
    }
    if record.agent_meta.is_none() {
        return false;
    }
    record
        .workflow_state
        .as_ref()
        .map_or(true, |state| state.appears_as_agent)
}

/// Per-family occupancy grouping key: `(project_name, agent_family)`.
///
/// A record with no `agent_family` falls back to its own `timestamp`, which
/// keeps standalone agents and independently launched clan members counting
/// individually. Mirrors Python `runner_slot_family_key`.
pub(crate) fn runner_slot_family_key(
    record: &AgentArtifactRecordWire,
) -> (String, String) {
    let family = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.agent_family.as_deref())
        .filter(|value| !value.is_empty())
        .unwrap_or(record.timestamp.as_str());
    (record.project_name.clone(), family.to_string())
}

/// Return whether *record* is occupying a runner slot in a live snapshot.
///
/// Mirrors Python `is_runner_slot_occupying_record`, including the
/// monitor-aware started rule: a monitor member (`monitor_id` set) only
/// needs a recorded `pid`, because the supervisor pid is written before
/// the starter's runner group is killed.
pub fn is_runner_slot_occupying_record(
    record: &AgentArtifactRecordWire,
    is_live: impl Fn(&AgentArtifactRecordWire) -> bool,
) -> bool {
    if !is_runner_occupancy_record(record) || record.has_done_marker {
        return false;
    }
    if record.pending_question.is_some() {
        return false;
    }
    let Some(meta) = record.agent_meta.as_ref() else {
        return false;
    };
    let monitor = meta
        .monitor_id
        .as_deref()
        .is_some_and(|value| !value.is_empty());
    let started = if monitor {
        meta.pid.is_some()
    } else {
        meta.run_started_at
            .as_deref()
            .is_some_and(|value| !value.is_empty())
    };
    started && is_live(record)
}

/// Count runner slots held right now, one per occupied family.
///
/// A family holds one slot while any non-parallel member is occupying.
/// Each live parallel member (`agent_family_parallel`) additionally holds
/// its own slot. Mirrors Python `running_agent_slot_count`.
pub fn running_agent_slot_count(
    records: &[AgentArtifactRecordWire],
    is_live: impl Fn(&AgentArtifactRecordWire) -> bool,
) -> u64 {
    let mut groups: BTreeMap<(String, String), Vec<&AgentArtifactRecordWire>> =
        BTreeMap::new();
    for record in records {
        groups
            .entry(runner_slot_family_key(record))
            .or_default()
            .push(record);
    }
    let mut count = 0u64;
    for group in groups.values() {
        let mut serial_occupying = false;
        let mut parallel_occupying = 0u64;
        for candidate in group {
            if !is_runner_slot_occupying_record(candidate, &is_live) {
                continue;
            }
            if candidate
                .agent_meta
                .as_ref()
                .is_some_and(|meta| meta.agent_family_parallel)
            {
                parallel_occupying += 1;
            } else {
                serial_occupying = true;
            }
        }
        count += u64::from(serial_occupying) + parallel_occupying;
    }
    count
}

/// One record's derived occupancy intervals, tagged for family merging.
#[derive(Debug, Clone)]
pub(crate) struct RunnerOccupancyContribution {
    pub family_key: (String, String),
    pub parallel: bool,
    pub monitor: bool,
    pub intervals: Vec<ActiveInterval>,
}

/// Merge per-record intervals into per-slot occupancy intervals.
///
/// Serial members of one family are unioned so overlapping in-process
/// handoffs do not double-count. A gap between a serial member's end and
/// a later monitor start is filled so the family's interval stays open
/// across the monitor handoff. Parallel members stay independent slots.
pub(crate) fn merge_family_occupancy_intervals(
    contributions: &[RunnerOccupancyContribution],
) -> Vec<ActiveInterval> {
    let mut groups: BTreeMap<
        &(String, String),
        Vec<&RunnerOccupancyContribution>,
    > = BTreeMap::new();
    for contribution in contributions {
        groups
            .entry(&contribution.family_key)
            .or_default()
            .push(contribution);
    }
    let mut occupancy = Vec::new();
    for group in groups.values() {
        let mut serial = Vec::new();
        let mut monitors = Vec::new();
        for contribution in group {
            if contribution.parallel {
                occupancy.extend(contribution.intervals.iter().copied());
                continue;
            }
            if contribution.monitor {
                monitors.extend(contribution.intervals.iter().copied());
            } else {
                serial.extend(contribution.intervals.iter().copied());
            }
        }
        occupancy.extend(merge_serial_family_intervals(&serial, &monitors));
    }
    occupancy
}

fn merge_serial_family_intervals(
    serial: &[ActiveInterval],
    monitors: &[ActiveInterval],
) -> Vec<ActiveInterval> {
    let mut filled = Vec::with_capacity(serial.len() + monitors.len());
    filled.extend(serial.iter().copied());
    for monitor in monitors {
        let prev_end = serial
            .iter()
            .filter(|interval| interval.end <= monitor.start)
            .map(|interval| interval.end)
            .max_by(|left, right| left.total_cmp(right));
        filled.push(ActiveInterval {
            start: prev_end.unwrap_or(monitor.start),
            end: monitor.end,
        });
    }
    merge_intervals(&mut filled)
}

/// Occupancy interval start for one record, as an epoch-seconds string.
///
/// Agent shells use `run_started_at`. Monitor members use the earlier of
/// `run_started_at` and the artifact-directory timestamp so occupancy
/// begins when the supervisor pid is recorded, not when the launch
/// barrier later writes `run_started_at`.
pub(crate) fn occupancy_member_start(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    let meta = record.agent_meta.as_ref()?;
    let started = meta
        .run_started_at
        .clone()
        .filter(|value| !value.is_empty());
    let monitor = meta
        .monitor_id
        .as_deref()
        .is_some_and(|value| !value.is_empty());
    if !monitor {
        return started;
    }
    let created = parse_compact_artifact_timestamp(&record.timestamp)
        .or_else(|| parse_runtime_timestamp(&record.timestamp));
    let Some(created) = created else {
        return started;
    };
    let started_ts = started.as_deref().and_then(parse_runtime_timestamp);
    if started_ts.map_or(true, |value| created < value) {
        Some(created.to_string())
    } else {
        started
    }
}

pub(crate) fn parse_compact_artifact_timestamp(value: &str) -> Option<f64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(datetime_seconds(parsed.and_utc()))
}

fn member_terminal(
    member: &ClanRuntimeMemberWire,
) -> Result<Option<f64>, ActiveIntervalError> {
    let mut declared = member.has_done_marker;
    let mut candidates = Vec::new();
    if let Some(value) = member.stopped_at.as_deref() {
        declared = true;
        if let Some(parsed) = parse_runtime_timestamp(value) {
            candidates.push(parsed);
        }
    }
    if !member.terminal_is_synthesized {
        if let Some(value) = member.finished_at {
            declared = true;
            if value.is_finite() {
                candidates.push(value);
            }
        }
    } else {
        declared = true;
    }
    if candidates.is_empty() {
        return if member.terminal_is_synthesized {
            Err(ActiveIntervalError::UnusableTerminal)
        } else if declared {
            Err(ActiveIntervalError::InvalidTerminal)
        } else {
            Ok(None)
        };
    }
    Ok(candidates.into_iter().min_by(f64::total_cmp))
}

fn member_human_waits(
    member: &ClanRuntimeMemberWire,
    start: f64,
    end: f64,
) -> Vec<ActiveInterval> {
    let mut waits = Vec::new();
    let mut feedback = parsed_sorted(&member.feedback_submitted_at);

    for submitted in parsed_sorted(&member.plan_submitted_at) {
        if submitted < start || submitted > end {
            continue;
        }
        if let Some(index) =
            feedback.iter().position(|value| *value >= submitted)
        {
            let resolved = feedback.remove(index).min(end);
            if resolved > submitted {
                waits.push(ActiveInterval {
                    start: submitted,
                    end: resolved,
                });
            }
        } else if !member.plan_approved && end > submitted {
            waits.push(ActiveInterval {
                start: submitted,
                end,
            });
        }
    }

    if member.question_response_path.is_none() {
        for submitted in parsed_sorted(&member.questions_submitted_at) {
            if submitted >= start && submitted < end {
                waits.push(ActiveInterval {
                    start: submitted,
                    end,
                });
            }
        }
    }

    if let Some(submitted) = member
        .pending_question_submitted_at
        .as_deref()
        .and_then(parse_runtime_timestamp)
        .filter(|value| *value >= start && *value < end)
    {
        waits.push(ActiveInterval {
            start: submitted,
            end,
        });
    }

    merge_intervals(&mut waits)
}

fn member_slot_yields(
    member: &ClanRuntimeMemberWire,
    start: f64,
    end: f64,
    resolved_question_answers: &[f64],
) -> Vec<ActiveInterval> {
    let mut waits = Vec::new();
    let mut answers = resolved_question_answers
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    answers.sort_by(f64::total_cmp);

    for submitted in parsed_sorted(&member.questions_submitted_at) {
        if submitted < start || submitted >= end {
            continue;
        }
        let Some(index) = answers.iter().position(|value| *value >= submitted)
        else {
            continue;
        };
        let answered = answers.remove(index).min(end);
        if answered > submitted {
            waits.push(ActiveInterval {
                start: submitted,
                end: answered,
            });
        }
    }

    if let Some(submitted) = member
        .pending_question_submitted_at
        .as_deref()
        .and_then(parse_runtime_timestamp)
        .filter(|value| *value >= start && *value < end)
    {
        waits.push(ActiveInterval {
            start: submitted,
            end,
        });
    }

    merge_intervals(&mut waits)
}

fn parsed_sorted(values: &[String]) -> Vec<f64> {
    let mut parsed = values
        .iter()
        .filter_map(|value| parse_runtime_timestamp(value))
        .collect::<Vec<_>>();
    parsed.sort_by(f64::total_cmp);
    parsed
}

pub(crate) fn parse_runtime_timestamp(value: &str) -> Option<f64> {
    if let Ok(seconds) = value.parse::<f64>() {
        return seconds.is_finite().then_some(seconds);
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Some(datetime_seconds(parsed.with_timezone(&Utc)));
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Some(datetime_seconds(parsed.and_utc()));
        }
    }
    None
}

fn datetime_seconds(value: DateTime<Utc>) -> f64 {
    value.timestamp() as f64 + f64::from(value.timestamp_subsec_nanos()) / 1e9
}

fn subtract_intervals(
    base: ActiveInterval,
    exclusions: &[ActiveInterval],
) -> Vec<ActiveInterval> {
    if base.end <= base.start {
        return Vec::new();
    }
    let mut cursor = base.start;
    let mut active = Vec::new();
    for exclusion in exclusions {
        let exclusion_start = exclusion.start.max(base.start);
        let exclusion_end = exclusion.end.min(base.end);
        if exclusion_end <= cursor || exclusion_start >= base.end {
            continue;
        }
        if exclusion_start > cursor {
            active.push(ActiveInterval {
                start: cursor,
                end: exclusion_start,
            });
        }
        cursor = cursor.max(exclusion_end);
    }
    if cursor < base.end {
        active.push(ActiveInterval {
            start: cursor,
            end: base.end,
        });
    }
    active
}

fn point_is_excluded(point: f64, exclusions: &[ActiveInterval]) -> bool {
    exclusions
        .iter()
        .any(|interval| interval.start <= point && point <= interval.end)
}

pub(crate) fn merge_intervals(
    intervals: &mut [ActiveInterval],
) -> Vec<ActiveInterval> {
    intervals.sort_by(|left, right| {
        left.start
            .total_cmp(&right.start)
            .then_with(|| left.end.total_cmp(&right.end))
    });
    let mut merged: Vec<ActiveInterval> = Vec::new();
    for interval in intervals.iter().copied() {
        if interval.end <= interval.start {
            continue;
        }
        if let Some(previous) = merged.last_mut() {
            if interval.start <= previous.end {
                previous.end = previous.end.max(interval.end);
                continue;
            }
        }
        merged.push(interval);
    }
    merged
}

fn union_measure(intervals: &mut [ActiveInterval]) -> f64 {
    merge_intervals(intervals)
        .iter()
        .map(|interval| interval.end - interval.start)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: f64 = 1_767_225_600.0; // 2026-01-01T00:00:00Z

    fn timestamp(offset: i64) -> String {
        DateTime::from_timestamp(BASE as i64 + offset, 0)
            .unwrap()
            .to_rfc3339()
    }

    fn member(start: i64, end: Option<i64>) -> ClanRuntimeMemberWire {
        ClanRuntimeMemberWire {
            run_started_at: Some(timestamp(start)),
            stopped_at: end.map(timestamp),
            ..ClanRuntimeMemberWire::default()
        }
    }

    fn derived_seconds(derived: &ActiveIntervalDerivation) -> f64 {
        derived
            .intervals
            .iter()
            .map(|interval| interval.end - interval.start)
            .sum()
    }

    #[test]
    fn empty_input_has_zero_inactive_runtime() {
        assert_eq!(
            aggregate_clan_runtime(&[], BASE + 100.0),
            ClanRuntimeWire::default()
        );
    }

    #[test]
    fn overlapping_members_are_measured_once() {
        let result = aggregate_clan_runtime(
            &[member(0, Some(20)), member(10, Some(30))],
            BASE + 100.0,
        );
        assert_eq!(result.wall_clock_seconds, 30.0);
        assert!(!result.active);
    }

    #[test]
    fn gaps_and_sequential_members_are_summed() {
        let result = aggregate_clan_runtime(
            &[member(0, Some(10)), member(20, Some(35))],
            BASE + 100.0,
        );
        assert_eq!(result.wall_clock_seconds, 25.0);
        assert!(!result.active);
    }

    #[test]
    fn open_intervals_end_at_now_and_mark_runtime_active() {
        let result = aggregate_clan_runtime(&[member(10, None)], BASE + 40.0);
        assert_eq!(result.wall_clock_seconds, 30.0);
        assert!(result.active);
    }

    #[test]
    fn plan_feedback_window_is_excised() {
        let mut value = member(0, Some(100));
        value.plan_submitted_at = vec![timestamp(20)];
        value.feedback_submitted_at = vec![timestamp(55)];
        let result = aggregate_clan_runtime(&[value], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 65.0);
        assert!(!result.active);
    }

    #[test]
    fn wait_policies_distinguish_plan_review_from_slot_yields() {
        let mut value = member(0, Some(100));
        value.plan_submitted_at = vec![timestamp(20)];
        value.feedback_submitted_at = vec![timestamp(55)];

        let human = derive_active_intervals(
            &value,
            BASE + 100.0,
            WaitPolicy::HumanWaits,
            &[],
        )
        .unwrap();
        let slot = derive_active_intervals(
            &value,
            BASE + 100.0,
            WaitPolicy::SlotYield,
            &[],
        )
        .unwrap();

        assert_eq!(derived_seconds(&human), 65.0);
        assert_eq!(derived_seconds(&slot), 100.0);
        assert_eq!(
            aggregate_clan_runtime(&[value], BASE + 100.0).wall_clock_seconds,
            65.0
        );
    }

    #[test]
    fn slot_yield_needs_a_resolved_question_answer_time() {
        let mut value = member(0, Some(100));
        value.questions_submitted_at = vec![timestamp(20)];

        let resolved = derive_active_intervals(
            &value,
            BASE + 100.0,
            WaitPolicy::SlotYield,
            &[BASE + 55.0],
        )
        .unwrap();
        let unresolved = derive_active_intervals(
            &value,
            BASE + 100.0,
            WaitPolicy::SlotYield,
            &[],
        )
        .unwrap();

        assert_eq!(derived_seconds(&resolved), 65.0);
        assert_eq!(derived_seconds(&unresolved), 100.0);
    }

    #[test]
    fn unresolved_plan_caps_a_live_member_and_does_not_tick() {
        let mut value = member(0, None);
        value.plan_submitted_at = vec![timestamp(20)];
        let result = aggregate_clan_runtime(&[value], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 20.0);
        assert!(!result.active);
    }

    #[test]
    fn approved_plan_followup_starts_a_new_active_segment() {
        let mut planner = member(0, Some(20));
        planner.plan_submitted_at = vec![timestamp(20)];
        let mut coder = member(50, None);
        coder.plan_submitted_at = vec![timestamp(20)];
        coder.plan_approved = true;
        let result = aggregate_clan_runtime(&[planner, coder], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 70.0);
        assert!(result.active);
    }

    #[test]
    fn answered_question_gap_between_segments_is_excluded() {
        let mut asker = member(0, Some(20));
        asker.questions_submitted_at = vec![timestamp(20)];
        let mut continuation = member(60, Some(100));
        continuation.questions_submitted_at = vec![timestamp(20)];
        continuation.question_response_path = Some("response.json".to_string());
        let result =
            aggregate_clan_runtime(&[asker, continuation], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 60.0);
        assert!(!result.active);
    }

    #[test]
    fn pending_question_window_extends_to_now_and_does_not_tick() {
        let mut value = member(0, None);
        value.questions_submitted_at = vec![timestamp(25)];
        value.pending_question_submitted_at = Some(timestamp(25));
        let result = aggregate_clan_runtime(&[value], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 25.0);
        assert!(!result.active);
    }

    #[test]
    fn malformed_timestamps_do_not_contribute() {
        let mut value = ClanRuntimeMemberWire {
            run_started_at: Some("not-a-time".to_string()),
            ..ClanRuntimeMemberWire::default()
        };
        value.plan_submitted_at = vec!["also-bad".to_string()];
        assert_eq!(
            aggregate_clan_runtime(&[value], BASE + 100.0),
            ClanRuntimeWire::default()
        );
    }

    #[test]
    fn earliest_valid_stop_or_finish_is_shared_runtime_end() {
        let mut value = member(0, Some(80));
        value.finished_at = Some(BASE + 20.0);
        value.has_done_marker = true;
        let result = aggregate_clan_runtime(&[value], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 20.0);
        assert!(!result.active);

        let mut fallback = member(0, None);
        fallback.stopped_at = Some("not-a-time".to_string());
        fallback.finished_at = Some(BASE + 30.0);
        fallback.has_done_marker = true;
        let result = aggregate_clan_runtime(&[fallback], BASE + 100.0);
        assert_eq!(result.wall_clock_seconds, 30.0);
    }

    #[test]
    fn synthesized_terminal_never_supplies_the_runtime_end() {
        let mut stopped = member(0, Some(10));
        stopped.finished_at = Some(BASE + 40.0 * 60.0 * 60.0);
        stopped.has_done_marker = true;
        stopped.terminal_is_synthesized = true;
        let derived = derive_active_intervals(
            &stopped,
            BASE + 100.0,
            WaitPolicy::SlotYield,
            &[],
        )
        .unwrap();
        assert_eq!(derived_seconds(&derived), 10.0);

        let mut unusable = member(0, None);
        unusable.finished_at = stopped.finished_at;
        unusable.has_done_marker = true;
        unusable.terminal_is_synthesized = true;
        assert_eq!(
            derive_active_intervals(
                &unusable,
                BASE + 100.0,
                WaitPolicy::SlotYield,
                &[],
            )
            .unwrap_err(),
            ActiveIntervalError::UnusableTerminal
        );
    }

    #[test]
    fn malformed_terminal_and_reversed_segments_are_rejected() {
        let malformed = ClanRuntimeMemberWire {
            run_started_at: Some(BASE.to_string()),
            stopped_at: Some("not-a-time".to_string()),
            ..ClanRuntimeMemberWire::default()
        };
        let mut reversed = member(20, Some(10));
        reversed.has_done_marker = true;
        assert_eq!(
            aggregate_clan_runtime(&[malformed, reversed], BASE + 100.0),
            ClanRuntimeWire::default()
        );
    }

    fn occupancy_record(
        artifact_dir: &str,
        extra_meta: serde_json::Value,
    ) -> AgentArtifactRecordWire {
        occupancy_record_on("proj", artifact_dir, extra_meta)
    }

    fn occupancy_record_on(
        project_name: &str,
        artifact_dir: &str,
        extra_meta: serde_json::Value,
    ) -> AgentArtifactRecordWire {
        let timestamp = artifact_dir.rsplit('/').next().unwrap();
        let mut meta = serde_json::json!({
            "name": timestamp,
            "pid": 100,
            "run_started_at": "2026-07-12T12:00:00+00:00"
        });
        if let Some(object) = extra_meta.as_object() {
            for (key, value) in object {
                meta[key] = value.clone();
            }
        }
        serde_json::from_value(serde_json::json!({
            "project_name": project_name,
            "project_dir": format!("/projects/{project_name}"),
            "project_file": format!("/projects/{project_name}/proj.sase"),
            "workflow_dir_name": "ace-run",
            "artifact_dir": artifact_dir,
            "timestamp": timestamp,
            "agent_meta": meta,
            "has_done_marker": extra_meta
                .get("has_done_marker")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        }))
        .unwrap()
    }

    fn always_live(_: &AgentArtifactRecordWire) -> bool {
        true
    }

    #[test]
    fn admission_predicate_excludes_serial_children_only() {
        let root = occupancy_record("/root", serde_json::json!({}));
        let parallel = occupancy_record(
            "/parallel",
            serde_json::json!({
                "parent_timestamp": "parent",
                "agent_family_parallel": true
            }),
        );
        let serial = occupancy_record(
            "/serial",
            serde_json::json!({ "parent_timestamp": "parent" }),
        );
        assert!(is_runner_eligible_record(&root));
        assert!(is_runner_eligible_record(&parallel));
        assert!(!is_runner_eligible_record(&serial));
        assert!(is_runner_occupancy_record(&serial));
    }

    #[test]
    fn standalone_agent_occupies_one_slot() {
        let records = [occupancy_record("/standalone", serde_json::json!({}))];
        assert_eq!(running_agent_slot_count(&records, always_live), 1);
    }

    #[test]
    fn root_plus_live_serial_child_occupies_exactly_one_slot() {
        let records = [
            occupancy_record(
                "/root",
                serde_json::json!({ "agent_family": "fam" }),
            ),
            occupancy_record(
                "/serial",
                serde_json::json!({
                    "agent_family": "fam",
                    "parent_timestamp": "root_ts"
                }),
            ),
        ];
        assert_eq!(running_agent_slot_count(&records, always_live), 1);
    }

    #[test]
    fn dead_root_with_live_monitor_member_still_occupies_one_slot() {
        let records = [
            occupancy_record(
                "/root",
                serde_json::json!({ "agent_family": "fam" }),
            ),
            occupancy_record(
                "/monitor",
                serde_json::json!({
                    "agent_family": "fam",
                    "monitor_id": "mon-1",
                    "run_started_at": serde_json::Value::Null
                }),
            ),
        ];
        let is_live =
            |record: &AgentArtifactRecordWire| record.artifact_dir != "/root";
        assert_eq!(running_agent_slot_count(&records, is_live), 1);
    }

    #[test]
    fn settled_monitor_with_live_followup_still_occupies_one_slot() {
        let mut records = [
            occupancy_record(
                "/root",
                serde_json::json!({ "agent_family": "fam" }),
            ),
            occupancy_record(
                "/monitor",
                serde_json::json!({
                    "agent_family": "fam",
                    "monitor_id": "mon-1"
                }),
            ),
            occupancy_record(
                "/followup",
                serde_json::json!({
                    "agent_family": "fam",
                    "parent_timestamp": "/root"
                }),
            ),
        ];
        records[0].has_done_marker = true;
        records[1].has_done_marker = true;
        assert_eq!(running_agent_slot_count(&records, always_live), 1);
    }

    #[test]
    fn two_independent_families_occupy_two_slots() {
        let records = [
            occupancy_record(
                "/a",
                serde_json::json!({ "agent_family": "fam-a" }),
            ),
            occupancy_record(
                "/b",
                serde_json::json!({ "agent_family": "fam-b" }),
            ),
        ];
        assert_eq!(running_agent_slot_count(&records, always_live), 2);
    }

    #[test]
    fn clan_members_launched_independently_count_individually() {
        let records = [
            occupancy_record("/clan-a", serde_json::json!({})),
            occupancy_record("/clan-b", serde_json::json!({})),
        ];
        assert_eq!(running_agent_slot_count(&records, always_live), 2);
    }

    #[test]
    fn live_parallel_family_members_count_individually() {
        let mut records = [
            occupancy_record(
                "/root",
                serde_json::json!({ "agent_family": "fam" }),
            ),
            occupancy_record(
                "/parallel-1",
                serde_json::json!({
                    "agent_family": "fam",
                    "parent_timestamp": "/root",
                    "agent_family_parallel": true
                }),
            ),
            occupancy_record(
                "/parallel-2",
                serde_json::json!({
                    "agent_family": "fam",
                    "parent_timestamp": "/root",
                    "agent_family_parallel": true
                }),
            ),
        ];
        records[0].has_done_marker = true;
        assert_eq!(running_agent_slot_count(&records, always_live), 2);
    }

    #[test]
    fn pending_question_on_familys_only_live_shell_frees_its_slot() {
        let mut records = [occupancy_record(
            "/root",
            serde_json::json!({ "agent_family": "fam" }),
        )];
        records[0].pending_question = Some(Default::default());
        assert_eq!(running_agent_slot_count(&records, always_live), 0);
    }

    #[test]
    fn done_marker_and_dead_pid_members_do_not_occupy() {
        let mut records = [
            occupancy_record(
                "/done",
                serde_json::json!({ "agent_family": "fam" }),
            ),
            occupancy_record(
                "/dead",
                serde_json::json!({ "agent_family": "fam" }),
            ),
        ];
        records[0].has_done_marker = true;
        let is_live =
            |record: &AgentArtifactRecordWire| record.artifact_dir != "/dead";
        assert_eq!(running_agent_slot_count(&records, is_live), 0);
    }

    #[test]
    fn monitor_member_with_pid_but_no_run_started_at_occupies_one_slot() {
        let records = [occupancy_record(
            "/monitor",
            serde_json::json!({
                "monitor_id": "mon-1",
                "run_started_at": serde_json::Value::Null
            }),
        )];
        assert_eq!(running_agent_slot_count(&records, always_live), 1);
    }

    #[test]
    fn non_agent_workflow_step_record_does_not_occupy() {
        let mut records = [occupancy_record("/step", serde_json::json!({}))];
        records[0].workflow_state = Some(
            serde_json::from_value(serde_json::json!({
                "workflow_name": "bookkeeping",
                "status": "running",
                "appears_as_agent": false
            }))
            .unwrap(),
        );
        assert_eq!(running_agent_slot_count(&records, always_live), 0);
        assert!(!is_runner_occupancy_record(&records[0]));
    }

    #[test]
    fn records_from_two_projects_sharing_a_family_name_count_separately() {
        let records = [
            occupancy_record_on(
                "proj-a",
                "/a",
                serde_json::json!({ "agent_family": "fam" }),
            ),
            occupancy_record_on(
                "proj-b",
                "/b",
                serde_json::json!({ "agent_family": "fam" }),
            ),
        ];
        assert_eq!(running_agent_slot_count(&records, always_live), 2);
    }

    #[test]
    fn family_interval_merge_unions_overlap_and_fills_monitor_gap() {
        let family = ("proj".to_string(), "fam".to_string());
        let merged = merge_family_occupancy_intervals(&[
            RunnerOccupancyContribution {
                family_key: family.clone(),
                parallel: false,
                monitor: false,
                intervals: vec![ActiveInterval {
                    start: 0.0,
                    end: 20.0,
                }],
            },
            RunnerOccupancyContribution {
                family_key: family.clone(),
                parallel: false,
                monitor: false,
                intervals: vec![ActiveInterval {
                    start: 10.0,
                    end: 30.0,
                }],
            },
            RunnerOccupancyContribution {
                family_key: family.clone(),
                parallel: false,
                monitor: true,
                intervals: vec![ActiveInterval {
                    start: 40.0,
                    end: 80.0,
                }],
            },
            RunnerOccupancyContribution {
                family_key: family,
                parallel: true,
                monitor: false,
                intervals: vec![ActiveInterval {
                    start: 70.0,
                    end: 90.0,
                }],
            },
        ]);
        let mut bounds = merged
            .iter()
            .map(|interval| (interval.start, interval.end))
            .collect::<Vec<_>>();
        bounds.sort_by(|left, right| left.0.total_cmp(&right.0));
        assert_eq!(bounds, vec![(0.0, 80.0), (70.0, 90.0)]);
    }

    #[test]
    fn occupancy_member_start_uses_earlier_monitor_artifact_timestamp() {
        let record = occupancy_record(
            "/20260712120000",
            serde_json::json!({
                "monitor_id": "mon-1",
                "run_started_at": "2026-07-12T12:00:10+00:00"
            }),
        );
        let start = occupancy_member_start(&record).unwrap();
        let parsed = parse_runtime_timestamp(&start).unwrap();
        let created =
            parse_compact_artifact_timestamp("20260712120000").unwrap();
        assert_eq!(parsed, created);
        assert!(
            created
                < parse_runtime_timestamp("2026-07-12T12:00:10+00:00").unwrap()
        );
    }
}
