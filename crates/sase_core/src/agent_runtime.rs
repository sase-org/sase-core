//! Wall-clock runtime aggregation for agent clans and sequential families.
//!
//! A member contributes the interval from `run_started_at` through its
//! terminal marker (or `now` while it is live). Plan-review and question
//! windows are removed before member intervals are unioned, so concurrent
//! agents never double-count wall-clock time and human waits never accrue.

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::agent_scan::AgentArtifactRecordWire;

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
    ImpossibleBounds,
}

#[derive(Debug, Default)]
pub(crate) struct ActiveIntervalDerivation {
    pub(crate) intervals: Vec<ActiveInterval>,
    pub(crate) live_at_end: bool,
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
        let Ok(derived) = derive_active_intervals(member, now_epoch_seconds)
        else {
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

/// Derive the active, human-wait-free intervals for one member, capped at an
/// exclusive query end. Runner analytics clips these same intervals at its
/// lower analysis bound; clan/family runtime consumes them from run start.
pub(crate) fn derive_active_intervals(
    member: &ClanRuntimeMemberWire,
    query_end: f64,
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

    let exclusions = member_human_waits(member, start, end);
    let live_at_end =
        terminal.is_none() && !point_is_excluded(query_end, &exclusions);
    Ok(ActiveIntervalDerivation {
        intervals: subtract_intervals(
            ActiveInterval { start, end },
            &exclusions,
        ),
        live_at_end,
    })
}

/// Return whether an artifact represents a user agent governed by runner-slot
/// admission. Terminal records remain eligible for historical analytics.
pub(crate) fn is_runner_eligible_record(
    record: &AgentArtifactRecordWire,
) -> bool {
    if record.workflow_dir_name != crate::agent_scan::ACE_RUN_WORKFLOW_DIR {
        return false;
    }
    let Some(meta) = record.agent_meta.as_ref() else {
        return false;
    };
    if meta.parent_timestamp.is_some() && !meta.agent_family_parallel {
        return false;
    }
    record
        .workflow_state
        .as_ref()
        .map_or(true, |state| state.appears_as_agent)
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
    if let Some(value) = member.finished_at {
        declared = true;
        if value.is_finite() {
            candidates.push(value);
        }
    }
    if candidates.is_empty() {
        return if declared {
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

fn merge_intervals(intervals: &mut [ActiveInterval]) -> Vec<ActiveInterval> {
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
}
