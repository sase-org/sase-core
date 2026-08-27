use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::agent_launch::{
    list_workspace_claims_from_content, WorkspaceClaimWire,
};
use crate::agent_runtime::{
    derive_active_intervals, is_real_monitor_member_record,
    is_runner_occupancy_record, merge_family_occupancy_intervals,
    occupancy_member_start, runner_slot_family_key, ActiveInterval,
    ActiveIntervalError, ClanRuntimeMemberWire, RunnerOccupancyContribution,
    WaitPolicy,
};
use crate::agent_scan::{AgentArtifactRecordWire, RunningMarkerWire};

use super::wire::{
    AgentRunnerOccupancyWire, AgentRunnerStatsWire, AgentRunnerTrendSliceWire,
};

pub(super) const MAX_RUNNER_TREND_SLICES: usize = 512;

pub(super) trait RunnerLivenessProbe {
    fn is_live(&self, record: &AgentArtifactRecordWire) -> bool;
}

impl<F> RunnerLivenessProbe for F
where
    F: Fn(&AgentArtifactRecordWire) -> bool,
{
    fn is_live(&self, record: &AgentArtifactRecordWire) -> bool {
        self(record)
    }
}

/// Current-host liveness proof for an open-ended runner record.
///
/// A PID is necessary but not sufficient: it must still name a non-zombie
/// SASE/Python process and remain attached to the record's home running marker
/// or project workspace claim. The project-file cache is scoped to one query.
#[derive(Debug, Default)]
pub(super) struct HostRunnerLivenessProbe {
    project_claims: RefCell<BTreeMap<PathBuf, Option<Vec<WorkspaceClaimWire>>>>,
}

impl RunnerLivenessProbe for HostRunnerLivenessProbe {
    fn is_live(&self, record: &AgentArtifactRecordWire) -> bool {
        if record.has_done_marker || record.done.is_some() {
            return false;
        }
        let meta = match record.agent_meta.as_ref() {
            Some(meta) => meta,
            None => return false,
        };
        if meta
            .stopped_at
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        {
            return false;
        }

        let pid = match meta
            .pid
            .or_else(|| record.running.as_ref().and_then(|marker| marker.pid))
        {
            Some(pid) if pid > 1 && pid <= i64::from(i32::MAX) => pid,
            _ => return false,
        };
        if !process_is_live_agent(pid as i32) {
            return false;
        }

        if record.project_name == "home" {
            return home_running_marker_matches(record, pid);
        }
        self.workspace_claim_matches(record, pid)
    }
}

impl HostRunnerLivenessProbe {
    fn workspace_claim_matches(
        &self,
        record: &AgentArtifactRecordWire,
        pid: i64,
    ) -> bool {
        let project_file = PathBuf::from(&record.project_file);
        let mut cache = self.project_claims.borrow_mut();
        let claims = cache.entry(project_file.clone()).or_insert_with(|| {
            fs::read_to_string(&project_file)
                .ok()
                .map(|content| list_workspace_claims_from_content(&content))
        });
        let Some(claims) = claims.as_ref() else {
            return false;
        };
        let workspace_num = record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.workspace_num)
            .and_then(|value| u32::try_from(value).ok());
        claims.iter().any(|claim| {
            let identity_matches = match claim.artifacts_timestamp.as_deref() {
                Some(timestamp) => timestamp == record.timestamp,
                None => workspace_num
                    .is_some_and(|value| value == claim.workspace_num),
            };
            identity_matches && i64::from(claim.pid) == pid
        })
    }
}

fn home_running_marker_matches(
    record: &AgentArtifactRecordWire,
    pid: i64,
) -> bool {
    let path = Path::new(&record.artifact_dir).join("running.json");
    let Ok(content) = fs::read_to_string(path) else {
        return false;
    };
    serde_json::from_str::<RunningMarkerWire>(&content)
        .ok()
        .and_then(|marker| marker.pid)
        == Some(pid)
}

fn process_is_live_agent(pid: i32) -> bool {
    if !process_exists(pid) {
        return false;
    }

    #[cfg(target_os = "linux")]
    {
        let proc_dir = PathBuf::from(format!("/proc/{pid}"));
        match fs::read_to_string(proc_dir.join("status")) {
            Ok(status) => {
                if status.lines().any(|line| {
                    line.strip_prefix("State:").is_some_and(|value| {
                        value.trim_start().starts_with('Z')
                    })
                }) {
                    return false;
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => return false,
            Err(_) => {}
        }

        match fs::read(proc_dir.join("cmdline")) {
            Ok(command) => {
                if !command.windows(4).any(|part| part == b"sase")
                    && !command.windows(6).any(|part| part == b"python")
                {
                    return false;
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => return false,
            Err(_) => {}
        }
    }

    true
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    // SAFETY: signal 0 performs an existence/permission probe and does not
    // deliver a signal. `pid` was validated as a positive process ID.
    let result = unsafe { libc::kill(pid, 0) };
    if result == 0 {
        return true;
    }
    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(not(unix))]
fn process_exists(_pid: i32) -> bool {
    false
}

#[derive(Debug, Default)]
pub(super) struct RunnerStatsBuilder {
    contributions: Vec<RunnerOccupancyContribution>,
    diagnostics: RunnerDiagnostics,
}

#[derive(Debug, Default, Clone, Copy)]
struct RunnerDiagnostics {
    lanes_counted: u64,
    lanes_without_end_skipped: u64,
    user_hidden_skipped: u64,
    malformed_rows_skipped: u64,
    invalid_intervals_skipped: u64,
}

#[derive(Debug, Clone, Copy)]
struct OccupancySegment {
    start: f64,
    end: f64,
    runners: u64,
}

impl RunnerStatsBuilder {
    pub(super) fn record_malformed_row(&mut self) {
        self.diagnostics.malformed_rows_skipped += 1;
    }

    pub(super) fn record_user_hidden(&mut self) {
        self.diagnostics.user_hidden_skipped += 1;
    }

    pub(super) fn add_record(
        &mut self,
        record: &AgentArtifactRecordWire,
        requested_start: f64,
        requested_end: f64,
        resolved_question_answers: &[f64],
        liveness: &dyn RunnerLivenessProbe,
    ) {
        if !is_runner_occupancy_record(record) {
            return;
        }
        let member = occupancy_runtime_member(record);
        let derived = match derive_active_intervals(
            &member,
            requested_end,
            WaitPolicy::SlotYield,
            resolved_question_answers,
        ) {
            Ok(derived) => derived,
            Err(ActiveIntervalError::InvalidStart) => return,
            Err(ActiveIntervalError::UnusableTerminal) => {
                self.diagnostics.lanes_without_end_skipped += 1;
                return;
            }
            Err(
                ActiveIntervalError::InvalidTerminal
                | ActiveIntervalError::ImpossibleBounds,
            ) => {
                self.diagnostics.invalid_intervals_skipped += 1;
                return;
            }
        };
        if derived.open_ended && !liveness.is_live(record) {
            self.diagnostics.lanes_without_end_skipped += 1;
            return;
        }
        if derived.intervals.iter().any(|interval| {
            interval.end > requested_start && interval.start < requested_end
        }) {
            self.diagnostics.lanes_counted += 1;
        }
        self.push_contribution(record, derived.intervals);
    }

    pub(super) fn add_handoff_carry_record(
        &mut self,
        record: &AgentArtifactRecordWire,
        requested_end: f64,
        resolved_question_answers: &[f64],
    ) {
        if !is_runner_occupancy_record(record) {
            return;
        }
        let member = occupancy_runtime_member(record);
        if let Ok(derived) = derive_active_intervals(
            &member,
            requested_end,
            WaitPolicy::SlotYield,
            resolved_question_answers,
        ) {
            self.push_contribution(record, derived.intervals);
        }
    }

    fn push_contribution(
        &mut self,
        record: &AgentArtifactRecordWire,
        intervals: Vec<ActiveInterval>,
    ) {
        let meta = record.agent_meta.as_ref();
        self.contributions.push(RunnerOccupancyContribution {
            family_key: runner_slot_family_key(record),
            parallel: meta.is_some_and(|value| value.agent_family_parallel),
            monitor: is_real_monitor_member_record(record),
            intervals,
        });
    }

    pub(super) fn finish(
        self,
        requested_start: f64,
        requested_end: f64,
        bucket_seconds: u64,
        all_time: bool,
    ) -> Option<AgentRunnerStatsWire> {
        let intervals = merge_family_occupancy_intervals(&self.contributions);
        let effective_start = if all_time {
            intervals
                .iter()
                .map(|interval| interval.start)
                .min_by(f64::total_cmp)?
        } else {
            requested_start
        };
        let segments =
            occupancy_segments(&intervals, effective_start, requested_end);
        Some(finish_stats(
            effective_start,
            requested_end,
            bucket_seconds,
            segments,
            self.diagnostics,
        ))
    }
}

fn occupancy_runtime_member(
    record: &AgentArtifactRecordWire,
) -> ClanRuntimeMemberWire {
    let mut member = ClanRuntimeMemberWire::from_record(record);
    if let Some(start) = occupancy_member_start(record) {
        member.run_started_at = Some(start);
    }
    member
}

fn occupancy_segments(
    intervals: &[ActiveInterval],
    analysis_start: f64,
    analysis_end: f64,
) -> Vec<OccupancySegment> {
    let mut events = Vec::with_capacity(intervals.len() * 2);
    for interval in intervals {
        let start = interval.start.max(analysis_start);
        let end = interval.end.min(analysis_end);
        if end > start {
            events.push((start, 1i64));
            events.push((end, -1i64));
        }
    }
    events.sort_by(|left, right| left.0.total_cmp(&right.0));

    let mut segments = Vec::new();
    let mut cursor = analysis_start;
    let mut occupancy = 0i64;
    let mut index = 0usize;
    while index < events.len() {
        let event_ts = events[index].0;
        if event_ts > cursor {
            append_segment(&mut segments, cursor, event_ts, occupancy as u64);
            cursor = event_ts;
        }
        let mut delta = 0i64;
        while index < events.len()
            && events[index].0.total_cmp(&event_ts).is_eq()
        {
            delta += events[index].1;
            index += 1;
        }
        occupancy += delta;
        debug_assert!(occupancy >= 0);
    }
    if cursor < analysis_end {
        append_segment(&mut segments, cursor, analysis_end, occupancy as u64);
    }
    segments
}

fn append_segment(
    segments: &mut Vec<OccupancySegment>,
    start: f64,
    end: f64,
    runners: u64,
) {
    if end <= start {
        return;
    }
    if let Some(previous) = segments.last_mut() {
        if previous.runners == runners && previous.end == start {
            previous.end = end;
            return;
        }
    }
    segments.push(OccupancySegment {
        start,
        end,
        runners,
    });
}

fn finish_stats(
    analysis_start: f64,
    analysis_end: f64,
    bucket_seconds: u64,
    segments: Vec<OccupancySegment>,
    diagnostics: RunnerDiagnostics,
) -> AgentRunnerStatsWire {
    let span = analysis_end - analysis_start;
    let peak_runners = segments
        .iter()
        .map(|segment| segment.runners)
        .max()
        .unwrap_or(0);
    let mut seconds_by_occupancy = vec![0.0; peak_runners as usize + 1];
    for segment in &segments {
        seconds_by_occupancy[segment.runners as usize] +=
            segment.end - segment.start;
    }

    // Assign any sub-nanosecond summation drift to the final observed level
    // so the dense distribution remains exactly tied to the declared span
    // without manufacturing a negative idle duration in a fully busy range.
    let measured: f64 = seconds_by_occupancy.iter().sum();
    let final_occupancy = segments
        .last()
        .map(|segment| segment.runners as usize)
        .unwrap_or(0);
    seconds_by_occupancy[final_occupancy] += span - measured;
    let runner_seconds = seconds_by_occupancy
        .iter()
        .enumerate()
        .map(|(runners, seconds)| runners as f64 * seconds)
        .sum::<f64>();
    let busy_seconds = span - seconds_by_occupancy[0];
    let peak_seconds = seconds_by_occupancy[peak_runners as usize];
    let distribution = seconds_by_occupancy
        .into_iter()
        .enumerate()
        .map(|(runners, seconds)| AgentRunnerOccupancyWire {
            runners: runners as u64,
            seconds,
            share: seconds / span,
        })
        .collect();

    AgentRunnerStatsWire {
        start_ts: analysis_start,
        end_ts: analysis_end,
        peak_runners,
        peak_seconds,
        average_runners: runner_seconds / span,
        busy_seconds,
        busy_share: busy_seconds / span,
        runner_seconds,
        distribution,
        trend: trend_slices(
            &segments,
            analysis_start,
            analysis_end,
            bucket_seconds,
        ),
        lanes_counted: diagnostics.lanes_counted,
        lanes_without_end_skipped: diagnostics.lanes_without_end_skipped,
        user_hidden_skipped: diagnostics.user_hidden_skipped,
        malformed_rows_skipped: diagnostics.malformed_rows_skipped,
        invalid_intervals_skipped: diagnostics.invalid_intervals_skipped,
    }
}

fn trend_slices(
    segments: &[OccupancySegment],
    analysis_start: f64,
    analysis_end: f64,
    requested_bucket_seconds: u64,
) -> Vec<AgentRunnerTrendSliceWire> {
    let span = analysis_end - analysis_start;
    let requested_width = requested_bucket_seconds as f64;
    let requested_count = (span / requested_width).ceil().max(1.0) as usize;
    let multiplier = requested_count.div_ceil(MAX_RUNNER_TREND_SLICES);
    let width = requested_width * multiplier as f64;
    let slice_count =
        ((span / width).ceil().max(1.0) as usize).min(MAX_RUNNER_TREND_SLICES);

    let mut slices = (0..slice_count)
        .map(|index| {
            let start = analysis_start + width * index as f64;
            let end = if index + 1 == slice_count {
                analysis_end
            } else {
                (start + width).min(analysis_end)
            };
            AgentRunnerTrendSliceWire {
                start_ts: start,
                end_ts: end,
                ..AgentRunnerTrendSliceWire::default()
            }
        })
        .collect::<Vec<_>>();

    // Occupancy segments and slices are both chronological partitions, so a
    // monotonic merge visits each boundary once instead of rescanning every
    // segment for every slice.
    let mut segment_index = 0usize;
    for slice in &mut slices {
        while let Some(segment) = segments.get(segment_index) {
            if segment.start >= slice.end_ts {
                break;
            }
            let overlap_start = slice.start_ts.max(segment.start);
            let overlap_end = slice.end_ts.min(segment.end);
            if overlap_end > overlap_start {
                let seconds = overlap_end - overlap_start;
                slice.peak_runners = slice.peak_runners.max(segment.runners);
                if segment.runners > 0 {
                    slice.busy_seconds += seconds;
                }
                slice.runner_seconds += segment.runners as f64 * seconds;
            }
            if segment.end <= slice.end_ts {
                segment_index += 1;
            } else {
                break;
            }
        }
        slice.average_runners =
            slice.runner_seconds / (slice.end_ts - slice.start_ts);
    }
    slices
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn open_record(name: &str, start: Option<&str>) -> AgentArtifactRecordWire {
        serde_json::from_value(json!({
            "project_name": "proj",
            "project_dir": "/tmp/proj",
            "project_file": "/tmp/proj/proj.sase",
            "workflow_dir_name": "ace-run",
            "artifact_dir": format!("/tmp/proj/artifacts/ace-run/{name}"),
            "timestamp": name,
            "agent_meta": {
                "name": name,
                "run_started_at": start
            }
        }))
        .unwrap()
    }

    #[test]
    fn simultaneous_boundaries_do_not_create_phantom_occupancy() {
        let segments = occupancy_segments(
            &[
                ActiveInterval {
                    start: 0.0,
                    end: 10.0,
                },
                ActiveInterval {
                    start: 10.0,
                    end: 20.0,
                },
            ],
            0.0,
            20.0,
        );
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].runners, 1);
        assert_eq!(segments[0].start, 0.0);
        assert_eq!(segments[0].end, 20.0);
    }

    #[test]
    fn trend_is_strictly_bounded_and_keeps_partial_final_slice() {
        let segments = occupancy_segments(
            &[ActiveInterval {
                start: 0.0,
                end: 10_000.5,
            }],
            0.0,
            10_000.5,
        );
        let trend = trend_slices(&segments, 0.0, 10_000.5, 1);
        assert!(trend.len() <= MAX_RUNNER_TREND_SLICES);
        assert_eq!(trend.first().unwrap().start_ts, 0.0);
        assert_eq!(trend.last().unwrap().end_ts, 10_000.5);
        assert!(trend
            .windows(2)
            .all(|pair| pair[0].end_ts == pair[1].start_ts));
        assert!(
            trend.last().unwrap().end_ts - trend.last().unwrap().start_ts
                < 20.0
        );
    }

    #[test]
    fn open_intervals_require_liveness_and_missing_starts_are_not_invalid() {
        let mut builder = RunnerStatsBuilder::default();
        let live = |record: &AgentArtifactRecordWire| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.as_deref())
                == Some("live")
        };
        builder.add_record(
            &open_record("stale", Some("0")),
            0.0,
            100.0,
            &[],
            &live,
        );
        builder.add_record(
            &open_record("live", Some("20")),
            0.0,
            100.0,
            &[],
            &live,
        );
        builder.add_record(&open_record("never", None), 0.0, 100.0, &[], &live);

        let result = builder.finish(0.0, 100.0, 100, false).unwrap();
        assert_eq!(result.runner_seconds, 80.0);
        assert_eq!(result.distribution[0].seconds, 20.0);
        assert_eq!(result.distribution[1].seconds, 80.0);
        assert_eq!(result.lanes_counted, 1);
        assert_eq!(result.lanes_without_end_skipped, 1);
        assert_eq!(result.invalid_intervals_skipped, 0);
    }

    #[test]
    fn host_liveness_requires_current_matching_claim_identity() {
        let tmp = tempdir().unwrap();
        let project_file = tmp.path().join("proj.sase");
        let artifact_dir = tmp.path().join("artifacts/ace-run/20260722010101");
        fs::create_dir_all(&artifact_dir).unwrap();
        let pid = std::process::id();
        fs::write(
            &project_file,
            format!("RUNNING:\n  #7 | {pid} | run | demo | 20260722010101\n"),
        )
        .unwrap();
        let mut record = open_record("live", Some("10"));
        record.project_file = project_file.to_string_lossy().into_owned();
        record.artifact_dir = artifact_dir.to_string_lossy().into_owned();
        record.timestamp = "20260722010101".to_string();
        let meta = record.agent_meta.as_mut().unwrap();
        meta.pid = Some(i64::from(pid));
        meta.workspace_num = Some(7);

        assert!(HostRunnerLivenessProbe::default().is_live(&record));

        fs::write(
            &project_file,
            format!(
                "RUNNING:\n  #7 | {} | run | demo | 20260722010101\n",
                pid + 1
            ),
        )
        .unwrap();
        assert!(!HostRunnerLivenessProbe::default().is_live(&record));

        record.project_name = "home".to_string();
        fs::write(
            artifact_dir.join("running.json"),
            serde_json::to_vec(&json!({"pid": pid})).unwrap(),
        )
        .unwrap();
        assert!(HostRunnerLivenessProbe::default().is_live(&record));
        fs::write(
            artifact_dir.join("running.json"),
            serde_json::to_vec(&json!({"pid": pid + 1})).unwrap(),
        )
        .unwrap();
        assert!(!HostRunnerLivenessProbe::default().is_live(&record));
    }

    fn family_record(
        name: &str,
        start: &str,
        extra_meta: serde_json::Value,
    ) -> AgentArtifactRecordWire {
        let mut record = open_record(name, Some(start));
        let meta = record.agent_meta.as_mut().unwrap();
        if let Some(family) = extra_meta
            .get("agent_family")
            .and_then(|value| value.as_str())
        {
            meta.agent_family = Some(family.to_string());
        }
        if extra_meta
            .get("agent_family_parallel")
            .and_then(|value| value.as_bool())
            == Some(true)
        {
            meta.agent_family_parallel = true;
        }
        if let Some(parent) = extra_meta
            .get("parent_timestamp")
            .and_then(|value| value.as_str())
        {
            meta.parent_timestamp = Some(parent.to_string());
        }
        if let Some(role) = extra_meta
            .get("agent_family_role")
            .and_then(|value| value.as_str())
        {
            meta.agent_family_role = Some(role.to_string());
        }
        if let Some(monitor_id) = extra_meta
            .get("monitor_id")
            .and_then(|value| value.as_str())
        {
            meta.family_shell = Some(crate::agent_scan::FamilyShellWire {
                kind: "monitor".to_string(),
                id: Some(monitor_id.to_string()),
                ..Default::default()
            });
        }
        if let Some(end) = extra_meta
            .get("stopped_at")
            .and_then(|value| value.as_str())
        {
            meta.stopped_at = Some(end.to_string());
        }
        record
    }

    #[test]
    fn overlapping_serial_family_shells_count_as_one_slot() {
        let mut builder = RunnerStatsBuilder::default();
        let live = |_: &AgentArtifactRecordWire| false;
        builder.add_record(
            &family_record(
                "root",
                "0",
                json!({
                    "agent_family": "fam",
                    "stopped_at": "50"
                }),
            ),
            0.0,
            100.0,
            &[],
            &live,
        );
        builder.add_record(
            &family_record(
                "serial",
                "20",
                json!({
                    "agent_family": "fam",
                    "parent_timestamp": "root",
                    "stopped_at": "60"
                }),
            ),
            0.0,
            100.0,
            &[],
            &live,
        );

        let result = builder.finish(0.0, 100.0, 100, false).unwrap();
        assert_eq!(result.runner_seconds, 60.0);
        assert_eq!(result.peak_runners, 1);
        assert_eq!(result.distribution[0].seconds, 40.0);
        assert_eq!(result.distribution[1].seconds, 60.0);
        assert_eq!(result.lanes_counted, 2);
    }

    #[test]
    fn monitor_handoff_gap_does_not_reopen_family_interval() {
        let mut builder = RunnerStatsBuilder::default();
        let live = |_: &AgentArtifactRecordWire| false;
        builder.add_record(
            &family_record(
                "20260710000000",
                "0",
                json!({
                    "agent_family": "fam",
                    "stopped_at": "20"
                }),
            ),
            0.0,
            100.0,
            &[],
            &live,
        );
        builder.add_record(
            &family_record(
                "20260710000030",
                "30",
                json!({
                    "agent_family": "fam",
                    "agent_family_role": "monitor",
                    "monitor_id": "mon-1",
                    "stopped_at": "80"
                }),
            ),
            0.0,
            100.0,
            &[],
            &live,
        );

        let result = builder.finish(0.0, 100.0, 100, false).unwrap();
        assert_eq!(result.runner_seconds, 80.0);
        assert_eq!(result.peak_runners, 1);
        assert_eq!(result.distribution[0].seconds, 20.0);
        assert_eq!(result.distribution[1].seconds, 80.0);
    }

    #[test]
    fn inherited_monitor_id_does_not_fill_family_gap() {
        let mut builder = RunnerStatsBuilder::default();
        let live = |_: &AgentArtifactRecordWire| false;
        builder.add_record(
            &family_record(
                "root",
                "0",
                json!({
                    "agent_family": "fam",
                    "stopped_at": "20"
                }),
            ),
            0.0,
            100.0,
            &[],
            &live,
        );
        builder.add_record(
            &family_record(
                "ordinary",
                "30",
                json!({
                    "agent_family": "fam",
                    "agent_family_role": "code",
                    "monitor_id": "mon-1",
                    "stopped_at": "80"
                }),
            ),
            0.0,
            100.0,
            &[],
            &live,
        );

        let result = builder.finish(0.0, 100.0, 100, false).unwrap();
        assert_eq!(result.runner_seconds, 70.0);
        assert_eq!(result.peak_runners, 1);
        assert_eq!(
            result
                .distribution
                .iter()
                .map(|row| (row.runners, row.seconds))
                .collect::<Vec<_>>(),
            vec![(0, 30.0), (1, 70.0)]
        );
    }
}
