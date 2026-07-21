use crate::agent_runtime::{
    derive_active_intervals, is_runner_eligible_record, ActiveInterval,
    ClanRuntimeMemberWire,
};
use crate::agent_scan::AgentArtifactRecordWire;

use super::wire::{
    AgentRunnerOccupancyWire, AgentRunnerStatsWire, AgentRunnerTrendSliceWire,
};

pub(super) const MAX_RUNNER_TREND_SLICES: usize = 512;

#[derive(Debug, Default)]
pub(super) struct RunnerStatsBuilder {
    intervals: Vec<ActiveInterval>,
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
        self.malformed_rows_skipped += 1;
    }

    pub(super) fn add_record(
        &mut self,
        record: &AgentArtifactRecordWire,
        requested_start: f64,
        requested_end: f64,
    ) {
        if !is_runner_eligible_record(record) {
            return;
        }
        let member = ClanRuntimeMemberWire::from_record(record);
        let Ok(derived) = derive_active_intervals(&member, requested_end)
        else {
            self.invalid_intervals_skipped += 1;
            return;
        };
        self.intervals
            .extend(derived.intervals.into_iter().filter_map(|interval| {
                let start = interval.start.max(requested_start);
                let end = interval.end.min(requested_end);
                (end > start).then_some(ActiveInterval { start, end })
            }));
    }

    pub(super) fn finish(
        self,
        requested_start: f64,
        requested_end: f64,
        bucket_seconds: u64,
        all_time: bool,
    ) -> Option<AgentRunnerStatsWire> {
        let effective_start = if all_time {
            self.intervals
                .iter()
                .map(|interval| interval.start)
                .min_by(f64::total_cmp)?
        } else {
            requested_start
        };
        let segments =
            occupancy_segments(&self.intervals, effective_start, requested_end);
        Some(finish_stats(
            effective_start,
            requested_end,
            bucket_seconds,
            segments,
            self.malformed_rows_skipped,
            self.invalid_intervals_skipped,
        ))
    }
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
    malformed_rows_skipped: u64,
    invalid_intervals_skipped: u64,
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
        malformed_rows_skipped,
        invalid_intervals_skipped,
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
    use super::*;

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
}
