use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use super::sources::PerfLogRecord;
use super::wire::{
    PerfAgentLoadStageWire, PerfAgentLoadsSnapshotWire, PerfCountWire,
    PerfExternalToolWaitsSnapshotWire, PerfGitOpsSnapshotWire,
    PerfLaunchStageWire, PerfLaunchesSnapshotWire, PerfLogSourceIdWire,
    PerfLogsSnapshotWire, PerfNumericSummaryWire, PerfStageSummaryWire,
    PerfStallEventStatsWire, PerfStallsSnapshotWire,
    PerfStartupSeriesPointWire, PerfStartupSessionWire,
    PerfStartupSnapshotWire, PERF_LOGS_WIRE_SCHEMA_VERSION,
};

const STARTUP_SERIES_LIMIT: usize = 200;
const TOP_CONTEXT_LIMIT: usize = 5;
const WORST_STAGE_LIMIT: usize = 5;
const TOP_COUNT_LIMIT: usize = 8;

const STARTUP_STAGES: [(&str, &str); 4] = [
    ("process_to_mount", "process_start_to_on_mount_seconds"),
    ("mount_to_first_paint", "on_mount_to_first_paint_seconds"),
    ("visible_ready", "visible_ready_seconds"),
    ("all_surfaces_ready", "all_surfaces_ready_seconds"),
];

const STALL_EVENTS: [&str; 4] =
    ["tui_stall", "tui_pump_stall", "tui_hitch", "tui_pump_hitch"];

const RECOVERY_EVENTS: [&str; 4] = [
    "tui_stall_recovered",
    "tui_pump_stall_recovered",
    "tui_hitch_recovered",
    "tui_pump_hitch_recovered",
];

pub(crate) fn aggregate_records(
    start_ts: i64,
    end_ts: i64,
    records: &[PerfLogRecord],
    coverage: Vec<super::wire::PerfLogCoverageWire>,
) -> PerfLogsSnapshotWire {
    let mut startup = StartupAccumulator::default();
    let mut stalls = StallsAccumulator::default();
    let mut launches = LaunchAccumulator::default();
    let mut agent_loads = AgentLoadAccumulator::default();
    let mut git_ops = GitOpsAccumulator::default();
    let mut external_tools = ExternalToolAccumulator::default();

    for record in records {
        match record.source {
            PerfLogSourceIdWire::Startup => startup.record(record),
            PerfLogSourceIdWire::Stalls => stalls.record(record),
            PerfLogSourceIdWire::LaunchTiming => launches.record(record),
            PerfLogSourceIdWire::AgentLoads => agent_loads.record(record),
            PerfLogSourceIdWire::GitOps => git_ops.record(record),
            PerfLogSourceIdWire::ExternalTools => external_tools.record(record),
        }
    }

    PerfLogsSnapshotWire {
        schema_version: PERF_LOGS_WIRE_SCHEMA_VERSION,
        start_ts,
        end_ts,
        startup: startup.finish(),
        stalls: stalls.finish(),
        launches: launches.finish(),
        agent_loads: agent_loads.finish(),
        git_ops: git_ops.finish(),
        external_tool_waits: external_tools.finish(),
        coverage,
    }
}

#[derive(Default)]
struct StartupAccumulator {
    sessions: u64,
    stages: BTreeMap<&'static str, Vec<f64>>,
    visible_ready_series: Vec<PerfStartupSeriesPointWire>,
    slowest_session: Option<PerfStartupSessionWire>,
}

impl StartupAccumulator {
    fn record(&mut self, record: &PerfLogRecord) {
        self.sessions += 1;
        for (stage, field) in STARTUP_STAGES {
            if let Some(value) = json_f64(&record.value, field) {
                self.stages.entry(stage).or_default().push(value);
            }
        }
        let Some(visible_ready_seconds) =
            json_f64(&record.value, "visible_ready_seconds")
        else {
            return;
        };
        let initial_tab = json_string(&record.value, "initial_tab");
        self.visible_ready_series.push(PerfStartupSeriesPointWire {
            ts: record.ts,
            visible_ready_seconds,
            initial_tab: initial_tab.clone(),
        });
        let session = PerfStartupSessionWire {
            ts: record.ts,
            visible_ready_seconds,
            all_surfaces_ready_seconds: json_f64(
                &record.value,
                "all_surfaces_ready_seconds",
            ),
            initial_tab,
            source: json_string(&record.value, "source"),
            tier: json_string(&record.value, "tier"),
            agent_row_count: json_u64(&record.value, "agent_row_count"),
            index_row_count: json_u64(&record.value, "index_row_count"),
        };
        let replace = match self.slowest_session.as_ref() {
            Some(current) => {
                session.visible_ready_seconds > current.visible_ready_seconds
            }
            None => true,
        };
        if replace {
            self.slowest_session = Some(session);
        }
    }

    fn finish(mut self) -> PerfStartupSnapshotWire {
        self.visible_ready_series
            .sort_by(|left, right| left.ts.total_cmp(&right.ts));
        if self.visible_ready_series.len() > STARTUP_SERIES_LIMIT {
            self.visible_ready_series = self.visible_ready_series.split_off(
                self.visible_ready_series.len() - STARTUP_SERIES_LIMIT,
            );
        }
        PerfStartupSnapshotWire {
            sessions: self.sessions,
            stages: STARTUP_STAGES
                .into_iter()
                .map(|(stage, _)| PerfStageSummaryWire {
                    stage: stage.to_string(),
                    summary: numeric_summary(
                        self.stages.get(stage).map_or(&[], Vec::as_slice),
                    ),
                })
                .collect(),
            visible_ready_series: self.visible_ready_series,
            slowest_session: self.slowest_session,
        }
    }
}

#[derive(Default)]
struct StallsAccumulator {
    by_event: BTreeMap<String, StallEventAccumulator>,
    contexts: BTreeMap<String, u64>,
    recovery_count: u64,
}

#[derive(Default)]
struct StallEventAccumulator {
    count: u64,
    durations: Vec<f64>,
    last_seen_ts: Option<f64>,
    suppressed_count: u64,
}

impl StallsAccumulator {
    fn record(&mut self, record: &PerfLogRecord) {
        let Some(event) = json_string(&record.value, "event") else {
            return;
        };
        if RECOVERY_EVENTS.contains(&event.as_str()) {
            self.recovery_count += 1;
            return;
        }
        if !STALL_EVENTS.contains(&event.as_str()) {
            return;
        }
        let accumulator = self.by_event.entry(event).or_default();
        accumulator.count += 1;
        accumulator.last_seen_ts = Some(
            accumulator
                .last_seen_ts
                .map_or(record.ts, |existing| existing.max(record.ts)),
        );
        if let Some(seconds) = json_f64(&record.value, "stall_seconds") {
            accumulator.durations.push(seconds);
        }
        accumulator.suppressed_count +=
            json_u64(&record.value, "suppressed_count").unwrap_or(0);
        if let Some(current_tab) = json_string(&record.value, "current_tab") {
            *self.contexts.entry(current_tab).or_default() += 1;
        }
    }

    fn finish(self) -> PerfStallsSnapshotWire {
        PerfStallsSnapshotWire {
            events: STALL_EVENTS
                .into_iter()
                .map(|event| {
                    let accumulator = self.by_event.get(event);
                    let summary = numeric_summary(
                        accumulator.map_or(&[], |acc| acc.durations.as_slice()),
                    );
                    PerfStallEventStatsWire {
                        event: event.to_string(),
                        count: accumulator.map_or(0, |acc| acc.count),
                        worst_seconds: summary.max,
                        median_seconds: summary.p50,
                        last_seen_ts: accumulator
                            .and_then(|acc| acc.last_seen_ts),
                        suppressed_count: accumulator
                            .map_or(0, |acc| acc.suppressed_count),
                    }
                })
                .collect(),
            top_contexts: top_counts(self.contexts, TOP_CONTEXT_LIMIT),
            recovery_count: self.recovery_count,
        }
    }
}

#[derive(Default)]
struct LaunchAccumulator {
    total_ms: Vec<f64>,
    slow_stage_count: u64,
    worst_stages: Vec<PerfLaunchStageWire>,
}

impl LaunchAccumulator {
    fn record(&mut self, record: &PerfLogRecord) {
        if let Some(total_ms) = json_f64(&record.value, "total_ms") {
            self.total_ms.push(total_ms);
        }
        let mut stage_slow_count = 0;
        if let Some(stages) =
            record.value.get("stages").and_then(JsonValue::as_array)
        {
            for stage in stages {
                let Some(elapsed_ms) = json_f64(stage, "elapsed_ms") else {
                    continue;
                };
                let slow_stage =
                    stage.get("slow_stage").and_then(JsonValue::as_bool)
                        == Some(true);
                if slow_stage {
                    stage_slow_count += 1;
                }
                self.worst_stages.push(PerfLaunchStageWire {
                    ts: record.ts,
                    operation: json_string(&record.value, "operation"),
                    stage: json_string(stage, "stage")
                        .unwrap_or_else(|| "unknown".to_string()),
                    elapsed_ms,
                    slow_stage,
                });
            }
        }
        self.slow_stage_count += json_u64(&record.value, "slow_stage_count")
            .unwrap_or(stage_slow_count);
    }

    fn finish(mut self) -> PerfLaunchesSnapshotWire {
        self.worst_stages.sort_by(|left, right| {
            right.elapsed_ms.total_cmp(&left.elapsed_ms)
        });
        self.worst_stages.truncate(WORST_STAGE_LIMIT);
        PerfLaunchesSnapshotWire {
            count: self.total_ms.len() as u64,
            total_ms: numeric_summary(&self.total_ms),
            slow_stage_count: self.slow_stage_count,
            worst_stages: self.worst_stages,
        }
    }
}

#[derive(Default)]
struct AgentLoadAccumulator {
    count: u64,
    slow_stage_count: u64,
    worst_stage_seconds: Vec<f64>,
    worst_stages: Vec<PerfAgentLoadStageWire>,
}

impl AgentLoadAccumulator {
    fn record(&mut self, record: &PerfLogRecord) {
        self.count += 1;
        if let Some(stages) = record
            .value
            .get("slow_stages")
            .and_then(JsonValue::as_array)
        {
            self.slow_stage_count += stages.len() as u64;
        }
        let Some(stages) = record
            .value
            .get("stages_seconds")
            .and_then(JsonValue::as_object)
        else {
            return;
        };
        let mut record_worst: Option<f64> = None;
        for (stage, value) in stages {
            let Some(elapsed_seconds) =
                value.as_f64().filter(|value| value.is_finite())
            else {
                continue;
            };
            record_worst =
                Some(record_worst.map_or(elapsed_seconds, |existing| {
                    existing.max(elapsed_seconds)
                }));
            self.worst_stages.push(PerfAgentLoadStageWire {
                ts: record.ts,
                source: json_string(&record.value, "source"),
                load_kind: json_string(&record.value, "load_kind"),
                stage: stage.clone(),
                elapsed_seconds,
            });
        }
        if let Some(worst) = record_worst {
            self.worst_stage_seconds.push(worst);
        }
    }

    fn finish(mut self) -> PerfAgentLoadsSnapshotWire {
        self.worst_stages.sort_by(|left, right| {
            right.elapsed_seconds.total_cmp(&left.elapsed_seconds)
        });
        self.worst_stages.truncate(WORST_STAGE_LIMIT);
        PerfAgentLoadsSnapshotWire {
            count: self.count,
            slow_stage_count: self.slow_stage_count,
            worst_stage_seconds: numeric_summary(&self.worst_stage_seconds),
            worst_stages: self.worst_stages,
        }
    }
}

#[derive(Default)]
struct GitOpsAccumulator {
    count: u64,
    duration_ms: Vec<f64>,
    timeout_count: u64,
    suppressed_count: u64,
    operations: BTreeMap<String, u64>,
    statuses: BTreeMap<String, u64>,
}

impl GitOpsAccumulator {
    fn record(&mut self, record: &PerfLogRecord) {
        self.count += 1;
        if let Some(duration_ms) = json_f64(&record.value, "duration_ms") {
            self.duration_ms.push(duration_ms);
        }
        if let Some(operation) = json_string(&record.value, "operation") {
            *self.operations.entry(operation).or_default() += 1;
        }
        if let Some(status) = json_string(&record.value, "status") {
            if status == "timeout" {
                self.timeout_count += 1;
            }
            if status == "suppressed" {
                self.suppressed_count += 1;
            }
            *self.statuses.entry(status).or_default() += 1;
        }
    }

    fn finish(self) -> PerfGitOpsSnapshotWire {
        PerfGitOpsSnapshotWire {
            count: self.count,
            duration_ms: numeric_summary(&self.duration_ms),
            timeout_count: self.timeout_count,
            suppressed_count: self.suppressed_count,
            operations: top_counts(self.operations, TOP_COUNT_LIMIT),
            statuses: top_counts(self.statuses, TOP_COUNT_LIMIT),
        }
    }
}

#[derive(Default)]
struct ExternalToolAccumulator {
    count: u64,
    elapsed_seconds: Vec<f64>,
    tools: BTreeMap<String, u64>,
}

impl ExternalToolAccumulator {
    fn record(&mut self, record: &PerfLogRecord) {
        self.count += 1;
        if let Some(elapsed_seconds) =
            json_f64(&record.value, "elapsed_seconds")
        {
            self.elapsed_seconds.push(elapsed_seconds);
        }
        let name = json_string(&record.value, "tool_kind")
            .or_else(|| json_string(&record.value, "action"))
            .unwrap_or_else(|| "unknown".to_string());
        *self.tools.entry(name).or_default() += 1;
    }

    fn finish(self) -> PerfExternalToolWaitsSnapshotWire {
        PerfExternalToolWaitsSnapshotWire {
            count: self.count,
            elapsed_seconds: numeric_summary(&self.elapsed_seconds),
            tools: top_counts(self.tools, TOP_COUNT_LIMIT),
        }
    }
}

fn numeric_summary(samples: &[f64]) -> PerfNumericSummaryWire {
    let mut sorted: Vec<f64> = samples
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect();
    sorted.sort_by(f64::total_cmp);
    PerfNumericSummaryWire {
        samples: sorted.len() as u64,
        p50: percentile_sorted(&sorted, 0.5),
        p95: percentile_sorted(&sorted, 0.95),
        max: sorted.last().copied(),
    }
}

fn percentile_sorted(sorted: &[f64], quantile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let last = sorted.len() - 1;
    let index = (quantile.clamp(0.0, 1.0) * last as f64).round() as usize;
    sorted.get(index.min(last)).copied()
}

fn top_counts(
    counts: BTreeMap<String, u64>,
    limit: usize,
) -> Vec<PerfCountWire> {
    let mut values: Vec<PerfCountWire> = counts
        .into_iter()
        .map(|(name, count)| PerfCountWire { name, count })
        .collect();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    values.truncate(limit);
    values
}

fn json_f64(value: &JsonValue, field: &str) -> Option<f64> {
    value
        .get(field)
        .and_then(JsonValue::as_f64)
        .filter(|value| value.is_finite())
}

fn json_u64(value: &JsonValue, field: &str) -> Option<u64> {
    value.get(field).and_then(JsonValue::as_u64)
}

fn json_string(value: &JsonValue, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::json;

    use super::super::perf_logs_query;
    use super::super::wire::{
        default_max_bytes_per_source, default_max_records_per_source,
        PerfLogSourceIdWire, PerfLogSourceWire, PerfLogsQueryWire,
    };

    fn source(id: PerfLogSourceIdWire, path: PathBuf) -> PerfLogSourceWire {
        PerfLogSourceWire { id, path }
    }

    fn request(
        start_ts: i64,
        end_ts: i64,
        sources: Vec<PerfLogSourceWire>,
    ) -> PerfLogsQueryWire {
        PerfLogsQueryWire {
            start_ts,
            end_ts,
            max_records_per_source: default_max_records_per_source(),
            max_bytes_per_source: default_max_bytes_per_source(),
            sources,
        }
    }

    fn write_jsonl(path: &Path, records: &[serde_json::Value]) {
        let text = records
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{text}\n")).unwrap();
    }

    #[test]
    fn aggregates_mixed_timestamp_formats_and_window() {
        let temp = tempfile::tempdir().unwrap();
        let startup = temp.path().join("startup.jsonl");
        let stalls = temp.path().join("stalls.jsonl");
        let agent_loads = temp.path().join("agent_loads.jsonl");
        let launches = temp.path().join("launches.jsonl");
        let git_ops = temp.path().join("git_ops.jsonl");
        let external = temp.path().join("external.jsonl");

        write_jsonl(
            &startup,
            &[
                json!({
                    "timestamp": "1970-01-01T00:01:00Z",
                    "event": "tui_startup",
                    "visible_ready_seconds": 9.0
                }),
                json!({
                    "timestamp": "1970-01-01T00:02:30Z",
                    "event": "tui_startup",
                    "initial_tab": "agents",
                    "visible_ready_seconds": 1.5,
                    "all_surfaces_ready_seconds": 2.0,
                    "process_start_to_on_mount_seconds": 0.2,
                    "on_mount_to_first_paint_seconds": 0.3
                }),
            ],
        );
        write_jsonl(
            &stalls,
            &[json!({
                "ts": 150.25,
                "event": "tui_stall",
                "stall_seconds": 2.5,
                "current_tab": "agents"
            })],
        );
        write_jsonl(
            &agent_loads,
            &[json!({
                "timestamp": "1970-01-01T00:02:31Z",
                "event": "tui_agent_load_slow",
                "source": "startup",
                "load_kind": "full",
                "slow_stages": ["disk"],
                "stages_seconds": {"disk": 2.25, "prep": 0.1}
            })],
        );
        write_jsonl(
            &launches,
            &[json!({
                "ts": 152.0,
                "event": "launch_timing",
                "operation": "bead.work",
                "total_ms": 400.0,
                "slow_stage_count": 1,
                "stages": [{"stage": "claim", "elapsed_ms": 300.0, "slow_stage": true}]
            })],
        );
        write_jsonl(
            &git_ops,
            &[json!({
                "ts": 153.0,
                "event": "sdd_git_operation",
                "operation": "push",
                "status": "timeout",
                "duration_ms": 7000.0
            })],
        );
        write_jsonl(
            &external,
            &[json!({
                "ts": 154.0,
                "event": "external_tool_wait",
                "tool_kind": "editor",
                "elapsed_seconds": 3.0
            })],
        );

        let snapshot = perf_logs_query(request(
            100,
            200,
            vec![
                source(PerfLogSourceIdWire::Startup, startup),
                source(PerfLogSourceIdWire::Stalls, stalls),
                source(PerfLogSourceIdWire::AgentLoads, agent_loads),
                source(PerfLogSourceIdWire::LaunchTiming, launches),
                source(PerfLogSourceIdWire::GitOps, git_ops),
                source(PerfLogSourceIdWire::ExternalTools, external),
            ],
        ))
        .unwrap();

        assert_eq!(snapshot.startup.sessions, 1);
        assert_eq!(snapshot.startup.visible_ready_series.len(), 1);
        assert_eq!(snapshot.startup.stages[2].summary.p50, Some(1.5));
        assert_eq!(snapshot.stalls.events[0].count, 1);
        assert_eq!(snapshot.stalls.top_contexts[0].name, "agents");
        assert_eq!(snapshot.agent_loads.count, 1);
        assert_eq!(snapshot.agent_loads.slow_stage_count, 1);
        assert_eq!(snapshot.launches.count, 1);
        assert_eq!(snapshot.launches.slow_stage_count, 1);
        assert_eq!(snapshot.git_ops.timeout_count, 1);
        assert_eq!(snapshot.external_tool_waits.count, 1);
        let startup_coverage = snapshot
            .coverage
            .iter()
            .find(|coverage| coverage.source == PerfLogSourceIdWire::Startup)
            .unwrap();
        assert_eq!(startup_coverage.records_scanned, 2);
        assert_eq!(startup_coverage.records_in_window, 1);
    }

    #[test]
    fn byte_cap_truncates_tail_without_counting_leading_fragment() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("startup.jsonl");
        let old = json!({
            "timestamp": "1970-01-01T00:01:00Z",
            "event": "tui_startup",
            "visible_ready_seconds": 9.0,
            "padding": "x".repeat(200)
        })
        .to_string();
        let recent = json!({
            "timestamp": "1970-01-01T00:02:30Z",
            "event": "tui_startup",
            "visible_ready_seconds": 1.0
        })
        .to_string();
        fs::write(&path, format!("{old}\n{recent}\n")).unwrap();

        let mut query =
            request(100, 200, vec![source(PerfLogSourceIdWire::Startup, path)]);
        query.max_bytes_per_source = (recent.len() + 1) as u64;
        let snapshot = perf_logs_query(query).unwrap();

        assert_eq!(snapshot.startup.sessions, 1);
        assert!(snapshot.coverage[0].truncated);
        assert_eq!(snapshot.coverage[0].records_scanned, 1);
        assert_eq!(snapshot.coverage[0].malformed_skipped, 0);
    }

    #[test]
    fn malformed_timestamp_and_partial_trailing_lines_are_reported() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("stalls.jsonl");
        let valid = json!({
            "ts": 150.0,
            "event": "tui_hitch",
            "stall_seconds": 0.2,
            "suppressed_count": 3,
            "current_tab": "agents"
        })
        .to_string();
        let bad_timestamp = json!({
            "ts": "not-a-timestamp",
            "event": "tui_stall",
            "stall_seconds": 2.0
        })
        .to_string();
        fs::write(
            &path,
            format!("{valid}\n{{not json}}\n{bad_timestamp}\n{{\"ts\":151,\"event\":\"tui_pump_hitch\",\"stall_seconds\":"),
        )
        .unwrap();

        let snapshot = perf_logs_query(request(
            100,
            200,
            vec![source(PerfLogSourceIdWire::Stalls, path)],
        ))
        .unwrap();

        let coverage = &snapshot.coverage[0];
        assert_eq!(coverage.records_scanned, 4);
        assert_eq!(coverage.records_in_window, 1);
        assert_eq!(coverage.malformed_skipped, 3);
        let hitch = snapshot
            .stalls
            .events
            .iter()
            .find(|event| event.event == "tui_hitch")
            .unwrap();
        assert_eq!(hitch.count, 1);
        assert_eq!(hitch.suppressed_count, 3);
        assert_eq!(hitch.median_seconds, Some(0.2));
        assert_eq!(snapshot.stalls.top_contexts[0].name, "agents");
    }

    #[test]
    fn absent_and_empty_sources_are_covered_without_failure() {
        let temp = tempfile::tempdir().unwrap();
        let absent = temp.path().join("missing.jsonl");
        let empty = temp.path().join("empty.jsonl");
        fs::write(&empty, "").unwrap();

        let snapshot = perf_logs_query(request(
            100,
            200,
            vec![
                source(PerfLogSourceIdWire::Startup, absent),
                source(PerfLogSourceIdWire::Stalls, empty),
            ],
        ))
        .unwrap();

        assert!(!snapshot.coverage[0].present);
        assert!(snapshot.coverage[1].present);
        assert_eq!(snapshot.coverage[0].records_scanned, 0);
        assert_eq!(snapshot.coverage[1].records_scanned, 0);
        assert_eq!(snapshot.startup.sessions, 0);
        assert_eq!(snapshot.stalls.events[0].count, 0);
    }

    #[test]
    fn percentile_edges_use_nearest_rank_rounding() {
        let temp = tempfile::tempdir().unwrap();
        let one = temp.path().join("one.jsonl");
        write_jsonl(
            &one,
            &[json!({
                "ts": 150.0,
                "event": "launch_timing",
                "total_ms": 100.0
            })],
        );
        let one_snapshot = perf_logs_query(request(
            100,
            200,
            vec![source(PerfLogSourceIdWire::LaunchTiming, one)],
        ))
        .unwrap();
        assert_eq!(one_snapshot.launches.total_ms.p50, Some(100.0));
        assert_eq!(one_snapshot.launches.total_ms.p95, Some(100.0));
        assert_eq!(one_snapshot.launches.total_ms.max, Some(100.0));

        let two = temp.path().join("two.jsonl");
        write_jsonl(
            &two,
            &[
                json!({
                    "ts": 150.0,
                    "event": "launch_timing",
                    "total_ms": 100.0
                }),
                json!({
                    "ts": 151.0,
                    "event": "launch_timing",
                    "total_ms": 200.0
                }),
            ],
        );
        let two_snapshot = perf_logs_query(request(
            100,
            200,
            vec![source(PerfLogSourceIdWire::LaunchTiming, two)],
        ))
        .unwrap();
        assert_eq!(two_snapshot.launches.total_ms.p50, Some(200.0));
        assert_eq!(two_snapshot.launches.total_ms.p95, Some(200.0));
    }

    #[test]
    fn record_cap_marks_source_truncated() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("startup.jsonl");
        write_jsonl(
            &path,
            &[
                json!({
                    "timestamp": "1970-01-01T00:02:20Z",
                    "event": "tui_startup",
                    "visible_ready_seconds": 1.0
                }),
                json!({
                    "timestamp": "1970-01-01T00:02:30Z",
                    "event": "tui_startup",
                    "visible_ready_seconds": 2.0
                }),
            ],
        );

        let mut query =
            request(100, 200, vec![source(PerfLogSourceIdWire::Startup, path)]);
        query.max_records_per_source = 1;
        let snapshot = perf_logs_query(query).unwrap();

        assert!(snapshot.coverage[0].truncated);
        assert_eq!(snapshot.coverage[0].records_scanned, 1);
        assert_eq!(snapshot.startup.sessions, 1);
        assert_eq!(
            snapshot.startup.visible_ready_series[0].visible_ready_seconds,
            2.0
        );
    }
}
