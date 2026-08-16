mod aggregate;
mod sources;
mod wire;

pub use wire::{
    PerfAgentLoadStageWire, PerfAgentLoadsSnapshotWire, PerfCountWire,
    PerfExternalToolWaitsSnapshotWire, PerfGitOpsSnapshotWire,
    PerfLaunchStageWire, PerfLaunchesSnapshotWire, PerfLogCoverageWire,
    PerfLogSourceIdWire, PerfLogSourceWire, PerfLogsQueryWire,
    PerfLogsSnapshotWire, PerfNumericSummaryWire, PerfStageSummaryWire,
    PerfStallEventStatsWire, PerfStallsSnapshotWire,
    PerfStartupSeriesPointWire, PerfStartupSessionWire,
    PerfStartupSnapshotWire, PERF_LOGS_WIRE_SCHEMA_VERSION,
};

pub fn perf_logs_query(
    request: PerfLogsQueryWire,
) -> Result<PerfLogsSnapshotWire, String> {
    if request.end_ts <= request.start_ts {
        return Err(format!(
            "perf log query end_ts ({}) must be greater than start_ts ({})",
            request.end_ts, request.start_ts
        ));
    }

    let mut coverage = Vec::with_capacity(request.sources.len());
    let mut records = Vec::new();
    for source in &request.sources {
        let read = sources::read_source(source, &request)?;
        coverage.push(read.coverage);
        records.extend(read.records);
    }

    Ok(aggregate::aggregate_records(
        request.start_ts,
        request.end_ts,
        &records,
        coverage,
    ))
}
