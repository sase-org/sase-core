mod store;
mod wire;

pub use store::{
    cleanup_matching_labels, prune, query_instant, query_range, record_batch,
    store_stats, TELEMETRY_MAX_BUSY_TIMEOUT,
};
pub use wire::{
    TelemetryCleanupReportWire, TelemetryCleanupRequestWire,
    TelemetryHistogramBucketWire, TelemetryInstantQueryResultWire,
    TelemetryInstantQueryWire, TelemetryInstantValueWire,
    TelemetryMetricKindWire, TelemetryPointWire, TelemetryPruneReportWire,
    TelemetryPruneRequestWire, TelemetryRangeQueryResultWire,
    TelemetryRangeQueryWire, TelemetryRecordBatchResultWire,
    TelemetryRecordBatchWire, TelemetryRetentionWire, TelemetrySampleWire,
    TelemetrySeriesWire, TelemetryStoreStatsWire,
    TELEMETRY_WIRE_SCHEMA_VERSION,
};
