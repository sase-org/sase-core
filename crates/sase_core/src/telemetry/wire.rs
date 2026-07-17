use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const TELEMETRY_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_staleness_seconds() -> u64 {
    30
}

fn default_step_seconds() -> u64 {
    60
}

fn default_raw_retention_seconds() -> u64 {
    48 * 60 * 60
}

fn default_rollup_5m_retention_seconds() -> u64 {
    30 * 24 * 60 * 60
}

fn default_rollup_1h_retention_seconds() -> u64 {
    365 * 24 * 60 * 60
}

/// The metric semantics represented by one flush sample.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryMetricKindWire {
    Counter,
    Gauge,
    Histogram,
}

impl TelemetryMetricKindWire {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            other => Err(format!("unknown telemetry metric kind {other:?}")),
        }
    }
}

/// One finite cumulative histogram bucket from a flush.
///
/// The implicit `+Inf` bucket is represented by the sample's `count` field,
/// avoiding non-finite JSON values at the wire boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryHistogramBucketWire {
    pub le: f64,
    pub count: u64,
}

/// One delta (counter/histogram) or last value (gauge) from a process flush.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySampleWire {
    /// Unix timestamp in whole seconds.
    #[serde(alias = "timestamp")]
    pub ts: i64,
    pub metric: String,
    pub kind: TelemetryMetricKindWire,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Stable process identity. Empty values are normalized to `unknown`.
    #[serde(default)]
    pub source: String,
    /// Counter delta or gauge last value. Ignored for histograms.
    #[serde(default)]
    pub value: f64,
    /// Histogram observation-count delta.
    #[serde(default)]
    pub count: u64,
    /// Histogram observation-sum delta.
    #[serde(default)]
    pub sum: f64,
    /// Histogram minimum observed by this flush, when count is non-zero.
    #[serde(default)]
    pub min: Option<f64>,
    /// Histogram maximum observed by this flush, when count is non-zero.
    #[serde(default)]
    pub max: Option<f64>,
    /// Finite cumulative bucket-count deltas, sorted by `le` on ingestion.
    #[serde(default)]
    pub buckets: Vec<TelemetryHistogramBucketWire>,
    /// Optional explicit subsystem for freshness reporting. When absent it is
    /// derived from the metric name (for example `sase_agent_*` -> `agent`).
    #[serde(default)]
    pub subsystem: Option<String>,
}

/// Local retention policy. Folding happens before deletion, so every tier is
/// queryable as one logical series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryRetentionWire {
    #[serde(default = "default_raw_retention_seconds")]
    pub raw_seconds: u64,
    #[serde(default = "default_rollup_5m_retention_seconds")]
    pub rollup_5m_seconds: u64,
    #[serde(default = "default_rollup_1h_retention_seconds")]
    pub rollup_1h_seconds: u64,
}

impl Default for TelemetryRetentionWire {
    fn default() -> Self {
        Self {
            raw_seconds: default_raw_retention_seconds(),
            rollup_5m_seconds: default_rollup_5m_retention_seconds(),
            rollup_1h_seconds: default_rollup_1h_retention_seconds(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryRecordBatchWire {
    #[serde(default)]
    pub samples: Vec<TelemetrySampleWire>,
    /// Clock injection for deterministic maintenance. Defaults to wall time.
    #[serde(default, alias = "now")]
    pub now_ts: Option<i64>,
    #[serde(default)]
    pub retention: TelemetryRetentionWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryPruneReportWire {
    pub schema_version: u32,
    pub raw_rows_folded: u64,
    pub rollup_5m_rows_folded: u64,
    pub rollup_1h_rows_deleted: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryRecordBatchResultWire {
    pub schema_version: u32,
    pub samples_recorded: u64,
    pub maintenance: TelemetryPruneReportWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryInstantQueryWire {
    pub metric: String,
    #[serde(default)]
    pub kind: Option<TelemetryMetricKindWire>,
    /// Exact-match label filters applied before grouping.
    #[serde(default, alias = "label_filters")]
    pub filters: BTreeMap<String, String>,
    /// `None` preserves every label; an empty list aggregates all labels.
    #[serde(default, alias = "group_by_labels")]
    pub group_by: Option<Vec<String>>,
    /// `sum`, `avg`, `min`, `max`, `count`, or histogram `quantile`/`pNN`.
    #[serde(default, alias = "agg")]
    pub aggregation: Option<String>,
    #[serde(default)]
    pub quantile: Option<f64>,
    #[serde(default, alias = "now")]
    pub now_ts: Option<i64>,
    #[serde(default = "default_staleness_seconds")]
    pub staleness_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryRangeQueryWire {
    pub metric: String,
    #[serde(default)]
    pub kind: Option<TelemetryMetricKindWire>,
    #[serde(alias = "start")]
    pub start_ts: i64,
    #[serde(alias = "end")]
    pub end_ts: i64,
    #[serde(default = "default_step_seconds", alias = "step")]
    pub step_seconds: u64,
    #[serde(default, alias = "label_filters")]
    pub filters: BTreeMap<String, String>,
    /// `None` preserves every label; an empty list aggregates all labels.
    #[serde(default, alias = "group_by_labels")]
    pub group_by: Option<Vec<String>>,
    /// Counters support `sum`, `rate`, `avg`, `min`, `max`, and `count`;
    /// gauges support those except `rate`; histograms also support `quantile`
    /// or `pNN` and default to `avg`.
    #[serde(default, alias = "agg")]
    pub aggregation: Option<String>,
    #[serde(default)]
    pub quantile: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryInstantValueWire {
    pub metric: String,
    pub kind: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub ts: i64,
    pub value: f64,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub sum: f64,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default)]
    pub buckets: Vec<TelemetryHistogramBucketWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryInstantQueryResultWire {
    pub schema_version: u32,
    pub metric: String,
    pub aggregation: String,
    #[serde(default)]
    pub values: Vec<TelemetryInstantValueWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryPointWire {
    pub ts: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySeriesWire {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub points: Vec<TelemetryPointWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryRangeQueryResultWire {
    pub schema_version: u32,
    pub metric: String,
    pub kind: String,
    pub aggregation: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub step_seconds: u64,
    /// `raw`, `5m`, `1h`, or `mixed`, describing the rows used.
    pub resolution: String,
    #[serde(default)]
    pub series: Vec<TelemetrySeriesWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryPruneRequestWire {
    #[serde(default, alias = "now")]
    pub now_ts: Option<i64>,
    #[serde(default)]
    pub retention: TelemetryRetentionWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryStoreStatsWire {
    pub schema_version: u32,
    pub store_path: String,
    pub db_size_bytes: u64,
    pub raw_sample_count: u64,
    pub rollup_5m_count: u64,
    pub rollup_1h_count: u64,
    pub earliest_sample_ts: Option<i64>,
    pub latest_sample_ts: Option<i64>,
    pub last_write_ts: Option<i64>,
    #[serde(default)]
    pub last_write_by_subsystem: BTreeMap<String, i64>,
}
