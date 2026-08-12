//! Serde-friendly wire records for portable chop-overrun classification.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version for chop-overrun requests and verdicts.
pub const CHOP_OVERRUN_SCHEMA_VERSION: u32 = 2;

/// A path-specific structural request failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(deny_unknown_fields)]
#[error("{code} at {path}: {message}")]
pub struct ChopOverrunError {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl ChopOverrunError {
    pub(crate) fn new(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// One cached chop run entry, as the host already has it stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOverrunRunEntryWire {
    pub status: String,
    pub started_at: String,
    pub duration_ms: Option<i64>,
    #[serde(default)]
    pub script_duration_ms: Option<i64>,
}

/// Complete host-collected input to the pure classifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOverrunRequestWire {
    pub schema_version: u32,
    pub now: String,
    pub interval_seconds: i64,
    pub runs: Vec<ChopOverrunRunEntryWire>,
}

/// How stretched a chop's run history is against its lumberjack's interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopOverrunLevelWire {
    None,
    Intermittent,
    Over,
}

/// Stable version-2 overrun verdict for one chop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOverrunWire {
    pub schema_version: u32,
    pub level: ChopOverrunLevelWire,
    pub sampled_runs: u32,
    pub over_runs: u32,
    pub worst_ratio: Option<f64>,
    pub worst_blocking_ms: Option<i64>,
    pub latest_ratio: Option<f64>,
    /// One entry per raw request run, in the same order as `request.runs`.
    /// `None` marks an unsampled raw entry; `Some` carries that entry's own
    /// blocking/interval ratio so a consumer can key a per-run verdict to
    /// whichever raw run is currently selected.
    pub run_ratios: Vec<Option<f64>>,
}
