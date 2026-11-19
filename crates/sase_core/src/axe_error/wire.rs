//! Serde-friendly wire records for error signature grouping.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version for error-group requests and responses.
pub const AXE_ERROR_WIRE_SCHEMA_VERSION: u32 = 1;

/// Maximum signature length in characters; longer signatures truncate.
pub const AXE_ERROR_MAX_SIGNATURE_CHARS: usize = 256;

/// Maximum group sample length in characters; longer samples truncate.
pub const AXE_ERROR_MAX_SAMPLE_CHARS: usize = 2000;

/// A path-specific structural request failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(deny_unknown_fields)]
#[error("{code} at {path}: {message}")]
pub struct AxeErrorWireError {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl AxeErrorWireError {
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

/// One recorded error entry, already keyed by its signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeErrorEntryWire {
    pub routine: String,
    pub job: String,
    pub signature: String,
    pub timestamp: String,
    pub sample: String,
}

/// Complete host-collected input to the pure grouping function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeErrorGroupRequestWire {
    pub schema_version: u32,
    pub entries: Vec<AxeErrorEntryWire>,
}

/// One signature group: exact count, time bounds, and one bounded sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeErrorGroupWire {
    pub routine: String,
    pub job: String,
    pub signature: String,
    pub count: u64,
    pub first_ts: String,
    pub last_ts: String,
    pub sample: String,
}

/// Grouped view of the request entries in first-seen group order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeErrorGroupResponseWire {
    pub schema_version: u32,
    pub groups: Vec<AxeErrorGroupWire>,
}
