//! Wire types and closed vocabularies for the Footprint ledger.

use rusqlite::ErrorCode;
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;

/// Wire and store schema version for the Footprint ledger.
pub const FOOTPRINT_WIRE_SCHEMA_VERSION: u32 = 1;

/// Return the Footprint wire schema version.
pub fn footprint_wire_schema_version() -> u32 {
    FOOTPRINT_WIRE_SCHEMA_VERSION
}

/// Closed actor vocabulary.
pub const ACTORS: &[&str] = &["human", "agent", "automation", "unknown"];

/// Closed via vocabulary.
pub const VIAS: &[&str] = &[
    "key",
    "mouse",
    "palette",
    "mode",
    "cmdline",
    "cli",
    "completion",
    "editor",
    "chat",
    "prompt",
    "history",
    "auto",
];

/// Closed outcome vocabulary.
pub const OUTCOMES: &[&str] =
    &["ok", "error", "usage_error", "help", "cancelled"];

/// Closed origin vocabulary.
pub const ORIGINS: &[&str] = &[
    "live",
    "derived.macros",
    "derived.skills",
    "derived.config",
    "derived.auto",
    "derived.history",
];

/// Closed surface-family vocabulary.
pub const SURFACES: &[&str] =
    &["cli", "tui", "macro", "skill", "config", "auto", "plugin"];

/// Closed catalog `kind` vocabulary.
pub const KINDS: &[&str] = &[
    "command", "option", "action", "mode_key", "view", "modal", "macro",
    "skill", "config", "auto", "plugin",
];

/// Closed overlay `protected` vocabulary.
pub const PROTECTED_CLASSES: &[&str] =
    &["safety", "recovery", "bootstrap", "infra"];

/// Closed overlay `keep` vocabulary.
pub const KEEP_REASONS: &[&str] = &[
    "rare_by_design",
    "safety",
    "agent_facing",
    "api_contract",
    "other",
];

/// Catalog JSONL schema identifier.
pub const CATALOG_SCHEMA: &str = "sase.footprint.catalog/1";

/// Overlay schema identifier.
pub const OVERLAY_SCHEMA: &str = "sase.footprint.overlay/1";

/// Feature-ID pattern: `<ns>:<path>` with no spaces or uppercase.
pub const FEATURE_ID_PATTERN: &str = r"^[a-z]+:[a-z0-9_.~#-]+$";

/// Option `dest` names expanded from spool `opt` arrays.
pub const OPTION_DEST_PATTERN: &str = r"^[a-z][a-z0-9_]*$";

/// Local calendar date `YYYY-MM-DD`.
pub const DAY_PATTERN: &str = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$";

/// sase version token stored on spool lines.
pub const VERSION_PATTERN: &str = r"^[0-9A-Za-z._+-]{1,64}$";

/// Errors from Footprint store, fold, and catalog operations.
#[derive(Debug, Error)]
pub enum FootprintError {
    #[error("footprint I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("footprint sqlite failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("{0}")]
    Validation(String),
    #[error("timed out waiting for the footprint ledger")]
    Busy,
    #[error(
        "footprint store schema version {actual} is newer than supported version {expected}"
    )]
    NewerSchema { expected: u32, actual: u32 },
}

impl FootprintError {
    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub(crate) fn from_sqlite(error: rusqlite::Error) -> Self {
        match error.sqlite_error_code() {
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
                Self::Busy
            }
            _ => Self::Sqlite(error),
        }
    }
}

/// Result of one checkpointed spool fold.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldResultWire {
    pub schema_version: u32,
    pub files_folded: u64,
    pub records_folded: u64,
    pub torn: u64,
    pub dropped: u64,
    pub unknown_ids: u64,
    pub files_deleted: u64,
    pub files_expired: u64,
}

/// Catalog presence of one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogStatusWire {
    pub schema_version: u32,
    pub rev: String,
    pub known: bool,
    pub count: u64,
}

/// Result of `footprint_clear`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClearResultWire {
    pub schema_version: u32,
    pub generation: u64,
}

/// Result of `footprint_prune`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneResultWire {
    pub schema_version: u32,
    pub daily_deleted: u64,
    pub coverage_deleted: u64,
}

/// One coverage row as stored and as returned in status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageRowWire {
    pub day: String,
    pub surface: String,
    pub origin: String,
    pub measured: i64,
    pub human_active: i64,
    pub records: i64,
    pub torn: i64,
    pub dropped: i64,
    pub unknown_ids: i64,
    pub incomplete: i64,
}

/// Pending spool file observed by status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolPendingWire {
    pub file: String,
    pub bytes_pending: u64,
}

/// Ledger status snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusWire {
    pub schema_version: u32,
    pub generation: u64,
    pub first_record_day: Option<String>,
    pub last_fold_at: Option<String>,
    pub spool_files_pending: u64,
    pub spool_bytes_pending: u64,
    pub spool_pending: Vec<SpoolPendingWire>,
    pub ledger_bytes: u64,
    pub catalog_rev: Option<String>,
    pub catalog_count: u64,
    pub coverage: Vec<CoverageRowWire>,
    pub torn_total: u64,
    pub dropped_total: u64,
    pub unknown_ids_total: u64,
}

/// One derived daily aggregate row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedDailyRowWire {
    pub day: String,
    pub feature_id: String,
    pub actor: String,
    pub via: String,
    pub outcome: String,
    #[serde(default)]
    pub n: i64,
    #[serde(default)]
    pub dur_ms_sum: i64,
    #[serde(default)]
    pub dur_ms_max: i64,
}

/// One derived coverage row (origin is the upsert argument).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedCoverageRowWire {
    pub day: String,
    pub surface: String,
    pub measured: i64,
    pub human_active: i64,
    pub records: i64,
    pub torn: i64,
    pub dropped: i64,
    pub unknown_ids: i64,
    pub incomplete: i64,
}

/// One catalog JSONL entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntryWire {
    pub id: String,
    pub ns: String,
    pub area: String,
    pub kind: String,
    pub label: String,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub src: Option<String>,
}

/// Catalog JSONL header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogHeaderWire {
    pub schema: String,
    pub rev: String,
    pub count: u64,
    #[serde(default)]
    pub sase_version: Option<String>,
}

pub(crate) fn require_member(
    value: &str,
    allowed: &[&str],
    label: &str,
) -> Result<(), FootprintError> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        Err(FootprintError::validation(format!(
            "invalid {label} {value:?}"
        )))
    }
}

pub(crate) fn ns_of(feature_id: &str) -> Option<&str> {
    feature_id.split_once(':').map(|(ns, _)| ns)
}

pub(crate) fn surface_for_ns(ns: &str) -> Option<&'static str> {
    if ns == "view" {
        return Some("tui");
    }
    SURFACES.iter().copied().find(|surface| *surface == ns)
}

pub(crate) fn remap_unknown_id(feature_id: &str) -> String {
    match ns_of(feature_id) {
        Some(ns) => format!("{ns}:~other"),
        None => "unknown:~other".to_string(),
    }
}
