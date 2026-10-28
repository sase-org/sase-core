//! Wire types and closed vocabularies for the Footprint ledger.

use rusqlite::ErrorCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
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
    "config",
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

/// Closed actor-scope vocabulary for report requests.
pub const ACTOR_SCOPES: &[&str] = &["you", "agents", "automation", "everyone"];

/// Closed lens vocabulary.
pub const LENSES: &[&str] = &[
    "map",
    "cold",
    "fading",
    "agent-only",
    "dead-paths",
    "friction",
    "hot",
    "coverage",
    "decisions",
];

/// Closed evidence-state vocabulary.
pub const EVIDENCE_STATES: &[&str] = &[
    "not_measured",
    "too_new",
    "used",
    "rare_by_design",
    "no_recorded_use",
    "retired",
];

/// Stored local review decisions (`clear` is a decide action, not stored).
pub const REVIEW_DECISIONS: &[&str] = &["keep", "later", "cut"];

/// Catalog JSONL schema identifier.
pub const CATALOG_SCHEMA: &str = "sase.footprint.catalog/1";

/// Weekly export-pack schema identifier.
pub const EXPORT_SCHEMA: &str = "sase.footprint.export/1";

/// Closed OS vocabulary on export packs.
pub const EXPORT_OS: &[&str] = &["linux", "darwin"];

/// Count buckets on export packs (nonzero rows only).
pub const N_BUCKETS: &[&str] = &["1", "2-5", "6-20", "21+"];

/// ISO week label `YYYY-Www`.
pub const ISO_WEEK_PATTERN: &str = r"^[0-9]{4}-W[0-9]{2}$";

/// `sase` field on an export pack: major.minor only.
pub const EXPORT_SASE_PATTERN: &str = r"^[0-9]+\.[0-9]+$";

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

fn default_window_days() -> Option<u32> {
    Some(60)
}

fn default_actor() -> String {
    "you".to_string()
}

fn default_lens() -> String {
    "map".to_string()
}

fn default_min_catalog_days() -> u32 {
    60
}

fn default_min_active_days() -> u32 {
    30
}

fn default_calibration_days() -> u32 {
    3
}

fn default_fading_days() -> u32 {
    30
}

fn default_overlay() -> Value {
    Value::Object(
        [("schema".to_string(), Value::String(OVERLAY_SCHEMA.into()))]
            .into_iter()
            .collect(),
    )
}

/// Request for `footprint_report` and `footprint_card`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequestWire {
    pub store_path: String,
    #[serde(default = "default_overlay")]
    pub overlay: Value,
    pub today: String,
    #[serde(default = "default_window_days")]
    pub window_days: Option<u32>,
    #[serde(default = "default_actor")]
    pub actor: String,
    #[serde(default = "default_lens")]
    pub lens: String,
    #[serde(default)]
    pub area: Option<String>,
    #[serde(default)]
    pub feature_id: Option<String>,
    #[serde(default = "default_min_catalog_days")]
    pub min_catalog_days: u32,
    #[serde(default = "default_min_active_days")]
    pub min_active_days: u32,
    #[serde(default = "default_calibration_days")]
    pub calibration_days: u32,
    #[serde(default = "default_fading_days")]
    pub fading_days: u32,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Trust header on a report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportHeaderWire {
    pub actor: String,
    pub window_days: Option<u32>,
    pub window_start: Option<String>,
    pub window_end: String,
    pub observed_active_days: BTreeMap<String, u32>,
    pub recording_since: Option<String>,
    pub gate_active_days: u32,
    pub gate_required_days: u32,
    pub gate_met: bool,
    pub generation: u64,
    pub imported_pack_count: u64,
    #[serde(default)]
    pub imported_day: Option<String>,
}

/// Summary tiles on a report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TilesWire {
    pub used: u64,
    pub total: u64,
    pub candidates: u64,
    pub candidates_preview: bool,
    pub rare_by_design: u64,
    pub agent_only: u64,
    pub not_measured: u64,
}

/// One map-lens area row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AreaRowWire {
    pub area: String,
    pub ns: String,
    pub total: u64,
    pub used: u64,
    pub used_pct: i64,
    pub recency_7d: u64,
    pub recency_30d: u64,
    pub recency_window: u64,
    pub recency_none: u64,
    pub recency_not_measured: u64,
}

/// Area summary on a card, including the largest unused features.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AreaSummaryWire {
    pub area: String,
    pub ns: String,
    pub total: u64,
    pub used: u64,
    pub used_pct: i64,
    pub recency_7d: u64,
    pub recency_30d: u64,
    pub recency_window: u64,
    pub recency_none: u64,
    pub recency_not_measured: u64,
    pub unused_by_loc: Vec<FeatureRowWire>,
}

/// Per-actor use counts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActorUsesWire {
    pub human: i64,
    pub agent: i64,
    pub automation: i64,
    pub unknown: i64,
}

/// One feature row in a report or card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureRowWire {
    pub id: String,
    pub label: String,
    pub ns: String,
    pub area: String,
    pub kind: String,
    pub state: String,
    pub coverage_incomplete: bool,
    pub uses: ActorUsesWire,
    pub days_used: u32,
    pub last_used_day: Option<String>,
    pub last_used_actor: Option<String>,
    pub via: BTreeMap<String, i64>,
    pub outcome: BTreeMap<String, i64>,
    pub dur_ms_sum: i64,
    pub loc: Option<i64>,
    pub parent: Option<String>,
    pub protected: Option<String>,
    pub decision: Option<String>,
    pub decision_reason: Option<String>,
    pub decided_day: Option<String>,
    pub snooze_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub friction_ratio_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_days: Option<u32>,
}

/// One day in a card series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyPointWire {
    pub day: String,
    pub n: i64,
    pub measured: bool,
}

/// One coverage-lens surface summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageSummaryWire {
    pub surface: String,
    pub measured_days: u32,
    pub active_days: u32,
    pub torn: i64,
    pub dropped: i64,
    pub unknown_ids: i64,
    pub first_measured_day: Option<String>,
    pub incomplete_days: u32,
}

/// `footprint_report` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportWire {
    pub schema_version: u32,
    pub header: ReportHeaderWire,
    pub tiles: TilesWire,
    pub areas: Vec<AreaRowWire>,
    pub rows: Vec<FeatureRowWire>,
    pub coverage: Vec<CoverageSummaryWire>,
}

/// `footprint_card` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CardWire {
    pub schema_version: u32,
    pub row: FeatureRowWire,
    pub daily_series: Vec<DailyPointWire>,
    pub first_in_catalog: Option<String>,
    pub last_in_catalog: Option<String>,
    pub parent: Option<String>,
    pub children: Vec<String>,
    pub retire_with: Vec<String>,
    pub area_summary: Option<AreaSummaryWire>,
}

/// Result of `footprint_decide`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecideResultWire {
    pub schema_version: u32,
    pub feature_id: String,
    pub decision: Option<String>,
    pub reason: Option<String>,
    pub decided_day: Option<String>,
    pub snooze_until: Option<String>,
}

/// Request for `footprint_export`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRequestWire {
    pub store_path: String,
    pub today: String,
    #[serde(default)]
    pub weeks: Option<u32>,
    pub sase: String,
    pub os: String,
}

/// One feature row inside an export pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackFeatureWire {
    pub id: String,
    pub actor: String,
    pub days: u32,
    pub n: String,
}

/// One identity-free weekly export pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackWire {
    pub schema: String,
    pub nonce: String,
    pub week: String,
    pub sase: String,
    pub os: String,
    pub observed: BTreeMap<String, u32>,
    pub features: Vec<PackFeatureWire>,
    pub not_measured: Vec<String>,
}

/// Result of `footprint_import`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportResultWire {
    pub schema_version: u32,
    pub packs_imported: u64,
    pub packs_deduped: u64,
    pub feature_rows: u64,
    pub dry_run: bool,
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
