//! Versioned wire records for published agent-run state reconciliation.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const AGENT_RUN_STATE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const AGENT_RUN_STATE_WINDOW_MIN_LIMIT: u32 = 1;
pub const AGENT_RUN_STATE_WINDOW_MAX_LIMIT: u32 = 500;
pub const AGENT_RUN_STATE_WINDOW_DEFAULT_LIMIT: u32 = 100;
pub const AGENT_RUN_STATE_MAX_DISCOVERY_HOODS: usize = 5;
pub const AGENT_RUN_STATE_BACKOFF_BASE_MS: u64 = 60_000;
pub const AGENT_RUN_STATE_BACKOFF_MAX_MS: u64 = 3_600_000;

pub const AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER: &str = "terminal_marker";
pub const AGENT_RUN_STATE_EVIDENCE_DISMISSED: &str = "dismissed";
pub const AGENT_RUN_STATE_EVIDENCE_UNFINISHED: &str = "unfinished";
pub const AGENT_RUN_STATE_EVIDENCE_MISSING: &str = "missing";
pub const AGENT_RUN_STATE_EVIDENCE_UNREADABLE: &str = "unreadable";
pub const AGENT_RUN_STATE_EVIDENCE_IDENTITY_CONFLICT: &str =
    "identity_conflict";
pub const AGENT_RUN_STATE_EVIDENCE_MALFORMED: &str = "malformed";

pub const AGENT_RUN_STATE_REASON_UPDATED_TERMINAL_MARKER: &str =
    "updated_from_terminal_marker";
pub const AGENT_RUN_STATE_REASON_UPDATED_DISMISSED: &str =
    "updated_from_dismissed";
pub const AGENT_RUN_STATE_REASON_PRESERVED_TERMINAL: &str =
    "preserved_terminal_state";
pub const AGENT_RUN_STATE_REASON_UNCHANGED: &str = "unchanged_current";
pub const AGENT_RUN_STATE_REASON_LIVE_EVIDENCE: &str = "live_evidence";
pub const AGENT_RUN_STATE_REASON_MISSING_EVIDENCE: &str = "missing_evidence";
pub const AGENT_RUN_STATE_REASON_UNREADABLE_EVIDENCE: &str =
    "unreadable_evidence";
pub const AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE: &str =
    "malformed_evidence";
pub const AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT: &str = "identity_conflict";
pub const AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL: &str =
    "conflicting_terminal_evidence";
pub const AGENT_RUN_STATE_REASON_LATER_DISMISSAL: &str =
    "verified_later_dismissal";

pub const AGENT_RUN_STATE_OUTCOME_SUCCESS: &str = "success";
pub const AGENT_RUN_STATE_OUTCOME_FAILURE: &str = "failure";

pub const AGENT_RUN_STATE_OBSERVATION_PRESENT: &str = "present";
pub const AGENT_RUN_STATE_OBSERVATION_MISSING: &str = "missing";
pub const AGENT_RUN_STATE_OBSERVATION_UNREADABLE: &str = "unreadable";
pub const AGENT_RUN_STATE_OBSERVATION_MALFORMED: &str = "malformed";

pub const AGENT_RUN_STATE_HOOD_AVAILABLE: &str = "available";
pub const AGENT_RUN_STATE_HOOD_EMPTY: &str = "empty";
pub const AGENT_RUN_STATE_HOOD_CORRUPT: &str = "corrupt";
pub const AGENT_RUN_STATE_MANIFEST_VALID: &str = "valid";
pub const AGENT_RUN_STATE_SCAN_COMPLETE: &str = "complete";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentRunStateError {
    #[error("unsupported agent run state wire schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid agent run state request: {0}")]
    InvalidRequest(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunIdentityWire {
    pub project: String,
    pub owner: String,
    pub global_name: String,
    pub source_run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateResolveRequestWire {
    pub schema_version: u32,
    pub identity: AgentRunIdentityWire,
    pub published_state: String,
    #[serde(default)]
    pub published_started_at: Option<String>,
    #[serde(default)]
    pub published_finished_at: Option<String>,
    pub evidence_kind: String,
    #[serde(default)]
    pub evidence_identity: Option<AgentRunIdentityWire>,
    #[serde(default)]
    pub evidence_state: Option<String>,
    #[serde(default)]
    pub evidence_started_at: Option<String>,
    #[serde(default)]
    pub evidence_finished_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateResolveResponseWire {
    pub schema_version: u32,
    pub selected_state: String,
    #[serde(default)]
    pub selected_started_at: Option<String>,
    #[serde(default)]
    pub selected_finished_at: Option<String>,
    pub changed: bool,
    pub reason: String,
}

/// Optional identity fields copied from a filesystem observation.
///
/// Older done markers do not carry every field. The artifact-directory and
/// metadata identity supplied by the host remains authoritative when a claim
/// is absent; a claim that is present must agree with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AgentRunStateIdentityClaimsWire {
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub global_name: Option<String>,
    #[serde(default)]
    pub local_name: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub source_run_id: Option<String>,
}

/// One uncollapsed filesystem candidate for a selected immutable run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateRawObservationWire {
    /// `terminal_marker`, `dismissed`, or `unfinished`.
    pub source: String,
    /// `present`, `missing`, `unreadable`, or `malformed`.
    pub read_status: String,
    #[serde(default)]
    pub identity_claims: Option<AgentRunStateIdentityClaimsWire>,
    #[serde(default)]
    pub outcome: Option<String>,
    /// Used for dismissed archives which already carry a projected state.
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub repeat_stopped: bool,
    #[serde(default)]
    pub monitor_state: Option<String>,
    #[serde(default)]
    pub gate_state: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<String>,
    #[serde(default)]
    pub imported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateObservationsResolveRequestWire {
    pub schema_version: u32,
    pub identity: AgentRunIdentityWire,
    #[serde(default)]
    pub local_name: Option<String>,
    pub published_state: String,
    #[serde(default)]
    pub published_started_at: Option<String>,
    #[serde(default)]
    pub published_finished_at: Option<String>,
    #[serde(default)]
    pub observations: Vec<AgentRunStateRawObservationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateObservationClassificationWire {
    pub index: u32,
    pub source: String,
    pub evidence_kind: String,
    #[serde(default)]
    pub evidence_state: Option<String>,
    #[serde(default)]
    pub evidence_started_at: Option<String>,
    #[serde(default)]
    pub evidence_finished_at: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateObservationsResolveResponseWire {
    pub schema_version: u32,
    pub selected_state: String,
    #[serde(default)]
    pub selected_started_at: Option<String>,
    #[serde(default)]
    pub selected_finished_at: Option<String>,
    pub changed: bool,
    pub reason: String,
    pub observations: Vec<AgentRunStateObservationClassificationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateWindowRowWire {
    pub local_hood: String,
    pub source_run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateWindowRequestWire {
    pub schema_version: u32,
    pub project: String,
    pub owner: String,
    #[serde(default = "default_agent_run_state_window_limit")]
    pub limit: u32,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub rows: Vec<AgentRunStateWindowRowWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateWindowResponseWire {
    pub schema_version: u32,
    pub selected: Vec<AgentRunStateWindowRowWire>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    pub pass_complete: bool,
}

/// A sorted, bounded list of owner-manifest hood names selected before any
/// hood snapshot is hydrated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateHoodWindowRequestWire {
    pub schema_version: u32,
    pub project: String,
    pub owner: String,
    /// Must be `valid`; unreadable manifests are not empty manifests.
    pub manifest_status: String,
    #[serde(default)]
    pub after: Option<String>,
    pub hoods: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateHoodWindowResponseWire {
    pub schema_version: u32,
    pub selected_hoods: Vec<String>,
    pub has_more_hoods: bool,
}

/// Run IDs read from one of the at most five selected hood snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateDiscoveredHoodWire {
    pub local_hood: String,
    /// `available`, `empty`, or `corrupt`.
    pub status: String,
    #[serde(default)]
    pub source_run_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateDiscoveryWindowRequestWire {
    pub schema_version: u32,
    pub project: String,
    pub owner: String,
    /// Must be `valid`; unreadable manifests are not empty manifests.
    pub manifest_status: String,
    /// Must be `complete`; interrupted snapshot discovery cannot advance.
    pub scan_status: String,
    #[serde(default = "default_agent_run_state_window_limit")]
    pub limit: u32,
    #[serde(default)]
    pub after: Option<String>,
    /// Names from the already validated owner manifest.
    pub manifest_hoods: Vec<String>,
    /// Snapshot observations for exactly the selected hood window.
    pub hoods: Vec<AgentRunStateDiscoveredHoodWire>,
}

fn default_agent_run_state_window_limit() -> u32 {
    AGENT_RUN_STATE_WINDOW_DEFAULT_LIMIT
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateDiscoveryDiagnosticWire {
    pub local_hood: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateDiscoveryWindowResponseWire {
    pub schema_version: u32,
    pub selected: Vec<AgentRunStateWindowRowWire>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    pub pass_complete: bool,
    /// At most `AGENT_RUN_STATE_MAX_DISCOVERY_HOODS` rows per response.
    pub diagnostics: Vec<AgentRunStateDiscoveryDiagnosticWire>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct AgentRunStateProjectIdentityWire {
    pub project: String,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateProjectWire {
    pub identity: AgentRunStateProjectIdentityWire,
    pub enabled: bool,
    pub configured: bool,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub failure_count: u32,
    #[serde(default)]
    pub retry_after_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateScheduleOutcomeWire {
    pub identity: AgentRunStateProjectIdentityWire,
    pub result: String,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub pass_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateScheduleRequestWire {
    pub schema_version: u32,
    pub now_ms: u64,
    #[serde(default)]
    pub last_selected: Option<AgentRunStateProjectIdentityWire>,
    #[serde(default)]
    pub projects: Vec<AgentRunStateProjectWire>,
    #[serde(default)]
    pub outcome: Option<AgentRunStateScheduleOutcomeWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateProjectCheckpointWire {
    pub identity: AgentRunStateProjectIdentityWire,
    #[serde(default)]
    pub cursor: Option<String>,
    pub failure_count: u32,
    pub retry_after_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateScheduleResponseWire {
    pub schema_version: u32,
    #[serde(default)]
    pub next_project: Option<AgentRunStateProjectIdentityWire>,
    #[serde(default)]
    pub updated_checkpoint: Option<AgentRunStateProjectCheckpointWire>,
}
