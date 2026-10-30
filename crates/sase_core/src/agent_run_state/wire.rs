//! Versioned wire records for published agent-run state reconciliation.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const AGENT_RUN_STATE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const AGENT_RUN_STATE_WINDOW_MIN_LIMIT: u32 = 1;
pub const AGENT_RUN_STATE_WINDOW_MAX_LIMIT: u32 = 500;
pub const AGENT_RUN_STATE_BACKOFF_BASE_MS: u64 = 60_000;
pub const AGENT_RUN_STATE_BACKOFF_MAX_MS: u64 = 3_600_000;

pub const AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER: &str = "terminal_marker";
pub const AGENT_RUN_STATE_EVIDENCE_DISMISSED: &str = "dismissed";
pub const AGENT_RUN_STATE_EVIDENCE_UNFINISHED: &str = "unfinished";
pub const AGENT_RUN_STATE_EVIDENCE_MISSING: &str = "missing";
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
pub const AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE: &str =
    "malformed_evidence";
pub const AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT: &str = "identity_conflict";
pub const AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL: &str =
    "conflicting_terminal_evidence";
pub const AGENT_RUN_STATE_REASON_LATER_DISMISSAL: &str =
    "verified_later_dismissal";

pub const AGENT_RUN_STATE_OUTCOME_SUCCESS: &str = "success";
pub const AGENT_RUN_STATE_OUTCOME_FAILURE: &str = "failure";

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
