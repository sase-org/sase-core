//! Versioned records for Agent Hijack policy and recovery.

use serde::{Deserialize, Serialize};

pub const AGENT_HIJACK_WIRE_SCHEMA_VERSION: u32 = 1;

pub const HIJACK_REQUESTED: &str = "requested";
pub const HIJACK_STOPPING: &str = "stopping";
pub const HIJACK_HELD: &str = "held";
pub const HIJACK_RESOLVING: &str = "resolving";
pub const HIJACK_RESOLVED: &str = "resolved";
pub const HIJACK_DISMISSED: &str = "dismissed";
pub const HIJACK_EXPIRED: &str = "expired";
pub const HIJACK_STOP_FAILED: &str = "stop_failed";
pub const HIJACK_FINISHED_FIRST: &str = "finished_first";

pub const HIJACK_EVENT_BEGIN_STOP: &str = "begin_stop";
pub const HIJACK_EVENT_CONFIRMED_STOP: &str = "confirmed_stop";
pub const HIJACK_EVENT_STOP_FAILED: &str = "stop_failed";
pub const HIJACK_EVENT_FINISHED_FIRST: &str = "finished_first";
pub const HIJACK_EVENT_BEGIN_RESOLUTION: &str = "begin_resolution";
pub const HIJACK_EVENT_COMPLETE_RESOLUTION: &str = "complete_resolution";
pub const HIJACK_EVENT_RESOLUTION_FAILED: &str = "resolution_failed";
pub const HIJACK_EVENT_EXPIRE: &str = "expire";

pub const HIJACK_ACTION_ESCALATE: &str = "escalate";
pub const HIJACK_ACTION_CONTINUE: &str = "continue";
pub const HIJACK_ACTION_INSTRUCT: &str = "instruct";
pub const HIJACK_ACTION_RESTART: &str = "restart";
pub const HIJACK_ACTION_EDIT: &str = "edit";
pub const HIJACK_ACTION_VIEW: &str = "view";
pub const HIJACK_ACTION_DISMISS: &str = "dismiss";

/// Per-repository change summary captured when a run is hijacked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackRepoDiffWire {
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub files_changed: u32,
    #[serde(default)]
    pub insertions: i64,
    #[serde(default)]
    pub deletions: i64,
    #[serde(default)]
    pub binary_files: u32,
}

/// Snapshot evidence retained before a held workspace can be released.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackSnapshotWire {
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub reference: String,
    #[serde(default)]
    pub fingerprint: String,
}

/// Durable hijack record. File locking and persistence stay in Python; this
/// wire and its state machine define the shared contract.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentHijackRecordWire {
    pub schema_version: u32,
    #[serde(default)]
    pub hijack_id: String,
    #[serde(default)]
    pub agent_name: String,
    #[serde(default)]
    pub artifacts_dir: String,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub process_identity_token: Option<String>,
    #[serde(default)]
    pub project_file: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub expected_claim_owner_pid: Option<i64>,
    #[serde(default)]
    pub prior_pin_state: Option<bool>,
    #[serde(default)]
    pub model_alias: Option<String>,
    #[serde(default)]
    pub model_alias_trail: Vec<String>,
    #[serde(default)]
    pub model_origin: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub run_duration_seconds: Option<f64>,
    #[serde(default)]
    pub last_tool: Option<String>,
    #[serde(default)]
    pub last_tool_age_seconds: Option<f64>,
    #[serde(default)]
    pub repo_diffs: Vec<AgentHijackRepoDiffWire>,
    #[serde(default)]
    pub snapshots: Vec<AgentHijackSnapshotWire>,
    #[serde(default)]
    pub stale_lock_files: Vec<String>,
    #[serde(default)]
    pub survivor_pids: Vec<i64>,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub hijacked_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub resolution_action: Option<String>,
    #[serde(default)]
    pub resolution_origin_state: Option<String>,
    #[serde(default)]
    pub reserved_successor_name: Option<String>,
    #[serde(default)]
    pub concrete_target_model: Option<String>,
    #[serde(default)]
    pub model_changed: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub acknowledged: bool,
}

/// Event payload consumed by [`crate::agent_hijack::advance_agent_hijack_record`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackEventWire {
    pub event: String,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub reserved_successor_name: Option<String>,
    #[serde(default)]
    pub concrete_target_model: Option<String>,
    #[serde(default)]
    pub model_changed: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub at: Option<String>,
}

/// Facts needed to decide whether a running row supports v1 hijack.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackEligibilityFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub is_local: bool,
    #[serde(default)]
    pub is_llm_agent: bool,
    #[serde(default)]
    pub is_monitor_turn: bool,
    #[serde(default)]
    pub is_gate_turn: bool,
    #[serde(default)]
    pub is_named_proc: bool,
    #[serde(default)]
    pub is_home_mode: bool,
    #[serde(default)]
    pub is_remote: bool,
    #[serde(default)]
    pub is_clan_container: bool,
    #[serde(default)]
    pub is_bulk: bool,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub lifecycle_phase: Option<String>,
    #[serde(default)]
    pub has_hijack_capability: bool,
    #[serde(default)]
    pub pid: Option<i64>,
}

/// Eligibility is rectangular at the Python boundary: a refusal has a code
/// and message; an eligible row has neither.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackEligibilityDecisionWire {
    pub schema_version: u32,
    pub eligible: bool,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}
