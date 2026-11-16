//! Versioned records for Agent Hijack policy and recovery.

use serde::{Deserialize, Serialize};

/// Ledger record schema version. Records already on disk keep advancing:
/// every field added after v1 carries `#[serde(default)]`.
pub const AGENT_HIJACK_WIRE_SCHEMA_VERSION: u32 = 1;

/// Facts/decision wire schema version. Split from the ledger schema so the
/// decision table can evolve without invalidating durable records.
pub const AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION: u32 = 2;

pub const HIJACK_REQUESTED: &str = "requested";
pub const HIJACK_STOPPING: &str = "stopping";
pub const HIJACK_STOP_BLOCKED: &str = "stop_blocked";
pub const HIJACK_HELD: &str = "held";
pub const HIJACK_RESOLVING: &str = "resolving";
pub const HIJACK_RESOLVED: &str = "resolved";
pub const HIJACK_DISMISSED: &str = "dismissed";
pub const HIJACK_EXPIRED: &str = "expired";
pub const HIJACK_STOP_FAILED: &str = "stop_failed";
pub const HIJACK_FINISHED_FIRST: &str = "finished_first";

/// Ledger scope for a hijack record: which stop mechanism owns the target.
pub const HIJACK_SCOPE_LLM: &str = "llm";
pub const HIJACK_SCOPE_PROCESSLESS: &str = "processless";
pub const HIJACK_SCOPE_MONITOR: &str = "monitor";
pub const HIJACK_SCOPE_GATE: &str = "gate";

pub const HIJACK_EVENT_BEGIN_STOP: &str = "begin_stop";
pub const HIJACK_EVENT_CONFIRMED_STOP: &str = "confirmed_stop";
pub const HIJACK_EVENT_STOP_FAILED: &str = "stop_failed";
pub const HIJACK_EVENT_STOP_BLOCKED: &str = "stop_blocked";
pub const HIJACK_EVENT_RETRY_STOP: &str = "retry_stop";
pub const HIJACK_EVENT_ABANDON_STOP: &str = "abandon_stop";
pub const HIJACK_EVENT_FINISHED_FIRST: &str = "finished_first";
pub const HIJACK_EVENT_BEGIN_RESOLUTION: &str = "begin_resolution";
pub const HIJACK_EVENT_RECORD_LAUNCH: &str = "record_launch";
pub const HIJACK_EVENT_COMPLETE_RESOLUTION: &str = "complete_resolution";
pub const HIJACK_EVENT_RESOLUTION_FAILED: &str = "resolution_failed";
pub const HIJACK_EVENT_EXPIRE: &str = "expire";

/// Decision modes returned by [`crate::agent_hijack::agent_hijack_decide`].
pub const HIJACK_DECISION_IMMEDIATE: &str = "immediate";
pub const HIJACK_DECISION_LATCHED: &str = "latched";
pub const HIJACK_DECISION_REDIRECT: &str = "redirect";
pub const HIJACK_DECISION_REFUSED: &str = "refused";

/// Stop adapter carried by immediate decisions.
pub const HIJACK_ADAPTER_LLM: &str = "llm";
pub const HIJACK_ADAPTER_PROCESSLESS: &str = "processless";
pub const HIJACK_ADAPTER_MONITOR: &str = "monitor";

/// Occupying-turn kinds accepted in v2 facts.
pub const HIJACK_OCCUPANT_LLM: &str = "llm";
pub const HIJACK_OCCUPANT_WAIT: &str = "wait";
pub const HIJACK_OCCUPANT_QUEUE: &str = "queue";
pub const HIJACK_OCCUPANT_MONITOR: &str = "monitor";
pub const HIJACK_OCCUPANT_GATE: &str = "gate";
pub const HIJACK_OCCUPANT_WORKFLOW_STEP: &str = "workflow_step";
pub const HIJACK_OCCUPANT_NONE: &str = "none";

/// Non-destructive navigation operations returned with redirect decisions.
pub const HIJACK_REDIRECT_OPEN_RECOVERY: &str = "open_recovery";
pub const HIJACK_REDIRECT_SELECT_MEMBER: &str = "select_member";
pub const HIJACK_REDIRECT_GATE_REVIEW: &str = "gate_review";
pub const HIJACK_REDIRECT_COPY_REMOTE_COMMAND: &str = "copy_remote_command";
pub const HIJACK_REDIRECT_SHOW_ACTIVE_HIJACK: &str = "show_active_hijack";
pub const HIJACK_REDIRECT_KILL_AND_EDIT: &str = "kill_and_edit";

pub const HIJACK_ACTION_ESCALATE: &str = "escalate";
pub const HIJACK_ACTION_CONTINUE: &str = "continue";
pub const HIJACK_ACTION_INSTRUCT: &str = "instruct";
pub const HIJACK_ACTION_RESTART: &str = "restart";
pub const HIJACK_ACTION_EDIT: &str = "edit";
pub const HIJACK_ACTION_VIEW: &str = "view";
pub const HIJACK_ACTION_DISMISS: &str = "dismiss";

/// Durable successor launch stage persisted on a resolving record.
pub const HIJACK_LAUNCH_QUEUED: &str = "queued";
pub const HIJACK_LAUNCH_PREPARED: &str = "prepared";
pub const HIJACK_LAUNCH_CONFIRMED: &str = "confirmed";

/// Observed successor states supplied to recovery reconciliation.
pub const HIJACK_SUCCESSOR_ABSENT: &str = "absent";
pub const HIJACK_SUCCESSOR_QUEUED: &str = "queued";
pub const HIJACK_SUCCESSOR_PREPARED: &str = "prepared";
pub const HIJACK_SUCCESSOR_LIVE: &str = "live";
pub const HIJACK_SUCCESSOR_COMPLETED: &str = "completed";
pub const HIJACK_SUCCESSOR_MISMATCHED: &str = "mismatched";
pub const HIJACK_SUCCESSOR_UNCERTAIN: &str = "uncertain";
/// The admission journal proves the reserved request was never dispatched
/// (missing, cancelled, or terminally failed/rejected) and no ack, child
/// metadata, or successor artifacts exist.
pub const HIJACK_SUCCESSOR_NOT_DISPATCHED: &str = "not_dispatched";

/// Reconciliation outcomes. `retain_with_error` keeps custody.
pub const HIJACK_RECONCILE_KEEP_RESOLVING: &str = "keep_resolving";
pub const HIJACK_RECONCILE_ADOPT: &str = "adopt";
pub const HIJACK_RECONCILE_RETURN_TO_HELD: &str = "return_to_held";
pub const HIJACK_RECONCILE_RETAIN_WITH_ERROR: &str = "retain_with_error";

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
    /// Pid of the process that won the held→resolving CAS.
    #[serde(default)]
    pub resolver_pid: Option<i64>,
    /// Process-identity token for [`Self::resolver_pid`] (`boot_id:start_ticks`).
    #[serde(default)]
    pub resolver_process_identity_token: Option<String>,
    /// Durable successor launch identity. Names alone are not proof.
    ///
    /// Keep-work recovery binds this to one admission request by storing
    /// [`crate::agent_launch::LaunchAdmissionJournalEntryWire::fingerprint`]
    /// in [`Self::admission_request_id`] and that entry's `identity` in
    /// [`Self::admission_identity`]. Retries must address that request
    /// rather than submit another. Wait following uses
    /// `recovery.successor` after settlement; wait wires are unchanged.
    #[serde(default)]
    pub launch_identity: Option<String>,
    #[serde(default)]
    pub admission_request_id: Option<String>,
    #[serde(default)]
    pub admission_identity: Option<String>,
    /// `queued`, `prepared`, or `confirmed`.
    #[serde(default)]
    pub launch_stage: Option<String>,
    #[serde(default)]
    pub successor_pid: Option<i64>,
    #[serde(default)]
    pub successor_process_identity_token: Option<String>,
    #[serde(default)]
    pub successor_artifacts_dir: Option<String>,
    #[serde(default)]
    pub inheritance_acknowledged: Option<bool>,
    /// Dedup target key: `sha256(project_file + "::" + session-or-agent name)`.
    /// One target index entry maps each key to its non-terminal hijack id.
    #[serde(default)]
    pub target_key: String,
    /// Which stop mechanism owns the target: `llm`, `processless`,
    /// `monitor`, or `gate`.
    #[serde(default)]
    pub scope: Option<String>,
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
    pub resolver_pid: Option<i64>,
    #[serde(default)]
    pub resolver_process_identity_token: Option<String>,
    #[serde(default)]
    pub launch_identity: Option<String>,
    #[serde(default)]
    pub admission_request_id: Option<String>,
    #[serde(default)]
    pub admission_identity: Option<String>,
    #[serde(default)]
    pub launch_stage: Option<String>,
    #[serde(default)]
    pub successor_pid: Option<i64>,
    #[serde(default)]
    pub successor_process_identity_token: Option<String>,
    #[serde(default)]
    pub successor_artifacts_dir: Option<String>,
    #[serde(default)]
    pub inheritance_acknowledged: Option<bool>,
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

/// Facts needed for the v2 hijack decision.
///
/// Row-kind fields describe the selected row; occupant fields describe the
/// session's occupying turn (resolved by Python through occupant selection);
/// workspace/process fields describe the numbered claim; history fields carry
/// capability, successor, and in-flight hijack state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackEligibilityFactsV2Wire {
    pub schema_version: u32,
    #[serde(default)]
    pub is_local: bool,
    #[serde(default)]
    pub is_remote: bool,
    #[serde(default)]
    pub is_bulk: bool,
    #[serde(default)]
    pub is_named_proc: bool,
    #[serde(default)]
    pub is_home_mode: bool,
    #[serde(default)]
    pub is_clan_container: bool,
    #[serde(default)]
    pub is_session_container: bool,
    #[serde(default)]
    pub live_member_count: i64,
    /// `llm`, `wait`, `queue`, `monitor`, `gate`, `workflow_step`, or `none`.
    #[serde(default)]
    pub occupying_kind: String,
    #[serde(default)]
    pub status: Option<String>,
    /// Runner lifecycle phase of the occupying turn.
    #[serde(default)]
    pub turn_phase: Option<String>,
    /// Finalizer phase (`declaring`/`executing`) when finalizing.
    #[serde(default)]
    pub finalizer_phase: Option<String>,
    /// Gate state (`pending` awaits a human; anything else is executing).
    #[serde(default)]
    pub gate_state: Option<String>,
    /// Monitor state (`running` while the monitor turn is live).
    #[serde(default)]
    pub monitor_state: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub has_numbered_claim: bool,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub process_alive: bool,
    #[serde(default)]
    pub has_hijack_capability: bool,
    #[serde(default)]
    pub successor_attachable: bool,
    #[serde(default)]
    pub done_outcome: Option<String>,
    /// State of any non-terminal ledger record for this target.
    #[serde(default)]
    pub active_hijack_state: Option<String>,
}

/// V2 decision: `mode` is `immediate`, `latched`, `redirect`, or `refused`.
/// `eligible` stays true for `immediate` and `latched` for older callers.
/// Redirects carry `redirect_op`; every non-immediate decision carries a
/// `code`, a one-line `message`, and a `next_action`. Immediate decisions
/// carry `adapter` (`llm`, `processless`, or `monitor`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackEligibilityDecisionV2Wire {
    pub schema_version: u32,
    pub mode: String,
    pub eligible: bool,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub redirect_op: Option<String>,
    #[serde(default)]
    pub adapter: Option<String>,
}

/// One session member as seen by occupant selection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackOccupantFactsWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: Option<String>,
    /// The member's `artifacts_timestamp` matches the numbered claim.
    #[serde(default)]
    pub holds_claim: bool,
    /// STARTING/RUNNING/RETRYING, provider running, or finalizing.
    #[serde(default)]
    pub in_flight: bool,
    /// WAITING/QUEUED.
    #[serde(default)]
    pub waiting: bool,
    /// Terminal outcome recorded; never selected while a live member exists.
    #[serde(default)]
    pub settled: bool,
    /// Session order for newest/earliest tie-breaks.
    #[serde(default)]
    pub order: i64,
}

/// Request wire for occupant selection: the session's member roster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackOccupantSelectionRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub members: Vec<AgentHijackOccupantFactsWire>,
}

/// Occupant selection result: the chosen roster index plus why it won.
/// `selected_index` is `None` when no member occupies the session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHijackOccupantSelectionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub selected_index: Option<usize>,
    #[serde(default)]
    pub reason: String,
}
