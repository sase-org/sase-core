//! Wire records mirroring `sase_100/src/sase/core/agent_scan_wire.py`.
//!
//! These types are the stable boundary between the Rust agent-artifact
//! scanner and Python's `sase.core.agent_scan_facade`. Field declaration
//! order matches the Python dataclasses so JSON output is identical when
//! serialized with order-preserving serializers (Python's
//! `dataclasses.asdict` is order-preserving).
//!
//! JSON shape rules match `wire.rs`:
//!
//! - `Option<T>::None` serializes as JSON `null` (not omitted).
//! - Empty list fields serialize as `[]` (never `null`).
//! - All field names are lowercase `snake_case` (serde default).
//! - `schema_version` lives on `AgentArtifactScanWire` so a Rust scanner
//!   can refuse to deserialize newer records.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Schema version mirrored from
/// `agent_scan_wire.py::AGENT_SCAN_WIRE_SCHEMA_VERSION`.
pub const AGENT_SCAN_WIRE_SCHEMA_VERSION: u32 = 7;

/// Workflow directory categories the scanner walks.
///
/// Matches `agent_scan_wire.py::DONE_WORKFLOW_DIR_NAMES`.
pub const DONE_WORKFLOW_DIR_NAMES: &[&str] =
    &["ace-run", "run", "fix-hook", "crs", "summarize-hook"];

/// Matches `agent_scan_wire.py::DONE_WORKFLOW_DIR_PREFIXES`.
pub const DONE_WORKFLOW_DIR_PREFIXES: &[&str] = &["mentor-"];

/// Matches `agent_scan_wire.py::WORKFLOW_STATE_DIR_NAMES`.
pub const WORKFLOW_STATE_DIR_NAMES: &[&str] = &["ace-run", "run"];

/// Matches `agent_scan_wire.py::WORKFLOW_STATE_DIR_PREFIXES`.
pub const WORKFLOW_STATE_DIR_PREFIXES: &[&str] = &["workflow-"];

/// Caller-supplied knobs for one snapshot scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactScanOptionsWire {
    #[serde(default = "default_true")]
    pub include_prompt_step_markers: bool,
    #[serde(default = "default_true")]
    pub include_raw_prompt_snippets: bool,
    #[serde(default = "default_prompt_snippet_bytes")]
    pub max_prompt_snippet_bytes: u32,
    #[serde(default)]
    pub only_workflow_dirs: Vec<String>,
    #[serde(default)]
    pub max_records: Option<u32>,
    #[serde(default)]
    pub newest_first: bool,
    #[serde(default)]
    pub not_before_timestamp: Option<String>,
    #[serde(default = "default_true")]
    pub include_done_markers: bool,
    #[serde(default = "default_true")]
    pub include_workflow_state: bool,
    #[serde(default = "default_true")]
    pub include_waiting: bool,
    #[serde(default)]
    pub only_projects: Vec<String>,
    #[serde(default)]
    pub include_project_states: Vec<String>,
}

impl Default for AgentArtifactScanOptionsWire {
    fn default() -> Self {
        Self {
            include_prompt_step_markers: true,
            include_raw_prompt_snippets: true,
            max_prompt_snippet_bytes: 200,
            only_workflow_dirs: Vec::new(),
            max_records: None,
            newest_first: false,
            not_before_timestamp: None,
            include_done_markers: true,
            include_workflow_state: true,
            include_waiting: true,
            only_projects: Vec::new(),
            include_project_states: Vec::new(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_prompt_snippet_bytes() -> u32 {
    200
}

/// Diagnostic counters for one snapshot scan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactScanStatsWire {
    pub projects_visited: u64,
    pub artifact_dirs_visited: u64,
    pub marker_files_parsed: u64,
    pub json_decode_errors: u64,
    pub os_errors: u64,
    pub prompt_step_markers_parsed: u64,
}

/// Metadata for an intentionally bounded artifact-index window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexWindowWire {
    #[serde(default)]
    pub requested_limit: Option<u32>,
    #[serde(default)]
    pub selected_candidate_count: u64,
    #[serde(default)]
    pub returned_record_count: u64,
    #[serde(default)]
    pub active_candidate_count: u64,
    #[serde(default)]
    pub completed_candidate_count: u64,
    #[serde(default)]
    pub has_more: bool,
    #[serde(default)]
    pub truncated: bool,
}

/// Compact projection of `done.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DoneMarkerWire {
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub finished_at: Option<f64>,
    /// Whether `finished_at` was chosen by stale-artifact terminalization
    /// rather than recorded by the agent process itself.
    #[serde(default)]
    pub finished_at_estimated: bool,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub project_file: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub vcs_provider: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub plan_path: Option<String>,
    #[serde(default)]
    pub diff_path: Option<String>,
    #[serde(default)]
    pub markdown_pdf_paths: Vec<String>,
    #[serde(default)]
    pub image_paths: Vec<String>,
    #[serde(default)]
    pub video_paths: Vec<String>,
    #[serde(default)]
    pub response_path: Option<String>,
    #[serde(default)]
    pub output_path: Option<String>,
    #[serde(default)]
    pub step_output: Option<Map<String, Value>>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
    #[serde(default)]
    pub retried_as_timestamp: Option<String>,
    #[serde(default)]
    pub retry_chain_root_timestamp: Option<String>,
    #[serde(default)]
    pub retry_error_category: Option<String>,
    #[serde(default)]
    pub approve: bool,
    #[serde(default)]
    pub hidden: bool,
    /// `true` for a repeat-chain slot a predecessor's `STOP` output variable
    /// skipped. The marker keeps `outcome: "completed"` so `%wait` cascades,
    /// while the TUI renders a distinct `STOPPED` status.
    #[serde(default)]
    pub repeat_stopped: bool,
    /// Name of the chain predecessor that set `STOP`, when recorded.
    #[serde(default)]
    pub stopped_by: Option<String>,
    /// Project-scoped import journal key for transaction-gated history.
    #[serde(default)]
    pub imported_transaction_key: Option<String>,
    #[serde(default)]
    pub imported_source_owner: Option<ImportedSourceOwnerWire>,
    #[serde(default)]
    pub status_label: Option<String>,
    /// Terminal monitor or gate-shell projection, folding the flat
    /// `monitor_*` / `gate_*` fields. `None` when the record is neither.
    #[serde(default)]
    pub family_shell: Option<FamilyShellWire>,
}

/// Bounded JSON value stored under `agent_meta.json::output_variables`.
///
/// The artifact scanner applies the reliability caps before a value reaches
/// this wire type. Keeping the public alias preserves the established export
/// name while allowing every JSON scalar and container shape.
pub type OutputVariableValue = Value;

/// Schema version for indexed output-variable history queries.
pub const AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_output_variable_key_limit() -> u32 {
    20
}

fn default_output_variable_value_limit() -> u32 {
    5
}

/// Query knobs for grouped output-variable history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableHistoryQueryWire {
    /// Exact project display names to include. Empty means every project.
    #[serde(default)]
    pub projects: Vec<String>,
    /// Agent-name globs to include. `hood.*` also matches the hood root.
    #[serde(default)]
    pub agents: Vec<String>,
    /// Variable-key globs to include.
    #[serde(default)]
    pub keys: Vec<String>,
    /// Case-insensitive substring matches over scalar text and canonical JSON.
    #[serde(default)]
    pub values: Vec<String>,
    /// Exact typed JSON value matches after canonical serialization.
    #[serde(default)]
    pub value_json: Vec<OutputVariableValue>,
    /// Inclusive lower artifact timestamp bound (`YYYYmmddHHMMSS`).
    #[serde(default)]
    pub since_timestamp: Option<String>,
    /// Inclusive upper artifact timestamp bound (`YYYYmmddHHMMSS`).
    #[serde(default)]
    pub until_timestamp: Option<String>,
    #[serde(default)]
    pub include_hidden: bool,
    /// Maximum keys returned. Zero means unlimited.
    #[serde(default = "default_output_variable_key_limit")]
    pub key_limit: u32,
    /// Maximum distinct values returned per key. Zero means unlimited.
    #[serde(default = "default_output_variable_value_limit")]
    pub value_limit: u32,
    /// Invert the normal recent-first key and value ordering.
    #[serde(default)]
    pub reverse: bool,
}

impl Default for AgentOutputVariableHistoryQueryWire {
    fn default() -> Self {
        Self {
            projects: Vec::new(),
            agents: Vec::new(),
            keys: Vec::new(),
            values: Vec::new(),
            value_json: Vec::new(),
            since_timestamp: None,
            until_timestamp: None,
            include_hidden: false,
            key_limit: default_output_variable_key_limit(),
            value_limit: default_output_variable_value_limit(),
            reverse: false,
        }
    }
}

/// Effective limit and truncation metadata for one history dimension.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOutputVariableLimitWire {
    pub limit: u32,
    pub total_count: u64,
    pub returned_count: u64,
    pub truncated: bool,
}

/// One indexed output-variable occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableOccurrenceWire {
    pub artifact_dir: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub key: String,
    pub value: OutputVariableValue,
    pub value_json: String,
    pub hidden: bool,
}

/// One distinct typed value for a variable key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableValueGroupWire {
    pub value: OutputVariableValue,
    pub value_json: String,
    pub occurrence_count: u64,
    pub agent_count: u64,
    pub agents: Vec<String>,
    pub projects: Vec<String>,
    pub first_seen_timestamp: String,
    pub last_seen_timestamp: String,
    pub newest: AgentOutputVariableOccurrenceWire,
}

/// Grouped history for one output-variable key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableKeyGroupWire {
    pub key: String,
    pub occurrence_count: u64,
    pub distinct_value_count: u64,
    pub values_limit: AgentOutputVariableLimitWire,
    pub values: Vec<AgentOutputVariableValueGroupWire>,
}

/// Grouped output-variable history returned by the artifact index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableHistoryWire {
    pub schema_version: u32,
    pub index_path: String,
    pub query: AgentOutputVariableHistoryQueryWire,
    pub keys_limit: AgentOutputVariableLimitWire,
    pub groups: Vec<AgentOutputVariableKeyGroupWire>,
}

/// Schema version for output-variable selector parse and get queries.
pub const AGENT_OUTPUT_VARIABLE_SELECTOR_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_output_variable_selector_limit() -> u32 {
    20
}

/// Scope of one output-variable selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputVariableSelectorScopeWire {
    Unscoped,
    Global,
    Exact { name: String },
    Hood { name: String },
}

/// One JSON-path step applied after an occurrence is selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputVariableSelectorPathWire {
    Index { index: u64 },
    Key { key: String },
}

/// Parsed `sase var get` selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputVariableSelectorWire {
    pub raw: String,
    pub scope: OutputVariableSelectorScopeWire,
    /// `None` means the key wildcard `*`.
    pub key: Option<String>,
    #[serde(default)]
    pub path: Vec<OutputVariableSelectorPathWire>,
}

/// Query knobs for selector-based output-variable retrieval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableSelectorQueryWire {
    pub selectors: Vec<OutputVariableSelectorWire>,
    #[serde(default)]
    pub projects: Vec<String>,
    #[serde(default)]
    pub include_hidden: bool,
    /// Maximum matches returned from wildcard expansion. Zero is unlimited.
    #[serde(default = "default_output_variable_selector_limit")]
    pub limit: u32,
}

impl Default for AgentOutputVariableSelectorQueryWire {
    fn default() -> Self {
        Self {
            selectors: Vec::new(),
            projects: Vec::new(),
            include_hidden: false,
            limit: default_output_variable_selector_limit(),
        }
    }
}

/// One attributed selector match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableSelectorMatchWire {
    pub selector: String,
    pub key: String,
    pub path: Vec<OutputVariableSelectorPathWire>,
    pub value: OutputVariableValue,
    pub value_json: String,
    pub artifact_dir: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub hidden: bool,
}

/// Selector matches returned by the artifact index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputVariableSelectorResultWire {
    pub schema_version: u32,
    pub index_path: String,
    pub query: AgentOutputVariableSelectorQueryWire,
    pub matches_limit: AgentOutputVariableLimitWire,
    pub matches: Vec<AgentOutputVariableSelectorMatchWire>,
}

/// Provenance of an imported run, copied from `imported_source_owner`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ImportedSourceOwnerWire {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub machine_name: String,
}

/// Compact projection of `agent_meta.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentMetaWire {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub artifact_agent_id: Option<String>,
    #[serde(default)]
    pub artifact_source_dir: Option<String>,
    #[serde(default, alias = "patch_name")]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub bead_id: Option<String>,
    #[serde(default)]
    pub plan_path: Option<String>,
    #[serde(default)]
    pub sdd_prompt_path: Option<String>,
    #[serde(default)]
    pub sdd_plan_path: Option<String>,
    #[serde(default)]
    pub epic_plan_ref: Option<String>,
    #[serde(default)]
    pub question_request_path: Option<String>,
    #[serde(default)]
    pub question_response_path: Option<String>,
    #[serde(default)]
    pub question_session_id: Option<String>,
    #[serde(default)]
    pub epic_bead_id: Option<String>,
    #[serde(default)]
    pub phase_bead_id: Option<String>,
    #[serde(default, alias = "commit_patch_name")]
    pub commit_changespec_name: Option<String>,
    #[serde(default)]
    pub commit_entry_id: Option<String>,
    #[serde(default)]
    pub commit_result: Option<String>,
    #[serde(default)]
    pub commit_diff_path: Option<String>,
    #[serde(default)]
    pub parent_agent_timestamp: Option<String>,
    #[serde(default)]
    pub parent_agent_name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub agent_clan: Option<String>,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
    #[serde(default)]
    pub clan_summary: Option<String>,
    #[serde(default)]
    pub agent_family: Option<String>,
    #[serde(default)]
    pub agent_family_role: Option<String>,
    #[serde(default)]
    pub agent_family_parallel: bool,
    #[serde(default)]
    pub imported_source_owner: Option<ImportedSourceOwnerWire>,
    #[serde(default)]
    pub plan_chain_root: bool,
    #[serde(default, alias = "tag")]
    pub tribe: Option<String>,
    #[serde(default)]
    pub output_variables: BTreeMap<String, OutputVariableValue>,
    #[serde(default)]
    pub output_path: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub model_alias: Option<String>,
    #[serde(default)]
    pub model_alias_trail: Vec<String>,
    #[serde(default)]
    pub model_alias_origin: Option<String>,
    #[serde(default)]
    pub vcs_provider: Option<String>,
    #[serde(default)]
    pub role_suffix: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub linked_repos: Vec<Map<String, Value>>,
    #[serde(default)]
    pub approve: bool,
    #[serde(default)]
    pub auto_approve_plan_action: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    pub plan_approved: bool,
    #[serde(default)]
    pub plan_action: Option<String>,
    #[serde(default)]
    pub plan_committed: Option<bool>,
    #[serde(default)]
    pub wait_for: Vec<String>,
    #[serde(default)]
    pub wait_for_beads: Vec<String>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
    #[serde(default)]
    pub wait_priority: Option<i64>,
    #[serde(default)]
    pub wait_completed_at: Option<String>,
    #[serde(default)]
    pub plan_submitted_at: Vec<String>,
    #[serde(default)]
    pub epic_started_at: Option<String>,
    #[serde(default)]
    pub feedback_submitted_at: Vec<String>,
    #[serde(default)]
    pub questions_submitted_at: Vec<String>,
    #[serde(default)]
    pub retry_started_at: Vec<String>,
    #[serde(default)]
    pub run_started_at: Option<String>,
    #[serde(default)]
    pub stopped_at: Option<String>,
    #[serde(default)]
    pub retry_of_timestamp: Option<String>,
    #[serde(default)]
    pub retry_attempt: Option<i64>,
    #[serde(default)]
    pub retry_chain_root_timestamp: Option<String>,
    #[serde(default)]
    pub retried_as_timestamp: Option<String>,
    #[serde(default)]
    pub retry_terminal: bool,
    #[serde(default)]
    pub retry_error_category: Option<String>,
    /// Terminal monitor or gate-shell projection, folding the flat
    /// `monitor_*` / `gate_*` fields. `None` when the record is neither.
    #[serde(default)]
    pub family_shell: Option<FamilyShellWire>,
    #[serde(default)]
    pub shell_kind: Option<String>,
    #[serde(default)]
    pub proc_id: Option<String>,
}

/// Monitor-only fields of a `family_shell` record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FamilyShellMonitorWire {
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub starter_agent: Option<String>,
    #[serde(default)]
    pub tail_lines: Option<i64>,
    #[serde(default)]
    pub pgid: Option<i64>,
    #[serde(default)]
    pub supervisor_identity: Option<String>,
    #[serde(default)]
    pub settled: bool,
    #[serde(default)]
    pub idle_timeout_seconds: Option<f64>,
}

/// Gate-only fields of a `family_shell` record.
///
/// `kind` here is the gate's own flavor (e.g. `"approval"`), not the
/// `FamilyShellWire::kind` discriminator.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FamilyShellGateWire {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub accent: Option<String>,
    #[serde(default)]
    pub creator_agent: Option<String>,
    #[serde(default)]
    pub next_fork: Option<String>,
    #[serde(default)]
    pub workspace_policy: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub notification_id: Option<String>,
    #[serde(default)]
    pub decision_path: Option<String>,
}

/// One durable family-shell member: a monitor or a gate, never both.
///
/// `kind` discriminates `"monitor"` / `"gate"`. The fields below `kind` are
/// the ones both shells carry (mirroring the two flat `monitor_*` /
/// `gate_*` prefixes they replace); `monitor` / `gate` hold whichever
/// kind's own fields, with the other left `None`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FamilyShellWire {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub start_status: Option<String>,
    #[serde(default)]
    pub stop_status: Option<String>,
    #[serde(default)]
    pub timeout_seconds: Option<f64>,
    #[serde(default)]
    pub elapsed_seconds: Option<f64>,
    #[serde(default)]
    pub output_path: Option<String>,
    #[serde(default)]
    pub output_truncated: bool,
    #[serde(default)]
    pub request_fingerprint: Option<String>,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub next_output: Option<String>,
    #[serde(default)]
    pub next_model: Option<String>,
    #[serde(default)]
    pub followup_agent: Option<String>,
    #[serde(default)]
    pub followup_outcome: Option<String>,
    #[serde(default)]
    pub followup_error: Option<String>,
    #[serde(default)]
    pub followup_degraded_reason: Option<String>,
    #[serde(default)]
    pub followup_prompt_path: Option<String>,
    #[serde(default)]
    pub monitor: Option<FamilyShellMonitorWire>,
    #[serde(default)]
    pub gate: Option<FamilyShellGateWire>,
}

/// Compact projection of `running.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningMarkerWire {
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub vcs_provider: Option<String>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
}

/// Compact projection of `waiting.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WaitingMarkerWire {
    #[serde(default)]
    pub waiting_for: Vec<String>,
    #[serde(default)]
    pub wait_for_beads: Vec<String>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
    #[serde(default)]
    pub wait_runners: Option<i64>,
    #[serde(default)]
    pub wait_priority: Option<i64>,
    #[serde(default)]
    pub wait_priority_explicit: bool,
    #[serde(default)]
    pub wait_runners_explicit: bool,
    #[serde(default)]
    pub slot_requested_at: Option<String>,
}

/// Compact projection of `pending_question.json`.
///
/// The marker is written by `handle_questions_flow()` immediately before the
/// response-wait poll loop and removed on every loop exit path. Its presence
/// is the authoritative signal that the agent is currently blocked on user
/// input, independent of the corresponding `UserQuestion` notification's
/// dismissed/read state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingQuestionMarkerWire {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub request_path: Option<String>,
    #[serde(default)]
    pub submitted_at: Option<String>,
}

/// One step entry from `workflow_state.json`'s `steps` array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStepStateWire {
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub output: Option<Map<String, Value>>,
    #[serde(default)]
    pub output_types: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
}

impl Default for WorkflowStepStateWire {
    fn default() -> Self {
        Self {
            name: String::new(),
            status: "pending".to_string(),
            output: None,
            output_types: None,
            error: None,
            traceback: None,
        }
    }
}

/// Compact projection of `workflow_state.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStateWire {
    pub workflow_name: String,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub status: String,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub appears_as_agent: bool,
    #[serde(default)]
    pub is_anonymous: bool,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub current_step_index: i64,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
    #[serde(default)]
    pub activity: Option<String>,
    #[serde(default)]
    pub pdf_status: Option<Map<String, Value>>,
    #[serde(default)]
    pub steps: Vec<WorkflowStepStateWire>,
}

impl Default for WorkflowStateWire {
    fn default() -> Self {
        Self {
            workflow_name: "unknown".to_string(),
            cl_name: None,
            status: "running".to_string(),
            pid: None,
            appears_as_agent: false,
            is_anonymous: false,
            hidden: false,
            current_step_index: 0,
            start_time: None,
            error: None,
            traceback: None,
            activity: None,
            pdf_status: None,
            steps: Vec::new(),
        }
    }
}

/// Compact projection of one `prompt_step_*.json` marker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptStepMarkerWire {
    pub file_name: String,
    pub workflow_name: String,
    pub step_name: String,
    pub step_type: String,
    #[serde(default)]
    pub step_source: Option<String>,
    #[serde(default)]
    pub step_index: Option<i64>,
    #[serde(default)]
    pub total_steps: Option<i64>,
    #[serde(default)]
    pub parent_step_index: Option<i64>,
    #[serde(default)]
    pub parent_total_steps: Option<i64>,
    pub status: String,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub is_pre_prompt_step: bool,
    #[serde(default)]
    pub embedded_workflow_name: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub diff_path: Option<String>,
    #[serde(default)]
    pub response_path: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub model_alias: Option<String>,
    #[serde(default)]
    pub model_alias_trail: Vec<String>,
    #[serde(default)]
    pub model_alias_origin: Option<String>,
    #[serde(default)]
    pub output: Option<Map<String, Value>>,
    #[serde(default)]
    pub output_types: Option<BTreeMap<String, String>>,
}

/// Compact projection of `plan_path.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPathMarkerWire {
    #[serde(default)]
    pub plan_path: Option<String>,
}

/// Compact projection of one launch-boundary `xprompts.json` entry,
/// deduplicated by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsedXPromptWire {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub references: u64,
}

/// Payload shape for an indexed artifact record.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactRecordShapeWire {
    #[default]
    Full,
    List,
}

impl AgentArtifactRecordShapeWire {
    pub fn is_full(&self) -> bool {
        *self == Self::Full
    }
}

/// One artifact directory's parsed markers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactRecordWire {
    pub project_name: String,
    pub project_dir: String,
    pub project_file: String,
    pub workflow_dir_name: String,
    pub artifact_dir: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_meta: Option<AgentMetaWire>,
    #[serde(default)]
    pub done: Option<DoneMarkerWire>,
    #[serde(default)]
    pub running: Option<RunningMarkerWire>,
    #[serde(default)]
    pub waiting: Option<WaitingMarkerWire>,
    #[serde(default)]
    pub pending_question: Option<PendingQuestionMarkerWire>,
    #[serde(default)]
    pub workflow_state: Option<WorkflowStateWire>,
    #[serde(default)]
    pub plan_path: Option<PlanPathMarkerWire>,
    #[serde(default)]
    pub prompt_steps: Vec<PromptStepMarkerWire>,
    #[serde(default)]
    pub raw_prompt_snippet: Option<String>,
    #[serde(default)]
    pub used_xprompts: Vec<UsedXPromptWire>,
    #[serde(default)]
    pub has_done_marker: bool,
    #[serde(
        default,
        skip_serializing_if = "AgentArtifactRecordShapeWire::is_full"
    )]
    pub record_shape: AgentArtifactRecordShapeWire,
}

/// Authoritative resolved attributes for one represented clan generation.
///
/// These values are semantic context for visible records, not additional
/// agent rows. Declaration sources may therefore be terminal, hidden, or
/// outside a bounded history window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentClanContextWire {
    pub agent_clan: String,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
    #[serde(default)]
    pub clan_summary: Option<String>,
    #[serde(default)]
    pub clan_tribe_source_launch_timestamp: Option<String>,
    #[serde(default)]
    pub clan_tribe_source_identity: Option<String>,
    #[serde(default)]
    pub clan_summary_source_launch_timestamp: Option<String>,
    #[serde(default)]
    pub clan_summary_source_identity: Option<String>,
}

/// Top-level snapshot returned by [`scan_agent_artifacts`].
///
/// [`scan_agent_artifacts`]: super::scan_agent_artifacts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactScanWire {
    pub schema_version: u32,
    pub projects_root: String,
    pub options: AgentArtifactScanOptionsWire,
    pub stats: AgentArtifactScanStatsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_window: Option<AgentArtifactIndexWindowWire>,
    #[serde(default)]
    pub records: Vec<AgentArtifactRecordWire>,
    #[serde(default)]
    pub clan_context: Vec<AgentClanContextWire>,
}

/// Return true iff `name` is one of the workflow folder names the
/// scanner walks. Matches `_supported_workflow_dir` in the Python facade.
pub fn is_supported_workflow_dir(name: &str) -> bool {
    if DONE_WORKFLOW_DIR_NAMES.contains(&name) {
        return true;
    }
    if WORKFLOW_STATE_DIR_NAMES.contains(&name) {
        return true;
    }
    for prefix in DONE_WORKFLOW_DIR_PREFIXES {
        if name.starts_with(prefix) {
            return true;
        }
    }
    for prefix in WORKFLOW_STATE_DIR_PREFIXES {
        if name.starts_with(prefix) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_meta_wire_round_trips_every_monitor_field() {
        let meta = AgentMetaWire {
            name: Some("acme--mon".to_string()),
            family_shell: Some(FamilyShellWire {
                kind: "monitor".to_string(),
                id: Some("m4kq".to_string()),
                label: Some("just check-full".to_string()),
                reason: Some("Verify the refactor".to_string()),
                start_status: Some("MONITORING".to_string()),
                stop_status: Some("MONITORED".to_string()),
                timeout_seconds: Some(2_700.0),
                elapsed_seconds: None,
                state: Some("running".to_string()),
                output_path: Some("live_reply.md".to_string()),
                output_truncated: true,
                next_action: Some("Reply to the user.".to_string()),
                next_output: Some("tail".to_string()),
                next_model: Some("@small".to_string()),
                followup_agent: Some("acme--1".to_string()),
                request_fingerprint: Some("sha256:deadbeef".to_string()),
                followup_outcome: Some("launched-degraded".to_string()),
                followup_error: Some(
                    "workspace claim transfer failed".to_string(),
                ),
                followup_degraded_reason: Some(
                    "original claim already released".to_string(),
                ),
                followup_prompt_path: Some(
                    "artifacts/followup_prompt.md".to_string(),
                ),
                monitor: Some(FamilyShellMonitorWire {
                    command: Some("just check-full".to_string()),
                    cwd: Some("/home/bryan/workspaces/acme".to_string()),
                    exit_code: Some(1),
                    starter_agent: Some("acme--0".to_string()),
                    tail_lines: Some(200),
                    pgid: Some(4242),
                    supervisor_identity: Some("boot-abc123:98765".to_string()),
                    settled: true,
                    idle_timeout_seconds: Some(600.0),
                }),
                gate: None,
            }),
            ..Default::default()
        };

        let encoded = serde_json::to_value(&meta).unwrap();
        let decoded: AgentMetaWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn agent_meta_wire_without_monitor_fields_still_parses() {
        let old_record = serde_json::json!({
            "name": "pre-monitor-agent",
            "cl_name": "cl_alpha",
        });

        let decoded: AgentMetaWire =
            serde_json::from_value(old_record).unwrap();

        assert_eq!(decoded.name.as_deref(), Some("pre-monitor-agent"));
        assert_eq!(decoded.family_shell, None);
        assert!(decoded.model_alias_trail.is_empty());
        assert_eq!(decoded.model_alias_origin, None);
    }

    #[test]
    fn agent_meta_wire_round_trips_every_gate_field() {
        let meta = AgentMetaWire {
            name: Some("acme--gate".to_string()),
            family_shell: Some(FamilyShellWire {
                kind: "gate".to_string(),
                id: Some("gate-1".to_string()),
                state: Some("pending".to_string()),
                start_status: Some("WAITING".to_string()),
                stop_status: Some("ANSWERED".to_string()),
                output_path: Some("gate.out".to_string()),
                output_truncated: true,
                next_action: Some("Resume after gate.".to_string()),
                next_output: Some("summary,details".to_string()),
                next_model: Some("@large".to_string()),
                followup_agent: Some("acme--1".to_string()),
                followup_outcome: Some("launched".to_string()),
                followup_error: Some("claim moved late".to_string()),
                followup_degraded_reason: Some(
                    "workspace claim unavailable".to_string(),
                ),
                followup_prompt_path: Some(
                    "artifacts/gate_followup.md".to_string(),
                ),
                elapsed_seconds: Some(12.5),
                label: Some("approval/gate-1".to_string()),
                reason: Some("Need owner approval".to_string()),
                timeout_seconds: Some(600.0),
                request_fingerprint: Some("sha256:cafe".to_string()),
                monitor: None,
                gate: Some(FamilyShellGateWire {
                    kind: Some("approval".to_string()),
                    accent: Some("#0BCDEC".to_string()),
                    creator_agent: Some("acme--0".to_string()),
                    next_fork: Some("family".to_string()),
                    workspace_policy: Some("inherit".to_string()),
                    bundle_path: Some("gate_bundle.json".to_string()),
                    notification_id: Some("notif-1".to_string()),
                    decision_path: Some("gate_decision.md".to_string()),
                }),
            }),
            shell_kind: Some("gate".to_string()),
            proc_id: Some("proc-gate".to_string()),
            ..Default::default()
        };

        let encoded = serde_json::to_value(&meta).unwrap();
        let decoded: AgentMetaWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn agent_meta_wire_without_gate_fields_still_parses() {
        let old_record = serde_json::json!({
            "name": "pre-gate-agent",
            "cl_name": "cl_alpha",
        });

        let decoded: AgentMetaWire =
            serde_json::from_value(old_record).unwrap();

        assert_eq!(decoded.name.as_deref(), Some("pre-gate-agent"));
        assert_eq!(decoded.family_shell, None);
        assert_eq!(decoded.shell_kind, None);
        assert_eq!(decoded.proc_id, None);
    }

    #[test]
    fn agent_meta_wire_round_trips_alias_trail_and_origin() {
        let meta = AgentMetaWire {
            name: Some("trail-agent".to_string()),
            model_alias: Some("coder".to_string()),
            model_alias_trail: vec!["coder".to_string(), "large".to_string()],
            model_alias_origin: Some("directive".to_string()),
            ..Default::default()
        };

        let encoded = serde_json::to_value(&meta).unwrap();
        let decoded: AgentMetaWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, meta);
        assert_eq!(
            decoded.model_alias_trail,
            vec!["coder".to_string(), "large".to_string()]
        );
        assert_eq!(decoded.model_alias_origin.as_deref(), Some("directive"));
    }

    #[test]
    fn prompt_step_marker_wire_defaults_absent_alias_trail() {
        let old_record = serde_json::json!({
            "file_name": "prompt_step_001_plan.json",
            "workflow_name": "wf",
            "step_name": "plan",
            "step_type": "agent",
            "status": "completed",
            "model_alias": "medium",
        });

        let decoded: PromptStepMarkerWire =
            serde_json::from_value(old_record).unwrap();

        assert_eq!(decoded.model_alias.as_deref(), Some("medium"));
        assert!(decoded.model_alias_trail.is_empty());
        assert_eq!(decoded.model_alias_origin, None);
    }

    #[test]
    fn prompt_step_marker_wire_round_trips_alias_trail_and_origin() {
        let step = PromptStepMarkerWire {
            file_name: "prompt_step_001_plan.json".to_string(),
            workflow_name: "wf".to_string(),
            step_name: "plan".to_string(),
            step_type: "agent".to_string(),
            step_source: None,
            step_index: Some(1),
            total_steps: Some(2),
            parent_step_index: None,
            parent_total_steps: None,
            status: "completed".to_string(),
            hidden: false,
            is_pre_prompt_step: false,
            embedded_workflow_name: None,
            artifacts_dir: None,
            diff_path: None,
            response_path: None,
            error: None,
            traceback: None,
            model: Some("claude-opus".to_string()),
            llm_provider: Some("claude".to_string()),
            reasoning_effort: Some("high".to_string()),
            model_alias: Some("coder".to_string()),
            model_alias_trail: vec!["coder".to_string(), "large".to_string()],
            model_alias_origin: Some("default_model".to_string()),
            output: None,
            output_types: None,
        };

        let encoded = serde_json::to_value(&step).unwrap();
        let decoded: PromptStepMarkerWire =
            serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, step);
    }

    #[test]
    fn done_marker_wire_round_trips_every_monitor_field() {
        let done = DoneMarkerWire {
            outcome: Some("monitored".to_string()),
            status_label: Some("MONITORED".to_string()),
            family_shell: Some(FamilyShellWire {
                kind: "monitor".to_string(),
                state: Some("completed".to_string()),
                elapsed_seconds: Some(17.5),
                followup_outcome: Some("not-launchable".to_string()),
                followup_error: Some("no lane to launch into".to_string()),
                followup_degraded_reason: Some(
                    "claim transfer failed".to_string(),
                ),
                followup_prompt_path: Some(
                    "artifacts/monitor_followup.md".to_string(),
                ),
                monitor: Some(FamilyShellMonitorWire {
                    exit_code: Some(0),
                    ..Default::default()
                }),
                gate: None,
                ..Default::default()
            }),
            ..Default::default()
        };

        let encoded = serde_json::to_value(&done).unwrap();
        let decoded: DoneMarkerWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, done);
    }

    #[test]
    fn done_marker_wire_round_trips_every_gate_field() {
        let done = DoneMarkerWire {
            outcome: Some("gated".to_string()),
            status_label: Some("ANSWERED".to_string()),
            family_shell: Some(FamilyShellWire {
                kind: "gate".to_string(),
                id: Some("gate-1".to_string()),
                state: Some("answered".to_string()),
                elapsed_seconds: Some(5.25),
                output_path: Some("gate.out".to_string()),
                output_truncated: true,
                followup_outcome: Some("launched-degraded".to_string()),
                followup_error: Some("claim transfer failed".to_string()),
                followup_degraded_reason: Some(
                    "workspace already reclaimed".to_string(),
                ),
                followup_prompt_path: Some(
                    "artifacts/gate_followup.md".to_string(),
                ),
                monitor: None,
                gate: Some(FamilyShellGateWire {
                    kind: Some("approval".to_string()),
                    bundle_path: Some("gate_bundle.json".to_string()),
                    notification_id: Some("notif-1".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let encoded = serde_json::to_value(&done).unwrap();
        let decoded: DoneMarkerWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, done);
    }

    #[test]
    fn done_marker_wire_without_monitor_fields_still_parses() {
        let old_record = serde_json::json!({
            "outcome": "completed",
            "finished_at": 1_777_900_000.0,
        });

        let decoded: DoneMarkerWire =
            serde_json::from_value(old_record).unwrap();

        assert_eq!(decoded.outcome.as_deref(), Some("completed"));
        assert_eq!(decoded.status_label, None);
        assert_eq!(decoded.family_shell, None);
    }
}
