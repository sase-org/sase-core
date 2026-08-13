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
pub const AGENT_SCAN_WIRE_SCHEMA_VERSION: u32 = 6;

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
    pub monitor_state: Option<String>,
    #[serde(default)]
    pub monitor_exit_code: Option<i64>,
    #[serde(default)]
    pub monitor_elapsed_seconds: Option<f64>,
    #[serde(default)]
    pub status_label: Option<String>,
    /// `--next` launch disposition (`launched` / `launched-degraded` /
    /// `not-launchable`), when the monitor carried a follow-up action.
    #[serde(default)]
    pub monitor_followup_outcome: Option<String>,
    /// Human-readable reason a `--next` action was dropped or degraded,
    /// mirroring `agent_meta.json`.
    #[serde(default)]
    pub monitor_followup_error: Option<String>,
}

/// Bounded JSON value stored under `agent_meta.json::output_variables`.
///
/// The artifact scanner applies the reliability caps before a value reaches
/// this wire type. Keeping the public alias preserves the established export
/// name while allowing every JSON scalar and container shape.
pub type OutputVariableValue = Value;

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
    #[serde(default)]
    pub monitor_id: Option<String>,
    #[serde(default)]
    pub monitor_command: Option<String>,
    #[serde(default)]
    pub monitor_cwd: Option<String>,
    #[serde(default)]
    pub monitor_label: Option<String>,
    #[serde(default)]
    pub monitor_reason: Option<String>,
    #[serde(default)]
    pub monitor_next_action: Option<String>,
    #[serde(default)]
    pub monitor_start_status: Option<String>,
    #[serde(default)]
    pub monitor_stop_status: Option<String>,
    #[serde(default)]
    pub monitor_timeout_seconds: Option<f64>,
    #[serde(default)]
    pub monitor_state: Option<String>,
    #[serde(default)]
    pub monitor_exit_code: Option<i64>,
    #[serde(default)]
    pub monitor_output_path: Option<String>,
    #[serde(default)]
    pub monitor_output_truncated: bool,
    #[serde(default)]
    pub monitor_starter_agent: Option<String>,
    #[serde(default)]
    pub monitor_followup_agent: Option<String>,
    #[serde(default)]
    pub monitor_tail_lines: Option<i64>,
    #[serde(default)]
    pub monitor_pgid: Option<i64>,
    #[serde(default)]
    pub monitor_supervisor_identity: Option<String>,
    #[serde(default)]
    pub monitor_settled: bool,
    #[serde(default)]
    pub monitor_idle_timeout_seconds: Option<f64>,
    #[serde(default)]
    pub monitor_next_output: Option<String>,
    #[serde(default)]
    pub monitor_request_fingerprint: Option<String>,
    /// `--next` launch disposition (`launched` / `launched-degraded` /
    /// `not-launchable`), when the monitor carried a follow-up action.
    #[serde(default)]
    pub monitor_followup_outcome: Option<String>,
    /// Human-readable reason a `--next` action was dropped or degraded.
    #[serde(default)]
    pub monitor_followup_error: Option<String>,
    /// Why a launched follow-up landed in a degraded workspace (e.g. the
    /// original claim could not transfer).
    #[serde(default)]
    pub monitor_followup_degraded_reason: Option<String>,
    /// Durable artifact path the composed follow-up prompt was persisted to
    /// when it could not be launched.
    #[serde(default)]
    pub monitor_followup_prompt_path: Option<String>,
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
            monitor_id: Some("m4kq".to_string()),
            monitor_command: Some("just check-full".to_string()),
            monitor_cwd: Some("/home/bryan/workspaces/acme".to_string()),
            monitor_label: Some("just check-full".to_string()),
            monitor_reason: Some("Verify the refactor".to_string()),
            monitor_next_action: Some("Reply to the user.".to_string()),
            monitor_start_status: Some("MONITORING".to_string()),
            monitor_stop_status: Some("MONITORED".to_string()),
            monitor_timeout_seconds: Some(2_700.0),
            monitor_state: Some("running".to_string()),
            monitor_exit_code: Some(1),
            monitor_output_path: Some("live_reply.md".to_string()),
            monitor_output_truncated: true,
            monitor_starter_agent: Some("acme--0".to_string()),
            monitor_followup_agent: Some("acme--1".to_string()),
            monitor_tail_lines: Some(200),
            monitor_pgid: Some(4242),
            monitor_supervisor_identity: Some("boot-abc123:98765".to_string()),
            monitor_settled: true,
            monitor_idle_timeout_seconds: Some(600.0),
            monitor_next_output: Some("tail".to_string()),
            monitor_request_fingerprint: Some("sha256:deadbeef".to_string()),
            monitor_followup_outcome: Some("launched-degraded".to_string()),
            monitor_followup_error: Some(
                "workspace claim transfer failed".to_string(),
            ),
            monitor_followup_degraded_reason: Some(
                "original claim already released".to_string(),
            ),
            monitor_followup_prompt_path: Some(
                "artifacts/followup_prompt.md".to_string(),
            ),
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
        assert_eq!(decoded.monitor_id, None);
        assert_eq!(decoded.monitor_state, None);
        assert!(!decoded.monitor_output_truncated);
        assert_eq!(decoded.monitor_pgid, None);
        assert_eq!(decoded.monitor_supervisor_identity, None);
        assert!(!decoded.monitor_settled);
        assert_eq!(decoded.monitor_idle_timeout_seconds, None);
        assert_eq!(decoded.monitor_next_output, None);
        assert_eq!(decoded.monitor_request_fingerprint, None);
        assert_eq!(decoded.monitor_followup_outcome, None);
        assert_eq!(decoded.monitor_followup_error, None);
        assert_eq!(decoded.monitor_followup_degraded_reason, None);
        assert_eq!(decoded.monitor_followup_prompt_path, None);
    }

    #[test]
    fn done_marker_wire_round_trips_every_monitor_field() {
        let done = DoneMarkerWire {
            outcome: Some("monitored".to_string()),
            monitor_state: Some("completed".to_string()),
            monitor_exit_code: Some(0),
            monitor_elapsed_seconds: Some(17.5),
            status_label: Some("MONITORED".to_string()),
            monitor_followup_outcome: Some("not-launchable".to_string()),
            monitor_followup_error: Some("no lane to launch into".to_string()),
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
        assert_eq!(decoded.monitor_state, None);
        assert_eq!(decoded.monitor_exit_code, None);
        assert_eq!(decoded.status_label, None);
    }
}
