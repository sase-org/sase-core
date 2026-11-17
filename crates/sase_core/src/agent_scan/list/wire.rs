//! Compact list DTOs for agent artifact rows.
//!
//! Field names differ from the full record (`meta` vs `agent_meta`,
//! `workflow` vs `workflow_state`) so a compact payload cannot be
//! mistaken for a nested full record. `Option::None` serializes as JSON
//! `null`; empty lists serialize as `[]`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::agent_scan::wire::{
    AgentArtifactIndexCompletenessWire, AgentArtifactIndexWindowWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentClanContextWire, CreatedEpicWire, ImportedSourceOwnerWire,
    WaitEpicFollowEntryWire, WaitTurnBindingWire,
};

/// Schema version for the additive compact list snapshot.
pub const AGENT_ARTIFACT_LIST_WIRE_SCHEMA_VERSION: u32 = 1;

/// Payload shape discriminator for a compact list record.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactListShapeWire {
    #[default]
    Compact,
}

/// List-time facts extracted from a step or done output map.
///
/// Only `meta_*` keys and `diff_path` survive. `_raw`, `_data`, and other
/// detail payloads are dropped.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListOutputFactsWire {
    #[serde(default)]
    pub meta: BTreeMap<String, Value>,
    #[serde(default)]
    pub diff_path: Option<String>,
}

/// Compact recovery facts used by list restart and hijack chips.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListRecoveryWire {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub reason_text: Option<String>,
    #[serde(default)]
    pub requested_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub episode_id: Option<String>,
    #[serde(default)]
    pub hijack_id: Option<String>,
}

/// One compact finalizer instance used by the list chip.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListFinalizerInstanceWire {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub op: Option<String>,
    #[serde(default)]
    pub step: Option<String>,
}

/// Compact finalizer overlay used by FINALIZING list paint.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListFinalizerWire {
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub updated_at: Option<f64>,
    #[serde(default)]
    pub instance_count: Option<i64>,
    #[serde(default)]
    pub instances: Vec<AgentArtifactListFinalizerInstanceWire>,
}

/// Compact monitor-only list facts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListMonitorWire {
    #[serde(default)]
    pub exit_code: Option<i64>,
}

/// Compact gate-only list facts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListGateWire {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub accent: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
    #[serde(default)]
    pub notification_id: Option<String>,
}

/// Compact session-turn facts for list paint and row-local actions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListTurnWire {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub start_status: Option<String>,
    #[serde(default)]
    pub stop_status: Option<String>,
    #[serde(default)]
    pub followup_outcome: Option<String>,
    #[serde(default)]
    pub followup_error: Option<String>,
    #[serde(default)]
    pub followup_agent: Option<String>,
    #[serde(default)]
    pub followup_degraded_reason: Option<String>,
    #[serde(default)]
    pub host_completion_status: Option<String>,
    #[serde(default)]
    pub monitor: Option<AgentArtifactListMonitorWire>,
    #[serde(default)]
    pub gate: Option<AgentArtifactListGateWire>,
}

/// Compact metadata facts needed to construct an Agent list row.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListMetaWire {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub process_identity: Option<String>,
    #[serde(default)]
    pub runner_claim_owner_key: Option<String>,
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
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub plan_path: Option<String>,
    #[serde(default)]
    pub sdd_plan_path: Option<String>,
    #[serde(default)]
    pub epic_plan_ref: Option<String>,
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    pub plan_approved: bool,
    #[serde(default)]
    pub plan_action: Option<String>,
    #[serde(default)]
    pub plan_committed: Option<bool>,
    #[serde(default)]
    pub plan_submitted_at: Vec<String>,
    #[serde(default)]
    pub epic_bead_id: Option<String>,
    #[serde(default)]
    pub phase_bead_id: Option<String>,
    #[serde(default)]
    pub created_epics: Vec<CreatedEpicWire>,
    #[serde(default)]
    pub commit_diff_path: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    #[serde(default)]
    pub agent_tab: Option<String>,
    #[serde(default)]
    pub wait_for: Vec<String>,
    #[serde(default)]
    pub wait_for_beads: Vec<String>,
    #[serde(default)]
    pub wait_for_hoods: Vec<String>,
    #[serde(default)]
    pub wait_for_epics_of: Vec<String>,
    #[serde(default)]
    pub wait_epic_follows: Vec<WaitEpicFollowEntryWire>,
    #[serde(default)]
    pub wait_turn_bindings: Vec<WaitTurnBindingWire>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
    #[serde(default)]
    pub queue_capacity: Option<i64>,
    #[serde(default)]
    pub queue_capacity_explicit: bool,
    #[serde(default)]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default)]
    pub wait_priority: Option<i64>,
    #[serde(default)]
    pub queue_weight: Option<f64>,
    #[serde(default)]
    pub queue_weight_explicit: bool,
    #[serde(default)]
    pub queue_weight_invalid: bool,
    #[serde(default)]
    pub queue_weight_error: Option<String>,
    #[serde(default)]
    pub approve: bool,
    #[serde(default)]
    pub auto_approve_plan_action: Option<String>,
    #[serde(default)]
    pub auto_approve_argument: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub role_suffix: Option<String>,
    #[serde(default, alias = "agent_family")]
    pub agent_session: Option<String>,
    #[serde(default, alias = "agent_family_role")]
    pub agent_session_role: Option<String>,
    #[serde(default, alias = "agent_family_parallel")]
    pub agent_session_parallel: bool,
    #[serde(default)]
    pub plan_chain_root: bool,
    #[serde(default)]
    pub agent_clan: Option<String>,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
    #[serde(default)]
    pub clan_summary: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
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
    pub retry_started_at: Vec<String>,
    #[serde(default)]
    pub epic_started_at: Option<String>,
    #[serde(default)]
    pub feedback_submitted_at: Vec<String>,
    #[serde(default)]
    pub questions_submitted_at: Vec<String>,
    #[serde(default)]
    pub run_started_at: Option<String>,
    #[serde(default)]
    pub stopped_at: Option<String>,
    #[serde(default)]
    pub wait_completed_at: Option<String>,
    #[serde(default)]
    pub question_request_path: Option<String>,
    #[serde(default)]
    pub question_response_path: Option<String>,
    #[serde(default)]
    pub question_session_id: Option<String>,
    #[serde(default)]
    pub imported_source_owner: Option<ImportedSourceOwnerWire>,
    #[serde(default)]
    pub source_machine: Option<String>,
    #[serde(default)]
    pub output_path: Option<String>,
    #[serde(default)]
    pub finalizer_status: Option<AgentArtifactListFinalizerWire>,
    #[serde(default)]
    pub auto_restart: Option<Map<String, Value>>,
    #[serde(default)]
    pub agent_session_turn: Option<AgentArtifactListTurnWire>,
}

/// Compact done-marker facts for list paint and status overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListDoneWire {
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub project_file: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
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
    pub step_output: Option<AgentArtifactListOutputFactsWire>,
    #[serde(default)]
    pub error: Option<String>,
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
    #[serde(default)]
    pub repeat_stopped: bool,
    #[serde(default)]
    pub status_label: Option<String>,
    #[serde(default)]
    pub imported_source_owner: Option<ImportedSourceOwnerWire>,
    #[serde(default)]
    pub source_machine: Option<String>,
    #[serde(default)]
    pub recovery: Option<AgentArtifactListRecoveryWire>,
    #[serde(default)]
    pub agent_session_turn: Option<AgentArtifactListTurnWire>,
}

/// Compact running-marker facts. Presence is required for a home running
/// row; `None` means the marker was absent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListRunningWire {
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub process_identity: Option<String>,
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

/// Compact waiting-marker facts. Presence on STARTING/RUNNING forces
/// WAITING even when wait lists are empty.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListWaitingWire {
    #[serde(default)]
    pub waiting_for: Vec<String>,
    #[serde(default)]
    pub wait_for_beads: Vec<String>,
    #[serde(default)]
    pub wait_for_hoods: Vec<String>,
    #[serde(default)]
    pub wait_for_epics_of: Vec<String>,
    #[serde(default)]
    pub wait_epic_follows: Vec<WaitEpicFollowEntryWire>,
    #[serde(default)]
    pub wait_turn_bindings: Vec<WaitTurnBindingWire>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
    #[serde(default)]
    pub queue_capacity: Option<i64>,
    #[serde(default)]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default)]
    pub queue_capacity_explicit: bool,
    #[serde(default)]
    pub wait_priority: Option<i64>,
    #[serde(default)]
    pub wait_priority_explicit: Option<bool>,
    #[serde(default)]
    pub queue_weight: Option<f64>,
    #[serde(default)]
    pub queue_weight_explicit: bool,
    #[serde(default)]
    pub queue_weight_invalid: bool,
    #[serde(default)]
    pub queue_weight_error: Option<String>,
    #[serde(default)]
    pub slot_requested_at: Option<String>,
}

/// Compact pending-question marker. Presence yields the runner slot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactListPendingQuestionWire {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub request_path: Option<String>,
    #[serde(default)]
    pub submitted_at: Option<String>,
}

/// Compact workflow-state step used by parent merge and child status.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListWorkflowStepWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub output: Option<AgentArtifactListOutputFactsWire>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Compact workflow parent facts. Presence is required for a workflow
/// parent row; `None` means the marker was absent.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListWorkflowWire {
    #[serde(default)]
    pub workflow_name: String,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
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
    pub activity: Option<String>,
    #[serde(default)]
    pub steps: Vec<AgentArtifactListWorkflowStepWire>,
}

/// Compact prompt-step child facts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListStepWire {
    #[serde(default)]
    pub file_name: String,
    #[serde(default)]
    pub workflow_name: String,
    #[serde(default)]
    pub step_name: String,
    #[serde(default)]
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
    #[serde(default)]
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
    pub error: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub model_alias: Option<String>,
    #[serde(default)]
    pub output: Option<AgentArtifactListOutputFactsWire>,
}

/// One compact artifact row for TUI list construction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListRecordWire {
    pub project_name: String,
    pub project_dir: String,
    pub project_file: String,
    pub workflow_dir_name: String,
    pub artifact_dir: String,
    pub timestamp: String,
    #[serde(default)]
    pub has_done_marker: bool,
    #[serde(default)]
    pub record_shape: AgentArtifactListShapeWire,
    #[serde(default)]
    pub meta: Option<AgentArtifactListMetaWire>,
    #[serde(default)]
    pub done: Option<AgentArtifactListDoneWire>,
    #[serde(default)]
    pub running: Option<AgentArtifactListRunningWire>,
    #[serde(default)]
    pub waiting: Option<AgentArtifactListWaitingWire>,
    #[serde(default)]
    pub pending_question: Option<AgentArtifactListPendingQuestionWire>,
    #[serde(default)]
    pub workflow: Option<AgentArtifactListWorkflowWire>,
    #[serde(default)]
    pub plan_path: Option<String>,
    #[serde(default)]
    pub prompt_steps: Vec<AgentArtifactListStepWire>,
}

/// Compact snapshot returned by list index queries and source scans.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentArtifactListSnapshotWire {
    pub schema_version: u32,
    pub projects_root: String,
    pub options: AgentArtifactScanOptionsWire,
    pub stats: AgentArtifactScanStatsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_window: Option<AgentArtifactIndexWindowWire>,
    #[serde(default)]
    pub records: Vec<AgentArtifactListRecordWire>,
    #[serde(default)]
    pub clan_context: Vec<AgentClanContextWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_completeness: Option<AgentArtifactIndexCompletenessWire>,
}
