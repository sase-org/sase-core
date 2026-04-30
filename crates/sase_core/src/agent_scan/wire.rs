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
pub const AGENT_SCAN_WIRE_SCHEMA_VERSION: u32 = 1;

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
    pub include_prompt_step_markers: bool,
    pub include_raw_prompt_snippets: bool,
    pub max_prompt_snippet_bytes: u32,
    #[serde(default)]
    pub only_workflow_dirs: Vec<String>,
}

impl Default for AgentArtifactScanOptionsWire {
    fn default() -> Self {
        Self {
            include_prompt_step_markers: true,
            include_raw_prompt_snippets: true,
            max_prompt_snippet_bytes: 200,
            only_workflow_dirs: Vec::new(),
        }
    }
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
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub project_file: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
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
}

/// Compact projection of `agent_meta.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentMetaWire {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub vcs_provider: Option<String>,
    #[serde(default)]
    pub role_suffix: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub approve: bool,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    pub plan_approved: bool,
    #[serde(default)]
    pub plan_action: Option<String>,
    #[serde(default)]
    pub wait_for: Vec<String>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
    #[serde(default)]
    pub plan_submitted_at: Vec<String>,
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
}

/// Compact projection of `waiting.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WaitingMarkerWire {
    #[serde(default)]
    pub waiting_for: Vec<String>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
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
    pub current_step_index: i64,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
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
            current_step_index: 0,
            start_time: None,
            error: None,
            traceback: None,
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
    pub workflow_state: Option<WorkflowStateWire>,
    #[serde(default)]
    pub plan_path: Option<PlanPathMarkerWire>,
    #[serde(default)]
    pub prompt_steps: Vec<PromptStepMarkerWire>,
    #[serde(default)]
    pub raw_prompt_snippet: Option<String>,
    #[serde(default)]
    pub has_done_marker: bool,
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
