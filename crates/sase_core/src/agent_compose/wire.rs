//! Wire records for deterministic agent-list composition.
//!
//! These records mirror `sase_100/src/sase/core/agent_compose_wire.py`.
//! The pure Rust core intentionally stays PyO3-free; Python binding and product
//! routing are later migration phases.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::agent_scan::AgentArtifactScanWire;
use crate::wire::ChangeSpecWire;

pub const AGENT_COMPOSE_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentIdentityWire(pub String, pub String, pub Option<String>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunningClaimWire {
    pub project_file: String,
    pub project_name: String,
    pub cl_name: String,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub vcs_provider: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub approve: bool,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub bug: Option<String>,
    #[serde(default)]
    pub cl_num: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentComposeOptionsWire {
    pub include_diagnostics: bool,
    pub include_workflow_steps: bool,
    pub tui_mode: bool,
}

impl Default for AgentComposeOptionsWire {
    fn default() -> Self {
        Self {
            include_diagnostics: true,
            include_workflow_steps: true,
            tui_mode: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentComposeInputWire {
    pub schema_version: u32,
    #[serde(default)]
    pub artifact_scan: Option<AgentArtifactScanWire>,
    #[serde(default)]
    pub changespecs: Vec<ChangeSpecWire>,
    #[serde(default)]
    pub running_claims: Vec<RunningClaimWire>,
    #[serde(default)]
    pub alive_pids: Vec<i64>,
    #[serde(default)]
    pub dead_pids: Vec<i64>,
    #[serde(default)]
    pub dismissed_identities: Vec<AgentIdentityWire>,
    #[serde(default)]
    pub dismissed_suffixes: Vec<String>,
    #[serde(default)]
    pub options: AgentComposeOptionsWire,
}

impl Default for AgentComposeInputWire {
    fn default() -> Self {
        Self {
            schema_version: AGENT_COMPOSE_WIRE_SCHEMA_VERSION,
            artifact_scan: None,
            changespecs: Vec::new(),
            running_claims: Vec::new(),
            alive_pids: Vec::new(),
            dead_pids: Vec::new(),
            dismissed_identities: Vec::new(),
            dismissed_suffixes: Vec::new(),
            options: AgentComposeOptionsWire::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropReasonWire {
    pub stage: String,
    pub identity: AgentIdentityWire,
    pub reason: String,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeReasonWire {
    pub stage: String,
    pub source_identity: AgentIdentityWire,
    pub target_identity: AgentIdentityWire,
    pub reason: String,
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentWire {
    pub agent_type: String,
    pub cl_name: String,
    pub project_file: String,
    pub status: String,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub run_start_time: Option<String>,
    #[serde(default)]
    pub stop_time: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub hook_command: Option<String>,
    #[serde(default)]
    pub commit_entry_id: Option<String>,
    #[serde(default)]
    pub mentor_profile: Option<String>,
    #[serde(default)]
    pub mentor_name: Option<String>,
    #[serde(default)]
    pub reviewer: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub response_path: Option<String>,
    #[serde(default)]
    pub diff_path: Option<String>,
    #[serde(default)]
    pub extra_files: Vec<String>,
    #[serde(default)]
    pub bug: Option<String>,
    #[serde(default)]
    pub cl_num: Option<String>,
    #[serde(default)]
    pub parent_workflow: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub step_name: Option<String>,
    #[serde(default)]
    pub step_type: Option<String>,
    #[serde(default)]
    pub step_source: Option<String>,
    #[serde(default)]
    pub step_output: Option<Map<String, Value>>,
    #[serde(default)]
    pub step_index: Option<i64>,
    #[serde(default)]
    pub total_steps: Option<i64>,
    #[serde(default)]
    pub parent_step_index: Option<i64>,
    #[serde(default)]
    pub parent_total_steps: Option<i64>,
    #[serde(default)]
    pub is_hidden_step: bool,
    #[serde(default)]
    pub appears_as_agent: bool,
    #[serde(default)]
    pub is_anonymous: bool,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub error_traceback: Option<String>,
    #[serde(default)]
    pub output_path: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub llm_provider: Option<String>,
    #[serde(default)]
    pub vcs_provider: Option<String>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub waiting_for: Vec<String>,
    #[serde(default)]
    pub wait_duration: Option<f64>,
    #[serde(default)]
    pub wait_until: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub embedded_workflow_name: Option<String>,
    #[serde(default)]
    pub is_pre_prompt_step: bool,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub retry_count: i64,
    #[serde(default)]
    pub max_retries: i64,
    #[serde(default)]
    pub retry_next_at_epoch: Option<f64>,
    #[serde(default)]
    pub retry_wait_seconds: i64,
    #[serde(default)]
    pub using_fallback: bool,
    #[serde(default)]
    pub fallback_model: Option<String>,
    #[serde(default)]
    pub retry_status: Option<String>,
    #[serde(default)]
    pub from_changespec: bool,
    #[serde(default)]
    pub approve: bool,
    #[serde(default)]
    pub role_suffix: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub retry_of_timestamp: Option<String>,
    #[serde(default)]
    pub retry_attempt: i64,
    #[serde(default)]
    pub retry_chain_root_timestamp: Option<String>,
    #[serde(default)]
    pub retried_as_timestamp: Option<String>,
    #[serde(default)]
    pub retry_terminal: bool,
    #[serde(default)]
    pub retry_error_category: Option<String>,
    #[serde(default)]
    pub plan_times: Vec<String>,
    #[serde(default)]
    pub code_time: Option<String>,
    #[serde(default)]
    pub feedback_times: Vec<String>,
    #[serde(default)]
    pub questions_times: Vec<String>,
    #[serde(default)]
    pub retry_times: Vec<String>,
    #[serde(default)]
    pub followup_identities: Vec<AgentIdentityWire>,
    #[serde(default)]
    pub retry_chain_sibling_identities: Vec<AgentIdentityWire>,
}

impl AgentWire {
    pub fn identity(&self) -> AgentIdentityWire {
        AgentIdentityWire(
            self.agent_type.clone(),
            self.cl_name.clone(),
            self.raw_suffix.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComposedAgentListWire {
    pub schema_version: u32,
    #[serde(default)]
    pub agents: Vec<AgentWire>,
    #[serde(default)]
    pub workflow_agent_steps: Vec<AgentWire>,
    #[serde(default)]
    pub dismissed_from_loader: Vec<AgentWire>,
    #[serde(default)]
    pub dropped: Vec<DropReasonWire>,
    #[serde(default)]
    pub merge_log: Vec<MergeReasonWire>,
}
