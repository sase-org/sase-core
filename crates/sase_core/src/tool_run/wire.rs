//! Versioned ToolRun ledger wire types.
//!
//! Public JSON envelopes always include `schema_version: 1` and
//! `diagnostics`. Missing observations are `null` plus a typed explanation;
//! callers must never substitute zero for a missing duration, PSI sample,
//! or fingerprint component.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub const TOOL_RUN_WIRE_SCHEMA_VERSION: u32 = 1;
pub const TOOL_RUN_MAX_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
pub const TOOL_RUN_DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_millis(250);

pub const TOOL_RUN_DEFAULT_SUMMARY_DAYS: u32 = 180;
pub const TOOL_RUN_DEFAULT_DETAIL_DAYS: u32 = 60;
pub const TOOL_RUN_DEFAULT_LOG_DAYS: u32 = 14;
pub const TOOL_RUN_DEFAULT_LOG_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const TOOL_RUN_DEFAULT_RUN_LOG_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const TOOL_RUN_DEFAULT_EVENT_MAX_BYTES: u64 = 16 * 1024 * 1024;

pub const TOOL_RUN_LIST_DEFAULT_LIMIT: u32 = 50;
pub const TOOL_RUN_LIST_MAX_LIMIT: u32 = 1000;
pub const TOOL_RUN_TYPICAL_SAMPLE_LIMIT: u32 = 30;
pub const TOOL_RUN_TYPICAL_WINDOW_DAYS: u32 = 30;

pub const TOOL_RUN_LOST_REASON_RUNNER_EXITED: &str =
    "runner exited without settling";

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn empty_diagnostics() -> Vec<String> {
    Vec::new()
}

fn default_attempt() -> u32 {
    1
}

fn default_commit_running() -> bool {
    true
}

fn default_list_limit() -> u32 {
    TOOL_RUN_LIST_DEFAULT_LIMIT
}

fn default_summary_days() -> u32 {
    TOOL_RUN_DEFAULT_SUMMARY_DAYS
}

fn default_detail_days() -> u32 {
    TOOL_RUN_DEFAULT_DETAIL_DAYS
}

fn default_log_days() -> u32 {
    TOOL_RUN_DEFAULT_LOG_DAYS
}

fn default_log_max_bytes() -> u64 {
    TOOL_RUN_DEFAULT_LOG_MAX_BYTES
}

fn default_run_log_max_bytes() -> u64 {
    TOOL_RUN_DEFAULT_RUN_LOG_MAX_BYTES
}

fn default_event_max_bytes() -> u64 {
    TOOL_RUN_DEFAULT_EVENT_MAX_BYTES
}

fn default_args_policy() -> ToolArgsPolicyWire {
    ToolArgsPolicyWire::Deny
}

fn default_stages() -> ToolStagesWire {
    ToolStagesWire::None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunStateWire {
    Created,
    Running,
    Succeeded,
    Failed,
    Signaled,
    Interrupted,
    Lost,
}

impl ToolRunStateWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Signaled => "signaled",
            Self::Interrupted => "interrupted",
            Self::Lost => "lost",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "created" => Ok(Self::Created),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "signaled" => Ok(Self::Signaled),
            "interrupted" => Ok(Self::Interrupted),
            "lost" => Ok(Self::Lost),
            other => Err(format!("unknown tool run state {other:?}")),
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::Signaled
                | Self::Interrupted
                | Self::Lost
        )
    }

    pub fn is_unsettled(self) -> bool {
        matches!(self, Self::Created | Self::Running)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunSourceWire {
    Native,
}

impl ToolRunSourceWire {
    pub fn as_str(self) -> &'static str {
        "native"
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "native" => Ok(Self::Native),
            other => Err(format!("unknown tool run source {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunExecutorWire {
    Inline,
}

impl ToolRunExecutorWire {
    pub fn as_str(self) -> &'static str {
        "inline"
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "inline" => Ok(Self::Inline),
            other => Err(format!("unknown tool run executor {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolStagesWire {
    RunSilent,
    None,
}

impl ToolStagesWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunSilent => "run_silent",
            Self::None => "none",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "run_silent" => Ok(Self::RunSilent),
            "none" => Ok(Self::None),
            other => Err(format!("unknown tool stages value {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolArgsPolicyWire {
    Allow,
    Deny,
}

impl ToolArgsPolicyWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "allow" => Ok(Self::Allow),
            "deny" => Ok(Self::Deny),
            other => Err(format!("unknown tool args policy {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunEventKindWire {
    Created,
    Running,
    Succeeded,
    Failed,
    Signaled,
    Interrupted,
    Lost,
    StageStarted,
    StageFinished,
    Sample,
}

impl ToolRunEventKindWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Signaled => "signaled",
            Self::Interrupted => "interrupted",
            Self::Lost => "lost",
            Self::StageStarted => "stage_started",
            Self::StageFinished => "stage_finished",
            Self::Sample => "sample",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "created" => Ok(Self::Created),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "signaled" => Ok(Self::Signaled),
            "interrupted" => Ok(Self::Interrupted),
            "lost" => Ok(Self::Lost),
            "stage_started" => Ok(Self::StageStarted),
            "stage_finished" => Ok(Self::StageFinished),
            "sample" => Ok(Self::Sample),
            other => Err(format!("unknown tool run event kind {other:?}")),
        }
    }

    pub fn is_lifecycle(self) -> bool {
        matches!(
            self,
            Self::Created
                | Self::Running
                | Self::Succeeded
                | Self::Failed
                | Self::Signaled
                | Self::Interrupted
                | Self::Lost
        )
    }

    pub fn target_state(self) -> Option<ToolRunStateWire> {
        match self {
            Self::Created => Some(ToolRunStateWire::Created),
            Self::Running => Some(ToolRunStateWire::Running),
            Self::Succeeded => Some(ToolRunStateWire::Succeeded),
            Self::Failed => Some(ToolRunStateWire::Failed),
            Self::Signaled => Some(ToolRunStateWire::Signaled),
            Self::Interrupted => Some(ToolRunStateWire::Interrupted),
            Self::Lost => Some(ToolRunStateWire::Lost),
            Self::StageStarted | Self::StageFinished | Self::Sample => None,
        }
    }

    pub fn is_detail(self) -> bool {
        matches!(
            self,
            Self::StageStarted | Self::StageFinished | Self::Sample
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolLivenessObservationWire {
    Alive,
    Dead,
    Unknown,
}

impl ToolLivenessObservationWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Dead => "dead",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolFingerprintSpecWire {
    #[serde(default)]
    pub repos: Vec<String>,
    #[serde(default)]
    pub toolchain: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDefinitionWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub name: String,
    pub argv: Vec<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_stages")]
    pub stages: ToolStagesWire,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default = "default_args_policy")]
    pub args: ToolArgsPolicyWire,
    #[serde(default)]
    pub fingerprint: ToolFingerprintSpecWire,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDefinitionNormalizeResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub definition: ToolDefinitionWire,
    pub digest: String,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDirtyPathWire {
    pub path: String,
    pub status: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRepoFingerprintWire {
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_tree: Option<String>,
    #[serde(default)]
    pub dirty_paths: Vec<ToolDirtyPathWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInputMatchWire {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInputFingerprintWire {
    pub pattern: String,
    #[serde(default)]
    pub matches: Vec<ToolInputMatchWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolToolchainProbeWire {
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolEvidenceCompletenessWire {
    pub complete: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

impl Default for ToolEvidenceCompletenessWire {
    fn default() -> Self {
        Self {
            complete: false,
            missing: vec!["evidence not yet observed".to_string()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolFingerprintWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_args_digest: Option<String>,
    #[serde(default)]
    pub repos: Vec<ToolRepoFingerprintWire>,
    #[serde(default)]
    pub inputs: Vec<ToolInputFingerprintWire>,
    #[serde(default)]
    pub env: BTreeMap<String, Option<String>>,
    #[serde(default)]
    pub toolchain: BTreeMap<String, ToolToolchainProbeWire>,
    #[serde(default)]
    pub completeness: ToolEvidenceCompletenessWire,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

impl Default for ToolFingerprintWire {
    fn default() -> Self {
        Self {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project_identity: None,
            definition_digest: None,
            extra_args_digest: None,
            repos: Vec::new(),
            inputs: Vec::new(),
            env: BTreeMap::new(),
            toolchain: BTreeMap::new(),
            completeness: ToolEvidenceCompletenessWire::default(),
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolFingerprintCanonicalizeResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub fingerprint: ToolFingerprintWire,
    pub digest: String,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolStageWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub stage_id: String,
    pub run_id: String,
    #[serde(default = "default_attempt")]
    pub attempt: u32,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<i64>,
    #[serde(default)]
    pub incomplete: bool,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolLoadSampleWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub sample_id: String,
    pub run_id: String,
    #[serde(default = "default_attempt")]
    pub attempt: u32,
    pub observed_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loadavg_1: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loadavg_5: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loadavg_15: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_cpus: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub psi_cpu_some: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub psi_memory_some: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub psi_io_some: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_identity: Option<String>,
    #[serde(default)]
    pub availability: Vec<String>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunEventWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub event_id: String,
    pub run_id: String,
    #[serde(default = "default_attempt")]
    pub attempt: u32,
    pub kind: ToolRunEventKindWire,
    pub created_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<ToolStageWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample: Option<ToolLoadSampleWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolAttemptWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub attempt: u32,
    pub state: ToolRunStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunLogMetadataWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_path: Option<String>,
    pub has_private_argv: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_log_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub state: ToolRunStateWire,
    pub source: ToolRunSourceWire,
    pub executor: ToolRunExecutorWire,
    #[serde(default = "default_attempt")]
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub definition_digest: String,
    pub extra_args_digest: String,
    pub display_argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub created_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_missing: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lost_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrapper_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_pgid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutated_input: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_before: Option<ToolFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_after: Option<ToolFingerprintWire>,
    pub logs: ToolRunLogMetadataWire,
    #[serde(default)]
    pub evidence_completeness: ToolEvidenceCompletenessWire,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_cause: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_request: Option<super::handoff_wire::ToolRunStopRecordWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launcher: Option<super::handoff_wire::ToolRunProcessIdentityWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunBeginRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub definition: ToolDefinitionWire,
    #[serde(default)]
    pub extra_args: Vec<String>,
    pub display_argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_argv: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrapper_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_stdout_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_stderr_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
    #[serde(default = "default_commit_running")]
    pub commit_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_mode: Option<super::handoff_wire::ToolRunLaunchModeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch: Option<super::handoff_wire::ToolRunLaunchEnvelopeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_log_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunBeginResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run: ToolRunWire,
    pub created_event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_event_id: Option<String>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunAppendRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub event: ToolRunEventWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunAppendResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub event_id: String,
    pub replayed: bool,
    pub run: ToolRunWire,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunFinishRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub state: ToolRunStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lost_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_pgid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_before: Option<ToolFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_after: Option<ToolFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutated_input: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_cause: Option<super::handoff_wire::ToolRunTerminalCauseWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunFinishResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run: ToolRunWire,
    pub event_id: String,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunLivenessFactWire {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrapper_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_identity: Option<String>,
    pub observation: ToolLivenessObservationWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<super::handoff_wire::ToolRunOwnerFactWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunReconcileRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub facts: Vec<ToolRunLivenessFactWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunReapCandidateWire {
    pub run_id: String,
    pub pgid: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_process_start_identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunReconcileResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub marked_lost: Vec<String>,
    pub persisted: bool,
    #[serde(default)]
    pub reap_candidates: Vec<ToolRunReapCandidateWire>,
    #[serde(default)]
    pub settled: Vec<super::handoff_wire::ToolRunReconcileSettlementWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunObserveRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_pgid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_before: Option<ToolFingerprintWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunObserveResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run: ToolRunWire,
    pub replayed: bool,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunListRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ToolRunStateWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default)]
    pub include_all: bool,
    #[serde(default = "default_list_limit")]
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunListResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub runs: Vec<ToolRunWire>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunShowRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunShowResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<ToolRunWire>,
    #[serde(default)]
    pub attempt: Option<ToolAttemptWire>,
    #[serde(default)]
    pub events: Vec<ToolRunEventWire>,
    #[serde(default)]
    pub stages: Vec<ToolStageWire>,
    #[serde(default)]
    pub samples: Vec<ToolLoadSampleWire>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunSummaryRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub project: String,
    pub tool_name: String,
    pub definition_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunSummaryResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<ToolRunWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_duration_ms: Option<i64>,
    pub typical_sample_count: u32,
    #[serde(default)]
    pub typical_status_breakdown: BTreeMap<String, u32>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunRetentionPolicyWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_summary_days")]
    pub summary_days: u32,
    #[serde(default = "default_detail_days")]
    pub detail_days: u32,
    #[serde(default = "default_log_days")]
    pub log_days: u32,
    #[serde(default = "default_log_max_bytes")]
    pub log_max_bytes: u64,
    #[serde(default = "default_run_log_max_bytes")]
    pub run_log_max_bytes: u64,
    #[serde(default = "default_event_max_bytes")]
    pub event_max_bytes: u64,
}

impl Default for ToolRunRetentionPolicyWire {
    fn default() -> Self {
        Self {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            summary_days: TOOL_RUN_DEFAULT_SUMMARY_DAYS,
            detail_days: TOOL_RUN_DEFAULT_DETAIL_DAYS,
            log_days: TOOL_RUN_DEFAULT_LOG_DAYS,
            log_max_bytes: TOOL_RUN_DEFAULT_LOG_MAX_BYTES,
            run_log_max_bytes: TOOL_RUN_DEFAULT_RUN_LOG_MAX_BYTES,
            event_max_bytes: TOOL_RUN_DEFAULT_EVENT_MAX_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunDeletionCandidateWire {
    pub kind: String,
    pub run_id: Option<String>,
    pub path: Option<String>,
    pub protected: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunRetentionRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub policy: ToolRunRetentionPolicyWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunRetentionResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub dry_run: bool,
    pub summary_rows: u64,
    pub detail_rows: u64,
    pub file_candidates: Vec<ToolRunDeletionCandidateWire>,
    pub protected_unsettled: u64,
    /// Bytes of retained log/event files that remain after every candidate is
    /// deleted, measured with a no-follow `lstat`.
    #[serde(default)]
    pub retained_bytes: u64,
    /// Bytes held by unsettled runs, which age and aggregate deletion never touch.
    #[serde(default)]
    pub protected_bytes: u64,
    /// How far `retained_bytes` still exceeds `log_max_bytes` because protected
    /// or unselectable files alone are over the aggregate target.
    #[serde(default)]
    pub over_target_bytes: u64,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStoreStatsWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub store_path: String,
    pub exists: bool,
    pub db_size_bytes: u64,
    pub run_count: u64,
    pub attempt_count: u64,
    pub event_count: u64,
    pub stage_count: u64,
    pub sample_count: u64,
    pub unsettled_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_write_ts: Option<i64>,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}
