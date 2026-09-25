use serde::{Deserialize, Serialize};

use crate::agent_hold::AgentHoldRecordWire;

use crate::queue_directive::resolve_queue_capacity;

pub const RUNNER_CAPACITY_POLICY_SCHEMA_VERSION: u32 = 5;
pub const DEFAULT_WAIT_PRIORITY: i32 = 10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityRequestWire {
    #[serde(default = "runner_capacity_policy_schema_version")]
    pub schema_version: u32,
    pub effective_limit: f64,
    #[serde(default)]
    pub records: Vec<RunnerCapacityRecordWire>,
    #[serde(default)]
    pub holds: Vec<AgentHoldRecordWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<RunnerCapacityRecordWire>,
    #[serde(default)]
    pub now: Option<String>,
    #[serde(default)]
    pub deference_seconds_per_step: u32,
    #[serde(default)]
    pub deference_max_seconds: u32,
    #[serde(default)]
    pub feature_flags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityRecordWire {
    pub artifact_dir: String,
    pub project_name: String,
    #[serde(default = "default_workflow_dir")]
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub clan: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    #[serde(default)]
    pub tribes: Vec<String>,
    #[serde(default)]
    pub created_at: Option<f64>,
    #[serde(default = "default_true")]
    pub has_agent_meta: bool,
    #[serde(default)]
    pub has_done_marker: bool,
    #[serde(default = "default_true")]
    pub appears_as_agent: bool,
    #[serde(default = "default_true")]
    pub live: bool,
    #[serde(default)]
    pub pending_question: bool,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub run_started_at: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default, rename = "agent_family", alias = "agent_session")]
    pub agent_session: Option<String>,
    #[serde(
        default,
        rename = "agent_family_role",
        alias = "agent_session_role"
    )]
    pub agent_session_role: Option<String>,
    // legacy agent-family spelling; flips in core-contract
    #[serde(default, rename = "agent_family_parallel")]
    pub agent_session_parallel: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_claim_owner_key: Option<String>,
    #[serde(
        default,
        rename = "family_shell_kind",
        alias = "agent_session_shell_kind"
    )]
    pub agent_session_shell_kind: Option<String>,
    #[serde(
        default,
        rename = "family_shell_id",
        alias = "agent_session_shell_id"
    )]
    pub agent_session_shell_id: Option<String>,
    #[serde(
        default,
        rename = "family_shell_state",
        alias = "agent_session_shell_state"
    )]
    pub agent_session_shell_state: Option<String>,
    #[serde(default)]
    pub queue_weight: Option<f64>,
    #[serde(default)]
    pub queue_weight_explicit: bool,
    #[serde(default)]
    pub queue_weight_invalid: bool,
    #[serde(default)]
    pub slot_requested_at: Option<String>,
    #[serde(default)]
    pub queue_capacity: Option<i64>,
    #[serde(default)]
    pub queue_capacity_explicit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default, skip_serializing)]
    pub(super) wait_runners: Option<i64>,
    #[serde(default, skip_serializing)]
    pub(super) wait_runners_explicit: bool,
    #[serde(default)]
    pub wait_priority: Option<i64>,
    #[serde(default)]
    pub eligible_since: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityDiagnosticWire {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityClaimWire {
    pub owner_key: String,
    pub project_name: String,
    pub claim_kind: String,
    pub lineage_key: String,
    pub occupied_lanes: u32,
    pub occupied_capacity: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_artifact_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_timestamp: Option<String>,
    #[serde(default)]
    pub artifact_dirs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityBlockerWire {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needed_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occupied_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_threshold: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_limit: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_expires_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityWaiterWire {
    pub artifact_dir: String,
    pub queue_position: u32,
    pub priority: i32,
    pub slot_requested_at: String,
    pub timestamp: String,
    pub requested_weight: f64,
    #[serde(
        default,
        alias = "wait_runners",
        skip_serializing_if = "Option::is_none"
    )]
    pub queue_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity_multiplier: Option<f64>,
    pub admission_limit: f64,
    pub eligible: bool,
    #[serde(default)]
    pub parked: bool,
    #[serde(default)]
    pub capacity_shortfall: f64,
    #[serde(default)]
    pub wait_capacity_shortfall: f64,
    #[serde(default)]
    pub blockers: Vec<RunnerCapacityBlockerWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacityCandidateDecisionWire {
    pub artifact_dir: String,
    pub decision: String,
    pub owner_key: String,
    pub claim_kind: String,
    pub lineage_key: String,
    pub requested_weight: f64,
    pub effective_weight: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_weight: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_claim_weight: Option<f64>,
    pub explicit_weight_compatibility: String,
    pub eligible: bool,
    #[serde(default)]
    pub blockers: Vec<RunnerCapacityBlockerWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerCapacitySnapshotWire {
    pub schema_version: u32,
    pub effective_limit: f64,
    pub occupied_lanes: u32,
    pub occupied_capacity: f64,
    #[serde(default)]
    pub claims: Vec<RunnerCapacityClaimWire>,
    #[serde(default)]
    pub waiters: Vec<RunnerCapacityWaiterWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_eligible_artifact_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_decision: Option<RunnerCapacityCandidateDecisionWire>,
    #[serde(default)]
    pub diagnostics: Vec<RunnerCapacityDiagnosticWire>,
}

pub fn runner_capacity_policy_schema_version() -> u32 {
    RUNNER_CAPACITY_POLICY_SCHEMA_VERSION
}

impl RunnerCapacityRecordWire {
    pub(super) fn normalize_queue_capacity_aliases(&mut self) {
        let (capacity, explicit) = resolve_queue_capacity(
            self.queue_capacity,
            self.wait_runners.take(),
            self.queue_capacity_explicit,
            self.wait_runners_explicit,
        );
        self.queue_capacity = capacity;
        self.queue_capacity_explicit = explicit;
        self.wait_runners_explicit = false;
    }
}

pub(super) fn diagnostic(
    code: &str,
    message: &str,
    artifact_dir: Option<String>,
) -> RunnerCapacityDiagnosticWire {
    RunnerCapacityDiagnosticWire {
        code: code.to_string(),
        message: message.to_string(),
        artifact_dir,
    }
}

pub(super) fn blocker(
    code: &str,
    message: &str,
    needed_capacity: Option<f64>,
    free_capacity: Option<f64>,
    occupied_capacity: Option<f64>,
    capacity_threshold: Option<u32>,
    admission_limit: Option<f64>,
) -> RunnerCapacityBlockerWire {
    RunnerCapacityBlockerWire {
        code: code.to_string(),
        message: message.to_string(),
        needed_capacity,
        free_capacity,
        occupied_capacity,
        capacity_threshold,
        admission_limit,
        held_by: None,
        hold_expires_at: None,
    }
}

fn default_true() -> bool {
    true
}

fn default_workflow_dir() -> String {
    "ace-run".to_string()
}
