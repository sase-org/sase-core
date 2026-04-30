//! Wire records for the pure agent-cleanup planning contract.
//!
//! These types intentionally model the backend cleanup decision as
//! rectangular JSON: Python/TUI callers gather current agent rows, choose a
//! scope and mode, then Rust returns a plan with no side effects performed.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const AGENT_CLEANUP_WIRE_SCHEMA_VERSION: u32 = 1;

pub const CLEANUP_SCOPE_FOCUSED_PANEL: &str = "focused_panel";
pub const CLEANUP_SCOPE_ALL_PANELS: &str = "all_panels";
pub const CLEANUP_SCOPE_EXPLICIT_IDENTITIES: &str = "explicit_identities";
pub const CLEANUP_SCOPE_TAG: &str = "tag";
pub const CLEANUP_SCOPE_FOCUSED_GROUP: &str = "focused_group";
pub const CLEANUP_SCOPE_CUSTOM_SELECTION: &str = "custom_selection";

pub const CLEANUP_MODE_DISMISS_COMPLETED: &str = "dismiss_completed";
pub const CLEANUP_MODE_KILL_AND_DISMISS: &str = "kill_and_dismiss";
pub const CLEANUP_MODE_PREVIEW_ONLY: &str = "preview_only";

pub const CONFIRMATION_SEVERITY_NONE: &str = "none";
pub const CONFIRMATION_SEVERITY_DISMISS: &str = "dismiss";
pub const CONFIRMATION_SEVERITY_DESTRUCTIVE: &str = "destructive";

pub const KILL_KIND_RUNNING: &str = "running";
pub const KILL_KIND_HOOK: &str = "hook";
pub const KILL_KIND_MENTOR: &str = "mentor";
pub const KILL_KIND_CRS: &str = "crs";
pub const KILL_KIND_WORKFLOW: &str = "workflow";

pub const SKIPPED_NOT_IN_SCOPE: &str = "not_in_scope";
pub const SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY: &str =
    "workflow_child_cascade_only";
pub const SKIPPED_NOT_DISMISSABLE: &str = "not_dismissable";
pub const SKIPPED_NOT_KILLABLE: &str = "not_killable";
pub const SKIPPED_UNKNOWN_KILL_KIND: &str = "unknown_kill_kind";
pub const SKIPPED_DUPLICATE: &str = "duplicate";

/// Stable agent identity used by the Python TUI:
/// `(agent_type, cl_name, raw_suffix)`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct AgentCleanupIdentityWire {
    pub agent_type: String,
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
}

/// One host-provided agent row. The planner reads only this wire shape and
/// does no filesystem/process inspection of its own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCleanupTargetWire {
    pub identity: AgentCleanupIdentityWire,
    pub agent_type: String,
    pub status: String,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub parent_workflow: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub raw_suffix: Option<String>,
    #[serde(default)]
    pub project_file: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub from_changespec: bool,
    #[serde(default)]
    pub workspace: Option<i64>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub stop_time: Option<String>,
    #[serde(default)]
    pub is_workflow_child: bool,
    #[serde(default)]
    pub appears_as_agent: bool,
    #[serde(default)]
    pub step_type: Option<String>,
}

/// Scope and mode requested by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupRequestWire {
    pub schema_version: u32,
    pub scope: String,
    pub mode: String,
    #[serde(default)]
    pub focused_panel_tag: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub identities: Vec<AgentCleanupIdentityWire>,
    #[serde(default)]
    pub include_pidless_as_dismissable: bool,
    #[serde(default)]
    pub taken_dismissed_names: Vec<String>,
}

/// One process/workflow kill decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupKillItemWire {
    pub identity: AgentCleanupIdentityWire,
    pub kind: String,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub display_name: Option<String>,
}

/// One dismissal decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupDismissItemWire {
    pub identity: AgentCleanupIdentityWire,
    #[serde(default)]
    pub display_name: Option<String>,
}

/// One target the planner did not include in the kill/dismiss sets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupSkippedItemWire {
    pub identity: AgentCleanupIdentityWire,
    pub reason: String,
    #[serde(default)]
    pub detail: Option<String>,
}

/// Count summary for the host's preview UI.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupCountsWire {
    pub candidates: u64,
    pub selected: u64,
    pub kill: u64,
    pub dismiss: u64,
    pub cascaded_workflow_children: u64,
    pub skipped: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
}

/// Rename allocation for dismissal-time name prefixing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupDismissalRenameIntentWire {
    pub identity: AgentCleanupIdentityWire,
    #[serde(default)]
    pub old_name: Option<String>,
    pub new_name: String,
}

/// Candidate whose agent bundle should be saved by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupBundleSaveIntentWire {
    pub identity: AgentCleanupIdentityWire,
}

/// Artifact path whose loader marker files should be removed by the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupArtifactDeleteIntentWire {
    pub identity: AgentCleanupIdentityWire,
    pub artifacts_dir: String,
}

/// Workspace release request. Python still performs any project-file writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupWorkspaceReleaseIntentWire {
    pub identity: AgentCleanupIdentityWire,
    pub project_file: String,
    #[serde(default)]
    pub workspace: Option<i64>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub cl_name: Option<String>,
    #[serde(default)]
    pub lookup_workflow: bool,
}

/// Notification lookup key to dismiss after cleanup succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupNotificationDismissIntentWire {
    pub identity: AgentCleanupIdentityWire,
    pub cl_name: String,
    #[serde(default)]
    pub raw_suffix: Option<String>,
}

/// Deterministic side-effect plan. The host consumes these intents and keeps
/// ownership of all process signalling, filesystem, and project-file writes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupSideEffectsWire {
    #[serde(default)]
    pub dismissed_index_additions: Vec<AgentCleanupIdentityWire>,
    #[serde(default)]
    pub bundle_save_candidates: Vec<AgentCleanupBundleSaveIntentWire>,
    #[serde(default)]
    pub artifact_delete_paths: Vec<AgentCleanupArtifactDeleteIntentWire>,
    #[serde(default)]
    pub workspace_release_requests: Vec<AgentCleanupWorkspaceReleaseIntentWire>,
    #[serde(default)]
    pub notification_dismiss_candidates:
        Vec<AgentCleanupNotificationDismissIntentWire>,
    #[serde(default)]
    pub dismissal_rename_allocations:
        Vec<AgentCleanupDismissalRenameIntentWire>,
    #[serde(default)]
    pub wait_reference_rewrite_map: Vec<(String, String)>,
}

/// Complete pure plan. The host may use it for preview only or execute its
/// side effects in Python.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCleanupPlanWire {
    pub schema_version: u32,
    #[serde(default)]
    pub selected_identities: Vec<AgentCleanupIdentityWire>,
    #[serde(default)]
    pub kill_items: Vec<AgentCleanupKillItemWire>,
    #[serde(default)]
    pub dismiss_items: Vec<AgentCleanupDismissItemWire>,
    #[serde(default)]
    pub cascaded_workflow_children: Vec<AgentCleanupIdentityWire>,
    #[serde(default)]
    pub skipped_items: Vec<AgentCleanupSkippedItemWire>,
    pub counts: AgentCleanupCountsWire,
    pub confirmation_severity: String,
    #[serde(default)]
    pub summary_lines: Vec<String>,
    #[serde(default)]
    pub side_effects: AgentCleanupSideEffectsWire,
}

pub fn cleanup_request_from_json_value(
    value: &JsonValue,
) -> Result<AgentCleanupRequestWire, String> {
    let schema = value
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            "agent cleanup wire missing or non-integer schema_version"
                .to_string()
        })?;
    if schema != AGENT_CLEANUP_WIRE_SCHEMA_VERSION as u64 {
        return Err(format!(
            "agent cleanup wire schema mismatch: got {schema}, expected {}",
            AGENT_CLEANUP_WIRE_SCHEMA_VERSION
        ));
    }
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}

pub fn cleanup_plan_from_json_value(
    value: &JsonValue,
) -> Result<AgentCleanupPlanWire, String> {
    let schema = value
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            "agent cleanup wire missing or non-integer schema_version"
                .to_string()
        })?;
    if schema != AGENT_CLEANUP_WIRE_SCHEMA_VERSION as u64 {
        return Err(format!(
            "agent cleanup wire schema mismatch: got {schema}, expected {}",
            AGENT_CLEANUP_WIRE_SCHEMA_VERSION
        ));
    }
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}
