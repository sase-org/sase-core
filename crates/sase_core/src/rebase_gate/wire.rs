//! Versioned request and response types for the rebase gate policy.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

pub const REBASE_GATE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const REBASE_GATE_DEFAULT_TIMEOUT_SECONDS: f64 = 10.0;
pub const REBASE_GATE_MAX_TIMEOUT_SECONDS: f64 = 30.0;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("invalid rebase gate input: {0}")]
pub struct RebaseGateError(pub String);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RebaseGateChangeKindWire {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
    TypeChanged,
    Unmerged,
}

/// One path change. `path` is the destination for a rename or copy; `old_path`
/// retains its source. Deletions remain explicit so consumers do not mistake
/// a removed path for a present file.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateChangedPathWire {
    pub path: String,
    pub kind: RebaseGateChangeKindWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_path: Option<String>,
}

/// Immutable object IDs captured around one rebase attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateInputIdsWire {
    pub old_head: String,
    pub upstream_tip: String,
    pub merge_base: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebaseGateRebaseOutcomeWire {
    /// At least one commit was successfully replayed onto a divergent tip.
    Applied,
    /// The rebase stopped for manual conflict resolution; retain the context.
    Conflict,
    /// Git determined that no replay was needed.
    NoOp,
    /// The attempt failed for a reason that does not leave a resumable rebase.
    Failed,
}

/// Host-collected facts from one rebase attempt. The caller runs Git; Rust
/// decides eligibility and constructs the deterministic path union.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateFactsWire {
    pub schema_version: u32,
    pub repository_root: String,
    pub old_head: String,
    pub upstream_tip: String,
    pub merge_base: String,
    pub rebase_outcome: RebaseGateRebaseOutcomeWire,
    pub current_head: String,
    pub current_tree: String,
    #[serde(default)]
    pub local_changes: Vec<RebaseGateChangedPathWire>,
    #[serde(default)]
    pub upstream_changes: Vec<RebaseGateChangedPathWire>,
}

/// Pending context saved across conflict resolution and resume attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateContextWire {
    pub schema_version: u32,
    pub repository_root: String,
    pub inputs: Vec<RebaseGateInputIdsWire>,
    pub current_head: String,
    pub current_tree: String,
    pub changed_paths: Vec<RebaseGateChangedPathWire>,
}

/// New HEAD/tree and additional paths discovered after conflict repair or an
/// amend. Original input IDs remain immutable in the context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateContextUpdateWire {
    pub schema_version: u32,
    pub repository_root: String,
    pub current_head: String,
    pub current_tree: String,
    #[serde(default)]
    pub changed_paths: Vec<RebaseGateChangedPathWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateContextMergeRequestWire {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing: Option<RebaseGateContextWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incoming: Option<RebaseGateContextWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateContextUpdateRequestWire {
    pub schema_version: u32,
    pub context: RebaseGateContextWire,
    pub update: RebaseGateContextUpdateWire,
}

/// Settings relevant to the after-rebase command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateConfigWire {
    pub hook_command: String,
    pub timeout_seconds: f64,
}

impl Default for RebaseGateConfigWire {
    fn default() -> Self {
        Self {
            hook_command: String::new(),
            timeout_seconds: REBASE_GATE_DEFAULT_TIMEOUT_SECONDS,
        }
    }
}

/// Process facts collected by the bounded command runner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateCommandFactsWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebaseGateReportStatusWire {
    Clean,
    Findings,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateFindingWire {
    pub path: String,
    pub line: u32,
    pub module: String,
    pub imported_name: String,
    pub explanation: String,
}

/// Untrusted JSON emitted by the checker. It is parsed and validated by the
/// policy before it can block publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateReportWire {
    pub schema_version: u32,
    pub checked_head: String,
    pub checked_tree: String,
    pub status: RebaseGateReportStatusWire,
    #[serde(default)]
    pub findings: Vec<RebaseGateFindingWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebaseGateVerdictWire {
    Skip,
    Pass,
    Block,
    Warn,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateClassifyRequestWire {
    pub schema_version: u32,
    pub context: RebaseGateContextWire,
    pub config: RebaseGateConfigWire,
    pub command: RebaseGateCommandFactsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebaseGateDecisionWire {
    pub schema_version: u32,
    pub verdict: RebaseGateVerdictWire,
    pub reason: String,
    #[serde(default)]
    pub findings: Vec<RebaseGateFindingWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
