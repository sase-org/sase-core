//! Launch wire records, error types, and timestamp-batch allocation.
//!
//! These are the stable `agent_launch::*` shapes shared by the typed
//! planner, the fanout planner, admission, and the workspace-claim helpers.
use crate::fenced_code::CodeValueWire;
use crate::hold_directive::HoldFieldsWire;
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub const AGENT_LAUNCH_WIRE_SCHEMA_VERSION: u32 = 1;
pub const LAUNCH_PLAN_WIRE_SCHEMA_VERSION: u32 = 1;
pub const BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimWire {
    pub workspace_num: u32,
    pub workflow: String,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub pid: u32,
    #[serde(default)]
    pub artifacts_timestamp: Option<String>,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimRequestWire {
    pub project_file: String,
    pub workspace_num: u32,
    pub workflow_name: String,
    pub pid: u32,
    #[serde(default)]
    pub cl_name: String,
    #[serde(default)]
    pub artifacts_timestamp: String,
    #[serde(default)]
    pub transfer_from_pid: Option<u32>,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimOutcomeWire {
    pub success: bool,
    pub workspace_num: u32,
    pub project_file: String,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimPlanWire {
    pub content: String,
    pub outcome: WorkspaceClaimOutcomeWire,
    pub changed: bool,
}

/// The per-checkout occupant marker written to
/// `<checkout>/.sase/occupant.json` when an agent takes a workspace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OccupantRecordWire {
    pub pid: u32,
    #[serde(default)]
    pub artifacts_timestamp: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    pub workflow: String,
    pub project: String,
    pub workspace_num: u32,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub claimed_at: f64,
}

/// The identity of the process asking whether it may destructively prepare
/// a checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccupancyCallerWire {
    pub pid: u32,
    pub workspace_num: u32,
    pub project: String,
    pub workflow: String,
    #[serde(default)]
    pub artifacts_timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccupancyConflictDecisionWire {
    pub may_proceed: bool,
    pub conflict: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLaunchRequestWire {
    pub schema_version: u32,
    pub cl_name: String,
    pub project_file: String,
    pub workspace_dir: String,
    pub workspace_num: u32,
    pub workflow_name: String,
    pub prompt: String,
    pub timestamp: String,
    #[serde(default)]
    pub update_target: String,
    #[serde(default)]
    pub project_name: String,
    #[serde(default)]
    pub history_sort_key: String,
    #[serde(default)]
    pub is_home_mode: bool,
    #[serde(default)]
    pub vcs_workflow_type: Option<String>,
    #[serde(default)]
    pub vcs_ref: Option<String>,
    #[serde(default)]
    pub deferred_workspace: bool,
    #[serde(default)]
    pub local_xprompts_file: Option<String>,
    #[serde(default)]
    pub extra_env: BTreeMap<String, String>,
    #[serde(default)]
    pub retry_transfer_from_pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLaunchPreparedWire {
    pub schema_version: u32,
    pub prompt_file: String,
    pub output_path: String,
    pub safe_name: String,
    #[serde(default)]
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env_delta: BTreeMap<String, String>,
    #[serde(default)]
    pub claim_request: Option<WorkspaceClaimRequestWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchFanoutSlotWire {
    pub prompt: String,
    pub launch_kind: String,
    pub slot_index: u32,
    #[serde(default)]
    pub alt_id: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub repeat_name: Option<String>,
    #[serde(default)]
    pub bead_id: Option<String>,
    #[serde(default)]
    pub wait_for_previous: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchFanoutPlanWire {
    pub schema_version: u32,
    pub launch_kind: String,
    #[serde(default)]
    pub slots: Vec<LaunchFanoutSlotWire>,
    #[serde(default)]
    pub requires_sequential_naming_wait: bool,
    #[serde(default)]
    pub fanout_sleep_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPredecessorContextWire {
    pub schema_version: u32,
    pub project_name: String,
    pub timestamp: String,
    pub artifact_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPredecessorWaitBindingWire {
    pub schema_version: u32,
    pub prompt: String,
    #[serde(default)]
    pub wait_names: Vec<String>,
    #[serde(default)]
    pub wait_for_artifacts: Vec<BatchPredecessorContextWire>,
    pub bound_wait_count: u32,
}

/// Pure, schema-versioned launch graph. It is produced before approval and
/// contains only logical launch units, typed waits, code digests/previews, and
/// resource intent. Runtime identities remain layered on top by later phases.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchPlanWire {
    pub schema_version: u32,
    pub launch_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_project: Option<String>,
    #[serde(default)]
    pub units: Vec<LaunchUnitWire>,
    #[serde(default)]
    pub approval_preview: Vec<String>,
    pub content_digest: String,
    #[serde(default)]
    pub diagnostics: Vec<LaunchPlanDiagnosticWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchUnitWire {
    pub logical_id: String,
    pub source_order: u32,
    #[serde(default)]
    pub waits: Vec<WaitTargetWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<LaunchConditionWire>,
    pub payload: LaunchUnitPayloadWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LaunchUnitPayloadWire {
    Agent(AgentUnitWire),
    Proc(ProcUnitWire),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AgentUnitWire {
    pub prompt: String,
    /// Positional `%id` member id, or the explicit full name for a plain
    /// `%id:<name>` / clan-declarer form. Joiners keep the member id here and
    /// put the clan on [`Self::clan`] so dispatch can rebuild
    /// `%id(<member>, clan=<clan>)` without treating the member as the
    /// complete agent name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default)]
    pub identity_explicit: bool,
    #[serde(default, skip_serializing_if = "skip_if_false")]
    pub identity_force_reuse: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan: Option<String>,
    #[serde(default, skip_serializing_if = "skip_if_false")]
    pub clan_declared: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan_tribe: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan_summary_script: Option<String>,
    #[serde(
        default,
        rename = "family_attach_parent",
        alias = "agent_session_attach_parent",
        skip_serializing_if = "Option::is_none"
    )]
    pub agent_session_attach_parent: Option<String>,
    #[serde(
        default,
        rename = "family_attach_suffix",
        alias = "agent_session_attach_suffix",
        skip_serializing_if = "Option::is_none"
    )]
    pub agent_session_attach_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tribe: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_id: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub auto_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_mode: Option<String>,
    #[serde(default)]
    pub finalizers: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default, skip_serializing)]
    pub(crate) wait_runners: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_weight: Option<f64>,
    #[serde(default, skip_serializing_if = "skip_if_false")]
    pub queue_weight_explicit: bool,
    /// Workspace provider for this unit's VCS tag (`gh` or `git`).
    /// Distinct from the plan-wide [`LaunchPlanWire::selected_project`]: a
    /// plan-level project never replaces a per-unit `#gh:` / `#git:` ref.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_provider: Option<String>,
    /// Exact per-unit workspace tag, for example `#gh:sase` or `#git:dotfiles`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_reference: Option<String>,
    /// Remote machine alias from `%dispatch:<alias>`. Local units leave this
    /// unset so admission never treats leftover prompt text as routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold: Option<HoldFieldsWire>,
}

fn skip_if_false(value: &bool) -> bool {
    !*value
}

impl AgentUnitWire {
    pub fn authored_queue_capacity(&self) -> Option<u32> {
        self.queue_capacity.or(self.wait_runners)
    }

    pub fn normalize_queue_capacity_aliases(&mut self) {
        if self.queue_capacity.is_none() {
            self.queue_capacity = self.wait_runners;
        }
        self.wait_runners = None;
    }

    /// Return the launch identity used for waits and collision checks.
    ///
    /// Clan joiners compose `<clan>.<member>`; agent-session attachments
    /// compose `<parent>--<suffix>` when the suffix is already concrete.
    /// Auto-named units, including `%id(@, family=...)`, have no durable
    /// name yet.
    pub fn effective_identity(&self) -> Option<String> {
        if let (Some(parent), Some(suffix)) = (
            self.agent_session_attach_parent.as_deref(),
            self.agent_session_attach_suffix.as_deref(),
        ) {
            if suffix == "@" {
                return None;
            }
            return Some(format!("{parent}--{suffix}"));
        }
        if !self.clan_declared {
            if let (Some(clan), Some(member)) =
                (self.clan.as_deref(), self.identity.as_deref())
            {
                return Some(format!("{clan}.{member}"));
            }
        }
        self.identity.clone()
    }

    pub(crate) fn identity_directive_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(line) = self.format_id_directive() {
            lines.push(line);
        }
        if let Some(line) = self.format_clan_directive() {
            lines.push(line);
        }
        lines
    }

    fn format_id_directive(&self) -> Option<String> {
        let bead = self.bead_id.as_deref();
        let bang = |value: &str| {
            if self.identity_force_reuse {
                format!("!{value}")
            } else {
                value.to_string()
            }
        };
        let bead_suffix = |prefix_comma: bool| match bead {
            Some(bead_id) if prefix_comma => format!(", bead={bead_id}"),
            Some(bead_id) => format!("bead={bead_id}"),
            None => String::new(),
        };
        if let (Some(parent), Some(suffix)) = (
            self.agent_session_attach_parent.as_deref(),
            self.agent_session_attach_suffix.as_deref(),
        ) {
            // legacy agent-family spelling; flips in core-contract
            return Some(format!(
                "%id({}, family={parent}{})",
                bang(suffix),
                bead_suffix(true)
            ));
        }
        if let (Some(clan), Some(member)) =
            (self.clan.as_deref(), self.identity.as_deref())
        {
            if !self.clan_declared {
                return Some(format!(
                    "%id({}, clan={clan}{})",
                    bang(member),
                    bead_suffix(true)
                ));
            }
        }
        if let Some(tribe) = self.tribe.as_deref() {
            return Some(match self.identity.as_deref() {
                Some(identity) => format!(
                    "%id({}, tribe={tribe}{})",
                    bang(identity),
                    bead_suffix(true)
                ),
                None if bead.is_some() => {
                    format!("%id(tribe={tribe}{})", bead_suffix(true))
                }
                None => format!("%id(tribe={tribe})"),
            });
        }
        if self.identity_explicit {
            let identity = self.identity.as_deref()?;
            if bead.is_some() || self.identity_force_reuse {
                return Some(format!(
                    "%id({}{})",
                    bang(identity),
                    bead_suffix(true)
                ));
            }
            return Some(format!("%id:{identity}"));
        }
        bead.map(|bead_id| format!("%id(bead={bead_id})"))
    }

    fn format_clan_directive(&self) -> Option<String> {
        if !self.clan_declared {
            return None;
        }
        let clan = self.clan.as_deref()?;
        let mut args = Vec::new();
        if let Some(tribe) = self.clan_tribe.as_deref() {
            args.push(format!("tribe={tribe}"));
        }
        if let Some(summary) = self.clan_summary.as_deref() {
            args.push(format!("summary=[[{summary}]]"));
        }
        if let Some(script) = self.clan_summary_script.as_deref() {
            args.push(format!("summary_script={script}"));
        }
        if args.is_empty() {
            Some(format!("%clan:{clan}"))
        } else {
            Some(format!("%clan({clan}, {})", args.join(", ")))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcUnitWire {
    pub code: CodeValueWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    pub workspace: bool,
    #[serde(default)]
    pub workspace_explicit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_weight: Option<f64>,
    #[serde(default, skip_serializing_if = "skip_if_false")]
    pub queue_weight_explicit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold: Option<HoldFieldsWire>,
}

impl ProcUnitWire {
    pub fn has_authored_queue_fields(&self) -> bool {
        self.queue_capacity.is_some()
            || self.queue_capacity_multiplier.is_some()
            || self.wait_priority.is_some()
            || self.queue_weight.is_some()
            || self.queue_weight_explicit
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchConditionWire {
    pub code: CodeValueWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default)]
    pub context_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitTargetWire {
    Logical {
        logical_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },
    Agent {
        name: String,
    },
    Proc {
        identifier: String,
    },
    Bead {
        bead_id: String,
    },
    Time {
        value: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchOutcomeWire {
    Eligible,
    Launched,
    Skipped,
    ConditionError,
    LaunchError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchUnitResultWire {
    pub logical_id: String,
    pub outcome: LaunchOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_key: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertain: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchPlanDiagnosticWire {
    pub code: String,
    pub severity: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_span: Option<[usize; 2]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_id: Option<String>,
}

#[derive(Debug)]
pub enum AgentLaunchPreparationError {
    SchemaVersion { expected: u32, actual: u32 },
    CreateTempFile(std::io::Error),
    WritePrompt(std::io::Error),
    KeepTempFile(std::io::Error),
    CreateOutputRoot(std::io::Error),
}

impl fmt::Display for AgentLaunchPreparationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaVersion { expected, actual } => write!(
                f,
                "unsupported AgentLaunchRequestWire schema_version {actual}; expected {expected}"
            ),
            Self::CreateTempFile(err) => {
                write!(f, "failed to create prompt temp file: {err}")
            }
            Self::WritePrompt(err) => {
                write!(f, "failed to write prompt temp file: {err}")
            }
            Self::KeepTempFile(err) => {
                write!(f, "failed to keep prompt temp file: {err}")
            }
            Self::CreateOutputRoot(err) => {
                write!(f, "failed to create launch output root: {err}")
            }
        }
    }
}

impl std::error::Error for AgentLaunchPreparationError {}

#[derive(Debug)]
pub enum TimestampBatchAllocationError {
    InvalidTimestamp {
        field: &'static str,
        value: String,
        error: chrono::ParseError,
    },
}

impl fmt::Display for TimestampBatchAllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimestamp {
                field,
                value,
                error,
            } => write!(
                f,
                "invalid {field} launch timestamp {value:?}; expected YYmmdd_HHMMSS: {error}"
            ),
        }
    }
}

impl std::error::Error for TimestampBatchAllocationError {}

#[derive(Debug)]
pub enum AgentLaunchFanoutPlanError {
    UnsupportedKind(String),
    MultiModelUnsupported(String),
    UnclosedDirective {
        name: String,
        close: char,
    },
    InvalidBatchPredecessorContext(String),
    TypedLaunchPlan {
        diagnostics: Vec<LaunchPlanDiagnosticWire>,
    },
}

impl fmt::Display for AgentLaunchFanoutPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedKind(kind) => {
                write!(f, "unsupported launch fan-out kind {kind:?}")
            }
            Self::MultiModelUnsupported(message) => write!(f, "{message}"),
            Self::UnclosedDirective { name, close } => {
                write!(
                    f,
                    "unclosed {name} directive: missing closing '{close}'"
                )
            }
            Self::InvalidBatchPredecessorContext(message) => {
                write!(f, "invalid batch predecessor context: {message}")
            }
            Self::TypedLaunchPlan { diagnostics } => {
                if let Some(first) = diagnostics.first() {
                    write!(f, "{}", first.message)
                } else {
                    write!(f, "typed launch plan validation failed")
                }
            }
        }
    }
}

impl std::error::Error for AgentLaunchFanoutPlanError {}

pub fn allocate_launch_timestamp_batch(
    count: usize,
    base_timestamp: &str,
    after_timestamp: Option<&str>,
) -> Result<Vec<String>, TimestampBatchAllocationError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let base = parse_launch_timestamp("base_timestamp", base_timestamp)?;
    let start = match after_timestamp {
        Some(after) if !after.is_empty() => {
            let after = parse_launch_timestamp("after_timestamp", after)?;
            std::cmp::max(base, after + Duration::seconds(1))
        }
        _ => base,
    };

    Ok((0..count)
        .map(|offset| {
            (start + Duration::seconds(offset as i64))
                .format("%y%m%d_%H%M%S")
                .to_string()
        })
        .collect())
}

pub(crate) fn parse_launch_timestamp(
    field: &'static str,
    value: &str,
) -> Result<NaiveDateTime, TimestampBatchAllocationError> {
    NaiveDateTime::parse_from_str(value, "%y%m%d_%H%M%S").map_err(|error| {
        TimestampBatchAllocationError::InvalidTimestamp {
            field,
            value: value.to_string(),
            error,
        }
    })
}
