//! Wire records and deterministic helpers for agent launch.

mod admission;
mod condition;
mod proc_runtime;

pub use admission::{
    admission_unit_results, agent_unit_dispatch_prompt, dispatch_fingerprint,
    next_admission_actions, reconcile_admission_journal, summarize_admission,
    wait_target_key, LaunchAdmissionActionWire,
    LaunchAdmissionJournalEntryWire, LaunchAdmissionSummaryWire,
    LaunchAdmissionUnitStateWire, LaunchAdmissionWaitFactWire,
    LaunchUnitPhaseWire, WaitedOutcomeWire,
    LAUNCH_ADMISSION_JOURNAL_SCHEMA_VERSION,
};
pub use condition::{
    build_condition_context, classify_condition_status, condition_command_argv,
    condition_context_digest, evaluate_launch_condition, sanitize_safe_inputs,
    sanitized_condition_env, ConditionCheckWire, ConditionContextWire,
    ConditionEvalRequestWire, ConditionEvalResultWire,
    ConditionLogicalUnitWire, ConditionWaitedOutcomeWire,
    CONDITION_CONTEXT_SCHEMA_VERSION, CONDITION_DEFAULT_TIMEOUT_SECONDS,
    CONDITION_EVAL_WIRE_SCHEMA_VERSION, CONDITION_MAX_TIMEOUT_SECONDS,
    CONDITION_OUTPUT_CAP_BYTES,
};
pub use proc_runtime::{
    cleanup_proc_private_inputs, parse_proc_duration_seconds,
    prepare_proc_script, proc_script_argv, resolve_proc_execution_cwd,
    sanitized_proc_env, validate_proc_workspace_intent,
    validate_standalone_proc_shell_name, ProcDispatchPreparedWire,
    ProcDispatchRequestWire, PROC_DISPATCH_WIRE_SCHEMA_VERSION,
    PROC_PHASE_ACQUIRING_WORKSPACE, PROC_PHASE_CHECKING,
    PROC_PHASE_PREPARING_SCRIPT, PROC_PHASE_RUNNING, PROC_PHASE_SETTLING,
    PROC_PHASE_WAITING, XPROMPT_PROC_ORIGIN,
};

use crate::effort::split_model_effort;
use crate::fenced_code::{
    fenced_block_ranges, language_from_info_string,
    scan_directive_owned_fences, CodeLanguage, CodeValue, CodeValueWire,
};
use crate::prompt_literals::inline_code_ranges;
use crate::xprompt_text_block::find_text_block_close_for_args;
use chrono::{Duration, NaiveDateTime};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

pub const AGENT_LAUNCH_WIRE_SCHEMA_VERSION: u32 = 1;
pub const LAUNCH_PLAN_WIRE_SCHEMA_VERSION: u32 = 1;
const EMPTY_ALT_SENTINEL: char = '\u{E000}';
const EMPTY_ALT_SENTINEL_STR: &str = "\u{E000}";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family_attach_parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family_attach_suffix: Option<String>,
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
    pub wait_runners: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_priority: Option<i32>,
}

fn skip_if_false(value: &bool) -> bool {
    !*value
}

impl AgentUnitWire {
    /// Return the launch identity used for waits and collision checks.
    ///
    /// Clan joiners compose `<clan>.<member>`; family attachments compose
    /// `<parent>--<suffix>` when the suffix is already concrete. Auto-named
    /// units, including `%id(@, family=...)`, have no durable name yet.
    pub fn effective_identity(&self) -> Option<String> {
        if let (Some(parent), Some(suffix)) = (
            self.family_attach_parent.as_deref(),
            self.family_attach_suffix.as_deref(),
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
            self.family_attach_parent.as_deref(),
            self.family_attach_suffix.as_deref(),
        ) {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchUnitResultWire {
    pub logical_id: String,
    pub outcome: LaunchOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
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

fn parse_launch_timestamp(
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

#[derive(Debug, Clone)]
struct DirectiveOccurrence {
    canonical_name: String,
    start: usize,
    end: usize,
    args: Vec<String>,
    has_plus_suffix: bool,
    // True when a single colon argument came from a backtick literal
    // (`` %model:`literal@id` ``). Such values bypass the `@effort` split so any
    // `@` in the model id is preserved, mirroring the Python parser.
    from_backtick_literal: bool,
}

#[derive(Debug, Clone)]
struct XPromptOccurrence {
    name: String,
    start: usize,
    end: usize,
    has_time_argument: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectiveArg {
    name: Option<String>,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeBranch {
    value: String,
    id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeDirective {
    start: usize,
    end: usize,
    args: Vec<DirectiveArg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeAxis {
    start: usize,
    variants: Vec<AlternativeVariant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeVariant {
    id: String,
    replacements: Vec<AlternativeReplacement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeReplacement {
    directive_index: usize,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeSlot {
    prompt: String,
    alt_id: String,
}

/// Which surface form opened an alternative directive. The legacy `%alt(...)`
/// and `%(...)` shorthand use parens with comma-separated branches; the new
/// `%{...}` shorthand uses braces with top-level `|`-separated branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AltDelimiter {
    Paren,
    Brace,
}

impl AltDelimiter {
    fn open(self) -> char {
        match self {
            Self::Paren => '(',
            Self::Brace => '{',
        }
    }

    fn close(self) -> char {
        match self {
            Self::Paren => ')',
            Self::Brace => '}',
        }
    }

    /// Branch separator inside the directive body.
    fn separator(self) -> char {
        match self {
            Self::Paren => ',',
            Self::Brace => '|',
        }
    }

    /// Human-readable directive name used in unclosed-directive errors.
    fn directive_label(self) -> &'static str {
        match self {
            Self::Paren => "%alt",
            Self::Brace => "%{",
        }
    }
}

pub fn plan_agent_launch_fanout(
    prompt: &str,
    launch_kind: Option<&str>,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let requested = launch_kind.unwrap_or("auto");
    match requested {
        "multi_prompt" => Ok(plan_multi_prompt_fanout(prompt)),
        "alternatives" => plan_alternative_fanout(prompt),
        "model" => plan_model_fanout(prompt),
        "repeat" => Ok(plan_repeat_fanout(prompt)),
        "auto" => {
            let multi = plan_multi_prompt_fanout(prompt);
            if multi.slots.len() > 1 {
                return Ok(multi);
            }
            let model = plan_model_fanout(prompt)?;
            if !model.slots.is_empty() {
                return Ok(model);
            }
            let repeat = plan_repeat_fanout(prompt);
            if !repeat.slots.is_empty() {
                return Ok(repeat);
            }
            Ok(LaunchFanoutPlanWire {
                schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
                launch_kind: "single".to_string(),
                slots: vec![LaunchFanoutSlotWire {
                    prompt: prompt.to_string(),
                    launch_kind: "single".to_string(),
                    slot_index: 0,
                    alt_id: None,
                    timestamp: None,
                    workflow_name: None,
                    model: None,
                    repeat_name: None,
                    bead_id: None,
                    wait_for_previous: has_wait_directive(prompt),
                }],
                requires_sequential_naming_wait: false,
                fanout_sleep_seconds: 0.0,
            })
        }
        other => Err(AgentLaunchFanoutPlanError::UnsupportedKind(
            other.to_string(),
        )),
    }
}

pub fn plan_typed_launch_units(
    prompt: &str,
    launch_kind: Option<&str>,
    selected_project: Option<&str>,
) -> Result<LaunchPlanWire, AgentLaunchFanoutPlanError> {
    let fanout = plan_agent_launch_fanout(prompt, launch_kind)?;
    let plan_project = selected_project
        .map(str::to_string)
        .or_else(|| project_context_from_prompt(prompt));
    let mut diagnostics = Vec::new();
    let mut raw_units = Vec::with_capacity(fanout.slots.len());

    for slot in &fanout.slots {
        raw_units.push(classify_typed_launch_unit(
            slot,
            plan_project.as_deref(),
            &mut diagnostics,
        ));
    }
    validate_typed_unit_identities(&raw_units, &mut diagnostics);
    resolve_typed_waits(&mut raw_units, &mut diagnostics);
    validate_typed_wait_cycles(&raw_units, &mut diagnostics);

    if !diagnostics.is_empty() {
        return Err(AgentLaunchFanoutPlanError::TypedLaunchPlan {
            diagnostics,
        });
    }

    let units: Vec<LaunchUnitWire> =
        raw_units.into_iter().map(|raw| raw.unit).collect();
    let approval_preview = render_launch_approval_preview(
        &fanout.launch_kind,
        plan_project.as_deref(),
        &units,
    );
    let content_digest = launch_plan_content_digest(
        &fanout.launch_kind,
        plan_project.as_deref(),
        &units,
    );
    Ok(LaunchPlanWire {
        schema_version: LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
        launch_kind: fanout.launch_kind,
        selected_project: plan_project,
        units,
        approval_preview,
        content_digest,
        diagnostics: Vec::new(),
    })
}

#[derive(Debug, Clone)]
struct RawLaunchUnit {
    unit: LaunchUnitWire,
    raw_waits: Vec<RawWaitTarget>,
}

#[derive(Debug, Clone)]
struct RawWaitTarget {
    target: RawWaitTargetKind,
    source: Option<String>,
    source_span: Option<[usize; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawWaitTargetKind {
    Previous,
    Unit(String),
    Agent(String),
    Proc(String),
    Bead(String),
    Time(String),
}

#[derive(Debug, Clone)]
struct ParsedProcDirective {
    code: Option<CodeValueWire>,
    options: BTreeMap<String, String>,
}

fn classify_typed_launch_unit(
    slot: &LaunchFanoutSlotWire,
    selected_project: Option<&str>,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> RawLaunchUnit {
    let prompt = slot.prompt.as_str();
    let logical_id = format!("unit-{}", slot.slot_index + 1);
    let mut regions_to_remove = project_ref_ranges(prompt);
    let mut condition: Option<LaunchConditionWire> = None;
    let mut proc_code: Option<CodeValueWire> = None;
    let mut proc_options: BTreeMap<String, String> = BTreeMap::new();
    let mut raw_waits = Vec::new();
    let mut agent_identity = slot.repeat_name.clone();
    let mut agent_identity_explicit = agent_identity.is_some();
    let mut agent_identity_force_reuse = false;
    let mut agent_clan: Option<String> = None;
    let mut agent_clan_declared = false;
    let mut agent_clan_tribe: Option<String> = None;
    let mut agent_clan_summary: Option<String> = None;
    let mut agent_clan_summary_script: Option<String> = None;
    let mut agent_family_parent: Option<String> = None;
    let mut agent_family_suffix: Option<String> = None;
    let mut agent_tribe: Option<String> = None;
    let mut parsed_id: Option<ParsedIdDirective> = None;
    let mut parsed_clan: Option<ParsedClanDirective> = None;
    let mut agent_model = slot.model.clone();
    let mut agent_effort: Option<String> = None;
    let mut agent_bead_id = slot.bead_id.clone();
    let mut agent_hidden = false;
    let mut auto_enabled = false;
    let mut auto_mode: Option<String> = None;
    let mut finalizers = Vec::new();
    let mut wait_runners: Option<u32> = None;
    let mut wait_priority: Option<i32> = None;
    let mut proc_forbidden_directives = Vec::new();

    let scan = scan_directive_owned_fences(prompt);
    let owned_spans: Vec<(usize, usize)> = scan
        .directives
        .iter()
        .map(|directive| (directive.span[0], directive.span[1]))
        .collect();
    for diagnostic in scan.diagnostics {
        diagnostics.push(LaunchPlanDiagnosticWire {
            code: diagnostic.code,
            severity: "error".to_string(),
            message: diagnostic.message,
            source_span: Some(diagnostic.span),
            logical_id: Some(logical_id.clone()),
        });
    }
    for directive in scan.directives {
        regions_to_remove.push((directive.span[0], directive.span[1]));
        match directive.name.as_str() {
            "if" => {
                if condition.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-condition",
                        "Only one %if is allowed per launch unit.",
                        &logical_id,
                        Some(directive.span),
                    ));
                    continue;
                }
                if let Some(code) = directive.code {
                    condition = Some(LaunchConditionWire {
                        code,
                        cwd: None,
                        context_fields: vec![
                            "logical_unit".to_string(),
                            "selected_project".to_string(),
                            "safe_inputs".to_string(),
                            "waited_outcomes".to_string(),
                        ],
                    });
                }
            }
            "proc" => {
                if proc_code.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-proc",
                        "Only one %proc is allowed per launch unit.",
                        &logical_id,
                        Some(directive.span),
                    ));
                    continue;
                }
                proc_code = directive.code;
            }
            _ => {}
        }
    }

    let ignored_ranges = typed_directive_ignored_ranges(prompt);
    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        // `%if::` is owned by the fence scanner. Re-parsing it as a bare
        // `%if` would emit a false invalid-if-form diagnostic after the
        // condition was already captured. `%proc(...)::` still needs this
        // loop so parenthesized options survive.
        if directive.canonical_name == "if"
            && position_in_ranges(directive.start, &owned_spans)
        {
            continue;
        }
        let span = [directive.start, directive.end];
        match directive.canonical_name.as_str() {
            "proc" => {
                regions_to_remove.push((directive.start, directive.end));
                match parse_proc_directive(
                    prompt,
                    &directive,
                    proc_code.is_some(),
                ) {
                    Ok(parsed) => {
                        if let Some(code) = parsed.code {
                            if proc_code.is_some() {
                                diagnostics.push(typed_unit_diagnostic(
                                    "duplicate-proc-body",
                                    "%proc cannot combine a parenthesized body with a fenced body.",
                                    &logical_id,
                                    Some(span),
                                ));
                            } else {
                                proc_code = Some(code);
                            }
                        }
                        for (key, value) in parsed.options {
                            proc_options.insert(key, value);
                        }
                    }
                    Err(diagnostic) => {
                        diagnostics
                            .push(with_logical_id(diagnostic, &logical_id));
                    }
                }
            }
            "if" => {
                regions_to_remove.push((directive.start, directive.end));
                diagnostics.push(typed_unit_diagnostic(
                    "invalid-if-form",
                    "%if requires %if:: followed by exactly one closed bash or python fence.",
                    &logical_id,
                    Some(span),
                ));
            }
            "wait" => {
                regions_to_remove.push((directive.start, directive.end));
                parse_wait_directive(
                    prompt,
                    &directive,
                    &logical_id,
                    &mut raw_waits,
                    &mut wait_runners,
                    &mut wait_priority,
                    diagnostics,
                );
            }
            "id" => {
                regions_to_remove.push((directive.start, directive.end));
                if parsed_id.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-id",
                        "Duplicate directive '%id' in prompt; use %id(<id>, tribe=<tribe>) to assign a tribe to an explicitly named agent, and add bead=<bead> to that same directive when needed.",
                        &logical_id,
                        Some(span),
                    ));
                } else {
                    let parsed = parse_id_directive(
                        &directive,
                        &logical_id,
                        diagnostics,
                    );
                    if parsed.unsupported_on_proc {
                        proc_forbidden_directives.push(
                            "%id(..., clan=|family=|tribe=|bead=...)"
                                .to_string(),
                        );
                    }
                    parsed_id = Some(parsed);
                }
            }
            "model" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%model".to_string());
                if let Some(value) =
                    directive.args.first().filter(|arg| !arg.is_empty())
                {
                    let (model, effort) = split_model_effort(value);
                    agent_model = Some(model.to_string());
                    if let Some(effort) = effort {
                        agent_effort = Some(effort.to_string());
                    }
                }
            }
            "effort" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%effort".to_string());
                if let Some(value) =
                    directive.args.first().filter(|arg| !arg.is_empty())
                {
                    agent_effort = Some(value.clone());
                }
            }
            "auto" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%auto".to_string());
                auto_enabled = true;
                auto_mode = Some(
                    directive
                        .args
                        .first()
                        .filter(|arg| !arg.is_empty() && arg.as_str() != "true")
                        .cloned()
                        .unwrap_or_else(|| "plan".to_string()),
                );
            }
            "final" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%final".to_string());
                finalizers.extend(
                    directive
                        .args
                        .iter()
                        .filter(|arg| !arg.is_empty())
                        .cloned(),
                );
            }
            "clan" => {
                proc_forbidden_directives.push("%clan".to_string());
                if parsed_clan.is_some() {
                    regions_to_remove.push((directive.start, directive.end));
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-clan",
                        "Duplicate directive '%clan' in prompt.",
                        &logical_id,
                        Some(span),
                    ));
                } else {
                    let parsed = parse_clan_directive(
                        prompt,
                        &directive,
                        &logical_id,
                        &ignored_ranges,
                        diagnostics,
                    );
                    regions_to_remove
                        .push((directive.start, parsed.region_end));
                    parsed_clan = Some(parsed);
                }
            }
            "hide" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%hide".to_string());
                agent_hidden = true;
            }
            "repeat" => {
                regions_to_remove.push((directive.start, directive.end));
            }
            _ => {}
        }
    }

    apply_parsed_identity(
        parsed_id.as_ref(),
        parsed_clan.as_ref(),
        &logical_id,
        &mut agent_identity,
        &mut agent_identity_explicit,
        &mut agent_identity_force_reuse,
        &mut agent_clan,
        &mut agent_clan_declared,
        &mut agent_clan_tribe,
        &mut agent_clan_summary,
        &mut agent_clan_summary_script,
        &mut agent_family_parent,
        &mut agent_family_suffix,
        &mut agent_tribe,
        &mut agent_bead_id,
        diagnostics,
    );

    let cleaned_prompt = strip_prompt_regions(prompt, &regions_to_remove)
        .trim()
        .to_string();
    let unit_project = selected_project
        .map(str::to_string)
        .or_else(|| project_context_from_prompt(prompt));
    let payload = if let Some(code) = proc_code {
        if !cleaned_prompt.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "proc-residual-prompt",
                "%proc launch units cannot include residual prompt prose; put launch text in an agent unit.",
                &logical_id,
                None,
            ));
        }
        if !proc_forbidden_directives.is_empty() {
            proc_forbidden_directives.sort();
            proc_forbidden_directives.dedup();
            diagnostics.push(typed_unit_diagnostic(
                "agent-directive-on-proc",
                &format!(
                    "{} {} not valid on %proc launch units.",
                    proc_forbidden_directives.join(", "),
                    if proc_forbidden_directives.len() == 1 {
                        "is"
                    } else {
                        "are"
                    }
                ),
                &logical_id,
                None,
            ));
        }
        let shell_name = agent_identity.clone();
        validate_proc_shell_name(
            shell_name.as_deref(),
            &logical_id,
            diagnostics,
        );
        let workspace = parse_proc_workspace(
            proc_options.get("workspace").map(String::as_str),
            unit_project.as_deref(),
            &logical_id,
            diagnostics,
        );
        validate_proc_project_policy(
            unit_project.as_deref(),
            workspace,
            proc_options.get("cwd").map(String::as_str),
            &logical_id,
            diagnostics,
        );
        LaunchUnitPayloadWire::Proc(ProcUnitWire {
            code,
            shell_name,
            label: proc_options.get("label").cloned().filter(|v| !v.is_empty()),
            timeout: proc_options
                .get("timeout")
                .cloned()
                .filter(|v| !v.is_empty()),
            idle_timeout: proc_options
                .get("idle_timeout")
                .cloned()
                .filter(|v| !v.is_empty()),
            cwd: proc_options.get("cwd").cloned().filter(|v| !v.is_empty()),
            workspace,
            workspace_explicit: proc_options.contains_key("workspace"),
            selected_project: unit_project,
        })
    } else {
        LaunchUnitPayloadWire::Agent(AgentUnitWire {
            prompt: cleaned_prompt,
            identity: agent_identity,
            identity_explicit: agent_identity_explicit,
            identity_force_reuse: agent_identity_force_reuse,
            clan: agent_clan,
            clan_declared: agent_clan_declared,
            clan_tribe: agent_clan_tribe,
            clan_summary: agent_clan_summary,
            clan_summary_script: agent_clan_summary_script,
            family_attach_parent: agent_family_parent,
            family_attach_suffix: agent_family_suffix,
            tribe: agent_tribe,
            model: agent_model,
            reasoning_effort: agent_effort,
            bead_id: agent_bead_id,
            hidden: agent_hidden,
            auto_enabled,
            auto_mode,
            finalizers,
            wait_runners,
            wait_priority,
        })
    };

    RawLaunchUnit {
        unit: LaunchUnitWire {
            logical_id,
            source_order: slot.slot_index,
            waits: Vec::new(),
            condition,
            payload,
        },
        raw_waits,
    }
}

fn parse_proc_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
    fenced_body_present: bool,
) -> Result<ParsedProcDirective, LaunchPlanDiagnosticWire> {
    let source = &prompt[directive.start..directive.end];
    let Some(open_rel) = source.find('(') else {
        return Err(typed_plan_diagnostic(
            "invalid-proc-form",
            "%proc requires a body: %proc(\"cmd\"), %proc(bash=...|python=...), or %proc:: plus a fence.",
            Some([directive.start, directive.end]),
        ));
    };
    let open = directive.start + open_rel;
    let Some(close) = find_matching_paren(prompt, open) else {
        return Err(typed_plan_diagnostic(
            "malformed-proc",
            "Malformed %proc(...) directive: missing closing ')'.",
            Some([directive.start, directive.end]),
        ));
    };
    let args = parse_directive_args_with_names(&prompt[open + 1..close], ',');
    let allowed: BTreeSet<&str> = [
        "bash",
        "python",
        "timeout",
        "idle_timeout",
        "cwd",
        "workspace",
        "label",
    ]
    .into_iter()
    .collect();
    let mut positional_body: Option<String> = None;
    let mut named_body: Option<(String, String)> = None;
    let mut options = BTreeMap::new();
    for arg in args {
        match arg.name.as_deref() {
            None => {
                if !arg.value.is_empty() {
                    if positional_body.is_some() {
                        return Err(typed_plan_diagnostic(
                            "duplicate-proc-body",
                            "%proc accepts exactly one body.",
                            Some([directive.start, directive.end]),
                        ));
                    }
                    positional_body = Some(arg.value);
                }
            }
            Some("bash") | Some("python") => {
                if named_body.is_some() {
                    return Err(typed_plan_diagnostic(
                        "duplicate-proc-body",
                        "%proc cannot combine bash= and python=.",
                        Some([directive.start, directive.end]),
                    ));
                }
                named_body = Some((arg.name.clone().unwrap(), arg.value));
            }
            Some(key) if allowed.contains(key) => {
                options.insert(key.to_string(), arg.value);
            }
            Some(key) => {
                return Err(typed_plan_diagnostic(
                    "unknown-proc-option",
                    &format!(
                        "Unsupported keyword on %proc: {key}=. Only bash=, python=, timeout=, idle_timeout=, cwd=, workspace=, and label= are supported."
                    ),
                    Some([directive.start, directive.end]),
                ));
            }
        }
    }
    if positional_body.is_some() && named_body.is_some() {
        return Err(typed_plan_diagnostic(
            "duplicate-proc-body",
            "%proc cannot combine a positional body with bash= or python=.",
            Some([directive.start, directive.end]),
        ));
    }
    let code = match (positional_body, named_body) {
        (Some(source), None) => Some(make_proc_code_value(source, CodeLanguage::Bash)?),
        (None, Some((language, source))) => {
            Some(make_proc_code_value(source, parse_code_language(&language)?)?)
        }
        (None, None) if fenced_body_present => None,
        (None, None) => {
            return Err(typed_plan_diagnostic(
                "missing-proc-body",
                "%proc requires a body: %proc(\"cmd\"), %proc(bash=...|python=...), or %proc:: plus a fence.",
                Some([directive.start, directive.end]),
            ))
        }
        _ => unreachable!("duplicate combinations are validated above"),
    };
    Ok(ParsedProcDirective { code, options })
}

fn make_proc_code_value(
    source: String,
    language: CodeLanguage,
) -> Result<CodeValueWire, LaunchPlanDiagnosticWire> {
    if source.trim().is_empty() {
        return Err(typed_plan_diagnostic(
            "empty-proc-body",
            "%proc requires a non-empty body.",
            None,
        ));
    }
    Ok(CodeValue {
        source,
        language,
        info_string: None,
    }
    .to_wire())
}

fn parse_code_language(
    value: &str,
) -> Result<CodeLanguage, LaunchPlanDiagnosticWire> {
    language_from_info_string(Some(value)).map_err(|message| {
        typed_plan_diagnostic("unknown-code-language", &message, None)
    })
}

fn parse_wait_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
    logical_id: &str,
    raw_waits: &mut Vec<RawWaitTarget>,
    wait_runners: &mut Option<u32>,
    wait_priority: &mut Option<i32>,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let span = [directive.start, directive.end];
    let source = Some(prompt[directive.start..directive.end].to_string());
    let mut args = Vec::new();
    for arg in &directive.args {
        if arg.contains(',') && !arg.contains('=') {
            args.extend(
                arg.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
            );
        } else {
            args.push(arg.clone());
        }
    }
    if args.is_empty() || args.iter().all(|arg| arg.is_empty()) {
        raw_waits.push(RawWaitTarget {
            target: RawWaitTargetKind::Previous,
            source,
            source_span: Some(span),
        });
        return;
    }
    for arg in args {
        let (name, value_raw) = split_named_directive_arg(&arg);
        let value = unquote_directive_arg_value(value_raw.trim());
        match name.as_deref() {
            Some("unit") => raw_waits.push(raw_wait("unit", value, span, source.clone())),
            Some("agent") => raw_waits.push(raw_wait("agent", value, span, source.clone())),
            Some("proc") => raw_waits.push(raw_wait("proc", value, span, source.clone())),
            Some("bead") => raw_waits.push(raw_wait("bead", value, span, source.clone())),
            Some("time") => raw_waits.push(raw_wait("time", value, span, source.clone())),
            Some("runners") => match value.parse::<u32>() {
                Ok(parsed) => *wait_runners = Some(parsed),
                Err(_) => diagnostics.push(typed_unit_diagnostic(
                    "invalid-wait-runners",
                    "%wait(runners=...) requires a non-negative integer.",
                    logical_id,
                    Some(span),
                )),
            },
            Some("priority") => match value.parse::<i32>() {
                Ok(parsed) => *wait_priority = Some(parsed),
                Err(_) => diagnostics.push(typed_unit_diagnostic(
                    "invalid-wait-priority",
                    "%wait(priority=...) requires an integer.",
                    logical_id,
                    Some(span),
                )),
            },
            Some(key) => diagnostics.push(typed_unit_diagnostic(
                "unknown-wait-target",
                &format!(
                    "Unsupported keyword on %wait: {key}=. Use unit=, agent=, proc=, bead=, time=, runners=, or priority=."
                ),
                logical_id,
                Some(span),
            )),
            None => raw_waits.push(RawWaitTarget {
                target: RawWaitTargetKind::Agent(value),
                source: source.clone(),
                source_span: Some(span),
            }),
        }
    }
}

fn raw_wait(
    kind: &str,
    value: String,
    span: [usize; 2],
    source: Option<String>,
) -> RawWaitTarget {
    let target = match kind {
        "unit" => RawWaitTargetKind::Unit(value),
        "agent" => RawWaitTargetKind::Agent(value),
        "proc" => RawWaitTargetKind::Proc(value),
        "bead" => RawWaitTargetKind::Bead(value),
        "time" => RawWaitTargetKind::Time(value),
        _ => RawWaitTargetKind::Agent(value),
    };
    RawWaitTarget {
        target,
        source,
        source_span: Some(span),
    }
}

#[derive(Debug, Default)]
struct ParsedIdDirective {
    identity: Option<String>,
    bead_id: Option<String>,
    clan: Option<String>,
    tribe: Option<String>,
    family_parent: Option<String>,
    family_suffix: Option<String>,
    force_reuse: bool,
    unsupported_on_proc: bool,
}

#[derive(Debug, Default)]
struct ParsedClanDirective {
    clan: Option<String>,
    tribe: Option<String>,
    summary: Option<String>,
    summary_script: Option<String>,
    region_end: usize,
}

fn parse_id_directive(
    directive: &DirectiveOccurrence,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> ParsedIdDirective {
    let mut parsed = ParsedIdDirective::default();
    let span = [directive.start, directive.end];
    if let Some(keys) = duplicate_named_args(&directive.args) {
        diagnostics.push(typed_unit_diagnostic(
            "duplicate-id-keyword",
            &format!("Duplicate keyword argument '{keys}' on %id."),
            logical_id,
            Some(span),
        ));
        parsed.unsupported_on_proc = true;
        return parsed;
    }

    let mut positional: Vec<String> = Vec::new();
    let mut named: BTreeMap<String, String> = BTreeMap::new();
    for (index, arg) in directive.args.iter().enumerate() {
        let (name, value_raw) = split_named_directive_arg(arg);
        let value = unquote_directive_arg_value(value_raw.trim());
        match name.as_deref() {
            Some("bead") | Some("clan") | Some("family") | Some("tribe") => {
                parsed.unsupported_on_proc = true;
                named.insert(name.unwrap(), value);
            }
            Some(other) => {
                parsed.unsupported_on_proc = true;
                diagnostics.push(typed_unit_diagnostic(
                    "invalid-id-keyword",
                    &format!(
                        "Unsupported keyword on %id: {other}=. Only bead=, clan=, family=, and tribe= are supported."
                    ),
                    logical_id,
                    Some(span),
                ));
            }
            None if index == 0 || !value.is_empty() => positional.push(value),
            None => {}
        }
    }

    let membership: Vec<&str> = ["clan", "family", "tribe"]
        .into_iter()
        .filter(|key| named.contains_key(*key))
        .collect();
    if membership.len() > 1 {
        diagnostics.push(typed_unit_diagnostic(
            "id-keyword-conflict",
            "The clan=, family=, and tribe= keywords on %id are mutually exclusive; set at most one.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    if positional.len() > 1 {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-id-form",
            "The positional family form on %id is no longer supported; use %id(<suffix>, family=<parent>) instead.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }

    if let Some(bead_id) = named.get("bead") {
        if bead_id.is_empty() || bead_id.chars().any(char::is_whitespace) {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-bead",
                "The bead= keyword on %id requires a non-empty, whitespace-free bead ID.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.bead_id = Some(bead_id.clone());
        }
    }

    if let Some(clan) = named.get("clan") {
        if positional.len() != 1 {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-clan",
                "The clan= keyword on %id requires exactly one positional member id, e.g. %id(worker, clan=research).",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        let (force_reuse, member_id) = strip_force_reuse(&positional[0]);
        if member_id.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-clan",
                "The clan= keyword on %id requires a non-empty member id.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if clan.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-clan",
                "The clan= keyword on %id requires a non-empty clan name.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        parsed.identity = Some(member_id);
        parsed.clan = Some(clan.clone());
        parsed.force_reuse = force_reuse;
        return parsed;
    }

    if let Some(family) = named.get("family") {
        if positional.len() != 1 {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                "The family= keyword on %id requires exactly one positional suffix; use %id(<suffix>, family=<family>) or %id(@, family=<family>).",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        let (force_reuse, suffix) = strip_force_reuse(&positional[0]);
        let parent = family.trim();
        if parent.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                "The family= keyword on %id requires a non-empty family name.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if suffix.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                "The family= keyword on %id requires a non-empty suffix.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if let Some(message) = invalid_family_suffix_reason(&suffix) {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                &message,
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        parsed.family_parent = Some(parent.to_string());
        parsed.family_suffix = Some(suffix);
        parsed.force_reuse = force_reuse;
        return parsed;
    }

    if let Some(tribe) = named.get("tribe") {
        let raw = positional.first().cloned().unwrap_or_default();
        let (force_reuse, identity) = strip_force_reuse(&raw);
        if !positional.is_empty() && identity.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-tribe",
                "The tribe= keyword on %id requires a non-empty id when a positional id is supplied.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if tribe.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-tribe",
                "The tribe= keyword on %id requires a non-empty tribe name.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if let Some(message) = invalid_tribe_reason(tribe, "%id") {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-tribe",
                &message,
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        parsed.identity = if identity.is_empty() {
            None
        } else {
            Some(identity)
        };
        parsed.tribe = Some(tribe.clone());
        parsed.force_reuse = force_reuse;
        return parsed;
    }

    if let Some(raw) = positional.first() {
        let (force_reuse, identity) = strip_force_reuse(raw);
        if !identity.is_empty() {
            parsed.identity = Some(identity);
            parsed.force_reuse = force_reuse;
        } else if force_reuse {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-form",
                "The tribe= keyword on %id requires a non-empty id when a positional id is supplied.",
                logical_id,
                Some(span),
            ));
        }
    }
    parsed
}

fn parse_clan_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
    logical_id: &str,
    ignored_ranges: &[(usize, usize)],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> ParsedClanDirective {
    let mut parsed = ParsedClanDirective {
        region_end: directive.end,
        ..ParsedClanDirective::default()
    };
    let span = [directive.start, directive.end];
    if directive.has_plus_suffix {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-clan-form",
            "%clan does not support '+'; use %clan:<name> or %clan(<name>, tribe=<tribe>).",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    if let Some(keys) = duplicate_named_args(&directive.args) {
        diagnostics.push(typed_unit_diagnostic(
            "duplicate-clan-keyword",
            &format!("Duplicate keyword argument '{keys}' on %clan."),
            logical_id,
            Some(span),
        ));
        return parsed;
    }

    let mut positional: Vec<String> = Vec::new();
    let mut named: BTreeMap<String, (String, bool)> = BTreeMap::new();
    for (index, arg) in directive.args.iter().enumerate() {
        let (name, value_raw) = split_named_directive_arg(arg);
        let trimmed_raw = value_raw.trim();
        let from_text_block = trimmed_raw.starts_with("[[");
        let value = unquote_directive_arg_value(trimmed_raw);
        match name.as_deref() {
            Some("tribe") | Some("summary") | Some("summary_script") => {
                named.insert(name.unwrap(), (value, from_text_block));
            }
            Some(other) => {
                diagnostics.push(typed_unit_diagnostic(
                    "invalid-clan-keyword",
                    &format!(
                        "Unsupported keyword on %clan: {other}=. Only summary=, summary_script=, and tribe= are supported."
                    ),
                    logical_id,
                    Some(span),
                ));
            }
            None if index == 0 || !value.is_empty() => positional.push(value),
            None => {}
        }
    }
    if positional.len() > 1 {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-clan-form",
            "%clan accepts exactly one positional clan name argument.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    let clan = positional.first().cloned().unwrap_or_default();
    if clan.trim().is_empty() {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-clan-form",
            "'%clan' directive requires a clan name argument (e.g., %clan:research.@).",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    parsed.clan = Some(clan);

    if named.contains_key("summary") && named.contains_key("summary_script") {
        diagnostics.push(typed_unit_diagnostic(
            "clan-summary-conflict",
            "'%clan' summary= and summary_script= are mutually exclusive.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    if let Some((tribe, _)) = named.get("tribe") {
        if tribe.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-tribe",
                "'%clan(..., tribe=...)' requires a non-empty tribe name.",
                logical_id,
                Some(span),
            ));
        } else if let Some(message) = invalid_tribe_reason(tribe, "%clan") {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-tribe",
                &message,
                logical_id,
                Some(span),
            ));
        } else {
            parsed.tribe = Some(tribe.clone());
        }
    }
    if let Some((summary, from_text_block)) = named.get("summary") {
        if summary.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-summary",
                "'%clan(..., summary=...)' requires a non-empty value.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.summary =
                Some(normalize_clan_summary(summary, *from_text_block));
        }
    }
    if let Some((script, _)) = named.get("summary_script") {
        if script.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-summary-script",
                "'%clan(..., summary_script=...)' requires a non-empty value.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.summary_script = Some(script.trim().to_string());
        }
    }

    if prompt[directive.end..].starts_with(":: ") {
        if parsed.summary.is_some() || parsed.summary_script.is_some() {
            diagnostics.push(typed_unit_diagnostic(
                "clan-shorthand-conflict",
                "Cannot combine %clan(...):: shorthand with explicit summary= or summary_script=.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        let text_start = directive.end + 3;
        let text_end =
            clan_double_colon_text_end(prompt, text_start, ignored_ranges);
        let text = prompt[text_start..text_end].trim_end();
        if text.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-summary",
                "'%clan(..., summary=...)' requires a non-empty value.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.summary = Some(normalize_clan_summary(text, true));
            parsed.region_end = text_end;
        }
    }
    parsed
}

#[allow(clippy::too_many_arguments)]
fn apply_parsed_identity(
    parsed_id: Option<&ParsedIdDirective>,
    parsed_clan: Option<&ParsedClanDirective>,
    logical_id: &str,
    agent_identity: &mut Option<String>,
    agent_identity_explicit: &mut bool,
    agent_identity_force_reuse: &mut bool,
    agent_clan: &mut Option<String>,
    agent_clan_declared: &mut bool,
    agent_clan_tribe: &mut Option<String>,
    agent_clan_summary: &mut Option<String>,
    agent_clan_summary_script: &mut Option<String>,
    agent_family_parent: &mut Option<String>,
    agent_family_suffix: &mut Option<String>,
    agent_tribe: &mut Option<String>,
    agent_bead_id: &mut Option<String>,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    if let Some(id) = parsed_id {
        if id.family_parent.is_some() {
            *agent_identity = None;
            *agent_identity_explicit = false;
            *agent_family_parent = id.family_parent.clone();
            *agent_family_suffix = id.family_suffix.clone();
        } else if let Some(identity) = id.identity.as_ref() {
            *agent_identity = Some(identity.clone());
            *agent_identity_explicit = true;
        }
        if let Some(bead_id) = id.bead_id.as_ref() {
            *agent_bead_id = Some(bead_id.clone());
        }
        if let Some(clan) = id.clan.as_ref() {
            *agent_clan = Some(clan.clone());
        }
        *agent_tribe = id.tribe.clone();
        *agent_identity_force_reuse = id.force_reuse;
    }
    if let Some(clan) = parsed_clan {
        *agent_clan = clan.clan.clone();
        *agent_clan_declared = clan.clan.is_some();
        *agent_clan_tribe = clan.tribe.clone();
        *agent_clan_summary = clan.summary.clone();
        *agent_clan_summary_script = clan.summary_script.clone();
    }

    let join_clan = parsed_id.and_then(|id| id.clan.as_ref());
    let family = parsed_id.and_then(|id| id.family_parent.as_ref());
    let id_tribe = parsed_id.and_then(|id| id.tribe.as_ref());
    if parsed_clan.is_some() && join_clan.is_some() {
        diagnostics.push(typed_unit_diagnostic(
            "clan-id-conflict",
            "Cannot combine %clan with %id(..., clan=...); a declaring prompt uses %clan(<clan>, tribe=<tribe>) with a full %id:<clan>.<id>, while a joining prompt uses only %id(<id>, clan=<clan>).",
            logical_id,
            None,
        ));
    }
    if parsed_clan.is_some() && id_tribe.is_some() {
        diagnostics.push(typed_unit_diagnostic(
            "clan-id-conflict",
            "Cannot combine %clan with %id(..., tribe=...); use %clan(<clan>, tribe=<tribe>) to set the clan's tribe.",
            logical_id,
            None,
        ));
    }
    if parsed_clan.is_some() && family.is_some() {
        diagnostics.push(typed_unit_diagnostic(
            "clan-id-conflict",
            "Cannot combine %clan with %id(..., family=...); choose clan membership or serial family attachment.",
            logical_id,
            None,
        ));
    }
}

fn duplicate_named_args(args: &[String]) -> Option<String> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for arg in args {
        if let Some(name) = split_named_directive_arg(arg).0 {
            if !seen.insert(name.clone()) {
                duplicates.insert(name);
            }
        }
    }
    if duplicates.is_empty() {
        None
    } else {
        Some(duplicates.into_iter().collect::<Vec<_>>().join(", "))
    }
}

fn strip_force_reuse(raw: &str) -> (bool, String) {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix('!') {
        (true, rest.to_string())
    } else {
        (false, trimmed.to_string())
    }
}

fn invalid_family_suffix_reason(suffix: &str) -> Option<String> {
    if suffix == "@" {
        return None;
    }
    if suffix.starts_with('.')
        || suffix.starts_with('-')
        || suffix.contains("--")
    {
        return Some(format!(
            "Invalid %i family suffix '{suffix}'. Pass the bare suffix without a family separator, e.g. %i(reviewer, family=parent)."
        ));
    }
    if !suffix
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Some(format!(
            "Invalid %i family suffix '{suffix}'. Use letters, numbers, and underscores only, or @ to allocate the next free suffix."
        ));
    }
    None
}

fn invalid_tribe_reason(tribe: &str, directive: &str) -> Option<String> {
    if tribe.starts_with('@') {
        return Some(format!(
            "Invalid '{directive}' tribe= value: tribe name {tribe:?} must not start with '@' (the '@' is added on display only — drop it from the input)"
        ));
    }
    if !tribe
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
    {
        return Some(format!(
            "Invalid '{directive}' tribe= value: tribe name {tribe:?} must match ^[A-Za-z0-9_.-]+$ (letters, digits, underscore, dot, dash)"
        ));
    }
    None
}

fn normalize_clan_summary(raw: &str, from_text_block: bool) -> String {
    if !from_text_block {
        return raw.trim().to_string();
    }
    let lines: Vec<&str> = raw.split('\n').collect();
    if lines.is_empty() {
        return String::new();
    }
    let first = lines[0].trim_start();
    let continuation = &lines[1..];
    let min_indent = continuation
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    let mut out = vec![first.to_string()];
    for line in continuation {
        if line.trim().is_empty() {
            out.push(String::new());
        } else {
            out.push(line[min_indent.min(line.len())..].to_string());
        }
    }
    out.join("\n").trim().to_string()
}

fn clan_double_colon_text_end(
    prompt: &str,
    start: usize,
    ignored_ranges: &[(usize, usize)],
) -> usize {
    let bytes = prompt.as_bytes();
    let mut idx = start;
    while idx < bytes.len() {
        if bytes[idx] == b'\n' {
            let item_start = idx + 1;
            if item_start < bytes.len()
                && !position_in_ranges(item_start, ignored_ranges)
                && is_prompt_item_start(&prompt[item_start..])
            {
                return idx;
            }
        }
        idx += 1;
    }
    prompt.len()
}

fn is_prompt_item_start(text: &str) -> bool {
    let rest = match text.as_bytes().first() {
        Some(b'%') => &text[1..],
        Some(b'#') => &text[1..],
        _ => return false,
    };
    let mut chars = rest.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    let mut consumed = first.len_utf8();
    for ch in chars {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '/' {
            consumed += ch.len_utf8();
            continue;
        }
        return ch.is_whitespace() || matches!(ch, '(' | ':' | '+' | '[');
    }
    consumed == rest.len()
}

fn validate_typed_unit_identities(
    raw_units: &[RawLaunchUnit],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for raw in raw_units {
        let identity = match &raw.unit.payload {
            LaunchUnitPayloadWire::Agent(agent) => agent.effective_identity(),
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                proc_unit.shell_name.clone()
            }
        };
        let Some(identity) = identity else {
            continue;
        };
        if let Some(first) =
            seen.insert(identity.clone(), raw.unit.logical_id.clone())
        {
            diagnostics.push(typed_unit_diagnostic(
                "identity-collision",
                &format!(
                    "Launch identity {identity:?} is ambiguous between {first} and {}.",
                    raw.unit.logical_id
                ),
                &raw.unit.logical_id,
                None,
            ));
        }
    }
}

fn resolve_typed_waits(
    raw_units: &mut [RawLaunchUnit],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let logical_ids: BTreeSet<String> = raw_units
        .iter()
        .map(|raw| raw.unit.logical_id.clone())
        .collect();
    let mut agent_names: BTreeMap<String, String> = BTreeMap::new();
    let mut proc_names: BTreeMap<String, String> = BTreeMap::new();
    for raw in raw_units.iter() {
        match &raw.unit.payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                if let Some(identity) = agent.effective_identity() {
                    agent_names.insert(identity, raw.unit.logical_id.clone());
                }
            }
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                if let Some(shell_name) = proc_unit.shell_name.as_ref() {
                    proc_names.insert(
                        shell_name.clone(),
                        raw.unit.logical_id.clone(),
                    );
                }
            }
        }
    }

    for index in 0..raw_units.len() {
        let logical_id = raw_units[index].unit.logical_id.clone();
        let mut waits = Vec::new();
        for wait in raw_units[index].raw_waits.clone() {
            match wait.target {
                RawWaitTargetKind::Previous => {
                    if index == 0 {
                        diagnostics.push(typed_unit_diagnostic(
                            "bare-wait-without-predecessor",
                            "Bare %wait requires a preceding launch unit.",
                            &logical_id,
                            wait.source_span,
                        ));
                    } else {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: raw_units[index - 1]
                                .unit
                                .logical_id
                                .clone(),
                            source: wait.source,
                        });
                    }
                }
                RawWaitTargetKind::Unit(target) => {
                    if logical_ids.contains(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: target,
                            source: wait.source,
                        });
                    } else {
                        diagnostics.push(typed_unit_diagnostic(
                            "unknown-logical-wait",
                            &format!(
                                "Unknown launch unit wait target {target:?}."
                            ),
                            &logical_id,
                            wait.source_span,
                        ));
                    }
                }
                RawWaitTargetKind::Agent(target) => {
                    if let Some(unit) = agent_names.get(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: unit.clone(),
                            source: wait.source,
                        });
                    } else {
                        waits.push(WaitTargetWire::Agent { name: target });
                    }
                }
                RawWaitTargetKind::Proc(target) => {
                    if let Some(unit) = proc_names.get(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: unit.clone(),
                            source: wait.source,
                        });
                    } else if logical_ids.contains(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: target,
                            source: wait.source,
                        });
                    } else {
                        waits.push(WaitTargetWire::Proc { identifier: target });
                    }
                }
                RawWaitTargetKind::Bead(bead_id) => {
                    waits.push(WaitTargetWire::Bead { bead_id });
                }
                RawWaitTargetKind::Time(value) => {
                    waits.push(WaitTargetWire::Time { value });
                }
            }
        }
        raw_units[index].unit.waits = waits;
    }
}

fn validate_typed_wait_cycles(
    raw_units: &[RawLaunchUnit],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let index_by_id: BTreeMap<String, usize> = raw_units
        .iter()
        .enumerate()
        .map(|(index, raw)| (raw.unit.logical_id.clone(), index))
        .collect();
    let mut graph: Vec<Vec<usize>> = vec![Vec::new(); raw_units.len()];
    for (index, raw) in raw_units.iter().enumerate() {
        for wait in &raw.unit.waits {
            if let WaitTargetWire::Logical { logical_id, .. } = wait {
                if let Some(target) = index_by_id.get(logical_id) {
                    graph[index].push(*target);
                }
            }
        }
    }
    let mut state = vec![0_u8; raw_units.len()];
    for index in 0..raw_units.len() {
        if state[index] == 0
            && wait_cycle_visit(index, &graph, &mut state).is_some()
        {
            diagnostics.push(typed_plan_diagnostic(
                "wait-cycle",
                "Typed launch waits contain a cycle.",
                None,
            ));
            return;
        }
    }
}

fn wait_cycle_visit(
    index: usize,
    graph: &[Vec<usize>],
    state: &mut [u8],
) -> Option<usize> {
    state[index] = 1;
    for target in &graph[index] {
        if state[*target] == 1 {
            return Some(*target);
        }
        if state[*target] == 0
            && wait_cycle_visit(*target, graph, state).is_some()
        {
            return Some(*target);
        }
    }
    state[index] = 2;
    None
}

fn validate_proc_shell_name(
    shell_name: Option<&str>,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let Some(shell_name) = shell_name else {
        return;
    };
    if shell_name.contains("--") {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-proc-shell-name",
            "Proc %id names cannot use the agent-family `--` convention.",
            logical_id,
            None,
        ));
    }
    if !is_valid_proc_shell_name(shell_name) {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-proc-shell-name",
            "Proc %id names must be bare identifiers containing only letters, digits, `_`, `.`, or `-`.",
            logical_id,
            None,
        ));
    }
}

fn is_valid_proc_shell_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')
        })
}

fn parse_proc_workspace(
    raw: Option<&str>,
    selected_project: Option<&str>,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> bool {
    let Some(raw) = raw else {
        return selected_project.is_some();
    };
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" => false,
        _ => {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-proc-workspace",
                "%proc workspace= must be a Boolean.",
                logical_id,
                None,
            ));
            false
        }
    }
}

fn validate_proc_project_policy(
    selected_project: Option<&str>,
    workspace: bool,
    cwd: Option<&str>,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    if workspace && selected_project.is_none() {
        diagnostics.push(typed_unit_diagnostic(
            "workspace-without-project",
            "%proc workspace=true requires a selected project.",
            logical_id,
            None,
        ));
    }
    if selected_project.is_none() && !workspace && cwd.is_none() {
        diagnostics.push(typed_unit_diagnostic(
            "proc-cwd-required",
            "%proc without a selected project requires an explicit cwd=.",
            logical_id,
            None,
        ));
    }
}

fn typed_directive_ignored_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut ranges = fenced_block_ranges(prompt);
    ranges.extend(disabled_region_ranges(prompt));
    if prompt.contains('`') {
        ranges.extend(launch_inline_literal_ranges(prompt));
    }
    ranges
}

fn strip_prompt_regions(prompt: &str, regions: &[(usize, usize)]) -> String {
    let mut merged = merge_ranges(regions);
    merged.sort_by_key(|range| std::cmp::Reverse(range.0));
    let mut cleaned = prompt.to_string();
    for (start, end) in merged {
        if start <= end && end <= cleaned.len() {
            cleaned.replace_range(start..end, "");
        }
    }
    leading_blank_line_re().replace(&cleaned, "").to_string()
}

fn merge_ranges(regions: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut sorted: Vec<(usize, usize)> = regions
        .iter()
        .copied()
        .filter(|(start, end)| start < end)
        .collect();
    sorted.sort_by_key(|range| range.0);
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in sorted {
        if let Some((_, last_end)) = merged.last_mut() {
            if start <= *last_end {
                *last_end = (*last_end).max(end);
                continue;
            }
        }
        merged.push((start, end));
    }
    merged
}

fn project_context_from_prompt(prompt: &str) -> Option<String> {
    let ignored = typed_directive_ignored_ranges(prompt);
    project_ref_re()
        .captures_iter(prompt)
        .filter_map(|captures| {
            let marker = captures.get(2)?;
            if position_in_ranges(marker.start(), &ignored) {
                return None;
            }
            Some(marker.as_str().to_string())
        })
        .next()
}

fn project_ref_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let ignored = typed_directive_ignored_ranges(prompt);
    project_ref_re()
        .captures_iter(prompt)
        .filter_map(|captures| {
            let marker = captures.get(2)?;
            (!position_in_ranges(marker.start(), &ignored))
                .then_some((marker.start(), marker.end()))
        })
        .collect()
}

fn project_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(^|[\s\(\[\{"'])(#(?:gh|git):[A-Za-z0-9_.~,+/@-]+)"#)
            .unwrap()
    })
}

fn render_launch_approval_preview(
    launch_kind: &str,
    selected_project: Option<&str>,
    units: &[LaunchUnitWire],
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "LaunchPlan v{} kind={} units={} project={}",
        LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
        launch_kind,
        units.len(),
        selected_project.unwrap_or("none")
    ));
    for unit in units {
        let waits = if unit.waits.is_empty() {
            "none".to_string()
        } else {
            unit.waits
                .iter()
                .map(wait_preview)
                .collect::<Vec<_>>()
                .join(",")
        };
        let condition = unit
            .condition
            .as_ref()
            .map(|condition| {
                format!(
                    " if={}:{}",
                    condition.code.language, condition.code.digest
                )
            })
            .unwrap_or_default();
        match &unit.payload {
            LaunchUnitPayloadWire::Agent(agent) => lines.push(format!(
                "{} agent identity={} model={} waits={}{} prompt={:?}",
                unit.logical_id,
                agent
                    .effective_identity()
                    .as_deref()
                    .unwrap_or("auto"),
                agent.model.as_deref().unwrap_or("default"),
                waits,
                condition,
                agent.prompt
            )),
            LaunchUnitPayloadWire::Proc(proc_unit) => lines.push(format!(
                "{} proc shell={} project={} workspace={} waits={}{} code={}:{} preview={:?}",
                unit.logical_id,
                proc_unit.shell_name.as_deref().unwrap_or("auto"),
                proc_unit.selected_project.as_deref().unwrap_or("none"),
                proc_unit.workspace,
                waits,
                condition,
                proc_unit.code.language,
                proc_unit.code.digest,
                proc_unit.code.preview
            )),
        }
    }
    lines
}

fn wait_preview(wait: &WaitTargetWire) -> String {
    match wait {
        WaitTargetWire::Logical { logical_id, .. } => {
            format!("unit:{logical_id}")
        }
        WaitTargetWire::Agent { name } => format!("agent:{name}"),
        WaitTargetWire::Proc { identifier } => format!("proc:{identifier}"),
        WaitTargetWire::Bead { bead_id } => format!("bead:{bead_id}"),
        WaitTargetWire::Time { value } => format!("time:{value}"),
    }
}

fn launch_plan_content_digest(
    launch_kind: &str,
    selected_project: Option<&str>,
    units: &[LaunchUnitWire],
) -> String {
    let value = serde_json::json!({
        "schema_version": LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
        "launch_kind": launch_kind,
        "selected_project": selected_project,
        "units": units,
    });
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}

fn typed_plan_diagnostic(
    code: &str,
    message: &str,
    source_span: Option<[usize; 2]>,
) -> LaunchPlanDiagnosticWire {
    LaunchPlanDiagnosticWire {
        code: code.to_string(),
        severity: "error".to_string(),
        message: message.to_string(),
        source_span,
        logical_id: None,
    }
}

fn typed_unit_diagnostic(
    code: &str,
    message: &str,
    logical_id: &str,
    source_span: Option<[usize; 2]>,
) -> LaunchPlanDiagnosticWire {
    LaunchPlanDiagnosticWire {
        code: code.to_string(),
        severity: "error".to_string(),
        message: message.to_string(),
        source_span,
        logical_id: Some(logical_id.to_string()),
    }
}

fn with_logical_id(
    mut diagnostic: LaunchPlanDiagnosticWire,
    logical_id: &str,
) -> LaunchPlanDiagnosticWire {
    diagnostic.logical_id = Some(logical_id.to_string());
    diagnostic
}

fn split_multi_prompt_segments(prompt: &str) -> Vec<String> {
    let body = prompt_body_after_frontmatter(prompt);
    let fenced_ranges = fenced_block_ranges(body);
    let mut segments = Vec::new();
    let mut segment_start = 0;
    let mut line_start = 0;

    for piece in body.split_inclusive('\n') {
        let line_end = line_start + piece.len();
        let content_end = if piece.ends_with('\n') {
            line_end - 1
        } else {
            line_end
        };
        let line = &body[line_start..content_end];
        if line.trim() == "---"
            && !position_in_ranges(line_start, &fenced_ranges)
        {
            push_nonempty_segment(
                &mut segments,
                &body[segment_start..line_start],
            );
            segment_start = line_end;
        }
        line_start = line_end;
    }
    if segment_start <= body.len() {
        push_nonempty_segment(&mut segments, &body[segment_start..]);
    }
    segments
}

fn prompt_body_after_frontmatter(prompt: &str) -> &str {
    let Some(first_line_end) = prompt.find('\n') else {
        return prompt;
    };
    if prompt[..first_line_end].trim() != "---" {
        return prompt;
    }

    let mut yaml_like = false;
    let mut offset = first_line_end + 1;
    for line in prompt[offset..].split_inclusive('\n') {
        let line_end = offset + line.len();
        let content_end = if line.ends_with('\n') {
            line_end - 1
        } else {
            line_end
        };
        let content = &prompt[offset..content_end];
        if content.trim() == "---" {
            return if yaml_like {
                &prompt[line_end..]
            } else {
                prompt
            };
        }
        if content.contains(':') {
            yaml_like = true;
        }
        offset = line_end;
    }
    prompt
}

fn push_nonempty_segment(out: &mut Vec<String>, segment: &str) {
    let trimmed = segment.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
}

fn split_prompt_for_models_with_ids(
    prompt: &str,
) -> Result<Vec<AlternativeSlot>, AgentLaunchFanoutPlanError> {
    if !prompt.contains('%') {
        return Ok(Vec::new());
    }

    let mut ignored_ranges = launch_literal_zone_ranges(prompt);
    ignored_ranges.extend(alt_inner_ranges(prompt, &ignored_ranges)?);

    let mut valued_directive_spans: Vec<(usize, usize, String)> = Vec::new();
    for directive in directive_occurrences(prompt)? {
        if directive.canonical_name != "model" {
            continue;
        }
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        if directive.has_plus_suffix {
            continue;
        }
        let values: Vec<String> = directive
            .args
            .iter()
            .filter(|arg| !arg.is_empty())
            .cloned()
            .collect();
        if values.len() > 1 {
            let source = prompt[directive.start..directive.end].to_string();
            return Err(AgentLaunchFanoutPlanError::MultiModelUnsupported(
                multi_model_unsupported_message(&source, &values),
            ));
        }
        if let Some(value) = values.first() {
            valued_directive_spans.push((
                directive.start,
                directive.end,
                value.clone(),
            ));
        }
    }

    if valued_directive_spans.len() > 1 {
        let source = valued_directive_spans
            .iter()
            .map(|(start, end, _)| prompt[*start..*end].to_string())
            .collect::<Vec<_>>()
            .join(" ... ");
        let models = valued_directive_spans
            .iter()
            .map(|(_, _, value)| value.clone())
            .collect::<Vec<_>>();
        return Err(AgentLaunchFanoutPlanError::MultiModelUnsupported(
            multi_model_unsupported_message(&source, &models),
        ));
    }

    Ok(split_prompt_for_alternatives_with_ids(prompt)?.unwrap_or_default())
}

fn multi_model_unsupported_message(source: &str, models: &[String]) -> String {
    let replacement = models
        .iter()
        .map(|model| format!("%m:{model}"))
        .collect::<Vec<_>>()
        .join(" | ");
    format!("{source} is no longer supported; use %{{{replacement}}} instead")
}

fn split_prompt_for_alternatives_with_ids(
    prompt: &str,
) -> Result<Option<Vec<AlternativeSlot>>, AgentLaunchFanoutPlanError> {
    let ignored_ranges = launch_literal_zone_ranges(prompt);
    let mut directives: Vec<AlternativeDirective> = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if position_in_ranges(start, &ignored_ranges) {
            continue;
        }
        let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) else {
            return Err(AgentLaunchFanoutPlanError::UnclosedDirective {
                name: delimiter.directive_label().to_string(),
                close: delimiter.close(),
            });
        };
        let inner = &prompt[open_start + 1..close_end];
        let args =
            parse_directive_args_with_names(inner, delimiter.separator());
        if args.is_empty() {
            continue;
        }
        directives.push(AlternativeDirective {
            start,
            end: close_end + 1,
            args,
        });
    }

    if directives.is_empty() {
        return Ok(None);
    }

    let mut axes = alternative_axes_for_directives(&directives);
    axes.sort_by_key(|axis| axis.start);
    let arg_lists: Vec<Vec<AlternativeVariant>> =
        axes.into_iter().map(|axis| axis.variants).collect();
    let mut combinations = Vec::new();
    cartesian_product(&arg_lists, 0, &mut Vec::new(), &mut combinations);

    let mut result = Vec::with_capacity(combinations.len());
    for combination in combinations {
        let alt_id = combination
            .iter()
            .map(|variant| variant.id.as_str())
            .collect::<Vec<_>>()
            .join(".");
        let replaced =
            render_alternative_prompt(prompt, &directives, &combination);
        result.push(AlternativeSlot {
            prompt: replaced,
            alt_id,
        });
    }
    Ok(Some(result))
}

/// Split `%alt(...)`, `%(...)`, and `%{...}` directives into launch slots.
///
/// Explicit branch names that appear in multiple directives are correlated:
/// the matching named branches render into the same slot instead of producing
/// a Cartesian product. Directives without shared explicit names keep the
/// historical Cartesian behavior, including the implicit empty branch for a
/// single-branch directive.
fn alternative_axes_for_directives(
    directives: &[AlternativeDirective],
) -> Vec<AlternativeAxis> {
    alternative_correlation_groups(directives)
        .into_iter()
        .map(|group| {
            if group.len() == 1 {
                alternative_singleton_axis(directives, group[0])
            } else {
                alternative_correlated_axis(directives, &group)
            }
        })
        .collect()
}

fn alternative_correlation_groups(
    directives: &[AlternativeDirective],
) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..directives.len()).collect();
    let mut first_directive_by_name: BTreeMap<String, usize> = BTreeMap::new();

    for (directive_index, directive) in directives.iter().enumerate() {
        for arg in &directive.args {
            let Some(name) = &arg.name else {
                continue;
            };
            if let Some(first_directive) =
                first_directive_by_name.get(name).copied()
            {
                union_alternative_group(
                    &mut parent,
                    first_directive,
                    directive_index,
                );
            } else {
                first_directive_by_name.insert(name.clone(), directive_index);
            }
        }
    }

    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for directive_index in 0..directives.len() {
        let root = find_alternative_group(&mut parent, directive_index);
        groups.entry(root).or_default().push(directive_index);
    }
    groups.into_values().collect()
}

fn find_alternative_group(parent: &mut [usize], index: usize) -> usize {
    if parent[index] != index {
        let root = find_alternative_group(parent, parent[index]);
        parent[index] = root;
    }
    parent[index]
}

fn union_alternative_group(parent: &mut [usize], left: usize, right: usize) {
    let left_root = find_alternative_group(parent, left);
    let right_root = find_alternative_group(parent, right);
    if left_root == right_root {
        return;
    }
    if left_root < right_root {
        parent[right_root] = left_root;
    } else {
        parent[left_root] = right_root;
    }
}

fn alternative_singleton_axis(
    directives: &[AlternativeDirective],
    directive_index: usize,
) -> AlternativeAxis {
    let directive = &directives[directive_index];
    let mut args = directive.args.clone();
    if args.len() == 1 {
        args.push(DirectiveArg {
            name: None,
            value: String::new(),
        });
    }
    let variants = allocate_alternative_branch_ids(args)
        .into_iter()
        .map(|branch| AlternativeVariant {
            id: branch.id,
            replacements: vec![AlternativeReplacement {
                directive_index,
                value: branch.value,
            }],
        })
        .collect();
    AlternativeAxis {
        start: directive.start,
        variants,
    }
}

fn alternative_correlated_axis(
    directives: &[AlternativeDirective],
    group: &[usize],
) -> AlternativeAxis {
    let allocated =
        allocate_correlated_alternative_branch_ids(directives, group);
    let mut variant_keys = Vec::new();
    let mut seen_keys = BTreeSet::new();
    let mut values_by_directive: BTreeMap<usize, BTreeMap<String, String>> =
        BTreeMap::new();

    for (directive_index, branches) in allocated {
        let mut values_by_id = BTreeMap::new();
        for branch in branches {
            if seen_keys.insert(branch.id.clone()) {
                variant_keys.push(branch.id.clone());
            }
            values_by_id.entry(branch.id).or_insert(branch.value);
        }
        values_by_directive.insert(directive_index, values_by_id);
    }

    let variants = variant_keys
        .into_iter()
        .map(|key| {
            let replacements = group
                .iter()
                .map(|directive_index| AlternativeReplacement {
                    directive_index: *directive_index,
                    value: values_by_directive
                        .get(directive_index)
                        .and_then(|values_by_id| values_by_id.get(&key))
                        .cloned()
                        .unwrap_or_default(),
                })
                .collect();
            AlternativeVariant {
                id: key,
                replacements,
            }
        })
        .collect();

    AlternativeAxis {
        start: group
            .iter()
            .map(|directive_index| directives[*directive_index].start)
            .min()
            .unwrap_or(0),
        variants,
    }
}

fn allocate_correlated_alternative_branch_ids(
    directives: &[AlternativeDirective],
    group: &[usize],
) -> Vec<(usize, Vec<AlternativeBranch>)> {
    let named_ids: BTreeSet<String> = group
        .iter()
        .flat_map(|directive_index| directives[*directive_index].args.iter())
        .filter_map(|arg| arg.name.clone())
        .collect();
    let mut next_numeric = 1_u32;

    group
        .iter()
        .map(|directive_index| {
            let branches = directives[*directive_index]
                .args
                .iter()
                .map(|arg| {
                    let id = match &arg.name {
                        Some(name) => name.clone(),
                        None => {
                            while named_ids.contains(&next_numeric.to_string())
                            {
                                next_numeric += 1;
                            }
                            let id = next_numeric.to_string();
                            next_numeric += 1;
                            id
                        }
                    };
                    AlternativeBranch {
                        value: arg.value.clone(),
                        id,
                    }
                })
                .collect();
            (*directive_index, branches)
        })
        .collect()
}

fn render_alternative_prompt(
    prompt: &str,
    directives: &[AlternativeDirective],
    combination: &[AlternativeVariant],
) -> String {
    let mut replacements: Vec<(usize, usize, String)> = combination
        .iter()
        .flat_map(|variant| {
            variant.replacements.iter().map(|replacement| {
                let directive = &directives[replacement.directive_index];
                (directive.start, directive.end, replacement.value.clone())
            })
        })
        .collect();
    replacements.sort_by_key(|replacement| std::cmp::Reverse(replacement.0));
    let has_empty_replacement =
        replacements.iter().any(|(_, _, value)| value.is_empty());

    let mut replaced = prompt.to_string();
    for (start, end, value) in replacements {
        if value.is_empty() {
            replaced.replace_range(start..end, EMPTY_ALT_SENTINEL_STR);
        } else {
            replaced.replace_range(start..end, &value);
        }
    }
    if has_empty_replacement {
        collapse_empty_alternative_whitespace(&replaced)
    } else {
        replaced
    }
}

/// Collapse the horizontal whitespace left by empty alt renders.
///
/// Empty branches remove adjacent spaces/tabs when they would leave doubled
/// spaces, leading/trailing spaces, or a space stranded against punctuation.
/// A single word-separating space is kept only between two alphanumeric
/// neighbors that already had horizontal whitespace at the empty site.
/// Newlines are hard boundaries and line-leading indentation is preserved;
/// spaces that keep a following `%directive` parseable are preserved; non-empty
/// branches never enter this pass.
fn collapse_empty_alternative_whitespace(rendered: &str) -> String {
    if !rendered.contains(EMPTY_ALT_SENTINEL) {
        return rendered.to_string();
    }

    let mut collapsed = String::with_capacity(rendered.len());
    let mut cursor = 0;
    while cursor < rendered.len() {
        let Some(ch) = rendered[cursor..].chars().next() else {
            break;
        };
        if !is_empty_alt_run_char(ch) {
            collapsed.push(ch);
            cursor += ch.len_utf8();
            continue;
        }

        let run_start = cursor;
        let mut run_end = cursor;
        let mut contains_sentinel = false;
        while run_end < rendered.len() {
            let ch = rendered[run_end..].chars().next().unwrap();
            if !is_empty_alt_run_char(ch) {
                break;
            }
            contains_sentinel |= ch == EMPTY_ALT_SENTINEL;
            run_end += ch.len_utf8();
        }

        if contains_sentinel {
            push_collapsed_empty_alt_run(
                rendered,
                run_start,
                run_end,
                &mut collapsed,
            );
        } else {
            collapsed.push_str(&rendered[run_start..run_end]);
        }
        cursor = run_end;
    }
    collapsed
}

fn push_collapsed_empty_alt_run(
    rendered: &str,
    run_start: usize,
    run_end: usize,
    collapsed: &mut String,
) {
    let line_leading = is_line_start(rendered, run_start);
    let mut collapse_start = run_start;
    if line_leading {
        while collapse_start < run_end {
            let ch = rendered[collapse_start..].chars().next().unwrap();
            if !is_horizontal_ws(ch) {
                break;
            }
            collapse_start += ch.len_utf8();
        }
        collapsed.push_str(&rendered[run_start..collapse_start]);
    }

    let had_horizontal_ws = rendered[collapse_start..run_end]
        .chars()
        .any(is_horizontal_ws);
    let left = if line_leading {
        None
    } else {
        rendered[..run_start].chars().next_back()
    };
    let right = rendered[run_end..].chars().next();

    if (had_horizontal_ws
        && should_preserve_directive_separator(rendered, run_end, left))
        || (had_horizontal_ws
            && left.is_some_and(char::is_alphanumeric)
            && right.is_some_and(char::is_alphanumeric))
    {
        collapsed.push(' ');
    }
}

fn should_preserve_directive_separator(
    rendered: &str,
    run_end: usize,
    left: Option<char>,
) -> bool {
    let Some(left) = left else {
        return false;
    };
    starts_with_directive_marker(&rendered[run_end..])
        && !is_directive_left_boundary(left)
}

fn starts_with_directive_marker(text: &str) -> bool {
    let mut chars = text.chars();
    if chars.next() != Some('%') {
        return false;
    }
    matches!(chars.next(), Some('{') | Some('(') | Some('a'..='z' | 'A'..='Z' | '_'))
}

fn is_directive_left_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '(' | '[' | '{' | '"' | '\'')
}

fn is_empty_alt_run_char(ch: char) -> bool {
    ch == EMPTY_ALT_SENTINEL || is_horizontal_ws(ch)
}

fn is_horizontal_ws(ch: char) -> bool {
    ch == ' ' || ch == '\t'
}

fn is_line_start(rendered: &str, index: usize) -> bool {
    index == 0
        || rendered[..index]
            .chars()
            .next_back()
            .is_some_and(|ch| ch == '\n' || ch == '\r')
}

fn extract_repeat_and_id_rust(
    prompt: &str,
) -> (Option<u32>, Option<String>, Option<String>, String) {
    if !prompt.contains('%') {
        return (None, None, None, prompt.to_string());
    }

    let ignored_ranges = launch_literal_zone_ranges(prompt);
    let mut repeat_count = None;
    let mut explicit_id = None;
    let mut bead_id = None;
    let mut regions = Vec::new();

    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        if directive.canonical_name != "repeat"
            && directive.canonical_name != "id"
        {
            continue;
        }
        regions.push((directive.start, directive.end));
        let raw_arg = if directive.has_plus_suffix {
            "true".to_string()
        } else {
            directive.args.first().cloned().unwrap_or_default()
        };
        if directive.canonical_name == "repeat" {
            repeat_count = raw_arg.parse::<u32>().ok();
        } else {
            for (index, arg) in directive.args.iter().enumerate() {
                let (name, value) = split_named_directive_arg(arg);
                match name.as_deref() {
                    Some("bead") => {
                        bead_id =
                            Some(unquote_directive_arg_value(value.trim()));
                    }
                    None if index == 0 && !arg.is_empty() => {
                        explicit_id = Some(arg.clone());
                    }
                    _ => {}
                }
            }
        }
    }

    if !matches!(repeat_count, Some(count) if count > 1) {
        return (None, None, None, prompt.to_string());
    }

    let mut cleaned = prompt.to_string();
    for (start, end) in regions.into_iter().rev() {
        cleaned.replace_range(start..end, "");
    }
    cleaned = leading_blank_line_re().replace(&cleaned, "").to_string();
    cleaned = strip_disabled_region_markers(&cleaned);
    (repeat_count, explicit_id, bead_id, cleaned)
}

fn has_wait_directive(prompt: &str) -> bool {
    let ignored_ranges = launch_literal_zone_ranges(prompt);
    if prompt.contains('%')
        && directive_occurrences(prompt)
            .unwrap_or_default()
            .iter()
            .any(|directive| {
                directive.canonical_name == "wait"
                    && !position_in_ranges(directive.start, &ignored_ranges)
            })
    {
        return true;
    }
    prompt.contains("#t")
        && xprompt_occurrences(prompt).iter().any(|reference| {
            reference.name == "t"
                && reference.has_time_argument
                && !position_in_ranges(reference.start, &ignored_ranges)
        })
}

fn extract_first_model_value(prompt: &str) -> Option<String> {
    if !prompt.contains('%') {
        return None;
    }
    let ignored_ranges = launch_literal_zone_ranges(prompt);
    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if directive.canonical_name == "model"
            && !position_in_ranges(directive.start, &ignored_ranges)
        {
            let value = directive.args.first()?;
            // Backtick-literal model values keep any `@` verbatim; every other
            // value has its trailing `@<effort>` peeled off so the slot is
            // named by the clean model, matching the Python fan-out namer.
            if directive.from_backtick_literal {
                return Some(value.clone());
            }
            let (clean_model, _) = split_model_effort(value);
            return Some(clean_model.to_string());
        }
    }
    None
}

fn directive_occurrences(
    prompt: &str,
) -> Result<Vec<DirectiveOccurrence>, AgentLaunchFanoutPlanError> {
    let mut out = Vec::new();
    for caps in directive_re().captures_iter(prompt) {
        let marker = caps.get(2).expect("directive marker group");
        let raw_name = caps.get(3).expect("directive name group").as_str();
        let canonical_name = canonical_directive_name(raw_name).to_string();
        let mut end = marker.end();
        let mut args = Vec::new();
        let mut has_plus_suffix = false;
        let mut from_backtick_literal = false;

        if caps.get(4).is_some() {
            let paren_start = marker.end() - 1;
            if let Some(paren_end) = find_matching_paren(prompt, paren_start) {
                args =
                    parse_directive_args(&prompt[paren_start + 1..paren_end]);
                end = paren_end + 1;
            }
        } else if let Some(colon_arg) = caps.get(5) {
            from_backtick_literal = colon_arg.as_str().starts_with('`');
            args = vec![unquote_backticks(colon_arg.as_str())];
        } else if caps.get(6).is_some() {
            has_plus_suffix = true;
            args = vec!["true".to_string()];
        } else {
            args = vec![String::new()];
        }

        out.push(DirectiveOccurrence {
            canonical_name,
            start: marker.start(),
            end,
            args,
            has_plus_suffix,
            from_backtick_literal,
        });
    }
    Ok(out)
}

fn xprompt_occurrences(prompt: &str) -> Vec<XPromptOccurrence> {
    xprompt_reference_re()
        .captures_iter(prompt)
        .filter_map(|captures| {
            let marker = captures.get(2)?;
            let name_match = captures.get(3)?;
            let name = name_match.as_str().replace("__", "/");
            let has_time_argument = prompt
                .as_bytes()
                .get(name_match.end())
                .is_some_and(|byte| matches!(byte, b':' | b'('));
            let mut end = marker.end();
            if captures.get(4).is_some() {
                let paren_start = marker.end() - 1;
                if let Some(paren_end) =
                    find_matching_paren(prompt, paren_start)
                {
                    end = paren_end + 1;
                }
            }
            Some(XPromptOccurrence {
                name,
                start: marker.start(),
                end,
                has_time_argument,
            })
        })
        .collect()
}

pub(crate) fn launch_literal_zone_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut ranges = fenced_block_ranges(prompt);
    ranges.extend(disabled_region_ranges(prompt));
    ranges.extend(code_directive_call_ranges(prompt));
    if prompt.contains('`') {
        ranges.extend(launch_inline_literal_ranges(prompt));
    }
    ranges
}

fn code_directive_call_ranges(prompt: &str) -> Vec<(usize, usize)> {
    directive_occurrences(prompt)
        .unwrap_or_default()
        .into_iter()
        .filter(|directive| {
            matches!(directive.canonical_name.as_str(), "if" | "proc")
        })
        .map(|directive| (directive.start, directive.end))
        .collect()
}

fn launch_inline_literal_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut masks = fenced_block_ranges(prompt);
    masks.extend(disabled_region_ranges(prompt));
    masks.extend(
        directive_occurrences(prompt)
            .unwrap_or_default()
            .into_iter()
            .map(|directive| (directive.start, directive.end)),
    );
    masks.extend(
        xprompt_occurrences(prompt)
            .into_iter()
            .map(|reference| (reference.start, reference.end)),
    );
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            masks.push((start, close_end + 1));
        }
    }
    inline_code_ranges(prompt, &masks)
}

fn alt_directive_starts(prompt: &str) -> Vec<(usize, usize, AltDelimiter)> {
    alt_directive_re()
        .captures_iter(prompt)
        .filter_map(|caps| {
            let marker = caps.get(2)?;
            let open = marker.end() - 1;
            let delimiter = if prompt.as_bytes()[open] == b'{' {
                AltDelimiter::Brace
            } else {
                AltDelimiter::Paren
            };
            Some((marker.start(), open, delimiter))
        })
        .collect()
}

fn alt_inner_ranges(
    prompt: &str,
    ignored_ranges: &[(usize, usize)],
) -> Result<Vec<(usize, usize)>, AgentLaunchFanoutPlanError> {
    let mut ranges = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if position_in_ranges(start, ignored_ranges) {
            continue;
        }
        if let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            ranges.push((open_start + 1, close_end));
        }
    }
    Ok(ranges)
}

fn parse_directive_args(inner: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = 0;
    while idx < inner.len() {
        if !in_backticks && !in_double_quotes {
            if let Some(next) = skip_directive_text_block(inner, idx) {
                idx = next;
                continue;
            }
        }
        let ch = inner[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            ',' if depth == 0 => {
                push_arg(&mut args, &inner[start..idx]);
                start = idx + ch_len;
            }
            _ => {}
        }
        idx += ch_len;
    }
    push_arg(&mut args, &inner[start..]);
    args.into_iter().filter(|arg| !arg.is_empty()).collect()
}

fn parse_directive_args_with_names(
    inner: &str,
    separator: char,
) -> Vec<DirectiveArg> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = 0;
    while idx < inner.len() {
        if !in_backticks && !in_double_quotes {
            if let Some(next) = skip_directive_text_block(inner, idx) {
                idx = next;
                continue;
            }
        }
        let ch = inner[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            _ if ch == separator && depth == 0 => {
                push_directive_arg(&mut args, &inner[start..idx]);
                start = idx + ch_len;
            }
            _ => {}
        }
        idx += ch_len;
    }
    push_directive_arg(&mut args, &inner[start..]);
    args.into_iter()
        .filter(|arg| !arg.value.is_empty() || arg.name.is_some())
        .collect()
}

fn push_directive_arg(args: &mut Vec<DirectiveArg>, raw: &str) {
    let trimmed = raw.trim();
    let (name, value_raw) = split_named_directive_arg(trimmed);
    let value_trimmed = value_raw.trim();
    let value = unquote_directive_arg_value(value_trimmed);
    args.push(DirectiveArg { name, value });
}

fn push_arg(args: &mut Vec<String>, raw: &str) {
    let trimmed = raw.trim();
    args.push(unquote_directive_arg_value(trimmed));
}

fn split_named_directive_arg(raw: &str) -> (Option<String>, &str) {
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = 0;
    while idx < raw.len() {
        if !in_backticks && !in_double_quotes {
            if let Some(next) = skip_directive_text_block(raw, idx) {
                idx = next;
                continue;
            }
        }
        let ch = raw[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            '=' if depth == 0 => {
                let name = raw[..idx].trim();
                let value = &raw[idx + ch_len..];
                if !name.is_empty() {
                    return (Some(unquote_backticks(name)), value);
                }
                return (None, raw);
            }
            _ => {}
        }
        idx += ch_len;
    }
    (None, raw)
}

fn unquote_directive_arg_value(trimmed: &str) -> String {
    if trimmed.starts_with("[[")
        && trimmed.ends_with("]]")
        && trimmed.len() >= 4
    {
        trimmed[2..trimmed.len() - 2].to_string()
    } else if ((trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\'')))
        && trimmed.len() >= 2
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        unquote_backticks(trimmed)
    }
}

fn allocate_alternative_branch_ids(
    args: Vec<DirectiveArg>,
) -> Vec<AlternativeBranch> {
    let named_ids: BTreeSet<String> =
        args.iter().filter_map(|arg| arg.name.clone()).collect();
    let mut next_numeric = 1_u32;
    args.into_iter()
        .map(|arg| {
            let id = match arg.name {
                Some(name) => name,
                None => {
                    while named_ids.contains(&next_numeric.to_string()) {
                        next_numeric += 1;
                    }
                    let id = next_numeric.to_string();
                    next_numeric += 1;
                    id
                }
            };
            AlternativeBranch {
                value: arg.value,
                id,
            }
        })
        .collect()
}

fn unquote_backticks(value: &str) -> String {
    if value.starts_with('`') && value.ends_with('`') && value.len() >= 2 {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn find_matching_paren(text: &str, paren_start: usize) -> Option<usize> {
    find_matching_delimiter(text, paren_start, '(', ')')
}

/// Find the index of the delimiter that closes the `open` character at
/// `open_start`, counting only that delimiter pair and ignoring matches inside
/// backtick-quoted spans.
fn find_matching_delimiter(
    text: &str,
    open_start: usize,
    open: char,
    close: char,
) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_backticks = false;
    let mut in_double_quotes = false;
    let mut idx = open_start;
    while idx < text.len() {
        if !in_backticks
            && !in_double_quotes
            && text.as_bytes().get(idx..idx + 2) == Some(b"[[")
        {
            idx = find_text_block_close_for_args(text, idx, text.len())? + 2;
            continue;
        }
        let ch = text[idx..].chars().next().expect("char boundary");
        let ch_len = ch.len_utf8();
        if ch == '`' && !in_double_quotes {
            in_backticks = !in_backticks;
            idx += ch_len;
            continue;
        }
        if ch == '"' && !in_backticks {
            in_double_quotes = !in_double_quotes;
            idx += ch_len;
            continue;
        }
        if in_backticks || in_double_quotes {
            idx += ch_len;
            continue;
        }
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(idx);
            }
        }
        idx += ch_len;
    }
    None
}

/// Skip a `[[...]]` argument text block at `idx`, or the rest of `text` when
/// the block is unterminated. Returns `None` when `idx` is not a block opener.
fn skip_directive_text_block(text: &str, idx: usize) -> Option<usize> {
    if text.as_bytes().get(idx..idx + 2) != Some(b"[[") {
        return None;
    }
    Some(
        find_text_block_close_for_args(text, idx, text.len())
            .map(|close| close + 2)
            .unwrap_or(text.len()),
    )
}

fn cartesian_product<T: Clone>(
    lists: &[Vec<T>],
    idx: usize,
    current: &mut Vec<T>,
    out: &mut Vec<Vec<T>>,
) {
    if idx == lists.len() {
        out.push(current.clone());
        return;
    }
    for item in &lists[idx] {
        current.push(item.clone());
        cartesian_product(lists, idx + 1, current, out);
        current.pop();
    }
}

fn disabled_region_ranges(text: &str) -> Vec<(usize, usize)> {
    disabled_region_re()
        .find_iter(text)
        .map(|m| (m.start(), m.end()))
        .collect()
}

fn strip_disabled_region_markers(text: &str) -> String {
    disabled_marker_re().replace_all(text, "").to_string()
}

fn position_in_ranges(pos: usize, ranges: &[(usize, usize)]) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start <= pos && pos < *end)
}

/// Canonicalize a directive name for fan-out planning by deferring to the
/// shared editor directive registry. This keeps the planner in lock-step with
/// the advertised directive set, including `%a`→`auto`, instead of maintaining
/// a second alias table that can drift. Unknown names pass through unchanged.
fn canonical_directive_name(name: &str) -> &str {
    crate::editor::directive::canonical_directive_name(name).unwrap_or(name)
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // The colon-arg class includes `@` so a `%model:<model>@<effort>`
        // suffix is captured as one directive value (matching the Python
        // `_DIRECTIVE_PATTERN`); the `@effort` token is split off in
        // `extract_first_model_value` via `split_model_effort`.
        Regex::new(
            r#"(?m)(^|[\s\(\[\{"'])(%([A-Za-z_][A-Za-z0-9_]*)(?:(\()|:(`[^`]*`|[A-Za-z0-9_#/.,()@-]*[A-Za-z0-9_#/,()@-])|(\+))?)"#,
        )
        .unwrap()
    })
}

fn xprompt_reference_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(^|[\s\(\[\{"'])(#!?([A-Za-z_][A-Za-z0-9_]*(?:/[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?(?:(\()|:(`[^`]*`|\$\([^)]*\)|\{\{[^}]*\}\}|\{[^}]*\}|[A-Za-z0-9_.~,+/@-]*[A-Za-z0-9_~,+/@-])|(\+))?)"#,
        )
        .unwrap()
    })
}

fn alt_directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(^|[\s\(\[\{"':])(%(?:alt)?\(|%\{)"#).unwrap()
    })
}

fn disabled_region_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?ms)^[ \t]*%xprompts_enabled:false[ \t]*\n.*?(?:^[ \t]*|[ \t]+)%xprompts_enabled:true[ \t]*\n?",
        )
        .unwrap()
    })
}

fn disabled_marker_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?m)^[ \t]*%xprompts_enabled:(?:false|true)[ \t]*\n?|[ \t]+%xprompts_enabled:(?:false|true)[ \t]*",
        )
        .unwrap()
    })
}

fn leading_blank_line_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*\n").unwrap())
}

fn plan_multi_prompt_fanout(prompt: &str) -> LaunchFanoutPlanWire {
    let segments = split_multi_prompt_segments(prompt);
    LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "multi_prompt".to_string(),
        slots: segments
            .into_iter()
            .enumerate()
            .map(|(idx, segment)| LaunchFanoutSlotWire {
                wait_for_previous: has_wait_directive(&segment),
                prompt: segment,
                launch_kind: "multi_prompt".to_string(),
                slot_index: idx as u32,
                alt_id: None,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: None,
                bead_id: None,
            })
            .collect(),
        requires_sequential_naming_wait: true,
        fanout_sleep_seconds: 0.0,
    }
}

fn plan_alternative_fanout(
    prompt: &str,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let slots_with_ids =
        split_prompt_for_alternatives_with_ids(prompt)?.unwrap_or_default();
    Ok(LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "alternatives".to_string(),
        slots: slots_with_ids
            .into_iter()
            .enumerate()
            .map(|(idx, slot)| LaunchFanoutSlotWire {
                wait_for_previous: has_wait_directive(&slot.prompt),
                prompt: slot.prompt,
                launch_kind: "alternatives".to_string(),
                slot_index: idx as u32,
                alt_id: Some(slot.alt_id),
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: None,
                bead_id: None,
            })
            .collect(),
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    })
}

fn plan_model_fanout(
    prompt: &str,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let slots_with_ids = split_prompt_for_models_with_ids(prompt)?;
    Ok(LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "model".to_string(),
        slots: slots_with_ids
            .into_iter()
            .enumerate()
            .map(|(idx, slot)| {
                let model = extract_first_model_value(&slot.prompt);
                LaunchFanoutSlotWire {
                    wait_for_previous: has_wait_directive(&slot.prompt),
                    prompt: slot.prompt,
                    launch_kind: "model".to_string(),
                    slot_index: idx as u32,
                    alt_id: Some(slot.alt_id),
                    timestamp: None,
                    workflow_name: None,
                    model,
                    repeat_name: None,
                    bead_id: None,
                }
            })
            .collect(),
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    })
}

fn plan_repeat_fanout(prompt: &str) -> LaunchFanoutPlanWire {
    let (count, explicit_id, bead_id, stripped) =
        extract_repeat_and_id_rust(prompt);
    let slots = match count {
        Some(count) if count > 1 => (0..count)
            .map(|idx| LaunchFanoutSlotWire {
                prompt: stripped.clone(),
                launch_kind: "repeat".to_string(),
                slot_index: idx,
                alt_id: None,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: explicit_id.clone(),
                bead_id: bead_id.clone(),
                wait_for_previous: idx > 0,
            })
            .collect(),
        _ => Vec::new(),
    };
    LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "repeat".to_string(),
        slots,
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    }
}

pub fn prepare_agent_launch(
    request: &AgentLaunchRequestWire,
    python_executable: &str,
    runner_script: &str,
    sase_tmpdir: Option<&str>,
    output_root: &str,
    preallocated_env: &BTreeMap<String, String>,
) -> Result<AgentLaunchPreparedWire, AgentLaunchPreparationError> {
    if request.schema_version != AGENT_LAUNCH_WIRE_SCHEMA_VERSION {
        return Err(AgentLaunchPreparationError::SchemaVersion {
            expected: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }

    let prompt_file =
        write_prompt_temp_file(sase_tmpdir, request.prompt.as_bytes())?;
    let safe_name = safe_launch_name(&request.cl_name);
    let output_root_path = Path::new(output_root);
    std::fs::create_dir_all(output_root_path)
        .map_err(AgentLaunchPreparationError::CreateOutputRoot)?;
    let output_path = output_root_path
        .join(format!("{safe_name}_ace-run-{}.txt", request.timestamp))
        .to_string_lossy()
        .into_owned();

    let mut env_delta = request.extra_env.clone();
    env_delta.insert("SASE_AGENT".to_string(), "1".to_string());
    env_delta.insert("SASE_AGENT_CL_NAME".to_string(), request.cl_name.clone());
    env_delta.insert(
        "SASE_AGENT_PROJECT_FILE".to_string(),
        request.project_file.clone(),
    );
    env_delta.insert(
        "SASE_AGENT_TIMESTAMP".to_string(),
        request.timestamp.clone(),
    );

    if request.deferred_workspace {
        env_delta.insert(
            "SASE_AGENT_DEFERRED_WORKSPACE".to_string(),
            "1".to_string(),
        );
        if let Some(workflow_type) = request.vcs_workflow_type.as_ref() {
            env_delta.insert(
                "SASE_AGENT_VCS_WORKFLOW_TYPE".to_string(),
                workflow_type.clone(),
            );
        }
    }

    for (key, value) in preallocated_env {
        env_delta.insert(key.clone(), value.clone());
    }

    if let Some(local_xprompts_file) = request.local_xprompts_file.as_ref() {
        env_delta.insert(
            "SASE_AGENT_LOCAL_XPROMPTS".to_string(),
            local_xprompts_file.clone(),
        );
    }

    let prompt_file_str = prompt_file.to_string_lossy().into_owned();
    let argv = vec![
        python_executable.to_string(),
        runner_script.to_string(),
        request.cl_name.clone(),
        request.project_file.clone(),
        request.workspace_dir.clone(),
        output_path.clone(),
        request.workspace_num.to_string(),
        request.workflow_name.clone(),
        prompt_file_str.clone(),
        request.timestamp.clone(),
        request.update_target.clone(),
        request.project_name.clone(),
        request.history_sort_key.clone(),
        if request.is_home_mode {
            "1".to_string()
        } else {
            String::new()
        },
    ];

    let claim_request = if request.is_home_mode {
        None
    } else {
        Some(WorkspaceClaimRequestWire {
            project_file: request.project_file.clone(),
            workspace_num: if request.deferred_workspace {
                0
            } else {
                request.workspace_num
            },
            workflow_name: request.workflow_name.clone(),
            pid: 0,
            cl_name: request.cl_name.clone(),
            artifacts_timestamp: String::new(),
            transfer_from_pid: request.retry_transfer_from_pid,
            pinned: false,
        })
    };

    Ok(AgentLaunchPreparedWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        prompt_file: prompt_file_str,
        output_path,
        safe_name,
        argv,
        cwd: request.workspace_dir.clone(),
        env_delta,
        claim_request,
    })
}

pub fn safe_launch_name(cl_name: &str) -> String {
    cl_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn write_prompt_temp_file(
    sase_tmpdir: Option<&str>,
    prompt: &[u8],
) -> Result<std::path::PathBuf, AgentLaunchPreparationError> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("sase_ace_prompt_").suffix(".md");
    let mut file = match sase_tmpdir {
        Some(dir) if !dir.is_empty() => builder
            .tempfile_in(dir)
            .map_err(AgentLaunchPreparationError::CreateTempFile)?,
        _ => builder
            .tempfile()
            .map_err(AgentLaunchPreparationError::CreateTempFile)?,
    };
    file.write_all(prompt)
        .map_err(AgentLaunchPreparationError::WritePrompt)?;
    let (_file, path) = file
        .keep()
        .map_err(|err| AgentLaunchPreparationError::KeepTempFile(err.error))?;
    Ok(path)
}

pub fn list_workspace_claims_from_content(
    content: &str,
) -> Vec<WorkspaceClaimWire> {
    let mut claims = Vec::new();
    let mut in_running_field = false;

    for line in content.split('\n') {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if !in_running_field {
            continue;
        }
        if !is_running_continuation_line(line) {
            break;
        }
        if let Some(claim) = WorkspaceClaimLine::parse(line) {
            claims.push(claim.into_wire());
        }
    }

    claims
}

pub fn plan_claim_workspace_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let (_running_idx, running_end_idx) = find_running_field_bounds(&lines);

    if request.workspace_num != 0 {
        for line in running_claim_lines(&lines) {
            if let Some(existing) = WorkspaceClaimLine::parse(line) {
                if existing.workspace_num == request.workspace_num {
                    return claim_plan(
                        content.to_string(),
                        false,
                        request,
                        Some(format!(
                            "workspace #{} is already claimed",
                            request.workspace_num
                        )),
                        false,
                    );
                }
            }
        }
    }

    let new_claim = WorkspaceClaimLine::from_request(request);
    if let Some(end) = running_end_idx {
        lines.insert(end + 1, new_claim.to_line());
    } else {
        lines.insert(0, String::new());
        lines.insert(0, new_claim.to_line());
        lines.insert(0, "RUNNING:".to_string());
    }

    claim_plan(
        normalize_running_field_spacing(&lines.join("\n")),
        true,
        request,
        None,
        true,
    )
}

pub fn plan_transfer_workspace_claim_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let Some(from_pid) = request.transfer_from_pid else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some("transfer_from_pid is required".to_string()),
            false,
        );
    };

    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let mut in_running_field = false;

    for line in &mut lines {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if in_running_field && is_running_continuation_line(line) {
            if let Some(claim) = WorkspaceClaimLine::parse(line) {
                let cl_matches = request.cl_name.is_empty()
                    || claim.cl_name.as_deref()
                        == Some(request.cl_name.as_str());
                if claim.workspace_num == request.workspace_num
                    && claim.pid == from_pid
                    && cl_matches
                {
                    let replacement = claim.transfer_to(request);
                    *line = replacement.to_line();
                    return claim_plan(
                        lines.join("\n"),
                        true,
                        request,
                        None,
                        true,
                    );
                }
            }
        } else {
            in_running_field = false;
        }
    }

    claim_plan(
        content.to_string(),
        false,
        request,
        Some(format!(
            "workspace #{} with pid {from_pid} was not found",
            request.workspace_num
        )),
        false,
    )
}

pub fn allocate_and_claim_workspace_from_content(
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let claimed: BTreeSet<u32> = list_workspace_claims_from_content(content)
        .into_iter()
        .map(|claim| claim.workspace_num)
        .collect();
    let Some(workspace_num) =
        (min_workspace..=max_workspace).find(|n| !claimed.contains(n))
    else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some(format!(
                "all workspaces ({min_workspace}-{max_workspace}) are claimed"
            )),
            false,
        );
    };

    let mut allocated_request = request.clone();
    allocated_request.workspace_num = workspace_num;
    plan_claim_workspace_from_content(content, &allocated_request)
}

fn claim_plan(
    content: String,
    success: bool,
    request: &WorkspaceClaimRequestWire,
    error: Option<String>,
    changed: bool,
) -> WorkspaceClaimPlanWire {
    WorkspaceClaimPlanWire {
        content,
        outcome: WorkspaceClaimOutcomeWire {
            success,
            workspace_num: request.workspace_num,
            project_file: request.project_file.clone(),
            pid: Some(request.pid),
            error,
        },
        changed,
    }
}

/// Decide whether a destructive workspace-preparation step (clean, reset,
/// checkout) may proceed against a checkout that may be occupied by another
/// live agent.
///
/// `occupant` is the parsed `.sase/occupant.json` record for the checkout,
/// if one exists; a missing record is always treated as unoccupied so
/// checkouts created before this guard existed are never bricked.
/// `occupant_pid_alive` and `running_claim_pid_alive` are supplied by the
/// caller, which alone knows how to probe process liveness.  `running_claim`
/// is the RUNNING-field claim row for `caller.workspace_num`, used only to
/// cross-check against the occupant record; a disagreement between the two
/// sources of truth is itself treated as a conflict.
pub fn decide_workspace_occupant_conflict(
    occupant: Option<&OccupantRecordWire>,
    caller: &OccupancyCallerWire,
    occupant_pid_alive: bool,
    running_claim: Option<&WorkspaceClaimWire>,
    running_claim_pid_alive: bool,
) -> OccupancyConflictDecisionWire {
    let Some(occupant) = occupant else {
        return OccupancyConflictDecisionWire {
            may_proceed: true,
            conflict: false,
            reason:
                "no occupant record present; treating checkout as unoccupied"
                    .to_string(),
        };
    };

    let occupant_is_live_other =
        occupant.pid != caller.pid && occupant_pid_alive;
    let claim_is_live_other = running_claim
        .map(|claim| claim.pid != caller.pid && running_claim_pid_alive)
        .unwrap_or(false);

    if !occupant_is_live_other {
        if claim_is_live_other {
            let claim =
                running_claim.expect("claim_is_live_other implies Some");
            return OccupancyConflictDecisionWire {
                may_proceed: false,
                conflict: true,
                reason: format!(
                    "occupant record for workspace #{} is stale but the RUNNING \
                     field still claims it for pid {} (workflow {}); refusing to \
                     prepare until that claim is resolved",
                    caller.workspace_num, claim.pid, claim.workflow
                ),
            };
        }
        return OccupancyConflictDecisionWire {
            may_proceed: true,
            conflict: false,
            reason: if occupant.pid == caller.pid {
                "caller already holds this checkout".to_string()
            } else {
                format!(
                    "occupant pid {} is not alive; treating as stale and allowing \
                     takeover",
                    occupant.pid
                )
            },
        };
    }

    let disagrees_with_running_field = match running_claim {
        Some(claim) => claim.pid != occupant.pid,
        None => true,
    };
    let occupant_label = occupant
        .agent_name
        .clone()
        .unwrap_or_else(|| occupant.workflow.clone());
    let artifacts_part = occupant
        .artifacts_timestamp
        .as_ref()
        .map(|ts| format!(", artifacts {ts}"))
        .unwrap_or_default();
    let mut reason = format!(
        "workspace #{} checkout is occupied by {} (pid {}, live{})",
        caller.workspace_num, occupant_label, occupant.pid, artifacts_part
    );
    if disagrees_with_running_field {
        reason.push_str(
            "; RUNNING field and occupant record disagree, which itself \
             indicates a corrupted claim state",
        );
    }
    OccupancyConflictDecisionWire {
        may_proceed: false,
        conflict: true,
        reason,
    }
}

fn running_claim_lines(lines: &[String]) -> impl Iterator<Item = &str> {
    let (start, end) = find_running_field_bounds(lines);
    let start = start.unwrap_or(0);
    let end = end.unwrap_or(0);
    lines
        .iter()
        .enumerate()
        .filter(move |(idx, _)| *idx > start && *idx <= end)
        .map(|(_, line)| line.as_str())
}

fn find_running_field_bounds(
    lines: &[String],
) -> (Option<usize>, Option<usize>) {
    for (i, line) in lines.iter().enumerate() {
        if line.starts_with("RUNNING:") {
            let mut running_end_idx = i;
            for (j, candidate) in lines.iter().enumerate().skip(i + 1) {
                if is_running_continuation_line(candidate) {
                    running_end_idx = j;
                } else {
                    break;
                }
            }
            return (Some(i), Some(running_end_idx));
        }
    }
    (None, None)
}

fn is_running_continuation_line(line: &str) -> bool {
    line.starts_with("  ")
        && (line.trim().starts_with('#') || line.trim().starts_with('|'))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceClaimLine {
    workspace_num: u32,
    pid: u32,
    workflow: String,
    cl_name: Option<String>,
    artifacts_timestamp: Option<String>,
    pinned: bool,
    suffix_parts: Vec<WorkspaceClaimSuffixPart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkspaceClaimSuffixPart {
    Timestamp(String),
    Pinned,
    Unknown(String),
}

impl WorkspaceClaimSuffixPart {
    fn raw_value(&self) -> &str {
        match self {
            Self::Timestamp(value) | Self::Unknown(value) => value,
            Self::Pinned => "PINNED",
        }
    }
}

impl WorkspaceClaimLine {
    fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        if !trimmed.starts_with('#') {
            return None;
        }
        let parts: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        if parts.len() < 4 {
            return None;
        }

        let workspace_num = parts[0].strip_prefix('#')?.parse::<u32>().ok()?;
        let pid = parts[1].parse::<u32>().ok()?;
        let workflow = parts[2];
        if workflow.is_empty() {
            return None;
        }

        let mut artifacts_timestamp = None;
        let mut pinned = false;
        let mut suffix_parts = Vec::new();
        for part in parts.iter().skip(4) {
            if *part == "PINNED" {
                pinned = true;
                suffix_parts.push(WorkspaceClaimSuffixPart::Pinned);
            } else if is_timestamp_part(part) {
                let value = (*part).to_string();
                if artifacts_timestamp.is_none() {
                    artifacts_timestamp = Some(value.clone());
                    suffix_parts
                        .push(WorkspaceClaimSuffixPart::Timestamp(value));
                } else {
                    suffix_parts.push(WorkspaceClaimSuffixPart::Unknown(value));
                }
            } else {
                suffix_parts.push(WorkspaceClaimSuffixPart::Unknown(
                    (*part).to_string(),
                ));
            }
        }

        Some(Self {
            workspace_num,
            pid,
            workflow: workflow.to_string(),
            cl_name: if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            },
            artifacts_timestamp,
            pinned,
            suffix_parts,
        })
    }

    fn from_request(request: &WorkspaceClaimRequestWire) -> Self {
        let mut suffix_parts = Vec::new();
        let artifacts_timestamp = if request.artifacts_timestamp.is_empty() {
            None
        } else {
            suffix_parts.push(WorkspaceClaimSuffixPart::Timestamp(
                request.artifacts_timestamp.clone(),
            ));
            Some(request.artifacts_timestamp.clone())
        };
        if request.pinned {
            suffix_parts.push(WorkspaceClaimSuffixPart::Pinned);
        }
        Self {
            workspace_num: request.workspace_num,
            pid: request.pid,
            workflow: request.workflow_name.clone(),
            cl_name: if request.cl_name.is_empty() {
                None
            } else {
                Some(request.cl_name.clone())
            },
            artifacts_timestamp,
            pinned: request.pinned,
            suffix_parts,
        }
    }

    fn transfer_to(&self, request: &WorkspaceClaimRequestWire) -> Self {
        let mut replacement = self.clone();
        replacement.pid = request.pid;
        replacement.workflow = request.workflow_name.clone();
        if !request.artifacts_timestamp.is_empty() {
            replacement
                .set_artifacts_timestamp(request.artifacts_timestamp.clone());
        }
        replacement
    }

    fn set_artifacts_timestamp(&mut self, value: String) {
        self.artifacts_timestamp = Some(value.clone());
        for part in &mut self.suffix_parts {
            if matches!(part, WorkspaceClaimSuffixPart::Timestamp(_)) {
                *part = WorkspaceClaimSuffixPart::Timestamp(value);
                return;
            }
        }
        let insert_idx = self
            .suffix_parts
            .iter()
            .position(|part| matches!(part, WorkspaceClaimSuffixPart::Pinned))
            .unwrap_or(self.suffix_parts.len());
        self.suffix_parts
            .insert(insert_idx, WorkspaceClaimSuffixPart::Timestamp(value));
    }

    fn into_wire(self) -> WorkspaceClaimWire {
        WorkspaceClaimWire {
            workspace_num: self.workspace_num,
            workflow: self.workflow,
            cl_name: self.cl_name,
            pid: self.pid,
            artifacts_timestamp: self.artifacts_timestamp,
            pinned: self.pinned,
        }
    }

    fn to_line(&self) -> String {
        let cl_part = self.cl_name.as_deref().unwrap_or("");
        let suffix = self
            .suffix_parts
            .iter()
            .map(WorkspaceClaimSuffixPart::raw_value)
            .collect::<Vec<_>>()
            .join(" | ");
        let suffix_part = if suffix.is_empty() {
            String::new()
        } else {
            format!(" | {suffix}")
        };
        format!(
            "  #{} | {} | {} | {}{}",
            self.workspace_num, self.pid, self.workflow, cl_part, suffix_part
        )
    }
}

fn is_timestamp_part(value: &str) -> bool {
    (value.len() == 14 && value.as_bytes().iter().all(u8::is_ascii_digit))
        || (value.len() == 15
            && value.as_bytes()[0..8].iter().all(u8::is_ascii_digit)
            && value.as_bytes()[8] == b'_'
            && value.as_bytes()[9..15].iter().all(u8::is_ascii_digit))
        || (value.len() == 13
            && value.as_bytes()[0..6].iter().all(u8::is_ascii_digit)
            && value.as_bytes()[6] == b'_'
            && value.as_bytes()[7..13].iter().all(u8::is_ascii_digit))
}

fn normalize_running_field_spacing(content: &str) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut result_lines = Vec::with_capacity(lines.len());
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        if line.starts_with("RUNNING:") {
            result_lines.push(line.to_string());
            i += 1;
            while i < lines.len() {
                let entry_line = lines[i];
                if entry_line.starts_with("  ")
                    && entry_line.trim().starts_with('#')
                {
                    result_lines.push(entry_line.to_string());
                    i += 1;
                } else {
                    break;
                }
            }
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            if i < lines.len() {
                result_lines.push(String::new());
                result_lines.push(String::new());
            }
        } else {
            result_lines.push(line.to_string());
            i += 1;
        }
    }

    result_lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(workspace_num: u32) -> WorkspaceClaimRequestWire {
        WorkspaceClaimRequestWire {
            project_file: "/tmp/project.sase".to_string(),
            workspace_num,
            workflow_name: "run".to_string(),
            pid: 222,
            cl_name: "demo".to_string(),
            artifacts_timestamp: String::new(),
            transfer_from_pid: None,
            pinned: false,
        }
    }

    #[test]
    fn launch_request_round_trips_json_shape() {
        let mut extra_env = BTreeMap::new();
        extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
        let request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "feature/test".to_string(),
            project_file: "/tmp/project.sase".to_string(),
            workspace_dir: "/tmp/ws".to_string(),
            workspace_num: 2,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: "p4head".to_string(),
            project_name: "proj".to_string(),
            history_sort_key: "feature/test".to_string(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: true,
            local_xprompts_file: Some("/tmp/xp.json".to_string()),
            extra_env,
            retry_transfer_from_pid: Some(10),
        };

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["extra_env"]["SASE_REPEAT_NAME"], json!("task.1"));
        let back: AgentLaunchRequestWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(back, request);
    }

    #[test]
    fn prepared_wire_preserves_null_claim_request() {
        let prepared = AgentLaunchPreparedWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            prompt_file: "/tmp/prompt.md".to_string(),
            output_path: "/tmp/out.txt".to_string(),
            safe_name: "home".to_string(),
            argv: vec!["python".to_string()],
            cwd: "/home/user".to_string(),
            env_delta: BTreeMap::new(),
            claim_request: None,
        };
        let value = serde_json::to_value(&prepared).unwrap();
        assert_eq!(value["claim_request"], json!(null));
        let back: AgentLaunchPreparedWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(back, prepared);
    }

    #[test]
    fn fanout_plan_round_trips_slots() {
        let plan = LaunchFanoutPlanWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            launch_kind: "repeat".to_string(),
            slots: vec![LaunchFanoutSlotWire {
                prompt: "%i:task.1\nfix it".to_string(),
                launch_kind: "repeat".to_string(),
                slot_index: 0,
                alt_id: None,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: Some("task.1".to_string()),
                bead_id: Some("sase-8f.2".to_string()),
                wait_for_previous: false,
            }],
            requires_sequential_naming_wait: false,
            fanout_sleep_seconds: 1.0,
        };
        let value = serde_json::to_value(&plan).unwrap();
        assert_eq!(value["slots"][0]["repeat_name"], json!("task.1"));
        assert_eq!(value["slots"][0]["bead_id"], json!("sase-8f.2"));
        assert_eq!(value["slots"][0]["alt_id"], json!(null));
        let back: LaunchFanoutPlanWire = serde_json::from_value(value).unwrap();
        assert_eq!(back, plan);
    }

    #[test]
    fn typed_launch_plan_builds_mixed_proc_agent_wait_graph() {
        let prompt = "%proc(\"just check\")\n---\n%wait\n%id:reviewer\n%model:opus\nReview";

        let plan =
            plan_typed_launch_units(prompt, Some("multi_prompt"), Some("sase"))
                .unwrap();

        assert_eq!(plan.schema_version, LAUNCH_PLAN_WIRE_SCHEMA_VERSION);
        assert_eq!(plan.launch_kind, "multi_prompt");
        assert_eq!(plan.units.len(), 2);
        match &plan.units[0].payload {
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                assert_eq!(proc_unit.code.source, "just check");
                assert_eq!(proc_unit.code.language, "bash");
                assert_eq!(proc_unit.selected_project.as_deref(), Some("sase"));
                assert!(proc_unit.workspace);
            }
            other => panic!("expected proc payload, got {other:?}"),
        }
        match &plan.units[1].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.identity.as_deref(), Some("reviewer"));
                assert_eq!(agent.model.as_deref(), Some("opus"));
                assert_eq!(agent.prompt, "Review");
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
        assert_eq!(
            plan.units[1].waits,
            vec![WaitTargetWire::Logical {
                logical_id: "unit-1".to_string(),
                source: Some("%wait".to_string())
            }]
        );
        assert!(plan.approval_preview[1].contains("proc"));
        assert_eq!(plan.content_digest.len(), 64);
    }

    #[test]
    fn typed_launch_plan_captures_if_fence_without_duplicate_form_error() {
        let prompt = "%if::\n\n```bash\ntest -f pyproject.toml\n```\nReview";

        let plan = plan_typed_launch_units(prompt, Some("auto"), Some("sase"))
            .unwrap();

        assert_eq!(plan.units.len(), 1);
        let condition =
            plan.units[0].condition.as_ref().expect("conditioned unit");
        assert_eq!(condition.code.language, "bash");
        assert!(condition.code.source.contains("test -f pyproject.toml"));
        match &plan.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.prompt, "Review");
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    }

    #[test]
    fn typed_launch_plan_rejects_bare_if_without_owned_fence() {
        let err = plan_typed_launch_units(
            "%if:true\nReview",
            Some("auto"),
            Some("sase"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("%if requires %if::"));
    }

    #[test]
    fn typed_launch_plan_keeps_fenced_proc_options() {
        let prompt = "%proc(timeout=\"20m\", cwd=\"docs\", workspace=\"true\")::\n\n```bash\njust docs-check\n```\n";

        let plan = plan_typed_launch_units(prompt, Some("auto"), Some("sase"))
            .unwrap();

        match &plan.units[0].payload {
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                assert_eq!(proc_unit.timeout.as_deref(), Some("20m"));
                assert_eq!(proc_unit.cwd.as_deref(), Some("docs"));
                assert!(proc_unit.workspace);
                assert!(proc_unit.code.source.contains("just docs-check"));
            }
            other => panic!("expected proc payload, got {other:?}"),
        }
    }

    #[test]
    fn typed_launch_plan_resolves_forward_proc_wait() {
        let prompt =
            "%wait(proc=build)\nReview\n---\n%id:build\n%proc(\"echo ready\")";

        let plan =
            plan_typed_launch_units(prompt, Some("multi_prompt"), Some("sase"))
                .unwrap();

        assert_eq!(
            plan.units[0].waits,
            vec![WaitTargetWire::Logical {
                logical_id: "unit-2".to_string(),
                source: Some("%wait(proc=build)".to_string())
            }]
        );
        match &plan.units[1].payload {
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                assert_eq!(proc_unit.shell_name.as_deref(), Some("build"));
            }
            other => panic!("expected proc payload, got {other:?}"),
        }
    }

    #[test]
    fn typed_launch_plan_rejects_agent_directives_on_proc() {
        let err = plan_typed_launch_units(
            "%model:opus\n%proc(\"just check\")",
            Some("auto"),
            Some("sase"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("not valid on %proc"));
        match err {
            AgentLaunchFanoutPlanError::TypedLaunchPlan { diagnostics } => {
                assert!(diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code
                        == "agent-directive-on-proc"));
            }
            other => panic!("expected typed launch diagnostic, got {other:?}"),
        }
    }

    #[test]
    fn typed_launch_plan_rejects_wait_cycles() {
        let err = plan_typed_launch_units(
            "%wait(unit=unit-2)\nFirst\n---\n%wait(unit=unit-1)\nSecond",
            Some("multi_prompt"),
            Some("sase"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn typed_launch_plan_validates_proc_project_policy() {
        let err = plan_typed_launch_units(
            "%proc(workspace=false)::\n```bash\njust check\n```",
            Some("auto"),
            None,
        )
        .unwrap_err();

        assert!(err.to_string().contains("requires an explicit cwd"));
    }

    #[test]
    fn agent_unit_legacy_json_defaults_to_plain_identity() {
        let value = json!({
            "prompt": "Review",
            "identity": "reviewer",
            "identity_explicit": true,
        });
        let agent: AgentUnitWire = serde_json::from_value(value).unwrap();
        assert_eq!(agent.identity.as_deref(), Some("reviewer"));
        assert!(agent.identity_explicit);
        assert!(!agent.identity_force_reuse);
        assert!(agent.clan.is_none());
        assert!(!agent.clan_declared);
        assert!(agent.clan_tribe.is_none());
        assert!(agent.clan_summary.is_none());
        assert!(agent.clan_summary_script.is_none());
        assert!(agent.family_attach_parent.is_none());
        assert!(agent.family_attach_suffix.is_none());
        assert!(agent.tribe.is_none());
        let serialized = serde_json::to_value(&agent).unwrap();
        assert!(serialized.get("clan").is_none());
        assert!(serialized.get("clan_declared").is_none());
        assert!(serialized.get("tribe").is_none());
    }

    #[test]
    fn agent_unit_identity_forms_round_trip_json() {
        let cases = [
            AgentUnitWire {
                prompt: "plain".to_string(),
                identity: Some("reviewer".to_string()),
                identity_explicit: true,
                ..Default::default()
            },
            AgentUnitWire {
                prompt: "join".to_string(),
                identity: Some("worker".to_string()),
                identity_explicit: true,
                clan: Some("research".to_string()),
                ..Default::default()
            },
            AgentUnitWire {
                prompt: "declare".to_string(),
                identity: Some("research.worker".to_string()),
                identity_explicit: true,
                clan: Some("research".to_string()),
                clan_declared: true,
                clan_tribe: Some("study".to_string()),
                clan_summary: Some("[bold]Research[/bold]".to_string()),
                clan_summary_script: None,
                ..Default::default()
            },
            AgentUnitWire {
                prompt: "family".to_string(),
                family_attach_parent: Some("parent".to_string()),
                family_attach_suffix: Some("reviewer".to_string()),
                ..Default::default()
            },
            AgentUnitWire {
                prompt: "tribe".to_string(),
                identity: Some("worker".to_string()),
                identity_explicit: true,
                tribe: Some("review".to_string()),
                ..Default::default()
            },
            AgentUnitWire {
                prompt: "auto-tribe".to_string(),
                tribe: Some("review".to_string()),
                ..Default::default()
            },
        ];
        for agent in cases {
            let value = serde_json::to_value(&agent).unwrap();
            let back: AgentUnitWire = serde_json::from_value(value).unwrap();
            assert_eq!(back, agent);
        }
    }

    #[test]
    fn typed_launch_plan_preserves_clan_declaration_and_join() {
        let prompt = "%id:toobig-3j.foo.0\n%clan(toobig-3j, tribe=chop, summary=[[ [bold]Large[/bold]\n  Split safely. ]])\nLead\n---\n%id(bar.0, clan=toobig-3j)\n%wait:toobig-3j.foo.0\nJoin";

        let plan =
            plan_typed_launch_units(prompt, Some("multi_prompt"), Some("sase"))
                .unwrap();

        match &plan.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.identity.as_deref(), Some("toobig-3j.foo.0"));
                assert!(agent.identity_explicit);
                assert_eq!(agent.clan.as_deref(), Some("toobig-3j"));
                assert!(agent.clan_declared);
                assert_eq!(agent.clan_tribe.as_deref(), Some("chop"));
                assert_eq!(
                    agent.clan_summary.as_deref(),
                    Some("[bold]Large[/bold]\nSplit safely.")
                );
                assert_eq!(agent.prompt, "Lead");
                assert_eq!(
                    agent.effective_identity().as_deref(),
                    Some("toobig-3j.foo.0")
                );
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
        match &plan.units[1].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.identity.as_deref(), Some("bar.0"));
                assert_eq!(agent.clan.as_deref(), Some("toobig-3j"));
                assert!(!agent.clan_declared);
                assert_eq!(
                    agent.effective_identity().as_deref(),
                    Some("toobig-3j.bar.0")
                );
                assert_eq!(agent.prompt, "Join");
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
        assert_eq!(
            plan.units[1].waits,
            vec![WaitTargetWire::Logical {
                logical_id: "unit-1".to_string(),
                source: Some("%wait:toobig-3j.foo.0".to_string())
            }]
        );
        let reconstructed =
            agent_unit_dispatch_prompt(match &plan.units[0].payload {
                LaunchUnitPayloadWire::Agent(agent) => agent,
                other => panic!("expected agent payload, got {other:?}"),
            });
        assert!(reconstructed.contains("%id:toobig-3j.foo.0"));
        assert!(reconstructed.contains("%clan(toobig-3j, tribe=chop"));
        assert!(!reconstructed.contains("%wait:"));
    }

    #[test]
    fn typed_launch_clan_summary_ignores_inner_text_block_marker() {
        let summary =
            "Use `[<web>:<keyword> [...]]` for example, then continue.\n\
Keep this comma, and the rest of the prose in the summary.";
        let prompt = format!(
            "%clan(research, tribe=study, summary=[[{summary}]])\nDo work"
        );
        let plan = plan_typed_launch_units(
            &prompt,
            Some("multi_prompt"),
            Some("sase"),
        )
        .unwrap();
        match &plan.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.clan.as_deref(), Some("research"));
                assert_eq!(agent.clan_tribe.as_deref(), Some("study"));
                assert_eq!(agent.clan_summary.as_deref(), Some(summary));
                assert_eq!(agent.prompt, "Do work");
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
    }

    #[test]
    fn typed_launch_clan_summary_keeps_unbalanced_inner_closer() {
        let summary = "note: use ]] here, and more";
        let prompt = format!(
            "%clan(research, tribe=study, summary=[[{summary}]])\nDo work"
        );
        let plan = plan_typed_launch_units(
            &prompt,
            Some("multi_prompt"),
            Some("sase"),
        )
        .unwrap();
        match &plan.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.clan.as_deref(), Some("research"));
                assert_eq!(agent.clan_tribe.as_deref(), Some("study"));
                assert_eq!(agent.clan_summary.as_deref(), Some(summary));
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
    }

    #[test]
    fn parse_directive_args_text_block_corpus_matches_python() {
        use std::collections::BTreeMap;

        for case in crate::xprompt_text_block::xprompt_args_corpus() {
            let parsed = parse_directive_args_with_names(&case.source, ',');
            let mut positional = Vec::new();
            let mut named = BTreeMap::new();
            for arg in parsed {
                if let Some(name) = arg.name {
                    named.insert(name, arg.value);
                } else {
                    positional.push(arg.value);
                }
            }
            assert_eq!(positional, case.positional, "{}", case.id);
            assert_eq!(named, case.named, "{}", case.id);
        }
    }

    #[test]
    fn typed_launch_plan_preserves_family_and_direct_tribe() {
        let family = plan_typed_launch_units(
            "%id(reviewer, family=parent)\nReview",
            Some("auto"),
            Some("sase"),
        )
        .unwrap();
        match &family.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(
                    agent.family_attach_parent.as_deref(),
                    Some("parent")
                );
                assert_eq!(
                    agent.family_attach_suffix.as_deref(),
                    Some("reviewer")
                );
                assert!(agent.identity.is_none());
                assert_eq!(
                    agent.effective_identity().as_deref(),
                    Some("parent--reviewer")
                );
            }
            other => panic!("expected agent payload, got {other:?}"),
        }

        let named_tribe = plan_typed_launch_units(
            "%id(worker, tribe=review)\nReview",
            Some("auto"),
            Some("sase"),
        )
        .unwrap();
        match &named_tribe.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert_eq!(agent.identity.as_deref(), Some("worker"));
                assert_eq!(agent.tribe.as_deref(), Some("review"));
            }
            other => panic!("expected agent payload, got {other:?}"),
        }

        let auto_tribe = plan_typed_launch_units(
            "%id(tribe=review)\nReview",
            Some("auto"),
            Some("sase"),
        )
        .unwrap();
        match &auto_tribe.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                assert!(agent.identity.is_none());
                assert!(!agent.identity_explicit);
                assert_eq!(agent.tribe.as_deref(), Some("review"));
            }
            other => panic!("expected agent payload, got {other:?}"),
        }
    }

    #[test]
    fn typed_launch_plan_rejects_conflicting_identity_forms() {
        let err = plan_typed_launch_units(
            "%clan:research\n%id(worker, clan=research)\nDo work",
            Some("auto"),
            Some("sase"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("Cannot combine %clan with %id"));

        let err = plan_typed_launch_units(
            "%id(worker, clan=research, tribe=review)\nDo work",
            Some("auto"),
            Some("sase"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));

        let err = plan_typed_launch_units(
            "%clan(foo, color=blue)\nDo work",
            Some("auto"),
            Some("sase"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("Unsupported keyword on %clan"));
    }

    #[test]
    fn timestamp_batch_allocates_unique_visible_timestamps() {
        let timestamps =
            allocate_launch_timestamp_batch(3, "260501_120000", None).unwrap();

        assert_eq!(
            timestamps,
            vec!["260501_120000", "260501_120001", "260501_120002"]
        );
    }

    #[test]
    fn timestamp_batch_starts_after_previous_allocation() {
        let timestamps = allocate_launch_timestamp_batch(
            2,
            "260501_120000",
            Some("260501_120005"),
        )
        .unwrap();

        assert_eq!(timestamps, vec!["260501_120006", "260501_120007"]);
    }

    #[test]
    fn timestamp_batch_rejects_invalid_format() {
        let err = allocate_launch_timestamp_batch(1, "not-a-timestamp", None)
            .unwrap_err();

        assert!(err.to_string().contains("expected YYmmdd_HHMMSS"));
    }

    #[test]
    fn prepare_agent_launch_writes_prompt_and_shapes_process_data() {
        let tmp = tempfile::tempdir().unwrap();
        let prompt_dir = tmp.path().join("prompts");
        std::fs::create_dir(&prompt_dir).unwrap();
        let output_root = tmp.path().join("workflows").join("202605");
        let mut extra_env = BTreeMap::new();
        extra_env.insert("SASE_AGENT".to_string(), "caller".to_string());
        extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
        let request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "feature/test".to_string(),
            project_file: "/tmp/project.sase".to_string(),
            workspace_dir: "/tmp/ws".to_string(),
            workspace_num: 4,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: "p4head".to_string(),
            project_name: "proj".to_string(),
            history_sort_key: "feature/test".to_string(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: false,
            local_xprompts_file: Some("/tmp/xprompts.json".to_string()),
            extra_env,
            retry_transfer_from_pid: Some(99),
        };
        let mut preallocated = BTreeMap::new();
        preallocated.insert("GH_PRE_ALLOCATED".to_string(), "1".to_string());
        preallocated.insert("GH_WORKSPACE_NUM".to_string(), "4".to_string());

        let prepared = prepare_agent_launch(
            &request,
            "/venv/bin/python",
            "/repo/run_agent_runner.py",
            Some(prompt_dir.to_str().unwrap()),
            output_root.to_str().unwrap(),
            &preallocated,
        )
        .unwrap();

        assert_eq!(prepared.safe_name, "feature_test");
        assert_eq!(
            std::fs::read_to_string(&prepared.prompt_file).unwrap(),
            "fix it"
        );
        assert!(prepared
            .prompt_file
            .starts_with(prompt_dir.to_str().unwrap()));
        assert_eq!(
            prepared.output_path,
            output_root
                .join("feature_test_ace-run-260501_120000.txt")
                .to_string_lossy()
        );
        assert_eq!(prepared.argv[0], "/venv/bin/python");
        assert_eq!(prepared.argv[2], "feature/test");
        assert_eq!(prepared.argv[5], prepared.output_path);
        assert_eq!(prepared.argv[8], prepared.prompt_file);
        assert_eq!(prepared.env_delta["SASE_AGENT"], "1");
        assert_eq!(prepared.env_delta["SASE_REPEAT_NAME"], "task.1");
        assert_eq!(prepared.env_delta["GH_PRE_ALLOCATED"], "1");
        assert_eq!(
            prepared.env_delta["SASE_AGENT_LOCAL_XPROMPTS"],
            "/tmp/xprompts.json"
        );
        assert!(!prepared
            .env_delta
            .contains_key("SASE_AGENT_VCS_WORKFLOW_TYPE"));
        assert_eq!(prepared.claim_request.unwrap().transfer_from_pid, Some(99));
    }

    #[test]
    fn prepare_agent_launch_deferred_and_home_claim_shapes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "home".to_string(),
            project_file: "/tmp/home.sase".to_string(),
            workspace_dir: "/home/me".to_string(),
            workspace_num: 9,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: String::new(),
            project_name: String::new(),
            history_sort_key: String::new(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: true,
            local_xprompts_file: None,
            extra_env: BTreeMap::new(),
            retry_transfer_from_pid: None,
        };

        let deferred = prepare_agent_launch(
            &request,
            "python",
            "runner.py",
            None,
            tmp.path().to_str().unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(deferred.claim_request.unwrap().workspace_num, 0);
        assert_eq!(deferred.env_delta["SASE_AGENT_DEFERRED_WORKSPACE"], "1");
        assert_eq!(deferred.env_delta["SASE_AGENT_VCS_WORKFLOW_TYPE"], "gh");

        request.is_home_mode = true;
        let home = prepare_agent_launch(
            &request,
            "python",
            "runner.py",
            None,
            tmp.path().to_str().unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(home.claim_request.is_none());
        assert_eq!(home.argv[13], "1");
    }

    #[test]
    fn workspace_claims_parse_valid_rows_and_ignore_malformed() {
        let content = "RUNNING:\n  #0 | 111 | wait | deferred | 20260501120000 | PINNED\n  #bad | nope\n  #2 | 222 | run | demo\n\n\nNAME: demo\n";

        let claims = list_workspace_claims_from_content(content);

        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].workspace_num, 0);
        assert_eq!(
            claims[0].artifacts_timestamp.as_deref(),
            Some("20260501120000")
        );
        assert!(claims[0].pinned);
        assert_eq!(claims[1].workspace_num, 2);
    }

    #[test]
    fn workspace_claims_keep_suffix_corrupt_rows_occupied() {
        let content = "RUNNING:\n  #10 | 111 | run | demo | 20260820_121314 | LEGACY=bad | PINNED\n\n\nNAME: demo\n";

        let claims = list_workspace_claims_from_content(content);
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].workspace_num, 10);
        assert_eq!(
            claims[0].artifacts_timestamp.as_deref(),
            Some("20260820_121314")
        );
        assert!(claims[0].pinned);

        let duplicate =
            plan_claim_workspace_from_content(content, &request(10));
        assert!(!duplicate.outcome.success);
        assert!(!duplicate.changed);

        let allocated = allocate_and_claim_workspace_from_content(
            content,
            10,
            11,
            &request(0),
        );
        assert!(allocated.outcome.success);
        assert_eq!(allocated.outcome.workspace_num, 11);
        assert!(allocated.content.contains("#11 | 222 | run | demo"));
    }

    #[test]
    fn claim_workspace_rejects_duplicate_nonzero_but_allows_zero() {
        let content = "RUNNING:\n  #2 | 111 | run | demo\n\n\nNAME: demo\n";

        let duplicate = plan_claim_workspace_from_content(content, &request(2));
        assert!(!duplicate.outcome.success);
        assert!(!duplicate.changed);

        let zero = plan_claim_workspace_from_content(content, &request(0));
        assert!(zero.outcome.success);
        assert!(zero.content.contains("#0 | 222 | run | demo"));
    }

    #[test]
    fn allocate_and_claim_picks_first_available_workspace() {
        let content = "RUNNING:\n  #100 | 111 | run | a\n  #102 | 333 | run | c\n\n\nNAME: demo\n";
        let mut req = request(0);
        req.cl_name = "b".to_string();
        req.artifacts_timestamp = "20260501120000".to_string();
        req.pinned = true;

        let plan =
            allocate_and_claim_workspace_from_content(content, 100, 102, &req);

        assert!(plan.outcome.success);
        assert_eq!(plan.outcome.workspace_num, 101);
        assert!(plan
            .content
            .contains("#101 | 222 | run | b | 20260501120000 | PINNED"));
    }

    #[test]
    fn transfer_workspace_claim_matches_pid_and_preserves_claim_name() {
        let content = "RUNNING:\n  #101 | 111 | run | demo | 20260501115959\n\n\nNAME: demo\n";
        let mut req = request(101);
        req.workflow_name = "run-retry".to_string();
        req.artifacts_timestamp = "20260501120000".to_string();
        req.transfer_from_pid = Some(111);

        let plan = plan_transfer_workspace_claim_from_content(content, &req);

        assert!(plan.outcome.success);
        assert!(plan
            .content
            .contains("#101 | 222 | run-retry | demo | 20260501120000"));
    }

    #[test]
    fn transfer_workspace_claim_preserves_unknown_suffix_fields() {
        let content = "RUNNING:\n  #101 | 111 | run | demo | 20260820_121314 | LEGACY=bad | PINNED | extra\n\n\nNAME: demo\n";
        let mut req = request(101);
        req.workflow_name = "run-retry".to_string();
        req.artifacts_timestamp = "20260820121516".to_string();
        req.transfer_from_pid = Some(111);

        let plan = plan_transfer_workspace_claim_from_content(content, &req);

        assert!(plan.outcome.success);
        assert!(plan.content.contains(
            "#101 | 222 | run-retry | demo | 20260820121516 | LEGACY=bad | PINNED | extra"
        ));
    }

    #[test]
    fn fanout_planner_splits_multi_prompt_outside_fences() {
        let prompt = "one\n```\n---\n```\n---\n%wait\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        assert_eq!(plan.launch_kind, "multi_prompt");
        assert_eq!(plan.slots.len(), 2);
        assert!(plan.slots[0].prompt.contains("---"));
        assert_eq!(plan.slots[1].prompt, "%wait\ntwo");
        assert!(plan.slots[1].wait_for_previous);
    }

    #[test]
    fn fanout_planner_time_waits_defer_workspace() {
        let prompt = "%wait(time=5m)\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        // `%time` is no longer an advertised directive. The time floor now
        // travels through `%wait(time=...)`, which still marks the slot as
        // deferred for workspace allocation.
        // `%tribe` and `%t` are removed identity directives and stay raw.
        assert_eq!(canonical_directive_name("tribe"), "tribe");
        assert_eq!(canonical_directive_name("t"), "t");
        assert_eq!(canonical_directive_name("time"), "time");
        assert_eq!(canonical_directive_name("c"), "clan");
        assert_eq!(canonical_directive_name("f"), "f");
        assert_eq!(canonical_directive_name("g"), "g");
        // `%edit` was removed and stays a non-special raw name, but `%e` is now
        // the `%effort` alias, so the launch planner canonicalizes it.
        assert_eq!(canonical_directive_name("edit"), "edit");
        assert_eq!(canonical_directive_name("e"), "effort");
        assert_eq!(plan.slots.len(), 1);
        assert!(plan.slots[0].wait_for_previous);
    }

    #[test]
    fn fanout_planner_t_xprompt_defer_workspace() {
        let prompt = "#t:5m\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        assert_eq!(plan.slots.len(), 1);
        assert!(plan.slots[0].wait_for_previous);
    }

    #[test]
    fn fanout_planner_ignores_wait_forms_inside_adjacent_inline_code() {
        for prompt in [
            "keep `foo`/`%wait` and `#t:5m` literal",
            "prefix`%wait(time=5m)`suffix",
            "bare #t is not a time reference",
        ] {
            let plan =
                plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();
            assert!(!plan.slots[0].wait_for_previous, "prompt was {prompt:?}");
        }

        for prompt in ["#t:`5m` active", "%wait(time=`5m`) active"] {
            let plan =
                plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();
            assert!(plan.slots[0].wait_for_previous, "prompt was {prompt:?}");
        }
    }

    #[test]
    fn fanout_planner_deprecated_time_directive_is_not_special() {
        let prompt = "%time:5m\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        assert_eq!(plan.slots.len(), 1);
        assert!(!plan.slots[0].wait_for_previous);
    }

    #[test]
    fn fanout_planner_preserves_named_alt_ids_and_values_only() {
        let prompt = "%alt(sec=[[security]],perf=[[performance]])\nReview";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.launch_kind, "alternatives");
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("sec"));
        assert_eq!(plan.slots[0].prompt, "security\nReview");
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("perf"));
        assert_eq!(plan.slots[1].prompt, "performance\nReview");
    }

    #[test]
    fn fanout_planner_allocates_unnamed_alt_ids_after_named_ids() {
        let prompt = "%(fast=a,b,2=c,d)";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("fast"), Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
    }

    #[test]
    fn fanout_planner_composes_cartesian_alt_ids() {
        let prompt = "%alt(left=a,right=b) %alt(red=x,blue=y)";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("left.red"),
                Some("left.blue"),
                Some("right.red"),
                Some("right.blue")
            ]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a x", "a y", "b x", "b y"]
        );
    }

    #[test]
    fn fanout_planner_correlates_shared_named_alt_keys() {
        let prompt =
            "#gh:sase %{a=Describe | b=Explain} how this repo works %{a=in detail}.";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "#gh:sase Describe how this repo works in detail.",
                "#gh:sase Explain how this repo works."
            ]
        );
    }

    #[test]
    fn fanout_planner_correlates_transitive_alt_keys() {
        let prompt = "%{a=1|b=2} x %{a=3} y %{a=4|b=5}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["1 x 3 y 4", "2 x y 5"]
        );
    }

    #[test]
    fn fanout_planner_single_shared_key_collapses_to_one_slot() {
        let prompt = "%{a=X} %{a=Y}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 1);
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("a"));
        assert_eq!(plan.slots[0].prompt, "X Y");
    }

    #[test]
    fn fanout_planner_cartesian_products_independent_correlated_groups() {
        let prompt = "%{a=A | b=B} %{x=X | y=Y} %{a=C} %{x=Z}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a.x"), Some("a.y"), Some("b.x"), Some("b.y")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["A X C Z", "A Y C", "B X Z", "B Y"]
        );
    }

    #[test]
    fn fanout_planner_correlated_group_mixes_named_and_unnamed_ids() {
        let prompt = "%{a=X | Y} %{a=Z}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("1")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["X Z", "Y"]
        );
    }

    #[test]
    fn fanout_planner_rejects_repeated_top_level_models_with_alternatives() {
        let prompt = "%id:foo\n%model:opus\n%model:sonnet %alt(x,y)\nReview";

        let err = plan_agent_launch_fanout(prompt, Some("model")).unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%model:opus ... %model:sonnet"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_splits_model_branches_and_alternatives() {
        let prompt = "%id:foo\n%{%m:opus | %m:sonnet} %alt(x,y)\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 4);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("1.1"));
        assert!(plan.slots[0].prompt.contains("%m:opus x\nReview"));
        assert_eq!(plan.slots[3].model.as_deref(), Some("sonnet"));
        assert_eq!(plan.slots[3].alt_id.as_deref(), Some("2.2"));
        assert!(plan.slots[3].prompt.contains("%m:sonnet y\nReview"));
    }

    #[test]
    fn extract_first_model_value_strips_known_effort_suffix() {
        // A trailing `@<known-effort>` is peeled off so the slot is named by
        // the clean model, mirroring the Python `split_model_effort` rule.
        assert_eq!(
            extract_first_model_value("%model:opus@xhigh do work"),
            Some("opus".to_string())
        );
        assert_eq!(
            extract_first_model_value("%m:codex/gpt-5.6-sol@low do work"),
            Some("codex/gpt-5.6-sol".to_string())
        );
        // No suffix → unchanged.
        assert_eq!(
            extract_first_model_value("%model:opus do work"),
            Some("opus".to_string())
        );
        // Unknown trailing token is not an effort level → left intact.
        assert_eq!(
            extract_first_model_value("%model:agy/flash@v2 do work"),
            Some("agy/flash@v2".to_string())
        );
        // Backtick-literal model values keep any `@` verbatim.
        assert_eq!(
            extract_first_model_value("%model:`agy/flash@xhigh` do work"),
            Some("agy/flash@xhigh".to_string())
        );
    }

    #[test]
    fn fanout_planner_strips_branch_effort_for_slot_naming() {
        // Per-branch `@effort` fan-out: slots are named by the clean model
        // while each branch body retains its `@effort` token for the launched
        // agent's own directive parsing.
        let prompt = "%{%m:opus@xhigh | %m:sonnet@low} %alt(x,y)\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 4);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert!(plan.slots[0].prompt.contains("%m:opus@xhigh"));
        assert_eq!(plan.slots[3].model.as_deref(), Some("sonnet"));
        assert!(plan.slots[3].prompt.contains("%m:sonnet@low"));
    }

    #[test]
    fn fanout_planner_model_alt_ids_preserve_named_model_branches() {
        let prompt = "%alt(opus=%model:opus,sonnet=%model:sonnet)\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("opus"));
        assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("sonnet"));
    }

    #[test]
    fn fanout_planner_ignores_models_inside_adjacent_inline_code() {
        let prompt =
            "keep `foo`/`%m:wrong` then %{left=%m:opus | right=%m:sonnet}";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
        assert!(plan
            .slots
            .iter()
            .all(|slot| slot.prompt.contains("`%m:wrong`")));
    }

    #[test]
    fn launch_inline_scanner_preserves_argument_parser_precedence() {
        let prompt = concat!(
            "#name:`arg with spaces` #research(compare `a` and `b`) ",
            "%model:`custom model` %wait(time=`5m`)"
        );

        assert!(launch_inline_literal_ranges(prompt).is_empty());
    }

    #[test]
    fn fanout_planner_extracts_repeat_slots() {
        for prompt in [
            "%repeat:3 %id:task %model:opus do work",
            "%r:3 %i:task %model:opus do work",
        ] {
            let plan =
                plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

            assert_eq!(plan.launch_kind, "repeat");
            assert_eq!(plan.slots.len(), 3);
            assert_eq!(plan.slots[0].repeat_name.as_deref(), Some("task"));
            assert_eq!(plan.slots[0].prompt, "  %model:opus do work");
            assert!(!plan.slots[0].wait_for_previous);
            assert!(plan.slots[1].wait_for_previous);
        }
    }

    #[test]
    fn fanout_planner_preserves_repeat_bead_association() {
        for prompt in [
            "%repeat:2 %id(task, bead=sase-8f.2) do work",
            "%r:2 %i(bead=sase-8f.2) do work",
        ] {
            let plan =
                plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

            assert_eq!(plan.slots.len(), 2);
            assert!(plan
                .slots
                .iter()
                .all(|slot| slot.bead_id.as_deref() == Some("sase-8f.2")));
            assert!(plan
                .slots
                .iter()
                .all(|slot| !slot.prompt.contains("bead=sase-8f.2")));
        }

        let named = plan_agent_launch_fanout(
            "%r:2 %id(task, clan=research, bead=`sase-8f.2`) do work",
            Some("repeat"),
        )
        .unwrap();
        assert_eq!(named.slots[0].repeat_name.as_deref(), Some("task"));
        assert_eq!(named.slots[0].bead_id.as_deref(), Some("sase-8f.2"));
    }

    #[test]
    fn fanout_planner_preserves_repeat_and_id_inside_literal_zones() {
        let prompt = concat!(
            "%xprompts_enabled:false\n",
            "%r:9 %i:disabled\n",
            "%xprompts_enabled:true\n",
            "```text\n%repeat:8 %id:fenced\n```\n",
            "keep `foo`/`%r:7` and prefix`%i:inline`suffix ",
            "%r:2 %id:right work",
        );

        let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].repeat_name.as_deref(), Some("right"));
        assert!(plan.slots[0].prompt.contains("%r:9 %i:disabled"));
        assert!(plan.slots[0]
            .prompt
            .contains("```text\n%repeat:8 %id:fenced\n```"));
        assert!(plan.slots[0].prompt.contains("`%r:7`"));
        assert!(plan.slots[0].prompt.contains("`%i:inline`"));
    }

    #[test]
    fn fanout_planner_does_not_support_removed_name_spellings() {
        let prompt = "%r:2 %name:legacy %n:short work";

        let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].repeat_name, None);
        assert!(plan.slots[0].prompt.contains("%name:legacy"));
        assert!(plan.slots[0].prompt.contains("%n:short"));
        assert_eq!(canonical_directive_name("name"), "name");
        assert_eq!(canonical_directive_name("n"), "n");
    }

    #[test]
    fn fanout_planner_brace_shorthand_splits_pipe_branches() {
        let prompt = "%{a | b | c}\nReview";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.launch_kind, "alternatives");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a\nReview", "b\nReview", "c\nReview"]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
    }

    #[test]
    fn fanout_planner_ignores_alternative_inside_adjacent_inline_code() {
        let prompt = "keep `foo`/`%{a | b}` then %{x | y}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].prompt, "keep `foo`/`%{a | b}` then x");
        assert_eq!(plan.slots[1].prompt, "keep `foo`/`%{a | b}` then y");
    }

    #[test]
    fn fanout_planner_brace_branch_text_keeps_commas() {
        let prompt = "%{foo, bar | baz}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].prompt, "foo, bar");
        assert_eq!(plan.slots[1].prompt, "baz");
    }

    #[test]
    fn fanout_planner_brace_named_and_numeric_branch_ids() {
        let prompt = "%{fast=a | b | 2=c | d}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("fast"), Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
    }

    #[test]
    fn fanout_planner_brace_named_text_blocks() {
        let prompt = "%{sec=[[security]] | perf=[[performance]]}\nReview";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("sec"));
        assert_eq!(plan.slots[0].prompt, "security\nReview");
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("perf"));
        assert_eq!(plan.slots[1].prompt, "performance\nReview");
    }

    #[test]
    fn fanout_planner_brace_single_branch_has_implicit_empty_variant() {
        let prompt = "before %{a} after";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].prompt, "before a after");
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("1"));
        assert_eq!(plan.slots[1].prompt, "before after");
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("2"));
    }

    #[test]
    fn fanout_planner_empty_branch_removes_space_before_punctuation() {
        let prompt = "works %{extra}.";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["works extra.", "works."]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_collapses_between_words() {
        let prompt = "A %{extra} B";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["A extra B", "A B"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_removes_leading_space() {
        let prompt = "%{extra} Review";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["extra Review", "Review"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_removes_trailing_space() {
        let prompt = "Review %{extra}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["Review extra", "Review"]
        );
    }

    #[test]
    fn render_alternative_prompt_empty_branch_does_not_invent_space() {
        let prompt = "A%B";
        let directives = vec![AlternativeDirective {
            start: 1,
            end: 2,
            args: Vec::new(),
        }];
        let combination = vec![AlternativeVariant {
            id: "empty".to_string(),
            replacements: vec![AlternativeReplacement {
                directive_index: 0,
                value: String::new(),
            }],
        }];

        assert_eq!(
            render_alternative_prompt(prompt, &directives, &combination),
            "AB"
        );
    }

    #[test]
    fn fanout_planner_empty_branch_collapses_multiple_spaces() {
        let prompt = "A  %{extra}  B";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["A  extra  B", "A B"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_preserves_newlines_and_indentation() {
        let prompt = "Header\n  %{extra}\n  Footer";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["Header\n  extra\n  Footer", "Header\n  \n  Footer"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_preserves_following_directive_separator() {
        let prompt = "Do work. %{extra} %{%m:opus | %m:gpt-5.6-sol}";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("opus"),
                Some("gpt-5.6-sol"),
                Some("opus"),
                Some("gpt-5.6-sol")
            ]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "Do work. extra %m:opus",
                "Do work. extra %m:gpt-5.6-sol",
                "Do work. %m:opus",
                "Do work. %m:gpt-5.6-sol",
            ]
        );
    }

    #[test]
    fn fanout_planner_brace_nested_pipes_do_not_split() {
        let prompt = "%{a (x | y) | b [c | d] | `e | f`}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a (x | y)", "b [c | d]", "e | f"]
        );
    }

    #[test]
    fn fanout_planner_brace_composes_cartesian_with_paren_alt() {
        let prompt = "%{a | b} %alt(x,y)";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a x", "a y", "b x", "b y"]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1.1"), Some("1.2"), Some("2.1"), Some("2.2")]
        );
    }

    #[test]
    fn fanout_planner_brace_model_branches_match_paren_parity() {
        let prompt = "%{opus=%model:opus | sonnet=%model:sonnet}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("opus"));
        assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("sonnet"));
    }

    #[test]
    fn fanout_planner_brace_value_fanout_after_directive_colon() {
        let prompt = "%m:opus %effort:%{medium | high | xhigh}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("opus"), Some("opus")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "%m:opus %effort:medium\nReview",
                "%m:opus %effort:high\nReview",
                "%m:opus %effort:xhigh\nReview",
            ]
        );
    }

    #[test]
    fn fanout_planner_brace_value_fanout_after_effort_e_alias() {
        // `%e:%{...}` fans out exactly like `%effort:%{...}`; the alias prefix
        // is preserved verbatim in each slot body.
        let prompt = "%m:opus %e:%{medium | high | xhigh}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("opus"), Some("opus")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "%m:opus %e:medium\nReview",
                "%m:opus %e:high\nReview",
                "%m:opus %e:xhigh\nReview",
            ]
        );
    }

    #[test]
    fn fanout_planner_model_value_fanout_after_directive_colon() {
        let prompt = "%m:%{opus | sonnet}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("sonnet")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["%m:opus\nReview", "%m:sonnet\nReview"]
        );
    }

    #[test]
    fn fanout_planner_value_fanouts_compose_cartesian() {
        let prompt = "%m:%{opus | sonnet} %effort:%{medium | high}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("opus"), Some("sonnet"), Some("sonnet")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1.1"), Some("1.2"), Some("2.1"), Some("2.2")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "%m:opus %effort:medium\nReview",
                "%m:opus %effort:high\nReview",
                "%m:sonnet %effort:medium\nReview",
                "%m:sonnet %effort:high\nReview",
            ]
        );
    }

    #[test]
    fn fanout_planner_rejects_repeated_models_with_brace_alternatives() {
        let prompt = "%model:opus\n%model:sonnet %{x | y}\nReview";

        let err = plan_agent_launch_fanout(prompt, Some("model")).unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%model:opus ... %model:sonnet"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_rejects_paren_multi_model_directive() {
        let err =
            plan_agent_launch_fanout("%m(opus,sonnet) review", Some("model"))
                .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%m(opus,sonnet) is no longer supported"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_rejects_repeated_top_level_model_directives() {
        let err = plan_agent_launch_fanout(
            "%model:opus\n%model:sonnet\nreview",
            Some("model"),
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%model:opus ... %model:sonnet"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_rejects_same_value_repeated_model_directives() {
        let err = plan_agent_launch_fanout(
            "%model:opus\n%model:opus\nreview",
            Some("model"),
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("use %{%m:opus | %m:opus} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_single_top_level_model_is_single_launch() {
        for prompt in ["%m:opus review", "%model(opus) review"] {
            let plan = plan_agent_launch_fanout(prompt, Some("auto")).unwrap();

            assert_eq!(plan.launch_kind, "single");
            assert_eq!(plan.slots.len(), 1);
            assert_eq!(plan.slots[0].prompt, prompt);
        }
    }

    #[test]
    fn fanout_planner_brace_model_branches_report_model_slots() {
        let prompt = "%{%m:opus | %m:sonnet}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("sonnet")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2")]
        );
    }

    #[test]
    fn fanout_planner_unvalued_model_markers_do_not_count_as_repeated() {
        let prompt = "%model\n%model()\n%model+ %{x | y}";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 2);
    }

    #[test]
    fn fanout_planner_unclosed_brace_reports_missing_close() {
        let err = plan_agent_launch_fanout("%{a | b", Some("alternatives"))
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("%{"), "message was {message:?}");
        assert!(message.contains('}'), "message was {message:?}");
    }

    fn occupancy_caller(pid: u32) -> OccupancyCallerWire {
        OccupancyCallerWire {
            pid,
            workspace_num: 17,
            project: "sase".to_string(),
            workflow: "ace(run)-260818_120000".to_string(),
            artifacts_timestamp: Some("20260818T120000".to_string()),
        }
    }

    fn occupant(pid: u32) -> OccupantRecordWire {
        OccupantRecordWire {
            pid,
            artifacts_timestamp: Some("20260818T115900".to_string()),
            agent_name: Some("06e--plan".to_string()),
            workflow: "ace(run)-260818_115900".to_string(),
            project: "sase".to_string(),
            workspace_num: 17,
            cl_name: Some("demo".to_string()),
            claimed_at: 1_755_000_000.0,
        }
    }

    fn claim(pid: u32) -> WorkspaceClaimWire {
        WorkspaceClaimWire {
            workspace_num: 17,
            workflow: "ace(run)-260818_115900".to_string(),
            cl_name: Some("demo".to_string()),
            pid,
            artifacts_timestamp: Some("20260818T115900".to_string()),
            pinned: false,
        }
    }

    #[test]
    fn occupancy_proceeds_when_no_occupant_record() {
        let decision = decide_workspace_occupant_conflict(
            None,
            &occupancy_caller(500),
            false,
            Some(&claim(999)),
            true,
        );
        assert!(decision.may_proceed);
        assert!(!decision.conflict);
    }

    #[test]
    fn occupancy_proceeds_when_occupant_is_caller() {
        let decision = decide_workspace_occupant_conflict(
            Some(&occupant(500)),
            &occupancy_caller(500),
            true,
            Some(&claim(500)),
            true,
        );
        assert!(decision.may_proceed);
        assert!(!decision.conflict);
    }

    #[test]
    fn occupancy_proceeds_when_occupant_pid_is_dead() {
        let decision = decide_workspace_occupant_conflict(
            Some(&occupant(111)),
            &occupancy_caller(500),
            false,
            None,
            false,
        );
        assert!(decision.may_proceed);
        assert!(!decision.conflict);
    }

    #[test]
    fn occupancy_refuses_when_occupant_is_live_other_pid() {
        let decision = decide_workspace_occupant_conflict(
            Some(&occupant(111)),
            &occupancy_caller(500),
            true,
            Some(&claim(111)),
            true,
        );
        assert!(!decision.may_proceed);
        assert!(decision.conflict);
        assert!(decision.reason.contains("06e--plan"));
        assert!(decision.reason.contains("111"));
    }

    #[test]
    fn occupancy_refuses_when_running_field_disagrees_with_dead_occupant() {
        let decision = decide_workspace_occupant_conflict(
            Some(&occupant(111)),
            &occupancy_caller(500),
            false,
            Some(&claim(222)),
            true,
        );
        assert!(!decision.may_proceed);
        assert!(decision.conflict);
        assert!(decision.reason.contains("222"));
    }

    #[test]
    fn occupancy_refuses_and_flags_disagreement_when_running_field_missing() {
        let decision = decide_workspace_occupant_conflict(
            Some(&occupant(111)),
            &occupancy_caller(500),
            true,
            None,
            false,
        );
        assert!(!decision.may_proceed);
        assert!(decision.conflict);
        assert!(decision.reason.contains("disagree"));
    }

    #[test]
    fn occupancy_refuses_and_flags_disagreement_when_running_pid_differs() {
        let decision = decide_workspace_occupant_conflict(
            Some(&occupant(111)),
            &occupancy_caller(500),
            true,
            Some(&claim(333)),
            true,
        );
        assert!(!decision.may_proceed);
        assert!(decision.conflict);
        assert!(decision.reason.contains("disagree"));
    }
}
