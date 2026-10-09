//! `%auto` E1 autonomy record wires: one persisted, revisioned record per
//! agent that every automatic gate outcome derives from.
//!
//! The four compatibility profiles (`manual`, `standard`, `tale`, `epic`)
//! are built in and not configurable in E1; config profiles arrive in E3.
//! `overrides` is always empty in E1 and `deny` is reserved for E3.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Core wire schema version for every autonomy record this module owns.
pub const AUTONOMY_WIRE_SCHEMA_VERSION: u32 = 1;

/// Profile with no automatic behavior.
pub const AUTONOMY_PROFILE_MANUAL: &str = "manual";
/// Profile selected by bare `%auto` (plus `%a`, `%auto+`, `%auto:true`).
pub const AUTONOMY_PROFILE_STANDARD: &str = "standard";
/// Profile selected by `%auto:tale` and `%auto:plan`.
pub const AUTONOMY_PROFILE_TALE: &str = "tale";
/// Profile selected by `%auto:epic`.
pub const AUTONOMY_PROFILE_EPIC: &str = "epic";

/// Gate policy values: `ask` parks, the rest name an automatic selection.
pub const AUTONOMY_VALUE_ASK: &str = "ask";
/// Approve a tale plan and archive it, then implement.
pub const AUTONOMY_VALUE_APPROVE_ARCHIVE: &str = "approve_archive";
/// Approve an epic plan and launch its clan.
pub const AUTONOMY_VALUE_APPROVE: &str = "approve";
/// Take the first question option.
pub const AUTONOMY_VALUE_FIRST: &str = "first";

/// E1 parks on ask; `deny` arrives in E3.
pub const AUTONOMY_ON_ASK_PARK: &str = "park";

/// Record sources.
pub const AUTONOMY_SOURCE_PROMPT: &str = "prompt";
pub const AUTONOMY_SOURCE_TUI: &str = "tui";
pub const AUTONOMY_SOURCE_CLI: &str = "cli";
pub const AUTONOMY_SOURCE_INHERITED: &str = "inherited";
pub const AUTONOMY_SOURCE_LEGACY: &str = "legacy";

/// Per-kind automatic selections for one profile.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyGatesWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
}

/// The policy block of an autonomy record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyPolicyWire {
    #[serde(default)]
    pub gates: AutonomyGatesWire,
    #[serde(default = "default_on_ask")]
    pub on_ask: String,
}

fn default_on_ask() -> String {
    AUTONOMY_ON_ASK_PARK.to_string()
}

impl Default for AutonomyPolicyWire {
    fn default() -> Self {
        Self {
            gates: AutonomyGatesWire::default(),
            on_ask: default_on_ask(),
        }
    }
}

/// The actor credited with a record write, modeled on the goal ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyActorWire {
    /// `human`, `agent`, or `host`.
    #[serde(default)]
    pub kind: String,
    /// The surface that applied the write (TUI toggle, CLI, prompt...).
    #[serde(default)]
    pub surface: String,
    /// Dotted `username.machine` principal, or the agent name.
    #[serde(default)]
    pub principal: String,
}

impl Default for AutonomyActorWire {
    fn default() -> Self {
        Self {
            kind: "human".to_string(),
            surface: String::new(),
            principal: String::new(),
        }
    }
}

/// The last non-manual profile and selection, used by the `A` restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyLastWire {
    #[serde(default)]
    pub profile: String,
    #[serde(default)]
    pub selection: String,
}

/// One persisted autonomy record (`agent_meta.autonomy`, core-owned wire).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyRecordWire {
    #[serde(default = "default_autonomy_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub profile: String,
    /// The text after `%auto` that produced the profile (`""`, `plan`,
    /// `tale`, `epic`, or `manual`), kept so legacy projections and
    /// `explain` stay exact.
    #[serde(default)]
    pub selection: String,
    #[serde(default)]
    pub policy: AutonomyPolicyWire,
    /// Always empty in E1.
    #[serde(default)]
    pub overrides: BTreeMap<String, Value>,
    #[serde(default)]
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<AutonomyLastWire>,
    #[serde(default = "default_revision")]
    pub revision: u64,
    /// `canonical_json_sha256` of `policy`.
    #[serde(default)]
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<AutonomyActorWire>,
}

fn default_autonomy_schema_version() -> u32 {
    AUTONOMY_WIRE_SCHEMA_VERSION
}

fn default_revision() -> u64 {
    1
}

/// One gate's question to the evaluator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyEvaluateRequestWire {
    /// Gate kind: `plan`, `epic_plan`, `question`, or a privileged kind
    /// (`launch`, `sudo`, `custom`, `hitl`, triage, snooze, ...).
    #[serde(default)]
    pub gate_kind: String,
    /// The concrete option IDs the gate offers right now.
    #[serde(default)]
    pub option_ids: Vec<String>,
    /// The decision values the gate's adapter can execute.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// One evaluated gate decision. `deny` is reserved for E3 and never
/// produced in E1.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyDecisionWire {
    #[serde(default)]
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default)]
    pub option_ids: Vec<String>,
    #[serde(default)]
    pub rule: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub profile: String,
    #[serde(default)]
    pub selection: String,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub digest: String,
    #[serde(default)]
    pub source: String,
}

/// Legacy `%auto` meta keys reproduced from a record.
///
/// Field order mirrors today's writer output (`approve`,
/// `auto_approve_argument`, `auto_approve_plan_action`, `plan`) so a
/// projection serialized in order is byte-identical to what the old
/// writers wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyLegacyProjectionWire {
    #[serde(default)]
    pub approve: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_approve_argument: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_approve_plan_action: Option<String>,
    #[serde(default)]
    pub plan: bool,
    /// The mode for `set_prompt_auto_mode` (`plan`, `tale`, `epic`), or
    /// `None` to strip the directive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_mode: Option<String>,
}

/// One per-kind row of a summary or profile catalog entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomySummaryCellWire {
    /// Policy key: `plan`, `epic`, or `question`.
    #[serde(default)]
    pub kind: String,
    /// `✓` when the kind is automatic, `✋` when it waits.
    #[serde(default)]
    pub glyph: String,
    /// The exact UX-baseline effect wording for this cell.
    #[serde(default)]
    pub effect: String,
    /// The evaluate rule this cell reports (`gates.<kind>`).
    #[serde(default)]
    pub rule: String,
}

/// The human-facing summary of one autonomy record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomySummaryWire {
    #[serde(default)]
    pub profile: String,
    /// `autopilot` (all automatic), `attended` (at least one asks),
    /// `unattended` (`on_ask` deny; unreachable in E1), or `manual`.
    #[serde(default)]
    pub class: String,
    /// One human sentence describing the record.
    #[serde(default)]
    pub sentence: String,
    /// The one-liner, e.g. `tales ✓ · epics ✓ · questions first`.
    #[serde(default)]
    pub short: String,
    #[serde(default)]
    pub cells: Vec<AutonomySummaryCellWire>,
    /// Always [`AUTONOMY_COVERAGE`].
    #[serde(default)]
    pub coverage: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub selection: String,
    #[serde(default)]
    pub revision: u64,
}

/// One built-in compatibility profile catalog entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyProfileWire {
    #[serde(default)]
    pub name: String,
    /// Always `builtin` in E1; config profiles arrive in E3.
    #[serde(default)]
    pub layer: String,
    /// `manual`, `default` (`standard`), or `compatibility`.
    #[serde(default)]
    pub kind: String,
    /// The `%auto` value texts that resolve to this profile. A prompt
    /// with no `%auto` also resolves to `manual`.
    #[serde(default)]
    pub selections: Vec<String>,
    #[serde(default)]
    pub cells: Vec<AutonomySummaryCellWire>,
    /// The generated one-liner (same shape as summary `short`).
    #[serde(default)]
    pub oneliner: String,
}

/// The context for one decision sentence: the gate kind the decision
/// was evaluated for (`plan`, `epic_plan`, `question`, ...).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyDecisionSentenceContextWire {
    #[serde(default)]
    pub gate_kind: String,
}

/// A `%auto` selection change against a live record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyMutateRequestWire {
    /// `manual`, `restore`, or any `%auto` value text (a leading
    /// `%auto` / `%auto:` prefix is accepted and stripped).
    #[serde(default)]
    pub selection: String,
    /// Refuse with `stale` when this differs from the live revision.
    /// `None` skips the revision check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub actor: AutonomyActorWire,
    /// Stored as `updated_at` when non-empty; otherwise the previous
    /// timestamp is kept.
    #[serde(default)]
    pub now: String,
}

/// The outcome of [`crate::autonomy::mutate_autonomy`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyMutateResultWire {
    /// `applied`, `unchanged`, `refused`, or `stale`.
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub record: AutonomyRecordWire,
    #[serde(default)]
    pub reason: String,
}

/// The outcome of [`crate::autonomy::autonomy_inherit`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyInheritResultWire {
    /// `inherited`, `narrowed`, or `refused`.
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub record: AutonomyRecordWire,
    #[serde(default)]
    pub reason: String,
}

/// One row of the host decision log (`<sase_home>/autonomy/decisions.jsonl`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyLogEntryWire {
    #[serde(default = "default_autonomy_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub at: String,
    #[serde(default)]
    pub agent: String,
    #[serde(default)]
    pub agent_session: String,
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub gate_kind: String,
    #[serde(default)]
    pub gate_id: String,
    #[serde(default)]
    pub creator_role: String,
    #[serde(default)]
    pub decision: AutonomyDecisionWire,
}

/// Filters for [`crate::autonomy::read_autonomy_decisions`]. Every field
/// is exact-match except `since` (lexicographic `at >= since`) and
/// `limit` (at most that many entries, newest first).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutonomyLogQueryWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}
