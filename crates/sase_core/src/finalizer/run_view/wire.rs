//! Request/response wires for the finalizer node projection.
//!
//! The projection answers "what happened, in what state, and why" for one
//! Agents-tab node from already-collected artifact text. Python supplies the
//! text; this module owns the shapes only. Decoding lives in `decode`,
//! source precedence in `precedence`, selection in `selection`, and the
//! projector in `detail`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::wire::FinalizerDiagnosticWire;

/// Schema version of the node-view request/response wires.
pub const RUN_VIEW_WIRE_SCHEMA_VERSION: u64 = 1;

/// Every JSON text input over this size is `too_large` and never parsed.
pub const RUN_VIEW_MAX_BYTES: usize = 4 * 1024 * 1024;

/// Hard cap for free-form output strings, applied on char boundaries.
pub const RUN_VIEW_TEXT_CAP_CHARS: usize = 512;

/// One collected artifact text, decoded UTF-8 with replacement by Python.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewTextInputWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub too_large: bool,
}

/// Which kind of shell produced a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunViewRunKindWire {
    Agent,
    Monitor,
}

/// One collected file inside a finalizer instance directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewFileInputWire {
    pub name: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<RunViewTextInputWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<String>,
}

/// One finalizer instance directory listing for a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewInstanceInputWire {
    pub instance_id: String,
    #[serde(default)]
    pub files: Vec<RunViewFileInputWire>,
}

/// One run's inputs: identity and liveness from Python, artifact text rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewRunInputWire {
    pub run_id: String,
    #[serde(default)]
    pub number: u64,
    #[serde(default)]
    pub label: String,
    pub kind: RunViewRunKindWire,
    pub turn_terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_live: Option<bool>,
    #[serde(default)]
    pub agent_meta: RunViewTextInputWire,
    #[serde(default)]
    pub plan: RunViewTextInputWire,
    #[serde(default)]
    pub authority_plan: RunViewTextInputWire,
    #[serde(default)]
    pub context: RunViewTextInputWire,
    #[serde(default)]
    pub submission: RunViewTextInputWire,
    #[serde(default)]
    pub submission_attempts: RunViewTextInputWire,
    #[serde(default)]
    pub journal: RunViewTextInputWire,
    #[serde(default)]
    pub result: RunViewTextInputWire,
    #[serde(default)]
    pub instances: Vec<RunViewInstanceInputWire>,
    #[serde(default)]
    pub recovery_files: Vec<String>,
}

/// Projection request: one node is one run, or every member shell's run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerNodeViewRequestWire {
    pub schema_version: u64,
    #[serde(default)]
    pub runs: Vec<RunViewRunInputWire>,
    #[serde(default = "default_tail_lines")]
    pub tail_lines: u32,
}

fn default_tail_lines() -> u32 {
    12
}

/// Run disposition: what happened to one concrete shell's finalizer run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunViewDispositionWire {
    Active,
    Ran,
    Skipped,
    NotReached,
    Interrupted,
    Unavailable,
}

/// One declaration timeline entry: an accepted or rejected submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewDeclarationWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t: Option<f64>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_line: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_count: Option<u32>,
}

/// The declaration-recovery model turn fact, when one ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewRecoveryTurnWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// One sealed-vs-live configuration drift entry from `agent_meta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewDriftWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
}

/// One appearance of an instance inside one run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewAppearanceWire {
    pub run_id: String,
    pub status: String,
}

/// One node-level instance: the union across runs in DAG order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewNodeInstanceWire {
    pub instance_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    pub selection_reason: String,
    #[serde(default)]
    pub after: Vec<String>,
    pub status: String,
    #[serde(default)]
    pub appearances: Vec<RunViewAppearanceWire>,
}

/// One configured-but-unselected instance with its reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewUnselectedWire {
    pub instance_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    pub reason: String,
}

/// Per-instance detail. Attempt, operation, and evidence content lands in
/// `core-run-view-detail`; this layer fills identity, status, trigger, the
/// declared payload summary, and the live journal position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewRunInstanceWire {
    pub instance_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_reason: Option<String>,
    #[serde(default)]
    pub after: Vec<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_on: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_kind: Option<String>,
    #[serde(default)]
    pub submission_required: bool,
    #[serde(default)]
    pub obligation_count: u32,
    #[serde(default)]
    pub payload_summary: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
    /// Typed evidence and headline selection land in
    /// `core-run-view-detail`; this layer always reports them empty.
    #[serde(default)]
    pub evidence: Vec<super::evidence::RunViewEvidenceWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headline: Option<super::evidence::RunViewEvidenceWire>,
}

/// One run's projected view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewRunWire {
    pub run_id: String,
    pub number: u64,
    pub label: String,
    pub kind: RunViewRunKindWire,
    pub disposition: RunViewDispositionWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_status: Option<String>,
    pub cycles: u32,
    #[serde(default)]
    pub earlier_segments: u32,
    #[serde(default)]
    pub reactivated: bool,
    #[serde(default)]
    pub declarations: Vec<RunViewDeclarationWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_turn: Option<RunViewRecoveryTurnWire>,
    #[serde(default)]
    pub drift: Vec<RunViewDriftWire>,
    #[serde(default)]
    pub diagnostics: Vec<FinalizerDiagnosticWire>,
    #[serde(default)]
    pub instances: Vec<RunViewRunInstanceWire>,
}

/// The projected node view: one run for a lone turn, every member run for
/// a session container.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerNodeViewWire {
    pub schema_version: u64,
    pub status: String,
    pub glyph: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_instance_id: Option<String>,
    #[serde(default)]
    pub run_level_trouble: bool,
    #[serde(default)]
    pub instances: Vec<RunViewNodeInstanceWire>,
    #[serde(default)]
    pub unselected: Vec<RunViewUnselectedWire>,
    #[serde(default)]
    pub runs: Vec<RunViewRunWire>,
}
