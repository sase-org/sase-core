//! Versioned gate-decision-acceptance wires.
//!
//! A gate decision is durably accepted independently of how long its
//! execution (archive publication, workspace preparation, successor launch)
//! takes. This module owns the receipt shape and the identity fingerprint
//! used to tell an identical resubmission (replay) from a conflicting one.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const GATE_DECISION_WIRE_SCHEMA_VERSION: u32 = 1;

pub const GATE_DECISION_CODE_CONFLICT: &str = "gate_decision_conflict";
pub const GATE_DECISION_CODE_INVALID_REQUEST: &str =
    "invalid_gate_decision_request";
pub const GATE_DECISION_CODE_UNSUPPORTED_SCHEMA: &str =
    "unsupported_gate_decision_schema";

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct GateDecisionError {
    pub code: String,
    pub message: String,
}

impl GateDecisionError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateDecisionExecutionOwnerKindWire {
    Proc,
    Process,
}

/// Structured execution owner for newly accepted decisions.
///
/// Legacy receipts used a bare string proc id. The untagged wrapper below
/// keeps those receipts readable while letting newer callers record a
/// verifiable process identity when no supervisor proc owns the execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionExecutionOwnerRecordWire {
    pub kind: GateDecisionExecutionOwnerKindWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GateDecisionExecutionOwnerWire {
    LegacyProcId(String),
    Structured(GateDecisionExecutionOwnerRecordWire),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateDecisionFailureStageWire {
    Command,
    TerminalPrepare,
    SideEffects,
    FollowUp,
}

/// Durable, redacted failure outcome for the current accepted execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionFailureOutcomeWire {
    pub outcome_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_id: Option<String>,
    pub attempt_id: String,
    pub stage: GateDecisionFailureStageWire,
    pub code: String,
    pub message: String,
    pub at_unix: f64,
    pub error_record: String,
}

/// Host-collected execution facts used by the Rust policy.
///
/// The host does the filesystem/process reads; Rust decides whether those
/// facts make the existing receipt live, failed, or owner-lost. Unknown
/// facts are conservative and therefore treated as live.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionExecutionFactsWire {
    #[serde(default)]
    pub response_lock_held: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_host_matches: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_pid_running: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_identity_matches: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_from_previous_boot: Option<bool>,
    /// Legacy proc status string, e.g. pending/running/settling/success/error/killed,
    /// or the sentinel "missing" when the recorded proc id was not found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_proc_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_proc_supervisor_alive: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_failure: Option<GateDecisionFailureOutcomeWire>,
}

/// The durable receipt for one accepted gate decision.
///
/// `identity_fingerprint` is derived, never host-supplied: it binds
/// `request_hash`, `selected_option_ids`, `input_identity`, and
/// `feedback_identity`, the fields the plan calls "every semantically
/// relevant input". `source` and `accepted_at_unix` are provenance only and
/// never affect whether a resubmission replays or conflicts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionReceiptWire {
    pub schema_version: u32,
    pub gate_id: String,
    pub request_hash: String,
    pub selected_option_ids: Vec<String>,
    pub input_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_id: Option<String>,
    pub source: String,
    pub accepted_at_unix: f64,
    /// Opaque durable recovery pointer for whoever owns finishing this
    /// decision's execution. Legacy receipts use a bare proc-id string;
    /// newer receipts can use a structured, verifiable process owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_owner: Option<GateDecisionExecutionOwnerWire>,
    pub identity_fingerprint: String,
}

/// Host-collected facts needed to decide one gate-decision acceptance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionAcceptanceRequestWire {
    pub schema_version: u32,
    pub gate_id: String,
    pub request_hash: String,
    pub selected_option_ids: Vec<String>,
    pub input_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_id: Option<String>,
    pub source: String,
    pub accepted_at_unix: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_owner: Option<GateDecisionExecutionOwnerWire>,
    /// The durable receipt already on disk for this gate, if any. `None`
    /// means this is the first submission the host has found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_receipt: Option<GateDecisionReceiptWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_facts: Option<GateDecisionExecutionFactsWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateDecisionOutcomeStatusWire {
    Accepted,
    Replayed,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionAcceptanceOutcomeWire {
    pub status: GateDecisionOutcomeStatusWire,
    pub receipt: GateDecisionReceiptWire,
}

/// Claim execution of the currently accepted receipt before appending an
/// attempt event. The caller has already read the receipt while holding the
/// response lock and acceptance lock; this policy verifies that it is still
/// the same accepted decision and returns the receipt with the new owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionExecutionClaimRequestWire {
    pub schema_version: u32,
    pub gate_id: String,
    pub request_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_id: Option<String>,
    pub receipt: GateDecisionReceiptWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_owner: Option<GateDecisionExecutionOwnerWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionExecutionClaimOutcomeWire {
    pub receipt: GateDecisionReceiptWire,
}

pub const GATE_LIFECYCLE_WIRE_SCHEMA_VERSION: u32 = 1;

pub const GATE_LIFECYCLE_CODE_INVALID_REQUEST: &str =
    "invalid_gate_lifecycle_request";
pub const GATE_LIFECYCLE_CODE_UNSUPPORTED_SCHEMA: &str =
    "unsupported_gate_lifecycle_schema";
pub const GATE_LIFECYCLE_CODE_INVALID_RECEIPT: &str =
    "invalid_gate_decision_receipt";

pub const GATE_LIFECYCLE_DISPOSITION_ANSWERED: &str = "answered";
pub const GATE_LIFECYCLE_DISPOSITION_CANCELLED_TIMEOUT: &str =
    "cancelled_timeout";
pub const GATE_LIFECYCLE_DISPOSITION_CANCELLED_LOST: &str = "cancelled_lost";
pub const GATE_LIFECYCLE_DISPOSITION_CANCELLED_STOPPED: &str =
    "cancelled_stopped";
pub const GATE_LIFECYCLE_DISPOSITION_ACCEPTED_UNFINISHED: &str =
    "accepted_unfinished";
pub const GATE_LIFECYCLE_DISPOSITION_ACCEPTED_FAILED: &str = "accepted_failed";
pub const GATE_LIFECYCLE_DISPOSITION_ACCEPTED_OWNER_LOST: &str =
    "accepted_owner_lost";
pub const GATE_LIFECYCLE_DISPOSITION_PENDING: &str = "pending";
pub const GATE_LIFECYCLE_DISPOSITION_EXPIRED_REVIEW: &str = "expired_review";
pub const GATE_LIFECYCLE_DISPOSITION_EXPIRED_GRACE: &str = "expired_grace";

/// Host-collected evidence needed to classify one gate's lifecycle.
///
/// The host reads bundle files (``response.json``, ``cancellation.json``,
/// ``decision_receipt.json``) and the review deadline; this wire carries
/// only the facts that matter to the policy, never raw file contents beyond
/// the receipt itself (reused verbatim so its identity can be verified
/// against `gate_id`/`request_hash`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateLifecycleRequestWire {
    pub schema_version: u32,
    pub gate_id: String,
    pub request_hash: String,
    pub now_unix: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix: Option<f64>,
    pub grace_seconds: f64,
    pub has_response: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<GateDecisionReceiptWire>,
    /// True when a ``decision_receipt.json`` file exists but could not be
    /// read or parsed -- distinct from no receipt file existing at all.
    #[serde(default)]
    pub receipt_unreadable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_facts: Option<GateDecisionExecutionFactsWire>,
}

/// Deterministic verdict for one gate's current lifecycle disposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateLifecycleDecisionWire {
    pub schema_version: u32,
    pub disposition: String,
    pub reason: String,
    pub can_cancel: bool,
    pub can_supersede: bool,
}
