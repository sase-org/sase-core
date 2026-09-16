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
    pub source: String,
    pub accepted_at_unix: f64,
    /// Opaque durable recovery pointer for whoever owns finishing this
    /// decision's execution -- a journal attempt id, a reserved proc id, or
    /// similar. Informational: recovery itself is the owner's own
    /// responsibility, not this receipt's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_owner: Option<String>,
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
    pub source: String,
    pub accepted_at_unix: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_owner: Option<String>,
    /// The durable receipt already on disk for this gate, if any. `None`
    /// means this is the first submission the host has found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_receipt: Option<GateDecisionReceiptWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateDecisionOutcomeStatusWire {
    Accepted,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateDecisionAcceptanceOutcomeWire {
    pub status: GateDecisionOutcomeStatusWire,
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
}

/// Deterministic verdict for one gate's current lifecycle disposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateLifecycleDecisionWire {
    pub schema_version: u32,
    pub disposition: String,
    pub reason: String,
}
