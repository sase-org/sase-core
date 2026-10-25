//! Versioned question-round projection wires.
//!
//! A question round is one question-gate bundle
//! (`~/.sase/interaction_requests/question/<gate_id>/`): the questions from
//! `request.json`, the answers from `response.json`, and the terminal record
//! from `cancellation.json`. These wires carry the host-collected bundle
//! contents (as raw JSON, so core owns tolerant parsing) and the projected
//! read model. This module performs no filesystem, subprocess, clock, or
//! notification I/O.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Wire schema version for question-round projection requests and views.
pub const QUESTION_ROUND_WIRE_SCHEMA_VERSION: u32 = 1;

pub const QUESTION_ROUND_CODE_UNSUPPORTED_SCHEMA: &str =
    "unsupported_question_round_schema";
pub const QUESTION_ROUND_CODE_INVALID_REQUEST: &str =
    "invalid_question_round_request";

/// Round status values. See the shared-contracts section of the
/// `questions_main_deck` epic plan: the status derives from `gate_state`,
/// except that a durably recorded submit result always yields `answered`.
pub const QUESTION_ROUND_STATUS_PENDING: &str = "pending";
pub const QUESTION_ROUND_STATUS_ANSWERED: &str = "answered";
pub const QUESTION_ROUND_STATUS_TIMED_OUT: &str = "timed_out";
pub const QUESTION_ROUND_STATUS_CANCELLED: &str = "cancelled";
pub const QUESTION_ROUND_STATUS_FAILED: &str = "failed";
pub const QUESTION_ROUND_STATUS_LOST: &str = "lost";
pub const QUESTION_ROUND_STATUS_UNKNOWN: &str = "unknown";

/// Data-completeness values for the projected round.
pub const QUESTION_ROUND_DATA_COMPLETE: &str = "complete";
pub const QUESTION_ROUND_DATA_PARTIAL: &str = "partial";
pub const QUESTION_ROUND_DATA_UNAVAILABLE: &str = "unavailable";

/// Actor values for the projected provenance.
pub const QUESTION_ROUND_ACTOR_AUTO_POLICY: &str = "auto_policy";
pub const QUESTION_ROUND_ACTOR_HUMAN: &str = "human";
pub const QUESTION_ROUND_ACTOR_AGENT: &str = "agent";
pub const QUESTION_ROUND_ACTOR_UNKNOWN: &str = "unknown";

/// Recommendation-basis values.
pub const QUESTION_ROUND_BASIS_OPTION_ORDER: &str = "option_order";
pub const QUESTION_ROUND_BASIS_NONE: &str = "none";

/// Recommendation-match values for answered entries.
pub const QUESTION_ROUND_MATCH_FOLLOWED: &str = "followed";
pub const QUESTION_ROUND_MATCH_DIFFERENT_OPTION: &str = "different_option";
pub const QUESTION_ROUND_MATCH_OTHER_TEXT: &str = "other_text";
pub const QUESTION_ROUND_MATCH_FREE_TEXT: &str = "free_text";

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct QuestionRoundError {
    pub code: String,
    pub message: String,
}

impl QuestionRoundError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

/// Host-collected identity for the round being projected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionRoundGateWire {
    pub gate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asked_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asked_at_unix: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_index: Option<u64>,
}

/// Host-collected bundle contents for one question round.
///
/// `request`, `response`, and `cancellation` are the parsed contents of the
/// bundle's `request.json`, `response.json`, and `cancellation.json` (or
/// `None` when the file is missing or unreadable). Core owns tolerant
/// parsing of their shapes; malformed bundle content degrades `data_state`
/// instead of erroring.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionRoundRequestWire {
    pub schema_version: u32,
    pub gate: QuestionRoundGateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<serde_json::Value>,
}

/// One offered option for a question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionOptionWire {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// One recorded answer for a question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionAnswerWire {
    #[serde(default)]
    pub selected: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_feedback: Option<String>,
}

/// One projected question with its aligned answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionEntryWire {
    pub index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    pub question: String,
    #[serde(default)]
    pub options: Vec<QuestionOptionWire>,
    #[serde(default)]
    pub multi_select: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_index: Option<u64>,
    pub recommendation_basis: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<QuestionAnswerWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommendation_match: Option<String>,
}

/// The auto policy that settled the round, when recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionPolicyWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// How the round was answered: the raw channel, the classified actor, the
/// recorded policy, and the settlement time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionProvenanceWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<QuestionPolicyWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responded_at_unix: Option<f64>,
}

/// The terminal record for a cancelled or timed-out round.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionCancellationWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_at_unix: Option<f64>,
}

/// The projected read model for one question round.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionRoundWire {
    pub schema_version: u32,
    pub gate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asked_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asked_at_unix: Option<f64>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_state: Option<String>,
    pub data_state: String,
    #[serde(default)]
    pub entries: Vec<QuestionEntryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<QuestionProvenanceWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<QuestionCancellationWire>,
}
