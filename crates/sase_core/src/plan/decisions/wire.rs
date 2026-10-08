//! Additive validated-plan wire records for Plan Decisions.
//!
//! Every new field uses `skip_serializing_if` so plans without decisions
//! serialize exactly as before (`PLAN_WIRE_SCHEMA_VERSION` stays 3).

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// One authored choice option: its key and the one-line consequence label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionChoiceWire {
    pub key: String,
    pub label: String,
}

/// The memory selectors of a toggle memory decision, as authored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionMemoryWire {
    pub selectors: Vec<String>,
}

/// One validated plan decision in author order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionWire {
    pub id: String,
    /// `toggle` or `choice`.
    pub kind: String,
    pub ask: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<PlanDecisionChoiceWire>,
    /// A YAML boolean for toggles, an authored choice key for choices.
    pub default: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<PlanDecisionMemoryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<String>,
    /// System-written answer; only present outside Authoring mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<JsonValue>,
}

/// One parsed branch callout with its inclusive original-document line span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionCalloutWire {
    pub id: String,
    /// The authored choice key for `choice` branches; absent for toggles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// `yes`, `no`, or `choice`.
    pub branch: String,
    pub start_line: u64,
    pub end_line: u64,
}
