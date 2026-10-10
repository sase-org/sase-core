//! One-line decision sentences.
//!
//! All user-facing text comes only from core; Python never composes it.

use super::wires::{
    AutonomyDecisionSentenceContextWire, AutonomyDecisionWire,
    AUTONOMY_VALUE_APPROVE, AUTONOMY_VALUE_APPROVE_ARCHIVE,
    AUTONOMY_VALUE_FIRST,
};

/// The artifact noun an automatic decision acts on.
fn auto_subject(gate_kind: &str) -> &str {
    match gate_kind {
        "plan" => "tale",
        "epic_plan" => "epic",
        "question" => "question",
        _ => gate_kind,
    }
}

/// The gate noun an asking decision waits on.
fn ask_subject(gate_kind: &str) -> &str {
    match gate_kind {
        "plan" => "tale plan",
        "epic_plan" => "epic plan",
        "question" => "question",
        _ => gate_kind,
    }
}

/// One line per decision, used by `log`, `explain`, and `gate show`.
/// For example `✓ tale approved + archived · standard · gates.plan`
/// or `✋ epic plan waits for you · tale · gates.epic`.
pub fn autonomy_decision_sentence(
    decision: &AutonomyDecisionWire,
    context: &AutonomyDecisionSentenceContextWire,
) -> String {
    let phrase = match decision.outcome.as_str() {
        "auto" => match decision.value.as_deref() {
            Some(AUTONOMY_VALUE_APPROVE_ARCHIVE) => {
                format!(
                    "{} approved + archived",
                    auto_subject(&context.gate_kind)
                )
            }
            Some(AUTONOMY_VALUE_APPROVE) => {
                format!(
                    "{} approved + launched",
                    auto_subject(&context.gate_kind)
                )
            }
            Some(AUTONOMY_VALUE_FIRST) => {
                format!(
                    "{} answered with the first option",
                    auto_subject(&context.gate_kind)
                )
            }
            Some(value) => {
                format!(
                    "{} automatically decided ({value})",
                    auto_subject(&context.gate_kind)
                )
            }
            None => {
                format!(
                    "{} automatically decided",
                    auto_subject(&context.gate_kind)
                )
            }
        },
        "deny" => {
            format!("{} denied", ask_subject(&context.gate_kind))
        }
        _ => format!("{} waits for you", ask_subject(&context.gate_kind)),
    };
    let glyph = match decision.outcome.as_str() {
        "auto" => "✓",
        "deny" => "⛔",
        _ => "✋",
    };
    format!(
        "{glyph} {phrase} · {} · {}",
        decision.profile, decision.rule
    )
}
