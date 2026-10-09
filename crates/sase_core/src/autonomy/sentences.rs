//! One-line decision sentences and the agent awareness block.
//!
//! All user-facing text comes only from core; Python never composes it.
//! The awareness block is advisory and always carries the coverage line.

use super::wires::{
    AutonomyDecisionSentenceContextWire, AutonomyDecisionWire,
    AutonomyRecordWire, AUTONOMY_PROFILE_MANUAL, AUTONOMY_VALUE_APPROVE,
    AUTONOMY_VALUE_APPROVE_ARCHIVE, AUTONOMY_VALUE_FIRST,
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

fn plan_consequence(automatic: bool) -> &'static str {
    if automatic {
        "approved and archived automatically, then implemented without review."
    } else {
        "wait for a human review before anything launches."
    }
}

fn epic_consequence(automatic: bool) -> &'static str {
    if automatic {
        "archived and launched automatically, then implemented without review."
    } else {
        "wait for a human review before anything launches."
    }
}

fn question_consequence(automatic: bool) -> &'static str {
    if automatic {
        "answered automatically with each question's first option; no human \
         reads them, so put your recommended option first."
    } else {
        "wait for a human to answer; nothing is decided automatically."
    }
}

fn kind_is_automatic(record: &AutonomyRecordWire, kind: &str) -> bool {
    let value = match kind {
        "plan" => record.policy.gates.plan.as_deref(),
        "epic" => record.policy.gates.epic.as_deref(),
        _ => record.policy.gates.question.as_deref(),
    };
    matches!(
        value,
        Some(
            AUTONOMY_VALUE_APPROVE_ARCHIVE
                | AUTONOMY_VALUE_APPROVE
                | AUTONOMY_VALUE_FIRST
        )
    )
}

/// The advisory awareness block for one record, or `None` for `manual`.
/// At most six lines: a header, one consequence line each for tale
/// plans, epic plans, and questions, and the privileged-gate footer.
pub fn autonomy_awareness_text(record: &AutonomyRecordWire) -> Option<String> {
    if record.profile == AUTONOMY_PROFILE_MANUAL {
        return None;
    }
    Some(format!(
        "SASE autonomy: {} (advisory; covers host checkpoints only, your \
         shell is not restricted)\n\
         - Tale plans: {}\n\
         - Epic plans: {}\n\
         - Questions: {}\n\
         - Launch, sudo, and custom gates: wait for a human.",
        record.profile,
        plan_consequence(kind_is_automatic(record, "plan")),
        epic_consequence(kind_is_automatic(record, "epic")),
        question_consequence(kind_is_automatic(record, "question")),
    ))
}
