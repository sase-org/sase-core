//! Autonomy summary wire, profile catalog, and one-liners.
//!
//! The per-kind cells, glyphs, effect wording, and coverage line come
//! from the UX baseline and are shared by `summary`, the `profiles`
//! catalog, and (through the bindings) `sase autonomy show` and
//! `sase agent show`.

use super::profiles::profile_policy;
use super::wires::{
    AutonomyPolicyWire, AutonomyProfileWire, AutonomyRecordWire,
    AutonomySummaryCellWire, AutonomySummaryWire, AUTONOMY_PROFILE_EPIC,
    AUTONOMY_PROFILE_MANUAL, AUTONOMY_PROFILE_STANDARD, AUTONOMY_PROFILE_TALE,
    AUTONOMY_VALUE_APPROVE, AUTONOMY_VALUE_APPROVE_ARCHIVE,
    AUTONOMY_VALUE_FIRST,
};

/// Glyph for an automatic kind.
pub const AUTONOMY_GLYPH_AUTO: &str = "✓";
/// Glyph for a kind that waits for a human.
pub const AUTONOMY_GLYPH_WAIT: &str = "✋";

/// Effect wording for an automatic tale-plan kind.
pub const AUTONOMY_EFFECT_PLAN_AUTO: &str =
    "Tales: approve + archive, then implement";
/// Effect wording for an automatic epic-plan kind.
pub const AUTONOMY_EFFECT_EPIC_AUTO: &str = "Epics: archive + launch workers";
/// Effect wording for an automatic question kind.
pub const AUTONOMY_EFFECT_QUESTION_AUTO: &str =
    "Questions: take the first option";
/// Effect wording for a kind that waits for a human.
pub const AUTONOMY_EFFECT_WAIT: &str = "Waits for you";

/// Coverage line carried by every summary and awareness block.
pub const AUTONOMY_COVERAGE: &str =
    "Covers host checkpoints only · the agent's shell is not restricted";

/// Summary class for a fully automatic record.
pub const AUTONOMY_CLASS_AUTOPILOT: &str = "autopilot";
/// Summary class when at least one kind asks.
pub const AUTONOMY_CLASS_ATTENDED: &str = "attended";
/// Summary class when `on_ask` is deny; unreachable in E1.
pub const AUTONOMY_CLASS_UNATTENDED: &str = "unattended";
/// Summary class for the `manual` profile.
pub const AUTONOMY_CLASS_MANUAL: &str = "manual";

const POLICY_KINDS: &[&str] = &["plan", "epic", "question"];

fn policy_value<'a>(
    policy: &'a AutonomyPolicyWire,
    kind: &str,
) -> Option<&'a str> {
    match kind {
        "plan" => policy.gates.plan.as_deref(),
        "epic" => policy.gates.epic.as_deref(),
        _ => policy.gates.question.as_deref(),
    }
}

fn is_automatic(value: Option<&str>) -> bool {
    matches!(
        value,
        Some(
            AUTONOMY_VALUE_APPROVE_ARCHIVE
                | AUTONOMY_VALUE_APPROVE
                | AUTONOMY_VALUE_FIRST
        )
    )
}

fn effect_for(kind: &str, value: Option<&str>) -> &'static str {
    if !is_automatic(value) {
        return AUTONOMY_EFFECT_WAIT;
    }
    match kind {
        "plan" => AUTONOMY_EFFECT_PLAN_AUTO,
        "epic" => AUTONOMY_EFFECT_EPIC_AUTO,
        _ => AUTONOMY_EFFECT_QUESTION_AUTO,
    }
}

/// One summary cell for `kind` under `policy`. A missing value reports
/// `unmentioned`, mirroring `evaluate()`.
pub fn summary_cell(
    policy: &AutonomyPolicyWire,
    kind: &str,
) -> AutonomySummaryCellWire {
    let value = policy_value(policy, kind);
    let automatic = is_automatic(value);
    let rule = match value {
        None => "unmentioned".to_string(),
        _ => format!("gates.{kind}"),
    };
    AutonomySummaryCellWire {
        kind: kind.to_string(),
        glyph: if automatic {
            AUTONOMY_GLYPH_AUTO
        } else {
            AUTONOMY_GLYPH_WAIT
        }
        .to_string(),
        effect: effect_for(kind, value).to_string(),
        rule,
    }
}

/// The summary class for a record: `autopilot` when every kind is
/// automatic, `unattended` when `on_ask` denies, `manual` for the
/// `manual` profile, else `attended`.
pub fn summary_class(record: &AutonomyRecordWire) -> &'static str {
    if record.policy.on_ask == "deny" {
        return AUTONOMY_CLASS_UNATTENDED;
    }
    if POLICY_KINDS
        .iter()
        .all(|kind| is_automatic(policy_value(&record.policy, kind)))
    {
        return AUTONOMY_CLASS_AUTOPILOT;
    }
    if record.profile == AUTONOMY_PROFILE_MANUAL {
        return AUTONOMY_CLASS_MANUAL;
    }
    AUTONOMY_CLASS_ATTENDED
}

fn short_token(kind: &str, value: Option<&str>) -> String {
    if kind == "question" && value == Some(AUTONOMY_VALUE_FIRST) {
        return "first".to_string();
    }
    if is_automatic(value) {
        AUTONOMY_GLYPH_AUTO.to_string()
    } else {
        AUTONOMY_GLYPH_WAIT.to_string()
    }
}

/// The one-liner for a policy, e.g.
/// `tales ✓ · epics ✓ · questions first`.
pub fn summary_short(policy: &AutonomyPolicyWire) -> String {
    format!(
        "tales {} · epics {} · questions {}",
        short_token("plan", policy_value(policy, "plan")),
        short_token("epic", policy_value(policy, "epic")),
        short_token("question", policy_value(policy, "question")),
    )
}

fn sentence_fragment(kind: &str, value: Option<&str>) -> &'static str {
    match (kind, is_automatic(value)) {
        ("plan", true) => "tales approve + archive",
        ("plan", false) => "tale plans wait",
        ("epic", true) => "epics launch",
        ("epic", false) => "epic plans wait",
        ("question", true) => "questions take the first option",
        _ => "questions wait",
    }
}

fn profile_display(profile: &str) -> &str {
    match profile {
        AUTONOMY_PROFILE_STANDARD => "Standard",
        AUTONOMY_PROFILE_TALE => "Tale",
        AUTONOMY_PROFILE_EPIC => "Epic",
        AUTONOMY_PROFILE_MANUAL => "Manual",
        _ => "Manual",
    }
}

/// One human sentence describing a record.
pub fn summary_sentence(record: &AutonomyRecordWire) -> String {
    if record.profile == AUTONOMY_PROFILE_MANUAL {
        return "Manual: every plan, epic, and question waits for you."
            .to_string();
    }
    format!(
        "{}: {}; {}; {}.",
        profile_display(&record.profile),
        sentence_fragment("plan", policy_value(&record.policy, "plan")),
        sentence_fragment("epic", policy_value(&record.policy, "epic")),
        sentence_fragment("question", policy_value(&record.policy, "question")),
    )
}

/// Human-facing summary of one autonomy record.
pub fn autonomy_summary(record: &AutonomyRecordWire) -> AutonomySummaryWire {
    let cells = POLICY_KINDS
        .iter()
        .map(|kind| summary_cell(&record.policy, kind))
        .collect();
    AutonomySummaryWire {
        profile: record.profile.clone(),
        class: summary_class(record).to_string(),
        sentence: summary_sentence(record),
        short: summary_short(&record.policy),
        cells,
        coverage: AUTONOMY_COVERAGE.to_string(),
        source: record.source.clone(),
        selection: record.selection.clone(),
        revision: record.revision,
    }
}

fn profile_kind(profile: &str) -> &'static str {
    match profile {
        AUTONOMY_PROFILE_MANUAL => "manual",
        AUTONOMY_PROFILE_STANDARD => "default",
        _ => "compatibility",
    }
}

fn profile_selections(profile: &str) -> Vec<String> {
    let selections: &[&str] = match profile {
        AUTONOMY_PROFILE_MANUAL => &["manual", "off"],
        AUTONOMY_PROFILE_STANDARD => &["", "true", "+"],
        AUTONOMY_PROFILE_TALE => &["plan", "tale"],
        AUTONOMY_PROFILE_EPIC => &["epic"],
        _ => &[],
    };
    selections.iter().map(|item| item.to_string()).collect()
}

/// The built-in E1 compatibility profile catalog.
pub fn autonomy_profiles() -> Vec<AutonomyProfileWire> {
    [
        AUTONOMY_PROFILE_MANUAL,
        AUTONOMY_PROFILE_STANDARD,
        AUTONOMY_PROFILE_TALE,
        AUTONOMY_PROFILE_EPIC,
    ]
    .iter()
    .map(|name| {
        let policy = profile_policy(name);
        let cells = POLICY_KINDS
            .iter()
            .map(|kind| summary_cell(&policy, kind))
            .collect();
        AutonomyProfileWire {
            name: name.to_string(),
            layer: "builtin".to_string(),
            kind: profile_kind(name).to_string(),
            selections: profile_selections(name),
            cells,
            oneliner: summary_short(&policy),
        }
    })
    .collect()
}
