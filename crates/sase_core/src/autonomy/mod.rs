//! Autonomy record, compatibility profiles, and `evaluate()`.

mod decision_log;
mod evaluate;
mod legacy;
mod mutate;
mod profiles;
mod resolve;
mod sentences;
mod summary;
mod wires;

#[cfg(test)]
mod tests;

pub use decision_log::{
    append_autonomy_decision, append_autonomy_decision_with_cap,
    read_autonomy_decisions, AutonomyLogError, AUTONOMY_DECISIONS_FILE,
    AUTONOMY_DECISIONS_ROTATED, AUTONOMY_LOG_DIR, AUTONOMY_LOG_MAX_BYTES,
};
pub use evaluate::{evaluate, fleet_auto_approved};
pub use legacy::{
    autonomy_legacy_projection, autonomy_record_from_legacy_meta,
};
pub use mutate::{autonomy_inherit, mutate_autonomy};
pub use profiles::{
    builtin_profiles, policy_digest, profile_policy, required_option_ids,
};
pub use resolve::{
    resolve_autonomy_selection, valid_actor_kinds, valid_sources, AutonomyError,
};
pub use sentences::autonomy_decision_sentence;
pub use summary::{
    autonomy_profiles, autonomy_summary, summary_cell, summary_class,
    summary_sentence, summary_short, AUTONOMY_CLASS_ATTENDED,
    AUTONOMY_CLASS_AUTOPILOT, AUTONOMY_CLASS_MANUAL, AUTONOMY_CLASS_UNATTENDED,
    AUTONOMY_COVERAGE, AUTONOMY_EFFECT_EPIC_AUTO, AUTONOMY_EFFECT_PLAN_AUTO,
    AUTONOMY_EFFECT_QUESTION_AUTO, AUTONOMY_EFFECT_WAIT, AUTONOMY_GLYPH_AUTO,
    AUTONOMY_GLYPH_WAIT,
};
pub use wires::{
    AutonomyActorWire, AutonomyDecisionSentenceContextWire,
    AutonomyDecisionWire, AutonomyEvaluateRequestWire, AutonomyGatesWire,
    AutonomyInheritResultWire, AutonomyLastWire, AutonomyLegacyProjectionWire,
    AutonomyLogEntryWire, AutonomyLogQueryWire, AutonomyMutateRequestWire,
    AutonomyMutateResultWire, AutonomyPolicyWire, AutonomyProfileWire,
    AutonomyRecordWire, AutonomySummaryCellWire, AutonomySummaryWire,
    AUTONOMY_ON_ASK_PARK, AUTONOMY_PROFILE_EPIC, AUTONOMY_PROFILE_MANUAL,
    AUTONOMY_PROFILE_STANDARD, AUTONOMY_PROFILE_TALE, AUTONOMY_SOURCE_CLI,
    AUTONOMY_SOURCE_INHERITED, AUTONOMY_SOURCE_LEGACY, AUTONOMY_SOURCE_PROMPT,
    AUTONOMY_SOURCE_TUI, AUTONOMY_VALUE_APPROVE,
    AUTONOMY_VALUE_APPROVE_ARCHIVE, AUTONOMY_VALUE_ASK, AUTONOMY_VALUE_FIRST,
    AUTONOMY_WIRE_SCHEMA_VERSION,
};
