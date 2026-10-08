//! Plan Decision authoring grammar (Section 2 of `plan:202610/plan_decisions.md`).
//!
//! This module owns the additive validated-plan wire records plus the
//! frontmatter and body validation behind them. Resolution, quote matching,
//! the Decision Sheet, and the implementer block arrive in later phases and
//! build on these records.

pub mod callout;
pub mod grammar;
pub mod wire;

#[cfg(test)]
mod tests;

pub use callout::{
    parse_decision_callouts, uncovered_memory_edits, whole_word_mentions_body,
};
pub use grammar::{
    validate_decision_body, validate_decision_frontmatter, DecisionBodyInfo,
    DecisionBodyOutcome, DecisionFrontmatterOutcome,
};
pub use wire::{
    PlanDecisionCalloutWire, PlanDecisionChoiceWire, PlanDecisionMemoryWire,
    PlanDecisionWire,
};
