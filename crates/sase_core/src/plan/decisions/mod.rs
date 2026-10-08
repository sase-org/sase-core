//! Plan Decision authoring grammar (Section 2 of `plan:202610/plan_decisions.md`).
//!
//! This module owns the additive validated-plan wire records plus the
//! frontmatter and body validation behind them. Frozen definitions, the
//! digest, and strict resolution live in `resolver`; human quote
//! matching lives in `quote`; the Decision Sheet, the shared summary
//! sentence, and the implementer block live in `sheet`.

pub mod callout;
pub mod grammar;
pub mod quote;
pub mod resolver;
pub mod sheet;
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
pub use quote::{
    plan_decision_quote_match, PlanDecisionQuoteClosestWire,
    PlanDecisionQuoteMatchWire, PlanDecisionQuoteTextWire, QUOTE_MIN_WORDS,
};
pub use resolver::{
    plan_decisions_digest, plan_decisions_payload, plan_decisions_resolve,
    PlanDecisionDefinitionWire, PlanDecisionHostFactWire,
    PlanDecisionMemoryRecordWire, PlanDecisionResolveErrorWire,
    PlanDecisionResolveRowWire, PlanDecisionResolveWire, DECISION_PROVENANCES,
};
pub use sheet::{
    plan_decision_sheet, plan_decision_summary, plan_decisions_prompt_block,
    PlanDecisionInheritedWire, PlanDecisionSheetMemoryWire,
    PlanDecisionSheetRowWire, PlanDecisionSheetWire, DECISION_AUDIENCES,
    DECISION_SUMMARY_FORMS, DECISION_SURFACES, DECISION_VERDICTS,
};
pub use wire::{
    PlanDecisionCalloutWire, PlanDecisionChoiceWire, PlanDecisionMemoryWire,
    PlanDecisionWire,
};
