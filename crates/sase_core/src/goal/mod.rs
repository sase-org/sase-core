//! Goal ledger domain: ids, events, reduction, actions, and views.
//!
//! Implements the frozen G1 contract from the goal-ledger epic plan:
//! Crockford ids, the frozen event vocabulary, the total deterministic
//! reducer, action validation, publish classes, and presentation-neutral
//! card and row view models.

mod actions;
mod ids;
pub mod ledger;
mod reduce;
pub mod render;
#[cfg(test)]
mod tests;
mod view;
mod wire;

pub use actions::{
    payload_to_json, plan_goal_action, DeterministicGoalIdMint, GoalActionWire,
    GoalIdMint, GoalRefusalWire, OsGoalIdMint, GOAL_CRITERIA_MAX,
    GOAL_CRITERION_TEXT_MAX, GOAL_MESSAGE_MAX, GOAL_NOTE_MAX, GOAL_OUTCOME_MAX,
    GOAL_TITLE_MAX,
};
pub use ids::{
    event_id_timestamp_ms, mint_event_id, mint_event_id_with, mint_goal_id,
    parse_event_id, parse_goal_id, GoalIdError, GOAL_EVENT_ID_LEN,
    GOAL_EVENT_ID_RANDOM_LEN, GOAL_EVENT_ID_TIMESTAMP_LEN, GOAL_ID_ALPHABET,
    GOAL_ID_LEN,
};
pub use reduce::{
    goal_event_publish_class, reduce_goal_events, GoalPublishClassWire,
};
pub use render::{
    goal_card_markdown, goal_citation_line, GOAL_CITATION_LINE_MAX,
};
pub use view::{
    goal_card_view, goal_row_view, goal_row_view_at, GOAL_ACCENT_HEX,
    GOAL_GLYPH,
};
pub use view::{GoalCardViewWire, GoalRowViewWire};
pub use wire::{
    GoalActorKindWire, GoalActorWire, GoalAdoptedPayloadWire,
    GoalAgentAttachedPayloadWire, GoalClaimRetractReasonWire,
    GoalClaimRetractedPayloadWire, GoalClaimStatusWire, GoalClaimStrengthWire,
    GoalClaimWire, GoalClaimedPayloadWire, GoalCreatedPayloadWire,
    GoalCriterionInputWire, GoalCriterionSourceWire, GoalCriterionWire,
    GoalDiagnosticWire, GoalEditedPayloadWire, GoalEventKindWire,
    GoalEventPayloadWire, GoalEventWire, GoalEvidenceWire,
    GoalMergedPayloadWire, GoalNamedPayloadWire, GoalOriginWire,
    GoalPlanAttachedPayloadWire, GoalProgressPayloadWire, GoalSettleFlavorWire,
    GoalSettledPayloadWire, GoalStateWire, GoalStatusWire,
    GoalTimelineEffectWire, GoalTimelineEntryWire, GoalWireError,
    GOAL_LEDGER_SCHEMA_VERSION, GOAL_WIRE_SCHEMA_VERSION,
};
