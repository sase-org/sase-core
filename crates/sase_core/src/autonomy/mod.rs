//! Autonomy record, compatibility profiles, and `evaluate()`.

mod evaluate;
mod legacy;
mod profiles;
mod resolve;
mod wires;

#[cfg(test)]
mod tests;

pub use evaluate::{evaluate, fleet_auto_approved};
pub use legacy::{
    autonomy_legacy_projection, autonomy_record_from_legacy_meta,
};
pub use profiles::{
    builtin_profiles, policy_digest, profile_policy, required_option_ids,
};
pub use resolve::{
    resolve_autonomy_selection, valid_actor_kinds, valid_sources, AutonomyError,
};
pub use wires::{
    AutonomyActorWire, AutonomyDecisionWire, AutonomyEvaluateRequestWire,
    AutonomyGatesWire, AutonomyLastWire, AutonomyLegacyProjectionWire,
    AutonomyPolicyWire, AutonomyRecordWire, AUTONOMY_ON_ASK_PARK,
    AUTONOMY_PROFILE_EPIC, AUTONOMY_PROFILE_MANUAL, AUTONOMY_PROFILE_STANDARD,
    AUTONOMY_PROFILE_TALE, AUTONOMY_SOURCE_CLI, AUTONOMY_SOURCE_INHERITED,
    AUTONOMY_SOURCE_LEGACY, AUTONOMY_SOURCE_PROMPT, AUTONOMY_SOURCE_TUI,
    AUTONOMY_VALUE_APPROVE, AUTONOMY_VALUE_APPROVE_ARCHIVE, AUTONOMY_VALUE_ASK,
    AUTONOMY_VALUE_FIRST, AUTONOMY_WIRE_SCHEMA_VERSION,
};
