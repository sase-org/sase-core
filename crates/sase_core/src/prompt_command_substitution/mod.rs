//! Source-versus-rendered command-substitution eligibility and span planning.
//!
//! The host supplies prompt text, typed provenance, and optional protected
//! UTF-8 byte ranges. This module decides whether `$(...)` spans may execute
//! and returns ordered replacement actions. It does not launch subprocesses
//! or embed Jinja.

mod plan;
mod wire;

pub use plan::{
    plan_prompt_command_substitution,
    plan_prompt_command_substitution_from_json,
    prompt_command_substitution_wire_schema_version,
};
pub use wire::{
    PromptCommandReplacementWire, PromptCommandSubstitutionError,
    PromptCommandSubstitutionPlanWire, PromptCommandSubstitutionRequestWire,
    PromptCommandTextKindWire, PromptLaunchAuthorshipWire,
    PromptUtf8ByteRangeWire, PROMPT_COMMAND_SUBSTITUTION_OFFSET_UNIT,
    PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
