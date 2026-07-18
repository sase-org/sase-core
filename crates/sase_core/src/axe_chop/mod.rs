//! Deterministic chop contracts and planning transforms.
//!
//! The host owns YAML decoding, filesystem/process IO, project discovery,
//! snapshots, and state persistence.  This module owns the reusable domain
//! rules: result/proposal validation, trigger and guard decisions, checkpoint
//! and once-per transforms, target expansion, and strict axe config
//! validation.

mod bookkeeping;
mod config;
mod decision;
mod targets;
mod validation;
mod wire;

pub use bookkeeping::{apply_checkpoint_update, check_and_record_once_per};
pub use config::{parse_chop_duration, validate_axe_config};
pub use decision::evaluate_chop_decision;
pub use targets::expand_chop_targets;
pub use validation::{
    derive_chop_agent_name, parse_chop_result, validate_chop_proposal,
    validate_chop_result,
};
pub use wire::*;

#[cfg(test)]
mod tests;
