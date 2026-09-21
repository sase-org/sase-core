//! Narrow `sase bead` CLI execution planner.
//!
//! Python still owns workspace discovery, help text, and host-coupled
//! commands. This module handles the common bead commands once Python has
//! resolved the store paths.

mod create_command;
mod design_refs;
mod dispatch;
mod mutate_commands;
mod parsing;
mod presentation;
mod read_commands;
mod rendering;
mod resolution;
#[cfg(test)]
mod tests;

pub use dispatch::{
    execute_bead_cli, BeadCliMutationSummaryWire, BeadCliOutcomeWire,
    BeadCliStatusTransitionWire,
};
