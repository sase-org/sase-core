//! Shared policy for reconciling published agent-run state.
//!
//! Filesystem evidence and publication writes stay in the host. This module
//! validates identity, resolves state precedence, pages through published
//! rows, and advances fair scheduled-refresh checkpoints.

mod resolve;
mod schedule;
mod window;
pub mod wire;

#[cfg(test)]
mod tests;

pub use resolve::resolve_agent_run_state;
pub use schedule::advance_agent_run_state_schedule;
pub use window::select_agent_run_state_window;
pub use wire::*;
