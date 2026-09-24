//! PyO3 bindings for `sase_core`.
//!
//! The Python surface is the `#[pyo3(name = ...)]` functions each domain's
//! `register_<domain>` registers. Nothing parses this header, so do not grow
//! it back into a binding list.
//!
//! Dict shapes mirror the Python wire dataclasses in
//! `src/sase/core/query_wire.py` (rectangular, all fields always
//! present) so the Python side can rehydrate them with the existing wire
//! converters. The pure `sase_core` crate uses serde's tagged-union shape
//! for `QueryExprWire`; the converters in `query/` translate between the
//! two so neither side has to bend.
//!
//! `QueryErrorWire` is surfaced as a Python `ValueError` whose message is
//! the wire error's `Display` form so existing UI validation that catches
//! `ValueError` keeps working.

// `pyo3::pyfunction` macro expansion contains a `From::from` for `PyErr`
// that clippy 1.95+ reports as `useless_conversion`. The annotation has
// to live at the module scope because the macro generates wrapper code
// outside the user-written function body.
#![allow(clippy::useless_conversion)]

mod agent_custody;
mod agent_holds;
mod agent_identity;
mod agent_launch;
mod agent_scan;
mod artifact_links;
mod artifact_refs;
mod axe;
mod bead_decisions;
mod beads;
mod command_line;
mod config;
mod continuation;
mod editor_completion;
mod editor_content;
mod fleet;
mod fleet_attention;
mod json_bridge;
mod migration;
mod notifications;
mod plans;
mod prelude;
mod procs;
mod project_tag;
mod provider_policy;
mod query;
mod sudo;
mod telemetry;
mod vcs;

#[cfg(test)]
mod test_support;

use prelude::*;

pub use sase_core as core;

#[pymodule]
#[pyo3(name = "sase_core_rs")]
fn sase_core_rs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    query::register_query(m)?;
    command_line::register_command_line(m)?;
    agent_identity::register_agent_identity(m)?;
    vcs::register_vcs(m)?;
    config::register_config(m)?;
    editor_completion::register_editor_completion(m)?;
    editor_content::register_editor_content(m)?;
    agent_scan::register_agent_scan(m)?;
    agent_custody::register_agent_custody(m)?;
    bead_decisions::register_bead_decisions(m)?;
    beads::register_beads(m)?;
    plans::register_plans(m)?;
    artifact_refs::register_artifact_refs(m)?;
    artifact_links::register_artifact_links(m)?;
    migration::register_migration(m)?;
    notifications::register_notifications(m)?;
    procs::register_procs(m)?;
    project_tag::register_project_tag(m)?;
    agent_launch::register_agent_launch(m)?;
    provider_policy::register_provider_policy(m)?;
    axe::register_axe(m)?;
    agent_holds::register_agent_holds(m)?;
    fleet::register_fleet(m)?;
    fleet_attention::register_fleet_attention(m)?;
    sudo::register_sudo(m)?;
    continuation::register_continuation(m)?;
    telemetry::register_telemetry(m)?;
    Ok(())
}
