//! SQLite materialized view for agent artifact summaries.
//!
//! The artifact tree remains the source of truth. This module stores one
//! row per artifact directory with denormalized query fields and the
//! scanner's canonical `AgentArtifactRecordWire` JSON payload so indexed
//! queries can return loader-equivalent records without walking every
//! historical timestamp directory.

mod alias_history;
mod candidates;
mod dismissal;
mod index_wire;
mod lineage;
mod maintenance;
mod output_variables;
mod query;
mod record_summary;
mod refresh;
mod selection;
mod storage;

#[cfg(test)]
mod tests;

pub use alias_history::query_agent_alias_history;

pub use dismissal::{
    reconcile_agent_artifact_index_dismissed_agent_session_members,
    replace_agent_artifact_index_dismissed_agents,
    replace_agent_artifact_index_dismissed_agents_with_force,
    AgentArtifactIndexDismissalReconcileWire,
};

pub use index_wire::{
    AgentAliasHistoryGroupWire, AgentAliasHistoryLimitWire,
    AgentAliasHistoryQueryWire, AgentAliasHistoryWire, AgentAliasRunWire,
    AgentArtifactCandidateFieldWire, AgentArtifactCandidateFilterWire,
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
    AgentArtifactIndexStatusWire, AgentArtifactIndexUpdateWire,
    AgentArtifactIndexVacuumWire, AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION,
    AGENT_ARTIFACT_INDEX_SCHEMA_VERSION, DEFAULT_HIDDEN_TERMINAL_HOT_ROWS,
};

pub use lineage::{
    query_related_agent_artifact_dirs, resolve_agent_session_dismissal_lineage,
    AgentSessionDismissalLineageCandidateWire,
    AgentSessionDismissalLineageResultWire,
};

pub use maintenance::{
    delete_agent_artifact_index_row,
    delete_agent_artifact_index_row_with_busy_timeout,
    prune_hidden_terminal_agent_artifact_index_rows,
    rebuild_agent_artifact_index,
    terminalize_stale_active_agent_artifact_index_rows,
    upsert_agent_artifact_index_row,
};

pub use output_variables::query_agent_output_variable_history;

pub use query::{
    agent_artifact_index_status, find_gate_shell_by_gate_id,
    load_agent_artifact_records, query_agent_artifact_index,
    read_agent_artifact_index_meta, vacuum_agent_artifact_index,
    write_agent_artifact_index_meta,
};

pub(crate) use maintenance::cl_name_is_unknownish;

pub(crate) use output_variables::{
    canonical_output_variable_json, load_output_variable_occurrences,
};

// Test instrumentation and the shared SQL placeholder helper live
// here so every index submodule reaches them through `super`.

#[cfg(test)]
thread_local! {
    static LAST_INDEX_SQL_STATEMENTS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

fn record_index_sql_statements(count: u64) {
    let _ = count;
    #[cfg(test)]
    LAST_INDEX_SQL_STATEMENTS.with(|cell| cell.set(count));
}

#[cfg(test)]
fn last_index_sql_statements() -> u64 {
    LAST_INDEX_SQL_STATEMENTS.with(|cell| cell.get())
}

#[cfg(test)]
thread_local! {
    /// Records decoded (`record_json` parses) by the last
    /// [`find_gate_shell_by_gate_id`] call. A warm-cache lookup that stays
    /// fast could still be decoding every historical row in Rust after an
    /// unfiltered SQL scan; this proves the `WHERE gate_shell_id = ?`
    /// predicate — not warm caches or an incidentally fast host — is what
    /// keeps the lookup bounded as unrelated history grows.
    static LAST_GATE_SHELL_LOOKUP_RECORDS_DECODED: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

fn record_gate_shell_lookup_records_decoded(count: u64) {
    let _ = count;
    #[cfg(test)]
    LAST_GATE_SHELL_LOOKUP_RECORDS_DECODED.with(|cell| cell.set(count));
}

#[cfg(test)]
fn last_gate_shell_lookup_records_decoded() -> u64 {
    LAST_GATE_SHELL_LOOKUP_RECORDS_DECODED.with(|cell| cell.get())
}

fn placeholders(len: usize) -> String {
    std::iter::repeat_n("?", len).collect::<Vec<_>>().join(", ")
}

pub(super) const AGENT_SESSION_INDEX_COLUMN: &str = "agent_session";
