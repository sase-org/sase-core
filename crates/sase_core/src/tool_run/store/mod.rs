//! SQLite ToolRun store: event+projection transactions, reconciliation,
//! retention, and query side-effect boundaries.
//!
//! Split from the former 3,229-line `store.rs` along domain seams:
//! [`connection`](connection) owns schema, connection setup, and corruption
//! quarantine; [`lifecycle`](lifecycle) owns run writes; [`query`](query)
//! owns run queries and loaders; and [`retention`](retention) owns retention
//! preview, apply, and log reclamation.

pub(crate) mod connection;
mod failures;
mod handoff;
mod lifecycle;
mod query;
mod reconcile;
mod retention;
#[cfg(test)]
mod tests;
mod triage;
mod triage_stage;

pub use failures::tool_run_failures;
pub use handoff::{claim, request_stop};
pub use lifecycle::{append_event, begin, finish, observe};
pub use query::{list_runs, show_run, store_stats, summarize};
pub use reconcile::reconcile;
pub use retention::{retention_apply, retention_preview};
pub use triage::{triage_record, triage_show};
pub use triage_stage::{triage_settle, triage_stage};
