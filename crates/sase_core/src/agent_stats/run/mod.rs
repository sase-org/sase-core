//! Run-stats aggregation over durable artifact-index records.
//!
//! Split from the former 3,732-line `run.rs` along domain seams:
//! [`query`](query) drives, [`lifecycle`](lifecycle) classifies,
//! [`folds`](folds) and [`xprompts`](xprompts) accumulate
//! per-dimension state, [`attribution`](attribution) resolves
//! project/patch work, and [`finishing`](finishing) ranks. Shared
//! accumulator types live in [`types`](types).

mod attribution;
mod finishing;
mod folds;
mod lifecycle;
mod query;
#[cfg(test)]
mod tests;
mod types;
mod xprompts;

pub(super) use lifecycle::parse_timestamp;
pub use query::query_run_stats;
