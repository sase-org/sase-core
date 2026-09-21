//! Pure runner-capacity accounting and queue eligibility.
//!
//! Host adapters own locking, filesystem markers, and process liveness. This
//! module owns the deterministic projection from already-collected record facts
//! to occupied capacity, waiters, blockers, and the next admissible waiter.
//!
//! Split from the former 2,938-line `runner_capacity.rs` along domain seams:
//! [`wire`](wire) owns the capacity wire records and schema version;
//! [`snapshot`](snapshot) owns the snapshot entry point;
//! [`claims`](claims) owns claim building and claim lineage;
//! [`records`](records) owns record predicates, weights, and the record index;
//! [`waiters`](waiters) owns waiter evaluation, blockers, and ordering;
//! [`candidate`](candidate) owns the candidate admission decision;
//! [`holds`](holds) owns hold-barrier blocking; and
//! [`capacity_math`](capacity_math) owns the floating-point capacity math.

mod candidate;
mod capacity_math;
mod claims;
mod holds;
mod records;
mod snapshot;
#[cfg(test)]
mod tests;
mod waiters;
mod wire;

pub use snapshot::runner_capacity_snapshot;
pub use wire::{
    runner_capacity_policy_schema_version, RunnerCapacityBlockerWire,
    RunnerCapacityCandidateDecisionWire, RunnerCapacityClaimWire,
    RunnerCapacityDiagnosticWire, RunnerCapacityRecordWire,
    RunnerCapacityRequestWire, RunnerCapacitySnapshotWire,
    RunnerCapacityWaiterWire, DEFAULT_WAIT_PRIORITY,
    RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
};
