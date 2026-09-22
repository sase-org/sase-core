//! Durable background-proc store and runtime retention.
pub mod runtime;
pub mod store;
pub mod wire;

pub use runtime::apply_proc_runtime_retention;
pub use store::{
    append_proc, begin_proc_settlement, claim_proc_supervisor, finish_proc,
    prune_procs, read_procs_snapshot, request_proc_stop, reserve_proc,
    update_proc, ProcStoreError, SERVICE_PROC_HISTORY_LIMIT,
};
pub use wire::{
    ProcAppendOutcomeWire, ProcFinishWire, ProcPruneOutcomeWire,
    ProcReserveOutcomeWire, ProcReserveWire, ProcRuntimeRetentionEntryWire,
    ProcRuntimeRetentionRequestWire, ProcRuntimeRetentionResultWire,
    ProcServiceWire, ProcSettlementWire, ProcStopRequestWire,
    ProcStoreSnapshotWire, ProcStoreStatsWire, ProcSupervisorClaimWire,
    ProcUpdateOutcomeWire, ProcUpdateWire, ProcWire, XpromptProcMetaWire,
    PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION, PROC_WIRE_SCHEMA_VERSION,
    SUPPORTED_PROC_WIRE_SCHEMA_VERSIONS,
};
