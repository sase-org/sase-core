//! Stranded-work reducer: facts-to-verdicts for epic work nothing will resume.
//!
//! A pure function over bead and session-member facts. Python collects the
//! facts (process liveness, marker files, finalizer declarations); this
//! module maps them to `owned`/`delegated`/`settling`/`stranded`/`blocked`/
//! `unknown` verdicts with causes, then propagates `blocked` upward through
//! candidate ancestors. It performs no I/O.

mod classify;
mod propagate;
#[cfg(test)]
mod tests;
mod wire;

pub use classify::reduce_stranded_work;
pub use propagate::apply_stranded_propagation;
pub use wire::{
    StrandedWorkBeadFactsWire, StrandedWorkInputWire,
    StrandedWorkMemberFactsWire, StrandedWorkReportWire,
    StrandedWorkVerdictWire, STRANDED_CAUSE_CHILD_LANDED,
    STRANDED_CAUSE_CLOSE_FAILED, STRANDED_CAUSE_FAILED,
    STRANDED_CAUSE_FOLLOWUP_FAILED, STRANDED_CAUSE_KEPT, STRANDED_CAUSE_LOST,
    STRANDED_CAUSE_STOPPED, STRANDED_CAUSE_UNCLOSED, STRANDED_STATE_BLOCKED,
    STRANDED_STATE_DELEGATED, STRANDED_STATE_OWNED, STRANDED_STATE_SETTLING,
    STRANDED_STATE_STRANDED, STRANDED_STATE_UNKNOWN,
    STRANDED_UNKNOWN_NO_RECORDS, STRANDED_UNKNOWN_RECORDS_UNAVAILABLE,
    STRANDED_WORK_WIRE_SCHEMA_VERSION,
};
