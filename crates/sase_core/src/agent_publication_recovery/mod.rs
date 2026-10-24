//! Deterministic policy for publication recovery, failure accounting, and
//! bounded drain selection.
//!
//! The host owns the outbox lock, Git transaction, snapshot reads, and page
//! writes. This module classifies publication errors, decides whether a
//! failure may charge request rows, selects which terminal rows to revive,
//! selects a bounded drain set, classifies deferred-prompt obligations,
//! decides whether a request is fulfilled, and classifies stale-replay
//! preservation conflicts.

mod accounting;
mod classify;
mod complete;
mod replay;
mod select;
mod stale;
mod support;
pub mod wire;

#[cfg(test)]
mod tests;

pub use accounting::decide_publication_failure_accounting;
pub use classify::{
    classify_publication_error, classify_publication_error_class,
};
pub use complete::{
    classify_deferred_prompt_obligation, decide_publication_request_completion,
};
pub use replay::{
    classify_replay_path, classify_replay_paths, decide_replay_apply,
    decide_replay_byte_divergence, decide_replay_hood_merge,
};
pub use select::{select_publication_drain, select_publication_retries};
pub use stale::classify_stale_publication_request;
pub use wire::{
    DeferredPromptClassifyRequestWire, DeferredPromptClassifyResponseWire,
    PublicationCompletionRequestWire, PublicationCompletionResponseWire,
    PublicationContainerObservationWire, PublicationDrainSelectionRequestWire,
    PublicationDrainSelectionResponseWire, PublicationErrorClassifyRequestWire,
    PublicationErrorClassifyResponseWire,
    PublicationFailureAccountingRequestWire,
    PublicationFailureAccountingResponseWire, PublicationFailureChargeWire,
    PublicationFailureRowWire, PublicationLogicalKeyWire,
    PublicationPageObservationWire, PublicationRecoveryError,
    PublicationRequestIdentityWire, PublicationRetryRowWire,
    PublicationRetrySelectedWire, PublicationRetrySelectionRequestWire,
    PublicationRetrySelectionResponseWire, PublicationRunObservationWire,
    ReplayApplyDecisionRequestWire, ReplayApplyDecisionResponseWire,
    ReplayByteDivergenceRequestWire, ReplayByteDivergenceResponseWire,
    ReplayHoodMergeRequestWire, ReplayHoodMergeResponseWire,
    ReplayPathClassWire, ReplayPathClassifyRequestWire,
    ReplayPathClassifyResponseWire, StalePublicationRepairRequestWire,
    StalePublicationRepairResponseWire, PROMPT_STATUS_ALREADY_ARCHIVED,
    PROMPT_STATUS_FAILED, PROMPT_STATUS_NOT_APPLICABLE, PROMPT_STATUS_RESTORED,
    PROMPT_STATUS_UNAVAILABLE, PUBLICATION_ERROR_INDEX_LOCK,
    PUBLICATION_ERROR_LOCK_BUSY, PUBLICATION_ERROR_MANIFEST_MISMATCH,
    PUBLICATION_ERROR_OTHER, PUBLICATION_ERROR_REBASE_CONFLICT,
    PUBLICATION_ERROR_TIMEOUT, PUBLICATION_ERROR_UNKNOWN,
    PUBLICATION_ERROR_UNSTAGED_CHANGES, PUBLICATION_FAILURE_SCOPE_HOOD,
    PUBLICATION_FAILURE_SCOPE_REPOSITORY, PUBLICATION_FAILURE_SCOPE_UNKNOWN,
    PUBLICATION_KIND_RUN, PUBLICATION_KIND_SESSION,
    PUBLICATION_LIFECYCLE_ACTIVE, PUBLICATION_LIFECYCLE_QUARANTINED,
    PUBLICATION_LIFECYCLE_RETIRED, PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
    PUBLICATION_SCOPE_LINKED, PUBLICATION_SCOPE_REVISION,
    REPLAY_APPLY_ACTION_ALREADY_COMPLETE, REPLAY_APPLY_ACTION_APPLY,
    REPLAY_APPLY_ACTION_REFUSE, REPLAY_APPLY_ACTION_REPORT,
    REPLAY_BYTES_KEEP_CURRENT, REPLAY_BYTES_KEEP_PREVIOUS, REPLAY_BYTES_REFUSE,
    REPLAY_HOOD_ACTION_MERGE, REPLAY_HOOD_ACTION_REFUSE,
    REPLAY_PATH_ARCHIVE_OBJECT, REPLAY_PATH_BROWSING,
    REPLAY_PATH_OWNER_CONTENT, REPLAY_PATH_UNKNOWN, STALE_REPAIR_ACTION_LINKED,
    STALE_REPAIR_ACTION_REPAIRED, STALE_REPAIR_ACTION_UNCHANGED,
    STALE_REPAIR_ACTION_UNRESOLVED,
};
