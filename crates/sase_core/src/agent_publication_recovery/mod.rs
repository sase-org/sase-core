//! Deterministic policy for publication recovery, failure accounting, and
//! bounded drain selection.
//!
//! The host owns the outbox lock, Git transaction, snapshot reads, and page
//! writes. This module classifies publication errors, decides whether a
//! failure may charge request rows, selects which terminal rows to revive,
//! selects a bounded drain set, classifies deferred-prompt obligations, and
//! decides whether a request is fulfilled.

mod accounting;
mod classify;
mod complete;
mod select;
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
pub use select::{select_publication_drain, select_publication_retries};
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
    PROMPT_STATUS_ALREADY_ARCHIVED, PROMPT_STATUS_FAILED,
    PROMPT_STATUS_NOT_APPLICABLE, PROMPT_STATUS_RESTORED,
    PROMPT_STATUS_UNAVAILABLE, PUBLICATION_ERROR_INDEX_LOCK,
    PUBLICATION_ERROR_LOCK_BUSY, PUBLICATION_ERROR_MANIFEST_MISMATCH,
    PUBLICATION_ERROR_OTHER, PUBLICATION_ERROR_REBASE_CONFLICT,
    PUBLICATION_ERROR_TIMEOUT, PUBLICATION_ERROR_UNKNOWN,
    PUBLICATION_ERROR_UNSTAGED_CHANGES, PUBLICATION_FAILURE_SCOPE_HOOD,
    PUBLICATION_FAILURE_SCOPE_REPOSITORY, PUBLICATION_FAILURE_SCOPE_UNKNOWN,
    PUBLICATION_KIND_RUN, PUBLICATION_KIND_SESSION,
    PUBLICATION_LIFECYCLE_ACTIVE, PUBLICATION_LIFECYCLE_QUARANTINED,
    PUBLICATION_LIFECYCLE_RETIRED, PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
};
