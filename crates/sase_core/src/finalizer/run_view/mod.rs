//! Finalizer node projection: decoders, precedence, selection, and
//! per-instance detail.
//!
//! The shared `finalizer::run_view` projection behind
//! `project_finalizer_node_view`. Attempt, operation, evidence, and
//! diagnostic detail lives in `detail_content`; multi-run node composition
//! follows the D10 supersede rule in `detail`.

pub mod decode;
pub mod detail;
pub mod detail_content;
pub mod evidence;
pub mod precedence;
pub mod selection;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_detail;
pub mod wire;

pub use decode::RunViewError;
pub use detail::project_finalizer_node_view;
pub use evidence::{
    classify_evidence_kind, select_headline, typed_evidence,
    RunViewEvidenceWire,
};
pub use wire::{
    FinalizerNodeViewRequestWire, FinalizerNodeViewWire, RunViewAppearanceWire,
    RunViewAttemptWire, RunViewDeclarationWire, RunViewDeferralWire,
    RunViewDispositionWire, RunViewDriftWire, RunViewFileInputWire,
    RunViewInstanceDiagnosticWire, RunViewInstanceInputWire, RunViewLogWire,
    RunViewNodeInstanceWire, RunViewOperationWire, RunViewRecoveryTurnWire,
    RunViewRunInputWire, RunViewRunInstanceWire, RunViewRunKindWire,
    RunViewRunWire, RunViewStepWire, RunViewTextInputWire,
    RunViewUnselectedWire, RUN_VIEW_MAX_BYTES, RUN_VIEW_TEXT_CAP_CHARS,
    RUN_VIEW_WIRE_SCHEMA_VERSION,
};
