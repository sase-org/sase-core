//! Finalizer node projection: decoders, precedence, and selection.
//!
//! The shared `finalizer::run_view` projection behind
//! `project_finalizer_node_view`. Attempt, operation, and evidence detail
//! lands in `core-run-view-detail`.

pub mod decode;
pub mod detail;
pub mod evidence;
pub mod precedence;
pub mod selection;
#[cfg(test)]
mod tests;
pub mod wire;

pub use decode::RunViewError;
pub use detail::project_finalizer_node_view;
pub use evidence::{classify_evidence_kind, RunViewEvidenceWire};
pub use wire::{
    FinalizerNodeViewRequestWire, FinalizerNodeViewWire, RunViewAppearanceWire,
    RunViewDeclarationWire, RunViewDispositionWire, RunViewDriftWire,
    RunViewFileInputWire, RunViewInstanceInputWire, RunViewNodeInstanceWire,
    RunViewRecoveryTurnWire, RunViewRunInputWire, RunViewRunInstanceWire,
    RunViewRunKindWire, RunViewRunWire, RunViewTextInputWire,
    RunViewUnselectedWire, RUN_VIEW_MAX_BYTES, RUN_VIEW_TEXT_CAP_CHARS,
    RUN_VIEW_WIRE_SCHEMA_VERSION,
};
