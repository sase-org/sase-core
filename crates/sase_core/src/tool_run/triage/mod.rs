//! Durable failure items: pure extractors plus store-backed triage tables.

mod classify;
mod extract;
mod extractors;
mod failures;
mod normalize;
mod stage;
#[cfg(test)]
pub(crate) mod tests;
mod verdict;
pub(crate) mod wire;

pub use classify::{
    tool_run_triage_classify, ToolRunTriageClassifyLabelWire,
    ToolRunTriageClassifyRequestWire, ToolRunTriageClassifyResultWire,
    ToolRunTriageEvidenceItemWire, ToolRunTriageEvidenceRunWire,
    ToolRunTriageFlakeEntryWire, ToolRunTriageKnobsWire,
    ToolRunTriageOwnerCandidateWire, ToolRunTriageSubjectItemWire,
    ToolRunTriageSubjectRunWire, TOOL_RUN_TRIAGE_LOOKBACK_SECS,
    TOOL_RUN_TRIAGE_RULE_VERSION, TOOL_RUN_TRIAGE_TEST_EXTRACTORS,
};
pub use extract::{
    compare_triage_signatures, extract_triage_items, extractor_version,
    triage_signature,
};
pub use failures::{
    ToolRunFailuresGroupWire, ToolRunFailuresRequestWire,
    ToolRunFailuresResultWire,
};
pub use normalize::{clean_locator_path, display_text, normalize_line};
pub use stage::{
    ToolRunTriageSettleRequestWire, ToolRunTriageSettleResultWire,
    ToolRunTriageStageInputWire, ToolRunTriageStageRequestWire,
    ToolRunTriageStageResultWire,
};
pub use verdict::{
    tool_run_triage_verdict, ToolRunTriageFailureKindWire,
    ToolRunTriageVerdictItemWire, ToolRunTriageVerdictRequestWire,
    ToolRunTriageVerdictResultWire, ToolRunTriageVerdictWire,
};
pub use wire::{
    ToolRunTriageClassWire, ToolRunTriageContinuationModeWire,
    ToolRunTriageDecisionKindWire, ToolRunTriageDecisionWire,
    ToolRunTriageExtractRequestWire, ToolRunTriageExtractResultWire,
    ToolRunTriageExtractionStatusWire, ToolRunTriageItemWire,
    ToolRunTriageLabelWire, ToolRunTriageRecordRequestWire,
    ToolRunTriageRecordResultWire, ToolRunTriageRefusalWire,
    ToolRunTriageRunFactsWire, ToolRunTriageShowRequestWire,
    ToolRunTriageShowResultWire, ToolRunTriageStageFactsWire,
    ToolRunTriageStageRecordWire, TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS,
    TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};
