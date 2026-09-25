//! Durable failure items: pure extractors plus store-backed triage tables.

mod extract;
mod extractors;
mod normalize;
#[cfg(test)]
pub(crate) mod tests;
pub(crate) mod wire;

pub use extract::{
    compare_triage_signatures, extract_triage_items, extractor_version,
    triage_signature,
};
pub use normalize::{clean_locator_path, display_text, normalize_line};
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
