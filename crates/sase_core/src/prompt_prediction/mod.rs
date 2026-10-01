//! Next-word prompt prediction: prose n-gram model over typed prompts.
//!
//! The tokenizer, legacy origin heuristic, compiled corpus, and
//! multi-source stupid-backoff model live in sibling modules; this facade
//! only re-exports them. The host resolves history rows off the event loop;
//! every operation here is pure and synchronous.

pub mod corpus;

pub mod model;
pub mod origin;
pub mod predict;
pub mod replay;
pub mod tokenize;
pub mod wire;

#[cfg(test)]
mod tests;

pub use corpus::{
    compile_prompt_prediction_corpus, recency_weight,
    CompiledPromptPredictionCorpus, PromptPredictionBuilder,
    PromptSuccessorSource,
};
pub use model::PromptPredictionModel;
pub use origin::{looks_generated, MARKER_LAND_EPIC, MARKER_WORK_PHASE_BEAD};
pub use predict::{
    parse_confidence, score_and_gate, ConfidencePreset, DraftCounts,
    OrderSuffixes, ScoringQuery, MODEL_MAX_CONTEXT_WORDS, PRESET_BALANCED,
    PRESET_CAUTIOUS, PRESET_EAGER,
};
pub use replay::evaluate_prompt_prediction_replay;
pub use tokenize::{
    canonical_surface, classify_word, split_partial_word, tokenize_cursor_text,
    tokenize_prompt_text, BLOCKED_FRONTMATTER, BLOCKED_NO_WORD_CONTEXT,
    BLOCKED_STRUCTURAL_TAIL, BLOCKED_UNCLOSED_ALTERNATION,
    BLOCKED_UNCLOSED_CODE_SPAN, BLOCKED_UNCLOSED_FENCE, BLOCKED_UNCLOSED_JINJA,
    SEQUENCE_START,
};
pub use wire::{
    PromptPredictionCandidateWire, PromptPredictionCorpusOptionsWire,
    PromptPredictionCorpusStatsWire, PromptPredictionModelConfigWire,
    PromptPredictionReplayCohortWire, PromptPredictionReplayGateMetricsWire,
    PromptPredictionReplayOptionsWire, PromptPredictionReplayReportWire,
    PromptPredictionReplaySweepPointWire, PromptPredictionRequestWire,
    PromptPredictionResultWire, PromptPredictionRowWire,
    PromptPredictionSourceRole, PromptPredictionSourceSharesWire,
    PromptPredictionWordCompletionWire, PromptPrefixRankMatchWire,
    PromptPrefixRankRequestWire, PromptPrefixRankResultWire,
    PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
};
