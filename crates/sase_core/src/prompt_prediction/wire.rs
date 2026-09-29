//! Wire records for the next-word prompt prediction engine.
//!
//! All output structs carry `schema_version` so Python readers can reject
//! skullduggery from a stale core, following `prompt_history_filter::wire`.

use serde::{Deserialize, Serialize};

/// Wire schema for every struct in this module.
pub const PROMPT_PREDICTION_WIRE_SCHEMA_VERSION: u32 = 1;

/// Default recency half-life in days for corpus compile.
pub const DEFAULT_RECENCY_HALF_LIFE_DAYS: f64 = 14.0;
/// Default maximum context length in words (`<s>` counts as a context token).
pub const DEFAULT_MAX_CONTEXT_WORDS: usize = 4;
/// Default maximum successors kept per context.
pub const DEFAULT_MAX_SUCCESSORS_PER_CONTEXT: usize = 32;
/// Default stupid-backoff multiplier.
pub const DEFAULT_BACKOFF_ALPHA: f64 = 0.4;
/// Default extra mass for same-project partition observations.
pub const DEFAULT_PROJECT_BOOST: f64 = 1.0;
/// Default weight of the per-request draft source.
pub const DEFAULT_DRAFT_WEIGHT: f64 = 1.0;
/// Default candidate limit for prediction requests.
pub const DEFAULT_PREDICT_LIMIT: usize = 5;
/// Default maximum ghost length in words.
pub const DEFAULT_PREDICT_MAX_WORDS: usize = 4;

/// Corpus role for one model source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptPredictionSourceRole {
    History,
    Session,
    Archive,
}

impl PromptPredictionSourceRole {
    /// Canonical lowercase wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Session => "session",
            Self::Archive => "archive",
        }
    }

    /// Parse a lowercase role spelling.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "history" => Some(Self::History),
            "session" => Some(Self::Session),
            "archive" => Some(Self::Archive),
            _ => None,
        }
    }
}

/// One history row offered to the corpus compiler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionRowWire {
    pub text: String,
    pub epoch_seconds: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default)]
    pub cancelled: bool,
}

/// Options for [`crate::prompt_prediction::compile_prompt_prediction_corpus`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionCorpusOptionsWire {
    pub schema_version: u32,
    pub now_epoch: i64,
    #[serde(default = "default_recency_half_life")]
    pub recency_half_life_days: f64,
    #[serde(default = "default_max_context_words")]
    pub max_context_words: usize,
    #[serde(default = "default_max_successors")]
    pub max_successors_per_context: usize,
    #[serde(default)]
    pub prune_singleton_contexts: bool,
    #[serde(default)]
    pub excluded_words: Vec<String>,
}

fn default_recency_half_life() -> f64 {
    DEFAULT_RECENCY_HALF_LIFE_DAYS
}

fn default_max_context_words() -> usize {
    DEFAULT_MAX_CONTEXT_WORDS
}

fn default_max_successors() -> usize {
    DEFAULT_MAX_SUCCESSORS_PER_CONTEXT
}

impl Default for PromptPredictionCorpusOptionsWire {
    fn default() -> Self {
        Self {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            now_epoch: 0,
            recency_half_life_days: DEFAULT_RECENCY_HALF_LIFE_DAYS,
            max_context_words: DEFAULT_MAX_CONTEXT_WORDS,
            max_successors_per_context: DEFAULT_MAX_SUCCESSORS_PER_CONTEXT,
            prune_singleton_contexts: false,
            excluded_words: Vec::new(),
        }
    }
}

/// Model composition config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionModelConfigWire {
    pub schema_version: u32,
    #[serde(default = "default_backoff_alpha")]
    pub backoff_alpha: f64,
    #[serde(default = "default_project_boost")]
    pub project_boost: f64,
    #[serde(default = "default_draft_weight")]
    pub draft_weight: f64,
    #[serde(default = "default_reject_conflicts")]
    pub reject_conflicts: bool,
}

fn default_backoff_alpha() -> f64 {
    DEFAULT_BACKOFF_ALPHA
}

fn default_project_boost() -> f64 {
    DEFAULT_PROJECT_BOOST
}

fn default_draft_weight() -> f64 {
    DEFAULT_DRAFT_WEIGHT
}

fn default_reject_conflicts() -> bool {
    true
}

impl Default for PromptPredictionModelConfigWire {
    fn default() -> Self {
        Self {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            backoff_alpha: DEFAULT_BACKOFF_ALPHA,
            project_boost: DEFAULT_PROJECT_BOOST,
            draft_weight: DEFAULT_DRAFT_WEIGHT,
            reject_conflicts: true,
        }
    }
}

/// One next-word prediction request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionRequestWire {
    pub schema_version: u32,
    pub text_before_cursor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default = "default_predict_limit")]
    pub limit: usize,
    #[serde(default = "default_predict_max_words")]
    pub max_words: usize,
    #[serde(default = "default_confidence")]
    pub confidence: String,
    #[serde(default = "default_include_draft")]
    pub include_draft: bool,
}

fn default_predict_limit() -> usize {
    DEFAULT_PREDICT_LIMIT
}

fn default_predict_max_words() -> usize {
    DEFAULT_PREDICT_MAX_WORDS
}

fn default_confidence() -> String {
    "balanced".to_string()
}

fn default_include_draft() -> bool {
    true
}

/// Per-source share of one candidate's combined mass at its best order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PromptPredictionSourceSharesWire {
    #[serde(default)]
    pub history: f64,
    #[serde(default)]
    pub project: f64,
    #[serde(default)]
    pub session: f64,
    #[serde(default)]
    pub draft: f64,
    #[serde(default)]
    pub archive: f64,
}

/// One ranked next-word candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionCandidateWire {
    pub word: String,
    pub key: String,
    pub score: f64,
    pub probability: f64,
    pub support: u64,
    pub order: usize,
    pub source_shares: PromptPredictionSourceSharesWire,
    #[serde(default)]
    pub continuation: Vec<String>,
}

/// Next-word prediction result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionResultWire {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(default)]
    pub context_words: Vec<String>,
    #[serde(default)]
    pub confident: bool,
    #[serde(default)]
    pub ghost: Vec<String>,
    #[serde(default)]
    pub candidates: Vec<PromptPredictionCandidateWire>,
}

/// One prefix-rank request for current-word completion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPrefixRankRequestWire {
    pub schema_version: u32,
    pub text_before_word: String,
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default = "default_predict_limit")]
    pub limit: usize,
}

/// One prefix-rank match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPrefixRankMatchWire {
    pub word: String,
    pub key: String,
    pub score: f64,
    pub order: usize,
    pub support: u64,
}

/// Prefix-rank result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPrefixRankResultWire {
    pub schema_version: u32,
    #[serde(default)]
    pub context_words: Vec<String>,
    #[serde(default)]
    pub matches: Vec<PromptPrefixRankMatchWire>,
}

/// Default warm fraction for prequential replay: the oldest share of typed
/// rows that seed the builder before scoring starts.
pub const DEFAULT_REPLAY_WARM_FRACTION: f64 = 0.4;

/// Options for [`crate::prompt_prediction::evaluate_prompt_prediction_replay`].
///
/// The corpus fields mirror [`PromptPredictionCorpusOptionsWire`], the model
/// fields mirror [`PromptPredictionModelConfigWire`], and `warm_fraction`
/// splits the typed rows into the warm prefix and the scored suffix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionReplayOptionsWire {
    pub schema_version: u32,
    pub now_epoch: i64,
    #[serde(default = "default_recency_half_life")]
    pub recency_half_life_days: f64,
    #[serde(default = "default_max_context_words")]
    pub max_context_words: usize,
    #[serde(default = "default_max_successors")]
    pub max_successors_per_context: usize,
    #[serde(default)]
    pub prune_singleton_contexts: bool,
    #[serde(default)]
    pub excluded_words: Vec<String>,
    #[serde(default = "default_backoff_alpha")]
    pub backoff_alpha: f64,
    #[serde(default = "default_project_boost")]
    pub project_boost: f64,
    #[serde(default = "default_draft_weight")]
    pub draft_weight: f64,
    #[serde(default = "default_reject_conflicts")]
    pub reject_conflicts: bool,
    #[serde(default = "default_replay_warm_fraction")]
    pub warm_fraction: f64,
}

fn default_replay_warm_fraction() -> f64 {
    DEFAULT_REPLAY_WARM_FRACTION
}

impl Default for PromptPredictionReplayOptionsWire {
    fn default() -> Self {
        Self {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            now_epoch: 0,
            recency_half_life_days: DEFAULT_RECENCY_HALF_LIFE_DAYS,
            max_context_words: DEFAULT_MAX_CONTEXT_WORDS,
            max_successors_per_context: DEFAULT_MAX_SUCCESSORS_PER_CONTEXT,
            prune_singleton_contexts: false,
            excluded_words: Vec::new(),
            backoff_alpha: DEFAULT_BACKOFF_ALPHA,
            project_boost: DEFAULT_PROJECT_BOOST,
            draft_weight: DEFAULT_DRAFT_WEIGHT,
            reject_conflicts: true,
            warm_fraction: DEFAULT_REPLAY_WARM_FRACTION,
        }
    }
}

/// Gated metrics for one threshold setting: coverage (share of positions
/// gated), precision (share of gated positions whose top-1 is correct;
/// `None` when nothing gated), the ghost keystroke-savings upper bound, and
/// the gated-correct run-length distribution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionReplayGateMetricsWire {
    pub coverage: f64,
    pub precision: Option<f64>,
    pub savings: f64,
    pub run_mean: f64,
    pub run_p95: f64,
    pub run_max: u64,
}

/// One cohort slice of the replay report: ungated top-1/top-3 accuracy plus
/// gated metrics at each confidence preset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionReplayCohortWire {
    pub cohort: String,
    pub positions: u64,
    pub top1: f64,
    pub top3: f64,
    pub cautious: PromptPredictionReplayGateMetricsWire,
    pub balanced: PromptPredictionReplayGateMetricsWire,
    pub eager: PromptPredictionReplayGateMetricsWire,
}

/// One threshold-grid point swept over the recorded per-position evidence
/// without replaying. The novel tallies reuse the same records filtered to
/// the novel cohort, so preset calibration can constrain novel precision
/// without replaying.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionReplaySweepPointWire {
    pub min_p: f64,
    pub min_margin: f64,
    pub min_support: u64,
    pub coverage: f64,
    pub precision: Option<f64>,
    #[serde(default)]
    pub novel_coverage: f64,
    pub novel_precision: Option<f64>,
}

/// Aggregate-only prequential replay report. It never carries prompt text:
/// positions are counted, cohorts are named, and the sweep holds rates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptPredictionReplayReportWire {
    pub schema_version: u32,
    pub rows_total: u64,
    pub rows_typed: u64,
    pub rows_warmed: u64,
    pub rows_scored: u64,
    pub positions_total: u64,
    pub overall_top1: f64,
    pub overall_top3: f64,
    pub cautious: PromptPredictionReplayGateMetricsWire,
    pub balanced: PromptPredictionReplayGateMetricsWire,
    pub eager: PromptPredictionReplayGateMetricsWire,
    pub cohorts: Vec<PromptPredictionReplayCohortWire>,
    pub sweep: Vec<PromptPredictionReplaySweepPointWire>,
    pub latency_us_p50: u64,
    pub latency_us_p95: u64,
    pub corpus_bytes: u64,
    pub corpus_rows_used: u64,
    pub corpus_contexts: u64,
}

/// Corpus compile statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptPredictionCorpusStatsWire {
    pub schema_version: u32,
    pub rows_used: u64,
    pub rows_generated_skipped: u64,
    pub rows_duplicate_skipped: u64,
    pub tokens: u64,
    pub contexts: u64,
    pub successor_entries: u64,
    pub approx_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_defaults_match_spec() {
        let options = PromptPredictionCorpusOptionsWire::default();
        assert_eq!(options.recency_half_life_days, 14.0);
        assert_eq!(options.max_context_words, 4);
        assert_eq!(options.max_successors_per_context, 32);
        assert!(!options.prune_singleton_contexts);
        assert!(options.excluded_words.is_empty());

        let config = PromptPredictionModelConfigWire::default();
        assert_eq!(config.backoff_alpha, 0.4);
        assert_eq!(config.project_boost, 1.0);
        assert_eq!(config.draft_weight, 1.0);
        assert!(config.reject_conflicts);
    }

    #[test]
    fn source_role_round_trip() {
        for role in [
            PromptPredictionSourceRole::History,
            PromptPredictionSourceRole::Session,
            PromptPredictionSourceRole::Archive,
        ] {
            assert_eq!(
                PromptPredictionSourceRole::parse(role.as_str()),
                Some(role)
            );
        }
        assert_eq!(PromptPredictionSourceRole::parse("draft"), None);
    }

    #[test]
    fn result_wire_serde_round_trip() {
        let result = PromptPredictionResultWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            blocked_reason: None,
            context_words: vec!["help".to_string(), "me".to_string()],
            confident: true,
            ghost: vec!["implement".to_string()],
            candidates: vec![PromptPredictionCandidateWire {
                word: "implement".to_string(),
                key: "implement".to_string(),
                score: 0.8,
                probability: 0.8,
                support: 5,
                order: 2,
                source_shares: PromptPredictionSourceSharesWire {
                    history: 1.0,
                    ..Default::default()
                },
                continuation: Vec::new(),
            }],
        };
        let json = serde_json::to_string(&result).expect("serialize");
        let back: PromptPredictionResultWire =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(result, back);
    }
}
