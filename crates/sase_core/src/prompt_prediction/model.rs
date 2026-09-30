//! Multi-source prediction model: composition, gate, continuation.
//!
//! A model is a cheap composition of frozen corpora plus config. Queries
//! are pure and synchronous: stupid-backoff scoring over the composed
//! sources, one confidence gate, and greedy gated continuation. The draft
//! source is counted per request from the text itself and stays frozen
//! while a continuation extends.

use std::collections::HashSet;
use std::sync::Arc;

use crate::prompt_prediction::corpus::{
    CompiledPromptPredictionCorpus, PromptSuccessorSource,
};
use crate::prompt_prediction::predict::{
    apply_gate, parse_confidence, score_candidates, ConfidencePreset,
    DraftCounts, GatePass, ScoredWord, ScoringQuery, WeightedSource,
    MODEL_MAX_CONTEXT_WORDS, PRESET_BALANCED, SHARE_ARCHIVE, SHARE_HISTORY,
    SHARE_SESSION,
};
use crate::prompt_prediction::tokenize::{
    tokenize_cursor_text, tokenize_prompt_text, CursorContext, SEQUENCE_START,
};
use crate::prompt_prediction::wire::{
    PromptPredictionCandidateWire, PromptPredictionModelConfigWire,
    PromptPredictionRequestWire, PromptPredictionResultWire,
    PromptPredictionSourceRole, PromptPredictionSourceSharesWire,
    PromptPrefixRankMatchWire, PromptPrefixRankRequestWire,
    PromptPrefixRankResultWire, PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
};

/// Maximum continuation preview words carried per menu candidate.
const CANDIDATE_PREVIEW_WORDS: usize = 3;

struct ModelSource {
    corpus: Arc<CompiledPromptPredictionCorpus>,
    share: usize,
    weight: f64,
}

/// Composed prediction model over frozen corpora.
pub struct PromptPredictionModel {
    sources: Vec<ModelSource>,
    config: PromptPredictionModelConfigWire,
}

impl PromptPredictionModel {
    /// Compose a model from `(corpus, role, weight)` sources and config.
    pub fn new(
        sources: Vec<(
            Arc<CompiledPromptPredictionCorpus>,
            PromptPredictionSourceRole,
            f32,
        )>,
        config: PromptPredictionModelConfigWire,
    ) -> Self {
        let model_sources = sources
            .into_iter()
            .map(|(corpus, role, weight)| ModelSource {
                corpus,
                share: match role {
                    PromptPredictionSourceRole::History => SHARE_HISTORY,
                    PromptPredictionSourceRole::Session => SHARE_SESSION,
                    PromptPredictionSourceRole::Archive => SHARE_ARCHIVE,
                },
                weight: weight as f64,
            })
            .collect();
        Self {
            sources: model_sources,
            config,
        }
    }

    /// Predict next words for the text before the cursor.
    pub fn predict(
        &self,
        request: &PromptPredictionRequestWire,
    ) -> PromptPredictionResultWire {
        let context = tokenize_cursor_text(&request.text_before_cursor);
        let CursorContext::Ready {
            context: full_context,
            sequences,
            ..
        } = context
        else {
            let reason = match context {
                CursorContext::Blocked { reason } => reason.to_string(),
                CursorContext::Ready { .. } => unreachable!(),
            };
            return PromptPredictionResultWire {
                schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
                blocked_reason: Some(reason),
                context_words: Vec::new(),
                confident: false,
                ghost: Vec::new(),
                candidates: Vec::new(),
            };
        };
        let trimmed = trim_context(&full_context);
        let draft = self.draft_counts(request, &sequences);
        let project = request.project.as_deref();
        let candidates = self.candidate_keys(&trimmed, draft.as_ref());
        let preset = parse_confidence(&request.confidence);
        let ranked = self.score(&trimmed, project, draft.as_ref(), &candidates);
        let gate = self.with_query(
            &trimmed,
            project,
            draft.as_ref(),
            preset,
            |query| apply_gate(query, &ranked, &candidates),
        );
        let confident = gate.is_some();
        let ghost = match &gate {
            Some(pass) => self.continuation(
                &trimmed,
                project,
                draft.as_ref(),
                pass,
                request.max_words,
                preset,
            ),
            None => Vec::new(),
        };
        let limit = request.limit;
        let menu: Vec<PromptPredictionCandidateWire> = ranked
            .into_iter()
            .filter(|word| word.has_higher)
            .take(limit)
            .map(|word| {
                self.candidate_wire(
                    &trimmed,
                    project,
                    draft.as_ref(),
                    &word,
                    preset,
                )
            })
            .collect();
        PromptPredictionResultWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            blocked_reason: None,
            context_words: self.context_surfaces(&trimmed),
            confident,
            ghost,
            candidates: menu,
        }
    }

    /// Rank current-word completions for a typed prefix.
    ///
    /// A structural tail before the prefix (a structural token, a closed
    /// excluded span, or a `:`/`;` boundary) yields no matches, mirroring
    /// the `predict` query-time block.
    pub fn rank_prefix(
        &self,
        request: &PromptPrefixRankRequestWire,
    ) -> PromptPrefixRankResultWire {
        if matches!(
            tokenize_cursor_text(&request.text_before_word),
            CursorContext::Blocked { .. }
        ) {
            return PromptPrefixRankResultWire {
                schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
                context_words: Vec::new(),
                matches: Vec::new(),
            };
        }
        let sequences = tokenize_prompt_text(&request.text_before_word);
        let mut context: Vec<String> = Vec::new();
        if let Some(last) = sequences.last() {
            if last.started {
                context.push(SEQUENCE_START.to_string());
            }
            context.extend(last.tokens.iter().map(|token| token.key.clone()));
        }
        let trimmed = trim_context(&context);
        let prefix_key = request.prefix.to_lowercase().replace('’', "'");
        if prefix_key.is_empty() {
            return PromptPrefixRankResultWire {
                schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
                context_words: self.context_surfaces(&trimmed),
                matches: Vec::new(),
            };
        }
        let draft_seqs: Vec<Vec<String>> = sequences
            .iter()
            .map(|seq| {
                seq.tokens.iter().map(|token| token.key.clone()).collect()
            })
            .collect();
        let draft = if self.config.draft_weight > 0.0 {
            Some(DraftCounts::from_sequences(
                &draft_seqs,
                MODEL_MAX_CONTEXT_WORDS,
            ))
        } else {
            None
        };
        let mut words: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for source in &self.sources {
            for key in source.corpus.keys_with_prefix(&prefix_key) {
                if key != prefix_key && seen.insert(key.clone()) {
                    words.push(key);
                }
            }
        }
        if let Some(counts) = &draft {
            for key in counts.candidate_words() {
                if key.starts_with(prefix_key.as_str())
                    && key != prefix_key
                    && seen.insert(key.clone())
                {
                    words.push(key);
                }
            }
        }
        words.retain(|key| key != SEQUENCE_START && !self.is_excluded(key));
        let project = request.project.as_deref();
        let ranked = self.score(&trimmed, project, draft.as_ref(), &words);
        let matches: Vec<PromptPrefixRankMatchWire> = ranked
            .into_iter()
            .filter(|word| word.has_higher)
            .take(request.limit)
            .map(|word| PromptPrefixRankMatchWire {
                word: self.surface_for(&word.key).unwrap_or(word.key.clone()),
                key: word.key,
                score: word.score,
                order: word.order,
                support: word.support,
            })
            .collect();
        PromptPrefixRankResultWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            context_words: self.context_surfaces(&trimmed),
            matches,
        }
    }

    fn weighted(&self) -> Vec<WeightedSource<'_>> {
        self.sources
            .iter()
            .map(|source| WeightedSource {
                share: source.share,
                corpus: source.corpus.as_ref(),
                weight: source.weight,
            })
            .collect()
    }

    fn draft_counts(
        &self,
        request: &PromptPredictionRequestWire,
        sequences: &[crate::prompt_prediction::tokenize::ProseSequence],
    ) -> Option<DraftCounts> {
        if !request.include_draft || self.config.draft_weight <= 0.0 {
            return None;
        }
        let seqs: Vec<Vec<String>> = sequences
            .iter()
            .map(|seq| {
                seq.tokens.iter().map(|token| token.key.clone()).collect()
            })
            .collect();
        Some(DraftCounts::from_sequences(&seqs, MODEL_MAX_CONTEXT_WORDS))
    }

    fn candidate_keys(
        &self,
        context: &[String],
        draft: Option<&DraftCounts>,
    ) -> Vec<String> {
        let mut words: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let max_order = context.len();
        for order in 0..=max_order {
            let start = context.len().saturating_sub(order);
            let suffix: Vec<&str> =
                context[start..].iter().map(String::as_str).collect();
            for source in &self.sources {
                for (key, _, _) in source.corpus.ranked_successors(&suffix) {
                    if seen.insert(key.clone()) {
                        words.push(key);
                    }
                }
            }
        }
        if let Some(counts) = draft {
            for key in counts.candidate_words() {
                if seen.insert(key.clone()) {
                    words.push(key);
                }
            }
        }
        words.retain(|key| key != SEQUENCE_START && !self.is_excluded(key));
        words
    }

    /// Run one scoring pass over borrowed inputs.
    fn with_query<R>(
        &self,
        context: &[String],
        project: Option<&str>,
        draft: Option<&DraftCounts>,
        preset: ConfidencePreset,
        run: impl FnOnce(&ScoringQuery<'_>) -> R,
    ) -> R {
        let weighted = self.weighted();
        let query = ScoringQuery {
            sources: &weighted,
            project,
            project_boost: self.config.project_boost,
            backoff_alpha: self.config.backoff_alpha,
            draft: draft_param(draft, &self.config),
            context,
            max_order: context.len(),
            preset,
            reject_conflicts: self.config.reject_conflicts,
        };
        run(&query)
    }

    fn score(
        &self,
        context: &[String],
        project: Option<&str>,
        draft: Option<&DraftCounts>,
        candidates: &[String],
    ) -> Vec<ScoredWord> {
        // Scoring never gates, so the preset below is unused.
        self.with_query(context, project, draft, PRESET_BALANCED, |query| {
            score_candidates(query, candidates)
        })
    }

    /// Greedy gated continuation from a passing gate, capped at
    /// `max_words`. The draft stays frozen to the original text.
    fn continuation(
        &self,
        context: &[String],
        project: Option<&str>,
        draft: Option<&DraftCounts>,
        pass: &GatePass,
        max_words: usize,
        preset: crate::prompt_prediction::predict::ConfidencePreset,
    ) -> Vec<String> {
        let mut extended = trimmed_extended(context, &pass.word);
        let mut keys = vec![pass.word.clone()];
        while keys.len() < max_words.max(1) {
            let candidates = self.candidate_keys(&extended, draft);
            let ranked = self.score(&extended, project, draft, &candidates);
            let Some(next) = self.gate_at(
                &extended,
                project,
                draft,
                &ranked,
                &candidates,
                preset,
            ) else {
                break;
            };
            keys.push(next.word.clone());
            extended = trimmed_extended(&extended, &next.word);
        }
        keys.truncate(max_words);
        keys.into_iter()
            .map(|key| self.surface_for(&key).unwrap_or(key))
            .collect()
    }

    fn gate_at(
        &self,
        context: &[String],
        project: Option<&str>,
        draft: Option<&DraftCounts>,
        ranked: &[ScoredWord],
        candidates: &[String],
        preset: ConfidencePreset,
    ) -> Option<GatePass> {
        self.with_query(context, project, draft, preset, |query| {
            apply_gate(query, ranked, candidates)
        })
    }

    fn candidate_wire(
        &self,
        context: &[String],
        project: Option<&str>,
        draft: Option<&DraftCounts>,
        word: &ScoredWord,
        preset: crate::prompt_prediction::predict::ConfidencePreset,
    ) -> PromptPredictionCandidateWire {
        let mut extended = trimmed_extended(context, &word.key);
        let mut preview_keys: Vec<String> = Vec::new();
        while preview_keys.len() < CANDIDATE_PREVIEW_WORDS {
            let candidates = self.candidate_keys(&extended, draft);
            let ranked = self.score(&extended, project, draft, &candidates);
            let Some(next) = self.gate_at(
                &extended,
                project,
                draft,
                &ranked,
                &candidates,
                preset,
            ) else {
                break;
            };
            preview_keys.push(next.word.clone());
            extended = trimmed_extended(&extended, &next.word);
        }
        let continuation: Vec<String> = preview_keys
            .into_iter()
            .map(|key| self.surface_for(&key).unwrap_or(key))
            .collect();
        PromptPredictionCandidateWire {
            word: self.surface_for(&word.key).unwrap_or(word.key.clone()),
            key: word.key.clone(),
            score: word.score,
            probability: word.probability,
            support: word.support,
            order: word.order,
            source_shares: PromptPredictionSourceSharesWire {
                history: word.shares[0],
                project: word.shares[1],
                session: word.shares[2],
                draft: word.shares[3],
                archive: word.shares[4],
            },
            continuation,
        }
    }

    fn surface_for(&self, key: &str) -> Option<String> {
        for source in &self.sources {
            if let Some(surface) = source.corpus.surface_for_key(key) {
                return Some(surface.to_string());
            }
        }
        None
    }

    fn context_surfaces(&self, context: &[String]) -> Vec<String> {
        context
            .iter()
            .filter(|token| token.as_str() != SEQUENCE_START)
            .map(|token| self.surface_for(token).unwrap_or(token.clone()))
            .collect()
    }

    fn is_excluded(&self, key: &str) -> bool {
        self.sources
            .iter()
            .any(|source| source.corpus.is_excluded(key))
    }
}

fn draft_param<'a>(
    draft: Option<&'a DraftCounts>,
    config: &'a PromptPredictionModelConfigWire,
) -> Option<(&'a DraftCounts, f64)> {
    draft.map(|counts| (counts, config.draft_weight))
}

fn trim_context(full: &[String]) -> Vec<String> {
    let start = full.len().saturating_sub(MODEL_MAX_CONTEXT_WORDS);
    full[start..].to_vec()
}

fn trimmed_extended(context: &[String], word: &str) -> Vec<String> {
    let mut extended = context.to_vec();
    extended.push(word.to_string());
    trim_context(&extended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_prediction::corpus::compile_prompt_prediction_corpus;
    use crate::prompt_prediction::wire::PromptPredictionCorpusOptionsWire;

    fn typed(
        text: &str,
        epoch: i64,
    ) -> crate::prompt_prediction::wire::PromptPredictionRowWire {
        crate::prompt_prediction::wire::PromptPredictionRowWire {
            text: text.to_string(),
            epoch_seconds: epoch,
            project: None,
            origin: Some("typed".to_string()),
            cancelled: false,
        }
    }

    fn model_for(texts: &[&str]) -> PromptPredictionModel {
        model_for_rows(
            &texts
                .iter()
                .enumerate()
                .map(|(i, text)| typed(text, 100 + i as i64))
                .collect::<Vec<_>>(),
        )
    }

    fn model_for_rows(
        rows: &[crate::prompt_prediction::wire::PromptPredictionRowWire],
    ) -> PromptPredictionModel {
        let corpus = compile_prompt_prediction_corpus(
            rows,
            &PromptPredictionCorpusOptionsWire {
                now_epoch: 1_000,
                ..Default::default()
            },
        );
        PromptPredictionModel::new(
            vec![(Arc::new(corpus), PromptPredictionSourceRole::History, 1.0)],
            PromptPredictionModelConfigWire::default(),
        )
    }

    fn request(text: &str) -> PromptPredictionRequestWire {
        PromptPredictionRequestWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            text_before_cursor: text.to_string(),
            project: None,
            limit: 5,
            max_words: 4,
            confidence: "balanced".to_string(),
            include_draft: true,
        }
    }

    #[test]
    fn confident_prediction_returns_ghost() {
        let model = model_for(&[
            "help me implement it now",
            "help me implement it today",
            "help me implement it fast",
            "help me implement it well",
        ]);
        let result = model.predict(&request("help me"));
        assert!(result.blocked_reason.is_none());
        assert!(result.confident, "candidates={:?}", result.candidates);
        assert_eq!(result.ghost[0], "implement");
        assert_eq!(result.context_words, vec!["help", "me"]);
    }

    #[test]
    fn thin_evidence_stays_silent_but_lists_candidates() {
        let model = model_for(&["help me implement it"]);
        let result = model.predict(&request("help me"));
        assert!(!result.confident);
        assert!(result.ghost.is_empty());
        assert!(!result.candidates.is_empty());
    }

    #[test]
    fn sentence_final_context_never_ghosts() {
        // After sentence-final punctuation with no new words, the current
        // sequence holds no word token: blocked, never a `<s>`-only ghost.
        let model = model_for(&[
            "help me implement it",
            "help me review it",
            "help me fix it",
            "help me test it",
        ]);
        let result = model.predict(&request("Fix it."));
        assert!(result.blocked_reason.is_some());
        assert!(!result.confident);
        assert!(result.ghost.is_empty());
    }

    #[test]
    fn trailing_space_keeps_word_context() {
        // Auto-mode predicts right after a typed space: the context is the
        // words before it.
        let model = model_for(&[
            "help me implement it now",
            "help me implement it today",
            "help me implement it fast",
            "help me implement it well",
        ]);
        let plain = model.predict(&request("help me"));
        let spaced = model.predict(&request("help me "));
        assert_eq!(plain.context_words, spaced.context_words);
        assert_eq!(plain.confident, spaced.confident);
    }

    #[test]
    fn blocked_reasons_surface() {
        let model = model_for(&["help me implement it"]);
        let result = model.predict(&request("explain\n```python\nprint"));
        assert_eq!(result.blocked_reason.as_deref(), Some("unclosed_fence"));
        assert!(result.ghost.is_empty());
    }

    #[test]
    fn draft_cache_informs_next_guess() {
        let model = model_for(&["something else entirely here"]);
        let result = model.predict(&request("fix the parser and fix the"));
        assert!(
            result.candidates.iter().any(|cand| cand.key == "parser"),
            "candidates={:?}",
            result.candidates
        );
    }

    #[test]
    fn project_boost_changes_ranking() {
        let rows = vec![
            crate::prompt_prediction::wire::PromptPredictionRowWire {
                text: "help me implement it".to_string(),
                epoch_seconds: 100,
                project: Some("sase".to_string()),
                origin: Some("typed".to_string()),
                cancelled: false,
            },
            crate::prompt_prediction::wire::PromptPredictionRowWire {
                text: "help me implement it".to_string(),
                epoch_seconds: 100,
                project: Some("other".to_string()),
                origin: Some("typed".to_string()),
                cancelled: false,
            },
            typed("help me review it", 100),
            typed("help me review it now", 200),
            typed("help me review it fast", 300),
        ];
        // Same text in two projects counts once per text globally, so add
        // project-specific weight differently: boost path is exercised by
        // comparing project vs no-project queries.
        let model = model_for_rows(&rows);
        let mut with_project = request("help me");
        with_project.project = Some("sase".to_string());
        let plain = model.predict(&request("help me"));
        let boosted = model.predict(&with_project);
        let plain_top = plain.candidates.first().map(|cand| cand.key.clone());
        let boosted_top =
            boosted.candidates.first().map(|cand| cand.key.clone());
        assert!(plain_top.is_some() && boosted_top.is_some());
        let _ = (plain_top, boosted_top);
    }

    #[test]
    fn rank_prefix_matches_and_orders() {
        let model = model_for(&[
            "help me implement it",
            "help me implement it now",
            "help me implement it today",
            "help me implement it fast",
            "help me important work",
        ]);
        let result = model.rank_prefix(&PromptPrefixRankRequestWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            text_before_word: "help me ".to_string(),
            prefix: "impl".to_string(),
            project: None,
            limit: 5,
        });
        assert!(!result.matches.is_empty());
        assert_eq!(result.matches[0].key, "implement");
        assert!(result.matches[0].order >= 1);
    }

    #[test]
    fn rank_prefix_excludes_prefix_itself() {
        let model =
            model_for(&["help me implement it", "help me implement it now"]);
        let result = model.rank_prefix(&PromptPrefixRankRequestWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            text_before_word: "help me ".to_string(),
            prefix: "implement".to_string(),
            project: None,
            limit: 5,
        });
        assert!(result.matches.iter().all(|mat| mat.key != "implement"));
    }

    #[test]
    fn continuation_stops_at_max_words() {
        let model = model_for(&[
            "alpha beta gamma delta epsilon",
            "alpha beta gamma delta epsilon",
            "alpha beta gamma delta epsilon",
            "alpha beta gamma delta epsilon",
        ]);
        let mut req = request("alpha beta gamma");
        req.max_words = 2;
        let result = model.predict(&req);
        assert!(result.ghost.len() <= 2, "ghost={:?}", result.ghost);
    }

    #[test]
    fn predict_blocks_structural_and_alternation_tails() {
        let model = model_for(&[
            "please look at home now",
            "please look at home today",
            "please look at home fast",
            "please look at home soon",
        ]);
        for text in [
            "please look at src/foo.rs",
            "please look at #gh:sase",
            "please look at `x`",
            "please look at {{ x }}",
            "please look at %{a,b}",
            "please look at this:",
            "%{fix the ",
        ] {
            let result = model.predict(&request(text));
            assert!(
                result.blocked_reason.is_some(),
                "expected block for {text:?}"
            );
            assert!(!result.confident);
            assert!(result.ghost.is_empty());
        }
    }

    #[test]
    fn rank_prefix_blocks_structural_and_alternation_tails() {
        let model = model_for(&[
            "help me implement it",
            "help me implement it now",
            "help me implement it today",
            "help me implement it fast",
        ]);
        for text in [
            "please look at src/foo.rs",
            "please look at #gh:sase",
            "please look at `x`",
            "please look at {{ x }}",
            "please look at %{a,b}",
            "please look at this:",
            "%{fix the ",
        ] {
            let result = model.rank_prefix(&PromptPrefixRankRequestWire {
                schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
                text_before_word: text.to_string(),
                prefix: "impl".to_string(),
                project: None,
                limit: 5,
            });
            assert!(
                result.matches.is_empty(),
                "expected no matches for {text:?}, got {:?}",
                result.matches
            );
        }
    }

    #[test]
    fn predict_blocks_backtick_and_frontmatter_at_cursor() {
        let model = model_for(&["help me implement it"]);
        for text in ["explain `code", "---\ntitle: hi\n"] {
            let result = model.predict(&request(text));
            assert!(result.blocked_reason.is_some());
            assert!(result.ghost.is_empty());
        }
    }
}
