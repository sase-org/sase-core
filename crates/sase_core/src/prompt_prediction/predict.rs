//! Stupid-backoff scoring, confidence gates, and greedy continuation.
//!
//! The scorer composes every model source at each context order: the
//! combined mass is `Σ source weight · mass (+ project_boost ·
//! same-project partition mass)` over the combined total. Each word keeps
//! its best backoff-discounted score, so results are fully deterministic
//! (score descending, key ascending).

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::prompt_prediction::corpus::{
    CompiledPromptPredictionCorpus, FastHashMap,
};
use crate::prompt_prediction::tokenize::SEQUENCE_START;

/// Confidence gate thresholds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConfidencePreset {
    /// Minimum top-1 share at the evidence order.
    pub min_p: f64,
    /// Minimum top-1 minus runner-up share at the evidence order.
    pub min_margin: f64,
    /// Minimum combined distinct support.
    pub min_support: u64,
}

/// Cautious preset: precision first.
pub const PRESET_CAUTIOUS: ConfidencePreset = ConfidencePreset {
    min_p: 0.75,
    min_margin: 0.35,
    min_support: 4,
};
/// Balanced preset: the default.
pub const PRESET_BALANCED: ConfidencePreset = ConfidencePreset {
    min_p: 0.6,
    min_margin: 0.2,
    min_support: 3,
};
/// Eager preset: coverage first.
pub const PRESET_EAGER: ConfidencePreset = ConfidencePreset {
    min_p: 0.45,
    min_margin: 0.1,
    min_support: 2,
};

/// Parse a confidence name; unknown names fall back to balanced.
pub fn parse_confidence(name: &str) -> ConfidencePreset {
    match name {
        "cautious" => PRESET_CAUTIOUS,
        "eager" => PRESET_EAGER,
        _ => PRESET_BALANCED,
    }
}

/// Maximum context tokens consulted per query (mirrors the compile default).
pub const MODEL_MAX_CONTEXT_WORDS: usize = 4;

/// One draft observation bundle: mass sums occurrences, distinct counts
/// each pair once.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct DraftStats {
    mass: f64,
    distinct: u64,
}

/// Per-request draft counts, frozen for continuation.
#[derive(Debug, Clone, Default)]
pub struct DraftCounts {
    pairs: FastHashMap<Vec<String>, FastHashMap<String, DraftStats>>,
    totals: FastHashMap<Vec<String>, (f64, u64)>,
}

impl DraftCounts {
    /// Count the draft's own sequences as an extra source, leaving out the
    /// final (queried) position: pairs whose word is the last token of the
    /// last sequence are skipped.
    pub fn from_sequences(
        sequences: &[Vec<String>],
        max_context: usize,
    ) -> Self {
        let mut counts = Self::default();
        let mut seen: HashSet<(Vec<String>, String)> = HashSet::new();
        for (seq_index, seq) in sequences.iter().enumerate() {
            let is_last_seq = seq_index + 1 == sequences.len();
            for (pos, word) in seq.iter().enumerate() {
                if is_last_seq && pos + 1 == seq.len() {
                    continue;
                }
                let depth = pos.min(max_context);
                for order in 0..=depth {
                    let context: Vec<String> = seq[pos - order..pos].to_vec();
                    let words =
                        counts.pairs.entry(context.clone()).or_default();
                    let entry = words.entry(word.clone()).or_default();
                    entry.mass += 1.0;
                    if seen.insert((context.clone(), word.clone())) {
                        entry.distinct += 1;
                    }
                    let total =
                        counts.totals.entry(context).or_insert((0.0, 0));
                    total.0 += 1.0;
                }
            }
        }
        // Totals count distinct rows conceptually; with one draft, count
        // each context once.
        for total in counts.totals.values_mut() {
            total.1 = 1;
        }
        counts
    }

    /// Every word observed in the draft.
    pub(crate) fn candidate_words(&self) -> Vec<String> {
        let mut words: Vec<String> = self
            .pairs
            .values()
            .flat_map(|words| words.keys().cloned())
            .collect();
        words.sort();
        words.dedup();
        words
    }

    fn pair(&self, context: &[String], word: &str) -> (f64, u64) {
        self.pairs
            .get(context)
            .and_then(|words| words.get(word))
            .map(|stats| (stats.mass, stats.distinct))
            .unwrap_or((0.0, 0))
    }

    fn totals(&self, context: &[String]) -> (f64, u64) {
        self.totals.get(context).copied().unwrap_or((0.0, 0))
    }
}

/// One scored word with its evidence order.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredWord {
    pub key: String,
    pub score: f64,
    pub probability: f64,
    pub support: u64,
    pub order: usize,
    pub shares: [f64; 5],
    /// True when the word has combined mass at some order 1 or higher.
    /// Unigram-only words are never offered as candidates.
    pub has_higher: bool,
}

/// Shared inputs for one scoring pass: sources, weights, and context.
pub struct ScoringQuery<'a> {
    pub sources: &'a [WeightedSource<'a>],
    pub project: Option<&'a str>,
    pub project_boost: f64,
    pub backoff_alpha: f64,
    pub draft: Option<(&'a DraftCounts, f64)>,
    pub context: &'a [String],
    pub max_order: usize,
    pub preset: ConfidencePreset,
    pub reject_conflicts: bool,
}

/// Source shares index: history, project, session, draft, archive.
pub const SHARE_HISTORY: usize = 0;
pub const SHARE_PROJECT: usize = 1;
pub const SHARE_SESSION: usize = 2;
pub const SHARE_DRAFT: usize = 3;
pub const SHARE_ARCHIVE: usize = 4;

/// One weighted corpus source.
pub struct WeightedSource<'a> {
    /// 0 = history, 2 = session, 4 = archive share index.
    pub share: usize,
    pub corpus: &'a CompiledPromptPredictionCorpus,
    pub weight: f64,
}

/// One source with its per-order contexts resolved to word ids once per
/// scoring pass. Candidates then hit direct map lookups with no string
/// hashing and no per-call allocation.
struct ResolvedView<'a> {
    corpus: &'a CompiledPromptPredictionCorpus,
    share: usize,
    weight: f64,
    orders: Vec<Option<Vec<u32>>>,
}

/// Resolved inputs for one scoring pass: id-space source views plus
/// the string-level project, boost, and draft settings.
struct PassCtx<'a> {
    views: Vec<ResolvedView<'a>>,
    project: Option<&'a str>,
    boost: f64,
    draft: Option<(&'a DraftCounts, f64)>,
}

impl<'a> PassCtx<'a> {
    fn of(query: &ScoringQuery<'a>, suffixes: &OrderSuffixes) -> Self {
        Self {
            views: resolve_views(query.sources, suffixes),
            project: query.project,
            boost: query.project_boost,
            draft: query.draft,
        }
    }
}

fn resolve_views<'a>(
    sources: &[WeightedSource<'a>],
    suffixes: &OrderSuffixes,
) -> Vec<ResolvedView<'a>> {
    sources
        .iter()
        .map(|source| {
            let orders = suffixes
                .iter()
                .map(|suffix| {
                    let refs: Vec<&str> =
                        suffix.iter().map(String::as_str).collect();
                    source.corpus.resolve_context(&refs)
                })
                .collect();
            ResolvedView {
                corpus: source.corpus,
                share: source.share,
                weight: source.weight,
                orders,
            }
        })
        .collect()
}

/// Owned order suffixes of a query context, indexed by order.
/// Built once per scoring pass so per-candidate lookups share them.
pub type OrderSuffixes = Vec<Vec<String>>;

fn order_suffixes(context: &[String], max_order: usize) -> OrderSuffixes {
    (0..=max_order)
        .map(|order| context[context.len().saturating_sub(order)..].to_vec())
        .collect()
}

/// Deterministic mass comparison: greater wins, ties break by key
/// ascending (`total_cmp` keeps the tie behavior clippy-clean).
fn outranks(mass: f64, best_mass: f64, word: &str, best_word: &str) -> bool {
    match mass.total_cmp(&best_mass) {
        Ordering::Greater => true,
        Ordering::Equal => word < best_word,
        Ordering::Less => false,
    }
}

/// Combined mass, total, and support of one word at one order, plus
/// per-share mass contributions. `word_ids` holds each view's resolved
/// word id (or `None` when the view never saw the word).
fn combined_at_order(
    ctx: &PassCtx<'_>,
    suffix_owned: &[String],
    order: usize,
    word_ids: &[Option<u32>],
    word: &str,
) -> (f64, f64, u64, [f64; 5]) {
    let mut mass = 0.0;
    let mut total = 0.0;
    let mut support = 0u64;
    let mut shares = [0.0; 5];
    for (view, word_id) in ctx.views.iter().zip(word_ids.iter()) {
        let Some(context) = view.orders.get(order).and_then(|opt| opt.as_ref())
        else {
            continue;
        };
        let (ctx_mass, _) = view.corpus.totals_for(context);
        total += view.weight * ctx_mass;
        if let Some(id) = word_id {
            let (word_mass, word_distinct) =
                view.corpus.word_stats_for(context, *id);
            mass += view.weight * word_mass;
            support += word_distinct;
            shares[view.share] += view.weight * word_mass;
        }
        if let Some(name) = ctx.project {
            let (proj_total, _) = view.corpus.project_totals_for(name, context);
            total += ctx.boost * proj_total;
            if let Some(id) = word_id {
                let (proj_mass, proj_word_distinct) =
                    view.corpus.project_word_stats_for(name, context, *id);
                mass += ctx.boost * proj_mass;
                support += proj_word_distinct;
                shares[SHARE_PROJECT] += ctx.boost * proj_mass;
            }
        }
    }
    // Support counts successor observations; context distinct totals live
    // in the totals helper below.
    if let Some((counts, weight)) = ctx.draft {
        let (draft_mass, draft_distinct) = counts.pair(suffix_owned, word);
        mass += weight * draft_mass;
        let (draft_total, _) = counts.totals(suffix_owned);
        total += weight * draft_total;
        support += draft_distinct;
        shares[SHARE_DRAFT] += weight * draft_mass;
    }
    // Normalize shares against the combined mass.
    if mass > 0.0 {
        for share in &mut shares {
            *share /= mass;
        }
    }
    (mass, total, support, shares)
}

/// Combined totals of one precomputed order suffix: mass, mass (kept as
/// a pair for call-site symmetry), and distinct rows.
fn combined_totals_at_order(
    ctx: &PassCtx<'_>,
    suffix_owned: &[String],
    order: usize,
) -> (f64, f64, u64) {
    let mut total = 0.0;
    let mut distinct = 0u64;
    for view in &ctx.views {
        let Some(context) = view.orders.get(order).and_then(|opt| opt.as_ref())
        else {
            continue;
        };
        let (ctx_mass, ctx_distinct) = view.corpus.totals_for(context);
        total += view.weight * ctx_mass;
        distinct += ctx_distinct;
        if let Some(name) = ctx.project {
            let (proj_total, proj_distinct) =
                view.corpus.project_totals_for(name, context);
            total += ctx.boost * proj_total;
            distinct += proj_distinct;
        }
    }
    if let Some((counts, weight)) = ctx.draft {
        let (draft_total, draft_distinct) = counts.totals(suffix_owned);
        total += weight * draft_total;
        distinct += draft_distinct;
    }
    (total, total, distinct)
}

/// Top two successors at one order by combined mass, key ascending on
/// ties, plus the runner-up share. One pass serves both the leader check
/// and the margin check.
fn top_two_at_order(
    ctx: &PassCtx<'_>,
    suffixes: &OrderSuffixes,
    order: usize,
    candidates: &[ScoredCandidate],
) -> (Option<(String, f64, f64, u64)>, f64) {
    let mut best: Option<(String, f64, f64, u64)> = None;
    for candidate in candidates {
        let (mass, total, support, _) = combined_at_order(
            ctx,
            &suffixes[order],
            order,
            &candidate.word_ids,
            &candidate.key,
        );
        if total <= 0.0 || mass <= 0.0 {
            continue;
        }
        let share = mass / total;
        let replace = match &best {
            None => true,
            Some((best_word, best_mass, _, _)) => {
                outranks(mass, *best_mass, &candidate.key, best_word)
            }
        };
        if replace {
            best = Some((candidate.key.clone(), mass, share, support));
        }
    }
    let runner_up = best
        .as_ref()
        .map(|(top_key, _, _, _)| {
            let mut second: f64 = 0.0;
            for candidate in candidates {
                if candidate.key == *top_key {
                    continue;
                }
                let (mass, total, _, _) = combined_at_order(
                    ctx,
                    &suffixes[order],
                    order,
                    &candidate.word_ids,
                    &candidate.key,
                );
                if total > 0.0 {
                    second = second.max(mass / total);
                }
            }
            second
        })
        .unwrap_or(0.0);
    (best, runner_up)
}

/// One candidate with word ids resolved once per scoring pass.
struct ScoredCandidate {
    key: String,
    word_ids: Vec<Option<u32>>,
}

fn resolve_candidates(
    views: &[ResolvedView<'_>],
    candidates: &[String],
) -> Vec<ScoredCandidate> {
    candidates
        .iter()
        .filter(|key| key.as_str() != SEQUENCE_START)
        .map(|key| {
            let word_ids = views
                .iter()
                .map(|view| view.corpus.resolve_word(key))
                .collect();
            ScoredCandidate {
                key: key.clone(),
                word_ids,
            }
        })
        .collect()
}

/// Score every candidate with stupid backoff over orders `max_order..=0`.
pub fn score_candidates(
    query: &ScoringQuery<'_>,
    candidates: &[String],
) -> Vec<ScoredWord> {
    let max_order = query.max_order;
    let suffixes = order_suffixes(query.context, max_order);
    let ctx = PassCtx::of(query, &suffixes);
    let resolved = resolve_candidates(&ctx.views, candidates);
    let mut scored = Vec::new();
    for candidate in &resolved {
        let mut best: Option<ScoredWord> = None;
        let mut has_higher = false;
        for order in (0..=max_order).rev() {
            let (mass, total, support, shares) = combined_at_order(
                &ctx,
                &suffixes[order],
                order,
                &candidate.word_ids,
                &candidate.key,
            );
            if total <= 0.0 || mass <= 0.0 {
                continue;
            }
            if order >= 1 {
                has_higher = true;
            }
            let probability = mass / total;
            let score = probability
                * query.backoff_alpha.powi((max_order - order) as i32);
            let replace = match &best {
                None => true,
                Some(current) => {
                    outranks(score, current.score, &candidate.key, &current.key)
                }
            };
            if replace {
                best = Some(ScoredWord {
                    key: candidate.key.clone(),
                    score,
                    probability,
                    support,
                    order,
                    shares,
                    has_higher: false,
                });
            }
        }
        if let Some(mut word_score) = best {
            word_score.has_higher = has_higher;
            scored.push(word_score);
        }
    }
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.key.cmp(&b.key))
    });
    scored
}

/// Evidence order: the largest order whose context holds a real word and
/// whose combined distinct total reaches `min_support`.
pub fn evidence_order(
    sources: &[WeightedSource<'_>],
    project: Option<&str>,
    project_boost: f64,
    draft: Option<(&DraftCounts, f64)>,
    suffixes: &OrderSuffixes,
    min_support: u64,
) -> Option<usize> {
    let ctx = PassCtx {
        views: resolve_views(sources, suffixes),
        project,
        boost: project_boost,
        draft,
    };
    evidence_order_views(&ctx, suffixes, min_support)
}

fn evidence_order_views(
    ctx: &PassCtx<'_>,
    suffixes: &OrderSuffixes,
    min_support: u64,
) -> Option<usize> {
    for order in (1..suffixes.len()).rev() {
        let suffix = &suffixes[order];
        if !suffix.iter().any(|token| token != SEQUENCE_START) {
            continue;
        }
        let (_, _, distinct) = combined_totals_at_order(ctx, suffix, order);
        if distinct >= min_support {
            return Some(order);
        }
    }
    None
}

/// Gate outcome for one context: passing top-1 key plus its evidence.
pub struct GatePass {
    pub word: String,
    pub order: usize,
    pub probability: f64,
    pub support: u64,
}

/// Apply the confidence gate. `ranked` must be backoff-sorted with the
/// best first.
pub fn apply_gate(
    query: &ScoringQuery<'_>,
    ranked: &[ScoredWord],
    candidates: &[String],
) -> Option<GatePass> {
    let top = ranked.first()?;
    let suffixes = order_suffixes(query.context, query.max_order);
    let ctx = PassCtx::of(query, &suffixes);
    let resolved = resolve_candidates(&ctx.views, candidates);
    let evidence =
        evidence_order_views(&ctx, &suffixes, query.preset.min_support)?;
    let top_ids = resolved
        .iter()
        .find(|candidate| candidate.key == top.key)
        .map(|candidate| candidate.word_ids.clone())
        .unwrap_or_default();
    let (top_mass, top_total, top_support, _) = combined_at_order(
        &ctx,
        &suffixes[evidence],
        evidence,
        &top_ids,
        &top.key,
    );
    if top_total <= 0.0 {
        return None;
    }
    let (leader, runner_up) =
        top_two_at_order(&ctx, &suffixes, evidence, &resolved);
    let leader = leader?;
    if leader.0 != top.key {
        return None;
    }
    let probability = top_mass / top_total;
    if probability < query.preset.min_p {
        return None;
    }
    if top_support < query.preset.min_support {
        return None;
    }
    if probability - runner_up < query.preset.min_margin {
        return None;
    }
    if query.reject_conflicts {
        for higher in evidence + 1..suffixes.len() {
            let (total, _, _) =
                combined_totals_at_order(&ctx, &suffixes[higher], higher);
            if total <= 0.0 {
                continue;
            }
            let (higher_top, _) =
                top_two_at_order(&ctx, &suffixes, higher, &resolved);
            if higher_top.is_some_and(|(word, _, _, _)| word != top.key) {
                return None;
            }
        }
    }
    Some(GatePass {
        word: top.key.clone(),
        order: evidence,
        probability,
        support: top_support,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_prediction::corpus::{
        compile_prompt_prediction_corpus, CompiledPromptPredictionCorpus,
    };
    use crate::prompt_prediction::wire::PromptPredictionRowWire;

    fn typed(text: &str, epoch: i64) -> PromptPredictionRowWire {
        PromptPredictionRowWire {
            text: text.to_string(),
            epoch_seconds: epoch,
            project: None,
            origin: Some("typed".to_string()),
            cancelled: false,
        }
    }

    fn corpus_for(texts: &[&str]) -> CompiledPromptPredictionCorpus {
        let rows: Vec<PromptPredictionRowWire> = texts
            .iter()
            .enumerate()
            .map(|(i, text)| typed(text, 100 + i as i64))
            .collect();
        compile_prompt_prediction_corpus(
            &rows,
            &crate::prompt_prediction::wire::PromptPredictionCorpusOptionsWire {
                now_epoch: 1_000,
                ..Default::default()
            },
        )
    }

    fn sources_of(
        corpus: &CompiledPromptPredictionCorpus,
    ) -> Vec<WeightedSource<'_>> {
        vec![WeightedSource {
            share: SHARE_HISTORY,
            corpus,
            weight: 1.0,
        }]
    }

    #[test]
    fn confidence_names_parse() {
        assert_eq!(parse_confidence("cautious"), PRESET_CAUTIOUS);
        assert_eq!(parse_confidence("balanced"), PRESET_BALANCED);
        assert_eq!(parse_confidence("eager"), PRESET_EAGER);
        assert_eq!(parse_confidence("nope"), PRESET_BALANCED);
    }

    #[test]
    fn backoff_prefers_longer_context() {
        let corpus = corpus_for(&[
            "help me implement it",
            "help me implement it now",
            "help me review it",
            "help me implement tests",
            "help me implement code",
        ]);
        let sources = sources_of(&corpus);
        let context: Vec<String> = vec!["help".into(), "me".into()];
        let candidates: Vec<String> = vec!["implement".into(), "review".into()];
        let query = ScoringQuery {
            sources: &sources,
            project: None,
            project_boost: 1.0,
            backoff_alpha: 0.4,
            draft: None,
            context: &context,
            max_order: 2,
            preset: PRESET_BALANCED,
            reject_conflicts: true,
        };
        let ranked = score_candidates(&query, &candidates);
        assert_eq!(ranked[0].key, "implement");
        assert!(ranked[0].order >= 1);
    }

    #[test]
    fn deterministic_ties_break_by_key() {
        // Equal epochs: identical mass, so the key breaks the tie.
        let rows =
            vec![typed("help me alpha", 100), typed("help me beta", 100)];
        let corpus = compile_prompt_prediction_corpus(
            &rows,
            &crate::prompt_prediction::wire::PromptPredictionCorpusOptionsWire {
                now_epoch: 1_000,
                ..Default::default()
            },
        );
        let sources = sources_of(&corpus);
        let context: Vec<String> = vec!["help".into(), "me".into()];
        let candidates: Vec<String> = vec!["beta".into(), "alpha".into()];
        let query = ScoringQuery {
            sources: &sources,
            project: None,
            project_boost: 1.0,
            backoff_alpha: 0.4,
            draft: None,
            context: &context,
            max_order: 2,
            preset: PRESET_BALANCED,
            reject_conflicts: true,
        };
        let ranked = score_candidates(&query, &candidates);
        assert_eq!(ranked[0].key, "alpha");
        assert_eq!(ranked[1].key, "beta");
    }

    #[test]
    fn evidence_order_needs_a_real_word() {
        let corpus = corpus_for(&["help me implement it"]);
        let sources = sources_of(&corpus);
        let context: Vec<String> = vec![SEQUENCE_START.into()];
        let suffixes = order_suffixes(&context, 1);
        assert_eq!(
            evidence_order(&sources, None, 1.0, None, &suffixes, 1),
            None
        );
    }

    #[test]
    fn draft_counts_skip_final_position() {
        let seqs = vec![vec!["fix".to_string(), "the".to_string()]];
        let counts = DraftCounts::from_sequences(&seqs, 4);
        // Pair (* -> fix) counts; pair (fix -> the) is the final position.
        assert!(counts.pair(&[], "fix").0 > 0.0);
        assert_eq!(counts.pair(&["fix".to_string()], "the"), (0.0, 0));
    }
}
