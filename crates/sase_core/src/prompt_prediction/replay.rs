//! Prequential replay evaluator for prompt prediction.
//!
//! The evaluator replays typed history in epoch order: it warms a mutable
//! builder on the oldest share of rows, then scores every word-boundary
//! position of each later row before adding that row. Scoring mirrors
//! [`crate::prompt_prediction::model::PromptPredictionModel::predict`]
//! (single history source, project boost, per-request draft, stupid
//! backoff, confidence gate) but queries the live builder through
//! [`PromptSuccessorSource`](crate::prompt_prediction::corpus::PromptSuccessorSource),
//! so one pass never recompiles. Per-position evidence is recorded once;
//! preset and grid thresholds sweep over those records. The report holds
//! aggregates only, never prompt text.

use std::collections::HashSet;
use std::time::Instant;

use crate::prompt_prediction::corpus::{
    is_generated_row, FastHashSet, PromptPredictionBuilder,
    PromptSuccessorSource,
};
use crate::prompt_prediction::predict::{
    ConfidencePreset, DraftCounts, PRESET_BALANCED, PRESET_CAUTIOUS,
    PRESET_EAGER,
};
use crate::prompt_prediction::tokenize::{
    tokenize_prompt_text, SEQUENCE_START,
};
use crate::prompt_prediction::wire::{
    PromptPredictionCorpusOptionsWire, PromptPredictionReplayCohortWire,
    PromptPredictionReplayGateMetricsWire,
    PromptPredictionReplayMidwordCohortWire,
    PromptPredictionReplayMidwordMetricsWire,
    PromptPredictionReplayMidwordPresetWire, PromptPredictionReplayOptionsWire,
    PromptPredictionReplayReportWire, PromptPredictionReplaySweepPointWire,
    PromptPredictionRowWire, PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
};

/// Cohort index for rows with under 30% 5-gram overlap with prior text.
const COHORT_NOVEL: u8 = 0;
/// Cohort index for rows between the novel and near-duplicate cutoffs.
const COHORT_MID: u8 = 1;
/// Cohort index for rows with 70% or more 5-gram overlap with prior text.
const COHORT_NEAR: u8 = 2;

/// Cohort names in index order, as they appear in the report.
const COHORT_NAMES: [&str; 3] = ["novel", "mid", "near-duplicate"];

/// Resolved replay tuning: model weights plus corpus-shape knobs.
struct ReplayTuning {
    max_context: usize,
    backoff_alpha: f64,
    project_boost: f64,
    draft_weight: f64,
    reject_conflicts: bool,
    prune_singletons: bool,
    excluded: FastHashSet<String>,
}

/// Per-order gate evidence for the backoff top-1 word.
#[derive(Debug, Clone, Default)]
struct OrderEvidence {
    /// The order context holds at least one real word (`<s>` alone excluded).
    real: bool,
    /// Combined distinct total at this order.
    distinct: u64,
    /// Combined mass, total, and support of the backoff top-1 at this order.
    top_mass: f64,
    top_total: f64,
    top_support: u64,
    /// The backoff top-1 also leads this order by combined mass.
    leader: bool,
    /// Runner-up share at this order.
    runner_up: f64,
    /// A higher order with observations ranks a different word first.
    conflict_above: bool,
}

/// One scored word-boundary position.
struct PositionRecord {
    row: u32,
    cohort: u8,
    scored: bool,
    top1: bool,
    top3: bool,
    target_chars: u32,
    latency_us: u64,
    orders: Vec<OrderEvidence>,
}

/// One backoff-scored candidate: key, best backoff score, and whether it
/// holds mass at some order 1 or higher (unigram-only words are scored
/// for the gate's leader check but never offered as menu rows).
struct ReplayScored {
    key: String,
    score: f64,
    has_higher: bool,
}

/// Evaluate a prequential replay over `rows` with `options`.
///
/// Typed rows are deduped by exact text (newest epoch wins, matching corpus
/// compile), sorted oldest first, warmed on the oldest `warm_fraction`, and
/// scored position by position before each row joins the builder. Generated
/// rows are skipped throughout.
pub fn evaluate_prompt_prediction_replay(
    rows: &[PromptPredictionRowWire],
    options: &PromptPredictionReplayOptionsWire,
) -> PromptPredictionReplayReportWire {
    let tuning = ReplayTuning {
        max_context: options.max_context_words.max(1),
        backoff_alpha: options.backoff_alpha,
        project_boost: options.project_boost,
        draft_weight: options.draft_weight,
        reject_conflicts: options.reject_conflicts,
        prune_singletons: options.prune_singleton_contexts,
        excluded: options
            .excluded_words
            .iter()
            .map(|word| word.to_lowercase().replace('’', "'"))
            .collect(),
    };
    let corpus_options = PromptPredictionCorpusOptionsWire {
        schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
        now_epoch: options.now_epoch,
        recency_half_life_days: options.recency_half_life_days,
        max_context_words: options.max_context_words,
        max_successors_per_context: options.max_successors_per_context,
        prune_singleton_contexts: options.prune_singleton_contexts,
        excluded_words: options.excluded_words.clone(),
    };
    let mut typed = dedup_typed_rows(rows);
    typed.sort_by(|a, b| {
        a.epoch_seconds
            .cmp(&b.epoch_seconds)
            .then_with(|| a.text.cmp(&b.text))
    });
    let mut builder = PromptPredictionBuilder::new(corpus_options);
    let warm_n = ((typed.len() as f64) * options.warm_fraction.clamp(0.0, 1.0))
        .floor() as usize;
    let warm_n = warm_n.min(typed.len());
    let mut prior_five: HashSet<u64> = HashSet::new();
    let mut prior_vocab: HashSet<String> = HashSet::new();
    for row in typed.iter().take(warm_n) {
        builder.add_row(row);
        observe_row_text(&row.text, &mut prior_five, &mut prior_vocab);
    }
    let mut records: Vec<PositionRecord> = Vec::new();
    let mut midword: Vec<MidwordTrial> = Vec::new();
    let mut cohort_chars = [0u64; 3];
    let mut rows_scored = 0u64;
    // Deterministic scoring stride: every K-th post-warm row is scored,
    // but every row still joins the corpus and the cohort history, so a
    // sampled run sees the same evidence as the full run.
    let score_every = options.score_every.max(1);
    for (row_id, row) in typed.iter().enumerate().skip(warm_n) {
        let row_index = row_id as u32;
        let sequences = tokenize_prompt_text(&row.text);
        let cohort = cohort_for(&sequences, &prior_five, &prior_vocab);
        let mut scored_any = false;
        let sampled = ((row_id - warm_n) as u64).is_multiple_of(score_every);
        for (seq_index, sequence) in sequences.iter().enumerate() {
            if !sampled {
                break;
            }
            let keys: Vec<String> = sequence
                .tokens
                .iter()
                .map(|token| token.key.clone())
                .collect();
            for (pos, target) in keys.iter().enumerate() {
                let context = replay_context(
                    sequence.started,
                    &keys[..pos],
                    tuning.max_context,
                );
                let prefix: Vec<Vec<String>> = sequences[..seq_index]
                    .iter()
                    .map(|seq| {
                        seq.tokens
                            .iter()
                            .map(|token| token.key.clone())
                            .collect()
                    })
                    .chain(std::iter::once(keys[..pos].to_vec()))
                    .collect();
                let started = Instant::now();
                let record = score_position(
                    &builder,
                    &tuning,
                    &context,
                    &prefix,
                    row.project.as_deref(),
                    target,
                    row_index,
                    cohort,
                    started,
                );
                let boundary_index = records.len();
                records.push(record);
                // Mid-word trials: each typed-prefix length of the target
                // word is a completion trial through the restricted gate,
                // with the boundary just scored as its continuation base.
                score_midword_trials(
                    &builder,
                    &tuning,
                    &context,
                    &prefix,
                    row.project.as_deref(),
                    target,
                    &keys,
                    pos,
                    row_index,
                    cohort,
                    boundary_index,
                    &mut midword,
                );
                scored_any = true;
            }
        }
        if scored_any {
            rows_scored += 1;
            cohort_chars[cohort as usize] += row.text.chars().count() as u64;
        }
        builder.add_row(row);
        observe_row_text(&row.text, &mut prior_five, &mut prior_vocab);
    }
    assemble_report(
        &tuning,
        &builder,
        &records,
        &midword,
        &cohort_chars,
        rows_total(rows.len()),
        typed.len() as u64,
        warm_n as u64,
        rows_scored,
    )
}

fn rows_total(len: usize) -> u64 {
    len as u64
}

/// Keep typed rows only, deduped by exact text with the newest epoch
/// winning (ties keep the first row, matching corpus compile).
fn dedup_typed_rows(
    rows: &[PromptPredictionRowWire],
) -> Vec<PromptPredictionRowWire> {
    let mut newest: std::collections::HashMap<&str, &PromptPredictionRowWire> =
        std::collections::HashMap::new();
    for row in rows {
        if is_generated_row(row) {
            continue;
        }
        match newest.get(row.text.as_str()) {
            Some(existing) if existing.epoch_seconds >= row.epoch_seconds => {}
            _ => {
                newest.insert(row.text.as_str(), row);
            }
        }
    }
    let mut kept: Vec<PromptPredictionRowWire> =
        newest.values().map(|row| (*row).clone()).collect();
    kept.sort_by(|a, b| {
        a.epoch_seconds
            .cmp(&b.epoch_seconds)
            .then_with(|| a.text.cmp(&b.text))
    });
    kept
}

/// Tokenized key sequences of one row, for cohort overlap.
fn row_key_sequences(text: &str) -> Vec<Vec<String>> {
    tokenize_prompt_text(text)
        .iter()
        .map(|seq| seq.tokens.iter().map(|token| token.key.clone()).collect())
        .collect()
}

/// FNV-1a hash of one n-gram, for the prior-text overlap sets.
fn gram_hash(words: &[String]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for word in words {
        for byte in word.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0x1f;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Add one row's 5-grams and vocabulary to the prior-text sets.
fn observe_row_text(
    text: &str,
    prior_five: &mut HashSet<u64>,
    prior_vocab: &mut HashSet<String>,
) {
    for keys in row_key_sequences(text) {
        for key in &keys {
            prior_vocab.insert(key.clone());
        }
        if keys.len() >= 5 {
            for window in keys.windows(5) {
                prior_five.insert(gram_hash(window));
            }
        }
    }
}

/// Cohort of one row: share of its 5-grams already seen in prior text
/// (unigram share when the row holds fewer than 5 words in a sequence).
fn cohort_for(
    sequences: &[crate::prompt_prediction::tokenize::ProseSequence],
    prior_five: &HashSet<u64>,
    prior_vocab: &HashSet<String>,
) -> u8 {
    let mut hits = 0u64;
    let mut total = 0u64;
    let mut uni_hits = 0u64;
    let mut uni_total = 0u64;
    for sequence in sequences {
        let keys: Vec<String> = sequence
            .tokens
            .iter()
            .map(|token| token.key.clone())
            .collect();
        for key in &keys {
            uni_total += 1;
            if prior_vocab.contains(key) {
                uni_hits += 1;
            }
        }
        if keys.len() >= 5 {
            for window in keys.windows(5) {
                total += 1;
                if prior_five.contains(&gram_hash(window)) {
                    hits += 1;
                }
            }
        }
    }
    let overlap = if total > 0 {
        hits as f64 / total as f64
    } else if uni_total > 0 {
        uni_hits as f64 / uni_total as f64
    } else {
        return COHORT_NOVEL;
    };
    if overlap < 0.3 {
        COHORT_NOVEL
    } else if overlap >= 0.7 {
        COHORT_NEAR
    } else {
        COHORT_MID
    }
}

/// Trimmed query context: `[<s>]`-prefixed word keys before the target,
///
/// capped at `max_context` tokens.
fn replay_context(
    started: bool,
    before: &[String],
    max_context: usize,
) -> Vec<String> {
    let mut full: Vec<String> = Vec::with_capacity(before.len() + 1);
    if started {
        full.push(SEQUENCE_START.to_string());
    }
    full.extend(before.iter().cloned());
    let start = full.len().saturating_sub(max_context);
    full[start..].to_vec()
}

/// Score one word-boundary position against the live builder.
///
/// Mirrors `PromptPredictionModel` scoring: stupid backoff over the composed
/// history source (weight 1.0, project boost, per-request draft), with the
/// same deterministic tie-breaks (score descending, key ascending).
#[allow(clippy::too_many_arguments)]
fn score_position(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    context: &[String],
    prefix_seqs: &[Vec<String>],
    project: Option<&str>,
    target: &str,
    row: u32,
    cohort: u8,
    started: Instant,
) -> PositionRecord {
    let draft = if tuning.draft_weight > 0.0 {
        Some(DraftCounts::from_sequences(prefix_seqs, tuning.max_context))
    } else {
        None
    };
    let draft_param =
        draft.as_ref().map(|counts| (counts, tuning.draft_weight));
    let max_order = context.len();
    let mut candidates: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for order in 0..=max_order {
        let suffix = &context[context.len().saturating_sub(order)..];
        let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
        for (key, _, _) in builder.ranked_successors(&refs) {
            if seen.insert(key.clone()) {
                candidates.push(key);
            }
        }
    }
    if let Some(counts) = &draft {
        for key in counts.candidate_words() {
            if seen.insert(key.clone()) {
                candidates.push(key);
            }
        }
    }
    candidates.retain(|key| {
        key.as_str() != SEQUENCE_START && !tuning.excluded.contains(key)
    });
    let ranked = score_candidates(
        builder,
        tuning,
        draft_param,
        project,
        context,
        max_order,
        &candidates,
    );
    let menu: Vec<&ReplayScored> =
        ranked.iter().filter(|word| word.has_higher).collect();
    let top1 = menu.first().is_some_and(|word| word.key.as_str() == target);
    let top3 = menu.iter().take(3).any(|word| word.key.as_str() == target);
    let mut orders = vec![OrderEvidence::default(); max_order + 1];
    if let Some(top) = ranked.first() {
        // Leaders per order serve both the leader check and the
        // conflict scan, mirroring `apply_gate`.
        let mut leaders: Vec<Option<String>> = vec![None; max_order + 1];
        let mut runner_ups = vec![0.0f64; max_order + 1];
        for order in 0..=max_order {
            let (leader, runner) = top_two_at(
                builder,
                tuning,
                draft_param,
                project,
                context,
                order,
                &candidates,
            );
            leaders[order] = leader.map(|(key, _, _, _)| key);
            runner_ups[order] = runner;
        }
        for order in 1..=max_order {
            let suffix = &context[context.len().saturating_sub(order)..];
            let real =
                suffix.iter().any(|token| token.as_str() != SEQUENCE_START);
            let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
            let (mass, total, support) = combined_at(
                builder,
                tuning,
                draft_param,
                project,
                suffix,
                &refs,
                &top.key,
            );
            let (_, _, distinct) =
                totals_at(builder, tuning, draft_param, project, suffix, &refs);
            let mut conflict_above = false;
            for higher in order + 1..=max_order {
                let higher_suffix =
                    &context[context.len().saturating_sub(higher)..];
                let higher_refs: Vec<&str> =
                    higher_suffix.iter().map(String::as_str).collect();
                let (_, higher_total, _) = totals_at(
                    builder,
                    tuning,
                    draft_param,
                    project,
                    higher_suffix,
                    &higher_refs,
                );
                if higher_total <= 0.0 {
                    continue;
                }
                if leaders[higher].as_deref() != Some(top.key.as_str()) {
                    conflict_above = true;
                    break;
                }
            }
            orders[order] = OrderEvidence {
                real,
                distinct,
                top_mass: mass,
                top_total: total,
                top_support: support,
                leader: leaders[order].as_deref() == Some(top.key.as_str()),
                runner_up: runner_ups[order],
                conflict_above,
            };
        }
    }
    PositionRecord {
        row,
        cohort,
        scored: !ranked.is_empty(),
        top1,
        top3,
        target_chars: target.chars().count() as u32,
        latency_us: started.elapsed().as_micros().min(u128::from(u64::MAX))
            as u64,
        orders,
    }
}

/// Typed-prefix lengths evaluated by the mid-word pass.
const MIDWORD_KS: [u8; 4] = [1, 2, 3, 4];
/// Cap on counted continuation words: production counts the completed word
/// in `max_words` (default 4), so at most three words follow it.
const MIDWORD_MAX_CONTINUATION: usize = 3;

/// One mid-word completion trial: the restricted-distribution evidence for
/// one typed-prefix length of one word-boundary target, plus the hooks the
/// report needs to score gated continuation.
///
/// `record` holds the restricted evidence with `top1` set when the
/// restricted top-1 equals the target (the completed word is correct), so
/// the shared [`gate_passes`] applies unchanged; the `min_prefix_chars`
/// cutoff is applied at report time, not here, so the trials stay valid
/// when the preset constants move. `boundary_index` is the trial target's
/// own word-boundary record in the boundary stream, and `follows` counts
/// the same-sequence words after it (capped at
/// [`MIDWORD_MAX_CONTINUATION`]) available as continuation.
struct MidwordTrial {
    record: PositionRecord,
    k: u8,
    boundary_index: usize,
    follows: u8,
}

/// Score one mid-word trial per typed-prefix length of `target`.
///
/// For each `k` in 1..=4 with the target longer than `k` characters, the
/// first `k` key characters form the typed prefix and the restricted gate
/// runs over the prefix-matching candidates, mirroring production
/// `score_and_gate_restricted`: per-word masses stay unrestricted while
/// each order total becomes the restricted kept mass plus the dropped
/// tail. The draft counts the sequences with the partial word removed,
/// exactly like the boundary trial's draft. Excluded words never
/// complete: they are filtered from the candidates, so an excluded target
/// can never be top-1. Candidates come from the same truncated top-N union
/// as the boundary path (production scans every key with the prefix), so
/// a matching key outside every order's kept successors is missed here.
#[allow(clippy::too_many_arguments)]
fn score_midword_trials(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    context: &[String],
    prefix_seqs: &[Vec<String>],
    project: Option<&str>,
    target: &str,
    keys: &[String],
    pos: usize,
    row: u32,
    cohort: u8,
    boundary_index: usize,
    out: &mut Vec<MidwordTrial>,
) {
    let target_chars = target.chars().count();
    let draft = if tuning.draft_weight > 0.0 {
        Some(DraftCounts::from_sequences(prefix_seqs, tuning.max_context))
    } else {
        None
    };
    let draft_param =
        draft.as_ref().map(|counts| (counts, tuning.draft_weight));
    let max_order = context.len();
    let follows = keys
        .len()
        .saturating_sub(pos + 1)
        .min(MIDWORD_MAX_CONTINUATION) as u8;
    for k in MIDWORD_KS {
        if target_chars <= usize::from(k) {
            continue;
        }
        let typed: String = target.chars().take(usize::from(k)).collect();
        let mut candidates: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for order in 0..=max_order {
            let suffix = &context[context.len().saturating_sub(order)..];
            let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
            for (key, _, _) in builder.ranked_successors(&refs) {
                if key.starts_with(typed.as_str()) && seen.insert(key.clone()) {
                    candidates.push(key);
                }
            }
        }
        if let Some(counts) = &draft {
            for key in counts.candidate_words() {
                if key.starts_with(typed.as_str()) && seen.insert(key.clone()) {
                    candidates.push(key);
                }
            }
        }
        candidates.retain(|key| {
            key.as_str() != SEQUENCE_START && !tuning.excluded.contains(key)
        });
        // Restricted denominators, once per order and shared across
        // candidates: each totals call scans every successor of its
        // context.
        let order_totals: Vec<(f64, u64)> = (0..=max_order)
            .map(|order| {
                let suffix = &context[context.len().saturating_sub(order)..];
                let refs: Vec<&str> =
                    suffix.iter().map(String::as_str).collect();
                restricted_order_totals(
                    builder,
                    tuning,
                    draft_param,
                    project,
                    suffix,
                    &refs,
                    typed.as_str(),
                )
            })
            .collect();
        let ranked = restricted_score_candidates(
            builder,
            tuning,
            draft_param,
            project,
            context,
            max_order,
            &candidates,
            &order_totals,
        );
        let top1 = ranked
            .first()
            .is_some_and(|word| word.key.as_str() == target);
        let mut orders = vec![OrderEvidence::default(); max_order + 1];
        if let Some(top) = ranked.first() {
            let mut leaders: Vec<Option<String>> = vec![None; max_order + 1];
            let mut runner_ups = vec![0.0f64; max_order + 1];
            for order in 0..=max_order {
                let (leader, runner) = restricted_top_two(
                    builder,
                    tuning,
                    draft_param,
                    project,
                    context,
                    order,
                    &candidates,
                    &order_totals,
                );
                leaders[order] = leader.map(|(key, _, _, _)| key);
                runner_ups[order] = runner;
            }
            for order in 1..=max_order {
                let suffix = &context[context.len().saturating_sub(order)..];
                let real =
                    suffix.iter().any(|token| token.as_str() != SEQUENCE_START);
                let refs: Vec<&str> =
                    suffix.iter().map(String::as_str).collect();
                let (mass, total, support) = restricted_combined_at(
                    builder,
                    tuning,
                    draft_param,
                    project,
                    suffix,
                    &refs,
                    order_totals[order].0,
                    &top.key,
                );
                let distinct = order_totals[order].1;
                let mut conflict_above = false;
                for higher in order + 1..=max_order {
                    if order_totals[higher].0 <= 0.0 {
                        continue;
                    }
                    if leaders[higher].as_deref() != Some(top.key.as_str()) {
                        conflict_above = true;
                        break;
                    }
                }
                orders[order] = OrderEvidence {
                    real,
                    distinct,
                    top_mass: mass,
                    top_total: total,
                    top_support: support,
                    leader: leaders[order].as_deref() == Some(top.key.as_str()),
                    runner_up: runner_ups[order],
                    conflict_above,
                };
            }
        }
        out.push(MidwordTrial {
            record: PositionRecord {
                row,
                cohort,
                scored: !ranked.is_empty(),
                top1,
                top3: false,
                target_chars: target_chars as u32,
                latency_us: 0,
                orders,
            },
            k,
            boundary_index,
            follows,
        });
    }
}

/// Combined restricted total and distinct of one order suffix: history
/// restricted mass plus dropped tail, the project-boosted partition, and
/// the draft restricted sums (no truncation, so no draft dropped mass).
/// Computed once per order and shared across candidates.
fn restricted_order_totals(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    suffix: &[String],
    refs: &[&str],
    prefix: &str,
) -> (f64, u64) {
    let (restricted_mass, restricted_distinct, dropped) =
        builder.prefix_restricted_sums_by_str(refs, prefix);
    let mut total = restricted_mass + dropped;
    let mut distinct = restricted_distinct;
    if let Some(name) = project {
        let (proj_mass, proj_dropped) =
            builder.project_prefix_restricted_sums_by_str(name, refs, prefix);
        total += tuning.project_boost * (proj_mass + proj_dropped);
    }
    if let Some((counts, weight)) = draft {
        if let Some(key) = counts.resolve_key(suffix) {
            let (draft_mass, draft_distinct) =
                counts.prefix_restricted_sums(key, prefix);
            total += weight * draft_mass;
            distinct += draft_distinct;
        }
    }
    if tuning.prune_singletons && refs.len() >= 2 && distinct <= 1 {
        return (0.0, 0);
    }
    (total, distinct)
}

/// Combined mass, total, and support of one word at one order under the
/// restricted gate: the per-word mass stays unrestricted (history plus
/// project-boosted partition plus draft, exactly like the boundary path)
/// while the total is the precomputed prefix-restricted denominator.
/// This mirrors production `combined_at_order` after `restrict_to_prefix`,
/// where word lookups are untouched and only the per-order totals shrink.
/// Totals come from [`restricted_order_totals`], computed once per order
/// and shared across candidates: the totals scan every successor of the
/// context, so per-candidate recomputation would blow the replay up.
#[allow(clippy::too_many_arguments)]
fn restricted_combined_at(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    suffix: &[String],
    refs: &[&str],
    total: f64,
    word: &str,
) -> (f64, f64, u64) {
    if total <= 0.0 {
        return (0.0, 0.0, 0);
    }
    let (mut mass, mut support) = builder.successor_stats(refs, word);
    if let Some(name) = project {
        let (proj_mass, _) = builder.project_successor_stats(name, refs, word);
        mass += tuning.project_boost * proj_mass;
    }
    if let Some((counts, weight)) = draft {
        let (draft_mass, draft_distinct) = counts.pair_by_str(suffix, word);
        mass += weight * draft_mass;
        support += draft_distinct;
    }
    (mass, total, support)
}

/// Score prefix-matching candidates with stupid backoff over the
/// restricted distribution. Mirrors [`score_candidates`]: same
/// order-discounted shares, same deterministic tie-breaks, with
/// [`restricted_combined_at`] as the mass source.
#[allow(clippy::too_many_arguments)]
fn restricted_score_candidates(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    context: &[String],
    max_order: usize,
    candidates: &[String],
    totals: &[(f64, u64)],
) -> Vec<ReplayScored> {
    let mut scored = Vec::new();
    for key in candidates {
        let mut best: Option<ReplayScored> = None;
        let mut has_higher = false;
        for order in (0..=max_order).rev() {
            let suffix = &context[context.len().saturating_sub(order)..];
            let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
            let (mass, total, _) = restricted_combined_at(
                builder,
                tuning,
                draft,
                project,
                suffix,
                &refs,
                totals[order].0,
                key,
            );
            if total <= 0.0 || mass <= 0.0 {
                continue;
            }
            if order >= 1 {
                has_higher = true;
            }
            let probability = mass / total;
            let score = probability
                * tuning.backoff_alpha.powi((max_order - order) as i32);
            let replace = match &best {
                None => true,
                Some(current) => {
                    outranks(score, current.score, key, &current.key)
                }
            };
            if replace {
                best = Some(ReplayScored {
                    key: key.clone(),
                    score,
                    has_higher: false,
                });
            }
        }
        if let Some(mut word) = best {
            word.has_higher = has_higher;
            scored.push(word);
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

/// Top prefix-matching successor at one order by restricted combined
/// mass (key ascending on ties), plus the restricted runner-up share.
/// Mirrors [`top_two_at`] over the restricted distribution.
#[allow(clippy::too_many_arguments)]
fn restricted_top_two(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    context: &[String],
    order: usize,
    candidates: &[String],
    totals: &[(f64, u64)],
) -> (Option<(String, f64, f64, u64)>, f64) {
    let suffix = &context[context.len().saturating_sub(order)..];
    let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
    let mut best: Option<(String, f64, f64, u64)> = None;
    for key in candidates {
        if key.as_str() == SEQUENCE_START {
            continue;
        }
        let (mass, total, support) = restricted_combined_at(
            builder,
            tuning,
            draft,
            project,
            suffix,
            &refs,
            totals[order].0,
            key,
        );
        if total <= 0.0 || mass <= 0.0 {
            continue;
        }
        let share = mass / total;
        let replace = match &best {
            None => true,
            Some((best_word, best_mass, _, _)) => {
                outranks(mass, *best_mass, key, best_word)
            }
        };
        if replace {
            best = Some((key.clone(), mass, share, support));
        }
    }
    let runner_up = best
        .as_ref()
        .map(|(top_key, _, _, _)| {
            let mut second: f64 = 0.0;
            for key in candidates {
                if key == top_key {
                    continue;
                }
                let (mass, total, _) = restricted_combined_at(
                    builder,
                    tuning,
                    draft,
                    project,
                    suffix,
                    &refs,
                    totals[order].0,
                    key,
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

/// Combined mass, total, and support of one word at one order: history
/// mass plus project-boosted partition mass plus draft mass.
fn combined_at(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    suffix: &[String],
    refs: &[&str],
    word: &str,
) -> (f64, f64, u64) {
    let (mut mass, mut total, mut support) = (0.0, 0.0, 0u64);
    let (word_mass, word_distinct) = builder.successor_stats(refs, word);
    let (ctx_mass, _) = builder.context_totals(refs);
    mass += word_mass;
    total += ctx_mass;
    support += word_distinct;
    if let Some(name) = project {
        // Project partition is a subset of the same rows: mass only, never
        // distinct, mirroring production `combined_at_order`.
        let (proj_mass, _) = builder.project_successor_stats(name, refs, word);
        let (proj_total, _) = builder.project_context_totals(name, refs);
        mass += tuning.project_boost * proj_mass;
        total += tuning.project_boost * proj_total;
    }
    if let Some((counts, weight)) = draft {
        let (draft_mass, draft_distinct) = counts.pair_by_str(suffix, word);
        let (draft_total, _) = counts.totals_by_str(suffix);
        mass += weight * draft_mass;
        total += weight * draft_total;
        support += draft_distinct;
    }
    if tuning.prune_singletons && refs.len() >= 2 {
        let (_, _, distinct) =
            totals_at(builder, tuning, draft, project, suffix, refs);
        if distinct <= 1 {
            return (0.0, 0.0, 0);
        }
    }
    (mass, total, support)
}

/// Combined totals (mass, mass, distinct) of one order suffix.
fn totals_at(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    suffix: &[String],
    refs: &[&str],
) -> (f64, f64, u64) {
    let (ctx_mass, ctx_distinct) = builder.context_totals(refs);
    let mut total = ctx_mass;
    let mut distinct = ctx_distinct;
    if let Some(name) = project {
        let (proj_total, _) = builder.project_context_totals(name, refs);
        total += tuning.project_boost * proj_total;
    }
    if let Some((counts, weight)) = draft {
        let (draft_total, draft_distinct) = counts.totals_by_str(suffix);
        total += weight * draft_total;
        distinct += draft_distinct;
    }
    (total, total, distinct)
}

/// Deterministic mass comparison: greater wins, ties break by key
/// ascending, mirroring the production scorer.
fn outranks(mass: f64, best_mass: f64, word: &str, best_word: &str) -> bool {
    match mass.total_cmp(&best_mass) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => word < best_word,
        std::cmp::Ordering::Less => false,
    }
}

/// Score every candidate with stupid backoff over orders `max_order..=0`.
fn score_candidates(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    context: &[String],
    max_order: usize,
    candidates: &[String],
) -> Vec<ReplayScored> {
    let mut scored = Vec::new();
    for key in candidates {
        let mut best: Option<ReplayScored> = None;
        let mut has_higher = false;
        for order in (0..=max_order).rev() {
            let suffix = &context[context.len().saturating_sub(order)..];
            let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
            let (mass, total, _) = combined_at(
                builder, tuning, draft, project, suffix, &refs, key,
            );
            if total <= 0.0 || mass <= 0.0 {
                continue;
            }
            if order >= 1 {
                has_higher = true;
            }
            let probability = mass / total;
            let score = probability
                * tuning.backoff_alpha.powi((max_order - order) as i32);
            let replace = match &best {
                None => true,
                Some(current) => {
                    outranks(score, current.score, key, &current.key)
                }
            };
            if replace {
                best = Some(ReplayScored {
                    key: key.clone(),
                    score,
                    has_higher: false,
                });
            }
        }
        if let Some(mut word) = best {
            word.has_higher = has_higher;
            scored.push(word);
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

/// Top successor at one order by combined mass (key ascending on ties),
/// plus the runner-up share. Mirrors the production gate helper.
fn top_two_at(
    builder: &PromptPredictionBuilder,
    tuning: &ReplayTuning,
    draft: Option<(&DraftCounts, f64)>,
    project: Option<&str>,
    context: &[String],
    order: usize,
    candidates: &[String],
) -> (Option<(String, f64, f64, u64)>, f64) {
    let suffix = &context[context.len().saturating_sub(order)..];
    let refs: Vec<&str> = suffix.iter().map(String::as_str).collect();
    let mut best: Option<(String, f64, f64, u64)> = None;
    for key in candidates {
        if key.as_str() == SEQUENCE_START {
            continue;
        }
        let (mass, total, support) =
            combined_at(builder, tuning, draft, project, suffix, &refs, key);
        if total <= 0.0 || mass <= 0.0 {
            continue;
        }
        let share = mass / total;
        let replace = match &best {
            None => true,
            Some((best_word, best_mass, _, _)) => {
                outranks(mass, *best_mass, key, best_word)
            }
        };
        if replace {
            best = Some((key.clone(), mass, share, support));
        }
    }
    let runner_up = best
        .as_ref()
        .map(|(top_key, _, _, _)| {
            let mut second: f64 = 0.0;
            for key in candidates {
                if key == top_key {
                    continue;
                }
                let (mass, total, _) = combined_at(
                    builder, tuning, draft, project, suffix, &refs, key,
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

/// Whether the gate passes for one position at explicit thresholds.
///
/// Mirrors `apply_gate`: the evidence order is the largest order whose
/// context holds a real word and whose distinct total reaches
/// `min_support`; the backoff top-1 must lead there with share, margin,
/// and support, and (when the tuning rejects conflicts) no higher order
/// may rank another word first.
fn gate_passes(
    record: &PositionRecord,
    tuning: &ReplayTuning,
    min_p: f64,
    min_margin: f64,
    min_support: u64,
) -> bool {
    if !record.scored {
        return false;
    }
    let mut evidence = None;
    for order in (1..record.orders.len()).rev() {
        let evidence_order = &record.orders[order];
        if evidence_order.real && evidence_order.distinct >= min_support {
            evidence = Some(order);
            break;
        }
    }
    let Some(order) = evidence else {
        return false;
    };
    let gate = &record.orders[order];
    if !gate.leader || gate.top_total <= 0.0 {
        return false;
    }
    let probability = gate.top_mass / gate.top_total;
    if probability < min_p
        || gate.top_support < min_support
        || probability - gate.runner_up < min_margin
    {
        return false;
    }
    if tuning.reject_conflicts && gate.conflict_above {
        return false;
    }
    true
}

/// Assemble the aggregate-only report from the recorded positions.
///
/// Preset thresholds come from the production gate presets; the grid
/// sweeps `min_p` 0.40–0.85, `min_margin` 0.05–0.40, and `min_support`
/// 1–5 without replaying.
#[allow(clippy::too_many_arguments)]
fn assemble_report(
    tuning: &ReplayTuning,
    builder: &PromptPredictionBuilder,
    records: &[PositionRecord],
    midword: &[MidwordTrial],
    cohort_chars: &[u64; 3],
    rows_total: u64,
    rows_typed: u64,
    rows_warmed: u64,
    rows_scored: u64,
) -> PromptPredictionReplayReportWire {
    let presets = [PRESET_CAUTIOUS, PRESET_BALANCED, PRESET_EAGER];
    let positions_total = records.len() as u64;
    let total_chars: u64 = cohort_chars.iter().sum();
    let overall_top1 = rate(
        records.iter().filter(|record| record.top1).count(),
        records.len(),
    );
    let overall_top3 = rate(
        records.iter().filter(|record| record.top3).count(),
        records.len(),
    );
    let mut preset_reports = Vec::with_capacity(3);
    for preset in &presets {
        preset_reports.push(gate_metrics_for(
            tuning,
            records,
            None,
            total_chars,
            preset.min_p,
            preset.min_margin,
            preset.min_support,
        ));
    }
    let mut cohorts = Vec::with_capacity(3);
    for (index, name) in COHORT_NAMES.iter().enumerate() {
        let cohort_records: Vec<&PositionRecord> = records
            .iter()
            .filter(|record| record.cohort as usize == index)
            .collect();
        let positions = cohort_records.len();
        let top1 = rate(
            cohort_records.iter().filter(|record| record.top1).count(),
            positions,
        );
        let top3 = rate(
            cohort_records.iter().filter(|record| record.top3).count(),
            positions,
        );
        let mut gated = Vec::with_capacity(3);
        for preset in &presets {
            gated.push(gate_metrics_for(
                tuning,
                records,
                Some(index as u8),
                cohort_chars[index],
                preset.min_p,
                preset.min_margin,
                preset.min_support,
            ));
        }
        cohorts.push(PromptPredictionReplayCohortWire {
            cohort: name.to_string(),
            positions: positions as u64,
            top1,
            top3,
            cautious: gated[0].clone(),
            balanced: gated[1].clone(),
            eager: gated[2].clone(),
        });
    }
    let novel_positions = records
        .iter()
        .filter(|record| record.cohort == COHORT_NOVEL)
        .count();
    let mut sweep = Vec::new();
    for p100 in (40..=85).step_by(5) {
        for m100 in (5..=40).step_by(5) {
            for support in 1..=5u64 {
                let min_p = f64::from(p100) / 100.0;
                let min_margin = f64::from(m100) / 100.0;
                let (gated, correct, novel_gated, novel_correct) =
                    sweep_tallies(tuning, records, min_p, min_margin, support);
                sweep.push(PromptPredictionReplaySweepPointWire {
                    min_p,
                    min_margin,
                    min_support: support,
                    coverage: rate(gated, records.len()),
                    precision: precision_of(correct, gated),
                    novel_coverage: rate(novel_gated, novel_positions),
                    novel_precision: precision_of(novel_correct, novel_gated),
                });
            }
        }
    }
    let mut latencies: Vec<u64> =
        records.iter().map(|record| record.latency_us).collect();
    latencies.sort_unstable();
    // A clone keeps the live builder usable while the frozen snapshot
    // reports production-equivalent memory stats.
    let frozen = builder.clone().finish();
    let stats = frozen.stats();
    PromptPredictionReplayReportWire {
        schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
        rows_total,
        rows_typed,
        rows_warmed,
        rows_scored,
        positions_total,
        overall_top1,
        overall_top3,
        cautious: preset_reports[0].clone(),
        balanced: preset_reports[1].clone(),
        eager: preset_reports[2].clone(),
        cohorts,
        sweep,
        latency_us_p50: percentile(&latencies, 50),
        latency_us_p95: percentile(&latencies, 95),
        corpus_bytes: stats.approx_bytes,
        corpus_rows_used: stats.rows_used,
        corpus_contexts: stats.contexts,
        midword: Some(midword_report(tuning, records, midword, cohort_chars)),
    }
}

/// Mid-word report section: per preset, per typed-prefix length, overall
/// plus per cohort. Trials below the preset's `min_prefix_chars` never
/// complete, so those cells report zero coverage with no precision.
fn midword_report(
    tuning: &ReplayTuning,
    records: &[PositionRecord],
    trials: &[MidwordTrial],
    cohort_chars: &[u64; 3],
) -> Vec<PromptPredictionReplayMidwordPresetWire> {
    let presets = [
        ("cautious", PRESET_CAUTIOUS),
        ("balanced", PRESET_BALANCED),
        ("eager", PRESET_EAGER),
    ];
    let total_chars: u64 = cohort_chars.iter().sum();
    presets
        .iter()
        .map(|(name, preset)| {
            let by_k = MIDWORD_KS
                .iter()
                .map(|k| {
                    midword_metrics_for(
                        tuning,
                        records,
                        trials,
                        *k,
                        preset,
                        None,
                        total_chars,
                    )
                })
                .collect();
            let cohorts = COHORT_NAMES
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    let by_k = MIDWORD_KS
                        .iter()
                        .map(|k| {
                            midword_metrics_for(
                                tuning,
                                records,
                                trials,
                                *k,
                                preset,
                                Some(index as u8),
                                cohort_chars[index],
                            )
                        })
                        .collect();
                    PromptPredictionReplayMidwordCohortWire {
                        cohort: name.to_string(),
                        by_k,
                    }
                })
                .collect();
            PromptPredictionReplayMidwordPresetWire {
                preset: name.to_string(),
                by_k,
                cohorts,
            }
        })
        .collect()
}

/// Metrics for one preset at one typed-prefix length: coverage, precision
/// (completed word equals target), and keystroke savings over scored-row
/// chars (`target_chars - k` per gated-correct trial plus each
/// gated-correct continuation word's chars plus one separator, mirroring
/// production's gated continuation after the completed word).
#[allow(clippy::too_many_arguments)]
fn midword_metrics_for(
    tuning: &ReplayTuning,
    records: &[PositionRecord],
    trials: &[MidwordTrial],
    k: u8,
    preset: &ConfidencePreset,
    cohort: Option<u8>,
    total_chars: u64,
) -> PromptPredictionReplayMidwordMetricsWire {
    let mut positions = 0u64;
    if usize::from(k) < preset.min_prefix_chars {
        for trial in trials {
            if trial.k == k
                && cohort.is_none_or(|cohort| trial.record.cohort == cohort)
            {
                positions += 1;
            }
        }
        return PromptPredictionReplayMidwordMetricsWire {
            k: u64::from(k),
            positions,
            coverage: 0.0,
            precision: None,
            savings: 0.0,
        };
    }
    let mut gated = 0u64;
    let mut correct = 0u64;
    let mut saved = 0u64;
    for trial in trials {
        if trial.k != k
            || cohort.is_some_and(|cohort| trial.record.cohort != cohort)
        {
            continue;
        }
        positions += 1;
        if !gate_passes(
            &trial.record,
            tuning,
            preset.min_p,
            preset.min_margin,
            preset.min_support,
        ) {
            continue;
        }
        gated += 1;
        if !trial.record.top1 {
            continue;
        }
        correct += 1;
        saved +=
            u64::from(trial.record.target_chars).saturating_sub(u64::from(k));
        // Gated continuation: the boundary run after the completed word,
        // capped the way production caps the ghost at `max_words - 1`.
        for step in 1..=usize::from(trial.follows) {
            let Some(next) = records.get(trial.boundary_index + step) else {
                break;
            };
            if next.row != trial.record.row
                || !next.top1
                || !gate_passes(
                    next,
                    tuning,
                    preset.min_p,
                    preset.min_margin,
                    preset.min_support,
                )
            {
                break;
            }
            saved += u64::from(next.target_chars) + 1;
        }
    }
    PromptPredictionReplayMidwordMetricsWire {
        k: u64::from(k),
        positions,
        coverage: rate(gated as usize, positions as usize),
        precision: precision_of(correct as usize, gated as usize),
        savings: if total_chars == 0 {
            0.0
        } else {
            saved as f64 / total_chars as f64
        },
    }
}

fn rate(hits: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

fn precision_of(correct: usize, gated: usize) -> Option<f64> {
    if gated == 0 {
        None
    } else {
        Some(correct as f64 / gated as f64)
    }
}

fn percentile(sorted: &[u64], pct: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() * pct as usize / 100).min(sorted.len() - 1)]
}

/// Gated metrics for one threshold setting over all positions or one
/// cohort: coverage, precision, the keystroke-savings upper bound (chars
/// of correctly gated words plus one separator each, over scored-row
/// chars), and the gated-correct run-length distribution.
fn gate_metrics_for(
    tuning: &ReplayTuning,
    records: &[PositionRecord],
    cohort: Option<u8>,
    total_chars: u64,
    min_p: f64,
    min_margin: f64,
    min_support: u64,
) -> PromptPredictionReplayGateMetricsWire {
    let mut gated = 0usize;
    let mut correct = 0usize;
    let mut saved = 0u64;
    let mut positions = 0usize;
    let mut runs: Vec<u64> = Vec::new();
    let mut current_run = 0u64;
    let mut current_row = u32::MAX;
    for record in records {
        if cohort.is_some_and(|cohort| record.cohort != cohort) {
            continue;
        }
        if record.row != current_row {
            if current_run > 0 {
                runs.push(current_run);
                current_run = 0;
            }
            current_row = record.row;
        }
        positions += 1;
        if gate_passes(record, tuning, min_p, min_margin, min_support) {
            gated += 1;
            if record.top1 {
                correct += 1;
                saved += u64::from(record.target_chars) + 1;
                current_run += 1;
            } else {
                if current_run > 0 {
                    runs.push(current_run);
                    current_run = 0;
                }
            }
        } else if current_run > 0 {
            runs.push(current_run);
            current_run = 0;
        }
    }
    if current_run > 0 {
        runs.push(current_run);
    }
    runs.sort_unstable();
    let run_max = runs.last().copied().unwrap_or(0);
    let run_mean = if runs.is_empty() {
        0.0
    } else {
        runs.iter().sum::<u64>() as f64 / runs.len() as f64
    };
    PromptPredictionReplayGateMetricsWire {
        coverage: rate(gated, positions),
        precision: precision_of(correct, gated),
        savings: if total_chars == 0 {
            0.0
        } else {
            saved as f64 / total_chars as f64
        },
        run_mean,
        run_p95: percentile(&runs, 95) as f64,
        run_max,
    }
}

/// Overall plus novel-cohort gated/correct tallies for one grid point.
fn sweep_tallies(
    tuning: &ReplayTuning,
    records: &[PositionRecord],
    min_p: f64,
    min_margin: f64,
    min_support: u64,
) -> (usize, usize, usize, usize) {
    let mut gated = 0usize;
    let mut correct = 0usize;
    let mut novel_gated = 0usize;
    let mut novel_correct = 0usize;
    for record in records {
        if gate_passes(record, tuning, min_p, min_margin, min_support) {
            gated += 1;
            let novel = record.cohort == COHORT_NOVEL;
            if novel {
                novel_gated += 1;
            }
            if record.top1 {
                correct += 1;
                if novel {
                    novel_correct += 1;
                }
            }
        }
    }
    (gated, correct, novel_gated, novel_correct)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_prediction::model::PromptPredictionModel;
    use crate::prompt_prediction::wire::{
        PromptPredictionModelConfigWire, PromptPredictionRequestWire,
        PromptPredictionSourceRole,
    };
    use std::sync::Arc;

    fn typed(text: &str, epoch: i64) -> PromptPredictionRowWire {
        PromptPredictionRowWire {
            text: text.to_string(),
            epoch_seconds: epoch,
            project: None,
            origin: Some("typed".to_string()),
            cancelled: false,
        }
    }

    fn replay_options() -> PromptPredictionReplayOptionsWire {
        PromptPredictionReplayOptionsWire {
            now_epoch: 10_000,
            ..Default::default()
        }
    }

    fn formulaic_rows() -> Vec<PromptPredictionRowWire> {
        let endings = [
            "now",
            "today",
            "fast",
            "soon",
            "well",
            "please",
            "again",
            "right now",
            "this week",
            "on time",
        ];
        endings
            .iter()
            .enumerate()
            .map(|(i, ending)| {
                typed(
                    &format!("can you help me implement it {ending}"),
                    100 + i as i64,
                )
            })
            .collect()
    }

    #[test]
    fn replay_warms_then_scores_later_rows() {
        let report = evaluate_prompt_prediction_replay(
            &formulaic_rows(),
            &replay_options(),
        );
        assert_eq!(
            report.schema_version,
            PROMPT_PREDICTION_WIRE_SCHEMA_VERSION
        );
        assert_eq!(report.rows_total, 10);
        assert_eq!(report.rows_typed, 10);
        assert_eq!(report.rows_warmed, 4);
        assert_eq!(report.rows_scored, 6);
        assert!(report.positions_total > 0);
        assert_eq!(report.cohorts.len(), 3);
        assert_eq!(report.cohorts[0].cohort, "novel");
        // Formulaic prompts: ungated top-1 is strong once warmed.
        assert!(report.overall_top1 > 0.5, "top1={}", report.overall_top1);
        // The sweep covers the full grid without replaying.
        assert_eq!(report.sweep.len(), 10 * 8 * 5);
        // Every gated grid point also reports the novel-cohort tallies used
        // for preset calibration.
        for point in &report.sweep {
            if point.precision.is_some() {
                assert!(
                    point.novel_coverage >= 0.0 && point.novel_coverage <= 1.0,
                    "novel_coverage={} for {point:?}",
                    point.novel_coverage,
                );
            }
            assert_eq!(
                point.novel_precision.is_some(),
                point.novel_coverage > 0.0,
                "novel precision/coverage agree for {point:?}",
            );
        }
    }

    #[test]
    fn replay_matches_production_ranking_and_gate() {
        // The replay scorer must agree with the frozen model on the same
        // evidence: same menu top-3 and same balanced gate verdict.
        let rows = formulaic_rows();
        let corpus =
            crate::prompt_prediction::corpus::compile_prompt_prediction_corpus(
                &rows[..7],
                &PromptPredictionCorpusOptionsWire {
                    now_epoch: 10_000,
                    ..Default::default()
                },
            );
        let model = PromptPredictionModel::new(
            vec![(Arc::new(corpus), PromptPredictionSourceRole::History, 1.0)],
            PromptPredictionModelConfigWire::default(),
        );
        let mut builder =
            PromptPredictionBuilder::new(PromptPredictionCorpusOptionsWire {
                now_epoch: 10_000,
                ..Default::default()
            });
        for row in &rows[..7] {
            assert!(builder.add_row(row));
        }
        let tuning = ReplayTuning {
            max_context: 4,
            backoff_alpha: 0.4,
            project_boost: 1.0,
            draft_weight: 1.0,
            reject_conflicts: true,
            prune_singletons: false,
            excluded: FastHashSet::default(),
        };
        for prefix in [
            "can you help me",
            "can you help me implement",
            "can you help me implement it",
        ] {
            let words: Vec<String> =
                prefix.split_whitespace().map(str::to_string).collect();
            let prefix_seqs = vec![words.clone()];
            let record = score_position(
                &builder,
                &tuning,
                &words,
                &prefix_seqs,
                None,
                "implement",
                0,
                COHORT_MID,
                Instant::now(),
            );
            let request = PromptPredictionRequestWire {
                schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
                text_before_cursor: format!("{prefix} "),
                project: None,
                limit: 5,
                max_words: 4,
                confidence: "balanced".to_string(),
                include_draft: true,
                complete_current_word: false,
            };
            let result = model.predict(&request);
            let menu_top3: Vec<&str> = result
                .candidates
                .iter()
                .take(3)
                .map(|candidate| candidate.key.as_str())
                .collect();
            // Recompute the replay menu top-3 from the same record path.
            let replay_top1_pass = gate_passes(
                &record,
                &tuning,
                PRESET_BALANCED.min_p,
                PRESET_BALANCED.min_margin,
                PRESET_BALANCED.min_support,
            );
            assert_eq!(
                replay_top1_pass, result.confident,
                "gate parity for {prefix:?}"
            );
            assert_eq!(
                record.top1,
                menu_top3.first() == Some(&"implement"),
                "top-1 parity for {prefix:?}"
            );
        }
    }

    #[test]
    fn replay_skips_generated_rows() {
        let mut rows = formulaic_rows();
        rows.push(PromptPredictionRowWire {
            text: "can you help me implement it now".to_string(),
            epoch_seconds: 1,
            project: None,
            origin: Some("generated".to_string()),
            cancelled: false,
        });
        let report =
            evaluate_prompt_prediction_replay(&rows, &replay_options());
        assert_eq!(report.rows_total, 11);
        assert_eq!(report.rows_typed, 10);
    }

    #[test]
    fn replay_handles_empty_and_single_row() {
        let empty = evaluate_prompt_prediction_replay(&[], &replay_options());
        assert_eq!(empty.positions_total, 0);
        assert_eq!(empty.overall_top1, 0.0);
        assert!(empty.sweep.iter().all(|point| point.precision.is_none()));
        assert!(empty
            .sweep
            .iter()
            .all(|point| point.novel_precision.is_none()));
        let single = evaluate_prompt_prediction_replay(
            &[typed("help me implement it", 100)],
            &replay_options(),
        );
        // One row warms nothing at 40%... it warms floor(0.4)=0 rows and
        // scores the single row with an empty builder.
        assert_eq!(single.rows_warmed, 0);
        assert_eq!(single.rows_scored, 1);
    }

    #[test]
    fn replay_score_every_samples_rows_but_keeps_evidence() {
        let full = evaluate_prompt_prediction_replay(
            &formulaic_rows(),
            &replay_options(),
        );
        let sampled = evaluate_prompt_prediction_replay(
            &formulaic_rows(),
            &PromptPredictionReplayOptionsWire {
                score_every: 2,
                ..replay_options()
            },
        );
        // Every K-th post-warm row is scored: ordinals 0, 2, 4 of the 6
        // post-warm rows.
        assert_eq!(sampled.rows_scored, 3);
        assert!(sampled.positions_total < full.positions_total);
        // Every row still joins the corpus and warms identically.
        assert_eq!(sampled.corpus_rows_used, full.corpus_rows_used);
        assert_eq!(sampled.rows_warmed, full.rows_warmed);
        // Deterministic: the same stride selects the same positions.
        let again = evaluate_prompt_prediction_replay(
            &formulaic_rows(),
            &PromptPredictionReplayOptionsWire {
                score_every: 2,
                ..replay_options()
            },
        );
        assert_eq!(sampled.positions_total, again.positions_total);
        assert_eq!(sampled.overall_top1, again.overall_top1);
    }

    #[test]
    fn replay_gate_presets_order_by_coverage() {
        let report = evaluate_prompt_prediction_replay(
            &formulaic_rows(),
            &replay_options(),
        );
        assert!(
            report.eager.coverage >= report.balanced.coverage,
            "eager={} balanced={}",
            report.eager.coverage,
            report.balanced.coverage
        );
        assert!(
            report.balanced.coverage >= report.cautious.coverage,
            "balanced={} cautious={}",
            report.balanced.coverage,
            report.balanced.coverage
        );
    }

    #[test]
    fn midword_replay_matches_production_completion() {
        use crate::prompt_prediction::predict::PRESET_BALANCED;
        // The mid-word scorer must agree with the frozen model on the
        // same evidence: same restricted gate verdict and same completed
        // word for a typed prefix of a formulaic target.
        let rows = formulaic_rows();
        let corpus =
            crate::prompt_prediction::corpus::compile_prompt_prediction_corpus(
                &rows[..7],
                &PromptPredictionCorpusOptionsWire {
                    now_epoch: 10_000,
                    ..Default::default()
                },
            );
        let model = PromptPredictionModel::new(
            vec![(Arc::new(corpus), PromptPredictionSourceRole::History, 1.0)],
            PromptPredictionModelConfigWire::default(),
        );
        let mut builder =
            PromptPredictionBuilder::new(PromptPredictionCorpusOptionsWire {
                now_epoch: 10_000,
                ..Default::default()
            });
        for row in &rows[..7] {
            assert!(builder.add_row(row));
        }
        let tuning = ReplayTuning {
            max_context: 4,
            backoff_alpha: 0.4,
            project_boost: 1.0,
            draft_weight: 1.0,
            reject_conflicts: true,
            prune_singletons: false,
            excluded: FastHashSet::default(),
        };
        // Target "implement" at position 4 of its sequence; the draft sees
        // the words before it, exactly like the production completion
        // draft with the partial word removed.
        let keys: Vec<String> = ["can", "you", "help", "me", "implement"]
            .iter()
            .map(|word| (*word).to_string())
            .collect();
        let pos = 4;
        let context = replay_context(true, &keys[..pos], tuning.max_context);
        let prefix_seqs = vec![keys[..pos].to_vec()];
        let mut trials = Vec::new();
        score_midword_trials(
            &builder,
            &tuning,
            &context,
            &prefix_seqs,
            None,
            &keys[pos],
            &keys,
            pos,
            0,
            COHORT_MID,
            0,
            &mut trials,
        );
        // "implement" is 9 chars: trials for k=1..=4.
        assert_eq!(trials.len(), 4);
        let request = PromptPredictionRequestWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            text_before_cursor: "can you help me imp".to_string(),
            project: None,
            limit: 5,
            max_words: 4,
            confidence: "balanced".to_string(),
            include_draft: true,
            complete_current_word: true,
        };
        let result = model.predict(&request);
        let completion = result.word_completion.expect("word_completion");
        assert_eq!(completion.word, "implement");
        assert_eq!(completion.suffix, "lement");
        let trial =
            trials.iter().find(|trial| trial.k == 3).expect("k=3 trial");
        let replay_pass = gate_passes(
            &trial.record,
            &tuning,
            PRESET_BALANCED.min_p,
            PRESET_BALANCED.min_margin,
            PRESET_BALANCED.min_support,
        );
        assert_eq!(
            replay_pass, result.confident,
            "restricted gate parity for 'imp'"
        );
        assert_eq!(
            trial.record.top1,
            completion.word == keys[pos],
            "completion parity for 'imp'"
        );
    }

    #[test]
    fn midword_report_enforces_min_prefix_chars() {
        let report = evaluate_prompt_prediction_replay(
            &formulaic_rows(),
            &replay_options(),
        );
        let midword = report.midword.expect("midword section");
        assert_eq!(midword.len(), 3);
        for preset in &midword {
            assert_eq!(preset.by_k.len(), 4);
            assert_eq!(preset.cohorts.len(), 3);
            for cell in &preset.by_k {
                assert!(
                    (1..=4).contains(&cell.k),
                    "k={} for {}",
                    cell.k,
                    preset.preset
                );
            }
        }
        let balanced = midword
            .iter()
            .find(|preset| preset.preset == "balanced")
            .expect("balanced midword");
        // Balanced needs 2 typed chars: the k=1 cell never completes.
        let k1 = &balanced.by_k[0];
        assert_eq!(k1.k, 1);
        assert!(k1.positions > 0);
        assert_eq!(k1.coverage, 0.0);
        assert!(k1.precision.is_none());
        // Longer prefixes trial real completions on formulaic prompts.
        let k3 = &balanced.by_k[2];
        assert_eq!(k3.k, 3);
        assert!(k3.positions > 0);
        assert!(k3.coverage > 0.0, "coverage={}", k3.coverage);
        assert!(k3.precision.is_some());
    }
}
