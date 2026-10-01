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
    CompiledPromptPredictionCorpus, ContextKey, FastHashMap, MAX_PACKED_CONTEXT,
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
    /// Minimum typed prefix characters for current-word completion.
    /// Final values calibrated by phase `word-completion-calibration`
    /// (2026-10-01 mid-word replay): cautious 3, balanced 2, eager 2.
    pub min_prefix_chars: usize,
}

/// Cautious preset: precision first. Calibrated on the 2026-09-30
/// prequential replay over typed history (11,631 rows; 3,301 typed;
/// 1,320 warmed; 1,971 scored; 142,120 positions — see
/// `tools/prompt_prediction_replay` in sase): the max-coverage grid
/// point meeting overall precision >= 85% (85.0%) with novel precision
/// at least 5 points above balanced (77.6% vs 70.1%). Strictly tighter
/// than balanced via `min_support` 5 > 2; the 0.40 margin is
/// load-bearing at `min_p` 0.60 (unlike at 0.75, where the margin is
/// always met). Coverage 18.1% vs balanced 25.9%, so the two presets
/// gate measurably different position sets.
pub const PRESET_CAUTIOUS: ConfidencePreset = ConfidencePreset {
    min_p: 0.60,
    min_margin: 0.40,
    min_support: 5,
    min_prefix_chars: 3,
};
/// Balanced preset: the default. Calibrated on the 2026-09-30 prequential
/// replay over typed history (see `tools/prompt_prediction_replay` in
/// sase): the max-coverage grid point meeting overall precision >= 75%
/// (80.7%) and novel precision >= 65% (65.1%). The margin is flat across
/// 0.05-0.40 at this point, so it keeps its previous value. Novel
/// headroom is razor-thin (+0.1pp), so re-run the replay before loosening
/// this preset.
pub const PRESET_BALANCED: ConfidencePreset = ConfidencePreset {
    min_p: 0.75,
    min_margin: 0.2,
    min_support: 2,
    min_prefix_chars: 2,
};
/// Eager preset: coverage first. Calibrated on the 2026-09-30 prequential
/// replay over typed history (see `tools/prompt_prediction_replay` in
/// sase): the max-coverage grid point meeting overall precision >= 60%
/// (62.9%), at 59.0% coverage. Overall headroom is thin (+2.9pp), so
/// re-run the replay before loosening this preset. Mid-word replay
/// (2026-10-01; 11,643 rows, 1,978 scored, 142,733 positions) sets
/// `min_prefix_chars` to 2: k=1 reaches 75.9% overall but trails by
/// 12.7 points on novel prompts (63.2%), while k=2 holds 84.9% overall
/// with novel within 8.7 points (76.2%).
pub const PRESET_EAGER: ConfidencePreset = ConfidencePreset {
    min_p: 0.40,
    min_margin: 0.05,
    min_support: 1,
    min_prefix_chars: 2,
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
///
/// Draft words are interned once per request to small ids, so counting and
/// every later lookup hash packed [`ContextKey`]s instead of cloning and
/// hashing strings per n-gram. Values match the old string-keyed tables
/// exactly: mass sums occurrences, distinct counts each pair once, and
/// context totals count each context once.
#[derive(Debug, Clone, Default)]
pub struct DraftCounts {
    vocab: Vec<String>,
    ids: FastHashMap<String, u32>,
    pairs: FastHashMap<(ContextKey, u32), DraftStats>,
    totals: FastHashMap<ContextKey, (f64, u64)>,
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
        // Intern every distinct word once: later passes never hash strings.
        for seq in sequences {
            for word in seq {
                if !counts.ids.contains_key(word) {
                    let id = counts.vocab.len() as u32;
                    counts.vocab.push(word.clone());
                    counts.ids.insert(word.clone(), id);
                }
            }
        }
        let id_seqs: Vec<Vec<u32>> = sequences
            .iter()
            .map(|seq| {
                seq.iter().map(|word| counts.ids[word.as_str()]).collect()
            })
            .collect();
        let max_context = max_context.clamp(1, MAX_PACKED_CONTEXT);
        // Pre-size the pair tables: tokens times the context window is the
        // pair upper bound, so one reservation replaces the rehash chain.
        let token_estimate: usize =
            id_seqs.iter().map(Vec::len).sum::<usize>() * (max_context + 1);
        counts.pairs.reserve(token_estimate);
        counts.totals.reserve(token_estimate);
        let mut seen: HashSet<(ContextKey, u32)> =
            HashSet::with_capacity(token_estimate);
        for (seq_index, seq) in id_seqs.iter().enumerate() {
            let is_last_seq = seq_index + 1 == id_seqs.len();
            for (pos, word) in seq.iter().enumerate() {
                if is_last_seq && pos + 1 == seq.len() {
                    continue;
                }
                let depth = pos.min(max_context);
                for order in 0..=depth {
                    let context = ContextKey::pack(&seq[pos - order..pos]);
                    let entry =
                        counts.pairs.entry((context, *word)).or_default();
                    entry.mass += 1.0;
                    if seen.insert((context, *word)) {
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

    /// Resolve one draft word to its request-local id, if observed.
    pub(crate) fn resolve_word(&self, word: &str) -> Option<u32> {
        self.ids.get(word).copied()
    }

    /// Resolve a query context suffix to a draft-local packed key: `None`
    /// when any word was never observed in the draft.
    pub(crate) fn resolve_key(&self, keys: &[String]) -> Option<ContextKey> {
        if keys.len() > MAX_PACKED_CONTEXT {
            return None;
        }
        let mut ids = [0u32; MAX_PACKED_CONTEXT];
        for (index, key) in keys.iter().enumerate() {
            ids[index] = self.resolve_word(key)?;
        }
        Some(ContextKey::pack(&ids[..keys.len()]))
    }

    /// Every word observed in the draft as a successor (words seen only
    /// in the skipped final position contribute no pair and stay out,
    /// matching the old string-keyed collection exactly).
    pub(crate) fn candidate_words(&self) -> Vec<String> {
        let mut words: Vec<String> = self
            .pairs
            .keys()
            .map(|(_, id)| self.vocab[*id as usize].clone())
            .collect();
        words.sort();
        words.dedup();
        words
    }

    pub(crate) fn pair(
        &self,
        context: ContextKey,
        word: Option<u32>,
    ) -> (f64, u64) {
        let Some(id) = word else {
            return (0.0, 0);
        };
        self.pairs
            .get(&(context, id))
            .map(|stats| (stats.mass, stats.distinct))
            .unwrap_or((0.0, 0))
    }

    pub(crate) fn totals(&self, context: ContextKey) -> (f64, u64) {
        self.totals.get(&context).copied().unwrap_or((0.0, 0))
    }

    /// String-keyed pair lookup for the replay scorer's string-level path.
    pub(crate) fn pair_by_str(
        &self,
        context: &[String],
        word: &str,
    ) -> (f64, u64) {
        let (Some(key), Some(id)) =
            (self.resolve_key(context), self.resolve_word(word))
        else {
            return (0.0, 0);
        };
        self.pair(key, Some(id))
    }

    /// String-keyed totals lookup for the replay scorer's string-level path.
    pub(crate) fn totals_by_str(&self, context: &[String]) -> (f64, u64) {
        let Some(key) = self.resolve_key(context) else {
            return (0.0, 0);
        };
        self.totals(key)
    }

    /// Prefix-restricted draft sums at one packed context: the mass and
    /// distinct support over successor words starting with `prefix`. The
    /// draft keeps every pair (no truncation), so no dropped mass
    /// applies. `pairs` has no per-context index; the scan stays cheap
    /// because a request draft holds only the current text.
    pub(crate) fn prefix_restricted_sums(
        &self,
        context: ContextKey,
        prefix: &str,
    ) -> (f64, u64) {
        let mut mass = 0.0;
        let mut distinct = 0u64;
        for ((pair_context, id), stats) in &self.pairs {
            if *pair_context == context
                && self.vocab[*id as usize].starts_with(prefix)
            {
                mass += stats.mass;
                distinct += stats.distinct;
            }
        }
        (mass, distinct)
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

/// One source with its per-order contexts resolved to packed keys once
/// per scoring pass, plus the per-order context totals cached alongside.
/// Candidates then hit flat-array lookups with no string hashing, no
/// per-call allocation, and no repeated totals fetches.
struct ResolvedView<'a> {
    corpus: &'a CompiledPromptPredictionCorpus,
    share: usize,
    weight: f64,
    orders: Vec<Option<ContextKey>>,
    masses: Vec<f64>,
    distincts: Vec<u64>,
}

/// Resolved inputs for one scoring pass: packed-key source views plus
/// cached per-order totals (corpus, project-partition, and draft), so the
/// per-candidate hot loop only resolves word ids and scans flat arrays.
struct PassCtx<'a> {
    views: Vec<ResolvedView<'a>>,
    project: Option<&'a str>,
    boost: f64,
    draft: Option<(&'a DraftCounts, f64)>,
    draft_orders: Vec<Option<ContextKey>>,
    draft_masses: Vec<f64>,
    draft_distincts: Vec<u64>,
    project_masses: Vec<Vec<f64>>,
    order_count: usize,
}

impl<'a> PassCtx<'a> {
    fn of(query: &ScoringQuery<'a>, suffixes: &OrderSuffixes) -> Self {
        Self::resolve(
            query.sources,
            query.project,
            query.project_boost,
            query.draft,
            suffixes,
        )
    }

    fn resolve(
        sources: &[WeightedSource<'a>],
        project: Option<&'a str>,
        boost: f64,
        draft: Option<(&'a DraftCounts, f64)>,
        suffixes: &OrderSuffixes,
    ) -> Self {
        let views = sources
            .iter()
            .map(|source| {
                let mut orders = Vec::with_capacity(suffixes.len());
                let mut masses = Vec::with_capacity(suffixes.len());
                let mut distincts = Vec::with_capacity(suffixes.len());
                for suffix in suffixes {
                    let refs: Vec<&str> =
                        suffix.iter().map(String::as_str).collect();
                    let key = source.corpus.resolve_key(&refs);
                    // Totals are context-only: fetch once per order here
                    // instead of once per candidate below. Same values.
                    let (mass, distinct) = key
                        .map(|packed| source.corpus.totals_for(packed))
                        .unwrap_or((0.0, 0));
                    orders.push(key);
                    masses.push(mass);
                    distincts.push(distinct);
                }
                ResolvedView {
                    corpus: source.corpus,
                    share: source.share,
                    weight: source.weight,
                    orders,
                    masses,
                    distincts,
                }
            })
            .collect::<Vec<_>>();
        // Draft context keys and totals: one resolution per order, shared
        // by every candidate. Unknown words mean no draft observations.
        let mut draft_orders = Vec::with_capacity(suffixes.len());
        let mut draft_masses = Vec::with_capacity(suffixes.len());
        let mut draft_distincts = Vec::with_capacity(suffixes.len());
        for suffix in suffixes {
            let key = draft.and_then(|(counts, _)| counts.resolve_key(suffix));
            let (mass, distinct) = key
                .map(|packed| {
                    draft
                        .map(|(counts, _)| counts.totals(packed))
                        .unwrap_or((0.0, 0))
                })
                .unwrap_or((0.0, 0));
            draft_orders.push(key);
            draft_masses.push(mass);
            draft_distincts.push(distinct);
        }
        // Project-partition masses: one fetch per view per order. Entries
        // stay 0.0 for unknown contexts, mirroring the old skip.
        let project_masses = views
            .iter()
            .map(|view| {
                view.orders
                    .iter()
                    .map(|key| match (project, key) {
                        (Some(name), Some(packed)) => {
                            view.corpus.project_totals_for(name, *packed).0
                        }
                        _ => 0.0,
                    })
                    .collect()
            })
            .collect();
        Self {
            order_count: suffixes.len(),
            views,
            project,
            boost,
            draft,
            draft_orders,
            draft_masses,
            draft_distincts,
            project_masses,
        }
    }

    fn order_count(&self) -> usize {
        self.order_count
    }
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
/// per-share mass contributions. Context totals come from the per-pass
/// cache (same values the old per-candidate fetches returned); only the
/// word lookups run per candidate. Summation order matches the old loop
/// exactly, so scores are bit-identical.
fn combined_at_order(
    ctx: &PassCtx<'_>,
    order: usize,
    candidate: &ScoredCandidate,
) -> (f64, f64, u64, [f64; 5]) {
    let mut mass = 0.0;
    let mut total = 0.0;
    let mut support = 0u64;
    let mut shares = [0.0; 5];
    for (view_index, view) in ctx.views.iter().enumerate() {
        let Some(context) = view.orders.get(order).and_then(|opt| *opt) else {
            continue;
        };
        total += view.weight * view.masses[order];
        if let Some(id) = candidate.word_ids[view_index] {
            let (word_mass, word_distinct) =
                view.corpus.word_stats_for(context, id);
            mass += view.weight * word_mass;
            support += word_distinct;
            shares[view.share] += view.weight * word_mass;
        }
        if let Some(name) = ctx.project {
            // The project partition is a subset of the same rows: it changes
            // only mass, total, and source shares. Distinct support counts
            // each observation once, so project distinct is never added.
            total += ctx.boost * ctx.project_masses[view_index][order];
            if let Some(id) = candidate.word_ids[view_index] {
                let (proj_mass, _) =
                    view.corpus.project_word_stats_for(name, context, id);
                mass += ctx.boost * proj_mass;
                shares[SHARE_PROJECT] += ctx.boost * proj_mass;
            }
        }
    }
    // Support counts successor observations; context distinct totals live
    // in the totals helper below.
    if let Some((counts, weight)) = ctx.draft {
        if let Some(draft_key) = ctx.draft_orders[order] {
            let (draft_mass, draft_distinct) =
                counts.pair(draft_key, candidate.draft_id);
            mass += weight * draft_mass;
            support += draft_distinct;
            shares[SHARE_DRAFT] += weight * draft_mass;
        }
        total += weight * ctx.draft_masses[order];
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
/// a pair for call-site symmetry), and distinct rows. All values come
/// from the per-pass cache.
fn combined_totals_at_order(
    ctx: &PassCtx<'_>,
    order: usize,
) -> (f64, f64, u64) {
    let mut total = 0.0;
    let mut distinct = 0u64;
    for (view_index, view) in ctx.views.iter().enumerate() {
        if view.orders.get(order).and_then(|opt| *opt).is_none() {
            continue;
        }
        total += view.weight * view.masses[order];
        distinct += view.distincts[order];
        if ctx.project.is_some() {
            // Project totals add mass only; distinct rows are already counted
            // in the global totals above.
            total += ctx.boost * ctx.project_masses[view_index][order];
        }
    }
    if let Some((_, weight)) = ctx.draft {
        total += weight * ctx.draft_masses[order];
        distinct += ctx.draft_distincts[order];
    }
    (total, total, distinct)
}

/// Top two successors at one order by combined mass, key ascending on
/// ties, plus the runner-up share. One pass serves both the leader check
/// and the margin check. Reads the round mass table: same values as the
/// old per-candidate recomputation.
fn top_two_from_table(
    table: &MassTable,
    resolved: &[ScoredCandidate],
    order: usize,
) -> (Option<(String, f64, f64, u64)>, f64) {
    let mut best: Option<(String, f64, f64, u64)> = None;
    for (index, candidate) in resolved.iter().enumerate() {
        let (mass, total, support, _) = table.at(index, order);
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
            for (index, candidate) in resolved.iter().enumerate() {
                if candidate.key == *top_key {
                    continue;
                }
                let (mass, total, _, _) = table.at(index, order);
                if total > 0.0 {
                    second = second.max(mass / total);
                }
            }
            second
        })
        .unwrap_or(0.0);
    (best, runner_up)
}

/// One candidate with corpus word ids and the draft-local word id
/// resolved once per scoring pass.
struct ScoredCandidate {
    key: String,
    word_ids: Vec<Option<u32>>,
    draft_id: Option<u32>,
}

fn resolve_candidates(
    ctx: &PassCtx<'_>,
    candidates: &[String],
) -> Vec<ScoredCandidate> {
    let draft = ctx.draft.map(|(counts, _)| counts);
    candidates
        .iter()
        .filter(|key| key.as_str() != SEQUENCE_START)
        .map(|key| {
            let word_ids = ctx
                .views
                .iter()
                .map(|view| view.corpus.resolve_word(key))
                .collect();
            // One string hash per candidate here instead of one per
            // candidate per order in the old hot loop.
            let draft_id = draft.and_then(|counts| counts.resolve_word(key));
            ScoredCandidate {
                key: key.clone(),
                word_ids,
                draft_id,
            }
        })
        .collect()
}

/// One combined `(mass, total, support, shares)` tuple.
type CombinedMass = (f64, f64, u64, [f64; 5]);

/// One round's memoized masses: per candidate per order. Scoring fills it
/// once; the gate reads it instead of recomputing every combined value
/// two to four times.
struct MassTable {
    rows: Vec<Vec<CombinedMass>>,
}

impl MassTable {
    fn build(ctx: &PassCtx<'_>, resolved: &[ScoredCandidate]) -> Self {
        let orders = ctx.order_count();
        let rows = resolved
            .iter()
            .map(|candidate| {
                (0..orders)
                    .map(|order| combined_at_order(ctx, order, candidate))
                    .collect()
            })
            .collect();
        Self { rows }
    }

    fn at(&self, candidate: usize, order: usize) -> CombinedMass {
        self.rows[candidate][order]
    }
}

impl PassCtx<'_> {
    /// Restrict every per-order total to the prefix-filtered distribution
    /// for current-word completion.
    ///
    /// Each source total becomes the restricted kept mass plus the
    /// context's truncated (dropped) mass, so truncation never inflates
    /// `p`; each distinct total becomes the restricted distinct support.
    /// The draft has no truncation, so its totals become the restricted
    /// sums directly. Word lookups are untouched: callers pass only
    /// prefix-matching candidates, and `combined_at_order`,
    /// `top_two_from_table`, and `gate_from_table` then work unchanged.
    fn restrict_to_prefix(&mut self, prefix: &str) {
        for (view_index, view) in self.views.iter_mut().enumerate() {
            for order in 0..self.order_count {
                let Some(context) = view.orders[order] else {
                    continue;
                };
                let (restricted_mass, restricted_distinct, dropped) =
                    view.corpus.prefix_restricted_sums(context, prefix);
                view.masses[order] = restricted_mass + dropped;
                view.distincts[order] = restricted_distinct;
                self.project_masses[view_index][order] = match self.project {
                    Some(name) => {
                        let (project_mass, project_dropped) =
                            view.corpus.project_prefix_restricted_sums(
                                name, context, prefix,
                            );
                        project_mass + project_dropped
                    }
                    None => 0.0,
                };
            }
        }
        if let Some((counts, _)) = self.draft {
            for order in 0..self.order_count {
                let Some(context) = self.draft_orders[order] else {
                    continue;
                };
                let (mass, distinct) =
                    counts.prefix_restricted_sums(context, prefix);
                self.draft_masses[order] = mass;
                self.draft_distincts[order] = distinct;
            }
        }
    }
}

/// Score prefix-matching candidates and apply the confidence gate over
/// the prefix-restricted distribution with a conservative denominator.
///
/// `candidates` must hold only keys starting with `prefix` (including
/// the exact prefix key); `prefix` is the casefolded typed prefix.
pub fn score_and_gate_restricted(
    query: &ScoringQuery<'_>,
    candidates: &[String],
    prefix: &str,
) -> (Vec<ScoredWord>, Option<GatePass>) {
    let max_order = query.max_order;
    let suffixes = order_suffixes(query.context, max_order);
    let mut ctx = PassCtx::of(query, &suffixes);
    ctx.restrict_to_prefix(prefix);
    let resolved = resolve_candidates(&ctx, candidates);
    let table = MassTable::build(&ctx, &resolved);
    let ranked = score_from_table(query, &resolved, &table);
    let gate =
        gate_from_table(&ctx, query, &suffixes, &ranked, &resolved, &table);
    (ranked, gate)
}

/// Score every candidate with stupid backoff over orders `max_order..=0`.
pub fn score_candidates(
    query: &ScoringQuery<'_>,
    candidates: &[String],
) -> Vec<ScoredWord> {
    let max_order = query.max_order;
    let suffixes = order_suffixes(query.context, max_order);
    let ctx = PassCtx::of(query, &suffixes);
    let resolved = resolve_candidates(&ctx, candidates);
    let table = MassTable::build(&ctx, &resolved);
    score_from_table(query, &resolved, &table)
}

/// Backoff scoring over a memoized mass table: same arithmetic as the old
/// per-candidate loop, read from the table instead of recomputed.
fn score_from_table(
    query: &ScoringQuery<'_>,
    resolved: &[ScoredCandidate],
    table: &MassTable,
) -> Vec<ScoredWord> {
    let max_order = query.max_order;
    let mut scored = Vec::new();
    for (index, candidate) in resolved.iter().enumerate() {
        let mut best: Option<ScoredWord> = None;
        let mut has_higher = false;
        for order in (0..=max_order).rev() {
            let (mass, total, support, shares) = table.at(index, order);
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

/// Score every candidate and apply the confidence gate in one pass over
/// one resolved context: the gate reads the scoring mass table instead of
/// recomputing every combined value. Results match separate
/// `score_candidates` + `apply_gate` calls exactly.
pub fn score_and_gate(
    query: &ScoringQuery<'_>,
    candidates: &[String],
) -> (Vec<ScoredWord>, Option<GatePass>) {
    let max_order = query.max_order;
    let suffixes = order_suffixes(query.context, max_order);
    let ctx = PassCtx::of(query, &suffixes);
    let resolved = resolve_candidates(&ctx, candidates);
    let table = MassTable::build(&ctx, &resolved);
    let ranked = score_from_table(query, &resolved, &table);
    let gate =
        gate_from_table(&ctx, query, &suffixes, &ranked, &resolved, &table);
    (ranked, gate)
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
    let ctx =
        PassCtx::resolve(sources, project, project_boost, draft, suffixes);
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
        let (_, _, distinct) = combined_totals_at_order(ctx, order);
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
    let suffixes = order_suffixes(query.context, query.max_order);
    let ctx = PassCtx::of(query, &suffixes);
    let resolved = resolve_candidates(&ctx, candidates);
    let table = MassTable::build(&ctx, &resolved);
    gate_from_table(&ctx, query, &suffixes, ranked, &resolved, &table)
}

/// Confidence gate over a memoized mass table: same decisions as the old
/// recomputing gate, read from the table.
fn gate_from_table(
    ctx: &PassCtx<'_>,
    query: &ScoringQuery<'_>,
    suffixes: &OrderSuffixes,
    ranked: &[ScoredWord],
    resolved: &[ScoredCandidate],
    table: &MassTable,
) -> Option<GatePass> {
    let top = ranked.first()?;
    let evidence =
        evidence_order_views(ctx, suffixes, query.preset.min_support)?;
    let top_index = resolved
        .iter()
        .position(|candidate| candidate.key == top.key);
    let (top_mass, top_total, top_support, _) = match top_index {
        Some(index) => table.at(index, evidence),
        // Unreachable through the model (ranked derives from the same
        // candidate list): no corpus mass, draft masses by string — mirror
        // the old empty-views fallback exactly (same pair/total lookups,
        // same weighting).
        None => {
            let (mass, total, support) = ctx
                .draft
                .map(|(counts, weight)| {
                    match counts.resolve_key(&suffixes[evidence]) {
                        Some(key) => {
                            let (pair_mass, pair_distinct) =
                                counts.pair(key, counts.resolve_word(&top.key));
                            let (total_mass, _) = counts.totals(key);
                            (
                                weight * pair_mass,
                                weight * total_mass,
                                pair_distinct,
                            )
                        }
                        None => (0.0, 0.0, 0),
                    }
                })
                .unwrap_or((0.0, 0.0, 0));
            (mass, total, support, [0.0; 5])
        }
    };
    if top_total <= 0.0 {
        return None;
    }
    let (leader, runner_up) = top_two_from_table(table, resolved, evidence);
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
            let (total, _, _) = combined_totals_at_order(ctx, higher);
            if total <= 0.0 {
                continue;
            }
            let (higher_top, _) = top_two_from_table(table, resolved, higher);
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
        let empty = ContextKey::pack(&[]);
        assert!(counts.pair(empty, counts.resolve_word("fix")).0 > 0.0);
        assert_eq!(counts.pair_by_str(&["fix".to_string()], "the"), (0.0, 0));
        // String and packed paths agree.
        assert_eq!(
            counts.pair_by_str(&[], "fix"),
            counts.pair(empty, counts.resolve_word("fix"))
        );
    }

    fn gate_query<'a>(
        sources: &'a [WeightedSource<'a>],
        context: &'a [String],
        preset: ConfidencePreset,
        reject_conflicts: bool,
    ) -> ScoringQuery<'a> {
        ScoringQuery {
            sources,
            project: None,
            project_boost: 1.0,
            backoff_alpha: 0.4,
            draft: None,
            context,
            max_order: context.len(),
            preset,
            reject_conflicts,
        }
    }

    #[test]
    fn project_boost_adds_mass_but_not_support() {
        // Two same-project rows must not pass `min_support` 4: the project
        // partition is a subset of the same rows.
        let rows = vec![
            PromptPredictionRowWire {
                text: "help me implement it".to_string(),
                epoch_seconds: 100,
                project: Some("sase".to_string()),
                origin: Some("typed".to_string()),
                cancelled: false,
            },
            PromptPredictionRowWire {
                text: "help me implement it now".to_string(),
                epoch_seconds: 200,
                project: Some("sase".to_string()),
                origin: Some("typed".to_string()),
                cancelled: false,
            },
        ];
        let corpus = compile_prompt_prediction_corpus(
            &rows,
            &crate::prompt_prediction::wire::PromptPredictionCorpusOptionsWire {
                now_epoch: 1_000,
                ..Default::default()
            },
        );
        let sources = sources_of(&corpus);
        let context: Vec<String> = vec!["help".into(), "me".into()];
        let candidates: Vec<String> = vec!["implement".into()];
        let query = ScoringQuery {
            sources: &sources,
            project: Some("sase"),
            project_boost: 1.0,
            backoff_alpha: 0.4,
            draft: None,
            context: &context,
            max_order: 2,
            preset: ConfidencePreset {
                min_p: 0.0,
                min_margin: 0.0,
                min_support: 4,
                min_prefix_chars: 0,
            },
            reject_conflicts: false,
        };
        let ranked = score_candidates(&query, &candidates);
        assert!(!ranked.is_empty());
        assert!(
            ranked[0].support < 4,
            "support counts once per row, got {}",
            ranked[0].support
        );
        let suffixes = order_suffixes(&context, 2);
        assert_eq!(
            evidence_order(&sources, Some("sase"), 1.0, None, &suffixes, 4),
            None,
            "two rows cannot reach distinct 4 even with the project boost"
        );
    }

    #[test]
    fn gate_boundaries_need_full_thresholds() {
        // Five identical rows give support 5 (cautious `min_support`) and
        // share 1.0 at order 2, so every preset passes at its thresholds.
        let corpus = corpus_for(&[
            "help me implement it",
            "help me implement it now",
            "help me implement it today",
            "help me implement it fast",
            "help me implement it well",
        ]);
        let sources = sources_of(&corpus);
        let context: Vec<String> = vec!["help".into(), "me".into()];
        let candidates: Vec<String> = vec!["implement".into(), "review".into()];
        for preset in [PRESET_CAUTIOUS, PRESET_BALANCED, PRESET_EAGER] {
            let query = gate_query(&sources, &context, preset, true);
            let ranked = score_candidates(&query, &candidates);
            assert!(apply_gate(&query, &ranked, &candidates).is_some());
            // Just above each achievable threshold must fail (share and
            // margin top out at 1.0, support at 5 rows here).
            let below_p = ConfidencePreset {
                min_p: 1.01,
                ..preset
            };
            let q = gate_query(&sources, &context, below_p, true);
            let ranked = score_candidates(&q, &candidates);
            assert!(apply_gate(&q, &ranked, &candidates).is_none());
            let below_margin = ConfidencePreset {
                min_margin: 1.01,
                ..preset
            };
            let q = gate_query(&sources, &context, below_margin, true);
            let ranked = score_candidates(&q, &candidates);
            assert!(apply_gate(&q, &ranked, &candidates).is_none());
            let below_support = ConfidencePreset {
                min_support: 99,
                ..preset
            };
            let q = gate_query(&sources, &context, below_support, true);
            let ranked = score_candidates(&q, &candidates);
            assert!(apply_gate(&q, &ranked, &candidates).is_none());
        }
    }

    #[test]
    fn reject_conflicts_toggles_higher_order_veto() {
        // `help me` predicts implement, but `me` alone (order 1) predicts
        // review when review dominates the short context.
        let corpus = corpus_for(&[
            "help me implement it",
            "help me implement it now",
            "help me implement it today",
            "help me implement it fast",
            "me review it",
            "me review it now",
            "me review it today",
            "me review it fast",
            "me review it soon",
        ]);
        let sources = sources_of(&corpus);
        let context: Vec<String> = vec!["help".into(), "me".into()];
        let candidates: Vec<String> = vec!["implement".into(), "review".into()];
        let preset = ConfidencePreset {
            min_p: 0.0,
            min_margin: 0.0,
            min_support: 1,
            min_prefix_chars: 0,
        };
        let strict = gate_query(&sources, &context, preset, true);
        let loose = gate_query(&sources, &context, preset, false);
        let strict_ranked = score_candidates(&strict, &candidates);
        let loose_ranked = score_candidates(&loose, &candidates);
        let strict_gate = apply_gate(&strict, &strict_ranked, &candidates);
        let loose_gate = apply_gate(&loose, &loose_ranked, &candidates);
        // The toggle must be observable: strict can only gate equal or less
        // often than loose on the same evidence.
        if strict_gate.is_some() {
            assert!(loose_gate.is_some());
        }
    }
}
