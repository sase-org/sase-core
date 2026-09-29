//! Compiled n-gram corpus for prompt prediction.
//!
//! Each history row contributes recency-weighted mass and a distinct-row
//! observation per `(context, word)` pair, at most once per row. Context
//! totals are accumulated before successor truncation. Per-project
//! partitions carry the same-project mass behind the model-level project
//! boost. [`PromptPredictionBuilder`] offers the same accumulation behind
//! the [`PromptSuccessorSource`] trait so the replay phase can query a
//! mutable builder without recompiling.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use crate::prompt_prediction::origin::looks_generated;
use crate::prompt_prediction::tokenize::{
    canonical_surface, tokenize_prompt_text, SEQUENCE_START,
};
use crate::prompt_prediction::wire::{
    PromptPredictionCorpusOptionsWire, PromptPredictionCorpusStatsWire,
    PromptPredictionRowWire, PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
};

/// Minimum retained recency weight.
const MIN_RECENCY_WEIGHT: f64 = 0.05;
/// Seconds per day, for recency decay.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// FNV-1a hasher: cheap and deterministic for internal n-gram maps.
/// `std` SipHash dominates debug profiles, so every hot map here uses this.
#[derive(Debug, Clone)]
pub(crate) struct FnvBuild;

#[derive(Debug, Clone)]
pub(crate) struct FnvHasher(u64);

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

impl Default for FnvBuild {
    fn default() -> Self {
        Self
    }
}

impl BuildHasher for FnvBuild {
    type Hasher = FnvHasher;

    fn build_hasher(&self) -> FnvHasher {
        FnvHasher(FNV_OFFSET)
    }
}

impl Hasher for FnvHasher {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// Fast internal map/set aliases for hot n-gram tables.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, FnvBuild>;
pub(crate) type FastHashSet<K> = HashSet<K, FnvBuild>;

fn fast_map<K, V>() -> FastHashMap<K, V> {
    HashMap::default()
}

fn fast_set<K>() -> FastHashSet<K> {
    HashSet::default()
}

/// One successor observation bundle.
#[derive(Debug, Clone, PartialEq, Default)]
struct SuccessorStats {
    mass: f64,
    distinct: u64,
}

/// One context entry: pre-truncation totals plus kept successors.
#[derive(Debug, Clone, PartialEq, Default)]
struct ContextStats {
    total_mass: f64,
    total_distinct: u64,
    successors: FastHashMap<u32, SuccessorStats>,
}

/// Query interface over successor statistics.
///
/// Contexts hold casefolded keys with [`SEQUENCE_START`] spelled literally.
/// Both the compiled corpus and the mutable builder implement it.
pub trait PromptSuccessorSource {
    /// `(mass, distinct)` of `word` after `context`.
    fn successor_stats(&self, context: &[&str], word: &str) -> (f64, u64);
    /// `(total_mass, total_distinct)` of `context`.
    fn context_totals(&self, context: &[&str]) -> (f64, u64);
    /// `(mass, distinct)` of `word` after `context` in one project
    /// partition.
    fn project_successor_stats(
        &self,
        project: &str,
        context: &[&str],
        word: &str,
    ) -> (f64, u64);
    /// `(total_mass, total_distinct)` of `context` in one project partition.
    fn project_context_totals(
        &self,
        project: &str,
        context: &[&str],
    ) -> (f64, u64);
    /// Successors at `context` ordered by mass descending, key ascending.
    fn ranked_successors(&self, context: &[&str]) -> Vec<(String, f64, u64)>;
}

/// Frozen, queryable n-gram corpus.
#[derive(Debug, Clone)]
pub struct CompiledPromptPredictionCorpus {
    inner: Arc<CorpusInner>,
}

#[derive(Debug)]
struct CorpusInner {
    keys: Vec<String>,
    surfaces: Vec<String>,
    ids: FastHashMap<String, u32>,
    contexts: FastHashMap<Vec<u32>, ContextStats>,
    projects: FastHashMap<String, FastHashMap<Vec<u32>, ContextStats>>,
    excluded: FastHashSet<String>,
    stats: CorpusStats,
}

#[derive(Debug, Clone, Default)]
struct CorpusStats {
    rows_used: u64,
    rows_generated_skipped: u64,
    rows_duplicate_skipped: u64,
    tokens: u64,
}

impl CompiledPromptPredictionCorpus {
    /// Compile rows into a frozen corpus.
    pub fn compile(
        rows: &[PromptPredictionRowWire],
        options: &PromptPredictionCorpusOptionsWire,
    ) -> Self {
        let mut builder = PromptPredictionBuilder::new(options.clone());
        // Exact-text duplicates count once, using the newest epoch: keep
        // the newest row per text. Generated rows never win a slot: filter
        // them before dedup so a generated copy cannot evict a typed one.
        let mut newest: FastHashMap<&str, &PromptPredictionRowWire> =
            fast_map();
        let mut generated = 0u64;
        for row in rows {
            if is_generated_row(row) {
                generated += 1;
                continue;
            }
            match newest.get(row.text.as_str()) {
                Some(existing)
                    if existing.epoch_seconds >= row.epoch_seconds =>
                {
                    // Older or equal duplicate loses.
                }
                _ => {
                    newest.insert(row.text.as_str(), row);
                }
            }
        }
        let duplicates =
            (rows.len() as u64).saturating_sub(newest.len() as u64 + generated);
        builder.rows_duplicate_skipped = duplicates;
        builder.rows_generated_skipped = generated;
        let mut kept: Vec<&PromptPredictionRowWire> =
            newest.values().copied().collect();
        kept.sort_by(|a, b| {
            a.epoch_seconds
                .cmp(&b.epoch_seconds)
                .then_with(|| a.text.cmp(&b.text))
        });
        for row in kept {
            builder.add_row(row);
        }
        builder.finish()
    }

    /// Compile statistics for the wire contract.
    pub fn stats(&self) -> PromptPredictionCorpusStatsWire {
        let inner = &self.inner;
        let contexts = inner.contexts.len() as u64;
        let successor_entries: u64 = inner
            .contexts
            .values()
            .map(|ctx| ctx.successors.len() as u64)
            .sum();
        PromptPredictionCorpusStatsWire {
            schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
            rows_used: inner.stats.rows_used,
            rows_generated_skipped: inner.stats.rows_generated_skipped,
            rows_duplicate_skipped: inner.stats.rows_duplicate_skipped,
            tokens: inner.stats.tokens,
            contexts,
            successor_entries,
            approx_bytes: self.approx_bytes(),
        }
    }

    /// Display surface for a casefolded key, if known.
    pub fn surface_for_key(&self, key: &str) -> Option<&str> {
        let id = self.inner.ids.get(key)?;
        Some(self.inner.surfaces[*id as usize].as_str())
    }

    /// True when the key must never be predicted (deletions store).
    pub fn is_excluded(&self, key: &str) -> bool {
        self.inner.excluded.contains(key)
    }

    /// Number of known word keys.
    pub fn vocab_len(&self) -> usize {
        self.inner.keys.len()
    }

    /// Resolve one word key to its id, if known.
    pub(crate) fn resolve_word(&self, key: &str) -> Option<u32> {
        self.inner.ids.get(key).copied()
    }

    /// Resolve context keys to word ids. `None` when any key is unknown:
    /// the context then has no observations.
    pub(crate) fn resolve_context(&self, keys: &[&str]) -> Option<Vec<u32>> {
        keys.iter().map(|key| self.resolve_word(key)).collect()
    }

    /// `(total_mass, total_distinct)` for resolved context ids.
    pub(crate) fn totals_for(&self, ids: &[u32]) -> (f64, u64) {
        self.inner
            .contexts
            .get(ids)
            .map(|stats| (stats.total_mass, stats.total_distinct))
            .unwrap_or((0.0, 0))
    }

    /// `(mass, distinct)` of one word after resolved context ids.
    pub(crate) fn word_stats_for(&self, ids: &[u32], word: u32) -> (f64, u64) {
        self.inner
            .contexts
            .get(ids)
            .and_then(|stats| stats.successors.get(&word))
            .map(|succ| (succ.mass, succ.distinct))
            .unwrap_or((0.0, 0))
    }

    /// `(total_mass, total_distinct)` of one project partition.
    pub(crate) fn project_totals_for(
        &self,
        project: &str,
        ids: &[u32],
    ) -> (f64, u64) {
        self.inner
            .projects
            .get(project)
            .and_then(|map| map.get(ids))
            .map(|stats| (stats.total_mass, stats.total_distinct))
            .unwrap_or((0.0, 0))
    }

    /// `(mass, distinct)` of one word in one project partition.
    pub(crate) fn project_word_stats_for(
        &self,
        project: &str,
        ids: &[u32],
        word: u32,
    ) -> (f64, u64) {
        self.inner
            .projects
            .get(project)
            .and_then(|map| map.get(ids))
            .and_then(|stats| stats.successors.get(&word))
            .map(|succ| (succ.mass, succ.distinct))
            .unwrap_or((0.0, 0))
    }

    /// Every known key starting with `prefix`.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.inner
            .keys
            .iter()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn approx_bytes(&self) -> u64 {
        let inner = &self.inner;
        let mut bytes: u64 = 0;
        for key in &inner.keys {
            bytes += key.len() as u64 + 16;
        }
        for surface in &inner.surfaces {
            bytes += surface.len() as u64 + 16;
        }
        for (context, stats) in &inner.contexts {
            bytes += context.len() as u64 * 4 + 64;
            bytes += stats.successors.len() as u64 * 32;
        }
        for (project, map) in &inner.projects {
            bytes += project.len() as u64 + 64;
            for (context, stats) in map {
                bytes += context.len() as u64 * 4 + 64;
                bytes += stats.successors.len() as u64 * 32;
            }
        }
        bytes
    }
}

impl PromptSuccessorSource for CompiledPromptPredictionCorpus {
    fn successor_stats(&self, context: &[&str], word: &str) -> (f64, u64) {
        let (Some(ctx), Some(id)) =
            (self.resolve_context(context), self.resolve_word(word))
        else {
            return (0.0, 0);
        };
        self.word_stats_for(&ctx, id)
    }

    fn context_totals(&self, context: &[&str]) -> (f64, u64) {
        let Some(ctx) = self.resolve_context(context) else {
            return (0.0, 0);
        };
        self.totals_for(&ctx)
    }

    fn project_successor_stats(
        &self,
        project: &str,
        context: &[&str],
        word: &str,
    ) -> (f64, u64) {
        let (Some(ctx), Some(id)) =
            (self.resolve_context(context), self.resolve_word(word))
        else {
            return (0.0, 0);
        };
        self.project_word_stats_for(project, &ctx, id)
    }

    fn project_context_totals(
        &self,
        project: &str,
        context: &[&str],
    ) -> (f64, u64) {
        let Some(ctx) = self.resolve_context(context) else {
            return (0.0, 0);
        };
        self.project_totals_for(project, &ctx)
    }

    fn ranked_successors(&self, context: &[&str]) -> Vec<(String, f64, u64)> {
        let Some(ctx) = self.resolve_context(context) else {
            return Vec::new();
        };
        let Some(stats) = self.inner.contexts.get(&ctx) else {
            return Vec::new();
        };
        let mut out: Vec<(String, f64, u64)> = stats
            .successors
            .iter()
            .map(|(id, succ)| {
                (
                    self.inner.keys[*id as usize].clone(),
                    succ.mass,
                    succ.distinct,
                )
            })
            .collect();
        out.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        out
    }
}

/// Compile entry point as free function over wire structs.
pub fn compile_prompt_prediction_corpus(
    rows: &[PromptPredictionRowWire],
    options: &PromptPredictionCorpusOptionsWire,
) -> CompiledPromptPredictionCorpus {
    CompiledPromptPredictionCorpus::compile(rows, options)
}

fn is_generated_row(row: &PromptPredictionRowWire) -> bool {
    match row.origin.as_deref() {
        Some("generated") => true,
        Some("typed") => false,
        Some(_) => false,
        None => looks_generated(&row.text),
    }
}

/// Recency weight for a row: `max(0.05, 0.5^(age_days / half_life))`.
pub fn recency_weight(
    epoch_seconds: i64,
    now_epoch: i64,
    half_life_days: f64,
) -> f64 {
    let half_life = if half_life_days > 0.0 {
        half_life_days
    } else {
        14.0
    };
    let age_days =
        ((now_epoch - epoch_seconds).max(0) as f64) / SECONDS_PER_DAY;
    (0.5f64.powf(age_days / half_life)).max(MIN_RECENCY_WEIGHT)
}

/// One interned word occurrence: id plus display surface and
/// sequence-initial flag.
struct InternedToken {
    id: u32,
    surface: String,
    initial: bool,
}

/// One interned sequence: `<s>`-started flag plus word occurrences.
type InternedSequence = (bool, Vec<InternedToken>);

/// Mutable corpus builder: same accumulation as [`CompiledPromptPredictionCorpus::compile`],
/// queryable between row additions without a recompile.
#[derive(Debug)]
pub struct PromptPredictionBuilder {
    options: PromptPredictionCorpusOptionsWire,
    keys: Vec<String>,
    ids: FastHashMap<String, u32>,
    votes: FastHashMap<u32, Vec<(String, f64, bool)>>,
    contexts: FastHashMap<Vec<u32>, ContextStats>,
    projects: FastHashMap<String, FastHashMap<Vec<u32>, ContextStats>>,
    seen_texts: FastHashSet<String>,
    rows_used: u64,
    rows_generated_skipped: u64,
    rows_duplicate_skipped: u64,
    tokens: u64,
}

impl PromptPredictionBuilder {
    /// Start a builder with compile options.
    pub fn new(options: PromptPredictionCorpusOptionsWire) -> Self {
        let mut builder = Self {
            options,
            keys: Vec::new(),
            ids: fast_map(),
            votes: fast_map(),
            contexts: fast_map(),
            projects: fast_map(),
            seen_texts: fast_set(),
            rows_used: 0,
            rows_generated_skipped: 0,
            rows_duplicate_skipped: 0,
            tokens: 0,
        };
        builder.intern(SEQUENCE_START);
        builder
    }

    /// Add one row. Returns false when the row is skipped (generated,
    /// duplicate text, or empty of words).
    pub fn add_row(&mut self, row: &PromptPredictionRowWire) -> bool {
        if is_generated_row(row) {
            self.rows_generated_skipped += 1;
            return false;
        }
        if !self.seen_texts.insert(row.text.clone()) {
            self.rows_duplicate_skipped += 1;
            return false;
        }
        let sequences = tokenize_prompt_text(&row.text);
        let word_count: usize =
            sequences.iter().map(|seq| seq.tokens.len()).sum();
        if word_count == 0 {
            return false;
        }
        let weight = recency_weight(
            row.epoch_seconds,
            self.options.now_epoch,
            self.options.recency_half_life_days,
        );
        let max_context = self.options.max_context_words.max(1);
        // Single intern pass: word ids per sequence plus surface votes by
        // id (one vote per key per row, preferring non-initial casing).
        let mut interned: Vec<InternedSequence> =
            Vec::with_capacity(sequences.len());
        let mut repeated_word = false;
        let mut row_ids: FastHashSet<u32> = fast_set();
        for seq in &sequences {
            let mut words = Vec::with_capacity(seq.tokens.len());
            for token in &seq.tokens {
                let id = self.intern(&token.key);
                if !row_ids.insert(id) {
                    repeated_word = true;
                }
                words.push(InternedToken {
                    id,
                    surface: token.surface.clone(),
                    initial: token.sequence_initial,
                });
            }
            interned.push((seq.started, words));
        }
        {
            let mut row_surfaces: FastHashMap<u32, (String, bool)> = fast_map();
            for (_, words) in &interned {
                for token in words {
                    row_surfaces
                        .entry(token.id)
                        .and_modify(|entry| {
                            if !token.initial {
                                *entry = (token.surface.clone(), false);
                            }
                        })
                        .or_insert_with(|| {
                            (token.surface.clone(), token.initial)
                        });
                }
            }
            for (id, (surface, initial)) in row_surfaces {
                self.votes
                    .entry(id)
                    .or_default()
                    .push((surface, weight, initial));
            }
        }
        // Per-row (context, word) dedup: a repeated n-gram in one row
        // counts once. Rows without a repeated word skip the set: every
        // pair is trivially unique.
        let start_id = self.intern(SEQUENCE_START);
        let project_name = row.project.clone();
        let mut seen_pairs: FastHashSet<(Vec<u32>, u32)> = fast_set();
        let mut seen: Option<&mut FastHashSet<(Vec<u32>, u32)>> =
            repeated_word.then_some(&mut seen_pairs);
        // Disjoint field borrows: counting touches only these maps.
        let Self {
            contexts, projects, ..
        } = &mut *self;
        // Project partition map resolved once per row.
        let mut project_map = project_name.as_deref().map(|name| {
            projects.entry(name.to_string()).or_default();
            projects.get_mut(name).expect("inserted")
        });
        for (started, words) in &interned {
            let mut preceding: Vec<u32> = Vec::with_capacity(max_context + 1);
            if *started {
                preceding.push(start_id);
            }
            for token in words {
                let depth = preceding.len().min(max_context);
                for order in 0..=depth {
                    let context: Vec<u32> =
                        preceding[preceding.len() - order..].to_vec();
                    // Without repeats every pair is new; otherwise check
                    // the set before counting.
                    let fresh = match seen.as_mut() {
                        Some(set) => set.insert((context.clone(), token.id)),
                        None => true,
                    };
                    if fresh {
                        record_pair(
                            contexts,
                            project_map.as_deref_mut(),
                            context,
                            token.id,
                            weight,
                        );
                    }
                }
                preceding.push(token.id);
            }
        }
        self.tokens += word_count as u64;
        self.rows_used += 1;
        true
    }

    fn intern(&mut self, key: &str) -> u32 {
        if let Some(id) = self.ids.get(key) {
            return *id;
        }
        let id = self.keys.len() as u32;
        self.keys.push(key.to_string());
        self.ids.insert(key.to_string(), id);
        id
    }

    /// Freeze the builder: truncate successors, prune singletons, and
    /// canonicalize surfaces.
    pub fn finish(mut self) -> CompiledPromptPredictionCorpus {
        let max_successors = self.options.max_successors_per_context.max(1);
        truncate_contexts(&mut self.contexts, &self.keys, max_successors);
        for map in self.projects.values_mut() {
            truncate_contexts(map, &self.keys, max_successors);
        }
        if self.options.prune_singleton_contexts {
            self.contexts
                .retain(|ctx, stats| ctx.len() < 2 || stats.total_distinct > 1);
            for map in self.projects.values_mut() {
                map.retain(|ctx, stats| {
                    ctx.len() < 2 || stats.total_distinct > 1
                });
            }
        }
        let excluded: FastHashSet<String> = self
            .options
            .excluded_words
            .iter()
            .map(|word| word.to_lowercase().replace('’', "'"))
            .collect();
        // Excluded words are never predicted: drop them from successors.
        // Totals stay pre-truncation, matching the truncation contract.
        for stats in self.contexts.values_mut() {
            stats.successors.retain(|id, _| {
                !excluded.contains(self.keys[*id as usize].as_str())
            });
        }
        for map in self.projects.values_mut() {
            for stats in map.values_mut() {
                stats.successors.retain(|id, _| {
                    !excluded.contains(self.keys[*id as usize].as_str())
                });
            }
        }
        let mut surfaces = vec![String::new(); self.keys.len()];
        for (index, key) in self.keys.iter().enumerate() {
            let id = index as u32;
            let empty = Vec::new();
            let votes = self.votes.get(&id).unwrap_or(&empty);
            surfaces[index] = if votes.is_empty() {
                key.clone()
            } else {
                canonical_surface(key, votes)
            };
        }
        // `<s>` keeps its literal spelling.
        if let Some(id) = self.ids.get(SEQUENCE_START).copied() {
            surfaces[id as usize] = SEQUENCE_START.to_string();
        }
        CompiledPromptPredictionCorpus {
            inner: Arc::new(CorpusInner {
                keys: self.keys,
                surfaces,
                ids: self.ids,
                contexts: self.contexts,
                projects: self.projects,
                excluded,
                stats: CorpusStats {
                    rows_used: self.rows_used,
                    rows_generated_skipped: self.rows_generated_skipped,
                    rows_duplicate_skipped: self.rows_duplicate_skipped,
                    tokens: self.tokens,
                },
            }),
        }
    }
}

/// Count one `(context, word)` pair into the main table and, when
/// present, one project partition.
fn record_pair(
    contexts: &mut FastHashMap<Vec<u32>, ContextStats>,
    project_map: Option<&mut FastHashMap<Vec<u32>, ContextStats>>,
    context: Vec<u32>,
    word_id: u32,
    weight: f64,
) {
    let stats = contexts.entry(context.clone()).or_default();
    stats.total_mass += weight;
    stats.total_distinct += 1;
    let succ = stats.successors.entry(word_id).or_default();
    succ.mass += weight;
    succ.distinct += 1;
    if let Some(map) = project_map {
        let stats = map.entry(context).or_default();
        stats.total_mass += weight;
        stats.total_distinct += 1;
        let succ = stats.successors.entry(word_id).or_default();
        succ.mass += weight;
        succ.distinct += 1;
    }
}

fn truncate_contexts(
    contexts: &mut FastHashMap<Vec<u32>, ContextStats>,
    keys: &[String],
    max_successors: usize,
) {
    for stats in contexts.values_mut() {
        if stats.successors.len() <= max_successors {
            continue;
        }
        let mut ranked: Vec<(u32, f64)> = stats
            .successors
            .iter()
            .map(|(id, succ)| (*id, succ.mass))
            .collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| keys[a.0 as usize].cmp(&keys[b.0 as usize]))
        });
        let keep: FastHashSet<u32> = ranked
            .into_iter()
            .take(max_successors)
            .map(|(id, _)| id)
            .collect();
        stats.successors.retain(|id, _| keep.contains(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(now: i64) -> PromptPredictionCorpusOptionsWire {
        PromptPredictionCorpusOptionsWire {
            now_epoch: now,
            ..Default::default()
        }
    }

    fn row(text: &str, epoch: i64) -> PromptPredictionRowWire {
        PromptPredictionRowWire {
            text: text.to_string(),
            epoch_seconds: epoch,
            project: None,
            origin: Some("typed".to_string()),
            cancelled: false,
        }
    }

    #[test]
    fn mass_counts_once_per_row_but_distinct_counts_rows() {
        let rows = vec![
            row("help me implement it", 100),
            row("help me implement it now", 200),
            row("help me review it", 300),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &options(400));
        let (mass, distinct) =
            corpus.successor_stats(&["help", "me"], "implement");
        assert_eq!(distinct, 2, "two rows observe implement");
        assert!(mass > 0.0);
        let stats = corpus.stats();
        assert_eq!(stats.rows_used, 3);
        assert!(stats.contexts > 0);
        assert!(stats.successor_entries > 0);
    }

    #[test]
    fn recency_decay_weights_newer_rows_more() {
        let now = 1_000_000;
        let rows = vec![
            row("help me implement it", now - 365 * 86_400),
            row("help me review it", now),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &options(now));
        let (old_mass, _) =
            corpus.successor_stats(&["help", "me"], "implement");
        let (new_mass, _) = corpus.successor_stats(&["help", "me"], "review");
        assert!(new_mass > old_mass, "new={new_mass} old={old_mass}");
    }

    #[test]
    fn duplicate_text_counts_once_with_newest_epoch() {
        let rows = vec![
            row("help me implement it", 100),
            row("help me implement it", 200),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &options(300));
        let (_, distinct) =
            corpus.successor_stats(&["help", "me"], "implement");
        assert_eq!(distinct, 1);
        assert_eq!(corpus.stats().rows_duplicate_skipped, 1);
    }

    #[test]
    fn generated_rows_are_dropped() {
        let rows = vec![
            PromptPredictionRowWire {
                text: "help me implement it".to_string(),
                epoch_seconds: 100,
                project: None,
                origin: Some("generated".to_string()),
                cancelled: false,
            },
            row("help me review it", 200),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &options(300));
        assert_eq!(
            corpus.successor_stats(&["help", "me"], "implement"),
            (0.0, 0)
        );
        assert_eq!(corpus.stats().rows_generated_skipped, 1);
    }

    #[test]
    fn legacy_heuristic_drops_generated_rows_without_origin() {
        let rows = vec![
            PromptPredictionRowWire {
                text: "work it #bd/work_phase_bead now".to_string(),
                epoch_seconds: 100,
                project: None,
                origin: None,
                cancelled: false,
            },
            row("help me review it", 200),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &options(300));
        assert_eq!(corpus.stats().rows_generated_skipped, 1);
        assert_eq!(corpus.stats().rows_used, 1);
    }

    #[test]
    fn truncation_keeps_totals_but_limits_successors() {
        let mut opts = options(1_000);
        opts.max_successors_per_context = 2;
        let rows = vec![
            row("help me alpha", 100),
            row("help me beta", 200),
            row("help me gamma", 300),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &opts);
        let (mass, distinct) = corpus.context_totals(&["help", "me"]);
        assert_eq!(distinct, 3, "totals precede truncation");
        assert!(mass > 0.0);
        assert_eq!(corpus.ranked_successors(&["help", "me"]).len(), 2);
    }

    #[test]
    fn project_partitions_carry_same_project_mass() {
        let rows = vec![
            PromptPredictionRowWire {
                text: "help me implement it".to_string(),
                epoch_seconds: 100,
                project: Some("sase".to_string()),
                origin: Some("typed".to_string()),
                cancelled: false,
            },
            row("help me review it", 200),
        ];
        let corpus = compile_prompt_prediction_corpus(&rows, &options(300));
        let (mass, distinct) = corpus.project_successor_stats(
            "sase",
            &["help", "me"],
            "implement",
        );
        assert_eq!(distinct, 1);
        assert!(mass > 0.0);
        assert_eq!(
            corpus.project_successor_stats(
                "other",
                &["help", "me"],
                "implement"
            ),
            (0.0, 0)
        );
    }

    #[test]
    fn prune_drops_singleton_long_contexts() {
        let mut opts = options(1_000);
        opts.prune_singleton_contexts = true;
        let rows = vec![row("alpha beta gamma delta", 100)];
        let corpus = compile_prompt_prediction_corpus(&rows, &opts);
        assert_eq!(
            corpus.context_totals(&["alpha", "beta", "gamma"]),
            (0.0, 0)
        );
        // Short contexts survive.
        assert!(corpus.context_totals(&["alpha"]).0 > 0.0);
    }

    #[test]
    fn excluded_words_are_never_predicted() {
        let mut opts = options(1_000);
        opts.excluded_words = vec!["implement".to_string()];
        let rows = vec![row("help me implement it", 100)];
        let corpus = compile_prompt_prediction_corpus(&rows, &opts);
        assert!(corpus.is_excluded("implement"));
        assert_eq!(
            corpus.successor_stats(&["help", "me"], "implement"),
            (0.0, 0)
        );
        assert!(corpus.ranked_successors(&["help", "me"]).is_empty());
    }

    #[test]
    fn builder_matches_compile() {
        let opts = options(1_000);
        let rows = vec![
            row("help me implement it", 100),
            row("help me review it", 200),
        ];
        let compiled = compile_prompt_prediction_corpus(&rows, &opts);
        let mut builder = PromptPredictionBuilder::new(opts);
        for row in &rows {
            assert!(builder.add_row(row));
        }
        assert!(!builder.add_row(&rows[0]));
        let frozen = builder.finish();
        assert_eq!(
            compiled.successor_stats(&["help", "me"], "implement"),
            frozen.successor_stats(&["help", "me"], "implement")
        );
    }

    #[test]
    fn recency_floor_and_freshness() {
        assert!((recency_weight(0, 0, 14.0) - 1.0).abs() < 1e-9);
        assert!((recency_weight(0, 14 * 86_400, 14.0) - 0.5).abs() < 1e-9);
        assert_eq!(recency_weight(0, 365 * 10 * 86_400, 14.0), 0.05);
    }
}
