//! Query evaluation engine and batch APIs.
//!
//! Mirrors `src/sase/ace/query/context.py`. The Rust shape:
//!
//! - [`compile_query`] turns a query string into a [`QueryProgram`] (parsed
//!   AST plus the original source).
//! - [`QueryCorpus`] owns a Patch list plus reusable per-corpus derived
//!   data such as parent lookup, base statuses, project names, sibling bases,
//!   and searchable text.
//! - [`QueryEvaluationContext`] holds query-specific state. Today that is
//!   only ancestor memoization, which must not be shared across unrelated
//!   query evaluations.
//! - [`evaluate_query_many_in_corpus`] evaluates a compiled program against a
//!   persistent corpus. [`evaluate_query_many`] preserves the older API by
//!   constructing a temporary corpus.
//! - [`evaluate_query_one`] is a convenience for tests.
//!
//! Substring matching is case-insensitive via lowercasing the searchable
//! text — never via regex.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::query::flat::{parse_bool_literal, parse_int_literal};
use crate::query::matchers::{get_base_status, strip_reverted_suffix};
use crate::query::parser::parse_query_with_profile;
use crate::query::profile::{
    patch_query_profile, CompiledQueryProfile, FieldValueKind,
};
use crate::query::row::{patch_rows_from_specs, IndexedQueryRow, QueryRow};
use crate::query::searchable::effective_project_name;
use crate::query::types::{QueryErrorWire, QueryExprWire};
use crate::wire::ChangeSpecWire;

/// A compiled query — the parsed AST plus the original source for diagnostics.
///
/// Mirrors the JSON shape of `QueryProgramWire` (defined in `types.rs`) but
/// is the in-memory handle the evaluator uses. Callers that need a
/// serializable handle should hold `QueryProgramWire` and convert with
/// [`QueryProgram::from_wire`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryProgram {
    pub source: String,
    pub expr: QueryExprWire,
    #[serde(default)]
    pub profile_digest: String,
}

impl QueryProgram {
    pub fn new(source: impl Into<String>, expr: QueryExprWire) -> Self {
        Self {
            source: source.into(),
            expr,
            profile_digest: patch_query_profile().digest.clone(),
        }
    }

    pub fn from_wire(wire: crate::query::types::QueryProgramWire) -> Self {
        Self {
            source: wire.source,
            expr: wire.expr,
            profile_digest: if wire.profile_digest.is_empty() {
                patch_query_profile().digest.clone()
            } else {
                wire.profile_digest
            },
        }
    }

    pub fn into_wire(self) -> crate::query::types::QueryProgramWire {
        crate::query::types::QueryProgramWire {
            source: self.source,
            expr: self.expr,
            profile_digest: self.profile_digest,
        }
    }

    fn digest(&self) -> &str {
        if self.profile_digest.is_empty() {
            patch_query_profile().digest.as_str()
        } else {
            self.profile_digest.as_str()
        }
    }
}

/// Compile a query string into a [`QueryProgram`].
///
/// Tokenize + parse, mirroring `parse_query_python`. The AST is folded so
/// `!!`/`!@`/`!$` and `*` produce the same shape Python does, and the
/// resulting program can be reused across many `evaluate_*` calls without
/// re-parsing.
pub fn compile_query(query: &str) -> Result<QueryProgram, QueryErrorWire> {
    compile_query_with_profile(query, patch_query_profile())
}

/// Compile `query` against a compiled profile.
pub fn compile_query_with_profile(
    query: &str,
    profile: &CompiledQueryProfile,
) -> Result<QueryProgram, QueryErrorWire> {
    let expr = parse_query_with_profile(query, profile)?;
    Ok(QueryProgram {
        source: query.to_string(),
        expr,
        profile_digest: profile.digest.clone(),
    })
}

/// Persistent Patch corpus used across many query evaluations.
///
/// This owns the wire specs and the expensive or repeated derived values that
/// depend only on the corpus, not on a particular query. A single
/// [`QueryCorpus`] can safely be reused for different compiled programs
/// because query-specific mutable state lives in [`QueryEvaluationContext`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryCorpus {
    pub specs: Vec<ChangeSpecWire>,
    pub name_map: HashMap<String, usize>,
    pub lower_names: Vec<String>,
    pub base_statuses: Vec<String>,
    pub project_names: Vec<String>,
    pub sibling_bases: Vec<String>,
    pub searchable_text: Vec<String>,
    pub searchable_lower: Vec<String>,
    pub profile_digest: String,
    indexed: Vec<IndexedQueryRow>,
}

impl QueryCorpus {
    pub fn new(specs: Vec<ChangeSpecWire>) -> Self {
        Self::from_patches(specs)
    }

    /// Build a Patch-compatible corpus from `ChangeSpecWire` records.
    pub fn from_patches(specs: Vec<ChangeSpecWire>) -> Self {
        let profile = patch_query_profile();
        let rows = patch_rows_from_specs(&specs);
        Self::from_parts(profile, rows, specs)
    }

    /// Build a generic corpus from a compiled profile and precomputed rows.
    pub fn from_rows(
        profile: &CompiledQueryProfile,
        rows: Vec<QueryRow>,
    ) -> Self {
        Self::from_parts(profile, rows, Vec::new())
    }

    fn from_parts(
        profile: &CompiledQueryProfile,
        rows: Vec<QueryRow>,
        specs: Vec<ChangeSpecWire>,
    ) -> Self {
        let mut name_map = HashMap::with_capacity(specs.len());
        let mut lower_names = Vec::with_capacity(specs.len());
        let mut base_statuses = Vec::with_capacity(specs.len());
        let mut project_names = Vec::with_capacity(specs.len());
        let mut sibling_bases = Vec::with_capacity(specs.len());
        for (idx, cs) in specs.iter().enumerate() {
            let lower_name = cs.name.to_lowercase();
            name_map.insert(lower_name.clone(), idx);
            lower_names.push(lower_name);
            base_statuses.push(get_base_status(&cs.status));
            project_names.push(effective_project_name(cs).to_string());
            sibling_bases.push(strip_reverted_suffix(&cs.name).to_lowercase());
        }

        let mut searchable_text = Vec::with_capacity(rows.len());
        let mut searchable_lower = Vec::with_capacity(rows.len());
        let mut indexed = Vec::with_capacity(rows.len());
        for row in rows {
            let indexed_row = IndexedQueryRow::index(row, profile);
            searchable_text.push(indexed_row.searchable_text.clone());
            searchable_lower.push(indexed_row.searchable_lower.clone());
            indexed.push(indexed_row);
        }

        Self {
            specs,
            name_map,
            lower_names,
            base_statuses,
            project_names,
            sibling_bases,
            searchable_text,
            searchable_lower,
            profile_digest: profile.digest.clone(),
            indexed,
        }
    }

    pub fn len(&self) -> usize {
        self.indexed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indexed.is_empty()
    }

    fn digest(&self) -> &str {
        if self.profile_digest.is_empty() {
            patch_query_profile().digest.as_str()
        } else {
            self.profile_digest.as_str()
        }
    }
}

/// Query-evaluation scratch state.
///
/// - `ancestor_memo` keys on `(name_lower, ancestor_value_lower)` to avoid
///   re-walking parent chains.
#[derive(Debug, Clone, Default)]
pub struct QueryEvaluationContext {
    pub ancestor_memo: HashMap<(String, String), bool>,
}

impl QueryEvaluationContext {
    /// Build fresh query-evaluation scratch state.
    ///
    /// The `specs` argument is retained for source compatibility with older
    /// callers that built the context directly. Per-corpus data now lives in
    /// [`QueryCorpus`].
    pub fn build(specs: &[ChangeSpecWire]) -> Self {
        let _ = specs;
        Self {
            ancestor_memo: HashMap::new(),
        }
    }

    fn match_string(
        &self,
        corpus: &QueryCorpus,
        idx: usize,
        value: &str,
        case_sensitive: bool,
    ) -> bool {
        if case_sensitive {
            corpus.indexed[idx].searchable_text.contains(value)
        } else {
            let needle = value.to_lowercase();
            corpus.indexed[idx].searchable_lower.contains(&needle)
        }
    }

    fn match_property(
        &self,
        corpus: &QueryCorpus,
        idx: usize,
        key: &str,
        value: &str,
    ) -> bool {
        let row = &corpus.indexed[idx];
        let Some(field) = row.fields.get(&key.to_ascii_lowercase()) else {
            return false;
        };
        let query_value = if key.eq_ignore_ascii_case("sibling") {
            strip_reverted_suffix(value)
        } else {
            value.to_string()
        };
        match field.kind {
            FieldValueKind::Bool => {
                let Some(wanted) = parse_bool_literal(&query_value) else {
                    return false;
                };
                field
                    .values
                    .iter()
                    .any(|item| parse_bool_literal(item) == Some(wanted))
            }
            FieldValueKind::Int => {
                let Some(wanted) = parse_int_literal(&query_value) else {
                    return false;
                };
                field
                    .values
                    .iter()
                    .any(|item| parse_int_literal(item) == Some(wanted))
            }
            FieldValueKind::String
            | FieldValueKind::Enum
            | FieldValueKind::Date => {
                let wanted = query_value.to_ascii_lowercase();
                field.values_lower.iter().any(|item| item == &wanted)
            }
        }
    }

    fn evaluate(
        &self,
        corpus: &QueryCorpus,
        idx: usize,
        expr: &QueryExprWire,
    ) -> bool {
        let row = &corpus.indexed[idx];
        match expr {
            QueryExprWire::StringMatch {
                value,
                case_sensitive,
                is_error_suffix,
                is_running_agent,
                is_running_process,
            } => {
                if *is_error_suffix {
                    return row.predicates.error_suffix;
                }
                if *is_running_agent {
                    return row.predicates.running_agent;
                }
                if *is_running_process {
                    return row.predicates.running_process;
                }
                self.match_string(corpus, idx, value, *case_sensitive)
            }
            QueryExprWire::PropertyMatch { key, value } => {
                self.match_property(corpus, idx, key, value)
            }
            QueryExprWire::Not { operand } => {
                !self.evaluate(corpus, idx, operand)
            }
            QueryExprWire::And { operands } => {
                operands.iter().all(|op| self.evaluate(corpus, idx, op))
            }
            QueryExprWire::Or { operands } => {
                operands.iter().any(|op| self.evaluate(corpus, idx, op))
            }
        }
    }
}

/// Evaluate a compiled query against every spec in a persistent corpus,
/// returning one boolean per corpus row.
///
/// Compatibility entry point: Patch-compatible handles share a digest and
/// cannot mismatch. Prefer [`try_evaluate_query_many_in_corpus`] when the
/// program and corpus may come from different profiles.
pub fn evaluate_query_many_in_corpus(
    program: &QueryProgram,
    corpus: &QueryCorpus,
) -> Vec<bool> {
    try_evaluate_query_many_in_corpus(program, corpus)
        .expect("query program and corpus profile digest must match")
}

/// Evaluate a compiled query against a persistent corpus, rejecting
/// program/corpus profile-digest mismatches.
pub fn try_evaluate_query_many_in_corpus(
    program: &QueryProgram,
    corpus: &QueryCorpus,
) -> Result<Vec<bool>, QueryErrorWire> {
    if program.digest() != corpus.digest() {
        return Err(QueryErrorWire::profile(
            "query program profile digest does not match corpus",
        ));
    }
    let ctx = QueryEvaluationContext::default();
    Ok((0..corpus.len())
        .map(|idx| ctx.evaluate(corpus, idx, &program.expr))
        .collect())
}

/// Evaluate a compiled query against every spec in `specs`, returning a
/// boolean per spec. Compatibility API for callers that do not yet own a
/// persistent [`QueryCorpus`].
pub fn evaluate_query_many(
    program: &QueryProgram,
    specs: &[ChangeSpecWire],
) -> Vec<bool> {
    let corpus = QueryCorpus::new(specs.to_vec());
    evaluate_query_many_in_corpus(program, &corpus)
}

/// Evaluate a compiled query against one spec inside an existing list.
/// Useful for parity tests; production callers should prefer
/// [`evaluate_query_many`].
pub fn evaluate_query_one(
    program: &QueryProgram,
    cs: &ChangeSpecWire,
    all_specs: &[ChangeSpecWire],
) -> bool {
    let idx = all_specs
        .iter()
        .position(|candidate| std::ptr::eq(candidate, cs))
        .or_else(|| {
            all_specs
                .iter()
                .position(|candidate| candidate.name == cs.name)
        });
    let corpus = QueryCorpus::new(all_specs.to_vec());
    match idx {
        Some(idx) => {
            let ctx = QueryEvaluationContext::default();
            ctx.evaluate(&corpus, idx, &program.expr)
        }
        None => false,
    }
}
