//! Query evaluation engine and batch APIs.
//!
//! Mirrors `src/sase/ace/query/context.py`. The Rust shape:
//!
//! - [`compile_query`] turns a query string into a [`QueryProgram`] (parsed
//!   AST plus the original source).
//! - [`QueryCorpus`] owns a ChangeSpec list plus reusable per-corpus derived
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

use crate::query::matchers::{
    get_base_status, has_any_status_suffix, match_name, strip_reverted_suffix,
};
use crate::query::parser::parse_query;
use crate::query::searchable::{
    get_searchable_text, project_dir_name, RUNNING_AGENT_MARKER,
    RUNNING_PROCESS_MARKER,
};
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
}

impl QueryProgram {
    pub fn new(source: impl Into<String>, expr: QueryExprWire) -> Self {
        Self {
            source: source.into(),
            expr,
        }
    }

    pub fn from_wire(wire: crate::query::types::QueryProgramWire) -> Self {
        Self {
            source: wire.source,
            expr: wire.expr,
        }
    }

    pub fn into_wire(self) -> crate::query::types::QueryProgramWire {
        crate::query::types::QueryProgramWire {
            source: self.source,
            expr: self.expr,
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
    let expr = parse_query(query)?;
    Ok(QueryProgram::new(query, expr))
}

/// Persistent ChangeSpec corpus used across many query evaluations.
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
}

impl QueryCorpus {
    pub fn new(specs: Vec<ChangeSpecWire>) -> Self {
        let mut name_map = HashMap::with_capacity(specs.len());
        let mut lower_names = Vec::with_capacity(specs.len());
        let mut base_statuses = Vec::with_capacity(specs.len());
        let mut project_names = Vec::with_capacity(specs.len());
        let mut sibling_bases = Vec::with_capacity(specs.len());
        let mut searchable_text = Vec::with_capacity(specs.len());
        let mut searchable_lower = Vec::with_capacity(specs.len());

        for (idx, cs) in specs.iter().enumerate() {
            let lower_name = cs.name.to_lowercase();
            name_map.insert(lower_name.clone(), idx);
            lower_names.push(lower_name);
            base_statuses.push(get_base_status(&cs.status));
            project_names.push(project_dir_name(&cs.file_path).to_string());
            sibling_bases.push(strip_reverted_suffix(&cs.name).to_lowercase());

            let text = get_searchable_text(cs);
            searchable_lower.push(text.to_lowercase());
            searchable_text.push(text);
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
        }
    }

    pub fn len(&self) -> usize {
        self.specs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
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
        &mut self,
        corpus: &QueryCorpus,
        idx: usize,
        value: &str,
        case_sensitive: bool,
    ) -> bool {
        if case_sensitive {
            corpus.searchable_text[idx].contains(value)
        } else {
            let needle = value.to_lowercase();
            corpus.searchable_lower[idx].contains(&needle)
        }
    }

    /// Recursive ancestor matcher with memoization across the parent chain.
    ///
    /// Walks `cs` and its parents; on success memoizes every node visited so
    /// subsequent calls hit the cache. Mirrors the Python implementation,
    /// including the cycle guard (a parent that already appears in `visited`
    /// terminates the walk without matching).
    fn match_ancestor(
        &mut self,
        corpus: &QueryCorpus,
        idx: usize,
        ancestor_value: &str,
    ) -> bool {
        let ancestor_value_lower = ancestor_value.to_lowercase();
        let cache_key = (
            corpus.lower_names[idx].clone(),
            ancestor_value_lower.clone(),
        );
        if let Some(cached) = self.ancestor_memo.get(&cache_key) {
            return *cached;
        }

        let mut visited: Vec<String> = Vec::new();
        let mut current_name_lower = corpus.lower_names[idx].clone();
        let mut current_parent: Option<String> =
            corpus.specs[idx].parent.clone();
        let mut found = false;

        loop {
            if visited.iter().any(|v| v == &current_name_lower) {
                break;
            }
            visited.push(current_name_lower.clone());

            let memo_key =
                (current_name_lower.clone(), ancestor_value_lower.clone());
            if let Some(prior) = self.ancestor_memo.get(&memo_key) {
                found = *prior;
                break;
            }

            if current_name_lower == ancestor_value_lower {
                found = true;
                break;
            }

            match current_parent.as_deref() {
                Some(parent) if !parent.is_empty() => {
                    let parent_lower = parent.to_lowercase();
                    if parent_lower == ancestor_value_lower {
                        found = true;
                        break;
                    }
                    match corpus.name_map.get(&parent_lower).copied() {
                        Some(idx) => {
                            current_name_lower =
                                corpus.lower_names[idx].clone();
                            current_parent = corpus.specs[idx].parent.clone();
                        }
                        None => break,
                    }
                }
                _ => break,
            }
        }

        for v in visited {
            self.ancestor_memo
                .entry((v, ancestor_value_lower.clone()))
                .or_insert(found);
        }
        found
    }

    fn match_property(
        &mut self,
        corpus: &QueryCorpus,
        idx: usize,
        key: &str,
        value: &str,
    ) -> bool {
        match key {
            "status" => corpus.base_statuses[idx].eq_ignore_ascii_case(value),
            "project" => corpus.project_names[idx].eq_ignore_ascii_case(value),
            "ancestor" => self.match_ancestor(corpus, idx, value),
            "name" => match_name(value, &corpus.specs[idx]),
            "sibling" => {
                let search_base = strip_reverted_suffix(value).to_lowercase();
                corpus.sibling_bases[idx] == search_base
            }
            // Unknown property keys never match (parser rejects unknown keys
            // up front, so this is just defensive).
            _ => false,
        }
    }

    fn evaluate(
        &mut self,
        corpus: &QueryCorpus,
        idx: usize,
        expr: &QueryExprWire,
    ) -> bool {
        let cs = &corpus.specs[idx];
        match expr {
            QueryExprWire::StringMatch {
                value,
                case_sensitive,
                is_error_suffix,
                is_running_agent,
                is_running_process,
            } => {
                if *is_error_suffix {
                    return has_any_status_suffix(cs);
                }
                if *is_running_agent {
                    return corpus.searchable_text[idx]
                        .contains(RUNNING_AGENT_MARKER);
                }
                if *is_running_process {
                    return corpus.searchable_text[idx]
                        .contains(RUNNING_PROCESS_MARKER);
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
pub fn evaluate_query_many_in_corpus(
    program: &QueryProgram,
    corpus: &QueryCorpus,
) -> Vec<bool> {
    let mut ctx = QueryEvaluationContext::default();
    (0..corpus.len())
        .map(|idx| ctx.evaluate(corpus, idx, &program.expr))
        .collect()
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
            let mut ctx = QueryEvaluationContext::default();
            ctx.evaluate(&corpus, idx, &program.expr)
        }
        None => false,
    }
}
