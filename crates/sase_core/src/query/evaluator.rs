//! Query evaluation engine and batch APIs.
//!
//! Mirrors `src/sase/ace/query/context.py`. The Rust shape:
//!
//! - [`compile_query`] turns a query string into a [`QueryProgram`] (parsed
//!   AST plus the original source).
//! - [`QueryEvaluationContext`] holds a per-list cache keyed by lowercased
//!   ChangeSpec name (name map, status map, searchable text/lowercase, and
//!   ancestor memo).
//! - [`evaluate_query_many`] is the primary public API: build the context
//!   once, evaluate the program against every spec.
//! - [`evaluate_query_one`] is a convenience for tests.
//!
//! Substring matching is case-insensitive via lowercasing the searchable
//! text — never via regex.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::query::matchers::{
    get_base_status, has_any_status_suffix, match_name, match_project,
    match_sibling, match_status,
};
use crate::query::parser::parse_query;
use crate::query::searchable::{
    get_searchable_text, RUNNING_AGENT_MARKER, RUNNING_PROCESS_MARKER,
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

/// Per-ChangeSpec-list cache used during query evaluation.
///
/// Mirrors `QueryEvaluationContext` in Python:
///
/// - `name_map` maps lowercased name → index into `all_specs` so ancestor
///   walks resolve parents in O(1).
/// - `status_map` caches the base status per spec.
/// - `searchable_text` / `searchable_lower` are lazy: they're populated the
///   first time a string match touches a spec, so unfiltered batches don't
///   pay the cost.
/// - `ancestor_memo` keys on `(name_lower, ancestor_value_lower)` to avoid
///   re-walking parent chains.
#[derive(Debug, Clone, Default)]
pub struct QueryEvaluationContext {
    pub name_map: HashMap<String, usize>,
    pub status_map: HashMap<String, String>,
    pub searchable_text: HashMap<String, String>,
    pub searchable_lower: HashMap<String, String>,
    pub ancestor_memo: HashMap<(String, String), bool>,
}

impl QueryEvaluationContext {
    /// Build a context from a ChangeSpec list.
    ///
    /// Eagerly fills `name_map` and `status_map` (cheap, used by every row).
    /// Searchable text is populated lazily during evaluation.
    pub fn build(specs: &[ChangeSpecWire]) -> Self {
        let mut name_map = HashMap::with_capacity(specs.len());
        let mut status_map = HashMap::with_capacity(specs.len());
        for (idx, cs) in specs.iter().enumerate() {
            let key = cs.name.to_lowercase();
            name_map.insert(key.clone(), idx);
            status_map.insert(key, get_base_status(&cs.status));
        }
        Self {
            name_map,
            status_map,
            searchable_text: HashMap::new(),
            searchable_lower: HashMap::new(),
            ancestor_memo: HashMap::new(),
        }
    }

    fn searchable_text<'a>(&'a mut self, cs: &ChangeSpecWire) -> &'a str {
        let key = cs.name.to_lowercase();
        if !self.searchable_text.contains_key(&key) {
            self.searchable_text
                .insert(key.clone(), get_searchable_text(cs));
        }
        self.searchable_text.get(&key).unwrap()
    }

    fn searchable_lower<'a>(&'a mut self, cs: &ChangeSpecWire) -> &'a str {
        let key = cs.name.to_lowercase();
        if !self.searchable_lower.contains_key(&key) {
            let text = self.searchable_text(cs).to_string();
            self.searchable_lower
                .insert(key.clone(), text.to_lowercase());
        }
        self.searchable_lower.get(&key).unwrap()
    }

    fn match_string(
        &mut self,
        cs: &ChangeSpecWire,
        value: &str,
        case_sensitive: bool,
    ) -> bool {
        if case_sensitive {
            self.searchable_text(cs).contains(value)
        } else {
            let needle = value.to_lowercase();
            self.searchable_lower(cs).contains(&needle)
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
        all_specs: &[ChangeSpecWire],
        cs: &ChangeSpecWire,
        ancestor_value: &str,
    ) -> bool {
        let ancestor_value_lower = ancestor_value.to_lowercase();
        let cache_key = (cs.name.to_lowercase(), ancestor_value_lower.clone());
        if let Some(cached) = self.ancestor_memo.get(&cache_key) {
            return *cached;
        }

        let mut visited: Vec<String> = Vec::new();
        let mut current_name_lower = cs.name.to_lowercase();
        let mut current_parent: Option<String> = cs.parent.clone();
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
                    match self.name_map.get(&parent_lower).copied() {
                        Some(idx) => {
                            let parent_cs = &all_specs[idx];
                            current_name_lower = parent_cs.name.to_lowercase();
                            current_parent = parent_cs.parent.clone();
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
        all_specs: &[ChangeSpecWire],
        cs: &ChangeSpecWire,
        key: &str,
        value: &str,
    ) -> bool {
        match key {
            "status" => match_status(value, cs),
            "project" => match_project(value, cs),
            "ancestor" => self.match_ancestor(all_specs, cs, value),
            "name" => match_name(value, cs),
            "sibling" => match_sibling(value, cs),
            // Unknown property keys never match (parser rejects unknown keys
            // up front, so this is just defensive).
            _ => false,
        }
    }

    fn evaluate(
        &mut self,
        all_specs: &[ChangeSpecWire],
        cs: &ChangeSpecWire,
        expr: &QueryExprWire,
    ) -> bool {
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
                    return self
                        .searchable_text(cs)
                        .contains(RUNNING_AGENT_MARKER);
                }
                if *is_running_process {
                    return self
                        .searchable_text(cs)
                        .contains(RUNNING_PROCESS_MARKER);
                }
                self.match_string(cs, value, *case_sensitive)
            }
            QueryExprWire::PropertyMatch { key, value } => {
                self.match_property(all_specs, cs, key, value)
            }
            QueryExprWire::Not { operand } => {
                !self.evaluate(all_specs, cs, operand)
            }
            QueryExprWire::And { operands } => {
                operands.iter().all(|op| self.evaluate(all_specs, cs, op))
            }
            QueryExprWire::Or { operands } => {
                operands.iter().any(|op| self.evaluate(all_specs, cs, op))
            }
        }
    }
}

/// Evaluate a compiled query against every spec in `specs`, returning a
/// boolean per spec. The primary public API for Phase 2C.
pub fn evaluate_query_many(
    program: &QueryProgram,
    specs: &[ChangeSpecWire],
) -> Vec<bool> {
    let mut ctx = QueryEvaluationContext::build(specs);
    specs
        .iter()
        .map(|cs| ctx.evaluate(specs, cs, &program.expr))
        .collect()
}

/// Evaluate a compiled query against one spec inside an existing list.
/// Useful for parity tests; production callers should prefer
/// [`evaluate_query_many`].
pub fn evaluate_query_one(
    program: &QueryProgram,
    cs: &ChangeSpecWire,
    all_specs: &[ChangeSpecWire],
) -> bool {
    let mut ctx = QueryEvaluationContext::build(all_specs);
    ctx.evaluate(all_specs, cs, &program.expr)
}
