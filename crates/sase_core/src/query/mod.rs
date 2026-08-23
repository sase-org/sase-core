//! Pure-Rust query language: tokenizer, AST, parser, canonicalizer, and
//! batch evaluator.
//!
//! Phases 2B and 2C of the Rust backend migration. The tokenizer/parser
//! mirror `src/sase/ace/query/{tokenizer,parser,types}.py` exactly; the
//! evaluator mirrors `src/sase/ace/query/{searchable,matchers,context}.py`.
//! The AST and canonical-string output match the Python golden corpus, and
//! the evaluator's output matches the Python golden evaluation matrix.
//! No PyO3 leaks here.
//!
//! ## Regex semantics
//!
//! The query language uses substring matching (case-insensitive via
//! lowercasing) on `Patch` text — **not** user-supplied regex
//! matching. The two `re` helpers in the Python evaluator only exist to
//! strip status suffixes; they are not exposed to query authors. The Rust
//! port follows suit: `regex::Regex` is used internally for status
//! stripping and reverted-suffix parsing only.

pub mod evaluator;
pub mod flat;
pub mod matchers;
pub mod parser;
pub mod profile;
pub mod row;
pub mod searchable;
pub mod tokenizer;
pub mod types;

#[cfg(test)]
mod tests;

pub use evaluator::{
    compile_query, compile_query_with_profile, evaluate_query_many,
    evaluate_query_many_in_corpus, evaluate_query_one,
    try_evaluate_query_many_in_corpus, QueryCorpus, QueryEvaluationContext,
    QueryProgram,
};
pub use matchers::{
    get_base_status, has_any_status_suffix, match_name, match_project,
    match_sibling, match_status, strip_reverted_suffix,
};
pub use parser::{
    canonicalize_query, canonicalize_query_with_profile, parse_query,
    parse_query_with_profile,
};
pub use profile::{
    host_date_bound_direction, host_duration_bound_direction,
    patch_query_profile, CompiledQueryProfile, FieldValueKind, QueryFieldSpec,
    QueryMacroSpec, QuerySigilSpec, HOST_DATE_BOUND_KEYS,
    HOST_DURATION_BOUND_KEYS, HOST_MACRO_TRIGGERS, HOST_PREDICATE_NAMES,
    HOST_SIGIL_CHARS,
};
pub use row::{
    patch_rows_from_specs, IndexedQueryRow, QueryFieldValues,
    QueryPredicateFacts, QueryRow,
};
pub use searchable::{
    effective_project_name, get_searchable_text, project_dir_name,
    RUNNING_AGENT_MARKER, RUNNING_PROCESS_MARKER,
};
pub use tokenizer::{tokenize_query, tokenize_query_with_profile};
pub use types::{
    QueryErrorWire, QueryExprWire, QueryProgramWire, QueryTokenKind,
    QueryTokenWire, ERROR_SUFFIX_QUERY, RUNNING_AGENT_QUERY,
    RUNNING_PROCESS_QUERY, VALID_PROPERTY_KEYS,
};
