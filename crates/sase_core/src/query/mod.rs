//! Pure-Rust query language: tokenizer, AST, parser, canonicalizer.
//!
//! Phase 2B of the Rust backend migration. Mirrors the Python implementation
//! in `src/sase/ace/query/{tokenizer,parser,types}.py` exactly so the AST and
//! canonical-string output match the Python golden corpus. No PyO3 leaks here.
//!
//! ## Regex semantics
//!
//! The query language uses substring matching (case-insensitive via lowercasing)
//! on `ChangeSpec` text — **not** user-supplied regex matching. The two `re`
//! helpers in the Python evaluator only exist to strip status suffixes; they
//! are not exposed to query authors. Phase 2C should therefore not pull in a
//! regex engine for the user-facing matchers.

pub mod parser;
pub mod tokenizer;
pub mod types;

#[cfg(test)]
mod tests;

pub use parser::{canonicalize_query, parse_query};
pub use tokenizer::tokenize_query;
pub use types::{
    QueryErrorWire, QueryExprWire, QueryProgramWire, QueryTokenKind,
    QueryTokenWire, ERROR_SUFFIX_QUERY, RUNNING_AGENT_QUERY,
    RUNNING_PROCESS_QUERY, VALID_PROPERTY_KEYS,
};
