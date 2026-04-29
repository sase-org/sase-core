//! Query AST and wire records.
//!
//! Mirrors the Python query AST in `src/sase/ace/query/types.py` and the
//! Phase 2A wire shape sketched in `plans/202604/rust_backend_phase2_query.md`:
//! tokens (`QueryTokenWire`), expressions (`QueryExprWire`), compiled programs
//! (`QueryProgramWire`), and structured errors (`QueryErrorWire`).
//!
//! The wire shape is owned (no borrowed lifetimes) so it round-trips through
//! JSON identically to the Python dataclasses.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The internal expansion of the `!!!` shorthand (matches error-suffix markers
/// inside `ChangeSpec` text). Mirrors `ERROR_SUFFIX_QUERY` in Python.
pub const ERROR_SUFFIX_QUERY: &str = " - (!: ";

/// Internal marker for `@@@` (running-agent) — mirrors `RUNNING_AGENT_QUERY`.
pub const RUNNING_AGENT_QUERY: &str = "@@@ internal marker";

/// Internal marker for `$$$` (running-process) — mirrors `RUNNING_PROCESS_QUERY`.
pub const RUNNING_PROCESS_QUERY: &str = "$$$ internal marker";

/// Property keys accepted by the parser.
pub const VALID_PROPERTY_KEYS: &[&str] =
    &["status", "project", "ancestor", "name", "sibling"];

/// Token kinds matching `TokenType` in the Python tokenizer. Serialized as
/// lowercase tags for JSON parity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryTokenKind {
    String,
    Property,
    And,
    Or,
    Not,
    ErrorSuffix,
    NotErrorSuffix,
    RunningAgent,
    NotRunningAgent,
    RunningProcess,
    NotRunningProcess,
    AnySpecial,
    Lparen,
    Rparen,
    Eof,
}

/// Wire representation of one token. `position` is a byte offset in the input
/// (the Python tokenizer indexes characters, but in the supported grammar all
/// significant tokens are ASCII, so byte offsets are equivalent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTokenWire {
    pub kind: QueryTokenKind,
    pub value: String,
    #[serde(default)]
    pub case_sensitive: bool,
    pub position: u32,
    #[serde(default)]
    pub property_key: Option<String>,
}

/// Tagged AST node. Mirrors the Python `QueryExpr` union but uses serde's
/// internally-tagged form so JSON shape is `{"kind": "...", ...}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryExprWire {
    StringMatch {
        value: String,
        #[serde(default)]
        case_sensitive: bool,
        #[serde(default)]
        is_error_suffix: bool,
        #[serde(default)]
        is_running_agent: bool,
        #[serde(default)]
        is_running_process: bool,
    },
    PropertyMatch {
        key: String,
        value: String,
    },
    Not {
        operand: Box<QueryExprWire>,
    },
    And {
        operands: Vec<QueryExprWire>,
    },
    Or {
        operands: Vec<QueryExprWire>,
    },
}

impl QueryExprWire {
    pub fn string_match(
        value: impl Into<String>,
        case_sensitive: bool,
    ) -> Self {
        QueryExprWire::StringMatch {
            value: value.into(),
            case_sensitive,
            is_error_suffix: false,
            is_running_agent: false,
            is_running_process: false,
        }
    }

    pub fn error_suffix() -> Self {
        QueryExprWire::StringMatch {
            value: ERROR_SUFFIX_QUERY.to_string(),
            case_sensitive: false,
            is_error_suffix: true,
            is_running_agent: false,
            is_running_process: false,
        }
    }

    pub fn running_agent() -> Self {
        QueryExprWire::StringMatch {
            value: RUNNING_AGENT_QUERY.to_string(),
            case_sensitive: false,
            is_error_suffix: false,
            is_running_agent: true,
            is_running_process: false,
        }
    }

    pub fn running_process() -> Self {
        QueryExprWire::StringMatch {
            value: RUNNING_PROCESS_QUERY.to_string(),
            case_sensitive: false,
            is_error_suffix: false,
            is_running_agent: false,
            is_running_process: true,
        }
    }

    pub fn property(key: impl Into<String>, value: impl Into<String>) -> Self {
        QueryExprWire::PropertyMatch {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn negate(operand: QueryExprWire) -> Self {
        QueryExprWire::Not {
            operand: Box::new(operand),
        }
    }
}

/// Compiled-query handle. Phase 2B emits the AST directly; later phases may
/// add a precomputed lowercase string or other caches without breaking the
/// JSON shape thanks to `#[serde(default)]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryProgramWire {
    pub source: String,
    pub expr: QueryExprWire,
}

/// Structured error returned by the tokenizer or parser.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[error("{kind}: {message} (at position {position})")]
pub struct QueryErrorWire {
    pub kind: String,
    pub message: String,
    pub position: u32,
}

impl QueryErrorWire {
    pub(crate) fn tokenizer(
        message: impl Into<String>,
        position: usize,
    ) -> Self {
        QueryErrorWire {
            kind: "tokenizer".to_string(),
            message: message.into(),
            position: position as u32,
        }
    }

    pub(crate) fn parser(message: impl Into<String>, position: usize) -> Self {
        QueryErrorWire {
            kind: "parser".to_string(),
            message: message.into(),
            position: position as u32,
        }
    }
}
