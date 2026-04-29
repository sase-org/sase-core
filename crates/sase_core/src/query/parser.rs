//! Pure-Rust port of `src/sase/ace/query/parser.py`.
//!
//! Recursive-descent parser with the same precedence as Python:
//!
//! 1. `NOT` binds tightest.
//! 2. `AND` (explicit or implicit via juxtaposition).
//! 3. `OR` binds loosest.
//!
//! Parentheses override precedence. `*` expands to `Or(!!!, @@@, $$$)`.

use crate::query::tokenizer::tokenize_query;
use crate::query::types::{
    QueryErrorWire, QueryExprWire, QueryTokenKind, QueryTokenWire,
};

struct Parser {
    tokens: Vec<QueryTokenWire>,
    pos: usize,
}

impl Parser {
    fn new(query: &str) -> Result<Self, QueryErrorWire> {
        let tokens = tokenize_query(query)?;
        Ok(Parser { tokens, pos: 0 })
    }

    fn current(&self) -> &QueryTokenWire {
        // tokens always ends with Eof
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn check(&self, kind: QueryTokenKind) -> bool {
        self.current().kind == kind
    }

    fn advance(&mut self) -> QueryTokenWire {
        let tok = self.current().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect(
        &mut self,
        kind: QueryTokenKind,
    ) -> Result<QueryTokenWire, QueryErrorWire> {
        let tok = self.current();
        if tok.kind != kind {
            return Err(QueryErrorWire::parser(
                format!("Expected {:?}, got {:?}", kind, tok.kind),
                tok.position as usize,
            ));
        }
        Ok(self.advance())
    }

    fn parse(&mut self) -> Result<QueryExprWire, QueryErrorWire> {
        if self.check(QueryTokenKind::Eof) {
            return Err(QueryErrorWire::parser("Empty query", 0));
        }
        let expr = self.parse_or()?;
        if !self.check(QueryTokenKind::Eof) {
            let tok = self.current();
            let label = if tok.value.is_empty() {
                format!("{:?}", tok.kind)
            } else {
                tok.value.clone()
            };
            return Err(QueryErrorWire::parser(
                format!("Unexpected token: {}", label),
                tok.position as usize,
            ));
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<QueryExprWire, QueryErrorWire> {
        let left = self.parse_and()?;
        let mut operands = vec![left];
        while self.check(QueryTokenKind::Or) {
            self.advance();
            operands.push(self.parse_and()?);
        }
        if operands.len() == 1 {
            Ok(operands.pop().unwrap())
        } else {
            Ok(QueryExprWire::Or { operands })
        }
    }

    fn can_start_unary(&self) -> bool {
        matches!(
            self.current().kind,
            QueryTokenKind::String
                | QueryTokenKind::Property
                | QueryTokenKind::Not
                | QueryTokenKind::Lparen
                | QueryTokenKind::ErrorSuffix
                | QueryTokenKind::NotErrorSuffix
                | QueryTokenKind::RunningAgent
                | QueryTokenKind::NotRunningAgent
                | QueryTokenKind::RunningProcess
                | QueryTokenKind::NotRunningProcess
                | QueryTokenKind::AnySpecial
        )
    }

    fn parse_and(&mut self) -> Result<QueryExprWire, QueryErrorWire> {
        let left = self.parse_unary()?;
        let mut operands = vec![left];
        loop {
            if self.check(QueryTokenKind::And) {
                self.advance();
                operands.push(self.parse_unary()?);
            } else if self.can_start_unary() {
                operands.push(self.parse_unary()?);
            } else {
                break;
            }
        }
        if operands.len() == 1 {
            Ok(operands.pop().unwrap())
        } else {
            Ok(QueryExprWire::And { operands })
        }
    }

    fn parse_unary(&mut self) -> Result<QueryExprWire, QueryErrorWire> {
        let mut not_count = 0usize;
        while self.check(QueryTokenKind::Not) {
            self.advance();
            not_count += 1;
        }
        let mut expr = self.parse_primary()?;
        for _ in 0..not_count {
            expr = QueryExprWire::negate(expr);
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<QueryExprWire, QueryErrorWire> {
        let tok = self.current().clone();
        match tok.kind {
            QueryTokenKind::String => {
                self.advance();
                Ok(QueryExprWire::string_match(tok.value, tok.case_sensitive))
            }
            QueryTokenKind::Property => {
                self.advance();
                let key = tok.property_key.ok_or_else(|| {
                    QueryErrorWire::parser(
                        "Property token missing property_key",
                        tok.position as usize,
                    )
                })?;
                Ok(QueryExprWire::property(key, tok.value))
            }
            QueryTokenKind::ErrorSuffix => {
                self.advance();
                Ok(QueryExprWire::error_suffix())
            }
            QueryTokenKind::NotErrorSuffix => {
                self.advance();
                Ok(QueryExprWire::negate(QueryExprWire::error_suffix()))
            }
            QueryTokenKind::RunningAgent => {
                self.advance();
                Ok(QueryExprWire::running_agent())
            }
            QueryTokenKind::NotRunningAgent => {
                self.advance();
                Ok(QueryExprWire::negate(QueryExprWire::running_agent()))
            }
            QueryTokenKind::RunningProcess => {
                self.advance();
                Ok(QueryExprWire::running_process())
            }
            QueryTokenKind::NotRunningProcess => {
                self.advance();
                Ok(QueryExprWire::negate(QueryExprWire::running_process()))
            }
            QueryTokenKind::AnySpecial => {
                self.advance();
                Ok(QueryExprWire::Or {
                    operands: vec![
                        QueryExprWire::error_suffix(),
                        QueryExprWire::running_agent(),
                        QueryExprWire::running_process(),
                    ],
                })
            }
            QueryTokenKind::Lparen => {
                self.advance();
                let expr = self.parse_or()?;
                self.expect(QueryTokenKind::Rparen)?;
                Ok(expr)
            }
            other => {
                let label = if tok.value.is_empty() {
                    format!("{:?}", other)
                } else {
                    tok.value
                };
                Err(QueryErrorWire::parser(
                    format!("Expected string or '(', got {}", label),
                    tok.position as usize,
                ))
            }
        }
    }
}

/// Parse a query string into an AST. Mirrors `parse_query_python`.
pub fn parse_query(query: &str) -> Result<QueryExprWire, QueryErrorWire> {
    let mut parser = Parser::new(query)?;
    parser.parse()
}

/// Convert a query expression to canonical string form. Mirrors
/// `to_canonical_string` in Python's `types.py`.
pub fn canonicalize_query(expr: &QueryExprWire) -> String {
    match expr {
        QueryExprWire::StringMatch {
            value,
            case_sensitive,
            is_error_suffix,
            is_running_agent,
            is_running_process,
        } => {
            if *is_error_suffix {
                "!!!".to_string()
            } else if *is_running_agent {
                "@@@".to_string()
            } else if *is_running_process {
                "$$$".to_string()
            } else {
                let escaped = escape_string_value(value);
                if *case_sensitive {
                    format!("c\"{}\"", escaped)
                } else {
                    format!("\"{}\"", escaped)
                }
            }
        }
        QueryExprWire::PropertyMatch { key, value } => {
            format!("{}:{}", key, value)
        }
        QueryExprWire::Not { operand } => {
            let inner = canonicalize_query(operand);
            if matches!(
                operand.as_ref(),
                QueryExprWire::And { .. } | QueryExprWire::Or { .. }
            ) {
                format!("NOT ({})", inner)
            } else {
                format!("NOT {}", inner)
            }
        }
        QueryExprWire::And { operands } => operands
            .iter()
            .map(|op| {
                let inner = canonicalize_query(op);
                if matches!(op, QueryExprWire::Or { .. }) {
                    format!("({})", inner)
                } else {
                    inner
                }
            })
            .collect::<Vec<_>>()
            .join(" AND "),
        QueryExprWire::Or { operands } => operands
            .iter()
            .map(|op| {
                let inner = canonicalize_query(op);
                if matches!(op, QueryExprWire::And { .. }) {
                    format!("({})", inner)
                } else {
                    inner
                }
            })
            .collect::<Vec<_>>()
            .join(" OR "),
    }
}

fn escape_string_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}
