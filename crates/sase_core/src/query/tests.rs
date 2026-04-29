//! Phase 2B Rust unit tests for the query tokenizer/parser/canonicalizer.
//!
//! Mirrors the Python golden corpus (`tests/test_query_tokenizer.py`,
//! `tests/test_query_parser.py`, `tests/test_query_canonicalization.py`,
//! `tests/test_query_property_filters.py`) so behavior parity is enforced
//! without depending on the Python toolchain.

use crate::query::types::{
    QueryErrorWire, QueryExprWire, QueryTokenKind, ERROR_SUFFIX_QUERY,
    RUNNING_AGENT_QUERY, RUNNING_PROCESS_QUERY,
};
use crate::query::{canonicalize_query, parse_query, tokenize_query};

// ---------- helpers ----------

fn tok(query: &str) -> Vec<crate::query::types::QueryTokenWire> {
    tokenize_query(query).expect("tokenize")
}

fn parse(query: &str) -> QueryExprWire {
    parse_query(query).expect("parse")
}

// ---------- tokenizer ----------

#[test]
fn tokenize_bare_word_with_numbers() {
    let toks = tok("foo123");
    assert_eq!(toks[0].kind, QueryTokenKind::String);
    assert_eq!(toks[0].value, "foo123");
    assert_eq!(toks[1].kind, QueryTokenKind::Eof);
}

#[test]
fn tokenize_quoted_with_escapes() {
    let toks = tok(r#""hello\nworld""#);
    assert_eq!(toks[0].kind, QueryTokenKind::String);
    assert_eq!(toks[0].value, "hello\nworld");

    let toks = tok(r#""say \"hi\"""#);
    assert_eq!(toks[0].value, r#"say "hi""#);
}

#[test]
fn tokenize_case_sensitive_string() {
    let toks = tok(r#"c"FooBar""#);
    assert_eq!(toks[0].kind, QueryTokenKind::String);
    assert_eq!(toks[0].value, "FooBar");
    assert!(toks[0].case_sensitive);
}

#[test]
fn tokenize_not_keyword_with_error_suffix() {
    let toks = tok("NOT !!!");
    assert_eq!(toks[0].kind, QueryTokenKind::Not);
    assert_eq!(toks[0].value, "NOT");
    assert_eq!(toks[1].kind, QueryTokenKind::ErrorSuffix);
    assert_eq!(toks[1].value, "!!!");
}

#[test]
fn tokenize_standalone_at_and_bang() {
    let toks = tok("@");
    assert_eq!(toks[0].kind, QueryTokenKind::RunningAgent);
    assert_eq!(toks[0].value, "@");

    let toks = tok("!");
    assert_eq!(toks[0].kind, QueryTokenKind::ErrorSuffix);
    assert_eq!(toks[0].value, "!");

    let toks = tok("$");
    assert_eq!(toks[0].kind, QueryTokenKind::RunningProcess);
    assert_eq!(toks[0].value, "$");
}

#[test]
fn tokenize_triple_specials() {
    assert_eq!(tok("@@@")[0].kind, QueryTokenKind::RunningAgent);
    assert_eq!(tok("!!!")[0].kind, QueryTokenKind::ErrorSuffix);
    assert_eq!(tok("$$$")[0].kind, QueryTokenKind::RunningProcess);
}

#[test]
fn tokenize_at_not_standalone_is_error() {
    let err = tokenize_query("@foo").unwrap_err();
    assert!(err.message.contains("Unexpected character"), "{:?}", err);
}

#[test]
fn tokenize_dollar_not_standalone_is_error() {
    let err = tokenize_query("$foo").unwrap_err();
    assert!(err.message.contains("Unexpected character"), "{:?}", err);
}

#[test]
fn tokenize_not_at_with_space() {
    let toks = tok(r#"!@ "foo""#);
    assert_eq!(toks[0].kind, QueryTokenKind::NotRunningAgent);
    assert_eq!(toks[0].value, "!@");
    assert_eq!(toks[1].kind, QueryTokenKind::String);
    assert_eq!(toks[1].value, "foo");
}

#[test]
fn tokenize_not_dollar_with_space() {
    let toks = tok(r#"!$ "foo""#);
    assert_eq!(toks[0].kind, QueryTokenKind::NotRunningProcess);
    assert_eq!(toks[1].kind, QueryTokenKind::String);
}

#[test]
fn tokenize_double_exclamation_not_standalone_is_two_nots() {
    // !!"foo" → NOT NOT STRING, not NOT_ERROR_SUFFIX
    let toks = tok(r#"!!"foo""#);
    assert_eq!(toks[0].kind, QueryTokenKind::Not);
    assert_eq!(toks[1].kind, QueryTokenKind::Not);
    assert_eq!(toks[2].kind, QueryTokenKind::String);
    assert_eq!(toks[2].value, "foo");
}

#[test]
fn tokenize_double_exclamation_standalone_is_not_error_suffix() {
    let toks = tok("!! foo");
    assert_eq!(toks[0].kind, QueryTokenKind::NotErrorSuffix);
    assert_eq!(toks[1].kind, QueryTokenKind::String);
}

#[test]
fn tokenize_status_shorthands() {
    for (input, status) in [
        ("%d", "DRAFT"),
        ("%m", "MAILED"),
        ("%r", "REVERTED"),
        ("%s", "SUBMITTED"),
        ("%w", "WIP"),
        ("%y", "READY"),
    ] {
        let toks = tok(input);
        assert_eq!(toks[0].kind, QueryTokenKind::Property, "{input}");
        assert_eq!(toks[0].value, status, "{input}");
        assert_eq!(toks[0].property_key.as_deref(), Some("status"), "{input}");
    }
}

#[test]
fn tokenize_status_shorthand_uppercase() {
    let toks = tok("%Y");
    assert_eq!(toks[0].value, "READY");
    assert_eq!(toks[0].property_key.as_deref(), Some("status"));
}

#[test]
fn tokenize_status_shorthand_invalid() {
    let err = tokenize_query("%x").unwrap_err();
    assert!(
        err.message.contains("Invalid status shorthand"),
        "{:?}",
        err
    );
}

#[test]
fn tokenize_property_quoted_value() {
    let toks = tok(r#"status:"my status""#);
    assert_eq!(toks[0].kind, QueryTokenKind::Property);
    assert_eq!(toks[0].value, "my status");
    assert_eq!(toks[0].property_key.as_deref(), Some("status"));
}

#[test]
fn tokenize_invalid_property_key() {
    let err = tokenize_query("unknown:value").unwrap_err();
    assert!(err.message.contains("Unknown property key"), "{:?}", err);
}

#[test]
fn tokenize_property_shorthands() {
    let toks = tok("+myproject");
    assert_eq!(toks[0].kind, QueryTokenKind::Property);
    assert_eq!(toks[0].value, "myproject");
    assert_eq!(toks[0].property_key.as_deref(), Some("project"));

    let toks = tok("^parent_feature");
    assert_eq!(toks[0].property_key.as_deref(), Some("ancestor"));
    assert_eq!(toks[0].value, "parent_feature");

    let toks = tok("~sibling_branch");
    assert_eq!(toks[0].property_key.as_deref(), Some("sibling"));

    let toks = tok("&my_name");
    assert_eq!(toks[0].property_key.as_deref(), Some("name"));
    assert_eq!(toks[0].value, "my_name");
}

#[test]
fn tokenize_paren_and_keywords() {
    let toks = tok(r#"("a" AND "b") OR NOT "c""#);
    let kinds: Vec<_> = toks.iter().map(|t| t.kind).collect();
    assert_eq!(
        kinds,
        vec![
            QueryTokenKind::Lparen,
            QueryTokenKind::String,
            QueryTokenKind::And,
            QueryTokenKind::String,
            QueryTokenKind::Rparen,
            QueryTokenKind::Or,
            QueryTokenKind::Not,
            QueryTokenKind::String,
            QueryTokenKind::Eof,
        ]
    );
}

#[test]
fn tokenize_unterminated_string() {
    let err = tokenize_query(r#""abc"#).unwrap_err();
    assert!(err.message.contains("Unterminated string"), "{:?}", err);
}

#[test]
fn tokenize_invalid_escape() {
    let err = tokenize_query(r#""bad \q""#).unwrap_err();
    assert!(err.message.contains("Invalid escape sequence"), "{:?}", err);
}

#[test]
fn tokenize_any_special_only_standalone() {
    let toks = tok("*");
    assert_eq!(toks[0].kind, QueryTokenKind::AnySpecial);
    let err = tokenize_query("*foo").unwrap_err();
    assert!(err.message.contains("Unexpected character"), "{:?}", err);
}

// ---------- parser ----------

#[test]
fn parse_error_empty_query() {
    let err = parse_query("").unwrap_err();
    assert!(err.message.contains("Empty query"), "{:?}", err);
}

#[test]
fn parse_error_unmatched_paren() {
    let err = parse_query(r#"("a""#).unwrap_err();
    assert!(
        err.message.contains("Rparen") || err.message.contains("RPAREN"),
        "{:?}",
        err
    );
}

#[test]
fn parse_error_missing_operand() {
    let err = parse_query(r#""a" AND"#).unwrap_err();
    assert!(err.message.contains("Expected"), "{:?}", err);
}

#[test]
fn parse_bare_word() {
    let expr = parse("foobar");
    match expr {
        QueryExprWire::StringMatch {
            value,
            case_sensitive,
            ..
        } => {
            assert_eq!(value, "foobar");
            assert!(!case_sensitive);
        }
        other => panic!("unexpected: {:?}", other),
    }
}

#[test]
fn parse_case_sensitive_string() {
    let expr = parse(r#"c"Foo""#);
    match expr {
        QueryExprWire::StringMatch {
            value,
            case_sensitive,
            ..
        } => {
            assert_eq!(value, "Foo");
            assert!(case_sensitive);
        }
        other => panic!("unexpected: {:?}", other),
    }
}

#[test]
fn parse_implicit_and() {
    let expr = parse(r#""a" "b""#);
    match expr {
        QueryExprWire::And { operands } => {
            assert_eq!(operands.len(), 2);
        }
        other => panic!("unexpected: {:?}", other),
    }
}

#[test]
fn parse_implicit_and_with_parens() {
    let expr = parse(r#""a" ("b" OR "c")"#);
    let QueryExprWire::And { operands } = expr else {
        panic!("expected And");
    };
    assert_eq!(operands.len(), 2);
    assert!(matches!(operands[0], QueryExprWire::StringMatch { .. }));
    assert!(matches!(operands[1], QueryExprWire::Or { .. }));
}

#[test]
fn parse_or_loosest() {
    let expr = parse(r#""a" OR "b" "c""#);
    // OR is loosest: parsed as ("a") OR ("b" AND "c")
    let QueryExprWire::Or { operands } = expr else {
        panic!("expected Or");
    };
    assert_eq!(operands.len(), 2);
    assert!(matches!(operands[0], QueryExprWire::StringMatch { .. }));
    assert!(matches!(operands[1], QueryExprWire::And { .. }));
}

#[test]
fn parse_not_tightest() {
    // NOT binds tighter than AND/OR: !"a" "b" parses as (NOT "a") AND "b"
    let expr = parse(r#"!"a" "b""#);
    let QueryExprWire::And { operands } = expr else {
        panic!("expected And");
    };
    assert!(matches!(operands[0], QueryExprWire::Not { .. }));
    assert!(matches!(operands[1], QueryExprWire::StringMatch { .. }));
}

#[test]
fn parse_standalone_exclamation_is_error_suffix() {
    let expr = parse("!");
    match expr {
        QueryExprWire::StringMatch {
            value,
            is_error_suffix,
            ..
        } => {
            assert_eq!(value, ERROR_SUFFIX_QUERY);
            assert!(is_error_suffix);
        }
        other => panic!("unexpected: {:?}", other),
    }
}

#[test]
fn parse_error_suffix_and_string() {
    let expr = parse(r#"!!! AND "foo""#);
    let QueryExprWire::And { operands } = expr else {
        panic!("expected And");
    };
    assert_eq!(operands.len(), 2);
    match &operands[0] {
        QueryExprWire::StringMatch {
            is_error_suffix, ..
        } => assert!(is_error_suffix),
        other => panic!("expected error-suffix StringMatch: {:?}", other),
    }
}

#[test]
fn parse_not_running_agent_shorthand() {
    let expr = parse("!@");
    let QueryExprWire::Not { operand } = expr else {
        panic!("expected Not");
    };
    let QueryExprWire::StringMatch {
        value,
        is_running_agent,
        ..
    } = *operand
    else {
        panic!("expected StringMatch");
    };
    assert_eq!(value, RUNNING_AGENT_QUERY);
    assert!(is_running_agent);
}

#[test]
fn parse_not_running_process_shorthand() {
    let expr = parse("!$");
    let QueryExprWire::Not { operand } = expr else {
        panic!("expected Not");
    };
    let QueryExprWire::StringMatch {
        value,
        is_running_process,
        ..
    } = *operand
    else {
        panic!("expected StringMatch");
    };
    assert_eq!(value, RUNNING_PROCESS_QUERY);
    assert!(is_running_process);
}

#[test]
fn parse_any_special_expands_to_or() {
    let expr = parse("*");
    let QueryExprWire::Or { operands } = expr else {
        panic!("expected Or");
    };
    assert_eq!(operands.len(), 3);
    assert!(matches!(
        operands[0],
        QueryExprWire::StringMatch {
            is_error_suffix: true,
            ..
        }
    ));
    assert!(matches!(
        operands[1],
        QueryExprWire::StringMatch {
            is_running_agent: true,
            ..
        }
    ));
    assert!(matches!(
        operands[2],
        QueryExprWire::StringMatch {
            is_running_process: true,
            ..
        }
    ));
}

#[test]
fn parse_property_match() {
    let expr = parse("status:WIP");
    match expr {
        QueryExprWire::PropertyMatch { key, value } => {
            assert_eq!(key, "status");
            assert_eq!(value, "WIP");
        }
        other => panic!("unexpected: {:?}", other),
    }
}

#[test]
fn parse_double_not_collapses_to_two_nots() {
    // !!"foo" tokenizes as two NOTs (since !! followed by " is not standalone).
    // Parser applies them inside-out so the AST is Not(Not(StringMatch))).
    let expr = parse(r#"!!"foo""#);
    let QueryExprWire::Not { operand } = expr else {
        panic!("expected outer Not");
    };
    let QueryExprWire::Not { operand: inner } = *operand else {
        panic!("expected inner Not");
    };
    assert!(matches!(*inner, QueryExprWire::StringMatch { .. }));
}

// ---------- canonicalization ----------

#[test]
fn canonical_simple_string() {
    assert_eq!(canonicalize_query(&parse("foo")), "\"foo\"");
}

#[test]
fn canonical_case_sensitive_string() {
    assert_eq!(canonicalize_query(&parse(r#"c"Foo""#)), r#"c"Foo""#);
}

#[test]
fn canonical_not_string() {
    assert_eq!(canonicalize_query(&parse(r#"!"foo""#)), "NOT \"foo\"");
}

#[test]
fn canonical_implicit_and() {
    assert_eq!(canonicalize_query(&parse(r#""a" "b""#)), "\"a\" AND \"b\"");
}

#[test]
fn canonical_or() {
    assert_eq!(
        canonicalize_query(&parse(r#""a" OR "b""#)),
        "\"a\" OR \"b\""
    );
}

#[test]
fn canonical_or_inside_and_gets_parens() {
    // "a" AND ("b" OR "c") canonicalizes with parens around the OR
    assert_eq!(
        canonicalize_query(&parse(r#""a" ("b" OR "c")"#)),
        "\"a\" AND (\"b\" OR \"c\")"
    );
}

#[test]
fn canonical_and_inside_or_gets_parens() {
    assert_eq!(
        canonicalize_query(&parse(r#""a" OR "b" "c""#)),
        "\"a\" OR (\"b\" AND \"c\")"
    );
}

#[test]
fn canonical_not_around_and_gets_parens() {
    let expr = QueryExprWire::negate(QueryExprWire::And {
        operands: vec![
            QueryExprWire::string_match("a", false),
            QueryExprWire::string_match("b", false),
        ],
    });
    assert_eq!(canonicalize_query(&expr), "NOT (\"a\" AND \"b\")");
}

#[test]
fn canonical_status_property() {
    assert_eq!(canonicalize_query(&parse("%d")), "status:DRAFT");
}

#[test]
fn canonical_any_special_implicit_and() {
    assert_eq!(
        canonicalize_query(&parse(r#"* "foo""#)),
        "(!!! OR @@@ OR $$$) AND \"foo\""
    );
}

#[test]
fn canonical_error_suffix_running_markers() {
    assert_eq!(canonicalize_query(&parse("!!!")), "!!!");
    assert_eq!(canonicalize_query(&parse("@@@")), "@@@");
    assert_eq!(canonicalize_query(&parse("$$$")), "$$$");
    assert_eq!(canonicalize_query(&parse("!")), "!!!");
    assert_eq!(canonicalize_query(&parse("@")), "@@@");
    assert_eq!(canonicalize_query(&parse("$")), "$$$");
}

#[test]
fn canonical_property_filter_shorthands() {
    assert_eq!(canonicalize_query(&parse("+myproj")), "project:myproj");
    assert_eq!(canonicalize_query(&parse("^parent")), "ancestor:parent");
    assert_eq!(canonicalize_query(&parse("~sib")), "sibling:sib");
    assert_eq!(canonicalize_query(&parse("&nm")), "name:nm");
}

#[test]
fn canonical_escape_string_value() {
    // Backslash, quote, newline, tab, return all escape on canonicalization.
    let expr = QueryExprWire::string_match("a\\b\"c\nd\te\rf", false);
    assert_eq!(canonicalize_query(&expr), r#""a\\b\"c\nd\te\rf""#);
}

// ---------- error wire shape ----------

#[test]
fn tokenizer_error_wire_kind_is_tokenizer() {
    let err: QueryErrorWire = tokenize_query("@foo").unwrap_err();
    assert_eq!(err.kind, "tokenizer");
    assert!(err.position == 0);
}

#[test]
fn parser_error_wire_kind_is_parser() {
    let err: QueryErrorWire = parse_query("").unwrap_err();
    assert_eq!(err.kind, "parser");
}

// ---------- JSON parity smoke ----------

#[test]
fn ast_round_trips_through_json() {
    let expr = parse(r#""a" OR ("b" AND !"c")"#);
    let s = serde_json::to_string(&expr).unwrap();
    let back: QueryExprWire = serde_json::from_str(&s).unwrap();
    assert_eq!(back, expr);
}

#[test]
fn token_round_trips_through_json() {
    let toks = tok("status:WIP");
    let s = serde_json::to_string(&toks).unwrap();
    let back: Vec<crate::query::types::QueryTokenWire> =
        serde_json::from_str(&s).unwrap();
    assert_eq!(back, toks);
}
