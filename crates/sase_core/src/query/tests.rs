//! Phase 2B Rust unit tests for the query tokenizer/parser/canonicalizer.
//!
//! Mirrors the Python golden corpus (`tests/test_query_tokenizer.py`,
//! `tests/test_query_parser.py`, `tests/test_query_canonicalization.py`,
//! `tests/test_query_property_filters.py`) so behavior parity is enforced
//! without depending on the Python toolchain.

use crate::query::profile::{
    profile_from_parts, CompiledQueryProfile, FieldValueKind, QueryFieldSpec,
    QueryMacroSpec, QuerySigilSpec, HOST_DATE_BOUND_KEYS,
    HOST_DURATION_BOUND_KEYS,
};
use crate::query::row::{QueryFieldValues, QueryPredicateFacts, QueryRow};
use crate::query::types::{
    QueryErrorWire, QueryExprWire, QueryTokenKind, ERROR_SUFFIX_QUERY,
    RUNNING_AGENT_QUERY, RUNNING_PROCESS_QUERY,
};
use crate::query::{
    canonicalize_query, canonicalize_query_with_profile, compile_query,
    compile_query_with_profile, evaluate_query_many_in_corpus, parse_query,
    parse_query_with_profile, patch_query_profile, tokenize_query,
    tokenize_query_with_profile, try_evaluate_query_many_in_corpus,
    QueryCorpus,
};

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

    let toks = tok("9lives");
    assert_eq!(toks[0].kind, QueryTokenKind::String);
    assert_eq!(toks[0].value, "9lives");

    let toks = tok("sase-r8.9.land");
    assert_eq!(toks[0].kind, QueryTokenKind::String);
    assert_eq!(toks[0].value, "sase-r8.9.land");
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
fn tokenize_origin_property() {
    let toks = tok("origin:external");
    assert_eq!(toks[0].kind, QueryTokenKind::Property);
    assert_eq!(toks[0].value, "external");
    assert_eq!(toks[0].property_key.as_deref(), Some("origin"));
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

    let toks = tok("&0b4");
    assert_eq!(toks[0].property_key.as_deref(), Some("name"));
    assert_eq!(toks[0].value, "0b4");

    let toks = tok("&sase-r8.9.land");
    assert_eq!(toks[0].property_key.as_deref(), Some("name"));
    assert_eq!(toks[0].value, "sase-r8.9.land");
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

    let expr = parse("name:sase-r8.9.land");
    match expr {
        QueryExprWire::PropertyMatch { key, value } => {
            assert_eq!(key, "name");
            assert_eq!(value, "sase-r8.9.land");
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
    assert_eq!(canonicalize_query(&parse("&0b4")), "name:0b4");
    assert_eq!(
        canonicalize_query(&parse("&sase-r8.9.land")),
        "name:sase-r8.9.land"
    );
}

#[test]
fn canonical_widened_boolean_value_shapes_round_trip() {
    for (input, canonical) in [
        ("name:sase-r8.9.land", "name:sase-r8.9.land"),
        ("name:0b4", "name:0b4"),
        ("name:001--2", "name:001--2"),
        ("project:research.12", "project:research.12"),
        ("9lives", "\"9lives\""),
    ] {
        assert_eq!(canonicalize_query(&parse(input)), canonical, "{input}");
        assert_eq!(
            canonicalize_query(&parse(canonical)),
            canonical,
            "{canonical}"
        );
    }
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

fn string_field(
    key: &str,
    filterable: bool,
    searchable: bool,
) -> QueryFieldSpec {
    exact_string_field(key, filterable, searchable, false)
}

fn exact_string_field(
    key: &str,
    filterable: bool,
    searchable: bool,
    exact_match: bool,
) -> QueryFieldSpec {
    QueryFieldSpec {
        key: key.to_string(),
        value_kind: FieldValueKind::String,
        filterable,
        searchable,
        repeatable: false,
        negatable: false,
        exact_match,
        static_values: Vec::new(),
        hint: String::new(),
    }
}

fn typed_field(
    key: &str,
    value_kind: FieldValueKind,
    negatable: bool,
) -> QueryFieldSpec {
    QueryFieldSpec {
        key: key.to_string(),
        value_kind,
        filterable: true,
        searchable: false,
        repeatable: false,
        negatable,
        exact_match: false,
        static_values: if value_kind == FieldValueKind::Bool {
            vec!["true".into(), "false".into()]
        } else {
            Vec::new()
        },
        hint: String::new(),
    }
}

fn flags_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "flags",
        false,
        vec![
            typed_field("flag", FieldValueKind::Bool, true),
            typed_field("locked", FieldValueKind::Bool, false),
            QueryFieldSpec {
                key: "title".into(),
                value_kind: FieldValueKind::String,
                filterable: false,
                searchable: true,
                repeatable: false,
                negatable: false,
                exact_match: false,
                static_values: Vec::new(),
                hint: String::new(),
            },
        ],
        vec![],
        vec![],
        false,
        vec![],
    )
    .expect("flags profile")
}

fn bounds_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "bounds",
        false,
        vec![
            typed_field("after", FieldValueKind::Date, false),
            typed_field("before", FieldValueKind::Date, false),
            typed_field("since", FieldValueKind::Date, false),
            typed_field("until", FieldValueKind::Date, false),
            typed_field("created", FieldValueKind::Date, false),
            typed_field("min", FieldValueKind::Int, false),
            typed_field("max", FieldValueKind::Int, false),
            typed_field("exit", FieldValueKind::Int, false),
        ],
        vec![],
        vec![],
        false,
        vec![],
    )
    .expect("bounds profile")
}

fn sidecar_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "stitches",
        false,
        vec![
            typed_field("sidecar", FieldValueKind::Bool, false),
            QueryFieldSpec {
                key: "subject".into(),
                value_kind: FieldValueKind::String,
                filterable: false,
                searchable: true,
                repeatable: false,
                negatable: false,
                exact_match: false,
                static_values: Vec::new(),
                hint: String::new(),
            },
        ],
        vec![],
        vec![],
        false,
        vec![],
    )
    .expect("sidecar profile")
}

fn flat_predicate_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "facts",
        false,
        vec![
            QueryFieldSpec {
                key: "kind".into(),
                value_kind: FieldValueKind::Enum,
                filterable: true,
                searchable: false,
                repeatable: true,
                negatable: true,
                exact_match: false,
                static_values: vec!["note".into(), "doc".into()],
                hint: String::new(),
            },
            QueryFieldSpec {
                key: "title".into(),
                value_kind: FieldValueKind::String,
                filterable: false,
                searchable: true,
                repeatable: false,
                negatable: false,
                exact_match: false,
                static_values: Vec::new(),
                hint: String::new(),
            },
        ],
        vec![],
        vec![
            "error_suffix".into(),
            "running_agent".into(),
            "running_process".into(),
        ],
        true,
        vec![],
    )
    .expect("flat predicate profile")
}

fn notes_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "notes",
        false,
        vec![
            QueryFieldSpec {
                key: "active".into(),
                value_kind: FieldValueKind::Bool,
                filterable: true,
                searchable: false,
                repeatable: false,
                negatable: false,
                exact_match: false,
                static_values: vec!["true".into(), "false".into()],
                hint: String::new(),
            },
            QueryFieldSpec {
                key: "count".into(),
                value_kind: FieldValueKind::Int,
                filterable: true,
                searchable: false,
                repeatable: false,
                negatable: false,
                exact_match: false,
                static_values: Vec::new(),
                hint: String::new(),
            },
            QueryFieldSpec {
                key: "kind".into(),
                value_kind: FieldValueKind::Enum,
                filterable: true,
                searchable: false,
                repeatable: true,
                negatable: true,
                exact_match: false,
                static_values: vec!["note".into(), "doc".into()],
                hint: String::new(),
            },
            QueryFieldSpec {
                key: "title".into(),
                value_kind: FieldValueKind::String,
                filterable: false,
                searchable: true,
                repeatable: false,
                negatable: false,
                exact_match: false,
                static_values: Vec::new(),
                hint: String::new(),
            },
        ],
        vec![],
        vec![],
        false,
        vec![],
    )
    .expect("notes profile")
}

fn boolean_custom_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "custom",
        true,
        vec![
            string_field("label", true, true),
            string_field("owner", true, false),
            string_field("body", false, true),
        ],
        vec![QuerySigilSpec {
            sigil: "+".into(),
            field: "owner".into(),
        }],
        vec![
            "error_suffix".into(),
            "running_agent".into(),
            "running_process".into(),
        ],
        true,
        vec![QueryMacroSpec {
            trigger: "%".into(),
            letter: "x".into(),
            field: "label".into(),
            value: "urgent".into(),
        }],
    )
    .expect("custom boolean profile")
}

fn boolean_value_profile() -> CompiledQueryProfile {
    profile_from_parts(
        "values",
        true,
        vec![
            string_field("name", true, true),
            string_field("family", true, false),
            typed_field("since", FieldValueKind::Date, false),
            typed_field("until", FieldValueKind::Date, false),
            typed_field("min", FieldValueKind::Int, false),
            typed_field("attempt", FieldValueKind::Int, false),
        ],
        vec![QuerySigilSpec {
            sigil: "&".into(),
            field: "name".into(),
        }],
        vec![],
        false,
        vec![],
    )
    .expect("boolean value profile")
}

#[test]
fn tokenize_uses_profile_fields_and_sigils() {
    let profile = boolean_custom_profile();
    let toks =
        tokenize_query_with_profile("+alice %x label:ship", &profile).unwrap();
    assert_eq!(toks[0].property_key.as_deref(), Some("owner"));
    assert_eq!(toks[0].value, "alice");
    assert_eq!(toks[1].property_key.as_deref(), Some("label"));
    assert_eq!(toks[1].value, "urgent");
    assert_eq!(toks[2].property_key.as_deref(), Some("label"));
    assert_eq!(toks[2].value, "ship");
}

#[test]
fn boolean_profile_accepts_widened_values_and_normalizes_typed_literals() {
    let profile = boolean_value_profile();
    for (input, canonical) in [
        ("name:sase-r8.9.land", "name:sase-r8.9.land"),
        ("name:0b4", "name:0b4"),
        ("name:001--2", "name:001--2"),
        ("family:research.12", "family:research.12"),
        ("&0b4", "name:0b4"),
        ("since:2h", "since:2h"),
        ("since:2026-08-01", "since:2026-08-01"),
        ("until:7d", "until:7d"),
        ("min:5m", "min:300"),
        ("attempt:002", "attempt:2"),
        ("9lives", "\"9lives\""),
    ] {
        assert_eq!(
            canonicalize_query_with_profile(input, &profile).unwrap(),
            canonical,
            "{input}"
        );
        assert_eq!(
            canonicalize_query_with_profile(canonical, &profile).unwrap(),
            canonical,
            "{canonical}"
        );
    }
}

#[test]
fn tokenize_rejects_undeclared_property_and_keeps_span() {
    let profile = boolean_custom_profile();
    let err = tokenize_query_with_profile("status:wip", &profile).unwrap_err();
    assert_eq!(err.kind, "tokenizer");
    assert_eq!(err.position, 0);
    assert!(err.message.contains("Unknown property key"), "{err}");
}

#[test]
fn tokenize_rejects_disabled_predicate_and_any_special() {
    let profile = profile_from_parts(
        "plain",
        true,
        vec![string_field("label", true, true)],
        vec![],
        vec![],
        false,
        vec![],
    )
    .unwrap();
    for query in ["@", "$", "*"] {
        let err = tokenize_query_with_profile(query, &profile).unwrap_err();
        assert!(
            err.message.contains("Unexpected character"),
            "{query}: {err}"
        );
    }
    let err = parse_query_with_profile("!!!", &profile).unwrap_err();
    assert!(err.message.contains("Expected"), "{err}");
}

#[test]
fn flat_tokenizer_rejects_boolean_syntax() {
    let profile = notes_profile();
    for query in ["foo AND bar", "(foo)", r#"c"Foo""#] {
        let err = tokenize_query_with_profile(query, &profile).unwrap_err();
        assert_eq!(err.kind, "tokenizer", "{query}");
        assert!(err.message.contains("not enabled"), "{query}: {err}");
    }
}

#[test]
fn flat_negation_and_comma_rules() {
    let profile = notes_profile();
    let expr =
        parse_query_with_profile(r#"kind:note,doc -kind:note hello"#, &profile)
            .unwrap();
    let QueryExprWire::And { operands } = expr else {
        panic!("expected And: {expr:?}");
    };
    assert!(matches!(operands[0], QueryExprWire::Or { .. }));
    assert!(matches!(operands[1], QueryExprWire::Not { .. }));
    assert!(matches!(operands[2], QueryExprWire::StringMatch { .. }));
}

#[test]
fn flat_canonical_groups_fields_then_predicates_then_text() {
    let profile = notes_profile();
    let query = r#"hello kind:note "two words" -kind:doc"#;
    assert_eq!(
        canonicalize_query_with_profile(query, &profile).unwrap(),
        r#"kind:note -kind:doc hello "two words""#
    );
}

#[test]
fn flat_validates_enum_bool_and_int_literals() {
    let profile = notes_profile();
    let enum_err = parse_query_with_profile("kind:task", &profile).unwrap_err();
    assert!(enum_err.message.contains("must be one of"), "{enum_err}");
    assert_eq!(enum_err.position, 0);

    let bool_err =
        parse_query_with_profile("active:maybe", &profile).unwrap_err();
    assert!(bool_err.message.contains("true or false"), "{bool_err}");

    let int_err = parse_query_with_profile("count:1.5", &profile).unwrap_err();
    assert!(int_err.message.contains("integer"), "{int_err}");
}

#[test]
fn invalid_profile_is_structured_error() {
    let err = CompiledQueryProfile::from_wire(&serde_json::json!({
        "pane_id": "",
        "boolean": false,
        "fields": [],
    }))
    .unwrap_err();
    assert_eq!(err.kind, "profile");
    assert!(err.message.contains("pane_id"), "{err}");
}

#[test]
fn digest_mismatch_is_rejected_before_evaluation() {
    let notes = notes_profile();
    let program = compile_query_with_profile("hello", &notes).unwrap();
    let corpus = QueryCorpus::from_rows(
        patch_query_profile(),
        vec![QueryRow::default()],
    );
    let err = try_evaluate_query_many_in_corpus(&program, &corpus).unwrap_err();
    assert_eq!(err.kind, "profile");
    assert!(err.message.contains("digest"), "{err}");
}

#[test]
fn generic_rows_honor_searchable_fields_and_predicates() {
    let profile = boolean_custom_profile();
    let rows = vec![
        QueryRow {
            fields: [
                ("label".into(), QueryFieldValues::from_string("urgent")),
                ("owner".into(), QueryFieldValues::from_string("ada")),
                (
                    "body".into(),
                    QueryFieldValues::from_string("hidden haystack"),
                ),
            ]
            .into_iter()
            .collect(),
            searchable_text: "urgent\nhidden haystack".into(),
            predicates: QueryPredicateFacts {
                error_suffix: true,
                running_agent: false,
                running_process: true,
            },
        },
        QueryRow {
            fields: [("label".into(), QueryFieldValues::from_string("later"))]
                .into_iter()
                .collect(),
            searchable_text: "later".into(),
            predicates: QueryPredicateFacts {
                error_suffix: false,
                running_agent: true,
                running_process: false,
            },
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let matches = |query: &str| {
        let program = compile_query_with_profile(query, &profile).unwrap();
        evaluate_query_many_in_corpus(&program, &corpus)
    };
    assert_eq!(matches("haystack"), vec![true, false]);
    assert_eq!(matches("label:urgent"), vec![true, false]);
    assert_eq!(matches("!!!"), vec![true, false]);
    assert_eq!(matches("@@@"), vec![false, true]);
    assert_eq!(matches("$$$"), vec![true, false]);
    assert_eq!(matches("*"), vec![true, true]);
}

#[test]
fn flat_profile_parses_closed_host_predicates_without_boolean_syntax() {
    let profile = flat_predicate_profile();
    for (query, spelling) in [
        ("!", "!!!"),
        ("!!!", "!!!"),
        ("@", "@@@"),
        ("@@@", "@@@"),
        ("$", "$$$"),
        ("$$$", "$$$"),
        ("*", "*"),
        ("!!", "!!"),
        ("!@", "!@"),
        ("!$", "!$"),
    ] {
        let canonical =
            canonicalize_query_with_profile(query, &profile).unwrap();
        assert_eq!(canonical, spelling, "{query}");
        let expr = parse_query_with_profile(query, &profile).unwrap();
        match query {
            "!" | "!!!" => assert!(matches!(
                expr,
                QueryExprWire::StringMatch {
                    is_error_suffix: true,
                    ..
                }
            )),
            "@" | "@@@" => assert!(matches!(
                expr,
                QueryExprWire::StringMatch {
                    is_running_agent: true,
                    ..
                }
            )),
            "$" | "$$$" => assert!(matches!(
                expr,
                QueryExprWire::StringMatch {
                    is_running_process: true,
                    ..
                }
            )),
            "*" => assert!(matches!(expr, QueryExprWire::Or { .. })),
            "!!" | "!@" | "!$" => {
                assert!(matches!(expr, QueryExprWire::Not { .. }))
            }
            _ => unreachable!(),
        }
    }
    let err = parse_query_with_profile("foo AND bar", &profile).unwrap_err();
    assert!(err.message.contains("not enabled"), "{err}");
}

#[test]
fn flat_predicates_evaluate_absent_facts_as_false() {
    let profile = flat_predicate_profile();
    let rows = vec![
        QueryRow {
            fields: [("kind".into(), QueryFieldValues::from_string("note"))]
                .into_iter()
                .collect(),
            searchable_text: "alpha".into(),
            predicates: QueryPredicateFacts {
                error_suffix: true,
                running_agent: false,
                running_process: false,
            },
        },
        QueryRow {
            fields: [("kind".into(), QueryFieldValues::from_string("doc"))]
                .into_iter()
                .collect(),
            searchable_text: "beta".into(),
            predicates: QueryPredicateFacts::default(),
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let matches = |query: &str| {
        let program = compile_query_with_profile(query, &profile).unwrap();
        evaluate_query_many_in_corpus(&program, &corpus)
    };
    assert_eq!(matches("!"), vec![true, false]);
    assert_eq!(matches("!!"), vec![false, true]);
    assert_eq!(matches("@"), vec![false, false]);
    assert_eq!(matches("*"), vec![true, false]);
    assert_eq!(matches("kind:note !"), vec![true, false]);
}

#[test]
fn flat_repeated_values_are_any_match_and_exclusions_negate() {
    let profile = notes_profile();
    let rows = vec![
        QueryRow {
            fields: [("kind".into(), QueryFieldValues::from_string("note"))]
                .into_iter()
                .collect(),
            searchable_text: "alpha note".into(),
            predicates: QueryPredicateFacts::default(),
        },
        QueryRow {
            fields: [("kind".into(), QueryFieldValues::from_string("doc"))]
                .into_iter()
                .collect(),
            searchable_text: "beta doc".into(),
            predicates: QueryPredicateFacts::default(),
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let program =
        compile_query_with_profile("kind:note,doc -kind:doc", &profile)
            .unwrap();
    assert_eq!(
        evaluate_query_many_in_corpus(&program, &corpus),
        vec![true, false]
    );
}

#[test]
fn sha_field_matches_a_prefix_of_the_stored_value() {
    let profile = profile_from_parts(
        "stitches",
        false,
        vec![string_field("sha", true, false)],
        vec![],
        vec![],
        false,
        vec![],
    )
    .expect("sha profile");
    let rows = vec![
        QueryRow {
            fields: [(
                "sha".into(),
                QueryFieldValues::from_string("abc1234567890"),
            )]
            .into_iter()
            .collect(),
            searchable_text: String::new(),
            predicates: QueryPredicateFacts::default(),
        },
        QueryRow {
            fields: [(
                "sha".into(),
                QueryFieldValues::from_string("fff0000000000"),
            )]
            .into_iter()
            .collect(),
            searchable_text: String::new(),
            predicates: QueryPredicateFacts::default(),
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let program = compile_query_with_profile("sha:abc1234", &profile).unwrap();
    assert_eq!(
        evaluate_query_many_in_corpus(&program, &corpus),
        vec![true, false]
    );
}

#[test]
fn boolean_precedence_works_on_generic_rows() {
    let profile = boolean_custom_profile();
    let rows = vec![
        QueryRow {
            fields: [("label".into(), QueryFieldValues::from_string("ship"))]
                .into_iter()
                .collect(),
            searchable_text: "alpha ship".into(),
            predicates: QueryPredicateFacts::default(),
        },
        QueryRow {
            fields: [("label".into(), QueryFieldValues::from_string("hold"))]
                .into_iter()
                .collect(),
            searchable_text: "beta hold".into(),
            predicates: QueryPredicateFacts::default(),
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let program =
        compile_query_with_profile(r#""alpha" OR "beta" "hold""#, &profile)
            .unwrap();
    assert_eq!(
        evaluate_query_many_in_corpus(&program, &corpus),
        vec![true, true]
    );
}

#[test]
fn generic_corpus_reuses_indexes_across_evaluations() {
    let profile = notes_profile();
    let corpus = QueryCorpus::from_rows(
        &profile,
        vec![QueryRow {
            fields: [("kind".into(), QueryFieldValues::from_string("note"))]
                .into_iter()
                .collect(),
            searchable_text: "alpha".into(),
            predicates: QueryPredicateFacts::default(),
        }],
    );
    let program = compile_query_with_profile("alpha", &profile).unwrap();
    let first = evaluate_query_many_in_corpus(&program, &corpus);
    let second = evaluate_query_many_in_corpus(&program, &corpus);
    assert_eq!(first, second);
    assert_eq!(first, vec![true]);
}

#[test]
fn patch_wrappers_match_explicit_patch_profile() {
    let query = r#""alpha" OR status:Ready"#;
    let via_compat = parse_query(query).unwrap();
    let via_profile =
        parse_query_with_profile(query, patch_query_profile()).unwrap();
    assert_eq!(via_compat, via_profile);
    assert_eq!(
        canonicalize_query(&via_compat),
        canonicalize_query_with_profile(query, patch_query_profile()).unwrap()
    );
    let program = compile_query(query).unwrap();
    assert_eq!(program.profile_digest, patch_query_profile().digest);
}

#[test]
fn host_bound_key_tables_match_python_registry() {
    assert_eq!(
        HOST_DATE_BOUND_KEYS,
        &[
            ("since", ">="),
            ("after", ">="),
            ("until", "<="),
            ("before", "<="),
        ]
    );
    assert_eq!(HOST_DURATION_BOUND_KEYS, &[("min", ">="), ("max", "<=")]);
}

#[test]
fn flat_bare_boolean_flags_canonicalize_to_long_form() {
    let profile = flags_profile();
    assert_eq!(
        canonicalize_query_with_profile("flag", &profile).unwrap(),
        "flag:true"
    );
    assert_eq!(
        canonicalize_query_with_profile("-flag", &profile).unwrap(),
        "-flag:true"
    );
    let toks = tokenize_query_with_profile("flag", &profile).unwrap();
    assert_eq!(toks[0].kind, QueryTokenKind::Property);
    assert_eq!(toks[0].value, "true");
    assert_eq!(toks[0].property_key.as_deref(), Some("flag"));
}

#[test]
fn flat_quoted_boolean_key_remains_free_text() {
    let profile = flags_profile();
    assert_eq!(
        canonicalize_query_with_profile(r#""flag""#, &profile).unwrap(),
        r#""flag""#
    );
    assert_eq!(
        canonicalize_query_with_profile(r#"-"flag""#, &profile).unwrap(),
        r#"-"flag""#
    );
}

#[test]
fn flat_partially_quoted_boolean_key_canonicalizes_as_quoted_text() {
    let profile = flags_profile();
    assert_eq!(
        canonicalize_query_with_profile(r#"fl"ag""#, &profile).unwrap(),
        r#""flag""#
    );
}

#[test]
fn flat_bare_boolean_flags_keep_existing_field_guards() {
    let profile = flags_profile();
    let once = parse_query_with_profile("flag -flag", &profile).unwrap_err();
    assert!(
        once.message.contains("flag: may only appear once"),
        "{once}"
    );
    let locked = parse_query_with_profile("-locked", &profile).unwrap_err();
    assert!(
        locked.message.contains("locked: may not be negated"),
        "{locked}"
    );
}

#[test]
fn stitches_sidecar_bare_token_is_a_boolean_flag() {
    let profile = sidecar_profile();
    assert_eq!(
        canonicalize_query_with_profile("sidecar", &profile).unwrap(),
        "sidecar:true"
    );
    let rows = vec![
        QueryRow {
            fields: [("sidecar".into(), QueryFieldValues::from_string("true"))]
                .into_iter()
                .collect(),
            searchable_text: String::new(),
            predicates: QueryPredicateFacts::default(),
        },
        QueryRow {
            fields: [(
                "sidecar".into(),
                QueryFieldValues::from_string("false"),
            )]
            .into_iter()
            .collect(),
            searchable_text: "sidecar".into(),
            predicates: QueryPredicateFacts::default(),
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let program = compile_query_with_profile("sidecar", &profile).unwrap();
    assert_eq!(
        evaluate_query_many_in_corpus(&program, &corpus),
        vec![true, false]
    );
}

#[test]
fn flat_bare_boolean_flags_evaluate_like_key_true() {
    let profile = flags_profile();
    let rows = vec![
        QueryRow {
            fields: [("flag".into(), QueryFieldValues::from_string("true"))]
                .into_iter()
                .collect(),
            searchable_text: "plain".into(),
            predicates: QueryPredicateFacts::default(),
        },
        QueryRow {
            fields: [("flag".into(), QueryFieldValues::from_string("false"))]
                .into_iter()
                .collect(),
            searchable_text: "flag".into(),
            predicates: QueryPredicateFacts::default(),
        },
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let matches = |query: &str| {
        let program = compile_query_with_profile(query, &profile).unwrap();
        evaluate_query_many_in_corpus(&program, &corpus)
    };
    assert_eq!(matches("flag"), vec![true, false]);
    assert_eq!(matches("-flag"), vec![false, true]);
    assert_eq!(matches(r#""flag""#), vec![false, true]);
    assert_eq!(matches(r#"-"flag""#), vec![true, false]);
}

#[test]
fn flat_date_and_duration_bound_keys_compare_by_host_direction() {
    let profile = bounds_profile();
    let rows = vec![
        bound_row("1999", "299"),
        bound_row("2000", "300"),
        bound_row("2001", "301"),
    ];
    let corpus = QueryCorpus::from_rows(&profile, rows);
    let matches = |query: &str| {
        let program = compile_query_with_profile(query, &profile).unwrap();
        evaluate_query_many_in_corpus(&program, &corpus)
    };
    assert_eq!(matches("after:2000"), vec![false, true, true]);
    assert_eq!(matches("since:2000"), vec![false, true, true]);
    assert_eq!(matches("before:2000"), vec![true, true, false]);
    assert_eq!(matches("until:2000"), vec![true, true, false]);
    assert_eq!(matches("created:2000"), vec![false, true, false]);
    assert_eq!(matches("min:5m"), vec![false, true, true]);
    assert_eq!(matches("max:5m"), vec![true, true, false]);
    assert_eq!(matches("exit:300"), vec![false, true, false]);
}

#[test]
fn flat_duration_bound_values_normalize_canonically() {
    let profile = bounds_profile();
    assert_eq!(
        canonicalize_query_with_profile("min:30s max:2h", &profile).unwrap(),
        "max:7200 min:30"
    );
    assert_eq!(
        canonicalize_query_with_profile("min:1d", &profile).unwrap(),
        "min:86400"
    );
    let composite =
        parse_query_with_profile("min:1h30m", &profile).unwrap_err();
    assert!(
        composite.message.contains("composite durations"),
        "{composite}"
    );
    let not_duration =
        parse_query_with_profile("exit:5m", &profile).unwrap_err();
    assert!(not_duration.message.contains("integer"), "{not_duration}");
}

fn bound_row(epoch: &str, duration: &str) -> QueryRow {
    QueryRow {
        fields: [
            ("after", epoch),
            ("before", epoch),
            ("since", epoch),
            ("until", epoch),
            ("created", epoch),
            ("min", duration),
            ("max", duration),
            ("exit", duration),
        ]
        .into_iter()
        .map(|(key, value)| {
            (key.to_string(), QueryFieldValues::from_string(value))
        })
        .collect(),
        searchable_text: String::new(),
        predicates: QueryPredicateFacts::default(),
    }
}
