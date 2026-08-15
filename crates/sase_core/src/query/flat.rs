//! Flat (non-boolean) token grammar used when `profile.boolean` is false.
//!
//! Whitespace is conjunction. A leading `-` is profile-gated negation.
//! Commas expand only on repeatable fields. Boolean operators, parentheses,
//! and case-sensitive `c"..."` literals are rejected. Repeated positive
//! values for one field compile as an any-match constraint.

use std::collections::HashMap;

use crate::query::profile::{CompiledQueryProfile, FieldValueKind};
use crate::query::types::{
    QueryErrorWire, QueryExprWire, QueryTokenKind, QueryTokenWire,
};

struct FlatLexer<'a> {
    src: &'a [u8],
    pos: usize,
}

#[derive(Debug, Clone)]
struct DecodedToken {
    raw: String,
    value: String,
    quoted: Vec<bool>,
    wholly_quoted: bool,
    start: usize,
}

#[derive(Debug, Clone)]
enum FlatClause {
    Text {
        value: String,
        negated: bool,
        quoted: bool,
        position: u32,
    },
    Field {
        key: String,
        values: Vec<String>,
        negated: bool,
        position: u32,
    },
}

pub fn tokenize_flat(
    query: &str,
    profile: &CompiledQueryProfile,
) -> Result<Vec<QueryTokenWire>, QueryErrorWire> {
    let clauses = collect_clauses(query, profile)?;
    let mut tokens = Vec::new();
    for clause in clauses {
        match clause {
            FlatClause::Text {
                value,
                negated,
                position,
                ..
            } => {
                if negated {
                    tokens.push(not_token(position));
                }
                tokens.push(QueryTokenWire {
                    kind: QueryTokenKind::String,
                    value,
                    case_sensitive: false,
                    position,
                    property_key: None,
                });
            }
            FlatClause::Field {
                key,
                values,
                negated,
                position,
            } => {
                if negated {
                    tokens.push(not_token(position));
                }
                for value in values {
                    tokens.push(QueryTokenWire {
                        kind: QueryTokenKind::Property,
                        value,
                        case_sensitive: false,
                        position,
                        property_key: Some(key.clone()),
                    });
                }
            }
        }
    }
    tokens.push(QueryTokenWire {
        kind: QueryTokenKind::Eof,
        value: String::new(),
        case_sensitive: false,
        position: query.len() as u32,
        property_key: None,
    });
    Ok(tokens)
}

pub fn parse_flat(
    query: &str,
    profile: &CompiledQueryProfile,
) -> Result<QueryExprWire, QueryErrorWire> {
    let clauses = collect_clauses(query, profile)?;
    if clauses.is_empty() {
        return Ok(QueryExprWire::And { operands: vec![] });
    }
    Ok(compile_clauses(&clauses))
}

pub fn canonicalize_flat(
    query: &str,
    profile: &CompiledQueryProfile,
) -> Result<String, QueryErrorWire> {
    let clauses = collect_clauses(query, profile)?;
    Ok(reprint_clauses(&clauses))
}

fn collect_clauses(
    query: &str,
    profile: &CompiledQueryProfile,
) -> Result<Vec<FlatClause>, QueryErrorWire> {
    let mut lexer = FlatLexer {
        src: query.as_bytes(),
        pos: 0,
    };
    let mut clauses = Vec::new();
    let mut seen_positive: HashMap<String, ()> = HashMap::new();
    while let Some(token) = lexer.next_token()? {
        clauses.push(classify_token(token, profile, &mut seen_positive)?);
    }
    Ok(clauses)
}

fn classify_token(
    token: DecodedToken,
    profile: &CompiledQueryProfile,
    seen_positive: &mut HashMap<String, ()>,
) -> Result<FlatClause, QueryErrorWire> {
    let negated = is_negated(&token);
    if negated && !profile.allows_negation() {
        return Err(QueryErrorWire::tokenizer(
            "negation is not enabled for this profile",
            token.start,
        ));
    }
    if looks_like_case_sensitive(&token) {
        return Err(QueryErrorWire::tokenizer(
            "case-sensitive literals are not enabled for this profile",
            token.start,
        ));
    }
    if contains_unquoted_paren(&token) {
        return Err(QueryErrorWire::tokenizer(
            "parentheses are not enabled for this profile",
            token.start,
        ));
    }

    let body = if negated {
        &token.value[1..]
    } else {
        &token.value
    };
    let body_quoted = if negated {
        &token.quoted[1..]
    } else {
        token.quoted.as_slice()
    };
    let colon = unquoted_index(body, body_quoted, b':');
    if token.wholly_quoted || colon.is_none() {
        if is_boolean_keyword(body) && !token.wholly_quoted {
            return Err(QueryErrorWire::tokenizer(
                "Boolean operators are not enabled for this profile",
                token.start,
            ));
        }
        if body.is_empty() {
            return Err(QueryErrorWire::tokenizer(
                "Free-text terms must not be empty",
                token.start,
            ));
        }
        return Ok(FlatClause::Text {
            value: body.to_string(),
            negated,
            quoted: token.wholly_quoted,
            position: token.start as u32,
        });
    }

    let colon = colon.expect("colon present");
    let key = body[..colon].to_ascii_lowercase();
    let field = profile.field(&key).filter(|item| item.filterable);
    let Some(field) = field else {
        return Err(QueryErrorWire::tokenizer(
            unknown_key_message(&key, profile),
            token.start,
        ));
    };
    if negated && !field.negatable {
        return Err(QueryErrorWire::tokenizer(
            format!("{}: may not be negated", field.key),
            token.start,
        ));
    }

    let value = &body[colon + 1..];
    let value_quoted = &body_quoted[colon + 1..];
    if value.is_empty() {
        return Err(QueryErrorWire::tokenizer(
            format!("{}: requires a value", field.key),
            token.start,
        ));
    }
    let parts = split_unquoted(value, value_quoted, b',');
    if parts.iter().any(|part| part.is_empty()) {
        return Err(QueryErrorWire::tokenizer(
            format!("{}: contains an empty value", field.key),
            token.start,
        ));
    }
    if !field.repeatable && parts.len() > 1 {
        return Err(QueryErrorWire::tokenizer(
            format!("{}: does not accept comma-separated values", field.key),
            token.start,
        ));
    }
    if !field.repeatable && !negated {
        if seen_positive.contains_key(&field.key) {
            return Err(QueryErrorWire::tokenizer(
                format!("{}: may only appear once", field.key),
                token.start,
            ));
        }
        seen_positive.insert(field.key.clone(), ());
    }
    for part in &parts {
        validate_typed_value(field, part, token.start)?;
    }
    Ok(FlatClause::Field {
        key: field.key.clone(),
        values: parts,
        negated,
        position: token.start as u32,
    })
}

fn compile_clauses(clauses: &[FlatClause]) -> QueryExprWire {
    let mut grouped: Vec<(String, Vec<QueryExprWire>)> = Vec::new();
    for clause in clauses {
        if let FlatClause::Field {
            key,
            values,
            negated: false,
            ..
        } = clause
        {
            let extras = values.iter().map(|value| {
                QueryExprWire::property(key.clone(), value.clone())
            });
            if let Some(existing) =
                grouped.iter_mut().find(|(seen, _)| seen == key)
            {
                existing.1.extend(extras);
            } else {
                grouped.push((key.clone(), extras.collect()));
            }
        }
    }

    let mut emitted = HashMap::new();
    let mut operands = Vec::new();
    for clause in clauses {
        match clause {
            FlatClause::Field {
                key,
                values,
                negated: true,
                ..
            } => {
                for value in values {
                    operands.push(QueryExprWire::negate(
                        QueryExprWire::property(key.clone(), value.clone()),
                    ));
                }
            }
            FlatClause::Field {
                key,
                negated: false,
                ..
            } => {
                if emitted.insert(key.clone(), ()).is_none() {
                    if let Some((_, values)) =
                        grouped.iter().find(|(seen, _)| seen == key)
                    {
                        operands.push(or_of(values.clone()));
                    }
                }
            }
            FlatClause::Text { value, negated, .. } => {
                let expr = QueryExprWire::string_match(value.clone(), false);
                operands.push(if *negated {
                    QueryExprWire::negate(expr)
                } else {
                    expr
                });
            }
        }
    }
    if operands.len() == 1 {
        operands.pop().unwrap()
    } else {
        QueryExprWire::And { operands }
    }
}

fn or_of(mut operands: Vec<QueryExprWire>) -> QueryExprWire {
    if operands.len() == 1 {
        operands.pop().unwrap()
    } else {
        QueryExprWire::Or { operands }
    }
}

fn reprint_clauses(clauses: &[FlatClause]) -> String {
    clauses
        .iter()
        .map(|clause| match clause {
            FlatClause::Text {
                value,
                negated,
                quoted,
                ..
            } => {
                let rendered = quote_value(value, false, *quoted);
                if *negated {
                    format!("-{rendered}")
                } else {
                    rendered
                }
            }
            FlatClause::Field {
                key,
                values,
                negated,
                ..
            } => {
                let joined = values
                    .iter()
                    .map(|value| quote_value(value, true, false))
                    .collect::<Vec<_>>()
                    .join(",");
                if *negated {
                    format!("-{key}:{joined}")
                } else {
                    format!("{key}:{joined}")
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn quote_value(value: &str, keyed: bool, force: bool) -> String {
    let needs_quotes = force
        || value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_ascii_whitespace() || ch == '"')
        || (keyed && value.contains(','))
        || (!keyed && (value.contains(':') || value.starts_with('-')));
    if !needs_quotes {
        return value.to_string();
    }
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

impl<'a> FlatLexer<'a> {
    fn next_token(&mut self) -> Result<Option<DecodedToken>, QueryErrorWire> {
        self.skip_whitespace();
        if self.pos >= self.src.len() {
            return Ok(None);
        }
        let start = self.pos;
        let mut value = String::new();
        let mut quoted = Vec::new();
        let mut in_quotes = false;
        let mut saw_quote = false;
        let mut saw_unquoted = false;
        while self.pos < self.src.len() {
            let byte = self.src[self.pos];
            if !in_quotes && is_ws(byte) {
                break;
            }
            if byte == b'"' {
                saw_quote = true;
                in_quotes = !in_quotes;
                self.pos += 1;
                continue;
            }
            if in_quotes && byte == b'\\' && self.pos + 1 < self.src.len() {
                let escaped = self.src[self.pos + 1];
                if escaped == b'"' || escaped == b'\\' {
                    value.push(escaped as char);
                    quoted.push(true);
                    self.pos += 2;
                    continue;
                }
            }
            let char_len = utf8_char_len(byte);
            let end = (self.pos + char_len).min(self.src.len());
            let chunk = std::str::from_utf8(&self.src[self.pos..end]).unwrap();
            value.push_str(chunk);
            for _ in chunk.chars() {
                quoted.push(in_quotes);
            }
            if !in_quotes {
                saw_unquoted = true;
            }
            self.pos = end;
        }
        if in_quotes {
            return Err(QueryErrorWire::tokenizer(
                "Unterminated double quote",
                start,
            ));
        }
        Ok(Some(DecodedToken {
            raw: std::str::from_utf8(&self.src[start..self.pos])
                .unwrap()
                .to_string(),
            value,
            quoted,
            wholly_quoted: saw_quote && !saw_unquoted,
            start,
        }))
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.src.len() && is_ws(self.src[self.pos]) {
            self.pos += 1;
        }
    }
}

fn is_negated(token: &DecodedToken) -> bool {
    token
        .value
        .as_bytes()
        .first()
        .is_some_and(|byte| *byte == b'-')
        && token.quoted.first().is_some_and(|quoted| !*quoted)
        && !token.wholly_quoted
}

fn looks_like_case_sensitive(token: &DecodedToken) -> bool {
    let bytes = token.raw.as_bytes();
    bytes.len() >= 2 && bytes[0] == b'c' && bytes[1] == b'"'
}

fn contains_unquoted_paren(token: &DecodedToken) -> bool {
    token
        .value
        .bytes()
        .zip(token.quoted.iter())
        .any(|(byte, quoted)| !*quoted && (byte == b'(' || byte == b')'))
}

fn is_boolean_keyword(value: &str) -> bool {
    matches!(value.to_ascii_uppercase().as_str(), "AND" | "OR" | "NOT")
}

fn unknown_key_message(key: &str, profile: &CompiledQueryProfile) -> String {
    let keys = profile.filterable_keys();
    if keys.is_empty() {
        format!("Unknown filter key '{key}'")
    } else {
        format!(
            "Unknown filter key '{key}' (valid keys: {})",
            keys.join(", ")
        )
    }
}

fn validate_typed_value(
    field: &crate::query::profile::QueryFieldSpec,
    value: &str,
    position: usize,
) -> Result<(), QueryErrorWire> {
    match field.value_kind {
        FieldValueKind::Enum => {
            let allowed = &field.static_values;
            let ok =
                allowed.iter().any(|item| item.eq_ignore_ascii_case(value));
            if !ok {
                return Err(QueryErrorWire::tokenizer(
                    format!(
                        "{}: value '{}' must be one of {}",
                        field.key,
                        value,
                        allowed.join(", ")
                    ),
                    position,
                ));
            }
        }
        FieldValueKind::Bool => {
            if parse_bool_literal(value).is_none() {
                return Err(QueryErrorWire::tokenizer(
                    format!(
                        "{}: value '{}' must be true or false",
                        field.key, value
                    ),
                    position,
                ));
            }
        }
        FieldValueKind::Int => {
            if parse_int_literal(value).is_none() {
                return Err(QueryErrorWire::tokenizer(
                    format!(
                        "{}: value '{}' must be an integer",
                        field.key, value
                    ),
                    position,
                ));
            }
        }
        FieldValueKind::String | FieldValueKind::Date => {}
    }
    Ok(())
}

pub fn parse_bool_literal(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" | "1" => Some(true),
        "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

pub fn parse_int_literal(value: &str) -> Option<i64> {
    value.parse::<i64>().ok()
}

fn not_token(position: u32) -> QueryTokenWire {
    QueryTokenWire {
        kind: QueryTokenKind::Not,
        value: "-".to_string(),
        case_sensitive: false,
        position,
        property_key: None,
    }
}

fn is_ws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

fn utf8_char_len(lead: u8) -> usize {
    if lead < 0xC0 {
        1
    } else if lead < 0xE0 {
        2
    } else if lead < 0xF0 {
        3
    } else {
        4
    }
}

fn unquoted_index(value: &str, quoted: &[bool], needle: u8) -> Option<usize> {
    for (idx, byte) in value.as_bytes().iter().enumerate() {
        if *byte == needle && quoted.get(idx).is_some_and(|flag| !*flag) {
            return Some(idx);
        }
    }
    None
}

fn split_unquoted(value: &str, quoted: &[bool], separator: u8) -> Vec<String> {
    let bytes = value.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    for (idx, byte) in bytes.iter().enumerate() {
        if *byte == separator && quoted.get(idx).is_some_and(|flag| !*flag) {
            parts
                .push(String::from_utf8_lossy(&bytes[start..idx]).into_owned());
            start = idx + 1;
        }
    }
    parts.push(String::from_utf8_lossy(&bytes[start..]).into_owned());
    parts
}
