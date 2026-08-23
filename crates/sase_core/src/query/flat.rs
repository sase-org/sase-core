//! Flat (non-boolean) token grammar used when `profile.boolean` is false.
//!
//! Whitespace is conjunction. A leading `-` is profile-gated negation.
//! Commas expand only on repeatable fields. Boolean operators, parentheses,
//! and case-sensitive `c"..."` literals are rejected. Repeated positive
//! values for one field compile as an any-match constraint.

use std::collections::{HashMap, HashSet};

use crate::query::profile::{
    host_duration_bound_direction, CompiledQueryProfile, FieldValueKind,
};
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
    Predicate {
        expr: QueryExprWire,
        spelling: String,
        source: String,
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
            FlatClause::Predicate {
                source, position, ..
            } => {
                tokens.push(predicate_token(&source, position));
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
    Ok(reprint_clauses(profile, &clauses))
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
    let mut seen_fields: HashSet<String> = HashSet::new();
    while let Some(token) = lexer.next_token()? {
        clauses.push(classify_token(token, profile, &mut seen_fields)?);
    }
    Ok(clauses)
}

fn classify_token(
    token: DecodedToken,
    profile: &CompiledQueryProfile,
    seen_fields: &mut HashSet<String>,
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
    if let Some(clause) = classify_predicate(body, negated, &token, profile)? {
        return Ok(clause);
    }
    let colon = unquoted_index(body, body_quoted, b':');
    if let Some(clause) = classify_bare_bool_flag(
        body,
        body_quoted,
        colon,
        negated,
        &token,
        profile,
        seen_fields,
    )? {
        return Ok(clause);
    }
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
    record_single_field(seen_fields, field, token.start)?;
    let mut values = Vec::with_capacity(parts.len());
    for part in &parts {
        values.push(normalize_typed_value(field, part, token.start)?);
    }
    Ok(FlatClause::Field {
        key: field.key.clone(),
        values,
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
                let terms = values
                    .iter()
                    .map(|value| {
                        QueryExprWire::property(key.clone(), value.clone())
                    })
                    .collect();
                operands.push(QueryExprWire::negate(or_of(terms)));
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
            FlatClause::Predicate { expr, .. } => {
                operands.push(expr.clone());
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

fn reprint_clauses(
    profile: &CompiledQueryProfile,
    clauses: &[FlatClause],
) -> String {
    let mut positive_fields: HashMap<String, Vec<String>> = HashMap::new();
    let mut negative_fields: HashMap<String, Vec<String>> = HashMap::new();
    let mut predicates = Vec::new();
    let mut text_tokens = Vec::new();
    for clause in clauses {
        match clause {
            FlatClause::Text {
                value,
                negated,
                quoted,
                ..
            } => {
                let rendered = render_text_value(value, *quoted, profile);
                text_tokens.push(if *negated {
                    format!("-{rendered}")
                } else {
                    rendered
                });
            }
            FlatClause::Field {
                key,
                values,
                negated,
                ..
            } => {
                if *negated {
                    negative_fields
                        .entry(key.clone())
                        .or_default()
                        .extend(values.clone());
                } else {
                    positive_fields
                        .entry(key.clone())
                        .or_default()
                        .extend(values.clone());
                }
            }
            FlatClause::Predicate { spelling, .. } => {
                predicates.push(spelling.clone());
            }
        }
    }

    let mut rendered = Vec::new();
    for field in profile.fields.iter().filter(|field| field.filterable) {
        if let Some(values) = positive_fields.get(&field.key) {
            rendered.push(format!(
                "{}:{}",
                field.key,
                render_field_values(values)
            ));
        }
        if let Some(values) = negative_fields.get(&field.key) {
            rendered.push(format!(
                "-{}:{}",
                field.key,
                render_field_values(values)
            ));
        }
    }
    rendered.extend(predicates);
    rendered.extend(text_tokens);
    rendered.join(" ")
}

fn render_field_values(values: &[String]) -> String {
    values
        .iter()
        .map(|value| quote_value(value, true, false))
        .collect::<Vec<_>>()
        .join(",")
}

fn render_text_value(
    value: &str,
    quoted: bool,
    profile: &CompiledQueryProfile,
) -> String {
    let rendered = quote_value(value, false, quoted);
    if rendered == value && is_filterable_bool_key(value, profile) {
        let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        rendered
    }
}

fn is_filterable_bool_key(value: &str, profile: &CompiledQueryProfile) -> bool {
    profile
        .field(&value.to_ascii_lowercase())
        .is_some_and(|field| {
            field.filterable && field.value_kind == FieldValueKind::Bool
        })
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

fn classify_bare_bool_flag(
    body: &str,
    body_quoted: &[bool],
    colon: Option<usize>,
    negated: bool,
    token: &DecodedToken,
    profile: &CompiledQueryProfile,
    seen_fields: &mut HashSet<String>,
) -> Result<Option<FlatClause>, QueryErrorWire> {
    if token.wholly_quoted
        || colon.is_some()
        || body_quoted.iter().any(|quoted| *quoted)
    {
        return Ok(None);
    }
    let key = body.to_ascii_lowercase();
    let Some(field) = profile.field(&key).filter(|item| item.filterable) else {
        return Ok(None);
    };
    if field.value_kind != FieldValueKind::Bool {
        return Ok(None);
    }
    if negated && !field.negatable {
        return Err(QueryErrorWire::tokenizer(
            format!("{}: may not be negated", field.key),
            token.start,
        ));
    }
    record_single_field(seen_fields, field, token.start)?;
    Ok(Some(FlatClause::Field {
        key: field.key.clone(),
        values: vec!["true".to_string()],
        negated,
        position: token.start as u32,
    }))
}

fn record_single_field(
    seen_fields: &mut HashSet<String>,
    field: &crate::query::profile::QueryFieldSpec,
    position: usize,
) -> Result<(), QueryErrorWire> {
    if field.repeatable {
        return Ok(());
    }
    if !seen_fields.insert(field.key.clone()) {
        return Err(QueryErrorWire::tokenizer(
            format!("{}: may only appear once", field.key),
            position,
        ));
    }
    Ok(())
}

fn classify_predicate(
    body: &str,
    negated: bool,
    token: &DecodedToken,
    profile: &CompiledQueryProfile,
) -> Result<Option<FlatClause>, QueryErrorWire> {
    if negated || token.wholly_quoted {
        return Ok(None);
    }
    let classified = match body {
        "!" | "!!!" if profile.has_predicate("error_suffix") => {
            Some((QueryExprWire::error_suffix(), "!!!"))
        }
        "!!" if profile.has_predicate("error_suffix") => {
            Some((QueryExprWire::negate(QueryExprWire::error_suffix()), "!!"))
        }
        "@" | "@@@" if profile.has_predicate("running_agent") => {
            Some((QueryExprWire::running_agent(), "@@@"))
        }
        "!@" if profile.has_predicate("running_agent") => {
            Some((QueryExprWire::negate(QueryExprWire::running_agent()), "!@"))
        }
        "$" | "$$$" if profile.has_predicate("running_process") => {
            Some((QueryExprWire::running_process(), "$$$"))
        }
        "!$" if profile.has_predicate("running_process") => Some((
            QueryExprWire::negate(QueryExprWire::running_process()),
            "!$",
        )),
        "*" if profile.any_special => Some((any_special_expr(profile), "*")),
        _ => None,
    };
    let Some((expr, spelling)) = classified else {
        return Ok(None);
    };
    Ok(Some(FlatClause::Predicate {
        expr,
        spelling: spelling.to_string(),
        source: body.to_string(),
        position: token.start as u32,
    }))
}

fn any_special_expr(profile: &CompiledQueryProfile) -> QueryExprWire {
    let operands = profile
        .predicates
        .iter()
        .map(|name| match name.as_str() {
            "running_agent" => QueryExprWire::running_agent(),
            "running_process" => QueryExprWire::running_process(),
            _ => QueryExprWire::error_suffix(),
        })
        .collect();
    QueryExprWire::Or { operands }
}

fn predicate_token(source: &str, position: u32) -> QueryTokenWire {
    let kind = match source {
        "!" | "!!!" => QueryTokenKind::ErrorSuffix,
        "!!" => QueryTokenKind::NotErrorSuffix,
        "@" | "@@@" => QueryTokenKind::RunningAgent,
        "!@" => QueryTokenKind::NotRunningAgent,
        "$" | "$$$" => QueryTokenKind::RunningProcess,
        "!$" => QueryTokenKind::NotRunningProcess,
        "*" => QueryTokenKind::AnySpecial,
        _ => QueryTokenKind::String,
    };
    QueryTokenWire {
        kind,
        value: source.to_string(),
        case_sensitive: false,
        position,
        property_key: None,
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

fn normalize_typed_value(
    field: &crate::query::profile::QueryFieldSpec,
    value: &str,
    position: usize,
) -> Result<String, QueryErrorWire> {
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
            Ok(value.to_string())
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
            Ok(value.to_string())
        }
        FieldValueKind::Int => {
            if host_duration_bound_direction(&field.key).is_some() {
                return parse_duration_bound_value(&field.key, value, position);
            }
            if parse_int_literal(value).is_none() {
                return Err(QueryErrorWire::tokenizer(
                    format!(
                        "{}: value '{}' must be an integer",
                        field.key, value
                    ),
                    position,
                ));
            }
            Ok(value.to_string())
        }
        FieldValueKind::String | FieldValueKind::Date => Ok(value.to_string()),
    }
}

fn parse_duration_bound_value(
    key: &str,
    value: &str,
    position: usize,
) -> Result<String, QueryErrorWire> {
    if let Some(seconds) = parse_whole_unit_duration(value) {
        return Ok(seconds.to_string());
    }
    if is_composite_duration(value) {
        return Err(QueryErrorWire::tokenizer(
            format!(
                "{key}: composite durations are not supported; use seconds \
                 or one whole-unit literal such as 90m"
            ),
            position,
        ));
    }
    Err(QueryErrorWire::tokenizer(
        format!("{key}: must be seconds or a whole-unit duration like 5m"),
        position,
    ))
}

fn parse_whole_unit_duration(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == 0 {
        return None;
    }
    let amount: i64 = value[..idx].parse().ok()?;
    let unit = if idx == bytes.len() {
        b's'
    } else if idx + 1 == bytes.len() {
        bytes[idx].to_ascii_lowercase()
    } else {
        return None;
    };
    let multiplier = match unit {
        b's' => 1,
        b'm' => 60,
        b'h' => 60 * 60,
        b'd' => 24 * 60 * 60,
        _ => return None,
    };
    amount.checked_mul(multiplier)
}

fn is_composite_duration(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut idx = 0usize;
    let mut units = 0usize;
    while idx < bytes.len() {
        let start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == start || idx >= bytes.len() {
            return false;
        }
        let unit = bytes[idx].to_ascii_lowercase();
        if !matches!(unit, b's' | b'm' | b'h' | b'd') {
            return false;
        }
        idx += 1;
        units += 1;
    }
    units >= 2
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
