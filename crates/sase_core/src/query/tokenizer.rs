//! Pure-Rust port of `src/sase/ace/query/tokenizer.py`.
//!
//! The tokenizer walks the query byte-by-byte; all special characters are
//! ASCII so byte indexing is safe. Multi-byte UTF-8 inside quoted strings or
//! bare words is preserved as-is (Python's tokenizer also stores those
//! verbatim in the resulting `Token.value`).

use crate::query::profile::{
    join_shorthand_list, patch_query_profile, CompiledQueryProfile,
};
use crate::query::types::{
    QueryErrorWire, QueryTokenKind, QueryTokenWire, VALID_PROPERTY_KEYS,
};

fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

fn is_bare_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

fn is_alpha_or_underscore(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

struct Tokenizer<'a> {
    src: &'a [u8],
    pos: usize,
    out: Vec<QueryTokenWire>,
    profile: &'a CompiledQueryProfile,
}

impl<'a> Tokenizer<'a> {
    fn new(src: &'a str, profile: &'a CompiledQueryProfile) -> Self {
        Tokenizer {
            src: src.as_bytes(),
            pos: 0,
            out: Vec::new(),
            profile,
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.src.len() && is_ws(self.src[self.pos]) {
            self.pos += 1;
        }
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.src.get(self.pos + offset).copied()
    }

    fn starts_with(&self, needle: &[u8]) -> bool {
        self.src
            .get(self.pos..self.pos + needle.len())
            .map(|s| s == needle)
            .unwrap_or(false)
    }

    fn standalone_after(&self, n: usize) -> bool {
        // "Standalone" means: at end-of-input or followed by whitespace.
        match self.peek_at(n) {
            None => true,
            Some(b) => is_ws(b),
        }
    }

    /// Parse a quoted string starting at `self.pos`, where `self.src[self.pos]`
    /// is the opening `"`. Returns the unescaped value and advances `self.pos`
    /// past the closing quote.
    fn parse_quoted(&mut self) -> Result<String, QueryErrorWire> {
        let start = self.pos;
        self.pos += 1; // skip opening quote
        let mut value = String::new();
        while self.pos < self.src.len() {
            let b = self.src[self.pos];
            if b == b'"' {
                self.pos += 1;
                return Ok(value);
            }
            if b == b'\\' {
                if self.pos + 1 >= self.src.len() {
                    return Err(QueryErrorWire::tokenizer(
                        "Unterminated escape sequence",
                        self.pos,
                    ));
                }
                let next = self.src[self.pos + 1];
                let ch = match next {
                    b'\\' => '\\',
                    b'"' => '"',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    other => {
                        return Err(QueryErrorWire::tokenizer(
                            format!(
                                "Invalid escape sequence: \\{}",
                                other as char
                            ),
                            self.pos,
                        ));
                    }
                };
                value.push(ch);
                self.pos += 2;
            } else {
                // Preserve UTF-8 multi-byte sequences verbatim. Find the
                // length of this UTF-8 character via the leading byte.
                let char_len = utf8_char_len(b);
                let end = (self.pos + char_len).min(self.src.len());
                // SAFETY: src came from a &str so it is valid UTF-8.
                value.push_str(
                    std::str::from_utf8(&self.src[self.pos..end]).unwrap(),
                );
                self.pos = end;
            }
        }
        Err(QueryErrorWire::tokenizer("Unterminated string", start))
    }

    /// Parse a property value: quoted string or bare word.
    fn parse_property_value(&mut self) -> Result<String, QueryErrorWire> {
        if self.pos >= self.src.len() {
            return Err(QueryErrorWire::tokenizer(
                "Expected property value",
                self.pos,
            ));
        }
        if self.src[self.pos] == b'"' {
            return self.parse_quoted();
        }
        let start = self.pos;
        while self.pos < self.src.len() && is_bare_word_byte(self.src[self.pos])
        {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(QueryErrorWire::tokenizer(
                "Expected property value",
                self.pos,
            ));
        }
        Ok(std::str::from_utf8(&self.src[start..self.pos])
            .unwrap()
            .to_string())
    }

    fn emit(
        &mut self,
        kind: QueryTokenKind,
        value: impl Into<String>,
        position: usize,
    ) {
        self.out.push(QueryTokenWire {
            kind,
            value: value.into(),
            case_sensitive: false,
            position: position as u32,
            property_key: None,
        });
    }

    fn emit_string(
        &mut self,
        value: String,
        case_sensitive: bool,
        position: usize,
    ) {
        self.out.push(QueryTokenWire {
            kind: QueryTokenKind::String,
            value,
            case_sensitive,
            position: position as u32,
            property_key: None,
        });
    }

    fn emit_property(&mut self, key: &str, value: String, position: usize) {
        self.out.push(QueryTokenWire {
            kind: QueryTokenKind::Property,
            value,
            case_sensitive: false,
            position: position as u32,
            property_key: Some(key.to_string()),
        });
    }

    fn run(mut self) -> Result<Vec<QueryTokenWire>, QueryErrorWire> {
        loop {
            self.skip_whitespace();
            if self.pos >= self.src.len() {
                break;
            }
            let start = self.pos;
            let b = self.src[self.pos];

            // Case-sensitive string prefix: c"..."
            if b == b'c' && self.peek_at(1) == Some(b'"') {
                self.pos += 1; // skip 'c'
                let value = self.parse_quoted()?;
                self.emit_string(value, true, start);
                continue;
            }
            // Quoted string
            if b == b'"' {
                let value = self.parse_quoted()?;
                self.emit_string(value, false, start);
                continue;
            }
            // ! family
            if b == b'!' {
                if self.profile.has_predicate("error_suffix")
                    && self.starts_with(b"!!!")
                {
                    self.emit(QueryTokenKind::ErrorSuffix, "!!!", start);
                    self.pos += 3;
                    continue;
                }
                if self.profile.has_predicate("error_suffix")
                    && self.starts_with(b"!!")
                    && self.standalone_after(2)
                {
                    self.emit(QueryTokenKind::NotErrorSuffix, "!!", start);
                    self.pos += 2;
                    continue;
                }
                if self.profile.has_predicate("running_agent")
                    && self.starts_with(b"!@")
                    && self.standalone_after(2)
                {
                    self.emit(QueryTokenKind::NotRunningAgent, "!@", start);
                    self.pos += 2;
                    continue;
                }
                if self.profile.has_predicate("running_process")
                    && self.starts_with(b"!$")
                    && self.standalone_after(2)
                {
                    self.emit(QueryTokenKind::NotRunningProcess, "!$", start);
                    self.pos += 2;
                    continue;
                }
                if self.profile.has_predicate("error_suffix")
                    && self.standalone_after(1)
                {
                    // Standalone ! → ERROR_SUFFIX (transforms to !!!)
                    self.emit(QueryTokenKind::ErrorSuffix, "!", start);
                    self.pos += 1;
                    continue;
                }
                // Regular NOT: !"foo"
                self.emit(QueryTokenKind::Not, "!", start);
                self.pos += 1;
                continue;
            }
            // @ family
            if b == b'@' {
                if self.profile.has_predicate("running_agent") {
                    if self.starts_with(b"@@@") {
                        self.emit(QueryTokenKind::RunningAgent, "@@@", start);
                        self.pos += 3;
                        continue;
                    }
                    if self.standalone_after(1) {
                        self.emit(QueryTokenKind::RunningAgent, "@", start);
                        self.pos += 1;
                        continue;
                    }
                }
                return Err(QueryErrorWire::tokenizer(
                    format!("Unexpected character: {}", b as char),
                    start,
                ));
            }
            // $ family
            if b == b'$' {
                if self.profile.has_predicate("running_process") {
                    if self.starts_with(b"$$$") {
                        self.emit(QueryTokenKind::RunningProcess, "$$$", start);
                        self.pos += 3;
                        continue;
                    }
                    if self.standalone_after(1) {
                        self.emit(QueryTokenKind::RunningProcess, "$", start);
                        self.pos += 1;
                        continue;
                    }
                }
                return Err(QueryErrorWire::tokenizer(
                    format!("Unexpected character: {}", b as char),
                    start,
                ));
            }
            // * (any-special)
            if b == b'*' {
                if self.profile.any_special && self.standalone_after(1) {
                    self.emit(QueryTokenKind::AnySpecial, "*", start);
                    self.pos += 1;
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    format!("Unexpected character: {}", b as char),
                    start,
                ));
            }
            // Parens
            if b == b'(' {
                self.emit(QueryTokenKind::Lparen, "(", start);
                self.pos += 1;
                continue;
            }
            if b == b')' {
                self.emit(QueryTokenKind::Rparen, ")", start);
                self.pos += 1;
                continue;
            }
            // Macro shorthand, e.g. %d
            if let Some(field_value) = self.try_macro(b as char, start)? {
                let (field, value) = field_value;
                self.emit_property(&field, value, start);
                continue;
            }
            // Field sigil, e.g. +project
            if let Some(field) = self.profile.sigil_field(b as char) {
                let field = field.to_string();
                self.pos += 1;
                if self.peek_at(0).map(is_alpha_or_underscore).unwrap_or(false)
                {
                    let value = self.parse_property_value()?;
                    self.emit_property(&field, value, start);
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    sigil_value_error(b as char, &field),
                    start,
                ));
            }
            // Bare word, keyword, or property filter (key:value)
            if b.is_ascii_alphabetic() || b == b'_' {
                let word_start = self.pos;
                while self.pos < self.src.len()
                    && is_bare_word_byte(self.src[self.pos])
                {
                    self.pos += 1;
                }
                let word = std::str::from_utf8(&self.src[word_start..self.pos])
                    .unwrap();
                let upper = word.to_ascii_uppercase();
                let lower = word.to_ascii_lowercase();
                if upper == "AND" {
                    self.emit(
                        QueryTokenKind::And,
                        word.to_string(),
                        word_start,
                    );
                    continue;
                }
                if upper == "OR" {
                    self.emit(QueryTokenKind::Or, word.to_string(), word_start);
                    continue;
                }
                if upper == "NOT" {
                    self.emit(
                        QueryTokenKind::Not,
                        word.to_string(),
                        word_start,
                    );
                    continue;
                }
                // property filter syntax
                if self.peek_at(0) == Some(b':') {
                    if let Some(field) = self.profile.field(&lower) {
                        if field.filterable {
                            let key = field.key.clone();
                            self.pos += 1; // skip ':'
                            let value = self.parse_property_value()?;
                            self.emit_property(&key, value, word_start);
                            continue;
                        }
                    }
                    return Err(QueryErrorWire::tokenizer(
                        unknown_property_message(word, self.profile),
                        word_start,
                    ));
                }
                // bare word string match (case-insensitive)
                self.emit_string(word.to_string(), false, word_start);
                continue;
            }

            return Err(QueryErrorWire::tokenizer(
                format!("Unexpected character: {}", b as char),
                start,
            ));
        }

        self.out.push(QueryTokenWire {
            kind: QueryTokenKind::Eof,
            value: String::new(),
            case_sensitive: false,
            position: self.pos as u32,
            property_key: None,
        });
        Ok(self.out)
    }

    fn try_macro(
        &mut self,
        trigger: char,
        start: usize,
    ) -> Result<Option<(String, String)>, QueryErrorWire> {
        let macros = self.profile.macros_for_trigger(trigger);
        if macros.is_empty() {
            return Ok(None);
        }
        self.pos += 1;
        let next = self.peek_at(0).map(|byte| byte as char);
        if let Some(letter) = next {
            if let Some((field, value)) =
                self.profile.macro_target(trigger, letter)
            {
                self.pos += 1;
                return Ok(Some((field.to_string(), value.to_string())));
            }
        }
        Err(QueryErrorWire::tokenizer(
            invalid_macro_message(trigger, self.profile),
            start,
        ))
    }
}

/// UTF-8 leading-byte length. Defaults to 1 for invalid leading or
/// continuation bytes (callers only invoke this on bytes from a valid `&str`).
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

/// Tokenize a query string. Equivalent to Python's `tokenize(query)` and
/// returns the same sequence of tokens, terminated with an `Eof` token.
pub fn tokenize_query(
    query: &str,
) -> Result<Vec<QueryTokenWire>, QueryErrorWire> {
    tokenize_query_with_profile(query, patch_query_profile())
}

/// Tokenize `query` against a compiled profile.
///
/// Boolean profiles use the Patch-compatible punctuation grammar with
/// profile-selected keys, sigils, macros, and predicates. Flat profiles
/// are tokenized by [`crate::query::flat::tokenize_flat`].
pub fn tokenize_query_with_profile(
    query: &str,
    profile: &CompiledQueryProfile,
) -> Result<Vec<QueryTokenWire>, QueryErrorWire> {
    if profile.boolean {
        Tokenizer::new(query, profile).run()
    } else {
        crate::query::flat::tokenize_flat(query, profile)
    }
}

fn unknown_property_message(
    word: &str,
    profile: &CompiledQueryProfile,
) -> String {
    let keys = profile.filterable_keys();
    if keys.as_slice() == VALID_PROPERTY_KEYS {
        format!(
            "Unknown property key: {word} (valid keys: status, project, ancestor, name, sibling, origin)"
        )
    } else if keys.is_empty() {
        format!("Unknown property key: {word}")
    } else {
        format!(
            "Unknown property key: {word} (valid keys: {})",
            keys.join(", ")
        )
    }
}

fn sigil_value_error(sigil: char, field: &str) -> String {
    if field == "name" {
        format!("Expected name after '{sigil}'")
    } else {
        format!("Expected {field} name after '{sigil}'")
    }
}

fn invalid_macro_message(
    trigger: char,
    profile: &CompiledQueryProfile,
) -> String {
    let macros = profile.macros_for_trigger(trigger);
    if macros.is_empty() {
        return format!("Invalid {trigger} shorthand");
    }
    let field = macros[0].field.as_str();
    let tokens: Vec<String> = macros
        .iter()
        .map(|item| format!("{trigger}{}", item.letter))
        .collect();
    format!(
        "Invalid {field} shorthand (use {})",
        join_shorthand_list(&tokens)
    )
}
