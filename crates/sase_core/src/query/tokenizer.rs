//! Pure-Rust port of `src/sase/ace/query/tokenizer.py`.
//!
//! The tokenizer walks the query byte-by-byte; all special characters are
//! ASCII so byte indexing is safe. Multi-byte UTF-8 inside quoted strings or
//! bare words is preserved as-is (Python's tokenizer also stores those
//! verbatim in the resulting `Token.value`).

use crate::query::types::{
    QueryErrorWire, QueryTokenKind, QueryTokenWire, VALID_PROPERTY_KEYS,
};

/// Status shorthand mappings (Python: `STATUS_SHORTHANDS`).
fn status_shorthand(c: char) -> Option<&'static str> {
    match c.to_ascii_lowercase() {
        'd' => Some("DRAFT"),
        'm' => Some("MAILED"),
        'r' => Some("REVERTED"),
        's' => Some("SUBMITTED"),
        'w' => Some("WIP"),
        'y' => Some("READY"),
        _ => None,
    }
}

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
}

impl<'a> Tokenizer<'a> {
    fn new(src: &'a str) -> Self {
        Tokenizer {
            src: src.as_bytes(),
            pos: 0,
            out: Vec::new(),
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
                if self.starts_with(b"!!!") {
                    self.emit(QueryTokenKind::ErrorSuffix, "!!!", start);
                    self.pos += 3;
                    continue;
                }
                if self.starts_with(b"!!") && self.standalone_after(2) {
                    self.emit(QueryTokenKind::NotErrorSuffix, "!!", start);
                    self.pos += 2;
                    continue;
                }
                if self.starts_with(b"!@") && self.standalone_after(2) {
                    self.emit(QueryTokenKind::NotRunningAgent, "!@", start);
                    self.pos += 2;
                    continue;
                }
                if self.starts_with(b"!$") && self.standalone_after(2) {
                    self.emit(QueryTokenKind::NotRunningProcess, "!$", start);
                    self.pos += 2;
                    continue;
                }
                if self.standalone_after(1) {
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
                return Err(QueryErrorWire::tokenizer(
                    format!("Unexpected character: {}", b as char),
                    start,
                ));
            }
            // $ family
            if b == b'$' {
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
                return Err(QueryErrorWire::tokenizer(
                    format!("Unexpected character: {}", b as char),
                    start,
                ));
            }
            // * (any-special)
            if b == b'*' {
                if self.standalone_after(1) {
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
            // %X status shorthand
            if b == b'%' {
                self.pos += 1;
                let next = self.peek_at(0);
                let mapped = next.and_then(|c| status_shorthand(c as char));
                if let Some(value) = mapped {
                    self.pos += 1;
                    self.emit_property("status", value.to_string(), start);
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    "Invalid status shorthand (use %d, %m, %r, %s, %w, or %y)",
                    start,
                ));
            }
            // +project
            if b == b'+' {
                self.pos += 1;
                if self.peek_at(0).map(is_alpha_or_underscore).unwrap_or(false)
                {
                    let value = self.parse_property_value()?;
                    self.emit_property("project", value, start);
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    "Expected project name after '+'",
                    start,
                ));
            }
            // ^ancestor
            if b == b'^' {
                self.pos += 1;
                if self.peek_at(0).map(is_alpha_or_underscore).unwrap_or(false)
                {
                    let value = self.parse_property_value()?;
                    self.emit_property("ancestor", value, start);
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    "Expected ancestor name after '^'",
                    start,
                ));
            }
            // ~sibling
            if b == b'~' {
                self.pos += 1;
                if self.peek_at(0).map(is_alpha_or_underscore).unwrap_or(false)
                {
                    let value = self.parse_property_value()?;
                    self.emit_property("sibling", value, start);
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    "Expected sibling name after '~'",
                    start,
                ));
            }
            // &name
            if b == b'&' {
                self.pos += 1;
                if self.peek_at(0).map(is_alpha_or_underscore).unwrap_or(false)
                {
                    let value = self.parse_property_value()?;
                    self.emit_property("name", value, start);
                    continue;
                }
                return Err(QueryErrorWire::tokenizer(
                    "Expected name after '&'",
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
                    if VALID_PROPERTY_KEYS.contains(&lower.as_str()) {
                        let key = lower.clone();
                        self.pos += 1; // skip ':'
                        let value = self.parse_property_value()?;
                        self.emit_property(&key, value, word_start);
                        continue;
                    }
                    return Err(QueryErrorWire::tokenizer(
                        format!(
                            "Unknown property key: {} (valid keys: status, project, ancestor, name, sibling)",
                            word
                        ),
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
    Tokenizer::new(query).run()
}
