use crate::command_line::wire::LineTokenWire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawToken {
    pub text: String,
    pub start: usize,
    pub end: usize,
    pub quoted: bool,
    pub unterminated: bool,
}

pub fn lex_line(line: &str) -> Vec<RawToken> {
    let chars: Vec<char> = line.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    let len = chars.len();
    while index < len {
        while index < len && is_shell_whitespace(chars[index]) {
            index += 1;
        }
        if index >= len {
            break;
        }
        let start = index;
        let mut text = String::new();
        let mut quoted = false;
        let mut unterminated = false;
        let mut closed_any = false;
        while index < len && !is_shell_whitespace(chars[index]) {
            let c = chars[index];
            if c == '\'' {
                quoted = true;
                index += 1;
                let mut closed = false;
                while index < len {
                    if chars[index] == '\'' {
                        closed = true;
                        index += 1;
                        break;
                    }
                    text.push(chars[index]);
                    index += 1;
                }
                if !closed {
                    unterminated = true;
                    break;
                }
                closed_any = true;
            } else if c == '"' {
                quoted = true;
                index += 1;
                let mut closed = false;
                while index < len {
                    let d = chars[index];
                    if d == '"' {
                        closed = true;
                        index += 1;
                        break;
                    }
                    if d == '\\' && index + 1 < len {
                        let next = chars[index + 1];
                        if next == '\\'
                            || next == '"'
                            || next == '$'
                            || next == '`'
                        {
                            text.push(next);
                            index += 2;
                            continue;
                        }
                        if next == '\n' {
                            index += 2;
                            continue;
                        }
                        text.push(d);
                        index += 1;
                        continue;
                    }
                    if d == '\n' {
                        text.push(d);
                        index += 1;
                        continue;
                    }
                    text.push(d);
                    index += 1;
                }
                if !closed {
                    unterminated = true;
                    break;
                }
                closed_any = true;
            } else if c == '\\' {
                if index + 1 < len {
                    if chars[index + 1] == '\n' {
                        index += 2;
                    } else {
                        text.push(chars[index + 1]);
                        index += 2;
                    }
                } else {
                    index += 1;
                }
            } else {
                text.push(c);
                index += 1;
            }
        }
        let _ = closed_any;
        tokens.push(RawToken {
            text,
            start,
            end: index,
            quoted,
            unterminated,
        });
        if unterminated {
            break;
        }
    }
    tokens
}

pub fn unquoted_prefix(line: &str, start: usize, cursor: usize) -> String {
    let chars: Vec<char> = line.chars().collect();
    let end = cursor.min(chars.len());
    let begin = start.min(end);
    let slice: String = chars[begin..end].iter().collect();
    unquote_slice(&slice)
}

fn unquote_slice(slice: &str) -> String {
    let chars: Vec<char> = slice.chars().collect();
    let mut out = String::new();
    let mut index = 0;
    while index < chars.len() {
        let c = chars[index];
        if c == '\'' {
            index += 1;
            while index < chars.len() && chars[index] != '\'' {
                out.push(chars[index]);
                index += 1;
            }
            if index < chars.len() {
                index += 1;
            }
        } else if c == '"' {
            index += 1;
            while index < chars.len() && chars[index] != '"' {
                let d = chars[index];
                if d == '\\'
                    && index + 1 < chars.len()
                    && matches!(chars[index + 1], '\\' | '"' | '$' | '`')
                {
                    out.push(chars[index + 1]);
                    index += 2;
                    continue;
                }
                if d == '\\'
                    && index + 1 < chars.len()
                    && chars[index + 1] == '\n'
                {
                    index += 2;
                    continue;
                }
                out.push(d);
                index += 1;
            }
            if index < chars.len() {
                index += 1;
            }
        } else if c == '\\' {
            if index + 1 < chars.len() {
                if chars[index + 1] == '\n' {
                    index += 2;
                } else {
                    out.push(chars[index + 1]);
                    index += 2;
                }
            } else {
                index += 1;
            }
        } else {
            out.push(c);
            index += 1;
        }
    }
    out
}

fn is_shell_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

pub fn raw_to_wire(token: &RawToken, role: &str) -> LineTokenWire {
    LineTokenWire {
        text: token.text.clone(),
        start: token.start,
        end: token.end,
        role: role.to_string(),
        quoted: token.quoted,
        unterminated: token.unterminated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_words_span_chars() {
        let tokens = lex_line("bead close sase-1");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "bead");
        assert_eq!((tokens[0].start, tokens[0].end), (0, 4));
        assert!(!tokens[0].quoted);
    }

    #[test]
    fn single_and_double_quotes() {
        let tokens = lex_line("run 'fix it' \"done\"");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[1].text, "fix it");
        assert!(tokens[1].quoted);
        assert_eq!(tokens[2].text, "done");
    }

    #[test]
    fn adjacent_concatenation_joins() {
        let tokens = lex_line("a\"b c\"d");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "ab cd");
    }

    #[test]
    fn backslash_escapes_inside_and_outside() {
        let tokens = lex_line("a\\ b \"a\\\"b\"");
        assert_eq!(tokens[0].text, "a b");
        assert_eq!(tokens[1].text, "a\"b");
    }

    #[test]
    fn unterminated_quotes_run_to_end() {
        let single = lex_line("bead close \"sase-1");
        assert_eq!(single.len(), 3);
        assert!(single[2].unterminated);
        assert_eq!(single[2].text, "sase-1");
        let double = lex_line("run 'abc");
        assert!(double[1].unterminated);
    }

    #[test]
    fn multibyte_spans_are_char_offsets() {
        let tokens = lex_line("note é 🎉");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[1].text, "é");
        assert_eq!((tokens[1].start, tokens[1].end), (5, 6));
        assert_eq!((tokens[2].start, tokens[2].end), (7, 8));
        assert_eq!(unquoted_prefix("note é 🎉", 7, 8), "🎉");
    }

    #[test]
    fn whitespace_only_has_no_tokens() {
        assert!(lex_line("   ").is_empty());
        assert!(lex_line("").is_empty());
    }
}
