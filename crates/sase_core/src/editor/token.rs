use super::wire::{EditorPosition, EditorRange, TokenInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentSnapshot {
    text: String,
    line_starts: Vec<usize>,
}

impl DocumentSnapshot {
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        let mut line_starts = vec![0];
        for (idx, byte) in text.bytes().enumerate() {
            if byte == b'\n' {
                line_starts.push(idx + 1);
            }
        }
        Self { text, line_starts }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn line_count(&self) -> usize {
        self.line_starts.len()
    }

    pub fn line_text(&self, line: u32) -> Option<&str> {
        let line = usize::try_from(line).ok()?;
        let start = *self.line_starts.get(line)?;
        let end = self
            .line_starts
            .get(line + 1)
            .copied()
            .unwrap_or(self.text.len());
        Some(self.text[start..end].trim_end_matches(['\r', '\n']))
    }

    pub fn position_to_byte_offset(
        &self,
        position: EditorPosition,
    ) -> Option<usize> {
        let line = usize::try_from(position.line).ok()?;
        let line_text = self.line_text(position.line)?;
        let line_start = *self.line_starts.get(line)?;
        let target_units = usize::try_from(position.character).ok()?;
        let mut units = 0usize;
        for (byte_idx, ch) in line_text.char_indices() {
            if units == target_units {
                return Some(line_start + byte_idx);
            }
            let width = ch.len_utf16();
            if units + width > target_units {
                return None;
            }
            units += width;
        }
        if units == target_units {
            Some(line_start + line_text.len())
        } else {
            None
        }
    }

    pub fn byte_offset_to_position(
        &self,
        byte_offset: usize,
    ) -> Option<EditorPosition> {
        if byte_offset > self.text.len()
            || !self.text.is_char_boundary(byte_offset)
        {
            return None;
        }
        let line_idx = match self.line_starts.binary_search(&byte_offset) {
            Ok(idx) => idx,
            Err(idx) => idx.checked_sub(1)?,
        };
        let line_start = self.line_starts[line_idx];
        let mut units = 0u32;
        for ch in self.text[line_start..byte_offset].chars() {
            units = units.checked_add(u32::try_from(ch.len_utf16()).ok()?)?;
        }
        Some(EditorPosition {
            line: u32::try_from(line_idx).ok()?,
            character: units,
        })
    }

    pub fn byte_range_to_range(
        &self,
        byte_start: usize,
        byte_end: usize,
    ) -> Option<EditorRange> {
        Some(EditorRange {
            start: self.byte_offset_to_position(byte_start)?,
            end: self.byte_offset_to_position(byte_end)?,
        })
    }
}

pub fn extract_token_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<TokenInfo> {
    let cursor = document.position_to_byte_offset(position)?;
    let line = document.line_text(position.line)?;
    let line_start = *document
        .line_starts
        .get(usize::try_from(position.line).ok()?)?;
    let line_end = line_start + line.len();
    let cursor = cursor.clamp(line_start, line_end);

    let mut start = cursor;
    while start > line_start {
        let prev_start = previous_char_boundary(document.text(), start)?;
        if is_token_delimiter_at(document.text(), prev_start) {
            break;
        }
        start = prev_start;
    }

    let mut end = cursor;
    while end < line_end {
        if is_token_delimiter_at(document.text(), end) {
            break;
        }
        end = next_char_boundary(document.text(), end)?;
    }

    if start >= line_start + 2
        && document.text().get(start - 2..start) == Some("#!")
        && (start == line_start
            || document.text().is_char_boundary(start - 2)
                && delimiter_before(document.text(), start - 2, line_start))
    {
        start -= 2;
    }

    if start == end
        && cursor >= line_start + 2
        && document.text().get(cursor - 2..cursor) == Some("#!")
    {
        let marker_start = cursor - 2;
        if marker_start == line_start
            || delimiter_before(document.text(), marker_start, line_start)
        {
            return token_info(document, marker_start, cursor);
        }
    }

    if start == end {
        return None;
    }
    token_info(document, start, end)
}

pub fn is_xprompt_like_token(token: &str) -> bool {
    token.starts_with('#')
        && !token.is_empty()
        && !token.chars().any(char::is_whitespace)
}

pub fn is_slash_skill_like_token(token: &str) -> bool {
    let Some(rest) = token.strip_prefix('/') else {
        return false;
    };
    rest.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub fn is_path_like_token(token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    let bare = token.strip_prefix('@').unwrap_or(token);
    if bare.is_empty() {
        return false;
    }
    bare.starts_with("~/")
        || bare.starts_with('/')
        || bare.starts_with("./")
        || bare.starts_with("../")
        || bare.starts_with(".sase/")
        || bare.contains('/')
}

pub fn is_snippet_trigger_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub fn xprompt_reference_name(token: &str) -> Option<String> {
    token
        .strip_prefix("#!")
        .or_else(|| token.strip_prefix('#'))
        .map(|name| name.replace("__", "/"))
}

pub fn slash_skill_reference_name(token: &str) -> Option<&str> {
    token.strip_prefix('/').filter(|name| !name.is_empty())
}

fn token_info(
    document: &DocumentSnapshot,
    byte_start: usize,
    byte_end: usize,
) -> Option<TokenInfo> {
    Some(TokenInfo {
        text: document.text().get(byte_start..byte_end)?.to_string(),
        range: document.byte_range_to_range(byte_start, byte_end)?,
        byte_start,
        byte_end,
    })
}

fn is_token_delimiter_at(text: &str, byte_idx: usize) -> bool {
    let Some(ch) = text.get(byte_idx..).and_then(|tail| tail.chars().next())
    else {
        return true;
    };
    if ch == '!'
        && byte_idx > 0
        && text.is_char_boundary(byte_idx - 1)
        && text.get(byte_idx - 1..byte_idx) == Some("#")
    {
        return false;
    }
    ch.is_whitespace() || "'\"`?!;,()[]{}<>|&=+*^%$:\\".contains(ch)
}

fn delimiter_before(text: &str, byte_idx: usize, line_start: usize) -> bool {
    if byte_idx == line_start {
        return true;
    }
    previous_char_boundary(text, byte_idx)
        .map(|prev| is_token_delimiter_at(text, prev))
        .unwrap_or(true)
}

fn previous_char_boundary(text: &str, byte_idx: usize) -> Option<usize> {
    text.get(..byte_idx)?
        .char_indices()
        .last()
        .map(|(idx, _)| idx)
}

fn next_char_boundary(text: &str, byte_idx: usize) -> Option<usize> {
    let ch = text.get(byte_idx..)?.chars().next()?;
    Some(byte_idx + ch.len_utf8())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(line: u32, character: u32) -> EditorPosition {
        EditorPosition { line, character }
    }

    #[test]
    fn maps_lsp_utf16_positions_defensively() {
        let doc = DocumentSnapshot::new("a\nx🙂y");
        assert_eq!(doc.position_to_byte_offset(pos(1, 0)), Some(2));
        assert_eq!(doc.position_to_byte_offset(pos(1, 1)), Some(3));
        assert_eq!(doc.position_to_byte_offset(pos(1, 2)), None);
        assert_eq!(doc.position_to_byte_offset(pos(1, 3)), Some(7));
        assert_eq!(
            doc.byte_offset_to_position(7),
            Some(EditorPosition {
                line: 1,
                character: 3
            })
        );
    }

    #[test]
    fn extracts_expected_tokens() {
        let cases = [
            ("#foo", 4, Some("#foo")),
            ("#!", 2, Some("#!")),
            ("#!foo", 5, Some("#!foo")),
            ("/skill", 6, Some("/skill")),
            ("@src/foo", 8, Some("@src/foo")),
            ("./src/foo", 9, Some("./src/foo")),
            ("../foo", 6, Some("../foo")),
            (".sase/foo", 9, Some(".sase/foo")),
            ("~/foo", 5, Some("~/foo")),
            ("/tmp/foo", 8, Some("/tmp/foo")),
            ("say #foo:bar", 8, Some("#foo")),
            ("", 0, None),
            ("foo bar", 3, Some("foo")),
            ("foo bar", 4, Some("bar")),
        ];
        for (line, col, expected) in cases {
            let doc = DocumentSnapshot::new(line);
            let token = extract_token_at_position(&doc, pos(0, col));
            assert_eq!(
                token.as_ref().map(|t| t.text.as_str()),
                expected,
                "{line}"
            );
        }
    }

    #[test]
    fn extracts_mid_token_ranges() {
        let doc = DocumentSnapshot::new("ask #foobar now");
        let token = extract_token_at_position(&doc, pos(0, 7)).unwrap();
        assert_eq!(token.text, "#foobar");
        assert_eq!(token.byte_start, 4);
        assert_eq!(token.byte_end, 11);
    }

    #[test]
    fn recognizes_prompt_widget_snippet_trigger_tokens() {
        assert!(is_snippet_trigger_token("foo"));
        assert!(is_snippet_trigger_token("foo_1"));
        assert!(!is_snippet_trigger_token("#foo"));
        assert!(!is_snippet_trigger_token("/foo"));
        assert!(!is_snippet_trigger_token("@foo"));
        assert!(!is_snippet_trigger_token("foo-bar"));
        assert!(!is_snippet_trigger_token(""));
    }
}
