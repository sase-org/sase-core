use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextTailWire {
    pub text: String,
    pub omitted_lines: u64,
    pub omitted_chars: u64,
}

/// Return the tail of `text`, bounded by both logical lines and Unicode chars.
///
/// Line selection matches Python's `splitlines()` shape closely enough for
/// prompt output: line terminators are not retained, and a trailing newline
/// does not create an extra empty selected line. The character budget is
/// applied after line selection and counts Unicode scalar values, matching
/// Python's `len(str)` for valid strings.
pub fn tail_text_by_lines_and_chars(
    text: &str,
    max_lines: usize,
    max_chars: usize,
) -> TextTailWire {
    let lines: Vec<&str> = text.lines().collect();
    let omitted_lines = lines.len().saturating_sub(max_lines);
    let selected_lines = if max_lines == 0 {
        &lines[lines.len()..]
    } else {
        &lines[omitted_lines..]
    };

    let selected = selected_lines.join("\n");
    let selected_chars = selected.chars().count();
    if selected_chars <= max_chars {
        return TextTailWire {
            text: selected,
            omitted_lines: omitted_lines as u64,
            omitted_chars: 0,
        };
    }

    let mut tail_chars: Vec<char> =
        selected.chars().rev().take(max_chars).collect();
    tail_chars.reverse();
    TextTailWire {
        text: tail_chars.into_iter().collect(),
        omitted_lines: omitted_lines as u64,
        omitted_chars: (selected_chars - max_chars) as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_short_text_unchanged() {
        assert_eq!(
            tail_text_by_lines_and_chars("one\ntwo\nthree\n", 10, 100),
            TextTailWire {
                text: "one\ntwo\nthree".to_string(),
                omitted_lines: 0,
                omitted_chars: 0,
            }
        );
    }

    #[test]
    fn applies_line_budget_before_character_budget() {
        assert_eq!(
            tail_text_by_lines_and_chars("one\ntwo\nthree\nfour", 2, 100),
            TextTailWire {
                text: "three\nfour".to_string(),
                omitted_lines: 2,
                omitted_chars: 0,
            }
        );
    }

    #[test]
    fn trims_selected_tail_by_unicode_character_count() {
        assert_eq!(
            tail_text_by_lines_and_chars("alpha\nbeta\nééévalue", 2, 7),
            TextTailWire {
                text: "éévalue".to_string(),
                omitted_lines: 1,
                omitted_chars: 6,
            }
        );
    }

    #[test]
    fn zero_line_budget_returns_empty_tail() {
        assert_eq!(
            tail_text_by_lines_and_chars("one\ntwo", 0, 100),
            TextTailWire {
                text: String::new(),
                omitted_lines: 2,
                omitted_chars: 0,
            }
        );
    }

    #[test]
    fn zero_character_budget_returns_empty_tail_after_line_selection() {
        assert_eq!(
            tail_text_by_lines_and_chars("one\ntwo", 1, 0),
            TextTailWire {
                text: String::new(),
                omitted_lines: 1,
                omitted_chars: 3,
            }
        );
    }
}
