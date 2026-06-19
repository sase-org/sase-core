use crate::agent_launch::multi_prompt_separator_spans;

use super::{DocumentSnapshot, EditorRange};

pub fn multi_prompt_separator_ranges(
    document: &DocumentSnapshot,
) -> Vec<EditorRange> {
    multi_prompt_separator_spans(document.text())
        .into_iter()
        .filter_map(|(start, end)| document.byte_range_to_range(start, end))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::editor::EditorPosition;

    #[test]
    fn converts_separator_spans_to_editor_ranges() {
        let document = DocumentSnapshot::new("é before\n  ---  \nafter");

        assert_eq!(
            multi_prompt_separator_ranges(&document),
            vec![EditorRange {
                start: EditorPosition {
                    line: 1,
                    character: 2,
                },
                end: EditorPosition {
                    line: 1,
                    character: 5,
                },
            }]
        );
    }
}
