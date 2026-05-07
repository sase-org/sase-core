use lsp_types::{
    CompletionItem, CompletionItemKind, CompletionItemLabelDetails,
    CompletionItemTag, CompletionResponse, CompletionTextEdit, Documentation,
    InsertTextFormat, MarkupContent, MarkupKind, Position, Range, TextEdit,
};
use sase_core::{
    CompletionCandidate, CompletionList, EditorPosition, EditorRange,
    EditorTextEdit,
};

pub fn to_editor_position(position: Position) -> EditorPosition {
    EditorPosition {
        line: position.line,
        character: position.character,
    }
}

pub fn to_lsp_range(range: EditorRange) -> Range {
    Range {
        start: to_lsp_position(range.start),
        end: to_lsp_position(range.end),
    }
}

pub fn completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .map(|candidate| completion_item(candidate, replacement_range))
            .collect(),
    )
}

pub fn snippet_completion_item(
    label: String,
    new_text: String,
    detail: Option<String>,
    documentation: Option<String>,
    replacement_range: EditorRange,
) -> CompletionItem {
    CompletionItem {
        label,
        label_details: Some(CompletionItemLabelDetails {
            detail: Some(" snippet".to_string()),
            description: None,
        }),
        kind: Some(CompletionItemKind::SNIPPET),
        detail,
        documentation: documentation.map(markdown_doc),
        insert_text_format: Some(InsertTextFormat::SNIPPET),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(replacement_range),
            new_text,
        })),
        ..Default::default()
    }
}

fn completion_item(
    candidate: CompletionCandidate,
    replacement_range: EditorRange,
) -> CompletionItem {
    let range = candidate
        .replacement
        .as_ref()
        .map(|replacement| replacement.range)
        .unwrap_or(replacement_range);
    let new_text = candidate
        .replacement
        .map(|replacement| replacement.new_text)
        .unwrap_or_else(|| candidate.insertion.clone());
    CompletionItem {
        label: candidate.display,
        kind: Some(if candidate.is_dir {
            CompletionItemKind::FOLDER
        } else {
            CompletionItemKind::TEXT
        }),
        detail: candidate.detail,
        documentation: candidate.documentation.map(markdown_doc),
        filter_text: Some(candidate.name),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(range),
            new_text,
        })),
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

pub fn apply_replacement(
    list: CompletionList,
    range: EditorRange,
) -> CompletionList {
    CompletionList {
        candidates: list
            .candidates
            .into_iter()
            .map(|mut candidate| {
                if candidate.replacement.is_none() {
                    candidate.replacement = Some(EditorTextEdit {
                        range,
                        new_text: candidate.insertion.clone(),
                    });
                }
                candidate
            })
            .collect(),
        shared_extension: list.shared_extension,
    }
}

fn to_lsp_position(position: EditorPosition) -> Position {
    Position {
        line: position.line,
        character: position.character,
    }
}

fn markdown_doc(value: String) -> Documentation {
    Documentation::MarkupContent(MarkupContent {
        kind: MarkupKind::Markdown,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_editor_range_to_lsp_range() {
        let range = EditorRange {
            start: EditorPosition {
                line: 1,
                character: 2,
            },
            end: EditorPosition {
                line: 1,
                character: 5,
            },
        };

        assert_eq!(to_lsp_range(range).start.character, 2);
        assert_eq!(to_lsp_range(range).end.character, 5);
    }

    #[test]
    fn completion_item_uses_replacement_text_edit() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 1,
            },
            end: EditorPosition {
                line: 0,
                character: 3,
            },
        };
        let list = CompletionList {
            candidates: vec![CompletionCandidate {
                display: "#foo".to_string(),
                insertion: "#foo".to_string(),
                detail: None,
                documentation: None,
                is_dir: false,
                name: "foo".to_string(),
                replacement: Some(EditorTextEdit {
                    range,
                    new_text: "#foo".to_string(),
                }),
            }],
            shared_extension: String::new(),
        };

        let CompletionResponse::Array(items) = completion_response(list, range)
        else {
            panic!("expected array response");
        };
        assert!(items[0].text_edit.is_some());
    }
}
