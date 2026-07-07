use lsp_types::{
    CompletionItem, CompletionItemKind, CompletionItemLabelDetails,
    CompletionItemTag, CompletionResponse, CompletionTextEdit, Documentation,
    InsertTextFormat, MarkupContent, MarkupKind, NumberOrString, Position,
    Range, TextEdit,
};
use sase_core::{
    CompletionCandidate, CompletionList, DiagnosticSeverity, EditorDiagnostic,
    EditorPosition, EditorRange, EditorTextEdit, HoverPayload, VcsRepoEntry,
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

/// Build the completion response for the `#+`/`+` (`vcs_project`) completion
/// kind.
///
/// Differs from [`completion_response`] in two ways: the `filter_text` is the
/// trigger spelling (`#+name` for a hash-plus token, or `+name` for a BOF
/// bare-plus token, so typing `#+sa`/`+sa` keeps the `sase` item), and the item
/// kind/label details distinguish projects from PRs. The primary
/// `text_edit` and `additional_text_edits` (the prepend/replace edit) are
/// carried over from the candidate's `replacement` / `additional_edits`.
///
/// `trigger_prefix` is the literal trigger spelling typed by the user (`"#+"`
/// or `"+"`); it prefixes each item's `filter_text` so client-side filtering
/// matches what is in the buffer.
pub fn vcs_project_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
    trigger_prefix: &str,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .map(|candidate| {
                vcs_project_completion_item(
                    candidate,
                    replacement_range,
                    trigger_prefix,
                )
            })
            .collect(),
    )
}

pub fn vcs_repo_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
    entries: &[VcsRepoEntry],
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .zip(entries.iter())
            .enumerate()
            .map(|(index, (candidate, entry))| {
                vcs_repo_completion_item(
                    candidate,
                    entry,
                    replacement_range,
                    index,
                )
            })
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

pub fn sase_snippet_completion_item(
    label: String,
    template: String,
    detail: Option<String>,
    documentation: Option<String>,
    replacement_range: EditorRange,
) -> CompletionItem {
    snippet_completion_item(
        label,
        sase_template_to_lsp_snippet(&template),
        detail,
        documentation,
        replacement_range,
    )
}

pub fn sase_template_to_lsp_snippet(template: &str) -> String {
    let mut converted = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '$' if chars.peek().is_some_and(|next| next.is_ascii_digit()) => {
                converted.push('$');
                while let Some(next) = chars.peek().copied() {
                    if !next.is_ascii_digit() {
                        break;
                    }
                    converted.push(next);
                    chars.next();
                }
            }
            '$' => converted.push_str("\\$"),
            '}' => converted.push_str("\\}"),
            '\\' => converted.push_str("\\\\"),
            _ => converted.push(ch),
        }
    }
    converted
}

pub fn hover(payload: HoverPayload) -> lsp_types::Hover {
    lsp_types::Hover {
        contents: lsp_types::HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: payload.markdown,
        }),
        range: Some(to_lsp_range(payload.range)),
    }
}

pub fn diagnostic(diagnostic: EditorDiagnostic) -> lsp_types::Diagnostic {
    lsp_types::Diagnostic {
        range: to_lsp_range(diagnostic.range),
        severity: Some(to_lsp_diagnostic_severity(diagnostic.severity)),
        code: Some(NumberOrString::String(diagnostic.code)),
        source: Some("sase-xprompt".to_string()),
        message: diagnostic.message,
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
        additional_text_edits: additional_text_edits(
            candidate.additional_edits,
        ),
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

/// Convert one `vcs_project` candidate, overriding the generic item's kind,
/// label details, and `filter_text` so the `#+name`/`+name` trigger spelling
/// (per `trigger_prefix`) drives client-side filtering.
fn vcs_project_completion_item(
    candidate: CompletionCandidate,
    replacement_range: EditorRange,
    trigger_prefix: &str,
) -> CompletionItem {
    let filter_text = format!("{trigger_prefix}{}", candidate.name);
    let is_changespec = candidate.kind == "changespec";
    let label_details = if is_changespec {
        Some(CompletionItemLabelDetails {
            detail: (!candidate.project.is_empty())
                .then(|| format!(" · {}", candidate.project)),
            description: Some(if candidate.status.is_empty() {
                "PR".to_string()
            } else {
                format!("PR · {}", candidate.status)
            }),
        })
    } else {
        Some(CompletionItemLabelDetails {
            detail: None,
            description: Some("project".to_string()),
        })
    };
    let detail = Some(candidate.insertion.clone());
    let kind = if is_changespec {
        CompletionItemKind::EVENT
    } else {
        CompletionItemKind::MODULE
    };
    let mut item = completion_item(candidate, replacement_range);
    item.kind = Some(kind);
    item.filter_text = Some(filter_text);
    item.label_details = label_details;
    item.detail = detail;
    item
}

fn vcs_repo_completion_item(
    candidate: CompletionCandidate,
    entry: &VcsRepoEntry,
    replacement_range: EditorRange,
    index: usize,
) -> CompletionItem {
    let badges = vcs_repo_badges(entry);
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
        label_details: Some(CompletionItemLabelDetails {
            detail: Some(format!(" · {}", entry.r#ref)),
            description: (!badges.is_empty()).then(|| badges.join(" ")),
        }),
        kind: Some(CompletionItemKind::MODULE),
        detail: Some(vcs_repo_detail(entry, &badges)),
        documentation: vcs_repo_documentation(entry, &badges),
        filter_text: Some(entry.r#ref.clone()),
        sort_text: Some(format!("{index:04}")),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(range),
            new_text,
        })),
        additional_text_edits: None,
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

fn vcs_repo_detail(entry: &VcsRepoEntry, badges: &[String]) -> String {
    if badges.is_empty() {
        return entry.r#ref.clone();
    }
    format!("{} {}", entry.r#ref, badges.join(" "))
}

fn vcs_repo_documentation(
    entry: &VcsRepoEntry,
    badges: &[String],
) -> Option<Documentation> {
    let mut sections = Vec::new();
    if !entry.description.is_empty() {
        sections.push(entry.description.clone());
    }
    if !badges.is_empty() {
        sections.push(badges.join(" "));
    }
    if sections.is_empty() {
        None
    } else {
        Some(markdown_doc(sections.join("\n\n")))
    }
}

fn vcs_repo_badges(entry: &VcsRepoEntry) -> Vec<String> {
    let mut badges = Vec::new();
    if entry.visibility == "private" {
        badges.push("[private]".to_string());
    }
    if entry.is_fork {
        badges.push("[fork]".to_string());
    }
    if entry.is_archived {
        badges.push("[archived]".to_string());
    }
    badges
}

/// Map the candidate's secondary edits (the prepend/replace-at-start tag edit)
/// to LSP `additionalTextEdits`, returning `None` when there are none.
fn additional_text_edits(edits: Vec<EditorTextEdit>) -> Option<Vec<TextEdit>> {
    if edits.is_empty() {
        return None;
    }
    Some(
        edits
            .into_iter()
            .map(|edit| TextEdit {
                range: to_lsp_range(edit.range),
                new_text: edit.new_text,
            })
            .collect(),
    )
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

fn to_lsp_diagnostic_severity(
    severity: DiagnosticSeverity,
) -> lsp_types::DiagnosticSeverity {
    match severity {
        DiagnosticSeverity::Error => lsp_types::DiagnosticSeverity::ERROR,
        DiagnosticSeverity::Warning => lsp_types::DiagnosticSeverity::WARNING,
        DiagnosticSeverity::Information => {
            lsp_types::DiagnosticSeverity::INFORMATION
        }
        DiagnosticSeverity::Hint => lsp_types::DiagnosticSeverity::HINT,
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
                additional_edits: Vec::new(),
                kind: String::new(),
                project: String::new(),
                status: String::new(),
            }],
            shared_extension: String::new(),
        };

        let CompletionResponse::Array(items) = completion_response(list, range)
        else {
            panic!("expected array response");
        };
        assert!(items[0].text_edit.is_some());
    }

    #[test]
    fn converts_sase_snippet_template_to_lsp_snippet_syntax() {
        assert_eq!(
            sase_template_to_lsp_snippet(r"cost $5 $1 \ path } $0"),
            r"cost $5 $1 \\ path \} $0"
        );
        assert_eq!(sase_template_to_lsp_snippet("$foo"), r"\$foo");
    }
}
