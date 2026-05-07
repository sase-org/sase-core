use super::completion::classify_completion_context;
use super::directive::directive_metadata;
use super::token::{extract_token_at_position, DocumentSnapshot};
use super::wire::{
    CompletionContextKind, EditorPosition, HoverPayload, XpromptAssistEntry,
};

pub fn hover_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<HoverPayload> {
    if let Some(context) =
        classify_completion_context(document, position, entries)
    {
        if matches!(
            context.kind,
            CompletionContextKind::XpromptArgumentName
                | CompletionContextKind::XpromptArgumentPath
                | CompletionContextKind::XpromptArgumentValue
                | CompletionContextKind::XpromptArgumentTypeHint
        ) {
            let entry_name = context.active_xprompt.as_ref()?;
            let entry =
                entries.iter().find(|entry| &entry.name == entry_name)?;
            return Some(HoverPayload {
                range: context.replacement_range,
                markdown: active_input_markdown(
                    entry,
                    context.active_input.as_deref(),
                ),
            });
        }

        if context.kind == CompletionContextKind::DirectiveArgument {
            let name = context.directive_name.as_deref()?;
            let metadata = directive_metadata(name)?;
            return Some(HoverPayload {
                range: context.replacement_range,
                markdown: format!(
                    "**%{}**\n\n{}",
                    metadata.name, metadata.description
                ),
            });
        }
    }

    let token = extract_token_at_position(document, position)?;
    if let Some(name) = token
        .text
        .strip_prefix("#!")
        .or_else(|| token.text.strip_prefix('#'))
    {
        let normalized = name.replace("__", "/");
        let entry = entries.iter().find(|entry| entry.name == normalized)?;
        return Some(HoverPayload {
            range: token.range,
            markdown: xprompt_markdown(entry),
        });
    }
    if let Some(name) = token.text.strip_prefix('/') {
        let entry = entries
            .iter()
            .find(|entry| entry.is_skill && entry.name == name)?;
        return Some(HoverPayload {
            range: token.range,
            markdown: xprompt_markdown(entry),
        });
    }
    None
}

fn xprompt_markdown(entry: &XpromptAssistEntry) -> String {
    let mut lines = vec![format!("**{}**", entry.insertion)];
    if let Some(signature) = &entry.input_signature {
        lines.push(String::new());
        lines.push(format!("`{signature}`"));
    }
    if let Some(description) = &entry.description {
        lines.push(String::new());
        lines.push(description.clone());
    } else if let Some(preview) = &entry.content_preview {
        lines.push(String::new());
        lines.push(preview.clone());
    }
    if let Some(source) = &entry.source_path_display {
        lines.push(String::new());
        lines.push(format!("Source: `{source}`"));
    }
    lines.join("\n")
}

fn active_input_markdown(
    entry: &XpromptAssistEntry,
    active_input: Option<&str>,
) -> String {
    let mut lines = vec![format!("**{} inputs**", entry.name)];
    for input in &entry.inputs {
        let marker = if Some(input.name.as_str()) == active_input {
            "- **"
        } else {
            "- `"
        };
        let close = if Some(input.name.as_str()) == active_input {
            "**"
        } else {
            "`"
        };
        let required = if input.required {
            "required"
        } else {
            "optional"
        };
        let default = input
            .default_display
            .as_ref()
            .map(|value| format!(", default `{value}`"))
            .unwrap_or_default();
        lines.push(format!(
            "{marker}{}{close}: `{}` ({required}{default})",
            input.name, input.r#type
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::editor::wire::XpromptInputHint;

    #[test]
    fn builds_xprompt_and_argument_hover() {
        let entries = vec![XpromptAssistEntry {
            name: "review".to_string(),
            insertion: "#review".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            input_signature: Some("(path: path)".to_string()),
            inputs: vec![XpromptInputHint {
                name: "path".to_string(),
                r#type: "path".to_string(),
                required: true,
                default_display: None,
                position: 0,
            }],
            content_preview: Some("Body preview".to_string()),
            description: Some("Review code".to_string()),
            source_path_display: Some("xprompts/review.md".to_string()),
            is_skill: false,
        }];
        let doc = DocumentSnapshot::new("#review:");
        let hover = hover_at_position(
            &doc,
            EditorPosition {
                line: 0,
                character: 3,
            },
            &entries,
        )
        .unwrap();
        assert!(hover.markdown.contains("Review code"));

        let arg_hover = hover_at_position(
            &doc,
            EditorPosition {
                line: 0,
                character: 8,
            },
            &entries,
        )
        .unwrap();
        assert!(arg_hover.markdown.contains("path"));
    }
}
