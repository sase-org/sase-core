use super::completion::classify_completion_context;
use super::directive::directive_metadata_with_flags;
use super::frontmatter;
use super::macro_arg_choices::macro_input_type_label;
use super::token::{
    extract_token_at_position, macro_reference_name,
    slash_skill_reference_name, DocumentSnapshot,
};
use super::wire::{
    CompletionContextKind, EditorPosition, HoverPayload, MacroAssistEntry,
    MacroInputHint,
};
use crate::MobileInputChoiceWire;

pub fn hover_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[MacroAssistEntry],
) -> Option<HoverPayload> {
    hover_at_position_with_flags(document, position, entries, &[])
}

pub fn hover_at_position_with_flags(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[MacroAssistEntry],
    enabled_feature_flags: &[String],
) -> Option<HoverPayload> {
    if let Some(context) =
        classify_completion_context(document, position, entries)
    {
        if matches!(
            context.kind,
            CompletionContextKind::MacroArgumentName
                | CompletionContextKind::MacroArgumentPath
                | CompletionContextKind::MacroArgumentValue
                | CompletionContextKind::MacroArgumentAgent
                | CompletionContextKind::MacroArgumentTypeHint
        ) {
            let entry_name = context.active_macro.as_ref()?;
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

        if context.kind == CompletionContextKind::DirectiveName {
            let token = context.token.as_ref()?;
            let raw = token.text.strip_prefix('%').unwrap_or(&token.text);
            let metadata =
                directive_metadata_with_flags(raw, enabled_feature_flags)?;
            return Some(HoverPayload {
                range: token.range,
                markdown: format!(
                    "**%{}**\n\n{}",
                    metadata.name, metadata.description
                ),
            });
        }

        if matches!(
            context.kind,
            CompletionContextKind::DirectiveArgument
                | CompletionContextKind::DirectiveArgumentKeyword
                | CompletionContextKind::DirectiveArgumentValue
        ) {
            let name = context.directive_name.as_deref()?;
            let metadata =
                directive_metadata_with_flags(name, enabled_feature_flags)?;
            return Some(HoverPayload {
                range: context.replacement_range,
                markdown: format!(
                    "**%{}**\n\n{}",
                    metadata.name, metadata.description
                ),
            });
        }
    }

    if let Some(hover) = frontmatter::hover(document, position) {
        return Some(hover);
    }

    let token = extract_token_at_position(document, position)?;
    if let Some(name) = macro_reference_name(&token.text) {
        let entry = entries.iter().find(|entry| entry.name == name)?;
        return Some(HoverPayload {
            range: token.range,
            markdown: xprompt_markdown(entry),
        });
    }
    if let Some(name) = slash_skill_reference_name(&token.text) {
        let entry = entries.iter().find(|entry| {
            entry.is_skill && entry.skill_name.as_deref() == Some(name)
        })?;
        return Some(HoverPayload {
            range: token.range,
            markdown: xprompt_markdown(entry),
        });
    }
    None
}

fn xprompt_markdown(entry: &MacroAssistEntry) -> String {
    let mut lines = vec![format!("**{}**", entry.insertion)];
    let mut meta = Vec::new();
    if let Some(kind) = &entry.kind {
        meta.push(kind.clone());
    }
    meta.push(format!("canonical `{}`", entry.reference_prefix));
    if let Some(skill_name) = &entry.skill_name {
        meta.push(format!("skill `/{skill_name}`"));
    }
    if let Some(memory_type) = entry.memory_type {
        meta.push(format!("tier `{}`", memory_type.as_str()));
    }
    if !entry.source_bucket.is_empty() {
        meta.push(entry.source_bucket.clone());
    }
    if let Some(project) = &entry.project {
        meta.push(format!("project `{project}`"));
    }
    if !meta.is_empty() {
        lines.push(String::new());
        lines.push(meta.join(" | "));
    }
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
    if !entry.tags.is_empty() {
        lines.push(String::new());
        lines.push(format!("Tags: {}", entry.tags.join(", ")));
    }
    if entry.description.is_some() {
        if let Some(preview) = &entry.content_preview {
            lines.push(String::new());
            lines.push(bounded_preview(preview));
        }
    }
    lines.join("\n")
}

fn bounded_preview(preview: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 600;
    let mut out = String::new();
    for (idx, ch) in preview.chars().enumerate() {
        if idx == MAX_PREVIEW_CHARS {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

fn active_input_markdown(
    entry: &MacroAssistEntry,
    active_input: Option<&str>,
) -> String {
    if let Some(name) = active_input {
        if let Some(input) =
            entry.inputs.iter().find(|input| input.name == name)
        {
            return argument_hover_markdown(input);
        }
    }
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
        let description = input
            .description
            .as_ref()
            .filter(|value| !value.is_empty())
            .map(|value| format!(" - {value}"))
            .unwrap_or_default();
        lines.push(format!(
            "{marker}{}{close}: `{}` ({required}{default})",
            input.name,
            macro_input_type_label(input)
        ));
        if !description.is_empty() {
            let last = lines.last_mut().expect("just pushed input hover line");
            last.push_str(&description);
        }
    }
    lines.join("\n")
}

fn argument_hover_markdown(input: &MacroInputHint) -> String {
    let mut lines = vec![
        format!("**{}**", input.name),
        String::new(),
        format!("`{}`", macro_input_type_label(input)),
        format!("Source: {}", input_type_source(input)),
    ];
    if let Some(default) = &input.default_display {
        lines.push(format!("Default: `{default}`"));
    }
    if let Some(description) =
        input.description.as_ref().filter(|value| !value.is_empty())
    {
        lines.push(String::new());
        lines.push(description.clone());
    }
    if !input.choices.is_empty() {
        lines.push(String::new());
        lines.push(choice_markdown_table(&input.choices));
    }
    lines.join("\n")
}

fn input_type_source(hint: &MacroInputHint) -> String {
    let Some(named) =
        hint.named_type.as_deref().filter(|name| !name.is_empty())
    else {
        return "builtin".to_string();
    };
    match named.split_once('@') {
        Some((distribution, _))
            if !distribution.eq_ignore_ascii_case("builtin") =>
        {
            format!("plugin {distribution}")
        }
        _ => "builtin".to_string(),
    }
}

const CHOICE_TABLE_LIMIT: usize = 12;

fn choice_markdown_table(choices: &[MobileInputChoiceWire]) -> String {
    let mut lines = vec![
        "| Value | Label | Description |".to_string(),
        "| --- | --- | --- |".to_string(),
    ];
    let shown = choices.len().min(CHOICE_TABLE_LIMIT);
    for choice in &choices[..shown] {
        lines.push(format!(
            "| {} | {} | {} |",
            escape_table_cell(&format!("`{}`", escape_ticks(&choice.value))),
            escape_table_cell(choice.label.as_deref().unwrap_or("")),
            escape_table_cell(choice.description.as_deref().unwrap_or("")),
        ));
    }
    if choices.len() > CHOICE_TABLE_LIMIT {
        lines.push(String::new());
        lines.push(format!("… {} more", choices.len() - CHOICE_TABLE_LIMIT));
    }
    lines.join("\n")
}

fn escape_table_cell(text: &str) -> String {
    text.replace('|', "\\|")
        .replace('\n', " ")
        .replace('\r', "")
}

fn escape_ticks(text: &str) -> String {
    text.replace('`', "\\`")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{content_layout::MemoryTierWire, editor::wire::MacroInputHint};

    #[test]
    fn hovers_a_skill_through_both_of_its_names() {
        let entries = vec![MacroAssistEntry {
            name: "skill/sase_plan".to_string(),
            display_label: "skill/sase_plan".to_string(),
            insertion: "#skill/sase_plan".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "builtin".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: Vec::new(),
            content_preview: None,
            description: Some("Create an implementation plan".to_string()),
            source_path_display: Some("sase/skills/sase_plan.md".to_string()),
            definition_path: None,
            definition_range: None,
            is_skill: true,
            skill_name: Some("sase_plan".to_string()),
            memory_type: None,
        }];
        let at = |text: &str, character: u32| {
            hover_at_position(
                &DocumentSnapshot::new(text),
                EditorPosition { line: 0, character },
                &entries,
            )
        };

        let namespaced = at("#skill/sase_plan", 3).unwrap();
        assert!(namespaced
            .markdown
            .contains("Create an implementation plan"));
        assert!(namespaced.markdown.contains("skill `/sase_plan`"));

        let slash = at("/sase_plan", 3).unwrap();
        assert_eq!(slash.markdown, namespaced.markdown);

        // Neither the bare reference nor the namespaced slash form resolves.
        assert!(at("#sase_plan", 3).is_none());
        assert!(at("/skill", 3).is_none());
    }

    #[test]
    fn hovers_an_xprompt_memory_with_its_kind_and_tier() {
        let entries = vec![MacroAssistEntry {
            name: "memory/glossary".to_string(),
            display_label: "memory/glossary".to_string(),
            insertion: "#memory/glossary".to_string(),
            reference_prefix: "#".to_string(),
            kind: Some("memory".to_string()),
            source_bucket: "project".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: Vec::new(),
            content_preview: Some("Glossary body".to_string()),
            description: Some("SASE terms".to_string()),
            source_path_display: Some("sase/memory/glossary.md".to_string()),
            definition_path: None,
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: Some(MemoryTierWire::Reference),
        }];
        let at = |text: &str, character: u32| {
            hover_at_position(
                &DocumentSnapshot::new(text),
                EditorPosition { line: 0, character },
                &entries,
            )
        };

        let hover = at("#memory/glossary", 3).unwrap();
        assert!(hover.markdown.contains("memory"), "{}", hover.markdown);
        assert!(
            hover.markdown.contains("tier `reference`"),
            "{}",
            hover.markdown
        );
        assert!(hover.markdown.contains("SASE terms"));
        // No bare alias and no slash form.
        assert!(at("#glossary", 3).is_none());
        assert!(at("/glossary", 3).is_none());
    }

    #[test]
    fn builds_xprompt_and_argument_hover() {
        let entries = vec![MacroAssistEntry {
            name: "review".to_string(),
            display_label: "review".to_string(),
            insertion: "#review".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "builtin".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: Some("(path: path)".to_string()),
            inputs: vec![MacroInputHint {
                name: "path".to_string(),
                r#type: "path".to_string(),
                description: Some("Path to review".to_string()),
                required: true,
                default_display: None,
                position: 0,
                repeatable: false,
                choices: Vec::new(),
                named_type: None,
                value_role: None,
            }],
            content_preview: Some("Body preview".to_string()),
            description: Some("Review code".to_string()),
            source_path_display: Some("sase/xprompts/review.md".to_string()),
            definition_path: Some("/tmp/sase/xprompts/review.md".to_string()),
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
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
        assert!(arg_hover.markdown.contains("Path to review"));
        assert!(arg_hover.markdown.contains("Source: builtin"));
    }

    fn choice_hint(
        name: &str,
        named_type: Option<&str>,
        default: Option<&str>,
        choices: &[(&str, Option<&str>, Option<&str>)],
        value_role: Option<&str>,
        r#type: &str,
    ) -> MacroInputHint {
        MacroInputHint {
            name: name.to_string(),
            r#type: r#type.to_string(),
            description: None,
            required: true,
            default_display: default.map(str::to_string),
            position: 0,
            repeatable: false,
            choices: choices
                .iter()
                .map(|(value, label, description)| {
                    crate::MobileInputChoiceWire {
                        value: (*value).to_string(),
                        label: label.map(str::to_string),
                        description: description.map(str::to_string),
                    }
                })
                .collect(),
            named_type: named_type.map(str::to_string),
            value_role: value_role.map(str::to_string),
        }
    }

    fn entry_with_inputs(
        name: &str,
        inputs: Vec<MacroInputHint>,
    ) -> MacroAssistEntry {
        MacroAssistEntry {
            name: name.to_string(),
            display_label: name.to_string(),
            insertion: format!("#{name}"),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "project".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs,
            content_preview: None,
            description: None,
            source_path_display: Some("macros/review.md".to_string()),
            definition_path: None,
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
        }
    }

    #[test]
    fn argument_hover_lists_choices_source_and_truncates() {
        let entries = vec![entry_with_inputs(
            "choose",
            vec![choice_hint(
                "edition",
                None,
                Some("brief"),
                &[
                    ("brief", Some("Brief"), Some("Short")),
                    ("full", Some("Full"), Some("Long")),
                ],
                None,
                "enum",
            )],
        )];
        let hover = hover_at_position(
            &DocumentSnapshot::new("#choose(edition="),
            EditorPosition {
                line: 0,
                character: 16,
            },
            &entries,
        )
        .unwrap();
        assert!(
            hover.markdown.contains("`brief | full`"),
            "{}",
            hover.markdown
        );
        assert!(hover.markdown.contains("Source: builtin"));
        assert!(hover.markdown.contains("Default: `brief`"));
        assert!(hover.markdown.contains("| Value | Label | Description |"));
        assert!(hover.markdown.contains("`brief`"));
        assert!(hover.markdown.contains("Short"));
        assert!(
            !hover.markdown.contains("macros/review.md"),
            "must not use the macro source as the input type source: {}",
            hover.markdown
        );

        let mut large_hint = choice_hint(
            "mode",
            Some("sase-research-artifacts@audio_edition"),
            None,
            &[],
            None,
            "enum",
        );
        large_hint.choices = (0..15)
            .map(|i| crate::MobileInputChoiceWire {
                value: format!("v{i}"),
                label: None,
                description: None,
            })
            .collect();
        let large = vec![entry_with_inputs("big", vec![large_hint])];
        let large_hover = hover_at_position(
            &DocumentSnapshot::new("#big(mode="),
            EditorPosition {
                line: 0,
                character: 10,
            },
            &large,
        )
        .unwrap();
        assert!(
            large_hover
                .markdown
                .contains("Source: plugin sase-research-artifacts"),
            "{}",
            large_hover.markdown
        );
        assert!(large_hover.markdown.contains("… 3 more"));
        assert!(
            large_hover
                .markdown
                .contains("`sase-research-artifacts@audio_edition (15)`")
                || large_hover.markdown.contains("`audio_edition (15)`")
                || large_hover.markdown.contains("(15)")
        );
    }

    #[test]
    fn agent_argument_hover_is_dispatched() {
        let entries = vec![entry_with_inputs(
            "assign",
            vec![choice_hint(
                "who",
                Some("agent"),
                None,
                &[],
                Some("agent"),
                "agent",
            )],
        )];
        let hover = hover_at_position(
            &DocumentSnapshot::new("#assign(who="),
            EditorPosition {
                line: 0,
                character: 12,
            },
            &entries,
        )
        .unwrap();
        assert!(hover.markdown.contains("**who**"), "{}", hover.markdown);
        assert!(hover.markdown.contains("`agent`"));
        assert!(hover.markdown.contains("Source: builtin"));
    }

    #[test]
    fn directive_argument_hover_uses_current_identity_and_clan_metadata() {
        for (text, character, heading, description) in [
            (
                "%id(worker, session=review)",
                14,
                "**%id**",
                "Assign an agent ID with optional bead, clan, session, or user-managed tribe",
            ),
            (
                "%i(worker, tribe=review)",
                13,
                "**%id**",
                "Assign an agent ID with optional bead, clan, session, or user-managed tribe",
            ),
            (
                "%clan(research, tr)",
                18,
                "**%clan**",
                "Declare a new parallel agent clan",
            ),
            (
                "%c(research, tr)",
                15,
                "**%clan**",
                "Declare a new parallel agent clan",
            ),
            (
                "%final:commit",
                8,
                "**%final**",
                "Select configured finalizer instances for this launch",
            ),
        ] {
            let hover = hover_at_position(
                &DocumentSnapshot::new(text),
                EditorPosition { line: 0, character },
                &[],
            )
            .unwrap_or_else(|| panic!("missing hover for {text}"));
            assert!(hover.markdown.contains(heading), "{text}");
            assert!(hover.markdown.contains(description), "{text}");
        }

        for text in ["%tribe:research", "%t:research"] {
            assert!(
                hover_at_position(
                    &DocumentSnapshot::new(text),
                    EditorPosition {
                        line: 0,
                        character: 1
                    },
                    &[],
                )
                .is_none(),
                "removed directive should not hover: {text}"
            );
        }
    }

    #[test]
    fn builds_frontmatter_field_hover() {
        let doc = DocumentSnapshot::new(
            "---\ndescription: Demo\nxprompts:\n  _helper:\n    content: Helper\n---\n#_helper\n",
        );
        let hover = hover_at_position(
            &doc,
            EditorPosition {
                line: 2,
                character: 2,
            },
            &[],
        )
        .unwrap();

        let field_start = doc.text().find("xprompts").unwrap();
        assert_eq!(
            hover.range,
            doc.byte_range_to_range(
                field_start,
                field_start + "xprompts".len()
            )
            .unwrap()
        );
        assert!(hover.markdown.contains("**xprompts**"));
        assert!(hover.markdown.contains("local xprompts"));
        assert!(hover.markdown.contains("current file"));
    }

    #[test]
    fn frontmatter_hover_ignores_body_and_non_field_positions() {
        let doc = DocumentSnapshot::new(
            "---\nxprompts:\n  _helper:\n    content: Helper\n---\nBody xprompts\n",
        );

        assert!(hover_at_position(
            &doc,
            EditorPosition {
                line: 5,
                character: 7,
            },
            &[],
        )
        .is_none());
        assert!(hover_at_position(
            &doc,
            EditorPosition {
                line: 1,
                character: 8,
            },
            &[],
        )
        .is_none());
        assert!(hover_at_position(
            &DocumentSnapshot::new("xprompts:\nBody\n"),
            EditorPosition {
                line: 0,
                character: 2,
            },
            &[],
        )
        .is_none());
    }
}
