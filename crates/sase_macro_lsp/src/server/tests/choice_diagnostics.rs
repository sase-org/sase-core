use std::sync::Arc;

use lsp_types::{
    CodeActionContext, CodeActionKind, CodeActionOrCommand, Hover,
    HoverContents, Position, Range, WorkspaceEdit,
};

use sase_core::{
    MobileInputChoiceWire, MobileMacroInputWire, StaticHelperHostBridge,
};

use super::super::*;
use super::support::*;

fn choice_bridge() -> StaticHelperHostBridge {
    let mut agent = input_hint("who", "agent", true, 0);
    agent.value_role = Some("agent".to_string());
    agent.named_type = Some("agent".to_string());
    let mut many = input_hint("mode", "enum", true, 0);
    many.named_type = Some("sase-research-artifacts@audio_edition".to_string());
    many.choices = (0..15)
        .map(|i| MobileInputChoiceWire {
            value: format!("v{i}"),
            label: None,
            description: None,
        })
        .collect();
    bridge_with_catalog_entries(vec![
        catalog_entry(
            "choose",
            "#choose",
            Some("(edition: brief|full)".to_string()),
            vec![enum_hint(
                "edition",
                true,
                0,
                &[
                    ("brief", Some("Brief"), Some("Short edition")),
                    ("full", Some("Full"), Some("Complete edition")),
                ],
            )],
            None,
        ),
        catalog_entry(
            "editions",
            "#editions",
            Some("(edition*: brief|full)".to_string()),
            vec![{
                let mut hint = enum_hint(
                    "edition",
                    false,
                    0,
                    &[("brief", None, None), ("full", None, None)],
                );
                hint.repeatable = true;
                hint
            }],
            None,
        ),
        catalog_entry(
            "assign",
            "#assign",
            Some("(who: agent)".to_string()),
            vec![agent],
            None,
        ),
        catalog_entry(
            "big",
            "#big",
            Some("(mode: enum)".to_string()),
            vec![many],
            None,
        ),
    ])
}

fn enum_hint(
    name: &str,
    required: bool,
    position: u32,
    choices: &[(&str, Option<&str>, Option<&str>)],
) -> MobileMacroInputWire {
    let mut hint = input_hint(name, "enum", required, position);
    hint.choices = choices
        .iter()
        .map(|(value, label, description)| MobileInputChoiceWire {
            value: (*value).to_string(),
            label: label.map(str::to_string),
            description: description.map(str::to_string),
        })
        .collect();
    hint
}

fn service() -> LspService<MacroLspServer> {
    LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(choice_bridge()))
    })
    .0
}

fn action_title(action: &CodeActionOrCommand) -> Option<&str> {
    match action {
        CodeActionOrCommand::CodeAction(action) => Some(action.title.as_str()),
        CodeActionOrCommand::Command(_) => None,
    }
}

fn preferred_edit(actions: &[CodeActionOrCommand]) -> (String, String) {
    let action = actions
        .iter()
        .filter_map(|action| match action {
            CodeActionOrCommand::CodeAction(action)
                if action.is_preferred == Some(true) =>
            {
                Some(action)
            }
            _ => None,
        })
        .next()
        .expect("preferred quick fix");
    let WorkspaceEdit {
        document_changes: Some(lsp_types::DocumentChanges::Edits(edits)),
        ..
    } = action.edit.as_ref().expect("workspace edit")
    else {
        panic!("expected document changes");
    };
    let lsp_types::OneOf::Left(edit) = &edits[0].edits[0] else {
        panic!("expected text edit");
    };
    (action.title.clone(), edit.new_text.clone())
}

#[tokio::test]
async fn published_choice_diagnostic_data_drives_preferred_quick_fix() {
    let service = service();
    let server = service.inner();
    let text = "#choose(edition=breif)";
    let diagnostics = server.diagnostics_for_text(text.to_string()).await;
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "invalid_xprompt_arg_choice"
            )
        })
        .expect("choice diagnostic");
    assert_eq!(
        diagnostic.severity,
        Some(lsp_types::DiagnosticSeverity::ERROR)
    );
    assert!(
        diagnostic.data.is_some(),
        "diagnostic must carry suggestion data"
    );

    let uri = file_uri("/tmp/sase_prompt_choice.md");
    let actions = server
        .code_actions_for_request(
            uri,
            text.to_string(),
            diagnostic.range,
            CodeActionContext {
                diagnostics: vec![diagnostic.clone()],
                only: Some(vec![CodeActionKind::QUICKFIX]),
                ..Default::default()
            },
        )
        .await;
    assert!(
        actions
            .iter()
            .any(|action| action_title(action) == Some("Replace with `brief`")),
        "{actions:?}"
    );
    let (title, new_text) = preferred_edit(&actions);
    assert_eq!(title, "Replace with `brief`");
    assert_eq!(new_text, "brief");

    let start = diagnostic.range.start.character as usize;
    let end = diagnostic.range.end.character as usize;
    let mut edited = text.to_string();
    edited.replace_range(start..end, &new_text);
    assert_eq!(edited, "#choose(edition=brief)");
    let cleaned = server.diagnostics_for_text(edited).await;
    assert!(
        cleaned.iter().all(|diagnostic| {
            !matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "invalid_xprompt_arg_choice"
            )
        }),
        "{cleaned:?}"
    );
}

#[tokio::test]
async fn malformed_diagnostic_data_is_ignored_and_kinds_are_respected() {
    let service = service();
    let server = service.inner();
    let uri = file_uri("/tmp/sase_prompt_choice.md");
    let text = "#choose(edition=breif)";
    let mut diagnostics = server.diagnostics_for_text(text.to_string()).await;
    diagnostics[0].data = Some(serde_json::json!("not-an-object"));
    let ignored = server
        .code_actions_for_request(
            uri.clone(),
            text.to_string(),
            Range {
                start: Position::new(0, 0),
                end: Position::new(0, text.len() as u32),
            },
            CodeActionContext {
                diagnostics: diagnostics.clone(),
                only: Some(vec![CodeActionKind::QUICKFIX]),
                ..Default::default()
            },
        )
        .await;
    assert!(
        ignored.iter().all(|action| {
            action_title(action) != Some("Replace with `brief`")
        }),
        "{ignored:?}"
    );

    let original = server.diagnostics_for_text(text.to_string()).await;
    let refactor_only = server
        .code_actions_for_request(
            uri,
            text.to_string(),
            original[0].range,
            CodeActionContext {
                diagnostics: original,
                only: Some(vec![CodeActionKind::REFACTOR_REWRITE]),
                ..Default::default()
            },
        )
        .await;
    assert!(
        refactor_only.iter().all(|action| {
            action_title(action) != Some("Replace with `brief`")
        }),
        "{refactor_only:?}"
    );
}

#[tokio::test]
async fn hover_shows_choices_agent_and_large_table_truncation() {
    let service = service();
    let server = service.inner();
    let Hover {
        contents: HoverContents::Markup(choices),
        ..
    } = server
        .hover_for_text("#choose(edition=".to_string(), Position::new(0, 16))
        .await
        .unwrap()
    else {
        panic!("expected markdown hover");
    };
    assert!(choices.value.contains("Source: builtin"));
    assert!(choices.value.contains("`brief | full`"));
    assert!(choices.value.contains("| Value | Label | Description |"));

    let Hover {
        contents: HoverContents::Markup(agent),
        ..
    } = server
        .hover_for_text("#assign(who=".to_string(), Position::new(0, 12))
        .await
        .unwrap()
    else {
        panic!("expected agent hover");
    };
    assert!(agent.value.contains("`agent`"), "{}", agent.value);
    assert!(agent.value.contains("Source: builtin"));

    let Hover {
        contents: HoverContents::Markup(large),
        ..
    } = server
        .hover_for_text("#big(mode=".to_string(), Position::new(0, 10))
        .await
        .unwrap()
    else {
        panic!("expected large hover");
    };
    assert!(large.value.contains("… 3 more"), "{}", large.value);
    assert!(
        large.value.contains("plugin sase-research-artifacts"),
        "{}",
        large.value
    );
}
