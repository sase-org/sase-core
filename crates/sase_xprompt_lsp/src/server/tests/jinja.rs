use std::path::PathBuf;
use std::sync::Arc;

use lsp_types::{
    CompletionItemKind, CompletionItemTag, CompletionResponse,
    CompletionTextEdit, CompletionTriggerKind, Documentation, Position,
};

use super::super::*;
use super::support::*;

fn test_server() -> LspService<XpromptLspServer> {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    service
}

fn position_at(text: &str, offset: usize) -> Position {
    let mut line = 0u32;
    let mut character = 0u32;
    for (idx, ch) in text.char_indices() {
        if idx >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            character = 0;
        } else {
            character += 1;
        }
    }
    Position::new(line, character)
}

async fn items_at(
    server: &XpromptLspServer,
    text: &str,
    character: u32,
) -> Vec<CompletionItem> {
    items_at_offset(server, text, character as usize).await
}

async fn items_at_offset(
    server: &XpromptLspServer,
    text: &str,
    offset: usize,
) -> Vec<CompletionItem> {
    let position = position_at(text, offset);
    let response = server
        .completion_for_text(text.to_string(), position)
        .await
        .unwrap_or_else(|| panic!("expected a response for {text:?}"));
    match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    }
}

fn labels(items: &[CompletionItem]) -> Vec<&str> {
    items.iter().map(|item| item.label.as_str()).collect()
}

#[tokio::test]
async fn frontmatter_inputs_listed_first() {
    let service = test_server();
    let server = service.inner();
    let text = "---\ninput:\n  topic: word\n---\n{{ }}";
    let character = text.find("{{ }}").unwrap() as u32 + 3;
    let items = items_at(server, text, character).await;
    let ordered = labels(&items);
    let topic = ordered.iter().position(|name| *name == "topic").unwrap();
    let root = ordered.iter().position(|name| *name == "root").unwrap();
    assert!(topic < root, "{ordered:?}");
    let input = items.iter().find(|item| item.label == "topic").unwrap();
    assert_eq!(input.kind, Some(CompletionItemKind::VARIABLE));
    let details = input.label_details.as_ref().expect("label details");
    assert!(
        details
            .description
            .as_deref()
            .unwrap_or("")
            .contains("input"),
        "{details:?}"
    );
}

#[tokio::test]
async fn conditional_n_without_repeat_is_dimmed_with_hint() {
    let service = test_server();
    let server = service.inner();
    let items = items_at(server, "{{ }}", 3).await;
    let item = items
        .iter()
        .find(|item| item.label == "n")
        .expect("expected conditional n");
    let description = item
        .label_details
        .as_ref()
        .and_then(|details| details.description.as_deref())
        .unwrap_or("");
    assert!(description.contains("needs %repeat"), "{item:?}");
    let Some(Documentation::MarkupContent(documentation)) =
        item.documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    assert!(
        documentation.value.contains("%repeat"),
        "{}",
        documentation.value
    );
}

#[tokio::test]
async fn repeat_makes_n_available() {
    let service = test_server();
    let server = service.inner();
    let text = "%repeat:2\n{{ }}";
    let items = items_at(server, text, text.len() as u32 - 3).await;
    assert!(labels(&items).contains(&"n"));
}

#[tokio::test]
async fn run_time_names_hidden_in_input_declaring_prompt() {
    let service = test_server();
    let server = service.inner();
    let text = "---\ninput:\n  topic: word\n---\n{{ }}";
    let character = text.find("{{ }}").unwrap() as u32 + 3;
    let items = items_at(server, text, character).await;
    assert!(
        !labels(&items).contains(&"patch_name"),
        "{:?}",
        labels(&items)
    );
}

#[tokio::test]
async fn xprompt_path_offers_positional_args() {
    let service = test_server();
    let server = service.inner();
    let path = PathBuf::from("/repo/xprompts/review.md");
    let response = server
        .completion_for_document(
            "{{ }}".to_string(),
            Position::new(0, 3),
            Some(path),
            "markdown",
            None,
            None,
        )
        .await
        .expect("expected a response");
    let items = match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    };
    assert!(labels(&items).contains(&"_args"), "{:?}", labels(&items));
}

#[tokio::test]
async fn skill_frontmatter_offers_provider_vars() {
    let service = test_server();
    let server = service.inner();
    let path = PathBuf::from("/repo/xprompts/skill.md");
    let text = "---\nskill: true\n---\n{{ }}";
    let offset = text.find("{{ }}").unwrap() + 3;
    let response = server
        .completion_for_document(
            text.to_string(),
            position_at(text, offset),
            Some(path),
            "markdown",
            None,
            None,
        )
        .await
        .expect("expected a response");
    let items = match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    };
    assert!(
        labels(&items).contains(&"provider_name"),
        "{:?}",
        labels(&items)
    );
}

#[tokio::test]
async fn filters_complete_after_pipe() {
    let service = test_server();
    let server = service.inner();
    let items = items_at(server, "{{ x | }}", 7).await;
    assert!(labels(&items).contains(&"join"), "{:?}", labels(&items));
    assert!(
        items
            .iter()
            .all(|item| item.kind == Some(CompletionItemKind::FUNCTION)),
        "{items:?}"
    );
}

#[tokio::test]
async fn members_complete_after_wait_dot() {
    let service = test_server();
    let server = service.inner();
    let items = items_at(server, "{{ wait. }}", 8).await;
    assert!(labels(&items).contains(&"chats"), "{:?}", labels(&items));
    assert_eq!(
        items
            .iter()
            .find(|item| item.label == "chats")
            .and_then(|item| item.kind),
        Some(CompletionItemKind::FIELD)
    );
}

#[tokio::test]
async fn statement_closers_lead_inside_for() {
    let service = test_server();
    let server = service.inner();
    let text = "{% for x in y %}{%  %}";
    let character = text.find("{%  %}").unwrap() as u32 + 3;
    let response = server
        .completion_for_text(text.to_string(), Position::new(0, character))
        .await
        .expect("expected a response");
    let items = match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    };
    assert_eq!(items[0].label, "endfor", "{:?}", labels(&items));
    assert_eq!(items[0].kind, Some(CompletionItemKind::KEYWORD));
}

#[tokio::test]
async fn fenced_code_has_no_jinja_menu() {
    let service = test_server();
    let server = service.inner();
    let text = "```\n{{  }}\n```";
    // Cursor inside the fenced block: the engine returns None, so
    // the server falls through past Jinja (and finds nothing else).
    let response = server
        .completion_for_text(text.to_string(), Position::new(1, 3))
        .await;
    if let Some(response) = response {
        let items = match response {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => list.items,
        };
        assert!(
            !labels(&items).contains(&"patch_name"),
            "{:?}",
            labels(&items)
        );
    }
}

#[tokio::test]
async fn gitcommit_has_no_jinja_completion() {
    let service = test_server();
    let server = service.inner();
    let response = server
        .completion_for_document(
            "{{ }}".to_string(),
            Position::new(0, 3),
            None,
            "gitcommit",
            None,
            None,
        )
        .await;
    if let Some(response) = response {
        let items = match response {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => list.items,
        };
        assert!(
            !labels(&items).contains(&"patch_name"),
            "{:?}",
            labels(&items)
        );
    }
}

#[tokio::test]
async fn angle_bracket_inside_tag_stays_jinja() {
    let service = test_server();
    let server = service.inner();
    // Regression: `{{ a < b` was misread as a `<placeholder>`.
    let items = items_at(server, "{{ a < b }}", 7).await;
    assert!(!items.is_empty());
    assert!(
        items
            .iter()
            .any(|item| item.kind == Some(CompletionItemKind::VARIABLE)),
        "{items:?}"
    );
}

#[tokio::test]
async fn brace_trigger_outside_tag_returns_none() {
    let service = test_server();
    let server = service.inner();
    let response = server
        .completion_for_document(
            "plain { text".to_string(),
            Position::new(0, 8),
            None,
            "markdown",
            Some(CompletionTriggerKind::TRIGGER_CHARACTER),
            Some("{".to_string()),
        )
        .await;
    assert!(response.is_none(), "{response:?}");
}

#[tokio::test]
async fn pipe_trigger_outside_tag_returns_none() {
    let service = test_server();
    let server = service.inner();
    let response = server
        .completion_for_document(
            "%{a | b}".to_string(),
            Position::new(0, 5),
            None,
            "markdown",
            Some(CompletionTriggerKind::TRIGGER_CHARACTER),
            Some("|".to_string()),
        )
        .await;
    assert!(response.is_none(), "{response:?}");
}

#[tokio::test]
async fn empty_in_tag_slot_claims_cursor_with_empty_menu() {
    let service = test_server();
    let server = service.inner();
    // `{% set x` is a new-name position: Jinja returns Some with
    // empty items and never falls through to other surfaces.
    let response = server
        .completion_for_text("{% set x".to_string(), Position::new(0, 8))
        .await
        .expect("Jinja claims the in-tag cursor");
    let items = match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    };
    assert!(items.is_empty());
}

#[tokio::test]
async fn completion_item_carries_edits_sort_and_deprecation() {
    let service = test_server();
    let server = service.inner();
    let text = "---\ninput:\n  topic: word\n---\n{{ }}";
    let character = text.find("{{ }}").unwrap() as u32 + 3;
    let items = items_at(server, text, character).await;
    assert_eq!(items[0].sort_text.as_deref(), Some("0000"));
    assert_eq!(items[0].preselect, Some(true));
    assert_eq!(items[1].preselect, None);
    for item in &items {
        assert_eq!(item.filter_text.as_deref(), Some(item.label.as_str()));
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected a text edit for {}", item.label);
        };
        assert_eq!(edit.range.start.line, 4, "{item:?}");
    }
    // `cl_name` is the legacy alias of `patch_name`: it sorts right
    // after its canonical name and carries DEPRECATED.
    let plain = items_at(server, "{{ cl_ }}", 6).await;
    let index = labels(&plain)
        .iter()
        .position(|name| *name == "cl_name")
        .expect("cl_name is offered for prefix cl_");
    let item = &plain[index];
    assert!(
        item.tags
            .as_deref()
            .unwrap_or(&[])
            .contains(&CompletionItemTag::DEPRECATED),
        "{item:?}"
    );
}

#[tokio::test]
async fn hover_shows_engine_markdown() {
    let service = test_server();
    let server = service.inner();
    let text = "{{ patch_name }}".to_string();
    let hover = server
        .hover_for_text(text, Position::new(0, 4))
        .await
        .expect("expected hover");
    let lsp_types::HoverContents::Markup(content) = hover.contents else {
        panic!("expected markup hover");
    };
    assert!(content.value.contains("patch_name"), "{}", content.value);
}

#[tokio::test]
async fn hover_resolves_filter() {
    let service = test_server();
    let server = service.inner();
    let text = "{{ x | join }}".to_string();
    let hover = server
        .hover_for_text(text, Position::new(0, 9))
        .await
        .expect("expected filter hover");
    let lsp_types::HoverContents::Markup(content) = hover.contents else {
        panic!("expected markup hover");
    };
    assert!(content.value.contains("join"), "{}", content.value);
}
