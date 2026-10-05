use std::sync::Arc;

use lsp_types::{
    CompletionItem, CompletionItemKind, CompletionResponse, CompletionTextEdit,
    Documentation, Position,
};

use sase_core::{
    MobileInputChoiceWire, MobileMacroInputWire, StaticHelperHostBridge,
};

use super::super::*;
use super::support::*;

fn choice_bridge() -> StaticHelperHostBridge {
    bridge_with_catalog_entries(vec![
        catalog_entry(
            "deploy",
            "#deploy",
            Some("(env: staging|prod)".to_string()),
            vec![enum_hint(
                "env",
                true,
                0,
                &[
                    ("staging", Some("Staging"), Some("Pre-prod cluster")),
                    ("prod", Some("Production"), Some("Customer traffic")),
                ],
            )],
            None,
        ),
        catalog_entry(
            "review",
            "#review",
            Some("(deep?: bool)".to_string()),
            vec![{
                let mut hint = input_hint("deep", "bool", false, 0);
                hint.default_display = Some("false".to_string());
                hint
            }],
            None,
        ),
        catalog_entry(
            "data",
            "#data",
            Some("(val: enum)".to_string()),
            vec![enum_hint(
                "val",
                true,
                0,
                &[("a,b", None, None), ("c+d", None, None)],
            )],
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

fn items(response: CompletionResponse) -> (Vec<CompletionItem>, bool) {
    match response {
        CompletionResponse::List(list) => (list.items, list.is_incomplete),
        CompletionResponse::Array(items) => (items, false),
    }
}

async fn complete(text: &str, character: u32) -> (Vec<CompletionItem>, bool) {
    let (service, _) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(choice_bridge()))
    });
    let server = service.inner();
    let response = server
        .completion_for_text(text.to_string(), Position { line: 0, character })
        .await
        .unwrap();
    items(response)
}

#[tokio::test]
async fn named_enum_completes_declared_choices_with_metadata() {
    let (items, incomplete) = complete("#deploy(env=", 12).await;
    assert!(!incomplete, "untruncated enum list must be complete");
    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["staging", "prod"]
    );
    assert_eq!(items[0].kind, Some(CompletionItemKind::ENUM_MEMBER));
    assert_eq!(items[0].filter_text.as_deref(), Some("staging"));
    assert_eq!(items[0].sort_text.as_deref(), Some("0000"));
    assert_eq!(
        items[0]
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("Staging")
    );
    let Some(Documentation::MarkupContent(doc)) =
        items[0].documentation.as_ref()
    else {
        panic!("expected description documentation");
    };
    assert!(doc.value.contains("Pre-prod cluster"));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected full-value text edit");
    };
    assert_eq!(edit.new_text, "staging");
    assert_eq!(edit.range.start, Position::new(0, 12));
    assert_eq!(edit.range.end, Position::new(0, 12));
}

#[tokio::test]
async fn mid_value_completion_replaces_the_whole_current_value() {
    let (items, _) = complete("#deploy(env=staging", 14).await;
    let item = items
        .iter()
        .find(|item| item.label == "staging")
        .expect("staging");
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 12));
    assert_eq!(edit.range.end, Position::new(0, 19));
    assert_eq!(edit.new_text, "staging");
}

#[tokio::test]
async fn colon_and_positional_forms_complete_enum_values() {
    let (colon, _) = complete("#deploy:", 8).await;
    assert_eq!(
        colon
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["staging", "prod"]
    );
    let (positional, _) = complete("#deploy(", 8).await;
    assert!(
        positional.iter().any(|item| item.label == "env="),
        "positional open should still offer the name: {positional:?}"
    );
}

#[tokio::test]
async fn bool_arguments_use_the_shared_choice_builder() {
    let (items, incomplete) = complete("#review(deep=", 13).await;
    assert!(!incomplete);
    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["true", "false"]
    );
    assert_eq!(items[0].kind, Some(CompletionItemKind::ENUM_MEMBER));
    assert_eq!(
        items[1]
            .label_details
            .as_ref()
            .and_then(|details| details.detail.as_deref()),
        Some(" default")
    );
}

#[tokio::test]
async fn quoted_punctuation_inserts_the_encoded_value() {
    let (items, _) = complete("#data(val=", 10).await;
    let comma = items
        .iter()
        .find(|item| item.label == "a,b")
        .expect("comma choice");
    let Some(CompletionTextEdit::Edit(edit)) = comma.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.new_text, "\"a,b\"");
}

#[tokio::test]
async fn utf16_positions_replace_the_value_after_non_bmp() {
    let text = "😀 #deploy(env=st)";
    let (service, _) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(choice_bridge()))
    });
    let server = service.inner();
    let response = server
        .completion_for_text(text.to_string(), Position::new(0, 17))
        .await
        .unwrap();
    let (items, _) = items(response);
    let item = items
        .iter()
        .find(|item| item.label == "staging")
        .expect("staging");
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 15));
    assert_eq!(edit.range.end, Position::new(0, 17));
}

#[tokio::test]
async fn frontmatter_completes_advertised_catalog_types() {
    let text = "---\nname: deploy\ninput:\n  env: \n---\n";
    let (service, _) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(choice_bridge()))
    });
    let server = service.inner();
    let response = server
        .completion_for_document(
            text.to_string(),
            Position::new(3, 7),
            None,
            "markdown",
            None,
            None,
        )
        .await
        .unwrap();
    let (items, incomplete) = items(response);
    assert!(!incomplete);
    let labels: Vec<&str> =
        items.iter().map(|item| item.label.as_str()).collect();
    assert!(labels.contains(&"word"));
    assert!(labels.contains(&"enum"));
    assert!(labels.contains(&"agent"));
    assert!(!labels.contains(&"string"));
    let word = items.iter().find(|item| item.label == "word").unwrap();
    assert_eq!(word.kind, Some(CompletionItemKind::ENUM_MEMBER));
    let Some(Documentation::MarkupContent(doc)) = word.documentation.as_ref()
    else {
        panic!("expected catalog description");
    };
    assert!(!doc.value.is_empty());
    let Some(CompletionTextEdit::Edit(edit)) = word.text_edit.as_ref() else {
        panic!("expected type value edit");
    };
    assert_eq!(edit.new_text, "word");
}

#[tokio::test]
async fn frontmatter_type_field_replaces_the_current_value() {
    let text = "---\ninput:\n  env:\n    type: wor\n---\n";
    let (service, _) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(choice_bridge()))
    });
    let server = service.inner();
    let response = server
        .completion_for_document(
            text.to_string(),
            Position::new(3, 13),
            None,
            "markdown",
            None,
            None,
        )
        .await
        .unwrap();
    let (items, _) = items(response);
    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["word"]
    );
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected type value edit");
    };
    assert_eq!(edit.new_text, "word");
    assert_eq!(edit.range.start, Position::new(3, 10));
    assert_eq!(edit.range.end, Position::new(3, 13));
}

#[tokio::test]
async fn frontmatter_does_not_offer_types_in_the_markdown_body() {
    let text = "---\ninput:\n  env: word\n---\nword\n";
    let (service, _) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(choice_bridge()))
    });
    let server = service.inner();
    let response = server
        .completion_for_document(
            text.to_string(),
            Position::new(4, 4),
            None,
            "markdown",
            None,
            None,
        )
        .await;
    if let Some(response) = response {
        let (items, _) = items(response);
        assert!(
            !items.iter().any(|item| item.label == "enum"),
            "body must not complete input types: {items:?}"
        );
    }
}
