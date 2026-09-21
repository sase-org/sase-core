use std::sync::Arc;

use lsp_types::{
    CompletionItemKind, CompletionResponse, CompletionTextEdit, Documentation,
    Position,
};

use super::super::*;

use super::support::*;

use super::super::catalogs::load_model_catalog;

#[test]
fn load_model_catalog_rejects_unknown_schema() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    fs::write(&catalog_path, r#"{"schema_version": 99, "entries": []}"#)
        .unwrap();

    let entries = load_model_catalog(Some(&catalog_path));

    assert!(entries.is_empty());
}

#[tokio::test]
async fn completes_model_directive_values_from_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:".to_string(), Position::new(0, 7))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 3);
    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["claude-fable-5", "gpt-5.6-sol", "gpt-5.5"]
    );
    let item = &items[0];
    assert_eq!(item.label, "claude-fable-5");
    assert_eq!(item.filter_text.as_deref(), Some("claude-fable-5"));
    assert_eq!(item.kind, Some(CompletionItemKind::VALUE));
    assert_eq!(item.detail.as_deref(), Some("claude"));
    assert_eq!(item.sort_text.as_deref(), Some("0:0000"));
    assert_eq!(
        item.label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("model")
    );
    let Some(Documentation::MarkupContent(documentation)) =
        item.documentation.as_ref()
    else {
        panic!("expected model documentation");
    };
    assert_eq!(documentation.value, "Claude (fable)");
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 7));
    assert_eq!(edit.range.end, Position::new(0, 7));
    assert_eq!(edit.new_text, "claude-fable-5");
}

#[tokio::test]
async fn enriched_model_catalog_renders_alias_detail_and_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:".to_string(), Position::new(0, 7))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec![
            "opus",
            "gpt-5.6-sol",
            "@default",
            "@claude_coder",
            "@scout",
            "claude/",
            "codex/"
        ]
    );
    assert_eq!(
        items.iter().map(|item| item.kind).collect::<Vec<_>>(),
        vec![
            Some(CompletionItemKind::VALUE),
            Some(CompletionItemKind::VALUE),
            Some(CompletionItemKind::ENUM_MEMBER),
            Some(CompletionItemKind::ENUM_MEMBER),
            Some(CompletionItemKind::ENUM_MEMBER),
            Some(CompletionItemKind::VALUE),
            Some(CompletionItemKind::VALUE),
        ]
    );
    assert_eq!(
        items
            .iter()
            .map(|item| item.sort_text.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "0:0000", "0:0001", "1:0002", "1:0003", "1:0004", "2:0005",
            "2:0006"
        ]
    );

    let default = items.iter().find(|item| item.label == "@default").unwrap();
    assert_eq!(default.detail.as_deref(), Some("CLAUDE(opus) @ high"));
    assert_eq!(
        default
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("default")
    );
    let Some(Documentation::MarkupContent(documentation)) =
        default.documentation.as_ref()
    else {
        panic!("expected alias documentation");
    };
    assert_eq!(
        documentation.value,
        "Default model for prompts.\n\n\
         **Provenance:** implicit → @coder @ medium"
    );

    let coder = items
        .iter()
        .find(|item| item.label == "@claude_coder")
        .unwrap();
    assert_eq!(
        coder
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("coder")
    );

    let scout = items.iter().find(|item| item.label == "@scout").unwrap();
    assert_eq!(scout.detail.as_deref(), Some("CODEX(gpt-5.6-sol) @ low"));
    assert_eq!(
        scout
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("custom")
    );
    let provider = items.iter().find(|item| item.label == "claude/").unwrap();
    assert_eq!(
        provider
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("provider")
    );
    let Some(Documentation::MarkupContent(documentation)) =
        scout.documentation.as_ref()
    else {
        panic!("expected pooled alias documentation");
    };
    assert_eq!(
        documentation.value,
        "Fast scouting pool.\n\n\
         **Provenance:** configured\n\n\
         **Config:** `llm_provider.model_aliases.custom.scout`\n\n\
         **Bucket:** `fast`\n\n\
         **Pool:** 2/3 available"
    );
}

#[tokio::test]
async fn leading_at_filters_model_completion_to_aliases() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:@".to_string(), Position::new(0, 8))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@default", "@claude_coder", "@scout"]
    );
    assert!(items
        .iter()
        .all(|item| item.kind == Some(CompletionItemKind::ENUM_MEMBER)));
}

#[tokio::test]
async fn provider_scoped_model_directive_completion_returns_qualified_rows() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog_with_providers(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:claude/".to_string(), Position::new(0, 14))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["claude/claude-fable-5", "claude/opus"]
    );
    assert_eq!(
        items
            .iter()
            .map(|item| item.filter_text.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["claude/claude-fable-5", "claude/opus"]
    );
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 7));
    assert_eq!(edit.range.end, Position::new(0, 14));
    assert_eq!(edit.new_text, "claude/claude-fable-5");
}

#[tokio::test]
async fn provider_scoped_model_directive_completion_matches_short_alias() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog_with_providers(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text(
            "%model:claude/fa".to_string(),
            Position::new(0, 16),
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "claude/claude-fable-5");
    assert_eq!(items[0].filter_text.as_deref(), Some("claude/fable"));
}

#[tokio::test]
async fn provider_scoped_model_directive_completion_uses_first_slash() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog_with_providers(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let scoped = server
        .completion_for_text(
            "%model:opencode/anthropic/".to_string(),
            Position::new(0, 26),
        )
        .await
        .unwrap();
    let CompletionResponse::Array(scoped_items) = scoped else {
        panic!("expected completion array");
    };
    assert_eq!(
        scoped_items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["opencode/anthropic/claude-sonnet-4-5"]
    );

    let fallback = server
        .completion_for_text(
            "%model:anthropic/".to_string(),
            Position::new(0, 17),
        )
        .await
        .unwrap();
    let CompletionResponse::Array(fallback_items) = fallback else {
        panic!("expected completion array");
    };
    assert_eq!(
        fallback_items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["anthropic/claude-sonnet-4-5"]
    );
}

#[tokio::test]
async fn provider_scope_requires_provider_catalog_entry_for_old_catalogs() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:claude/".to_string(), Position::new(0, 14))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert!(items.is_empty());
}

#[tokio::test]
async fn stale_v1_alias_catalog_still_produces_items() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    fs::write(
        &catalog_path,
        r#"{
            "schema_version": 1,
            "entries": [{
                "value": "@default",
                "display": "@default",
                "description": "alias for the default model",
                "kind": "implicit_alias",
                "provider": "",
                "aliases": ["default"]
            }]
        }"#,
    )
    .unwrap();
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:def".to_string(), Position::new(0, 10))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "@default");
    assert_eq!(items[0].filter_text.as_deref(), Some("default"));
    assert_eq!(items[0].kind, Some(CompletionItemKind::ENUM_MEMBER));
    assert_eq!(
        items[0].detail.as_deref(),
        Some("alias for the default model")
    );
}

#[tokio::test]
async fn model_directive_completion_filters_by_alias_hint() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:fa".to_string(), Position::new(0, 9))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "claude-fable-5");
    assert_eq!(item.filter_text.as_deref(), Some("fable"));
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 7));
    assert_eq!(edit.range.end, Position::new(0, 9));
    assert_eq!(edit.new_text, "claude-fable-5");
}

#[tokio::test]
async fn model_directive_completion_without_catalog_is_empty() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = None;
    }

    let response = server
        .completion_for_text("%model:".to_string(), Position::new(0, 7))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert!(items.is_empty());
}

#[tokio::test]
async fn model_at_suffix_still_completes_effort_vocabulary() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.model_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("%model:opus@".to_string(), Position::new(0, 12))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert!(items.iter().any(|item| item.label == "xhigh"));
    let xhigh = items.iter().find(|item| item.label == "xhigh").unwrap();
    let Some(CompletionTextEdit::Edit(edit)) = xhigh.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 12));
    assert_eq!(edit.range.end, Position::new(0, 12));
    assert_eq!(edit.new_text, "xhigh");
}
