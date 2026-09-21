use std::{path::Path, sync::Arc};

use lsp_types::{
    CompletionItemKind, CompletionResponse, CompletionTextEdit, Documentation,
    Position,
};

use super::super::*;

use super::support::*;

#[tokio::test]
async fn advertises_plus_completion_trigger_character() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let triggers = result
        .capabilities
        .completion_provider
        .and_then(|completion| completion.trigger_characters)
        .unwrap_or_default();

    assert!(triggers.contains(&"+".to_string()), "{triggers:?}");
}

#[tokio::test]
async fn advertises_placeholder_completion_trigger_character() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let triggers = result
        .capabilities
        .completion_provider
        .and_then(|completion| completion.trigger_characters)
        .unwrap_or_default();

    assert!(triggers.contains(&"<".to_string()), "{triggers:?}");
}

#[tokio::test]
async fn advertises_at_reference_completion_trigger_character() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let triggers = result
        .capabilities
        .completion_provider
        .and_then(|completion| completion.trigger_characters)
        .unwrap_or_default();

    assert!(triggers.contains(&"@".to_string()), "{triggers:?}");
}

#[tokio::test]
async fn advertises_slash_completion_trigger_character() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let triggers = result
        .capabilities
        .completion_provider
        .and_then(|completion| completion.trigger_characters)
        .unwrap_or_default();

    assert!(triggers.contains(&"/".to_string()), "{triggers:?}");
}

#[tokio::test]
async fn advertises_vcs_ref_completion_trigger_characters() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let triggers = result
        .capabilities
        .completion_provider
        .and_then(|completion| completion.trigger_characters)
        .unwrap_or_default();

    assert!(triggers.contains(&":".to_string()), "{triggers:?}");
    assert!(triggers.contains(&"(".to_string()), "{triggers:?}");
}

#[tokio::test]
async fn advertises_model_shortcut_trigger_character() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let triggers = result
        .capabilities
        .completion_provider
        .and_then(|completion| completion.trigger_characters)
        .unwrap_or_default();

    assert!(triggers.contains(&"=".to_string()), "{triggers:?}");
    assert!(!triggers.contains(&"*".to_string()), "{triggers:?}");
}

// --- `=alias` shortcut completion ---------------------------------------

fn model_alias_shortcut_service(
    catalog_path: Option<&Path>,
) -> LspService<XpromptLspServer> {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    if let Some(path) = catalog_path {
        let mut config = service.inner().config.write().unwrap();
        config.model_catalog = Some(path.to_path_buf());
    }
    service
}

async fn shortcut_items_at(
    server: &XpromptLspServer,
    text: &str,
    line: u32,
    character: u32,
) -> Vec<CompletionItem> {
    let response = server
        .completion_for_text(text.to_string(), Position::new(line, character))
        .await
        .unwrap_or_else(|| panic!("expected a response for {text:?}"));
    let CompletionResponse::List(list) = response else {
        panic!("expected an incomplete shortcut list for {text:?}");
    };
    assert!(list.is_incomplete, "expected isIncomplete for {text:?}");
    list.items
}

#[tokio::test]
async fn model_alias_shortcut_offers_alias_rows_only_in_catalog_order() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "=", 0, 1).await;

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@default", "@claude_coder", "@scout"]
    );
}

#[tokio::test]
async fn model_alias_shortcut_filters_by_partial_case_insensitive_query() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let lower = shortcut_items_at(server, "=sc", 0, 3).await;
    assert_eq!(
        lower
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@scout"]
    );

    let upper = shortcut_items_at(server, "=SC", 0, 3).await;
    assert_eq!(
        upper
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@scout"]
    );
}

#[tokio::test]
async fn model_alias_shortcut_detects_later_line_and_after_leading_space() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items =
        shortcut_items_at(server, "Explain the plan.\n  =sc", 1, 5).await;

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@scout"]
    );
}

#[tokio::test]
async fn model_alias_shortcut_sets_filter_text_sort_text_and_preselect() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "=", 0, 1).await;

    assert_eq!(items.len(), 3);
    for item in &items {
        assert_eq!(item.filter_text.as_deref(), Some("="));
    }
    assert_eq!(items[0].sort_text.as_deref(), Some("0000"));
    assert_eq!(items[1].sort_text.as_deref(), Some("0001"));
    assert_eq!(items[2].sort_text.as_deref(), Some("0002"));
    assert_eq!(items[0].preselect, Some(true));
    assert_eq!(items[1].preselect, None);
    assert_eq!(items[2].preselect, None);
}

#[tokio::test]
async fn model_alias_shortcut_shows_expansion_detail_and_metadata_documentation(
) {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "=sc", 0, 3).await;
    let item = &items[0];

    assert_eq!(item.label, "@scout");
    assert_eq!(item.kind, Some(CompletionItemKind::ENUM_MEMBER));
    let details = item.label_details.as_ref().expect("label details");
    assert_eq!(details.detail.as_deref(), Some(" → %m:@scout"));
    assert_eq!(details.description.as_deref(), Some("custom"));
    assert_eq!(item.detail.as_deref(), Some("CODEX(gpt-5.6-sol) @ low"));
    let Some(Documentation::MarkupContent(documentation)) =
        item.documentation.as_ref()
    else {
        panic!("expected alias documentation");
    };
    assert!(
        documentation.value.contains("Fast scouting pool."),
        "{}",
        documentation.value
    );
    assert!(
        documentation
            .value
            .contains("**Config:** `llm_provider.model_aliases.custom.scout`"),
        "{}",
        documentation.value
    );
    assert!(
        documentation.value.contains("**Pool:** 2/3 available"),
        "{}",
        documentation.value
    );
}

#[tokio::test]
async fn model_alias_shortcut_replaces_whole_token_from_mid_token_caret() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    // Caret sits between "sc" and the trailing "X" garbage; the whole
    // "=scX" token is replaced, "X" included, not just the typed prefix.
    let items = shortcut_items_at(server, "Use =scX later", 0, 7).await;
    assert_eq!(items.len(), 1);
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected a text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 4));
    assert_eq!(edit.range.end, Position::new(0, 9));
    assert_eq!(edit.new_text, "%m:@scout ");
}

#[tokio::test]
async fn model_alias_shortcut_text_edit_covers_every_trailing_whitespace_case()
{
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    for (text, character, expected_end, expected_new_text) in [
        ("Use =sc", 7, 7, "%m:@scout "),
        ("Use =sc\tnow", 7, 7, "%m:@scout"),
        ("Use =sc\nnow", 7, 7, "%m:@scout "),
    ] {
        let items = shortcut_items_at(server, text, 0, character).await;
        assert_eq!(items.len(), 1, "text={text:?}");
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected a text edit for {text:?}");
        };
        assert_eq!(edit.range.start, Position::new(0, 4), "text={text:?}");
        assert_eq!(
            edit.range.end,
            Position::new(0, expected_end),
            "text={text:?}"
        );
        assert_eq!(edit.new_text, expected_new_text, "text={text:?}");
    }
}

#[tokio::test]
async fn model_alias_shortcut_no_match_returns_empty_list_not_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "=zzz", 0, 4).await;

    assert!(items.is_empty());
}

#[tokio::test]
async fn model_alias_shortcut_missing_catalog_returns_empty_list() {
    let service = model_alias_shortcut_service(None);
    let server = service.inner();

    let items = shortcut_items_at(server, "=", 0, 1).await;

    assert!(items.is_empty());
}

#[tokio::test]
async fn model_alias_shortcut_malformed_catalog_returns_empty_list() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    fs::write(&catalog_path, "not json").unwrap();
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "=", 0, 1).await;

    assert!(items.is_empty());
}

#[tokio::test]
async fn model_alias_shortcut_leaves_protected_equals_to_ordinary_completion() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    // An equals marker inside an active `%model:` value is excluded by the shared
    // detector, so ordinary `%model:` value completion still answers —
    // it is never hijacked into (or dropped by) the shortcut path.
    let response = server
        .completion_for_text("%model:=la".to_string(), Position::new(0, 10))
        .await
        .expect("expected ordinary directive completion");
    assert!(
        matches!(response, CompletionResponse::Array(_)),
        "expected the ordinary %model: completion array, not a shortcut list"
    );
}

#[tokio::test]
async fn legacy_star_shortcuts_do_not_open_model_shortcut_completion() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    for (text, character) in [
        ("*", 1),
        ("*sc", 3),
        ("**", 2),
        ("**gpt", 5),
        ("Use *sc", 7),
        ("Use **gpt", 9),
    ] {
        let response = server
            .completion_for_text(text.to_string(), Position::new(0, character))
            .await;
        assert!(
            !completion_response_contains_model_shortcut_edit(response),
            "legacy star text should not produce a model shortcut edit for {text:?}"
        );
    }
}

fn completion_response_contains_model_shortcut_edit(
    response: Option<CompletionResponse>,
) -> bool {
    let Some(response) = response else {
        return false;
    };
    let items = match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    };
    items.into_iter().any(|item| {
        matches!(
            item.text_edit,
            Some(CompletionTextEdit::Edit(edit))
                if edit.new_text.starts_with("%m:")
        )
    })
}

#[tokio::test]
async fn model_shortcut_offers_model_rows_only_in_catalog_order() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "==", 0, 2).await;

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["opus", "gpt-5.6-sol"]
    );
    assert!(items
        .iter()
        .all(|item| item.kind == Some(CompletionItemKind::VALUE)));
    assert_eq!(items[0].filter_text.as_deref(), Some("=="));
    assert_eq!(items[0].preselect, Some(true));
}

#[tokio::test]
async fn model_shortcut_filters_and_replaces_provider_scoped_rows() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog_with_providers(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "Use ==claude/fa", 0, 15).await;

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "claude/claude-fable-5");
    assert_eq!(item.filter_text.as_deref(), Some("==claude/fa"));
    let details = item.label_details.as_ref().expect("label details");
    assert_eq!(
        details.detail.as_deref(),
        Some(" → %m:claude/claude-fable-5")
    );
    assert_eq!(details.description.as_deref(), Some("model"));
    assert_eq!(
        item.detail.as_deref(),
        Some("%m:claude/claude-fable-5 · claude")
    );
    let Some(Documentation::MarkupContent(documentation)) =
        item.documentation.as_ref()
    else {
        panic!("expected model documentation");
    };
    assert!(
        documentation
            .value
            .contains("**Expansion:** `%m:claude/claude-fable-5`"),
        "{}",
        documentation.value
    );
    assert!(documentation.value.contains("Claude (fable)"));
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 4));
    assert_eq!(edit.range.end, Position::new(0, 15));
    assert_eq!(edit.new_text, "%m:claude/claude-fable-5 ");
}

#[tokio::test]
async fn model_shortcut_matches_short_hint_but_inserts_canonical_model() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "Use ==fa", 0, 8).await;

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "claude-fable-5");
    assert_eq!(items[0].filter_text.as_deref(), Some("==fa"));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 4));
    assert_eq!(edit.range.end, Position::new(0, 8));
    assert_eq!(edit.new_text, "%m:claude-fable-5 ");
}

#[tokio::test]
async fn model_shortcut_skips_unsafe_model_values_and_keeps_owned_empty_lists()
{
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog_with_unsafe_shortcut_rows(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let items = shortcut_items_at(server, "Use ==gpt", 0, 9).await;

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["gpt-safe"]
    );
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected text edit");
    };
    assert_eq!(edit.new_text, "%m:gpt-safe ");

    let unsafe_only = shortcut_items_at(server, "Use ==unsafe", 0, 12).await;
    assert!(unsafe_only.is_empty());
}

#[tokio::test]
async fn model_shortcut_backspace_transitions_between_marker_kinds() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_enriched_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let filtered = shortcut_items_at(server, "==gpt", 0, 5).await;
    assert_eq!(
        filtered
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["gpt-5.6-sol"]
    );

    let bare_model = shortcut_items_at(server, "==", 0, 2).await;
    assert_eq!(
        bare_model
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["opus", "gpt-5.6-sol"]
    );

    let alias = shortcut_items_at(server, "=", 0, 1).await;
    assert_eq!(
        alias
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@default", "@claude_coder", "@scout"]
    );
}

#[tokio::test]
async fn model_shortcut_empty_and_protected_contexts_do_not_fall_through() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    write_model_catalog(&catalog_path);
    let service = model_alias_shortcut_service(Some(&catalog_path));
    let server = service.inner();

    let no_match = shortcut_items_at(server, "==zzz", 0, 5).await;
    assert!(no_match.is_empty());

    let missing_catalog_service = model_alias_shortcut_service(None);
    let missing =
        shortcut_items_at(missing_catalog_service.inner(), "==", 0, 2).await;
    assert!(missing.is_empty());

    let response = server
        .completion_for_text("%model:==gpt".to_string(), Position::new(0, 12))
        .await
        .expect("expected ordinary directive completion");
    assert!(
        matches!(response, CompletionResponse::Array(_)),
        "expected protected directive context to avoid shortcut list"
    );
}

#[tokio::test]
async fn advertises_full_semantic_tokens_with_standard_legend() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let provider = result
        .capabilities
        .semantic_tokens_provider
        .expect("semantic tokens provider");
    let SemanticTokensServerCapabilities::SemanticTokensOptions(options) =
        provider
    else {
        panic!("expected semantic token options");
    };

    assert_eq!(
        options
            .legend
            .token_types
            .iter()
            .map(|token_type| token_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "namespace",
            "string",
            "number",
            "type",
            "function",
            "macro",
            "parameter",
            "operator",
            "keyword"
        ]
    );
    assert_eq!(
        options
            .legend
            .token_modifiers
            .iter()
            .map(|modifier| modifier.as_str())
            .collect::<Vec<_>>(),
        vec!["documentation", "deprecated"]
    );
    assert!(matches!(
        options.full,
        Some(SemanticTokensFullOptions::Bool(true))
    ));
    assert_eq!(options.range, None);
}
