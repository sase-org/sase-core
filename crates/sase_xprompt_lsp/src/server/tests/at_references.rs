use std::sync::Arc;

use lsp_types::{
    CompletionResponse, CompletionTextEdit, CompletionTriggerKind,
    GotoDefinitionResponse, Position, Range,
};

use super::super::*;

use super::support::*;

#[tokio::test]
async fn artifact_payload_inventory_cache_rebuilds_on_all_invalidation_paths() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
    let designs = temp.path().join("sase/designs");
    fs::create_dir_all(&designs).unwrap();
    fs::write(designs.join("first.md"), "first").unwrap();

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().artifact_ref_catalog =
        Some(artifact_path.clone());

    let items = completion_items(
        server
            .completion_for_text(
                "@designs:first".to_string(),
                Position::new(0, 14),
            )
            .await
            .unwrap(),
    );
    assert_eq!(items.len(), 1);

    // A stable catalog signature reuses the cached filesystem inventory.
    fs::write(designs.join("second.md"), "second").unwrap();
    let items = completion_items(
        server
            .completion_for_text(
                "@designs:second".to_string(),
                Position::new(0, 15),
            )
            .await
            .unwrap(),
    );
    assert!(items.is_empty());

    // A launcher catalog rewrite invalidates by path metadata.
    let mut raw = fs::read(&artifact_path).unwrap();
    raw.push(b'\n');
    fs::write(&artifact_path, raw).unwrap();
    let items = completion_items(
        server
            .completion_for_text(
                "@designs:second".to_string(),
                Position::new(0, 15),
            )
            .await
            .unwrap(),
    );
    assert_eq!(items.len(), 1);

    // The explicit refresh command invalidates even when the catalog file
    // itself is unchanged.
    fs::write(designs.join("third.md"), "third").unwrap();
    server.refresh_catalog_explicit().await;
    let items = completion_items(
        server
            .completion_for_text(
                "@designs:third".to_string(),
                Position::new(0, 14),
            )
            .await
            .unwrap(),
    );
    assert_eq!(items.len(), 1);

    // The short TTL eventually notices sidecar writes that do not touch
    // the launcher catalog.
    fs::write(designs.join("fourth.md"), "fourth").unwrap();
    server.artifact_ref_cache.write().unwrap().loaded_at =
        Some(Instant::now() - ARTIFACT_REF_CACHE_TTL);
    let items = completion_items(
        server
            .completion_for_text(
                "@designs:fourth".to_string(),
                Position::new(0, 15),
            )
            .await
            .unwrap(),
    );
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn artifact_completion_discloses_the_display_cap() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
    let designs = temp.path().join("sase/designs");
    fs::create_dir_all(&designs).unwrap();
    for index in 0..205 {
        fs::write(designs.join(format!("{index:03}.md")), "design").unwrap();
    }

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().artifact_ref_catalog = Some(artifact_path);

    let items = completion_items(
        server
            .completion_for_text("@designs:".to_string(), Position::new(0, 9))
            .await
            .unwrap(),
    );

    assert_eq!(
        items.len(),
        sase_core::editor::at_reference::AT_REFERENCE_MAX_GROUP_ROWS
    );
    assert!(items.iter().all(|item| item
        .detail
        .as_deref()
        .is_some_and(|detail| detail
            .contains("at least 5 additional payloads not shown"))));
}

#[tokio::test]
async fn fuzzy_at_reference_payloads_survive_client_filtering() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
    let bundle = temp
        .path()
        .join("sase/designs/202607/sase_sites_hub_and_pages");
    fs::create_dir_all(&bundle).unwrap();
    fs::write(
        bundle.join("sase_sites_hub_and_pages.md"),
        "---\ntitle: SASE Sites Hub and Pages\n---\n",
    )
    .unwrap();

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().artifact_ref_catalog = Some(artifact_path);

    let text = "@designs:site";
    let response = server
        .completion_for_text(
            text.to_string(),
            Position::new(0, text.len() as u32),
        )
        .await
        .unwrap();
    let CompletionResponse::List(list) = &response else {
        panic!("expected an incomplete list: {response:?}");
    };
    assert!(list.is_incomplete);

    let payload = "202607/sase_sites_hub_and_pages/\
                   sase_sites_hub_and_pages.md";
    let items = completion_items(response);
    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec![format!("@designs:{payload}").as_str()]
    );
    // Every item filters on the typed text, so a client that prefix-filters
    // `filterText` against the typed word keeps the server-ranked rows.
    assert!(items
        .iter()
        .all(|item| item.filter_text.as_deref() == Some(text)));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected an artifact payload text edit");
    };
    assert_eq!(edit.new_text, format!("@designs:{payload}"));
    let Some(lsp_types::Documentation::MarkupContent(documentation)) =
        items[0].documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    // The matched run is bolded in the basename the query was aimed at, and
    // the document frontmatter title is carried into the preview.
    assert_eq!(
        documentation.value,
        "202607/sase_sites_hub_and_pages/sase_**site**s_hub_and_pages.md\n\nSASE Sites Hub and Pages"
    );
    assert_eq!(
        items[0]
            .label_details
            .as_ref()
            .and_then(|details| details.detail.as_deref()),
        Some(" · SASE Sites Hub and Pages")
    );
}

#[tokio::test]
async fn completes_grouped_at_references_from_the_client_root() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(workspace.join("src")).unwrap();
    fs::create_dir_all(workspace.join("plans")).unwrap();
    fs::write(workspace.join("src/main.rs"), "fn main() {}").unwrap();
    fs::write(workspace.join("Justfile"), "check:").unwrap();
    fs::write(workspace.join(".hidden"), "secret").unwrap();

    let project_root = temp.path().join("sase");
    fs::create_dir_all(project_root.join("plans")).unwrap();
    fs::write(project_root.join("plans/roadmap.md"), "roadmap").unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.root_dir = Some(workspace);
        config.project = Some("sase".to_string());
        config.artifact_ref_catalog = Some(artifact_path);
    }

    let bare_items = completion_items(
        server
            .completion_for_text("@".to_string(), Position::new(0, 1))
            .await
            .unwrap(),
    );
    let bare_labels = bare_items
        .iter()
        .map(|item| item.label.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        bare_labels,
        vec![
            "@commit:",
            "@chat:",
            "@bug:",
            "@file:",
            "@bead:",
            "@agent:",
            "@designs:",
            "@plan:",
        ]
    );
    assert!(!bare_labels.contains(&"@.hidden"));
    assert!(bare_items.iter().all(|item| {
        item.kind == Some(lsp_types::CompletionItemKind::ENUM_MEMBER)
            && item
                .sort_text
                .as_deref()
                .is_some_and(|sort| sort.starts_with("0:"))
    }));
    for item in &bare_items {
        assert_eq!(item.filter_text.as_deref(), Some("@"));
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected @ reference text edit");
        };
        assert_eq!(
            edit.range,
            Range::new(Position::new(0, 0), Position::new(0, 1))
        );
    }

    let narrowed_items = completion_items(
        server
            .completion_for_text_with_trigger(
                "@p".to_string(),
                Position::new(0, 2),
                Some(CompletionTriggerKind::TRIGGER_CHARACTER),
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        narrowed_items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@plan:"]
    );

    let invoked_items = completion_items(
        server
            .completion_for_text_with_trigger(
                "@p".to_string(),
                Position::new(0, 2),
                Some(CompletionTriggerKind::INVOKED),
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        invoked_items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        vec!["@plan:", "@plans/"]
    );
    assert_eq!(
        invoked_items[1].kind,
        Some(lsp_types::CompletionItemKind::FOLDER)
    );

    for trigger in [
        CompletionTriggerKind::TRIGGER_CHARACTER,
        CompletionTriggerKind::INVOKED,
    ] {
        let path_items = completion_items(
            server
                .completion_for_text_with_trigger(
                    "@src/".to_string(),
                    Position::new(0, 5),
                    Some(trigger),
                )
                .await
                .unwrap(),
        );
        assert_eq!(path_items.len(), 1);
        assert_eq!(path_items[0].label, "@src/main.rs");
        assert_eq!(
            path_items[0].kind,
            Some(lsp_types::CompletionItemKind::FILE)
        );
        let Some(CompletionTextEdit::Edit(path_edit)) =
            path_items[0].text_edit.as_ref()
        else {
            panic!("expected local path text edit");
        };
        assert_eq!(
            path_edit.range,
            Range::new(Position::new(0, 0), Position::new(0, 5))
        );
    }

    let payload_items = completion_items(
        server
            .completion_for_text("@plan:".to_string(), Position::new(0, 6))
            .await
            .unwrap(),
    );
    assert_eq!(payload_items.len(), 1);
    assert_eq!(payload_items[0].label, "@plan:roadmap.md");
}

#[tokio::test]
async fn appends_known_kind_artifact_diagnostics_from_active_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    let vcs_path = temp.path().join("vcs_project_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("local"));
    write_vcs_ref_catalog(&vcs_path);
    fs::create_dir_all(temp.path().join("sase/designs")).unwrap();
    fs::write(temp.path().join("sase/designs/exists.md"), "exists").unwrap();
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.artifact_ref_catalog = Some(artifact_path);
        config.vcs_project_catalog = Some(vcs_path);
    }

    let diagnostics = server
        .diagnostics_for_text(
            "#gh:sase @designs:exists.md @designs:missing.md \
             @designs:bad.md#page=0 @user:handle \
             `@designs:literal.md` @commit:missing@0123456 @bug:missing#1"
                .to_string(),
        )
        .await;

    assert_eq!(
        diagnostics
            .iter()
            .filter(|diagnostic| matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "unresolved_artifact_ref"
            ))
            .count(),
        1,
        "{diagnostics:?}"
    );
    assert_eq!(
        diagnostics
            .iter()
            .filter(|diagnostic| matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "malformed_artifact_ref"
            ))
            .count(),
        1,
        "{diagnostics:?}"
    );
}

#[tokio::test]
async fn encodes_known_artifact_refs_and_skips_unknown_and_literal_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.artifact_ref_catalog = Some(artifact_path);
        config.vcs_project_catalog = None;
    }

    let tokens = server.semantic_tokens_for_text(
        "é @designs:guide.md#L2-L4 @commit:sase@0123456 \
         @user:handle\n```\n@designs:fenced.md\n```"
            .to_string(),
    );

    assert_eq!(
        tokens.data,
        vec![
            lsp_types::SemanticToken {
                delta_line: 0,
                delta_start: 3,
                length: 7,
                token_type: 0,
                token_modifiers_bitset: 1,
            },
            lsp_types::SemanticToken {
                delta_line: 0,
                delta_start: 8,
                length: 8,
                token_type: 1,
                token_modifiers_bitset: 1,
            },
            lsp_types::SemanticToken {
                delta_line: 0,
                delta_start: 8,
                length: 6,
                token_type: 2,
                token_modifiers_bitset: 1,
            },
            lsp_types::SemanticToken {
                delta_line: 0,
                delta_start: 8,
                length: 6,
                token_type: 0,
                token_modifiers_bitset: 0,
            },
            lsp_types::SemanticToken {
                delta_line: 0,
                delta_start: 7,
                length: 12,
                token_type: 1,
                token_modifiers_bitset: 0,
            },
        ]
    );
}

#[tokio::test]
async fn semantic_tokens_mark_directive_owned_code_bodies() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();

    let tokens = server.semantic_tokens_for_text(
        "%if::\n\n```bash\necho ok\n```\n".to_string(),
    );
    let absolute = absolute_semantic_tokens(&tokens.data);

    assert!(absolute.contains(&(3, 0, 7, 1, 0)), "{absolute:?}");
}

#[tokio::test]
async fn encodes_glossary_tokens_by_active_project_without_overlaps() {
    let temp = tempfile::tempdir().unwrap();
    let glossary_path = temp.path().join("glossary_catalog.json");
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    let vcs_path = temp.path().join("vcs_project_catalog.json");
    write_glossary_catalog(&glossary_path, temp.path(), Some("sase"));
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
    write_vcs_ref_catalog(&vcs_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.glossary_catalog = Some(glossary_path);
        config.artifact_ref_catalog = Some(artifact_path);
        config.vcs_project_catalog = Some(vcs_path);
    }

    let tokens = server.semantic_tokens_for_text(
        "#gh:sase Agent Clan `clan` @designs:clan".to_string(),
    );
    let absolute = absolute_semantic_tokens(&tokens.data);

    assert!(absolute.contains(&(0, 9, 10, 3, 0)), "{absolute:?}");
    assert!(absolute.iter().any(|token| token.3 == 0));
    assert!(absolute.iter().any(|token| token.3 == 1));
    assert_eq!(
        absolute.iter().filter(|token| token.3 == 3).count(),
        1,
        "inline-code and artifact-overlapping aliases must not tokenize: {absolute:?}"
    );

    let local =
        server.semantic_tokens_for_text("#git:local Workspace".to_string());
    assert!(absolute_semantic_tokens(&local.data).contains(&(0, 11, 9, 3, 0)));
}

#[tokio::test]
async fn glossary_hover_and_definition_use_source_ranges() {
    let temp = tempfile::tempdir().unwrap();
    let glossary_path = temp.path().join("glossary_catalog.json");
    let vcs_path = temp.path().join("vcs_project_catalog.json");
    write_glossary_catalog(&glossary_path, temp.path(), Some("sase"));
    write_vcs_ref_catalog(&vcs_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.glossary_catalog = Some(glossary_path);
        config.vcs_project_catalog = Some(vcs_path);
    }

    let text = "#gh:sase ask clan".to_string();
    let hover = server
        .hover_for_text(text.clone(), Position::new(0, 14))
        .await
        .expect("glossary hover");
    let contents = hover.contents;
    let range = hover.range;
    let lsp_types::HoverContents::Markup(markup) = contents else {
        panic!("expected markdown hover");
    };
    assert!(markup.value.contains("**Agent Clan**"));
    assert!(markup.value.contains("Aliases: `clan`"));
    assert!(markup.value.contains("A named rootless container."));
    assert!(markup.value.contains("project `sase`"));
    assert_eq!(
        range,
        Some(Range::new(Position::new(0, 13), Position::new(0, 17)))
    );

    let definition = server
        .definition_for_text(text, Position::new(0, 14))
        .await
        .expect("glossary definition");
    let GotoDefinitionResponse::Scalar(location) = definition else {
        panic!("expected scalar definition");
    };
    assert_eq!(
        location.uri,
        file_uri(temp.path().join("sase/sase/sase.yml"))
    );
    assert_eq!(
        location.range,
        Range::new(Position::new(4, 16), Position::new(4, 27))
    );
}

#[tokio::test]
async fn malformed_glossary_catalog_degrades_to_no_semantics() {
    let temp = tempfile::tempdir().unwrap();
    let glossary_path = temp.path().join("glossary_catalog.json");
    fs::write(&glossary_path, r#"{"schema_version": 99, "projects": []}"#)
        .unwrap();
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().glossary_catalog = Some(glossary_path);

    let tokens = server.semantic_tokens_for_text("Agent Clan".to_string());
    let hover = server
        .hover_for_text("Agent Clan".to_string(), Position::new(0, 1))
        .await;
    let definition = server
        .definition_for_text("Agent Clan".to_string(), Position::new(0, 1))
        .await;

    assert!(tokens.data.is_empty());
    assert!(hover.is_none());
    assert!(definition.is_none());
}
