use std::sync::Arc;

use lsp_types::{
    CodeActionKind, CodeActionOrCommand, CompletionItemKind,
    CompletionResponse, CompletionTextEdit, DiagnosticSeverity,
    DocumentChanges, Documentation, InitializeParams, OneOf, Position, Range,
};

use super::super::catalogs::{leading_vcs_project, load_vcs_project_catalog};
use super::super::*;
use super::support::*;

#[tokio::test]
async fn completes_v5_project_rows_as_tags_in_catalog_order() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text(
            "Fix +".to_string(),
            Position {
                line: 0,
                character: 5,
            },
        )
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
        vec!["+sase", "+notes", "ship"]
    );

    let sase = &items[0];
    assert_eq!(sase.kind, Some(CompletionItemKind::MODULE));
    assert_eq!(sase.filter_text.as_deref(), Some("+sase"));
    assert_eq!(sase.detail.as_deref(), Some("GitHub · #gh:sase"));
    assert_eq!(sase.sort_text.as_deref(), Some("0000"));
    let Some(Documentation::MarkupContent(documentation)) =
        sase.documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    assert!(
        documentation.value.contains("SASE repo"),
        "{documentation:?}"
    );
    assert!(
        documentation.value.contains("key `gh_sase-org__sase`"),
        "{documentation:?}"
    );
    assert!(
        documentation.value.contains("aliases `sase-core`"),
        "{documentation:?}"
    );
    assert!(documentation.value.contains("current"), "{documentation:?}");
    let label_details = sase.label_details.as_ref().unwrap();
    assert_eq!(label_details.description.as_deref(), Some("project"));

    let notes = &items[1];
    assert_eq!(notes.label, "+notes");
    assert_eq!(notes.filter_text.as_deref(), Some("+notes"));
    assert_eq!(notes.detail.as_deref(), Some("Bare Git · #git:notes"));
    assert_eq!(notes.sort_text.as_deref(), Some("0001"));

    // Patch rows keep their `#` spelling and in-place edits.
    let patch = &items[2];
    assert_eq!(patch.label, "ship");
    assert_eq!(patch.kind, Some(CompletionItemKind::EVENT));
    assert_eq!(patch.detail.as_deref(), Some("#gh:ship "));
    assert_eq!(patch.sort_text.as_deref(), Some("0002"));
    let Some(CompletionTextEdit::Edit(edit)) = patch.text_edit.as_ref() else {
        panic!("expected patch text edit");
    };
    assert_eq!(edit.new_text, "#gh:ship ");
}

#[test]
fn semantic_tokens_emit_sigil_and_name_with_accent() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let catalog = load_vcs_project_catalog(Some(&catalog_path));
    let document = DocumentSnapshot::new("+sase fix +notes");
    let tokens = document_semantic_tokens(
        &document,
        None,
        None,
        None,
        &catalog.project_tags,
        &catalog.entries,
    );
    let absolute = absolute_semantic_tokens(&tokens.data);
    // accent2 -> bit 7 (128); accent5 -> bit 10 (1024); sigil -> bit 2 (4).
    assert!(absolute.contains(&(0, 0, 1, 9, 4 | 128)), "{absolute:?}");
    assert!(absolute.contains(&(0, 1, 4, 9, 128)), "{absolute:?}");
    assert!(absolute.contains(&(0, 10, 1, 9, 4 | 1024)), "{absolute:?}");
    assert!(absolute.contains(&(0, 11, 5, 9, 1024)), "{absolute:?}");
    assert_eq!(absolute.len(), 4, "{absolute:?}");
}

#[test]
fn semantic_tokens_mark_unknown_and_providerless_tags() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let catalog = load_vcs_project_catalog(Some(&catalog_path));
    let document = DocumentSnapshot::new("+ssae +orphan");
    let tokens = document_semantic_tokens(
        &document,
        None,
        None,
        None,
        &catalog.project_tags,
        &catalog.entries,
    );
    let absolute = absolute_semantic_tokens(&tokens.data);
    // unknown -> bit 3 (8); disabled -> bit 4 (16).
    assert!(absolute.contains(&(0, 0, 1, 9, 4 | 8)), "{absolute:?}");
    assert!(absolute.contains(&(0, 1, 4, 9, 8)), "{absolute:?}");
    assert!(absolute.contains(&(0, 6, 1, 9, 4 | 16)), "{absolute:?}");
    assert!(absolute.contains(&(0, 7, 6, 9, 16)), "{absolute:?}");
    assert_eq!(absolute.len(), 4, "{absolute:?}");
}

#[test]
fn semantic_tokens_v4_catalog_has_no_tag_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
    let catalog = load_vcs_project_catalog(Some(&catalog_path));
    assert!(catalog.project_tags.is_empty());
    let document = DocumentSnapshot::new("+sase fix");
    let tokens = document_semantic_tokens(
        &document,
        None,
        None,
        None,
        &catalog.project_tags,
        &catalog.entries,
    );
    assert!(
        absolute_semantic_tokens(&tokens.data)
            .iter()
            .all(|token| token.3 != 9),
        "{:?}",
        tokens.data
    );
}

#[tokio::test]
async fn hover_shows_resolved_tag_details() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let hover = server
        .hover_for_text("+sase fix".to_string(), Position::new(0, 2))
        .await
        .expect("tag hover");
    let lsp_types::HoverContents::Markup(markup) = hover.contents else {
        panic!("expected markdown hover");
    };
    assert!(markup.value.contains("**+sase**"), "{}", markup.value);
    assert!(markup.value.contains("GitHub"), "{}", markup.value);
    assert!(markup.value.contains("#gh:sase"), "{}", markup.value);
    assert!(
        markup.value.contains("key `gh_sase-org__sase`"),
        "{}",
        markup.value
    );
    assert!(
        markup.value.contains("aliases `sase-core`"),
        "{}",
        markup.value
    );
    assert!(markup.value.contains("current"), "{}", markup.value);
    assert!(markup.value.contains("SASE repo"), "{}", markup.value);
    assert_eq!(
        hover.range,
        Some(Range::new(Position::new(0, 0), Position::new(0, 5)))
    );

    assert!(
        server
            .hover_for_text("+ssae fix".to_string(), Position::new(0, 2))
            .await
            .is_none(),
        "unknown tags have no hover"
    );
}

#[tokio::test]
async fn diagnostics_follow_anchored_tag_policy() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let diagnostics = server.diagnostics_for_text("+ssae do".to_string()).await;
    let unknown = diagnostics
        .iter()
        .find(|diagnostic| {
            matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "unknown_project_tag"
            )
        })
        .expect("anchored unknown tag warns");
    assert_eq!(unknown.severity, Some(DiagnosticSeverity::WARNING));
    assert!(
        unknown.message.contains("`+ssae` (line 1)"),
        "{}",
        unknown.message
    );
    assert!(
        unknown.message.contains("Did you mean `+sase`?"),
        "{}",
        unknown.message
    );
    assert!(
        unknown.message.contains("Known: +notes +orphan +sase"),
        "{}",
        unknown.message
    );

    // Non-anchored unknown tags are plain text: no diagnostic.
    let diagnostics = server.diagnostics_for_text("do +ssae".to_string()).await;
    assert!(
        !diagnostics_contain_code(&diagnostics, "unknown_project_tag"),
        "{diagnostics:?}"
    );

    // Healthy and provider-less tags.
    let diagnostics = server.diagnostics_for_text("+sase do".to_string()).await;
    assert!(
        !diagnostics_contain_code(&diagnostics, "unknown_project_tag")
            && !diagnostics_contain_code(&diagnostics, "ambiguous_project_tag")
            && !diagnostics_contain_code(
                &diagnostics,
                "providerless_project_tag"
            ),
        "{diagnostics:?}"
    );
    let diagnostics =
        server.diagnostics_for_text("+orphan do".to_string()).await;
    let providerless = diagnostics
        .iter()
        .find(|diagnostic| {
            matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "providerless_project_tag"
            )
        })
        .expect("provider-less tag warns");
    assert_eq!(providerless.severity, Some(DiagnosticSeverity::WARNING));
}

#[tokio::test]
async fn diagnostics_flag_ambiguous_tags_as_errors() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_ambiguous_project_tag_catalog(&catalog_path);
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let diagnostics = server.diagnostics_for_text("+sase do".to_string()).await;
    let ambiguous = diagnostics
        .iter()
        .find(|diagnostic| {
            matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "ambiguous_project_tag"
            )
        })
        .expect("ambiguous tag errors");
    assert_eq!(ambiguous.severity, Some(DiagnosticSeverity::ERROR));
    assert!(
        ambiguous.message.contains("sase doctor"),
        "{}",
        ambiguous.message
    );
}

fn quickfix_actions(
    actions: &lsp_types::CodeActionResponse,
) -> Vec<&lsp_types::CodeAction> {
    actions
        .iter()
        .filter_map(|action| match action {
            CodeActionOrCommand::CodeAction(action)
                if action.kind == Some(CodeActionKind::QUICKFIX) =>
            {
                Some(action)
            }
            _ => None,
        })
        .collect()
}

fn rewrite_actions(
    actions: &lsp_types::CodeActionResponse,
) -> Vec<&lsp_types::CodeAction> {
    actions
        .iter()
        .filter_map(|action| match action {
            CodeActionOrCommand::CodeAction(action)
                if action.kind == Some(CodeActionKind::REFACTOR_REWRITE) =>
            {
                Some(action)
            }
            _ => None,
        })
        .collect()
}

fn first_edit_text(action: &lsp_types::CodeAction) -> (Range, String) {
    let edit = action.edit.as_ref().expect("code action edit");
    let Some(DocumentChanges::Edits(edits)) = edit.document_changes.as_ref()
    else {
        panic!("expected document-changing edit");
    };
    let OneOf::Left(text_edit) = &edits[0].edits[0] else {
        panic!("expected plain text edit");
    };
    (text_edit.range, text_edit.new_text.clone())
}

#[tokio::test]
async fn code_actions_offer_tag_quickfixes() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let uri = file_uri(temp.path().join("prompt.md"));
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let actions = server
        .code_actions_for_text(
            uri.clone(),
            "+ssae do".to_string(),
            Range::new(Position::new(0, 0), Position::new(0, 8)),
        )
        .await;
    let fixes = quickfix_actions(&actions);
    assert!(!fixes.is_empty(), "{actions:?}");
    let fix = fixes
        .iter()
        .find(|fix| fix.title == "Use +sase")
        .expect("suggestion quickfix");
    let (range, new_text) = first_edit_text(fix);
    assert_eq!(new_text, "+sase");
    assert_eq!(range, Range::new(Position::new(0, 0), Position::new(0, 5)));

    let actions = server
        .code_actions_for_text(
            uri,
            "+sase do".to_string(),
            Range::new(Position::new(0, 0), Position::new(0, 8)),
        )
        .await;
    assert!(quickfix_actions(&actions).is_empty(), "{actions:?}");
}

#[tokio::test]
async fn code_actions_rewrite_colon_refs_to_tags() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let uri = file_uri(temp.path().join("prompt.md"));
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let actions = server
        .code_actions_for_text(
            uri.clone(),
            "#gh:sase do this".to_string(),
            Range::new(Position::new(0, 0), Position::new(0, 14)),
        )
        .await;
    let rewrites = rewrite_actions(&actions);
    assert_eq!(rewrites.len(), 1, "{actions:?}");
    assert_eq!(rewrites[0].title, "Use project tag +sase");
    let (range, new_text) = first_edit_text(rewrites[0]);
    assert_eq!(new_text, "+sase");
    assert_eq!(range, Range::new(Position::new(0, 0), Position::new(0, 8)));

    // Patches, owner/repo paths, paren forms, and provider mismatches
    // never rewrite.
    for text in [
        "#gh:ship do this",
        "#gh:bbugyi200/sase do this",
        "#gh(sase) do this",
        "#git:sase do this",
    ] {
        let actions = server
            .code_actions_for_text(
                uri.clone(),
                text.to_string(),
                Range::new(Position::new(0, 0), Position::new(0, 14)),
            )
            .await;
        assert!(
            rewrite_actions(&actions).is_empty(),
            "{text:?}: {actions:?}"
        );
    }
}

#[test]
fn leading_project_recognizes_tags() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(
        leading_vcs_project(
            "+sase do this",
            &catalog.entries,
            &catalog.project_tags
        ),
        Some("sase".to_string())
    );
    // Alias and casefold spellings resolve too.
    assert_eq!(
        leading_vcs_project(
            "+SASE-CORE do this",
            &catalog.entries,
            &catalog.project_tags
        ),
        Some("sase".to_string())
    );
    // The `#` spelling keeps working.
    assert_eq!(
        leading_vcs_project(
            "#gh:sase do this",
            &catalog.entries,
            &catalog.project_tags
        ),
        Some("sase".to_string())
    );
    // Unknown tags and non-tag first words contribute no project.
    assert_eq!(
        leading_vcs_project(
            "+ssae do this",
            &catalog.entries,
            &catalog.project_tags
        ),
        None
    );
    assert_eq!(
        leading_vcs_project(
            "+sase, do this",
            &catalog.entries,
            &catalog.project_tags
        ),
        None
    );
    assert_eq!(
        leading_vcs_project(
            "do +sase",
            &catalog.entries,
            &catalog.project_tags
        ),
        None
    );
}

#[tokio::test]
async fn leading_tag_drives_glossary_context() {
    let temp = tempfile::tempdir().unwrap();
    let glossary_path = temp.path().join("glossary_catalog.json");
    let vcs_path = temp.path().join("vcs_project_catalog.json");
    write_glossary_catalog(&glossary_path, temp.path(), None);
    write_v5_project_tag_catalog(&vcs_path);
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

    // No default project: only the leading tag selects the glossary.
    let hover = server
        .hover_for_text("+sase ask clan".to_string(), Position::new(0, 12))
        .await
        .expect("glossary hover via leading tag");
    let lsp_types::HoverContents::Markup(markup) = hover.contents else {
        panic!("expected markdown hover");
    };
    assert!(markup.value.contains("**Agent Clan**"));

    assert!(
        server
            .hover_for_text("ask clan".to_string(), Position::new(0, 6))
            .await
            .is_none(),
        "no leading tag means no glossary project"
    );
}

#[tokio::test]
async fn initialize_advertises_project_tag_palette() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_v5_project_tag_catalog(&catalog_path);
    struct EnvGuard;
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("SASE_XPROMPT_VCS_PROJECT_CATALOG");
        }
    }
    // Mirrors the integration tests' set_var precedent; the guard
    // restores the environment even when an assertion fails.
    std::env::set_var("SASE_XPROMPT_VCS_PROJECT_CATALOG", &catalog_path);
    let _guard = EnvGuard;
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();

    let result = server
        .initialize(InitializeParams::default())
        .await
        .unwrap();
    let experimental = result.capabilities.experimental.expect("experimental");
    assert_eq!(
        experimental
            .pointer("/sase/projectTagPalette")
            .expect("projectTagPalette"),
        &serde_json::json!([
            "#111111", "#222222", "#333333", "#444444", "#555555", "#666666"
        ])
    );
}
