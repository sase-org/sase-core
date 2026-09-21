use std::sync::Arc;

use lsp_types::{
    CodeActionOrCommand, CompletionClientCapabilities,
    CompletionItemCapability, GotoDefinitionResponse, Hover, Position, Range,
    TextDocumentClientCapabilities, Uri,
};

use sase_core::{EditorPosition as CorePosition, EditorRange as CoreRange};

use super::super::*;

use super::support::*;

use super::super::actions::zero_range;
use super::super::initialize::snippet_support;

#[tokio::test]
async fn exposes_hover_diagnostics_code_actions_and_definition() {
    let source_path = std::env::current_dir().unwrap().join("Cargo.toml");

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(Some(
                source_path.to_string_lossy().into_owned(),
            ))),
        )
    });
    let server = service.inner();

    let hover = server
        .hover_for_text(
            "#foo".to_string(),
            Position {
                line: 0,
                character: 2,
            },
        )
        .await
        .unwrap();
    let Hover {
        contents: lsp_types::HoverContents::Markup(markup),
        ..
    } = hover
    else {
        panic!("expected markdown hover");
    };
    assert!(markup.value.contains("Foo prompt"));

    let frontmatter_hover = server
        .hover_for_text(
            "---\nxprompts:\n  _helper:\n    content: Helper\n---\nBody\n"
                .to_string(),
            Position {
                line: 1,
                character: 2,
            },
        )
        .await
        .unwrap();
    let Hover {
        contents: lsp_types::HoverContents::Markup(frontmatter_markup),
        range: Some(frontmatter_range),
    } = frontmatter_hover
    else {
        panic!("expected markdown frontmatter hover with range");
    };
    assert_eq!(
        frontmatter_range,
        Range {
            start: Position {
                line: 1,
                character: 0,
            },
            end: Position {
                line: 1,
                character: 8,
            },
        }
    );
    assert!(frontmatter_markup.value.contains("local xprompts"));
    assert!(frontmatter_markup.value.contains("current file"));

    let diagnostics = server
        .diagnostics_for_text("#missing %wat".to_string())
        .await;
    assert!(diagnostics
        .iter()
        .any(|diagnostic| diagnostic.message.contains("Unknown xprompt")));
    assert!(diagnostics
        .iter()
        .any(|diagnostic| diagnostic.message.contains("Unknown directive")));

    let missing_arg_diagnostics =
        server.diagnostics_for_text("#foo".to_string()).await;
    assert!(missing_arg_diagnostics.iter().any(|diagnostic| {
        diagnostic.source.as_deref() == Some("sase-xprompt")
            && diagnostic.severity == Some(lsp_types::DiagnosticSeverity::ERROR)
            && matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "missing_required_arg"
            )
    }));

    let invalid_type_diagnostics = server
        .diagnostics_for_text("#foo(path=\"bad value\")".to_string())
        .await;
    assert!(invalid_type_diagnostics.iter().any(|diagnostic| {
        diagnostic.source.as_deref() == Some("sase-xprompt")
            && diagnostic.severity == Some(lsp_types::DiagnosticSeverity::ERROR)
            && matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "invalid_xprompt_arg_type"
            )
    }));

    let uri = Uri::from_file_path(&source_path).unwrap();
    let actions = server
        .code_actions_for_text(
            uri.clone(),
            "#!foo".to_string(),
            Range {
                start: Position {
                    line: 0,
                    character: 1,
                },
                end: Position {
                    line: 0,
                    character: 1,
                },
            },
        )
        .await;
    assert!(actions.iter().any(|action| match action {
        CodeActionOrCommand::CodeAction(action) =>
            action.title.contains("canonical"),
        CodeActionOrCommand::Command(command) =>
            command.command == REFRESH_COMMAND,
    }));
    assert!(actions.iter().any(|action| match action {
        CodeActionOrCommand::CodeAction(action) =>
            action.title == "Insert required named args",
        CodeActionOrCommand::Command(_) => false,
    }));

    let definition = server
        .definition_for_text(
            "#foo".to_string(),
            Position {
                line: 0,
                character: 2,
            },
        )
        .await
        .unwrap();
    let GotoDefinitionResponse::Scalar(location) = definition else {
        panic!("expected scalar definition");
    };
    assert_eq!(location.uri, uri);
}

#[tokio::test]
async fn typed_launch_diagnostics_and_code_actions_use_cached_flag() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let disabled = server
        .diagnostics_for_text("%if::\n\n```bash\ntrue\n```".to_string())
        .await;
    assert!(diagnostics_contain_code(
        &disabled,
        "typed_launch_units_disabled"
    ));

    server.config.write().unwrap().typed_launch_units = true;
    let missing_fence = server
        .diagnostics_for_text("%if::\n\nReview".to_string())
        .await;
    assert!(diagnostics_contain_code(
        &missing_fence,
        "typed_launch_missing_fence"
    ));

    let valid = server
        .diagnostics_for_text("%if::\n\n```bash\ntrue\n```".to_string())
        .await;
    assert!(!diagnostics_contain_code(
        &valid,
        "typed_launch_missing_fence"
    ));
    assert!(!diagnostics_contain_code(
        &valid,
        "typed_launch_units_disabled"
    ));

    let uri =
        Uri::from_file_path(std::env::current_dir().unwrap().join("prompt.md"))
            .unwrap();
    let actions = server
        .code_actions_for_text(
            uri,
            "%if::".to_string(),
            Range::new(Position::new(0, 0), Position::new(0, 0)),
        )
        .await;
    assert!(actions.iter().any(|action| matches!(
        action,
        CodeActionOrCommand::CodeAction(action)
            if action.title == "Complete %if bash fence"
    )));
}

#[tokio::test]
async fn diagnostics_for_uri_text_honors_canonical_memory_file_uri() {
    let temp = tempfile::tempdir().unwrap();
    let memory_dir = temp.path().join("sase/memory");
    fs::create_dir_all(&memory_dir).unwrap();
    let memory_uri =
        Uri::from_file_path(memory_dir.join("generated_skills.md")).unwrap();
    let normal_uri =
        Uri::from_file_path(temp.path().join("sase/xprompts").join("foo.md"))
            .unwrap();
    let text = "---\nkeywords: [topic]\n---\nBody".to_string();

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let memory_diagnostics = server
        .diagnostics_for_uri_text(&memory_uri, text.clone())
        .await;
    assert!(
        !diagnostics_contain_code(
            &memory_diagnostics,
            "missing_xprompt_memory_tag"
        ),
        "{memory_diagnostics:?}"
    );

    let normal_diagnostics =
        server.diagnostics_for_uri_text(&normal_uri, text).await;
    assert!(
        !diagnostics_contain_code(
            &normal_diagnostics,
            "missing_xprompt_memory_tag"
        ),
        "{normal_diagnostics:?}"
    );
}

#[tokio::test]
async fn diagnostics_for_uri_text_accepts_markdown_local_xprompts() {
    let temp = tempfile::tempdir().unwrap();
    let uri =
        Uri::from_file_path(temp.path().join("sase/xprompts").join("reads.md"))
            .unwrap();
    let text = "---\nxprompts:\n  _article_search_agent:\n    input:\n      topic: word\n    content: Search {{ topic }}\n---\n#_article_search_agent(news)\n"
        .to_string();

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();

    let diagnostics = server.diagnostics_for_uri_text(&uri, text).await;
    assert!(
        diagnostics.iter().all(|diagnostic| {
            !matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code))
                    if code == "unknown_xprompt"
            ) || !diagnostic.message.contains("_article_search_agent")
        }),
        "{diagnostics:?}"
    );
}

#[tokio::test]
async fn definition_uses_definition_path_outside_workspace_root() {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("outside-workspace.md");
    fs::write(&source_path, "source").unwrap();

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(Some(
                source_path.to_string_lossy().into_owned(),
            ))),
        )
    });
    let server = service.inner();

    let definition = server
        .definition_for_text(
            "#foo".to_string(),
            Position {
                line: 0,
                character: 2,
            },
        )
        .await
        .unwrap();

    let GotoDefinitionResponse::Scalar(location) = definition else {
        panic!("expected scalar definition");
    };
    assert_eq!(location.uri, Uri::from_file_path(source_path).unwrap());
    assert_eq!(location.range, zero_range());
}

#[tokio::test]
async fn definition_preserves_catalog_definition_range() {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("sase/sase.yml");
    fs::create_dir_all(source_path.parent().unwrap()).unwrap();
    fs::write(&source_path, "xprompts:\n  foo:\n    content: body\n").unwrap();
    let mut entry = catalog_entry(
        "foo",
        "#foo",
        None,
        Vec::new(),
        Some(source_path.to_string_lossy().into_owned()),
    );
    entry.definition_range = Some(CoreRange {
        start: CorePosition {
            line: 1,
            character: 2,
        },
        end: CorePosition {
            line: 1,
            character: 5,
        },
    });

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(vec![entry])),
        )
    });
    let server = service.inner();

    let definition = server
        .definition_for_text(
            "#foo".to_string(),
            Position {
                line: 0,
                character: 2,
            },
        )
        .await
        .unwrap();

    let GotoDefinitionResponse::Scalar(location) = definition else {
        panic!("expected scalar definition");
    };
    assert_eq!(
        location.range,
        Range {
            start: Position {
                line: 1,
                character: 2,
            },
            end: Position {
                line: 1,
                character: 5,
            },
        }
    );
}

#[tokio::test]
async fn definition_returns_none_for_pseudo_or_missing_sources() {
    for definition_path in [None, Some("plugin:module/name".to_string())] {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(definition_path.clone())),
            )
        });
        let server = service.inner();

        assert_eq!(
            server
                .definition_for_text(
                    "#foo".to_string(),
                    Position {
                        line: 0,
                        character: 2,
                    },
                )
                .await,
            None
        );
    }
}

#[test]
fn detects_snippet_support_from_client_capabilities() {
    let capabilities = ClientCapabilities {
        text_document: Some(TextDocumentClientCapabilities {
            completion: Some(CompletionClientCapabilities {
                completion_item: Some(CompletionItemCapability {
                    snippet_support: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    assert!(snippet_support(&capabilities));
}
