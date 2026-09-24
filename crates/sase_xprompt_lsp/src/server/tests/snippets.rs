use std::sync::Arc;

use lsp_types::{
    CompletionItemKind, CompletionResponse, CompletionTextEdit, Hover,
    InsertTextFormat, Position,
};

use sase_core::{EditorPosition as CorePosition, EditorRange as CoreRange};

use super::super::*;

use super::support::*;

use super::super::completion_items::directive_snippet_items;
use super::super::state::ServerConfig;

#[tokio::test]
async fn completes_placeholders_from_the_current_document() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    let response = server
        .completion_for_text(
            "<Beta> <bravo> choose <b>".to_string(),
            Position::new(0, 24),
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    let labels: Vec<&str> =
        items.iter().map(|item| item.label.as_str()).collect();
    assert_eq!(labels, vec!["Beta", "bravo"]);
    assert_eq!(items[0].kind, Some(CompletionItemKind::VARIABLE));
    assert_eq!(items[0].filter_text.as_deref(), Some("b"));
    assert_eq!(items[0].sort_text.as_deref(), Some("0000"));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected placeholder text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 23));
    assert_eq!(edit.range.end, Position::new(0, 25));
    assert_eq!(edit.new_text, "Beta>");
}

#[tokio::test]
async fn placeholder_completion_appends_a_missing_closing_bracket() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    let response = server
        .completion_for_text("<alpha> use <a".to_string(), Position::new(0, 14))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected placeholder text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 13));
    assert_eq!(edit.range.end, Position::new(0, 14));
    assert_eq!(edit.new_text, "alpha>");
}

#[tokio::test]
async fn placeholder_completion_is_empty_without_another_span() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    let response = server
        .completion_for_text("<only>".to_string(), Position::new(0, 5))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert!(items.is_empty());
}

#[tokio::test]
async fn xprompt_snippet_completions_use_single_row_skeletons() {
    let entries = vec![
        catalog_entry(
            "many",
            "#many",
            Some("(path: path, mode: word)".to_string()),
            vec![
                input_hint("path", "path", true, 0),
                input_hint("mode", "word", true, 1),
            ],
            None,
        ),
        catalog_entry("none", "#none", None, Vec::new(), None),
        catalog_entry(
            "optional",
            "#optional",
            Some("(path?: path)".to_string()),
            vec![input_hint("path", "path", false, 0)],
            None,
        ),
        catalog_entry(
            "path",
            "#path",
            Some("(path: path)".to_string()),
            vec![input_hint("path", "path", true, 0)],
            None,
        ),
        catalog_entry(
            "text",
            "#text",
            Some("(body: text)".to_string()),
            vec![input_hint("body", "text", true, 0)],
            None,
        ),
    ];
    let items = snippet_completion_items(entries, "#", 1).await;

    assert_eq!(items.len(), 5);
    assert_snippet_item(&items, "#many", "#many($0)");
    assert_snippet_item(&items, "#none", "#none ");
    assert_snippet_item(&items, "#optional", "#optional ");
    assert_snippet_item(&items, "#path", "#path:");
    // End-of-line required-text completion appends the free-form delimiter
    // space (`#text:: `).
    assert_snippet_item(&items, "#text", "#text:: ");
}

#[tokio::test]
async fn required_text_skeleton_keeps_double_colon_before_existing_text() {
    let entries = vec![catalog_entry(
        "text",
        "#text",
        Some("(body: text)".to_string()),
        vec![input_hint("body", "text", true, 0)],
        None,
    )];
    // `#text` token followed by more text on the line: the completion ends
    // mid-line, so the skeleton stays `#text::` and the following ` x`
    // supplies the single delimiter rather than doubling the space.
    let items = snippet_completion_items(entries, "#text x", 5).await;

    assert_eq!(items.len(), 1);
    assert_snippet_item(&items, "#text", "#text::");
}

#[tokio::test]
async fn xprompt_snippet_completion_returns_one_row_per_match() {
    let entries = vec![catalog_entry(
        "foo",
        "#foo",
        Some("(path: path)".to_string()),
        vec![input_hint("path", "path", true, 0)],
        None,
    )];
    let items = snippet_completion_items(entries, "#fo", 3).await;

    assert_eq!(items.len(), 1);
    assert_snippet_item(&items, "#foo", "#foo:");
}

#[tokio::test]
async fn bare_trigger_snippet_completion_uses_snippet_items() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_and_snippets(
                Vec::new(),
                vec![
                    snippet_entry(
                        "foo",
                        r"literal $ $1 \ brace } $0",
                        "ace.snippets",
                    ),
                    snippet_entry("bar", "bar", "ace.snippets"),
                ],
            )),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        *config = ServerConfig {
            snippet_support: true,
            ..ServerConfig::default()
        };
    }

    let response = server
        .completion_for_text(
            "fo".to_string(),
            Position {
                line: 0,
                character: 2,
            },
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    assert_snippet_item(&items, "foo", r"literal \$ $1 \\ brace \} $0");
    assert_eq!(items[0].detail.as_deref(), Some("ace.snippets"));
}

#[tokio::test]
async fn placeholder_tabstop_snippet_item_retriggers_suggestions() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_and_snippets(
                Vec::new(),
                vec![snippet_entry("cbi", "`<$1>`$0", "ace.snippets")],
            )),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        *config = ServerConfig {
            snippet_support: true,
            ..ServerConfig::default()
        };
    }

    let response = server
        .completion_for_text("cb".to_string(), Position::new(0, 2))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_eq!(
        items[0]
            .command
            .as_ref()
            .map(|command| command.command.as_str()),
        Some("editor.action.triggerSuggest")
    );
}

#[tokio::test]
async fn bare_trigger_snippets_require_client_snippet_support() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_and_snippets(
                Vec::new(),
                vec![snippet_entry("foo", "$1$0", "ace.snippets")],
            )),
        )
    });
    let server = service.inner();

    let response = server
        .completion_for_text(
            "fo".to_string(),
            Position {
                line: 0,
                character: 2,
            },
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert!(items.is_empty());
}

#[tokio::test]
async fn snippet_clients_receive_identity_and_clan_forms() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        *config = ServerConfig {
            snippet_support: true,
            ..ServerConfig::default()
        };
    }

    for token in ["%clan", "%c"] {
        let response = server
            .completion_for_text(
                token.to_string(),
                Position::new(0, token.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_snippet_item(&items, "%clan:...", "%clan:${1:name}$0");
        assert_snippet_item(
            &items,
            "%clan(..., tribe=...)",
            "%clan(${1:name}, tribe=${2:tribe})$0",
        );
        for item in items
            .iter()
            .filter(|item| item.kind == Some(CompletionItemKind::SNIPPET))
        {
            let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
            else {
                panic!("expected clan snippet text edit");
            };
            assert_eq!(edit.range.start, Position::new(0, 0));
            assert_eq!(edit.range.end, Position::new(0, token.len() as u32));
        }
    }

    let response = server
        .completion_for_text("%t".to_string(), Position::new(0, 2))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert!(
        items.is_empty(),
        "removed %t directive completed: {items:?}"
    );

    for token in ["%f", "%final"] {
        let response = server
            .completion_for_text(
                token.to_string(),
                Position::new(0, token.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_canonical_final_name(&items, token.len() as u32);
        assert_snippet_item(&items, "%final:...", "%final:${1:instance}$0");
        assert_snippet_item(
            &items,
            "%final(...)",
            "%final(${1:instance}, ${2:instance})$0",
        );
        for item in items.iter().filter(|item| {
            item.label == "%final"
                || item.label.starts_with("%final:")
                || item.label.starts_with("%final(")
        }) {
            let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
            else {
                panic!("expected %final clause-local text edit");
            };
            assert_eq!(edit.range.start, Position::new(0, 0));
            assert_eq!(edit.range.end, Position::new(0, token.len() as u32));
        }
    }

    for token in ["%id", "%i"] {
        let response = server
            .completion_for_text(
                token.to_string(),
                Position::new(0, token.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_snippet_item(&items, "%id:...", "%id:${1:agent-id}$0");
        assert_snippet_item(
            &items,
            "%id(..., clan=...)",
            "%id(${1:id}, clan=${2:clan})$0",
        );
        assert_snippet_item(
            &items,
            "%id(..., session=...)",
            "%id(${1:suffix}, session=${2:session})$0",
        );
        assert_snippet_item(
            &items,
            "%id(tribe=...)",
            "%id(tribe=${1:tribe})$0",
        );
        assert!(!items.iter().any(|item| item.label.starts_with("%name")
            || item.label.starts_with("%n:")
            || item.label.starts_with("%tribe")
            || item.label.starts_with("%t:")));
    }

    let wait_items = server
        .completion_for_text("%wait".to_string(), Position::new(0, 5))
        .await
        .unwrap();
    let CompletionResponse::Array(wait_items) = wait_items else {
        panic!("expected completion array");
    };
    assert_snippet_item(
        &wait_items,
        "%wait(..., bead=...)",
        "%wait(${1:agent}, bead=${2:bead-id})$0",
    );
    let model_items = server
        .completion_for_text("%model".to_string(), Position::new(0, 6))
        .await
        .unwrap();
    let CompletionResponse::Array(model_items) = model_items else {
        panic!("expected completion array");
    };
    assert_snippet_item(
        &model_items,
        "%model(..., alias=...)",
        "%model(${1:model}, ${2:alias}=${3:model})$0",
    );
}

#[tokio::test]
async fn typed_launch_directive_recipes_follow_flag_and_snippet_support() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        *config = ServerConfig {
            snippet_support: true,
            typed_launch_units: false,
            ..ServerConfig::default()
        };
    }

    let disabled = server
        .completion_for_text("%if".to_string(), Position::new(0, 3))
        .await
        .unwrap();
    let disabled_items = completion_items(disabled);
    assert_snippet_item(
        &disabled_items,
        "%if(should_run=...)",
        "%if(should_run=${1|true,false|})$0",
    );
    assert!(!disabled_items.iter().any(|item| item.label == "%if:: bash"));

    server.config.write().unwrap().typed_launch_units = true;
    let enabled = server
        .completion_for_text("%if".to_string(), Position::new(0, 3))
        .await
        .unwrap();
    let enabled_items = completion_items(enabled);
    assert_snippet_item(
        &enabled_items,
        "%if:: bash",
        "%if::\n\n```bash\n${1:test -f pyproject.toml}\n```$0",
    );

    {
        let mut config = server.config.write().unwrap();
        config.snippet_support = false;
    }
    let plain = server
        .completion_for_text("%proc".to_string(), Position::new(0, 5))
        .await
        .unwrap();
    let plain_items = completion_items(plain);
    let item = plain_items
        .iter()
        .find(|item| item.label == "%proc:: bash")
        .expect("plain %proc:: bash recipe");
    assert_eq!(item.kind, Some(CompletionItemKind::TEXT));
    assert_eq!(item.insert_text_format, None);
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected plain text edit");
    };
    assert_eq!(
        edit.new_text.as_str(),
        "%proc(timeout=\"20m\")::\n\n```bash\n\n```"
    );
}

#[test]
fn directive_snippet_for_alt_uses_brace_shorthand() {
    let range = CoreRange {
        start: CorePosition {
            line: 0,
            character: 0,
        },
        end: CorePosition {
            line: 0,
            character: 4,
        },
    };
    let wait_items = directive_snippet_items(Some("%wait"), range, &[], true);
    assert!(wait_items
        .iter()
        .any(|item| item.label == "%wait(..., bead=...)"));
    let model_items = directive_snippet_items(Some("%model"), range, &[], true);
    assert!(model_items
        .iter()
        .any(|item| item.label == "%model(..., alias=...)"));
    let items = directive_snippet_items(Some("%alt"), range, &[], true);
    let alt = items
        .iter()
        .find(|item| item.label == "%alt:...")
        .expect("alt directive snippet item");
    assert_eq!(alt.kind, Some(CompletionItemKind::SNIPPET));
    assert_eq!(alt.insert_text_format, Some(InsertTextFormat::SNIPPET));
    let Some(CompletionTextEdit::Edit(edit)) = alt.text_edit.as_ref() else {
        panic!("expected text edit for alt snippet");
    };
    assert_eq!(edit.new_text.as_str(), "%{${1:A} | ${2:B}\\}$0");

    let final_meta = sase_core::editor_directive_contract()
        .into_iter()
        .find(|directive| directive.name == "final")
        .expect("final directive contract");
    assert_eq!(
        final_meta
            .recipes
            .iter()
            .map(|recipe| (recipe.label.clone(), recipe.insert_text.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                "%final:...".to_string(),
                "%final:${1:instance}$0".to_string()
            ),
            (
                "%final(...)".to_string(),
                "%final(${1:instance}, ${2:instance})$0".to_string()
            ),
        ]
    );
    for token in [Some("%final"), Some("%f")] {
        let items = directive_snippet_items(token, range, &[], true);
        assert_snippet_item(&items, "%final:...", "%final:${1:instance}$0");
        assert_snippet_item(
            &items,
            "%final(...)",
            "%final(${1:instance}, ${2:instance})$0",
        );
        for item in &items {
            let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
            else {
                panic!("expected %final snippet text edit");
            };
            assert_eq!(edit.range.start, Position::new(0, 0));
            assert_eq!(edit.range.end, Position::new(0, 4));
        }
    }

    // No directive snippet should still emit the legacy `%(...)` spelling.
    for item in &items {
        if let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() {
            assert!(
                !edit.new_text.contains("%("),
                "directive snippet still advertises %(: {}",
                edit.new_text
            );
        }
    }
}

#[tokio::test]
async fn identity_and_clan_editor_surfaces_use_current_metadata() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    for (text, cursor, heading, description) in [
        (
            "%i(worker, session=review)",
            13,
            "**%id**",
            "Assign an agent ID with optional bead, clan, session, or user-managed tribe",
        ),
        (
            "%c(research, tr)",
            15,
            "**%clan**",
            "Declare a new parallel agent clan",
        ),
    ] {
        let hover = server
            .hover_for_text(text.to_string(), Position::new(0, cursor))
            .await
            .unwrap_or_else(|| panic!("missing hover for {text}"));
        let Hover {
            contents: lsp_types::HoverContents::Markup(markup),
            ..
        } = hover
        else {
            panic!("expected markdown hover");
        };
        assert!(markup.value.contains(heading), "{text}");
        assert!(markup.value.contains(description), "{text}");
    }

    for text in ["%tribe:research", "%t:research"] {
        assert!(
            server
                .hover_for_text(text.to_string(), Position::new(0, 1))
                .await
                .is_none(),
            "removed directive should not hover: {text}"
        );
    }

    let current = server
        .diagnostics_for_text(
            "%id(worker, clan=research) %id(worker, family=review) %id(worker, tribe=review) %id(tribe=review) %i:worker %clan(research.@, tribe=research) %c:research".to_string(),
        )
        .await;
    assert!(!current.iter().any(|diagnostic| matches!(
        diagnostic.code.as_ref(),
        Some(lsp_types::NumberOrString::String(code)) if code == "unknown_directive"
    )));

    let removed = server
        .diagnostics_for_text(
            "%name:x %n:x %family:x %f:x %group:x %g:x %tribe:x %t:x %wat:x"
                .to_string(),
        )
        .await;
    assert_eq!(
        removed
            .iter()
            .filter(|diagnostic| matches!(
                diagnostic.code.as_ref(),
                Some(lsp_types::NumberOrString::String(code)) if code == "unknown_directive"
            ))
            .count(),
        9
    );
}
