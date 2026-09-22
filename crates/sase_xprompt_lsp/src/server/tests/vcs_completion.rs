use std::sync::Arc;

use lsp_types::{
    CompletionContext, CompletionItemKind, CompletionResponse,
    CompletionTextEdit, CompletionTriggerKind, Documentation, Position,
    TextDocumentIdentifier, TextDocumentPositionParams,
};

use super::super::*;

use super::support::*;

#[tokio::test]
async fn completes_vcs_project_with_primary_and_additional_edits() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
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
            "Describe this repo. +".to_string(),
            Position {
                line: 0,
                character: 21,
            },
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "sase");
    assert_eq!(item.kind, Some(CompletionItemKind::MODULE));
    let label_details = item.label_details.as_ref().unwrap();
    assert_eq!(label_details.description.as_deref(), Some("project"));
    // `filter_text` is the `+name` trigger spelling so typing `+sa` keeps
    // the item under client-side filtering.
    assert_eq!(item.filter_text.as_deref(), Some("+sase"));
    assert_eq!(item.detail.as_deref(), Some("+sase "));
    let Some(Documentation::MarkupContent(documentation)) =
        item.documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    assert_eq!(documentation.value, "SASE repo");

    // The primary edit replaces the `+` trigger token in place with the
    // project tag; nothing else in the segment needs deleting.
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected primary text edit");
    };
    assert_eq!(edit.new_text, "+sase ");
    assert_eq!(edit.range.start, Position::new(0, 20));
    assert_eq!(edit.range.end, Position::new(0, 21));
    assert!(item.additional_text_edits.is_none());
}

#[tokio::test]
async fn completes_vcs_project_replacing_existing_tag_at_eof() {
    // `#git:foo +` -- an existing leading VCS tag immediately followed by
    // the `+` trigger at end-of-input. Selecting a project inserts the tag
    // in place and deletes the existing `#git:foo` workspace target, so the
    // prompt never carries two targets.
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
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
            "#git:foo +".to_string(),
            Position {
                line: 0,
                character: 10,
            },
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "sase");

    // Primary edit replaces the trailing `+` trigger span (byte 9..10)
    // in place with the project tag.
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected primary text edit");
    };
    assert_eq!(edit.new_text, "+sase ");
    assert_eq!(edit.range.start, Position::new(0, 9));
    assert_eq!(edit.range.end, Position::new(0, 10));

    // Additional edit deletes the existing `#git:foo ` (bytes 0..9) target.
    let additional = item.additional_text_edits.as_ref().unwrap();
    assert_eq!(additional.len(), 1);
    assert_eq!(additional[0].new_text, "");
    assert_eq!(additional[0].range.start, Position::new(0, 0));
    assert_eq!(additional[0].range.end, Position::new(0, 9));
}

#[tokio::test]
async fn completes_vcs_patch_with_pr_label_details() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog_with_pr(&catalog_path);
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
            "+ship".to_string(),
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

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "ship-completion");
    assert_eq!(item.kind, Some(CompletionItemKind::EVENT));
    // `detail` mirrors the in-place insertion (with its trailing space)
    // until the LSP phase restyles project items.
    assert_eq!(item.detail.as_deref(), Some("#gh:ship-completion "));
    assert_eq!(item.filter_text.as_deref(), Some("+ship-completion"));
    let label_details = item.label_details.as_ref().unwrap();
    assert_eq!(label_details.detail.as_deref(), Some(" · sase"));
    assert_eq!(label_details.description.as_deref(), Some("PR · Ready"));
}

#[tokio::test]
async fn obsolete_and_unspaced_plus_forms_do_not_complete_vcs_projects() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
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

    // Line starts and tabs are tag left boundaries, so `line\n+` and
    // `\t+` complete now; only genuinely unclaimed `+` forms are listed.
    for (text, position) in [
        ("#+", Position::new(0, 2)),
        ("Fix #+sa", Position::new(0, 8)),
        ("word+", Position::new(0, 5)),
        ("a+b", Position::new(0, 3)),
        ("c++", Position::new(0, 3)),
    ] {
        let response =
            server.completion_for_text(text.to_string(), position).await;
        let has_vcs_project_item = match response {
            Some(CompletionResponse::Array(items)) => items
                .iter()
                .any(|item| item.detail.as_deref() == Some("#gh:sase")),
            Some(CompletionResponse::List(list)) => list
                .items
                .iter()
                .any(|item| item.detail.as_deref() == Some("#gh:sase")),
            None => false,
        };
        assert!(!has_vcs_project_item, "{text:?} should not complete");
    }
}

#[tokio::test]
async fn bare_plus_at_bof_completes_vcs_project() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
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

    // `+sa` at byte offset 0 completes, filtering by the bare-plus query.
    let response = server
        .completion_for_text(
            "+sa".to_string(),
            Position {
                line: 0,
                character: 3,
            },
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "sase");
    assert_eq!(item.kind, Some(CompletionItemKind::MODULE));
    let label_details = item.label_details.as_ref().unwrap();
    assert_eq!(label_details.description.as_deref(), Some("project"));
    // `filter_text` uses the bare-plus trigger spelling so typing `+sa`
    // keeps the item under client-side filtering.
    assert_eq!(item.filter_text.as_deref(), Some("+sase"));
    // BOF bare-plus: the trigger deletion is the whole change, so the edits
    // merge into one primary edit with no additional edits.
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected primary text edit");
    };
    assert_eq!(edit.new_text, "+sase ");
    assert!(item.additional_text_edits.is_none());
}

#[tokio::test]
async fn space_delimited_plus_completes_vcs_project() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
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
            "Fix +sa".to_string(),
            Position {
                line: 0,
                character: 7,
            },
        )
        .await
        .unwrap();

    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "sase");
    assert_eq!(items[0].filter_text.as_deref(), Some("+sase"));
}

#[tokio::test]
async fn automatic_and_manual_space_plus_completion_match() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
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
    let uri = file_uri(temp.path().join("sase_prompt_completion.md"));

    for (text, position, trigger_kind, trigger_character) in [
        (
            "+",
            Position::new(0, 1),
            CompletionTriggerKind::TRIGGER_CHARACTER,
            Some("+".to_string()),
        ),
        (
            "+sa",
            Position::new(0, 3),
            CompletionTriggerKind::INVOKED,
            None,
        ),
        (
            "Fix +",
            Position::new(0, 5),
            CompletionTriggerKind::TRIGGER_CHARACTER,
            Some("+".to_string()),
        ),
        (
            "Fix +sa",
            Position::new(0, 7),
            CompletionTriggerKind::INVOKED,
            None,
        ),
    ] {
        let document = server.open_document(
            &uri,
            "markdown".to_string(),
            text.to_string(),
        );
        server
            .documents
            .write()
            .unwrap()
            .insert(uri.to_string(), document);

        let response = server
            .completion(CompletionParams {
                text_document_position: TextDocumentPositionParams {
                    text_document: TextDocumentIdentifier { uri: uri.clone() },
                    position,
                },
                work_done_progress_params: Default::default(),
                partial_result_params: Default::default(),
                context: Some(CompletionContext {
                    trigger_kind,
                    trigger_character,
                }),
            })
            .await
            .unwrap()
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(items.len(), 1, "{text:?}");
        assert_eq!(items[0].label, "sase", "{text:?}");
        assert_eq!(items[0].filter_text.as_deref(), Some("+sase"));
    }
}

#[tokio::test]
async fn vcs_project_completion_without_catalog_is_empty() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = None;
    }

    let response = server
        .completion_for_text(
            "+".to_string(),
            Position {
                line: 0,
                character: 1,
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
async fn completes_vcs_ref_from_v3_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_ref_catalog(&catalog_path);
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
        .completion_for_text("#gh:".to_string(), Position::new(0, 4))
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
        vec!["sase", "ship-completion", "sase-org/", "bbugyi200/"]
    );

    let project = &items[0];
    assert_eq!(project.kind, Some(CompletionItemKind::MODULE));
    assert_eq!(project.filter_text.as_deref(), Some("sase"));
    assert_eq!(project.sort_text.as_deref(), Some("0:sase:0000"));
    assert_eq!(project.detail.as_deref(), Some("GitHub · #gh:sase"));
    assert_eq!(
        project
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("project")
    );
    let Some(Documentation::MarkupContent(documentation)) =
        project.documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    assert_eq!(documentation.value, "SASE repo");
    let Some(CompletionTextEdit::Edit(project_edit)) =
        project.text_edit.as_ref()
    else {
        panic!("expected project text edit");
    };
    assert_eq!(project_edit.range.start, Position::new(0, 4));
    assert_eq!(project_edit.range.end, Position::new(0, 4));
    assert_eq!(project_edit.new_text, "sase ");

    let patch = &items[1];
    assert_eq!(patch.kind, Some(CompletionItemKind::REFERENCE));
    assert_eq!(patch.filter_text.as_deref(), Some("ship-completion"));
    assert_eq!(patch.sort_text.as_deref(), Some("1:ship-completion:0001"));
    assert_eq!(
        patch.detail.as_deref(),
        Some("GitHub · #gh:ship-completion")
    );
    let patch_details = patch.label_details.as_ref().unwrap();
    assert_eq!(patch_details.detail.as_deref(), Some(" · sase"));
    assert_eq!(patch_details.description.as_deref(), Some("PR · Ready"));

    let namespace = &items[2];
    assert_eq!(namespace.kind, Some(CompletionItemKind::FOLDER));
    assert_eq!(namespace.filter_text.as_deref(), Some("sase-org"));
    assert_eq!(namespace.sort_text.as_deref(), Some("2:sase-org:0002"));
    assert_eq!(namespace.detail.as_deref(), Some("2 enabled projects"));
    assert_eq!(
        namespace
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("org")
    );
    let command = namespace.command.as_ref().unwrap();
    assert_eq!(command.command, "editor.action.triggerSuggest");
    let Some(CompletionTextEdit::Edit(namespace_edit)) =
        namespace.text_edit.as_ref()
    else {
        panic!("expected namespace text edit");
    };
    assert_eq!(namespace_edit.range.start, Position::new(0, 4));
    assert_eq!(namespace_edit.range.end, Position::new(0, 4));
    assert_eq!(namespace_edit.new_text, "sase-org/");
}

#[tokio::test]
async fn vcs_ref_completion_filters_aliases_and_namespaces() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_ref_catalog(&catalog_path);
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
        .completion_for_text("#gh:sase-c".to_string(), Position::new(0, 10))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "sase");
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected alias text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 4));
    assert_eq!(edit.range.end, Position::new(0, 10));
    assert_eq!(edit.new_text, "sase ");

    let response = server
        .completion_for_text("#gh:sa".to_string(), Position::new(0, 6))
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
        vec!["sase", "sase-org/"]
    );
}

#[tokio::test]
async fn vcs_ref_completion_accepts_v2_catalog_without_namespaces() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog_with_pr(&catalog_path);
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
        .completion_for_text("#gh:".to_string(), Position::new(0, 4))
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
        vec!["sase", "ship-completion"]
    );
    assert!(!items
        .iter()
        .any(|item| item.kind == Some(CompletionItemKind::FOLDER)));
}

#[tokio::test]
async fn vcs_ref_completion_ignores_malformed_namespaces() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 3,
            "workflow_names": ["gh"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub"
                }
            ],
            "namespaces": ["not", "a", "map"]
        }"##,
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
        config.vcs_project_catalog = Some(catalog_path);
    }

    let response = server
        .completion_for_text("#gh:".to_string(), Position::new(0, 4))
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
        vec!["sase"]
    );
    assert!(items[0].command.is_none());
}

#[tokio::test]
async fn vcs_ref_owner_slash_still_uses_repo_completion() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_ref_catalog(&catalog_path);
    let repo_response = vcs_repo_catalog_response(
        "ok",
        "",
        vec![repo_entry(
            "sase",
            "Structured Agentic Software Engineering",
            "private",
            false,
            false,
            Some("2026-07-07T18:00:00Z"),
        )],
    );
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_vcs_repo_catalog(repo_response.clone())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let text = "#gh:bbugyi200/".to_string();
    let response = server
        .completion_for_text(text.clone(), Position::new(0, text.len() as u32))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "sase");
    assert_eq!(items[0].filter_text.as_deref(), Some("bbugyi200/sase"));
    assert!(items[0].command.is_none());
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected repo text edit");
    };
    assert_eq!(edit.new_text, "bbugyi200/sase ");
}

#[tokio::test]
async fn completes_vcs_repo_with_ranked_items_and_text_edit() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
    let repo_response = vcs_repo_catalog_response(
        "ok",
        "",
        vec![
            repo_entry(
                "tooling",
                "Tooling repo",
                "public",
                false,
                false,
                Some("2026-07-07T18:30:00Z"),
            ),
            repo_entry(
                "sase-old",
                "Old SASE repo",
                "public",
                false,
                false,
                Some("2025-01-01T00:00:00Z"),
            ),
            repo_entry(
                "sase",
                "Structured Agentic Software Engineering",
                "private",
                true,
                true,
                Some("2026-07-07T18:00:00Z"),
            ),
        ],
    );
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_vcs_repo_catalog(repo_response.clone())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let text = "#gh:bbugyi200/sa".to_string();
    let response = server
        .completion_for_text(text.clone(), Position::new(0, text.len() as u32))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert_eq!(items.len(), 3);
    assert_eq!(items[0].label, "sase");
    assert_eq!(items[1].label, "sase-old");
    assert_eq!(items[2].label, "tooling");
    let item = &items[0];
    assert_eq!(item.kind, Some(CompletionItemKind::MODULE));
    assert_eq!(item.filter_text.as_deref(), Some("bbugyi200/sase"));
    assert_eq!(item.sort_text.as_deref(), Some("0000"));
    assert_eq!(
        item.label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("[private] [fork] [archived]")
    );
    assert_eq!(
        item.detail.as_deref(),
        Some("bbugyi200/sase [private] [fork] [archived]")
    );
    let Some(Documentation::MarkupContent(documentation)) =
        item.documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    assert!(documentation
        .value
        .contains("Structured Agentic Software Engineering"));
    assert!(documentation.value.contains("[private] [fork] [archived]"));

    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 4));
    assert_eq!(edit.range.end, Position::new(0, text.len() as u32));
    assert_eq!(edit.new_text, "bbugyi200/sase ");
    assert!(item.additional_text_edits.is_none());
}

#[tokio::test]
async fn vcs_repo_completion_error_response_is_empty() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    write_vcs_project_catalog(&catalog_path);
    let repo_response = vcs_repo_catalog_response(
        "error",
        "repo listing failed - run gh auth login",
        Vec::new(),
    );
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_vcs_repo_catalog(repo_response.clone())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.vcs_project_catalog = Some(catalog_path);
    }

    let text = "#gh:bbugyi200/".to_string();
    let response = server
        .completion_for_text(text.clone(), Position::new(0, text.len() as u32))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };

    assert!(items.is_empty());
}
