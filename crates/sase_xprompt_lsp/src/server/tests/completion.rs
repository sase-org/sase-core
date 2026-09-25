use std::sync::Arc;

use lsp_types::{
    CompletionItemKind, CompletionResponse, CompletionTextEdit, Documentation,
    MarkupKind, Position,
};

use sase_core::UnavailableHelperHostBridge;

use super::super::*;

use super::support::*;

#[tokio::test]
async fn completes_xprompt_from_static_catalog() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();
    let response = server
        .completion_for_text(
            "#fo".to_string(),
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
    assert!(items.iter().any(|item| item.label == "#foo"));
}

#[tokio::test]
async fn completes_identity_and_clan_from_the_public_editor_surface() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();
    for (token, name, alias, description) in [
        (
            "%id",
            "id",
            "i",
            "Assign an agent ID with optional bead, clan, session, or user-managed tribe",
        ),
        (
            "%i",
            "id",
            "i",
            "Assign an agent ID with optional bead, clan, session, or user-managed tribe",
        ),
        ("%cla", "clan", "c", "Declare a new parallel agent clan"),
        ("%c", "clan", "c", "Declare a new parallel agent clan"),
    ] {
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
        let expected_label = format!("%{name}");
        let item = items
            .iter()
            .find(|item| item.label == expected_label)
            .unwrap_or_else(|| panic!("expected canonical row for {token}"));
        let expected_detail = format!("alias %{alias}");
        assert_eq!(item.kind, Some(CompletionItemKind::TEXT));
        assert_eq!(item.filter_text.as_deref(), Some(name));
        assert_eq!(item.detail.as_deref(), Some(expected_detail.as_str()));
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected directive completion text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 0));
        assert_eq!(edit.range.end, Position::new(0, token.len() as u32));
        assert_eq!(edit.new_text, format!("%{name}"));
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected directive completion documentation");
        };
        assert_eq!(documentation.value, description);
    }
}

#[tokio::test]
async fn removed_identity_directives_do_not_complete() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    for token in ["%name", "%n", "%family", "%group", "%g", "%tribe", "%t"] {
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
        assert!(items.is_empty(), "{token}: {items:?}");
    }
}

#[tokio::test]
async fn directive_keyword_completion_uses_the_active_fragment_range() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    for (text, cursor, start, keyword, expected_documentation) in [
        (
            "%clan(research, tr)",
            18,
            16,
            "tribe=",
            "Assign this clan to a user-managed tribe",
        ),
        (
            "%c(research, tr)",
            15,
            13,
            "tribe=",
            "Assign this clan to a user-managed tribe",
        ),
        (
            "%clan(research, su)",
            18,
            16,
            "summary=",
            "Attach a Rich-markup summary to this clan",
        ),
        (
            "%clan(research, su)",
            18,
            16,
            "summary_script=",
            "Generate this clan's summary with an executable script",
        ),
        (
            "%id(worker, cl)",
            14,
            12,
            "clan=",
            "Derive the full ID and join this agent clan",
        ),
        (
            "%i(worker, cl)",
            13,
            11,
            "clan=",
            "Derive the full ID and join this agent clan",
        ),
        (
            "%id(worker, se)",
            14,
            12,
            "session=",
            "Attach this suffix to an existing agent session",
        ),
        (
            "%i(worker, tr)",
            13,
            11,
            "tribe=",
            "Assign this agent to a user-managed tribe",
        ),
    ] {
        let response = server
            .completion_for_text(text.to_string(), Position::new(0, cursor))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_completion_edit(
            &items,
            keyword,
            start,
            cursor,
            keyword,
            CompletionItemKind::KEYWORD,
        );
        let item = items
            .iter()
            .find(|item| item.label == keyword)
            .unwrap_or_else(|| panic!("missing {keyword} completion"));
        let Some(Documentation::MarkupContent(item_documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected directive keyword documentation");
        };
        assert_eq!(item_documentation.value, expected_documentation);
    }

    for (text, cursor) in [
        ("%clan(re", 8),
        ("%clan(research, tribe=blue)", 26),
        ("%id(wo", 6),
        ("%id(worker, clan=research)", 25),
    ] {
        let response = server
            .completion_for_text(text.to_string(), Position::new(0, cursor))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert!(items.is_empty(), "{text}: {items:?}");
    }
}

#[tokio::test]
async fn completes_directive_argument_values() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog(None)),
        )
    });
    let server = service.inner();

    let response = server
        .completion_for_text(
            "%effort:".to_string(),
            Position {
                line: 0,
                character: 8,
            },
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    let labels: Vec<&str> =
        items.iter().map(|item| item.label.as_str()).collect();
    assert_eq!(
        labels,
        vec!["none", "minimal", "low", "medium", "high", "xhigh", "max"]
    );
    assert_completion_edit(
        &items,
        "high",
        8,
        8,
        "high",
        CompletionItemKind::VALUE,
    );

    let response = server
        .completion_for_text(
            "%auto:t".to_string(),
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
    let labels: Vec<&str> =
        items.iter().map(|item| item.label.as_str()).collect();
    assert_eq!(labels, vec!["tale"]);
    assert_completion_edit(
        &items,
        "tale",
        6,
        7,
        "tale",
        CompletionItemKind::VALUE,
    );
}

#[tokio::test]
async fn wait_completion_uses_kind_aware_agent_catalog() {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.agent_catalog_response = serde_json::from_value(
        serde_json::json!({
            "schema_version": 1,
            "status": "ok",
            "message": "",
            "entries": [
                {"name": "planner", "status": "RUNNING", "project": "sase"},
                {"name": "review", "kind": "family", "member_count": 2, "detail": "family · 2 members", "documentation": "# review\n\nplan preview"},
                {"name": "builders", "kind": "clan", "member_count": 3, "detail": "clan · 3 members"},
                {"name": "@ops", "kind": "tribe", "member_count": 4, "detail": "tribe · 4 agents"}
            ]
        }),
    )
    .unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();

    let text = "%wait(planner, ";
    let response = server
        .completion_for_text(
            text.to_string(),
            Position::new(0, text.len() as u32),
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
        vec![
            "agent=", "bead=", "hood=", "proc=", "time=", "unit=", "@ops",
            "builders", "review"
        ]
    );
    assert_eq!(items[0].kind, Some(CompletionItemKind::KEYWORD));
    assert_eq!(items[6].kind, Some(CompletionItemKind::ENUM_MEMBER));
    assert_eq!(items[7].kind, Some(CompletionItemKind::MODULE));
    assert_eq!(items[8].kind, Some(CompletionItemKind::CLASS));
    assert_eq!(items[6].sort_text.as_deref(), Some("2:0006"));
    assert_eq!(
        items[7]
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("clan · 3 members")
    );
    assert!(items[7].documentation.is_none());
    let Some(Documentation::MarkupContent(review_doc)) =
        items[8].documentation.as_ref()
    else {
        panic!("expected markdown documentation for review family entry");
    };
    assert_eq!(review_doc.kind, MarkupKind::Markdown);
    assert_eq!(review_doc.value, "# review\n\nplan preview");

    let response = server
        .completion_for_text("%wait:op".to_string(), Position::new(0, 8))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "@ops");
    assert_eq!(items[0].filter_text.as_deref(), Some("ops"));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected tribe text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 6));
    assert_eq!(edit.new_text, "@ops");
}

#[tokio::test]
async fn hold_completion_warms_agent_catalog_for_positional_targets() {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.agent_catalog_response =
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "status": "ok",
            "message": "",
            "entries": [
                {"name": "planner", "status": "WAITING"},
                {"name": "coder", "status": "QUEUED"},
                {"name": "@builders", "kind": "tribe", "detail": "tribe"},
                {"name": "review", "kind": "clan", "detail": "clan"},
                {"name": "ship", "kind": "family", "detail": "family"},
                {"name": "build-shell", "kind": "proc", "status": "PENDING", "detail": "proc · PENDING"}
            ]
        }))
        .unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();

    let labels = labels_at(server, "%hold(").await;

    assert_eq!(
        labels,
        vec![
            "hood=",
            "scope=",
            "ttl=",
            "tribe=",
            "pending",
            "future",
            "planner",
            "coder",
            "@builders",
            "review",
            "ship",
            "build-shell",
        ]
    );
}

#[tokio::test]
async fn wait_bead_value_completion_uses_helper_rows() {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.agent_catalog_response = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "message": "",
        "entries": [
            {"name": "planner", "status": "RUNNING", "project": "sase"}
        ],
        "beads": [
            {
                "id": "sase-a",
                "title": "Active bug",
                "status": "in_progress",
                "type_label": "task",
                "updated_at": "2026-08-20T12:00:00Z",
                "task_type": "bug",
                "project": "sase"
            }
        ]
    }))
    .unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();
    let text = "%wait(bead=";
    let response = server
        .completion_for_text(
            text.to_string(),
            Position::new(0, text.len() as u32),
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
        vec!["sase-a"]
    );
    assert_eq!(items[0].kind, Some(CompletionItemKind::REFERENCE));
    assert!(items[0]
        .documentation
        .as_ref()
        .is_some_and(|doc| match doc {
            Documentation::MarkupContent(content) => {
                content.value.contains("Active bug")
            }
            _ => false,
        }));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected bead text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 11));
    assert_eq!(edit.new_text, "sase-a");
}

fn finalizer_catalog_json() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "message": "",
        "entries": [
            {
                "value": "commit",
                "provider_ref": "builtin@commit",
                "required": true,
                "default": true,
                "documentation": "Commit attributable repository changes"
            },
            {
                "value": "lint",
                "provider_ref": "builtin@command",
                "default": true,
                "after": ["format"],
                "max_attempts": 2,
                "documentation": "Lint the tree"
            },
            {
                "value": "zoom",
                "provider_ref": "plugin@zoom",
                "documentation": "Optional zoom"
            }
        ]
    })
}

#[tokio::test]
async fn final_completion_uses_catalog_and_dedicated_lsp_path() {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.finalizer_catalog_response =
        serde_json::from_value(finalizer_catalog_json()).unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();

    let response = server
        .completion_for_text("%final:".to_string(), Position::new(0, 7))
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
        vec!["commit", "lint", "zoom"]
    );
    assert_eq!(items[0].kind, Some(CompletionItemKind::ENUM_MEMBER));
    assert_eq!(items[1].kind, Some(CompletionItemKind::VALUE));
    assert_eq!(items[0].sort_text.as_deref(), Some("0:0000"));
    assert_eq!(
        items[0]
            .label_details
            .as_ref()
            .and_then(|details| details.detail.as_deref()),
        Some(" · required")
    );
    assert_eq!(
        items[0]
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("builtin@commit")
    );
    let Some(Documentation::MarkupContent(doc)) =
        items[0].documentation.as_ref()
    else {
        panic!("expected markdown documentation");
    };
    assert_eq!(doc.kind, MarkupKind::Markdown);
    assert!(doc.value.contains("Commit attributable repository changes"));
    assert!(doc.value.contains("Provider: `builtin@commit`"));
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected finalizer text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 7));
    assert_eq!(edit.new_text, "commit");

    let response = server
        .completion_for_text("%final:!".to_string(), Position::new(0, 8))
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
        vec!["!lint", "!zoom"]
    );
    assert_eq!(items[0].kind, Some(CompletionItemKind::OPERATOR));
    assert!(!items.iter().any(|item| item.label == "none"));

    let text = "%final(commit, !l";
    let response = server
        .completion_for_text(
            text.to_string(),
            Position::new(0, text.len() as u32),
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_eq!(items[0].label, "!lint");
    let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
    else {
        panic!("expected clause-local text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 15));
    assert_eq!(edit.new_text, "!lint");
}

#[tokio::test]
async fn final_completion_returns_empty_on_helper_failure() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(UnavailableHelperHostBridge),
        )
    });
    let server = service.inner();
    let response = server
        .completion_for_text("%final:".to_string(), Position::new(0, 7))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert!(items.is_empty(), "{items:?}");
}

#[tokio::test]
async fn final_completion_does_not_fetch_agent_catalog() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut inner = bridge_with_catalog_entries(Vec::new());
    inner.finalizer_catalog_response =
        serde_json::from_value(finalizer_catalog_json()).unwrap();
    let bridge = CountingAgentBridge {
        calls: calls.clone(),
        inner,
    };
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();
    let _ = server
        .completion_for_text("%final:".to_string(), Position::new(0, 7))
        .await
        .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn wait_keywords_survive_helper_failure_and_mixed_version_payloads() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(UnavailableHelperHostBridge),
        )
    });
    let server = service.inner();
    let response = server
        .completion_for_text("%wait(".to_string(), Position::new(0, 6))
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
        vec!["agent=", "bead=", "hood=", "proc=", "time=", "unit="]
    );

    let mut mixed = bridge_with_catalog_entries(Vec::new());
    mixed.agent_catalog_response = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "entries": [{"name": "planner", "status": "RUNNING"}]
    }))
    .unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(mixed))
    });
    let server = service.inner();
    let response = server
        .completion_for_text("%wait(bead=".to_string(), Position::new(0, 11))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert!(items.is_empty(), "{items:?}");
}

#[tokio::test]
async fn identity_and_static_value_roles_use_the_shared_contract() {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.agent_catalog_response = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "entries": [
            {"name": "planner", "kind": "agent"},
            {"name": "sase-11l", "kind": "hood", "member_count": 2, "detail": "hood · 2 members"},
            {"name": "builders", "kind": "clan", "member_count": 3, "detail": "clan · 3 members"},
            {"name": "review", "kind": "family", "member_count": 2},
            {"name": "@ops", "kind": "tribe", "member_count": 4}
        ],
        "beads": [{"id": "sase-a", "title": "Active bug", "status": "in_progress"}]
    }))
    .unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();

    let clan = labels_at(server, "%id(worker, clan=").await;
    assert_eq!(clan, vec!["builders"]);
    let family = labels_at(server, "%i(worker, family=").await;
    assert_eq!(family, vec!["review"]);
    let session = labels_at(server, "%i(worker, session=").await;
    assert_eq!(session, vec!["review"]);
    let tribe = labels_at(server, "%clan(research, tribe=").await;
    assert_eq!(tribe, vec!["@ops"]);
    let hood = labels_at(server, "%hold(hood=s").await;
    assert_eq!(hood, vec!["sase-11l"]);
    let bead = labels_at(server, "%id(worker, bead=").await;
    assert_eq!(bead, vec!["sase-a"]);
    assert_eq!(labels_at(server, "%wait(time=").await, vec!["5m", "1430"]);
    assert!(labels_at(server, "%wait(runners=").await.is_empty());
    assert!(labels_at(server, "%wait(priority=").await.is_empty());
    assert_eq!(labels_at(server, "%q:").await, vec!["1", "100"]);
    assert_eq!(labels_at(server, "%q(p=").await, vec!["10", "1"]);
    assert_eq!(
        labels_at(server, "%q(weight=").await,
        vec!["0", "0.25", "1.0", "2.0"]
    );
    assert_eq!(labels_at(server, "%repeat:").await, vec!["2", "3"]);
    assert_eq!(
        labels_at(server, "%xprompts_enabled:").await,
        vec!["false", "true"]
    );
    assert_eq!(labels_at(server, "%e:xh").await, vec!["xhigh"]);
    assert!(labels_at(server, "%w:t")
        .await
        .iter()
        .all(|value| !value.ends_with('=')));
    assert_eq!(
        labels_at(server, "%q").await,
        vec![
            "%queue",
            "%queue:...",
            "%q:...",
            "%queue(capacity=..., priority=...)"
        ]
    );
    assert_eq!(
        labels_at(server, "%queue(").await,
        vec!["capacity=", "p=", "priority=", "w=", "weight=", "1", "100"]
    );
    assert_eq!(
        labels_at(server, "%q(weight=").await,
        vec!["0", "0.25", "1.0", "2.0"]
    );
}

#[tokio::test]
async fn directive_name_completion_documents_queue_capacity_flag_state() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();

    let response = server
        .completion_for_text("%q".to_string(), Position::new(0, 2))
        .await
        .unwrap();
    let items = completion_items(response);
    let queue = items
        .iter()
        .find(|item| item.label == "%queue")
        .expect("%queue item");
    assert_eq!(
        markdown_documentation(queue),
        Some(
            "Set this launch's capacity budget, priority, and capacity weight"
        )
    );

    server.config.write().unwrap().queue_capacity_budget = false;
    let response = server
        .completion_for_text("%q".to_string(), Position::new(0, 2))
        .await
        .unwrap();
    let items = completion_items(response);
    let queue = items
        .iter()
        .find(|item| item.label == "%queue")
        .expect("%queue item");
    assert_eq!(
        markdown_documentation(queue),
        Some("Set weighted-load capacity, priority, and capacity weight")
    );
}

#[tokio::test]
async fn queue_completion_avoids_agent_targets() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut inner = bridge_with_catalog_entries(Vec::new());
    inner.agent_catalog_response = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "entries": [{"name": "planner", "status": "RUNNING"}]
    }))
    .unwrap();
    let bridge = CountingAgentBridge {
        calls: calls.clone(),
        inner,
    };
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();

    assert_eq!(
        labels_at(server, "%q(").await,
        vec!["capacity=", "p=", "priority=", "w=", "weight=", "1", "100"]
    );
    assert_eq!(labels_at(server, "%q:").await, vec!["1", "100"]);
    assert_eq!(
        labels_at(server, "%q(5, ").await,
        vec!["p=", "priority=", "w=", "weight="]
    );
    assert_eq!(labels_at(server, "%q(p=").await, vec!["10", "1"]);
    assert_eq!(
        labels_at(server, "%queue(capacity=").await,
        vec!["1", "100"]
    );
    assert_eq!(
        labels_at(server, "%wait(").await,
        vec!["agent=", "bead=", "hood=", "proc=", "time=", "unit=", "planner"]
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(labels_at(server, "%q(")
        .await
        .iter()
        .all(|value| value != "planner"));
    server.config.write().unwrap().queue_capacity_budget = false;
    assert_eq!(labels_at(server, "%q:").await, vec!["0", "1"]);
    assert_eq!(labels_at(server, "%queue(capacity=").await, vec!["0", "1"]);
}

#[tokio::test]
async fn wait_unicode_mid_clause_uses_utf16_replacement_range() {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.agent_catalog_response = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "entries": []
    }))
    .unwrap();
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();
    let text = "%wait(café, be";
    let response = server
        .completion_for_text(text.to_string(), Position::new(0, 14))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_completion_edit(
        &items,
        "bead=",
        12,
        14,
        "bead=",
        CompletionItemKind::KEYWORD,
    );
}

#[tokio::test]
async fn host_catalog_is_not_fetched_for_static_value_roles() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut inner = bridge_with_catalog_entries(Vec::new());
    inner.agent_catalog_response = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "status": "ok",
        "entries": [{"name": "planner"}],
        "beads": [{"id": "sase-a"}]
    }))
    .unwrap();
    let bridge = CountingAgentBridge {
        calls: calls.clone(),
        inner,
    };
    let (service, _) = LspService::new(move |client| {
        XpromptLspServer::with_bridge(client, Arc::new(bridge))
    });
    let server = service.inner();
    let _ = labels_at(server, "%wait(time=").await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let _ = labels_at(server, "%wait(bead=").await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let _ = labels_at(server, "%id(worker, clan=").await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn model_paren_completion_offers_alias_keys_and_values() {
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

    let first = labels_at(server, "%model(scout").await;
    assert!(first.iter().any(|label| *label == "scout="), "{first:?}");

    let keys = labels_at(server, "%m(opus, ").await;
    assert!(keys.contains(&"scout=".to_string()), "{keys:?}");
    assert!(keys.contains(&"default=".to_string()), "{keys:?}");
    assert!(!keys.iter().any(|label| label == "opus"), "{keys:?}");

    let values = labels_at(server, "%model(opus, scout=").await;
    assert!(values.contains(&"opus".to_string()), "{values:?}");
    assert!(!values.iter().any(|label| label == "@scout"), "{values:?}");

    let earlier = "%model(op, scout=sonnet)";
    let response = server
        .completion_for_text(earlier.to_string(), Position::new(0, 9))
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert_completion_edit(
        &items,
        "opus",
        7,
        9,
        "opus",
        CompletionItemKind::VALUE,
    );
}

#[tokio::test]
async fn directive_matrix_completes_every_advertised_name_and_alias() {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    for (token, expected) in [
        ("%m", "%model"),
        ("%e", "%effort"),
        ("%i", "%id"),
        ("%c", "%clan"),
        ("%w", "%wait"),
        ("%a", "%auto"),
        ("%h", "%hide"),
        ("%r", "%repeat"),
        ("%f", "%final"),
        ("%final", "%final"),
        ("%xprompts_enabled", "%xprompts_enabled"),
    ] {
        let labels = labels_at(server, token).await;
        assert!(
            labels.contains(&expected.to_string()),
            "{token}: {labels:?}"
        );
    }
    let alt = labels_at(server, "%alt").await;
    assert!(alt.contains(&"%alt".to_string()), "{alt:?}");
}
