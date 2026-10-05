//! Focused server tests for completion-owned xprompt spacer acceptance.

use std::sync::Arc;

use lsp_types::{
    CompletionContext, CompletionParams, CompletionResponse,
    CompletionTextEdit, CompletionTriggerKind, PartialResultParams, Position,
    TextDocumentIdentifier, TextDocumentPositionParams, Uri,
    WorkDoneProgressParams,
};
use tower_lsp_server::{LanguageServer, LspService};

use sase_core::DocumentSnapshot;

use super::super::spacer::ACCEPT_COMMAND;
use super::support::{
    bridge_with_catalog_entries, catalog_entry, file_uri, input_hint,
};

fn optional_entries() -> Vec<sase_core::MobileMacroCatalogEntryWire> {
    vec![
        catalog_entry(
            "optional",
            "#optional",
            None,
            vec![
                input_hint("topic", "word", false, 0),
                input_hint("count", "int", false, 1),
            ],
            None,
        ),
        catalog_entry("plain", "#plain", None, Vec::new(), None),
    ]
}

fn eligible_uri(temp: &tempfile::TempDir) -> Uri {
    file_uri(temp.path().join("sase_prompt_spacer.md"))
}

async fn completion_at(
    server: &crate::server::state::MacroLspServer,
    uri: &Uri,
    position: Position,
    trigger_character: Option<String>,
) -> CompletionResponse {
    let trigger_kind = if trigger_character.is_some() {
        CompletionTriggerKind::TRIGGER_CHARACTER
    } else {
        CompletionTriggerKind::INVOKED
    };
    <crate::server::state::MacroLspServer as LanguageServer>::completion(
        server,
        CompletionParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: WorkDoneProgressParams::default(),
            partial_result_params: PartialResultParams::default(),
            context: Some(CompletionContext {
                trigger_kind,
                trigger_character,
            }),
        },
    )
    .await
    .unwrap()
    .unwrap()
}

fn find_item(
    response: &CompletionResponse,
    label: &str,
) -> lsp_types::CompletionItem {
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    items
        .iter()
        .find(|item| item.label == label)
        .unwrap_or_else(|| panic!("expected item {label}"))
        .clone()
}

fn apply_edit(text: &str, item: &lsp_types::CompletionItem) -> String {
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit");
    };
    let doc = DocumentSnapshot::new(text);
    let start = doc
        .position_to_byte_offset(crate::lsp_convert::to_editor_position(
            edit.range.start,
        ))
        .unwrap();
    let end = doc
        .position_to_byte_offset(crate::lsp_convert::to_editor_position(
            edit.range.end,
        ))
        .unwrap();
    format!("{}{}{}", &text[..start], edit.new_text, &text[end..])
}

#[tokio::test]
async fn eligible_completion_carries_command_while_zero_input_does_not() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.snippet_support = true;
    }
    let uri = eligible_uri(&temp);
    let document =
        server.open_document(&uri, "markdown".to_string(), "#".to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), document);

    let response = completion_at(server, &uri, Position::new(0, 1), None).await;
    let optional = find_item(&response, "#optional");
    assert!(optional.command.is_some(), "eligible item needs a command");
    let command = optional.command.as_ref().unwrap();
    assert_eq!(command.command, ACCEPT_COMMAND);

    let plain = find_item(&response, "#plain");
    assert!(
        plain.command.is_none(),
        "zero-input must not carry a command"
    );
}

async fn accept_through_command(
    server: &crate::server::state::MacroLspServer,
    item: &lsp_types::CompletionItem,
) {
    let command = item.command.as_ref().expect("item needs a command");
    let value = command.arguments.as_ref().unwrap()[0].clone();
    let accepted = server.handle_accept_command(&value);
    assert!(accepted, "acceptance command must record");
}

fn did_change(
    server: &crate::server::state::MacroLspServer,
    uri: &Uri,
    text: String,
) {
    let previous = server.document_for_uri(uri);
    let language = previous
        .as_ref()
        .map(|document| document.language_id.clone())
        .unwrap_or_else(|| "markdown".to_string());
    let next = server.changed_document(uri, language, text, previous.as_ref());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), next);
}

async fn on_type(
    server: &crate::server::state::MacroLspServer,
    uri: &Uri,
    position: Position,
    ch: &str,
) -> Option<Vec<lsp_types::TextEdit>> {
    <crate::server::state::MacroLspServer as LanguageServer>::on_type_formatting(
        server,
        lsp_types::DocumentOnTypeFormattingParams {
            text_document_position: lsp_types::TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            ch: ch.to_string(),
            options: lsp_types::FormattingOptions::default(),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn accept_after_change_then_format_deletes_owned_spacer() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().snippet_support = true;
    let uri = eligible_uri(&temp);
    let document =
        server.open_document(&uri, "markdown".to_string(), "#o".to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), document);

    let response = completion_at(server, &uri, Position::new(0, 2), None).await;
    let item = find_item(&response, "#optional");
    let accepted_text = apply_edit("#o", &item);
    assert_eq!(accepted_text, "#optional ");
    did_change(server, &uri, accepted_text);
    accept_through_command(server, &item).await;

    // Plain `(`: `#optional (` with caret after the opener.
    did_change(server, &uri, "#optional (".to_string());
    let edits = on_type(server, &uri, Position::new(0, 11), "(")
        .await
        .expect("formatting must delete the spacer");
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].new_text, "");
    assert_eq!(edits[0].range.start, Position::new(0, 9));
    assert_eq!(edits[0].range.end, Position::new(0, 10));

    // Repeated requests on the unchanged snapshot stay consistent.
    let again = on_type(server, &uri, Position::new(0, 11), "(")
        .await
        .unwrap();
    assert_eq!(again, edits);

    // Applying the deletion clears the state; the normalized buffer uses the
    // ordinary argument route with no extra deletion.
    did_change(server, &uri, "#optional(".to_string());
    let after = completion_at(
        server,
        &uri,
        Position::new(0, 10),
        Some("(".to_string()),
    )
    .await;
    let CompletionResponse::Array(items) = after else {
        panic!("expected completion array");
    };
    assert!(items.iter().any(|item| item.label == "topic="));
    for item in &items {
        assert!(item.command.is_none());
        for edit in item.additional_text_edits.clone().unwrap_or_default() {
            assert_ne!(edit.new_text, "", "no spacer deletion after normalize");
        }
    }
}

#[tokio::test]
async fn accept_before_change_and_coalesced_opener() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().snippet_support = true;
    let uri = eligible_uri(&temp);
    let document =
        server.open_document(&uri, "markdown".to_string(), "#o".to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), document);

    let response = completion_at(server, &uri, Position::new(0, 2), None).await;
    let item = find_item(&response, "#optional");
    // Command before `didChange`: parked as pending.
    accept_through_command(server, &item).await;
    // Coalesced acceptance plus `(` in one change.
    did_change(server, &uri, "#optional (".to_string());
    let edits = on_type(server, &uri, Position::new(0, 11), "(")
        .await
        .expect("coalesced transition must format");
    assert_eq!(edits[0].range.start, Position::new(0, 9));
    assert_eq!(edits[0].new_text, "");
}

#[tokio::test]
async fn transition_completion_maps_ranges_with_spacer_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().snippet_support = true;
    let uri = eligible_uri(&temp);
    let document =
        server.open_document(&uri, "markdown".to_string(), "#o".to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), document);

    let response = completion_at(server, &uri, Position::new(0, 2), None).await;
    let item = find_item(&response, "#optional");
    did_change(server, &uri, apply_edit("#o", &item));
    accept_through_command(server, &item).await;
    // Still `#optional (`: argument completion must come from the normalized
    // view with actual coordinates plus a spacer deletion.
    did_change(server, &uri, "#optional (".to_string());
    let mapped = completion_at(
        server,
        &uri,
        Position::new(0, 11),
        Some("(".to_string()),
    )
    .await;
    let CompletionResponse::Array(items) = mapped else {
        panic!("expected completion array");
    };
    let topic = items
        .iter()
        .find(|item| item.label == "topic=")
        .expect("topic=");
    let Some(CompletionTextEdit::Edit(primary)) = topic.text_edit.as_ref()
    else {
        panic!("expected primary edit");
    };
    // Primary edit must use actual (unnormalized) coordinates: inserting
    // `topic=` at the caret after `(` in `#optional (`.
    assert_eq!(primary.range.start, Position::new(0, 11));
    let additional = topic.additional_text_edits.clone().unwrap_or_default();
    assert!(additional.iter().any(|edit| {
        edit.range.start == Position::new(0, 9)
            && edit.range.end == Position::new(0, 10)
            && edit.new_text.is_empty()
    }));
}

#[tokio::test]
async fn autopair_unicode_suffix_and_before_opener_conventions() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().snippet_support = true;
    let uri = eligible_uri(&temp);
    let document =
        server.open_document(&uri, "markdown".to_string(), "#o".to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), document);

    let response = completion_at(server, &uri, Position::new(0, 2), None).await;
    let item = find_item(&response, "#optional");
    did_change(server, &uri, apply_edit("#o", &item));
    accept_through_command(server, &item).await;

    // Separate autopair `()`: `#optional ()` with caret between parens.
    did_change(server, &uri, "#optional ()".to_string());
    let edits = on_type(server, &uri, Position::new(0, 11), "(")
        .await
        .expect("autopair must format");
    assert_eq!(edits[0].range.start, Position::new(0, 9));

    // Suffix preservation: opener plus trailing prose still deletes only the
    // owned spacer.
    did_change(server, &uri, "#optional (suffix)".to_string());
    let suffix_edits = on_type(server, &uri, Position::new(0, 11), "(")
        .await
        .expect("suffix must format");
    assert_eq!(suffix_edits[0].new_text, "");

    // Before-opener caret (on the `(` itself) formats the same deletion.
    did_change(server, &uri, "#optional (".to_string());
    let before = on_type(server, &uri, Position::new(0, 10), "(")
        .await
        .expect("before-opener must format");
    assert_eq!(before[0].range.start, Position::new(0, 9));
}

#[tokio::test]
async fn unicode_prefix_keeps_exact_deletion_range() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().snippet_support = true;
    let uri = eligible_uri(&temp);
    // Astral prefix present before completion is served, so predicted
    // positions already account for UTF-16 columns.
    let document =
        server.open_document(&uri, "markdown".to_string(), "🙂 #o".to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), document);

    // `🙂 ` is 3 UTF-16 units (astral is 2 plus a space); `#o` ends at 5.
    let response = completion_at(server, &uri, Position::new(0, 5), None).await;
    let item = find_item(&response, "#optional");
    // Reference starts at column 3; applying the edit preserves the prefix.
    let accepted = apply_edit("🙂 #o", &item);
    assert_eq!(accepted, "🙂 #optional ");
    did_change(server, &uri, accepted);
    accept_through_command(server, &item).await;

    did_change(server, &uri, "🙂 #optional (".to_string());
    // Spacer at column 12 (3 + 9), caret after the opener at 14.
    let edits = on_type(server, &uri, Position::new(0, 14), "(")
        .await
        .expect("unicode must format");
    assert_eq!(edits[0].range.start, Position::new(0, 12));
    assert_eq!(edits[0].range.end, Position::new(0, 13));
    assert_eq!(edits[0].new_text, "");
}

#[tokio::test]
async fn stale_close_and_unrelated_changes_reject() {
    let temp = tempfile::tempdir().unwrap();
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(optional_entries())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().snippet_support = true;
    let uri = eligible_uri(&temp);
    let other = file_uri(temp.path().join("sase_prompt_other.md"));
    for other_uri in [&uri, &other] {
        let document = server.open_document(
            other_uri,
            "markdown".to_string(),
            "#o".to_string(),
        );
        server
            .documents
            .write()
            .unwrap()
            .insert(other_uri.to_string(), document);
    }

    let response = completion_at(server, &uri, Position::new(0, 2), None).await;
    let item = find_item(&response, "#optional");
    let value =
        item.command.as_ref().unwrap().arguments.as_ref().unwrap()[0].clone();

    // No acceptance command: formatting must not delete authored whitespace.
    did_change(server, &uri, "#optional (".to_string());
    assert!(on_type(server, &uri, Position::new(0, 11), "(")
        .await
        .is_none());

    // Unrelated change invalidates a later stale command, even when the text
    // later looks right again: the generation has advanced past the serving
    // snapshot plus its acceptance change.
    did_change(server, &uri, "#optional ".to_string());
    did_change(server, &uri, "unrelated edit\n#optional ".to_string());
    assert!(!server.handle_accept_command(&value));
    did_change(server, &uri, "#optional ".to_string());
    assert!(!server.handle_accept_command(&value));

    // Multiple documents stay independent: the other buffer never acquired
    // acceptance, so its identical-looking transition must not format.
    did_change(server, &other, "#optional (".to_string());
    assert!(on_type(server, &other, Position::new(0, 11), "(")
        .await
        .is_none());

    // Close/reopen drops acceptance state: a fresh acceptance confirms, but
    // reopening without it must not format.
    did_change(server, &other, "#o".to_string());
    let fresh = completion_at(server, &other, Position::new(0, 2), None).await;
    // `#o` in the other buffer still completes to `#optional `.
    let other_item = find_item(&fresh, "#optional");
    did_change(server, &other, apply_edit("#o", &other_item));
    accept_through_command(server, &other_item).await;
    did_change(server, &other, "#optional (".to_string());
    assert!(
        on_type(server, &other, Position::new(0, 11), "(")
            .await
            .is_some(),
        "fresh acceptance must format"
    );
    server.documents.write().unwrap().remove(&other.to_string());
    let reopened = server.open_document(
        &other,
        "markdown".to_string(),
        "#optional (".to_string(),
    );
    server
        .documents
        .write()
        .unwrap()
        .insert(other.to_string(), reopened);
    assert!(on_type(server, &other, Position::new(0, 11), "(")
        .await
        .is_none());
}
