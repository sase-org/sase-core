//! Server coverage for completion after reopening a macro argument list.

use std::sync::Arc;

use lsp_types::{
    CompletionContext, CompletionParams, CompletionResponse,
    CompletionTriggerKind, PartialResultParams, Position,
    TextDocumentIdentifier, TextDocumentPositionParams, Uri,
    WorkDoneProgressParams,
};
use tower_lsp_server::{LanguageServer, LspService};

use sase_core::DocumentSnapshot;

use super::support::{bridge_with_catalog_entries, catalog_entry, input_hint};

fn apply_edits(text: &str, edits: &[lsp_types::TextEdit]) -> String {
    let document = DocumentSnapshot::new(text);
    let mut byte_edits = edits
        .iter()
        .map(|edit| {
            let start = document
                .position_to_byte_offset(
                    crate::lsp_convert::to_editor_position(edit.range.start),
                )
                .expect("edit start");
            let end = document
                .position_to_byte_offset(
                    crate::lsp_convert::to_editor_position(edit.range.end),
                )
                .expect("edit end");
            (start, end, edit.new_text.as_str())
        })
        .collect::<Vec<_>>();
    byte_edits.sort_by_key(|edit| std::cmp::Reverse(edit.0));
    let mut formatted = text.to_string();
    for (start, end, replacement) in byte_edits {
        formatted.replace_range(start..end, replacement);
    }
    formatted
}

#[tokio::test]
async fn completion_offers_remaining_argument_at_continuation_caret() {
    let entries = vec![catalog_entry(
        "foo",
        "#foo",
        Some("(bar: word, baz: word)".to_string()),
        vec![
            input_hint("bar", "word", false, 0),
            input_hint("baz", "word", false, 1),
        ],
        None,
    )];
    let (service, _) = LspService::new(|client| {
        crate::server::state::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(entries)),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().allow_all_markdown = true;
    let uri: Uri = "file:///tmp/sase_argument_continuation_completion.md"
        .parse()
        .unwrap();
    let source = "#foo(bar=1):: (Some text";
    let opened =
        server.open_document(&uri, "markdown".to_string(), source.to_string());
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), opened);

    let edits = server
        .on_type_formatting_for_text(
            source.to_string(),
            Position::new(
                0,
                source.find("Some text").expect("suffix start") as u32,
            ),
            "(",
        )
        .expect("on-type continuation edit");
    let formatted = apply_edits(source, &edits);
    assert_eq!(formatted, "#foo(bar=1,):: Some text");

    let previous = server.document_for_uri(&uri);
    let changed = server.changed_document(
        &uri,
        "markdown".to_string(),
        formatted.clone(),
        previous.as_ref(),
    );
    server
        .documents
        .write()
        .unwrap()
        .insert(uri.to_string(), changed);

    let response =
        <crate::server::state::MacroLspServer as LanguageServer>::completion(
            server,
            CompletionParams {
                text_document_position: TextDocumentPositionParams {
                    text_document: TextDocumentIdentifier { uri },
                    position: Position::new(0, "#foo(bar=1,".len() as u32),
                },
                work_done_progress_params: WorkDoneProgressParams::default(),
                partial_result_params: PartialResultParams::default(),
                context: Some(CompletionContext {
                    trigger_kind: CompletionTriggerKind::TRIGGER_CHARACTER,
                    trigger_character: Some("(".to_string()),
                }),
            },
        )
        .await
        .unwrap()
        .expect("completion response");
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    assert!(
        items.iter().any(|item| item.label == "baz="),
        "remaining baz= argument missing: {items:?}"
    );
}
