//! JSON-RPC stdio coverage for the `=alias` shortcut completion trigger.
//!
//! This file deliberately holds exactly one test. The model catalog path is
//! read from the process environment at `initialize` time (mirroring the real
//! launcher), and Rust runs the tests inside one binary on parallel threads,
//! so a second test here would race this one's `set_var`.

use std::sync::Arc;

use sase_core::HelperHostBridge;
use sase_xprompt_lsp::XpromptLspServer;
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tower_lsp_server::{LspService, Server};

const MODEL_CATALOG_ENV: &str = "SASE_XPROMPT_MODEL_CATALOG";

/// Every [`HelperHostBridge`] method already defaults to
/// `BridgeUnavailable`; the `=alias` shortcut never calls the helper bridge
/// (its catalog is the launcher-materialized file), so no override is needed.
struct NoopBridge;

impl HelperHostBridge for NoopBridge {}

#[tokio::test]
async fn stdio_jsonrpc_model_alias_shortcut_completion() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("model_catalog.json");
    std::fs::write(
        &catalog_path,
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "entries": [
                {
                    "value": "opus",
                    "display": "opus",
                    "description": "Claude",
                    "kind": "model",
                    "provider": "claude"
                },
                {
                    "value": "@large",
                    "display": "@large",
                    "description": "Large pool",
                    "kind": "user_alias",
                    "aliases": ["large"],
                    "alias_kind": "user",
                    "target_provider": "claude",
                    "target_model": "opus",
                    "target_effort": "high"
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    std::env::set_var(MODEL_CATALOG_ENV, &catalog_path);

    let (mut client_writer, server_stdin) = duplex(8192);
    let (server_stdout, mut client_reader) = duplex(8192);
    let (service, socket) = LspService::new(|client| {
        XpromptLspServer::with_bridge(client, Arc::new(NoopBridge))
    });
    let server_task = tokio::spawn(async move {
        Server::new(server_stdin, server_stdout, socket)
            .serve(service)
            .await;
    });

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "processId": null,
                "rootUri": null,
                "capabilities": {}
            }
        }),
    )
    .await;
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(1)
    {}

    let uri = "file:///tmp/sase_prompt_model_alias_shortcut.md";
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": uri,
                    "languageId": "markdown",
                    "version": 1,
                    "text": "Use =la"
                }
            }
        }),
    )
    .await;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            break;
        }
    }

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 7}
            }
        }),
    )
    .await;

    let mut result = None;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(2) {
            result = Some(message["result"].clone());
            break;
        }
    }
    let result = result.expect("expected a shortcut completion response");

    assert_eq!(result["isIncomplete"], json!(true));
    let items = result["items"].as_array().expect("completion items");
    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item["label"], "@large");
    assert_eq!(item["filterText"], "=la");
    assert_eq!(item["preselect"], json!(true));
    assert_eq!(item["labelDetails"]["detail"], json!(" → %m:@large"));
    assert_eq!(
        item["textEdit"],
        json!({
            "range": {
                "start": {"line": 0, "character": 4},
                "end": {"line": 0, "character": 7}
            },
            "newText": "%m:@large "
        })
    );

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": 2},
                "contentChanges": [{"text": "Use *la"}]
            }
        }),
    )
    .await;
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 7}
            }
        }),
    )
    .await;
    let mut legacy_result = None;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(3) {
            legacy_result = Some(message["result"].clone());
            break;
        }
    }
    let legacy_result = legacy_result.expect("expected legacy star response");
    assert!(
        !json_result_contains_model_shortcut_edit(&legacy_result),
        "legacy star text should not produce a model shortcut edit"
    );

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 4, "method": "shutdown", "params": null}),
    )
    .await;
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(4)
    {}
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();

    std::env::remove_var(MODEL_CATALOG_ENV);
}

async fn write_message(writer: &mut tokio::io::DuplexStream, value: Value) {
    let body = value.to_string();
    writer
        .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
        .await
        .unwrap();
    writer.write_all(body.as_bytes()).await.unwrap();
}

async fn read_message(reader: &mut tokio::io::DuplexStream) -> Value {
    let mut header = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await.unwrap();
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header = String::from_utf8(header).unwrap();
    let length = header
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap();
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn json_result_contains_model_shortcut_edit(result: &Value) -> bool {
    let items = result
        .get("items")
        .and_then(Value::as_array)
        .or_else(|| result.as_array());
    items.is_some_and(|items| {
        items.iter().any(|item| {
            item.pointer("/textEdit/newText")
                .and_then(Value::as_str)
                .is_some_and(|new_text| new_text.starts_with("%m:"))
        })
    })
}
