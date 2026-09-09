//! JSON-RPC stdio coverage for the `**model` shortcut completion trigger.
//!
//! The model catalog path is read from the process environment at
//! `initialize` time, so this file keeps all transition coverage in one test.

use std::sync::Arc;

use sase_core::HelperHostBridge;
use sase_xprompt_lsp::XpromptLspServer;
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tower_lsp_server::{LspService, Server};

const MODEL_CATALOG_ENV: &str = "SASE_XPROMPT_MODEL_CATALOG";

struct NoopBridge;

impl HelperHostBridge for NoopBridge {}

#[tokio::test]
async fn stdio_jsonrpc_model_shortcut_completion_transitions() {
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
                    "provider": "claude",
                    "aliases": []
                },
                {
                    "value": "gpt-5.6-sol",
                    "display": "gpt-5.6-sol",
                    "description": "Codex",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": ["gpt56sol"]
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

    let (mut client_writer, server_stdin) = duplex(16384);
    let (server_stdout, mut client_reader) = duplex(16384);
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
    read_response(&mut client_reader, 1).await;

    let uri = "file:///tmp/sase_prompt_model_shortcut.md";
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
                    "text": "Use *"
                }
            }
        }),
    )
    .await;

    let alias = request_completion(
        &mut client_writer,
        &mut client_reader,
        uri,
        2,
        5,
        json!({"triggerKind": 2, "triggerCharacter": "*"}),
    )
    .await;
    assert_eq!(labels(&alias), vec!["@large"]);
    assert_eq!(alias["items"][0]["filterText"], json!("*"));

    did_change(&mut client_writer, uri, 2, "Use **").await;
    let bare_model = request_completion(
        &mut client_writer,
        &mut client_reader,
        uri,
        3,
        6,
        json!({"triggerKind": 2, "triggerCharacter": "*"}),
    )
    .await;
    assert_eq!(labels(&bare_model), vec!["opus", "gpt-5.6-sol"]);
    assert_eq!(bare_model["items"][0]["filterText"], json!("**"));

    did_change(&mut client_writer, uri, 3, "Use **gp").await;
    let filtered_model = request_completion(
        &mut client_writer,
        &mut client_reader,
        uri,
        4,
        8,
        json!({"triggerKind": 3}),
    )
    .await;
    assert_eq!(labels(&filtered_model), vec!["gpt-5.6-sol"]);
    let item = &filtered_model["items"][0];
    assert_eq!(item["filterText"], json!("**gp"));
    assert_eq!(item["labelDetails"]["detail"], json!(" → %m:gpt-5.6-sol"));
    assert_eq!(item["detail"], json!("%m:gpt-5.6-sol · codex"));
    assert_eq!(
        item["textEdit"],
        json!({
            "range": {
                "start": {"line": 0, "character": 4},
                "end": {"line": 0, "character": 8}
            },
            "newText": "%m:gpt-5.6-sol "
        })
    );

    did_change(&mut client_writer, uri, 4, "Use **").await;
    let back_to_bare_model = request_completion(
        &mut client_writer,
        &mut client_reader,
        uri,
        5,
        6,
        json!({"triggerKind": 3}),
    )
    .await;
    assert_eq!(labels(&back_to_bare_model), vec!["opus", "gpt-5.6-sol"]);

    did_change(&mut client_writer, uri, 5, "Use *").await;
    let back_to_alias = request_completion(
        &mut client_writer,
        &mut client_reader,
        uri,
        6,
        5,
        json!({"triggerKind": 1}),
    )
    .await;
    assert_eq!(labels(&back_to_alias), vec!["@large"]);

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 7, "method": "shutdown", "params": null}),
    )
    .await;
    read_response(&mut client_reader, 7).await;
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();

    std::env::remove_var(MODEL_CATALOG_ENV);
}

async fn did_change(
    writer: &mut tokio::io::DuplexStream,
    uri: &str,
    version: i32,
    text: &str,
) {
    write_message(
        writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": version},
                "contentChanges": [{"text": text}]
            }
        }),
    )
    .await;
}

async fn request_completion(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    uri: &str,
    id: i64,
    character: u32,
    context: Value,
) -> Value {
    write_message(
        writer,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": character},
                "context": context
            }
        }),
    )
    .await;
    let result = read_response(reader, id).await;
    assert_eq!(result["isIncomplete"], json!(true));
    result
}

fn labels(result: &Value) -> Vec<&str> {
    result["items"]
        .as_array()
        .expect("completion items")
        .iter()
        .map(|item| item["label"].as_str().expect("label"))
        .collect()
}

async fn write_message(writer: &mut tokio::io::DuplexStream, value: Value) {
    let body = value.to_string();
    writer
        .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
        .await
        .unwrap();
    writer.write_all(body.as_bytes()).await.unwrap();
}

async fn read_response(reader: &mut tokio::io::DuplexStream, id: i64) -> Value {
    loop {
        let message = read_message(reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(id) {
            return message["result"].clone();
        }
    }
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
