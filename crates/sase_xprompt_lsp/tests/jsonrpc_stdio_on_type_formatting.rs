//! JSON-RPC stdio coverage for `(` on-type argument-colon conversion.

use std::sync::Arc;

use sase_core::HelperHostBridge;
use sase_xprompt_lsp::XpromptLspServer;
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tower_lsp_server::{LspService, Server};

struct NoopBridge;

impl HelperHostBridge for NoopBridge {}

#[tokio::test]
async fn stdio_jsonrpc_on_type_formatting_deletes_invocation_colon() {
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
    let initialized = read_response(&mut client_reader, 1).await;
    assert_eq!(
        initialized["capabilities"]["documentOnTypeFormattingProvider"],
        json!({"firstTriggerCharacter": "("})
    );

    let uri = "file:///tmp/sase_prompt_on_type.md";
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;
    did_open(&mut client_writer, uri, "%q:(").await;

    let basic =
        request_on_type(&mut client_writer, &mut client_reader, uri, 2, 4, "(")
            .await;
    assert_eq!(
        basic,
        json!([{
            "range": {
                "start": {"line": 0, "character": 2},
                "end": {"line": 0, "character": 3}
            },
            "newText": ""
        }])
    );
    let trigger_at_cursor =
        request_on_type(&mut client_writer, &mut client_reader, uri, 3, 3, "(")
            .await;
    assert_eq!(trigger_at_cursor, basic);

    did_change(&mut client_writer, uri, 2, "%q:()").await;
    let with_editor_closer =
        request_on_type(&mut client_writer, &mut client_reader, uri, 4, 4, "(")
            .await;
    assert_eq!(with_editor_closer, basic);

    did_change(&mut client_writer, uri, 3, "%q()").await;
    let completion =
        request_completion(&mut client_writer, &mut client_reader, uri, 5, 3)
            .await;
    assert!(
        labels(&completion).contains(&"priority="),
        "parenthesized queue completion missing priority=: {completion:?}"
    );

    did_change(&mut client_writer, uri, 4, "🙂 %q:(").await;
    let unicode =
        request_on_type(&mut client_writer, &mut client_reader, uri, 6, 7, "(")
            .await;
    assert_eq!(
        unicode,
        json!([{
            "range": {
                "start": {"line": 0, "character": 5},
                "end": {"line": 0, "character": 6}
            },
            "newText": ""
        }])
    );

    did_change(&mut client_writer, uri, 5, "%q(").await;
    assert_eq!(
        request_on_type(
            &mut client_writer,
            &mut client_reader,
            uri,
            7,
            3,
            "(",
        )
        .await,
        Value::Null
    );
    did_change(&mut client_writer, uri, 6, "%q:(").await;
    assert_eq!(
        request_on_type(
            &mut client_writer,
            &mut client_reader,
            uri,
            8,
            4,
            ")",
        )
        .await,
        Value::Null
    );

    let ineligible_uri = "file:///tmp/ordinary_markdown.md";
    did_open(&mut client_writer, ineligible_uri, "%q:(").await;
    assert_eq!(
        request_on_type(
            &mut client_writer,
            &mut client_reader,
            ineligible_uri,
            9,
            4,
            "(",
        )
        .await,
        Value::Null
    );

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 9, "method": "shutdown", "params": null}),
    )
    .await;
    read_response(&mut client_reader, 9).await;
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();
}

async fn did_open(writer: &mut tokio::io::DuplexStream, uri: &str, text: &str) {
    write_message(
        writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": uri,
                    "languageId": "markdown",
                    "version": 1,
                    "text": text
                }
            }
        }),
    )
    .await;
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

async fn request_on_type(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    uri: &str,
    id: i64,
    character: u32,
    ch: &str,
) -> Value {
    write_message(
        writer,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "textDocument/onTypeFormatting",
            "params": {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": character},
                "ch": ch,
                "options": {"tabSize": 2, "insertSpaces": true}
            }
        }),
    )
    .await;
    read_response(reader, id).await
}

async fn request_completion(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    uri: &str,
    id: i64,
    character: u32,
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
                "context": {"triggerKind": 2, "triggerCharacter": "("}
            }
        }),
    )
    .await;
    read_response(reader, id).await
}

fn labels(result: &Value) -> Vec<&str> {
    result["items"]
        .as_array()
        .or_else(|| result.as_array())
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
