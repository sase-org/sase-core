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

#[tokio::test]
async fn stdio_jsonrpc_on_type_formatting_moves_double_colon_delimiter() {
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
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;

    let uri = "file:///tmp/sase_prompt_double_colon_on_type.md";
    did_open(&mut client_writer, uri, "#foo::  ").await;
    did_change(&mut client_writer, uri, 2, "#foo::  (").await;
    let unpaired =
        request_on_type(&mut client_writer, &mut client_reader, uri, 2, 9, "(")
            .await;
    assert_eq!(
        unpaired,
        json!([
            {
                "range": {
                    "start": {"line": 0, "character": 4},
                    "end": {"line": 0, "character": 8}
                },
                "newText": ""
            },
            {
                "range": {
                    "start": {"line": 0, "character": 9},
                    "end": {"line": 0, "character": 9}
                },
                "newText": ")::  "
            }
        ])
    );
    assert_eq!(apply_text_edits("#foo::  (", &unpaired), "#foo()::  ");

    did_change(&mut client_writer, uri, 3, "#foo::  ").await;
    did_change(&mut client_writer, uri, 4, "#foo::  ()").await;
    let paired =
        request_on_type(&mut client_writer, &mut client_reader, uri, 3, 9, "(")
            .await;
    assert_eq!(
        paired,
        json!([
            {
                "range": {
                    "start": {"line": 0, "character": 4},
                    "end": {"line": 0, "character": 8}
                },
                "newText": ""
            },
            {
                "range": {
                    "start": {"line": 0, "character": 9},
                    "end": {"line": 0, "character": 10}
                },
                "newText": ")::  "
            }
        ])
    );
    assert_eq!(apply_text_edits("#foo::  ()", &paired), "#foo()::  ");

    did_change(&mut client_writer, uri, 5, "#foo::  )").await;
    did_change(&mut client_writer, uri, 6, "#foo::  ()").await;
    let existing_suffix =
        request_on_type(&mut client_writer, &mut client_reader, uri, 4, 9, "(")
            .await;
    assert_eq!(
        apply_text_edits("#foo::  ()", &existing_suffix),
        "#foo()::  )"
    );

    did_change(&mut client_writer, uri, 7, "#foo:: body").await;
    did_change(&mut client_writer, uri, 8, "#foo:: (body").await;
    let suffix =
        request_on_type(&mut client_writer, &mut client_reader, uri, 5, 8, "(")
            .await;
    assert_eq!(apply_text_edits("#foo:: (body", &suffix), "#foo():: body");

    did_change(&mut client_writer, uri, 9, "🙂\n#foo:: ").await;
    did_change(&mut client_writer, uri, 10, "🙂\n#foo:: (").await;
    let multiline = request_on_type_at(
        &mut client_writer,
        &mut client_reader,
        uri,
        6,
        1,
        8,
        "(",
    )
    .await;
    assert_eq!(
        apply_text_edits("🙂\n#foo:: (", &multiline),
        "🙂\n#foo():: "
    );

    did_change(&mut client_writer, uri, 11, "%clan:: ").await;
    did_change(&mut client_writer, uri, 12, "%clan:: (").await;
    let directive =
        request_on_type(&mut client_writer, &mut client_reader, uri, 7, 9, "(")
            .await;
    assert_eq!(apply_text_edits("%clan:: (", &directive), "%clan():: ");

    for (version, text, character) in [
        (14, "#foo::\t(", 8),
        (15, "#foo:::(", 8),
        (16, "%if:: (", 7),
        (17, "#foo(args):: (", 14),
    ] {
        did_change(&mut client_writer, uri, version, text).await;
        assert_eq!(
            request_on_type(
                &mut client_writer,
                &mut client_reader,
                uri,
                i64::from(version),
                character,
                "(",
            )
            .await,
            Value::Null,
            "{text}"
        );
    }

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 99, "method": "shutdown", "params": null}),
    )
    .await;
    read_response(&mut client_reader, 99).await;
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
    request_on_type_at(writer, reader, uri, id, 0, character, ch).await
}

async fn request_on_type_at(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    uri: &str,
    id: i64,
    line: u32,
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
                "position": {"line": line, "character": character},
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

fn apply_text_edits(text: &str, edits: &Value) -> String {
    let mut text = text.to_string();
    let mut edits: Vec<(usize, usize, String)> = edits
        .as_array()
        .expect("edit array")
        .iter()
        .map(|edit| {
            let range = &edit["range"];
            (
                offset_for_position(&text, &range["start"]),
                offset_for_position(&text, &range["end"]),
                edit["newText"].as_str().expect("newText").to_string(),
            )
        })
        .collect();
    edits.sort_by_key(|edit| std::cmp::Reverse(edit.0));
    for (start, end, new_text) in edits {
        text.replace_range(start..end, &new_text);
    }
    text
}

fn offset_for_position(text: &str, position: &Value) -> usize {
    let target_line = position["line"].as_u64().expect("line") as usize;
    let target_character =
        position["character"].as_u64().expect("character") as usize;
    let mut line_start = 0usize;
    for (line, segment) in text.split_inclusive('\n').enumerate() {
        if line == target_line {
            return line_start
                + utf16_offset(
                    segment.trim_end_matches('\n'),
                    target_character,
                );
        }
        line_start += segment.len();
    }
    if target_line == text.lines().count() {
        return text.len();
    }
    panic!("position line out of bounds: {position:?}");
}

fn utf16_offset(line: &str, target_units: usize) -> usize {
    let mut units = 0usize;
    for (byte_idx, ch) in line.char_indices() {
        if units == target_units {
            return byte_idx;
        }
        units += ch.len_utf16();
        assert!(units <= target_units, "position splits UTF-16 character");
    }
    assert_eq!(units, target_units, "position beyond line");
    line.len()
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
