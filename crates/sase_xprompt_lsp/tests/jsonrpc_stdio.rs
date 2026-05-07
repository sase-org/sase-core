use std::{fs, sync::Arc};

use lsp_types::Uri;
use sase_core::{
    HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
    MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
    MobileXpromptCatalogStatsWire,
};
use sase_xprompt_lsp::XpromptLspServer;
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tower_lsp_server::UriExt;
use tower_lsp_server::{LspService, Server};

#[derive(Debug)]
struct FixtureBridge {
    definition_path: String,
}

impl HelperHostBridge for FixtureBridge {
    fn xprompt_catalog(
        &self,
        _request: &MobileXpromptCatalogRequestWire,
    ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
        Ok(MobileXpromptCatalogResponseWire {
            schema_version: 1,
            result: MobileHelperResultWire {
                status: MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: MobileHelperProjectContextWire {
                project: Some("sase".to_string()),
                scope: MobileHelperProjectScopeWire::Explicit,
            },
            entries: vec![MobileXpromptCatalogEntryWire {
                name: "foo".to_string(),
                display_label: "foo".to_string(),
                insertion: Some("#foo".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("prompt".to_string()),
                description: Some("Foo prompt".to_string()),
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: Vec::new(),
                is_skill: false,
                content_preview: None,
                source_path_display: None,
                definition_path: Some(self.definition_path.clone()),
                definition_range: None,
            }],
            stats: MobileXpromptCatalogStatsWire {
                total_count: 1,
                project_count: 0,
                skill_count: 0,
                pdf_requested: false,
            },
            catalog_attachment: None,
        })
    }
}

#[tokio::test]
async fn stdio_jsonrpc_initialize_and_completion() {
    let temp = tempfile::tempdir().unwrap();
    let definition_path = temp.path().join("foo.md");
    fs::write(&definition_path, "foo").unwrap();

    let (mut client_writer, server_stdin) = duplex(8192);
    let (server_stdout, mut client_reader) = duplex(8192);
    let (service, socket) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(FixtureBridge {
                definition_path: definition_path.to_string_lossy().into_owned(),
            }),
        )
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
    let initialize_response = read_message(&mut client_reader).await;
    assert_eq!(
        initialize_response.get("id").and_then(Value::as_i64),
        Some(1)
    );

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
                    "uri": "file:///tmp/prompt.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "#foo"
                }
            }
        }),
    )
    .await;
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/prompt.md"},
                "position": {"line": 0, "character": 3}
            }
        }),
    )
    .await;

    let mut saw_completion = false;
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(2) {
            let items = message["result"]
                .as_array()
                .or_else(|| message["result"]["items"].as_array())
                .unwrap_or_else(|| {
                    panic!("unexpected completion response: {message}")
                });
            saw_completion = items.iter().any(|item| item["label"] == "#foo");
            break;
        }
    }

    assert!(saw_completion);
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "textDocument/definition",
            "params": {
                "textDocument": {"uri": "file:///tmp/prompt.md"},
                "position": {"line": 0, "character": 2}
            }
        }),
    )
    .await;

    let mut saw_definition = false;
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(3) {
            let result = &message["result"];
            saw_definition = result["uri"]
                == serde_json::Value::String(
                    Uri::from_file_path(&definition_path).unwrap().to_string(),
                )
                && result["range"]["start"]["line"] == 0
                && result["range"]["start"]["character"] == 0
                && result["range"]["end"]["line"] == 0
                && result["range"]["end"]["character"] == 0;
            break;
        }
    }

    assert!(saw_definition);
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
