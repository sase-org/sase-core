use std::sync::Arc;

use sase_core::{
    AgentCatalogRequest, AgentCatalogResponse, EditorSnippetCatalogRequestWire,
    EditorSnippetCatalogResponseWire, EditorSnippetCatalogStatsWire,
    HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperStatusWire, MobileMacroCatalogRequestWire,
    MobileMacroCatalogResponseWire, MobileMacroCatalogStatsWire,
};
use sase_macro_lsp::MacroLspServer;
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tower_lsp_server::{LspService, Server};

#[derive(Debug)]
struct EmptyBridge;

impl HelperHostBridge for EmptyBridge {
    fn agent_catalog(
        &self,
        _request: &AgentCatalogRequest,
    ) -> Result<AgentCatalogResponse, HostBridgeError> {
        Ok(AgentCatalogResponse {
            schema_version: 1,
            status: "ok".to_string(),
            message: String::new(),
            entries: Vec::new(),
            beads: Vec::new(),
        })
    }

    fn macro_catalog(
        &self,
        _request: &MobileMacroCatalogRequestWire,
    ) -> Result<MobileMacroCatalogResponseWire, HostBridgeError> {
        Ok(MobileMacroCatalogResponseWire {
            schema_version: 1,
            result: MobileHelperResultWire {
                status: MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: MobileHelperProjectContextWire {
                project: None,
                scope: MobileHelperProjectScopeWire::Explicit,
            },
            entries: Vec::new(),
            stats: MobileMacroCatalogStatsWire {
                total_count: 0,
                project_count: 0,
                skill_count: 0,
                memory_count: 0,
                pdf_requested: false,
            },
            catalog_attachment: None,
        })
    }

    fn snippet_catalog(
        &self,
        _request: &EditorSnippetCatalogRequestWire,
    ) -> Result<EditorSnippetCatalogResponseWire, HostBridgeError> {
        Ok(EditorSnippetCatalogResponseWire {
            schema_version: 1,
            result: MobileHelperResultWire {
                status: MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: MobileHelperProjectContextWire {
                project: None,
                scope: MobileHelperProjectScopeWire::Explicit,
            },
            entries: Vec::new(),
            stats: EditorSnippetCatalogStatsWire { total_count: 0 },
        })
    }
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

async fn read_response_result(
    reader: &mut tokio::io::DuplexStream,
    id: i64,
) -> Value {
    for _ in 0..16 {
        let message = read_message(reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(id) {
            return message["result"].clone();
        }
    }
    panic!("missing response id {id}");
}

#[tokio::test]
async fn stdio_jsonrpc_jinja_completion_and_silent_brace_trigger() {
    let (mut client_writer, server_stdin) = duplex(8192);
    let (server_stdout, mut client_reader) = duplex(8192);
    let (service, socket) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(EmptyBridge))
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
    // The server advertises the Jinja `{` and `|` triggers.
    let triggers = initialize_response["result"]["capabilities"]
        ["completionProvider"]["triggerCharacters"]
        .as_array()
        .expect("trigger characters");
    assert!(triggers.contains(&json!("{")));
    assert!(triggers.contains(&json!("|")));

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
                    "uri": "file:///tmp/sase_prompt_jinja.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "{{  }}"
                }
            }
        }),
    )
    .await;
    // Drain the publishDiagnostics notification.
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            break;
        }
    }

    // Complete inside the tag: the Jinja engine answers, not the
    // placeholder or snippet surfaces.
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_jinja.md"},
                "position": {"line": 0, "character": 3}
            }
        }),
    )
    .await;
    let result = read_response_result(&mut client_reader, 2).await;
    let items = result
        .as_array()
        .or_else(|| result["items"].as_array())
        .expect("completion items");
    assert!(
        items.iter().any(|item| item["label"] == "root"),
        "{items:?}"
    );

    // A `{` trigger outside any tag stays silent (null).
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": "file:///tmp/sase_prompt_plain.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "plain { text"
                }
            }
        }),
    )
    .await;
    for _ in 0..4 {
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
            "id": 3,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_plain.md"},
                "position": {"line": 0, "character": 7},
                "context": {
                    "triggerKind": 2,
                    "triggerCharacter": "{"
                }
            }
        }),
    )
    .await;
    let silent = read_response_result(&mut client_reader, 3).await;
    assert!(silent.is_null(), "{silent:?}");

    client_writer.shutdown().await.unwrap();
    server_task.abort();
}
