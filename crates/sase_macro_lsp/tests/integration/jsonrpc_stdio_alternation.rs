use std::{fs, sync::Arc};

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
struct FixtureBridge;

impl HelperHostBridge for FixtureBridge {
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

#[tokio::test]
async fn stdio_jsonrpc_alternation_tokens_and_unclosed_diagnostic() {
    let temp = tempfile::tempdir().unwrap();
    let definition_path = temp.path().join("foo.md");
    fs::write(&definition_path, "foo").unwrap();

    let (mut client_writer, server_stdin) = duplex(8192);
    let (server_stdout, mut client_reader) = duplex(8192);
    let (service, socket) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(FixtureBridge))
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
    read_response_result(&mut client_reader, 1).await;

    let uri = "file:///tmp/sase_prompt_alternation.md";
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
                    "text": "foo%{bar | baz}qux"
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
            "method": "textDocument/semanticTokens/full",
            "params": {"textDocument": {"uri": uri}}
        }),
    )
    .await;
    let semantic_result = read_response_result(&mut client_reader, 2).await;
    let absolute =
        absolute_semantic_tokens_from_value(&semantic_result["data"]);
    // Operator is token type 7; the alternation modifier follows the
    // accent block (bit 23) and separator follows it (bit 24).
    const OPERATOR: u32 = 7;
    const ALTERNATION: u32 = 1 << 23;
    const SEPARATOR: u32 = 1 << 24;
    assert!(
        absolute.contains(&(0, 3, 2, OPERATOR, ALTERNATION)),
        "{absolute:?}"
    );
    assert!(
        absolute.contains(&(0, 9, 1, OPERATOR, ALTERNATION | SEPARATOR)),
        "{absolute:?}"
    );
    assert!(
        absolute.contains(&(0, 14, 1, OPERATOR, ALTERNATION)),
        "{absolute:?}"
    );
    assert_no_semantic_token_overlaps(&absolute);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {
                    "uri": uri,
                    "version": 2
                },
                "contentChanges": [{"text": "foo%{bar"}]
            }
        }),
    )
    .await;

    let mut saw_unclosed_diagnostic = false;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            != Some("textDocument/publishDiagnostics")
        {
            continue;
        }
        saw_unclosed_diagnostic = message["params"]["diagnostics"]
            .as_array()
            .is_some_and(|diagnostics| {
                diagnostics.iter().any(|diagnostic| {
                    diagnostic["source"] == "sase-macro"
                        && diagnostic["severity"] == 1
                        && diagnostic["code"] == "unclosed_alternation"
                        && diagnostic["message"]
                            == "unclosed %{ directive: missing closing '}'"
                        && diagnostic["range"]["start"]
                            == json!({"line": 0, "character": 3})
                        && diagnostic["range"]["end"]
                            == json!({"line": 0, "character": 5})
                })
            });
        if saw_unclosed_diagnostic {
            break;
        }
    }
    assert!(saw_unclosed_diagnostic);

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 3, "method": "shutdown", "params": null}),
    )
    .await;
    read_response_result(&mut client_reader, 3).await;
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

fn absolute_semantic_tokens_from_value(
    data: &Value,
) -> Vec<(u32, u32, u32, u32, u32)> {
    let chunks = data.as_array().expect("semantic token data array");
    assert_eq!(chunks.len() % 5, 0, "semantic token chunk length");
    let mut line = 0u32;
    let mut start = 0u32;
    chunks
        .chunks(5)
        .map(|chunk| {
            let delta_line = chunk[0].as_u64().unwrap() as u32;
            let delta_start = chunk[1].as_u64().unwrap() as u32;
            line += delta_line;
            if delta_line == 0 {
                start += delta_start;
            } else {
                start = delta_start;
            }
            (
                line,
                start,
                chunk[2].as_u64().unwrap() as u32,
                chunk[3].as_u64().unwrap() as u32,
                chunk[4].as_u64().unwrap() as u32,
            )
        })
        .collect()
}

fn assert_no_semantic_token_overlaps(tokens: &[(u32, u32, u32, u32, u32)]) {
    for pair in tokens.windows(2) {
        let left = pair[0];
        let right = pair[1];
        if left.0 == right.0 {
            assert!(
                left.1 + left.2 <= right.1,
                "overlapping semantic tokens: {tokens:?}"
            );
        }
    }
}
