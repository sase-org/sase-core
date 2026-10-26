use std::sync::Arc;

use sase_core::{
    AgentCatalogRequest, AgentCatalogResponse, EditorSnippetCatalogRequestWire,
    EditorSnippetCatalogResponseWire, EditorSnippetCatalogStatsWire,
    HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperStatusWire, MobileInputChoiceWire, MobileMacroCatalogEntryWire,
    MobileMacroCatalogRequestWire, MobileMacroCatalogResponseWire,
    MobileMacroCatalogStatsWire, MobileMacroInputWire,
};
use sase_macro_lsp::MacroLspServer;
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tower_lsp_server::{LspService, Server};

#[derive(Debug)]
struct ChoiceDiagBridge;

impl HelperHostBridge for ChoiceDiagBridge {
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
                project: Some("sase".to_string()),
                scope: MobileHelperProjectScopeWire::Explicit,
            },
            entries: vec![MobileMacroCatalogEntryWire {
                name: "choose".to_string(),
                display_label: "choose".to_string(),
                insertion: Some("#choose".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("prompt".to_string()),
                description: Some("Choose".to_string()),
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: Some("(edition: brief|full)".to_string()),
                inputs: vec![MobileMacroInputWire {
                    name: "edition".to_string(),
                    r#type: "enum".to_string(),
                    description: None,
                    required: true,
                    default_display: Some("brief".to_string()),
                    position: 0,
                    repeatable: false,
                    choices: vec![
                        MobileInputChoiceWire {
                            value: "brief".to_string(),
                            label: Some("Brief".to_string()),
                            description: Some("Short edition".to_string()),
                        },
                        MobileInputChoiceWire {
                            value: "full".to_string(),
                            label: Some("Full".to_string()),
                            description: Some("Complete edition".to_string()),
                        },
                    ],
                    named_type: None,
                    value_role: None,
                }],
                is_skill: false,
                skill_name: None,
                memory_type: None,
                content_preview: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
            }],
            stats: MobileMacroCatalogStatsWire {
                total_count: 1,
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

async fn read_diagnostics(reader: &mut tokio::io::DuplexStream) -> Vec<Value> {
    for _ in 0..8 {
        let message = read_message(reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            return message["params"]["diagnostics"]
                .as_array()
                .cloned()
                .unwrap_or_default();
        }
    }
    panic!("missing publishDiagnostics");
}

#[tokio::test]
async fn stdio_jsonrpc_choice_diagnostic_quickfix_and_hover() {
    let (mut client_writer, server_stdin) = duplex(8192);
    let (server_stdout, mut client_reader) = duplex(8192);
    let (service, socket) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(ChoiceDiagBridge))
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
    let _initialize = read_message(&mut client_reader).await;
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;

    let uri = "file:///tmp/sase_prompt_choice.md";
    let text = "#choose(edition=breif)";
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
                    "text": text
                }
            }
        }),
    )
    .await;
    let diagnostics = read_diagnostics(&mut client_reader).await;
    let choice = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["code"] == "invalid_macro_arg_choice")
        .expect("choice diagnostic");
    assert_eq!(choice["severity"], json!(1)); // Error
    assert_eq!(choice["data"]["suggestions"][0]["value"], json!("brief"));
    assert_eq!(
        choice["data"]["suggestions"][0]["title"],
        json!("Replace with `brief`")
    );
    assert_eq!(choice["data"]["suggestions"][0]["preferred"], json!(true));
    assert_eq!(
        choice["data"]["suggestions"][0]["edit"]["new_text"],
        json!("brief")
    );

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/codeAction",
            "params": {
                "textDocument": {"uri": uri},
                "range": choice["range"],
                "context": {
                    "diagnostics": [choice],
                    "only": ["quickfix"]
                }
            }
        }),
    )
    .await;
    let actions = read_response_result(&mut client_reader, 2).await;
    let preferred = actions
        .as_array()
        .expect("actions")
        .iter()
        .find(|action| {
            action["title"] == "Replace with `brief`"
                && action["isPreferred"] == true
        })
        .expect("preferred replace action");
    let new_text =
        &preferred["edit"]["documentChanges"][0]["edits"][0]["newText"];
    assert_eq!(new_text, "brief");

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": 2},
                "contentChanges": [{"text": "#choose(edition=brief)"}]
            }
        }),
    )
    .await;
    let cleaned = read_diagnostics(&mut client_reader).await;
    assert!(
        cleaned
            .iter()
            .all(|diagnostic| diagnostic["code"] != "invalid_macro_arg_choice"),
        "{cleaned:?}"
    );

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "textDocument/hover",
            "params": {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 18}
            }
        }),
    )
    .await;
    let hover = read_response_result(&mut client_reader, 3).await;
    let markdown = hover["contents"]["value"].as_str().unwrap();
    assert!(markdown.contains("Source: builtin"), "{markdown}");
    assert!(markdown.contains("Default: `brief`"), "{markdown}");
    assert!(
        markdown.contains("| Value | Label | Description |"),
        "{markdown}"
    );
    assert!(markdown.contains("Short edition"), "{markdown}");

    let frontmatter = "---\ninput:\n  mode: string\n---\n";
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": 3},
                "contentChanges": [{"text": frontmatter}]
            }
        }),
    )
    .await;
    let frontmatter_diagnostics = read_diagnostics(&mut client_reader).await;
    let deprecated = frontmatter_diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic["code"] == "deprecated_macro_frontmatter_input_type"
        })
        .expect("string deprecation");
    assert_eq!(
        deprecated["data"]["suggestions"][0]["title"],
        json!("Use `line`")
    );
    assert_eq!(
        deprecated["data"]["suggestions"][0]["edit"]["new_text"],
        json!("line")
    );

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 99, "method": "shutdown", "params": null}),
    )
    .await;
    let _ = read_response_result(&mut client_reader, 99).await;
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();
}
