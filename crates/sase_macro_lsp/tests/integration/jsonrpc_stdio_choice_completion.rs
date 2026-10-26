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
struct ChoiceBridge;

impl HelperHostBridge for ChoiceBridge {
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
                name: "deploy".to_string(),
                display_label: "deploy".to_string(),
                insertion: Some("#deploy".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("prompt".to_string()),
                description: Some("Deploy".to_string()),
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: Some("(env: staging|prod)".to_string()),
                inputs: vec![MobileMacroInputWire {
                    name: "env".to_string(),
                    r#type: "enum".to_string(),
                    description: None,
                    required: true,
                    default_display: None,
                    position: 0,
                    repeatable: false,
                    choices: vec![
                        MobileInputChoiceWire {
                            value: "staging".to_string(),
                            label: Some("Staging".to_string()),
                            description: Some("Pre-prod cluster".to_string()),
                        },
                        MobileInputChoiceWire {
                            value: "prod".to_string(),
                            label: Some("Production".to_string()),
                            description: Some("Customer traffic".to_string()),
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

fn completion_items(result: &Value) -> &[Value] {
    result
        .as_array()
        .or_else(|| result["items"].as_array())
        .map(Vec::as_slice)
        .unwrap_or_else(|| panic!("unexpected completion result: {result}"))
}

#[tokio::test]
async fn stdio_jsonrpc_choice_and_frontmatter_type_completion() {
    let (mut client_writer, server_stdin) = duplex(8192);
    let (server_stdout, mut client_reader) = duplex(8192);
    let (service, socket) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(ChoiceBridge))
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
    let initialize = read_message(&mut client_reader).await;
    assert_eq!(initialize.get("id").and_then(Value::as_i64), Some(1));
    assert_eq!(
        initialize["result"]["capabilities"]["completionProvider"]
            ["triggerCharacters"],
        json!([
            "#", "!", "/", "%", ".", "@", ":", "(", ",", "+", "<", "=", "{",
            "|"
        ])
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
                    "uri": "file:///tmp/sase_prompt_choice.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "#deploy(env="
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
                "textDocument": {"uri": "file:///tmp/sase_prompt_choice.md"},
                "position": {"line": 0, "character": 12}
            }
        }),
    )
    .await;
    let result = read_response_result(&mut client_reader, 2).await;
    assert_eq!(result["isIncomplete"], json!(false));
    let items = completion_items(&result);
    let labels: Vec<&str> = items
        .iter()
        .filter_map(|item| item["label"].as_str())
        .collect();
    assert_eq!(labels, vec!["staging", "prod"]);
    assert_eq!(items[0]["kind"], json!(20)); // ENUM_MEMBER
    assert_eq!(items[0]["filterText"], json!("staging"));
    assert_eq!(items[0]["sortText"], json!("0000"));
    assert_eq!(items[0]["labelDetails"]["description"], json!("Staging"));
    assert_eq!(items[0]["textEdit"]["newText"], json!("staging"));
    assert_eq!(
        items[0]["textEdit"]["range"]["start"]["character"],
        json!(12)
    );
    assert_eq!(items[0]["textEdit"]["range"]["end"]["character"], json!(12));
    assert!(items[0]["documentation"]["value"]
        .as_str()
        .unwrap()
        .contains("Pre-prod cluster"));

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {
                    "uri": "file:///tmp/sase_prompt_choice.md",
                    "version": 2
                },
                "contentChanges": [{"text": "#deploy(env=staging"}]
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
            "id": 3,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_choice.md"},
                "position": {"line": 0, "character": 14}
            }
        }),
    )
    .await;
    let mid = read_response_result(&mut client_reader, 3).await;
    let mid_items = completion_items(&mid);
    let staging = mid_items
        .iter()
        .find(|item| item["label"] == "staging")
        .expect("staging");
    assert_eq!(staging["textEdit"]["newText"], json!("staging"));
    assert_eq!(
        staging["textEdit"]["range"]["start"]["character"],
        json!(12)
    );
    assert_eq!(staging["textEdit"]["range"]["end"]["character"], json!(19));

    let frontmatter = "---\ninput:\n  env: \n---\n";
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {
                    "uri": "file:///tmp/sase_prompt_choice.md",
                    "version": 3
                },
                "contentChanges": [{"text": frontmatter}]
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
            "id": 4,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_choice.md"},
                "position": {"line": 2, "character": 7}
            }
        }),
    )
    .await;
    let types = read_response_result(&mut client_reader, 4).await;
    let type_items = completion_items(&types);
    let type_labels: Vec<&str> = type_items
        .iter()
        .filter_map(|item| item["label"].as_str())
        .collect();
    assert!(type_labels.contains(&"word"));
    assert!(type_labels.contains(&"enum"));
    assert!(!type_labels.contains(&"string"));
    let word = type_items
        .iter()
        .find(|item| item["label"] == "word")
        .unwrap();
    assert_eq!(word["kind"], json!(20));
    assert_eq!(word["textEdit"]["newText"], json!("word"));

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 5, "method": "shutdown", "params": null}),
    )
    .await;
    let _ = read_response_result(&mut client_reader, 5).await;
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit"}),
    )
    .await;
    server_task.await.unwrap();
}
