//! JSON-RPC stdio coverage for accepted-completion `(` spacer rewrites.

use std::sync::Arc;

use sase_core::{
    HelperHostBridge, MobileMacroCatalogEntryWire, MobileMacroInputWire,
};
use sase_macro_lsp::MacroLspServer;
use serde_json::{json, Value};
use tower_lsp_server::{LspService, Server};

fn input(
    name: &str,
    r#type: &str,
    required: bool,
    position: u32,
) -> MobileMacroInputWire {
    MobileMacroInputWire {
        name: name.to_string(),
        r#type: r#type.to_string(),
        description: None,
        required,
        default_display: None,
        position,
        repeatable: false,
        choices: Vec::new(),
        named_type: None,
        value_role: None,
    }
}

fn entry(
    name: &str,
    insertion: &str,
    inputs: Vec<MobileMacroInputWire>,
) -> MobileMacroCatalogEntryWire {
    MobileMacroCatalogEntryWire {
        name: name.to_string(),
        display_label: name.to_string(),
        insertion: Some(insertion.to_string()),
        reference_prefix: Some("#".to_string()),
        kind: Some("prompt".to_string()),
        description: Some(format!("{name} prompt")),
        source_bucket: "builtin".to_string(),
        project: None,
        tags: Vec::new(),
        input_signature: None,
        inputs,
        is_skill: true,
        skill_name: Some(name.to_string()),
        memory_type: None,
        content_preview: None,
        source_path_display: Some("Cargo.toml".to_string()),
        definition_path: None,
        definition_range: None,
    }
}

struct SpacerBridge {
    inner: sase_core::StaticHelperHostBridge,
}

impl HelperHostBridge for SpacerBridge {
    fn agent_catalog(
        &self,
        request: &sase_core::AgentCatalogRequest,
    ) -> Result<sase_core::AgentCatalogResponse, sase_core::HostBridgeError>
    {
        self.inner.agent_catalog(request)
    }

    fn finalizer_catalog(
        &self,
        request: &sase_core::FinalizerCatalogRequest,
    ) -> Result<sase_core::FinalizerCatalogResponse, sase_core::HostBridgeError>
    {
        self.inner.finalizer_catalog(request)
    }

    fn macro_catalog(
        &self,
        request: &sase_core::MobileMacroCatalogRequestWire,
    ) -> Result<
        sase_core::MobileMacroCatalogResponseWire,
        sase_core::HostBridgeError,
    > {
        self.inner.macro_catalog(request)
    }

    fn snippet_catalog(
        &self,
        request: &sase_core::EditorSnippetCatalogRequestWire,
    ) -> Result<
        sase_core::EditorSnippetCatalogResponseWire,
        sase_core::HostBridgeError,
    > {
        self.inner.snippet_catalog(request)
    }
}

fn spacer_bridge() -> SpacerBridge {
    let entries = vec![
        entry(
            "optional",
            "#optional",
            vec![
                input("topic", "word", false, 0),
                input("count", "int", false, 1),
            ],
        ),
        entry("plain", "#plain", Vec::new()),
    ];
    let total = entries.len() as u64;
    let inner = sase_core::StaticHelperHostBridge {
        agent_catalog_response: serde_json::from_value(json!({
            "schema_version": 1,
            "status": "ok",
            "message": "",
            "entries": []
        }))
        .unwrap(),
        finalizer_catalog_response: serde_json::from_value(json!({
            "schema_version": 1,
            "status": "ok",
            "message": "",
            "entries": []
        }))
        .unwrap(),
        changespec_tags_response: serde_json::from_value(json!({
            "schema_version": 1,
            "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
            "context": {"project": "sase", "scope": "explicit"},
            "tags": [],
            "total_count": 0
        }))
        .unwrap(),
        macro_catalog_response: sase_core::MobileMacroCatalogResponseWire {
            schema_version: 1,
            result: sase_core::MobileHelperResultWire {
                status: sase_core::MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: sase_core::MobileHelperProjectContextWire {
                project: Some("sase".to_string()),
                scope: sase_core::MobileHelperProjectScopeWire::Explicit,
            },
            entries,
            stats: sase_core::MobileMacroCatalogStatsWire {
                total_count: total,
                project_count: 0,
                skill_count: total,
                memory_count: 0,
                pdf_requested: false,
            },
            catalog_attachment: None,
        },
        snippet_catalog_response: sase_core::EditorSnippetCatalogResponseWire {
            schema_version: 1,
            result: sase_core::MobileHelperResultWire {
                status: sase_core::MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: sase_core::MobileHelperProjectContextWire {
                project: Some("sase".to_string()),
                scope: sase_core::MobileHelperProjectScopeWire::Explicit,
            },
            entries: Vec::new(),
            stats: sase_core::EditorSnippetCatalogStatsWire {
                total_count: 0,
            },
        },
        vcs_repo_catalog_response: sase_core::VcsRepoCatalogResponse {
            schema_version: 1,
            status: "ok".to_string(),
            error_kind: None,
            message: String::new(),
            provider_display: "GitHub".to_string(),
            stale: false,
            entries: Vec::new(),
        },
        bead_list_response: serde_json::from_value(json!({
            "schema_version": 1,
            "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
            "context": {"project": "sase", "scope": "explicit"},
            "beads": [],
            "total_count": 0
        }))
        .unwrap(),
        bead_show_response: serde_json::from_value(json!({
            "schema_version": 1,
            "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
            "context": {"project": "sase", "scope": "explicit"},
            "bead": {
                "summary": {"id": "sase-1", "title": "Example", "status": "open", "bead_type": "phase", "tier": null, "project": "sase", "parent_id": null, "assignee": null, "updated_at": null, "dependency_count": 0, "block_count": 0, "child_count": 0, "plan_path_display": null, "changespec_name": null, "changespec_status": null},
                "description": null, "notes": null, "design_path_display": null, "dependencies": [], "blocks": [], "children": [], "workspace_display": null
            }
        }))
        .unwrap(),
        update_start_response: serde_json::from_value(json!({
            "schema_version": 1,
            "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
            "job": {"job_id": "job", "status": "running", "started_at": null, "finished_at": null, "message": null, "log_path_display": null, "completion_path_display": null}
        }))
        .unwrap(),
        update_status_response: serde_json::from_value(json!({
            "schema_version": 1,
            "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
            "job": {"job_id": "job", "status": "succeeded", "started_at": null, "finished_at": null, "message": null, "log_path_display": null, "completion_path_display": null}
        }))
        .unwrap(),
    };
    SpacerBridge { inner }
}

#[tokio::test]
async fn stdio_jsonrpc_spacer_paren_accept_format_and_complete() {
    let (mut client_writer, server_stdin) = tokio::io::duplex(16384);
    let (server_stdout, mut client_reader) = tokio::io::duplex(16384);
    let (service, socket) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(spacer_bridge()))
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
                "capabilities": {
                    "textDocument": {
                        "completion": {
                            "completionItem": {"snippetSupport": true}
                        }
                    }
                }
            }
        }),
    )
    .await;
    let initialized = read_response(&mut client_reader, 1).await;
    assert_eq!(
        initialized["capabilities"]["documentOnTypeFormattingProvider"],
        json!({"firstTriggerCharacter": "("})
    );
    let commands = initialized["capabilities"]["executeCommandProvider"]
        ["commands"]
        .clone();
    assert!(commands
        .as_array()
        .unwrap()
        .iter()
        .any(|command| { command == "sase.macroLsp.acceptCompletion" }));

    let uri = "file:///tmp/sase_prompt_spacer_paren.md";
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;
    did_open(&mut client_writer, uri, "#o").await;

    // Completion for `#o` must attach a server-owned command to the eligible
    // `#optional` item but not to zero-input `#plain` (which does not match
    // this prefix, so request `#` to see both).
    did_change(&mut client_writer, uri, 2, "#").await;
    let items =
        request_completion(&mut client_writer, &mut client_reader, uri, 10, 1)
            .await;
    let array = items.as_array().unwrap();
    let optional = array
        .iter()
        .find(|item| item["label"] == "#optional")
        .expect("optional");
    let command = optional.get("command").expect("eligible needs a command");
    assert_eq!(command["command"], "sase.macroLsp.acceptCompletion");
    let plain = array
        .iter()
        .find(|item| item["label"] == "#plain")
        .expect("plain");
    assert!(
        plain.get("command").is_none(),
        "zero-input must not carry a command"
    );

    // Accept `#optional` from the `#o` prefix to keep the flow focused.
    did_change(&mut client_writer, uri, 3, "#o").await;
    let narrow =
        request_completion(&mut client_writer, &mut client_reader, uri, 11, 2)
            .await;
    let narrow_array = narrow.as_array().unwrap();
    let narrow_item = narrow_array
        .iter()
        .find(|item| item["label"] == "#optional")
        .expect("narrow");
    let narrow_command =
        narrow_item.get("command").expect("narrow command").clone();
    let narrow_edit = narrow_item["textEdit"].clone();
    assert_eq!(narrow_edit["newText"], "#optional ");
    // Client applies the completion edit.
    did_change(&mut client_writer, uri, 4, "#optional ").await;
    // Standard command runs after insertion.
    execute_command(
        &mut client_writer,
        &mut client_reader,
        uri,
        12,
        &narrow_command,
    )
    .await;

    // `(` completion while still `#optional (` comes from the normalized view
    // with actual coordinates plus a spacer deletion.
    did_change(&mut client_writer, uri, 5, "#optional (").await;
    let transition = request_paren_completion(
        &mut client_writer,
        &mut client_reader,
        uri,
        13,
    )
    .await;
    let transition_items = transition.as_array().unwrap();
    let topic = transition_items
        .iter()
        .find(|item| item["label"] == "topic=")
        .expect("topic=");
    let additional = topic
        .get("additionalTextEdits")
        .expect("spacer deletion")
        .as_array()
        .unwrap();
    assert!(additional.iter().any(|edit| {
        edit["range"]["start"] == json!({"line": 0, "character": 9})
            && edit["range"]["end"] == json!({"line": 0, "character": 10})
            && edit["newText"] == ""
    }));

    // `(` formatting deletes only the owned spacer, preserving the opener.
    let formatting = request_on_type(
        &mut client_writer,
        &mut client_reader,
        uri,
        14,
        11,
        "(",
    )
    .await;
    assert_eq!(
        formatting,
        json!([{
            "range": {"start": {"line": 0, "character": 9}, "end": {"line": 0, "character": 10}},
            "newText": ""
        }])
    );
    // Client applies the deletion.
    did_change(&mut client_writer, uri, 6, "#optional(").await;

    // After the deletion, ordinary argument completion runs with no extra deletion.
    let after = request_paren_completion_at(
        &mut client_writer,
        &mut client_reader,
        uri,
        15,
        10,
    )
    .await;
    let after_items = after.as_array().unwrap();
    assert!(after_items.iter().any(|item| item["label"] == "topic="));
    for item in after_items {
        for edit in item
            .get("additionalTextEdits")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
        {
            assert_ne!(
                edit["newText"], "",
                "no spacer deletion after normalize"
            );
        }
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
                "position": {"line": 0, "character": character}
            }
        }),
    )
    .await;
    read_response(reader, id).await
}

async fn request_paren_completion(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    uri: &str,
    id: i64,
) -> Value {
    request_paren_completion_at(writer, reader, uri, id, 11).await
}

async fn request_paren_completion_at(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    uri: &str,
    id: i64,
    character: u32,
) -> Value {
    // Caret after the opener in `#optional (` (or `#optional(` once normalized).
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

async fn execute_command(
    writer: &mut tokio::io::DuplexStream,
    reader: &mut tokio::io::DuplexStream,
    _uri: &str,
    id: i64,
    command: &Value,
) -> Value {
    write_message(
        writer,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "workspace/executeCommand",
            "params": {
                "command": command["command"],
                "arguments": command["arguments"]
            }
        }),
    )
    .await;
    read_response(reader, id).await
}

async fn write_message(writer: &mut tokio::io::DuplexStream, value: Value) {
    use tokio::io::AsyncWriteExt;
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
    use tokio::io::AsyncReadExt;
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
    let length: usize = header
        .lines()
        .find(|line| line.to_lowercase().starts_with("content-length:"))
        .and_then(|line| line.split(':').nth(1))
        .map(|value| value.trim().parse().unwrap())
        .unwrap();
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}
