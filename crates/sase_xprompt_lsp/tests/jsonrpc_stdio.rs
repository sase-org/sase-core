use std::{fs, sync::Arc};

use lsp_types::Uri;
use sase_core::{
    AgentCatalogRequest, AgentCatalogResponse, AgentCompletionEntry,
    EditorSnippetCatalogRequestWire, EditorSnippetCatalogResponseWire,
    EditorSnippetCatalogStatsWire, EditorSnippetEntryWire, HelperHostBridge,
    HostBridgeError, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
    MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
    MobileXpromptCatalogStatsWire, MobileXpromptInputWire,
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
    fn agent_catalog(
        &self,
        _request: &AgentCatalogRequest,
    ) -> Result<AgentCatalogResponse, HostBridgeError> {
        Ok(AgentCatalogResponse {
            schema_version: 1,
            status: "ok".to_string(),
            message: String::new(),
            entries: vec![
                AgentCompletionEntry {
                    name: "planner".to_string(),
                    status: "RUNNING".to_string(),
                    project: "sase".to_string(),
                    kind: String::new(),
                    member_count: 0,
                    detail: String::new(),
                },
                AgentCompletionEntry {
                    name: "coder".to_string(),
                    status: "DONE".to_string(),
                    project: "sase-core".to_string(),
                    kind: String::new(),
                    member_count: 0,
                    detail: String::new(),
                },
            ],
        })
    }

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
            entries: vec![
                MobileXpromptCatalogEntryWire {
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
                    inputs: vec![MobileXpromptInputWire {
                        name: "path".to_string(),
                        r#type: "path".to_string(),
                        description: Some("File to process".to_string()),
                        required: true,
                        default_display: None,
                        position: 0,
                        repeatable: false,
                    }],
                    is_skill: false,
                    content_preview: None,
                    source_path_display: None,
                    definition_path: Some(self.definition_path.clone()),
                    definition_range: None,
                },
                MobileXpromptCatalogEntryWire {
                    name: "fork".to_string(),
                    display_label: "fork".to_string(),
                    insertion: Some("#fork".to_string()),
                    reference_prefix: Some("#".to_string()),
                    kind: Some("workflow".to_string()),
                    description: Some("Fork conversations".to_string()),
                    source_bucket: "builtin".to_string(),
                    project: None,
                    tags: Vec::new(),
                    input_signature: Some("(names…?: agent)".to_string()),
                    inputs: vec![MobileXpromptInputWire {
                        name: "names".to_string(),
                        r#type: "agent".to_string(),
                        description: None,
                        required: false,
                        default_display: None,
                        position: 0,
                        repeatable: true,
                    }],
                    is_skill: false,
                    content_preview: None,
                    source_path_display: None,
                    definition_path: None,
                    definition_range: None,
                },
            ],
            stats: MobileXpromptCatalogStatsWire {
                total_count: 2,
                project_count: 0,
                skill_count: 0,
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
                project: Some("sase".to_string()),
                scope: MobileHelperProjectScopeWire::Explicit,
            },
            entries: vec![EditorSnippetEntryWire {
                trigger: "foo".to_string(),
                template: "body $1$0".to_string(),
                source: "ace.snippets".to_string(),
                xprompt_name: None,
                description: Some("Foo snippet".to_string()),
                source_path_display: Some("ace.snippets".to_string()),
            }],
            stats: EditorSnippetCatalogStatsWire { total_count: 1 },
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
                    "uri": "file:///tmp/sase_prompt_rpc.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "#foo"
                }
            }
        }),
    )
    .await;

    let mut saw_missing_arg_diagnostic = false;
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            saw_missing_arg_diagnostic = message["params"]["diagnostics"]
                .as_array()
                .is_some_and(|diagnostics| {
                    diagnostics.iter().any(|diagnostic| {
                        diagnostic["source"] == "sase-xprompt"
                            && diagnostic["severity"] == 1
                            && diagnostic["code"] == "missing_required_arg"
                    })
                });
            if saw_missing_arg_diagnostic {
                break;
            }
        }
    }
    assert!(saw_missing_arg_diagnostic);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {
                    "uri": "file:///tmp/sase_prompt_rpc.md",
                    "version": 2
                },
                "contentChanges": [{"text": "#fork:planner,co"}]
            }
        }),
    )
    .await;
    let mut saw_repeatable_diagnostics = false;
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            let diagnostics = message["params"]["diagnostics"]
                .as_array()
                .expect("diagnostic array");
            saw_repeatable_diagnostics = diagnostics.iter().all(|diagnostic| {
                diagnostic["code"] != "too_many_args"
                    && diagnostic["code"] != "invalid_xprompt_arg_type"
            });
            if saw_repeatable_diagnostics {
                break;
            }
        }
    }
    assert!(saw_repeatable_diagnostics);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_rpc.md"},
                "position": {"line": 0, "character": 16}
            }
        }),
    )
    .await;
    let mut saw_agent_completion = false;
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(4) {
            let items = message["result"]
                .as_array()
                .or_else(|| message["result"]["items"].as_array())
                .unwrap_or_else(|| {
                    panic!("unexpected agent completion response: {message}")
                });
            assert_eq!(items.len(), 1);
            assert_eq!(items[0]["label"], "coder");
            assert_eq!(items[0]["detail"], "DONE · sase-core");
            assert_eq!(
                items[0]["textEdit"]["range"],
                json!({
                    "start": {"line": 0, "character": 14},
                    "end": {"line": 0, "character": 16}
                })
            );
            assert_eq!(items[0]["textEdit"]["newText"], "coder");
            saw_agent_completion = true;
            break;
        }
    }
    assert!(saw_agent_completion);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {
                    "uri": "file:///tmp/sase_prompt_rpc.md",
                    "version": 3
                },
                "contentChanges": [{"text": "#foo"}]
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
            "id": 2,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_rpc.md"},
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
                "textDocument": {"uri": "file:///tmp/sase_prompt_rpc.md"},
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

#[tokio::test]
async fn stdio_jsonrpc_unsupported_markdown_has_no_xprompt_behavior() {
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
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(1)
    {}

    let unsupported_uri =
        "file:///tmp/project/sdd/research/202605/memory_system_prior_art.md";
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
                    "uri": unsupported_uri,
                    "languageId": "markdown",
                    "version": 1,
                    "text": "#foo"
                }
            }
        }),
    )
    .await;

    let mut saw_empty_diagnostics = false;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
            && message["params"]["uri"] == unsupported_uri
        {
            saw_empty_diagnostics = message["params"]["diagnostics"]
                .as_array()
                .is_some_and(|diagnostics| diagnostics.is_empty());
            break;
        }
    }
    assert!(saw_empty_diagnostics);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": unsupported_uri},
                "position": {"line": 0, "character": 3}
            }
        }),
    )
    .await;

    let mut saw_no_completion = false;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(2) {
            saw_no_completion = message["result"].is_null();
            break;
        }
    }
    assert!(saw_no_completion);

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 3, "method": "shutdown", "params": null}),
    )
    .await;
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(3)
    {}
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();
}

#[tokio::test]
async fn stdio_jsonrpc_frontmatter_diagnostics() {
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
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(1)
    {}

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
                    "uri": "file:///tmp/sase/xprompts/bad_pick_plan_xprompt.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "---\nowner: me\ninput:\n  target: wordd\nsnippet: bad-trigger!\nkeywords: [topic]\nskill: true\n---\nBody"
                }
            }
        }),
    )
    .await;

    let mut saw_frontmatter_diagnostic = false;
    for _ in 0..4 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            saw_frontmatter_diagnostic = message["params"]["diagnostics"]
                .as_array()
                .is_some_and(|diagnostics| {
                    let expected_codes = [
                        "unknown_xprompt_frontmatter_field",
                        "invalid_xprompt_frontmatter_input_type",
                        "invalid_xprompt_frontmatter_snippet_trigger",
                        "missing_xprompt_memory_tag",
                        "missing_xprompt_skill_description",
                    ];
                    expected_codes.iter().all(|code| {
                        diagnostics.iter().any(|diagnostic| {
                            diagnostic["source"] == "sase-xprompt"
                                && diagnostic["code"] == *code
                        })
                    }) && diagnostics.iter().any(|diagnostic| {
                        diagnostic["source"] == "sase-xprompt"
                            && diagnostic["severity"] == 1
                            && diagnostic["code"]
                                == "invalid_xprompt_frontmatter_input_type"
                            && diagnostic["range"]["start"]["line"] == 3
                            && diagnostic["range"]["start"]["character"] == 10
                            && diagnostic["range"]["end"]["line"] == 3
                            && diagnostic["range"]["end"]["character"] == 15
                    })
                });
            if saw_frontmatter_diagnostic {
                break;
            }
        }
    }
    assert!(saw_frontmatter_diagnostic);

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 2, "method": "shutdown", "params": null}),
    )
    .await;
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(2)
    {}
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();
}

#[tokio::test]
async fn stdio_jsonrpc_bare_snippet_completion() {
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
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(1)
    {}

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
                    "uri": "file:///tmp/sase_prompt_snippet.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "fo"
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
                "textDocument": {"uri": "file:///tmp/sase_prompt_snippet.md"},
                "position": {"line": 0, "character": 2}
            }
        }),
    )
    .await;

    let mut saw_snippet = false;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(2) {
            let items = message["result"]
                .as_array()
                .or_else(|| message["result"]["items"].as_array())
                .unwrap_or_else(|| {
                    panic!("unexpected completion response: {message}")
                });
            saw_snippet = items.iter().any(|item| {
                item["label"] == "foo"
                    && item["kind"] == 15
                    && item["insertTextFormat"] == 2
                    && item["textEdit"]["newText"] == "body $1$0"
            });
            break;
        }
    }

    assert!(saw_snippet);
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 3, "method": "shutdown", "params": null}),
    )
    .await;
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(3)
    {}
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();
}

#[tokio::test]
async fn stdio_jsonrpc_placeholder_completion_uses_open_document_text() {
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
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(1)
    {}

    let uri = "file:///tmp/sase_prompt_placeholder.md";
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
                    "text": "`<the plan>` then <>"
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
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 19}
            }
        }),
    )
    .await;

    let mut completion_item = None;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(2) {
            completion_item = message["result"]
                .as_array()
                .and_then(|items| items.first())
                .cloned();
            break;
        }
    }
    let item = completion_item.expect("expected placeholder completion item");
    assert_eq!(item["label"], "the plan");
    assert_eq!(item["kind"], 6);
    assert_eq!(item["textEdit"]["range"]["start"]["character"], 19);
    assert_eq!(item["textEdit"]["range"]["end"]["character"], 20);
    assert_eq!(item["textEdit"]["newText"], "the plan>");

    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": 3, "method": "shutdown", "params": null}),
    )
    .await;
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(3)
    {}
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    server_task.await.unwrap();
}

#[tokio::test]
async fn stdio_jsonrpc_clan_tribe_diagnostics_completion_and_snippets() {
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
    while read_message(&mut client_reader)
        .await
        .get("id")
        .and_then(Value::as_i64)
        != Some(1)
    {}

    let uri = "file:///tmp/sase_prompt_clan_tribe.md";
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
                    "text": "%clan(research.@, tribe=research)\n%c(research, tr)\n%tribe:review\n%t:review\n%family:old\n%group:old"
                }
            }
        }),
    )
    .await;

    let mut unknown_directives = Vec::new();
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            unknown_directives = message["params"]["diagnostics"]
                .as_array()
                .expect("diagnostic array")
                .iter()
                .filter(|diagnostic| diagnostic["code"] == "unknown_directive")
                .filter_map(|diagnostic| diagnostic["message"].as_str())
                .map(str::to_string)
                .collect();
            break;
        }
    }
    assert_eq!(
        unknown_directives,
        ["Unknown directive `%family`", "Unknown directive `%group`"]
    );

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/completion",
            "params": {
                "textDocument": {"uri": uri},
                "position": {"line": 1, "character": 15}
            }
        }),
    )
    .await;
    let mut keyword_completion = None;
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(2) {
            keyword_completion = message["result"]
                .as_array()
                .and_then(|items| {
                    items.iter().find(|item| item["label"] == "tribe=")
                })
                .cloned();
            break;
        }
    }
    let keyword_completion =
        keyword_completion.expect("expected tribe= completion item");
    assert_eq!(
        keyword_completion["textEdit"],
        json!({
            "range": {
                "start": {"line": 1, "character": 13},
                "end": {"line": 1, "character": 15}
            },
            "newText": "tribe="
        })
    );

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": 2},
                "contentChanges": [{"text": "%c"}]
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
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 2}
            }
        }),
    )
    .await;
    let mut labels = Vec::new();
    for _ in 0..8 {
        let message = read_message(&mut client_reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(3) {
            labels = message["result"]
                .as_array()
                .expect("completion array")
                .iter()
                .filter_map(|item| item["label"].as_str())
                .map(str::to_string)
                .collect();
            break;
        }
    }
    for expected in ["%clan", "%clan:...", "%clan(..., tribe=...)"] {
        assert!(labels.iter().any(|label| label == expected), "{labels:?}");
    }

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
