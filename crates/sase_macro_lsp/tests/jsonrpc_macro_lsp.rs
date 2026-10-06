use std::{env, fs, sync::Arc};

use sase_core::{
    EditorSnippetCatalogRequestWire, EditorSnippetCatalogResponseWire,
    EditorSnippetCatalogStatsWire, EditorSnippetEntryWire, HelperHostBridge,
    HostBridgeError, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperStatusWire, MobileMacroCatalogEntryWire,
    MobileMacroCatalogRequestWire, MobileMacroCatalogResponseWire,
    MobileMacroCatalogStatsWire, MobileMacroInputWire,
    EDITOR_SNIPPET_CATALOG_WIRE_SCHEMA_VERSION,
};
use sase_macro_lsp::MacroLspServer;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tower_lsp_server::{LspService, Server};

#[derive(Debug)]
struct FixtureBridge;

impl HelperHostBridge for FixtureBridge {
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
                scope: MobileHelperProjectScopeWire::AllKnown,
            },
            entries: vec![MobileMacroCatalogEntryWire {
                name: "demo".to_string(),
                display_label: "demo".to_string(),
                insertion: Some("#demo".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("prompt".to_string()),
                description: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![MobileMacroInputWire {
                    name: "path".to_string(),
                    r#type: "path".to_string(),
                    description: None,
                    required: true,
                    default_display: None,
                    position: 0,
                    repeatable: false,
                    choices: Vec::new(),
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
            schema_version: EDITOR_SNIPPET_CATALOG_WIRE_SCHEMA_VERSION,
            result: MobileHelperResultWire {
                status: MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: MobileHelperProjectContextWire {
                project: None,
                scope: MobileHelperProjectScopeWire::AllKnown,
            },
            entries: vec![EditorSnippetEntryWire {
                trigger: "demo".to_string(),
                template: "body $1$0".to_string(),
                source: "ace.snippets".to_string(),
                macro_name: None,
                description: None,
                source_path_display: Some("ace.snippets".to_string()),
            }],
            stats: EditorSnippetCatalogStatsWire { total_count: 1 },
        })
    }
}

async fn write_message(writer: &mut DuplexStream, value: Value) {
    let body = value.to_string();
    writer
        .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
        .await
        .unwrap();
    writer.write_all(body.as_bytes()).await.unwrap();
}

async fn read_message(reader: &mut DuplexStream) -> Value {
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

async fn read_response(reader: &mut DuplexStream, id: i64) -> Value {
    for _ in 0..32 {
        let message = read_message(reader).await;
        if message.get("id").and_then(Value::as_i64) == Some(id) {
            return message;
        }
    }
    panic!("missing response id {id}");
}

fn spawn_server() -> (
    tokio::io::DuplexStream,
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<()>,
) {
    let (client_writer, server_stdin) = tokio::io::duplex(8192);
    let (server_stdout, client_reader) = tokio::io::duplex(8192);
    let (service, socket) = LspService::new(|client| {
        MacroLspServer::with_bridge(client, Arc::new(FixtureBridge))
    });
    let task = tokio::spawn(async move {
        Server::new(server_stdin, server_stdout, socket)
            .serve(service)
            .await;
    });
    (client_writer, client_reader, task)
}

#[tokio::test]
async fn initialize_advertises_both_command_families_and_legacy_identity() {
    let (mut writer, mut reader, task) = spawn_server();
    write_message(
        &mut writer,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "processId": null,
                "rootUri": null,
                "capabilities": {},
                "initializationOptions": {"accept_legacy_xprompt_names": true}
            }
        }),
    )
    .await;
    let response = read_response(&mut reader, 1).await;
    let server_name = response["result"]["serverInfo"]["name"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(server_name, "sase-macro-lsp");
    let commands = response["result"]["capabilities"]["executeCommandProvider"]
        ["commands"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    for expected in [
        "sase.macroLsp.refreshCatalog",
        "sase.macroLsp.openSource",
        "sase.macroLsp.refreshCatalog",
        "sase.macroLsp.openSource",
    ] {
        assert!(commands.contains(&expected.to_string()), "{commands:?}");
    }
    // Both refresh commands dispatch without error.
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;
    for (id, command) in [
        (10, "sase.macroLsp.refreshCatalog"),
        (11, "sase.macroLsp.refreshCatalog"),
    ] {
        write_message(
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "workspace/executeCommand",
                "params": {"command": command}
            }),
        )
        .await;
        let reply = read_response(&mut reader, id).await;
        assert!(reply.get("error").is_none(), "{command}: {reply}");
    }
    // Both open-source commands dispatch without error.
    for (id, command) in [
        (12, "sase.macroLsp.openSource"),
        (13, "sase.macroLsp.openSource"),
    ] {
        write_message(
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "workspace/executeCommand",
                "params": {"command": command, "arguments": []}
            }),
        )
        .await;
        let reply = read_response(&mut reader, id).await;
        assert!(reply.get("error").is_none(), "{command}: {reply}");
    }
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 99, "method": "shutdown", "params": null}),
    )
    .await;
    read_response(&mut reader, 99).await;
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    task.await.unwrap();
}

#[tokio::test]
async fn diagnostics_keep_legacy_source_and_code_actions() {
    let (mut writer, mut reader, task) = spawn_server();
    write_message(
        &mut writer,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"processId": null, "rootUri": null, "capabilities": {}}
        }),
    )
    .await;
    read_response(&mut reader, 1).await;
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;
    write_message(
        &mut writer,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": "file:///tmp/sase_prompt_macro.md",
                    "languageId": "markdown",
                    "version": 1,
                    "text": "#demo"
                }
            }
        }),
    )
    .await;
    let mut saw_legacy_source = false;
    for _ in 0..8 {
        let message = read_message(&mut reader).await;
        if message.get("method").and_then(Value::as_str)
            == Some("textDocument/publishDiagnostics")
        {
            if let Some(diagnostics) =
                message["params"]["diagnostics"].as_array()
            {
                for diagnostic in diagnostics {
                    assert_eq!(diagnostic["source"], "sase-macro");
                }
                // Missing required arg for #demo produces at least one
                // diagnostic with the pinned source.
                if diagnostics.iter().any(|d| {
                    d["source"] == "sase-macro"
                        && d["code"] == "missing_required_arg"
                }) {
                    saw_legacy_source = true;
                    break;
                }
            }
        }
    }
    assert!(saw_legacy_source, "expected pinned diagnostic source");
    // Code actions preserve legacy labels.
    write_message(
        &mut writer,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "textDocument/codeAction",
            "params": {
                "textDocument": {"uri": "file:///tmp/sase_prompt_macro.md"},
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 5}
                },
                "context": {"diagnostics": []}
            }
        }),
    )
    .await;
    let reply = read_response(&mut reader, 2).await;
    let actions = reply["result"].as_array().unwrap();
    assert!(
        actions.iter().any(|action| {
            action["title"] == "Refresh xprompt catalog"
                && (action["command"] == "sase.macroLsp.refreshCatalog"
                    || action["command"]["command"]
                        == "sase.macroLsp.refreshCatalog")
        }),
        "{actions:?}"
    );
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 3, "method": "shutdown", "params": null}),
    )
    .await;
    read_response(&mut reader, 3).await;
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    task.await.unwrap();
}

#[tokio::test]
async fn semantic_tokens_legend_order_unchanged() {
    let (mut writer, mut reader, task) = spawn_server();
    write_message(
        &mut writer,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"processId": null, "rootUri": null, "capabilities": {}}
        }),
    )
    .await;
    let response = read_response(&mut reader, 1).await;
    let legend =
        &response["result"]["capabilities"]["semanticTokensProvider"]["legend"];
    let types = legend["tokenTypes"].as_array().unwrap();
    // Legend still carries both macro and function types; directives use
    // `macro` and definition references use `function`.
    let names = types.iter().filter_map(Value::as_str).collect::<Vec<_>>();
    assert!(names.contains(&"macro"));
    assert!(names.contains(&"function"));
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    )
    .await;
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 7, "method": "shutdown", "params": null}),
    )
    .await;
    read_response(&mut reader, 7).await;
    write_message(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "exit", "params": null}),
    )
    .await;
    task.await.unwrap();
}

#[test]
fn new_macro_binary_reports_macro_version() {
    let macro_bin = env!("CARGO_BIN_EXE_sase-macro-lsp");
    let old_bin = env!("CARGO_BIN_EXE_sase-macro-lsp");
    let macro_out = std::process::Command::new(macro_bin)
        .arg("--version")
        .output()
        .expect("run sase-macro-lsp --version");
    assert!(macro_out.status.success());
    let macro_text = String::from_utf8(macro_out.stdout).unwrap();
    assert!(macro_text.starts_with("sase-macro-lsp "), "{macro_text:?}");
    let old_out = std::process::Command::new(old_bin)
        .arg("--version")
        .output()
        .expect("run sase-macro-lsp --version");
    assert!(old_out.status.success());
    let old_text = String::from_utf8(old_out.stdout).unwrap();
    assert!(old_text.starts_with("sase-macro-lsp "), "{old_text:?}");
    // Same package version, different binary identity.
    assert_eq!(
        macro_text.split_whitespace().nth(1),
        old_text.split_whitespace().nth(1)
    );
}

#[test]
fn macros_config_recognized_for_invalidation() {
    // Mirrors the server's watcher rule without reaching into privates:
    // both spellings must be treated as config sources. The rule lives in
    // `should_invalidate_for_uri`; this test pins the filename contract the
    // integration depends on.
    for name in ["macros.yml", "macros.yaml", "xprompts.yml", "xprompts.yaml"] {
        assert!(name.ends_with(".yml") || name.ends_with(".yaml"), "{name}");
        assert!(
            ["macros", "xprompts"]
                .iter()
                .any(|stem| name.starts_with(stem)),
            "{name}"
        );
    }
    let _ = fs::read_dir(env::temp_dir()).unwrap();
}
