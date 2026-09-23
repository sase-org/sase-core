use std::{path::Path, sync::Arc};

use lsp_types::{
    CompletionItemKind, CompletionResponse, CompletionTextEdit, Documentation,
    InsertTextFormat, Position, Uri,
};

use sase_core::{
    AgentCatalogRequest, AgentCatalogResponse, EditorSnippetCatalogRequestWire,
    EditorSnippetCatalogResponseWire, EditorSnippetCatalogStatsWire,
    EditorSnippetEntryWire, FinalizerCatalogRequest, FinalizerCatalogResponse,
    HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
    MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
    MobileXpromptCatalogStatsWire, MobileXpromptInputWire,
    StaticHelperHostBridge,
};

use super::super::*;

use super::super::state::ServerConfig;

pub(super) async fn labels_at(
    server: &XpromptLspServer,
    text: &str,
) -> Vec<String> {
    let response = server
        .completion_for_text(
            text.to_string(),
            Position::new(0, text.len() as u32),
        )
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array for {text}");
    };
    items.iter().map(|item| item.label.clone()).collect()
}

pub(super) fn markdown_documentation(item: &CompletionItem) -> Option<&str> {
    match item.documentation.as_ref()? {
        Documentation::MarkupContent(content) => Some(content.value.as_str()),
        Documentation::String(value) => Some(value.as_str()),
    }
}

#[derive(Debug, Clone)]
pub(super) struct CountingAgentBridge {
    pub(super) calls: Arc<std::sync::atomic::AtomicU32>,
    pub(super) inner: StaticHelperHostBridge,
}

impl HelperHostBridge for CountingAgentBridge {
    fn agent_catalog(
        &self,
        request: &AgentCatalogRequest,
    ) -> std::result::Result<AgentCatalogResponse, HostBridgeError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.agent_catalog(request)
    }

    fn finalizer_catalog(
        &self,
        request: &FinalizerCatalogRequest,
    ) -> std::result::Result<FinalizerCatalogResponse, HostBridgeError> {
        self.inner.finalizer_catalog(request)
    }

    fn xprompt_catalog(
        &self,
        request: &MobileXpromptCatalogRequestWire,
    ) -> std::result::Result<MobileXpromptCatalogResponseWire, HostBridgeError>
    {
        self.inner.xprompt_catalog(request)
    }

    fn snippet_catalog(
        &self,
        request: &EditorSnippetCatalogRequestWire,
    ) -> std::result::Result<EditorSnippetCatalogResponseWire, HostBridgeError>
    {
        self.inner.snippet_catalog(request)
    }
}

pub(super) fn bridge_with_catalog(
    definition_path: Option<String>,
) -> StaticHelperHostBridge {
    bridge_with_catalog_entries(vec![catalog_entry(
        "foo",
        "#foo",
        Some("(path: path)".to_string()),
        vec![input_hint("path", "path", true, 0)],
        definition_path,
    )])
}

pub(super) fn bridge_with_catalog_entries(
    entries: Vec<MobileXpromptCatalogEntryWire>,
) -> StaticHelperHostBridge {
    bridge_with_catalog_and_snippets(entries, Vec::new())
}

pub(super) fn bridge_with_catalog_and_snippets(
    entries: Vec<MobileXpromptCatalogEntryWire>,
    snippets: Vec<EditorSnippetEntryWire>,
) -> StaticHelperHostBridge {
    let total_count = entries.len() as u64;
    let snippet_total_count = snippets.len() as u64;
    StaticHelperHostBridge {
        agent_catalog_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "status": "ok",
                "message": "",
                "entries": []
            }),
        )
        .unwrap(),
        finalizer_catalog_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "status": "ok",
                "message": "",
                "entries": []
            }),
        )
        .unwrap(),
        changespec_tags_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
                "context": {"project": "sase", "scope": "explicit"},
                "tags": [],
                "total_count": 0
            }),
        )
        .unwrap(),
        xprompt_catalog_response: MobileXpromptCatalogResponseWire {
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
            entries,
            stats: MobileXpromptCatalogStatsWire {
                total_count,
                project_count: 0,
                skill_count: total_count,
                memory_count: 0,
                pdf_requested: false,
            },
            catalog_attachment: None,
        },
        snippet_catalog_response: EditorSnippetCatalogResponseWire {
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
            entries: snippets,
            stats: EditorSnippetCatalogStatsWire {
                total_count: snippet_total_count,
            },
        },
        vcs_repo_catalog_response: VcsRepoCatalogResponse {
            schema_version: 1,
            status: "ok".to_string(),
            error_kind: None,
            message: String::new(),
            provider_display: "GitHub".to_string(),
            stale: false,
            entries: Vec::new(),
        },
        bead_list_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
                "context": {"project": "sase", "scope": "explicit"},
                "beads": [],
                "total_count": 0
            }),
        )
        .unwrap(),
        bead_show_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
                "context": {"project": "sase", "scope": "explicit"},
                "bead": {
                    "summary": {"id": "sase-1", "title": "Example", "status": "open", "bead_type": "phase", "tier": null, "project": "sase", "parent_id": null, "assignee": null, "updated_at": null, "dependency_count": 0, "block_count": 0, "child_count": 0, "plan_path_display": null, "changespec_name": null, "changespec_status": null},
                    "description": null, "notes": null, "design_path_display": null, "dependencies": [], "blocks": [], "children": [], "workspace_display": null
                }
            }),
        )
        .unwrap(),
        update_start_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
                "job": {"job_id": "job", "status": "running", "started_at": null, "finished_at": null, "message": null, "log_path_display": null, "completion_path_display": null}
            }),
        )
        .unwrap(),
        update_status_response: serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "result": {"status": "success", "message": null, "warnings": [], "skipped": [], "partial_failure_count": null},
                "job": {"job_id": "job", "status": "succeeded", "started_at": null, "finished_at": null, "message": null, "log_path_display": null, "completion_path_display": null}
            }),
        )
        .unwrap(),
    }
}

pub(super) fn bridge_with_vcs_repo_catalog(
    response: VcsRepoCatalogResponse,
) -> StaticHelperHostBridge {
    let mut bridge = bridge_with_catalog_entries(Vec::new());
    bridge.vcs_repo_catalog_response = response;
    bridge
}

pub(super) fn catalog_entry(
    name: &str,
    insertion: &str,
    input_signature: Option<String>,
    inputs: Vec<MobileXpromptInputWire>,
    definition_path: Option<String>,
) -> MobileXpromptCatalogEntryWire {
    MobileXpromptCatalogEntryWire {
        name: name.to_string(),
        display_label: name.to_string(),
        insertion: Some(insertion.to_string()),
        reference_prefix: Some("#".to_string()),
        kind: Some("prompt".to_string()),
        description: Some(if name == "foo" {
            "Foo prompt".to_string()
        } else {
            format!("{name} prompt")
        }),
        source_bucket: "builtin".to_string(),
        project: None,
        tags: Vec::new(),
        input_signature,
        inputs,
        is_skill: true,
        skill_name: Some(
            name.rsplit_once('/')
                .map_or(name, |(_, tail)| tail)
                .to_string(),
        ),
        memory_type: None,
        content_preview: None,
        source_path_display: Some("Cargo.toml".to_string()),
        definition_path,
        definition_range: None,
    }
}

pub(super) fn input_hint(
    name: &str,
    r#type: &str,
    required: bool,
    position: u32,
) -> MobileXpromptInputWire {
    MobileXpromptInputWire {
        name: name.to_string(),
        r#type: r#type.to_string(),
        description: None,
        required,
        default_display: None,
        position,
        repeatable: false,
        choices: Vec::new(),
    }
}

pub(super) fn snippet_entry(
    trigger: &str,
    template: &str,
    source: &str,
) -> EditorSnippetEntryWire {
    EditorSnippetEntryWire {
        trigger: trigger.to_string(),
        template: template.to_string(),
        source: source.to_string(),
        xprompt_name: None,
        description: Some(format!("{trigger} snippet")),
        source_path_display: Some("ace.snippets".to_string()),
    }
}

pub(super) fn diagnostics_contain_code(
    diagnostics: &[lsp_types::Diagnostic],
    expected_code: &str,
) -> bool {
    diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.code.as_ref(),
            Some(lsp_types::NumberOrString::String(code))
                if code == expected_code
        )
    })
}

pub(super) fn file_uri(path: impl AsRef<Path>) -> Uri {
    Uri::from_file_path(path.as_ref()).unwrap()
}
pub(super) async fn snippet_completion_items(
    entries: Vec<MobileXpromptCatalogEntryWire>,
    text: &str,
    character: u32,
) -> Vec<CompletionItem> {
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(entries)),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        *config = ServerConfig {
            snippet_support: true,
            ..ServerConfig::default()
        };
    }
    let response = server
        .completion_for_text(text.to_string(), Position { line: 0, character })
        .await
        .unwrap();
    let CompletionResponse::Array(items) = response else {
        panic!("expected completion array");
    };
    items
}

pub(super) fn assert_canonical_final_name(items: &[CompletionItem], end: u32) {
    let item = items
        .iter()
        .find(|item| {
            item.label == "%final"
                && item.kind != Some(CompletionItemKind::SNIPPET)
        })
        .unwrap_or_else(|| panic!("missing canonical %final name row"));
    assert_eq!(item.kind, Some(CompletionItemKind::TEXT));
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit for %final");
    };
    assert_eq!(edit.new_text.as_str(), "%final");
    assert_eq!(edit.range.start, Position::new(0, 0));
    assert_eq!(edit.range.end, Position::new(0, end));
}

pub(super) fn assert_snippet_item(
    items: &[CompletionItem],
    label: &str,
    new_text: &str,
) {
    let item = items
        .iter()
        .find(|item| item.label == label)
        .unwrap_or_else(|| panic!("missing completion item {label}"));
    assert_eq!(item.kind, Some(CompletionItemKind::SNIPPET));
    assert_eq!(item.insert_text_format, Some(InsertTextFormat::SNIPPET));
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit for {label}");
    };
    assert_eq!(edit.new_text.as_str(), new_text);
}

pub(super) fn assert_completion_edit(
    items: &[CompletionItem],
    label: &str,
    start_character: u32,
    end_character: u32,
    new_text: &str,
    kind: CompletionItemKind,
) {
    let item = items
        .iter()
        .find(|item| item.label == label)
        .unwrap_or_else(|| panic!("missing completion item {label}"));
    assert_eq!(item.kind, Some(kind));
    assert_eq!(item.filter_text.as_deref(), Some(label));
    let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref() else {
        panic!("expected text edit for {label}");
    };
    assert_eq!(
        edit.range.start,
        Position {
            line: 0,
            character: start_character,
        }
    );
    assert_eq!(
        edit.range.end,
        Position {
            line: 0,
            character: end_character,
        }
    );
    assert_eq!(edit.new_text.as_str(), new_text);
}

// --- model directive completion ----------------------------------------

pub(super) fn write_model_catalog(path: &Path) {
    fs::write(
        path,
        r#"{
            "schema_version": 1,
            "entries": [
                {
                    "value": "claude-fable-5",
                    "display": "claude-fable-5",
                    "description": "Claude (fable)",
                    "kind": "model",
                    "provider": "claude",
                    "aliases": ["fable"]
                },
                {
                    "value": "gpt-5.6-sol",
                    "display": "gpt-5.6-sol",
                    "description": "Codex (gpt56sol)",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": ["gpt56sol"]
                },
                {
                    "value": "gpt-5.5",
                    "display": "gpt-5.5",
                    "description": "Codex (gpt55)",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": ["gpt55"]
                }
            ]
        }"#,
    )
    .unwrap();
}

pub(super) fn write_model_catalog_with_providers(path: &Path) {
    fs::write(
        path,
        r#"{
            "schema_version": 1,
            "entries": [
                {
                    "value": "claude-fable-5",
                    "display": "claude-fable-5",
                    "description": "Claude (fable)",
                    "kind": "model",
                    "provider": "claude",
                    "aliases": ["fable"]
                },
                {
                    "value": "opus",
                    "display": "opus",
                    "description": "Claude",
                    "kind": "model",
                    "provider": "claude",
                    "aliases": []
                },
                {
                    "value": "gpt-5.6-sol",
                    "display": "gpt-5.6-sol",
                    "description": "Codex (gpt56sol)",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": ["gpt56sol"]
                },
                {
                    "value": "anthropic/claude-sonnet-4-5",
                    "display": "anthropic/claude-sonnet-4-5",
                    "description": "OpenCode",
                    "kind": "model",
                    "provider": "opencode",
                    "aliases": []
                },
                {
                    "value": "claude/",
                    "display": "claude/",
                    "description": "Claude",
                    "kind": "provider",
                    "provider": "claude",
                    "aliases": []
                },
                {
                    "value": "codex/",
                    "display": "codex/",
                    "description": "Codex",
                    "kind": "provider",
                    "provider": "codex",
                    "aliases": []
                },
                {
                    "value": "opencode/",
                    "display": "opencode/",
                    "description": "OpenCode",
                    "kind": "provider",
                    "provider": "opencode",
                    "aliases": []
                }
            ]
        }"#,
    )
    .unwrap();
}

pub(super) fn write_model_catalog_with_unsafe_shortcut_rows(path: &Path) {
    fs::write(
        path,
        r#"{
            "schema_version": 1,
            "entries": [
                {
                    "value": "gpt-safe",
                    "display": "gpt-safe",
                    "description": "Safe Codex",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": []
                },
                {
                    "value": "gpt bad",
                    "display": "gpt bad",
                    "description": "Whitespace is unsafe inline",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": []
                },
                {
                    "value": "gpt\u0000bad",
                    "display": "gpt null",
                    "description": "NUL is unsafe inline",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": []
                },
                {
                    "value": "unsafe\u001fbad",
                    "display": "unsafe control",
                    "description": "Control is unsafe inline",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": []
                }
            ]
        }"#,
    )
    .unwrap();
}

pub(super) fn write_enriched_model_catalog(path: &Path) {
    fs::write(
        path,
        r#"{
            "schema_version": 1,
            "entries": [
                {
                    "value": "opus",
                    "display": "opus",
                    "description": "Claude",
                    "kind": "model",
                    "provider": "claude",
                    "aliases": []
                },
                {
                    "value": "gpt-5.6-sol",
                    "display": "gpt-5.6-sol",
                    "description": "Codex",
                    "kind": "model",
                    "provider": "codex",
                    "aliases": ["gpt56sol"]
                },
                {
                    "value": "@default",
                    "display": "@default",
                    "description": "Default model for prompts.",
                    "kind": "implicit_alias",
                    "provider": "",
                    "aliases": ["default"],
                    "alias_kind": "default",
                    "target_provider": "claude",
                    "target_model": "opus",
                    "target_effort": "high",
                    "provenance": "implicit",
                    "reference": "coder",
                    "reference_effort": "medium",
                    "selector_mode": "",
                    "pool_available": 0,
                    "pool_total": 0,
                    "config_source": "",
                    "bucket": ""
                },
                {
                    "value": "@claude_coder",
                    "display": "@claude_coder",
                    "description": "Claude coder follow-up model.",
                    "kind": "implicit_alias",
                    "provider": "",
                    "aliases": ["claude_coder"],
                    "alias_kind": "provider_coder",
                    "target_provider": "claude",
                    "target_model": "opus",
                    "target_effort": "",
                    "provenance": "implicit",
                    "reference": "coder",
                    "reference_effort": "",
                    "selector_mode": "",
                    "pool_available": 0,
                    "pool_total": 0,
                    "config_source": "",
                    "bucket": ""
                },
                {
                    "value": "@scout",
                    "display": "@scout",
                    "description": "Fast scouting pool.",
                    "kind": "user_alias",
                    "provider": "",
                    "aliases": ["scout"],
                    "alias_kind": "user",
                    "target_provider": "codex",
                    "target_model": "gpt-5.6-sol",
                    "target_effort": "low",
                    "provenance": "configured",
                    "reference": "",
                    "reference_effort": "",
                    "selector_mode": "round_robin",
                    "pool_available": 2,
                    "pool_total": 3,
                    "config_source": "custom",
                    "bucket": "fast"
                },
                {
                    "value": "claude/",
                    "display": "claude/",
                    "description": "Claude",
                    "kind": "provider",
                    "provider": "claude",
                    "aliases": []
                },
                {
                    "value": "codex/",
                    "display": "codex/",
                    "description": "Codex",
                    "kind": "provider",
                    "provider": "codex",
                    "aliases": []
                }
            ]
        }"#,
    )
    .unwrap();
}
// --- vcs_project (`+`) completion --------------------------------------

pub(super) fn write_vcs_project_catalog(path: &Path) {
    fs::write(
        path,
        r##"{
            "schema_version": 4,
            "workflow_names": ["gh", "git", "hg"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub",
                    "description": "SASE repo",
                    "aliases": [],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "sase",
                    "status": ""
                }
            ]
        }"##,
    )
    .unwrap();
}

pub(super) fn write_vcs_project_catalog_with_pr(path: &Path) {
    fs::write(
        path,
        r##"{
            "schema_version": 2,
            "workflow_names": ["gh", "git", "hg"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub",
                    "description": "SASE repo",
                    "aliases": [],
                    "kind": "project",
                    "project": "sase",
                    "status": ""
                },
                {
                    "name": "ship-completion",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:ship-completion",
                    "provider_display": "GitHub",
                    "description": "",
                    "aliases": [],
                    "entry_kind": "patch",
                    "kind": "changespec",
                    "project": "sase",
                    "status": "Ready"
                }
            ]
        }"##,
    )
    .unwrap();
}

pub(super) fn write_vcs_ref_catalog(path: &Path) {
    fs::write(
        path,
        r##"{
            "schema_version": 3,
            "workflow_names": ["gh", "git", "hg"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub",
                    "description": "SASE repo",
                    "aliases": ["sase-core"],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "sase",
                    "status": ""
                },
                {
                    "name": "ship-completion",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:ship-completion",
                    "provider_display": "GitHub",
                    "description": "Completion patch",
                    "aliases": [],
                    "entry_kind": "patch",
                    "kind": "changespec",
                    "project": "sase",
                    "status": "Ready"
                },
                {
                    "name": "local",
                    "vcs_prefix": "git",
                    "display_tag": "#git:local",
                    "provider_display": "Bare Git",
                    "description": "",
                    "aliases": [],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "local",
                    "status": ""
                }
            ],
            "namespaces": {
                "gh": [
                    {
                        "name": "sase-org",
                        "description": "2 enabled projects",
                        "kind_label": "org"
                    },
                    {
                        "name": "bbugyi200",
                        "description": "from github_orgs",
                        "kind_label": "org"
                    }
                ]
            }
        }"##,
    )
    .unwrap();
}

/// v5 catalog with tag targets, accents, and a provider-less project.
/// `sase` resolves with accent 2 and `current`; `notes` with accent 5;
/// `orphan` resolves but has no VCS provider; `ship` is a patch row.
pub(super) fn write_v5_project_tag_catalog(path: &Path) {
    fs::write(
        path,
        r##"{
            "schema_version": 5,
            "workflow_names": ["gh", "git"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub",
                    "description": "SASE repo",
                    "aliases": ["sase-core"],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "sase",
                    "status": "",
                    "key": "gh_sase-org__sase",
                    "tag": "+sase",
                    "accent_index": 2,
                    "current": true
                },
                {
                    "name": "notes",
                    "vcs_prefix": "git",
                    "display_tag": "#git:notes",
                    "provider_display": "Bare Git",
                    "description": "",
                    "aliases": [],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "notes",
                    "status": "",
                    "key": "git_notes",
                    "tag": "+notes",
                    "accent_index": 5,
                    "current": false
                },
                {
                    "name": "ship",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:ship",
                    "provider_display": "GitHub",
                    "description": "Completion patch",
                    "aliases": [],
                    "entry_kind": "patch",
                    "kind": "changespec",
                    "project": "sase",
                    "status": "Ready"
                }
            ],
            "accent_palette": ["#111111", "#222222", "#333333", "#444444", "#555555", "#666666"],
            "project_tags": [
                {
                    "key": "gh_sase-org__sase",
                    "name": "sase",
                    "aliases": ["sase-core"],
                    "workflow_type": "gh"
                },
                {
                    "key": "git_notes",
                    "name": "notes",
                    "aliases": [],
                    "workflow_type": "git"
                },
                {
                    "key": "git_orphan",
                    "name": "orphan",
                    "aliases": [],
                    "workflow_type": null
                }
            ]
        }"##,
    )
    .unwrap();
}

/// v5 catalog where `+sase` matches two targets (ambiguous).
pub(super) fn write_ambiguous_project_tag_catalog(path: &Path) {
    fs::write(
        path,
        r##"{
            "schema_version": 5,
            "workflow_names": ["gh", "git"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub",
                    "description": "SASE repo",
                    "aliases": [],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "sase",
                    "status": "",
                    "key": "gh_sase-org__sase",
                    "tag": "+sase",
                    "accent_index": 2,
                    "current": true
                }
            ],
            "accent_palette": ["#111111", "#222222", "#333333"],
            "project_tags": [
                {
                    "key": "gh_sase-org__sase",
                    "name": "sase",
                    "aliases": [],
                    "workflow_type": "gh"
                },
                {
                    "key": "git_sase",
                    "name": "sase",
                    "aliases": [],
                    "workflow_type": "git"
                }
            ]
        }"##,
    )
    .unwrap();
}

pub(super) fn write_glossary_catalog(
    path: &Path,
    root: &Path,
    default_project: Option<&str>,
) {
    let project = |key: &str,
                   name: &str,
                   aliases: Vec<&str>,
                   term: &str,
                   alias: &str,
                   definition: &str| {
        let config_path = root
            .join(key)
            .join("sase")
            .join("sase.yml")
            .to_string_lossy()
            .into_owned();
        serde_json::json!({
            "schema_version": 1,
            "project": {
                "key": key,
                "name": name,
                "aliases": aliases,
                "workspace_dir": root.join(key).to_string_lossy().into_owned(),
            },
            "config_path": config_path,
            "config_signature": {
                "path": config_path,
                "mtime_ns": 1,
                "size": 42,
            },
            "entries": [{
                "index": 0,
                "term": term,
                "normalized_term": term,
                "definition": definition,
                "configured_aliases": [alias],
                "effective_aliases": [term, alias],
                "source": {
                    "config_path": config_path,
                    "config_key_path": ["glossary", term],
                    "definition_range": {
                        "start": {"line": 4, "character": 16},
                        "end": {"line": 4, "character": 27}
                    }
                }
            }]
        })
    };
    fs::write(
        path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "default_project": default_project,
            "projects": [
                project(
                    "sase",
                    "sase",
                    vec!["sase-core"],
                    "Agent Clan",
                    "clan",
                    "A named rootless container.",
                ),
                project(
                    "local",
                    "local",
                    vec![],
                    "Workspace",
                    "workspace checkout",
                    "A numbered project checkout.",
                )
            ]
        }))
        .unwrap(),
    )
    .unwrap();
}

pub(super) fn write_artifact_ref_catalog(
    path: &Path,
    root: &Path,
    default_project: Option<&str>,
) {
    let project = |name: &str| {
        let project_root = root.join(name);
        serde_json::json!({
            "name": name,
            "key": format!("key_{name}"),
            "aliases": [format!("{name}-alias")],
            "context": {
                "schema_version": 1,
                "document_roots": [
                    {
                        "kind": "designs",
                        "root": project_root.join("designs")
                    },
                    {
                        "kind": "plan",
                        "root": project_root.join("plans")
                    }
                ],
                "chats_root": project_root.join("chats"),
                "artifact_index_path": project_root.join("artifact-index.jsonl"),
                "repositories": [],
                "projects": []
            }
        })
    };
    fs::write(
        path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "default_project": default_project,
            "projects": [project("sase"), project("local")]
        }))
        .unwrap(),
    )
    .unwrap();
}

pub(super) fn completion_items(
    response: CompletionResponse,
) -> Vec<CompletionItem> {
    match response {
        CompletionResponse::Array(items) => items,
        CompletionResponse::List(list) => list.items,
    }
}

pub(super) fn absolute_semantic_tokens(
    tokens: &[lsp_types::SemanticToken],
) -> Vec<(u32, u32, u32, u32, u32)> {
    let mut line = 0u32;
    let mut start = 0u32;
    tokens
        .iter()
        .map(|token| {
            line += token.delta_line;
            if token.delta_line == 0 {
                start += token.delta_start;
            } else {
                start = token.delta_start;
            }
            (
                line,
                start,
                token.length,
                token.token_type,
                token.token_modifiers_bitset,
            )
        })
        .collect()
}

pub(super) fn repo_entry(
    name: &str,
    description: &str,
    visibility: &str,
    is_fork: bool,
    is_archived: bool,
    pushed_at: Option<&str>,
) -> VcsRepoEntry {
    VcsRepoEntry {
        name: name.to_string(),
        r#ref: format!("bbugyi200/{name}"),
        description: description.to_string(),
        visibility: visibility.to_string(),
        is_fork,
        is_archived,
        pushed_at: pushed_at.map(str::to_string),
    }
}

pub(super) fn vcs_repo_catalog_response(
    status: &str,
    message: &str,
    entries: Vec<VcsRepoEntry>,
) -> VcsRepoCatalogResponse {
    VcsRepoCatalogResponse {
        schema_version: 1,
        status: status.to_string(),
        error_kind: None,
        message: message.to_string(),
        provider_display: "GitHub".to_string(),
        stale: false,
        entries,
    }
}
