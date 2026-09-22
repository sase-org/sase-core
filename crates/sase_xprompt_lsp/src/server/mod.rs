use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime},
};

use lsp_types::{
    ClientCapabilities, CodeAction, CodeActionKind, CodeActionOptions,
    CodeActionOrCommand, CodeActionParams, CodeActionProviderCapability,
    CodeActionResponse, Command, CompletionItem, CompletionOptions,
    CompletionParams, CompletionResponse, CompletionTriggerKind,
    DidChangeTextDocumentParams, DidChangeWatchedFilesParams,
    DidCloseTextDocumentParams, DidOpenTextDocumentParams, DocumentChanges,
    DocumentOnTypeFormattingOptions, DocumentOnTypeFormattingParams,
    ExecuteCommandOptions, ExecuteCommandParams, GotoDefinitionParams,
    GotoDefinitionResponse, Hover, HoverParams, HoverProviderCapability,
    InitializeParams, InitializeResult, InitializedParams, LSPAny, Location,
    MessageType, OneOf, OptionalVersionedTextDocumentIdentifier, Position,
    Range, SemanticTokens, SemanticTokensFullOptions, SemanticTokensOptions,
    SemanticTokensParams, SemanticTokensResult,
    SemanticTokensServerCapabilities, ServerCapabilities, ServerInfo,
    TextDocumentEdit, TextDocumentSyncCapability, TextDocumentSyncKind,
    TextEdit, Uri, WorkDoneProgressOptions, WorkspaceEdit,
};
use sase_core::project_tag::ProjectTagTargetWire;
use sase_core::{
    editor_analyze_artifact_refs, editor_analyze_document,
    editor_build_agent_completion_candidates,
    editor_build_artifact_ref_payload_inventory,
    editor_build_at_reference_menu_with_options,
    editor_build_directive_clause_candidates,
    editor_build_directive_completion_candidates_with_flags,
    editor_build_file_completion_candidates_with_base,
    editor_build_file_history_completion_candidates,
    editor_build_placeholder_completion_candidates,
    editor_build_snippet_completion_candidates,
    editor_build_vcs_project_completion_candidates,
    editor_build_vcs_ref_completion_candidates,
    editor_build_vcs_repo_completion_candidates,
    editor_build_xprompt_arg_name_candidates,
    editor_build_xprompt_completion_candidates,
    editor_classify_completion_context_with_artifacts_and_workflows,
    editor_classify_completion_context_with_workflows,
    editor_definition_at_position, editor_detect_at_reference_context,
    editor_detect_model_alias_shortcut_context,
    editor_directive_contract_with_flags,
    editor_directive_is_hidden_from_name_completion_with_flags,
    editor_extract_token_at_position,
    editor_filter_explicit_model_shortcut_entries,
    editor_filter_model_alias_shortcut_entries,
    editor_hover_at_position_with_flags, editor_model_shortcut_context,
    editor_model_shortcut_edit, editor_plan_argument_colon_to_parentheses_edit,
    editor_plan_argument_double_colon_to_parentheses_edit,
    editor_plan_model_alias_shortcut_edit,
    editor_typed_launch_directive_diagnostics,
    filter_model_completion_candidates, ArtifactRefContextWire,
    AtReferenceContextWire, AtReferenceInventoryWire, AtReferenceKindRowWire,
    AtReferenceMenuOptionsWire, AtReferencePathRowWire,
    AtReferencePayloadIndex, AtReferenceStage, CompiledGlossaryCatalog,
    CompletionCandidate, CompletionContextKind, CompletionList,
    DirectiveClauseKind, DirectiveCompletionInventories, DirectiveMachineEntry,
    DirectiveModelAliasKey, DirectiveSyntaxForm, DirectiveValueRole,
    DocumentSnapshot, EditorPosition, EditorRange, EditorSnippetEntryWire,
    GlossaryCatalogWire, GlossaryEntryWire, GlossarySpanWire, HelperHostBridge,
    HoverPayload, ModelAliasShortcutContextWire, ModelCompletionEntryWire,
    ModelShortcutContextWire, ModelShortcutKind, VcsNamespaceEntry,
    VcsProjectEntry, VcsRepoCatalogResponse, VcsRepoEntry, XpromptAssistEntry,
    MEMORY_NAMESPACE_SEGMENT,
};
use serde::Deserialize;
use tower_lsp_server::jsonrpc::Result;
use tower_lsp_server::{Client, LanguageServer, LspService, Server, UriExt};
use tracing::{info, warn};

use crate::catalog_cache::{CatalogCache, CatalogFailure};
use crate::lsp_convert::{
    agent_completion_response, apply_replacement,
    at_reference_completion_response, completion_response,
    diagnostic as lsp_diagnostic, finalizer_completion_response,
    hover as lsp_hover, model_alias_shortcut_completion_response,
    model_completion_response, model_shortcut_completion_response,
    placeholder_completion_response, sase_snippet_completion_item,
    snippet_completion_item, to_editor_position, to_lsp_range,
    vcs_project_completion_response, vcs_ref_completion_response,
    vcs_repo_completion_response,
};
use crate::semantic_tokens::{document_semantic_tokens, legend};

const SERVER_NAME: &str = "sase-xprompt-lsp";
const REFRESH_COMMAND: &str = "sase.xpromptLsp.refreshCatalog";
const OPEN_SOURCE_COMMAND: &str = "sase.xpromptLsp.openSource";
const ARTIFACT_REF_CACHE_TTL: Duration = Duration::from_secs(2);
const GLOSSARY_CACHE_TTL: Duration = Duration::from_secs(2);

/// Env var carrying the path to the JSON `vcs_project` completion catalog
/// (enabled-project entries + known VCS workflow names). Materialized by the
/// Python launcher (`integrations/xprompt_lsp.py`) at LSP startup and re-read
/// fresh on every `+` completion request so external rewrites are picked up.
const VCS_PROJECT_CATALOG_ENV: &str = "SASE_XPROMPT_VCS_PROJECT_CATALOG";
const MODEL_CATALOG_ENV: &str = "SASE_XPROMPT_MODEL_CATALOG";
const MACHINE_CATALOG_ENV: &str = "SASE_XPROMPT_MACHINE_CATALOG";
const ARTIFACT_REF_CATALOG_ENV: &str = "SASE_XPROMPT_ARTIFACT_REF_CATALOG";
const GLOSSARY_CATALOG_ENV: &str = "SASE_XPROMPT_GLOSSARY_CATALOG";
const TYPED_LAUNCH_UNITS_ENV: &str = "SASE_TYPED_LAUNCH_UNITS";
const QUEUE_CAPACITY_BUDGET_ENV: &str = "SASE_QUEUE_CAPACITY_BUDGET";

mod actions;
mod catalogs;
mod completion;
mod completion_items;
mod documents;
mod initialize;
mod state;

#[cfg(test)]
mod tests;

pub use state::XpromptLspServer;

use self::actions::should_invalidate_for_uri;
use self::initialize::config_from_initialize;

impl LanguageServer for XpromptLspServer {
    async fn initialize(
        &self,
        params: InitializeParams,
    ) -> Result<InitializeResult> {
        let config = config_from_initialize(&params);
        if let Ok(mut stored) = self.config.write() {
            *stored = config;
        }

        Ok(InitializeResult {
            server_info: Some(ServerInfo {
                name: SERVER_NAME.to_string(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(true),
                    trigger_characters: Some(vec![
                        "#".to_string(),
                        "!".to_string(),
                        "/".to_string(),
                        "%".to_string(),
                        ".".to_string(),
                        "@".to_string(),
                        ":".to_string(),
                        "(".to_string(),
                        ",".to_string(),
                        "+".to_string(),
                        "<".to_string(),
                        "=".to_string(),
                    ]),
                    work_done_progress_options: WorkDoneProgressOptions {
                        work_done_progress: Some(false),
                    },
                    all_commit_characters: None,
                    completion_item: None,
                }),
                execute_command_provider: Some(ExecuteCommandOptions {
                    commands: vec![
                        REFRESH_COMMAND.to_string(),
                        OPEN_SOURCE_COMMAND.to_string(),
                    ],
                    work_done_progress_options: WorkDoneProgressOptions {
                        work_done_progress: Some(false),
                    },
                }),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                definition_provider: Some(OneOf::Left(true)),
                code_action_provider: Some(
                    CodeActionProviderCapability::Options(CodeActionOptions {
                        code_action_kinds: Some(vec![
                            CodeActionKind::QUICKFIX,
                            CodeActionKind::REFACTOR_REWRITE,
                            CodeActionKind::SOURCE,
                        ]),
                        work_done_progress_options: WorkDoneProgressOptions {
                            work_done_progress: Some(false),
                        },
                        resolve_provider: Some(false),
                    }),
                ),
                semantic_tokens_provider: Some(
                    SemanticTokensServerCapabilities::SemanticTokensOptions(
                        SemanticTokensOptions {
                            work_done_progress_options:
                                WorkDoneProgressOptions {
                                    work_done_progress: Some(false),
                                },
                            legend: legend(),
                            range: None,
                            full: Some(SemanticTokensFullOptions::Bool(true)),
                        },
                    ),
                ),
                document_on_type_formatting_provider: Some(
                    DocumentOnTypeFormattingOptions {
                        first_trigger_character: "(".to_string(),
                        more_trigger_character: None,
                    },
                ),
                ..Default::default()
            },
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        info!("sase xprompt LSP initialized");
        self.refresh_catalog_explicit().await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let text = params.text_document.text;
        let document =
            self.open_document(&uri, params.text_document.language_id, text);
        if let Ok(mut documents) = self.documents.write() {
            documents.insert(uri.to_string(), document.clone());
        }
        self.publish_document_diagnostics(uri, document).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let Some(change) = params.content_changes.into_iter().last() else {
            return;
        };
        let uri = params.text_document.uri;
        let text = change.text;
        let previous = self.document_for_uri(&uri);
        let language_id = previous
            .as_ref()
            .map(|document| document.language_id.clone())
            .unwrap_or_default();
        let document =
            self.changed_document(&uri, language_id, text, previous.as_ref());
        if let Ok(mut documents) = self.documents.write() {
            documents.insert(uri.to_string(), document.clone());
        }
        self.publish_document_diagnostics(uri, document).await;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;
        if let Ok(mut documents) = self.documents.write() {
            documents.remove(&uri.to_string());
        }
        self.client.publish_diagnostics(uri, Vec::new(), None).await;
    }

    async fn semantic_tokens_full(
        &self,
        params: SemanticTokensParams,
    ) -> Result<Option<SemanticTokensResult>> {
        let uri = params.text_document.uri;
        let Some(document) = self.document_for_uri(&uri) else {
            return Ok(None);
        };
        if !document.eligible {
            return Ok(None);
        }
        Ok(Some(
            self.semantic_tokens_for_uri_text(&uri, document.text)
                .into(),
        ))
    }

    async fn completion(
        &self,
        params: CompletionParams,
    ) -> Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri;
        let Some(document) = self.document_for_uri(&uri) else {
            return Ok(None);
        };
        if !document.eligible {
            return Ok(None);
        }
        Ok(self
            .completion_for_text_with_trigger(
                document.text,
                params.text_document_position.position,
                params.context.map(|context| context.trigger_kind),
            )
            .await)
    }

    async fn completion_resolve(
        &self,
        params: CompletionItem,
    ) -> Result<CompletionItem> {
        Ok(params)
    }

    async fn on_type_formatting(
        &self,
        params: DocumentOnTypeFormattingParams,
    ) -> Result<Option<Vec<TextEdit>>> {
        let uri = params.text_document_position.text_document.uri;
        let Some(document) = self.document_for_uri(&uri) else {
            return Ok(None);
        };
        if !document.eligible {
            return Ok(None);
        }
        Ok(self.on_type_formatting_for_text_with_recent(
            document.text,
            params.text_document_position.position,
            &params.ch,
            document.recent_paren_insertion,
        ))
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri;
        let Some(document) = self.document_for_uri(&uri) else {
            return Ok(None);
        };
        if !document.eligible {
            return Ok(None);
        }
        Ok(self
            .hover_for_text(
                document.text,
                params.text_document_position_params.position,
            )
            .await)
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri;
        let Some(document) = self.document_for_uri(&uri) else {
            return Ok(None);
        };
        if !document.eligible {
            return Ok(None);
        }
        Ok(self
            .definition_for_text(
                document.text,
                params.text_document_position_params.position,
            )
            .await)
    }

    async fn code_action(
        &self,
        params: CodeActionParams,
    ) -> Result<Option<CodeActionResponse>> {
        let uri = params.text_document.uri;
        let Some(document) = self.document_for_uri(&uri) else {
            return Ok(None);
        };
        if !document.eligible {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(
            self.code_actions_for_text(uri, document.text, params.range)
                .await,
        ))
    }

    async fn execute_command(
        &self,
        params: ExecuteCommandParams,
    ) -> Result<Option<LSPAny>> {
        if params.command == REFRESH_COMMAND {
            self.refresh_catalog_explicit().await;
        } else if params.command == OPEN_SOURCE_COMMAND {
            self.client
                .log_message(MessageType::INFO, "open source command invoked")
                .await;
        }
        Ok(None)
    }

    async fn did_change_watched_files(
        &self,
        params: DidChangeWatchedFilesParams,
    ) {
        if params
            .changes
            .iter()
            .any(|change| should_invalidate_for_uri(&change.uri))
        {
            self.catalog_cache.invalidate_all();
            self.invalidate_artifact_ref_cache();
            self.invalidate_glossary_cache();
            self.request_semantic_tokens_refresh();
        }
    }
}

pub async fn run_stdio() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let (service, socket) = LspService::new(XpromptLspServer::new);
    Server::new(stdin, stdout, socket).serve(service).await;
}
