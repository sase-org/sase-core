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
use sase_core::editor::wire::MacroAssistEntry;
use sase_core::editor::{
    build_macro_arg_name_candidates, build_macro_completion_candidates,
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
    editor_build_vcs_project_completion_candidates_with_targets,
    editor_build_vcs_ref_completion_candidates,
    editor_build_vcs_repo_completion_candidates,
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
    VcsProjectEntry, VcsRepoCatalogResponse, VcsRepoEntry,
    MEMORY_NAMESPACE_SEGMENT,
};
use serde::Deserialize;
use tower_lsp_server::jsonrpc::Result;
use tower_lsp_server::{Client, LanguageServer, LspService, Server, UriExt};
use tracing::{info, warn};

use self::spacer as spacer_util;

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
const REFRESH_COMMAND_MACRO: &str = "sase.macroLsp.refreshCatalog";
const OPEN_SOURCE_COMMAND_MACRO: &str = "sase.macroLsp.openSource";
const ARTIFACT_REF_CACHE_TTL: Duration = Duration::from_secs(2);
const GLOSSARY_CACHE_TTL: Duration = Duration::from_secs(2);

/// Env var carrying the path to the JSON `vcs_project` completion catalog
/// (enabled-project entries + known VCS workflow names). Materialized by the
/// Python launcher (`integrations/xprompt_lsp.py`) at LSP startup and re-read
/// fresh on every `+` completion request so external rewrites are picked up.
// legacy xprompt spelling
const VCS_PROJECT_CATALOG_ENV: &str = "SASE_XPROMPT_VCS_PROJECT_CATALOG";
const VCS_PROJECT_CATALOG_MACRO_ENV: &str = "SASE_MACRO_VCS_PROJECT_CATALOG";
// legacy xprompt spelling
const MODEL_CATALOG_ENV: &str = "SASE_XPROMPT_MODEL_CATALOG";
const MODEL_CATALOG_MACRO_ENV: &str = "SASE_MACRO_MODEL_CATALOG";
// legacy xprompt spelling
const MACHINE_CATALOG_ENV: &str = "SASE_XPROMPT_MACHINE_CATALOG";
const MACHINE_CATALOG_MACRO_ENV: &str = "SASE_MACRO_MACHINE_CATALOG";
// legacy xprompt spelling
const ARTIFACT_REF_CATALOG_ENV: &str = "SASE_XPROMPT_ARTIFACT_REF_CATALOG";
const ARTIFACT_REF_CATALOG_MACRO_ENV: &str = "SASE_MACRO_ARTIFACT_REF_CATALOG";
// legacy xprompt spelling
const GLOSSARY_CATALOG_ENV: &str = "SASE_XPROMPT_GLOSSARY_CATALOG";
const GLOSSARY_CATALOG_MACRO_ENV: &str = "SASE_MACRO_GLOSSARY_CATALOG";
const TYPED_LAUNCH_UNITS_ENV: &str = "SASE_TYPED_LAUNCH_UNITS";
const QUEUE_CAPACITY_BUDGET_ENV: &str = "SASE_QUEUE_CAPACITY_BUDGET";

mod actions;
mod catalogs;
mod completion;
mod completion_items;
mod documents;
mod initialize;
pub(crate) mod jinja;
mod spacer;
mod state;

#[cfg(test)]
mod tests;

pub use state::{MacroLspServer, XpromptLspServer};

use self::actions::should_invalidate_for_uri;
use self::catalogs::load_vcs_project_catalog;
use self::initialize::config_from_initialize;

impl LanguageServer for MacroLspServer {
    async fn initialize(
        &self,
        params: InitializeParams,
    ) -> Result<InitializeResult> {
        let config = config_from_initialize(&params);
        let project_tag_palette =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref())
                .accent_palette;
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
                        "{".to_string(),
                        "|".to_string(),
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
                        REFRESH_COMMAND_MACRO.to_string(),
                        OPEN_SOURCE_COMMAND_MACRO.to_string(),
                        spacer::ACCEPT_COMMAND.to_string(),
                        spacer::ACCEPT_COMMAND_MACRO.to_string(),
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
                experimental: Some(serde_json::json!({
                    "sase": {
                        "projectTagPalette": project_tag_palette,
                    },
                })),
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
        // Confirmed pending transition (`#optional (` or `#optional ()`):
        // derive argument completion from a temporary normalized document
        // with the owned space removed, then map candidate ranges back.
        // Only for a confirmed acceptance; arbitrary prose resembling the
        // transition never takes this path.
        let (trigger_kind_opt, trigger_character_opt) = match &params.context {
            Some(context) => (
                Some(context.trigger_kind),
                context.trigger_character.clone(),
            ),
            None => (None, None),
        };
        if let Some(confirmed) = document.confirmed_spacer.clone() {
            if let Some(spacer_byte) =
                spacer_util::transition_spacer_byte(&document.text, &confirmed)
            {
                if let Some(mapped) = self
                    .transition_argument_completion(
                        &uri,
                        &document,
                        &confirmed,
                        spacer_byte,
                        params.text_document_position.position,
                        trigger_kind_opt,
                        trigger_character_opt.clone(),
                    )
                    .await
                {
                    return Ok(Some(mapped));
                }
                // Fall through to the ordinary route when the transition
                // view cannot be built; never use normalized coordinates.
            }
        }
        let source_path = uri.to_file_path().map(|path| path.into_owned());
        let (trigger_kind, trigger_character) =
            (trigger_kind_opt, trigger_character_opt);
        let response = self
            .completion_for_document(
                document.text.clone(),
                params.text_document_position.position,
                source_path,
                &document.language_id,
                trigger_kind,
                trigger_character,
            )
            .await;
        let mut response = match response {
            Some(response) => response,
            None => return Ok(None),
        };
        self.attach_spacer_acceptance(&uri, &document, &mut response)
            .await;
        Ok(Some(response))
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
        // Confirmed owned spacer first; the shared core planner validates the
        // exact reference, single space, adjacency, and excluded regions.
        // Preserves the typed opener, any editor-inserted closer, and every
        // suffix character. Repeated requests on one unchanged snapshot stay
        // consistent; the state clears only on the normalizing `didChange`.
        if let Some(edits) = self.spacer_on_type(
            &document,
            params.text_document_position.position,
            &params.ch,
        ) {
            return Ok(Some(edits));
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
        let source_path = uri.to_file_path().map(|path| path.into_owned());
        Ok(self
            .hover_for_document(
                document.text,
                params.text_document_position_params.position,
                source_path,
                &document.language_id,
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
        if params.command == REFRESH_COMMAND
            || params.command == REFRESH_COMMAND_MACRO
        {
            self.refresh_catalog_explicit().await;
        } else if params.command == OPEN_SOURCE_COMMAND
            || params.command == OPEN_SOURCE_COMMAND_MACRO
        {
            self.client
                .log_message(MessageType::INFO, "open source command invoked")
                .await;
        } else if params.command == spacer::ACCEPT_COMMAND
            || params.command == spacer::ACCEPT_COMMAND_MACRO
        {
            // Server-owned acceptance: the only effect is to record that the
            // user accepted an eligible completion. The later on-type
            // response performs the edit. Rejects stale commands after
            // unrelated changes or document close/reopen, and never infers
            // acceptance from buffer text alone.
            for argument in params.arguments {
                if self.handle_accept_command(&argument) {
                    break;
                }
            }
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
    let (service, socket) = LspService::new(MacroLspServer::new);
    Server::new(stdin, stdout, socket).serve(service).await;
}
