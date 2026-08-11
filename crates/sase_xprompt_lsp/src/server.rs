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
use sase_core::{
    editor_analyze_artifact_refs, editor_analyze_document,
    editor_build_agent_completion_candidates,
    editor_build_artifact_ref_payload_inventory,
    editor_build_at_reference_menu_with_options,
    editor_build_directive_completion_candidates,
    editor_build_file_completion_candidates_with_base,
    editor_build_file_history_completion_candidates,
    editor_build_placeholder_completion_candidates,
    editor_build_snippet_completion_candidates,
    editor_build_vcs_project_completion_candidates,
    editor_build_vcs_ref_completion_candidates,
    editor_build_vcs_repo_completion_candidates,
    editor_build_wait_completion_candidates,
    editor_build_xprompt_arg_name_candidates,
    editor_build_xprompt_completion_candidates,
    editor_classify_completion_context_with_artifacts_and_workflows,
    editor_classify_completion_context_with_workflows,
    editor_definition_at_position, editor_detect_at_reference_context,
    editor_directive_argument_candidates, editor_directive_metadata,
    editor_extract_token_at_position, editor_hover_at_position,
    ArtifactRefContextWire, AtReferenceContextWire, AtReferenceInventoryWire,
    AtReferenceKindRowWire, AtReferenceMenuOptionsWire, AtReferencePathRowWire,
    AtReferencePayloadIndex, AtReferenceStage, CompiledGlossaryCatalog,
    CompletionCandidate, CompletionContextKind, CompletionList,
    DocumentSnapshot, EditorRange, EditorSnippetEntryWire, GlossaryCatalogWire,
    GlossaryEntryWire, GlossarySpanWire, HelperHostBridge, HoverPayload,
    VcsNamespaceEntry, VcsProjectEntry, VcsRepoCatalogResponse, VcsRepoEntry,
    XpromptAssistEntry, MEMORY_NAMESPACE_SEGMENT,
};
use serde::Deserialize;
use tower_lsp_server::jsonrpc::Result;
use tower_lsp_server::{Client, LanguageServer, LspService, Server, UriExt};
use tracing::{info, warn};

use crate::catalog_cache::{CatalogCache, CatalogFailure};
use crate::lsp_convert::{
    agent_completion_response, apply_replacement,
    at_reference_completion_response, completion_response,
    diagnostic as lsp_diagnostic, hover as lsp_hover,
    model_completion_response, placeholder_completion_response,
    sase_snippet_completion_item, snippet_completion_item, to_editor_position,
    to_lsp_range, vcs_project_completion_response, vcs_ref_completion_response,
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
const ARTIFACT_REF_CATALOG_ENV: &str = "SASE_XPROMPT_ARTIFACT_REF_CATALOG";
const GLOSSARY_CATALOG_ENV: &str = "SASE_XPROMPT_GLOSSARY_CATALOG";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerConfig {
    root_dir: Option<PathBuf>,
    project: Option<String>,
    catalog_key: String,
    snippet_support: bool,
    allow_all_markdown: bool,
    /// Path to the materialized `vcs_project` completion catalog, captured from
    /// [`VCS_PROJECT_CATALOG_ENV`] at startup. The file itself is re-read fresh
    /// on each `+` completion request (see [`load_vcs_project_catalog`]).
    vcs_project_catalog: Option<PathBuf>,
    /// Path to the materialized `%model` completion catalog, captured from
    /// [`MODEL_CATALOG_ENV`] at startup. The file itself is re-read fresh on
    /// each `%model` argument completion request.
    model_catalog: Option<PathBuf>,
    /// Path to the launcher-materialized artifact-reference catalog. The file
    /// and its enumerated payload inventories are cached briefly; path metadata
    /// changes and explicit refreshes invalidate the cache immediately.
    artifact_ref_catalog: Option<PathBuf>,
    /// Path to the launcher-materialized project glossary catalog. Parsed
    /// catalogs are cached briefly and invalidated by file signature, explicit
    /// refresh, or watched project config changes.
    glossary_catalog: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            root_dir: std::env::current_dir().ok(),
            project: None,
            catalog_key: "default".to_string(),
            snippet_support: false,
            allow_all_markdown: false,
            vcs_project_catalog: vcs_project_catalog_path(),
            model_catalog: model_catalog_path(),
            artifact_ref_catalog: artifact_ref_catalog_path(),
            glossary_catalog: glossary_catalog_path(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelCompletionEntry {
    value: String,
    display: String,
    description: String,
    kind: String,
    provider: String,
    aliases: Vec<String>,
    alias_kind: String,
    target_provider: String,
    target_model: String,
    target_effort: String,
    provenance: String,
    reference: String,
    reference_effort: String,
    selector_mode: String,
    pool_available: u64,
    pool_total: u64,
    config_source: String,
    bucket: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct VcsProjectCatalog {
    entries: Vec<VcsProjectEntry>,
    workflow_names: Vec<String>,
    namespaces: HashMap<String, Vec<VcsNamespaceEntry>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ArtifactRefCatalog {
    default_project: Option<String>,
    projects: Vec<ArtifactRefCatalogProject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ArtifactRefCatalogProject {
    name: String,
    key: String,
    #[serde(default)]
    aliases: Vec<String>,
    context: ArtifactRefContextWire,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArtifactRefCatalogSignature {
    path: Option<PathBuf>,
    modified: Option<SystemTime>,
    len: u64,
}

#[derive(Debug)]
struct CachedArtifactRefPayload {
    index: AtReferencePayloadIndex,
    truncated_payloads: usize,
}

#[derive(Debug, Default)]
struct ArtifactRefCache {
    signature: Option<ArtifactRefCatalogSignature>,
    loaded_at: Option<Instant>,
    catalog: ArtifactRefCatalog,
    payloads: HashMap<(String, String), Arc<CachedArtifactRefPayload>>,
}

#[derive(Debug, Clone, Default)]
struct GlossaryCatalog {
    default_project: Option<String>,
    projects: Vec<GlossaryCatalogProject>,
}

#[derive(Debug, Clone)]
struct GlossaryCatalogProject {
    key: String,
    name: String,
    aliases: Vec<String>,
    config_path: String,
    catalog: Arc<CompiledGlossaryCatalog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct GlossaryCatalogProjectPayload {
    schema_version: u32,
    project: GlossaryCatalogProjectIdentity,
    config_path: String,
    entries: Vec<GlossaryEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct GlossaryCatalogProjectIdentity {
    key: String,
    name: String,
    #[serde(default)]
    aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GlossaryCatalogSignature {
    path: Option<PathBuf>,
    modified: Option<SystemTime>,
    len: u64,
}

#[derive(Debug, Default)]
struct GlossaryCache {
    signature: Option<GlossaryCatalogSignature>,
    loaded_at: Option<Instant>,
    catalog: GlossaryCatalog,
}

#[derive(Debug, Clone)]
struct OpenDocument {
    text: String,
    language_id: String,
    eligible: bool,
}

#[derive(Debug)]
pub struct XpromptLspServer {
    client: Client,
    documents: RwLock<HashMap<String, OpenDocument>>,
    catalog_cache: Arc<CatalogCache>,
    artifact_ref_cache: RwLock<ArtifactRefCache>,
    glossary_cache: RwLock<GlossaryCache>,
    config: RwLock<ServerConfig>,
}

impl XpromptLspServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            documents: RwLock::new(HashMap::new()),
            catalog_cache: Arc::new(CatalogCache::command_backed()),
            artifact_ref_cache: RwLock::new(ArtifactRefCache::default()),
            glossary_cache: RwLock::new(GlossaryCache::default()),
            config: RwLock::new(ServerConfig::default()),
        }
    }

    pub fn with_bridge(
        client: Client,
        bridge: Arc<dyn HelperHostBridge>,
    ) -> Self {
        Self {
            client,
            documents: RwLock::new(HashMap::new()),
            catalog_cache: Arc::new(CatalogCache::new(bridge)),
            artifact_ref_cache: RwLock::new(ArtifactRefCache::default()),
            glossary_cache: RwLock::new(GlossaryCache::default()),
            config: RwLock::new(ServerConfig::default()),
        }
    }

    pub async fn completion_for_text(
        &self,
        text: String,
        position: Position,
    ) -> Option<CompletionResponse> {
        self.completion_for_text_with_trigger(text, position, None)
            .await
    }

    pub async fn completion_for_text_with_trigger(
        &self,
        text: String,
        position: Position,
        trigger: Option<CompletionTriggerKind>,
    ) -> Option<CompletionResponse> {
        let config = self.current_config();
        let document = DocumentSnapshot::new(text);
        let editor_position = to_editor_position(position);

        // Placeholder completion is document-local. Classify it before any
        // catalog refresh so this source never depends on the helper bridge.
        if let Some(context) =
            editor_classify_completion_context_with_workflows(
                &document,
                editor_position,
                &[],
                &[],
            )
            .filter(|context| {
                context.kind == CompletionContextKind::Placeholder
            })
        {
            let list = self.completion_list_for_context(
                &context,
                &[],
                &config,
                &document,
                position,
                None,
            );
            let prefix = context
                .token
                .as_ref()
                .map(|token| token.text.as_str())
                .unwrap_or_default();
            return Some(placeholder_completion_response(
                list,
                context.replacement_range,
                prefix,
            ));
        }

        let vcs_catalog =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let artifact_catalog =
            self.artifact_ref_catalog(config.artifact_ref_catalog.as_deref());
        let artifact_project = active_artifact_ref_project(
            &document,
            &config,
            &vcs_catalog,
            &artifact_catalog,
        );
        let artifact_context = artifact_project.map(|project| &project.context);
        let known_kinds = known_at_reference_kinds(artifact_context);
        if let Some(context) = editor_detect_at_reference_context(
            &document,
            editor_position,
            &known_kinds,
        ) {
            return Some(self.at_reference_completion(
                &context,
                artifact_project,
                &config,
                &document,
                AtReferenceMenuOptionsWire {
                    include_files: trigger
                        == Some(CompletionTriggerKind::INVOKED),
                },
            ));
        }

        let entries = self.entries_for_completion(&config).await;
        let context =
            editor_classify_completion_context_with_artifacts_and_workflows(
                &document,
                editor_position,
                entries.as_slice(),
                &vcs_catalog.workflow_names,
                artifact_context,
            )?;
        if context.kind == CompletionContextKind::VcsProject {
            return Some(self.vcs_project_completion(
                &context, &document, position, &config,
            ));
        }
        if context.kind == CompletionContextKind::VcsRepo {
            return Some(self.vcs_repo_completion(&context, &document).await);
        }
        if context.kind == CompletionContextKind::VcsRef {
            return Some(self.vcs_ref_completion(
                &context,
                &document,
                &vcs_catalog,
            ));
        }
        if context.kind == CompletionContextKind::XpromptArgumentAgent
            || (context.kind == CompletionContextKind::DirectiveArgument
                && context.directive_name.as_deref() == Some("wait"))
        {
            return Some(self.agent_completion(&context).await);
        }
        let list = self.completion_list_for_context(
            &context,
            &entries,
            &config,
            &document,
            position,
            artifact_project,
        );
        if context.kind == CompletionContextKind::Placeholder {
            let prefix = context
                .token
                .as_ref()
                .map(|token| token.text.as_str())
                .unwrap_or_default();
            return Some(placeholder_completion_response(
                list,
                context.replacement_range,
                prefix,
            ));
        }
        if context.kind == CompletionContextKind::SnippetTrigger {
            if !config.snippet_support {
                return Some(CompletionResponse::Array(Vec::new()));
            }
            let snippets = self.snippets_for_completion(&config).await;
            let token = context
                .token
                .as_ref()
                .map(|token| token.text.as_str())
                .unwrap_or_default();
            let snippet_list = editor_build_snippet_completion_candidates(
                token,
                Some(context.replacement_range),
                snippets.as_slice(),
            );
            return Some(CompletionResponse::Array(sase_snippet_items(
                snippet_list,
                context.replacement_range,
            )));
        }
        if config.snippet_support
            && context.kind == CompletionContextKind::Xprompt
        {
            let append_text_arg_space =
                replacement_ends_line(&document, context.replacement_range);
            return Some(CompletionResponse::Array(xprompt_snippet_items(
                list,
                entries.as_slice(),
                context.replacement_range,
                append_text_arg_space,
            )));
        }
        if context.kind == CompletionContextKind::DirectiveArgument
            && context.directive_name.as_deref() == Some("model")
        {
            return Some(model_completion_response(
                list,
                context.replacement_range,
            ));
        }
        let mut response = completion_response(list, context.replacement_range);
        if config.snippet_support
            && context.kind == CompletionContextKind::DirectiveName
        {
            if let CompletionResponse::Array(items) = &mut response {
                items.extend(directive_snippet_items(
                    context.token.as_ref().map(|token| token.text.as_str()),
                    context.replacement_range,
                ));
            }
        }
        Some(response)
    }

    fn at_reference_completion(
        &self,
        context: &AtReferenceContextWire,
        artifact_project: Option<&ArtifactRefCatalogProject>,
        config: &ServerConfig,
        document: &DocumentSnapshot,
        options: AtReferenceMenuOptionsWire,
    ) -> CompletionResponse {
        let Some(replacement_range) = document.byte_range_to_range(
            context.candidate_span.0,
            context.candidate_span.1,
        ) else {
            return empty_completion_response();
        };
        let artifact_context = artifact_project.map(|project| &project.context);
        let payload = artifact_project.and_then(|project| {
            self.cached_at_reference_payload_inventory(context, project)
        });
        let inventory = AtReferenceInventoryWire {
            kinds: at_reference_kind_inventory(artifact_context),
            paths: at_reference_path_inventory(context, config),
            truncated_payloads: payload
                .as_ref()
                .map_or(0, |payload| payload.truncated_payloads),
            ..Default::default()
        };
        let mut menu = editor_build_at_reference_menu_with_options(
            context,
            &inventory,
            payload.as_ref().map(|payload| &payload.index),
            options,
        );
        menu.truncated_payloads = menu
            .truncated_payloads
            .saturating_add(menu.payload_count.saturating_sub(menu.rows.len()));
        at_reference_completion_response(menu, context, replacement_range)
    }

    /// Build the `+` (`vcs_project`) completion response.
    ///
    /// The project catalog (enabled-project entries + known VCS workflow names)
    /// is read fresh from the materialized JSON file on every request so
    /// external rewrites are picked up without restarting the server. The
    /// canonical expansion is produced by the shared core builder, keeping the
    /// LSP byte-for-byte aligned with the TUI and the Python golden vectors.
    fn vcs_project_completion(
        &self,
        context: &sase_core::CompletionContext,
        document: &DocumentSnapshot,
        position: Position,
        config: &ServerConfig,
    ) -> CompletionResponse {
        let Some(token) = context.token.as_ref() else {
            return CompletionResponse::Array(Vec::new());
        };
        let vcs_catalog =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let list = editor_build_vcs_project_completion_candidates(
            token,
            document,
            to_editor_position(position),
            &vcs_catalog.entries,
            &vcs_catalog.workflow_names,
        );
        vcs_project_completion_response(list, context.replacement_range)
    }

    /// Build the `#workflow:` / `#workflow(` root-ref completion response.
    ///
    /// The enabled project/PR rows and optional namespace rows come from the
    /// materialized catalog already loaded for context classification. No helper
    /// bridge call is needed on this completion path.
    fn vcs_ref_completion(
        &self,
        context: &sase_core::CompletionContext,
        document: &DocumentSnapshot,
        vcs_catalog: &VcsProjectCatalog,
    ) -> CompletionResponse {
        let Some(trigger) = context.vcs_ref.as_ref() else {
            return empty_completion_response();
        };
        let namespaces = vcs_catalog
            .namespaces
            .get(&trigger.workflow)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let list = editor_build_vcs_ref_completion_candidates(
            document,
            context,
            &vcs_catalog.entries,
            namespaces,
        );
        vcs_ref_completion_response(list, context.replacement_range)
    }

    async fn vcs_repo_completion(
        &self,
        context: &sase_core::CompletionContext,
        document: &DocumentSnapshot,
    ) -> CompletionResponse {
        let Some(trigger) = context.vcs_repo.as_ref() else {
            return empty_completion_response();
        };
        let Some(response) = self
            .vcs_repo_catalog_for_completion(
                &trigger.workflow,
                &trigger.namespace,
            )
            .await
        else {
            return empty_completion_response();
        };
        if response.entries.is_empty() {
            if response.status != "ok" && !response.message.is_empty() {
                warn!(
                    "vcs repo catalog returned no entries: {}",
                    response.message
                );
            }
            return empty_completion_response();
        }

        let entries =
            ranked_vcs_repo_entries(&response.entries, &trigger.query);
        let list = editor_build_vcs_repo_completion_candidates(
            document, context, &entries,
        );
        vcs_repo_completion_response(list, context.replacement_range, &entries)
    }

    async fn agent_completion(
        &self,
        context: &sase_core::CompletionContext,
    ) -> CompletionResponse {
        let response =
            match self.catalog_cache.agent_catalog_for_completion().await {
                Ok(response) => response,
                Err(error) => {
                    self.warn_once(&error).await;
                    return empty_completion_response();
                }
            };
        if response.status != "ok" {
            if !response.message.is_empty() {
                warn!(
                    "agent catalog returned no entries: {}",
                    response.message
                );
            }
            return empty_completion_response();
        }
        let token = context
            .token
            .as_ref()
            .map(|token| token.text.as_str())
            .unwrap_or_default();
        let list = if context.directive_name.as_deref() == Some("wait") {
            editor_build_wait_completion_candidates(
                token,
                None,
                &response.entries,
                &context.selected_values,
            )
        } else {
            editor_build_agent_completion_candidates(
                token,
                None,
                &response.entries,
                &context.selected_values,
            )
        };
        agent_completion_response(list, context.replacement_range)
    }

    async fn vcs_repo_catalog_for_completion(
        &self,
        workflow: &str,
        namespace: &str,
    ) -> Option<Arc<VcsRepoCatalogResponse>> {
        if !self
            .catalog_cache
            .vcs_repo_catalog_stale_or_missing(workflow, namespace)
        {
            if let Some(response) = self
                .catalog_cache
                .cached_vcs_repo_catalog(workflow, namespace)
            {
                return Some(response);
            }
        }

        match self
            .catalog_cache
            .refresh_vcs_repo_for_completion(
                workflow.to_string(),
                namespace.to_string(),
            )
            .await
        {
            Ok(response) => Some(response),
            Err(error) => {
                self.warn_once(&error).await;
                self.catalog_cache
                    .cached_vcs_repo_catalog(workflow, namespace)
            }
        }
    }

    pub async fn hover_for_text(
        &self,
        text: String,
        position: Position,
    ) -> Option<Hover> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
        if let Some(hover) = editor_hover_at_position(
            &document,
            to_editor_position(position),
            entries.as_slice(),
        ) {
            return Some(lsp_hover(hover));
        }

        let vcs_catalog =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let glossary_catalog =
            self.glossary_catalog(config.glossary_catalog.as_deref());
        active_glossary_project(
            &document,
            &config,
            &vcs_catalog,
            &glossary_catalog,
        )
        .and_then(|project| {
            glossary_hover_at_position(
                &document,
                to_editor_position(position),
                project,
            )
        })
        .map(lsp_hover)
    }

    pub async fn diagnostics_for_text(
        &self,
        text: String,
    ) -> Vec<lsp_types::Diagnostic> {
        self.diagnostics_for_document(DocumentSnapshot::new(text))
            .await
    }

    pub async fn diagnostics_for_uri_text(
        &self,
        uri: &Uri,
        text: String,
    ) -> Vec<lsp_types::Diagnostic> {
        let document = if let Some(path) = uri.to_file_path() {
            DocumentSnapshot::with_source_path(text, path.into_owned())
        } else {
            DocumentSnapshot::new(text)
        };
        self.diagnostics_for_document(document).await
    }

    async fn diagnostics_for_document(
        &self,
        document: DocumentSnapshot,
    ) -> Vec<lsp_types::Diagnostic> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let mut diagnostics =
            editor_analyze_document(&document, entries.as_slice());
        let vcs_catalog =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let artifact_catalog =
            self.artifact_ref_catalog(config.artifact_ref_catalog.as_deref());
        if let Some(context) = active_artifact_ref_context(
            &document,
            &config,
            &vcs_catalog,
            &artifact_catalog,
        ) {
            diagnostics
                .extend(editor_analyze_artifact_refs(&document, context));
        }
        diagnostics.into_iter().map(lsp_diagnostic).collect()
    }

    pub fn semantic_tokens_for_text(&self, text: String) -> SemanticTokens {
        self.semantic_tokens_for_document(DocumentSnapshot::new(text))
    }

    pub fn semantic_tokens_for_uri_text(
        &self,
        uri: &Uri,
        text: String,
    ) -> SemanticTokens {
        let document = if let Some(path) = uri.to_file_path() {
            DocumentSnapshot::with_source_path(text, path.into_owned())
        } else {
            DocumentSnapshot::new(text)
        };
        self.semantic_tokens_for_document(document)
    }

    fn semantic_tokens_for_document(
        &self,
        document: DocumentSnapshot,
    ) -> SemanticTokens {
        let config = self.current_config();
        let vcs_catalog =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let artifact_catalog =
            self.artifact_ref_catalog(config.artifact_ref_catalog.as_deref());
        let glossary_catalog =
            self.glossary_catalog(config.glossary_catalog.as_deref());
        let artifact_context = active_artifact_ref_context(
            &document,
            &config,
            &vcs_catalog,
            &artifact_catalog,
        );
        let glossary_project = active_glossary_project(
            &document,
            &config,
            &vcs_catalog,
            &glossary_catalog,
        );
        document_semantic_tokens(
            &document,
            artifact_context,
            glossary_project.map(|project| project.catalog.as_ref()),
        )
    }

    pub async fn code_actions_for_text(
        &self,
        uri: Uri,
        text: String,
        range: Range,
    ) -> CodeActionResponse {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
        let position = to_editor_position(range.start);
        let mut actions = Vec::new();

        if let Some(token) =
            editor_extract_token_at_position(&document, position)
        {
            if let Some(entry) =
                entry_for_token(&token.text, entries.as_slice())
            {
                if token.text.starts_with('#') {
                    if let Some(action) = canonical_marker_action(
                        &uri,
                        token.range,
                        &token.text,
                        entry,
                    ) {
                        actions.push(action.into());
                    }
                    if !entry.inputs.is_empty() {
                        actions.push(
                            text_edit_action(
                                "Insert required named args",
                                &uri,
                                token.range,
                                plain_named_args_skeleton(entry),
                                CodeActionKind::REFACTOR_REWRITE,
                                false,
                            )
                            .into(),
                        );
                        actions.push(
                            text_edit_action(
                                "Insert colon arg skeleton",
                                &uri,
                                token.range,
                                format!("{}:", entry.insertion),
                                CodeActionKind::REFACTOR_REWRITE,
                                false,
                            )
                            .into(),
                        );
                    }
                }
                if let Some(source_uri) =
                    definition_uri_at_position(&document, position, &entries)
                {
                    actions.push(
                        CodeAction {
                            title: "Open xprompt source".to_string(),
                            kind: Some(CodeActionKind::SOURCE),
                            command: Some(Command::new(
                                "Open xprompt source".to_string(),
                                OPEN_SOURCE_COMMAND.to_string(),
                                Some(vec![serde_json::json!(
                                    source_uri.to_string()
                                )]),
                            )),
                            ..Default::default()
                        }
                        .into(),
                    );
                }
            }
        }

        actions.push(CodeActionOrCommand::Command(Command::new(
            "Refresh xprompt catalog".to_string(),
            REFRESH_COMMAND.to_string(),
            None,
        )));
        actions
    }

    pub async fn definition_for_text(
        &self,
        text: String,
        position: Position,
    ) -> Option<GotoDefinitionResponse> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
        if let Some(target) = editor_definition_at_position(
            &document,
            to_editor_position(position),
            entries.as_slice(),
        ) {
            let uri = Uri::from_file_path(target.path)?;
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri,
                range: target
                    .range
                    .map(to_lsp_range)
                    .unwrap_or_else(zero_range),
            }));
        }

        let vcs_catalog =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let glossary_catalog =
            self.glossary_catalog(config.glossary_catalog.as_deref());
        active_glossary_project(
            &document,
            &config,
            &vcs_catalog,
            &glossary_catalog,
        )
        .and_then(|project| {
            glossary_definition_at_position(
                &document,
                to_editor_position(position),
                project,
            )
        })
        .map(GotoDefinitionResponse::Scalar)
    }

    fn current_config(&self) -> ServerConfig {
        self.config
            .read()
            .map(|config| config.clone())
            .unwrap_or_default()
    }

    fn artifact_ref_catalog(&self, path: Option<&Path>) -> ArtifactRefCatalog {
        let signature = artifact_ref_catalog_signature(path);
        let now = Instant::now();
        if let Ok(cache) = self.artifact_ref_cache.read() {
            let fresh = cache.signature.as_ref() == Some(&signature)
                && cache.loaded_at.is_some_and(|loaded_at| {
                    now.saturating_duration_since(loaded_at)
                        < ARTIFACT_REF_CACHE_TTL
                });
            if fresh {
                return cache.catalog.clone();
            }
        }

        let catalog = load_artifact_ref_catalog(path);
        if let Ok(mut cache) = self.artifact_ref_cache.write() {
            cache.signature = Some(signature);
            cache.loaded_at = Some(now);
            cache.catalog = catalog.clone();
            cache.payloads.clear();
        }
        catalog
    }

    fn glossary_catalog(&self, path: Option<&Path>) -> GlossaryCatalog {
        let signature = glossary_catalog_signature(path);
        let now = Instant::now();
        if let Ok(cache) = self.glossary_cache.read() {
            let fresh = cache.signature.as_ref() == Some(&signature)
                && cache.loaded_at.is_some_and(|loaded_at| {
                    now.saturating_duration_since(loaded_at)
                        < GLOSSARY_CACHE_TTL
                });
            if fresh {
                return cache.catalog.clone();
            }
        }

        let catalog = load_glossary_catalog(path);
        if let Ok(mut cache) = self.glossary_cache.write() {
            cache.signature = Some(signature);
            cache.loaded_at = Some(now);
            cache.catalog = catalog.clone();
        }
        catalog
    }

    fn cached_at_reference_payload_inventory(
        &self,
        context: &AtReferenceContextWire,
        project: &ArtifactRefCatalogProject,
    ) -> Option<Arc<CachedArtifactRefPayload>> {
        if context.stage != AtReferenceStage::Payload {
            return None;
        }
        let kind = context.kind.as_deref()?;
        if kind == "bug" {
            return None;
        }
        let key = (project.key.clone(), kind.to_string());
        if let Ok(cache) = self.artifact_ref_cache.read() {
            if let Some(payload) = cache.payloads.get(&key) {
                return Some(Arc::clone(payload));
            }
        }

        let Ok(inventory) =
            editor_build_artifact_ref_payload_inventory(kind, &project.context)
        else {
            return None;
        };
        let payload = Arc::new(CachedArtifactRefPayload {
            index: AtReferencePayloadIndex::new(inventory.payloads),
            truncated_payloads: inventory.truncated_payloads,
        });
        if let Ok(mut cache) = self.artifact_ref_cache.write() {
            return Some(Arc::clone(
                cache
                    .payloads
                    .entry(key)
                    .or_insert_with(|| Arc::clone(&payload)),
            ));
        }
        Some(payload)
    }

    fn invalidate_artifact_ref_cache(&self) {
        if let Ok(mut cache) = self.artifact_ref_cache.write() {
            *cache = ArtifactRefCache::default();
        }
    }

    fn invalidate_glossary_cache(&self) {
        if let Ok(mut cache) = self.glossary_cache.write() {
            *cache = GlossaryCache::default();
        }
    }

    fn open_document(
        &self,
        uri: &Uri,
        language_id: String,
        text: String,
    ) -> OpenDocument {
        let config = self.current_config();
        OpenDocument {
            eligible: document_eligible(uri, &language_id, &config),
            language_id,
            text,
        }
    }

    fn document_for_uri(&self, uri: &Uri) -> Option<OpenDocument> {
        self.documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri.to_string()).cloned())
    }

    async fn entries_for_completion(
        &self,
        config: &ServerConfig,
    ) -> Arc<Vec<XpromptAssistEntry>> {
        if !self.catalog_cache.stale_or_missing(&config.catalog_key) {
            if let Some(entries) =
                self.catalog_cache.cached_entries(&config.catalog_key)
            {
                return entries;
            }
        }

        match self
            .catalog_cache
            .refresh_for_completion(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await
        {
            Ok(entries) => entries,
            Err(error) => {
                self.warn_once(&error).await;
                self.catalog_cache
                    .cached_entries(&config.catalog_key)
                    .unwrap_or_else(|| Arc::new(Vec::new()))
            }
        }
    }

    async fn snippets_for_completion(
        &self,
        config: &ServerConfig,
    ) -> Arc<Vec<EditorSnippetEntryWire>> {
        if !self
            .catalog_cache
            .snippets_stale_or_missing(&config.catalog_key)
        {
            if let Some(entries) = self
                .catalog_cache
                .cached_snippet_entries(&config.catalog_key)
            {
                return entries;
            }
        }

        match self
            .catalog_cache
            .refresh_snippets_for_completion(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await
        {
            Ok(entries) => entries,
            Err(error) => {
                self.warn_once(&error).await;
                self.catalog_cache
                    .cached_snippet_entries(&config.catalog_key)
                    .unwrap_or_else(|| Arc::new(Vec::new()))
            }
        }
    }

    fn completion_list_for_context(
        &self,
        context: &sase_core::CompletionContext,
        entries: &[XpromptAssistEntry],
        config: &ServerConfig,
        document: &DocumentSnapshot,
        position: Position,
        _artifact_project: Option<&ArtifactRefCatalogProject>,
    ) -> CompletionList {
        let token = context
            .token
            .as_ref()
            .map(|token| token.text.as_str())
            .unwrap_or_default();
        let list = match context.kind {
            CompletionContextKind::Placeholder => {
                editor_build_placeholder_completion_candidates(
                    document,
                    to_editor_position(position),
                    // The LSP has no common-placeholder source of its own.
                    &[],
                )
                .map(|completion| completion.into_completion_list())
                .unwrap_or_else(empty_completion_list)
            }
            CompletionContextKind::ArtifactRefKind
            | CompletionContextKind::ArtifactRefPayload => {
                empty_completion_list()
            }
            CompletionContextKind::Xprompt
            | CompletionContextKind::SlashSkill => {
                editor_build_xprompt_completion_candidates(
                    token,
                    Some(context.replacement_range),
                    entries,
                )
            }
            CompletionContextKind::FilePath
            | CompletionContextKind::XpromptArgumentPath => {
                editor_build_file_completion_candidates_with_base(
                    token,
                    config.root_dir.as_deref(),
                )
            }
            CompletionContextKind::FileHistory => {
                editor_build_file_history_completion_candidates(file_history())
            }
            CompletionContextKind::DirectiveName => {
                editor_build_directive_completion_candidates(token)
            }
            CompletionContextKind::DirectiveArgument => context
                .directive_name
                .as_deref()
                .map(|name| {
                    if name == "model" {
                        model_completion_list(
                            token,
                            config.model_catalog.as_deref(),
                        )
                    } else if matches!(name, "clan" | "id") {
                        empty_completion_list()
                    } else {
                        editor_directive_argument_candidates(name)
                    }
                })
                .unwrap_or_else(empty_completion_list),
            CompletionContextKind::DirectiveArgumentKeyword => context
                .directive_name
                .as_deref()
                .map(editor_directive_argument_candidates)
                .unwrap_or_else(empty_completion_list),
            CompletionContextKind::XpromptArgumentName => context
                .active_xprompt
                .as_deref()
                .and_then(|name| {
                    entries.iter().find(|entry| entry.name == name)
                })
                .map(|entry| {
                    editor_build_xprompt_arg_name_candidates(
                        entry,
                        &Default::default(),
                        token,
                        Some(context.replacement_range),
                    )
                })
                .unwrap_or_else(empty_completion_list),
            CompletionContextKind::XpromptArgumentValue => {
                bool_completion_list()
            }
            CompletionContextKind::XpromptArgumentAgent => {
                empty_completion_list()
            }
            CompletionContextKind::XpromptArgumentTypeHint => {
                empty_completion_list()
            }
            CompletionContextKind::SnippetTrigger => empty_completion_list(),
            // Handled out-of-band in `completion_for_text` /
            // `vcs_repo_completion`, which fetches helper-bridge candidates
            // asynchronously before using the core accept-edit builder.
            CompletionContextKind::VcsRepo => empty_completion_list(),
            // Handled out-of-band in `completion_for_text` /
            // `vcs_project_completion`, which loads the materialized project
            // catalog and known workflow names the core builder needs.
            CompletionContextKind::VcsProject => empty_completion_list(),
            // Handled out-of-band in `completion_for_text` /
            // `vcs_ref_completion`, which uses the materialized project and
            // namespace catalog.
            CompletionContextKind::VcsRef => empty_completion_list(),
        };
        apply_replacement(list, context.replacement_range)
    }

    async fn refresh_catalog_explicit(&self) {
        self.invalidate_artifact_ref_cache();
        self.invalidate_glossary_cache();
        let config = self.current_config();
        let xprompt_result = self
            .catalog_cache
            .refresh_explicit(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await;
        let snippet_result = self
            .catalog_cache
            .refresh_snippets_explicit(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await;

        match (xprompt_result, snippet_result) {
            (Ok(entries), Ok(snippets)) => {
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!(
                            "refreshed {} xprompt entries and {} snippets",
                            entries.len(),
                            snippets.len()
                        ),
                    )
                    .await;
            }
            (Ok(entries), Err(snippet_error)) => {
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!("refreshed {} xprompt entries", entries.len()),
                    )
                    .await;
                self.warn_once(&snippet_error).await;
            }
            (Err(xprompt_error), Ok(snippets)) => {
                self.warn_once(&xprompt_error).await;
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!("refreshed {} snippets", snippets.len()),
                    )
                    .await;
            }
            (Err(xprompt_error), Err(snippet_error)) => {
                self.warn_once(&xprompt_error).await;
                self.warn_once(&snippet_error).await;
            }
        }
        self.request_semantic_tokens_refresh();
    }

    async fn publish_document_diagnostics(
        &self,
        uri: Uri,
        document: OpenDocument,
    ) {
        let diagnostics = if document.eligible {
            self.diagnostics_for_uri_text(&uri, document.text).await
        } else {
            Vec::new()
        };
        self.client
            .publish_diagnostics(uri, diagnostics, None)
            .await;
    }

    async fn warn_once(&self, error: &CatalogFailure) {
        warn!("{}", error.message);
        if self.catalog_cache.should_warn(&error.class) {
            self.client
                .show_message(MessageType::WARNING, error.message.clone())
                .await;
        }
    }

    fn request_semantic_tokens_refresh(&self) {
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.semantic_tokens_refresh().await;
        });
    }
}

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
        let language_id = self
            .document_for_uri(&uri)
            .map(|document| document.language_id)
            .unwrap_or_default();
        let document = self.open_document(&uri, language_id, text);
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

fn config_from_initialize(params: &InitializeParams) -> ServerConfig {
    #[allow(deprecated)]
    let root_uri_dir = params
        .root_uri
        .as_ref()
        .and_then(|uri| uri.to_file_path().map(|path| path.into_owned()));
    let root_dir = params
        .workspace_folders
        .as_ref()
        .and_then(|folders| folders.first())
        .and_then(|folder| {
            folder.uri.to_file_path().map(|path| path.into_owned())
        })
        .or(root_uri_dir)
        .or_else(|| std::env::current_dir().ok());
    let project = root_dir
        .as_deref()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(str::to_string);
    let catalog_key = root_dir
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| "default".to_string());
    ServerConfig {
        root_dir,
        project,
        catalog_key,
        snippet_support: snippet_support(&params.capabilities),
        allow_all_markdown: params
            .initialization_options
            .as_ref()
            .and_then(|options| options.get("allow_all_markdown"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        vcs_project_catalog: vcs_project_catalog_path(),
        model_catalog: model_catalog_path(),
        artifact_ref_catalog: artifact_ref_catalog_path(),
        glossary_catalog: glossary_catalog_path(),
    }
}

fn vcs_project_catalog_path() -> Option<PathBuf> {
    std::env::var_os(VCS_PROJECT_CATALOG_ENV).map(PathBuf::from)
}

fn model_catalog_path() -> Option<PathBuf> {
    std::env::var_os(MODEL_CATALOG_ENV).map(PathBuf::from)
}

fn artifact_ref_catalog_path() -> Option<PathBuf> {
    std::env::var_os(ARTIFACT_REF_CATALOG_ENV).map(PathBuf::from)
}

fn glossary_catalog_path() -> Option<PathBuf> {
    std::env::var_os(GLOSSARY_CATALOG_ENV).map(PathBuf::from)
}

fn snippet_support(capabilities: &ClientCapabilities) -> bool {
    capabilities
        .text_document
        .as_ref()
        .and_then(|text| text.completion.as_ref())
        .and_then(|completion| completion.completion_item.as_ref())
        .and_then(|item| item.snippet_support)
        .unwrap_or(false)
}

fn xprompt_snippet_items(
    list: CompletionList,
    entries: &[XpromptAssistEntry],
    replacement_range: sase_core::EditorRange,
    append_text_arg_space: bool,
) -> Vec<CompletionItem> {
    list.candidates
        .into_iter()
        .filter_map(|candidate| {
            let entry =
                entries.iter().find(|entry| entry.name == candidate.name)?;
            Some(snippet_completion_item(
                candidate.display,
                xprompt_completion_skeleton(entry, append_text_arg_space),
                candidate.detail,
                candidate.documentation,
                replacement_range,
            ))
        })
        .collect()
}

fn xprompt_completion_skeleton(
    entry: &XpromptAssistEntry,
    append_text_arg_space: bool,
) -> String {
    let required = entry
        .inputs
        .iter()
        .filter(|input| input.required)
        .collect::<Vec<_>>();
    match required.as_slice() {
        [] => format!("{} ", entry.insertion),
        // The free-form double-colon shorthand is `:: ` followed by text. An
        // end-of-line completion appends that space so the user lands one
        // keystroke from typing the body; an inline completion keeps `::` so the
        // following text supplies the single delimiter.
        [input] if input.r#type == "text" => {
            if append_text_arg_space {
                format!("{}:: ", entry.insertion)
            } else {
                format!("{}::", entry.insertion)
            }
        }
        [_] => format!("{}:", entry.insertion),
        _ => format!("{}($0)", entry.insertion),
    }
}

/// Whether `range`'s end sits at the end of its line (no trailing text), so the
/// required-text `::` skeleton may be widened to `:: `. Compared in UTF-16 units
/// to match [`EditorPosition::character`].
fn replacement_ends_line(
    document: &DocumentSnapshot,
    range: EditorRange,
) -> bool {
    document
        .line_text(range.end.line)
        .map(|line| {
            line.chars().map(char::len_utf16).sum::<usize>()
                == range.end.character as usize
        })
        .unwrap_or(false)
}

fn directive_snippet_items(
    token: Option<&str>,
    replacement_range: sase_core::EditorRange,
) -> Vec<CompletionItem> {
    let partial = token
        .unwrap_or_default()
        .strip_prefix('%')
        .unwrap_or_default();
    sase_core::EDITOR_DIRECTIVES
        .iter()
        .filter(|directive| directive.takes_argument)
        .filter(|directive| {
            directive.name.starts_with(partial)
                || directive
                    .alias
                    .is_some_and(|alias| alias.starts_with(partial))
        })
        .flat_map(|directive| {
            let documentation = Some(
                editor_directive_metadata(directive.name)
                    .map(|metadata| metadata.description.to_string())
                    .unwrap_or_else(|| directive.description.to_string()),
            );
            let syntax = if directive.name == "alt" {
                // The advertised alt spelling is the `%{A | B}` brace shorthand.
                "%{${1:A} | ${2:B}\\}$0".to_string()
            } else {
                let placeholder = match directive.name {
                    "clan" => "name",
                    "id" => "agent-id",
                    _ => "value",
                };
                format!("%{}:${{1:{placeholder}}}$0", directive.name)
            };
            let mut items = vec![snippet_completion_item(
                format!("%{}:...", directive.name),
                syntax,
                Some("directive snippet".to_string()),
                documentation.clone(),
                replacement_range,
            )];
            if directive.name == "clan" {
                items.push(snippet_completion_item(
                    "%clan(..., tribe=...)".to_string(),
                    "%clan(${1:name}, tribe=${2:tribe})$0".to_string(),
                    Some("directive snippet".to_string()),
                    documentation,
                    replacement_range,
                ));
            } else if directive.name == "id" {
                items.push(snippet_completion_item(
                    "%id(..., clan=...)".to_string(),
                    "%id(${1:id}, clan=${2:clan})$0".to_string(),
                    Some("directive snippet".to_string()),
                    documentation.clone(),
                    replacement_range,
                ));
                items.push(snippet_completion_item(
                    "%id(..., family=...)".to_string(),
                    "%id(${1:suffix}, family=${2:family})$0".to_string(),
                    Some("directive snippet".to_string()),
                    documentation.clone(),
                    replacement_range,
                ));
                items.push(snippet_completion_item(
                    "%id(tribe=...)".to_string(),
                    "%id(tribe=${1:tribe})$0".to_string(),
                    Some("directive snippet".to_string()),
                    documentation,
                    replacement_range,
                ));
            }
            items
        })
        .collect()
}

fn sase_snippet_items(
    list: CompletionList,
    replacement_range: sase_core::EditorRange,
) -> Vec<CompletionItem> {
    list.candidates
        .into_iter()
        .map(|candidate| {
            sase_snippet_completion_item(
                candidate.display,
                candidate.insertion,
                candidate.detail,
                candidate.documentation,
                replacement_range,
            )
        })
        .collect()
}

fn bool_completion_list() -> CompletionList {
    CompletionList {
        candidates: ["false", "true"]
            .into_iter()
            .map(|value| CompletionCandidate {
                display: value.to_string(),
                insertion: value.to_string(),
                detail: None,
                documentation: None,
                is_dir: false,
                name: value.to_string(),
                replacement: None,
                additional_edits: Vec::new(),
                kind: String::new(),
                project: String::new(),
                status: String::new(),
            })
            .collect(),
        shared_extension: String::new(),
    }
}

fn empty_completion_list() -> CompletionList {
    CompletionList {
        candidates: Vec::new(),
        shared_extension: String::new(),
    }
}

fn empty_completion_response() -> CompletionResponse {
    CompletionResponse::Array(Vec::new())
}

fn ranked_vcs_repo_entries(
    entries: &[VcsRepoEntry],
    query: &str,
) -> Vec<VcsRepoEntry> {
    let query = query.to_lowercase();
    let mut ranked = entries.to_vec();
    ranked.sort_by(|left, right| {
        vcs_repo_name_matches_query(right, &query)
            .cmp(&vcs_repo_name_matches_query(left, &query))
            .then_with(|| compare_vcs_repo_pushed_at(left, right))
            .then_with(|| {
                left.name.to_lowercase().cmp(&right.name.to_lowercase())
            })
            .then_with(|| left.name.cmp(&right.name))
    });
    ranked
}

fn vcs_repo_name_matches_query(entry: &VcsRepoEntry, query: &str) -> bool {
    query.is_empty() || entry.name.to_lowercase().starts_with(query)
}

fn compare_vcs_repo_pushed_at(
    left: &VcsRepoEntry,
    right: &VcsRepoEntry,
) -> Ordering {
    match (left.pushed_at.as_deref(), right.pushed_at.as_deref()) {
        (Some(left), Some(right)) => right.cmp(left),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn model_completion_list(partial: &str, path: Option<&Path>) -> CompletionList {
    let entries = load_model_catalog(path);
    let needle = partial.to_lowercase();
    let mut candidates = Vec::new();
    for entry in entries {
        let value_lower = entry.value.to_lowercase();
        let matched_alias = entry
            .aliases
            .iter()
            .find(|alias| alias.to_lowercase().starts_with(&needle));
        let filter_text =
            if needle.is_empty() || value_lower.starts_with(&needle) {
                entry.value.clone()
            } else if let Some(alias) = matched_alias {
                alias.clone()
            } else {
                continue;
            };
        let display = if entry.display.is_empty() {
            entry.value.clone()
        } else {
            entry.display.clone()
        };
        let detail = model_completion_detail(&entry);
        let documentation = model_completion_documentation(&entry);
        candidates.push(CompletionCandidate {
            display,
            insertion: entry.value.clone(),
            detail,
            documentation,
            is_dir: false,
            name: filter_text,
            replacement: None,
            additional_edits: Vec::new(),
            kind: entry.kind,
            project: String::new(),
            status: entry.alias_kind,
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

fn is_model_alias_kind(kind: &str) -> bool {
    matches!(kind, "implicit_alias" | "user_alias")
}

fn model_completion_detail(entry: &ModelCompletionEntry) -> Option<String> {
    if !is_model_alias_kind(&entry.kind) {
        return (!entry.provider.is_empty()).then(|| entry.provider.clone());
    }

    let mut target = match (
        entry.target_provider.is_empty(),
        entry.target_model.is_empty(),
    ) {
        (false, false) => format!(
            "{}({})",
            entry.target_provider.to_uppercase(),
            entry.target_model
        ),
        (true, false) => entry.target_model.clone(),
        (false, true) => entry.target_provider.to_uppercase(),
        (true, true) => String::new(),
    };
    if !target.is_empty() && !entry.target_effort.is_empty() {
        target.push_str(" @ ");
        target.push_str(&entry.target_effort);
    }
    if !target.is_empty() {
        return Some(target);
    }

    // Additive v1 compatibility: an older catalog has none of the structured
    // target fields, so retain its legacy provider/description detail.
    let legacy_parts: Vec<&str> = [
        (!entry.provider.is_empty()).then_some(entry.provider.as_str()),
        (!entry.description.is_empty()).then_some(entry.description.as_str()),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!legacy_parts.is_empty()).then(|| legacy_parts.join("  "))
}

fn model_completion_documentation(
    entry: &ModelCompletionEntry,
) -> Option<String> {
    let mut sections = Vec::new();
    if !entry.description.is_empty() {
        sections.push(entry.description.clone());
    }
    if !entry.provenance.is_empty() {
        let mut provenance = entry.provenance.clone();
        if !entry.reference.is_empty() {
            provenance.push_str(" → @");
            provenance.push_str(entry.reference.trim_start_matches('@'));
            if !entry.reference_effort.is_empty() {
                provenance.push_str(" @ ");
                provenance.push_str(&entry.reference_effort);
            }
        }
        sections.push(format!("**Provenance:** {provenance}"));
    }
    if !entry.config_source.is_empty() {
        sections.push(format!(
            "**Config:** `llm_provider.model_aliases.{}.{}`",
            entry.config_source,
            entry.value.trim_start_matches('@')
        ));
    }
    if !entry.bucket.is_empty() {
        sections.push(format!("**Bucket:** `{}`", entry.bucket));
    }
    if entry.selector_mode == "round_robin" {
        sections.push(format!(
            "**Pool:** {}/{} available",
            entry.pool_available, entry.pool_total
        ));
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

fn file_history() -> Vec<String> {
    let Some(home) = std::env::var_os("HOME") else {
        return Vec::new();
    };
    let path = PathBuf::from(home)
        .join(".sase")
        .join("file_reference_history.json");
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return Vec::new();
    };
    value
        .get("paths")
        .and_then(|paths| paths.as_array())
        .into_iter()
        .flatten()
        .filter_map(|path| path.as_str())
        .filter(|path| !path.starts_with(".sase/"))
        .map(str::to_string)
        .collect()
}

/// Load the enabled project/PR completion catalog from the materialized JSON
/// file at `path`.
///
/// Read fresh on every `+` completion request. Any failure (no path, unreadable
/// file, malformed JSON) degrades to empty results so the `+` menu simply
/// shows nothing rather than breaking completion. Schema versions 1 through 4
/// are accepted; v1 entries default to project rows, and v1/v2 catalogs default
/// `namespaces` to empty. The v4 file shape is
/// `{ "schema_version": 4, "workflow_names": [..], "entries": [VcsProjectEntry, ..], "namespaces": {"gh": [VcsNamespaceEntry, ..]} }`.
fn load_vcs_project_catalog(path: Option<&Path>) -> VcsProjectCatalog {
    let Some(path) = path else {
        return VcsProjectCatalog::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return VcsProjectCatalog::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse vcs project catalog at {path:?}");
        return VcsProjectCatalog::default();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    if !matches!(schema_version, 1..=4) {
        warn!(
            "unsupported vcs project catalog schema_version {schema_version} at {path:?}"
        );
        return VcsProjectCatalog::default();
    }
    let entries = value
        .get("entries")
        .cloned()
        .and_then(|entries| {
            serde_json::from_value::<Vec<VcsProjectEntry>>(entries).ok()
        })
        .unwrap_or_default();
    let workflow_names = value
        .get("workflow_names")
        .and_then(serde_json::Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(|name| name.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let namespaces = value
        .get("namespaces")
        .cloned()
        .and_then(|namespaces| {
            serde_json::from_value::<HashMap<String, Vec<VcsNamespaceEntry>>>(
                namespaces,
            )
            .ok()
        })
        .unwrap_or_default();
    VcsProjectCatalog {
        entries,
        workflow_names,
        namespaces,
    }
}

fn artifact_ref_catalog_signature(
    path: Option<&Path>,
) -> ArtifactRefCatalogSignature {
    let metadata = path.and_then(|path| fs::metadata(path).ok());
    ArtifactRefCatalogSignature {
        path: path.map(Path::to_path_buf),
        modified: metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok()),
        len: metadata.as_ref().map_or(0, fs::Metadata::len),
    }
}

/// Load the launcher-generated local artifact-reference catalog.
///
/// The schema is version-gated and every failure degrades to no artifact
/// assistance. [`XpromptLspServer::artifact_ref_catalog`] caches this parsed
/// value together with the payload inventories and invalidates it by file
/// signature, TTL, or explicit refresh.
fn load_artifact_ref_catalog(path: Option<&Path>) -> ArtifactRefCatalog {
    let Some(path) = path else {
        return ArtifactRefCatalog::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return ArtifactRefCatalog::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse artifact-reference catalog at {path:?}");
        return ArtifactRefCatalog::default();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    if schema_version != Some(1) {
        warn!(
            "unsupported artifact-reference catalog schema_version {:?} at {path:?}",
            schema_version
        );
        return ArtifactRefCatalog::default();
    }
    ArtifactRefCatalog {
        default_project: value
            .get("default_project")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        projects: value
            .get("projects")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|project| {
                serde_json::from_value::<ArtifactRefCatalogProject>(
                    project.clone(),
                )
                .ok()
            })
            .filter(|project| {
                !project.name.is_empty() && !project.key.is_empty()
            })
            .collect(),
    }
}

fn glossary_catalog_signature(path: Option<&Path>) -> GlossaryCatalogSignature {
    let metadata = path.and_then(|path| fs::metadata(path).ok());
    GlossaryCatalogSignature {
        path: path.map(Path::to_path_buf),
        modified: metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok()),
        len: metadata.as_ref().map_or(0, fs::Metadata::len),
    }
}

/// Load and compile the launcher-generated project glossary catalog.
///
/// The schema is version-gated and every failure degrades to no glossary
/// semantics. [`XpromptLspServer::glossary_catalog`] caches this parsed value
/// and invalidates it by file signature, TTL, explicit refresh, or watched
/// config changes.
fn load_glossary_catalog(path: Option<&Path>) -> GlossaryCatalog {
    let Some(path) = path else {
        return GlossaryCatalog::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return GlossaryCatalog::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse glossary catalog at {path:?}");
        return GlossaryCatalog::default();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    if schema_version != Some(1) {
        warn!(
            "unsupported glossary catalog schema_version {:?} at {path:?}",
            schema_version
        );
        return GlossaryCatalog::default();
    }
    GlossaryCatalog {
        default_project: value
            .get("default_project")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        projects: value
            .get("projects")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|project| glossary_catalog_project(project.clone()))
            .collect(),
    }
}

fn glossary_catalog_project(
    value: serde_json::Value,
) -> Option<GlossaryCatalogProject> {
    let payload =
        serde_json::from_value::<GlossaryCatalogProjectPayload>(value).ok()?;
    if payload.schema_version != 1
        || payload.project.key.is_empty()
        || payload.project.name.is_empty()
        || payload.entries.is_empty()
    {
        return None;
    }
    let catalog = CompiledGlossaryCatalog::new(GlossaryCatalogWire {
        schema_version: payload.schema_version,
        entries: payload.entries,
    })
    .ok()?;
    if catalog.is_empty() {
        return None;
    }
    Some(GlossaryCatalogProject {
        key: payload.project.key,
        name: payload.project.name,
        aliases: payload.project.aliases,
        config_path: payload.config_path,
        catalog: Arc::new(catalog),
    })
}

fn known_at_reference_kinds(
    context: Option<&ArtifactRefContextWire>,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    sase_core::editor::at_reference::BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .copied()
        .chain(
            context
                .into_iter()
                .flat_map(|context| context.document_roots.iter())
                .map(|root| root.kind.as_str()),
        )
        .filter(|kind| !kind.is_empty())
        .filter(|kind| seen.insert((*kind).to_string()))
        .map(str::to_string)
        .collect()
}

fn at_reference_kind_inventory(
    context: Option<&ArtifactRefContextWire>,
) -> Vec<AtReferenceKindRowWire> {
    known_at_reference_kinds(context)
        .into_iter()
        .map(|kind| {
            let builtin =
                sase_core::editor::at_reference::is_builtin_at_reference_kind(
                    &kind,
                );
            let detail = if builtin {
                "builtin artifact kind".to_string()
            } else {
                context
                    .into_iter()
                    .flat_map(|context| context.document_roots.iter())
                    .find(|root| root.kind == kind)
                    .map(|root| format!("document artifact · {}", root.root))
                    .unwrap_or_else(|| "document artifact".to_string())
            };
            AtReferenceKindRowWire {
                kind,
                builtin,
                detail,
            }
        })
        .collect()
}

fn at_reference_path_inventory(
    context: &AtReferenceContextWire,
    config: &ServerConfig,
) -> Vec<AtReferencePathRowWire> {
    if context.stage != AtReferenceStage::Kind {
        return Vec::new();
    }
    let Some(path_query) = context.path_query.as_ref() else {
        return Vec::new();
    };
    let Some(directory) = resolve_at_reference_directory(
        config.root_dir.as_deref(),
        &path_query.directory,
    ) else {
        return Vec::new();
    };
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .take(1_000)
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            Some(AtReferencePathRowWire {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: file_type.is_dir(),
            })
        })
        .collect()
}

fn resolve_at_reference_directory(
    root_dir: Option<&Path>,
    directory: &str,
) -> Option<PathBuf> {
    let expanded = if directory == "~/" {
        std::env::var_os("HOME").map(PathBuf::from)?
    } else if let Some(rest) = directory.strip_prefix("~/") {
        std::env::var_os("HOME").map(PathBuf::from)?.join(rest)
    } else {
        PathBuf::from(directory)
    };
    let resolved = if expanded.is_absolute() {
        expanded
    } else {
        root_dir?.join(expanded)
    };
    resolved.canonicalize().ok()
}

fn active_artifact_ref_project<'a>(
    document: &DocumentSnapshot,
    config: &ServerConfig,
    vcs_catalog: &VcsProjectCatalog,
    artifact_catalog: &'a ArtifactRefCatalog,
) -> Option<&'a ArtifactRefCatalogProject> {
    let leading_project =
        leading_vcs_project(document.text(), &vcs_catalog.entries);
    leading_project
        .and_then(|project| artifact_ref_project(artifact_catalog, project))
        .or_else(|| {
            artifact_catalog
                .default_project
                .as_deref()
                .and_then(|project| {
                    artifact_ref_project(artifact_catalog, project)
                })
        })
        .or_else(|| {
            config.project.as_deref().and_then(|project| {
                artifact_ref_project(artifact_catalog, project).or_else(|| {
                    initialized_project_basename(project).and_then(|basename| {
                        artifact_ref_project(artifact_catalog, basename)
                    })
                })
            })
        })
}

fn active_artifact_ref_context<'a>(
    document: &DocumentSnapshot,
    config: &ServerConfig,
    vcs_catalog: &VcsProjectCatalog,
    artifact_catalog: &'a ArtifactRefCatalog,
) -> Option<&'a ArtifactRefContextWire> {
    active_artifact_ref_project(document, config, vcs_catalog, artifact_catalog)
        .map(|project| &project.context)
}

fn active_glossary_project<'a>(
    document: &DocumentSnapshot,
    config: &ServerConfig,
    vcs_catalog: &VcsProjectCatalog,
    glossary_catalog: &'a GlossaryCatalog,
) -> Option<&'a GlossaryCatalogProject> {
    let leading_project =
        leading_vcs_project(document.text(), &vcs_catalog.entries);
    leading_project
        .and_then(|project| glossary_project(glossary_catalog, project))
        .or_else(|| {
            glossary_catalog
                .default_project
                .as_deref()
                .and_then(|project| glossary_project(glossary_catalog, project))
        })
        .or_else(|| {
            config.project.as_deref().and_then(|project| {
                glossary_project(glossary_catalog, project).or_else(|| {
                    initialized_project_basename(project).and_then(|basename| {
                        glossary_project(glossary_catalog, basename)
                    })
                })
            })
        })
}

fn leading_vcs_project<'a>(
    text: &str,
    entries: &'a [VcsProjectEntry],
) -> Option<&'a str> {
    let token = text.split_ascii_whitespace().next()?;
    if !token.starts_with('#') {
        return None;
    }
    entries.iter().find_map(|entry| {
        let canonical = token == entry.display_tag;
        let alias = token
            .strip_prefix(&format!("#{}:", entry.vcs_prefix))
            .is_some_and(|value| {
                value.eq_ignore_ascii_case(&entry.name)
                    || entry
                        .aliases
                        .iter()
                        .any(|alias| alias.eq_ignore_ascii_case(value))
            });
        if !canonical && !alias {
            return None;
        }
        if !entry.project.is_empty() {
            Some(entry.project.as_str())
        } else {
            Some(entry.name.as_str())
        }
    })
}

fn artifact_ref_project<'a>(
    catalog: &'a ArtifactRefCatalog,
    identity: &str,
) -> Option<&'a ArtifactRefCatalogProject> {
    catalog.projects.iter().find(|project| {
        project.name.eq_ignore_ascii_case(identity)
            || project.key.eq_ignore_ascii_case(identity)
            || project
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(identity))
    })
}

fn glossary_project<'a>(
    catalog: &'a GlossaryCatalog,
    identity: &str,
) -> Option<&'a GlossaryCatalogProject> {
    catalog.projects.iter().find(|project| {
        project.name.eq_ignore_ascii_case(identity)
            || project.key.eq_ignore_ascii_case(identity)
            || project
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(identity))
    })
}

fn initialized_project_basename(project: &str) -> Option<&str> {
    let (basename, suffix) = project.rsplit_once('_')?;
    (!basename.is_empty()
        && !suffix.is_empty()
        && suffix.bytes().all(|byte| byte.is_ascii_digit()))
    .then_some(basename)
}

/// Load the `%model` completion catalog from the materialized JSON file.
///
/// Read fresh on every `%model` completion request. Any failure (no path,
/// unreadable file, malformed JSON) degrades to empty results.
fn load_model_catalog(path: Option<&Path>) -> Vec<ModelCompletionEntry> {
    let Some(path) = path else {
        return Vec::new();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse model catalog at {path:?}");
        return Vec::new();
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    if schema_version != 1 {
        warn!(
            "unsupported model catalog schema_version {schema_version} at {path:?}"
        );
        return Vec::new();
    }
    value
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .map(|entries| entries.iter().filter_map(model_entry).collect())
        .unwrap_or_default()
}

fn model_entry(value: &serde_json::Value) -> Option<ModelCompletionEntry> {
    let object = value.as_object()?;
    let model_value = object.get("value")?.as_str()?.to_string();
    if model_value.is_empty() {
        return None;
    }
    let aliases = object
        .get("aliases")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(ModelCompletionEntry {
        value: model_value,
        display: json_string(object, "display"),
        description: json_string(object, "description"),
        kind: json_string(object, "kind"),
        provider: json_string(object, "provider"),
        aliases,
        alias_kind: json_string(object, "alias_kind"),
        target_provider: json_string(object, "target_provider"),
        target_model: json_string(object, "target_model"),
        target_effort: json_string(object, "target_effort"),
        provenance: json_string(object, "provenance"),
        reference: json_string(object, "reference"),
        reference_effort: json_string(object, "reference_effort"),
        selector_mode: json_string(object, "selector_mode"),
        pool_available: json_u64(object, "pool_available"),
        pool_total: json_u64(object, "pool_total"),
        config_source: json_string(object, "config_source"),
        bucket: json_string(object, "bucket"),
    })
}

fn json_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> String {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn json_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> u64 {
    object
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default()
}

fn entry_for_token<'a>(
    token: &str,
    entries: &'a [XpromptAssistEntry],
) -> Option<&'a XpromptAssistEntry> {
    if let Some(name) =
        token.strip_prefix("#!").or_else(|| token.strip_prefix('#'))
    {
        let normalized = name.replace("__", "/");
        return entries.iter().find(|entry| entry.name == normalized);
    }
    if let Some(name) = token.strip_prefix('/') {
        // Slash tokens carry the provider skill name (`/foo`), not the
        // namespaced xprompt reference (`#skill/foo`).
        return entries.iter().find(|entry| {
            entry.is_skill && entry.skill_name.as_deref() == Some(name)
        });
    }
    None
}

fn canonical_marker_action(
    uri: &Uri,
    range: EditorRange,
    token: &str,
    entry: &XpromptAssistEntry,
) -> Option<CodeAction> {
    if token.starts_with(&entry.reference_prefix) {
        return None;
    }
    Some(text_edit_action(
        &format!("Use canonical `{}` marker", entry.reference_prefix),
        uri,
        range,
        entry.insertion.clone(),
        CodeActionKind::QUICKFIX,
        true,
    ))
}

fn text_edit_action(
    title: &str,
    uri: &Uri,
    range: EditorRange,
    new_text: String,
    kind: CodeActionKind,
    preferred: bool,
) -> CodeAction {
    let text_edit = TextEdit {
        range: to_lsp_range(range),
        new_text,
    };
    CodeAction {
        title: title.to_string(),
        kind: Some(kind),
        edit: Some(WorkspaceEdit {
            changes: None,
            document_changes: Some(DocumentChanges::Edits(vec![
                TextDocumentEdit {
                    text_document: OptionalVersionedTextDocumentIdentifier {
                        uri: uri.clone(),
                        version: None,
                    },
                    edits: vec![OneOf::Left(text_edit)],
                },
            ])),
            change_annotations: None,
        }),
        is_preferred: Some(preferred),
        ..Default::default()
    }
}

fn plain_named_args_skeleton(entry: &XpromptAssistEntry) -> String {
    let required = entry
        .inputs
        .iter()
        .filter(|input| input.required)
        .map(|input| format!("{}=", input.name))
        .collect::<Vec<_>>();
    if required.is_empty() {
        entry.insertion.clone()
    } else {
        format!("{}({})", entry.insertion, required.join(", "))
    }
}

fn definition_uri_at_position(
    document: &DocumentSnapshot,
    position: sase_core::EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<Uri> {
    let target = editor_definition_at_position(document, position, entries)?;
    Uri::from_file_path(target.path)
}

fn glossary_hover_at_position(
    document: &DocumentSnapshot,
    position: sase_core::EditorPosition,
    project: &GlossaryCatalogProject,
) -> Option<HoverPayload> {
    let span = project.catalog.lookup(document.text(), position)?;
    let entry = glossary_entry_for_span(project, &span)?;
    Some(HoverPayload {
        range: span.range,
        markdown: glossary_hover_markdown(project, entry),
    })
}

fn glossary_definition_at_position(
    document: &DocumentSnapshot,
    position: sase_core::EditorPosition,
    project: &GlossaryCatalogProject,
) -> Option<Location> {
    let span = project.catalog.lookup(document.text(), position)?;
    let entry = glossary_entry_for_span(project, &span)?;
    let source = entry.source.as_ref();
    let path = source
        .and_then(|source| source.config_path.as_deref())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or(project.config_path.as_str());
    let uri = Uri::from_file_path(Path::new(path))?;
    Some(Location {
        uri,
        range: source
            .and_then(|source| source.definition_range)
            .map(to_lsp_range)
            .unwrap_or_else(zero_range),
    })
}

fn glossary_entry_for_span<'a>(
    project: &'a GlossaryCatalogProject,
    span: &GlossarySpanWire,
) -> Option<&'a GlossaryEntryWire> {
    project.catalog.catalog().entries.get(span.entry_index)
}

fn glossary_hover_markdown(
    project: &GlossaryCatalogProject,
    entry: &GlossaryEntryWire,
) -> String {
    let mut lines = vec![format!("**{}**", entry.term)];
    if !entry.configured_aliases.is_empty() {
        lines.push(String::new());
        lines.push(format!(
            "Aliases: {}",
            entry
                .configured_aliases
                .iter()
                .map(|alias| markdown_code(alias))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.push(String::new());
    lines.push(entry.definition.clone());

    let mut meta = vec![format!("project `{}`", project.name)];
    if !project.config_path.is_empty() {
        meta.push(format!("source `{}`", project.config_path));
    }
    lines.push(String::new());
    lines.push(meta.join(" | "));
    lines.join("\n")
}

fn markdown_code(value: &str) -> String {
    format!("`{}`", value.replace('`', "\\`"))
}

fn zero_range() -> Range {
    Range {
        start: Position {
            line: 0,
            character: 0,
        },
        end: Position {
            line: 0,
            character: 0,
        },
    }
}

fn document_eligible(
    uri: &Uri,
    language_id: &str,
    config: &ServerConfig,
) -> bool {
    match language_id {
        "markdown" => config.allow_all_markdown || markdown_uri_eligible(uri),
        "gitcommit" | "sase" | "sase_prompt" => true,
        _ => false,
    }
}

fn markdown_uri_eligible(uri: &Uri) -> bool {
    let Some(path) = uri.to_file_path().map(|path| path.into_owned()) else {
        return false;
    };
    if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
        return false;
    }
    if path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("xprompts" | ".xprompts" | "default_xprompts")
        )
    }) {
        return true;
    }
    if is_memory_note_path(&path) {
        return true;
    }
    let Some(file_name) = path.file_name().and_then(|name| name.to_str())
    else {
        return false;
    };
    is_prompt_temp_markdown_name(file_name)
}

/// Whether `path` is a flat note in a canonical or legacy memory root.
///
/// Memory notes are xprompt memories, so editing one must refresh the catalog
/// and the note itself gets prompt assistance for the references it holds.
fn is_memory_note_path(path: &Path) -> bool {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        == Some(MEMORY_NAMESPACE_SEGMENT)
}

fn is_prompt_temp_markdown_name(file_name: &str) -> bool {
    ["sase_ace_prompt_", "sase_prompt_"].iter().any(|prefix| {
        file_name.strip_prefix(prefix).is_some_and(|rest| {
            rest.len() > ".md".len() && rest.ends_with(".md")
        })
    })
}

fn should_invalidate_for_uri(uri: &Uri) -> bool {
    let Some(path) = uri.to_file_path().map(|path| path.into_owned()) else {
        return false;
    };
    let Some(file_name) = path.file_name().and_then(|name| name.to_str())
    else {
        return false;
    };
    if matches!(
        file_name,
        "xprompts.yml" | "xprompts.yaml" | "sase.yml" | "default_config.yml"
    ) {
        return true;
    }
    if file_name == "file_reference_history.json"
        && path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(".sase")
    {
        return true;
    }
    let extension = path.extension().and_then(|ext| ext.to_str());
    if !matches!(extension, Some("md" | "yml" | "yaml")) {
        return false;
    }
    if is_memory_note_path(&path) {
        return true;
    }
    path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("xprompts" | ".xprompts" | "default_xprompts" | "refs")
        )
    })
}

#[cfg(test)]
mod tests {
    use std::{path::Path, process::Command, sync::Arc};

    use lsp_types::{
        CodeActionOrCommand, CompletionClientCapabilities, CompletionContext,
        CompletionItemCapability, CompletionItemKind, CompletionResponse,
        CompletionTextEdit, CompletionTriggerKind, Documentation,
        GotoDefinitionResponse, Hover, InsertTextFormat, Position, Range,
        TextDocumentClientCapabilities, TextDocumentIdentifier,
        TextDocumentPositionParams, Uri,
    };
    use sase_core::{
        EditorPosition as CorePosition, EditorRange as CoreRange,
        EditorSnippetCatalogResponseWire, EditorSnippetCatalogStatsWire,
        EditorSnippetEntryWire, MobileHelperProjectContextWire,
        MobileHelperProjectScopeWire, MobileHelperResultWire,
        MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
        MobileXpromptCatalogResponseWire, MobileXpromptCatalogStatsWire,
        MobileXpromptInputWire, StaticHelperHostBridge,
    };
    use tower_lsp_server::UriExt;

    use super::*;

    fn bridge_with_catalog(
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

    fn bridge_with_catalog_entries(
        entries: Vec<MobileXpromptCatalogEntryWire>,
    ) -> StaticHelperHostBridge {
        bridge_with_catalog_and_snippets(entries, Vec::new())
    }

    fn bridge_with_catalog_and_snippets(
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

    fn bridge_with_vcs_repo_catalog(
        response: VcsRepoCatalogResponse,
    ) -> StaticHelperHostBridge {
        let mut bridge = bridge_with_catalog_entries(Vec::new());
        bridge.vcs_repo_catalog_response = response;
        bridge
    }

    fn catalog_entry(
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

    fn input_hint(
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

    fn snippet_entry(
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

    fn diagnostics_contain_code(
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

    fn file_uri(path: impl AsRef<Path>) -> Uri {
        Uri::from_file_path(path.as_ref()).unwrap()
    }

    #[test]
    fn document_eligibility_narrows_plain_markdown() {
        let temp = std::env::temp_dir();
        let config = ServerConfig::default();
        let canonical_xprompts_uri = file_uri(
            temp.join("project")
                .join("sase")
                .join("xprompts")
                .join("foo.md"),
        );
        let legacy_xprompts_uri =
            file_uri(temp.join("project").join("xprompts").join("foo.md"));
        let dot_xprompts_uri =
            file_uri(temp.join("project").join(".xprompts").join("foo.md"));
        let default_xprompts_uri = file_uri(
            temp.join("project")
                .join("src")
                .join("sase")
                .join("default_xprompts")
                .join("research_swarm.md"),
        );
        let ace_prompt_uri = file_uri(temp.join("sase_ace_prompt_abc.md"));
        let cli_prompt_uri = file_uri(temp.join("sase_prompt_abc.md"));
        let prose_uri = file_uri(
            temp.join("project")
                .join("sdd")
                .join("research")
                .join("202605")
                .join("memory_system_prior_art.md"),
        );

        assert!(document_eligible(
            &canonical_xprompts_uri,
            "markdown",
            &config
        ));
        assert!(document_eligible(&legacy_xprompts_uri, "markdown", &config));
        assert!(document_eligible(&dot_xprompts_uri, "markdown", &config));
        assert!(document_eligible(
            &default_xprompts_uri,
            "markdown",
            &config
        ));
        assert!(document_eligible(&ace_prompt_uri, "markdown", &config));
        assert!(document_eligible(&cli_prompt_uri, "markdown", &config));
        assert!(!document_eligible(&prose_uri, "markdown", &config));

        let all_markdown = ServerConfig {
            allow_all_markdown: true,
            ..ServerConfig::default()
        };
        assert!(document_eligible(&prose_uri, "markdown", &all_markdown));
        assert!(document_eligible(&prose_uri, "gitcommit", &config));
        assert!(document_eligible(&prose_uri, "sase", &config));
        assert!(document_eligible(&prose_uri, "sase_prompt", &config));

        // Memory notes are xprompt memories, so a flat note in a canonical or
        // legacy memory root gets prompt assistance too.
        let canonical_memory_uri = file_uri(
            temp.join("project")
                .join("sase")
                .join("memory")
                .join("glossary.md"),
        );
        let legacy_memory_uri =
            file_uri(temp.join("project").join("memory").join("glossary.md"));
        let nested_memory_asset_uri = file_uri(
            temp.join("project")
                .join("sase")
                .join("memory")
                .join("assets")
                .join("diagram.md"),
        );
        assert!(document_eligible(
            &canonical_memory_uri,
            "markdown",
            &config
        ));
        assert!(document_eligible(&legacy_memory_uri, "markdown", &config));
        assert!(!document_eligible(
            &nested_memory_asset_uri,
            "markdown",
            &config
        ));
    }

    #[test]
    fn catalog_invalidation_tracks_xprompt_source_dirs() {
        let temp = std::env::temp_dir();
        let canonical_xprompts_uri = file_uri(
            temp.join("project")
                .join("sase")
                .join("xprompts")
                .join("foo.md"),
        );
        let legacy_xprompts_uri =
            file_uri(temp.join("project").join("xprompts").join("foo.md"));
        let dot_xprompts_uri =
            file_uri(temp.join("project").join(".xprompts").join("foo.md"));
        let default_xprompts_uri = file_uri(
            temp.join("project")
                .join("src")
                .join("sase")
                .join("default_xprompts")
                .join("research_swarm.md"),
        );
        let canonical_refs_uri = file_uri(
            temp.join("project")
                .join("sase")
                .join("refs")
                .join("research.md"),
        );
        let home_refs_uri = file_uri(
            temp.join("home")
                .join(".config")
                .join("sase")
                .join("refs")
                .join("plans.md"),
        );
        let plugin_refs_uri = file_uri(
            temp.join("plugin")
                .join("sase_xprompts")
                .join("refs")
                .join("designs.md"),
        );
        let prose_uri = file_uri(
            temp.join("project")
                .join("sdd")
                .join("research")
                .join("202605")
                .join("memory_system_prior_art.md"),
        );

        assert!(should_invalidate_for_uri(&canonical_xprompts_uri));
        assert!(should_invalidate_for_uri(&legacy_xprompts_uri));
        assert!(should_invalidate_for_uri(&dot_xprompts_uri));
        assert!(should_invalidate_for_uri(&default_xprompts_uri));
        assert!(should_invalidate_for_uri(&canonical_refs_uri));
        assert!(should_invalidate_for_uri(&home_refs_uri));
        assert!(should_invalidate_for_uri(&plugin_refs_uri));
        assert!(should_invalidate_for_uri(&file_uri(
            temp.join("plugin").join("default_config.yml"),
        )));
        assert!(!should_invalidate_for_uri(&prose_uri));

        // Creating, editing, renaming, or deleting a memory note changes the
        // xprompt-memory catalog, so it must invalidate too.
        assert!(should_invalidate_for_uri(&file_uri(
            temp.join("project")
                .join("sase")
                .join("memory")
                .join("glossary.md"),
        )));
        assert!(should_invalidate_for_uri(&file_uri(
            temp.join("home").join("memory").join("glossary.md"),
        )));
        assert!(!should_invalidate_for_uri(&file_uri(
            temp.join("project")
                .join("sase")
                .join("memory")
                .join("assets")
                .join("diagram.md"),
        )));
    }

    #[tokio::test]
    async fn completes_xprompt_from_static_catalog() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();
        let response = server
            .completion_for_text(
                "#fo".to_string(),
                Position {
                    line: 0,
                    character: 3,
                },
            )
            .await
            .unwrap();

        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert!(items.iter().any(|item| item.label == "#foo"));
    }

    #[tokio::test]
    async fn completes_identity_and_clan_from_the_public_editor_surface() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();
        for (token, name, alias, description) in [
            (
                "%id",
                "id",
                "i",
                "Assign an agent ID with optional bead, clan, family, or user-managed tribe",
            ),
            (
                "%i",
                "id",
                "i",
                "Assign an agent ID with optional bead, clan, family, or user-managed tribe",
            ),
            ("%cla", "clan", "c", "Declare a new parallel agent clan"),
            ("%c", "clan", "c", "Declare a new parallel agent clan"),
        ] {
            let response = server
                .completion_for_text(
                    token.to_string(),
                    Position::new(0, token.len() as u32),
                )
                .await
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert_eq!(items.len(), 1, "{token}");
            let item = &items[0];
            let expected_detail = format!("alias %{alias}");
            assert_eq!(item.label, format!("%{name}"), "{token}");
            assert_eq!(item.kind, Some(CompletionItemKind::TEXT));
            assert_eq!(item.filter_text.as_deref(), Some(name));
            assert_eq!(item.detail.as_deref(), Some(expected_detail.as_str()));
            let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
            else {
                panic!("expected directive completion text edit");
            };
            assert_eq!(edit.range.start, Position::new(0, 0));
            assert_eq!(edit.range.end, Position::new(0, token.len() as u32));
            assert_eq!(edit.new_text, format!("%{name}"));
            let Some(Documentation::MarkupContent(documentation)) =
                item.documentation.as_ref()
            else {
                panic!("expected directive completion documentation");
            };
            assert_eq!(documentation.value, description);
        }
    }

    #[tokio::test]
    async fn removed_identity_directives_do_not_complete() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        for token in [
            "%name", "%n", "%family", "%f", "%group", "%g", "%tribe", "%t",
        ] {
            let response = server
                .completion_for_text(
                    token.to_string(),
                    Position::new(0, token.len() as u32),
                )
                .await
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert!(items.is_empty(), "{token}: {items:?}");
        }
    }

    #[tokio::test]
    async fn directive_keyword_completion_uses_the_active_fragment_range() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        for (text, cursor, start, keyword, expected_documentation) in [
            (
                "%clan(research, tr)",
                18,
                16,
                "tribe=",
                "Assign this clan to a user-managed tribe",
            ),
            (
                "%c(research, tr)",
                15,
                13,
                "tribe=",
                "Assign this clan to a user-managed tribe",
            ),
            (
                "%clan(research, su)",
                18,
                16,
                "summary=",
                "Attach a Rich-markup summary to this clan",
            ),
            (
                "%clan(research, su)",
                18,
                16,
                "summary_script=",
                "Generate this clan's summary with an executable script",
            ),
            (
                "%id(worker, cl)",
                14,
                12,
                "clan=",
                "Derive the full ID and join this agent clan",
            ),
            (
                "%i(worker, cl)",
                13,
                11,
                "clan=",
                "Derive the full ID and join this agent clan",
            ),
            (
                "%id(worker, fa)",
                14,
                12,
                "family=",
                "Attach this suffix to an existing agent family",
            ),
            (
                "%i(worker, tr)",
                13,
                11,
                "tribe=",
                "Assign this agent to a user-managed tribe",
            ),
        ] {
            let response = server
                .completion_for_text(text.to_string(), Position::new(0, cursor))
                .await
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert_text_completion_item(
                &items, keyword, start, cursor, keyword,
            );
            let item = items
                .iter()
                .find(|item| item.label == keyword)
                .unwrap_or_else(|| panic!("missing {keyword} completion"));
            let Some(Documentation::MarkupContent(item_documentation)) =
                item.documentation.as_ref()
            else {
                panic!("expected directive keyword documentation");
            };
            assert_eq!(item_documentation.value, expected_documentation);
        }

        for (text, cursor) in [
            ("%clan(re", 8),
            ("%clan(research, tribe=blue)", 26),
            ("%id(wo", 6),
            ("%id(worker, clan=research)", 25),
        ] {
            let response = server
                .completion_for_text(text.to_string(), Position::new(0, cursor))
                .await
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert!(items.is_empty(), "{text}: {items:?}");
        }
    }

    #[tokio::test]
    async fn completes_directive_argument_values() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let response = server
            .completion_for_text(
                "%effort:".to_string(),
                Position {
                    line: 0,
                    character: 8,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        let labels: Vec<&str> =
            items.iter().map(|item| item.label.as_str()).collect();
        assert_eq!(
            labels,
            vec!["none", "minimal", "low", "medium", "high", "xhigh", "max"]
        );
        assert_text_completion_item(&items, "high", 8, 8, "high");

        let response = server
            .completion_for_text(
                "%auto:t".to_string(),
                Position {
                    line: 0,
                    character: 7,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        let labels: Vec<&str> =
            items.iter().map(|item| item.label.as_str()).collect();
        assert_eq!(labels, vec!["plan", "tale", "epic"]);
        assert_text_completion_item(&items, "tale", 6, 7, "tale");
    }

    #[tokio::test]
    async fn wait_completion_uses_kind_aware_agent_catalog() {
        let mut bridge = bridge_with_catalog_entries(Vec::new());
        bridge.agent_catalog_response = serde_json::from_value(
            serde_json::json!({
                "schema_version": 1,
                "status": "ok",
                "message": "",
                "entries": [
                    {"name": "planner", "status": "RUNNING", "project": "sase"},
                    {"name": "review", "kind": "family", "member_count": 2, "detail": "family · 2 members"},
                    {"name": "builders", "kind": "clan", "member_count": 3, "detail": "clan · 3 members"},
                    {"name": "@ops", "kind": "tribe", "member_count": 4, "detail": "tribe · 4 agents"}
                ]
            }),
        )
        .unwrap();
        let (service, _) = LspService::new(move |client| {
            XpromptLspServer::with_bridge(client, Arc::new(bridge))
        });
        let server = service.inner();

        let text = "%wait(planner, ";
        let response = server
            .completion_for_text(
                text.to_string(),
                Position::new(0, text.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec![
                "time=",
                "runners=",
                "priority=",
                "@ops",
                "builders",
                "review"
            ]
        );
        assert_eq!(items[0].kind, Some(CompletionItemKind::KEYWORD));
        assert_eq!(items[3].kind, Some(CompletionItemKind::ENUM_MEMBER));
        assert_eq!(items[4].kind, Some(CompletionItemKind::MODULE));
        assert_eq!(items[5].kind, Some(CompletionItemKind::CLASS));
        assert_eq!(items[3].sort_text.as_deref(), Some("1:0003"));
        assert_eq!(
            items[4]
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("clan · 3 members")
        );

        let response = server
            .completion_for_text("%wait:op".to_string(), Position::new(0, 8))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, "@ops");
        assert_eq!(items[0].filter_text.as_deref(), Some("ops"));
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected tribe text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 6));
        assert_eq!(edit.new_text, "@ops");
    }

    #[tokio::test]
    async fn completes_placeholders_from_the_current_document() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        let response = server
            .completion_for_text(
                "<Beta> <bravo> choose <b>".to_string(),
                Position::new(0, 24),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        let labels: Vec<&str> =
            items.iter().map(|item| item.label.as_str()).collect();
        assert_eq!(labels, vec!["Beta", "bravo"]);
        assert_eq!(items[0].kind, Some(CompletionItemKind::VARIABLE));
        assert_eq!(items[0].filter_text.as_deref(), Some("b"));
        assert_eq!(items[0].sort_text.as_deref(), Some("0000"));
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected placeholder text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 23));
        assert_eq!(edit.range.end, Position::new(0, 25));
        assert_eq!(edit.new_text, "Beta>");
    }

    #[tokio::test]
    async fn placeholder_completion_appends_a_missing_closing_bracket() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        let response = server
            .completion_for_text(
                "<alpha> use <a".to_string(),
                Position::new(0, 14),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected placeholder text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 13));
        assert_eq!(edit.range.end, Position::new(0, 14));
        assert_eq!(edit.new_text, "alpha>");
    }

    #[tokio::test]
    async fn placeholder_completion_is_empty_without_another_span() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        let response = server
            .completion_for_text("<only>".to_string(), Position::new(0, 5))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn xprompt_snippet_completions_use_single_row_skeletons() {
        let entries = vec![
            catalog_entry(
                "many",
                "#many",
                Some("(path: path, mode: word)".to_string()),
                vec![
                    input_hint("path", "path", true, 0),
                    input_hint("mode", "word", true, 1),
                ],
                None,
            ),
            catalog_entry("none", "#none", None, Vec::new(), None),
            catalog_entry(
                "optional",
                "#optional",
                Some("(path?: path)".to_string()),
                vec![input_hint("path", "path", false, 0)],
                None,
            ),
            catalog_entry(
                "path",
                "#path",
                Some("(path: path)".to_string()),
                vec![input_hint("path", "path", true, 0)],
                None,
            ),
            catalog_entry(
                "text",
                "#text",
                Some("(body: text)".to_string()),
                vec![input_hint("body", "text", true, 0)],
                None,
            ),
        ];
        let items = snippet_completion_items(entries, "#", 1).await;

        assert_eq!(items.len(), 5);
        assert_snippet_item(&items, "#many", "#many($0)");
        assert_snippet_item(&items, "#none", "#none ");
        assert_snippet_item(&items, "#optional", "#optional ");
        assert_snippet_item(&items, "#path", "#path:");
        // End-of-line required-text completion appends the free-form delimiter
        // space (`#text:: `).
        assert_snippet_item(&items, "#text", "#text:: ");
    }

    #[tokio::test]
    async fn required_text_skeleton_keeps_double_colon_before_existing_text() {
        let entries = vec![catalog_entry(
            "text",
            "#text",
            Some("(body: text)".to_string()),
            vec![input_hint("body", "text", true, 0)],
            None,
        )];
        // `#text` token followed by more text on the line: the completion ends
        // mid-line, so the skeleton stays `#text::` and the following ` x`
        // supplies the single delimiter rather than doubling the space.
        let items = snippet_completion_items(entries, "#text x", 5).await;

        assert_eq!(items.len(), 1);
        assert_snippet_item(&items, "#text", "#text::");
    }

    #[tokio::test]
    async fn xprompt_snippet_completion_returns_one_row_per_match() {
        let entries = vec![catalog_entry(
            "foo",
            "#foo",
            Some("(path: path)".to_string()),
            vec![input_hint("path", "path", true, 0)],
            None,
        )];
        let items = snippet_completion_items(entries, "#fo", 3).await;

        assert_eq!(items.len(), 1);
        assert_snippet_item(&items, "#foo", "#foo:");
    }

    #[tokio::test]
    async fn bare_trigger_snippet_completion_uses_snippet_items() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_and_snippets(
                    Vec::new(),
                    vec![
                        snippet_entry(
                            "foo",
                            r"literal $ $1 \ brace } $0",
                            "ace.snippets",
                        ),
                        snippet_entry("bar", "bar", "ace.snippets"),
                    ],
                )),
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
            .completion_for_text(
                "fo".to_string(),
                Position {
                    line: 0,
                    character: 2,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        assert_snippet_item(&items, "foo", r"literal \$ $1 \\ brace \} $0");
        assert_eq!(items[0].detail.as_deref(), Some("ace.snippets"));
    }

    #[tokio::test]
    async fn placeholder_tabstop_snippet_item_retriggers_suggestions() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_and_snippets(
                    Vec::new(),
                    vec![snippet_entry("cbi", "`<$1>`$0", "ace.snippets")],
                )),
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
            .completion_for_text("cb".to_string(), Position::new(0, 2))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(
            items[0]
                .command
                .as_ref()
                .map(|command| command.command.as_str()),
            Some("editor.action.triggerSuggest")
        );
    }

    #[tokio::test]
    async fn bare_trigger_snippets_require_client_snippet_support() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_and_snippets(
                    Vec::new(),
                    vec![snippet_entry("foo", "$1$0", "ace.snippets")],
                )),
            )
        });
        let server = service.inner();

        let response = server
            .completion_for_text(
                "fo".to_string(),
                Position {
                    line: 0,
                    character: 2,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn snippet_clients_receive_identity_and_clan_forms() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
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

        for token in ["%clan", "%c"] {
            let response = server
                .completion_for_text(
                    token.to_string(),
                    Position::new(0, token.len() as u32),
                )
                .await
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert_snippet_item(&items, "%clan:...", "%clan:${1:name}$0");
            assert_snippet_item(
                &items,
                "%clan(..., tribe=...)",
                "%clan(${1:name}, tribe=${2:tribe})$0",
            );
            for item in items
                .iter()
                .filter(|item| item.kind == Some(CompletionItemKind::SNIPPET))
            {
                let Some(CompletionTextEdit::Edit(edit)) =
                    item.text_edit.as_ref()
                else {
                    panic!("expected clan snippet text edit");
                };
                assert_eq!(edit.range.start, Position::new(0, 0));
                assert_eq!(
                    edit.range.end,
                    Position::new(0, token.len() as u32)
                );
            }
        }

        let response = server
            .completion_for_text("%t".to_string(), Position::new(0, 2))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert!(
            items.is_empty(),
            "removed %t directive completed: {items:?}"
        );

        for token in ["%id", "%i"] {
            let response = server
                .completion_for_text(
                    token.to_string(),
                    Position::new(0, token.len() as u32),
                )
                .await
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert_snippet_item(&items, "%id:...", "%id:${1:agent-id}$0");
            assert_snippet_item(
                &items,
                "%id(..., clan=...)",
                "%id(${1:id}, clan=${2:clan})$0",
            );
            assert_snippet_item(
                &items,
                "%id(..., family=...)",
                "%id(${1:suffix}, family=${2:family})$0",
            );
            assert_snippet_item(
                &items,
                "%id(tribe=...)",
                "%id(tribe=${1:tribe})$0",
            );
            assert!(!items.iter().any(|item| item.label.starts_with("%name")
                || item.label.starts_with("%n:")
                || item.label.starts_with("%tribe")
                || item.label.starts_with("%t:")));
        }
    }

    #[test]
    fn directive_snippet_for_alt_uses_brace_shorthand() {
        let range = CoreRange {
            start: CorePosition {
                line: 0,
                character: 0,
            },
            end: CorePosition {
                line: 0,
                character: 4,
            },
        };
        let items = directive_snippet_items(Some("%alt"), range);
        let alt = items
            .iter()
            .find(|item| item.label == "%alt:...")
            .expect("alt directive snippet item");
        assert_eq!(alt.kind, Some(CompletionItemKind::SNIPPET));
        assert_eq!(alt.insert_text_format, Some(InsertTextFormat::SNIPPET));
        let Some(CompletionTextEdit::Edit(edit)) = alt.text_edit.as_ref()
        else {
            panic!("expected text edit for alt snippet");
        };
        assert_eq!(edit.new_text.as_str(), "%{${1:A} | ${2:B}\\}$0");

        // No directive snippet should still emit the legacy `%(...)` spelling.
        for item in &items {
            if let Some(CompletionTextEdit::Edit(edit)) =
                item.text_edit.as_ref()
            {
                assert!(
                    !edit.new_text.contains("%("),
                    "directive snippet still advertises %(: {}",
                    edit.new_text
                );
            }
        }
    }

    #[tokio::test]
    async fn identity_and_clan_editor_surfaces_use_current_metadata() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        for (text, cursor, heading, description) in [
            (
                "%i(worker, family=review)",
                13,
                "**%id**",
                "Assign an agent ID with optional bead, clan, family, or user-managed tribe",
            ),
            (
                "%c(research, tr)",
                15,
                "**%clan**",
                "Declare a new parallel agent clan",
            ),
        ] {
            let hover = server
                .hover_for_text(text.to_string(), Position::new(0, cursor))
                .await
                .unwrap_or_else(|| panic!("missing hover for {text}"));
            let Hover {
                contents: lsp_types::HoverContents::Markup(markup),
                ..
            } = hover
            else {
                panic!("expected markdown hover");
            };
            assert!(markup.value.contains(heading), "{text}");
            assert!(markup.value.contains(description), "{text}");
        }

        for text in ["%tribe:research", "%t:research"] {
            assert!(
                server
                    .hover_for_text(text.to_string(), Position::new(0, 1))
                    .await
                    .is_none(),
                "removed directive should not hover: {text}"
            );
        }

        let current = server
            .diagnostics_for_text(
                "%id(worker, clan=research) %id(worker, family=review) %id(worker, tribe=review) %id(tribe=review) %i:worker %clan(research.@, tribe=research) %c:research".to_string(),
            )
            .await;
        assert!(!current.iter().any(|diagnostic| matches!(
            diagnostic.code.as_ref(),
            Some(lsp_types::NumberOrString::String(code)) if code == "unknown_directive"
        )));

        let removed = server
            .diagnostics_for_text(
                "%name:x %n:x %family:x %f:x %group:x %g:x %tribe:x %t:x %wat:x".to_string(),
            )
            .await;
        assert_eq!(
            removed
                .iter()
                .filter(|diagnostic| matches!(
                    diagnostic.code.as_ref(),
                    Some(lsp_types::NumberOrString::String(code)) if code == "unknown_directive"
                ))
                .count(),
            9
        );
    }

    #[tokio::test]
    async fn exposes_hover_diagnostics_code_actions_and_definition() {
        let source_path = std::env::current_dir().unwrap().join("Cargo.toml");

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(Some(
                    source_path.to_string_lossy().into_owned(),
                ))),
            )
        });
        let server = service.inner();

        let hover = server
            .hover_for_text(
                "#foo".to_string(),
                Position {
                    line: 0,
                    character: 2,
                },
            )
            .await
            .unwrap();
        let Hover {
            contents: lsp_types::HoverContents::Markup(markup),
            ..
        } = hover
        else {
            panic!("expected markdown hover");
        };
        assert!(markup.value.contains("Foo prompt"));

        let frontmatter_hover = server
            .hover_for_text(
                "---\nxprompts:\n  _helper:\n    content: Helper\n---\nBody\n"
                    .to_string(),
                Position {
                    line: 1,
                    character: 2,
                },
            )
            .await
            .unwrap();
        let Hover {
            contents: lsp_types::HoverContents::Markup(frontmatter_markup),
            range: Some(frontmatter_range),
        } = frontmatter_hover
        else {
            panic!("expected markdown frontmatter hover with range");
        };
        assert_eq!(
            frontmatter_range,
            Range {
                start: Position {
                    line: 1,
                    character: 0,
                },
                end: Position {
                    line: 1,
                    character: 8,
                },
            }
        );
        assert!(frontmatter_markup.value.contains("local xprompts"));
        assert!(frontmatter_markup.value.contains("current file"));

        let diagnostics = server
            .diagnostics_for_text("#missing %wat".to_string())
            .await;
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("Unknown xprompt")));
        assert!(diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .contains("Unknown directive")));

        let missing_arg_diagnostics =
            server.diagnostics_for_text("#foo".to_string()).await;
        assert!(missing_arg_diagnostics.iter().any(|diagnostic| {
            diagnostic.source.as_deref() == Some("sase-xprompt")
                && diagnostic.severity
                    == Some(lsp_types::DiagnosticSeverity::ERROR)
                && matches!(
                    diagnostic.code.as_ref(),
                    Some(lsp_types::NumberOrString::String(code))
                        if code == "missing_required_arg"
                )
        }));

        let invalid_type_diagnostics = server
            .diagnostics_for_text("#foo(path=\"bad value\")".to_string())
            .await;
        assert!(invalid_type_diagnostics.iter().any(|diagnostic| {
            diagnostic.source.as_deref() == Some("sase-xprompt")
                && diagnostic.severity
                    == Some(lsp_types::DiagnosticSeverity::ERROR)
                && matches!(
                    diagnostic.code.as_ref(),
                    Some(lsp_types::NumberOrString::String(code))
                        if code == "invalid_xprompt_arg_type"
                )
        }));

        let uri = Uri::from_file_path(&source_path).unwrap();
        let actions = server
            .code_actions_for_text(
                uri.clone(),
                "#!foo".to_string(),
                Range {
                    start: Position {
                        line: 0,
                        character: 1,
                    },
                    end: Position {
                        line: 0,
                        character: 1,
                    },
                },
            )
            .await;
        assert!(actions.iter().any(|action| match action {
            CodeActionOrCommand::CodeAction(action) =>
                action.title.contains("canonical"),
            CodeActionOrCommand::Command(command) =>
                command.command == REFRESH_COMMAND,
        }));
        assert!(actions.iter().any(|action| match action {
            CodeActionOrCommand::CodeAction(action) =>
                action.title == "Insert required named args",
            CodeActionOrCommand::Command(_) => false,
        }));

        let definition = server
            .definition_for_text(
                "#foo".to_string(),
                Position {
                    line: 0,
                    character: 2,
                },
            )
            .await
            .unwrap();
        let GotoDefinitionResponse::Scalar(location) = definition else {
            panic!("expected scalar definition");
        };
        assert_eq!(location.uri, uri);
    }

    #[tokio::test]
    async fn diagnostics_for_uri_text_honors_canonical_memory_file_uri() {
        let temp = tempfile::tempdir().unwrap();
        let memory_dir = temp.path().join("sase/memory");
        fs::create_dir_all(&memory_dir).unwrap();
        let memory_uri =
            Uri::from_file_path(memory_dir.join("generated_skills.md"))
                .unwrap();
        let normal_uri = Uri::from_file_path(
            temp.path().join("sase/xprompts").join("foo.md"),
        )
        .unwrap();
        let text = "---\nkeywords: [topic]\n---\nBody".to_string();

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let memory_diagnostics = server
            .diagnostics_for_uri_text(&memory_uri, text.clone())
            .await;
        assert!(
            !diagnostics_contain_code(
                &memory_diagnostics,
                "missing_xprompt_memory_tag"
            ),
            "{memory_diagnostics:?}"
        );

        let normal_diagnostics =
            server.diagnostics_for_uri_text(&normal_uri, text).await;
        assert!(
            !diagnostics_contain_code(
                &normal_diagnostics,
                "missing_xprompt_memory_tag"
            ),
            "{normal_diagnostics:?}"
        );
    }

    #[tokio::test]
    async fn diagnostics_for_uri_text_accepts_markdown_local_xprompts() {
        let temp = tempfile::tempdir().unwrap();
        let uri = Uri::from_file_path(
            temp.path().join("sase/xprompts").join("reads.md"),
        )
        .unwrap();
        let text = "---\nxprompts:\n  _article_search_agent:\n    input:\n      topic: word\n    content: Search {{ topic }}\n---\n#_article_search_agent(news)\n"
            .to_string();

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();

        let diagnostics = server.diagnostics_for_uri_text(&uri, text).await;
        assert!(
            diagnostics.iter().all(|diagnostic| {
                !matches!(
                    diagnostic.code.as_ref(),
                    Some(lsp_types::NumberOrString::String(code))
                        if code == "unknown_xprompt"
                ) || !diagnostic.message.contains("_article_search_agent")
            }),
            "{diagnostics:?}"
        );
    }

    #[tokio::test]
    async fn definition_uses_definition_path_outside_workspace_root() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("outside-workspace.md");
        fs::write(&source_path, "source").unwrap();

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(Some(
                    source_path.to_string_lossy().into_owned(),
                ))),
            )
        });
        let server = service.inner();

        let definition = server
            .definition_for_text(
                "#foo".to_string(),
                Position {
                    line: 0,
                    character: 2,
                },
            )
            .await
            .unwrap();

        let GotoDefinitionResponse::Scalar(location) = definition else {
            panic!("expected scalar definition");
        };
        assert_eq!(location.uri, Uri::from_file_path(source_path).unwrap());
        assert_eq!(location.range, zero_range());
    }

    #[tokio::test]
    async fn definition_preserves_catalog_definition_range() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("sase/sase.yml");
        fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        fs::write(&source_path, "xprompts:\n  foo:\n    content: body\n")
            .unwrap();
        let mut entry = catalog_entry(
            "foo",
            "#foo",
            None,
            Vec::new(),
            Some(source_path.to_string_lossy().into_owned()),
        );
        entry.definition_range = Some(CoreRange {
            start: CorePosition {
                line: 1,
                character: 2,
            },
            end: CorePosition {
                line: 1,
                character: 5,
            },
        });

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(vec![entry])),
            )
        });
        let server = service.inner();

        let definition = server
            .definition_for_text(
                "#foo".to_string(),
                Position {
                    line: 0,
                    character: 2,
                },
            )
            .await
            .unwrap();

        let GotoDefinitionResponse::Scalar(location) = definition else {
            panic!("expected scalar definition");
        };
        assert_eq!(
            location.range,
            Range {
                start: Position {
                    line: 1,
                    character: 2,
                },
                end: Position {
                    line: 1,
                    character: 5,
                },
            }
        );
    }

    #[tokio::test]
    async fn definition_returns_none_for_pseudo_or_missing_sources() {
        for definition_path in [None, Some("plugin:module/name".to_string())] {
            let (service, _) = LspService::new(|client| {
                XpromptLspServer::with_bridge(
                    client,
                    Arc::new(bridge_with_catalog(definition_path.clone())),
                )
            });
            let server = service.inner();

            assert_eq!(
                server
                    .definition_for_text(
                        "#foo".to_string(),
                        Position {
                            line: 0,
                            character: 2,
                        },
                    )
                    .await,
                None
            );
        }
    }

    #[test]
    fn detects_snippet_support_from_client_capabilities() {
        let capabilities = ClientCapabilities {
            text_document: Some(TextDocumentClientCapabilities {
                completion: Some(CompletionClientCapabilities {
                    completion_item: Some(CompletionItemCapability {
                        snippet_support: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(snippet_support(&capabilities));
    }

    async fn snippet_completion_items(
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
            .completion_for_text(
                text.to_string(),
                Position { line: 0, character },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        items
    }

    fn assert_snippet_item(
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
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected text edit for {label}");
        };
        assert_eq!(edit.new_text.as_str(), new_text);
    }

    fn assert_text_completion_item(
        items: &[CompletionItem],
        label: &str,
        start_character: u32,
        end_character: u32,
        new_text: &str,
    ) {
        let item = items
            .iter()
            .find(|item| item.label == label)
            .unwrap_or_else(|| panic!("missing completion item {label}"));
        assert_eq!(item.kind, Some(CompletionItemKind::TEXT));
        assert_eq!(item.filter_text.as_deref(), Some(label));
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
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

    fn write_model_catalog(path: &Path) {
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

    fn write_enriched_model_catalog(path: &Path) {
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
                    }
                ]
            }"#,
        )
        .unwrap();
    }

    #[test]
    fn load_model_catalog_rejects_unknown_schema() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        fs::write(&catalog_path, r#"{"schema_version": 99, "entries": []}"#)
            .unwrap();

        let entries = load_model_catalog(Some(&catalog_path));

        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn completes_model_directive_values_from_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        write_model_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("%model:".to_string(), Position::new(0, 7))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 3);
        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["claude-fable-5", "gpt-5.6-sol", "gpt-5.5"]
        );
        let item = &items[0];
        assert_eq!(item.label, "claude-fable-5");
        assert_eq!(item.filter_text.as_deref(), Some("claude-fable-5"));
        assert_eq!(item.kind, Some(CompletionItemKind::VALUE));
        assert_eq!(item.detail.as_deref(), Some("claude"));
        assert_eq!(item.sort_text.as_deref(), Some("0:0000"));
        assert_eq!(
            item.label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("model")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected model documentation");
        };
        assert_eq!(documentation.value, "Claude (fable)");
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 7));
        assert_eq!(edit.range.end, Position::new(0, 7));
        assert_eq!(edit.new_text, "claude-fable-5");
    }

    #[tokio::test]
    async fn enriched_model_catalog_renders_alias_detail_and_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        write_enriched_model_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("%model:".to_string(), Position::new(0, 7))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["opus", "gpt-5.6-sol", "@default", "@claude_coder", "@scout"]
        );
        assert_eq!(
            items.iter().map(|item| item.kind).collect::<Vec<_>>(),
            vec![
                Some(CompletionItemKind::VALUE),
                Some(CompletionItemKind::VALUE),
                Some(CompletionItemKind::ENUM_MEMBER),
                Some(CompletionItemKind::ENUM_MEMBER),
                Some(CompletionItemKind::ENUM_MEMBER),
            ]
        );
        assert_eq!(
            items
                .iter()
                .map(|item| item.sort_text.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["0:0000", "0:0001", "1:0002", "1:0003", "1:0004"]
        );

        let default =
            items.iter().find(|item| item.label == "@default").unwrap();
        assert_eq!(default.detail.as_deref(), Some("CLAUDE(opus) @ high"));
        assert_eq!(
            default
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("default")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            default.documentation.as_ref()
        else {
            panic!("expected alias documentation");
        };
        assert_eq!(
            documentation.value,
            "Default model for prompts.\n\n\
             **Provenance:** implicit → @coder @ medium"
        );

        let coder = items
            .iter()
            .find(|item| item.label == "@claude_coder")
            .unwrap();
        assert_eq!(
            coder
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("coder")
        );

        let scout = items.iter().find(|item| item.label == "@scout").unwrap();
        assert_eq!(scout.detail.as_deref(), Some("CODEX(gpt-5.6-sol) @ low"));
        assert_eq!(
            scout
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("custom")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            scout.documentation.as_ref()
        else {
            panic!("expected pooled alias documentation");
        };
        assert_eq!(
            documentation.value,
            "Fast scouting pool.\n\n\
             **Provenance:** configured\n\n\
             **Config:** `llm_provider.model_aliases.custom.scout`\n\n\
             **Bucket:** `fast`\n\n\
             **Pool:** 2/3 available"
        );
    }

    #[tokio::test]
    async fn leading_at_filters_model_completion_to_aliases() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        write_enriched_model_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("%model:@".to_string(), Position::new(0, 8))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["@default", "@claude_coder", "@scout"]
        );
        assert!(items
            .iter()
            .all(|item| item.kind == Some(CompletionItemKind::ENUM_MEMBER)));
    }

    #[tokio::test]
    async fn stale_v1_alias_catalog_still_produces_items() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        fs::write(
            &catalog_path,
            r#"{
                "schema_version": 1,
                "entries": [{
                    "value": "@default",
                    "display": "@default",
                    "description": "alias for the default model",
                    "kind": "implicit_alias",
                    "provider": "",
                    "aliases": ["default"]
                }]
            }"#,
        )
        .unwrap();
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("%model:def".to_string(), Position::new(0, 10))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, "@default");
        assert_eq!(items[0].filter_text.as_deref(), Some("default"));
        assert_eq!(items[0].kind, Some(CompletionItemKind::ENUM_MEMBER));
        assert_eq!(
            items[0].detail.as_deref(),
            Some("alias for the default model")
        );
    }

    #[tokio::test]
    async fn model_directive_completion_filters_by_alias_hint() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        write_model_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("%model:fa".to_string(), Position::new(0, 9))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.label, "claude-fable-5");
        assert_eq!(item.filter_text.as_deref(), Some("fable"));
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 7));
        assert_eq!(edit.range.end, Position::new(0, 9));
        assert_eq!(edit.new_text, "claude-fable-5");
    }

    #[tokio::test]
    async fn model_directive_completion_without_catalog_is_empty() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = None;
        }

        let response = server
            .completion_for_text("%model:".to_string(), Position::new(0, 7))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn model_at_suffix_still_completes_effort_vocabulary() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("model_catalog.json");
        write_model_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.model_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text(
                "%model:opus@".to_string(),
                Position::new(0, 12),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert!(items.iter().any(|item| item.label == "xhigh"));
        let xhigh = items.iter().find(|item| item.label == "xhigh").unwrap();
        let Some(CompletionTextEdit::Edit(edit)) = xhigh.text_edit.as_ref()
        else {
            panic!("expected text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 12));
        assert_eq!(edit.range.end, Position::new(0, 12));
        assert_eq!(edit.new_text, "xhigh");
    }

    // --- vcs_project (`+`) completion --------------------------------------

    fn write_vcs_project_catalog(path: &Path) {
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

    fn write_vcs_project_catalog_with_pr(path: &Path) {
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

    fn write_vcs_ref_catalog(path: &Path) {
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

    fn write_glossary_catalog(
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

    fn write_artifact_ref_catalog(
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

    fn completion_items(response: CompletionResponse) -> Vec<CompletionItem> {
        match response {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => list.items,
        }
    }

    fn absolute_semantic_tokens(
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

    fn repo_entry(
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

    fn vcs_repo_catalog_response(
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

    #[tokio::test]
    async fn advertises_plus_completion_trigger_character() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let result = server
            .initialize(InitializeParams::default())
            .await
            .unwrap();
        let triggers = result
            .capabilities
            .completion_provider
            .and_then(|completion| completion.trigger_characters)
            .unwrap_or_default();

        assert!(triggers.contains(&"+".to_string()), "{triggers:?}");
    }

    #[tokio::test]
    async fn advertises_placeholder_completion_trigger_character() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let result = server
            .initialize(InitializeParams::default())
            .await
            .unwrap();
        let triggers = result
            .capabilities
            .completion_provider
            .and_then(|completion| completion.trigger_characters)
            .unwrap_or_default();

        assert!(triggers.contains(&"<".to_string()), "{triggers:?}");
    }

    #[tokio::test]
    async fn advertises_at_reference_completion_trigger_character() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let result = server
            .initialize(InitializeParams::default())
            .await
            .unwrap();
        let triggers = result
            .capabilities
            .completion_provider
            .and_then(|completion| completion.trigger_characters)
            .unwrap_or_default();

        assert!(triggers.contains(&"@".to_string()), "{triggers:?}");
    }

    #[tokio::test]
    async fn advertises_slash_completion_trigger_character() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let result = server
            .initialize(InitializeParams::default())
            .await
            .unwrap();
        let triggers = result
            .capabilities
            .completion_provider
            .and_then(|completion| completion.trigger_characters)
            .unwrap_or_default();

        assert!(triggers.contains(&"/".to_string()), "{triggers:?}");
    }

    #[tokio::test]
    async fn advertises_vcs_ref_completion_trigger_characters() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let result = server
            .initialize(InitializeParams::default())
            .await
            .unwrap();
        let triggers = result
            .capabilities
            .completion_provider
            .and_then(|completion| completion.trigger_characters)
            .unwrap_or_default();

        assert!(triggers.contains(&":".to_string()), "{triggers:?}");
        assert!(triggers.contains(&"(".to_string()), "{triggers:?}");
    }

    #[tokio::test]
    async fn advertises_full_semantic_tokens_with_standard_legend() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog(None)),
            )
        });
        let server = service.inner();

        let result = server
            .initialize(InitializeParams::default())
            .await
            .unwrap();
        let provider = result
            .capabilities
            .semantic_tokens_provider
            .expect("semantic tokens provider");
        let SemanticTokensServerCapabilities::SemanticTokensOptions(options) =
            provider
        else {
            panic!("expected semantic token options");
        };

        assert_eq!(
            options
                .legend
                .token_types
                .iter()
                .map(|token_type| token_type.as_str())
                .collect::<Vec<_>>(),
            vec!["namespace", "string", "number", "type"]
        );
        assert_eq!(
            options
                .legend
                .token_modifiers
                .iter()
                .map(|modifier| modifier.as_str())
                .collect::<Vec<_>>(),
            vec!["documentation"]
        );
        assert!(matches!(
            options.full,
            Some(SemanticTokensFullOptions::Bool(true))
        ));
        assert_eq!(options.range, None);
    }

    #[test]
    fn loads_v1_vcs_project_catalog_with_project_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        fs::write(
            &catalog_path,
            r##"{
                "schema_version": 1,
                "workflow_names": ["gh"],
                "entries": [
                    {
                        "name": "sase",
                        "vcs_prefix": "gh",
                        "display_tag": "#gh:sase",
                        "provider_display": "GitHub",
                        "description": "",
                        "aliases": []
                    }
                ]
            }"##,
        )
        .unwrap();

        let catalog = load_vcs_project_catalog(Some(&catalog_path));

        assert_eq!(catalog.workflow_names, vec!["gh"]);
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].kind, "project");
        assert_eq!(catalog.entries[0].project, "");
        assert_eq!(catalog.entries[0].status, "");
        assert!(catalog.namespaces.is_empty());
    }

    #[test]
    fn loads_v3_vcs_project_catalog_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        fs::write(
            &catalog_path,
            r##"{
                "schema_version": 3,
                "workflow_names": ["gh", "git"],
                "entries": [],
                "namespaces": {
                    "gh": [
                        {
                            "name": "sase-org",
                            "description": "2 enabled projects",
                            "kind_label": "org"
                        },
                        {
                            "name": "bbugyi200"
                        }
                    ]
                }
            }"##,
        )
        .unwrap();

        let catalog = load_vcs_project_catalog(Some(&catalog_path));

        assert_eq!(catalog.workflow_names, vec!["gh", "git"]);
        assert!(catalog.entries.is_empty());
        let namespaces = catalog.namespaces.get("gh").unwrap();
        assert_eq!(namespaces.len(), 2);
        assert_eq!(namespaces[0].name, "sase-org");
        assert_eq!(namespaces[0].description, "2 enabled projects");
        assert_eq!(namespaces[0].kind_label, "org");
        assert_eq!(namespaces[1].name, "bbugyi200");
        assert_eq!(namespaces[1].kind_label, "org");
    }

    #[test]
    fn loads_v4_vcs_project_catalog_with_patch_entry_kind() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        fs::write(
            &catalog_path,
            r##"{
                "schema_version": 4,
                "workflow_names": ["gh"],
                "entries": [
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

        let catalog = load_vcs_project_catalog(Some(&catalog_path));

        assert_eq!(catalog.workflow_names, vec!["gh"]);
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].entry_kind, "patch");
        assert_eq!(catalog.entries[0].kind, "changespec");
        assert_eq!(catalog.entries[0].project, "sase");
        assert_eq!(catalog.entries[0].status, "Ready");
    }

    #[test]
    fn load_vcs_project_catalog_ignores_malformed_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        fs::write(
            &catalog_path,
            r##"{
                "schema_version": 3,
                "workflow_names": ["gh"],
                "entries": [
                    {
                        "name": "sase",
                        "vcs_prefix": "gh",
                        "display_tag": "#gh:sase",
                        "provider_display": "GitHub"
                    }
                ],
                "namespaces": ["not", "a", "map"]
            }"##,
        )
        .unwrap();

        let catalog = load_vcs_project_catalog(Some(&catalog_path));

        assert_eq!(catalog.workflow_names, vec!["gh"]);
        assert_eq!(catalog.entries.len(), 1);
        assert!(catalog.namespaces.is_empty());
    }

    #[test]
    fn load_vcs_project_catalog_rejects_unknown_schema() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        fs::write(
            &catalog_path,
            r#"{"schema_version": 99, "workflow_names": ["gh"], "entries": []}"#,
        )
        .unwrap();

        let catalog = load_vcs_project_catalog(Some(&catalog_path));

        assert!(catalog.entries.is_empty());
        assert!(catalog.workflow_names.is_empty());
        assert!(catalog.namespaces.is_empty());
    }

    #[test]
    fn artifact_catalog_loader_is_tolerant_and_schema_gated() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("artifact_ref_catalog.json");

        assert_eq!(
            load_artifact_ref_catalog(None),
            ArtifactRefCatalog::default()
        );
        fs::write(&catalog_path, "{not json").unwrap();
        assert_eq!(
            load_artifact_ref_catalog(Some(&catalog_path)),
            ArtifactRefCatalog::default()
        );
        fs::write(&catalog_path, r#"{"schema_version":99,"projects":[]}"#)
            .unwrap();
        assert_eq!(
            load_artifact_ref_catalog(Some(&catalog_path)),
            ArtifactRefCatalog::default()
        );
        fs::write(
            &catalog_path,
            r#"{
                "schema_version": 1,
                "default_project": "sase",
                "projects": [
                    {"name": "broken"},
                    {
                        "name": "sase",
                        "key": "key_sase",
                        "context": {"schema_version": 1, "document_roots": []}
                    }
                ]
            }"#,
        )
        .unwrap();

        let catalog = load_artifact_ref_catalog(Some(&catalog_path));

        assert_eq!(catalog.default_project.as_deref(), Some("sase"));
        assert_eq!(catalog.projects.len(), 1);
        assert_eq!(catalog.projects[0].key, "key_sase");
    }

    #[tokio::test]
    async fn completes_artifact_kinds_and_local_payloads_per_active_project() {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        let vcs_path = temp.path().join("vcs_project_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("local"));
        write_vcs_ref_catalog(&vcs_path);
        for project in ["sase", "local"] {
            let project_root = temp.path().join(project);
            fs::create_dir_all(project_root.join("designs")).unwrap();
            fs::create_dir_all(project_root.join("chats/202607")).unwrap();
            fs::write(
                project_root.join("designs").join(format!("{project}.md")),
                project,
            )
            .unwrap();
            fs::write(project_root.join("chats/202607/agent.md"), project)
                .unwrap();
            fs::write(
                project_root.join("artifact-index.jsonl"),
                format!(
                    "{{\"schema_version\":1,\"artifact\":{{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/{project}/image.png\"}}}}\n"
                ),
            )
            .unwrap();
        }
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.artifact_ref_catalog = Some(artifact_path.clone());
            config.vcs_project_catalog = Some(vcs_path);
            config.project = Some("sase_18".to_string());
        }

        for (text, expected) in [
            ("#gh:ship-completion @designs:sa", "@designs:sase.md"),
            ("#git:local @designs:lo", "@designs:local.md"),
            ("@designs:lo", "@designs:local.md"),
            ("@chat:202607/a", "@chat:202607/agent.md"),
            ("@file:default:", "@file:default:52895d68931185056fd0e49f"),
        ] {
            let items = completion_items(
                server
                    .completion_for_text(
                        text.to_string(),
                        Position::new(0, text.len() as u32),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(items.len(), 1, "{text}: {items:?}");
            assert_eq!(items[0].label, expected, "{text}");
        }

        let kind_items = completion_items(
            server
                .completion_for_text("@de".to_string(), Position::new(0, 3))
                .await
                .unwrap(),
        );
        assert_eq!(kind_items.len(), 1);
        assert_eq!(kind_items[0].label, "@designs:");

        let payload_text = "@designs:lo";
        let payload_items = completion_items(
            server
                .completion_for_text(
                    payload_text.to_string(),
                    Position::new(0, payload_text.len() as u32),
                )
                .await
                .unwrap(),
        );
        let Some(CompletionTextEdit::Edit(edit)) =
            payload_items[0].text_edit.as_ref()
        else {
            panic!("expected artifact payload text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 0));
        assert_eq!(edit.range.end, Position::new(0, 11));

        for text in ["@commit:sase@0123456", "@bug:sase#1"] {
            let items = completion_items(
                server
                    .completion_for_text(
                        text.to_string(),
                        Position::new(0, text.len() as u32),
                    )
                    .await
                    .unwrap(),
            );
            assert!(items.is_empty(), "{text}: {items:?}");
        }

        write_artifact_ref_catalog(&artifact_path, temp.path(), None);
        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:sa".to_string(),
                    Position::new(0, 11),
                )
                .await
                .unwrap(),
        );
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, "@designs:sase.md");
    }

    fn git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn init_commit_git_repo(repo: &Path) {
        fs::create_dir_all(repo).unwrap();
        git(repo, &["init", "--quiet"]);
        git(repo, &["config", "user.name", "Commit Test"]);
        git(repo, &["config", "user.email", "commit@example.com"]);
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args([
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                "fix(stats): expose occupancy",
            ])
            .env("GIT_AUTHOR_DATE", "1700000000 +0000")
            .env("GIT_COMMITTER_DATE", "1700000000 +0000")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn completes_commit_payloads_from_a_real_git_checkout() {
        let temp = tempfile::tempdir().unwrap();
        let checkout = temp.path().join("sase-core-checkout");
        init_commit_git_repo(&checkout);

        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        fs::write(
            &artifact_path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "default_project": "sase",
                "projects": [{
                    "name": "sase",
                    "key": "key_sase",
                    "context": {
                        "schema_version": 1,
                        "repositories": [{
                            "name": "sase-core",
                            "checkout_paths": [checkout.to_string_lossy()],
                        }],
                    },
                }],
            }))
            .unwrap(),
        )
        .unwrap();

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        server.config.write().unwrap().artifact_ref_catalog =
            Some(artifact_path);

        // The regression this phase fixes: `commit` used to short-circuit to
        // an empty inventory before the payload cache was ever consulted.
        let text = "@commit:sase-core@fix";
        let items = completion_items(
            server
                .completion_for_text(
                    text.to_string(),
                    Position::new(0, text.len() as u32),
                )
                .await
                .unwrap(),
        );
        assert!(!items.is_empty(), "{text}: expected ranked commit items");
        assert!(items[0].label.starts_with("@commit:sase-core@"));
        assert_eq!(items[0].kind, Some(CompletionItemKind::REFERENCE));
        assert_eq!(
            items[0]
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("commit")
        );
    }

    #[tokio::test]
    async fn artifact_payload_inventory_cache_rebuilds_on_all_invalidation_paths(
    ) {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
        let designs = temp.path().join("sase/designs");
        fs::create_dir_all(&designs).unwrap();
        fs::write(designs.join("first.md"), "first").unwrap();

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        server.config.write().unwrap().artifact_ref_catalog =
            Some(artifact_path.clone());

        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:first".to_string(),
                    Position::new(0, 14),
                )
                .await
                .unwrap(),
        );
        assert_eq!(items.len(), 1);

        // A stable catalog signature reuses the cached filesystem inventory.
        fs::write(designs.join("second.md"), "second").unwrap();
        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:second".to_string(),
                    Position::new(0, 15),
                )
                .await
                .unwrap(),
        );
        assert!(items.is_empty());

        // A launcher catalog rewrite invalidates by path metadata.
        let mut raw = fs::read(&artifact_path).unwrap();
        raw.push(b'\n');
        fs::write(&artifact_path, raw).unwrap();
        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:second".to_string(),
                    Position::new(0, 15),
                )
                .await
                .unwrap(),
        );
        assert_eq!(items.len(), 1);

        // The explicit refresh command invalidates even when the catalog file
        // itself is unchanged.
        fs::write(designs.join("third.md"), "third").unwrap();
        server.refresh_catalog_explicit().await;
        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:third".to_string(),
                    Position::new(0, 14),
                )
                .await
                .unwrap(),
        );
        assert_eq!(items.len(), 1);

        // The short TTL eventually notices sidecar writes that do not touch
        // the launcher catalog.
        fs::write(designs.join("fourth.md"), "fourth").unwrap();
        server.artifact_ref_cache.write().unwrap().loaded_at =
            Some(Instant::now() - ARTIFACT_REF_CACHE_TTL);
        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:fourth".to_string(),
                    Position::new(0, 15),
                )
                .await
                .unwrap(),
        );
        assert_eq!(items.len(), 1);
    }

    #[tokio::test]
    async fn artifact_completion_discloses_the_display_cap() {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
        let designs = temp.path().join("sase/designs");
        fs::create_dir_all(&designs).unwrap();
        for index in 0..205 {
            fs::write(designs.join(format!("{index:03}.md")), "design")
                .unwrap();
        }

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        server.config.write().unwrap().artifact_ref_catalog =
            Some(artifact_path);

        let items = completion_items(
            server
                .completion_for_text(
                    "@designs:".to_string(),
                    Position::new(0, 9),
                )
                .await
                .unwrap(),
        );

        assert_eq!(
            items.len(),
            sase_core::editor::at_reference::AT_REFERENCE_MAX_GROUP_ROWS
        );
        assert!(items.iter().all(|item| item
            .detail
            .as_deref()
            .is_some_and(|detail| detail
                .contains("at least 5 additional payloads not shown"))));
    }

    #[tokio::test]
    async fn fuzzy_at_reference_payloads_survive_client_filtering() {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
        let bundle = temp
            .path()
            .join("sase/designs/202607/sase_sites_hub_and_pages");
        fs::create_dir_all(&bundle).unwrap();
        fs::write(
            bundle.join("sase_sites_hub_and_pages.md"),
            "---\ntitle: SASE Sites Hub and Pages\n---\n",
        )
        .unwrap();

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        server.config.write().unwrap().artifact_ref_catalog =
            Some(artifact_path);

        let text = "@designs:site";
        let response = server
            .completion_for_text(
                text.to_string(),
                Position::new(0, text.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::List(list) = &response else {
            panic!("expected an incomplete list: {response:?}");
        };
        assert!(list.is_incomplete);

        let payload = "202607/sase_sites_hub_and_pages/\
                       sase_sites_hub_and_pages.md";
        let items = completion_items(response);
        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec![format!("@designs:{payload}").as_str()]
        );
        // Every item filters on the typed text, so a client that prefix-filters
        // `filterText` against the typed word keeps the server-ranked rows.
        assert!(items
            .iter()
            .all(|item| item.filter_text.as_deref() == Some(text)));
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected an artifact payload text edit");
        };
        assert_eq!(edit.new_text, format!("@designs:{payload}"));
        let Some(lsp_types::Documentation::MarkupContent(documentation)) =
            items[0].documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        // The matched run is bolded in the basename the query was aimed at, and
        // the document frontmatter title is carried into the preview.
        assert_eq!(
            documentation.value,
            "202607/sase_sites_hub_and_pages/sase_**site**s_hub_and_pages.md\n\nSASE Sites Hub and Pages"
        );
        assert_eq!(
            items[0]
                .label_details
                .as_ref()
                .and_then(|details| details.detail.as_deref()),
            Some(" · SASE Sites Hub and Pages")
        );
    }

    #[tokio::test]
    async fn completes_grouped_at_references_from_the_client_root() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("src")).unwrap();
        fs::create_dir_all(workspace.join("plans")).unwrap();
        fs::write(workspace.join("src/main.rs"), "fn main() {}").unwrap();
        fs::write(workspace.join("Justfile"), "check:").unwrap();
        fs::write(workspace.join(".hidden"), "secret").unwrap();

        let project_root = temp.path().join("sase");
        fs::create_dir_all(project_root.join("plans")).unwrap();
        fs::write(project_root.join("plans/roadmap.md"), "roadmap").unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.root_dir = Some(workspace);
            config.project = Some("sase".to_string());
            config.artifact_ref_catalog = Some(artifact_path);
        }

        let bare_items = completion_items(
            server
                .completion_for_text("@".to_string(), Position::new(0, 1))
                .await
                .unwrap(),
        );
        let bare_labels = bare_items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            bare_labels,
            vec![
                "@commit:",
                "@chat:",
                "@bug:",
                "@file:",
                "@bead:",
                "@agent:",
                "@designs:",
                "@plan:",
            ]
        );
        assert!(!bare_labels.contains(&"@.hidden"));
        assert!(bare_items.iter().all(|item| {
            item.kind == Some(lsp_types::CompletionItemKind::ENUM_MEMBER)
                && item
                    .sort_text
                    .as_deref()
                    .is_some_and(|sort| sort.starts_with("0:"))
        }));
        for item in &bare_items {
            assert_eq!(item.filter_text.as_deref(), Some("@"));
            let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
            else {
                panic!("expected @ reference text edit");
            };
            assert_eq!(
                edit.range,
                Range::new(Position::new(0, 0), Position::new(0, 1))
            );
        }

        let narrowed_items = completion_items(
            server
                .completion_for_text_with_trigger(
                    "@p".to_string(),
                    Position::new(0, 2),
                    Some(CompletionTriggerKind::TRIGGER_CHARACTER),
                )
                .await
                .unwrap(),
        );
        assert_eq!(
            narrowed_items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:"]
        );

        let invoked_items = completion_items(
            server
                .completion_for_text_with_trigger(
                    "@p".to_string(),
                    Position::new(0, 2),
                    Some(CompletionTriggerKind::INVOKED),
                )
                .await
                .unwrap(),
        );
        assert_eq!(
            invoked_items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:", "@plans/"]
        );
        assert_eq!(
            invoked_items[1].kind,
            Some(lsp_types::CompletionItemKind::FOLDER)
        );

        for trigger in [
            CompletionTriggerKind::TRIGGER_CHARACTER,
            CompletionTriggerKind::INVOKED,
        ] {
            let path_items = completion_items(
                server
                    .completion_for_text_with_trigger(
                        "@src/".to_string(),
                        Position::new(0, 5),
                        Some(trigger),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(path_items.len(), 1);
            assert_eq!(path_items[0].label, "@src/main.rs");
            assert_eq!(
                path_items[0].kind,
                Some(lsp_types::CompletionItemKind::FILE)
            );
            let Some(CompletionTextEdit::Edit(path_edit)) =
                path_items[0].text_edit.as_ref()
            else {
                panic!("expected local path text edit");
            };
            assert_eq!(
                path_edit.range,
                Range::new(Position::new(0, 0), Position::new(0, 5))
            );
        }

        let payload_items = completion_items(
            server
                .completion_for_text("@plan:".to_string(), Position::new(0, 6))
                .await
                .unwrap(),
        );
        assert_eq!(payload_items.len(), 1);
        assert_eq!(payload_items[0].label, "@plan:roadmap.md");
    }

    #[tokio::test]
    async fn appends_known_kind_artifact_diagnostics_from_active_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        let vcs_path = temp.path().join("vcs_project_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("local"));
        write_vcs_ref_catalog(&vcs_path);
        fs::create_dir_all(temp.path().join("sase/designs")).unwrap();
        fs::write(temp.path().join("sase/designs/exists.md"), "exists")
            .unwrap();
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.artifact_ref_catalog = Some(artifact_path);
            config.vcs_project_catalog = Some(vcs_path);
        }

        let diagnostics = server
            .diagnostics_for_text(
                "#gh:sase @designs:exists.md @designs:missing.md \
                 @designs:bad.md#page=0 @user:handle \
                 `@designs:literal.md` @commit:missing@0123456 @bug:missing#1"
                    .to_string(),
            )
            .await;

        assert_eq!(
            diagnostics
                .iter()
                .filter(|diagnostic| matches!(
                    diagnostic.code.as_ref(),
                    Some(lsp_types::NumberOrString::String(code))
                        if code == "unresolved_artifact_ref"
                ))
                .count(),
            1,
            "{diagnostics:?}"
        );
        assert_eq!(
            diagnostics
                .iter()
                .filter(|diagnostic| matches!(
                    diagnostic.code.as_ref(),
                    Some(lsp_types::NumberOrString::String(code))
                        if code == "malformed_artifact_ref"
                ))
                .count(),
            1,
            "{diagnostics:?}"
        );
    }

    #[tokio::test]
    async fn encodes_known_artifact_refs_and_skips_unknown_and_literal_tokens()
    {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.artifact_ref_catalog = Some(artifact_path);
            config.vcs_project_catalog = None;
        }

        let tokens = server.semantic_tokens_for_text(
            "é @designs:guide.md#L2-L4 @commit:sase@0123456 \
             @user:handle\n```\n@designs:fenced.md\n```"
                .to_string(),
        );

        assert_eq!(
            tokens.data,
            vec![
                lsp_types::SemanticToken {
                    delta_line: 0,
                    delta_start: 3,
                    length: 7,
                    token_type: 0,
                    token_modifiers_bitset: 1,
                },
                lsp_types::SemanticToken {
                    delta_line: 0,
                    delta_start: 8,
                    length: 8,
                    token_type: 1,
                    token_modifiers_bitset: 1,
                },
                lsp_types::SemanticToken {
                    delta_line: 0,
                    delta_start: 8,
                    length: 6,
                    token_type: 2,
                    token_modifiers_bitset: 1,
                },
                lsp_types::SemanticToken {
                    delta_line: 0,
                    delta_start: 8,
                    length: 6,
                    token_type: 0,
                    token_modifiers_bitset: 0,
                },
                lsp_types::SemanticToken {
                    delta_line: 0,
                    delta_start: 7,
                    length: 12,
                    token_type: 1,
                    token_modifiers_bitset: 0,
                },
            ]
        );
    }

    #[tokio::test]
    async fn encodes_glossary_tokens_by_active_project_without_overlaps() {
        let temp = tempfile::tempdir().unwrap();
        let glossary_path = temp.path().join("glossary_catalog.json");
        let artifact_path = temp.path().join("artifact_ref_catalog.json");
        let vcs_path = temp.path().join("vcs_project_catalog.json");
        write_glossary_catalog(&glossary_path, temp.path(), Some("sase"));
        write_artifact_ref_catalog(&artifact_path, temp.path(), Some("sase"));
        write_vcs_ref_catalog(&vcs_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.glossary_catalog = Some(glossary_path);
            config.artifact_ref_catalog = Some(artifact_path);
            config.vcs_project_catalog = Some(vcs_path);
        }

        let tokens = server.semantic_tokens_for_text(
            "#gh:sase Agent Clan `clan` @designs:clan".to_string(),
        );
        let absolute = absolute_semantic_tokens(&tokens.data);

        assert!(absolute.contains(&(0, 9, 10, 3, 0)), "{absolute:?}");
        assert!(absolute.iter().any(|token| token.3 == 0));
        assert!(absolute.iter().any(|token| token.3 == 1));
        assert_eq!(
            absolute.iter().filter(|token| token.3 == 3).count(),
            1,
            "inline-code and artifact-overlapping aliases must not tokenize: {absolute:?}"
        );

        let local =
            server.semantic_tokens_for_text("#git:local Workspace".to_string());
        assert!(
            absolute_semantic_tokens(&local.data).contains(&(0, 11, 9, 3, 0))
        );
    }

    #[tokio::test]
    async fn glossary_hover_and_definition_use_source_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let glossary_path = temp.path().join("glossary_catalog.json");
        let vcs_path = temp.path().join("vcs_project_catalog.json");
        write_glossary_catalog(&glossary_path, temp.path(), Some("sase"));
        write_vcs_ref_catalog(&vcs_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.glossary_catalog = Some(glossary_path);
            config.vcs_project_catalog = Some(vcs_path);
        }

        let text = "#gh:sase ask clan".to_string();
        let hover = server
            .hover_for_text(text.clone(), Position::new(0, 14))
            .await
            .expect("glossary hover");
        let contents = hover.contents;
        let range = hover.range;
        let lsp_types::HoverContents::Markup(markup) = contents else {
            panic!("expected markdown hover");
        };
        assert!(markup.value.contains("**Agent Clan**"));
        assert!(markup.value.contains("Aliases: `clan`"));
        assert!(markup.value.contains("A named rootless container."));
        assert!(markup.value.contains("project `sase`"));
        assert_eq!(
            range,
            Some(Range::new(Position::new(0, 13), Position::new(0, 17)))
        );

        let definition = server
            .definition_for_text(text, Position::new(0, 14))
            .await
            .expect("glossary definition");
        let GotoDefinitionResponse::Scalar(location) = definition else {
            panic!("expected scalar definition");
        };
        assert_eq!(
            location.uri,
            file_uri(temp.path().join("sase/sase/sase.yml"))
        );
        assert_eq!(
            location.range,
            Range::new(Position::new(4, 16), Position::new(4, 27))
        );
    }

    #[tokio::test]
    async fn malformed_glossary_catalog_degrades_to_no_semantics() {
        let temp = tempfile::tempdir().unwrap();
        let glossary_path = temp.path().join("glossary_catalog.json");
        fs::write(&glossary_path, r#"{"schema_version": 99, "projects": []}"#)
            .unwrap();
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        server.config.write().unwrap().glossary_catalog = Some(glossary_path);

        let tokens = server.semantic_tokens_for_text("Agent Clan".to_string());
        let hover = server
            .hover_for_text("Agent Clan".to_string(), Position::new(0, 1))
            .await;
        let definition = server
            .definition_for_text("Agent Clan".to_string(), Position::new(0, 1))
            .await;

        assert!(tokens.data.is_empty());
        assert!(hover.is_none());
        assert!(definition.is_none());
    }

    #[tokio::test]
    async fn completes_vcs_project_with_primary_and_additional_edits() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text(
                "Describe this repo. +".to_string(),
                Position {
                    line: 0,
                    character: 21,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.label, "sase");
        assert_eq!(item.kind, Some(CompletionItemKind::MODULE));
        let label_details = item.label_details.as_ref().unwrap();
        assert_eq!(label_details.description.as_deref(), Some("project"));
        // `filter_text` is the `+name` trigger spelling so typing `+sa` keeps
        // the item under client-side filtering.
        assert_eq!(item.filter_text.as_deref(), Some("+sase"));
        assert_eq!(item.detail.as_deref(), Some("#gh:sase"));
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert_eq!(documentation.value, "SASE repo");

        // Primary edit consumes the `+` trigger token...
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected primary text edit");
        };
        assert_eq!(edit.new_text, "");
        // ...and the additional edit prepends the tag at the document start.
        let additional = item.additional_text_edits.as_ref().unwrap();
        assert_eq!(additional.len(), 1);
        assert_eq!(additional[0].new_text, "#gh:sase ");
        assert_eq!(additional[0].range.start, additional[0].range.end);
    }

    #[tokio::test]
    async fn completes_vcs_project_replacing_existing_tag_at_eof() {
        // `#git:foo +` -- an existing leading VCS tag immediately followed by
        // the `+` trigger at end-of-input. Selecting a project must replace the
        // existing tag, not double it. The primary edit deletes the trailing
        // ` +` trigger span; the additional edit replaces the `#git:foo` range
        // with the selected `#gh:sase ` tag.
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text(
                "#git:foo +".to_string(),
                Position {
                    line: 0,
                    character: 10,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.label, "sase");

        // Primary edit deletes the trailing ` +` trigger span (bytes 8..10).
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected primary text edit");
        };
        assert_eq!(edit.new_text, "");
        assert_eq!(edit.range.start, Position::new(0, 8));
        assert_eq!(edit.range.end, Position::new(0, 10));

        // Additional edit replaces the existing `#git:foo` (bytes 0..8) tag.
        let additional = item.additional_text_edits.as_ref().unwrap();
        assert_eq!(additional.len(), 1);
        assert_eq!(additional[0].new_text, "#gh:sase ");
        assert_eq!(additional[0].range.start, Position::new(0, 0));
        assert_eq!(additional[0].range.end, Position::new(0, 8));
    }

    #[tokio::test]
    async fn completes_vcs_patch_with_pr_label_details() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog_with_pr(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text(
                "+ship".to_string(),
                Position {
                    line: 0,
                    character: 5,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.label, "ship-completion");
        assert_eq!(item.kind, Some(CompletionItemKind::EVENT));
        assert_eq!(item.detail.as_deref(), Some("#gh:ship-completion"));
        assert_eq!(item.filter_text.as_deref(), Some("+ship-completion"));
        let label_details = item.label_details.as_ref().unwrap();
        assert_eq!(label_details.detail.as_deref(), Some(" · sase"));
        assert_eq!(label_details.description.as_deref(), Some("PR · Ready"));
    }

    #[tokio::test]
    async fn obsolete_and_unspaced_plus_forms_do_not_complete_vcs_projects() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        for (text, position) in [
            ("#+", Position::new(0, 2)),
            ("Fix #+sa", Position::new(0, 8)),
            ("line\n+", Position::new(1, 1)),
            ("\t+", Position::new(0, 2)),
            ("word+", Position::new(0, 5)),
            ("a+b", Position::new(0, 3)),
            ("c++", Position::new(0, 3)),
        ] {
            let response =
                server.completion_for_text(text.to_string(), position).await;
            let has_vcs_project_item = match response {
                Some(CompletionResponse::Array(items)) => items
                    .iter()
                    .any(|item| item.detail.as_deref() == Some("#gh:sase")),
                Some(CompletionResponse::List(list)) => list
                    .items
                    .iter()
                    .any(|item| item.detail.as_deref() == Some("#gh:sase")),
                None => false,
            };
            assert!(!has_vcs_project_item, "{text:?} should not complete");
        }
    }

    #[tokio::test]
    async fn bare_plus_at_bof_completes_vcs_project() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        // `+sa` at byte offset 0 completes, filtering by the bare-plus query.
        let response = server
            .completion_for_text(
                "+sa".to_string(),
                Position {
                    line: 0,
                    character: 3,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.label, "sase");
        assert_eq!(item.kind, Some(CompletionItemKind::MODULE));
        let label_details = item.label_details.as_ref().unwrap();
        assert_eq!(label_details.description.as_deref(), Some("project"));
        // `filter_text` uses the bare-plus trigger spelling so typing `+sa`
        // keeps the item under client-side filtering.
        assert_eq!(item.filter_text.as_deref(), Some("+sase"));
        // BOF bare-plus: the prepend point coincides with the trigger deletion,
        // so the edits merge into one primary edit with no additional edits.
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected primary text edit");
        };
        assert_eq!(edit.new_text, "#gh:sase ");
        assert!(item.additional_text_edits.is_none());
    }

    #[tokio::test]
    async fn space_delimited_plus_completes_vcs_project() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text(
                "Fix +sa".to_string(),
                Position {
                    line: 0,
                    character: 7,
                },
            )
            .await
            .unwrap();

        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, "sase");
        assert_eq!(items[0].filter_text.as_deref(), Some("+sase"));
    }

    #[tokio::test]
    async fn automatic_and_manual_space_plus_completion_match() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }
        let uri = file_uri(temp.path().join("sase_prompt_completion.md"));

        for (text, position, trigger_kind, trigger_character) in [
            (
                "+",
                Position::new(0, 1),
                CompletionTriggerKind::TRIGGER_CHARACTER,
                Some("+".to_string()),
            ),
            (
                "+sa",
                Position::new(0, 3),
                CompletionTriggerKind::INVOKED,
                None,
            ),
            (
                "Fix +",
                Position::new(0, 5),
                CompletionTriggerKind::TRIGGER_CHARACTER,
                Some("+".to_string()),
            ),
            (
                "Fix +sa",
                Position::new(0, 7),
                CompletionTriggerKind::INVOKED,
                None,
            ),
        ] {
            let document = server.open_document(
                &uri,
                "markdown".to_string(),
                text.to_string(),
            );
            server
                .documents
                .write()
                .unwrap()
                .insert(uri.to_string(), document);

            let response = server
                .completion(CompletionParams {
                    text_document_position: TextDocumentPositionParams {
                        text_document: TextDocumentIdentifier {
                            uri: uri.clone(),
                        },
                        position,
                    },
                    work_done_progress_params: Default::default(),
                    partial_result_params: Default::default(),
                    context: Some(CompletionContext {
                        trigger_kind,
                        trigger_character,
                    }),
                })
                .await
                .unwrap()
                .unwrap();
            let CompletionResponse::Array(items) = response else {
                panic!("expected completion array");
            };
            assert_eq!(items.len(), 1, "{text:?}");
            assert_eq!(items[0].label, "sase", "{text:?}");
            assert_eq!(items[0].filter_text.as_deref(), Some("+sase"));
        }
    }

    #[tokio::test]
    async fn vcs_project_completion_without_catalog_is_empty() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = None;
        }

        let response = server
            .completion_for_text(
                "+".to_string(),
                Position {
                    line: 0,
                    character: 1,
                },
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn completes_vcs_ref_from_v3_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_ref_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("#gh:".to_string(), Position::new(0, 4))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "ship-completion", "sase-org/", "bbugyi200/"]
        );

        let project = &items[0];
        assert_eq!(project.kind, Some(CompletionItemKind::MODULE));
        assert_eq!(project.filter_text.as_deref(), Some("sase"));
        assert_eq!(project.sort_text.as_deref(), Some("0:sase:0000"));
        assert_eq!(project.detail.as_deref(), Some("GitHub · #gh:sase"));
        assert_eq!(
            project
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("project")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            project.documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert_eq!(documentation.value, "SASE repo");
        let Some(CompletionTextEdit::Edit(project_edit)) =
            project.text_edit.as_ref()
        else {
            panic!("expected project text edit");
        };
        assert_eq!(project_edit.range.start, Position::new(0, 4));
        assert_eq!(project_edit.range.end, Position::new(0, 4));
        assert_eq!(project_edit.new_text, "sase ");

        let patch = &items[1];
        assert_eq!(patch.kind, Some(CompletionItemKind::REFERENCE));
        assert_eq!(patch.filter_text.as_deref(), Some("ship-completion"));
        assert_eq!(patch.sort_text.as_deref(), Some("1:ship-completion:0001"));
        assert_eq!(
            patch.detail.as_deref(),
            Some("GitHub · #gh:ship-completion")
        );
        let patch_details = patch.label_details.as_ref().unwrap();
        assert_eq!(patch_details.detail.as_deref(), Some(" · sase"));
        assert_eq!(patch_details.description.as_deref(), Some("PR · Ready"));

        let namespace = &items[2];
        assert_eq!(namespace.kind, Some(CompletionItemKind::FOLDER));
        assert_eq!(namespace.filter_text.as_deref(), Some("sase-org"));
        assert_eq!(namespace.sort_text.as_deref(), Some("2:sase-org:0002"));
        assert_eq!(namespace.detail.as_deref(), Some("2 enabled projects"));
        assert_eq!(
            namespace
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("org")
        );
        let command = namespace.command.as_ref().unwrap();
        assert_eq!(command.command, "editor.action.triggerSuggest");
        let Some(CompletionTextEdit::Edit(namespace_edit)) =
            namespace.text_edit.as_ref()
        else {
            panic!("expected namespace text edit");
        };
        assert_eq!(namespace_edit.range.start, Position::new(0, 4));
        assert_eq!(namespace_edit.range.end, Position::new(0, 4));
        assert_eq!(namespace_edit.new_text, "sase-org/");
    }

    #[tokio::test]
    async fn vcs_ref_completion_filters_aliases_and_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_ref_catalog(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("#gh:sase-c".to_string(), Position::new(0, 10))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, "sase");
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected alias text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 4));
        assert_eq!(edit.range.end, Position::new(0, 10));
        assert_eq!(edit.new_text, "sase ");

        let response = server
            .completion_for_text("#gh:sa".to_string(), Position::new(0, 6))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };
        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "sase-org/"]
        );
    }

    #[tokio::test]
    async fn vcs_ref_completion_accepts_v2_catalog_without_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog_with_pr(&catalog_path);
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("#gh:".to_string(), Position::new(0, 4))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "ship-completion"]
        );
        assert!(!items
            .iter()
            .any(|item| item.kind == Some(CompletionItemKind::FOLDER)));
    }

    #[tokio::test]
    async fn vcs_ref_completion_ignores_malformed_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        fs::write(
            &catalog_path,
            r##"{
                "schema_version": 3,
                "workflow_names": ["gh"],
                "entries": [
                    {
                        "name": "sase",
                        "vcs_prefix": "gh",
                        "display_tag": "#gh:sase",
                        "provider_display": "GitHub"
                    }
                ],
                "namespaces": ["not", "a", "map"]
            }"##,
        )
        .unwrap();
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog_entries(Vec::new())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let response = server
            .completion_for_text("#gh:".to_string(), Position::new(0, 4))
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(
            items
                .iter()
                .map(|item| item.label.as_str())
                .collect::<Vec<_>>(),
            vec!["sase"]
        );
        assert!(items[0].command.is_none());
    }

    #[tokio::test]
    async fn vcs_ref_owner_slash_still_uses_repo_completion() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_ref_catalog(&catalog_path);
        let repo_response = vcs_repo_catalog_response(
            "ok",
            "",
            vec![repo_entry(
                "sase",
                "Structured Agentic Software Engineering",
                "private",
                false,
                false,
                Some("2026-07-07T18:00:00Z"),
            )],
        );
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_vcs_repo_catalog(repo_response.clone())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let text = "#gh:bbugyi200/".to_string();
        let response = server
            .completion_for_text(
                text.clone(),
                Position::new(0, text.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].label, "sase");
        assert_eq!(items[0].filter_text.as_deref(), Some("bbugyi200/sase"));
        assert!(items[0].command.is_none());
        let Some(CompletionTextEdit::Edit(edit)) = items[0].text_edit.as_ref()
        else {
            panic!("expected repo text edit");
        };
        assert_eq!(edit.new_text, "bbugyi200/sase ");
    }

    #[tokio::test]
    async fn completes_vcs_repo_with_ranked_items_and_text_edit() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let repo_response = vcs_repo_catalog_response(
            "ok",
            "",
            vec![
                repo_entry(
                    "tooling",
                    "Tooling repo",
                    "public",
                    false,
                    false,
                    Some("2026-07-07T18:30:00Z"),
                ),
                repo_entry(
                    "sase-old",
                    "Old SASE repo",
                    "public",
                    false,
                    false,
                    Some("2025-01-01T00:00:00Z"),
                ),
                repo_entry(
                    "sase",
                    "Structured Agentic Software Engineering",
                    "private",
                    true,
                    true,
                    Some("2026-07-07T18:00:00Z"),
                ),
            ],
        );
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_vcs_repo_catalog(repo_response.clone())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let text = "#gh:bbugyi200/sa".to_string();
        let response = server
            .completion_for_text(
                text.clone(),
                Position::new(0, text.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].label, "sase");
        assert_eq!(items[1].label, "sase-old");
        assert_eq!(items[2].label, "tooling");
        let item = &items[0];
        assert_eq!(item.kind, Some(CompletionItemKind::MODULE));
        assert_eq!(item.filter_text.as_deref(), Some("bbugyi200/sase"));
        assert_eq!(item.sort_text.as_deref(), Some("0000"));
        assert_eq!(
            item.label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("[private] [fork] [archived]")
        );
        assert_eq!(
            item.detail.as_deref(),
            Some("bbugyi200/sase [private] [fork] [archived]")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert!(documentation
            .value
            .contains("Structured Agentic Software Engineering"));
        assert!(documentation.value.contains("[private] [fork] [archived]"));

        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected text edit");
        };
        assert_eq!(edit.range.start, Position::new(0, 4));
        assert_eq!(edit.range.end, Position::new(0, text.len() as u32));
        assert_eq!(edit.new_text, "bbugyi200/sase ");
        assert!(item.additional_text_edits.is_none());
    }

    #[tokio::test]
    async fn vcs_repo_completion_error_response_is_empty() {
        let temp = tempfile::tempdir().unwrap();
        let catalog_path = temp.path().join("vcs_project_catalog.json");
        write_vcs_project_catalog(&catalog_path);
        let repo_response = vcs_repo_catalog_response(
            "error",
            "repo listing failed - run gh auth login",
            Vec::new(),
        );
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_vcs_repo_catalog(repo_response.clone())),
            )
        });
        let server = service.inner();
        {
            let mut config = server.config.write().unwrap();
            config.vcs_project_catalog = Some(catalog_path);
        }

        let text = "#gh:bbugyi200/".to_string();
        let response = server
            .completion_for_text(
                text.clone(),
                Position::new(0, text.len() as u32),
            )
            .await
            .unwrap();
        let CompletionResponse::Array(items) = response else {
            panic!("expected completion array");
        };

        assert!(items.is_empty());
    }
}
