use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use lsp_types::{
    ClientCapabilities, CodeAction, CodeActionKind, CodeActionOptions,
    CodeActionOrCommand, CodeActionParams, CodeActionProviderCapability,
    CodeActionResponse, Command, CompletionItem, CompletionOptions,
    CompletionParams, CompletionResponse, DidChangeTextDocumentParams,
    DidChangeWatchedFilesParams, DidCloseTextDocumentParams,
    DidOpenTextDocumentParams, DocumentChanges, ExecuteCommandOptions,
    ExecuteCommandParams, GotoDefinitionParams, GotoDefinitionResponse, Hover,
    HoverParams, HoverProviderCapability, InitializeParams, InitializeResult,
    InitializedParams, LSPAny, Location, MessageType, OneOf,
    OptionalVersionedTextDocumentIdentifier, Position, Range,
    ServerCapabilities, ServerInfo, TextDocumentEdit,
    TextDocumentSyncCapability, TextDocumentSyncKind, TextEdit, Uri,
    WorkDoneProgressOptions, WorkspaceEdit,
};
use sase_core::{
    editor_analyze_document, editor_build_directive_completion_candidates,
    editor_build_file_completion_candidates_with_base,
    editor_build_file_history_completion_candidates,
    editor_build_snippet_completion_candidates,
    editor_build_vcs_project_completion_candidates,
    editor_build_xprompt_arg_name_candidates,
    editor_build_xprompt_completion_candidates,
    editor_classify_completion_context, editor_definition_at_position,
    editor_directive_argument_candidates, editor_directive_metadata,
    editor_extract_token_at_position, editor_hover_at_position,
    CompletionCandidate, CompletionContextKind, CompletionList,
    DocumentSnapshot, EditorRange, EditorSnippetEntryWire, HelperHostBridge,
    VcsProjectEntry, XpromptAssistEntry,
};
use tower_lsp_server::jsonrpc::Result;
use tower_lsp_server::{Client, LanguageServer, LspService, Server, UriExt};
use tracing::{info, warn};

use crate::catalog_cache::{CatalogCache, CatalogFailure};
use crate::lsp_convert::{
    apply_replacement, completion_response, diagnostic as lsp_diagnostic,
    hover as lsp_hover, sase_snippet_completion_item, snippet_completion_item,
    to_editor_position, to_lsp_range, vcs_project_completion_response,
};

const SERVER_NAME: &str = "sase-xprompt-lsp";
const REFRESH_COMMAND: &str = "sase.xpromptLsp.refreshCatalog";
const OPEN_SOURCE_COMMAND: &str = "sase.xpromptLsp.openSource";

/// Env var carrying the path to the JSON `vcs_project` completion catalog
/// (active-project entries + known VCS workflow names). Materialized by the
/// Python launcher (`integrations/xprompt_lsp.py`) at LSP startup and re-read
/// fresh on every `#+` completion request so external rewrites are picked up.
const VCS_PROJECT_CATALOG_ENV: &str = "SASE_XPROMPT_VCS_PROJECT_CATALOG";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerConfig {
    root_dir: Option<PathBuf>,
    project: Option<String>,
    catalog_key: String,
    snippet_support: bool,
    allow_all_markdown: bool,
    /// Path to the materialized `vcs_project` completion catalog, captured from
    /// [`VCS_PROJECT_CATALOG_ENV`] at startup. The file itself is re-read fresh
    /// on each `#+` completion request (see [`load_vcs_project_catalog`]).
    vcs_project_catalog: Option<PathBuf>,
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
        }
    }
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
    config: RwLock<ServerConfig>,
}

impl XpromptLspServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            documents: RwLock::new(HashMap::new()),
            catalog_cache: Arc::new(CatalogCache::command_backed()),
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
            config: RwLock::new(ServerConfig::default()),
        }
    }

    pub async fn completion_for_text(
        &self,
        text: String,
        position: Position,
    ) -> Option<CompletionResponse> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
        let context = editor_classify_completion_context(
            &document,
            to_editor_position(position),
            entries.as_slice(),
        )?;
        if context.kind == CompletionContextKind::VcsProject {
            return Some(self.vcs_project_completion(
                &context, &document, position, &config,
            ));
        }
        let list =
            self.completion_list_for_context(&context, &entries, &config);
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
            return Some(CompletionResponse::Array(xprompt_snippet_items(
                list,
                entries.as_slice(),
                context.replacement_range,
            )));
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

    /// Build the `#+` (`vcs_project`) completion response.
    ///
    /// The project catalog (active-project entries + known VCS workflow names)
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
        let (entries, workflow_names) =
            load_vcs_project_catalog(config.vcs_project_catalog.as_deref());
        let list = editor_build_vcs_project_completion_candidates(
            token,
            document,
            to_editor_position(position),
            &entries,
            &workflow_names,
        );
        // The trigger spelling the user typed (`#+` for a hash-plus token, `+`
        // for a BOF bare-plus token) drives the items' client-side `filter_text`.
        let trigger_prefix = if token.text.starts_with("#+") {
            "#+"
        } else {
            "+"
        };
        vcs_project_completion_response(
            list,
            context.replacement_range,
            trigger_prefix,
        )
    }

    pub async fn hover_for_text(
        &self,
        text: String,
        position: Position,
    ) -> Option<Hover> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
        editor_hover_at_position(
            &document,
            to_editor_position(position),
            entries.as_slice(),
        )
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
        editor_analyze_document(&document, entries.as_slice())
            .into_iter()
            .map(lsp_diagnostic)
            .collect()
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
        let target = editor_definition_at_position(
            &document,
            to_editor_position(position),
            entries.as_slice(),
        )?;
        let uri = Uri::from_file_path(target.path)?;
        Some(GotoDefinitionResponse::Scalar(Location {
            uri,
            range: target.range.map(to_lsp_range).unwrap_or_else(zero_range),
        }))
    }

    fn current_config(&self) -> ServerConfig {
        self.config
            .read()
            .map(|config| config.clone())
            .unwrap_or_default()
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
    ) -> CompletionList {
        let token = context
            .token
            .as_ref()
            .map(|token| token.text.as_str())
            .unwrap_or_default();
        let list = match context.kind {
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
            CompletionContextKind::XpromptArgumentTypeHint => {
                empty_completion_list()
            }
            CompletionContextKind::SnippetTrigger => empty_completion_list(),
            // Handled out-of-band in `completion_for_text` /
            // `vcs_project_completion`, which loads the materialized project
            // catalog and known workflow names the core builder needs.
            CompletionContextKind::VcsProject => empty_completion_list(),
        };
        apply_replacement(list, context.replacement_range)
    }

    async fn refresh_catalog_explicit(&self) {
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
            .completion_for_text(
                document.text,
                params.text_document_position.position,
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
    }
}

fn vcs_project_catalog_path() -> Option<PathBuf> {
    std::env::var_os(VCS_PROJECT_CATALOG_ENV).map(PathBuf::from)
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
) -> Vec<CompletionItem> {
    list.candidates
        .into_iter()
        .filter_map(|candidate| {
            let entry =
                entries.iter().find(|entry| entry.name == candidate.name)?;
            Some(snippet_completion_item(
                candidate.display,
                xprompt_completion_skeleton(entry),
                candidate.detail,
                candidate.documentation,
                replacement_range,
            ))
        })
        .collect()
}

fn xprompt_completion_skeleton(entry: &XpromptAssistEntry) -> String {
    let required = entry
        .inputs
        .iter()
        .filter(|input| input.required)
        .collect::<Vec<_>>();
    match required.as_slice() {
        [] => format!("{} ", entry.insertion),
        [input] if input.r#type == "text" => format!("{}::", entry.insertion),
        [_] => format!("{}:", entry.insertion),
        _ => format!("{}($0)", entry.insertion),
    }
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
                    .filter(|alias| *alias != "(")
                    .is_some_and(|alias| alias.starts_with(partial))
        })
        .map(|directive| {
            let syntax = if directive.name == "alt" {
                "%(${1:variant})$0".to_string()
            } else {
                format!("%{}:${{1:value}}$0", directive.name)
            };
            snippet_completion_item(
                format!("%{}:...", directive.name),
                syntax,
                Some("directive snippet".to_string()),
                Some(
                    editor_directive_metadata(directive.name)
                        .map(|metadata| metadata.description.to_string())
                        .unwrap_or_else(|| directive.description.to_string()),
                ),
                replacement_range,
            )
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

/// Load the active project/PR completion catalog from the materialized JSON
/// file at `path`, returning `(entries, workflow_names)`.
///
/// Read fresh on every `#+` completion request. Any failure (no path, unreadable
/// file, malformed JSON) degrades to empty results so the `#+` menu simply
/// shows nothing rather than breaking completion. Schema versions 1 and 2 are
/// accepted; v1 entries default to project rows. The file shape is
/// `{ "schema_version": N, "workflow_names": [..], "entries": [VcsProjectEntry, ..] }`.
fn load_vcs_project_catalog(
    path: Option<&Path>,
) -> (Vec<VcsProjectEntry>, Vec<String>) {
    let Some(path) = path else {
        return (Vec::new(), Vec::new());
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return (Vec::new(), Vec::new());
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        warn!("failed to parse vcs project catalog at {path:?}");
        return (Vec::new(), Vec::new());
    };
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    if !matches!(schema_version, 1 | 2) {
        warn!(
            "unsupported vcs project catalog schema_version {schema_version} at {path:?}"
        );
        return (Vec::new(), Vec::new());
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
    (entries, workflow_names)
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
        return entries
            .iter()
            .find(|entry| entry.is_skill && entry.name == name);
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
    let Some(file_name) = path.file_name().and_then(|name| name.to_str())
    else {
        return false;
    };
    is_prompt_temp_markdown_name(file_name)
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
    if matches!(file_name, "xprompts.yml" | "xprompts.yaml" | "sase.yml") {
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
    path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("xprompts" | ".xprompts" | "default_xprompts")
        )
    })
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use lsp_types::{
        CodeActionOrCommand, CompletionClientCapabilities,
        CompletionItemCapability, CompletionItemKind, CompletionResponse,
        CompletionTextEdit, Documentation, GotoDefinitionResponse, Hover,
        InsertTextFormat, Position, Range, TextDocumentClientCapabilities, Uri,
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
        let xprompts_uri =
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

        assert!(document_eligible(&xprompts_uri, "markdown", &config));
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
    }

    #[test]
    fn catalog_invalidation_tracks_xprompt_source_dirs() {
        let temp = std::env::temp_dir();
        let xprompts_uri =
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
        let prose_uri = file_uri(
            temp.join("project")
                .join("sdd")
                .join("research")
                .join("202605")
                .join("memory_system_prior_art.md"),
        );

        assert!(should_invalidate_for_uri(&xprompts_uri));
        assert!(should_invalidate_for_uri(&dot_xprompts_uri));
        assert!(should_invalidate_for_uri(&default_xprompts_uri));
        assert!(!should_invalidate_for_uri(&prose_uri));
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
    async fn diagnostics_for_uri_text_honors_memory_long_file_uri() {
        let temp = tempfile::tempdir().unwrap();
        let memory_dir = temp.path().join("memory").join("long");
        fs::create_dir_all(&memory_dir).unwrap();
        let memory_uri =
            Uri::from_file_path(memory_dir.join("generated_skills.md"))
                .unwrap();
        let normal_uri =
            Uri::from_file_path(temp.path().join("xprompts").join("foo.md"))
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
            diagnostics_contain_code(
                &normal_diagnostics,
                "missing_xprompt_memory_tag"
            ),
            "{normal_diagnostics:?}"
        );
    }

    #[tokio::test]
    async fn diagnostics_for_uri_text_accepts_markdown_local_xprompts() {
        let temp = tempfile::tempdir().unwrap();
        let uri =
            Uri::from_file_path(temp.path().join("xprompts").join("reads.md"))
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
        let source_path = temp.path().join("sase.yml");
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

    // --- vcs_project (`#+`) completion -------------------------------------

    fn write_vcs_project_catalog(path: &Path) {
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
                        "kind": "changespec",
                        "project": "sase",
                        "status": "Ready"
                    }
                ]
            }"##,
        )
        .unwrap();
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

        let (entries, workflow_names) =
            load_vcs_project_catalog(Some(&catalog_path));

        assert_eq!(workflow_names, vec!["gh"]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, "project");
        assert_eq!(entries[0].project, "");
        assert_eq!(entries[0].status, "");
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

        let (entries, workflow_names) =
            load_vcs_project_catalog(Some(&catalog_path));

        assert!(entries.is_empty());
        assert!(workflow_names.is_empty());
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
                "Describe this repo. #+".to_string(),
                Position {
                    line: 0,
                    character: 22,
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
        // `filter_text` is the `#+name` trigger spelling so typing `#+sa` keeps
        // the item under client-side filtering.
        assert_eq!(item.filter_text.as_deref(), Some("#+sase"));
        assert_eq!(item.detail.as_deref(), Some("#gh:sase"));
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert_eq!(documentation.value, "SASE repo");

        // Primary edit consumes the `#+` trigger token...
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
    async fn completes_vcs_changespec_with_pr_label_details() {
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
                "#+ship".to_string(),
                Position {
                    line: 0,
                    character: 6,
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
        assert_eq!(item.filter_text.as_deref(), Some("#+ship-completion"));
        let label_details = item.label_details.as_ref().unwrap();
        assert_eq!(label_details.detail.as_deref(), Some(" · sase"));
        assert_eq!(label_details.description.as_deref(), Some("PR · Ready"));
    }

    #[tokio::test]
    async fn hash_plus_trigger_merges_into_single_primary_edit() {
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
                "#+".to_string(),
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
        let item = &items[0];
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected primary text edit");
        };
        // BOF `#+`: prepend point coincides with the trigger deletion, so the
        // edits merge into one primary edit with no additional edits.
        assert_eq!(edit.new_text, "#gh:sase ");
        assert!(item.additional_text_edits.is_none());
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
    async fn bare_plus_outside_bof_does_not_complete_vcs_project() {
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
                "Fix +".to_string(),
                Position {
                    line: 0,
                    character: 5,
                },
            )
            .await;

        assert!(response.is_none());
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
                "#+".to_string(),
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
}
