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
    editor_build_xprompt_arg_name_candidates,
    editor_build_xprompt_completion_candidates,
    editor_classify_completion_context, editor_colon_args_skeleton,
    editor_directive_argument_candidates, editor_directive_metadata,
    editor_extract_token_at_position, editor_hover_at_position,
    editor_named_args_skeleton, CompletionCandidate, CompletionContextKind,
    CompletionList, DocumentSnapshot, EditorRange, HelperHostBridge,
    XpromptAssistEntry,
};
use tower_lsp_server::jsonrpc::Result;
use tower_lsp_server::{Client, LanguageServer, LspService, Server, UriExt};
use tracing::{info, warn};

use crate::catalog_cache::{CatalogCache, CatalogFailure};
use crate::lsp_convert::{
    apply_replacement, completion_response, diagnostic as lsp_diagnostic,
    hover as lsp_hover, snippet_completion_item, to_editor_position,
    to_lsp_range,
};

const SERVER_NAME: &str = "sase-xprompt-lsp";
const REFRESH_COMMAND: &str = "sase.xpromptLsp.refreshCatalog";
const OPEN_SOURCE_COMMAND: &str = "sase.xpromptLsp.openSource";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerConfig {
    root_dir: Option<PathBuf>,
    project: Option<String>,
    catalog_key: String,
    snippet_support: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            root_dir: std::env::current_dir().ok(),
            project: None,
            catalog_key: "default".to_string(),
            snippet_support: false,
        }
    }
}

#[derive(Debug)]
pub struct XpromptLspServer {
    client: Client,
    documents: RwLock<HashMap<String, String>>,
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
        let list =
            self.completion_list_for_context(&context, &entries, &config);
        let mut response = completion_response(list, context.replacement_range);
        if config.snippet_support
            && context.kind == CompletionContextKind::Xprompt
        {
            if let CompletionResponse::Array(items) = &mut response {
                items.extend(snippet_items(
                    context.token.as_ref().map(|token| token.text.as_str()),
                    entries.as_slice(),
                    context.replacement_range,
                ));
            }
        }
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
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
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
                    safe_source_uri(entry, config.root_dir.as_deref())
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
        let token = editor_extract_token_at_position(
            &document,
            to_editor_position(position),
        )?;
        let entry = entry_for_token(&token.text, entries.as_slice())?;
        let uri = safe_source_uri(entry, config.root_dir.as_deref())?;
        Some(GotoDefinitionResponse::Scalar(Location {
            uri,
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: 0,
                },
            },
        }))
    }

    fn current_config(&self) -> ServerConfig {
        self.config
            .read()
            .map(|config| config.clone())
            .unwrap_or_default()
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
        };
        apply_replacement(list, context.replacement_range)
    }

    async fn refresh_catalog_explicit(&self) {
        let config = self.current_config();
        match self
            .catalog_cache
            .refresh_explicit(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await
        {
            Ok(entries) => {
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!("refreshed {} xprompt entries", entries.len()),
                    )
                    .await;
            }
            Err(error) => self.warn_once(&error).await,
        }
    }

    async fn publish_document_diagnostics(&self, uri: Uri, text: String) {
        let diagnostics = self.diagnostics_for_text(text).await;
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
        if let Ok(mut documents) = self.documents.write() {
            documents.insert(uri.to_string(), text.clone());
        }
        self.publish_document_diagnostics(uri, text).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let Some(change) = params.content_changes.into_iter().last() else {
            return;
        };
        let uri = params.text_document.uri;
        let text = change.text;
        if let Ok(mut documents) = self.documents.write() {
            documents.insert(uri.to_string(), text.clone());
        }
        self.publish_document_diagnostics(uri, text).await;
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
        let uri = params.text_document_position.text_document.uri.to_string();
        let text = self
            .documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri).cloned());
        let Some(text) = text else {
            return Ok(None);
        };
        Ok(self
            .completion_for_text(text, params.text_document_position.position)
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
        let text = self
            .documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri.to_string()).cloned());
        let Some(text) = text else {
            return Ok(None);
        };
        Ok(self
            .hover_for_text(text, params.text_document_position_params.position)
            .await)
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri;
        let text = self
            .documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri.to_string()).cloned());
        let Some(text) = text else {
            return Ok(None);
        };
        Ok(self
            .definition_for_text(
                text,
                params.text_document_position_params.position,
            )
            .await)
    }

    async fn code_action(
        &self,
        params: CodeActionParams,
    ) -> Result<Option<CodeActionResponse>> {
        let uri = params.text_document.uri;
        let text = self
            .documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri.to_string()).cloned());
        let Some(text) = text else {
            return Ok(None);
        };
        Ok(Some(
            self.code_actions_for_text(uri, text, params.range).await,
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
    }
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

fn snippet_items(
    token: Option<&str>,
    entries: &[XpromptAssistEntry],
    replacement_range: sase_core::EditorRange,
) -> Vec<CompletionItem> {
    let token = token.unwrap_or_default();
    let partial = token
        .strip_prefix("#!")
        .or_else(|| token.strip_prefix('#'))
        .unwrap_or(token)
        .to_lowercase();
    entries
        .iter()
        .filter(|entry| !entry.inputs.is_empty())
        .filter(|entry| entry.name.to_lowercase().starts_with(&partial))
        .flat_map(|entry| {
            [
                snippet_completion_item(
                    format!("{}(...)", entry.insertion),
                    editor_named_args_skeleton(entry),
                    entry.input_signature.clone(),
                    entry.description.clone(),
                    replacement_range,
                ),
                snippet_completion_item(
                    format!("{}:", entry.insertion),
                    editor_colon_args_skeleton(entry),
                    entry.input_signature.clone(),
                    entry.description.clone(),
                    replacement_range,
                ),
            ]
        })
        .collect()
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

fn safe_source_uri(
    entry: &XpromptAssistEntry,
    root_dir: Option<&Path>,
) -> Option<Uri> {
    let display = entry.source_path_display.as_deref()?.trim();
    if display.is_empty()
        || display.contains("://")
        || display.contains('\n')
        || display.contains('\r')
    {
        return None;
    }
    let raw = Path::new(display);
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        root_dir?.join(raw)
    };
    let canonical = candidate.canonicalize().ok()?;
    if let Some(root) = root_dir {
        let root = root.canonicalize().ok()?;
        if !canonical.starts_with(root) {
            return None;
        }
    }
    if !canonical.is_file() {
        return None;
    }
    Uri::from_file_path(canonical)
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
    path.components()
        .any(|component| component.as_os_str() == "xprompts")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lsp_types::{
        CodeActionOrCommand, CompletionClientCapabilities,
        CompletionItemCapability, CompletionResponse, GotoDefinitionResponse,
        Hover, Position, Range, TextDocumentClientCapabilities, Uri,
    };
    use sase_core::{
        MobileHelperProjectContextWire, MobileHelperProjectScopeWire,
        MobileHelperResultWire, MobileHelperStatusWire,
        MobileXpromptCatalogEntryWire, MobileXpromptCatalogResponseWire,
        MobileXpromptCatalogStatsWire, MobileXpromptInputWire,
        StaticHelperHostBridge,
    };
    use tower_lsp_server::UriExt;

    use super::*;

    fn bridge_with_catalog() -> StaticHelperHostBridge {
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
                entries: vec![MobileXpromptCatalogEntryWire {
                    name: "foo".to_string(),
                    display_label: "foo".to_string(),
                    insertion: Some("#foo".to_string()),
                    reference_prefix: Some("#".to_string()),
                    kind: Some("prompt".to_string()),
                    description: Some("Foo prompt".to_string()),
                    source_bucket: "builtin".to_string(),
                    project: None,
                    tags: Vec::new(),
                    input_signature: Some("(path: path)".to_string()),
                    inputs: vec![MobileXpromptInputWire {
                        name: "path".to_string(),
                        r#type: "path".to_string(),
                        required: true,
                        default_display: None,
                        position: 0,
                    }],
                    is_skill: true,
                    content_preview: None,
                    source_path_display: Some("Cargo.toml".to_string()),
                    definition_path: None,
                }],
                stats: MobileXpromptCatalogStatsWire {
                    total_count: 1,
                    project_count: 0,
                    skill_count: 1,
                    pdf_requested: false,
                },
                catalog_attachment: None,
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

    #[tokio::test]
    async fn completes_xprompt_from_static_catalog() {
        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog()),
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
    async fn exposes_hover_diagnostics_code_actions_and_definition() {
        let source_path = std::env::current_dir().unwrap().join("Cargo.toml");

        let (service, _) = LspService::new(|client| {
            XpromptLspServer::with_bridge(
                client,
                Arc::new(bridge_with_catalog()),
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

        let diagnostics = server
            .diagnostics_for_text("#missing %wat".to_string())
            .await;
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("Unknown xprompt")));
        assert!(diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .contains("Unknown directive")));

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
}
