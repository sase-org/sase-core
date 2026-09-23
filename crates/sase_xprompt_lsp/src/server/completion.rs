use super::catalogs::{
    active_artifact_ref_project, at_reference_kind_inventory,
    at_reference_path_inventory, file_history, known_at_reference_kinds,
    load_machine_catalog, load_vcs_project_catalog,
};
use super::completion_items::{
    bool_completion_list, directive_snippet_items, empty_completion_list,
    empty_completion_response, is_directive_argument_context,
    is_finalizer_value_context, is_rich_model_value_context,
    model_alias_keys_from_catalog, model_alias_shortcut_completion,
    model_completion_list, model_insertion_is_self_ref,
    model_shortcut_completion, needs_agent_entries, needs_bead_entries,
    needs_host_catalog, needs_machine_entries, needs_model_alias_keys,
    ranked_vcs_repo_entries, replacement_ends_line, sase_snippet_items,
    xprompt_snippet_items,
};
use super::initialize::enabled_feature_flags;
use super::state::{
    ArtifactRefCatalogProject, ServerConfig, VcsProjectCatalog,
    XpromptLspServer,
};
use super::*;

impl XpromptLspServer {
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

        // The `=alias` shortcut is also document-local (the shared detector
        // already excludes placeholder/directive/literal zones), and its
        // model catalog is a synchronous file read, so classify it before
        // any catalog refresh too. A valid equals context owns the response
        // even when no alias matches, so this never falls through to the
        // generic classifier below.
        if let Some(context) = editor_detect_model_alias_shortcut_context(
            document.text(),
            editor_position,
        ) {
            return Some(model_alias_shortcut_completion(
                document.text(),
                editor_position,
                &context,
                config.model_catalog.as_deref(),
            ));
        }
        if let Some(context) =
            editor_model_shortcut_context(document.text(), editor_position)
                .filter(|context| context.kind == ModelShortcutKind::Model)
        {
            return Some(model_shortcut_completion(
                document.text(),
                editor_position,
                &context,
                config.model_catalog.as_deref(),
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
        if context.kind == CompletionContextKind::XpromptArgumentAgent {
            return Some(self.agent_completion(&context, &config).await);
        }
        if is_directive_argument_context(&context) {
            return Some(
                self.directive_completion(&context, &config, &document)
                    .await,
            );
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
        let mut response = completion_response(list, context.replacement_range);
        if context.kind == CompletionContextKind::DirectiveName {
            if let CompletionResponse::Array(items) = &mut response {
                items.extend(directive_snippet_items(
                    context.token.as_ref().map(|token| token.text.as_str()),
                    context.replacement_range,
                    &enabled_feature_flags(
                        config.typed_launch_units,
                        config.queue_capacity_budget,
                    ),
                    config.snippet_support,
                ));
            }
        }
        Some(response)
    }

    pub(super) fn at_reference_completion(
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
    pub(super) fn vcs_project_completion(
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
        vcs_project_completion_response(
            list,
            context.replacement_range,
            &vcs_catalog.entries,
        )
    }

    /// Build the `#workflow:` / `#workflow(` root-ref completion response.
    ///
    /// The enabled project/PR rows and optional namespace rows come from the
    /// materialized catalog already loaded for context classification. No helper
    /// bridge call is needed on this completion path.
    pub(super) fn vcs_ref_completion(
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

    pub(super) async fn vcs_repo_completion(
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

    pub(super) async fn agent_completion(
        &self,
        context: &sase_core::CompletionContext,
        config: &ServerConfig,
    ) -> CompletionResponse {
        let response = match self
            .catalog_cache
            .agent_catalog_for_completion(config.project.clone())
            .await
        {
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
        let list = editor_build_agent_completion_candidates(
            token,
            None,
            &response.entries,
            &context.selected_values,
        );
        agent_completion_response(list, context.replacement_range)
    }

    pub(super) async fn directive_completion(
        &self,
        context: &sase_core::CompletionContext,
        config: &ServerConfig,
        document: &DocumentSnapshot,
    ) -> CompletionResponse {
        if context.value_role() == Some(DirectiveValueRole::PathOrExecutable) {
            let token = context
                .token
                .as_ref()
                .map(|token| token.text.as_str())
                .unwrap_or_default();
            let list = editor_build_file_completion_candidates_with_base(
                token,
                config.root_dir.as_deref(),
            );
            return completion_response(
                apply_replacement(list, context.replacement_range),
                context.replacement_range,
            );
        }

        if is_finalizer_value_context(context) {
            return self.finalizer_directive_completion(context, config).await;
        }
        let inventories =
            self.directive_inventories(context, config, document).await;
        if is_rich_model_value_context(context) {
            return self.model_directive_completion(
                context,
                &inventories,
                config,
            );
        }
        let list =
            editor_build_directive_clause_candidates(context, &inventories);
        agent_completion_response(list, context.replacement_range)
    }

    pub(super) async fn finalizer_directive_completion(
        &self,
        context: &sase_core::CompletionContext,
        config: &ServerConfig,
    ) -> CompletionResponse {
        let mut inventories = DirectiveCompletionInventories {
            enabled_feature_flags: enabled_feature_flags(
                config.typed_launch_units,
                config.queue_capacity_budget,
            ),
            ..DirectiveCompletionInventories::default()
        };
        match self
            .catalog_cache
            .finalizer_catalog_for_completion(config.project.clone())
            .await
        {
            Ok(response) if response.status == "ok" => {
                inventories.finalizers = response.entries.clone();
            }
            Ok(response) => {
                if !response.message.is_empty() {
                    warn!(
                        "finalizer catalog returned no entries: {}",
                        response.message
                    );
                }
                return empty_completion_response();
            }
            Err(error) => {
                self.warn_once(&error).await;
                return empty_completion_response();
            }
        }
        let list =
            editor_build_directive_clause_candidates(context, &inventories);
        finalizer_completion_response(list, context.replacement_range)
    }

    pub(super) async fn directive_inventories(
        &self,
        context: &sase_core::CompletionContext,
        config: &ServerConfig,
        _document: &DocumentSnapshot,
    ) -> DirectiveCompletionInventories {
        let mut inventories = DirectiveCompletionInventories {
            enabled_feature_flags: enabled_feature_flags(
                config.typed_launch_units,
                config.queue_capacity_budget,
            ),
            ..DirectiveCompletionInventories::default()
        };
        if needs_model_alias_keys(context) {
            inventories.model_alias_keys =
                model_alias_keys_from_catalog(config.model_catalog.as_deref());
        }
        if needs_machine_entries(context) {
            inventories.machines =
                load_machine_catalog(config.machine_catalog.as_deref());
        }
        if !needs_host_catalog(context) {
            return inventories;
        }
        match self
            .catalog_cache
            .agent_catalog_for_completion(config.project.clone())
            .await
        {
            Ok(response) if response.status == "ok" => {
                if needs_agent_entries(context) {
                    inventories.agents = response.entries.clone();
                }
                if needs_bead_entries(context) {
                    inventories.beads = response.beads.clone();
                }
            }
            Ok(response) => {
                if !response.message.is_empty() {
                    warn!(
                        "agent catalog returned no entries: {}",
                        response.message
                    );
                }
            }
            Err(error) => {
                self.warn_once(&error).await;
            }
        }
        inventories
    }

    pub(super) fn model_directive_completion(
        &self,
        context: &sase_core::CompletionContext,
        inventories: &DirectiveCompletionInventories,
        config: &ServerConfig,
    ) -> CompletionResponse {
        let token = context
            .token
            .as_ref()
            .map(|token| token.text.as_str())
            .unwrap_or_default();
        let mut list =
            model_completion_list(token, config.model_catalog.as_deref());
        if let Some(keyword) = context.active_keyword() {
            list.candidates.retain(|candidate| {
                !model_insertion_is_self_ref(&candidate.insertion, keyword)
            });
        }
        let mut items =
            match model_completion_response(list, context.replacement_range) {
                CompletionResponse::Array(items) => items,
                other => return other,
            };
        if context.kind == CompletionContextKind::DirectiveArgument
            && context.syntax_form() == Some(DirectiveSyntaxForm::Parenthesized)
            && context.clause_kind() == Some(DirectiveClauseKind::Positional)
        {
            let mut alias_context = context.clone();
            alias_context.kind =
                CompletionContextKind::DirectiveArgumentKeyword;
            let aliases = editor_build_directive_clause_candidates(
                &alias_context,
                inventories,
            );
            if let CompletionResponse::Array(alias_items) =
                agent_completion_response(aliases, context.replacement_range)
            {
                items.extend(alias_items);
            }
        }
        CompletionResponse::Array(items)
    }

    pub(super) async fn vcs_repo_catalog_for_completion(
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

    pub(super) async fn entries_for_completion(
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

    pub(super) async fn snippets_for_completion(
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

    pub(super) fn completion_list_for_context(
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
                editor_build_directive_completion_candidates_with_flags(
                    token,
                    &enabled_feature_flags(
                        config.typed_launch_units,
                        config.queue_capacity_budget,
                    ),
                )
            }
            CompletionContextKind::DirectiveArgument
            | CompletionContextKind::DirectiveArgumentKeyword
            | CompletionContextKind::DirectiveArgumentValue => {
                // Handled out-of-band in `directive_completion`, which loads
                // host inventories only for the active value role.
                empty_completion_list()
            }
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
}
