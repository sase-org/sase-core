use super::catalogs::{
    active_artifact_ref_context, active_glossary_project,
    load_vcs_project_catalog,
};
use super::initialize::enabled_feature_flags;
use super::state::{
    GlossaryCatalogProject, RecentParenInsertion, ServerConfig,
    XpromptLspServer,
};
use super::*;

impl XpromptLspServer {
    pub async fn hover_for_text(
        &self,
        text: String,
        position: Position,
    ) -> Option<Hover> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let document = DocumentSnapshot::new(text);
        if let Some(hover) = editor_hover_at_position_with_flags(
            &document,
            to_editor_position(position),
            entries.as_slice(),
            &enabled_feature_flags(
                config.typed_launch_units,
                config.queue_capacity_budget,
            ),
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

    pub(super) async fn diagnostics_for_document(
        &self,
        document: DocumentSnapshot,
    ) -> Vec<lsp_types::Diagnostic> {
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let mut diagnostics =
            editor_analyze_document(&document, entries.as_slice());
        diagnostics.extend(editor_typed_launch_directive_diagnostics(
            &document,
            config.typed_launch_units,
        ));
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

    pub(super) fn semantic_tokens_for_document(
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
        let argument_entries =
            if !self.catalog_cache.stale_or_missing(&config.catalog_key) {
                self.catalog_cache.cached_entries(&config.catalog_key)
            } else {
                None
            };
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
            argument_entries.as_deref().map(Vec::as_slice),
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

        if config.typed_launch_units {
            actions.extend(typed_launch_code_actions(&uri, &document, range));
        }

        actions.push(CodeActionOrCommand::Command(Command::new(
            "Refresh xprompt catalog".to_string(),
            REFRESH_COMMAND.to_string(),
            None,
        )));
        actions
    }

    // Client-visible URI form: the returned URI echoes the catalog-supplied
    // definition path verbatim (no canonicalization). LSP clients match URIs
    // by string, so resolving a symlinked ancestor here (e.g. `/tmp` to
    // `/private/tmp` on macOS) would break go-to-definition for any client
    // holding the unresolved form.
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

    pub fn on_type_formatting_for_text(
        &self,
        text: String,
        position: Position,
        ch: &str,
    ) -> Option<Vec<TextEdit>> {
        self.on_type_formatting_for_text_with_recent(text, position, ch, None)
    }

    pub(super) fn on_type_formatting_for_text_with_recent(
        &self,
        text: String,
        position: Position,
        ch: &str,
        recent_paren_insertion: Option<RecentParenInsertion>,
    ) -> Option<Vec<TextEdit>> {
        if ch != "(" {
            return None;
        }
        let document = DocumentSnapshot::new(text);
        let cursor =
            document.position_to_byte_offset(to_editor_position(position))?;
        let text = document.text();
        let (opener_idx, after_opener_idx) =
            if text.as_bytes().get(cursor) == Some(&b'(') {
                (cursor, cursor + 1)
            } else {
                let opener_idx = cursor.checked_sub(1)?;
                if text.as_bytes().get(opener_idx) != Some(&b'(') {
                    return None;
                }
                (opener_idx, cursor)
            };

        let mut pre_insert_text =
            String::with_capacity(text.len().saturating_sub(1));
        pre_insert_text.push_str(text.get(..opener_idx)?);
        pre_insert_text.push_str(text.get(after_opener_idx..)?);
        let pre_insert_document = DocumentSnapshot::new(pre_insert_text);
        let pre_insert_position =
            pre_insert_document.byte_offset_to_position(opener_idx)?;
        if let Some(edit) =
            editor_plan_argument_double_colon_to_parentheses_edit(
                &pre_insert_document,
                pre_insert_position,
            )
        {
            let delimiter = edit.new_text.strip_prefix("()")?;
            let closer_end = if recent_paren_insertion.is_some_and(|recent| {
                recent.opener_idx == opener_idx
                    && recent.closer_idx == Some(after_opener_idx)
            }) && text.as_bytes().get(after_opener_idx)
                == Some(&b')')
            {
                after_opener_idx + 1
            } else {
                after_opener_idx
            };
            return Some(vec![
                TextEdit {
                    range: to_lsp_range(edit.range),
                    new_text: String::new(),
                },
                TextEdit {
                    range: to_lsp_range(
                        document.byte_range_to_range(
                            after_opener_idx,
                            closer_end,
                        )?,
                    ),
                    new_text: format!("){delimiter}"),
                },
            ]);
        }
        let edit = editor_plan_argument_colon_to_parentheses_edit(
            &pre_insert_document,
            pre_insert_position,
        )?;
        Some(vec![TextEdit {
            range: to_lsp_range(edit.range),
            new_text: edit.new_text,
        }])
    }
}

pub(super) fn entry_for_token<'a>(
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

pub(super) fn canonical_marker_action(
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

pub(super) fn typed_launch_code_actions(
    uri: &Uri,
    document: &DocumentSnapshot,
    range: Range,
) -> Vec<CodeActionOrCommand> {
    let Some(line) = document.line_text(range.start.line) else {
        return Vec::new();
    };
    let leading = line.len() - line.trim_start_matches([' ', '\t']).len();
    let header = line[leading..].trim_end();
    if !is_typed_launch_fence_header(header) {
        return Vec::new();
    }
    let line_end = line.chars().map(char::len_utf16).sum::<usize>() as u32;
    let edit_range = EditorRange {
        start: sase_core::EditorPosition {
            line: range.start.line,
            character: leading_utf16_units(line),
        },
        end: sase_core::EditorPosition {
            line: range.start.line,
            character: line_end,
        },
    };
    let directive = if header.starts_with("%if") {
        "%if"
    } else {
        "%proc"
    };
    vec![text_edit_action(
        &format!("Complete {directive} bash fence"),
        uri,
        edit_range,
        format!("{header}\n\n```bash\n\n```"),
        CodeActionKind::QUICKFIX,
        false,
    )
    .into()]
}

pub(super) fn is_typed_launch_fence_header(header: &str) -> bool {
    if !header.ends_with("::") {
        return false;
    }
    header == "%if::"
        || header == "%proc::"
        || (header.starts_with("%proc(") && header.ends_with(")::"))
}

pub(super) fn leading_utf16_units(line: &str) -> u32 {
    line.chars()
        .take_while(|ch| matches!(ch, ' ' | '\t'))
        .map(char::len_utf16)
        .sum::<usize>() as u32
}

pub(super) fn text_edit_action(
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

pub(super) fn plain_named_args_skeleton(entry: &XpromptAssistEntry) -> String {
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

pub(super) fn definition_uri_at_position(
    document: &DocumentSnapshot,
    position: sase_core::EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<Uri> {
    let target = editor_definition_at_position(document, position, entries)?;
    Uri::from_file_path(target.path)
}

pub(super) fn glossary_hover_at_position(
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

pub(super) fn glossary_definition_at_position(
    document: &DocumentSnapshot,
    position: sase_core::EditorPosition,
    project: &GlossaryCatalogProject,
) -> Option<Location> {
    let span = project.catalog.lookup(document.text(), position)?;
    let entry = glossary_entry_for_span(project, &span)?;
    let source = entry.source.as_ref();
    let path = source
        .and_then(|source| source.source_path.as_deref())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or(project.config_path.as_str());
    let uri = Uri::from_file_path(Path::new(path))?;
    Some(Location {
        uri,
        range: source
            .and_then(|source| source.body_range)
            .map(to_lsp_range)
            .unwrap_or_else(zero_range),
    })
}

pub(super) fn glossary_entry_for_span<'a>(
    project: &'a GlossaryCatalogProject,
    span: &GlossarySpanWire,
) -> Option<&'a GlossaryEntryWire> {
    project.catalog.catalog().entries.get(span.entry_index)
}

pub(super) fn glossary_hover_markdown(
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

pub(super) fn markdown_code(value: &str) -> String {
    format!("`{}`", value.replace('`', "\\`"))
}

pub(super) fn zero_range() -> Range {
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

pub(super) fn detect_recent_paren_insertion(
    previous: &str,
    current: &str,
    previous_recent: Option<RecentParenInsertion>,
) -> Option<RecentParenInsertion> {
    if current.len() <= previous.len() {
        return None;
    }
    let previous_bytes = previous.as_bytes();
    let current_bytes = current.as_bytes();
    let mut prefix = 0usize;
    while prefix < previous_bytes.len()
        && prefix < current_bytes.len()
        && previous_bytes[prefix] == current_bytes[prefix]
    {
        prefix += 1;
    }

    let mut previous_suffix = previous_bytes.len();
    let mut current_suffix = current_bytes.len();
    while previous_suffix > prefix
        && current_suffix > prefix
        && previous_bytes[previous_suffix - 1]
            == current_bytes[current_suffix - 1]
    {
        previous_suffix -= 1;
        current_suffix -= 1;
    }
    if previous_suffix != prefix
        || !previous.is_char_boundary(prefix)
        || !current.is_char_boundary(prefix)
        || !current.is_char_boundary(current_suffix)
    {
        return None;
    }

    match &current[prefix..current_suffix] {
        "(" => Some(RecentParenInsertion {
            opener_idx: prefix,
            closer_idx: None,
        }),
        "()" => Some(RecentParenInsertion {
            opener_idx: prefix,
            closer_idx: Some(prefix + 1),
        }),
        ")" => previous_recent.and_then(|recent| {
            (recent.closer_idx.is_none() && prefix == recent.opener_idx + 1)
                .then_some(RecentParenInsertion {
                    opener_idx: recent.opener_idx,
                    closer_idx: Some(prefix),
                })
        }),
        _ => None,
    }
}

pub(super) fn document_eligible(
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

pub(super) fn markdown_uri_eligible(uri: &Uri) -> bool {
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
pub(super) fn is_memory_note_path(path: &Path) -> bool {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        == Some(MEMORY_NAMESPACE_SEGMENT)
}

pub(super) fn is_prompt_temp_markdown_name(file_name: &str) -> bool {
    ["sase_ace_prompt_", "sase_prompt_"].iter().any(|prefix| {
        file_name.strip_prefix(prefix).is_some_and(|rest| {
            rest.len() > ".md".len() && rest.ends_with(".md")
        })
    })
}

pub(super) fn should_invalidate_for_uri(uri: &Uri) -> bool {
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
