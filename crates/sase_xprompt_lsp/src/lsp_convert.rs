use lsp_types::{
    Command, CompletionItem, CompletionItemKind, CompletionItemLabelDetails,
    CompletionItemTag, CompletionResponse, CompletionTextEdit, Documentation,
    InsertTextFormat, MarkupContent, MarkupKind, NumberOrString, Position,
    Range, TextEdit,
};
use sase_core::{
    AtReferenceContextWire, AtReferenceGroup, AtReferenceMenuWire,
    AtReferenceRowWire, AtReferenceStage, CompletionCandidate, CompletionList,
    DiagnosticSeverity, EditorDiagnostic, EditorPosition, EditorRange,
    EditorTextEdit, HoverPayload, VcsRepoEntry,
};

pub fn to_editor_position(position: Position) -> EditorPosition {
    EditorPosition {
        line: position.line,
        character: position.character,
    }
}

pub fn to_lsp_range(range: EditorRange) -> Range {
    Range {
        start: to_lsp_position(range.start),
        end: to_lsp_position(range.end),
    }
}

pub fn completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .map(|candidate| completion_item(candidate, replacement_range))
            .collect(),
    )
}

/// Render the `@` reference menu as an *incomplete* list whose items all filter
/// on the reference text the user actually typed.
///
/// The rows are ranked server-side by the shared fuzzy matcher, and a client
/// that prefix-filters the inserted reference (`@research:202607/…`) against the
/// typed query (`@research:site`) would throw every fuzzy row away. Setting
/// `filterText` to the typed text keeps every row alive in prefix *and* fuzzy
/// clients, and `isIncomplete` makes them re-request per keystroke instead of
/// re-filtering (and re-sorting) a stale list, so `sortText` decides the order.
pub fn at_reference_completion_response(
    menu: AtReferenceMenuWire,
    context: &AtReferenceContextWire,
    replacement_range: EditorRange,
) -> CompletionResponse {
    let filter_text = at_reference_filter_text(context);
    let payload_kind = context.kind.as_deref();
    let truncated_payloads = menu.truncated_payloads;
    CompletionResponse::List(lsp_types::CompletionList {
        is_incomplete: true,
        items: menu
            .rows
            .into_iter()
            .enumerate()
            .map(|(index, row)| {
                at_reference_completion_item(
                    row,
                    payload_kind,
                    &filter_text,
                    replacement_range,
                    index,
                    truncated_payloads,
                )
            })
            .collect(),
    })
}

/// Reconstruct the reference text as typed, which is what clients filter on.
fn at_reference_filter_text(context: &AtReferenceContextWire) -> String {
    match (context.stage, context.kind.as_deref()) {
        (AtReferenceStage::Payload, Some(kind)) => {
            format!("@{kind}:{}", context.query)
        }
        _ => format!("@{}", context.query),
    }
}

fn at_reference_completion_item(
    row: AtReferenceRowWire,
    payload_kind: Option<&str>,
    filter_text: &str,
    replacement_range: EditorRange,
    index: usize,
    truncated_payloads: usize,
) -> CompletionItem {
    let (group, kind, description) = match row.group {
        AtReferenceGroup::Artifact => {
            (0, CompletionItemKind::ENUM_MEMBER, "artifact kind")
        }
        AtReferenceGroup::File if row.is_dir => {
            (1, CompletionItemKind::FOLDER, "directory")
        }
        AtReferenceGroup::File => (1, CompletionItemKind::FILE, "file"),
        AtReferenceGroup::Payload => {
            let kind = payload_kind.unwrap_or("file");
            (0, payload_completion_item_kind(kind), kind)
        }
    };
    // Providers that have no separate title echo the primary text back as one;
    // repeating it beside the label and in the preview is pure noise.
    let title = if row.title == row.label {
        ""
    } else {
        row.title.as_str()
    };
    CompletionItem {
        label: row.insertion.clone(),
        label_details: Some(CompletionItemLabelDetails {
            detail: (!title.is_empty()).then(|| format!(" · {title}")),
            description: Some(description.to_string()),
        }),
        kind: Some(kind),
        documentation: at_reference_documentation(
            &row.label,
            &row.label_match,
            title,
            &row.body,
        ),
        detail: at_reference_item_detail(row.detail, truncated_payloads),
        filter_text: Some(filter_text.to_string()),
        sort_text: Some(format!("{group}:{index:04}")),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(replacement_range),
            new_text: row.insertion,
        })),
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

/// Payload kinds that resolve to an identifier (a SHA, a bead id, an agent
/// name) rather than a filesystem path render with a reference icon; every
/// other kind keeps the file icon it always had.
fn payload_completion_item_kind(kind: &str) -> CompletionItemKind {
    if matches!(kind, "commit" | "bead" | "agent") {
        CompletionItemKind::REFERENCE
    } else {
        CompletionItemKind::FILE
    }
}

fn at_reference_item_detail(
    detail: String,
    truncated_payloads: usize,
) -> Option<String> {
    if truncated_payloads == 0 {
        return (!detail.is_empty()).then_some(detail);
    }
    let omitted = format!(
        "at least {truncated_payloads} additional payload{} not shown",
        if truncated_payloads == 1 { "" } else { "s" }
    );
    Some(if detail.is_empty() {
        omitted
    } else {
        format!("{detail} · {omitted}")
    })
}

/// Hover documentation shows at most this many lines of a payload row's body
/// (a commit message, say) so a large one cannot flood the popup.
const AT_REFERENCE_BODY_DOC_MAX_LINES: usize = 12;

/// Show *why* a fuzzy row is in the list: the matched payload with its matched
/// runs bolded, and the title underneath. Editors cannot highlight inside a
/// completion label, so the preview window carries the match affordance the ACE
/// prompt input paints inline.
fn at_reference_documentation(
    label: &str,
    label_match: &[(u32, u32)],
    title: &str,
    body: &str,
) -> Option<Documentation> {
    if label.is_empty() {
        return None;
    }
    let mut value = bold_match_runs(label, label_match);
    if !title.is_empty() {
        value.push('\n');
        value.push('\n');
        value.push_str(title);
    }
    if let Some(body) = truncated_body_block(body) {
        value.push('\n');
        value.push('\n');
        value.push_str(&body);
    }
    Some(markdown_doc(value))
}

/// Render `body` as a fenced block, truncated to
/// [`AT_REFERENCE_BODY_DOC_MAX_LINES`] lines. `None` when `body` is empty.
fn truncated_body_block(body: &str) -> Option<String> {
    let body = body.trim();
    if body.is_empty() {
        return None;
    }
    let mut lines = body.lines();
    let kept = lines
        .by_ref()
        .take(AT_REFERENCE_BODY_DOC_MAX_LINES)
        .collect::<Vec<_>>()
        .join("\n");
    let mut block = String::from("```\n");
    block.push_str(&kept);
    if lines.next().is_some() {
        block.push_str("\n…");
    }
    block.push_str("\n```");
    Some(block)
}

/// Wrap each half-open char range of `runs` in `**`, leaving `text` otherwise
/// verbatim. Runs are already ordered and non-overlapping.
fn bold_match_runs(text: &str, runs: &[(u32, u32)]) -> String {
    if runs.is_empty() {
        return text.to_string();
    }
    let mut rendered = String::with_capacity(text.len() + runs.len() * 4);
    let mut runs = runs.iter().peekable();
    for (index, ch) in text.chars().enumerate() {
        let index = index as u32;
        if runs.peek().is_some_and(|(start, _)| *start == index) {
            rendered.push_str("**");
        }
        rendered.push(ch);
        if let Some((_, end)) = runs.peek().copied() {
            if *end == index + 1 {
                rendered.push_str("**");
                runs.next();
            }
        }
    }
    rendered
}

/// Render `%model` rows with model/alias kinds and stable catalog ordering.
pub fn model_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .enumerate()
            .map(|(index, candidate)| {
                let kind = candidate.kind.clone();
                let alias_kind = candidate.status.clone();
                let is_alias = is_model_alias_kind(&kind);
                let mut item = completion_item(candidate, replacement_range);
                item.kind = Some(if is_alias {
                    CompletionItemKind::ENUM_MEMBER
                } else {
                    CompletionItemKind::VALUE
                });
                item.label_details = Some(CompletionItemLabelDetails {
                    detail: None,
                    description: Some(model_completion_kind_label(
                        &kind,
                        &alias_kind,
                    )),
                });
                let group = u8::from(is_alias);
                item.sort_text = Some(format!("{group}:{index:04}"));
                item
            })
            .collect(),
    )
}

fn is_model_alias_kind(kind: &str) -> bool {
    matches!(kind, "implicit_alias" | "user_alias")
}

fn model_completion_kind_label(kind: &str, alias_kind: &str) -> String {
    if !is_model_alias_kind(kind) {
        return "model".to_string();
    }
    match alias_kind {
        "default" => "default",
        "role" => "role",
        "provider_coder" => "coder",
        "user" => "custom",
        _ if kind == "user_alias" => "custom",
        _ => "role",
    }
    .to_string()
}

/// Render kind-aware wait/fork targets without losing the core candidate order.
pub fn agent_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .enumerate()
            .map(|(index, candidate)| {
                let kind = candidate.kind.clone();
                let detail = candidate.detail.clone();
                let mut item = completion_item(candidate, replacement_range);
                item.kind = Some(agent_completion_item_kind(&kind));
                item.label_details = Some(CompletionItemLabelDetails {
                    detail: None,
                    description: Some(agent_completion_label(
                        &kind,
                        detail.as_deref(),
                    )),
                });
                item.sort_text = Some(format!(
                    "{}:{index:04}",
                    agent_completion_sort_group(&kind)
                ));
                item
            })
            .collect(),
    )
}

fn agent_completion_item_kind(kind: &str) -> CompletionItemKind {
    match kind {
        "keyword" => CompletionItemKind::KEYWORD,
        "tribe" => CompletionItemKind::ENUM_MEMBER,
        "clan" => CompletionItemKind::MODULE,
        "family" => CompletionItemKind::CLASS,
        _ => CompletionItemKind::VALUE,
    }
}

fn agent_completion_sort_group(kind: &str) -> u8 {
    match kind {
        "keyword" => 0,
        "tribe" => 1,
        "clan" => 2,
        "family" => 3,
        _ => 4,
    }
}

fn agent_completion_label(kind: &str, detail: Option<&str>) -> String {
    let normalized = match kind {
        "keyword" | "tribe" | "clan" | "family" => kind,
        _ => "agent",
    };
    match detail.filter(|detail| !detail.is_empty()) {
        Some(detail) if detail.starts_with(normalized) => detail.to_string(),
        Some(detail) => format!("{normalized} · {detail}"),
        None => normalized.to_string(),
    }
}

/// Build variable-like placeholder completion items in document order.
pub fn placeholder_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
    prefix: &str,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .enumerate()
            .map(|(index, candidate)| {
                let mut item = completion_item(candidate, replacement_range);
                item.kind = Some(CompletionItemKind::VARIABLE);
                item.filter_text = Some(prefix.to_string());
                item.sort_text = Some(format!("{index:04}"));
                item
            })
            .collect(),
    )
}

/// Build the completion response for the `+` (`vcs_project`) completion kind.
///
/// Differs from [`completion_response`] in two ways: the `filter_text` is the
/// `+name` trigger spelling (so typing `+sa` keeps the `sase` item), and the
/// item kind/label details distinguish projects from PRs. The primary
/// `text_edit` and `additional_text_edits` (the prepend/replace edit) are
/// carried over from the candidate's `replacement` / `additional_edits`.
pub fn vcs_project_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .map(|candidate| {
                vcs_project_completion_item(candidate, replacement_range)
            })
            .collect(),
    )
}

pub fn vcs_repo_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
    entries: &[VcsRepoEntry],
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .zip(entries.iter())
            .enumerate()
            .map(|(index, (candidate, entry))| {
                vcs_repo_completion_item(
                    candidate,
                    entry,
                    replacement_range,
                    index,
                )
            })
            .collect(),
    )
}

pub fn vcs_ref_completion_response(
    list: CompletionList,
    replacement_range: EditorRange,
) -> CompletionResponse {
    CompletionResponse::Array(
        list.candidates
            .into_iter()
            .enumerate()
            .map(|(index, candidate)| {
                vcs_ref_completion_item(candidate, replacement_range, index)
            })
            .collect(),
    )
}

pub fn snippet_completion_item(
    label: String,
    new_text: String,
    detail: Option<String>,
    documentation: Option<String>,
    replacement_range: EditorRange,
) -> CompletionItem {
    CompletionItem {
        label,
        label_details: Some(CompletionItemLabelDetails {
            detail: Some(" snippet".to_string()),
            description: None,
        }),
        kind: Some(CompletionItemKind::SNIPPET),
        detail,
        documentation: documentation.map(markdown_doc),
        insert_text_format: Some(InsertTextFormat::SNIPPET),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(replacement_range),
            new_text,
        })),
        ..Default::default()
    }
}

pub fn sase_snippet_completion_item(
    label: String,
    template: String,
    detail: Option<String>,
    documentation: Option<String>,
    replacement_range: EditorRange,
) -> CompletionItem {
    let retrigger_placeholder =
        first_tabstop_is_immediately_inside_angles(&template);
    let mut item = snippet_completion_item(
        label,
        sase_template_to_lsp_snippet(&template),
        detail,
        documentation,
        replacement_range,
    );
    if retrigger_placeholder {
        item.command = Some(Command::new(
            "Trigger Suggest".to_string(),
            "editor.action.triggerSuggest".to_string(),
            None,
        ));
    }
    item
}

fn first_tabstop_is_immediately_inside_angles(template: &str) -> bool {
    let marker = ["$1", "$0"].into_iter().find_map(|needle| {
        template.match_indices(needle).find_map(|(index, marker)| {
            let end = index + marker.len();
            let digit_continues = template
                .get(end..)
                .and_then(|tail| tail.chars().next())
                .is_some_and(|ch| ch.is_ascii_digit());
            (!digit_continues).then_some((index, end))
        })
    });
    let Some((start, end)) = marker else {
        return false;
    };
    template
        .get(..start)
        .is_some_and(|before| before.ends_with('<'))
        && template
            .get(end..)
            .is_some_and(|after| after.starts_with('>'))
}

pub fn sase_template_to_lsp_snippet(template: &str) -> String {
    let mut converted = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '$' if chars.peek().is_some_and(|next| next.is_ascii_digit()) => {
                converted.push('$');
                while let Some(next) = chars.peek().copied() {
                    if !next.is_ascii_digit() {
                        break;
                    }
                    converted.push(next);
                    chars.next();
                }
            }
            '$' => converted.push_str("\\$"),
            '}' => converted.push_str("\\}"),
            '\\' => converted.push_str("\\\\"),
            _ => converted.push(ch),
        }
    }
    converted
}

pub fn hover(payload: HoverPayload) -> lsp_types::Hover {
    lsp_types::Hover {
        contents: lsp_types::HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: payload.markdown,
        }),
        range: Some(to_lsp_range(payload.range)),
    }
}

pub fn diagnostic(diagnostic: EditorDiagnostic) -> lsp_types::Diagnostic {
    lsp_types::Diagnostic {
        range: to_lsp_range(diagnostic.range),
        severity: Some(to_lsp_diagnostic_severity(diagnostic.severity)),
        code: Some(NumberOrString::String(diagnostic.code)),
        source: Some("sase-xprompt".to_string()),
        message: diagnostic.message,
        ..Default::default()
    }
}

fn completion_item(
    candidate: CompletionCandidate,
    replacement_range: EditorRange,
) -> CompletionItem {
    let range = candidate
        .replacement
        .as_ref()
        .map(|replacement| replacement.range)
        .unwrap_or(replacement_range);
    let new_text = candidate
        .replacement
        .map(|replacement| replacement.new_text)
        .unwrap_or_else(|| candidate.insertion.clone());
    CompletionItem {
        label: candidate.display,
        kind: Some(if candidate.kind == "artifact_payload" {
            payload_completion_item_kind(&candidate.status)
        } else if candidate.is_dir {
            CompletionItemKind::FOLDER
        } else {
            CompletionItemKind::TEXT
        }),
        detail: candidate.detail,
        documentation: candidate.documentation.map(markdown_doc),
        filter_text: Some(candidate.name),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(range),
            new_text,
        })),
        additional_text_edits: additional_text_edits(
            candidate.additional_edits,
        ),
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

/// Convert one `vcs_project` candidate, overriding the generic item's kind,
/// label details, and `filter_text` so the `+name` trigger spelling drives
/// client-side filtering.
fn vcs_project_completion_item(
    candidate: CompletionCandidate,
    replacement_range: EditorRange,
) -> CompletionItem {
    let filter_text = format!("+{}", candidate.name);
    let is_patch = is_patch_completion_kind(&candidate.kind);
    let label_details = if is_patch {
        Some(CompletionItemLabelDetails {
            detail: (!candidate.project.is_empty())
                .then(|| format!(" · {}", candidate.project)),
            description: Some(if candidate.status.is_empty() {
                "PR".to_string()
            } else {
                format!("PR · {}", candidate.status)
            }),
        })
    } else {
        Some(CompletionItemLabelDetails {
            detail: None,
            description: Some("project".to_string()),
        })
    };
    let detail = Some(candidate.insertion.clone());
    let kind = if is_patch {
        CompletionItemKind::EVENT
    } else {
        CompletionItemKind::MODULE
    };
    let mut item = completion_item(candidate, replacement_range);
    item.kind = Some(kind);
    item.filter_text = Some(filter_text);
    item.label_details = label_details;
    item.detail = detail;
    item
}

fn vcs_ref_completion_item(
    candidate: CompletionCandidate,
    replacement_range: EditorRange,
    index: usize,
) -> CompletionItem {
    let range = candidate
        .replacement
        .as_ref()
        .map(|replacement| replacement.range)
        .unwrap_or(replacement_range);
    let new_text = candidate
        .replacement
        .as_ref()
        .map(|replacement| replacement.new_text.clone())
        .unwrap_or_else(|| candidate.insertion.clone());
    let group = vcs_ref_sort_group(&candidate);
    let is_namespace = candidate.kind == "namespace";
    let label_details = vcs_ref_label_details(&candidate);
    let item_kind = vcs_ref_item_kind(&candidate);
    let filter_text = candidate.name.clone();
    let sort_text =
        format!("{group}:{}:{index:04}", candidate.name.to_lowercase());

    CompletionItem {
        label: candidate.display,
        label_details: Some(label_details),
        kind: Some(item_kind),
        detail: candidate.detail.clone(),
        documentation: candidate.documentation.map(markdown_doc),
        filter_text: Some(filter_text),
        sort_text: Some(sort_text),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(range),
            new_text,
        })),
        additional_text_edits: additional_text_edits(
            candidate.additional_edits,
        ),
        command: is_namespace.then(|| {
            Command::new(
                "Trigger Suggest".to_string(),
                "editor.action.triggerSuggest".to_string(),
                None,
            )
        }),
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

fn vcs_ref_item_kind(candidate: &CompletionCandidate) -> CompletionItemKind {
    match candidate.kind.as_str() {
        "patch" | "changespec" => CompletionItemKind::REFERENCE,
        "namespace" => CompletionItemKind::FOLDER,
        _ => CompletionItemKind::MODULE,
    }
}

fn vcs_ref_sort_group(candidate: &CompletionCandidate) -> u8 {
    match candidate.kind.as_str() {
        "patch" | "changespec" => 1,
        "namespace" => 2,
        _ => 0,
    }
}

fn vcs_ref_label_details(
    candidate: &CompletionCandidate,
) -> CompletionItemLabelDetails {
    match candidate.kind.as_str() {
        "patch" | "changespec" => CompletionItemLabelDetails {
            detail: (!candidate.project.is_empty())
                .then(|| format!(" · {}", candidate.project)),
            description: Some(if candidate.status.is_empty() {
                "PR".to_string()
            } else {
                format!("PR · {}", candidate.status)
            }),
        },
        "namespace" => CompletionItemLabelDetails {
            detail: None,
            description: Some(if candidate.status.is_empty() {
                "org".to_string()
            } else {
                candidate.status.clone()
            }),
        },
        _ => CompletionItemLabelDetails {
            detail: None,
            description: Some("project".to_string()),
        },
    }
}

fn is_patch_completion_kind(kind: &str) -> bool {
    matches!(kind, "patch" | "changespec")
}

fn vcs_repo_completion_item(
    candidate: CompletionCandidate,
    entry: &VcsRepoEntry,
    replacement_range: EditorRange,
    index: usize,
) -> CompletionItem {
    let badges = vcs_repo_badges(entry);
    let range = candidate
        .replacement
        .as_ref()
        .map(|replacement| replacement.range)
        .unwrap_or(replacement_range);
    let new_text = candidate
        .replacement
        .map(|replacement| replacement.new_text)
        .unwrap_or_else(|| candidate.insertion.clone());

    CompletionItem {
        label: candidate.display,
        label_details: Some(CompletionItemLabelDetails {
            detail: Some(format!(" · {}", entry.r#ref)),
            description: (!badges.is_empty()).then(|| badges.join(" ")),
        }),
        kind: Some(CompletionItemKind::MODULE),
        detail: Some(vcs_repo_detail(entry, &badges)),
        documentation: vcs_repo_documentation(entry, &badges),
        filter_text: Some(entry.r#ref.clone()),
        sort_text: Some(format!("{index:04}")),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
            range: to_lsp_range(range),
            new_text,
        })),
        additional_text_edits: None,
        tags: None::<Vec<CompletionItemTag>>,
        ..Default::default()
    }
}

fn vcs_repo_detail(entry: &VcsRepoEntry, badges: &[String]) -> String {
    if badges.is_empty() {
        return entry.r#ref.clone();
    }
    format!("{} {}", entry.r#ref, badges.join(" "))
}

fn vcs_repo_documentation(
    entry: &VcsRepoEntry,
    badges: &[String],
) -> Option<Documentation> {
    let mut sections = Vec::new();
    if !entry.description.is_empty() {
        sections.push(entry.description.clone());
    }
    if !badges.is_empty() {
        sections.push(badges.join(" "));
    }
    if sections.is_empty() {
        None
    } else {
        Some(markdown_doc(sections.join("\n\n")))
    }
}

fn vcs_repo_badges(entry: &VcsRepoEntry) -> Vec<String> {
    let mut badges = Vec::new();
    if entry.visibility == "private" {
        badges.push("[private]".to_string());
    }
    if entry.is_fork {
        badges.push("[fork]".to_string());
    }
    if entry.is_archived {
        badges.push("[archived]".to_string());
    }
    badges
}

/// Map the candidate's secondary edits (the prepend/replace-at-start tag edit)
/// to LSP `additionalTextEdits`, returning `None` when there are none.
fn additional_text_edits(edits: Vec<EditorTextEdit>) -> Option<Vec<TextEdit>> {
    if edits.is_empty() {
        return None;
    }
    Some(
        edits
            .into_iter()
            .map(|edit| TextEdit {
                range: to_lsp_range(edit.range),
                new_text: edit.new_text,
            })
            .collect(),
    )
}

pub fn apply_replacement(
    list: CompletionList,
    range: EditorRange,
) -> CompletionList {
    CompletionList {
        candidates: list
            .candidates
            .into_iter()
            .map(|mut candidate| {
                if candidate.replacement.is_none() {
                    candidate.replacement = Some(EditorTextEdit {
                        range,
                        new_text: candidate.insertion.clone(),
                    });
                }
                candidate
            })
            .collect(),
        shared_extension: list.shared_extension,
    }
}

fn to_lsp_position(position: EditorPosition) -> Position {
    Position {
        line: position.line,
        character: position.character,
    }
}

fn to_lsp_diagnostic_severity(
    severity: DiagnosticSeverity,
) -> lsp_types::DiagnosticSeverity {
    match severity {
        DiagnosticSeverity::Error => lsp_types::DiagnosticSeverity::ERROR,
        DiagnosticSeverity::Warning => lsp_types::DiagnosticSeverity::WARNING,
        DiagnosticSeverity::Information => {
            lsp_types::DiagnosticSeverity::INFORMATION
        }
        DiagnosticSeverity::Hint => lsp_types::DiagnosticSeverity::HINT,
    }
}

fn markdown_doc(value: String) -> Documentation {
    Documentation::MarkupContent(MarkupContent {
        kind: MarkupKind::Markdown,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_editor_range_to_lsp_range() {
        let range = EditorRange {
            start: EditorPosition {
                line: 1,
                character: 2,
            },
            end: EditorPosition {
                line: 1,
                character: 5,
            },
        };

        assert_eq!(to_lsp_range(range).start.character, 2);
        assert_eq!(to_lsp_range(range).end.character, 5);
    }

    #[test]
    fn completion_item_uses_replacement_text_edit() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 1,
            },
            end: EditorPosition {
                line: 0,
                character: 3,
            },
        };
        let list = CompletionList {
            candidates: vec![CompletionCandidate {
                display: "#foo".to_string(),
                insertion: "#foo".to_string(),
                detail: None,
                documentation: None,
                is_dir: false,
                name: "foo".to_string(),
                replacement: Some(EditorTextEdit {
                    range,
                    new_text: "#foo".to_string(),
                }),
                additional_edits: Vec::new(),
                kind: String::new(),
                project: String::new(),
                status: String::new(),
            }],
            shared_extension: String::new(),
        };

        let CompletionResponse::Array(items) = completion_response(list, range)
        else {
            panic!("expected array response");
        };
        assert!(items[0].text_edit.is_some());
    }

    #[test]
    fn agent_completion_items_render_distinct_kinds_and_stable_sort_groups() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 2,
            },
        };
        let candidates = [
            ("keyword", "time=", "wait duration"),
            ("tribe", "@ops", "tribe · 2 agents"),
            ("clan", "builders", "clan · 3 members"),
            ("family", "review", "family · 2 members"),
            ("agent", "worker", "RUNNING · sase"),
        ]
        .into_iter()
        .map(|(kind, name, detail)| CompletionCandidate {
            display: name.to_string(),
            insertion: name.to_string(),
            detail: Some(detail.to_string()),
            documentation: None,
            is_dir: false,
            name: name.to_string(),
            replacement: None,
            additional_edits: Vec::new(),
            kind: kind.to_string(),
            project: String::new(),
            status: String::new(),
        })
        .collect();
        let CompletionResponse::Array(items) = agent_completion_response(
            CompletionList {
                candidates,
                shared_extension: String::new(),
            },
            range,
        ) else {
            panic!("expected array response");
        };

        assert_eq!(
            items.iter().map(|item| item.kind).collect::<Vec<_>>(),
            vec![
                Some(CompletionItemKind::KEYWORD),
                Some(CompletionItemKind::ENUM_MEMBER),
                Some(CompletionItemKind::MODULE),
                Some(CompletionItemKind::CLASS),
                Some(CompletionItemKind::VALUE),
            ]
        );
        assert_eq!(items[0].sort_text.as_deref(), Some("0:0000"));
        assert_eq!(items[4].sort_text.as_deref(), Some("4:0004"));
        assert_eq!(
            items[4]
                .label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("agent · RUNNING · sase")
        );
    }

    #[test]
    fn at_reference_items_filter_on_the_typed_text_and_preview_the_match() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 13,
            },
        };
        let context = AtReferenceContextWire {
            stage: AtReferenceStage::Payload,
            candidate_span: (0, 13),
            replacement_span: (0, 13),
            query_span: (9, 13),
            query: "site".to_string(),
            kind: Some("research".to_string()),
            path_query: None,
        };
        let menu = AtReferenceMenuWire {
            rows: vec![AtReferenceRowWire {
                group: AtReferenceGroup::Payload,
                label: "202607/sase_sites.md".to_string(),
                title: "SASE Sites Hub".to_string(),
                insertion: "@research:202607/sase_sites.md".to_string(),
                is_dir: false,
                detail: "research · 3d".to_string(),
                builtin: false,
                label_match: vec![(12, 16)],
                title_match: vec![(5, 9)],
                match_tier: 2,
                body: String::new(),
            }],
            truncated_payloads: 3,
            ..Default::default()
        };

        let response = at_reference_completion_response(menu, &context, range);
        let CompletionResponse::List(list) = response else {
            panic!("expected an incomplete list");
        };
        // `isIncomplete` keeps clients re-requesting per keystroke instead of
        // re-filtering and re-sorting a stale, server-ranked list.
        assert!(list.is_incomplete);
        let item = &list.items[0];
        assert_eq!(item.label, "@research:202607/sase_sites.md");
        assert_eq!(item.filter_text.as_deref(), Some("@research:site"));
        assert_eq!(item.sort_text.as_deref(), Some("0:0000"));
        // A filesystem-backed kind keeps the file icon and takes its own name
        // as the description, rather than the fixed word "file".
        assert_eq!(item.kind, Some(CompletionItemKind::FILE));
        assert_eq!(
            item.label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("research")
        );
        assert_eq!(
            item.label_details
                .as_ref()
                .and_then(|details| details.detail.as_deref()),
            Some(" · SASE Sites Hub")
        );
        assert_eq!(
            item.detail.as_deref(),
            Some("research · 3d · at least 3 additional payloads not shown")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert_eq!(documentation.kind, MarkupKind::Markdown);
        assert_eq!(
            documentation.value,
            "202607/sase_**site**s.md\n\nSASE Sites Hub"
        );
        let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
        else {
            panic!("expected a reference text edit");
        };
        assert_eq!(edit.new_text, "@research:202607/sase_sites.md");
    }

    fn commit_payload_row(body: &str) -> AtReferenceRowWire {
        AtReferenceRowWire {
            group: AtReferenceGroup::Payload,
            label: "sase-core@5143cb981f0a".to_string(),
            title: "fix(stats): expose occupancy".to_string(),
            insertion: "@commit:sase-core@5143cb981f0a".to_string(),
            is_dir: false,
            detail: "sase-core · 2h".to_string(),
            builtin: false,
            label_match: vec![(5, 9)],
            title_match: Vec::new(),
            match_tier: 0,
            body: body.to_string(),
        }
    }

    fn commit_completion_context(query: &str) -> AtReferenceContextWire {
        AtReferenceContextWire {
            stage: AtReferenceStage::Payload,
            candidate_span: (0, 8 + query.len()),
            replacement_span: (0, 8 + query.len()),
            query_span: (8, 8 + query.len()),
            query: query.to_string(),
            kind: Some("commit".to_string()),
            path_query: None,
        }
    }

    #[test]
    fn commit_payload_rows_render_as_references_with_body_in_documentation() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 12,
            },
        };
        let context = commit_completion_context("core");
        let menu = AtReferenceMenuWire {
            rows: vec![commit_payload_row(
                "Expose runner occupancy diagnostics for stats.",
            )],
            ..Default::default()
        };

        let response = at_reference_completion_response(menu, &context, range);
        let CompletionResponse::List(list) = response else {
            panic!("expected an incomplete list");
        };
        let item = &list.items[0];
        // Commits resolve to a canonical SHA, not a filesystem path, so they
        // render with a reference icon rather than a file icon.
        assert_eq!(item.kind, Some(CompletionItemKind::REFERENCE));
        assert_eq!(
            item.label_details
                .as_ref()
                .and_then(|details| details.description.as_deref()),
            Some("commit")
        );
        assert_eq!(
            item.label_details
                .as_ref()
                .and_then(|details| details.detail.as_deref()),
            Some(" · fix(stats): expose occupancy")
        );
        let Some(Documentation::MarkupContent(documentation)) =
            item.documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert_eq!(
            documentation.value,
            "sase-**core**@5143cb981f0a\n\nfix(stats): expose occupancy\n\n\
             ```\nExpose runner occupancy diagnostics for stats.\n```"
        );
    }

    #[test]
    fn commit_documentation_omits_the_body_block_when_empty() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 12,
            },
        };
        let context = commit_completion_context("core");
        let menu = AtReferenceMenuWire {
            rows: vec![commit_payload_row("")],
            ..Default::default()
        };

        let response = at_reference_completion_response(menu, &context, range);
        let CompletionResponse::List(list) = response else {
            panic!("expected an incomplete list");
        };
        let Some(Documentation::MarkupContent(documentation)) =
            list.items[0].documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert!(!documentation.value.contains("```"));
    }

    #[test]
    fn commit_documentation_truncates_a_long_body_to_a_bounded_line_count() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 12,
            },
        };
        let context = commit_completion_context("core");
        let long_body = (1..=50)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let menu = AtReferenceMenuWire {
            rows: vec![commit_payload_row(&long_body)],
            ..Default::default()
        };

        let response = at_reference_completion_response(menu, &context, range);
        let CompletionResponse::List(list) = response else {
            panic!("expected an incomplete list");
        };
        let Some(Documentation::MarkupContent(documentation)) =
            list.items[0].documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert!(documentation.value.contains("line 1\n"));
        assert!(!documentation.value.contains("line 50"));
        assert!(documentation.value.contains('…'));
        assert!(
            documentation.value.lines().count() < long_body.lines().count()
        );
    }

    #[test]
    fn at_reference_kind_stage_items_filter_on_the_bare_typed_word() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 5,
            },
        };
        let context = AtReferenceContextWire {
            stage: AtReferenceStage::Kind,
            candidate_span: (0, 5),
            replacement_span: (0, 5),
            query_span: (1, 5),
            query: "rsch".to_string(),
            kind: None,
            path_query: None,
        };
        let menu = AtReferenceMenuWire {
            rows: vec![AtReferenceRowWire {
                group: AtReferenceGroup::Artifact,
                label: "research".to_string(),
                title: String::new(),
                insertion: "@research:".to_string(),
                is_dir: false,
                detail: String::new(),
                builtin: true,
                label_match: vec![(0, 1), (2, 3), (4, 6)],
                title_match: Vec::new(),
                match_tier: 3,
                body: String::new(),
            }],
            ..Default::default()
        };

        let items =
            match at_reference_completion_response(menu, &context, range) {
                CompletionResponse::List(list) => list.items,
                CompletionResponse::Array(items) => items,
            };
        assert_eq!(items[0].filter_text.as_deref(), Some("@rsch"));
        let Some(Documentation::MarkupContent(documentation)) =
            items[0].documentation.as_ref()
        else {
            panic!("expected markdown documentation");
        };
        assert_eq!(documentation.value, "**r**e**s**e**ar**ch");
    }

    #[test]
    fn converts_sase_snippet_template_to_lsp_snippet_syntax() {
        assert_eq!(
            sase_template_to_lsp_snippet(r"cost $5 $1 \ path } $0"),
            r"cost $5 $1 \\ path \} $0"
        );
        assert_eq!(sase_template_to_lsp_snippet("$foo"), r"\$foo");
    }

    #[test]
    fn placeholder_tabstop_snippet_retriggers_completion() {
        let range = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 3,
            },
        };
        let item = sase_snippet_completion_item(
            "cbi".to_string(),
            "`<$1>`$0".to_string(),
            None,
            None,
            range,
        );
        assert_eq!(
            item.command
                .as_ref()
                .map(|command| command.command.as_str()),
            Some("editor.action.triggerSuggest")
        );

        let ordinary = sase_snippet_completion_item(
            "plain".to_string(),
            "$1 body $0".to_string(),
            None,
            None,
            range,
        );
        assert!(ordinary.command.is_none());
    }
}
