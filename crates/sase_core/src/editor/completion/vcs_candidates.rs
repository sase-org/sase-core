//! `vcs:` repository, ref, and project completions, including trigger
//! detection and selection application.

use crate::editor::token::{vcs_project_trigger_token, DocumentSnapshot};
use crate::editor::wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, EditorPosition, EditorTextEdit, TokenInfo,
    VcsNamespaceEntry, VcsProjectEntry, VcsRefTrigger, VcsRepoEntry,
    VcsRepoTrigger,
};
use crate::project_tag::{
    is_tag_name, project_tag_selection_edits, ProjectTagSelectionEdit,
    ProjectTagTargetWire,
};

pub fn build_vcs_repo_completion_candidates(
    document: &DocumentSnapshot,
    context: &CompletionContext,
    entries: &[VcsRepoEntry],
) -> CompletionList {
    let Some(trigger) = context.vcs_repo.as_ref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let Some(replacement_range) =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };

    let candidates = entries
        .iter()
        .map(|entry| {
            let new_text = vcs_repo_replacement_text(
                document.text(),
                trigger,
                &entry.r#ref,
            );
            CompletionCandidate {
                display: entry.name.clone(),
                insertion: entry.r#ref.clone(),
                detail: (!entry.visibility.is_empty())
                    .then(|| entry.visibility.clone()),
                documentation: (!entry.description.is_empty())
                    .then(|| entry.description.clone()),
                is_dir: false,
                name: entry.name.clone(),
                replacement: Some(EditorTextEdit {
                    range: replacement_range,
                    new_text,
                }),
                additional_edits: Vec::new(),
                kind: "repo".to_string(),
                project: trigger.namespace.clone(),
                status: entry.visibility.clone(),
            }
        })
        .collect();

    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}
pub fn apply_vcs_repo_selection(
    text: &str,
    trigger: &VcsRepoTrigger,
    selected_ref: &str,
) -> String {
    let replacement = vcs_repo_replacement_text(text, trigger, selected_ref);
    format!(
        "{}{}{}",
        &text[..trigger.ref_start],
        replacement,
        &text[trigger.ref_end..]
    )
}
fn vcs_repo_replacement_text(
    text: &str,
    trigger: &VcsRepoTrigger,
    selected_ref: &str,
) -> String {
    let after = &text[trigger.ref_end..];
    if trigger.separator == "(" {
        let suffix = if after.starts_with(')') { "" } else { ")" };
        return format!("{selected_ref}{suffix}");
    }

    let suffix = if after.starts_with([' ', '\t'])
        || after.starts_with('\r') && after != "\r" && after != "\r\n"
        || after.starts_with('\n') && after != "\n"
    {
        ""
    } else {
        " "
    };
    format!("{selected_ref}{suffix}")
}
pub fn detect_vcs_repo_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut names: Vec<&str> = known_workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text[prev..].chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    if start >= text.len() || text.get(start..start + 1) != Some("#") {
        return None;
    }

    let workflow = names
        .iter()
        .copied()
        .find(|name| text[start + 1..].starts_with(name))?;
    let mut pos = start + 1 + workflow.len();
    if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
        pos += 2;
    }
    let separator = match text[pos..].chars().next()? {
        ':' => ":",
        '(' => "(",
        _ => return None,
    };
    let ref_start = pos + 1;
    if cursor < ref_start {
        return None;
    }
    if separator == "(" && text[ref_start..cursor].contains(')') {
        return None;
    }

    let (ref_end, token_end) = find_vcs_repo_ref_end(text, cursor, separator)?;
    if cursor > ref_end {
        return None;
    }
    let ref_before_cursor = &text[ref_start..cursor];
    let slash_offset = ref_before_cursor.rfind('/')?;
    let full_ref = &text[ref_start..ref_end];
    if full_ref.contains("://") {
        return None;
    }

    let namespace = &ref_before_cursor[..slash_offset];
    if namespace.is_empty()
        || namespace.starts_with('~')
        || namespace.starts_with('.')
    {
        return None;
    }

    let query_start = ref_start + slash_offset + 1;
    let trigger = VcsRepoTrigger {
        start,
        end: token_end,
        workflow: workflow.to_string(),
        separator: separator.to_string(),
        ref_start,
        ref_end,
        namespace: namespace.to_string(),
        query: ref_before_cursor[slash_offset + 1..].to_string(),
        namespace_span: (ref_start, ref_start + slash_offset),
        query_span: (query_start, cursor),
    };
    let token_range = document.byte_range_to_range(start, token_end)?;
    let replacement_range =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsRepo,
        token: Some(TokenInfo {
            text: text[start..token_end].to_string(),
            range: token_range,
            byte_start: start,
            byte_end: token_end,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: Some(trigger),
        vcs_ref: None,
        artifact_ref: None,
        replacement_range,
    })
}
fn find_vcs_repo_ref_end(
    text: &str,
    cursor: usize,
    separator: &str,
) -> Option<(usize, usize)> {
    let mut end = cursor;
    if separator == "(" {
        while end < text.len() {
            let ch = text[end..].chars().next()?;
            if ch.is_whitespace() || ch == ')' {
                break;
            }
            end += ch.len_utf8();
        }
        let token_end = if end < text.len()
            && text[end..].chars().next().is_some_and(|ch| ch == ')')
        {
            end + 1
        } else {
            end
        };
        return Some((end, token_end));
    }

    while end < text.len() {
        let ch = text[end..].chars().next()?;
        if ch.is_whitespace() {
            break;
        }
        end += ch.len_utf8();
    }
    Some((end, end))
}
pub fn build_vcs_ref_completion_candidates(
    document: &DocumentSnapshot,
    context: &CompletionContext,
    entries: &[VcsProjectEntry],
    namespaces: &[VcsNamespaceEntry],
) -> CompletionList {
    let Some(trigger) = context.vcs_ref.as_ref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let Some(replacement_range) =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };

    let query = trigger.query.to_lowercase();
    let mut candidates = Vec::new();
    for include_patches in [false, true] {
        for entry in entries {
            let entry_kind = vcs_project_entry_kind(entry);
            let is_patch = entry_kind == "patch";
            if is_patch != include_patches
                || entry.vcs_prefix != trigger.workflow
                || !vcs_ref_project_matches(entry, &query)
            {
                continue;
            }

            let new_text = vcs_ref_replacement_text(
                document.text(),
                trigger,
                &entry.name,
                false,
            );
            candidates.push(CompletionCandidate {
                display: entry.name.clone(),
                insertion: entry.name.clone(),
                detail: Some(format!(
                    "{} · {}",
                    entry.provider_display, entry.display_tag
                )),
                documentation: (!entry.description.is_empty())
                    .then(|| entry.description.clone()),
                is_dir: false,
                name: entry.name.clone(),
                replacement: Some(EditorTextEdit {
                    range: replacement_range,
                    new_text,
                }),
                additional_edits: Vec::new(),
                kind: entry_kind.to_string(),
                project: entry.project.clone(),
                status: entry.status.clone(),
            });
        }
    }

    for namespace in namespaces {
        if !prefix_matches(&namespace.name, &query) {
            continue;
        }
        let insertion = vcs_ref_namespace_insertion(&namespace.name);
        let new_text = vcs_ref_replacement_text(
            document.text(),
            trigger,
            &insertion,
            true,
        );
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion,
            detail: (!namespace.description.is_empty())
                .then(|| namespace.description.clone()),
            documentation: None,
            is_dir: true,
            name: namespace.name.clone(),
            replacement: Some(EditorTextEdit {
                range: replacement_range,
                new_text,
            }),
            additional_edits: Vec::new(),
            kind: "namespace".to_string(),
            project: trigger.workflow.clone(),
            status: if namespace.kind_label.is_empty() {
                "org".to_string()
            } else {
                namespace.kind_label.clone()
            },
        });
    }

    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}
fn vcs_project_entry_kind(entry: &VcsProjectEntry) -> &str {
    let raw_kind = if entry.entry_kind.is_empty() {
        entry.kind.as_str()
    } else {
        entry.entry_kind.as_str()
    };
    match raw_kind {
        // Legacy completion metadata maps to the canonical patch kind.
        "changespec" => "patch",
        "patch" => "patch",
        _ => "project",
    }
}
pub fn apply_vcs_ref_selection(
    text: &str,
    trigger: &VcsRefTrigger,
    selected_ref: &str,
    chain: bool,
) -> String {
    let replacement =
        vcs_ref_replacement_text(text, trigger, selected_ref, chain);
    format!(
        "{}{}{}",
        &text[..trigger.ref_start],
        replacement,
        &text[trigger.ref_end..]
    )
}
fn vcs_ref_replacement_text(
    text: &str,
    trigger: &VcsRefTrigger,
    selected_ref: &str,
    chain: bool,
) -> String {
    let selected_ref = if chain {
        vcs_ref_namespace_insertion(selected_ref)
    } else {
        selected_ref.to_string()
    };
    if chain {
        return selected_ref;
    }

    let after = &text[trigger.ref_end..];
    if trigger.separator == "(" {
        let suffix = if after.starts_with(')') { "" } else { ")" };
        return format!("{selected_ref}{suffix}");
    }

    let suffix = if after.starts_with([' ', '\t'])
        || after.starts_with('\r') && after != "\r" && after != "\r\n"
        || after.starts_with('\n') && after != "\n"
    {
        ""
    } else {
        " "
    };
    format!("{selected_ref}{suffix}")
}
fn vcs_ref_namespace_insertion(name: &str) -> String {
    format!("{}/", name.trim_end_matches('/'))
}
fn vcs_ref_project_matches(entry: &VcsProjectEntry, query: &str) -> bool {
    prefix_matches(&entry.name, query)
        || entry
            .aliases
            .iter()
            .any(|alias| prefix_matches(alias, query))
}
fn prefix_matches(value: &str, query_lower: &str) -> bool {
    query_lower.is_empty() || value.to_lowercase().starts_with(query_lower)
}
pub fn detect_vcs_ref_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut names: Vec<&str> = known_workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text[prev..].chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    if start >= text.len() || text.get(start..start + 1) != Some("#") {
        return None;
    }

    let workflow = names
        .iter()
        .copied()
        .find(|name| text[start + 1..].starts_with(name))?;
    let mut pos = start + 1 + workflow.len();
    if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
        pos += 2;
    }
    let separator = match text[pos..].chars().next()? {
        ':' => ":",
        '(' => "(",
        _ => return None,
    };
    let ref_start = pos + 1;
    if cursor < ref_start {
        return None;
    }
    let ref_before_cursor = &text[ref_start..cursor];
    if ref_before_cursor.contains(')') || ref_before_cursor.contains('/') {
        return None;
    }

    let (ref_end, token_end) = find_vcs_repo_ref_end(text, cursor, separator)?;
    if cursor > ref_end {
        return None;
    }
    let full_ref = &text[ref_start..ref_end];
    if full_ref.contains('/')
        || full_ref.contains("://")
        || full_ref.starts_with(['~', '.'])
        || full_ref.contains(')')
    {
        return None;
    }

    let trigger = VcsRefTrigger {
        start,
        end: token_end,
        workflow: workflow.to_string(),
        separator: separator.to_string(),
        ref_start,
        ref_end,
        query: ref_before_cursor.to_string(),
        query_span: (ref_start, cursor),
    };
    let token_range = document.byte_range_to_range(start, token_end)?;
    let replacement_range =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsRef,
        token: Some(TokenInfo {
            text: text[start..token_end].to_string(),
            range: token_range,
            byte_start: start,
            byte_end: token_end,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: Some(trigger),
        artifact_ref: None,
        replacement_range,
    })
}
fn previous_char_boundary(text: &str, byte_idx: usize) -> Option<usize> {
    text.get(..byte_idx)?
        .char_indices()
        .last()
        .map(|(idx, _)| idx)
}
/// Build `vcs_project` completion candidates for a `+query` trigger token.
///
/// Each candidate applies the shared project-tag accept algorithm (see
/// [`crate::project_tag::apply_project_tag_selection`]): the primary edit
/// replaces the typed `+query` token in place with the row's insertion — a
/// project row inserts `+<name> ` (or `#<workflow>:<name> ` when the name is
/// not in the tag grammar), a PR row inserts its `#` spelling — while
/// `additional_edits` delete every other workspace target in the trigger's
/// `---` segment. The edits never overlap.
///
/// `entries` drive the visible rows (enabled projects plus patches);
/// `targets` is the catalog's tag-resolution set (every non-sibling project
/// plus `home`, including disabled rows) and drives the other-target
/// deletions, so the LSP and the TUI accept remove the same resolved tags.
pub fn build_vcs_project_completion_candidates_with_targets(
    token: &TokenInfo,
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[VcsProjectEntry],
    targets: &[ProjectTagTargetWire],
    known_workflow_names: &[String],
) -> CompletionList {
    let text = document.text();
    let t0 = token.byte_start;
    let t1 = token.byte_end;
    let cursor = document
        .position_to_byte_offset(position)
        .unwrap_or(t1)
        .clamp(t0, t1);
    // Filter query is the text after the plus up to the cursor (empty for a
    // bare `+`), matching the Python `find_vcs_project_trigger`.
    let query = text.get(t0 + 1..cursor).unwrap_or("").to_lowercase();

    let mut candidates = Vec::new();
    for entry in entries {
        let matches_query = query.is_empty()
            || entry.name.to_lowercase().starts_with(&query)
            || entry
                .aliases
                .iter()
                .any(|alias| alias.to_lowercase().starts_with(&query));
        if !matches_query {
            continue;
        }

        let insertion = completion_insertion(entry);
        let (primary, additional) = project_tag_selection_edits(
            text,
            (t0, t1),
            &insertion,
            known_workflow_names,
            targets,
        );
        let Some(primary) = selection_edit_to_text_edit(document, &primary)
        else {
            continue;
        };
        let additional: Option<Vec<EditorTextEdit>> = additional
            .iter()
            .map(|edit| selection_edit_to_text_edit(document, edit))
            .collect();
        let Some(additional) = additional else {
            continue;
        };

        candidates.push(CompletionCandidate {
            display: entry.name.clone(),
            insertion: insertion.clone(),
            detail: Some(format!(
                "{} · {}",
                entry.provider_display, entry.display_tag
            )),
            documentation: (!entry.description.is_empty())
                .then(|| entry.description.clone()),
            is_dir: false,
            name: entry.name.clone(),
            replacement: Some(primary),
            additional_edits: additional,
            kind: vcs_project_entry_kind(entry).to_string(),
            project: entry.project.clone(),
            status: entry.status.clone(),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}
/// Tag-resolution targets behind the `+` menu, derived from the visible
/// entries (project rows only). Patch rows are accepted through their `#`
/// spelling, never as tags. The LSP falls back to this set for pre-v5
/// catalogs, which carry no `project_tags`.
pub fn vcs_project_entry_targets(
    entries: &[VcsProjectEntry],
) -> Vec<ProjectTagTargetWire> {
    entries
        .iter()
        .filter(|entry| vcs_project_entry_kind(entry) != "patch")
        .map(|entry| ProjectTagTargetWire {
            key: if entry.key.is_empty() {
                entry.name.clone()
            } else {
                entry.key.clone()
            },
            name: entry.name.clone(),
            aliases: entry.aliases.clone(),
            workflow_type: Some(entry.vcs_prefix.clone()),
            state: None,
            workspace_dir: None,
        })
        .collect()
}
/// Row insertion text: a project row inserts its tag (`+<name> `), falling
/// back to `#<workflow>:<name> ` when the name is not in the tag grammar; a
/// PR row inserts its `#` spelling.
fn completion_insertion(entry: &VcsProjectEntry) -> String {
    if vcs_project_entry_kind(entry) == "patch" {
        return format!("{} ", entry.display_tag);
    }
    if !entry.tag.is_empty() {
        return format!("{} ", entry.tag);
    }
    if is_tag_name(&entry.name) {
        return format!("+{} ", entry.name);
    }
    format!("#{}:{} ", entry.vcs_prefix, entry.name)
}
fn selection_edit_to_text_edit(
    document: &DocumentSnapshot,
    edit: &ProjectTagSelectionEdit,
) -> Option<EditorTextEdit> {
    Some(EditorTextEdit {
        range: document.byte_range_to_range(edit.start, edit.end)?,
        new_text: edit.new_text.clone(),
    })
}
pub(crate) fn detect_vcs_project_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<CompletionContext> {
    let token = vcs_project_trigger_token(document, position)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsProject,
        replacement_range: token.range,
        token: Some(token),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
    })
}
