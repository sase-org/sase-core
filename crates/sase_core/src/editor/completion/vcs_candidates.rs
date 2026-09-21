//! `vcs:` repository, ref, and project completions, including trigger
//! detection, selection application, and project byte-edit computation.

use crate::editor::token::{vcs_project_trigger_token, DocumentSnapshot};
use crate::editor::wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, EditorPosition, EditorTextEdit, TokenInfo,
    VcsNamespaceEntry, VcsProjectEntry, VcsRefTrigger, VcsRepoEntry,
    VcsRepoTrigger,
};
use regex::Regex;
use std::sync::OnceLock;

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
/// Each candidate expands the selected project into the prompt via the
/// canonical VCS-tag expansion algorithm (see [`apply_vcs_project_selection`]),
/// represented as a primary edit that consumes the trigger span plus
/// `additional_edits` that prepend/replace the VCS workflow tag at the start of
/// the document. When those edits would overlap they are merged into a single
/// primary edit. The output is byte-for-byte identical to the Python
/// `apply_vcs_project_selection` for the shared golden test vectors.
pub fn build_vcs_project_completion_candidates(
    token: &TokenInfo,
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[VcsProjectEntry],
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

    let replace_re = vcs_replace_regex(known_workflow_names);
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

        let edits = vcs_project_byte_edits(
            text,
            t0,
            t1,
            &entry.display_tag,
            &replace_re,
        );
        let Some(primary) = byte_edit_to_text_edit(document, &edits.primary)
        else {
            continue;
        };
        let additional: Option<Vec<EditorTextEdit>> = edits
            .additional
            .iter()
            .map(|edit| byte_edit_to_text_edit(document, edit))
            .collect();
        let Some(additional) = additional else {
            continue;
        };

        candidates.push(CompletionCandidate {
            display: entry.name.clone(),
            insertion: entry.display_tag.clone(),
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
/// Apply a selected project's VCS tag to `text`, returning the new full text.
///
/// This is the canonical expansion algorithm (the cross-language parity
/// contract). It mirrors the Python `apply_vcs_project_selection`: remove the
/// `[t0, t1)` trigger token, collapse one adjacent space, then either replace
/// every line-start VCS workflow tag with `display_tag` or -- when none exist --
/// prepend `display_tag` after any leading frontmatter / whitespace /
/// `%directive` tokens.
pub fn apply_vcs_project_selection(
    text: &str,
    t0: usize,
    t1: usize,
    display_tag: &str,
    replace_re: &Regex,
) -> String {
    let (d0, d1) = strip_trigger_region(text, t0, t1);
    let base = format!("{}{}", &text[..d0], &text[d1..]);
    let tag_with_space = format!("{display_tag} ");

    if replace_re.is_match(&base) {
        return replace_re
            .replace_all(&base, |caps: &regex::Captures| {
                let prefix = caps.get(1).map_or("", |m| m.as_str());
                format!("{prefix}{tag_with_space}")
            })
            .into_owned();
    }

    let offset = vcs_prepend_offset(&base);
    format!("{}{}{}", &base[..offset], tag_with_space, &base[offset..])
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
/// A single byte-range edit: replace `text[start..end]` with `new_text`.
pub(crate) struct VcsByteEdit {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) new_text: String,
}
pub(crate) struct VcsProjectByteEdits {
    pub(crate) primary: VcsByteEdit,
    pub(crate) additional: Vec<VcsByteEdit>,
}
/// Compute the primary + additional edits for one project selection.
///
/// Edits are expressed in original-document byte coordinates and are guaranteed
/// not to overlap (overlapping cases are either merged into the primary edit or,
/// defensively, collapsed into a single full-document replacement).
pub(crate) fn vcs_project_byte_edits(
    text: &str,
    trigger_start: usize,
    trigger_end: usize,
    display_tag: &str,
    replace_re: &Regex,
) -> VcsProjectByteEdits {
    let (d0, d1) = strip_trigger_region(text, trigger_start, trigger_end);
    let base = format!("{}{}", &text[..d0], &text[d1..]);
    let gap = d1 - d0;
    // Map a `base` byte offset back to original-document coordinates.
    let to_original = |p: usize| if p <= d0 { p } else { p + gap };
    let tag_with_space = format!("{display_tag} ");

    let tag_matches: Vec<(usize, usize, usize)> = replace_re
        .captures_iter(&base)
        .map(|caps| {
            let whole = caps.get(0).expect("group 0 always present");
            let prefix_len = caps.get(1).map_or(0, |m| m.len());
            (whole.start(), whole.end(), prefix_len)
        })
        .collect();

    let (primary, additional) = if tag_matches.is_empty() {
        // Prepend branch: insert the tag at the frontmatter/directive-aware
        // offset.
        let insert_at = to_original(vcs_prepend_offset(&base));
        if insert_at == d0 {
            // The prepend point coincides with the trigger-deletion start;
            // merge into one edit (the deleted region collapses to the tag).
            (
                VcsByteEdit {
                    start: d0,
                    end: d1,
                    new_text: tag_with_space,
                },
                Vec::new(),
            )
        } else {
            (
                VcsByteEdit {
                    start: d0,
                    end: d1,
                    new_text: String::new(),
                },
                vec![VcsByteEdit {
                    start: insert_at,
                    end: insert_at,
                    new_text: tag_with_space,
                }],
            )
        }
    } else {
        // Replace branch: rewrite every line-start tag, preserving any leading
        // `%directive` prefix captured in group 1.
        let additional = tag_matches
            .into_iter()
            .map(|(match_start, match_end, prefix_len)| {
                let prefix =
                    base[match_start..match_start + prefix_len].to_string();
                VcsByteEdit {
                    start: to_original(match_start),
                    end: to_original(match_end),
                    new_text: format!("{prefix}{tag_with_space}"),
                }
            })
            .collect();
        (
            VcsByteEdit {
                start: d0,
                end: d1,
                new_text: String::new(),
            },
            additional,
        )
    };

    // Defensive guard: if the edits would overlap (no realistic input produces
    // this, but LSP forbids overlapping ranges), fall back to a single
    // full-document replacement with the canonical result.
    if vcs_edits_conflict(&primary, &additional) {
        let canonical = apply_vcs_project_selection(
            text,
            trigger_start,
            trigger_end,
            display_tag,
            replace_re,
        );
        return VcsProjectByteEdits {
            primary: VcsByteEdit {
                start: 0,
                end: text.len(),
                new_text: canonical,
            },
            additional: Vec::new(),
        };
    }

    VcsProjectByteEdits {
        primary,
        additional,
    }
}
pub(crate) fn vcs_edits_conflict(
    primary: &VcsByteEdit,
    additional: &[VcsByteEdit],
) -> bool {
    let mut spans: Vec<(usize, usize)> =
        std::iter::once((primary.start, primary.end))
            .chain(additional.iter().map(|edit| (edit.start, edit.end)))
            .collect();
    spans.sort_by_key(|&(start, end)| (start, end));
    spans.windows(2).any(|pair| pair[1].0 < pair[0].1)
}
fn byte_edit_to_text_edit(
    document: &DocumentSnapshot,
    edit: &VcsByteEdit,
) -> Option<EditorTextEdit> {
    Some(EditorTextEdit {
        range: document.byte_range_to_range(edit.start, edit.end)?,
        new_text: edit.new_text.clone(),
    })
}
/// Remove the `[t0, t1)` trigger span, collapsing one adjacent space, and
/// return the resulting deletion region `[d0, d1)`. Mirrors the Python
/// `_strip_trigger_token`.
fn strip_trigger_region(text: &str, t0: usize, t1: usize) -> (usize, usize) {
    let before = &text[..t0];
    let after = &text[t1..];
    let before_space = before.ends_with(' ');
    let after_space = after.starts_with(' ');

    if before_space && after_space {
        // Token sat between two spaces; drop the following one.
        (t0, t1 + 1)
    } else if before_space
        && (after.is_empty() || after.starts_with(['\r', '\n']))
    {
        // A trailing space would be orphaned at end of line/prompt.
        (t0 - 1, t1)
    } else if after_space
        && (before.is_empty() || before.ends_with(['\r', '\n']))
    {
        // A leading space would be orphaned at start of line/prompt.
        (t0, t1 + 1)
    } else {
        (t0, t1)
    }
}
/// Where a leading VCS workflow tag should be inserted: after any leading YAML
/// frontmatter block, leading horizontal whitespace, and leading `%directive`
/// tokens.
/// Mirrors the Python `find_vcs_workflow_tag_prepend_offset`.
pub(crate) fn vcs_prepend_offset(text: &str) -> usize {
    let frontmatter_len = frontmatter_block_len(text);
    let body = &text[frontmatter_len..];
    let leading_ws = body
        .char_indices()
        .find(|(_, ch)| !ch.is_whitespace() || matches!(ch, '\n' | '\r'))
        .map_or(body.len(), |(idx, _)| idx);
    let after_ws = &body[leading_ws..];
    let directive_len = directive_prefix_regex()
        .find(after_ws)
        .map_or(0, |m| m.end());
    frontmatter_len + leading_ws + directive_len
}
/// Byte length of a leading YAML frontmatter block (`---` ... `---`), or 0 when
/// `text` does not begin with one. Mirrors the Python `_split_frontmatter_block`.
fn frontmatter_block_len(text: &str) -> usize {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let Some(first) = lines.first() else {
        return 0;
    };
    if first.trim() != "---" {
        return 0;
    }
    let mut consumed = first.len();
    for line in &lines[1..] {
        consumed += line.len();
        if line.trim() == "---" {
            return consumed;
        }
    }
    0
}
fn directive_prefix_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^(?:%\S+[\s]+)+").unwrap())
}
/// Build the multiline pattern matching VCS workflow tags at the start of any
/// line, mirroring the Python `_get_vcs_replace_pattern`. Group 1 captures any
/// leading `%directive` prefix to preserve it during replacement.
pub(crate) fn vcs_replace_regex(known_workflow_names: &[String]) -> Regex {
    let mut names: Vec<&str> =
        known_workflow_names.iter().map(String::as_str).collect();
    names.sort_unstable();
    let alternation = names
        .iter()
        .map(|name| regex::escape(name))
        .collect::<Vec<_>>()
        .join("|");
    // The boundary after a tag is whitespace OR end-of-input. `\s` is tried
    // first, so any actual whitespace (including a newline) is consumed and
    // replaced exactly as before; `$` only wins at true EOF, letting a
    // line-start tag with no trailing whitespace (e.g. `#gh:sase` alone) still
    // be replaced rather than treated as absent.
    let pattern = format!(
        r"(?m)^((?:%\S+[\s]+)*)#(?:{alternation})(?:!!|\?\?)?(?:\([^)]*\)|\+|[_:][^\s]*|)(?:\s|$)"
    );
    Regex::new(&pattern).expect("valid vcs replace pattern")
}
