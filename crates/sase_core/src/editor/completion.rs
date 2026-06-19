use std::collections::BTreeSet;

use regex::Regex;
use std::sync::OnceLock;

use crate::{EditorSnippetEntryWire, EditorXpromptCatalogEntryWire};

use super::directive::canonical_directive_name;
use super::token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_snippet_trigger_token, is_xprompt_like_token, vcs_project_trigger_token,
    DocumentSnapshot,
};
use super::wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, EditorPosition, EditorRange, EditorTextEdit, TokenInfo,
    VcsProjectEntry, XpromptAssistEntry, XpromptInputHint,
};

pub fn assist_entries_from_catalog(
    entries: &[EditorXpromptCatalogEntryWire],
) -> Vec<XpromptAssistEntry> {
    entries
        .iter()
        .map(|entry| {
            let reference_prefix =
                entry.reference_prefix.as_deref().unwrap_or("#").to_string();
            let insertion = entry
                .insertion
                .clone()
                .unwrap_or_else(|| format!("{reference_prefix}{}", entry.name));
            XpromptAssistEntry {
                name: entry.name.clone(),
                display_label: entry.display_label.clone(),
                insertion,
                reference_prefix,
                kind: entry.kind.clone(),
                source_bucket: entry.source_bucket.clone(),
                project: entry.project.clone(),
                tags: entry.tags.clone(),
                input_signature: entry.input_signature.clone(),
                inputs: entry
                    .inputs
                    .iter()
                    .map(|input| XpromptInputHint {
                        name: input.name.clone(),
                        r#type: input.r#type.clone(),
                        description: input.description.clone(),
                        required: input.required,
                        default_display: input.default_display.clone(),
                        position: input.position,
                    })
                    .collect(),
                content_preview: entry.content_preview.clone(),
                description: entry.description.clone(),
                source_path_display: entry.source_path_display.clone(),
                definition_path: entry.definition_path.clone(),
                definition_range: entry.definition_range,
                is_skill: entry.is_skill,
            }
        })
        .collect()
}

pub fn classify_completion_context(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<CompletionContext> {
    if let Some(context) =
        detect_xprompt_arg_completion_at_position(document, position, entries)
    {
        return Some(context);
    }
    if let Some(context) =
        detect_directive_context_at_position(document, position)
    {
        return Some(context);
    }
    if let Some(context) =
        detect_vcs_project_context_at_position(document, position)
    {
        return Some(context);
    }

    let token = extract_token_at_position(document, position);
    match token {
        None => {
            let byte = document.position_to_byte_offset(position)?;
            if byte > 0
                && document.text()[..byte].chars().next_back() == Some('+')
            {
                return None;
            }
            Some(CompletionContext {
                kind: CompletionContextKind::FileHistory,
                token: None,
                active_xprompt: None,
                active_input: None,
                directive_name: None,
                replacement_range: document.byte_range_to_range(byte, byte)?,
            })
        }
        Some(token) if is_xprompt_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::Xprompt, token))
        }
        Some(token) if is_slash_skill_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::SlashSkill, token))
        }
        Some(token) if is_path_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::FilePath, token))
        }
        Some(token) if is_snippet_trigger_token(&token.text) => Some(
            context_for_token(CompletionContextKind::SnippetTrigger, token),
        ),
        _ => None,
    }
}

pub fn build_xprompt_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[XpromptAssistEntry],
) -> CompletionList {
    let slash_skill = token.starts_with('/');
    let standalone_only = token.starts_with("#!");
    let partial = if slash_skill {
        token.strip_prefix('/').unwrap_or_default()
    } else if standalone_only {
        token.strip_prefix("#!").unwrap_or_default()
    } else {
        token.strip_prefix('#').unwrap_or(token)
    };
    let partial_lower = partial.to_lowercase();
    let mut candidates = Vec::new();

    for entry in entries {
        if slash_skill && !entry.is_skill {
            continue;
        }
        if standalone_only && entry.reference_prefix != "#!" {
            continue;
        }
        if !entry.name.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        let insertion = if slash_skill {
            format!("/{}", entry.name)
        } else {
            entry.insertion.clone()
        };
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion: insertion.clone(),
            detail: entry
                .input_signature
                .clone()
                .or_else(|| entry.kind.clone()),
            documentation: entry
                .description
                .clone()
                .or_else(|| entry.content_preview.clone()),
            is_dir: false,
            name: entry.name.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    candidates.sort_by_key(|candidate| candidate.name.to_lowercase());
    CompletionList {
        shared_extension: shared_extension(&candidates, partial),
        candidates,
    }
}

pub fn build_xprompt_arg_name_candidates(
    entry: &XpromptAssistEntry,
    used_arg_names: &BTreeSet<String>,
    token: &str,
    replacement_range: Option<EditorRange>,
) -> CompletionList {
    let partial = token.to_lowercase();
    let mut candidates = Vec::new();
    for input in &entry.inputs {
        if used_arg_names.contains(&input.name) {
            continue;
        }
        if !input.name.to_lowercase().starts_with(&partial) {
            continue;
        }
        let insertion = format!("{}=", input.name);
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion: insertion.clone(),
            detail: Some(input_label(input)),
            documentation: input_documentation(input),
            is_dir: false,
            name: input.name.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn build_snippet_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[EditorSnippetEntryWire],
) -> CompletionList {
    let partial_lower = token.to_lowercase();
    let mut candidates = Vec::new();
    for entry in entries {
        if !entry.trigger.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        candidates.push(CompletionCandidate {
            display: entry.trigger.clone(),
            insertion: entry.template.clone(),
            detail: Some(entry.source.clone()),
            documentation: snippet_documentation(entry),
            is_dir: false,
            name: entry.trigger.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: entry.template.clone(),
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    candidates.sort_by_key(|candidate| candidate.name.to_lowercase());
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}

// --- vcs_project (`#+`) completion ----------------------------------------

/// Build `vcs_project` completion candidates for a `#+query` or BOF `+query`
/// trigger token.
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
    // Filter query is the text after the trigger prefix up to the cursor (empty
    // for a bare `#+`/`+`), matching the Python `find_vcs_project_trigger`. The
    // prefix is `#+` (offset 2) for a hash-plus token, or `+` (offset 1) for a
    // BOF bare-plus token.
    let prefix_len = if token.text.starts_with("#+") { 2 } else { 1 };
    let query = text
        .get(t0 + prefix_len..cursor)
        .unwrap_or("")
        .to_lowercase();

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
            kind: entry.kind.clone(),
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

fn detect_vcs_project_context_at_position(
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
    })
}

/// A single byte-range edit: replace `text[start..end]` with `new_text`.
struct VcsByteEdit {
    start: usize,
    end: usize,
    new_text: String,
}

struct VcsProjectByteEdits {
    primary: VcsByteEdit,
    additional: Vec<VcsByteEdit>,
}

/// Compute the primary + additional edits for one project selection.
///
/// Edits are expressed in original-document byte coordinates and are guaranteed
/// not to overlap (overlapping cases are either merged into the primary edit or,
/// defensively, collapsed into a single full-document replacement).
fn vcs_project_byte_edits(
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

fn vcs_edits_conflict(
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
fn vcs_prepend_offset(text: &str) -> usize {
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
fn vcs_replace_regex(known_workflow_names: &[String]) -> Regex {
    let mut names: Vec<&str> =
        known_workflow_names.iter().map(String::as_str).collect();
    names.sort_unstable();
    let alternation = names
        .iter()
        .map(|name| regex::escape(name))
        .collect::<Vec<_>>()
        .join("|");
    let pattern = format!(
        r"(?m)^((?:%\S+[\s]+)*)#(?:{alternation})(?:!!|\?\?)?(?:\([^)]*\)|\+|[_:][^\s]*|)\s"
    );
    Regex::new(&pattern).expect("valid vcs replace pattern")
}

pub fn named_args_skeleton(entry: &XpromptAssistEntry) -> String {
    let required: Vec<_> =
        entry.inputs.iter().filter(|input| input.required).collect();
    if required.is_empty() {
        return entry.insertion.clone();
    }
    let args = required
        .iter()
        .enumerate()
        .map(|(idx, input)| format!("{}=${}", input.name, idx + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}({args})$0", entry.insertion)
}

pub fn colon_args_skeleton(entry: &XpromptAssistEntry) -> String {
    format!("{}:$0", entry.insertion)
}

fn detect_xprompt_arg_completion_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let text = document.text();
    let prefix = text.get(..cursor)?;
    let captures = xprompt_ref_re().captures_iter(prefix);
    for caps in captures {
        let whole = caps.get(0)?;
        let name = caps.name("name")?.as_str().replace("__", "/");
        let entry = entries.iter().find(|entry| entry.name == name)?;
        if entry.inputs.is_empty() {
            continue;
        }
        let base_end = whole.end();
        let suffix = text.get(base_end..cursor)?;
        if suffix.starts_with(':') {
            let (kind, active_input, token_start) =
                colon_arg_context(entry, base_end, suffix)?;
            return arg_context(
                document,
                kind,
                cursor,
                token_start,
                entry,
                active_input,
                BTreeSet::new(),
            );
        }
        if suffix.starts_with('(') {
            let (kind, active_input, token_start, used) =
                paren_arg_context(entry, base_end, suffix)?;
            return arg_context(
                document,
                kind,
                cursor,
                token_start,
                entry,
                active_input,
                used,
            );
        }
    }
    None
}

fn colon_arg_context(
    entry: &XpromptAssistEntry,
    base_end: usize,
    suffix: &str,
) -> Option<(CompletionContextKind, XpromptInputHint, usize)> {
    let value = suffix.strip_prefix(':')?;
    if value.chars().any(char::is_whitespace)
        || value.contains('+')
        || value.contains('(')
        || value.contains(')')
    {
        return None;
    }
    let index = value.matches(',').count().min(entry.inputs.len() - 1);
    let active_input = entry.inputs.get(index)?.clone();
    let token_start =
        base_end + 1 + value.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    Some((
        completion_kind_for_input(&active_input),
        active_input,
        token_start,
    ))
}

fn paren_arg_context(
    entry: &XpromptAssistEntry,
    base_end: usize,
    suffix: &str,
) -> Option<(
    CompletionContextKind,
    XpromptInputHint,
    usize,
    BTreeSet<String>,
)> {
    let body = suffix.strip_prefix('(')?;
    if body.contains(')') {
        return None;
    }
    let clause_start = body.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    let clause = &body[clause_start..];
    let stripped = clause.trim_start();
    let leading_ws = clause.len() - stripped.len();
    let value_start = base_end + 1 + clause_start + leading_ws;
    let used = used_named_arg_names(&body[..clause_start]);

    if !stripped.contains('=') {
        if stripped.chars().any(char::is_whitespace) {
            return None;
        }
        let placeholder = XpromptInputHint {
            name: String::new(),
            r#type: String::new(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
        };
        return Some((
            CompletionContextKind::XpromptArgumentName,
            placeholder,
            value_start,
            used,
        ));
    }

    let (name_part, value_part) = stripped.split_once('=')?;
    let name = name_part.trim();
    let active_input = entry
        .inputs
        .iter()
        .find(|input| input.name == name)?
        .clone();
    let value_leading_ws = value_part.len() - value_part.trim_start().len();
    let token_start = value_start + name_part.len() + 1 + value_leading_ws;
    Some((
        completion_kind_for_input(&active_input),
        active_input,
        token_start,
        used,
    ))
}

fn arg_context(
    document: &DocumentSnapshot,
    kind: CompletionContextKind,
    cursor: usize,
    token_start: usize,
    entry: &XpromptAssistEntry,
    active_input: XpromptInputHint,
    _used: BTreeSet<String>,
) -> Option<CompletionContext> {
    let range = document.byte_range_to_range(token_start, cursor)?;
    Some(CompletionContext {
        kind,
        token: Some(TokenInfo {
            text: document.text().get(token_start..cursor)?.to_string(),
            range,
            byte_start: token_start,
            byte_end: cursor,
        }),
        active_xprompt: Some(entry.name.clone()),
        active_input: (!active_input.name.is_empty())
            .then_some(active_input.name),
        directive_name: None,
        replacement_range: range,
    })
}

fn detect_directive_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let line = document.line_text(position.line)?;
    let line_start = document.position_to_byte_offset(EditorPosition {
        line: position.line,
        character: 0,
    })?;
    let cursor_in_line = cursor.checked_sub(line_start)?;
    let before = line.get(..cursor_in_line)?;

    if let Some((start, token)) = directive_name_token(before) {
        let byte_start = line_start + start;
        let range = document.byte_range_to_range(byte_start, cursor)?;
        return Some(CompletionContext {
            kind: CompletionContextKind::DirectiveName,
            token: Some(TokenInfo {
                text: token.to_string(),
                range,
                byte_start,
                byte_end: cursor,
            }),
            active_xprompt: None,
            active_input: None,
            directive_name: None,
            replacement_range: range,
        });
    }

    if let Some((name, arg_start)) = directive_arg_token(before) {
        let byte_start = line_start + arg_start;
        let range = document.byte_range_to_range(byte_start, cursor)?;
        return Some(CompletionContext {
            kind: CompletionContextKind::DirectiveArgument,
            token: Some(TokenInfo {
                text: before[arg_start..].to_string(),
                range,
                byte_start,
                byte_end: cursor,
            }),
            active_xprompt: None,
            active_input: None,
            directive_name: Some(name.to_string()),
            replacement_range: range,
        });
    }
    None
}

fn directive_name_token(before: &str) -> Option<(usize, &str)> {
    let start = before.rfind('%')?;
    let token = &before[start..];
    if token == "%("
        || token[1..]
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Some((start, token));
    }
    None
}

fn directive_arg_token(before: &str) -> Option<(&str, usize)> {
    let percent = before.rfind('%')?;
    let directive = &before[percent + 1..];
    let split = directive.find([':', '('])?;
    let name = &directive[..split];
    canonical_directive_name(name)?;
    let sep_idx = percent + 1 + split;
    if directive.as_bytes().get(split) == Some(&b'(') && directive.contains(')')
    {
        return None;
    }
    Some((name, sep_idx + 1))
}

fn context_for_token(
    kind: CompletionContextKind,
    token: TokenInfo,
) -> CompletionContext {
    CompletionContext {
        kind,
        replacement_range: token.range,
        token: Some(token),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
    }
}

fn completion_kind_for_input(
    input: &XpromptInputHint,
) -> CompletionContextKind {
    match input.r#type.as_str() {
        "path" => CompletionContextKind::XpromptArgumentPath,
        "bool" => CompletionContextKind::XpromptArgumentValue,
        _ => CompletionContextKind::XpromptArgumentTypeHint,
    }
}

fn input_label(input: &XpromptInputHint) -> String {
    let suffix = if input.required { "" } else { "?" };
    format!("{}{suffix}: {}", input.name, input.r#type)
}

fn input_documentation(input: &XpromptInputHint) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(description) = input
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(description.to_string());
    }
    if let Some(default) = &input.default_display {
        parts.push(format!("default: {default}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn snippet_documentation(entry: &EditorSnippetEntryWire) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(description) = entry
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(description.to_string());
    }
    if let Some(source_path) = entry
        .source_path_display
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("Source: {source_path}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn used_named_arg_names(body_prefix: &str) -> BTreeSet<String> {
    body_prefix
        .split(',')
        .filter_map(|clause| {
            clause.split_once('=').map(|(name, _)| name.trim())
        })
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

fn shared_extension(
    candidates: &[CompletionCandidate],
    partial: &str,
) -> String {
    if candidates.len() <= 1 {
        return String::new();
    }
    let mut prefix = candidates[0].name.clone();
    for candidate in &candidates[1..] {
        prefix = common_prefix(&prefix, &candidate.name);
    }
    if prefix.len() > partial.len() {
        prefix[partial.len()..].to_string()
    } else {
        String::new()
    }
}

fn common_prefix(left: &str, right: &str) -> String {
    let mut end = 0;
    for ((left_idx, left_ch), (_, right_ch)) in
        left.char_indices().zip(right.char_indices())
    {
        if left_ch != right_ch {
            break;
        }
        end = left_idx + left_ch.len_utf8();
    }
    left[..end].to_string()
}

fn xprompt_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EditorXpromptCatalogEntryWire, MobileXpromptInputWire};

    fn pos(character: u32) -> EditorPosition {
        EditorPosition { line: 0, character }
    }

    fn entries() -> Vec<XpromptAssistEntry> {
        assist_entries_from_catalog(&[
            EditorXpromptCatalogEntryWire {
                name: "review".to_string(),
                display_label: "review".to_string(),
                insertion: Some("#review".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("xprompt".to_string()),
                description: Some("Review code".to_string()),
                source_bucket: "builtin".to_string(),
                project: None,
                tags: vec![],
                input_signature: Some("(path: path, deep?: bool)".to_string()),
                inputs: vec![
                    MobileXpromptInputWire {
                        name: "path".to_string(),
                        r#type: "path".to_string(),
                        description: Some("Path to review".to_string()),
                        required: true,
                        default_display: None,
                        position: 0,
                    },
                    MobileXpromptInputWire {
                        name: "deep".to_string(),
                        r#type: "bool".to_string(),
                        description: Some("Run a deeper pass".to_string()),
                        required: false,
                        default_display: Some("false".to_string()),
                        position: 1,
                    },
                ],
                is_skill: false,
                content_preview: Some("review body".to_string()),
                source_path_display: Some("xprompts/review.md".to_string()),
                definition_path: Some("/tmp/xprompts/review.md".to_string()),
                definition_range: None,
            },
            EditorXpromptCatalogEntryWire {
                name: "run".to_string(),
                display_label: "run".to_string(),
                insertion: Some("#!run".to_string()),
                reference_prefix: Some("#!".to_string()),
                kind: Some("workflow".to_string()),
                description: None,
                source_bucket: "project".to_string(),
                project: None,
                tags: vec![],
                input_signature: None,
                inputs: vec![],
                is_skill: true,
                content_preview: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
            },
        ])
    }

    #[test]
    fn classifies_primary_completion_modes() {
        let catalog = entries();
        for (text, col, kind) in [
            ("#re", 3, CompletionContextKind::Xprompt),
            ("/ru", 3, CompletionContextKind::SlashSkill),
            ("./sr", 4, CompletionContextKind::FilePath),
            ("", 0, CompletionContextKind::FileHistory),
            ("%mo", 3, CompletionContextKind::DirectiveName),
            ("%model:", 7, CompletionContextKind::DirectiveArgument),
            ("foo", 3, CompletionContextKind::SnippetTrigger),
            ("foo_1", 5, CompletionContextKind::SnippetTrigger),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
        }
    }

    #[test]
    fn builds_catalog_completions_with_marker_filters() {
        let catalog = entries();
        let inline = build_xprompt_completion_candidates("#r", None, &catalog);
        assert_eq!(
            inline
                .candidates
                .iter()
                .map(|c| c.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["#review", "#!run"]
        );

        let standalone =
            build_xprompt_completion_candidates("#!r", None, &catalog);
        assert_eq!(standalone.candidates[0].insertion, "#!run");

        let skill = build_xprompt_completion_candidates("/r", None, &catalog);
        assert_eq!(skill.candidates[0].insertion, "/run");
    }

    #[test]
    fn snippet_context_does_not_steal_higher_priority_tokens() {
        let catalog = entries();
        for text in ["#foo", "/foo", "@foo", "%model", "./foo", ""] {
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(
                &doc,
                pos(text.len() as u32),
                &catalog,
            );
            assert_ne!(
                context.map(|context| context.kind),
                Some(CompletionContextKind::SnippetTrigger),
                "{text}"
            );
        }
    }

    #[test]
    fn builds_snippet_completions_by_case_insensitive_prefix() {
        let list = build_snippet_completion_candidates(
            "fo",
            None,
            &[
                snippet_entry("Foo", "body $1$0", "ace.snippets"),
                snippet_entry("bar", "bar", "xprompt"),
            ],
        );

        assert_eq!(list.candidates.len(), 1);
        assert_eq!(list.candidates[0].display, "Foo");
        assert_eq!(list.candidates[0].insertion, "body $1$0");
        assert_eq!(list.candidates[0].detail.as_deref(), Some("ace.snippets"));
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
            description: None,
            source_path_display: None,
        }
    }

    #[test]
    fn detects_narrow_argument_contexts() {
        let catalog = entries();
        for (text, col, kind, active_input) in [
            (
                "#review:",
                8,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
            (
                "#review(path=",
                13,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
            (
                "#review(de",
                10,
                CompletionContextKind::XpromptArgumentName,
                None,
            ),
            (
                "#review!!:",
                10,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
            assert_eq!(context.active_input.as_deref(), active_input);
        }

        let doc = DocumentSnapshot::new("#ns__foo(arg=");
        let ns_entry = XpromptAssistEntry {
            name: "ns/foo".to_string(),
            display_label: "ns/foo".to_string(),
            insertion: "#ns/foo".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "project".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: vec![XpromptInputHint {
                name: "arg".to_string(),
                r#type: "word".to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
            }],
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
        };
        assert!(
            classify_completion_context(&doc, pos(13), &[ns_entry]).is_some()
        );
    }

    #[test]
    fn builds_argument_name_completions() {
        let catalog = entries();
        let list = build_xprompt_arg_name_candidates(
            &catalog[0],
            &BTreeSet::from(["path".to_string()]),
            "d",
            None,
        );
        assert_eq!(list.candidates[0].insertion, "deep=");
        assert_eq!(
            list.candidates[0].documentation.as_deref(),
            Some("Run a deeper pass\n\ndefault: false")
        );
    }

    // --- vcs_project (`#+`) completion -------------------------------------

    fn vcs_names() -> Vec<String> {
        ["gh", "git", "hg"].iter().map(|s| s.to_string()).collect()
    }

    fn project_entry(name: &str, prefix: &str) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: prefix.to_string(),
            display_tag: format!("#{prefix}:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            kind: "project".to_string(),
            project: name.to_string(),
            status: String::new(),
        }
    }

    fn changespec_entry(
        name: &str,
        project: &str,
        status: &str,
    ) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: "gh".to_string(),
            display_tag: format!("#gh:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            kind: "changespec".to_string(),
            project: project.to_string(),
            status: status.to_string(),
        }
    }

    fn apply_test_edits(text: &str, edits: &VcsProjectByteEdits) -> String {
        let mut all: Vec<&VcsByteEdit> = std::iter::once(&edits.primary)
            .chain(edits.additional.iter())
            .collect();
        all.sort_by_key(|edit| (edit.start, edit.end));
        let mut out = String::new();
        let mut pos = 0;
        for edit in all {
            out.push_str(&text[pos..edit.start]);
            out.push_str(&edit.new_text);
            pos = edit.end;
        }
        out.push_str(&text[pos..]);
        out
    }

    /// Detect the trigger in `marked` (where `‸` is the cursor), then expand
    /// the `#gh:sase` selection both via the canonical transform and via the
    /// applied byte edits. Returns `(canonical, via_edits)`.
    fn expand_via_both(marked: &str) -> (String, String) {
        let cursor_byte = marked.find('‸').expect("cursor marker");
        let text = marked.replacen('‸', "", 1);
        let doc = DocumentSnapshot::new(text.clone());
        let position = doc
            .byte_offset_to_position(cursor_byte)
            .expect("cursor on a char boundary");
        let token =
            vcs_project_trigger_token(&doc, position).expect("a trigger token");
        let re = vcs_replace_regex(&vcs_names());

        let canonical = apply_vcs_project_selection(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        let edits = vcs_project_byte_edits(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        (canonical, apply_test_edits(&text, &edits))
    }

    #[test]
    fn vcs_project_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `_GOLDEN_VECTORS` table. `‸` marks the cursor.
        let cases = [
            ("Describe this repo. #+‸", "#gh:sase Describe this repo."),
            ("#+‸", "#gh:sase "),
            ("#+sa‸", "#gh:sase "),
            ("#+s‸\n", "#gh:sase \n"),
            ("#+s‸\nmore text", "#gh:sase \nmore text"),
            ("#git:foo Fix bug #+‸", "#gh:sase Fix bug"),
            ("#gh!!:foo do X #+‸", "#gh:sase do X"),
            ("Fix #+bug‸ here", "#gh:sase Fix here"),
            ("Line one\n#+‸", "#gh:sase Line one\n"),
            (
                "---\nname: x\n---\nBody #+‸",
                "---\nname: x\n---\n#gh:sase Body",
            ),
            ("%model:opus Body #+‸", "%model:opus #gh:sase Body"),
            // Bare `+` at the very beginning of the prompt (offset 0).
            ("+‸", "#gh:sase "),
            ("+sa‸", "#gh:sase "),
            ("+sa‸ Fix", "#gh:sase Fix"),
        ];
        for (marked, expected) in cases {
            let (canonical, via_edits) = expand_via_both(marked);
            assert_eq!(canonical, expected, "canonical: {marked:?}");
            assert_eq!(via_edits, expected, "via edits: {marked:?}");
        }
    }

    #[test]
    fn vcs_prepend_offset_skips_horizontal_whitespace_only() {
        assert_eq!(vcs_prepend_offset("\n"), 0);
        assert_eq!(vcs_prepend_offset("\nmore"), 0);
        assert_eq!(vcs_prepend_offset("  Body"), 2);
        assert_eq!(vcs_prepend_offset("\tBody"), 1);
    }

    #[test]
    fn classifies_vcs_project_trigger() {
        for (text, col) in [
            ("#+", 2),
            ("Fix #+", 6),
            ("#+sa", 4),
            ("2 #+ 2", 4),
            // Bare `+` at byte offset 0.
            ("+", 1),
            ("+sa", 3),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &[]).unwrap();
            assert_eq!(
                context.kind,
                CompletionContextKind::VcsProject,
                "{text}"
            );
        }

        // A bare `+` outside byte offset 0 is not a project trigger.
        for (text, col) in [("Fix +", 5), (" +", 2), ("# +", 3), ("c#+x", 4)] {
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(&doc, pos(col), &[]);
            assert_ne!(
                context.map(|context| context.kind),
                Some(CompletionContextKind::VcsProject),
                "{text}"
            );
        }
    }

    #[test]
    fn bare_trigger_merges_into_single_primary_edit() {
        let doc = DocumentSnapshot::new("#+");
        let context = classify_completion_context(&doc, pos(2), &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            pos(2),
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert_eq!(candidate.name, "sase");
        assert_eq!(candidate.insertion, "#gh:sase");
        // BOF `#+`: the prepend point coincides with the trigger deletion, so
        // the edits merge into one primary edit with no additional edits.
        assert!(candidate.additional_edits.is_empty());
        assert_eq!(
            candidate.replacement.as_ref().unwrap().new_text,
            "#gh:sase "
        );
    }

    #[test]
    fn trailing_trigger_emits_primary_plus_additional_edit() {
        let doc = DocumentSnapshot::new("Describe this repo. #+");
        let cursor = pos(22);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        let candidate = &list.candidates[0];
        // The primary edit consumes the trigger token; the additional edit
        // prepends the tag at the start of the document.
        assert_eq!(candidate.replacement.as_ref().unwrap().new_text, "");
        assert_eq!(candidate.additional_edits.len(), 1);
        assert_eq!(candidate.additional_edits[0].new_text, "#gh:sase ");
        let prepend_range = candidate.additional_edits[0].range;
        assert_eq!(prepend_range.start, prepend_range.end);
    }

    #[test]
    fn vcs_project_candidates_filter_preserves_catalog_order() {
        let doc = DocumentSnapshot::new("#+sa");
        let cursor = pos(4);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                project_entry("saseling", "gh"),
                project_entry("bob", "git"),
            ],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "saseling"]
        );
    }

    #[test]
    fn vcs_project_candidates_include_changespec_context() {
        let doc = DocumentSnapshot::new("#+ship");
        let cursor = pos(6);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                changespec_entry("ship-completion", "sase", "Ready"),
            ],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert_eq!(candidate.name, "ship-completion");
        assert_eq!(candidate.insertion, "#gh:ship-completion");
        assert_eq!(candidate.kind, "changespec");
        assert_eq!(candidate.project, "sase");
        assert_eq!(candidate.status, "Ready");
    }

    #[test]
    fn vcs_project_candidates_filter_for_bare_plus_query() {
        // The query for a BOF `+sa` token is `sa` (prefix length 1), so the
        // candidate list filters and sorts exactly as the `#+sa` case does.
        let doc = DocumentSnapshot::new("+sa");
        let cursor = pos(3);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsProject);
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                project_entry("saseling", "gh"),
                project_entry("bob", "git"),
            ],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "saseling"]
        );
    }

    #[test]
    fn bare_plus_trigger_merges_into_single_primary_edit() {
        // A BOF `+`: the prepend point coincides with the trigger deletion, so
        // the edits merge into one primary edit with no additional edits.
        let doc = DocumentSnapshot::new("+");
        let context = classify_completion_context(&doc, pos(1), &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            pos(1),
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert!(candidate.additional_edits.is_empty());
        assert_eq!(
            candidate.replacement.as_ref().unwrap().new_text,
            "#gh:sase "
        );
    }

    #[test]
    fn vcs_project_candidates_match_aliases() {
        let doc = DocumentSnapshot::new("#+sea");
        let cursor = pos(5);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let mut entry = project_entry("sase", "gh");
        entry.aliases = vec!["seaside".to_string()];
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[entry, project_entry("bob", "git")],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase"]
        );
    }

    #[test]
    fn vcs_project_edits_never_overlap() {
        // Every golden input must yield non-overlapping edits (LSP requires
        // it); `vcs_edits_conflict` is the guard.
        for marked in [
            "Describe this repo. #+‸",
            "#+‸",
            "#+sa‸",
            "#git:foo Fix bug #+‸",
            "%model:opus Body #+‸",
            "+‸",
            "+sa‸",
            "+sa‸ Fix",
        ] {
            let cursor_byte = marked.find('‸').unwrap();
            let text = marked.replacen('‸', "", 1);
            let doc = DocumentSnapshot::new(text.clone());
            let position = doc.byte_offset_to_position(cursor_byte).unwrap();
            let token = vcs_project_trigger_token(&doc, position).unwrap();
            let re = vcs_replace_regex(&vcs_names());
            let edits = vcs_project_byte_edits(
                &text,
                token.byte_start,
                token.byte_end,
                "#gh:sase",
                &re,
            );
            assert!(
                !vcs_edits_conflict(&edits.primary, &edits.additional),
                "overlapping edits for {marked:?}"
            );
        }
    }
}
