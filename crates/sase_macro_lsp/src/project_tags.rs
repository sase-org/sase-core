//! Shared xprompt project-tag (`+sase`) display helpers.
//!
//! The v5 `vcs_project` catalog carries tag-resolution targets
//! ([`ProjectTagTargetWire`]) plus per-entry display metadata
//! ([`VcsProjectEntry`]: `tag`, `accent_index`, `current`). Targets resolve;
//! entries describe. Every helper here maps a resolved target to its entry
//! by directory key (falling back to name) so hover, diagnostics, semantic
//! tokens, and completion render one consistent story.
//!
//! The catalog wire carries each target's lifecycle `state` and
//! `workspace_dir` (serde-defaulted, so older catalogs without them keep
//! working); disabled targets warn with an enable hint, provider-less
//! targets (no `workflow_type`) warn as unclaimed, and hover plus
//! completion docs show the state and workspace dir when present.

use sase_core::project_tag::{
    is_tag_name, resolve_project_tag, scan_project_tags,
    ProjectTagResolutionWire, ProjectTagTargetWire,
};
use sase_core::{
    prompt_literal_zone_ranges, DiagnosticSeverity, DocumentSnapshot,
    EditorDiagnostic, EditorPosition, EditorRange, HoverPayload,
    VcsProjectEntry,
};

/// One tag edit offered as a code action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TagEdit {
    pub(crate) range: EditorRange,
    pub(crate) new_text: String,
    pub(crate) title: String,
}

/// The entry describing a resolved target: project rows only, matched by
/// directory key first, then by exact and casefolded name. Patch rows never
/// describe tags; they keep their `#` spelling.
pub(crate) fn entry_for_target<'a>(
    targets: &[ProjectTagTargetWire],
    index: usize,
    entries: &'a [VcsProjectEntry],
) -> Option<&'a VcsProjectEntry> {
    let target = targets.get(index)?;
    entries
        .iter()
        .filter(|entry| !is_patch_entry(entry))
        .find(|entry| !entry.key.is_empty() && entry.key == target.key)
        .or_else(|| {
            entries
                .iter()
                .filter(|entry| !is_patch_entry(entry))
                .find(|entry| entry.name == target.name)
        })
        .or_else(|| {
            entries
                .iter()
                .filter(|entry| !is_patch_entry(entry))
                .find(|entry| entry.name.eq_ignore_ascii_case(&target.name))
        })
}

/// Catalog index and entry behind one completion candidate.
pub(crate) fn entry_for_candidate<'a>(
    name: &str,
    is_patch: bool,
    entries: &'a [VcsProjectEntry],
) -> Option<(usize, &'a VcsProjectEntry)> {
    entries.iter().enumerate().find(|(_, entry)| {
        entry.name == name && is_patch_entry(entry) == is_patch
    })
}

/// Whether the entry is a patch/PR row.
pub(crate) fn is_patch_entry(entry: &VcsProjectEntry) -> bool {
    let raw = if entry.entry_kind.is_empty() {
        entry.kind.as_str()
    } else {
        entry.entry_kind.as_str()
    };
    matches!(raw, "patch" | "changespec")
}

/// The tag spelling for an entry: its catalog `tag`, else `+name` when the
/// name fits the tag grammar, else the `#workflow:name` fallback (mirrors
/// the core accept algorithm's insertion).
pub(crate) fn tag_spelling(entry: &VcsProjectEntry) -> String {
    if !entry.tag.is_empty() {
        return entry.tag.clone();
    }
    if is_tag_name(&entry.name) {
        return format!("+{}", entry.name);
    }
    format!("#{}:{}", entry.vcs_prefix, entry.name)
}

/// Accent palette index for a resolved target, via its entry.
pub(crate) fn accent_index_for_target(
    targets: &[ProjectTagTargetWire],
    index: usize,
    entries: &[VcsProjectEntry],
) -> Option<u32> {
    entry_for_target(targets, index, entries)?.accent_index
}

/// Identity string a resolved target contributes to project lookup: the
/// entry's project basename (or name), else the target key.
pub(crate) fn identity_for_target(
    targets: &[ProjectTagTargetWire],
    index: usize,
    entries: &[VcsProjectEntry],
) -> Option<String> {
    let target = targets.get(index)?;
    if let Some(entry) = entry_for_target(targets, index, entries) {
        if !entry.project.is_empty() {
            return Some(entry.project.clone());
        }
        return Some(entry.name.clone());
    }
    Some(target.key.clone())
}

/// Sorted, de-duplicated `+` spellings for "Known:" diagnostic lists.
pub(crate) fn known_tag_spellings(
    targets: &[ProjectTagTargetWire],
    entries: &[VcsProjectEntry],
) -> Vec<String> {
    let mut spellings = Vec::new();
    for (index, target) in targets.iter().enumerate() {
        let spelling = entry_for_target(targets, index, entries)
            .map(tag_spelling)
            .filter(|spelling| spelling.starts_with('+'))
            .or_else(|| {
                is_tag_name(&target.name).then(|| format!("+{}", target.name))
            });
        if let Some(spelling) = spelling {
            if !spellings.contains(&spelling) {
                spellings.push(spelling);
            }
        }
    }
    spellings.sort();
    spellings
}

/// Whether a tag target is disabled (`state == "disabled"`).
pub(crate) fn is_disabled_target(target: &ProjectTagTargetWire) -> bool {
    target.state.as_deref() == Some("disabled")
}

/// Markdown documentation for a project completion row: the description,
/// then the ref, key, aliases, current marker, and — when the catalog
/// target carries them — the lifecycle state and workspace dir.
pub(crate) fn project_entry_documentation(
    entry: &VcsProjectEntry,
    target: Option<&ProjectTagTargetWire>,
) -> String {
    let mut blocks = Vec::new();
    if !entry.description.is_empty() {
        blocks.push(entry.description.clone());
    }
    let mut meta = vec![format!("`{}`", entry.display_tag)];
    if !entry.key.is_empty() {
        meta.push(format!("key `{}`", entry.key));
    }
    if !entry.aliases.is_empty() {
        meta.push(format!(
            "aliases {}",
            entry
                .aliases
                .iter()
                .map(|alias| format!("`{alias}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if entry.current == Some(true) {
        meta.push("current".to_string());
    }
    if let Some(state) = target.and_then(|target| target.state.as_deref()) {
        if !state.is_empty() {
            meta.push(format!("state `{state}`"));
        }
    }
    if let Some(dir) = target.and_then(|target| target.workspace_dir.as_deref())
    {
        if !dir.is_empty() {
            meta.push(format!("workspace `{dir}`"));
        }
    }
    blocks.push(meta.join(" · "));
    blocks.join("\n\n")
}

/// Hover markdown for a resolved tag: name, provider and VCS ref,
/// directory key, aliases, current marker, state, workspace dir, and
/// description.
fn resolved_tag_markdown(
    tag: &str,
    target: &ProjectTagTargetWire,
    entry: Option<&VcsProjectEntry>,
) -> String {
    let mut lines = vec![format!("**{tag}**")];
    let mut meta = Vec::new();
    if let Some(entry) = entry {
        meta.push(format!(
            "{} · `{}`",
            entry.provider_display, entry.display_tag
        ));
    } else if let Some(workflow) = target.workflow_type.as_deref() {
        meta.push(format!("`#{workflow}:{}`", target.name));
    }
    if !target.key.is_empty() {
        meta.push(format!("key `{}`", target.key));
    }
    if !target.aliases.is_empty() {
        meta.push(format!(
            "aliases {}",
            target
                .aliases
                .iter()
                .map(|alias| format!("`{alias}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if entry.is_some_and(|entry| entry.current == Some(true)) {
        meta.push("current".to_string());
    }
    if let Some(state) = target.state.as_deref() {
        if !state.is_empty() {
            meta.push(format!("state `{state}`"));
        }
    }
    if let Some(dir) = target.workspace_dir.as_deref() {
        if !dir.is_empty() {
            meta.push(format!("workspace `{dir}`"));
        }
    }
    if target.workflow_type.is_none() {
        meta.push("no VCS provider detected".to_string());
    }
    if !meta.is_empty() {
        lines.push(String::new());
        lines.push(meta.join(" · "));
    }
    if let Some(description) =
        entry.filter(|entry| !entry.description.is_empty())
    {
        lines.push(String::new());
        lines.push(description.description.clone());
    }
    lines.join("\n")
}

/// Hover for the tag under `position`, if it resolves to a project.
pub(crate) fn hover_at_tag(
    document: &DocumentSnapshot,
    position: EditorPosition,
    targets: &[ProjectTagTargetWire],
    entries: &[VcsProjectEntry],
) -> Option<HoverPayload> {
    if targets.is_empty() {
        return None;
    }
    let cursor = document.position_to_byte_offset(position)?;
    let text = document.text();
    for span in scan_project_tags(text) {
        if span.start <= cursor && cursor <= span.end {
            let ProjectTagResolutionWire::Resolved { target_index } =
                resolve_project_tag(&span.name, targets)
            else {
                return None;
            };
            let target = targets.get(target_index)?;
            let entry = entry_for_target(targets, target_index, entries);
            let tag = entry
                .map(tag_spelling)
                .unwrap_or_else(|| format!("+{}", span.name));
            return Some(HoverPayload {
                range: document.byte_range_to_range(span.start, span.end)?,
                markdown: resolved_tag_markdown(tag.as_str(), target, entry),
            });
        }
    }
    None
}

/// D3 editor-column diagnostics: anchored unknown tags warn (with
/// suggestions), ambiguous tags error, disabled tags warn with an enable
/// hint, and resolved tags without a VCS provider warn. Non-anchored
/// unknown tags are plain text: no diagnostic.
pub(crate) fn tag_diagnostics(
    document: &DocumentSnapshot,
    targets: &[ProjectTagTargetWire],
    entries: &[VcsProjectEntry],
) -> Vec<EditorDiagnostic> {
    if targets.is_empty() {
        return Vec::new();
    }
    let known = known_tag_spellings(targets, entries);
    let known_list = if known.is_empty() {
        "none known".to_string()
    } else {
        known.join(" ")
    };
    let mut diagnostics = Vec::new();
    for span in scan_project_tags(document.text()) {
        let Some(range) = document.byte_range_to_range(span.start, span.end)
        else {
            continue;
        };
        let line = document
            .byte_offset_to_position(span.start)
            .map(|position| position.line + 1)
            .unwrap_or(0);
        match resolve_project_tag(&span.name, targets) {
            ProjectTagResolutionWire::Resolved { target_index } => {
                let Some(target) = targets.get(target_index) else {
                    continue;
                };
                if is_disabled_target(target) {
                    diagnostics.push(EditorDiagnostic {
                        range,
                        severity: DiagnosticSeverity::Warning,
                        code: "disabled_project_tag".to_string(),
                        message: format!(
                            "`+{}` is disabled — `sase project enable {}`",
                            span.name, target.name,
                        ),
                    });
                } else if target.workflow_type.is_none() {
                    diagnostics.push(EditorDiagnostic {
                        range,
                        severity: DiagnosticSeverity::Warning,
                        code: "providerless_project_tag".to_string(),
                        message: format!(
                            "`+{}` resolves to `{}` but no VCS provider \
                             was detected for its workspace",
                            span.name, target.key,
                        ),
                    });
                }
            }
            ProjectTagResolutionWire::Ambiguous { candidates } => {
                let names = candidates
                    .iter()
                    .filter_map(|index| targets.get(*index))
                    .map(|target| format!("`{}`", target.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                diagnostics.push(EditorDiagnostic {
                    range,
                    severity: DiagnosticSeverity::Error,
                    code: "ambiguous_project_tag".to_string(),
                    message: format!(
                        "Ambiguous project tag `+{}` (line {line}): \
                         matches {names}. Run `sase doctor` to resolve \
                         the collision",
                        span.name,
                    ),
                });
            }
            ProjectTagResolutionWire::Unknown { suggestions } => {
                if !span.anchored {
                    continue;
                }
                let mut message = format!(
                    "Unknown project tag `+{}` (line {line})",
                    span.name,
                );
                if let Some(first) = suggestions.first() {
                    message.push_str(
                        format!(". Did you mean `{first}`?").as_str(),
                    );
                }
                message.push_str(format!(". Known: {known_list}").as_str());
                diagnostics.push(EditorDiagnostic {
                    range,
                    severity: DiagnosticSeverity::Warning,
                    code: "unknown_project_tag".to_string(),
                    message,
                });
            }
        }
    }
    diagnostics
}

/// Quickfixes replacing each anchored unknown tag with its suggestions.
pub(crate) fn tag_quickfixes(
    document: &DocumentSnapshot,
    targets: &[ProjectTagTargetWire],
) -> Vec<TagEdit> {
    if targets.is_empty() {
        return Vec::new();
    }
    let mut fixes = Vec::new();
    for span in scan_project_tags(document.text()) {
        if !span.anchored {
            continue;
        }
        let ProjectTagResolutionWire::Unknown { suggestions } =
            resolve_project_tag(&span.name, targets)
        else {
            continue;
        };
        let Some(range) = document.byte_range_to_range(span.start, span.end)
        else {
            continue;
        };
        for suggestion in suggestions {
            fixes.push(TagEdit {
                range,
                new_text: suggestion.clone(),
                title: format!("Use {suggestion}"),
            });
        }
    }
    fixes
}

/// `refactor.rewrite` edits turning colon-form project refs
/// (`#gh:sase`) into their tag (`+sase`).
///
/// Only plain project refs qualify: the ref must resolve against the tag
/// targets with a matching provider, the entry must have a tag spelling,
/// and patches, `owner/repo` paths, paren forms, and HITL-marked refs are
/// skipped. The rewritten `+tag` must itself be a standalone tag under D1
/// (a valid left boundary before it and end/whitespace/`|`/`}` after it),
/// so `#gh:sase.`, `#gh:sase,`, and `(#gh:sase)` never rewrite to text
/// that would not expand. Literal zones are inert.
pub(crate) fn tag_rewrites(
    document: &DocumentSnapshot,
    targets: &[ProjectTagTargetWire],
    entries: &[VcsProjectEntry],
    workflow_names: &[String],
) -> Vec<TagEdit> {
    if targets.is_empty() || entries.is_empty() {
        return Vec::new();
    }
    let mut names: Vec<&str> = workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return Vec::new();
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });
    let zones = prompt_literal_zone_ranges(document.text());
    let text = document.text();
    let bytes = text.as_bytes();
    let mut rewrites = Vec::new();
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] != b'#' {
            idx += 1;
            continue;
        }
        let Some((workflow, after_workflow)) = names.iter().find_map(|name| {
            text[idx + 1..]
                .starts_with(name)
                .then(|| (*name, idx + 1 + name.len()))
        }) else {
            idx += 1;
            continue;
        };
        if !is_ref_left_boundary(text, idx) {
            idx += 1;
            continue;
        }
        let mut pos = after_workflow;
        if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
            idx += 1;
            continue;
        }
        if text[pos..].starts_with(':') {
            pos += 1;
        } else {
            idx += 1;
            continue;
        }
        let run_len = text[pos..]
            .bytes()
            .take_while(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'_' | b'.' | b'-')
            })
            .count();
        let mut name = &text[pos..pos + run_len];
        while name.ends_with(['.', '-']) {
            name = &name[..name.len() - 1];
        }
        if name.is_empty() {
            idx += 1;
            continue;
        }
        let ref_end = pos + name.len();
        let after = text[ref_end..].chars().next();
        if after.is_some_and(|ch| ch == '/' || ch == '(') {
            idx = ref_end;
            continue;
        }
        // The rewritten `+tag` must stand alone under D1: `#gh:sase.`,
        // `#gh:sase,`, and `(#gh:sase)` would become `+sase.`, `+sase,`,
        // and `(+sase)`, none of which ever expand.
        if !is_tag_left_boundary(text, idx)
            || !is_tag_right_boundary(text, ref_end)
        {
            idx = ref_end.max(idx + 1);
            continue;
        }
        if zones.iter().any(|zone| idx >= zone.0 && ref_end <= zone.1) {
            idx = ref_end.max(idx + 1);
            continue;
        }
        let ProjectTagResolutionWire::Resolved { target_index } =
            resolve_project_tag(name, targets)
        else {
            idx = ref_end.max(idx + 1);
            continue;
        };
        if targets
            .get(target_index)
            .and_then(|target| target.workflow_type.as_deref())
            != Some(workflow)
        {
            idx = ref_end.max(idx + 1);
            continue;
        }
        if patch_ref_matches(name, workflow, entries) {
            idx = ref_end.max(idx + 1);
            continue;
        }
        let Some(entry) = entry_for_target(targets, target_index, entries)
        else {
            idx = ref_end.max(idx + 1);
            continue;
        };
        let tag = tag_spelling(entry);
        if !tag.starts_with('+') {
            idx = ref_end.max(idx + 1);
            continue;
        }
        if text[idx..ref_end] == tag {
            idx = ref_end.max(idx + 1);
            continue;
        }
        let Some(range) = document.byte_range_to_range(idx, ref_end) else {
            idx = ref_end.max(idx + 1);
            continue;
        };
        rewrites.push(TagEdit {
            range,
            new_text: tag.clone(),
            title: format!("Use project tag {tag}"),
        });
        idx = ref_end.max(idx + 1);
    }
    rewrites
}

/// A `#workflow:ref` spelling already owned by a patch row is never a
/// project rewrite, even when a project resolves the same name.
fn patch_ref_matches(
    name: &str,
    workflow: &str,
    entries: &[VcsProjectEntry],
) -> bool {
    entries.iter().any(|entry| {
        is_patch_entry(entry)
            && entry.vcs_prefix == workflow
            && (entry.name.eq_ignore_ascii_case(name)
                || entry
                    .aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(name)))
    })
}

fn is_ref_left_boundary(text: &str, hash: usize) -> bool {
    if hash == 0 {
        return true;
    }
    text[..hash].chars().next_back().is_some_and(|ch| {
        ch.is_whitespace() || matches!(ch, '{' | '|' | '(' | ',' | ':')
    })
}

/// Whether a rewritten `+tag` starting where `idx` (the `#`) sits would
/// have a valid D1 left boundary: start of text, whitespace, `{`, or `|`.
fn is_tag_left_boundary(text: &str, idx: usize) -> bool {
    if idx == 0 {
        return true;
    }
    text[..idx]
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_whitespace() || ch == '{' || ch == '|')
}

/// Whether a rewritten `+tag` ending at `end` would have a valid D1 right
/// boundary: end of text, whitespace, `|`, or `}`.
fn is_tag_right_boundary(text: &str, end: usize) -> bool {
    if end >= text.len() {
        return true;
    }
    text[end..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_whitespace() || ch == '|' || ch == '}')
}

/// Identity behind a leading `+tag` first word, for project context.
pub(crate) fn leading_tag_identity(
    text: &str,
    targets: &[ProjectTagTargetWire],
    entries: &[VcsProjectEntry],
) -> Option<String> {
    if targets.is_empty() {
        return None;
    }
    let trimmed_start = text.len() - text.trim_start_matches([' ', '\t']).len();
    let token_end = text[trimmed_start..]
        .find([' ', '\t', '\r', '\n'])
        .map(|offset| trimmed_start + offset)
        .unwrap_or(text.len());
    let token = text.get(trimmed_start..token_end)?;
    if !token.starts_with('+') {
        return None;
    }
    let spans = scan_project_tags(text);
    if !spans
        .iter()
        .any(|span| span.start == trimmed_start && span.end == token_end)
    {
        return None;
    }
    let ProjectTagResolutionWire::Resolved { target_index } =
        resolve_project_tag(&token[1..], targets)
    else {
        return None;
    };
    identity_for_target(targets, target_index, entries)
}
