//! Project tag completion trigger and in-place accept.
//!
//! The trigger opens the projects menu at any tag left boundary (start of
//! text, whitespace, `{`, or `|`). Accepting a row replaces the typed
//! `+query` token in place and removes every *other* workspace target (VCS
//! refs and resolved tags) from the same `---` segment, outside literal
//! zones, so picking a project always leaves exactly one.

use regex::Regex;

use super::resolve::resolve_project_tag;
use super::scan::{
    is_left_boundary, project_tag_literal_zones, scan_project_tags,
};
use super::wire::{
    ProjectTagApplyWire, ProjectTagResolutionWire, ProjectTagTargetWire,
    ProjectTagTriggerWire,
};

/// Detect a live `+query` completion trigger at byte offset `cursor`.
///
/// `cursor` is a UTF-8 byte offset. The returned span covers the whole typed
/// token (it may extend past the cursor); `query` is the text after the `+`
/// up to the cursor and filters the menu rows.
pub fn project_tag_trigger(
    text: &str,
    cursor: usize,
) -> Option<ProjectTagTriggerWire> {
    if cursor == 0 || cursor > text.len() || !text.is_char_boundary(cursor) {
        return None;
    }
    let bytes = text.as_bytes();
    let mut query_start = cursor;
    while query_start > 0 && is_name_byte(bytes[query_start - 1]) {
        query_start -= 1;
    }
    if query_start == 0 || bytes[query_start - 1] != b'+' {
        return None;
    }
    let start = query_start - 1;
    if !is_left_boundary(text, start) {
        return None;
    }
    let mut end = cursor;
    while end < bytes.len() && is_name_byte(bytes[end]) {
        end += 1;
    }
    Some(ProjectTagTriggerWire {
        start,
        end,
        query: text[query_start..cursor].to_string(),
    })
}

/// Apply an accepted completion row to `text`.
///
/// `trigger_span` is the byte span of the typed `+query` token (from
/// [`project_tag_trigger`]). `insertion` is the row's text verbatim — a
/// project row passes `+<name> ` (or `#<workflow>:<name> ` when the name is
/// not in the tag grammar) and a PR row passes `#<workflow>:<patch> ` — so
/// it must carry its own trailing separator.
///
/// The trigger token is replaced in place. Every other workspace target in
/// the trigger's `---` segment is deleted with whitespace collapsing: VCS
/// workflow refs at line starts (the historical replace pattern) and
/// resolved tag spans. Unknown tags are plain text and stay; literal zones
/// are never touched. Returns the new text and the caret byte offset just
/// past the insertion.
///
/// An invalid trigger span degrades to the input text with the caret clamped
/// into range; it never panics.
pub fn apply_project_tag_selection(
    text: &str,
    trigger_span: (usize, usize),
    insertion: &str,
    workflow_names: &[String],
    targets: &[ProjectTagTargetWire],
) -> ProjectTagApplyWire {
    let (primary, additional) = project_tag_selection_edits(
        text,
        trigger_span,
        insertion,
        workflow_names,
        targets,
    );
    let mut edits: Vec<(&ProjectTagSelectionEdit, bool)> =
        vec![(&primary, true)];
    edits.extend(additional.iter().map(|edit| (edit, false)));
    edits.sort_by_key(|(edit, _)| (edit.start, edit.end));

    let mut out = String::with_capacity(text.len() + insertion.len());
    let mut cursor = trigger_span.0.min(text.len());
    let mut pos = 0;
    for (edit, is_primary) in edits {
        if edit.start < pos {
            continue;
        }
        out.push_str(text.get(pos..edit.start).unwrap_or_default());
        if is_primary {
            cursor = out.len() + edit.new_text.len();
        }
        out.push_str(&edit.new_text);
        pos = edit.end.max(pos);
    }
    out.push_str(text.get(pos..).unwrap_or_default());
    ProjectTagApplyWire { text: out, cursor }
}

/// One byte-range replacement behind [`apply_project_tag_selection`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectTagSelectionEdit {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) new_text: String,
}

/// Compute the trigger replacement plus the other-target deletions.
///
/// Edits are in original-document coordinates and never overlap: a deletion
/// that would collide with the trigger replacement is dropped (the explicit
/// trigger wins).
pub(crate) fn project_tag_selection_edits(
    text: &str,
    trigger_span: (usize, usize),
    insertion: &str,
    workflow_names: &[String],
    targets: &[ProjectTagTargetWire],
) -> (ProjectTagSelectionEdit, Vec<ProjectTagSelectionEdit>) {
    let (trigger_start, trigger_end) = trigger_span;
    if !valid_trigger_span(text, trigger_start, trigger_end) {
        return (
            ProjectTagSelectionEdit {
                start: trigger_start.min(text.len()),
                end: trigger_start.min(text.len()),
                new_text: String::new(),
            },
            Vec::new(),
        );
    }
    let (delete_start, delete_end) =
        trigger_strip_region(text, trigger_start, trigger_end);
    let primary = ProjectTagSelectionEdit {
        start: delete_start,
        end: delete_end,
        new_text: insertion.to_string(),
    };

    let (segment_start, segment_end) = segment_containing(text, trigger_start);
    let zones = removal_literal_zones(text);
    let mut additional: Vec<ProjectTagSelectionEdit> = Vec::new();
    for (start, end) in workspace_ref_deletions(
        text,
        segment_start,
        segment_end,
        workflow_names,
    ) {
        if ranges_overlap(start, end, trigger_start, trigger_end)
            || span_in_zones(start, end, &zones)
        {
            continue;
        }
        let (delete_start, delete_end) = orphan_strip_region(text, start, end);
        if ranges_overlap(delete_start, delete_end, primary.start, primary.end)
        {
            continue;
        }
        additional.push(ProjectTagSelectionEdit {
            start: delete_start,
            end: delete_end,
            new_text: String::new(),
        });
    }
    for span in scan_project_tags(text) {
        if span.end <= segment_start
            || span.start >= segment_end
            || ranges_overlap(span.start, span.end, trigger_start, trigger_end)
            || span_in_zones(span.start, span.end, &zones)
            || !is_workspace_target(&span.name, targets)
        {
            continue;
        }
        let (delete_start, delete_end) =
            orphan_strip_region(text, span.start, span.end);
        if ranges_overlap(delete_start, delete_end, primary.start, primary.end)
        {
            continue;
        }
        additional.push(ProjectTagSelectionEdit {
            start: delete_start,
            end: delete_end,
            new_text: String::new(),
        });
    }
    additional.sort_by_key(|edit| (edit.start, edit.end));
    (primary, additional)
}

/// Whether the scanned tag name is a workspace target: anything but unknown
/// (unknown non-anchored tags are plain text; anchored unknowns error at
/// launch and the backend reports them).
fn is_workspace_target(name: &str, targets: &[ProjectTagTargetWire]) -> bool {
    !matches!(
        resolve_project_tag(name, targets),
        ProjectTagResolutionWire::Unknown { .. }
    )
}

fn valid_trigger_span(text: &str, start: usize, end: usize) -> bool {
    start <= end
        && end <= text.len()
        && text.is_char_boundary(start)
        && text.is_char_boundary(end)
        && text.as_bytes().get(start).copied() == Some(b'+')
}

/// Delete the trigger token, eating one following literal space when present.
///
/// The insertion carries its own trailing separator, so a preceding space is
/// always kept as the separator. Mirrors the historical
/// `_strip_trigger_token` except in the end-of-line case, where the space is
/// no longer orphaned once the insertion lands.
fn trigger_strip_region(
    text: &str,
    start: usize,
    end: usize,
) -> (usize, usize) {
    if end < text.len() && text.as_bytes()[end] == b' ' {
        (start, end + 1)
    } else {
        (start, end)
    }
}

/// Delete a span, collapsing one adjacent literal space the way the
/// historical `_strip_trigger_token` did.
fn orphan_strip_region(text: &str, start: usize, end: usize) -> (usize, usize) {
    let before_space =
        start > 0 && text.as_bytes().get(start - 1).copied() == Some(b' ');
    let after_space =
        end < text.len() && text.as_bytes().get(end).copied() == Some(b' ');
    let after_eol = end >= text.len() || text[end..].starts_with(['\r', '\n']);
    let before_bol = start == 0 || text[..start].ends_with(['\r', '\n']);

    if before_space && after_space {
        (start, end + 1)
    } else if before_space && after_eol {
        (start - 1, end)
    } else if after_space && before_bol {
        (start, end + 1)
    } else {
        (start, end)
    }
}

/// Line-start VCS workflow ref cores (without any `%directive` prefix)
/// inside `[segment_start, segment_end)`.
///
/// This is the historical replace pattern: group 1 captures a leading
/// `%directive` prefix, which callers preserve.
fn workspace_ref_deletions(
    text: &str,
    segment_start: usize,
    segment_end: usize,
    workflow_names: &[String],
) -> Vec<(usize, usize)> {
    let pattern = workspace_target_ref_regex(workflow_names);
    pattern
        .captures_iter(text)
        .filter_map(|caps| {
            let whole = caps.get(0)?;
            let prefix_len = caps.get(1).map_or(0, |m| m.len());
            let start = whole.start() + prefix_len;
            let end = whole.end();
            if start < segment_start || end > segment_end || start >= end {
                return None;
            }
            Some((start, end))
        })
        .collect()
}

/// The historical VCS-tag replace pattern, now used to *find* other
/// workspace targets for deletion.
fn workspace_target_ref_regex(workflow_names: &[String]) -> Regex {
    let mut names: Vec<&str> =
        workflow_names.iter().map(String::as_str).collect();
    names.sort_unstable();
    let alternation = names
        .iter()
        .map(|name| regex::escape(name))
        .collect::<Vec<_>>()
        .join("|");
    let pattern = format!(
        r"(?m)^((?:%\S+[\s]+)*)#(?:{alternation})(?:!!|\?\?)?(?:\([^)]*\)|\+|[_:][^\s]*|)(?:\s|$)"
    );
    Regex::new(&pattern).expect("valid workspace target pattern")
}

/// The `---` segment containing `offset`.
///
/// Separators are lines matching `^---\s*$` (the same rule as the Python
/// `_SEGMENT_SEPARATOR_RE`). Frontmatter delimiters split segments too, but
/// frontmatter is a literal zone so nothing is ever removed there.
fn segment_containing(text: &str, offset: usize) -> (usize, usize) {
    let mut seg_start = 0;
    let mut seg_end = text.len();
    let mut line_start = 0;
    for line in text.split_inclusive('\n') {
        let line_end = line_start + line.len();
        if is_segment_separator(line) {
            if line_end <= offset {
                seg_start = line_end;
            } else if line_start > offset {
                seg_end = line_start;
                break;
            }
        }
        line_start = line_end;
    }
    (seg_start.min(seg_end), seg_end)
}

/// Whether `line` (with its newline) is a `---` segment separator.
fn is_segment_separator(line: &str) -> bool {
    let content = line.trim_end_matches(['\r', '\n']);
    content == "---"
        || content.strip_prefix("---").is_some_and(|tail| {
            !tail.is_empty() && tail.chars().all(|ch| ch == ' ' || ch == '\t')
        })
}

fn removal_literal_zones(text: &str) -> Vec<(usize, usize)> {
    project_tag_literal_zones(text)
}

fn span_in_zones(start: usize, end: usize, zones: &[(usize, usize)]) -> bool {
    zones.iter().any(|(s, e)| start < *e && *s < end)
}

fn ranges_overlap(
    left_start: usize,
    left_end: usize,
    right_start: usize,
    right_end: usize,
) -> bool {
    left_start < right_end && right_start < left_end
}

fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')
}
