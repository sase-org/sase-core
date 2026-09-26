//! Project tag completion trigger and target-position accept.
//!
//! The trigger opens the projects menu at any tag left boundary (start of
//! text, whitespace, `{`, or `|`). Accepting a row removes the typed
//! `+query` token and puts the selected row at the earliest existing
//! workspace target in the trigger's `---` segment, deleting every other
//! workspace target there so the accepted prompt keeps exactly one. With no
//! existing target the row goes to the segment's leading project-tag
//! position (after leading whitespace and `%directive` tokens), mirroring
//! the pre-regression prepend behavior. Other segments, unknown tags, and
//! literal zones are never touched.

use std::sync::OnceLock;

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
/// The typed trigger is always removed. The insertion lands at the earliest
/// existing workspace target (VCS workflow refs and resolved tags) in the
/// trigger's `---` segment; every other workspace target in that segment is
/// deleted so the accepted prompt keeps exactly one. With no existing
/// target the insertion goes to the segment's leading project-tag position
/// (after leading whitespace and `%directive` tokens). Unknown tags are
/// plain text and stay; literal zones are never touched. Returns the new
/// text and the caret byte offset just past the insertion.
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
    // The insertion lands at the destination edit (the one carrying
    // `insertion`); the trigger deletion only clears the typed query. The
    // invalid-span fallback carries no insertion, so the caret stays clamped.
    let mut edits: Vec<(&ProjectTagSelectionEdit, bool)> = vec![(
        &primary,
        primary.new_text == insertion && !insertion.is_empty(),
    )];
    edits.extend(additional.iter().map(|edit| {
        (edit, edit.new_text == insertion && !insertion.is_empty())
    }));
    edits.sort_by_key(|(edit, _)| (edit.start, edit.end));

    let mut out = String::with_capacity(text.len() + insertion.len());
    let mut cursor = trigger_span.0.min(text.len());
    let mut pos = 0;
    let mut saw_insertion = false;
    for (edit, is_insertion) in edits {
        if edit.start < pos {
            continue;
        }
        out.push_str(text.get(pos..edit.start).unwrap_or_default());
        if is_insertion && !saw_insertion {
            cursor = out.len() + edit.new_text.len();
            saw_insertion = true;
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

/// Compute the trigger deletion plus the destination insertion and the
/// other-target deletions.
///
/// Edits are in original-document coordinates and never overlap: the primary
/// edit always covers the trigger deletion (or, when the destination
/// coincides with the trigger, the merged insertion), while the additional
/// edits carry the destination replacement or leading insertion plus the
/// remaining same-segment deletions. A deletion that would collide with the
/// trigger deletion or the destination is dropped (the explicit destination
/// wins).
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
    // `base` is the prompt with the typed trigger removed. Destinations are
    // located in `base` so the earliest-target and leading-position logic
    // never sees the query, then mapped back to original coordinates.
    let base = format!("{}{}", &text[..delete_start], &text[delete_end..]);
    let gap = delete_end - delete_start;
    let to_original = |pos: usize| {
        if pos <= delete_start {
            pos
        } else {
            pos + gap
        }
    };

    let offset_in_base = delete_start.min(base.len());
    let (segment_start, segment_end) =
        segment_containing(&base, offset_in_base);
    let zones = removal_literal_zones(&base);

    let mut target_spans: Vec<(usize, usize)> = Vec::new();
    for (start, end) in workspace_ref_deletions(
        &base,
        segment_start,
        segment_end,
        workflow_names,
    ) {
        if span_in_zones(start, end, &zones) {
            continue;
        }
        target_spans.push((start, end));
    }
    for span in scan_project_tags(&base) {
        if span.end <= segment_start
            || span.start >= segment_end
            || span_in_zones(span.start, span.end, &zones)
            || !is_workspace_target(&span.name, targets)
        {
            continue;
        }
        target_spans.push((span.start, span.end));
    }
    target_spans.sort_by_key(|(start, end)| (*start, *end));
    target_spans.dedup();
    // Overlapping target spans (a VCS ref pattern can never overlap a `+`
    // tag, but defensively) keep the earliest so later edits never overlap.
    let mut deduped: Vec<(usize, usize)> = Vec::new();
    for (start, end) in target_spans {
        if start >= end {
            continue;
        }
        if deduped
            .last()
            .is_some_and(|(_, prev_end)| start < *prev_end)
        {
            continue;
        }
        deduped.push((start, end));
    }

    if let Some((dest_start, dest_end)) = deduped.first().copied() {
        // Replace the earliest target with the selected row, consuming one
        // following space or tab (the insertion carries its own trailing
        // separator). Newlines are never consumed so neighbors never join.
        let mut replace_end = dest_end;
        if replace_end < base.len()
            && replace_end < segment_end
            && matches!(base.as_bytes()[replace_end], b' ' | b'\t')
        {
            replace_end += 1;
        }
        let dest_orig_start = to_original(dest_start);
        let dest_orig_end = to_original(replace_end);
        // The trigger deletion and the destination never overlap: targets
        // are located in `base` (trigger removed), so a destination either
        // ends at or before the deletion point or starts after it. A
        // straddling span would mean the trigger sat inside a target token,
        // which the token grammars forbid; merge defensively if it happens.
        if ranges_overlap(
            dest_orig_start,
            dest_orig_end,
            delete_start,
            delete_end,
        ) {
            let start = dest_orig_start.min(delete_start);
            let end = dest_orig_end.max(delete_end);
            return (
                ProjectTagSelectionEdit {
                    start,
                    end,
                    new_text: insertion.to_string(),
                },
                Vec::new(),
            );
        }
        let primary = ProjectTagSelectionEdit {
            start: delete_start,
            end: delete_end,
            new_text: String::new(),
        };
        let mut additional = vec![ProjectTagSelectionEdit {
            start: dest_orig_start,
            end: dest_orig_end,
            new_text: insertion.to_string(),
        }];
        for (start, end) in deduped.into_iter().skip(1) {
            let (del_start, del_end) = orphan_strip_region(&base, start, end);
            let orig_start = to_original(del_start);
            let orig_end = to_original(del_end);
            if ranges_overlap(orig_start, orig_end, delete_start, delete_end)
                || ranges_overlap(
                    orig_start,
                    orig_end,
                    dest_orig_start,
                    dest_orig_end,
                )
                || additional.iter().any(|edit: &ProjectTagSelectionEdit| {
                    ranges_overlap(orig_start, orig_end, edit.start, edit.end)
                })
            {
                continue;
            }
            additional.push(ProjectTagSelectionEdit {
                start: orig_start,
                end: orig_end,
                new_text: String::new(),
            });
        }
        additional.sort_by_key(|edit| (edit.start, edit.end));
        (primary, additional)
    } else {
        // No existing target: insert at the segment's leading project-tag
        // position (after leading whitespace and `%directive` tokens).
        let insert_at_base =
            segment_leading_insert_offset(&base, segment_start, segment_end);
        let insert_at = to_original(insert_at_base);
        if insert_at == delete_start {
            // The trigger occupies the destination: merge into one edit so
            // the primary never overlaps an additional.
            return (
                ProjectTagSelectionEdit {
                    start: delete_start,
                    end: delete_end,
                    new_text: insertion.to_string(),
                },
                Vec::new(),
            );
        }
        (
            ProjectTagSelectionEdit {
                start: delete_start,
                end: delete_end,
                new_text: String::new(),
            },
            vec![ProjectTagSelectionEdit {
                start: insert_at,
                end: insert_at,
                new_text: insertion.to_string(),
            }],
        )
    }
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

/// Delete a span, collapsing one adjacent horizontal space the way the
/// historical `_strip_trigger_token` did.
///
/// Only horizontal whitespace (space or tab) around the ref is ever
/// eaten: a ref at end of line keeps its newline so neighbors never join.
/// A ref alone on its line removes the whole line (with its line break)
/// instead of leaving a blank line behind.
fn orphan_strip_region(text: &str, start: usize, end: usize) -> (usize, usize) {
    if let Some(region) = lone_line_region(text, start, end) {
        return region;
    }
    let before_hspace =
        start > 0 && matches!(text.as_bytes()[start - 1], b' ' | b'\t');
    let after_hspace = matches!(text.as_bytes().get(end), Some(b' ' | b'\t'));
    let after_eol = end >= text.len() || text[end..].starts_with(['\r', '\n']);
    let before_bol = start == 0 || text[..start].ends_with(['\r', '\n']);

    if before_hspace && after_hspace {
        (start, end + 1)
    } else if before_hspace && after_eol {
        (start - 1, end)
    } else if after_hspace && before_bol {
        (start, end + 1)
    } else {
        (start, end)
    }
}

/// When the ref is the only non-blank content on its line, delete the
/// whole line with one line break. Returns `None` when other text shares
/// the line.
fn lone_line_region(
    text: &str,
    start: usize,
    end: usize,
) -> Option<(usize, usize)> {
    let line_start = text[..start].rfind('\n').map_or(0, |index| index + 1);
    if !text[line_start..start]
        .chars()
        .all(|ch| ch == ' ' || ch == '\t')
    {
        return None;
    }
    let after = &text[end..];
    let trailing_ws: usize = after
        .chars()
        .take_while(|ch| *ch == ' ' || *ch == '\t')
        .map(char::len_utf8)
        .sum();
    let after_ws = &after[trailing_ws..];
    if after_ws.starts_with("\r\n") {
        Some((line_start, end + trailing_ws + 2))
    } else if after_ws.starts_with('\n') {
        Some((line_start, end + trailing_ws + 1))
    } else if after_ws.is_empty() {
        // Last line with no trailing break: take the preceding break too.
        // `line_start` sits just past a `\n` here, or at zero.
        Some((line_start.saturating_sub(1), text.len()))
    } else {
        None
    }
}

/// Where a selected row goes when its segment has no workspace target: the
/// segment's leading project-tag position, after leading whitespace and
/// leading `%directive` tokens.
///
/// Mirrors the pre-regression prepend behavior (`find_vcs_workflow_tag_prepend_offset`
/// and the segment normalizers): the segment start already sits past any
/// `---` separator (frontmatter delimiters split segments too), so only
/// whitespace and directives remain before the insertion point.
fn segment_leading_insert_offset(
    text: &str,
    segment_start: usize,
    segment_end: usize,
) -> usize {
    let segment_start = segment_start.min(text.len());
    let segment_end = segment_end.min(text.len()).max(segment_start);
    let segment = &text[segment_start..segment_end];
    let leading_ws = segment.len() - segment.trim_start().len();
    let after_ws = &segment[leading_ws..];
    let directive_len = directive_prefix_len(after_ws);
    segment_start + leading_ws + directive_len
}

/// Byte length of the leading `%directive` prefix in `body` (which must
/// already start past whitespace). Each directive needs trailing whitespace,
/// matching Python's `_DIRECTIVE_PREFIX_RE`.
fn directive_prefix_len(body: &str) -> usize {
    static RE: OnceLock<Regex> = OnceLock::new();
    let pattern = RE.get_or_init(|| {
        Regex::new(r"^(?:%[^\s(]+(?:\((?:[^()]|\([^()]*\))*\))?[\s]+)+")
            .expect("valid directive prefix pattern")
    });
    pattern.find(body).map_or(0, |hit| hit.end())
}

/// VCS workflow ref cores inside `[segment_start, segment_end)`, matching
/// the Python one-target guard (`find_vcs_workflow_tag_span`).
///
/// A ref starts at text start or after whitespace (the embedded-tag rule),
/// so mid-line refs like `fix in #gh:foo now` count. Leading `%directive`
/// tokens are never part of the span, so they survive deletion. The span
/// covers the ref itself, never the trailing boundary whitespace, so a
/// ref at end of line keeps its newline. Callers still scope to the
/// trigger's `---` segment and skip literal zones.
fn workspace_ref_deletions(
    text: &str,
    segment_start: usize,
    segment_end: usize,
    workflow_names: &[String],
) -> Vec<(usize, usize)> {
    let Some(pattern) = workspace_target_ref_regex(workflow_names) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut pos = 0;
    while pos <= text.len() {
        let Some(caps) = pattern.captures_at(text, pos) else {
            break;
        };
        let whole = caps.get(0).expect("pattern always matches");
        let start = whole.start();
        // Group 1 is the ref itself; the trailing `(?:\s|$)` only proves
        // the ref ends at a token boundary and is never deleted.
        let end = caps.get(1).map_or(whole.end(), |span| span.end());
        // A rejected glued match can swallow a valid ref inside it (the
        // left boundary is checked after matching because the `regex`
        // crate has no lookbehind), so resume at the next byte instead of
        // skipping the whole match. Matches always start at the ASCII
        // `#`, so `start + 1` is a character boundary.
        if !is_ref_left_boundary(text, start) {
            pos = start + 1;
            continue;
        }
        pos = whole.end().max(start + 1);
        if start < segment_start || end > segment_end || start >= end {
            continue;
        }
        out.push((start, end));
    }
    out
}

/// The historical VCS-tag replace pattern, now used to *find* other
/// workspace targets for deletion.
///
/// Mirrors Python `get_embedded_vcs_tag_pattern`: the `#` must sit at text
/// start or after whitespace. The regex itself matches from the `#`; the
/// left boundary is enforced in `workspace_ref_deletions` because the
/// `regex` crate has no lookbehind. Group 1 covers the ref itself, while
/// the trailing `(?:\s|$)` only checks the right boundary. Returns `None`
/// when `workflow_names` is empty so an empty alternation can never match
/// `# Heading`.
fn workspace_target_ref_regex(workflow_names: &[String]) -> Option<Regex> {
    let mut names: Vec<&str> = workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_unstable();
    let alternation = names
        .iter()
        .map(|name| regex::escape(name))
        .collect::<Vec<_>>()
        .join("|");
    let pattern = format!(
        r"#((?:{alternation})(?:!!|\?\?)?(?:\([^)]*\)|\+|[_:][^\s]*|))(?:\s|$)"
    );
    Some(Regex::new(&pattern).expect("valid workspace target pattern"))
}

/// Whether a `#` ref at byte offset `hash` sits at text start or directly
/// after whitespace, mirroring Python `(?:^|(?<=\s))`.
fn is_ref_left_boundary(text: &str, hash: usize) -> bool {
    if hash == 0 {
        return true;
    }
    text[..hash]
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_whitespace())
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
