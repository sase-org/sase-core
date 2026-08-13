use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnippetExpansionPlan {
    pub text: String,
    pub tabstop_offsets: Vec<usize>,
}

pub fn plan_snippet_expansion(
    template: &str,
    line_indent: &str,
    indent_continuation_lines: bool,
) -> SnippetExpansionPlan {
    let mut markers = Vec::<(usize, usize)>::new();
    let mut cleaned_parts = Vec::<String>::new();
    let mut last_end = 0usize;
    let mut cleaned_offset = 0usize;
    let mut seen = BTreeSet::<usize>::new();

    for (marker_start, marker_end, number) in iter_unescaped_tabstops(template)
    {
        let before =
            unescape_literal_dollars(&template[last_end..marker_start]);
        cleaned_offset += before.chars().count();
        cleaned_parts.push(before);
        if seen.insert(number) {
            markers.push((number, cleaned_offset));
        }
        last_end = marker_end;
    }

    cleaned_parts.push(unescape_literal_dollars(&template[last_end..]));
    let mut text = cleaned_parts.concat();

    if indent_continuation_lines
        && !line_indent.is_empty()
        && text.contains('\n')
    {
        let pre_indent = text;
        text = indent_continuation_lines_to(&pre_indent, line_indent);
        let indent_len = line_indent.chars().count();
        markers = markers
            .into_iter()
            .map(|(number, offset)| {
                (
                    number,
                    offset
                        + newlines_before_char_offset(&pre_indent, offset)
                            * indent_len,
                )
            })
            .collect();
    }

    if markers.is_empty() {
        return SnippetExpansionPlan {
            text,
            tabstop_offsets: Vec::new(),
        };
    }

    if !seen.contains(&0) {
        markers.push((0, text.chars().count()));
    }

    markers.sort_by_key(|(number, _)| (*number == 0, *number));

    SnippetExpansionPlan {
        text,
        tabstop_offsets: markers
            .into_iter()
            .map(|(_, offset)| offset)
            .collect(),
    }
}

pub(crate) fn iter_unescaped_tabstops(
    text: &str,
) -> Vec<(usize, usize, usize)> {
    let mut tabstops = Vec::new();
    let bytes = text.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'$' || is_escaped(text, cursor) {
            cursor += 1;
            continue;
        }
        let digit_start = cursor + 1;
        let mut digit_end = digit_start;
        while digit_end < bytes.len() && bytes[digit_end].is_ascii_digit() {
            digit_end += 1;
        }
        if digit_end == digit_start {
            cursor += 1;
            continue;
        }
        if let Ok(number) = text[digit_start..digit_end].parse::<usize>() {
            tabstops.push((cursor, digit_end, number));
        }
        cursor = digit_end;
    }
    tabstops
}

fn unescape_literal_dollars(text: &str) -> String {
    text.replace("\\$", "$")
}

fn indent_continuation_lines_to(text: &str, indent: &str) -> String {
    let mut lines = text.split('\n');
    let mut rendered = lines.next().unwrap_or_default().to_string();
    for line in lines {
        rendered.push('\n');
        rendered.push_str(indent);
        rendered.push_str(line);
    }
    rendered
}

fn newlines_before_char_offset(text: &str, offset: usize) -> usize {
    text.chars()
        .take(offset)
        .filter(|character| *character == '\n')
        .count()
}

fn is_escaped(text: &str, index: usize) -> bool {
    let mut backslashes = 0usize;
    let mut cursor = index;
    let bytes = text.as_bytes();
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        backslashes += 1;
        cursor -= 1;
    }
    backslashes % 2 == 1
}

/// Bump when the shape of [`SnippetSessionState`] changes in a way that
/// breaks round-tripping older serialized states.
pub const SNIPPET_SESSION_SCHEMA_VERSION: u32 = 1;

/// Nesting depth cap. Overflow drops the outermost session rather than
/// refusing to expand, because a runaway stack is worse than a lost outer
/// snippet.
const MAX_SESSION_DEPTH: usize = 8;

/// One tabstop in the flat, document-ordered stop list, tagged with the
/// session that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetStop {
    pub offset: usize,
    pub session: u32,
}

/// The document span of one live expansion, used only for the nesting
/// containment test and for identifying a session's stops on removal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetSpan {
    pub id: u32,
    pub start: usize,
    pub end: usize,
}

/// A nested snippet session: one flat ordered stop list with a single
/// cursor, plus the session spans needed to decide nesting. An empty
/// `stops` list means no active session; [`SnippetSessionState::empty`] is
/// the canonical representation of that state, and every transition that
/// would otherwise leave `stops` empty returns exactly that value so state
/// equality can be used to test for "no active session".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetSessionState {
    pub schema_version: u32,
    pub stops: Vec<SnippetStop>,
    pub index: usize,
    pub sessions: Vec<SnippetSpan>,
    pub next_session_id: u32,
}

impl SnippetSessionState {
    pub fn empty() -> Self {
        Self {
            schema_version: SNIPPET_SESSION_SCHEMA_VERSION,
            stops: Vec::new(),
            index: 0,
            sessions: Vec::new(),
            next_session_id: 0,
        }
    }

    pub fn is_active(&self) -> bool {
        !self.stops.is_empty()
    }
}

impl Default for SnippetSessionState {
    fn default() -> Self {
        Self::empty()
    }
}

/// Apply a new expansion over document range `[range_start, range_end)`.
///
/// Nests inside the active session when the range is fully contained in
/// the innermost session's span; otherwise resets, dropping every prior
/// session and stop. A plan with no stops never starts a session: nested,
/// it leaves the enclosing session untouched; not nested, it resets to no
/// session at all, matching "no markers, no session".
pub fn expand(
    mut state: SnippetSessionState,
    range_start: usize,
    range_end: usize,
    plan: &SnippetExpansionPlan,
) -> SnippetSessionState {
    let contained = state.sessions.last().is_some_and(|innermost| {
        range_start >= innermost.start && range_end <= innermost.end
    });

    if plan.tabstop_offsets.is_empty() {
        return if contained {
            state
        } else {
            SnippetSessionState::empty()
        };
    }

    let session_id = state.next_session_id;
    let new_span = SnippetSpan {
        id: session_id,
        start: range_start,
        end: range_end,
    };
    let new_stops: Vec<SnippetStop> = plan
        .tabstop_offsets
        .iter()
        .map(|&relative_offset| SnippetStop {
            offset: range_start + relative_offset,
            session: session_id,
        })
        .collect();

    if contained {
        let insert_at = state.index + 1;
        state.stops.splice(insert_at..insert_at, new_stops);
        state.index = insert_at;
        state.sessions.push(new_span);
    } else {
        state.stops = new_stops;
        state.index = 0;
        state.sessions = vec![new_span];
    }
    state.next_session_id = session_id + 1;

    if state.sessions.len() > MAX_SESSION_DEPTH {
        if let Some(outermost_id) = state.sessions.first().map(|span| span.id) {
            remove_session(&mut state, outermost_id);
        }
    }

    state
}

/// Move the cursor to the next stop. Advancing off the last stop ends the
/// session (clearing all state) and reports no target, exactly like
/// tabbing off the end of a single un-nested snippet does today.
pub fn advance(
    mut state: SnippetSessionState,
) -> (SnippetSessionState, Option<usize>) {
    if state.stops.is_empty() {
        return (state, None);
    }
    if state.index + 1 < state.stops.len() {
        state.index += 1;
        let offset = state.stops[state.index].offset;
        (state, Some(offset))
    } else {
        (SnippetSessionState::empty(), None)
    }
}

/// Move the cursor to the previous stop. Retreating at the first stop
/// reports no target but leaves the session active, so the caller can
/// still consume the keypress without disturbing the session.
pub fn retreat(
    mut state: SnippetSessionState,
) -> (SnippetSessionState, Option<usize>) {
    if state.stops.is_empty() || state.index == 0 {
        return (state, None);
    }
    state.index -= 1;
    let offset = state.stops[state.index].offset;
    (state, Some(offset))
}

/// Remap every stored offset for a document edit `[edit_start, edit_end)`
/// that inserted `inserted_len` characters. Stops are sticky-right so
/// typing at a stop grows past it; session spans are sticky-left on
/// `start` and sticky-right on `end` so a session keeps containing
/// everything typed inside it. Sessions whose span collapses or that lose
/// every stop are dropped; if that empties the stop list, all state is
/// cleared.
pub fn apply_edit(
    mut state: SnippetSessionState,
    edit_start: usize,
    edit_end: usize,
    inserted_len: usize,
) -> SnippetSessionState {
    if !state.is_active() {
        return state;
    }
    let (edit_start, edit_end) = if edit_start <= edit_end {
        (edit_start, edit_end)
    } else {
        (edit_end, edit_start)
    };

    for stop in &mut state.stops {
        stop.offset =
            remap_sticky_right(stop.offset, edit_start, edit_end, inserted_len);
    }
    for session in &mut state.sessions {
        session.start = remap_sticky_left(
            session.start,
            edit_start,
            edit_end,
            inserted_len,
        );
        session.end =
            remap_sticky_right(session.end, edit_start, edit_end, inserted_len);
    }

    let stale_session_ids: Vec<u32> = state
        .sessions
        .iter()
        .filter(|session| {
            session.end <= session.start
                || !state.stops.iter().any(|stop| stop.session == session.id)
        })
        .map(|session| session.id)
        .collect();

    for session_id in stale_session_ids {
        if !state.is_active() {
            break;
        }
        remove_session(&mut state, session_id);
    }

    state
}

/// End the session unconditionally, e.g. on `load_text` or leaving INSERT
/// mode.
pub fn clear(_state: SnippetSessionState) -> SnippetSessionState {
    SnippetSessionState::empty()
}

/// Wire-shaped event driving the session engine's single entry point,
/// tagged by `kind` so a caller across the PyO3 boundary has exactly one
/// binding to call. Each variant mirrors one of the pure functions above:
/// `Plan` is the exception, a stateless call over
/// [`plan_snippet_expansion`] rather than a state transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnippetSessionEvent {
    Plan {
        template: String,
        line_indent: String,
        indent_continuation_lines: bool,
    },
    Expand {
        range_start: usize,
        range_end: usize,
        tabstop_offsets: Vec<usize>,
    },
    Advance,
    Retreat,
    ApplyEdit {
        edit_start: usize,
        edit_end: usize,
        inserted_len: usize,
    },
    Clear,
}

/// Result of [`apply_session_event`]. Rectangular: every field is always
/// present, `None`/empty when the event kind does not populate it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnippetSessionEventResult {
    pub state: SnippetSessionState,
    pub cursor_offset: Option<usize>,
    pub text: Option<String>,
    pub tabstop_offsets: Vec<usize>,
}

/// The session engine's single entry point: apply one wire event to the
/// current state and report the new state plus whatever the event
/// resolved.
///
/// `Expand` reports the offset the cursor now sits at (the new state's
/// current stop) as a convenience for the caller, even though the pure
/// [`expand`] transition itself has no notion of "cursor offset". `Plan`
/// echoes `state` back unchanged, since planning is stateless.
pub fn apply_session_event(
    state: SnippetSessionState,
    event: SnippetSessionEvent,
) -> SnippetSessionEventResult {
    match event {
        SnippetSessionEvent::Plan {
            template,
            line_indent,
            indent_continuation_lines,
        } => {
            let planned = plan_snippet_expansion(
                &template,
                &line_indent,
                indent_continuation_lines,
            );
            SnippetSessionEventResult {
                state,
                cursor_offset: None,
                text: Some(planned.text),
                tabstop_offsets: planned.tabstop_offsets,
            }
        }
        SnippetSessionEvent::Expand {
            range_start,
            range_end,
            tabstop_offsets,
        } => {
            let plan = SnippetExpansionPlan {
                text: String::new(),
                tabstop_offsets,
            };
            let state = expand(state, range_start, range_end, &plan);
            let cursor_offset =
                state.stops.get(state.index).map(|stop| stop.offset);
            SnippetSessionEventResult {
                state,
                cursor_offset,
                text: None,
                tabstop_offsets: Vec::new(),
            }
        }
        SnippetSessionEvent::Advance => {
            let (state, cursor_offset) = advance(state);
            SnippetSessionEventResult {
                state,
                cursor_offset,
                text: None,
                tabstop_offsets: Vec::new(),
            }
        }
        SnippetSessionEvent::Retreat => {
            let (state, cursor_offset) = retreat(state);
            SnippetSessionEventResult {
                state,
                cursor_offset,
                text: None,
                tabstop_offsets: Vec::new(),
            }
        }
        SnippetSessionEvent::ApplyEdit {
            edit_start,
            edit_end,
            inserted_len,
        } => {
            let state = apply_edit(state, edit_start, edit_end, inserted_len);
            SnippetSessionEventResult {
                state,
                cursor_offset: None,
                text: None,
                tabstop_offsets: Vec::new(),
            }
        }
        SnippetSessionEvent::Clear => SnippetSessionEventResult {
            state: clear(state),
            cursor_offset: None,
            text: None,
            tabstop_offsets: Vec::new(),
        },
    }
}

/// Drop `session_id` and every stop it produced, wherever they occur in
/// the flat list, and repoint the cursor at the nearest surviving stop at
/// or before its old position (falling back to the first surviving stop).
/// If nothing survives, the canonical empty state is installed.
fn remove_session(state: &mut SnippetSessionState, session_id: u32) {
    let old_index = state.index;
    let mut new_stops = Vec::with_capacity(state.stops.len());
    let mut new_index: Option<usize> = None;
    for (i, stop) in state.stops.iter().enumerate() {
        if stop.session == session_id {
            continue;
        }
        new_stops.push(stop.clone());
        if i <= old_index {
            new_index = Some(new_stops.len() - 1);
        }
    }

    if new_stops.is_empty() {
        *state = SnippetSessionState::empty();
        return;
    }

    state.sessions.retain(|span| span.id != session_id);
    state.stops = new_stops;
    state.index = new_index.unwrap_or(0);
}

/// `offset < edit_start` stays put; `offset >= edit_end` shifts by the
/// edit's net length delta; an offset inside the deleted range clamps to
/// just past the inserted text.
fn remap_sticky_right(
    offset: usize,
    edit_start: usize,
    edit_end: usize,
    inserted_len: usize,
) -> usize {
    if offset < edit_start {
        offset
    } else if offset >= edit_end {
        offset - (edit_end - edit_start) + inserted_len
    } else {
        edit_start + inserted_len
    }
}

/// Same as [`remap_sticky_right`], except an offset sitting exactly at
/// `edit_start` stays put instead of shifting, and an offset inside the
/// deleted range clamps to just before the inserted text.
fn remap_sticky_left(
    offset: usize,
    edit_start: usize,
    edit_end: usize,
    inserted_len: usize,
) -> usize {
    if offset <= edit_start {
        offset
    } else if offset >= edit_end {
        offset - (edit_end - edit_start) + inserted_len
    } else {
        edit_start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(template: &str) -> SnippetExpansionPlan {
        plan_snippet_expansion(template, "", true)
    }

    #[test]
    fn escaped_dollars_are_literal_and_not_tabstops() {
        let planned = plan(r"\$1 before $1 after \$0 then $0");

        assert_eq!(planned.text, "$1 before  after $0 then ");
        assert_eq!(planned.tabstop_offsets, vec![10, 25]);
    }

    #[test]
    fn repeated_tabstop_numbers_only_create_one_stop() {
        let planned = plan("$1 a $1 b $2 $0");

        assert_eq!(planned.text, " a  b  ");
        assert_eq!(planned.tabstop_offsets, vec![0, 6, 7]);
    }

    #[test]
    fn missing_zero_appends_implicit_final_stop() {
        let planned = plan("a $2 b $1 c");

        assert_eq!(planned.text, "a  b  c");
        assert_eq!(planned.tabstop_offsets, vec![5, 2, 7]);
    }

    #[test]
    fn templates_without_markers_have_no_stops() {
        let planned = plan(r"cost \$5 and $x");

        assert_eq!(planned.text, "cost $5 and $x");
        assert!(planned.tabstop_offsets.is_empty());
    }

    #[test]
    fn multi_digit_tabstops_sort_by_number() {
        let planned = plan("$10 ten $2 two $0");

        assert_eq!(planned.text, " ten  two ");
        assert_eq!(planned.tabstop_offsets, vec![5, 0, 10]);
    }

    #[test]
    fn continuation_lines_are_indented_and_offsets_shift() {
        let planned = plan_snippet_expansion("a\n$1\nb $2", "  ", true);

        assert_eq!(planned.text, "a\n  \n  b ");
        assert_eq!(planned.tabstop_offsets, vec![4, 9, 9]);
    }

    #[test]
    fn continuation_line_indentation_can_be_disabled() {
        let planned = plan_snippet_expansion("a\n$1\nb $2", "  ", false);

        assert_eq!(planned.text, "a\n\nb ");
        assert_eq!(planned.tabstop_offsets, vec![2, 5, 5]);
    }

    #[test]
    fn offsets_are_character_offsets_not_byte_offsets() {
        let planned = plan("α $1 β $0");

        assert_eq!(planned.text, "α  β ");
        assert_eq!(planned.tabstop_offsets, vec![2, 5]);
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use serde_json::json;

    fn plan(offsets: &[usize]) -> SnippetExpansionPlan {
        SnippetExpansionPlan {
            text: String::new(),
            tabstop_offsets: offsets.to_vec(),
        }
    }

    #[test]
    fn nesting_at_a_stop_resumes_outer_session_after_inner_exhausts() {
        // The reported bug: expand `foo $1 bar $2 baz $3 buz`, advance to
        // $2, nest a second snippet there, exhaust the inner stops, and
        // the next advance must land on the outer $3, not fall off the
        // end of a discarded session.
        let outer_plan =
            plan_snippet_expansion("foo $1 bar $2 baz $3 buz", "", true);
        assert_eq!(outer_plan.tabstop_offsets, vec![4, 9, 14, 18]);

        let state = expand(SnippetSessionState::empty(), 0, 100, &outer_plan);
        let (state, offset) = advance(state);
        assert_eq!(offset, Some(9));

        let inner_plan = plan_snippet_expansion("inner $1 done", "", true);
        let state = expand(state, 8, 19, &inner_plan);
        assert_eq!(state.index, 2);
        assert_eq!(state.stops[state.index].offset, 14);

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(19), "inner's own last stop");

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(14), "resumes outer's $3 after inner exhausts");

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(18), "resumes outer's $0");

        let (state, offset) = advance(state);
        assert_eq!(
            offset, None,
            "advancing off the last stop ends the session"
        );
        assert_eq!(state, SnippetSessionState::empty());
    }

    #[test]
    fn expand_outside_active_span_resets_instead_of_nesting() {
        let state = expand(SnippetSessionState::empty(), 5, 20, &plan(&[0, 2]));
        assert_eq!(state.sessions.len(), 1);
        assert_eq!(state.sessions[0].id, 0);

        // A whole-document replacement can never be contained in an
        // existing expansion, so it must reset rather than nest.
        let state = expand(state, 0, 100, &plan(&[0, 3]));

        assert_eq!(state.sessions.len(), 1);
        assert_eq!(
            state.sessions[0],
            SnippetSpan {
                id: 1,
                start: 0,
                end: 100
            }
        );
        assert_eq!(
            state.stops,
            vec![
                SnippetStop {
                    offset: 0,
                    session: 1
                },
                SnippetStop {
                    offset: 3,
                    session: 1
                },
            ]
        );
        assert_eq!(state.index, 0);
    }

    #[test]
    fn nesting_a_plan_with_no_stops_leaves_the_enclosing_session_untouched() {
        let state = expand(SnippetSessionState::empty(), 0, 20, &plan(&[2, 8]));
        let before = state.clone();

        // A trigger that expands to a static snippet (no tabstops at all)
        // must not start a session or disturb the one it lands inside.
        let empty_plan = SnippetExpansionPlan {
            text: "static".to_string(),
            tabstop_offsets: vec![],
        };
        let state = expand(state, 5, 5, &empty_plan);

        assert_eq!(state, before);
    }

    #[test]
    fn expanding_a_no_stop_plan_outside_any_session_clears_state() {
        let state = expand(SnippetSessionState::empty(), 0, 20, &plan(&[2, 8]));
        assert!(state.is_active());

        let empty_plan = SnippetExpansionPlan {
            text: "static".to_string(),
            tabstop_offsets: vec![],
        };
        // Not contained in the active session, so this must reset -- and
        // since the new plan has no stops, resetting means clearing.
        let state = expand(state, 50, 55, &empty_plan);

        assert_eq!(state, SnippetSessionState::empty());
    }

    #[test]
    fn two_levels_of_nesting_resume_enclosing_sessions_in_order() {
        let outer = plan(&[2, 8, 14]);
        let middle = plan(&[1, 5]);
        let inner = plan(&[0, 2]);

        let state = expand(SnippetSessionState::empty(), 0, 20, &outer);
        let (state, offset) = advance(state);
        assert_eq!(offset, Some(8));

        let state = expand(state, 8, 16, &middle);
        assert_eq!(state.index, 2);

        let state = expand(state, 9, 11, &inner);
        assert_eq!(state.index, 3);

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(11), "inner's own last stop");

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(13), "resumes middle's remaining stop");

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(14), "resumes outer's remaining stop");

        let (state, offset) = advance(state);
        assert_eq!(offset, None);
        assert_eq!(state, SnippetSessionState::empty());
    }

    #[test]
    fn depth_cap_drops_outermost_session_on_overflow() {
        let one_stop = plan(&[0]);
        let mut state = expand(SnippetSessionState::empty(), 0, 5, &one_stop);
        for _ in 0..8 {
            state = expand(state, 0, 5, &one_stop);
        }

        let ids: Vec<u32> = state.sessions.iter().map(|span| span.id).collect();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            "outermost session (id 0) dropped"
        );
        assert!(state.stops.iter().all(|stop| stop.session != 0));
        assert_eq!(state.stops.len(), 8);
    }

    #[test]
    fn retreat_at_start_and_advance_past_end_report_no_target() {
        let state = expand(SnippetSessionState::empty(), 0, 10, &plan(&[0, 4]));

        let (state, offset) = retreat(state);
        assert_eq!(offset, None, "retreat at index 0 has no target");
        assert_eq!(state.index, 0, "but the session stays active");

        let (state, offset) = advance(state);
        assert_eq!(offset, Some(4));

        let (state, offset) = advance(state);
        assert_eq!(offset, None, "advance off the last stop has no target");
        assert_eq!(
            state,
            SnippetSessionState::empty(),
            "and clears the session"
        );
    }

    #[test]
    fn advance_and_retreat_on_an_inactive_session_are_no_ops() {
        let (state, offset) = advance(SnippetSessionState::empty());
        assert_eq!(offset, None);
        assert_eq!(state, SnippetSessionState::empty());

        let (state, offset) = retreat(SnippetSessionState::empty());
        assert_eq!(offset, None);
        assert_eq!(state, SnippetSessionState::empty());
    }

    #[test]
    fn apply_edit_remaps_stops_before_at_inside_and_after_the_edit() {
        let state =
            expand(SnippetSessionState::empty(), 0, 30, &plan(&[5, 10, 20]));

        // Insert after every stop: nothing before the edit moves, but the
        // containing session's end still grows to keep containing it.
        let state = apply_edit(state, 25, 25, 3);
        assert_eq!(
            state.stops,
            vec![
                SnippetStop {
                    offset: 5,
                    session: 0
                },
                SnippetStop {
                    offset: 10,
                    session: 0
                },
                SnippetStop {
                    offset: 20,
                    session: 0
                },
            ]
        );
        assert_eq!(
            state.sessions[0],
            SnippetSpan {
                id: 0,
                start: 0,
                end: 33
            }
        );

        // Insert exactly at a stop: sticky-right means it grows past the
        // stop rather than pushing the stop ahead of the typed text.
        let state = apply_edit(state, 10, 10, 4);
        assert_eq!(
            state.stops[1],
            SnippetStop {
                offset: 14,
                session: 0
            }
        );
        assert_eq!(
            state.stops[0],
            SnippetStop {
                offset: 5,
                session: 0
            },
            "stop before the edit is untouched"
        );
        assert_eq!(
            state.stops[2],
            SnippetStop {
                offset: 24,
                session: 0
            },
            "stop after the edit shifts"
        );

        // Delete a range that spans a stop: the stop clamps to just past
        // the (empty) inserted text rather than landing inside deleted
        // content.
        let state = apply_edit(state, 12, 16, 0);
        assert_eq!(
            state.stops[1],
            SnippetStop {
                offset: 12,
                session: 0
            },
            "clamped into the deletion point"
        );
        assert_eq!(
            state.stops[2],
            SnippetStop {
                offset: 20,
                session: 0
            },
            "shifts left by the deleted span"
        );
    }

    #[test]
    fn apply_edit_deletes_offsets_falling_before_the_first_stop() {
        let state =
            expand(SnippetSessionState::empty(), 10, 20, &plan(&[0, 8]));

        // A deletion entirely before the session's start shifts
        // everything left, including the session span itself.
        let state = apply_edit(state, 0, 5, 0);

        assert_eq!(
            state.sessions[0],
            SnippetSpan {
                id: 0,
                start: 5,
                end: 15
            }
        );
        assert_eq!(
            state.stops,
            vec![
                SnippetStop {
                    offset: 5,
                    session: 0
                },
                SnippetStop {
                    offset: 13,
                    session: 0
                },
            ]
        );
    }

    #[test]
    fn deleting_a_whole_nested_expansion_drops_it_without_corrupting_the_enclosing_session(
    ) {
        let outer = plan(&[2, 25]);
        let inner = plan(&[0, 5]);

        let state = expand(SnippetSessionState::empty(), 0, 30, &outer);
        let state = expand(state, 10, 20, &inner);
        assert_eq!(state.sessions.len(), 2);

        // Delete exactly the inner expansion's span.
        let state = apply_edit(state, 10, 20, 0);

        assert_eq!(
            state.sessions,
            vec![SnippetSpan {
                id: 0,
                start: 0,
                end: 20
            }]
        );
        assert_eq!(
            state.stops,
            vec![
                SnippetStop {
                    offset: 2,
                    session: 0
                },
                SnippetStop {
                    offset: 15,
                    session: 0
                },
            ]
        );
        assert_eq!(state.index, 0);
    }

    #[test]
    fn apply_edit_on_an_inactive_session_is_a_no_op() {
        let state = apply_edit(SnippetSessionState::empty(), 0, 5, 3);
        assert_eq!(state, SnippetSessionState::empty());
    }

    #[test]
    fn apply_edit_normalizes_unordered_edit_bounds() {
        let sorted =
            expand(SnippetSessionState::empty(), 0, 30, &plan(&[5, 20]));
        let unsorted = sorted.clone();

        let sorted = apply_edit(sorted, 10, 15, 2);
        let unsorted = apply_edit(unsorted, 15, 10, 2);

        assert_eq!(sorted, unsorted);
    }

    #[test]
    fn clear_ends_an_active_session() {
        let state = expand(SnippetSessionState::empty(), 0, 10, &plan(&[0, 4]));
        assert!(state.is_active());

        assert_eq!(clear(state), SnippetSessionState::empty());
    }

    #[test]
    fn state_serializes_with_the_documented_field_order() {
        let state =
            expand(SnippetSessionState::empty(), 10, 60, &plan(&[32, 37]));

        let value = serde_json::to_value(&state).unwrap();
        let expected_order = [
            "schema_version",
            "stops",
            "index",
            "sessions",
            "next_session_id",
        ];
        let keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, expected_order);

        let round_tripped: SnippetSessionState =
            serde_json::from_value(value).unwrap();
        assert_eq!(round_tripped, state);
    }

    #[test]
    fn event_kind_tags_match_the_documented_snake_case_names() {
        let events = [
            (
                json!({
                    "kind": "plan",
                    "template": "a $1 b",
                    "line_indent": "",
                    "indent_continuation_lines": true
                }),
                "Plan",
            ),
            (
                json!({
                    "kind": "expand",
                    "range_start": 0,
                    "range_end": 5,
                    "tabstop_offsets": [1, 3]
                }),
                "Expand",
            ),
            (json!({"kind": "advance"}), "Advance"),
            (json!({"kind": "retreat"}), "Retreat"),
            (
                json!({
                    "kind": "apply_edit",
                    "edit_start": 0,
                    "edit_end": 1,
                    "inserted_len": 2
                }),
                "ApplyEdit",
            ),
            (json!({"kind": "clear"}), "Clear"),
        ];

        for (value, expected_variant) in events {
            let event: SnippetSessionEvent =
                serde_json::from_value(value).unwrap();
            let debug = format!("{event:?}");
            assert!(
                debug.starts_with(expected_variant),
                "expected {debug} to start with {expected_variant}"
            );
        }
    }

    #[test]
    fn apply_session_event_plan_is_stateless_and_echoes_state_unchanged() {
        let state = expand(SnippetSessionState::empty(), 0, 10, &plan(&[0, 4]));

        let result = apply_session_event(
            state.clone(),
            SnippetSessionEvent::Plan {
                template: "foo $1 bar $2 baz $3 buz".to_string(),
                line_indent: String::new(),
                indent_continuation_lines: true,
            },
        );

        assert_eq!(result.state, state, "planning must not touch the session");
        assert_eq!(result.cursor_offset, None);
        assert_eq!(result.text, Some("foo  bar  baz  buz".to_string()));
        assert_eq!(result.tabstop_offsets, vec![4, 9, 14, 18]);
    }

    #[test]
    fn apply_session_event_expand_reports_the_new_cursor_offset() {
        let empty = SnippetSessionState::empty();

        let expanded = apply_session_event(
            empty,
            SnippetSessionEvent::Expand {
                range_start: 10,
                range_end: 20,
                tabstop_offsets: vec![2, 8],
            },
        );
        assert_eq!(expanded.cursor_offset, Some(12));
        assert!(expanded.state.is_active());

        // A plan with no stops that isn't contained in the active session
        // resets to empty, and the reported cursor offset follows: none.
        let cleared = apply_session_event(
            expanded.state,
            SnippetSessionEvent::Expand {
                range_start: 50,
                range_end: 55,
                tabstop_offsets: vec![],
            },
        );
        assert_eq!(cleared.cursor_offset, None);
        assert_eq!(cleared.state, SnippetSessionState::empty());
    }

    #[test]
    fn apply_session_event_drives_the_reported_bug_scenario_end_to_end() {
        let outer_plan =
            plan_snippet_expansion("foo $1 bar $2 baz $3 buz", "", true);
        let state = SnippetSessionState::empty();

        let result = apply_session_event(
            state,
            SnippetSessionEvent::Expand {
                range_start: 0,
                range_end: 100,
                tabstop_offsets: outer_plan.tabstop_offsets.clone(),
            },
        );
        let result =
            apply_session_event(result.state, SnippetSessionEvent::Advance);
        assert_eq!(result.cursor_offset, Some(9));

        let inner_plan = plan_snippet_expansion("inner $1 done", "", true);
        let result = apply_session_event(
            result.state,
            SnippetSessionEvent::Expand {
                range_start: 8,
                range_end: 19,
                tabstop_offsets: inner_plan.tabstop_offsets,
            },
        );

        let result =
            apply_session_event(result.state, SnippetSessionEvent::Advance);
        assert_eq!(result.cursor_offset, Some(19), "inner's own last stop");

        let result =
            apply_session_event(result.state, SnippetSessionEvent::Advance);
        assert_eq!(
            result.cursor_offset,
            Some(14),
            "resumes outer's $3 after inner exhausts"
        );
    }

    #[test]
    fn apply_session_event_advance_retreat_apply_edit_and_clear_round_trip() {
        let state = expand(SnippetSessionState::empty(), 0, 10, &plan(&[0, 4]));

        let result = apply_session_event(
            state,
            SnippetSessionEvent::ApplyEdit {
                edit_start: 10,
                edit_end: 10,
                inserted_len: 3,
            },
        );
        assert_eq!(result.cursor_offset, None);
        assert_eq!(result.state.sessions[0].end, 13);

        let result =
            apply_session_event(result.state, SnippetSessionEvent::Retreat);
        assert_eq!(result.cursor_offset, None, "already at index 0");

        let result =
            apply_session_event(result.state, SnippetSessionEvent::Advance);
        assert_eq!(result.cursor_offset, Some(4));

        let result =
            apply_session_event(result.state, SnippetSessionEvent::Clear);
        assert_eq!(result.state, SnippetSessionState::empty());
        assert_eq!(result.cursor_offset, None);
    }

    #[test]
    fn event_result_serializes_with_the_documented_field_order() {
        let result = apply_session_event(
            SnippetSessionState::empty(),
            SnippetSessionEvent::Plan {
                template: "$1$0".to_string(),
                line_indent: String::new(),
                indent_continuation_lines: true,
            },
        );

        let value = serde_json::to_value(&result).unwrap();
        let expected_order =
            ["state", "cursor_offset", "text", "tabstop_offsets"];
        let keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, expected_order);
    }
}
