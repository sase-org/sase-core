//! Document scope analysis: declared inputs, skill flag, `%repeat`/`%wait`
//! directives, and position-aware template locals with the open-block stack.
//!
//! Input parsing reuses `diagnostics::parse_local_inputs` (shared, not
//! forked); directive detection reuses the launch directive scanner and
//! drops occurrences inside inert regions and Jinja tags.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};

use super::context::{
    find_top_level_assign, find_top_level_word, first_ident, split_top_level,
};
use super::scan::{
    first_word, floor_char_boundary, scan_jinja_inert_ranges, scan_jinja_tags,
    scan_statement_tags,
};
use crate::agent_launch::directive_occurrences;
use crate::editor::diagnostics::{
    frontmatter_mapping, mapping_get, parse_local_inputs, value_as_string,
};
use crate::editor::exclusion::{frontmatter_block_len, position_in_ranges};
use crate::editor::frontmatter::value_is_truthy;
use crate::prompt_literal_zone_ranges;

/// Prompt scope (a top-level agent prompt) or macro scope (a macro
/// definition body). The assist phase reads this from the request wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaScopeKind {
    Prompt,
    Macro,
}

/// One declared `input:` / `inputs:` entry from frontmatter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JinjaDeclaredInput {
    pub name: String,
    pub type_name: String,
    pub description: Option<String>,
    pub required: bool,
    pub default_display: Option<String>,
    pub choices: Vec<String>,
}

/// The shape a template local was declared with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JinjaLocalKind {
    Set,
    ForTarget,
    Loop,
    MacroName,
    MacroParam,
    MacroSpecial,
    With,
    Import,
}

/// A template local visible at the cursor. Offsets are UTF-8 bytes into the
/// document; `scope_end` is the end of the enclosing block body (or the end
/// of text for top-level names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JinjaTemplateLocal {
    pub name: String,
    pub kind: JinjaLocalKind,
    pub scope_start: usize,
    pub scope_end: usize,
}

/// A block still open at the cursor, innermost first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JinjaOpenBlock {
    pub keyword: String,
    pub closer: String,
}

/// Full document scope for one cursor position.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JinjaDocumentScope {
    pub inputs: Vec<JinjaDeclaredInput>,
    pub skill: bool,
    pub has_repeat: bool,
    pub has_wait: bool,
    pub locals: Vec<JinjaTemplateLocal>,
    pub open_blocks: Vec<JinjaOpenBlock>,
}

/// Collect the document scope at `cursor`.
///
/// `external_frontmatter` is lifted frontmatter supplied alongside the body
/// (the LSP's lifted-frontmatter case); it wins over in-text frontmatter and
/// dedupes by name.
pub fn jinja_document_scope(
    text: &str,
    cursor: usize,
    external_frontmatter: Option<&str>,
) -> JinjaDocumentScope {
    let cursor = floor_cursor(text, cursor);
    let (has_repeat, has_wait) = jinja_directive_presence(text);
    let (locals, open_blocks) = jinja_template_scope(text, cursor);
    JinjaDocumentScope {
        inputs: jinja_declared_inputs(text, external_frontmatter),
        skill: jinja_skill_enabled(text, external_frontmatter),
        has_repeat,
        has_wait,
        locals,
        open_blocks,
    }
}

fn floor_cursor(text: &str, cursor: usize) -> usize {
    floor_char_boundary(text, cursor)
}

/// Declared inputs from the external frontmatter first, then the in-text
/// leading frontmatter; deduped by name. Accepts `input:` and `inputs:`, in
/// shortform and longform.
pub fn jinja_declared_inputs(
    text: &str,
    external_frontmatter: Option<&str>,
) -> Vec<JinjaDeclaredInput> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    if let Some(source) = external_frontmatter {
        if let Some(mapping) = parse_frontmatter_source(source) {
            collect_inputs(&mapping, &mut out, &mut seen);
        }
    }
    if let Some(mapping) = frontmatter_mapping(text) {
        collect_inputs(&mapping, &mut out, &mut seen);
    }
    out
}

/// Truthy `skill` flag from either frontmatter source.
pub fn jinja_skill_enabled(
    text: &str,
    external_frontmatter: Option<&str>,
) -> bool {
    let inline = frontmatter_mapping(text);
    let external = external_frontmatter.and_then(parse_frontmatter_source);
    [external, inline].into_iter().flatten().any(|mapping| {
        mapping_get(&mapping, "skill").is_some_and(value_is_truthy)
    })
}

/// Parse a frontmatter source: either a leading `---` block or raw YAML.
fn parse_frontmatter_source(source: &str) -> Option<Mapping> {
    let mut lines = source.lines();
    if lines.next().is_some_and(|line| line.trim() == "---") {
        frontmatter_mapping(source)
    } else {
        serde_yaml::from_str::<Value>(source)
            .ok()
            .and_then(|value| value.as_mapping().cloned())
    }
}

fn collect_inputs(
    mapping: &Mapping,
    out: &mut Vec<JinjaDeclaredInput>,
    seen: &mut HashSet<String>,
) {
    for key in ["input", "inputs"] {
        let Some(value) = mapping_get(mapping, key) else {
            continue;
        };
        for hint in parse_local_inputs(value) {
            if !seen.insert(hint.name.clone()) {
                continue;
            }
            out.push(JinjaDeclaredInput {
                name: hint.name.clone(),
                type_name: hint.r#type,
                description: hint.description,
                required: hint.required,
                default_display: hint.default_display,
                choices: input_choices(value, &hint.name),
            });
        }
    }
}

/// `choices:` sequences beside the shared shortform/longform parsing: the
/// same value the parser read, keyed by input name.
fn input_choices(value: &Value, name: &str) -> Vec<String> {
    if let Some(mapping) = value.as_mapping() {
        let choices = mapping_get(mapping, name)
            .and_then(|entry| entry.as_mapping())
            .and_then(|entry| mapping_get(entry, "choices"));
        return choices_sequence(choices);
    }
    if let Some(sequence) = value.as_sequence() {
        for item in sequence {
            let Some(mapping) = item.as_mapping() else {
                continue;
            };
            if mapping_get(mapping, "name").and_then(value_as_string)
                != Some(name.to_string())
            {
                continue;
            }
            return choices_sequence(mapping_get(mapping, "choices"));
        }
    }
    Vec::new()
}

fn choices_sequence(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_sequence)
        .map(|sequence| sequence.iter().filter_map(value_as_string).collect())
        .unwrap_or_default()
}

/// Whether `%repeat`/`%r` and `%wait`/`%w` appear outside inert regions and
/// Jinja tags, via the launch directive scanner (no regex here).
pub fn jinja_directive_presence(text: &str) -> (bool, bool) {
    let occurrences = directive_occurrences(text).unwrap_or_default();
    if occurrences.is_empty() {
        return (false, false);
    }
    let mut zones = prompt_literal_zone_ranges(text);
    if let Some(len) = frontmatter_block_len(text) {
        zones.push((0, len));
    }
    zones.extend(scan_jinja_inert_ranges(text));
    zones.extend(
        scan_jinja_tags(text)
            .into_iter()
            .map(|tag| (tag.open_start, tag.tag_end)),
    );
    let mut repeat = false;
    let mut wait = false;
    for occurrence in &occurrences {
        if position_in_ranges(occurrence.start, &zones) {
            continue;
        }
        match occurrence.canonical_name.as_str() {
            "repeat" => repeat = true,
            "wait" => wait = true,
            _ => {}
        }
    }
    (repeat, wait)
}

/// Position-aware locals plus the open-block stack (innermost first).
///
/// Only statement tags fully before the cursor contribute; a tag containing
/// the cursor is still being typed. Locals whose block closed before the
/// cursor are dropped.
pub fn jinja_template_scope(
    text: &str,
    cursor: usize,
) -> (Vec<JinjaTemplateLocal>, Vec<JinjaOpenBlock>) {
    let cursor = floor_cursor(text, cursor);
    let end = text.len();
    let mut frames: Vec<BlockFrame> = Vec::new();
    let mut pending: Vec<PendingLocal> = Vec::new();
    for tag in scan_statement_tags(text) {
        if tag.tag_end > cursor {
            break;
        }
        let content = match text.get(tag.content_start..tag.content_end) {
            Some(content) => content,
            None => continue,
        };
        let Some((keyword, _, keyword_end)) = first_word(content) else {
            continue;
        };
        let rest = &content[keyword_end..];
        let enclosing = innermost_open(&frames);
        match keyword.as_str() {
            "if" => frames.push(BlockFrame::open("if", "endif")),
            "for" => {
                frames.push(BlockFrame::open("for", "endfor"));
                declare_for_targets(
                    rest,
                    tag.tag_end,
                    frames.len() - 1,
                    end,
                    &mut pending,
                );
            }
            "macro" => {
                frames.push(BlockFrame::open("macro", "endmacro"));
                declare_macro(
                    rest,
                    tag.tag_end,
                    frames.len() - 1,
                    enclosing,
                    end,
                    &mut pending,
                );
            }
            "call" => frames.push(BlockFrame::open("call", "endcall")),
            "filter" => {
                frames.push(BlockFrame::open("filter", "endfilter"));
            }
            "with" => {
                frames.push(BlockFrame::open("with", "endwith"));
                declare_with_assignments(
                    rest,
                    tag.tag_end,
                    frames.len() - 1,
                    end,
                    &mut pending,
                );
            }
            "block" => frames.push(BlockFrame::open("block", "endblock")),
            "raw" => frames.push(BlockFrame::open("raw", "endraw")),
            "set" => {
                if find_top_level_assign(content).is_none() {
                    // Block `{% set x %}...{% endset %}`: the name belongs
                    // to the enclosing scope; the frame matches the closer.
                    declare_targets(
                        rest,
                        tag.tag_end,
                        enclosing,
                        end,
                        JinjaLocalKind::Set,
                        &mut pending,
                    );
                    frames.push(BlockFrame::open("set", "endset"));
                } else {
                    declare_targets(
                        &assign_targets(content),
                        tag.tag_end,
                        enclosing,
                        end,
                        JinjaLocalKind::Set,
                        &mut pending,
                    );
                }
            }
            "import" => declare_import_names(
                rest,
                tag.tag_end,
                enclosing,
                end,
                &mut pending,
            ),
            "from" => declare_from_names(
                rest,
                tag.tag_end,
                enclosing,
                end,
                &mut pending,
            ),
            "elif" | "else" => {}
            _ if keyword.starts_with("end") => {
                close_block(
                    &keyword,
                    tag.open_start,
                    &mut frames,
                    &mut pending,
                );
            }
            _ => {}
        }
    }
    let open_blocks = frames
        .iter()
        .rev()
        .filter(|frame| frame.close_at.is_none())
        .map(|frame| JinjaOpenBlock {
            keyword: frame.keyword.clone(),
            closer: frame.closer.clone(),
        })
        .collect();
    let locals = pending
        .into_iter()
        .filter(|local| {
            local.scope_start <= cursor && cursor <= local.scope_end
        })
        .map(|local| JinjaTemplateLocal {
            name: local.name,
            kind: local.kind,
            scope_start: local.scope_start,
            scope_end: local.scope_end,
        })
        .collect();
    (locals, open_blocks)
}

#[derive(Debug)]
struct BlockFrame {
    keyword: String,
    closer: String,
    close_at: Option<usize>,
}

impl BlockFrame {
    fn open(keyword: &str, closer: &str) -> Self {
        Self {
            keyword: keyword.to_string(),
            closer: closer.to_string(),
            close_at: None,
        }
    }
}

#[derive(Debug)]
struct PendingLocal {
    name: String,
    kind: JinjaLocalKind,
    scope_start: usize,
    scope_end: usize,
    frame: Option<usize>,
}

/// Index of the innermost still-open frame, if any.
fn innermost_open(frames: &[BlockFrame]) -> Option<usize> {
    frames.iter().rposition(|frame| frame.close_at.is_none())
}

/// Close the nearest open frame expecting `closer`, truncating every local
/// declared in it (or in a frame opened after it) to the closer's start.
fn close_block(
    closer: &str,
    close_at: usize,
    frames: &mut [BlockFrame],
    pending: &mut [PendingLocal],
) {
    let Some(position) = frames
        .iter()
        .rposition(|frame| frame.close_at.is_none() && frame.closer == closer)
    else {
        return;
    };
    frames[position].close_at = Some(close_at);
    for local in pending.iter_mut() {
        if local.frame.is_some_and(|frame| frame >= position) {
            local.scope_end = local.scope_end.min(close_at);
        }
    }
}

fn push_local(
    name: String,
    kind: JinjaLocalKind,
    scope_start: usize,
    scope_end: usize,
    frame: Option<usize>,
    pending: &mut Vec<PendingLocal>,
) {
    if name.is_empty() {
        return;
    }
    pending.push(PendingLocal {
        name,
        kind,
        scope_start,
        scope_end,
        frame,
    });
}

/// Targets before the top-level `=` of an inline `{% set %}`.
fn assign_targets(content: &str) -> String {
    match find_top_level_assign(content) {
        Some(equal) => {
            let (keyword, _, keyword_end) =
                first_word(content).unwrap_or_default();
            debug_assert_eq!(keyword, "set");
            content[keyword_end..equal].to_string()
        }
        None => String::new(),
    }
}

/// Declare each comma-separated target's leading identifier.
fn declare_targets(
    targets: &str,
    scope_start: usize,
    frame: Option<usize>,
    end: usize,
    kind: JinjaLocalKind,
    pending: &mut Vec<PendingLocal>,
) {
    for (start, stop) in split_top_level(targets, b',') {
        if let Some(name) = first_ident(&targets[start..stop]) {
            push_local(name, kind, scope_start, end, frame, pending);
        }
    }
}

/// `{% for <targets> in ... %}` targets plus `loop`, scoped to the for body.
fn declare_for_targets(
    rest: &str,
    scope_start: usize,
    frame: usize,
    end: usize,
    pending: &mut Vec<PendingLocal>,
) {
    let Some(into) = find_top_level_word(rest, "in") else {
        return;
    };
    declare_targets(
        &rest[..into],
        scope_start,
        Some(frame),
        end,
        JinjaLocalKind::ForTarget,
        pending,
    );
    push_local(
        "loop".to_string(),
        JinjaLocalKind::Loop,
        scope_start,
        end,
        Some(frame),
        pending,
    );
}

/// `{% macro name(params) %}`: the name in the enclosing scope; params plus
/// `varargs`, `kwargs`, and `caller` inside the macro body.
fn declare_macro(
    rest: &str,
    scope_start: usize,
    frame: usize,
    enclosing: Option<usize>,
    end: usize,
    pending: &mut Vec<PendingLocal>,
) {
    let rest = rest.trim_start();
    let Some(name) = first_ident(rest) else {
        return;
    };
    push_local(
        name.clone(),
        JinjaLocalKind::MacroName,
        scope_start,
        end,
        enclosing,
        pending,
    );
    let after_name = &rest[name.len()..];
    let params = after_name
        .find('(')
        .and_then(|open| {
            find_matching_paren(after_name, open)
                .map(|close| &after_name[open + 1..close])
        })
        .unwrap_or("");
    for (start, stop) in split_top_level(params, b',') {
        let part = params[start..stop].trim();
        let target = find_top_level_assign(part)
            .map(|equal| &part[..equal])
            .unwrap_or(part);
        if let Some(param) = first_ident(target) {
            push_local(
                param,
                JinjaLocalKind::MacroParam,
                scope_start,
                end,
                Some(frame),
                pending,
            );
        }
    }
    for special in ["varargs", "kwargs", "caller"] {
        push_local(
            special.to_string(),
            JinjaLocalKind::MacroSpecial,
            scope_start,
            end,
            Some(frame),
            pending,
        );
    }
}

/// Byte offset of the `)` matching the `(` at `open`, skipping strings.
fn find_matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut index = open;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                index = super::scan::skip_quoted(text, index);
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// `{% with a=1, b=c %}` assignments, scoped to the with body.
fn declare_with_assignments(
    rest: &str,
    scope_start: usize,
    frame: usize,
    end: usize,
    pending: &mut Vec<PendingLocal>,
) {
    for (start, stop) in split_top_level(rest, b',') {
        let part = rest[start..stop].trim();
        let Some(equal) = find_top_level_assign(part) else {
            continue;
        };
        if let Some(name) = first_ident(&part[..equal]) {
            push_local(
                name,
                JinjaLocalKind::With,
                scope_start,
                end,
                Some(frame),
                pending,
            );
        }
    }
}

/// `{% import ... as name %}` aliases after the statement.
fn declare_import_names(
    rest: &str,
    scope_start: usize,
    frame: Option<usize>,
    end: usize,
    pending: &mut Vec<PendingLocal>,
) {
    for (start, stop) in split_top_level(rest, b',') {
        let part = &rest[start..stop];
        if let Some(alias_at) = find_top_level_word(part, "as") {
            if let Some(alias) = first_ident(&part[alias_at + 2..]) {
                push_local(
                    alias,
                    JinjaLocalKind::Import,
                    scope_start,
                    end,
                    frame,
                    pending,
                );
            }
        }
    }
}

/// `{% from ... import a as b, c %}` names after the statement.
fn declare_from_names(
    rest: &str,
    scope_start: usize,
    frame: Option<usize>,
    end: usize,
    pending: &mut Vec<PendingLocal>,
) {
    let Some(import_at) = find_top_level_word(rest, "import") else {
        return;
    };
    for (start, stop) in split_top_level(&rest[import_at + 6..], b',') {
        let part = &rest[import_at + 6..][start..stop];
        let target = find_top_level_word(part, "as")
            .and_then(|alias_at| first_ident(&part[alias_at + 2..]))
            .or_else(|| first_ident(part));
        if let Some(name) = target {
            push_local(
                name,
                JinjaLocalKind::Import,
                scope_start,
                end,
                frame,
                pending,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locals_at(text: &str, cursor: usize) -> Vec<String> {
        jinja_template_scope(text, cursor)
            .0
            .into_iter()
            .map(|local| local.name)
            .collect()
    }

    fn scope_of(
        text: &str,
        cursor: usize,
        name: &str,
    ) -> Option<(usize, usize)> {
        jinja_template_scope(text, cursor)
            .0
            .into_iter()
            .find(|local| local.name == name)
            .map(|local| (local.scope_start, local.scope_end))
    }

    #[test]
    fn set_targets_visible_after_declaration() {
        let text = "{% set x = 1 %}{{ x }}";
        assert!(locals_at(text, 5).is_empty());
        assert_eq!(locals_at(text, 20), ["x"]);
        let text = "{% set x, y = 1, 2 %}{{ y }}";
        assert_eq!(locals_at(text, 28), ["x", "y"]);
    }

    #[test]
    fn for_targets_and_loop_only_inside_body() {
        let text =
            "{% for x in items %}{{ x }}{{ loop.index }}{% endfor %}{{ x }}";
        assert!(locals_at(text, 10).is_empty());
        assert_eq!(locals_at(text, 25), ["x", "loop"]);
        assert!(locals_at(text, text.len()).is_empty());
    }

    #[test]
    fn nested_for_if_stacks_track_open_blocks() {
        let text = "{% for a in b %}{% if c %}{{ a }}{% endif %}{% endfor %}";
        let (_, blocks) = jinja_template_scope(text, 33);
        assert_eq!(
            blocks
                .iter()
                .map(|block| block.closer.as_str())
                .collect::<Vec<_>>(),
            ["endif", "endfor"]
        );
        assert_eq!(locals_at(text, 33), ["a", "loop"]);
        let (_, closed) = jinja_template_scope(text, text.len());
        assert!(closed.is_empty());
    }

    #[test]
    fn macro_name_params_and_specials() {
        let text =
            "{% macro m(a, b=1) %}{{ a }}{{ caller() }}{% endmacro %}{{ m }}";
        assert_eq!(
            locals_at(text, 30),
            ["m", "a", "b", "varargs", "kwargs", "caller"]
        );
        assert_eq!(locals_at(text, text.len()), ["m"]);
    }

    #[test]
    fn with_import_and_from_names() {
        let text = "{% with a=1 %}{{ a }}{% endwith %}";
        assert_eq!(locals_at(text, 18), ["a"]);
        assert!(locals_at(text, text.len()).is_empty());
        let text = "{% import \"x\" as y %}{{ y }}";
        assert_eq!(locals_at(text, text.len()), ["y"]);
        let text = "{% from \"m\" import a as b, c %}{{ b }}{{ c }}";
        assert_eq!(locals_at(text, text.len()), ["b", "c"]);
    }

    #[test]
    fn frontmatter_input_forms() {
        let text = "---\ninput:\n  name: word\n  count:\n    type: int\n    default: 3\n---\n{{ name }}";
        let inputs = jinja_declared_inputs(text, None);
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name, "name");
        assert!(inputs[0].required);
        assert_eq!(inputs[1].type_name, "int");
        assert!(!inputs[1].required);
        assert_eq!(inputs[1].default_display.as_deref(), Some("3"));

        let text = "---\ninputs:\n  - name: who\n    type: text\n    choices: [a, b]\n---\n{{ who }}";
        let inputs = jinja_declared_inputs(text, None);
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].choices, ["a", "b"]);
    }

    #[test]
    fn external_frontmatter_wins_and_dedupes() {
        let text = "---\ninput:\n  a: word\n  b: word\n---\n{{ a }}";
        let inputs =
            jinja_declared_inputs(text, Some("input:\n  a: text\n  c: word\n"));
        let names = inputs
            .iter()
            .map(|input| input.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a", "c", "b"]);
        assert_eq!(inputs[0].type_name, "text");
    }

    #[test]
    fn skill_flag_from_either_source() {
        let text = "---\nskill: true\n---\n{{ x }}";
        assert!(jinja_skill_enabled(text, None));
        let text = "{{ x }}";
        assert!(jinja_skill_enabled(text, Some("skill: [openai]")));
        assert!(!jinja_skill_enabled(text, None));
    }

    #[test]
    fn directive_aliases_and_zones() {
        assert_eq!(
            jinja_directive_presence("%repeat:3\n{{ x }}"),
            (true, false)
        );
        assert_eq!(jinja_directive_presence("%r:3\n{{ x }}"), (true, false));
        assert_eq!(
            jinja_directive_presence("%wait:co\n{{ x }}"),
            (false, true)
        );
        assert_eq!(jinja_directive_presence("%w:co\n{{ x }}"), (false, true));
        assert_eq!(
            jinja_directive_presence("```\n%repeat:3\n```\n{{ x }}"),
            (false, false)
        );
        assert_eq!(
            jinja_directive_presence("{{ \"%repeat:3\" }}\n{{ x }}"),
            (false, false)
        );
        assert_eq!(
            jinja_directive_presence("%model:foo\n{{ x }}"),
            (false, false)
        );
    }

    #[test]
    fn document_scope_combines_everything() {
        let text =
            "---\ninput:\n  name: word\n---\n%repeat:2\n{% set x = 1 %}{{ x }}";
        let scope = jinja_document_scope(text, text.len(), None);
        assert_eq!(scope.inputs.len(), 1);
        assert!(scope.has_repeat);
        assert!(!scope.has_wait);
        assert_eq!(
            scope
                .locals
                .iter()
                .map(|local| local.name.as_str())
                .collect::<Vec<_>>(),
            ["x"]
        );
    }

    #[test]
    fn local_scope_bounds_end_at_block_close() {
        let text = "{% for x in y %}{{ x }}{% endfor %}";
        // Inside the body the block is still open as far as the cursor
        // knows, so the local runs to the end of text ...
        assert_eq!(scope_of(text, 20, "x"), Some((16, text.len())));
        // ... and past the closer the local is gone.
        assert!(scope_of(text, text.len(), "x").is_none());
    }

    #[test]
    fn closed_raw_block_leaves_no_open_frame() {
        let text = "{% raw %}x{% endraw %} {% ";
        let (_, blocks) = jinja_template_scope(text, text.len());
        assert!(blocks.is_empty(), "{blocks:?}");
    }

    #[test]
    fn unclosed_raw_block_stays_open() {
        let text = "{% raw %}x{% ";
        let (_, blocks) = jinja_template_scope(text, text.len());
        assert_eq!(
            blocks
                .iter()
                .map(|block| block.closer.as_str())
                .collect::<Vec<_>>(),
            ["endraw"]
        );
    }

    #[test]
    fn fenced_for_block_does_not_leak_locals() {
        let text = "```\n{% for a in b %}\n```\n{% ";
        let (locals, blocks) = jinja_template_scope(text, text.len());
        assert!(locals.is_empty(), "{locals:?}");
        assert!(blocks.is_empty(), "{blocks:?}");
    }
}
