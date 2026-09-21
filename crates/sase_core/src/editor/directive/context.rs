use super::super::token::DocumentSnapshot;
use super::super::wire::{
    CompletionContext, CompletionContextKind, DirectiveClauseContext,
    DirectiveClauseKind, DirectiveMetadata, DirectiveSyntaxForm,
    DirectiveValueRole, EditorPosition, TokenInfo,
};
use super::candidate_lists::mixes_positional_and_keyword_clauses;
use super::contract::{
    canonical_directive_name, directive_allows_keywords, directive_metadata,
};
use crate::xprompt_text_block::find_text_block_close_for_args_bytes;

pub fn is_directive_like_token(token: &str) -> bool {
    token.starts_with('%') || token == "%(" || token == "%{"
}

pub fn detect_directive_context_at_position(
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
            selected_values: Vec::new(),
            directive: None,
            vcs_repo: None,
            vcs_ref: None,
            artifact_ref: None,
            replacement_range: range,
        });
    }

    let target = directive_arg_context(line, cursor_in_line)?;
    let mut directive_name = target.directive_name;
    let mut arg_start = target.arg_start;
    if directive_name == "model" {
        match before
            .get(target.arg_start..)
            .and_then(|text| text.rfind('@'))
        {
            Some(rel_at) if rel_at > 0 => {
                directive_name = "effort";
                arg_start = target.arg_start + rel_at + 1;
            }
            _ => {}
        }
    }
    let byte_start = line_start + arg_start;
    let byte_end = line_start + target.arg_end;
    let range = document.byte_range_to_range(byte_start, byte_end)?;
    let token_range = document.byte_range_to_range(byte_start, cursor)?;
    let mut clause = target.clause;
    if directive_name == "effort" && target.directive_name == "model" {
        clause.value_role = directive_metadata("effort")
            .and_then(|metadata| metadata.positional_role);
        clause.clause_kind = DirectiveClauseKind::Positional;
        clause.active_keyword = None;
    }
    Some(CompletionContext {
        kind: target.kind,
        token: Some(TokenInfo {
            text: before.get(arg_start..).unwrap_or_default().to_string(),
            range: token_range,
            byte_start,
            byte_end: cursor,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: Some(directive_name.to_string()),
        selected_values: target.selected_values,
        directive: Some(clause),
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
        replacement_range: range,
    })
}

fn directive_name_token(before: &str) -> Option<(usize, &str)> {
    let start = before.rfind('%')?;
    let token = &before[start..];
    if token == "%("
        || token == "%{"
        || token[1..]
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Some((start, token));
    }
    None
}

struct DirectiveArgCompletionTarget {
    directive_name: &'static str,
    arg_start: usize,
    arg_end: usize,
    kind: CompletionContextKind,
    selected_values: Vec<String>,
    clause: DirectiveClauseContext,
}

fn directive_arg_context(
    line: &str,
    cursor: usize,
) -> Option<DirectiveArgCompletionTarget> {
    let before = line.get(..cursor)?;
    let percent = before.rfind('%')?;
    let rest = &before[percent + 1..];
    let split = rest.find([':', '(', '{'])?;
    let name = &rest[..split];
    let canonical = canonical_directive_name(name)?;
    let metadata = directive_metadata(canonical)?;
    let sep = rest.as_bytes().get(split).copied()?;
    let syntax_form = match sep {
        b':' => DirectiveSyntaxForm::Colon,
        b'(' => DirectiveSyntaxForm::Parenthesized,
        b'{' => DirectiveSyntaxForm::BraceShorthand,
        _ => return None,
    };
    let open_idx = percent + 1 + split;
    if syntax_form == DirectiveSyntaxForm::Parenthesized {
        return parenthesized_arg_context(line, cursor, metadata, open_idx);
    }
    colon_arg_context(line, cursor, metadata, syntax_form, open_idx)
}

fn colon_arg_context(
    line: &str,
    cursor: usize,
    metadata: &'static DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
    colon_idx: usize,
) -> Option<DirectiveArgCompletionTarget> {
    let body_start = colon_idx + 1;
    if cursor < body_start {
        return None;
    }
    let body_end = unterminated_body_end(line, body_start);
    if mixes_positional_and_keyword_clauses(metadata) {
        return comma_clause_context(
            line,
            cursor,
            metadata,
            syntax_form,
            body_start,
            body_end,
            false,
        );
    }
    Some(DirectiveArgCompletionTarget {
        directive_name: metadata.name,
        arg_start: body_start,
        arg_end: body_end.max(cursor),
        kind: CompletionContextKind::DirectiveArgument,
        selected_values: Vec::new(),
        clause: DirectiveClauseContext {
            syntax_form,
            clause_kind: DirectiveClauseKind::Positional,
            active_keyword: None,
            value_role: metadata.positional_role,
            selected_keywords: Vec::new(),
            clause_range: None,
        },
    })
}

fn parenthesized_arg_context(
    line: &str,
    cursor: usize,
    metadata: &'static DirectiveMetadata,
    open_idx: usize,
) -> Option<DirectiveArgCompletionTarget> {
    if line.as_bytes().get(open_idx) != Some(&b'(') {
        return None;
    }
    let close = find_matching_paren_quoted(line, open_idx);
    if close.is_some_and(|close| cursor > close) {
        return None;
    }
    let body_start = open_idx + 1;
    let body_end =
        close.unwrap_or_else(|| unterminated_body_end(line, body_start));
    comma_clause_context(
        line,
        cursor,
        metadata,
        DirectiveSyntaxForm::Parenthesized,
        body_start,
        body_end,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn comma_clause_context(
    line: &str,
    cursor: usize,
    metadata: &'static DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
    body_start: usize,
    body_end: usize,
    keywords_allowed: bool,
) -> Option<DirectiveArgCompletionTarget> {
    if cursor < body_start || cursor > body_end {
        return None;
    }
    let body = line.get(body_start..body_end)?;
    let cursor_in_body = cursor - body_start;
    let clauses = split_top_level_clauses(body);
    let clause_index = clauses
        .iter()
        .position(|(start, end)| {
            cursor_in_body >= *start && cursor_in_body <= *end
        })
        .or_else(|| clauses.len().checked_sub(1))?;
    let (clause_start, clause_end) = clauses[clause_index];
    let clause = &body[clause_start..clause_end];
    let trimmed = clause.trim_start();
    let leading = clause.len() - trimmed.len();
    let trailing = clause.len() - clause.trim_end().len();
    let content_start = body_start + clause_start + leading;
    let content_end = body_start + clause_end - trailing;

    let mut selected_values = Vec::new();
    let mut selected_keywords = Vec::new();
    for (index, (start, end)) in clauses.iter().enumerate() {
        if index == clause_index {
            continue;
        }
        let other = body[*start..*end].trim();
        if other.is_empty() {
            continue;
        }
        selected_values.push(other.to_string());
        if let Some((name, _)) = split_keyword_clause(other) {
            selected_keywords.push(name.to_string());
        }
    }

    let keywords_in_form =
        keywords_allowed && directive_allows_keywords(metadata, syntax_form);
    let (kind, clause_kind, active_keyword, value_role, arg_start) =
        classify_active_clause(
            metadata,
            syntax_form,
            trimmed,
            keywords_in_form,
            clause_index,
            content_start,
        );

    Some(DirectiveArgCompletionTarget {
        directive_name: metadata.name,
        arg_start: arg_start.min(content_end.max(content_start)),
        arg_end: content_end.max(arg_start),
        kind,
        selected_values,
        clause: DirectiveClauseContext {
            syntax_form,
            clause_kind,
            active_keyword,
            value_role,
            selected_keywords,
            clause_range: None,
        },
    })
}

fn classify_active_clause(
    metadata: &'static DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
    trimmed: &str,
    keywords_in_form: bool,
    clause_index: usize,
    content_start: usize,
) -> (
    CompletionContextKind,
    DirectiveClauseKind,
    Option<String>,
    Option<DirectiveValueRole>,
    usize,
) {
    if keywords_in_form {
        if let Some((name, value)) = split_keyword_clause(trimmed) {
            let keyword = metadata
                .keywords
                .iter()
                .find(|keyword| keyword.name == name);
            let value_leading = value.len() - value.trim_start().len();
            let name_len = trimmed.find('=').unwrap_or(name.len());
            let arg_start = content_start + name_len + 1 + value_leading;
            let value_role = keyword.map(|keyword| keyword.value_role).or({
                if metadata.dynamic_keyword_role
                    == Some(DirectiveValueRole::ModelAliasKey)
                {
                    Some(DirectiveValueRole::Model)
                } else {
                    None
                }
            });
            return (
                CompletionContextKind::DirectiveArgumentValue,
                DirectiveClauseKind::KeywordValue,
                Some(name.to_string()),
                value_role,
                arg_start,
            );
        }
        if !mixes_positional_and_keyword_clauses(metadata) && clause_index > 0 {
            return (
                CompletionContextKind::DirectiveArgumentKeyword,
                DirectiveClauseKind::KeywordName,
                None,
                metadata.dynamic_keyword_role,
                content_start,
            );
        }
    }
    let value_role = if syntax_form == DirectiveSyntaxForm::Parenthesized
        && metadata.name == "model"
        && clause_index == 0
    {
        Some(DirectiveValueRole::Model)
    } else {
        metadata.positional_role
    };
    (
        CompletionContextKind::DirectiveArgument,
        DirectiveClauseKind::Positional,
        None,
        value_role,
        content_start,
    )
}

fn split_keyword_clause(clause: &str) -> Option<(&str, &str)> {
    let equals = find_top_level_equals(clause)?;
    let name = clause[..equals].trim();
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }
    Some((name, &clause[equals + 1..]))
}

fn split_top_level_clauses(body: &str) -> Vec<(usize, usize)> {
    let mut clauses = Vec::new();
    let mut start = 0usize;
    let bytes = body.as_bytes();
    let mut index = 0usize;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if !state.in_quotes() && bytes[index] == b',' {
            clauses.push((start, index));
            start = index + 1;
        }
        index += consumed;
    }
    clauses.push((start, body.len()));
    clauses
}

/// Byte offset where an unterminated directive argument body ends.
fn unterminated_body_end(line: &str, body_start: usize) -> usize {
    let bytes = line.as_bytes();
    let mut index = body_start;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if consumed == 1
            && !state.in_quotes()
            && bytes[index].is_ascii_whitespace()
            && !(index > body_start && bytes[index - 1] == b',')
        {
            return index;
        }
        index += consumed;
    }
    bytes.len()
}

fn find_top_level_equals(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if !state.in_quotes() && bytes[index] == b'=' {
            return Some(index);
        }
        index += consumed;
    }
    None
}

fn find_matching_paren_quoted(text: &str, open: usize) -> Option<usize> {
    if text.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let bytes = text.as_bytes();
    let mut depth = 1usize;
    let mut index = open + 1;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if !state.in_quotes() {
            match bytes[index] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        index += consumed;
    }
    None
}

#[derive(Default)]
struct QuoteState {
    quote: Option<u8>,
}

impl QuoteState {
    fn in_quotes(&self) -> bool {
        self.quote.is_some()
    }

    fn consume(&mut self, bytes: &[u8], index: usize) -> usize {
        if self.quote.is_none() && bytes.get(index..index + 2) == Some(b"[[") {
            return match find_text_block_close_for_args_bytes(
                bytes,
                index,
                bytes.len(),
            ) {
                Some(close) => close + 2 - index,
                None => bytes.len() - index,
            };
        }
        let byte = bytes[index];
        match self.quote {
            None if byte == b'"' || byte == b'\'' || byte == b'`' => {
                self.quote = Some(byte);
                1
            }
            Some(quote) if byte == quote => {
                self.quote = None;
                1
            }
            _ => 1,
        }
    }
}
