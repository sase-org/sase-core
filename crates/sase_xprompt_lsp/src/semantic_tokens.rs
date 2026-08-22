use std::collections::HashSet;

use lsp_types::{
    SemanticToken, SemanticTokenModifier, SemanticTokenType, SemanticTokens,
    SemanticTokensLegend,
};
use sase_core::{
    fenced_block_details, prompt_literal_zone_ranges, scan_artifact_refs,
    scan_directive_owned_fences, ArtifactRefContextWire, ArtifactRefSpanWire,
    CompiledGlossaryCatalog, DocumentSnapshot,
};

const KIND_TOKEN_TYPE: u32 = 0;
const PAYLOAD_TOKEN_TYPE: u32 = 1;
const FRAGMENT_TOKEN_TYPE: u32 = 2;
const GLOSSARY_TOKEN_TYPE: u32 = 3;
const DOCUMENT_ROLE_MODIFIER: u32 = 1;
const ARTIFACT_PRIORITY: u8 = 0;
const CODE_PRIORITY: u8 = 0;
const GLOSSARY_PRIORITY: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawSemanticToken {
    byte_start: usize,
    byte_end: usize,
    token_type: u32,
    token_modifiers_bitset: u32,
    priority: u8,
}

pub(crate) fn legend() -> SemanticTokensLegend {
    SemanticTokensLegend {
        token_types: vec![
            SemanticTokenType::NAMESPACE,
            SemanticTokenType::STRING,
            SemanticTokenType::NUMBER,
            SemanticTokenType::TYPE,
        ],
        token_modifiers: vec![SemanticTokenModifier::DOCUMENTATION],
    }
}

pub(crate) fn document_semantic_tokens(
    document: &DocumentSnapshot,
    artifact_context: Option<&ArtifactRefContextWire>,
    glossary_catalog: Option<&CompiledGlossaryCatalog>,
) -> SemanticTokens {
    let mut raw_tokens = Vec::new();
    if let Some(context) = artifact_context {
        raw_tokens.extend(raw_artifact_ref_tokens(document, context));
    }
    raw_tokens.extend(raw_directive_code_tokens(document));
    if let Some(catalog) = glossary_catalog {
        raw_tokens.extend(raw_glossary_tokens(document, catalog));
    }
    encode_tokens(document, non_overlapping_tokens(raw_tokens))
}

fn raw_artifact_ref_tokens(
    document: &DocumentSnapshot,
    context: &ArtifactRefContextWire,
) -> Vec<RawSemanticToken> {
    let literal_ranges = prompt_literal_zone_ranges(document.text());
    let document_roles = context
        .document_roots
        .iter()
        .map(|root| root.kind.as_str())
        .filter(|kind| !is_builtin_kind(kind))
        .collect::<HashSet<_>>();
    let mut tokens = Vec::new();

    for candidate in scan_artifact_refs(document.text()) {
        let candidate_range =
            (candidate.candidate_span.start, candidate.candidate_span.end);
        if literal_ranges
            .iter()
            .any(|literal| ranges_intersect(candidate_range, *literal))
        {
            continue;
        }
        let is_document_role = document_roles.contains(candidate.kind.as_str());
        if !is_builtin_kind(&candidate.kind) && !is_document_role {
            continue;
        }
        let modifiers = if is_document_role {
            DOCUMENT_ROLE_MODIFIER
        } else {
            0
        };
        push_raw_token(
            &mut tokens,
            candidate.kind_span,
            KIND_TOKEN_TYPE,
            modifiers,
            ARTIFACT_PRIORITY,
        );
        push_raw_token(
            &mut tokens,
            candidate.payload_span,
            PAYLOAD_TOKEN_TYPE,
            modifiers,
            ARTIFACT_PRIORITY,
        );
        if let Some(fragment_span) = candidate.fragment_span {
            push_raw_token(
                &mut tokens,
                fragment_span,
                FRAGMENT_TOKEN_TYPE,
                modifiers,
                ARTIFACT_PRIORITY,
            );
        }
    }

    tokens
}

fn raw_directive_code_tokens(
    document: &DocumentSnapshot,
) -> Vec<RawSemanticToken> {
    let text = document.text();
    let scan = scan_directive_owned_fences(text);
    if scan.directives.is_empty() {
        return Vec::new();
    }
    let fences = fenced_block_details(text);
    let mut tokens = Vec::new();
    for directive in scan.directives {
        if directive.code.is_none() {
            continue;
        }
        let directive_span = (directive.span[0], directive.span[1]);
        let Some(fence) = fences.iter().find(|fence| {
            directive_span.0 <= fence.block_range.0
                && fence.block_range.1 <= directive_span.1
        }) else {
            continue;
        };
        for (start, end) in non_empty_line_spans(text, fence.content_range) {
            tokens.push(RawSemanticToken {
                byte_start: start,
                byte_end: end,
                token_type: PAYLOAD_TOKEN_TYPE,
                token_modifiers_bitset: 0,
                priority: CODE_PRIORITY,
            });
        }
    }
    tokens
}

fn non_empty_line_spans(
    text: &str,
    range: (usize, usize),
) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = range.0;
    while start < range.1 {
        let relative = &text[start..range.1];
        let line_end = relative
            .find('\n')
            .map(|offset| start + offset)
            .unwrap_or(range.1);
        let trimmed_end =
            text[start..line_end].trim_end_matches('\r').len() + start;
        if !text[start..trimmed_end].trim().is_empty() {
            spans.push((start, trimmed_end));
        }
        if line_end == range.1 {
            break;
        }
        start = line_end + 1;
    }
    spans
}

fn raw_glossary_tokens(
    document: &DocumentSnapshot,
    catalog: &CompiledGlossaryCatalog,
) -> Vec<RawSemanticToken> {
    catalog
        .scan(document.text())
        .into_iter()
        .flat_map(|span| {
            span.segments.into_iter().map(|segment| RawSemanticToken {
                byte_start: segment.byte_start,
                byte_end: segment.byte_end,
                token_type: GLOSSARY_TOKEN_TYPE,
                token_modifiers_bitset: 0,
                priority: GLOSSARY_PRIORITY,
            })
        })
        .collect()
}

fn push_raw_token(
    tokens: &mut Vec<RawSemanticToken>,
    span: ArtifactRefSpanWire,
    token_type: u32,
    token_modifiers_bitset: u32,
    priority: u8,
) {
    tokens.push(RawSemanticToken {
        byte_start: span.start,
        byte_end: span.end,
        token_type,
        token_modifiers_bitset,
        priority,
    });
}

fn non_overlapping_tokens(
    mut tokens: Vec<RawSemanticToken>,
) -> Vec<RawSemanticToken> {
    tokens.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.byte_start.cmp(&right.byte_start))
            .then_with(|| span_len(right).cmp(&span_len(left)))
            .then_with(|| left.token_type.cmp(&right.token_type))
    });

    let mut accepted: Vec<RawSemanticToken> = Vec::new();
    for token in tokens {
        if accepted.iter().any(|existing| {
            ranges_intersect(
                (existing.byte_start, existing.byte_end),
                (token.byte_start, token.byte_end),
            )
        }) {
            continue;
        }
        accepted.push(token);
    }
    accepted.sort_by(|left, right| {
        left.byte_start
            .cmp(&right.byte_start)
            .then_with(|| left.byte_end.cmp(&right.byte_end))
            .then_with(|| left.token_type.cmp(&right.token_type))
    });
    accepted
}

fn encode_tokens(
    document: &DocumentSnapshot,
    tokens: Vec<RawSemanticToken>,
) -> SemanticTokens {
    let mut data = Vec::new();
    let mut previous = None;
    for token in tokens {
        push_token(document, &mut data, &mut previous, token);
    }
    SemanticTokens {
        result_id: None,
        data,
    }
}

fn push_token(
    document: &DocumentSnapshot,
    data: &mut Vec<SemanticToken>,
    previous: &mut Option<(u32, u32)>,
    token: RawSemanticToken,
) {
    let Some(range) =
        document.byte_range_to_range(token.byte_start, token.byte_end)
    else {
        return;
    };
    if range.start.line != range.end.line
        || range.start.character == range.end.character
    {
        return;
    }
    let (previous_line, previous_start) = previous.unwrap_or((0, 0));
    let delta_line = range.start.line.saturating_sub(previous_line);
    let delta_start = if delta_line == 0 {
        range.start.character.saturating_sub(previous_start)
    } else {
        range.start.character
    };
    data.push(SemanticToken {
        delta_line,
        delta_start,
        length: range.end.character - range.start.character,
        token_type: token.token_type,
        token_modifiers_bitset: token.token_modifiers_bitset,
    });
    *previous = Some((range.start.line, range.start.character));
}

fn is_builtin_kind(kind: &str) -> bool {
    matches!(kind, "commit" | "chat" | "bug" | "file")
}

fn ranges_intersect(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}

fn span_len(token: &RawSemanticToken) -> usize {
    token.byte_end.saturating_sub(token.byte_start)
}

#[cfg(test)]
mod tests {
    use lsp_types::SemanticToken;
    use sase_core::{compile_glossary_catalog, GlossaryInputEntryWire};

    use super::*;

    fn entry(term: &str) -> GlossaryInputEntryWire {
        GlossaryInputEntryWire {
            term: term.to_string(),
            definition: "Definition.".to_string(),
            aliases: Vec::new(),
            source: None,
        }
    }

    #[test]
    fn glossary_tokens_split_wrapped_segments_and_keep_artifacts() {
        let document =
            DocumentSnapshot::new("xprompt\n  memory @file:README.md");
        let catalog =
            compile_glossary_catalog(vec![entry("Xprompt Memory")]).unwrap();
        let tokens = document_semantic_tokens(
            &document,
            Some(&ArtifactRefContextWire::default()),
            Some(&catalog),
        );

        assert_eq!(
            tokens.data,
            vec![
                SemanticToken {
                    delta_line: 0,
                    delta_start: 0,
                    length: 7,
                    token_type: GLOSSARY_TOKEN_TYPE,
                    token_modifiers_bitset: 0,
                },
                SemanticToken {
                    delta_line: 1,
                    delta_start: 2,
                    length: 6,
                    token_type: GLOSSARY_TOKEN_TYPE,
                    token_modifiers_bitset: 0,
                },
                SemanticToken {
                    delta_line: 0,
                    delta_start: 8,
                    length: 4,
                    token_type: KIND_TOKEN_TYPE,
                    token_modifiers_bitset: 0,
                },
                SemanticToken {
                    delta_line: 0,
                    delta_start: 5,
                    length: 9,
                    token_type: PAYLOAD_TOKEN_TYPE,
                    token_modifiers_bitset: 0,
                },
            ]
        );
    }
}
