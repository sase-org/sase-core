use std::collections::HashSet;

use lsp_types::{
    SemanticToken, SemanticTokenModifier, SemanticTokenType, SemanticTokens,
    SemanticTokensLegend,
};
use sase_core::{
    editor_extract_xprompt_argument_spans,
    editor_extract_xprompt_argument_spans_with_catalog,
    editor_extract_xprompt_call_name_spans, fenced_block_details,
    project_tag::{
        resolve_project_tag, scan_project_tags, ProjectTagResolutionWire,
        ProjectTagTargetWire,
    },
    prompt_literal_zone_ranges, scan_artifact_refs,
    scan_directive_owned_fences, ArtifactRefContextWire, ArtifactRefSpanWire,
    CompiledGlossaryCatalog, DocumentSnapshot, VcsProjectEntry,
    XpromptArgumentSource, XpromptArgumentSpanRole,
    XpromptArgumentSpanValidity, XpromptAssistEntry,
};

use crate::project_tags::accent_index_for_target;

const KIND_TOKEN_TYPE: u32 = 0;
const PAYLOAD_TOKEN_TYPE: u32 = 1;
const FRAGMENT_TOKEN_TYPE: u32 = 2;
const GLOSSARY_TOKEN_TYPE: u32 = 3;
const FUNCTION_TOKEN_TYPE: u32 = 4;
const MACRO_TOKEN_TYPE: u32 = 5;
const PARAMETER_TOKEN_TYPE: u32 = 6;
const OPERATOR_TOKEN_TYPE: u32 = 7;
const KEYWORD_TOKEN_TYPE: u32 = 8;
const PROJECT_TAG_TOKEN_TYPE: u32 = 9;
const DOCUMENT_ROLE_MODIFIER: u32 = 1 << 0;
const DEPRECATED_MODIFIER: u32 = 1 << 1;
const SIGIL_MODIFIER: u32 = 1 << 2;
const UNKNOWN_TAG_MODIFIER: u32 = 1 << 3;
const DISABLED_TAG_MODIFIER: u32 = 1 << 4;
/// `accent0`…`accent17` live at bits 5..23, inside the 32-bit budget.
const ACCENT_MODIFIER_SHIFT: u32 = 5;
const ACCENT_MODIFIER_COUNT: usize = 18;
const ARTIFACT_PRIORITY: u8 = 0;
const CODE_PRIORITY: u8 = 0;
const NAME_PRIORITY: u8 = 0;
const PROJECT_TAG_PRIORITY: u8 = 0;
const GLOSSARY_PRIORITY: u8 = 1;
const ARGUMENT_PRIORITY: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawSemanticToken {
    byte_start: usize,
    byte_end: usize,
    token_type: u32,
    token_modifiers_bitset: u32,
    priority: u8,
}

pub(crate) fn legend() -> SemanticTokensLegend {
    let mut token_modifiers = vec![
        SemanticTokenModifier::DOCUMENTATION,
        SemanticTokenModifier::DEPRECATED,
        SemanticTokenModifier::new("sigil"),
        SemanticTokenModifier::new("unknown"),
        SemanticTokenModifier::new("disabled"),
    ];
    token_modifiers.extend(accent_modifiers());
    SemanticTokensLegend {
        token_types: vec![
            SemanticTokenType::NAMESPACE,
            SemanticTokenType::STRING,
            SemanticTokenType::NUMBER,
            SemanticTokenType::TYPE,
            SemanticTokenType::FUNCTION,
            SemanticTokenType::MACRO,
            SemanticTokenType::PARAMETER,
            SemanticTokenType::OPERATOR,
            SemanticTokenType::KEYWORD,
            SemanticTokenType::new("saseProjectTag"),
        ],
        token_modifiers,
    }
}

/// The `accent0`…`accent17` legend modifiers, in bit order.
fn accent_modifiers() -> Vec<SemanticTokenModifier> {
    vec![
        SemanticTokenModifier::new("accent0"),
        SemanticTokenModifier::new("accent1"),
        SemanticTokenModifier::new("accent2"),
        SemanticTokenModifier::new("accent3"),
        SemanticTokenModifier::new("accent4"),
        SemanticTokenModifier::new("accent5"),
        SemanticTokenModifier::new("accent6"),
        SemanticTokenModifier::new("accent7"),
        SemanticTokenModifier::new("accent8"),
        SemanticTokenModifier::new("accent9"),
        SemanticTokenModifier::new("accent10"),
        SemanticTokenModifier::new("accent11"),
        SemanticTokenModifier::new("accent12"),
        SemanticTokenModifier::new("accent13"),
        SemanticTokenModifier::new("accent14"),
        SemanticTokenModifier::new("accent15"),
        SemanticTokenModifier::new("accent16"),
        SemanticTokenModifier::new("accent17"),
    ]
}

pub(crate) fn accent_modifier_bit(index: u32) -> u32 {
    1 << (ACCENT_MODIFIER_SHIFT + index)
}

pub(crate) fn document_semantic_tokens(
    document: &DocumentSnapshot,
    artifact_context: Option<&ArtifactRefContextWire>,
    glossary_catalog: Option<&CompiledGlossaryCatalog>,
    argument_entries: Option<&[XpromptAssistEntry]>,
    project_tags: &[ProjectTagTargetWire],
    project_entries: &[VcsProjectEntry],
) -> SemanticTokens {
    let mut raw_tokens = Vec::new();
    if let Some(context) = artifact_context {
        raw_tokens.extend(raw_artifact_ref_tokens(document, context));
    }
    raw_tokens.extend(raw_directive_code_tokens(document));
    if let Some(catalog) = glossary_catalog {
        raw_tokens.extend(raw_glossary_tokens(document, catalog));
    }
    raw_tokens.extend(raw_xprompt_call_name_tokens(document));
    raw_tokens.extend(raw_xprompt_argument_tokens(document, argument_entries));
    raw_tokens.extend(raw_project_tag_tokens(
        document,
        project_tags,
        project_entries,
    ));
    encode_tokens(document, non_overlapping_tokens(raw_tokens))
}

/// One sigil (`+`) token plus one name token per project tag. Resolved tags
/// carry their `accentN` modifier; unknown or ambiguous tags carry
/// `unknown`; resolved tags without a VCS provider carry `disabled`.
fn raw_project_tag_tokens(
    document: &DocumentSnapshot,
    targets: &[ProjectTagTargetWire],
    entries: &[VcsProjectEntry],
) -> Vec<RawSemanticToken> {
    if targets.is_empty() {
        return Vec::new();
    }
    let mut tokens = Vec::new();
    for span in scan_project_tags(document.text()) {
        let resolution = match resolve_project_tag(&span.name, targets) {
            ProjectTagResolutionWire::Resolved { target_index } => {
                let target = &targets[target_index];
                if target.workflow_type.is_none() {
                    DISABLED_TAG_MODIFIER
                } else {
                    accent_index_for_target(targets, target_index, entries)
                        .filter(|index| {
                            (*index as usize) < ACCENT_MODIFIER_COUNT
                        })
                        .map(accent_modifier_bit)
                        .unwrap_or(0)
                }
            }
            ProjectTagResolutionWire::Ambiguous { .. }
            | ProjectTagResolutionWire::Unknown { .. } => UNKNOWN_TAG_MODIFIER,
        };
        tokens.push(RawSemanticToken {
            byte_start: span.start,
            byte_end: span.start + 1,
            token_type: PROJECT_TAG_TOKEN_TYPE,
            token_modifiers_bitset: SIGIL_MODIFIER | resolution,
            priority: PROJECT_TAG_PRIORITY,
        });
        tokens.push(RawSemanticToken {
            byte_start: span.name_start,
            byte_end: span.end,
            token_type: PROJECT_TAG_TOKEN_TYPE,
            token_modifiers_bitset: resolution,
            priority: PROJECT_TAG_PRIORITY,
        });
    }
    tokens
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

fn raw_xprompt_argument_tokens(
    document: &DocumentSnapshot,
    entries: Option<&[XpromptAssistEntry]>,
) -> Vec<RawSemanticToken> {
    let spans = if let Some(entries) = entries {
        editor_extract_xprompt_argument_spans_with_catalog(document, entries)
    } else {
        editor_extract_xprompt_argument_spans(document)
    };

    spans
        .into_iter()
        .map(|span| RawSemanticToken {
            byte_start: span.start,
            byte_end: span.end,
            token_type: argument_token_type(span.role),
            token_modifiers_bitset: argument_token_modifiers(span.validity),
            priority: ARGUMENT_PRIORITY,
        })
        .collect()
}

fn raw_xprompt_call_name_tokens(
    document: &DocumentSnapshot,
) -> Vec<RawSemanticToken> {
    editor_extract_xprompt_call_name_spans(document)
        .into_iter()
        .map(|span| RawSemanticToken {
            byte_start: span.start,
            byte_end: span.end,
            token_type: match span.source {
                XpromptArgumentSource::Xprompt => FUNCTION_TOKEN_TYPE,
                XpromptArgumentSource::Directive => MACRO_TOKEN_TYPE,
            },
            token_modifiers_bitset: 0,
            priority: NAME_PRIORITY,
        })
        .collect()
}

fn argument_token_type(role: XpromptArgumentSpanRole) -> u32 {
    match role {
        XpromptArgumentSpanRole::ArgDelimiter
        | XpromptArgumentSpanRole::ArgAssign => OPERATOR_TOKEN_TYPE,
        XpromptArgumentSpanRole::ArgKey => PARAMETER_TOKEN_TYPE,
        XpromptArgumentSpanRole::ArgValue
        | XpromptArgumentSpanRole::ArgValueString => PAYLOAD_TOKEN_TYPE,
        XpromptArgumentSpanRole::ArgValueNumber => FRAGMENT_TOKEN_TYPE,
        XpromptArgumentSpanRole::ArgValueBool => KEYWORD_TOKEN_TYPE,
    }
}

fn argument_token_modifiers(validity: XpromptArgumentSpanValidity) -> u32 {
    match validity {
        XpromptArgumentSpanValidity::UnknownKey => DEPRECATED_MODIFIER,
        XpromptArgumentSpanValidity::Ok
        | XpromptArgumentSpanValidity::TypeMismatch
        | XpromptArgumentSpanValidity::DuplicateKey
        | XpromptArgumentSpanValidity::Unresolvable => 0,
    }
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
        let mut remaining = vec![(token.byte_start, token.byte_end)];
        for existing in &accepted {
            remaining = remaining
                .into_iter()
                .flat_map(|segment| {
                    subtract_range(
                        segment,
                        (existing.byte_start, existing.byte_end),
                    )
                })
                .collect();
            if remaining.is_empty() {
                break;
            }
        }
        for (byte_start, byte_end) in remaining {
            if byte_start >= byte_end {
                continue;
            }
            accepted.push(RawSemanticToken {
                byte_start,
                byte_end,
                ..token
            });
        }
    }
    accepted.sort_by(|left, right| {
        left.byte_start
            .cmp(&right.byte_start)
            .then_with(|| left.byte_end.cmp(&right.byte_end))
            .then_with(|| left.token_type.cmp(&right.token_type))
    });
    accepted
}

fn subtract_range(
    segment: (usize, usize),
    blocker: (usize, usize),
) -> Vec<(usize, usize)> {
    if !ranges_intersect(segment, blocker) {
        return vec![segment];
    }
    let mut pieces = Vec::new();
    if segment.0 < blocker.0 {
        pieces.push((segment.0, blocker.0.min(segment.1)));
    }
    if blocker.1 < segment.1 {
        pieces.push((blocker.1.max(segment.0), segment.1));
    }
    pieces
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
    for (byte_start, byte_end) in single_line_token_spans(
        document.text(),
        token.byte_start,
        token.byte_end,
    ) {
        push_single_line_token(
            document,
            data,
            previous,
            RawSemanticToken {
                byte_start,
                byte_end,
                ..token
            },
        );
    }
}

fn push_single_line_token(
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

fn single_line_token_spans(
    text: &str,
    byte_start: usize,
    byte_end: usize,
) -> Vec<(usize, usize)> {
    if byte_start >= byte_end {
        return Vec::new();
    }
    let mut spans = Vec::new();
    let mut start = byte_start;
    while start < byte_end {
        let relative = &text[start..byte_end];
        let line_end = relative
            .find('\n')
            .map(|offset| start + offset)
            .unwrap_or(byte_end);
        let mut piece_end = line_end;
        if piece_end > start
            && text.as_bytes().get(piece_end - 1) == Some(&b'\r')
        {
            piece_end -= 1;
        }
        if start < piece_end {
            spans.push((start, piece_end));
        }
        if line_end == byte_end {
            break;
        }
        start = line_end + 1;
    }
    spans
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
    use sase_core::{
        compile_glossary_catalog, GlossaryInputEntryWire, XpromptInputHint,
    };

    use super::*;

    fn entry(term: &str) -> GlossaryInputEntryWire {
        GlossaryInputEntryWire {
            term: term.to_string(),
            definition: "Definition.".to_string(),
            aliases: Vec::new(),
            source: None,
        }
    }

    fn assist_entry(
        name: &str,
        inputs: Vec<XpromptInputHint>,
    ) -> XpromptAssistEntry {
        XpromptAssistEntry {
            name: name.to_string(),
            display_label: name.to_string(),
            insertion: format!("#{name}"),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "test".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs,
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
        }
    }

    fn input(name: &str, r#type: &str, position: u32) -> XpromptInputHint {
        XpromptInputHint {
            name: name.to_string(),
            r#type: r#type.to_string(),
            description: None,
            required: true,
            default_display: None,
            position,
            repeatable: false,
        }
    }

    #[test]
    fn legend_appends_argument_entries_without_reordering_existing_tokens() {
        let legend = legend();

        assert_eq!(
            legend.token_types[..4]
                .iter()
                .map(|token_type| token_type.as_str())
                .collect::<Vec<_>>(),
            vec!["namespace", "string", "number", "type"]
        );
        assert_eq!(
            legend
                .token_types
                .iter()
                .map(|token_type| token_type.as_str())
                .collect::<Vec<_>>(),
            vec![
                "namespace",
                "string",
                "number",
                "type",
                "function",
                "macro",
                "parameter",
                "operator",
                "keyword",
                "saseProjectTag"
            ]
        );
        assert_eq!(
            legend
                .token_modifiers
                .iter()
                .map(|modifier| modifier.as_str())
                .collect::<Vec<_>>()[..5],
            vec![
                "documentation",
                "deprecated",
                "sigil",
                "unknown",
                "disabled"
            ]
        );
        assert_eq!(legend.token_modifiers.len(), 2 + 3 + 18);
    }

    #[test]
    fn argument_tokens_cover_structure_and_literal_values() {
        let document =
            DocumentSnapshot::new("#foo(path=\"a\", count=2, enabled=true)");
        let tokens =
            document_semantic_tokens(&document, None, None, None, &[], &[]);

        assert_eq!(
            absolute_semantic_tokens(&tokens.data),
            vec![
                (0, 1, 3, FUNCTION_TOKEN_TYPE, 0),
                (0, 4, 1, OPERATOR_TOKEN_TYPE, 0),
                (0, 5, 4, PARAMETER_TOKEN_TYPE, 0),
                (0, 9, 1, OPERATOR_TOKEN_TYPE, 0),
                (0, 10, 3, PAYLOAD_TOKEN_TYPE, 0),
                (0, 13, 1, OPERATOR_TOKEN_TYPE, 0),
                (0, 15, 5, PARAMETER_TOKEN_TYPE, 0),
                (0, 20, 1, OPERATOR_TOKEN_TYPE, 0),
                (0, 21, 1, FRAGMENT_TOKEN_TYPE, 0),
                (0, 22, 1, OPERATOR_TOKEN_TYPE, 0),
                (0, 24, 7, PARAMETER_TOKEN_TYPE, 0),
                (0, 31, 1, OPERATOR_TOKEN_TYPE, 0),
                (0, 32, 4, KEYWORD_TOKEN_TYPE, 0),
                (0, 36, 1, OPERATOR_TOKEN_TYPE, 0),
            ]
        );
    }

    #[test]
    fn name_tokens_cover_xprompts_directives_aliases_and_literal_zones() {
        let document = DocumentSnapshot::new(
            "#foo #bar:value #baz:: body\n%q(capacity=2)\n```\n#hidden %wait\n```",
        );
        let tokens =
            document_semantic_tokens(&document, None, None, None, &[], &[]);

        assert_eq!(
            absolute_semantic_tokens(&tokens.data)
                .into_iter()
                .filter(|token| {
                    token.3 == FUNCTION_TOKEN_TYPE
                        || token.3 == MACRO_TOKEN_TYPE
                })
                .collect::<Vec<_>>(),
            vec![
                (0, 1, 3, FUNCTION_TOKEN_TYPE, 0),
                (0, 6, 3, FUNCTION_TOKEN_TYPE, 0),
                (0, 17, 3, FUNCTION_TOKEN_TYPE, 0),
                (1, 1, 1, MACRO_TOKEN_TYPE, 0),
            ]
        );
    }

    #[test]
    fn multiline_argument_values_split_on_lines_with_utf16_ranges() {
        let document = DocumentSnapshot::new(
            "🙂 #foo(text=[[alpha\r\nbeta 🙂\r\ngamma]])",
        );
        let tokens =
            document_semantic_tokens(&document, None, None, None, &[], &[]);
        let absolute = absolute_semantic_tokens(&tokens.data);

        assert!(
            absolute.contains(&(0, 4, 3, FUNCTION_TOKEN_TYPE, 0)),
            "{absolute:?}"
        );
        assert!(
            absolute.contains(&(0, 13, 7, PAYLOAD_TOKEN_TYPE, 0)),
            "{absolute:?}"
        );
        assert!(
            absolute.contains(&(1, 0, 7, PAYLOAD_TOKEN_TYPE, 0)),
            "{absolute:?}"
        );
        assert!(
            absolute.contains(&(2, 0, 7, PAYLOAD_TOKEN_TYPE, 0)),
            "{absolute:?}"
        );
    }

    #[test]
    fn unknown_argument_keys_get_deprecated_modifier_when_catalog_is_warm() {
        let document = DocumentSnapshot::new("#foo(path=a, nope=b)");
        let entries = vec![assist_entry("foo", vec![input("path", "path", 0)])];
        let tokens = document_semantic_tokens(
            &document,
            None,
            None,
            Some(&entries),
            &[],
            &[],
        );

        assert!(absolute_semantic_tokens(&tokens.data).contains(&(
            0,
            13,
            4,
            PARAMETER_TOKEN_TYPE,
            DEPRECATED_MODIFIER,
        )));
    }

    #[test]
    fn argument_values_preserve_pieces_around_nested_artifact_tokens() {
        let document =
            DocumentSnapshot::new("#foo(path=pre @file:README.md post)");
        let tokens = document_semantic_tokens(
            &document,
            Some(&ArtifactRefContextWire::default()),
            None,
            None,
            &[],
            &[],
        );
        let absolute = absolute_semantic_tokens(&tokens.data);

        assert!(absolute.contains(&(0, 15, 4, KIND_TOKEN_TYPE, 0)));
        assert!(absolute.contains(&(0, 20, 9, PAYLOAD_TOKEN_TYPE, 0)));
        assert!(absolute.contains(&(0, 10, 5, PAYLOAD_TOKEN_TYPE, 0)));
        assert!(absolute.contains(&(0, 19, 1, PAYLOAD_TOKEN_TYPE, 0)));
        assert!(absolute.contains(&(0, 29, 5, PAYLOAD_TOKEN_TYPE, 0)));
        assert_no_token_overlaps(&absolute);
    }

    #[test]
    fn argument_tokens_skip_fenced_blocks() {
        let document = DocumentSnapshot::new("```\n#foo(path=\"a\")\n```");
        let tokens =
            document_semantic_tokens(&document, None, None, None, &[], &[]);

        assert!(tokens.data.is_empty());
    }

    fn absolute_semantic_tokens(
        tokens: &[SemanticToken],
    ) -> Vec<(u32, u32, u32, u32, u32)> {
        let mut line = 0u32;
        let mut start = 0u32;
        tokens
            .iter()
            .map(|token| {
                line += token.delta_line;
                if token.delta_line == 0 {
                    start += token.delta_start;
                } else {
                    start = token.delta_start;
                }
                (
                    line,
                    start,
                    token.length,
                    token.token_type,
                    token.token_modifiers_bitset,
                )
            })
            .collect()
    }

    fn assert_no_token_overlaps(tokens: &[(u32, u32, u32, u32, u32)]) {
        for pair in tokens.windows(2) {
            let left = pair[0];
            let right = pair[1];
            if left.0 == right.0 {
                assert!(
                    left.1 + left.2 <= right.1,
                    "overlapping tokens: {tokens:?}"
                );
            }
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
            None,
            &[],
            &[],
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
