use std::collections::HashSet;

use lsp_types::{
    SemanticToken, SemanticTokenModifier, SemanticTokenType, SemanticTokens,
    SemanticTokensLegend,
};
use sase_core::{
    prompt_literal_zone_ranges, scan_artifact_refs, ArtifactRefContextWire,
    ArtifactRefSpanWire, DocumentSnapshot,
};

const KIND_TOKEN_TYPE: u32 = 0;
const PAYLOAD_TOKEN_TYPE: u32 = 1;
const FRAGMENT_TOKEN_TYPE: u32 = 2;
const DOCUMENT_ROLE_MODIFIER: u32 = 1;

pub(crate) fn legend() -> SemanticTokensLegend {
    // Keep this legend extensible for future xprompt/directive token emission.
    // This provider intentionally emits artifact-reference tokens only.
    SemanticTokensLegend {
        token_types: vec![
            SemanticTokenType::NAMESPACE,
            SemanticTokenType::STRING,
            SemanticTokenType::NUMBER,
        ],
        token_modifiers: vec![SemanticTokenModifier::DOCUMENTATION],
    }
}

pub(crate) fn artifact_ref_tokens(
    document: &DocumentSnapshot,
    context: &ArtifactRefContextWire,
) -> SemanticTokens {
    let literal_ranges = prompt_literal_zone_ranges(document.text());
    let document_roles = context
        .document_roots
        .iter()
        .map(|root| root.kind.as_str())
        .filter(|kind| !is_builtin_kind(kind))
        .collect::<HashSet<_>>();
    let mut data = Vec::new();
    let mut previous = None;

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
        push_token(
            document,
            &mut data,
            &mut previous,
            candidate.kind_span,
            KIND_TOKEN_TYPE,
            modifiers,
        );
        push_token(
            document,
            &mut data,
            &mut previous,
            candidate.payload_span,
            PAYLOAD_TOKEN_TYPE,
            modifiers,
        );
        if let Some(fragment_span) = candidate.fragment_span {
            push_token(
                document,
                &mut data,
                &mut previous,
                fragment_span,
                FRAGMENT_TOKEN_TYPE,
                modifiers,
            );
        }
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
    span: ArtifactRefSpanWire,
    token_type: u32,
    token_modifiers_bitset: u32,
) {
    let Some(range) = document.byte_range_to_range(span.start, span.end) else {
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
        token_type,
        token_modifiers_bitset,
    });
    *previous = Some((range.start.line, range.start.character));
}

fn is_builtin_kind(kind: &str) -> bool {
    matches!(kind, "commit" | "chat" | "bug" | "file")
}

fn ranges_intersect(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}
