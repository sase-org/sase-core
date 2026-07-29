use super::parse_artifact_ref;
use super::wire::{
    ArtifactRefPromptCandidateWire, ArtifactRefSpanWire,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
};

const TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', ':', '!', '?', ')'];

pub fn scan_artifact_refs(text: &str) -> Vec<ArtifactRefPromptCandidateWire> {
    let mut candidates = Vec::new();
    for (start, character) in text.char_indices() {
        if character != '@' || !has_allowed_left_context(text, start) {
            continue;
        }
        let raw_end = text[start + 1..]
            .char_indices()
            .find_map(|(offset, character)| {
                (character.is_whitespace()
                    || matches!(character, '"' | '\'' | '`'))
                .then_some(start + 1 + offset)
            })
            .unwrap_or(text.len());
        let end = trim_candidate_end(text, start + 1, raw_end);
        if end <= start + 1 {
            continue;
        }

        let reference = &text[start + 1..end];
        let Some(separator_offset) = reference.find(':') else {
            continue;
        };
        let separator = start + 1 + separator_offset;
        let kind_start = start + 1;
        let kind_end = separator;
        let kind = &text[kind_start..kind_end];
        let payload_start = separator + 1;
        let fragment_start = if kind == "bug" {
            None
        } else {
            text[payload_start..end]
                .find('#')
                .map(|offset| payload_start + offset)
        };
        let payload_end = fragment_start.unwrap_or(end);

        candidates.push(ArtifactRefPromptCandidateWire {
            schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
            text: text[start..end].to_string(),
            reference: reference.to_string(),
            kind: kind.to_string(),
            well_formed: parse_artifact_ref(reference).is_ok(),
            candidate_span: span(start, end),
            sigil_span: span(start, start + 1),
            kind_span: span(kind_start, kind_end),
            separator_span: span(separator, separator + 1),
            payload_span: span(payload_start, payload_end),
            fragment_span: fragment_start.map(|start| span(start, end)),
        });
    }
    candidates
}

fn has_allowed_left_context(text: &str, start: usize) -> bool {
    start == 0
        || text[..start].chars().next_back().is_some_and(|character| {
            character.is_whitespace() || matches!(character, '"' | '\'' | '`')
        })
}

fn trim_candidate_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start {
        let Some(character) = text[start..end].chars().next_back() else {
            break;
        };
        if !TRAILING_PUNCTUATION.contains(&character) {
            break;
        }
        end -= character.len_utf8();
    }
    end
}

const fn span(start: usize, end: usize) -> ArtifactRefSpanWire {
    ArtifactRefSpanWire { start, end }
}
