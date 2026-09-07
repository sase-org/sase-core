use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use regex::Regex;

use super::wire::{
    ArtifactRefDocumentScanWire, ArtifactRefDocumentTargetKindWire,
    ArtifactRefDocumentTargetWire, ArtifactRefPromptCandidateWire,
    ArtifactRefSpanWire, ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
};
use super::{
    artifact_ref_kind_catalog, canonical_artifact_ref_kind, parse_artifact_ref,
    parse_artifact_ref_canonical,
};
use crate::artifact_link::{LINKS_BLOCK_END_MARKER, LINKS_BLOCK_START_MARKER};
use crate::markdown_link_refs::scan_markdown_reference_links;

const TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', ':', '!', '?', ')'];
const URL_TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', '!', '?'];

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

        if text[payload_start..].starts_with('"') {
            candidates.push(scan_quoted_candidate(
                text,
                start,
                kind_start,
                kind_end,
                separator,
                payload_start,
                kind,
            ));
            continue;
        }

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
            quoted: false,
        });
    }
    candidates
}

/// Scan rendered documents for pager-activatable targets.
///
/// Unlike [`scan_artifact_refs`], this document-oriented contract separates
/// the visible source span from the semantic destination. It understands
/// Markdown inline/reference links, SASE-generated Links tables, prompt-style
/// `@kind:payload` refs, unsigiled document refs for configured kinds, URLs,
/// and the same path shapes the Python pager historically linked.
pub fn scan_artifact_ref_document_links(
    text: &str,
    known_kinds: &[String],
) -> ArtifactRefDocumentScanWire {
    let known_labels = known_document_kind_labels(known_kinds);
    let link_table_ranges = managed_links_table_ranges(text);
    let mut links = Vec::new();
    let mut occupied = Vec::new();

    for link in
        scan_markdown_document_links(text, &known_labels, &link_table_ranges)
    {
        occupied.push((link.source_span.start, link.source_span.end));
        links.push(link);
    }

    for candidate in scan_artifact_refs(text) {
        let link = prompt_candidate_document_link(candidate, &known_labels);
        if link.well_formed {
            if overlaps(link.source_span.start, link.source_span.end, &occupied)
            {
                continue;
            }
            occupied.push((link.source_span.start, link.source_span.end));
        }
        links.push(link);
    }

    for link in scan_unsigiled_artifact_refs(text, &known_labels) {
        if link.well_formed {
            if overlaps(link.source_span.start, link.source_span.end, &occupied)
            {
                continue;
            }
            occupied.push((link.source_span.start, link.source_span.end));
        }
        links.push(link);
    }

    for link in scan_document_urls(text, &occupied) {
        occupied.push((link.source_span.start, link.source_span.end));
        links.push(link);
    }

    for link in scan_document_file_paths(text, &occupied) {
        occupied.push((link.source_span.start, link.source_span.end));
        links.push(link);
    }

    links.sort_by_key(|link| {
        (
            link.source_span.start,
            link.source_span.end,
            link.target_kind.label(),
        )
    });
    ArtifactRefDocumentScanWire {
        schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
        links,
        diagnostics: Vec::new(),
    }
}

fn known_document_kind_labels(known_kinds: &[String]) -> BTreeSet<String> {
    let mut labels: BTreeSet<String> = [
        "agent", "bead", "bug", "chat", "commit", "file", "patch", "plan",
        "plans", "stitch",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    for descriptor in artifact_ref_kind_catalog() {
        labels.insert(descriptor.kind);
        labels.extend(descriptor.aliases);
    }
    for raw in known_kinds {
        let label = raw.trim();
        if label.is_empty() {
            continue;
        }
        labels.insert(label.to_string());
        labels.insert(canonical_artifact_ref_kind(label).canonical);
    }
    labels
}

fn prompt_candidate_document_link(
    candidate: ArtifactRefPromptCandidateWire,
    known_labels: &BTreeSet<String>,
) -> ArtifactRefDocumentTargetWire {
    let well_formed =
        candidate.well_formed && known_labels.contains(&candidate.kind);
    let target = canonical_document_artifact_ref(&candidate.reference)
        .unwrap_or_else(|| candidate.reference.clone());
    let target_span =
        span(candidate.kind_span.start, candidate.candidate_span.end);
    ArtifactRefDocumentTargetWire {
        schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
        target_kind: ArtifactRefDocumentTargetKindWire::ArtifactRef,
        text: candidate.text,
        target: target.clone(),
        well_formed,
        source_span: candidate.candidate_span,
        candidate_span: candidate.candidate_span,
        target_span,
        label_span: None,
        destination_span: None,
        reference_label: None,
        markdown_destination: None,
        hosted_destination: None,
        artifact_reference: well_formed.then_some(target),
        quoted: candidate.quoted,
    }
}

fn scan_unsigiled_artifact_refs(
    text: &str,
    known_labels: &BTreeSet<String>,
) -> Vec<ArtifactRefDocumentTargetWire> {
    let labels = sorted_known_labels(known_labels);
    let mut links = Vec::new();
    for (start, character) in text.char_indices() {
        if !character.is_ascii_lowercase()
            || !has_allowed_left_context(text, start)
        {
            continue;
        }
        let Some(label) = labels.iter().find(|label| {
            text[start..].starts_with(label.as_str())
                && text[start + label.len()..].starts_with(':')
        }) else {
            continue;
        };
        let synthetic = format!("@{}", &text[start..]);
        let Some(candidate) = scan_artifact_refs(&synthetic).into_iter().next()
        else {
            continue;
        };
        if candidate.candidate_span.start != 0 || candidate.kind != **label {
            continue;
        }
        let end = start + candidate.candidate_span.end.saturating_sub(1);
        let well_formed = candidate.well_formed;
        let target = canonical_document_artifact_ref(&candidate.reference)
            .unwrap_or_else(|| candidate.reference.clone());
        links.push(ArtifactRefDocumentTargetWire {
            schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
            target_kind: ArtifactRefDocumentTargetKindWire::ArtifactRef,
            text: text[start..end].to_string(),
            target: target.clone(),
            well_formed,
            source_span: span(start, end),
            candidate_span: span(start, end),
            target_span: span(start, end),
            label_span: None,
            destination_span: None,
            reference_label: None,
            markdown_destination: None,
            hosted_destination: None,
            artifact_reference: well_formed.then_some(target),
            quoted: candidate.quoted,
        });
    }
    links
}

fn sorted_known_labels(known_labels: &BTreeSet<String>) -> Vec<String> {
    let mut labels = known_labels.iter().cloned().collect::<Vec<_>>();
    labels.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });
    labels
}

#[derive(Debug)]
struct MarkdownDocumentLink<'a> {
    source_span: ArtifactRefSpanWire,
    label_span: ArtifactRefSpanWire,
    label: &'a str,
    destination_span: Option<ArtifactRefSpanWire>,
    destination: String,
    reference_label: Option<String>,
}

fn scan_markdown_document_links(
    text: &str,
    known_labels: &BTreeSet<String>,
    link_table_ranges: &[(usize, usize)],
) -> Vec<ArtifactRefDocumentTargetWire> {
    let definitions: BTreeMap<String, String> =
        scan_markdown_reference_links(text)
            .definitions
            .into_iter()
            .map(|definition| (definition.label, definition.destination))
            .collect();
    let mut links = Vec::new();
    let bytes = text.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'[' {
            index += 1;
            continue;
        }
        let Some((label, label_span, first_end)) =
            scan_markdown_bracket(text, index)
        else {
            index += 1;
            continue;
        };
        if bytes.get(first_end) == Some(&b':') {
            index = first_end;
            continue;
        }

        let link = if bytes.get(first_end) == Some(&b'(') {
            parse_inline_markdown_link(
                text, index, label, label_span, first_end,
            )
        } else if bytes.get(first_end) == Some(&b'[') {
            parse_full_or_collapsed_reference_link(
                text,
                index,
                label,
                label_span,
                first_end,
                &definitions,
            )
        } else {
            parse_shortcut_reference_link(
                text,
                index,
                label,
                label_span,
                first_end,
                &definitions,
            )
        };

        let Some(link) = link else {
            index += 1;
            continue;
        };
        index = link.source_span.end;
        links.push(markdown_link_to_document_target(
            text,
            link,
            known_labels,
            link_table_ranges,
        ));
    }
    links
}

fn parse_inline_markdown_link<'a>(
    text: &'a str,
    source_start: usize,
    label: &'a str,
    label_span: ArtifactRefSpanWire,
    open_paren: usize,
) -> Option<MarkdownDocumentLink<'a>> {
    let (destination, destination_span, source_end) =
        scan_inline_destination(text, open_paren)?;
    Some(MarkdownDocumentLink {
        source_span: span(source_start, source_end),
        label_span,
        label,
        destination_span: Some(destination_span),
        destination,
        reference_label: None,
    })
}

fn parse_full_or_collapsed_reference_link<'a>(
    text: &'a str,
    source_start: usize,
    label: &'a str,
    label_span: ArtifactRefSpanWire,
    second_start: usize,
    definitions: &BTreeMap<String, String>,
) -> Option<MarkdownDocumentLink<'a>> {
    let (raw_ref_label, _, source_end) =
        scan_markdown_bracket(text, second_start)?;
    let reference_label = if raw_ref_label.is_empty() {
        unescape_markdown_text(label)
    } else {
        unescape_markdown_text(raw_ref_label)
    };
    let destination = definitions.get(&reference_label)?;
    Some(MarkdownDocumentLink {
        source_span: span(source_start, source_end),
        label_span,
        label,
        destination_span: None,
        destination: destination.clone(),
        reference_label: Some(reference_label),
    })
}

fn parse_shortcut_reference_link<'a>(
    _text: &'a str,
    source_start: usize,
    label: &'a str,
    label_span: ArtifactRefSpanWire,
    source_end: usize,
    definitions: &BTreeMap<String, String>,
) -> Option<MarkdownDocumentLink<'a>> {
    let reference_label = unescape_markdown_text(label);
    let destination = definitions.get(&reference_label)?;
    Some(MarkdownDocumentLink {
        source_span: span(source_start, source_end),
        label_span,
        label,
        destination_span: None,
        destination: destination.clone(),
        reference_label: Some(reference_label),
    })
}

fn markdown_link_to_document_target(
    text: &str,
    link: MarkdownDocumentLink<'_>,
    known_labels: &BTreeSet<String>,
    link_table_ranges: &[(usize, usize)],
) -> ArtifactRefDocumentTargetWire {
    let label = unescape_markdown_text(link.label);
    let inside_links_table =
        contains_position(link_table_ranges, link.source_span.start);
    let markdown_destination = Some(link.destination.clone());
    if inside_links_table && is_url_target(&link.destination) {
        if let Some(reference) =
            canonical_artifact_ref_from_document_text(&label, known_labels)
        {
            return ArtifactRefDocumentTargetWire {
                schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
                target_kind: ArtifactRefDocumentTargetKindWire::ArtifactRef,
                text: text[link.source_span.start..link.source_span.end]
                    .to_string(),
                target: reference.clone(),
                well_formed: true,
                source_span: link.source_span,
                candidate_span: link.source_span,
                target_span: link.label_span,
                label_span: Some(link.label_span),
                destination_span: link.destination_span,
                reference_label: link.reference_label,
                markdown_destination,
                hosted_destination: Some(link.destination),
                artifact_reference: Some(reference),
                quoted: false,
            };
        }
    }

    if let Some(reference) = canonical_artifact_ref_from_document_text(
        &link.destination,
        known_labels,
    ) {
        return ArtifactRefDocumentTargetWire {
            schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
            target_kind: ArtifactRefDocumentTargetKindWire::ArtifactRef,
            text: text[link.source_span.start..link.source_span.end]
                .to_string(),
            target: reference.clone(),
            well_formed: true,
            source_span: link.source_span,
            candidate_span: link.source_span,
            target_span: link.destination_span.unwrap_or(link.label_span),
            label_span: Some(link.label_span),
            destination_span: link.destination_span,
            reference_label: link.reference_label,
            markdown_destination,
            hosted_destination: None,
            artifact_reference: Some(reference),
            quoted: false,
        };
    }

    let target_kind = if is_url_target(&link.destination) {
        ArtifactRefDocumentTargetKindWire::Url
    } else {
        ArtifactRefDocumentTargetKindWire::FilePath
    };
    ArtifactRefDocumentTargetWire {
        schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
        target_kind,
        text: text[link.source_span.start..link.source_span.end].to_string(),
        target: link.destination.clone(),
        well_formed: true,
        source_span: link.source_span,
        candidate_span: link.source_span,
        target_span: link.destination_span.unwrap_or(link.label_span),
        label_span: Some(link.label_span),
        destination_span: link.destination_span,
        reference_label: link.reference_label,
        markdown_destination,
        hosted_destination: (target_kind
            == ArtifactRefDocumentTargetKindWire::Url)
            .then_some(link.destination),
        artifact_reference: None,
        quoted: false,
    }
}

fn scan_markdown_bracket(
    text: &str,
    start: usize,
) -> Option<(&str, ArtifactRefSpanWire, usize)> {
    let content_start = start + 1;
    let mut escaped = false;
    for (offset, character) in text[content_start..].char_indices() {
        if character == '\n' || character == '\r' {
            return None;
        }
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if character == ']' {
            let content_end = content_start + offset;
            return Some((
                &text[content_start..content_end],
                span(content_start, content_end),
                content_end + 1,
            ));
        }
    }
    None
}

fn scan_inline_destination(
    text: &str,
    open_paren: usize,
) -> Option<(String, ArtifactRefSpanWire, usize)> {
    let destination_start = open_paren + 1;
    if text[destination_start..].starts_with('<') {
        let content_start = destination_start + 1;
        let mut escaped = false;
        for (offset, character) in text[content_start..].char_indices() {
            if character == '\n' || character == '\r' {
                return None;
            }
            if escaped {
                escaped = false;
                continue;
            }
            if character == '\\' {
                escaped = true;
                continue;
            }
            if character == '>' {
                let content_end = content_start + offset;
                let close = content_end + 1;
                if text[close..].starts_with(')') {
                    return Some((
                        unescape_markdown_text(
                            &text[content_start..content_end],
                        ),
                        span(content_start, content_end),
                        close + 1,
                    ));
                }
                return None;
            }
        }
        return None;
    }

    let mut escaped = false;
    let mut paren_depth = 0usize;
    for (offset, character) in text[destination_start..].char_indices() {
        if character == '\n' || character == '\r' || character.is_whitespace() {
            return None;
        }
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if character == '(' {
            paren_depth += 1;
            continue;
        }
        if character == ')' {
            let at = destination_start + offset;
            if paren_depth == 0 {
                return Some((
                    unescape_markdown_text(&text[destination_start..at]),
                    span(destination_start, at),
                    at + 1,
                ));
            }
            paren_depth -= 1;
        }
    }
    None
}

fn canonical_artifact_ref_from_document_text(
    raw: &str,
    known_labels: &BTreeSet<String>,
) -> Option<String> {
    let trimmed = raw.trim();
    let candidate = trimmed.strip_prefix('@').unwrap_or(trimmed);
    let (kind, _) = candidate.split_once(':')?;
    if !known_labels.contains(kind) {
        return None;
    }
    canonical_document_artifact_ref(candidate)
}

fn canonical_document_artifact_ref(value: &str) -> Option<String> {
    parse_artifact_ref_canonical(value)
        .ok()
        .map(|parsed| parsed.reference.rendered)
}

fn managed_links_table_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut search_start = 0usize;
    while let Some(relative_start) =
        text[search_start..].find(LINKS_BLOCK_START_MARKER)
    {
        let start = search_start + relative_start;
        let content_start = start + LINKS_BLOCK_START_MARKER.len();
        let end = text[content_start..]
            .find(LINKS_BLOCK_END_MARKER)
            .map(|relative_end| {
                content_start + relative_end + LINKS_BLOCK_END_MARKER.len()
            })
            .unwrap_or(text.len());
        ranges.push((start, end));
        search_start = end;
    }
    ranges
}

fn scan_document_urls(
    text: &str,
    occupied: &[(usize, usize)],
) -> Vec<ArtifactRefDocumentTargetWire> {
    static URL_PREFIX_RE: OnceLock<Regex> = OnceLock::new();
    let regex =
        URL_PREFIX_RE.get_or_init(|| Regex::new(r"(?i)https?://").unwrap());
    let mut links = Vec::new();
    for match_ in regex.find_iter(text) {
        let start = match_.start();
        if !has_url_left_context(text, start) {
            continue;
        }
        let end = trim_url_end(text, start, scan_url_end(text, start));
        if end <= start || overlaps(start, end, occupied) {
            continue;
        }
        let target = text[start..end].to_string();
        links.push(simple_document_link(
            ArtifactRefDocumentTargetKindWire::Url,
            start,
            end,
            start,
            target.clone(),
            target,
        ));
    }
    links
}

fn scan_url_end(text: &str, start: usize) -> usize {
    let mut end = start;
    let mut paren_depth = 0usize;
    for (offset, character) in text[start..].char_indices() {
        if character.is_whitespace()
            || matches!(
                character,
                '<' | '>' | '[' | ']' | '{' | '}' | '\'' | '"' | '`'
            )
        {
            break;
        }
        if character == '(' {
            paren_depth += 1;
        } else if character == ')' {
            if paren_depth == 0 {
                break;
            }
            paren_depth -= 1;
        }
        end = start + offset + character.len_utf8();
    }
    end
}

fn trim_url_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start {
        let Some(character) = text[start..end].chars().next_back() else {
            break;
        };
        if !URL_TRAILING_PUNCTUATION.contains(&character) {
            break;
        }
        end -= character.len_utf8();
    }
    end
}

fn scan_document_file_paths(
    text: &str,
    occupied: &[(usize, usize)],
) -> Vec<ArtifactRefDocumentTargetWire> {
    static FILE_PATH_RE: OnceLock<Regex> = OnceLock::new();
    let regex = FILE_PATH_RE.get_or_init(|| {
        Regex::new(
            r"@?(?:~?/[\w.+-][\w.+/-]*|\.{1,2}/[\w.+-][\w.+/-]*|\.[\w-]+/[\w.+/-]*|[\w-]+/[\w.+/-]*\.[\w]+)(?::\d+(?::\d+)?)?",
        )
        .unwrap()
    });
    let mut links = Vec::new();
    for match_ in regex.find_iter(text) {
        let start = match_.start();
        if !has_file_path_left_context(text, start) {
            continue;
        }
        let end = trim_file_path_end(text, start, match_.end());
        if end <= start || overlaps(start, end, occupied) {
            continue;
        }
        let target_start = if text[start..end].starts_with('@') {
            start + 1
        } else {
            start
        };
        let target = text[target_start..end].to_string();
        links.push(simple_document_link(
            ArtifactRefDocumentTargetKindWire::FilePath,
            start,
            end,
            target_start,
            text[start..end].to_string(),
            target,
        ));
    }
    links
}

fn trim_file_path_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start {
        let Some(character) = text[start..end].chars().next_back() else {
            break;
        };
        if character != '.' {
            break;
        }
        end -= character.len_utf8();
    }
    end
}

fn simple_document_link(
    target_kind: ArtifactRefDocumentTargetKindWire,
    start: usize,
    end: usize,
    target_start: usize,
    text: String,
    target: String,
) -> ArtifactRefDocumentTargetWire {
    let hosted_destination = (target_kind
        == ArtifactRefDocumentTargetKindWire::Url)
        .then(|| target.clone());
    ArtifactRefDocumentTargetWire {
        schema_version: ARTIFACT_REF_DOCUMENT_SCAN_WIRE_SCHEMA_VERSION,
        target_kind,
        text,
        target,
        well_formed: true,
        source_span: span(start, end),
        candidate_span: span(start, end),
        target_span: span(target_start, end),
        label_span: None,
        destination_span: None,
        reference_label: None,
        markdown_destination: None,
        hosted_destination,
        artifact_reference: None,
        quoted: false,
    }
}

fn unescape_markdown_text(raw: &str) -> String {
    let mut result = String::with_capacity(raw.len());
    let mut escaped = false;
    for character in raw.chars() {
        if escaped {
            result.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
        } else {
            result.push(character);
        }
    }
    if escaped {
        result.push('\\');
    }
    result
}

fn is_url_target(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn has_url_left_context(text: &str, start: usize) -> bool {
    start == 0
        || text[..start].chars().next_back().is_some_and(|character| {
            !character.is_alphanumeric() && character != '_'
        })
}

fn has_file_path_left_context(text: &str, start: usize) -> bool {
    start == 0
        || text[..start].chars().next_back().is_some_and(|character| {
            !(character.is_alphanumeric()
                || matches!(character, '/' | '@' | '.' | '_'))
        })
}

fn contains_position(ranges: &[(usize, usize)], position: usize) -> bool {
    ranges
        .iter()
        .any(|(start, end)| position >= *start && position < *end)
}

fn overlaps(start: usize, end: usize, ranges: &[(usize, usize)]) -> bool {
    ranges.iter().any(|(range_start, range_end)| {
        start < *range_end && *range_start < end
    })
}

/// Build the candidate for an `@kind:"…"` quoted argument.
///
/// `payload_start` points at the opening quote. Trailing-punctuation
/// trimming never applies here — a quoted argument ends at its own closing
/// quote, and an optional fragment right after the quote extends only to the
/// next whitespace/quote/backtick terminator.
#[allow(clippy::too_many_arguments)]
fn scan_quoted_candidate(
    text: &str,
    start: usize,
    kind_start: usize,
    kind_end: usize,
    separator: usize,
    payload_start: usize,
    kind: &str,
) -> ArtifactRefPromptCandidateWire {
    let content_start = payload_start + 1;
    let (close, terminated) = scan_quoted_argument(text, payload_start);

    if !terminated {
        let line_end = close;
        return ArtifactRefPromptCandidateWire {
            schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
            text: text[start..line_end].to_string(),
            reference: format!(
                "{kind}:{}",
                unescape_quoted_argument(&text[content_start..line_end])
            ),
            kind: kind.to_string(),
            well_formed: false,
            candidate_span: span(start, line_end),
            sigil_span: span(start, start + 1),
            kind_span: span(kind_start, kind_end),
            separator_span: span(separator, separator + 1),
            payload_span: span(content_start, line_end),
            fragment_span: None,
            quoted: true,
        };
    }

    let quote_end = close + 1;
    let argument = unescape_quoted_argument(&text[content_start..close]);
    let (end, fragment_start) =
        if kind != "bug" && text[quote_end..].starts_with('#') {
            let raw_fragment_end = text[quote_end..]
                .char_indices()
                .find_map(|(offset, character)| {
                    (character.is_whitespace()
                        || matches!(character, '"' | '\'' | '`'))
                    .then_some(quote_end + offset)
                })
                .unwrap_or(text.len());
            (raw_fragment_end, Some(quote_end))
        } else {
            (quote_end, None)
        };
    let fragment_text =
        fragment_start.map_or("", |fragment_start| &text[fragment_start..end]);
    let reference = format!("{kind}:{argument}{fragment_text}");

    ArtifactRefPromptCandidateWire {
        schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
        text: text[start..end].to_string(),
        well_formed: parse_artifact_ref(&reference).is_ok(),
        reference,
        kind: kind.to_string(),
        candidate_span: span(start, end),
        sigil_span: span(start, start + 1),
        kind_span: span(kind_start, kind_end),
        separator_span: span(separator, separator + 1),
        payload_span: span(content_start, close),
        fragment_span: fragment_start
            .map(|fragment_start| span(fragment_start, end)),
        quoted: true,
    }
}

/// Scan a quoted argument starting at its opening `"`.
///
/// Returns the byte offset of the terminator and whether it is the matching
/// closing quote. When unterminated, the terminator is either an embedded
/// newline (the argument never crosses a line boundary) or the end of text.
fn scan_quoted_argument(text: &str, payload_start: usize) -> (usize, bool) {
    let content_start = payload_start + 1;
    let mut chars = text[content_start..].char_indices().peekable();
    while let Some((offset, character)) = chars.next() {
        match character {
            '\\' => {
                if let Some(&(_, next)) = chars.peek() {
                    if next == '"' || next == '\\' {
                        chars.next();
                    }
                }
            }
            '"' => return (content_start + offset, true),
            '\n' => return (content_start + offset, false),
            _ => {}
        }
    }
    (text.len(), false)
}

/// Undo `\"` and `\\` escapes. Any other backslash is a literal backslash.
fn unescape_quoted_argument(raw: &str) -> String {
    let mut result = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '\\' {
            result.push(character);
            continue;
        }
        match chars.peek() {
            Some('"') | Some('\\') => result.push(chars.next().unwrap()),
            _ => result.push('\\'),
        }
    }
    result
}

/// Render `argument` as a bare or quoted-and-escaped artifact-ref argument.
///
/// Quoting is applied when the argument contains whitespace, a quote
/// character, a backtick, or a trailing character `trim_candidate_end` would
/// otherwise strip from an unquoted candidate.
pub fn quote_artifact_ref_argument(argument: &str) -> String {
    if !argument_needs_quoting(argument) {
        return argument.to_string();
    }
    let mut quoted = String::with_capacity(argument.len() + 2);
    quoted.push('"');
    for character in argument.chars() {
        if character == '"' || character == '\\' {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

fn argument_needs_quoting(argument: &str) -> bool {
    if argument.is_empty() {
        return false;
    }
    if argument.chars().any(|character| {
        character.is_whitespace() || matches!(character, '"' | '\'' | '`')
    }) {
        return true;
    }
    argument
        .chars()
        .next_back()
        .is_some_and(|character| TRAILING_PUNCTUATION.contains(&character))
}

fn has_allowed_left_context(text: &str, start: usize) -> bool {
    start == 0
        || text[..start].chars().next_back().is_some_and(|character| {
            character.is_whitespace()
                || matches!(
                    character,
                    '"' | '\'' | '`' | '(' | '[' | '{' | ',' | '='
                )
        })
}

fn trim_candidate_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start {
        let Some(character) = text[start..end].chars().next_back() else {
            break;
        };
        // A lone trailing colon is the kind separator for an incomplete
        // `@kind:` reference, not prose punctuation. Keep it so editor
        // completion and diagnostics can classify the empty payload.
        if character == ':' {
            let candidate = &text[start..end];
            let colon_count = candidate.matches(':').count();
            let kind = candidate.split_once(':').map(|(kind, _)| kind);
            if colon_count == 1 || kind == Some("file") && colon_count == 2 {
                break;
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn only(text: &str) -> ArtifactRefPromptCandidateWire {
        let mut candidates = scan_artifact_refs(text);
        assert_eq!(candidates.len(), 1, "{text}");
        candidates.remove(0)
    }

    fn document_links(text: &str) -> Vec<ArtifactRefDocumentTargetWire> {
        scan_artifact_ref_document_links(text, &["plan".to_string()])
            .links
            .into_iter()
            .filter(|link| link.well_formed)
            .collect()
    }

    #[test]
    fn document_scan_separates_visible_prompt_ref_from_canonical_target() {
        let source = r##"é @plans:"a b.md"#L3 and @src/app.py:42"##;
        let links = document_links(source);

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].text, r##"@plans:"a b.md"#L3"##);
        assert_eq!(links[0].target, "plan:a b.md#L3");
        assert_eq!(
            &source[links[0].source_span.start..links[0].source_span.end],
            links[0].text
        );
        assert_eq!(links[0].source_span.start, "é ".len());
        assert_eq!(
            links[1].target_kind,
            ArtifactRefDocumentTargetKindWire::FilePath
        );
        assert_eq!(links[1].text, "@src/app.py:42");
        assert_eq!(links[1].target, "src/app.py:42");
    }

    #[test]
    fn document_scan_handles_unsigiled_refs_for_known_kinds() {
        let links = scan_artifact_ref_document_links(
            "see plan:202609/pager_target_integrity.md and custom:guide.md",
            &["plan".to_string(), "custom".to_string()],
        );

        let targets = links
            .links
            .iter()
            .filter(|link| link.well_formed)
            .map(|link| link.target.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            targets,
            ["plan:202609/pager_target_integrity.md", "custom:guide.md"]
        );
    }

    #[test]
    fn document_scan_uses_markdown_destination_for_ordinary_links() {
        let links = document_links(
            "open [the plan](plan:202609/pager_target_integrity.md#L4) \
             and [source](src/sase/pager/link_scan.py:12)",
        );

        assert_eq!(links.len(), 2);
        assert_eq!(
            links[0].text,
            "[the plan](plan:202609/pager_target_integrity.md#L4)"
        );
        assert_eq!(links[0].target, "plan:202609/pager_target_integrity.md#L4");
        assert_eq!(
            links[1].target_kind,
            ArtifactRefDocumentTargetKindWire::FilePath
        );
        assert_eq!(links[1].target, "src/sase/pager/link_scan.py:12");
    }

    #[test]
    fn document_scan_keeps_generated_links_table_ref_and_hosted_url() {
        let document = concat!(
            "<!-- sase:links:start -->\n\n",
            "## Links\n\n",
            "| Relation | Artifact | Why |\n",
            "| --- | --- | --- |\n",
            "| implements | [plan:202609/capture_line_edge_cycling.md][2] | screenshot |\n\n",
            "[2]: https://github.com/bobs-org/bob-cli/blob/main/.sase/plans/202609/capture_line_edge_cycling.md\n\n",
            "<!-- sase:links:end -->\n"
        );
        let links = document_links(document);

        assert_eq!(links.len(), 2);
        assert_eq!(
            links[0].text,
            "[plan:202609/capture_line_edge_cycling.md][2]"
        );
        assert_eq!(links[0].target, "plan:202609/capture_line_edge_cycling.md");
        assert_eq!(
            links[0].hosted_destination.as_deref(),
            Some("https://github.com/bobs-org/bob-cli/blob/main/.sase/plans/202609/capture_line_edge_cycling.md")
        );
        assert_eq!(
            links[1].target_kind,
            ArtifactRefDocumentTargetKindWire::Url
        );
        assert_eq!(
            links[1].target,
            "https://github.com/bobs-org/bob-cli/blob/main/.sase/plans/202609/capture_line_edge_cycling.md"
        );
    }

    #[test]
    fn document_scan_keeps_urls_whole_without_inner_file_links() {
        let links = document_links(
            "see https://example.com/src/foo.py:12?q=a#frag and \
             https://example.com/a_(b). then src/foo.py:12",
        );

        assert_eq!(
            links
                .iter()
                .map(|link| (link.target_kind, link.target.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (
                    ArtifactRefDocumentTargetKindWire::Url,
                    "https://example.com/src/foo.py:12?q=a#frag"
                ),
                (
                    ArtifactRefDocumentTargetKindWire::Url,
                    "https://example.com/a_(b)"
                ),
                (ArtifactRefDocumentTargetKindWire::FilePath, "src/foo.py:12"),
            ]
        );
    }

    #[test]
    fn quoted_argument_with_spaces_parses_and_round_trips() {
        let candidate = only(r#"@plan:"a b.md""#);
        assert!(candidate.quoted);
        assert_eq!(candidate.text, r#"@plan:"a b.md""#);
        assert_eq!(candidate.reference, "plan:a b.md");
        assert!(candidate.well_formed, "{candidate:?}");
        assert_eq!(
            &candidate.text
                [candidate.payload_span.start..candidate.payload_span.end],
            "a b.md"
        );
    }

    #[test]
    fn quoted_argument_supports_escaped_quote_and_backslash() {
        let escaped_quote = only(r#"@plan:"say \"hi\".md""#);
        assert_eq!(escaped_quote.reference, r#"plan:say "hi".md"#);

        let escaped_backslash = only(r#"@plan:"a\\b.md""#);
        assert_eq!(escaped_backslash.reference, r"plan:a\b.md");

        let literal_backslash = only(r#"@plan:"a\zb.md""#);
        assert_eq!(literal_backslash.reference, r"plan:a\zb.md");
    }

    #[test]
    fn fragment_splits_after_the_closing_quote() {
        let candidate = only(r##"@plan:"a b.md"#L3"##);
        assert_eq!(candidate.reference, "plan:a b.md#L3");
        assert!(candidate.fragment_span.is_some());
        assert_eq!(
            &candidate.text[candidate.fragment_span.unwrap().start
                ..candidate.fragment_span.unwrap().end],
            "#L3"
        );
        assert!(candidate.well_formed);
    }

    #[test]
    fn unterminated_quote_ends_at_the_current_line_never_at_eof() {
        let text = "@plan:\"unterminated\nnext line @plan:ok.md";
        let candidates = scan_artifact_refs(text);
        assert_eq!(candidates.len(), 2);
        assert!(candidates[0].quoted);
        assert!(!candidates[0].well_formed);
        assert_eq!(candidates[0].text, "@plan:\"unterminated");
        assert_eq!(candidates[1].text, "@plan:ok.md");
    }

    #[test]
    fn unterminated_quote_at_true_eof_ends_at_end_of_text() {
        let candidate = only(r#"@plan:"unterminated"#);
        assert!(candidate.quoted);
        assert!(!candidate.well_formed);
        assert_eq!(candidate.text, r#"@plan:"unterminated"#);
    }

    #[test]
    fn quoted_trailing_punctuation_is_not_trimmed() {
        let candidate = only(r#"@plan:"a b.md.""#);
        assert_eq!(candidate.reference, "plan:a b.md.");
        let unquoted = only("@plan:a.md.");
        assert_eq!(unquoted.reference, "plan:a.md");
    }

    #[test]
    fn xprompt_argument_delimiters_are_allowed_left_context() {
        let candidates = scan_artifact_refs(
            "#work(@plan:a.md) %alt(left=@plans:b.md, right=@commit:sase@abcdef1)",
        );

        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.text.as_str())
                .collect::<Vec<_>>(),
            ["@plan:a.md", "@plans:b.md", "@commit:sase@abcdef1"]
        );
    }

    #[test]
    fn quote_artifact_ref_argument_round_trips_through_the_scanner() {
        for argument in [
            "plain",
            "has space",
            "trailing.period.",
            "with \"quote\"",
            "with backtick ` here",
        ] {
            let quoted = quote_artifact_ref_argument(argument);
            let text = format!("@plan:{quoted}");
            let candidate = only(&text);
            let (_, unescaped) = candidate.reference.split_once(':').unwrap();
            assert_eq!(
                unescaped, argument,
                "argument={argument} quoted={quoted}"
            );
        }
    }
}
