//! Shared Markdown numeric reference-link machinery.
//!
//! Commit footers and the `Referenced By` block both emit `[label]: dest`
//! reference definitions and both need the lowest-available-integer label
//! allocator, so the mechanics live here instead of being duplicated. This
//! module is not artifact-ref-specific, which is why it sits beside
//! `commit_footer.rs` rather than under `artifact_ref/`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::prompt_literal_zone_ranges;

pub const MARKDOWN_LINK_REFS_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkdownReferenceDefinitionWire {
    pub label: String,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkdownReferenceScanWire {
    pub schema_version: u64,
    /// First-definition-wins order: one entry per label, at most.
    pub definitions: Vec<MarkdownReferenceDefinitionWire>,
    /// Numeric reference uses (full, collapsed, and shortcut forms),
    /// deduped and sorted by numeric value.
    pub used_labels: Vec<String>,
}

/// Scan `document` for reference definitions and numeric reference uses.
///
/// Fenced code blocks and inline code (per `prompt_literal_zone_ranges`)
/// never reserve a label. Footnote labels (`[^1]`) live in their own
/// namespace and are ignored automatically, since they never match the
/// all-digits check used for both definitions and uses.
pub fn scan_markdown_reference_links(
    document: &str,
) -> MarkdownReferenceScanWire {
    let literal_zones = prompt_literal_zone_ranges(document);

    let mut definitions = Vec::new();
    let mut seen_definition_labels: BTreeSet<String> = BTreeSet::new();
    let mut line_start = 0usize;
    for line in document.split_inclusive('\n') {
        if !in_literal_zone(&literal_zones, line_start) {
            let trimmed = line.trim_end_matches(['\n', '\r']).trim();
            if let Some(reference) = parse_reference_definition(trimmed) {
                if seen_definition_labels.insert(reference.id.to_string()) {
                    definitions.push(MarkdownReferenceDefinitionWire {
                        label: reference.id.to_string(),
                        destination: reference.destination.to_string(),
                    });
                }
            }
        }
        line_start += line.len();
    }

    let mut used_labels: BTreeSet<String> = BTreeSet::new();
    let bytes = document.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'[' || in_literal_zone(&literal_zones, index) {
            index += 1;
            continue;
        }
        let Some((first_content, first_end)) = scan_bracket(document, index)
        else {
            index += 1;
            continue;
        };
        if bytes.get(first_end) == Some(&b'[') {
            if let Some((second_content, second_end)) =
                scan_bracket(document, first_end)
            {
                if second_content.is_empty() {
                    if is_numeric_label(first_content) {
                        used_labels.insert(first_content.to_string());
                    }
                } else if is_numeric_label(second_content) {
                    used_labels.insert(second_content.to_string());
                }
                index = second_end;
                continue;
            }
        }
        let trailing = bytes.get(first_end).copied();
        if is_numeric_label(first_content)
            && !matches!(trailing, Some(b'(') | Some(b':'))
        {
            used_labels.insert(first_content.to_string());
        }
        index = first_end;
    }

    let mut used_labels: Vec<String> = used_labels.into_iter().collect();
    used_labels.sort_by_key(|label| label.parse::<u128>().unwrap_or(u128::MAX));

    MarkdownReferenceScanWire {
        schema_version: MARKDOWN_LINK_REFS_WIRE_SCHEMA_VERSION,
        definitions,
        used_labels,
    }
}

/// Allocate a numeric reference label for `destination`.
///
/// Reuses an existing label (from the scan or already assigned this run)
/// whose destination already matches; otherwise returns the lowest positive
/// integer not used by any definition, any numeric use, or any label
/// assigned this run.
pub fn allocate_markdown_reference_label(
    scan: &MarkdownReferenceScanWire,
    destination: &str,
    assigned: &BTreeMap<String, String>,
) -> String {
    if let Some(definition) = scan
        .definitions
        .iter()
        .find(|definition| definition.destination == destination)
    {
        return definition.label.clone();
    }
    if let Some((label, _)) = assigned
        .iter()
        .find(|(_, dest)| dest.as_str() == destination)
    {
        return label.clone();
    }

    let mut reserved: BTreeSet<String> =
        scan.definitions.iter().map(|d| d.label.clone()).collect();
    reserved.extend(scan.used_labels.iter().cloned());
    reserved.extend(assigned.keys().cloned());
    allocate_numeric_id(&reserved, &BTreeMap::new())
}

/// Append every definition in `definitions` that is missing from `document`.
///
/// Definitions are appended in numeric label order at the bottom, separated
/// from the existing document by exactly one blank line. A definition that
/// already exists with the same destination is not re-emitted. The
/// document's trailing-newline shape is preserved, and a second call with
/// the same input is byte-identical to the first.
pub fn append_markdown_reference_definitions(
    document: &str,
    definitions: &[MarkdownReferenceDefinitionWire],
) -> String {
    let scan = scan_markdown_reference_links(document);
    let existing: BTreeMap<&str, &str> = scan
        .definitions
        .iter()
        .map(|definition| {
            (definition.label.as_str(), definition.destination.as_str())
        })
        .collect();

    let mut missing: Vec<&MarkdownReferenceDefinitionWire> = definitions
        .iter()
        .filter(|definition| {
            existing.get(definition.label.as_str())
                != Some(&definition.destination.as_str())
        })
        .collect();
    if missing.is_empty() {
        return document.to_string();
    }
    missing.sort_by_key(|definition| {
        definition.label.parse::<u128>().unwrap_or(u128::MAX)
    });

    let block = missing
        .iter()
        .map(|definition| {
            format!("[{}]: {}", definition.label, definition.destination)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let trimmed = document.trim_end_matches('\n');
    let body = if trimmed.is_empty() {
        block
    } else {
        format!("{trimmed}\n\n{block}")
    };
    with_trailing_newline_like(document, body)
}

/// Render `body` with the same trailing-newline shape as `reference`: one
/// trailing newline when `reference` had any, none otherwise.
pub(crate) fn with_trailing_newline_like(
    reference: &str,
    body: String,
) -> String {
    if reference.ends_with('\n') {
        if body.ends_with('\n') {
            body
        } else {
            format!("{body}\n")
        }
    } else {
        body.trim_end_matches('\n').to_string()
    }
}

fn in_literal_zone(zones: &[(usize, usize)], position: usize) -> bool {
    zones
        .iter()
        .any(|(start, end)| position >= *start && position < *end)
}

fn scan_bracket(text: &str, start: usize) -> Option<(&str, usize)> {
    let content_start = start + 1;
    let relative_close = text[content_start..].find([']', '\n'])?;
    if text.as_bytes()[content_start + relative_close] != b']' {
        return None;
    }
    let content_end = content_start + relative_close;
    Some((&text[content_start..content_end], content_end + 1))
}

fn is_numeric_label(content: &str) -> bool {
    !content.is_empty() && content.bytes().all(|byte| byte.is_ascii_digit())
}

#[derive(Clone, Debug)]
pub(crate) struct ParsedReference<'a> {
    pub(crate) id: &'a str,
    pub(crate) destination: &'a str,
}

pub(crate) fn parse_reference_definition(
    line: &str,
) -> Option<ParsedReference<'_>> {
    if !line.starts_with('[') {
        return None;
    }
    let separator = line.find("]:")?;
    let id = &line[1..separator];
    let destination = line[separator + 2..].trim();
    (!id.is_empty() && !id.contains(']') && !destination.is_empty())
        .then_some(ParsedReference { id, destination })
}

pub(crate) fn allocate_numeric_id(
    reserved_ids: &BTreeSet<String>,
    used_ids: &BTreeMap<String, String>,
) -> String {
    let mut number = 1_u64;
    loop {
        let candidate = number.to_string();
        if !reserved_ids.contains(&candidate)
            && !used_ids.contains_key(&candidate)
        {
            return candidate;
        }
        number += 1;
    }
}

/// Assign a reference id for one `destination`, preferring an id already in
/// use for the same destination (this run or the original document), then a
/// caller-preferred id, then the lowest unused number.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assign_reference_id(
    destination: &str,
    preferred_id: Option<&str>,
    existing_target_ids: &BTreeMap<String, Vec<String>>,
    reserved_ids: &BTreeSet<String>,
    used_ids: &mut BTreeMap<String, String>,
    target_ids: &mut BTreeMap<String, Vec<String>>,
    reference_order: &mut Vec<String>,
) -> String {
    let preferred = preferred_id.and_then(sanitize);
    let mut candidates = Vec::new();
    if let Some(id) = preferred {
        candidates.push(id);
    }
    if let Some(ids) = target_ids.get(destination) {
        candidates.extend(ids.iter().cloned());
    }
    if let Some(ids) = existing_target_ids.get(destination) {
        candidates.extend(ids.iter().cloned());
    }

    let id = candidates
        .into_iter()
        .find(|id| match used_ids.get(id) {
            Some(used) => used == destination,
            None => true,
        })
        .unwrap_or_else(|| allocate_numeric_id(reserved_ids, used_ids));
    if !used_ids.contains_key(&id) {
        used_ids.insert(id.clone(), destination.to_string());
        target_ids
            .entry(destination.to_string())
            .or_default()
            .push(id.clone());
        reference_order.push(id.clone());
    }
    id
}

pub(crate) fn sanitize(value: &str) -> Option<String> {
    let cleaned = value.replace(['\r', '\n'], " ");
    let trimmed = cleaned.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_collects_definitions_first_wins_and_uses_all_three_forms() {
        let document = "See [text][7], [3][], and [9].\n\n[7]: https://a\n[7]: https://b\n[9]: https://c\n";
        let scan = scan_markdown_reference_links(document);
        assert_eq!(
            scan.definitions,
            vec![
                MarkdownReferenceDefinitionWire {
                    label: "7".to_string(),
                    destination: "https://a".to_string(),
                },
                MarkdownReferenceDefinitionWire {
                    label: "9".to_string(),
                    destination: "https://c".to_string(),
                },
            ]
        );
        assert_eq!(scan.used_labels, ["3", "7", "9"]);
    }

    #[test]
    fn scan_ignores_footnotes_inline_links_definitions_and_literal_zones() {
        let document = "[^1] and [2](https://x) and [3]: https://y and `[4]` and\n```text\n[5]\n```\nreal [6]\n";
        let scan = scan_markdown_reference_links(document);
        assert_eq!(scan.used_labels, ["6"]);
    }

    #[test]
    fn scan_skips_duplicate_bracket_pairs_used_as_link_text() {
        let scan = scan_markdown_reference_links("[7][not-numeric]");
        assert!(scan.used_labels.is_empty());
    }

    #[test]
    fn allocate_reuses_matching_destination_before_picking_a_new_number() {
        let scan = scan_markdown_reference_links(
            "[1]: https://same\n[2]: https://other\n",
        );
        assert_eq!(
            allocate_markdown_reference_label(
                &scan,
                "https://same",
                &BTreeMap::new()
            ),
            "1"
        );
        let assigned =
            BTreeMap::from([("3".to_string(), "https://new".to_string())]);
        assert_eq!(
            allocate_markdown_reference_label(&scan, "https://new", &assigned),
            "3"
        );
        assert_eq!(
            allocate_markdown_reference_label(
                &scan,
                "https://fresh",
                &assigned
            ),
            "4"
        );
    }

    #[test]
    fn allocate_fills_gaps_between_existing_definitions() {
        let scan = scan_markdown_reference_links(
            "[1]: https://one\n[3]: https://three\n",
        );
        assert_eq!(
            allocate_markdown_reference_label(
                &scan,
                "https://fresh",
                &BTreeMap::new()
            ),
            "2"
        );
    }

    #[test]
    fn allocate_reserves_a_dangling_numeric_use_with_no_definition() {
        // `[x][3]` uses label 3 with no `[3]: ...` definition anywhere.
        // Allocating a fresh label must not hand out "3" and retroactively
        // turn that dangling use into a link to unrelated content.
        let scan = scan_markdown_reference_links(
            "dangling [x][3]\n\n[1]: https://one\n",
        );
        assert_eq!(scan.used_labels, ["3"]);
        assert_eq!(
            allocate_markdown_reference_label(
                &scan,
                "https://fresh",
                &BTreeMap::new()
            ),
            "2"
        );
    }

    #[test]
    fn append_definitions_in_numeric_order_and_skips_matching_ones() {
        let document = "Body\n\n[1]: https://one\n";
        let appended = append_markdown_reference_definitions(
            document,
            &[
                MarkdownReferenceDefinitionWire {
                    label: "1".to_string(),
                    destination: "https://one".to_string(),
                },
                MarkdownReferenceDefinitionWire {
                    label: "3".to_string(),
                    destination: "https://three".to_string(),
                },
                MarkdownReferenceDefinitionWire {
                    label: "2".to_string(),
                    destination: "https://two".to_string(),
                },
            ],
        );
        assert_eq!(
            appended,
            "Body\n\n[1]: https://one\n\n[2]: https://two\n[3]: https://three\n"
        );
    }

    #[test]
    fn append_is_idempotent_and_preserves_trailing_newline_shape() {
        let definitions = [MarkdownReferenceDefinitionWire {
            label: "1".to_string(),
            destination: "https://one".to_string(),
        }];
        let no_trailing_newline = "Body";
        let once = append_markdown_reference_definitions(
            no_trailing_newline,
            &definitions,
        );
        assert!(!once.ends_with('\n'));
        let twice = append_markdown_reference_definitions(&once, &definitions);
        assert_eq!(once, twice);

        let with_trailing_newline = "Body\n";
        let once = append_markdown_reference_definitions(
            with_trailing_newline,
            &definitions,
        );
        assert!(once.ends_with('\n'));
        assert!(!once.ends_with("\n\n"));
        let twice = append_markdown_reference_definitions(&once, &definitions);
        assert_eq!(once, twice);
    }
}
