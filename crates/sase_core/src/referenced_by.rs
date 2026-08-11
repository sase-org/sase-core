//! Bottom-anchored `Referenced By` managed block.
//!
//! Mirrors the proven properties of `plan/artifact_link.rs`'s header block —
//! deterministic render, tolerant parse, idempotent upsert — but anchored at
//! the bottom of the document instead of the top, since citations are
//! appended as they accumulate rather than declared up front.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::artifact_ref::ArtifactRefError;
use crate::markdown_link_refs::{
    allocate_markdown_reference_label, parse_reference_definition,
    scan_markdown_reference_links, with_trailing_newline_like,
    MarkdownReferenceDefinitionWire,
};

pub const REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION: u64 = 1;
pub const MAX_RENDERED_REFERENCED_BY_ROWS: usize = 50;

const START_MARKER: &str = "<!-- sase:referenced-by:start -->";
const END_MARKER: &str = "<!-- sase:referenced-by:end -->";
const HEADING: &str = "## Referenced By";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedByColumnWire {
    pub key: String,
    pub label: String,
    pub numeric: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedByRowWire {
    pub values: BTreeMap<String, String>,
    #[serde(default)]
    pub link_targets: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedByTableWire {
    pub schema_version: u64,
    pub columns: Vec<ReferencedByColumnWire>,
    pub rows: Vec<ReferencedByRowWire>,
    #[serde(default)]
    pub omitted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedByDocumentWire {
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<ReferencedByTableWire>,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Render one `Referenced By` block's heading, table, and link definitions.
///
/// Rows sort deterministically by column values in declared column order,
/// cap at [`MAX_RENDERED_REFERENCED_BY_ROWS`], and a linked cell is numbered
/// through `markdown_link_refs` rather than a second allocator.
pub fn render_referenced_by_block(
    table: &ReferencedByTableWire,
) -> Result<String, ArtifactRefError> {
    if table.schema_version != REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION {
        return Err(ArtifactRefError::validation(format!(
            "unsupported referenced-by table schema_version {}; expected {}",
            table.schema_version, REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION
        )));
    }
    if table.columns.is_empty() {
        return Err(ArtifactRefError::validation(
            "referenced-by table must declare at least one column",
        ));
    }

    let mut rows = table.rows.clone();
    rows.sort_by(|a, b| compare_rows(&table.columns, a, b));
    let overflow = rows.len().saturating_sub(MAX_RENDERED_REFERENCED_BY_ROWS);
    let omitted = table.omitted + overflow;
    rows.truncate(MAX_RENDERED_REFERENCED_BY_ROWS);

    let empty_scan = scan_markdown_reference_links("");
    let mut assigned: BTreeMap<String, String> = BTreeMap::new();
    let mut definitions: Vec<MarkdownReferenceDefinitionWire> = Vec::new();

    let mut lines = vec![
        table_row(
            table
                .columns
                .iter()
                .map(|column| escape_cell(&column.label)),
        ),
        table_row(table.columns.iter().map(|column| {
            if column.numeric { "---:" } else { "---" }.to_string()
        })),
    ];
    for row in &rows {
        let mut cells = Vec::with_capacity(table.columns.len());
        for column in &table.columns {
            let raw_value = row
                .values
                .get(&column.key)
                .map(String::as_str)
                .unwrap_or("");
            let escaped = escape_cell(raw_value);
            if let Some(destination) = row.link_targets.get(&column.key) {
                let label = allocate_markdown_reference_label(
                    &empty_scan,
                    destination,
                    &assigned,
                );
                if assigned
                    .insert(label.clone(), destination.clone())
                    .is_none()
                {
                    definitions.push(MarkdownReferenceDefinitionWire {
                        label: label.clone(),
                        destination: destination.clone(),
                    });
                }
                cells.push(format!("[{escaped}][{label}]"));
            } else {
                cells.push(escaped);
            }
        }
        lines.push(table_row(cells));
    }

    let mut block = format!("{HEADING}\n\n{}", lines.join("\n"));
    if omitted > 0 {
        block.push_str(&format!("\n\n_… and {omitted} more_"));
    }
    if !definitions.is_empty() {
        definitions
            .sort_by_key(|d| d.label.parse::<u128>().unwrap_or(u128::MAX));
        let def_block = definitions
            .iter()
            .map(|d| format!("[{}]: {}", d.label, d.destination))
            .collect::<Vec<_>>()
            .join("\n");
        block.push_str(&format!("\n\n{def_block}"));
    }
    Ok(block)
}

/// Parse the managed `Referenced By` block out of `document`.
///
/// Marker recovery: a duplicate block collapses to one; an unterminated
/// start marker is treated as extending to end of document; a stray end
/// marker with no start is left alone. All three are reported through
/// `reason`.
pub fn parse_referenced_by_block(document: &str) -> ReferencedByDocumentWire {
    let starts = find_all(document, START_MARKER);
    let ends = find_all(document, END_MARKER);

    let Some(&first_start) = starts.first() else {
        let reason = if ends.is_empty() {
            None
        } else {
            Some(
                "stray referenced-by end marker with no start marker"
                    .to_string(),
            )
        };
        return ReferencedByDocumentWire {
            schema_version: REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION,
            table: None,
            body: document.to_string(),
            reason,
        };
    };

    let ends_at_or_after_start: Vec<usize> = ends
        .iter()
        .copied()
        .filter(|&end| end >= first_start)
        .collect();
    let stray_ends_before_start = ends.iter().any(|&end| end < first_start);

    let (block_end, terminated) = match ends_at_or_after_start.last() {
        Some(&end) => (end + END_MARKER.len(), true),
        None => (document.len(), false),
    };

    let raw_block = &document[first_start..block_end];
    let inner = extract_inner(raw_block, terminated);
    let table = parse_table_block(inner);

    let mut body =
        String::with_capacity(document.len() - (block_end - first_start));
    body.push_str(&document[..first_start]);
    body.push_str(&document[block_end..]);

    let mut reason: Option<String> = None;
    let mut push_reason = |note: &str| {
        reason = Some(match reason.take() {
            Some(existing) => format!("{existing}; {note}"),
            None => note.to_string(),
        });
    };
    if starts.len() > 1 || ends_at_or_after_start.len() > 1 {
        push_reason("duplicate referenced-by block collapsed to one");
    }
    if stray_ends_before_start {
        push_reason(
            "stray referenced-by end marker with no start marker before the managed block",
        );
    }
    if !terminated {
        push_reason(
            "unterminated referenced-by start marker; treated as extending to end of document",
        );
    }

    ReferencedByDocumentWire {
        schema_version: REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION,
        table,
        body,
        reason,
    }
}

/// Insert, replace, or remove the managed block so it reflects `table`.
///
/// The block sits at the very bottom, separated from the rest of the
/// document by exactly one blank line, and the document's trailing-newline
/// shape is preserved. An empty table removes the block entirely. A second
/// call with the same input is byte-identical to the first.
pub fn upsert_referenced_by_block(
    document: &str,
    table: &ReferencedByTableWire,
) -> Result<String, ArtifactRefError> {
    if table.rows.is_empty() {
        return Ok(remove_referenced_by_block(document));
    }
    let rendered = render_referenced_by_block(table)?;
    let block = format!("{START_MARKER}\n\n{rendered}\n\n{END_MARKER}");
    let parsed = parse_referenced_by_block(document);
    let trimmed_body = parsed.body.trim_end_matches('\n');
    let combined = if trimmed_body.is_empty() {
        block
    } else {
        format!("{trimmed_body}\n\n{block}")
    };
    Ok(with_trailing_newline_like(document, combined))
}

/// Remove the managed block, if present, preserving document shape.
pub fn remove_referenced_by_block(document: &str) -> String {
    if !document.contains(START_MARKER) {
        return document.to_string();
    }
    let parsed = parse_referenced_by_block(document);
    with_trailing_newline_like(
        document,
        parsed.body.trim_end_matches('\n').to_string(),
    )
}

/// Strip the managed block for content hashing and change detection.
///
/// A citation never counts as a new version of the cited artifact: digest
/// and diff logic should run on this function's output, not on the raw
/// document.
pub fn strip_referenced_by_block(document: &str) -> String {
    if !document.contains(START_MARKER) {
        return document.to_string();
    }
    parse_referenced_by_block(document)
        .body
        .trim_end()
        .to_string()
}

fn compare_rows(
    columns: &[ReferencedByColumnWire],
    a: &ReferencedByRowWire,
    b: &ReferencedByRowWire,
) -> Ordering {
    for column in columns {
        let left = a.values.get(&column.key).map(String::as_str).unwrap_or("");
        let right = b.values.get(&column.key).map(String::as_str).unwrap_or("");
        match left.cmp(right) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    Ordering::Equal
}

fn table_row<I, S>(cells: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let joined = cells
        .into_iter()
        .map(|cell| cell.as_ref().to_string())
        .collect::<Vec<_>>()
        .join(" | ");
    format!("| {joined} |")
}

fn escape_cell(value: &str) -> String {
    value
        .replace('|', "\\|")
        .replace("\r\n", "\n")
        .replace(['\r', '\n'], "<br>")
}

fn unescape_cell(value: &str) -> String {
    value.replace("<br>", "\n").replace("\\|", "|")
}

fn find_all(document: &str, marker: &str) -> Vec<usize> {
    document
        .match_indices(marker)
        .map(|(index, _)| index)
        .collect()
}

fn extract_inner(raw_block: &str, terminated: bool) -> &str {
    let after_start = raw_block.strip_prefix(START_MARKER).unwrap_or(raw_block);
    if terminated {
        after_start.strip_suffix(END_MARKER).unwrap_or(after_start)
    } else {
        after_start
    }
}

fn parse_table_block(inner: &str) -> Option<ReferencedByTableWire> {
    let mut definitions_map: BTreeMap<&str, &str> = BTreeMap::new();
    for line in inner.lines() {
        if let Some(reference) = parse_reference_definition(line.trim()) {
            definitions_map
                .entry(reference.id)
                .or_insert(reference.destination);
        }
    }

    let mut lines = inner.lines();
    let heading = lines.find(|line| !line.trim().is_empty())?;
    if heading.trim() != HEADING {
        return None;
    }
    let header_line = lines.find(|line| !line.trim().is_empty())?;
    let alignment_line = lines.next()?;
    let header_cells = split_table_row(header_line)?;
    let alignment_cells = split_table_row(alignment_line)?;
    if header_cells.is_empty() || header_cells.len() != alignment_cells.len() {
        return None;
    }

    let columns: Vec<ReferencedByColumnWire> = header_cells
        .iter()
        .zip(&alignment_cells)
        .map(|(label, alignment)| ReferencedByColumnWire {
            key: slug_key(label),
            label: unescape_cell(label),
            numeric: alignment.trim().ends_with(':'),
        })
        .collect();

    let mut rows = Vec::new();
    let mut trailer_lines = Vec::new();
    let mut in_rows = true;
    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            in_rows = false;
            continue;
        }
        if !in_rows {
            trailer_lines.push(trimmed);
            continue;
        }
        let Some(cells) = split_table_row(line) else {
            in_rows = false;
            trailer_lines.push(trimmed);
            continue;
        };
        if cells.len() != columns.len() {
            in_rows = false;
            trailer_lines.push(trimmed);
            continue;
        }
        let mut values = BTreeMap::new();
        let mut link_targets = BTreeMap::new();
        for (column, cell) in columns.iter().zip(&cells) {
            let (value, label) = parse_cell(cell);
            values.insert(column.key.clone(), value);
            if let Some(label) = label {
                if let Some(&destination) = definitions_map.get(label.as_str())
                {
                    link_targets
                        .insert(column.key.clone(), destination.to_string());
                }
            }
        }
        rows.push(ReferencedByRowWire {
            values,
            link_targets,
        });
    }

    let omitted = trailer_lines
        .iter()
        .find_map(|line| parse_omitted_line(line))
        .unwrap_or(0);

    Some(ReferencedByTableWire {
        schema_version: REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION,
        columns,
        rows,
        omitted,
    })
}

fn parse_cell(cell: &str) -> (String, Option<String>) {
    let trimmed = cell.trim();
    if let Some(rest) = trimmed.strip_prefix('[') {
        if let Some(split) = rest.find("][") {
            let value = &rest[..split];
            let after = &rest[split + 2..];
            if let Some(label_end) = after.find(']') {
                if after[label_end + 1..].is_empty() {
                    let label = &after[..label_end];
                    return (unescape_cell(value), Some(label.to_string()));
                }
            }
        }
    }
    (unescape_cell(trimmed), None)
}

fn split_table_row(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    let trimmed = trimmed.strip_prefix('|')?;
    let trimmed = trimmed.strip_suffix('|').unwrap_or(trimmed);
    let mut cells = Vec::new();
    let mut current = String::new();
    let mut chars = trimmed.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\\' {
            if let Some(&next) = chars.peek() {
                current.push(character);
                current.push(next);
                chars.next();
                continue;
            }
        }
        if character == '|' {
            cells.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(character);
        }
    }
    cells.push(current.trim().to_string());
    Some(cells)
}

fn parse_omitted_line(line: &str) -> Option<usize> {
    let inner = line.strip_prefix('_')?.strip_suffix('_')?;
    let count = inner.strip_prefix("… and ")?.strip_suffix(" more")?;
    count.parse().ok()
}

fn slug_key(label: &str) -> String {
    let mut slug = String::new();
    let mut last_was_separator = true;
    for character in label.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            last_was_separator = false;
        } else if !last_was_separator {
            slug.push('_');
            last_was_separator = true;
        }
    }
    slug.trim_end_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(rows: Vec<ReferencedByRowWire>) -> ReferencedByTableWire {
        ReferencedByTableWire {
            schema_version: REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION,
            columns: vec![
                ReferencedByColumnWire {
                    key: "agent".to_string(),
                    label: "Agent".to_string(),
                    numeric: false,
                },
                ReferencedByColumnWire {
                    key: "count".to_string(),
                    label: "Count".to_string(),
                    numeric: true,
                },
            ],
            rows,
            omitted: 0,
        }
    }

    fn row(
        agent: &str,
        count: &str,
        link: Option<&str>,
    ) -> ReferencedByRowWire {
        let mut values = BTreeMap::new();
        values.insert("agent".to_string(), agent.to_string());
        values.insert("count".to_string(), count.to_string());
        let mut link_targets = BTreeMap::new();
        if let Some(destination) = link {
            link_targets.insert("agent".to_string(), destination.to_string());
        }
        ReferencedByRowWire {
            values,
            link_targets,
        }
    }

    #[test]
    fn render_sorts_rows_and_numbers_links_through_the_shared_allocator() {
        let table = table(vec![
            row("zeta", "1", Some("https://z")),
            row("alpha", "2", Some("https://z")),
            row("beta", "3", None),
        ]);
        let rendered = render_referenced_by_block(&table).unwrap();
        assert_eq!(
            rendered,
            "## Referenced By\n\n\
             | Agent | Count |\n\
             | --- | ---: |\n\
             | [alpha][1] | 2 |\n\
             | beta | 3 |\n\
             | [zeta][1] | 1 |\n\n\
             [1]: https://z"
        );
    }

    #[test]
    fn render_caps_at_fifty_rows_and_reports_omitted() {
        let rows = (0..60)
            .map(|index| row(&format!("agent{index:02}"), "1", None))
            .collect();
        let mut table = table(rows);
        table.omitted = 5;
        let rendered = render_referenced_by_block(&table).unwrap();
        assert!(rendered.contains("_… and 15 more_"));
        assert_eq!(
            rendered.matches("| agent").count(),
            MAX_RENDERED_REFERENCED_BY_ROWS
        );
    }

    #[test]
    fn upsert_places_block_at_bottom_and_is_idempotent() {
        let table = table(vec![row("alpha", "1", None)]);
        let document = "# Doc\n\nBody text.\n";
        let once = upsert_referenced_by_block(document, &table).unwrap();
        assert!(once.starts_with(
            "# Doc\n\nBody text.\n\n<!-- sase:referenced-by:start -->\n"
        ));
        assert!(once.ends_with("<!-- sase:referenced-by:end -->\n"));
        let twice = upsert_referenced_by_block(&once, &table).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn upsert_with_empty_table_removes_the_block() {
        let table = table(vec![row("alpha", "1", None)]);
        let document = "# Doc\n\nBody text.\n";
        let with_block = upsert_referenced_by_block(document, &table).unwrap();
        let empty = table.clone();
        let mut empty = empty;
        empty.rows.clear();
        let removed = upsert_referenced_by_block(&with_block, &empty).unwrap();
        assert_eq!(removed, document);
    }

    #[test]
    fn render_parse_round_trips() {
        let table = table(vec![
            row("alpha", "1", Some("https://alpha")),
            row("beta", "2", None),
        ]);
        let rendered = render_referenced_by_block(&table).unwrap();
        let full_document =
            format!("{START_MARKER}\n\n{rendered}\n\n{END_MARKER}\n");
        let parsed = parse_referenced_by_block(&full_document);
        assert_eq!(parsed.table, Some(table));
        assert_eq!(parsed.body, "\n");
        assert!(parsed.reason.is_none());
    }

    #[test]
    fn duplicate_block_collapses_to_one() {
        let table = table(vec![row("alpha", "1", None)]);
        let block = format!(
            "{START_MARKER}\n\n{}\n\n{END_MARKER}",
            render_referenced_by_block(&table).unwrap()
        );
        let document = format!("Body\n\n{block}\n\n{block}\n");
        let parsed = parse_referenced_by_block(&document);
        assert_eq!(parsed.body, "Body\n\n\n");
        assert_eq!(
            parsed.reason.as_deref(),
            Some("duplicate referenced-by block collapsed to one")
        );
    }

    #[test]
    fn unterminated_start_marker_extends_to_eof() {
        let document = format!("Body\n\n{START_MARKER}\nstray content\n");
        let parsed = parse_referenced_by_block(&document);
        assert_eq!(parsed.body, "Body\n\n");
        assert_eq!(
            parsed.reason.as_deref(),
            Some("unterminated referenced-by start marker; treated as extending to end of document")
        );
    }

    #[test]
    fn stray_end_marker_with_no_start_is_left_alone() {
        let document = format!("Body\n\n{END_MARKER}\nmore\n");
        let parsed = parse_referenced_by_block(&document);
        assert_eq!(parsed.body, document);
        assert_eq!(
            parsed.reason.as_deref(),
            Some("stray referenced-by end marker with no start marker")
        );
        assert!(parsed.table.is_none());
    }

    #[test]
    fn strip_leaves_a_document_with_no_block_untouched() {
        let document = "Just prose, no managed block.\n";
        assert_eq!(strip_referenced_by_block(document), document);
        assert_eq!(remove_referenced_by_block(document), document);
    }

    #[test]
    fn strip_normalizes_trailing_whitespace_for_stable_digests() {
        let table = table(vec![row("alpha", "1", None)]);
        let document = "Body\n";
        let with_block = upsert_referenced_by_block(document, &table).unwrap();
        assert_eq!(strip_referenced_by_block(&with_block), "Body");
        assert_eq!(remove_referenced_by_block(&with_block), "Body\n");
    }
}
