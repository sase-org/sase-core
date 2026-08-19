//! Marker-bounded Markdown table blocks, top- or bottom-anchored.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::artifact_ref::ArtifactRefError;
use crate::markdown_link_refs::{
    allocate_markdown_reference_label, parse_reference_definition,
    scan_markdown_reference_links, with_trailing_newline_like,
    MarkdownReferenceDefinitionWire, MarkdownReferenceScanWire,
};
use crate::plan::artifact_link::sdd_plan_header_block_span;
use crate::plan::artifact_link::split_document;

pub const MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION: u64 = 1;
pub const MAX_RENDERED_MANAGED_TABLE_ROWS: usize = 50;

pub const REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION: u64 =
    MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION;
pub const MAX_RENDERED_REFERENCED_BY_ROWS: usize =
    MAX_RENDERED_MANAGED_TABLE_ROWS;
pub const REFERENCED_BY_BLOCK_START_MARKER: &str =
    "<!-- sase:referenced-by:start -->";
pub const REFERENCED_BY_BLOCK_END_MARKER: &str =
    "<!-- sase:referenced-by:end -->";
pub const REFERENCED_BY_BLOCK_HEADING: &str = "## Referenced By";

pub const LINKS_BLOCK_WIRE_SCHEMA_VERSION: u64 =
    MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION;
pub const LINKS_BLOCK_START_MARKER: &str = "<!-- sase:links:start -->";
pub const LINKS_BLOCK_END_MARKER: &str = "<!-- sase:links:end -->";
pub const LINKS_BLOCK_HEADING: &str = "## Links";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedTableAnchorWire {
    Top,
    Bottom,
}

/// One managed Markdown table instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedTableBlock {
    pub start_marker: String,
    pub end_marker: String,
    pub heading: String,
    pub anchor: ManagedTableAnchorWire,
    /// Phrase used in recovery `reason` strings (`referenced-by`, `links`).
    pub reason_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedTableColumnWire {
    pub key: String,
    pub label: String,
    pub numeric: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedTableRowWire {
    pub values: BTreeMap<String, String>,
    #[serde(default)]
    pub link_targets: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedTableTableWire {
    pub schema_version: u64,
    pub columns: Vec<ManagedTableColumnWire>,
    pub rows: Vec<ManagedTableRowWire>,
    #[serde(default)]
    pub omitted: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pointer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedTableDocumentWire {
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<ManagedTableTableWire>,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

pub fn referenced_by_table_block() -> ManagedTableBlock {
    ManagedTableBlock {
        start_marker: REFERENCED_BY_BLOCK_START_MARKER.to_string(),
        end_marker: REFERENCED_BY_BLOCK_END_MARKER.to_string(),
        heading: REFERENCED_BY_BLOCK_HEADING.to_string(),
        anchor: ManagedTableAnchorWire::Bottom,
        reason_label: "referenced-by".to_string(),
    }
}

pub fn links_table_block() -> ManagedTableBlock {
    ManagedTableBlock {
        start_marker: LINKS_BLOCK_START_MARKER.to_string(),
        end_marker: LINKS_BLOCK_END_MARKER.to_string(),
        heading: LINKS_BLOCK_HEADING.to_string(),
        anchor: ManagedTableAnchorWire::Top,
        reason_label: "links".to_string(),
    }
}

/// Render one managed block's heading, table, optional pointer, and defs.
///
/// When `host_scan` is `None`, labels are allocated in isolation (the
/// historical Referenced By behavior). When it is `Some`, allocation reuses
/// [`scan_markdown_reference_links`] so the block never renumbers the host
/// document's own refs.
pub fn render_managed_table_block(
    block: &ManagedTableBlock,
    table: &ManagedTableTableWire,
    host_scan: Option<&MarkdownReferenceScanWire>,
) -> Result<String, ArtifactRefError> {
    if table.schema_version != MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION {
        return Err(ArtifactRefError::validation(format!(
            "unsupported {} table schema_version {}; expected {}",
            block.reason_label,
            table.schema_version,
            MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION
        )));
    }
    if table.columns.is_empty() {
        return Err(ArtifactRefError::validation(format!(
            "{} table must declare at least one column",
            block.reason_label
        )));
    }

    let mut rows = table.rows.clone();
    rows.sort_by(|a, b| compare_rows(&table.columns, a, b));
    let overflow = rows.len().saturating_sub(MAX_RENDERED_MANAGED_TABLE_ROWS);
    let omitted = table.omitted + overflow;
    rows.truncate(MAX_RENDERED_MANAGED_TABLE_ROWS);

    let empty_scan = scan_markdown_reference_links("");
    let scan = host_scan.unwrap_or(&empty_scan);
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
                    scan,
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

    let mut rendered = format!("{}\n\n{}", block.heading, lines.join("\n"));
    if omitted > 0 {
        rendered.push_str(&format!("\n\n_… and {omitted} more_"));
    }
    if let Some(pointer) = table.pointer.as_deref().map(str::trim) {
        if !pointer.is_empty() {
            rendered.push_str(&format!("\n\n_{pointer}_"));
        }
    }
    if !definitions.is_empty() {
        definitions
            .sort_by_key(|d| d.label.parse::<u128>().unwrap_or(u128::MAX));
        let def_block = definitions
            .iter()
            .map(|d| format!("[{}]: {}", d.label, d.destination))
            .collect::<Vec<_>>()
            .join("\n");
        rendered.push_str(&format!("\n\n{def_block}"));
    }
    Ok(rendered)
}

pub fn parse_managed_table_block(
    block: &ManagedTableBlock,
    document: &str,
) -> ManagedTableDocumentWire {
    let starts = find_all(document, &block.start_marker);
    let ends = find_all(document, &block.end_marker);

    let Some(&first_start) = starts.first() else {
        let reason = if ends.is_empty() {
            None
        } else {
            Some(format!(
                "stray {} end marker with no start marker",
                block.reason_label
            ))
        };
        return ManagedTableDocumentWire {
            schema_version: MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION,
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
        Some(&end) => (end + block.end_marker.len(), true),
        None => (document.len(), false),
    };

    let raw_block = &document[first_start..block_end];
    let inner = extract_inner(
        raw_block,
        &block.start_marker,
        &block.end_marker,
        terminated,
    );
    let table = parse_table_block(inner, &block.heading);

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
        push_reason(&format!(
            "duplicate {} block collapsed to one",
            block.reason_label
        ));
    }
    if stray_ends_before_start {
        push_reason(&format!(
            "stray {} end marker with no start marker before the managed block",
            block.reason_label
        ));
    }
    if !terminated {
        push_reason(&format!(
            "unterminated {} start marker; treated as extending to end of document",
            block.reason_label
        ));
    }

    ManagedTableDocumentWire {
        schema_version: MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION,
        table,
        body,
        reason,
    }
}

pub fn upsert_managed_table_block(
    block: &ManagedTableBlock,
    document: &str,
    table: &ManagedTableTableWire,
) -> Result<String, ArtifactRefError> {
    if table.rows.is_empty() {
        return Ok(remove_managed_table_block(block, document));
    }
    let parsed = parse_managed_table_block(block, document);
    let host_scan = match block.anchor {
        ManagedTableAnchorWire::Top => {
            Some(scan_markdown_reference_links(&parsed.body))
        }
        ManagedTableAnchorWire::Bottom => None,
    };
    let rendered =
        render_managed_table_block(block, table, host_scan.as_ref())?;
    let wrapped = format!(
        "{}\n\n{rendered}\n\n{}",
        block.start_marker, block.end_marker
    );
    let combined = match block.anchor {
        ManagedTableAnchorWire::Bottom => insert_bottom(&parsed.body, &wrapped),
        ManagedTableAnchorWire::Top => insert_top(&parsed.body, &wrapped),
    };
    Ok(with_trailing_newline_like(document, combined))
}

pub fn remove_managed_table_block(
    block: &ManagedTableBlock,
    document: &str,
) -> String {
    if !document.contains(&block.start_marker) {
        return document.to_string();
    }
    let parsed = parse_managed_table_block(block, document);
    with_trailing_newline_like(
        document,
        normalize_removed_body(block, parsed.body),
    )
}

pub fn strip_managed_table_block(
    block: &ManagedTableBlock,
    document: &str,
) -> String {
    if !document.contains(&block.start_marker) {
        return document.to_string();
    }
    let parsed = parse_managed_table_block(block, document);
    let body = normalize_removed_body(block, parsed.body);
    body.trim_end().to_string()
}

fn normalize_removed_body(block: &ManagedTableBlock, body: String) -> String {
    let trimmed = body.trim_end_matches('\n');
    if block.anchor == ManagedTableAnchorWire::Top {
        trimmed.trim_start_matches('\n').to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn render_links_block(
    table: &ManagedTableTableWire,
    host_document: Option<&str>,
) -> Result<String, ArtifactRefError> {
    let scan = host_document.map(scan_markdown_reference_links);
    render_managed_table_block(&links_table_block(), table, scan.as_ref())
}

pub fn parse_links_block(document: &str) -> ManagedTableDocumentWire {
    parse_managed_table_block(&links_table_block(), document)
}

pub fn upsert_links_block(
    document: &str,
    table: &ManagedTableTableWire,
) -> Result<String, ArtifactRefError> {
    upsert_managed_table_block(&links_table_block(), document, table)
}

pub fn remove_links_block(document: &str) -> String {
    remove_managed_table_block(&links_table_block(), document)
}

pub fn strip_links_block(document: &str) -> String {
    strip_managed_table_block(&links_table_block(), document)
}

fn insert_bottom(body: &str, wrapped: &str) -> String {
    let trimmed_body = body.trim_end_matches('\n');
    if trimmed_body.is_empty() {
        wrapped.to_string()
    } else {
        format!("{trimmed_body}\n\n{wrapped}")
    }
}

fn insert_top(body: &str, wrapped: &str) -> String {
    let (prefix, rest) = split_top_anchor(body);
    let prefix = prefix.trim_end_matches('\n');
    let rest = rest.trim_start_matches(['\n', '\r']);
    match (prefix.is_empty(), rest.is_empty()) {
        (true, true) => wrapped.to_string(),
        (true, false) => format!("{wrapped}\n\n{rest}"),
        (false, true) => format!("{prefix}\n\n{wrapped}"),
        (false, false) => format!("{prefix}\n\n{wrapped}\n\n{rest}"),
    }
}

/// Split after YAML frontmatter and a parsed plan-header block, if any.
fn split_top_anchor(document: &str) -> (&str, &str) {
    if let Some((_, header_end)) = sdd_plan_header_block_span(document) {
        let after = &document[header_end.min(document.len())..];
        let rest = after.strip_prefix('\n').unwrap_or(after);
        return (&document[..header_end], rest);
    }
    match split_document(document) {
        Ok(parts) if parts.has_frontmatter => (parts.prefix, parts.body),
        _ => ("", document),
    }
}

fn compare_rows(
    columns: &[ManagedTableColumnWire],
    a: &ManagedTableRowWire,
    b: &ManagedTableRowWire,
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

fn extract_inner<'a>(
    raw_block: &'a str,
    start_marker: &str,
    end_marker: &str,
    terminated: bool,
) -> &'a str {
    let after_start = raw_block.strip_prefix(start_marker).unwrap_or(raw_block);
    if terminated {
        after_start.strip_suffix(end_marker).unwrap_or(after_start)
    } else {
        after_start
    }
}

fn parse_table_block(
    inner: &str,
    heading: &str,
) -> Option<ManagedTableTableWire> {
    let mut definitions_map: BTreeMap<&str, &str> = BTreeMap::new();
    for line in inner.lines() {
        if let Some(reference) = parse_reference_definition(line.trim()) {
            definitions_map
                .entry(reference.id)
                .or_insert(reference.destination);
        }
    }

    let mut lines = inner.lines();
    let found_heading = lines.find(|line| !line.trim().is_empty())?;
    if found_heading.trim() != heading {
        return None;
    }
    let header_line = lines.find(|line| !line.trim().is_empty())?;
    let alignment_line = lines.next()?;
    let header_cells = split_table_row(header_line)?;
    let alignment_cells = split_table_row(alignment_line)?;
    if header_cells.is_empty() || header_cells.len() != alignment_cells.len() {
        return None;
    }

    let columns: Vec<ManagedTableColumnWire> = header_cells
        .iter()
        .zip(&alignment_cells)
        .map(|(label, alignment)| ManagedTableColumnWire {
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
        rows.push(ManagedTableRowWire {
            values,
            link_targets,
        });
    }

    let omitted = trailer_lines
        .iter()
        .find_map(|line| parse_omitted_line(line))
        .unwrap_or(0);
    let pointer = trailer_lines
        .iter()
        .find_map(|line| parse_pointer_line(line));

    Some(ManagedTableTableWire {
        schema_version: MANAGED_TABLE_BLOCK_WIRE_SCHEMA_VERSION,
        columns,
        rows,
        omitted,
        pointer,
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

fn parse_pointer_line(line: &str) -> Option<String> {
    let inner = line.strip_prefix('_')?.strip_suffix('_')?;
    if inner.starts_with("… and ") && inner.ends_with(" more") {
        return None;
    }
    Some(inner.to_string())
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
    use crate::plan::plan_validate;

    fn links_table(rows: Vec<ManagedTableRowWire>) -> ManagedTableTableWire {
        ManagedTableTableWire {
            schema_version: LINKS_BLOCK_WIRE_SCHEMA_VERSION,
            columns: vec![
                ManagedTableColumnWire {
                    key: "relation".to_string(),
                    label: "Relation".to_string(),
                    numeric: false,
                },
                ManagedTableColumnWire {
                    key: "artifact".to_string(),
                    label: "Artifact".to_string(),
                    numeric: false,
                },
                ManagedTableColumnWire {
                    key: "why".to_string(),
                    label: "Why".to_string(),
                    numeric: false,
                },
            ],
            rows,
            omitted: 0,
            pointer: None,
        }
    }

    fn link_row(
        relation: &str,
        artifact: &str,
        why: &str,
        url: Option<&str>,
    ) -> ManagedTableRowWire {
        let mut values = BTreeMap::new();
        values.insert("relation".to_string(), relation.to_string());
        values.insert("artifact".to_string(), artifact.to_string());
        values.insert("why".to_string(), why.to_string());
        let mut link_targets = BTreeMap::new();
        if let Some(destination) = url {
            link_targets
                .insert("artifact".to_string(), destination.to_string());
        }
        ManagedTableRowWire {
            values,
            link_targets,
        }
    }

    #[test]
    fn links_render_sorts_and_emits_pointer() {
        let mut table = links_table(vec![
            link_row(
                "related",
                "bead:sase-ct",
                "shares the ACE-TUI flake root cause",
                Some("https://ct"),
            ),
            link_row(
                "implements",
                "bead:sase-js",
                "extends the ref contract this epic landed",
                Some("https://js"),
            ),
        ]);
        table.pointer = Some(
            "Plus 12 automatic references — see [Referenced By](#referenced-by)."
                .to_string(),
        );
        let rendered = render_links_block(&table, None).unwrap();
        assert!(rendered.starts_with("## Links\n"));
        assert!(rendered.contains("| implements | [bead:sase-js]["));
        assert!(rendered.contains("_Plus 12 automatic references — see [Referenced By](#referenced-by)._"));
        let implements_at = rendered.find("implements").unwrap();
        let related_at = rendered.find("related").unwrap();
        assert!(implements_at < related_at);
    }

    #[test]
    fn links_upsert_is_top_anchored_and_idempotent() {
        let table =
            links_table(vec![link_row("related", "bead:sase-ct", "why", None)]);
        let document = "# Doc\n\nBody text.\n";
        let once = upsert_links_block(document, &table).unwrap();
        assert!(once.starts_with("<!-- sase:links:start -->\n"));
        assert!(once.contains("# Doc\n"));
        assert!(once.contains("Body text."));
        let twice = upsert_links_block(&once, &table).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn links_after_plan_header_does_not_trip_header_invalid() {
        let table = links_table(vec![link_row(
            "implements",
            "bead:sase-js",
            "extends the ref contract this epic landed",
            Some("https://github.com/sase-org/sase--beads/blob/main/pages/sase-js/README.md"),
        )]);
        let document = "---\ntier: tale\ntitle: Ship the feature\ngoal: Ship the feature\nsize: small\n---\n\n- **PARENT:** [202608/epic.md](epic.md)\n\n# Plan\nDo it.\n";
        let with_links = upsert_links_block(document, &table).unwrap();
        assert!(with_links.contains("- **PARENT:** [202608/epic.md](epic.md)"));
        let parent_at = with_links.find("- **PARENT:**").unwrap();
        let links_at = with_links.find("<!-- sase:links:start -->").unwrap();
        let heading_at = with_links.find("# Plan").unwrap();
        assert!(parent_at < links_at);
        assert!(links_at < heading_at);
        let result = plan_validate(&with_links, "tale").unwrap();
        assert!(
            !result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "header-invalid"),
            "{:?}",
            result.diagnostics
        );
        assert!(result.ok, "{:?}", result.diagnostics);
    }

    #[test]
    fn links_skip_host_document_reference_labels() {
        let table = links_table(vec![link_row(
            "related",
            "bead:sase-ct",
            "why",
            Some("https://ct"),
        )]);
        let host = "# Doc\n\nSee [note][1].\n\n[1]: https://already-used\n";
        let rendered = render_links_block(&table, Some(host)).unwrap();
        assert!(rendered.contains("[bead:sase-ct][2]"));
        assert!(rendered.contains("[2]: https://ct"));
        assert!(!rendered.contains("[1]: https://ct"));
    }

    #[test]
    fn empty_links_table_removes_the_block() {
        let table =
            links_table(vec![link_row("related", "bead:sase-ct", "why", None)]);
        let document = "# Doc\n\nBody.\n";
        let with_block = upsert_links_block(document, &table).unwrap();
        let mut empty = table;
        empty.rows.clear();
        let removed = upsert_links_block(&with_block, &empty).unwrap();
        assert_eq!(removed, document);
    }
}
