//! Bottom-anchored `Referenced By` managed block.
//!
//! Facades over [`crate::artifact_link::ManagedTableBlock`]. Render, parse,
//! upsert, remove, and strip stay byte-stable with the historical markers.

use crate::artifact_link::{
    parse_managed_table_block, referenced_by_table_block,
    remove_managed_table_block, render_managed_table_block,
    strip_managed_table_block, upsert_managed_table_block,
    ManagedTableColumnWire, ManagedTableDocumentWire, ManagedTableRowWire,
    ManagedTableTableWire, MAX_RENDERED_MANAGED_TABLE_ROWS,
};
use crate::artifact_ref::ArtifactRefError;

pub const REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION: u64 =
    crate::artifact_link::REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION;
pub const MAX_RENDERED_REFERENCED_BY_ROWS: usize =
    MAX_RENDERED_MANAGED_TABLE_ROWS;

pub type ReferencedByColumnWire = ManagedTableColumnWire;
pub type ReferencedByRowWire = ManagedTableRowWire;
pub type ReferencedByTableWire = ManagedTableTableWire;
pub type ReferencedByDocumentWire = ManagedTableDocumentWire;

/// Render one `Referenced By` block's heading, table, and link definitions.
///
/// Rows sort deterministically by column values in declared column order,
/// cap at [`MAX_RENDERED_REFERENCED_BY_ROWS`], and a linked cell is numbered
/// through `markdown_link_refs` rather than a second allocator.
pub fn render_referenced_by_block(
    table: &ReferencedByTableWire,
) -> Result<String, ArtifactRefError> {
    render_managed_table_block(&referenced_by_table_block(), table, None)
}

/// Parse the managed `Referenced By` block out of `document`.
///
/// Marker recovery: a duplicate block collapses to one; an unterminated
/// start marker is treated as extending to end of document; a stray end
/// marker with no start is left alone. All three are reported through
/// `reason`.
pub fn parse_referenced_by_block(document: &str) -> ReferencedByDocumentWire {
    parse_managed_table_block(&referenced_by_table_block(), document)
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
    upsert_managed_table_block(&referenced_by_table_block(), document, table)
}

/// Remove the managed block, if present, preserving document shape.
pub fn remove_referenced_by_block(document: &str) -> String {
    remove_managed_table_block(&referenced_by_table_block(), document)
}

/// Strip the managed block for content hashing and change detection.
///
/// A citation never counts as a new version of the cited artifact: digest
/// and diff logic should run on this function's output, not on the raw
/// document.
pub fn strip_referenced_by_block(document: &str) -> String {
    strip_managed_table_block(&referenced_by_table_block(), document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const START_MARKER: &str =
        crate::artifact_link::REFERENCED_BY_BLOCK_START_MARKER;
    const END_MARKER: &str =
        crate::artifact_link::REFERENCED_BY_BLOCK_END_MARKER;

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
            pointer: None,
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
