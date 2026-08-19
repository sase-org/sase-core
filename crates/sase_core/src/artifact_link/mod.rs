//! First-class artifact link graph: row schema, relation registry, managed
//! tables, companion paths, and frontmatter inlet.

mod inlet;
mod managed_table;
mod path;
mod relation;
mod wire;

pub use inlet::{
    parse_artifact_link_frontmatter_inlet,
    ArtifactLinkFrontmatterInletKindWire, ArtifactLinkFrontmatterInletWire,
    ArtifactLinkInletEntryWire, ARTIFACT_LINK_INLET_WIRE_SCHEMA_VERSION,
};
pub use managed_table::{
    links_table_block, parse_links_block, parse_managed_table_block,
    referenced_by_table_block, remove_links_block, remove_managed_table_block,
    render_links_block, render_managed_table_block, strip_links_block,
    strip_managed_table_block, upsert_links_block, upsert_managed_table_block,
    ManagedTableAnchorWire, ManagedTableBlock, ManagedTableColumnWire,
    ManagedTableDocumentWire, ManagedTableRowWire, ManagedTableTableWire,
    LINKS_BLOCK_END_MARKER, LINKS_BLOCK_HEADING, LINKS_BLOCK_START_MARKER,
    LINKS_BLOCK_WIRE_SCHEMA_VERSION, MAX_RENDERED_MANAGED_TABLE_ROWS,
    MAX_RENDERED_REFERENCED_BY_ROWS, REFERENCED_BY_BLOCK_END_MARKER,
    REFERENCED_BY_BLOCK_HEADING, REFERENCED_BY_BLOCK_START_MARKER,
    REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION,
};
pub use path::{
    artifact_md_path, bead_lineage_root, bead_page_relpath, companion_md_path,
    ArtifactCompanionPathWire, ArtifactMdPathKindWire,
    ArtifactMdPathRequestWire, ArtifactMdPathWire,
    ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
};
pub use relation::{
    builtin_artifact_relations, lookup_artifact_relation,
    relation_label_from_perspective, reserved_artifact_relation_slugs,
    ArtifactRelationWire, ARTIFACT_RELATION_WIRE_SCHEMA_VERSION,
    RESERVED_ARTIFACT_RELATION_SLUGS,
};
pub use wire::{
    artifact_link_dedup_key, canonicalize_artifact_link_ref,
    upsert_artifact_link_row, validate_artifact_link_description,
    validate_artifact_link_row, ArtifactLinkAggregateWire,
    ArtifactLinkDedupKeyWire, ArtifactLinkError, ArtifactLinkIndexWire,
    ArtifactLinkOriginWire, ArtifactLinkRowWire, ArtifactLinkUpsertKindWire,
    ArtifactLinkUpsertWire, BeadLinkWire, ARTIFACT_LINK_ROW_SCHEMA_VERSION,
};
