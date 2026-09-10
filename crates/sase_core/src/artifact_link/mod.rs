//! First-class artifact link graph: row schema, relation registry, managed
//! tables, companion paths, and frontmatter inlet.

mod events;
mod inlet;
mod managed_table;
mod merge;
mod path;
mod publication_retry;
mod relation;
mod row_resolution;
mod wire;

pub use events::{
    artifact_link_alias_producer_id, artifact_link_derived_producer_id,
    artifact_link_event_canonical_json, artifact_link_event_digest,
    artifact_link_event_path_for_digest, artifact_link_event_validate_bytes,
    artifact_link_event_validate_path, artifact_link_machine_run_id,
    artifact_link_stable_fact_created_at, artifact_link_stable_operation_id,
    canonicalize_artifact_link_alias, canonicalize_artifact_link_event,
    canonicalize_artifact_link_event_json_value, reduce_link_events,
    resolve_artifact_link_event_aliases, ArtifactLinkAliasResolutionWire,
    ArtifactLinkAliasWire, ArtifactLinkEventCanonicalWire,
    ArtifactLinkEventEdgeWire, ArtifactLinkEventKindWire,
    ArtifactLinkEventReductionWire, ArtifactLinkEventWire,
    ArtifactLinkReducedEdgeWire, ArtifactLinkReducedTombstoneWire,
    ArtifactLinkReducedVersionWire,
    ARTIFACT_LINK_EVENT_REDUCTION_WIRE_SCHEMA_VERSION,
    ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION,
};
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
pub use merge::merge_artifact_link_indexes;
pub use path::{
    artifact_md_path, bead_lineage_root, bead_page_relpath, companion_md_path,
    ArtifactCompanionPathWire, ArtifactMdPathKindWire,
    ArtifactMdPathRequestWire, ArtifactMdPathWire,
    ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
};
pub use publication_retry::{
    artifact_link_publication_due, artifact_link_publication_mark_attempt,
    artifact_link_publication_record_key,
    artifact_link_publication_register_pending,
    retry_backoff_seconds_after_attempt,
    ArtifactLinkPublicationAttemptStatusWire,
    ArtifactLinkPublicationAttemptWire, ArtifactLinkPublicationDueWire,
    ArtifactLinkPublicationObservationWire, ArtifactLinkPublicationRecordWire,
    ARTIFACT_LINK_PUBLICATION_AGING_WARNING_SECONDS,
    ARTIFACT_LINK_PUBLICATION_INITIAL_RETRY_SECONDS,
    ARTIFACT_LINK_PUBLICATION_MAX_BACKOFF_SECONDS,
    ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION,
};
pub use relation::{
    builtin_artifact_relations, lookup_artifact_relation,
    relation_label_from_perspective, reserved_artifact_relation_slugs,
    ArtifactRelationWire, ARTIFACT_RELATION_WIRE_SCHEMA_VERSION,
    RESERVED_ARTIFACT_RELATION_SLUGS,
};
pub use row_resolution::{
    artifact_row_index_keys, artifact_row_ref_lookup_keys,
    parse_artifact_link_ref_parts, resolve_artifact_row_identity,
    ArtifactLinkRefPartsWire, ArtifactRowIdentityWire, ArtifactRowRefQueryWire,
    ARTIFACT_ROW_RESOLUTION_WIRE_SCHEMA_VERSION,
};
pub use wire::{
    artifact_link_dedup_key, canonicalize_artifact_link_ref,
    upsert_artifact_link_row, validate_artifact_link_description,
    validate_artifact_link_row, ArtifactLinkAggregateWire,
    ArtifactLinkDedupKeyWire, ArtifactLinkError, ArtifactLinkIndexWire,
    ArtifactLinkOriginWire, ArtifactLinkRowWire, ArtifactLinkUpsertKindWire,
    ArtifactLinkUpsertWire, BeadLinkDirectionWire, BeadLinkWire,
    ARTIFACT_LINK_ROW_SCHEMA_VERSION,
};
