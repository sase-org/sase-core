//! Unified artifact graph wire types and SQLite persistence.
//!
//! This module is intentionally PyO3-free. Python bindings should convert
//! through serde-shaped JSON records in `sase_core_py`.

pub mod export;
pub mod ingest;
pub mod query;
pub mod store;
pub mod wire;

pub use export::{
    artifact_export_dot, artifact_export_json, artifact_export_mermaid,
    artifact_materialize_graph,
};
pub use ingest::{
    artifact_rebuild, artifact_upsert_path, derived_payload,
    directory_node_for_path, file_node_for_path, merge_mutation_results,
    normalize_artifact_path, path_to_artifact_id, select_directory_parent_id,
    upsert_derived_payload, ArtifactIngestMutations,
    ResolvedArtifactRebuildRequest, ARTIFACT_SOURCE_AGENT_ARTIFACT,
    ARTIFACT_SOURCE_AGENT_CREATED_FILE, ARTIFACT_SOURCE_AGENT_THOUGHT,
    ARTIFACT_SOURCE_BEAD_STORE, ARTIFACT_SOURCE_CHANGESPEC,
    ARTIFACT_SOURCE_COMMIT, ARTIFACT_SOURCE_DIRECTORY,
    ARTIFACT_SOURCE_PROJECT_FILE,
};
pub use query::{
    artifact_children, artifact_detail, artifact_doctor,
    artifact_inbound_neighbors, artifact_is_reachable_to_root, artifact_list,
    artifact_outbound_neighbors, artifact_path_to_root, artifact_search,
    artifact_show, artifact_show_paged, artifact_summary,
};
pub use store::{
    deterministic_artifact_link_id, open_artifact_store, remove_artifact_link,
    remove_artifact_node, upsert_artifact_link, upsert_artifact_node,
    upsert_artifact_payload, ArtifactStore,
};
pub use wire::{
    canonical_file_artifact_types, file_artifact_type, set_file_artifact_type,
    validate_file_artifact_type, ArtifactDetailPagedWire, ArtifactDetailWire,
    ArtifactDoctorIssueWire, ArtifactDoctorOptionsWire, ArtifactDoctorWire,
    ArtifactGraphOptionsWire, ArtifactGraphWire, ArtifactGroupSummaryWire,
    ArtifactKindWire, ArtifactLinkRemoveWire, ArtifactLinkTypeWire,
    ArtifactLinkUpsertWire, ArtifactLinkWire, ArtifactMutationResultWire,
    ArtifactNodeRemoveWire, ArtifactNodeUpsertWire, ArtifactNodeWire,
    ArtifactPageRequestWire, ArtifactPathUpsertRequestWire,
    ArtifactPayloadWire, ArtifactQueryWire, ArtifactRebuildRequestWire,
    ArtifactRelationPageWire, ArtifactSummaryRequestWire, ArtifactSummaryWire,
    ArtifactTypeCountWire, ARTIFACT_FILE_TYPE_CHAT, ARTIFACT_FILE_TYPE_DIFF,
    ARTIFACT_FILE_TYPE_METADATA_KEY, ARTIFACT_FILE_TYPE_MISC,
    ARTIFACT_FILE_TYPE_PLAN, ARTIFACT_FILE_TYPE_PROJECT,
    ARTIFACT_FILE_TYPE_PROMPT, ARTIFACT_KIND_AGENT, ARTIFACT_KIND_BEAD,
    ARTIFACT_KIND_CHANGESPEC, ARTIFACT_KIND_COMMIT, ARTIFACT_KIND_DIRECTORY,
    ARTIFACT_KIND_FILE, ARTIFACT_KIND_PROJECT, ARTIFACT_KIND_ROOT,
    ARTIFACT_KIND_THOUGHT, ARTIFACT_KIND_UNKNOWN, ARTIFACT_LINK_CREATED,
    ARTIFACT_LINK_PARENT, ARTIFACT_LINK_RELATED, ARTIFACT_LINK_WORKER,
    ARTIFACT_PROVENANCE_DERIVED, ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID,
    ARTIFACT_STALE_CLEANUP_MARK, ARTIFACT_STALE_CLEANUP_NONE,
    ARTIFACT_TOMBSTONE_LINK, ARTIFACT_TOMBSTONE_NODE,
    ARTIFACT_WIRE_SCHEMA_VERSION,
};
