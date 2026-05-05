//! Unified artifact graph wire types and SQLite persistence.
//!
//! This module is intentionally PyO3-free. Python bindings should convert
//! through serde-shaped JSON records in `sase_core_py`.

pub mod export;
pub mod query;
pub mod store;
pub mod wire;

pub use export::{
    artifact_export_dot, artifact_export_json, artifact_export_mermaid,
    artifact_materialize_graph,
};
pub use query::{
    artifact_children, artifact_detail, artifact_doctor,
    artifact_inbound_neighbors, artifact_is_reachable_to_root, artifact_list,
    artifact_outbound_neighbors, artifact_path_to_root, artifact_search,
    artifact_show,
};
pub use store::{
    deterministic_artifact_link_id, open_artifact_store, remove_artifact_link,
    remove_artifact_node, upsert_artifact_link, upsert_artifact_node,
    upsert_artifact_payload, ArtifactStore,
};
pub use wire::{
    ArtifactDetailWire, ArtifactDoctorIssueWire, ArtifactDoctorOptionsWire,
    ArtifactDoctorWire, ArtifactGraphOptionsWire, ArtifactGraphWire,
    ArtifactKindWire, ArtifactLinkRemoveWire, ArtifactLinkTypeWire,
    ArtifactLinkUpsertWire, ArtifactLinkWire, ArtifactMutationResultWire,
    ArtifactNodeRemoveWire, ArtifactNodeUpsertWire, ArtifactNodeWire,
    ArtifactPayloadWire, ArtifactQueryWire, ARTIFACT_KIND_AGENT,
    ARTIFACT_KIND_BEAD, ARTIFACT_KIND_CHANGESPEC, ARTIFACT_KIND_COMMIT,
    ARTIFACT_KIND_DIRECTORY, ARTIFACT_KIND_FILE, ARTIFACT_KIND_PROJECT,
    ARTIFACT_KIND_ROOT, ARTIFACT_KIND_THOUGHT, ARTIFACT_KIND_UNKNOWN,
    ARTIFACT_LINK_CREATED, ARTIFACT_LINK_PARENT, ARTIFACT_LINK_RELATED,
    ARTIFACT_LINK_WORKER, ARTIFACT_PROVENANCE_DERIVED,
    ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID, ARTIFACT_TOMBSTONE_LINK,
    ARTIFACT_TOMBSTONE_NODE, ARTIFACT_WIRE_SCHEMA_VERSION,
};
