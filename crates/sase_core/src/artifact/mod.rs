//! Unified artifact graph wire types and SQLite persistence.
//!
//! This module is intentionally PyO3-free. Python bindings should convert
//! through serde-shaped JSON records in `sase_core_py`.

pub mod store;
pub mod wire;

pub use store::{open_artifact_store, ArtifactStore};
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
