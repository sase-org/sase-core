//! Immutable artifact-link inverted index: compile once, query by ref.

mod aliases;
mod compile;
mod prepared;
mod query;
mod target;
mod wire;

#[cfg(test)]
mod tests;

pub use compile::compile_artifact_link_index;
pub use prepared::PreparedArtifactLinkIndex;
pub use query::{
    artifact_link_index_chips, artifact_link_index_project,
    artifact_link_index_rows, artifact_link_index_target,
};
pub use wire::{
    ArtifactLinkIndexBuildRequestWire, ArtifactLinkIndexChipWire,
    ArtifactLinkIndexIdentityWire, ArtifactLinkIndexProjectRequestWire,
    ArtifactLinkIndexProjectResponseWire, ArtifactLinkIndexProjectedEdgeWire,
    ArtifactLinkIndexRawRowWire, ArtifactLinkIndexRelationMetaWire,
    ArtifactLinkIndexRowsRequestWire, ArtifactLinkIndexRowsResponseWire,
    ArtifactLinkIndexStatsWire, ArtifactLinkIndexTargetWire,
    ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
};
