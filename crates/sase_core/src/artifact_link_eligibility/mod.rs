//! Run-scoped eligibility policy for automatic artifact-link publication.
//!
//! An automatic consumer link recorded from a `read` must not become a
//! durable commit or publication until the recording run itself has a
//! host-verified, non-bookkeeping file change. This module owns that
//! decision and the versioned evidence record that binds a later
//! publication attempt back to the exact run that earned it -- an agent
//! name or family publication elsewhere is not sufficient.

mod policy;
mod wire;

pub use policy::{
    artifact_link_release_evidence, decide_artifact_link_eligibility,
    validate_artifact_link_release_evidence, ArtifactLinkEligibilityError,
};
pub use wire::{
    ArtifactLinkChangeRoleWire, ArtifactLinkChangedPathWire,
    ArtifactLinkEligibilityDecisionWire, ArtifactLinkEligibilityRequestWire,
    ArtifactLinkReleaseEvidenceWire, ArtifactLinkRepoEvidenceWire,
    ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
};
