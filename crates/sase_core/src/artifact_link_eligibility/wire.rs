//! Versioned wire contract for automatic artifact-link publish eligibility.
//!
//! An automatic consumer link (a `read`-origin row recorded from `sase
//! artifact read`) is only eligible for durable publication once the
//! consuming run itself has a host-verified, non-bookkeeping file change.
//! This module models that decision and the durable evidence that a
//! qualifying change was verified, both bound to one run's stable
//! `(run_id, agent_id)` identity so a different run -- even one in the same
//! agent family -- cannot borrow another run's eligibility.

use serde::{Deserialize, Serialize};

pub const ARTIFACT_LINK_ELIGIBILITY_WIRE_SCHEMA_VERSION: u64 = 1;

pub const ARTIFACT_LINK_ELIGIBILITY_ID_MAX_LEN: usize = 160;
pub const ARTIFACT_LINK_ELIGIBILITY_LIST_MAX_LEN: usize = 128;
pub const ARTIFACT_LINK_ELIGIBILITY_TEXT_MAX_CHARS: usize = 4_096;

/// Role of one changed path for eligibility purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkChangeRoleWire {
    /// An authored addition, edit, deletion, or rename that is not
    /// artifact-link bookkeeping.
    Real,
    /// A versioned per-artifact link-index JSON file, a lock sentinel, or
    /// other bookkeeping the link store maintains on its own (a managed
    /// `.gitignore` entry, a generated link table).
    Bookkeeping,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkChangedPathWire {
    pub path: String,
    pub role: ArtifactLinkChangeRoleWire,
}

/// One repository's host-collected change evidence for a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkRepoEvidenceWire {
    pub repo_id: String,
    pub kind: String,
    #[serde(default)]
    pub changed_paths: Vec<ArtifactLinkChangedPathWire>,
}

/// Host-collected facts a run submits to ask whether its pending automatic
/// links are eligible for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkEligibilityRequestWire {
    pub schema_version: u64,
    pub run_id: String,
    pub agent_id: String,
    #[serde(default)]
    pub repos: Vec<ArtifactLinkRepoEvidenceWire>,
}

/// The verdict: which repos (if any) qualify this run to publish its
/// pending automatic links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkEligibilityDecisionWire {
    pub schema_version: u64,
    pub run_id: String,
    pub agent_id: String,
    pub eligible: bool,
    #[serde(default)]
    pub qualifying_repo_ids: Vec<String>,
}

/// Durable evidence recorded once the host verifies a qualifying commit for
/// one run. Persisted so a later process -- possibly after the originating
/// workspace is gone -- can confirm release without re-deriving eligibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkReleaseEvidenceWire {
    pub schema_version: u64,
    pub run_id: String,
    pub agent_id: String,
    pub qualifying_repo_ids: Vec<String>,
    pub recorded_at: String,
}
