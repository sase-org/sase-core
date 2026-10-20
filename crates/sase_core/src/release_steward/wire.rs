//! Versioned release-steward request and result wires.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::facts::{EvidenceRefWire, ReleaseStewardSnapshotWire};

pub const RELEASE_STEWARD_WIRE_SCHEMA_VERSION: u32 = 1;

pub const RELEASE_STEWARD_CODE_UNSUPPORTED_SCHEMA: &str =
    "unsupported_release_steward_schema";
pub const RELEASE_STEWARD_CODE_INVALID_REQUEST: &str =
    "invalid_release_steward_request";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReleaseStewardError {
    #[error(
        "{code}: unsupported schema_version {actual}; expected {expected}",
        code = RELEASE_STEWARD_CODE_UNSUPPORTED_SCHEMA
    )]
    UnsupportedSchema { actual: u32, expected: u32 },
    #[error(
        "{code}: {message}",
        code = RELEASE_STEWARD_CODE_INVALID_REQUEST
    )]
    InvalidRequest { message: String },
}

impl ReleaseStewardError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }
}

/// Daily cadence is the only accepted kind; `eligible` is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CadencePolicyWire {
    pub kind: String,
    pub merge_after_hour_utc: u32,
    pub max_merges_per_utc_day: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseStewardPolicyWire {
    pub merge_authority_enabled: bool,
    pub observe_only: bool,
    pub tick_identity: String,
    pub cadence: CadencePolicyWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseStewardRequestWire {
    pub schema_version: u32,
    /// RFC 3339 UTC observation time. The clock is never implicit.
    pub observed_at: String,
    pub policy: ReleaseStewardPolicyWire,
    pub snapshot: ReleaseStewardSnapshotWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionDispositionWire {
    Proposed,
    Suppressed,
    AlreadySatisfied,
    Executed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TrainActionKindWire {
    MergeReleasePr {
        pr_number: u64,
        head_sha: String,
    },
    DispatchRelease {
        tag: String,
        sha: String,
        mode: String,
        generation: u64,
    },
    DispatchPublishMetadata {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pr_number: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        head_sha: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pin: Option<String>,
    },
    RerunFailedJobs {
        run_id: String,
        attempt: u32,
        job_ids: Vec<String>,
    },
    DispatchCoreCut {
        source_pin: String,
    },
    AbandonCandidate {
        candidate_id: String,
        generation: u64,
        observation_asset: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PrimaryStateWire {
    WatcherBroken {
        source: String,
        remedy: String,
    },
    PublishConflict {
        candidate_id: String,
        detail: String,
    },
    DependencyUnpublished {
        package: String,
        detail: String,
    },
    Vetoed {
        reason: String,
    },
    PublishPartial {
        version: String,
        candidate_id: String,
        generation: u64,
    },
    Released {
        version: String,
    },
    QualifyRed {
        signature: String,
        recovered: bool,
    },
    ExerciseRed {
        platform: String,
    },
    CoreCiRed {
        detail: String,
    },
    CoreCutRed {
        check: String,
    },
    CoreLag {
        pin: String,
    },
    CoreIncompatible {
        detail: String,
    },
    GateRed {
        sha: String,
    },
    Releasable {
        pr_number: u64,
        head_sha: String,
    },
    Waiting {
        reason: String,
    },
    NothingToRelease {
        reason: String,
    },
    InstallBroken {
        member: String,
    },
    AmbiguousTrains {
        detail: String,
    },
}

impl PrimaryStateWire {
    pub fn family(&self) -> &'static str {
        match self {
            Self::WatcherBroken { .. } => "watcher_broken",
            Self::PublishConflict { .. } => "publish_conflict",
            Self::DependencyUnpublished { .. } => "dependency_unpublished",
            Self::Vetoed { .. } => "vetoed",
            Self::PublishPartial { .. } => "publish_partial",
            Self::Released { .. } => "released",
            Self::QualifyRed { .. } => "qualify_red",
            Self::ExerciseRed { .. } => "exercise_red",
            Self::CoreCiRed { .. } => "core_ci_red",
            Self::CoreCutRed { .. } => "core_cut_red",
            Self::CoreLag { .. } => "core_lag",
            Self::CoreIncompatible { .. } => "core_incompatible",
            Self::GateRed { .. } => "gate_red",
            Self::Releasable { .. } => "releasable",
            Self::Waiting { .. } => "waiting",
            Self::NothingToRelease { .. } => "nothing_to_release",
            Self::InstallBroken { .. } => "install_broken",
            Self::AmbiguousTrains { .. } => "ambiguous_trains",
        }
    }

    pub fn identity(&self) -> String {
        match self {
            Self::WatcherBroken { source, .. } => source.clone(),
            Self::PublishConflict { candidate_id, .. }
            | Self::PublishPartial { candidate_id, .. } => candidate_id.clone(),
            Self::DependencyUnpublished { package, .. } => package.clone(),
            Self::Vetoed { reason } => reason.clone(),
            Self::Released { version } => version.clone(),
            Self::QualifyRed { signature, .. } => signature.clone(),
            Self::ExerciseRed { platform } => platform.clone(),
            Self::CoreCiRed { detail } => detail.clone(),
            Self::CoreCutRed { check } => check.clone(),
            Self::CoreLag { pin } => pin.clone(),
            Self::CoreIncompatible { detail } => detail.clone(),
            Self::GateRed { sha } => sha.clone(),
            Self::Releasable {
                pr_number,
                head_sha,
            } => format!("{pr_number}:{head_sha}"),
            Self::Waiting { reason } | Self::NothingToRelease { reason } => {
                reason.clone()
            }
            Self::InstallBroken { member } => member.clone(),
            Self::AmbiguousTrains { detail } => detail.clone(),
        }
    }

    pub fn escalates_immediately(&self) -> bool {
        matches!(
            self,
            Self::PublishConflict { .. }
                | Self::DependencyUnpublished { .. }
                | Self::InstallBroken { .. }
        )
    }

    pub fn is_advisory_qualification(&self) -> bool {
        matches!(self, Self::QualifyRed { .. })
    }

    pub fn is_blocking(&self) -> bool {
        !matches!(
            self,
            Self::Released { .. }
                | Self::Releasable { .. }
                | Self::NothingToRelease { .. }
                | Self::Waiting { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationSeverityWire {
    Immediate,
    Aged,
    Recovery,
    Advisory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EscalationOpWire {
    Open {
        family: String,
        title: String,
        labels: Vec<String>,
        body: String,
        identity_marker: String,
    },
    Comment {
        family: String,
        issue_number: u64,
        body: String,
        tick_identity: String,
    },
    Close {
        family: String,
        issue_number: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedActionWire {
    pub action: TrainActionKindWire,
    pub disposition: ActionDispositionWire,
    pub key: String,
    pub preconditions: Vec<String>,
    pub argv: Vec<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedEscalationWire {
    pub operation: EscalationOpWire,
    pub disposition: ActionDispositionWire,
    pub key: String,
    pub severity: EscalationSeverityWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecondaryFindingWire {
    pub state: PrimaryStateWire,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseStewardResultWire {
    pub schema_version: u32,
    pub primary: PrimaryStateWire,
    pub details: String,
    pub evidence: Vec<EvidenceRefWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker_started_at: Option<String>,
    pub blocker_duration_uncertain: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub train_action: Option<ProposedActionWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<ProposedEscalationWire>,
    pub secondary: Vec<SecondaryFindingWire>,
    /// 0 for every well-formed state; 1 for `WATCHER_BROKEN`.
    pub process_exit: u32,
}
