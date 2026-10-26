//! Snapshot fact wires. Absent evidence is distinct from an unreadable
//! transport response; only the latter forbids authorizing an action.

use serde::{Deserialize, Serialize};

/// One reconstructed fact: present, known-missing, or unreadable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FactRead<T> {
    Ready {
        value: T,
    },
    Absent {
        reason: String,
    },
    Unreadable {
        source: String,
        message: String,
        remedy: String,
    },
}

impl<T> FactRead<T> {
    pub fn ready(value: T) -> Self {
        Self::Ready { value }
    }

    pub fn absent(reason: impl Into<String>) -> Self {
        Self::Absent {
            reason: reason.into(),
        }
    }

    pub fn unreadable(
        source: impl Into<String>,
        message: impl Into<String>,
        remedy: impl Into<String>,
    ) -> Self {
        Self::Unreadable {
            source: source.into(),
            message: message.into(),
            remedy: remedy.into(),
        }
    }

    pub fn as_ready(&self) -> Option<&T> {
        match self {
            Self::Ready { value } => Some(value),
            Self::Absent { .. } | Self::Unreadable { .. } => None,
        }
    }

    pub fn unreadable_break(&self) -> Option<(&str, &str, &str)> {
        match self {
            Self::Unreadable {
                source,
                message,
                remedy,
            } => Some((source, message, remedy)),
            Self::Ready { .. } | Self::Absent { .. } => None,
        }
    }
}

/// Required-check conclusion. Only `success` is green.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckConclusionWire {
    Success,
    Failure,
    Cancelled,
    Skipped,
    Neutral,
    Stale,
    TimedOut,
    ActionRequired,
    StartupFailure,
    Missing,
    Unknown,
}

impl CheckConclusionWire {
    pub fn is_success(self) -> bool {
        matches!(self, Self::Success)
    }
}

/// GitHub Actions run status. Pending/queued/waiting counts as in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatusWire {
    Queued,
    Pending,
    InProgress,
    Waiting,
    Completed,
    Requested,
    WaitingForDeploy,
    Unknown,
}

impl RunStatusWire {
    pub fn is_in_flight(self) -> bool {
        matches!(
            self,
            Self::Queued
                | Self::Pending
                | Self::InProgress
                | Self::Waiting
                | Self::Requested
                | Self::WaitingForDeploy
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryFactsWire {
    pub owner: String,
    pub name: String,
    pub default_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredCheckWire {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckFactsWire {
    pub name: String,
    pub conclusion: CheckConclusionWire,
    pub is_test: bool,
    pub is_lint: bool,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFactsWire {
    pub id: String,
    pub name: String,
    pub conclusion: Option<CheckConclusionWire>,
    pub is_test: bool,
    pub is_lint: bool,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowRunFactsWire {
    pub workflow: String,
    pub run_id: String,
    pub attempt: u32,
    pub ref_name: String,
    pub sha: String,
    pub status: RunStatusWire,
    pub conclusion: Option<CheckConclusionWire>,
    pub jobs: Vec<JobFactsWire>,
    pub started_at: Option<String>,
    pub updated_at: Option<String>,
    pub event: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MasterFactsWire {
    pub sha: String,
    pub gate_run: Option<WorkflowRunFactsWire>,
    pub expected_required_checks: Vec<RequiredCheckWire>,
    pub expected_checks_source: String,
    pub expected_checks_readable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeEventWire {
    pub pr_number: u64,
    pub head_sha: String,
    pub merged_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    #[serde(default)]
    pub abandoned_after: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePrFactsWire {
    pub number: u64,
    pub head_sha: String,
    pub source_pin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_public_core_pin: Option<String>,
    pub labels: Vec<String>,
    pub checks: Vec<CheckFactsWire>,
    pub merge_history: Vec<MergeEventWire>,
    pub stale_head: bool,
    pub pin_changed: bool,
    pub merged: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_at: Option<String>,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_smoke: Option<CheckFactsWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagFactsWire {
    pub name: String,
    pub peeled_sha: String,
    pub has_manifest: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetFactsWire {
    pub name: String,
    pub kind: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseFactsWire {
    pub tag: String,
    pub draft: bool,
    pub name: String,
    pub assets: Vec<AssetFactsWire>,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PypiFileWire {
    pub filename: String,
    pub version: String,
    pub sha256: String,
    pub yanked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageInventoryWire {
    pub package: String,
    pub files: Vec<PypiFileWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PypiFactsWire {
    pub sase: PackageInventoryWire,
    pub core: PackageInventoryWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainFactsWire {
    pub candidate_id: String,
    pub version: String,
    pub tag: String,
    pub sha: String,
    pub generation: u64,
    pub confirmed: bool,
    pub abandoned: bool,
    /// Fully published historical tag without a train manifest.
    pub pre_train_legacy: bool,
    pub manifest_mismatch: bool,
    pub evidence_mismatch: bool,
    pub py_files_present: u32,
    pub py_files_expected: u32,
    pub py_digest_conflict: bool,
    pub py_extra_files: bool,
    pub confirmation_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClauseWire {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateEvalWire {
    pub candidate_id: String,
    pub generation: u64,
    pub version: String,
    pub publishable: bool,
    pub rerun_eligible: bool,
    pub rerun_reason: String,
    pub clauses: Vec<ClauseWire>,
    pub active_veto: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualify_red: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exercise_red: Option<String>,
    pub retained_asset_missing: bool,
    pub workflow_broken_at_tag: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreEnsureFactsWire {
    Pinned {
        version: String,
        files: Vec<String>,
    },
    CoreLag {
        pin: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        newest_complete: Option<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorePrFactsWire {
    pub number: u64,
    pub head_sha: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreCutRunWire {
    pub run_id: String,
    pub attempt: u32,
    pub source_pin: String,
    pub status: RunStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conclusion: Option<CheckConclusionWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_failure: Option<String>,
    pub diagnostic_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreFactsWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_ci: Option<WorkflowRunFactsWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_release_pr: Option<CorePrFactsWire>,
    /// Core-cut runs, newest first: every in-flight cut plus the newest 20
    /// completed cuts. Policy walks this order; a later success supersedes a
    /// stale typed failure. The wire shape does not include timestamps, so
    /// readers must already sort newest first.
    pub cuts: Vec<CoreCutRunWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryResultWire {
    pub member: String,
    pub run_id: String,
    pub attempt: u32,
    pub sha: String,
    pub verdict: String,
    pub packages: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failing_step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub scheduled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryFactsWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linux: Option<CanaryResultWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub macos: Option<CanaryResultWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueCommentWire {
    pub tick_identity: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueFactsWire {
    pub number: u64,
    pub title: String,
    pub state: String,
    pub labels: Vec<String>,
    pub identity_marker: String,
    pub family: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub comments: Vec<IssueCommentWire>,
    pub opened_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreActionsAccessWire {
    pub readable: bool,
    pub detail: String,
}

/// Host-measured continuous blocked lineage. Missing proof is uncertainty,
/// never a fabricated 24-hour age.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockerLineageWire {
    pub family: String,
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    pub continuous: bool,
    pub identity_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRefWire {
    pub kind: String,
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowFactsWire {
    pub release_yml: Vec<WorkflowRunFactsWire>,
    pub publish_yml: Vec<WorkflowRunFactsWire>,
}

/// Complete per-tick snapshot. Every collection is reconstructed fresh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseStewardSnapshotWire {
    pub repository: FactRead<RepositoryFactsWire>,
    pub master: FactRead<MasterFactsWire>,
    pub release_pr: FactRead<Option<ReleasePrFactsWire>>,
    pub tags: FactRead<Vec<TagFactsWire>>,
    pub releases: FactRead<Vec<ReleaseFactsWire>>,
    pub pypi: FactRead<PypiFactsWire>,
    pub trains: FactRead<Vec<TrainFactsWire>>,
    pub workflows: FactRead<WorkflowFactsWire>,
    pub core: FactRead<CoreFactsWire>,
    pub canary: FactRead<CanaryFactsWire>,
    pub candidate: FactRead<Option<CandidateEvalWire>>,
    pub core_ensure: FactRead<CoreEnsureFactsWire>,
    pub issues: FactRead<Vec<IssueFactsWire>>,
    pub core_actions_access: FactRead<CoreActionsAccessWire>,
    pub merge_history: FactRead<Vec<MergeEventWire>>,
    pub blocker_lineage: FactRead<Option<BlockerLineageWire>>,
}
