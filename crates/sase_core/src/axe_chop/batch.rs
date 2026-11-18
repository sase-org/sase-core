//! Crash-safe chop launch batch contract.
//!
//! The host owns YAML decoding, filesystem/process IO, and launching. This
//! module owns the versioned batch manifest wire, the fail-closed validation,
//! the member/batch state machine, the resume reconciliation rules, and the
//! progress counts. Python persists manifests and observes spawn evidence; it
//! must not reimplement these transitions.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::wire::{ChopEngineError, ProposalWaitOnWire};

/// Version for chop launch batch manifests and batch requests.
pub const CHOP_BATCH_SCHEMA_VERSION: u32 = 1;

/// Largest accepted batch: well above the largest observed clan (61 members)
/// while keeping resume scans bounded.
pub const CHOP_BATCH_MAX_MEMBERS: usize = 512;

const CHOP_BATCH_MAX_ID_CHARS: usize = 256;
const CHOP_BATCH_MAX_PROMPT_BYTES: usize = 256 * 1024;
const CHOP_BATCH_MAX_ENV_ENTRIES: usize = 64;
const CHOP_BATCH_MAX_ENV_KEY_CHARS: usize = 256;
const CHOP_BATCH_MAX_ENV_VALUE_BYTES: usize = 64 * 1024;
const CHOP_BATCH_MAX_REASON_CHARS: usize = 512;

/// Lifecycle state of one batch member.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChopBatchMemberStatusWire {
    #[default]
    Pending,
    Dispatching,
    Launched,
    Skipped,
    Blocked,
    Cancelled,
}

/// Lifecycle state of the batch itself.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChopBatchStatusWire {
    #[default]
    Open,
    Paused,
    Cancelled,
    Complete,
}

/// How one member relates to its clan: first declarer, joiner of an existing
/// generation, or a standalone launch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChopBatchLaunchRoleWire {
    #[default]
    Standalone,
    Declare,
    Join,
}

/// One state-machine event applied to a manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopBatchEventKindWire {
    BeginDispatch,
    AcknowledgeLaunch,
    RecordSkip,
    MarkBlocked,
    RequestPause,
    Resume,
    CancelClanGeneration,
}

/// One explicit spawn observation for a batch member, recorded by the host
/// after reconciling durable evidence (spawn artifacts, launch receipts, and
/// existing chop linkage). A name reservation alone is [`ChopBatchSpawnObservationKindWire::ReservationOnly`],
/// never proof of launch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopBatchSpawnObservationKindWire {
    /// Durable evidence shows this batch already spawned the member.
    MatchedSpawn,
    /// The name is reserved but no spawn evidence exists.
    ReservationOnly,
    /// An unrelated live owner holds the name; never allocate a new identity.
    ForeignConflict,
    /// The previous owner is still alive; leave the member alone.
    LiveOwner,
}

/// Original routine/job/run identity a batch resumes. Extra preflight and
/// checkpoint context is reserved here so later phases never reshape the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchRunContextWire {
    pub routine: String,
    pub job: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_scope: Option<String>,
}

/// Reference to the typed admission bundle linked to this batch. The bundle
/// stays the sole per-unit dispatch authority; the batch only mirrors it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchTypedBundleRefWire {
    pub bundle_id: String,
    pub bundle_digest: String,
}

/// One accepted concrete launch plan frozen before the first dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchMemberWire {
    pub source_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
    pub agent_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan_generation: Option<String>,
    #[serde(default)]
    pub launch_role: ChopBatchLaunchRoleWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_on: Option<ProposalWaitOnWire>,
    pub prompt: String,
    pub workspace: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tribe: Option<String>,
    #[serde(default)]
    pub status: ChopBatchMemberStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
}

/// Versioned crash-safe chop launch batch manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchManifestWire {
    pub schema_version: u32,
    pub batch_id: String,
    pub run: ChopBatchRunContextWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_bundle: Option<ChopBatchTypedBundleRefWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clan_generation: Option<String>,
    #[serde(default)]
    pub status: ChopBatchStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub members: Vec<ChopBatchMemberWire>,
}

/// Request to apply one state-machine event to a manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchTransitionRequestWire {
    pub schema_version: u32,
    pub manifest: ChopBatchManifestWire,
    pub event: ChopBatchEventKindWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One host-recorded spawn observation for a batch member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchSpawnObservationWire {
    pub agent_name: String,
    pub observation: ChopBatchSpawnObservationKindWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Request to select the resume work for a manifest from spawn observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchReconcileRequestWire {
    pub schema_version: u32,
    pub manifest: ChopBatchManifestWire,
    #[serde(default)]
    pub observations: Vec<ChopBatchSpawnObservationWire>,
}

/// One member that must not be relaunched automatically, with the reason why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchBlockedEntryWire {
    pub member_index: usize,
    pub agent_name: String,
    pub reason: String,
}

/// Per-status member counts for one batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchCountsWire {
    pub total: usize,
    pub pending: usize,
    pub dispatching: usize,
    pub launched: usize,
    pub skipped: usize,
    pub blocked: usize,
    pub cancelled: usize,
}

/// Resume selection for one manifest: members to launch, members already
/// proven launched, and members held back with reasons.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchResumeWire {
    pub schema_version: u32,
    pub batch_id: String,
    pub batch_status: ChopBatchStatusWire,
    pub to_launch: Vec<usize>,
    pub already_launched: Vec<usize>,
    pub blocked: Vec<ChopBatchBlockedEntryWire>,
    pub counts: ChopBatchCountsWire,
}

/// Batch identity plus progress counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopBatchSummaryWire {
    pub schema_version: u32,
    pub batch_id: String,
    pub batch_status: ChopBatchStatusWire,
    pub counts: ChopBatchCountsWire,
}

/// Validate a batch manifest without changing it.
pub fn validate_chop_batch_manifest(
    manifest: &ChopBatchManifestWire,
) -> Result<ChopBatchManifestWire, ChopEngineError> {
    validate_manifest(manifest)?;
    Ok(manifest.clone())
}

/// Apply one state-machine event to a manifest.
pub fn apply_chop_batch_transition(
    request: &ChopBatchTransitionRequestWire,
) -> Result<ChopBatchManifestWire, ChopEngineError> {
    if request.schema_version != CHOP_BATCH_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_BATCH_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    validate_manifest(&request.manifest)?;
    let mut manifest = request.manifest.clone();
    match request.event {
        ChopBatchEventKindWire::BeginDispatch => {
            let index = require_member(request)?;
            require_batch_open(&manifest, "begin_dispatch")?;
            let member = require_member_mut(&mut manifest, index)?;
            match member.status {
                ChopBatchMemberStatusWire::Pending
                | ChopBatchMemberStatusWire::Dispatching => {
                    member.status = ChopBatchMemberStatusWire::Dispatching;
                }
                _ => {
                    return Err(terminal_state_error(index, "begin_dispatch"));
                }
            }
        }
        ChopBatchEventKindWire::AcknowledgeLaunch => {
            let index = require_member(request)?;
            require_batch_dispatchable(&manifest, "acknowledge_launch")?;
            let member = require_member_mut(&mut manifest, index)?;
            match member.status {
                ChopBatchMemberStatusWire::Dispatching
                | ChopBatchMemberStatusWire::Launched => {
                    member.status = ChopBatchMemberStatusWire::Launched;
                    member.status_reason = None;
                }
                ChopBatchMemberStatusWire::Pending => {
                    return Err(ChopEngineError::new(
                        "launch_without_dispatch",
                        format!("$.members[{index}].status"),
                        "acknowledge_launch requires begin_dispatch first: intent \
                         must be persisted before process creation",
                    ));
                }
                _ => {
                    return Err(terminal_state_error(
                        index,
                        "acknowledge_launch",
                    ));
                }
            }
        }
        ChopBatchEventKindWire::RecordSkip => {
            let index = require_member(request)?;
            let reason = require_reason(request, "record_skip")?;
            require_batch_dispatchable(&manifest, "record_skip")?;
            let member = require_member_mut(&mut manifest, index)?;
            match member.status {
                ChopBatchMemberStatusWire::Pending
                | ChopBatchMemberStatusWire::Dispatching
                | ChopBatchMemberStatusWire::Skipped => {
                    member.status = ChopBatchMemberStatusWire::Skipped;
                    member.status_reason = Some(reason);
                }
                _ => {
                    return Err(terminal_state_error(index, "record_skip"));
                }
            }
        }
        ChopBatchEventKindWire::MarkBlocked => {
            let index = require_member(request)?;
            let reason = require_reason(request, "mark_blocked")?;
            require_batch_dispatchable(&manifest, "mark_blocked")?;
            let member = require_member_mut(&mut manifest, index)?;
            match member.status {
                ChopBatchMemberStatusWire::Pending
                | ChopBatchMemberStatusWire::Dispatching
                | ChopBatchMemberStatusWire::Blocked => {
                    member.status = ChopBatchMemberStatusWire::Blocked;
                    member.status_reason = Some(reason);
                }
                _ => {
                    return Err(terminal_state_error(index, "mark_blocked"));
                }
            }
        }
        ChopBatchEventKindWire::RequestPause => {
            if request.member_index.is_some() {
                return Err(ChopEngineError::new(
                    "unexpected_member",
                    "$.member_index",
                    "request_pause applies to the batch, not one member",
                ));
            }
            match manifest.status {
                ChopBatchStatusWire::Open | ChopBatchStatusWire::Paused => {
                    manifest.status = ChopBatchStatusWire::Paused;
                    if let Some(reason) = request.reason.clone() {
                        manifest.status_reason = Some(reason);
                    }
                }
                _ => {
                    return Err(ChopEngineError::new(
                        "terminal_batch_conflict",
                        "$.status",
                        "request_pause cannot pause a cancelled or complete batch",
                    ));
                }
            }
        }
        ChopBatchEventKindWire::Resume => {
            if request.member_index.is_some() {
                return Err(ChopEngineError::new(
                    "unexpected_member",
                    "$.member_index",
                    "resume applies to the batch, not one member",
                ));
            }
            match manifest.status {
                ChopBatchStatusWire::Paused | ChopBatchStatusWire::Open => {
                    manifest.status = ChopBatchStatusWire::Open;
                    manifest.status_reason = None;
                }
                _ => {
                    return Err(ChopEngineError::new(
                        "terminal_batch_conflict",
                        "$.status",
                        "resume cannot reopen a cancelled or complete batch",
                    ));
                }
            }
        }
        ChopBatchEventKindWire::CancelClanGeneration => {
            if request.member_index.is_some() {
                return Err(ChopEngineError::new(
                    "unexpected_member",
                    "$.member_index",
                    "cancel_clan_generation applies to the batch, not one member",
                ));
            }
            let reason = require_reason(request, "cancel_clan_generation")?;
            match manifest.status {
                ChopBatchStatusWire::Open
                | ChopBatchStatusWire::Paused
                | ChopBatchStatusWire::Cancelled => {
                    manifest.status = ChopBatchStatusWire::Cancelled;
                    manifest.status_reason = Some(reason.clone());
                    for member in &mut manifest.members {
                        match member.status {
                            ChopBatchMemberStatusWire::Pending
                            | ChopBatchMemberStatusWire::Dispatching
                            | ChopBatchMemberStatusWire::Blocked => {
                                member.status =
                                    ChopBatchMemberStatusWire::Cancelled;
                                member.status_reason = Some(reason.clone());
                            }
                            _ => {}
                        }
                    }
                }
                ChopBatchStatusWire::Complete => {
                    return Err(ChopEngineError::new(
                        "terminal_batch_conflict",
                        "$.status",
                        "cancel_clan_generation cannot cancel a complete batch",
                    ));
                }
            }
        }
    }
    maybe_complete(&mut manifest);
    validate_manifest(&manifest)?;
    Ok(manifest)
}

/// Select the resume work for a manifest from explicit spawn observations.
///
/// This never mutates the manifest and never infers cancellation or phase
/// changes from process liveness: liveness only decides whether a
/// reservation-only member is relaunched or held for its live owner.
pub fn reconcile_chop_batch(
    request: &ChopBatchReconcileRequestWire,
) -> Result<ChopBatchResumeWire, ChopEngineError> {
    if request.schema_version != CHOP_BATCH_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_BATCH_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    validate_manifest(&request.manifest)?;
    let manifest = &request.manifest;
    let mut observed: BTreeMap<&str, &ChopBatchSpawnObservationWire> =
        BTreeMap::new();
    for observation in &request.observations {
        if observation.agent_name.trim().is_empty() {
            return Err(ChopEngineError::new(
                "blank_value",
                "$.observations[].agent_name",
                "observation agent_name must not be blank",
            ));
        }
        if observed
            .insert(observation.agent_name.as_str(), observation)
            .is_some()
        {
            return Err(ChopEngineError::new(
                "duplicate_observation",
                "$.observations",
                format!(
                    "duplicate observation for agent `{}`",
                    observation.agent_name
                ),
            ));
        }
    }

    let mut to_launch = Vec::new();
    let mut already_launched = Vec::new();
    let mut blocked = Vec::new();
    for (index, member) in manifest.members.iter().enumerate() {
        match member.status {
            ChopBatchMemberStatusWire::Launched => {
                already_launched.push(index);
            }
            ChopBatchMemberStatusWire::Skipped
            | ChopBatchMemberStatusWire::Cancelled => {}
            ChopBatchMemberStatusWire::Blocked => {
                blocked.push(ChopBatchBlockedEntryWire {
                    member_index: index,
                    agent_name: member.agent_name.clone(),
                    reason: member
                        .status_reason
                        .clone()
                        .unwrap_or_else(|| "blocked".to_string()),
                });
            }
            ChopBatchMemberStatusWire::Pending
            | ChopBatchMemberStatusWire::Dispatching => {
                reconcile_open_member(
                    manifest,
                    member,
                    index,
                    observed.get(member.agent_name.as_str()),
                    &mut to_launch,
                    &mut already_launched,
                    &mut blocked,
                );
            }
        }
    }
    Ok(ChopBatchResumeWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        batch_id: manifest.batch_id.clone(),
        batch_status: manifest.status,
        to_launch,
        already_launched,
        blocked,
        counts: count_members(&manifest.members),
    })
}

/// Return batch identity plus progress counts.
pub fn summarize_chop_batch(
    manifest: &ChopBatchManifestWire,
) -> Result<ChopBatchSummaryWire, ChopEngineError> {
    validate_manifest(manifest)?;
    Ok(ChopBatchSummaryWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        batch_id: manifest.batch_id.clone(),
        batch_status: manifest.status,
        counts: count_members(&manifest.members),
    })
}

#[allow(clippy::too_many_arguments)]
fn reconcile_open_member(
    manifest: &ChopBatchManifestWire,
    member: &ChopBatchMemberWire,
    index: usize,
    observation: Option<&&ChopBatchSpawnObservationWire>,
    to_launch: &mut Vec<usize>,
    already_launched: &mut Vec<usize>,
    blocked: &mut Vec<ChopBatchBlockedEntryWire>,
) {
    let mut hold = |reason: &str| {
        blocked.push(ChopBatchBlockedEntryWire {
            member_index: index,
            agent_name: member.agent_name.clone(),
            reason: reason.to_string(),
        });
    };
    match manifest.status {
        ChopBatchStatusWire::Cancelled => {
            hold("batch_cancelled");
            return;
        }
        ChopBatchStatusWire::Paused => {
            hold("batch_paused");
            return;
        }
        ChopBatchStatusWire::Complete => {
            hold("batch_complete");
            return;
        }
        ChopBatchStatusWire::Open => {}
    }
    match observation {
        None => to_launch.push(index),
        Some(found) => match found.observation {
            ChopBatchSpawnObservationKindWire::MatchedSpawn => {
                already_launched.push(index);
            }
            ChopBatchSpawnObservationKindWire::ReservationOnly => {
                to_launch.push(index);
            }
            ChopBatchSpawnObservationKindWire::ForeignConflict => {
                hold("foreign_name_conflict");
            }
            ChopBatchSpawnObservationKindWire::LiveOwner => {
                hold("live_owner");
            }
        },
    }
}

fn count_members(members: &[ChopBatchMemberWire]) -> ChopBatchCountsWire {
    let mut counts = ChopBatchCountsWire {
        total: members.len(),
        pending: 0,
        dispatching: 0,
        launched: 0,
        skipped: 0,
        blocked: 0,
        cancelled: 0,
    };
    for member in members {
        match member.status {
            ChopBatchMemberStatusWire::Pending => counts.pending += 1,
            ChopBatchMemberStatusWire::Dispatching => counts.dispatching += 1,
            ChopBatchMemberStatusWire::Launched => counts.launched += 1,
            ChopBatchMemberStatusWire::Skipped => counts.skipped += 1,
            ChopBatchMemberStatusWire::Blocked => counts.blocked += 1,
            ChopBatchMemberStatusWire::Cancelled => counts.cancelled += 1,
        }
    }
    counts
}

fn maybe_complete(manifest: &mut ChopBatchManifestWire) {
    match manifest.status {
        ChopBatchStatusWire::Open | ChopBatchStatusWire::Paused => {}
        _ => return,
    }
    let settled = manifest.members.iter().all(|member| {
        matches!(
            member.status,
            ChopBatchMemberStatusWire::Launched
                | ChopBatchMemberStatusWire::Skipped
                | ChopBatchMemberStatusWire::Cancelled
        )
    });
    if settled {
        manifest.status = ChopBatchStatusWire::Complete;
        manifest.status_reason = None;
    }
}

fn require_member(
    request: &ChopBatchTransitionRequestWire,
) -> Result<usize, ChopEngineError> {
    let index = request.member_index.ok_or_else(|| {
        ChopEngineError::new(
            "missing_member",
            "$.member_index",
            "this batch event requires a member_index",
        )
    })?;
    if index >= request.manifest.members.len() {
        return Err(ChopEngineError::new(
            "unknown_member",
            "$.member_index",
            format!(
                "member_index {index} is out of range for {} members",
                request.manifest.members.len()
            ),
        ));
    }
    Ok(index)
}

fn require_member_mut(
    manifest: &mut ChopBatchManifestWire,
    index: usize,
) -> Result<&mut ChopBatchMemberWire, ChopEngineError> {
    manifest.members.get_mut(index).ok_or_else(|| {
        ChopEngineError::new(
            "unknown_member",
            "$.member_index",
            format!("member_index {index} is out of range"),
        )
    })
}

fn require_reason(
    request: &ChopBatchTransitionRequestWire,
    event: &str,
) -> Result<String, ChopEngineError> {
    request
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .map(str::to_string)
        .filter(|reason| reason.chars().count() <= CHOP_BATCH_MAX_REASON_CHARS)
        .ok_or_else(|| {
            ChopEngineError::new(
                "missing_reason",
                "$.reason",
                format!("{event} requires a non-blank reason"),
            )
        })
}

fn require_batch_open(
    manifest: &ChopBatchManifestWire,
    event: &str,
) -> Result<(), ChopEngineError> {
    if manifest.status != ChopBatchStatusWire::Open {
        return Err(ChopEngineError::new(
            "batch_not_open",
            "$.status",
            format!("{event} requires an open batch"),
        ));
    }
    Ok(())
}

fn require_batch_dispatchable(
    manifest: &ChopBatchManifestWire,
    event: &str,
) -> Result<(), ChopEngineError> {
    match manifest.status {
        ChopBatchStatusWire::Open | ChopBatchStatusWire::Paused => Ok(()),
        _ => Err(ChopEngineError::new(
            "terminal_batch_conflict",
            "$.status",
            format!("{event} cannot run on a cancelled or complete batch"),
        )),
    }
}

fn terminal_state_error(index: usize, event: &str) -> ChopEngineError {
    ChopEngineError::new(
        "terminal_state_conflict",
        format!("$.members[{index}].status"),
        format!("{event} cannot move a member out of its terminal state"),
    )
}

fn validate_manifest(
    manifest: &ChopBatchManifestWire,
) -> Result<(), ChopEngineError> {
    if manifest.schema_version != CHOP_BATCH_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_BATCH_SCHEMA_VERSION}",
                manifest.schema_version
            ),
        ));
    }
    validate_id(&manifest.batch_id, "$.batch_id")?;
    validate_id(&manifest.run.routine, "$.run.routine")?;
    validate_id(&manifest.run.job, "$.run.job")?;
    validate_id(&manifest.run.run_id, "$.run.run_id")?;
    if let Some(bundle) = manifest.typed_bundle.as_ref() {
        validate_id(&bundle.bundle_id, "$.typed_bundle.bundle_id")?;
        if bundle.bundle_digest.trim().is_empty() {
            return Err(ChopEngineError::new(
                "blank_value",
                "$.typed_bundle.bundle_digest",
                "typed bundle digest must not be blank",
            ));
        }
    }
    if manifest.status == ChopBatchStatusWire::Cancelled
        && manifest
            .status_reason
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
    {
        return Err(ChopEngineError::new(
            "missing_status_reason",
            "$.status_reason",
            "a cancelled batch requires a status_reason",
        ));
    }
    if manifest.members.is_empty() {
        return Err(ChopEngineError::new(
            "empty_batch",
            "$.members",
            "a batch requires at least one member",
        ));
    }
    if manifest.members.len() > CHOP_BATCH_MAX_MEMBERS {
        return Err(ChopEngineError::new(
            "batch_too_large",
            "$.members",
            format!(
                "a batch holds at most {CHOP_BATCH_MAX_MEMBERS} members, got {}",
                manifest.members.len()
            ),
        ));
    }
    let mut names = BTreeSet::new();
    let mut indexes = BTreeSet::new();
    let mut proposal_ids = BTreeSet::new();
    for member in &manifest.members {
        if let Some(id) = member.proposal_id.as_deref() {
            if !id.trim().is_empty() {
                proposal_ids.insert(id);
            }
        }
    }
    for (index, member) in manifest.members.iter().enumerate() {
        let path = format!("$.members[{index}]");
        if !indexes.insert(member.source_index) {
            return Err(ChopEngineError::new(
                "duplicate_source_index",
                format!("{path}.source_index"),
                format!("duplicate source_index {}", member.source_index),
            ));
        }
        validate_id(&member.agent_name, &format!("{path}.agent_name"))?;
        if !names.insert(member.agent_name.as_str()) {
            return Err(ChopEngineError::new(
                "duplicate_agent_name",
                format!("{path}.agent_name"),
                format!("duplicate agent_name `{}`", member.agent_name),
            ));
        }
        if member.prompt.trim().is_empty() {
            return Err(ChopEngineError::new(
                "blank_value",
                format!("{path}.prompt"),
                "member prompt must not be blank",
            ));
        }
        if member.prompt.len() > CHOP_BATCH_MAX_PROMPT_BYTES {
            return Err(ChopEngineError::new(
                "prompt_too_large",
                format!("{path}.prompt"),
                format!(
                    "member prompt must not exceed {CHOP_BATCH_MAX_PROMPT_BYTES} bytes"
                ),
            ));
        }
        if member.workspace.trim().is_empty() {
            return Err(ChopEngineError::new(
                "blank_value",
                format!("{path}.workspace"),
                "member workspace must not be blank",
            ));
        }
        validate_env(&member.env, &path)?;
        if let Some(wait_on) = member.wait_on.as_ref() {
            validate_wait_on(
                wait_on,
                &proposal_ids,
                manifest.members.len(),
                &path,
            )?;
        }
        if matches!(
            member.status,
            ChopBatchMemberStatusWire::Blocked
                | ChopBatchMemberStatusWire::Skipped
        ) && member
            .status_reason
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
        {
            return Err(ChopEngineError::new(
                "missing_status_reason",
                format!("{path}.status_reason"),
                "a blocked or skipped member requires a status_reason",
            ));
        }
    }
    Ok(())
}

fn validate_id(value: &str, path: &str) -> Result<(), ChopEngineError> {
    if value.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            path,
            "this value must not be blank",
        ));
    }
    if value.chars().count() > CHOP_BATCH_MAX_ID_CHARS {
        return Err(ChopEngineError::new(
            "value_too_long",
            path,
            format!(
                "this value must not exceed {CHOP_BATCH_MAX_ID_CHARS} characters"
            ),
        ));
    }
    Ok(())
}

fn validate_env(
    env: &BTreeMap<String, String>,
    path: &str,
) -> Result<(), ChopEngineError> {
    if env.len() > CHOP_BATCH_MAX_ENV_ENTRIES {
        return Err(ChopEngineError::new(
            "env_too_large",
            format!("{path}.env"),
            format!(
                "member env holds at most {CHOP_BATCH_MAX_ENV_ENTRIES} entries"
            ),
        ));
    }
    for (key, value) in env {
        if key.trim().is_empty()
            || key.chars().count() > CHOP_BATCH_MAX_ENV_KEY_CHARS
        {
            return Err(ChopEngineError::new(
                "invalid_env_key",
                format!("{path}.env"),
                "env keys must be non-blank and bounded",
            ));
        }
        if value.len() > CHOP_BATCH_MAX_ENV_VALUE_BYTES {
            return Err(ChopEngineError::new(
                "env_value_too_large",
                format!("{path}.env"),
                format!(
                    "env values must not exceed {CHOP_BATCH_MAX_ENV_VALUE_BYTES} bytes"
                ),
            ));
        }
    }
    Ok(())
}

fn validate_wait_on(
    wait_on: &ProposalWaitOnWire,
    proposal_ids: &BTreeSet<&str>,
    member_len: usize,
    path: &str,
) -> Result<(), ChopEngineError> {
    match wait_on {
        ProposalWaitOnWire::Index(target) => {
            if *target >= member_len {
                return Err(ChopEngineError::new(
                    "unknown_wait_target",
                    format!("{path}.wait_on"),
                    format!(
                        "wait target index {target} is out of range for {member_len} members"
                    ),
                ));
            }
        }
        ProposalWaitOnWire::Id(target) => {
            if !proposal_ids.contains(target.as_str()) {
                return Err(ChopEngineError::new(
                    "unknown_wait_target",
                    format!("{path}.wait_on"),
                    format!("wait target id `{target}` names no batch member"),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
