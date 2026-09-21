use super::content::ResourceRevisionWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::FLEET_INITIAL_CURSOR_GENERATION;
use super::error::FLEET_READ_MAX_REPLAY_EVENTS;
use super::error::MAX_LABEL_BYTES;
use super::snapshot::FleetAuthoritativeSnapshotWire;
use super::validation::reject_path_like;
use super::validation::reject_secretish;
use super::validation::validate_identifier;
use super::validation::validate_key;
use super::validation::validate_label;
use super::validation::validate_schema;
use serde::{Deserialize, Serialize};

/// Event-feed cursor. This is separate from mutation resource revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCursorWire {
    pub schema_version: u32,
    pub store_generation: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorReplayRequestWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub current_generation: String,
    pub newest_sequence: u64,
    pub oldest_replayable_sequence: u64,
    pub deletion_history_complete: bool,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CursorReplayClassificationWire {
    Current,
    Replayable,
    ResyncRequired,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CursorReplayReasonWire {
    AtAuthorityHead,
    BehindWithinReplayWindow,
    InitialCursor,
    GenerationMismatch,
    SequenceAheadOfAuthority,
    ReplayGap,
    IncompleteDeletionHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorReplayDecisionWire {
    pub schema_version: u32,
    pub classification: CursorReplayClassificationWire,
    pub reason: CursorReplayReasonWire,
    pub replay_from_sequence: Option<u64>,
    pub authoritative_generation: String,
    pub authoritative_newest_sequence: u64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetInvalidationKindWire {
    Launched,
    LifecycleChanged,
    AttentionChanged,
    RevisionChanged,
    Deleted,
    ProcessExited,
    SnapshotReplaced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetInvalidationEventWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub kind: FleetInvalidationKindWire,
    pub logical_key: Option<String>,
    pub row_revision: Option<ResourceRevisionWire>,
    pub reason: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetResyncReasonWire {
    InitialCursor,
    GenerationMismatch,
    SequenceAheadOfAuthority,
    ReplayGap,
    IncompleteDeletionHistory,
    ReceiverLag,
    RingRolledOver,
    GenerationReplaced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetResyncRequiredWire {
    pub schema_version: u32,
    pub reason: FleetResyncReasonWire,
    pub snapshot: FleetAuthoritativeSnapshotWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum FleetEventStreamItemWire {
    Invalidation(FleetInvalidationEventWire),
    ResyncRequired(FleetResyncRequiredWire),
    Heartbeat { cursor: StoreCursorWire },
}

pub fn validate_store_cursor(
    cursor: &StoreCursorWire,
) -> Result<StoreCursorWire, FleetContractError> {
    cursor.validate()?;
    Ok(cursor.clone())
}

pub fn validate_fleet_replay_capacity(
    capacity: usize,
) -> Result<usize, FleetContractError> {
    if capacity == 0 {
        return Err(FleetContractError::Validation(
            "fleet replay capacity must be positive".to_string(),
        ));
    }
    if capacity > FLEET_READ_MAX_REPLAY_EVENTS {
        return Err(FleetContractError::Validation(format!(
            "fleet replay capacity exceeds {FLEET_READ_MAX_REPLAY_EVENTS} events"
        )));
    }
    Ok(capacity)
}

pub fn cursor_replay_reason_to_resync_reason(
    reason: CursorReplayReasonWire,
) -> FleetResyncReasonWire {
    match reason {
        CursorReplayReasonWire::InitialCursor => {
            FleetResyncReasonWire::InitialCursor
        }
        CursorReplayReasonWire::GenerationMismatch => {
            FleetResyncReasonWire::GenerationMismatch
        }
        CursorReplayReasonWire::SequenceAheadOfAuthority => {
            FleetResyncReasonWire::SequenceAheadOfAuthority
        }
        CursorReplayReasonWire::ReplayGap => FleetResyncReasonWire::ReplayGap,
        CursorReplayReasonWire::IncompleteDeletionHistory => {
            FleetResyncReasonWire::IncompleteDeletionHistory
        }
        CursorReplayReasonWire::AtAuthorityHead
        | CursorReplayReasonWire::BehindWithinReplayWindow => {
            FleetResyncReasonWire::ReplayGap
        }
    }
}

pub fn validate_fleet_invalidation_event(
    event: &FleetInvalidationEventWire,
) -> Result<FleetInvalidationEventWire, FleetContractError> {
    validate_schema("fleet invalidation event", event.schema_version)?;
    event.cursor.validate()?;
    if let Some(logical_key) = &event.logical_key {
        validate_key("fleet invalidation logical_key", logical_key)?;
    }
    if let Some(row_revision) = &event.row_revision {
        row_revision.validate()?;
        if let Some(logical_key) = &event.logical_key {
            if &row_revision.logical_key != logical_key {
                return Err(FleetContractError::Validation(
                    "fleet invalidation row_revision logical_key does not match event logical_key"
                        .to_string(),
                ));
            }
        }
    }
    validate_label(
        "fleet invalidation reason",
        &event.reason,
        MAX_LABEL_BYTES,
    )?;
    reject_path_like("fleet invalidation reason", &event.reason)?;
    reject_secretish("fleet invalidation reason", &event.reason)?;
    Ok(event.clone())
}

pub fn classify_cursor_replay(
    request: &CursorReplayRequestWire,
) -> Result<CursorReplayDecisionWire, FleetContractError> {
    validate_schema("cursor replay request", request.schema_version)?;
    request.cursor.validate()?;
    validate_identifier("current_generation", &request.current_generation)?;
    if request.oldest_replayable_sequence > request.newest_sequence {
        return Err(FleetContractError::Validation(
            "oldest_replayable_sequence cannot exceed newest_sequence"
                .to_string(),
        ));
    }
    let resync = |reason| CursorReplayDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        classification: CursorReplayClassificationWire::ResyncRequired,
        reason,
        replay_from_sequence: None,
        authoritative_generation: request.current_generation.clone(),
        authoritative_newest_sequence: request.newest_sequence,
    };
    if !request.deletion_history_complete {
        return Ok(resync(CursorReplayReasonWire::IncompleteDeletionHistory));
    }
    if request.cursor.store_generation == FLEET_INITIAL_CURSOR_GENERATION
        && request.cursor.sequence == 0
    {
        return Ok(resync(CursorReplayReasonWire::InitialCursor));
    }
    if request.cursor.store_generation != request.current_generation {
        return Ok(resync(CursorReplayReasonWire::GenerationMismatch));
    }
    if request.cursor.sequence > request.newest_sequence {
        return Ok(resync(CursorReplayReasonWire::SequenceAheadOfAuthority));
    }
    if request.cursor.sequence == request.newest_sequence {
        return Ok(CursorReplayDecisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            classification: CursorReplayClassificationWire::Current,
            reason: CursorReplayReasonWire::AtAuthorityHead,
            replay_from_sequence: None,
            authoritative_generation: request.current_generation.clone(),
            authoritative_newest_sequence: request.newest_sequence,
        });
    }
    let replay_from = request.cursor.sequence.saturating_add(1);
    if replay_from < request.oldest_replayable_sequence {
        return Ok(resync(CursorReplayReasonWire::ReplayGap));
    }
    Ok(CursorReplayDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        classification: CursorReplayClassificationWire::Replayable,
        reason: CursorReplayReasonWire::BehindWithinReplayWindow,
        replay_from_sequence: Some(replay_from),
        authoritative_generation: request.current_generation.clone(),
        authoritative_newest_sequence: request.newest_sequence,
    })
}

impl StoreCursorWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("store cursor", self.schema_version)?;
        validate_identifier("store_generation", &self.store_generation)
    }
}
