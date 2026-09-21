//! Fleet cache-invalidation hub and event subscriptions.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use sase_core::fleet_contract::{
    classify_cursor_replay, cursor_replay_reason_to_resync_reason,
    validate_fleet_invalidation_event, validate_fleet_replay_capacity,
    CursorReplayClassificationWire, CursorReplayRequestWire,
    FleetAuthoritativeSnapshotWire, FleetEventStreamItemWire,
    FleetInvalidationEventWire, FleetInvalidationKindWire,
    FleetResyncReasonWire, FleetResyncRequiredWire, ResourceRevisionWire,
    StoreCursorWire, FLEET_CONTRACT_SCHEMA_VERSION,
    FLEET_INITIAL_CURSOR_GENERATION,
};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use super::errors::FleetReadError;

const FLEET_EVENT_BROADCAST_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct FleetInvalidationHub {
    inner: Arc<Mutex<FleetInvalidationHubInner>>,
    sender: broadcast::Sender<FleetInvalidationEventWire>,
    capacity: usize,
}

impl std::fmt::Debug for FleetInvalidationHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FleetInvalidationHub")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FleetInvalidationHubInner {
    store_generation: String,
    sequence: u64,
    ring: VecDeque<FleetInvalidationEventWire>,
    deletion_history_complete: bool,
}

pub struct FleetEventSubscription {
    pub initial: Vec<FleetEventStreamItemWire>,
    pub receiver: broadcast::Receiver<FleetInvalidationEventWire>,
}

impl FleetInvalidationHub {
    pub fn new(capacity: usize) -> Result<Self, FleetReadError> {
        validate_fleet_replay_capacity(capacity)
            .map_err(FleetReadError::from)?;
        let (sender, _) = broadcast::channel(FLEET_EVENT_BROADCAST_CAPACITY);
        Ok(Self {
            inner: Arc::new(Mutex::new(FleetInvalidationHubInner {
                store_generation: new_generation(),
                sequence: 0,
                ring: VecDeque::new(),
                deletion_history_complete: true,
            })),
            sender,
            capacity,
        })
    }

    pub fn current_cursor(&self) -> StoreCursorWire {
        let inner = self.inner.lock().expect("fleet event hub poisoned");
        StoreCursorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            store_generation: inner.store_generation.clone(),
            sequence: inner.sequence,
        }
    }

    pub fn subscribe(
        &self,
        cursor: Option<StoreCursorWire>,
        snapshot: FleetAuthoritativeSnapshotWire,
    ) -> Result<FleetEventSubscription, FleetReadError> {
        let mut initial = Vec::new();
        let receiver = {
            let inner = self.inner.lock().map_err(|_| {
                FleetReadError::Backend("fleet_events".to_string())
            })?;
            let receiver = self.sender.subscribe();
            let cursor = cursor.unwrap_or(StoreCursorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                store_generation: FLEET_INITIAL_CURSOR_GENERATION.to_string(),
                sequence: 0,
            });
            let oldest = inner
                .ring
                .front()
                .map(|event| event.cursor.sequence)
                .unwrap_or(inner.sequence);
            let decision = classify_cursor_replay(&CursorReplayRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor,
                current_generation: inner.store_generation.clone(),
                newest_sequence: inner.sequence,
                oldest_replayable_sequence: oldest,
                deletion_history_complete: inner.deletion_history_complete,
            })
            .map_err(FleetReadError::from)?;
            match decision.classification {
                CursorReplayClassificationWire::Current => {}
                CursorReplayClassificationWire::Replayable => {
                    let replay_from =
                        decision.replay_from_sequence.unwrap_or(0);
                    for event in &inner.ring {
                        if event.cursor.sequence >= replay_from {
                            initial.push(
                                FleetEventStreamItemWire::Invalidation(
                                    event.clone(),
                                ),
                            );
                        }
                    }
                }
                CursorReplayClassificationWire::ResyncRequired => {
                    initial.push(FleetEventStreamItemWire::ResyncRequired(
                        FleetResyncRequiredWire {
                            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                            reason: cursor_replay_reason_to_resync_reason(
                                decision.reason,
                            ),
                            snapshot,
                        },
                    ));
                }
            }
            receiver
        };
        Ok(FleetEventSubscription { initial, receiver })
    }

    pub fn publish(
        &self,
        kind: FleetInvalidationKindWire,
        logical_key: Option<String>,
        row_revision: Option<ResourceRevisionWire>,
        reason: &str,
    ) -> Result<FleetInvalidationEventWire, FleetReadError> {
        let event = {
            let mut inner = self.inner.lock().map_err(|_| {
                FleetReadError::Backend("fleet_events".to_string())
            })?;
            inner.sequence = inner.sequence.saturating_add(1);
            let event = FleetInvalidationEventWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: StoreCursorWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    store_generation: inner.store_generation.clone(),
                    sequence: inner.sequence,
                },
                kind,
                logical_key,
                row_revision,
                reason: reason.to_string(),
            };
            validate_fleet_invalidation_event(&event)
                .map_err(FleetReadError::from)?;
            inner.ring.push_back(event.clone());
            while inner.ring.len() > self.capacity {
                inner.ring.pop_front();
            }
            event
        };
        let _ = self.sender.send(event.clone());
        Ok(event)
    }

    #[cfg(test)]
    pub(super) fn replace_generation(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.store_generation = new_generation();
            inner.sequence = 0;
            inner.ring.clear();
            inner.deletion_history_complete = true;
        }
    }

    #[cfg(test)]
    pub(super) fn mark_deletion_history_incomplete(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.deletion_history_complete = false;
        }
    }
}

fn new_generation() -> String {
    let now = Utc::now();
    let mut hasher = Sha256::new();
    hasher.update(now.timestamp_nanos_opt().unwrap_or(0).to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    format!("gen-{}", &hex::encode(hasher.finalize())[..24])
}

pub fn resync_item(
    reason: FleetResyncReasonWire,
    snapshot: FleetAuthoritativeSnapshotWire,
) -> FleetEventStreamItemWire {
    FleetEventStreamItemWire::ResyncRequired(FleetResyncRequiredWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        reason,
        snapshot,
    })
}
